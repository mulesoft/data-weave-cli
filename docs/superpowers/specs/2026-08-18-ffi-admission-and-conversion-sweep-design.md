# FFI Admission & Conversion Sweep — Round 7 (W-23692110)

**Status:** Design approved, ready for planning.

**Source review:** `docs/pr-157-follow-up-andy-code-review-7.md` (three findings, all verified against live source at commit `d6cd4ec`, the round-6 tip).

**Scope:** `native-lib/node` only — `src/addon.c`, `docs/external-modules.md`, and new tests under `tests/`. Do **not** touch `native-lib/python/**`.

## Problem

The seventh "andy" follow-up review of PR #157 raised three findings. All three were verified against live source and are real. Two of them (#1 and #2) are the **structurally-identical siblings** of sites that round 6 fixed — round 6's own final review flagged them as "Minor / pre-existing, out-of-scope," and this review escalates #1 to P1.

### Root cause of the recurrence

The concurrency machinery introduced across rounds 3–6 is sound; the recurrence is a **scoping habit**, not a new class of bug each round. Each round fixed exactly the sites its review named, and the next review walked to the sibling site with the same defect:

- Round 6 made **streaming + transform** admission atomic under `g_mutex`, but left the **synchronous `run()`** path out because that review cited only streaming/transform. → round-7 #1.
- Round 6 validated the **three handle-read** `napi_get_value_int64` conversions, but not the **string-length reads** or **`destroyEngine`**, because those weren't cited. → round-7 #2.

Round 7 breaks the cycle by fixing both defect **classes** uniformly, so no structurally-identical site is left for a round 8 to find.

### The three findings (all confirmed)

**#1 (P1) — buffered `run()` is not protected from concurrent isolate teardown.**
`napi_run_script_engine` (addon.c:1500-1534) touches the isolate (`fn_attach_thread` → `fn_run_script_engine` → `fn_detach_thread`) with only the top-of-function `if (!g_initialized)` fast-path. It never reserves `g_active_ops` under `g_mutex`. A second Node Worker performing the last `cleanup()` can observe `g_active_ops == 0` (`napi_cleanup` Case 4), tear down `g_isolate`, and leave this synchronous op attaching to / executing in a dead isolate — a use-after-free.

**#2 (P2) — raw addon callers can pass malformed values that become uninitialized native inputs.**
Multiple FFI-facing entrypoints ignore the return status of `napi_get_value_*` conversions:
- `destroyEngine` (addon.c:1441) — ignores `napi_get_value_int64`; a non-integer handle yields an indeterminate `handle64` and could destroy an unrelated engine.
- `run` string lengths (addon.c:1513-1514), `streaming` (addon.c:751-752), `transform` (addon.c:1146-1167) — ignore the `napi_get_value_string_utf8` size-probe status; on a non-string argument `*_len` stays uninitialized before `malloc(len + 1)` and the subsequent buffer write.

**#3 (P2) — documentation examples do not await asynchronous `cleanup()`.**
`native-lib/node/docs/external-modules.md:197-198` and `:310` call `cleanup()` without `await`, contradicting round 6's new async lifecycle contract (`cleanup(): Promise<void>`).

## Design

### 1. Atomic admission for synchronous `run()` (finding #1)

Give `napi_run_script_engine` the same mutex-protected lifecycle admission that streaming/transform got in round 6, but reserve **late** — immediately before `fn_attach_thread`, not at the top of the function.

```c
uv_mutex_lock(&g_mutex);
if (!g_initialized || g_teardown_state != TEARDOWN_NONE) {
  uv_mutex_unlock(&g_mutex);
  free(script); free(inputs);
  napi_throw_error(env, NULL, "Not initialized. Call initialize() first.");
  return NULL;
}
g_active_ops++;
uv_mutex_unlock(&g_mutex);

void* thread = NULL;
if (fn_attach_thread(g_isolate, &thread) != 0) {
  uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
  free(script); free(inputs);
  napi_throw_error(env, NULL, "Failed to attach thread");
  return NULL;
}

char* result = (char*)fn_run_script_engine(thread, handle, script, inputs);
// ... existing resolver_results_free_all, strdup, fn_free_cstring, fn_detach_thread, free(script/inputs) ...

uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
```

**Why late, not early (unlike streaming/transform):** the string `malloc`s and argument extraction don't touch the isolate, so the reservation only needs to span `attach → detach`. Reserving just before attach yields exactly **two** unwind sites — the attach-failure branch and normal completion — instead of additionally having to unwind the OOM/allocation path. `run()` is fully synchronous on the JS thread, so both the reservation and the release happen inline; there is no worker thread. The `uv_cond_broadcast(&g_teardown_cond)` on decrement is what wakes a `teardown_waiter_thread_fn` blocked on `g_active_ops > 0`, matching how the streaming/transform worker threads decrement.

**Ordering vs. Part 2:** the string-length checks (Part 2) run before the reservation, so a malformed-input throw there returns before `g_active_ops++` and needs no unwind. The reservation block is placed after the buffers are populated and before attach.

**Keep the top-of-function `!g_initialized` fast-path** as a cheap early reject; the authoritative check is the one under the lock. The already-validated handle `int64` read (round 6, addon.c:1505-1510) is unchanged.

### 2. Uniform `napi_get_value_*` status checks (finding #2 → whole class)

Every FFI-facing entrypoint checks the status of **every** `napi_get_value_*` conversion and throws via `napi_throw_error` (consistent with all existing throws in the file — round-6 handle validation, "Not initialized", "OOM") **before** using the converted value.

Guiding invariant: **no converted value is read before its conversion status is confirmed `napi_ok`, and no throw leaves `g_active_ops` reserved.**

Sites:
- **`destroyEngine` (addon.c:1441):** check `napi_get_value_int64`; throw "destroyEngine: handle must be an integer" before any registry lookup or destroy. No `g_active_ops` on this path.
- **`run` (addon.c:1513-1519):** check both `napi_get_value_string_utf8` size probes; throw before `malloc(len + 1)`. These checks run **before** the Part 1 reservation, so no unwind needed. Also check the fill-phase `napi_get_value_string_utf8` calls.
- **`streaming` (addon.c:751-759):** check both size probes and both fills. A throw here happens **after** `g_active_ops++` (round-6 admission block sits above), so each must `g_active_ops--; uv_cond_broadcast(&g_teardown_cond);` under `g_mutex` and free any already-allocated buffers before returning.
- **`transform` (addon.c:1146-1167):** same — check every size probe and fill, and the `napi_typeof` for `argv[5]`; throw-after-reservation paths must unwind `g_active_ops` and free partial allocations.

The already-validated handle `int64` reads at the streaming/transform sites (round 6) are left as-is. Scope is the FFI-facing entrypoints' conversions — not a blanket audit of unrelated `napi_*` calls (YAGNI).

### 3. Docs await `cleanup()` (finding #3)

In `native-lib/node/docs/external-modules.md`, make the example functions that call `cleanup()` `async` and `await cleanup()` in their `finally` blocks (lines 197-198, 310). Sweep the whole document for any other bare `cleanup()` call and fix consistently.

### 4. Testing

New regression tests use the **real addon** (no `vi.mock` of `ffi`), mirroring `tests/integration/handle-validation.test.ts` and `admission-during-teardown.test.ts`, and all fully clean up (balance every `ffi.initialize()` with `await ffi.cleanup()`) so they do not perturb the shared process-wide isolate for sibling integration tests.

1. **Finding #1 — `run()` admission.** Drive raw `ffi.runScriptEngine` and assert the admission-rejection path: a `run()` attempted while teardown is pending throws rather than attaching to a dead isolate. Document in the test that the genuine cross-Worker TOCTOU is not reliably forceable from JS (same limitation as round-6 #2); the C-level reasoning — check-and-reserve is now atomic under `g_mutex` on the `run()` path — is what covers the race.
2. **Finding #2 — malformed inputs throw, nothing allocated on an uninitialized length.** Raw-`ffi` calls: a non-integer handle to `destroyEngine`; non-string `script`/`inputs` to `run`, `runStreaming`, `runTransform`. Each throws synchronously. Extends the `handle-validation.test.ts` pattern.
3. **Finding #3 — docs only.** No automated test; verified by inspection.

## Verification

- `cd native-lib/node && npm run build:addon` clean (no new warnings in the touched C regions); `npm run build` (tsc) clean.
- `npm test` green: current baseline **873 passed / 59 skipped / 0 failed**, plus the new regression tests.
- `git diff --check`.

## Global Constraints

- Node-binding-only. Never touch `native-lib/python/**`.
- Never touch the legacy singleton entrypoints (`run_script`, `run_script_callback`, `run_script_input_output_callback`) or `ScriptRuntime.getInstance()`.
- Handle width stays C `long long` everywhere.
- Errors for run/streaming/transform APIs surface as **resolved** JSON string values or a synchronous `napi_throw_error` at admission / argument validation — never `napi_reject_deferred` (absent from addon.c; do not introduce).
- `napi_env`/`napi_ref`/`napi_deferred`/`napi_threadsafe_function` are thread-affine to the env's JS thread.
- All shared C state (`g_initialized`, `g_active_ops`, `g_teardown_state`, `g_teardown_cancelled`, `g_ref_count`) is read/written only under `g_mutex`. (The cheap top-of-function `!g_initialized` fast-path read is a benign optimization; the authoritative check is under the lock.)
- Preserve every round-1..6 fix: coalesced `cleanup()`, the JS three-state lifecycle machine, the `TEARDOWN_*` state machine and `napi_initialize` adoption path, the worker-thread `g_active_ops` decrement, the streaming/transform atomic admission blocks, the round-6 handle-read validations, guarded cross-thread `destroyEngine`, N-API allocation checks in `teardown_waiter_create`, the enqueue-failure waiter free.
- Node vitest baseline **873 passed / 59 skipped / 0 failed** — every task leaves the suite green.

## Rejected Alternatives

- **Finding #1 — reserve early (top of function) like streaming/transform.** Rejected: the string `malloc`s and argument extraction don't touch the isolate, so an early reservation would force the OOM/allocation-failure path to also unwind `g_active_ops`, adding a third unwind site for no safety benefit. Late reservation (just before attach) spans exactly the isolate-touching window with two unwind sites.
- **Finding #2 — `napi_throw_type_error` (TypeError).** Considered because the review says "JavaScript type error" and TypeError is the N-API convention for wrong-type args. Rejected in favor of `napi_throw_error` (generic Error) for consistency with every existing throw in addon.c; the message text conveys the type problem. (User decision.)
- **Finding #2 — blanket-audit every `napi_*` call in addon.c.** Rejected as scope creep (YAGNI). Sweep the conversions in the FFI-facing entrypoints — the defect class the review names — not unrelated N-API calls.
- **Finding #1 — only fix the exact cited lines without sweeping `run()`'s siblings.** Rejected: this is the very habit that produced the round-N-finds-the-sibling recurrence. Round 7 covers both defect classes uniformly.
