# Review #6 Remediation — Singleton, Stream, and Teardown Hardening (Round 15)

**Status:** Design approved. Ready for implementation plan.

**Scope decision (user):** Fix all 8 code findings (#1–#8). Finding #9 (Python-binding scope) is left as-is with a PR note, not a code change. Full pipeline (spec → plan → SDD). Standing finish: push + update PR #157.

**Reviewed head:** `7017ded` (round-14 HEAD). All 8 code findings validated against live source before this design.

---

## Context

`docs/pr-157-follow-up-code-review-6.md` raised 9 findings against PR #157 head `7017ded`. Eight are code fixes; #9 is a scope/process observation (the PR carries broad Python-binding modernization beyond the Node multi-engine change) handled by a PR comment, not code.

The findings fall into three clusters plus one process note:

- **Cluster A (TypeScript, 2 High):** a real user-facing singleton-poisoning bug (#1) and a real stream-hang bug (#2).
- **Cluster B (C teardown, 2 Medium):** two hardening gaps (#3, #4) in the round-14 teardown machinery.
- **Cluster C (C teardown design, 1 Medium):** the drain-reachability gap (#5) that round 14's own final reviewer flagged as a non-blocking observation.
- **Cluster D (tests, 2 Medium + 1 Low):** test-hygiene fixes (#6, #7, #8) that keep the suite honest.

**Preserved invariant (unchanged from round 14, binding on every C change here):**
`g_ref_count == Σ per-env init_refs` at every `g_mutex` release. `g_teardown_needed` is a retry SIGNAL, not a reference — set only when `g_ref_count == 0` and the isolate is still live; never added to any count; read/written only under `g_mutex`.

**Global constraints (carried from prior rounds):**
- Node-binding only. Never touch `native-lib/python/**`, the Java side, or the legacy singleton entrypoints (`run_script`, `run_script_callback`, `run_script_input_output_callback`, `dw_napi_run_script`, `ScriptRuntime.getInstance()`).
- Handle width stays C `long long`.
- Errors surface as resolved JSON strings (async) or synchronous `napi_throw_error` / thrown `DataWeaveError` — never `napi_reject_deferred`.
- `napi_env`/`napi_ref`/`napi_deferred`/`napi_threadsafe_function` are thread-affine to the env's JS thread; no env-affine napi call from the wrong thread.

---

## Cluster A — TypeScript user-facing bugs

### #1 (High): a failed first module-level initialization permanently poisons the singleton

**Defect:** `getGlobalInstance()` (`native-lib/node/src/dataweave.ts:366-372`) assigns `globalInstance` *before* `initialize()` succeeds:

```ts
function getGlobalInstance(): DataWeave {
  if (!globalInstance) {
    globalInstance = new DataWeave();
    globalInstance.initialize();   // if this throws, globalInstance stays set-but-uninitialized
    registerExitHooksOnce();
  }
  return globalInstance;
}
```

If `initialize()` throws (bad `DATAWEAVE_NATIVE_LIB` path, transient native failure), the singleton remains a non-null, uninitialized `DataWeave`. Every later `run*()` reuses it and fails only with "not initialized" — even after the underlying cause is fixed.

**Fix:** construct and initialize a *local candidate*; assign `globalInstance` only after `initialize()` returns; register exit hooks after the successful assignment.

```ts
function getGlobalInstance(): DataWeave {
  if (!globalInstance) {
    // Initialize a LOCAL candidate first; publish the singleton only after
    // initialize() succeeds. A failed first init (bad lib path / transient
    // native failure) must NOT leave a poisoned, uninitialized singleton that
    // makes every later run*() fail "not initialized" even after the fault is
    // fixed (review #6 #1). On throw, globalInstance stays null and the next
    // call retries cleanly.
    const candidate = new DataWeave();
    candidate.initialize();
    globalInstance = candidate;
    registerExitHooksOnce();
  }
  return globalInstance;
}
```

**Regression:** fail singleton init once (mock `ffi.initialize` to throw), assert the call rejects/throws and `globalInstance` was not published; then correct the fault (mock initialize to succeed) and assert the next `run()` builds a fresh working singleton.

### #2 (High): a rejected native streaming promise can hang the consumer forever

**Defect:** `streamFromNative()` (`native-lib/node/src/stream.ts:39-47`) wires only the fulfilled branch:

```ts
const metaPromise = start(chunkCb).then((raw) => {
  metaRaw = raw;
  done = true;
  while (pendingResolves.length > 0) {
    const resolve = pendingResolves.shift();
    if (resolve) resolve();
  }
});
```

If `start()` rejects, `done` never becomes `true` and parked `next()` consumers (waiting on a `pendingResolves` promise, stream.ts:55) are never woken → the generator hangs forever. The rejection is also unhandled.

**Fix:** handle both settlement branches — on rejection, record the error, set completion, wake all waiters. After the drain loop, if a start error was recorded, throw it (so the consumer sees a rejection, not a silent empty completion). Buffered chunks that arrived before the rejection still drain first.

```ts
  let startError: unknown;
  const wakeAll = () => {
    while (pendingResolves.length > 0) {
      const resolve = pendingResolves.shift();
      if (resolve) resolve();
    }
  };
  const metaPromise = start(chunkCb).then(
    (raw) => { metaRaw = raw; done = true; wakeAll(); },
    (err) => {
      // Native start() rejected. Without this branch, `done` stays false and a
      // consumer parked in next() is never woken -> the generator hangs forever,
      // and the rejection is unhandled (review #6 #2). Record the failure, mark
      // completion, and wake every waiter; the error is re-thrown after draining
      // any chunks that arrived before the rejection.
      startError = err;
      done = true;
      wakeAll();
    }
  );

  while (true) {
    if (chunks.length > 0) { yield chunks.shift()!; continue; }
    if (done) break;
    await new Promise<void>((resolve) => { pendingResolves.push(resolve); });
  }

  while (chunks.length > 0) { yield chunks.shift()!; }

  await metaPromise;              // settles (fulfilled) since we handled rejection above
  if (startError !== undefined) throw startError;
  return parseStreamingResult(metaRaw ?? "");
```

Note: because the `.then(onFulfilled, onRejected)` form handles rejection, `metaPromise` itself always fulfills, so `await metaPromise` never throws and there is no unhandled rejection. The consumer-visible error is the explicit `throw startError`.

**Regression:** `start: () => Promise.reject(new Error("native start boom"))` with a consumer that is already parked in `next()` before the rejection settles — assert `next()` (or the `for await`) rejects with the error and does not hang. A second test: chunks buffered then rejection — assert buffered chunks yield first, then it throws.

---

## Cluster B — C teardown hardening

### #3 (Medium): isolate teardown reports success even when Graal teardown fails

**Defect:** both teardown sites treat calling `graal_tear_down_isolate()` as success without checking its `int` return (`typedef int (*graal_tear_down_isolate_fn)(void*)`, addon.c:12):

- `cleanup_thread_fn` (addon.c:2332): `fn_tear_down_isolate(local_thread); *out_torn_down = 1;`
- `teardown_waiter_thread_fn` (addon.c:2366-2368): `fn_tear_down_isolate(local_thread); torn_down = true;` (comment at 2360 literally says "Ignore the return code, matching today's behavior.")

If teardown returns nonzero, the callers still clear `g_isolate`/`g_initialized`/`g_ref_count` as if the isolate is gone — orphaning a live isolate and allowing a *second* `graal_create_isolate` in the same process (unsupported).

**Fix:** set `torn_down` only when the call returns 0.

- `cleanup_thread_fn`:
  ```c
  *out_torn_down = (fn_tear_down_isolate(local_thread) == 0) ? 1 : 0;
  // Nonzero: teardown failed, isolate still live -- leave *out_torn_down = 0 so
  // the caller retains g_isolate/g_initialized and arms the retry (review #6 #3).
  ```
- `teardown_waiter_thread_fn`:
  ```c
  torn_down = (fn_tear_down_isolate(local_thread) == 0);
  ```
  Update the stale comment at 2360.

The existing "attach failed → leave torn_down 0" paths already handle the retained-live-isolate case correctly; #3 just extends that to the "attach succeeded but teardown returned nonzero" case. Arming the retry on a nonzero teardown is handled together with #4 below (both are in `teardown_waiter_thread_fn`'s post-teardown block).

### #4 (Medium): async teardown-waiter attach failure leaves an ownerless isolate without retry

**Defect:** in `teardown_waiter_thread_fn`'s post-teardown lock (addon.c:2379-2389), when `!cancelled && !torn_down` (attach failed, or — after #3 — teardown returned nonzero), the code leaves `g_ref_count == 0`, no owner, no pending waiter, `g_teardown_state = TEARDOWN_NONE`, and does *not* arm `g_teardown_needed`. The comment claims "retried on the next last release" — but this async waiter path IS the last-release path (`isolate_ref_release_n_locked`'s `g_active_ops > 0` branch spawned it). There is no future last-release; the isolate is stranded with no retry signal.

**Fix:** in that post-teardown block, when teardown did not happen and the isolate is still live with zero owners, arm the retry signal:

```c
  uv_mutex_lock(&g_mutex);
  if (!cancelled && torn_down) {
    g_thread = NULL;
    g_isolate = NULL;
    g_initialized = 0;
    g_ref_count = 0;
  } else if (!cancelled && g_isolate != NULL && g_ref_count == 0) {
    // Teardown did not happen (attach failed, or graal_tear_down_isolate
    // returned nonzero -- review #6 #3) and this async-waiter path IS the
    // last release: g_ref_count is already 0 with no owner and no pending
    // waiter. Arm the retry signal so a later op-completion drain or an
    // initialize() retries teardown -- otherwise the live isolate is stranded
    // with nothing to reclaim it (review #6 #4).
    g_teardown_needed = true;
  }
  g_teardown_state = TEARDOWN_NONE;
  g_teardown_cancelled = false;
  ...
```

This mirrors the arm already present in `isolate_ref_release_n_locked`'s waiter-spawn-failure path (addon.c:2570) and Case-4 sync-failure path.

---

## Cluster C — the #5 drain-reachability gap

### #5 (Medium): stranded-teardown retry is not guaranteed to run when no operation remains

**Defect:** the zero-active-op synchronous failure paths arm `g_teardown_needed`:
- `isolate_ref_release_n_locked` sync branch (addon.c:2543-2548)
- `release_isolate_ref_locked` Case-4 (addon.c:2725)

But `retry_stranded_teardown_locked()` is called ONLY from the streaming (addon.c:967) and transform op-completion drains. In the zero-op state there is no pending operation to drain, so the retry never fires. Worse, a later `initialize()` currently *adopts* the isolate and clears the flag (the fast-path / adoption clears at addon.c:623/643/724) instead of completing the pending teardown. `cleanup()` has already resolved, so from the caller's view the reference was released — but the isolate the retry was meant to reclaim is silently kept alive and its retry intent discarded.

**Chosen fix (user decision): make the next `initialize()` complete the pending teardown before adopting — no new async infrastructure.**

At the top of `napi_initialize`, under `g_mutex`, before the existing adoption / fast-path / create-path logic: if `g_teardown_needed` is set (a prior teardown failed and the isolate is stranded with zero owners), call `retry_stranded_teardown_locked()` first.

- If the retry succeeds, `g_isolate` becomes `NULL` and `g_initialized` becomes 0 → `napi_initialize` falls through to the create path and builds a fresh isolate. The pending teardown is honored, not discarded.
- If the retry fails again (spawn/attach/teardown still failing), the isolate is still live; `napi_initialize` proceeds to adopt it via the existing fast path (which clears the now-still-set flag). Adopting a live isolate whose teardown was merely resource-reclamation (not a malfunction) is safe and functionally identical to normal adoption.

```c
  uv_mutex_lock(&g_mutex);
  // A prior last-release could not tear the isolate down and armed the retry
  // signal (review #6 #3/#4). Because retries otherwise fire only at op
  // completion, a zero-op stranded isolate would never be reclaimed and a naive
  // adoption below would silently discard the pending teardown (review #6 #5).
  // Drive the pending teardown to completion here first: on success g_isolate is
  // cleared and we build a fresh isolate below; on repeated failure the live
  // isolate is adopted by the fast path (safe -- teardown was reclamation, not a
  // malfunction).
  retry_stranded_teardown_locked();
  // ... existing TEARDOWN_PENDING_WAIT adoption / fast-path / create-path logic ...
```

`retry_stranded_teardown_locked()` already no-ops safely when `g_teardown_needed` is false, when `g_active_ops > 0`, or when a teardown is in progress — so this call is a cheap guard on the common path (flag clear → immediate return).

**Documented residual degradation (accepted):** if a teardown fails AND no later `initialize()` or streaming/transform op ever occurs, the stranded isolate lingers until process exit, where the OS reclaims it. This is benign (a single process-lifetime isolate, no correctness or reference-count violation) and is the deliberate tradeoff for avoiding event-loop-affine async retry infrastructure on this concurrency-sensitive code. This residual is documented in a comment at the arming sites and in the spec's Rejected Alternatives.

---

## Cluster D — test hardening

### #6 (Medium): Worker clean-lifecycle scenarios suppress explicit engine-destruction errors

**Defect:** in `runWorker`'s worker body (`native-lib/node/tests/integration/worker-lifecycle.test.ts:62-65`), the `cleanup: true` path swallows `destroyEngine` errors:

```js
if (workerData.cleanup) {
  try { addon.destroyEngine(handle); } catch (_) {}
  await addon.cleanup();
}
```

A broken explicit-destruction path can be masked by the subsequent `addon.cleanup()`, so a "clean lifecycle" test still passes.

**Fix:** capture the destruction error, still run `addon.cleanup()` in a `finally`, then fold the original error into the posted message (so the stricter `runWorker` exit handling and the caller's `msg.ok` assertion surface it):

```js
if (workerData.cleanup) {
  let destroyErr;
  try {
    addon.destroyEngine(handle);
  } catch (e) {
    destroyErr = e;   // preserve; do NOT let cleanup() mask a broken destroy path
  } finally {
    await addon.cleanup();
  }
  if (destroyErr) msg = { ok: false, error: "destroyEngine failed: " + String(destroyErr) };
}
```

This keeps all existing clean-path Workers green (destroy succeeds → `destroyErr` undefined → `msg` unchanged) while surfacing a real destruction failure as `ok: false`.

### #7 (Medium): the cross-env regression can contaminate later tests on failure

**Defect:** the round-14 cross-env test (`worker-lifecycle.test.ts:209-272`) acquires `hMain` and a main-thread init reference with no `try/finally`. Any Worker or assertion failure before the final `destroyEngine(hMain)` + `cleanup()` leaves global native state (live isolate, held reference) for subsequent tests.

**Fix:** wrap the test body in `try/finally`. In `finally`, destroy `hMain` if it was acquired and balance the main init reference (`await ffi.cleanup()`), guarded so the balancing does not throw over and mask an original assertion failure:

```ts
    let hMain: number | null = null;
    try {
      ffi.initialize(LIB_PATH);
      hMain = ffi.createEngine();
      // ... existing test body, using hMain ...
    } finally {
      // Balance global native state even if a Worker/assertion failed above, so
      // this test cannot strand a live isolate + held reference for sibling
      // integration tests (review #6 #7). Do not let cleanup errors mask the
      // original failure.
      try {
        if (hMain !== null) ffi.destroyEngine(hMain);
        await ffi.cleanup();
      } catch { /* balancing best-effort; original failure (if any) propagates */ }
    }
```

The final positive assertions (main engine survives; raw op throws `/not initialized/i` after balancing) stay in the `try` so the test still proves what it did before; only the reference balancing moves to `finally`. Because the `finally` now always balances, the `/not initialized/i` probe must run inside `try` *before* the finally's cleanup (it already does — it is the last positive step of the body). The RED-on-round-12 behavior is unchanged: the main-engine survival assertion still fails on round-12.

### #8 (Low): the initialization unit test can falsely pass when reinitialization is a no-op

**Defect:** `dataweave-initialize.test.ts:248-252` asserts a second `initialize()` via `toHaveBeenLastCalledWith()`, but the *first* `initialize()` already called `createEngine()` with the same (no) arguments — so the assertion passes even if the second init created no engine.

**Fix:** clear the `createEngine` mock before the re-initialization (`vi.mocked(ffi.createEngine).mockClear()`), or assert the call count went from 1 to 2. The design uses `mockClear()` before the second `initialize()` plus `expect(ffi.createEngine).toHaveBeenCalledTimes(1)` after, proving the re-init genuinely created a fresh engine.

---

## File / task structure

Each task ends with an independently testable deliverable and a fresh reviewer gate.

| Task | Finding(s) | Files | Test |
|------|-----------|-------|------|
| 1 | #1 | `src/dataweave.ts` (`getGlobalInstance`) | `tests/unit/dataweave-initialize.test.ts` (+1) |
| 2 | #2 | `src/stream.ts` (`streamFromNative`) | `tests/unit/stream.test.ts` (+2) |
| 3 | #3, #4 | `src/addon.c` (`cleanup_thread_fn`, `teardown_waiter_thread_fn`) | error-path C hardening; suite unchanged |
| 4 | #5 | `src/addon.c` (`napi_initialize`) | error-path C hardening; suite unchanged |
| 5 | #6, #7 | `tests/integration/worker-lifecycle.test.ts` | suite unchanged (all green paths still pass) |
| 6 | #8 | `tests/unit/dataweave-initialize.test.ts` | tightened assertion; suite unchanged |

**Task ordering rationale:**
- Tasks 1 and 6 both touch `dataweave-initialize.test.ts`; Task 1 *appends* a new test, Task 6 *tightens an existing* test — no overlap, but Task 6 runs after Task 1 to avoid a stale line-anchor.
- Tasks 3 and 4 both touch `addon.c` teardown machinery; 3 (thread-fn return codes + arm) precedes 4 (`napi_initialize` drives the retry), since 4's fix relies on 3's arming being correct.
- Task 5's two changes (#6, #7) are in one file and reviewed together.

**Expected suite deltas:** Task 1 +1 unit, Task 2 +2 unit; Tasks 3–6 no count change (error-path C hardening + test tightening). Round-14 baseline 899/59/0 → **902/59/0** after this round.

---

## Verification (end-to-end)

1. `cd native-lib/node && npm run build:addon` — clean, no new warnings in `cleanup_thread_fn`, `teardown_waiter_thread_fn`, or `napi_initialize`. `npm run build:ts` clean.
2. `npm test` (with `DATAWEAVE_NATIVE_LIB` set) — **902 passed / 59 skipped / 0 failed**.
3. **Invariant audit (review gate):** every `g_ref_count` mutation still paired with an `init_refs` mutation or a rollback to `env_init_refs_total_locked()`/0; `g_teardown_needed` set only when `g_ref_count == 0`, never added to a count; all new shared-state access under `g_mutex`. The #3 return-code check must never clear `g_isolate`/`g_initialized`/`g_ref_count` on a nonzero teardown.
4. #1/#2 regressions genuinely reproduce the bug (fail on the pre-fix code): singleton stays poisoned; stream hangs/rejects unhandled.
5. `git diff --check` — no whitespace errors.

---

## Rejected Alternatives

- **#1: reset `globalInstance = null` in a `catch` inside `getGlobalInstance` instead of a local candidate.** Works, but the local-candidate pattern is clearer (the singleton is never observably set to a bad value, even transiently across an `await` boundary elsewhere) and matches the "construct-then-publish" idiom the reviewer requested.
- **#2: reject via `napi_reject_deferred` / a rejected returned promise from the generator.** The generator contract is to throw from `next()`; the codebase deliberately surfaces errors as thrown values, not rejected deferreds (global constraint). An explicit `throw startError` after draining is the idiomatic fit.
- **#5: dedicated async retry owner (uv_async / uv_timer).** Fully closes the no-future-init-or-op residual, but adds event-loop-affine async infrastructure and new concurrency surface to the most sensitive code in the binding. The user chose init-driven completion + documented degradation as the lower-risk option; the residual (isolate lingers to process exit if nothing else ever happens) is benign.
- **#5: block `napi_initialize` until the pending teardown physically completes on a helper thread even when it keeps failing.** Could deadlock or spin on a persistently failing `graal_tear_down_isolate`; adopting the live isolate after one retry attempt is safe and bounded.
- **#9: split the Python-binding work into its own PR now.** User chose to leave the PR as-is and note the bundling in a PR comment; no git surgery this round.
