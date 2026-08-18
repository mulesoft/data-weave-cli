# Engine Lifecycle & Worker-OOM Hardening — Round 9 (W-23692110)

**Status:** Design approved, ready for planning.

**Source review:** `docs/pr-157-follow-up-andy-code-review-9.md` (three findings, all verified against live source at commit `05f8b31`, the round-8 tip).

**Scope:** `native-lib/node` only — `src/addon.c` and `src/dataweave.ts` if needed. Do **not** touch `native-lib/python/**`, the legacy singleton entrypoints, or `ScriptRuntime.getInstance()`. The Java side (`NativeLib.java`, `ScriptRuntime`) is read for context but not modified — the fix keeps the C addon from calling `fn_destroy_engine` too early rather than changing Java's registry semantics.

## Problem

The ninth "andy" follow-up review raised three findings. All three verified against live source and are real.

### #1 (P1) — `cleanup()` can invalidate an already-admitted stream/transform before its worker begins execution

`doCleanup()` (dataweave.ts:151-155) calls `ffi.destroyEngine(handle)` and only then `await ffi.cleanup()`. `napi_destroy_engine` (addon.c:1543-1546) calls `fn_destroy_engine(thread, handle)` **unconditionally and synchronously**, which removes the handle from `ScriptRuntime.REGISTRY`. A streaming/transform op that already passed admission (`g_active_ops++` at addon.c:753 / 1166) but whose background worker has not yet called `fn_run_script_callback_engine` / `fn_run_script_input_output_callback_engine` will then hit `ScriptRuntime.get(handle) == null` (NativeLib.java:457-460) and return `{"success":false,"error":"Unknown engine handle"}` instead of completing.

**Why the existing deferral does not cover this:** the `in_flight`/`destroy_pending` machinery (addon.c:91-107, 259-281, 1554-1568) defers only the resolver **bridge** free, and it exists **only for resolver-backed engines** (`bridge_begin_op` increments `in_flight` only when `bridge_find != NULL`, addon.c:262). The registry removal (`fn_destroy_engine`) is never deferred, and resolver-less engines have no per-engine op accounting at all. So the registry entry is yanked regardless of in-flight ops.

### #2 (P2) — output-callback / worker allocations crash on OOM

Unchecked allocations in the streaming/transform worker + callback machinery dereference NULL / `strlen(NULL)` / strand worker state on OOM:
- `streaming_write_cb` (addon.c:616-619): `malloc(sizeof chunk)` and `malloc(len)` then `memcpy`.
- `transform_write_cb` (addon.c:985-988): same shape.
- Worker `strdup`/sentinel sites: streaming (640, 646, 649, 666-669), transform (1072, 1081, 1084, 1097-1100).

### #3 (P3) — N-API resource creation unchecked after reserving `g_active_ops`

Streaming (addon.c:798-803) and transform (1243-1250) ignore the status of `napi_create_string_utf8`, `napi_create_threadsafe_function`, and `napi_create_promise`. A failed TSFN/promise leaves `w->tsfn` / `w->deferred` zeroed for the worker → crash or a stranded `g_active_ops` (teardown wedge).

### Recurrence note

#2 and #3 are the structurally-identical siblings of round 8's setup-allocation fix — round 8 hardened the *setup* mallocs because review #8 named those; review #9 walks to the *worker/callback* allocations and the *resource-creation* checks. Round 9 sweeps the whole class (**every fallible native op in the streaming/transform worker + callback paths**: `malloc`/`strdup`/`memcpy`, `napi_create_*`) so no structurally-identical site is left for a round 10. #1 is a distinct cross-layer lifecycle race, fixed on its own.

## Design

### 1. Defer registry removal until this engine's admitted ops drain (finding #1)

Generalize the existing per-engine deferral so the **registry removal** (`fn_destroy_engine`) is deferred exactly like the bridge free already is, and make the per-engine in-flight count exist for **all** engines (resolver-backed and resolver-less).

**Data model (user decision — extend the record to all engines):** every engine gets a per-engine record (today's `engine_bridge_t`) at `createEngine` time, carrying `handle`, `in_flight`, `destroy_pending`. The resolver-specific fields (`resolver_js`, `env`, `owner`, `results`, the env cleanup hook) remain populated **only for resolver-backed engines**; a resolver-less engine gets a record with those fields zero/NULL.

**Admission (JS thread, both streaming + transform), before spawning the worker:** increment this engine's `in_flight` for **every** engine (not just `bridge_find != NULL`). Store the record pointer on `w` (`w->bridge` already exists; it now is non-NULL for all engines). The completion sentinel already calls `bridge_end_op(w->bridge, ...)`, which decrements `in_flight` and finalizes on drain — this now runs for all engines.

**`napi_destroy_engine`:** under `g_mutex`, if the engine's `in_flight > 0`, set `destroy_pending = true` and **defer** the `fn_destroy_engine` registry-removal call (do not call it now); the last op to drain (`bridge_end_op` → finalize) performs `fn_destroy_engine` on completion. If `in_flight == 0`, call `fn_destroy_engine` now, as today. `fn_destroy_engine` attaches its own fresh isolate thread (addon.c:1544-1545), so it is **not** JS-thread-affine and is safe to call from the completion sentinel (which runs on the owner JS thread) or from `destroyEngine` directly.

**Finalize path:** `bridge_finalize` gains responsibility for the deferred `fn_destroy_engine` call (guarded so it happens exactly once, only when it was deferred). The resolver `napi_ref` deletion + env-cleanup-hook removal stay exactly as today, only for resolver-backed engines, on the owner thread.

**CRITICAL invariant to preserve — do NOT change the owner-thread destroy restriction's scope.** Today the cross-thread guard (addon.c:1530-1541) fires only for resolver-backed engines (`bridge_find != NULL`) because only they hold thread-affine `napi_ref`/cleanup-hook state. Now that resolver-less engines also have a record, the guard must still fire **only when the record has resolver state** (`resolver_js != NULL` / an env-cleanup hook was registered) — a resolver-less engine must remain destroyable from any thread, unchanged. Gate the owner check on "has resolver napi state," not on "record exists."

**Ordering / correctness to confirm during review:**
- The `in_flight++` at admission happens under `g_mutex` on the JS thread before the worker is spawned, so `destroyEngine` either sees `in_flight > 0` (defers) or the op has not yet been admitted (nothing to protect). No admitted op can have its registry entry removed before it runs.
- `fn_destroy_engine` is called **exactly once** per handle — either the immediate path (in_flight == 0) or the deferred finalize path (last drain), never both. Guard with the same `destroy_pending`/unlink-once discipline the bridge free already uses.
- Resolver-less engines: `bridge_end_op` now runs for them (previously `w->bridge == NULL` short-circuited). Confirm `bridge_finalize` on a resolver-less record deletes no `napi_ref` (there is none) and removes no cleanup hook (none registered), just performs the deferred `fn_destroy_engine` (if pending) and frees the record.
- `g_active_ops` (global isolate drain) and the per-engine `in_flight` (per-handle registry drain) are **distinct** counters with distinct jobs; this round does not merge them. `g_active_ops` still gates isolate teardown; `in_flight` now gates registry removal.

### 2. Worker/callback OOM → terminal error result (finding #2)

Every allocation in the worker + callback machinery checks its result and fails the op cleanly, with **no `g_active_ops` / `in_flight` leak** (user decision — terminal error result, never a hung promise):

- **`streaming_write_cb` / `transform_write_cb`:** if `malloc(sizeof chunk)` or `malloc(len)` returns NULL, free any partial (`free(chunk)` if the inner malloc failed) and `return -1`. Returning -1 aborts the native run cleanly (the existing contract: write callback returns non-zero → the DataWeave run stops), and the worker still produces a terminal `meta_result` and sentinel.
- **Worker `strdup` of `meta_result`** (streaming 640/646/649, transform 1072/1081/1084): if `strdup` returns NULL, fall back to a **static** const OOM JSON string (e.g. `"{\"success\":false,\"error\":\"Out of memory\"}"`). The sentinel-drop / `call_js_write` completion path must then **not** `free()` a static pointer — introduce a flag or a convention (e.g. only `free(sentinel->buf)` when it was heap-allocated) so the static string is never freed. Simplest: keep a `static const char OOM_JSON[]` and a small helper that returns either a `strdup` or, on failure, sets a "do not free" marker. Design detail deferred to the plan; the invariant is: **the op always resolves with a terminal result and no buffer is double-freed or freed-if-static.**
- **Sentinel `malloc`** (streaming 666-669, transform 1097-1100): if the sentinel `malloc` returns NULL, skip the `napi_call_threadsafe_function` enqueue and run the same finalize-here path the env-dead (`napi_closing`) branch already runs (release tsfn, `bridge_end_op`, free `w`, free `meta_result` if heap) — so `g_active_ops`/`in_flight` are released and nothing is stranded. `g_active_ops` is already decremented before the sentinel block, so only `bridge_end_op` + resource frees remain.

The bare error string wording matches the existing worker error style (`"Empty response"`, `"Failed to attach thread"`). Keep it terse.

### 3. Check N-API resource creation after the reservation (finding #3)

In both `napi_run_script_streaming_engine` (798-803) and `napi_run_script_transform_engine` (1243-1250), check the status of every `napi_create_string_utf8`, `napi_create_threadsafe_function`, and `napi_create_promise`. On any failure, unwind in reverse order of what was created so far:
- release any already-created threadsafe function(s) (`napi_release_threadsafe_function`),
- release the per-engine `in_flight` hold if `bridge_begin_op` already ran (it runs *after* these creates today — confirm ordering; if the creates are above `bridge_begin_op`, no `in_flight` unwind is needed there),
- release `g_active_ops` with the verbatim pattern,
- free `w` (and its buffers),
- `napi_throw_error(env, NULL, "...")` and return NULL.

Because these creates sit **after** `g_active_ops++` but the exact position relative to `bridge_begin_op` matters, the plan must place each check so the unwind set is complete and ordered. The worker must never observe a zeroed `w->tsfn` / `w->write_tsfn` / `w->read_tsfn` / `w->deferred`.

### 4. Testing

**No new runtime test — all three findings are covered by C-level code reasoning.** This is the same documented limitation as rounds 6–8: the failure paths are not deterministically forceable from JS/vitest.

- **#2 / #3** — the OOM and N-API-create-failure paths need allocator / N-API fault injection at the addon boundary, which does not exist. Coverage is code reasoning: every allocation/create is checked before use; every failure path unwinds `g_active_ops`, `in_flight`, and frees partials; no double-free; no hung promise.
- **#1** — despite the spec's earlier draft, this is **not** deterministically forceable either. `ScriptRuntime.get(handle)` (`NativeLib.java:457`, `:492`) is the **first statement** of the worker's Java entrypoint — it runs *before* any read/write callback fires. So the observable "Unknown engine handle" window is the gap between op **admission** (worker spawned, promise returned) and the worker's Java **lookup**, which is entirely *before* the first chunk. A test that fires `destroyEngine` from inside a callback cannot reproduce it (the lookup already succeeded; the worker holds its `runtime` locally and completes fine even on unfixed code). The review itself calls the symptom "nondeterministic." A synchronous-fire-after-admission race-window loop would be green-on-fixed but only *probabilistically* red-on-unfixed — not the deterministic guard rounds 5's test provides — so per the round-9 decision #1 gets **no new runtime test**; its correctness is established by code reasoning against the ordering invariants below.

Baseline is therefore unchanged at **878 passed / 59 skipped / 0 failed** — no new test, no regression.

## Verification

- `cd native-lib/node && npm run build:addon` clean (no new warnings in the touched regions); `npm run build` (tsc) clean.
- `npm test` green: baseline **878 passed / 59 skipped / 0 failed**, unchanged (no new test — see §4).
- `git diff --check`.

## Global Constraints

- Node-binding-only. Never touch `native-lib/python/**`.
- Never touch the legacy singleton entrypoints (`run_script`, `run_script_callback`, `run_script_input_output_callback`), `dw_napi_run_script`, or `ScriptRuntime.getInstance()`. The Java side is not modified.
- Handle width stays C `long long` everywhere.
- Errors for run/streaming/transform APIs surface as **resolved** JSON string values (the worker's terminal `meta_result`) or a synchronous `napi_throw_error` at admission / argument validation / allocation / resource-creation failure — never `napi_reject_deferred`.
- Allocation-failure rejections at the synchronous admission layer use `napi_throw_error` (generic Error). Worker-thread OOM produces a terminal error JSON result string (static when the copy itself failed).
- `napi_env`/`napi_ref`/`napi_deferred`/`napi_threadsafe_function` are thread-affine to the env's JS thread. No env-affine call from the worker thread except through the existing tsfn.
- All shared C state (`g_initialized`, `g_active_ops`, `g_teardown_state`, `g_teardown_cancelled`, `g_ref_count`, and every engine record's `in_flight`/`destroy_pending`) is read/written only under `g_mutex`.
- The `g_active_ops` release pattern is EXACTLY, verbatim: `uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);`
- `fn_destroy_engine` is called **exactly once** per handle — never both the immediate and the deferred path.
- The owner-thread `destroyEngine` restriction stays scoped to engines with resolver `napi_ref` state; resolver-less engines remain destroyable from any thread.
- Preserve every round-1..8 fix: coalesced `cleanup()`, the JS three-state lifecycle machine, the `TEARDOWN_*` state machine and `napi_initialize` adoption path (incl. round-7's `g_teardown_cancelled` admission carve-out), the worker-thread `g_active_ops` decrement, the streaming/transform atomic admission blocks, the round-6 handle-read validations, the round-7 conversion-status checks, the round-8 setup-allocation NULL checks, guarded cross-thread `destroyEngine`, N-API allocation checks in `teardown_waiter_create`, the enqueue-failure waiter free, the resolver-bridge `in_flight`/`destroy_pending` deferral and its owner-thread `napi_ref` discipline.
- Node vitest baseline **878 passed / 59 skipped / 0 failed** — every task leaves the suite green.

## Rejected Alternatives

- **#1 via a JS-side reorder in `doCleanup()` (await per-engine drain before `destroyEngine`).** Rejected: there is no per-engine "await my ops" primitive at the JS layer; streaming is an abandonable generator and `run()` is synchronous, so the class cannot reliably await outstanding ops, and `destroyEngine`'s owner-thread `napi_ref` deletion cannot move into the global `ffi.cleanup()` isolate teardown. The authoritative drain state lives in C.
- **#1 via a separate per-handle op map alongside the resolver-only bridge.** Considered (keeps `engine_bridge_t` focused on resolver state). Rejected in favor of extending the existing record to all engines (user decision) — one structure, one deferral path, no second linked list to keep in sync with the first.
- **#2 abort-op-without-result on worker OOM.** Rejected (user decision): leaving the op's promise unresolved is a worse failure than a terminal error result; the static-OOM-JSON terminal result keeps the op's contract (always resolves) intact.
- **#2/#3 fixing only the cited lines.** Rejected: the per-site habit that produced the round-N-finds-the-sibling recurrence. Round 9 sweeps the whole worker/callback allocation + resource-creation class.
- **Merging `g_active_ops` and per-engine `in_flight` into one counter.** Rejected: they gate different resources (global isolate teardown vs. per-handle registry removal) with different lifetimes; conflating them would reintroduce the class of bug rounds 5–7 fixed.
- **Adding an allocator/N-API fault-injection hook to test #2/#3.** Rejected as test-only production surface (YAGNI), consistent with rounds 6–8.
- **A race-window loop test for #1** (synchronous `destroyEngine` right after admission, looped N times). Rejected: green-on-fixed but only *probabilistically* red-on-unfixed, so it is not the deterministic guard round 5's deadlock test is — it would pass on the unfixed code whenever the worker's Java lookup happens to win the race. Not worth a permanently-running probabilistic test; #1's correctness rests on the ordering invariants in §Design.1 verified by code reasoning.
- **Modifying the Java `ScriptRuntime` registry to tolerate late lookups.** Rejected as out of scope and the wrong layer — the C addon must not remove the entry early in the first place; changing Java semantics would mask the ordering bug rather than fix it.
