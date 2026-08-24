# Review #7 — Teardown-detach, init-rollback, and lifecycle-doc hardening (W-23692110)

**Status:** Approved design. Reviewed head at review time: `aaeafb9`; live head at design time: `6fb5603` (post-rebase onto master). All findings re-verified against `6fb5603`; line numbers below are the live ones.

**Reviewer:** `docs/pr-157-follow-up-code-review-7.md` (the "code-review" series, round 7). Eight findings. Seven are in scope this round (#1–#7); #8 (Python-scope split) is kept as the standing "leave-as-is, note in PR" decision and answered to the reviewer, not actioned in code.

## Goal

Close the seven code/documentation findings from review #7 without regressing the multi-engine lifecycle invariants established in rounds 1–15. Two are genuine native-lifecycle defects (a phantom attached GraalVM thread on failed teardown; an unsignaled init-wait after a failed init-hook rollback), one is a TypeScript rollback-observability defect, one is a low-severity stream mis-report, one is a test-hygiene gap, and two are documentation corrections.

## Invariant (unchanged, preserved by every fix)

`g_ref_count == Σ per-env init_refs` at every `g_mutex` release. `g_teardown_needed` is a **retry signal, not a reference** — set only when `g_ref_count == 0` and the isolate is still live; never added to any count; read/written only under `g_mutex`. Teardown state machine: `TEARDOWN_NONE` / `TEARDOWN_PENDING_WAIT` / `TEARDOWN_TEARING_DOWN`, all transitions under `g_mutex`. Errors surface as resolved JSON strings (async) or synchronous `napi_throw_error` / thrown `DataWeaveError` — never `napi_reject_deferred`.

## Global Constraints (binding on every task)

- Node-binding-only scope. NEVER touch `native-lib/python/**`.
- Never touch the legacy singleton entrypoints (`run_script`, `run_script_callback`, `run_script_input_output_callback`), `dw_napi_run_script`, or `ScriptRuntime.getInstance()`. Java side not modified this round.
- Handle width stays C `long long` everywhere.
- Errors surface as resolved JSON strings (async) or synchronous `napi_throw_error` / thrown `DataWeaveError` — NEVER `napi_reject_deferred`.
- `napi_env`/`napi_ref`/`napi_deferred`/`napi_threadsafe_function` are thread-affine to the env's JS thread; no env-affine napi call from the waiter/wrong thread.
- All new shared state read/written only under `g_mutex`.
- `initialize()` and `run()` stay **synchronous** (returning `void` / `ExecutionResult`, not Promises) — an async signature is an API break and is a rejected alternative.
- `docs/superpowers/plans/` is git-ignored; only specs are tracked. Do not `git add -A` — untracked scratch docs under `docs/` must stay untracked; stage only named files.

## Findings and chosen fixes

### #1 (High) — failed Graal teardown leaves the cleanup thread attached to the live isolate

**Where:** `native-lib/node/src/addon.c` — `cleanup_thread_fn` (~2337–2347) and `teardown_waiter_thread_fn` (~2379–2388).

**Defect:** Both paths attach a local IsolateThread (`fn_attach_thread`), call `fn_tear_down_isolate(local_thread)`, and treat a nonzero return as failure (correctly, since review #6 #3) by leaving the isolate live and arming the retry. But on that failure branch they exit the helper thread **without detaching** `local_thread`. `graal_tear_down_isolate` does not tear down on a nonzero return, so the attachment is still live; exiting the OS thread while attached leaves a phantom attached thread in the isolate, which can make a later retry teardown block or fail indefinitely.

**Fix:** On the nonzero-teardown branch **only**, call `fn_detach_thread(local_thread)` before leaving `torn_down` / `*out_torn_down` at 0. The success branch (return 0) is untouched — the isolate is gone and the thread must NOT be detached against a torn-down isolate. The attach-failure branch is untouched — no thread was attached.

- `cleanup_thread_fn`: change `*out_torn_down = (fn_tear_down_isolate(local_thread) == 0) ? 1 : 0;` to capture the result, and on nonzero call `fn_detach_thread(local_thread)` before leaving `*out_torn_down` at 0.
- `teardown_waiter_thread_fn`: same shape around `torn_down = (fn_tear_down_isolate(local_thread) == 0);`.

This does not change any state-machine transition, ref count, or the retry arming — it only reclaims the thread attachment on the already-existing failure path.

### #2 (Medium) — init-hook failure can wedge all future initialization if compensating teardown fails

**Where:** `native-lib/node/src/addon.c` `napi_initialize` (~682–726), the `if (!env_init_acquire_and_hook(env))` rollback block.

**Defect:** When `env_init_acquire_and_hook` fails after the isolate was built, the code spawns `cleanup_thread_fn` to tear the just-built isolate back down. On success (`torn_down`) it clears `g_isolate`/`g_thread`/`g_initialized` — recoverable. But on the `else` branch (spawn failed, or attach/teardown failed) it leaves `g_isolate != NULL, g_initialized == 0, g_teardown_state == TEARDOWN_NONE`, and **no retry signal armed**. The next `initialize()` on any env reaches the wait loop condition `g_isolate != NULL && !g_initialized`, and with `TEARDOWN_NONE` it cannot take the adoption branch, so it falls into `uv_cond_wait(&g_teardown_cond, ...)` that nothing will ever broadcast → every future `initialize()` hangs forever.

**Fix:** In that `else` branch, arm the retry signal: `g_teardown_needed = true;`. `retry_stranded_teardown_locked()` already runs at the very top of `napi_initialize` (~607, under `g_mutex`), so the next `initialize()` retries the stranded teardown before reaching the wait loop — either clearing the isolate (then building fresh) or, on repeated failure, adopting the still-live isolate via the fast path. This mirrors the identical arm already present in `teardown_waiter_thread_fn` (~2402–2410) and `isolate_ref_release_n_locked`'s waiter-spawn-failure path. Ref-count reasoning is unchanged: `g_ref_count` is still 0 here (we never bumped it), so arming `g_teardown_needed` (a signal, not a reference) does not perturb the invariant.

### #3 (Medium) — module/instance initialization rollback starts async cleanup without observing it

**Where:** `native-lib/node/src/dataweave.ts` `initialize()` (~92–105), the `catch` block calling `ffi.cleanup()`.

**Defect:** When engine creation throws after `ffi.initialize()` succeeded, the catch calls `ffi.cleanup()` (which returns `Promise<void>`) to release the native library ref, but neither awaits nor attaches a handler. A rollback-teardown rejection becomes an unhandledRejection, and a caller can immediately retry `initialize()`/`run()` while that rollback is still in flight — racing a fresh `graal_create_isolate` against the in-flight release.

**Fix:** Model the rollback as pending state using the existing `state`/`cleanupPromise` machinery, keeping `initialize()` synchronous:
- Before throwing, set `this.state = "cleaning-up"` and assign `this.cleanupPromise` to the rollback promise: `ffi.cleanup()` wrapped so that when it settles the state returns to `"uninitialized"` and `cleanupPromise` clears (in a `.finally`), mirroring `doCleanup()`/`cleanup()`.
- Attach a `.catch(() => {})` to the stored promise so an un-awaited rollback never surfaces as an unhandledRejection.
- Because `state` is `"cleaning-up"` until the rollback settles, a concurrent `initialize()` hits the existing `"cleaning-up"` guard and throws "Cannot initialize while cleanup is in progress; await cleanup() first." — deterministic rejection instead of a race. A concurrent `cleanup()` coalesces onto the same `cleanupPromise` (existing behavior).
- The synchronous `throw new DataWeaveError(...)` to the *current* caller is preserved (the initialize attempt failed); the difference is the rollback is now observable and re-initialization is gated until it settles.

This reuses the exact coalescing/observability contract the codebase already documents for `cleanup()`; no new field is required beyond reusing `cleanupPromise`.

### #4 (Medium) — root native-lib README documents stale synchronous Node cleanup

**Where:** `native-lib/README.md` §4 "Explicit instance lifecycle" (~460, `dw.cleanup()` with no await, no try/finally) and §9 "Cleanup" (~638–644, describes only a `process.on('exit')` hook and shows bare `cleanup()`).

**Fix:** Update both examples to `await dw.cleanup()` / `await cleanup()` inside `try/finally`, and align the hook/lifecycle prose with the accurate package README (`native-lib/node/README.md`): the async `Promise<void>` return, the `beforeExit` (awaits/drains) + `exit` (sync fallback) hook pair, that signals are not covered, and the last-reference teardown condition. Documentation-only; no code.

### #5 (Medium) — class-level and package-README cleanup docs overstate teardown completion

**Where:** `native-lib/node/src/dataweave.ts` `cleanup()` JSDoc (~109–119) and `native-lib/node/README.md:222`.

**Defect:** Both say cleanup "resolves once the underlying native isolate has actually finished tearing down" unconditionally. That holds only when the call releases the **final** shared native reference; otherwise it resolves after releasing this instance's engine while the isolate stays live for other instances.

**Fix:** State the final-reference condition explicitly, matching the wording already correct in the module-level `cleanup` doc (README.md:193): resolves after isolate teardown only when releasing the last initialized instance; otherwise resolves as soon as this instance is released. Documentation/JSDoc only.

### #6 (Low) — native stream rejection of `undefined` is misreported as an empty response

**Where:** `native-lib/node/src/stream.ts` (~39 `let startError: unknown;`, ~54–57 the two-arg `.then`, ~74 `if (startError !== undefined) throw startError;`).

**Defect:** `startError !== undefined` is the rejection sentinel. `Promise.reject(undefined)` is valid JS, so a native `start()` that rejects with literal `undefined` is indistinguishable from "never rejected" — the generator swallows it and returns the normal empty-metadata result instead of throwing. Previously triaged as an unreachable non-blocking Minor; flagged again in review #7, so close it properly.

**Fix:** Replace the value sentinel with a dedicated `let startRejected = false;` boolean, set to `true` in the rejection handler (alongside recording `startError`), and gate the re-throw on `if (startRejected) throw startError;`. This tracks rejection by settlement state, not by the rejected value, so `Promise.reject(undefined)` propagates correctly. The chunk-draining/wake logic is unchanged.

### #7 (Low) — test cleanup can hide a regression when the test body otherwise passes

**Where:** `native-lib/node/tests/integration/worker-lifecycle.test.ts` balancing `finally` (~282–291) of the "inits once + creates N engines + exits without cleanup" test.

**Defect:** The `finally` swallows `destroyEngine()`/`ffi.cleanup()` failures unconditionally (`catch { }`). Suppression is correct only to avoid masking an already-propagating body failure; if the body **succeeded**, a cleanup failure (a real lifecycle regression) is silently discarded and the test still passes.

**Fix:** Track whether the try body completed successfully (e.g. set `bodySucceeded = true` as the last statement inside `try`, after the final assertion). In the `finally`'s catch, `throw` the cleanup error when `bodySucceeded` is true (surface the regression); only suppress when the body was already failing (a body throw means `bodySucceeded` stayed false and the original error is already propagating). Preserve the round-12/round-6 property that the survival assertions stay inside `try` and best-effort balancing still runs.

### #8 (Medium, NOT actioned) — PR scope includes Python-binding modernization

**Decision:** Kept as the standing "leave-as-is, note in PR" decision. No git surgery to split the Python work this round. Answered to the reviewer in the PR: the Python modernization split is acknowledged and deferred to its own follow-up PR; doing the split now would rewrite history mid-review-cycle. This matches the decision recorded in every prior round.

## Rejected alternatives

- **Make `initialize()`/`run()` async to await the #3 rollback.** API break — `run()` returns `ExecutionResult`, `initialize()` returns `void`. The pending-state model (reusing `cleanupPromise` + the `"cleaning-up"` guard) gives observability and re-init gating without changing the sync signatures. Same reasoning as the round-5 deadlock fix.
- **#1: detach on every path (including success).** Wrong — on a successful teardown the isolate is destroyed; `fn_detach_thread` against a torn-down isolate is a use-after-free. Detach only on the nonzero-return failure branch, where the isolate is provably still live.
- **#2: block/spin in the rollback path until teardown succeeds.** Reintroduces a potential hang on the init thread. Arming `g_teardown_needed` and letting the existing top-of-`napi_initialize` retry reclaim it is the established, bounded pattern.
- **#6: keep the value sentinel but special-case `undefined`.** Fragile; a dedicated boolean is the direct fix and matches "track rejection with a separate settlement flag" from the review.

## Verification

- `cd native-lib/node && npm run build:addon` clean (no new warnings in the touched C regions); `npm run build` (tsc) clean.
- `npm test` green. Current baseline on the rebased tree: **26 files, 943 passed / 32 skipped / 0 failed** (includes master's rebased-in TCK infrastructure). Doc-only findings (#4, #5) add no tests; #3, #6, #7 each may add/adjust a targeted regression. Target: **0 failures**, with the new/adjusted regressions passing and the full 729-case TCK conformance run still 0-failed.
- Whole-branch final review on the most capable model, tracing the #1/#2 native-lifecycle changes against the teardown state machine and retry-signal invariant.
