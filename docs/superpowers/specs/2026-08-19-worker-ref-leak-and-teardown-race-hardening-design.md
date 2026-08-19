# Worker Ref-Leak & Teardown-Race Hardening — Round 12 (W-23692110)

**Status:** Design approved, ready for planning.

**Source review:** `docs/pr-157-follow-up-code-review-3.md` (9 findings), verified against live source at commit `e1b9ee0` (round-12 tip; round-11 code + the #7 doc fix). Two findings are already resolved and are out of scope for the implementation round below:

- **#7 (docs)** — the two `cleanup()` README bugs (false "fatal signals" claim; over-broad "drains anywhere in the process" claim) are fixed in `e1b9ee0`.
- **#1 (dwlib C ABI break)** — factual and by design. The project is **pre-GA**; the multi-engine redesign intentionally replaces the `run_script_*_with_resolver` exports with the `*_engine` entrypoints and adds `ctx` to `ResolveModuleCallback`. No compatibility shims, no major-version ceremony required at this stage. **Decision (user): OK, not addressed.** No code change.

**Scope:** `native-lib/node` only — `src/addon.c`, `src/dataweave.ts`, and Node integration tests under `tests/`. Do **not** touch `native-lib/python/**`, the legacy singleton entrypoints (`run_script`, `run_script_callback`, `run_script_input_output_callback`), `dw_napi_run_script`, or `ScriptRuntime.getInstance()`. The Java side (`NativeLib.java`, `ScriptRuntime`) is read for context but not modified.

## Problem

Round 11 gave every engine an env cleanup hook so an abandoned Worker's env teardown reclaims the engine record and Java registry entry. A follow-up review found that reclamation is **incomplete** (the init reference leaks — #2) and that the deferred finalize path it relies on has a **teardown race** (#3), plus three medium code issues (#4, #5, #6) and two test-coverage gaps (#8, #9). All seven are verified real against live source.

### #2 (High) — Abandoned-env teardown leaks the initialization reference

Every `DataWeave` instance calls `ffi.initialize()` on construction (`dataweave.ts:87`), which does `g_ref_count++` (`addon.c:506`; also the fast-path `:477` and the adoption path `:463`). The only `g_ref_count--` is in `napi_cleanup` (`addon.c:2153`), reached from JS via `ffi.cleanup()`. When a Worker (or the main env) terminates **without** calling `cleanup()`, the env cleanup hook `bridge_env_cleanup` → `bridge_finalize` (`addon.c:252-293`) frees the engine record, deletes the napi_ref, and removes the Java registry entry — but never decrements `g_ref_count`. So the shared isolate's reference count never returns to zero and the isolate is never torn down. Repeated Worker create/terminate cycles without explicit `cleanup()` keep the isolate alive indefinitely. This directly contradicts the round-11 comment at `addon.c:1686` claiming the hook prevents leaking "the native-lib reference."

### #3 (High) — Deferred registry removal attaches to an isolate that teardown may be destroying

`bridge_finalize` (`addon.c:224-243`) reads `g_isolate` **without `g_mutex`** and calls `fn_attach_thread(g_isolate, &thread)` then `fn_destroy_engine(thread, …)` to remove the Java registry entry. The streaming/transform worker threads release their `g_active_ops` reservation (`addon.c:745-748` for streaming; the transform analogue) **before** the completion sentinel runs `bridge_end_op` → `bridge_finalize`. Once `g_active_ops` reaches 0, the `teardown_waiter_thread_fn` is free to begin `graal_tear_down_isolate()`. So the sequence

1. worker releases `g_active_ops` (now 0),
2. waiter wakes, transitions `TEARING_DOWN`, calls `graal_tear_down_isolate()`,
3. sentinel's `bridge_finalize` reads `g_isolate` (passes the NULL check because step 2's clear hasn't landed / is racing) and calls `fn_attach_thread` on an isolate being destroyed

is possible. This is **both** a C data race on `g_isolate` (lock-free read racing a write under lock) **and** an attach-vs-teardown TOCTOU. The round-11 whole-branch review adjudicated the *spawn-failure* variant benign because it runs with the reservation still held / isolate guaranteed alive; the **deferred-finalize** variant is not benign because it can run after `g_active_ops` is already 0.

### #4 (Medium) — `runTransform` can dispatch on an engine cleaned up during input pre-buffering

`runTransform` (`dataweave.ts:221-248`) calls `ensureReady()` (`:226`), then `await createChunkReader(input)` (`:234`) — a suspension point that, for async input, can take arbitrary time — then dispatches with `this.engineHandle!` (`:238`). A caller can start the transform, `cleanup()` the instance while the reader is pre-buffering, then resume into a dispatch with a cleared/destroyed handle. The round-11 C admission pin makes this **memory-safe** (worst case is a resolved `Unknown engine handle` envelope, not a UAF), but the readiness check is stale by the time of dispatch.

### #5 (Medium) — Module-level `cleanup()` does not coalesce overlapping calls

The module-level `cleanup()` (`dataweave.ts:371-383`) nulls `globalInstance` **synchronously** before awaiting `instance.cleanup()`. A second overlapping call sees `globalInstance === null` and resolves immediately, even though the first call's native teardown is still draining. The instance-level `cleanup()` correctly coalesces via `this.cleanupPromise` (`:131`); the module wrapper does not, so its contract ("resolves once native teardown has finished") is violated for the second caller.

### #6 (Medium) — Ignored `napi_add_env_cleanup_hook` status leaks a returned handle

`napi_create_engine` (`addon.c:1692`) and `napi_create_engine_with_resolver` (`addon.c:1743`) ignore the return status of `napi_add_env_cleanup_hook`. If registration fails, the function still returns a usable handle, but the engine now has **no** env cleanup hook, so an abandoned Worker permanently strands its engine record, Java registry entry, and (per #2) init reference. Engine creation is not all-or-nothing.

### #8 (Medium) — The run-vs-destroy test cannot prove the pin guarantee

`engine-handle-contract.test.ts:177-231` fires `destroyEngine()` on the same JS thread **after** admission, then accepts *either* success *or* the `Unknown engine handle` envelope. On fixed code the pin was already acquired at admission, so this ordering must deterministically succeed; accepting the error envelope means a regression that removes the pin still passes the test. The assertion is too weak to detect the very regression it exists to guard.

### #9 (Medium) — No Worker integration coverage for the documented per-Worker model

The README (`README.md:445-456`) instructs users to construct a separate resolver-backed `DataWeave` instance per Worker, but no test creates a `worker_threads` Worker. There is no coverage for resolver-backed/resolver-less engines inside a Worker, normal Worker exit without `cleanup()` (the #2 scenario), `Worker.terminate()`, independent module resolution, or subsequent main-thread initialization.

## Design

The two correctness fixes (#2, #3) share the teardown-coordination trio `g_ref_count` / `g_active_ops` / the lock-free `g_isolate` read. Per the approved approach, the fix is **robust but bounded**: close the race for real and track the init reference properly, using **targeted consolidation of only the ref-release/finalize step** where sharing is warranted — without re-opening the broader coordination substructure (the round-5 `TEARDOWN_*` state machine + adoption path, the round-9/10 deferred-removal logic) that took six rounds to stabilize.

### 1. Release the init reference on abandoned-env teardown (#2)

**New helper — `release_isolate_ref_locked()`** (caller holds `g_mutex`). It carries the exact "one initialization reference is going away" logic that `napi_cleanup` Case 5 already implements: decrement `g_ref_count`; if it reaches 0, drive the **existing** teardown decision (immediate teardown when `g_active_ops == 0`, or queue the `teardown_waiter` when `g_active_ops > 0`, setting `TEARDOWN_PENDING_WAIT`). This is *targeted* consolidation — only the decrement-and-maybe-teardown step, not the surrounding machinery. `napi_cleanup` is refactored to call it (behavior-preserving); the env-cleanup path calls it too.

**Env-cleanup path releases the ref.** `bridge_env_cleanup` reclaims an abandoned env's engine. Because that env's `initialize()` did one `g_ref_count++` per engine it created, the reclamation must do one matching release per engine:

- In `bridge_env_cleanup`'s **direct finalize** path (`in_flight == 0`, `addon.c:279-293`): after finalizing the record, call `release_isolate_ref_locked()` once, under `g_mutex`.
- In its **deferred-drain** path (`in_flight > 0`, marks `destroy_pending`/`deferred_registry_remove`, `addon.c:269-278`): the last op to drain (`bridge_end_op` → finalize) must perform the release. Thread a flag on the record — `deferred_ref_release` — set alongside `deferred_registry_remove` in the env-cleanup deferral, so `bridge_end_op` knows to release the ref exactly once when it finalizes. (The `destroyEngine` deferral does **not** set it — that path is paired with an explicit `ffi.cleanup()` in JS and must not double-release.)

**Ownership rule (the invariant):** exactly one `g_ref_count` release per `initialize()`. `napi_cleanup` releases for instances torn down via explicit JS `cleanup()`; the env-cleanup path releases for instances abandoned by a terminating env. `destroyEngine` never releases (its JS caller always follows with `ffi.cleanup()`). These are mutually exclusive per engine because `destroyEngine` removes the env hook (so an engine reclaimed by the hook was never explicitly destroyed) and JS `cleanup()` calls `destroyEngine` then `ffi.cleanup()` on the *live* env (so the hook never fires for it).

This makes the round-11 comment at `addon.c:1686` accurate. Update that comment to state the hook now also releases the init reference.

### 2. Perform the isolate-touching finalize under the held admission reservation (#3)

`g_active_ops > 0` is the exact invariant that keeps the isolate alive (the waiter blocks on `while (g_active_ops > 0)`). The fix makes the deferred registry-removal attach happen **while the op's own reservation still holds**, so the isolate provably cannot begin teardown during the attach.

**Split `bridge_finalize` into two phases:**

- `bridge_finalize_registry(b)` — the isolate-touching phase: `fn_attach_thread(g_isolate, …)` + `fn_destroy_engine(thread, b->handle)` + `fn_detach_thread`. Precondition: **called only while the caller holds a live `g_active_ops` reservation** (or otherwise guarantees the isolate is alive — e.g. the JS-thread destroyEngine paths where `g_isolate` is stable). Retains the `g_isolate != NULL` guard for the documented main-env-after-isolate-teardown corner, but that read is now inside the reservation window, so it is no longer racing a concurrent teardown.
- `bridge_finalize_free(b, env_still_alive)` — the non-isolate phase: napi_ref deletion (owner JS thread, env alive; stays resolver-gated) + `resolver_results_free_all` + `free(b)`. Touches no GraalVM isolate state.

**Reorder the streaming/transform completion sentinels** so the sequence on the worker/owner completion path is:

1. `bridge_end_op` decides finalize/registry-removal under `g_mutex` (unchanged decision logic),
2. if registry removal is due: `bridge_finalize_registry(b)` **while `g_active_ops` for this op is still held**,
3. release `g_active_ops` (the exact verbatim pattern),
4. `bridge_finalize_free(b, env_still_alive)`.

i.e. move the `g_active_ops--` release to sit **between** the registry-removal attach and the record-free, instead of before both. The napi_ref deletion and record-free never touch the isolate, so doing them after the release is safe; the attach never happens outside the reservation window, so `g_isolate` is never read while a teardown could be destroying it.

**Deadlock-safety (the load-bearing review gate):** holding `g_active_ops` across `bridge_finalize_registry` must not re-introduce the round-5 deadlock. The round-5 deadlock was: a *blocking wait on the JS event loop* while an op needed that loop. `bridge_finalize_registry` attaches its **own** Graal thread and makes **no** env-affine N-API call and **no** wait on the JS loop — it cannot depend on the event loop turning, so holding the reservation across it cannot wedge the waiter. This must be explicitly confirmed in review.

**Preserves round-5's decrement-on-worker-thread reasoning:** the `g_active_ops--` stays on the worker/completion thread (not moved back to a JS callback); we only move *where within that completion path* it sits relative to the registry-removal attach.

### 3. `runTransform` readiness re-check after pre-buffering (#4)

In `runTransform` (`dataweave.ts`), call `this.ensureReady()` again immediately after `await createChunkReader(input)`, before `streamFromNative(...)`. If the instance was cleaned up during the await, the caller gets a synchronous `DataWeaveError` (the same error `ensureReady` throws elsewhere) instead of a resolved `Unknown engine handle` envelope. No lease is introduced — the authoritative guard is the C admission pin (round 11 #2/#3); this only improves the failure ergonomics for a misused instance. The first `ensureReady()` at the top stays (fail fast before pre-buffering when already not-ready).

### 4. Module-level `cleanup()` coalescing (#5)

Add a module-scoped `cleanupPromise: Promise<void> | null`. The module `cleanup()` becomes: if `cleanupPromise` is set, return it; else if `globalInstance` is null, return; else capture the instance, null `globalInstance`, store `cleanupPromise = instance.cleanup()`, `await` it in a `try`, and clear `cleanupPromise` in `finally`. Overlapping callers all await the same promise and resolve only when the underlying native teardown finishes — matching the instance-level coalescing pattern. The `cleanupStarted` exit-hook coalescer is unchanged (it coalesces `beforeExit`/`exit` for a shutdown; this coalesces overlapping manual calls). Keep the `cleanupStarted = false` reset last, as today.

### 5. Check `napi_add_env_cleanup_hook` status; make creation all-or-nothing (#6)

In both `napi_create_engine` and `napi_create_engine_with_resolver`, capture the `napi_status` from `napi_add_env_cleanup_hook`. On non-`napi_ok`:

- unlink the just-linked record from `g_bridges` (under `g_mutex`),
- `bridge_finalize_registry(record)` to remove the Java registry entry (the engine was just created on this same live thread; the isolate is alive and `g_active_ops` need not be held because we are on the creating JS thread before returning — `g_isolate` is stable here, the same condition the existing destroyEngine fallback relies on),
- `release_isolate_ref_locked()` to release this creation's init reference (this instance's `initialize()` bumped it),
- `bridge_finalize_free(record, /*env_still_alive=*/true)`,
- `napi_throw_error` and return NULL — no usable handle escapes.

Because the record was just constructed and linked on this thread and no op could have been admitted against it yet (`in_flight == 0`, no concurrent admission — the JS wrapper hasn't returned the handle), the unlink-and-finalize is race-free.

### 6. Strengthen the run-vs-destroy test (#8)

In `engine-handle-contract.test.ts`, for the **admitted ordering** (destroy fired after the streaming/transform op is admitted), require **success + complete chunks** — remove the "or Unknown engine handle" acceptance for that specific ordering. On fixed code the pin is already held at admission, so success is guaranteed; a regression that drops the pin would now produce the error envelope and **fail** the test. Keep any genuinely-unforceable cross-thread interleaving as a separately-labeled best-effort probe.

### 7. Worker integration tests (#9)

Create `native-lib/node/tests/integration/worker-lifecycle.test.ts` using real `worker_threads` Workers loading the real compiled addon. Coverage:

- **Resolver-backed engine in a Worker:** create, run a script that resolves a custom module via the Worker's `resolveModule`, assert correct output — proving per-Worker resolver binding.
- **Resolver-less engine in a Worker:** create, run, assert output.
- **Normal Worker exit without `cleanup()` (the #2 proof):** run N cycles of {spawn Worker → create engine → run → let the Worker exit without `cleanup()`}, then assert the main thread can still `initialize()` and run, and that the process is not wedged. This is the behavioral observation of the #2 ref release (pre-fix, the leaked ref would keep the isolate alive; the test asserts continued healthy operation and clean final teardown).
- **`Worker.terminate()` mid-life** then subsequent main-thread `initialize()`/run succeeds.
- **Explicit `cleanup()` inside a Worker** resolves and leaves the main thread healthy.

**Shared-state discipline:** these Worker tests share the parent process's isolate. Each Worker's own engine lifecycle must be balanced, and the file must end with a final main-thread `cleanup()` so it doesn't perturb sibling integration files — the same discipline `independent-engines.test.ts` follows. Vitest `pool: "forks"` isolates per file, so the file's residual state does not leak across files, but within-file balance still matters for the assertions.

**Determinism posture (stated in the test file):** exact cross-thread timing interleavings (#3's race) are **not** deterministically forceable — matching the rounds 5–11 posture. The deterministic teeth are #8's required-success admitted-ordering assertion and #2's "Worker exits → main thread still works + final teardown clean" assertion. #3's correctness rests on the code reasoning in Design §2 (the reservation window), with the Worker tests as best-effort probabilistic guards over N iterations that are green on fixed code and cannot false-fail on it.

## Testing

- Strengthened `engine-handle-contract.test.ts` admitted-ordering assertion (#8).
- New `worker-lifecycle.test.ts` (#9), doubling as behavioral coverage for #2 (and best-effort for #3).
- Unit coverage for the module-level `cleanup()` coalescing (#5): two overlapping `cleanup()` calls both await the same drain and neither resolves before native teardown completes.
- Unit coverage for `runTransform` re-check (#4): consuming a transform generator after the instance was cleaned up during the input await surfaces a `DataWeaveError` synchronously at resume, not a resolved error envelope.
- No Java test change (the `@CEntryPoint` hosted-JVM limitation is unchanged; coverage stays at the Node integration layer).
- #2/#3 lifecycle correctness that is not deterministically forceable is covered by code reasoning against the invariants in §Design plus the best-effort Worker guards (same documented posture as rounds 5–11).

## Verification

- `cd native-lib/node && npm run build:addon` clean (no new warnings in the touched regions: `bridge_finalize*`, `bridge_env_cleanup`, `bridge_end_op`, `napi_cleanup`, the two creators, the streaming/transform completion sentinels); `npm run build` (tsc) clean.
- `npm test` green at the new baseline (currently 885 passed / 59 skipped / 0 failed; this round adds the #5, #4, #8 assertions and the #9 Worker suite — the plan sets the exact new counts).
- `git diff --check`.

## Global Constraints

- Node-binding-only. Never touch `native-lib/python/**`.
- Never touch the legacy singleton entrypoints (`run_script`, `run_script_callback`, `run_script_input_output_callback`), `dw_napi_run_script`, or `ScriptRuntime.getInstance()`. The Java side is not modified.
- Handle width stays C `long long` everywhere.
- Errors for run/streaming/transform APIs surface as **resolved** JSON string values (async) or a synchronous `napi_throw_error` (admission/arg/alloc/resource failures) — never `napi_reject_deferred`.
- `napi_env`/`napi_ref`/`napi_deferred`/`napi_threadsafe_function` are thread-affine to the env's JS thread. No env-affine napi call may be made off the owning thread. `bridge_finalize_free`'s napi_ref deletion stays resolver-gated (`resolver_js != NULL && env != NULL`) and on the owner thread.
- All shared C state (`g_initialized`, `g_active_ops`, `g_teardown_state`, `g_teardown_cancelled`, `g_ref_count`, every engine record's `in_flight`/`destroy_pending`/`deferred_registry_remove`/the new `deferred_ref_release`) is read/written only under `g_mutex`, **except** the `g_isolate` read inside `bridge_finalize_registry`, which is now only reached while a live `g_active_ops` reservation (or the creating JS thread's stable-isolate guarantee) holds the isolate alive — closing the round-12 #3 race.
- The `g_active_ops` release pattern is EXACTLY, verbatim: `uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);`
- **Exactly one `g_ref_count` release per `initialize()`** (the #2 invariant): `napi_cleanup` for explicitly-cleaned instances; the env-cleanup path for abandoned envs; `destroyEngine` never releases. Mutually exclusive per engine.
- `fn_destroy_engine` is called **exactly once** per handle.
- Per-engine `in_flight` and global `g_active_ops` stay **distinct** counters — not merged.
- The round-5 deadlock fix must be preserved: `g_active_ops--` stays on the worker/completion thread, never moved to a JS-thread callback; and no blocking wait on the JS event loop is introduced. Holding `g_active_ops` across `bridge_finalize_registry` is safe only because that phase makes no env-affine/JS-loop-dependent call — confirm in review.
- Preserve every round-1..11 fix: coalesced instance `cleanup()`, the JS three-state lifecycle machine, the `TEARDOWN_*` state machine + `napi_initialize` adoption path (incl. round-7's `g_teardown_cancelled` carve-out), the worker-thread `g_active_ops` decrement, the atomic admission blocks + round-11 admission-time engine pin (`bridge_begin_op_locked`) in all three run paths, the round-6 handle-read validations, round-7 conversion-status checks, round-8 setup-allocation NULL checks, round-9 worker/callback OOM + N-API-create checks + deferred registry removal, round-10 env-cleanup registry removal + `g_isolate`-guarded finalize, round-11 env cleanup hook for every engine + owner-thread destroy guard for every record + register-once exit hooks.
- Node vitest baseline currently **885 passed / 59 skipped / 0 failed**; this round raises it (new tests) and must stay green.

## Rejected Alternatives

- **#2 by decrementing `g_ref_count` inline in `bridge_finalize` without the shared helper.** Rejected: the "reached zero → immediate teardown vs. queue the waiter" decision already lives in `napi_cleanup` Case 5; duplicating it invites divergence. A single `release_isolate_ref_locked()` keeps both paths identical.
- **#2 by having `destroyEngine` also release the ref.** Rejected: `destroyEngine`'s JS caller (`doCleanup`) always follows with `ffi.cleanup()`, which releases the ref; adding a release in `destroyEngine` would double-release and tear the isolate down under live instances.
- **#3 by taking `g_mutex` around the `g_isolate` read + attach in `bridge_finalize`.** Rejected: `fn_attach_thread`/`fn_destroy_engine` enter GraalVM and can block; holding `g_mutex` across them would serialize all teardown coordination behind a GraalVM call and risk lock-ordering issues with the waiter. Gating on the already-held `g_active_ops` reservation is the correct, lock-free-read-safe closure.
- **#3 with a dedicated `g_finalizing` counter separate from `g_active_ops`.** Rejected as re-opening the coordination substructure the approved approach keeps bounded: it adds a second teardown-gating counter that the waiter must also wait on, duplicating what `g_active_ops` already expresses. Reusing the op's existing reservation window is simpler and provably correct.
- **#4 via a JS-side operation lease that blocks `cleanup()` until the transform completes.** Rejected (same as rounds 9/11): no per-engine "await my ops" primitive exists at the JS layer, and the authoritative pin already lives in C. The re-check is the minimal ergonomic close; the lease would duplicate the C pin's guarantee at a layer that cannot enforce it.
- **#5 by not nulling `globalInstance` until the drain settles.** Rejected: a concurrent convenience-API call would then revive/return the instance mid-teardown. Nulling synchronously (so new work builds a fresh instance) plus a module `cleanupPromise` (so overlapping `cleanup()`s coalesce) matches the instance-level design and is correct.
- **#6 by leaving the handle valid and logging on hook-registration failure.** Rejected: a handle with no env cleanup hook silently reintroduces exactly the #2 leak the round is closing. Creation must be all-or-nothing.
- **#8 keeping the "success OR error envelope" acceptance for the admitted ordering.** Rejected: that acceptance is precisely what lets a pin regression pass. The admitted ordering is deterministic on correct code, so the test must require success.
- **#9 driving the "cross-thread" scenario on a single JS thread only.** Rejected as insufficient for the documented per-Worker model: real `worker_threads` Workers are needed to exercise per-Worker engine binding and the abandoned-env (#2) path. The exact race remains best-effort, but the Worker lifecycle itself must be really exercised.
- **Modifying the Java `ScriptRuntime` to reference-count or tolerate late lookups.** Rejected as out of scope and the wrong layer — the C addon owns the isolate reference and the pin.
