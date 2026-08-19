# Engine-Pin & All-Engines-Cleanup Hardening — Round 11 (W-23692110)

**Status:** Design approved, ready for planning.

**Source reviews:** `docs/pr-157-follow-up-andy-code-review-11.md` (2 findings) and `docs/pr-157-follow-up-code-review-2.md` (6 findings). All overlapping; deduplicated into 6 work items below. Verified against live source at commit `50b2930` (round-10 tip).

**Scope:** `native-lib/node` only — `src/addon.c`, `src/dataweave.ts`, and Node integration tests under `tests/`. Do **not** touch `native-lib/python/**`, the legacy singleton entrypoints (`run_script`, `run_script_callback`, `run_script_input_output_callback`), `dw_napi_run_script`, or `ScriptRuntime.getInstance()`. The Java side (`NativeLib.java`, `ScriptRuntime`) is read for context but not modified.

## Problem

The 11th "andy" review and a second general code review together raise 7 findings; 6 are real and one (the C ABI break) is a documented-by-design decision, not a code change.

### #1 (P1) — Resolver-less engines leak on Worker exit (no env cleanup hook)

`napi_create_engine` (resolver-less, `addon.c:1644`) links a per-engine record into `g_bridges` but registers **no** `napi_add_env_cleanup_hook`; only `napi_create_engine_with_resolver` does (`addon.c:1695`). A Worker (or the main thread) that creates a resolver-less `DataWeave` instance and terminates without calling `destroyEngine()` strands: the native `engine_bridge_t` record, the Java `ScriptRuntime` registry entry, and the native-library reference (`g_ref_count` never decremented for that instance). Repeated Worker create/terminate cycles leak engines and prevent isolate teardown.

### #2 (P1) — Streaming/transform admission reserves the isolate before pinning the engine

`napi_run_script_streaming_engine` reserves `g_active_ops++` at `addon.c:841` but does not pin the engine (`bridge_begin_op`) until `addon.c:925` — a wide window (arg extraction, `w`/tsfn/promise allocation) in which a concurrent Worker's `destroyEngine(handle)` observes `in_flight == 0`, unlinks and frees the bridge, and removes the Java registry entry. The already-admitted op then spawns its worker with `w->bridge` pointing at freed memory (or NULL after the fact) and can fail with "Unknown engine handle" or dereference the freed bridge in `resolve_module_callback`. `napi_run_script_transform_engine` has the identical shape (`g_active_ops++` at `addon.c:1324`, `bridge_begin_op` at `addon.c:1429`).

### #3 (P1) — Synchronous `runScriptEngine` never pins the engine at all

`napi_run_script_engine` (`addon.c:1791-1879`) increments `g_active_ops` (`:1847`) to protect the isolate but never calls `bridge_begin_op`. A concurrent Worker can `destroyEngine(handle)` while this synchronous call is attaching to Graal or executing `fn_run_script_engine` (`:1858`); for a resolver-backed engine that frees the bridge Java still holds as the resolver ctx → `resolve_module_callback` dereferences freed memory. `g_active_ops` gates only the *global isolate*, not the *per-engine* record.

### #4 (documented, not a code change) — dwlib C ABI break

This branch removes the exported `run_script_with_resolver` / `run_script_callback_with_resolver` / `run_script_input_output_callback_with_resolver` entrypoints (present on master) and replaces them with `create_engine` / `create_engine_with_resolver` / `destroy_engine` / `run_script_engine` / `run_script_callback_engine` / `run_script_input_output_callback_engine`, and inserts a `ctx` parameter into the `ResolveModuleCallback` signature. The three legacy singleton entrypoints (`run_script`, `run_script_callback`, `run_script_input_output_callback`) are preserved. This is the intended multi-engine redesign; dwlib is consumed by this repo's own Python and Node bindings in lockstep. **Decision (user):** document the break in the PR/spec; do NOT add compatibility shims. No code change in this round.

### #5 (Medium) — Process exit listeners accumulate across singleton re-creation

`getGlobalInstance` (`dataweave.ts:289-304`) attaches a `beforeExit` and an `exit` listener every time it (re)creates `globalInstance`; the module-level `cleanup()` (`:341-353`) nulls the singleton but never removes those listeners. Repeated init→cleanup→reinit cycles accumulate two listeners per cycle and eventually emit Node's `MaxListenersExceededWarning`.

### #6 (Medium) — Unknown-handle coverage does not exercise the native entrypoints

`ScriptRuntimeTest.unknownEngineHandleProducesExactErrorJson` (`ScriptRuntimeTest.java:677-683`) only asserts on the `UNKNOWN_ENGINE_HANDLE_JSON` constant and `ScriptRuntime.get`; it deliberately cannot invoke the `@CEntryPoint` methods (GraalVM word types don't box in a hosted JVM). So no test drives the `*_engine` entrypoints against unknown/destroyed handles through the real addon, nor exercises the cross-Worker run-vs-destroy race in #2/#3.

## Design

### 1. Register an env cleanup hook for every engine + extend the owner-thread destroy guard (finding #1)

**Cleanup hook for all engines.** In `napi_create_engine`, store `rec->env = env` and register `napi_add_env_cleanup_hook(env, bridge_env_cleanup, rec)` — exactly as `napi_create_engine_with_resolver` already does. `bridge_env_cleanup` and `bridge_finalize` already handle a resolver-less record correctly: `resolver_js == NULL` → skip `napi_delete_reference`, still unlink from `g_bridges`, remove the Java registry entry (round-10 `do_registry_remove=true`), and free the record. So the round-10 registry-removal path now also reclaims resolver-less engines abandoned by a terminating env. `rec->owner` is already recorded (`addon.c:1643`).

**Owner-thread destroy guard extends to all engines (approved contract change).** Registering a cleanup hook gives every engine env-affine state: the hook is bound to its creating env, and `napi_remove_env_cleanup_hook` (called by `destroyEngine` before an early free, `addon.c:1738`) is only valid on that owner env/thread. Today the cross-thread guard in `napi_destroy_engine` (`addon.c:1703`) fires only when `owned->resolver_js != NULL`. Change it to fire for **any** record (`owned != NULL`), so a resolver-less engine is also only destroyable from its creating thread.

- **Why this is safe:** every JS `DataWeave` instance is constructed and destroyed on a single thread (its owning env), so the guard never rejects a legitimate call. This reverses the round-9 invariant "resolver-less engines remain destroyable from any thread," which was only ever exercised by the (now-closed) case of a resolver-less engine having no env-affine state.
- **Why the alternative is worse:** leaving the guard resolver-only while registering a hook means a cross-thread `destroyEngine` would either skip `napi_remove_env_cleanup_hook` (leaving Node holding a hook pointing at a freed record → UAF at env teardown) or call it cross-thread (undefined behavior). Extending the guard is the correct closure.

Update the guard's comment block (`addon.c:1683-1700`) to state the guard now keys on "a record exists" because every engine carries an env cleanup hook, not just resolver `napi_ref` state.

**`bridge_finalize` napi_ref deletion stays resolver-gated** (`addon.c:237`: `resolver_js != NULL && env != NULL`) — a resolver-less record has no ref to delete; only the hook registration and the owner guard change.

### 2. Fold engine lookup + `in_flight++` into the locked admission transaction (findings #2, #3)

Introduce a locked-admission variant so the per-engine pin happens in the **same** critical section as the `g_active_ops` reservation and lifecycle check, before any window a concurrent `destroyEngine` could use.

**New helper** (`addon.c`, near `bridge_begin_op`):
```c
// Increment this engine's in_flight while g_mutex is ALREADY held (admission
// transaction). Caller must hold g_mutex. Returns the record (NULL if unknown
// handle -- nothing to pin, worker will surface "Unknown engine handle").
static engine_bridge_t* bridge_begin_op_locked(long long handle) {
    engine_bridge_t* b = bridge_find(handle);
    if (b != NULL) b->in_flight++;
    return b;
}
```
`bridge_begin_op` stays for callers that need the self-locking form; internally it becomes `lock; b = bridge_begin_op_locked(handle); unlock; return b;`.

**Streaming / transform:** in the admission critical section (`addon.c:835-842` / `1318-1325`), after `g_active_ops++`, also call `w->bridge = bridge_begin_op_locked(handle64)` **before** unlocking, and delete the later standalone `bridge_begin_op` call (`:925` / `:1429`). Every existing failure path between admission and the worker spawn (conversion errors, OOM, tsfn/promise creation failures, `spawn_rc != 0`) must now **also** release the pin. Because those paths currently only do the `g_active_ops--` release, each must additionally call `bridge_end_op(w->bridge, /*env_still_alive=*/true)` (the env is live on the JS admission thread) to balance `in_flight` and finalize if a concurrent destroy is now pending. The completion sentinel path is unchanged — it already calls `bridge_end_op`.

- **Ordering:** with the pin taken under the same lock as the admission check, a concurrent `destroyEngine` either runs entirely before admission (then `bridge_find` in admission returns the record only if not yet destroyed; if already destroyed, the record is gone and the worker surfaces "Unknown engine handle" — no freed access) or entirely after (then `in_flight > 0`, so destroy defers per round-9/10). There is no interleaving where an admitted op observes a freed bridge.
- **Unwind completeness:** the plan must enumerate every early-return between the locked admission and the spawn and add the `bridge_end_op` release, mirroring how each already releases `g_active_ops`. A pin leaked here would wedge `destroyEngine` (never drains) exactly like a leaked `g_active_ops` wedges teardown.

**Synchronous `runScriptEngine`:** pin the engine for the isolate-touching window. Because this path reserves `g_active_ops` *late* (`addon.c:1840-1848`, after arg extraction), take the pin in that same critical section:
```c
uv_mutex_lock(&g_mutex);
if (!g_initialized || (g_teardown_state != TEARDOWN_NONE && !g_teardown_cancelled)) { ... release, throw ... }
g_active_ops++;
engine_bridge_t* bridge = bridge_begin_op_locked(handle);
uv_mutex_unlock(&g_mutex);
```
Then release the pin in **both** the attach-failure path and normal completion, alongside the existing `g_active_ops--`. The current post-run `bridge_find` + `resolver_results_free_all` (`addon.c:1860-1863`) uses the pinned `bridge` directly (no second lookup needed; the pin kept it alive). Release ordering at completion: after `resolver_results_free_all` and detach, call `bridge_end_op(bridge, /*env_still_alive=*/true)` — which may finalize a deferred destroy — then the existing `g_active_ops--` broadcast. `bridge_end_op` handles `NULL` (unknown handle) as a no-op.

- **Sync-path note:** unlike streaming/transform there is no background thread, so `env_still_alive` is always true here (the JS thread runs the whole op). An unknown handle (`bridge == NULL`) still runs `fn_run_script_engine`, which returns the resolved "Unknown engine handle" JSON — behavior unchanged.

### 3. Register process exit listeners exactly once (finding #5)

Move the `beforeExit`/`exit` registration out of `getGlobalInstance` so it runs once per module, guarded by a module-scoped `let exitHooksRegistered = false` that is **never reset** (unlike `cleanupStarted`). The listeners already tolerate a null `globalInstance`: `cleanup()` no-ops when `globalInstance` is null, and `cleanupStarted` still coalesces `beforeExit`/`exit` for a given shutdown. So one registration covers every current and future revived singleton, and init→cleanup→reinit cycles no longer accumulate listeners.

```ts
let exitHooksRegistered = false;
function registerExitHooksOnce(): void {
  if (exitHooksRegistered) return;
  exitHooksRegistered = true;
  process.on("beforeExit", async () => { if (cleanupStarted) return; cleanupStarted = true; await cleanup(); });
  process.on("exit", () => { if (cleanupStarted) return; cleanup(); });
}
```
`getGlobalInstance` calls `registerExitHooksOnce()` after `globalInstance.initialize()`. Update the doc comment (`dataweave.ts:267-287`) to say the hooks are registered once for the process, not per singleton.

### 4. Real *_engine unknown/destroyed-handle + run-vs-destroy tests (finding #6)

Add **Node integration tests** (real addon, `vi.mock` of `ffi` is forbidden — mirror `tests/integration/independent-engines.test.ts`):

- **Unknown / destroyed handle envelope:** for each of `runScriptEngine` (sync), `runScriptStreamingEngine`, `runScriptTransformEngine`, invoke against (a) a never-registered handle and (b) a handle whose engine was `destroyEngine`'d, and assert the result is the terminal `{"success":false,"error":"Unknown engine handle"}` envelope (resolved, not thrown for the async ops; the sync op returns the JSON string) and that the process does not crash and no C string leaks (the op resolves/returns cleanly).
- **Cross-Worker run-vs-destroy (findings #2/#3):** spin a `worker_threads` Worker that creates an engine and runs a stream/transform, and from another context destroy/cleanup during the admission window, asserting no crash and a clean terminal result. Note in the test file that this race is **not** deterministically forceable at a fixed interleaving (same limitation rounds 5–10 documented); the test is a best-effort probabilistic guard (loop N iterations) that is green on fixed code and cannot false-fail on it. If a deterministic hook proves infeasible, the test still asserts the unknown/destroyed-handle envelope contract, which is deterministic, and the concurrency correctness rests on the code reasoning in §2.

These raise the vitest baseline above 878. The plan sets the exact new counts.

## Testing

- New Node integration tests per §4 (deterministic envelope assertions + best-effort race guard).
- No Java test change (the `@CEntryPoint` hosted-JVM limitation is real; coverage moves to the Node integration layer against the real addon, which is the correct layer).
- Findings #1/#2/#3 lifecycle correctness that is not deterministically forceable is covered by code reasoning against the invariants in §Design (same documented posture as rounds 5–10).

## Verification

- `cd native-lib/node && npm run build:addon` clean (no new warnings in touched regions); `npm run build` (tsc) clean.
- `npm test` green at the new baseline (set in the plan; ≥ 878 + new tests).
- `git diff --check`.

## Global Constraints

- Node-binding-only. Never touch `native-lib/python/**`.
- Never touch the legacy singleton entrypoints (`run_script`, `run_script_callback`, `run_script_input_output_callback`), `dw_napi_run_script`, or `ScriptRuntime.getInstance()`. The Java side is not modified.
- Handle width stays C `long long` everywhere.
- Errors for run/streaming/transform APIs surface as **resolved** JSON string values (async) or a synchronous `napi_throw_error` (admission/arg/alloc/resource failures) — never `napi_reject_deferred`.
- `napi_env`/`napi_ref`/`napi_deferred`/`napi_threadsafe_function` are thread-affine to the env's JS thread.
- All shared C state (`g_initialized`, `g_active_ops`, `g_teardown_state`, `g_teardown_cancelled`, `g_ref_count`, every engine record's `in_flight`/`destroy_pending`/`deferred_registry_remove`) is read/written only under `g_mutex`, except the documented lock-free `g_isolate` NULL-check in `bridge_finalize`.
- The `g_active_ops` release pattern is EXACTLY, verbatim: `uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);`
- `fn_destroy_engine` is called **exactly once** per handle.
- Every engine now carries an env cleanup hook, so the owner-thread `destroyEngine` guard keys on "a record exists," not on resolver `napi_ref` state. `bridge_finalize`'s `napi_ref` deletion stays resolver-gated.
- Per-engine `in_flight` and global `g_active_ops` stay **distinct** counters (per-handle registry drain vs. global isolate teardown) — not merged.
- Preserve every round-1..10 fix: coalesced `cleanup()`, the JS three-state lifecycle machine, the `TEARDOWN_*` state machine + `napi_initialize` adoption path (incl. round-7's `g_teardown_cancelled` carve-out), the worker-thread `g_active_ops` decrement, the atomic admission blocks, the round-6 handle-read validations, round-7 conversion-status checks, round-8 setup-allocation NULL checks, round-9 worker/callback OOM + N-API-create checks + deferred registry removal, round-10 env-cleanup registry removal + `g_isolate`-guarded finalize.
- Node vitest baseline currently **878 passed / 59 skipped / 0 failed**; this round raises it (new tests) and must stay green.

## Rejected Alternatives

- **#1 via a teardown-time sweep of `g_bridges` instead of per-engine hooks.** Rejected: a global sweep would run on whatever thread triggers isolate teardown, deleting env-affine records off their owner thread — the exact thread-affinity violation the per-env-hook design (F2) exists to avoid. Per-engine hooks dispose each record on its own env's thread.
- **#1 leaving the owner guard resolver-only while adding a hook to resolver-less engines.** Rejected: `napi_remove_env_cleanup_hook` on an early destroy would then run cross-thread (UB) or be skipped (dangling hook → UAF at env teardown). The guard must cover every hooked engine.
- **#2/#3 via a JS-side lease (await per-engine drain before destroy).** Rejected (same as round-9): no per-engine "await my ops" primitive exists at the JS layer; `run()` is synchronous and streaming is an abandonable generator. The authoritative pin lives in C, taken atomically at admission.
- **#2/#3 by re-looking-up the bridge after admission.** Rejected: a second lookup still races destroy in the gap; only holding the pin (`in_flight++`) under the admission lock closes the window.
- **#3 pinning the sync run at the top (before arg extraction).** Rejected: the arg-extraction/OOM path does not touch the engine, so pinning there only adds unwind sites; pin in the same late critical section as `g_active_ops`, matching the existing round-7 reasoning for that path.
- **#4 compatibility shims for the removed `*_with_resolver` ABI.** Rejected (user decision): dwlib is consumed by this repo's own bindings in lockstep; the redesign intentionally replaces that ABI. Documented as an intended break; no shims.
- **#5 removing listeners in `cleanup()` (retain references, `removeListener`).** Rejected in favor of register-once: simpler, no per-instance bookkeeping, and the hooks already tolerate a null singleton, so a single lifetime registration is correct and leak-free.
- **#6 adding a native fault-injection hook to force the race deterministically.** Rejected as test-only production surface (YAGNI), consistent with rounds 6–10; the deterministic envelope assertions plus a best-effort probabilistic race guard are the coverage.
- **Modifying the Java `ScriptRuntime` registry to tolerate late lookups.** Rejected as out of scope and the wrong layer — the C addon must hold the pin so the registry entry is never removed under an admitted op.
