# Worker-Teardown Dangling Resolver Ctx & Shutdown-Doc Accuracy — Round 10 (W-23692110)

**Status:** Design approved (lightweight round), ready for direct implementation.

**Source review:** `docs/pr-157-follow-up-andy-code-review-10.md` (two findings, both verified against live source at commit `d504c0f`, the round-9 tip).

**Scope:** `native-lib/node` only — `src/addon.c` (finding 1) and `src/dataweave.ts` (finding 2). Do **not** touch `native-lib/python/**`, the legacy singleton entrypoints, or `ScriptRuntime.getInstance()`. The Java side is not modified — the C addon must stop leaving a live registry entry pointed at freed memory rather than change Java's registry semantics.

## Problem

### #1 (P1) — Worker teardown frees a resolver bridge but leaves its Java registry entry (and resolver ctx) dangling

`napi_create_engine_with_resolver` passes the `engine_bridge_t* bridge` to Java as the resolver ctx (`addon.c:1640`); Java's `CallbackWeaveResourceResolver` retains it, and `resolve_module_callback` casts that same ctx word back to `engine_bridge_t*` (`addon.c:1450`).

When the owning Worker/main env tears down, the per-env cleanup hook `bridge_env_cleanup` runs. It **frees** the bridge — `bridge_finalize(b, /*env_still_alive=*/true, /*do_registry_remove=*/false)` at `addon.c:265` — but deliberately passes `do_registry_remove=false`, so it does **not** call `fn_destroy_engine`. The `ScriptRuntime` stays in the Java registry with a `CallbackWeaveResourceResolver` whose ctx now points at freed native memory. A subsequent invocation of that handle dereferences freed memory (UAF).

This is exactly the round-9 decision: round 9 gave every engine a record and deferred registry removal for the `destroyEngine` path, but chose `do_registry_remove=false` on the env-cleanup path (`addon.c:105-108`) out of caution about calling `fn_destroy_engine` during env teardown. Round 10 shows that caution was wrong: leaving the registry entry is a UAF.

**Both env-cleanup sub-paths have the gap:**
- Direct free (`in_flight == 0`, `addon.c:265`): frees with `do_registry_remove=false`.
- Deferred (`in_flight > 0`, `addon.c:254-258`): sets `destroy_pending=true` but leaves `destroy_via_destroy_engine=false`, so the later `bridge_end_op` → `bridge_finalize` drain (`addon.c:297-303`) also skips the registry removal.

### #2 (P2) — Shutdown doc over-promises `exit`-hook coverage

`dataweave.ts:276-280` says the synchronous `exit` hook is "the last-ditch fallback for `process.exit()`, uncaught exceptions, and fatal signals." Node does **not** emit `exit` for termination signals such as SIGTERM/SIGKILL (absent a JS signal handler), nor for all fatal failure modes. The comment should describe `exit` as best-effort only and tell callers who need guaranteed graceful shutdown to register and await their own signal handlers.

## Design

### 1. Remove the registry entry during env cleanup (finding #1)

Make `bridge_env_cleanup` remove the Java registry entry before/when it frees the bridge, on **both** sub-paths, guarded on isolate liveness.

**Why calling `fn_destroy_engine` here is safe (the round-9 caution, resolved):**
- `bridge_env_cleanup` is registered **only for resolver-backed engines** (`addon.c:1666`; resolver-less engines register no hook, `addon.c:1598-1601`), so this path is exactly the dangling-ctx case.
- `destroyEngine` removes the hook (`napi_remove_env_cleanup_hook`, `addon.c:1738`) for any engine it handles — deferred or not — so `bridge_env_cleanup` only ever fires for an engine that was **never** passed to `destroyEngine`. Such an engine's `initialize()` ref was likewise never released (both go through `doCleanup()`), so `g_ref_count > 0` and the process-wide GraalVM isolate is still alive: `fn_destroy_engine`'s fresh-thread attach is legal.
- `fn_destroy_engine` attaches its **own** isolate thread (not JS-thread-affine), so it is safe from the env-cleanup hook thread — the same property `destroyEngine`'s deferred-drain finalize already relies on.
- **The one exception:** the main env can tear down *after* `napi_cleanup` already tore down the isolate (`g_isolate == NULL`). Then the Java registry died with the isolate and there is nothing to remove — so the registry removal must be **guarded on `g_isolate != NULL`**.

**Exactly-once preserved:** `destroyEngine` and `bridge_env_cleanup` are mutually exclusive per handle (destroyEngine removes the hook), so `fn_destroy_engine` still runs at most once per handle.

**Changes (`addon.c`):**

a. **Harden `bridge_finalize`'s registry-removal guard** to skip when the isolate is gone — protects every caller and covers the "isolate torn down by drain time" case for the deferred path:
```c
if (do_registry_remove && fn_destroy_engine && g_isolate) {
    void* thread = NULL;
    if (fn_attach_thread(g_isolate, &thread) == 0) { fn_destroy_engine(thread, b->handle); fn_detach_thread(thread); }
}
```
(`g_isolate` is read outside `g_mutex` here — the same accepted pattern as `napi_destroy_engine`'s fallback at `addon.c:1753-1756`; the NULL check narrows the window and makes a torn-down isolate a no-op instead of an unsafe `fn_attach_thread(NULL, …)`.)

b. **`bridge_env_cleanup` direct path** (`addon.c:265`): pass `do_registry_remove=true`:
```c
bridge_finalize(b, /*env_still_alive=*/true, /*do_registry_remove=*/true);
```

c. **`bridge_env_cleanup` deferred path** (`addon.c:254-258`): set the deferred-registry-removal flag so the draining op removes the entry:
```c
if (b->in_flight > 0) {
    b->destroy_pending = true;
    b->deferred_registry_remove = true;   // env-cleanup, like destroyEngine, must remove the registry on drain
    uv_mutex_unlock(&g_mutex);
    return;
}
```

d. **Rename `destroy_via_destroy_engine` → `deferred_registry_remove`.** The field now gates the deferred registry removal for **both** `destroyEngine` and `bridge_env_cleanup`, so the old name (implying "only via destroyEngine") is actively misleading. Update the declaration/comment (`addon.c:105-109`), the set site in `napi_destroy_engine` (`addon.c:1729`), the new set site in `bridge_env_cleanup`, and the read in `bridge_end_op` (`addon.c:298`). Update the stale comments at `addon.c:105-108`, `261-265`, and `300-302` to state that the env-cleanup path now removes the registry.

### 2. Correct the shutdown doc (finding #2)

Reword `dataweave.ts:276-280` so the `exit` hook is described as best-effort synchronous cleanup that runs for `process.exit()`, uncaught exceptions, and normal process end — and explicitly note that Node does **not** emit `exit` for termination signals (SIGTERM/SIGKILL) or all fatal failure modes, so callers needing guaranteed graceful shutdown must register and await their own signal handlers. Doc-only; no behavior change.

## Testing

**No new runtime test.** Consistent with rounds 6–9: the env-teardown UAF path is not deterministically forceable from JS/vitest (it requires a Worker to exit with a live resolver engine and then re-invoke a freed handle across the teardown boundary — no addon-boundary fault-injection exists). Coverage is code reasoning against the exactly-once and isolate-liveness invariants above. #2 is doc-only.

Baseline unchanged: **878 passed / 59 skipped / 0 failed**.

## Verification

- `cd native-lib/node && npm run build:addon` clean (no new warnings in the touched regions); `npm run build` (tsc) clean.
- `npm test` green: **878 passed / 59 skipped / 0 failed**, unchanged.
- `git diff --check`.

## Global Constraints

- Node-binding-only. Never touch `native-lib/python/**`.
- Never touch the legacy singleton entrypoints (`run_script`, `run_script_callback`, `run_script_input_output_callback`), `dw_napi_run_script`, or `ScriptRuntime.getInstance()`. The Java side is not modified.
- Handle width stays C `long long` everywhere.
- Errors for run/streaming/transform APIs surface as **resolved** JSON string values or a synchronous `napi_throw_error` — never `napi_reject_deferred`.
- `napi_env`/`napi_ref`/`napi_deferred`/`napi_threadsafe_function` are thread-affine to the env's JS thread.
- All shared C state (incl. every engine record's `in_flight`/`destroy_pending`/`deferred_registry_remove`) is read/written only under `g_mutex`, except the documented lock-free `g_isolate` NULL-check in `bridge_finalize` (matching the existing `napi_destroy_engine` fallback pattern).
- `fn_destroy_engine` is called **exactly once** per handle — the `destroyEngine` and `bridge_env_cleanup` paths stay mutually exclusive via hook removal.
- The owner-thread `destroyEngine` restriction stays scoped to engines with resolver `napi_ref` state.
- Preserve every round-1..9 fix.
- Node vitest baseline **878 passed / 59 skipped / 0 failed**.

## Rejected Alternatives

- **Leave the env-cleanup path as `do_registry_remove=false` and instead make Java's registry tolerate a freed ctx.** Rejected: out of scope (Node-binding-only) and the wrong layer — the addon must not leave a live registry entry pointing at freed memory. It also cannot: the ctx is opaque to Java.
- **Null the bridge's resolver fields instead of removing the registry entry, so a later `resolve_module_callback` fails closed.** Rejected: the bridge memory is freed, so there is nothing left to null; and the `ScriptRuntime` itself (script cache, module loader) would leak in the Java registry forever. Removing the registry entry reclaims both.
- **Unconditionally call `fn_destroy_engine` without the `g_isolate` guard.** Rejected: at main-env teardown after isolate destruction, `g_isolate == NULL` and `fn_attach_thread(NULL, …)` is unsafe; the registry is already gone, so the call is both dangerous and pointless.
- **Add a runtime regression test.** Rejected: not deterministically forceable (rounds 6–9 precedent); no addon-boundary fault injection for the Worker-exit-then-reinvoke race.
- **Keep the field name `destroy_via_destroy_engine`.** Rejected: after this change it also gates the env-cleanup path, so the name would misdescribe half its uses.
