# GA Cleanup Backlog

Non-blocking cleanup/refactor items identified while working on the multi-engine
Node binding (W-23692110, PR #157). None of these are required for that PR to
merge — tracked here to brainstorm and prioritize before GA, since pre-GA we
have no external ABI-stability commitment yet and more latitude to remove
legacy paths outright.

## Node binding

1. **Dead legacy `runScript` wrapper.** `native-lib/node/src/ffi.ts:6,44-46`
   (`runScript`), the `"runScript"` N-API export at
   `native-lib/node/src/addon.c:1234-1235`, and `dw_napi_run_script` itself
   (`addon.c:382-...`) are unreferenced — the Node singleton now routes
   through `createEngine()`/`runScriptEngine()` instead. Safe to delete from
   the Node addon without touching the underlying C `run_script` symbol,
   which Python still depends on.

2. **Undocumented owner-thread constraint on `destroyEngine`.**
   `native-lib/node/src/addon.c` (`napi_destroy_engine`) requires cleanup-hook
   removal / `napi_ref` deletion to happen on the bridge's owner thread. Today
   this is only implied by the general "don't share a `DataWeave` instance
   across Workers" rule in the README. Add an explicit one-line code comment
   stating the constraint directly on `napi_destroy_engine`.

3. **Test clarity: near-tautological assertion.**
   `native-lib/node/tests/integration/dataweave-resolver.test.ts` — the
   cleanup-during-streaming regression test's `expect(settled).toBe(true)`
   is near-tautological (the real protection is process survival, not the
   value). Add a comment explaining that if this test is touched again.

4. **Test tightening: throwing-resolver test.** Same file — the
   throwing-resolver test only asserts `result.success === false`; could
   additionally assert `result.error` is truthy for a slightly stronger
   check.

6. **~~Unchecked `malloc` before the fill `napi_get_value_string_utf8` in
   streaming/transform.~~ RESOLVED (round 8, commit `516311e`).** The streaming
   (`napi_run_script_streaming_engine`) and transform
   (`napi_run_script_transform_engine`) entrypoints passed `calloc`/`malloc`
   results straight to `w->handle` / the fill `napi_get_value_string_utf8`
   without a NULL check, unlike `napi_run_script_engine`. On OOM this
   segfaulted the host process (NULL deref) and stranded the `g_active_ops`
   reservation. The eighth "andy" review
   (`docs/pr-157-follow-up-andy-code-review-8.md`) escalated it Minor→P1, and
   round 8 fixed both sites: every `calloc`/`malloc` is NULL-checked before any
   dereference, each OOM path unwinds `g_active_ops` (verbatim pattern) and
   frees any partial work struct, throwing bare `"OOM"` to match
   `napi_run_script_engine`. Spec:
   `docs/superpowers/specs/2026-08-18-oom-safe-streaming-transform-setup-design.md`.
   Note: the identical gap in the legacy singleton `dw_napi_run_script` was
   deliberately left (out of scope by the Global Constraints) — subsumed by
   item 1's "delete the dead legacy wrapper".

## Cross-binding / architecture

5. **Retire the legacy `ScriptRuntime` singleton once Python adopts the
   per-engine registry.** `native-lib/src/main/java/org/mule/weave/lib/ScriptRuntime.java`
   (`defaultInstance`, `getInstance()`) backs the legacy `run_script` /
   `run_script_callback` / `run_script_input_output_callback` `@CEntryPoint`s
   in `NativeLib.java`, called today only by the Python binding. These are
   exported as part of `dwlib`'s public C ABI (`dwlib.h`), not just internal
   plumbing — so removing them is a bigger call than deleting an internal TS
   wrapper (item 1) and needs a deliberate decision, not just a "zero
   internal callers" grep.
   - Requires deciding whether Python migrates onto the same handle-keyed
     registry the Node binding uses (possibly with a single implicit handle
     if Python doesn't need multi-engine support), or keeps its own
     singleton path indefinitely.
   - Being pre-GA removes the "might break an external consumer of the C
     ABI" concern, but this is still cross-binding work broader than the
     Node-only scope of PR #157 — needs its own brainstorm/plan before
     starting.
