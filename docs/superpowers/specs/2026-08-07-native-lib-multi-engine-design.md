# Design: Multiple Isolated DataWeave Engines per Process (native-lib, Node)

**Date:** 2026-08-07
**Status:** Approved for implementation
**Tracks:** GUS [W-23692110](https://gus.my.salesforce.com/lightning/r/ADM_Work__c/a07EE00002gS7SOYA0/view) — "Native-lib: support multiple DataWeave engine instances with independent module resolvers"
**Related:** [docs/superpowers/specs/2026-08-04-nodejs-external-modules-design.md](./2026-08-04-nodejs-external-modules-design.md) (the design during which this limitation was discovered)

## Goal

Let multiple `DataWeave` instances coexist in one Node process, each with its own module resolver and script cache, so that different resolvers never collide. Today the second `new DataWeave({ resolveModule })` in a process silently keeps the first instance's resolver.

## Background

`native-lib`'s `ScriptRuntime` (`native-lib/src/main/java/org/mule/weave/lib/ScriptRuntime.java`) is a `static final` singleton (`:33`) holding one `engine` and a **write-once** `static volatile resolver` (`:36`). `setResolver` refuses to run a second time per process (`:58-63`, logs a warning and returns). Every `@CEntryPoint` in `NativeLib.java` routes through `ScriptRuntime.getInstance()`. So two `DataWeave` instances in one process cannot have independent module sets — whichever calls a resolver-backed `run()` first wins.

**This is not a GraalVM constraint.** `native-cli`'s `NativeRuntime` (`native-cli/src/main/scala/org/mule/weave/dwnative/NativeRuntime.scala:50-60`) already builds one independent `DataWeaveScriptingEngine` + `CompositeWeaveResourceResolver` per instance — there is no shared static state there. GraalVM Java statics are scoped per-isolate, which is also why the Python binding (one GraalVM isolate per `DataWeave()` instance) already gets resolver isolation "for free" today. The limitation is specific to `native-lib`'s deliberate Java static singleton plus the Node C addon's global resolver bridge.

## Scope

**In scope:**
- `native-lib` Java layer: turn `ScriptRuntime` from a static singleton into a handle-addressable registry of instances, each with its own engine + resolver.
- Node C addon (`addon.c`): per-handle resolver bridge state instead of one process-global bridge.
- Node TypeScript layer (`ffi.ts`, `dataweave.ts`): each `DataWeave` instance owns an engine handle for its whole lifecycle.

**Out of scope:**
- Python binding changes. Python already achieves isolation via one isolate per instance; unifying it onto the same handle-based API is a **follow-up task** (see Verification).
- Separate GraalVM isolates per engine — rejected as the isolation mechanism (see Alternatives Considered).
- Solving streaming/transform + **custom-module** resolution across the background-thread boundary. This is an existing, documented hazard (`NativeLib.java:386-390,471-475`) and stays as-is: streaming against a resolver-backed engine still fails closed (returns "not found") for custom modules reached from the background thread; built-in modules continue to resolve normally in all cases.

## Alternatives Considered

**Separate GraalVM isolates per engine (rejected).** Each engine gets its own isolate — the most complete form of isolation (own heap, own JIT, own Java statics), and what Python already does per-instance. Rejected for Node because:
- `addon.c` currently assumes exactly one isolate as global state (`g_isolate`, `g_thread`, `g_ref_count`); supporting N isolates means restructuring all of that into per-handle structs.
- Isolate teardown is documented as fragile: `graal_tear_down_isolate` blocks until every attached thread reaches a safepoint (`addon.c:172-178`), and multiple concurrent isolates multiply that fragility.
- It is unnecessarily heavy for the actual need: independent module resolution and script caching, not full JVM-level sandboxing between tenants.

**Chosen: object-level engines in one shared isolate.** Multiple `DWScriptingEngine` Java objects, each with its own resolver and compiled-script cache, all living in the single existing GraalVM isolate, addressed by an opaque handle. This mirrors what `native-cli` already does and requires no changes to isolate lifecycle management.

## Architecture

Three-layer change, following the existing callback/FFI layering.

### Layer 1 — Java (`native-lib/src/main/java/org/mule/weave/lib/`)

**`ScriptRuntime.java`** — from static singleton to per-instance + registry:
- Constructor becomes `ScriptRuntime(CallbackWeaveResourceResolver resolver)` (null ⇒ ClassLoader-only resolver, same as today's default). The resolver is now bound once at construction — immutable for the instance's lifetime. **Remove** the `static setResolver` write-once mutation entirely.
- Add a static registry:
  ```java
  private static final ConcurrentHashMap<Long, ScriptRuntime> REGISTRY = new ConcurrentHashMap<>();
  private static final AtomicLong NEXT_HANDLE = new AtomicLong(1);

  static long register(ScriptRuntime rt) {
      long handle = NEXT_HANDLE.getAndIncrement();
      REGISTRY.put(handle, rt);
      return handle;
  }
  static ScriptRuntime get(long handle) { return REGISTRY.get(handle); }
  static void destroy(long handle) { REGISTRY.remove(handle); }
  ```
- `compositeResolver()` / `createModuleComponentsFactory()` become instance methods operating on the instance's own resolver field instead of a static field.
- **Keep `getInstance()`** returning a lazily-created default (ClassLoader-only, handle-less) instance, so the existing resolver-less `@CEntryPoint`s (`run_script`, `run_script_callback`, `run_script_input_output_callback`) — used by the Python binding — are untouched.

**`CallbackWeaveResourceResolver.java`** — store a `PointerBase ctx` alongside the callback, forwarded on every `callback.invoke(...)` call (see Layer 2 crux below). Constructor becomes `(ResolveModuleCallback callback, PointerBase ctx)`.

**`NativeCallbacks.java`** — add a context parameter to the resolver callback, mirroring the existing `WriteCallback`/`ReadCallback` `ctx` idiom (`:31-49`):
```java
public interface ResolveModuleCallback extends CFunctionPointer {
    @InvokeCFunctionPointer
    CCharPointer invoke(IsolateThread thread, PointerBase ctx, CCharPointer modulePath);
}
```
This is what lets one shared native callback dispatch to the correct per-handle JS resolver on the C side.

**`NativeLib.java`** — add lifecycle + handle-based execution entrypoints; keep all existing entrypoints unchanged for Python:
- `create_engine(IsolateThread) -> long`
- `create_engine_with_resolver(IsolateThread, ResolveModuleCallback, PointerBase ctx) -> long`
- `destroy_engine(IsolateThread, long handle)`
- `run_script_engine(IsolateThread, long handle, CCharPointer script, CCharPointer inputs) -> CCharPointer`
- `run_script_callback_engine(...)` / `run_script_input_output_callback_engine(...)` — same bodies as today's streaming methods, but resolving the `ScriptRuntime` via `ScriptRuntime.get(handle)` instead of `getInstance()`.

The existing `run_script_with_resolver`, `run_script_callback_with_resolver`, and `run_script_input_output_callback_with_resolver` entrypoints (`NativeLib.java:348-566`) are **removed** — they are not called from any stable release path (per their own doc comments) and their functionality is fully subsumed by `create_engine_with_resolver` + the handle-based run methods.

### Layer 2 — C addon (`native-lib/node/src/addon.c`)

- Replace the process-global resolver bridge state (`g_resolver_env`, `g_resolver_ref`, `g_resolver_thread`, `:73-86`) with a small per-handle registry: `{ napi_env env; napi_ref resolver_js; uv_thread_t owner; }` keyed by handle (a fixed-size array or linked list is sufficient — engine counts per process are expected to be small).
- **Crux — dispatching to the right resolver.** `ResolveModuleCallback` gains a `ctx` parameter (Layer 1). `createEngineWithResolver` allocates the per-handle bridge struct and passes its address as `ctx` down through `create_engine_with_resolver`. When Java invokes `resolve_module_callback(thread, ctx, path)`, C casts `ctx` back to the bridge struct and calls the JS resolver it holds — synchronously on the JS thread, exactly as today (no `napi_threadsafe_function`; the existing deadlock rationale at `:62-72` still applies, since `createEngineWithResolver`'s native call runs synchronously on the calling JS thread).
- Keep the thread-affinity guard, now scoped per-handle: if `resolve_module_callback` is reached from a thread other than the bridge's recorded `owner` (e.g. from `streaming_thread_fn`/`transform_thread_fn`), fail closed — return "not found" — instead of touching `napi_env` from the wrong thread. This preserves today's safety property, just per-engine instead of process-wide.
- Reuse the existing per-call result-buffer tracking (`resolver_results_track`/`resolver_results_free_all`, `:94-118`) unchanged — it is already scoped to a single native call.
- New N-API methods: `createEngine()`, `createEngineWithResolver(resolverFn)`, `destroyEngine(handle)`, and handle-taking `runScriptEngine`, `runScriptStreamingEngine`, `runScriptTransformEngine` — each attaches/detaches an isolate thread exactly like the current per-call pattern (`fn_attach_thread`/`fn_detach_thread`).

### Layer 3 — Node TypeScript (`native-lib/node/src/`)

**`ffi.ts`** — add `createEngine()`, `createEngineWithResolver(resolver)`, `destroyEngine(handle)`, and handle-taking `runScriptEngine`, `runScriptStreamingEngine`, `runScriptTransformEngine`. Remove `runWithResolver`.

**`dataweave.ts`** — `DataWeave` gains a `private engineHandle?: number`:
- `initialize()`: after `ffi.initialize()`, call `ffi.createEngineWithResolver(this.resolveModule)` if a resolver was supplied at construction, else `ffi.createEngine()`; store the returned handle.
- `run()` / `runStreaming()` / `runTransform()`: always route through the handle-based FFI methods, passing `this.engineHandle`. Drop the `if (this.resolveModule) { ffi.runWithResolver(...) } else { ffi.runScript(...) }` branch (current `dataweave.ts:123-129`) — there is now exactly one code path per method, parameterized by handle.
- `cleanup()`: call `ffi.destroyEngine(this.engineHandle)` before releasing the library reference.
- Update the `resolveModule` docstring (`dataweave.ts:20-48`): remove the "one resolver per process / first instance wins / different-thread" caveats (`:26-42`) — this limitation is what this design fixes. Keep the synchronous-resolver requirement and the security/trust-model note (`:44-46`).

## Data Flow

```
new DataWeave({ resolveModule: A }).initialize()
  → ffi.createEngineWithResolver(A)
  → addon.c: createEngineWithResolver
      allocate bridge_A { env, ref to A, owner=thisThread }
      call create_engine_with_resolver(thread, resolve_module_callback, &bridge_A)
  → Java: new CallbackWeaveResourceResolver(callback, ctx=&bridge_A)
          new ScriptRuntime(resolver) → handle_A = ScriptRuntime.register(rt)
  → returns handle_A to JS, stored as this.engineHandle

dwA.run(script importing "custom/lib.dwl")
  → ffi.runScriptEngine(handle_A, script, inputs)
  → Java: ScriptRuntime.get(handle_A).run(...)
      compositeResolver: ClassLoader (miss) → CallbackWeaveResourceResolver
        callback.invoke(thread, ctx=&bridge_A, "custom/lib.dwl")
  → C: resolve_module_callback(thread, &bridge_A, path)
      cast ctx → bridge_A; thread == bridge_A.owner? yes
      call bridge_A.resolver_js(path) synchronously  →  resolver A's source
  → result flows back through Java, script compiles

// Second, independent instance in the SAME process:
new DataWeave({ resolveModule: B }).initialize()  →  handle_B, bridge_B (different resolver, different owner-checked bridge)
dwB.run(script importing "custom/lib.dwl")
  → resolves via resolver B, NOT resolver A — no cross-talk, and A's cache is untouched
```

## Error Handling

Unchanged from the existing resolver design (`ScriptRuntime` compositeResolver, `CallbackWeaveResourceResolver.resolve`) except scoped per-handle:
- **Module not found:** resolver returns `null` → `Option.empty()` → composite resolver falls through → standard DataWeave "unable to resolve module" error, same as today.
- **Resolver throws / callback fails:** caught in `CallbackWeaveResourceResolver.resolve`'s existing try/catch, logged, treated as not-found — unchanged.
- **Wrong-thread resolver invocation (streaming/transform against a resolver-backed engine):** the per-handle `owner` check in `addon.c` fails closed to "not found" instead of touching `napi_env` cross-thread. This is the same safety property as today's process-wide guard, just correctly scoped to the specific engine instance instead of the whole process.
- **Invalid/unknown handle** (`run_script_engine` called after `destroy_engine`, or with a bogus value): `ScriptRuntime.get(handle)` returns `null`; the `@CEntryPoint` returns a `{"success":false,"error":"Unknown engine handle"}` JSON error rather than throwing an NPE.

## Backward Compatibility

- **Python binding:** zero changes. It never called the `*_with_resolver` entrypoints being removed, and continues using `run_script`/`run_script_callback`/`run_script_input_output_callback` against the default `getInstance()` runtime.
- **Node, resolver-less usage:** `new DataWeave()` with no `resolveModule` behaves identically — `initialize()` calls `createEngine()` (no resolver), execution unchanged from the caller's perspective.
- **Node, single-resolver usage:** existing tests that construct exactly one `DataWeave({ resolveModule })` per process continue to pass — the new code path is functionally a superset (it now also supports a second, independent instance).
- **Breaking (internal-only) change:** `ResolveModuleCallback`'s native signature gains a `ctx` parameter. This is an internal FFI contract with no external callers documented outside this repo (the Node addon is the sole consumer), so it is not a public API break.

## Testing Strategy

1. **Java unit test** (`native-lib:test`, new test class alongside `ScriptRuntime`): register two `ScriptRuntime` instances with different in-memory `CallbackWeaveResourceResolver`s; assert each instance's `run()` resolves only its own module; assert `destroy()` removes an instance so `get()` returns `null` afterward.
2. **Node integration test** (`native-lib:nodeTest`) — the direct W-23692110 regression: construct two `DataWeave` instances in the same process with different `modulesFromMap` resolvers; assert each `run()` resolves its own import and fails to resolve the other's; assert built-in modules (e.g. `dw::core::Strings`) resolve correctly through both.
3. **Backward-compat regression:** existing resolver-less and single-resolver Node tests continue to pass unchanged. Full Python test suite (`native-lib:pythonTest`) passes unchanged (no Python-facing code touched).
4. **Native image build:** `./gradlew native-lib:nativeCompile` stays green; check build output for any new `--initialize-at-run-time` requirement introduced by the registry (`ConcurrentHashMap`/`AtomicLong` are standard JDK classes already used elsewhere in this codebase, so none expected).

## Follow-Up Work

- **Python binding parity:** file a GUS work item (child of W-23692110) to port the handle-based `create_engine`/`run_script_engine` API to the Python binding, so both bindings share one mental model instead of Python's implicit "one isolate per instance" and Node's explicit "one handle per instance."
- **Streaming/transform + custom-module resolution:** the cross-thread hazard preventing custom-module resolution during streaming/transform (documented in `NativeLib.java`) is unrelated to the singleton fix and remains a separate, not-yet-scoped effort.

## References

| Item | Location |
|------|----------|
| GUS ticket | W-23692110 |
| Singleton root cause | `native-lib/src/main/java/org/mule/weave/lib/ScriptRuntime.java:33-45` |
| Write-once resolver guard | `native-lib/src/main/java/org/mule/weave/lib/ScriptRuntime.java:58-63` |
| CLI's per-instance pattern (proof it's not a GraalVM constraint) | `native-cli/src/main/scala/org/mule/weave/dwnative/NativeRuntime.scala:50-60` |
| Existing resolver-aware entrypoints (to be removed) | `native-lib/src/main/java/org/mule/weave/lib/NativeLib.java:348-566` |
| Existing WriteCallback/ReadCallback ctx idiom | `native-lib/src/main/java/org/mule/weave/lib/NativeCallbacks.java:31-49` |
| C addon process-global resolver bridge (to be made per-handle) | `native-lib/node/src/addon.c:62-118` |
| Documented streaming/transform cross-thread hazard | `native-lib/src/main/java/org/mule/weave/lib/NativeLib.java:386-390,471-475` |
| Node dataweave.ts resolver caveats (to be removed) | `native-lib/node/src/dataweave.ts:26-46` |
| Original external-modules design (where this limitation was discovered) | `docs/superpowers/specs/2026-08-04-nodejs-external-modules-design.md` |
