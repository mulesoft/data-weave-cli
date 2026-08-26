# Design: Unify Node & Python on One Handle-Based Engine Model (remove the ScriptRuntime singleton)

**Date:** 2026-08-26
**Status:** Approved (brainstorm); pending implementation plan
**Branch:** `w-23692110-multi-engine-design` (extends PR #157 — one unified change, not a follow-up)
**Tracks:** GUS W-23692110 — "Native-lib: support multiple DataWeave engine instances with independent module resolvers"
**Related:** [2026-08-07-native-lib-multi-engine-design.md](./2026-08-07-native-lib-multi-engine-design.md) (the Node multi-engine design this extends), [2026-08-04-nodejs-external-modules-design.md](./2026-08-04-nodejs-external-modules-design.md)

> **Pre-GA.** `dwlib` is consumed only by this repo's own Node and Python bindings, in lockstep.
> This design intentionally breaks the C ABI and removes the Java singleton; there are no
> compatibility shims. The Python *public* API is preserved.

## 1. Goal

Put the Node and Python bindings on a **single, unified engine-isolation model** — one shared
process-wide GraalVM isolate holding N handle-addressed engines, each with its own module
resolver and script cache — and **remove the `ScriptRuntime` singleton entirely** so that all
execution is handle-addressed in both bindings. Maximize shared code by making the Java engine
layer the single source of truth that both bindings drive through the identical C ABI.

## 2. Background

PR #157 gave the **Node** binding multiple isolated engines per process using "one shared isolate
+ object-level engine handles" (see the related design). It kept, for backward compatibility, the
`ScriptRuntime` static singleton (`defaultInstance` / `getInstance()`) and three legacy
singleton C entrypoints (`run_script`, `run_script_callback`, `run_script_input_output_callback`),
because the **Python** binding still used them.

Two facts make unification the right move now:

1. **The rebase exposed a real collision.** Master shipped a Python module-resolver feature that
   calls `run_script_with_resolver` (a 2-arg-callback singleton entrypoint). PR #157's ABI break
   removed exactly that entrypoint and changed the resolver callback to a 3-arg (ctx) form. After
   rebasing, master's Python tests run against this branch's dwlib and fail
   (`run_script_with_resolver not found`) — the current red CI.
2. **The two bindings had diverged in isolation model.** Python historically used **one isolate
   per `DataWeave` instance** (isolate-per-instance); Node uses **one shared isolate + engine
   handles**. Maintaining two models is undesirable. The maintainer's decision: unify on one
   model and reuse as much code as possible.

## 3. Why the shared-isolate model is the unifying choice

There are two candidate isolation models:

- **Isolate-per-engine** (Python's current model): each instance gets its own isolate; separate
  heaps; teardown is trivially independent.
- **Shared isolate + engine handles** (Node's model): one process-wide isolate; engines are cheap
  Java objects in a registry; the isolate is reference-counted and torn down on last release.

Unifying on **shared isolate + engine handles** is correct because:

- **Node cannot cheaply move to isolate-per-engine.** `graal_tear_down_isolate` blocks until all
  GraalVM-attached threads reach a safepoint; Node's streaming workers deliver chunks via a
  `napi_threadsafe_function` that needs the libuv event loop to run. Tearing an isolate down while
  the loop must keep running is the deadlock PR #157 spent ~15 review rounds hardening. Multiplying
  that per-isolate is strictly worse, and switching Node off its shipped model discards that work.
- **Python can trivially move to the shared model.** Its ctypes calls are synchronous and it owns
  its stream-worker threads directly, so it needs *none* of Node's `PENDING_WAIT`/adoption/retry
  machinery — just a reference count and a synchronous drain-before-teardown.
- **The engine logic already lives in a shared layer** (Java `ScriptRuntime` registry + the
  `*_engine` C ABI), so both bindings reuse it verbatim.

**Accepted trade-off:** Python instances in one process now share one isolate's heap instead of
having separate heaps. This is weaker memory isolation, relevant only if mutually-untrusted scripts
run in one process expecting heap-level separation. The maintainer accepted this in exchange for a
single maintained model.

## 4. The reuse boundary (fixed by the architecture)

**Shared common core (used identically by both bindings):**
- Java `ScriptRuntime` + the handle registry (`register`/`get`/`destroy`).
- The engine C ABI: `create_engine`, `create_engine_with_resolver`, `run_script_engine`,
  `run_script_callback_engine`, `run_script_input_output_callback_engine`, `destroy_engine`.
- The 3-arg `ResolveModuleCallback(thread, ctx, modulePath)` contract.

**Necessarily binding-specific (cannot share source):**
- Isolate lifecycle (`graal_create_isolate`/`graal_tear_down_isolate`) + reference count, thread
  attach/detach, resolver-callback marshalling, stream worker threads. This *must* live in the
  binding because the isolate C API is called from *outside* the isolate; Java code runs *inside*
  one and cannot create/tear down its own. Node implements this in `addon.c` (N-API/C); Python in
  `native.py` (ctypes). They share no source but implement the **same lifecycle contract** — which
  is captured in a short shared "engine lifecycle contract" doc.

Net: one shared Java engine ABI as the source of truth; each binding drives it with thin,
host-appropriate glue.

## 5. Architecture (layer map)

### Java — `native-lib/src/main/java/org/mule/weave/lib/` (shared core; mostly subtractive)
- **`ScriptRuntime.java`** — delete `defaultInstance` + `getInstance()`. Keep the registry, the
  resolver-bound-at-construction model, and instance execution. `ScriptRuntime` becomes purely
  handle-addressed.
- **`NativeLib.java`** — delete the 3 legacy `@CEntryPoint`s (`run_script`, `run_script_callback`,
  `run_script_input_output_callback`). Keep only the `*_engine` + `create_engine[_with_resolver]`
  + `destroy_engine` set. (The `*_with_resolver` entrypoints removed by PR #157 stay removed.)
- **`NativeCallbacks.java`** — `ResolveModuleCallback` stays the 3-arg ctx form; the old 2-arg form
  is gone. Both bindings use the 3-arg form.

### Node — `native-lib/node/src/addon.c` (near-zero change)
- Delete `dw_napi_run_script`, the `fn_run_script`/`run_script` `dlsym`, and its entry in the
  required-symbols guard. The engine API and teardown state machine are otherwise untouched.
- Verify whether any public JS export still surfaces the legacy `run`; if so, remove it as a
  documented pre-GA break.

### Python — `native-lib/python/src/dataweave/` (the bulk of the work)
- **`native.py` (`NativeRuntime`)** — rewrite the glue to the shared model:
  - Module-level shared state guarded by one lock: `_isolate` (or None), `_isolate_ref_count`,
    the main attached thread.
  - Bind the `*_engine` + `create_engine[_with_resolver]` + `destroy_engine` symbols.
  - The 3-arg ctx resolver trampoline + a `{handle: (resolver, buffers)}` map.
  - The reference-count + drain-before-teardown lifecycle (§6).
- **`runtime.py` (`DataWeave`)** — `initialize()` acquires an isolate ref + creates one engine
  (`create_engine` or `create_engine_with_resolver`) and stores its `handle`; run methods route
  through the `*_engine` entrypoints with that handle; `cleanup()` drains this instance's stream
  workers, `destroy_engine(handle)`, releases the isolate ref. **The public Python API surface is
  unchanged.**
- **`models.py`** — `RESOLVE_MODULE_CALLBACK` ctypes signature gains the `ctx` argument.
- **Tests** — migrate off `run_script_with_resolver` to the engine ABI; add multi-instance
  isolation + refcount-teardown coverage.

### New shared artifact
- A short **engine lifecycle contract** doc (the invariant list) that both bindings reference.

## 6. Python lifecycle & teardown model

**Shared state (module-level in `native.py`), all mutations under one module lock:**
- `_isolate` (the single process-wide isolate, or None), `_isolate_ref_count`, main attached thread.
- **Invariant:** `_isolate_ref_count` == number of live engines across all `DataWeave` instances,
  and the isolate exists iff the count > 0.

**`initialize()` (per instance):**
1. Under the lock: if `_isolate` is None → `graal_create_isolate()` + attach the main thread once;
   then `_isolate_ref_count += 1`.
2. `create_engine()` or `create_engine_with_resolver(ctx=handle, trampoline)` → store `handle` on
   the instance. (Handle is allocated by Java; for the resolver case the ctx *is* that handle, so
   the map entry is added immediately after the handle is returned — see §7 for the ordering note.)

Each instance owns exactly one engine handle and contributes exactly one to the isolate refcount.

**`run` / `run_streaming` / `run_callback` / `run_transform`:** route through the `*_engine`
entrypoints with the instance's `handle`. Stream workers attach their own GraalVM thread, run,
detach on completion (existing pattern).

**`cleanup()` (per instance):**
1. Drain *this instance's* stream workers — signal cancel + **join** the threads (synchronous;
   Python owns the threads, so no event loop and no deadlock).
2. `destroy_engine(handle)`; remove the resolver-map entry; clear the instance handle.
3. Under the lock: `_isolate_ref_count -= 1`; **if it reaches 0** → detach the main thread and
   `graal_tear_down_isolate()`, set `_isolate = None`.

**Why this stays simple:** teardown happens only on the *last* release, by which point every
instance has already joined its own workers in step 1 — so the isolate has no attached worker
threads when `graal_tear_down_isolate` runs. Hence **none** of Node's `PENDING_WAIT`/adoption/retry
machinery is needed.

**Idempotency / safety:** `cleanup()` on an uninitialized or already-cleaned instance is a no-op;
double-`cleanup()` releases the ref only once (guarded by the instance's handle being cleared).

**Concurrency:** keep today's **per-instance serialization** of native execution calls
(`_serialized_native_operation`), but allow **different instances to run concurrently** — each on
its own attached thread in the shared isolate (GraalVM supports multiple attached threads). The
module lock guards only isolate refcount/create/teardown; it is **not** held during script
execution, so one engine's long-running script never blocks another engine's `initialize()`/`run()`.

## 7. Resolver dispatch & the streaming/resolver hazard

**Per-engine resolver dispatch (ctx mechanism):**
- `create_engine_with_resolver` passes `ctx = handle` (the engine handle).
- Python registers **one** C trampoline (`RESOLVE_MODULE_CALLBACK`). GraalVM calls it with
  `(thread, ctx, module_path)`; it looks up `ctx` in `{handle: (resolver, buffers)}`, invokes that
  engine's Python resolver, and returns the source-buffer pointer.
- This is the Python analog of Node's per-handle bridge — same ctx concept, so the Java/ABI side is
  identical. Multiple Python engines with different resolvers dispatch correctly within the shared
  isolate.

**Ordering note:** the ctx passed to `create_engine_with_resolver` is the handle it returns, so
either (a) allocate the handle first and pass it as ctx, or (b) register the trampoline against a
provisional key and re-key once the handle is known. The plan will pick the concrete mechanism; the
requirement is that no resolve callback can fire for a handle before its map entry exists (resolves
only occur during a `run` on that engine, which happens strictly after `create_engine_with_resolver`
returns, so this is naturally safe).

**Streaming / transform + custom modules — parity with Node (out of scope):**
Resolving a *custom* module reached from a background stream-worker thread is the pre-existing
documented hazard. Python adopts the **same owner-thread guard** as Node: the trampoline resolves
only when invoked on the engine's owner thread and fails closed ("not found") on a background stream
thread — identical behavior across bindings. Built-in modules resolve normally everywhere;
synchronous `run()` with a resolver resolves custom modules fully in both bindings.

This is a conservative parity choice, not a hard Python limitation: because Python callbacks hold
the GIL, Python could potentially support streaming custom-module resolution later as a
Python-specific enhancement. Out of scope here to preserve one unified behavior.

## 8. Data flow

```
dwA = DataWeave(resolve_module=A); dwA.initialize()
  → lock: _isolate None → graal_create_isolate() + attach main thread; ref 0→1
  → create_engine_with_resolver(ctx=handleA, trampoline); map[handleA] = (A, buffers); dwA._handle = handleA

dwB = DataWeave(resolve_module=B); dwB.initialize()
  → lock: _isolate exists → reuse; ref 1→2
  → create_engine_with_resolver(ctx=handleB, trampoline); map[handleB] = (B, buffers)

dwA.run("... import custom/lib ...")
  → run_script_engine(handleA, script, inputs)   [own attached thread]
  → Java engine A: ClassLoader miss → callback(thread, ctx=handleA, "custom/lib")
  → trampoline: map[handleA] → resolver A → source; A's cache used, B untouched

dwB.run(...)  → resolves via B only; independent cache; no cross-talk

dwA.cleanup()  → join dwA workers; destroy_engine(handleA); del map[handleA]; ref 2→1 (isolate stays)
dwB.cleanup()  → join workers; destroy_engine(handleB); ref 1→0 → detach main + graal_tear_down_isolate(); _isolate=None
```

## 9. Error handling

- **Isolate create fails** → `DataWeaveError`; refcount not incremented; `_isolate` stays None.
- **`create_engine` fails after isolate create** → release the isolate ref (tearing down if this
  call created it), then raise — a failed init leaks nothing.
- **Unknown/destroyed handle** → Java `get(handle)` is null → entrypoint returns
  `{"success":false,"error":"Unknown engine handle"}`; Python surfaces an unsuccessful
  `ExecutionResult`/`DataWeaveError`, never a crash.
- **Resolver raises / returns non-str** → trampoline returns None → standard "unable to resolve
  module" (unchanged Python behavior).
- **`run`/stream after `cleanup()`** → instance guard raises `DataWeaveError` (handle already
  cleared); the C layer never sees a stale handle.
- **`destroy_engine` throws during `cleanup()`** → still release the isolate ref (so a throwing
  destroy cannot strand the isolate), then re-raise — mirrors Node's `doCleanup()`.
- **`graal_tear_down_isolate` returns nonzero** → raise `DataWeaveError`, but leave `_isolate` set
  with count 0; the next `initialize()` reuses that live isolate (count 0→1). No retry flag needed
  — there is no event loop to defer to.

## 10. Backward compatibility (all intended, pre-GA, no shims)

- **dwlib C ABI:** removes `run_script` / `run_script_callback` / `run_script_input_output_callback`;
  keeps only the `*_engine` + `create_engine[_with_resolver]` + `destroy_engine` set;
  `ResolveModuleCallback` is 3-arg only. Consumed by this repo's own bindings in lockstep.
- **Java:** `getInstance()`/`defaultInstance` removed — `ScriptRuntime` is purely handle-addressed.
- **Node:** removes the legacy `dw_napi_run_script` path (and any JS export surfacing it).
- **Python:** **public API unchanged** — `DataWeave(...)`, `initialize`, `run`, `run_streaming`,
  `run_callback`, `run_transform`, `cleanup`, and the module-level functions keep signatures and
  behavior. Only `native.py`'s internal ABI changes.
- **Docs:** update the 2026-08-07 consolidated design's Python notes: singleton removed; all
  execution handle-addressed; both bindings on one engine ABI.

## 11. Testing strategy

- **Java unit** — two `ScriptRuntime` instances with different in-memory resolvers each resolve only
  their own module; `destroy(handle)` removes one. Delete/adjust tests referencing `getInstance()`.
- **Python unit** (fake/mocked lib, no dwlib) — migrate `test_native.py` off `run_script_with_resolver`
  to the engine ABI; cover refcount create/reuse/last-release-teardown, ctx→resolver trampoline
  dispatch (two handles → two resolvers), `cleanup()` idempotency + double-cleanup, and the
  `create_engine`-failure rollback releasing the isolate ref.
- **Python integration** (real dwlib) — the core W-23692110 regression (two instances, different
  resolvers, one process, no cross-talk); multi-instance teardown via a refcount proxy (after all
  instances clean up, a fresh raw engine call fails "not initialized"); synchronous `run()` with a
  resolver resolves custom modules; streaming/transform still stream; streaming custom-module
  resolution fails closed (parity); **TCK conformance stays green**.
- **Node** — existing suite stays green; remove the `dw_napi_run_script` test with its entrypoint.
- **Build** — `native-lib:nativeCompile` green with the 3 legacy `@CEntryPoint`s removed (confirm no
  SPI/reflection config references them).
- **CI** — the currently-red Python module-resolver tests pass, because Python now calls
  `create_engine_with_resolver` instead of the removed `run_script_with_resolver`.

## 12. Follow-Up Work

- Optional Python-specific enhancement: support custom-module resolution during streaming/transform
  (feasible under the GIL; deliberately out of scope here for cross-binding parity).
- Fold the two multi-engine design docs' shared concepts into a single reference if they drift.

## References

| Item | Location |
|------|----------|
| GUS ticket | W-23692110 |
| Node multi-engine design (extended here) | `docs/superpowers/specs/2026-08-07-native-lib-multi-engine-design.md` |
| Java engine registry / entrypoints | `native-lib/src/main/java/org/mule/weave/lib/{ScriptRuntime,NativeLib,NativeCallbacks}.java` |
| Node addon (legacy path to remove) | `native-lib/node/src/addon.c` (`dw_napi_run_script`) |
| Python glue to rewrite | `native-lib/python/src/dataweave/{native,runtime,models}.py` |
| Current Python isolate-per-instance model | `native-lib/python/src/dataweave/native.py` (`graal_create_isolate` in `NativeRuntime.initialize`) |

## Engine lifecycle contract (shared by both bindings)

These invariants are the shared artifact both `native-lib/node/src/addon.c` and
`native-lib/python/src/dataweave/native.py` implement. Any binding on the `*_engine` C ABI must
uphold all six:

1. One process-wide isolate; engines are handle-addressed objects in the Java registry.
2. The isolate is reference-counted; the refcount equals the number of live engines; the isolate
   exists iff the refcount > 0.
3. Create-on-first-ref, tear-down-on-last-release; the binding calls
   `graal_create_isolate` / `graal_tear_down_isolate` from *outside* the isolate.
4. Each engine handle is created by `create_engine` / `create_engine_with_resolver` and destroyed
   by `destroy_engine`.
5. Resolver dispatch is per-engine via the opaque `ctx` echoed to the 3-arg `ResolveModuleCallback`;
   custom-module resolution fails closed off the engine's owner thread.
6. A failed engine-create rolls back the isolate ref; a throwing `destroy_engine` still releases
   the ref.
