# Design: Multiple Isolated DataWeave Engines per Process (native-lib — Node & Python)

**Date:** 2026-08-07 (consolidated 2026-08-25; unified Node + Python 2026-08-26)
**Status:** Approved and implemented on `w-23692110-multi-engine-design` (PR #157)
**Tracks:** GUS [W-23692110](https://gus.my.salesforce.com/lightning/r/ADM_Work__c/a07EE00002gS7SOYA0/view) — "Native-lib: support multiple DataWeave engine instances with independent module resolvers"
**Related:** [docs/superpowers/specs/2026-08-04-nodejs-external-modules-design.md](./2026-08-04-nodejs-external-modules-design.md) (the design during which this limitation was discovered)

> **About this document.** This is the single, consolidated design for the multi-engine
> `native-lib` feature across **both** consumer bindings — Node and Python. It describes the
> **final state** as shipped on PR #157. The core feature (object-level engines behind opaque
> handles, one shared GraalVM isolate) is common to both bindings and is driven through the
> identical `*_engine` C ABI. Two substantial bodies of work are folded in here rather than kept
> as separate documents: the Node **concurrency & lifecycle model** (§6), hardened across a long
> series of code reviews, and the **Python unification** (§7) that removed the `ScriptRuntime`
> singleton and moved Python off its former isolate-per-instance model onto the shared model.
> A provenance map for git archaeology lives in the [Appendix](#appendix-hardening-provenance).
> The product-facing `DataWeave` classes are pre-GA, so several internal contracts (async Node
> `cleanup()`, the removed `*_with_resolver` and legacy-singleton C ABI) changed during this work
> without a compatibility ceremony.

## 1. Goal

Let multiple `DataWeave` instances coexist in one process — in **either** binding — each with its
own module resolver and script cache, so that different resolvers never collide. Before this
change the second `new DataWeave({ resolveModule })` in a Node process silently kept the first
instance's resolver, and Python achieved isolation only by paying for a whole GraalVM isolate per
instance. The isolation must hold with instances created, run, and torn down concurrently
(including across Node Worker threads and Python worker threads), without leaking native resources
or wedging the shared GraalVM isolate. A secondary goal, realized by the 2026-08-26 unification, is
that both bindings drive **one** shared Java engine layer through the **same** C ABI, so there is a
single mental model and a single source of truth to maintain.

## 2. Background

`native-lib`'s `ScriptRuntime` (`native-lib/src/main/java/org/mule/weave/lib/ScriptRuntime.java`)
was a `static final` singleton holding one `engine` and a **write-once** `static volatile resolver`.
`setResolver` refused to run a second time per process (logged a warning and returned). Every
`@CEntryPoint` in `NativeLib.java` routed through `ScriptRuntime.getInstance()`. So two
`DataWeave` instances in one process could not have independent module sets — whichever called a
resolver-backed `run()` first won.

**This is not a GraalVM constraint.** `native-cli`'s `NativeRuntime`
(`native-cli/src/main/scala/org/mule/weave/dwnative/NativeRuntime.scala:50-60`) already builds one
independent `DataWeaveScriptingEngine` + `CompositeWeaveResourceResolver` per instance — there is
no shared static state there. GraalVM Java statics are scoped per-isolate, which is also why the
Python binding — which **originally** used one GraalVM isolate per `DataWeave()` instance — got
resolver isolation "for free." The limitation was specific to `native-lib`'s deliberate Java static
singleton plus the Node C addon's global resolver bridge.

**Why unification followed.** The Node change (this PR's original scope) fixed Node but, for
backward compatibility, kept the `ScriptRuntime` singleton (`getInstance()`/`defaultInstance`) and
three legacy singleton C entrypoints (`run_script`, `run_script_callback`,
`run_script_input_output_callback`) because the Python binding still used them. A later rebase
exposed a collision: master had shipped a Python module-resolver feature calling a
`run_script_with_resolver` entrypoint that this branch's ABI redesign removed. Rather than maintain
two isolation models (Python isolate-per-instance vs. Node shared-isolate + handles) and a
compatibility shim, the maintainer chose to **unify**: remove the singleton entirely and put both
bindings on the shared-isolate + handle-addressed-engine model through the identical `*_engine`
ABI (§7).

## 3. Scope

**In scope:**
- `native-lib` Java layer: turn `ScriptRuntime` from a static singleton into a handle-addressable
  registry of instances, each with its own engine + resolver; **remove** `getInstance()` /
  `defaultInstance` so `ScriptRuntime` is purely handle-addressed.
- Node C addon (`native-lib/node/src/addon.c`): per-handle resolver bridge state instead of one
  process-global bridge, plus the concurrency & lifecycle machinery in §6.
- Node TypeScript layer (`ffi.ts`, `dataweave.ts`, `stream.ts`, `reader.ts`): each `DataWeave`
  instance owns an engine handle for its whole lifecycle.
- Python layer (`native-lib/python/src/dataweave/{native,runtime,models}.py`): move off
  isolate-per-instance and off the legacy singleton onto a module-level reference-counted shared
  isolate with one engine handle per `DataWeave` instance (§7). The public Python API is unchanged.

**Out of scope:**
- Separate GraalVM isolates per engine — rejected as the isolation mechanism (see §5).
- Solving streaming/transform + **custom-module** resolution across the background-thread
  boundary. Streaming against a resolver-backed engine still fails closed (returns "not found")
  for custom modules reached from a background worker thread, in **both** bindings; built-in
  modules continue to resolve normally in all cases. This is a pre-existing, documented hazard,
  deliberately kept identical across bindings, not introduced here.

## 4. Definitions

- **Isolate** — the single process-wide GraalVM isolate. All engines share it. In Node its lifetime
  is governed by `g_ref_count` (§6.1); in Python by `_isolate_ref_count` (§7).
- **Engine** — a `ScriptRuntime` Java object (own resolver + compiled-script cache) addressed by
  an opaque `long long` handle. Many engines per isolate.
- **Init reference / isolate ref** — the reference-count unit a binding acquires per engine on
  `initialize()` and releases on `cleanup()` (or on owner death). Distinct from an engine handle.
- **Op** — one in-flight `run()`/`runStreaming()`/`runTransform()` native call.
- **Owner env / owner thread** — the `napi_env` (and its JS thread) that created a given Node engine
  or init reference. `napi_env`/`napi_ref`/`napi_deferred`/`napi_threadsafe_function` are
  thread-affine; env-affine calls only ever happen on the owner thread. (Python has no `napi_env`;
  its thread model is §7.)

## 5. Alternatives Considered (isolation mechanism)

**Separate GraalVM isolates per engine (rejected).** The most complete isolation (own heap, own
JIT, own Java statics), and what Python originally did per-instance. Rejected as the unifying model
for two reasons:

- **Node cannot cheaply move to isolate-per-engine.** `addon.c` assumed exactly one isolate as
  global state; supporting N isolates means restructuring all of that into per-handle structs, and
  isolate teardown is fragile — `graal_tear_down_isolate` blocks until every attached thread
  reaches a safepoint, and Node's streaming workers deliver chunks via a `napi_threadsafe_function`
  that needs the libuv event loop to keep running. Multiplying that per-isolate is strictly worse
  and discards the hardening work in §6.
- It is unnecessarily heavy for the actual need: independent module resolution and script caching,
  not full JVM-level sandboxing.

**Chosen: object-level engines in one shared isolate (for both bindings).** Multiple `ScriptRuntime`
Java objects, each with its own resolver and compiled-script cache, all in the single existing
GraalVM isolate, addressed by an opaque handle. Mirrors what `native-cli` already does and requires
no change to isolate lifecycle management for the *feature*. Node requires the careful
reference-and-teardown coordination in §6 because the isolate is shared by independently created and
destroyed engines across threads. **Python can adopt the same model trivially**: its ctypes calls
are synchronous and it owns its stream-worker threads directly, so it needs none of Node's
`PENDING_WAIT`/adoption/retry machinery — just a reference count and a synchronous
drain-before-teardown (§7).

**Accepted trade-off (Python).** Python instances in one process now share one isolate's heap
instead of having separate heaps. This is weaker memory isolation, relevant only if
mutually-untrusted scripts run in one process expecting heap-level separation. The maintainer
accepted this in exchange for a single maintained model.

## 6. Node Concurrency & Lifecycle Model

This section governs how the shared isolate, per-engine registry entries, and in-flight ops
coordinate in the **Node** binding so that no thread ever attaches to, executes on, or resolves a
module against a torn-down isolate or a freed engine record, and no native resource leaks — under
concurrent creation, execution, abandonment (env death without `cleanup()`), and teardown across
Worker threads. (Python's simpler model is §7.)

All shared C state is read and written **only under `g_mutex`**, with two documented exceptions:
the cheap top-of-function `!g_initialized` fast-path read (a benign optimization; the
authoritative check is under the lock), and the lock-free `g_isolate` NULL-check that narrows a
window before a guarded re-check.

### 6.1 The reference-ownership invariant

The isolate lives while any env holds an init reference. The governing invariant is:

> **`g_ref_count` == Σ `init_refs` over all live per-env records.**

`g_ref_count` is a derived total, not a bare global that any code path may drive to zero.
Reference accounting is **per `napi_env`**, tracked in a `g_mutex`-guarded linked list of
`env_init_rec_t { napi_env env; int init_refs; next; }`:

- **`initialize()`** acquires one init reference *on the calling env's record* (find-or-create the
  record, `init_refs++`, `g_ref_count++`, both under the same lock). The record registers exactly
  one env-death hook (`env_init_cleanup`) on first creation.
- **`cleanup()`** releases one reference **only if the calling env owns one** (`init_refs > 0`).
  A `cleanup()` with no matching `initialize()` on that env, or a double-`cleanup()`, is a no-op
  that resolves immediately — it must never steal another env's reference and tear the isolate
  down under a live user.
- **Env death** (`env_init_cleanup`, an env-cleanup hook) releases *all* of that env's remaining
  references at once, from a single env-scoped decision point. This is what reclaims an abandoned
  Worker that exited without calling `cleanup()`.
- **`destroyEngine` never releases an init reference** — engines and init references have distinct
  lifetimes (Java registry entry vs. isolate). The product `doCleanup()` calls `destroyEngine`
  then `ffi.cleanup()`; the latter is the sole release.

Because every release is keyed on a specific env's balance, an abandoned env-A can only reach
`g_ref_count == 0` when no other env holds a reference — so it can never tear down the isolate
under a live env-B. This closes both the cross-env abandonment UAF and the symmetric
over-`cleanup()` UAF.

The three `g_ref_count` mutators after this design are: the three `initialize()` acquire sites
(adoption / already-initialized fast path / create path), `release_isolate_ref_locked` (the
`cleanup()` path), and `env_init_cleanup` (env death, via the bounded multi-release helper
`isolate_ref_release_n_locked(n)`, which makes the reached-zero teardown decision *at most once*
regardless of how many references it drops).

### 6.2 The teardown state machine

When a release drops `g_ref_count` to 0, the isolate must be torn down — but only after every
in-flight op has drained, because `graal_tear_down_isolate` blocks until every GraalVM-attached
worker thread detaches, and those workers deliver chunks via a `napi_threadsafe_function` that
needs the JS event loop to run. A naïve synchronous join-on-teardown from the JS thread
therefore **deadlocks**: JS thread waits for teardown → teardown waits for the worker to detach →
the worker waits for the JS thread to run its chunk callback.

The resolution is a `g_active_ops` counter (all in-flight ops, every engine, every thread) plus a
tri-state machine, all under `g_mutex`:

```
TEARDOWN_NONE          no teardown queued or running.
TEARDOWN_PENDING_WAIT  a reached-zero release queued a teardown; a detached waiter thread is
                       blocked on `while (g_active_ops > 0 && !g_teardown_cancelled)`. The
                       isolate is STILL LIVE here — a fresh initialize() may ADOPT it.
TEARDOWN_TEARING_DOWN  the waiter passed the point of no return and is in
                       graal_tear_down_isolate(). Adoption is unsafe; initialize() blocks
                       (deadlock-free, because g_active_ops is already 0 — nothing depends on
                       the JS loop).
```

- **Reached-zero release, `g_active_ops == 0`:** synchronous fast path — spawn+join
  `cleanup_thread_fn` inline (it attaches its own Graal thread, tears down, and reports
  success only when `graal_tear_down_isolate` returns 0), then clear
  `g_thread`/`g_isolate`/`g_initialized`/`g_ref_count`.
- **Reached-zero release, `g_active_ops > 0`:** set `TEARDOWN_PENDING_WAIT`, spawn the detached
  waiter thread, return a pending promise. Each op's completion sentinel decrements `g_active_ops`
  and broadcasts `g_teardown_cond`; when it reaches 0 the waiter publishes `TEARDOWN_TEARING_DOWN`
  (under the lock, the point of no return) and tears down.
- **Adoption (the deadlock fix):** an `initialize()` arriving in `TEARDOWN_PENDING_WAIT` sets
  `g_teardown_cancelled = true`, takes a fresh init reference, broadcasts, and returns — the
  waiter re-checks the flag, tears down nothing, and resolves every queued `cleanup()` promise
  anyway (from each caller's perspective the reference it dropped is gone, whether the isolate was
  physically destroyed or adopted by a newcomer is immaterial).
- **Multiple concurrent `cleanup()` calls** waiting on the same teardown each append a node
  `{env, deferred, tsfn}` to `g_teardown_waiters` — a *list*, because a second/third `cleanup()`
  can arrive from a different Worker env, and each thread-affine deferred must be resolved via its
  own env's tsfn on its own thread.

**Teardown-failure retry signal.** If a reached-zero teardown cannot be carried out — waiter
alloc/spawn fails, `fn_attach_thread` fails, or `graal_tear_down_isolate` returns nonzero — the
isolate is left live with `g_ref_count == 0` and no owner. Rather than fabricate a phantom
reference (which would violate the §6.1 invariant and the resolved `cleanup()` promise's
contract), a `g_mutex`-guarded `g_teardown_needed` **retry signal** is armed. It is *not* a
reference (never added to any count). It is cleared when the isolate is actually torn down or
adopted. Retry runs at two natural, already-locked points: each op-completion drain (once
`g_active_ops` reaches 0), and the top of the next `napi_initialize` (before adoption, so a
pending teardown is honored rather than silently discarded). On a nonzero-return teardown the
helper threads also **detach** their local IsolateThread before exiting (the isolate is still
live; exiting attached would leave a phantom thread that blocks later retries). The documented,
accepted residual: if teardown fails *and* no later `initialize()` or op ever occurs, the isolate
lingers until process exit — benign (one process-lifetime isolate, no invariant violation), the
deliberate tradeoff for not adding event-loop-affine async retry infrastructure to this code.

### 6.3 Per-engine records, admission pinning, and deferred destroy

Every engine — resolver-backed **and** resolver-less — gets a per-engine record
(`engine_bridge_t`) at creation, linked into `g_bridges`, carrying `handle`, `in_flight`,
`destroy_pending`, `deferred_registry_remove`, and (for resolver-backed engines only)
`resolver_js`/`env`/`owner`/`results`. Resolver-less records leave those resolver fields
zero/NULL. `in_flight` (per-handle registry drain) and `g_active_ops` (global isolate teardown)
are **distinct counters**, never merged.

**Admission pins the engine atomically.** Each of the three run paths reserves `g_active_ops`
*and* pins the engine (`in_flight++` via `bridge_begin_op_locked`) in the **same** critical
section as the lifecycle check, before any window a concurrent `destroyEngine` could use:

```c
uv_mutex_lock(&g_mutex);
if (!g_initialized || (g_teardown_state != TEARDOWN_NONE && !g_teardown_cancelled)) {
  uv_mutex_unlock(&g_mutex);
  /* free partials */  napi_throw_error(env, NULL, "Not initialized. Call initialize() first.");
  return NULL;
}
g_active_ops++;
w->bridge = bridge_begin_op_locked(handle);   // NULL for unknown handle -> worker surfaces the envelope
uv_mutex_unlock(&g_mutex);
```

- **Streaming / transform** reserve *early* (arg extraction is cheap relative to the async op) and
  release via the completion sentinel on the worker thread. Every early-return between admission
  and worker spawn (conversion error, OOM, tsfn/promise-create failure, spawn failure) unwinds
  **both** `g_active_ops` and the engine pin.
- **Synchronous `run()`** reserves *late* — immediately before `fn_attach_thread`, so the
  reservation spans exactly the isolate-touching window (attach→detach) with only two unwind
  sites (attach-failure and normal completion); the string mallocs and arg extraction don't touch
  the isolate.
- **`createEngine` / `createEngineWithResolver`** likewise do their lifecycle check + a transient
  `g_active_ops` reservation in one critical section, and additionally require that the **calling
  env owns an init reference** (`init_refs > 0`) — an env that never initialized must not create
  engines on the shared isolate.

With the pin taken under the admission lock, a concurrent `destroyEngine` either runs entirely
before admission (the handle is already gone → worker surfaces `Unknown engine handle`, no freed
access) or entirely after (`in_flight > 0` → destroy defers). There is no interleaving where an
admitted op observes a freed bridge.

**Deferred destroy.** `napi_destroy_engine`, under `g_mutex`: if `in_flight > 0`, set
`destroy_pending` and defer the Java-registry removal (`fn_destroy_engine`); the last op to drain
performs it on completion. If `in_flight == 0`, remove now. `fn_destroy_engine` is called
**exactly once** per handle (immediate xor deferred, never both), and it attaches its own fresh
Graal thread so it is safe to call from the completion sentinel or directly.

**The registry-removal step is itself teardown-guarded.** Removing the Java registry entry
touches the isolate (`fn_attach_thread(g_isolate, …)`), so it is split into
`bridge_finalize_registry` — which takes its **own** transient `g_active_ops` reservation, gated
on `g_teardown_state != TEARDOWN_TEARING_DOWN && g_isolate != NULL` in the *same* critical section
as the increment — and `bridge_finalize_free` (napi_ref deletion, still resolver-gated and on the
owner thread; result-buffer free; `free`). This closes the race where a deferred finalize could
attach to an isolate the waiter is destroying, without re-opening the completion-path
coordination: the op's own `g_active_ops--` stays on the worker thread; the finalize takes a fresh
short-lived reservation only around the attach, makes no env-affine or JS-loop-dependent call, and
never holds it across a JS callback (so it cannot re-introduce the §6.2 deadlock).

**Env cleanup hooks reclaim abandoned engines.** Every engine registers a
`napi_add_env_cleanup_hook` at creation (checked for failure — creation is all-or-nothing; on
hook-registration failure the record is unlinked, its registry entry removed, its init reference
released, and the create throws with no usable handle escaping). When the owner env dies without
`destroyEngine`, the hook removes the Java registry entry and frees the record. Because every
engine now carries an env-affine hook, the **owner-thread `destroyEngine` guard fires for any
record** (not only resolver-backed ones): `napi_remove_env_cleanup_hook` is valid only on the
owner env, so an engine is destroyable only from its creating thread. Node runs env-cleanup hooks
LIFO, and the per-env init-record hook is registered on the *first* `initialize()` (before any
engine) — so at env death every per-engine `bridge_env_cleanup` runs (isolate still alive) before
`env_init_cleanup` releases the isolate reference(s). Ordering preserved.

### 6.4 JS instance lifecycle

`DataWeave` models three states — `"uninitialized" | "ready" | "cleaning-up"` — not a boolean,
because a boolean cannot represent the window during which `cleanup()` has started but
`ffi.cleanup()` has not settled:

- **`initialize()`** — `ready` → no-op; `cleaning-up` → **throws** `DataWeaveError("Cannot
  initialize while cleanup is in progress; await cleanup() first.")`; `uninitialized` → does the
  load/create-engine work, `state = "ready"` on success. On engine-creation failure *after*
  `ffi.initialize()` succeeded, the rollback release is modeled as pending state: `state` goes
  `cleaning-up` and `cleanupPromise` is assigned the `ffi.cleanup()` rollback (with a
  `.catch(() => {})` so an un-awaited rollback never becomes an unhandledRejection), so a
  concurrent `initialize()` is deterministically rejected instead of racing a fresh isolate
  against the in-flight release. `initialize()` and `run()` stay **synchronous** (an async
  signature would be an API break).
- **`run()` / `runStreaming()` / `runTransform()`** — gated by `ensureReady()`: throw
  `DataWeaveError` unless `state === "ready"`, so the internal `engineHandle === null` cleanup
  window is unreachable by any public method (defense-in-depth behind the C admission check).
  `runTransform` additionally re-checks `ensureReady()` **after** `await createChunkReader(input)`
  (async input pre-buffering can span arbitrary time; the instance may be cleaned up during it) so
  a misused instance gets a synchronous `DataWeaveError` rather than a resolved `Unknown engine
  handle` envelope.
- **`cleanup()` / `doCleanup()`** — set `state = "cleaning-up"` synchronously *before*
  `ffi.destroyEngine` / `ffi.cleanup` (the key ordering). It always runs `await ffi.cleanup()`
  even if `destroyEngine()` throws (a real path — wrong-thread destruction throws synchronously),
  so a throwing destroy cannot strand this env's init reference; the destroy error is re-thrown
  after the release. `engineHandle` is cleared regardless so a retry cannot double-destroy.
  Overlapping calls coalesce on `this.cleanupPromise` (one native teardown). `cleanup()` returns
  `Promise<void>`.

**Module-level convenience API** (`run`/`cleanup`) drives a lazily-created singleton:
- `getGlobalInstance()` initializes a **local candidate** and publishes `globalInstance` only after
  `initialize()` succeeds — a failed first init leaves the singleton null so the next call retries
  cleanly, instead of poisoning it into permanent "not initialized".
- Process exit hooks (`beforeExit` async-drains, `exit` best-effort sync) are registered **once
  per process** (module-scoped `exitHooksRegistered`, never reset), not per singleton, so
  init→cleanup→reinit cycles don't accumulate listeners. `exit` is documented as best-effort:
  Node does not emit it for termination signals (SIGTERM/SIGKILL) or all fatal modes; callers
  needing guaranteed graceful shutdown register and await their own signal handlers.
- Module-level `cleanup()` coalesces overlapping calls via a module-scoped `cleanupPromise` (it
  nulls `globalInstance` synchronously so new work builds a fresh instance, but overlapping
  `cleanup()`s await the same drain and resolve only when native teardown finishes).

### 6.5 Robustness of native allocation and streaming

- **OOM safety.** Every allocation in the streaming/transform setup, worker, and callback paths
  (`calloc`/`malloc`/`strdup`/`memcpy`, and every `napi_create_string_utf8`/
  `napi_create_threadsafe_function`/`napi_create_promise`) is NULL/status-checked before use.
  Setup-phase failures throw a synchronous `napi_throw_error(env, NULL, "OOM")` (matching
  `napi_run_script_engine`) and unwind `g_active_ops` + the engine pin with no double-free
  (`calloc`-zeroed `w` makes the free-set `free(NULL)`-safe). Worker-thread OOM produces a
  **terminal error JSON result** (a static `{"success":false,"error":"Out of memory"}` string when
  the copy itself failed, flagged so it is never `free()`d), never a hung promise.
- **Argument validation.** Every FFI-facing entrypoint checks the status of every
  `napi_get_value_*` conversion (handle `int64`, string size-probes and fills, `napi_typeof` for
  nullable args) and throws before using the converted value, so a raw addon caller cannot turn a
  malformed argument into an uninitialized native input. `inputCharset` is nullable
  (`string | null | undefined`); any other type is rejected rather than silently coerced.
- **Stream error propagation.** `streamFromNative` handles **both** settlement branches of the
  native `start()` promise: on rejection it records the error, marks completion, and wakes every
  parked `next()` consumer (otherwise the generator hangs forever and the rejection is unhandled),
  then re-throws after draining any chunks that arrived first. Rejection is tracked by a dedicated
  `startRejected` boolean, not a value sentinel, so `Promise.reject(undefined)` propagates
  correctly.

## 7. Python Lifecycle & Teardown Model

Python drives the **same** shared Java engine layer and the **same** `*_engine` C ABI as Node, but
its isolate/thread glue (`native-lib/python/src/dataweave/native.py`) is much simpler than §6:
ctypes calls are synchronous and Python owns its stream-worker threads directly, so it needs none
of Node's `PENDING_WAIT`/adoption/retry machinery — just a reference count and a synchronous
drain-before-teardown. The **public Python API is unchanged** by the unification.

### 7.1 Shared state and the reference-count invariant

Module-level state in `native.py`, all mutations under one module lock (`_isolate_lock`):
`_lib`, `_lib_path`, `_isolate` (the single process-wide isolate, or None), `_isolate_ref_count`.

> **Invariant:** `_isolate_ref_count` == number of live engines across all `DataWeave` instances,
> and the isolate exists iff the count > 0.

Each `DataWeave` instance owns exactly one engine handle and contributes exactly one to the
refcount. The module lock guards only isolate refcount/create/teardown; it is **not** held during
script execution, so one engine's long-running script never blocks another engine's
`initialize()`/`run()`. Different instances can run concurrently, each on its own attached thread in
the shared isolate.

### 7.2 No persistent isolate-thread attachment (attach-on-demand)

`graal_tear_down_isolate` blocks forever waiting for every *other* GraalVM-attached thread to reach
a safepoint. If the isolate's creating ("bootstrap") thread stayed attached for the isolate's life,
a last-release teardown running on a *different* OS thread — e.g. an `atexit`/interpreter-shutdown
cleanup on the main thread after the first `run()` happened on a worker, or two instances torn down
from different threads — would block forever. The binding therefore holds **no persistent
attachment**, mirroring the Node and Go bindings:

- **`_acquire_isolate`** (first ref): `graal_create_isolate`, then **immediately
  `graal_detach_thread` on the bootstrap thread** (a nonzero return is surfaced as a
  `DataWeaveError`). No IsolateThread is retained.
- **Every synchronous native call** (`run`, `run_callback`, `run_input_output_callback`,
  `create_engine[_with_resolver]`, `destroy_engine`) attaches a **fresh** thread on demand, uses it
  for the whole call, and detaches it when done (`_current_thread_attachment`). A stream-worker
  thread that has already attached its own IsolateThread passes it through unchanged.
- **`_release_isolate`** (last ref): attaches a fresh thread solely to call
  `graal_tear_down_isolate`, then clears the globals.

Because nothing stays attached between calls, teardown never blocks on a phantom attachment
regardless of which OS thread performs the last release.

### 7.3 Instance lifecycle

- **`initialize()`** — under the lock, `_acquire_isolate` (create-on-first-ref + bootstrap detach,
  `_isolate_ref_count += 1`); then `create_engine()` or `create_engine_with_resolver(ctx, trampoline)`,
  storing the returned `handle` on the instance. If `create_engine` fails after the isolate ref was
  taken, the instance releases the ref (tearing down if it was the only one) and — when a resolver
  was installed before `initialize()` — unregisters its resolver token, so a failed init leaks
  nothing (neither an isolate ref nor a `_resolver_registry` entry).
- **`run` / `run_streaming` / `run_callback` / `run_transform`** — route through the `*_engine`
  entrypoints with the instance's `handle`, per §7.2's attachment rules. Per-instance execution is
  serialized (`_serialized_native_operation`); different instances run concurrently.
- **`cleanup()`** — drain *this instance's* stream workers (signal cancel + **join** the threads;
  synchronous, Python owns them, so no event loop and no deadlock); `destroy_engine(handle)`; remove
  the resolver-map entry; clear the instance handle; then release the isolate ref (`-= 1`), tearing
  the isolate down on the last release. `cleanup()` on an uninitialized/already-cleaned instance is a
  no-op; double-`cleanup()` releases the ref only once (guarded by the instance handle being
  cleared). If `destroy_engine` throws, the isolate ref is still released so a throwing destroy
  cannot strand the isolate; the error is re-raised after the release.

**Why this stays simple:** teardown happens only on the *last* release, by which point every
instance has already joined its own workers, so the isolate has no attached worker threads when
`graal_tear_down_isolate` runs.

### 7.4 Resolver dispatch and the streaming/resolver hazard

- `create_engine_with_resolver` passes an opaque `ctx` (a Python-allocated monotonic token
  registered in `_resolver_registry[token] = self` *before* the create call, so no resolve callback
  can fire for a handle before its map entry exists). Python registers **one** C trampoline
  (`RESOLVE_MODULE_CALLBACK`); GraalVM calls it with `(thread, ctx, module_path)`, and it dispatches
  to the engine's Python resolver via the registered token, returning the source-buffer pointer.
  This is the Python analog of Node's per-handle bridge — same `ctx` concept, identical Java/ABI
  side.
- **Streaming / transform + custom modules — parity with Node (out of scope):** the trampoline
  resolves custom modules only when invoked on the engine's owner thread and **fails closed**
  ("not found") on a background stream-worker thread. Built-in modules resolve normally everywhere;
  synchronous `run()` with a resolver resolves custom modules fully. This is a conservative parity
  choice (identical behavior across bindings), not a hard Python limitation.

## 8. Architecture (layer map)

### Layer 1 — Java (`native-lib/src/main/java/org/mule/weave/lib/`) — shared by both bindings

- **`ScriptRuntime.java`** — from static singleton to per-instance + a
  `ConcurrentHashMap<Long, ScriptRuntime>` registry with `register`/`get`/`destroy` and an
  `AtomicLong` handle allocator. The resolver is bound once at construction (immutable for the
  instance's lifetime); the `static setResolver` write-once mutation is removed.
  `compositeResolver()` / `createModuleComponentsFactory()` become instance methods.
  `getInstance()` / `defaultInstance` are **removed** — `ScriptRuntime` is purely handle-addressed.
- **`CallbackWeaveResourceResolver.java`** — stores a `PointerBase ctx` alongside the callback,
  forwarded on every `callback.invoke(...)`; constructor `(ResolveModuleCallback, PointerBase ctx)`.
- **`NativeCallbacks.java`** — `ResolveModuleCallback` is the 3-arg ctx form
  (`invoke(IsolateThread, PointerBase ctx, CCharPointer modulePath)`), mirroring the existing
  `WriteCallback`/`ReadCallback` ctx idiom. This is what lets one shared native callback dispatch
  to the correct per-handle resolver on the C/Python side. The old 2-arg form is gone.
- **`NativeLib.java`** — exposes only the handle-based lifecycle + execution entrypoints
  (`create_engine`, `create_engine_with_resolver`, `destroy_engine`, `run_script_engine`,
  `run_script_callback_engine`, `run_script_input_output_callback_engine`) resolving via
  `ScriptRuntime.get(handle)`. The three legacy singleton entrypoints (`run_script`,
  `run_script_callback`, `run_script_input_output_callback`) and the old `*_with_resolver`
  entrypoints are **removed** (see §10).

### Layer 2 — Node C addon (`native-lib/node/src/addon.c`)

- Per-handle resolver bridge state in `g_bridges` (§6.3) instead of a process-global bridge.
- **Resolver dispatch:** `createEngineWithResolver` passes the bridge record's address as the
  `ctx`; when Java invokes `resolve_module_callback(thread, ctx, path)`, C casts `ctx` back to the
  bridge and calls its JS resolver **synchronously on the JS thread** (no
  `napi_threadsafe_function` — the create call runs synchronously on the calling JS thread, so the
  original deadlock rationale still holds). A per-handle `owner`-thread guard fails closed to "not
  found" if `resolve_module_callback` is reached from a non-owner thread (e.g. a streaming worker).
- All of §6's machinery: `g_active_ops`, the `TEARDOWN_*` state machine, `g_teardown_cancelled`,
  `g_teardown_needed`, the per-env `g_env_recs` list, the `g_bridges` list, admission pinning, and
  the split finalize. The legacy `dw_napi_run_script` path and its `run_script` dlsym are removed.
- N-API methods: `createEngine`, `createEngineWithResolver`, `destroyEngine`, and handle-taking
  `runScriptEngine`, `runScriptStreamingEngine`, `runScriptTransformEngine`.

### Layer 3 — Node TypeScript (`native-lib/node/src/`)

- **`ffi.ts`** — `createEngine`, `createEngineWithResolver`, `destroyEngine`, and handle-taking
  `runScriptEngine`, `runScriptStreamingEngine`, `runScriptTransformEngine`. `runScript` /
  `runWithResolver` removed.
- **`dataweave.ts`** — `DataWeave` owns a `private engineHandle`, the three-state lifecycle
  machine, and the module-level singleton/exit-hook/coalescing logic (§6.4). `initialize()` calls
  `ffi.createEngineWithResolver(this.resolveModule)` or `ffi.createEngine()`; run methods route
  through the handle-based FFI (one code path per method, parameterized by handle);
  `cleanup()` calls `ffi.destroyEngine` then `ffi.cleanup`.
- **`stream.ts`** — `streamFromNative` error propagation (§6.5). **`reader.ts`** —
  `createChunkReader` pre-buffers async inputs (the native read callback is synchronous and cannot
  await), which is why `runTransform` re-checks readiness after it.

### Layer 4 — Python (`native-lib/python/src/dataweave/`)

- **`native.py` (`NativeRuntime`)** — the shared-model glue (§7): module-level refcounted isolate,
  attach-on-demand thread handling, the 3-arg ctx resolver trampoline + `_resolver_registry`, and
  the `*_engine` + `create_engine[_with_resolver]` + `destroy_engine` symbol bindings.
- **`runtime.py` (`DataWeave`)** — `initialize()` acquires an isolate ref + creates one engine and
  stores its `handle`; run methods route through the `*_engine` entrypoints with that handle;
  `cleanup()` drains this instance's stream workers, `destroy_engine(handle)`, releases the ref.
  The public API surface is unchanged.
- **`models.py`** — `RESOLVE_MODULE_CALLBACK` ctypes signature carries the `ctx` argument.

## 9. Data Flow

**Node:**
```
new DataWeave({ resolveModule: A }).initialize()
  → ffi.initialize()                       // env init record for this env: init_refs 0→1, g_ref_count++
  → ffi.createEngineWithResolver(A)
    → addon.c: allocate bridge_A { env, ref to A, owner=thisThread, in_flight:0 }; register env hook
    → Java: new CallbackWeaveResourceResolver(callback, ctx=&bridge_A);
            new ScriptRuntime(resolver) → handle_A = register(rt)
  → handle_A stored as this.engineHandle

dwA.run(script importing "custom/lib.dwl")
  → ffi.runScriptEngine(handle_A, script, inputs)
  → addon.c: admission (g_active_ops++, in_flight++ on bridge_A) → attach → fn_run_script_engine
  → Java: ScriptRuntime.get(handle_A).run(...)
      composite resolver: ClassLoader miss → callback.invoke(thread, ctx=&bridge_A, "custom/lib.dwl")
  → C: resolve_module_callback casts ctx→bridge_A; thread==owner? yes → call resolver A synchronously
  → result flows back, script compiles; on completion: in_flight--, g_active_ops--

new DataWeave({ resolveModule: B }).initialize()  →  handle_B, bridge_B (independent resolver + owner)
dwB.run(...)  →  resolves via resolver B only; A's cache untouched; no cross-talk
```

**Python:**
```
dwA = DataWeave(resolve_module=A); dwA.initialize()
  → lock: _isolate None → graal_create_isolate() + detach bootstrap thread; ref 0→1
  → create_engine_with_resolver(ctx=tokenA, trampoline); registry[tokenA]=dwA; dwA._handle = handleA

dwB = DataWeave(resolve_module=B); dwB.initialize()
  → lock: _isolate exists → reuse; ref 1→2
  → create_engine_with_resolver(ctx=tokenB, trampoline); registry[tokenB]=dwB

dwA.run("... import custom/lib ...")
  → attach a fresh thread on demand → run_script_engine(handleA, script, inputs) → detach
  → Java engine A: ClassLoader miss → callback(thread, ctx=tokenA, "custom/lib")
  → trampoline: registry[tokenA] → resolver A → source; A's cache used, B untouched

dwA.cleanup()  → join dwA workers; destroy_engine(handleA); ref 2→1 (isolate stays)
dwB.cleanup()  → join workers; destroy_engine(handleB); ref 1→0 → attach fresh thread + graal_tear_down_isolate(); _isolate=None
```

## 10. Error Handling & Backward Compatibility

- **Module not found / resolver throws:** resolver returns `null`/non-str → composite resolver
  falls through → standard DataWeave "unable to resolve module" error (unchanged, scoped
  per-handle).
- **Wrong-thread resolver invocation:** per-handle/per-token `owner` check fails closed to "not
  found" rather than touching the host callback cross-thread — identical in both bindings.
- **Invalid/unknown/destroyed handle:** `ScriptRuntime.get(handle)` returns null → the entrypoint
  returns `{"success":false,"error":"Unknown engine handle"}` (resolved for async ops, returned as
  the JSON string for sync `run()`), never an NPE/crash.
- **Node admission / argument / allocation failures:** synchronous `napi_throw_error` (generic
  Error); worker-thread OOM → terminal error JSON. Never `napi_reject_deferred` (absent from
  `addon.c`).
- **Python init failures:** isolate-create failure → `DataWeaveError`, refcount not incremented,
  `_isolate` stays None; `create_engine` failure after isolate create → release the ref (tearing
  down if this call created it) and unregister any resolver token, then raise. `run`/stream after
  `cleanup()` → instance guard raises `DataWeaveError` (handle already cleared).
- **Teardown failure (Python `graal_tear_down_isolate` returns nonzero):** surface a warning and
  re-raise `DataWeaveError`, and clear the isolate globals (`_lib`/`_lib_path`/`_isolate` → None,
  count already 0) so the next `initialize()` builds a fresh isolate rather than reusing one whose
  teardown just failed.
- **Intended breaking changes (pre-GA, no shims):** the dwlib C ABI drops the exported
  `run_script` / `run_script_callback` / `run_script_input_output_callback` legacy singleton
  entrypoints **and** the `run_script[...]_with_resolver` entrypoints, keeping only the `*_engine`
  + `create_engine[_with_resolver]` + `destroy_engine` set; `ResolveModuleCallback` is 3-arg only;
  Java `getInstance()`/`defaultInstance` are removed. dwlib is consumed by this repo's own Python
  and Node bindings in lockstep. Node `DataWeave.cleanup()` changed from `void` to `Promise<void>`.
  The **Python public API is unchanged** — only `native.py`'s internal ABI changed.

## 11. Testing Strategy

- **Java unit** (`native-lib:test`): two `ScriptRuntime` instances with different in-memory
  resolvers each resolve only their own module; `destroy()` removes an instance; `getInstance()`
  tests removed. (The `@CEntryPoint` methods can't be driven from a hosted JVM — GraalVM word types
  don't box — so handle-based entrypoint coverage lives at the binding integration layers.)
- **Node integration** (`native-lib:nodeTest`, real addon, `vi.mock` of `ffi` forbidden): the core
  W-23692110 regression (two independent resolvers in one process); unknown/destroyed-handle
  envelopes for all three run paths; the deadlock regression (active stream + `cleanup()` +
  concurrent `run()` resolves within a bounded timeout); same-instance lifecycle; ref-count-proxy
  teardown assertions; and `worker_threads` Worker lifecycle including **normal Worker exit without
  `cleanup()`** (the abandonment / init-reference-release proof), `Worker.terminate()` mid-life,
  and explicit in-Worker `cleanup()`.
- **Node unit** (`ffi` mocked, no dwlib): `DataWeave.initialize()` ref-count/rollback safety;
  module singleton poisoning recovery; module + instance `cleanup()` coalescing; `stream.ts`
  rejection propagation; `runTransform` post-pre-buffer re-check; `doCleanup()` releasing the init
  reference even when `destroyEngine` throws.
- **Python unit** (fake/mocked lib, no dwlib): refcount create/reuse/last-release-teardown;
  attach-on-demand thread accounting (bootstrap detached after create; every op attaches+detaches
  its own thread; teardown attaches a fresh thread); ctx→resolver trampoline dispatch (two handles →
  two resolvers); `cleanup()` idempotency + double-cleanup; `create_engine`-failure rollback
  releasing the ref; failed resolver-backed init unregistering the token.
- **Python integration** (real dwlib): the core W-23692110 regression (two instances, different
  resolvers, no cross-talk); multi-instance refcount teardown; synchronous `run()` resolving custom
  modules; streaming/transform still stream; streaming custom-module resolution fails closed
  (parity); a **foreign-thread last-release no-hang** regression (init on a worker thread, last
  release/cleanup on a different thread, bounded timeout); TCK conformance stays green.
- **Documented posture on non-forceable paths (Node).** Allocator/N-API fault injection and exact
  cross-thread teardown interleavings are **not deterministically forceable** from JS/vitest (no
  addon-boundary fault-injection hook — deliberately not added, YAGNI/test-only surface). Their
  correctness rests on the C-level invariants in §6, verified by code reasoning and adversarial
  review; the Worker tests are best-effort probabilistic guards. This is a standing, documented
  decision.
- **Native image build** (`native-lib:nativeCompile`) stays green with the legacy entrypoints
  removed (confirms no SPI/reflection config referenced them).

## 12. Engine lifecycle contract (shared by both bindings)

These invariants are the shared artifact both `native-lib/node/src/addon.c` and
`native-lib/python/src/dataweave/native.py` implement. Any binding on the `*_engine` C ABI must
uphold all six:

1. One process-wide isolate; engines are handle-addressed objects in the Java registry.
2. The isolate is reference-counted; the refcount equals the number of live engines; the isolate
   exists iff the refcount > 0.
3. Create-on-first-ref, tear-down-on-last-release; the binding calls
   `graal_create_isolate` / `graal_tear_down_isolate` from *outside* the isolate, and holds no
   thread persistently attached across calls (so teardown never blocks on a phantom attachment).
4. Each engine handle is created by `create_engine` / `create_engine_with_resolver` and destroyed
   by `destroy_engine`.
5. Resolver dispatch is per-engine via the opaque `ctx` echoed to the 3-arg `ResolveModuleCallback`;
   custom-module resolution fails closed off the engine's owner thread.
6. A failed engine-create rolls back the isolate ref; a throwing `destroy_engine` still releases
   the ref.

## 13. Follow-Up Work

- **Streaming/transform + custom-module resolution** across the background-thread boundary remains
  a separate, not-yet-scoped effort in both bindings (unrelated to the singleton fix). Because
  Python callbacks hold the GIL, Python *could* later support this as a Python-specific enhancement;
  kept out of scope here to preserve one unified behavior.

## References

| Item | Location |
|------|----------|
| GUS ticket | W-23692110 |
| Singleton root cause | `native-lib/src/main/java/org/mule/weave/lib/ScriptRuntime.java` |
| CLI's per-instance pattern (proof it's not a GraalVM constraint) | `native-cli/src/main/scala/org/mule/weave/dwnative/NativeRuntime.scala:50-60` |
| WriteCallback/ReadCallback ctx idiom | `native-lib/src/main/java/org/mule/weave/lib/NativeCallbacks.java` |
| Java engine registry / entrypoints | `native-lib/src/main/java/org/mule/weave/lib/{ScriptRuntime,NativeLib,NativeCallbacks}.java` |
| Node concurrency & lifecycle machinery | `native-lib/node/src/addon.c` |
| Node JS lifecycle / singleton / exit hooks | `native-lib/node/src/dataweave.ts` |
| Node stream error propagation | `native-lib/node/src/stream.ts` |
| Python isolate/engine glue | `native-lib/python/src/dataweave/{native,runtime,models}.py` |
| Node binding API + lifecycle docs | `native-lib/node/README.md`, `native-lib/node/docs/external-modules.md` |
| Original external-modules design | `docs/superpowers/specs/2026-08-04-nodejs-external-modules-design.md` |

## Appendix: Hardening provenance

The Node concurrency & lifecycle model (§6) converged over a series of code-review rounds, and the
Python unification (§7) was implemented and reviewed task-by-task; each round's decisions are folded
into the sections above. This map exists only for git archaeology — the per-round and Python
unification design documents were consolidated into this file.

| Round(s) | Area folded into | Decision |
|----------|------------------|----------|
| Feature (08-07) | §1–§5, §8–§10 | Object-level engines behind opaque handles; per-handle resolver bridge; ABI redesign. |
| 5 (08-11) | §6.2 | `cleanup()`-during-active-stream deadlock → async teardown + waiter thread + `TEARDOWN_*` adoption. |
| 6 (08-14) | §6.3, §6.4 | JS three-state lifecycle; atomic streaming/transform admission under `g_mutex`; handle-read validation. |
| 7 (08-18 ffi-sweep) | §6.3, §6.5, §10 | Atomic admission for sync `run()`; uniform `napi_get_value_*` status checks; docs await `cleanup()`. |
| 8 (08-18 oom-setup) | §6.5 | OOM-safe streaming/transform setup allocations. |
| 9 (08-18 engine/worker-oom) | §6.3, §6.5 | Deferred registry removal for all engines; worker/callback OOM → terminal result; N-API-create checks. |
| 10 (08-19 dangling-ctx) | §6.3, §6.4 | Env-cleanup removes the Java registry entry (`deferred_registry_remove`); shutdown-doc accuracy. |
| 11 (08-19 engine-pin) | §6.1, §6.3, §6.4 | Env hook + owner-guard for every engine; admission-time engine pin in all 3 paths; register-once exit hooks. |
| 12 (08-19 worker-ref-leak) | §6.1, §6.3, §6.4 | Init-reference release on abandoned env; teardown-guarded split finalize; module `cleanup()` coalescing; `runTransform` re-check; all-or-nothing engine creation. |
| 13 (08-20 per-env init) | §6.1 | Per-`napi_env` init-reference ownership; `g_ref_count == Σ init_refs`. |
| 14 (08-21 review5) | §6.2, §6.3 | Engine-creation admission requires an owned init reference; `g_teardown_needed` retry flag; `doCleanup()` releases the ref even when destroy throws. |
| 15 (08-21 review6) | §6.2, §6.4, §6.5 | Singleton-poisoning fix; stream rejection propagation; teardown return-code checks; init-driven stranded-teardown retry. |
| 16 (08-24 review7) | §6.2, §6.4, §6.5, §10 | Detach on failed teardown; init-hook-failure retry arming; observable init rollback; `Promise.reject(undefined)` fix; lifecycle-doc accuracy. |
| Python unification (08-26) | §2, §5, §7, §8 (Layer 1/4), §10–§12 | Remove `ScriptRuntime` singleton + 3 legacy C entrypoints; Python onto shared refcounted isolate + handle engines via `*_engine` ABI; 3-arg ctx resolver trampoline. |
| Python final review (08-26) | §7.2, §10 | Detach isolate bootstrap thread at create + attach-on-demand so cross-thread last-release teardown cannot hang; unregister resolver token on failed init. |
