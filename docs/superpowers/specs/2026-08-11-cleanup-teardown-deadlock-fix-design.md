# Fix `cleanup()`-During-Active-Stream Deadlock — Design

**Goal:** Eliminate a process-wide deadlock where calling `DataWeave.cleanup()` while any `runStreaming()`/`runTransform()` operation is still in flight (on any engine, in any thread) can freeze the process, by making isolate teardown wait for active operations to drain instead of blocking the JS thread they depend on.

**Architecture:** `napi_cleanup` becomes async: when it's the last release and no ops are active, it keeps today's synchronous spawn+join fast path unchanged. When ops are active, it defers teardown to a dedicated waiter thread that blocks on a condition variable until every op drains, then performs teardown and signals completion back into JS via a `napi_threadsafe_function` — the same pattern this addon already uses for streaming chunk delivery.

**Tech Stack:** N-API C addon (`napi_*`, `uv_thread`/`uv_mutex`/`uv_cond`), TypeScript (`DataWeave.cleanup()` signature change), vitest.

## Global Constraints

- Node binding only — do not touch `native-lib/python/**`.
- Legacy singleton entrypoints (`run_script`, `run_script_callback`, `run_script_input_output_callback`) and `ScriptRuntime.getInstance()` on the Java side are untouched by this fix; the bug and fix are entirely within `native-lib/node/src/addon.c` and `dataweave.ts`.
- Handle width stays C `long long` everywhere (unaffected by this fix, but any touched signature must not regress it).
- The existing per-bridge `in_flight`/`destroy_pending` accounting (F1 remediation, PR #157) is untouched — this fix adds a **separate, process-global** `g_active_ops` counter that covers all streaming/transform ops (resolver-backed or not), because isolate teardown blocks on *any* attached worker thread, not just resolver-backed ones.
- `DataWeave.cleanup()` signature changes from `void` to `Promise<void>` (async). This is acceptable pre-GA; no external ABI-stability commitment exists yet for the Node package.
- The module-level `process.on("exit", () => cleanup())` hook (`dataweave.ts:222`) stays fire-and-forget — not awaited. This is a pre-existing, acceptable tradeoff, not a new one.

---

## Background

### The bug

`napi_cleanup` (`addon.c:1189-1218`) decrements the process-global `g_ref_count`. When it drops to 0, it spawns a thread that calls `graal_tear_down_isolate`, then calls **`uv_thread_join` on that thread synchronously, blocking the calling JS thread** until teardown finishes.

`graal_tear_down_isolate` blocks until every GraalVM-attached thread reaches a safepoint/detaches. A `runStreaming()`/`runTransform()` background worker (`streaming_thread_fn`/`transform_thread_fn`) stays attached to the isolate for the duration of its native call, and delivers each chunk via `napi_call_threadsafe_function(..., napi_tsfn_blocking)`, which requires the JS event loop to run the corresponding `call_js_write`/`call_js_transform_write` callback before the worker can proceed.

If `cleanup()` is the call that drops `g_ref_count` to 0 while such a worker is still attached and mid-delivery, this produces a real circular wait:

```
JS thread: cleanup() -> uv_thread_join(teardown thread) -> blocked
Teardown thread: graal_tear_down_isolate() -> waiting for worker to detach -> blocked
Worker thread: napi_call_threadsafe_function(..., blocking) -> waiting for JS thread to run callback -> blocked
```

`g_isolate`/`g_ref_count` are process-global, so this is reachable even when the streaming op and the `cleanup()` call belong to different, unrelated `DataWeave` instances — not just same-instance self-cleanup.

### Why the existing F1 regression test didn't catch it

The Task 4 F1 test (added during the PR-157 remediation) uses a resolver that throws before emitting any data, so the streaming operation fails fast and the worker thread never reaches the mid-delivery, blocked-on-`napi_tsfn_blocking` state this bug requires.

---

## Design

### New global state (guarded by the existing `g_mutex`)

- **`g_active_ops`** (`int`) — count of all currently-running streaming/transform native calls, across every engine (resolver-backed or not) and every Worker thread.
- **`g_teardown_pending`** (`bool`) — true from the moment `cleanup()` drops `g_ref_count` to 0 while `g_active_ops > 0`, until teardown actually completes.
- **`g_teardown_cond`** (`uv_cond_t`) — condition variable the waiter thread blocks on; signaled by each op's completion sentinel after decrementing `g_active_ops`.
- **`g_teardown_waiters`** (linked list, each node `{napi_env env, napi_deferred deferred, napi_threadsafe_function tsfn}`) — one entry per `cleanup()` call currently waiting on the same in-progress teardown. A list rather than a single slot because a second (or third) `cleanup()` call can arrive from a **different** `napi_env` (a different Worker thread) while the first teardown is still pending — `napi_env`/`napi_deferred`/`napi_threadsafe_function` are thread-affine, so each waiting caller needs its own tsfn created on its own env; there is no way to resolve one env's deferred from another env's thread.

### Op accounting

Every streaming/transform entrypoint (`napi_run_script_streaming_engine`, `napi_run_script_transform_engine`) increments `g_active_ops` under `g_mutex`, immediately alongside the existing `bridge_begin_op` call and before spawning its worker thread — same timing, same "no early return in between" invariant already documented for `bridge_begin_op`.

The completion sentinel branch (`chunk->len == -1`) in `call_js_write`/`call_js_transform_write` decrements `g_active_ops` under `g_mutex`, alongside the existing `bridge_end_op` call, and signals `g_teardown_cond`. This is the only new responsibility added to the sentinel — it does not spawn anything or run teardown itself.

### `napi_cleanup` behavior

1. Lock `g_mutex`, decrement `g_ref_count` only if it's currently `> 0` (a second `cleanup()` call while one is already pending, with `g_ref_count` already at 0, must not decrement further into negative values).
2. If `g_ref_count > 0` after decrementing: unlock, return an already-resolved promise (today's "no-op until last release" behavior, promise-shaped). Every branch that returns "already resolved" (this one and case 4) creates a `napi_deferred`/promise and resolves it immediately before returning, rather than inventing a separate no-promise return path — keeps `napi_cleanup`'s return type uniformly "a promise" regardless of which branch runs.
3. If `g_ref_count <= 0` and `g_teardown_pending` is already true (re-entrant call — see Edge Cases): create a new deferred/promise + threadsafe function on *this call's* env, append it to `g_teardown_waiters`, unlock, return the pending promise. No second waiter thread is spawned — this call's node just joins the list the existing waiter thread will drain on completion.
4. If `g_ref_count <= 0`, `g_teardown_pending` is false, and `g_active_ops == 0`: unchanged fast path — spawn+join the teardown thread inline (`cleanup_thread_fn`, unmodified), reset `g_thread`/`g_isolate`/`g_initialized`/`g_ref_count`, unlock, return an already-resolved promise.
5. If `g_ref_count <= 0`, `g_teardown_pending` is false, and `g_active_ops > 0`: set `g_teardown_pending = true`; create a deferred/promise + threadsafe function on this env, append it as the first node of `g_teardown_waiters`; spawn the **waiter thread**; unlock; return the pending promise.

### Waiter thread

A dedicated thread (spawned only in case 5 above) that:
1. Locks `g_mutex`, waits on `g_teardown_cond` while `g_active_ops > 0`.
2. Once drained, runs teardown exactly as `cleanup_thread_fn` does today (attach a local thread to the isolate, call `graal_tear_down_isolate`, ignoring its return code — matching today's behavior of not propagating a teardown failure).
3. Resets `g_thread`/`g_isolate`/`g_initialized`/`g_ref_count`/`g_teardown_pending` under `g_mutex`, signals `g_teardown_cond` again (to release any `initialize()` call blocked in the re-entrant-init path below).
4. Walks `g_teardown_waiters`: for each node, calls its `tsfn` to resolve its `deferred` back on its own env, then releases that threadsafe function. Clears the list once every node has been signaled.

This thread is dedicated to this one teardown — no unrelated Worker's event loop is ever blocked as a side effect of finishing its own streaming op (rejected alternative: piggybacking teardown onto the last op's own completion sentinel, which would stall whichever unrelated thread happens to run that sentinel for the full teardown duration).

### `DataWeave.cleanup()` (TypeScript)

`cleanup(): Promise<void>` (was `void`). Awaits `ffi.cleanup()`'s now-Promise-returning addon call. Callers that need the old synchronous-fire-and-forget behavior (e.g. the module-level process-exit hook) simply don't await it — unchanged behavior for them, since the promise resolving or not doesn't block anything if nobody awaits it.

---

## Edge Cases

**Re-entrant `cleanup()` while teardown is pending, possibly from a different Worker/env.** Handled by case 3 above — `g_ref_count` doesn't go negative, no second waiter thread is spawned, and each caller's own env gets its own list node (deferred + tsfn) so it can be resolved on its own thread when teardown finishes, regardless of which env made the original triggering call. Preserves `cleanup()`'s documented idempotency (`dataweave.ts:105`, "a no-op if not initialized") at the addon layer, including across Workers.

**`initialize()` called while a teardown is pending.** `napi_initialize` must not re-create the isolate while the old one is still tearing down (risk of two live isolates, or use of a half-torn-down one). Add a check: if `g_teardown_pending` is true, block on `g_teardown_cond` until it's false and `g_isolate == NULL` is confirmed, then proceed with the existing create-isolate logic. This is a narrow, rare path (re-initializing mid-drain) but must not be skipped.

**`graal_tear_down_isolate` returning a non-zero/failure code.** Unchanged from today — the existing fast path already ignores this return value; the waiter thread preserves that (no new failure-propagation behavior invented for this fix).

**Process exit while ops are active and teardown is pending.** No new behavior introduced; an active native worker thread at process exit is already an existing, out-of-scope condition handled by libuv/Node's own exit sequencing, not this addon.

---

## Testing

1. **Deadlock regression (the core test).** For both `runStreaming()` and `runTransform()`: start an operation whose script produces multiple chunks with real volume/delay between them (so the worker is genuinely attached and mid-delivery, not failing fast like the existing F1 test). Call `gen.next()` once to pin the operation, then `await dw.cleanup()` before draining the generator. Assert the returned promise resolves within a bounded timeout (test-level timeout or explicit `Promise.race`) rather than hanging, and that the streaming generator itself eventually settles.
2. **Fast-path regression guard.** `cleanup()` called after a stream has already fully drained (`g_active_ops == 0` at the moment of last release) still resolves via the unchanged inline fast path — confirms the new branch didn't silently become the only path.
3. **Idempotency / re-entrant cleanup.** Two concurrent (or sequential, unawaited-then-awaited) `cleanup()` calls while a stream is active both resolve off the same underlying teardown, without spawning a second waiter thread or throwing.
4. **Re-initialize during pending teardown.** Start a stream, call `cleanup()` without awaiting, then immediately call `initialize()` again — confirms it blocks until the pending teardown finishes and the instance is usable afterward (a subsequent `run()` succeeds).
5. **No regression in the existing suite.** All current streaming/transform/lifecycle tests, including the Task 4 F1/F4/F6 additions from the PR-157 remediation, continue passing unmodified.
