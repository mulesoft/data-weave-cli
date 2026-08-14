# DataWeave Instance Lifecycle State Fix — Round 6 (W-23692110)

**Status:** Design approved, ready for planning.

**Source review:** `docs/pr-157-follow-up-andy-code-review-6.md` (three findings, all verified against live source at commit `49d2881`).

**Scope:** `native-lib/node` only — `src/dataweave.ts`, `src/addon.c`, and new tests under `tests/`. Do **not** touch `native-lib/python/**`.

## Problem

The sixth "andy" follow-up review of PR #157 raised three findings. All three were verified against the live source and are **new** (distinct from rounds 1–5, whose fixes remain intact at HEAD). Rounds 1–5 targeted the module-level singleton and the native isolate teardown; round 6 is the first to attack the **per-instance (`new DataWeave()`) lifecycle** and the **unguarded native lifecycle/handle reads**.

### Root cause

Lifecycle state is under-modeled at two layers:

1. **JS layer:** `DataWeave` uses a single boolean `initialized`. The real lifecycle has an intermediate "cleaning up" phase (`cleanup()` started but `await ffi.cleanup()` not yet settled), which a boolean cannot represent. Every `if (this.initialized)` check therefore treats the cleanup window as "ready." This is exactly what findings #1 and #3 exploit.
2. **C layer:** `napi_run_script_streaming_engine` / `napi_run_script_transform_engine` read the lifecycle flag `g_initialized` **outside** the `g_mutex` that guards it, then reserve `g_active_ops` in a later, separate critical section — a check-and-reserve TOCTOU (finding #2).

### The three findings (all confirmed)

**#1 (P1) — cleanup makes the engine handle invalid before marking the instance unavailable.**
`dataweave.ts` `doCleanup()` sets `engineHandle = null` synchronously, but `initialized` only flips to `false` in the `finally` *after* `await ffi.cleanup()`. In that window `initialized === true` && `engineHandle === null`, so `run()`/`runStreaming()`/`runTransform()` pass `ensureInitialized()` and send `null` as the handle. On the C side, `napi_get_value_int64` at addon.c:724-725, 1105-1106, and 1474 does not check its return status; on a null argument it leaves `handle64` as uninitialized stack data, then uses it as the engine handle.

**#2 (P1) — a Worker can tear down the isolate between stream admission and active-op registration.**
`napi_run_script_streaming_engine` (addon.c:706) and `napi_run_script_transform_engine` (addon.c:1084) read `g_initialized` without `g_mutex`, then take the lock only later to increment `g_active_ops` (addon.c:756-758 / 1155-1157). The C globals are process-shared `static`s, so a second Node Worker can call `napi_cleanup`, hit Case 4 (last ref, `g_active_ops == 0`, addon.c:1745-1781), and synchronously tear down the isolate in that gap. The first Worker's newly spawned thread then attaches to a dead isolate.

**#3 (P2) — `initialize()` during the same instance's pending cleanup is silently lost.**
`initialize()` (dataweave.ts:77) returns early on `if (this.initialized) return;`. During the cleanup window `initialized` is still `true`, so a second `initialize()` is a no-op; when cleanup then settles it sets `initialized = false`. Net: `dw.cleanup(); dw.initialize();` leaves the instance **uninitialized** despite the explicit second call. Round 5's regression coverage used two instances, so this same-instance path was never exercised.

## Design

### 1. JS instance lifecycle state (findings #1 + #3)

Replace `private initialized = false` with an explicit three-state field:

```ts
type LifecycleState = "uninitialized" | "ready" | "cleaning-up";
private state: LifecycleState = "uninitialized";
```

Transitions and gates:

- **`initialize()`**
  - `ready` → no-op (unchanged idempotency).
  - `cleaning-up` → **throw** `DataWeaveError("Cannot initialize while cleanup is in progress; await cleanup() first.")` (finding #3 — no more silent no-op).
  - `uninitialized` → run the existing load/create-engine work; on success set `state = "ready"`. On failure the existing ref-count-release path runs and state stays `uninitialized`.
- **`run()` / `runStreaming()` / `runTransform()`** — gated by `ensureReady()` (renamed from `ensureInitialized`): throw `DataWeaveError` unless `state === "ready"`.
  - In `uninitialized`: existing message ("DataWeave runtime not initialized. Call initialize() first.").
  - In `cleaning-up`: `DataWeaveError("DataWeave runtime is cleaning up; await cleanup() before running again.")` (finding #1 — the null handle can no longer reach C).
- **`cleanup()` / `doCleanup()`** — set `state = "cleaning-up"` **synchronously before** `ffi.destroyEngine` / `ffi.cleanup` (the key ordering fix). The `finally` sets `state = "uninitialized"` on both fulfilment and rejection. The existing `cleanupPromise` coalescing (round-4 F1) is preserved: the guard becomes `if (this.state !== "ready") return;` at the top of `cleanup()` for the not-ready early return, and the `if (this.cleanupPromise) return this.cleanupPromise;` coalescing check stays.

Notes:
- The `engineHandle === null` window still exists internally, but is now unreachable by any public method because every entry point checks `state` first.
- The `constructor` sets `state = "uninitialized"` (replacing `initialized = false`).

### 2. C admission atomicity (finding #2)

In both `napi_run_script_streaming_engine` and `napi_run_script_transform_engine`, fold the lifecycle check into the **same** `g_mutex` critical section that increments `g_active_ops`:

```c
uv_mutex_lock(&g_mutex);
if (!g_initialized || g_teardown_state != TEARDOWN_NONE) {
  uv_mutex_unlock(&g_mutex);
  napi_throw_error(env, NULL, "Not initialized. Call initialize() first.");
  return NULL;   // reject admission BEFORE any promise/work struct/tsfn is created
}
g_active_ops++;
uv_mutex_unlock(&g_mutex);
```

This must be positioned **before** any work struct allocation, tsfn creation, promise creation, or `bridge_begin_op`, so the rejection path frees nothing (mirrors the existing top-of-function `!g_initialized` throw). The cheap top-of-function `!g_initialized` fast-path guard stays; the authoritative check is the one under the lock. Rejecting on `g_teardown_state != TEARDOWN_NONE` also prevents admitting a new op once teardown is queued/underway.

**Constraint:** must not disturb round 5's `TEARDOWN_*` state machine, the deadlock-free `napi_initialize` adoption path, or the `g_active_ops` decrement-on-worker-thread invariant. Handle width stays `long long`. No `napi_reject_deferred` introduced (rejection here is a synchronous `napi_throw_error` at admission, before any deferred exists — consistent with the existing pattern).

### 3. N-API handle validation, defense-in-depth (finding #1)

At the three handle-read sites (addon.c:724-725, 1105-1106, 1474), check the return status of `napi_get_value_int64` (and, where cheap, the arg type via `napi_typeof`); on failure `napi_throw_error` and return `NULL` **before** allocating any work struct or reserving `g_active_ops`. Scope is deliberately these cited handle conversions only — not a blanket audit of every `napi_*` call in the file (YAGNI). This is belt-and-suspenders behind Section 1's JS guard, and the sole protection if the addon is driven directly.

### 4. Testing

New regression tests use the **real addon** (no `vi.mock` of `ffi`), mirroring `tests/integration/independent-engines.test.ts`, and are all **same-instance** (round 5's cross-instance coverage is exactly what let #3 slip through):

1. **Finding #3 — init-during-cleanup rejects, then recovers.** `dw.initialize(); const closing = dw.cleanup(); expect(() => dw.initialize()).toThrow(DataWeaveError)` (message mentions cleanup in progress). Then `await closing; dw.initialize();` succeeds and `dw.run(...)` works.
2. **Finding #1 — op-during-cleanup throws, no null handle to C.** `dw.initialize(); const closing = dw.cleanup(); expect(() => dw.run(...)).toThrow(DataWeaveError)`. Same for `runStreaming`/`runTransform` (their generators reject/throw on first pull). Then `await closing`.
3. **Finding #2 — admission rejected while teardown pending.** Deterministically forcing the cross-Worker isolate-teardown race from JS is not reliably possible; instead assert the admission-rejection path (attempt a streaming/transform op while a module-level teardown is pending → throws/rejects rather than sending work to a dead isolate). Document in the test that the genuine multi-Worker TOCTOU is covered by the C-level reasoning (the check-and-reserve is now atomic under `g_mutex`), not by this test.

All tests fully clean up (await the cleanup promise; idempotent final `cleanup()`) so they don't perturb sibling integration tests sharing the one process-wide isolate.

## Verification

- `cd native-lib/node && npm run build:addon` clean (no new warnings in the touched C regions); `npm run build` (tsc) clean.
- `npm test` green: current baseline **866 passed / 59 skipped / 0 failed**, plus the new same-instance regression tests.
- Optional: `./gradlew native-lib:nodeTest`, `git diff --check`.

## Global Constraints

- Node-binding-only. Never touch `native-lib/python/**`.
- Never touch the legacy singleton entrypoints (`run_script`, `run_script_callback`, `run_script_input_output_callback`) or `ScriptRuntime.getInstance()`.
- Handle width stays C `long long` everywhere.
- Errors for run/streaming/transform APIs surface as **resolved** JSON string values or thrown `DataWeaveError`/`napi_throw_error` at admission — never `napi_reject_deferred` (absent from addon.c; do not introduce).
- `napi_env`/`napi_ref`/`napi_deferred`/`napi_threadsafe_function` are thread-affine to the env's JS thread.
- All shared C state (`g_initialized`, `g_active_ops`, `g_teardown_state`, `g_teardown_cancelled`, `g_ref_count`) is read/written only under `g_mutex`.
- Preserve every round-1..5 fix: coalesced `cleanup()`, the `TEARDOWN_*` state machine and `napi_initialize` adoption path, the worker-thread `g_active_ops` decrement, guarded cross-thread `destroyEngine`, N-API allocation checks in `teardown_waiter_create`, the enqueue-failure waiter free.
- Node vitest baseline **866 passed / 59 skipped / 0 failed** — every task leaves the suite green.

## Rejected Alternatives

- **Finding #3 — queue a re-init after cleanupPromise, or make `initialize()` async.** Rejected: queuing adds async state to a synchronous API and gives queued-init errors no synchronous surface; making `initialize()` async is an API break (`run()` depends on `initialize()` completing synchronously). Deterministic rejection matches the synchronous API and forces callers to `await cleanup()` — chosen.
- **Finding #1 — return an error `ExecutionResult` from `run()` during cleanup instead of throwing.** Rejected for cross-method inconsistency: the streaming generators would still have to throw/yield-error, so behavior would diverge across the three entry points. Throwing `DataWeaveError` uniformly is symmetric with the existing not-initialized behavior and with the init-during-cleanup rejection — chosen.
- **Finding #1 — blanket-audit and validate every `napi_*` return in addon.c.** Rejected as scope creep (YAGNI). Validate the three cited handle conversions; the JS state guard is the primary protection.
