# OOM-Safe Allocation in Streaming/Transform Setup — Round 8 (W-23692110)

**Status:** Design approved, ready for planning.

**Source review:** `docs/pr-157-follow-up-andy-code-review-8.md` (one finding, P1, verified against live source at commit `3622179`, the round-7 tip).

**Scope:** `native-lib/node` only — `src/addon.c`, functions `napi_run_script_streaming_engine` and `napi_run_script_transform_engine`. Do **not** touch `native-lib/python/**` or the legacy singleton `dw_napi_run_script`.

## Problem

The eighth "andy" follow-up review of PR #157 raised one finding (escalated to P1). It was verified against live source and is real.

**Finding (P1) — OOM in streaming or transform setup can crash the process and strand active-operation state.**

Both `napi_run_script_streaming_engine` (addon.c:770-780) and `napi_run_script_transform_engine` (addon.c:1174-1207) reserve `g_active_ops` (streaming at :753, transform at :1166) and then, **after** the reservation, allocate a work struct and its string buffers and immediately use them without checking for allocation failure:

- Streaming: `struct streaming_work* w = calloc(...)` (:770) is dereferenced at `w->handle` (:771); `w->script = malloc(...)` / `w->inputs_json = malloc(...)` (:772-773) are passed to `napi_get_value_string_utf8` (:774-775) with no NULL check.
- Transform: `struct transform_work* w = calloc(...)` (:1174) is dereferenced at `w->handle` (:1176); each `w->field = malloc(len + 1)` (:1187, :1191, :1195, :1199, :1206) is passed to the fill `napi_get_value_string_utf8` with no NULL check.

If an allocation fails, the NULL dereference is a SIGSEGV that crashes the host Node process (not a catchable JS error). Because both sites sit *after* the `g_active_ops` reservation, the reservation is also never released — though in practice the segfault terminates the process first, so the crash is the dominant harm; releasing the reservation is the correct behavior on the (theoretical) non-crashing path and keeps the invariant clean.

### History / context (not a new defect)

This is the same gap logged as item 6 in `docs/ga-cleanup-backlog.md` and flagged as Minor/deferred by both the round-7 task review and the round-7 final whole-branch review (OOM-only, out of scope for round 7's conversion-*status* sweep). The eighth review escalates it from Minor to P1. It is a known deferred item re-prioritized, not a newly discovered class.

The fix pattern already exists in the same file: `napi_run_script_engine` checks its `malloc` results and throws `"OOM"` (addon.c ~1568). Streaming/transform simply never received the same treatment. `dw_napi_run_script` (the legacy singleton) has the identical gap but is off-limits by the Global Constraints.

## Design

Add allocation-failure checks at both sites, mirroring the existing `napi_run_script_engine` OOM pattern, so **no allocation result is dereferenced before its NULL check, and no OOM path leaves `g_active_ops` reserved or a partial `w` leaked.**

### 1. Streaming (`napi_run_script_streaming_engine`)

Immediately after `struct streaming_work* w = calloc(1, sizeof(struct streaming_work));` and **before** `w->handle = ...`, check `w == NULL`:

```c
struct streaming_work* w = calloc(1, sizeof(struct streaming_work));
if (w == NULL) {
  uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
  napi_throw_error(env, NULL, "OOM");
  return NULL;
}
w->handle = (long long)handle64;
w->script = malloc(script_len + 1);
w->inputs_json = malloc(inputs_len + 1);
if (w->script == NULL || w->inputs_json == NULL) {
  free(w->script); free(w->inputs_json); free(w);
  uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
  napi_throw_error(env, NULL, "OOM");
  return NULL;
}
if (napi_get_value_string_utf8(env, argv[1], w->script, script_len + 1, NULL) != napi_ok ||
    napi_get_value_string_utf8(env, argv[2], w->inputs_json, inputs_len + 1, NULL) != napi_ok) {
  free(w->script); free(w->inputs_json); free(w);
  uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
  napi_throw_error(env, NULL, "runScriptStreamingEngine: failed to read script/inputsJson");
  return NULL;
}
```

- The `w == NULL` branch must **not** free `w->script`/`w->inputs_json` (w is NULL — those dereferences would themselves crash); it frees nothing and unwinds.
- The combined `w->script == NULL || w->inputs_json == NULL` guard reuses the existing free-set (`free(w->script); free(w->inputs_json); free(w);` — all `free(NULL)`-safe since `calloc` zeroed `w` and a failed `malloc` returns NULL) and the verbatim `g_active_ops` unwind, sitting **before** the existing fill-status check.

### 2. Transform (`napi_run_script_transform_engine`)

Add a `w == NULL` check immediately after `calloc` and before `w->handle`, then a NULL check after each `malloc` via the existing `TRANSFORM_FAIL` macro (which already frees all five char* fields + `w` and unwinds `g_active_ops`):

```c
struct transform_work* w = calloc(1, sizeof(struct transform_work));
if (w == NULL) {
  uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
  napi_throw_error(env, NULL, "OOM");
  return NULL;
}
size_t len;
w->handle = (long long)handle64;

#define TRANSFORM_FAIL(msg) do { ... } while (0)   // unchanged

if (napi_get_value_string_utf8(env, argv[1], NULL, 0, &len) != napi_ok) TRANSFORM_FAIL("runScriptTransformEngine: script must be a string");
w->script = malloc(len + 1);
if (w->script == NULL) TRANSFORM_FAIL("OOM");
if (napi_get_value_string_utf8(env, argv[1], w->script, len + 1, NULL) != napi_ok) TRANSFORM_FAIL("runScriptTransformEngine: failed to read script");
```

…and the same `if (w->field == NULL) TRANSFORM_FAIL("OOM");` line after each of `w->inputs_json`, `w->input_name`, `w->input_mime_type`, and `w->input_charset` mallocs, placed **before** the corresponding fill `napi_get_value_string_utf8`.

- The `w == NULL` branch is a standalone unwind (it cannot use `TRANSFORM_FAIL`, which dereferences `w`).
- Each per-field NULL check uses `TRANSFORM_FAIL("OOM")`; because `calloc` zeroed `w` and any not-yet-reached field is still NULL, the macro's free-set is `free(NULL)`-safe for the unreached fields and frees the successfully-allocated ones exactly once.

### 3. Error message

Bare `napi_throw_error(env, NULL, "OOM")` for every allocation-failure throw, identical to `napi_run_script_engine`'s existing pattern. (User decision — maximum consistency with the current file over the descriptive per-entrypoint style of the conversion-status throws.) The existing conversion-status and read-failure messages in these functions are unchanged.

### 4. Testing

`malloc`/`calloc` failure is not deterministically forceable from JS/vitest (no allocator-injection hook at the addon boundary), the same limitation documented for the round-6/7 cross-Worker TOCTOU. So this round adds **no new runtime test**; coverage is:

- C-level code reasoning: every allocation result is NULL-checked before any dereference; every OOM path unwinds `g_active_ops` with the verbatim pattern and frees any partial `w` with no double-free.
- The full Node vitest suite stays green at **878 passed / 59 skipped / 0 failed** with no regression (the OOM branches are unreachable under normal allocation, so existing behavior is unchanged).

## Verification

- `cd native-lib/node && npm run build:addon` clean (no new warnings in the touched regions); `npm run build` (tsc) clean.
- `npm test` green: **878 passed / 59 skipped / 0 failed** (unchanged — no new test, no regression).
- `git diff --check`.

## Global Constraints

- Node-binding-only. Never touch `native-lib/python/**`.
- Never touch the legacy singleton entrypoints (`run_script`, `run_script_callback`, `run_script_input_output_callback`), `dw_napi_run_script`, or `ScriptRuntime.getInstance()`.
- Handle width stays C `long long` everywhere.
- Errors for run/streaming/transform APIs surface as **resolved** JSON string values or a synchronous `napi_throw_error` at admission / argument validation / allocation failure — never `napi_reject_deferred` (absent from addon.c; do not introduce).
- Allocation-failure rejections use `napi_throw_error` (generic Error) with the bare message `"OOM"`, matching `napi_run_script_engine`.
- `napi_env`/`napi_ref`/`napi_deferred`/`napi_threadsafe_function` are thread-affine to the env's JS thread.
- All shared C state (`g_initialized`, `g_active_ops`, `g_teardown_state`, `g_teardown_cancelled`, `g_ref_count`) is read/written only under `g_mutex`. (The cheap top-of-function `!g_initialized` fast-path read is a benign optimization.)
- The `g_active_ops` release pattern is EXACTLY, verbatim: `uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);` (matches the worker-thread decrement and every round-6/7 unwind site).
- Preserve every round-1..7 fix: coalesced `cleanup()`, the JS three-state lifecycle machine, the `TEARDOWN_*` state machine and `napi_initialize` adoption path (including round-7's `g_teardown_cancelled` admission carve-out), the worker-thread `g_active_ops` decrement, the streaming/transform atomic admission blocks, the round-6 handle-read validations, the round-7 conversion-status checks, guarded cross-thread `destroyEngine`, N-API allocation checks in `teardown_waiter_create`, the enqueue-failure waiter free.
- Node vitest baseline **878 passed / 59 skipped / 0 failed** — every task leaves the suite green.

## Rejected Alternatives

- **Descriptive per-entrypoint OOM messages** (`"runScriptStreamingEngine: out of memory"`). Considered for parity with the round-7 conversion-check message style in these same functions. Rejected in favor of bare `"OOM"` for consistency with `napi_run_script_engine`'s existing allocation-failure throw. (User decision.)
- **Abort/`ENOMEM`-style hard failure instead of a throwable error.** Rejected: a library must not take down the host process on a recoverable condition; surfacing a catchable N-API error is the contract used everywhere else in these entrypoints.
- **Also fixing `dw_napi_run_script`'s identical gap.** Rejected as out of scope — it is a forbidden legacy singleton entrypoint per the Global Constraints. Noted separately; not part of this round.
- **Adding a fault-injection test hook to force `malloc` failure.** Rejected as scope creep / test-only production surface (YAGNI). The OOM branches are covered by code reasoning, consistent with how the round-6/7 non-forceable paths were handled.
- **Retrofitting the whole file's allocations.** Rejected — this round fixes the two P1 sites the review names; a blanket allocation audit is out of scope (the same class-vs-blanket boundary drawn in round 7).
