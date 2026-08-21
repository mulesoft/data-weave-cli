# Per-Env Init-Reference Ownership — Round 13 (W-23692110)

**Status:** Design approved (user), ready for planning.

**Source review:** `docs/pr-157-follow-up-code-review-4.md`, Finding #5 (Medium), verified against live source at commit `765c273` (round-12 tip). Findings #1, #2, #3, #4, #6 in that review are test-quality/coverage items or already-shipped fixes and are **out of scope** for this round (they may be addressed in a separate test-hardening round); this round fixes only #5, the one production-correctness finding.

**Scope:** `native-lib/node` only — `src/addon.c` and Node integration tests under `tests/`. Do **not** touch `native-lib/python/**`, the legacy singleton entrypoints (`run_script`, `run_script_callback`, `run_script_input_output_callback`), `dw_napi_run_script`, or `ScriptRuntime.getInstance()`. The Java side is read for context but not modified. `src/dataweave.ts` is **not** modified — the product-facing `DataWeave` class already maintains the sanctioned 1:1 pairing, so no JS change is needed; the fix hardens the C boundary underneath it.

## Problem

### #5 (Medium) — Abandoned-env reference release relies on an unenforced raw-addon invariant

`g_ref_count` (`addon.c:37`) is a single process-global reference counter with **no notion of which `napi_env` owns each reference**. Its accounting assumes a strict **1 `initialize()` ↔ 1 engine ↔ 1 `cleanup()`** pairing:

- `napi_initialize` does `g_ref_count++` at three sites: the adoption path (`:531`), the already-initialized fast path (`:545`), and the create path (`:574`).
- `napi_cleanup` → `release_isolate_ref_locked` does the matching `g_ref_count--` (`:2358-2359`) and, on the last release, drives isolate teardown.
- An **abandoned engine's** env-cleanup hook also releases one reference: `bridge_env_cleanup` (`:339`, direct path) or `bridge_end_op` (`:404`, deferred path), gated by `engine_bridge_t.deferred_ref_release`, calling `isolate_ref_release_core_locked()`.

The defect: the **per-engine** env-cleanup hook releases a reference that logically belongs to **`initialize()`**, not to the engine. The product `DataWeave` class calls `initialize()` exactly once per engine and releases them together via one `cleanup()`, so the counts happen to match. But the addon exports raw `initialize`, `createEngine`, and `createEngineWithResolver` (`addon.c:2494-2504`) with **nothing enforcing the pairing**. A raw consumer that does `initialize()` **once**, then `createEngine()` **N times**, registers **N** per-engine cleanup hooks against a reference count of **1**. When that env is abandoned:

1. the first engine's hook (`bridge_env_cleanup` → `isolate_ref_release_core_locked`) drives `g_ref_count` `1 → 0`,
2. `isolate_ref_release_core_locked` (`:2297-2338`) sees zero and **tears the isolate down** (synchronously when `g_active_ops == 0`, or queues the waiter otherwise),
3. the remaining `N-1` engines — and, in a multi-env process, **another env's still-valid engines** — are now operating on a torn-down isolate.

This is a use-after-free / premature-teardown hazard, documented but unenforced in the comment at `addon.c:2289-2296`. Finding #5 asks that the addon boundary either enforce the pairing, track init ownership separately from engine records, or make the raw surface inaccessible. The raw `.node` file cannot truly be made inaccessible (anything can `require()` it), and enforcing one-engine-per-init would reject valid multi-engine usage. **Decision (user): track initialization ownership separately from engine records** — the robust option that fixes the UAF while preserving the multi-engine feature.

## Design

Introduce **per-`napi_env` init-reference accounting** so `g_ref_count` becomes a derived total rather than a bare global that any engine hook can drive to zero. One invariant governs the whole design:

> **`g_ref_count` == Σ `init_refs` over all live env records.**

Every reference in the global count is owned by exactly one env's record; a reference can only be released by the same env that acquired it (via that env's `cleanup()`) or by that env's death hook (releasing all of that env's outstanding references at once). The per-engine cleanup hook stops touching `g_ref_count` entirely — which is the actual bug fix. The teardown decision still fires only on the true global last-release, and only from an env-scoped release path, so it can never tear the isolate down while another env holds a reference.

### 1. New per-env record and registry

```c
// One record per napi_env that has ever taken an init reference (via
// initialize()). init_refs is that env's net initialize()-minus-cleanup()
// balance. The record is created lazily on the env's first initialize(),
// registers exactly one env-death hook (env_init_cleanup) at creation, and is
// freed when its env dies (that hook) after releasing every reference the env
// still holds. All fields mutated only under g_mutex.
//
// INVARIANT: g_ref_count == sum of init_refs over all records in g_env_recs.
typedef struct env_init_rec {
    napi_env env;
    int init_refs;
    struct env_init_rec* next;
} env_init_rec_t;
static env_init_rec_t* g_env_recs = NULL;  // linked list, guarded by g_mutex
```

Helpers (all require the caller to hold `g_mutex`):

- `env_init_rec_t* env_init_rec_find_locked(napi_env env)` — linear scan of `g_env_recs`, returns the record or NULL. Mirrors `bridge_find`.
- `env_init_rec_t* env_init_rec_acquire_locked(napi_env env, bool* is_new)` — find-or-create the record and `init_refs++`. Sets `*is_new = true` when it just allocated the record (the caller must then register the env-death hook, outside any napi-illegal context — see §3). Returns NULL only on `calloc` failure (caller treats as a hard error and does not bump `g_ref_count`).

### 2. `napi_initialize` — acquire a per-env reference alongside `g_ref_count`

Each of the three `g_ref_count++` sites gains a paired `init_refs` acquire on the calling env, under the same `g_mutex` hold that already guards the `g_ref_count++`:

- **Adoption path (`:530-534`):** currently `g_teardown_cancelled = true; g_ref_count++; broadcast; unlock; return`. Add `env_init_rec_acquire_locked(env, &is_new)` before the `g_ref_count++`. On `calloc` failure: do **not** cancel the teardown, do **not** bump `g_ref_count`; unlock and `napi_throw_error(env, NULL, "Failed to allocate env init record")`, return NULL. (The teardown stays queued; the caller's initialize failed cleanly.)
- **Fast path (`:544-548`):** `if (g_initialized) { g_ref_count++; ... }` — add the acquire before the bump, same failure handling (unlock + throw, no bump).
- **Create path (`:573-575`):** after a successful isolate build, before `g_ref_count++`, do the acquire. Perform the `env_init_rec_acquire_locked` **first** (it only allocates a small node); only if it succeeds proceed to `g_initialized = 1; g_ref_count++`. On acquire failure, `g_isolate` is already non-NULL (the create path's `init_thread_fn` just built it) while `g_initialized` is still 0 — simply unlocking and throwing would leave that combination in place, which the wait loop's `g_isolate != NULL && !g_initialized` clause treats as "a teardown is in flight," permanently hanging every subsequent `initialize()` in `uv_cond_wait` with nothing left to broadcast. So on this failure the just-built isolate is torn down (via the same `cleanup_thread_fn` idiom used elsewhere) before throwing, clearing `g_isolate`/`g_initialized` back to NULL/0 and restoring the same recoverable state the sibling spawn-failure/`init`-error paths already leave (they never built an isolate in the first place). If the teardown itself cannot attach to the isolate, `g_isolate` is left non-NULL as a best-effort degradation — the same posture already accepted for `cleanup_thread_fn`'s attach-failure path elsewhere.

**Hook registration for a new record.** When `env_init_rec_acquire_locked` reports `is_new`, register exactly one env-death hook for the init record:
`napi_add_env_cleanup_hook(env, env_init_cleanup, rec)`. This is legal in all three paths (they run on the env's own JS thread with the env alive). If the hook registration **fails**, the record cannot guarantee its references are reclaimed on env death — roll back: decrement the just-acquired `init_refs` (freeing the record if it drops to 0), do not bump `g_ref_count`, unlock, throw. This mirrors round-12 #6's all-or-nothing posture for the per-engine hook.

Ordering note (LIFO): because the init-record hook is registered on the **first** `initialize()` for an env — before any engine is created — Node's env-cleanup hooks run **LIFO**, so `env_init_cleanup` runs **after** every per-engine `bridge_env_cleanup` for that env. Every engine bridge is thus finalized (Java registry entry removed, napi_ref deleted) while the isolate is **still alive**, and only then does the init record release the isolate reference(s). This preserves the exact ordering the round-10/11/12 fixes rely on.

### 3. `env_init_cleanup` — release all of a dead env's references, once

New env-death hook, registered per §2. Runs on the dying env's own thread with the env still alive (standard env-cleanup-hook contract):

```c
static void env_init_cleanup(void* arg) {
    env_init_rec_t* rec = (env_init_rec_t*)arg;
    if (rec == NULL) return;
    uv_mutex_lock(&g_mutex);
    // Unlink from g_env_recs.
    env_init_rec_t** pp = &g_env_recs;
    while (*pp != NULL) { if (*pp == rec) { *pp = rec->next; break; } pp = &(*pp)->next; }
    int n = rec->init_refs;
    rec->init_refs = 0;
    free(rec);
    // Release exactly the references this env still held. release_n... makes the
    // teardown decision at most ONCE, after decrementing all n, so it never
    // spawns a second waiter or tears down an already-torn isolate mid-loop.
    isolate_ref_release_n_locked(n);
    uv_mutex_unlock(&g_mutex);
}
```

The env that reaches `env_init_cleanup` without having called `cleanup()` for each of its references (the abandoned-Worker case, and the raw multi-engine-per-init case) releases them here — **all at once, from a single env-scoped decision point.** Because `g_ref_count == Σ init_refs`, releasing this env's `n` reaches 0 **only** if no other env holds a reference, so an abandoned env-A can never tear down the isolate under a live env-B.

### 4. Bounded multi-release helper `isolate_ref_release_n_locked`

`isolate_ref_release_core_locked` (`:2297-2338`) currently decrements **one** reference and then makes the teardown decision. A naive loop calling it `n` times would, after the reference that reaches 0 tears down and sets `g_ref_count = 0`, make the remaining iterations no-op on an already-zero count — correct by luck, but it also re-runs the `g_teardown_state != TEARDOWN_NONE` early-return and would mis-handle the `g_active_ops > 0` waiter case if a second "last release" were computed. Make it explicit and single-decision:

```c
// Release n (>=0) initialization references at once, then make the teardown
// decision AT MOST ONCE. Caller holds g_mutex; this KEEPS it held. Equivalent
// to n serial core releases for the count, but guarantees the reached-zero
// teardown/waiter logic runs exactly once. n==0 is a no-op.
static void isolate_ref_release_n_locked(int n) {
    if (n <= 0) return;
    if (g_ref_count >= n) g_ref_count -= n; else g_ref_count = 0;
    if (g_ref_count > 0) return;                    // other envs still hold refs
    if (g_teardown_state != TEARDOWN_NONE) return;  // a teardown already drives
    // ... the SAME reached-zero body as isolate_ref_release_core_locked:
    //     g_active_ops == 0 -> synchronous cleanup_thread_fn + clear globals;
    //     g_active_ops  > 0 -> spawn waiter, TEARDOWN_PENDING_WAIT, empty list.
}
```

Refactor `isolate_ref_release_core_locked` to `isolate_ref_release_n_locked(1)` (behavior-preserving for the single-release callers). The single-decision reached-zero body is written once and shared.

### 5. `napi_cleanup` — gate the release on the calling env's ownership

`release_isolate_ref_locked(env)` (`:2353`) currently does an unconditional `if (g_ref_count > 0) g_ref_count--;`. Gate it on the calling env's own balance so an env can only release a reference it actually holds (user decision: gate `cleanup()` too, closing the symmetric over-`cleanup()` UAF):

```c
static napi_value release_isolate_ref_locked(napi_env env) {
  env_init_rec_t* rec = env_init_rec_find_locked(env);
  if (rec == NULL || rec->init_refs == 0) {
    // This env holds no init reference: a cleanup() with no matching
    // initialize() on this env (or a double-cleanup()). Do NOT touch
    // g_ref_count -- releasing here would steal another env's reference and
    // could tear the isolate down under a live user. No-op: resolve immediately.
    uv_mutex_unlock(&g_mutex);
    return already_resolved_promise(env);
  }
  rec->init_refs--;
  if (g_ref_count > 0) g_ref_count--;
  // ... the rest of Cases 1..5 UNCHANGED (the decrement above replaces the old
  //     unconditional one; g_ref_count-driven teardown decision is identical).
  ...
}
```

The record is **not** freed here even if `init_refs` hits 0 — its env is still alive and may `initialize()` again, and its env-death hook still needs to run (with `init_refs == 0`, `env_init_cleanup` releases nothing, which is correct). This matches the product pattern of `cleanup()` then possibly re-`initialize()` on the same env.

### 6. Per-engine hook stops touching `g_ref_count` (the core fix)

Remove the init-reference release from the per-engine path entirely:

- Delete the `deferred_ref_release` field from `engine_bridge_t` (`:121`) and every write (`bridge_env_cleanup:328`) and read (`bridge_end_op:398,404`).
- `bridge_env_cleanup`'s direct path (`:339`) no longer calls `isolate_ref_release_core_locked()`.
- `bridge_end_op` (`:404`) no longer conditionally releases the ref.

The per-engine hooks keep doing everything else — unlink the bridge, remove the Java registry entry (`do_registry_remove`), delete the resolver napi_ref, free the record. They simply no longer own an isolate reference, because they never did: the reference belongs to `initialize()`, now tracked by the env init record.

`isolate_ref_release_core_locked` becomes reachable only via `isolate_ref_release_n_locked`; if no other caller remains, it is folded into the `n==1` path (kept as a thin wrapper only if a call site still reads better with it).

## Invariants preserved / established

1. **`g_ref_count == Σ init_refs`** — established; every `g_ref_count` mutation is paired with an `init_refs` mutation on a specific env (init: both +1; cleanup: both −1 for the calling env; env death: −n for the dying env). The three initialize sites, `release_isolate_ref_locked`, and `env_init_cleanup` are the *only* mutators of `g_ref_count` after this round.
2. **An env releases only what it owns** — both `cleanup()` (§5) and env-death (§3) are keyed on a specific env's record; neither can drive `g_ref_count` below the references still held by *other* envs. Closes the cross-env UAF (abandoned env) **and** the symmetric over-`cleanup()` UAF.
3. **Teardown fires only on true global last-release** — the reached-zero body runs only when `g_ref_count` hits 0 after an env-scoped decrement, exactly as before; the multi-release helper makes that decision **once** per env-death.
4. **`destroyEngine` never releases an init reference** — unchanged; it was never a `g_ref_count` mutator and still isn't.
5. **`fn_destroy_engine` called exactly once per handle** — unchanged; the per-engine finalize path is untouched except for dropping the ref release.
6. **Thread affinity** — `env_init_cleanup` runs on its env's own thread with the env alive (env-cleanup-hook contract), doing only `g_mutex`-guarded integer/list work and `free` — no env-affine napi calls, no cross-thread napi. The init-record hook is registered on the env's own thread. Consistent with the round-11/12 per-engine hook design.
7. **Deadlock/adoption state machine (`TEARDOWN_*`, `g_teardown_cancelled`, the waiter)** — untouched; the reached-zero body it hooks into is the same, now shared via `isolate_ref_release_n_locked`.
8. **Sanctioned 1:1 usage is behavior-identical** — one env, one `initialize()` (`init_refs 1`, hook registered), one engine, one `cleanup()` (`init_refs 0`, `g_ref_count 0`, teardown as today). All existing round-1..12 tests must stay green with no assertion changes.

## Testing

Real-addon integration tests under `native-lib/node/tests/integration/` (no `vi.mock` of `ffi`), mirroring `instance-lifecycle.test.ts`'s ref-count proxy technique (a subsequent raw engine call throwing `/not initialized/` proves the isolate reached zero refs and was torn down; a call that succeeds proves it is still alive).

1. **Raw multi-engine-per-init does not prematurely tear down (the #5 core).** On one env (the main test thread): `ffi.initialize()` **once**, then `ffi.createEngine()` **twice** (handles h1, h2). Run a script on h2 to prove the isolate is live. `ffi.destroyEngine(h1)` — the isolate must remain alive: a run on h2 still succeeds. Then `ffi.destroyEngine(h2)` and one `ffi.cleanup()` (the single init reference). Now a fresh raw engine call must observe `/not initialized/`. *Pre-fix predicted behavior: acceptable here because destroyEngine (not the env hook) drives per-engine teardown and does not release the ref — so this test alone does not isolate #5; it guards that the multi-engine-per-init shape stays live under partial destroy.* **Primary #5 regression is test 2.**
2. **Over-`cleanup()` from an env cannot steal a reference / tear down under a live user.** `ffi.initialize()` once, `ffi.createEngine()` (h). Call `ffi.cleanup()` **twice**. The first releases this env's one reference (isolate torn down — this env owned exactly one). The second must be a **no-op** (`init_refs` already 0): it must not throw, and — critically — must not drive `g_ref_count` negative or perturb a *subsequently* re-initialized isolate. Prove: after the double-cleanup, `ffi.initialize()` again + `createEngine` + run succeeds (the second cleanup did not corrupt the count), then balance with one `cleanup()` and assert `/not initialized/`.
3. **Symmetric-ownership proof via the module API (regression guard for sanctioned path).** The existing `instance-lifecycle.test.ts` ref-count-proxy tests (napi_cleanup refactor; revived-singleton) must remain green unchanged — they already assert the 1:1 path tears down to zero. Add one assertion-level note only if needed; no new test required if these cover it.
4. **Worker abandonment still releases (round-12 #2 behavior preserved).** The existing `worker-lifecycle.test.ts` "N Workers exit without cleanup" test must remain green: each Worker does one `initialize()` + one engine, so its env-death `env_init_cleanup` releases exactly one reference — identical net behavior to the round-12 `deferred_ref_release` path it replaces. (If review round-4 Finding #1's stronger zero-reference assertion is added in a separate round, it must still pass here.)

All tests balance shared isolate state (final `cleanup()` / `/not initialized/` probe) so they do not perturb sibling integration files sharing the vitest worker process. Full Node suite target: **895 passed / 59 skipped / 0 failed** plus the new tests (2 new integration tests → **897 passed / 59 skipped / 0 failed**), unless a new test file adds more.

## Rejected alternatives

- **Enforce one engine per `initialize()` at the addon boundary (review option 1).** Rejected: rejects valid raw multi-engine-per-init usage, and still requires per-init tracking to detect "this env already has a live engine under its current init reference" — no simpler than per-env accounting, strictly more restrictive.
- **Make the raw addon private/inaccessible (review option 3).** Rejected: `dwlib_addon.node` is a file on disk; any consumer can `require()` it. Narrowing the package's documented surface is a docs change that leaves the underlying C hazard intact — effectively won't-fix.
- **Reference-count per engine instead of per env.** Rejected: the reference semantically belongs to `initialize()` (isolate lifetime), not to an engine (Java registry entry lifetime). Coupling it to engines is exactly the mispairing that caused #5.
- **Keep `deferred_ref_release` and additionally cap releases at the env's engine count.** Rejected: still keyed on engines, still lets an abandoned env with more engines than its true init count over-release; per-env accounting is the correct key.
- **Store the init record via `napi_set_instance_data`.** Rejected: `napi_set_instance_data` is single-slot per env and may already be reserved by future addon needs; a `g_mutex`-guarded list mirrors the existing `g_bridges` pattern the codebase already reasons about, and is visible to the cross-env teardown decision that instance-data (env-local) is not.
