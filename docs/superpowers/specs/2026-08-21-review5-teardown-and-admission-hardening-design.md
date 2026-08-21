# Review #5 Remediation — Engine-Creation Admission + Teardown-Failure Recovery + Regression-Test Strength

**Date:** 2026-08-21
**Branch:** `w-23692110-multi-engine-design` (PR #157)
**Round:** 14
**Addresses:** `docs/pr-157-follow-up-code-review-5.md` (1 High, 5 Medium, 1 Low)

## Context

PR #157 ships the multi-engine Node binding for the DataWeave native library. Round 13 replaced the unsafe per-engine init-reference release with per-`napi_env` init-reference ownership, establishing the invariant **`g_ref_count == Σ (per-env init_refs)`**. Review #5 confirms that fix is correct and turns to three residual risk areas: engine-creation admission (a live concurrency hole), teardown-failure recovery (a live but owner-less isolate can be stranded), and regression-test strength (the round-13 tests do not actually pin the round-12 defect).

The reviewed head is `bd68c70` — the exact round-13 HEAD. The subsequent master merge (`212424d`) touched only `native-lib/python/**` and a `package-lock.json` dep bump, so every line reference in the review is still accurate against the current tree.

This round is **Node-binding only**. It does not touch `native-lib/python/**`, the legacy singleton entrypoints (`run_script`, `run_script_callback`, `run_script_input_output_callback`), `dw_napi_run_script`, or `ScriptRuntime.getInstance()`. The Java side is unchanged. Handle width stays C `long long`. Errors surface as resolved JSON strings (async) or synchronous `napi_throw_error` / thrown `DataWeaveError` — never `napi_reject_deferred`. `napi_env`/`napi_ref`/`napi_deferred`/`napi_threadsafe_function` are thread-affine to the env's JS thread; no env-affine napi call is made from the waiter or a wrong thread.

**Preserved invariant (every g_mutex release):** `g_ref_count == Σ per-env init_refs`. No fix in this round resurrects a reference that no env owns.

## Findings and Fixes

### #1 (High) — engine creation can attach to an isolate being torn down

**Defect.** `napi_create_engine` (addon.c:1837–1923) and `napi_create_engine_with_resolver` (addon.c:1926+) test `g_initialized` **outside** `g_mutex`, then call `fn_attach_thread(g_isolate, …)` and `fn_create_engine(…)` with (a) no requirement that the calling `napi_env` owns an init reference, and (b) no `g_active_ops` reservation pinning the isolate across the attach. An env that never called `initialize()` (or that already released its reference) can observe a still-`g_initialized` isolate while another env drops the final reference and the waiter/cleanup thread begins `graal_tear_down_isolate()`. The create then attaches to / creates an engine on a tearing-down isolate — a use-after-free.

**Fix.** Mirror the proven admission pattern already used by `bridge_finalize_registry` (addon.c:286–313): perform the lifecycle check and the reservation in **one critical section** under `g_mutex`, at the top of each create function:

```c
uv_mutex_lock(&g_mutex);
// Admission (one critical section — no teardown can interleave between the
// checks and the reservation, because every teardown transition and the
// g_active_ops==0 fast path also hold g_mutex):
//  (1) isolate must be live and NOT past the point of no return,
//  (2) the CALLING env must own an init reference (round-13 ownership model:
//      an env with no reference must not create engines on the shared isolate),
//  (3) pin the live isolate for the duration of the attach/create.
env_init_rec_t* self = env_init_rec_find_locked(env);
if (!g_initialized || g_isolate == NULL ||
    g_teardown_state == TEARDOWN_TEARING_DOWN ||
    self == NULL || self->init_refs == 0) {
  uv_mutex_unlock(&g_mutex);
  napi_throw_error(env, NULL, "Not initialized. Call initialize() first.");
  return NULL;
}
g_active_ops++;  // pins the live isolate against teardown across the attach
uv_mutex_unlock(&g_mutex);
```

After this point, the existing attach/create/detach body runs unchanged, and the `g_active_ops` reservation is **released on every path that leaves the function after the reservation was taken** — success and each failure branch — with the verbatim pattern used everywhere else:

```c
uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
```

The `fn_create_engine`/`fn_create_engine_with_resolver`/`fn_attach_thread` availability checks (`if (!fn_create_engine) …`) move to *before* the lock (they throw without having taken the reservation) or stay after with the release — the implementer picks whichever keeps the diff minimal, provided every post-reservation exit balances `g_active_ops`.

**Consequence for the record/hook-registration tail.** The existing OOM/hook-failure rollback paths in both create functions (the `calloc`-failure and `napi_add_env_cleanup_hook`-failure branches) must release the `g_active_ops` reservation in addition to their current cleanup (destroy the created engine, unlink, finalize). The reservation is released once, right before the function returns, on both the success path (after `napi_create_int64` produces the return value) and every failure path.

**Retires a caveat.** Round-13's `env_init_cleanup` header documents a "pathological raw-ffi order" where `createEngine()` on env B succeeds because env A already initialized, *before* B's own `initialize()`. With requirement (2), that call is now correctly **rejected** (B owns no reference), so the caveat's premise no longer holds. Update that comment to note the create path now enforces per-env ownership.

**Confirm during review:**
- The lifecycle check and `g_active_ops++` are in one `g_mutex` critical section; no teardown transition can split them.
- Every exit after the reservation balances `g_active_ops` exactly once (no double-decrement, no leak). Count the paths: success, invalid-handle, calloc-fail, hook-fail (create-engine); success, invalid-handle, attach-fail, calloc-fail, reference-fail, hook-fail (resolver variant — note attach-fail and the alloc failures *before* the reservation is taken must NOT decrement).
- An env with `init_refs == 0` (never initialized, or already cleaned up) is rejected with `Not initialized`.
- `g_ref_count` is untouched by this fix (creation never mutated it post round-13); the invariant is unaffected.

### #2 + #3 (Medium) — teardown-failure paths strand a live, owner-less isolate

**Defect.** On a reached-zero release, three failure modes leave the isolate physically alive with `g_ref_count == 0` and no pending teardown:
- **#2:** `release_isolate_ref_locked` Case 5 (addon.c:2607–2656) — `teardown_waiter_create` fails (promise/tsfn/resource-name N-API allocation) after `g_ref_count` was decremented to 0. Current code returns `NULL` (throws) with `g_teardown_state` reset to `TEARDOWN_NONE`. Also the Case 5 waiter **spawn** failure restores `g_ref_count = env_init_refs_total_locked()` (= 0) and leaves the isolate live.
- **#3:** `isolate_ref_release_n_locked` (addon.c:2399–2452, called by `env_init_cleanup` on env death) — waiter thread spawn fails, or `cleanup_thread_fn` attach fails so `torn_down` stays 0. `g_ref_count` is restored to `env_init_refs_total_locked()` (= 0 when the dying env was the last), isolate stays live.

In all three, `g_ref_count == 0` and no env record remains that could call `cleanup()` again, and no `g_teardown_state` is set — so nothing ever retries teardown. The isolate is stranded until an unrelated later `initialize()` happens to adopt it (which may never come). The round-13 invariant (`g_ref_count == Σ init_refs`) is correctly *preserved* by these paths, but preserving it is not sufficient: a zero-owner live isolate needs a retry owner.

**Fix — a `g_mutex`-guarded retry flag, not a phantom reference.** Add:

```c
// Set under g_mutex when a reached-zero teardown could NOT be carried out
// (waiter alloc/spawn failed, or cleanup_thread_fn attach failed) and the
// isolate was therefore left live with g_ref_count == 0 and no pending
// teardown. This is a RETRY SIGNAL, not an ownership reference: g_ref_count
// stays 0 so the invariant g_ref_count == Σ init_refs is unaffected. It is
// cleared when the isolate is (a) actually torn down, or (b) adopted by a
// later initialize(). While set with g_active_ops > 0, the drain point at op
// completion retries the teardown once ops reach 0.
static bool g_teardown_needed = false;
```

Set `g_teardown_needed = true` in each of the three failure branches (#2 Case-5 waiter-create failure and waiter-spawn failure; #3 `isolate_ref_release_n_locked` waiter-spawn failure and `torn_down == 0` after the sync attempt) **only when** the isolate was left live (`g_isolate != NULL && g_ref_count == 0`).

**Retry trigger at the op-completion drain point.** The natural retry owner is the last active op finishing. Add a helper that runs the reached-zero teardown decision:

```c
// Caller holds g_mutex, KEEPS it held. If a prior teardown failed and left the
// isolate live with no owners (g_teardown_needed), and ops have now drained
// (g_active_ops == 0) with still no owners (g_ref_count == 0) and no teardown
// in progress, retry the synchronous teardown exactly as Case 4 does.
static void retry_stranded_teardown_locked(void) {
  if (!g_teardown_needed) return;
  if (g_ref_count > 0) { g_teardown_needed = false; return; } // adopted → no retry
  if (g_teardown_state != TEARDOWN_NONE) return;              // a teardown drives
  if (g_active_ops > 0) return;                               // wait for drain
  if (g_isolate == NULL) { g_teardown_needed = false; return; }
  // g_active_ops == 0, g_ref_count == 0, isolate live: same synchronous
  // teardown as Case 4 / isolate_ref_release_n_locked's g_active_ops==0 branch.
  uv_thread_t tid; uv_thread_options_t opts;
  opts.flags = UV_THREAD_HAS_STACK_SIZE; opts.stack_size = 2 * 1024 * 1024;
  int torn_down = 0;
  int spawn_rc = uv_thread_create_ex(&tid, &opts, cleanup_thread_fn, &torn_down);
  if (spawn_rc == 0) uv_thread_join(&tid);
  if (torn_down) {
    g_thread = NULL; g_isolate = NULL; g_initialized = 0; g_ref_count = 0;
    g_teardown_needed = false;
  }
  // else: spawn/attach failed again — leave g_teardown_needed set to retry on
  // the next drain (or a later initialize() adoption clears it).
}
```

Call `retry_stranded_teardown_locked()` under `g_mutex` at each op-completion drain point — i.e. immediately after the existing `g_active_ops--; uv_cond_broadcast(...)` blocks in the streaming/transform completion paths (the `bridge_end_op`/`g_active_ops--` sites). Since those sites already hold `g_mutex` for the decrement, fold the retry call into the same critical section (decrement, broadcast, then retry) to avoid re-locking.

**Adoption clears the flag.** In `napi_initialize`'s adoption path (the `TEARDOWN_PENDING_WAIT` branch and the fast-path ref bump), and anywhere a new reference is acquired on a surviving isolate, set `g_teardown_needed = false` — a new owner means the isolate is wanted again. Concretely: whenever `env_init_acquire_and_hook` succeeds and `g_ref_count` transitions from 0 to 1 on a live isolate, clear the flag. The simplest correct placement is at the acquire sites right after a successful `g_ref_count++` on an already-live isolate.

**Why a flag and not "restore caller ownership on alloc failure".** Restoring `self->init_refs` and `g_ref_count` on the failing env would (a) violate the caller's contract (the JS `cleanup()` promise resolves as if the reference was dropped, but the count says otherwise), and (b) for the env-death path (#3) the record is already freed — there is no env to restore ownership to. A separate retry signal decoupled from the reference count is the only model that works uniformly for both the live-caller and no-surviving-env cases while keeping `g_ref_count == Σ init_refs` exactly true.

**Confirm during review:**
- `g_teardown_needed` is read/written only under `g_mutex`.
- The invariant `g_ref_count == Σ init_refs` holds at every g_mutex release — the flag never substitutes for a reference.
- The retry is idempotent and bounded: it makes the reached-zero teardown decision at most once per drain, and a repeated attach failure simply re-arms for the next drain without spinning.
- No env-affine napi call is made from any thread but the env's own (the retry runs on the JS thread at op completion; `cleanup_thread_fn` attaches its own GraalVM thread and makes no napi calls).
- Adoption in `napi_initialize` clears the flag so a re-init does not later tear down a wanted isolate.
- No deadlock: `retry_stranded_teardown_locked` spawns+joins `cleanup_thread_fn` while holding `g_mutex`, exactly as the existing Case-4 / `isolate_ref_release_n_locked` g_active_ops==0 branch does; `cleanup_thread_fn` takes no lock.

### #4 (Medium) — cross-env regression test that actually pins the round-12 defect

**Defect.** Round-13's `env-init-ownership.test.ts` are single-env smoke tests whose own header admits they pass on the pre-fix addon. `worker-lifecycle.test.ts`'s N-Worker test creates only **one** engine per Worker init, so it never exercises the round-12 over-release (N per-engine releases against one init reference).

**Fix.** Add a Worker-based regression test to `worker-lifecycle.test.ts` (reusing its inline-JS-body + built-addon harness) that:
1. On the main thread: `initialize()` and create a live engine (`h_main`), run a script to confirm it works.
2. Spawn a Worker that: `initialize()` once, creates **N ≥ 3** engines (resolver-less is fine), runs a script on one, and exits **without** `cleanup()` and without destroying its engines — so the Worker env dies with N engines under one init reference.
3. After the Worker exits: assert `h_main` **still runs** (`6 * 7 === 42`) — proving the shared isolate was not torn down by the Worker's env death.
4. Balance the main reference (`destroyEngine(h_main)` + `cleanup()`), then assert a raw `runScriptEngine(Number.MAX_SAFE_INTEGER, …)` throws `/not initialized/i` — proving the count reached exactly zero (no leak, no over-release).

**Determinism note in the test.** On the **round-12** implementation this goes RED: the Worker's env-death hooks fired N per-engine releases against a count of 1, driving `g_ref_count` negative/to-zero and tearing the isolate down under the live `h_main` → step 3's run fails (isolate gone) or the process wedges. On round-13+ each abandoned env releases exactly one reference regardless of engine count, so `h_main` survives. The test must await Worker `exit` (not just `message`) before asserting step 3, so the env-death hooks have run. Use the stricter `runWorker` helper from #5.

The two existing `env-init-ownership.test.ts` smoke tests stay (they guard the single-env liveness path), but the file header's "known coverage gap … remains a follow-up" paragraph is updated to point at this new cross-env test as the gap's closure.

**Confirm during review:**
- The test loads the real built addon (no `vi.mock`), spawns a genuine Worker, and creates N ≥ 3 engines in it.
- It awaits Worker exit before the post-exit assertions.
- It balances all references so it does not perturb sibling integration files (main `cleanup()` at the end; the file's `afterAll` already calls `ffi.cleanup()` idempotently).
- The RED-on-round-12 / green-on-round-13 reasoning is documented in a comment.

### #5 (Medium) — Worker lifecycle helper hides a nonzero exit

**Defect.** `runWorker` (worker-lifecycle.test.ts:71–83) resolves as soon as the Worker posts a message, and its `exit` handler only rejects `if (code !== 0 && !msg)`. A Worker that posts its success result and *then* exits nonzero (e.g. an env-cleanup-hook failure during teardown) resolves as success — the failure is hidden.

**Fix.** Rework the promise so that:
- The message is captured but resolution waits for `exit`.
- On `exit`: reject **every** nonzero code (`new Error("Worker exited with code " + code + (msg ? "" : " and posted no message"))`).
- On `exit` code 0 **with** a captured message: resolve with the message.
- On `exit` code 0 **without** a message: reject as a distinct diagnostic (`"Worker exited 0 without posting a result"`).
- Keep the `error` handler rejecting.

All existing callers already `await` the result and assert `msg.ok`, so tightening resolution to `exit` is compatible; the abandon-variant Workers exit 0 after posting, so they still resolve.

**Confirm during review:** no caller regresses; the N-Worker abandon test and the new #4 test both still pass; a hypothetical nonzero-exit Worker now rejects.

### #6 (Medium) — `DataWeave.cleanup()` leaks the init reference if `destroyEngine()` throws

**Defect.** `DataWeave.doCleanup()` (dataweave.ts:145–159) calls `ffi.destroyEngine(this.engineHandle)` before `await ffi.cleanup()`. If `destroyEngine` throws (a real path: wrong-thread destruction throws synchronously), the `finally` resets `this.state`/`this.engineHandle` but `ffi.cleanup()` never runs — the native init reference for this env is never released, and the engine handle is no longer reachable from the instance. The reference leaks.

**Fix.** Ensure `ffi.cleanup()` runs even when `destroyEngine()` throws, preserving the primary (destruction) error:

```ts
private async doCleanup(): Promise<void> {
  this.state = "cleaning-up";
  let destroyError: unknown;
  try {
    if (this.engineHandle !== null) {
      try {
        ffi.destroyEngine(this.engineHandle);
      } catch (e) {
        // Preserve the primary error but STILL release the native init
        // reference below — otherwise a throwing destroyEngine() (e.g.
        // wrong-thread destruction) would strand this env's reference and
        // block isolate teardown. The engine handle is cleared regardless so
        // a retry does not double-destroy.
        destroyError = e;
      } finally {
        this.engineHandle = null;
      }
    }
    await ffi.cleanup();
  } finally {
    this.state = "uninitialized";
  }
  if (destroyError !== undefined) throw destroyError;
}
```

The `await ffi.cleanup()` now always runs (releasing the reference); a destruction error is re-thrown after cleanup so callers still observe it. If `ffi.cleanup()` itself also throws, its error propagates from the `await` (the destruction error is then suppressed — acceptable: the reference-release failure is the more actionable one, and this matches the "report/suppress secondary" guidance).

**Test.** Add a unit test (in the existing `dataweave.ts` unit suite, with `ffi` mocked) where `destroyEngine` is mocked to throw: assert (a) `ffi.cleanup()` was still called, (b) the original destruction error propagates from `cleanup()`, (c) `this.state` ends `uninitialized`.

**Confirm during review:** `ffi.cleanup()` is invoked on the throwing-`destroyEngine` path; the primary error is preserved; `engineHandle` is cleared so a subsequent cleanup does not re-destroy; the coalescing/`cleanupPromise` logic in the public `cleanup()` wrapper is unaffected.

### #7 (Low) — resolver quick-start examples omit cleanup

**Defect.** `external-modules.md:7–25` and `README.md:231–253` show resolver-backed `DataWeave` instances with no `await dw.cleanup()`, though later docs state uncleaned instances retain their engine and resolver closure.

**Fix.** Wrap each complete example's `dw.initialize()`/`dw.run()` in `try { … } finally { await dw.cleanup(); }` and make the surrounding scope `async` (or add a one-line note that the snippet runs inside an async function). Keep the example output comments intact.

**Confirm during review:** both examples show `await dw.cleanup()` in a `finally`; the snippets remain runnable (async context noted); no other doc claims are altered.

## Task Ordering

1. **#1** — engine-creation admission (isolated, High, `addon.c`).
2. **#2 + #3** — teardown-failure retry flag + drain-point retry + adoption clear (`addon.c`; shared machinery, done as one task).
3. **#6** — `doCleanup()` reference-leak fix + unit test (`dataweave.ts`).
4. **#5** — `runWorker` helper strictness (`worker-lifecycle.test.ts`).
5. **#4** — cross-env Worker regression test (`worker-lifecycle.test.ts`; depends on #5's stricter helper).
6. **#7** — docs cleanup (`external-modules.md`, `README.md`).

Each task ends green on the full Node vitest suite. Baseline before this round: **897 passed / 59 skipped / 0 failed**. Net new tests: #6 (1 unit) + #4 (1 integration) → target **899 passed / 59 skipped / 0 failed** (the helper change in #5 alters no test count).

## Build & Test

- Build: `cd native-lib/node && npm run build:addon && npm run build:ts`
- Test: `DATAWEAVE_NATIVE_LIB=/Users/lmariano/dev/mulesoft/data-weave-cli/native-lib/node/native/dwlib.dylib npm test`
- `dwlib.dylib` is unchanged this round (only `addon.c`, `dataweave.ts`, tests, and docs change — no Java).

## Rejected Alternatives

- **#1: check `g_initialized` under the lock but skip the `g_active_ops` reservation.** Insufficient: the attach happens after the lock is dropped, so a teardown can still start between the check and `fn_attach_thread`. The reservation is what pins the isolate across the attach, exactly as `bridge_finalize_registry` does.
- **#2/#3: restore the failing caller's ownership (`init_refs`/`g_ref_count`) instead of a flag.** Breaks the JS `cleanup()` contract (promise resolves as released while the count says held) and is impossible for the env-death path (the record is already freed). A retry signal decoupled from the count is the only uniform model.
- **#2/#3: spawn a dedicated retry thread that polls until teardown succeeds.** Adds a background thread and a spin loop for a rare OOM/spawn-failure path; the op-completion drain point is a natural, already-locked retry owner with no new thread.
- **#4: keep documenting the gap (round-13 decision).** The reviewer raised this class twice; the user chose to write the real cross-env test this round.
