# Adversarial Review: Native Library Language Bindings

**Branch:** `feat/new-native-bindings`
**Date:** 2026-07-06
**Method:** Adversarial review. A **Critic** agent hunts for flaws; a **Defender** agent
rebuts or concedes. Multiple rounds are moderated and persisted below for audit.

**Scope:** The FFI wrapper implementations around the DataWeave native (GraalVM) library:
- C binding — `native-lib/c/` (`src/dataweave.c`, `include/dataweave.h`, build)
- Go binding — `native-lib/go/` (`dataweave.go`, `streaming_callbacks.go`)
- Rust binding — `native-lib/rust/src/` (`lib.rs`, `ffi.rs`, `streaming.rs`, `result.rs`, `error.rs`)
- Python binding — `native-lib/python/src/dataweave/` (delta)
- Node binding — `native-lib/node/src/` (delta)
- FFI contract — `native-lib/FFI_CONTRACT.md`

Focus: memory safety, FFI contract adherence, concurrency/threading, error handling,
resource lifecycle, and correctness. Markdown-only docs are out of scope.

---

## Transcript

### Round 1 — Critic opens (18 findings)

## Critical Findings

**[1] C Streaming Worker Never Joins Detached Thread — Thread/Memory Leak** — Critical — native-lib/c/src/dataweave.c:912, 961-980

`dw_run_streaming` spawns a worker thread, immediately detaches it with `pthread_detach(worker_thread)` (line 912), then returns the stream handle to the caller. When the caller eventually calls `dw_stream_free`, the function frees chunks and metadata but **never joins the worker thread**. The detached thread may still be running (executing FFI calls into GraalVM) when the caller frees the stream struct from under it. The worker accesses `stream->mutex`, `stream->head`, `stream->tail`, etc., creating a use-after-free race. **Confidence:** High

**[2] C Streaming Worker Stores Caller-Owned Pointers — Use-After-Free** — Critical — native-lib/c/src/dataweave.c:795-896

The `stream_worker_thread` receives a `stream_worker_context` containing raw pointers `script` and `inputs_json` (lines 895-896). These are **caller-owned** `const char*` params from `dw_run_streaming`. The worker is detached and runs asynchronously; nothing guarantees the caller hasn't freed/overwritten those strings before the worker dereferences them in `run_script_callback(...)` (lines 825-830). Classic async callback lifetime bug. **Confidence:** High

**[3] Go `doneCh` Created But Never Closed — Goroutine Hang** — Critical — native-lib/go/dataweave.go:375-378, 444-448

`RunStreaming` (375) and `RunTransform` (444) create a `doneCh` assigned to `callbackContext` but **never closed**. `writeCallbackBridge` selects on it to abort if the consumer abandons (streaming_callbacks.go:41-45), but nothing signals it. If the consumer stops draining `Chunks`, the callback goroutine blocks forever in `ctx.chunkCh <- goBytes`, holding the GraalVM thread attached. **Confidence:** High

## High Findings

**[4] Rust `catch_unwind` in Callbacks — panic=abort gap** — High — native-lib/rust/src/streaming.rs:100-119, 127-146, 156-182

Callbacks wrap FFI-invoked code in `catch_unwind`, but it does not catch panics under `panic=abort` builds or stack overflows. Under abort, control unwinds into the C caller (UB). **Confidence:** Medium

**[5] C Base64 Decoder Silently Ignores Non-Padding '=' in Middle** — High — native-lib/c/src/dataweave.c:342-396

The decoder checks `len % 4 != 0` and counts trailing padding, but does not validate that '=' only appears in the final quantum. Mid-string '=' decodes to garbage without error. **Confidence:** High

**[6] Go `LockOSThread` without matching Unlock on attach failure** — High — native-lib/go/dataweave.go:150-172

`Run` locks the OS thread and defers unlock, but if `attachCurrentThread` fails, the deferred unlock still runs — however the concern is repeated transient failures leaking locked threads. **Confidence:** Medium *(moderator note: defer DOES run on early return; needs verification)*

**[7] Rust `metadata()` Joins Worker Thread Under Lock — Deadlock** — High — native-lib/rust/src/streaming.rs:69-74

`metadata()` locks `self.join`, then joins the worker. If the worker is blocked in `sender.send()` because the (unbounded? bounded?) channel consumer stopped, and the consumer is now in `join()`, deadlock. **Confidence:** Medium *(moderator note: mpsc::channel is unbounded — verify send can block)*

**[8] Python `Queue` unbounded — memory exhaustion** — High — native-lib/python/src/dataweave/__init__.py:570, 713

Streaming queues have no maxsize; a slow/stopped consumer lets the native callback fill memory until OOM. **Confidence:** High

**[9] Rust `SendPtr` transfer race before worker spawns** — High — native-lib/rust/src/streaming.rs:231-232, 299-300

Claimed race window between `Box::into_raw` and `thread::spawn`. **Confidence:** Low

## Medium Findings

**[10] Node read callback swallows JS exceptions** — Medium — native-lib/node/src/addon.c:394-400

`call_js_read` clears a pending JS exception and only sets `bytes_read = -1`; original error context lost. **Confidence:** High

**[11] C worker `free(ctx)` outside lock — fragile lifecycle** — Medium — native-lib/c/src/dataweave.c:850-851

`free(ctx)` after unlock; currently safe but fragile w.r.t. stream teardown ordering. **Confidence:** Medium

**[12] C `json_get_string` does not unescape** — Medium — native-lib/c/src/dataweave.c:398-424

Extracted strings keep raw `\n`, `\t`, `\"` escapes; error messages garbled. **Confidence:** High

**[13] C base64 coerces invalid sextets c/d to 0** — Medium — native-lib/c/src/dataweave.c:370-385

Only sextets a/b validated for -1; c/d with -1 silently coerced to 0 → wrong bytes. **Confidence:** High

**[14] Rust isolate bootstrap thread not detached on failure** — Medium — native-lib/rust/src/ffi.rs:80-91

Detach only in success branch; on `graal_create_isolate` failure the bootstrap thread handle leaks. **Confidence:** Medium

**[15] Node transform read callback memcpy without robust size check** — Medium — native-lib/node/src/addon.c:389

Theoretical overflow if N-API returns corrupted Buffer length. **Confidence:** Low

## Low Findings

**[16] Python global instance retained on init failure** — Low — native-lib/python/src/dataweave/__init__.py:929-932

Partially constructed instance stored before `initialize()` may raise; atexit `cleanup()` runs on broken instance. **Confidence:** Low

**[17] Compiled `.pyc` checked into git** — Low — native-lib/python/src/dataweave/__pycache__/__init__.cpython-314.pyc

Generated bytecode committed; should be gitignored. **Confidence:** Low

**[18] Go `isolateInited` set before detach** — Low — native-lib/go/dataweave.go:83-86

Flag set before `graal_detach_thread`; only matters across process crash (Go has no persistent state). **Confidence:** Low

---

### Round 1 — Defender responds

**[1] CONCEDE — Real UAF race.** dataweave.c:912 detaches worker; :961-980 `dw_stream_free` never joins. Worker keeps touching `stream` linked list. Fix: store `pthread_t` in `dw_stream`, set `finished`, `pthread_join` in `dw_stream_free`. True severity: **Critical**.

**[2] CONCEDE — Caller-owned pointers captured.** :895-896 stores `script`/`inputs_json` `const char*` verbatim; detached worker derefs later; FFI_CONTRACT.md specifies no lifetime rule; no strdup. Fix: `strdup` in `dw_run_streaming`, free in worker. True severity: **Critical**.

**[3] PARTIAL — doneCh never closed; hang is latent.** Buffer is 512 (dataweave.go:372) so blocking only past 512 undrained chunks + abandoned consumer; no path ever closes `doneCh`. Fix: watchdog/cancel closes `doneCh`, or document. True severity: **High (latent)**.

**[4] PARTIAL — catch_unwind gap only under panic=abort.** Rust defaults to unwind; gap is real only if a release profile sets `panic=abort` (check Cargo.toml). True severity: **Medium (conditional)**.

**[5] CONCEDE — Mid-string '=' coerced to 0, not rejected.** `AAAA=AAA` passes len%4 and trailing-padding checks; mid '=' → sextet 0. Fix: reject non-trailing '='. True severity: **Medium**.

**[6] REBUT — defer runs on early return.** Go spec: deferred call fires when the enclosing function returns, including `return nil, err` at :154-156. `UnlockOSThread` (:151) executes. No leak. **False positive.**

**[7] CONCEDE (recharacterized) — over-locking, not deadlock.** `mpsc::channel()` (:225) is UNBOUNDED → `send` never blocks, so the deadlock premise fails. Calling `.metadata()` before draining is documented user-order. Real issue: unnecessary lock around join. True severity: **Low**.

**[8] CONCEDE — Unbounded Queue.** python/__init__.py:570,713 `Queue()` no maxsize → OOM under slow consumer. Fix: `Queue(maxsize=512)` + backpressure. True severity: **High**.

**[9] REBUT — no race.** `SendPtr` is MOVED into the closure; main thread releases ownership before spawn; pointer is an integer; only the worker derefs, after receiving ownership. **False positive.**

**[10] PARTIAL — -1 IS the documented error signal.** FFI_CONTRACT.md:138-140 defines -1 as read-callback error; exception is cleared but error propagates. Losing JS stack is a DX gap, not a correctness bug. Fix: log exception before clearing. True severity: **Low**.

**[11] REBUT — free(ctx) outside lock is fine.** `ctx` is worker-private; by :851 the worker no longer derefs `stream`. The real race is [1], not this. **False concern.**

**[12] CONCEDE — json_get_string doesn't unescape.** :412-415 skip escapes but :417-422 copy raw bytes; `\n` stays literal. Fix: proper unescape / real JSON parser. True severity: **Medium**.

**[13] PARTIAL — input is native-produced.** Result base64 comes from the native lib itself (FFI_CONTRACT.md:63), so adversarial input is out of scope; coercion still hides native bugs. Fix: reject `-1` sextets. True severity: **Low**.

**[14] CONCEDE — detach only on success.** ffi.rs:80-91 detaches bootstrap thread only in the rc==0 branch; leaks on failure. Fix: detach unconditionally. True severity: **Low**.

**[15] REBUT — size check exists.** node/addon.c:388 computes `n = min(buf_len, buffer_size)` before the memcpy at :389. `int` cast overflow is a separate, narrow concern. **False positive.**

**[16] CONCEDE — global instance retained on init failure.** python/__init__.py:929-932 sets `_global_instance` before `initialize()` may raise. Fix: try/except → reset to None. True severity: **Low**.

**[17] CONCEDE — .pyc committed.** Build artifact; gitignore + `git rm --cached`. True severity: **Low**.

**[18] REBUT — isolateInited before detach is correct.** Isolate valid once created; detaching bootstrap thread doesn't invalidate it; mutex protects flag; Go has no cross-process persistence. **False positive.**

**Scorecard:** CONCEDE [1][2][5][8][12][14][16][17] · PARTIAL [3][4][7][10][13] · REBUT (false positive) [6][9][11][15][18].

---

### Round 2 — Critic rebuts, verifies disputes, and adds new findings

**Disputed items — Critic concedes the Defender was right on all six** (verified against code/config):
- **[4] Conceded** — `native-lib/rust/Cargo.toml` has no `[profile]`/`panic="abort"` (moderator independently confirmed: only one Cargo.toml in repo, no abort profile anywhere). `catch_unwind` is correct for default unwind builds. Gap is latent only.
- **[6] Conceded** — `defer UnlockOSThread` (dataweave.go:151) is registered before the early return (:155); Go runs deferred calls on all return paths. No leak.
- **[7] Conceded** — streaming.rs:225 `mpsc::channel()` is unbounded; `send` never blocks; the deadlock premise fails. Residual: unnecessary lock around join (cosmetic).
- **[9] Conceded** — `SendPtr` is moved into the spawned closure (streaming.rs:233); ownership transfers at spawn; no race.
- **[11] Conceded** — `free(ctx)` at :851 frees the worker-private `stream_worker_context`, not `stream`. Safe. The real race is [1].
- **[15] Conceded** — addon.c:388 computes `n = min(buf_len, buffer_size)` before the memcpy at :389 (moderator confirmed). False positive.
- **[18] Conceded** — Isolate is valid once `graal_create_isolate` succeeds; detaching the bootstrap thread does not invalidate it; mutex guards the flag. Correct.

**NEW FINDINGS (Round 2):**

**[19] C `dw_stream_next` returns unprotected pointers into mutable list — iterator fragility** — Medium/High — native-lib/c/src/dataweave.c:946-949

`*out_buffer = stream->current->data` is set under the mutex, then the mutex is released before return. Caller holds a pointer into `chunk_node->data` with no lock while the worker concurrently appends nodes. Currently safe (per-chunk malloc, never moved) but the API contract hands out unprotected pointers into mutable state — a maintenance hazard if buffers ever get pooled/realloc'd. **Confidence:** Medium.

**[20] Rust write callback rejects `length == 0` — may abort a valid empty chunk** — Medium — native-lib/rust/src/streaming.rs:104

`if ctx.is_null() || buf.is_null() || length <= 0 { return -1; }`. Contract (FFI_CONTRACT.md:100-104) doesn't forbid 0-length chunks; a script emitting `""`/`[]` could produce one, and returning -1 aborts. Inconsistent with Go, which allows length==0. **Confidence:** Medium (depends on whether native lib emits 0-length chunks).

**[21] Go `writeCallbackBridge` aborts on nil context with no diagnostics** — Medium — native-lib/go/streaming_callbacks.go:26-29

On `lookupContext(handle) == nil` the bridge returns -1 (aborts the script) with no logging/error path. If the handle is stale/unregistered mid-run, the user sees a generic streaming failure with no root cause. **Confidence:** High (behavior), Medium (that nil ever occurs).

**[23] C `stream->metadata` written and read without the mutex — data race** — High — native-lib/c/src/dataweave.c:834-838, 957-959

Worker sets `stream->metadata = parse_streaming_result(...)` (:835) OUTSIDE the mutex; `dw_stream_metadata` (:957-959) reads it lock-free. Even with the "read after EOF" usage pattern, there's no acquire/release barrier guaranteeing the reader sees the fully-written pointer/struct. Torn read → NULL or partially-visible pointer → segfault on weakly-ordered archs. **Confidence:** High. *(Moderator confirmed the two accesses; note the `finished=true` signal at :847 IS under the lock, so a reader that synchronizes through the mutex/condvar in dw_stream_next would be safe — but dw_stream_metadata does not take the lock at all.)*

**[24] Python worker doesn't unwind GraalVM on post-attach exception** — Medium — native-lib/python/src/dataweave/__init__.py:606-607

If the FFI call raises after `graal_attach_thread` succeeds, the `finally` detaches the thread but doesn't let GraalVM clean up its stack; isolate may be left inconsistent. **Confidence:** Medium (depends on whether ctypes can raise post-attach).

**[22] Go allows `length == 0` (benign but undocumented/ inconsistent with Rust [20])** — Low — native-lib/go/streaming_callbacks.go:37

**[25] RETRACTED by Critic** — Node `transform_read_cb` DOES destroy mutex/cond on the error path (addon.c:422-423). Not a defect.

---

### Round 2 — Defender responds to new findings

**[19] PARTIAL → Low.** Chunks are `malloc`'d per-node (:779) and never moved, so the handed-out `data` pointer is stable and valid until the node is freed. Real, narrow contract issue: pointer lifetime is undocumented and the design is fragile. Fix: document "valid until next `dw_stream_next`/free," or copy out.

**[20] REBUT.** `length <= 0` is a defensive guard; an empty DataWeave result is signaled by *not* calling the write callback, not by a 0-length call. Rejecting 0 prevents under-read corruption; Go's *lack* of the check is the minor inconsistency, not Rust's guard. Severity: None (Rust), Low note (Go).

**[21] CONCEDE → Medium.** streaming_callbacks.go:26-29 returns -1 on nil context with no diagnostics; stale/bad handle → generic failure, lost root cause. Fix: log the handle (stderr/global error) before aborting.

**[23] CONCEDE → High.** dataweave.c:835 writes `stream->metadata` outside the lock; dw_stream_metadata:958 reads it lock-free. The mutex/condvar establishes happens-before for `finished` but NOT for the separate `metadata` field. On weak-memory archs the read may see stale NULL or a torn pointer. Fix: write metadata under the lock (before :847), or atomic store/load, or lock in dw_stream_metadata.

**[24] PARTIAL → Low.** `graal_detach_thread` in the `finally` (:606) IS the correct GraalVM teardown for an attached thread; Python cannot "unwind" the native stack and doesn't need to. The isolate survives detach. Corruption claim unproven. No fix required. *(Moderator confirmed: :580-607 — detach is the only and correct cleanup.)*

**[22] Acknowledged — Low.** Go allowing length==0 is benign inconsistency vs Rust.

---

## Moderator adjudication & convergence

The debate converged after two rounds. Independent moderator verification (Cargo.toml profile, addon.c:388 guard, dataweave.c:835/957 metadata accesses, python:580-607 detach) backs the verdicts below.

**Confirmed defects (agreed by both agents):**

| # | Severity | Area | Issue | Fix |
|---|----------|------|-------|-----|
| 1 | **Critical** | C streaming | Detached worker never joined; `dw_stream_free` can free `stream` under a live worker → UAF | Store `pthread_t`, signal + `pthread_join` in free |
| 2 | **Critical** | C streaming | Worker stores caller-owned `script`/`inputs_json` pointers; async deref → UAF | `strdup` on entry, free in worker |
| 3 | **High** | Go streaming | `doneCh` created but never closed; abandoned consumer past the 512-buffer hangs the callback goroutine + holds GraalVM thread | Close `doneCh` on abandonment/cancel; add finalizer/Close() |
| 23 | **High** | C streaming | `stream->metadata` written/read without lock → data race, torn/NULL read | Write under lock or atomic |
| 8 | **High** | Python streaming | Unbounded `Queue()` → OOM under slow consumer | `maxsize` + backpressure |
| 5 | **Medium** | C base64 | Mid-string `=` coerced to 0, decodes garbage silently | Reject non-trailing `=` |
| 12 | **Medium** | C JSON | `json_get_string` doesn't unescape `\n`/`\t`/`\"` → garbled error messages | Proper unescape / real JSON parser |
| 10 | **Medium** | Node | Read-callback clears JS exception, propagates only -1 → lost error context | Stringify/log exception before clearing |
| 21 | **Medium** | Go streaming | nil-context callback aborts with no diagnostics | Log handle before returning -1 |
| 13 | **Low** | C base64 | Invalid sextets c/d coerced to 0 (input is native-produced, so low risk) | Reject `-1` sextets |
| 14 | **Low** | Rust FFI | Bootstrap thread not detached if `graal_create_isolate` fails | Detach unconditionally |
| 16 | **Low** | Python | Global instance retained if `initialize()` raises | try/except → reset to None |
| 17 | **Low** | Python | Compiled `.pyc` committed to git | gitignore + `git rm --cached` |
| 19 | **Low** | C streaming | `dw_stream_next` returns unprotected/undocumented pointer lifetime | Document lifetime or copy |
| 22 | **Low** | Go | Allows length==0 (benign; inconsistent with Rust) | Optional consistency |

**Dismissed as false positives (Critic conceded):** [6] Go defer, [7] Rust mpsc unbounded (no deadlock), [9] Rust SendPtr move, [11] C `free(ctx)`, [15] Node memcpy guard, [18] Go isolateInited ordering.

**Downgraded/conditional:** [4] Rust `catch_unwind` gap — latent only, no `panic="abort"` profile exists; [20] Rust `length<=0` guard is correct (not a bug); [24] Python detach is correct GraalVM teardown (not corruption).

**Cross-cutting observations:**
- **Doc-to-code ratio is extreme.** ~15k of ~23.5k added lines are markdown (plans, summaries, review reports, comparisons). Several read as generated status/marketing docs (`FIX-SUMMARY.md`, `MERGE_SUMMARY.md`, `FINAL delivery report`, multiple review `.md`s). This inflates the diff and should be pruned before merge.
- **The two Critical bugs are both in the C streaming worker** — that subsystem needs the most attention and ideally a TSan/ASan run.
- **Tests don't gate on absence of the native lib** (no `t.Skip`/`#[ignore]`), so CI must build `dwlib` for the suites to run — otherwise link failures, not skips.

---

## Reproductions (red tests, written before any fix)

Deterministic reproductions were added for the confirmed defects. They run
without the GraalVM `dwlib` build (mocks + standalone models), so they stay fast
and hermetic; the fixes were *additionally* verified against the real `dwlib`
end-to-end (see "End-to-end verification" below). Each asserts the *correct*
behavior, so it fails/faults today and will pass once the bug is fixed.

**C — `native-lib/c/tests/repro/` (`./run.sh`)** — all reproduce (ASan + TSan):
- **[1] Critical** detached-worker UAF — ASan `heap-use-after-free` (READ size 8) in `stream_write_callback` reading a `stream` freed by `dw_stream_free`, on worker thread T1. Stack: `stream_write_callback ← run_script_callback ← stream_worker_thread`.
- **[2] Critical** caller-owned pointers UAF — ASan `heap-use-after-free` (READ size 2) in `run_script_callback` `strlen`-ing a `script` buffer the caller freed; wrapper stored the pointer without `strdup`.
- **[23] High** metadata data race — TSan `data race`: `stream_worker_thread` writes `stream->metadata` (dataweave.c:835, outside the lock) vs. the lock-free read in `dw_stream_metadata` (:958). Same mock+barrier; a separate `-fsanitize=thread` binary (`repro_metadata_race`, cannot combine with ASan), isolated to exactly one race by not freeing on exit.
- **[5] Medium** base64 accepts non-trailing `=` — `dw_base64_decode("AB=DEFGH")` returns non-NULL.
- **[13] Low** base64 coerces invalid sextet to 0 — `dw_base64_decode("AB-DEFGH")` returns non-NULL.
- **[12] Medium** `json_get_string` no unescape — `"a\nb"` yields a literal backslash-n.

Technique (UAF + race): the wrapper `dlopen`s its native lib, so a mock `dwlib`
(`mock_dwlib.c`) substitutes via `DATAWEAVE_NATIVE_LIB`; a barrier parks the
worker inside `run_script_callback` after it captures the stream/script
pointers. For the UAFs the test frees them, then releases the worker → ASan. For
[23] the test releases the worker (which writes `metadata` unlocked) while the
main thread hammers `dw_stream_metadata` lock-free → TSan.

**Go — `native-lib/go/repro/` (`go test ./...`)** — both reproduce:
- **[3] High** `doneCh` never closed → abandoned-consumer hang. Standalone model
  mirroring `dataweave.go` (512 buffer + created-but-never-closed `doneCh`) and
  `streaming_callbacks.go`'s select. Verified against source: `doneCh` is never
  passed to `close()` anywhere in the package. Test fails via 2s timeout (the hang).
- **[21] Medium** nil-context abort has no diagnostics. Model of
  `writeCallbackBridge`'s `ctx == nil` branch (streaming_callbacks.go:27): it
  `return -1`s with nothing logged. Test asserts a diagnostic naming the bad
  handle is recorded on abort; today none is → fails.

**Python — `native-lib/python/repro/` (`python3 test_unbounded_queue.py`)** — reproduces:
- **[8] High** unbounded `Queue()` → no backpressure/OOM. Standalone model of the
  `Queue()` + `q.put` write callback (`__init__.py:570,573`). With a stalled
  consumer the producer enqueues all 200 000 chunks (peak qsize == 200 000 ≫ the
  512 bound) → fails. Fix: `Queue(maxsize=512)` + blocking callback.

**Node — `native-lib/node/repro/` (`./run.sh`)** — reproduces:
- **[10] Low** read callback swallows JS exceptions. Dependency-free C model of
  `call_js_read`'s `napi_get_and_clear_last_exception` branch (addon.c:394-401):
  the exception is cleared and discarded, so the thrown message is lost behind a
  generic `-1`. (C model for a fast, hermetic check; the fix is additionally
  verified end-to-end by the real vitest suite — see below.) Test asserts the
  message survives; today it doesn't.

All confirmed, runnable defects now have red tests. Remaining un-reproduced
items are the Low-severity/documentation findings ([14][16][17][19][22]) whose
"correct behavior" is a doc/consistency change rather than an observable fault.

---

## Fixes applied (all repros flipped green)

Fixed in parallel, one agent per wrapper. Each red test above was turned into a
regression guard that now **passes** and fails if the bug is reintroduced.

**C — `native-lib/c/src/dataweave.c`:**
- **[1]** `dw_stream` gains `bool worker_started`; `dw_run_streaming` stores the tid in `stream->worker_thread` (no longer detaches) and `dw_stream_free` `pthread_join`s it before freeing. Added a Windows `pthread_join` shim (`WaitForSingleObject`+`CloseHandle`).
- **[2]** worker context owns `strdup`'d copies of `script`/`inputs_json` (NULL inputs → `"{}"`), freed on every worker exit path; caller may free its buffers immediately.
- **[23]** `stream->metadata` is written under `stream->mutex` (folded into the finish lock) and `dw_stream_metadata` reads it under the lock.
- **[5]/[13]** `dw_base64_decode` rejects `=` before the final quantum and rejects invalid sextets in the c/d slots (returns NULL). Valid base64 round-trips verified (`TWFu`→`Man`, padding cases, `Hello, World!`).
- **[12]** `json_get_string` unescapes `\n \t \r \b \f \" \\ \/`; unknown escapes (e.g. `\uXXXX`) kept literal.

**Go — `native-lib/go/{dataweave.go,streaming_callbacks.go}`:**
- **[3]** `StreamResult` gains an idempotent `Close()` (`sync.Once` → `close(doneCh)`) so an abandoned consumer unblocks the write callback (`<-doneCh` → -1). Callers `defer sr.Close()`.
- **[21]** `writeCallbackBridge`/`readCallbackBridge` log the offending handle to stderr before the nil-context abort; negative-length abort also logged.
- **[3b] (regression found during review of the fix)** the fix originally closed `doneCh` in **two** places — the worker's `defer close(doneCh)` *and* `Close()` — so a naturally-completing stream that the caller also `Close()`s double-closes the channel → `panic: close of closed channel`. Fixed by making `Close()` the **sole** owner (removed both worker-side `defer close(doneCh)`). New guard `double_close_test.go` covers it; the abandoned-consumer model was also made deterministic (fill buffer while `doneCh` open, confirm the overflow write blocks, *then* abandon — the earlier model was flaky).

**Python — `native-lib/python/src/dataweave/__init__.py`:**
- **[8]** both streaming paths use `Queue(maxsize=512)` (`_OUTPUT_QUEUE_MAXSIZE`); the write callback does `q.put(..., timeout=30)` and returns -1 on timeout — backpressure onto the native producer without indefinite blocking if the consumer vanishes.

**Node — `native-lib/node/src/addon.c`:**
- **[10]** `call_js_read` extracts the pending exception's `message`/`stack` (`napi_get_named_property` + `napi_get_value_string_utf8`) and logs them to stderr with a clear prefix before clearing it, instead of silently discarding.

**Verification (independently re-run, not self-reported):**
- C `run.sh`: 4/4 pass — clean ASan (both UAF modes), clean TSan (metadata), pure helpers all correct.
- Go `go test ./...`: 3/3 pass, deterministic over 20× stress ([3] abandon, [3b] double-close, [21] diagnostics).
- Python: exit 0 — producer held at qsize 512, does not run to completion under a stalled consumer.
- Node model: exit 0 — exception message preserved (C model of `call_js_read`).

### End-to-end verification against the real `dwlib`

The GraalVM native library **is present in this repo** —
`native-lib/build/native/nativeCompile/dwlib.dylib` (100 MB, arm64, exporting
`run_script`, `run_script_callback`, `run_script_input_output_callback`,
`free_cstring`, and the `graal_*` isolate entry points). An earlier draft of this
report wrongly claimed it was absent/multi-GB and that nothing could be compiled
end-to-end; that was incorrect. All four wrappers were built and run against the
real library:

- **C** — `make test`: **10/10 pass** (real `dwlib`, no mock).
- **Go** — `go test`: **12/12 pass**. Required a `libdwlib.dylib -> dwlib.dylib`
  symlink so cgo's `-ldwlib` resolves; also fixed a stray unused `os` import the
  fix left in `dataweave.go` (only surfaced when cgo actually links).
- **Python** — `pytest`: **16/16 pass**.
- **Node** — `vitest`: **14/14 pass**, including the `runStreaming` (4) and
  `runTransform` (3) suites that exercise the `[10]` read/write callbacks
  end-to-end.

### [T] Pre-existing crash on isolate teardown (found & fixed during e2e)

Running the Node suite against the real `dwlib` initially crashed with a fatal
`StackOverflowError` inside `graal_tear_down_isolate` ("wrong IsolateThread").
Reduced to a minimal `run()` + `cleanup()` script (no streaming, no `[10]` path),
proving it is **pre-existing and unrelated to the `[10]` fix**. Two root causes in
`native-lib/node/src/addon.c`, both fixed:

1. `cleanup_thread_fn` passed `g_thread` (the IsolateThread created on the *init*
   OS thread) to `graal_tear_down_isolate` from a *different, freshly-spawned* OS
   thread. GraalVM requires the calling thread's own IsolateThread → fatal abort.
   Fix: `graal_attach_thread` the cleanup thread and tear down with that handle.
2. The init thread created the isolate and exited **without detaching**, leaving a
   phantom attached (dead) thread. `graal_tear_down_isolate` then blocks forever
   waiting for it to reach a safepoint. Fix: detach the bootstrap thread right
   after `graal_create_isolate` (mirrors the Go binding, dataweave.go:85).

With both fixes the full Node suite passes and the process exits 0.

_End of transcript._
