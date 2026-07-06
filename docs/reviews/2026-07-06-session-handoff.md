# Session Handoff — Adversarial Native-Bindings Review: Fixes + End-to-End Verification

**Branch:** `feat/new-native-bindings` · **Repo:** `data-weave-cli`
**Subject:** FFI wrappers (C, Go, Python, Node) around the GraalVM native lib `dwlib`.
**Purpose:** hand off to a cross-validating agent. Everything below is committed.

## What this session did (in order)
1. Wrote red reproductions for the remaining confirmed findings **before** any fix (test-first).
2. Fixed all confirmed defects, one agent per language wrapper, in parallel.
3. Caught defects the parallel fixers introduced/missed (see `[3b]` below).
4. **Verified every fix end-to-end against the real `dwlib.dylib` that lives in the repo** — the prior claim that it was unavailable was wrong.
5. Found & fixed a **pre-existing** GraalVM teardown crash surfaced only by real e2e runs (`[T]`).
6. Corrected the review doc's stale caveats.

## The real native library
`native-lib/build/native/nativeCompile/dwlib.dylib` — **100 MB, arm64**, exports
`run_script`, `run_script_callback`, `run_script_input_output_callback`,
`free_cstring`, and `graal_create_isolate` / `graal_attach_thread` /
`graal_detach_thread` / `graal_tear_down_isolate`. (Not multi-GB, not absent — an
earlier draft of the review doc claimed otherwise; that was incorrect.)

## Fixes to verify (source files changed)

### `native-lib/c/src/dataweave.c`
- **[1] Critical** — detached streaming worker never joined → UAF. Added `bool worker_started`; store tid in `stream->worker_thread` (removed `pthread_detach`); `dw_stream_free` `pthread_join`s before free. Added a Windows `pthread_join` shim (`WaitForSingleObject` + `CloseHandle`).
- **[2] Critical** — worker stored caller-owned `script`/`inputs_json` → UAF. Worker now owns `strdup`'d copies (NULL inputs → `"{}"`), freed on every exit path; caller may free immediately.
- **[23] High** — `stream->metadata` write and read now both under `stream->mutex`.
- **[5]/[13]** — `dw_base64_decode` rejects `=` before the final quantum and rejects invalid sextets in the c/d slots (returns NULL).
- **[12]** — `json_get_string` unescapes `\n \t \r \b \f \" \\ \/`; unknown escapes (e.g. `\uXXXX`) kept literal.

### `native-lib/go/dataweave.go` + `streaming_callbacks.go`
- **[3] High** — `doneCh` created but never closed → abandoned-consumer hang. Added idempotent `StreamResult.Close()` (`sync.Once` → `close(doneCh)`); callers `defer sr.Close()`.
- **[3b] regression found during review of the fix** — the original fix closed `doneCh` in **two** places (worker `defer close(doneCh)` *and* `Close()`), so a naturally-completing stream that the caller also `Close()`s double-closes the channel → `panic: close of closed channel`. Fixed: `Close()` is the **sole** owner; removed both worker-side closes.
- **[21] Medium** — nil-context / negative-length callback aborts now log the offending handle to stderr before returning -1.
- Removed a stray unused `os` import in `dataweave.go` (a real compile error the fix left behind — only surfaces when cgo actually links).

### `native-lib/python/src/dataweave/__init__.py`
- **[8] High** — unbounded `Queue()` → no backpressure/OOM. Both streaming paths use `Queue(maxsize=512)` (`_OUTPUT_QUEUE_MAXSIZE`); the write callback does `q.put(..., timeout=30)` and returns -1 on timeout.

### `native-lib/node/src/addon.c`
- **[10] Low** — read callback swallowed JS exceptions. `call_js_read` now extracts the pending exception's `message`/`stack` and logs them to stderr with a clear prefix before clearing.
- **[T] pre-existing teardown crash — found via real e2e, unrelated to [10]:**
  1. `cleanup_thread_fn` passed `g_thread` (the IsolateThread created on the now-dead init thread) to `graal_tear_down_isolate` from a *fresh* OS thread → fatal `StackOverflowError` ("wrong IsolateThread"). Fix: `graal_attach_thread` the cleanup thread and tear down with *its own* handle.
  2. After that fix, teardown *hung* forever: the init thread created the isolate then exited **without detaching**, leaving a phantom attached (dead) thread that `graal_tear_down_isolate` waits on. Fix: detach the bootstrap thread right after `graal_create_isolate` (mirrors the Go binding, `dataweave.go:85`).

## Reproductions added
- `native-lib/c/tests/repro/` — ASan `[1]`/`[2]` UAF, TSan `[23]` race, pure-helper `[5]`/`[13]`/`[12]`. Harness `run.sh` uses a mock `dwlib` (`mock_dwlib.c`) + sanitizers.
- `native-lib/go/repro/` — `donech_hang_test.go` `[3]`, `double_close_test.go` `[3b]`, `nilctx_silent_test.go` `[21]` (standalone models pinned to the shipped code structure; verified deterministic 20×).
- `native-lib/python/repro/test_unbounded_queue.py` — `[8]`.
- `native-lib/node/repro/repro_read_swallow.c` — `[10]` C model.

## Verification results (what the cross-validator should reproduce)
Set `LIB=native-lib/build/native/nativeCompile`, export `DYLD_LIBRARY_PATH=$LIB`
(and `DATAWEAVE_NATIVE_LIB=$LIB/dwlib.dylib` for Python/Node).

| Wrapper | Command | Expected |
|---|---|---|
| C | `cd native-lib/c && make test` | **10/10 pass** (real lib, no mock) |
| Go | `cd native-lib/go && go test .` | **pass** (use `.`, not `./...` — see below) |
| Python | `cd native-lib/python && pytest -q` | **17 passed** (16 module + the `[8]` repro) |
| Node | `cd native-lib/node && npm test` | **14/14 pass**, exit 0, no StackOverflow |
| Node minimal | `run()` + `cleanup()` script | exit 0 (was fatal StackOverflow before the `[T]` fix) |

The Node `runStreaming` (4) and `runTransform` (3) suites exercise the `[10]`
read/write callbacks end-to-end.

## Known pre-existing issues (NOT ours — flag, don't fix)
- **Go `./...` fails to build `examples/`**: `main redeclared` (`simple_demo.go` and `streaming_demo.go` both define `main` in one package). Last touched in commit `1dc581e`, outside our changed set. The actual package tests (`go test .`) pass.
- **Go needs `libdwlib.dylib -> dwlib.dylib`** for cgo's `-ldwlib` to resolve (create the symlink in `$LIB` before `go test`).

## Candidate cross-validation targets (independent second look)
- **Node `g_thread`** is now vestigial (assigned, never read as load-bearing). Harmless, but confirm no other path depends on it.
- **`[T]` idempotency**: `cleanup()` is registered in both `afterAll` and `process.on("exit")` — confirm double-invocation is safe (`g_ref_count` / `g_initialized` guard it).
- **`[8]` `timeout=30`**: confirm returning -1 on timeout propagates correctly through the native producer (backpressure vs. silent drop).
- **`[3b]`**: confirm no *other* caller closes `doneCh`; `Close()` must remain the sole owner.
- **`[1]`/`[2]`**: the two Criticals are both in the C streaming worker — highest-value area for a fresh ASan/TSan pass.

## Reference
Full audit transcript, per-finding disposition, fix log, and the `[T]` teardown
section: `docs/reviews/2026-07-06-adversarial-native-bindings-review.md`.
