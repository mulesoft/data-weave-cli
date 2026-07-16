# C binding — regression tests for adversarial review findings

These tests verify the fixes for six findings from the adversarial review
(`docs/reviews/2026-07-06-adversarial-native-bindings-review.md`). Each asserts
the *correct* behavior and should PASS with the fixes applied. They need no
GraalVM build.

## Run

```sh
./run.sh
```

Requires a C compiler with AddressSanitizer (Apple clang or gcc). Exit 0 means
all findings are fixed (tests pass).

## What each covers

| Finding | Severity | Driver | Mechanism |
|---------|----------|--------|-----------|
| [1] detached streaming worker never joined → UAF on freed `stream` | Critical | `repro_stream_uaf stream` | ASan: `stream_write_callback` reads `stream` freed by `dw_stream_free` |
| [2] `dw_run_streaming` stores caller-owned `script`/`inputs` → UAF | Critical | `repro_stream_uaf script` | ASan: `run_script_callback` reads `script` freed by caller |
| [23] `stream->metadata` written unlocked, read lock-free → data race | High | `repro_metadata_race` | TSan: `stream_worker_thread` write vs `dw_stream_metadata` read |
| [5] base64 accepts non-trailing `=` → silent garbage | Medium | `repro_pure` | `dw_base64_decode("AB=DEFGH")` should return NULL |
| [13] base64 coerces invalid sextet to 0 → garbage | Low | `repro_pure` | `dw_base64_decode("AB-DEFGH")` should return NULL |
| [12] `json_get_string` doesn't unescape | Medium | `repro_pure` | `"a\nb"` should decode to a real newline |

`repro_metadata_race` is a **separate ThreadSanitizer** binary (TSan and ASan
cannot be combined). `run.sh` skips it if the compiler lacks `-fsanitize=thread`.

## How the streaming UAFs are made deterministic

The real `dwlib` is a multi-GB GraalVM Native Image. The wrapper loads its
native library via `dlopen`/`dlsym` at runtime, so `mock_dwlib.c` substitutes a
fake one (selected with `DATAWEAVE_NATIVE_LIB`). Its `run_script_callback`
blocks on a barrier right after the worker captures the stream + script
pointers; the test then frees those objects and releases the worker, so the
next dereference is a use-after-free that AddressSanitizer flags on thread T1
(the worker). Symbolized reports land in `out_script.log` / `out_stream.log`.

## Artifacts (git-ignored)

`libdwlib_mock.*`, `dwlib.*`, `repro_pure`, `repro_stream_uaf`, `out_*.log`,
`*_full.log` are build outputs — see `.gitignore` here.
