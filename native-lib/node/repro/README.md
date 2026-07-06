# Node binding — finding [10] regression guard

Regression guard for finding **[10]** from
`docs/reviews/2026-07-06-adversarial-native-bindings-review.md`: the addon's
read callback (`call_js_read`, `src/addon.c:394-429`) now extracts a pending
JavaScript exception's message and stack properties via `napi_get_named_property`
+ `napi_get_value_string_utf8`, and logs them to stderr with a clear prefix
before clearing the exception. This fix ensures a user whose read callback
throws (e.g. `throw new Error("db connection lost")`) sees the real cause
rather than only a generic "read failed" error.

## Why it's a standalone model

The addon is N-API + libuv and needs `node-gyp` + node headers to compile/link,
which aren't assumed present in the CI/test environment. `repro_read_swallow.c`
is a dependency-free C model of the exact exception-handling branch with tiny
N-API stand-ins. The model mirrors the fix by extracting the exception message
and recording it through `g_diag` (modeling the stderr logging in the real
addon). The test now **PASSES** (exit 0) when the message is preserved, and
would **FAIL** (exit 1) if the fix regresses.

Faithfulness check (run from `native-lib/node`):

```sh
rg -n 'get_and_clear_last_exception|fprintf.*stderr.*Read callback threw' src/addon.c
```

## Run

```sh
./run.sh
```

Exit 0 == PASS (fix is working — exception message is captured).
Exit 1 == FAIL (bug present or regressed — exception message is dropped).
