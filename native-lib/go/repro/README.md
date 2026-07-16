# Go binding — finding regression guards (green tests)

Tests two FIXED findings from
`docs/reviews/2026-07-06-adversarial-native-bindings-review.md`:

- **[3]** (`donech_hang_test.go`) — the streaming `doneCh` is now **closed on
  abandonment** via `StreamResult.Close()`, so a consumer that abandons the
  stream causes the blocked write-callback to observe `<-doneCh` and return -1
  (no goroutine/GraalVM-thread leak). The test verifies the callback unblocks
  within the timeout.
- **[21]** (`nilctx_silent_test.go`) — `writeCallbackBridge`'s `ctx == nil`
  branch (`streaming_callbacks.go`) now **logs a diagnostic** naming the bad
  handle to stderr before returning -1, so a stale/bad `cgo.Handle` is no longer
  silently lost. The test asserts the diagnostic is recorded.

## Why it's a standalone model

The `dataweave` package is cgo and cannot compile/link without the native
`dwlib`. This test is a dependency-free model that mirrors the exact structure
of `dataweave.go` (`chunkCh` buffered at 512, `doneCh` now closed on
abandonment) and `streaming_callbacks.go` (`select { chunkCh<- ... ; <-doneCh }`
with diagnostics on nil context).

## Run

```sh
go test -v ./...
```

Both tests now **PASS** (callback unblocks on abandonment; diagnostic is
recorded on bad handle). If either fails, the fix has regressed.
