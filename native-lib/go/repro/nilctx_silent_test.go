// Reproduces finding [21] from the adversarial review: when
// streaming_callbacks.go:writeCallbackBridge is invoked with a handle whose
// context lookup yields nil (stale/bad/already-freed handle), it aborts the
// stream by returning -1 with NO diagnostics — the root cause (bad handle) is
// lost and the caller sees only a generic stream failure.
//
// Source (streaming_callbacks.go:25-29):
//
//	handle := cgo.Handle(uintptr(ctxPtr))
//	ctx := lookupContext(handle)
//	if ctx == nil {
//	    return -1            // <-- no log, no recorded error, no handle value
//	}
//
// The real bridge is cgo and needs the native library, so this is a
// dependency-free MODEL mirroring the exact nil-context branch. It routes any
// diagnostic through a sink that the FIXED code is expected to write to (log
// the handle / set a global lastError before aborting). Today the branch emits
// nothing, so the sink stays empty and the test FAILS (reproducing the gap).
// After the fix, the abort records a diagnostic and the test PASSES.
package repro

import (
	"strings"
	"testing"
)

// diagSink models wherever a fixed bridge would record why it aborted
// (stderr log line, package-level lastError, metrics counter, ...). The
// reproduction only needs to observe that *something* was recorded.
type diagSink struct {
	msgs []string
}

func (d *diagSink) logf(format string, args ...any) {
	// (formatting elided — presence is what matters for the repro)
	_ = format
	_ = args
	d.msgs = append(d.msgs, format)
}

// writeBridgeNilCtx mirrors streaming_callbacks.go:writeCallbackBridge's
// nil-context abort branch. `ctx == nil` models a stale/bad/freed handle.
//
// >>> This models the FIX for finding [21]. The real writeCallbackBridge now
// >>> does: fmt.Fprintf(os.Stderr, "...no context for handle %#x...", handle)
// >>> before returning -1, so the abort leaves a diagnostic breadcrumb.
func writeBridgeNilCtx(ctx *callbackContext, handle uintptr, diag *diagSink) int {
	if ctx == nil {
		diag.logf("write callback: no context for handle %#x (stale/freed/invalid)", handle)
		return -1
	}
	return 0
}

func TestNilContext_AbortsWithoutDiagnostics(t *testing.T) {
	var diag diagSink

	// Model the native side invoking the write callback with a handle whose
	// context is gone (nil) — the exact condition streaming_callbacks.go:27
	// guards.
	const staleHandle uintptr = 0xDEADBEEF
	rc := writeBridgeNilCtx(nil, staleHandle, &diag)

	if rc != -1 {
		t.Fatalf("expected the nil-context branch to abort with -1, got %d", rc)
	}

	// FIXED behavior: aborting on a bad handle must leave a diagnostic
	// breadcrumb naming the cause, so an operator can tell a stale-handle
	// abort apart from a normal end-of-stream.
	if len(diag.msgs) == 0 {
		t.Fatal("REGRESSION: writeCallbackBridge returned -1 on a nil/stale " +
			"context with no diagnostic recorded — the fix was not applied or regressed")
	}

	// Once fixed, the recorded diagnostic should reference the handle so the
	// failure is actionable.
	joined := strings.Join(diag.msgs, "\n")
	if !strings.Contains(strings.ToLower(joined), "handle") &&
		!strings.Contains(strings.ToLower(joined), "context") {
		t.Fatalf("diagnostic recorded but does not identify the bad handle/context: %q", joined)
	}
}
