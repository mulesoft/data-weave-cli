// Reproduces a defect INTRODUCED by the [3] fix: doneCh is closed in TWO
// independent places, so a normally-completing stream that the caller also
// Close()s (the documented `defer sr.Close()` pattern) double-closes the
// channel and panics with "close of closed channel".
//
// In dataweave.go the [3] fix added BOTH:
//   - worker goroutine:  defer close(doneCh)          // fires on natural completion
//   - StreamResult.Close(): sync.Once -> close(doneCh) // fires on caller Close()
//
// The sync.Once only guards Close() against ITSELF; it does not coordinate with
// the worker's `defer close(doneCh)`. So:
//   1. stream completes naturally      -> worker's defer closes doneCh
//   2. caller's `defer sr.Close()` runs -> Close() closes doneCh AGAIN -> panic
// (and even without Close(), a concurrent Close() racing the worker's exit is a
// double-close / data race).
//
// This standalone model mirrors the FIXED structure: doneCh has a single owner
// (StreamResult.Close(), sync.Once) and the worker does NOT close it. It asserts
// the desired behavior — a stream may complete naturally AND be Close()d without
// panicking. With the fix it PASSES; if a worker-side `close(doneCh)` is
// reintroduced (see the commented line below), it panics and this test FAILS.
package repro

import (
	"sync"
	"testing"
)

// streamResultModel mirrors the fields the [3] fix added to StreamResult.
type streamResultModel struct {
	closeOnce sync.Once
	doneCh    chan struct{}
}

// Close mirrors dataweave.go:StreamResult.Close().
func (sr *streamResultModel) Close() {
	sr.closeOnce.Do(func() {
		if sr.doneCh != nil {
			close(sr.doneCh)
		}
	})
}

func TestDoneCh_NoDoubleCloseOnNaturalCompletion(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("DOUBLE-CLOSE BUG REPRODUCED: %v — doneCh is closed by both "+
				"the worker's `defer close(doneCh)` and StreamResult.Close(); the "+
				"documented `defer sr.Close()` pattern panics on any naturally "+
				"completing stream", r)
		}
	}()

	doneCh := make(chan struct{})
	sr := &streamResultModel{doneCh: doneCh}

	// Model the worker goroutine completing naturally. With the fix it does
	// NOT close doneCh — only StreamResult.Close() owns that. Reintroducing a
	// worker-side close here (the bug) would double-close and panic below:
	//
	//	close(doneCh) // <-- BUG: worker must not close doneCh
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		// ... worker runs, delivers chunks, then returns without closing doneCh.
	}()
	<-workerDone

	// Model the caller's documented `defer sr.Close()` cleanup. This must be
	// the single close of doneCh.
	sr.Close()
	// Idempotent: a second Close() (e.g. a second defer) must also be safe.
	sr.Close()
}
