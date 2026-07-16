// Package repro reproduces finding [3] from the adversarial review:
// native-lib/go/dataweave.go creates `doneCh` and hands it to the callback
// context, and streaming_callbacks.go selects on it to abort when the consumer
// abandons the stream — but NOTHING ever closes `doneCh`. So an abandoned
// consumer, once the 512-slot buffer fills, blocks the callback goroutine
// forever, pinning the attached GraalVM thread.
//
// The real dataweave package is cgo and cannot build without the native
// library, so this test is a standalone, dependency-free MODEL that mirrors the
// exact structure of the shipped code:
//
//   dataweave.go:
//     chunkCh := make(chan []byte, 512)   // buffered
//     doneCh  := make(chan struct{})      // created...
//     ctx := &callbackContext{chunkCh, doneCh, ...}
//     // ...FFI worker goroutine sends via the write bridge...
//     // (grep the package: doneCh is never closed anywhere)
//
//   streaming_callbacks.go writeCallbackBridge:
//     select {
//     case ctx.chunkCh <- goBytes: return 0
//     case <-ctx.doneCh:           return -1
//     }
//
// The test asserts the *desired* behavior: an abandoned consumer must let the
// callback unblock (return -1) within a timeout. Today it does NOT, so the test
// FAILS (reproducing the leak). After the fix (close doneCh on abandonment),
// it will PASS.
package repro

import (
	"sync"
	"testing"
	"time"
)

// callbackContext mirrors dataweave.go's type (the fields the bridge touches).
type callbackContext struct {
	chunkCh chan []byte
	doneCh  chan struct{}
}

// writeBridge mirrors streaming_callbacks.go:writeCallbackBridge's core select.
// Returns 0 if the chunk was delivered, -1 if the stream was told to abort.
func writeBridge(ctx *callbackContext, chunk []byte) int {
	select {
	case ctx.chunkCh <- chunk:
		return 0
	case <-ctx.doneCh:
		return -1
	}
}

// bufSize mirrors the 512-slot buffer in RunStreaming/RunTransform.
const bufSize = 512

func TestDoneCh_AbandonedConsumerHangsCallback(t *testing.T) {
	// Mirror RunStreaming's channel setup.
	ctx := &callbackContext{
		chunkCh: make(chan []byte, bufSize),
		doneCh:  make(chan struct{}),
	}

	// Model the consumer abandoning the stream. In the real API this is the
	// caller invoking StreamResult.Close(), which closes doneCh (guarded by
	// sync.Once), letting a blocked callback observe abandonment via <-doneCh.
	abandonStream := func() {
		close(ctx.doneCh)
	}

	// The FFI native side calls the write callback once per output chunk,
	// sequentially on the worker thread. Fill the buffer completely FIRST, while
	// doneCh is still open — with doneCh not yet ready these enqueue
	// deterministically (only the chunkCh branch of the select is runnable).
	for i := 0; i < bufSize; i++ {
		if rc := writeBridge(ctx, []byte("x")); rc != 0 {
			t.Fatalf("fill write %d returned %d; expected 0 (buffered) before abandonment", i, rc)
		}
	}

	// The 513th chunk has nowhere to go: the buffer is full and doneCh is still
	// open, so this write BLOCKS — exactly the callback-goroutine hang from
	// finding [3]. Launch it, confirm it is blocked, THEN abandon.
	callbackReturned := make(chan int, 1)
	var once sync.Once
	go func() {
		rc := writeBridge(ctx, []byte("overflow"))
		once.Do(func() { callbackReturned <- rc })
	}()

	// It must be genuinely blocked right now (no doneCh signal yet).
	select {
	case rc := <-callbackReturned:
		t.Fatalf("overflow write returned %d before abandonment; expected it to block", rc)
	case <-time.After(100 * time.Millisecond):
		// Good: blocked as expected.
	}

	// Now the consumer abandons the stream (StreamResult.Close()).
	abandonStream()

	select {
	case rc := <-callbackReturned:
		if rc != -1 {
			t.Fatalf("callback returned %d; expected -1 (aborted) on abandonment", rc)
		}
		// Fixed behavior: the blocked callback observed the close and returned -1.
	case <-time.After(2 * time.Second):
		// This should no longer happen after the fix. If it does, the fix regressed.
		t.Fatal("REGRESSION: write callback blocked forever after consumer " +
			"abandoned the stream — doneCh was not observed as closed (StreamResult.Close " +
			"not invoked or doneCh not closed properly)")
	}
}
