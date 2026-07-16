package dataweave

/*
#include <string.h>
*/
import "C"
import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/cgo"
	"unsafe"
)

//export writeCallbackBridge
func writeCallbackBridge(ctxPtr unsafe.Pointer, buf *C.char, length C.int) C.int {
	// Defer-recover to catch panics and prevent unwinding into C
	defer func() {
		if r := recover(); r != nil {
			// Log or ignore panic, cannot unwind into C
		}
	}()

	// Safe: ctxPtr is the handle value itself (passed as uintptr then converted to unsafe.Pointer)
	// Cast it back to cgo.Handle by converting through uintptr
	handle := cgo.Handle(uintptr(ctxPtr))
	ctx := lookupContext(handle)
	if ctx == nil {
		fmt.Fprintf(os.Stderr, "dataweave: writeCallbackBridge: no context for handle %#x (stale/freed/invalid)\n", uintptr(ctxPtr))
		return -1
	}

	// Validate length to prevent C.GoBytes panic
	if length < 0 {
		fmt.Fprintf(os.Stderr, "dataweave: writeCallbackBridge: negative length %d from native callback\n", int(length))
		return -1
	}

	// Copy bytes from C buffer to Go slice before sending
	goBytes := C.GoBytes(unsafe.Pointer(buf), length)

	// Use select with done channel to avoid blocking forever if consumer abandons
	select {
	case ctx.chunkCh <- goBytes:
		return 0
	case <-ctx.doneCh:
		return -1
	}
}

//export readCallbackBridge
func readCallbackBridge(ctxPtr unsafe.Pointer, buf *C.char, bufSize C.int) C.int {
	// Defer-recover to catch panics and prevent unwinding into C
	defer func() {
		if r := recover(); r != nil {
			// Log or ignore panic, cannot unwind into C
		}
	}()

	// Safe: ctxPtr is the handle value itself (passed as uintptr then converted to unsafe.Pointer)
	// Cast it back to cgo.Handle by converting through uintptr
	handle := cgo.Handle(uintptr(ctxPtr))
	ctx := lookupContext(handle)
	if ctx == nil {
		fmt.Fprintf(os.Stderr, "dataweave: readCallbackBridge: no context for handle %#x (stale/freed/invalid)\n", uintptr(ctxPtr))
		return -1
	}

	if ctx.reader == nil {
		return 0 // EOF
	}

	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	goSlice := make([]byte, int(bufSize))

	// Loop until we get n > 0 or a real error/EOF
	// io.Reader is allowed to return (0, nil) which should not be treated as EOF
	for {
		n, err := ctx.reader.Read(goSlice)
		if n > 0 {
			C.memcpy(unsafe.Pointer(buf), unsafe.Pointer(&goSlice[0]), C.size_t(n))
			return C.int(n)
		}
		if err != nil {
			// io.EOF signals normal end-of-stream
			if errors.Is(err, io.EOF) {
				return 0
			}
			return -1
		}
		// n == 0 && err == nil: loop and try again
	}
}
