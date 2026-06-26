package dataweave

/*
#include <string.h>
*/
import "C"
import (
	"errors"
	"io"
	"runtime/cgo"
	"unsafe"
)

//export writeCallbackBridge
func writeCallbackBridge(ctxPtr unsafe.Pointer, buf *C.char, length C.int) C.int {
	// Safe: ctxPtr is a pointer to a cgo.Handle, which is designed for
	// passing Go values through C code. This avoids checkptr violations.
	handle := *(*cgo.Handle)(ctxPtr)
	ctx := lookupContext(handle)
	if ctx == nil {
		return -1
	}

	// Copy bytes from C buffer to Go slice before sending
	goBytes := C.GoBytes(unsafe.Pointer(buf), length)

	ctx.chunkCh <- goBytes
	return 0
}

//export readCallbackBridge
func readCallbackBridge(ctxPtr unsafe.Pointer, buf *C.char, bufSize C.int) C.int {
	// Safe: ctxPtr is a pointer to a cgo.Handle.
	handle := *(*cgo.Handle)(ctxPtr)
	ctx := lookupContext(handle)
	if ctx == nil {
		return -1
	}

	if ctx.reader == nil {
		return 0 // EOF
	}

	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	goSlice := make([]byte, int(bufSize))
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
	return 0
}
