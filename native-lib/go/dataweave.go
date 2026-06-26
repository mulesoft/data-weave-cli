package dataweave

/*
#cgo CFLAGS: -I${SRCDIR}/../build/native/nativeCompile
#cgo darwin LDFLAGS: -L${SRCDIR}/../build/native/nativeCompile -ldwlib
#cgo linux LDFLAGS: -L${SRCDIR}/../build/native/nativeCompile -ldwlib
#cgo windows LDFLAGS: -L${SRCDIR}/../build/native/nativeCompile -ldwlib

#include <stdlib.h>
#include <string.h>
#include "graal_isolate.h"

// Forward declarations for GraalVM entry points
extern char* run_script(graal_isolatethread_t* thread, const char* script, const char* inputsJson);
extern void free_cstring(graal_isolatethread_t* thread, char* pointer);

// Callback type definitions
typedef int (*WriteCallback)(void* ctx, const char* buffer, int length);
typedef int (*ReadCallback)(void* ctx, char* buffer, int bufferSize);

extern char* run_script_callback(graal_isolatethread_t* thread, const char* script,
                                  const char* inputsJson, WriteCallback cb, void* ctx);
extern char* run_script_input_output_callback(graal_isolatethread_t* thread, const char* script,
                                              const char* inputsJson,
                                              const char* inputName, const char* inputMimeType,
                                              const char* inputCharset,
                                              ReadCallback readCb, WriteCallback writeCb, void* ctx);

// Forward declarations for Go-exported callback functions.
// These are defined via //export in streaming_callbacks.go and compiled by cgo.
extern int writeCallbackBridge(void* ctx, const char* buffer, int length);
extern int readCallbackBridge(void* ctx, char* buffer, int bufferSize);
*/
import "C"
import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"runtime/cgo"
	"sync"
	"unsafe"
)

// globalIsolate is the GraalVM isolate shared by all calls into the native library.
// GraalVM Native Image requires every entry point to receive a thread attached to an
// isolate; the Go binding owns one isolate for the process lifetime and attaches the
// current OS thread on each call.
var (
	isolateOnce    sync.Once
	globalIsolate  *C.graal_isolate_t
	isolateInitErr error
)

func ensureIsolate() error {
	isolateOnce.Do(func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		var isolate *C.graal_isolate_t
		var thread *C.graal_isolatethread_t
		if rc := C.graal_create_isolate(nil, &isolate, &thread); rc != 0 {
			isolateInitErr = fmt.Errorf("graal_create_isolate failed: %d", int(rc))
			return
		}
		globalIsolate = isolate
		// Detach the bootstrap thread; subsequent calls attach the calling thread on demand.
		C.graal_detach_thread(thread)
	})
	return isolateInitErr
}

// attachCurrentThread attaches the current OS thread to the global isolate and returns
// the resulting GraalVM thread handle. Callers must runtime.LockOSThread() before
// invoking it and graal_detach_thread + runtime.UnlockOSThread() when finished.
func attachCurrentThread() (*C.graal_isolatethread_t, error) {
	if err := ensureIsolate(); err != nil {
		return nil, err
	}
	var thread *C.graal_isolatethread_t
	if rc := C.graal_attach_thread(globalIsolate, &thread); rc != 0 {
		return nil, fmt.Errorf("graal_attach_thread failed: %d", int(rc))
	}
	return thread, nil
}

// ExecutionResult represents the result of a DataWeave script execution.
type ExecutionResult struct {
	Success  bool
	Result   string
	Error    string
	Binary   bool
	MimeType string
	Charset  string
}

// GetBytes decodes the base64-encoded result into bytes.
func (r *ExecutionResult) GetBytes() ([]byte, error) {
	if !r.Success || r.Result == "" {
		return nil, fmt.Errorf("no result available")
	}
	return base64.StdEncoding.DecodeString(r.Result)
}

// GetString decodes the result into a UTF-8 string.
func (r *ExecutionResult) GetString() (string, error) {
	if !r.Success || r.Result == "" {
		return "", fmt.Errorf("no result available")
	}
	if r.Binary {
		return r.Result, nil
	}
	bytes, err := r.GetBytes()
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// Run executes a DataWeave script with the given inputs.
// inputs is a map of binding names to values (auto-encoded as JSON).
func Run(script string, inputs map[string]interface{}) (*ExecutionResult, error) {
	var inputsJson string
	if inputs != nil {
		encoded, err := encodeInputs(inputs)
		if err != nil {
			return nil, fmt.Errorf("failed to encode inputs: %w", err)
		}
		inputsJson = encoded
	} else {
		inputsJson = "{}"
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	thread, err := attachCurrentThread()
	if err != nil {
		return nil, err
	}
	defer C.graal_detach_thread(thread)

	cScript := C.CString(script)
	defer C.free(unsafe.Pointer(cScript))

	cInputs := C.CString(inputsJson)
	defer C.free(unsafe.Pointer(cInputs))

	cResult := C.run_script(thread, cScript, cInputs)
	if cResult == nil {
		return nil, fmt.Errorf("run_script returned NULL")
	}
	defer C.free_cstring(thread, cResult)

	rawResult := C.GoString(cResult)
	return parseExecutionResult(rawResult)
}

// encodeInputs converts a Go map into the JSON format expected by the native library.
func encodeInputs(inputs map[string]interface{}) (string, error) {
	encoded := make(map[string]interface{})
	for name, value := range inputs {
		switch v := value.(type) {
		case []byte:
			encoded[name] = map[string]interface{}{
				"content":  base64.StdEncoding.EncodeToString(v),
				"mimeType": "application/octet-stream",
			}
		case string:
			encoded[name] = map[string]interface{}{
				"content":  base64.StdEncoding.EncodeToString([]byte(v)),
				"mimeType": "text/plain",
			}
		default:
			jsonBytes, err := json.Marshal(v)
			if err != nil {
				return "", fmt.Errorf("failed to marshal input %s: %w", name, err)
			}
			encoded[name] = map[string]interface{}{
				"content":  base64.StdEncoding.EncodeToString(jsonBytes),
				"mimeType": "application/json",
			}
		}
	}
	result, err := json.Marshal(encoded)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

// parseExecutionResult parses the JSON response from the native library.
func parseExecutionResult(raw string) (*ExecutionResult, error) {
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, fmt.Errorf("failed to parse native response: %w", err)
	}

	result := &ExecutionResult{}
	if success, ok := parsed["success"].(bool); ok {
		result.Success = success
	}

	if !result.Success {
		if errMsg, ok := parsed["error"].(string); ok {
			result.Error = errMsg
		}
		return result, nil
	}

	if resultStr, ok := parsed["result"].(string); ok {
		result.Result = resultStr
	}
	if binary, ok := parsed["binary"].(bool); ok {
		result.Binary = binary
	}
	if mimeType, ok := parsed["mimeType"].(string); ok {
		result.MimeType = mimeType
	}
	if charset, ok := parsed["charset"].(string); ok {
		result.Charset = charset
	}

	return result, nil
}

// --- Streaming API ---

// StreamingMetadata contains metadata returned after streaming execution completes.
type StreamingMetadata struct {
	Success  bool
	Error    string
	MimeType string
	Charset  string
	Binary   bool
}

// StreamResult represents the result of a streaming DataWeave execution.
// Chunks delivers output data as it is produced. Metadata arrives after all chunks.
type StreamResult struct {
	Chunks   <-chan []byte
	Metadata <-chan StreamingMetadata
	Err      error
}

// TransformOptions configures bidirectional streaming.
type TransformOptions struct {
	InputName     string // Binding name for the streamed input (default "payload")
	InputMimeType string // MIME type of the streamed input (required)
	InputCharset  string // Charset of the streamed input (default "utf-8")
}

// callbackContext holds state shared between Go and the CGO callback.
//
// # Threading Model
//
// The native library (GraalVM) guarantees that callbacks are invoked sequentially
// on a single OS thread per script execution. This means:
//
//   - writeCallbackBridge is called sequentially (never concurrently)
//   - readCallbackBridge is called sequentially (never concurrently)
//   - No mutex needed for chunkCh writes (sent from callback thread)
//   - Mutex protects reader in case of future concurrent read callbacks
//
// The context is:
//   1. Created on the main goroutine
//   2. Registered in the global map (thread-safe via contextMu)
//   3. Passed to the FFI worker goroutine via an integer handle
//   4. Accessed from the native callback thread via lookupContext()
//   5. Unregistered after the FFI call completes
//
// # Memory Safety
//
// The handle-based lookup pattern is safe because:
//   - Handles are integers (uintptr), not Go pointers
//   - The GC cannot move integers or map entries
//   - The context remains valid until unregisterContext() is called
//   - The FFI call completes before unregisterContext() is called
type callbackContext struct {
	chunkCh chan []byte // Written by callback thread, read by consumer goroutine
	reader  io.Reader   // Read by callback thread (mutex-protected for future-proofing)
	mu      sync.Mutex  // Protects reader access
}

// contextRegistry provides a thread-safe mapping from cgo.Handle to callback contexts.
// We use cgo.Handle (Go 1.17+) which is designed for passing Go values through C code.
// This avoids unsafe.Pointer checkptr violations when the race detector is enabled.

func registerContext(ctx *callbackContext) cgo.Handle {
	return cgo.NewHandle(ctx)
}

func lookupContext(handle cgo.Handle) *callbackContext {
	return handle.Value().(*callbackContext)
}

func unregisterContext(handle cgo.Handle) {
	handle.Delete()
}

// parseStreamingMetadata parses the JSON metadata response from streaming callbacks.
func parseStreamingMetadata(raw string) StreamingMetadata {
	if raw == "" {
		return StreamingMetadata{Success: false, Error: "Empty response from native library"}
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return StreamingMetadata{Success: false, Error: fmt.Sprintf("Failed to parse metadata: %v", err)}
	}
	meta := StreamingMetadata{}
	if success, ok := parsed["success"].(bool); ok {
		meta.Success = success
	}
	if errMsg, ok := parsed["error"].(string); ok {
		meta.Error = errMsg
	}
	if mimeType, ok := parsed["mimeType"].(string); ok {
		meta.MimeType = mimeType
	}
	if charset, ok := parsed["charset"].(string); ok {
		meta.Charset = charset
	}
	if binary, ok := parsed["binary"].(bool); ok {
		meta.Binary = binary
	}
	return meta
}

// RunStreaming executes a DataWeave script and streams the output via channels.
// Output chunks are delivered as they are produced by the native engine.
func RunStreaming(script string, inputs map[string]interface{}) *StreamResult {
	var inputsJson string
	if inputs != nil {
		encoded, err := encodeInputs(inputs)
		if err != nil {
			return &StreamResult{Err: fmt.Errorf("failed to encode inputs: %w", err)}
		}
		inputsJson = encoded
	} else {
		inputsJson = "{}"
	}

	chunkCh := make(chan []byte, 64)
	metaCh := make(chan StreamingMetadata, 1)

	ctx := &callbackContext{chunkCh: chunkCh}
	handle := registerContext(ctx)

	go func() {
		defer unregisterContext(handle)
		defer close(chunkCh)
		defer close(metaCh)

		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		thread, err := attachCurrentThread()
		if err != nil {
			metaCh <- StreamingMetadata{Success: false, Error: err.Error()}
			return
		}
		defer C.graal_detach_thread(thread)

		cScript := C.CString(script)
		defer C.free(unsafe.Pointer(cScript))

		cInputs := C.CString(inputsJson)
		defer C.free(unsafe.Pointer(cInputs))

		cResult := C.run_script_callback(
			thread,
			cScript,
			cInputs,
			C.WriteCallback(C.writeCallbackBridge),
			unsafe.Pointer(&handle),
		)

		var rawResult string
		if cResult != nil {
			rawResult = C.GoString(cResult)
			C.free_cstring(thread, cResult)
		}

		metaCh <- parseStreamingMetadata(rawResult)
	}()

	return &StreamResult{
		Chunks:   chunkCh,
		Metadata: metaCh,
	}
}

// RunTransform executes a DataWeave script with streaming input and output.
// Input data is pulled from the reader and output chunks are delivered via channels.
func RunTransform(script string, inputReader io.Reader, opts TransformOptions) *StreamResult {
	if opts.InputName == "" {
		opts.InputName = "payload"
	}
	if opts.InputCharset == "" {
		opts.InputCharset = "utf-8"
	}

	var inputsJson string
	inputsJson = "{}"

	chunkCh := make(chan []byte, 64)
	metaCh := make(chan StreamingMetadata, 1)

	ctx := &callbackContext{
		chunkCh: chunkCh,
		reader:  inputReader,
	}
	handle := registerContext(ctx)

	go func() {
		defer unregisterContext(handle)
		defer close(chunkCh)
		defer close(metaCh)

		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		thread, err := attachCurrentThread()
		if err != nil {
			metaCh <- StreamingMetadata{Success: false, Error: err.Error()}
			return
		}
		defer C.graal_detach_thread(thread)

		cScript := C.CString(script)
		defer C.free(unsafe.Pointer(cScript))

		cInputs := C.CString(inputsJson)
		defer C.free(unsafe.Pointer(cInputs))

		cInputName := C.CString(opts.InputName)
		defer C.free(unsafe.Pointer(cInputName))

		cInputMimeType := C.CString(opts.InputMimeType)
		defer C.free(unsafe.Pointer(cInputMimeType))

		cInputCharset := C.CString(opts.InputCharset)
		defer C.free(unsafe.Pointer(cInputCharset))

		cResult := C.run_script_input_output_callback(
			thread,
			cScript,
			cInputs,
			cInputName,
			cInputMimeType,
			cInputCharset,
			C.ReadCallback(C.readCallbackBridge),
			C.WriteCallback(C.writeCallbackBridge),
			unsafe.Pointer(&handle),
		)

		var rawResult string
		if cResult != nil {
			rawResult = C.GoString(cResult)
			C.free_cstring(thread, cResult)
		}

		metaCh <- parseStreamingMetadata(rawResult)
	}()

	return &StreamResult{
		Chunks:   chunkCh,
		Metadata: metaCh,
	}
}
