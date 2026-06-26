// Package dataweave provides Go bindings for the DataWeave native library.
//
// It allows executing DataWeave scripts from Go with support for:
//   - Basic synchronous execution
//   - Output streaming via channels
//   - Bidirectional streaming (input and output)
//   - Automatic resource management
//
// Basic usage:
//
//	result, err := dataweave.Run("2 + 2", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Println(result.GetString()) // "4"
//
// With inputs:
//
//	inputs := map[string]interface{}{
//		"num1": 25,
//		"num2": 17,
//	}
//	result, err := dataweave.Run("num1 + num2", inputs)
//
// Output streaming:
//
//	stream, err := dataweave.RunStreaming("output json --- (1 to 10000) map {id: $}", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//	for chunk := range stream.Chunks {
//		os.Stdout.Write(chunk)
//	}
//	metadata := stream.Metadata
//
// Context manager (explicit lifecycle):
//
//	dw, err := dataweave.New()
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer dw.Cleanup()
//
//	result, err := dw.Run("2 + 2", nil)
package dataweave

/*
#cgo CFLAGS: -I.
// The native library file is named dwlib.dylib / dwlib.so (no `lib` prefix),
// so the standard `-ldwlib` flag does not find it. Link the file directly.
#cgo darwin LDFLAGS: ${SRCDIR}/../build/native/nativeCompile/dwlib.dylib -Wl,-rpath,${SRCDIR}/../build/native/nativeCompile
#cgo linux LDFLAGS: ${SRCDIR}/../build/native/nativeCompile/dwlib.so -Wl,-rpath=${SRCDIR}/../build/native/nativeCompile

#include <stdlib.h>
#include <string.h>
#include "callbacks.h"

// Forward declarations for GraalVM types
typedef struct graal_isolate_t graal_isolate_t;
typedef struct graal_isolatethread_t graal_isolatethread_t;

// GraalVM isolate management
int graal_create_isolate(void* params, graal_isolate_t** isolate, graal_isolatethread_t** thread);
int graal_attach_thread(graal_isolate_t* isolate, graal_isolatethread_t** thread);
int graal_detach_thread(graal_isolatethread_t* thread);
int graal_tear_down_isolate(graal_isolatethread_t* thread);

// DataWeave API
char* run_script(graal_isolatethread_t* thread, const char* script, const char* inputsJson);
void free_cstring(graal_isolatethread_t* thread, char* pointer);

// Callback types
typedef int (*WriteCallback)(void* ctx, const char* buffer, int length);
typedef int (*ReadCallback)(void* ctx, char* buffer, int bufferSize);

char* run_script_callback(
    graal_isolatethread_t* thread,
    const char* script,
    const char* inputsJson,
    WriteCallback writeCallback,
    void* ctx
);

char* run_script_input_output_callback(
    graal_isolatethread_t* thread,
    const char* script,
    const char* inputsJson,
    const char* inputName,
    const char* inputMimeType,
    const char* inputCharset,
    ReadCallback readCallback,
    WriteCallback writeCallback,
    void* ctx
);
*/
import "C"
import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"unsafe"
)

var (
	// ErrNotInitialized is returned when operations are attempted on an uninitialized DataWeave instance
	ErrNotInitialized = errors.New("dataweave: runtime not initialized")

	// ErrLibraryNotFound is returned when the native library cannot be located
	ErrLibraryNotFound = errors.New("dataweave: native library not found")

	// ErrStreamingNotSupported is returned when streaming APIs are not available
	ErrStreamingNotSupported = errors.New("dataweave: streaming API not supported")
)

// ExecutionResult represents the result of a basic (non-streaming) script execution
type ExecutionResult struct {
	Success  bool   `json:"success"`
	Result   string `json:"result,omitempty"`   // base64-encoded output
	Error    string `json:"error,omitempty"`    // error message if success=false
	Binary   bool   `json:"binary"`             // whether output is binary
	MimeType string `json:"mimeType,omitempty"` // MIME type of output
	Charset  string `json:"charset,omitempty"`  // charset of output
}

// GetBytes decodes the base64-encoded result to bytes
func (r *ExecutionResult) GetBytes() ([]byte, error) {
	if !r.Success || r.Result == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(r.Result)
}

// GetString decodes the result to a UTF-8 string
func (r *ExecutionResult) GetString() (string, error) {
	if !r.Success {
		return "", fmt.Errorf("execution failed: %s", r.Error)
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

// StreamingResult represents metadata returned after a streaming execution completes
type StreamingResult struct {
	Success  bool   `json:"success"`
	Error    string `json:"error,omitempty"`
	MimeType string `json:"mimeType,omitempty"`
	Charset  string `json:"charset,omitempty"`
	Binary   bool   `json:"binary"`
}

// Stream wraps a channel that yields output chunks and provides access to metadata
type Stream struct {
	Chunks   <-chan []byte
	Metadata *StreamingResult
	done     chan struct{}
}

// Wait blocks until the stream completes and returns the metadata
func (s *Stream) Wait() *StreamingResult {
	<-s.done
	return s.Metadata
}

// InputValue represents an input binding with explicit content and metadata
type InputValue struct {
	Content    interface{}       `json:"content"`              // string or []byte
	MimeType   string            `json:"mimeType"`             // required
	Charset    string            `json:"charset,omitempty"`    // optional, defaults to UTF-8
	Properties map[string]string `json:"properties,omitempty"` // optional metadata
}

type inputDescriptor struct {
	Content    string            `json:"content"`
	MimeType   string            `json:"mimeType"`
	Charset    string            `json:"charset,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

// DataWeave wraps the GraalVM isolate and provides DataWeave execution capabilities
type DataWeave struct {
	isolate     *C.graal_isolate_t
	thread      *C.graal_isolatethread_t
	initialized bool
	mu          sync.Mutex
}

var (
	globalInstance     *DataWeave
	globalInstanceOnce sync.Once
	globalInstanceErr  error
)

// New creates a new DataWeave instance with its own GraalVM isolate.
// Caller must call Cleanup() when done, typically via defer.
func New() (*DataWeave, error) {
	dw := &DataWeave{}
	if err := dw.Initialize(); err != nil {
		return nil, err
	}
	// Note: Finalizer disabled to avoid double-cleanup issues with GraalVM isolates
	// runtime.SetFinalizer(dw, (*DataWeave).Cleanup)
	return dw, nil
}

// Initialize creates the GraalVM isolate and prepares the runtime.
//
// IMPORTANT: GraalVM's graal_isolatethread_t handle is tied to the OS thread
// that created it. Subsequent calls using dw.thread (Run, Cleanup, etc.) MUST
// happen on the same OS thread. We pin the calling goroutine to its OS thread
// here and keep it pinned until Cleanup, so the same goroutine can drive the
// instance through its full lifecycle without surprises (e.g., a goroutine
// parking on a channel and resuming on a different OS thread before Cleanup).
func (dw *DataWeave) Initialize() error {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	if dw.initialized {
		return nil
	}

	runtime.LockOSThread()

	var isolate *C.graal_isolate_t
	var thread *C.graal_isolatethread_t

	rc := C.graal_create_isolate(nil, &isolate, &thread)
	if rc != 0 {
		runtime.UnlockOSThread()
		return fmt.Errorf("failed to create GraalVM isolate: code %d", rc)
	}

	dw.isolate = isolate
	dw.thread = thread
	dw.initialized = true

	return nil
}

// Cleanup tears down the GraalVM isolate and releases resources. Must be
// called from the same goroutine that called Initialize (see Initialize doc).
func (dw *DataWeave) Cleanup() {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	if !dw.initialized {
		return
	}

	if dw.thread != nil {
		C.graal_tear_down_isolate(dw.thread)
	}

	dw.initialized = false
	dw.thread = nil
	dw.isolate = nil

	runtime.UnlockOSThread()
}

// normalizeInput converts a Go value to the FFI input descriptor format
func normalizeInput(value interface{}) (inputDescriptor, error) {
	// Check if it's already an InputValue
	if iv, ok := value.(*InputValue); ok {
		var content []byte
		switch c := iv.Content.(type) {
		case []byte:
			content = c
		case string:
			charset := iv.Charset
			if charset == "" {
				charset = "utf-8"
			}
			content = []byte(c)
		default:
			return inputDescriptor{}, fmt.Errorf("InputValue.Content must be string or []byte")
		}

		return inputDescriptor{
			Content:    base64.StdEncoding.EncodeToString(content),
			MimeType:   iv.MimeType,
			Charset:    iv.Charset,
			Properties: iv.Properties,
		}, nil
	}

	// Check if it's a map with explicit content/mimeType
	if m, ok := value.(map[string]interface{}); ok {
		if content, hasContent := m["content"]; hasContent {
			mimeType, hasMime := m["mimeType"]
			if !hasMime {
				return inputDescriptor{}, fmt.Errorf("explicit input map must have both 'content' and 'mimeType'")
			}

			var contentBytes []byte
			switch c := content.(type) {
			case []byte:
				contentBytes = c
			case string:
				contentBytes = []byte(c)
			default:
				return inputDescriptor{}, fmt.Errorf("content must be string or []byte")
			}

			desc := inputDescriptor{
				Content:  base64.StdEncoding.EncodeToString(contentBytes),
				MimeType: mimeType.(string),
			}

			if charset, ok := m["charset"].(string); ok {
				desc.Charset = charset
			}

			if props, ok := m["properties"].(map[string]string); ok {
				desc.Properties = props
			}

			return desc, nil
		}
	}

	// Auto-convert based on type
	var content string
	var mimeType string

	switch v := value.(type) {
	case string:
		content = v
		mimeType = "text/plain"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		bytes, _ := json.Marshal(v)
		content = string(bytes)
		mimeType = "application/json"
	case nil:
		content = "null"
		mimeType = "application/json"
	default:
		// Try JSON marshaling
		bytes, err := json.Marshal(v)
		if err != nil {
			content = fmt.Sprint(v)
			mimeType = "text/plain"
		} else {
			content = string(bytes)
			mimeType = "application/json"
		}
	}

	return inputDescriptor{
		Content:  base64.StdEncoding.EncodeToString([]byte(content)),
		MimeType: mimeType,
		Charset:  "utf-8",
	}, nil
}

// Run executes a DataWeave script with the given inputs and returns the buffered result
func (dw *DataWeave) Run(script string, inputs map[string]interface{}) (*ExecutionResult, error) {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	if !dw.initialized {
		return nil, ErrNotInitialized
	}

	// Normalize inputs
	normalizedInputs := make(map[string]inputDescriptor)
	for k, v := range inputs {
		desc, err := normalizeInput(v)
		if err != nil {
			return nil, fmt.Errorf("failed to normalize input %q: %w", k, err)
		}
		normalizedInputs[k] = desc
	}

	inputsJSON, err := json.Marshal(normalizedInputs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal inputs: %w", err)
	}

	cScript := C.CString(script)
	defer C.free(unsafe.Pointer(cScript))

	cInputs := C.CString(string(inputsJSON))
	defer C.free(unsafe.Pointer(cInputs))

	resultPtr := C.run_script(dw.thread, cScript, cInputs)
	if resultPtr == nil {
		return nil, errors.New("native function returned null")
	}
	defer C.free_cstring(dw.thread, resultPtr)

	resultJSON := C.GoString(resultPtr)
	var result ExecutionResult
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		return nil, fmt.Errorf("failed to parse result: %w", err)
	}

	return &result, nil
}

// callbackContext holds the context for a callback invocation
type callbackContext struct {
	writeCallback func([]byte) int
	readCallback  func(int) ([]byte, error)
	chunks        chan []byte
	metadata      *StreamingResult
	metadataChan  chan *StreamingResult
	done          chan struct{}
}

var (
	callbackContexts   = make(map[uintptr]*callbackContext)
	callbackContextMu  sync.Mutex
	callbackContextID  uintptr
)

func registerCallbackContext(ctx *callbackContext) uintptr {
	callbackContextMu.Lock()
	defer callbackContextMu.Unlock()
	callbackContextID++
	id := callbackContextID
	callbackContexts[id] = ctx
	return id
}

func unregisterCallbackContext(id uintptr) {
	callbackContextMu.Lock()
	defer callbackContextMu.Unlock()
	delete(callbackContexts, id)
}

func getCallbackContext(id uintptr) *callbackContext {
	callbackContextMu.Lock()
	defer callbackContextMu.Unlock()
	return callbackContexts[id]
}

//export goWriteCallback
func goWriteCallback(ctxPtr unsafe.Pointer, buffer *C.char, length C.int) C.int {
	id := uintptr(ctxPtr)
	ctx := getCallbackContext(id)
	if ctx == nil {
		return -1
	}

	data := C.GoBytes(unsafe.Pointer(buffer), length)

	if ctx.writeCallback != nil {
		return C.int(ctx.writeCallback(data))
	}

	if ctx.chunks != nil {
		ctx.chunks <- data
		return 0
	}

	return -1
}

//export goReadCallback
func goReadCallback(ctxPtr unsafe.Pointer, buffer *C.char, bufferSize C.int) C.int {
	id := uintptr(ctxPtr)
	ctx := getCallbackContext(id)
	if ctx == nil || ctx.readCallback == nil {
		return -1
	}

	data, err := ctx.readCallback(int(bufferSize))
	if err != nil {
		return -1
	}

	if len(data) == 0 {
		return 0 // EOF
	}

	n := len(data)
	if n > int(bufferSize) {
		n = int(bufferSize)
	}

	// Copy data to C buffer - use copy pattern for byte slice to C char array
	for i := 0; i < n; i++ {
		*(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(buffer)) + uintptr(i))) = data[i]
	}

	return C.int(n)
}

// RunCallback executes a DataWeave script and streams output via a write callback
func (dw *DataWeave) RunCallback(script string, writeCallback func([]byte) int, inputs map[string]interface{}) (*StreamingResult, error) {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	if !dw.initialized {
		return nil, ErrNotInitialized
	}

	// Normalize inputs
	normalizedInputs := make(map[string]inputDescriptor)
	for k, v := range inputs {
		desc, err := normalizeInput(v)
		if err != nil {
			return nil, fmt.Errorf("failed to normalize input %q: %w", k, err)
		}
		normalizedInputs[k] = desc
	}

	inputsJSON, err := json.Marshal(normalizedInputs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal inputs: %w", err)
	}

	ctx := &callbackContext{
		writeCallback: writeCallback,
	}
	ctxID := registerCallbackContext(ctx)
	defer unregisterCallbackContext(ctxID)

	cScript := C.CString(script)
	defer C.free(unsafe.Pointer(cScript))

	cInputs := C.CString(string(inputsJSON))
	defer C.free(unsafe.Pointer(cInputs))

	resultPtr := C.run_script_callback(
		dw.thread,
		cScript,
		cInputs,
		(C.WriteCallback)(C.writeCallbackWrapper),
		unsafe.Pointer(ctxID),
	)

	if resultPtr == nil {
		return nil, errors.New("native function returned null")
	}
	defer C.free_cstring(dw.thread, resultPtr)

	resultJSON := C.GoString(resultPtr)
	var result StreamingResult
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		return nil, fmt.Errorf("failed to parse result: %w", err)
	}

	return &result, nil
}

// RunStreaming executes a DataWeave script and yields output chunks via a channel
func (dw *DataWeave) RunStreaming(script string, inputs map[string]interface{}) (*Stream, error) {
	chunks := make(chan []byte, 10)
	done := make(chan struct{})

	stream := &Stream{
		Chunks: chunks,
		done:   done,
	}

	go func() {
		// Pin this goroutine to its OS thread so the GraalVM isolate thread
		// handle remains valid for the duration of the native call. Without
		// this, the Go runtime may move the goroutine and use the detached
		// thread state for unrelated CGO calls (causing crashes in
		// graal_tear_down_isolate later).
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		defer close(chunks)
		defer func() {
			// Close done channel last, after worker thread is detached
			close(done)
		}()

		// Attach a new thread for background work
		var workerThread *C.graal_isolatethread_t
		rc := C.graal_attach_thread(dw.isolate, &workerThread)
		if rc != 0 {
			stream.Metadata = &StreamingResult{
				Success: false,
				Error:   fmt.Sprintf("failed to attach worker thread: code %d", rc),
			}
			return
		}
		defer func() {
			// Detach worker thread before signaling done
			C.graal_detach_thread(workerThread)
		}()

		// Normalize inputs
		normalizedInputs := make(map[string]inputDescriptor)
		for k, v := range inputs {
			desc, err := normalizeInput(v)
			if err != nil {
				stream.Metadata = &StreamingResult{
					Success: false,
					Error:   fmt.Sprintf("failed to normalize input %q: %v", k, err),
				}
				return
			}
			normalizedInputs[k] = desc
		}

		inputsJSON, err := json.Marshal(normalizedInputs)
		if err != nil {
			stream.Metadata = &StreamingResult{
				Success: false,
				Error:   fmt.Sprintf("failed to marshal inputs: %v", err),
			}
			return
		}

		ctx := &callbackContext{
			chunks: chunks,
		}
		ctxID := registerCallbackContext(ctx)
		defer unregisterCallbackContext(ctxID)

		cScript := C.CString(script)
		defer C.free(unsafe.Pointer(cScript))

		cInputs := C.CString(string(inputsJSON))
		defer C.free(unsafe.Pointer(cInputs))

		resultPtr := C.run_script_callback(
			workerThread,
			cScript,
			cInputs,
			(C.WriteCallback)(C.writeCallbackWrapper),
			unsafe.Pointer(ctxID),
		)

		if resultPtr == nil {
			stream.Metadata = &StreamingResult{
				Success: false,
				Error:   "native function returned null",
			}
			return
		}
		defer C.free_cstring(workerThread, resultPtr)

		resultJSON := C.GoString(resultPtr)
		var result StreamingResult
		if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
			stream.Metadata = &StreamingResult{
				Success: false,
				Error:   fmt.Sprintf("failed to parse result: %v", err),
			}
			return
		}

		stream.Metadata = &result
	}()

	return stream, nil
}

// RunTransform executes a DataWeave script with streaming input and output.
// Input is read from the inputStream channel, and output is yielded via the returned Stream.
func (dw *DataWeave) RunTransform(
	script string,
	inputStream <-chan []byte,
	inputName string,
	inputMimeType string,
	inputCharset string,
	inputs map[string]interface{},
) (*Stream, error) {
	chunks := make(chan []byte, 10)
	done := make(chan struct{})

	stream := &Stream{
		Chunks: chunks,
		done:   done,
	}

	go func() {
		// Pin this goroutine to its OS thread so the worker thread handle
		// remains valid throughout the native call (see RunStreaming for the
		// full explanation).
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		defer close(chunks)
		defer func() {
			// Close done channel last, after worker thread is detached
			close(done)
		}()

		// Attach a new thread for background work
		var workerThread *C.graal_isolatethread_t
		rc := C.graal_attach_thread(dw.isolate, &workerThread)
		if rc != 0 {
			stream.Metadata = &StreamingResult{
				Success: false,
				Error:   fmt.Sprintf("failed to attach worker thread: code %d", rc),
			}
			return
		}
		defer func() {
			// Detach worker thread before signaling done
			C.graal_detach_thread(workerThread)
		}()

		// Normalize inputs
		normalizedInputs := make(map[string]inputDescriptor)
		for k, v := range inputs {
			desc, err := normalizeInput(v)
			if err != nil {
				stream.Metadata = &StreamingResult{
					Success: false,
					Error:   fmt.Sprintf("failed to normalize input %q: %v", k, err),
				}
				return
			}
			normalizedInputs[k] = desc
		}

		inputsJSON, err := json.Marshal(normalizedInputs)
		if err != nil {
			stream.Metadata = &StreamingResult{
				Success: false,
				Error:   fmt.Sprintf("failed to marshal inputs: %v", err),
			}
			return
		}

		// Create read callback from input stream. Holds a leftover buffer so
		// that input chunks larger than the native buffer size are not
		// truncated — the remainder is delivered on subsequent calls.
		var leftover []byte
		readCallback := func(bufSize int) ([]byte, error) {
			if len(leftover) > 0 {
				if len(leftover) > bufSize {
					out := leftover[:bufSize]
					leftover = leftover[bufSize:]
					return out, nil
				}
				out := leftover
				leftover = nil
				return out, nil
			}
			data, ok := <-inputStream
			if !ok {
				return nil, nil // EOF
			}
			if len(data) > bufSize {
				leftover = data[bufSize:]
				return data[:bufSize], nil
			}
			return data, nil
		}

		ctx := &callbackContext{
			chunks:       chunks,
			readCallback: readCallback,
		}
		ctxID := registerCallbackContext(ctx)
		defer unregisterCallbackContext(ctxID)

		cScript := C.CString(script)
		defer C.free(unsafe.Pointer(cScript))

		cInputs := C.CString(string(inputsJSON))
		defer C.free(unsafe.Pointer(cInputs))

		cInputName := C.CString(inputName)
		defer C.free(unsafe.Pointer(cInputName))

		cInputMimeType := C.CString(inputMimeType)
		defer C.free(unsafe.Pointer(cInputMimeType))

		var cInputCharset *C.char
		if inputCharset != "" {
			cInputCharset = C.CString(inputCharset)
			defer C.free(unsafe.Pointer(cInputCharset))
		}

		resultPtr := C.run_script_input_output_callback(
			workerThread,
			cScript,
			cInputs,
			cInputName,
			cInputMimeType,
			cInputCharset,
			(C.ReadCallback)(C.readCallbackWrapper),
			(C.WriteCallback)(C.writeCallbackWrapper),
			unsafe.Pointer(ctxID),
		)

		if resultPtr == nil {
			stream.Metadata = &StreamingResult{
				Success: false,
				Error:   "native function returned null",
			}
			return
		}
		defer C.free_cstring(workerThread, resultPtr)

		resultJSON := C.GoString(resultPtr)
		var result StreamingResult
		if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
			stream.Metadata = &StreamingResult{
				Success: false,
				Error:   fmt.Sprintf("failed to parse result: %v", err),
			}
			return
		}

		stream.Metadata = &result
	}()

	return stream, nil
}

// getGlobalInstance returns the singleton DataWeave instance, creating it if necessary
func getGlobalInstance() (*DataWeave, error) {
	globalInstanceOnce.Do(func() {
		globalInstance, globalInstanceErr = New()
	})
	return globalInstance, globalInstanceErr
}

// Run executes a DataWeave script using the global instance
func Run(script string, inputs map[string]interface{}) (*ExecutionResult, error) {
	dw, err := getGlobalInstance()
	if err != nil {
		return nil, err
	}
	return dw.Run(script, inputs)
}

// RunStreaming executes a DataWeave script and yields output chunks using the global instance
func RunStreaming(script string, inputs map[string]interface{}) (*Stream, error) {
	dw, err := getGlobalInstance()
	if err != nil {
		return nil, err
	}
	return dw.RunStreaming(script, inputs)
}

// RunCallback executes a DataWeave script and streams output via callback using the global instance
func RunCallback(script string, writeCallback func([]byte) int, inputs map[string]interface{}) (*StreamingResult, error) {
	dw, err := getGlobalInstance()
	if err != nil {
		return nil, err
	}
	return dw.RunCallback(script, writeCallback, inputs)
}

// RunTransform executes a DataWeave script with streaming input and output using the global instance
func RunTransform(
	script string,
	inputStream <-chan []byte,
	inputName string,
	inputMimeType string,
	inputCharset string,
	inputs map[string]interface{},
) (*Stream, error) {
	dw, err := getGlobalInstance()
	if err != nil {
		return nil, err
	}
	return dw.RunTransform(script, inputStream, inputName, inputMimeType, inputCharset, inputs)
}

// Cleanup releases the global instance (typically called at program exit)
func Cleanup() {
	if globalInstance != nil {
		globalInstance.Cleanup()
		globalInstance = nil
	}
}

// init sets up cleanup at program exit
func init() {
	// Try to find library in various locations
	paths := []string{
		os.Getenv("DATAWEAVE_NATIVE_LIB"),
		"../../build/native/nativeCompile/dwlib.dylib",
		"../../build/native/nativeCompile/dwlib.so",
		"../../build/native/nativeCompile/dwlib.dll",
		"./dwlib.dylib",
		"./dwlib.so",
		"./dwlib.dll",
	}

	// Set LD_LIBRARY_PATH / DYLD_LIBRARY_PATH if not already set
	for _, path := range paths {
		if path == "" {
			continue
		}
		if _, err := os.Stat(path); err == nil {
			// Found it - no need to do anything, cgo handles it
			break
		}
	}
}
