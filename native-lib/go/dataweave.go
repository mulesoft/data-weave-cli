package dataweave

/*
#cgo CFLAGS: -I${SRCDIR}/../../build/native/nativeCompile
#cgo darwin LDFLAGS: -L${SRCDIR}/../../build/native/nativeCompile -ldwlib
#cgo linux LDFLAGS: -L${SRCDIR}/../../build/native/nativeCompile -ldwlib
#cgo windows LDFLAGS: -L${SRCDIR}/../../build/native/nativeCompile -ldwlib

#include <stdlib.h>
#include <string.h>

// Forward declarations for GraalVM entry points
extern char* run_script(void* thread, const char* script, const char* inputsJson);
extern void free_cstring(void* thread, char* pointer);

// Callback type definitions
typedef int (*WriteCallback)(void* ctx, const char* buffer, int length);
typedef int (*ReadCallback)(void* ctx, char* buffer, int bufferSize);

extern char* run_script_callback(void* thread, const char* script,
                                  const char* inputsJson, WriteCallback cb, void* ctx);
extern char* run_script_input_output_callback(void* thread, const char* script,
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
	"sync"
	"unsafe"
)

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

	cScript := C.CString(script)
	defer C.free(unsafe.Pointer(cScript))

	cInputs := C.CString(inputsJson)
	defer C.free(unsafe.Pointer(cInputs))

	cResult := C.run_script(nil, cScript, cInputs)
	if cResult == nil {
		return nil, fmt.Errorf("run_script returned NULL")
	}
	defer C.free_cstring(nil, cResult)

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
type callbackContext struct {
	chunkCh chan []byte
	reader  io.Reader
	mu      sync.Mutex
}

// contextRegistry provides a thread-safe mapping from integer handles to callback contexts.
// CGO cannot pass Go pointers to C, so we use integer handles instead.
var (
	contextMu      sync.Mutex
	contextCounter uintptr
	contextMap     = make(map[uintptr]*callbackContext)
)

func registerContext(ctx *callbackContext) uintptr {
	contextMu.Lock()
	defer contextMu.Unlock()
	contextCounter++
	handle := contextCounter
	contextMap[handle] = ctx
	return handle
}

func lookupContext(handle uintptr) *callbackContext {
	contextMu.Lock()
	defer contextMu.Unlock()
	return contextMap[handle]
}

func unregisterContext(handle uintptr) {
	contextMu.Lock()
	defer contextMu.Unlock()
	delete(contextMap, handle)
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

		cScript := C.CString(script)
		defer C.free(unsafe.Pointer(cScript))

		cInputs := C.CString(inputsJson)
		defer C.free(unsafe.Pointer(cInputs))

		cResult := C.run_script_callback(
			nil,
			cScript,
			cInputs,
			C.WriteCallback(C.writeCallbackBridge),
			unsafe.Pointer(handle),
		)

		var rawResult string
		if cResult != nil {
			rawResult = C.GoString(cResult)
			C.free_cstring(nil, cResult)
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
			nil,
			cScript,
			cInputs,
			cInputName,
			cInputMimeType,
			cInputCharset,
			C.ReadCallback(C.readCallbackBridge),
			C.WriteCallback(C.writeCallbackBridge),
			unsafe.Pointer(handle),
		)

		var rawResult string
		if cResult != nil {
			rawResult = C.GoString(cResult)
			C.free_cstring(nil, cResult)
		}

		metaCh <- parseStreamingMetadata(rawResult)
	}()

	return &StreamResult{
		Chunks:   chunkCh,
		Metadata: metaCh,
	}
}
