package dataweave

/*
#cgo CFLAGS: -I${SRCDIR}/../../build/native/nativeCompile
#cgo darwin LDFLAGS: -L${SRCDIR}/../../build/native/nativeCompile -ldwlib
#cgo linux LDFLAGS: -L${SRCDIR}/../../build/native/nativeCompile -ldwlib
#cgo windows LDFLAGS: -L${SRCDIR}/../../build/native/nativeCompile -ldwlib

#include <stdlib.h>

// Forward declarations for GraalVM entry points
extern char* run_script(void* thread, const char* script, const char* inputsJson);
extern void free_cstring(void* thread, char* pointer);
*/
import "C"
import (
	"encoding/base64"
	"encoding/json"
	"fmt"
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
