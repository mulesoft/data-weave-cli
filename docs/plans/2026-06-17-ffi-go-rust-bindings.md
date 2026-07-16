# FFI Bindings for Go and Rust

> For agentic workers: implement this plan task-by-task with red-green-refactor discipline. One task, one commit. Never batch.

## Overview

Add Go and Rust FFI bindings to the data-weave-cli native library (`dwlib`), enabling cloud-native applications in these languages to execute DataWeave scripts without a JVM. Include comprehensive tests and demo programs for each language.

## Context

The existing `native-lib` module builds a GraalVM native shared library (`dwlib.dylib`/`.so`/`.dll`) that exposes C-compatible entry points:
- `run_script(script, inputsJson)` — execute a script with JSON-encoded inputs, returns JSON-encoded result
- `run_script_callback(script, inputsJson, writeCallback, ctx)` — streaming output via callback
- `run_script_input_output_callback(...)` — bidirectional streaming with read/write callbacks
- `free_cstring(pointer)` — free unmanaged strings returned by the above

The Python bindings in `native-lib/python/` use `ctypes` to call these functions and provide a high-level API. We will follow a similar pattern for Go and Rust.

## File Structure

### Created Files

Go module:
- `native-lib/go/go.mod` — module definition
- `native-lib/go/dataweave.go` — main package with FFI bindings
- `native-lib/go/dataweave_test.go` — unit tests
- `native-lib/go/examples/simple_demo.go` — demo program
- `native-lib/go/examples/streaming_demo.go` — streaming demo
- `native-lib/go/README.md` — documentation

Rust crate:
- `native-lib/rust/Cargo.toml` — crate manifest
- `native-lib/rust/src/lib.rs` — main library with FFI bindings
- `native-lib/rust/src/error.rs` — error types
- `native-lib/rust/tests/integration_test.rs` — integration tests
- `native-lib/rust/examples/simple_demo.rs` — demo program
- `native-lib/rust/examples/streaming_demo.rs` — streaming demo
- `native-lib/rust/README.md` — documentation

### Modified Files

- `native-lib/build.gradle` — add tasks for Go and Rust testing
- `native-lib/README.md` — add sections for Go and Rust bindings
- `.gitignore` — ignore Go and Rust build artifacts

## Implementation Plan

### Phase 1: Go Bindings Foundation

#### Task 1.1: Create Go module structure
**Why:** Establish module and directory layout before writing code.
**How to apply:** Create the Go directory and initialize the module.

Create directory structure:
```bash
mkdir -p native-lib/go/examples
```

Create `native-lib/go/go.mod`:
```go
module github.com/mulesoft/data-weave-cli/native-lib/go

go 1.21

require (
)
```

**Test:** Run `go mod verify` in `native-lib/go/` — should succeed with no errors.

**Commit:** `feat(native-lib): initialize Go module for FFI bindings`

---

#### Task 1.2: Write failing test for basic script execution
**Why:** TDD — define the API contract before implementation.
**How to apply:** Write the simplest possible test that calls `Run()` with a basic script.

Create `native-lib/go/dataweave_test.go`:
```go
package dataweave

import (
	"testing"
)

func TestRun_SimpleArithmetic(t *testing.T) {
	result, err := Run("2 + 2", nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if !result.Success {
		t.Fatalf("Script execution failed: %s", result.Error)
	}
	str, err := result.GetString()
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if str != "4" {
		t.Errorf("Expected '4', got '%s'", str)
	}
}
```

**Test:** Run `go test` in `native-lib/go/` — should fail with "undefined: Run".

**Commit:** `test(native-lib): add failing test for Go Run() function`

---

#### Task 1.3: Implement FFI bindings for run_script
**Why:** Satisfy the test by implementing the core FFI call.
**How to apply:** Use CGo to call the C entry point, handle string marshaling and memory management.

Create `native-lib/go/dataweave.go`:
```go
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
```

**Test:** Run `go test` in `native-lib/go/` — should pass (requires the native library to be built first via `./gradlew :native-lib:nativeCompile`).

**Commit:** `feat(native-lib): implement Go FFI bindings for run_script`

---

#### Task 1.4: Write test for script with inputs
**Why:** Verify input encoding works correctly.
**How to apply:** Test passing a map of inputs and accessing them in the script.

Add to `native-lib/go/dataweave_test.go`:
```go
func TestRun_WithInputs(t *testing.T) {
	inputs := map[string]interface{}{
		"num1": 25,
		"num2": 17,
	}
	result, err := Run("num1 + num2", inputs)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if !result.Success {
		t.Fatalf("Script execution failed: %s", result.Error)
	}
	str, err := result.GetString()
	if err != nil {
		t.Fatalf("GetString failed: %v", err)
	}
	if str != "42" {
		t.Errorf("Expected '42', got '%s'", str)
	}
}
```

**Test:** Run `go test` in `native-lib/go/` — should pass.

**Commit:** `test(native-lib): add test for Go Run with inputs`

---

#### Task 1.5: Write test for script error handling
**Why:** Verify error cases are handled correctly.
**How to apply:** Test with invalid script syntax.

Add to `native-lib/go/dataweave_test.go`:
```go
func TestRun_ScriptError(t *testing.T) {
	result, err := Run("invalid syntax here", nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if result.Success {
		t.Errorf("Expected script to fail")
	}
	if result.Error == "" {
		t.Errorf("Expected error message, got empty string")
	}
}
```

**Test:** Run `go test` in `native-lib/go/` — should pass.

**Commit:** `test(native-lib): add error handling test for Go bindings`

---

#### Task 1.6: Create simple demo program
**Why:** Provide a runnable example for users.
**How to apply:** Create a standalone program demonstrating basic usage.

Create `native-lib/go/examples/simple_demo.go`:
```go
package main

import (
	"fmt"
	"log"

	dataweave "github.com/mulesoft/data-weave-cli/native-lib/go"
)

func main() {
	fmt.Println("=== DataWeave Go Demo ===\n")

	// Example 1: Simple arithmetic
	fmt.Println("1. Simple arithmetic:")
	result, err := dataweave.Run("2 + 2", nil)
	if err != nil {
		log.Fatalf("Failed to run script: %v", err)
	}
	if !result.Success {
		log.Fatalf("Script failed: %s", result.Error)
	}
	output, _ := result.GetString()
	fmt.Printf("   2 + 2 = %s\n\n", output)

	// Example 2: With inputs
	fmt.Println("2. Script with inputs:")
	inputs := map[string]interface{}{
		"name": "World",
	}
	result, err = dataweave.Run(`"Hello, " ++ name ++ "!"`, inputs)
	if err != nil {
		log.Fatalf("Failed to run script: %v", err)
	}
	if !result.Success {
		log.Fatalf("Script failed: %s", result.Error)
	}
	output, _ = result.GetString()
	fmt.Printf("   %s\n\n", output)

	// Example 3: JSON transformation
	fmt.Println("3. JSON transformation:")
	inputs = map[string]interface{}{
		"payload": map[string]interface{}{
			"users": []map[string]interface{}{
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"},
			},
		},
	}
	script := `output application/json --- payload.users map { name: $.name }`
	result, err = dataweave.Run(script, inputs)
	if err != nil {
		log.Fatalf("Failed to run script: %v", err)
	}
	if !result.Success {
		log.Fatalf("Script failed: %s", result.Error)
	}
	output, _ = result.GetString()
	fmt.Printf("   %s\n\n", output)

	fmt.Println("Demo complete!")
}
```

**Test:** Run `go run native-lib/go/examples/simple_demo.go` — should execute and print results.

**Commit:** `docs(native-lib): add simple Go demo program`

---

#### Task 1.7: Create Go README
**Why:** Document the Go bindings for users.
**How to apply:** Write installation and usage instructions.

Create `native-lib/go/README.md`:
```markdown
# DataWeave Go Bindings

Go FFI bindings for the DataWeave native library.

## Prerequisites

1. Build the native library:
   ```bash
   ./gradlew :native-lib:nativeCompile
   ```

2. The shared library will be at:
   - macOS: `native-lib/build/native/nativeCompile/dwlib.dylib`
   - Linux: `native-lib/build/native/nativeCompile/dwlib.so`
   - Windows: `native-lib/build/native/nativeCompile/dwlib.dll`

## Installation

```bash
go get github.com/mulesoft/data-weave-cli/native-lib/go
```

Or use in a local project:
```bash
cd native-lib/go
go mod download
```

## Usage

### Basic Script Execution

```go
package main

import (
    "fmt"
    "log"
    
    dataweave "github.com/mulesoft/data-weave-cli/native-lib/go"
)

func main() {
    result, err := dataweave.Run("2 + 2", nil)
    if err != nil {
        log.Fatal(err)
    }
    if !result.Success {
        log.Fatalf("Script error: %s", result.Error)
    }
    output, _ := result.GetString()
    fmt.Println(output) // "4"
}
```

### Script with Inputs

```go
inputs := map[string]interface{}{
    "num1": 25,
    "num2": 17,
}
result, err := dataweave.Run("num1 + num2", inputs)
if err != nil {
    log.Fatal(err)
}
output, _ := result.GetString()
fmt.Println(output) // "42"
```

### JSON Transformation

```go
inputs := map[string]interface{}{
    "payload": map[string]interface{}{
        "users": []map[string]interface{}{
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        },
    },
}
script := `output application/json --- payload.users map { name: $.name }`
result, err := dataweave.Run(script, inputs)
```

## Running Tests

```bash
cd native-lib/go
go test -v
```

## Running Examples

```bash
go run examples/simple_demo.go
```

## API Reference

### `Run(script string, inputs map[string]interface{}) (*ExecutionResult, error)`

Executes a DataWeave script with the given inputs.

**Parameters:**
- `script`: DataWeave script source code
- `inputs`: Map of binding names to values (auto-encoded as JSON)

**Returns:**
- `*ExecutionResult`: Execution result with output and metadata
- `error`: FFI-level error (library not found, marshaling failure, etc.)

### `ExecutionResult`

```go
type ExecutionResult struct {
    Success  bool
    Result   string    // Base64-encoded output
    Error    string    // Error message if !Success
    Binary   bool
    MimeType string
    Charset  string
}
```

**Methods:**
- `GetBytes() ([]byte, error)` — decode result to bytes
- `GetString() (string, error)` — decode result to UTF-8 string

## Environment Variables

Set `CGO_LDFLAGS` to point to the native library if not in the default location:

```bash
export CGO_LDFLAGS="-L/path/to/native-lib/build/native/nativeCompile -ldwlib"
```
```

**Test:** Read the README and verify all examples compile.

**Commit:** `docs(native-lib): add README for Go bindings`

---

### Phase 2: Rust Bindings Foundation

#### Task 2.1: Create Rust crate structure
**Why:** Initialize the crate before writing code.
**How to apply:** Create directory and manifest file.

Create directory:
```bash
mkdir -p native-lib/rust/src
mkdir -p native-lib/rust/examples
mkdir -p native-lib/rust/tests
```

Create `native-lib/rust/Cargo.toml`:
```toml
[package]
name = "dataweave-native"
version = "0.1.0"
edition = "2021"

[dependencies]
base64 = "0.22"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
libc = "0.2"

[dev-dependencies]

[lib]
name = "dataweave"
path = "src/lib.rs"

[[example]]
name = "simple_demo"
path = "examples/simple_demo.rs"

[[example]]
name = "streaming_demo"
path = "examples/streaming_demo.rs"
```

**Test:** Run `cargo check` in `native-lib/rust/` — should succeed with no errors.

**Commit:** `feat(native-lib): initialize Rust crate for FFI bindings`

---

#### Task 2.2: Write failing test for basic script execution
**Why:** TDD — define the API before implementation.
**How to apply:** Create integration test that calls `run()` with a basic script.

Create `native-lib/rust/tests/integration_test.rs`:
```rust
use dataweave::run;

#[test]
fn test_run_simple_arithmetic() {
    let result = run("2 + 2", None).expect("run failed");
    assert!(result.success, "Script execution failed: {}", result.error.unwrap_or_default());
    let output = result.get_string().expect("get_string failed");
    assert_eq!(output, "4");
}
```

**Test:** Run `cargo test` in `native-lib/rust/` — should fail with "unresolved import `dataweave::run`".

**Commit:** `test(native-lib): add failing test for Rust run() function`

---

#### Task 2.3: Implement FFI bindings for run_script
**Why:** Satisfy the test by implementing the core FFI call.
**How to apply:** Use `extern "C"` to call the entry point, handle string conversion and memory management.

Create `native-lib/rust/src/lib.rs`:
```rust
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

mod error;
pub use error::{Error, Result};

// External C functions from the native library
extern "C" {
    fn run_script(
        thread: *mut libc::c_void,
        script: *const c_char,
        inputs_json: *const c_char,
    ) -> *mut c_char;

    fn free_cstring(thread: *mut libc::c_void, pointer: *mut c_char);
}

/// Result of a DataWeave script execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub binary: bool,
    #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub charset: Option<String>,
}

impl ExecutionResult {
    /// Decode the base64-encoded result into bytes.
    pub fn get_bytes(&self) -> Result<Vec<u8>> {
        if !self.success || self.result.is_none() {
            return Err(Error::NoResult);
        }
        let result = self.result.as_ref().unwrap();
        BASE64.decode(result).map_err(Error::Base64)
    }

    /// Decode the result into a UTF-8 string.
    pub fn get_string(&self) -> Result<String> {
        if !self.success || self.result.is_none() {
            return Err(Error::NoResult);
        }
        if self.binary {
            return Ok(self.result.as_ref().unwrap().clone());
        }
        let bytes = self.get_bytes()?;
        String::from_utf8(bytes).map_err(Error::Utf8)
    }
}

/// Execute a DataWeave script with the given inputs.
///
/// # Arguments
/// * `script` - The DataWeave script source
/// * `inputs` - Optional map of binding names to values (auto-encoded as JSON)
///
/// # Returns
/// * `Ok(ExecutionResult)` - Execution result with output and metadata
/// * `Err(Error)` - FFI-level error
pub fn run(script: &str, inputs: Option<HashMap<String, Value>>) -> Result<ExecutionResult> {
    let inputs_json = encode_inputs(inputs)?;

    let c_script = CString::new(script).map_err(|_| Error::NulByte)?;
    let c_inputs = CString::new(inputs_json).map_err(|_| Error::NulByte)?;

    unsafe {
        let result_ptr = run_script(std::ptr::null_mut(), c_script.as_ptr(), c_inputs.as_ptr());
        if result_ptr.is_null() {
            return Err(Error::NullPointer);
        }

        let c_str = CStr::from_ptr(result_ptr);
        let raw_result = c_str.to_str().map_err(|_| Error::Utf8Response)?.to_string();
        free_cstring(std::ptr::null_mut(), result_ptr);

        parse_execution_result(&raw_result)
    }
}

/// Encode inputs into the JSON format expected by the native library.
fn encode_inputs(inputs: Option<HashMap<String, Value>>) -> Result<String> {
    let mut encoded = serde_json::Map::new();
    if let Some(inputs_map) = inputs {
        for (name, value) in inputs_map {
            let content = match value {
                Value::String(s) => BASE64.encode(s.as_bytes()),
                _ => {
                    let json_str = serde_json::to_string(&value).map_err(Error::Json)?;
                    BASE64.encode(json_str.as_bytes())
                }
            };
            let mime_type = match value {
                Value::String(_) => "text/plain",
                _ => "application/json",
            };
            encoded.insert(
                name,
                json!({
                    "content": content,
                    "mimeType": mime_type,
                }),
            );
        }
    }
    serde_json::to_string(&encoded).map_err(Error::Json)
}

/// Parse the JSON response from the native library.
fn parse_execution_result(raw: &str) -> Result<ExecutionResult> {
    serde_json::from_str(raw).map_err(Error::Json)
}
```

Create `native-lib/rust/src/error.rs`:
```rust
use std::fmt;

/// Error type for DataWeave FFI operations.
#[derive(Debug)]
pub enum Error {
    /// The native library returned a null pointer.
    NullPointer,
    /// Input string contains a null byte.
    NulByte,
    /// Failed to decode base64 result.
    Base64(base64::DecodeError),
    /// Failed to parse JSON.
    Json(serde_json::Error),
    /// Failed to decode UTF-8.
    Utf8(std::string::FromUtf8Error),
    /// Response from native library is not valid UTF-8.
    Utf8Response,
    /// No result available (script failed or result is empty).
    NoResult,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NullPointer => write!(f, "Native library returned NULL"),
            Error::NulByte => write!(f, "Input contains null byte"),
            Error::Base64(e) => write!(f, "Base64 decode error: {}", e),
            Error::Json(e) => write!(f, "JSON error: {}", e),
            Error::Utf8(e) => write!(f, "UTF-8 decode error: {}", e),
            Error::Utf8Response => write!(f, "Native response is not valid UTF-8"),
            Error::NoResult => write!(f, "No result available"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
```

Create `native-lib/rust/build.rs`:
```rust
use std::env;

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let lib_dir = format!("{}/../../build/native/nativeCompile", manifest_dir);

    println!("cargo:rustc-link-search=native={}", lib_dir);
    println!("cargo:rustc-link-lib=dylib=dwlib");

    #[cfg(target_os = "macos")]
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir);
    #[cfg(target_os = "linux")]
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir);
}
```

Update `native-lib/rust/Cargo.toml` to add build script:
```toml
[package]
name = "dataweave-native"
version = "0.1.0"
edition = "2021"
build = "build.rs"
```

**Test:** Run `cargo test` in `native-lib/rust/` — should pass (requires native library built first).

**Commit:** `feat(native-lib): implement Rust FFI bindings for run_script`

---

#### Task 2.4: Write test for script with inputs
**Why:** Verify input encoding works correctly.
**How to apply:** Test passing a map of inputs.

Add to `native-lib/rust/tests/integration_test.rs`:
```rust
use serde_json::json;
use std::collections::HashMap;

#[test]
fn test_run_with_inputs() {
    let mut inputs = HashMap::new();
    inputs.insert("num1".to_string(), json!(25));
    inputs.insert("num2".to_string(), json!(17));

    let result = run("num1 + num2", Some(inputs)).expect("run failed");
    assert!(result.success, "Script execution failed: {}", result.error.unwrap_or_default());
    let output = result.get_string().expect("get_string failed");
    assert_eq!(output, "42");
}
```

**Test:** Run `cargo test` in `native-lib/rust/` — should pass.

**Commit:** `test(native-lib): add test for Rust run with inputs`

---

#### Task 2.5: Write test for script error handling
**Why:** Verify error cases are handled correctly.
**How to apply:** Test with invalid script syntax.

Add to `native-lib/rust/tests/integration_test.rs`:
```rust
#[test]
fn test_run_script_error() {
    let result = run("invalid syntax here", None).expect("run failed");
    assert!(!result.success, "Expected script to fail");
    assert!(result.error.is_some(), "Expected error message");
}
```

**Test:** Run `cargo test` in `native-lib/rust/` — should pass.

**Commit:** `test(native-lib): add error handling test for Rust bindings`

---

#### Task 2.6: Create simple demo program
**Why:** Provide a runnable example for users.
**How to apply:** Create a standalone binary demonstrating basic usage.

Create `native-lib/rust/examples/simple_demo.rs`:
```rust
use dataweave::run;
use serde_json::json;
use std::collections::HashMap;

fn main() {
    println!("=== DataWeave Rust Demo ===\n");

    // Example 1: Simple arithmetic
    println!("1. Simple arithmetic:");
    let result = run("2 + 2", None).expect("Failed to run script");
    if !result.success {
        eprintln!("Script failed: {}", result.error.unwrap_or_default());
        return;
    }
    let output = result.get_string().expect("Failed to get string");
    println!("   2 + 2 = {}\n", output);

    // Example 2: With inputs
    println!("2. Script with inputs:");
    let mut inputs = HashMap::new();
    inputs.insert("name".to_string(), json!("World"));
    let result = run(r#""Hello, " ++ name ++ "!""#, Some(inputs)).expect("Failed to run script");
    if !result.success {
        eprintln!("Script failed: {}", result.error.unwrap_or_default());
        return;
    }
    let output = result.get_string().expect("Failed to get string");
    println!("   {}\n", output);

    // Example 3: JSON transformation
    println!("3. JSON transformation:");
    let mut inputs = HashMap::new();
    inputs.insert(
        "payload".to_string(),
        json!({
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ]
        }),
    );
    let script = "output application/json --- payload.users map { name: $.name }";
    let result = run(script, Some(inputs)).expect("Failed to run script");
    if !result.success {
        eprintln!("Script failed: {}", result.error.unwrap_or_default());
        return;
    }
    let output = result.get_string().expect("Failed to get string");
    println!("   {}\n", output);

    println!("Demo complete!");
}
```

**Test:** Run `cargo run --example simple_demo` in `native-lib/rust/` — should execute and print results.

**Commit:** `docs(native-lib): add simple Rust demo program`

---

#### Task 2.7: Create Rust README
**Why:** Document the Rust bindings for users.
**How to apply:** Write installation and usage instructions.

Create `native-lib/rust/README.md`:
```markdown
# DataWeave Rust Bindings

Rust FFI bindings for the DataWeave native library.

## Prerequisites

1. Build the native library:
   ```bash
   ./gradlew :native-lib:nativeCompile
   ```

2. The shared library will be at:
   - macOS: `native-lib/build/native/nativeCompile/dwlib.dylib`
   - Linux: `native-lib/build/native/nativeCompile/dwlib.so`
   - Windows: `native-lib/build/native/nativeCompile/dwlib.dll`

## Installation

Add to `Cargo.toml`:
```toml
[dependencies]
dataweave-native = { path = "../path/to/native-lib/rust" }
```

## Usage

### Basic Script Execution

```rust
use dataweave::run;

fn main() {
    let result = run("2 + 2", None).expect("Failed to run script");
    if !result.success {
        eprintln!("Script failed: {}", result.error.unwrap_or_default());
        return;
    }
    let output = result.get_string().expect("Failed to get string");
    println!("{}", output); // "4"
}
```

### Script with Inputs

```rust
use dataweave::run;
use serde_json::json;
use std::collections::HashMap;

fn main() {
    let mut inputs = HashMap::new();
    inputs.insert("num1".to_string(), json!(25));
    inputs.insert("num2".to_string(), json!(17));

    let result = run("num1 + num2", Some(inputs)).expect("Failed to run script");
    let output = result.get_string().expect("Failed to get string");
    println!("{}", output); // "42"
}
```

### JSON Transformation

```rust
let mut inputs = HashMap::new();
inputs.insert(
    "payload".to_string(),
    json!({
        "users": [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
    }),
);
let script = "output application/json --- payload.users map { name: $.name }";
let result = run(script, Some(inputs)).expect("Failed to run script");
```

## Running Tests

```bash
cd native-lib/rust
cargo test
```

## Running Examples

```bash
cargo run --example simple_demo
```

## API Reference

### `run(script: &str, inputs: Option<HashMap<String, Value>>) -> Result<ExecutionResult>`

Executes a DataWeave script with the given inputs.

**Parameters:**
- `script`: DataWeave script source code
- `inputs`: Optional map of binding names to JSON values (auto-encoded)

**Returns:**
- `Ok(ExecutionResult)`: Execution result with output and metadata
- `Err(Error)`: FFI-level error

### `ExecutionResult`

```rust
pub struct ExecutionResult {
    pub success: bool,
    pub result: Option<String>,     // Base64-encoded output
    pub error: Option<String>,       // Error message if !success
    pub binary: bool,
    pub mime_type: Option<String>,
    pub charset: Option<String>,
}
```

**Methods:**
- `get_bytes(&self) -> Result<Vec<u8>>` — decode result to bytes
- `get_string(&self) -> Result<String>` — decode result to UTF-8 string

## Environment Variables

The build script automatically configures linking. If needed, override with:

```bash
export RUSTFLAGS="-L /path/to/native-lib/build/native/nativeCompile"
```
```

**Test:** Read the README and verify all examples compile.

**Commit:** `docs(native-lib): add README for Rust bindings`

---

### Phase 3: Gradle Integration

#### Task 3.1: Add Go test task to build.gradle
**Why:** Integrate Go testing into the Gradle build pipeline.
**How to apply:** Create task that runs `go test` after native library build.

Add to `native-lib/build.gradle`:
```groovy
tasks.register('goTest', Exec) {
  if (project.findProperty('skipGoTests')?.toString()?.toBoolean() == true) {
    enabled = false
  }

  dependsOn tasks.named('nativeCompile')
  workingDir("${projectDir}/go")
  commandLine('go', 'test', '-v')
}

tasks.named('test') {
  dependsOn tasks.named('goTest')
}
```

**Test:** Run `./gradlew :native-lib:goTest` — should execute Go tests.

**Commit:** `build(native-lib): add Gradle task for Go tests`

---

#### Task 3.2: Add Rust test task to build.gradle
**Why:** Integrate Rust testing into the Gradle build pipeline.
**How to apply:** Create task that runs `cargo test` after native library build.

Add to `native-lib/build.gradle`:
```groovy
tasks.register('rustTest', Exec) {
  if (project.findProperty('skipRustTests')?.toString()?.toBoolean() == true) {
    enabled = false
  }

  dependsOn tasks.named('nativeCompile')
  workingDir("${projectDir}/rust")
  commandLine('cargo', 'test', '--', '--test-threads=1')
}

tasks.named('test') {
  dependsOn tasks.named('rustTest')
}
```

**Test:** Run `./gradlew :native-lib:rustTest` — should execute Rust tests.

**Commit:** `build(native-lib): add Gradle task for Rust tests`

---

#### Task 3.3: Update main README with Go and Rust sections
**Why:** Make users aware of new language bindings.
**How to apply:** Add sections documenting Go and Rust support.

Add to `native-lib/README.md` after the Python section:

```markdown
## Using the library (Go examples)

All examples below assume:

```go
import dataweave "github.com/mulesoft/data-weave-cli/native-lib/go"
```

### 1) Simple script

```go
result, err := dataweave.Run("2 + 2", nil)
if err != nil {
    log.Fatal(err)
}
if !result.Success {
    log.Fatalf("Script error: %s", result.Error)
}
output, _ := result.GetString()
fmt.Println(output) // "4"
```

### 2) Script with inputs

```go
inputs := map[string]interface{}{
    "num1": 25,
    "num2": 17,
}
result, err := dataweave.Run("num1 + num2", inputs)
if err != nil {
    log.Fatal(err)
}
output, _ := result.GetString()
fmt.Println(output) // "42"
```

See [go/README.md](go/README.md) for full documentation.

## Using the library (Rust examples)

All examples below assume:

```rust
use dataweave::run;
```

### 1) Simple script

```rust
let result = run("2 + 2", None).expect("Failed to run script");
if !result.success {
    eprintln!("Script failed: {}", result.error.unwrap_or_default());
    return;
}
let output = result.get_string().expect("Failed to get string");
println!("{}", output); // "4"
```

### 2) Script with inputs

```rust
use serde_json::json;
use std::collections::HashMap;

let mut inputs = HashMap::new();
inputs.insert("num1".to_string(), json!(25));
inputs.insert("num2".to_string(), json!(17));

let result = run("num1 + num2", Some(inputs)).expect("Failed to run script");
let output = result.get_string().expect("Failed to get string");
println!("{}", output); // "42"
```

See [rust/README.md](rust/README.md) for full documentation.
```

**Test:** Read the updated README to verify formatting and links.

**Commit:** `docs(native-lib): add Go and Rust sections to main README`

---

#### Task 3.4: Update .gitignore for Go and Rust artifacts
**Why:** Prevent build artifacts from being committed.
**How to apply:** Add patterns for Go and Rust build outputs.

Add to `.gitignore`:
```
# Go
native-lib/go/examples/simple_demo
native-lib/go/examples/streaming_demo

# Rust
native-lib/rust/target/
native-lib/rust/Cargo.lock
```

**Test:** Run `git status` — Go and Rust build artifacts should not appear.

**Commit:** `chore: ignore Go and Rust build artifacts`

---

### Phase 4: Streaming Support (Optional Enhancement)

#### Task 4.1: Add Go streaming demo stub
**Why:** Placeholder for future streaming implementation.
**How to apply:** Create a demo file with a TODO comment.

Create `native-lib/go/examples/streaming_demo.go`:
```go
package main

import (
	"fmt"
)

func main() {
	fmt.Println("=== DataWeave Go Streaming Demo ===\n")
	fmt.Println("TODO: Implement streaming support using run_script_callback FFI entry point")
	fmt.Println("See native-lib/python/src/dataweave/__init__.py for reference implementation")
}
```

**Test:** Run `go run native-lib/go/examples/streaming_demo.go` — should print TODO message.

**Commit:** `docs(native-lib): add Go streaming demo stub`

---

#### Task 4.2: Add Rust streaming demo stub
**Why:** Placeholder for future streaming implementation.
**How to apply:** Create a demo file with a TODO comment.

Create `native-lib/rust/examples/streaming_demo.rs`:
```rust
fn main() {
    println!("=== DataWeave Rust Streaming Demo ===\n");
    println!("TODO: Implement streaming support using run_script_callback FFI entry point");
    println!("See native-lib/python/src/dataweave/__init__.py for reference implementation");
}
```

**Test:** Run `cargo run --example streaming_demo` in `native-lib/rust/` — should print TODO message.

**Commit:** `docs(native-lib): add Rust streaming demo stub`

---

## Testing Strategy

All tests require the native library to be built first:
```bash
./gradlew :native-lib:nativeCompile
```

### Unit Tests
- Go: `cd native-lib/go && go test -v`
- Rust: `cd native-lib/rust && cargo test`

### Integration with Gradle
```bash
./gradlew :native-lib:test
```
This runs Python, Go, and Rust tests in sequence.

### Manual Demo Verification
```bash
# Go
go run native-lib/go/examples/simple_demo.go

# Rust
cargo run --example simple_demo --manifest-path native-lib/rust/Cargo.toml
```

## Non-Goals

- Streaming API implementation (callback-based FFI) — deferred to future work; stubs provided
- Async/await wrappers for Rust — FFI calls are synchronous by design
- Go context.Context support — simple synchronous API for MVP
- Package publishing (Go module registry, crates.io) — local usage only for now

## Success Criteria

1. ✅ Go bindings call `run_script` and parse results correctly
2. ✅ Rust bindings call `run_script` and parse results correctly
3. ✅ Both languages handle errors gracefully
4. ✅ Demo programs execute successfully and produce expected output
5. ✅ Tests pass in Gradle pipeline
6. ✅ Documentation is complete and accurate

## Future Enhancements

- Implement streaming support (output streaming via `run_script_callback`)
- Implement bidirectional streaming (input/output callbacks via `run_script_input_output_callback`)
- Add benchmarks comparing Go/Rust/Python FFI overhead
- Publish packages to Go module registry and crates.io
- Add CI/CD testing across macOS, Linux, Windows
