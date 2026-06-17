# Implementation Plan: Go and Rust Streaming Support

## Overview

This plan details the implementation of streaming capabilities for DataWeave's Go and Rust native bindings, mirroring the functionality already implemented in Python. The native library already exposes the required FFI entry points (`run_script_callback` and `run_script_input_output_callback`), so this work focuses on wrapping these APIs in idiomatic Go and Rust interfaces.

---

## Architecture Context

The GraalVM native library (`dwlib`) exposes three FFI entry points:

1. **`run_script`** (already implemented in Go/Rust) - Buffered execution, returns complete result
2. **`run_script_callback`** (needs Go/Rust wrapper) - Output streaming with write callback
3. **`run_script_input_output_callback`** (needs Go/Rust wrapper) - Bidirectional streaming with read/write callbacks

Python's implementation strategy (in `/native-lib/python/src/dataweave/__init__.py`) provides the reference architecture:
- Background thread for callback invocation (prevents blocking)
- Queue-based chunk delivery for streaming output
- Iterator/generator patterns for consuming streams
- Metadata capture after streaming completes

---

## Design Decisions

### Go Streaming API Design

**Option A (Recommended): Channel-based streaming with metadata**
```go
type StreamResult struct {
    Chunks   <-chan []byte        // Read-only channel of output chunks
    Metadata <-chan StreamingMetadata  // Metadata arrives after all chunks
    Err      error                // FFI-level error (nil on success)
}

type StreamingMetadata struct {
    Success  bool
    Error    string
    MimeType string
    Charset  string
    Binary   bool
}

// Output streaming
func RunStreaming(script string, inputs map[string]interface{}) *StreamResult

// Bidirectional streaming
func RunTransform(script string, inputReader io.Reader, opts TransformOptions) *StreamResult
```

**Rationale:**
- Go channels are the idiomatic way to represent streams of data
- Separating chunks and metadata channels allows metadata to arrive after streaming completes
- `io.Reader` is the Go standard for streaming input
- Follows Python's `Stream` wrapper pattern but uses channels instead of generators

**Error Handling:**
- FFI errors (library load, callback setup) returned in `StreamResult.Err`
- Script errors (compilation, runtime) delivered via `Metadata.Success` and `Metadata.Error`
- Callback errors cause channel closure and error in metadata

### Rust Streaming API Design

**Option A (Recommended): Iterator-based streaming with metadata**
```rust
pub struct StreamResult {
    chunks: Box<dyn Iterator<Item = Result<Vec<u8>>>>,
    metadata: Arc<Mutex<Option<StreamingMetadata>>>,
}

pub struct StreamingMetadata {
    pub success: bool,
    pub error: Option<String>,
    pub mime_type: Option<String>,
    pub charset: Option<String>,
    pub binary: bool,
}

impl Iterator for StreamResult {
    type Item = Result<Vec<u8>>;
    fn next(&mut self) -> Option<Self::Item>;
}

impl StreamResult {
    pub fn metadata(&self) -> Option<StreamingMetadata>;
}

// Output streaming
pub fn run_streaming(script: &str, inputs: Option<HashMap<String, Value>>) -> Result<StreamResult>

// Bidirectional streaming  
pub fn run_transform<R: Read>(script: &str, input_reader: R, opts: TransformOptions) -> Result<StreamResult>
```

**Rationale:**
- Rust iterators are the idiomatic way to represent lazy sequences
- `Iterator<Item = Result<Vec<u8>>>` allows per-chunk error handling
- `std::io::Read` is the Rust standard for streaming input
- `Arc<Mutex<>>` for metadata allows safe access after iteration completes
- Follows Python's `Stream` pattern but uses iterators instead of generators

**Error Handling:**
- FFI errors returned as `Result<StreamResult>` outer error
- Chunk read errors yielded as `Err` items in the iterator
- Script errors captured in metadata after iteration completes

### FFI Callback Strategy

Both languages must:
1. **Declare C callback signatures** matching `NativeCallbacks.WriteCallback` and `ReadCallback`
2. **Bridge to native types**: Convert Go/Rust closures to C function pointers
3. **Manage callback lifetime**: Ensure callbacks outlive the FFI call
4. **Handle threading**: Native library may invoke callbacks on different threads
5. **Marshal data safely**: Copy bytes from C buffers to Go/Rust memory

**Go CGO Strategy:**
```go
//export writeCallbackGo
func writeCallbackGo(ctx unsafe.Pointer, buf *C.char, length C.int) C.int

// Use CGO function pointer for the callback
```

**Rust FFI Strategy:**
```rust
extern "C" fn write_callback_rust(ctx: *mut c_void, buf: *const c_char, length: c_int) -> c_int
```

---

## Implementation Breakdown

### Phase 1: Go Output Streaming (`RunStreaming`)

**Files to modify/create:**
- `/native-lib/go/dataweave.go` - Add streaming functions
- `/native-lib/go/dataweave_test.go` - Add streaming tests
- `/native-lib/go/examples/streaming_demo.go` - Update demo with real implementation

**Tasks (TDD order):**

1. **Write failing test for `RunStreaming` basic case**
   ```go
   func TestRunStreaming_SimpleOutput(t *testing.T) {
       result := RunStreaming("output application/json --- (1 to 5)", nil)
       if result.Err != nil {
           t.Fatalf("RunStreaming failed: %v", result.Err)
       }
       var chunks [][]byte
       for chunk := range result.Chunks {
           chunks = append(chunks, chunk)
       }
       metadata := <-result.Metadata
       if !metadata.Success {
           t.Fatalf("Script failed: %s", metadata.Error)
       }
       output := string(bytes.Join(chunks, nil))
       // Verify output is valid JSON array
   }
   ```

2. **Implement `StreamResult` and `StreamingMetadata` types**
   - Define structs in `dataweave.go`
   - Add constructor that creates channels

3. **Declare `run_script_callback` FFI binding in CGO**
   ```go
   /*
   #include <stdlib.h>
   typedef int (*WriteCallback)(void* ctx, const char* buffer, int length);
   extern char* run_script_callback(void* thread, const char* script, 
                                     const char* inputsJson, WriteCallback cb, void* ctx);
   */
   import "C"
   ```

4. **Implement CGO export callback function**
   ```go
   //export writeCallbackGo
   func writeCallbackGo(ctx unsafe.Pointer, buf *C.char, length C.int) C.int {
       // Extract context (channel to send chunks)
       // Copy bytes from C buffer to Go slice
       // Send slice to channel
       // Return 0 on success
   }
   ```

5. **Implement `RunStreaming` function**
   - Create result struct with channels
   - Encode inputs to JSON
   - Launch goroutine to invoke `run_script_callback`
   - Pass `writeCallbackGo` as callback
   - Parse metadata JSON response
   - Close channels after streaming completes
   - Handle errors at each stage

6. **Add test for streaming with inputs**
   ```go
   func TestRunStreaming_WithInputs(t *testing.T) {
       inputs := map[string]interface{}{
           "payload": []int{1, 2, 3},
       }
       result := RunStreaming("output application/csv --- payload", inputs)
       // Verify CSV output
   }
   ```

7. **Add test for script errors during streaming**
   ```go
   func TestRunStreaming_ScriptError(t *testing.T) {
       result := RunStreaming("invalid syntax", nil)
       // Should still get metadata with Success=false
   }
   ```

8. **Update `examples/streaming_demo.go` with working examples**
   - Simple streaming example
   - Streaming large dataset
   - Error handling example

9. **Update Go README.md with streaming documentation**
   - Add API reference for `RunStreaming`
   - Add usage examples
   - Document threading considerations

### Phase 2: Rust Output Streaming (`run_streaming`)

**Files to modify/create:**
- `/native-lib/rust/src/lib.rs` - Add streaming functions and types
- `/native-lib/rust/tests/integration_test.rs` - Add streaming tests
- `/native-lib/rust/examples/streaming_demo.rs` - Update demo with real implementation

**Tasks (TDD order):**

1. **Write failing test for `run_streaming` basic case**
   ```rust
   #[test]
   fn test_run_streaming_simple_output() {
       let result = run_streaming("output application/json --- (1 to 5)", None)
           .expect("run_streaming failed");
       let mut chunks = Vec::new();
       for chunk_result in result {
           let chunk = chunk_result.expect("chunk read failed");
           chunks.push(chunk);
       }
       let metadata = result.metadata().expect("no metadata");
       assert!(metadata.success, "Script failed: {}", metadata.error.unwrap_or_default());
       // Verify JSON output
   }
   ```

2. **Implement `StreamingMetadata` type**
   ```rust
   #[derive(Debug, Clone)]
   pub struct StreamingMetadata {
       pub success: bool,
       pub error: Option<String>,
       pub mime_type: Option<String>,
       pub charset: Option<String>,
       pub binary: bool,
   }
   ```

3. **Implement `StreamResult` type as an iterator**
   ```rust
   pub struct StreamResult {
       receiver: std::sync::mpsc::Receiver<Vec<u8>>,
       metadata: Arc<Mutex<Option<StreamingMetadata>>>,
       done: bool,
   }
   
   impl Iterator for StreamResult {
       type Item = Result<Vec<u8>>;
       fn next(&mut self) -> Option<Self::Item> {
           // Receive from channel, return None when done
       }
   }
   ```

4. **Declare `run_script_callback` FFI binding**
   ```rust
   extern "C" {
       fn run_script_callback(
           thread: *mut libc::c_void,
           script: *const c_char,
           inputs_json: *const c_char,
           callback: extern "C" fn(*mut c_void, *const c_char, c_int) -> c_int,
           ctx: *mut c_void,
       ) -> *mut c_char;
   }
   ```

5. **Implement callback function**
   ```rust
   extern "C" fn write_callback_rust(
       ctx: *mut c_void,
       buf: *const c_char,
       length: c_int,
   ) -> c_int {
       unsafe {
           // Extract sender from context
           // Copy bytes from C buffer
           // Send to channel
           // Return 0 on success, -1 on error
       }
   }
   ```

6. **Implement `run_streaming` function**
   - Create channel for chunks
   - Create shared metadata Arc
   - Spawn thread to invoke FFI
   - Parse metadata from JSON response
   - Return `StreamResult` wrapping receiver

7. **Add test for streaming with inputs**
   ```rust
   #[test]
   fn test_run_streaming_with_inputs() {
       let mut inputs = HashMap::new();
       inputs.insert("payload".to_string(), json!([1, 2, 3]));
       let result = run_streaming("output application/csv --- payload", Some(inputs))
           .expect("run_streaming failed");
       // Verify CSV output
   }
   ```

8. **Add test for script errors**
   ```rust
   #[test]
   fn test_run_streaming_script_error() {
       let result = run_streaming("invalid syntax", None)
           .expect("run_streaming should return result even for script errors");
       // Metadata should show Success=false
   }
   ```

9. **Update `examples/streaming_demo.rs` with working examples**

10. **Update Rust README.md with streaming documentation**

### Phase 3: Go Bidirectional Streaming (`RunTransform`)

**Files to modify/create:**
- `/native-lib/go/dataweave.go` - Add transform functions
- `/native-lib/go/dataweave_test.go` - Add transform tests

**Tasks (TDD order):**

1. **Define `TransformOptions` struct**
   ```go
   type TransformOptions struct {
       InputName     string // default "payload"
       InputMimeType string // required
       InputCharset  string // default "utf-8"
   }
   ```

2. **Write failing test for `RunTransform`**
   ```go
   func TestRunTransform_SimpleCase(t *testing.T) {
       input := strings.NewReader(`[1,2,3,4,5]`)
       opts := TransformOptions{
           InputMimeType: "application/json",
       }
       result := RunTransform("output application/json --- payload map ($ * $)", input, opts)
       // Verify output is [1,4,9,16,25]
   }
   ```

3. **Declare `run_script_input_output_callback` FFI binding**
   ```go
   /*
   typedef int (*ReadCallback)(void* ctx, char* buffer, int bufferSize);
   extern char* run_script_input_output_callback(
       void* thread, const char* script, const char* inputsJson,
       const char* inputName, const char* inputMimeType, const char* inputCharset,
       ReadCallback readCb, WriteCallback writeCb, void* ctx);
   */
   ```

4. **Implement CGO read callback**
   ```go
   //export readCallbackGo
   func readCallbackGo(ctx unsafe.Pointer, buf *C.char, bufSize C.int) C.int {
       // Extract io.Reader from context
       // Read up to bufSize bytes
       // Copy to C buffer
       // Return bytes read, 0 on EOF, -1 on error
   }
   ```

5. **Implement `RunTransform` function**
   - Create channels for output chunks and metadata
   - Set up context struct with input reader and output channel
   - Launch goroutine to call FFI
   - Pass both read and write callbacks
   - Parse metadata response

6. **Add test for large file streaming**
   ```go
   func TestRunTransform_LargeFile(t *testing.T) {
       // Create temporary large JSON file
       // Stream through DataWeave transformation
       // Verify memory usage stays constant
   }
   ```

7. **Add test for input read errors**
   ```go
   func TestRunTransform_InputError(t *testing.T) {
       reader := &errorReader{} // io.Reader that returns error
       // Should propagate error correctly
   }
   ```

8. **Update documentation and examples**

### Phase 4: Rust Bidirectional Streaming (`run_transform`)

**Files to modify/create:**
- `/native-lib/rust/src/lib.rs` - Add transform functions
- `/native-lib/rust/tests/integration_test.rs` - Add transform tests

**Tasks (TDD order):**

1. **Define `TransformOptions` struct**
   ```rust
   pub struct TransformOptions {
       pub input_name: String,      // default "payload"
       pub input_mime_type: String, // required
       pub input_charset: Option<String>,
   }
   ```

2. **Write failing test for `run_transform`**
   ```rust
   #[test]
   fn test_run_transform_simple_case() {
       let input = b"[1,2,3,4,5]";
       let opts = TransformOptions {
           input_name: "payload".to_string(),
           input_mime_type: "application/json".to_string(),
           input_charset: None,
       };
       let result = run_transform(
           "output application/json --- payload map ($ * $)",
           &input[..],
           opts,
       ).expect("run_transform failed");
       // Verify output
   }
   ```

3. **Declare `run_script_input_output_callback` FFI binding**
   ```rust
   extern "C" {
       fn run_script_input_output_callback(
           thread: *mut c_void,
           script: *const c_char,
           inputs_json: *const c_char,
           input_name: *const c_char,
           input_mime_type: *const c_char,
           input_charset: *const c_char,
           read_callback: extern "C" fn(*mut c_void, *mut c_char, c_int) -> c_int,
           write_callback: extern "C" fn(*mut c_void, *const c_char, c_int) -> c_int,
           ctx: *mut c_void,
       ) -> *mut c_char;
   }
   ```

4. **Implement read callback**
   ```rust
   extern "C" fn read_callback_rust(
       ctx: *mut c_void,
       buf: *mut c_char,
       buf_size: c_int,
   ) -> c_int {
       unsafe {
           // Extract Read trait object from context
           // Read bytes into temporary buffer
           // Copy to C buffer
           // Return bytes read, 0 on EOF, -1 on error
       }
   }
   ```

5. **Implement `run_transform` function**
   - Accept `R: Read` generic parameter
   - Create context struct with reader and sender
   - Spawn thread for FFI call
   - Return `StreamResult`

6. **Add test for streaming from file**
   ```rust
   #[test]
   fn test_run_transform_from_file() {
       use std::fs::File;
       let file = File::open("test_data.json").expect("open file");
       // Stream through transformation
   }
   ```

7. **Add test for input read errors**

8. **Update documentation and examples**

### Phase 5: Documentation and Integration

**Tasks:**

1. **Update `/native-lib/README.md`**
   - Add streaming examples for both Go and Rust
   - Document when to use streaming vs buffered execution
   - Add performance considerations
   - Add troubleshooting section

2. **Create comprehensive examples**
   - Go: Process large CSV files with streaming
   - Rust: JSON to CSV transformation with constant memory
   - Both: Real-world use cases (log processing, data migration)

3. **Add Gradle integration testing**
   - Update `native-lib/build.gradle` if needed
   - Ensure `goTest` runs streaming tests
   - Ensure `rustTest` runs streaming tests

4. **Add memory benchmarks**
   - Go: Benchmark memory usage for 100MB JSON file
   - Rust: Benchmark memory usage for 100MB JSON file
   - Document that streaming uses constant memory

5. **Add error handling guide**
   - Document all error scenarios
   - Provide troubleshooting steps
   - Add examples of proper error handling

---

## Testing Strategy

### Unit Tests

**Go:**
- `TestRunStreaming_SimpleOutput` - Basic streaming works
- `TestRunStreaming_WithInputs` - Streaming with input bindings
- `TestRunStreaming_ScriptError` - Error handling
- `TestRunStreaming_LargeDataset` - Many chunks
- `TestRunTransform_SimpleCase` - Bidirectional streaming
- `TestRunTransform_LargeFile` - Constant memory usage
- `TestRunTransform_InputError` - Input read errors

**Rust:**
- `test_run_streaming_simple_output` - Basic streaming
- `test_run_streaming_with_inputs` - Input bindings
- `test_run_streaming_script_error` - Error handling
- `test_run_streaming_large_dataset` - Many chunks
- `test_run_transform_simple_case` - Bidirectional streaming
- `test_run_transform_from_file` - File streaming
- `test_run_transform_input_error` - Read errors

### Integration Tests

- **End-to-end streaming**: 10MB JSON → CSV transformation
- **Concurrent streaming**: Multiple streams running simultaneously
- **Error propagation**: Script errors, input errors, callback errors
- **Memory profiling**: Verify constant memory usage for large files

### Example Programs

- **`streaming_demo.go`**: Output streaming demo
- **`streaming_demo.rs`**: Output streaming demo
- Both should include:
  - Simple streaming example
  - Large dataset example (1M records)
  - Error handling example
  - Performance comparison vs buffered

---

## Dependencies and Ordering

**Sequential Dependencies:**
1. Phase 1 (Go output streaming) and Phase 2 (Rust output streaming) can be done in parallel
2. Phase 3 (Go bidirectional) requires Phase 1 complete
3. Phase 4 (Rust bidirectional) requires Phase 2 complete
4. Phase 5 (documentation) requires Phases 1-4 complete

**Within Each Phase:**
- Tests must be written before implementation (TDD)
- FFI bindings must be declared before callback implementation
- Callback functions must be implemented before main streaming functions
- Basic tests must pass before adding complex tests

**Build Prerequisites:**
- Native library must be compiled: `./gradlew :native-lib:nativeCompile`
- For Go tests: Go 1.21+ with CGO enabled
- For Rust tests: Rust 1.70+ with cargo

---

## Critical Implementation Notes

1. **Memory Management**
   - Go: Use `C.CString` with `defer C.free` for strings
   - Rust: Use `CString::new` and manage lifetime carefully
   - Both: Always free metadata JSON returned by callbacks using `free_cstring`

2. **Threading**
   - Go: Callbacks may be invoked on different goroutines - channels handle this naturally
   - Rust: Use `Arc` for shared state between FFI thread and consumer thread
   - Both: Test concurrent streaming to ensure thread safety

3. **Buffer Sizes**
   - Native library uses 8KB chunks (`CALLBACK_BUFFER_SIZE`)
   - Go/Rust buffers should match or be multiples of 8KB for efficiency
   - Document optimal buffer sizes in API docs

4. **Error Handling Layers**
   - FFI errors (library load, null pointers) → Return early with error
   - Script errors (compilation, runtime) → Success=false in metadata
   - Callback errors (read/write failures) → Abort and return error in metadata
   - Test each layer independently

5. **CGO Considerations (Go)**
   - CGO export functions cannot return Go errors
   - Must use C integer return codes (0 = success, -1 = error)
   - Cannot pass Go pointers to C (use handles or indices)
   - Document these limitations

6. **FFI Safety (Rust)**
   - All FFI calls must be in `unsafe` blocks
   - Document safety invariants for each `unsafe` usage
   - Use `CStr` and `CString` rather than raw pointers where possible
   - Consider using `safer_ffi` crate for additional safety (optional)

---

## Potential Challenges and Mitigations

**Challenge 1: Callback Lifetime Management**
- **Problem**: Callbacks must outlive the FFI call
- **Mitigation**: Use channels/mpsc to decouple callback execution from consumer
- **Test**: Create test that consumes stream slowly while native side produces fast

**Challenge 2: Threading and CGO**
- **Problem**: Go's CGO has restrictions on goroutines and callbacks
- **Mitigation**: Use C function pointers with `//export`, not Go closures
- **Test**: Test concurrent streaming with multiple goroutines

**Challenge 3: Error Propagation Across FFI**
- **Problem**: Rich error types don't cross FFI boundary well
- **Mitigation**: Use JSON metadata for structured errors, integer codes for callback errors
- **Test**: Test each error scenario with assertions on error messages

**Challenge 4: Memory Leaks**
- **Problem**: Forgetting to free C strings
- **Mitigation**: Document all free requirements, use `defer` (Go) and RAII (Rust)
- **Test**: Run valgrind/address sanitizer on examples

**Challenge 5: Platform-Specific Behavior**
- **Problem**: Different behavior on macOS vs Linux vs Windows
- **Mitigation**: CI testing on all three platforms
- **Test**: Run full test suite on macOS, Linux (Ubuntu), Windows

---

## Success Criteria

1. **Functionality**
   - All unit tests pass for Go streaming APIs
   - All unit tests pass for Rust streaming APIs
   - Examples run successfully and demonstrate constant memory usage
   - Gradle tasks `goTest` and `rustTest` include streaming tests

2. **Performance**
   - Streaming a 100MB JSON file uses <50MB RAM (constant memory)
   - Throughput comparable to Python implementation (within 20%)
   - No memory leaks detected by valgrind/ASAN

3. **Documentation**
   - API docs for all new functions
   - Usage examples in README files
   - Example programs that run successfully
   - Error handling guide

4. **Code Quality**
   - Follows existing code style conventions
   - Comprehensive error handling
   - Thread-safe implementation
   - No compiler warnings

---

## Critical Files for Implementation

- `/native-lib/go/dataweave.go` - Go streaming API implementation
- `/native-lib/go/dataweave_test.go` - Go streaming tests
- `/native-lib/rust/src/lib.rs` - Rust streaming API implementation
- `/native-lib/rust/tests/integration_test.rs` - Rust streaming tests
- `/native-lib/README.md` - Main documentation with streaming examples
