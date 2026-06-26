# DataWeave Native Library - Go Bindings

Go bindings for the DataWeave native library, allowing you to execute DataWeave scripts from Go applications with full support for streaming input and output.

## Features

- **Basic Execution**: Synchronous script execution with buffered results
- **Output Streaming**: Stream output chunks as they're produced via Go channels
- **Bidirectional Streaming**: Stream both input and output with constant memory overhead
- **Type Safety**: Strongly-typed result structures
- **Resource Management**: Automatic cleanup via `defer` or explicit control
- **Error Handling**: Idiomatic Go error handling
- **Thread Safety**: Safe concurrent usage with multiple goroutines

## Prerequisites

1. Build the native library:
   ```bash
   cd ../..
   ./gradlew nativeCompile
   ```

2. The shared library will be at: `build/native/nativeCompile/dwlib.{dylib|so|dll}`

## Installation

```bash
go get github.com/mulesoft-labs/data-weave-native/go
```

Or add to your `go.mod`:
```
require github.com/mulesoft-labs/data-weave-native/go v0.1.0
```

## Usage

### Basic Execution

Execute a simple script:

```go
package main

import (
    "fmt"
    "log"
    
    "github.com/mulesoft-labs/data-weave-native/go"
)

func main() {
    result, err := dataweave.Run("2 + 2", nil)
    if err != nil {
        log.Fatal(err)
    }
    
    output, _ := result.GetString()
    fmt.Println(output) // "4"
}
```

### Execution with Inputs

Pass input bindings to your script:

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

### Auto-Conversion of Native Types

The library automatically converts Go types to DataWeave inputs:

```go
inputs := map[string]interface{}{
    "numbers": []int{1, 2, 3},
    "config": map[string]interface{}{
        "enabled": true,
        "count": 42,
    },
    "name": "Alice",
}

result, err := dataweave.Run("numbers[0]", inputs)
// result: "1"
```

### Explicit Input Metadata

For advanced use cases, explicitly specify MIME type and charset:

```go
xmlBytes, _ := os.ReadFile("input.xml")

inputs := map[string]interface{}{
    "payload": &dataweave.InputValue{
        Content:  xmlBytes,
        MimeType: "application/xml",
        Charset:  "UTF-16",
    },
}

script := `output application/csv header=true
---
[payload.person]
`

result, err := dataweave.Run(script, inputs)
```

### Context Manager (Explicit Lifecycle)

When you need explicit control over the lifecycle:

```go
dw, err := dataweave.New()
if err != nil {
    log.Fatal(err)
}
defer dw.Cleanup()

result1, _ := dw.Run("sqrt(144)", nil)
result2, _ := dw.Run("sqrt(10000)", nil)
```

### Output Streaming

Stream output chunks as they're produced:

```go
stream, err := dataweave.RunStreaming(
    `output application/json --- (1 to 10000) map {id: $}`,
    nil,
)
if err != nil {
    log.Fatal(err)
}

// Process chunks as they arrive
for chunk := range stream.Chunks {
    os.Stdout.Write(chunk)
}

// Get metadata after streaming completes
metadata := stream.Wait()
fmt.Println("MIME Type:", metadata.MimeType)
```

### Bidirectional Streaming

Stream both input and output for constant memory usage:

```go
// Open a large input file
file, _ := os.Open("large-data.json")
defer file.Close()

// Create input stream channel
inputStream := make(chan []byte, 10)
go func() {
    defer close(inputStream)
    buf := make([]byte, 8192)
    for {
        n, err := file.Read(buf)
        if n > 0 {
            chunk := make([]byte, n)
            copy(chunk, buf[:n])
            inputStream <- chunk
        }
        if err == io.EOF {
            break
        }
        if err != nil {
            log.Fatal(err)
        }
    }
}()

// Transform with streaming I/O
script := `output application/csv --- payload.items`
stream, err := dataweave.RunTransform(
    script,
    inputStream,
    "payload",           // input binding name
    "application/json",  // input MIME type
    "",                  // input charset (empty = UTF-8)
    nil,                 // additional inputs
)
if err != nil {
    log.Fatal(err)
}

// Stream output to stdout
for chunk := range stream.Chunks {
    os.Stdout.Write(chunk)
}

metadata := stream.Wait()
fmt.Fprintln(os.Stderr, "Transformation complete:", metadata.Success)
```

### Callback-Based Streaming

For lower-level control, use callbacks:

```go
var chunks [][]byte

writeCallback := func(data []byte) int {
    chunks = append(chunks, data)
    return 0 // return 0 for success, non-zero to abort
}

result, err := dataweave.RunCallback("2 + 2", writeCallback, nil)
if err != nil {
    log.Fatal(err)
}

fullOutput := bytes.Join(chunks, nil)
fmt.Println(string(fullOutput))
```

## API Reference

### Functions

#### `Run(script string, inputs map[string]interface{}) (*ExecutionResult, error)`

Execute a DataWeave script with buffered output using the global instance.

- **Parameters:**
  - `script`: DataWeave script source code
  - `inputs`: Input bindings (map of name → value)
- **Returns:** `ExecutionResult` with output and metadata, or error

#### `RunStreaming(script string, inputs map[string]interface{}) (*Stream, error)`

Execute a script and stream output chunks via a channel.

- **Parameters:**
  - `script`: DataWeave script source code
  - `inputs`: Input bindings
- **Returns:** `Stream` with `Chunks` channel and `Metadata`, or error

#### `RunTransform(script, inputStream, inputName, inputMimeType, inputCharset, inputs) (*Stream, error)`

Execute a script with streaming input and output.

- **Parameters:**
  - `script`: DataWeave script source code
  - `inputStream`: Channel yielding input chunks (`<-chan []byte`)
  - `inputName`: Binding name for streamed input (e.g., `"payload"`)
  - `inputMimeType`: MIME type of input (e.g., `"application/json"`)
  - `inputCharset`: Charset of input (empty string = UTF-8)
  - `inputs`: Additional input bindings
- **Returns:** `Stream` with output chunks, or error

#### `RunCallback(script string, writeCallback func([]byte) int, inputs map[string]interface{}) (*StreamingResult, error)`

Execute a script and stream output via callback.

- **Parameters:**
  - `script`: DataWeave script source code
  - `writeCallback`: Function called for each output chunk; return 0 for success, non-zero to abort
  - `inputs`: Input bindings
- **Returns:** `StreamingResult` with metadata, or error

#### `Cleanup()`

Release the global DataWeave instance (typically called at program exit).

### Types

#### `ExecutionResult`

```go
type ExecutionResult struct {
    Success  bool
    Result   string  // base64-encoded output
    Error    string  // error message if success=false
    Binary   bool
    MimeType string
    Charset  string
}

// Methods
func (r *ExecutionResult) GetBytes() ([]byte, error)
func (r *ExecutionResult) GetString() (string, error)
```

#### `StreamingResult`

```go
type StreamingResult struct {
    Success  bool
    Error    string
    MimeType string
    Charset  string
    Binary   bool
}
```

#### `Stream`

```go
type Stream struct {
    Chunks   <-chan []byte        // Channel yielding output chunks
    Metadata *StreamingResult     // Available after stream completes
}

// Methods
func (s *Stream) Wait() *StreamingResult  // Blocks until stream completes
```

#### `InputValue`

```go
type InputValue struct {
    Content    interface{}       // string or []byte
    MimeType   string            // required
    Charset    string            // optional, defaults to UTF-8
    Properties map[string]string // optional metadata
}
```

#### `DataWeave`

```go
type DataWeave struct {
    // ... (internal fields)
}

// Methods
func New() (*DataWeave, error)
func (dw *DataWeave) Initialize() error
func (dw *DataWeave) Cleanup()
func (dw *DataWeave) Run(script string, inputs map[string]interface{}) (*ExecutionResult, error)
func (dw *DataWeave) RunStreaming(script string, inputs map[string]interface{}) (*Stream, error)
func (dw *DataWeave) RunTransform(...) (*Stream, error)
func (dw *DataWeave) RunCallback(...) (*StreamingResult, error)
```

## Error Handling

All functions return standard Go errors:

```go
result, err := dataweave.Run("invalid syntax", nil)
if err != nil {
    log.Fatal(err)
}

if !result.Success {
    log.Printf("Script failed: %s", result.Error)
}
```

### Common Errors

- `ErrNotInitialized`: Runtime not initialized
- `ErrLibraryNotFound`: Native library not found
- `ErrStreamingNotSupported`: Streaming APIs not available

## Environment Variables

- `DATAWEAVE_NATIVE_LIB`: Override the library search path (absolute path to `dwlib.{dylib|so|dll}`)

## Testing

Run the test suite:

```bash
# From the go/ directory
go test -v

# With coverage
go test -v -cover

# Run specific test
go test -v -run TestBasic
```

## Build Instructions

### Building the Native Library

From the repository root:

```bash
./gradlew nativeCompile
```

The library will be at `build/native/nativeCompile/dwlib.{dylib|so|dll}`.

### Building the Go Module

```bash
cd native-lib/go
go build
```

### CGO Configuration

The `dataweave.go` file includes CGO directives that automatically link against the native library in the build directory. For production deployments, you may need to adjust the `LDFLAGS` in the `#cgo` directives or set `DATAWEAVE_NATIVE_LIB` to point to your installed library location.

## Thread Safety

- The global instance (`Run`, `RunStreaming`, etc.) is thread-safe
- Individual `DataWeave` instances are thread-safe
- Callbacks are invoked on native threads; ensure your callback functions are thread-safe

## Memory Management

- Go manages memory for Go objects automatically
- C strings are freed using `C.free()` and `free_cstring()`
- The finalizer automatically cleans up instances, but explicit `defer dw.Cleanup()` is recommended
- Streaming APIs use constant memory regardless of data size

## Examples

See `dataweave_test.go` for comprehensive examples covering:
- Basic execution
- Script with inputs
- Context manager pattern
- Encoding conversion (UTF-16 XML → CSV)
- Auto-conversion of Go types
- Callback output streaming
- Bidirectional streaming
- Large data streaming
- Error handling
- File-based streaming

## Performance Tips

1. **Reuse instances**: Create one `DataWeave` instance and reuse it for multiple executions
2. **Use streaming**: For large inputs/outputs, use `RunStreaming` or `RunTransform` to avoid buffering everything in memory
3. **Buffer size**: When streaming from files, use 8KB-64KB chunks for optimal performance
4. **Cleanup**: Always call `Cleanup()` or use `defer` to release native resources promptly

## Troubleshooting

### Library Not Found

If you get `ErrLibraryNotFound`:

1. Ensure the native library is built: `./gradlew nativeCompile`
2. Check the library exists: `ls ../../build/native/nativeCompile/dwlib.*`
3. Set `DATAWEAVE_NATIVE_LIB` to the absolute path:
   ```bash
   export DATAWEAVE_NATIVE_LIB=/path/to/dwlib.dylib
   ```

### Linking Errors

If you get CGO linking errors:

1. Verify CGO is enabled: `go env CGO_ENABLED` (should be `1`)
2. Check compiler is installed (gcc/clang)
3. Adjust `#cgo LDFLAGS` in `dataweave.go` if using a custom library location

### Runtime Errors

If scripts fail at runtime:

1. Check `result.Success` and `result.Error` for error messages
2. Verify input format matches expected MIME types
3. Enable verbose logging to see native library output

## License

See the main repository LICENSE file.

## Contributing

Contributions welcome! Please ensure:
- All tests pass (`go test -v`)
- Code follows Go conventions (`gofmt`, `go vet`)
- New features include tests
- Documentation is updated

## See Also

- [FFI Contract](../FFI_CONTRACT.md) - Detailed native API specification
- [Python Bindings](../python/) - Python reference implementation
- [DataWeave Documentation](https://docs.mulesoft.com/dataweave/) - DataWeave language reference
