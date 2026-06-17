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
go run examples/streaming_demo.go
```

## API Reference

### `Run(script string, inputs map[string]interface{}) (*ExecutionResult, error)`

Executes a DataWeave script with the given inputs (buffered mode).

**Parameters:**
- `script`: DataWeave script source code
- `inputs`: Map of binding names to values (auto-encoded as JSON)

**Returns:**
- `*ExecutionResult`: Execution result with output and metadata
- `error`: FFI-level error (library not found, marshaling failure, etc.)

### `RunStreaming(script string, inputs map[string]interface{}) *StreamResult`

Executes a DataWeave script and streams the output via channels. Output chunks are delivered as they are produced by the native engine without buffering the entire result in memory.

**Parameters:**
- `script`: DataWeave script source code
- `inputs`: Map of binding names to values (auto-encoded as JSON), or nil

**Returns:**
- `*StreamResult`: Contains `Chunks` channel (output data), `Metadata` channel (after streaming completes), and `Err` (FFI-level error)

**Usage:**
```go
result := dataweave.RunStreaming("output application/json --- (1 to 10000)", nil)
if result.Err != nil {
    log.Fatal(result.Err)
}
for chunk := range result.Chunks {
    os.Stdout.Write(chunk)
}
metadata := <-result.Metadata
fmt.Printf("Done: %s, %s\n", metadata.MimeType, metadata.Charset)
```

### `RunTransform(script string, inputReader io.Reader, opts TransformOptions) *StreamResult`

Executes a DataWeave script with streaming input and output. Input data is pulled from the reader and output chunks are delivered via channels. Ideal for processing large files with constant memory overhead.

**Parameters:**
- `script`: DataWeave script source code
- `inputReader`: An `io.Reader` providing streaming input data
- `opts`: `TransformOptions` with input name, MIME type, and charset

**Returns:**
- `*StreamResult`: Same as `RunStreaming`

**Usage:**
```go
file, _ := os.Open("large.json")
defer file.Close()
opts := dataweave.TransformOptions{
    InputMimeType: "application/json",
}
result := dataweave.RunTransform("output application/csv --- payload", file, opts)
if result.Err != nil {
    log.Fatal(result.Err)
}
for chunk := range result.Chunks {
    outFile.Write(chunk)
}
metadata := <-result.Metadata
```

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

### `StreamResult`

```go
type StreamResult struct {
    Chunks   <-chan []byte             // Read-only channel of output chunks
    Metadata <-chan StreamingMetadata   // Metadata arrives after all chunks
    Err      error                     // FFI-level error (nil on success)
}
```

### `StreamingMetadata`

```go
type StreamingMetadata struct {
    Success  bool
    Error    string
    MimeType string
    Charset  string
    Binary   bool
}
```

### `TransformOptions`

```go
type TransformOptions struct {
    InputName     string // Binding name (default "payload")
    InputMimeType string // MIME type (required)
    InputCharset  string // Charset (default "utf-8")
}
```

## Threading Considerations

- `RunStreaming` and `RunTransform` launch a goroutine internally to call the native FFI
- Output chunks are delivered via a buffered channel (capacity 64)
- Callbacks may be invoked on different OS threads by the native library
- It is safe to call `RunStreaming`/`RunTransform` from multiple goroutines concurrently
- Each `StreamResult` is independent and can be consumed by a single goroutine

## When to Use Streaming vs Buffered

| Use Case | Recommended API |
|----------|----------------|
| Small scripts, immediate result | `Run()` |
| Large output, process as produced | `RunStreaming()` |
| Large input and output, constant memory | `RunTransform()` |
| File-to-file transformation | `RunTransform()` |

## Environment Variables

Set `CGO_LDFLAGS` to point to the native library if not in the default location:

```bash
export CGO_LDFLAGS="-L/path/to/native-lib/build/native/nativeCompile -ldwlib"
```
