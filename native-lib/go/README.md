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
