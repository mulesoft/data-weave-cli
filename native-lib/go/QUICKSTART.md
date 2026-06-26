# Go Bindings - Quick Start Guide

## 1. Build the Native Library

```bash
# From repository root
./gradlew :native-lib:nativeCompile
```

This creates `native-lib/build/native/nativeCompile/dwlib.dylib` (macOS) or `.so` (Linux) or `.dll` (Windows).

## 2. Install Go Dependencies

```bash
cd native-lib/go
go mod download
```

## 3. Run Tests

```bash
go test -v
```

## 4. Try the Example

```bash
cd example
go build
./dataweave-example
```

## 5. Use in Your Project

**Add to your `go.mod`:**

```go
require github.com/mulesoft-labs/data-weave-native/go v0.1.0
```

**Simple usage:**

```go
package main

import (
    "fmt"
    "log"
    
    dw "github.com/mulesoft-labs/data-weave-native/go"
)

func main() {
    // Basic execution
    result, err := dw.Run("2 + 2", nil)
    if err != nil {
        log.Fatal(err)
    }
    
    output, _ := result.GetString()
    fmt.Println(output) // "4"
}
```

**With inputs:**

```go
inputs := map[string]interface{}{
    "user": map[string]interface{}{
        "name": "Alice",
        "age": 30,
    },
}

script := `output application/json
---
{
  greeting: "Hello, " ++ user.name,
  isAdult: user.age >= 18
}`

result, err := dw.Run(script, inputs)
if err != nil {
    log.Fatal(err)
}

fmt.Println(result.GetString())
// {"greeting": "Hello, Alice", "isAdult": true}
```

**Streaming large data:**

```go
file, _ := os.Open("large-input.json")
defer file.Close()

// Create input stream
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
    }
}()

// Transform streaming
stream, _ := dw.RunTransform(
    "output application/csv --- payload.records",
    inputStream,
    "payload",
    "application/json",
    "",
    nil,
)

// Process output
for chunk := range stream.Chunks {
    os.Stdout.Write(chunk)
}

metadata := stream.Wait()
fmt.Println("Success:", metadata.Success)
```

## Common Patterns

### Error Handling

```go
result, err := dw.Run(script, inputs)
if err != nil {
    log.Fatal(err)
}

if !result.Success {
    log.Printf("Script error: %s", result.Error)
}
```

### Context Manager (Explicit Cleanup)

```go
dw, err := dataweave.New()
if err != nil {
    log.Fatal(err)
}
defer dw.Cleanup()

// Use dw for multiple executions
result1, _ := dw.Run("sqrt(144)", nil)
result2, _ := dw.Run("sqrt(10000)", nil)
```

### Custom Input Types

```go
xmlBytes, _ := os.ReadFile("input.xml")

inputs := map[string]interface{}{
    "payload": &dw.InputValue{
        Content:  xmlBytes,
        MimeType: "application/xml",
        Charset:  "UTF-16",
    },
}

result, _ := dw.Run("output csv --- payload.records", inputs)
```

## Troubleshooting

### Library Not Found

If you get `ld: library 'dwlib' not found`:

1. Build the native library first: `./gradlew :native-lib:nativeCompile`
2. Or set environment variable:
   ```bash
   export DATAWEAVE_NATIVE_LIB=/path/to/dwlib.dylib
   ```

### CGO Not Enabled

If you get CGO errors:

```bash
export CGO_ENABLED=1
go build
```

### Architecture Mismatch

Ensure the native library matches your CPU architecture:
- macOS Apple Silicon: arm64
- macOS Intel: amd64
- Linux: usually amd64

## Next Steps

- Read the full [README.md](README.md) for complete API reference
- Check [IMPLEMENTATION_NOTES.md](IMPLEMENTATION_NOTES.md) for internals
- Review [dataweave_test.go](dataweave_test.go) for comprehensive examples
- See [example/main.go](example/main.go) for working code samples

## Performance Tips

1. **Reuse instances** - Create one DataWeave instance and reuse it
2. **Use streaming** - For large data (>1MB), use RunStreaming or RunTransform
3. **Chunk size** - Use 8KB-64KB chunks when streaming from files
4. **Concurrent execution** - Each instance is thread-safe; use separate instances for parallel work

## More Examples

See the test suite in `dataweave_test.go` for examples of:
- Auto-conversion of Go types
- Callback-based streaming
- Large data processing
- File-based transformations
- Error handling
- UTF-16 XML → CSV conversion
