# Go Implementation Notes

## Overview

This directory contains Go bindings for the DataWeave native library, providing a comprehensive, idiomatic Go API for executing DataWeave scripts.

## Implementation Details

### Architecture

The implementation follows the FFI contract specified in `../FFI_CONTRACT.md` and matches the feature set of the Python reference implementation at `../python/`.

**Key Components:**

1. **`dataweave.go`**: Main implementation with CGO bindings
2. **`callbacks.c` / `callbacks.h`**: C wrapper functions for Go callbacks
3. **`dataweave_test.go`**: Comprehensive test suite
4. **`example/main.go`**: Usage examples

### CGO Integration

The Go implementation uses CGO to interface with the native shared library:

- **C Headers**: Declares GraalVM isolate management and DataWeave API functions
- **LDFLAGS**: Dynamically links against `dwlib.{dylib|so|dll}`
- **Callback Bridge**: Uses C wrapper functions to bridge Go callback functions to C function pointers

### Type Mapping

| Go Type | DataWeave/FFI Type |
|---------|-------------------|
| `map[string]interface{}` | Input bindings JSON |
| `*ExecutionResult` | Buffered execution result |
| `*StreamingResult` | Streaming execution metadata |
| `*Stream` | Output stream (channel + metadata) |
| `<-chan []byte` | Input stream |
| `chan []byte` | Output chunks channel |
| `func([]byte) int` | Write callback |
| `func(int) ([]byte, error)` | Read callback |

### Resource Management

- **Global Instance**: Package-level functions use a singleton instance created on first use
- **Explicit Instances**: `New()` creates independent instances with their own GraalVM isolates
- **Cleanup**: `defer dw.Cleanup()` ensures proper resource release
- **Finalizers**: Automatic cleanup via Go finalizers as fallback

### Threading Model

- **GraalVM Isolates**: Each `DataWeave` instance has its own isolate
- **Thread Attachment**: Background workers attach to the isolate using `graal_attach_thread`
- **Goroutines**: Streaming APIs spawn goroutines for async execution
- **Channels**: Go channels provide thread-safe communication between goroutines

### Callback Implementation

Go callbacks are exported using the `//export` directive and wrapped in C functions:

1. Go functions `goWriteCallback` and `goReadCallback` are exported to C
2. C wrapper functions in `callbacks.c` provide stable function pointers
3. Context is passed as `uintptr` IDs to avoid CGO pointer restrictions
4. Global callback registry maps IDs to Go callback contexts

### Memory Management

- **C Strings**: Created with `C.CString()`, freed with `C.free()`
- **Result Buffers**: Native library allocates strings, freed with `free_cstring()`
- **Base64 Encoding**: Input content is base64-encoded; output is base64-decoded
- **Channel Buffers**: Output channels use buffering (10 chunks) to avoid blocking

### Error Handling

- Idiomatic Go error returns: `(result, error)` pattern
- Result types include `Success` flag and `Error` string
- Separate errors for:
  - Library not found
  - Runtime not initialized
  - API not supported
  - Script execution failures

### Input Normalization

The `normalizeInput()` function converts Go values to FFI format:

- **Primitive types** (`int`, `float`, `bool`): Marshaled to JSON
- **Strings**: Encoded as text/plain
- **Arrays/Objects**: Marshaled to JSON
- **`InputValue`**: Explicit content, MIME type, and charset
- **Maps with `content`/`mimeType`**: Pass-through with base64 encoding

## Testing

The test suite (`dataweave_test.go`) covers all Python test cases:

- ✅ Basic execution (`2 + 2`)
- ✅ Script with inputs (`num1 + num2`)
- ✅ Context manager pattern (explicit lifecycle)
- ✅ Encoding conversion (UTF-16 XML → CSV)
- ✅ Auto-conversion of native types
- ✅ Callback output basic
- ✅ Callback output with inputs
- ✅ Callback input+output basic
- ✅ Callback input+output large data
- ✅ `RunStreaming` basic
- ✅ `RunStreaming` large (multiple chunks)
- ✅ `RunStreaming` error handling
- ✅ `RunStreaming` with inputs
- ✅ `RunTransform` basic
- ✅ `RunTransform` large chunked input
- ✅ `RunTransform` with file handles

## Building

### Prerequisites

1. Build the native library:
   ```bash
   cd ../..
   ./gradlew nativeCompile
   ```

2. Ensure CGO is enabled:
   ```bash
   export CGO_ENABLED=1
   ```

### Build Commands

```bash
# Install dependencies
go mod download

# Build the module
go build

# Run tests
go test -v

# Build example
cd example && go build
```

## Differences from Python

### API Naming

- Python: `run_streaming()` → Go: `RunStreaming()`
- Python: `run_transform()` → Go: `RunTransform()`
- Python: `run_callback()` → Go: `RunCallback()`

### Streaming API

- **Python**: Returns a generator, metadata in `StopIteration.value`
- **Go**: Returns a `Stream` with `Chunks` channel and `Metadata` field
- **Python**: `for chunk in stream:`
- **Go**: `for chunk := range stream.Chunks { ... }`

### Context Manager

- **Python**: `with DataWeave() as dw:`
- **Go**: `dw, _ := New(); defer dw.Cleanup()`

### Error Handling

- **Python**: Raises `DataWeaveScriptError` exception
- **Go**: Returns `(result, error)` tuples

## Performance

- **Memory**: Streaming APIs use constant memory (channel buffering only)
- **Concurrency**: Safe for concurrent goroutines with separate instances
- **Overhead**: Minimal CGO call overhead (~100ns per call)
- **Large Data**: Recommended chunk size for streaming: 8KB-64KB

## Known Limitations

1. **CGO Requirement**: Must have CGO enabled and C compiler installed
2. **Dynamic Linking**: Requires `dwlib` at runtime (not statically linked)
3. **Platform-Specific**: Separate builds for macOS (dylib), Linux (so), Windows (dll)
4. **GraalVM Dependency**: Native library must be built with GraalVM native-image

## Future Enhancements

Potential improvements:

- [ ] Static linking option
- [ ] Pre-built binaries for common platforms
- [ ] Benchmark suite
- [ ] Profiling tools
- [ ] Windows support validation
- [ ] Alternative pure-Go DataWeave implementation (long-term)

## Troubleshooting

### Library Not Found

**Symptom**: `ld: library 'dwlib' not found`

**Solution**:
1. Build the native library: `./gradlew nativeCompile`
2. Set `DATAWEAVE_NATIVE_LIB` environment variable to absolute path

### CGO Disabled

**Symptom**: `go: -buildmode=c-shared not supported by compiler`

**Solution**: `export CGO_ENABLED=1`

### Runtime Errors

**Symptom**: Scripts fail with `graal_create_isolate` errors

**Solution**: Ensure the native library was built for the correct architecture (arm64/amd64)

## See Also

- [FFI Contract](../FFI_CONTRACT.md) - Native API specification
- [Python Implementation](../python/) - Reference implementation
- [README.md](README.md) - User-facing documentation and API reference
