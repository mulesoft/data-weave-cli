# DataWeave C Library

High-level C wrapper for the DataWeave native library (`dwlib`). Provides a clean, well-documented API for executing DataWeave scripts from C applications with proper error handling and resource management.

## Features

- **Simple API**: Easy-to-use functions for common operations
- **Memory Safety**: Clear ownership semantics with explicit free functions
- **Error Handling**: Thread-local error messages and detailed result objects
- **Streaming Support**: Callback-based streaming for both input and output
- **Type Safety**: Opaque structs for encapsulation
- **Comprehensive Tests**: Full test coverage matching Python reference implementation

## Quick Start

### Prerequisites

1. Build the DataWeave native library:
   ```bash
   cd ../..
   ./gradlew :native-lib:nativeCompile
   ```

2. The native library will be at:
   - macOS: `../build/native/nativeCompile/dwlib.dylib`
   - Linux: `../build/native/nativeCompile/dwlib.so`
   - Windows: `../build/native/nativeCompile/dwlib.dll`

### Build the C Library

```bash
cd native-lib/c
make
```

This builds:
- Static library: `build/lib/libdataweave.a`
- Shared library: `build/lib/libdataweave.dylib` (or `.so`/`.dll`)
- Test binary: `build/test/test_dataweave`

### Run Tests

```bash
# Set path to native library
export DATAWEAVE_NATIVE_LIB=../../native-lib/build/native/nativeCompile/dwlib.dylib

# Run tests
make test
```

Or copy the native library to the tests directory:
```bash
cp ../../native-lib/build/native/nativeCompile/dwlib.dylib tests/
make test
```

## Usage Examples

### Basic Script Execution

```c
#include <dataweave.h>
#include <stdio.h>

int main(void) {
    // Initialize runtime
    dw_runtime *runtime = dw_init();
    if (!runtime) {
        fprintf(stderr, "Init failed: %s\n", dw_get_last_error());
        return 1;
    }

    // Execute script
    dw_execution_result *result = dw_run(runtime, "2 + 2", NULL);
    if (result && dw_result_success(result)) {
        printf("Result: %s\n", dw_result_get_string(result));
    } else {
        fprintf(stderr, "Error: %s\n", dw_result_error(result));
    }

    // Cleanup
    dw_free_result(result);
    dw_cleanup(runtime);
    return 0;
}
```

### Script with Inputs

```c
// Create inputs using helper function
char *inputs = dw_create_input_string("name", "World", "text/plain");

dw_execution_result *result = dw_run(runtime, 
    "\"Hello, \" ++ name", 
    inputs);

if (dw_result_success(result)) {
    printf("%s\n", dw_result_get_string(result));  // "Hello, World"
}

dw_free_string(inputs);
dw_free_result(result);
```

### Manual Input Construction

For more control, construct the JSON input manually:

```c
const char *inputs_json = "{"
    "\"num1\": {"
        "\"content\": \"MjU=\","  // Base64 for "25"
        "\"mimeType\": \"application/json\","
        "\"charset\": \"UTF-8\""
    "},"
    "\"num2\": {"
        "\"content\": \"MTc=\","  // Base64 for "17"
        "\"mimeType\": \"application/json\""
    "}"
"}";

dw_execution_result *result = dw_run(runtime, "num1 + num2", inputs_json);
```

### Binary Data Input

```c
unsigned char data[] = {0x48, 0x65, 0x6c, 0x6c, 0x6f};  // "Hello"
char *inputs = dw_create_input_bytes("payload", data, sizeof(data), 
                                     "application/octet-stream", NULL);

dw_execution_result *result = dw_run(runtime, 
    "sizeOf(payload)", 
    inputs);

dw_free_string(inputs);
dw_free_result(result);
```

### Streaming Output with Callback

```c
// Callback receives chunks as they're produced
int my_write_callback(void *ctx, const char *buffer, int length) {
    FILE *f = (FILE *)ctx;
    fwrite(buffer, 1, length, f);
    return 0;  // 0 = success, non-zero = abort
}

FILE *output = fopen("output.json", "wb");

dw_streaming_result *result = dw_run_callback(
    runtime,
    "output application/json --- (1 to 10000) map {id: $}",
    my_write_callback,
    output,
    NULL
);

fclose(output);

if (dw_streaming_result_success(result)) {
    printf("Wrote %s output\n", dw_streaming_result_mime_type(result));
}

dw_free_streaming_result(result);
```

### Bidirectional Streaming

Stream input AND output with constant memory:

```c
typedef struct {
    FILE *input_file;
    FILE *output_file;
} transform_ctx;

int read_cb(void *ctx, char *buffer, int buffer_size) {
    transform_ctx *tc = (transform_ctx *)ctx;
    size_t n = fread(buffer, 1, buffer_size, tc->input_file);
    return n > 0 ? (int)n : 0;  // 0 = EOF
}

int write_cb(void *ctx, const char *buffer, int length) {
    transform_ctx *tc = (transform_ctx *)ctx;
    fwrite(buffer, 1, length, tc->output_file);
    return 0;
}

transform_ctx ctx;
ctx.input_file = fopen("large.json", "rb");
ctx.output_file = fopen("output.csv", "wb");

dw_streaming_result *result = dw_run_transform(
    runtime,
    "output application/csv --- payload",
    read_cb,
    write_cb,
    "payload",
    "application/json",
    NULL,
    &ctx,
    NULL
);

fclose(ctx.input_file);
fclose(ctx.output_file);
dw_free_streaming_result(result);
```

### Error Handling

```c
dw_execution_result *result = dw_run(runtime, "invalid syntax", NULL);

if (!dw_result_success(result)) {
    fprintf(stderr, "Script error: %s\n", dw_result_error(result));
}

dw_free_result(result);
```

### Base64 Encoding/Decoding

```c
// Encode
const char *text = "Hello, World!";
char *encoded = dw_base64_encode((const unsigned char *)text, strlen(text));
printf("Encoded: %s\n", encoded);
dw_free_string(encoded);

// Decode
size_t decoded_size;
unsigned char *decoded = dw_base64_decode(encoded, &decoded_size);
printf("Decoded: %.*s\n", (int)decoded_size, decoded);
dw_free_bytes(decoded);
```

## API Reference

### Runtime Management

```c
dw_runtime *dw_init(void);
dw_runtime *dw_init_with_path(const char *lib_path);
void dw_cleanup(dw_runtime *runtime);
const char *dw_get_last_error(void);
```

### Basic Execution

```c
dw_execution_result *dw_run(dw_runtime *runtime, const char *script, 
                             const char *inputs_json);
void dw_free_result(dw_execution_result *result);
```

### Result Accessors

```c
bool dw_result_success(const dw_execution_result *result);
const char *dw_result_error(const dw_execution_result *result);
const char *dw_result_get_string(const dw_execution_result *result);
const unsigned char *dw_result_get_bytes(const dw_execution_result *result, 
                                         size_t *out_size);
const char *dw_result_mime_type(const dw_execution_result *result);
const char *dw_result_charset(const dw_execution_result *result);
bool dw_result_is_binary(const dw_execution_result *result);
```

### Streaming Output

```c
dw_stream *dw_run_streaming(dw_runtime *runtime, const char *script, 
                            const char *inputs_json);
int dw_stream_next(dw_stream *stream, const unsigned char **out_buffer, 
                   size_t *out_size);
const dw_streaming_result *dw_stream_metadata(dw_stream *stream);
void dw_stream_free(dw_stream *stream);
```

### Callback-based Streaming

```c
dw_streaming_result *dw_run_callback(dw_runtime *runtime, const char *script,
                                     dw_write_callback callback, void *ctx,
                                     const char *inputs_json);
void dw_free_streaming_result(dw_streaming_result *result);
```

### Bidirectional Streaming

```c
dw_streaming_result *dw_run_transform(dw_runtime *runtime, const char *script,
                                      dw_read_callback read_callback,
                                      dw_write_callback write_callback,
                                      const char *input_name,
                                      const char *input_mime_type,
                                      const char *input_charset,
                                      void *ctx,
                                      const char *inputs_json);
```

### Utility Functions

```c
char *dw_base64_encode(const unsigned char *data, size_t size);
unsigned char *dw_base64_decode(const char *encoded, size_t *out_size);
void dw_free_string(char *str);
void dw_free_bytes(unsigned char *bytes);
char *dw_create_input_string(const char *name, const char *content, 
                              const char *mime_type);
char *dw_create_input_bytes(const char *name, const unsigned char *data, 
                            size_t size, const char *mime_type, 
                            const char *charset);
```

## Compiling Your Application

### Using the Static Library

```bash
gcc -o myapp myapp.c -I/path/to/include -L/path/to/lib \
    -ldataweave -lpthread -ldl
```

### Using the Shared Library

```bash
gcc -o myapp myapp.c -I/path/to/include -L/path/to/lib \
    -ldataweave -lpthread -ldl

# Set library path at runtime
export LD_LIBRARY_PATH=/path/to/lib:$LD_LIBRARY_PATH  # Linux
export DYLD_LIBRARY_PATH=/path/to/lib:$DYLD_LIBRARY_PATH  # macOS
```

### Linking Directly

```bash
gcc -o myapp myapp.c src/dataweave.c -Iinclude -lpthread -ldl
```

## Installation

Install system-wide (requires sudo):

```bash
make install
```

This installs to `/usr/local` by default. To install elsewhere:

```bash
make install PREFIX=/opt/local
```

Uninstall:

```bash
make uninstall
```

## Thread Safety

- `dw_init()` is **NOT thread-safe** - call once per thread
- `dw_run()` and variants are **thread-safe** when using separate runtimes
- **Do not share** a single runtime across threads
- Callbacks may be invoked on background threads (see documentation)

## Memory Management

All functions that allocate memory require explicit cleanup:

- `dw_free_result()` for `dw_execution_result`
- `dw_free_streaming_result()` for `dw_streaming_result`
- `dw_stream_free()` for `dw_stream`
- `dw_free_string()` for strings from utility functions
- `dw_free_bytes()` for bytes from utility functions
- `dw_cleanup()` for `dw_runtime`

## Input Format

The `inputs_json` parameter follows this schema:

```json
{
  "inputName": {
    "content": "<base64-encoded-data>",
    "mimeType": "application/json",
    "charset": "UTF-8",
    "properties": {
      "key": "value"
    }
  }
}
```

Use helper functions to avoid manual base64 encoding:
- `dw_create_input_string()` for text
- `dw_create_input_bytes()` for binary data

## Callback Contracts

### Write Callback

```c
typedef int (*dw_write_callback)(void *ctx, const char *buffer, int length);
```

- Receives output chunks as they're produced
- `buffer` is **NOT null-terminated**
- Return `0` on success, non-zero to abort
- May be called multiple times

### Read Callback

```c
typedef int (*dw_read_callback)(void *ctx, char *buffer, int buffer_size);
```

- Write input data into `buffer` (up to `buffer_size` bytes)
- Return bytes written, `0` on EOF, `-1` on error
- Called on **background thread** (must be thread-safe)
- May be called multiple times

## Project Structure

```
native-lib/c/
├── include/
│   └── dataweave.h       # Public API header
├── src/
│   └── dataweave.c       # Implementation
├── tests/
│   ├── test_dataweave.c  # Comprehensive test suite
│   └── person.xml        # Test data
├── Makefile              # Build system
└── README.md             # This file
```

## Building from Source

```bash
# Build library
make lib

# Build and run tests
make test

# Clean build artifacts
make clean

# Show help
make help
```

## Dependencies

- C compiler (gcc, clang)
- pthread
- dl (dynamic linking)
- DataWeave native library (dwlib.dylib/so/dll)

## Troubleshooting

### "Failed to load library"

Set the `DATAWEAVE_NATIVE_LIB` environment variable:

```bash
export DATAWEAVE_NATIVE_LIB=/path/to/dwlib.dylib
```

Or copy `dwlib` to your working directory.

### "Failed to create GraalVM isolate"

Ensure the native library was built with GraalVM native-image:

```bash
cd ../..
./gradlew :native-lib:nativeCompile
```

### Segmentation fault

- Check that all pointers are valid before use
- Ensure proper cleanup order (free results before runtime)
- Verify callbacks return correct status codes

## License

Same as the parent DataWeave CLI project.

## See Also

- [FFI Contract](../FFI_CONTRACT.md) - Low-level FFI specification
- [Python Implementation](../python/) - Reference implementation
- [Native Library README](../README.md) - Building the native library
