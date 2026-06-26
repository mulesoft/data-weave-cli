# DataWeave Native Library FFI Contract

This document specifies the C FFI interface exported by the `dwlib` shared library (dylib/so/dll) that language wrappers must implement.

## Shared Library Name
- **macOS**: `dwlib.dylib`
- **Linux**: `dwlib.so`  
- **Windows**: `dwlib.dll`

## Core C Functions

### 1. Isolate Management (GraalVM)

```c
int graal_create_isolate(
    void* params,
    graal_isolate_t** isolate,
    graal_isolatethread_t** thread
);
```
Creates a GraalVM isolate and attaches the calling thread. Returns 0 on success.

```c
int graal_attach_thread(
    graal_isolate_t* isolate,
    graal_isolatethread_t** thread
);
```
Attaches a new thread to an existing isolate. Required for background worker threads.

```c
int graal_detach_thread(graal_isolatethread_t* thread);
```
Detaches a thread from the isolate.

```c
int graal_tear_down_isolate(graal_isolatethread_t* thread);
```
Tears down the entire isolate and releases resources.

### 2. Basic Script Execution

```c
char* run_script(
    graal_isolatethread_t* thread,
    const char* script,
    const char* inputsJson
);
```
Executes a DataWeave script with JSON-encoded inputs.

**Parameters:**
- `thread`: Active isolate thread
- `script`: UTF-8 DataWeave script source
- `inputsJson`: JSON object mapping input names to input descriptors (see Input Format below)

**Returns:** JSON-encoded result (must be freed with `free_cstring`):
```json
{
  "success": true,
  "result": "<base64-encoded-output>",
  "mimeType": "application/json",
  "charset": "UTF-8",
  "binary": false
}
```
Or on error:
```json
{
  "success": false,
  "error": "<error-message>"
}
```

### 3. Memory Management

```c
void free_cstring(
    graal_isolatethread_t* thread,
    char* pointer
);
```
Frees a C string returned by native functions.

### 4. Callback-based Output Streaming

```c
typedef int (*WriteCallback)(void* ctx, const char* buffer, int length);

char* run_script_callback(
    graal_isolatethread_t* thread,
    const char* script,
    const char* inputsJson,
    WriteCallback writeCallback,
    void* ctx
);
```
Executes a script and streams output chunks via callback.

**WriteCallback contract:**
- Invoked for each output chunk
- Must return 0 on success, non-zero to abort
- `buffer` is NOT null-terminated
- `length` specifies chunk size in bytes

**Returns:** JSON metadata (must be freed with `free_cstring`):
```json
{
  "success": true,
  "mimeType": "application/json",
  "charset": "UTF-8",
  "binary": false
}
```

### 5. Bidirectional Streaming

```c
typedef int (*ReadCallback)(void* ctx, char* buffer, int bufferSize);
typedef int (*WriteCallback)(void* ctx, const char* buffer, int length);

char* run_script_input_output_callback(
    graal_isolatethread_t* thread,
    const char* script,
    const char* inputsJson,
    const char* inputName,
    const char* inputMimeType,
    const char* inputCharset,
    ReadCallback readCallback,
    WriteCallback writeCallback,
    void* ctx
);
```
Executes a script with callback-driven input AND output.

**ReadCallback contract:**
- Invoked on background thread to pull input data
- Must write data into `buffer` (up to `bufferSize` bytes)
- Returns bytes written, 0 on EOF, -1 on error

**Parameters:**
- `inputName`: Binding name for callback-supplied input (e.g., "payload")
- `inputMimeType`: MIME type of input (e.g., "application/json")
- `inputCharset`: Charset of input (NULL for UTF-8)
- `inputsJson`: Additional input bindings (callback input is added automatically)

**Returns:** Same JSON metadata format as `run_script_callback`.

## Input Format (inputsJson)

The `inputsJson` parameter is a JSON object where each key is an input binding name, and each value is an input descriptor:

```json
{
  "payload": {
    "content": "<base64-encoded-data>",
    "mimeType": "application/json",
    "charset": "UTF-8",
    "properties": {
      "optional-key": "optional-value"
    }
  },
  "num1": {
    "content": "MjU=",
    "mimeType": "application/json",
    "charset": "UTF-8"
  }
}
```

**Fields:**
- `content` (required): Base64-encoded input data
- `mimeType` (required): MIME type of the input
- `charset` (optional): Charset (defaults to UTF-8)
- `properties` (optional): Additional metadata key-value pairs

## Data Encoding

1. **Script source**: Always UTF-8 encoded C strings
2. **Input content**: Base64-encoded in JSON  
3. **Output result**: Base64-encoded in JSON response for `run_script`, raw bytes via callback for streaming APIs
4. **Strings**: All JSON strings are UTF-8

## Threading Model

- `run_script` and `run_script_callback`: Single-threaded on calling thread
- `run_script_input_output_callback`: 
  - `WriteCallback` invoked on calling thread
  - `ReadCallback` invoked on background thread (requires `graal_attach_thread`)
- Isolate thread handles are thread-specific
- Multiple threads require separate attached threads via `graal_attach_thread`

## Error Handling

All functions that return `char*` return JSON with `"success": false` on error.

Callback functions return status codes:
- `0`: Success
- Non-zero: Error (aborts execution)

## Example Workflows

### Basic Execution
1. `graal_create_isolate` → get isolate + thread
2. `run_script` → get JSON result
3. `free_cstring` → release result
4. `graal_tear_down_isolate` → cleanup

### Streaming Output
1. `graal_create_isolate`
2. `run_script_callback` with WriteCallback → receive chunks in callback
3. `free_cstring` → release metadata
4. `graal_tear_down_isolate`

### Bidirectional Streaming
1. `graal_create_isolate`
2. `run_script_input_output_callback` with ReadCallback + WriteCallback
3. Background thread attached automatically, calls ReadCallback
4. Calling thread receives WriteCallback invocations
5. `free_cstring` → release metadata
6. `graal_tear_down_isolate`

## Feature Parity Requirements

All language wrappers must implement:

1. **Basic execution**: Synchronous script execution with buffered result
2. **Error handling**: Proper exception/error types for script failures
3. **Input value normalization**: Auto-convert native types to base64+MIME JSON format
4. **Output decoding**: Decode base64 results to bytes/strings based on charset
5. **Streaming output**: Generator/iterator-based streaming from write callback
6. **Bidirectional streaming**: Iterable input + generator output via callbacks
7. **Context manager / RAII**: Automatic resource cleanup (initialize/cleanup)
8. **Type safety**: Strongly typed result objects (ExecutionResult, StreamingResult)
9. **Comprehensive tests**: Port all Python test cases to the target language

## Test Coverage Requirements

Each wrapper must pass equivalent tests for:
- Basic arithmetic (`2 + 2`)
- Script with inputs (`num1 + num2`)
- Context manager / RAII pattern
- Encoding conversion (UTF-16 XML → CSV)
- Auto-conversion of native types (arrays, objects)
- Callback output basic
- Callback output with inputs
- Callback input+output basic
- Callback input+output large data
- run_streaming basic
- run_streaming large (verify multiple chunks)
- run_streaming error handling
- run_streaming with inputs
- run_transform basic
- run_transform large chunked input
- run_transform with file handles
