# native-lib

## Overview

`native-lib` builds a **GraalVM native shared library** that embeds the MuleSoft **DataWeave runtime** and exposes a small C-compatible API.

The main purpose is to allow non-JVM consumers (most notably the Python package in `native-lib/python`) to execute DataWeave scripts **without running a JVM**, while still using the official DataWeave runtime.

## Architecture (GraalVM + FFI)

```
┌─────────────────────────────────────────────┐
│              Python Process                 │
│                                             │
│  ┌────────────────────────────────────────┐ │
│  │  Application Script                    │ │
│  │  - Python: ctypes                      │ │
│  └──────────────┬─────────────────────────┘ │
│                 │                           │
│                 │ FFI Call                  │
│                 ▼                           │
│  ┌────────────────────────────────────────┐ │
│  │  Native Shared Library (dwlib)         │ │
│  │  ┌──────────────────────────────────┐  │ │
│  │  │  GraalVM Isolate                 │  │ │
│  │  │  - NativeLib.run_script()        │  │ │
│  │  │  - DataWeave script execution    │  │ │
│  │  └──────────────────────────────────┘  │ │
│  └────────────────────────────────────────┘ │
└─────────────────────────────────────────────┘
```

## Building with Gradle

### Prerequisites

- A GraalVM distribution installed that includes `native-image`.
- Enough memory for native-image (this build config uses `-J-Xmx6G`).

### Build the shared library

From the repository root:

```bash
./gradlew :native-lib:nativeCompile
```

The shared library is produced under:

- `native-lib/build/native/nativeCompile/`

and is named:

- macOS: `dwlib.dylib`
- Linux: `dwlib.so`
- Windows: `dwlib.dll`

### Stage the library into the Python package (dev workflow)

```bash
./gradlew :native-lib:stagePythonNativeLib
```

This copies `dwlib.*` into:

- `native-lib/python/src/dataweave/native/`

### Build a Python wheel (bundles the native library)

```bash
./gradlew :native-lib:buildPythonWheel
```

The wheel will be created in:

- `native-lib/python/dist/`

## Installing for use in a Python project

### Option A: Install the produced wheel (recommended)

After `:native-lib:buildPythonWheel`:

```bash
python3 -m pip install native-lib/python/dist/dataweave_native-0.0.1-*.whl
```

This wheel includes the `dwlib.*` shared library inside the Python package.

### Option B: Editable install for development

1. Stage the native library:

```bash
./gradlew :native-lib:stagePythonNativeLib
```

2. Install the Python package in editable mode:

```bash
python3 -m pip install -e native-lib/python
```

### Option C: Use an externally-built library via an environment variable

If you want to point Python at a specific built artifact, set:

- `DATAWEAVE_NATIVE_LIB=/absolute/path/to/dwlib.(dylib|so|dll)`

The Python module will also try a few fallbacks (including the wheel-bundled location).

## Using the library (Python examples)

All examples below assume:

```python
import dataweave
```

### 1) Simple script

```python
result = dataweave.run("2 + 2")
assert result.success is True
print(result.get_string())  # "4"
```

### 2) Script with inputs (auto-detected types)

Inputs can be plain Python values. The module auto-encodes them as JSON or text.

```python
result = dataweave.run(
    "num1 + num2",
    {"num1": 25, "num2": 17},
)
print(result.get_string())  # "42"
```

### 3) Script with inputs (explicit mime type, charset, properties)

Use an explicit input dict when you need full control over how DataWeave interprets bytes.

```python
script = "payload.person"
xml_bytes = b"<?xml version=\"1.0\" encoding=\"UTF-16\"?><person><name>Billy</name><age>31</age></person>".decode("utf-8").encode("utf-16")

result = dataweave.run(
    script,
    {
        "payload": {
            "content": xml_bytes,
            "mimeType": "application/xml",
            "charset": "UTF-16",
            "properties": {
                "nullValueOn": "empty",
                "maxAttributeSize": 256
            },
        }
    },
)

if result.success:
    print(result.get_string())
else:
    print(result.error)
```

You can also use `InputValue` for the same purpose:

```python
input_value = dataweave.InputValue(
    content="1234567",
    mime_type="application/csv",
    properties={"header": False, "separator": "4"},
)

result = dataweave.run("in0.column_1[0]", {"in0": input_value})
print(result.get_string())  # '"567"'
```

### 4) Context manager (explicit lifecycle)

The module-level API (`dataweave.run(...)`) uses a shared singleton. Use `DataWeave` directly when you need explicit control over isolate lifecycle or want multiple independent instances:

```python
with dataweave.DataWeave() as dw:
    r1 = dw.run("2 + 2")
    r2 = dw.run("x + y", {"x": 10, "y": 32})

    print(r1.get_string())  # "4"
    print(r2.get_string())  # "42"
```

### 5) Error handling

There are three error types:

- `DataWeaveLibraryNotFoundError` — the native library cannot be located/loaded.
- `DataWeaveScriptError` — script compilation or runtime error (subclass of `DataWeaveError`). Carries the full result on `.result`.
- `DataWeaveError` — FFI-level failures (isolate creation, library calls).

**Option A: Use `raise_on_error=True` for a single try/except (recommended)**

```python
try:
    result = dataweave.run("invalid syntax here", raise_on_error=True)
    print(result.get_string())

except dataweave.DataWeaveScriptError as e:
    print(f"Script error: {e.result.error}")

except dataweave.DataWeaveLibraryNotFoundError:
    # Build it first: ./gradlew :native-lib:nativeCompile
    raise
```

**Option B: Check `result.success` manually (default, backward-compatible)**

```python
result = dataweave.run("invalid syntax here")

if not result.success:
    print(f"Error: {result.error}")
else:
    print(result.get_string())
```

### 6) Output streaming

Use `run_streaming` to execute a script and receive output chunks as they are produced, without buffering the entire result in memory.

```python
with dataweave.DataWeave() as dw:
    stream = dw.run_streaming("output application/json --- (1 to 10000) map {id: $}")
    for chunk in stream:
        sys.stdout.buffer.write(chunk)
    metadata = stream.metadata  # StreamingResult with mime_type, charset, etc.
    print(f"\nDone: {metadata.mime_type}, {metadata.charset}")
```

Or with the module-level API:

```python
stream = dataweave.run_streaming("output application/csv --- payload", {"payload": [1, 2, 3]})
output = b"".join(stream)
```

### 7) Input and output streaming

Use `run_transform` to stream both input and output — feed an iterable of bytes in, receive a generator of bytes out. Ideal for processing large files or network streams with constant memory.

```python
# Stream a file through DataWeave
with open("large.json", "rb") as f:
    stream = dataweave.run_transform(
        "output application/csv --- payload",
        input_stream=iter(lambda: f.read(8192), b""),
        input_mime_type="application/json",
    )
    with open("output.csv", "wb") as out:
        for chunk in stream:
            out.write(chunk)
    metadata = stream.metadata
```

Works with any iterable — generators, lists, network sockets:

```python
# From an in-memory list
stream = dataweave.run_transform(
    "output application/json --- payload map ($ * $)",
    input_stream=[b"[1,2,3,4,5]"],
    input_mime_type="application/json",
)
print(b"".join(stream))  # [1,4,9,16,25]
```

```python
# From a generator producing chunks
def read_from_network(sock):
    while chunk := sock.recv(4096):
        yield chunk

stream = dataweave.run_transform(
    "output application/json --- sizeOf(payload)",
    input_stream=read_from_network(conn),
    input_mime_type="application/json",
)
for chunk in stream:
    process(chunk)
```

### 8) I/O streaming with callbacks (low-level)

Use `run_input_output_callback` when you need direct callback control (e.g. integration with event-driven frameworks). For most use cases, prefer `run_transform` above.

```python
json_input = b'[1,2,3,4,5]'
pos = 0

def read_cb(buf_size):
    nonlocal pos
    chunk = json_input[pos:pos + buf_size]
    pos += len(chunk)
    return chunk  # return b"" when done

chunks = []
def write_cb(data):
    chunks.append(data)
    return 0  # 0 = success

result = dataweave.run_input_output_callback(
    "output application/json deferred=true --- payload map ($ * $)",
    input_name="payload",
    input_mime_type="application/json",
    read_callback=read_cb,
    write_callback=write_cb,
)

print(result)            # StreamingResult(success=True, ...)
print(b"".join(chunks))  # [1,4,9,16,25]
```

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

### 3) Output streaming

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

### 4) Bidirectional streaming (input + output)

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

See [go/README.md](go/README.md) for full documentation.

## Using the library (Rust examples)

All examples below assume:

```rust
use dataweave::{run, run_streaming, run_transform, TransformOptions};
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

### 3) Output streaming

```rust
let result = run_streaming("output application/json --- (1 to 10000)", None)
    .expect("run_streaming failed");
for chunk_result in &result {
    let chunk = chunk_result.expect("chunk failed");
    std::io::stdout().write_all(&chunk).unwrap();
}
let metadata = result.metadata().expect("no metadata");
println!("Done: {:?}, {:?}", metadata.mime_type, metadata.charset);
```

### 4) Bidirectional streaming (input + output)

```rust
use std::fs::File;

let file = File::open("large.json").expect("open file");
let opts = TransformOptions {
    input_name: "payload".to_string(),
    input_mime_type: "application/json".to_string(),
    input_charset: None,
};
let result = run_transform("output application/csv --- payload", file, opts)
    .expect("run_transform failed");
let mut out = File::create("output.csv").expect("create output");
for chunk_result in &result {
    let chunk = chunk_result.expect("chunk failed");
    out.write_all(&chunk).unwrap();
}
let metadata = result.metadata().expect("no metadata");
```

See [rust/README.md](rust/README.md) for full documentation.
