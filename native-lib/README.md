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

---

## Installing for use in a Node.js project

### Option A: Install the produced tarball (recommended)

After `:native-lib:buildNodePackage`:

```bash
npm install native-lib/node/dataweave-native-0.0.1.tgz
```

This tarball includes the pre-built native addon and the `dwlib.*` shared library.

### Option B: Development install (link)

1. Stage the native library:

```bash
./gradlew :native-lib:stageNodeNativeLib
```

2. Build the Node package:

```bash
cd native-lib/node
npm install
npx node-gyp rebuild
npx tsc
```

3. Link into your project:

```bash
npm link native-lib/node
```

### Option C: Use an externally-built library via an environment variable

Set `DATAWEAVE_NATIVE_LIB=/absolute/path/to/dwlib.(dylib|so|dll)` before running your application.

The module also searches:
1. `<package>/native/dwlib.*`
2. `<repo>/native-lib/build/native/nativeCompile/dwlib.*` (dev fallback)
3. Current working directory

### Building with Gradle

```bash
# Stage native library into node/native/
./gradlew :native-lib:stageNodeNativeLib

# Build the full .tgz package (stage + compile addon + tsc + npm pack)
./gradlew :native-lib:buildNodePackage

# Run Node.js tests
./gradlew :native-lib:nodeTest

# Skip Node tests in CI: -PskipNodeTests=true
```

### Requirements

- Node.js >= 18
- A C compiler (for `node-gyp` to build the native addon)
- The `dwlib` shared library (staged by Gradle or pointed to via env var)

## Using the library (Node.js examples)

All examples below assume:

```typescript
import { run, runStreaming, runTransform, cleanup } from "@dataweave/native";
```

### 1) Simple script

```typescript
const result = run("2 + 2");
console.log(result.getString()); // "4"
```

### 2) Script with inputs (auto-detected types)

Inputs can be plain JS values. The module auto-encodes them as JSON.

```typescript
const result = run("num1 + num2", { num1: 25, num2: 17 });
console.log(result.getString()); // "42"
```

### 3) Script with inputs (explicit mime type, charset, properties)

Use an explicit input object when you need full control over how DataWeave interprets bytes.

```typescript
import { readFileSync } from "fs";

const xmlBytes = readFileSync("person.xml");

const result = run("payload.person", {
  payload: {
    content: xmlBytes,
    mimeType: "application/xml",
    charset: "UTF-16",
    properties: {
      nullValueOn: "empty",
      maxAttributeSize: 256,
    },
  },
});

if (result.success) {
  console.log(result.getString());
} else {
  console.error(result.error);
}
```

### 4) Explicit instance lifecycle

The module-level API (`run(...)`) uses a shared singleton. Use the `DataWeave` class directly when you need explicit control over isolate lifecycle:

```typescript
import { DataWeave } from "@dataweave/native";

const dw = new DataWeave();
dw.initialize();

const r1 = dw.run("2 + 2");
const r2 = dw.run("x + y", { x: 10, y: 32 });

console.log(r1.getString()); // "4"
console.log(r2.getString()); // "42"

dw.cleanup();
```

### 5) Error handling

There are two error classes:

- `DataWeaveError` — library/isolate-level failures (library not found, initialization failed).
- `DataWeaveScriptError` — script compilation or runtime error (subclass of `DataWeaveError`). Carries the full result on `.result`.

**Option A: Use `raiseOnError: true` for try/catch (recommended)**

```typescript
import { run, DataWeaveScriptError } from "@dataweave/native";

try {
  const result = run("invalid syntax here", {}, { raiseOnError: true });
  console.log(result.getString());
} catch (e) {
  if (e instanceof DataWeaveScriptError) {
    console.error(`Script error: ${e.result.error}`);
  } else {
    throw e;
  }
}
```

**Option B: Check `result.success` manually (default)**

```typescript
const result = run("invalid syntax here");

if (!result.success) {
  console.error(`Error: ${result.error}`);
} else {
  console.log(result.getString());
}
```

### 6) Output streaming

Use `runStreaming` to execute a script and receive output chunks as they are produced, without buffering the entire result in memory. Returns an `AsyncGenerator<Buffer, StreamingResult>`.

```typescript
const gen = runStreaming(
  'output application/json --- (1 to 10000) map {id: $, name: "item_" ++ $}'
);

let result = await gen.next();
while (!result.done) {
  process.stdout.write(result.value);
  result = await gen.next();
}

const metadata = result.value; // StreamingResult
console.log(`\nDone: ${metadata.mimeType}, ${metadata.charset}`);
```

Or with `for await`:

```typescript
const gen = runStreaming("output application/csv --- payload", {
  payload: [1, 2, 3],
});

const chunks: Buffer[] = [];
for await (const chunk of gen) {
  chunks.push(chunk);
}
const output = Buffer.concat(chunks).toString("utf-8");
```

### 7) Input and output streaming (bidirectional)

Use `runTransform` to stream both input and output — feed an `Iterable<Buffer>` or `AsyncIterable<Buffer>` in, receive an `AsyncGenerator<Buffer>` out.

**Important: sync vs async input and memory usage**

The native read callback is invoked synchronously on the JS main thread, which means:

- **Synchronous iterables** (arrays, generators) are consumed **on-demand** — only one chunk is held in memory at a time. This gives constant-memory streaming, comparable to the Python API.
- **Async iterables** (e.g. `fs.createReadStream()`) **must be fully pre-buffered** into memory before the transform starts, because their `.next()` returns a Promise that cannot be awaited inside a synchronous callback.

For large inputs, prefer a **synchronous generator** to get true streaming with minimal memory:

```typescript
import { readFileSync } from "fs";

// Good: sync generator → constant memory (~150 MB for 50M elements)
function* chunked(data: Buffer, size = 8192): Generator<Buffer> {
  for (let i = 0; i < data.length; i += size) {
    yield data.subarray(i, i + size);
  }
}
const gen = runTransform("output csv --- payload", chunked(readFileSync("large.json")), {
  mimeType: "application/json",
});
```

Using an async readable stream still works but will buffer the entire input first:

```typescript
import { createReadStream } from "fs";
import { createWriteStream } from "fs";

// Works but pre-buffers the full input into memory
const input = createReadStream("large.json");
const gen = runTransform("output application/csv --- payload", input, {
  mimeType: "application/json",
});

const out = createWriteStream("output.csv");
for await (const chunk of gen) {
  out.write(chunk);
}
out.end();
```

Works with any iterable — arrays, generators, streams:

```typescript
// From an in-memory array
const input = [Buffer.from("[1,2,3,4,5]")];
const gen = runTransform(
  "output application/json --- payload map ($ * $)",
  input,
  { mimeType: "application/json" }
);

const chunks: Buffer[] = [];
for await (const chunk of gen) {
  chunks.push(chunk);
}
console.log(Buffer.concat(chunks).toString()); // [1,4,9,16,25]
```

```typescript
// From a generator producing chunks
function* chunked(data: Buffer, size = 4096): Generator<Buffer> {
  for (let i = 0; i < data.length; i += size) {
    yield data.subarray(i, i + size);
  }
}

const largeJson = Buffer.from(JSON.stringify(Array.from({ length: 1000 }, (_, i) => ({ id: i }))));
const gen = runTransform(
  "output application/json --- sizeOf(payload)",
  chunked(largeJson),
  { mimeType: "application/json" }
);

for await (const chunk of gen) {
  process.stdout.write(chunk); // "1000"
}
```

### 8) Transform with additional inputs

Pass extra named inputs alongside the streamed input:

```typescript
const input = [Buffer.from('[{"price": 100}, {"price": 200}]')];
const gen = runTransform(
  "output application/json --- payload map ($.price * rate)",
  input,
  {
    mimeType: "application/json",
    inputs: { rate: 1.5 },
  }
);

for await (const chunk of gen) {
  process.stdout.write(chunk); // [150.0, 300.0]
}
```

### 9) Cleanup

The module registers a `process.on('exit')` handler to clean up automatically. For explicit control:

```typescript
import { cleanup } from "@dataweave/native";

// When done with all DataWeave operations
cleanup();
```
