# DataWeave Native Bindings - API Quick Reference

Side-by-side comparison of the API across all five language bindings.

## 1. Basic Execution (Buffered)

Execute a DataWeave script with inputs and get the complete result in memory.

### Python
```python
import dataweave

result = dataweave.run("2 + 2", inputs={"x": 10})
if result.success:
    output = result.get_string()  # or result.get_bytes()
else:
    print(f"Error: {result.error}")
```

### Node.js
```javascript
const dataweave = require('dataweave-native');

const result = await dataweave.run("2 + 2", { x: 10 });
const output = result.getString();  // or result.getBytes()
```

### Rust
```rust
use dataweave_native::*;

let inputs = maplit::hashmap!{
    "x".to_string() => serde_json::json!(10)
};

let result = dataweave::run("2 + 2", Some(inputs))?;
let output = result.as_string()?;
```

### Go
```go
import dw "github.com/mulesoft/data-weave-cli/native-lib/go"

inputs := map[string]interface{}{
    "x": 10,
}

result, err := dw.Run("2 + 2", inputs)
if err != nil {
    log.Fatal(err)
}
output, _ := result.GetString()
```

### C
```c
#include "dataweave.h"

const char *inputs = "{\"x\": 10}";
dw_result_t *result = dw_run("2 + 2", inputs);

if (result->success) {
    printf("%s\n", result->result);
}
dw_free_result(result);
```

---

## 2. Output Streaming (Large Results)

Stream output in chunks for constant memory usage with large datasets.

### Python
```python
def on_chunk(chunk: bytes):
    process(chunk)  # Handle each chunk

metadata = dataweave.run_streaming(
    script="payload map transform($)",
    inputs={"payload": large_array},
    callback=on_chunk
)
```

### Node.js
```javascript
const stream = await dataweave.runStreaming(
    "payload map transform($)",
    { payload: largeArray }
);

for await (const chunk of stream) {
    process(chunk);  // Handle each chunk
}
```

### Rust
```rust
dataweave::run_streaming(script, Some(inputs), |chunk| {
    process(chunk);  // Handle each chunk
})?;
```

### Go
```go
outputChan, err := dw.RunStreaming(script, inputs)
if err != nil {
    log.Fatal(err)
}

for chunk := range outputChan {
    process(chunk)  // Handle each chunk
}
```

### C
```c
int callback(void *ctx, const char *chunk, int length) {
    process(chunk, length);
    return 0;  // Continue streaming
}

dw_streaming_result_t *result = dw_run_streaming(script, inputs, callback, context);
dw_free_streaming_result(result);
```

---

## 3. Bidirectional Streaming (Input + Output)

Stream both input and output for ETL pipelines and large file transformations.

### Python
```python
def read_input(buffer_size: int) -> bytes:
    return input_stream.read(buffer_size)

def write_output(chunk: bytes):
    output_stream.write(chunk)

metadata = dataweave.run_transform(
    script="transform script",
    inputs={"payload": InputValue(content=b"", mime_type="application/csv")},
    read_callback=read_input,
    write_callback=write_output
)
```

### Node.js
```javascript
async function* inputProvider() {
    for await (const chunk of inputStream) {
        yield chunk;
    }
}

const stream = await dataweave.runTransform(
    "transform script",
    { payload: inputProvider() }
);

for await (const chunk of stream) {
    outputStream.write(chunk);
}
```

### Rust
```rust
fn read_input(buffer: &mut [u8]) -> usize {
    // Read from source, return bytes read
}

fn write_output(chunk: &[u8]) {
    // Write to destination
}

dataweave::run_transform(script, Some(inputs), read_input, write_output)?;
```

### Go
```go
func readInput(bufferSize int) []byte {
    // Read from source
    return data
}

func writeOutput(chunk []byte) {
    // Write to destination
}

result, err := dw.RunTransform(script, inputs, readInput, writeOutput)
```

### C
```c
int read_callback(void *ctx, char *buffer, int bufferSize) {
    // Read into buffer, return bytes read
    return bytesRead;
}

int write_callback(void *ctx, const char *chunk, int length) {
    // Write chunk to destination
    return 0;
}

dw_transform_result_t *result = dw_run_transform(
    script, inputs, read_callback, write_callback, context
);
dw_free_transform_result(result);
```

---

## 4. Error Handling

### Python
```python
result = dataweave.run("invalid script")
if not result.success:
    print(f"Error: {result.error}")
    print(f"Binary: {result.binary}")
```

### Node.js
```javascript
try {
    const result = await dataweave.run("invalid script");
} catch (error) {
    console.error(`Error: ${error.message}`);
}
```

### Rust
```rust
match dataweave::run("invalid script", None) {
    Ok(result) => println!("Success: {}", result.as_string()?),
    Err(e) => eprintln!("Error: {}", e),
}
```

### Go
```go
result, err := dw.Run("invalid script", nil)
if err != nil {
    log.Printf("Error: %v", err)
    return
}
```

### C
```c
dw_result_t *result = dw_run("invalid script", NULL);
if (!result->success) {
    fprintf(stderr, "Error: %s\n", result->error);
}
dw_free_result(result);
```

---

## 5. Working with Different Input Formats

### Python
```python
# JSON input (auto-detected)
inputs = {"payload": {"key": "value"}}

# Explicit format
from dataweave import InputValue
inputs = {
    "payload": InputValue(
        content=csv_bytes,
        mime_type="application/csv",
        properties={"header": "true"}
    )
}

result = dataweave.run(script, inputs)
```

### Node.js
```javascript
// JSON input (auto-detected)
const inputs = { payload: { key: "value" } };

// Explicit format
const inputs = {
    payload: {
        content: Buffer.from(csvData),
        mimeType: "application/csv",
        properties: { header: "true" }
    }
};

const result = await dataweave.run(script, inputs);
```

### Rust
```rust
// JSON input (auto-detected from serde_json::Value)
let inputs = hashmap!{
    "payload".to_string() => json!({"key": "value"})
};

// For explicit formats, use the raw JSON encoding:
let inputs = hashmap!{
    "payload".to_string() => json!({
        "content": base64_content,
        "mimeType": "application/csv",
        "properties": {"header": "true"}
    })
};

let result = dataweave::run(script, Some(inputs))?;
```

### Go
```go
// JSON input (auto-marshaled)
inputs := map[string]interface{}{
    "payload": map[string]interface{}{"key": "value"},
}

// Explicit format via JSON encoding
inputs = map[string]interface{}{
    "payload": map[string]interface{}{
        "content":    base64Content,
        "mimeType":   "application/csv",
        "properties": map[string]string{"header": "true"},
    },
}

result, err := dw.Run(script, inputs)
```

### C
```c
// All inputs are JSON strings
const char *inputs = "{\"payload\": {\"key\": \"value\"}}";

// Explicit format
const char *inputs = "{"
    "\"payload\": {"
        "\"content\": \"<base64>\","
        "\"mimeType\": \"application/csv\","
        "\"properties\": {\"header\": \"true\"}"
    "}"
"}";

dw_result_t *result = dw_run(script, inputs);
```

---

## 6. Lifecycle Management

### Python
```python
# Option 1: Module-level (singleton, auto-cleanup)
import dataweave
result = dataweave.run(script)

# Option 2: Explicit lifecycle
with dataweave.DataWeave() as dw:
    result = dw.run(script)
# Automatically cleaned up
```

### Node.js
```javascript
// Singleton, auto-cleanup on process exit
const result = await dataweave.run(script);

// Manual cleanup (rarely needed)
dataweave.cleanup();
```

### Rust
```rust
// RAII - automatic cleanup via Drop trait
let result = dataweave::run(script, None)?;
// Result dropped here, resources freed
```

### Go
```go
// Option 1: Module-level (singleton)
result, err := dw.Run(script, nil)

// Option 2: Explicit lifecycle
dwInstance, err := dw.New()
if err != nil {
    log.Fatal(err)
}
defer dwInstance.Cleanup()

result, err := dwInstance.Run(script, nil)
```

### C
```c
// Manual memory management
dw_result_t *result = dw_run(script, inputs);
// Use result...
dw_free_result(result);  // MUST call to prevent leaks

// Safe to call on NULL
dw_free_result(NULL);  // No-op, like free()
```

---

## 7. Concurrent/Parallel Execution

### Python
```python
import threading

def worker(script):
    result = dataweave.run(script)
    # Process result...

threads = [threading.Thread(target=worker, args=(script,)) for _ in range(10)]
for t in threads:
    t.start()
for t in threads:
    t.join()
```

### Node.js
```javascript
// Concurrent promises
const promises = [];
for (let i = 0; i < 10; i++) {
    promises.push(dataweave.run(script, inputs));
}

const results = await Promise.all(promises);
```

### Rust
```rust
use std::thread;

let handles: Vec<_> = (0..10).map(|i| {
    thread::spawn(move || {
        dataweave::run(script, Some(inputs))
    })
}).collect();

let results: Vec<_> = handles.into_iter()
    .map(|h| h.join().unwrap())
    .collect();
```

### Go
```go
var wg sync.WaitGroup

for i := 0; i < 10; i++ {
    wg.Add(1)
    go func(index int) {
        defer wg.Done()
        result, err := dw.Run(script, inputs)
        // Process result...
    }(i)
}

wg.Wait()
```

### C
```c
// Use POSIX threads
pthread_t threads[10];

void* worker(void* arg) {
    dw_result_t *result = dw_run(script, inputs);
    dw_free_result(result);
    return NULL;
}

for (int i = 0; i < 10; i++) {
    pthread_create(&threads[i], NULL, worker, NULL);
}

for (int i = 0; i < 10; i++) {
    pthread_join(threads[i], NULL);
}
```

---

## API Comparison Matrix

| Feature | Python | Node.js | Rust | Go | C |
|---------|--------|---------|------|----|----|
| **Basic Execution** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Output Streaming** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Bidirectional Streaming** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Auto JSON Conversion** | ✅ | ✅ | ✅ | ✅ | Manual |
| **Explicit Input Formats** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Error Handling** | Result object | Exceptions | Result<T,E> | (T, error) | Success flag |
| **Memory Management** | Auto | Auto | RAII | Auto | Manual |
| **Thread Safety** | ✅ | ✅ | ✅ Send+Sync | ✅ | ✅ |
| **Async Support** | Callbacks | async/await | Sync only | Channels | Callbacks |
| **Type Safety** | Runtime | Runtime | Compile-time | Runtime | Manual |

---

## Performance Characteristics

| Binding | Overhead | Best Use Case |
|---------|----------|---------------|
| **Python** | ~100-200µs | Scripting, data science, rapid prototyping |
| **Node.js** | ~50-100µs | Web services, async I/O, JavaScript ecosystems |
| **Rust** | ~10-50µs | Systems programming, zero-copy pipelines |
| **Go** | ~20-80µs | Microservices, cloud-native, high concurrency |
| **C** | ~5-20µs | Legacy integration, embedded, low-level |

*Overhead measured on basic `2 + 2` execution (single-core, M1 Mac)*

---

## Installation

| Language | Install Command |
|----------|----------------|
| **Python** | `pip install dataweave-native-*.whl` |
| **Node.js** | `npm install dataweave-native-*.tgz` |
| **Rust** | `cargo add dataweave-native` (or Cargo.toml) |
| **Go** | `go get github.com/mulesoft/data-weave-cli/native-lib/go` |
| **C** | Link against `libdw.a` and `libdwlib.{so,dylib,dll}` |

---

## Documentation

For detailed documentation, see:
- Python: `native-lib/python/README.md`
- Node.js: `native-lib/node/README.md`  
- Rust: `native-lib/rust/README.md`
- Go: `native-lib/go/README.md`
- C: `native-lib/c/README.md`

For comprehensive examples, see: `native-lib/demos/`
