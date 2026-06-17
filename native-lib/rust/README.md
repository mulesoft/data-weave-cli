# DataWeave Rust Bindings

Rust FFI bindings for the DataWeave native library.

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

Add to `Cargo.toml`:
```toml
[dependencies]
dataweave-native = { path = "../path/to/native-lib/rust" }
```

## Usage

### Basic Script Execution

```rust
use dataweave::run;

fn main() {
    let result = run("2 + 2", None).expect("Failed to run script");
    if !result.success {
        eprintln!("Script failed: {}", result.error.unwrap_or_default());
        return;
    }
    let output = result.get_string().expect("Failed to get string");
    println!("{}", output); // "4"
}
```

### Script with Inputs

```rust
use dataweave::run;
use serde_json::json;
use std::collections::HashMap;

fn main() {
    let mut inputs = HashMap::new();
    inputs.insert("num1".to_string(), json!(25));
    inputs.insert("num2".to_string(), json!(17));

    let result = run("num1 + num2", Some(inputs)).expect("Failed to run script");
    let output = result.get_string().expect("Failed to get string");
    println!("{}", output); // "42"
}
```

### JSON Transformation

```rust
let mut inputs = HashMap::new();
inputs.insert(
    "payload".to_string(),
    json!({
        "users": [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
    }),
);
let script = "output application/json --- payload.users map { name: $.name }";
let result = run(script, Some(inputs)).expect("Failed to run script");
```

## Running Tests

```bash
cd native-lib/rust
cargo test
```

## Running Examples

```bash
cargo run --example simple_demo
cargo run --example streaming_demo
```

## API Reference

### `run(script: &str, inputs: Option<HashMap<String, Value>>) -> Result<ExecutionResult>`

Executes a DataWeave script with the given inputs (buffered mode).

**Parameters:**
- `script`: DataWeave script source code
- `inputs`: Optional map of binding names to JSON values (auto-encoded)

**Returns:**
- `Ok(ExecutionResult)`: Execution result with output and metadata
- `Err(Error)`: FFI-level error

### `run_streaming(script: &str, inputs: Option<HashMap<String, Value>>) -> Result<StreamResult>`

Executes a DataWeave script and streams the output via an iterator. Output chunks are delivered as they are produced by the native engine without buffering the entire result in memory.

**Parameters:**
- `script`: DataWeave script source code
- `inputs`: Optional map of binding names to JSON values

**Returns:**
- `Ok(StreamResult)`: An iterator yielding `Result<Vec<u8>>` chunks
- `Err(Error)`: FFI-level error (before streaming starts)

**Usage:**
```rust
let result = run_streaming("output application/json --- (1 to 10000)", None)
    .expect("run_streaming failed");
for chunk_result in &result {
    let chunk = chunk_result.expect("chunk read failed");
    std::io::stdout().write_all(&chunk).unwrap();
}
let metadata = result.metadata().expect("no metadata");
println!("Done: {:?}, {:?}", metadata.mime_type, metadata.charset);
```

### `run_transform<R: Read + Send + 'static>(script: &str, input_reader: R, opts: TransformOptions) -> Result<StreamResult>`

Executes a DataWeave script with streaming input and output. Input data is pulled from the reader and output chunks are delivered via the iterator. Ideal for processing large files with constant memory overhead.

**Parameters:**
- `script`: DataWeave script source code
- `input_reader`: Any type implementing `Read + Send + 'static`
- `opts`: `TransformOptions` with input name, MIME type, and charset

**Returns:**
- `Ok(StreamResult)`: An iterator yielding output chunks
- `Err(Error)`: FFI-level error

**Usage:**
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
    let chunk = chunk_result.expect("chunk read failed");
    out.write_all(&chunk).unwrap();
}
let metadata = result.metadata().expect("no metadata");
```

### `ExecutionResult`

```rust
pub struct ExecutionResult {
    pub success: bool,
    pub result: Option<String>,     // Base64-encoded output
    pub error: Option<String>,       // Error message if !success
    pub binary: bool,
    pub mime_type: Option<String>,
    pub charset: Option<String>,
}
```

**Methods:**
- `get_bytes(&self) -> Result<Vec<u8>>` — decode result to bytes
- `get_string(&self) -> Result<String>` — decode result to UTF-8 string

### `StreamResult`

```rust
pub struct StreamResult { /* fields omitted */ }
```

**Iterator:** Yields `Result<Vec<u8>>` for each output chunk.

**Methods:**
- `metadata(&self) -> Option<StreamingMetadata>` — access metadata after iteration completes

### `StreamingMetadata`

```rust
pub struct StreamingMetadata {
    pub success: bool,
    pub error: Option<String>,
    pub mime_type: Option<String>,
    pub charset: Option<String>,
    pub binary: bool,
}
```

### `TransformOptions`

```rust
pub struct TransformOptions {
    pub input_name: String,       // Binding name (default "payload")
    pub input_mime_type: String,  // MIME type (required)
    pub input_charset: Option<String>,
}
```

## Threading Considerations

- `run_streaming` and `run_transform` spawn a background thread for the FFI call
- Output chunks are delivered via `mpsc::channel`
- Callbacks are invoked from the FFI thread
- Metadata is shared via `Arc<Mutex<>>` and available after iteration
- Each `StreamResult` is independent; you can have multiple active streams

## When to Use Streaming vs Buffered

| Use Case | Recommended API |
|----------|----------------|
| Small scripts, immediate result | `run()` |
| Large output, process as produced | `run_streaming()` |
| Large input and output, constant memory | `run_transform()` |
| File-to-file transformation | `run_transform()` |

## Environment Variables

The build script automatically configures linking. If needed, override with:

```bash
export RUSTFLAGS="-L /path/to/native-lib/build/native/nativeCompile"
```
