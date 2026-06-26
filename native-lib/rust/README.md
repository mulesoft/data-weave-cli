# DataWeave Native Library - Rust Bindings

Rust bindings for the DataWeave native library, providing safe, idiomatic access to DataWeave script execution via GraalVM.

## Features

- **Safe FFI abstractions**: Minimal unsafe code, automatic resource cleanup via RAII (Drop trait)
- **Buffered execution**: Execute scripts and get complete results
- **Output streaming**: Stream large outputs with constant memory usage
- **Bidirectional streaming**: Stream both input and output for memory-efficient transformations
- **Type-safe results**: Strongly typed `ExecutionResult` and `StreamingResult`
- **Auto-conversion**: Convert native Rust types to DataWeave inputs automatically
- **Thread-safe**: Send + Sync traits where appropriate

## Prerequisites

Before using this library, you must build the DataWeave native library:

```bash
cd ../../
./gradlew nativeCompile
```

This creates the native library at:
- **macOS**: `build/native/nativeCompile/dwlib.dylib`
- **Linux**: `build/native/nativeCompile/dwlib.so`
- **Windows**: `build/native/nativeCompile/dwlib.dll`

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
dataweave-native = { path = "../native-lib/rust" }
```

Or if published to crates.io:

```toml
[dependencies]
dataweave-native = "0.1"
```

## Library Discovery

The native library is discovered in the following order:

1. `DATAWEAVE_NATIVE_LIB` environment variable (absolute path)
2. `native/dwlib.{dylib,so,dll}` relative to executable
3. `../build/native/nativeCompile/dwlib.{dylib,so,dll}` (dev builds)
4. Current directory

Set the environment variable for custom locations:

```bash
export DATAWEAVE_NATIVE_LIB=/path/to/dwlib.dylib
```

## Usage Examples

### Basic Execution

```rust
use dataweave_native::{DataWeave, ToInputValue};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a DataWeave runtime instance
    let dw = DataWeave::new()?;

    // Execute a simple script
    let result = dw.run("2 + 2", HashMap::new())?;
    println!("Result: {}", result.get_string()?); // "4"

    Ok(())
}
```

### Script with Inputs

```rust
use dataweave_native::{DataWeave, ToInputValue};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dw = DataWeave::new()?;

    let mut inputs = HashMap::new();
    inputs.insert("num1".to_string(), 25.to_input_value()?);
    inputs.insert("num2".to_string(), 17.to_input_value()?);

    let result = dw.run("num1 + num2", inputs)?;
    println!("Result: {}", result.get_string()?); // "42"

    Ok(())
}
```

### Auto-Conversion of Native Types

```rust
use dataweave_native::{DataWeave, ToInputValue};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dw = DataWeave::new()?;

    let mut inputs = HashMap::new();
    inputs.insert("numbers".to_string(), vec![1, 2, 3].to_input_value()?);
    inputs.insert("message".to_string(), "Hello".to_input_value()?);
    inputs.insert("flag".to_string(), true.to_input_value()?);

    let result = dw.run("numbers[0]", inputs)?;
    println!("Result: {}", result.get_string()?); // "1"

    Ok(())
}
```

### Output Streaming

Stream large outputs with constant memory overhead:

```rust
use dataweave_native::DataWeave;
use std::collections::HashMap;
use std::io::Write;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dw = DataWeave::new()?;

    let script = r#"
        output application/json
        ---
        (1 to 10000) map { id: $, name: "item_" ++ $ }
    "#;

    let mut stream = dw.run_streaming(script, HashMap::new())?;

    // Process chunks as they arrive
    while let Some(chunk) = stream.next() {
        std::io::stdout().write_all(&chunk)?;
    }

    // Access metadata after iteration
    let metadata = stream.metadata().expect("Expected metadata");
    println!("\nMIME type: {}", metadata.mime_type.as_deref().unwrap_or("unknown"));

    Ok(())
}
```

### Bidirectional Streaming

Stream both input and output for memory-efficient transformations:

```rust
use dataweave_native::DataWeave;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dw = DataWeave::new()?;

    // Read input file in chunks
    let file = File::open("large.json")?;
    let chunks: Vec<Vec<u8>> = file
        .bytes()
        .collect::<Result<Vec<_>, _>>()?
        .chunks(8192)
        .map(|c| c.to_vec())
        .collect();

    let script = r#"
        output application/csv header=true
        ---
        payload
    "#;

    let mut stream = dw.run_transform(
        script,
        chunks.into_iter(),
        "payload",
        "application/json",
        None,
        HashMap::new(),
    )?;

    // Write output chunks as they arrive
    let mut output = File::create("output.csv")?;
    while let Some(chunk) = stream.next() {
        output.write_all(&chunk)?;
    }

    Ok(())
}
```

### Callback-Based Streaming

For lower-level control, use callbacks directly:

```rust
use dataweave_native::DataWeave;
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dw = DataWeave::new()?;

    let mut output_chunks = Vec::new();

    let result = dw.run_callback(
        "2 + 2",
        |data: &[u8]| {
            output_chunks.push(data.to_vec());
            0 // Return 0 for success
        },
        HashMap::new(),
    )?;

    let full: Vec<u8> = output_chunks.into_iter().flatten().collect();
    println!("Result: {}", String::from_utf8(full)?);

    Ok(())
}
```

### Encoding Conversion

Read UTF-16 XML and convert to CSV:

```rust
use dataweave_native::{DataWeave, InputValue};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dw = DataWeave::new()?;

    let mut file = File::open("data.xml")?;
    let mut xml_bytes = Vec::new();
    file.read_to_end(&mut xml_bytes)?;

    let script = r#"
        output application/csv header=true
        ---
        [payload.person]
    "#;

    let mut inputs = HashMap::new();
    inputs.insert(
        "payload".to_string(),
        InputValue::new(xml_bytes, "application/xml")
            .with_charset("UTF-16"),
    );

    let result = dw.run(script, inputs)?;
    println!("{}", result.get_string()?);

    Ok(())
}
```

### Error Handling

```rust
use dataweave_native::{DataWeave, DataWeaveError};
use std::collections::HashMap;

fn main() {
    let dw = match DataWeave::new() {
        Ok(dw) => dw,
        Err(DataWeaveError::LibraryNotFound) => {
            eprintln!("Native library not found. Build it with: ./gradlew nativeCompile");
            return;
        }
        Err(e) => {
            eprintln!("Failed to initialize: {}", e);
            return;
        }
    };

    let result = dw.run("invalid_syntax", HashMap::new()).unwrap();

    if !result.success {
        eprintln!("Script error: {}", result.error.unwrap_or_default());
    }
}
```

## API Reference

### `DataWeave`

Main runtime interface. Automatically cleans up resources on drop.

#### Methods

- `new() -> Result<Self, DataWeaveError>` - Create a new runtime instance
- `run(script: &str, inputs: HashMap<String, InputValue>) -> Result<ExecutionResult, DataWeaveError>` - Execute a script with buffered output
- `run_streaming(script: &str, inputs: HashMap<String, InputValue>) -> Result<Stream, DataWeaveError>` - Execute a script with streaming output
- `run_callback<F>(script: &str, write_callback: F, inputs: HashMap<String, InputValue>) -> Result<StreamingResult, DataWeaveError>` - Execute with callback-based output
- `run_transform<I>(script: &str, input_stream: I, input_name: &str, input_mime_type: &str, input_charset: Option<&str>, inputs: HashMap<String, InputValue>) -> Result<Stream, DataWeaveError>` - Bidirectional streaming

### `ExecutionResult`

Result of buffered execution.

#### Fields

- `success: bool` - Whether execution succeeded
- `result: Option<String>` - Base64-encoded output (if successful)
- `error: Option<String>` - Error message (if failed)
- `binary: bool` - Whether output is binary
- `mime_type: Option<String>` - Output MIME type
- `charset: Option<String>` - Output charset

#### Methods

- `get_bytes() -> Result<Vec<u8>, DataWeaveError>` - Get output as raw bytes
- `get_string() -> Result<String, DataWeaveError>` - Get output as UTF-8 string

### `StreamingResult`

Metadata from streaming execution.

#### Fields

- `success: bool` - Whether execution succeeded
- `error: Option<String>` - Error message (if failed)
- `mime_type: Option<String>` - Output MIME type
- `charset: Option<String>` - Output charset
- `binary: bool` - Whether output is binary

### `Stream`

Iterator over output chunks.

#### Methods

- `next() -> Option<Vec<u8>>` - Get next chunk
- `metadata() -> Option<&StreamingResult>` - Get metadata after iteration completes
- `error() -> Option<&DataWeaveError>` - Get error if stream failed

### `InputValue`

Input binding descriptor.

#### Methods

- `new(content: Vec<u8>, mime_type: impl Into<String>) -> Self` - Create new input
- `with_charset(charset: impl Into<String>) -> Self` - Set charset
- `with_properties(properties: HashMap<String, JsonValue>) -> Self` - Set metadata properties

### `ToInputValue` Trait

Auto-convert native Rust types to `InputValue`.

Implemented for:
- `String`, `&str` → `text/plain`
- `i32`, `i64`, `f64`, `bool` → `application/json`
- `Vec<T>` where `T: Serialize` → `application/json`
- `serde_json::Value` → `application/json`

## Error Types

### `DataWeaveError`

Main error type:

- `LibraryNotFound` - Native library not found
- `LibraryLoad(String)` - Failed to load library
- `SymbolNotFound(String)` - Missing function in library
- `IsolateCreation(i32)` - Failed to create GraalVM isolate
- `ScriptError(String)` - Script execution failed
- `Utf8Error`, `JsonError`, `Base64Error` - Data conversion errors

### `DataWeaveScriptError`

Wraps `ExecutionResult` for script failures (not currently used, but available for explicit error handling).

## Testing

Run the test suite:

```bash
cargo test
```

Tests cover:
- Basic script execution
- Script with inputs
- RAII pattern (Drop trait)
- Encoding conversion (UTF-16 → UTF-8)
- Auto-conversion of native types
- Callback output streaming
- Callback input+output streaming
- `run_streaming` with various scenarios
- `run_transform` with file I/O
- Error handling

## Thread Safety

- `DataWeave` is `Send + Sync`
- Each instance manages its own GraalVM isolate
- Streaming operations spawn background threads with proper isolate attachment
- Multiple `DataWeave` instances can be used concurrently

## Performance Considerations

- **Buffered execution** (`run`): Simple but holds entire output in memory
- **Streaming** (`run_streaming`, `run_transform`): Constant memory overhead, ideal for large outputs
- **Callbacks** (`run_callback`, `run_input_output_callback`): Lowest-level API, maximum control

## FFI Safety

- All FFI calls are encapsulated in safe abstractions
- Unsafe blocks are minimized and carefully reviewed
- Resources are automatically cleaned up via RAII (Drop trait)
- Thread attachment/detachment is handled automatically for streaming operations

## License

MIT OR Apache-2.0

## Contributing

Ensure all tests pass before submitting PRs:

```bash
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```
