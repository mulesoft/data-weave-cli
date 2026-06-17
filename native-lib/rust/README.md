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
```

## API Reference

### `run(script: &str, inputs: Option<HashMap<String, Value>>) -> Result<ExecutionResult>`

Executes a DataWeave script with the given inputs.

**Parameters:**
- `script`: DataWeave script source code
- `inputs`: Optional map of binding names to JSON values (auto-encoded)

**Returns:**
- `Ok(ExecutionResult)`: Execution result with output and metadata
- `Err(Error)`: FFI-level error

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

## Environment Variables

The build script automatically configures linking. If needed, override with:

```bash
export RUSTFLAGS="-L /path/to/native-lib/build/native/nativeCompile"
```
