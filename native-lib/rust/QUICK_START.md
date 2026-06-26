# Quick Start Guide

Get up and running with DataWeave Rust bindings in 5 minutes.

## Prerequisites

1. **Rust toolchain** (install from https://rustup.rs/)
2. **DataWeave native library** (build instructions below)

## Build Native Library

```bash
# From repository root
cd data-weave-cli
./gradlew nativeCompile
```

This creates `build/native/nativeCompile/dwlib.{dylib,so,dll}`.

## Create a New Project

```bash
cargo new my-dataweave-app
cd my-dataweave-app
```

## Add Dependency

Edit `Cargo.toml`:

```toml
[dependencies]
dataweave-native = { path = "../data-weave-cli/native-lib/rust" }
```

## Set Environment Variable

```bash
# macOS
export DATAWEAVE_NATIVE_LIB=/path/to/data-weave-cli/build/native/nativeCompile/dwlib.dylib

# Linux
export DATAWEAVE_NATIVE_LIB=/path/to/data-weave-cli/build/native/nativeCompile/dwlib.so

# Windows
set DATAWEAVE_NATIVE_LIB=C:\path\to\data-weave-cli\build\native\nativeCompile\dwlib.dll
```

## Write Your First Program

Edit `src/main.rs`:

```rust
use dataweave_native::{DataWeave, ToInputValue};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize runtime
    let dw = DataWeave::new()?;

    // Simple calculation
    let result = dw.run("2 + 2", HashMap::new())?;
    println!("Result: {}", result.get_string()?);

    // With inputs
    let mut inputs = HashMap::new();
    inputs.insert("x".to_string(), 10.to_input_value()?);
    inputs.insert("y".to_string(), 32.to_input_value()?);

    let result = dw.run("x + y", inputs)?;
    println!("Sum: {}", result.get_string()?);

    Ok(())
}
```

## Run It

```bash
cargo run
```

Expected output:
```
Result: 4
Sum: 42
```

## Common Patterns

### Array Transformation

```rust
let mut inputs = HashMap::new();
inputs.insert("data".to_string(), vec![1, 2, 3, 4, 5].to_input_value()?);

let script = "output application/json\n---\ndata map ($ * 2)";
let result = dw.run(script, inputs)?;
println!("{}", result.get_string()?); // [2, 4, 6, 8, 10]
```

### JSON to CSV

```rust
use dataweave_native::InputValue;

let json = r#"[{"name":"Alice","age":30},{"name":"Bob","age":25}]"#;
let mut inputs = HashMap::new();
inputs.insert(
    "payload".to_string(),
    InputValue::new(json.as_bytes().to_vec(), "application/json"),
);

let script = "output application/csv header=true\n---\npayload";
let result = dw.run(script, inputs)?;
println!("{}", result.get_string()?);
```

### Streaming Large Output

```rust
let script = r#"
    output application/json
    ---
    (1 to 10000) map { id: $, value: $ * 2 }
"#;

let mut stream = dw.run_streaming(script, HashMap::new())?;

while let Some(chunk) = stream.next() {
    // Process chunk
    std::io::stdout().write_all(&chunk)?;
}

let metadata = stream.metadata().unwrap();
println!("\nType: {}", metadata.mime_type.as_deref().unwrap_or("unknown"));
```

### File Transformation

```rust
use std::fs::File;
use std::io::{Read, Write};

// Read input file in chunks
let mut file = File::open("input.json")?;
let mut buffer = Vec::new();
file.read_to_end(&mut buffer)?;
let chunks: Vec<Vec<u8>> = buffer.chunks(8192).map(|c| c.to_vec()).collect();

// Transform
let script = "output application/csv\n---\npayload";
let mut stream = dw.run_transform(
    script,
    chunks.into_iter(),
    "payload",
    "application/json",
    None,
    HashMap::new(),
)?;

// Write output
let mut output = File::create("output.csv")?;
while let Some(chunk) = stream.next() {
    output.write_all(&chunk)?;
}
```

## Error Handling

```rust
match DataWeave::new() {
    Ok(dw) => {
        match dw.run("invalid", HashMap::new()) {
            Ok(result) if result.success => {
                println!("Output: {}", result.get_string()?);
            }
            Ok(result) => {
                eprintln!("Script error: {}", result.error.unwrap_or_default());
            }
            Err(e) => {
                eprintln!("Runtime error: {}", e);
            }
        }
    }
    Err(e) => {
        eprintln!("Failed to initialize: {}", e);
    }
}
```

## Next Steps

- Read the [full README](README.md) for comprehensive documentation
- See [examples/](examples/) for more complete programs
- Review [IMPLEMENTATION.md](IMPLEMENTATION.md) for architecture details
- Check [tests/](tests/) for usage patterns

## Troubleshooting

### Library Not Found

**Error**: `DataWeave native library not found`

**Solution**: Set `DATAWEAVE_NATIVE_LIB` environment variable to the full path of `dwlib.{dylib,so,dll}`.

### Symbol Not Found

**Error**: `Symbol not found: run_script`

**Solution**: Ensure the native library is built with `./gradlew nativeCompile` and matches your platform.

### Thread Attachment Failed

**Error**: `Failed to attach worker thread to isolate`

**Solution**: This usually indicates a corrupted isolate. Recreate the `DataWeave` instance.

## Platform Notes

### macOS

```bash
export DATAWEAVE_NATIVE_LIB="$(pwd)/build/native/nativeCompile/dwlib.dylib"
```

### Linux

```bash
export DATAWEAVE_NATIVE_LIB="$(pwd)/build/native/nativeCompile/dwlib.so"
```

May need to install additional dependencies:
```bash
sudo apt-get install build-essential
```

### Windows

```powershell
$env:DATAWEAVE_NATIVE_LIB = "$PWD\build\native\nativeCompile\dwlib.dll"
```

Requires Visual Studio Build Tools.

## Performance Tips

1. **Reuse `DataWeave` instances** - Creating an instance is expensive (GraalVM isolate creation)
2. **Use streaming for large outputs** - Avoids memory pressure
3. **Batch small operations** - Thread spawning has overhead
4. **Pre-allocate input buffers** - Reduces allocations

## Community

- Report issues: [GitHub Issues](https://github.com/mulesoft/data-weave-cli/issues)
- Documentation: [DataWeave Docs](https://docs.mulesoft.com/dataweave/)
