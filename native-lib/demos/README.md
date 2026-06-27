# DataWeave Native Bindings - Comprehensive Demos

This directory contains comprehensive demonstration programs for all DataWeave native language bindings. Each demo showcases the full capability set of the bindings in an idiomatic way for that language.

## Overview

All demos demonstrate:

1. **Basic Operations** - Arithmetic, string manipulation, array operations
2. **Working with Inputs** - Variable substitution, payload transformation
3. **JSON Transformations** - Array mapping, filtering, grouping
4. **Format Conversions** - JSON ↔ CSV ↔ XML transformations (Python only)
5. **Streaming** - Constant memory processing of large datasets
6. **Error Handling** - Syntax, runtime, and type errors
7. **Advanced Features** - Reduce, pattern matching, complex transformations
8. **Language-Specific** - Concurrency (Rust/Go), memory management (C)

## Available Demos

| Language | File | Lines | Key Features |
|----------|------|-------|--------------|
| **Python** | `python_comprehensive_demo.py` | ~500 | Streaming, bidirectional I/O, format conversions |
| **Rust** | `rust_comprehensive_demo.rs` | ~400 | Thread safety, concurrent execution, RAII |
| **Go** | `go_comprehensive_demo.go` | ~450 | Goroutine safety, channel-based streaming |
| **C** | `c_comprehensive_demo.c` | ~450 | Explicit memory management, callback patterns |

## Prerequisites

Before running the demos, you must:

1. **Build the native library**:
   ```bash
   cd ..  # Go to native-lib directory
   ./gradlew nativeCompile
   ```

2. **Language-specific requirements**:
   - **Python**: Python 3.7+ with `dataweave` module installed
   - **Rust**: Cargo 1.70+ with native library linked
   - **Go**: Go 1.21+ with CGO enabled
   - **C**: GCC or Clang compiler

## Running the Demos

### Python

```bash
# Option 1: Direct execution (development mode)
cd demos
python3 python_comprehensive_demo.py

# Option 2: After installing the wheel
pip install ../python/dist/dataweave_native-*.whl
python3 python_comprehensive_demo.py
```

**Expected output**: ~60 lines showing 8 demo sections with JSON transformations, streaming stats, and error handling.

### Rust

```bash
# Option 1: Using cargo
cd ../rust
export DATAWEAVE_NATIVE_LIB=../build/native/nativeCompile
cargo run --example comprehensive_demo

# Option 2: Standalone compilation
rustc -L ../build/native/nativeCompile \
      --extern dataweave=../rust/target/debug/libdataweave.rlib \
      rust_comprehensive_demo.rs
./rust_comprehensive_demo
```

**Expected output**: ~70 lines including concurrent execution test with 10 threads.

### Go

```bash
# Set library path
export CGO_LDFLAGS="-L../build/native/nativeCompile -ldwlib"
export LD_LIBRARY_PATH="../build/native/nativeCompile:$LD_LIBRARY_PATH"  # Linux
export DYLD_LIBRARY_PATH="../build/native/nativeCompile:$DYLD_LIBRARY_PATH"  # macOS

# Run
cd ../go
go run ../demos/go_comprehensive_demo.go
```

**Expected output**: ~70 lines including goroutine safety test with 10 concurrent executions.

### C

```bash
# Compile
gcc -I../c/include -L../build/native/nativeCompile \
    -o c_demo c_comprehensive_demo.c -ldw -ldwlib

# Run (macOS)
DYLD_LIBRARY_PATH=../build/native/nativeCompile:$DYLD_LIBRARY_PATH ./c_demo

# Run (Linux)
LD_LIBRARY_PATH=../build/native/nativeCompile:$LD_LIBRARY_PATH ./c_demo
```

**Expected output**: ~65 lines including memory management demonstrations.

## Demo Highlights by Language

### Python: Most Feature-Rich

- **Bidirectional streaming**: Demonstrates input AND output streaming simultaneously
- **Format conversions**: JSON→CSV, CSV→JSON with headers
- **Context managers**: `with DataWeave() as dw:` idiom
- **Type hints**: InputValue for explicit MIME types and properties

**Best for**: Data engineers, ETL pipelines, format conversion workflows

### Rust: Safety-Focused

- **Concurrent execution**: 10 threads running transformations simultaneously
- **Type safety**: Compile-time guarantees via Send/Sync bounds
- **RAII patterns**: Automatic cleanup via Drop trait
- **Zero-copy**: Efficient memory usage with minimal allocations

**Best for**: Systems programming, performance-critical applications, embedded systems

### Go: Idiomatic Concurrency

- **Goroutine safety**: 10 concurrent goroutines with channel-based communication
- **Safe context passing**: Uses `cgo.Handle` (no pointer corruption)
- **Race detector validated**: Passes `go test -race` with zero warnings
- **Struct marshaling**: Go structs → DataWeave transformation

**Best for**: Microservices, cloud-native apps, high-concurrency backends

### C: Low-Level Control

- **Explicit memory management**: Manual `dw_free_*` calls
- **Callback-based streaming**: Function pointers for output chunks
- **NULL safety**: Handles NULL parameters gracefully
- **No dependencies**: Pure C with standard library only

**Best for**: Legacy system integration, embedded systems, performance-critical C code

## Performance Characteristics

All demos include a **streaming test** that processes 1000 records to demonstrate constant memory usage:

| Binding | Memory Pattern | Best For |
|---------|---------------|----------|
| Python | Constant (via callbacks) | General-purpose scripting |
| Rust | Constant (via iterators) | Zero-allocation streaming |
| Go | Constant (via channels) | Concurrent streaming |
| C | Constant (via callbacks) | Low-level control |

## Common Issues and Solutions

### Issue: "Cannot find library"

**Solution**:
```bash
# macOS
export DYLD_LIBRARY_PATH=../build/native/nativeCompile:$DYLD_LIBRARY_PATH

# Linux
export LD_LIBRARY_PATH=../build/native/nativeCompile:$LD_LIBRARY_PATH

# Windows
set PATH=..\build\native\nativeCompile;%PATH%
```

### Issue: "Module not found" (Python)

**Solution**: Install the wheel or add to PYTHONPATH:
```bash
export PYTHONPATH=../python/src:$PYTHONPATH
```

### Issue: CGO errors (Go)

**Solution**: Ensure CGO is enabled and flags are set:
```bash
export CGO_ENABLED=1
export CGO_LDFLAGS="-L../build/native/nativeCompile -ldwlib"
```

### Issue: Undefined symbols (C)

**Solution**: Link against both `dwlib` and the C wrapper:
```bash
gcc ... -ldw -ldwlib
```

## Customizing the Demos

Each demo is self-contained and can be modified to test specific scenarios:

1. **Change the dataset size**: Modify the loop count in streaming demos (default: 1000)
2. **Add custom transformations**: Replace scripts with your own DataWeave code
3. **Test error handling**: Uncomment error cases or add new ones
4. **Benchmark performance**: Add timing around `run()` calls

## Next Steps

After running the demos:

1. **Explore the READMEs**: Each language has detailed documentation in `native-lib/{lang}/README.md`
2. **Run the test suites**: See `native-lib/*/tests/` for comprehensive unit tests
3. **Check the examples**: Simple examples available in `native-lib/{lang}/examples/`
4. **Read the API docs**: Inline documentation in source files

## Feedback

If you find issues or have suggestions for the demos:

1. Check the main native-lib README for troubleshooting
2. Review the comparison report: `NATIVE_BINDINGS_COMPARISON.md`
3. Open an issue in the repository

---

**Demo Statistics**:
- Total lines of demo code: ~1,800
- Languages covered: 5 (Python, Rust, Go, C, + Node.js in examples/)
- Demo scenarios: 8 per language
- Execution modes tested: 3 (buffered, streaming, bidirectional)
