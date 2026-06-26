# DataWeave Rust Bindings - Implementation Notes

This document describes the implementation details of the Rust bindings for the DataWeave native library.

## Architecture Overview

```
┌─────────────────┐
│  User Code      │  (Safe Rust API)
├─────────────────┤
│  lib.rs         │  Public API, type conversions, RAII
├─────────────────┤
│  streaming.rs   │  Background threads, Queue-based streaming
├─────────────────┤
│  result.rs      │  Result types, JSON parsing
├─────────────────┤
│  error.rs       │  Error types (thiserror)
├─────────────────┤
│  ffi.rs         │  Raw FFI bindings (unsafe)
├─────────────────┤
│  dwlib.dylib    │  GraalVM native library
└─────────────────┘
```

## Key Design Decisions

### 1. Safe Abstractions Over Unsafe FFI

All unsafe FFI calls are encapsulated in safe public APIs:

- `DataWeave` struct provides safe methods
- FFI details are hidden in `ffi.rs`
- Unsafe blocks are minimized and localized
- Resource cleanup is automatic via RAII (Drop trait)

### 2. RAII Pattern for Resource Management

The `DataWeave` struct implements `Drop` to automatically clean up GraalVM isolates:

```rust
impl Drop for DataWeave {
    fn drop(&mut self) {
        if !self.thread.is_null() {
            unsafe {
                let _ = (self.lib.graal_tear_down_isolate)(self.thread);
            }
        }
    }
}
```

This ensures resources are always released, even if panics occur.

### 3. Streaming Implementation

Streaming is implemented using background threads and synchronization:

#### Output Streaming (`run_streaming`)

1. Spawn a background thread
2. Attach thread to GraalVM isolate
3. Register write callback that pushes chunks to a queue
4. Main thread consumes from queue
5. Background thread detaches and signals completion
6. Metadata is captured and returned

#### Bidirectional Streaming (`run_transform`)

1. Spawn a background thread
2. Attach thread to GraalVM isolate
3. Register read callback that pulls from input iterator
4. Register write callback that pushes to output queue
5. Read callback runs on native background thread
6. Write callback runs on calling thread
7. Both threads coordinate via mutex-protected data structures

### 4. Type Conversions

Auto-conversion trait `ToInputValue` provides ergonomic input binding:

```rust
pub trait ToInputValue {
    fn to_input_value(&self) -> Result<InputValue, DataWeaveError>;
}

impl ToInputValue for i32 {
    fn to_input_value(&self) -> Result<InputValue, DataWeaveError> {
        let json = serde_json::to_string(self)?;
        Ok(InputValue::new(json.as_bytes().to_vec(), "application/json"))
    }
}
```

This allows natural Rust syntax:

```rust
inputs.insert("num".to_string(), 42.to_input_value()?);
```

### 5. Error Handling

All errors are strongly typed using `thiserror`:

```rust
#[derive(Error, Debug)]
pub enum DataWeaveError {
    #[error("DataWeave native library not found")]
    LibraryNotFound,
    
    #[error("Failed to create GraalVM isolate (error code: {0})")]
    IsolateCreation(i32),
    
    // ... more variants
}
```

This provides:
- Clear error messages
- Type-safe error handling
- Automatic `Error` trait implementation
- Easy error propagation with `?`

## FFI Bindings

### Function Signatures

The `ffi.rs` module declares extern "C" function signatures matching the native library:

```rust
pub graal_create_isolate: unsafe extern "C" fn(
    params: *mut c_void,
    isolate: *mut *mut GraalIsolate,
    thread: *mut *mut GraalIsolateThread,
) -> c_int,
```

### Callback Trampolines

Rust closures are converted to C function pointers using trampolines:

```rust
unsafe extern "C" fn trampoline<F>(
    ctx: *mut c_void,
    buffer: *const c_char,
    length: c_int,
) -> c_int
where
    F: FnMut(&[u8]) -> i32,
{
    let callback = &mut *(ctx as *mut F);
    let data = std::slice::from_raw_parts(buffer as *const u8, length as usize);
    callback(data)
}
```

This allows passing Rust closures to C callbacks safely.

### Memory Management

String memory returned by native functions is managed carefully:

```rust
unsafe fn decode_and_free(&self, ptr: *mut c_char) -> Result<String, DataWeaveError> {
    if ptr.is_null() {
        return Err(DataWeaveError::NullResponse);
    }

    let c_str = CStr::from_ptr(ptr);
    let result = c_str.to_str()?.to_string();

    // Free native memory
    (self.lib.free_cstring)(self.thread, ptr);

    Ok(result)
}
```

## Threading Model

### Thread Attachment

Background threads must attach to the GraalVM isolate:

```rust
let mut worker_thread: *mut GraalIsolateThread = ptr::null_mut();

let rc = (lib.graal_attach_thread)(
    isolate,
    &mut worker_thread as *mut *mut GraalIsolateThread,
);

// ... use worker_thread ...

(lib.graal_detach_thread)(worker_thread);
```

### Synchronization

- `Mutex` protects shared data between threads
- `Arc` provides thread-safe reference counting
- `Cell`/`RefCell` allow interior mutability for closures

Example from streaming:

```rust
struct StreamContext {
    chunks: Mutex<Vec<Vec<u8>>>,
    metadata: Mutex<Option<String>>,
}

let context = Arc::new(StreamContext { ... });
let context_clone = Arc::clone(&context);

// Pass to background thread
thread::spawn(move || {
    context_clone.chunks.lock().unwrap().push(data);
});
```

## Testing Strategy

### Test Coverage

All Python test cases are ported to Rust:

1. `test_basic` - Simple arithmetic
2. `test_with_inputs` - Input bindings
3. `test_raii_pattern` - Resource cleanup
4. `test_encoding` - UTF-16 XML → CSV
5. `test_auto_conversion` - Type conversions
6. `test_callback_output_basic` - Callback streaming
7. `test_callback_output_with_inputs` - Callback with inputs
8. `test_callback_input_output` - Bidirectional callbacks
9. `test_callback_input_output_large` - Large data handling
10. `test_run_streaming_basic` - Output streaming
11. `test_run_streaming_large` - Multiple chunks
12. `test_run_streaming_error` - Error handling
13. `test_run_streaming_with_inputs` - Streaming with inputs
14. `test_run_transform_basic` - Bidirectional streaming
15. `test_run_transform_large` - Large chunked input
16. `test_run_transform_with_file` - File I/O

### Test Execution

Tests require the native library to be built:

```bash
# Build native library
cd ../..
./gradlew nativeCompile

# Run tests
cd native-lib/rust
export DATAWEAVE_NATIVE_LIB=../../build/native/nativeCompile/dwlib.dylib
cargo test
```

## Performance Considerations

### Memory Efficiency

- **Buffered execution**: Holds entire output in memory (simple but memory-intensive)
- **Streaming**: Constant memory overhead regardless of output size
- **Chunking**: 4KB default chunk size balances throughput and latency

### Zero-Copy Where Possible

- Callbacks receive raw byte slices (`&[u8]`) without copying
- Streaming uses `Vec<u8>` to minimize allocations
- Base64 encoding/decoding happens once per input/output

### Thread Overhead

- Background threads are spawned per streaming operation
- Thread creation overhead is amortized over large operations
- For small outputs, buffered execution may be faster

## Safety Guarantees

### Unsafe Usage

Unsafe code is limited to:

1. FFI function calls (unavoidable)
2. Raw pointer dereferencing in callbacks (necessary for C interop)
3. `ctypes::string_at` for reading C strings
4. `std::slice::from_raw_parts` for callback data

All unsafe blocks are:
- Minimized in scope
- Carefully reviewed
- Documented with safety comments

### Thread Safety

- `DataWeave` is `Send + Sync` (can be shared across threads)
- `NativeLib` is `Send + Sync` (function pointers are thread-safe)
- Isolate handles are thread-local (attached threads are separate)

### Resource Leaks

Prevented by:
- RAII pattern (Drop trait)
- Automatic string deallocation (`free_cstring`)
- Thread detachment in all code paths (including panics via join())

## Comparison with Python Implementation

### Similarities

- Identical API surface (run, run_streaming, run_transform, etc.)
- Same input/output formats (JSON, base64)
- Same streaming model (chunks + metadata)
- Same threading model (background workers with callbacks)

### Differences

| Aspect | Python | Rust |
|--------|--------|------|
| Memory safety | Runtime checks | Compile-time checks |
| Resource cleanup | `__del__` + atexit | RAII (Drop trait) |
| Error handling | Exceptions | Result<T, E> |
| Type safety | Duck typing | Strong static typing |
| Concurrency | GIL limitations | True parallelism |
| Closure capture | Mutable by default | Explicit Rc<Cell<T>> for mutation |

### Performance

Rust should be comparable or faster than Python due to:
- No interpreter overhead
- More efficient memory layout
- True multi-threading (no GIL)
- Zero-cost abstractions

## Future Improvements

1. **Async/Await Support**: Use `tokio` for async streaming
2. **Builder Pattern**: More ergonomic API construction
3. **Error Context**: More detailed error messages with context
4. **Custom Allocators**: Reduce allocation overhead
5. **Benchmark Suite**: Performance regression tests
6. **Type-Safe Builders**: For input construction
7. **Streaming Iterators**: Implement proper `Stream` trait when stabilized

## Dependencies

- `libloading` - Dynamic library loading
- `base64` - Base64 encoding/decoding
- `serde` + `serde_json` - JSON serialization
- `thiserror` - Derive Error trait
- `once_cell` - (Future use for global state)

All dependencies are:
- Widely used in Rust ecosystem
- Well-maintained
- Minimal transitive dependencies
- Pure Rust (no C dependencies)

## Contributing

When contributing, ensure:

1. All tests pass: `cargo test`
2. Code is formatted: `cargo fmt`
3. No clippy warnings: `cargo clippy -- -D warnings`
4. Documentation is updated
5. Examples compile and run

## References

- [FFI Contract](../FFI_CONTRACT.md) - Native library specification
- [Python Implementation](../python/) - Reference implementation
- [Rust FFI Guide](https://doc.rust-lang.org/nomicon/ffi.html)
- [GraalVM Native Image](https://www.graalvm.org/latest/reference-manual/native-image/)
