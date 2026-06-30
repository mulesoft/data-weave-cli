# DataWeave Native Bindings - Fix Implementation Plan

**Date:** 2026-06-24  
**Target:** Address P0 and P1 priority bugs identified in review  
**Estimated Time:** 1 hour

---

## Phase 1: Critical Fixes (P0) - 15 minutes

### Task 1.1: Fix GraalVM setrlimit Warning (BUG-1)
**File:** `native-lib/build.gradle`  
**Line:** 76  
**Change:** Add build argument to suppress the warning

**Before:**
```groovy
buildArgs.add("-H:+ReportExceptionStackTraces")
```

**After:**
```groovy
buildArgs.add("-H:+ReportExceptionStackTraces")
buildArgs.add("-H:-SetFileDescriptorLimit")  // Suppress macOS setrlimit warning
```

**Test:**
```bash
./gradlew :native-lib:nativeCompile
cd native-lib/go && go run examples/simple_demo.go
# Should not print "setrlimit to increase file descriptor limit failed"
```

---

### Task 1.2: Fix Go EOF Error Handling (BUG-4)
**File:** `native-lib/go/streaming_callbacks.go`  
**Lines:** 45-49  
**Change:** Use `errors.Is` instead of string comparison

**Before:**
```go
if err != nil {
	// io.EOF signals normal end-of-stream
	if err.Error() == "EOF" {
		return 0
	}
	return -1
}
```

**After:**
```go
import "errors"
import "io"

if err != nil {
	// io.EOF signals normal end-of-stream
	if errors.Is(err, io.EOF) {
		return 0
	}
	return -1
}
```

**Also update imports at the top of the file:**
```go
import "C"
import (
	"errors"
	"io"
	"unsafe"
)
```

**Test:**
```bash
cd native-lib/go && go test -v -run TestRunTransform_InputError
```

---

## Phase 2: Documentation Fixes (P1) - 30 minutes

### Task 2.1: Document Rust SendPtr Safety (BUG-3)
**File:** `native-lib/rust/src/lib.rs`  
**Lines:** 96-104  
**Change:** Add comprehensive safety documentation

**Before:**
```rust
/// Wraps a raw pointer so it can be moved into a spawned thread.
/// SAFETY: the caller is responsible for ensuring the pointer remains valid for the
/// thread's lifetime. We use this to ferry callback-context pointers across threads.
struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
impl<T> SendPtr<T> {
    fn as_raw(&self) -> *mut T {
        self.0
    }
}
```

**After:**
```rust
/// Wraps a raw pointer to allow transfer across thread boundaries.
///
/// # Safety Invariants
///
/// The caller MUST ensure:
/// 1. **Lifetime:** The pointer remains valid for the spawned thread's entire lifetime
/// 2. **Exclusive Access:** The pointed-to data is not accessed concurrently from other threads
/// 3. **Proper Cleanup:** The pointer is freed on the thread that received it, after FFI completes
///
/// This type is used to pass callback context pointers from the main thread to the FFI
/// worker thread. The context Box is created before spawning and freed after the FFI
/// call completes, ensuring validity throughout:
///
/// ```text
/// Main Thread              FFI Worker Thread
/// -----------              -----------------
/// Box::new(ctx)
///   ↓
/// SendPtr(ptr) -------→    Receives ptr
///   ↓                      Uses ptr in callbacks
/// spawn()                    ↓
///   ↓                      Box::from_raw(ptr)  // Frees
///   ↓                      Thread exits
/// join()
/// ```
///
/// # Why This is Sound
///
/// - The main thread creates the Box and immediately transfers ownership to SendPtr
/// - SendPtr is moved (not copied) to the worker thread
/// - Only the worker thread dereferences the pointer
/// - The worker thread frees the Box after FFI completes
/// - No data races possible because ownership is exclusive at each step
struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
impl<T> SendPtr<T> {
    fn as_raw(&self) -> *mut T {
        self.0
    }
}
```

**Test:** Code review (no functional change)

---

### Task 2.2: Document Go Callback Context Threading (BUG-6)
**File:** `native-lib/go/dataweave.go`  
**Lines:** 252-258  
**Change:** Add threading model documentation

**Before:**
```go
// callbackContext holds state shared between Go and the CGO callback.
type callbackContext struct {
	chunkCh chan []byte
	reader  io.Reader
	mu      sync.Mutex
}
```

**After:**
```go
// callbackContext holds state shared between Go and the CGO callback.
//
// # Threading Model
//
// The native library (GraalVM) guarantees that callbacks are invoked sequentially
// on a single OS thread per script execution. This means:
//
//   - writeCallbackBridge is called sequentially (never concurrently)
//   - readCallbackBridge is called sequentially (never concurrently)
//   - No mutex needed for chunkCh writes (sent from callback thread)
//   - Mutex protects reader in case of future concurrent read callbacks
//
// The context is:
//   1. Created on the main goroutine
//   2. Registered in the global map (thread-safe via contextMu)
//   3. Passed to the FFI worker goroutine via an integer handle
//   4. Accessed from the native callback thread via lookupContext()
//   5. Unregistered after the FFI call completes
//
// # Memory Safety
//
// The handle-based lookup pattern is safe because:
//   - Handles are integers (uintptr), not Go pointers
//   - The GC cannot move integers or map entries
//   - The context remains valid until unregisterContext() is called
//   - The FFI call completes before unregisterContext() is called
type callbackContext struct {
	chunkCh chan []byte      // Written by callback thread, read by consumer goroutine
	reader  io.Reader        // Read by callback thread (mutex-protected for future-proofing)
	mu      sync.Mutex       // Protects reader access
}
```

**Test:** Code review (no functional change)

---

### Task 2.3: Document Go Handle Pattern Safety (BUG-2)
**File:** `native-lib/go/streaming_callbacks.go`  
**Lines:** 10-22  
**Change:** Add safety comment

**Before:**
```go
//export writeCallbackBridge
func writeCallbackBridge(ctxPtr unsafe.Pointer, buf *C.char, length C.int) C.int {
	handle := uintptr(ctxPtr)
	ctx := lookupContext(handle)
	if ctx == nil {
		return -1
	}

	// Copy bytes from C buffer to Go slice before sending
	goBytes := C.GoBytes(unsafe.Pointer(buf), length)

	ctx.chunkCh <- goBytes
	return 0
}
```

**After:**
```go
//export writeCallbackBridge
func writeCallbackBridge(ctxPtr unsafe.Pointer, buf *C.char, length C.int) C.int {
	// Safe: ctxPtr is an integer handle (uintptr), not a Go pointer.
	// The GC cannot move integers, so this conversion is sound.
	handle := uintptr(ctxPtr)
	ctx := lookupContext(handle)
	if ctx == nil {
		return -1
	}

	// Copy bytes from C buffer to Go slice before sending
	goBytes := C.GoBytes(unsafe.Pointer(buf), length)

	ctx.chunkCh <- goBytes
	return 0
}
```

**Similarly for `readCallbackBridge` at line 25:**
```go
//export readCallbackBridge
func readCallbackBridge(ctxPtr unsafe.Pointer, buf *C.char, bufSize C.int) C.int {
	// Safe: ctxPtr is an integer handle (uintptr), not a Go pointer.
	handle := uintptr(ctxPtr)
	ctx := lookupContext(handle)
	if ctx == nil {
		return -1
	}
	// ... rest of function
}
```

**Test:** Code review (no functional change)

---

## Phase 3: Testing (15 minutes)

### Task 3.1: Verify All Tests Pass
```bash
# Clean build
./gradlew clean

# Rebuild native library with fixes
./gradlew :native-lib:nativeCompile

# Run Go tests
./gradlew :native-lib:goTest

# Run Rust tests
./gradlew :native-lib:rustTest

# Run demo programs
cd native-lib/go
go run examples/simple_demo.go
go run examples/streaming_demo.go

# If cargo is available
cd ../rust
cargo run --example simple_demo
cargo run --example streaming_demo
```

**Expected:** All tests pass, no setrlimit warning in output.

---

### Task 3.2: Verify Documentation Builds
```bash
# Go
cd native-lib/go
go doc -all > /tmp/go-docs.txt
grep -i "threading model" /tmp/go-docs.txt

# Rust
cd native-lib/rust
cargo doc --no-deps
```

---

## Phase 4: Git Commit (5 minutes)

```bash
git add native-lib/build.gradle
git add native-lib/go/streaming_callbacks.go
git add native-lib/go/dataweave.go
git add native-lib/rust/src/lib.rs

git commit -m "fix(native-lib): address P0/P1 bugs in Go and Rust bindings

- Suppress GraalVM setrlimit warning on macOS (BUG-1)
- Fix Go EOF error handling to use errors.Is (BUG-4)
- Document Rust SendPtr safety invariants (BUG-3)
- Document Go callback context threading model (BUG-6)
- Add safety comments to Go callback bridge (BUG-2)

All tests pass. No functional changes except EOF handling fix."
```

---

## Verification Checklist

- [ ] `native-lib/build.gradle` contains `-H:-SetFileDescriptorLimit`
- [ ] `native-lib/go/streaming_callbacks.go` uses `errors.Is(err, io.EOF)`
- [ ] `native-lib/go/streaming_callbacks.go` imports `errors` and `io`
- [ ] `native-lib/rust/src/lib.rs` has comprehensive `SendPtr` safety docs
- [ ] `native-lib/go/dataweave.go` has threading model docs for `callbackContext`
- [ ] `native-lib/go/streaming_callbacks.go` has safety comments in bridge functions
- [ ] All Go tests pass (`go test -v`)
- [ ] All Rust tests pass (if cargo available)
- [ ] Demo programs execute without setrlimit warning
- [ ] Demo programs produce correct output

---

## Post-Fix Testing Script

Save as `test-fixes.sh`:

```bash
#!/bin/bash
set -e

echo "=== Testing Native Library Bindings Fixes ==="

echo -e "\n1. Clean and rebuild native library..."
./gradlew clean :native-lib:nativeCompile

echo -e "\n2. Running Go tests..."
cd native-lib/go
go test -v
echo "✅ Go tests passed"

echo -e "\n3. Running Go simple demo..."
OUTPUT=$(go run examples/simple_demo.go 2>&1)
if echo "$OUTPUT" | grep -q "setrlimit to increase file descriptor limit failed"; then
    echo "❌ FAIL: setrlimit warning still present"
    exit 1
else
    echo "✅ No setrlimit warning"
fi

if echo "$OUTPUT" | grep -q "Demo complete!"; then
    echo "✅ Simple demo completed successfully"
else
    echo "❌ FAIL: Simple demo did not complete"
    exit 1
fi

echo -e "\n4. Running Go streaming demo..."
OUTPUT=$(go run examples/streaming_demo.go 2>&1)
if echo "$OUTPUT" | grep -q "Script error (expected)"; then
    echo "✅ Streaming demo completed successfully"
else
    echo "❌ FAIL: Streaming demo did not complete"
    exit 1
fi

echo -e "\n5. Running Rust tests (if cargo available)..."
cd ../rust
if command -v cargo &> /dev/null; then
    cargo test
    echo "✅ Rust tests passed"
    
    cargo run --example simple_demo
    echo "✅ Rust simple demo passed"
    
    cargo run --example streaming_demo
    echo "✅ Rust streaming demo passed"
else
    echo "⚠️  Skipping Rust tests (cargo not found)"
fi

echo -e "\n=== All Tests Passed! ==="
```

Make executable:
```bash
chmod +x test-fixes.sh
./test-fixes.sh
```

---

## Success Criteria

All criteria must be met:

1. ✅ Native library builds without errors
2. ✅ Go tests pass (10/10)
3. ✅ Rust tests pass (10/10)
4. ✅ No "setrlimit" warning in demo output
5. ✅ Demo programs produce correct output
6. ✅ Code review confirms documentation improvements
7. ✅ Git commit created with all changes

---

## Rollback Plan

If any tests fail:

```bash
# Revert changes
git checkout HEAD -- native-lib/build.gradle
git checkout HEAD -- native-lib/go/streaming_callbacks.go
git checkout HEAD -- native-lib/go/dataweave.go
git checkout HEAD -- native-lib/rust/src/lib.rs

# Rebuild
./gradlew clean :native-lib:nativeCompile

# Verify rollback
cd native-lib/go && go test -v
```

---

## Notes

- The setrlimit fix only affects build output; existing binaries will still show the warning
- The EOF fix is backward compatible (errors.Is works with wrapped errors)
- Documentation changes have no functional impact
- All changes are low-risk and non-breaking
