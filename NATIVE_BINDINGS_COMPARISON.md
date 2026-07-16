# Native Language Bindings Comparison Report

**Date**: 2026-06-26  
**Author**: Claude Code (claude-unleashed session)  
**Branches Compared**: `fix/rust-go-bindings`, `feat/native-lib-language-wrappers`  
**Target**: Merged best-of-both into `feat/native-bindings-merged` branch cut from `master`

## Executive Summary

Both branches add native language bindings (Rust, Go, and—in one branch—C) to complement the existing Node.js and Python bindings in the DataWeave CLI. After comprehensive analysis, the branches represent **independent implementations** that diverged from the same base commit (`de0207a`), with critical differences in safety, architecture, and completeness:

- **fix/rust-go-bindings**: Focuses on correctness and production readiness with critical memory safety fixes, race detector validation, and sophisticated Gradle integration. Adds Rust and Go bindings.
- **feat/native-lib-language-wrappers**: Provides comprehensive API surface and documentation with modular architecture. Adds Rust, Go, and C bindings, but contains a **critical memory safety bug** in Go bindings.

**Merge Decision**: Start from fix/rust-go-bindings (critical safety fixes), port feat's C bindings (unique), adopt feat's Rust modular architecture while preserving fix's soundness fixes, and carefully evaluate feat's Go API improvements after applying fix's safety patches.

---

## 1. Functionality Coverage

### Rust Bindings

| Feature | fix/rust-go-bindings | feat/native-lib-language-wrappers | Merged Choice |
|---------|---------------------|----------------------------------|---------------|
| **API Surface** | 5 execution modes: `run()`, `run_streaming()`, `run_transform()`, `run_callback()`, `run_input_output_callback()` | Same 5 modes | **Equal** |
| **Architecture** | Monolithic (2 files: lib.rs 586 lines, error.rs 41 lines) | Modular (5 files: lib.rs, error.rs, ffi.rs, result.rs, streaming.rs) | **feat** - Better separation of concerns |
| **Error Handling** | 9 manual error variants | 14 variants using `thiserror` crate, includes `DataWeaveScriptError` wrapper | **feat** - More ergonomic and comprehensive |
| **Soundness Fix** | ✅ `T: Sync` bound on `SendPtr<T>` (commit `0e87b19`) | ❌ Missing | **fix** - Critical for thread safety |
| **DataWeave APIs Exposed** | Full CLI surface: script execution, streaming I/O, context/properties | Same | **Equal** |

**Justification**: Use feat's modular architecture (easier maintenance, clearer FFI boundaries in ffi.rs) but apply fix's `SendPtr<T: Sync>` bound, which prevents unsound Send implementation for non-thread-safe types.

### Go Bindings

| Feature | fix/rust-go-bindings | feat/native-lib-language-wrappers | Merged Choice |
|---------|---------------------|----------------------------------|---------------|
| **API Surface** | 4 execution modes: `Run()`, `RunStreaming()`, `RunCallback()`, `RunTransform()` | Same 4 modes | **Equal** |
| **Lines of Code** | 470 lines (dataweave.go) | 904 lines (dataweave.go) | **feat** has richer API surface to evaluate |
| **Context Passing** | ✅ `cgo.Handle` (Go 1.17+) - checkptr-safe | ❌ `uintptr(ctxPtr)` → `unsafe.Pointer` - **VIOLATES Go checkptr** | **fix** - **CRITICAL SAFETY** |
| **Race Detector** | ✅ Passes `go test -race` (12/12 tests) | ❌ Fatal crashes with `-race` flag | **fix** - **PRODUCTION BLOCKER** |
| **Deadlock Protection** | ✅ Non-blocking channel send (`select/default`) | ❌ Not evident in code | **fix** - Prevents runtime hangs |
| **Thread Safety** | OS thread pinning (`runtime.LockOSThread()`) for GraalVM isolate thread affinity | Same approach | **Equal** |

**Justification**: fix/rust-go-bindings MUST be the foundation due to the checkptr violation in feat branch. The unsafe pointer cast in feat's `goWriteCallback` causes immediate crashes under race detector and violates Go's memory safety guarantees (commit `0e87b19` in fix branch replaces this with `cgo.Handle`). After adopting fix's safety infrastructure, evaluate feat's additional API surface (904 vs 470 lines suggests more convenience methods or examples).

### C Bindings

| Feature | fix/rust-go-bindings | feat/native-lib-language-wrappers | Merged Choice |
|---------|---------------------|----------------------------------|---------------|
| **Existence** | ❌ No C bindings | ✅ Complete C bindings (14 files) | **feat** - Only source |
| **API Surface** | N/A | 30+ functions: `dw_run()`, `dw_run_streaming()`, `dw_run_callback()`, `dw_run_transform()` | **feat** |
| **Build System** | N/A | Makefile + CMakeLists.txt, static/shared library targets | **feat** |
| **Test Coverage** | N/A | 10 tests (461 lines) in `tests/test_dataweave.c` | **feat** |
| **Documentation** | N/A | 503-line README with API reference | **feat** |
| **Memory Safety** | N/A | Opaque structs, explicit `dw_free_*` functions, documented ownership | **feat** |

**Justification**: No conflict—fix branch has zero C code. Port feat's C bindings wholesale, but review for Windows compatibility (fix branch addressed Windows symlink issues for Go/Rust that may apply to C build system).

---

## 2. Correctness (Build, Tests, Error Handling, Safety)

### Build Status

| Aspect | fix/rust-go-bindings | feat/native-lib-language-wrappers | Merged Choice |
|--------|---------------------|----------------------------------|---------------|
| **Gradle Integration** | ✅ Sophisticated: `goTest`, `goTestRace`, `rustTest` tasks; proper task inputs/outputs; Windows file-copy fallback | Basic task definitions (116 lines), no race detector tasks | **fix** - Production-ready |
| **Incremental Builds** | ✅ Task dependencies and up-to-date checks (commit `043da3e`) | Not evident | **fix** |
| **Windows Support** | ✅ Explicit symlink → file copy workaround (commit `6c2aa7e`) | Unclear | **fix** - Critical for cross-platform |
| **Race Detector Integration** | ✅ `goTestRace` Gradle task | ❌ Missing | **fix** - Caught the checkptr bug |

**Evidence**: fix branch commit `6c2aa7e` titled "comprehensive Go/Rust bindings audit fixes" added explicit task inputs/outputs and Windows compatibility. The `goTestRace` task discovered the critical checkptr violation that feat branch's 17 tests (run without `-race`) did not catch.

### Test Coverage

| Language | fix/rust-go-bindings | feat/native-lib-language-wrappers | Merged Choice |
|----------|---------------------|----------------------------------|---------------|
| **Rust** | 12 tests (317 lines) including 2 concurrent tests (20 threads, 10 streaming threads) | 17 tests (494 lines), NO concurrent tests | **fix tests + feat's additional scenarios** |
| **Go** | 12 tests including 2 concurrent tests (`TestRun_Concurrent`, `TestRunStreaming_Concurrent`) | 17 tests, NO concurrent tests | **fix tests + feat's additional scenarios** |
| **C** | N/A | 10 tests (461 lines) | **feat** (only source) |

**Analysis**: fix branch has FEWER tests (12 vs 17) but BETTER coverage—the concurrent tests caught the race conditions that feat's larger but serial test suite missed. Merged branch should combine both: use fix's concurrent test infrastructure and add feat's additional edge-case scenarios.

### Error Handling & FFI Safety

**Rust:**
- fix: Manual error types, basic coverage of FFI errors
- feat: `thiserror`-derived errors, more granular error types (14 variants), includes script-specific errors
- **Merged choice**: feat's error types (better UX) + fix's `SendPtr<T: Sync>` bound (soundness)

**Go:**
- fix: Safe context passing via `cgo.Handle`, prevents use-after-free
- feat: Unsafe pointer cast, **memory corruption risk**
- **Merged choice**: fix's approach (non-negotiable safety requirement)

**C:**
- feat: Explicit ownership (`dw_free_*` functions), opaque structs prevent ABI issues
- **Merged choice**: feat (only implementation)

---

## 3. Feature Completeness (Packaging, Examples, Docs, CI)

### Packaging

| Aspect | fix/rust-go-bindings | feat/native-lib-language-wrappers | Merged Choice |
|--------|---------------------|----------------------------------|---------------|
| **Rust** | Cargo.toml with crates.io metadata (version 0.1.0) | Same structure | **Equal** (neither published yet) |
| **Go** | `go.mod` with module path `github.com/mulesoft-labs/data-weave-native/go` | Same | **Equal** |
| **C** | N/A | Makefile with `install` target to PREFIX | **feat** |

### Examples

| Language | fix/rust-go-bindings | feat/native-lib-language-wrappers | Merged Choice |
|----------|---------------------|----------------------------------|---------------|
| **Rust** | `examples/basic.rs` (99 lines) | Similar examples | **Equal** |
| **Go** | `example/main.go` (120 lines) | Similar | **Equal** |
| **C** | N/A | Working examples in README | **feat** |

### Documentation

| Document Type | fix/rust-go-bindings | feat/native-lib-language-wrappers | Merged Choice |
|---------------|---------------------|----------------------------------|---------------|
| **Forensic/Debugging** | FIX-SUMMARY.md (257 lines), SECOND-REVIEW-FINDINGS.md (439 lines), CLI-GAPS-AND-OPPORTUNITIES.md (1127 lines) | None | **fix** - Valuable for maintainers |
| **Architectural** | None | FFI_CONTRACT.md (255 lines), LANGUAGE_WRAPPERS_SUMMARY.md (362 lines), IMPLEMENTATION_NOTES.md, QUICK_START.md | **feat** - Essential for onboarding |
| **Rust README** | 236 lines | 437 lines | **feat** - More comprehensive |
| **Go README** | 239 lines | 497 lines | **feat** - More comprehensive |
| **C README** | N/A | 503 lines | **feat** (only source) |

**Merged strategy**: Combine both documentation sets—fix's forensic docs explain WHY certain decisions were made (critical for debugging), feat's architectural docs explain HOW to use the bindings (critical for adoption).

### CI Integration

| Aspect | fix/rust-go-bindings | feat/native-lib-language-wrappers | Merged Choice |
|--------|---------------------|----------------------------------|---------------|
| **Status** | ❌ Not integrated (manual build) | ❌ Not integrated (manual build) | **TODO: Add to CI** |
| **Race Detector** | ✅ Gradle task exists (`goTestRace`) | ❌ Missing | **fix** - Must be in CI |
| **Windows Matrix** | Addressed in Gradle (file copy fallback) | Unknown | **fix** |

**Recommendation**: Neither branch integrated Rust/Go/C into `.github/workflows/`, but fix branch has the Gradle infrastructure ready. Add CI jobs that invoke fix's `rustTest`, `goTest`, `goTestRace` tasks.

---

## 4. Code Quality and Idiomatic Style

### Rust

**fix/rust-go-bindings:**
- ✅ Idiomatic RAII via Drop trait
- ✅ Safe abstractions (`SendPtr` wrapper for thread safety)
- ❌ Monolithic lib.rs (586 lines)
- ✅ Minimal unsafe blocks (isolated to FFI layer)

**feat/native-lib-language-wrappers:**
- ✅ Modular architecture (ffi.rs, result.rs, streaming.rs)
- ✅ `thiserror` for ergonomic error handling
- ✅ Comprehensive result types
- ❌ Missing `T: Sync` bound (soundness hole)

**Merged approach**: feat's structure + fix's safety bounds

### Go

**fix/rust-go-bindings:**
- ✅ Idiomatic error handling (`error` return values)
- ✅ Proper CGO patterns (`cgo.Handle`, thread pinning)
- ✅ Channel-based streaming (idiomatic concurrency)
- ✅ Race detector validated

**feat/native-lib-language-wrappers:**
- ✅ More comprehensive API surface (904 vs 470 lines)
- ❌ **Unsafe pointer cast violates Go memory safety**
- ❌ No race detector validation

**Merged approach**: fix's safety foundation, then evaluate feat's API additions

### C

**feat/native-lib-language-wrappers** (only implementation):
- ✅ Opaque structs (ABI stability)
- ✅ Clear ownership semantics
- ✅ Const-correct function signatures
- ✅ Proper header guards

---

## 5. Per-Binding Merge Decisions

### Rust: feat architecture + fix safety

1. **Base structure**: Use feat's 5-file modular layout
   - `src/lib.rs` - Public API surface
   - `src/ffi.rs` - Unsafe FFI layer (isolates unsafe code)
   - `src/result.rs` - Result type definitions
   - `src/streaming.rs` - Streaming abstractions
   - `src/error.rs` - Error types (with `thiserror`)

2. **Safety patches from fix**:
   - Apply `T: Sync` bound to `SendPtr<T>` (fix:lib.rs:129)
   - Verify all Send/Sync implementations are sound

3. **Tests**: Merge both suites
   - Keep fix's 2 concurrent tests (`TestRun_Concurrent` equivalent)
   - Add feat's additional edge-case scenarios (17 - 12 = 5 unique tests)

4. **Documentation**: Combine both
   - feat's README (437 lines, more user-focused)
   - fix's FIX-SUMMARY.md (maintainer context)

### Go: fix foundation + feat API evaluation

1. **Base implementation**: Use fix/rust-go-bindings wholesale
   - Critical: `cgo.Handle` for context passing (fix:streaming_callbacks.go)
   - Non-blocking channel sends (deadlock prevention)
   - Race detector validated

2. **Evaluate feat additions**: After adopting fix's safety layer, review feat's additional 434 lines (904 - 470) for:
   - Convenience methods worth porting
   - API sugar that doesn't compromise safety
   - Better examples or documentation

3. **Tests**: fix's 12 (including concurrent) + feat's 5 unique scenarios

4. **Documentation**: feat's 497-line README (more comprehensive) + fix's FIX-SUMMARY.md

### C: feat wholesale (no conflict)

1. **Port entire feat/native-lib-language-wrappers:native-lib/c/** directory
2. **Verify Windows support**: Apply fix's learnings about Windows symlink issues to C Makefile/CMake
3. **No modifications needed**: feat is the only C implementation

### Build System: fix Gradle + feat targets

1. **Use fix's Gradle integration**:
   - `rustTest`, `goTest`, `goTestRace` tasks
   - Proper task inputs/outputs for incremental builds
   - Windows file-copy fallback

2. **Add C tasks**:
   - `cTest` - run C test suite
   - `stageNativeLibC` - copy dwlib.* to C directory
   - `buildCLibrary` - invoke Makefile

3. **Extend CI**: Add Rust/Go/C to `.github/workflows/main.yml` matrix

---

## 6. Critical Issues Requiring Resolution

### BLOCKER: Go checkptr violation (feat branch)

**Issue**: `feat/native-lib-language-wrappers:native-lib/go/dataweave.go` casts `uintptr` to `unsafe.Pointer` to pass context through CGO callbacks, violating Go's checkptr rules.

**Evidence**:
```go
// feat branch - UNSAFE
export "C" goWriteCallback(chunk *C.char, ctx unsafe.Pointer) {
    ctxPtr := uintptr(ctx)  // Store as integer
    // Later: unsafe.Pointer(ctxPtr)  // VIOLATION: pointer recreated from integer
}
```

**Impact**: Immediate crashes with `go test -race`, memory corruption risk in production

**Fix** (from fix branch commit `0e87b19`):
```go
// fix branch - SAFE
import "runtime/cgo"

handle := cgo.NewHandle(callbackContext)
defer handle.Delete()

export "C" goWriteCallback(chunk *C.char, handleValue C.uintptr_t) {
    ctx := cgo.Handle(handleValue).Value().(CallbackContext)  // SAFE
}
```

**Resolution**: Use fix branch's `cgo.Handle` approach (Go 1.17+ required, acceptable given Python bindings already require recent runtime).

### IMPORTANT: Rust SendPtr soundness (fix branch)

**Issue**: `SendPtr<T>` in feat branch implements `Send` without requiring `T: Sync`, allowing thread-unsafe types to be sent across threads.

**Fix** (from fix branch):
```rust
// fix branch adds T: Sync bound
unsafe impl<T: Sync> Send for SendPtr<T> {}
```

**Resolution**: Apply fix's bound when porting feat's modular architecture.

---

## 7. Merge Implementation Plan

### Phase 1: Foundation (fix/rust-go-bindings)
1. Merge fix/rust-go-bindings into feat/native-bindings-merged
2. Verify all tests pass (`rustTest`, `goTest`, `goTestRace`)
3. Commit: "Merge fix/rust-go-bindings: safe Rust/Go bindings with race detector validation"

### Phase 2: C Bindings (feat branch)
1. Cherry-pick feat's `native-lib/c/` directory
2. Add C Gradle tasks (cTest, stageNativeLibC)
3. Verify builds on Linux/macOS (Windows if possible)
4. Commit: "Add C bindings from feat/native-lib-language-wrappers"

### Phase 3: Rust Modularization (feat architecture)
1. Refactor Rust bindings to feat's 5-file structure
2. Port `thiserror` error types
3. Preserve fix's `SendPtr<T: Sync>` bound
4. Merge test suites (12 + 5 unique)
5. Commit: "Refactor Rust bindings: modular architecture with preserved safety bounds"

### Phase 4: Go API Evaluation (feat additions)
1. Review feat's additional Go code (434 lines)
2. Port safe, valuable additions (convenience methods, better examples)
3. Merge test suites (12 + 5 unique)
4. Commit: "Enhance Go bindings API surface from feat branch"

### Phase 5: Documentation & CI
1. Merge documentation (fix's forensics + feat's architecture)
2. Add CI jobs for Rust/Go/C (including `goTestRace`)
3. Update top-level native-lib/README.md
4. Commit: "Complete documentation and CI integration for native bindings"

---

## 8. Testing Requirements for Merged Branch

Before declaring success, the merged branch MUST:

1. ✅ **Build successfully**:
   - `./gradlew nativeCompile` (GraalVM shared library)
   - `./gradlew rustTest` (Rust bindings)
   - `./gradlew goTest` (Go bindings)
   - `./gradlew cTest` (C bindings)

2. ✅ **Pass race detector**:
   - `./gradlew goTestRace` (CRITICAL: validates checkptr fix)

3. ✅ **Concurrent execution**:
   - Rust: 20-thread concurrent execution test
   - Go: 20-goroutine concurrent execution test

4. ✅ **Cross-platform** (if CI available):
   - Linux (ubuntu-latest)
   - macOS (macos-latest)
   - Windows (windows-latest) - at minimum, Rust/Go should build

5. ✅ **Memory leak check** (manual):
   - Long-running streaming test (valgrind for C, Go's `-race` for Go, Miri for Rust if practical)

---

## 9. Conclusion

The merged `feat/native-bindings-merged` branch combines:

- **fix/rust-go-bindings**: Critical memory safety fixes, race detector validation, production-ready Gradle integration
- **feat/native-lib-language-wrappers**: Modular Rust architecture, comprehensive documentation, unique C bindings

**Key decisions justified**:
- Go bindings use fix's `cgo.Handle` (eliminates checkptr violation)
- Rust bindings use feat's structure + fix's `SendPtr<T: Sync>` (soundness)
- C bindings from feat (only implementation)
- Build system from fix (race detector, Windows support, incremental builds)
- Documentation merged (forensics + architecture)

**Production readiness**: After merge and CI integration, all three bindings (Rust, Go, C) will match the quality bar of existing Node.js and Python bindings—feature-complete, race-validated, cross-platform, with comprehensive tests and documentation.

---

## Appendix: Commit Evidence

### fix/rust-go-bindings key commits
- `0e87b19` - "fix(native-lib): resolve checkptr violation and add race detector tests"
- `6c2aa7e` - "fix(native-lib): comprehensive Go/Rust bindings audit fixes and improvements"
- `043da3e` - "fix(native-lib): add explicit task inputs/outputs for Gradle dependency resolution"

### feat/native-lib-language-wrappers key commit
- `de0207a` - Common ancestor: "W-20884161: Add native DataWeave shared library with FFI bindings"

### Branch stats
- fix/rust-go-bindings: +7,790 lines, 30 files changed
- feat/native-lib-language-wrappers: +49,791 lines, 44 files changed
- Overlap: Rust, Go (different implementations)
- Unique to feat: C bindings (14 files)
