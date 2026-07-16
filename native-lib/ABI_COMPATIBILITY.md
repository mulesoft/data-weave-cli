# ABI Compatibility Policy

This document defines the **Application Binary Interface (ABI)** compatibility guarantees for the DataWeave native library and language bindings.

## Versioning Scheme

All DataWeave native bindings follow **Semantic Versioning 2.0.0**:

```
MAJOR.MINOR.PATCH (e.g., 1.2.3)
```

- **MAJOR** (X.0.0): Breaking ABI changes, recompilation required
- **MINOR** (1.X.0): Backward-compatible additions, no recompilation required
- **PATCH** (1.0.X): Bug fixes, no API/ABI changes

### Version Source of Truth

The canonical version is defined in `gradle.properties`:
```properties
nativeBindingsVersion=1.0.0
```

All language bindings (Python, Node.js, Go, Rust, C) share this version number.

## C API Compatibility

The C API (`dwlib.h`) is the **stable ABI boundary** for all language bindings.

### Guarantees (within MAJOR version)

✅ **Stable ABI Elements:**
- Function signatures (argument types, return types)
- Struct layouts (field order, sizes, alignment)
- Enum values (numeric values of constants)
- SONAME (shared library version, e.g., `libdwlib.so.1`)

### Breaking Changes (MAJOR version bump required)

❌ **ABI-Breaking Changes:**
- Changing function signatures (adding/removing/reordering parameters)
- Changing struct layouts (adding/removing/reordering fields)
- Changing enum numeric values
- Removing public functions
- Changing calling conventions

### Backward-Compatible Additions (MINOR version bump)

✅ **ABI-Compatible Additions:**
- Adding new functions (does not break existing callers)
- Adding new structs (does not affect existing structs)
- Adding new enum values (does not affect existing values)
- Adding optional function variants (e.g., `dw_run_v2()` alongside `dw_run()`)

### Example: MINOR Version Addition

**v1.0.0**:
```c
typedef struct {
  bool success;
  char* result;
  char* error;
} dw_result_t;

dw_result_t* dw_run(const char* script, const char* inputs);
```

**v1.1.0** (adds `dw_run_with_timeout()` — backward compatible):
```c
// Existing API unchanged
dw_result_t* dw_run(const char* script, const char* inputs);

// New function added
dw_result_t* dw_run_with_timeout(const char* script, const char* inputs, int timeout_ms);
```

**v2.0.0** (changes signature — breaking):
```c
// ❌ BREAKING: added parameter to existing function
dw_result_t* dw_run(const char* script, const char* inputs, dw_options_t* opts);
```

## Language Binding Compatibility

### Python Binding

**Version**: Follows `nativeBindingsVersion` (e.g., `1.0.0`)

**Compatibility Guarantees:**
- **MINOR**: Backward-compatible API additions (new functions, optional parameters)
- **PATCH**: Bug fixes, no API changes
- **MAJOR**: Breaking API changes (removed functions, changed signatures)

**Example: Deprecation Path**
```python
# v1.0.0
def run(script: str, inputs: dict = None) -> ExecutionResult:
    """Original API"""

# v1.1.0 - add new parameter with default
def run(script: str, inputs: dict = None, timeout: int = None) -> ExecutionResult:
    """Extended API - backward compatible"""

# v2.0.0 - remove old parameter format
def run(script: str, inputs: dict = None, options: Options = None) -> ExecutionResult:
    """Breaking change - new options parameter"""
```

### Node.js Binding

**Version**: Follows `nativeBindingsVersion` (e.g., `1.0.0`)

**Compatibility Guarantees:**
- **MINOR**: Backward-compatible additions (new functions, optional parameters)
- **PATCH**: Bug fixes, TypeScript definition fixes
- **MAJOR**: Breaking changes (removed functions, changed signatures, removed deprecated APIs)

**TypeScript Compatibility:**
- Type definitions in `dist/index.d.ts` follow the same versioning
- Adding optional parameters is MINOR (backward compatible)
- Changing required parameters is MAJOR (breaking)

### Go Binding

**Version**: Follows `nativeBindingsVersion` via Git tags (e.g., `v1.0.0`)

**Compatibility Guarantees:**
- **MINOR**: Backward-compatible additions (new functions, new optional fields in structs)
- **PATCH**: Bug fixes, documentation updates
- **MAJOR**: Breaking changes (removed functions, changed signatures, changed struct fields)

**Go Module Versioning:**
- Go uses Git tags for versioning: `v1.0.0`, `v1.1.0`, `v2.0.0`
- MAJOR version changes require module path suffix: `github.com/.../dataweave/v2`

**Example: Struct Evolution**
```go
// v1.0.0
type ExecutionResult struct {
    Success bool
    Result  string
    Error   string
}

// v1.1.0 - add optional field (backward compatible)
type ExecutionResult struct {
    Success  bool
    Result   string
    Error    string
    MimeType string  // New field - optional, zero value if not set
}

// v2.0.0 - change field type (breaking)
type ExecutionResult struct {
    Success bool
    Result  []byte  // Changed from string to []byte
    Error   error   // Changed from string to error
}
```

### Rust Binding

**Version**: Follows `nativeBindingsVersion` in `Cargo.toml` (e.g., `1.0.0`)

**Compatibility Guarantees:**
- **MINOR**: Backward-compatible additions (new functions, new traits, optional fields)
- **PATCH**: Bug fixes, documentation updates
- **MAJOR**: Breaking changes (removed functions, changed signatures, removed traits)

**Rust Semver:**
- Follows [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Adding trait implementations is MINOR
- Changing `pub` visibility is MAJOR

**Example: Error Type Evolution**
```rust
// v1.0.0
#[derive(Debug)]
pub enum DataWeaveError {
    ScriptError(String),
    IOError(String),
}

// v1.1.0 - add new variant (backward compatible if users use wildcard match)
#[derive(Debug)]
#[non_exhaustive]  // Allows future additions
pub enum DataWeaveError {
    ScriptError(String),
    IOError(String),
    TimeoutError(String),  // New variant
}

// v2.0.0 - change variant data (breaking)
#[derive(Debug)]
pub enum DataWeaveError {
    ScriptError { message: String, line: usize },  // Changed from String to struct
    IOError(std::io::Error),  // Changed from String to std::io::Error
}
```

### C Binding

**Version**: Follows `nativeBindingsVersion` with SONAME versioning

**Compatibility Guarantees:**
- **SONAME**: `libdwlib.so.MAJOR` (e.g., `libdwlib.so.1`)
- **MINOR**: Backward-compatible additions (new functions, no SONAME change)
- **PATCH**: Bug fixes (no SONAME change)
- **MAJOR**: Breaking changes (SONAME bump: `libdwlib.so.1` → `libdwlib.so.2`)

**SONAME Versioning:**
```bash
# v1.0.0
libdwlib.so.1.0.0 -> libdwlib.so.1

# v1.1.0 (backward compatible)
libdwlib.so.1.1.0 -> libdwlib.so.1  # Same SONAME

# v2.0.0 (breaking change)
libdwlib.so.2.0.0 -> libdwlib.so.2  # New SONAME
```

## Deprecation Policy

### Deprecation Timeline

1. **Announce** - Mark API as deprecated in release notes
2. **Warning Period** - Minimum **2 MINOR versions** before removal
3. **Remove** - Remove in next MAJOR version

### Example Timeline

```
v1.0.0 - Original API
v1.1.0 - Deprecate old_function(), add new_function()
v1.2.0 - old_function() still present, prints deprecation warning
v1.3.0 - Last version with old_function()
v2.0.0 - old_function() removed
```

### Deprecation Markers

**Python:**
```python
import warnings

@deprecated("Use new_function() instead")
def old_function():
    warnings.warn("old_function() is deprecated", DeprecationWarning)
```

**Node.js:**
```typescript
/** @deprecated Use newFunction() instead */
export function oldFunction() { }
```

**Go:**
```go
// Deprecated: Use NewFunction instead.
func OldFunction() { }
```

**Rust:**
```rust
#[deprecated(since = "1.1.0", note = "Use new_function instead")]
pub fn old_function() { }
```

**C:**
```c
// Deprecated: Use dw_new_function() instead.
// This function will be removed in v2.0.0.
__attribute__((deprecated("Use dw_new_function")))
dw_result_t* dw_old_function(const char* script);
```

## Testing ABI Compatibility

### ABI Compliance Checker

Use [abi-compliance-checker](https://lvc.github.io/abi-compliance-checker/) to validate C API compatibility:

```bash
abi-compliance-checker -l dwlib \
  -old dwlib-1.0.0/dwlib.h \
  -new dwlib-1.1.0/dwlib.h
```

### Language-Specific Compatibility Tests

**Python**: Use [pytest-backward-compatibility](https://pypi.org/project/pytest-backward-compatibility/)

**Go**: Use `go test` with old client code against new library

**Rust**: Use `cargo semver-checks` to detect breaking changes

## Release Checklist

Before releasing a new version:

- [ ] Review all API changes (additions, modifications, removals)
- [ ] Determine version bump (MAJOR, MINOR, or PATCH)
- [ ] Update `nativeBindingsVersion` in `gradle.properties`
- [ ] Update CHANGELOG.md with breaking changes and migration guide
- [ ] Run ABI compliance checker (C API)
- [ ] Run language-specific compatibility tests
- [ ] Update deprecation warnings (if removing deprecated APIs)
- [ ] Update SONAME (if MAJOR version bump)
- [ ] Tag release: `git tag v1.0.0`
- [ ] Build and upload release artifacts

## References

- [Semantic Versioning 2.0.0](https://semver.org/)
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- [Go Modules Versioning](https://go.dev/doc/modules/version-numbers)
- [SONAME Versioning](https://tldp.org/HOWTO/Program-Library-HOWTO/shared-libraries.html)
- [ABI Compliance Checker](https://lvc.github.io/abi-compliance-checker/)

---

**Last Updated**: 2026-06-30  
**Version**: 1.0.0
