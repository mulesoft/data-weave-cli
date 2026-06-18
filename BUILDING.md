# Building DataWeave CLI

This document covers building the DataWeave CLI and native library from source.

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| JDK | 17+ | GraalVM CE or Oracle GraalVM recommended |
| GraalVM `native-image` | 24.0+ | Must be installed via `gu install native-image` or bundled with GraalVM |
| Go | 1.21+ | Required for Go bindings (`native-lib/go`) |
| Rust | 1.70+ | Required for Rust bindings (`native-lib/rust`) |
| Python | 3.9+ | Required for Python bindings (`native-lib/python`) |
| Cargo | (with Rust) | Required for Rust tests |

### Setting Up GraalVM

```bash
# Download and extract GraalVM (example for macOS)
export GRAALVM_HOME=/path/to/graalvm
export JAVA_HOME=$GRAALVM_HOME

# Verify native-image is available
native-image --version
```

## Project Structure

```
data-weave-cli/
  native-cli/        # CLI binary (GraalVM native executable)
  native-lib/        # Shared library with language bindings
    go/              # Go bindings
    rust/            # Rust bindings
    python/          # Python bindings
  build.gradle       # Root build file
  settings.gradle    # Includes native-cli, native-lib
```

## Building

### CLI Binary

```bash
./gradlew :native-cli:nativeCompile
```

Output: `native-cli/build/native/nativeCompile/dw`

### Shared Library (native-lib)

```bash
./gradlew :native-lib:nativeCompile
```

Output: `native-lib/build/native/nativeCompile/dwlib.(dylib|so|dll)`

This also generates the GraalVM headers (`graal_isolate.h`, `dwlib.h`) needed by Go and Rust bindings.

### Full Build

```bash
./gradlew build
```

This compiles everything, runs all tests (Java, Go, Rust, Python), and produces distribution artifacts.

## Testing

### All Tests

```bash
./gradlew test
```

### Go Bindings

```bash
./gradlew :native-lib:goTest
```

This task depends on `nativeCompile` and will build the shared library first if needed.

### Rust Bindings

```bash
./gradlew :native-lib:rustTest
```

This task depends on `nativeCompile` and will build the shared library first if needed.

### Python Bindings

```bash
./gradlew :native-lib:pythonTest
```

This stages the native library and runs the Python test suite.

### Skipping Language-Specific Tests

```bash
./gradlew build -PskipGoTests=true -PskipRustTests=true -PskipPythonTests=true
```

## Task Dependency Graph

```
build
  └── test
        ├── goTest ──────┐
        ├── rustTest ────┤── nativeCompile ── nativeCompileClasspathJar ── jar
        └── pythonTest ──┘        (via stagePythonNativeLib)
```

The `nativeCompile` task produces headers and shared library files that the Go, Rust, and Python tests link against. Gradle's `inputs`/`outputs` declarations ensure proper ordering even with `--parallel`.

## Troubleshooting

### `graal_isolate.h: No such file or directory`

The Go or Rust compiler cannot find GraalVM-generated headers. This means `nativeCompile` has not run or its output was cleaned.

**Fix:** Run `./gradlew :native-lib:nativeCompile` before running Go/Rust tests directly.

### `nativeCompile` runs out of memory

The native image build requires significant memory (configured at `-J-Xmx6G`).

**Fix:** Ensure at least 8GB of available RAM. Close other applications if needed.

### Go tests fail with linker errors

The Go tests link against `dwlib` at compile time. If the shared library path has changed:

**Fix:** Verify `native-lib/build/native/nativeCompile/` contains `dwlib.(dylib|so)`.

### Rust tests fail with missing library

Rust uses `build.rs` to locate the shared library.

**Fix:** Ensure `native-lib/build/native/nativeCompile/` contains the shared library and header files.

### `native-image` not found

GraalVM's `native-image` tool is not on the PATH.

**Fix:**
```bash
export GRAALVM_HOME=/path/to/graalvm
export JAVA_HOME=$GRAALVM_HOME
export PATH=$GRAALVM_HOME/bin:$PATH
```

### Python wheel build fails

The wheel build requires the staged native library.

**Fix:** Run `./gradlew :native-lib:stagePythonNativeLib` first, or use `./gradlew :native-lib:buildPythonWheel` which handles staging automatically.
