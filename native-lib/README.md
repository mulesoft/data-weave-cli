# native-lib

## Overview

`native-lib` builds a **GraalVM native shared library** that embeds the MuleSoft **DataWeave runtime** and exposes a small C-compatible API.

The main purpose is to allow non-JVM consumers (most notably the Python package in `native-lib/python`) to execute DataWeave scripts **without running a JVM**, while still using the official DataWeave runtime.

## Architecture (GraalVM + FFI)

```
┌─────────────────────────────────────────────┐
│              Python Process                 │
│                                             │
│  ┌────────────────────────────────────────┐ │
│  │  Application Script                    │ │
│  │  - Python: ctypes                      │ │
│  └──────────────┬─────────────────────────┘ │
│                 │                           │
│                 │ FFI Call                  │
│                 ▼                           │
│  ┌────────────────────────────────────────┐ │
│  │  Native Shared Library (dwlib)         │ │
│  │  ┌──────────────────────────────────┐  │ │
│  │  │  GraalVM Isolate                 │  │ │
│  │  │  - NativeLib.run_script()        │  │ │
│  │  │  - DataWeave script execution    │  │ │
│  │  └──────────────────────────────────┘  │ │
│  └────────────────────────────────────────┘ │
└─────────────────────────────────────────────┘
```

## Building with Gradle

### Prerequisites

- A GraalVM distribution installed that includes `native-image`.
- Enough memory for native-image (this build config uses `-J-Xmx6G`).

### Build the shared library

From the repository root:

```bash
./gradlew :native-lib:nativeCompile
```

The shared library is produced under:

- `native-lib/build/native/nativeCompile/`

and is named:

- macOS: `dwlib.dylib`
- Linux: `dwlib.so`
- Windows: `dwlib.dll`

### Stage the library into the Python package (dev workflow)

```bash
./gradlew :native-lib:stagePythonNativeLib
```

This copies `dwlib.*` into:

- `native-lib/python/src/dataweave/native/`

### Build a Python wheel (bundles the native library)

```bash
./gradlew :native-lib:buildPythonWheel
```

The wheel will be created in:

- `native-lib/python/dist/`

## Installing for use in a Python project

### Option A: Install the produced wheel (recommended)

After `:native-lib:buildPythonWheel`:

```bash
python3 -m pip install native-lib/python/dist/dataweave_native-0.0.1-*.whl
```

This wheel includes the `dwlib.*` shared library inside the Python package.

### Option B: Editable install for development

1. Stage the native library:

```bash
./gradlew :native-lib:stagePythonNativeLib
```

2. Install the Python package in editable mode:

```bash
python3 -m pip install -e native-lib/python
```

### Option C: Use an externally-built library via an environment variable

If you want to point Python at a specific built artifact, set:

- `DATAWEAVE_NATIVE_LIB=/absolute/path/to/dwlib.(dylib|so|dll)`

The Python module will also try a few fallbacks (including the wheel-bundled location).

## Using the library (Python examples)

All examples below assume:

```python
import dataweave
```

### 1) Simple script

```python
result = dataweave.run_script("2 + 2")
assert result.success is True
print(result.get_string())  # "4"
```

### 2) Script with inputs (no explicit `mimeType`)

Inputs can be plain Python values. The wrapper auto-encodes them as JSON or text.

```python
result = dataweave.run_script(
    "num1 + num2",
    {"num1": 25, "num2": 17},
)
print(result.get_string())  # "42"
```

### 3) Script with inputs (explicit `mimeType`, `charset`, `properties`)

Use an explicit input dict when you need full control over how DataWeave interprets bytes.

```python
script = "payload.person"
xml_bytes = b"<?xml version=\"1.0\" encoding=\"UTF-16\"?><person><name>Billy</name><age>31</age></person>".decode("utf-8").encode("utf-16")

result = dataweave.run_script(
    script,
    {
        "payload": {
            "content": xml_bytes,
            "mimeType": "application/xml",
            "charset": "UTF-16",
            "properties": {
                "nullValueOn": "empty",
                "maxAttributeSize": 256
            },
        }
    },
)

if result.success:
    print(result.get_string())
else:
    print(result.error)
```

You can also use `InputValue` for the same purpose:

```python
input_value = dataweave.InputValue(
    content="1234567",
    mimeType="application/csv",
    properties={"header": False, "separator": "4"},
)

result = dataweave.run_script("in0.column_1[0]", {"in0": input_value})
print(result.get_string())  # '"567"'
```

### 4) Reusing a DataWeave context to run multiple scripts quicker

Creating an isolate/runtime has overhead. For repeated executions, reuse a single `DataWeave` instance:

```python
with dataweave.DataWeave() as dw:
    r1 = dw.run("2 + 2")
    r2 = dw.run("x + y", {"x": 10, "y": 32})

    print(r1.get_string())  # "4"
    print(r2.get_string())  # "42"
```

### 5) Error handling

There are two common classes of errors:

- The native library cannot be located/loaded.
- Script compilation/execution fails (reported as an unsuccessful `ExecutionResult`).

```python
try:
    result = dataweave.run_script("invalid syntax here")

    if not result.success:
        raise dataweave.DataWeaveError(result.error or "Unknown DataWeave error")

    print(result.get_string())

except dataweave.DataWeaveLibraryNotFoundError as e:
    # Build it (and/or install a wheel) first.
    # Example build command (from repo root): ./gradlew :native-lib:nativeCompile
    raise

except dataweave.DataWeaveError:
    raise

finally:
    # Optional: if you used the global API and want to force cleanup
    dataweave.cleanup()
```
