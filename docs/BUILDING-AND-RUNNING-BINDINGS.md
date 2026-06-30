# Building and Running Native Bindings

This guide shows you how to build and run the DataWeave native library bindings, with detailed examples for Go and Rust.

## Prerequisites

### Common Requirements

1. **Java 24 with GraalVM**
   ```bash
   # Download from https://www.graalvm.org/downloads/
   export JAVA_HOME=/path/to/graalvm-24
   export PATH=$JAVA_HOME/bin:$PATH
   
   # Verify
   java -version  # Should show GraalVM 24
   ```

2. **Gradle 8.x** (comes with the project via wrapper)
   ```bash
   ./gradlew --version
   ```

### Language-Specific Requirements

**Go**:
- Go 1.21 or later
- CGO enabled (default)

**Rust**:
- Rust 1.70 or later (stable recommended)
- Cargo (comes with Rust)

**Python**:
- Python 3.9 or later
- pip

**Node.js**:
- Node.js 18 or later
- npm

**C**:
- CMake 3.20 or later
- C99 compiler (gcc, clang, MSVC)

---

## Step 1: Build the Native Library

The native library (`dwlib`) must be built first before any binding can use it.

```bash
# Clone the repository
git clone https://github.com/mulesoft-labs/data-weave-cli.git
cd data-weave-cli

# Build the native library (takes 5-10 minutes)
./gradlew :native-lib:nativeCompile

# Verify the library was built
ls -lh native-lib/build/native/nativeCompile/
```

**Output locations**:
- macOS: `native-lib/build/native/nativeCompile/dwlib.dylib`
- Linux: `native-lib/build/native/nativeCompile/dwlib.so`
- Windows: `native-lib/build/native/nativeCompile/dwlib.dll`

**Build time**: ~5-10 minutes (first build), ~2-3 minutes (incremental)

---

## Go Binding

### Build and Run

#### Option A: Using Gradle (Recommended)

```bash
# Build native library and run Go tests
./gradlew :native-lib:goTest

# The tests demonstrate all API functionality
```

#### Option B: Manual Build

```bash
# 1. Build the native library (if not already built)
./gradlew :native-lib:nativeCompile

# 2. Set library path
export DYLD_LIBRARY_PATH=$(pwd)/native-lib/build/native/nativeCompile  # macOS
export LD_LIBRARY_PATH=$(pwd)/native-lib/build/native/nativeCompile    # Linux

# 3. Navigate to Go binding
cd native-lib/go

# 4. Run the example
go run examples/simple_demo.go
```

### Example 1: Basic Execution

Create `my_script.go`:

```go
package main

import (
	"fmt"
	"log"

	"github.com/mulesoft-labs/data-weave-cli/native-lib/go/dataweave"
)

func main() {
	// Initialize the runtime
	if err := dataweave.Initialize(); err != nil {
		log.Fatal("Failed to initialize:", err)
	}
	defer dataweave.Cleanup()

	// Execute a simple DataWeave script
	result, err := dataweave.Run(`
		%dw 2.0
		output application/json
		---
		{
			message: "Hello from Go!",
			sum: 2 + 2,
			timestamp: now()
		}
	`, nil)

	if err != nil {
		log.Fatal("Execution failed:", err)
	}

	if !result.Success {
		log.Fatal("Script error:", result.Error)
	}

	fmt.Println("Result:", result.GetString())
}
```

**Run it**:

```bash
# Make sure library path is set
export DYLD_LIBRARY_PATH=/path/to/data-weave-cli/native-lib/build/native/nativeCompile

go run my_script.go
```

**Expected output**:
```json
{
  "message": "Hello from Go!",
  "sum": 4,
  "timestamp": "2026-06-30T10:30:00-07:00"
}
```

### Example 2: JSON Transformation with Inputs

Create `transform.go`:

```go
package main

import (
	"fmt"
	"log"

	"github.com/mulesoft-labs/data-weave-cli/native-lib/go/dataweave"
)

func main() {
	dataweave.Initialize()
	defer dataweave.Cleanup()

	// Define input data
	inputs := map[string]interface{}{
		"users": []map[string]interface{}{
			{"id": 1, "name": "Alice", "age": 30, "role": "admin"},
			{"id": 2, "name": "Bob", "age": 25, "role": "user"},
			{"id": 3, "name": "Charlie", "age": 35, "role": "admin"},
		},
	}

	// DataWeave script to filter and transform
	script := `
		%dw 2.0
		output application/json
		---
		{
			admins: payload.users 
				filter $.role == "admin" 
				map {
					id: $.id,
					name: $.name,
					ageInMonths: $.age * 12
				}
		}
	`

	result, err := dataweave.Run(script, map[string]interface{}{
		"payload": inputs,
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.GetString())
}
```

**Run it**:
```bash
go run transform.go
```

**Expected output**:
```json
{
  "admins": [
    { "id": 1, "name": "Alice", "ageInMonths": 360 },
    { "id": 3, "name": "Charlie", "ageInMonths": 420 }
  ]
}
```

### Example 3: Streaming Output

Create `streaming.go`:

```go
package main

import (
	"fmt"
	"log"

	"github.com/mulesoft-labs/data-weave-cli/native-lib/go/dataweave"
)

func main() {
	dataweave.Initialize()
	defer dataweave.Cleanup()

	// Generate large JSON array
	script := `
		%dw 2.0
		output application/json
		---
		(1 to 100) map {
			id: $,
			name: "Item " ++ ($$ as String),
			value: $ * 10
		}
	`

	// Stream the output in chunks
	chunkChan, metaChan, err := dataweave.RunStreaming(script, nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Streaming output:")
	for chunk := range chunkChan {
		fmt.Printf("Received chunk: %d bytes\n", len(chunk))
		// Process chunk (e.g., write to file, send over network)
	}

	// Get final metadata
	meta := <-metaChan
	if !meta.Success {
		log.Fatal("Stream error:", meta.Error)
	}

	fmt.Printf("Stream completed. MIME type: %s\n", meta.MimeType)
}
```

---

## Rust Binding

### Build and Run

#### Option A: Using Gradle (Recommended)

```bash
# Build native library and run Rust tests
./gradlew :native-lib:rustTest

# The tests demonstrate all API functionality
```

#### Option B: Manual Build

```bash
# 1. Build the native library (if not already built)
./gradlew :native-lib:nativeCompile

# 2. Set library path
export DYLD_LIBRARY_PATH=$(pwd)/native-lib/build/native/nativeCompile  # macOS
export LD_LIBRARY_PATH=$(pwd)/native-lib/build/native/nativeCompile    # Linux

# 3. Navigate to Rust binding
cd native-lib/rust

# 4. Run the example
cargo run --example simple_demo
```

### Example 1: Basic Execution

Create a new Rust project:

```bash
cargo new dataweave-example
cd dataweave-example
```

Add to `Cargo.toml`:

```toml
[dependencies]
dataweave = { path = "../../native-lib/rust" }
```

Create `src/main.rs`:

```rust
use dataweave::{initialize, cleanup, run, DataWeaveError};

fn main() -> Result<(), DataWeaveError> {
    // Initialize the runtime
    initialize()?;

    // Execute a simple DataWeave script
    let result = run(
        r#"
        %dw 2.0
        output application/json
        ---
        {
            message: "Hello from Rust!",
            sum: 2 + 2,
            timestamp: now()
        }
        "#,
        None,
    )?;

    if result.success {
        println!("Result: {}", result.get_string()?);
    } else {
        eprintln!("Error: {:?}", result.error);
    }

    // Cleanup
    cleanup();
    Ok(())
}
```

**Run it**:

```bash
# Set library path
export DYLD_LIBRARY_PATH=/path/to/data-weave-cli/native-lib/build/native/nativeCompile

cargo run
```

### Example 2: JSON Transformation with Inputs

Create `src/main.rs`:

```rust
use dataweave::{initialize, cleanup, run, DataWeaveError};
use serde_json::json;
use std::collections::HashMap;

fn main() -> Result<(), DataWeaveError> {
    initialize()?;

    // Define input data
    let users = json!([
        {"id": 1, "name": "Alice", "age": 30, "role": "admin"},
        {"id": 2, "name": "Bob", "age": 25, "role": "user"},
        {"id": 3, "name": "Charlie", "age": 35, "role": "admin"},
    ]);

    // Create inputs map
    let mut inputs = HashMap::new();
    inputs.insert(
        "payload".to_string(),
        json!({ "users": users }).to_string(),
    );

    // DataWeave script
    let script = r#"
        %dw 2.0
        output application/json
        ---
        {
            admins: payload.users 
                filter $.role == "admin" 
                map {
                    id: $.id,
                    name: $.name,
                    ageInMonths: $.age * 12
                }
        }
    "#;

    let result = run(script, Some(inputs))?;

    if result.success {
        println!("{}", result.get_string()?);
    } else {
        eprintln!("Error: {:?}", result.error);
    }

    cleanup();
    Ok(())
}
```

**Add serde_json to Cargo.toml**:
```toml
[dependencies]
dataweave = { path = "../../native-lib/rust" }
serde_json = "1.0"
```

**Run it**:
```bash
cargo run
```

### Example 3: Streaming Output

Create `src/main.rs`:

```rust
use dataweave::{initialize, cleanup, run_streaming, DataWeaveError};

fn main() -> Result<(), DataWeaveError> {
    initialize()?;

    let script = r#"
        %dw 2.0
        output application/json
        ---
        (1 to 100) map {
            id: $,
            name: "Item " ++ ($$ as String),
            value: $ * 10
        }
    "#;

    println!("Streaming output:");
    
    // Get the streaming iterator
    let mut stream = run_streaming(script, None)?;

    // Process chunks as they arrive
    while let Some(chunk) = stream.next_chunk()? {
        println!("Received chunk: {} bytes", chunk.len());
        // Process chunk (e.g., write to file)
    }

    // Get final metadata
    let meta = stream.finish()?;
    if meta.success {
        println!("Stream completed. MIME type: {:?}", meta.mime_type);
    } else {
        eprintln!("Stream error: {:?}", meta.error);
    }

    cleanup();
    Ok(())
}
```

---

## Python Binding

### Quick Start

```bash
# Build and install
./gradlew :native-lib:buildPythonWheel
pip install native-lib/python/dist/dataweave_native-1.0.0-py3-none-any.whl

# Run example
python3 -c "
import dataweave
result = dataweave.run('output json --- {message: \"Hello from Python!\"}')
print(result.get_string())
"
```

### Full Example

Create `example.py`:

```python
import dataweave

# Initialize (happens automatically)
result = dataweave.run(
    """
    %dw 2.0
    output application/json
    ---
    {
        message: "Hello from Python!",
        users: payload.users filter $.age > 25
    }
    """,
    {
        "payload": {
            "users": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
                {"name": "Charlie", "age": 35}
            ]
        }
    }
)

if result.success:
    print(result.get_string())
else:
    print(f"Error: {result.error}")
```

---

## Node.js Binding

### Quick Start

```bash
# Build and pack
./gradlew :native-lib:buildNodePackage
npm install native-lib/node/dataweave-native-1.0.0.tgz

# Run example
node -e "
const dw = require('@dataweave/native');
const result = dw.run('output json --- {message: \"Hello from Node!\"}');
console.log(result.getString());
"
```

### Full Example

Create `example.js`:

```javascript
const { run } = require('@dataweave/native');

const result = run(
  `
  %dw 2.0
  output application/json
  ---
  {
    message: "Hello from Node.js!",
    admins: payload.users filter $.role == "admin" map $.name
  }
  `,
  {
    payload: {
      users: [
        { name: "Alice", role: "admin" },
        { name: "Bob", role: "user" },
        { name: "Charlie", role: "admin" }
      ]
    }
  }
);

if (result.success) {
  console.log(result.getString());
} else {
  console.error('Error:', result.error);
}
```

---

## C Binding

### Build and Run

```bash
# Build the C binding
cd native-lib/c
cmake -B build -DDWLIB_PATH=../build/native/nativeCompile
cmake --build build

# Run example
./build/simple
```

### Example

Create `example.c`:

```c
#include "dataweave.h"
#include <stdio.h>
#include <stdlib.h>

int main() {
    // Initialize
    if (dw_initialize() != 0) {
        fprintf(stderr, "Failed to initialize\n");
        return 1;
    }

    // Run script
    dw_result_t* result = dw_run(
        "%dw 2.0\n"
        "output application/json\n"
        "---\n"
        "{ message: \"Hello from C!\" }",
        "{}"
    );

    if (result->success) {
        const char* output = dw_result_get_string(result);
        printf("Result: %s\n", output);
    } else {
        fprintf(stderr, "Error: %s\n", result->error);
    }

    // Cleanup
    dw_result_free(result);
    dw_cleanup();
    
    return 0;
}
```

**Compile and run**:
```bash
gcc example.c -I native-lib/c/include -L native-lib/build/native/nativeCompile -ldwlib -o example
./example
```

---

## Troubleshooting

### Common Issues

**1. "Library not found" / "Cannot load library"**

Solution: Set library path environment variable:

```bash
# macOS
export DYLD_LIBRARY_PATH=/path/to/data-weave-cli/native-lib/build/native/nativeCompile

# Linux
export LD_LIBRARY_PATH=/path/to/data-weave-cli/native-lib/build/native/nativeCompile

# Windows (PowerShell)
$env:PATH = "C:\path\to\data-weave-cli\native-lib\build\native\nativeCompile;$env:PATH"
```

**2. "Native library not initialized"**

Solution: Call `initialize()` before running scripts:

```go
dataweave.Initialize()  // Go
```

```rust
initialize()?;  // Rust
```

**3. Go: "cgo: C compiler not found"**

Solution: Install a C compiler:
- macOS: `xcode-select --install`
- Linux: `sudo apt install build-essential`
- Windows: Install MinGW or Visual Studio

**4. Rust: "linking with cc failed"**

Solution: Make sure library path is set before running `cargo build` or `cargo run`.

---

## Performance Tips

1. **Reuse the runtime**: Initialize once, run many scripts
2. **Use streaming for large outputs**: Reduces memory usage
3. **Use bidirectional streaming for large inputs**: Constant memory
4. **Precompile scripts**: Cache compiled scripts for repeated use (future feature)

---

## Next Steps

- Read language-specific READMEs:
  - [Go README](../native-lib/go/README.md)
  - [Rust README](../native-lib/rust/README.md)
  - [Python README](../native-lib/python/README.md)
  - [Node.js README](../native-lib/node/README.md)
  - [C README](../native-lib/c/README.md)

- Check out comprehensive demos:
  - [Go Demo](../native-lib/go/examples/streaming_demo.go)
  - [Rust Demo](../native-lib/rust/examples/comprehensive_demo.rs)

- Review API documentation:
  - [API Quick Reference](../native-lib/demos/API_QUICK_REFERENCE.md)

---

**Last Updated**: 2026-06-30  
**Version**: 1.0.0
