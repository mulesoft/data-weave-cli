# DataWeave Native Bindings - Demo Walkthrough

**Purpose**: A step-by-step demo script for recording a video showcasing the Go and Rust native bindings.

**Duration**: ~10-15 minutes

**Target Audience**: Developers evaluating DataWeave for data transformation in Go/Rust projects

---

## Prerequisites Check

Before starting the demo, verify:

```bash
# Check Java/GraalVM
java -version  # Should show GraalVM 24

# Check Go
go version     # Should show Go 1.21+

# Check Rust
rustc --version  # Should show Rust 1.70+

# Navigate to project
cd /path/to/data-weave-cli
```

---

## Demo Script

### Part 1: Introduction (1 minute)

**[SCREEN: Terminal in project root]**

> "Today I'm going to show you the DataWeave native library bindings for Go and Rust. DataWeave is a powerful data transformation language, and now you can embed it directly into your Go and Rust applications with a simple FFI binding over a GraalVM native library."

> "We'll build the native library once, then create some real-world transformation examples in both languages."

---

### Part 2: Build the Native Library (2 minutes)

**[SCREEN: Terminal]**

> "First, let's build the native library. This is a one-time step that creates a shared library that both Go and Rust will link against."

```bash
# Show what we're building
ls -la native-lib/

# Start the build
./gradlew :native-lib:nativeCompile

# This takes 5-10 minutes, so I'll fast-forward...
# [EDIT: Speed up the video during build]
```

**[AFTER BUILD COMPLETES]**

```bash
# Verify the library was built
ls -lh native-lib/build/native/nativeCompile/
```

> "There's our shared library - `dwlib.dylib` on macOS, `dwlib.so` on Linux, or `dwlib.dll` on Windows. Now let's use it."

---

### Part 3: Go Binding Demo (5 minutes)

**[SCREEN: Split - Code editor + Terminal]**

#### Example 1: Basic User Transformation

> "Let's start with Go. I'll create a simple program that transforms user data from one format to another."

**Create `demo_go_transform.go`:**

```go
package main

import (
	"fmt"
	"log"

	"github.com/mulesoft-labs/data-weave-cli/native-lib/go/dataweave"
)

func main() {
	// Initialize the DataWeave runtime
	if err := dataweave.Initialize(); err != nil {
		log.Fatal("Failed to initialize:", err)
	}
	defer dataweave.Cleanup()

	// Sample input: API response with user data
	inputs := map[string]interface{}{
		"payload": map[string]interface{}{
			"users": []map[string]interface{}{
				{"id": 1, "firstName": "Alice", "lastName": "Smith", "email": "alice@example.com", "role": "admin", "age": 30},
				{"id": 2, "firstName": "Bob", "lastName": "Jones", "email": "bob@example.com", "role": "user", "age": 25},
				{"id": 3, "firstName": "Charlie", "lastName": "Brown", "email": "charlie@example.com", "role": "admin", "age": 35},
			},
		},
	}

	// DataWeave script: Filter admins and restructure
	script := `
		%dw 2.0
		output application/json
		---
		{
			adminCount: sizeOf(payload.users filter $.role == "admin"),
			admins: payload.users 
				filter $.role == "admin"
				map {
					userId: $.id,
					fullName: $.firstName ++ " " ++ $.lastName,
					contact: $.email,
					tenureMonths: $.age * 12
				}
		}
	`

	fmt.Println("🔄 Transforming user data with DataWeave...\n")

	result, err := dataweave.Run(script, inputs)
	if err != nil {
		log.Fatal(err)
	}

	if result.Success {
		fmt.Println("✅ Transformation successful!")
		fmt.Println("\nOutput:")
		fmt.Println(result.GetString())
	} else {
		fmt.Printf("❌ Error: %s\n", result.Error)
	}
}
```

**[TERMINAL]**

> "Let me run this. First, we need to set the library path so Go can find the native library."

```bash
# Set library path
export DYLD_LIBRARY_PATH=$(pwd)/native-lib/build/native/nativeCompile

# Navigate to demo location
mkdir -p demos/go-demo
cd demos/go-demo

# Copy the demo file here (or create it in the editor)
# Initialize Go module
cat > go.mod <<'EOF'
module demo

go 1.21

replace github.com/mulesoft-labs/data-weave-cli/native-lib/go/dataweave => ../../native-lib/go
EOF

# Run it
go run demo_go_transform.go
```

**[EXPECTED OUTPUT]**

```
🔄 Transforming user data with DataWeave...

✅ Transformation successful!

Output:
{
  "adminCount": 2,
  "admins": [
    {
      "userId": 1,
      "fullName": "Alice Smith",
      "contact": "alice@example.com",
      "tenureMonths": 360
    },
    {
      "userId": 3,
      "fullName": "Charlie Brown",
      "contact": "charlie@example.com",
      "tenureMonths": 420
    }
  ]
}
```

> "Perfect! Notice how DataWeave filtered only the admins, restructured the fields, and even calculated tenure in months - all in a declarative script. The Go code just passes data in and gets structured results back."

---

### Part 4: Rust Binding Demo (5 minutes)

**[SCREEN: Split - Code editor + Terminal]**

#### Example 2: CSV to JSON Transformation

> "Now let's switch to Rust. I'll show a more complex example: transforming CSV data into a hierarchical JSON structure."

**Create `demo_rust_csv/src/main.rs`:**

```rust
use dataweave::{initialize, cleanup, run, DataWeaveError};
use std::collections::HashMap;

fn main() -> Result<(), DataWeaveError> {
    initialize()?;

    println!("🔄 Transforming CSV order data to hierarchical JSON...\n");

    // Sample input: CSV orders data
    let csv_data = r#"orderId,customerId,customerName,product,quantity,price
1001,C001,Acme Corp,Widget A,10,25.50
1001,C001,Acme Corp,Widget B,5,30.00
1002,C002,TechStart,Widget A,3,25.50
1003,C001,Acme Corp,Widget C,2,45.00
1004,C003,Global Inc,Widget B,8,30.00
1004,C003,Global Inc,Widget A,12,25.50"#;

    let mut inputs = HashMap::new();
    inputs.insert("payload".to_string(), csv_data.to_string());

    // DataWeave script: Group by customer and aggregate
    let script = r#"
        %dw 2.0
        input payload application/csv
        output application/json
        ---
        {
            summary: {
                totalOrders: sizeOf(payload distinctBy $.orderId),
                totalRevenue: sum(payload map ($.quantity * $.price))
            },
            customers: (payload groupBy $.customerId) mapObject ((orders, customerId) -> {
                (customerId): {
                    name: orders[0].customerName,
                    orders: (orders groupBy $.orderId) mapObject ((items, orderId) -> {
                        (orderId): {
                            items: items map {
                                product: $.product,
                                quantity: $.quantity as Number,
                                subtotal: ($.quantity as Number) * ($.price as Number)
                            },
                            total: sum(items map (($.quantity as Number) * ($.price as Number)))
                        }
                    })
                }
            })
        }
    "#;

    let result = run(script, Some(inputs))?;

    if result.success {
        println!("✅ Transformation successful!");
        println!("\nOutput:");
        
        // Pretty-print the JSON
        let json_str = result.get_string()?;
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&json_str) {
            println!("{}", serde_json::to_string_pretty(&json).unwrap());
        } else {
            println!("{}", json_str);
        }
    } else {
        eprintln!("❌ Error: {:?}", result.error);
    }

    cleanup();
    Ok(())
}
```

**Add `Cargo.toml`:**

```toml
[package]
name = "demo_rust_csv"
version = "0.1.0"
edition = "2021"

[dependencies]
dataweave = { path = "../../native-lib/rust" }
serde_json = "1.0"
```

**[TERMINAL]**

> "Let's run this Rust example. Same library path setup."

```bash
# Back to project root
cd ../..

# Create Rust demo
mkdir -p demos/rust-demo
cd demos/rust-demo

# (Copy the files created above)

# Run it
export DYLD_LIBRARY_PATH=$(pwd)/../../native-lib/build/native/nativeCompile
cargo run
```

**[EXPECTED OUTPUT]**

```
🔄 Transforming CSV order data to hierarchical JSON...

✅ Transformation successful!

Output:
{
  "summary": {
    "totalOrders": 4,
    "totalRevenue": 951.0
  },
  "customers": {
    "C001": {
      "name": "Acme Corp",
      "orders": {
        "1001": {
          "items": [
            {
              "product": "Widget A",
              "quantity": 10,
              "subtotal": 255.0
            },
            {
              "product": "Widget B",
              "quantity": 5,
              "subtotal": 150.0
            }
          ],
          "total": 405.0
        },
        "1003": {
          "items": [
            {
              "product": "Widget C",
              "quantity": 2,
              "subtotal": 90.0
            }
          ],
          "total": 90.0
        }
      }
    },
    "C002": {
      "name": "TechStart",
      "orders": {
        "1002": {
          "items": [
            {
              "product": "Widget A",
              "quantity": 3,
              "subtotal": 76.5
            }
          ],
          "total": 76.5
        }
      }
    },
    "C003": {
      "name": "Global Inc",
      "orders": {
        "1004": {
          "items": [
            {
              "product": "Widget B",
              "quantity": 8,
              "subtotal": 240.0
            },
            {
              "product": "Widget A",
              "quantity": 12,
              "subtotal": 306.0
            }
          ],
          "total": 546.0
        }
      }
    }
  }
}
```

> "Excellent! DataWeave parsed the CSV, grouped orders by customer and order ID, calculated subtotals and totals, and built a complete hierarchical structure. This is the kind of complex transformation that would take dozens of lines of imperative code, done declaratively."

---

### Part 5: Performance & Streaming Demo (2 minutes)

**[OPTIONAL - If time permits]**

> "One more thing - DataWeave supports streaming for large datasets. Let me show a quick example."

**Create `demo_streaming.go`:**

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

	script := `
		%dw 2.0
		output application/json
		---
		// Generate 1000 records
		(1 to 1000) map {
			id: $,
			name: "Record " ++ ($ as String),
			timestamp: now(),
			data: randomBytes(100)  // Large payload
		}
	`

	fmt.Println("📊 Generating large dataset with streaming...\n")

	chunkChan, metaChan, err := dataweave.RunStreaming(script, nil)
	if err != nil {
		log.Fatal(err)
	}

	totalBytes := 0
	chunkCount := 0

	for chunk := range chunkChan {
		totalBytes += len(chunk)
		chunkCount++
		fmt.Printf("📦 Received chunk %d: %d bytes\n", chunkCount, len(chunk))
	}

	meta := <-metaChan
	if meta.Success {
		fmt.Printf("\n✅ Streaming completed!\n")
		fmt.Printf("   Total chunks: %d\n", chunkCount)
		fmt.Printf("   Total bytes: %s\n", formatBytes(totalBytes))
		fmt.Printf("   MIME type: %s\n", meta.MimeType)
	}
}

func formatBytes(bytes int) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	}
	kb := float64(bytes) / 1024
	if kb < 1024 {
		return fmt.Sprintf("%.2f KB", kb)
	}
	mb := kb / 1024
	return fmt.Sprintf("%.2f MB", mb)
}
```

```bash
go run demo_streaming.go
```

> "See how the data streams in chunks? This means constant memory usage even with gigabytes of output. Perfect for ETL pipelines."

---

### Part 6: Wrap-up (1 minute)

**[SCREEN: Terminal showing project structure]**

```bash
# Show what we've built
tree demos/
```

> "Let's recap what we've seen:"

> "✅ **One native library** - Built once with GraalVM, used by all languages"

> "✅ **Go binding** - Idiomatic Go API with channels for streaming"

> "✅ **Rust binding** - Safe FFI with comprehensive error handling"

> "✅ **Real-world transformations** - JSON, CSV, complex hierarchical data"

> "✅ **Production-ready** - Thread-safe, memory-efficient, tested in CI"

**[SCREEN: Show README or docs]**

```bash
# Show available resources
cat README.md
ls docs/
```

> "All five bindings - Python, Node.js, Go, Rust, and C - are fully documented with examples and test suites. Check out the docs for more examples, API references, and troubleshooting guides."

> "The library is ready for production use. Give it a try and let us know what you build!"

---

## Recording Tips

### Before Recording

1. **Clean terminal history**:
   ```bash
   history -c
   clear
   ```

2. **Set a readable terminal theme**:
   - Font size: 14-16pt
   - Color scheme: Light background or high-contrast dark
   - Terminal width: 100-120 columns

3. **Pre-build the native library** (unless showing the build is part of the demo)

4. **Test all commands** in sequence to ensure they work

5. **Prepare demo files** in advance or use a text expander tool

### During Recording

- **Speak clearly** and at a moderate pace
- **Pause between sections** for easy editing
- **Show the terminal output** fully before moving on
- **Use visual cues**: ✅ ❌ 🔄 📊 emojis help viewers track progress
- **Highlight key lines** with your cursor or comments

### Screen Recording Setup

```bash
# macOS: Use QuickTime or OBS
# Recommended resolution: 1920x1080
# Framerate: 30 fps
# Audio: Built-in mic with noise suppression

# Optional: Use a tool to highlight keystrokes
brew install keycastr  # macOS
```

### Post-Production Checklist

- [ ] Speed up the native library build (5-10 minutes → 10-20 seconds)
- [ ] Add chapter markers for each section
- [ ] Include on-screen titles for each demo part
- [ ] Add captions or subtitles
- [ ] Render at 1080p minimum
- [ ] Add intro/outro with links to repo and docs

---

## Demo Variations

### Short Version (5 minutes)
- Skip Part 2 (assume library is built)
- Show only Go OR Rust (not both)
- Skip streaming demo

### Long Version (20 minutes)
- Add error handling examples
- Show bidirectional streaming
- Demonstrate concurrent execution
- Compare performance vs pure Go/Rust implementations

### Live Coding Version
- Start with empty files
- Write code step-by-step
- Explain each DataWeave operator
- Show auto-completion and IDE integration

---

## Backup Demos (If Something Fails)

### Python Quick Demo
```bash
cd native-lib/python
python3 -c "
import dataweave
result = dataweave.run('output json --- {msg: \"Hello from Python!\"}')
print(result.get_string())
"
```

### Node.js Quick Demo
```bash
cd native-lib/node
node -e "
const dw = require('./dist');
const r = dw.run('output json --- {msg: \"Hello from Node!\"}');
console.log(r.getString());
"
```

---

## Links to Include in Video Description

- **GitHub Repository**: https://github.com/mulesoft-labs/data-weave-cli
- **Building Guide**: docs/BUILDING-AND-RUNNING-BINDINGS.md
- **Go README**: native-lib/go/README.md
- **Rust README**: native-lib/rust/README.md
- **DataWeave Documentation**: https://docs.mulesoft.com/dataweave/

---

**Last Updated**: 2026-06-30  
**Demo Script Version**: 1.0.0
