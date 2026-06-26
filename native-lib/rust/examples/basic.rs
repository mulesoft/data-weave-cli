//! Basic usage example for DataWeave Rust bindings

use dataweave_native::{DataWeave, InputValue, ToInputValue};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("DataWeave Rust Bindings - Basic Example\n");
    println!("========================================\n");

    // Initialize DataWeave runtime
    let dw = DataWeave::new()?;

    // Example 1: Simple arithmetic
    println!("Example 1: Simple arithmetic");
    let result = dw.run("2 + 2", HashMap::new())?;
    println!("  Script: 2 + 2");
    println!("  Result: {}\n", result.get_string()?);

    // Example 2: With inputs
    println!("Example 2: With inputs");
    let mut inputs = HashMap::new();
    inputs.insert("num1".to_string(), 25.to_input_value()?);
    inputs.insert("num2".to_string(), 17.to_input_value()?);

    let result = dw.run("num1 + num2", inputs)?;
    println!("  Script: num1 + num2");
    println!("  Inputs: num1=25, num2=17");
    println!("  Result: {}\n", result.get_string()?);

    // Example 3: Array manipulation
    println!("Example 3: Array manipulation");
    let mut inputs = HashMap::new();
    inputs.insert(
        "numbers".to_string(),
        vec![10, 20, 30, 40, 50].to_input_value()?,
    );

    let script = "output application/json\n---\nnumbers map ($ * 2)";
    let result = dw.run(script, inputs)?;
    println!("  Script: numbers map ($ * 2)");
    println!("  Input: [10, 20, 30, 40, 50]");
    println!("  Result: {}\n", result.get_string()?);

    // Example 4: String manipulation
    println!("Example 4: String manipulation");
    let mut inputs = HashMap::new();
    inputs.insert("message".to_string(), "hello world".to_input_value()?);

    let script = "upper(message)";
    let result = dw.run(script, inputs)?;
    println!("  Script: upper(message)");
    println!("  Input: 'hello world'");
    println!("  Result: {}\n", result.get_string()?);

    // Example 5: JSON transformation
    println!("Example 5: JSON transformation");
    let json_input = r#"{"name": "John", "age": 30}"#;
    let mut inputs = HashMap::new();
    inputs.insert(
        "payload".to_string(),
        InputValue::new(json_input.as_bytes().to_vec(), "application/json"),
    );

    let script = r#"
output application/json
---
{
    fullName: payload.name,
    yearsOld: payload.age,
    isAdult: payload.age >= 18
}
"#;
    let result = dw.run(script, inputs)?;
    println!("  Script: Transform JSON structure");
    println!("  Input: {}", json_input);
    println!("  Result: {}\n", result.get_string()?);

    // Example 6: Streaming output
    println!("Example 6: Streaming large output");
    let script = r#"output application/json --- (1 to 100) map { id: $, name: "item_" ++ $ }"#;
    let mut stream = dw.run_streaming(script, HashMap::new())?;

    let mut total_bytes = 0;
    let mut chunk_count = 0;
    while let Some(chunk) = stream.next() {
        total_bytes += chunk.len();
        chunk_count += 1;
    }

    let metadata = stream.metadata().expect("Expected metadata");
    println!("  Script: Generate array of 100 items");
    println!("  Chunks received: {}", chunk_count);
    println!("  Total bytes: {}", total_bytes);
    println!("  MIME type: {}\n", metadata.mime_type.as_deref().unwrap_or("unknown"));

    println!("All examples completed successfully!");

    Ok(())
}
