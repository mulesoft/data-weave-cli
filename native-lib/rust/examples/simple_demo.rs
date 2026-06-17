use dataweave::run;
use serde_json::json;
use std::collections::HashMap;

fn main() {
    println!("=== DataWeave Rust Demo ===\n");

    // Example 1: Simple arithmetic
    println!("1. Simple arithmetic:");
    let result = run("2 + 2", None).expect("Failed to run script");
    if !result.success {
        eprintln!("Script failed: {}", result.error.unwrap_or_default());
        return;
    }
    let output = result.get_string().expect("Failed to get string");
    println!("   2 + 2 = {}\n", output);

    // Example 2: With inputs
    println!("2. Script with inputs:");
    let mut inputs = HashMap::new();
    inputs.insert("name".to_string(), json!("World"));
    let result = run(r#""Hello, " ++ name ++ "!""#, Some(inputs)).expect("Failed to run script");
    if !result.success {
        eprintln!("Script failed: {}", result.error.unwrap_or_default());
        return;
    }
    let output = result.get_string().expect("Failed to get string");
    println!("   {}\n", output);

    // Example 3: JSON transformation
    println!("3. JSON transformation:");
    let mut inputs = HashMap::new();
    inputs.insert(
        "payload".to_string(),
        json!({
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ]
        }),
    );
    let script = "output application/json --- payload.users map { name: $.name }";
    let result = run(script, Some(inputs)).expect("Failed to run script");
    if !result.success {
        eprintln!("Script failed: {}", result.error.unwrap_or_default());
        return;
    }
    let output = result.get_string().expect("Failed to get string");
    println!("   {}\n", output);

    println!("Demo complete!");
}
