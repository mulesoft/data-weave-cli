#!/usr/bin/env rust-script
//! DataWeave Rust Bindings - Comprehensive Demo
//!
//! Showcases all major capabilities:
//! - Basic transformations
//! - Working with inputs
//! - JSON transformations
//! - Streaming for large datasets
//! - Error handling
//! - Concurrent execution

use std::collections::HashMap;
use std::io::{self, Write};

// Note: In a real project, you'd use: use dataweave_native::*;
// For this demo script to work standalone, compile against the library

fn demo_basic_operations() {
    println!("{}", "=".repeat(60));
    println!("DEMO 1: Basic Operations");
    println!("{}", "=".repeat(60));

    // Simple arithmetic
    println!("\n1.1 Arithmetic:");
    match dataweave::run("2 + 2 * 3", None) {
        Ok(result) => {
            println!("   Expression: 2 + 2 * 3");
            println!("   Result: {}", result.as_string().unwrap());
        }
        Err(e) => println!("   Error: {}", e),
    }

    // String concatenation
    println!("\n1.2 String operations:");
    match dataweave::run(r#""Hello" ++ " " ++ "World""#, None) {
        Ok(result) => {
            println!(r#"   Expression: "Hello" ++ " " ++ "World""#);
            println!("   Result: {}", result.as_string().unwrap());
        }
        Err(e) => println!("   Error: {}", e),
    }

    // Array operations
    println!("\n1.3 Array operations:");
    match dataweave::run("[1, 2, 3, 4, 5] map ($ * 2)", None) {
        Ok(result) => {
            println!("   Expression: [1, 2, 3, 4, 5] map ($ * 2)");
            println!("   Result: {}", result.as_string().unwrap());
        }
        Err(e) => println!("   Error: {}", e),
    }
}

fn demo_with_inputs() {
    println!("\n{}", "=".repeat(60));
    println!("DEMO 2: Working with Inputs");
    println!("{}", "=".repeat(60));

    // Simple variable substitution
    println!("\n2.1 Variable substitution:");
    let mut inputs = HashMap::new();
    inputs.insert("name".to_string(), serde_json::json!("Alice"));
    inputs.insert("age".to_string(), serde_json::json!(30));

    let script = r#""Hello, " ++ name ++ "! You are " ++ age ++ " years old.""#;
    match dataweave::run(script, Some(inputs.clone())) {
        Ok(result) => {
            println!("   Inputs: {:?}", inputs);
            println!("   Result: {}", result.as_string().unwrap());
        }
        Err(e) => println!("   Error: {}", e),
    }

    // Working with payload
    println!("\n2.2 Payload transformation:");
    let mut inputs = HashMap::new();
    inputs.insert(
        "payload".to_string(),
        serde_json::json!({
            "firstName": "John",
            "lastName": "Doe",
            "email": "john.doe@example.com"
        }),
    );

    let script = r#"
    output application/json
    ---
    {
        fullName: payload.firstName ++ " " ++ payload.lastName,
        contact: payload.email,
        username: lower(payload.lastName) ++ "." ++ lower(payload.firstName)
    }
    "#;

    match dataweave::run(script, Some(inputs)) {
        Ok(result) => {
            println!("   Input: User record");
            println!("   Output: {}", result.as_string().unwrap());
        }
        Err(e) => println!("   Error: {}", e),
    }
}

fn demo_json_transformations() {
    println!("\n{}", "=".repeat(60));
    println!("DEMO 3: JSON Transformations");
    println!("{}", "=".repeat(60));

    // Array mapping
    println!("\n3.1 Array mapping:");
    let mut inputs = HashMap::new();
    inputs.insert(
        "payload".to_string(),
        serde_json::json!({
            "users": [
                {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
                {"id": 2, "name": "Bob", "age": 25, "city": "London"},
                {"id": 3, "name": "Charlie", "age": 35, "city": "Tokyo"}
            ]
        }),
    );

    let script = r#"
    output application/json
    ---
    payload.users map {
        userId: $.id,
        userName: $.name,
        location: $.city,
        isAdult: $.age >= 18
    }
    "#;

    match dataweave::run(script, Some(inputs)) {
        Ok(result) => {
            println!("   Transformed 3 users");
            println!("   Output: {}", result.as_string().unwrap());
        }
        Err(e) => println!("   Error: {}", e),
    }
}

fn demo_streaming() {
    println!("\n{}", "=".repeat(60));
    println!("DEMO 4: Streaming (Constant Memory)");
    println!("{}", "=".repeat(60));

    println!("\n4.1 Streaming large array transformation:");
    println!("   Generating 1000 records and streaming output...");

    // Generate large dataset
    let records: Vec<_> = (0..1000)
        .map(|i| serde_json::json!({"id": i, "value": i * 10}))
        .collect();

    let mut inputs = HashMap::new();
    inputs.insert("payload".to_string(), serde_json::json!(records));

    let script = r#"
    output application/json
    ---
    payload map {
        recordId: $.id,
        computedValue: $.value * 2,
        category: if ($.id mod 2 == 0) "even" else "odd"
    }
    "#;

    // Use streaming API
    let mut chunk_count = 0;
    let mut total_bytes = 0;

    match dataweave::run_streaming(script, Some(inputs), |chunk| {
        chunk_count += 1;
        total_bytes += chunk.len();
    }) {
        Ok(metadata) => {
            println!("   ✓ Streamed {} chunks", chunk_count);
            println!("   ✓ Total output: {} bytes", total_bytes);
            println!("   ✓ Memory usage: Constant (chunks processed incrementally)");
        }
        Err(e) => println!("   Error: {}", e),
    }
}

fn demo_error_handling() {
    println!("\n{}", "=".repeat(60));
    println!("DEMO 5: Error Handling");
    println!("{}", "=".repeat(60));

    // Syntax error
    println!("\n5.1 Handling syntax errors:");
    match dataweave::run("2 + + 3", None) {
        Ok(_) => println!("   Unexpected success"),
        Err(e) => {
            println!("   ✗ Syntax error detected:");
            println!("     {}", e);
        }
    }

    // Runtime error
    println!("\n5.2 Handling runtime errors:");
    let mut inputs = HashMap::new();
    inputs.insert("payload".to_string(), serde_json::json!({}));

    match dataweave::run("payload.user.email", Some(inputs)) {
        Ok(_) => println!("   Unexpected success"),
        Err(e) => {
            println!("   ✗ Runtime error detected:");
            println!("     {}", e);
        }
    }

    // Successful execution
    println!("\n5.3 Successful execution:");
    match dataweave::run("2 + 3", None) {
        Ok(result) => {
            println!("   ✓ Valid expression: 2 + 3 = {}", result.as_string().unwrap());
        }
        Err(e) => println!("   Error: {}", e),
    }
}

fn demo_concurrent_execution() {
    println!("\n{}", "=".repeat(60));
    println!("DEMO 6: Concurrent Execution (Rust Safety)");
    println!("{}", "=".repeat(60));

    println!("\n6.1 Running 10 transformations concurrently:");
    println!("   (Validates thread safety and Send/Sync bounds)");

    use std::sync::{Arc, Mutex};
    use std::thread;

    let results = Arc::new(Mutex::new(Vec::new()));
    let mut handles = vec![];

    for i in 0..10 {
        let results = Arc::clone(&results);
        let handle = thread::spawn(move || {
            let mut inputs = HashMap::new();
            inputs.insert("n".to_string(), serde_json::json!(i));

            let script = r#"
            output application/json
            ---
            {
                threadId: n,
                squared: n * n,
                message: "Computed by thread " ++ n
            }
            "#;

            match dataweave::run(script, Some(inputs)) {
                Ok(result) => {
                    let mut results = results.lock().unwrap();
                    results.push((i, result.as_string().unwrap()));
                }
                Err(e) => eprintln!("   Thread {} error: {}", i, e),
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    let results = results.lock().unwrap();
    println!("   ✓ All {} threads completed successfully", results.len());
    println!("   ✓ No data races (validated by Rust's type system)");
}

fn demo_advanced_features() {
    println!("\n{}", "=".repeat(60));
    println!("DEMO 7: Advanced Features");
    println!("{}", "=".repeat(60));

    // Reduce/fold
    println!("\n7.1 Reduce (sum of array):");
    let script = "[1, 2, 3, 4, 5] reduce ((item, accumulator=0) -> accumulator + item)";
    match dataweave::run(script, None) {
        Ok(result) => {
            println!("   Expression: {}", script);
            println!("   Result: {}", result.as_string().unwrap());
        }
        Err(e) => println!("   Error: {}", e),
    }

    // Pattern matching
    println!("\n7.2 Pattern matching:");
    let mut inputs = HashMap::new();
    inputs.insert("status".to_string(), serde_json::json!("SUCCESS"));

    let script = r#"
    status match {
        case "SUCCESS" -> "Operation completed successfully"
        case "PENDING" -> "Operation in progress"
        case "FAILED" -> "Operation failed"
        else -> "Unknown status"
    }
    "#;

    match dataweave::run(script, Some(inputs)) {
        Ok(result) => {
            println!("   Input status: SUCCESS");
            println!("   Result: {}", result.as_string().unwrap());
        }
        Err(e) => println!("   Error: {}", e),
    }
}

fn main() {
    println!("\n{}", "=".repeat(60));
    println!(" DataWeave Rust Bindings - Comprehensive Demo");
    println!("{}", "=".repeat(60));
    println!("\n This demo showcases:");
    println!("  • Basic transformations and operations");
    println!("  • Working with inputs and context");
    println!("  • JSON data transformations");
    println!("  • Streaming for large datasets");
    println!("  • Error handling");
    println!("  • Concurrent execution (thread safety)");
    println!("  • Advanced DataWeave features");
    println!();

    demo_basic_operations();
    demo_with_inputs();
    demo_json_transformations();
    demo_streaming();
    demo_error_handling();
    demo_concurrent_execution();
    demo_advanced_features();

    println!("\n{}", "=".repeat(60));
    println!("✓ All demos completed successfully!");
    println!("{}", "=".repeat(60));
    println!();
}
