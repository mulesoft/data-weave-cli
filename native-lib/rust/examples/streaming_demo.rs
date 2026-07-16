use dataweave::{run_streaming, run_transform, TransformOptions};
use serde_json::json;
use std::collections::HashMap;

fn main() {
    println!("=== DataWeave Rust Streaming Demo ===\n");

    // Example 1: Simple output streaming
    println!("--- Example 1: Output Streaming ---");
    simple_streaming();

    // Example 2: Streaming with inputs
    println!("\n--- Example 2: Streaming with Inputs ---");
    streaming_with_inputs();

    // Example 3: Bidirectional streaming
    println!("\n--- Example 3: Bidirectional Streaming ---");
    bidirectional_streaming();

    // Example 4: Error handling
    println!("\n--- Example 4: Error Handling ---");
    error_handling();
}

fn simple_streaming() {
    let mut result = run_streaming("output application/json --- (1 to 10)", None)
        .expect("run_streaming failed");

    let mut all_chunks = Vec::new();
    for chunk_result in result.by_ref() {
        let chunk = chunk_result.expect("chunk read failed");
        println!("  Received chunk: {} bytes", chunk.len());
        all_chunks.push(chunk);
    }

    let metadata = result.metadata().expect("no metadata");
    assert!(metadata.success, "Script failed: {}", metadata.error.unwrap_or_default());

    let output = String::from_utf8(all_chunks.concat()).expect("invalid utf8");
    println!("  Output: {}", output);
    println!("  MimeType: {:?}, Charset: {:?}", metadata.mime_type, metadata.charset);
}

fn streaming_with_inputs() {
    let mut inputs = HashMap::new();
    inputs.insert("payload".to_string(), json!({
        "users": [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"}
        ]
    }));

    let script = "output application/json --- payload.users map { name: $.name }";
    let mut result = run_streaming(script, Some(inputs))
        .expect("run_streaming failed");

    let mut all_chunks = Vec::new();
    for chunk_result in result.by_ref() {
        let chunk = chunk_result.expect("chunk read failed");
        all_chunks.push(chunk);
    }

    let metadata = result.metadata().expect("no metadata");
    assert!(metadata.success, "Script failed: {}", metadata.error.unwrap_or_default());

    let output = String::from_utf8(all_chunks.concat()).expect("invalid utf8");
    println!("  Output: {}", output);
}

fn bidirectional_streaming() {
    let input = b"[1,2,3,4,5]";
    let opts = TransformOptions {
        input_name: "payload".to_string(),
        input_mime_type: "application/json".to_string(),
        input_charset: None,
    };

    let mut result = run_transform(
        "output application/json --- payload map ($ * $)",
        &input[..],
        opts,
    ).expect("run_transform failed");

    let mut all_chunks = Vec::new();
    for chunk_result in result.by_ref() {
        let chunk = chunk_result.expect("chunk read failed");
        println!("  Received chunk: {} bytes", chunk.len());
        all_chunks.push(chunk);
    }

    let metadata = result.metadata().expect("no metadata");
    assert!(metadata.success, "Script failed: {}", metadata.error.unwrap_or_default());

    let output = String::from_utf8(all_chunks.concat()).expect("invalid utf8");
    println!("  Output (squares): {}", output);
}

fn error_handling() {
    let mut result = run_streaming("this is invalid !!!", None)
        .expect("run_streaming should return result");

    // Drain chunks
    for _ in result.by_ref() {}

    let metadata = result.metadata().expect("no metadata");
    if !metadata.success {
        println!("  Script error (expected): {}", metadata.error.unwrap_or_default());
    } else {
        println!("  Script unexpectedly succeeded");
    }
}
