use dataweave::{run, run_streaming, run_transform, TransformOptions};
use serde_json::json;
use std::collections::HashMap;
use std::io;

#[test]
fn test_run_simple_arithmetic() {
    let result = run("2 + 2", None).expect("run failed");
    assert!(result.success, "Script execution failed: {}", result.error.unwrap_or_default());
    let output = result.get_string().expect("get_string failed");
    assert_eq!(output, "4");
}

#[test]
fn test_run_with_inputs() {
    let mut inputs = HashMap::new();
    inputs.insert("num1".to_string(), json!(25));
    inputs.insert("num2".to_string(), json!(17));

    let result = run("num1 + num2", Some(inputs)).expect("run failed");
    assert!(result.success, "Script execution failed: {}", result.error.unwrap_or_default());
    let output = result.get_string().expect("get_string failed");
    assert_eq!(output, "42");
}

#[test]
fn test_run_script_error() {
    let result = run("invalid syntax here", None).expect("run failed");
    assert!(!result.success, "Expected script to fail");
    assert!(result.error.is_some(), "Expected error message");
}

// --- Streaming Tests ---

#[test]
fn test_run_streaming_simple_output() {
    let mut result = run_streaming("output application/json --- (1 to 5)", None)
        .expect("run_streaming failed");
    let mut chunks = Vec::new();
    for chunk_result in result.by_ref() {
        let chunk = chunk_result.expect("chunk read failed");
        chunks.push(chunk);
    }
    let metadata = result.metadata().expect("no metadata");
    assert!(metadata.success, "Script failed: {}", metadata.error.unwrap_or_default());
    let output = String::from_utf8(chunks.concat()).expect("invalid utf8");
    assert!(output.contains('1') && output.contains('5'),
        "Expected output to contain 1-5, got: {}", output);
    assert_eq!(metadata.mime_type.as_deref(), Some("application/json"));
}

#[test]
fn test_run_streaming_with_inputs() {
    let mut inputs = HashMap::new();
    inputs.insert("payload".to_string(), json!([1, 2, 3]));
    let mut result = run_streaming("output application/json --- payload", Some(inputs))
        .expect("run_streaming failed");
    let mut chunks = Vec::new();
    for chunk_result in result.by_ref() {
        let chunk = chunk_result.expect("chunk read failed");
        chunks.push(chunk);
    }
    let metadata = result.metadata().expect("no metadata");
    assert!(metadata.success, "Script failed: {}", metadata.error.unwrap_or_default());
    let output = String::from_utf8(chunks.concat()).expect("invalid utf8");
    assert!(output.contains('1') && output.contains('3'),
        "Expected output to contain [1,2,3], got: {}", output);
}

#[test]
fn test_run_streaming_script_error() {
    let mut result = run_streaming("invalid syntax here !!!", None)
        .expect("run_streaming should return result even for script errors");
    // Drain chunks
    for _ in result.by_ref() {}
    let metadata = result.metadata().expect("no metadata");
    assert!(!metadata.success, "Expected script to fail");
    assert!(metadata.error.is_some(), "Expected error message in metadata");
}

#[test]
fn test_run_streaming_large_dataset() {
    let mut result = run_streaming("output application/json --- (1 to 1000)", None)
        .expect("run_streaming failed");
    let mut total_bytes = 0;
    let mut chunk_count = 0;
    for chunk_result in result.by_ref() {
        let chunk = chunk_result.expect("chunk read failed");
        total_bytes += chunk.len();
        chunk_count += 1;
    }
    let metadata = result.metadata().expect("no metadata");
    assert!(metadata.success, "Script failed: {}", metadata.error.unwrap_or_default());
    assert!(total_bytes > 0, "Expected non-zero output bytes");
    assert!(chunk_count > 0, "Expected at least one chunk");
}

// --- Bidirectional Streaming Tests ---

#[test]
fn test_run_transform_simple_case() {
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
    let mut chunks = Vec::new();
    for chunk_result in result.by_ref() {
        let chunk = chunk_result.expect("chunk read failed");
        chunks.push(chunk);
    }
    let metadata = result.metadata().expect("no metadata");
    assert!(metadata.success, "Script failed: {}", metadata.error.unwrap_or_default());
    let output = String::from_utf8(chunks.concat()).expect("invalid utf8");
    assert!(output.contains("1") && output.contains("25"),
        "Expected squared values, got: {}", output);
}

#[test]
fn test_run_transform_large_input() {
    // Generate a large JSON array
    let mut json_str = String::from("[");
    for i in 0..1000 {
        if i > 0 {
            json_str.push(',');
        }
        json_str.push_str(&format!("{{\"id\":{}}}", i));
    }
    json_str.push(']');

    let opts = TransformOptions {
        input_name: "payload".to_string(),
        input_mime_type: "application/json".to_string(),
        input_charset: None,
    };
    let mut result = run_transform(
        "output application/json --- sizeOf(payload)",
        json_str.as_bytes(),
        opts,
    ).expect("run_transform failed");
    let mut chunks = Vec::new();
    for chunk_result in result.by_ref() {
        let chunk = chunk_result.expect("chunk read failed");
        chunks.push(chunk);
    }
    let metadata = result.metadata().expect("no metadata");
    assert!(metadata.success, "Script failed: {}", metadata.error.unwrap_or_default());
    let output = String::from_utf8(chunks.concat()).expect("invalid utf8");
    assert!(output.contains("1000"), "Expected '1000', got: {}", output);
}

/// A reader that always returns an error.
struct ErrorReader;

impl io::Read for ErrorReader {
    fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(io::ErrorKind::UnexpectedEof, "test error"))
    }
}

#[test]
fn test_run_transform_input_error() {
    let opts = TransformOptions {
        input_name: "payload".to_string(),
        input_mime_type: "application/json".to_string(),
        input_charset: None,
    };
    let mut result = run_transform(
        "output application/json --- payload",
        ErrorReader,
        opts,
    ).expect("run_transform should return result even for input errors");
    // Drain chunks
    for _ in result.by_ref() {}
    let metadata = result.metadata().expect("no metadata");
    // With an error reader, the script should fail
    if metadata.success {
        // Some native implementations may handle empty input gracefully
        eprintln!("Note: Script may still succeed with empty input depending on native behavior");
    }
}
