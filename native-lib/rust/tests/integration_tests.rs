//! Integration tests for DataWeave Rust bindings
//!
//! These tests match the Python test suite to ensure feature parity

use dataweave_native::{DataWeave, InputValue, ToInputValue};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

/// Test basic script execution
#[test]
fn test_basic() {
    let dw = DataWeave::new().expect("Failed to create DataWeave instance");
    let result = dw.run("2 + 2", HashMap::new()).expect("Failed to run script");
    assert!(result.success, "Expected success");
    assert_eq!(result.get_string().unwrap(), "4");
}

/// Test script with inputs
#[test]
fn test_with_inputs() {
    let dw = DataWeave::new().expect("Failed to create DataWeave instance");

    let mut inputs = HashMap::new();
    inputs.insert("num1".to_string(), 25.to_input_value().unwrap());
    inputs.insert("num2".to_string(), 17.to_input_value().unwrap());

    let result = dw
        .run("num1 + num2", inputs)
        .expect("Failed to run script");
    assert!(result.success, "Expected success");
    assert_eq!(result.get_string().unwrap(), "42");
}

/// Test RAII pattern with Drop
#[test]
fn test_raii_pattern() {
    {
        let dw = DataWeave::new().expect("Failed to create DataWeave instance");
        let result = dw.run("sqrt(144)", HashMap::new()).unwrap();
        assert_eq!(result.get_string().unwrap(), "12");

        let result = dw.run("sqrt(10000)", HashMap::new()).unwrap();
        assert_eq!(result.get_string().unwrap(), "100");
    }
    // dw is dropped here, resources should be cleaned up
}

/// Test encoding conversion (UTF-16 XML -> CSV)
#[test]
fn test_encoding() {
    let dw = DataWeave::new().expect("Failed to create DataWeave instance");

    let xml_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("person.xml");
    let mut file = File::open(xml_path).expect("Failed to open person.xml");
    let mut xml_bytes = Vec::new();
    file.read_to_end(&mut xml_bytes)
        .expect("Failed to read person.xml");

    let script = r#"output application/csv header=true
---
[payload.person]
"#;

    let mut inputs = HashMap::new();
    inputs.insert(
        "payload".to_string(),
        InputValue::new(xml_bytes, "application/xml").with_charset("UTF-16"),
    );

    let result = dw.run(script, inputs).expect("Failed to run script");
    assert!(result.success, "Expected success");

    let output = result.get_string().unwrap();
    assert!(output.contains("name"), "CSV header missing 'name'");
    assert!(output.contains("age"), "CSV header missing 'age'");
    assert!(output.contains("Billy"), "Expected name 'Billy' in CSV");
    assert!(output.contains("31"), "Expected age '31' in CSV");
}

/// Test auto-conversion of different types
#[test]
fn test_auto_conversion() {
    let dw = DataWeave::new().expect("Failed to create DataWeave instance");

    // Test array
    let mut inputs = HashMap::new();
    inputs.insert(
        "numbers".to_string(),
        vec![1, 2, 3].to_input_value().unwrap(),
    );

    let result = dw.run("numbers[0]", inputs).expect("Failed to run script");
    assert_eq!(result.get_string().unwrap(), "1");
}

/// Test callback-based output streaming
#[test]
fn test_callback_output_basic() {
    let dw = DataWeave::new().expect("Failed to create DataWeave instance");

    let mut chunks = Vec::new();
    let write_callback = |data: &[u8]| {
        chunks.push(data.to_vec());
        0
    };

    let result = dw
        .run_callback("2 + 2", write_callback, HashMap::new())
        .expect("Failed to run callback");

    assert!(result.success, "Expected success");
    let full: Vec<u8> = chunks.into_iter().flatten().collect();
    let text = String::from_utf8(full).unwrap();
    assert_eq!(text, "4");
}

/// Test callback output with inputs
#[test]
fn test_callback_output_with_inputs() {
    let dw = DataWeave::new().expect("Failed to create DataWeave instance");

    let mut chunks = Vec::new();
    let write_callback = |data: &[u8]| {
        chunks.push(data.to_vec());
        0
    };

    let mut inputs = HashMap::new();
    inputs.insert("num1".to_string(), 25.to_input_value().unwrap());
    inputs.insert("num2".to_string(), 17.to_input_value().unwrap());

    let result = dw
        .run_callback("num1 + num2", write_callback, inputs)
        .expect("Failed to run callback");

    assert!(result.success, "Expected success");
    let full: Vec<u8> = chunks.into_iter().flatten().collect();
    let text = String::from_utf8(full).unwrap();
    assert_eq!(text, "42");
}

/// Test callback input+output
#[test]
fn test_callback_input_output() {
    use std::cell::Cell;
    use std::rc::Rc;

    let dw = DataWeave::new().expect("Failed to create DataWeave instance");

    let source = b"[10, 20, 30, 40, 50]";
    let source_pos = Rc::new(Cell::new(0));
    let source_pos_clone = Rc::clone(&source_pos);

    let read_callback = move |buf_size: usize| {
        let pos = source_pos_clone.get();
        let remaining = &source[pos..];
        let n = remaining.len().min(buf_size);
        let chunk = remaining[..n].to_vec();
        source_pos_clone.set(pos + n);
        chunk
    };

    let output_chunks = Rc::new(std::cell::RefCell::new(Vec::new()));
    let output_chunks_clone = Rc::clone(&output_chunks);

    let write_callback = move |data: &[u8]| {
        output_chunks_clone.borrow_mut().push(data.to_vec());
        0
    };

    let script = "output application/json\n---\npayload map ($ * 2)";
    let result = dw
        .run_input_output_callback(
            script,
            "payload",
            "application/json",
            None,
            read_callback,
            write_callback,
            HashMap::new(),
        )
        .expect("Failed to run input/output callback");

    assert!(result.success, "Expected success");
    let full: Vec<u8> = output_chunks.borrow().iter().flatten().copied().collect();
    let text = String::from_utf8(full).unwrap();
    assert!(text.contains("20"), "Expected 20 in result (10*2)");
    assert!(text.contains("100"), "Expected 100 in result (50*2)");
}

/// Test callback input+output with large data
#[test]
fn test_callback_input_output_large() {
    use std::cell::Cell;
    use std::rc::Rc;

    let dw = DataWeave::new().expect("Failed to create DataWeave instance");

    // Build a large JSON array
    let mut source_parts = vec![b"[".to_vec()];
    for i in 1..=1000 {
        if i > 1 {
            source_parts.push(b",".to_vec());
        }
        source_parts.push(format!(r#"{{"id":{}}}"#, i).into_bytes());
    }
    source_parts.push(b"]".to_vec());
    let source: Vec<u8> = source_parts.into_iter().flatten().collect();

    let source_pos = Rc::new(Cell::new(0));
    let source_pos_clone = Rc::clone(&source_pos);

    let read_callback = move |buf_size: usize| {
        let pos = source_pos_clone.get();
        let remaining = &source[pos..];
        let n = remaining.len().min(buf_size);
        let chunk = remaining[..n].to_vec();
        source_pos_clone.set(pos + n);
        chunk
    };

    let output_chunks = Rc::new(std::cell::RefCell::new(Vec::new()));
    let output_chunks_clone = Rc::clone(&output_chunks);

    let write_callback = move |data: &[u8]| {
        output_chunks_clone.borrow_mut().push(data.to_vec());
        0
    };

    let script = "output application/json\n---\nsizeOf(payload)";
    let result = dw
        .run_input_output_callback(
            script,
            "payload",
            "application/json",
            None,
            read_callback,
            write_callback,
            HashMap::new(),
        )
        .expect("Failed to run input/output callback");

    assert!(result.success, "Expected success");
    let full: Vec<u8> = output_chunks.borrow().iter().flatten().copied().collect();
    let text = String::from_utf8(full).unwrap();
    assert_eq!(text, "1000");
}

/// Test run_streaming basic
#[test]
fn test_run_streaming_basic() {
    let dw = DataWeave::new().expect("Failed to create DataWeave instance");

    let mut stream = dw
        .run_streaming(
            "output application/json --- {a: 1, b: 2}",
            HashMap::new(),
        )
        .expect("Failed to create stream");

    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next() {
        chunks.push(chunk);
    }

    let full: Vec<u8> = chunks.into_iter().flatten().collect();
    let text = String::from_utf8(full).unwrap();

    let metadata = stream.metadata().expect("Expected metadata");
    assert!(metadata.success, "Expected success");
    assert_eq!(
        metadata.mime_type,
        Some("application/json".to_string())
    );
    assert!(
        text.contains(r#""a": 1"#) || text.contains(r#""a":1"#),
        "Expected key 'a' in JSON"
    );
}

/// Test run_streaming with large output (multiple chunks)
#[test]
fn test_run_streaming_large() {
    let dw = DataWeave::new().expect("Failed to create DataWeave instance");

    let script = r#"output application/json --- (1 to 5000) map {id: $, name: "item_" ++ $}"#;
    let mut stream = dw
        .run_streaming(script, HashMap::new())
        .expect("Failed to create stream");

    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next() {
        chunks.push(chunk);
    }

    let num_chunks = chunks.len();
    let full: Vec<u8> = chunks.into_iter().flatten().collect();
    let text = String::from_utf8(full).unwrap();

    let metadata = stream.metadata().expect("Expected metadata");
    assert!(metadata.success, "Expected success");
    assert!(
        num_chunks > 1,
        "Expected multiple chunks for large output, got {}",
        num_chunks
    );
    assert!(
        text.contains(r#""id": 5000"#) || text.contains(r#""id":5000"#),
        "Expected last item in output"
    );
}

/// Test run_streaming error handling
#[test]
fn test_run_streaming_error() {
    let dw = DataWeave::new().expect("Failed to create DataWeave instance");

    let mut stream = dw
        .run_streaming(
            "output application/json --- invalid_var",
            HashMap::new(),
        )
        .expect("Failed to create stream");

    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next() {
        chunks.push(chunk);
    }

    let metadata = stream.metadata().expect("Expected metadata");
    assert!(!metadata.success, "Expected failure");
    assert!(metadata.error.is_some(), "Expected error message");
    assert_eq!(chunks.len(), 0, "Expected no chunks on error");
}

/// Test run_streaming with inputs
#[test]
fn test_run_streaming_with_inputs() {
    let dw = DataWeave::new().expect("Failed to create DataWeave instance");

    let mut inputs = HashMap::new();
    inputs.insert("num1".to_string(), 25.to_input_value().unwrap());
    inputs.insert("num2".to_string(), 17.to_input_value().unwrap());

    let mut stream = dw
        .run_streaming("num1 + num2", inputs)
        .expect("Failed to create stream");

    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next() {
        chunks.push(chunk);
    }

    let full: Vec<u8> = chunks.into_iter().flatten().collect();
    let text = String::from_utf8(full).unwrap();

    let metadata = stream.metadata().expect("Expected metadata");
    assert!(metadata.success, "Expected success");
    assert_eq!(text.trim(), "42");
}

/// Test run_transform basic
#[test]
fn test_run_transform_basic() {
    let dw = DataWeave::new().expect("Failed to create DataWeave instance");

    let input_data = vec![b"[10, 20, 30, 40, 50]".to_vec()];
    let script = "output application/json\n---\npayload map ($ * 2)";

    let mut stream = dw
        .run_transform(
            script,
            input_data.into_iter(),
            "payload",
            "application/json",
            None,
            HashMap::new(),
        )
        .expect("Failed to create transform stream");

    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next() {
        chunks.push(chunk);
    }

    let full: Vec<u8> = chunks.into_iter().flatten().collect();
    let text = String::from_utf8(full).unwrap();

    let metadata = stream.metadata().expect("Expected metadata");
    assert!(metadata.success, "Expected success");
    assert!(text.contains("20"), "Expected 20 in result (10*2)");
    assert!(text.contains("100"), "Expected 100 in result (50*2)");
}

/// Test run_transform with large chunked input
#[test]
fn test_run_transform_large() {
    let dw = DataWeave::new().expect("Failed to create DataWeave instance");

    // Build a large JSON array as chunked input
    let mut parts = vec![b"[".to_vec()];
    for i in 1..=1000 {
        if i > 1 {
            parts.push(b",".to_vec());
        }
        parts.push(format!(r#"{{"id":{}}}"#, i).into_bytes());
    }
    parts.push(b"]".to_vec());
    let full_input: Vec<u8> = parts.into_iter().flatten().collect();

    // Feed in 4KB chunks
    let chunk_size = 4096;
    let chunked: Vec<Vec<u8>> = full_input
        .chunks(chunk_size)
        .map(|c| c.to_vec())
        .collect();

    let script = "output application/json\n---\nsizeOf(payload)";
    let mut stream = dw
        .run_transform(
            script,
            chunked.into_iter(),
            "payload",
            "application/json",
            None,
            HashMap::new(),
        )
        .expect("Failed to create transform stream");

    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next() {
        chunks.push(chunk);
    }

    let full: Vec<u8> = chunks.into_iter().flatten().collect();
    let text = String::from_utf8(full).unwrap();

    let metadata = stream.metadata().expect("Expected metadata");
    assert!(metadata.success, "Expected success");
    assert_eq!(text, "1000");
}

/// Test run_transform with file
#[test]
fn test_run_transform_with_file() {
    let dw = DataWeave::new().expect("Failed to create DataWeave instance");

    let xml_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("person.xml");
    let mut file = File::open(xml_path).expect("Failed to open person.xml");

    // Read file in chunks
    let mut chunked_input = Vec::new();
    loop {
        let mut buffer = vec![0u8; 4096];
        match file.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => {
                buffer.truncate(n);
                chunked_input.push(buffer);
            }
            Err(e) => panic!("Failed to read file: {}", e),
        }
    }

    let script = "output application/csv header=true\n---\n[payload.person]";
    let mut stream = dw
        .run_transform(
            script,
            chunked_input.into_iter(),
            "payload",
            "application/xml",
            Some("UTF-16"),
            HashMap::new(),
        )
        .expect("Failed to create transform stream");

    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next() {
        chunks.push(chunk);
    }

    let full: Vec<u8> = chunks.into_iter().flatten().collect();
    let text = String::from_utf8(full).unwrap();

    let metadata = stream.metadata().expect("Expected metadata");
    assert!(metadata.success, "Expected success");
    assert!(text.contains("Billy"), "Expected 'Billy' in CSV");
    assert!(text.contains("31"), "Expected '31' in CSV");
}
