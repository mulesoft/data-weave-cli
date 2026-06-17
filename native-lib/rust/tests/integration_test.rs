use dataweave::run;
use serde_json::json;
use std::collections::HashMap;

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
