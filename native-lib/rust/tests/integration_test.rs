use dataweave::run;

#[test]
fn test_run_simple_arithmetic() {
    let result = run("2 + 2", None).expect("run failed");
    assert!(result.success, "Script execution failed: {}", result.error.unwrap_or_default());
    let output = result.get_string().expect("get_string failed");
    assert_eq!(output, "4");
}
