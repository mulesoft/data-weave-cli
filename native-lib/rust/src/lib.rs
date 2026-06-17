use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

mod error;
pub use error::{Error, Result};

// External C functions from the native library
extern "C" {
    fn run_script(
        thread: *mut libc::c_void,
        script: *const c_char,
        inputs_json: *const c_char,
    ) -> *mut c_char;

    fn free_cstring(thread: *mut libc::c_void, pointer: *mut c_char);
}

/// Result of a DataWeave script execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub binary: bool,
    #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub charset: Option<String>,
}

impl ExecutionResult {
    /// Decode the base64-encoded result into bytes.
    pub fn get_bytes(&self) -> Result<Vec<u8>> {
        if !self.success || self.result.is_none() {
            return Err(Error::NoResult);
        }
        let result = self.result.as_ref().unwrap();
        BASE64.decode(result).map_err(Error::Base64)
    }

    /// Decode the result into a UTF-8 string.
    pub fn get_string(&self) -> Result<String> {
        if !self.success || self.result.is_none() {
            return Err(Error::NoResult);
        }
        if self.binary {
            return Ok(self.result.as_ref().unwrap().clone());
        }
        let bytes = self.get_bytes()?;
        String::from_utf8(bytes).map_err(Error::Utf8)
    }
}

/// Execute a DataWeave script with the given inputs.
///
/// # Arguments
/// * `script` - The DataWeave script source
/// * `inputs` - Optional map of binding names to values (auto-encoded as JSON)
///
/// # Returns
/// * `Ok(ExecutionResult)` - Execution result with output and metadata
/// * `Err(Error)` - FFI-level error
pub fn run(script: &str, inputs: Option<HashMap<String, Value>>) -> Result<ExecutionResult> {
    let inputs_json = encode_inputs(inputs)?;

    let c_script = CString::new(script).map_err(|_| Error::NulByte)?;
    let c_inputs = CString::new(inputs_json).map_err(|_| Error::NulByte)?;

    unsafe {
        let result_ptr = run_script(std::ptr::null_mut(), c_script.as_ptr(), c_inputs.as_ptr());
        if result_ptr.is_null() {
            return Err(Error::NullPointer);
        }

        let c_str = CStr::from_ptr(result_ptr);
        let raw_result = c_str.to_str().map_err(|_| Error::Utf8Response)?.to_string();
        free_cstring(std::ptr::null_mut(), result_ptr);

        parse_execution_result(&raw_result)
    }
}

/// Encode inputs into the JSON format expected by the native library.
fn encode_inputs(inputs: Option<HashMap<String, Value>>) -> Result<String> {
    let mut encoded = serde_json::Map::new();
    if let Some(inputs_map) = inputs {
        for (name, value) in inputs_map {
            let content = match value {
                Value::String(s) => BASE64.encode(s.as_bytes()),
                _ => {
                    let json_str = serde_json::to_string(&value).map_err(Error::Json)?;
                    BASE64.encode(json_str.as_bytes())
                }
            };
            let mime_type = match value {
                Value::String(_) => "text/plain",
                _ => "application/json",
            };
            encoded.insert(
                name,
                json!({
                    "content": content,
                    "mimeType": mime_type,
                }),
            );
        }
    }
    serde_json::to_string(&encoded).map_err(Error::Json)
}

/// Parse the JSON response from the native library.
fn parse_execution_result(raw: &str) -> Result<ExecutionResult> {
    serde_json::from_str(raw).map_err(Error::Json)
}
