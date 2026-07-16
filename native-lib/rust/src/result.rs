//! Result types for DataWeave execution.
//!
//! Contains [`ExecutionResult`] for buffered execution and helper methods
//! for decoding base64-encoded output.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

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

/// Parse the JSON response from the native library into an [`ExecutionResult`].
pub(crate) fn parse_execution_result(raw: &str) -> Result<ExecutionResult> {
    serde_json::from_str(raw).map_err(Error::Json)
}
