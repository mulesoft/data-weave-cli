//! Result types for DataWeave execution

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde::{Deserialize, Serialize};

use crate::error::DataWeaveError;

/// Result of a buffered DataWeave script execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub binary: bool,
    #[serde(rename = "mimeType")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub charset: Option<String>,
}

impl ExecutionResult {
    pub fn from_json(json: &str) -> Result<Self, DataWeaveError> {
        if json.is_empty() {
            return Ok(Self {
                success: false,
                result: None,
                error: Some("Native returned empty response".to_string()),
                binary: false,
                mime_type: None,
                charset: None,
            });
        }

        let parsed: serde_json::Value = serde_json::from_str(json)
            .map_err(|e| DataWeaveError::ScriptError(format!("Failed to parse JSON: {}", e)))?;

        if !parsed.is_object() {
            return Ok(Self {
                success: false,
                result: None,
                error: Some("Native response JSON is not an object".to_string()),
                binary: false,
                mime_type: None,
                charset: None,
            });
        }

        let success = parsed
            .get("success")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if !success {
            return Ok(Self {
                success: false,
                result: None,
                error: parsed
                    .get("error")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                binary: false,
                mime_type: None,
                charset: None,
            });
        }

        Ok(Self {
            success: true,
            result: parsed
                .get("result")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            error: None,
            binary: parsed
                .get("binary")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            mime_type: parsed
                .get("mimeType")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            charset: parsed
                .get("charset")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        })
    }

    /// Get the result as raw bytes (base64 decoded)
    pub fn get_bytes(&self) -> Result<Vec<u8>, DataWeaveError> {
        if !self.success {
            return Err(DataWeaveError::ScriptError(
                self.error.clone().unwrap_or_else(|| "Unknown error".to_string()),
            ));
        }

        match &self.result {
            Some(base64_str) => Ok(BASE64.decode(base64_str)?),
            None => Ok(Vec::new()),
        }
    }

    /// Get the result as a UTF-8 string
    pub fn get_string(&self) -> Result<String, DataWeaveError> {
        let bytes = self.get_bytes()?;
        let charset = self.charset.as_deref().unwrap_or("utf-8");

        if charset.eq_ignore_ascii_case("utf-8") || charset.eq_ignore_ascii_case("utf8") {
            Ok(String::from_utf8(bytes)
                .map_err(|e| DataWeaveError::Utf8Error(e.utf8_error()))?)
        } else {
            // For non-UTF-8 charsets, attempt UTF-8 decoding anyway
            // A full implementation would use encoding_rs or similar
            Ok(String::from_utf8_lossy(&bytes).to_string())
        }
    }
}

/// Metadata returned after a streaming execution completes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingResult {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "mimeType")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub charset: Option<String>,
    #[serde(default)]
    pub binary: bool,
}

impl StreamingResult {
    pub fn from_json(json: &str) -> Result<Self, DataWeaveError> {
        if json.is_empty() {
            return Ok(Self {
                success: false,
                error: Some("Empty response from native library".to_string()),
                mime_type: None,
                charset: None,
                binary: false,
            });
        }

        let parsed: serde_json::Value = serde_json::from_str(json)
            .map_err(|e| DataWeaveError::ScriptError(format!("Failed to parse JSON: {}", e)))?;

        let success = parsed
            .get("success")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if !success {
            return Ok(Self {
                success: false,
                error: parsed
                    .get("error")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                mime_type: None,
                charset: None,
                binary: false,
            });
        }

        Ok(Self {
            success: true,
            error: None,
            mime_type: parsed
                .get("mimeType")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            charset: parsed
                .get("charset")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            binary: parsed
                .get("binary")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_result_success() {
        let json = r#"{
            "success": true,
            "result": "NA==",
            "mimeType": "application/json",
            "charset": "UTF-8",
            "binary": false
        }"#;

        let result = ExecutionResult::from_json(json).unwrap();
        assert!(result.success);
        assert_eq!(result.get_string().unwrap(), "4");
    }

    #[test]
    fn test_execution_result_error() {
        let json = r#"{
            "success": false,
            "error": "Script compilation failed"
        }"#;

        let result = ExecutionResult::from_json(json).unwrap();
        assert!(!result.success);
        assert!(result.error.is_some());
    }

    #[test]
    fn test_streaming_result_parsing() {
        let json = r#"{
            "success": true,
            "mimeType": "application/json",
            "charset": "UTF-8",
            "binary": false
        }"#;

        let result = StreamingResult::from_json(json).unwrap();
        assert!(result.success);
        assert_eq!(result.mime_type, Some("application/json".to_string()));
    }
}
