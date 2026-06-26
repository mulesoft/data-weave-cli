//! Error types for DataWeave operations

use std::ffi::NulError;
use std::str::Utf8Error;
use thiserror::Error;

use crate::result::ExecutionResult;

/// Main error type for DataWeave operations
#[derive(Error, Debug)]
pub enum DataWeaveError {
    #[error("DataWeave native library not found. Set DATAWEAVE_NATIVE_LIB or ensure dwlib is in the search path")]
    LibraryNotFound,

    #[error("Failed to load native library: {0}")]
    LibraryLoad(String),

    #[error("Symbol not found in native library: {0}")]
    SymbolNotFound(String),

    #[error("Failed to create GraalVM isolate (error code: {0})")]
    IsolateCreation(i32),

    #[error("Failed to attach thread to isolate (error code: {0})")]
    ThreadAttachment(i32),

    #[error("Native function returned null response")]
    NullResponse,

    #[error("UTF-8 decoding error: {0}")]
    Utf8Error(#[from] Utf8Error),

    #[error("Null byte in C string: {0}")]
    NulError(#[from] NulError),

    #[error("JSON serialization error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Base64 decoding error: {0}")]
    Base64Error(#[from] base64::DecodeError),

    #[error("DataWeave script execution failed: {0}")]
    ScriptError(String),

    #[error("Empty response from native library")]
    EmptyResponse,

    #[error("Invalid JSON response from native library")]
    InvalidJsonResponse,

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Stream error: {0}")]
    StreamError(String),
}

/// Error type specifically for script execution failures
///
/// Contains the full ExecutionResult so callers can inspect error details
#[derive(Error, Debug)]
#[error("DataWeave script execution failed: {}", .0.error.as_deref().unwrap_or("unknown error"))]
pub struct DataWeaveScriptError(pub ExecutionResult);

impl DataWeaveScriptError {
    pub fn new(result: ExecutionResult) -> Self {
        Self(result)
    }

    pub fn result(&self) -> &ExecutionResult {
        &self.0
    }
}
