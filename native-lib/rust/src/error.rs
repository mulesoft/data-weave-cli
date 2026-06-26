//! Error types for DataWeave FFI operations.
//!
//! Uses `thiserror` for ergonomic derive-based error definitions.

use thiserror::Error;

/// Error type for DataWeave FFI operations.
#[derive(Error, Debug)]
pub enum Error {
    /// The native library returned a null pointer.
    #[error("Native library returned NULL")]
    NullPointer,

    /// Input string contains a null byte.
    #[error("Input contains null byte")]
    NulByte,

    /// Failed to decode base64 result.
    #[error("Base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),

    /// Failed to parse JSON.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Failed to decode UTF-8.
    #[error("UTF-8 decode error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),

    /// Response from native library is not valid UTF-8.
    #[error("Native response is not valid UTF-8")]
    Utf8Response,

    /// No result available (script failed or result is empty).
    #[error("No result available")]
    NoResult,

    /// Channel communication error during streaming.
    #[error("Channel error: {0}")]
    Channel(String),

    /// IO error during streaming operations.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Stream execution error.
    #[error("Stream error: {0}")]
    Stream(String),
}

pub type Result<T> = std::result::Result<T, Error>;
