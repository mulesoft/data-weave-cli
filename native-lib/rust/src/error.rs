use std::fmt;

/// Error type for DataWeave FFI operations.
#[derive(Debug)]
pub enum Error {
    /// The native library returned a null pointer.
    NullPointer,
    /// Input string contains a null byte.
    NulByte,
    /// Failed to decode base64 result.
    Base64(base64::DecodeError),
    /// Failed to parse JSON.
    Json(serde_json::Error),
    /// Failed to decode UTF-8.
    Utf8(std::string::FromUtf8Error),
    /// Response from native library is not valid UTF-8.
    Utf8Response,
    /// No result available (script failed or result is empty).
    NoResult,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NullPointer => write!(f, "Native library returned NULL"),
            Error::NulByte => write!(f, "Input contains null byte"),
            Error::Base64(e) => write!(f, "Base64 decode error: {}", e),
            Error::Json(e) => write!(f, "JSON error: {}", e),
            Error::Utf8(e) => write!(f, "UTF-8 decode error: {}", e),
            Error::Utf8Response => write!(f, "Native response is not valid UTF-8"),
            Error::NoResult => write!(f, "No result available"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
