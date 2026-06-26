//! # DataWeave Native Library - Rust Bindings
//!
//! Execute DataWeave scripts from Rust via a GraalVM native shared library.
//! Supports buffered execution, output streaming, and bidirectional streaming
//! with constant memory overhead.
//!
//! ## Module Structure
//!
//! - [`ffi`]: Low-level FFI bindings (GraalVM types, extern functions, isolate lifecycle)
//! - [`result`]: Execution result types and helpers
//! - [`streaming`]: Streaming abstractions (callbacks, iterators, metadata)
//! - [`error`]: Error types using `thiserror`

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::io::Read;

mod error;
mod ffi;
mod result;
mod streaming;

pub use error::{Error, Result};
pub use result::ExecutionResult;
pub use streaming::{StreamResult, StreamingMetadata, TransformOptions};

use ffi::{free_cstring, run_script, AttachedThread};
use result::parse_execution_result;
use streaming::{run_streaming_impl, run_transform_impl};

/// Wraps a raw pointer to allow transfer across thread boundaries.
///
/// # Safety Invariants
///
/// The caller MUST ensure:
/// 1. **Lifetime:** The pointer remains valid for the spawned thread's entire lifetime
/// 2. **Exclusive Access:** The pointed-to data is not accessed concurrently from other threads
/// 3. **Proper Cleanup:** The pointer is freed on the thread that received it, after FFI completes
///
/// This type is used to pass callback context pointers from the main thread to the FFI
/// worker thread. The context Box is created before spawning and freed after the FFI
/// call completes, ensuring validity throughout:
///
/// ```text
/// Main Thread              FFI Worker Thread
/// -----------              -----------------
/// Box::new(ctx)
///   |
/// SendPtr(ptr) ------->    Receives ptr
///   |                      Uses ptr in callbacks
/// spawn()                    |
///   |                      Box::from_raw(ptr)  // Frees
///   |                      Thread exits
/// join()
/// ```
///
/// # Why This is Sound
///
/// - The main thread creates the Box and immediately transfers ownership to SendPtr
/// - SendPtr is moved (not copied) to the worker thread
/// - Only the worker thread dereferences the pointer
/// - The worker thread frees the Box after FFI completes
/// - No data races possible because ownership is exclusive at each step
pub(crate) struct SendPtr<T>(pub(crate) *mut T);
unsafe impl<T: Sync> Send for SendPtr<T> {}
impl<T> SendPtr<T> {
    pub(crate) fn as_raw(&self) -> *mut T {
        self.0
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

    let attached = AttachedThread::new()?;
    let thread = attached.as_ptr();
    unsafe {
        let result_ptr = run_script(thread, c_script.as_ptr(), c_inputs.as_ptr());
        if result_ptr.is_null() {
            return Err(Error::NullPointer);
        }

        let c_str = CStr::from_ptr(result_ptr);
        let raw_result = c_str.to_str().map_err(|_| Error::Utf8Response)?.to_string();
        free_cstring(thread, result_ptr);

        parse_execution_result(&raw_result)
    }
}

/// Execute a DataWeave script and stream the output via an iterator.
///
/// Output chunks are delivered as they are produced by the native engine.
/// After iteration completes, call `.metadata()` to get execution metadata.
///
/// # Arguments
/// * `script` - The DataWeave script source
/// * `inputs` - Optional map of binding names to values
///
/// # Returns
/// * `Ok(StreamResult)` - An iterator of output chunks
/// * `Err(Error)` - FFI-level error (before streaming starts)
pub fn run_streaming(script: &str, inputs: Option<HashMap<String, Value>>) -> Result<StreamResult> {
    let inputs_json = encode_inputs(inputs)?;

    let c_script = CString::new(script).map_err(|_| Error::NulByte)?;
    let c_inputs = CString::new(inputs_json).map_err(|_| Error::NulByte)?;

    run_streaming_impl(c_script, c_inputs)
}

/// Execute a DataWeave script with streaming input and output.
///
/// Input data is pulled from the reader and output chunks are delivered
/// via the iterator. Ideal for processing large files with constant memory.
///
/// # Arguments
/// * `script` - The DataWeave script source
/// * `input_reader` - A `Read` source for streaming input
/// * `opts` - Transform options (input name, mime type, charset)
///
/// # Returns
/// * `Ok(StreamResult)` - An iterator of output chunks
/// * `Err(Error)` - FFI-level error (before streaming starts)
pub fn run_transform<R: Read + Send + 'static>(
    script: &str,
    input_reader: R,
    opts: TransformOptions,
) -> Result<StreamResult> {
    let inputs_json = encode_inputs(None)?;

    let c_script = CString::new(script).map_err(|_| Error::NulByte)?;
    let c_inputs = CString::new(inputs_json).map_err(|_| Error::NulByte)?;
    let c_input_name = CString::new(opts.input_name).map_err(|_| Error::NulByte)?;
    let c_input_mime_type = CString::new(opts.input_mime_type).map_err(|_| Error::NulByte)?;
    let c_input_charset = match opts.input_charset {
        Some(charset) => CString::new(charset).map_err(|_| Error::NulByte)?,
        None => CString::new("utf-8").map_err(|_| Error::NulByte)?,
    };

    run_transform_impl(
        c_script,
        c_inputs,
        c_input_name,
        c_input_mime_type,
        c_input_charset,
        input_reader,
    )
}

/// Encode inputs into the JSON format expected by the native library.
fn encode_inputs(inputs: Option<HashMap<String, Value>>) -> Result<String> {
    let mut encoded = serde_json::Map::new();
    if let Some(inputs_map) = inputs {
        for (name, value) in inputs_map {
            let is_string = matches!(value, Value::String(_));
            let content = match value {
                Value::String(s) => BASE64.encode(s.as_bytes()),
                other => {
                    let json_str = serde_json::to_string(&other).map_err(Error::Json)?;
                    BASE64.encode(json_str.as_bytes())
                }
            };
            let mime_type = if is_string { "text/plain" } else { "application/json" };
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
