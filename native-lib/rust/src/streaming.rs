//! Streaming execution support for DataWeave.
//!
//! Contains callback context types, streaming result types, iterator
//! implementations, and the streaming/transform execution logic.

use std::ffi::{CStr, CString};
use std::io::Read;
use std::os::raw::{c_char, c_int, c_void};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::error::Result;
use crate::ffi::{
    free_cstring, run_script_callback, run_script_input_output_callback, AttachedThread,
};
use crate::SendPtr;

/// Metadata returned after a streaming execution completes.
#[derive(Debug, Clone)]
pub struct StreamingMetadata {
    pub success: bool,
    pub error: Option<String>,
    pub mime_type: Option<String>,
    pub charset: Option<String>,
    pub binary: bool,
}

/// Options for bidirectional streaming.
pub struct TransformOptions {
    pub input_name: String,
    pub input_mime_type: String,
    pub input_charset: Option<String>,
}

impl Default for TransformOptions {
    fn default() -> Self {
        TransformOptions {
            input_name: "payload".to_string(),
            input_mime_type: "application/json".to_string(),
            input_charset: None,
        }
    }
}

/// Result of a streaming execution. Implements Iterator to yield chunks.
pub struct StreamResult {
    receiver: mpsc::Receiver<Vec<u8>>,
    metadata: Arc<Mutex<Option<StreamingMetadata>>>,
    /// Handle to the FFI worker thread. Joined on `metadata()` to ensure the
    /// worker has finished populating `metadata` before we read it.
    join: Mutex<Option<thread::JoinHandle<()>>>,
}

impl Iterator for StreamResult {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv() {
            Ok(chunk) => Some(Ok(chunk)),
            Err(_) => None, // Channel closed, iteration done
        }
    }
}

impl StreamResult {
    /// Access metadata after iteration completes. Joins the FFI worker thread
    /// on first call to ensure metadata has been populated.
    pub fn metadata(&self) -> Option<StreamingMetadata> {
        if let Some(handle) = self.join.lock().unwrap().take() {
            let _ = handle.join();
        }
        self.metadata.lock().unwrap().clone()
    }
}

// --- Callback context types ---

/// Context passed through the FFI callback for output streaming.
pub(crate) struct WriteCallbackContext {
    pub(crate) sender: mpsc::Sender<Vec<u8>>,
}

/// Context passed through the FFI callback for bidirectional streaming.
pub(crate) struct ReadWriteCallbackContext {
    pub(crate) sender: mpsc::Sender<Vec<u8>>,
    pub(crate) reader: Mutex<Box<dyn Read + Send>>,
}

// --- Callback functions ---

/// Write callback invoked by the native library for each output chunk.
/// # Safety
/// Called from C code. `ctx` must be a valid pointer to WriteCallbackContext.
pub(crate) extern "C" fn write_callback_streaming(
    ctx: *mut c_void,
    buf: *const c_char,
    length: c_int,
) -> c_int {
    if ctx.is_null() || buf.is_null() || length <= 0 {
        return -1;
    }
    unsafe {
        let sender = &(*(ctx as *const WriteCallbackContext)).sender;
        let slice = std::slice::from_raw_parts(buf as *const u8, length as usize);
        let chunk = slice.to_vec();
        match sender.send(chunk) {
            Ok(_) => 0,
            Err(_) => -1,
        }
    }
}

/// Write callback for bidirectional streaming (same logic, different context type).
pub(crate) extern "C" fn write_callback_transform(
    ctx: *mut c_void,
    buf: *const c_char,
    length: c_int,
) -> c_int {
    if ctx.is_null() || buf.is_null() || length <= 0 {
        return -1;
    }
    unsafe {
        let rw_ctx = &(*(ctx as *const ReadWriteCallbackContext));
        let slice = std::slice::from_raw_parts(buf as *const u8, length as usize);
        let chunk = slice.to_vec();
        match rw_ctx.sender.send(chunk) {
            Ok(_) => 0,
            Err(_) => -1,
        }
    }
}

/// Read callback invoked by the native library to pull input data.
/// # Safety
/// Called from C code. `ctx` must be a valid pointer to ReadWriteCallbackContext.
pub(crate) extern "C" fn read_callback_transform(
    ctx: *mut c_void,
    buf: *mut c_char,
    buf_size: c_int,
) -> c_int {
    if ctx.is_null() || buf.is_null() || buf_size <= 0 {
        return -1;
    }
    unsafe {
        let rw_ctx = &(*(ctx as *const ReadWriteCallbackContext));
        let mut reader_guard = match rw_ctx.reader.lock() {
            Ok(guard) => guard,
            Err(_) => return -1,
        };
        let mut temp_buf = vec![0u8; buf_size as usize];
        match reader_guard.read(&mut temp_buf) {
            Ok(0) => 0, // EOF
            Ok(n) => {
                std::ptr::copy_nonoverlapping(temp_buf.as_ptr(), buf as *mut u8, n);
                n as c_int
            }
            Err(_) => -1,
        }
    }
}

// --- Metadata parsing ---

/// Parse JSON metadata from a streaming callback response.
pub(crate) fn parse_streaming_metadata(raw: &str) -> StreamingMetadata {
    if raw.is_empty() {
        return StreamingMetadata {
            success: false,
            error: Some("Empty response from native library".to_string()),
            mime_type: None,
            charset: None,
            binary: false,
        };
    }
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(parsed) => StreamingMetadata {
            success: parsed.get("success").and_then(|v| v.as_bool()).unwrap_or(false),
            error: parsed.get("error").and_then(|v| v.as_str()).map(|s| s.to_string()),
            mime_type: parsed.get("mimeType").and_then(|v| v.as_str()).map(|s| s.to_string()),
            charset: parsed.get("charset").and_then(|v| v.as_str()).map(|s| s.to_string()),
            binary: parsed.get("binary").and_then(|v| v.as_bool()).unwrap_or(false),
        },
        Err(e) => StreamingMetadata {
            success: false,
            error: Some(format!("Failed to parse metadata: {}", e)),
            mime_type: None,
            charset: None,
            binary: false,
        },
    }
}

// --- Streaming execution functions ---

/// Execute a DataWeave script and stream the output via an iterator.
///
/// Output chunks are delivered as they are produced by the native engine.
/// After iteration completes, call `.metadata()` to get execution metadata.
pub(crate) fn run_streaming_impl(
    c_script: CString,
    c_inputs: CString,
) -> Result<StreamResult> {
    let (sender, receiver) = mpsc::channel::<Vec<u8>>();
    let metadata = Arc::new(Mutex::new(None));
    let metadata_clone = metadata.clone();

    // The callback context must live long enough for the FFI thread
    let ctx = Box::new(WriteCallbackContext { sender });
    let ctx_send = SendPtr(Box::into_raw(ctx));

    let join = thread::spawn(move || {
        let ctx_ptr = ctx_send.as_raw();
        let attached = match AttachedThread::new() {
            Ok(a) => a,
            Err(e) => {
                *metadata_clone.lock().unwrap() = Some(StreamingMetadata {
                    success: false,
                    error: Some(format!("Failed to attach thread to isolate: {:?}", e)),
                    mime_type: None,
                    charset: None,
                    binary: false,
                });
                unsafe { let _ = Box::from_raw(ctx_ptr); }
                return;
            }
        };
        let graal_thread = attached.as_ptr();
        unsafe {
            let result_ptr = run_script_callback(
                graal_thread,
                c_script.as_ptr(),
                c_inputs.as_ptr(),
                write_callback_streaming,
                ctx_ptr as *mut c_void,
            );

            // Free the context box now that the callback is done
            let _ = Box::from_raw(ctx_ptr);

            let raw_result = if result_ptr.is_null() {
                String::new()
            } else {
                let c_str = CStr::from_ptr(result_ptr);
                let result = c_str.to_str().unwrap_or("").to_string();
                free_cstring(graal_thread, result_ptr);
                result
            };

            let meta = parse_streaming_metadata(&raw_result);
            *metadata_clone.lock().unwrap() = Some(meta);
        }
    });

    Ok(StreamResult { receiver, metadata, join: Mutex::new(Some(join)) })
}

/// Execute a DataWeave script with streaming input and output.
///
/// Input data is pulled from the reader and output chunks are delivered
/// via the iterator. Ideal for processing large files with constant memory.
pub(crate) fn run_transform_impl<R: Read + Send + 'static>(
    c_script: CString,
    c_inputs: CString,
    c_input_name: CString,
    c_input_mime_type: CString,
    c_input_charset: CString,
    input_reader: R,
) -> Result<StreamResult> {
    let (sender, receiver) = mpsc::channel::<Vec<u8>>();
    let metadata = Arc::new(Mutex::new(None));
    let metadata_clone = metadata.clone();

    let ctx = Box::new(ReadWriteCallbackContext {
        sender,
        reader: Mutex::new(Box::new(input_reader)),
    });
    let ctx_send = SendPtr(Box::into_raw(ctx));

    let join = thread::spawn(move || {
        let ctx_ptr = ctx_send.as_raw();
        let attached = match AttachedThread::new() {
            Ok(a) => a,
            Err(e) => {
                *metadata_clone.lock().unwrap() = Some(StreamingMetadata {
                    success: false,
                    error: Some(format!("Failed to attach thread to isolate: {:?}", e)),
                    mime_type: None,
                    charset: None,
                    binary: false,
                });
                unsafe { let _ = Box::from_raw(ctx_ptr); }
                return;
            }
        };
        let graal_thread = attached.as_ptr();
        unsafe {
            let result_ptr = run_script_input_output_callback(
                graal_thread,
                c_script.as_ptr(),
                c_inputs.as_ptr(),
                c_input_name.as_ptr(),
                c_input_mime_type.as_ptr(),
                c_input_charset.as_ptr(),
                read_callback_transform,
                write_callback_transform,
                ctx_ptr as *mut c_void,
            );

            // Free the context box
            let _ = Box::from_raw(ctx_ptr);

            let raw_result = if result_ptr.is_null() {
                String::new()
            } else {
                let c_str = CStr::from_ptr(result_ptr);
                let result = c_str.to_str().unwrap_or("").to_string();
                free_cstring(graal_thread, result_ptr);
                result
            };

            let meta = parse_streaming_metadata(&raw_result);
            *metadata_clone.lock().unwrap() = Some(meta);
        }
    });

    Ok(StreamResult { receiver, metadata, join: Mutex::new(Some(join)) })
}
