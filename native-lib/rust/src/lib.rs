use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::io::Read;
use std::os::raw::{c_char, c_int, c_void};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, Once};
use std::thread;

mod error;
pub use error::{Error, Result};

// Opaque GraalVM types (mirrors graal_isolate.h)
#[repr(C)]
struct GraalIsolate {
    _private: [u8; 0],
}
#[repr(C)]
struct GraalIsolateThread {
    _private: [u8; 0],
}

// External C functions from the native library
extern "C" {
    fn graal_create_isolate(
        params: *mut c_void,
        isolate: *mut *mut GraalIsolate,
        thread: *mut *mut GraalIsolateThread,
    ) -> c_int;
    fn graal_attach_thread(
        isolate: *mut GraalIsolate,
        thread: *mut *mut GraalIsolateThread,
    ) -> c_int;
    fn graal_detach_thread(thread: *mut GraalIsolateThread) -> c_int;

    fn run_script(
        thread: *mut GraalIsolateThread,
        script: *const c_char,
        inputs_json: *const c_char,
    ) -> *mut c_char;

    fn free_cstring(thread: *mut GraalIsolateThread, pointer: *mut c_char);

    fn run_script_callback(
        thread: *mut GraalIsolateThread,
        script: *const c_char,
        inputs_json: *const c_char,
        callback: extern "C" fn(*mut c_void, *const c_char, c_int) -> c_int,
        ctx: *mut c_void,
    ) -> *mut c_char;

    fn run_script_input_output_callback(
        thread: *mut GraalIsolateThread,
        script: *const c_char,
        inputs_json: *const c_char,
        input_name: *const c_char,
        input_mime_type: *const c_char,
        input_charset: *const c_char,
        read_callback: extern "C" fn(*mut c_void, *mut c_char, c_int) -> c_int,
        write_callback: extern "C" fn(*mut c_void, *const c_char, c_int) -> c_int,
        ctx: *mut c_void,
    ) -> *mut c_char;
}

// Process-wide GraalVM isolate. Created lazily on first call; subsequent calls attach
// the current OS thread to it. Pointer is shared across threads but only mutated once.
static ISOLATE_INIT: Once = Once::new();
static mut ISOLATE_PTR: *mut GraalIsolate = std::ptr::null_mut();
static mut ISOLATE_INIT_RC: c_int = 0;

fn ensure_isolate() -> Result<*mut GraalIsolate> {
    ISOLATE_INIT.call_once(|| unsafe {
        let mut isolate: *mut GraalIsolate = std::ptr::null_mut();
        let mut thread: *mut GraalIsolateThread = std::ptr::null_mut();
        let rc = graal_create_isolate(std::ptr::null_mut(), &mut isolate, &mut thread);
        if rc == 0 {
            ISOLATE_PTR = isolate;
            // Detach the bootstrap thread; per-call code attaches its own thread.
            graal_detach_thread(thread);
        } else {
            ISOLATE_INIT_RC = rc;
        }
    });
    unsafe {
        if ISOLATE_PTR.is_null() {
            Err(Error::NullPointer)
        } else {
            Ok(ISOLATE_PTR)
        }
    }
}

/// Wraps a raw pointer so it can be moved into a spawned thread.
/// SAFETY: the caller is responsible for ensuring the pointer remains valid for the
/// thread's lifetime. We use this to ferry callback-context pointers across threads.
struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
impl<T> SendPtr<T> {
    fn as_raw(&self) -> *mut T {
        self.0
    }
}

/// RAII guard that attaches the current thread on construction and detaches on drop.
struct AttachedThread {
    thread: *mut GraalIsolateThread,
}

impl AttachedThread {
    fn new() -> Result<Self> {
        let isolate = ensure_isolate()?;
        let mut thread: *mut GraalIsolateThread = std::ptr::null_mut();
        let rc = unsafe { graal_attach_thread(isolate, &mut thread) };
        if rc != 0 || thread.is_null() {
            return Err(Error::NullPointer);
        }
        Ok(AttachedThread { thread })
    }

    fn as_ptr(&self) -> *mut GraalIsolateThread {
        self.thread
    }
}

impl Drop for AttachedThread {
    fn drop(&mut self) {
        unsafe {
            graal_detach_thread(self.thread);
        }
    }
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

/// Parse the JSON response from the native library.
fn parse_execution_result(raw: &str) -> Result<ExecutionResult> {
    serde_json::from_str(raw).map_err(Error::Json)
}

// --- Streaming API ---

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
    /// worker has finished populating `metadata` before we read it. The
    /// channel close (which ends iteration) happens just before metadata is
    /// written, so without joining there's a race.
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

/// Context passed through the FFI callback for output streaming.
struct WriteCallbackContext {
    sender: mpsc::Sender<Vec<u8>>,
}

/// Context passed through the FFI callback for bidirectional streaming.
struct ReadWriteCallbackContext {
    sender: mpsc::Sender<Vec<u8>>,
    reader: Mutex<Box<dyn Read + Send>>,
}

/// Write callback invoked by the native library for each output chunk.
/// # Safety
/// Called from C code. `ctx` must be a valid pointer to WriteCallbackContext or ReadWriteCallbackContext.
extern "C" fn write_callback_streaming(ctx: *mut c_void, buf: *const c_char, length: c_int) -> c_int {
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
extern "C" fn write_callback_transform(ctx: *mut c_void, buf: *const c_char, length: c_int) -> c_int {
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
extern "C" fn read_callback_transform(ctx: *mut c_void, buf: *mut c_char, buf_size: c_int) -> c_int {
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

/// Parse JSON metadata from a streaming callback response.
fn parse_streaming_metadata(raw: &str) -> StreamingMetadata {
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
