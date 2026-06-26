//! Streaming execution support

use std::collections::VecDeque;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::error::DataWeaveError;
use crate::ffi::{GraalIsolate, GraalIsolateThread, NativeLib};
use crate::result::StreamingResult;
use crate::{HashMap, InputValue};

/// Wrapper to make GraalIsolate pointer Send-safe for thread boundaries
struct SendIsolatePtr(*mut GraalIsolate);
unsafe impl Send for SendIsolatePtr {}

impl SendIsolatePtr {
    fn as_ptr(&self) -> *mut GraalIsolate {
        self.0
    }
}

/// Streaming output wrapper
///
/// Yields output chunks as they are produced. After iteration completes,
/// metadata is available via the `metadata()` method.
pub struct Stream {
    chunks: VecDeque<Vec<u8>>,
    metadata: Option<StreamingResult>,
    error: Option<DataWeaveError>,
}

impl Stream {
    fn new() -> Self {
        Self {
            chunks: VecDeque::new(),
            metadata: None,
            error: None,
        }
    }

    /// Get the next output chunk
    pub fn next(&mut self) -> Option<Vec<u8>> {
        self.chunks.pop_front()
    }

    /// Get metadata after stream completes
    pub fn metadata(&self) -> Option<&StreamingResult> {
        self.metadata.as_ref()
    }

    /// Get error if stream failed
    pub fn error(&self) -> Option<&DataWeaveError> {
        self.error.as_ref()
    }

    fn push_chunk(&mut self, chunk: Vec<u8>) {
        self.chunks.push_back(chunk);
    }

    fn set_metadata(&mut self, metadata: StreamingResult) {
        self.metadata = Some(metadata);
    }

    fn set_error(&mut self, error: DataWeaveError) {
        self.error = Some(error);
    }
}

impl Iterator for Stream {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next()
    }
}

struct StreamContext {
    chunks: Mutex<Vec<Vec<u8>>>,
    metadata: Mutex<Option<String>>,
}

/// Create a streaming execution that yields output chunks
pub fn create_stream(
    lib: Arc<NativeLib>,
    isolate: *mut GraalIsolate,
    _main_thread: *mut GraalIsolateThread,
    script: &str,
    inputs: HashMap<String, InputValue>,
) -> Result<Stream, DataWeaveError> {
    let script_owned = script.to_string();
    let inputs_json = serialize_inputs(inputs)?;

    let context = Arc::new(StreamContext {
        chunks: Mutex::new(Vec::new()),
        metadata: Mutex::new(None),
    });

    let context_clone = Arc::clone(&context);
    let lib_clone = Arc::clone(&lib);
    let isolate_send = SendIsolatePtr(isolate);

    let handle = thread::spawn(move || {
        // Extract pointer inside closure to ensure wrapper is moved
        let isolate_ptr = isolate_send.as_ptr();
        let mut worker_thread: *mut GraalIsolateThread = ptr::null_mut();

        unsafe {
            let rc = (lib_clone.graal_attach_thread)(
                isolate_ptr,
                &mut worker_thread as *mut *mut GraalIsolateThread,
            );

            if rc != 0 {
                let mut meta = context_clone.metadata.lock().unwrap();
                *meta = Some(format!(
                    r#"{{"success": false, "error": "Failed to attach worker thread (code {})"}}"#,
                    rc
                ));
                return;
            }

            let script_cstr = CString::new(script_owned).unwrap();
            let inputs_cstr = CString::new(inputs_json).unwrap();

            // Callback context
            let callback_ctx = Arc::clone(&context_clone);

            unsafe extern "C" fn write_callback(
                ctx: *mut c_void,
                buffer: *const c_char,
                length: c_int,
            ) -> c_int {
                let context = &*(ctx as *const Arc<StreamContext>);
                let data =
                    std::slice::from_raw_parts(buffer as *const u8, length as usize).to_vec();
                context.chunks.lock().unwrap().push(data);
                0
            }

            let ctx_ptr = &callback_ctx as *const Arc<StreamContext> as *mut c_void;

            let result_ptr = (lib_clone.run_script_callback)(
                worker_thread,
                script_cstr.as_ptr(),
                inputs_cstr.as_ptr(),
                Some(write_callback),
                ctx_ptr,
            );

            if !result_ptr.is_null() {
                let c_str = CStr::from_ptr(result_ptr);
                if let Ok(s) = c_str.to_str() {
                    let mut meta = context_clone.metadata.lock().unwrap();
                    *meta = Some(s.to_string());
                }
                (lib_clone.free_cstring)(worker_thread, result_ptr);
            }

            (lib_clone.graal_detach_thread)(worker_thread);
        }
    });

    // Wait for thread to complete
    handle.join().map_err(|_| {
        DataWeaveError::StreamError("Worker thread panicked".to_string())
    })?;

    // Collect results
    let chunks = context.chunks.lock().unwrap().clone();
    let metadata_json = context
        .metadata
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(|| r#"{"success": false, "error": "No metadata received"}"#.to_string());

    let metadata = StreamingResult::from_json(&metadata_json)?;

    let mut stream = Stream::new();
    for chunk in chunks {
        stream.push_chunk(chunk);
    }
    stream.set_metadata(metadata);

    Ok(stream)
}

struct TransformContext<I: Iterator<Item = Vec<u8>>> {
    input_iter: Mutex<I>,
    output_chunks: Mutex<Vec<Vec<u8>>>,
    metadata: Mutex<Option<String>>,
}

/// Create a bidirectional streaming execution
pub fn create_transform_stream<I>(
    lib: Arc<NativeLib>,
    isolate: *mut GraalIsolate,
    script: &str,
    input_stream: I,
    input_name: &str,
    input_mime_type: &str,
    input_charset: Option<&str>,
    inputs: HashMap<String, InputValue>,
) -> Result<Stream, DataWeaveError>
where
    I: Iterator<Item = Vec<u8>> + Send + 'static,
{
    let script_owned = script.to_string();
    let input_name_owned = input_name.to_string();
    let input_mime_type_owned = input_mime_type.to_string();
    let input_charset_owned = input_charset.map(|s| s.to_string());
    let inputs_json = serialize_inputs(inputs)?;

    let context = Arc::new(TransformContext {
        input_iter: Mutex::new(input_stream),
        output_chunks: Mutex::new(Vec::new()),
        metadata: Mutex::new(None),
    });

    let context_clone = Arc::clone(&context);
    let lib_clone = Arc::clone(&lib);
    let isolate_send = SendIsolatePtr(isolate);

    let handle = thread::spawn(move || {
        // Extract pointer inside closure to ensure wrapper is moved
        let isolate_ptr = isolate_send.as_ptr();
        let mut worker_thread: *mut GraalIsolateThread = ptr::null_mut();

        unsafe {
            let rc = (lib_clone.graal_attach_thread)(
                isolate_ptr,
                &mut worker_thread as *mut *mut GraalIsolateThread,
            );

            if rc != 0 {
                let mut meta = context_clone.metadata.lock().unwrap();
                *meta = Some(format!(
                    r#"{{"success": false, "error": "Failed to attach worker thread (code {})"}}"#,
                    rc
                ));
                return;
            }

            let script_cstr = CString::new(script_owned).unwrap();
            let inputs_cstr = CString::new(inputs_json).unwrap();
            let input_name_cstr = CString::new(input_name_owned).unwrap();
            let input_mime_type_cstr = CString::new(input_mime_type_owned).unwrap();
            let input_charset_cstr = input_charset_owned.map(|s| CString::new(s).unwrap());

            unsafe extern "C" fn read_callback<I: Iterator<Item = Vec<u8>>>(
                ctx: *mut c_void,
                buffer: *mut c_char,
                buf_size: c_int,
            ) -> c_int {
                let context = &*(ctx as *const Arc<TransformContext<I>>);
                let mut iter = context.input_iter.lock().unwrap();

                if let Some(data) = iter.next() {
                    let n = data.len().min(buf_size as usize);
                    std::ptr::copy_nonoverlapping(data.as_ptr(), buffer as *mut u8, n);
                    n as c_int
                } else {
                    0 // EOF
                }
            }

            unsafe extern "C" fn write_callback<I: Iterator<Item = Vec<u8>>>(
                ctx: *mut c_void,
                buffer: *const c_char,
                length: c_int,
            ) -> c_int {
                let context = &*(ctx as *const Arc<TransformContext<I>>);
                let data =
                    std::slice::from_raw_parts(buffer as *const u8, length as usize).to_vec();
                context.output_chunks.lock().unwrap().push(data);
                0
            }

            let ctx_ptr = &context_clone as *const Arc<TransformContext<I>> as *mut c_void;

            let result_ptr = (lib_clone.run_script_input_output_callback)(
                worker_thread,
                script_cstr.as_ptr(),
                inputs_cstr.as_ptr(),
                input_name_cstr.as_ptr(),
                input_mime_type_cstr.as_ptr(),
                input_charset_cstr
                    .as_ref()
                    .map(|s| s.as_ptr())
                    .unwrap_or(ptr::null()),
                Some(read_callback::<I>),
                Some(write_callback::<I>),
                ctx_ptr,
            );

            if !result_ptr.is_null() {
                let c_str = CStr::from_ptr(result_ptr);
                if let Ok(s) = c_str.to_str() {
                    let mut meta = context_clone.metadata.lock().unwrap();
                    *meta = Some(s.to_string());
                }
                (lib_clone.free_cstring)(worker_thread, result_ptr);
            }

            (lib_clone.graal_detach_thread)(worker_thread);
        }
    });

    // Wait for thread to complete
    handle.join().map_err(|_| {
        DataWeaveError::StreamError("Worker thread panicked".to_string())
    })?;

    // Collect results
    let chunks = context.output_chunks.lock().unwrap().clone();
    let metadata_json = context
        .metadata
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(|| r#"{"success": false, "error": "No metadata received"}"#.to_string());

    let metadata = StreamingResult::from_json(&metadata_json)?;

    let mut stream = Stream::new();
    for chunk in chunks {
        stream.push_chunk(chunk);
    }
    stream.set_metadata(metadata);

    Ok(stream)
}

fn serialize_inputs(inputs: HashMap<String, InputValue>) -> Result<String, DataWeaveError> {
    let mut json_inputs = serde_json::Map::new();

    for (key, value) in inputs {
        json_inputs.insert(key, value.to_json_value()?);
    }

    Ok(serde_json::to_string(&json_inputs)?)
}
