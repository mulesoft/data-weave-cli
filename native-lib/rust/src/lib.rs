//! # DataWeave Native Library - Rust Bindings
//!
//! Execute DataWeave scripts from Rust via a GraalVM native shared library.
//! Supports buffered execution, output streaming, and bidirectional streaming
//! with constant memory overhead.
//!
//! ## Basic Usage
//!
//! ```no_run
//! use dataweave_native::{DataWeave, InputValue};
//! use std::collections::HashMap;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let dw = DataWeave::new()?;
//!     let result = dw.run("2 + 2", HashMap::new())?;
//!     println!("{}", result.get_string()?);
//!     Ok(())
//! }
//! ```
//!
//! ## Output Streaming
//!
//! ```no_run
//! use dataweave_native::DataWeave;
//! use std::collections::HashMap;
//! use std::io::Write;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let dw = DataWeave::new()?;
//!     let mut stream = dw.run_streaming("output json --- (1 to 10000) map {id: $}", HashMap::new())?;
//!
//!     while let Some(chunk) = stream.next() {
//!         std::io::stdout().write_all(&chunk)?;
//!     }
//!     let metadata = stream.metadata();
//!     Ok(())
//! }
//! ```
//!
//! ## Bidirectional Streaming
//!
//! ```no_run
//! use dataweave_native::DataWeave;
//! use std::collections::HashMap;
//! use std::fs::File;
//! use std::io::Read;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let dw = DataWeave::new()?;
//!     let file = File::open("large.json")?;
//!     let chunks: Vec<Vec<u8>> = vec![/* file chunks */];
//!
//!     let mut stream = dw.run_transform(
//!         "output csv --- payload",
//!         chunks.into_iter(),
//!         "payload",
//!         "application/json",
//!         None,
//!         HashMap::new(),
//!     )?;
//!
//!     while let Some(chunk) = stream.next() {
//!         // Process chunk
//!     }
//!     Ok(())
//! }
//! ```

mod error;
mod ffi;
mod result;
mod streaming;

pub use error::{DataWeaveError, DataWeaveScriptError};
pub use result::{ExecutionResult, StreamingResult};
pub use streaming::Stream;

// Re-export HashMap for convenience
pub use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::path::PathBuf;
use std::ptr;
use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use ffi::{GraalIsolate, GraalIsolateThread, NativeLib};

/// Input value type for DataWeave script execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputValue {
    pub content: Vec<u8>,
    #[serde(rename = "mimeType")]
    pub mime_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub charset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, JsonValue>>,
}

impl InputValue {
    pub fn new(content: Vec<u8>, mime_type: impl Into<String>) -> Self {
        Self {
            content,
            mime_type: mime_type.into(),
            charset: None,
            properties: None,
        }
    }

    pub fn with_charset(mut self, charset: impl Into<String>) -> Self {
        self.charset = Some(charset.into());
        self
    }

    pub fn with_properties(mut self, properties: HashMap<String, JsonValue>) -> Self {
        self.properties = Some(properties);
        self
    }

    fn to_json_value(&self) -> Result<JsonValue, DataWeaveError> {
        let encoded = BASE64.encode(&self.content);
        let mut obj = serde_json::json!({
            "content": encoded,
            "mimeType": self.mime_type,
        });

        if let Some(charset) = &self.charset {
            obj["charset"] = charset.clone().into();
        }

        if let Some(props) = &self.properties {
            obj["properties"] = serde_json::to_value(props)?;
        }

        Ok(obj)
    }
}

/// Main DataWeave runtime interface
///
/// Manages the GraalVM isolate lifecycle and provides methods for script execution.
/// Resources are automatically cleaned up on drop.
pub struct DataWeave {
    lib: Arc<NativeLib>,
    isolate: *mut GraalIsolate,
    thread: *mut GraalIsolateThread,
}

unsafe impl Send for DataWeave {}
unsafe impl Sync for DataWeave {}

impl DataWeave {
    /// Create a new DataWeave runtime instance
    ///
    /// This will load the native library and create a GraalVM isolate.
    /// The library path is determined by:
    /// 1. DATAWEAVE_NATIVE_LIB environment variable
    /// 2. `native/dwlib.{dylib,so,dll}` relative to package
    /// 3. `../build/native/nativeCompile/dwlib.{dylib,so,dll}` for dev builds
    /// 4. Current directory
    pub fn new() -> Result<Self, DataWeaveError> {
        let lib_path = Self::find_library()?;
        let lib = Arc::new(NativeLib::load(&lib_path)?);

        let mut isolate: *mut GraalIsolate = ptr::null_mut();
        let mut thread: *mut GraalIsolateThread = ptr::null_mut();

        unsafe {
            let result = (lib.graal_create_isolate)(
                ptr::null_mut(),
                &mut isolate as *mut *mut GraalIsolate,
                &mut thread as *mut *mut GraalIsolateThread,
            );

            if result != 0 {
                return Err(DataWeaveError::IsolateCreation(result));
            }
        }

        Ok(Self {
            lib,
            isolate,
            thread,
        })
    }

    fn find_library() -> Result<PathBuf, DataWeaveError> {
        let candidates = Self::candidate_paths();

        for path in candidates {
            if path.exists() && path.is_file() {
                return Ok(path);
            }
        }

        Err(DataWeaveError::LibraryNotFound)
    }

    fn candidate_paths() -> Vec<PathBuf> {
        let mut paths = Vec::new();

        if let Ok(env_path) = std::env::var("DATAWEAVE_NATIVE_LIB") {
            if !env_path.is_empty() {
                paths.push(PathBuf::from(env_path));
            }
        }

        // Library names by platform
        #[cfg(target_os = "macos")]
        let lib_name = "dwlib.dylib";
        #[cfg(target_os = "linux")]
        let lib_name = "dwlib.so";
        #[cfg(target_os = "windows")]
        let lib_name = "dwlib.dll";

        // Package native directory
        if let Ok(exe_path) = std::env::current_exe() {
            if let Some(parent) = exe_path.parent() {
                paths.push(parent.join("native").join(lib_name));
            }
        }

        // Dev build location
        paths.push(PathBuf::from("../build/native/nativeCompile").join(lib_name));
        paths.push(PathBuf::from("../../build/native/nativeCompile").join(lib_name));

        // Current directory
        paths.push(PathBuf::from(lib_name));

        paths
    }

    /// Execute a DataWeave script with the given inputs
    ///
    /// # Arguments
    /// * `script` - The DataWeave script source code
    /// * `inputs` - Input bindings as a HashMap
    ///
    /// # Returns
    /// An `ExecutionResult` containing the output or error
    pub fn run(
        &self,
        script: &str,
        inputs: HashMap<String, InputValue>,
    ) -> Result<ExecutionResult, DataWeaveError> {
        let inputs_json = self.serialize_inputs(inputs)?;
        let script_cstr = CString::new(script)?;
        let inputs_cstr = CString::new(inputs_json)?;

        unsafe {
            let result_ptr =
                (self.lib.run_script)(self.thread, script_cstr.as_ptr(), inputs_cstr.as_ptr());

            let result_str = self.decode_and_free(result_ptr)?;
            ExecutionResult::from_json(&result_str)
        }
    }

    /// Execute a script and stream output chunks
    ///
    /// Returns a `Stream` that yields output chunks as they are produced.
    /// Metadata is available after iteration completes.
    pub fn run_streaming(
        &self,
        script: &str,
        inputs: HashMap<String, InputValue>,
    ) -> Result<Stream, DataWeaveError> {
        streaming::create_stream(Arc::clone(&self.lib), self.isolate, self.thread, script, inputs)
    }

    /// Execute a script with a write callback for output
    ///
    /// The callback is invoked for each output chunk. Return 0 for success,
    /// non-zero to abort execution.
    pub fn run_callback<F>(
        &self,
        script: &str,
        write_callback: F,
        inputs: HashMap<String, InputValue>,
    ) -> Result<StreamingResult, DataWeaveError>
    where
        F: FnMut(&[u8]) -> i32,
    {
        let inputs_json = self.serialize_inputs(inputs)?;
        let script_cstr = CString::new(script)?;
        let inputs_cstr = CString::new(inputs_json)?;

        let mut callback = write_callback;
        let callback_ptr = &mut callback as *mut _ as *mut c_void;

        unsafe extern "C" fn trampoline<F>(
            ctx: *mut c_void,
            buffer: *const c_char,
            length: c_int,
        ) -> c_int
        where
            F: FnMut(&[u8]) -> i32,
        {
            let callback = &mut *(ctx as *mut F);
            let data = std::slice::from_raw_parts(buffer as *const u8, length as usize);
            callback(data)
        }

        unsafe {
            let result_ptr = (self.lib.run_script_callback)(
                self.thread,
                script_cstr.as_ptr(),
                inputs_cstr.as_ptr(),
                Some(trampoline::<F>),
                callback_ptr,
            );

            let result_str = self.decode_and_free(result_ptr)?;
            StreamingResult::from_json(&result_str)
        }
    }

    /// Execute a script with bidirectional streaming
    ///
    /// Input is pulled from the provided iterator, output is streamed back.
    /// Returns a `Stream` that yields output chunks.
    pub fn run_transform<I>(
        &self,
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
        streaming::create_transform_stream(
            Arc::clone(&self.lib),
            self.isolate,
            script,
            input_stream,
            input_name,
            input_mime_type,
            input_charset,
            inputs,
        )
    }

    /// Execute a script with read and write callbacks for bidirectional streaming
    pub fn run_input_output_callback<R, W>(
        &self,
        script: &str,
        input_name: &str,
        input_mime_type: &str,
        input_charset: Option<&str>,
        read_callback: R,
        write_callback: W,
        inputs: HashMap<String, InputValue>,
    ) -> Result<StreamingResult, DataWeaveError>
    where
        R: FnMut(usize) -> Vec<u8>,
        W: FnMut(&[u8]) -> i32,
    {
        let inputs_json = self.serialize_inputs(inputs)?;
        let script_cstr = CString::new(script)?;
        let inputs_cstr = CString::new(inputs_json)?;
        let input_name_cstr = CString::new(input_name)?;
        let input_mime_type_cstr = CString::new(input_mime_type)?;
        let input_charset_cstr = input_charset.map(|s| CString::new(s)).transpose()?;

        struct Callbacks<R, W> {
            read: R,
            write: W,
        }

        let mut callbacks = Callbacks {
            read: read_callback,
            write: write_callback,
        };
        let ctx_ptr = &mut callbacks as *mut _ as *mut c_void;

        unsafe extern "C" fn read_trampoline<R, W>(
            ctx: *mut c_void,
            buffer: *mut c_char,
            buf_size: c_int,
        ) -> c_int
        where
            R: FnMut(usize) -> Vec<u8>,
            W: FnMut(&[u8]) -> i32,
        {
            let callbacks = &mut *(ctx as *mut Callbacks<R, W>);
            let data = (callbacks.read)(buf_size as usize);
            if data.is_empty() {
                return 0; // EOF
            }
            let n = data.len().min(buf_size as usize);
            std::ptr::copy_nonoverlapping(data.as_ptr(), buffer as *mut u8, n);
            n as c_int
        }

        unsafe extern "C" fn write_trampoline<R, W>(
            ctx: *mut c_void,
            buffer: *const c_char,
            length: c_int,
        ) -> c_int
        where
            R: FnMut(usize) -> Vec<u8>,
            W: FnMut(&[u8]) -> i32,
        {
            let callbacks = &mut *(ctx as *mut Callbacks<R, W>);
            let data = std::slice::from_raw_parts(buffer as *const u8, length as usize);
            (callbacks.write)(data)
        }

        unsafe {
            let result_ptr = (self.lib.run_script_input_output_callback)(
                self.thread,
                script_cstr.as_ptr(),
                inputs_cstr.as_ptr(),
                input_name_cstr.as_ptr(),
                input_mime_type_cstr.as_ptr(),
                input_charset_cstr
                    .as_ref()
                    .map(|s| s.as_ptr())
                    .unwrap_or(ptr::null()),
                Some(read_trampoline::<R, W>),
                Some(write_trampoline::<R, W>),
                ctx_ptr,
            );

            let result_str = self.decode_and_free(result_ptr)?;
            StreamingResult::from_json(&result_str)
        }
    }

    fn serialize_inputs(
        &self,
        inputs: HashMap<String, InputValue>,
    ) -> Result<String, DataWeaveError> {
        let mut json_inputs = serde_json::Map::new();

        for (key, value) in inputs {
            json_inputs.insert(key, value.to_json_value()?);
        }

        Ok(serde_json::to_string(&json_inputs)?)
    }

    unsafe fn decode_and_free(&self, ptr: *mut c_char) -> Result<String, DataWeaveError> {
        if ptr.is_null() {
            return Err(DataWeaveError::NullResponse);
        }

        let c_str = CStr::from_ptr(ptr);
        let result = c_str
            .to_str()
            .map_err(|e| DataWeaveError::Utf8Error(e))?
            .to_string();

        (self.lib.free_cstring)(self.thread, ptr);

        Ok(result)
    }
}

impl Drop for DataWeave {
    fn drop(&mut self) {
        if !self.thread.is_null() {
            unsafe {
                let _ = (self.lib.graal_tear_down_isolate)(self.thread);
            }
        }
    }
}

/// Auto-convert native Rust types to InputValue
pub trait ToInputValue {
    fn to_input_value(&self) -> Result<InputValue, DataWeaveError>;
}

impl ToInputValue for String {
    fn to_input_value(&self) -> Result<InputValue, DataWeaveError> {
        Ok(InputValue::new(
            self.as_bytes().to_vec(),
            "text/plain",
        ))
    }
}

impl ToInputValue for &str {
    fn to_input_value(&self) -> Result<InputValue, DataWeaveError> {
        Ok(InputValue::new(
            self.as_bytes().to_vec(),
            "text/plain",
        ))
    }
}

impl ToInputValue for i32 {
    fn to_input_value(&self) -> Result<InputValue, DataWeaveError> {
        let json = serde_json::to_string(self)?;
        Ok(InputValue::new(
            json.as_bytes().to_vec(),
            "application/json",
        ))
    }
}

impl ToInputValue for i64 {
    fn to_input_value(&self) -> Result<InputValue, DataWeaveError> {
        let json = serde_json::to_string(self)?;
        Ok(InputValue::new(
            json.as_bytes().to_vec(),
            "application/json",
        ))
    }
}

impl ToInputValue for f64 {
    fn to_input_value(&self) -> Result<InputValue, DataWeaveError> {
        let json = serde_json::to_string(self)?;
        Ok(InputValue::new(
            json.as_bytes().to_vec(),
            "application/json",
        ))
    }
}

impl ToInputValue for bool {
    fn to_input_value(&self) -> Result<InputValue, DataWeaveError> {
        let json = serde_json::to_string(self)?;
        Ok(InputValue::new(
            json.as_bytes().to_vec(),
            "application/json",
        ))
    }
}

impl<T: Serialize> ToInputValue for Vec<T> {
    fn to_input_value(&self) -> Result<InputValue, DataWeaveError> {
        let json = serde_json::to_string(self)?;
        Ok(InputValue::new(
            json.as_bytes().to_vec(),
            "application/json",
        ))
    }
}

impl ToInputValue for JsonValue {
    fn to_input_value(&self) -> Result<InputValue, DataWeaveError> {
        let json = serde_json::to_string(self)?;
        Ok(InputValue::new(
            json.as_bytes().to_vec(),
            "application/json",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_input_value_encoding() {
        let input = InputValue::new(b"test".to_vec(), "text/plain");
        let json = input.to_json_value().unwrap();
        assert_eq!(json["mimeType"], "text/plain");
        assert!(json["content"].is_string());
    }

    #[test]
    fn test_to_input_value_conversions() {
        assert!(25i32.to_input_value().is_ok());
        assert!("test".to_input_value().is_ok());
        assert!(true.to_input_value().is_ok());
        assert!(vec![1, 2, 3].to_input_value().is_ok());
    }
}
