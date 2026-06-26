//! Low-level FFI bindings to the DataWeave native library

use std::os::raw::{c_char, c_int, c_void};
use std::path::Path;

use crate::error::DataWeaveError;

/// Opaque GraalVM isolate handle
#[repr(C)]
pub struct GraalIsolate {
    _private: [u8; 0],
}

/// Opaque GraalVM isolate thread handle
#[repr(C)]
pub struct GraalIsolateThread {
    _private: [u8; 0],
}

/// Type for write callback: int (*WriteCallback)(void *ctx, const char *buffer, int length)
pub type WriteCallback =
    unsafe extern "C" fn(ctx: *mut c_void, buffer: *const c_char, length: c_int) -> c_int;

/// Type for read callback: int (*ReadCallback)(void *ctx, char *buffer, int bufferSize)
pub type ReadCallback =
    unsafe extern "C" fn(ctx: *mut c_void, buffer: *mut c_char, buf_size: c_int) -> c_int;

/// Native library function pointers
pub struct NativeLib {
    #[allow(dead_code)]
    lib: libloading::Library,

    pub graal_create_isolate: unsafe extern "C" fn(
        params: *mut c_void,
        isolate: *mut *mut GraalIsolate,
        thread: *mut *mut GraalIsolateThread,
    ) -> c_int,

    pub graal_attach_thread: unsafe extern "C" fn(
        isolate: *mut GraalIsolate,
        thread: *mut *mut GraalIsolateThread,
    ) -> c_int,

    pub graal_detach_thread: unsafe extern "C" fn(thread: *mut GraalIsolateThread) -> c_int,

    pub graal_tear_down_isolate: unsafe extern "C" fn(thread: *mut GraalIsolateThread) -> c_int,

    pub run_script: unsafe extern "C" fn(
        thread: *mut GraalIsolateThread,
        script: *const c_char,
        inputs_json: *const c_char,
    ) -> *mut c_char,

    pub free_cstring:
        unsafe extern "C" fn(thread: *mut GraalIsolateThread, ptr: *mut c_char) -> (),

    pub run_script_callback: unsafe extern "C" fn(
        thread: *mut GraalIsolateThread,
        script: *const c_char,
        inputs_json: *const c_char,
        write_callback: Option<WriteCallback>,
        ctx: *mut c_void,
    ) -> *mut c_char,

    pub run_script_input_output_callback: unsafe extern "C" fn(
        thread: *mut GraalIsolateThread,
        script: *const c_char,
        inputs_json: *const c_char,
        input_name: *const c_char,
        input_mime_type: *const c_char,
        input_charset: *const c_char,
        read_callback: Option<ReadCallback>,
        write_callback: Option<WriteCallback>,
        ctx: *mut c_void,
    ) -> *mut c_char,
}

impl NativeLib {
    pub fn load(path: &Path) -> Result<Self, DataWeaveError> {
        unsafe {
            let lib = libloading::Library::new(path)
                .map_err(|e| DataWeaveError::LibraryLoad(e.to_string()))?;

            let graal_create_isolate = *lib
                .get::<unsafe extern "C" fn(
                    *mut c_void,
                    *mut *mut GraalIsolate,
                    *mut *mut GraalIsolateThread,
                ) -> c_int>(b"graal_create_isolate\0")
                .map_err(|e| DataWeaveError::SymbolNotFound(format!("graal_create_isolate: {}", e)))?;

            let graal_attach_thread = *lib
                .get::<unsafe extern "C" fn(*mut GraalIsolate, *mut *mut GraalIsolateThread) -> c_int>(
                    b"graal_attach_thread\0",
                )
                .map_err(|e| DataWeaveError::SymbolNotFound(format!("graal_attach_thread: {}", e)))?;

            let graal_detach_thread = *lib
                .get::<unsafe extern "C" fn(*mut GraalIsolateThread) -> c_int>(
                    b"graal_detach_thread\0",
                )
                .map_err(|e| DataWeaveError::SymbolNotFound(format!("graal_detach_thread: {}", e)))?;

            let graal_tear_down_isolate = *lib
                .get::<unsafe extern "C" fn(*mut GraalIsolateThread) -> c_int>(
                    b"graal_tear_down_isolate\0",
                )
                .map_err(|e| DataWeaveError::SymbolNotFound(format!("graal_tear_down_isolate: {}", e)))?;

            let run_script = *lib
                .get::<unsafe extern "C" fn(
                    *mut GraalIsolateThread,
                    *const c_char,
                    *const c_char,
                ) -> *mut c_char>(b"run_script\0")
                .map_err(|e| DataWeaveError::SymbolNotFound(format!("run_script: {}", e)))?;

            let free_cstring = *lib
                .get::<unsafe extern "C" fn(*mut GraalIsolateThread, *mut c_char)>(
                    b"free_cstring\0",
                )
                .map_err(|e| DataWeaveError::SymbolNotFound(format!("free_cstring: {}", e)))?;

            let run_script_callback = *lib
                .get::<unsafe extern "C" fn(
                    *mut GraalIsolateThread,
                    *const c_char,
                    *const c_char,
                    Option<WriteCallback>,
                    *mut c_void,
                ) -> *mut c_char>(b"run_script_callback\0")
                .map_err(|e| DataWeaveError::SymbolNotFound(format!("run_script_callback: {}", e)))?;

            let run_script_input_output_callback = *lib
                .get::<unsafe extern "C" fn(
                    *mut GraalIsolateThread,
                    *const c_char,
                    *const c_char,
                    *const c_char,
                    *const c_char,
                    *const c_char,
                    Option<ReadCallback>,
                    Option<WriteCallback>,
                    *mut c_void,
                ) -> *mut c_char>(b"run_script_input_output_callback\0")
                .map_err(|e| {
                    DataWeaveError::SymbolNotFound(format!("run_script_input_output_callback: {}", e))
                })?;

            Ok(Self {
                lib,
                graal_create_isolate,
                graal_attach_thread,
                graal_detach_thread,
                graal_tear_down_isolate,
                run_script,
                free_cstring,
                run_script_callback,
                run_script_input_output_callback,
            })
        }
    }
}

unsafe impl Send for NativeLib {}
unsafe impl Sync for NativeLib {}
