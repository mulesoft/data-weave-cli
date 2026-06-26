//! Low-level FFI bindings to the DataWeave native library.
//!
//! This module contains opaque GraalVM types, extern "C" function declarations,
//! and the isolate lifecycle management (initialization, thread attachment).

use std::os::raw::{c_char, c_int, c_void};
use std::sync::Once;

use crate::error::{Error, Result};

// --- Opaque GraalVM types (mirrors graal_isolate.h) ---

#[repr(C)]
pub(crate) struct GraalIsolate {
    _private: [u8; 0],
}

#[repr(C)]
pub(crate) struct GraalIsolateThread {
    _private: [u8; 0],
}

// --- External C functions from the native library ---

extern "C" {
    pub(crate) fn graal_create_isolate(
        params: *mut c_void,
        isolate: *mut *mut GraalIsolate,
        thread: *mut *mut GraalIsolateThread,
    ) -> c_int;

    pub(crate) fn graal_attach_thread(
        isolate: *mut GraalIsolate,
        thread: *mut *mut GraalIsolateThread,
    ) -> c_int;

    pub(crate) fn graal_detach_thread(thread: *mut GraalIsolateThread) -> c_int;

    pub(crate) fn run_script(
        thread: *mut GraalIsolateThread,
        script: *const c_char,
        inputs_json: *const c_char,
    ) -> *mut c_char;

    pub(crate) fn free_cstring(thread: *mut GraalIsolateThread, pointer: *mut c_char);

    pub(crate) fn run_script_callback(
        thread: *mut GraalIsolateThread,
        script: *const c_char,
        inputs_json: *const c_char,
        callback: extern "C" fn(*mut c_void, *const c_char, c_int) -> c_int,
        ctx: *mut c_void,
    ) -> *mut c_char;

    pub(crate) fn run_script_input_output_callback(
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

// --- Isolate lifecycle ---

/// Process-wide GraalVM isolate. Created lazily on first call; subsequent calls attach
/// the current OS thread to it. Pointer is shared across threads but only mutated once.
static ISOLATE_INIT: Once = Once::new();
static mut ISOLATE_PTR: *mut GraalIsolate = std::ptr::null_mut();
static mut ISOLATE_INIT_RC: c_int = 0;

/// Ensures the process-wide GraalVM isolate is created. Returns a pointer to it.
pub(crate) fn ensure_isolate() -> Result<*mut GraalIsolate> {
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

/// RAII guard that attaches the current thread on construction and detaches on drop.
pub(crate) struct AttachedThread {
    thread: *mut GraalIsolateThread,
}

impl AttachedThread {
    pub(crate) fn new() -> Result<Self> {
        let isolate = ensure_isolate()?;
        let mut thread: *mut GraalIsolateThread = std::ptr::null_mut();
        let rc = unsafe { graal_attach_thread(isolate, &mut thread) };
        if rc != 0 || thread.is_null() {
            return Err(Error::NullPointer);
        }
        Ok(AttachedThread { thread })
    }

    pub(crate) fn as_ptr(&self) -> *mut GraalIsolateThread {
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
