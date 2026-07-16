# native-lib Architecture

This document explains how `native-lib` packages the DataWeave runtime as a C-callable shared library and how the Python, Go, and Rust bindings drive it. For end-user API examples see [README.md](README.md); this document focuses on *what is happening underneath* — native compilation, the FFI surface, isolate/thread management, memory ownership, and streaming.

## 1. The big picture

```
   ┌─ Host process (Python | Go | Rust) ───────────────────────────────┐
   │                                                                   │
   │   user code                                                       │
   │      │                                                            │
   │      │ language-specific binding                                  │
   │      ▼                                                            │
   │   ┌────────────────────────────────────────────────────────────┐  │
   │   │  dwlib.{dylib|so|dll}   ── single shared library ──        │  │
   │   │                                                            │  │
   │   │   GraalVM-compiled Java code:                              │  │
   │   │     · NativeLib.run_script (@CEntryPoint)                  │  │
   │   │     · NativeLib.run_script_callback                        │  │
   │   │     · NativeLib.run_script_input_output_callback           │  │
   │   │     · NativeLib.free_cstring                               │  │
   │   │     · graal_create_isolate / graal_attach_thread / …       │  │
   │   │                                                            │  │
   │   │   Embedded inside each isolate:                            │  │
   │   │     · ScriptRuntime  (singleton, lazy)                     │  │
   │   │     · DataWeave parser, compiler, runtime, modules         │  │
   │   └────────────────────────────────────────────────────────────┘  │
   └───────────────────────────────────────────────────────────────────┘
```

There is no JVM. The DataWeave runtime is *ahead-of-time* compiled into a single shared library by GraalVM Native Image. Every language binding loads that one `.dylib`/`.so`/`.dll` and calls into it through plain C ABI functions.

## 2. Native compilation (GraalVM Native Image)

### 2.1 Sources

Java entry points live in `native-lib/src/main/java/org/mule/weave/lib/`:

- `NativeLib.java` — every C-exported function. Each one is annotated with `@CEntryPoint(name = "...")`, which makes Native Image emit it as a C symbol with that exact name.
- `ScriptRuntime.java` — the actual DataWeave executor; hides parser/compiler/registry setup behind `getInstance()`.
- `StreamSession.java`, `InputStreamSession.java`, `NativeCallbacks.java` — helpers used by the streaming entry points (callback function-pointer types, session state).

The DataWeave runtime itself is a Maven dependency (`org.mule.weave:runtime`, `core-modules`, `parser`, `wlang`) pulled in by `native-lib/build.gradle`.

### 2.2 The Gradle build

Two plugins matter:

- **`org.graalvm.buildtools.native`** — wraps the `native-image` tool in a `nativeCompile` task.
- A custom block in `native-lib/build.gradle` that configures Native Image flags.

Key build.gradle pieces:

```groovy
graalvmNative {
  binaries {
    main {
      sharedLibrary = true              // produce dwlib.{dylib|so|dll}, not an executable
      fallback     = false              // pure native image, no JVM fallback
      useFatJar    = true               // single classpath jar for native-image
      buildArgs.add('-H:Name=dwlib')    // output name
      buildArgs.add('--initialize-at-build-time=…')
      buildArgs.add('-H:+AddAllCharsets')
      buildArgs.add('-H:+IncludeAllLocales')
      // …memory, locale, charset, debugging knobs
    }
  }
}
```

The `nativeCompileClasspathJar` task is reconfigured to substitute two `META-INF/services` files (`org.mule.weave.v2.module.DataFormat`, `org.mule.weave.v2.parser.phase.ModuleLoader`). These ServiceLoader registrations need a curated set of providers in the AOT image; the project's own copies under `src/main/resources/META-INF/services/…` replace whatever the dependencies ship.

### 2.3 What native-image produces

`./gradlew :native-lib:nativeCompile` writes everything to `native-lib/build/native/nativeCompile/`:

| File | Purpose |
| --- | --- |
| `dwlib.dylib` (`.so`/`.dll`) | The shared library itself. ~100 MB; contains the DataWeave runtime, the JDK pieces it transitively reaches, and Substrate VM. |
| `dwlib.h` | Declarations for the `@CEntryPoint` functions (`run_script`, `free_cstring`, `run_script_callback`, `run_script_input_output_callback`). |
| `graal_isolate.h` | Declarations for the GraalVM isolate API (`graal_create_isolate`, `graal_attach_thread`, `graal_detach_thread`, `graal_tear_down_isolate`, `graal_get_current_thread`). |
| `dwlib_dynamic.h`, `graal_isolate_dynamic.h` | Function-pointer-table variants for `dlopen`-style loading. We use the static variants. |

The compiled binary is fully self-contained: there is no `JAVA_HOME` / classpath / module path at runtime.

### 2.4 Library naming quirk and the symlink task

GraalVM emits the library as **`dwlib.dylib`** (no `lib` prefix). Python loads it by absolute path so it doesn't care, but the system linker resolves `-ldwlib` by looking for `libdwlib.dylib` / `libdwlib.so`. To support both, a `symlinkNativeLibForLinking` task creates `libdwlib.*` symlinks alongside the originals after each `nativeCompile`:

```groovy
tasks.register('symlinkNativeLibForLinking') {
  dependsOn tasks.named('nativeCompile')
  doLast {
    nativeDir.listFiles()?.findAll { it.name.startsWith('dwlib.') && !it.name.endsWith('.h') }?.each { src ->
      def link = new File(src.parentFile, "lib${src.name}")
      if (!link.exists()) {
        java.nio.file.Files.createSymbolicLink(link.toPath(), src.toPath().fileName)
      }
    }
  }
}
```

`goTest` and `rustTest` depend on this task, so by the time the linker runs both `dwlib.dylib` and `libdwlib.dylib` exist.

### 2.5 Locating `go` and `cargo` from Gradle

The Gradle daemon's `PATH` doesn't necessarily include `/opt/homebrew/bin` or `~/.cargo/bin`. `build.gradle` defines a small helper:

```groovy
def resolveTool = { String tool, List<String> extraDirs ->
  // Override via -PgoExe=… or GO_EXE=…; otherwise scan PATH + extraDirs.
}
def goExe    = resolveTool('go',    ['/opt/homebrew/bin', '/usr/local/bin'])
def cargoExe = resolveTool('cargo', ["${System.getenv('HOME')}/.cargo/bin", '/opt/homebrew/bin'])
```

`Exec` tasks invoke the resolved absolute paths, so the daemon's PATH doesn't matter.

## 3. The C ABI — what's exported

```c
// from dwlib.h + graal_isolate.h

// Isolate / thread lifecycle (from graal_isolate.h):
int graal_create_isolate(graal_create_isolate_params_t* params,
                         graal_isolate_t** isolate,
                         graal_isolatethread_t** thread);
int graal_attach_thread (graal_isolate_t* isolate,
                         graal_isolatethread_t** thread);
int graal_detach_thread (graal_isolatethread_t* thread);
int graal_tear_down_isolate(graal_isolatethread_t* thread);

// DataWeave entry points (from dwlib.h):
char* run_script(graal_isolatethread_t* thread,
                 const char* script,
                 const char* inputsJson);

void free_cstring(graal_isolatethread_t* thread, char* pointer);

char* run_script_callback(graal_isolatethread_t* thread,
                          const char* script,
                          const char* inputsJson,
                          int (*write_cb)(void* ctx, const char* buf, int len),
                          void* ctx);

char* run_script_input_output_callback(graal_isolatethread_t* thread,
                                       const char* script,
                                       const char* inputsJson,
                                       const char* inputName,
                                       const char* inputMimeType,
                                       const char* inputCharset,
                                       int (*read_cb) (void* ctx, char* buf, int bufSize),
                                       int (*write_cb)(void* ctx, const char* buf, int len),
                                       void* ctx);
```

Three things to remember about this surface:

1. **Every entry point takes a `graal_isolatethread_t*`**. Passing `NULL` is **not legal** — Native Image aborts with `Failed to enter the specified IsolateThread context`. Each binding has to call `graal_create_isolate` once and `graal_attach_thread` for every additional OS thread before calling DataWeave from it.
2. **Returned `char*` are heap-allocated by Native Image** and must be released with `free_cstring(thread, ptr)`. Using `libc` `free()` is undefined behaviour.
3. **Inputs are a JSON envelope, not raw values.** Each binding name maps to `{ "content": <base64 bytes>, "mimeType": "...", "charset": "...", "properties": {...} }`. The Java side base64-decodes content and feeds it to DataWeave with that mime type/charset.

### Result envelope

`run_script` returns a JSON document:

```json
{
  "success":  true,
  "result":   "<base64 of the script output>",
  "binary":   false,
  "mimeType": "application/json",
  "charset":  "UTF-8"
}
```

On failure: `{"success": false, "error": "…"}`.

The streaming entry points instead deliver chunks via the write callback and return only the metadata envelope (no `result` field).

## 4. The Isolate model — what every binding has to do

GraalVM Native Image runs Java code inside an *isolate* — a self-contained heap with its own GC. A *thread* must be attached to an isolate to call any `@CEntryPoint`. A correct call sequence is therefore:

```
graal_create_isolate(...)        // once per process, gives you isolate + first thread
graal_attach_thread(isolate, &t) // for every additional OS thread that will call in
…
run_script(t, …)
free_cstring(t, ptr)
…
graal_detach_thread(t)
```

A couple of properties shape the bindings:

- **One isolate per process** is sufficient and cheap. We create it lazily on first use and never tear it down — the OS reclaims memory at exit.
- **An attached thread is bound to a single OS thread.** If a runtime moves work across OS threads (Go's goroutine scheduler, work-stealing thread pools), it must lock the work to a single OS thread for the duration of the call, or attach/detach around each call.
- **Re-attaching is cheap; sharing a thread handle across goroutines/threads is wrong.**

## 5. Python binding (`native-lib/python`)

### 5.1 Loading the library

`dataweave/__init__.py` searches a list of candidate paths, in order:

1. `$DATAWEAVE_NATIVE_LIB` if set.
2. The bundled `native/dwlib.{dylib|so|dll}` packaged inside the wheel.
3. `native-lib/build/native/nativeCompile/` (dev workflow).
4. The current working directory.

It loads the chosen file with `ctypes.CDLL(path)` — no symbol prefix concerns since we go by full path.

### 5.2 Binding C signatures with `ctypes`

Python defines opaque structs to model `graal_isolate_t` and `graal_isolatethread_t`:

```python
class graal_isolate_t(ctypes.Structure): pass
class graal_isolatethread_t(ctypes.Structure): pass

self._graal_isolate_t_ptr       = ctypes.POINTER(graal_isolate_t)
self._graal_isolatethread_t_ptr = ctypes.POINTER(graal_isolatethread_t)
```

Each function then gets `argtypes`/`restype` set explicitly:

```python
self._lib.run_script.argtypes = [self._graal_isolatethread_t_ptr, ctypes.c_char_p, ctypes.c_char_p]
self._lib.run_script.restype  = ctypes.c_void_p
```

`restype = c_void_p` is intentional — receiving a `c_char_p` would copy bytes and we'd lose the original pointer needed for `free_cstring`.

### 5.3 Isolate lifecycle

`DataWeave.initialize()` calls `graal_create_isolate(NULL, &isolate, &thread)` once and stores both pointers. The same `_thread` is reused for all subsequent calls from the main thread.

For streaming, where the host caller might be on a worker thread, Python attaches a fresh thread:

```python
worker_thread = self._graal_isolatethread_t_ptr()
self._lib.graal_attach_thread(self._isolate, ctypes.byref(worker_thread))
…
self._lib.graal_detach_thread(worker_thread)
```

### 5.4 Calling `run_script`

1. JSON-encode `{"name": {"content": base64(value), "mimeType": …}}`.
2. `ptr = lib.run_script(thread, script_bytes, inputs_bytes)` — returns `c_void_p`.
3. `ctypes.string_at(ptr).decode("utf-8")` → result envelope.
4. `lib.free_cstring(thread, ptr)`.
5. Parse JSON, base64-decode `result` into bytes.

### 5.5 Streaming with callbacks

`run_streaming` and `run_transform` use `WRITE_CALLBACK = CFUNCTYPE(c_int, c_void_p, c_char_p, c_int)`. A Python function is wrapped as a `WRITE_CALLBACK` instance and passed to `run_script_callback`. Each invocation `chunks.append(string_at(buf, length))` and returns `0`.

For bidirectional `run_transform`, a second callback (`READ_CALLBACK = CFUNCTYPE(c_int, c_void_p, POINTER(c_char), c_int)`) pulls bytes out of an iterable and `memmove`s them into the buffer the native side provided.

The `run_script_input_output_callback` invocation runs on a background thread (so the read callback can pull from a generator while the write callback emits chunks). That thread must `graal_attach_thread` first, then detach when done.

## 6. Go binding (`native-lib/go`)

### 6.1 cgo and `#cgo`

Go bindings sit on top of cgo. The directives at the top of `dataweave.go` tell cgo where to find headers and how to link:

```go
/*
#cgo CFLAGS: -I${SRCDIR}/../build/native/nativeCompile
#cgo darwin LDFLAGS: -L${SRCDIR}/../build/native/nativeCompile -ldwlib
#cgo linux  LDFLAGS: -L${SRCDIR}/../build/native/nativeCompile -ldwlib
#cgo windows LDFLAGS: -L${SRCDIR}/../build/native/nativeCompile -ldwlib

#include <stdlib.h>
#include <string.h>
#include "graal_isolate.h"
extern char* run_script(graal_isolatethread_t* thread, const char* script, const char* inputsJson);
extern void  free_cstring(graal_isolatethread_t* thread, char* pointer);
…
*/
import "C"
```

`-ldwlib` requires `libdwlib.dylib` to exist — that's why the Gradle `symlinkNativeLibForLinking` task is on the dependency chain.

### 6.2 Isolate management with `runtime.LockOSThread`

cgo code can be executed from any goroutine; the goroutine scheduler may move that goroutine between OS threads. GraalVM, however, requires every native call to come from the same OS thread the GraalVM thread was attached to. The Go binding solves this with two mechanisms:

```go
var (
    isolateOnce    sync.Once
    globalIsolate  *C.graal_isolate_t
    isolateInitErr error
)

func ensureIsolate() error {
    isolateOnce.Do(func() {
        runtime.LockOSThread()
        defer runtime.UnlockOSThread()
        var isolate *C.graal_isolate_t
        var thread  *C.graal_isolatethread_t
        if rc := C.graal_create_isolate(nil, &isolate, &thread); rc != 0 {
            isolateInitErr = fmt.Errorf("graal_create_isolate failed: %d", int(rc))
            return
        }
        globalIsolate = isolate
        C.graal_detach_thread(thread)   // detach the bootstrap thread
    })
    return isolateInitErr
}
```

Each call to DataWeave then attaches the *current* OS thread for the duration of the call:

```go
func Run(script string, inputs map[string]interface{}) (*ExecutionResult, error) {
    runtime.LockOSThread()        // pin this goroutine to its OS thread
    defer runtime.UnlockOSThread()

    thread, err := attachCurrentThread() // graal_attach_thread(globalIsolate, &t)
    if err != nil { return nil, err }
    defer C.graal_detach_thread(thread)

    cScript := C.CString(script);  defer C.free(unsafe.Pointer(cScript))
    cInputs := C.CString(inputsJson); defer C.free(unsafe.Pointer(cInputs))

    cResult := C.run_script(thread, cScript, cInputs)
    if cResult == nil { return nil, fmt.Errorf("run_script returned NULL") }
    defer C.free_cstring(thread, cResult)

    return parseExecutionResult(C.GoString(cResult))
}
```

`runtime.LockOSThread` is the critical line — without it the Go scheduler would be free to pull this goroutine off the thread mid-call, leaving GraalVM with no attached thread.

### 6.3 Streaming with `//export` callbacks

cgo can hand C a function pointer to a Go function only via `//export`. `streaming_callbacks.go`:

```go
//export writeCallbackBridge
func writeCallbackBridge(ctxPtr unsafe.Pointer, buf *C.char, length C.int) C.int { … }

//export readCallbackBridge
func readCallbackBridge (ctxPtr unsafe.Pointer, buf *C.char, bufSize C.int) C.int { … }
```

Native Image cannot pass Go pointers to C (cgo forbids it), so the binding stores the per-call state in a Go map keyed by an integer "handle":

```go
var (
    contextMu      sync.Mutex
    contextCounter uintptr
    contextMap     = make(map[uintptr]*callbackContext)
)
func registerContext(ctx *callbackContext) uintptr { … }
func lookupContext  (handle uintptr) *callbackContext { … }
func unregisterContext(handle uintptr) { … }
```

The handle is what we pass as the `void* ctx` argument; the exported Go callback turns it back into a `*callbackContext` and pushes the chunk onto a Go channel.

The streaming entrypoints run on a goroutine (so the caller can iterate `<-result.Chunks` while DataWeave produces output). That goroutine `LockOSThread`s, attaches its thread, and detaches when the run completes.

### 6.4 Memory ownership

- Strings to C: `C.CString` allocates with `malloc`; we always pair it with `defer C.free(...)`.
- Strings from C: `C.GoString` *copies* into a Go string; the original `*C.char` from DataWeave still has to be released with `C.free_cstring(thread, ptr)`.
- Bytes from C in callbacks: `C.GoBytes(buf, length)` copies into a fresh Go slice. We send the *copy* on the channel; the C buffer is invalidated as soon as the callback returns.

## 7. Rust binding (`native-lib/rust`)

### 7.1 `build.rs` and linker setup

```rust
fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let lib_dir = format!("{}/../build/native/nativeCompile", manifest_dir);

    println!("cargo:rustc-link-search=native={}", lib_dir);
    println!("cargo:rustc-link-lib=dylib=dwlib");

    #[cfg(target_os = "macos")]
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir);
    #[cfg(target_os = "linux")]
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir);
}
```

The `-rpath` flag bakes the library path into the test binary so cargo doesn't need `LD_LIBRARY_PATH`/`DYLD_LIBRARY_PATH` set — the test executable finds `libdwlib.dylib` next to itself.

### 7.2 Declaring the FFI surface

```rust
#[repr(C)] struct GraalIsolate       { _private: [u8; 0] }
#[repr(C)] struct GraalIsolateThread { _private: [u8; 0] }

extern "C" {
    fn graal_create_isolate(p: *mut c_void,
                            isolate: *mut *mut GraalIsolate,
                            thread:  *mut *mut GraalIsolateThread) -> c_int;
    fn graal_attach_thread(isolate: *mut GraalIsolate,
                           thread:  *mut *mut GraalIsolateThread) -> c_int;
    fn graal_detach_thread(thread: *mut GraalIsolateThread) -> c_int;

    fn run_script(thread: *mut GraalIsolateThread,
                  script: *const c_char, inputs_json: *const c_char) -> *mut c_char;
    fn free_cstring(thread: *mut GraalIsolateThread, pointer: *mut c_char);

    fn run_script_callback(…)            -> *mut c_char;
    fn run_script_input_output_callback(…) -> *mut c_char;
}
```

The opaque zero-sized structs are how Rust models `struct __graal_isolate_t;` (incomplete C types) — you can hold a `*mut` to them but never construct or deref one.

### 7.3 Lazy isolate + RAII attach guard

```rust
static ISOLATE_INIT: Once = Once::new();
static mut ISOLATE_PTR: *mut GraalIsolate = std::ptr::null_mut();

fn ensure_isolate() -> Result<*mut GraalIsolate> {
    ISOLATE_INIT.call_once(|| unsafe {
        let mut isolate = std::ptr::null_mut();
        let mut thread  = std::ptr::null_mut();
        if graal_create_isolate(std::ptr::null_mut(), &mut isolate, &mut thread) == 0 {
            ISOLATE_PTR = isolate;
            graal_detach_thread(thread); // detach the bootstrap thread
        }
    });
    unsafe { if ISOLATE_PTR.is_null() { Err(Error::NullPointer) } else { Ok(ISOLATE_PTR) } }
}

struct AttachedThread { thread: *mut GraalIsolateThread }
impl AttachedThread {
    fn new() -> Result<Self> {
        let isolate = ensure_isolate()?;
        let mut t = std::ptr::null_mut();
        if unsafe { graal_attach_thread(isolate, &mut t) } != 0 || t.is_null() {
            return Err(Error::NullPointer);
        }
        Ok(AttachedThread { thread: t })
    }
    fn as_ptr(&self) -> *mut GraalIsolateThread { self.thread }
}
impl Drop for AttachedThread {
    fn drop(&mut self) { unsafe { graal_detach_thread(self.thread); } }
}
```

`AttachedThread` is the moral equivalent of Go's `runtime.LockOSThread`+`graal_attach_thread`+`defer detach`. Rust threads don't need a "lock to OS thread" call — Rust threads *are* OS threads — but every thread that calls into DataWeave must attach itself first; the `Drop` impl handles cleanup even on panic.

### 7.4 Synchronous `run`

```rust
pub fn run(script: &str, inputs: Option<HashMap<String,Value>>) -> Result<ExecutionResult> {
    let inputs_json = encode_inputs(inputs)?;
    let c_script = CString::new(script)?;
    let c_inputs = CString::new(inputs_json)?;

    let attached = AttachedThread::new()?;
    let thread = attached.as_ptr();
    unsafe {
        let result_ptr = run_script(thread, c_script.as_ptr(), c_inputs.as_ptr());
        if result_ptr.is_null() { return Err(Error::NullPointer); }

        let raw = CStr::from_ptr(result_ptr).to_str().map_err(|_| Error::Utf8Response)?.to_string();
        free_cstring(thread, result_ptr);    // critical: free with the same thread we used
        parse_execution_result(&raw)
    }
}
```

The `attached` guard is dropped at function exit — that's where `graal_detach_thread` runs. The drop ordering is fine because the result string is already copied to an owned `String` before then.

### 7.5 Streaming, `Send`, and the join handle

Streaming runs the FFI call on a `std::thread::spawn`'d thread so that the iterator on the calling side can `recv()` chunks as they arrive:

```rust
struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}

pub struct StreamResult {
    receiver: mpsc::Receiver<Vec<u8>>,
    metadata: Arc<Mutex<Option<StreamingMetadata>>>,
    join:     Mutex<Option<thread::JoinHandle<()>>>,
}

impl StreamResult {
    pub fn metadata(&self) -> Option<StreamingMetadata> {
        if let Some(h) = self.join.lock().unwrap().take() {
            let _ = h.join();   // wait for the FFI worker to write metadata
        }
        self.metadata.lock().unwrap().clone()
    }
}
```

Two non-obvious pieces:

- **`SendPtr<T>`**: raw pointers are not `Send`, so a `*mut WriteCallbackContext` cannot move into a `thread::spawn` closure on its own. `SendPtr` is a transparent wrapper that says "yes, we promise this pointer is safe to send" — needed because the box is created on the spawning thread and consumed (boxed back and dropped) on the worker thread.
- **`join` handle**: the worker writes metadata *after* the channel has been closed (closing the channel ends the iterator). Without joining the worker, `metadata()` could observe `None` because the worker hasn't finished yet. `metadata()` joins on first call, taking the handle out of the Mutex so subsequent calls don't block.

### 7.6 Memory ownership

- Going into C: `CString::new(s)` owns the bytes; `c_script.as_ptr()` is valid as long as `c_script` is.
- Coming out of C: `CStr::from_ptr(p).to_str()` *borrows* the C buffer. We immediately copy with `.to_string()` and then `free_cstring(thread, p)`.
- Callback context: `Box::into_raw(Box::new(ctx))` gives the FFI a stable pointer; on the worker thread we reclaim with `Box::from_raw(ptr)` so it drops cleanly.
- Chunks: `slice::from_raw_parts(buf, length).to_vec()` copies the buffer before sending it on the `mpsc` channel.

## 8. Streaming protocol — what's flowing where

The synchronous `run_script` returns the entire result as a single base64-encoded blob. For large outputs that doubles the memory footprint and gates everything on the runtime finishing. The streaming endpoints add two callbacks that flow data while the script is running:

```
host language ─►  run_script_callback(thread, script, inputs, write_cb, ctx)
                                                                │
                                                                │  for each chunk:
                                                                ▼
                                              write_cb(ctx, buf, len) → 0/-1
                                                                │
                                                                ▼
                  ◄── returns metadata JSON when script completes
```

For input-streaming (`run_transform`/`run_script_input_output_callback`) there is also a `read_cb` that the runtime calls to pull bytes; the binding fills the buffer from a generator/iterator/Reader and returns the byte count (`0` = EOF, `-1` = error).

Across all three languages the glue is the same:

1. Caller wraps user-side data (a Python iterable, a Go `io.Reader`, a Rust `Read`) and a chunk sink (a list, a channel, an `mpsc::Sender`) in a context object.
2. Caller passes a stable handle/pointer to that context as the `void* ctx`.
3. The C-callable callback function looks up the context, copies bytes from the C-supplied buffer into language-native form, and notifies the sink.
4. When the runtime finishes, it returns the metadata envelope; the binding parses it and surfaces it to the user.

## 9. End-to-end: what happens for a single call

Take `dataweave.Run("2 + 2", nil)` from Go:

1. **First call**: `ensureIsolate()` runs `graal_create_isolate`. The isolate pointer is stashed in a package-level variable; the bootstrap thread is detached.
2. `runtime.LockOSThread()` pins the goroutine to its OS thread.
3. `graal_attach_thread(globalIsolate, &thread)` gives us a thread handle for this OS thread.
4. `C.CString("2 + 2")` and `C.CString("{}")` allocate two C strings.
5. `C.run_script(thread, …)` enters the isolate, runs the DataWeave script in the embedded runtime, base64-encodes the result, JSON-wraps it, and returns a `char*` allocated with `UnmanagedMemory.malloc`.
6. We `C.GoString(cResult)` to copy into a Go string, `C.free_cstring(thread, cResult)` to release the native buffer, then `C.free` the input strings.
7. `graal_detach_thread(thread)` releases the GraalVM thread; `runtime.UnlockOSThread()` unpins the goroutine.
8. We JSON-parse `{"success":true,"result":"NA==", …}` and base64-decode `"NA=="` → `[]byte("4")`.

Every binding follows the same skeleton with language-appropriate primitives — `with` block + ctypes for Python, `LockOSThread` + cgo for Go, `Drop` guard + `extern "C"` for Rust.

## 10. Failure modes worth knowing about

| Symptom | What's actually wrong |
| --- | --- |
| `Failed to enter the specified IsolateThread context. (code 2)` | A binding passed `NULL` (or an unattached thread) to a `@CEntryPoint`. Every entry point needs a thread that has been `graal_attach_thread`'d on the *current* OS thread. |
| `ld: library 'dwlib' not found` | The linker path is wrong, or `libdwlib.*` symlink is missing. Ensure `nativeCompile` ran and `symlinkNativeLibForLinking` followed it. |
| `Cannot run program "go"` from Gradle | Gradle daemon's PATH lacks the toolchain. Restart the daemon or set `-PgoExe=/path/to/go`. |
| `no metadata` panic in Rust streaming tests | The FFI worker thread hadn't finished when `metadata()` was called. The `JoinHandle` machinery in `StreamResult` is what prevents that race. |
| Native lib loads but `run_script` segfaults on a worker thread | The worker thread is using a `IsolateThread*` that was attached on *another* OS thread. Each OS thread needs its own attach/detach. |

## 11. Where to look in the source

| Concern | File |
| --- | --- |
| C entry-point definitions | `native-lib/src/main/java/org/mule/weave/lib/NativeLib.java` |
| DataWeave runtime adapter | `native-lib/src/main/java/org/mule/weave/lib/ScriptRuntime.java` |
| Streaming session helpers | `native-lib/src/main/java/org/mule/weave/lib/{Stream,InputStream}Session.java`, `NativeCallbacks.java` |
| Native build configuration | `native-lib/build.gradle` |
| ServiceLoader overrides | `native-lib/src/main/resources/META-INF/services/` |
| Python binding | `native-lib/python/src/dataweave/__init__.py` |
| Go binding | `native-lib/go/dataweave.go`, `native-lib/go/streaming_callbacks.go` |
| Rust binding | `native-lib/rust/src/lib.rs`, `native-lib/rust/build.rs` |
