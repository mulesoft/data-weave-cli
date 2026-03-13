"""
DataWeave Python Module

A simple Python wrapper for executing DataWeave scripts via the native library.
This module abstracts all GraalVM and native library complexity, providing a
clean Python API for executing DataWeave scripts with or without inputs.

Basic Usage:
    import dataweave

    result = dataweave.run_script("2 + 2")
    print(result.get_string())

    # Call cleanup() when done to release native resources
    dataweave.cleanup()

Using context manager (recommended for automatic cleanup):
    from dataweave import DataWeave

    with DataWeave() as dw:
        result = dw.run("2 + 2")
        print(result.get_string())
    # Resources are automatically released when exiting the 'with' block

Handling errors:
    import dataweave

    result = dataweave.run_script("1 / 0")
    if not result.success:
        print(f"Error: {result.error}")
    else:
        print(result.get_string())
"""

import base64
import ctypes
import io
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Union


class DataWeaveError(Exception):
    pass


class DataWeaveLibraryNotFoundError(Exception):
    pass


_DEFAULT_CHUNK_SIZE = 8192

# ctypes callback signatures matching NativeCallbacks.WriteCallback / ReadCallback.
# Buffer parameters use c_void_p (not c_char_p) because ctypes gives c_char_p
# special treatment that prevents writing into the buffer.
# int (*WriteCallback)(void *ctx, const char *buffer, int length)
WRITE_CALLBACK = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int)
# int (*ReadCallback)(void *ctx, char *buffer, int bufferSize)
READ_CALLBACK = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int)


_ENV_NATIVE_LIB = "DATAWEAVE_NATIVE_LIB"


@dataclass
class InputValue:
    content: Union[str, bytes]
    mimeType: Optional[str] = None
    charset: Optional[str] = None
    properties: Optional[Dict[str, Union[str, int, bool]]] = None

    def encode_content(self) -> str:
        if isinstance(self.content, bytes):
            raw = self.content
        else:
            raw = self.content.encode(self.charset or "utf-8")
        return base64.b64encode(raw).decode("ascii")


@dataclass
class ExecutionResult:
    success: bool
    result: Optional[str]
    error: Optional[str]
    binary: bool
    mimeType: Optional[str]
    charset: Optional[str]

    def get_bytes(self) -> Optional[bytes]:
        if not self.success or self.result is None:
            return None
        return base64.b64decode(self.result)

    def get_string(self) -> Optional[str]:
        if not self.success or self.result is None:
            return None
        if self.binary:
            return self.result
        return self.get_bytes().decode(self.charset or "utf-8")


class DataWeaveStream(io.RawIOBase):
    """A file-like stream that reads script execution results from the native library
    without loading the entire output into memory.

    Implements :class:`io.RawIOBase` so it can be wrapped with
    :func:`io.BufferedReader` or used anywhere a binary file-like object is expected.

    Usage::

        with dw.run_stream("output application/json --- payload") as stream:
            for chunk in stream:
                process(chunk)

    The stream **must** be closed (or used as a context manager) to release native
    resources.  Metadata (``mimeType``, ``charset``, ``binary``) is available as
    attributes immediately after creation.
    """

    def __init__(self, lib, thread, handle: int, metadata: dict):
        super().__init__()
        self._lib = lib
        self._thread = thread
        self._handle = handle
        self.mimeType: Optional[str] = metadata.get("mimeType")
        self.charset: Optional[str] = metadata.get("charset")
        self.binary: bool = bool(metadata.get("binary", False))
        self._closed_native = False

    # ── io.RawIOBase interface ──────────────────────────────────────────

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> Optional[int]:
        """Read up to ``len(b)`` bytes into the pre-allocated buffer *b*."""
        if self.closed:
            raise ValueError("I/O operation on closed stream")
        buf = (ctypes.c_char * len(b))()
        n = self._lib.run_script_read(self._thread, self._handle, buf, len(b))
        if n <= 0:
            return 0
        b[:n] = buf[:n]
        return n

    def read(self, size: int = -1) -> bytes:
        """Read up to *size* bytes.  ``-1`` reads until EOF."""
        if self.closed:
            raise ValueError("I/O operation on closed stream")
        if size == 0:
            return b""
        if size < 0:
            chunks = []
            while True:
                chunk = self.read(_DEFAULT_CHUNK_SIZE)
                if not chunk:
                    break
                chunks.append(chunk)
            return b"".join(chunks)
        buf = ctypes.create_string_buffer(size)
        n = self._lib.run_script_read(self._thread, self._handle, buf, size)
        if n <= 0:
            return b""
        return buf.raw[:n]

    def close(self):
        if not self._closed_native and self._handle is not None:
            try:
                self._lib.run_script_close(self._thread, self._handle)
            except Exception:
                pass
            self._closed_native = True
        super().close()

    # ── convenience helpers ─────────────────────────────────────────────

    def read_all_string(self) -> str:
        """Read the full result and decode it as a string using the session charset."""
        raw = self.read(-1)
        return raw.decode(self.charset or "utf-8")

    def __iter__(self) -> Iterator[bytes]:
        """Iterate over the stream in chunks."""
        while True:
            chunk = self.read(_DEFAULT_CHUNK_SIZE)
            if not chunk:
                break
            yield chunk


class DataWeaveInputStream:
    """Wraps a native input stream session handle, allowing the caller to
    push data into the DW engine as an input binding.

    The caller writes bytes via :meth:`write` and signals EOF via :meth:`close`.
    This object is intended to be used from a **separate thread** that feeds
    input while the main thread reads the output stream.

    Usage::

        dw = DataWeave()
        dw.initialize()
        input_handle = dw.open_input_stream("application/json")

        def feed():
            with open("large.json", "rb") as f:
                while chunk := f.read(8192):
                    input_handle.write(chunk)
            input_handle.close()

        import threading
        t = threading.Thread(target=feed)
        t.start()

        with dw.run_stream("payload", inputs={"payload": input_handle}) as out:
            for chunk in out:
                process(chunk)
        t.join()
    """

    def __init__(self, lib, isolate, isolate_t_ptr, isolatethread_t_ptr, handle: int, mime_type: str, charset: Optional[str] = None):
        self._lib = lib
        self._isolate = isolate
        self._isolate_t_ptr = isolate_t_ptr
        self._isolatethread_t_ptr = isolatethread_t_ptr
        self._handle = handle
        self.mime_type: str = mime_type
        self.charset: Optional[str] = charset
        self._closed = False
        self._attached_thread = None

    @property
    def handle(self) -> int:
        """The native handle for this input stream session."""
        return self._handle

    def _ensure_thread_attached(self):
        """Attach the current OS thread to the GraalVM isolate if not already attached.

        Each OS thread that calls a ``@CEntryPoint`` function must have its own
        ``IsolateThread`` token.  This method calls ``graal_attach_thread`` to
        obtain one for the feeder thread.
        """
        if self._attached_thread is not None:
            return
        self._lib.graal_attach_thread.argtypes = [
            self._isolate_t_ptr,
            ctypes.POINTER(self._isolatethread_t_ptr),
        ]
        self._lib.graal_attach_thread.restype = ctypes.c_int

        thread = self._isolatethread_t_ptr()
        rc = self._lib.graal_attach_thread(self._isolate, ctypes.byref(thread))
        if rc != 0:
            raise DataWeaveError(f"Failed to attach feeder thread to GraalVM isolate (error {rc})")
        self._attached_thread = thread

    def _detach_thread(self):
        """Detach the feeder thread from the GraalVM isolate."""
        if self._attached_thread is not None:
            try:
                self._lib.graal_detach_thread.argtypes = [self._isolatethread_t_ptr]
                self._lib.graal_detach_thread.restype = ctypes.c_int
                self._lib.graal_detach_thread(self._attached_thread)
            except Exception:
                pass
            self._attached_thread = None

    def write(self, data: bytes) -> None:
        """Write bytes into the input stream.

        :param data: the bytes to write
        :raises DataWeaveError: on I/O failure or if the stream is closed
        """
        if self._closed:
            raise DataWeaveError("Input stream is already closed")
        if not data:
            return
        self._ensure_thread_attached()
        buf = ctypes.create_string_buffer(data)
        rc = self._lib.input_stream_write(self._attached_thread, self._handle, buf, len(data))
        if rc != 0:
            raise DataWeaveError("Failed to write to input stream")

    def close(self) -> None:
        """Close the write end of the pipe, signalling EOF to the DW engine."""
        if not self._closed:
            self._ensure_thread_attached()
            self._lib.input_stream_close(self._attached_thread, self._handle)
            self._closed = True
            self._detach_thread()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def _parse_native_encoded_response(raw: str) -> ExecutionResult:
    if raw is None:
        return ExecutionResult(False, None, "Native returned null", False, None, None)

    if raw == "":
        return ExecutionResult(False, None, "Native returned empty response", False, None, None)

    try:
        parsed = json.loads(raw)
    except Exception as e:
        return ExecutionResult(False, None, f"Failed to parse native JSON response: {e}", False, None, None)

    if not isinstance(parsed, dict):
        return ExecutionResult(False, None, "Native response JSON is not an object", False, None, None)

    success = bool(parsed.get("success", False))
    if not success:
        return ExecutionResult(False, None, parsed.get("error"), False, None, None)

    return ExecutionResult(
        success=True,
        result=parsed.get("result"),
        error=None,
        binary=bool(parsed.get("binary", False)),
        mimeType=parsed.get("mimeType"),
        charset=parsed.get("charset"),
    )


def _candidate_library_paths() -> list[Path]:
    paths: list[Path] = []

    env_value = (__import__("os").environ.get(_ENV_NATIVE_LIB) or "").strip()
    if env_value:
        paths.append(Path(env_value))

    pkg_dir = Path(__file__).resolve().parent
    native_dir = pkg_dir / "native"
    paths.append(native_dir / "dwlib.dylib")
    paths.append(native_dir / "dwlib.so")
    paths.append(native_dir / "dwlib.dll")

    # Dev fallback: if this package is being used from the data-weave-cli repo
    # tree, locate native-lib/build/native/nativeCompile.
    for parent in pkg_dir.parents:
        build_dir = parent / "build" / "native" / "nativeCompile"
        if build_dir.exists():
            paths.append(build_dir / "dwlib.dylib")
            paths.append(build_dir / "dwlib.so")
            paths.append(build_dir / "dwlib.dll")
            break

    # CWD fallback
    paths.append(Path("dwlib.dylib"))
    paths.append(Path("dwlib.so"))
    paths.append(Path("dwlib.dll"))

    return paths


def _find_library() -> str:
    for p in _candidate_library_paths():
        if p.exists() and p.is_file():
            return str(p)

    raise DataWeaveLibraryNotFoundError(
        "Could not find DataWeave native library (dwlib). "
        f"Set {_ENV_NATIVE_LIB} to an absolute path or install a wheel that bundles the native library."
    )


def _normalize_input_value(value: Any, mime_type: Optional[str] = None) -> Dict[str, Any]:
    if isinstance(value, dict):
        allowed_keys = {"content", "mimeType", "charset", "properties"}
        extra_keys = set(value.keys()) - allowed_keys
        if extra_keys:
            raise DataWeaveError(
                "Explicit input dict contains unsupported keys: " + ", ".join(sorted(extra_keys))
            )

        if "content" in value or "mimeType" in value:
            if "content" not in value or "mimeType" not in value:
                raise DataWeaveError(
                    "Explicit input dict must include both 'content' and 'mimeType'"
                )

            raw_content = value.get("content")
            charset = value.get("charset") or "utf-8"
            if isinstance(raw_content, bytes):
                encoded_content = base64.b64encode(raw_content).decode("ascii")
            else:
                encoded_content = base64.b64encode(str(raw_content).encode(charset)).decode("ascii")

            normalized: Dict[str, Any] = {
                "content": encoded_content,
                "mimeType": value.get("mimeType"),
            }
            if "charset" in value:
                normalized["charset"] = value.get("charset")
            if "properties" in value:
                normalized["properties"] = value.get("properties")
            return normalized

    if isinstance(value, InputValue):
        out: Dict[str, Any] = {
            "content": value.encode_content(),
            "mimeType": value.mimeType or mime_type,
        }
        if value.charset is not None:
            out["charset"] = value.charset
        if value.properties is not None:
            out["properties"] = value.properties
        return out

    if isinstance(value, str):
        content = value
        default_mime = "text/plain"
    elif isinstance(value, (int, float, bool)):
        content = json.dumps(value)
        default_mime = "application/json"
    elif value is None:
        content = "null"
        default_mime = "application/json"
    else:
        try:
            content = json.dumps(value)
            default_mime = "application/json"
        except (TypeError, ValueError):
            content = str(value)
            default_mime = "text/plain"

    charset = "utf-8"
    encoded_content = base64.b64encode(content.encode(charset)).decode("ascii")

    return {
        "content": encoded_content,
        "mimeType": mime_type or default_mime,
        "charset": charset,
        "properties": None,
    }


class DataWeave:
    def __init__(self, lib_path: Optional[str] = None):
        self._lib_path = lib_path or _find_library()
        self._lib = None
        self._isolate = None
        self._thread = None
        self._initialized = False

    def _load_library(self):
        try:
            self._lib = ctypes.CDLL(self._lib_path)
        except OSError as e:
            raise DataWeaveError(f"Failed to load library from {self._lib_path}: {e}")

    def _setup_graal_structures(self):
        class graal_isolate_t(ctypes.Structure):
            pass

        class graal_isolatethread_t(ctypes.Structure):
            pass

        self._graal_isolate_t_ptr = ctypes.POINTER(graal_isolate_t)
        self._graal_isolatethread_t_ptr = ctypes.POINTER(graal_isolatethread_t)

    def _create_isolate(self):
        self._lib.graal_create_isolate.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(self._graal_isolate_t_ptr),
            ctypes.POINTER(self._graal_isolatethread_t_ptr),
        ]
        self._lib.graal_create_isolate.restype = ctypes.c_int

        self._isolate = self._graal_isolate_t_ptr()
        self._thread = self._graal_isolatethread_t_ptr()

        result = self._lib.graal_create_isolate(None, ctypes.byref(self._isolate), ctypes.byref(self._thread))
        if result != 0:
            raise DataWeaveError(f"Failed to create GraalVM isolate. Error code: {result}")

    def _setup_functions(self):
        if not hasattr(self._lib, "run_script"):
            raise DataWeaveError("Native library does not export run_script")

        self._lib.run_script.argtypes = [
            self._graal_isolatethread_t_ptr,
            ctypes.c_char_p,
            ctypes.c_char_p,
        ]
        self._lib.run_script.restype = ctypes.c_void_p

        if hasattr(self._lib, "free_cstring"):
            self._lib.free_cstring.argtypes = [self._graal_isolatethread_t_ptr, ctypes.c_void_p]
            self._lib.free_cstring.restype = None

        # Streaming API
        if hasattr(self._lib, "run_script_open"):
            self._lib.run_script_open.argtypes = [
                self._graal_isolatethread_t_ptr,
                ctypes.c_char_p,
                ctypes.c_char_p,
            ]
            self._lib.run_script_open.restype = ctypes.c_long

            self._lib.run_script_read.argtypes = [
                self._graal_isolatethread_t_ptr,
                ctypes.c_long,
                ctypes.c_char_p,
                ctypes.c_int,
            ]
            self._lib.run_script_read.restype = ctypes.c_int

            self._lib.run_script_metadata.argtypes = [
                self._graal_isolatethread_t_ptr,
                ctypes.c_long,
            ]
            self._lib.run_script_metadata.restype = ctypes.c_void_p

            self._lib.run_script_stream_error.argtypes = [
                self._graal_isolatethread_t_ptr,
                ctypes.c_long,
            ]
            self._lib.run_script_stream_error.restype = ctypes.c_void_p

            self._lib.run_script_close.argtypes = [
                self._graal_isolatethread_t_ptr,
                ctypes.c_long,
            ]
            self._lib.run_script_close.restype = None

            self._has_streaming = True
        else:
            self._has_streaming = False

        # Streaming Input API
        if hasattr(self._lib, "input_stream_open"):
            self._lib.input_stream_open.argtypes = [
                self._graal_isolatethread_t_ptr,
                ctypes.c_char_p,
                ctypes.c_char_p,
            ]
            self._lib.input_stream_open.restype = ctypes.c_long

            self._lib.input_stream_write.argtypes = [
                self._graal_isolatethread_t_ptr,
                ctypes.c_long,
                ctypes.c_char_p,
                ctypes.c_int,
            ]
            self._lib.input_stream_write.restype = ctypes.c_int

            self._lib.input_stream_close.argtypes = [
                self._graal_isolatethread_t_ptr,
                ctypes.c_long,
            ]
            self._lib.input_stream_close.restype = ctypes.c_int

            self._has_streaming_input = True
        else:
            self._has_streaming_input = False

        # Callback-based Streaming API
        if hasattr(self._lib, "run_script_callback"):
            self._lib.run_script_callback.argtypes = [
                self._graal_isolatethread_t_ptr,
                ctypes.c_char_p,
                ctypes.c_char_p,
                WRITE_CALLBACK,
                ctypes.c_void_p,
            ]
            self._lib.run_script_callback.restype = ctypes.c_void_p

            self._has_callback_streaming = True
        else:
            self._has_callback_streaming = False

        if hasattr(self._lib, "run_script_input_output_callback"):
            self._lib.run_script_input_output_callback.argtypes = [
                self._graal_isolatethread_t_ptr,
                ctypes.c_char_p,
                ctypes.c_char_p,
                ctypes.c_char_p,
                ctypes.c_char_p,
                ctypes.c_char_p,
                READ_CALLBACK,
                WRITE_CALLBACK,
                ctypes.c_void_p,
            ]
            self._lib.run_script_input_output_callback.restype = ctypes.c_void_p

            self._has_callback_input_output = True
        else:
            self._has_callback_input_output = False

    def _decode_and_free(self, ptr: Optional[int]) -> str:
        if not ptr:
            return ""

        try:
            result_bytes = ctypes.string_at(ptr)
            return result_bytes.decode("utf-8")
        finally:
            if self._lib is not None and hasattr(self._lib, "free_cstring"):
                self._lib.free_cstring(self._thread, ptr)

    def initialize(self):
        if self._initialized:
            return

        self._load_library()
        self._setup_graal_structures()
        self._create_isolate()
        self._setup_functions()
        self._initialized = True

    def cleanup(self):
        if not self._initialized:
            return

        if hasattr(self._lib, "graal_detach_thread") and self._thread:
            try:
                self._lib.graal_detach_thread.argtypes = [self._graal_isolatethread_t_ptr]
                self._lib.graal_detach_thread.restype = ctypes.c_int
                self._lib.graal_detach_thread(self._thread)
            except Exception:
                pass

        self._initialized = False
        self._thread = None
        self._isolate = None
        self._lib = None

    def open_input_stream(self, mime_type: str, charset: Optional[str] = None) -> DataWeaveInputStream:
        """Create a new streaming input that can be written to from a separate thread.

        The returned :class:`DataWeaveInputStream` can be passed as a value in the
        ``inputs`` dict of :meth:`run_stream`. The caller **must** write data and
        call :meth:`DataWeaveInputStream.close` (or use it as a context manager)
        from a **separate thread** to avoid deadlocks.

        :param mime_type: the MIME type of the data being streamed
        :param charset: the charset (default UTF-8)
        :return: a :class:`DataWeaveInputStream`
        :raises DataWeaveError: if the runtime is not initialized or streaming input is unsupported
        """
        if not self._initialized:
            raise DataWeaveError("DataWeave runtime not initialized. Call initialize() first.")
        if not self._has_streaming_input:
            raise DataWeaveError("Native library does not support streaming input API (input_stream_open not found).")

        charset_arg = charset.encode("utf-8") if charset else None
        handle = self._lib.input_stream_open(
            self._thread,
            mime_type.encode("utf-8"),
            charset_arg,
        )
        if handle <= 0:
            raise DataWeaveError("Failed to create input stream session")
        return DataWeaveInputStream(
            self._lib, self._isolate,
            self._graal_isolate_t_ptr, self._graal_isolatethread_t_ptr,
            handle, mime_type, charset,
        )

    def run_stream(self, script: str, inputs: Optional[Dict[str, Any]] = None) -> DataWeaveStream:
        """Execute a DataWeave script and return a :class:`DataWeaveStream` for
        reading the result incrementally.

        The returned stream **must** be closed (or used as a context manager) to
        release native resources.

        :param script: the DataWeave script source
        :param inputs: optional input bindings
        :return: a :class:`DataWeaveStream`
        :raises DataWeaveError: if the runtime is not initialized or streaming is unsupported
        """
        if not self._initialized:
            raise DataWeaveError("DataWeave runtime not initialized. Call initialize() first.")
        if not self._has_streaming:
            raise DataWeaveError("Native library does not support the streaming API (run_script_open not found).")

        if inputs is None:
            inputs = {}

        normalized_inputs = {}
        for key, val in inputs.items():
            if isinstance(val, DataWeaveInputStream):
                normalized_inputs[key] = {
                    "streamHandle": str(val.handle),
                    "mimeType": val.mime_type,
                }
                if val.charset:
                    normalized_inputs[key]["charset"] = val.charset
            else:
                normalized_inputs[key] = _normalize_input_value(val)
        inputs_json = json.dumps(normalized_inputs)

        try:
            handle = self._lib.run_script_open(
                self._thread,
                script.encode("utf-8"),
                inputs_json.encode("utf-8"),
            )

            # Check for error session
            err_ptr = self._lib.run_script_stream_error(self._thread, handle)
            err_msg = ""
            if err_ptr:
                try:
                    err_msg = ctypes.string_at(err_ptr).decode("utf-8")
                finally:
                    if hasattr(self._lib, "free_cstring"):
                        self._lib.free_cstring(self._thread, err_ptr)

            if err_msg:
                self._lib.run_script_close(self._thread, handle)
                raise DataWeaveError(err_msg)

            # Fetch metadata
            meta_ptr = self._lib.run_script_metadata(self._thread, handle)
            metadata = {}
            if meta_ptr:
                try:
                    meta_raw = ctypes.string_at(meta_ptr).decode("utf-8")
                    metadata = json.loads(meta_raw)
                finally:
                    if hasattr(self._lib, "free_cstring"):
                        self._lib.free_cstring(self._thread, meta_ptr)

            return DataWeaveStream(self._lib, self._thread, handle, metadata)
        except DataWeaveError:
            raise
        except Exception as e:
            raise DataWeaveError(f"Failed to open streaming session: {e}")

    def run_callback(
        self,
        script: str,
        write_callback,
        inputs: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """Execute a DataWeave script and stream the output via a write callback.

        Instead of the session-based ``run_stream`` API, the native side reads the
        output internally and invokes *write_callback* for each chunk.

        :param script: the DataWeave script source
        :param write_callback: callable ``(data: bytes) -> int`` invoked with each
            output chunk. Must return ``0`` on success or non-zero to abort.
        :param inputs: optional input bindings (same format as :meth:`run`)
        :return: a dict with ``success``, ``mimeType``, ``charset``, ``binary`` on
            success, or ``success`` and ``error`` on failure
        :raises DataWeaveError: if the runtime is not initialized or the callback API
            is not available
        """
        if not self._initialized:
            raise DataWeaveError("DataWeave runtime not initialized. Call initialize() first.")
        if not self._has_callback_streaming:
            raise DataWeaveError(
                "Native library does not support callback streaming API (run_script_callback not found)."
            )

        if inputs is None:
            inputs = {}

        normalized_inputs = {}
        for key, val in inputs.items():
            if isinstance(val, DataWeaveInputStream):
                normalized_inputs[key] = {
                    "streamHandle": str(val.handle),
                    "mimeType": val.mime_type,
                }
                if val.charset:
                    normalized_inputs[key]["charset"] = val.charset
            else:
                normalized_inputs[key] = _normalize_input_value(val)
        inputs_json = json.dumps(normalized_inputs)

        @WRITE_CALLBACK
        def _write_cb(_ctx, buf, length):
            try:
                data = ctypes.string_at(buf, length)
                return write_callback(data)
            except Exception:
                return -1

        try:
            result_ptr = self._lib.run_script_callback(
                self._thread,
                script.encode("utf-8"),
                inputs_json.encode("utf-8"),
                _write_cb,
                None,
            )
            raw = self._decode_and_free(result_ptr)
            return json.loads(raw) if raw else {"success": False, "error": "Empty response"}
        except Exception as e:
            raise DataWeaveError(f"Failed to execute callback streaming: {e}")

    def run_input_output_callback(
        self,
        script: str,
        input_name: str,
        input_mime_type: str,
        read_callback,
        write_callback,
        input_charset: Optional[str] = None,
        inputs: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """Execute a DataWeave script with callback-driven input *and* output streaming.

        The native side calls *read_callback* on a background thread to pull input
        data for the binding named *input_name*, and calls *write_callback* on the
        calling thread to push output chunks.

        :param script: the DataWeave script source
        :param input_name: the binding name for the callback-supplied input
        :param input_mime_type: MIME type of the callback-supplied input
        :param read_callback: callable ``(buf_size: int) -> bytes`` returning the
            next chunk, empty bytes ``b""`` on EOF, or raising on error
        :param write_callback: callable ``(data: bytes) -> int`` returning ``0`` on
            success or non-zero to abort
        :param input_charset: charset of the callback-supplied input (default UTF-8)
        :param inputs: optional additional input bindings
        :return: a dict with metadata on success, or error info on failure
        :raises DataWeaveError: if the runtime is not initialized or the API is missing
        """
        if not self._initialized:
            raise DataWeaveError("DataWeave runtime not initialized. Call initialize() first.")
        if not self._has_callback_input_output:
            raise DataWeaveError(
                "Native library does not support callback input/output API "
                "(run_script_input_output_callback not found)."
            )

        if inputs is None:
            inputs = {}

        normalized_inputs = {key: _normalize_input_value(val) for key, val in inputs.items()}
        inputs_json = json.dumps(normalized_inputs)

        @READ_CALLBACK
        def _read_cb(_ctx, buf, buf_size):
            try:
                data = read_callback(buf_size)
                if not data:
                    return 0  # EOF
                n = len(data)
                ctypes.memmove(buf, data, n)
                return n
            except Exception:
                return -1

        @WRITE_CALLBACK
        def _write_cb(_ctx, buf, length):
            try:
                data = ctypes.string_at(buf, length)
                return write_callback(data)
            except Exception:
                return -1

        try:
            result_ptr = self._lib.run_script_input_output_callback(
                self._thread,
                script.encode("utf-8"),
                inputs_json.encode("utf-8"),
                input_name.encode("utf-8"),
                input_mime_type.encode("utf-8"),
                input_charset.encode("utf-8") if input_charset else None,
                _read_cb,
                _write_cb,
                None,
            )
            raw = self._decode_and_free(result_ptr)
            return json.loads(raw) if raw else {"success": False, "error": "Empty response"}
        except Exception as e:
            raise DataWeaveError(f"Failed to execute callback input/output streaming: {e}")

    def run(self, script: str, inputs: Optional[Dict[str, Any]] = None) -> ExecutionResult:
        if not self._initialized:
            raise DataWeaveError("DataWeave runtime not initialized. Call initialize() first.")

        if inputs is None:
            inputs = {}

        normalized_inputs = {key: _normalize_input_value(val) for key, val in inputs.items()}
        inputs_json = json.dumps(normalized_inputs)

        try:
            result_ptr = self._lib.run_script(
                self._thread,
                script.encode("utf-8"),
                inputs_json.encode("utf-8"),
            )
            raw = self._decode_and_free(result_ptr)
            return _parse_native_encoded_response(raw)
        except Exception as e:
            raise DataWeaveError(f"Failed to execute script: {e}")

    def __enter__(self):
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
        return False


_global_instance: Optional[DataWeave] = None


def _get_global_instance() -> DataWeave:
    global _global_instance
    if _global_instance is None:
        _global_instance = DataWeave()
        _global_instance.initialize()
    return _global_instance


def run_script(script: str, inputs: Optional[Dict[str, Any]] = None) -> ExecutionResult:
    return _get_global_instance().run(script, inputs)


def run_stream(script: str, inputs: Optional[Dict[str, Any]] = None) -> DataWeaveStream:
    """Execute a script and return a :class:`DataWeaveStream` for incremental reading."""
    return _get_global_instance().run_stream(script, inputs)


def open_input_stream(mime_type: str, charset: Optional[str] = None) -> DataWeaveInputStream:
    """Create a streaming input session. See :meth:`DataWeave.open_input_stream`."""
    return _get_global_instance().open_input_stream(mime_type, charset)


def run_callback(script: str, write_callback, inputs: Optional[Dict[str, Any]] = None) -> dict:
    """Execute a script and stream output via a write callback. See :meth:`DataWeave.run_callback`."""
    return _get_global_instance().run_callback(script, write_callback, inputs)


def run_input_output_callback(
    script: str,
    input_name: str,
    input_mime_type: str,
    read_callback,
    write_callback,
    input_charset: Optional[str] = None,
    inputs: Optional[Dict[str, Any]] = None,
) -> dict:
    """Execute a script with callback-driven input and output. See :meth:`DataWeave.run_input_output_callback`."""
    return _get_global_instance().run_input_output_callback(
        script, input_name, input_mime_type, read_callback, write_callback, input_charset, inputs,
    )


def cleanup():
    global _global_instance
    if _global_instance is not None:
        _global_instance.cleanup()
        _global_instance = None


__all__ = [
    "DataWeaveError",
    "DataWeaveInputStream",
    "DataWeaveLibraryNotFoundError",
    "DataWeaveStream",
    "ExecutionResult",
    "InputValue",
    "READ_CALLBACK",
    "WRITE_CALLBACK",
    "open_input_stream",
    "run_callback",
    "run_input_output_callback",
    "run_script",
    "run_stream",
    "cleanup",
]
