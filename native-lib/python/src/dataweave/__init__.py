"""
DataWeave Python Module

Execute DataWeave scripts from Python via a GraalVM native shared library.
Supports buffered execution, output streaming, and bidirectional streaming
with constant memory overhead.

Basic usage:
    import dataweave

    result = dataweave.run("2 + 2")
    print(result.get_string())  # "4"

Output streaming (yields chunks as produced):
    stream = dataweave.run_streaming("output json --- (1 to 10000) map {id: $}")
    for chunk in stream:
        sys.stdout.buffer.write(chunk)

Bidirectional streaming (iterable in, generator out):
    with open("large.json", "rb") as f:
        stream = dataweave.run_transform(
            "output csv --- payload",
            input_stream=iter(lambda: f.read(8192), b""),
            input_mime_type="application/json",
        )
        for chunk in stream:
            process(chunk)

Context manager (explicit lifecycle control):
    from dataweave import DataWeave

    with DataWeave() as dw:
        result = dw.run("2 + 2")
        print(result.get_string())

Error handling:
    try:
        result = dataweave.run("invalid", raise_on_error=True)
    except dataweave.DataWeaveScriptError as e:
        print(e.result.error)

Native resources are released automatically at interpreter exit via atexit.
Call dataweave.cleanup() to release them earlier if needed.
"""

import base64
import ctypes
import json
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import Any, Callable, Dict, Generator, Iterable, Optional, Union


class DataWeaveError(Exception):
    pass


class DataWeaveScriptError(DataWeaveError):
    """Raised when a DataWeave script fails (compile or runtime error).

    Carries the full result object so callers can inspect details.
    """

    def __init__(self, result):
        self.result = result
        super().__init__(result.error or "Script execution failed")


class DataWeaveLibraryNotFoundError(Exception):
    pass


# ctypes callback signatures matching NativeCallbacks.WriteCallback / ReadCallback.
# Buffer parameters use c_void_p (not c_char_p) because ctypes gives c_char_p
# special treatment that prevents writing into the buffer.
# int (*WriteCallback)(void *ctx, const char *buffer, int length)
WRITE_CALLBACK = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int)
# int (*ReadCallback)(void *ctx, char *buffer, int bufferSize)
READ_CALLBACK = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int)


WriteCallback = Callable[[bytes], int]
ReadCallback = Callable[[int], bytes]

_ENV_NATIVE_LIB = "DATAWEAVE_NATIVE_LIB"


@dataclass
class InputValue:
    content: Union[str, bytes]
    mime_type: Optional[str] = None
    charset: Optional[str] = None
    properties: Optional[Dict[str, Union[str, int, bool]]] = None

    def encode_content(self) -> str:
        if isinstance(self.content, bytes):
            raw = self.content
        else:
            raw = self.content.encode(self.charset or "utf-8")
        return base64.b64encode(raw).decode("ascii")


@dataclass(repr=False)
class ExecutionResult:
    success: bool
    result: Optional[str]
    error: Optional[str]
    binary: bool
    mime_type: Optional[str]
    charset: Optional[str]

    def __repr__(self):
        if not self.success:
            return f"ExecutionResult(success=False, error={self.error!r})"
        preview = (self.result[:50] + "...") if self.result and len(self.result) > 50 else self.result
        return f"ExecutionResult(success=True, mime_type={self.mime_type!r}, charset={self.charset!r}, result={preview!r})"

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


@dataclass
class StreamingResult:
    """Metadata returned after a streaming execution completes."""
    success: bool
    error: Optional[str]
    mime_type: Optional[str]
    charset: Optional[str]
    binary: bool


class Stream:
    """Wrapper around a streaming generator that captures metadata.

    Iterate to consume output chunks. After iteration completes,
    access ``.metadata`` for the :class:`StreamingResult`.
    """

    def __init__(self, gen: Generator[bytes, None, StreamingResult]):
        self._gen = gen
        self._metadata: Optional[StreamingResult] = None

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        try:
            return next(self._gen)
        except StopIteration as e:
            self._metadata = e.value
            raise

    @property
    def metadata(self) -> Optional[StreamingResult]:
        return self._metadata


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
        mime_type=parsed.get("mimeType"),
        charset=parsed.get("charset"),
    )


def _parse_streaming_result(meta: dict) -> StreamingResult:
    success = meta.get("success", False)
    if not success:
        return StreamingResult(
            success=False,
            error=meta.get("error"),
            mime_type=None,
            charset=None,
            binary=False,
        )
    return StreamingResult(
        success=True,
        error=None,
        mime_type=meta.get("mimeType"),
        charset=meta.get("charset"),
        binary=meta.get("binary", False),
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
            "mimeType": value.mime_type or mime_type,
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

        # Thread attachment for background threads
        if hasattr(self._lib, "graal_attach_thread"):
            self._lib.graal_attach_thread.argtypes = [self._graal_isolate_t_ptr, ctypes.POINTER(self._graal_isolatethread_t_ptr)]
            self._lib.graal_attach_thread.restype = ctypes.c_int
        if hasattr(self._lib, "graal_detach_thread"):
            self._lib.graal_detach_thread.argtypes = [self._graal_isolatethread_t_ptr]
            self._lib.graal_detach_thread.restype = ctypes.c_int
        if hasattr(self._lib, "graal_tear_down_isolate"):
            self._lib.graal_tear_down_isolate.argtypes = [self._graal_isolatethread_t_ptr]
            self._lib.graal_tear_down_isolate.restype = ctypes.c_int

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

        if hasattr(self._lib, "graal_tear_down_isolate") and self._thread:
            try:
                self._lib.graal_tear_down_isolate(self._thread)
            except Exception:
                pass
        elif hasattr(self._lib, "graal_detach_thread") and self._thread:
            try:
                self._lib.graal_detach_thread(self._thread)
            except Exception:
                pass

        self._initialized = False
        self._thread = None
        self._isolate = None
        self._lib = None

    def run_callback(
        self,
        script: str,
        write_callback: WriteCallback,
        inputs: Optional[Dict[str, Any]] = None,
    ) -> StreamingResult:
        """Execute a DataWeave script and stream the output via a write callback.

        The native side reads the output internally and invokes *write_callback*
        for each chunk.

        :param script: the DataWeave script source
        :param write_callback: callable ``(data: bytes) -> int`` invoked with each
            output chunk. Must return ``0`` on success or non-zero to abort.
        :param inputs: optional input bindings (same format as :meth:`run`)
        :return: a :class:`StreamingResult` with metadata
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

        normalized_inputs = {key: _normalize_input_value(val) for key, val in inputs.items()}
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
            meta = json.loads(raw) if raw else {"success": False, "error": "Empty response"}
        except Exception as e:
            raise DataWeaveError(f"Failed to execute callback streaming: {e}")

        return _parse_streaming_result(meta)

    def run_streaming(
        self,
        script: str,
        inputs: Optional[Dict[str, Any]] = None,
    ) -> Stream:
        """Execute a DataWeave script and yield output chunks as they arrive.

        Chunks are yielded in real-time as the native engine produces them,
        using a background thread and queue. The caller sees data before the
        script finishes executing.

        Usage::

            with DataWeave() as dw:
                stream = dw.run_streaming("output json --- {items: (1 to 100)}")
                for chunk in stream:
                    sys.stdout.buffer.write(chunk)
                metadata = stream.metadata  # StreamingResult with mime_type, charset, etc.

        :param script: the DataWeave script source
        :param inputs: optional input bindings (same format as :meth:`run`)
        :return: a :class:`Stream` yielding ``bytes`` chunks; after iteration,
            ``.metadata`` holds a :class:`StreamingResult`
        :raises DataWeaveError: if the runtime is not initialized or the callback API
            is not available
        """
        return Stream(self._run_streaming_gen(script, inputs))

    def _run_streaming_gen(
        self,
        script: str,
        inputs: Optional[Dict[str, Any]] = None,
    ) -> Generator[bytes, None, StreamingResult]:
        if not self._initialized:
            raise DataWeaveError("DataWeave runtime not initialized. Call initialize() first.")
        if not self._has_callback_streaming:
            raise DataWeaveError(
                "Native library does not support callback streaming API (run_script_callback not found)."
            )

        if inputs is None:
            inputs = {}

        normalized_inputs = {key: _normalize_input_value(val) for key, val in inputs.items()}
        inputs_json = json.dumps(normalized_inputs)

        _SENTINEL = object()
        q: Queue = Queue()

        @WRITE_CALLBACK
        def _write_cb(_ctx, buf, length):
            try:
                q.put(ctypes.string_at(buf, length))
                return 0
            except Exception:
                return -1

        def _run_native():
            worker_thread = self._graal_isolatethread_t_ptr()
            rc = self._lib.graal_attach_thread(self._isolate, ctypes.byref(worker_thread))
            if rc != 0:
                q.put({"success": False, "error": f"Failed to attach worker thread to isolate (code {rc})"})
                q.put(_SENTINEL)
                return
            try:
                result_ptr = self._lib.run_script_callback(
                    worker_thread,
                    script.encode("utf-8"),
                    inputs_json.encode("utf-8"),
                    _write_cb,
                    None,
                )
                raw_ptr = result_ptr
                if raw_ptr:
                    raw = ctypes.string_at(raw_ptr).decode("utf-8")
                    self._lib.free_cstring(worker_thread, raw_ptr)
                else:
                    raw = ""
                meta = json.loads(raw) if raw else {"success": False, "error": "Empty response"}
                q.put(meta)
            except Exception as e:
                q.put({"success": False, "error": str(e)})
            finally:
                self._lib.graal_detach_thread(worker_thread)
                q.put(_SENTINEL)

        worker = Thread(target=_run_native, name="dw-streaming-worker", daemon=True)
        worker.start()

        meta = None
        while True:
            item = q.get()
            if item is _SENTINEL:
                break
            if isinstance(item, dict):
                meta = item
            else:
                yield item

        worker.join()

        if meta is None:
            meta = {"success": False, "error": "No metadata received from native call"}

        success = meta.get("success", False)
        if not success:
            return StreamingResult(
                success=False,
                error=meta.get("error"),
                mime_type=None,
                charset=None,
                binary=False,
            )

        return StreamingResult(
            success=True,
            error=None,
            mime_type=meta.get("mimeType"),
            charset=meta.get("charset"),
            binary=meta.get("binary", False),
        )

    def run_transform(
        self,
        script: str,
        input_stream: Iterable[bytes],
        input_name: str = "payload",
        input_mime_type: str = "application/json",
        input_charset: Optional[str] = None,
        inputs: Optional[Dict[str, Any]] = None,
    ) -> Stream:
        """Execute a DataWeave script with streaming input and output.

        Input data is pulled from *input_stream* (any iterable of bytes) and
        output chunks are yielded as they are produced — fully streaming in
        both directions with constant memory overhead.

        Usage::

            with DataWeave() as dw:
                with open("large.json", "rb") as f:
                    stream = dw.run_transform(
                        "output application/csv --- payload",
                        input_stream=iter(lambda: f.read(8192), b""),
                        input_mime_type="application/json",
                    )
                    for chunk in stream:
                        sys.stdout.buffer.write(chunk)
                metadata = stream.metadata

        :param script: the DataWeave script source
        :param input_stream: iterable yielding ``bytes`` chunks for the input binding
        :param input_name: binding name for the streamed input (default ``"payload"``)
        :param input_mime_type: MIME type of the streamed input
        :param input_charset: charset of the streamed input (default UTF-8)
        :param inputs: optional additional input bindings (same format as :meth:`run`)
        :return: a :class:`Stream` yielding ``bytes`` output chunks; after iteration,
            ``.metadata`` holds a :class:`StreamingResult`
        :raises DataWeaveError: if the runtime is not initialized or the API is missing
        """
        return Stream(self._run_transform_gen(
            script, input_stream, input_name, input_mime_type, input_charset, inputs,
        ))

    def _run_transform_gen(
        self,
        script: str,
        input_stream: Iterable[bytes],
        input_name: str = "payload",
        input_mime_type: str = "application/json",
        input_charset: Optional[str] = None,
        inputs: Optional[Dict[str, Any]] = None,
    ) -> Generator[bytes, None, StreamingResult]:
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

        _SENTINEL = object()
        q: Queue = Queue()

        @WRITE_CALLBACK
        def _write_cb(_ctx, buf, length):
            try:
                q.put(ctypes.string_at(buf, length))
                return 0
            except Exception:
                return -1

        input_iter = iter(input_stream)

        @READ_CALLBACK
        def _read_cb(_ctx, buf, buf_size):
            try:
                data = next(input_iter, b"")
                if not data:
                    return 0
                n = min(len(data), buf_size)
                ctypes.memmove(buf, data, n)
                return n
            except Exception:
                return -1

        def _run_native():
            worker_thread = self._graal_isolatethread_t_ptr()
            rc = self._lib.graal_attach_thread(self._isolate, ctypes.byref(worker_thread))
            if rc != 0:
                q.put({"success": False, "error": f"Failed to attach worker thread to isolate (code {rc})"})
                q.put(_SENTINEL)
                return
            try:
                result_ptr = self._lib.run_script_input_output_callback(
                    worker_thread,
                    script.encode("utf-8"),
                    inputs_json.encode("utf-8"),
                    input_name.encode("utf-8"),
                    input_mime_type.encode("utf-8"),
                    input_charset.encode("utf-8") if input_charset else None,
                    _read_cb,
                    _write_cb,
                    None,
                )
                if result_ptr:
                    raw = ctypes.string_at(result_ptr).decode("utf-8")
                    self._lib.free_cstring(worker_thread, result_ptr)
                else:
                    raw = ""
                meta = json.loads(raw) if raw else {"success": False, "error": "Empty response"}
                q.put(meta)
            except Exception as e:
                q.put({"success": False, "error": str(e)})
            finally:
                self._lib.graal_detach_thread(worker_thread)
                q.put(_SENTINEL)

        worker = Thread(target=_run_native, name="dw-transform-worker", daemon=True)
        worker.start()

        meta = None
        while True:
            item = q.get()
            if item is _SENTINEL:
                break
            if isinstance(item, dict):
                meta = item
            else:
                yield item

        worker.join()

        if meta is None:
            meta = {"success": False, "error": "No metadata received from native call"}

        success = meta.get("success", False)
        if not success:
            return StreamingResult(
                success=False,
                error=meta.get("error"),
                mime_type=None,
                charset=None,
                binary=False,
            )

        return StreamingResult(
            success=True,
            error=None,
            mime_type=meta.get("mimeType"),
            charset=meta.get("charset"),
            binary=meta.get("binary", False),
        )

    def run_input_output_callback(
        self,
        script: str,
        input_name: str,
        input_mime_type: str,
        read_callback: ReadCallback,
        write_callback: WriteCallback,
        input_charset: Optional[str] = None,
        inputs: Optional[Dict[str, Any]] = None,
    ) -> StreamingResult:
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
        :return: a :class:`StreamingResult` with metadata
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
                n = min(len(data), buf_size)
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
            meta = json.loads(raw) if raw else {"success": False, "error": "Empty response"}
        except Exception as e:
            raise DataWeaveError(f"Failed to execute callback input/output streaming: {e}")

        return _parse_streaming_result(meta)

    def run(self, script: str, inputs: Optional[Dict[str, Any]] = None, raise_on_error: bool = False) -> ExecutionResult:
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
            result = _parse_native_encoded_response(raw)
        except Exception as e:
            raise DataWeaveError(f"Failed to execute script: {e}")

        if raise_on_error and not result.success:
            raise DataWeaveScriptError(result)
        return result

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
        import atexit
        _global_instance = DataWeave()
        _global_instance.initialize()
        atexit.register(cleanup)
    return _global_instance


def run(script: str, inputs: Optional[Dict[str, Any]] = None, raise_on_error: bool = False) -> ExecutionResult:
    return _get_global_instance().run(script, inputs, raise_on_error=raise_on_error)


def run_streaming(
    script: str, inputs: Optional[Dict[str, Any]] = None,
) -> Stream:
    """Execute a script and yield output chunks. See :meth:`DataWeave.run_streaming`."""
    return _get_global_instance().run_streaming(script, inputs)


def run_callback(script: str, write_callback: WriteCallback, inputs: Optional[Dict[str, Any]] = None) -> StreamingResult:
    """Execute a script and stream output via a write callback. See :meth:`DataWeave.run_callback`."""
    return _get_global_instance().run_callback(script, write_callback, inputs)


def run_transform(
    script: str,
    input_stream: Iterable[bytes],
    input_name: str = "payload",
    input_mime_type: str = "application/json",
    input_charset: Optional[str] = None,
    inputs: Optional[Dict[str, Any]] = None,
) -> Stream:
    """Execute a script with streaming input and output. See :meth:`DataWeave.run_transform`."""
    return _get_global_instance().run_transform(
        script, input_stream, input_name, input_mime_type, input_charset, inputs,
    )


def run_input_output_callback(
    script: str,
    input_name: str,
    input_mime_type: str,
    read_callback: ReadCallback,
    write_callback: WriteCallback,
    input_charset: Optional[str] = None,
    inputs: Optional[Dict[str, Any]] = None,
) -> StreamingResult:
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
    "DataWeave",
    "DataWeaveError",
    "DataWeaveLibraryNotFoundError",
    "DataWeaveScriptError",
    "ExecutionResult",
    "InputValue",
    "ReadCallback",
    "Stream",
    "StreamingResult",
    "WriteCallback",
    "READ_CALLBACK",
    "WRITE_CALLBACK",
    "run",
    "run_callback",
    "run_input_output_callback",
    "run_streaming",
    "run_transform",
    "cleanup",
]
