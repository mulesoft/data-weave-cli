import ctypes
import json
from queue import Empty, Full, Queue
from threading import Event, Thread
from typing import Any, Dict, Generator, Iterable, Optional

from .encoding import normalize_input_value, parse_native_encoded_response, parse_streaming_result
from .models import (
    READ_CALLBACK,
    WRITE_CALLBACK,
    DataWeaveError,
    DataWeaveScriptError,
    ExecutionResult,
    ReadCallback,
    Stream,
    StreamingResult,
    WriteCallback,
)
from .native import NativeRuntime


_OUTPUT_QUEUE_MAXSIZE = 512
_WORKER_TIMEOUT_SECONDS = 30


class DataWeave:
    """High-level execution API backed by a :class:`NativeRuntime`."""

    def __init__(self, lib_path: Optional[str] = None):
        self._native = NativeRuntime(lib_path)

    def __getattr__(self, name):
        # Preserve the private attributes used by existing embedders and tests.
        aliases = {
            "_lib": "lib", "_isolate": "isolate", "_thread": "thread",
            "_initialized": "initialized", "_has_callback_streaming": "has_callback_streaming",
            "_has_callback_input_output": "has_callback_input_output",
        }
        if name in aliases:
            return getattr(self._native, aliases[name])
        raise AttributeError(name)

    def __setattr__(self, name, value):
        aliases = {
            "_lib": "lib", "_isolate": "isolate", "_thread": "thread",
            "_initialized": "initialized", "_has_callback_streaming": "has_callback_streaming",
            "_has_callback_input_output": "has_callback_input_output",
        }
        if name != "_native" and name in aliases and "_native" in self.__dict__:
            setattr(self._native, aliases[name], value)
        else:
            super().__setattr__(name, value)

    def _setup_graal_structures(self):
        # Compatibility shim for the private characterization tests from Tasks 1-3.
        from .native import GraalIsolatePointer, GraalIsolateThreadPointer
        if "_native" not in self.__dict__:
            native = NativeRuntime.__new__(NativeRuntime)
            native.lib = self.__dict__.pop("_lib", None)
            native.isolate = self.__dict__.pop("_isolate", None)
            native.thread = self.__dict__.pop("_thread", None)
            native.initialized = self.__dict__.pop("_initialized", False)
            native.has_callback_streaming = self.__dict__.pop("_has_callback_streaming", False)
            native.has_callback_input_output = self.__dict__.pop("_has_callback_input_output", False)
            self._native = native
        self._graal_isolate_t_ptr = GraalIsolatePointer
        self._graal_isolatethread_t_ptr = GraalIsolateThreadPointer

    def _decode_and_free(self, ptr):
        if "_native" not in self.__dict__:
            self._setup_graal_structures()
        return self._native.decode_and_free(ptr)

    def initialize(self):
        self._native.initialize()

    def cleanup(self):
        self._native.cleanup()

    def _require_initialized(self, supported: bool, api_name: str) -> None:
        if not self._native.initialized:
            raise DataWeaveError("DataWeave runtime not initialized. Call initialize() first.")
        if not supported:
            raise DataWeaveError(f"Native library does not support {api_name}.")

    @staticmethod
    def _inputs_json(inputs: Optional[Dict[str, Any]]) -> bytes:
        return json.dumps({key: normalize_input_value(value) for key, value in (inputs or {}).items()}).encode("utf-8")

    def run(self, script: str, inputs: Optional[Dict[str, Any]] = None, raise_on_error: bool = False) -> ExecutionResult:
        self._require_initialized(True, "script execution")
        try:
            raw = self._native.decode_and_free(self._native.run_script(self._native.thread, script.encode("utf-8"), self._inputs_json(inputs)))
            result = parse_native_encoded_response(raw)
        except Exception as error:
            raise DataWeaveError(f"Failed to execute script: {error}")
        if raise_on_error and not result.success:
            raise DataWeaveScriptError(result)
        return result

    def run_callback(self, script: str, write_callback: WriteCallback, inputs: Optional[Dict[str, Any]] = None) -> StreamingResult:
        self._require_initialized(self._native.has_callback_streaming, "callback streaming API (run_script_callback not found)")
        @WRITE_CALLBACK
        def write_cb(_context, buffer, length):
            try:
                return write_callback(ctypes.string_at(buffer, length))
            except Exception:
                return -1
        try:
            ptr = self._native.run_script_callback(self._native.thread, script.encode("utf-8"), self._inputs_json(inputs), write_cb)
            raw = self._native.decode_and_free(ptr)
        except Exception as error:
            raise DataWeaveError(f"Failed to execute callback streaming: {error}")
        return parse_streaming_result(json.loads(raw) if raw else {"success": False, "error": "Empty response"})

    def _stream_worker(self, invoke, cancelled: Event) -> Generator[bytes, None, StreamingResult]:
        sentinel = object()
        queue: Queue = Queue(maxsize=_OUTPUT_QUEUE_MAXSIZE)

        def publish(item) -> None:
            while not cancelled.is_set():
                try:
                    queue.put(item, timeout=0.1)
                    return
                except Full:
                    pass

        @WRITE_CALLBACK
        def write_cb(_context, buffer, length):
            try:
                if cancelled.is_set():
                    return -1
                queue.put(ctypes.string_at(buffer, length), timeout=_WORKER_TIMEOUT_SECONDS)
                return 0
            except Exception:
                return -1

        def worker_main():
            worker_thread = None
            try:
                worker_thread = self._native.attach_thread()
                raw = self._native.decode_and_free(invoke(worker_thread, write_cb), worker_thread)
                publish(json.loads(raw) if raw else {"success": False, "error": "Empty response"})
            except Exception as error:
                publish({"success": False, "error": str(error)})
            finally:
                if worker_thread is not None:
                    try:
                        self._native.detach_thread(worker_thread)
                    except Exception:
                        pass
                publish(sentinel)

        worker = Thread(target=worker_main, name="dw-streaming-worker", daemon=False)
        worker.start()
        metadata = None
        try:
            while True:
                try:
                    item = queue.get(timeout=_WORKER_TIMEOUT_SECONDS)
                except Empty:
                    cancelled.set()
                    worker.join(timeout=_WORKER_TIMEOUT_SECONDS)
                    raise DataWeaveError(f"Worker thread timeout after {_WORKER_TIMEOUT_SECONDS} seconds")
                if item is sentinel:
                    break
                if isinstance(item, dict):
                    metadata = item
                else:
                    yield item
        finally:
            cancelled.set()
            worker.join(timeout=_WORKER_TIMEOUT_SECONDS)
            if worker.is_alive():
                raise DataWeaveError(f"Worker thread timeout after {_WORKER_TIMEOUT_SECONDS} seconds")
        return parse_streaming_result(metadata or {"success": False, "error": "No metadata received from native call"})

    def run_streaming(self, script: str, inputs: Optional[Dict[str, Any]] = None) -> Stream:
        self._require_initialized(self._native.has_callback_streaming, "callback streaming API (run_script_callback not found)")
        cancelled = Event()
        encoded_inputs = self._inputs_json(inputs)
        stream = Stream(self._stream_worker(lambda thread, write_cb: self._native.run_script_callback(thread, script.encode("utf-8"), encoded_inputs, write_cb), cancelled))
        stream._on_close = cancelled.set
        stream._cancelled = cancelled
        return stream

    @staticmethod
    def _chunk_reader(input_stream: Iterable[bytes]):
        iterator = iter(input_stream)
        state = {"chunk": b"", "offset": 0, "done": False}
        @READ_CALLBACK
        def read_cb(_context, buffer, buffer_size):
            try:
                while True:
                    chunk = state["chunk"]
                    if state["offset"] < len(chunk):
                        size = min(len(chunk) - state["offset"], buffer_size)
                        ctypes.memmove(buffer, chunk[state["offset"]:state["offset"] + size], size)
                        state["offset"] += size
                        return size
                    if state["done"]:
                        return 0
                    chunk = next(iterator, None)
                    if not chunk:
                        state["done"] = True
                        return 0
                    state["chunk"] = chunk
                    state["offset"] = 0
            except Exception:
                return -1
        return read_cb

    def run_transform(self, script: str, input_stream: Iterable[bytes], input_name: str = "payload", input_mime_type: str = "application/json", input_charset: Optional[str] = None, inputs: Optional[Dict[str, Any]] = None) -> Stream:
        self._require_initialized(self._native.has_callback_input_output, "callback input/output API (run_script_input_output_callback not found)")
        cancelled = Event()
        read_cb = self._chunk_reader(input_stream)
        encoded_inputs = self._inputs_json(inputs)
        def invoke(thread, write_cb):
            return self._native.run_script_input_output_callback(thread, script.encode("utf-8"), encoded_inputs, input_name.encode("utf-8"), input_mime_type.encode("utf-8"), input_charset.encode("utf-8") if input_charset else None, read_cb, write_cb)
        stream = Stream(self._stream_worker(invoke, cancelled))
        stream._on_close = cancelled.set
        stream._cancelled = cancelled
        return stream

    def run_input_output_callback(self, script: str, input_name: str, input_mime_type: str, read_callback: ReadCallback, write_callback: WriteCallback, input_charset: Optional[str] = None, inputs: Optional[Dict[str, Any]] = None) -> StreamingResult:
        self._require_initialized(self._native.has_callback_input_output, "callback input/output API (run_script_input_output_callback not found)")
        @READ_CALLBACK
        def read_cb(_context, buffer, buffer_size):
            try:
                data = read_callback(buffer_size)
                if not data:
                    return 0
                size = min(len(data), buffer_size)
                ctypes.memmove(buffer, data, size)
                return size
            except Exception:
                return -1
        @WRITE_CALLBACK
        def write_cb(_context, buffer, length):
            try:
                return write_callback(ctypes.string_at(buffer, length))
            except Exception:
                return -1
        try:
            ptr = self._native.run_script_input_output_callback(self._native.thread, script.encode("utf-8"), self._inputs_json(inputs), input_name.encode("utf-8"), input_mime_type.encode("utf-8"), input_charset.encode("utf-8") if input_charset else None, read_cb, write_cb)
            raw = self._native.decode_and_free(ptr)
        except Exception as error:
            raise DataWeaveError(f"Failed to execute callback input/output streaming: {error}")
        return parse_streaming_result(json.loads(raw) if raw else {"success": False, "error": "Empty response"})

    def __enter__(self):
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
        return False
