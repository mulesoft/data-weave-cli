import ctypes
from queue import Full, Queue
from threading import Event, Thread
from time import sleep

import pytest

import dataweave
from dataweave import runtime as runtime_module


class FakeNative:
    def __init__(self, metadata=None, attach_code=0, emit=b"", consume_input=False):
        self.metadata = metadata
        self.attach_code = attach_code
        self.emit = emit
        self.consume_input = consume_input
        self.detached = []
        self.freed = []
        self._buffers = []
        self.detached_event = Event()

    def graal_attach_thread(self, _isolate, _thread):
        return self.attach_code

    def graal_detach_thread(self, thread):
        self.detached.append(thread)
        self.detached_event.set()
        return 0

    def free_cstring(self, thread, ptr):
        self.freed.append((thread, ptr))

    def _response_pointer(self):
        if self.metadata is None:
            return None
        buffer = ctypes.create_string_buffer(self.metadata.encode("utf-8"))
        self._buffers.append(buffer)
        return ctypes.addressof(buffer)

    def run_script_callback(self, _thread, _script, _inputs, write_callback, _context):
        if self.emit:
            buffer = ctypes.create_string_buffer(self.emit)
            self.write_status = write_callback(None, ctypes.addressof(buffer), len(self.emit))
        return self._response_pointer()

    def run_script_input_output_callback(
        self, _thread, _script, _inputs, _input_name, _mime_type, _charset, read_callback, write_callback, _context,
    ):
        if self.consume_input:
            buffer = ctypes.create_string_buffer(3)
            read = []
            while True:
                size = read_callback(None, ctypes.addressof(buffer), len(buffer))
                self.read_status = size
                if size == 0:
                    break
                if size < 0:
                    return self._response_pointer()
                read.append(bytes(buffer.raw[:size]))
            self.read_input = b"".join(read)
        if self.emit:
            buffer = ctypes.create_string_buffer(self.emit)
            assert write_callback(None, ctypes.addressof(buffer), len(self.emit)) == 0
        return self._response_pointer()


def configured_runtime(native):
    runtime = dataweave.DataWeave.__new__(dataweave.DataWeave)
    native_runtime = runtime_module.NativeRuntime.__new__(runtime_module.NativeRuntime)
    native_runtime.initialized = True
    native_runtime.has_callback_streaming = True
    native_runtime.has_callback_input_output = True
    native_runtime.lib = native
    native_runtime.isolate = object()
    native_runtime.thread = object()
    runtime._native = native_runtime
    return runtime


@pytest.mark.unit
def test_run_callback_converts_write_callback_exception_to_abort_result():
    native = FakeNative('{"success": false, "error": "callback aborted"}', emit=b"chunk")
    runtime = configured_runtime(native)

    result = runtime.run_callback("script", lambda _chunk: (_ for _ in ()).throw(RuntimeError("stop")))

    assert result == dataweave.StreamingResult(False, "callback aborted", None, None, False)
    assert native.write_status == -1


@pytest.mark.unit
def test_run_input_output_callback_converts_read_exception_to_abort_result():
    native = FakeNative('{"success": false, "error": "read aborted"}', consume_input=True)
    runtime = configured_runtime(native)

    result = runtime.run_input_output_callback(
        "script", "payload", "application/json", lambda _size: (_ for _ in ()).throw(RuntimeError("stop")), lambda _data: 0,
    )

    assert result == dataweave.StreamingResult(False, "read aborted", None, None, False)
    assert native.read_status == -1


@pytest.mark.unit
def test_run_transform_preserves_remainder_of_large_input_chunk():
    native = FakeNative('{"success": true, "mimeType": "application/json", "charset": "utf-8"}', consume_input=True)
    runtime = configured_runtime(native)
    stream = runtime.run_transform("script", [b"abcdefgh"], input_mime_type="application/json")

    assert list(stream) == []
    assert native.read_input == b"abcdefgh"
    assert stream.metadata == dataweave.StreamingResult(True, None, "application/json", "utf-8", False)


@pytest.mark.unit
def test_run_streaming_returns_failure_metadata_when_worker_produces_no_metadata(monkeypatch):
    class MetadataDroppingQueue(Queue):
        def put(self, item, *args, **kwargs):
            if isinstance(item, dict):
                return None
            return super().put(item, *args, **kwargs)

    monkeypatch.setattr(runtime_module, "Queue", MetadataDroppingQueue)
    runtime = configured_runtime(FakeNative('{"success": true}'))
    stream = runtime.run_streaming("script")

    assert list(stream) == []
    assert stream.metadata == dataweave.StreamingResult(False, "No metadata received from native call", None, None, False)


@pytest.mark.unit
def test_run_streaming_returns_attach_failure_without_detaching_unattached_thread():
    native = FakeNative(attach_code=9)
    runtime = configured_runtime(native)
    stream = runtime.run_streaming("script")

    assert list(stream) == []
    assert stream.metadata == dataweave.StreamingResult(False, "Failed to attach worker thread to isolate (code 9)", None, None, False)
    assert native.detached == []


@pytest.mark.unit
def test_stream_private_close_aborts_worker_and_detaches_after_consumer_abandons_output():
    class BlockingFakeNative(FakeNative):
        def __init__(self):
            super().__init__('{"success": false, "error": "aborted"}')
            self.first_chunk_written = Event()
            self.cancelled = None

        def run_script_callback(self, _thread, _script, _inputs, write_callback, _context):
            first = ctypes.create_string_buffer(b"first")
            assert write_callback(None, ctypes.addressof(first), 5) == 0
            self.first_chunk_written.set()
            assert self.cancelled.wait(1)
            second = ctypes.create_string_buffer(b"second")
            self.write_status = write_callback(None, ctypes.addressof(second), 6)
            return self._response_pointer()

    native = BlockingFakeNative()
    runtime = configured_runtime(native)
    stream = runtime.run_streaming("script")
    native.cancelled = stream._cancelled

    assert next(stream) == b"first"
    assert native.first_chunk_written.wait(1)
    stream._close()

    assert native.detached_event.wait(1)
    assert native.write_status == -1


@pytest.mark.unit
def test_stream_early_close_does_not_block_terminal_publication_on_full_queue(monkeypatch):
    cancelled = []
    terminal_blocked = Event()

    class CancellationAwareQueue(Queue):
        def put(self, item, block=True, timeout=None):
            if not isinstance(item, bytes) and cancelled[0].is_set():
                if timeout is None:
                    terminal_blocked.set()
                raise Full
            return super().put(item, block, timeout)

    class FullQueueFakeNative(FakeNative):
        def __init__(self):
            super().__init__('{"success": false, "error": "aborted"}')
            self.queue_full = Event()
            self.cancelled = None

        def run_script_callback(self, _thread, _script, _inputs, write_callback, _context):
            first = ctypes.create_string_buffer(b"first")
            assert write_callback(None, ctypes.addressof(first), 5) == 0
            second = ctypes.create_string_buffer(b"second")
            assert write_callback(None, ctypes.addressof(second), 6) == 0
            self.queue_full.set()
            assert self.cancelled.wait(1)
            third = ctypes.create_string_buffer(b"third")
            self.write_status = write_callback(None, ctypes.addressof(third), 5)
            return self._response_pointer()

    monkeypatch.setattr(runtime_module, "Queue", CancellationAwareQueue)
    monkeypatch.setattr(runtime_module, "_OUTPUT_QUEUE_MAXSIZE", 1)
    native = FullQueueFakeNative()
    runtime = configured_runtime(native)
    stream = runtime.run_streaming("script")
    cancelled.append(stream._cancelled)
    native.cancelled = stream._cancelled

    assert next(stream) == b"first"
    assert native.queue_full.wait(1)
    stream._close()

    assert native.write_status == -1
    assert native.detached_event.wait(1)
    assert terminal_blocked.is_set() is False


@pytest.mark.unit
def test_runtime_module_owns_dataweave_orchestration():
    assert dataweave.DataWeave is runtime_module.DataWeave


@pytest.mark.unit
def test_run_streaming_reports_worker_timeout_when_native_call_produces_no_output(monkeypatch):
    class BlockingFakeNative(FakeNative):
        def run_script_callback(self, _thread, _script, _inputs, _write_callback, _context):
            sleep(0.05)
            return self._response_pointer()

    monkeypatch.setattr(runtime_module, "_WORKER_TIMEOUT_SECONDS", 0.01)
    runtime = configured_runtime(BlockingFakeNative('{"success": true}'))

    with pytest.raises(dataweave.DataWeaveError, match="Worker thread timeout after 0.01 seconds"):
        list(runtime.run_streaming("script"))


@pytest.mark.unit
def test_run_streaming_surfaces_detach_failure_without_primary_execution_failure():
    class DetachFailingNative(FakeNative):
        def graal_detach_thread(self, thread):
            super().graal_detach_thread(thread)
            return 7

    runtime = configured_runtime(DetachFailingNative('{"success": true}'))

    with pytest.raises(dataweave.DataWeaveError, match="Failed to detach worker thread from isolate. Error code: 7"):
        list(runtime.run_streaming("script"))
