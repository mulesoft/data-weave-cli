from pathlib import Path
import ctypes
from threading import current_thread, get_ident, Thread

import pytest

import dataweave
from dataweave import native


class Function:
    pass


class CallableFunction(Function):
    def __init__(self, callback):
        self.callback = callback

    def __call__(self, *args):
        return self.callback(*args)


class FakeLibrary:
    run_script = Function()
    free_cstring = Function()

    def __init__(self, *, resolver_export=True):
        self.attach_calls = []
        self.detach_calls = []
        self.tear_down_threads = []
        self.created_engines = []
        self.destroyed_engines = []
        self._next_handle = 1
        self.graal_create_isolate = CallableFunction(lambda _params, _isolate, _thread: 0)
        self.graal_attach_thread = CallableFunction(self._attach_thread)
        self.graal_detach_thread = CallableFunction(
            lambda thread: self.detach_calls.append((get_ident(), thread)) or 0
        )
        self.graal_tear_down_isolate = CallableFunction(
            lambda thread: self.tear_down_threads.append(thread) or 0
        )
        self.free_cstring = Function()
        self.create_engine = CallableFunction(self._create_engine)
        self.create_engine_with_resolver = CallableFunction(self._create_engine_with_resolver)
        self.destroy_engine = CallableFunction(
            lambda _thread, handle: self.destroyed_engines.append(handle)
        )
        self.run_script_engine = Function()
        self.run_script_callback_engine = Function()
        self.run_script_input_output_callback_engine = Function()

    def _create_engine(self, _thread):
        handle = self._next_handle
        self._next_handle += 1
        self.created_engines.append((handle, None, None))
        return handle

    def _create_engine_with_resolver(self, _thread, callback, ctx):
        handle = self._next_handle
        self._next_handle += 1
        self.created_engines.append((handle, callback, ctx))
        return handle

    def _attach_thread(self, _isolate, thread):
        worker_thread = native.GraalIsolateThreadPointer()
        ctypes.cast(
            thread,
            ctypes.POINTER(native.GraalIsolateThreadPointer),
        )[0] = worker_thread
        self.attach_calls.append((get_ident(), worker_thread))
        return 0


@pytest.mark.unit
def test_shared_isolate_is_created_once_and_torn_down_on_last_release(monkeypatch):
    library = FakeLibrary()
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)

    a = native.NativeRuntime("/tmp/dwlib")
    b = native.NativeRuntime("/tmp/dwlib")
    a.initialize()
    b.initialize()

    # One shared isolate, two engines, refcount == live engines.
    assert native._isolate_ref_count == 2
    assert len(library.tear_down_threads) == 0
    assert [h for h, _cb, _ctx in library.created_engines] == [a.handle, b.handle]
    assert a.handle != b.handle

    a.cleanup()
    assert native._isolate_ref_count == 1
    assert library.destroyed_engines == [a.handle]
    assert len(library.tear_down_threads) == 0  # isolate stays for b

    b.cleanup()
    assert native._isolate_ref_count == 0
    assert library.destroyed_engines == [a.handle, b.handle]
    assert len(library.tear_down_threads) == 1  # last release tears down

    # Idempotent double-cleanup releases the ref only once.
    b.cleanup()
    assert native._isolate_ref_count == 0
    assert len(library.tear_down_threads) == 1


@pytest.mark.unit
def test_engine_create_failure_releases_isolate_ref(monkeypatch):
    library = FakeLibrary()
    library.create_engine = CallableFunction(
        lambda _thread: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)

    runtime = native.NativeRuntime("/tmp/dwlib")
    with pytest.raises(native.DataWeaveError):
        runtime.initialize()

    # Failed init must leak nothing: the isolate it created is torn down.
    assert native._isolate_ref_count == 0
    assert native._isolate is None
    assert len(library.tear_down_threads) == 1
    assert runtime.initialized is False


@pytest.mark.unit
def test_parse_native_response_rejects_malformed_json():
    result = dataweave._parse_native_encoded_response("not json")

    assert result.success is False
    assert result.error.startswith("Failed to parse native JSON response:")


@pytest.mark.unit
@pytest.mark.parametrize("raw, expected_error", [
    (None, "Native returned null"),
    ("", "Native returned empty response"),
    ("[]", "Native response JSON is not an object"),
])
def test_parse_native_response_rejects_invalid_native_values(raw, expected_error):
    result = dataweave._parse_native_encoded_response(raw)

    assert result == dataweave.ExecutionResult(False, None, expected_error, False, None, None)


@pytest.mark.unit
def test_candidate_paths_prioritize_environment_override(monkeypatch, tmp_path):
    override = tmp_path / "custom-dwlib"
    monkeypatch.setenv("DATAWEAVE_NATIVE_LIB", str(override))

    paths = dataweave._candidate_library_paths()

    assert paths[0] == override
    assert paths[1:4] == [
        Path(dataweave.__file__).resolve().parent / "native" / "dwlib.dylib",
        Path(dataweave.__file__).resolve().parent / "native" / "dwlib.so",
        Path(dataweave.__file__).resolve().parent / "native" / "dwlib.dll",
    ]


@pytest.mark.unit
def test_decode_and_free_releases_native_string_when_decoding_fails(monkeypatch):
    freed = []
    runtime = native.NativeRuntime.__new__(native.NativeRuntime)
    runtime.thread = "thread"
    runtime.lib = type("Native", (), {"free_cstring": lambda _self, thread, ptr: freed.append((thread, ptr))})()
    monkeypatch.setattr(native.ctypes, "string_at", lambda _ptr: b"\xff")

    with pytest.raises(UnicodeDecodeError):
        runtime.decode_and_free(123)

    assert freed == [("thread", 123)]


@pytest.mark.unit
def test_decode_and_free_preserves_decode_failure_when_free_also_fails(monkeypatch):
    runtime = native.NativeRuntime.__new__(native.NativeRuntime)
    runtime.thread = "thread"
    runtime.lib = type("Native", (), {"free_cstring": lambda _self, _thread, _ptr: (_ for _ in ()).throw(RuntimeError("free failed"))})()
    monkeypatch.setattr(native.ctypes, "string_at", lambda _ptr: b"\xff")

    with pytest.raises(UnicodeDecodeError):
        runtime.decode_and_free(123)


@pytest.mark.unit
def test_native_runtime_registers_abi_and_cleans_up_idempotently(monkeypatch):
    library = FakeLibrary()
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    runtime = native.NativeRuntime("/tmp/dwlib")
    runtime.initialize()
    runtime.cleanup()
    runtime.cleanup()

    assert library.run_script_engine.argtypes[1:] == [
        native.ctypes.c_int64, native.ctypes.c_char_p, native.ctypes.c_char_p,
    ]
    assert library.free_cstring.argtypes[1] is native.ctypes.c_void_p
    assert len(library.tear_down_threads) == 1
    assert runtime.initialized is False


@pytest.mark.unit
def test_buffered_worker_execution_uses_one_current_thread_attachment_for_run_decode_and_free(monkeypatch):
    calls = []
    buffer = ctypes.create_string_buffer(b"result")
    library = FakeLibrary()
    library.run_script_engine = CallableFunction(
        lambda thread, _handle, _script, _inputs: calls.append(("run", get_ident(), thread))
        or ctypes.addressof(buffer)
    )
    library.free_cstring = CallableFunction(
        lambda thread, _ptr: calls.append(("free", get_ident(), thread))
    )
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    runtime = native.NativeRuntime("/tmp/dwlib")
    runtime.initialize()
    owner_ident = get_ident()
    outcomes = []

    worker = Thread(
        target=lambda: outcomes.append(
            (get_ident(), runtime.run_engine_and_decode(b"script", b"{}"))
        )
    )
    worker.start()
    worker.join(1)

    assert not worker.is_alive()
    worker_ident, result = outcomes[0]
    assert worker_ident != owner_ident
    assert result == "result"
    assert runtime._owner_thread is current_thread()
    assert len(library.attach_calls) == 1
    assert library.attach_calls[0][0] == worker_ident
    worker_pointer = ctypes.cast(calls[0][2], ctypes.c_void_p).value
    assert [(name, ident) for name, ident, _thread in calls] == [
        ("run", worker_ident),
        ("free", worker_ident),
    ]
    assert all(
        ctypes.cast(thread, ctypes.c_void_p).value == worker_pointer
        for _name, _ident, thread in calls
    )
    assert library.detach_calls[0][0] == worker_ident
    assert ctypes.cast(library.detach_calls[0][1], ctypes.c_void_p).value == worker_pointer


@pytest.mark.unit
def test_distinct_thread_object_attaches_when_python_thread_ident_is_reused(monkeypatch):
    buffer = ctypes.create_string_buffer(b"result")
    library = FakeLibrary()
    library.run_script_engine = CallableFunction(
        lambda _thread, _handle, _script, _inputs: ctypes.addressof(buffer)
    )
    library.free_cstring = CallableFunction(lambda _thread, _ptr: None)
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    monkeypatch.setattr(native, "get_ident", lambda: 7)
    runtime = native.NativeRuntime("/tmp/dwlib")
    runtime.initialize()
    owner_thread = current_thread()
    observed_threads = []

    worker = Thread(
        target=lambda: (
            observed_threads.append(current_thread()),
            runtime.run_engine_and_decode(b"script", b"{}"),
        )
    )
    worker.start()
    worker.join(1)

    assert not worker.is_alive()
    assert observed_threads == [worker]
    assert observed_threads[0] is not owner_thread
    assert runtime._owner_thread is owner_thread
    assert len(library.attach_calls) == 1
    assert len(library.detach_calls) == 1


@pytest.mark.unit
def test_cleanup_clears_owner_thread_reference(monkeypatch):
    library = FakeLibrary()
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    runtime = native.NativeRuntime("/tmp/dwlib")
    runtime.initialize()

    assert runtime._owner_thread is current_thread()

    runtime.cleanup()

    assert runtime._owner_thread is None
    assert native._isolate_ref_count == 0


@pytest.mark.unit
@pytest.mark.parametrize("failure", ["run", "decode", "free", "detach"])
def test_buffered_worker_execution_detaches_current_thread_after_failure(monkeypatch, failure):
    buffer = ctypes.create_string_buffer(b"result")
    library = FakeLibrary()

    def run_script_engine(_thread, _handle, _script, _inputs):
        if failure == "run":
            raise RuntimeError("run failed")
        return ctypes.addressof(buffer)

    def free_cstring(_thread, _ptr):
        if failure == "free":
            raise RuntimeError("free failed")

    library.run_script_engine = CallableFunction(run_script_engine)
    library.free_cstring = CallableFunction(free_cstring)
    if failure == "detach":
        library.graal_detach_thread = CallableFunction(
            lambda _thread: (_ for _ in ()).throw(RuntimeError("detach failed"))
        )
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    if failure == "decode":
        monkeypatch.setattr(native.ctypes, "string_at", lambda _ptr: b"\xff")
    runtime = native.NativeRuntime("/tmp/dwlib")
    runtime.initialize()
    errors = []

    worker = Thread(
        target=lambda: _capture_error(
            errors,
            lambda: runtime.run_engine_and_decode(b"script", b"{}"),
        )
    )
    worker.start()
    worker.join(1)

    assert not worker.is_alive()
    assert len(errors) == 1
    if failure == "detach":
        assert "detach failed" in str(errors[0])
    assert len(library.attach_calls) == 1
    if failure != "detach":
        assert len(library.detach_calls) == 1
        assert library.detach_calls[0][0] == library.attach_calls[0][0]


def _capture_error(errors, invoke):
    try:
        invoke()
    except Exception as error:
        errors.append(error)


@pytest.mark.unit
def test_cleanup_from_worker_uses_current_thread_for_isolate_teardown(monkeypatch):
    library = FakeLibrary()
    teardown_calls = []
    library.graal_tear_down_isolate = CallableFunction(
        lambda thread: teardown_calls.append((get_ident(), thread)) or 0
    )
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    runtime = native.NativeRuntime("/tmp/dwlib")
    runtime.initialize()
    owner_ident = get_ident()

    worker = Thread(target=runtime.cleanup)
    worker.start()
    worker.join(1)

    assert not worker.is_alive()
    worker_ident, teardown_thread = teardown_calls[0]
    assert worker_ident != owner_ident
    assert library.attach_calls[0][0] == worker_ident
    assert ctypes.cast(teardown_thread, ctypes.c_void_p).value == ctypes.cast(
        library.attach_calls[0][1], ctypes.c_void_p
    ).value
    assert library.detach_calls == []
    assert runtime.initialized is False


@pytest.mark.unit
def test_native_runtime_wraps_library_load_errors(monkeypatch):
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: (_ for _ in ()).throw(OSError("bad image")))

    with pytest.raises(dataweave.DataWeaveError, match="Failed to load library from /tmp/dwlib: bad image"):
        native.NativeRuntime("/tmp/dwlib").initialize()


@pytest.mark.unit
def test_initialize_resets_state_when_isolate_creation_fails(monkeypatch):
    library = FakeLibrary()
    library.graal_create_isolate = CallableFunction(lambda _params, _isolate, _thread: 9)
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    runtime = native.NativeRuntime("/tmp/dwlib")

    with pytest.raises(dataweave.DataWeaveError, match="Failed to create GraalVM isolate. Error code: 9"):
        runtime.initialize()

    assert runtime.lib is None
    assert runtime.isolate is None
    assert runtime.thread is None
    assert runtime.initialized is False
    assert native._isolate_ref_count == 0
    assert native._isolate is None


@pytest.mark.unit
def test_initialize_requires_create_isolate_export(monkeypatch):
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: object())

    with pytest.raises(dataweave.DataWeaveError, match="Native library does not export graal_create_isolate"):
        native.NativeRuntime("/tmp/dwlib").initialize()


@pytest.mark.unit
def test_initialize_wraps_create_isolate_exception(monkeypatch):
    library = FakeLibrary()
    library.graal_create_isolate = CallableFunction(
        lambda _params, _isolate, _thread: (_ for _ in ()).throw(RuntimeError("native create failure"))
    )
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)

    with pytest.raises(dataweave.DataWeaveError, match="Failed to create GraalVM isolate: native create failure"):
        native.NativeRuntime("/tmp/dwlib").initialize()

    assert native._isolate_ref_count == 0
    assert native._isolate is None


@pytest.mark.unit
@pytest.mark.parametrize("missing_symbol", ["graal_attach_thread", "graal_detach_thread"])
def test_initialize_rejects_streaming_export_without_thread_lifecycle_symbols(monkeypatch, missing_symbol):
    class Function:
        def __call__(self, *_args):
            return 0

    library = type("Native", (), {})()
    library.graal_create_isolate = Function()
    library.graal_tear_down_isolate = Function()
    library.run_script = Function()
    library.free_cstring = Function()
    library.run_script_callback = Function()
    for symbol in ("graal_attach_thread", "graal_detach_thread"):
        if symbol != missing_symbol:
            setattr(library, symbol, Function())
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)

    with pytest.raises(dataweave.DataWeaveError, match=f"Native library does not export {missing_symbol}"):
        native.NativeRuntime("/tmp/dwlib").initialize()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("method_name", "error_message"),
    [
        ("attach_thread", "Failed to attach worker thread to isolate: native attach failure"),
        ("detach_thread", "Failed to detach worker thread from isolate: native detach failure"),
    ],
)
def test_thread_lifecycle_wraps_native_invocation_errors(method_name, error_message):
    class Native:
        def graal_attach_thread(self, _isolate, _thread):
            raise RuntimeError("native attach failure")

        def graal_detach_thread(self, _thread):
            raise RuntimeError("native detach failure")

    runtime = native.NativeRuntime.__new__(native.NativeRuntime)
    runtime.lib = Native()
    runtime.isolate = object()

    with pytest.raises(dataweave.DataWeaveError, match=error_message):
        if method_name == "attach_thread":
            runtime.attach_thread()
        else:
            runtime.detach_thread(object())
