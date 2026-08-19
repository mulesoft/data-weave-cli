from pathlib import Path

import pytest

import dataweave
from dataweave import native


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
    class Function:
        pass

    class FakeLibrary:
        run_script = Function()
        free_cstring = Function()
        graal_attach_thread = Function()
        graal_detach_thread = Function()

        def __init__(self):
            self.tear_down_threads = []
            self.graal_create_isolate = Function()
            self.graal_create_isolate.__call__ = lambda _params, _isolate, _thread: 0
            self.graal_tear_down_isolate = Function()
            self.graal_tear_down_isolate.__call__ = lambda thread: self.tear_down_threads.append(thread) or 0

    class CallableFunction(Function):
        def __init__(self, callback):
            self.callback = callback

        def __call__(self, *args):
            return self.callback(*args)

    library = FakeLibrary()
    library.graal_create_isolate = CallableFunction(lambda _params, _isolate, _thread: 0)
    library.graal_tear_down_isolate = CallableFunction(lambda thread: library.tear_down_threads.append(thread) or 0)

    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    runtime = native.NativeRuntime("/tmp/dwlib")
    runtime.initialize()
    runtime.cleanup()
    runtime.cleanup()

    assert library.run_script.argtypes[1:] == [native.ctypes.c_char_p, native.ctypes.c_char_p]
    assert library.free_cstring.argtypes[1] is native.ctypes.c_void_p
    assert len(library.tear_down_threads) == 1
    assert runtime.initialized is False


@pytest.mark.unit
def test_native_runtime_wraps_library_load_errors(monkeypatch):
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: (_ for _ in ()).throw(OSError("bad image")))

    with pytest.raises(dataweave.DataWeaveError, match="Failed to load library from /tmp/dwlib: bad image"):
        native.NativeRuntime("/tmp/dwlib").initialize()


@pytest.mark.unit
def test_initialize_resets_state_when_isolate_creation_fails(monkeypatch):
    class Function:
        def __call__(self, *_args):
            return 9

    library = type("Native", (), {"graal_create_isolate": Function()})()
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    runtime = native.NativeRuntime("/tmp/dwlib")

    with pytest.raises(dataweave.DataWeaveError, match="Failed to create GraalVM isolate. Error code: 9"):
        runtime.initialize()

    assert runtime.lib is None
    assert runtime.isolate is None
    assert runtime.thread is None
    assert runtime.initialized is False


@pytest.mark.unit
def test_initialize_tears_down_isolate_when_required_export_is_missing(monkeypatch):
    class Function:
        def __init__(self, callback):
            self.callback = callback

        def __call__(self, *args):
            return self.callback(*args)

    torn_down = []
    library = type("Native", (), {})()
    library.graal_create_isolate = Function(lambda _params, _isolate, _thread: 0)
    library.graal_tear_down_isolate = Function(lambda thread: torn_down.append(thread) or 0)
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    runtime = native.NativeRuntime("/tmp/dwlib")

    with pytest.raises(dataweave.DataWeaveError, match="Native library does not export run_script"):
        runtime.initialize()

    assert len(torn_down) == 1
    assert runtime.lib is None
    assert runtime.isolate is None
    assert runtime.thread is None


@pytest.mark.unit
def test_initialize_rejects_streaming_export_without_required_lifecycle_symbols(monkeypatch):
    class Function:
        def __init__(self, callback=lambda *_args: 0):
            self.callback = callback

        def __call__(self, *args):
            return self.callback(*args)

    torn_down = []
    library = type("Native", (), {})()
    library.graal_create_isolate = Function()
    library.graal_tear_down_isolate = Function(lambda thread: torn_down.append(thread) or 0)
    library.run_script = Function()
    library.run_script_callback = Function()
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)

    with pytest.raises(dataweave.DataWeaveError, match="Native library does not export free_cstring"):
        native.NativeRuntime("/tmp/dwlib").initialize()

    assert len(torn_down) == 1


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

    with pytest.raises(dataweave.DataWeaveError, match=f"run_script_callback requires native export {missing_symbol}"):
        native.NativeRuntime("/tmp/dwlib").initialize()


@pytest.mark.unit
def test_cleanup_surfaces_native_teardown_error_code(monkeypatch):
    runtime = native.NativeRuntime.__new__(native.NativeRuntime)
    runtime.initialized = True
    runtime.thread = object()
    runtime.isolate = object()
    runtime.lib = type("Native", (), {"graal_tear_down_isolate": lambda _self, _thread: 7})()

    with pytest.raises(dataweave.DataWeaveError, match="Failed to tear down GraalVM isolate. Error code: 7"):
        runtime.cleanup()

    assert runtime.lib is None
    assert runtime.thread is None
    assert runtime.isolate is None
