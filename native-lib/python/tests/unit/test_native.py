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
    runtime = dataweave.DataWeave.__new__(dataweave.DataWeave)
    runtime._thread = "thread"
    runtime._lib = type("Native", (), {"free_cstring": lambda _self, thread, ptr: freed.append((thread, ptr))})()
    monkeypatch.setattr(dataweave.ctypes, "string_at", lambda _ptr: b"\xff")

    with pytest.raises(UnicodeDecodeError):
        runtime._decode_and_free(123)

    assert freed == [("thread", 123)]


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
