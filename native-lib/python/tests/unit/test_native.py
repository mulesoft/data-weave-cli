from pathlib import Path
import ctypes

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

    def __init__(self, *, resolver_export=False):
        self.tear_down_threads = []
        self.graal_create_isolate = CallableFunction(lambda _params, _isolate, _thread: 0)
        self.graal_tear_down_isolate = CallableFunction(
            lambda thread: self.tear_down_threads.append(thread) or 0
        )
        if resolver_export:
            self.run_script_with_resolver = Function()


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

    assert library.run_script.argtypes[1:] == [native.ctypes.c_char_p, native.ctypes.c_char_p]
    assert library.free_cstring.argtypes[1] is native.ctypes.c_void_p
    assert len(library.tear_down_threads) == 1
    assert runtime.initialized is False


@pytest.mark.unit
def test_native_runtime_registers_optional_module_resolver_export(monkeypatch):
    library = FakeLibrary(resolver_export=True)
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)

    runtime = native.NativeRuntime("/tmp/dwlib")
    runtime.initialize()

    assert runtime.has_module_resolver is True
    assert library.run_script_with_resolver.argtypes == [
        native.GraalIsolateThreadPointer,
        native.ctypes.c_char_p,
        native.ctypes.c_char_p,
        dataweave.RESOLVE_MODULE_CALLBACK,
    ]
    assert library.run_script_with_resolver.restype is native.ctypes.c_void_p


@pytest.mark.unit
def test_native_runtime_initializes_without_optional_module_resolver_export(monkeypatch):
    library = FakeLibrary()
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)

    runtime = native.NativeRuntime("/tmp/dwlib")
    runtime.initialize()

    assert runtime.has_module_resolver is False


@pytest.mark.unit
def test_run_script_with_resolver_adapts_path_and_retains_source_buffer(monkeypatch):
    observed = []
    resolver_paths = []
    library = FakeLibrary(resolver_export=True)

    def invoke(_thread, _script, _inputs, callback):
        address = callback(None, b"/org/test/lib.dwl")
        observed.append(ctypes.string_at(address).decode("utf-8"))
        assert library.runtime._resolver_buffers
        return 0

    library.run_script_with_resolver = CallableFunction(invoke)
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    runtime = native.NativeRuntime("/tmp/dwlib")
    library.runtime = runtime
    runtime.initialize()
    stale_buffer = ctypes.create_string_buffer(b"stale")
    runtime._resolver_buffers.append(stale_buffer)

    resolver = lambda path: resolver_paths.append(path) or "module source"
    result = runtime.run_script_with_resolver("thread", b"script", b"{}", resolver)

    assert result == 0
    assert resolver_paths == ["org/test/lib.dwl"]
    assert observed == ["module source"]
    assert runtime._resolver_buffers == []


@pytest.mark.unit
@pytest.mark.parametrize(
    ("module_path", "resolver"),
    [
        (b"/missing.dwl", lambda _path: None),
        (b"/invalid.dwl", lambda _path: 42),
        (b"\xff", lambda _path: "unreachable"),
    ],
)
def test_resolver_callback_returns_null_for_unresolved_or_invalid_values(
    monkeypatch, module_path, resolver
):
    addresses = []
    library = FakeLibrary(resolver_export=True)
    library.run_script_with_resolver = CallableFunction(
        lambda _thread, _script, _inputs, callback: addresses.append(
            callback(None, module_path)
        ) or 0
    )
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    runtime = native.NativeRuntime("/tmp/dwlib")
    runtime.initialize()

    runtime.run_script_with_resolver("thread", b"script", b"{}", resolver)

    assert addresses == [None]
    assert runtime._resolver_buffers == []


@pytest.mark.unit
def test_resolver_callback_contains_exceptions_and_hides_details_by_default(
    monkeypatch, capsys
):
    addresses = []
    library = FakeLibrary(resolver_export=True)
    library.run_script_with_resolver = CallableFunction(
        lambda _thread, _script, _inputs, callback: addresses.append(
            callback(None, b"/org/test/lib.dwl")
        ) or 0
    )
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    monkeypatch.delenv("DATAWEAVE_RESOLVER_DEBUG", raising=False)
    runtime = native.NativeRuntime("/tmp/dwlib")
    runtime.initialize()

    def resolver(_path):
        raise RuntimeError("secret /private/path")

    runtime.run_script_with_resolver("thread", b"script", b"{}", resolver)

    captured = capsys.readouterr()
    assert addresses == [None]
    assert "DataWeave module resolver callback failed." in captured.err
    assert "secret" not in captured.err
    assert "/private/path" not in captured.err


@pytest.mark.unit
def test_resolver_callback_prints_exception_details_in_debug_mode(
    monkeypatch, capsys
):
    library = FakeLibrary(resolver_export=True)
    library.run_script_with_resolver = CallableFunction(
        lambda _thread, _script, _inputs, callback: callback(
            None, b"/org/test/lib.dwl"
        ) or 0
    )
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    monkeypatch.setenv("DATAWEAVE_RESOLVER_DEBUG", "1")
    runtime = native.NativeRuntime("/tmp/dwlib")
    runtime.initialize()

    def resolver(_path):
        raise RuntimeError("secret /private/path")

    runtime.run_script_with_resolver("thread", b"script", b"{}", resolver)

    captured = capsys.readouterr()
    assert "RuntimeError: secret /private/path" in captured.err


@pytest.mark.unit
def test_run_script_with_resolver_clears_buffers_when_native_call_fails(monkeypatch):
    library = FakeLibrary(resolver_export=True)

    def invoke(_thread, _script, _inputs, callback):
        assert callback(None, b"/org/test/lib.dwl")
        raise RuntimeError("native failure")

    library.run_script_with_resolver = CallableFunction(invoke)
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    runtime = native.NativeRuntime("/tmp/dwlib")
    runtime.initialize()

    with pytest.raises(RuntimeError, match="native failure"):
        runtime.run_script_with_resolver(
            "thread", b"script", b"{}", lambda _path: "module source"
        )

    assert runtime._resolver_buffers == []


@pytest.mark.unit
def test_run_script_with_resolver_rejects_missing_native_export(monkeypatch):
    library = FakeLibrary()
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    runtime = native.NativeRuntime("/tmp/dwlib")
    runtime.initialize()

    with pytest.raises(
        dataweave.DataWeaveError,
        match=r"Native library does not support module resolver API \(run_script_with_resolver not found\)\.",
    ):
        runtime.run_script_with_resolver(
            "thread", b"script", b"{}", lambda _path: "module source"
        )


@pytest.mark.unit
def test_native_runtime_retains_one_resolver_callback_until_teardown(monkeypatch):
    retained_during_teardown = []
    library = FakeLibrary(resolver_export=True)
    library.run_script_with_resolver = CallableFunction(
        lambda _thread, _script, _inputs, _callback: 0
    )
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    runtime = native.NativeRuntime("/tmp/dwlib")
    library.graal_tear_down_isolate = CallableFunction(
        lambda _thread: retained_during_teardown.append(
            runtime._module_resolver_callback is not None
        ) or 0
    )
    runtime.initialize()
    resolver = lambda _path: "module source"

    runtime.run_script_with_resolver("thread", b"script", b"{}", resolver)
    callback = runtime._module_resolver_callback
    runtime.run_script_with_resolver("thread", b"script", b"{}", resolver)

    assert runtime._module_resolver_callback is callback
    with pytest.raises(dataweave.DataWeaveError):
        runtime.run_script_with_resolver(
            "thread", b"script", b"{}", lambda _path: "other source"
        )

    runtime.cleanup()

    assert retained_during_teardown == [True]
    assert runtime._module_resolver_callback is None
    assert runtime._module_resolver is None


@pytest.mark.unit
def test_cleanup_retains_resolver_state_when_isolate_teardown_fails(monkeypatch):
    library = FakeLibrary(resolver_export=True)
    library.run_script_with_resolver = CallableFunction(
        lambda _thread, _script, _inputs, _callback: 0
    )
    library.graal_tear_down_isolate = CallableFunction(lambda _thread: 7)
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)
    runtime = native.NativeRuntime("/tmp/dwlib")
    runtime.initialize()
    resolver = lambda _path: "module source"
    runtime.run_script_with_resolver("thread", b"script", b"{}", resolver)
    callback = runtime._module_resolver_callback

    with pytest.raises(
        dataweave.DataWeaveError,
        match="Failed to tear down GraalVM isolate. Error code: 7",
    ):
        runtime.cleanup()

    assert runtime._module_resolver is resolver
    assert runtime._module_resolver_callback is callback


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
def test_initialize_requires_create_isolate_export(monkeypatch):
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: object())

    with pytest.raises(dataweave.DataWeaveError, match="Native library does not export graal_create_isolate"):
        native.NativeRuntime("/tmp/dwlib").initialize()


@pytest.mark.unit
def test_initialize_wraps_create_isolate_exception(monkeypatch):
    class Function:
        def __call__(self, *_args):
            raise RuntimeError("native create failure")

    library = type("Native", (), {"graal_create_isolate": Function()})()
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: library)

    with pytest.raises(dataweave.DataWeaveError, match="Failed to create GraalVM isolate: native create failure"):
        native.NativeRuntime("/tmp/dwlib").initialize()


@pytest.mark.unit
def test_cleanup_wraps_teardown_exception():
    runtime = native.NativeRuntime.__new__(native.NativeRuntime)
    runtime.initialized = True
    runtime.thread = object()
    runtime.isolate = object()
    runtime.lib = type("Native", (), {"graal_tear_down_isolate": lambda _self, _thread: (_ for _ in ()).throw(RuntimeError("native teardown failure"))})()

    with pytest.raises(dataweave.DataWeaveError, match="Failed to tear down GraalVM isolate: native teardown failure"):
        runtime.cleanup()


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
