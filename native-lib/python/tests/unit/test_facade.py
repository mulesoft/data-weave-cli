import ctypes
import inspect
import threading

import pytest

import dataweave
from dataweave import native, runtime


class FakeNativeRuntime:
    def __init__(self):
        self.initialized = True
        self.thread = "thread"
        self.operation = native._EngineOperation(handle=7, generation=1)
        self.calls = []

    def capture_operation(self):
        return self.operation

    def run_engine_and_decode(self, script, inputs, *, operation):
        assert operation is self.operation
        assert operation.handle == 7
        self.calls.append(("run_engine_and_decode", script, inputs, operation))
        return self._result()

    @staticmethod
    def _result():
        return '{"success": true, "result": "SGVsbG8=", "binary": false, "mimeType": "text/plain", "charset": "utf-8"}'


def configured_runtime(resolve_module=None):
    instance = dataweave.DataWeave.__new__(dataweave.DataWeave)
    instance._native = FakeNativeRuntime()
    instance._resolve_module = resolve_module
    return instance


@pytest.mark.unit
def test_facade_exports_explicit_instance_api():
    expected_exports = {
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
        "RESOLVE_MODULE_CALLBACK",
        "WRITE_CALLBACK",
        "ModuleResolver",
        "compose_resolvers",
        "modules_from_directory",
        "modules_from_jars",
        "modules_from_map",
    }

    assert set(dataweave.__all__) == expected_exports
    for name in expected_exports:
        getattr(dataweave, name)


@pytest.mark.unit
@pytest.mark.parametrize(
    "name",
    [
        "run",
        "run_streaming",
        "run_transform",
        "run_callback",
        "run_input_output_callback",
        "cleanup",
    ],
)
def test_facade_does_not_export_module_execution_or_cleanup(name):
    assert name not in dataweave.__all__
    assert not hasattr(dataweave, name)


@pytest.mark.unit
def test_dataweave_constructor_stores_keyword_only_module_resolver(monkeypatch):
    resolver = lambda _path: "source"
    native_runtime = FakeNativeRuntime()
    monkeypatch.setattr(runtime, "NativeRuntime", lambda _lib_path: native_runtime)

    instance = dataweave.DataWeave(resolve_module=resolver)

    assert instance._resolve_module is resolver
    assert inspect.signature(dataweave.DataWeave).parameters[
        "resolve_module"
    ].kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.unit
def test_run_uses_engine_execution_regardless_of_resolver():
    resolver = lambda _path: "source"
    instance = configured_runtime(resolver)

    result = instance.run("payload", {"value": 1})

    assert result == dataweave.ExecutionResult(
        True, "SGVsbG8=", None, False, "text/plain", "utf-8"
    )
    assert instance._native.calls == [
        (
            "run_engine_and_decode",
            b"payload",
            b'{"value": {"content": "MQ==", "mimeType": "application/json", "charset": "utf-8"}}',
            instance._native.operation,
        )
    ]


@pytest.mark.unit
def test_run_without_resolver_routes_through_engine():
    instance = configured_runtime()

    result = instance.run("payload")

    assert result == dataweave.ExecutionResult(
        True, "SGVsbG8=", None, False, "text/plain", "utf-8"
    )
    assert instance._native.calls == [
        (
            "run_engine_and_decode",
            b"payload",
            b"{}",
            instance._native.operation,
        )
    ]


class _FakeCallable:
    """A settable-attribute stand-in for a ctypes function pointer: plain
    objects (unlike bound methods) accept `.argtypes`/`.restype` assignment,
    which `native._bind_abi` performs on every ABI export it binds."""

    def __init__(self, callback=None):
        self._callback = callback

    def __call__(self, *args):
        return self._callback(*args) if self._callback else 0


class FakeLifecycleLibrary:
    """Minimal ctypes-library stand-in that lets `NativeRuntime.initialize()`/
    `install_resolver()`/`cleanup()` run for real -- exercising the actual
    module-global `_resolver_registry` / `_isolate_ref_count` bookkeeping in
    `dataweave.native` -- without touching a real native library."""

    def __init__(self):
        self._next_handle = 1
        self.graal_create_isolate = _FakeCallable(lambda _params, _isolate, _thread: 0)
        self.graal_attach_thread = _FakeCallable(self._attach_thread)
        self.graal_detach_thread = _FakeCallable(lambda _thread: 0)
        self.graal_tear_down_isolate = _FakeCallable(lambda _thread: 0)
        self.free_cstring = _FakeCallable()
        self.create_engine = _FakeCallable(self._create_engine)
        self.create_engine_with_resolver = _FakeCallable(self._create_engine_with_resolver)
        self.destroy_engine = _FakeCallable()
        self.run_script_engine = _FakeCallable()
        self.run_script_callback_engine = _FakeCallable()
        self.run_script_input_output_callback_engine = _FakeCallable()

    @staticmethod
    def _attach_thread(_isolate, thread_out):
        ctypes.cast(thread_out, ctypes.POINTER(native.GraalIsolateThreadPointer))[0] = (
            native.GraalIsolateThreadPointer()
        )
        return 0

    def _create_engine(self, _thread):
        handle = self._next_handle
        self._next_handle += 1
        return handle

    def _create_engine_with_resolver(self, _thread, _callback, _ctx):
        handle = self._next_handle
        self._next_handle += 1
        return handle


@pytest.mark.unit
def test_concurrent_initialize_installs_exactly_one_resolver_token(monkeypatch):
    # review #12 finding #2 (Medium): DataWeave.initialize() called
    # self._native.install_resolver(...) then self._native.initialize() with no
    # instance-level lock. Concurrent initialize() calls on the SAME instance
    # can each pass the `if self._native.initialized: return` fast-path and
    # each call install_resolver(), which allocates a fresh token and
    # registers it in the module-global registry before any of them reaches
    # NativeRuntime's own _init_lock-guarded engine creation. Only the last
    # writer's token survives on `self._native._resolver_token`, so cleanup()
    # (which only pops that one token) leaks every earlier registration.
    monkeypatch.setattr(native.ctypes, "CDLL", lambda _path: FakeLifecycleLibrary())

    before = set(native._resolver_registry.keys())
    dw = dataweave.DataWeave(lib_path="/tmp/dwlib", resolve_module=lambda _path: None)

    thread_count = 8

    # A thread_count-party barrier with a timeout, patched into
    # NativeRuntime.initialize (the real engine-creation call, invoked AFTER
    # install_resolver() in DataWeave.initialize()). On the UNFIXED code every
    # thread passes the `if self._native.initialized: return` fast-path
    # concurrently (none of them has finished a full initialize() yet, so the
    # flag is still False for all), so every thread reaches this point having
    # ALREADY called install_resolver() -- reproducing thread_count
    # independent install_resolver() calls, each minting and registering its
    # own token, before any of them performs the real (locked) engine
    # creation. On the FIXED (per-instance-locked) code only one thread is
    # ever inside initialize() at a time, so it is the only caller that ever
    # reaches this barrier; the other threads see `initialized` already True
    # once they acquire the lock and never call install_resolver or this
    # method at all. The lone caller's wait times out, the barrier breaks,
    # and it proceeds normally -- this must NOT deadlock the fixed code.
    native_initialize_barrier = threading.Barrier(thread_count)
    orig_native_initialize = dw._native.initialize

    def synchronized_native_initialize():
        try:
            native_initialize_barrier.wait(timeout=0.5)
        except threading.BrokenBarrierError:
            pass
        return orig_native_initialize()

    monkeypatch.setattr(dw._native, "initialize", synchronized_native_initialize)

    barrier = threading.Barrier(thread_count)
    errors = []

    def go():
        barrier.wait()
        try:
            dw.initialize()
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=go) for _ in range(thread_count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    try:
        assert not errors
        new_tokens = set(native._resolver_registry.keys()) - before
        assert len(new_tokens) == 1  # exactly one token, no orphan
        assert native._isolate_ref_count == 1  # exactly one engine reference
    finally:
        dw.cleanup()
