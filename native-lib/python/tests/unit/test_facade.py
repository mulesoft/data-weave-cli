import inspect
import threading
import time

import pytest

import dataweave
from dataweave import runtime


class FakeNativeRuntime:
    def __init__(self):
        self.initialized = True
        self.thread = "thread"
        self.calls = []

    def run_engine_and_decode(self, *args):
        self.calls.append(("run_engine_and_decode", args))
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
def test_facade_preserves_fixed_legacy_public_exports():
    legacy_exports = [
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

    for name in legacy_exports:
        assert name in dataweave.__all__
        getattr(dataweave, name)


@pytest.mark.unit
def test_facade_exports_module_resolver_factories():
    resolver_exports = [
        "ModuleResolver",
        "RESOLVE_MODULE_CALLBACK",
        "compose_resolvers",
        "modules_from_directory",
        "modules_from_jars",
        "modules_from_map",
    ]

    for name in resolver_exports:
        assert name in dataweave.__all__
        getattr(dataweave, name)


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
            (
                b"payload",
                b'{"value": {"content": "MQ==", "mimeType": "application/json", "charset": "utf-8"}}',
            ),
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
        ("run_engine_and_decode", (b"payload", b"{}"))
    ]


@pytest.mark.unit
def test_module_level_run_does_not_accept_module_resolver():
    assert "resolve_module" not in inspect.signature(dataweave.run).parameters


@pytest.mark.unit
def test_global_facade_initializes_once_and_cleanup_allows_recreation(monkeypatch):
    created = []
    registered = []

    class FakeRuntime:
        def __init__(self):
            self.cleaned = False
            created.append(self)

        def initialize(self):
            pass

        def cleanup(self):
            self.cleaned = True

    monkeypatch.setattr(dataweave, "DataWeave", FakeRuntime)
    monkeypatch.setattr("atexit.register", registered.append)

    first = dataweave._get_global_instance()
    second = dataweave._get_global_instance()
    dataweave.cleanup()
    third = dataweave._get_global_instance()

    assert first is second
    assert first.cleaned is True
    assert third is not first
    assert registered == [dataweave.cleanup, dataweave.cleanup]


@pytest.mark.unit
def test_cleanup_is_noop_without_global_runtime():
    dataweave.cleanup()

    assert dataweave._global_instance is None


@pytest.mark.unit
def test_global_cleanup_clears_global_before_reraising_on_failure(monkeypatch):
    created = []

    class FakeRuntime:
        def __init__(self):
            created.append(self)

        def initialize(self):
            pass

        def cleanup(self):
            raise dataweave.DataWeaveError("teardown failed")

    monkeypatch.setattr(dataweave, "DataWeave", FakeRuntime)
    monkeypatch.setattr("atexit.register", lambda _fn: None)
    first = dataweave._get_global_instance()

    # cleanup() nulls the global under _global_lock *before* running the
    # (potentially slow) instance.cleanup() outside the lock, so a failing
    # teardown does not strand the lock held nor leave a half-torn-down
    # instance published. The instance identity is not retained for retry --
    # that's fine because isolate-level teardown retry lives one layer down
    # in dataweave.native (_teardown_needed), independent of which Python
    # DataWeave wrapper object is holding the reference.
    with pytest.raises(dataweave.DataWeaveError, match="teardown failed"):
        dataweave.cleanup()

    assert dataweave._global_instance is None

    second = dataweave._get_global_instance()
    assert second is not first
    assert len(created) == 2
    dataweave._global_instance = None


@pytest.mark.unit
def test_get_global_instance_publishes_exactly_one_instance_under_concurrent_first_use(monkeypatch):
    thread_count = 8
    barrier = threading.Barrier(thread_count)
    counts_lock = threading.Lock()
    counts = {"created": 0, "initialized": 0}

    class SlowRuntime:
        def __init__(self):
            with counts_lock:
                counts["created"] += 1
            # Widen the window between the "is it published yet" check and
            # publication so concurrent first-callers are very likely to
            # overlap while racing to construct+initialize a candidate.
            time.sleep(0.05)

        def initialize(self):
            with counts_lock:
                counts["initialized"] += 1

        def cleanup(self):
            pass

    monkeypatch.setattr(dataweave, "DataWeave", SlowRuntime)
    monkeypatch.setattr("atexit.register", lambda _fn: None)

    results = [None] * thread_count
    errors = []

    def worker(index):
        barrier.wait()
        try:
            results[index] = dataweave._get_global_instance()
        except Exception as exc:  # pragma: no cover - defensive, surfaced via `errors`
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    try:
        assert not errors
        # Exactly one instance is ever constructed and initialized: creation,
        # initialization, and publication all happen under _global_lock, so a
        # losing thread never builds (and leaks) a candidate engine.
        assert counts["created"] == 1
        assert counts["initialized"] == 1
        assert len({id(result) for result in results}) == 1
        assert results[0] is dataweave._global_instance
    finally:
        dataweave._global_instance = None
