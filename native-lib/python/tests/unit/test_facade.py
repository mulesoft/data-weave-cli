import inspect

import pytest

import dataweave
from dataweave import runtime


class FakeNativeRuntime:
    def __init__(self):
        self.initialized = True
        self.thread = "thread"
        self.calls = []

    def run_script_and_decode(self, *args):
        self.calls.append(("run_script_and_decode", args))
        return self._result()

    def run_script_with_resolver_and_decode(self, *args):
        self.calls.append(("run_script_with_resolver_and_decode", args))
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
def test_run_dispatches_to_resolver_aware_native_execution():
    resolver = lambda _path: "source"
    instance = configured_runtime(resolver)

    result = instance.run("payload", {"value": 1})

    assert result == dataweave.ExecutionResult(
        True, "SGVsbG8=", None, False, "text/plain", "utf-8"
    )
    assert instance._native.calls == [
        (
            "run_script_with_resolver_and_decode",
            (
                "thread",
                b"payload",
                b'{"value": {"content": "MQ==", "mimeType": "application/json", "charset": "utf-8"}}',
                resolver,
            ),
        )
    ]


@pytest.mark.unit
def test_run_without_resolver_preserves_native_execution_path():
    instance = configured_runtime()

    result = instance.run("payload")

    assert result == dataweave.ExecutionResult(
        True, "SGVsbG8=", None, False, "text/plain", "utf-8"
    )
    assert instance._native.calls == [
        ("run_script_and_decode", ("thread", b"payload", b"{}"))
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
def test_global_cleanup_retains_failed_runtime_for_retry(monkeypatch):
    created = []

    class FakeRuntime:
        def __init__(self):
            created.append(self)

        def initialize(self):
            pass

        def cleanup(self):
            raise dataweave.DataWeaveError("teardown failed")

    monkeypatch.setattr(dataweave, "DataWeave", FakeRuntime)
    first = dataweave._get_global_instance()

    with pytest.raises(dataweave.DataWeaveError, match="teardown failed"):
        dataweave.cleanup()

    second = dataweave._get_global_instance()
    assert second is first
    dataweave._global_instance = None
