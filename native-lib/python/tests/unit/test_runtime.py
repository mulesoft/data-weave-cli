import pytest

from dataweave import native, runtime
from dataweave.runtime import DataWeave


class _FakeNative:
    def __init__(self, lib_path=None):
        self.installed_resolver = None
        self.initialized = False
        self.handle = 0
        self.thread = object()
        self.has_callback_streaming = True
        self.has_callback_input_output = True
        self.cleaned = 0
        self.runs = []
        self.install_resolver_calls = 0

    def install_resolver(self, resolver):
        assert not self.initialized, "Cannot install a resolver after initialize()"
        self.installed_resolver = resolver
        self.install_resolver_calls += 1

    def initialize(self):
        self.initialized = True
        self.handle = 7

    def run_engine_and_decode(self, script, inputs):
        self.runs.append((script, inputs))
        return '{"success":true,"result":"","binary":false,"mimeType":"application/json","charset":"UTF-8"}'

    def cleanup(self):
        self.cleaned += 1
        self.initialized = False


@pytest.mark.unit
def test_dataweave_installs_resolver_before_initialize(monkeypatch):
    monkeypatch.setattr(runtime, "NativeRuntime", _FakeNative)
    resolver = lambda path: None
    dw = DataWeave(resolve_module=resolver)
    dw.initialize()
    assert dw._native.installed_resolver is resolver
    assert dw._native.initialized is True
    dw.cleanup()
    assert dw._native.cleaned == 1


@pytest.mark.unit
def test_initialize_is_idempotent_with_resolver(monkeypatch):
    monkeypatch.setattr(runtime, "NativeRuntime", _FakeNative)
    resolver = lambda path: None
    dw = DataWeave(resolve_module=resolver)
    dw.initialize()
    # Second call must be a harmless no-op, not raise "Cannot install a resolver after initialize()".
    dw.initialize()
    assert dw._native.initialized is True
    assert dw._native.install_resolver_calls == 1
    dw.cleanup()


@pytest.mark.unit
def test_run_routes_through_engine(monkeypatch):
    monkeypatch.setattr(runtime, "NativeRuntime", _FakeNative)
    dw = DataWeave()
    dw.initialize()
    dw.run("1 + 1")
    assert dw._native.runs == [(b"1 + 1", b"{}")]
    dw.cleanup()
