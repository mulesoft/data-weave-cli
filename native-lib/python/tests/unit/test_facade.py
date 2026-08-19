import pytest

import dataweave


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
