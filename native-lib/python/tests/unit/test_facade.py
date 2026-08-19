import pytest

import dataweave


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
