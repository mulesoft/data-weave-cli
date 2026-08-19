import pytest

import dataweave


@pytest.mark.integration
def test_context_manager_runs_multiple_scripts():
    with dataweave.DataWeave() as dw:
        assert dw.run("sqrt(144)").get_string() == "12"
        assert dw.run("sqrt(10000)").get_string() == "100"


@pytest.mark.unit
def test_context_exit_preserves_body_exception_when_cleanup_fails(monkeypatch):
    runtime = dataweave.DataWeave.__new__(dataweave.DataWeave)
    monkeypatch.setattr(runtime, "initialize", lambda: None)
    monkeypatch.setattr(runtime, "cleanup", lambda: (_ for _ in ()).throw(dataweave.DataWeaveError("cleanup failed")))

    with pytest.raises(ValueError, match="body failed"):
        with runtime:
            raise ValueError("body failed")


@pytest.mark.unit
def test_context_exit_surfaces_cleanup_failure_without_body_exception(monkeypatch):
    runtime = dataweave.DataWeave.__new__(dataweave.DataWeave)
    monkeypatch.setattr(runtime, "cleanup", lambda: (_ for _ in ()).throw(dataweave.DataWeaveError("cleanup failed")))

    with pytest.raises(dataweave.DataWeaveError, match="cleanup failed"):
        runtime.__exit__(None, None, None)
