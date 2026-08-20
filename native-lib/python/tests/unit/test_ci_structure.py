from pathlib import Path

import pytest


@pytest.mark.unit
def test_python_artifact_runs_python_test_before_building_wheel():
    action = (Path(__file__).resolve().parents[4] / ".github/actions/python/action.yml").read_text()

    assert "native-lib:pythonTest" in action
    assert action.index("native-lib:pythonTest") < action.index("native-lib:buildPythonWheel")


@pytest.mark.unit
def test_python_tck_is_gated_by_the_master_only_workflow_input():
    root = Path(__file__).resolve().parents[4]
    action = (root / ".github/actions/python/action.yml").read_text()
    workflow = (root / ".github/workflows/main.yml").read_text()

    assert "if: inputs.run-tck == 'true'" in action
    assert "native-lib:pythonTck" in action
    assert "run-tck: ${{ github.ref == 'refs/heads/master' }}" in workflow
