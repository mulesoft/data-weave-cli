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

    assert "if: always() && inputs.run-tck == 'true'" in action
    assert "native-lib:pythonTck" in action
    assert "run-tck: ${{ github.ref == 'refs/heads/master' }}" in workflow


@pytest.mark.unit
def test_python_artifact_owns_test_dependencies_and_tck_junit_upload():
    root = Path(__file__).resolve().parents[4]
    foundation = (root / ".github/actions/build-foundation/action.yml").read_text()
    action = (root / ".github/actions/python/action.yml").read_text()

    assert "Install Python test dependencies" not in foundation
    assert "native-lib/python[test]" in action
    assert "platform:" in action
    assert "python-tck-junit-${{ inputs.platform }}" in action
    assert action.index("Create Native Lib Python Wheel") < action.index("Run Python TCK Conformance")
    assert action.index("Upload Python wheel (artifact)") < action.index("Run Python TCK Conformance")
    assert action.index("Upload Python wheel to release") < action.index("Run Python TCK Conformance")
    assert action.index("Run Python TCK Conformance") < action.index("Upload Python TCK JUnit")
    assert "if: always() && inputs.run-tck == 'true'" in action
    assert "native-lib/python/build/test-results/pythonTck.xml" in action


@pytest.mark.unit
def test_master_tck_stages_the_shared_corpus_once_before_python_and_node():
    root = Path(__file__).resolve().parents[4]
    gradle = (root / "native-lib/build.gradle").read_text()
    workflow = (root / ".github/workflows/main.yml").read_text()
    node_action = (root / ".github/actions/node/action.yml").read_text()

    python_tck = gradle[gradle.index("tasks.register('pythonTck'"):gradle.index("tasks.register('buildNodePackage'")]
    assert "dependsOn tasks.named('stageTckSuites')" not in python_tck
    assert "native-lib:stageTckSuites" not in node_action
    assert workflow.index("Stage TCK corpus") < workflow.index("- name: Python")
    assert workflow.index("Stage TCK corpus") < workflow.index("- name: Node")


@pytest.mark.unit
def test_foundation_skips_python_tests_and_master_aggregates_binding_failures():
    root = Path(__file__).resolve().parents[4]
    foundation = (root / ".github/actions/build-foundation/action.yml").read_text()
    workflow = (root / ".github/workflows/main.yml").read_text()
    python_action = (root / ".github/actions/python/action.yml").read_text()
    node_action = (root / ".github/actions/node/action.yml").read_text()

    assert "-PskipPythonTests=true" in foundation
    assert "id: python" in workflow
    assert "id: node" in workflow
    assert workflow.count("continue-on-error: true") >= 2
    assert "Fail if binding artifacts failed" in workflow
    assert "steps.python.outcome == 'failure'" in workflow
    assert "steps.node.outcome == 'failure'" in workflow
    assert "platform: ${{ matrix.script_name }}" in workflow
    assert "if: always() && inputs.run-tck == 'true'" in python_action
    assert "if: always() && inputs.run-tck == 'true'" in node_action
    assert workflow.index("- name: Native library") < workflow.index("- name: Fail if binding artifacts failed")
