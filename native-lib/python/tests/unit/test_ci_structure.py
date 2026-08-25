from pathlib import Path
import re

import pytest


def named_step_if(document: str, name: str) -> str:
    step = re.search(
        rf"^(?P<indent>[ \t]*)- name: {re.escape(name)}\n"
        rf"(?P<body>(?:^(?P=indent)[ \t]+.*\n?)*)",
        document,
        re.MULTILINE,
    )
    assert step, f"missing step {name!r}"

    guard = re.search(r"^[ \t]+if: (?P<guard>.+)$", step.group("body"), re.MULTILINE)
    assert guard, f"missing if guard for step {name!r}"
    return guard.group("guard")


def assert_resolver_restrictions(document: str) -> None:
    normalized = " ".join(document.split())
    assert (
        "The `ModuleResolver` contract is a synchronous callable from a module "
        "key to the module source string or `None`."
    ) in normalized
    assert (
        "Custom resolver configuration is available only on an explicit "
        "`DataWeave` instance; the module-level `dataweave.run()` singleton "
        "does not accept `resolve_module`."
    ) in normalized
    assert (
        "`run_streaming()`, `run_transform()`, and the low-level callback "
        "streaming API do not use custom resolvers and can import only built-in "
        "modules."
    ) in normalized


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

    assert named_step_if(action, "Run Python TCK Conformance") == "always() && inputs.run-tck == 'true'"
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
    assert named_step_if(action, "Run Python TCK Conformance") == "always() && inputs.run-tck == 'true'"
    assert "native-lib/build/test-results/pythonTck.xml" in action


@pytest.mark.unit
def test_python_readme_documents_module_resolver_contract():
    readme = (Path(__file__).resolve().parents[2] / "README.md").read_text()

    for public_name in (
        "ModuleResolver",
        "modules_from_map",
        "modules_from_directory",
        "modules_from_jars",
        "compose_resolvers",
        "resolve_module",
        "DATAWEAVE_RESOLVER_DEBUG",
    ):
        assert f"`{public_name}`" in readme

    assert_resolver_restrictions(readme)
    normalized = " ".join(readme.split())
    assert "without a leading path separator" in normalized
    assert "without a leading slash or separator" not in normalized
    assert "Each initialized explicit Python `DataWeave` instance owns a dedicated Graal isolate." in normalized
    assert "retains the resolver callback until successful isolate teardown" in normalized


@pytest.mark.unit
@pytest.mark.parametrize(
    "contract,negated",
    (
        (
            "The `ModuleResolver` contract is a synchronous callable",
            "The `ModuleResolver` contract is an asynchronous callable",
        ),
        (
            "singleton does not accept `resolve_module`",
            "singleton does accept `resolve_module`",
        ),
        (
            "streaming API do not use custom resolvers and can import only built-in modules",
            "streaming API do use custom resolvers and can import external modules",
        ),
    ),
)
def test_python_readme_resolver_restrictions_reject_negation(contract, negated):
    readme = (Path(__file__).resolve().parents[2] / "README.md").read_text()
    mutated = " ".join(readme.split()).replace(contract, negated)

    with pytest.raises(AssertionError):
        assert_resolver_restrictions(mutated)


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
def test_tck_metadata_validation_is_not_selected_by_the_pr_python_test_lane():
    root = Path(__file__).resolve().parents[4]
    conformance = (root / "native-lib/python/tests/tck/test_conformance.py").read_text()

    assert "@pytest.mark.unit\ndef test_accepted_baseline_mismatches" not in conformance


@pytest.mark.unit
def test_tck_corpus_inventory_policy_is_not_selected_by_the_pr_python_test_lane():
    root = Path(__file__).resolve().parents[4]
    conformance = (root / "native-lib/python/tests/tck/test_conformance.py").read_text()

    assert "@pytest.mark.unit\ndef test_only_declared_case_identifiers_are_excluded" not in conformance


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
    assert named_step_if(python_action, "Run Python TCK Conformance") == "always() && inputs.run-tck == 'true'"
    assert named_step_if(node_action, "Run Node.js TCK Conformance") == "always() && inputs.run-tck == 'true'"
    assert named_step_if(workflow, "Fail if binding artifacts failed") == (
        "always() && (steps.python.outcome == 'failure' || steps.node.outcome == 'failure')"
    )
    assert workflow.index("- name: Native library") < workflow.index("- name: Fail if binding artifacts failed")


@pytest.mark.unit
def test_named_step_if_does_not_read_a_later_step_guard():
    mutated_workflow = """\
      - name: Fail if binding artifacts failed
        run: exit 1
      - name: Later step
        if: always() && (steps.python.outcome == 'failure' || steps.node.outcome == 'failure')
"""

    with pytest.raises(AssertionError, match="missing if guard"):
        named_step_if(mutated_workflow, "Fail if binding artifacts failed")
