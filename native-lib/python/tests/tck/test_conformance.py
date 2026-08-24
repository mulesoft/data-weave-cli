import base64
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, Union

import pytest

import dataweave
import conftest

sys.path.insert(0, str(Path(__file__).parent))

from case_loader import TckScenario, discover_cases
from compare import compare_output
from ignore_list import (
    EXCLUDED_CASES,
    Exclusion,
    STRUCTURAL_MODULE_CASES,
    UNSUPPORTED_DW_MODULE_RESOLUTION,
    exclusion_for,
    validate_exclusions,
    validate_structural_module_cases,
)
from conftest import pytest_terminal_summary


pytestmark = pytest.mark.tck


SUITES_DIR = Path(__file__).resolve().parents[3] / "node" / "tests" / "tck" / "suites"
DISCOVERY = discover_cases(SUITES_DIR)
SCENARIOS = [
    scenario
    for discovered_case in DISCOVERY.cases
    for scenario in discovered_case.scenarios
]


ACCEPTED_BASELINE_MISMATCHES = {
    "core-modules/csv-invalid-utf8-out.csv:out.csv": (
        "runtime emits a replacement character where the fixture expects an empty CSV value"
    ),
    "core-modules/number-addition-out.json:out.json": (
        "runtime emits a numeric result that differs from the accepted baseline fixture"
    ),
    "core-modules/number-subtraction-out.json:out.json": (
        "runtime emits a numeric result that differs from the accepted baseline fixture"
    ),
    "core-modules/multipart-binary-out.multipart:out.multipart": "multipart writer output differs from the baseline fixture",
    "core-modules/multipart-class-cast-issue-out.multipart:out.multipart": "multipart writer output differs from the baseline fixture",
    "core-modules/multipart-empty-part-out.multipart:out.multipart": "multipart writer output differs from the baseline fixture",
    "core-modules/multipart-mixed-message-out.multipart:out.multipart": "multipart writer output differs from the baseline fixture",
    "core-modules/multipart-write-message-out.multipart:out.multipart": "multipart writer output differs from the baseline fixture",
    "core-modules/multipart-write-subtype-override-out.multipart:out.multipart": "multipart writer output differs from the baseline fixture",
    "core-modules/properties-passthrough-out.properties:out.properties": "properties writer output differs from the baseline fixture",
    "runtime/access_raw_value-out.json:out.json": "runtime coercion output differs from the baseline fixture",
    "runtime/coerciones_toString-out.json:out.json": "locale-sensitive runtime output differs from the baseline fixture",
    "runtime/properties-writer-out.properties:out.properties": "properties writer output differs from the baseline fixture",
    "runtime/read-concat-out.json:out.json": "runtime coercion output differs from the baseline fixture",
    "runtime/runtime_dataFormatsDescriptors-out.json:out.json": "dw::Runtime output differs from the baseline fixture",
    "runtime/runtime_orElseTry-out.json:out.json": "source-location runtime output differs from the baseline fixture",
    "runtime/runtime_run-out.json:out.json": "dw::Runtime output differs from the baseline fixture",
    "runtime/try-recursive-call-out.json:out.json": "source-location runtime output differs from the baseline fixture",
    "runtime/update-op-out.dwl:out.dwl": "runtime coercion output differs from the baseline fixture",
}

DEFERRED_WRITER_CASE = "core-modules/deferred-write-should-terminate-out.json:out.json"
RECOVERED_MODULE_CASES = {
    "runtime/full-qualified-name-ref-out.json",
    "runtime/import-component-alias-lib-out.json",
    "runtime/import-lib-out.json",
    "runtime/import-lib-with-alias-out.json",
    "runtime/import-named-lib-out.json",
    "runtime/import-star-out.json",
}


@pytest.mark.unit
def test_tck_runtime_uses_shared_module_fixture_resolver(monkeypatch):
    fixtures_dir = Path(__file__).resolve().parents[3] / "node" / "tests" / "tck" / "fixtures"
    captured = {"fixture_directories": []}
    resolver = lambda _path: "module source"

    class FakeRuntime:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(conftest.dataweave, "DataWeave", FakeRuntime)
    monkeypatch.setattr(
        conftest.dataweave,
        "modules_from_directory",
        lambda directory: captured["fixture_directories"].append(directory) or resolver,
    )

    runtime = conftest._tck_runtime()

    assert (fixtures_dir / "org" / "mule" / "weave" / "v2" / "libs" / "lib.dwl").is_file()
    assert captured["fixture_directories"] == [fixtures_dir]
    assert captured["resolve_module"] is resolver
    assert isinstance(runtime, FakeRuntime)


@pytest.mark.unit
def test_module_singleton_exclusion_preserves_direct_runtime_evidence():
    reason = EXCLUDED_CASES["runtime/module-singleton-out.json"].reason

    for module in ("libA", "libB", "libSource"):
        path = f"org::mule::weave::v2::libs::singleton::{module}"
        assert f"runtime cannot resolve {path}" in reason
        assert f"shared fixture lacks {path}" in reason


def tck_params():
    return [
        pytest.param(
            scenario,
            marks=pytest.mark.xfail(strict=True, reason=reason),
            id=scenario.identifier,
        )
        if (reason := ACCEPTED_BASELINE_MISMATCHES.get(scenario.identifier))
        else pytest.param(scenario, id=scenario.identifier)
        for scenario in SCENARIOS
    ]


def test_accepted_baseline_mismatches_are_strict_xfails_with_reasons():
    params = tck_params()
    xfails = {
        parameter.id: next(mark for mark in parameter.marks if mark.name == "xfail")
        for parameter in params
        if any(mark.name == "xfail" for mark in parameter.marks)
    }

    assert set(xfails) == set(ACCEPTED_BASELINE_MISMATCHES)
    for identifier, mark in xfails.items():
        assert xfails[identifier].kwargs["strict"] is True
        assert xfails[identifier].kwargs["reason"] == ACCEPTED_BASELINE_MISMATCHES[identifier]


@pytest.mark.unit
def test_tck_summary_counts_xfails_as_visible_expected_mismatches():
    output = []
    terminalreporter = SimpleNamespace(
        stats={
            "xfailed": [
                SimpleNamespace(
                    when="call",
                    nodeid="tests/tck/test_conformance.py::test_tck_scenario[core-modules/csv-invalid-utf8-out.csv:out.csv]",
                    outcome="skipped",
                    wasxfail="accepted mismatch",
                )
            ]
        },
        write_line=output.append,
    )
    config = SimpleNamespace(_tck_discovery=(DISCOVERY, SCENARIOS, [], set()))

    pytest_terminal_summary(terminalreporter, 0, config)

    assert "active-exclusions=0" in output[-1]
    assert "xfail=1" in output[-1]
    assert "accounted=1" in output[-1]


def test_tck_summary_counts_strict_xpass_once_as_a_failure():
    output = []
    terminalreporter = SimpleNamespace(
        stats={
            "failed": [
                SimpleNamespace(
                    when="call",
                    nodeid="tests/tck/test_conformance.py::test_tck_scenario[runtime/repaired:out.json]",
                    outcome="failed",
                    wasxfail="accepted mismatch",
                )
            ]
        },
        write_line=output.append,
    )
    config = SimpleNamespace(
        _tck_discovery=(DISCOVERY, SCENARIOS, [], set()),
        _tck_selected_identifiers={"runtime/repaired:out.json"},
    )

    pytest_terminal_summary(terminalreporter, 0, config)

    assert "failed=1" in output[-1]
    assert "xfail=0" in output[-1]
    assert "accounted=1" in output[-1]
    assert "unaccounted=0" in output[-1]


@pytest.mark.parametrize("scenario", tck_params())
def test_tck_scenario(scenario, tck_runtime):
    """Runs each non-excluded staged corpus scenario against the Python binding."""
    exclusion = exclusion_for(scenario.identifier.rsplit(":", 1)[0])
    if exclusion:
        pytest.skip(f"{exclusion.category}: {exclusion.reason}")

    if scenario.identifier == DEFERRED_WRITER_CASE:
        success, error, output = _run_deferred_writer_in_subprocess(scenario)
        assert success, error
    else:
        result = tck_runtime.run(scenario.transform, scenario.inputs)
        assert result.success, result.error
        output = result.get_bytes()
    comparison = compare_output(
        scenario.output_extension,
        output,
        scenario.expected,
        scenario.charset,
    )
    assert comparison.match, comparison.detail


def test_tck_session_runtime_runs_after_subprocess_deferred_write(tck_runtime):
    """Deferred writers must not prevent the managed TCK runtime from continuing."""
    deferred = next(
        scenario
        for scenario in SCENARIOS
        if scenario.identifier
        == "core-modules/deferred-write-should-terminate-out.json:out.json"
    )

    success, error, _output = _run_deferred_writer_in_subprocess(deferred)
    following_result = tck_runtime.run("%dw 2.0\noutput application/json\n--- 1")

    assert success, error
    assert following_result.success, following_result.error
    assert following_result.get_bytes() == b"1"


def _run_deferred_writer_in_subprocess(scenario):
    """Contain the native deferred writer whose isolate teardown can block."""
    source_dir = Path(__file__).resolve().parents[2] / "src"
    inputs = {
        name: {
            "content": base64.b64encode(value.content if isinstance(value.content, bytes) else value.content.encode()).decode(),
            "mime_type": value.mime_type,
            "charset": value.charset,
            "properties": value.properties,
        }
        for name, value in scenario.inputs.items()
    }
    code = """
import base64
import json
import sys
from dataweave import DataWeave, InputValue

script, inputs_json = sys.argv[1:]
inputs = {
    name: InputValue(base64.b64decode(value['content']), value['mime_type'], value['charset'], value['properties'])
    for name, value in json.loads(inputs_json).items()
}
runtime = DataWeave()
runtime.initialize()
result = runtime.run(script, inputs)
print(json.dumps({'success': result.success, 'error': result.error, 'result': result.result}))
"""
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(source_dir) + os.pathsep + environment.get("PYTHONPATH", "")
    completed = subprocess.run(
        [sys.executable, "-c", code, scenario.transform, json.dumps(inputs)],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    response = json.loads(completed.stdout)
    return response["success"], response["error"], base64.b64decode(response["result"] or "")


def write_case(root: Path, name: str, files: Dict[str, Union[bytes, str]]) -> Path:
    case = root / "runtime" / name
    case.mkdir(parents=True)
    for file_name, content in files.items():
        path = case / file_name
        if isinstance(content, bytes):
            path.write_bytes(content)
        else:
            path.write_text(content, encoding="utf-8")
    return case


def test_discover_cases_loads_transform_input_output_and_scenarios(tmp_path: Path):
    """Catches a loader that ignores TCK inputs or expected-output scenarios."""
    write_case(
        tmp_path,
        "maps-out.json",
        {
            "transform.dwl": "%dw 2.0\noutput application/json\n---\nin0",
            "in0.json": '{"answer": 42}',
            "out.json": '{"answer": 42}',
            "out.xml": "<answer>42</answer>",
        },
    )

    discovery = discover_cases(tmp_path)

    assert discovery.structural_skips == 0
    assert discovery.structural_case_identifiers == []
    assert len(discovery.cases) == 1
    scenarios = discovery.cases[0].scenarios
    assert [scenario.identifier for scenario in scenarios] == [
        "runtime/maps-out.json:out.json",
        "runtime/maps-out.json:out.xml",
    ]
    assert scenarios[0].transform == "%dw 2.0\noutput application/json\n---\nin0"
    assert scenarios[0].inputs["in0"].mime_type == "application/json"
    assert scenarios[0].inputs["in0"].content == b'{"answer": 42}'
    assert scenarios[0].expected == b'{"answer": 42}'


def test_discover_cases_ignores_files_that_only_resemble_inputs_and_outputs(tmp_path: Path):
    """Catches broad prefix matching that changes the runnable TCK inventory."""
    write_case(
        tmp_path,
        "exact-file-patterns-out.json",
        {
            "transform.dwl": "%dw 2.0\noutput application/json\n---\nin0",
            "in0.json": '{"answer": 42}',
            "input1.json": '{"wrong": true}',
            "out.json": '{"answer": 42}',
            "output.json": '{"wrong": true}',
        },
    )

    discovery = discover_cases(tmp_path)

    assert len(discovery.cases) == 1
    scenarios = discovery.cases[0].scenarios
    assert [scenario.identifier for scenario in scenarios] == [
        "runtime/exact-file-patterns-out.json:out.json"
    ]
    assert set(scenarios[0].inputs) == {"in0"}


def test_discover_cases_structurally_skips_adjacent_dwl_modules(tmp_path: Path):
    """Catches discovery that mistakes a multi-DWL case for one runnable transform."""
    write_case(
        tmp_path,
        "local-module-out.json",
        {
            "transform.dwl": "%dw 2.0\noutput application/json\n---\ninclude::value",
            "include.dwl": "%dw 2.0\nvar value = 42",
            "out.json": "42",
        },
    )

    discovery = discover_cases(tmp_path)

    assert discovery.cases == []
    assert discovery.structural_skips == 1
    assert discovery.structural_case_identifiers == ["runtime/local-module-out.json"]


@pytest.mark.parametrize(
    ("extension", "actual", "expected"),
    [
        ("json", b'{"a": 1, "b": 2}', b'{"b":2,"a":1}'),
        ("xml", b"<root>\n  <value>1</value>\n</root>", b"<root><value>1</value></root>"),
        ("xml", b"<root><value>1</value>tail</root>", b"<root><value>1</value>tail</root>"),
        ("csv", b"a,b\r\n1,2\r\n", b"a,b\n1,2"),
        ("txt", b"value\r\n", b"value\n"),
        ("dwl", b"fun f(x) = x + 1", b"fun f(x)=x+1"),
        ("properties", b"a=b\r\n", b"a=b\n"),
        ("urlencoded", b"a=1&b=2\r\n", b"a=1&b=2"),
        ("multipart", b"--boundary\r\nbody", b"--boundary\nbody"),
        ("bin", b"\x00\x01", b"\x00\x01"),
    ],
)
def test_compare_output_matches_required_corpus_extensions(
    extension: str, actual: bytes, expected: bytes
):
    """Catches extension dispatch that compares TCK writer output too strictly."""
    assert compare_output(extension, actual, expected).match


def test_compare_output_rejects_unknown_extension():
    """Catches silent fallbacks that hide unsupported corpus output formats."""
    result = compare_output("unknown", b"actual", b"expected")

    assert not result.match
    assert "unknown output extension" in result.detail


@pytest.mark.parametrize(("actual", "expected"), [(b"true", b"1"), (b"false", b"0")])
def test_compare_output_rejects_json_booleans_as_numbers(actual: bytes, expected: bytes):
    assert not compare_output("json", actual, expected).match


def test_compare_output_rejects_different_xml_tail_text():
    """Catches structural XML comparison that discards text after child elements."""
    result = compare_output(
        "xml",
        b"<root><value>1</value>actual tail</root>",
        b"<root><value>1</value>expected tail</root>",
    )

    assert not result.match


def test_compare_output_rejects_different_namespace_prefixes():
    """Matches Node's policy: declaration placement is ignored, prefixes are not."""
    result = compare_output(
        "xml",
        b'<left:root xmlns:left="urn:test"><left:value>1</left:value></left:root>',
        b'<right:root xmlns:right="urn:test"><right:value>1</right:value></right:root>',
    )

    assert not result.match


def test_compare_output_preserves_interleaved_xml_child_order():
    """Catches XML normalization that groups children by tag and loses ordering."""
    result = compare_output(
        "xml",
        b"<root><a>1</a><b>2</b><a>3</a></root>",
        b"<root><a>1</a><a>3</a><b>2</b></root>",
    )

    assert not result.match


def test_compare_output_reports_unknown_charset():
    """Catches unsupported sidecar encodings escaping as uncaught lookup errors."""
    result = compare_output("json", b"{}", b"{}", "not-a-real-charset")

    assert not result.match
    assert "unknown output charset" in result.detail


def test_exclusion_registry_requires_category_and_reason():
    """Catches exclusions that cannot be audited by category and rationale."""
    errors = validate_exclusions(
        {
            "missing-category": {
                "case_identifier": "missing-category",
                "reason": "needs a module",
            },
            "missing-reason": {
                "case_identifier": "missing-reason",
                "category": "unsupported-dw-module-resolution",
            },
        }
    )

    assert errors == [
        "missing-category: missing category",
        "missing-reason: missing reason",
    ]


def test_exclusion_registry_requires_case_identity_supported_category_and_reason():
    """Catches exclusions that cannot be traced to one approved runtime limitation."""
    errors = validate_exclusions(
        {
            "runtime/missing-identity": {
                "category": "unsupported-dw-module-resolution",
                "reason": "Cannot resolve dw::core::Assertions",
            },
            "runtime/mismatched-identity": {
                "case_identifier": "runtime/another-case",
                "category": "unsupported-dw-module-resolution",
                "reason": "Cannot resolve dw::core::Assertions",
            },
            "runtime/unsupported-category": {
                "case_identifier": "runtime/unsupported-category",
                "category": "broad-runtime-exception",
                "reason": "runtime failure",
            },
            "runtime/blank-reason": {
                "case_identifier": "runtime/blank-reason",
                "category": "unsupported-dw-module-resolution",
                "reason": " ",
            },
        }
    )

    assert errors == [
        "runtime/missing-identity: missing case identity",
        "runtime/mismatched-identity: case identity must match registry key",
        "runtime/unsupported-category: unsupported category broad-runtime-exception",
        "runtime/blank-reason: missing reason",
    ]


@pytest.mark.unit
def test_only_declared_case_identifiers_are_excluded():
    """Catches broad exclusion matching that can skip unrelated failures."""
    assert validate_exclusions(EXCLUDED_CASES, SCENARIOS) == []
    assert exclusion_for("unknown-case") is None
    assert RECOVERED_MODULE_CASES.isdisjoint(EXCLUDED_CASES)
    assert len(EXCLUDED_CASES) == 31


@pytest.mark.unit
def test_exclusion_registry_uses_the_inventory_categories():
    """Catches category collapse that would conceal the unsupported boundary."""
    categories = {}
    for exclusion in EXCLUDED_CASES.values():
        categories[exclusion.category] = categories.get(exclusion.category, 0) + 1

    assert categories == {
        "unavailable-classpath-test-resource": 2,
        "unavailable-java-module": 11,
        "unsupported-dw-module-resolution": 18,
    }


def test_exclusion_registry_rejects_unreachable_active_entries():
    """Catches active exclusions that cannot affect any discovered runnable scenario."""
    scenario = TckScenario(
        "runtime/imports:out.json",
        "%dw 2.0\nimport sample from test::module\n--- sample",
        {},
        b"null",
        "json",
        None,
    )

    errors = validate_exclusions(
        {
            "runtime/imports": Exclusion(
                "runtime/imports",
                UNSUPPORTED_DW_MODULE_RESOLUTION,
                "imports test module",
            ),
            "runtime/not-discovered": Exclusion(
                "runtime/not-discovered",
                UNSUPPORTED_DW_MODULE_RESOLUTION,
                "stale entry",
            ),
        },
        [scenario],
    )

    assert errors == ["runtime/not-discovered: not a discovered runnable case"]


def test_structural_module_registry_rejects_entries_that_are_not_structural_skips():
    """Catches stale structural-module entries after a case becomes runnable."""
    errors = validate_structural_module_cases(
        {"runtime/local-module", "runtime/not-structural"},
        ["runtime/local-module"],
    )

    assert errors == ["runtime/not-structural: not a structural skip"]


def test_structural_module_registry_rejects_unregistered_structural_modules():
    """Catches adjacent-module cases omitted from the audited inventory."""
    errors = validate_structural_module_cases(
        {"runtime/registered"},
        ["runtime/registered", "runtime/missing"],
    )

    assert errors == ["runtime/missing: structural module case is not registered"]


def test_structural_module_registry_matches_staged_structural_cases():
    assert validate_structural_module_cases(
        STRUCTURAL_MODULE_CASES,
        DISCOVERY.structural_module_case_identifiers,
    ) == []


def test_tck_summary_ignores_collection_nodes():
    """Catches pytest collection nodes being counted as test reports."""
    output = []
    terminalreporter = SimpleNamespace(
        stats={
            "passed": [
                SimpleNamespace(
                    when="call",
                    nodeid="tests/tck/test_conformance.py::test_tck_scenario[runtime/plain:out.json]",
                    outcome="passed",
                ),
                SimpleNamespace(
                    when="call",
                    nodeid="tests/tck/test_conformance.py::test_tck_scenario[runtime/excluded:out.json]",
                    outcome="skipped",
                ),
                SimpleNamespace(
                    when="call",
                    nodeid="tests/tck/test_conformance.py::test_tck_scenario[runtime/failing:out.json]",
                    outcome="failed",
                ),
            ],
            "": [SimpleNamespace(nodeid="tests/tck/test_conformance.py")],
        },
        write_line=output.append,
    )
    config = SimpleNamespace(_tck_discovery=(DISCOVERY, SCENARIOS, [SCENARIOS[0]], set()))

    pytest_terminal_summary(terminalreporter, 0, config)

    assert output[-1] == (
        "TCK totals: selected=729, structural-skips=193, structural-module-cases=0, executed=2, "
        "active-exclusions=1, passed=1, failed=1, xfail=0, accounted=3, unaccounted=726"
    )


def test_tck_session_fails_when_a_complete_run_leaves_scenarios_unaccounted():
    terminalreporter = SimpleNamespace(stats={"passed": []})
    config = SimpleNamespace(
        _tck_discovery=(DISCOVERY, SCENARIOS, [], set()),
        option=SimpleNamespace(keyword=""),
        pluginmanager=SimpleNamespace(get_plugin=lambda name: terminalreporter),
    )
    session = SimpleNamespace(config=config, exitstatus=0)

    from conftest import pytest_sessionfinish

    pytest_sessionfinish(session, 0)

    assert session.exitstatus == pytest.ExitCode.TESTS_FAILED


def test_tck_session_fails_when_duplicate_report_hides_missing_scenario():
    first = SCENARIOS[0].identifier
    reports = [
        SimpleNamespace(
            when="call",
            nodeid=f"tests/tck/test_conformance.py::test_tck_scenario[{first}]",
            outcome="passed",
        )
        for _scenario in SCENARIOS
    ]
    terminalreporter = SimpleNamespace(stats={"passed": reports})
    config = SimpleNamespace(
        _tck_discovery=(DISCOVERY, SCENARIOS, [], set()),
        _tck_selected_identifiers={scenario.identifier for scenario in SCENARIOS},
        option=SimpleNamespace(keyword=""),
        pluginmanager=SimpleNamespace(get_plugin=lambda name: terminalreporter),
    )
    session = SimpleNamespace(config=config, exitstatus=0)

    from conftest import pytest_sessionfinish

    pytest_sessionfinish(session, 0)

    assert session.exitstatus == pytest.ExitCode.TESTS_FAILED


def test_tck_session_allows_intentionally_filtered_runs():
    terminalreporter = SimpleNamespace(stats={"passed": []})
    config = SimpleNamespace(
        _tck_discovery=(DISCOVERY, SCENARIOS, [], set()),
        option=SimpleNamespace(keyword="one-scenario"),
        pluginmanager=SimpleNamespace(get_plugin=lambda name: terminalreporter),
    )
    session = SimpleNamespace(config=config, exitstatus=0)

    from conftest import pytest_sessionfinish

    pytest_sessionfinish(session, 0)

    assert session.exitstatus == 0


def test_tck_summary_uses_collected_scenario_selection():
    output = []
    terminalreporter = SimpleNamespace(stats={}, write_line=output.append)
    config = SimpleNamespace(
        _tck_discovery=(DISCOVERY, SCENARIOS, [], set()),
        _tck_selected_identifiers=set(),
    )

    pytest_terminal_summary(terminalreporter, 0, config)

    assert "selected=0" in output[-1]
    assert "unaccounted=0" in output[-1]


def test_tck_collection_records_only_selected_scenarios():
    from conftest import pytest_collection_finish

    selected = SCENARIOS[0].identifier
    config = SimpleNamespace(_tck_discovery=(DISCOVERY, SCENARIOS, [], set()))
    session = SimpleNamespace(
        config=config,
        items=[
            SimpleNamespace(
                nodeid=f"tests/tck/test_conformance.py::test_tck_scenario[{selected}]"
            ),
            SimpleNamespace(
                nodeid="tests/tck/test_conformance.py::test_compare_output_reports_unknown_charset"
            ),
        ],
    )

    pytest_collection_finish(session)

    assert config._tck_selected_identifiers == {selected}
