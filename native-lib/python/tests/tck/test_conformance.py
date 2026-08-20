import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, Union

import pytest

import dataweave

sys.path.insert(0, str(Path(__file__).parent))

from case_loader import TckScenario, discover_cases
from compare import compare_output
from ignore_list import (
    EXCLUDED_CASES,
    Exclusion,
    MODULE_RESOLUTION_NOT_SUPPORTED,
    exclusion_for,
    validate_exclusions,
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


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda scenario: scenario.identifier)
def test_tck_scenario(scenario, tck_runtime):
    """Runs each non-excluded staged corpus scenario against the Python binding."""
    exclusion = exclusion_for(scenario.identifier.rsplit(":", 1)[0])
    if exclusion:
        pytest.skip(f"{exclusion.category}: {exclusion.reason}")

    result = tck_runtime.run(scenario.transform, scenario.inputs)
    assert result.success, result.error
    comparison = compare_output(
        scenario.output_extension,
        result.get_bytes(),
        scenario.expected,
        scenario.charset,
    )
    assert comparison.match, comparison.detail


def test_tck_session_runtime_runs_after_deferred_write_without_teardown(tck_runtime):
    """Catches per-scenario isolate cleanup after a deferred writer stalls the lane."""
    deferred = next(
        scenario
        for scenario in SCENARIOS
        if scenario.identifier
        == "core-modules/deferred-write-should-terminate-out.json:out.json"
    )

    deferred_result = tck_runtime.run(deferred.transform, deferred.inputs)
    following_result = tck_runtime.run("%dw 2.0\noutput application/json\n--- 1")

    assert deferred_result.success, deferred_result.error
    assert following_result.success, following_result.error
    assert following_result.get_bytes() == b"1"


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


def test_compare_output_rejects_different_xml_tail_text():
    """Catches structural XML comparison that discards text after child elements."""
    result = compare_output(
        "xml",
        b"<root><value>1</value>actual tail</root>",
        b"<root><value>1</value>expected tail</root>",
    )

    assert not result.match


def test_exclusion_registry_requires_category_and_reason():
    """Catches exclusions that cannot be audited by category and rationale."""
    errors = validate_exclusions(
        {
            "missing-category": {"reason": "needs a module"},
            "missing-reason": {"category": "module-resolution-not-supported"},
        }
    )

    assert errors == [
        "missing-category: missing category",
        "missing-reason: missing reason",
    ]


def test_only_declared_case_identifiers_are_excluded():
    """Catches broad exclusion matching that can skip unrelated failures."""
    assert validate_exclusions(EXCLUDED_CASES, SCENARIOS) == []
    assert exclusion_for("unknown-case") is None
    exclusion = exclusion_for("runtime/import-lib-out.json")
    assert exclusion.category == "module-resolution-not-supported"
    assert len(EXCLUDED_CASES) == 18


def test_module_resolution_exclusions_only_skip_importing_cases():
    """Catches a module-resolution exclusion that would hide an unrelated failure."""
    imported = TckScenario(
        "runtime/imports:out.json",
        "%dw 2.0\nimport sample from test::module\n--- sample",
        {},
        b"null",
        "json",
        None,
    )
    plain = TckScenario(
        "runtime/plain:out.json",
        "%dw 2.0\noutput application/json\n--- 1",
        {},
        b"1",
        "json",
        None,
    )
    errors = validate_exclusions(
        {
            "runtime/imports": Exclusion(
                MODULE_RESOLUTION_NOT_SUPPORTED, "imports test module"
            ),
            "runtime/plain": Exclusion(
                MODULE_RESOLUTION_NOT_SUPPORTED, "incorrectly broad"
            ),
        },
        [imported, plain],
    )

    assert errors == [
        "runtime/plain: module-resolution-not-supported requires a DW import"
    ]


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
                MODULE_RESOLUTION_NOT_SUPPORTED, "imports test module"
            ),
            "runtime/not-discovered": Exclusion(
                MODULE_RESOLUTION_NOT_SUPPORTED, "stale entry"
            ),
        },
        [scenario],
    )

    assert errors == ["runtime/not-discovered: not a discovered runnable case"]


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
        "TCK totals: selected=731, structural-skips=191, structural-module-cases=0, executed=2, "
        "active-exclusions=1, passed=1, failed=1"
    )
