import sys
from pathlib import Path

import pytest


PYTHON_SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(PYTHON_SRC_DIR))
sys.path.insert(0, str(Path(__file__).parent))

import dataweave


def _tck_discovery():
    from tck.case_loader import discover_cases
    from tck.ignore_list import (
        EXCLUDED_CASES,
        STRUCTURAL_MODULE_CASES,
        validate_exclusions,
        validate_structural_module_cases,
    )

    suites_dir = Path(__file__).resolve().parents[2] / "node" / "tests" / "tck" / "suites"
    discovery = discover_cases(suites_dir)
    scenarios = [
        scenario
        for discovered_case in discovery.cases
        for scenario in discovered_case.scenarios
    ]
    errors = validate_exclusions(EXCLUDED_CASES, scenarios)
    errors.extend(
        validate_structural_module_cases(
            STRUCTURAL_MODULE_CASES,
            discovery.structural_module_case_identifiers,
        )
    )
    if errors:
        raise pytest.UsageError("Invalid TCK policy: " + "; ".join(errors))
    exclusions = [
        scenario for scenario in scenarios
        if scenario.identifier.rsplit(":", 1)[0] in EXCLUDED_CASES
    ]
    structural_modules = set(discovery.structural_module_case_identifiers)
    return discovery, scenarios, exclusions, structural_modules


def pytest_configure(config):
    if config.option.markexpr == "tck":
        config._tck_discovery = _tck_discovery()


def pytest_collection_finish(session):
    config = session.config
    if not hasattr(config, "_tck_discovery"):
        return
    marker = "::test_tck_scenario["
    config._tck_selected_identifiers = {
        item.nodeid.split(marker, 1)[1].rsplit("]", 1)[0]
        for item in session.items
        if marker in item.nodeid
    }


def pytest_report_header(config):
    if not hasattr(config, "_tck_discovery"):
        return None
    discovery, scenarios, exclusions, structural_modules = config._tck_discovery
    selected_identifiers = getattr(
        config,
        "_tck_selected_identifiers",
        {scenario.identifier for scenario in scenarios},
    )
    categories = {}
    from tck.ignore_list import exclusion_for

    for scenario in exclusions:
        exclusion = exclusion_for(scenario.identifier.rsplit(":", 1)[0])
        categories[exclusion.category] = categories.get(exclusion.category, 0) + 1
    category_totals = ", ".join(
        f"{category}={count}" for category, count in sorted(categories.items())
    ) or "none"
    return (
        f"TCK: discovered={len(scenarios)}, structural-skips={discovery.structural_skips}, "
        f"structural-module-cases={len(structural_modules)}, "
        f"active-exclusions={len(exclusions)} ({category_totals})"
    )


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if not hasattr(config, "_tck_discovery"):
        return
    discovery, scenarios, exclusions, structural_modules = config._tck_discovery
    selected_identifiers = getattr(
        config,
        "_tck_selected_identifiers",
        {scenario.identifier for scenario in scenarios},
    )
    reports = _tck_reports(terminalreporter)
    totals = {
        outcome: sum(
            report.outcome == outcome
            and (outcome != "skipped" or not getattr(report, "wasxfail", None))
            for report in reports
        )
        for outcome in ("passed", "failed", "skipped")
    }
    xfailed = sum(
        report.outcome == "skipped" and bool(getattr(report, "wasxfail", None))
        for report in reports
    )
    executed = totals["passed"] + totals["failed"]
    accounted = executed + totals["skipped"] + xfailed
    unaccounted = len(selected_identifiers) - accounted
    terminalreporter.write_line(
        "TCK totals: "
        f"selected={len(selected_identifiers)}, structural-skips={discovery.structural_skips}, "
        f"structural-module-cases={len(structural_modules)}, "
        f"executed={executed}, active-exclusions={totals['skipped']}, passed={totals['passed']}, "
        f"failed={totals['failed']}, xfail={xfailed}, accounted={accounted}, "
        f"unaccounted={unaccounted}"
    )


def pytest_sessionfinish(session, exitstatus):
    config = session.config
    if (
        exitstatus != pytest.ExitCode.OK
        or not hasattr(config, "_tck_discovery")
        or config.option.keyword
    ):
        return
    _discovery, scenarios, _exclusions, _structural_modules = config._tck_discovery
    terminalreporter = config.pluginmanager.get_plugin("terminalreporter")
    if terminalreporter is None:
        return
    expected = getattr(
        config,
        "_tck_selected_identifiers",
        {scenario.identifier for scenario in scenarios},
    )
    reported = [_tck_report_identifier(report) for report in _tck_reports(terminalreporter)]
    if len(reported) != len(set(reported)) or set(reported) != expected:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


def _tck_reports(terminalreporter):
    return [
        report
        for reports in terminalreporter.stats.values()
        for report in reports
        if getattr(report, "when", None) == "call"
        and "::test_tck_scenario[" in report.nodeid
    ]


def _tck_report_identifier(report):
    return report.nodeid.split("::test_tck_scenario[", 1)[1].rsplit("]", 1)[0]


@pytest.fixture(autouse=True)
def clean_dataweave_runtime(request):
    """Keep module-level isolate state from leaking between integration tests."""
    if request.node.get_closest_marker("tck"):
        yield
        return
    dataweave.cleanup()
    yield
    dataweave.cleanup()


def _tck_runtime():
    fixtures_dir = Path(__file__).resolve().parents[2] / "node" / "tests" / "tck" / "fixtures"
    return dataweave.DataWeave(
        resolve_module=dataweave.modules_from_directory(fixtures_dir),
    )


@pytest.fixture(scope="session")
def tck_runtime():
    """Own one isolate for the TCK session and release it after the lane."""
    runtime = _tck_runtime()
    runtime.initialize()
    try:
        yield runtime
    finally:
        runtime.cleanup()


@pytest.fixture
def collect_stream():
    def collect(stream):
        chunks = list(stream)
        return b"".join(chunks), stream.metadata

    return collect
