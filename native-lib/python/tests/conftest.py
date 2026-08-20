import sys
from pathlib import Path

import pytest


PYTHON_SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(PYTHON_SRC_DIR))
sys.path.insert(0, str(Path(__file__).parent))

import dataweave


def _tck_discovery():
    from tck.case_loader import discover_cases
    from tck.ignore_list import EXCLUDED_CASES, STRUCTURAL_MODULE_CASES, validate_exclusions

    suites_dir = Path(__file__).resolve().parents[2] / "node" / "tests" / "tck" / "suites"
    discovery = discover_cases(suites_dir)
    scenarios = [
        scenario
        for discovered_case in discovery.cases
        for scenario in discovered_case.scenarios
    ]
    errors = validate_exclusions(EXCLUDED_CASES, scenarios)
    if errors:
        raise pytest.UsageError("Invalid active TCK exclusions: " + "; ".join(errors))
    exclusions = [
        scenario for scenario in scenarios
        if scenario.identifier.rsplit(":", 1)[0] in EXCLUDED_CASES
    ]
    structural_modules = set(discovery.structural_case_identifiers) & STRUCTURAL_MODULE_CASES
    return discovery, scenarios, exclusions, structural_modules


def pytest_configure(config):
    if config.option.markexpr == "tck":
        config._tck_discovery = _tck_discovery()


def pytest_report_header(config):
    if not hasattr(config, "_tck_discovery"):
        return None
    discovery, scenarios, exclusions, structural_modules = config._tck_discovery
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
    reports = [
        report
        for reports in terminalreporter.stats.values()
        for report in reports
        if getattr(report, "when", None) == "call"
        and "::test_tck_scenario[" in report.nodeid
    ]
    totals = {
        outcome: sum(report.outcome == outcome for report in reports)
        for outcome in ("passed", "failed", "skipped")
    }
    executed = totals["passed"] + totals["failed"]
    terminalreporter.write_line(
        "TCK totals: "
        f"selected={len(scenarios)}, structural-skips={discovery.structural_skips}, "
        f"structural-module-cases={len(structural_modules)}, "
        f"executed={executed}, active-exclusions={totals['skipped']}, passed={totals['passed']}, "
        f"failed={totals['failed']}"
    )


@pytest.fixture(autouse=True)
def clean_dataweave_runtime(request):
    """Keep module-level isolate state from leaking between integration tests."""
    if request.node.get_closest_marker("tck"):
        yield
        return
    dataweave.cleanup()
    yield
    dataweave.cleanup()


@pytest.fixture(scope="session")
def tck_runtime():
    """Own one isolate for the TCK session; deferred writers cannot be torn down per case."""
    runtime = dataweave.DataWeave()
    runtime.initialize()
    yield runtime


@pytest.fixture
def collect_stream():
    def collect(stream):
        chunks = list(stream)
        return b"".join(chunks), stream.metadata

    return collect
