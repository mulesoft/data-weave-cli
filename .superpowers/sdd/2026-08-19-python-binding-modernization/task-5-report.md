# Task 5 Report: Python TCK Harness

## Files

- `native-lib/build.gradle`
- `native-lib/python/tests/__init__.py`
- `native-lib/python/tests/conftest.py`
- `native-lib/python/tests/tck/__init__.py`
- `native-lib/python/tests/tck/case_loader.py`
- `native-lib/python/tests/tck/compare.py`
- `native-lib/python/tests/tck/ignore_list.py`
- `native-lib/python/tests/tck/test_conformance.py`

## TDD And Test Commands

1. RED: `python3 -m pytest tests/tck/test_conformance.py -m tck -k 'module_resolution_exclusions_only_skip_importing_cases' -vv`
   Result: failed as expected because `validate_exclusions` did not accept or validate staged scenarios.
2. GREEN: `python3 -m pytest tests/tck/test_conformance.py -m tck -k 'discover or compare or exclusion or tck_summary' -vv`
   Result: 14 passed, 732 deselected.
3. Exclusion visibility: `python3 -m pytest -m tck -k 'import-lib-out or import-star-out' -vv`
   Result: 2 skipped, with header `discovered=731`, `structural-skips=191`, and `categorized-exclusions=6 (module-resolution-not-supported=6)`.
4. Non-excluded failure guard: `python3 -m pytest -m tck -x -vv`
   Result: 59 passed, then `core-modules/csv-invalid-utf8-out.csv` failed with `text mismatch`; the harness exited nonzero. The terminal report showed `passed=59, failed=1`.
5. Corpus staging: `./gradlew native-lib:stageTckSuites`
   Result: passed; staged the resolved `runtime` and `core-modules` artifacts under the existing Node TCK corpus directory.
6. Gradle lane: `./gradlew native-lib:stageTckSuites native-lib:pythonTck`
   Result: invoked native staging, the shared corpus staging task, and pytest. The initial complete run exceeded the local command time limit after reaching the corpus tests. A subsequent direct pytest run established the required non-excluded failure behavior above.

## Exclusions Discovered

- 19 staged runtime corpus cases import test-only DW modules.
- Every exclusion is keyed by full suite/case identifier and categorized as `module-resolution-not-supported` with the explicit reason that the Python binding has no module resolver.
- Registry validation rejects missing category/reason and rejects this category for a discovered case whose transform does not contain a DW `import` directive.

## Commit

Pending commit: `test: add Python TCK conformance lane`

## Concerns

- The current staged corpus has a real, non-excluded failure in `core-modules/csv-invalid-utf8-out.csv`: the runtime emits a replacement character where the fixture expects an empty CSV value. It is intentionally not excluded so `pythonTck` remains a failing conformance gate.
- Full corpus execution can exceed the local command timeout because each parameterized scenario initializes the native runtime through the existing autouse cleanup fixture.
