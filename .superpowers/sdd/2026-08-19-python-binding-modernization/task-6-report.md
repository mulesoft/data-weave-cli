# Task 6 Report: Gate Artifacts And Document Actual Behavior

## Changes

- `.github/actions/python/action.yml` installs the Python `test` extra, runs
  `native-lib:pythonTest` before `native-lib:buildPythonWheel`, and exposes a
  `run-tck` input for the Python conformance lane.
- `.github/workflows/main.yml` passes `run-tck` only when the ref is `master`,
  alongside the existing Node TCK gate. `pythonTck` stages and reuses the same
  corpus as Node through its existing Gradle dependency.
- `native-lib/python/tests/unit/test_ci_structure.py` asserts the artifact-test
  ordering and the master-only TCK wiring.
- `native-lib/python/README.md` now documents pytest normal and TCK commands,
  bounded queue/chunk behavior, callback abort semantics, post-completion
  stream metadata, module-resolution exclusions, and the remaining TCK
  failures.
- `native-lib/python/examples/streaming_demo.py` no longer presents the
  unsupported `input_properties` argument.

## TDD And Verification

1. RED: `python3 -m pytest tests/unit/test_ci_structure.py -m unit -v`
   initially failed because the artifact action did not invoke
   `native-lib:pythonTest`.
2. GREEN: the same focused command passed with `2 passed` after the action and
   workflow wiring were added.
3. `./gradlew native-lib:pythonTest`
   passed with `72 passed, 751 deselected` and regenerated the configured JUnit
   and coverage XML reports.
4. `./gradlew native-lib:stageTckSuites native-lib:pythonTck`
   completed its terminal report and failed as expected: `693 passed, 55
   skipped, 3 failed`. The three intentional non-excluded mismatches are
   `csv-invalid-utf8-out.csv`, `number-addition-out.json`, and
   `number-subtraction-out.json`.
5. YAML parsing and `git diff --check` passed.

## Commit

- Pending: `ci: validate Python binding tests and TCK`

## Concerns

- The master-only `pythonTck` command is intentionally terminally failing
  while the three genuine runtime conformance mismatches remain. This task
  reports them and does not alter core TCK scope or hide them with exclusions.
- Native-image emits existing GraalVM deprecation warnings during local runs.
