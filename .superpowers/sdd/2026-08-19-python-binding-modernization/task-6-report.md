# Task 6 Report: Gate Artifacts And Document Actual Behavior

## Fix Round 1

- The three accepted baseline conformance mismatches are now visible strict
  pytest xfails. Each uses its full scenario identifier and an explicit reason:
  `core-modules/csv-invalid-utf8-out.csv`,
  `core-modules/number-addition-out.json`, and
  `core-modules/number-subtraction-out.json`.
- No other conformance failure is excluded or xfailed. A new unexpected
  mismatch still fails `pythonTck`; an XPASS also fails because the xfails are
  strict.
- Python test dependency installation now belongs solely to the Python artifact
  action, rather than the shared build foundation. The action publishes the
  Python TCK JUnit report with `always()` whenever its master-only TCK lane ran.
- The master workflow stages the runtime/core-modules corpus once before the
  Python and Node artifact actions. Neither binding action restages it; local
  `pythonTck` remains usable after an explicit `stageTckSuites` invocation.

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

1. RED: focused CI structure and strict-xfail tests failed before moving
   dependency ownership, staging, report upload, and mismatch marks. The
   failures identified foundation-owned dependencies, duplicate corpus staging,
   missing upload wiring, and absent xfail parameters.
2. GREEN: `python3 -m pytest tests/unit/test_ci_structure.py
   tests/tck/test_conformance.py -m unit -k 'strict_xfails or artifact_owns or
   stages_the_shared' -vv` passed: `3 passed`.
3. `./gradlew native-lib:pythonTest` passed with `76 passed, 751 deselected`
   and regenerated normal JUnit and coverage reports.
4. `./gradlew native-lib:stageTckSuites` stages the shared corpus once; the
   terminal `./gradlew native-lib:pythonTck` run passed with `695 passed, 55
   skipped, 3 xfailed`. Its terminal report records `xfail=3`; all other
   selected scenarios passed or used independently categorized exclusions.
5. YAML parsing and `git diff --check` are recorded with the final change.

## Commit

- Pending Fix Round 1 commit: `ci: make Python TCK mismatches strict xfails`

## Concerns

- The accepted baseline is deliberately narrow: only the three named strict
  xfails are tolerated. Any new mismatch remains a blocking `pythonTck`
  failure; a repaired accepted mismatch becomes an XPASS and also fails.
- Native-image emits existing GraalVM deprecation warnings during local runs.
