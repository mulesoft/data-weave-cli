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

## Fix Round 2

- The foundation Gradle build now passes `-PskipPythonTests=true`, leaving the
  Python artifact action as the only CI owner that installs Python dependencies
  and runs `native-lib:pythonTest`.
- The Python wheel is built and uploaded before the optional Python TCK. This
  preserves the package artifact when TCK conformance fails, while the TCK
  result still determines the final job outcome.
- The Python and Node artifact steps use `continue-on-error: true`, so a failed
  binding lane does not prevent the other lane from executing. A following
  `always()` aggregation step fails the job when either binding lane failed.
- Python TCK JUnit artifacts include the workflow matrix platform token:
  `python-tck-junit-${{ inputs.platform }}`, preventing cross-platform upload
  name collisions.
- The strict xfail baseline remains unchanged: only the three accepted named
  mismatches are strict xfails; any new mismatch and any XPASS fail the TCK.

## Fix Round 3

- The Python and Node master-only TCK conformance steps now use
  `always() && inputs.run-tck == 'true'`, so they run even if an earlier step
  in the same composite action failed.
- The binding failure aggregation step remains guarded by `always()` and the
  Python/Node step outcomes, but now follows the Native library artifact step.
  It is therefore the workflow's final binding-failure verdict.
- CI structure tests assert both exact TCK conditions and that Native library
  precedes the aggregation step. Strict Python TCK xfails remain unchanged.

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

### Fix Round 2 Verification

1. RED: `python3 -m pytest tests/unit/test_ci_structure.py -m unit -v` failed
   with the missing platform input/artifact name and missing
   `-PskipPythonTests=true` foundation flag.
2. GREEN: the same focused CI structure test passed with `5 passed` after the
   workflow, action, and structure-test updates.
3. YAML parsing for the changed workflow/actions and `git diff --check` passed.
4. Foundation-equivalent dry run/build command passed:
   `./gradlew --stacktrace --no-problems-report -PskipNodeTests=true
   -PskipPythonTests=true -PskipTCKTests=true build`.

### Fix Round 3 Verification

1. RED: `python3 -m pytest tests/unit/test_ci_structure.py -m unit -v` failed
   because the Node TCK condition lacked `always()` and the binding aggregation
   preceded Native library.
2. GREEN: the same focused test passed with `5 passed` after updating both
   composite action TCK guards and moving final aggregation.
3. YAML parsing for the changed workflow/actions and `git diff --check` passed.

## Commit

- Pending Fix Round 3 commit: `ci: run binding TCKs after failures`

## Concerns

- The accepted baseline is deliberately narrow: only the three named strict
  xfails are tolerated. Any new mismatch remains a blocking `pythonTck`
  failure; a repaired accepted mismatch becomes an XPASS and also fails.
- Native-image emits existing GraalVM deprecation warnings during local runs.
