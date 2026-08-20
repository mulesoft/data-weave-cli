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

- `b285d21 test: add Python TCK conformance lane`

## Concerns

- The current staged corpus has a real, non-excluded failure in `core-modules/csv-invalid-utf8-out.csv`: the runtime emits a replacement character where the fixture expects an empty CSV value. It is intentionally not excluded so `pythonTck` remains a failing conformance gate.
- The original full run did not reach terminal reporting because the generic
  per-test cleanup could block after a successful deferred writer. This is a
  lifecycle stall, not aggregate runtime-initialization cost; Fix Round 1
  replaces that lifecycle for TCK scenarios.

## Fix Round 1

### Lifecycle And Regression Coverage

- The TCK lane now owns one session-scoped `DataWeave` instance. The generic
  module-level runtime cleanup fixture is bypassed for all `tck`-marked tests,
  so a deferred writer is not followed by per-scenario isolate destruction.
- The session runtime deliberately remains process-scoped after pytest's
  terminal summary. Direct isolate teardown after
  `deferred-write-should-terminate-out.json` blocks in Graal; attempting it at
  session teardown would again prevent pytest from reaching its report. Python
  process exit owns final release of this TCK-only isolated runtime.
- RED: the deferred-write regression failed with `fixture 'tck_runtime' not
  found`. GREEN: it runs the actual deferred-write corpus transform and then a
  second transform successfully using the same runtime.

### Exclusions And Reporting

- The active registry contains exactly 18 runnable module-import scenarios:
  the original six plus the 12 observed unresolved `dw::Client`/`dw::Natives`
  cases. Registry validation now rejects stale or structurally unreachable
  entries.
- The 13 former registry entries which are loader `transform-shape` structural
  skips are not active exclusions. They are reported separately as
  `structural-module-cases=13`.
- The terminal report now reconciles scenario-only totals:
  `selected=731`, `executed=713`, `active-exclusions=18`, `passed=673`, and
  `failed=40`. Thus `executed + active-exclusions == selected` and
  `passed + failed == executed`.
- XML comparison retains child tail text, so structural comparison no longer
  treats `<child/>actual` and `<child/>expected` as equivalent.

### Verification

1. `python3 -m pytest tests/tck/test_conformance.py -m tck -k 'deferred-write-should-terminate or is-empty-using-empty-stream or streaming_binary_inside_value or try-handle' -vv`
   Result: 2 passed, 12 skipped. The deferred-write case completed and each of
   the 12 newly active unresolved-module exclusions skipped as categorized.
2. `python3 -m pytest tests/tck/test_conformance.py -m tck -k 'discover or compare or exclusion or tck_summary or tck_session_runtime' -vv`
   Result: 18 passed.
3. `./gradlew native-lib:stageTckSuites native-lib:pythonTck`
   Result: terminal pytest report was reached in 34.87 seconds. It reported
   `selected=731, structural-skips=191, structural-module-cases=13,
   executed=713, active-exclusions=18, passed=673, failed=40`, then Gradle
   failed as expected because non-excluded conformance mismatches remain.
   `core-modules/csv-invalid-utf8-out.csv` remains a reported text mismatch and
   was not excluded. The stage task continues to reuse the Node TCK artifacts;
   no duplicate suite download was introduced.

### Updated Concern

- `pythonTck` is intentionally still a failing conformance gate for the 40
  active, non-excluded mismatches. This round fixes the deferred-writer
  teardown stall and reporting attribution; it does not claim to resolve those
  runtime/output compatibility failures.
