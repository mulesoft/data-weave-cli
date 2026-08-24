# Task 4 Report: Real Native Module Resolution and Isolate Lifetime

## Status

Complete. Added a real native integration gate for Python synchronous module
resolution against the staged `dwlib`.

## Changes

- Added `native-lib/python/tests/integration/test_module_resolver.py` with
  coverage for map-backed resolution, missing modules, and `raise_on_error`.
- Proved directory and JAR resolution, including callback re-entry for
  transitive imports.
- Proved repeated resolver-backed runs on one explicit runtime.
- Proved simultaneous explicit runtimes can resolve the same module path to
  different source and that cleaning up one isolate does not invalidate the
  other.
- Proved streaming scripts using built-in modules do not invoke the configured
  external resolver, documenting that external streaming resolution remains
  unsupported.

## TDD Evidence

- The initial map, missing-module, and `raise_on_error` tests passed against the
  completed Tasks 1-3 implementation.
- Per the Task 4 brief, the map source was temporarily replaced with `None`.
  `test_run_resolves_module_from_map` then failed because the result was an
  unsuccessful `Unable to resolve module` response. The valid module source was
  restored and the focused tests passed.
- The first complete integration run exposed two test-assumption defects rather
  than production defects: DataWeave may request the same module multiple times,
  and `upper` is not exported from `dw::core::Strings` in this runtime. The
  repeated-run test was corrected to assert observable execution behavior, and
  the streaming test now imports the verified built-in
  `dw::core::Binaries::fromBase64` function.

## Verification

- `./gradlew native-lib:stagePythonNativeLib`: passed.
- `python3 -m pytest tests/integration/test_module_resolver.py -q`: 8 passed.
- `./gradlew native-lib:pythonTest`: 125 passed, 766 deselected; build
  successful. The normal Python lane did not execute the full TCK.

## Task 1-3 Defects

No implementation defects were found. No Task 1-3 production files required
changes.

## Constraints

- No Node changes.
- No Java/native changes.
- No ABI changes.
- Resolver support remains synchronous `DataWeave.run()` only.

## Concerns

The native runtime emits `Module resolver already set for this process` while
running resolver tests after an earlier resolver-backed isolate. The integration
tests nevertheless prove distinct live Python isolates retain independent
resolver behavior and cleanup isolation. This warning is existing native
behavior and is outside Task 4's no-Java/native-change boundary.
