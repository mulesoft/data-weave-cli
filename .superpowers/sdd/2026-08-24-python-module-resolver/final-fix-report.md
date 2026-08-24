# Python Module Resolver Final Fix Report

**Date:** 2026-08-24
**Status:** Complete

## Scope

Resolved all three final-review findings in the Python binding. No Node, Java/native, or ABI source was changed.

## Fixes

1. Resolver callback dynamic extent
   - Added an instance-owned `_resolver_active` gate around `run_script_with_resolver`.
   - The retained native callback now returns `NULL` unless a resolver-aware run is currently active.
   - Serialized all native execution entry points on the instance lock so a resolver-less call cannot observe the resolver-active window from another thread.
   - Added native integration coverage that installs the resolver synchronously, then verifies external imports fail through `run_streaming`, `run_transform`, `run_callback`, and `run_input_output_callback` without additional resolver calls or output callback data.

2. Resolver-aware execution concurrency
   - Added one `Lock` per `NativeRuntime` and hold it across resolver setup, native invocation, callback-buffer lifetime, and cleanup.
   - Cleanup uses the same lock, so isolate teardown cannot race an active resolver-aware invocation. The callback does not acquire the lock, avoiding callback re-entry deadlock.
   - Added a deterministic two-thread unit regression proving a second resolver-aware native invocation cannot overlap or clear the first call's buffer.
   - Added real native two-thread integration coverage proving overlapping resolver-aware `DataWeave.run()` calls complete serially.

3. JAR read context
   - `modules_from_jars` now wraps `RuntimeError` and `NotImplementedError` raised while reading ZIP entries in the existing archive-context `ValueError`.
   - Added focused parameterized coverage preserving the original exception as `__cause__` and naming the archive.

## TDD Evidence

- Dynamic-extent integration test initially failed because `run_streaming` succeeded after resolver installation.
- Two-thread unit test initially failed because the second native invocation entered before the first was released.
- Cleanup race test initially failed because isolate teardown entered while a resolver-aware call was active.
- JAR read tests initially exposed raw `RuntimeError` and `NotImplementedError` without archive context.
- Each regression passed after the corresponding minimal production change.

## Verification

- Focused resolver suites:
  - `python3 -m pytest tests/unit/test_native.py tests/unit/test_resolver.py tests/integration/test_module_resolver.py -q`
  - Result: `60 passed`.
- Python test lane:
  - `./gradlew native-lib:pythonTest`
  - Result: `139 passed, 764 deselected`; Gradle build successful.
- Python TCK:
  - `./gradlew native-lib:stageTckSuites native-lib:pythonTck`
  - Result: `719 passed, 31 skipped, 19 xfailed, 134 deselected`; `failed=0`, `accounted=729`, `unaccounted=0`; Gradle build successful.
- `git diff --check`: clean.

## Concerns

- The instance lock intentionally serializes all native execution on one `NativeRuntime`, including resolver-less streaming calls, once they reach the low-level bridge. This is the smallest safe fix because the isolate thread and retained resolver callback are instance-shared.
- Existing GraalVM/Gradle deprecation and native-access warnings remain unchanged.
