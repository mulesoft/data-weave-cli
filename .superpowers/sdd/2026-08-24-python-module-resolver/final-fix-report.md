# Python Module Resolver Final Fix Report

**Date:** 2026-08-24
**Status:** Complete

## Scope

Resolved the original three final-review findings and both residual re-review findings in the Python binding. No Node, Java/native, or ABI source was changed.

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

4. Explicit re-entry failure
   - Replaced implicit non-reentrant-lock deadlocks with an execution-owner guard around every serialized native operation.
   - Same-thread nested entry now raises `DataWeaveError` immediately, while other threads continue to block on the per-runtime lock and execute serially.
   - Resolver callbacks continue to translate failures to `NULL`; read and write callback adapters continue to translate failures to `-1`, so the re-entry error never unwinds across a C callback.
   - Added deterministic timeout-bounded regressions for direct low-level re-entry, resolver callback re-entry, write callback re-entry, and read callback re-entry.

5. Result decode/free lifecycle serialization
   - Added low-level invoke-and-decode methods that keep native invocation, UTF-8 decoding, and `free_cstring` inside one serialized operation.
   - Updated buffered, callback, streaming-worker, and input/output callback paths to use those methods.
   - Added a deterministic four-path regression that blocks `free_cstring` and proves cleanup cannot enter isolate teardown until result decoding/freeing completes.
   - Existing cleanup retry state preservation and exception masking behavior remain unchanged and covered.

## TDD Evidence

- Dynamic-extent integration test initially failed because `run_streaming` succeeded after resolver installation.
- Two-thread unit test initially failed because the second native invocation entered before the first was released.
- Cleanup race test initially failed because isolate teardown entered while a resolver-aware call was active.
- JAR read tests initially exposed raw `RuntimeError` and `NotImplementedError` without archive context.
- Direct, resolver, and write-callback re-entry tests initially timed out on the non-reentrant lock; the read callback case also verifies callback status translation.
- Invoke-and-decode lifecycle tests initially failed because the required atomic low-level methods did not exist; after adding the API shape, the old split operation allowed cleanup to race before `free_cstring`.
- Each regression passed after the corresponding minimal production change.

## Verification

- Focused Python runtime, resolver, callback, and lifecycle suites:
  - `python3 -m pytest tests/unit/test_native.py tests/unit/test_facade.py tests/unit/test_streaming.py tests/unit/test_resolver.py tests/integration/test_module_resolver.py tests/integration/test_callbacks.py tests/integration/test_lifecycle.py -q`
  - Result: `105 passed`.
- Python test lane:
  - `./gradlew native-lib:pythonTest`
  - Result: `147 passed, 764 deselected`; Gradle build successful.
- Python TCK:
  - `./gradlew native-lib:stageTckSuites native-lib:pythonTck`
  - Result: `719 passed, 31 skipped, 19 xfailed, 142 deselected`; `failed=0`, `accounted=729`, `unaccounted=0`; Gradle build successful.
- `git diff --check`: clean.

## Concerns

- The instance lock intentionally serializes all native execution on one `NativeRuntime`, including resolver-less streaming calls, once they reach the low-level bridge. This is the smallest safe fix because the isolate thread and retained resolver callback are instance-shared.
- Re-entry is intentionally unsupported for one `NativeRuntime`; callers needing nested evaluation must use a separate initialized `DataWeave` instance with its own isolate.
- Existing GraalVM/Gradle deprecation and native-access warnings remain unchanged.
