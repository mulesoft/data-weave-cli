# PR #167 CI Fix Report

## Status

Implemented both confirmed Python CI fixes and both validated follow-up review fixes without changing Node, Java/native code, or the C ABI.

## Changes

- Removed the `unit` marker from `test_only_declared_case_identifiers_are_excluded`, because it validates the staged TCK corpus inventory. Added a CI-structure regression test that prevents this corpus-dependent policy test from re-entering the normal `unit or integration` lane.
- Captured the Python owner thread identity after Graal isolate creation.
- Added a current-thread attachment context around synchronous buffered native operations. Worker-thread calls now use one worker-local isolate thread for native invocation, response decode/free, and final detach. Existing streaming workers pass their explicitly attached thread and therefore do not attach twice.
- Made cleanup from a non-owner OS thread attach that thread before isolate teardown. Failed teardown detaches the temporary thread while preserving runtime state for retry; a successful teardown does not detach because the isolate no longer exists.
- Replaced the direct in-process native concurrency regression with subprocess-backed coverage. The parent test asserts subprocess exit code 0, serialized resolver entry, and two successful results.
- Replaced the isolate owner's recyclable Python thread identifier with the owning `Thread` object. Attachment and cleanup decisions now compare `current_thread()` by object identity, and reset clears the owner reference.
- Restored all four raw-pointer methods to caller-owned explicit-thread semantics. They remain serialized but do not automatically attach or detach; automatic attachment remains around the atomic `*_and_decode` invoke/decode/free methods only.
- Added regression coverage for coincident thread identifiers across distinct `Thread` objects, owner-reference reset, all four raw-pointer methods, and the streaming worker's single explicit attachment.

## TDD Evidence

- The new CI-structure test initially failed because the corpus inventory test had an explicit `unit` marker, then passed after removing the marker.
- The worker execution tests initially failed because no owner identity or worker attachment existed, then passed after the attachment context was implemented.
- The worker cleanup tests initially failed because teardown reused the owner thread pointer, then passed after non-owner cleanup attachment was implemented.
- The primary-error test initially failed because a detach error masked the teardown error, then passed after cleanup preserved the teardown exception.
- The recycled-identifier regression initially failed because `NativeRuntime` had no owner `Thread` reference and the worker bypassed attachment when identifiers coincided, then passed after owner tracking switched to object identity.
- The raw-pointer regression initially failed for all four methods because passing the isolate's owner pointer from a worker caused automatic replacement with an attached pointer, then passed after raw methods stopped using the attachment context.

## CI Failure Correlation

The Linux log showing nine dots before exit 99 is consistent with the crash occurring in the tenth test in `tests/integration/test_module_resolver.py`: `test_overlapping_resolver_aware_runs_are_serialized`. The file has nine preceding integration tests, and the tenth test was the only one that initialized the isolate on the main OS thread and then called `DataWeave.run()` directly from Python worker threads. That reused the main thread's `IsolateThread`, matching Graal's wrong-thread abort behavior. The concurrency scenario is now isolated in a subprocess so any native abort is reported as a normal test failure rather than terminating pytest.

## Verification

- Focused owner/raw-pointer/streaming unit tests: `77 passed`.
- Normal lane: `162 passed`, `765 deselected`; `test_only_declared_case_identifiers_are_excluded` remains absent while pure TCK policy unit tests remain selected.
- `./gradlew native-lib:pythonTest`: `162 passed`, `765 deselected`; build successful.
- `./gradlew native-lib:stageTckSuites native-lib:pythonTck`: `719 passed`, `31 skipped`, `19 xfailed`, `158 deselected`; TCK accounting reports `729 selected`, `679 passed`, `0 failed`, `0 unaccounted`; build successful.
- `git diff --check`: clean.

One initial TCK invocation overlapped a concurrent `pythonTest` native build and failed Gradle output-state validation after both builds wrote the same native artifact. The required command was rerun serially and completed successfully with the totals above.

## Concerns

- Verification ran on macOS arm64 with GraalVM Community Java 24. Linux and Windows behavior is covered structurally and by subprocess containment but still relies on CI for platform-specific confirmation.
- Existing GraalVM/Gradle deprecation warnings and one TCK temporary-buffer warning remain unchanged.
