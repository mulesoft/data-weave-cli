# Task 4 Report: Native Adapter And Runtime Streaming

## Status

Complete. The Python binding now separates native ctypes/isolate ownership from the public `DataWeave` orchestration API.

## Changes

- Added `dataweave.native.NativeRuntime` for native library discovery/loading, opaque Graal pointer types, ABI registration, isolate/thread lifecycle, native invocation, and C-string release.
- Added `dataweave.runtime.DataWeave` for buffered execution, direct callbacks, and both streaming APIs.
- Consolidated output-only and duplex streaming onto one worker implementation with bounded queue backpressure, cancellation, timed consumer waits, worker join timeout, metadata propagation, and detach in `finally`.
- Preserved oversized input chunk remainders and Python callback failure translation to the native `-1` abort status.
- Reduced `dataweave.__init__` to the public facade, singleton lifecycle, re-exports, and legacy private helper aliases.
- Added unit coverage for native ABI registration, load failure, idempotent cleanup, module ownership, and worker timeout behavior.

## Verification

- `python3 -m pytest tests/unit tests/integration` passed: 53 tests.
- `./gradlew native-lib:pythonTest` passed: native image build and 53 Python tests.

## Concerns

- The Gradle native-image invocation emits existing Graal/Gradle deprecation and restricted-native-access warnings; it still completed successfully.
- No Task 5, documentation, or CI work was included.

## Fix Round 1

- Made initialization transactional: failed isolate creation or ABI validation now resets all native state; after isolate creation, validation failure attempts isolate teardown before reset and reraises the primary failure.
- Required `run_script`, `free_cstring`, and isolate teardown exports at initialization. Streaming callback exports additionally require attach/detach exports before the runtime is considered initialized.
- Validated isolate teardown and worker detach return codes. Cleanup failures now surface as `DataWeaveError`; worker detach failures surface only when there was no preceding execution failure.
- Removed obsolete `DataWeave` private-state forwarding and test-only Graal setup compatibility machinery. Tests now configure `NativeRuntime` directly.
- Added focused coverage for failed isolate creation, partial-initialization cleanup, missing required exports, teardown return codes, and detach return codes.

### Fix Round 1 Verification

- Final focused and full Python verification passed: 25 focused lifecycle/streaming tests and 61 unit/integration tests.
- `./gradlew native-lib:pythonTest` passed: native image build and all 61 Python tests.

## Fix Round 2

- Treated unsuccessful native streaming metadata as the primary execution outcome, so worker detach failures do not replace a script failure.
- Preserved an active context-manager body exception when cleanup also fails; cleanup failures still surface when no body exception is active.
- Cleared the facade singleton in a `finally` block, allowing reinitialization after native cleanup raises.
- Validated `graal_create_isolate` is exported and wrapped its invocation failure as contextual `DataWeaveError`.
- Wrapped `graal_attach_thread` and `graal_detach_thread` invocation failures as contextual `DataWeaveError`; the stream worker suppresses detach failures whenever attach, execution, decode, or native failure metadata is primary.
- Added focused tests for missing/throwing isolate creation, throwing attach/detach lifecycle calls, failed singleton cleanup, context cleanup precedence, and unsuccessful streaming metadata plus detach failure.

### Fix Round 2 Verification

- Focused lifecycle/native/streaming/facade tests passed: 38 tests.
- Full Python unit and integration suite passed: 70 tests.
- `./gradlew native-lib:pythonTest` passed: native image build and all 70 Python tests.

### Fix Round 2 Concerns

- The Gradle native-image build continues to emit pre-existing Graal/Gradle deprecation and restricted-native-access warnings, but the build and Python suite completed successfully.
