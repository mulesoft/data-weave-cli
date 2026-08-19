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
