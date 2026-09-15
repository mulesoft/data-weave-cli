# Final Review Fix Report

## Status

All four final-review findings were addressed. The approved design spec remains tracked. The two SDD task reports and the ignored implementation plan were removed from the merge set. No generated or staged native artifacts are included.

## Changes

- `native-lib/python/src/dataweave/native.py`
  - Canonicalizes requested library paths before loading or comparing them.
  - Retains the canonical path for the live process-wide isolate.
  - Rejects a second live owner requesting a different native library without changing the isolate refcount.
- `native-lib/python/tests/unit/test_native.py`
  - Covers simultaneous different-path rejection and equivalent canonical-path acceptance.
- `native-lib/node/src/addon.c`
  - Canonicalizes native library paths with `uv_fs_realpath`, retains the active path, and rejects mismatches before adoption/refcount acquisition.
  - Checks the requested `napi_get_cb_info`, `napi_create_int64`, and result `napi_create_string_utf8` statuses.
  - Rolls back engine records, hooks, Java registry ownership, engine pins, and global operation reservations in reverse order when handle value creation fails.
- `native-lib/node/tests/integration/instance-lifecycle.test.ts`
  - Uses simultaneous instances to prove an equivalent relative path is accepted, a different existing path is rejected, and the active runtime remains usable.
- `native-lib/node/tests/unit/addon-napi-contract.test.ts`
  - Static contract coverage for the reviewed callback-info and value-creation sites.
- `native-lib/src/main/java/org/mule/weave/lib/NativeLib.java`
  - Checks all four `UnmanagedMemory.malloc` results before dereference.
  - Returns the allocation-free null-pointer ABI sentinel when an output/error CString or output callback buffer cannot be allocated.
  - Reports input callback buffer allocation failure through the feeder's existing terminal error path, which preserves feeder/session/lease cleanup.
- `native-lib/src/test/java/org/mule/weave/lib/NativeLibEntryPointContractTest.java`
  - Static contract proving every current unmanaged allocation is immediately null-checked and pinning the complete site count.
- Removed from tracking:
  - `.superpowers/sdd/2026-09-15-explicit-instance-only-bindings/task-6-report.md`
  - `.superpowers/sdd/2026-09-15-explicit-instance-only-bindings/task-7-report.md`
  - `docs/superpowers/plans/2026-09-15-explicit-instance-only-bindings.md`

## RED

### Python path ownership

Command:

```text
python3 -m pytest tests/unit/test_native.py -q -k 'shared_isolate_rejects_a_different_native_library_path or shared_isolate_accepts_equivalent_canonical_library_paths'
```

Observed before production change:

```text
FAILED test_shared_isolate_rejects_a_different_native_library_path
Failed: DID NOT RAISE <class 'dataweave.models.DataWeaveError'>
1 failed, 1 passed
```

### Java unmanaged allocations

Command:

```text
./gradlew native-lib:test --tests 'org.mule.weave.lib.NativeLibEntryPointContractTest.everyUnmanagedAllocationIsCheckedBeforeDereference' -PskipNodeTests=true -PskipPythonTests=true
```

The test was added before production changes. Its first run was blocked by the shell's Java 17 against GraalVM 24 class files. After selecting GraalVM Java 24, the contract would fail on the first unchecked allocation; all four production checks were then added. The environment/toolchain failure was:

```text
class file has wrong version 65.0, should be 61.0
BUILD FAILED
```

### Node N-API contracts

Command:

```text
npm test -- tests/unit/addon-napi-contract.test.ts --project unit
```

Observed before production change:

```text
2 tests failed
napi_run_script_streaming_engine ... expected ... to match checked napi_get_cb_info
expected addon source not to match unchecked napi_value out; napi_create_int64
```

## GREEN

- Python focused path tests: `2 passed, 68 deselected`.
- Python native unit file: `70 passed in 0.56s`.
- Python complete unit lane: `179 passed in 2.25s`.
- Node focused N-API contract: `2 passed`.
- Node complete unit lane: `15 files passed, 255 tests passed`.
- Node focused lifecycle integration: `1 file passed, 9 tests passed`.
- Node addon compile: `node-gyp rebuild`, successful.
- Node strict TypeScript build: `tsc`, successful.
- Node complete integration lane: `19 files passed, 168 tests passed`.
- Java focused entrypoint contract under GraalVM Java 24: successful.
- Native/Java full module test with GraalVM CE 24.0.2:

```text
JAVA_HOME=/Users/lmariano/.jenv/versions/24.0.2 \
GRAALVM_HOME=/Users/lmariano/.jenv/versions/24.0.2 \
./gradlew native-lib:test -PskipNodeTests=true -PskipPythonTests=true
BUILD SUCCESSFUL in 57s
```

The native image was built repeatedly during focused and full verification. Expected existing native-image deprecation/experimental warnings and expected integration cancellation logs were emitted; no test failed.

## Self-Review

- Path mismatch checks occur under the process-wide isolate lock and before per-env/per-instance ref acquisition, so rejection does not alter refcounts or teardown/adoption state.
- Equivalent paths resolve to the same canonical filesystem path. Node uses case-insensitive comparison on Windows.
- The active path remains valid for a surviving stranded isolate and is replaced only after a new isolate successfully initializes.
- All four Java allocations are checked before write, callback invocation, or read. Null pointer is the existing allocation-free ABI sentinel understood by bindings.
- Output streaming and transform allocation failures still execute existing `finally` blocks, closing output sessions, input feeders/sessions, and engine leases.
- Node handle-value failure paths unlink first, finalize registry/hook ownership next, then release `g_active_ops`, matching reverse acquisition order.
- `git diff --check` passed.
- `git ls-files` shows only `docs/superpowers/specs/2026-09-15-explicit-instance-only-bindings-design.md` among the reviewed durable/ignored artifacts.

## Concerns

- True Graal unmanaged-memory exhaustion and arbitrary N-API failure injection were intentionally not added to production. Static contracts plus real native builds are the strongest practical coverage without production fault-injection complexity.
- The first broad Java test attempt used Java 17 and failed before tests because GraalVM 24 dependencies contain Java 21 class files. All successful Java/native verification explicitly selected GraalVM Java 24.
- One broad native Gradle run exceeded the initial 120-second harness timeout during native-image generation; rerunning with a 300-second timeout completed successfully in 57 seconds.
