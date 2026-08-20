# Final Review Remediation Report

## Resolutions

1. `Stream.close()` is now public and `Stream` supports `with`. Closing sets
   cancellation and generator cleanup is non-raising. Native workers use a
   bounded 0.1-second join and are daemon threads because Python cannot cancel
   an in-progress native call. Public early-close and uncancellable-worker
   finalization regression tests cover this policy.
2. The low-level read callback now rejects data larger than the native buffer
   with `-1`; it no longer silently truncates callback input. The iterable
   transform path continues preserving chunk remainders.
3. The TCK session fixture now always calls `runtime.cleanup()`. Only the
   deferred-writer scenario executes in a subprocess, preventing its known
   isolate-teardown stall from leaving the session runtime unmanaged.
4. TCK skips now contain only 39 binding/environment capability cases: 24
   module-resolution, 11 unavailable Java-module, and 4 unavailable classpath
   resource cases. The 19 runtime/output differences are exact strict xfails;
   an XPASS or an unlisted mismatch fails the lane.
5. XML comparison now preserves namespace prefixes while ignoring `xmlns`
   declaration placement, aligning with the Node comparator. Tail text remains
   significant.
6. README now documents strict xfails, deferred-writer subprocess isolation,
   `Stream.close()`/context-manager behavior, bounded native-worker policy,
   oversized callback input rejection, and the correct `DataWeave(lib_path)`
   parameter name.
7. Restored the tracked approved design at
   `docs/superpowers/specs/2026-08-19-python-binding-modernization-design.md`.

## TDD Evidence

- Red: public close/context manager, oversized callback input, bounded
  finalization, strict-xfail expansion, and namespace-prefix tests initially
  failed against the prior implementation.
- Green: focused streaming tests passed `4 passed`; strict-xfail characterization
  passed; complete unit suite passed `56 passed`.

## Verification

1. `./gradlew native-lib:pythonTest`
   - Passed: `81 passed, 752 deselected`.
2. `./gradlew native-lib:stageTckSuites native-lib:pythonTck`
   - Passed: `696 passed, 39 skipped, 79 deselected, 19 xfailed`.
   - TCK totals: `selected=731`, `executed=673`, `passed=673`, `failed=0`,
     `active-exclusions=39`, `xfail=19`.
3. `git diff --check`
   - Passed before final report creation; rerun before commit.

## Remaining Limitations

- Python cannot forcibly cancel a native call. A cancelled worker that never
  returns is daemonized after the bounded join, so it can retain native resources
  until process exit.
- The deferred-writer TCK scenario is isolated in a subprocess because its
  native isolate teardown can block. The main TCK session runtime is managed.
- Native-image continues to emit existing GraalVM deprecation and experimental
  option warnings during builds.
