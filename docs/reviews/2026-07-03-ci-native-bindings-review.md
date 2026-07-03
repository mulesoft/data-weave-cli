# Code Review: Fix CI Native Bindings Compilation

**Date**: 2026-07-03  
**PR**: https://github.com/mulesoft/data-weave-cli/pull/120  
**Commits**: 2105196..3187c94  
**Reviewer**: claude-unleashed session 940e3d0b

## Summary

The implementation successfully fixes the Windows Rust linker PATH conflict by splitting all Gradle build steps into OS-specific variants. The changes are **APPROVED** with minor observations noted below.

## Changes Overview

**File Modified**: `.github/workflows/main.yml`
- 96 insertions, 61 deletions
- Complete restructuring of build and package steps

### Key Improvements

1. **Fixed Windows Rust linker issue**: All Gradle steps now use `shell: cmd` on Windows and `shell: bash` on Unix
2. **Moved toolchain setup before build**: Node.js, Go, Rust, and CMake are now installed before the build step runs
3. **Removed `-PskipNodeTests=true`**: Node.js is now set up before build, so Node tests can run
4. **Removed duplicate test steps**: Tests run once via the `build -> test` dependency chain
5. **Added comprehensive documentation**: Comments explain the Windows shell requirement and test execution flow

## Detailed Analysis

### ✅ Correctness

**Root Cause Addressed**: The implementation correctly addresses the Rust linker PATH conflict by using `shell: cmd` on Windows. This ensures Visual Studio's `link.exe` is found before Git Bash's `/usr/bin/link.exe`.

**Test Coverage**: Verified that the Gradle `test` task in `native-lib/build.gradle` (lines 252-258) depends on all five binding tests:
- `pythonTest`
- `nodeTest`
- `goTest`
- `rustTest`
- `cTest`

This confirms that removing the separate test execution steps doesn't lose coverage—tests run during the build step.

**Toolchain Setup**: Moving Node.js, Go, Rust, and CMake setup before the build step is correct. This allows all binding tests to run during the build task without shell conflicts.

### ✅ Security

No security issues identified. The changes:
- Use official GitHub Actions (setup-node, setup-go, dtolnay/rust-toolchain, lukka/get-cmake)
- Don't introduce new secrets or credentials
- Don't modify authentication or authorization logic
- Switch from bash to cmd on Windows is actually a **defensive hardening** measure (prevents PATH manipulation via Git Bash)

### ✅ Code Quality

**Consistency**: All Gradle steps now follow the same pattern:
```yaml
- name: Step Name (Windows)
  if: runner.os == 'Windows'
  run: .\gradlew.bat [task]
  shell: cmd

- name: Step Name (Unix)
  if: runner.os != 'Windows'
  run: ./gradlew [task]
  shell: bash
```

**Documentation**: Excellent inline comments explaining:
- Why `shell: cmd` is required on Windows
- Which tests run during the build step
- How platform-specific shell selection works

**YAML Validation**: Passed Ruby `YAML.load_file` syntax check.

## Deviations from Plan

### 1. Removed `-PskipNodeTests=true` (Intentional)

**Plan said**: Keep the flag (Task 1, line 78)  
**Implementation**: Removed the flag

**Rationale**: The flag was originally needed because Node.js was set up *after* the build step. By moving Node.js setup before build, the flag became unnecessary and could be removed to enable Node tests.

**Verdict**: ✅ **Correct deviation**—improves test coverage

### 2. Removed All Individual Test Steps (Intentional)

**Plan Tasks 2-3**: Remove only Rust and C test steps  
**Implementation**: Removed Python, Node.js, Go, Rust, and C test steps

**Rationale**: All five test tasks run during `./gradlew build` (via the `test` task dependency). The plan's tasks 2-3 only called out Rust and C explicitly, but the same logic applies to all five binding tests.

**Verdict**: ✅ **Correct generalization**—reduces duplication

### 3. Moved All Toolchain Setup Before Build (Enhancement)

**Plan**: Didn't explicitly call out moving Node.js/Go/Rust/CMake setup  
**Implementation**: Moved all four toolchain setups before the build step

**Rationale**: Enables all binding tests to run during the build task without requiring separate test steps or shell gymnastics.

**Verdict**: ✅ **Smart enhancement**—simplifies workflow

## Observations

### Minor: Python Dependency Step Still Uses Bash on All Platforms

**Location**: Line 116-118

```yaml
- name: Install Python build dependencies
  run: python3 -m pip install --upgrade setuptools wheel
  shell: bash
```

This step still uses `shell: bash` on all platforms (no `if: runner.os` conditional). This is probably fine since it's just pip and doesn't involve native linking, but could be split if it causes issues on Windows in the future.

**Impact**: Low—pip operations typically don't have PATH conflicts  
**Action**: Monitor in CI; split only if it fails

### Excellent: Gradle Tasks Already Handle Platform-Specific Shells

Each individual test task in `native-lib/build.gradle` already handles Windows/Unix shell selection internally via `System.getProperty('os.name')` conditionals. This means the workflow fix (using `shell: cmd` on Windows) works in harmony with the existing Gradle logic.

## Test Strategy

### Completed
- ✅ YAML syntax validation (Ruby parser)
- ✅ Static analysis of Gradle task dependencies
- ✅ Code review for correctness, security, and quality

### Pending (CI Execution Required)
- ⏳ Windows build with all five binding tests
- ⏳ Ubuntu build with all five binding tests
- ⏳ Regression tests (2.9.8, 2.10)
- ⏳ Artifact packaging (distro, Python wheel, Node package, Go module, Rust crate, C library)

The workflow changes can only be fully validated by running on actual GitHub Actions runners. The implementation is correct based on static analysis, but CI execution is the ultimate verification.

## Acceptance Criteria

### Task 1: Fix Main Build Step ✅
- ✅ Windows build step uses `.\gradlew.bat` with `shell: cmd`
- ✅ Unix build step uses `./gradlew` with `shell: bash`
- ✅ Both steps have appropriate `if: runner.os ==` conditionals
- ✅ Gradle task name and flags are consistent (except `-PskipNodeTests=true` intentionally removed)

### Task 2-3: Remove Redundant Test Steps ✅
- ✅ Removed all five individual test steps (Python, Node.js, Go, Rust, C)
- ✅ Tests run once via `build -> test` dependency chain
- ✅ Verified `native-lib/build.gradle` task dependencies are correct

### Task 5: Add Documentation ✅
- ✅ Comments explain Windows shell requirement
- ✅ Comments document test execution flow
- ✅ Inline documentation is clear and accurate

## Verdict

**Status**: ✅ **APPROVED**

The implementation correctly fixes the Windows Rust linker PATH conflict and makes several smart enhancements:
1. Splits all Gradle steps into OS-specific variants for consistency
2. Moves toolchain setup before build to simplify test execution
3. Removes `-PskipNodeTests=true` to enable Node test coverage
4. Adds comprehensive documentation

All deviations from the plan are intentional improvements that enhance the fix. No critical issues found.

### Recommended Next Steps

1. **Merge the PR** after CI passes
2. **Monitor CI execution** on both Windows and Ubuntu runners
3. **Watch for Python pip step** on Windows—split to OS-specific if it fails
4. **Consider follow-up**: Apply the same OS-specific pattern to other workflows if they exist

## Files Modified

- `.github/workflows/main.yml` (96 insertions, 61 deletions)

## Related Documents

- Implementation Plan: `docs/plans/2026-07-03-fix-ci-native-bindings.md`
- GitHub Actions Run: https://github.com/mulesoft/data-weave-cli/actions/runs/28675742037/
- Pull Request: https://github.com/mulesoft/data-weave-cli/pull/120
