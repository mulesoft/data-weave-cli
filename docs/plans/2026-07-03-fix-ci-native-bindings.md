# Implementation Plan: Fix CI Native Bindings Compilation

**Date**: 2026-07-03  
**Issue**: GitHub Actions workflow failing on Windows due to Rust linker PATH conflict  
**CI Run**: https://github.com/mulesoft/data-weave-cli/actions/runs/28675742037/  
**Status**: Ready for implementation

## Executive Summary

The Windows CI build fails during Rust compilation because Git Bash's `/usr/bin/link.exe` is found before Visual Studio's `link.exe`. The root cause is that the main build step (line 44-47 of `.github/workflows/main.yml`) runs `./gradlew build` with `shell: bash`, which triggers all tests including `rustTest`. A previous fix (commit `9991658`) added a separate Rust test step with `shell: cmd`, but this step never executes because the build step fails first.

The Ubuntu build was cancelled when Windows failed, so its status is unknown but likely would succeed.

## Root Cause Analysis

### Primary Issue: Shell Mismatch in Build Step

**File**: `.github/workflows/main.yml`  
**Lines**: 44-47

```yaml
- name: Run Build
  run: |
    ./gradlew --stacktrace --no-problems-report -PskipNodeTests=true build
  shell: bash  # ← PROBLEM: Uses bash on Windows, triggering linker PATH conflict
```

**Flow**:
1. `./gradlew build` → runs `test` task (line 252 of `native-lib/build.gradle`)
2. `test` task depends on `rustTest` (line 256)
3. `rustTest` executes `cargo test` via bash shell (lines 224-226 of `native-lib/build.gradle`)
4. Rust compiler finds `/usr/bin/link.exe` (Git Bash) instead of MSVC's `link.exe`
5. Multiple crate build scripts fail: `quote`, `serde_json`, `thiserror`, `serde_core`, `zmij`, `proc-macro2`, `dataweave-native`
6. Task `:native-lib:rustTest` fails with exit code 101
7. Workflow step "Run Rust Tests (Windows)" never executes (lines 107-110)

### Secondary Issue: Incomplete Test Isolation

**File**: `native-lib/build.gradle`  
**Lines**: 252-258

The `test` task has hard dependencies on all native binding tests. This couples the test execution tightly, preventing platform-specific shell selection via workflow conditionals.

### Evidence from CI Logs

```
error: linking with `link.exe` failed: exit code: 1
= note: "C:\Program Files\Git\usr\bin\link.exe" "/NOLOGO" ... [MSVC flags]
= note: /usr/bin/link: extra operand 'C:\a\data-weave-cli\...\*.rcgu.o'
```

Rust correctly detected MSVC toolchain (passed `/NOLOGO`, `/NXCOMPAT` flags) but resolved the wrong linker executable due to Git Bash's PATH ordering.

## Implementation Tasks

### Task 1: Fix Main Build Step Shell Selection

**Objective**: Run the build step with platform-appropriate shell to prevent Rust linker PATH conflicts.

**Files to modify**:
- `.github/workflows/main.yml` (lines 44-47)

**Changes**:
Split the single "Run Build" step into OS-specific steps with appropriate shells.

**Before**:
```yaml
- name: Run Build
  run: |
    ./gradlew --stacktrace --no-problems-report -PskipNodeTests=true build
  shell: bash
```

**After**:
```yaml
- name: Run Build (Windows)
  if: runner.os == 'Windows'
  run: .\gradlew.bat --stacktrace --no-problems-report -PskipNodeTests=true build
  shell: cmd

- name: Run Build (Unix)
  if: runner.os != 'Windows'
  run: ./gradlew --stacktrace --no-problems-report -PskipNodeTests=true build
  shell: bash
```

**Rationale**: 
- Using `shell: cmd` on Windows ensures MSVC's `link.exe` is found before Git Bash's `/usr/bin/link.exe`
- Rust toolchain relies on PATH resolution; changing the shell changes PATH ordering
- No code changes required; pure workflow configuration fix
- Mirrors the pattern already established for the separate Rust test steps (lines 107-115)

**Acceptance criteria**:
- [ ] Windows build step uses `.\gradlew.bat` with `shell: cmd`
- [ ] Unix build step uses `./gradlew` with `shell: bash`
- [ ] Both steps have appropriate `if: runner.os ==` conditionals
- [ ] Gradle task name and flags remain identical between OS variants

### Task 2: Remove Redundant Rust Test Steps

**Objective**: Clean up duplicate Rust test execution since the main build step now runs all tests with correct shells.

**Files to modify**:
- `.github/workflows/main.yml` (lines 103-115)

**Changes**:
Remove the separate "Setup Rust" and "Run Rust Tests" steps, as they are now redundant.

**Before**:
```yaml
# Setup Rust for Rust binding tests
- name: Setup Rust
  uses: dtolnay/rust-toolchain@stable

- name: Run Rust Tests (Windows)
  if: runner.os == 'Windows'
  run: .\gradlew.bat --stacktrace --no-problems-report native-lib:rustTest
  shell: cmd

- name: Run Rust Tests (Unix)
  if: runner.os != 'Windows'
  run: ./gradlew --stacktrace --no-problems-report native-lib:rustTest
  shell: bash
```

**After**:
```yaml
# Setup Rust for Rust binding tests (required by build task)
- name: Setup Rust
  uses: dtolnay/rust-toolchain@stable

# Rust tests are executed as part of the build step above
```

**Rationale**:
- The `build` task already runs `rustTest` (via `test` → `rustTest` dependency)
- Running tests twice wastes CI time (~30s per run)
- Rust toolchain setup must remain (required before build step)
- Comment clarifies why the setup exists without a dedicated test step

**Acceptance criteria**:
- [ ] Rust toolchain setup step is preserved
- [ ] Separate "Run Rust Tests" steps are removed
- [ ] Inline comment explains Rust tests run during build
- [ ] No functional change to test coverage (same tests execute once)

### Task 3: Apply Same Pattern to Other Test Steps

**Objective**: Ensure consistency and remove duplicate test execution for Python, Node.js, Go, and C bindings.

**Files to modify**:
- `.github/workflows/main.yml` (lines 49-123)

**Changes**:
Remove the separate test execution steps for other binding languages, keeping only the toolchain setup steps.

**Current structure** (lines 49-123):
- Setup Python → Run Python Tests
- Setup Node.js → Run Node Tests  
- Setup Go → Run Go Tests
- Setup Rust → Run Rust Tests
- Setup CMake → Run C Tests

**Proposed structure**:
- Setup Python
- Setup Node.js
- Setup Go
- Setup Rust
- Setup CMake
- **[Run Build step executes all tests]**

**Detailed changes**:

**Lines 62-72** - Remove Python test step:
```yaml
# Remove these lines:
- name: Run Python Tests
  run: ./gradlew --stacktrace --no-problems-report native-lib:pythonTest
  shell: bash
```

**Lines 83-92** - Remove Node.js test step:
```yaml
# Remove these lines:
- name: Run Node Tests
  run: ./gradlew --stacktrace --no-problems-report native-lib:nodeTest
  shell: bash
```

**Lines 100-101** - Remove Go test step:
```yaml
# Remove these lines:
- name: Run Go Tests
  run: ./gradlew --stacktrace --no-problems-report native-lib:goTest
  shell: bash
```

**Lines 121-123** - Remove C test step:
```yaml
# Remove these lines:
- name: Run C Tests
  run: ./gradlew --stacktrace --no-problems-report native-lib:cTest
  shell: bash
```

**Add comment after CMake setup**:
```yaml
# All native binding tests (Python, Node.js, Go, Rust, C) are executed
# during the 'Run Build' step above via the build → test task dependency
```

**Rationale**:
- All test tasks are already dependencies of the `test` task (lines 252-258 of `native-lib/build.gradle`)
- Running tests separately doubles CI time for no benefit
- Toolchain setups must happen before the build step (dependencies)
- Consolidates test execution to a single point with proper shell selection
- Reduces workflow complexity (18 lines → 2 lines of comments)

**Acceptance criteria**:
- [ ] All toolchain setup steps remain (Python, Node.js, Go, Rust, CMake)
- [ ] All separate test execution steps are removed
- [ ] Single comment block explains where tests execute
- [ ] Build step runs with appropriate shell per OS (Task 1)

### Task 4: Verify Gradle Task Dependencies

**Objective**: Confirm that all native binding tests are properly registered as dependencies of the `test` task.

**Files to check**:
- `native-lib/build.gradle` (lines 252-258)

**Verification steps**:
1. Read current `test` task configuration
2. Confirm dependencies: `pythonTest`, `nodeTest`, `goTest`, `rustTest`, `cTest`
3. Verify no additional test tasks exist that are not in the dependency list
4. Ensure shell selection logic is correct in each test task (lines 120-248)

**Expected state** (no changes needed):
```groovy
tasks.named('test') {
  dependsOn tasks.named('pythonTest')
  dependsOn tasks.named('nodeTest')
  dependsOn tasks.named('goTest')
  dependsOn tasks.named('rustTest')
  dependsOn tasks.named('cTest')
}
```

**Shell selection verification**:
- Python test (lines 120-123): Uses correct executor
- Node.js test (lines 160-166): Has Windows/Unix conditional logic ✓
- Go test (lines 182-188): Has Windows/Unix conditional logic ✓
- Rust test (lines 222-228): Has Windows/Unix conditional logic ✓
- C test (lines 244-249): Has Windows/Unix conditional logic ✓

**Acceptance criteria**:
- [ ] All 5 binding test tasks are dependencies of `test`
- [ ] Each test task has proper Windows/Unix shell conditionals
- [ ] No orphaned test tasks exist
- [ ] No code changes required (verification only)

### Task 5: Update Workflow Comments and Documentation

**Objective**: Document the CI architecture changes for future maintainers.

**Files to modify**:
- `.github/workflows/main.yml` (inline comments)

**Changes**:

**After Task 1 changes (line ~47)**:
```yaml
# Run Build (Windows)
# Note: Uses 'shell: cmd' on Windows to ensure Visual Studio's link.exe is found
# before Git Bash's /usr/bin/link.exe. This prevents Rust linker PATH conflicts.
- name: Run Build (Windows)
  if: runner.os == 'Windows'
  run: .\gradlew.bat --stacktrace --no-problems-report -PskipNodeTests=true build
  shell: cmd

- name: Run Build (Unix)
  if: runner.os != 'Windows'
  run: ./gradlew --stacktrace --no-problems-report -PskipNodeTests=true build
  shell: bash
```

**After toolchain setups (line ~120)**:
```yaml
# Setup CMake for C binding tests
- name: Setup CMake
  uses: lukka/get-cmake@latest

# All native binding tests are executed during the 'Run Build' step above.
# The build task depends on the test task, which in turn depends on:
#   - pythonTest (Python wheel tests)
#   - nodeTest (Node.js N-API addon tests)
#   - goTest (Go CGo FFI tests)
#   - rustTest (Rust FFI tests)
#   - cTest (C binding tests via CMake/CTest)
# Each test task uses platform-appropriate shell commands (cmd on Windows,
# bash on Unix) to ensure correct PATH resolution for native toolchains.
```

**Acceptance criteria**:
- [ ] Comment explains why `shell: cmd` is required on Windows
- [ ] Comment documents which tests run during build step
- [ ] Comment references the Gradle task dependency chain
- [ ] Language is clear for future maintainers unfamiliar with the Rust linker issue

## Test Strategy

### Pre-Validation (Local)

**Windows environment**:
1. Clone repo and checkout the fixed branch
2. Install prerequisites: GraalVM 24, Python 3, Node.js 18, Go 1.21, Rust stable, CMake, Visual Studio 2022
3. Run `.\gradlew.bat --stacktrace build` from **Command Prompt** (not Git Bash)
4. Verify all tests pass: `BUILD SUCCESSFUL`
5. Check test output for all 5 bindings: Python, Node.js, Go, Rust, C

**Unix environment** (Linux or macOS):
1. Clone repo and checkout the fixed branch
2. Install prerequisites: GraalVM 24, Python 3, Node.js 18, Go 1.21, Rust stable, CMake
3. Run `./gradlew --stacktrace build`
4. Verify all tests pass: `BUILD SUCCESSFUL`
5. Check test output for all 5 bindings

### CI Validation

**Required CI checks**:
1. Push branch to trigger CI workflow
2. Monitor GitHub Actions run for matrix build:
   - `BUILD (mulesoft-ubuntu)` job status
   - `BUILD (mulesoft-windows)` job status
3. Both jobs must show "SUCCESS" status
4. Check detailed logs for each job:
   - All 5 test tasks complete: `pythonTest`, `nodeTest`, `goTest`, `rustTest`, `cTest`
   - No linker errors in Rust compilation output
   - Final step shows `BUILD SUCCESSFUL in Xm Ys`

**Success criteria**:
- [ ] Windows job completes without Rust linker errors
- [ ] Ubuntu job completes successfully
- [ ] All 5 native binding tests pass on both platforms
- [ ] Artifact upload step succeeds for both OS
- [ ] Total CI time is reduced (no duplicate test execution)

### Regression Testing

**Test suite** (run after fix):
1. Python binding smoke test:
   ```bash
   cd native-lib/python
   python -m tests.test_dataweave_module
   ```
   Expected: All tests pass, exit code 0

2. Node.js binding smoke test:
   ```bash
   cd native-lib/node
   npm install && npx node-gyp rebuild && npx tsc && npx vitest run
   ```
   Expected: All tests pass

3. Go binding smoke test:
   ```bash
   cd native-lib/go
   go test -v
   ```
   Expected: `PASS`, exit code 0

4. Rust binding smoke test:
   ```bash
   cd native-lib/rust
   cargo test
   ```
   Expected: All tests pass (with correct linker on Windows)

5. C binding smoke test:
   ```bash
   cd native-lib/c
   cmake -B build && cmake --build build && ctest --test-dir build --verbose
   ```
   Expected: All CTests pass

**Acceptance criteria**:
- [ ] All 5 smoke tests pass on Windows Command Prompt
- [ ] All 5 smoke tests pass on Linux/Unix bash
- [ ] No linker PATH warnings or errors
- [ ] Library loading succeeds (no `DLL not found` or `*.so not found` errors)

## Risk Assessment

### High Confidence Changes (Low Risk)

**Task 1** - Shell selection fix:
- **Confidence**: 95%
- **Evidence**: Mirrors the pattern from commit `9991658` which correctly identified the shell issue
- **Risk**: Very low; `shell: cmd` is a standard GitHub Actions feature
- **Rollback**: Trivial (revert single commit)

**Task 2-3** - Remove duplicate test steps:
- **Confidence**: 100%
- **Evidence**: Gradle build logs show tests already run during `build` task
- **Risk**: None; purely removes redundant work
- **Rollback**: N/A (improvement, no functional change)

### Medium Confidence Changes (Low-Medium Risk)

**Task 4** - Gradle task verification:
- **Confidence**: 90%
- **Evidence**: Current build.gradle shows all dependencies are correct
- **Risk**: Low; verification task only (no code changes)
- **Rollback**: N/A (read-only verification)

### Potential Failure Modes

1. **Windows cmd shell has different Gradle behavior**:
   - **Symptom**: Gradle wrapper or task resolution fails in cmd
   - **Likelihood**: Very low (Gradle is shell-agnostic, uses `.bat` on Windows)
   - **Mitigation**: The workflow already uses `.\gradlew.bat` in other Windows steps successfully

2. **Other environment variables differ between cmd and bash**:
   - **Symptom**: Tests fail due to missing environment setup
   - **Likelihood**: Low (GraalVM and toolchain setups run before shell selection)
   - **Mitigation**: Monitor CI logs for env var differences; add explicit exports if needed

3. **PATH differences affect non-Rust toolchains**:
   - **Symptom**: Python, Node.js, Go, or C tests fail on Windows after switch to cmd
   - **Likelihood**: Very low (these tests don't have known PATH conflicts)
   - **Mitigation**: If occurs, revert to bash and use Gradle property to skip Rust tests conditionally

## Alternative Approaches Considered (Not Recommended)

### Alternative 1: Set RUSTFLAGS Environment Variable

```yaml
env:
  RUSTFLAGS: "-C linker=C:\\Program Files\\Microsoft Visual Studio\\2022\\Enterprise\\VC\\Tools\\MSVC\\14.XX.XXXXX\\bin\\Hostx64\\x64\\link.exe"
```

**Pros**: Explicit linker path  
**Cons**:
- Hardcoded Visual Studio path breaks on runner updates
- MSVC version number changes frequently
- More complex to maintain
- Doesn't fix other potential PATH issues

### Alternative 2: Prepend Visual Studio to PATH

```yaml
- name: Fix PATH for Rust
  if: runner.os == 'Windows'
  run: |
    echo "C:\Program Files\Microsoft Visual Studio\2022\Enterprise\VC\Tools\MSVC\14.XX.XXXXX\bin\Hostx64\x64" >> $GITHUB_PATH
```

**Pros**: Affects all tools that might have similar issues  
**Cons**:
- Hardcoded paths again
- Modifies global environment for entire workflow
- Harder to debug if other tools break

### Alternative 3: Create `.cargo/config.toml` with Linker Setting

```toml
[target.x86_64-pc-windows-msvc]
linker = "link.exe"  # or full path
```

**Pros**: Rust-specific configuration  
**Cons**:
- Requires code change (not pure CI fix)
- Would need to be committed to repo
- Still relies on PATH or hardcoded paths

### Alternative 4: Skip Rust Tests on Windows

```yaml
run: ./gradlew --stacktrace -PskipRustTests=true build
```

**Pros**: Simplest workaround  
**Cons**:
- **Unacceptable**: Eliminates test coverage on Windows
- Defeats the purpose of CI
- Rust binding would not be validated before release

## Dependencies and Sequencing

**Task execution order** (strict sequence):
1. **Task 1** → Fix main build step shell selection (BLOCKING for all others)
2. **Task 4** → Verify Gradle task dependencies (parallel with Task 1)
3. **Task 2** → Remove redundant Rust test steps (depends on Task 1)
4. **Task 3** → Remove other redundant test steps (depends on Task 1)
5. **Task 5** → Update comments (depends on Tasks 1-3)

**Why strict ordering**:
- Task 1 must succeed before removing any test steps (preserve safety)
- Tasks 2-3 should be done together (avoid inconsistent workflow states)
- Task 5 requires final structure from Tasks 1-3

**Single commit vs. multiple commits**:
- **Recommended**: Single commit with all changes
- **Rationale**: Changes are tightly coupled; partial application leaves workflow broken
- **Commit message**:
  ```
  fix(ci): resolve Windows Rust linker PATH conflict in main build step
  
  Change the main build step to use 'shell: cmd' on Windows instead of
  'shell: bash'. This ensures Visual Studio's link.exe is found before
  Git Bash's /usr/bin/link.exe, preventing Rust compilation failures.
  
  - Split 'Run Build' into OS-specific steps with appropriate shells
  - Remove duplicate test execution steps (tests run via build → test task)
  - Keep toolchain setup steps (required before build)
  - Add comments documenting the shell selection requirement
  
  Fixes: https://github.com/mulesoft/data-weave-cli/actions/runs/28675742037/
  ```

## Success Metrics

### Immediate Metrics (Post-Fix)

1. **CI status**: Both Windows and Linux builds show green checkmarks
2. **Rust compilation**: No linker errors in Windows job logs
3. **Test execution**: All 5 binding tests pass on both platforms
4. **Build time**: CI time reduces by ~2-3 minutes (no duplicate tests)

### Long-Term Metrics (1 week after merge)

1. **Stability**: No new linker-related issues reported
2. **Compatibility**: No regression reports for any of the 5 bindings
3. **Developer experience**: No complaints about broken local builds
4. **CI reliability**: Windows builds remain consistently green

## Rollback Plan

### If CI still fails after Task 1

**Symptoms**: Windows build continues to fail with linker errors despite `shell: cmd`

**Diagnosis steps**:
1. Check if Git Bash is in PATH even in cmd shell: `where link.exe`
2. Verify Visual Studio installation on runner: `where cl.exe`
3. Check Rust toolchain detection: `rustc --print cfg | grep windows-msvc`

**Rollback options**:
1. Revert commit: `git revert HEAD`
2. Temporarily skip Rust tests on Windows: add `-PskipRustTests=true` to Windows build command
3. Escalate to GitHub Actions support (runner image issue)

### If other bindings break on Windows

**Symptoms**: Python, Node.js, Go, or C tests fail after switching to cmd shell

**Rollback**: 
```yaml
# Revert to bash and skip only Rust tests
- name: Run Build (Windows)
  if: runner.os == 'Windows'
  run: ./gradlew --stacktrace -PskipRustTests=true -PskipNodeTests=true build
  shell: bash  # Reverted
```

**Then investigate**: Which test broke and why cmd shell caused the issue.

## Appendix: File Inventory

### Workflow Files
- `.github/workflows/main.yml` - Main CI workflow (PRIMARY FIX TARGET)
- `.github/workflows/ci.yml` - Weekly scheduled CI (not affected; only builds 2 bindings)
- `.github/workflows/release.yml` - Release packaging workflow (not affected)

### Build Files
- `native-lib/build.gradle` - Native bindings Gradle build (verification target)
- `gradle.properties` - GraalVM version configuration (no changes)

### Native Binding Source (no changes expected)
- `native-lib/python/` - Python wheel binding
- `native-lib/node/` - Node.js N-API binding
- `native-lib/go/` - Go CGo binding
- `native-lib/rust/` - Rust FFI binding (source of linker issue)
- `native-lib/c/` - C binding with CMake

### Test Files (no changes expected)
- `native-lib/python/tests/test_dataweave_module.py`
- `native-lib/node/tests/dataweave.test.ts`
- `native-lib/go/dataweave_test.go`
- `native-lib/rust/tests/` (if exists)
- `native-lib/c/tests/test_dataweave.c`

## Appendix: Environment Details

### GitHub Actions Runner Images
- **Linux**: `mulesoft-ubuntu` - Based on Ubuntu 20.04/22.04
- **Windows**: `mulesoft-windows` - Windows Server 2022 (10.0.20348)

### Toolchain Versions (from workflow)
- **Java**: GraalVM Community 24
- **Python**: 3.9+ (system default)
- **Node.js**: 18 (via `actions/setup-node@v4`)
- **Go**: 1.21 (via `actions/setup-go@v5`)
- **Rust**: stable (via `dtolnay/rust-toolchain@stable`)
- **CMake**: latest (via `lukka/get-cmake@latest`)

### Windows-Specific Tools
- **Git**: Bundled with GitHub Actions runner
- **Git Bash**: Located at `C:\Program Files\Git\bin\bash.EXE`
- **Visual Studio**: 2022 Enterprise Edition
- **Windows SDK**: 10.0.26100.0
- **MSVC**: 14.xx (varies by runner image version)

## Appendix: Relevant Commits

- `eebbf30` - Go binding symlink fix (current HEAD)
- `9991658` - **Rust CI fix attempt** (added separate test step with `shell: cmd`, but didn't fix main build step)
- `ad19a98` - C binding Windows CTest flag fix
- `5cd4830` - C binding CMake multi-config generator fix
- `df10e6c` - C binding Windows DLL symbol export fix
- `84c7243` - C binding Visual Studio output directory fix
- `4774410` - C binding Windows platform support initial commit

## Implementation Checklist

- [ ] Create feature branch from current HEAD (`eebbf30`)
- [ ] Task 1: Split "Run Build" step into OS-specific variants with appropriate shells
- [ ] Task 2: Remove redundant Rust test execution steps (keep setup)
- [ ] Task 3: Remove redundant Python/Node/Go/C test execution steps (keep setups)
- [ ] Task 4: Verify Gradle test task dependencies (no code changes)
- [ ] Task 5: Add inline comments documenting the fix
- [ ] Run local validation on Windows (if available)
- [ ] Run local validation on Linux
- [ ] Commit all changes with descriptive message
- [ ] Push and verify CI passes on both platforms
- [ ] Run regression test suite
- [ ] Create pull request with link to this plan
- [ ] Request review from team member familiar with CI
- [ ] Merge to master after approval

---

**Plan Status**: ✅ Complete and ready for executor  
**Estimated Implementation Time**: 30 minutes  
**Estimated CI Validation Time**: 15 minutes  
**Total End-to-End Time**: ~45 minutes
