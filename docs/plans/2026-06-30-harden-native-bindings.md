# Native Bindings Production Hardening Plan

**Date**: 2026-06-30  
**Status**: Planning  
**Branch**: `feat/harden-native-bindings-production` (from `feat/native-bindings-merged` + Node.js merge)  
**Objective**: Harden all five native language bindings (Python, Node.js, Go, Rust, C) for external consumer testing and integration.

---

## Executive Summary

The DataWeave native library project has **five language bindings** at varying levels of production readiness:

- **Python**: 95% ready — excellent docs, tests, CI coverage; needs versioning alignment
- **Node.js**: 85% ready — complete implementation, tests; **missing README**, needs CI integration
- **Go**: 90% ready — solid docs, tests, examples; **no CI coverage**, missing release automation
- **Rust**: 85% ready — excellent API, docs, tests; **no CI coverage**, missing release automation
- **C**: 90% ready — comprehensive docs, tests; **no CI coverage**, missing release automation

**Critical gaps**:
1. Only Python has CI test coverage (Go/Rust/C/Node.js tests exist but don't run in CI)
2. Only Python has release artifacts (wheels) — no artifacts for other bindings
3. No macOS or arm64 testing in CI (Linux/Windows x86_64 only)
4. Node.js has no README despite complete implementation
5. Version numbers are inconsistent (Python 0.0.1, Rust 0.1.0, no version for Go/C)
6. Repo-level OSS hygiene gaps: no CONTRIBUTING.md, no PR template, no CHANGELOG, no CI badges

---

## Current State by Binding

### Python (`native-lib/python/`)

**Strengths**:
- ✅ Comprehensive 479-line README (install, API reference, examples, troubleshooting)
- ✅ Build system: `pyproject.toml` + `setup.py` with platform-specific wheel tagging
- ✅ Examples: `simple_demo.py` (6 examples), `streaming_demo.py` (8 examples)
- ✅ Tests: 16 test cases covering all API modes (buffered, streaming, bidirectional)
- ✅ CI: GitHub Actions builds wheels on Linux/Windows, uploads artifacts
- ✅ Type hints and comprehensive docstrings
- ✅ Native library integration via Gradle `stagePythonNativeLib` task

**Gaps**:
- ⚠️ Version hardcoded at `0.0.1` (no sync mechanism with other bindings)
- ⚠️ CI doesn't test multiple Python versions (3.9, 3.10, 3.11, 3.12)
- ⚠️ Custom test harness instead of pytest (works but non-standard)
- ⚠️ No `mypy --strict` validation for type hints

### Node.js (`native-lib/node/`)

**Strengths**:
- ✅ Complete N-API C addon implementation (avoids koffi SIGSEGV issue)
- ✅ TypeScript sources with full type definitions
- ✅ Build system: `binding.gyp` + `node-gyp` + TypeScript compiler
- ✅ Tests: Vitest with 225 lines covering all API modes
- ✅ Gradle tasks: `stageNodeNativeLib`, `buildNodePackage`, `nodeTest`
- ✅ Package: `@dataweave/native` v0.0.1, declares Node >=18, OS support
- ✅ CI configured in workflows (sets up Node 18, builds package, uploads .tgz)

**Gaps**:
- ❌ **No README.md** — critical documentation missing (Python/Go/Rust/C all have READMEs)
- ⚠️ CI may not actually run Node.js tests (need to verify `nodeTest` task execution)
- ⚠️ No standalone examples directory (tests serve as examples)
- ⚠️ No multi-Node-version testing (18, 20, 22)

### Go (`native-lib/go/`)

**Strengths**:
- ✅ Comprehensive 239-line README (prerequisites, install, API reference, error handling)
- ✅ Build system: `go.mod` (Go 1.21), CGO with platform-specific LDFLAGS
- ✅ Examples: `simple_demo.go`, `streaming_demo.go`
- ✅ Tests: 12 test cases including concurrent execution (20 goroutines), race detector
- ✅ Gradle tasks: `goTest`, `goTestRace` (race detector enabled)
- ✅ API parity with all other bindings

**Gaps**:
- ❌ **No CI coverage** — tests exist but never run in GitHub Actions
- ❌ **No release artifacts** — no Go module tarballs, no tagged releases
- ⚠️ No explicit versioning in `go.mod` (no semantic version tags)
- ⚠️ No Go version matrix testing (1.21+)
- ⚠️ No evidence of publishing to Go module proxy

### Rust (`native-lib/rust/`)

**Strengths**:
- ✅ Production-grade 237-line README (install, API modes, error handling, threading)
- ✅ Build system: `Cargo.toml` (edition 2021), `build.rs` with rpath config
- ✅ Examples: `simple_demo.rs`, `streaming_demo.rs`, comprehensive 300+ line demo
- ✅ Tests: 318-line integration test with 13 test functions, 20-thread stress test
- ✅ API documentation: module-level rustdoc, function docs, safety notes for unsafe code
- ✅ Error handling: idiomatic `thiserror` with 9 error types
- ✅ Version: 0.1.0 in `Cargo.toml`

**Gaps**:
- ❌ **No CI coverage** — zero mentions of Rust/cargo in GitHub Actions workflows
- ❌ **No release artifacts** — no `.crate` packages produced
- ⚠️ Tests not verified to pass (Cargo not in PATH during audit)
- ⚠️ Platform matrix unclear (macOS/Linux rpath, but Windows support uncertain)
- ⚠️ No docs.rs configuration (may fail to build on docs.rs without native lib)

### C (`native-lib/c/`)

**Strengths**:
- ✅ Exceptional 503-line README (installation, API reference, examples, troubleshooting)
- ✅ Dual build system: CMake + Makefile (both fully functional)
- ✅ Outputs: static library (.a), shared library (.dylib/.so), headers
- ✅ Examples: `simple.c`, `streaming.c`
- ✅ Tests: 461-line comprehensive test suite (10+ test functions, CTest integrated)
- ✅ SONAME versioning: VERSION=0.1.0, SOVERSION=0
- ✅ Header quality: single comprehensive header with full API docs, thread safety notes

**Gaps**:
- ❌ **No CI coverage** — C binding completely absent from GitHub Actions workflows
- ❌ **No release artifacts** — no library or header uploads
- ⚠️ No multi-platform testing in CI (CMake supports Windows/Linux/macOS but not tested)
- ⚠️ No ABI stability guarantees or compatibility policy documented

---

## Repository-Level Gaps

### Open Source Hygiene

**Present**:
- ✅ LICENSE.txt (BSD 3-Clause)
- ✅ CODE_OF_CONDUCT.md (Salesforce OSS / Contributor Covenant)
- ✅ SECURITY.md (security@salesforce.com)
- ✅ Issue templates (bug_report.md, feature_request.md)

**Missing**:
- ❌ **NOTICE file** — no third-party attribution file
- ❌ **CONTRIBUTING.md** — no formal contribution guide (PR process, coding standards, testing)
- ❌ **PR template** — no `.github/pull_request_template.md`
- ❌ **CHANGELOG.md** — no release notes or version history
- ❌ **CI badges** — README has no build status, test coverage, or version badges
- ❌ **Binding navigation** — main README doesn't link to any of the five language binding READMEs

### CI/Release Infrastructure

**Current coverage**:
- ✅ GitHub Actions workflows: `ci.yml`, `main.yml`, `release.yml`
- ✅ Native library (dwlib) built with GraalVM 24 Native Image
- ✅ Python wheel built and uploaded
- ✅ Native CLI integration tests run

**Critical gaps**:
- ❌ **No macOS runners** — only Linux (mulesoft-ubuntu) and Windows (mulesoft-windows)
- ❌ **No arm64 testing** — only x86_64 architectures
- ❌ **Go/Rust/C tests never run** — Gradle tasks exist but workflows don't call `native-lib:test`
- ❌ **Node.js tests uncertain** — CI configures Node but unclear if `nodeTest` runs
- ❌ **Release artifacts incomplete** — only CLI zip uploaded, not wheels/crates/tarballs
- ❌ **Version inconsistency** — hardcoded `100.100.100` in CI, `0.0.1`/`0.1.0` in bindings

---

## Implementation Tasks

### Task 1: Node.js — Write comprehensive README

**Priority**: High (blocking external integration)  
**Effort**: 4 hours  
**Owner**: TBD

**Description**: Node.js binding is fully implemented and tested but has no README. External consumers cannot discover or use the binding without documentation.

**Files to create**:
- `native-lib/node/README.md`

**Content sections** (match Python/Go/Rust/C structure):
1. Overview (what is this binding, what does it do)
2. Prerequisites (Node.js >= 18, platform support)
3. Installation (npm install, build from source)
4. Quick Start (basic example)
5. API Reference (buffered, streaming, bidirectional modes)
6. Examples (link to `tests/` as examples)
7. Error Handling
8. Threading Model (N-API worker threads)
9. Platform Support (darwin/linux/win32)
10. Troubleshooting (common build issues, native lib not found)
11. Development (how to build, run tests)

**Acceptance criteria**:
- [ ] README exists at `native-lib/node/README.md`
- [ ] Length comparable to other bindings (200-300 lines)
- [ ] All API modes documented with code examples
- [ ] Installation instructions work on Linux, macOS, Windows
- [ ] Linked from main `native-lib/README.md`

**Dependencies**: None

**Risk**: Low (pure documentation, no code changes)

---

### Task 2: Add comprehensive CI test coverage for all bindings

**Priority**: Critical (production readiness blocker)  
**Effort**: 8 hours  
**Owner**: TBD

**Description**: Currently only Python tests run in CI. Go, Rust, C, and Node.js have test suites but they're never executed, meaning regressions can go undetected.

**Files to modify**:
- `.github/workflows/main.yml`
- `.github/workflows/ci.yml`

**Changes**:
1. Add explicit test job after native library build:
   ```yaml
   - name: Test all language bindings
     run: ./gradlew native-lib:test
   ```
2. Verify Gradle `native-lib:test` task chains to:
   - `pythonTest`
   - `nodeTest`
   - `goTest` + `goTestRace`
   - `rustTest`
   - (C tests via CMake CTest)
3. Add separate test reporting for each language (JUnit XML, test summaries)
4. Upload test results as artifacts

**Acceptance criteria**:
- [ ] All five binding test suites run in CI on every PR/push
- [ ] Test failures block PR merge
- [ ] Test results viewable in GitHub Actions UI
- [ ] Test execution time < 5 minutes total

**Dependencies**: Native library build must complete first

**Risk**: Medium (may uncover latent test failures on CI environment)

---

### Task 3: Add macOS and arm64 CI runners

**Priority**: High (platform coverage gap)  
**Effort**: 6 hours  
**Owner**: TBD

**Description**: CI only tests on Linux/Windows x86_64. macOS (dominant DataWeave dev platform) and arm64 (M1/M2 Macs, AWS Graviton) are untested.

**Files to modify**:
- `.github/workflows/main.yml`
- `.github/workflows/ci.yml`
- `.github/workflows/release.yml`

**Changes**:
1. Add macOS runner to matrix:
   ```yaml
   strategy:
     matrix:
       os: [mulesoft-ubuntu, mulesoft-macos, mulesoft-windows]
   ```
2. Add architecture matrix if arm64 runners available:
   ```yaml
   strategy:
     matrix:
       include:
         - os: ubuntu-latest, arch: x86_64
         - os: ubuntu-latest, arch: aarch64
         - os: macos-latest, arch: arm64
         - os: macos-latest, arch: x86_64
   ```
3. Update Python wheel build to produce platform-specific wheels for all combos
4. Update Rust/Go builds to cross-compile for arm64

**Acceptance criteria**:
- [ ] CI runs on macOS (both x86_64 and arm64 if available)
- [ ] Python wheels produced for all platforms (manylinux, macosx-x86_64, macosx-arm64)
- [ ] Go/Rust binaries tested on arm64
- [ ] C library built and tested on macOS

**Dependencies**: Availability of mulesoft-macos runner (may need infra team coordination)

**Risk**: High (runner availability, cross-compilation complexity, platform-specific bugs)

---

### Task 4: Establish unified versioning strategy

**Priority**: Medium (release blocker)  
**Effort**: 3 hours  
**Owner**: TBD

**Description**: Bindings have inconsistent versions (Python 0.0.1, Rust 0.1.0, Go/C no version). Need single source of truth.

**Files to modify**:
- `gradle.properties` (create if missing)
- `native-lib/python/setup.cfg`
- `native-lib/node/package.json`
- `native-lib/rust/Cargo.toml`
- `native-lib/go/go.mod`
- `native-lib/c/include/dataweave.h` (VERSION macros)
- `native-lib/c/CMakeLists.txt` (VERSION property)
- `native-lib/build.gradle` (read version from properties)

**Changes**:
1. Add to `gradle.properties`:
   ```properties
   nativeBindingsVersion=1.0.0
   ```
2. Update all binding metadata files to read from Gradle:
   - Python: `setup.cfg` version = read from Gradle task
   - Node: `package.json` version = templated by Gradle
   - Rust: `Cargo.toml` version = generated by Gradle task
   - Go: apply git tags `v1.0.0` automatically
   - C: header macros generated from properties
3. Update CI workflows to use single version variable
4. Document versioning policy in CONTRIBUTING.md

**Acceptance criteria**:
- [ ] All bindings report same version number
- [ ] Version bumps require single edit (gradle.properties)
- [ ] Git tags match binding versions
- [ ] Release notes reference unified version

**Dependencies**: None

**Risk**: Low (pure metadata changes, no code impact)

---

### Task 5: Create release artifacts for all bindings

**Priority**: High (distribution blocker)  
**Effort**: 6 hours  
**Owner**: TBD

**Description**: Only Python wheel is uploaded as release artifact. Go/Rust/C bindings need distributable packages.

**Files to modify**:
- `.github/workflows/release.yml`
- `native-lib/build.gradle` (add packaging tasks)

**Changes**:
1. **Python**: Already done (wheel upload exists)
2. **Node.js**: Package as tarball via `npm pack`, upload `dw-native-$VERSION-$OS.tgz`
3. **Go**: Create module tarball, upload `dw-go-$VERSION.tar.gz`
4. **Rust**: Build crate package via `cargo package`, upload `dataweave-$VERSION.crate`
5. **C**: Package library + headers, upload `libdataweave-$VERSION-$OS-$ARCH.tar.gz`

**Gradle tasks to add**:
```gradle
task packageNode(type: Exec) {
    commandLine 'npm', 'pack'
    workingDir 'node'
}

task packageGo(type: Tar) {
    from 'go'
    archiveFileName = "dw-go-${version}.tar.gz"
}

task packageRust(type: Exec) {
    commandLine 'cargo', 'package'
    workingDir 'rust'
}

task packageC(type: Tar) {
    from 'c/build/lib', 'c/include'
    archiveFileName = "libdataweave-${version}-${osName}-${osArch}.tar.gz"
}
```

**Acceptance criteria**:
- [ ] All five bindings have downloadable artifacts in GitHub Releases
- [ ] Artifacts named consistently: `{language}-{version}-{platform}.{ext}`
- [ ] Checksums (SHA256) provided for all artifacts
- [ ] Release notes list all artifact URLs

**Dependencies**: Task 4 (versioning)

**Risk**: Medium (Rust/C packaging may fail without local Cargo/CMake setup)

---

### Task 6: Add multi-version testing matrices

**Priority**: Medium (compatibility assurance)  
**Effort**: 4 hours  
**Owner**: TBD

**Description**: Bindings only tested with single language runtime version. Need compatibility matrix.

**Files to modify**:
- `.github/workflows/main.yml`

**Changes**:
1. **Python**: Test on 3.9, 3.10, 3.11, 3.12
   ```yaml
   strategy:
     matrix:
       python-version: ['3.9', '3.10', '3.11', '3.12']
   ```
2. **Node.js**: Test on 18, 20, 22 (LTS + current)
   ```yaml
   strategy:
     matrix:
       node-version: ['18', '20', '22']
   ```
3. **Go**: Test on 1.21, 1.22, 1.23
   ```yaml
   strategy:
     matrix:
       go-version: ['1.21', '1.22', '1.23']
   ```
4. **Rust**: Test on stable, beta, MSRV (1.70)
   ```yaml
   strategy:
     matrix:
       rust-version: ['1.70', 'stable', 'beta']
   ```

**Acceptance criteria**:
- [ ] Each binding tested on 3+ language versions
- [ ] MSRV (minimum supported runtime version) documented in each README
- [ ] CI matrix completes in < 15 minutes total

**Dependencies**: Task 2 (test coverage)

**Risk**: Low (may discover incompatibilities)

---

### Task 7: Improve Python binding for production

**Priority**: Low (nice-to-have improvements)  
**Effort**: 3 hours  
**Owner**: TBD

**Description**: Python binding is already excellent but has minor gaps.

**Files to modify**:
- `native-lib/python/tests/test_dataweave_module.py`
- `native-lib/python/setup.cfg`
- `native-lib/python/pyproject.toml`

**Changes**:
1. Migrate from custom test harness to pytest:
   ```python
   # Replace manual test runner with pytest
   import pytest
   
   def test_basic_execution():
       assert result.success
   ```
2. Add `mypy --strict` validation to CI
3. Add PyPI metadata (classifiers, keywords, project URLs)
4. Bump version to 1.0.0 (per Task 4)

**Acceptance criteria**:
- [ ] Tests run with `pytest`
- [ ] Type hints pass `mypy --strict`
- [ ] PyPI-ready metadata in setup.cfg
- [ ] README includes PyPI installation instructions

**Dependencies**: Task 4 (versioning)

**Risk**: Low (incremental improvements)

---

### Task 8: Add repository-level documentation

**Priority**: Medium (OSS hygiene)  
**Effort**: 4 hours  
**Owner**: TBD

**Description**: Add missing OSS community files and improve discoverability.

**Files to create**:
- `CONTRIBUTING.md`
- `.github/pull_request_template.md`
- `CHANGELOG.md`

**Files to modify**:
- `README.md` (add binding navigation, CI badges)

**Changes**:

1. **CONTRIBUTING.md**:
   ```markdown
   # Contributing to DataWeave CLI
   
   ## Development Workflow
   1. Fork the repository
   2. Create a feature branch
   3. Make changes, add tests
   4. Run `./gradlew build` to verify
   5. Submit PR with description
   
   ## Coding Standards
   - Python: PEP 8, type hints
   - Node.js: TypeScript, ESLint
   - Go: gofmt, golint
   - Rust: rustfmt, clippy
   - C: C99, clang-format
   
   ## Testing Requirements
   - All PRs must include tests
   - Run language-specific test suite
   - CI must pass before merge
   ```

2. **PR template**:
   ```markdown
   ## Description
   <!-- What does this PR do? -->
   
   ## Motivation
   <!-- Why is this change needed? -->
   
   ## Testing
   - [ ] Added tests
   - [ ] Ran `./gradlew build` locally
   - [ ] CI passes
   
   ## Checklist
   - [ ] Documentation updated
   - [ ] CHANGELOG.md updated
   ```

3. **CHANGELOG.md**:
   ```markdown
   # Changelog
   
   ## [Unreleased]
   
   ### Added
   - Native language bindings (Python, Node.js, Go, Rust, C)
   - Streaming API support
   - CI/CD automation
   
   ## [1.0.0] - 2026-07-15
   
   First production release of native bindings.
   ```

4. **README.md updates**:
   - Add CI badges at top:
     ```markdown
     ![Build Status](https://github.com/.../workflows/CI/badge.svg)
     ![Test Coverage](https://codecov.io/.../badge.svg)
     ```
   - Add "Language Bindings" section:
     ```markdown
     ## Language Bindings
     
     Native DataWeave runtime available in:
     - [Python](native-lib/python/README.md)
     - [Node.js](native-lib/node/README.md)
     - [Go](native-lib/go/README.md)
     - [Rust](native-lib/rust/README.md)
     - [C](native-lib/c/README.md)
     
     See [API Quick Reference](native-lib/demos/API_QUICK_REFERENCE.md).
     ```

**Acceptance criteria**:
- [ ] CONTRIBUTING.md with PR workflow, coding standards, testing requirements
- [ ] PR template with description/testing checklist
- [ ] CHANGELOG.md with version history
- [ ] README links to all binding READMEs
- [ ] CI status badges visible in README

**Dependencies**: None

**Risk**: None (pure documentation)

---

### Task 9: Add comprehensive demos and examples

**Priority**: Low (nice-to-have)  
**Effort**: 4 hours  
**Owner**: TBD

**Description**: While each binding has examples, a unified set of cross-language demos would help external consumers compare bindings.

**Files to create**:
- `native-lib/demos/README.md` (navigation hub)
- `native-lib/demos/python_comprehensive_demo.py` (if missing)
- `native-lib/demos/nodejs_comprehensive_demo.js`
- `native-lib/demos/go_comprehensive_demo.go`
- `native-lib/demos/c_comprehensive_demo.c`

**Files to verify**:
- `native-lib/demos/rust_comprehensive_demo.rs` (already exists per audit)

**Content**: Each demo should showcase:
1. Basic execution (arithmetic, string manipulation)
2. JSON transformation
3. XML/CSV parsing
4. Input variables
5. Output streaming
6. Bidirectional streaming
7. Error handling
8. Performance (large dataset)

**Acceptance criteria**:
- [ ] All five languages have comprehensive demos
- [ ] Demos are runnable with single command
- [ ] Demos produce identical output across languages
- [ ] demos/README.md explains what each demo does

**Dependencies**: Task 1 (Node.js README)

**Risk**: Low (examples already exist in binding-specific directories)

---

### Task 10: Security and ABI stability documentation

**Priority**: Low (future-proofing)  
**Effort**: 2 hours  
**Owner**: TBD

**Description**: Document security model (sandboxing, resource limits) and ABI compatibility guarantees.

**Files to create/modify**:
- `native-lib/SECURITY.md`
- `native-lib/ABI_COMPATIBILITY.md`
- Each binding README (add Security section)

**Changes**:

1. **SECURITY.md**:
   ```markdown
   # Security Model
   
   ## Sandboxing
   - DataWeave scripts run in GraalVM isolate (separate memory space)
   - No native function access by default
   - Resource limits configurable
   
   ## Known Limitations
   - No filesystem access control (scripts can read/write files)
   - No network isolation (scripts can make HTTP requests)
   - Memory limits enforced by GraalVM, not OS
   
   ## Reporting Vulnerabilities
   Report to security@salesforce.com
   ```

2. **ABI_COMPATIBILITY.md**:
   ```markdown
   # ABI Compatibility Policy
   
   ## Versioning
   - MAJOR: Breaking ABI changes (recompile required)
   - MINOR: Backward-compatible additions
   - PATCH: Bug fixes, no ABI changes
   
   ## Guarantees
   - C API: stable within MAJOR version
   - Language bindings: semver (MAJOR.MINOR.PATCH)
   - SONAME: bumped only on breaking changes
   ```

**Acceptance criteria**:
- [ ] Security model documented
- [ ] ABI compatibility policy defined
- [ ] Each binding README links to security docs

**Dependencies**: None

**Risk**: None (documentation only)

---

## Task Dependency Graph

```
Task 4 (Versioning)
  ├─> Task 5 (Release artifacts)
  └─> Task 7 (Python improvements)

Task 2 (CI test coverage)
  └─> Task 6 (Multi-version matrices)

Task 1 (Node.js README)
  └─> Task 9 (Comprehensive demos)

Task 8 (Repo docs) — independent
Task 3 (macOS/arm64) — independent but blockedby runner availability
Task 10 (Security docs) — independent
```

**Critical path**: Task 4 → Task 5 (versioning → releases)  
**Recommended order**:
1. Task 1 (Node.js README) — high impact, low effort
2. Task 2 (CI tests) — critical for quality
3. Task 4 (Versioning) — blocks releases
4. Task 5 (Release artifacts) — enables distribution
5. Task 8 (Repo docs) — OSS hygiene
6. Task 3 (macOS/arm64) — platform coverage (if runners available)
7. Task 6 (Multi-version matrices) — compatibility
8. Task 7 (Python polish) — incremental improvement
9. Task 9 (Demos) — nice-to-have
10. Task 10 (Security docs) — future-proofing

---

## Risk Assessment

### High Risks

1. **macOS runner availability** (Task 3)
   - **Impact**: Cannot test or release macOS binaries
   - **Likelihood**: Medium (depends on infra team)
   - **Mitigation**: Test locally on macOS, request runner access early, consider GitHub-hosted macOS runners

2. **Latent test failures in CI** (Task 2)
   - **Impact**: May discover bugs requiring fixes before hardening completes
   - **Likelihood**: Medium (Go/Rust/C tests haven't run in CI)
   - **Mitigation**: Run tests locally first, fix failures incrementally, gate PR merge on green tests

3. **Cross-compilation for arm64** (Task 3)
   - **Impact**: May require significant build system changes
   - **Likelihood**: Medium (CGo/FFI complexity)
   - **Mitigation**: Start with native arm64 builds, add cross-compilation later if needed

### Medium Risks

1. **Rust/C packaging without Cargo/CMake** (Task 5)
   - **Impact**: Release workflow may fail without proper toolchain
   - **Likelihood**: Medium (CI environment differences)
   - **Mitigation**: Test packaging locally, ensure CI has required tools (cargo, cmake)

2. **Version synchronization complexity** (Task 4)
   - **Impact**: Bindings may get out of sync if Gradle templating fails
   - **Likelihood**: Low (straightforward Gradle string substitution)
   - **Mitigation**: Add CI check that all versions match, document manual fallback

### Low Risks

1. **Documentation tasks** (Tasks 1, 8, 10)
   - **Impact**: None (no code changes)
   - **Likelihood**: Negligible
   - **Mitigation**: None needed

2. **Python improvements** (Task 7)
   - **Impact**: Minor (incremental changes to stable binding)
   - **Likelihood**: Low (pytest/mypy well-understood)
   - **Mitigation**: Test locally before CI integration

---

## Acceptance Criteria (Plan-Level)

The production hardening effort is **complete** when:

- [ ] All five bindings have comprehensive READMEs
- [ ] All five bindings have passing tests in CI
- [ ] All five bindings produce release artifacts
- [ ] CI tests on Linux, macOS, Windows
- [ ] CI tests on x86_64 and arm64 (best-effort for arm64)
- [ ] Unified versioning across all bindings
- [ ] CONTRIBUTING.md, PR template, CHANGELOG exist
- [ ] Main README links to all binding READMEs
- [ ] CI badges visible in README
- [ ] At least one comprehensive demo per language
- [ ] No critical or high-severity security issues in native library
- [ ] External consumer can download artifacts and integrate within 1 hour

---

## Validation Plan

After implementation, validate production readiness:

1. **Fresh clone test**: Clone repo, follow each binding's README, verify builds work
2. **Artifact smoke test**: Download release artifacts, verify they work without repo
3. **Platform matrix validation**: Test on Ubuntu 22.04, macOS 13/14, Windows Server 2022
4. **Multi-version validation**: Test on min/max supported versions of each language runtime
5. **Integration test**: Build a small app in each language that uses the binding
6. **Performance baseline**: Run benchmarks on large DataWeave scripts (10MB+ JSON)
7. **CI regression check**: Introduce intentional test failure, verify CI catches it

---

## Timeline Estimate

**Total effort**: ~44 hours

Assuming 1 developer working full-time (8 hours/day):
- **Week 1** (Days 1-5): Tasks 1-4 (README, CI tests, versioning, macOS)
- **Week 2** (Days 6-8): Tasks 5-7 (Releases, matrices, Python polish)
- **Week 3** (Days 9-10): Tasks 8-10 (Docs, demos, security)

With 2 developers working in parallel:
- **Week 1** (Days 1-5): Developer A (Tasks 1, 2), Developer B (Tasks 3, 4)
- **Week 2** (Days 6-8): Developer A (Tasks 5, 6), Developer B (Tasks 7, 8)
- **Week 3** (Day 9): Both (Tasks 9, 10, validation)

**Recommended timeline**: 2 weeks with 2 developers (allows buffer for unexpected issues)

---

## Open Questions

1. **PyPI/npm/crates.io publishing**: Should bindings be published to public registries, or only distributed as GitHub Release artifacts?
2. **macOS runner availability**: Can we get mulesoft-macos runner, or should we use GitHub-hosted macOS runners?
3. **arm64 priority**: Is arm64 support required for v1.0.0, or can it be added in v1.1.0?
4. **DataWeave version pinning**: Should bindings lock to specific DataWeave runtime version, or support multiple?
5. **Breaking changes process**: How do we coordinate breaking changes across five bindings (monorepo workflow)?

---

## References

- **Audit findings**: Research conducted by 7 il-expert agents on 2026-06-30
- **Base branch**: `feat/native-bindings-merged` (merged with Node.js PR #115 from origin/master)
- **Related documents**:
  - `/Users/mcousido/repos/emu/data-weave-cli/native-lib/README.md`
  - `/Users/mcousido/repos/emu/data-weave-cli/native-lib/ARCHITECTURE.md`
  - `/Users/mcousido/repos/emu/data-weave-cli/native-lib/FFI_CONTRACT.md`
  - `/Users/mcousido/repos/emu/data-weave-cli/native-lib/LANGUAGE_WRAPPERS_SUMMARY.md`
  - `/Users/mcousido/repos/emu/data-weave-cli/native-lib/demos/API_QUICK_REFERENCE.md`
