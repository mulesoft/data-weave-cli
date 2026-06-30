# Changelog

All notable changes to the DataWeave CLI and Native Library Bindings will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive native library bindings for five languages:
  - **Python** - Full FFI binding with type hints and streaming support
  - **Node.js** - N-API binding with TypeScript definitions
  - **Go** - CGo binding with idiomatic Go interfaces
  - **Rust** - Safe FFI binding with comprehensive error handling
  - **C** - Direct C API with header documentation
- Streaming API support across all bindings
- Bidirectional streaming (input + output) for large data transformations
- Comprehensive test suites for all language bindings
- CI/CD automation for all bindings (Linux, Windows)
- **Release artifacts for all bindings**:
  - Python: Wheel packages (`.whl`)
  - Node.js: NPM tarballs (`.tgz`)
  - Go: Module tarballs (`.tar.gz`)
  - Rust: Crate packages (`.crate`)
  - C: Library + header tarballs (`.tar.gz`)
- Comprehensive documentation and examples for each binding
- Production-ready error handling and thread safety
- Gradle packaging tasks: `packageGo`, `packageRust`, `packageC`, `packageAllBindings`

### Changed
- Unified versioning across all native bindings (now v1.0.0)
- Improved CI test coverage to include all language bindings
- Enhanced build system with Gradle tasks for all bindings
- Updated release workflow to package and upload all binding artifacts

### Fixed
- Native library loading on various platforms
- Memory management in FFI boundaries
- Race conditions in concurrent usage scenarios

## [1.0.0] - 2026-07-15

First production release of the DataWeave Native Library with multi-language bindings.

### Features

#### Native Library Core
- GraalVM Native Image-based shared library (dwlib)
- Three execution modes:
  - **Buffered**: Complete in-memory execution
  - **Streaming**: Output streaming for large results
  - **Bidirectional**: Input and output streaming
- JSON, XML, CSV, YAML format support
- Cross-platform support (Linux, macOS, Windows)
- Thread-safe execution

#### Language Bindings

**Python (v1.0.0)**
- `pip install dataweave-native`
- Type hints and docstrings
- Pytest-compatible test suite
- PyPI-ready packaging

**Node.js (v1.0.0)**
- `npm install @dataweave/native`
- TypeScript definitions included
- N-API for stability across Node versions
- Async generator-based streaming API

**Go (v1.0.0)**
- `go get github.com/mulesoft-labs/data-weave-cli/native-lib/go/dataweave`
- Idiomatic Go interfaces
- Race detector validation
- Full concurrency support

**Rust (v1.0.0)**
- `cargo add dataweave`
- Safe FFI with comprehensive error types
- `thiserror`-based error handling
- Zero-cost abstractions over C API

**C (v1.0.0)**
- Direct C99 API
- CMake and Makefile build systems
- Comprehensive header documentation
- SONAME versioning

#### Documentation
- Language-specific READMEs with quickstarts
- API reference documentation
- Comprehensive examples and demos
- Troubleshooting guides
- Architecture and FFI contract documentation

#### CI/CD
- Automated builds on Linux, Windows, macOS
- Multi-version testing matrices
- Automated release artifact generation
- Integration test suites

### Known Limitations
- macOS arm64 CI coverage pending runner availability
- No official package registry publishing (PyPI/npm/crates.io) - artifacts available via GitHub Releases
- DataWeave runtime version pinned to 2.12.0

### Platform Support
- **Linux**: x86_64 (glibc 2.17+)
- **macOS**: x86_64, arm64 (macOS 11+)
- **Windows**: x86_64 (Windows Server 2022+)

### Dependencies
- GraalVM 24.0.2
- DataWeave Runtime 2.12.0

## [0.1.0] - 2026-06-15 (Pre-release)

### Added
- Initial native library implementation
- Python binding prototype
- Basic CLI functionality
- Core DataWeave execution engine

### Known Issues
- Limited platform testing
- No streaming support
- Single-threaded execution only

---

## Version History

### Native Bindings Versioning
Starting with v1.0.0, all language bindings share a unified version number defined in `gradle.properties`:
- `nativeBindingsVersion=1.0.0`

Version increments follow semantic versioning:
- **MAJOR** (X.0.0): Breaking API changes, ABI incompatibility
- **MINOR** (1.X.0): New features, backward-compatible additions
- **PATCH** (1.0.X): Bug fixes, no API changes

### Release Process
1. Update `nativeBindingsVersion` in `gradle.properties`
2. Update this CHANGELOG with release notes
3. Tag release: `git tag v1.0.0`
4. Push tag: `git push origin v1.0.0`
5. CI automatically builds and attaches release artifacts
6. Publish release notes on GitHub

---

## Links
- [GitHub Repository](https://github.com/mulesoft-labs/data-weave-cli)
- [Issue Tracker](https://github.com/mulesoft-labs/data-weave-cli/issues)
- [Security Policy](SECURITY.md)
- [Contributing Guide](CONTRIBUTING.md)
