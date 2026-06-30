# Contributing to DataWeave CLI

Thank you for your interest in contributing to the DataWeave CLI and native library bindings! This document provides guidelines and instructions for contributing to this project.

## Code of Conduct

This project adheres to the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Getting Started

### Prerequisites

- **Java 24** (GraalVM recommended)
- **Gradle 8.x**
- **Git**

Language-specific requirements for native bindings:
- **Python**: Python 3.9+ (for Python bindings)
- **Node.js**: Node 18+ (for Node.js bindings)
- **Go**: Go 1.21+ (for Go bindings)
- **Rust**: Rust 1.70+ (for Rust bindings)
- **C**: CMake 3.20+, C99 compiler (for C bindings)

### Development Setup

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/data-weave-cli.git
   cd data-weave-cli
   ```
3. **Add upstream remote**:
   ```bash
   git remote add upstream https://github.com/mulesoft-labs/data-weave-cli.git
   ```
4. **Build the project**:
   ```bash
   ./gradlew build
   ```

## Development Workflow

### 1. Create a Feature Branch

Always create a new branch for your work:

```bash
git checkout -b feature/your-feature-name
```

Branch naming conventions:
- `feature/` - New features
- `fix/` - Bug fixes
- `docs/` - Documentation changes
- `refactor/` - Code refactoring
- `test/` - Test additions or fixes

### 2. Make Changes

Follow the coding standards for the language you're working in (see below).

### 3. Write Tests

All code changes should include corresponding tests:
- Add unit tests for new functionality
- Update existing tests if behavior changes
- Ensure all tests pass locally before pushing

### 4. Run Tests Locally

```bash
# Run all tests
./gradlew test

# Run specific binding tests
./gradlew native-lib:pythonTest
./gradlew native-lib:nodeTest
./gradlew native-lib:goTest
./gradlew native-lib:rustTest
./gradlew native-lib:cTest
```

### 5. Commit Your Changes

Write clear, descriptive commit messages:

```bash
git commit -m "feat: add streaming support for XML parsing

- Implement streaming XML reader
- Add tests for large XML files
- Update documentation"
```

Commit message format:
- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation only
- `test:` - Adding or updating tests
- `refactor:` - Code refactoring
- `perf:` - Performance improvements
- `chore:` - Build process or auxiliary tool changes

### 6. Push and Create Pull Request

```bash
git push origin feature/your-feature-name
```

Then create a Pull Request on GitHub using the provided template.

## Coding Standards

### Python (native-lib/python/)

- Follow **PEP 8** style guide
- Use **type hints** for all function signatures
- Maximum line length: 120 characters
- Use meaningful variable names
- Add docstrings to all public functions/classes

```python
def run(script: str, inputs: Optional[Dict[str, Any]] = None) -> ExecutionResult:
    """Execute a DataWeave script with optional inputs.
    
    Args:
        script: DataWeave script source code
        inputs: Optional dictionary of input variables
        
    Returns:
        ExecutionResult containing output or error information
    """
```

### Node.js/TypeScript (native-lib/node/)

- Follow **TypeScript** best practices
- Use **ESLint** for linting
- Use **Prettier** for formatting (if configured)
- Prefer `const` over `let`
- Use async/await over raw promises
- Export types alongside implementations

```typescript
export interface ExecutionResult {
  success: boolean;
  result: string | null;
  error: string | null;
  getString(): string | null;
}
```

### Go (native-lib/go/)

- Follow **Effective Go** guidelines
- Run `gofmt` before committing
- Run `go vet` to catch common issues
- Use meaningful package and variable names
- Add comments to exported functions

```go
// Run executes a DataWeave script with the given inputs and returns the result.
// Returns an error if the script fails to execute or compile.
func Run(script string, inputs map[string]interface{}) (*ExecutionResult, error) {
    // Implementation
}
```

### Rust (native-lib/rust/)

- Follow **Rust API Guidelines**
- Run `cargo fmt` before committing
- Run `cargo clippy` to catch common issues
- Use `rustdoc` comments for public items
- Handle errors idiomatically with `Result<T, E>`

```rust
/// Execute a DataWeave script with optional inputs.
///
/// # Arguments
/// * `script` - The DataWeave script source code
/// * `inputs` - Optional map of input variables
///
/// # Errors
/// Returns `DataWeaveError` if execution fails
pub fn run(script: &str, inputs: Option<HashMap<String, Value>>) -> Result<ExecutionResult> {
    // Implementation
}
```

### C (native-lib/c/)

- Follow **C99 standard**
- Use `clang-format` for formatting (if configured)
- Use meaningful variable names (not single letters except loop counters)
- Add documentation comments for all public functions
- Check return values and handle errors

```c
/**
 * Execute a DataWeave script with inputs.
 * 
 * @param script The DataWeave script source code (null-terminated)
 * @param inputs_json JSON string of input variables (null-terminated)
 * @return ExecutionResult struct containing output or error. Caller must free with dw_result_free().
 */
dw_result_t* dw_run(const char* script, const char* inputs_json);
```

## Testing Requirements

### All Pull Requests Must:

1. **Include tests** for new functionality
2. **Pass all existing tests**
3. **Maintain or improve code coverage** (where applicable)
4. **Include integration tests** for user-facing features

### Test Coverage by Language

- **Python**: Use built-in test runner or pytest
- **Node.js**: Use Vitest (configured in project)
- **Go**: Use `go test` with table-driven tests
- **Rust**: Use `cargo test` with #[test] annotations
- **C**: Use CMake/CTest framework

### Running CI Locally

Before pushing, ensure CI will pass:

```bash
# Full build (includes all tests)
./gradlew build

# Build native library
./gradlew native-lib:nativeCompile

# Run all binding tests
./gradlew native-lib:test
```

## Documentation

### When to Update Documentation

- Adding new features or APIs
- Changing existing behavior
- Fixing bugs that affect user-facing functionality
- Improving build or development processes

### Documentation Locations

- **README.md** - Project overview, quick start
- **native-lib/*/README.md** - Language-specific binding docs
- **docs/** - Detailed guides and references
- **CHANGELOG.md** - Version history and release notes
- **Code comments** - Inline documentation

## Pull Request Process

### Before Submitting

1. ✅ All tests pass locally
2. ✅ Code follows style guidelines
3. ✅ Documentation updated (if needed)
4. ✅ CHANGELOG.md updated (for user-facing changes)
5. ✅ Commits are clear and well-organized

### PR Checklist

When you open a PR, the template will include:

- [ ] Description of changes
- [ ] Motivation/reasoning
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] All CI checks pass

### Review Process

1. **Automated checks** run on all PRs (build, tests, linting)
2. **Maintainer review** - at least one maintainer approval required
3. **Address feedback** - make requested changes
4. **Merge** - maintainer will merge once approved

### After Merge

- Your changes will be included in the next release
- Release notes will reference your contribution
- Thank you! 🎉

## Reporting Issues

### Bug Reports

Include:
- **Description** - Clear description of the bug
- **Steps to reproduce** - Minimal example to reproduce
- **Expected behavior** - What should happen
- **Actual behavior** - What actually happens
- **Environment** - OS, language versions, DataWeave CLI version
- **Logs/errors** - Any error messages or stack traces

### Feature Requests

Include:
- **Description** - What feature you'd like to see
- **Use case** - Why this feature would be useful
- **Alternatives** - Other approaches you've considered
- **Examples** - Code examples showing desired usage (if applicable)

## Community

- **Issues** - [GitHub Issues](https://github.com/mulesoft-labs/data-weave-cli/issues)
- **Discussions** - [GitHub Discussions](https://github.com/mulesoft-labs/data-weave-cli/discussions)
- **Security** - Report security issues to [security@salesforce.com](mailto:security@salesforce.com)

## License

By contributing to this project, you agree that your contributions will be licensed under the [BSD 3-Clause License](LICENSE.txt).

## Questions?

If you have questions about contributing, feel free to:
- Open a [GitHub Discussion](https://github.com/mulesoft-labs/data-weave-cli/discussions)
- Ask in an existing issue
- Reach out to maintainers

Thank you for contributing! 🙏
