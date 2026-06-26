.PHONY: all test build clean example install-deps build-native

# Default target
all: build test

# Install Go dependencies
install-deps:
	go mod download
	go mod tidy

# Build the native library (requires Gradle)
build-native:
	@echo "Building native library..."
	cd ../.. && ./gradlew nativeCompile
	@echo "Native library built at ../../build/native/nativeCompile/"

# Build Go module
build: install-deps
	@echo "Building Go module..."
	go build -v ./...

# Run tests
test: build
	@echo "Running tests..."
	go test -v -cover

# Run specific test
test-%:
	go test -v -run $*

# Build and run example
example: build
	@echo "Building example..."
	cd example && go build -o dataweave-example
	@echo "Running example..."
	cd example && ./dataweave-example

# Format code
fmt:
	go fmt ./...
	gofmt -s -w .

# Run linter
lint:
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed. Install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
		exit 1; \
	fi

# Run go vet
vet:
	go vet ./...

# Check for issues (fmt, vet, lint)
check: fmt vet
	@echo "All checks passed!"

# Clean build artifacts
clean:
	rm -f example/dataweave-example
	go clean -cache -testcache

# Show coverage in browser
coverage:
	go test -coverprofile=coverage.out
	go tool cover -html=coverage.out

# Full CI workflow (build native lib, build Go, run tests)
ci: build-native build test
	@echo "CI build completed successfully!"

# Help
help:
	@echo "Available targets:"
	@echo "  all          - Build and test (default)"
	@echo "  build-native - Build the native DataWeave library using Gradle"
	@echo "  build        - Build the Go module"
	@echo "  test         - Run all tests"
	@echo "  test-X       - Run specific test (e.g., make test-Basic)"
	@echo "  example      - Build and run example program"
	@echo "  fmt          - Format Go code"
	@echo "  lint         - Run linter (requires golangci-lint)"
	@echo "  vet          - Run go vet"
	@echo "  check        - Run fmt and vet"
	@echo "  coverage     - Generate and view test coverage"
	@echo "  clean        - Remove build artifacts"
	@echo "  ci           - Full CI build (native + Go + tests)"
	@echo "  help         - Show this help message"
