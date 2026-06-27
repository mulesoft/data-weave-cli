# DataWeave Native Bindings - Demo Makefile
#
# Quick commands to build and run all comprehensive demos

# Detect OS
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    LIB_EXT := dylib
    LIB_PATH_VAR := DYLD_LIBRARY_PATH
else ifeq ($(UNAME_S),Linux)
    LIB_EXT := so
    LIB_PATH_VAR := LD_LIBRARY_PATH
else
    LIB_EXT := dll
    LIB_PATH_VAR := PATH
endif

# Paths
NATIVE_LIB_DIR := ../build/native/nativeCompile
PYTHON_SRC := ../python/src
RUST_DIR := ../rust
GO_DIR := ../go
C_DIR := ../c

# Build native library first
.PHONY: build-native
build-native:
	@echo "Building native library..."
	cd .. && ./gradlew nativeCompile
	@echo "✓ Native library built: $(NATIVE_LIB_DIR)/dwlib.$(LIB_EXT)"

# Python demo
.PHONY: python
python: build-native
	@echo "\n========== Running Python Demo =========="
	PYTHONPATH=$(PYTHON_SRC):$$PYTHONPATH \
	$(LIB_PATH_VAR)=$(NATIVE_LIB_DIR):$$$(LIB_PATH_VAR) \
	python3 python_comprehensive_demo.py

# Rust demo
.PHONY: rust
rust: build-native
	@echo "\n========== Running Rust Demo =========="
	cd $(RUST_DIR) && \
	DATAWEAVE_NATIVE_LIB=$(NATIVE_LIB_DIR) \
	$(LIB_PATH_VAR)=$(NATIVE_LIB_DIR):$$$(LIB_PATH_VAR) \
	cargo run --example comprehensive_demo 2>/dev/null || \
	cargo build --example comprehensive_demo && \
	$(LIB_PATH_VAR)=$(NATIVE_LIB_DIR):$$$(LIB_PATH_VAR) \
	../rust/target/debug/examples/comprehensive_demo

# Go demo
.PHONY: go
go: build-native
	@echo "\n========== Running Go Demo =========="
	cd $(GO_DIR) && \
	CGO_ENABLED=1 \
	CGO_LDFLAGS="-L$(NATIVE_LIB_DIR) -ldwlib" \
	$(LIB_PATH_VAR)=$(NATIVE_LIB_DIR):$$$(LIB_PATH_VAR) \
	go run ../demos/go_comprehensive_demo.go

# C demo
.PHONY: c
c: build-native c_demo
	@echo "\n========== Running C Demo =========="
	$(LIB_PATH_VAR)=$(NATIVE_LIB_DIR):$$$(LIB_PATH_VAR) ./c_demo

c_demo: c_comprehensive_demo.c
	@echo "Compiling C demo..."
	gcc -I$(C_DIR)/include -L$(NATIVE_LIB_DIR) \
	    -o c_demo c_comprehensive_demo.c \
	    -ldw -ldwlib -Wl,-rpath,$(NATIVE_LIB_DIR)
	@echo "✓ C demo compiled: ./c_demo"

# Run all demos
.PHONY: all
all: python rust go c
	@echo "\n=========================================="
	@echo "✓ All demos completed successfully!"
	@echo "=========================================="

# Quick test (just Python, fastest)
.PHONY: quick
quick: python

# Clean build artifacts
.PHONY: clean
clean:
	@echo "Cleaning demo artifacts..."
	rm -f c_demo
	rm -f *.o
	cd $(RUST_DIR) && cargo clean
	cd $(GO_DIR) && go clean
	@echo "✓ Clean complete"

# Help
.PHONY: help
help:
	@echo "DataWeave Native Bindings - Demo Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  make build-native  - Build the GraalVM native library"
	@echo "  make python        - Run Python comprehensive demo"
	@echo "  make rust          - Run Rust comprehensive demo"
	@echo "  make go            - Run Go comprehensive demo"
	@echo "  make c             - Build and run C comprehensive demo"
	@echo "  make all           - Run all demos (default)"
	@echo "  make quick         - Run Python demo only (fastest)"
	@echo "  make clean         - Clean build artifacts"
	@echo "  make help          - Show this help"
	@echo ""
	@echo "Prerequisites:"
	@echo "  - GraalVM native-image (for build-native)"
	@echo "  - Python 3.7+ (for python)"
	@echo "  - Cargo 1.70+ (for rust)"
	@echo "  - Go 1.21+ with CGO (for go)"
	@echo "  - GCC or Clang (for c)"
	@echo ""
	@echo "Examples:"
	@echo "  make               # Run all demos"
	@echo "  make python        # Just Python"
	@echo "  make rust go       # Rust and Go only"

# Default target
.DEFAULT_GOAL := all
