#!/bin/bash
# verify-build.sh - Verify all native bindings can build and run

set -e  # Exit on error

echo "========================================="
echo "DataWeave Native Bindings Build Verification"
echo "========================================="
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Track results
PASSED=0
FAILED=0
SKIPPED=0

# Function to print status
print_status() {
  local status=$1
  local message=$2

  if [ "$status" == "PASS" ]; then
    echo -e "${GREEN}✅ PASS${NC}: $message"
    ((PASSED++))
  elif [ "$status" == "FAIL" ]; then
    echo -e "${RED}❌ FAIL${NC}: $message"
    ((FAILED++))
  elif [ "$status" == "SKIP" ]; then
    echo -e "${YELLOW}⏭️  SKIP${NC}: $message"
    ((SKIPPED++))
  fi
}

# Get project root
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "Project root: $PROJECT_ROOT"
echo ""

# Set Java to GraalVM if available via SDKMAN
if [ -d "$HOME/.sdkman/candidates/java/24.0.2-graal" ]; then
  export JAVA_HOME="$HOME/.sdkman/candidates/java/24.0.2-graal"
  export PATH="$JAVA_HOME/bin:$PATH"
  echo "Using GraalVM 24.0.2 from SDKMAN"
elif [ -d "$HOME/.sdkman/candidates/java" ]; then
  # Find any GraalVM 24.x version
  for dir in $HOME/.sdkman/candidates/java/24.*-graal; do
    if [ -d "$dir" ]; then
      export JAVA_HOME="$dir"
      export PATH="$JAVA_HOME/bin:$PATH"
      echo "Using GraalVM from SDKMAN: $(basename $dir)"
      break
    fi
  done
fi
echo ""

# Step 1: Check prerequisites
echo "=== Step 1: Checking Prerequisites ==="
echo ""

# Java/GraalVM
if command -v java &> /dev/null; then
  java_version=$(java -version 2>&1 | head -3 | tr '\n' ' ')
  if echo "$java_version" | grep -qi "graalvm\|oracle graalvm"; then
    print_status "PASS" "Java/GraalVM found: $(echo "$java_version" | grep -o 'version "[^"]*"' | head -1)"
  else
    print_status "FAIL" "Java found but not GraalVM: $java_version"
  fi
else
  print_status "FAIL" "Java not found"
fi

# Gradle
if [ -f "./gradlew" ]; then
  print_status "PASS" "Gradle wrapper found"
else
  print_status "FAIL" "Gradle wrapper not found"
fi

# Python
if command -v python3 &> /dev/null; then
  python_version=$(python3 --version 2>&1)
  print_status "PASS" "Python found: $python_version"
else
  print_status "FAIL" "Python3 not found"
fi

# Node.js
if command -v node &> /dev/null; then
  node_version=$(node --version 2>&1)
  print_status "PASS" "Node.js found: $node_version"
else
  print_status "FAIL" "Node.js not found"
fi

# Go
if command -v go &> /dev/null; then
  go_version=$(go version 2>&1)
  print_status "PASS" "Go found: $go_version"
else
  print_status "SKIP" "Go not found (optional)"
fi

# Rust
if command -v rustc &> /dev/null; then
  rust_version=$(rustc --version 2>&1)
  print_status "PASS" "Rust found: $rust_version"
else
  print_status "SKIP" "Rust not found (optional)"
fi

# CMake
if command -v cmake &> /dev/null; then
  cmake_version=$(cmake --version 2>&1 | head -1)
  print_status "PASS" "CMake found: $cmake_version"
else
  print_status "SKIP" "CMake not found (optional)"
fi

# C compiler
if command -v gcc &> /dev/null; then
  print_status "PASS" "GCC found"
elif command -v clang &> /dev/null; then
  print_status "PASS" "Clang found"
else
  print_status "SKIP" "C compiler not found (optional for Go/C bindings)"
fi

echo ""

# Step 2: Build native library
echo "=== Step 2: Building Native Library ==="
echo ""

if [ -f "native-lib/build/native/nativeCompile/dwlib.dylib" ] || \
   [ -f "native-lib/build/native/nativeCompile/dwlib.so" ] || \
   [ -f "native-lib/build/native/nativeCompile/dwlib.dll" ]; then
  print_status "PASS" "Native library already built"
  NATIVE_LIB_EXISTS=true
else
  echo "Building native library (this may take 5-10 minutes)..."
  if ./gradlew :native-lib:nativeCompile --no-daemon > /tmp/native-build.log 2>&1; then
    print_status "PASS" "Native library build succeeded"
    NATIVE_LIB_EXISTS=true
  else
    print_status "FAIL" "Native library build failed. See /tmp/native-build.log"
    NATIVE_LIB_EXISTS=false
  fi
fi

echo ""

# Set library path
if [ "$(uname)" == "Darwin" ]; then
  export DYLD_LIBRARY_PATH="$PROJECT_ROOT/native-lib/build/native/nativeCompile:$DYLD_LIBRARY_PATH"
  LIB_EXT="dylib"
elif [ "$(uname)" == "Linux" ]; then
  export LD_LIBRARY_PATH="$PROJECT_ROOT/native-lib/build/native/nativeCompile:$LD_LIBRARY_PATH"
  LIB_EXT="so"
else
  export PATH="$PROJECT_ROOT/native-lib/build/native/nativeCompile:$PATH"
  LIB_EXT="dll"
fi

echo "Library path: $(uname): $DYLD_LIBRARY_PATH$LD_LIBRARY_PATH"
echo ""

# Step 3: Test Python binding
echo "=== Step 3: Testing Python Binding ==="
echo ""

if [ "$NATIVE_LIB_EXISTS" == true ]; then
  if ./gradlew :native-lib:pythonTest --no-daemon > /tmp/python-test.log 2>&1; then
    print_status "PASS" "Python tests passed"
  else
    print_status "FAIL" "Python tests failed. See /tmp/python-test.log"
  fi
else
  print_status "SKIP" "Python tests (native library not built)"
fi

echo ""

# Step 4: Test Node.js binding
echo "=== Step 4: Testing Node.js Binding ==="
echo ""

if [ "$NATIVE_LIB_EXISTS" == true ]; then
  if ./gradlew :native-lib:nodeTest --no-daemon > /tmp/node-test.log 2>&1; then
    print_status "PASS" "Node.js tests passed"
  else
    print_status "FAIL" "Node.js tests failed. See /tmp/node-test.log"
  fi
else
  print_status "SKIP" "Node.js tests (native library not built)"
fi

echo ""

# Step 5: Test Go binding
echo "=== Step 5: Testing Go Binding ==="
echo ""

if command -v go &> /dev/null && [ "$NATIVE_LIB_EXISTS" == true ]; then
  # Go needs the library in the library path at link time
  cd "$PROJECT_ROOT/native-lib/go"

  # CGO needs to find the library
  export CGO_LDFLAGS="-L$PROJECT_ROOT/native-lib/build/native/nativeCompile -ldwlib"

  if go test -v > /tmp/go-test.log 2>&1; then
    print_status "PASS" "Go tests passed"
  else
    print_status "FAIL" "Go tests failed. See /tmp/go-test.log"
  fi
  cd "$PROJECT_ROOT"
else
  print_status "SKIP" "Go tests (Go not installed or native library not built)"
fi

echo ""

# Step 6: Test Rust binding
echo "=== Step 6: Testing Rust Binding ==="
echo ""

if command -v cargo &> /dev/null && [ "$NATIVE_LIB_EXISTS" == true ]; then
  cd "$PROJECT_ROOT/native-lib/rust"

  if cargo test > /tmp/rust-test.log 2>&1; then
    print_status "PASS" "Rust tests passed"
  else
    print_status "FAIL" "Rust tests failed. See /tmp/rust-test.log"
  fi
  cd "$PROJECT_ROOT"
else
  print_status "SKIP" "Rust tests (Rust not installed or native library not built)"
fi

echo ""

# Step 7: Test C binding
echo "=== Step 7: Testing C Binding ==="
echo ""

if command -v cmake &> /dev/null && [ "$NATIVE_LIB_EXISTS" == true ]; then
  if ./gradlew :native-lib:cTest --no-daemon > /tmp/c-test.log 2>&1; then
    print_status "PASS" "C tests passed"
  else
    print_status "FAIL" "C tests failed. See /tmp/c-test.log"
  fi
else
  print_status "SKIP" "C tests (CMake not installed or native library not built)"
fi

echo ""

# Summary
echo "========================================="
echo "Summary"
echo "========================================="
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"
echo -e "${YELLOW}Skipped: $SKIPPED${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
  echo -e "${GREEN}✅ All tests passed!${NC}"
  echo ""
  echo "Next steps:"
  echo "  - Run demos: cd native-lib/demos && ./run-demos.sh"
  echo "  - Build packages: ./gradlew :native-lib:packageAllBindings"
  echo "  - Read guide: docs/BUILDING-AND-RUNNING-BINDINGS.md"
  exit 0
else
  echo -e "${RED}❌ Some tests failed${NC}"
  echo ""
  echo "Troubleshooting:"
  echo "  - Check log files in /tmp/*-test.log"
  echo "  - Read guide: docs/TROUBLESHOOTING-BUILD.md"
  echo "  - Install missing dependencies (see guide)"
  exit 1
fi
