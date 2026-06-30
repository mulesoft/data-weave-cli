# Build Troubleshooting Guide

This guide helps you diagnose and fix common build issues for the DataWeave native library and all language bindings.

---

## Quick Diagnostic

Run this script to check all prerequisites:

```bash
#!/bin/bash
# save as: check-prerequisites.sh

echo "=== DataWeave Native Bindings Build Prerequisites ==="
echo ""

# Function to check command and version
check_command() {
  local cmd=$1
  local name=$2
  local min_version=$3
  
  if command -v $cmd &> /dev/null; then
    version=$($cmd --version 2>&1 | head -1)
    echo "✅ $name: $version"
    return 0
  else
    echo "❌ $name: NOT FOUND (required)"
    return 1
  fi
}

# Check Java/GraalVM
if command -v java &> /dev/null; then
  java_version=$(java -version 2>&1 | grep -i version | head -1)
  if echo "$java_version" | grep -qi "graalvm"; then
    echo "✅ Java: $java_version"
  else
    echo "⚠️  Java: $java_version (GraalVM recommended)"
  fi
else
  echo "❌ Java: NOT FOUND (required)"
fi

# Check Gradle
if [ -f "./gradlew" ]; then
  echo "✅ Gradle: Wrapper found"
else
  echo "❌ Gradle: gradlew not found"
fi

# Check language runtimes
check_command "python3" "Python" "3.9"
check_command "node" "Node.js" "18"
check_command "go" "Go" "1.21"
check_command "rustc" "Rust" "1.70"
check_command "cargo" "Cargo" "1.70"
check_command "cmake" "CMake" "3.20"

# Check compilers
check_command "gcc" "GCC" "" || check_command "clang" "Clang" ""

echo ""
echo "=== System Info ==="
echo "OS: $(uname -s) $(uname -m)"
echo "Shell: $SHELL"

echo ""
echo "=== Next Steps ==="
echo "Missing tools? See installation instructions below."
```

Run it:
```bash
chmod +x check-prerequisites.sh
./check-prerequisites.sh
```

---

## Common Issues and Fixes

### 1. CMake Not Found

**Error**:
```
> Task :native-lib:cTest FAILED
cmake: command not found
```

**Fix (macOS)**:
```bash
brew install cmake
```

**Fix (Linux)**:
```bash
# Ubuntu/Debian
sudo apt-get install cmake

# RHEL/CentOS/Fedora
sudo yum install cmake

# Or install from source
wget https://github.com/Kitware/CMake/releases/download/v3.28.0/cmake-3.28.0-linux-x86_64.sh
sudo sh cmake-3.28.0-linux-x86_64.sh --prefix=/usr/local --skip-license
```

**Fix (Windows)**:
```powershell
# Using Chocolatey
choco install cmake

# Or download installer from
# https://cmake.org/download/
```

**Verify**:
```bash
cmake --version
# Should show: cmake version 3.20+
```

---

### 2. Rust Not Found

**Error**:
```
> Task :native-lib:rustTest FAILED
cargo: command not found
```

**Fix (macOS)**:
```bash
brew install rust
```

**Fix (Linux/macOS via rustup - Recommended)**:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

**Fix (Windows)**:
```powershell
# Download and run from:
# https://www.rust-lang.org/tools/install
```

**Verify**:
```bash
rustc --version
cargo --version
# Should show: rustc 1.70+ and cargo 1.70+
```

---

### 3. Go Not Found

**Error**:
```
> Task :native-lib:goTest FAILED
go: command not found
```

**Fix (macOS)**:
```bash
brew install go
```

**Fix (Linux)**:
```bash
# Ubuntu/Debian
sudo apt-get install golang-go

# RHEL/CentOS/Fedora
sudo yum install golang

# Or download from https://go.dev/dl/
wget https://go.dev/dl/go1.21.5.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.21.5.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```

**Fix (Windows)**:
```powershell
choco install golang
```

**Verify**:
```bash
go version
# Should show: go version go1.21+
```

---

### 4. GraalVM Not Found or Wrong Version

**Error**:
```
> Task :native-lib:nativeCompile FAILED
Error: GraalVM 24.0.2 or later is required
```

**Fix (macOS)**:
```bash
# Using SDKMAN (recommended)
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install java 24.0.2-graal

# Or download manually from:
# https://www.graalvm.org/downloads/

# Set JAVA_HOME
export JAVA_HOME=$HOME/.sdkman/candidates/java/24.0.2-graal
export PATH=$JAVA_HOME/bin:$PATH
```

**Fix (Linux)**:
```bash
# Using SDKMAN
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install java 24.0.2-graal

# Or download manually
wget https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-24.0.2/graalvm-community-jdk-24.0.2_linux-x64_bin.tar.gz
tar -xzf graalvm-community-jdk-24.0.2_linux-x64_bin.tar.gz
export JAVA_HOME=$PWD/graalvm-community-openjdk-24.0.2
export PATH=$JAVA_HOME/bin:$PATH
```

**Fix (Windows)**:
```powershell
# Download from https://www.graalvm.org/downloads/
# Extract to C:\graalvm
# Set environment variables:
[System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\graalvm", "User")
[System.Environment]::SetEnvironmentVariable("PATH", "$env:PATH;C:\graalvm\bin", "User")
```

**Verify**:
```bash
java -version
# Should show: GraalVM Community JDK 24.0.2
```

---

### 5. Native Library Build Fails

**Error**:
```
> Task :native-lib:nativeCompile FAILED
Fatal error: Could not find symbol ...
```

**Diagnosis**:
```bash
# Check if build directory exists
ls -la native-lib/build/native/nativeCompile/

# Check Gradle daemon status
./gradlew --status

# Clean and rebuild
./gradlew clean
./gradlew :native-lib:nativeCompile --info
```

**Fix**:
```bash
# Option 1: Clean build
./gradlew clean
./gradlew :native-lib:nativeCompile

# Option 2: Kill Gradle daemon and retry
./gradlew --stop
./gradlew :native-lib:nativeCompile

# Option 3: Increase Gradle memory
export GRADLE_OPTS="-Xmx8g"
./gradlew :native-lib:nativeCompile
```

---

### 6. Python Tests Fail

**Error**:
```
> Task :native-lib:pythonTest FAILED
ModuleNotFoundError: No module named 'dataweave'
```

**Fix**:
```bash
# Make sure setuptools and wheel are installed
python3 -m pip install --upgrade setuptools wheel

# Build the wheel first
./gradlew :native-lib:buildPythonWheel

# Then run tests
./gradlew :native-lib:pythonTest
```

---

### 7. Node.js Tests Fail

**Error**:
```
> Task :native-lib:nodeTest FAILED
Error: Cannot find module '@dataweave/native'
```

**Fix**:
```bash
# Make sure Node.js 18+ is installed
node --version

# Install node-gyp globally
npm install -g node-gyp

# Build the package
./gradlew :native-lib:buildNodePackage

# Or manually in node directory
cd native-lib/node
npm install
npx node-gyp rebuild
npx tsc
```

---

### 8. Go Tests Fail (CGo Issues)

**Error**:
```
> Task :native-lib:goTest FAILED
cgo: C compiler not found
```

**Fix (macOS)**:
```bash
# Install Xcode Command Line Tools
xcode-select --install
```

**Fix (Linux)**:
```bash
# Ubuntu/Debian
sudo apt-get install build-essential

# RHEL/CentOS/Fedora
sudo yum groupinstall "Development Tools"
```

**Fix (Windows)**:
```powershell
# Install MinGW or Visual Studio
choco install mingw
```

**Verify**:
```bash
gcc --version
# or
clang --version
```

---

### 9. Library Path Issues (macOS/Linux)

**Error**:
```
dyld: Library not loaded: dwlib.dylib
  or
error while loading shared libraries: dwlib.so
```

**Fix (macOS)**:
```bash
export DYLD_LIBRARY_PATH=$(pwd)/native-lib/build/native/nativeCompile:$DYLD_LIBRARY_PATH
```

**Fix (Linux)**:
```bash
export LD_LIBRARY_PATH=$(pwd)/native-lib/build/native/nativeCompile:$LD_LIBRARY_PATH
```

**Fix (Windows)**:
```powershell
$env:PATH = "$(pwd)\native-lib\build\native\nativeCompile;$env:PATH"
```

**Permanent Fix**:
Add to your shell profile (`~/.zshrc`, `~/.bashrc`, etc.):
```bash
# For DataWeave CLI development
export DYLD_LIBRARY_PATH=/path/to/data-weave-cli/native-lib/build/native/nativeCompile:$DYLD_LIBRARY_PATH
export LD_LIBRARY_PATH=/path/to/data-weave-cli/native-lib/build/native/nativeCompile:$LD_LIBRARY_PATH
```

---

### 10. Memory Issues During Build

**Error**:
```
> Task :native-lib:nativeCompile FAILED
java.lang.OutOfMemoryError: Java heap space
```

**Fix**:
```bash
# Increase Gradle memory
export GRADLE_OPTS="-Xmx8g -XX:MaxMetaspaceSize=2g"
./gradlew :native-lib:nativeCompile

# Or add to gradle.properties
echo "org.gradle.jvmargs=-Xmx8g -XX:MaxMetaspaceSize=2g" >> gradle.properties
```

---

### 11. Build Times Out

**Error**:
```
> Task :native-lib:nativeCompile
... (hangs for >10 minutes)
```

**Fix**:
```bash
# Use quick build mode for development
./gradlew :native-lib:nativeCompile -Ob

# Or reduce optimization
# Edit native-lib/build.gradle and add:
# buildArgs.add('-Ob') // Quick build mode
```

---

## Complete Build Process

### Step 1: Install Prerequisites

```bash
# macOS
brew install cmake rust go
# Install GraalVM via SDKMAN
curl -s "https://get.sdkman.io" | bash
sdk install java 24.0.2-graal

# Linux (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install -y cmake build-essential golang-go
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# Install GraalVM via SDKMAN (same as macOS)
```

### Step 2: Verify Prerequisites

```bash
./check-prerequisites.sh
```

### Step 3: Build Native Library

```bash
# Clean build
./gradlew clean

# Build native library (takes 5-10 minutes first time)
./gradlew :native-lib:nativeCompile

# Verify output
ls -lh native-lib/build/native/nativeCompile/dwlib.*
```

### Step 4: Build All Bindings

```bash
# Python
./gradlew :native-lib:buildPythonWheel

# Node.js
./gradlew :native-lib:buildNodePackage

# Go (just needs native lib)
# No separate build needed

# Rust (just needs native lib)
# No separate build needed

# C (just needs native lib)
# No separate build needed
```

### Step 5: Run All Tests

```bash
# Set library path
export DYLD_LIBRARY_PATH=$(pwd)/native-lib/build/native/nativeCompile  # macOS
export LD_LIBRARY_PATH=$(pwd)/native-lib/build/native/nativeCompile    # Linux

# Run all tests
./gradlew :native-lib:test

# Or run individually
./gradlew :native-lib:pythonTest
./gradlew :native-lib:nodeTest
./gradlew :native-lib:goTest
./gradlew :native-lib:rustTest
./gradlew :native-lib:cTest
```

---

## Platform-Specific Notes

### macOS

- **Xcode Command Line Tools required** for C compiler (CGo, C binding)
- **Homebrew recommended** for package management
- **Library path**: Use `DYLD_LIBRARY_PATH`
- **Apple Silicon (M1/M2)**: Everything should work natively

### Linux

- **build-essential** or **Development Tools** group required
- **Library path**: Use `LD_LIBRARY_PATH`
- **RHEL 9**: Matches CI environment (Kilonova)

### Windows

- **MinGW or Visual Studio** required for C compiler
- **PowerShell recommended** over CMD
- **Library path**: Use `PATH` environment variable
- **Long path support**: May need to enable for deep node_modules

---

## CI Environment Matching

To match the CI environment locally:

```bash
# Using Docker (Linux only)
docker run --rm -it \
  -v $(pwd):/workspace \
  -w /workspace \
  ubuntu:22.04 \
  bash -c "
    apt-get update && \
    apt-get install -y cmake build-essential golang-go python3 python3-pip curl && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    source \$HOME/.cargo/env && \
    curl -s 'https://get.sdkman.io' | bash && \
    source \$HOME/.sdkman/bin/sdkman-init.sh && \
    sdk install java 24.0.2-graal && \
    ./gradlew clean && \
    ./gradlew :native-lib:nativeCompile && \
    ./gradlew :native-lib:test
  "
```

---

## Incremental Builds

After initial build, subsequent builds are much faster:

```bash
# Only rebuild native library
./gradlew :native-lib:nativeCompile

# Only rebuild specific binding
./gradlew :native-lib:buildPythonWheel
./gradlew :native-lib:buildNodePackage

# Only run specific tests
./gradlew :native-lib:goTest
./gradlew :native-lib:rustTest
```

---

## Build Performance Tips

1. **Use Gradle daemon** (enabled by default)
   ```bash
   # Check daemon status
   ./gradlew --status
   ```

2. **Parallel builds**
   ```bash
   # Add to gradle.properties
   org.gradle.parallel=true
   org.gradle.workers.max=4
   ```

3. **Skip tests during development**
   ```bash
   ./gradlew :native-lib:nativeCompile -x test
   ```

4. **Use quick build mode**
   ```bash
   # Faster native image build (less optimization)
   ./gradlew :native-lib:nativeCompile -Ob
   ```

5. **Increase memory**
   ```bash
   export GRADLE_OPTS="-Xmx8g"
   ```

---

## Getting Help

If you still have issues:

1. **Check existing issues**: [GitHub Issues](https://github.com/mulesoft-labs/data-weave-cli/issues)
2. **Run with debug output**:
   ```bash
   ./gradlew :native-lib:nativeCompile --info
   ./gradlew :native-lib:nativeCompile --debug > build.log 2>&1
   ```
3. **Share system info**:
   ```bash
   uname -a
   java -version
   ./gradlew --version
   ```
4. **Create detailed issue** with:
   - Error message (full stack trace)
   - System info (OS, versions)
   - Build log (`--debug` output)
   - Steps to reproduce

---

## Quick Reference

```bash
# Install all prerequisites (macOS)
brew install cmake rust go
curl -s "https://get.sdkman.io" | bash
sdk install java 24.0.2-graal

# Full clean build
./gradlew clean
./gradlew :native-lib:nativeCompile
./gradlew :native-lib:buildPythonWheel
./gradlew :native-lib:buildNodePackage

# Set library path
export DYLD_LIBRARY_PATH=$(pwd)/native-lib/build/native/nativeCompile  # macOS
export LD_LIBRARY_PATH=$(pwd)/native-lib/build/native/nativeCompile    # Linux

# Run all tests
./gradlew :native-lib:test

# Package for release
./gradlew :native-lib:packageAllBindings
```

---

**Last Updated**: 2026-06-30  
**For**: DataWeave CLI v1.0.0
