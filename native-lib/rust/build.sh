#!/usr/bin/env bash
set -e

echo "DataWeave Rust Bindings - Build Script"
echo "======================================="
echo ""

# Check if Rust is installed
if ! command -v cargo &> /dev/null; then
    echo "Error: Rust is not installed. Install from https://rustup.rs/"
    exit 1
fi

# Check if native library exists
NATIVE_LIB=""
if [[ "$OSTYPE" == "darwin"* ]]; then
    NATIVE_LIB="../../build/native/nativeCompile/dwlib.dylib"
elif [[ "$OSTYPE" == "linux"* ]]; then
    NATIVE_LIB="../../build/native/nativeCompile/dwlib.so"
else
    NATIVE_LIB="../../build/native/nativeCompile/dwlib.dll"
fi

if [ ! -f "$NATIVE_LIB" ]; then
    echo "Error: Native library not found at $NATIVE_LIB"
    echo ""
    echo "Build it first with:"
    echo "  cd ../.."
    echo "  ./gradlew nativeCompile"
    exit 1
fi

echo "Native library found: $NATIVE_LIB"
echo ""

# Set environment variable for tests
export DATAWEAVE_NATIVE_LIB="$(cd "$(dirname "$NATIVE_LIB")" && pwd)/$(basename "$NATIVE_LIB")"
echo "DATAWEAVE_NATIVE_LIB=$DATAWEAVE_NATIVE_LIB"
echo ""

# Build
echo "Building..."
cargo build

echo ""
echo "Build complete!"
echo ""
echo "To run tests:"
echo "  export DATAWEAVE_NATIVE_LIB=$DATAWEAVE_NATIVE_LIB"
echo "  cargo test"
echo ""
echo "To run examples:"
echo "  export DATAWEAVE_NATIVE_LIB=$DATAWEAVE_NATIVE_LIB"
echo "  cargo run --example basic"
