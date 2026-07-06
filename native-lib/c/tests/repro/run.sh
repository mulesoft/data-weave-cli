#!/usr/bin/env bash
#
# Regression harness for the adversarial-review findings in the C binding.
# See docs/reviews/2026-07-06-adversarial-native-bindings-review.md
#
# Builds a mock GraalVM dwlib, then runs:
#   - repro_pure:          findings [5], [13], [12]  (pure helpers, no lib)
#   - repro_stream_uaf:    findings [1], [2]         (use-after-free, ASan)
#   - repro_metadata_race: finding  [23]            (data race, ThreadSanitizer)
#
# With the fixes applied, all tests should PASS (clean execution, no sanitizer
# errors). Exit 0 when all findings are fixed (tests pass).

set -u
cd "$(dirname "$0")"

CC="${CC:-cc}"
DYLIB_EXT="so"; [ "$(uname)" = "Darwin" ] && DYLIB_EXT="dylib"
MOCK="./libdwlib_mock.${DYLIB_EXT}"
ASAN="-fsanitize=address -g -O1 -fno-omit-frame-pointer"

pass=0; fail=0
ok()   { echo "PASS: $1"; pass=$((pass+1)); }
bad()  { echo "FAIL: $1"; fail=$((fail+1)); }

echo "### Building mock dwlib"
$CC -shared -fPIC -o "$MOCK" mock_dwlib.c -lpthread || { echo "mock build failed"; exit 3; }
# The wrapper searches for a file literally named dwlib.<ext>; symlink it.
ln -sf "$(basename "$MOCK")" "dwlib.${DYLIB_EXT}"

echo
echo "### [5][13][12] Pure helper regression tests"
$CC $ASAN -I../../include -o repro_pure repro_pure.c -lpthread -ldl || { echo "repro_pure build failed"; exit 3; }
./repro_pure; n=$?
# repro_pure now exits with the count of findings still broken; expect 0 (all fixed).
if [ "$n" -eq 0 ]; then ok "pure findings [5],[13],[12] all fixed";
else bad "expected 0 pure findings broken, got $n"; fi

echo
echo "### [1][2] Streaming use-after-free regression tests (AddressSanitizer)"
$CC $ASAN -I../../include -o repro_stream_uaf repro_stream_uaf.c ../../src/dataweave.c -lpthread -ldl \
    || { echo "repro_stream_uaf build failed"; exit 3; }

SYM="$(xcrun -f llvm-symbolizer 2>/dev/null || command -v llvm-symbolizer || true)"
for mode in script stream; do
    label="[2] caller-owned pointers"; [ "$mode" = stream ] && label="[1] detached-worker UAF"
    echo "--- mode=$mode ($label)"
    # With the fix, ASan should NOT abort; clean exit (rc=0) == pass.
    DATAWEAVE_NATIVE_LIB="./dwlib.${DYLIB_EXT}" \
        ASAN_SYMBOLIZER_PATH="$SYM" \
        ASAN_OPTIONS="detect_leaks=0:symbolize=1:abort_on_error=1" \
        ./repro_stream_uaf "$mode" > "out_$mode.log" 2>&1
    rc=$?
    if [ "$rc" -eq 0 ] && ! grep -qi "use-after-free\|heap-use-after-free\|AddressSanitizer" "out_$mode.log"; then
        ok "$label fixed (clean ASan run)"
    else
        bad "$label NOT fixed (ASan error or nonzero rc=$rc); see out_$mode.log"
    fi
done

echo
echo "### [23] Metadata data-race regression test (ThreadSanitizer)"
# TSan and ASan cannot be combined, so this is a separate binary.
if $CC -fsanitize=thread -g -O1 -fno-omit-frame-pointer -I../../include \
        -o repro_metadata_race repro_metadata_race.c ../../src/dataweave.c -lpthread -ldl 2>/dev/null; then
    DATAWEAVE_NATIVE_LIB="./dwlib.${DYLIB_EXT}" \
        TSAN_SYMBOLIZER_PATH="$SYM" \
        TSAN_OPTIONS="symbolize=1:abort_on_error=1" \
        ./repro_metadata_race > out_metadata.log 2>&1
    rc=$?
    # With the fix, TSan should report NO data race; clean exit == pass.
    if [ "$rc" -eq 0 ] && ! grep -qi "ThreadSanitizer: data race" out_metadata.log; then
        ok "[23] metadata race fixed (clean TSan run)"
    else
        bad "[23] metadata race NOT fixed; see out_metadata.log"
    fi
else
    echo "SKIP: [23] — compiler lacks ThreadSanitizer (-fsanitize=thread)"
fi

echo
echo "### Summary: $pass passed, $fail failed"
# For a post-fix regression suite we WANT all tests to pass (findings fixed).
[ "$fail" -eq 0 ] && exit 0 || exit 1
