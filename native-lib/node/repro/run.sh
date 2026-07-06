#!/usr/bin/env bash
#
# Reproduction for finding [10] (Node read-callback swallows JS exceptions).
# See docs/reviews/2026-07-06-adversarial-native-bindings-review.md
#
# The real addon is N-API/libuv and needs node-gyp + node headers to build.
# This is a dependency-free C model of the exact exception-handling branch in
# addon.c:call_js_read (394-401). Exit 0 == finding reproduced (bug present).

set -u
cd "$(dirname "$0")"
CC="${CC:-cc}"

$CC -g -O1 -o repro_read_swallow repro_read_swallow.c || { echo "build failed"; exit 3; }
./repro_read_swallow
rc=$?
# repro_read_swallow returns 0 when the finding reproduces (bug present).
[ "$rc" -eq 0 ] && exit 0 || exit 1
