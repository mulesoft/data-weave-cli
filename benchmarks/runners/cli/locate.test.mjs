import { test } from "node:test";
import assert from "node:assert/strict";
import { locateBinary } from "./locate.mjs";

test("DW_BENCH_BIN override is returned as-is when it exists", () => {
  // Point at a file guaranteed to exist: this test file itself.
  const self = new URL(import.meta.url).pathname;
  process.env.DW_BENCH_BIN = self;
  try {
    assert.equal(locateBinary(), self);
  } finally {
    delete process.env.DW_BENCH_BIN;
  }
});

test("throws an actionable error when the binary is absent", () => {
  process.env.DW_BENCH_BIN = "/nonexistent/dw-binary-xyz";
  try {
    assert.throws(() => locateBinary(), /nativeCompile|not found|build/i);
  } finally {
    delete process.env.DW_BENCH_BIN;
  }
});
