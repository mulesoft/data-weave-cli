import { test } from "node:test";
import assert from "node:assert/strict";
import { locateBinary } from "./locate.mjs";

test("DW_BENCH_BIN override returns an ordinary dw executable as-is", () => {
  // Point at a file guaranteed to exist: this test file itself, representing dw.
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
    assert.throws(
      () => locateBinary(),
      (error) =>
        error.message.includes("Build it with: ./gradlew native-cli:nativeCompile") &&
        !error.message.includes("-Pbenchmark=true")
    );
  } finally {
    delete process.env.DW_BENCH_BIN;
  }
});
