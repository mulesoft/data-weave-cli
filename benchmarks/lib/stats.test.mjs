import { test } from "node:test";
import assert from "node:assert/strict";
import { computeStats, toMBps } from "./stats.mjs";

test("computeStats returns min/median/p90/p99/mean", () => {
  const s = computeStats([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  assert.equal(s.min, 1);
  assert.equal(s.mean, 5.5);
  assert.equal(s.median, 5);
  assert.equal(s.p90, 9);
  assert.equal(s.p99, 10);
});

test("computeStats handles a single sample", () => {
  const s = computeStats([42]);
  assert.deepEqual(s, { min: 42, median: 42, p90: 42, p99: 42, mean: 42 });
});

test("computeStats rejects empty input", () => {
  assert.throws(() => computeStats([]), /non-empty/);
  assert.throws(() => computeStats(null), /non-empty/);
});

test("toMBps converts bytes and elapsed to MB/s", () => {
  // 10 MB in 1000 ms => 10 MB/s
  assert.equal(toMBps(10_000_000, 1000), 10);
});
