import { test } from "node:test";
import assert from "node:assert/strict";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
  METRICS,
  loadManifest,
  casesForMetric,
  validateResultIds,
} from "./manifest.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "corpus");

test("METRICS lists exactly the four metrics", () => {
  assert.deepEqual([...METRICS].sort(), ["cold-start", "first-run", "streaming", "warm"]);
});

test("loadManifest validates the committed corpus", () => {
  const m = loadManifest(CORPUS);
  assert.ok(m.cases.length >= 6, "expected at least 6 corpus cases");
  assert.ok(m.ids.has("trivial"));
});

test("casesForMetric filters by declared metric", () => {
  const m = loadManifest(CORPUS);
  const streaming = casesForMetric(m, "streaming");
  assert.ok(streaming.every((c) => c.metrics.includes("streaming")));
  assert.ok(streaming.length >= 1);
});

test("validateResultIds throws on an orphan id", () => {
  const m = loadManifest(CORPUS);
  assert.throws(() => validateResultIds(m, [{ id: "does-not-exist" }]), /orphan id/);
  assert.doesNotThrow(() => validateResultIds(m, [{ id: "trivial" }]));
});
