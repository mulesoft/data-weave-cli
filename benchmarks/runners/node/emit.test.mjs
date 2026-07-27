import { test } from "node:test";
import assert from "node:assert/strict";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { loadManifest, validateResultIds } from "../../lib/manifest.mjs";
import { buildResult } from "./emit.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "..", "corpus");

test("buildResult produces a schema-shaped object", () => {
  const env = {
    runner: "node-wrapper", os: "x", cpu: "y", runtimeVersion: "node vX",
    weaveVersion: "2.12.0-x", commit: "abc", dwlibBuildId: "dwlib-x",
  };
  const cases = [{ id: "trivial", metric: "warm", unit: "ms", stats: { median: 1 }, iterations: 10 }];
  const r = buildResult(env, cases);
  assert.equal(r.schemaVersion, "1.0");
  assert.equal(r.runner, "node-wrapper");
  assert.ok(typeof r.timestamp === "string");
  assert.deepEqual(r.cases, cases);
});

test("orphan ids are rejected before writing", () => {
  const manifest = loadManifest(CORPUS);
  assert.throws(
    () => validateResultIds(manifest, [{ id: "totally-made-up" }]),
    /orphan id/
  );
});
