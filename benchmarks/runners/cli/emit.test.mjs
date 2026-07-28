import { test } from "node:test";
import assert from "node:assert/strict";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { loadManifest, validateResultIds } from "../../lib/manifest.mjs";
import { buildResult } from "./emit.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "..", "corpus");

test("buildResult produces a schema-shaped object with runner 'cli'", () => {
  const env = {
    runner: "cli", os: "x", cpu: "y", runtimeVersion: "dw vX",
    weaveVersion: "2.12.0-x", commit: "abc", dwlibBuildId: "n/a-cli",
  };
  const cases = [{ id: "trivial", metric: "cold-start", unit: "ms", stats: { median: 1 }, iterations: 10 }];
  const r = buildResult(env, cases);
  assert.equal(r.schemaVersion, "1.0");
  assert.equal(r.runner, "cli");
  assert.ok(typeof r.timestamp === "string");
  assert.deepEqual(r.cases, cases);
});

test("orphan ids are rejected before writing", () => {
  const manifest = loadManifest(CORPUS);
  assert.throws(() => validateResultIds(manifest, [{ id: "totally-made-up" }]), /orphan id/);
});
