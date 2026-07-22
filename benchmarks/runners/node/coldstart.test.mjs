import { test } from "node:test";
import assert from "node:assert/strict";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { loadManifest } from "../../lib/manifest.mjs";
import { runColdStartAndFirstRun } from "./coldstart.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "..", "corpus");

test("cold-start and first-run rows are produced from fresh processes", async () => {
  const manifest = loadManifest(CORPUS);
  // Keep sample count tiny so the test stays fast (each sample spawns a process).
  const rows = await runColdStartAndFirstRun(manifest, { samplesOverride: 3 });

  const cold = rows.filter((r) => r.metric === "cold-start");
  const first = rows.filter((r) => r.metric === "first-run");
  assert.ok(cold.length >= 1, "expected a cold-start row (trivial declares it)");
  assert.ok(first.length >= 1, "expected first-run rows");
  for (const r of [...cold, ...first]) {
    assert.equal(r.unit, "ms");
    assert.ok(r.stats.median > 0);
    assert.equal(r.iterations, 3);
  }
});
