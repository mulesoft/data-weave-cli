import { test } from "node:test";
import assert from "node:assert/strict";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { loadManifest } from "../../lib/manifest.mjs";
import { loadWrapper } from "./wrapper.mjs";
import { runWarmAndStreaming } from "./warm-bench.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "..", "corpus");

test("warm + streaming rows are produced with valid stats", async () => {
  const api = await loadWrapper();
  try {
    const manifest = loadManifest(CORPUS);
    const rows = await runWarmAndStreaming(api, manifest);

    const warm = rows.filter((r) => r.metric === "warm");
    const streaming = rows.filter((r) => r.metric === "streaming");
    assert.ok(warm.length >= 1, "expected at least one warm row");
    assert.ok(streaming.length >= 1, "expected at least one streaming row");

    for (const r of warm) {
      assert.equal(r.unit, "ms");
      assert.ok(r.stats.median >= 0);
      assert.ok(r.stats.p99 >= r.stats.median);
    }
    for (const r of streaming) {
      assert.equal(r.unit, "MB/s");
      assert.ok(r.stats.median > 0);
    }
  } finally {
    api.cleanup();
  }
});
