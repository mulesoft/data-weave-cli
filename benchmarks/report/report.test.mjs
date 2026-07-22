import { test } from "node:test";
import assert from "node:assert/strict";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { readFileSync } from "node:fs";
import { loadManifest } from "../lib/manifest.mjs";
import { computeDelta, detectSkew, buildTable, dedupeLatestByRunner } from "./report.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "corpus");
const load = (f) => JSON.parse(readFileSync(join(__dirname, "fixtures", f), "utf-8"));

test("computeDelta is raw percent change", () => {
  assert.equal(computeDelta(2, 4, "ms"), -50);   // node 2ms vs engine 4ms
  assert.equal(computeDelta(300, 150, "MB/s"), 100);
});

test("detectSkew finds differing weave versions", () => {
  const skew = detectSkew([load("node-a.json"), load("engine-b.json")]);
  assert.equal(skew.length, 2);
  assert.ok(skew.includes("2.12.0-20260413"));
  assert.ok(skew.includes("2.13.0-SNAPSHOT"));
});

test("buildTable joins by (id, metric) with a delta vs baseline", () => {
  const manifest = loadManifest(CORPUS);
  const { rows } = buildTable(manifest, [load("node-a.json"), load("engine-b.json")], "engine");
  const trivialWarm = rows.find((r) => r.id === "trivial" && r.metric === "warm");
  assert.ok(trivialWarm);
  assert.equal(trivialWarm.values["node-wrapper"], 2.0);
  assert.equal(trivialWarm.values["engine"], 4.0);
  assert.equal(trivialWarm.delta, -50); // node is 50% lower (faster) than engine baseline
});

test("dedupeLatestByRunner keeps the latest result per runner", () => {
  const older = {
    runner: "node-wrapper",
    timestamp: "2026-07-22T12:00:00.000Z",
    cases: [{ id: "trivial", metric: "warm", stats: { median: 3.0 }, unit: "ms" }],
    env: { weaveVersion: "2.12.0" }
  };
  const newer = {
    runner: "node-wrapper",
    timestamp: "2026-07-22T13:00:00.000Z",
    cases: [{ id: "trivial", metric: "warm", stats: { median: 2.5 }, unit: "ms" }],
    env: { weaveVersion: "2.12.0" }
  };
  const engine = {
    runner: "engine",
    timestamp: "2026-07-22T12:30:00.000Z",
    cases: [{ id: "trivial", metric: "warm", stats: { median: 4.0 }, unit: "ms" }],
    env: { weaveVersion: "2.12.0" }
  };

  // Single runner: newer wins
  const deduped1 = dedupeLatestByRunner([older, newer]);
  assert.equal(deduped1.length, 1);
  assert.equal(deduped1[0].timestamp, "2026-07-22T13:00:00.000Z");
  assert.equal(deduped1[0].cases[0].stats.median, 2.5);

  // Multiple runners: one per runner
  const deduped2 = dedupeLatestByRunner([older, engine, newer]);
  assert.equal(deduped2.length, 2);
  const nodeResult = deduped2.find((r) => r.runner === "node-wrapper");
  const engineResult = deduped2.find((r) => r.runner === "engine");
  assert.equal(nodeResult.timestamp, "2026-07-22T13:00:00.000Z");
  assert.equal(engineResult.timestamp, "2026-07-22T12:30:00.000Z");
});
