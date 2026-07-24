import { test } from "node:test";
import assert from "node:assert/strict";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { readFileSync } from "node:fs";
import { loadManifest } from "../lib/manifest.mjs";
import {
  computeDelta,
  formatDelta,
  detectSkew,
  buildTable,
  dedupeLatestByRunner,
  renderMermaidCharts,
  renderMarkdown,
} from "./report.mjs";

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

test("streaming metric is non-comparable — no cross-runner delta is printed", () => {
  const manifest = loadManifest(CORPUS);
  const results = [load("node-a.json"), load("engine-b.json")];
  const { rows } = buildTable(manifest, results, "engine");

  // Both fixtures report map-scale streaming (300 vs 150 MB/s) — a naive delta
  // would print +100%, which is meaningless given the methodology asymmetry.
  const streaming = rows.find((r) => r.id === "map-scale" && r.metric === "streaming");
  assert.ok(streaming, "fixture should exercise a streaming row");
  assert.equal(streaming.comparable, false);
  assert.equal(streaming.delta, null);
  assert.equal(formatDelta(streaming), "n/a");

  // A comparable metric (warm) still gets a real delta.
  const warm = rows.find((r) => r.id === "trivial" && r.metric === "warm");
  assert.equal(warm.comparable, true);
  assert.equal(formatDelta(warm), "-50.0%");
});

test("renderMarkdown footnotes the n/a delta when a non-comparable metric is present", () => {
  const manifest = loadManifest(CORPUS);
  const results = [load("node-a.json"), load("engine-b.json")];
  const table = buildTable(manifest, results, "engine");
  const md = renderMarkdown(table, results, {
    baselineRunner: "engine",
    stamp: { commit: "abc1234", date: "2026-07-24T14:33:03Z" },
  });
  assert.ok(md.includes("| map-scale | streaming | MB/s |"), "streaming row is present");
  assert.ok(/\| n\/a \|/.test(md), "streaming delta cell reads n/a");
  assert.ok(!md.includes("+100.0%"), "no misleading streaming delta is printed");
  assert.ok(md.includes("not like-for-like across runners"), "footnote explains why");
});

test("renderMermaidCharts emits one chart per (case, metric) with a bar per runner", () => {
  const manifest = loadManifest(CORPUS);
  const results = [load("node-a.json"), load("engine-b.json")];
  const table = buildTable(manifest, results, "engine");
  const md = renderMermaidCharts(table);

  // One chart per (case, metric) row in the table.
  const chartCount = (md.match(/```mermaid/g) ?? []).length;
  assert.equal(chartCount, table.rows.length);
  assert.ok(md.includes("xychart-beta"));
  // Bars only (one per runner); no line series in the by-case layout.
  assert.ok(md.includes("bar ["));
  assert.ok(!md.includes("line ["));

  // Each case that appears in the table gets a heading.
  const cases = [...new Set(table.rows.map((r) => r.id))];
  for (const id of cases) assert.ok(md.includes(`### ${id}`), `missing heading for ${id}`);

  // x-axis is the runners; the bar series length matches the runner count.
  const runners = table.header.slice(3, table.header.length - 1);
  for (const block of md.split("```mermaid").slice(1)) {
    const xs = (block.match(/x-axis \[([^\]]*)\]/) ?? [])[1]?.split(",").length ?? 0;
    assert.equal(xs, runners.length, "x-axis lists every runner");
    const bar = block.match(/bar \[([^\]]*)\]/);
    assert.equal(bar[1].split(",").length, runners.length, "one bar value per runner");
  }
});

test("renderMarkdown stamps commit + run date for provenance", () => {
  const manifest = loadManifest(CORPUS);
  const results = [load("node-a.json"), load("engine-b.json")];
  const table = buildTable(manifest, results, "engine");
  const md = renderMarkdown(table, results, {
    baselineRunner: "engine",
    stamp: { commit: "abc1234", date: "2026-07-24T14:33:03Z" },
  });
  assert.ok(md.includes("abc1234"), "commit is stamped");
  assert.ok(md.includes("2026-07-24T14:33:03Z"), "run date is stamped");
  assert.ok(md.includes("## Table"));
  assert.ok(md.includes("## Charts"));
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
