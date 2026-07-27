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

test("buildTable joins by (id, metric) with a per-runner delta vs baseline", () => {
  const manifest = loadManifest(CORPUS);
  const table = buildTable(manifest, [load("node-a.json"), load("engine-b.json")], "engine");
  const trivialWarm = table.rows.find((r) => r.id === "trivial" && r.metric === "warm");
  assert.ok(trivialWarm);
  assert.equal(trivialWarm.values["node-wrapper"], 2.0);
  assert.equal(trivialWarm.values["engine"], 4.0);
  assert.equal(trivialWarm.deltas["node-wrapper"], -50); // node is 50% lower (faster) than engine baseline
  // engine is the baseline, so it never gets its own delta entry.
  assert.deepEqual(table.otherRunners, ["node-wrapper"]);
  assert.equal(table.header.at(-1), "Δ node-wrapper vs engine");
});

test("buildTable emits one delta column per non-baseline runner (3 runners)", () => {
  const manifest = loadManifest(CORPUS);
  const engine = load("engine-b.json");
  // Derive a python runner from the node fixture so all three share (id, metric) rows.
  const node = load("node-a.json");
  const python = { ...node, runner: "python-wrapper" };
  const table = buildTable(manifest, [engine, node, python], "engine");
  assert.deepEqual(table.otherRunners, ["node-wrapper", "python-wrapper"]);
  assert.equal(table.header.at(-2), "Δ node-wrapper vs engine");
  assert.equal(table.header.at(-1), "Δ python-wrapper vs engine");
  const trivialWarm = table.rows.find((r) => r.id === "trivial" && r.metric === "warm");
  assert.equal(trivialWarm.deltas["node-wrapper"], -50);
  assert.equal(trivialWarm.deltas["python-wrapper"], -50);
});

test("streaming metric now carries a real cross-runner delta", () => {
  const manifest = loadManifest(CORPUS);
  const results = [load("node-a.json"), load("engine-b.json")];
  const { rows } = buildTable(manifest, results, "engine");

  // Fixtures: map-scale streaming node=300 vs engine=150 MB/s. With aligned
  // methodology this is a real +100% delta, no longer suppressed.
  const streaming = rows.find((r) => r.id === "map-scale" && r.metric === "streaming");
  assert.ok(streaming, "fixture should exercise a streaming row");
  assert.equal(streaming.comparable, true);
  assert.equal(streaming.deltas["node-wrapper"], 100);
  assert.equal(formatDelta(streaming.deltas["node-wrapper"], streaming.comparable), "+100.0%");
});

test("renderMarkdown emits no streaming non-comparable footnote", () => {
  const manifest = loadManifest(CORPUS);
  const results = [load("node-a.json"), load("engine-b.json")];
  const table = buildTable(manifest, results, "engine");
  const md = renderMarkdown(table, results, {
    baselineRunner: "engine",
    stamp: { commit: "abc1234", date: "2026-07-24T14:33:03Z" },
  });
  assert.ok(md.includes("| map-scale | streaming | MB/s |"), "streaming row is present");
  assert.ok(!md.includes("not like-for-like across runners"), "footnote removed");
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
  const runners = table.header.slice(3, table.header.length - table.otherRunners.length);
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
