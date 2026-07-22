import { execFileSync } from "node:child_process";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { casesForMetric } from "../../lib/manifest.mjs";
import { computeStats } from "../../lib/stats.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CHILD = join(__dirname, "coldstart-child.mjs");

/** Spawn one fresh process and parse its single JSON line. */
function sampleOnce(corpusDir, caseId) {
  const out = execFileSync(process.execPath, [CHILD, corpusDir, caseId], { encoding: "utf-8" });
  const line = out.trim().split("\n").pop();
  return JSON.parse(line);
}

/**
 * @returns {Promise<Array<{id,metric,unit,stats,iterations}>>}
 */
export async function runColdStartAndFirstRun(manifest, { samplesOverride } = {}) {
  const rows = [];
  const ids = new Set([
    ...casesForMetric(manifest, "cold-start").map((c) => c.id),
    ...casesForMetric(manifest, "first-run").map((c) => c.id),
  ]);

  for (const id of ids) {
    const c = manifest.cases.find((x) => x.id === id);
    const n = samplesOverride ?? c.iterations?.samples ?? 20;
    const inits = [];
    const firsts = [];
    for (let i = 0; i < n; i++) {
      const { initMs, firstRunMs } = sampleOnce(manifest.corpusDir, id);
      inits.push(initMs);
      firsts.push(firstRunMs);
    }
    if (c.metrics.includes("cold-start")) {
      rows.push({ id, metric: "cold-start", unit: "ms", stats: computeStats(inits), iterations: n });
    }
    if (c.metrics.includes("first-run")) {
      rows.push({ id, metric: "first-run", unit: "ms", stats: computeStats(firsts), iterations: n });
    }
  }
  return rows;
}
