import { spawn } from "node:child_process";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { casesForMetric } from "../../lib/manifest.mjs";
import { computeStats } from "../../lib/stats.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CHILD = join(__dirname, "coldstart-child.mjs");

/**
 * Spawn one fresh process and measure true cold-start: wall-clock from just
 * before spawn to the child's "READY" marker (process launch + module/addon load
 * + isolate init). first-run is timed in-process by the child. Rejects on a
 * non-zero exit or a missing READY/JSON line so a failed sample never records a
 * bogus timing.
 */
function sampleOnce(corpusDir, caseId) {
  return new Promise((resolve, reject) => {
    const t0 = process.hrtime.bigint();
    const child = spawn(process.execPath, [CHILD, corpusDir, caseId], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    let coldStartMs;
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf-8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
      if (coldStartMs === undefined && stdout.includes("READY\n")) {
        // Stamp the moment the runtime reported ready. Everything before this
        // line (launch, load, init) is the cold-start cost.
        coldStartMs = Number(process.hrtime.bigint() - t0) / 1e6;
      }
    });
    child.stderr.setEncoding("utf-8");
    child.stderr.on("data", (chunk) => (stderr += chunk));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`coldstart child failed for '${caseId}' (exit ${code})\n${stderr}`));
        return;
      }
      if (coldStartMs === undefined) {
        reject(new Error(`coldstart child for '${caseId}' never printed READY\n${stderr}`));
        return;
      }
      const jsonLine = stdout.split("\n").filter((l) => l && l !== "READY").pop();
      if (!jsonLine) {
        reject(new Error(`coldstart child for '${caseId}' printed no result line\n${stderr}`));
        return;
      }
      const { firstRunMs } = JSON.parse(jsonLine);
      resolve({ coldStartMs, firstRunMs });
    });
  });
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
    const colds = [];
    const firsts = [];
    for (let i = 0; i < n; i++) {
      const { coldStartMs, firstRunMs } = await sampleOnce(manifest.corpusDir, id);
      colds.push(coldStartMs);
      firsts.push(firstRunMs);
    }
    if (c.metrics.includes("cold-start")) {
      rows.push({ id, metric: "cold-start", unit: "ms", stats: computeStats(colds), iterations: n });
    }
    if (c.metrics.includes("first-run")) {
      rows.push({ id, metric: "first-run", unit: "ms", stats: computeStats(firsts), iterations: n });
    }
  }
  return rows;
}
