import { spawn } from "node:child_process";
import { join } from "node:path";
import { casesForMetric } from "../../lib/manifest.mjs";
import { computeStats } from "../../lib/stats.mjs";
import { locateBinary } from "./locate.mjs";

/** Build `--input=name=file\tmime\tcharset` args for a case (absolute paths). */
function inputArgs(manifest, c) {
  const args = [];
  for (const [name, inp] of Object.entries(c.inputs ?? {})) {
    const file = join(manifest.corpusDir, inp.file);
    const charset = inp.charset ?? "utf-8";
    args.push(`--input=${name}=${file}\t${inp.mimeType}\t${charset}`);
  }
  return args;
}

/**
 * Spawn one fresh dw process in coldfirst mode. Cold-start = wall-clock from just
 * before spawn to the child's "READY" marker (process launch + native image load +
 * NativeRuntime init). first-run is timed in-process by the child. Rejects on a
 * non-zero exit or a missing READY/JSON line so a failed sample never records a
 * bogus timing.
 */
function sampleOnce(bin, manifest, c) {
  const scriptPath = join(manifest.corpusDir, c.script);
  const args = ["--bench-mode=coldfirst", `--script=${scriptPath}`, ...inputArgs(manifest, c)];
  return new Promise((resolve, reject) => {
    const t0 = process.hrtime.bigint();
    const child = spawn(bin, args, {
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env, DW_BENCH: "1" },
    });
    let coldStartMs;
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf-8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
      if (coldStartMs === undefined && stdout.includes("READY\n")) {
        coldStartMs = Number(process.hrtime.bigint() - t0) / 1e6;
      }
    });
    child.stderr.setEncoding("utf-8");
    child.stderr.on("data", (chunk) => (stderr += chunk));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`cli coldfirst failed for '${c.id}' (exit ${code})\n${stderr}`));
        return;
      }
      if (coldStartMs === undefined) {
        reject(new Error(`cli coldfirst for '${c.id}' never printed READY\n${stderr}`));
        return;
      }
      const jsonLine = stdout.split("\n").filter((l) => l && l !== "READY").pop();
      if (!jsonLine) {
        reject(new Error(`cli coldfirst for '${c.id}' printed no result line\n${stderr}`));
        return;
      }
      let firstRunMs;
      try {
        ({ firstRunMs } = JSON.parse(jsonLine));
      } catch (error) {
        reject(new Error(`cli coldfirst for '${c.id}' printed invalid JSON: ${jsonLine}`, { cause: error }));
        return;
      }
      resolve({ coldStartMs, firstRunMs });
    });
  });
}

/** @returns {Promise<Array<{id,metric,unit,stats,iterations}>>} */
export async function runColdStartAndFirstRun(manifest, { samplesOverride } = {}) {
  const bin = locateBinary();
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
      const { coldStartMs, firstRunMs } = await sampleOnce(bin, manifest, c);
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
