import { spawn } from "node:child_process";
import { join } from "node:path";
import { casesForMetric } from "../../lib/manifest.mjs";
import { computeStats } from "../../lib/stats.mjs";
import { locateBinary } from "./locate.mjs";

function commandArgs(manifest, c) {
  const args = ["run"];
  for (const [name, input] of Object.entries(c.inputs ?? {})) {
    args.push("-i", `${name}=${join(manifest.corpusDir, input.file)}`);
  }
  args.push("--file", join(manifest.corpusDir, c.script));
  return args;
}

export function sampleOnce(bin, args, c, { spawnFn = spawn } = {}) {
  return new Promise((resolve, reject) => {
    const t0 = process.hrtime.bigint();
    const child = spawnFn(bin, args, { stdio: ["ignore", "ignore", "pipe"] });
    let stderr = "";
    child.stderr.setEncoding("utf-8");
    child.stderr.on("data", (chunk) => (stderr += chunk));
    child.on("error", (error) => {
      reject(new Error(`cli first-run failed for '${c.id}'\n${stderr}`, { cause: error }));
    });
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`cli first-run failed for '${c.id}' (exit ${code})\n${stderr}`));
        return;
      }
      resolve(Number(process.hrtime.bigint() - t0) / 1e6);
    });
  });
}

/** @returns {Promise<Array<{id,metric,unit,stats,iterations}>>} */
export async function runFirstRun(manifest, { sample: sampleOverride, binary, samplesOverride } = {}) {
  const bin = binary ?? locateBinary();
  const sampleFn = sampleOverride ?? sampleOnce;
  const rows = [];
  for (const c of casesForMetric(manifest, "first-run")) {
    const n = samplesOverride ?? c.iterations?.samples ?? 20;
    const samples = [];
    const args = commandArgs(manifest, c);
    for (let i = 0; i < n; i++) {
      samples.push(await sampleFn(bin, args, c));
    }
    rows.push({ id: c.id, metric: "first-run", unit: "ms", stats: computeStats(samples), iterations: n });
  }
  return rows;
}
