import { spawn } from "node:child_process";
import { join } from "node:path";
import { casesForMetric } from "../../lib/manifest.mjs";
import { computeStats } from "../../lib/stats.mjs";
import { locateBinary } from "./locate.mjs";

function inputArgs(manifest, c) {
  const args = [];
  for (const [name, inp] of Object.entries(c.inputs ?? {})) {
    const file = join(manifest.corpusDir, inp.file);
    const charset = inp.charset ?? "utf-8";
    args.push(`--input=${name}=${file}\t${inp.mimeType}\t${charset}`);
  }
  return args;
}

/** Spawn dw once in warm mode; resolve the parsed warmMs[] sample array. */
function warmSamples(bin, manifest, c) {
  const scriptPath = join(manifest.corpusDir, c.script);
  const warmup = c.iterations?.warmup ?? 10;
  const iters = c.iterations?.warm ?? 100;
  const args = [
    "--bench-mode=warm",
    `--script=${scriptPath}`,
    `--warmup=${warmup}`,
    `--iters=${iters}`,
    ...inputArgs(manifest, c),
  ];
  return new Promise((resolve, reject) => {
    const child = spawn(bin, args, {
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env, DW_BENCH: "1" },
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf-8");
    child.stdout.on("data", (chunk) => (stdout += chunk));
    child.stderr.setEncoding("utf-8");
    child.stderr.on("data", (chunk) => (stderr += chunk));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`cli warm failed for '${c.id}' (exit ${code})\n${stderr}`));
        return;
      }
      const jsonLine = stdout.split("\n").filter((l) => l && l !== "READY").pop();
      if (!jsonLine) {
        reject(new Error(`cli warm for '${c.id}' printed no result line\n${stderr}`));
        return;
      }
      const { warmMs } = JSON.parse(jsonLine);
      if (!Array.isArray(warmMs) || warmMs.length === 0) {
        reject(new Error(`cli warm for '${c.id}' returned no samples\n${stderr}`));
        return;
      }
      resolve({ warmMs, iters });
    });
  });
}

/** @returns {Promise<Array<{id,metric,unit,stats,iterations}>>} */
export async function runWarm(manifest) {
  const bin = locateBinary();
  const rows = [];
  for (const c of casesForMetric(manifest, "warm")) {
    const { warmMs, iters } = await warmSamples(bin, manifest, c);
    rows.push({ id: c.id, metric: "warm", unit: "ms", stats: computeStats(warmMs), iterations: iters });
  }
  return rows;
}
