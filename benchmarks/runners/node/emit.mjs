import { writeFileSync, mkdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { loadManifest, validateResultIds } from "../../lib/manifest.mjs";
import { gatherEnv } from "../../lib/env.mjs";
import { loadWrapper } from "./wrapper.mjs";
import { runWarmAndStreaming } from "./warm-bench.mjs";
import { runColdStartAndFirstRun } from "./coldstart.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "..", "corpus");
const RESULTS_DIR = join(__dirname, "..", "..", "results");

/** Assemble the full schema object. */
export function buildResult(env, cases) {
  return {
    schemaVersion: "1.0",
    runner: env.runner,
    env,
    timestamp: new Date().toISOString(),
    cases,
  };
}

export async function main() {
  const manifest = loadManifest(CORPUS);
  const env = gatherEnv({ runner: "node-wrapper", runtimeVersion: `node ${process.version}` });

  // Cold-start / first-run first (fresh processes), then warm/streaming in-process.
  const coldRows = await runColdStartAndFirstRun(manifest);
  const api = await loadWrapper();
  let warmRows;
  try {
    warmRows = await runWarmAndStreaming(api, manifest);
  } finally {
    api.cleanup();
  }

  const cases = [...coldRows, ...warmRows];
  validateResultIds(manifest, cases); // fail-fast on any orphan id

  mkdirSync(RESULTS_DIR, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outPath = join(RESULTS_DIR, `node-${stamp}.json`);
  writeFileSync(outPath, JSON.stringify(buildResult(env, cases), null, 2));
  console.log(`wrote ${outPath} (${cases.length} rows)`);
  return outPath;
}

// Run when invoked directly.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((e) => {
    console.error(e.message);
    process.exit(1);
  });
}
