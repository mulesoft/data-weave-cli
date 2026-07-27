import { writeFileSync, mkdirSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { join, dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { loadManifest, validateResultIds } from "../../lib/manifest.mjs";
import { gatherEnv } from "../../lib/env.mjs";
import { locateBinary } from "./locate.mjs";
import { runColdStartAndFirstRun } from "./coldstart.mjs";
import { runWarm } from "./warm.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "..", "corpus");
const RESULTS_DIR = join(__dirname, "..", "..", "results");

/** Assemble the full schema object (identical contract to the Node runner). */
export function buildResult(env, cases) {
  return {
    schemaVersion: "1.0",
    runner: env.runner,
    env,
    timestamp: new Date().toISOString(),
    cases,
  };
}

/** Best-effort `dw --version` first line; falls back to "dw". */
function probeVersion(bin) {
  try {
    const out = execFileSync(bin, ["--version"], { encoding: "utf-8" });
    const line = out.split("\n").map((l) => l.trim()).filter(Boolean)[0];
    return line ? `dw ${line}` : "dw";
  } catch {
    return "dw";
  }
}

export async function main() {
  const manifest = loadManifest(CORPUS);
  const bin = locateBinary();
  const env = gatherEnv({ runner: "cli", runtimeVersion: probeVersion(bin) });
  // The CLI is a native binary, not the staged dwlib — override the lib fingerprint.
  env.dwlibBuildId = "n/a-cli";

  const coldRows = await runColdStartAndFirstRun(manifest);
  const warmRows = await runWarm(manifest);

  const cases = [...coldRows, ...warmRows];
  validateResultIds(manifest, cases);

  mkdirSync(RESULTS_DIR, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outPath = join(RESULTS_DIR, `cli-${stamp}.json`);
  writeFileSync(outPath, JSON.stringify(buildResult(env, cases), null, 2));
  console.log(`wrote ${outPath} (${cases.length} rows)`);
  return outPath;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((e) => {
    console.error(e.message);
    process.exit(1);
  });
}
