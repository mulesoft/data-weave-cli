import { readFileSync, existsSync, statSync } from "node:fs";
import { execSync } from "node:child_process";
import { createHash } from "node:crypto";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import os from "node:os";

const __dirname = dirname(fileURLToPath(import.meta.url));
// benchmarks/lib -> benchmarks -> repo root
const REPO_ROOT = join(__dirname, "..", "..");

function readWeaveVersion() {
  const txt = readFileSync(join(REPO_ROOT, "gradle.properties"), "utf-8");
  const m = txt.match(/^weaveVersion=(.+)$/m);
  if (!m) throw new Error("weaveVersion not found in gradle.properties");
  return m[1].trim();
}

function readCommit() {
  try {
    return execSync("git rev-parse --short HEAD", { cwd: REPO_ROOT }).toString().trim();
  } catch {
    return "unknown";
  }
}

// Best-effort identity of the selected dwlib: first 8 hex of a sha256 over
// (size + first 64KB). Cheap, stable, and enough to detect a lib swap.
function readDwlibBuildId(dwlibPath) {
  const paths = dwlibPath && existsSync(dwlibPath)
    ? [dwlibPath]
    : [".dylib", ".so", ".dll"].map((ext) => {
      return join(REPO_ROOT, "native-lib", "node", "native", `dwlib${ext}`);
    });
  for (const p of paths) {
    if (existsSync(p)) {
      const buf = readFileSync(p).subarray(0, 65536);
      const size = statSync(p).size;
      return "dwlib-" + createHash("sha256").update(String(size)).update(buf).digest("hex").slice(0, 8);
    }
  }
  return "unknown";
}

/**
 * @param {{runner:string, runtimeVersion:string, dwlibPath?:string}} opts
 */
export function gatherEnv({ runner, runtimeVersion, dwlibPath }) {
  const cpus = os.cpus();
  return {
    runner,
    os: `${process.platform}-${process.arch}`,
    cpu: cpus.length ? cpus[0].model : "unknown",
    runtimeVersion,
    weaveVersion: readWeaveVersion(),
    commit: readCommit(),
    dwlibBuildId: readDwlibBuildId(dwlibPath),
  };
}
