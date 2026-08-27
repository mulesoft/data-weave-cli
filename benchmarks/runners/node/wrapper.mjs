import { existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
// benchmarks/runners/node -> benchmarks/runners -> benchmarks -> repo root
const REPO_ROOT = join(__dirname, "..", "..", "..");

function resolvePackageRoot() {
  return process.env.DW_BENCH_NODE_PACKAGE || join(REPO_ROOT, "native-lib", "node");
}

export function resolveWrapperPath() {
  const wrapperPath = join(resolvePackageRoot(), "dist", "index.js");
  if (existsSync(wrapperPath)) {
    return wrapperPath;
  }

  if (process.env.DW_BENCH_NODE_PACKAGE) {
    throw new Error(
      `DW_BENCH_NODE_PACKAGE=${process.env.DW_BENCH_NODE_PACKAGE} does not contain dist/index.js ` +
      `(expected an extracted dataweave-native package)`
    );
  }

  throw new Error(
    `Node wrapper not built at ${wrapperPath}. ` +
      `Run: ./gradlew native-lib:buildNodePackage`
  );
}

export function resolveDwlibPath() {
  const packageRoot = resolvePackageRoot();
  for (const ext of [".dylib", ".so", ".dll"]) {
    const candidate = join(packageRoot, "native", `dwlib${ext}`);
    if (existsSync(candidate)) {
      return candidate;
    }
  }
  return undefined;
}

/**
 * Import the built dataweave-native wrapper. The wrapper locates dwlib itself
 * (staged at native-lib/node/native/dwlib.*), so no env var is required here.
 */
export async function loadWrapper() {
  const wrapperPath = resolveWrapperPath();

  const mod = await import(pathToFileURL(wrapperPath).href);
  const api = mod.run ? mod : mod.default;
  if (!api || typeof api.run !== "function") {
    throw new Error(`Wrapper at ${wrapperPath} did not export a run() function`);
  }
  return api;
}
