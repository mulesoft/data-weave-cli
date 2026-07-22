import { existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
// benchmarks/runners/node -> benchmarks/runners -> benchmarks -> repo root
const REPO_ROOT = join(__dirname, "..", "..", "..");
const WRAPPER_DIST = join(REPO_ROOT, "native-lib", "node", "dist", "index.js");

/**
 * Import the built @dataweave/native wrapper. The wrapper locates dwlib itself
 * (staged at native-lib/node/native/dwlib.*), so no env var is required here.
 */
export async function loadWrapper() {
  if (!existsSync(WRAPPER_DIST)) {
    throw new Error(
      `Node wrapper not built at ${WRAPPER_DIST}. ` +
        `Run: ./gradlew native-lib:buildNodePackage`
    );
  }
  const mod = await import(pathToFileURL(WRAPPER_DIST).href);
  const api = mod.run ? mod : mod.default;
  if (!api || typeof api.run !== "function") {
    throw new Error(`Wrapper at ${WRAPPER_DIST} did not export a run() function`);
  }
  return api;
}
