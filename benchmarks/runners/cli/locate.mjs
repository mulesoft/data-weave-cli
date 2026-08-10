import { existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
// benchmarks/runners/cli -> benchmarks/runners -> benchmarks -> repo root
const REPO_ROOT = join(__dirname, "..", "..", "..");
const BIN_NAME = process.platform === "win32" ? "dw.exe" : "dw";
const DEFAULT_BIN = join(REPO_ROOT, "native-cli", "build", "native", "nativeCompile", BIN_NAME);

/**
 * Resolve a `dw` native executable. Honors DW_BENCH_BIN (absolute path to an
 * executable); otherwise the default nativeCompile output.
 */
export function locateBinary() {
  const candidate = process.env.DW_BENCH_BIN || DEFAULT_BIN;
  if (!existsSync(candidate)) {
    throw new Error(
      `dw binary not found at ${candidate}. ` +
        `Build it with: ./gradlew native-cli:nativeCompile ` +
        `(or set DW_BENCH_BIN to a dw executable).`
    );
  }
  return candidate;
}
