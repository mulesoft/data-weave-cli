import { existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
// benchmarks/runners/cli -> benchmarks/runners -> benchmarks -> repo root
const REPO_ROOT = join(__dirname, "..", "..", "..");
const BIN_NAME = process.platform === "win32" ? "dw.exe" : "dw";
const DEFAULT_BIN = join(REPO_ROOT, "native-cli", "build", "native", "nativeCompile", BIN_NAME);

/**
 * Resolve the benchmark-enabled `dw` native binary. Honors DW_BENCH_BIN (absolute
 * path to a bench-built dw); otherwise the default nativeCompile output. The binary
 * must be built with -Pbenchmark=true so BenchmarkHarness is reachable.
 */
export function locateBinary() {
  const candidate = process.env.DW_BENCH_BIN || DEFAULT_BIN;
  if (!existsSync(candidate)) {
    throw new Error(
      `dw benchmark binary not found at ${candidate}. ` +
        `Build it with: ./gradlew native-cli:nativeCompile -Pbenchmark=true ` +
        `(or set DW_BENCH_BIN to a bench-enabled dw).`
    );
  }
  return candidate;
}
