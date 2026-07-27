// Deterministically regenerate large inputs. No randomness -> comparable across
// machines and runners. Size overridable via BENCH_LARGE_N (default 50000).
import { writeFileSync, mkdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const outDir = join(__dirname, "inputs", "generated");
mkdirSync(outDir, { recursive: true });

const n = Number(process.env.BENCH_LARGE_N ?? 50000);
const records = [];
for (let i = 1; i <= n; i++) {
  records.push({ id: i, name: `item_${i}`, value: i * 3 });
}
const path = join(outDir, "records-large.json");
writeFileSync(path, JSON.stringify(records));
console.log(`wrote ${n} records to ${path}`);
