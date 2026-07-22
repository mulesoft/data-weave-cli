import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { loadManifest } from "../lib/manifest.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "corpus");

/** Raw percent change of value vs baseline. Sign interpretation is per-unit (caller decides). */
export function computeDelta(value, baseline, _unit) {
  if (baseline === 0) return NaN;
  return ((value - baseline) / baseline) * 100;
}

/** Distinct weaveVersions across results; length > 1 means the comparison spans versions. */
export function detectSkew(results) {
  return [...new Set(results.map((r) => r.env.weaveVersion))];
}

/** Keep only the latest result per distinct runner (by timestamp). */
export function dedupeLatestByRunner(results) {
  const latest = new Map();
  for (const r of results) {
    const existing = latest.get(r.runner);
    if (!existing || r.timestamp > existing.timestamp) {
      latest.set(r.runner, r);
    }
  }
  return results.filter((r) => latest.get(r.runner) === r);
}

/** True if lower is better for this unit. */
function lowerIsBetter(unit) {
  return unit === "ms";
}

/**
 * Join result files against the manifest into one row per (id, metric).
 * @param baselineRunner runner name whose value anchors the delta column
 */
export function buildTable(manifest, results, baselineRunner) {
  const runners = results.map((r) => r.runner);
  const cellByRunner = new Map(); // `${runner}|${id}|${metric}` -> {value, unit}
  for (const r of results) {
    for (const c of r.cases) {
      cellByRunner.set(`${r.runner}|${c.id}|${c.metric}`, { value: c.stats.median, unit: c.unit });
    }
  }

  const rows = [];
  for (const c of manifest.cases) {
    for (const metric of c.metrics) {
      const values = {};
      let unit = null;
      for (const runner of runners) {
        const cell = cellByRunner.get(`${runner}|${c.id}|${metric}`);
        if (cell) {
          values[runner] = cell.value;
          unit = cell.unit;
        }
      }
      if (Object.keys(values).length === 0) continue; // metric declared but no runner ran it
      const base = values[baselineRunner];
      const other = runners.find((r) => r !== baselineRunner && values[r] !== undefined);
      const delta =
        base !== undefined && other !== undefined ? computeDelta(values[other], base, unit) : null;
      rows.push({ id: c.id, metric, unit, values, delta, lowerIsBetter: lowerIsBetter(unit) });
    }
  }
  return { header: ["case", "metric", "unit", ...runners, `Δ vs ${baselineRunner}`], rows };
}

function fmt(n) {
  return n === undefined ? "—" : Number(n).toFixed(2);
}

export function main(argv) {
  const files = [];
  let baseline = null;
  let emit = null;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--baseline") baseline = argv[++i];
    else if (argv[i] === "--emit") emit = argv[++i];
    else files.push(argv[i]);
  }
  if (emit) throw new Error(`--emit ${emit} not implemented (exporter seam reserved for future history/dashboard)`);
  if (files.length === 0) throw new Error("usage: report.mjs <result.json...> [--baseline <runner>]");

  let results = files.map((f) => JSON.parse(readFileSync(f, "utf-8")));
  results = dedupeLatestByRunner(results);
  const manifest = loadManifest(CORPUS);
  const baselineRunner = baseline ?? (results.find((r) => r.runner === "engine")?.runner ?? results[0].runner);

  const skew = detectSkew(results);
  if (skew.length > 1) {
    console.log(`⚠️  WEAVE VERSION SKEW: comparing across ${skew.join(" vs ")} — deltas are not clean.`);
    console.log("");
  }

  const { header, rows } = buildTable(manifest, results, baselineRunner);
  console.log("| " + header.join(" | ") + " |");
  console.log("| " + header.map(() => "---").join(" | ") + " |");
  for (const row of rows) {
    const runnerCols = header.slice(3, header.length - 1).map((runner) => fmt(row.values[runner]));
    const deltaStr = row.delta === null ? "—" : `${row.delta > 0 ? "+" : ""}${row.delta.toFixed(1)}%`;
    console.log(`| ${row.id} | ${row.metric} | ${row.unit} | ${runnerCols.join(" | ")} | ${deltaStr} |`);
  }
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    main(process.argv.slice(2));
  } catch (e) {
    console.error(e.message);
    process.exit(1);
  }
}
