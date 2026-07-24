import { readFileSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { loadManifest } from "../lib/manifest.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CORPUS = join(__dirname, "..", "corpus");

/**
 * Metrics whose cross-runner numbers are NOT like-for-like, so a Δ between
 * runners is meaningless and must not be printed. `streaming` qualifies: the
 * engine times a full compile+write of the whole input per iteration, while the
 * Node runner times an incrementally-chunked runTransform (see
 * WarmBench.scala:36-46). warm/first-run/cold-start ARE comparable.
 */
const NON_COMPARABLE_METRICS = new Set(["streaming"]);

/** Raw percent change of value vs baseline. Sign interpretation is per-unit (caller decides). */
export function computeDelta(value, baseline, _unit) {
  if (baseline === 0) return NaN;
  return ((value - baseline) / baseline) * 100;
}

/**
 * Delta cell text for a table row. Distinguishes three states so the report is
 * never misleading: `n/a` = metric is not comparable across runners (methodology
 * differs), `—` = comparison not possible (missing baseline or only one runner),
 * else the signed percent.
 */
export function formatDelta(row) {
  if (!row.comparable) return "n/a";
  if (row.delta === null) return "—";
  return `${row.delta > 0 ? "+" : ""}${row.delta.toFixed(1)}%`;
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
      const comparable = !NON_COMPARABLE_METRICS.has(metric);
      const base = values[baselineRunner];
      const other = runners.find((r) => r !== baselineRunner && values[r] !== undefined);
      const delta =
        comparable && base !== undefined && other !== undefined
          ? computeDelta(values[other], base, unit)
          : null;
      rows.push({ id: c.id, metric, unit, values, delta, comparable, lowerIsBetter: lowerIsBetter(unit) });
    }
  }
  return { header: ["case", "metric", "unit", ...runners, `Δ vs ${baselineRunner}`], rows };
}

function fmt(n) {
  return n === undefined ? "—" : Number(n).toFixed(2);
}

/** Sanitize a label for use inside a Mermaid xychart quoted string. */
function mermaidLabel(s) {
  return `"${String(s).replace(/"/g, "'")}"`;
}

/** Distinct runner names from the table header (between the fixed cols and the Δ col). */
function runnersOf(table) {
  return table.header.slice(3, table.header.length - 1);
}

/**
 * Mermaid xychart-beta charts grouped by corpus case: one chart per (case,
 * metric), with x-axis = runners and one bar per runner. A single case can span
 * metrics of different units and scales (e.g. warm ms vs streaming MB/s), so
 * each metric is its own single-unit chart under the case's heading. Runners
 * missing a value render as 0 so the bar series length matches the x-axis
 * (Mermaid requires it). Cases and metrics follow manifest order.
 */
export function renderMermaidCharts(table) {
  const runners = runnersOf(table);
  const cases = [];
  for (const row of table.rows) if (!cases.includes(row.id)) cases.push(row.id);

  const blocks = [];
  for (const id of cases) {
    const rows = table.rows.filter((r) => r.id === id);
    const charts = [];
    for (const row of rows) {
      const better = row.lowerIsBetter ? "lower is better" : "higher is better";
      const bars = runners.map((r) => {
        const v = row.values[r];
        return v === undefined ? 0 : Number(v.toFixed(3));
      });
      const lines = ["```mermaid", "xychart-beta"];
      lines.push(`    title ${mermaidLabel(`${id} — ${row.metric} (${row.unit}, ${better})`)}`);
      lines.push(`    x-axis [${runners.map(mermaidLabel).join(", ")}]`);
      lines.push(`    y-axis ${mermaidLabel(row.unit)}`);
      lines.push(`    bar [${bars.join(", ")}]`);
      lines.push("```");
      charts.push(lines.join("\n"));
    }
    blocks.push(`### ${id}\n\n${charts.join("\n\n")}`);
  }
  return blocks.join("\n\n");
}

/**
 * A self-contained Markdown report: provenance (commit + date), the numeric
 * table, then a Mermaid bar chart per (case, metric) — one bar per runner.
 * `stamp` carries { commit, date } so a committed report says exactly which run
 * it reflects.
 */
export function renderMarkdown(table, results, { baselineRunner, stamp }) {
  const out = [];
  out.push("# DataWeave benchmark results", "");
  out.push(`_Generated from commit \`${stamp.commit}\` on ${stamp.date}._`, "");
  const envs = results
    .map((r) => `\`${r.runner}\` — ${r.env.runtimeVersion}, weave ${r.env.weaveVersion}`)
    .join("  \n");
  out.push(`**Environment** (${results[0]?.env.cpu ?? "?"}, ${results[0]?.env.os ?? "?"}):  \n${envs}`, "");
  out.push(
    "> Indicative only — timings are from a single run on one machine, not a dedicated bench box.",
    ""
  );

  out.push("## Table", "");
  out.push("| " + table.header.join(" | ") + " |");
  out.push("| " + table.header.map(() => "---").join(" | ") + " |");
  for (const row of table.rows) {
    const runnerCols = table.header.slice(3, table.header.length - 1).map((r) => fmt(row.values[r]));
    out.push(`| ${row.id} | ${row.metric} | ${row.unit} | ${runnerCols.join(" | ")} | ${formatDelta(row)} |`);
  }
  out.push("");
  if (table.rows.some((r) => !r.comparable)) {
    out.push(
      "> `n/a` deltas mark metrics that are not like-for-like across runners: " +
        "the engine's `streaming` times a full compile+write of the whole input per " +
        "iteration, while native-lib runners time an incrementally-chunked transform. " +
        "Compare each runner's absolute `streaming` throughput, not the delta.",
      ""
    );
  }

  out.push("## Charts", "");
  out.push(
    `One chart per corpus case, one bar per runner (${runnersOf(table).map((r) => `\`${r}\``).join(", ")}). ` +
      `A case's metrics differ in unit and scale, so each metric is a separate single-unit chart. ` +
      `\`${baselineRunner}\` is the table's delta baseline.`,
    ""
  );
  out.push(renderMermaidCharts(table));
  out.push("");
  return out.join("\n");
}

export function main(argv) {
  const files = [];
  let baseline = null;
  let emit = null;
  let markdown = null;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--baseline") baseline = argv[++i];
    else if (argv[i] === "--emit") emit = argv[++i];
    else if (argv[i] === "--markdown") markdown = argv[++i];
    else files.push(argv[i]);
  }
  if (emit) throw new Error(`--emit ${emit} not implemented (exporter seam reserved for future history/dashboard)`);
  if (files.length === 0) throw new Error("usage: report.mjs <result.json...> [--baseline <runner>] [--markdown <file>]");

  let results = files.map((f) => JSON.parse(readFileSync(f, "utf-8")));
  results = dedupeLatestByRunner(results);
  const manifest = loadManifest(CORPUS);
  const baselineRunner = baseline ?? (results.find((r) => r.runner === "engine")?.runner ?? results[0].runner);

  const skew = detectSkew(results);
  if (skew.length > 1) {
    console.log(`⚠️  WEAVE VERSION SKEW: comparing across ${skew.join(" vs ")} — deltas are not clean.`);
    console.log("");
  }

  const table = buildTable(manifest, results, baselineRunner);
  const { header, rows } = table;
  console.log("| " + header.join(" | ") + " |");
  console.log("| " + header.map(() => "---").join(" | ") + " |");
  for (const row of rows) {
    const runnerCols = header.slice(3, header.length - 1).map((runner) => fmt(row.values[runner]));
    console.log(`| ${row.id} | ${row.metric} | ${row.unit} | ${runnerCols.join(" | ")} | ${formatDelta(row)} |`);
  }
  if (rows.some((r) => !r.comparable)) {
    console.log("");
    console.log("note: `n/a` deltas mark metrics not comparable across runners (e.g. streaming — different methodology); compare absolute throughput, not the delta.");
  }

  if (markdown) {
    // Provenance from the data itself: commit is stamped into every result's env
    // (all runners share it in a clean run); date = latest result timestamp, so
    // the report is tied to the run, not to when it was rendered.
    const commit = results.find((r) => r.env?.commit)?.env.commit ?? "unknown";
    const date = results.map((r) => r.timestamp).sort().at(-1) ?? "unknown";
    const md = renderMarkdown(table, results, { baselineRunner, stamp: { commit, date } });
    writeFileSync(markdown, md);
    console.log(`\nwrote ${markdown}`);
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
