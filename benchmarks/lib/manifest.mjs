import { readFileSync, existsSync } from "node:fs";
import { join } from "node:path";

/** The only metrics a case may declare. */
export const METRICS = ["cold-start", "first-run", "warm", "streaming"];

/**
 * Load and validate corpus/manifest.json.
 * @param {string} corpusDir absolute path to the corpus directory
 */
export function loadManifest(corpusDir) {
  const raw = JSON.parse(readFileSync(join(corpusDir, "manifest.json"), "utf-8"));
  if (!Array.isArray(raw.cases)) throw new Error("manifest.cases must be an array");
  const ids = new Set();
  for (const c of raw.cases) {
    if (!c.id) throw new Error("manifest case is missing an id");
    if (ids.has(c.id)) throw new Error(`duplicate case id: ${c.id}`);
    ids.add(c.id);
    if (!Array.isArray(c.metrics) || c.metrics.length === 0) {
      throw new Error(`case ${c.id} must declare a non-empty metrics[]`);
    }
    for (const metric of c.metrics) {
      if (!METRICS.includes(metric)) throw new Error(`case ${c.id} has unknown metric: ${metric}`);
    }
    if (!c.script || !existsSync(join(corpusDir, c.script))) {
      throw new Error(`case ${c.id} script not found: ${c.script}`);
    }
    for (const [name, inp] of Object.entries(c.inputs ?? {})) {
      if (inp.file && !inp.generated && !existsSync(join(corpusDir, inp.file))) {
        throw new Error(`case ${c.id} input '${name}' file not found: ${inp.file}`);
      }
    }
  }
  return { corpusDir, cases: raw.cases, ids };
}

/** Cases whose declared metrics[] includes `metric`. */
export function casesForMetric(manifest, metric) {
  return manifest.cases.filter((c) => c.metrics.includes(metric));
}

/**
 * Read a case's declared inputs into Buffers.
 * @returns {Record<string,{buffer:Buffer, mimeType:string, charset?:string}>}
 */
export function resolveInputs(manifest, caseObj) {
  const out = {};
  for (const [name, inp] of Object.entries(caseObj.inputs ?? {})) {
    const buffer = readFileSync(join(manifest.corpusDir, inp.file));
    out[name] = { buffer, mimeType: inp.mimeType, charset: inp.charset };
  }
  return out;
}

/** Fail-fast: throw if any result case carries an id not present in the manifest. */
export function validateResultIds(manifest, resultCases) {
  for (const rc of resultCases) {
    if (!manifest.ids.has(rc.id)) {
      throw new Error(`result contains orphan id not in manifest: ${rc.id}`);
    }
  }
}
