// Fresh-process worker. Measures a cold isolate init + a cold (first) compile+exec
// for one case, then prints a single JSON line. Invoked by coldstart.mjs.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { loadManifest, resolveInputs } from "../../lib/manifest.mjs";
import { loadWrapper } from "./wrapper.mjs";

const [, , corpusDir, caseId] = process.argv;
const nowNs = () => process.hrtime.bigint();
const msSince = (s) => Number(nowNs() - s) / 1e6;

const manifest = loadManifest(corpusDir);
const c = manifest.cases.find((x) => x.id === caseId);
if (!c) throw new Error(`unknown case: ${caseId}`);
const script = readFileSync(join(corpusDir, c.script), "utf-8");

const resolved = resolveInputs(manifest, c);
const inputs = {};
for (const [name, v] of Object.entries(resolved)) {
  inputs[name] = { content: v.buffer, mimeType: v.mimeType, charset: v.charset ?? "utf-8" };
}

const api = await loadWrapper();
const dw = new api.DataWeave();

const initStart = nowNs();
dw.initialize();
const initMs = msSince(initStart);

const runStart = nowNs();
const result = dw.run(script, inputs);
const firstRunMs = msSince(runStart);
if (!result.success) throw new Error(`first run failed: ${result.error}`);

dw.cleanup();
process.stdout.write(JSON.stringify({ initMs, firstRunMs }) + "\n");
