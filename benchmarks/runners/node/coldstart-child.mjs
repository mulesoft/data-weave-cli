// Fresh-process worker for one case. Prints a "READY" marker the instant the
// runtime is initialized, then a JSON line with the in-process first-run timing.
// The PARENT (coldstart.mjs) measures cold-start as wall-clock from spawn to the
// READY marker, so process launch + module/addon load + isolate init are all
// included — not just the in-process initialize() call.
import { readFileSync, writeSync } from "node:fs";
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

dw.initialize();
// Runtime is ready: flush the marker synchronously so the parent's clock stops
// here. writeSync (fd 1) bypasses the async stdout buffer, so the timestamp the
// parent reads reflects init completion, not stream flushing.
writeSync(1, "READY\n");

const runStart = nowNs();
const result = dw.run(script, inputs);
const firstRunMs = msSince(runStart);
if (!result.success) throw new Error(`first run failed: ${result.error}`);

dw.cleanup();
process.stdout.write(JSON.stringify({ firstRunMs }) + "\n");
