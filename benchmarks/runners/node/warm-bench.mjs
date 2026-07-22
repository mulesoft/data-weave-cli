import { readFileSync } from "node:fs";
import { join } from "node:path";
import { casesForMetric, resolveInputs } from "../../lib/manifest.mjs";
import { computeStats, toMBps } from "../../lib/stats.mjs";

const nowNs = () => process.hrtime.bigint();
const msSince = (start) => Number(nowNs() - start) / 1e6;

/** Build the wrapper Inputs map from resolved corpus inputs. */
function toInputsMap(resolved) {
  const inputs = {};
  for (const [name, v] of Object.entries(resolved)) {
    inputs[name] = { content: v.buffer, mimeType: v.mimeType, charset: v.charset ?? "utf-8" };
  }
  return inputs;
}

/** Split a Buffer into fixed-size chunks for streaming input. */
function* chunked(buffer, size = 65536) {
  for (let i = 0; i < buffer.length; i += size) yield buffer.subarray(i, i + size);
}

async function drain(gen) {
  let total = 0;
  let step = await gen.next();
  while (!step.done) {
    total += step.value.length;
    step = await gen.next();
  }
  if (!step.value.success) throw new Error(`stream failed: ${step.value.error}`);
  return total;
}

/**
 * @returns {Promise<Array<{id,metric,unit,stats,iterations}>>}
 */
export async function runWarmAndStreaming(api, manifest) {
  const rows = [];

  for (const c of casesForMetric(manifest, "warm")) {
    const script = readScript(manifest, c);
    const inputs = toInputsMap(resolveInputs(manifest, c));
    const warmup = c.iterations?.warmup ?? 10;
    const iters = c.iterations?.warm ?? 100;

    for (let i = 0; i < warmup; i++) assertOk(api.run(script, inputs));
    const samples = [];
    for (let i = 0; i < iters; i++) {
      const start = nowNs();
      assertOk(api.run(script, inputs));
      samples.push(msSince(start));
    }
    rows.push({ id: c.id, metric: "warm", unit: "ms", stats: computeStats(samples), iterations: iters });
  }

  for (const c of casesForMetric(manifest, "streaming")) {
    const script = readScript(manifest, c);
    const resolved = resolveInputs(manifest, c);
    const [primaryName, primary] = Object.entries(resolved)[0];
    const iters = c.iterations?.streaming ?? 10;

    const mbps = [];
    for (let i = 0; i < iters; i++) {
      const start = nowNs();
      const gen = api.runTransform(script, chunked(primary.buffer), {
        inputName: primaryName,
        mimeType: primary.mimeType,
        charset: primary.charset,
      });
      await drain(gen);
      mbps.push(toMBps(primary.buffer.length, msSince(start)));
    }
    rows.push({ id: c.id, metric: "streaming", unit: "MB/s", stats: computeStats(mbps), iterations: iters });
  }

  return rows;
}

function assertOk(result) {
  if (!result.success) throw new Error(`run failed: ${result.error}`);
  return result;
}

function readScript(manifest, c) {
  return readFileSync(join(manifest.corpusDir, c.script), "utf-8");
}
