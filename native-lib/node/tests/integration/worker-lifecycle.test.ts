import { describe, it, expect, afterAll } from "vitest";
import { Worker } from "node:worker_threads";
import { join } from "node:path";
import * as ffi from "../../src/ffi";
import { findLibrary, buildInputsJson } from "../../src/utils";

// W-23692110 round 12 #9: real worker_threads coverage for the documented
// per-Worker engine model (README "Custom module resolvers and Worker threads").
//
// Workers cannot execute the TS sources (npm test runs vitest with no build for
// worker code, and a Worker spawns a fresh Node runtime), so each worker body is
// an inline JS string (eval:true) that require()s the BUILT addon directly --
// the same raw-addon boundary engine-handle-contract.test.ts drives. addonPath
// and the dwlib path are resolved on the main thread and passed via workerData.
//
// Determinism posture: exact cross-thread teardown interleavings are NOT
// deterministically forceable (best-effort, matching rounds 5-11). The
// deterministic assertions here are: resolver-backed/less engines produce
// correct output inside a Worker, and after N Worker create/exit-without-
// cleanup() cycles the main thread still initializes/runs and the final
// teardown is clean (the round-12 #2 behavioral proof).

const ADDON_PATH = join(__dirname, "..", "..", "build", "Release", "dwlib_addon.node");
const LIB_PATH = findLibrary();

// Runs one Worker to completion and returns its posted message. `mode` selects
// resolver-backed vs resolver-less and whether the Worker cleans up or abandons.
function runWorker(opts: {
  mode: "resolver" | "plain";
  cleanup: boolean;
  script: string;
}): Promise<{ ok: boolean; output?: string; error?: string }> {
  const body = `
    const { parentPort, workerData } = require('node:worker_threads');
    (async () => {
      const addon = require(workerData.addonPath);
      addon.initialize(workerData.libPath);
      let handle;
      if (workerData.mode === 'resolver') {
        const resolver = (modulePath) =>
          modulePath === 'org/test/w.dwl'
            ? '%dw 2.0\\nfun greet(n) = "W:" ++ n'
            : null;
        handle = addon.createEngineWithResolver(resolver);
      } else {
        handle = addon.createEngine();
      }
      let msg;
      try {
        const raw = addon.runScriptEngine(handle, workerData.script, '{}');
        const parsed = JSON.parse(raw);
        if (parsed.success === false) {
          msg = { ok: false, error: parsed.error };
        } else {
          // Non-streaming engine result carries base64 'result'; decode it.
          const out = parsed.result ? Buffer.from(parsed.result, 'base64').toString('utf-8') : '';
          msg = { ok: true, output: out };
        }
      } catch (e) {
        msg = { ok: false, error: String(e) };
      }
      if (workerData.cleanup) {
        try { addon.destroyEngine(handle); } catch (_) {}
        await addon.cleanup();
      }
      parentPort.postMessage(msg);
      // For the abandon variant we deliberately return WITHOUT cleanup so the
      // env cleanup hook fires as the Worker env tears down.
    })().catch((e) => { parentPort.postMessage({ ok: false, error: String(e) }); });
  `;
  return new Promise((resolve, reject) => {
    const w = new Worker(body, {
      eval: true,
      workerData: { addonPath: ADDON_PATH, libPath: LIB_PATH, mode: opts.mode, cleanup: opts.cleanup, script: opts.script },
    });
    let msg: any;
    w.once("message", (m) => { msg = m; });
    w.once("error", reject);
    w.once("exit", (code) => {
      if (code !== 0 && !msg) reject(new Error("Worker exited " + code));
      else resolve(msg);
    });
  });
}

describe("worker_threads engine lifecycle (round 12 #9)", () => {
  afterAll(async () => {
    // Final main-thread balancing cleanup so this file does not perturb sibling
    // integration files sharing the vitest worker process.
    await ffi.cleanup();
  });

  it("a resolver-backed engine in a Worker resolves the Worker's own module", async () => {
    const script = "%dw 2.0\nimport org::test::w\noutput application/json\n---\nw::greet(\"X\")";
    const msg = await runWorker({ mode: "resolver", cleanup: true, script });
    expect(msg.ok).toBe(true);
    expect(JSON.parse(msg.output!)).toBe("W:X");
  });

  it("a resolver-less engine in a Worker runs a plain script", async () => {
    const script = "%dw 2.0\noutput application/json\n---\n6 * 7";
    const msg = await runWorker({ mode: "plain", cleanup: true, script });
    expect(msg.ok).toBe(true);
    expect(JSON.parse(msg.output!)).toBe(42);
  });

  it("built-in modules resolve in a resolver-backed engine inside a Worker", async () => {
    const script =
      "%dw 2.0\nimport dw::core::Strings\noutput application/json\n---\nStrings::capitalize(\"hello\")";
    const msg = await runWorker({ mode: "resolver", cleanup: true, script });
    expect(msg.ok).toBe(true);
    expect(JSON.parse(msg.output!)).toBe("Hello");
  });

  it("N Workers that exit WITHOUT cleanup() do not wedge the isolate; main thread stays healthy (round 12 #2)", async () => {
    const CYCLES = 5;
    for (let i = 0; i < CYCLES; i++) {
      const msg = await runWorker({
        mode: "resolver",
        cleanup: false, // exit without cleanup -> env cleanup hook fires
        script: "%dw 2.0\noutput application/json\n---\n" + i,
      });
      expect(msg.ok).toBe(true);
    }
    // After all those abandoned Workers, the main thread must still initialize
    // and run. Pre-fix, each abandoned Worker leaked its init reference and the
    // isolate never returned to zero; the assertion here is behavioral (the
    // process is not wedged and cleanup still tears down cleanly at afterAll).
    ffi.initialize(LIB_PATH);
    const h = ffi.createEngine();
    const envelope = JSON.parse(
      ffi.runScriptEngine(h, "%dw 2.0\noutput application/json\n---\n1 + 1", buildInputsJson({}))
    );
    expect(envelope.success).toBe(true);
    expect(JSON.parse(Buffer.from(envelope.result, "base64").toString("utf-8"))).toBe(2);
    ffi.destroyEngine(h);
    await ffi.cleanup();
  });

  it("Worker.terminate() mid-life leaves the main thread able to initialize and run", async () => {
    const body = `
      const { workerData } = require('node:worker_threads');
      const addon = require(workerData.addonPath);
      addon.initialize(workerData.libPath);
      addon.createEngineWithResolver((p) => null);
      // Spin so the parent can terminate() us mid-life (no message posted).
      setInterval(() => {}, 10);
    `;
    const w = new Worker(body, {
      eval: true,
      workerData: { addonPath: ADDON_PATH, libPath: LIB_PATH },
    });
    // Give it time to initialize + create the engine, then terminate abruptly.
    await new Promise((r) => setTimeout(r, 500));
    await w.terminate();

    ffi.initialize(LIB_PATH);
    const h = ffi.createEngine();
    const envelope = JSON.parse(
      ffi.runScriptEngine(h, "%dw 2.0\noutput application/json\n---\n3 + 4", buildInputsJson({}))
    );
    expect(envelope.success).toBe(true);
    expect(JSON.parse(Buffer.from(envelope.result, "base64").toString("utf-8"))).toBe(7);
    ffi.destroyEngine(h);
    await ffi.cleanup();
  });
});
