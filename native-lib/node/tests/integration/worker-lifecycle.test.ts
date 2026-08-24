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
        let destroyErr;
        try {
          addon.destroyEngine(handle);
        } catch (e) {
          destroyErr = e; // preserve; do NOT let cleanup() mask a broken destroy
        } finally {
          await addon.cleanup();
        }
        if (destroyErr) msg = { ok: false, error: 'destroyEngine failed: ' + String(destroyErr) };
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
    let msg: { ok: boolean; output?: string; error?: string } | undefined;
    w.once("message", (m) => { msg = m; });
    w.once("error", reject);
    // Resolve only on a CLEAN exit that posted a result. A Worker can post a
    // success message and THEN exit nonzero (e.g. an env-cleanup-hook failure
    // during teardown) -- resolving on the message alone would hide that. So
    // wait for exit: reject every nonzero code, and treat a zero exit with no
    // posted message as its own diagnosable failure (round-14 #5).
    w.once("exit", (code) => {
      if (code !== 0) {
        reject(new Error("Worker exited with code " + code + (msg ? "" : " and posted no message")));
      } else if (msg === undefined) {
        reject(new Error("Worker exited 0 without posting a result"));
      } else {
        resolve(msg);
      }
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
      const { parentPort, workerData } = require('node:worker_threads');
      const addon = require(workerData.addonPath);
      addon.initialize(workerData.libPath);
      addon.createEngineWithResolver((p) => null);
      // Signal readiness only once the engine is actually live, so the parent
      // terminates a worker that genuinely has a live engine rather than
      // racing a fixed sleep against initialize()/createEngineWithResolver on
      // a possibly-loaded box (final review round 12 #3).
      parentPort.postMessage('ready');
      // Spin so the parent can terminate() us mid-life (no message posted).
      setInterval(() => {}, 10);
    `;
    const w = new Worker(body, {
      eval: true,
      workerData: { addonPath: ADDON_PATH, libPath: LIB_PATH },
    });
    // A throw in the worker body (e.g. a bad addon path) must fail this test
    // cleanly rather than crash the vitest process -- the ad hoc Worker here,
    // unlike runWorker() above, previously had no error listener wired up
    // (final review round 12 #2).
    const workerError = new Promise<never>((_, reject) => w.once("error", reject));
    // Avoid an unhandled-rejection warning if "error" fires (or would fire)
    // after the race below has already settled via the "ready" path.
    workerError.catch(() => {});
    // Wait for the worker to report the engine is live, racing against a
    // generous timeout so a slow box doesn't false-fail this test, then
    // terminate abruptly.
    const ready = new Promise<void>((resolve) => w.once("message", (m) => { if (m === "ready") resolve(); }));
    await Promise.race([
      ready,
      workerError,
      new Promise((_, reject) => setTimeout(() => reject(new Error("worker did not signal ready in time")), 10000)),
    ]);
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

  it("a Worker that inits once + creates N engines + exits without cleanup() does NOT tear down the isolate under a live main engine (round 13 #5)", async () => {
    // This is the cross-env regression the round-13 smoke tests could not pin
    // (env-init-ownership.test.ts is single-env). It fails RED on the round-12
    // implementation: the Worker's env death fired N per-engine init-reference
    // releases against the ONE reference the Worker owned, driving g_ref_count to
    // zero and tearing the shared isolate down under the live main engine -> the
    // main engine's run below would fail (isolate gone) or the process wedges. On
    // round-13+ each abandoned env releases exactly one reference regardless of
    // engine count, so the main engine survives.
    const N = 3;

    let hMain: number | null = null;
    let bodySucceeded = false;
    try {
      // 1. Main thread: initialize and keep a live engine.
      ffi.initialize(LIB_PATH);
      hMain = ffi.createEngine();
      const first = JSON.parse(
        ffi.runScriptEngine(hMain, "%dw 2.0\noutput application/json\n---\n6 * 7", buildInputsJson({}))
      );
      expect(first.success).toBe(true);
      expect(JSON.parse(Buffer.from(first.result, "base64").toString("utf-8"))).toBe(42);

      // 2. Worker: initialize ONCE, create N engines, run one, exit WITHOUT cleanup.
      const workerBody = `
        const { parentPort, workerData } = require('node:worker_threads');
        (async () => {
          const addon = require(workerData.addonPath);
          addon.initialize(workerData.libPath); // ONE init reference for this env
          const handles = [];
          for (let i = 0; i < workerData.n; i++) handles.push(addon.createEngine());
          const raw = addon.runScriptEngine(handles[0], workerData.script, '{}');
          const parsed = JSON.parse(raw);
          parentPort.postMessage({ ok: parsed.success !== false, count: handles.length });
          // Return WITHOUT destroyEngine/cleanup: the env dies with N engines under
          // one init reference -> env_init_cleanup releases exactly ONE reference.
        })().catch((e) => { parentPort.postMessage({ ok: false, error: String(e) }); });
      `;
      const workerMsg = await new Promise<{ ok: boolean; count?: number; error?: string }>((resolve, reject) => {
        const w = new Worker(workerBody, {
          eval: true,
          workerData: {
            addonPath: ADDON_PATH,
            libPath: LIB_PATH,
            n: N,
            script: "%dw 2.0\noutput application/json\n---\n1 + 1",
          },
        });
        let msg: { ok: boolean; count?: number; error?: string } | undefined;
        w.once("message", (m) => { msg = m; });
        w.once("error", reject);
        // Wait for EXIT (not just message) so the Worker env's death hooks
        // (env_init_cleanup) have run before we assert the main engine survived.
        w.once("exit", (code) => {
          if (code !== 0) reject(new Error("Worker exited with code " + code + (msg ? "" : " and posted no message")));
          else if (msg === undefined) reject(new Error("Worker exited 0 without posting a result"));
          else resolve(msg);
        });
      });
      expect(workerMsg.ok).toBe(true);
      expect(workerMsg.count).toBe(N);

      // 3. The Worker abandoned N engines under one init reference and its env
      // died. The main engine's reference must be intact and the isolate live.
      const second = JSON.parse(
        ffi.runScriptEngine(hMain, "%dw 2.0\noutput application/json\n---\n1 + 1", buildInputsJson({}))
      );
      expect(second.success).toBe(true);
      expect(JSON.parse(Buffer.from(second.result, "base64").toString("utf-8"))).toBe(2);

      // 4. Balance the main reference and prove the count reached exactly zero
      // (no leak, no over-release): a raw op now throws "not initialized".
      ffi.destroyEngine(hMain);
      hMain = null; // destroyed; finally must not double-destroy
      await ffi.cleanup();
      expect(() =>
        ffi.runScriptEngine(Number.MAX_SAFE_INTEGER, "%dw 2.0\noutput application/json\n---\n1", buildInputsJson({}))
      ).toThrow(/not initialized/i);
      bodySucceeded = true;
    } finally {
      // Balance global native state even if a Worker/assertion above threw, so
      // this test cannot strand a live isolate + held reference for sibling
      // integration tests (review #6 #7). Suppress a balancing-cleanup error
      // ONLY when the body already failed (so the original, more actionable
      // failure keeps propagating). When the body SUCCEEDED, a cleanup failure
      // is itself a real lifecycle regression and must fail the test rather than
      // be silently discarded (review #7 #7).
      try {
        if (hMain !== null) ffi.destroyEngine(hMain);
        await ffi.cleanup();
      } catch (cleanupErr) {
        if (bodySucceeded) throw cleanupErr;
        // else: the body is already throwing -- let that original error propagate.
      }
    }
  }, 20000);
});
