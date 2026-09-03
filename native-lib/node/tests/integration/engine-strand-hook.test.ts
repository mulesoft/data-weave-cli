import { describe, it, expect, afterAll } from "vitest";
import { Worker } from "node:worker_threads";
import { join } from "node:path";
import { findLibrary } from "../../src/utils";

// W-23692110 PR #157 reviews #12 #3 / #13.
//
// napi_destroy_engine used to remove the env cleanup hook and THEN finalize. If
// the finalize took a live-isolate strand (fn_attach_thread failed while the
// isolate was still alive) the bridge was enqueued on g_stranded_bridges WITHOUT
// deleting resolver_js. drain_stranded_bridges() later runs off the owner thread
// and frees with env_still_alive=false -- SKIPPING napi_delete_reference -- so the
// resolver JS function leaked while the owner env was still alive.
//
// The fix keeps the napi_ref deletion owned by the owner-thread env cleanup hook
// whenever a strand happens on the owner thread with the env alive: the bridge is
// kept by its (still-registered) cleanup hook instead of being enqueued on
// g_stranded_bridges, so the ref is deleted on the owner thread at env teardown.
//
// These tests use the addon's test-only entrypoints (registered only when
// DATAWEAVE_TEST_HOOKS is set -- the integration lane sets it, see
// vitest.config.ts) to deterministically force a SINGLE live-isolate strand:
//   - __test_forceStrandOnce():        arm one forced strand in the next
//                                       bridge_finalize_registry (simulates the
//                                       fn_attach_thread failure).
//   - __test_strandedCount():          length of g_stranded_bridges.
//   - __test_resolverRefDeleteCount(): process-wide count of owner-thread
//                                       napi_delete_reference(resolver_js) calls.

const ADDON_PATH = join(__dirname, "..", "..", "build", "Release", "dwlib_addon.node");
const LIB_PATH = findLibrary();

interface TestAddon {
  initialize(libPath: string): void;
  createEngineWithResolver(resolver: (p: string) => string | null): number;
  destroyEngine(handle: number): void;
  runScriptStreamingEngine(
    handle: number,
    script: string,
    inputsJson: string,
    chunkCb: (chunk: Buffer) => void
  ): Promise<string>;
  cleanup(): Promise<void>;
  __test_forceStrandOnce(): void;
  __test_strandedCount(): number;
  __test_resolverRefDeleteCount(): number;
}

// eslint-disable-next-line @typescript-eslint/no-var-requires
const addon = require(ADDON_PATH) as TestAddon;

describe("owner-env-hook-retained finalization for stranded resolver bridges (review #12 #3, #13)", () => {
  afterAll(async () => {
    // Balance any main-thread init reference this file took so it does not
    // perturb sibling integration files sharing the vitest worker process.
    await addon.cleanup();
  });

  it("exposes the test-only strand hooks (integration lane sets DATAWEAVE_TEST_HOOKS)", () => {
    // Guards against a silently-inert test: if the hooks were not registered the
    // strand assertions below would all trivially pass with 0-deltas.
    expect(typeof addon.__test_forceStrandOnce).toBe("function");
    expect(typeof addon.__test_strandedCount).toBe("function");
    expect(typeof addon.__test_resolverRefDeleteCount).toBe("function");
  });

  it(
    "a live-isolate strand during destroyEngine keeps the resolver bridge owned by its env cleanup hook, NOT on g_stranded_bridges",
    async () => {
      addon.initialize(LIB_PATH);
      try {
        const strandedBefore = addon.__test_strandedCount();

        const handle = addon.createEngineWithResolver((_p) => null);
        // Arm exactly one forced strand: the next bridge_finalize_registry (the
        // one inside this destroyEngine) reports the destroy as skipped with the
        // isolate still live -- the exact fault the finding is about.
        addon.__test_forceStrandOnce();
        expect(() => addon.destroyEngine(handle)).not.toThrow();

        const strandedAfter = addon.__test_strandedCount();
        // POST-FIX: the strand is kept by the owner-env cleanup hook, so the
        // bridge is NOT enqueued on g_stranded_bridges (delta 0). PRE-FIX: the
        // bridge was stranded (delta 1) with resolver_js left undeleted, and the
        // hook had already been removed -> the ref would leak.
        expect(strandedAfter - strandedBefore).toBe(0);
      } finally {
        // Release this test's init reference. The kept-hook bridge is finalized
        // (ref deleted, record freed) by the main env's cleanup hook at process
        // teardown; the isolate teardown here makes that a safe no-op registry
        // removal.
        await addon.cleanup();
      }
    }
  );

  it(
    "a strand in a Worker that then exits deletes the resolver ref on the owner thread (not leaked, not drained undeleted)",
    async () => {
      // Main thread holds an init reference so the shared isolate stays live
      // while the Worker's env tears down -- the Worker's env cleanup hook needs a
      // live isolate to remove the Java registry entry, and (post-fix) to delete
      // resolver_js on the Worker's owner thread.
      addon.initialize(LIB_PATH);
      try {
        const deletesBefore = addon.__test_resolverRefDeleteCount();

        const body = `
          const { parentPort, workerData } = require('node:worker_threads');
          const addon = require(workerData.addonPath);
          addon.initialize(workerData.libPath); // this env's own init reference
          const handle = addon.createEngineWithResolver((p) => null);
          addon.__test_forceStrandOnce();
          addon.destroyEngine(handle);
          // Report the stranded-list delta observed on the Worker thread right
          // after the forced strand, then return WITHOUT cleanup() so the env
          // cleanup hook fires as this Worker env tears down (post-fix: deletes
          // resolver_js on THIS owner thread).
          parentPort.postMessage({ strandedCount: addon.__test_strandedCount() });
        `;
        const workerMsg = await new Promise<{ strandedCount: number }>((resolve, reject) => {
          const w = new Worker(body, {
            eval: true,
            workerData: { addonPath: ADDON_PATH, libPath: LIB_PATH },
          });
          let msg: { strandedCount: number } | undefined;
          w.once("message", (m) => { msg = m; });
          w.once("error", reject);
          // Wait for EXIT (not just the message) so the Worker env's cleanup hooks
          // have run before we read the process-wide delete counter.
          w.once("exit", (code) => {
            if (code !== 0) reject(new Error("Worker exited with code " + code + (msg ? "" : " and posted no message")));
            else if (msg === undefined) reject(new Error("Worker exited 0 without posting a result"));
            else resolve(msg);
          });
        });

        // POST-FIX: the Worker's strand was kept by its cleanup hook (not stranded).
        expect(workerMsg.strandedCount).toBe(0);

        const deletesAfter = addon.__test_resolverRefDeleteCount();
        // POST-FIX: the Worker's env cleanup hook deleted resolver_js on the
        // Worker's owner thread at env teardown -> exactly one net delete.
        // PRE-FIX: destroyEngine removed the hook and stranded the bridge, so the
        // off-thread drain freed it with env_still_alive=false -> 0 deletes (leak).
        expect(deletesAfter - deletesBefore).toBe(1);
      } finally {
        await addon.cleanup();
      }
    },
    20000
  );

  it(
    "SMOKE (env alive): a deferred destroy with the op completing normally finalizes exactly once on the owner thread",
    async () => {
      // HONEST SCOPE (round-2): this is a happy-path SMOKE test, NOT a regression
      // guard for the defer-branch double-owner fix. It runs on the main thread and
      // lets the op complete with the env alive, so bridge_end_op runs with
      // env_still_alive=TRUE and bridge_finalize's FREE path removes the hook itself
      // regardless of whether destroyEngine's defer branch removed it -- so it
      // passes with OR without the b06b917 defer-branch hook-removal and cannot
      // catch that regression. The double-owner UAF only manifests when
      // env_still_alive=FALSE (env torn down mid-flight); that path is exercised by
      // the Worker test below. What this does verify: the defer path (in_flight>0 at
      // destroy time; round-11 pin taken atomically at admission) drains cleanly,
      // finalizes exactly once (resolver_js deleted delta 1 -- not 0=leak, not
      // 2=double finalize), and never strands.
      addon.initialize(LIB_PATH);
      try {
        const deletesBefore = addon.__test_resolverRefDeleteCount();
        const strandedBefore = addon.__test_strandedCount();

        const handle = addon.createEngineWithResolver((_p) => null);
        const chunks: Buffer[] = [];
        const resultPromise = addon.runScriptStreamingEngine(
          handle,
          "%dw 2.0\noutput application/json\n---\n[1, 2, 3]",
          "{}",
          (chunk) => chunks.push(chunk)
        );
        // Fire destroy synchronously after admission -> defer path (in_flight==1).
        expect(() => addon.destroyEngine(handle)).not.toThrow();
        const raw = await resultPromise;
        // Pin held at admission -> the op completes successfully.
        expect(JSON.parse(raw).success).toBe(true);

        const deletesAfter = addon.__test_resolverRefDeleteCount();
        const strandedAfter = addon.__test_strandedCount();
        expect(strandedAfter - strandedBefore).toBe(0);
        expect(deletesAfter - deletesBefore).toBe(1);
      } finally {
        await addon.cleanup();
      }
    },
    20000
  );

  it(
    "ROBUSTNESS (defer then Worker terminate mid-flight): the shared isolate/native state survives; state stays consistent",
    async () => {
      // HONEST SCOPE (round-2): this exercises the defer-then-terminate lifecycle
      // safely, but it is NOT a regression guard for the b06b917 defer-branch
      // hook-removal fix -- it passes WITH and WITHOUT that fix (verified: see the
      // report round-2 section for the empirical revert-check). It cannot open the
      // double-owner window because that window requires bridge_end_op to run with
      // env_still_alive=FALSE and take its FREE path (which skips the env-gated hook
      // removal) so a still-registered hook then fires on the freed bridge. Reaching
      // that free needs EITHER:
      //   (1) the background compute thread still running at teardown so its sentinel
      //       enqueue returns napi_closing and it runs bridge_end_op(false) itself --
      //       but that is an orphaned GraalVM-attached thread which aborts the process
      //       (SIGABRT) on completion, independent of the hook (see report); OR
      //   (2) Node draining the queued completion sentinel with env==NULL on the JS
      //       thread at teardown -- but worker.terminate() DROPS the queued
      //       threadsafe-function callback rather than draining it, so bridge_end_op
      //       never runs, in_flight stays pinned, and bridge_env_cleanup hits its
      //       in_flight>0 branch (addon.c ~L562) which DEFERS instead of freeing --
      //       no free, no UAF, with or without the fix.
      // So the env_still_alive=FALSE FREE-then-hook-fire window is not reachable from
      // JS here without the orthogonal orphaned-thread abort. This test therefore only
      // asserts that the defer+terminate path leaves the shared isolate and native
      // registry intact (a real UAF / double fn_destroy_engine that did not crash
      // outright would corrupt them). The double-owner fix's correctness rests on the
      // code review of the four invariants (see report), not on this test.
      //
      // The op uses a trivial fast script so the background thread finishes and
      // DETACHES from the isolate well before terminate() -- avoiding the orphaned-
      // thread abort of variant (1) above -- and the Worker blocks its JS event loop
      // so the completion sentinel stays queued (never processed while alive).
      const ITERATIONS = 10;
      for (let i = 0; i < ITERATIONS; i++) {
        const body = `
          const { parentPort, workerData } = require('node:worker_threads');
          const addon = require(workerData.addonPath);
          addon.initialize(workerData.libPath);
          const handle = addon.createEngineWithResolver((p) => null);
          // Trivial op: the background compute thread finishes and detaches fast,
          // then enqueues the completion sentinel (queued, not yet processed).
          addon
            .runScriptStreamingEngine(handle, "%dw 2.0\\noutput application/json\\n---\\n[1,2,3]", '{}', (c) => {})
            .then(() => {}, () => {});
          // destroyEngine synchronously after admission -> in_flight==1 -> DEFER.
          addon.destroyEngine(handle);
          parentPort.postMessage('deferred');
          // Block the JS event loop so the queued sentinel is NOT processed while
          // the env is alive; the parent terminates us during this window.
          const end = Date.now() + 500; while (Date.now() < end) {}
        `;
        const w = new Worker(body, {
          eval: true,
          workerData: { addonPath: ADDON_PATH, libPath: LIB_PATH },
        });
        const workerError = new Promise<never>((_, reject) => w.once("error", reject));
        workerError.catch(() => {}); // avoid unhandled rejection if it fires post-settle
        await Promise.race([
          new Promise<void>((resolve) => w.once("message", (m) => { if (m === "deferred") resolve(); })),
          workerError,
          new Promise((_, reject) => setTimeout(() => reject(new Error("worker did not signal deferred in time")), 10000)),
        ]);
        // Small settle so the background thread has finished and DETACHED (sentinel
        // queued) before we terminate -- terminate then lands after the isolate is
        // no longer attached on the worker's compute thread (no orphaned thread).
        await new Promise((r) => setTimeout(r, 100));
        const exitCode = await w.terminate();
        expect(typeof exitCode).toBe("number");
      }

      // Prove the shared isolate/native state survived every terminate: the main
      // thread must still initialize + create + destroy an engine cleanly (a UAF or
      // double fn_destroy_engine that did not crash outright would corrupt the
      // registry/isolate and break this).
      addon.initialize(LIB_PATH);
      const h = addon.createEngineWithResolver((_p) => null);
      expect(() => addon.destroyEngine(h)).not.toThrow();
      await addon.cleanup();
    },
    60000
  );
});
