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
    "a deferred destroy (destroyEngine while an op is in flight) finalizes exactly once on the owner thread, hook removed up-front (round-1 double-owner guard)",
    async () => {
      // Regression guard for the defer-path double-owner window (round-1 fix):
      // destroyEngine on a resolver-backed engine WHILE a streaming op is in
      // flight takes the defer path -- it must remove the env cleanup hook NOW
      // (owner thread, env alive) so the draining bridge_end_op is the SOLE
      // finalizer. Deterministic: the round-11 pin is taken atomically at
      // admission, so firing destroyEngine synchronously after starting the op
      // lands AFTER in_flight==1 (see engine-handle-contract.test.ts). After the
      // op drains, bridge_end_op finalizes on the owner thread with the env alive:
      // resolver_js is deleted EXACTLY once (delta 1 -- not 0=leak, not 2=double
      // finalize) and the bridge is never stranded.
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
});
