import { describe, it, expect } from "vitest";
import { run, runTransform, cleanup } from "../../src/dataweave";

// Regression test for W-23692110 round 5 (Task 1 fix in native-lib/node/src/addon.c).
//
// Bug: napi_initialize used to block the JS thread forever whenever it ran
// while a teardown was pending on the shared native isolate and a
// streaming/transform op was still active elsewhere -- because draining that
// active op can need the very same JS thread napi_initialize was blocking.
// The fix makes napi_initialize adopt the still-live isolate instead of
// waiting, in the window before the teardown waiter thread commits to
// physical teardown.
//
// This loads the REAL native addon (no `vi.mock` of ffi) -- the deadlock is
// entirely in C and cannot be reproduced at the mocked-ffi layer.
//
// Why runTransform (not runStreaming) drives this repro: runStreaming's
// output-chunk delivery uses an unbounded napi_threadsafe_function queue, and
// g_active_ops is decremented on the background worker thread right after it
// detaches from the isolate -- independent of whether the JS event loop ever
// turns. So a blocked JS thread does NOT stop a runStreaming() op from
// draining; there is no genuine circular wait on that path (verified
// empirically: the brief's originally-suggested runStreaming shape resolves
// promptly even against pre-Task-1 addon.c, because an earlier round already
// moved that decrement off the JS thread -- see commit ac8d520).
//
// runTransform's INPUT side is different: transform_read_cb (addon.c) calls
// napi_call_threadsafe_function(w->read_tsfn, &req, napi_tsfn_blocking) and
// then genuinely blocks the background worker thread on a condition variable
// until call_js_read runs on the JS thread and signals it. That JS-thread
// callback synchronously invokes our JS read callback (a plain
// Iterable<Buffer> consumed by a sync generator) via napi_call_function --
// so firing cleanup() and a concurrent run() from *inside* that generator
// deterministically executes them while the background worker is attached
// and blocked waiting for this exact call to return. No timing assumptions
// (no setTimeout/microtask races) are needed: the call graph itself
// guarantees the ordering "worker attached and mid-read" -> "cleanup()
// fired" -> "run() fired", all on the JS thread, before the generator call
// returns and the worker can proceed.
describe("re-init during pending teardown (W-23692110, round 5 P1)", () => {
  // On the UNFIXED addon.c this deadlocks for real: the JS thread never
  // returns from run()'s napi_initialize (blocked waiting for g_active_ops to
  // drain), so the background transform worker -- itself blocked waiting for
  // the JS thread to service its read callback -- can never proceed either.
  // Vitest kills the test at the timeout below, a bounded/deterministic red.
  // On the fixed code, napi_initialize adopts the still-live isolate and
  // run() returns promptly, letting everything drain normally.
  it(
    "module-level cleanup() during an active transform read does not deadlock a concurrent run()",
    async () => {
      let fired = false;
      let cleanupPromise: Promise<void> | undefined;
      let runResult: ReturnType<typeof run> | undefined;
      let runError: unknown;

      // Large enough that, at the moment of the very first read pull, the
      // vast majority of reads (and thus the transform op) are still
      // genuinely ahead -- not a timing-sensitive assumption, since the
      // trigger below fires unconditionally on the first pull regardless of
      // how many total reads there are.
      const totalReads = 200000;

      function* input(): Generator<Buffer> {
        for (let i = 0; i < totalReads; i++) {
          if (!fired) {
            fired = true;
            // We are executing synchronously inside the native read
            // callback (call_js_read in addon.c), on the JS thread, while
            // the background transform worker thread is blocked inside
            // transform_read_cb waiting for this exact call to return.
            // Deliberately do NOT await cleanup() here, and do NOT let an
            // assertion throw from inside this generator -- a thrown
            // exception here would be caught by the native read-callback
            // wrapper and reinterpreted as a read error, silently masking a
            // real assertion failure instead of surfacing it as a test
            // failure. Capture results and assert on them after the
            // generator (and the transform) have fully drained.
            cleanupPromise = cleanup();
            try {
              runResult = run('%dw 2.0\noutput application/json\n---\n1 + 1');
            } catch (e) {
              runError = e;
            }
          }
          yield Buffer.from("x");
        }
      }

      const gen = runTransform(
        "output application/octet-stream\n---\npayload",
        input(),
        { mimeType: "application/octet-stream" }
      );

      // Drain the whole transform. On unfixed code, execution never reaches
      // here: the trigger inside input() already froze the JS thread
      // forever before the first read even returns.
      let result = await gen.next();
      while (!result.done) {
        result = await gen.next();
      }

      expect(fired).toBe(true);
      expect(runError).toBeUndefined();
      expect(runResult?.success).toBe(true);
      expect(JSON.parse(runResult!.getString()!)).toBe(2);
      expect(result.value.success).toBe(true);

      // Let both the deferred teardown/cleanup and this test settle cleanly.
      // This is essential: the process shares one native isolate across all
      // integration test files, so leaving an unresolved cleanup here would
      // perturb sibling test files.
      await cleanupPromise;
      // Idempotent final cleanup: a no-op if the singleton is already fully
      // released, leaving the module in a clean state for subsequent tests.
      await cleanup();
    },
    20000
  );
});
