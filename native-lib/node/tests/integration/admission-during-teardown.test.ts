import { describe, it, expect } from "vitest";
import * as ffi from "../../src/ffi";
import { findLibrary, buildInputsJson } from "../../src/utils";

// Round-6 finding #2: napi_run_script_streaming_engine/napi_run_script_transform_engine
// used to read g_initialized outside g_mutex, then reserve g_active_ops in a
// LATER, separate critical section right before spawning the worker thread --
// with no reference to g_teardown_state at all. The fix folds the lifecycle
// check (including g_teardown_state) and the g_active_ops reservation into one
// atomic critical section, before any work/tsfn/promise/bridge is allocated,
// and rejects admission once a teardown is queued/underway
// (g_teardown_state != TEARDOWN_NONE), not just when the isolate is fully gone.
//
// Why this test drives the addon through the raw `ffi` module instead of the
// module-level `run`/`runStreaming`/`runTransform`/`cleanup` singleton (as the
// original brief sketch does): the module-level `cleanup()` nulls the
// singleton, so a later module-level `runStreaming()`/`runTransform()` call
// re-creates a fresh `DataWeave` instance and calls `initialize()` again.
// `napi_initialize`'s TEARDOWN_PENDING_WAIT branch (round-5's deadlock fix)
// treats that as a legitimate ADOPTION of the still-live isolate: it sets
// g_teardown_cancelled = true and cancels the pending teardown *before* the
// second op's admission check ever runs -- so by the time streaming/transform
// admission is checked, g_teardown_state is already back to TEARDOWN_NONE
// (verified empirically while developing this test: the brief's literal shape
// resolves the second op cleanly on both pre-fix and post-fix code, so it
// cannot distinguish them -- it never reaches the vulnerable window because
// the intervening initialize() call cancels the teardown as a side effect).
//
// To actually observe admission-during-pending-teardown, the second op must
// run against the SAME still-live handle/isolate WITHOUT any intervening
// ffi.initialize() call. Calling `ffi.cleanup()` directly (skipping
// `destroyEngine`) triggers exactly napi_cleanup's Case 5 (last ref release
// with an active op) and sets g_teardown_state = TEARDOWN_PENDING_WAIT
// synchronously, under g_mutex, before napi_cleanup returns its Promise to
// JS -- with no adoption path involved, since nothing calls initialize()
// afterward.
//
// Determinism: `ffi.cleanup()`'s synchronous prefix (native napi_cleanup body)
// runs entirely synchronously up to the point where it returns a Promise; the
// TEARDOWN_PENDING_WAIT transition happens on that same synchronous call, not
// after an await. The immediately-following `ffi.runScriptStreamingEngine`
// call re-enters native code synchronously (it's a plain N-API call), on the
// very same JS callstack, so it deterministically observes
// g_teardown_state == TEARDOWN_PENDING_WAIT with no timing assumptions --
// mirroring the round-5 teardown-deadlock test's use of a synchronous native
// read-callback to force deterministic ordering instead of timers.
//
// Real addon, no mocking.
describe("admission rejected while teardown pending (round 6 #2)", () => {
  it("a streaming op started on the same handle during pending teardown is rejected, not admitted", async () => {
    ffi.initialize(findLibrary());
    const handle = ffi.createEngine();

    let cleanupPromise: Promise<void> | undefined;
    let admitErr: unknown;
    let admitted = false;
    let secondOpSettled: Promise<void> = Promise.resolve();

    let firstRead = true;
    const readCb = (_bufSize: number): Buffer | null => {
      if (firstRead) {
        firstRead = false;

        // Trigger Case 5 of napi_cleanup: last release of the shared library
        // ref-count while this transform's worker is attached and
        // g_active_ops > 0. Synchronously sets g_teardown_state =
        // TEARDOWN_PENDING_WAIT before returning. Not awaited -- the point is
        // to observe the state it leaves behind, not its eventual settlement.
        cleanupPromise = ffi.cleanup();

        // Attempt a second admission on the SAME still-live handle/isolate
        // while teardown is pending. Fixed code rejects admission with a
        // synchronous napi_throw_error (the atomic admission check sees
        // g_teardown_state != TEARDOWN_NONE, before any promise is even
        // created). Pre-fix code admits it: the unlocked g_initialized check
        // passes (the isolate genuinely hasn't been torn down yet --
        // TEARDOWN_PENDING_WAIT hasn't reached physical teardown) and
        // g_active_ops is reserved without ever consulting g_teardown_state,
        // so the call returns a promise that goes on to resolve successfully.
        //
        // On rejection, napi_throw_error fires synchronously from this very
        // call (admission fails before any promise is created), so it must
        // be caught here rather than only via a rejected-promise `.then` --
        // mirroring the round-5 teardown-deadlock test's care not to let a
        // thrown exception escape a native read-callback body (it would be
        // reinterpreted as a read error, masking the real outcome).
        try {
          secondOpSettled = ffi
            .runScriptStreamingEngine(
              handle,
              "%dw 2.0\noutput application/json\n---\n[1,2,3]",
              buildInputsJson({}),
              () => {}
            )
            .then(
              () => { admitted = true; },
              (e) => { admitErr = e; }
            );
        } catch (e) {
          admitErr = e;
        }

        return Buffer.from("[1,2,3]");
      }
      return null; // EOF after the first chunk
    };

    const chunks: Buffer[] = [];
    const writeCb = (chunk: Buffer) => { chunks.push(chunk); };

    const resultRaw = await ffi.runScriptTransformEngine(
      handle,
      "output application/json\n---\npayload",
      "{}",
      "payload",
      "application/json",
      null,
      readCb,
      writeCb
    );
    const result = JSON.parse(resultRaw);
    expect(result.success).toBe(true);

    // Let the second op settle (whichever branch it took) before asserting,
    // and drain the pending teardown so the shared native isolate is left in
    // a clean, consistent state for sibling test files in this process.
    await secondOpSettled;
    await cleanupPromise;

    // The second op admitted while teardown was pending must have been
    // rejected, not silently admitted against an isolate a concurrent
    // teardown could tear down out from under it.
    expect(admitErr).toBeTruthy();
    expect(admitted).toBe(false);
  }, 20000);
});
