import { describe, it, expect } from "vitest";
import * as ffi from "../../src/ffi";
import { findLibrary, buildInputsJson } from "../../src/utils";

// Round-7 finding #1: the synchronous napi_run_script_engine touched the
// isolate (fn_attach_thread -> fn_run_script_engine -> fn_detach_thread) with
// only a top-of-function !g_initialized fast-path and NO g_active_ops
// reservation under g_mutex. A second Worker's last cleanup() (napi_cleanup
// Case 4) could observe g_active_ops == 0 and tear down g_isolate while this
// op was attaching/executing -- a use-after-free.
//
// The genuine cross-Worker TOCTOU is not reliably forceable from single-thread
// JS (same limitation the round-6 #2 admission-during-teardown test documents:
// re-init would trigger the adoption path and cancel the pending teardown
// before the admission check runs). What we CAN assert deterministically is
// the admission-rejection path the fix introduces: once a teardown is pending
// (g_teardown_state != TEARDOWN_NONE), a freshly started run() is rejected with
// a synchronous throw rather than attaching to an isolate a concurrent teardown
// could pull out from under it. The C-level reasoning -- check-and-reserve is
// now one atomic critical section on the run() path -- is what covers the race
// itself.
//
// We drive the addon through the raw `ffi` module (not the module-level
// singleton) so the second op runs against the SAME still-live handle/isolate
// with no intervening ffi.initialize() call to trigger adoption. Calling
// ffi.cleanup() directly triggers napi_cleanup Case 5 and sets
// g_teardown_state = TEARDOWN_PENDING_WAIT synchronously, before its Promise is
// returned; the immediately-following ffi.runScriptEngine re-enters native code
// synchronously on the same callstack and deterministically observes it.
//
// Real addon, no mocking.
describe("run() admission rejected while teardown pending (round 7 #1)", () => {
  it("a synchronous run() started during pending teardown throws, not attach to a dead isolate", async () => {
    ffi.initialize(findLibrary());
    const handle = ffi.createEngine();

    // Keep one op in flight so the ref release becomes Case 5 (pending
    // teardown) rather than Case 4 (immediate teardown): use a transform whose
    // read callback triggers cleanup() and then attempts a run() on the same
    // handle, all on the same synchronous callstack.
    let cleanupPromise: Promise<void> | undefined;
    let runErr: unknown;
    let ran = false;

    let firstRead = true;
    const readCb = (_bufSize: number): Buffer | null => {
      if (firstRead) {
        firstRead = false;
        // Case 5: last ref release with g_active_ops > 0 -> TEARDOWN_PENDING_WAIT,
        // set synchronously before this returns. Not awaited.
        cleanupPromise = ffi.cleanup();
        // Synchronous run() on the same still-live handle while teardown is
        // pending. Fixed code rejects admission with a synchronous throw
        // (g_teardown_state != TEARDOWN_NONE). Must be caught here -- it is a
        // synchronous throw, not a rejected promise. Do not let it escape the
        // native read-callback body.
        try {
          ffi.runScriptEngine(
            handle,
            "%dw 2.0\noutput application/json\n---\n1 + 1",
            buildInputsJson({})
          );
          ran = true;
        } catch (e) {
          runErr = e;
        }
        return Buffer.from("[1,2,3]");
      }
      return null;
    };

    const writeCb = (_chunk: Buffer) => {};

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

    await cleanupPromise;

    // run() started while teardown was pending must have been rejected.
    expect(runErr).toBeTruthy();
    expect(ran).toBe(false);
  }, 20000);
});
