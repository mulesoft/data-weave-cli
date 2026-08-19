import { describe, it, expect, afterEach } from "vitest";
import { DataWeave } from "../../src/dataweave";
import { DataWeaveError } from "../../src/errors";
import * as ffi from "../../src/ffi";
import { findLibrary, buildInputsJson } from "../../src/utils";

// Same-instance lifecycle regression tests (round 6, W-23692110). Round 5's
// coverage used a second instance; the same-instance cleanup window is exactly
// what findings #1 and #3 exploit. Real addon, no mocking.
describe("instance lifecycle during cleanup (round 6)", () => {
  let dw: DataWeave | undefined;
  afterEach(async () => {
    // Whatever state each test leaves it in, drain and release so the shared
    // process-wide isolate is clean for sibling tests.
    if (dw) {
      try { await dw.cleanup(); } catch { /* already released */ }
      dw = undefined;
    }
  });

  // Finding #3: initialize() during the same instance's pending cleanup must
  // reject deterministically, not be a silent no-op that leaves the instance
  // uninitialized after cleanup settles.
  it("initialize() during pending cleanup throws, and re-init works after cleanup settles", async () => {
    dw = new DataWeave();
    dw.initialize();
    const closing = dw.cleanup(); // not awaited: instance is now "cleaning-up"
    expect(() => dw!.initialize()).toThrow(DataWeaveError);
    expect(() => dw!.initialize()).toThrow(/cleanup is in progress/i);
    await closing; // now "uninitialized"
    // Explicit re-init now succeeds and the instance is usable again.
    dw.initialize();
    const r = dw.run("%dw 2.0\noutput application/json\n---\n1 + 1");
    expect(r.success).toBe(true);
    expect(JSON.parse(r.getString()!)).toBe(2);
  });

  // Finding #1: run() during the cleanup window must throw a clean DataWeaveError
  // (never send a null handle to C), because doCleanup() nulls engineHandle
  // synchronously before awaiting native cleanup.
  it("run() during pending cleanup throws DataWeaveError, not a native/null-handle error", async () => {
    dw = new DataWeave();
    dw.initialize();
    const closing = dw.cleanup();
    expect(() => dw!.run("%dw 2.0\noutput application/json\n---\n1")).toThrow(DataWeaveError);
    expect(() => dw!.run("%dw 2.0\noutput application/json\n---\n1")).toThrow(/cleaning up/i);
    await closing;
  });

  // Finding #1, streaming/transform variants: the async generators must reject
  // on first pull when started during the cleanup window.
  it("runStreaming()/runTransform() during pending cleanup reject on first pull", async () => {
    dw = new DataWeave();
    dw.initialize();
    const closing = dw.cleanup();

    const sgen = dw.runStreaming("%dw 2.0\noutput application/json\n---\n[1,2,3]");
    await expect(sgen.next()).rejects.toThrow(DataWeaveError);

    const tgen = dw.runTransform(
      "output application/json\n---\npayload",
      [Buffer.from("[1,2,3]")],
      { mimeType: "application/json" }
    );
    await expect(tgen.next()).rejects.toThrow(DataWeaveError);

    await closing;
  });

  // Idempotency preserved: cleanup() before initialize() is a no-op; double
  // cleanup() coalesces (round-4 F1 must survive this refactor).
  it("cleanup() is a no-op when uninitialized and coalesces when called twice", async () => {
    dw = new DataWeave();
    await expect(dw.cleanup()).resolves.toBeUndefined(); // uninitialized no-op
    dw.initialize();
    const a = dw.cleanup();
    const b = dw.cleanup(); // must return the same in-flight settlement, one native teardown
    await Promise.all([a, b]);
  });
});

// Round 12, Task 1: napi_cleanup's Case 1..5 decrement-and-teardown body was
// lifted verbatim into release_isolate_ref_locked() so a later task (round-12
// #2) can reuse it from the abandoned-env path. This is a behavior-preserving
// refactor; this test pins the observable contract it must not disturb: the
// balancing cleanup() call that drops the ref count to zero must actually
// tear the isolate down synchronously, not leave it silently live.
//
// Driven through the raw `ffi` boundary (like handle-validation.test.ts and
// engine-handle-contract.test.ts), with a balanced initialize()/cleanup()
// pair, so this file doesn't leak a ref-count bump into sibling integration
// test files sharing the same vitest worker process.
describe("napi_cleanup refactor preserves last-release teardown (round 12 Task 1)", () => {
  it("the balancing cleanup() actually tears the isolate down (subsequent engine call sees not-initialized)", async () => {
    ffi.initialize(findLibrary());
    const h = ffi.createEngine();
    const envelope = JSON.parse(
      ffi.runScriptEngine(h, "%dw 2.0\noutput application/json\n---\n1 + 1", buildInputsJson({}))
    );
    expect(envelope.success).toBe(true);
    expect(JSON.parse(Buffer.from(envelope.result, "base64").toString("utf-8"))).toBe(2);
    ffi.destroyEngine(h);
    await ffi.cleanup();
    // Ref count reached 0 and the isolate was torn down: a fresh engine call
    // must observe "not initialized", not silently run on a live isolate.
    expect(() =>
      ffi.runScriptEngine(Number.MAX_SAFE_INTEGER, "%dw 2.0\noutput application/json\n---\n1", buildInputsJson({}))
    ).toThrow(/not initialized/i);
  });
});
