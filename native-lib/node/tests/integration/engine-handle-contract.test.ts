import { describe, it, expect, beforeAll, afterAll } from "vitest";
import * as ffi from "../../src/ffi";
import { findLibrary, buildInputsJson } from "../../src/utils";

// W-23692110 round 11 finding #6.
//
// The Java `ScriptRuntimeTest` only asserts on the UNKNOWN_ENGINE_HANDLE_JSON
// constant -- the @CEntryPoint methods it wraps cannot run in a hosted JVM, so
// nothing has ever driven the real `*_engine` entrypoints through the
// compiled addon against an unknown or destroyed handle. This file closes
// that gap: it loads the REAL addon (no `vi.mock` of ffi) and drives
// `runScriptEngine` / `runScriptStreamingEngine` / `runScriptTransformEngine`
// directly through the raw `ffi` module -- the addon boundary the finding is
// about -- against handles that were never registered and against handles
// that were registered and then destroyed.
//
// Confirmed empirically (see task-6-report.md) against the real addon:
//   - sync `runScriptEngine` RETURNS the JSON string
//     `{"success":false,"error":"Unknown engine handle"}` -- it does not throw.
//   - `runScriptStreamingEngine` / `runScriptTransformEngine` RESOLVE (never
//     reject) their promise with that same JSON string as the terminal
//     metadata; no chunk callback fires for an unknown/destroyed handle.
// This is the same envelope produced by NativeLib.UNKNOWN_ENGINE_HANDLE_JSON
// on the Java side (native-lib/src/main/java/org/mule/weave/lib/NativeLib.java),
// threaded back through addon.c's engine entrypoints and unmodified by the TS
// parsing layer (parseNativeResponse / parseStreamingResult in src/result.ts).
//
// The native addon globals (g_ref_count, g_initialized, g_bridges, etc.) are
// process-wide C statics -- vitest's per-file module isolation does NOT reset
// them, and napi_initialize/napi_cleanup are plain integer ref-counts (one
// increment per initialize(), one decrement per cleanup(), teardown only on
// the transition to zero). So this file calls ffi.initialize() exactly ONCE
// for the whole suite (beforeAll), balanced by exactly one ffi.cleanup() that
// brings the ref count to zero (in the last real test, "final cleanup..."
// below) -- mirroring independent-engines.test.ts's single
// initialize()/cleanup() pair rather than handle-validation.test.ts's
// per-test balancing (that file calls initialize()/cleanup() once per test,
// which does not fit here since several tests below deliberately build on a
// still-live engine/isolate from a prior test). The trailing afterAll is a
// pure safety net (idempotent no-op on the happy path) in case an earlier
// assertion throws before the drainage test runs, so this file never strands
// a ref-count bump for sibling integration test files sharing the same
// vitest worker process.
describe("*_engine unknown/destroyed-handle contract (round 11 #6)", () => {
  beforeAll(() => {
    ffi.initialize(findLibrary());
  });

  afterAll(async () => {
    // Idempotent: a no-op if the ref count already reached zero (the normal
    // case -- the drainage test below already did that). A genuine safety
    // net only if an earlier test threw before reaching that point.
    await ffi.cleanup();
  });

  // A handle value that was never handed out by createEngine()/
  // createEngineWithResolver() (those only ever return small positive
  // handles from the Java-side registry) and can never collide with one.
  const UNKNOWN_HANDLE = Number.MAX_SAFE_INTEGER;
  const UNKNOWN_ENVELOPE = { success: false, error: "Unknown engine handle" };

  it("runScriptEngine on a never-registered handle returns the terminal envelope, does not throw", () => {
    let raw: string | undefined;
    expect(() => {
      raw = ffi.runScriptEngine(
        UNKNOWN_HANDLE,
        "%dw 2.0\noutput application/json\n---\n1 + 1",
        buildInputsJson({})
      );
    }).not.toThrow();

    expect(JSON.parse(raw!)).toEqual(UNKNOWN_ENVELOPE);
  });

  it("runScriptStreamingEngine on a never-registered handle resolves (never rejects) with the terminal envelope", async () => {
    const chunks: Buffer[] = [];
    const raw = await ffi.runScriptStreamingEngine(
      UNKNOWN_HANDLE,
      "%dw 2.0\noutput application/json\n---\n[1, 2, 3]",
      buildInputsJson({}),
      (chunk) => chunks.push(chunk)
    );

    expect(JSON.parse(raw)).toEqual(UNKNOWN_ENVELOPE);
    // No output was ever produced for an engine that doesn't exist.
    expect(chunks).toHaveLength(0);
  });

  it("runScriptTransformEngine on a never-registered handle resolves (never rejects) with the terminal envelope", async () => {
    let readCalls = 0;
    let firstRead = true;
    const readCb = (_bufSize: number): Buffer | null => {
      readCalls++;
      if (firstRead) {
        firstRead = false;
        return Buffer.from("1");
      }
      return null;
    };
    const chunks: Buffer[] = [];
    const writeCb = (chunk: Buffer) => chunks.push(chunk);

    const raw = await ffi.runScriptTransformEngine(
      UNKNOWN_HANDLE,
      "output application/json\n---\npayload",
      "{}",
      "payload",
      "application/json",
      null,
      readCb,
      writeCb
    );

    expect(JSON.parse(raw)).toEqual(UNKNOWN_ENVELOPE);
    // The unknown-handle rejection happens before a worker is ever spawned,
    // so the read/write callbacks are never invoked.
    expect(readCalls).toBe(0);
    expect(chunks).toHaveLength(0);
  });

  it("all three entrypoints on a destroyed handle return/resolve the same terminal envelope, after proving the handle worked", async () => {
    const handle = ffi.createEngine();

    // Prove the handle is genuinely live before destroying it.
    const preDestroy = JSON.parse(
      ffi.runScriptEngine(handle, "%dw 2.0\noutput application/json\n---\n1 + 1", buildInputsJson({}))
    );
    expect(preDestroy.success).toBe(true);

    ffi.destroyEngine(handle);

    // Sync entrypoint: returns the envelope, does not throw.
    let syncRaw: string | undefined;
    expect(() => {
      syncRaw = ffi.runScriptEngine(handle, "%dw 2.0\noutput application/json\n---\n1", buildInputsJson({}));
    }).not.toThrow();
    expect(JSON.parse(syncRaw!)).toEqual(UNKNOWN_ENVELOPE);

    // Streaming entrypoint: resolves with the envelope.
    const streamChunks: Buffer[] = [];
    const streamRaw = await ffi.runScriptStreamingEngine(
      handle,
      "%dw 2.0\noutput application/json\n---\n[1, 2, 3]",
      buildInputsJson({}),
      (chunk) => streamChunks.push(chunk)
    );
    expect(JSON.parse(streamRaw)).toEqual(UNKNOWN_ENVELOPE);
    expect(streamChunks).toHaveLength(0);

    // Transform entrypoint: resolves with the envelope.
    let transformReadCalls = 0;
    let transformFirstRead = true;
    const transformReadCb = (_bufSize: number): Buffer | null => {
      transformReadCalls++;
      if (transformFirstRead) {
        transformFirstRead = false;
        return Buffer.from("1");
      }
      return null;
    };
    const transformChunks: Buffer[] = [];
    const transformRaw = await ffi.runScriptTransformEngine(
      handle,
      "output application/json\n---\npayload",
      "{}",
      "payload",
      "application/json",
      null,
      transformReadCb,
      (chunk) => transformChunks.push(chunk)
    );
    expect(JSON.parse(transformRaw)).toEqual(UNKNOWN_ENVELOPE);
    expect(transformReadCalls).toBe(0);
    expect(transformChunks).toHaveLength(0);
  });

  // Same-thread post-admission ordering (deterministic, not best-effort):
  // destroyEngine() is fired synchronously immediately after admission of the
  // op (right after starting runScriptStreamingEngine, before awaiting it).
  // The round-11 #2/#3 pin is taken atomically at admission, under g_mutex, in
  // bridge_begin_op_locked -- so this same-thread ordering deterministically
  // lands AFTER the pin is already held. That means the op MUST complete
  // successfully with complete chunks; there is no closed set of "success or
  // Unknown-engine-handle envelope" to tolerate here, because the envelope can
  // only arise if the pin were NOT held at admission. Requiring success (and
  // no longer accepting the envelope) makes this test fail if a future
  // regression drops the admission-time pin, instead of silently passing by
  // returning the accepted terminal envelope.
  //
  // Genuinely concurrent cross-thread interleavings (a real Worker racing
  // destroyEngine() against admission on a different thread) are a distinct,
  // non-deterministic window that this same-thread ordering does not exercise
  // and cannot stand in for. That case remains covered best-effort by the
  // forthcoming Worker-based suite (Task 8), matching the documented posture
  // of rounds 5-10's cross-Worker races (see run-admission.test.ts /
  // admission-during-teardown.test.ts) -- it is not tolerated away in this
  // test.
  it(
    "destroyEngine() fired right after admission of an in-flight streaming op deterministically succeeds (pin held at admission)",
    async () => {
      const ITERATIONS = 50;
      for (let i = 0; i < ITERATIONS; i++) {
        const handle = ffi.createEngine();
        const chunks: Buffer[] = [];
        const resultPromise = ffi.runScriptStreamingEngine(
          handle,
          "%dw 2.0\noutput application/json\n---\n[1, 2, 3]",
          buildInputsJson({}),
          (chunk) => chunks.push(chunk)
        );
        // Fire destroy immediately after admission, before awaiting. The round-11
        // pin is taken atomically at admission (under g_mutex, in
        // bridge_begin_op_locked), so this ordering lands AFTER the pin and the
        // op MUST complete successfully. Requiring success (not tolerating the
        // Unknown-engine-handle envelope) makes this test fail if a regression
        // drops the admission-time pin.
        expect(() => ffi.destroyEngine(handle)).not.toThrow();

        const raw = await resultPromise;
        const parsed = JSON.parse(raw);
        expect(parsed.success).toBe(true);
        expect(JSON.parse(Buffer.concat(chunks).toString("utf-8"))).toEqual([1, 2, 3]);
      }
    },
    60000
  );

  it("deferred registry removal after an in-flight op finalizes without wedging the isolate (round 12 #3)", async () => {
    // Uses the shared beforeAll isolate. Create an engine, start a streaming
    // op, destroy the engine while the op is admitted, drain the op. The
    // deferred finalize (bridge_end_op -> bridge_finalize_registry) must
    // complete and a subsequent run on a fresh engine must still work
    // (isolate not torn down / not wedged by the transient reservation).
    const handle = ffi.createEngine();
    const chunks: Buffer[] = [];
    const resultPromise = ffi.runScriptStreamingEngine(
      handle,
      "%dw 2.0\noutput application/json\n---\n[1, 2, 3]",
      buildInputsJson({}),
      (chunk) => chunks.push(chunk)
    );
    expect(() => ffi.destroyEngine(handle)).not.toThrow();
    const raw = await resultPromise;
    const parsed = JSON.parse(raw);
    // Pin held at admission (round 11) -> success expected; either way no crash.
    if (parsed.success) {
      expect(JSON.parse(Buffer.concat(chunks).toString("utf-8"))).toEqual([1, 2, 3]);
    }
    // Isolate still healthy after the deferred finalize ran:
    const h2 = ffi.createEngine();
    const envelope = JSON.parse(
      ffi.runScriptEngine(h2, "%dw 2.0\noutput application/json\n---\n2 + 2", buildInputsJson({}))
    );
    expect(envelope.success).toBe(true);
    expect(JSON.parse(Buffer.from(envelope.result, "base64").toString("utf-8"))).toBe(4);
    ffi.destroyEngine(h2);
  });

  it("destroyEngine on an unknown / already-destroyed handle is a safe no-op and leaves the isolate healthy (review #10 #5)", () => {
    // Drives napi_destroy_engine's `found == NULL` else branch on a LIVE isolate
    // (the shared beforeAll isolate). Round-10 #5 added a teardown-state guard
    // there so the direct registry-removal attach reads g_isolate/g_teardown_state
    // under g_mutex and pins the isolate before fn_attach_thread, mirroring
    // bridge_finalize_registry. On a live isolate this is a benign no-op; the test
    // asserts it does not throw or wedge the isolate. (The crash it guards against
    // -- fn_attach_thread on a NULL or TEARDOWN_TEARING_DOWN isolate -- only arises
    // when destroyEngine races a concurrent cleanup() teardown, a non-deterministic
    // cross-thread window not reproducible through this single-threaded public API;
    // see this file's note above on best-effort race coverage.)
    expect(() => ffi.destroyEngine(UNKNOWN_HANDLE)).not.toThrow();

    // Double-destroy: the second call finds no record and takes the same
    // else branch. Must not throw or corrupt the isolate.
    const handle = ffi.createEngine();
    expect(() => ffi.destroyEngine(handle)).not.toThrow();
    expect(() => ffi.destroyEngine(handle)).not.toThrow();

    // Isolate remains fully usable after both no-op destroys.
    const h2 = ffi.createEngine();
    const envelope = JSON.parse(
      ffi.runScriptEngine(h2, "%dw 2.0\noutput application/json\n---\n3 + 4", buildInputsJson({}))
    );
    expect(envelope.success).toBe(true);
    expect(JSON.parse(Buffer.from(envelope.result, "base64").toString("utf-8"))).toBe(7);
    ffi.destroyEngine(h2);
  });

  it("final cleanup drains the shared isolate (idempotent)", async () => {
    // Exactly one ffi.initialize() ran for this whole file (beforeAll), so
    // this is the ONE balancing ffi.cleanup() that brings the native
    // g_ref_count to zero and genuinely tears the isolate down (napi_cleanup
    // Case 4, since no op is in flight) -- not a no-op decrement of a
    // still-positive count left over from other tests. Prove that teardown
    // actually happened, not just that the call resolved: a subsequent
    // engine-level call must now observe "not initialized" rather than
    // silently succeeding against a still-live isolate.
    await ffi.cleanup();

    expect(() =>
      ffi.runScriptEngine(UNKNOWN_HANDLE, "%dw 2.0\noutput application/json\n---\n1", buildInputsJson({}))
    ).toThrow(/not initialized/i);

    // review #10 #5: with the isolate torn down (g_isolate == NULL,
    // g_initialized == 0), destroyEngine on an unknown handle must be a safe
    // no-op and must NOT attach to the now-NULL global isolate -- it returns
    // early at the !g_initialized guard. The process must survive.
    expect(() => ffi.destroyEngine(UNKNOWN_HANDLE)).not.toThrow();

    // A second cleanup() call after the ref count already reached zero must
    // remain a safe no-op, mirroring independent-engines.test.ts's final
    // teardown discipline.
    await expect(ffi.cleanup()).resolves.toBeUndefined();
  });
});
