import { describe, it, expect, afterAll } from "vitest";
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
// them. Every ffi.initialize() here is balanced by a final ffi.cleanup() (via
// afterAll) so this file doesn't strand a ref-count bump or a leaked engine
// bridge for sibling integration test files sharing the same vitest worker
// process, mirroring independent-engines.test.ts and handle-validation.test.ts.
describe("*_engine unknown/destroyed-handle contract (round 11 #6)", () => {
  afterAll(async () => {
    // Idempotent: a no-op if nothing is left to release.
    await ffi.cleanup();
  });

  // A handle value that was never handed out by createEngine()/
  // createEngineWithResolver() (those only ever return small positive
  // handles from the Java-side registry) and can never collide with one.
  const UNKNOWN_HANDLE = Number.MAX_SAFE_INTEGER;
  const UNKNOWN_ENVELOPE = { success: false, error: "Unknown engine handle" };

  it("runScriptEngine on a never-registered handle returns the terminal envelope, does not throw", () => {
    ffi.initialize(findLibrary());

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
    ffi.initialize(findLibrary());

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
    ffi.initialize(findLibrary());

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
    ffi.initialize(findLibrary());
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

  // Best-effort probabilistic guard (green on fixed code, cannot false-fail
  // on it) -- matching the documented posture of rounds 5-10's cross-Worker
  // races (see run-admission.test.ts / admission-during-teardown.test.ts):
  // the exact interleaving of a concurrent destroyEngine() against the
  // admission window of an in-flight streaming/transform op on the SAME
  // handle is not deterministically forceable from JS.
  //
  // This harness has no existing `worker_threads` pattern to reuse (checked:
  // no test file under tests/integration uses `worker_threads`/`Worker`), and
  // spinning up a real Worker here would still race the SAME non-deterministic
  // window -- it would not make the interleaving forceable, only add overhead
  // and flakiness risk without truer coverage. Instead this uses the closest
  // deterministic proxy available on a single thread: destroyEngine() is
  // fired synchronously immediately after admission of the op (right after
  // starting runScriptStreamingEngine, before awaiting it), which is exactly
  // when a genuinely concurrent Worker's destroyEngine() would most plausibly
  // land relative to the round-11 #2/#3 pin taken under g_mutex at admission.
  // Because the pin is taken atomically at admission, this same-thread
  // ordering deterministically lands AFTER the pin, so on fixed code every
  // iteration is expected to observe a valid successful result (the pin keeps
  // the engine alive for the run) -- but the test tolerates either outcome
  // (success or the terminal Unknown-engine-handle envelope) and only fails
  // if the process crashes or an iteration returns something outside that
  // closed set, so it cannot false-fail on the fix and stays meaningful if
  // future changes narrow the pinned window.
  it(
    "best-effort: destroyEngine() racing an in-flight streaming op never crashes and always ends in a valid result or the terminal envelope",
    async () => {
      ffi.initialize(findLibrary());

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
        // Fire the racing destroy as close to the admission window as this
        // single thread allows: immediately after starting the op, before
        // awaiting it.
        expect(() => ffi.destroyEngine(handle)).not.toThrow();

        const raw = await resultPromise;
        const parsed = JSON.parse(raw);

        if (parsed.success) {
          expect(JSON.parse(Buffer.concat(chunks).toString("utf-8"))).toEqual([1, 2, 3]);
        } else {
          expect(parsed).toEqual(UNKNOWN_ENVELOPE);
          expect(chunks).toHaveLength(0);
        }
      }
    },
    60000
  );

  it("final cleanup drains the shared isolate (idempotent)", async () => {
    await ffi.cleanup();
    // Calling it again must remain a safe no-op, mirroring
    // independent-engines.test.ts's final teardown discipline.
    await expect(ffi.cleanup()).resolves.toBeUndefined();
  });
});
