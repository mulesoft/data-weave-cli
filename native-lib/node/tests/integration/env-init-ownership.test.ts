import { describe, it, expect } from "vitest";
import * as ffi from "../../src/ffi";
import { findLibrary, buildInputsJson } from "../../src/utils";

// W-23692110 round 13 #5: the init reference is owned per napi_env, not per
// engine. These raw-ffi tests (no vi.mock) drive the addon boundary directly --
// the exact surface the finding is about -- and use the ref-count proxy from
// instance-lifecycle.test.ts: after balancing to zero refs a raw engine call
// throws /not initialized/; while the isolate is live a run succeeds.
//
// IMPORTANT -- these are single-env SMOKE tests, NOT true #5 regression teeth.
// #5 is a CROSS-ENV bug: an abandoned/dying env with N engines under one
// initialize() firing N per-engine releases against the one reference it owns,
// or one env's cleanup()/env-death releasing a reference another env owns. Both
// require either a real dying env or two distinct napi_envs with asymmetric
// init/cleanup. Vitest runs these on the single main-thread env, so they cannot
// distinguish the fixed isolate from the pre-fix (buggy) one -- it was verified
// empirically that both cases below pass unchanged when rebuilt against the
// pre-round-13 addon (destroyEngine() never released the init ref in any
// revision, and the second cleanup() was already a no-op via the long-standing
// `if (g_ref_count > 0)` floor). They guard that the sanctioned single-env path
// still behaves (liveness + no double-decrement corruption); they do NOT prove
// #5 is fixed. The cross-env behavior that #5 is actually about -- an abandoned env with N
// engines under one initialize() -- is now pinned by the dedicated cross-env
// regression test in worker-lifecycle.test.ts ("a Worker that inits once +
// creates N engines + exits without cleanup() does NOT tear down the isolate
// under a live main engine"), which fails RED on the round-12 implementation
// and passes at round 13+. These single-env smoke tests remain as a fast guard
// on the sanctioned single-env liveness path.

const LIB = findLibrary();

function runOn(handle: number, expr: string): unknown {
  const envelope = JSON.parse(
    ffi.runScriptEngine(handle, `%dw 2.0\noutput application/json\n---\n${expr}`, buildInputsJson({}))
  );
  expect(envelope.success).toBe(true);
  return JSON.parse(Buffer.from(envelope.result, "base64").toString("utf-8"));
}

describe("per-env init-reference ownership -- single-env smoke tests (round 13 #5)", () => {
  // Smoke test (NOT a #5 regression test -- see file header): destroyEngine()
  // never released the init reference in any revision, so this held pre-fix too.
  it("smoke: one initialize() + multiple engines stays live when a single engine is destroyed", () => {
    ffi.initialize(LIB); // ONE init reference for this env
    const h1 = ffi.createEngine();
    const h2 = ffi.createEngine();
    expect(runOn(h2, "6 * 7")).toBe(42);

    // Destroy one engine. The isolate reference belongs to initialize(), not to
    // an engine, so the isolate must stay alive and h2 must still run.
    ffi.destroyEngine(h1);
    expect(runOn(h2, "1 + 1")).toBe(2);

    // Balance: destroy the other engine and release the single init reference.
    ffi.destroyEngine(h2);
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    return ffi.cleanup().then(() => {
      expect(() =>
        ffi.runScriptEngine(Number.MAX_SAFE_INTEGER, "%dw 2.0\noutput application/json\n---\n1", buildInputsJson({}))
      ).toThrow(/not initialized/i);
    });
  });

  // Smoke test (NOT a #5 regression test -- see file header): the second
  // cleanup() was already a no-op pre-fix via the `if (g_ref_count > 0)` floor,
  // so with one env this passes on the buggy addon too. #5's gate protects the
  // CROSS-env case (one env stealing another's reference), not observable here.
  it("smoke: a second cleanup() on an env that owns no reference does not corrupt the count", async () => {
    ffi.initialize(LIB); // init_refs = 1
    const h = ffi.createEngine();
    expect(runOn(h, "2 + 2")).toBe(4);
    ffi.destroyEngine(h);

    // First cleanup releases this env's one reference -> isolate torn down.
    await ffi.cleanup();
    // Second cleanup: this env's init_refs is already 0. Must be a no-op --
    // it must NOT drive g_ref_count negative or perturb a later isolate.
    await ffi.cleanup();

    // Prove the count was not corrupted: a fresh, fully-balanced init/run/cleanup
    // cycle still nets to zero (a corrupted negative count would leave the next
    // isolate un-torn-down and this final probe would NOT report not-initialized).
    ffi.initialize(LIB);
    const h2 = ffi.createEngine();
    expect(runOn(h2, "3 + 4")).toBe(7);
    ffi.destroyEngine(h2);
    await ffi.cleanup();

    expect(() =>
      ffi.runScriptEngine(Number.MAX_SAFE_INTEGER, "%dw 2.0\noutput application/json\n---\n1", buildInputsJson({}))
    ).toThrow(/not initialized/i);
  });
});
