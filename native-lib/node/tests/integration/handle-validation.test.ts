import { describe, it, expect } from "vitest";
import * as ffi from "../../src/ffi";
import { findLibrary } from "../../src/utils";

// Round-6 finding #1 (defense-in-depth): the native handle-read sites
// (napi_get_value_int64 in napi_run_script_engine,
// napi_run_script_streaming_engine, napi_run_script_transform_engine) must
// reject a non-integer handle argument instead of silently using
// uninitialized/garbage stack data as the engine handle.
//
// This is driven through `ffi` (the raw addon boundary), not through the
// `DataWeave` class, because Task 1's JS-layer state guard only ever passes
// `this.engineHandle` (always a number once initialized) down to the native
// call -- so a bad handle can never reach these C sites through the public
// TS API. Each `ffi.xxx` export is a pure pass-through to the native addon
// (see src/ffi.ts: no validation of its own), so calling them directly with
// a non-numeric "handle" exercises the raw C boundary while reusing the same
// initialize()/findLibrary() bootstrap the other integration tests use.
//
// One test covers all three sites (rather than three separate tests) to keep
// the suite's test count increasing by exactly one for this task.
//
// Real addon, no mocking.
describe("native handle validation (round 6 #1)", () => {
  it("runScriptEngine/runScriptStreamingEngine/runScriptTransformEngine all throw on a non-integer handle rather than using garbage", () => {
    ffi.initialize(findLibrary());

    // napi_get_value_int64 must fail (and be checked) for a non-numeric
    // handle argument; each site must throw cleanly instead of proceeding
    // with whatever `handle64` happened to contain on the stack.
    expect(() =>
      ffi.runScriptEngine(
        {} as unknown as number,
        "%dw 2.0\noutput application/json\n---\n1",
        "{}"
      )
    ).toThrow();

    expect(() =>
      ffi.runScriptStreamingEngine(
        {} as unknown as number,
        "%dw 2.0\noutput application/json\n---\n1",
        "{}",
        () => {}
      )
    ).toThrow();

    expect(() =>
      ffi.runScriptTransformEngine(
        {} as unknown as number,
        "output application/json\n---\npayload",
        "{}",
        "payload",
        "application/json",
        null,
        () => null,
        () => {}
      )
    ).toThrow();
  });
});
