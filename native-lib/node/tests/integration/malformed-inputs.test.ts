import { describe, it, expect, afterEach } from "vitest";
import * as ffi from "../../src/ffi";
import { findLibrary, buildInputsJson } from "../../src/utils";

// Round-7 finding #2 (whole-class sweep): every FFI-facing entrypoint must
// check the status of each napi_get_value_* conversion and throw before using
// the converted value. Pre-fix, non-string script/inputs left *_len
// uninitialized before malloc(len+1) and the buffer write, and destroyEngine
// used an indeterminate handle64 from an ignored napi_get_value_int64.
//
// Driven through the raw `ffi` boundary (the DataWeave TS class always passes
// well-typed values), so these calls exercise the C conversion checks directly.
// The addon globals are process-wide C statics -- balance every initialize()
// with a cleanup() so this file does not leak a ref-count into siblings.
//
// Real addon, no mocking.
describe("malformed raw-ffi inputs throw (round 7 #2)", () => {
  afterEach(async () => {
    await ffi.cleanup();
  });

  it("destroyEngine throws on a non-integer handle", () => {
    ffi.initialize(findLibrary());
    expect(() => ffi.destroyEngine({} as unknown as number)).toThrow();
  });

  it("runScriptEngine throws on non-string script/inputs", () => {
    ffi.initialize(findLibrary());
    const handle = ffi.createEngine();
    expect(() =>
      ffi.runScriptEngine(handle, {} as unknown as string, buildInputsJson({}))
    ).toThrow();
    expect(() =>
      ffi.runScriptEngine(handle, "%dw 2.0\n---\n1", {} as unknown as string)
    ).toThrow();
    ffi.destroyEngine(handle);
  });

  it("runScriptStreamingEngine throws on non-string script/inputs", () => {
    ffi.initialize(findLibrary());
    const handle = ffi.createEngine();
    expect(() =>
      ffi.runScriptStreamingEngine(
        handle,
        {} as unknown as string,
        buildInputsJson({}),
        () => {}
      )
    ).toThrow();
    ffi.destroyEngine(handle);
  });

  it("runScriptTransformEngine throws on non-string script", () => {
    ffi.initialize(findLibrary());
    const handle = ffi.createEngine();
    expect(() =>
      ffi.runScriptTransformEngine(
        handle,
        {} as unknown as string,
        "{}",
        "payload",
        "application/json",
        null,
        () => null,
        () => {}
      )
    ).toThrow();
    ffi.destroyEngine(handle);
  });
});
