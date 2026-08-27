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

  // Review #10 #5 (svacas P2): napi_initialize used to ignore the status of
  // napi_get_cb_info and napi_get_value_string_utf8 and never checked that
  // argv[0] is a string, so a non-string libPath left the 4096-byte stack
  // lib_path buffer uninitialized before uv_dlopen used it. The TS wrapper
  // always passes a string, so drive this through the raw ffi binding
  // directly with each malformed shape and assert it throws synchronously
  // (and the process survives) rather than reading the uninitialized buffer.
  it.each([
    { name: "number", value: 42 },
    { name: "object", value: {} },
    { name: "null", value: null },
  ])("initialize throws synchronously on a non-string libPath ($name)", ({ value }) => {
    // Assert on the specific validation message, not just toThrow(): without
    // the argv[0] type check, the garbage stack lib_path still happens to
    // make uv_dlopen fail downstream, so a bare toThrow() would pass even on
    // the unfixed addon for the wrong reason (an accidental "Failed to load
    // library" error instead of a synchronous, pre-buffer-use rejection).
    expect(() => ffi.initialize(value as unknown as string)).toThrow(
      /library path must be a string/
    );
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

  it("runScriptStreamingEngine throws on non-string inputsJson", () => {
    ffi.initialize(findLibrary());
    const handle = ffi.createEngine();
    expect(() =>
      ffi.runScriptStreamingEngine(
        handle,
        "%dw 2.0\noutput application/json\n---\n[1,2,3]",
        {} as unknown as string,
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

  // The transform entrypoint converts four string args (script already covered
  // above): inputsJson, inputName, inputMimeType, and a non-null inputCharset.
  // A dropped napi_get_value_string check on any of them must throw (review #9 #6).
  it.each([
    { name: "inputsJson", script: "%dw 2.0\noutput application/json\n---\npayload", inputsJson: {} as unknown as string, inputName: "payload", mimeType: "application/json", charset: null as string | null },
    { name: "inputName", script: "%dw 2.0\noutput application/json\n---\npayload", inputsJson: "{}", inputName: {} as unknown as string, mimeType: "application/json", charset: null as string | null },
    { name: "inputMimeType", script: "%dw 2.0\noutput application/json\n---\npayload", inputsJson: "{}", inputName: "payload", mimeType: {} as unknown as string, charset: null as string | null },
    { name: "non-null inputCharset", script: "%dw 2.0\noutput application/json\n---\npayload", inputsJson: "{}", inputName: "payload", mimeType: "application/json", charset: {} as unknown as string },
  ])("runScriptTransformEngine throws on non-string $name", ({ script, inputsJson, inputName, mimeType, charset }) => {
    ffi.initialize(findLibrary());
    const handle = ffi.createEngine();
    expect(() =>
      ffi.runScriptTransformEngine(
        handle,
        script,
        inputsJson,
        inputName,
        mimeType,
        charset,
        () => null,
        () => {}
      )
    ).toThrow();
    ffi.destroyEngine(handle);
  });
});
