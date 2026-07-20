import { describe, it, expect } from "vitest";
import { DataWeaveError, DataWeaveScriptError } from "../../src/errors";
import { makeResult } from "../../src/result";

describe("DataWeaveError", () => {
  it("is an Error with the correct name and message", () => {
    const e = new DataWeaveError("boom");
    expect(e).toBeInstanceOf(Error);
    expect(e).toBeInstanceOf(DataWeaveError);
    expect(e.name).toBe("DataWeaveError");
    expect(e.message).toBe("boom");
  });
});

describe("DataWeaveScriptError", () => {
  it("extends DataWeaveError and carries the name", () => {
    const e = new DataWeaveScriptError(makeResult(false, null, "bad script", false, null, null));
    expect(e).toBeInstanceOf(Error);
    expect(e).toBeInstanceOf(DataWeaveError);
    expect(e).toBeInstanceOf(DataWeaveScriptError);
    expect(e.name).toBe("DataWeaveScriptError");
  });

  it("uses the result's error as the message", () => {
    const result = makeResult(false, null, "unexpected token", false, null, null);
    const e = new DataWeaveScriptError(result);
    expect(e.message).toBe("unexpected token");
  });

  it("falls back to a default message when the result has no error", () => {
    const e = new DataWeaveScriptError(makeResult(false, null, null, false, null, null));
    expect(e.message).toBe("Script execution failed");
  });

  it("attaches the originating result", () => {
    const result = makeResult(false, null, "err", false, null, null);
    const e = new DataWeaveScriptError(result);
    expect(e.result).toBe(result);
  });
});