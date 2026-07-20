import { describe, it, expect } from "vitest";
import { parseNativeResponse, makeResult, parseStreamingResult } from "../../src/result";

const b64 = (s: string, enc: BufferEncoding = "utf-8") =>
  Buffer.from(s, enc).toString("base64");

describe("parseNativeResponse", () => {
  it("treats an empty string as a failure", () => {
    const r = parseNativeResponse("");
    expect(r.success).toBe(false);
    expect(r.error).toBe("Native returned empty response");
    expect(r.getString()).toBeNull();
    expect(r.getBytes()).toBeNull();
  });

  it("treats malformed JSON as a failure", () => {
    const r = parseNativeResponse("{not json");
    expect(r.success).toBe(false);
    expect(r.error).toContain("Failed to parse native JSON response");
  });

  it("maps an error envelope to a failed result", () => {
    const r = parseNativeResponse(JSON.stringify({ success: false, error: "boom" }));
    expect(r.success).toBe(false);
    expect(r.error).toBe("boom");
  });

  it("defaults error to null when a failure omits it", () => {
    const r = parseNativeResponse(JSON.stringify({ success: false }));
    expect(r.success).toBe(false);
    expect(r.error).toBeNull();
  });

  it("maps a success envelope with metadata", () => {
    const r = parseNativeResponse(
      JSON.stringify({
        success: true,
        result: b64("42"),
        binary: false,
        mimeType: "application/json",
        charset: "utf-8",
      })
    );
    expect(r.success).toBe(true);
    expect(r.error).toBeNull();
    expect(r.mimeType).toBe("application/json");
    expect(r.charset).toBe("utf-8");
    expect(r.binary).toBe(false);
    expect(r.getString()).toBe("42");
  });

  it("defaults optional success fields when absent", () => {
    const r = parseNativeResponse(JSON.stringify({ success: true, result: b64("x") }));
    expect(r.success).toBe(true);
    expect(r.binary).toBe(false);
    expect(r.mimeType).toBeNull();
    expect(r.charset).toBeNull();
  });
});

describe("makeResult", () => {
  it("getString/getBytes return null on failure", () => {
    const r = makeResult(false, null, "err", false, null, null);
    expect(r.getString()).toBeNull();
    expect(r.getBytes()).toBeNull();
  });

  it("getString/getBytes return null when result is null even on success", () => {
    const r = makeResult(true, null, null, false, null, null);
    expect(r.getString()).toBeNull();
    expect(r.getBytes()).toBeNull();
  });

  it("getBytes base64-decodes into a Buffer", () => {
    const r = makeResult(true, b64("hello"), null, false, null, null);
    const bytes = r.getBytes();
    expect(Buffer.isBuffer(bytes)).toBe(true);
    expect(bytes!.toString("utf-8")).toBe("hello");
  });

  it("getString decodes using the declared charset", () => {
    const r = makeResult(true, b64("café ☕", "utf-16le"), null, false, null, "utf-16le");
    expect(r.getString()).toBe("café ☕");
  });

  it("getString defaults to utf-8 when charset is null", () => {
    const r = makeResult(true, b64("café"), null, false, null, null);
    expect(r.getString()).toBe("café");
  });

  it("getString passes the raw result through when binary", () => {
    const raw = b64("anything");
    const r = makeResult(true, raw, null, true, "application/octet-stream", null);
    // binary results are not decoded — the base64 payload is returned as-is
    expect(r.getString()).toBe(raw);
    // getBytes still decodes the base64 payload
    expect(r.getBytes()!.toString("utf-8")).toBe("anything");
  });
});

describe("parseStreamingResult", () => {
  it("treats an empty string as a failure", () => {
    const r = parseStreamingResult("");
    expect(r.success).toBe(false);
    expect(r.error).toBe("Empty response");
  });

  it("treats malformed JSON as a failure", () => {
    const r = parseStreamingResult("{oops");
    expect(r.success).toBe(false);
    expect(r.error).toBe("Failed to parse metadata");
    expect(r.mimeType).toBeNull();
    expect(r.charset).toBeNull();
    expect(r.binary).toBe(false);
  });

  it("maps an error envelope to a failed result", () => {
    const r = parseStreamingResult(JSON.stringify({ success: false, error: "nope" }));
    expect(r.success).toBe(false);
    expect(r.error).toBe("nope");
  });

  it("defaults error to null when a failure omits it", () => {
    const r = parseStreamingResult(JSON.stringify({ success: false }));
    expect(r.success).toBe(false);
    expect(r.error).toBeNull();
  });

  it("maps a success envelope with metadata", () => {
    const r = parseStreamingResult(
      JSON.stringify({ success: true, mimeType: "application/csv", charset: "utf-8", binary: true })
    );
    expect(r.success).toBe(true);
    expect(r.error).toBeNull();
    expect(r.mimeType).toBe("application/csv");
    expect(r.charset).toBe("utf-8");
    expect(r.binary).toBe(true);
  });
});
