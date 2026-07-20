import { describe, it, expect } from "vitest";
import { parseNativeResponse, makeResult, parseStreamingResult, decodeBytes } from "../../src/result";

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

describe("decodeBytes", () => {
  it("defaults to UTF-8 when charset is null", () => {
    expect(decodeBytes(Buffer.from("café", "utf-8"), null)).toBe("café");
  });

  it("decodes UTF-8 by name", () => {
    expect(decodeBytes(Buffer.from("café", "utf-8"), "UTF-8")).toBe("café");
  });

  it("decodes little-endian UTF-16", () => {
    const le = Buffer.from("café", "utf16le");
    expect(decodeBytes(le, "UTF-16LE")).toBe("café");
  });

  it("decodes big-endian UTF-16 by label (byte-swapping)", () => {
    const be = Buffer.from("café", "utf16le");
    be.swap16();
    expect(decodeBytes(be, "UTF-16BE")).toBe("café");
  });

  it("honors a big-endian BOM regardless of label", () => {
    const body = Buffer.from("café", "utf16le");
    body.swap16();
    const beWithBom = Buffer.concat([Buffer.from([0xfe, 0xff]), body]);
    expect(decodeBytes(beWithBom, "UTF-16")).toBe("café");
  });

  it("honors a little-endian BOM and strips it", () => {
    const leWithBom = Buffer.concat([Buffer.from([0xff, 0xfe]), Buffer.from("café", "utf16le")]);
    expect(decodeBytes(leWithBom, "UTF-16")).toBe("café");
  });

  it("decodes ISO-8859-1 / latin1", () => {
    const latin = Buffer.from("café", "latin1");
    expect(decodeBytes(latin, "ISO-8859-1")).toBe("café");
  });

  it("decodes US-ASCII", () => {
    expect(decodeBytes(Buffer.from("hello", "ascii"), "US-ASCII")).toBe("hello");
  });

  it("falls back to UTF-8 for an unrecognized charset instead of throwing", () => {
    expect(() => decodeBytes(Buffer.from("hi", "utf-8"), "x-made-up-charset")).not.toThrow();
    expect(decodeBytes(Buffer.from("hi", "utf-8"), "x-made-up-charset")).toBe("hi");
  });

  it("does not mutate the caller's buffer when byte-swapping", () => {
    const be = Buffer.from("AB", "utf16le");
    be.swap16();
    const snapshot = Buffer.from(be);
    decodeBytes(be, "UTF-16BE");
    expect(be.equals(snapshot)).toBe(true);
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
