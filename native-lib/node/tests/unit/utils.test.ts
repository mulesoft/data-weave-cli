import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { existsSync } from "node:fs";
import { normalizeInputValue, buildInputsJson, findLibrary } from "../../src/utils";

vi.mock("node:fs", () => ({ existsSync: vi.fn() }));

const mockedExistsSync = vi.mocked(existsSync);

const decode = (o: Record<string, unknown>) =>
  Buffer.from(o.content as string, "base64").toString("utf-8");

describe("normalizeInputValue", () => {
  it("encodes null as JSON null", () => {
    const n = normalizeInputValue(null);
    expect(n.mimeType).toBe("application/json");
    expect(n.charset).toBe("utf-8");
    expect(decode(n)).toBe("null");
  });

  it("honors an explicit mimeType for null", () => {
    const n = normalizeInputValue(null, "text/plain");
    expect(n.mimeType).toBe("text/plain");
  });

  it("encodes a string as text/plain", () => {
    const n = normalizeInputValue("hello");
    expect(n.mimeType).toBe("text/plain");
    expect(decode(n)).toBe("hello");
  });

  it("encodes a number as JSON", () => {
    const n = normalizeInputValue(42);
    expect(n.mimeType).toBe("application/json");
    expect(decode(n)).toBe("42");
  });

  it("encodes a boolean as JSON", () => {
    const n = normalizeInputValue(true);
    expect(n.mimeType).toBe("application/json");
    expect(decode(n)).toBe("true");
  });

  it("encodes an array as JSON", () => {
    const n = normalizeInputValue([1, 2, 3]);
    expect(n.mimeType).toBe("application/json");
    expect(decode(n)).toBe("[1,2,3]");
  });

  it("encodes a plain object as JSON", () => {
    const n = normalizeInputValue({ a: 1 });
    expect(n.mimeType).toBe("application/json");
    expect(decode(n)).toBe('{"a":1}');
  });

  it("respects an explicit override mimeType for plain values", () => {
    const n = normalizeInputValue("a,b,c", "application/csv");
    expect(n.mimeType).toBe("application/csv");
    expect(decode(n)).toBe("a,b,c");
  });

  describe("explicit InputValue objects ({content, mimeType})", () => {
    it("base64-encodes string content using the declared charset", () => {
      const n = normalizeInputValue({ content: "café", mimeType: "text/plain", charset: "utf-8" });
      expect(n.mimeType).toBe("text/plain");
      expect(n.charset).toBe("utf-8");
      expect(decode(n)).toBe("café");
    });

    it("base64-encodes Buffer content directly", () => {
      const buf = Buffer.from("binary-bytes");
      const n = normalizeInputValue({ content: buf, mimeType: "application/octet-stream" });
      expect(n.content).toBe(buf.toString("base64"));
    });

    it("passes through properties and charset", () => {
      const n = normalizeInputValue({
        content: "1;2;3",
        mimeType: "application/csv",
        charset: "utf-8",
        properties: { separator: ";", header: false },
      });
      expect(n.charset).toBe("utf-8");
      expect(n.properties).toEqual({ separator: ";", header: false });
    });

    it("omits charset/properties when not provided", () => {
      const n = normalizeInputValue({ content: "x", mimeType: "text/plain" });
      expect("charset" in n).toBe(false);
      expect("properties" in n).toBe(false);
    });
  });
});

describe("buildInputsJson", () => {
  it("normalizes every entry into a JSON string", () => {
    const json = buildInputsJson({ num1: 25, name: "bob" });
    const parsed = JSON.parse(json);
    expect(decode(parsed.num1)).toBe("25");
    expect(parsed.num1.mimeType).toBe("application/json");
    expect(decode(parsed.name)).toBe("bob");
    expect(parsed.name.mimeType).toBe("text/plain");
  });

  it("produces an empty object for no inputs", () => {
    expect(buildInputsJson({})).toBe("{}");
  });
});

describe("findLibrary", () => {
  const ORIGINAL_ENV = process.env.DATAWEAVE_NATIVE_LIB;

  beforeEach(() => {
    mockedExistsSync.mockReset();
    delete process.env.DATAWEAVE_NATIVE_LIB;
  });

  afterEach(() => {
    if (ORIGINAL_ENV === undefined) delete process.env.DATAWEAVE_NATIVE_LIB;
    else process.env.DATAWEAVE_NATIVE_LIB = ORIGINAL_ENV;
  });

  it("returns the DATAWEAVE_NATIVE_LIB path when it exists", () => {
    process.env.DATAWEAVE_NATIVE_LIB = "/custom/dwlib.dylib";
    mockedExistsSync.mockImplementation((p) => p === "/custom/dwlib.dylib");
    expect(findLibrary()).toBe("/custom/dwlib.dylib");
  });

  it("prefers dwlib next to a resolved addon path", () => {
    mockedExistsSync.mockImplementation((p) => String(p) === "/opt/plat/dwlib.so");
    expect(findLibrary("/opt/plat/dwlib_addon.node")).toBe("/opt/plat/dwlib.so");
  });

  it("ignores the env var when the path does not exist and falls through", () => {
    process.env.DATAWEAVE_NATIVE_LIB = "/missing/dwlib.so";
    // env path missing; first existing candidate is the packaged native/ dir
    mockedExistsSync.mockImplementation((p) => String(p).includes("native") && !String(p).includes("/missing/"));
    const result = findLibrary();
    expect(result).not.toBe("/missing/dwlib.so");
    expect(result).toContain("dwlib.");
  });

  it("resolves the packaged native/ library when present", () => {
    mockedExistsSync.mockImplementation((p) => /native[\\/]dwlib\.(dylib|so|dll)$/.test(String(p)));
    expect(findLibrary()).toMatch(/native[\\/]dwlib\.(dylib|so|dll)$/);
  });

  it("throws a helpful error when no library can be located", () => {
    mockedExistsSync.mockReturnValue(false);
    expect(() => findLibrary()).toThrow(/Could not find DataWeave native library/);
    expect(() => findLibrary()).toThrow(/DATAWEAVE_NATIVE_LIB/);
  });
});
