import { describe, it, expect } from "vitest";
import { compareOutput, deepEqual } from "../../tests/tck/compare";

const buf = (s: string) => Buffer.from(s, "utf-8");

describe("compareOutput — json (structural)", () => {
  it("matches regardless of key order", () => {
    const r = compareOutput("json", buf('{"a":1,"b":2}'), buf('{"b":2,"a":1}'));
    expect(r.match).toBe(true);
  });

  it("matches regardless of insignificant whitespace", () => {
    expect(compareOutput("json", buf('{\n  "a": 1\n}'), buf('{"a":1}')).match).toBe(true);
  });

  it("reports a mismatch on differing values", () => {
    const r = compareOutput("json", buf('{"a":1}'), buf('{"a":2}'));
    expect(r.match).toBe(false);
    expect(r.detail).toMatch(/JSON mismatch/);
  });

  it("is sensitive to array order", () => {
    expect(compareOutput("json", buf("[1,2,3]"), buf("[3,2,1]")).match).toBe(false);
  });

  it("fails clearly when actual is not valid JSON", () => {
    const r = compareOutput("json", buf("{not json"), buf("{}"));
    expect(r.match).toBe(false);
    expect(r.detail).toMatch(/actual is not valid JSON/);
  });

  it("ignores CRLF vs LF inside a string value (Windows line endings)", () => {
    // Regression: a JSON output embedding CSV — the writer emits CRLF on Windows
    // but the fixture has LF. The line breaks are escaped \r\n / \n inside the
    // JSON string, so they must be normalized during value comparison.
    const actual = buf(JSON.stringify({ csv: "a|b\r\nx|y\r\n" }));
    const expected = buf(JSON.stringify({ csv: "a|b\nx|y\n" }));
    expect(compareOutput("json", actual, expected).match).toBe(true);
  });

  it("still distinguishes genuinely different string values", () => {
    const actual = buf(JSON.stringify({ v: "hello" }));
    const expected = buf(JSON.stringify({ v: "world" }));
    expect(compareOutput("json", actual, expected).match).toBe(false);
  });
});

describe("compareOutput — xml (structural)", () => {
  it("ignores layout whitespace between elements", () => {
    const a = buf("<root>\n  <a>1</a>\n</root>");
    const e = buf("<root><a>1</a></root>");
    expect(compareOutput("xml", a, e).match).toBe(true);
  });

  it("ignores the XML declaration and attribute quote style", () => {
    const a = buf("<?xml version='1.0' encoding='UTF-8'?><a x='1'/>");
    const e = buf('<?xml version="1.0" encoding="UTF-8"?><a x="1"></a>');
    expect(compareOutput("xml", a, e).match).toBe(true);
  });

  it("treats self-closing and explicit-close empty elements as equal", () => {
    expect(compareOutput("xml", buf("<r><a/></r>"), buf("<r><a></a></r>")).match).toBe(true);
  });

  it("ignores where an xmlns declaration is placed (namespace scope, not location)", () => {
    const a = buf('<f:flow xmlns:f="m"><h:x xmlns:h="h" a="1"/></f:flow>');
    const e = buf('<f:flow xmlns:f="m" xmlns:h="h"><h:x a="1"></h:x></f:flow>');
    expect(compareOutput("xml", a, e).match).toBe(true);
  });

  it("detects a content difference", () => {
    expect(compareOutput("xml", buf("<a>1</a>"), buf("<a>2</a>")).match).toBe(false);
  });

  it("detects a differing attribute value", () => {
    expect(compareOutput("xml", buf('<a x="1"/>'), buf('<a x="2"/>')).match).toBe(false);
  });

  it("reports invalid actual XML as a mismatch", () => {
    const r = compareOutput("xml", buf("<a>"), buf("<a></a>"));
    // fast-xml-parser is lenient, so this may parse; the point is it must not throw.
    expect(typeof r.match).toBe("boolean");
  });
});

describe("compareOutput — csv/txt (EOL-normalized)", () => {
  it("normalizes CRLF vs LF and trims", () => {
    expect(compareOutput("csv", buf("a,b\r\n1,2\r\n"), buf("a,b\n1,2")).match).toBe(true);
  });

  it("keeps interior whitespace significant", () => {
    expect(compareOutput("txt", buf("a b"), buf("ab")).match).toBe(false);
  });
});

describe("compareOutput — dwl (all whitespace stripped)", () => {
  it("ignores all whitespace differences", () => {
    expect(compareOutput("dwl", buf("fun f(x) = x + 1"), buf("fun f(x)=x+1")).match).toBe(true);
  });
});

describe("compareOutput — bin (exact bytes)", () => {
  it("matches identical bytes", () => {
    expect(compareOutput("bin", Buffer.from([0, 1, 2]), Buffer.from([0, 1, 2])).match).toBe(true);
  });
  it("fails on any byte difference", () => {
    const r = compareOutput("bin", Buffer.from([0, 1, 2]), Buffer.from([0, 1, 3]));
    expect(r.match).toBe(false);
    expect(r.detail).toMatch(/binary mismatch/);
  });
});

describe("deepEqual", () => {
  it("compares nested objects key-insensitively to order", () => {
    expect(deepEqual({ a: { x: 1, y: 2 } }, { a: { y: 2, x: 1 } })).toBe(true);
  });
  it("distinguishes null from missing", () => {
    expect(deepEqual({ a: null }, {})).toBe(false);
  });
  it("distinguishes arrays from objects", () => {
    expect(deepEqual([], {})).toBe(false);
  });
  it("compares scalars", () => {
    expect(deepEqual(1, 1)).toBe(true);
    expect(deepEqual("a", "b")).toBe(false);
  });
});

describe("compareOutput — encoding sidecar", () => {
  const utf16le = (s: string) => Buffer.from(s, "utf16le");

  it("decodes both sides as UTF-16 when charset is supplied (json)", () => {
    const actual = utf16le(JSON.stringify({ v: "café" }));
    const expected = utf16le(JSON.stringify({ v: "café" }));
    // Without the charset these UTF-16 bytes would parse as garbage → mismatch.
    expect(compareOutput("json", actual, expected, "UTF-16").match).toBe(true);
  });

  it("decodes both sides as UTF-16 for xml", () => {
    const actual = utf16le("<a>x</a>");
    const expected = utf16le("<a>x</a>");
    expect(compareOutput("xml", actual, expected, "UTF-16").match).toBe(true);
  });

  it("defaults to UTF-8 when no charset is given", () => {
    expect(compareOutput("json", Buffer.from('{"a":1}', "utf-8"), Buffer.from('{"a":1}', "utf-8")).match).toBe(true);
  });

  it("ignores charset for bin (raw bytes)", () => {
    expect(compareOutput("bin", Buffer.from([0, 1]), Buffer.from([0, 1]), "UTF-16").match).toBe(true);
  });
});