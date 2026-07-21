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
});

describe("compareOutput — xml (whitespace-collapsed)", () => {
  it("ignores layout whitespace between elements", () => {
    const a = buf("<root>\n  <a>1</a>\n</root>");
    const e = buf("<root><a>1</a></root>");
    expect(compareOutput("xml", a, e).match).toBe(true);
  });

  it("detects a content difference", () => {
    expect(compareOutput("xml", buf("<a>1</a>"), buf("<a>2</a>")).match).toBe(false);
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