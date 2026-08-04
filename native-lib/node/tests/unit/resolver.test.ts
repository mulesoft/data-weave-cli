import { describe, it, expect } from "vitest";
import { modulesFromMap } from "../../src/resolver";

describe("modulesFromMap", () => {
  it("returns source when module exists", () => {
    const resolver = modulesFromMap({
      "org/test/lib.dwl": '%dw 2.0\nfun greet(n) = "Hello " ++ n',
    });

    const result = resolver("org/test/lib.dwl");

    expect(result).toBe('%dw 2.0\nfun greet(n) = "Hello " ++ n');
  });

  it("returns null when module not found", () => {
    const resolver = modulesFromMap({
      "org/test/lib.dwl": "%dw 2.0\n...",
    });

    const result = resolver("missing/mod.dwl");

    expect(result).toBeNull();
  });

  it("handles multiple modules", () => {
    const resolver = modulesFromMap({
      "a.dwl": "source a",
      "b.dwl": "source b",
    });

    expect(resolver("a.dwl")).toBe("source a");
    expect(resolver("b.dwl")).toBe("source b");
    expect(resolver("c.dwl")).toBeNull();
  });

  it("returns empty string when module source is empty", () => {
    const resolver = modulesFromMap({
      "org/test/empty.dwl": "",
    });

    expect(resolver("org/test/empty.dwl")).toBe("");
  });
});
