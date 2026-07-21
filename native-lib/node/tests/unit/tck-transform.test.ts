import { describe, it, expect } from "vitest";
import { ensureOutputDirective } from "../../tests/tck/transform";

describe("ensureOutputDirective", () => {
  it("appends an output directive when the header has none", () => {
    const src = "type IName = String\n---\n{ a: 1 }";
    const out = ensureOutputDirective(src, "application/json");
    expect(out).toBe("type IName = String\noutput application/json\n---\n{ a: 1 }");
  });

  it("leaves a transform with an existing output directive unchanged", () => {
    const src = "%dw 2.0\noutput application/xml\n---\npayload";
    expect(ensureOutputDirective(src, "application/json")).toBe(src);
  });

  it("treats a transform with no separator as a bare body and adds a header", () => {
    const out = ensureOutputDirective("payload map (\$)", "application/json");
    expect(out).toBe("output application/json\n---\npayload map (\$)");
  });

  it("only inspects the header, not the body, for an existing directive", () => {
    // "output" appears in the body, not the header → still needs a directive.
    const src = "%dw 2.0\n---\n{ note: \"output application/xml\" }";
    const out = ensureOutputDirective(src, "application/json");
    expect(out).toContain("output application/json\n---");
    // body preserved
    expect(out).toContain('{ note: "output application/xml" }');
  });

  it("preserves an existing %dw version line", () => {
    const src = "%dw 2.0\n---\n1";
    expect(ensureOutputDirective(src, "application/json")).toBe("%dw 2.0\noutput application/json\n---\n1");
  });
});
