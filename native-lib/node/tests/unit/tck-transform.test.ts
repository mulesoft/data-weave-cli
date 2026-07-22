import { describe, it, expect } from "vitest";
import { ensureOutputDirective } from "../../tests/tck/transform";

describe("ensureOutputDirective", () => {
  it("appends an output directive when the header has none", () => {
    const src = "type IName = String\n---\n{ a: 1 }";
    const out = ensureOutputDirective(src, "application/json");
    expect(out).toBe("type IName = String\noutput application/json\n---\n{ a: 1 }");
  });

  it("leaves an existing directive unchanged when it already targets the format", () => {
    const src = "%dw 2.0\noutput application/json\n---\npayload";
    expect(ensureOutputDirective(src, "application/json")).toBe(src);
  });

  it("replaces the mime token when the pinned format differs from the target", () => {
    const src = "%dw 2.0\noutput application/json\n---\npayload";
    expect(ensureOutputDirective(src, "application/xml")).toBe("%dw 2.0\noutput application/xml\n---\npayload");
  });

  it("preserves trailing directive options when replacing the mime", () => {
    const src = '%dw 2.0\noutput application/json encoding="UTF-16"\n---\npayload';
    expect(ensureOutputDirective(src, "text/plain")).toBe('%dw 2.0\noutput text/plain encoding="UTF-16"\n---\npayload');
  });

  it("does not clobber a multipart subtype directive", () => {
    // A pinned multipart/mixed (with its boundary option) must survive even
    // though the expected extension maps to multipart/form-data.
    const src = '%dw 2.0\noutput multipart/mixed boundary="abc"\n---\npayload';
    expect(ensureOutputDirective(src, "multipart/form-data")).toBe(src);
  });

  it("does not touch a non-mime output selector (output :Type json)", () => {
    const src = "%dw 2.0\noutput :Test.number json\n---\npayload";
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

  it("splits on the column-0 separator, not an indented do-block separator", () => {
    // The do-block's indented `---` must not be mistaken for the header/body
    // split; the output directive belongs before the column-0 `---`.
    const src = ["var v = do {", "  var a = 1", "  ---", "  a + 1", "}", "---", "v"].join("\n");
    const out = ensureOutputDirective(src, "application/json");
    expect(out).toBe(["var v = do {", "  var a = 1", "  ---", "  a + 1", "}", "output application/json", "---", "v"].join("\n"));
  });

  it("treats a transform whose only separator is inside a do-block as a bare body", () => {
    // No column-0 `---` → the whole thing is a body and gets a fresh header.
    const src = ["fun f() = do {", "  var a = 1", "  ---", "  a", "}"].join("\n");
    const out = ensureOutputDirective(src, "application/json");
    expect(out).toBe(`output application/json\n---\n${src}`);
  });
});
