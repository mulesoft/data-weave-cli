import { describe, it, expect } from "vitest";
import { parseCase, extensionOf, hasAdjacentDwlModule, MAIN_TRANSFORM } from "../../tests/tck/case-loader";

/** Asserts parseCase skipped the case, returning the reason for further checks. */
function expectSkipped(caseName: string, files: string[]): string {
  const r = parseCase("runtime", caseName, files);
  expect(r.kind).toBe("skipped");
  return r.kind === "skipped" ? r.reason : "";
}

describe("extensionOf", () => {
  it("returns the lowercased extension without the dot", () => {
    expect(extensionOf("in0.JSON")).toBe("json");
    expect(extensionOf("out.xml")).toBe("xml");
  });
  it("returns empty string when there is no extension", () => {
    expect(extensionOf("Makefile")).toBe("");
  });
});

describe("parseCase — happy paths", () => {
  it("parses a single-input single-output case", () => {
    const r = parseCase("runtime", "as-operator-out.json", [MAIN_TRANSFORM, "in0.json", "out.json"]);
    expect(r.kind).toBe("scenarios");
    if (r.kind !== "scenarios") return;
    expect(r.scenarios).toHaveLength(1);
    const s = r.scenarios[0];
    expect(s.name).toBe("runtime/as-operator-out.json:out.json");
    expect(s.caseIdentifier).toBe("runtime/as-operator-out.json");
    expect(s.inputs).toEqual([{ name: "in0", fileName: "in0.json", mimeType: "application/json" }]);
    expect(s.outputMime).toBe("application/json");
    expect(s.outputExtension).toBe("json");
  });

  it("binds multiple inputs by base name, sorted", () => {
    const r = parseCase("runtime", "multi-out.json", [MAIN_TRANSFORM, "in1.xml", "in0.json", "out.json"]);
    if (r.kind !== "scenarios") throw new Error("expected scenarios");
    expect(r.scenarios[0].name).toBe("runtime/multi-out.json:out.json");
    expect(r.scenarios[0].inputs.map((i) => i.name)).toEqual(["in0", "in1"]);
    expect(r.scenarios[0].inputs.map((i) => i.mimeType)).toEqual(["application/json", "application/xml"]);
  });

  it("names the scenario after the case directory", () => {
    const r = parseCase("runtime", "literal-out.json", [MAIN_TRANSFORM, "out.json"]);
    if (r.kind !== "scenarios") throw new Error("expected scenarios");
    expect(r.scenarios[0].name).toBe("runtime/literal-out.json:out.json");
    expect(r.scenarios[0].inputs).toEqual([]);
  });
});

describe("parseCase — structural skips", () => {
  it("identifies adjacent DWL modules without treating inputs as modules", () => {
    expect(hasAdjacentDwlModule([MAIN_TRANSFORM, "include.dwl", "in0.dwl", "out.dwl"])).toBe(true);
    expect(hasAdjacentDwlModule([MAIN_TRANSFORM, "in0.dwl", "out.dwl"])).toBe(false);
  });

  it("skips _wip cases", () => {
    expect(expectSkipped("feature_wip", [MAIN_TRANSFORM, "out.json"])).toMatch(/wip/i);
  });

  it("skips cases with a bare config.properties", () => {
    expect(expectSkipped("c", [MAIN_TRANSFORM, "out.json", "config.properties"])).toMatch(/config\.properties/);
  });

  it("skips cases with per-input/output config properties", () => {
    expect(expectSkipped("c", [MAIN_TRANSFORM, "in0.json", "in0-config.properties", "out.json"]))
      .toMatch(/config\.properties/);
    expect(expectSkipped("c", [MAIN_TRANSFORM, "out.json", "out-config.properties"]))
      .toMatch(/config\.properties/);
  });

  it("skips groovy/java cases", () => {
    expect(expectSkipped("j", [MAIN_TRANSFORM, "out.json", "Helper.groovy"])).toMatch(/java|groovy/i);
  });

  it("skips cases without exactly one transform", () => {
    expect(expectSkipped("two", [MAIN_TRANSFORM, "other.dwl", "out.json"])).toMatch(/exactly one/);
    expect(expectSkipped("none", ["in0.json", "out.json"])).toMatch(/exactly one/);
  });

  it("does not count inN.dwl / out.dwl as the transform", () => {
    // in0.dwl is an input, not the transform → zero transforms found.
    expect(expectSkipped("x", ["in0.dwl", "out.json"])).toMatch(/exactly one/);
  });

  it("skips when the single dwl is not named transform.dwl", () => {
    expect(expectSkipped("x", ["mapping.dwl", "out.json"])).toMatch(/not named/);
  });

  it("skips cases with no output file", () => {
    expect(expectSkipped("x", [MAIN_TRANSFORM, "in0.json"])).toMatch(/no expected output/);
  });
});

describe("parseCase — unsupported formats", () => {
  it("skips a case whose input format is unsupported (yaml)", () => {
    expect(expectSkipped("y", [MAIN_TRANSFORM, "in0.yaml", "out.json"])).toMatch(/unsupported input/i);
  });

  it("drops unsupported output scenarios, keeping supported ones", () => {
    const r = parseCase("runtime", "mix", [MAIN_TRANSFORM, "in0.json", "out.json", "out.yaml"]);
    if (r.kind !== "scenarios") throw new Error("expected scenarios");
    expect(r.scenarios.map((s) => s.outputExtension)).toEqual(["json"]);
  });

  it("skips when all output formats are unsupported", () => {
    expect(expectSkipped("y", [MAIN_TRANSFORM, "in0.json", "out.yaml"]))
      .toMatch(/no scenarios with a supported output/);
  });
});
