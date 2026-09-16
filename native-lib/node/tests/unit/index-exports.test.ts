import { describe, expect, it } from "vitest";
import * as api from "../../src/index";

describe("public package exports", () => {
  it("exports DataWeave without module-level execution or cleanup", () => {
    expect(api.DataWeave).toBeTypeOf("function");
    for (const name of ["run", "runStreaming", "runTransform", "cleanup"]) {
      expect(api).not.toHaveProperty(name);
    }
  });
});
