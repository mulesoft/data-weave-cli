import { describe, expect, it } from "vitest";
import {
  IGNORED_CASES,
  STRUCTURAL_MODULE_CASES,
  validateIgnorePolicy,
  validateInventoryPolicy,
  validateStructuralModulePolicy,
} from "../../tests/tck/ignore-list";

describe("TCK ignore policy", () => {
  it("requires identity, supported category, and reason", () => {
    expect(validateIgnorePolicy({
      "runtime/missing-identity": { caseIdentifier: "", category: "unavailable-java-module", reason: "missing" },
      "runtime/mismatched": { caseIdentifier: "runtime/other", category: "unavailable-java-module", reason: "missing" },
      "runtime/bad-category": { caseIdentifier: "runtime/bad-category", category: "broad-runtime", reason: "missing" },
      "runtime/blank-reason": { caseIdentifier: "runtime/blank-reason", category: "unavailable-java-module", reason: " " },
    })).toEqual([
      "runtime/missing-identity: missing case identity",
      "runtime/mismatched: case identity must match registry key",
      "runtime/bad-category: unsupported category broad-runtime",
      "runtime/blank-reason: missing reason",
    ]);
  });

  it("rejects policy entries that are not runnable cases", () => {
    expect(validateIgnorePolicy({
      "runtime/runnable": {
        caseIdentifier: "runtime/runnable",
        category: "unavailable-java-module",
        reason: "missing Java type",
      },
      "runtime/stale": {
        caseIdentifier: "runtime/stale",
        category: "unavailable-java-module",
        reason: "stale",
      },
    }, new Set(["runtime/runnable"]))).toEqual([
      "runtime/stale: not a discovered runnable case",
    ]);
  });

  it("preserves the current 59 exclusions", () => {
    expect(Object.keys(IGNORED_CASES)).toHaveLength(59);
  });

  it("rejects drift in the staged suite inventory", () => {
    expect(validateInventoryPolicy(728, 194)).toEqual([
      "expected 729 runnable cases, discovered 728",
      "expected 193 structurally skipped cases, discovered 194",
    ]);
  });
});

describe("TCK structural-module policy", () => {
  it("validates the inventory in both directions", () => {
    expect(validateStructuralModulePolicy(
      new Set(["runtime/registered", "runtime/stale"]),
      new Set(["runtime/registered", "runtime/missing"]),
    )).toEqual([
      "runtime/stale: not a structural module case",
      "runtime/missing: structural module case is not registered",
    ]);
  });

  it("catalogues 17 adjacent-DWL cases", () => {
    expect(STRUCTURAL_MODULE_CASES.size).toBe(17);
  });
});
