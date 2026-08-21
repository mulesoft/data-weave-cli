import { describe, expect, it } from "vitest";
import { accountTckResults, validateTckAccounting } from "../../tests/tck/accounting";
import { matchesTestNamePattern } from "../../tests/tck/reporter";

describe("TCK result accounting", () => {
  it("reports each selected outcome exactly once", () => {
    const summary = accountTckResults(
      new Set(["runtime/pass:out.json", "runtime/fail:out.json", "runtime/skip:out.json"]),
      [
        { identifier: "runtime/pass:out.json", outcome: "passed" },
        { identifier: "runtime/fail:out.json", outcome: "failed" },
        { identifier: "runtime/skip:out.json", outcome: "skipped" },
      ],
    );

    expect(summary).toEqual({
      selected: 3,
      passed: 1,
      failed: 1,
      skipped: 1,
      accounted: 3,
      unaccounted: 0,
    });
  });

  it("rejects missing, duplicate, and unexpected results", () => {
    expect(validateTckAccounting(
      new Set(["runtime/a:out.json", "runtime/b:out.json"]),
      [
        { identifier: "runtime/a:out.json", outcome: "passed" },
        { identifier: "runtime/a:out.json", outcome: "passed" },
        { identifier: "runtime/extra:out.json", outcome: "passed" },
      ],
    )).toEqual([
      "runtime/a:out.json: duplicate result",
      "runtime/b:out.json: missing result",
      "runtime/extra:out.json: result was not selected",
    ]);
  });

  it("allows intentionally filtered selections", () => {
    expect(validateTckAccounting(
      new Set(["runtime/a:out.json"]),
      [{ identifier: "runtime/a:out.json", outcome: "passed" }],
    )).toEqual([]);
  });

  it("selects policy skips only when they match the active test-name filter", () => {
    const pattern = /runtime\/selected:out\.json/;

    expect(matchesTestNamePattern("runtime/selected:out.json [skip: unsupported]", pattern)).toBe(true);
    expect(matchesTestNamePattern("runtime/other:out.json [skip: unsupported]", pattern)).toBe(false);
    expect(matchesTestNamePattern("runtime/other:out.json [skip: unsupported]", undefined)).toBe(true);
  });
});
