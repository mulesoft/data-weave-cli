import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { modulesFromMap, modulesFromDirectory } from "../../src/resolver";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";

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

describe("modulesFromDirectory", () => {
  let tempDir: string;

  beforeEach(() => {
    // Create temp directory with test .dwl files
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "dw-test-"));

    // Create org/test/lib.dwl
    const orgTestDir = path.join(tempDir, "org", "test");
    fs.mkdirSync(orgTestDir, { recursive: true });
    fs.writeFileSync(
      path.join(orgTestDir, "lib.dwl"),
      '%dw 2.0\nfun greet(n) = "Hello " ++ n'
    );

    // Create top-level simple.dwl
    fs.writeFileSync(path.join(tempDir, "simple.dwl"), "%dw 2.0\nvar x = 42");
  });

  afterEach(() => {
    // Clean up temp directory
    fs.rmSync(tempDir, { recursive: true, force: true });
  });

  it("reads file from nested directory", () => {
    const resolver = modulesFromDirectory(tempDir);

    const result = resolver("org/test/lib.dwl");

    expect(result).toContain("%dw 2.0");
    expect(result).toContain("fun greet");
  });

  it("reads file from root directory", () => {
    const resolver = modulesFromDirectory(tempDir);

    const result = resolver("simple.dwl");

    expect(result).toContain("var x = 42");
  });

  it("returns null when file not found", () => {
    const resolver = modulesFromDirectory(tempDir);

    const result = resolver("missing/file.dwl");

    expect(result).toBeNull();
  });

  it("throws on unreadable file", () => {
    const badFile = path.join(tempDir, "bad.dwl");
    fs.writeFileSync(badFile, "content");
    fs.chmodSync(badFile, 0o000); // Make unreadable

    const resolver = modulesFromDirectory(tempDir);

    expect(() => resolver("bad.dwl")).toThrow("Failed to read module");

    // Cleanup
    fs.chmodSync(badFile, 0o644);
  });
});
