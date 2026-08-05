import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { modulesFromMap, modulesFromDirectory, modulesFromJars, composeResolvers } from "../../src/resolver";
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

  it.skipIf(process.platform === "win32")("throws on unreadable file", () => {
    const badFile = path.join(tempDir, "bad.dwl");
    fs.writeFileSync(badFile, "content");
    fs.chmodSync(badFile, 0o000); // Make unreadable

    const resolver = modulesFromDirectory(tempDir);

    expect(() => resolver("bad.dwl")).toThrow("Failed to read module");

    // Cleanup
    fs.chmodSync(badFile, 0o644);
  });

  it("returns null for path traversal attempts", () => {
    const resolver = modulesFromDirectory(tempDir);
    expect(resolver("../../outside.dwl")).toBeNull();
  });
});

describe("modulesFromJars", () => {
  it("extracts .dwl files from JAR", async () => {
    const jarPath = path.join(__dirname, "..", "fixtures", "test-lib.jar");

    const resolver = await modulesFromJars([jarPath]);

    const strings = resolver("dw/core/Strings.dwl");
    expect(strings).toContain("fun capitalize");

    const math = resolver("org/test/math.dwl");
    expect(math).toContain("fun multiply");
  });

  it("returns null when module not in JAR", async () => {
    const jarPath = path.join(__dirname, "..", "fixtures", "test-lib.jar");

    const resolver = await modulesFromJars([jarPath]);

    const result = resolver("missing/mod.dwl");

    expect(result).toBeNull();
  });

  it("handles multiple JARs", async () => {
    const jarPath = path.join(__dirname, "..", "fixtures", "test-lib.jar");

    // Use same JAR twice for test (simulates multiple JARs)
    const resolver = await modulesFromJars([jarPath, jarPath]);

    expect(resolver("dw/core/Strings.dwl")).toContain("fun capitalize");
  });

  it("throws on invalid JAR", async () => {
    const badJar = path.join(__dirname, "..", "fixtures", "not-a-jar.txt");
    fs.writeFileSync(badJar, "not a zip file");

    await expect(modulesFromJars([badJar])).rejects.toThrow("Failed to read JAR");

    fs.unlinkSync(badJar);
  });

  it("ignores non-.dwl files in JAR", async () => {
    // Test JAR contains only .dwl files, but verify behavior
    const jarPath = path.join(__dirname, "..", "fixtures", "test-lib.jar");

    const resolver = await modulesFromJars([jarPath]);

    // Should not throw, just not find non-.dwl paths
    expect(resolver("some-text-file.txt")).toBeNull();
  });
});

describe("composeResolvers", () => {
  it("returns first match", () => {
    const r1 = modulesFromMap({ "a.dwl": "source1" });
    const r2 = modulesFromMap({ "a.dwl": "source2" });

    const composed = composeResolvers(r1, r2);

    expect(composed("a.dwl")).toBe("source1"); // First wins
  });

  it("falls through to next resolver on null", () => {
    const r1 = modulesFromMap({ "a.dwl": "source1" });
    const r2 = modulesFromMap({ "b.dwl": "source2" });

    const composed = composeResolvers(r1, r2);

    expect(composed("a.dwl")).toBe("source1"); // r1 matched
    expect(composed("b.dwl")).toBe("source2"); // r1 returned null, r2 matched
  });

  it("returns null when all resolvers return null", () => {
    const r1 = modulesFromMap({ "a.dwl": "source1" });
    const r2 = modulesFromMap({ "b.dwl": "source2" });

    const composed = composeResolvers(r1, r2);

    expect(composed("c.dwl")).toBeNull();
  });

  it("handles three resolvers", () => {
    const r1 = modulesFromMap({ "a.dwl": "source1" });
    const r2 = modulesFromMap({ "b.dwl": "source2" });
    const r3 = modulesFromMap({ "c.dwl": "source3" });

    const composed = composeResolvers(r1, r2, r3);

    expect(composed("a.dwl")).toBe("source1");
    expect(composed("b.dwl")).toBe("source2");
    expect(composed("c.dwl")).toBe("source3");
    expect(composed("d.dwl")).toBeNull();
  });

  it("combines directory and map resolvers", () => {
    // Create temp dir with one file
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "dw-test-"));
    fs.writeFileSync(path.join(tempDir, "file.dwl"), "from disk");

    const composed = composeResolvers(
      modulesFromMap({ "override.dwl": "from map" }),
      modulesFromDirectory(tempDir)
    );

    expect(composed("override.dwl")).toBe("from map"); // Map first
    expect(composed("file.dwl")).toBe("from disk"); // Fallback to disk

    fs.rmSync(tempDir, { recursive: true, force: true });
  });
});
