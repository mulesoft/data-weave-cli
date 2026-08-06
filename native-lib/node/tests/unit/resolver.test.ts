import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { modulesFromMap, modulesFromDirectory, modulesFromJars, composeResolvers } from "../../src/resolver";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import AdmZip from "adm-zip";

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

  it.skipIf(process.platform === "win32")(
    "returns null when an in-tree symlink escapes baseDir",
    () => {
      // outsideDir sits alongside tempDir, outside the configured base.
      const outsideDir = fs.mkdtempSync(path.join(os.tmpdir(), "dw-outside-"));
      const secretFile = path.join(outsideDir, "secret.dwl");
      fs.writeFileSync(secretFile, "%dw 2.0\n// should never be resolved");

      // A symlink inside tempDir that resolves outside it. The lexical
      // containment check alone would accept this path; only realpath
      // canonicalization catches the escape.
      const linkPath = path.join(tempDir, "escape.dwl");
      fs.symlinkSync(secretFile, linkPath);

      const resolver = modulesFromDirectory(tempDir);
      expect(resolver("escape.dwl")).toBeNull();

      fs.rmSync(outsideDir, { recursive: true, force: true });
    }
  );

  it("keeps resolving a relative baseDir after process.chdir()", () => {
    // modulesFromDirectory captures an absolute baseDirLexical up front. The
    // candidate path built on each call must be derived from that captured
    // absolute base -- not re-resolved against baseDir (which, if relative,
    // silently tracks the *current* cwd) -- or a later chdir() breaks every
    // lookup against a resolver that was already constructed and working.
    const originalCwd = process.cwd();
    const relativeBase = path.relative(originalCwd, tempDir);
    const resolver = modulesFromDirectory(relativeBase);

    const elsewhere = fs.mkdtempSync(path.join(os.tmpdir(), "dw-elsewhere-"));
    try {
      process.chdir(elsewhere);
      const result = resolver("simple.dwl");
      expect(result).toContain("var x = 42");
    } finally {
      process.chdir(originalCwd);
      fs.rmSync(elsewhere, { recursive: true, force: true });
    }
  });
});

describe("modulesFromDirectory with a root-level baseDir", () => {
  // A root base (POSIX "/", or a Windows drive root) makes `base + path.sep`
  // duplicate the separator (e.g. "//"), which broke the old prefix-based
  // containment check for every child path. Exercise the actual filesystem
  // root's realpath so this covers whatever isContained() computes for it,
  // without assuming write access to "/" itself.
  it("does not reject a path solely because baseDir is a filesystem root", () => {
    const root = path.parse(process.cwd()).root; // e.g. "/" or "C:\\"
    const rootResolved = fs.realpathSync(root);
    const resolver = modulesFromDirectory(root);

    // A false containment rejection and a genuine "not found" both surface as
    // `null` from the resolver, so this needs a file that definitely exists
    // under root to tell them apart -- create one under the OS temp dir,
    // which is itself always a descendant of the filesystem root.
    const probeDir = fs.mkdtempSync(path.join(os.tmpdir(), "dw-root-probe-"));
    try {
      const probeResolved = fs.realpathSync(probeDir);
      expect(probeResolved.startsWith(rootResolved)).toBe(true);

      fs.writeFileSync(path.join(probeDir, "probe.dwl"), "%dw 2.0\nvar probe = true");
      // Resolve relative to the *actual* root, using the probe dir's path
      // relative to root as the "module path" -- this only works if root
      // containment doesn't reject valid, deeply-nested children.
      const relFromRoot = path.relative(rootResolved, path.join(probeResolved, "probe.dwl"));
      const result = resolver(relFromRoot);
      expect(result).toContain("var probe = true");
    } finally {
      fs.rmSync(probeDir, { recursive: true, force: true });
    }
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

    // Build a second, distinct JAR (different module name) on the fly so
    // this test actually exercises merging across archives, rather than
    // passing trivially because only the first archive's contents matter.
    const secondJarDir = fs.mkdtempSync(path.join(os.tmpdir(), "dw-jar-"));
    const secondJarPath = path.join(secondJarDir, "second-lib.jar");
    const zip = new AdmZip();
    zip.addFile("org/test/second.dwl", Buffer.from('%dw 2.0\nfun square(n) = n * n'));
    zip.writeZip(secondJarPath);

    const resolver = await modulesFromJars([jarPath, secondJarPath]);

    expect(resolver("dw/core/Strings.dwl")).toContain("fun capitalize");
    expect(resolver("org/test/second.dwl")).toContain("fun square");

    fs.rmSync(secondJarDir, { recursive: true, force: true });
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
