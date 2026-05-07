import { describe, it, expect, afterAll } from "vitest";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { DataWeave, run, runStreaming, runTransform, cleanup } from "../src/index";

afterAll(() => {
  cleanup();
});

describe("DataWeave Node.js API", () => {
  describe("run (buffered)", () => {
    it("basic arithmetic", () => {
      const result = run("2 + 2");
      expect(result.success).toBe(true);
      expect(result.getString()).toBe("4");
    });

    it("with inputs", () => {
      const result = run("num1 + num2", { num1: 25, num2: 17 });
      expect(result.success).toBe(true);
      expect(result.getString()).toBe("42");
    });

    it("explicit instance lifecycle", () => {
      const dw = new DataWeave();
      dw.initialize();
      try {
        const r1 = dw.run("sqrt(144)");
        expect(r1.getString()).toBe("12");
        const r2 = dw.run("sqrt(10000)");
        expect(r2.getString()).toBe("100");
      } finally {
        dw.cleanup();
      }
    });

    it("encoding: UTF-16 XML to CSV", () => {
      const xmlPath = join(__dirname, "fixtures", "person.xml");
      const xmlBytes = readFileSync(xmlPath);

      const script = `output application/csv header=true
---
[payload.person]`;

      const result = run(script, {
        payload: {
          content: xmlBytes,
          mimeType: "application/xml",
          charset: "UTF-16",
        },
      });

      expect(result.success).toBe(true);
      const out = result.getString()!;
      expect(out).toContain("name");
      expect(out).toContain("age");
      expect(out).toContain("Billy");
      expect(out).toContain("31");
    });

    it("auto-conversion of array input", () => {
      const result = run("numbers[0]", { numbers: [1, 2, 3] });
      expect(result.success).toBe(true);
      expect(result.getString()).toBe("1");
    });

    it("error handling", () => {
      const result = run("invalid_var_xyz");
      expect(result.success).toBe(false);
      expect(result.error).toBeTruthy();
    });

    it("raiseOnError throws", () => {
      expect(() => run("invalid_var_xyz", {}, { raiseOnError: true })).toThrow();
    });
  });

  describe("runStreaming", () => {
    it("basic streaming output", async () => {
      const chunks: Buffer[] = [];
      const gen = runStreaming("output application/json --- {a: 1, b: 2}");
      let result = await gen.next();
      while (!result.done) {
        chunks.push(result.value);
        result = await gen.next();
      }
      const metadata = result.value;

      const full = Buffer.concat(chunks).toString("utf-8");
      expect(full).toContain('"a": 1');
      expect(metadata.success).toBe(true);
      expect(metadata.mimeType).toBe("application/json");
    });

    it("large output produces multiple chunks", async () => {
      const chunks: Buffer[] = [];
      const gen = runStreaming(
        'output application/json --- (1 to 5000) map {id: $, name: "item_" ++ $}'
      );
      let result = await gen.next();
      while (!result.done) {
        chunks.push(result.value);
        result = await gen.next();
      }
      const metadata = result.value;

      expect(metadata.success).toBe(true);
      expect(chunks.length).toBeGreaterThan(1);
      const full = Buffer.concat(chunks).toString("utf-8");
      expect(full).toContain('"id": 5000');
    });

    it("error propagation", async () => {
      const chunks: Buffer[] = [];
      const gen = runStreaming("output application/json --- invalid_var");
      let result = await gen.next();
      while (!result.done) {
        chunks.push(result.value);
        result = await gen.next();
      }
      const metadata = result.value;

      expect(metadata.success).toBe(false);
      expect(metadata.error).toBeTruthy();
      expect(chunks.length).toBe(0);
    });

    it("with inputs", async () => {
      const chunks: Buffer[] = [];
      const gen = runStreaming("num1 + num2", { num1: 25, num2: 17 });
      let result = await gen.next();
      while (!result.done) {
        chunks.push(result.value);
        result = await gen.next();
      }
      const metadata = result.value;

      expect(metadata.success).toBe(true);
      const text = Buffer.concat(chunks).toString("utf-8");
      expect(text.trim()).toBe("42");
    });
  });

  describe("runTransform", () => {
    it("basic bidirectional streaming", async () => {
      const inputData = [Buffer.from("[10, 20, 30, 40, 50]")];
      const script = "output application/json\n---\npayload map ($ * 2)";

      const chunks: Buffer[] = [];
      const gen = runTransform(script, inputData, { mimeType: "application/json" });
      let result = await gen.next();
      while (!result.done) {
        chunks.push(result.value);
        result = await gen.next();
      }
      const metadata = result.value;

      expect(metadata.success).toBe(true);
      const text = Buffer.concat(chunks).toString("utf-8");
      expect(text).toContain("20");
      expect(text).toContain("100");
    });

    it("large chunked input", async () => {
      // Build a large JSON array in chunks
      const parts: Buffer[] = [Buffer.from("[")];
      for (let i = 1; i <= 1000; i++) {
        if (i > 1) parts.push(Buffer.from(","));
        parts.push(Buffer.from(`{"id":${i}}`));
      }
      parts.push(Buffer.from("]"));
      const fullInput = Buffer.concat(parts);

      // Feed in 4KB chunks
      function* chunked(data: Buffer, size = 4096): Generator<Buffer> {
        for (let i = 0; i < data.length; i += size) {
          yield data.subarray(i, i + size);
        }
      }

      const script = "output application/json\n---\nsizeOf(payload)";
      const chunks: Buffer[] = [];
      const gen = runTransform(script, chunked(fullInput), { mimeType: "application/json" });
      let result = await gen.next();
      while (!result.done) {
        chunks.push(result.value);
        result = await gen.next();
      }
      const metadata = result.value;

      expect(metadata.success).toBe(true);
      const text = Buffer.concat(chunks).toString("utf-8");
      expect(text).toBe("1000");
    });

    it("file-based streaming input", async () => {
      const xmlPath = join(__dirname, "fixtures", "person.xml");
      const xmlData = readFileSync(xmlPath);

      function* chunked(data: Buffer, size = 4096): Generator<Buffer> {
        for (let i = 0; i < data.length; i += size) {
          yield data.subarray(i, i + size);
        }
      }

      const script = "output application/csv header=true\n---\n[payload.person]";
      const chunks: Buffer[] = [];
      const gen = runTransform(script, chunked(xmlData), {
        mimeType: "application/xml",
        charset: "UTF-16",
      });
      let result = await gen.next();
      while (!result.done) {
        chunks.push(result.value);
        result = await gen.next();
      }
      const metadata = result.value;

      expect(metadata.success).toBe(true);
      const text = Buffer.concat(chunks).toString("utf-8");
      expect(text).toContain("Billy");
      expect(text).toContain("31");
    });
  });
});
