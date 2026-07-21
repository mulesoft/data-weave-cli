// Integration edge cases beyond the happy-path suite in dataweave.test.ts:
// concurrency / FFI lifecycle, and the error & edge-input matrix. These run
// against the real native library (dwlib). (Closes gaps F3 and F5 in
// native-lib/node/TESTING_ASSESSMENT.md.)
import { describe, it, expect, afterAll } from "vitest";
import { DataWeave, run, runStreaming, runTransform, cleanup } from "../../src/index";
import type { StreamingResult } from "../../src/types";

afterAll(() => {
  cleanup();
});

/** Drains a streaming/transform generator, returning its chunks and terminal metadata. */
async function drain(gen: AsyncGenerator<Buffer, StreamingResult, undefined>) {
  const chunks: Buffer[] = [];
  let r = await gen.next();
  while (!r.done) {
    chunks.push(r.value);
    r = await gen.next();
  }
  return { text: Buffer.concat(chunks).toString("utf-8"), chunks, metadata: r.value };
}

// --- Concurrency / FFI edge cases ----------------------------------------

describe("runTransform with async-iterable input", () => {
  it("consumes an async generator (pre-buffered read path)", async () => {
    async function* asyncChunks(): AsyncGenerator<Buffer> {
      for (const s of ["[1, 2, ", "3, 4", ", 5]"]) yield Buffer.from(s);
    }
    const { text, metadata } = await drain(
      runTransform("output application/json\n---\npayload map ($ * 10)", asyncChunks(), {
        mimeType: "application/json",
      })
    );
    expect(metadata.success).toBe(true);
    expect(text).toContain("10");
    expect(text).toContain("50");
  });

  it("handles an empty async input", async () => {
    async function* empty(): AsyncGenerator<Buffer> { /* yields nothing */ }
    const { metadata } = await drain(
      runTransform("output application/json\n---\n{ ok: true }", empty(), { mimeType: "application/json" })
    );
    // Script ignores payload; empty input must not hang or crash.
    expect(metadata.success).toBe(true);
  });

  it("treats a throwing async input as end-of-input for what was produced", async () => {
    async function* boom(): AsyncGenerator<Buffer> {
      yield Buffer.from("[1, 2, 3]");
      throw new Error("source failed");
    }
    const { text, metadata } = await drain(
      runTransform("output application/json\n---\nsizeOf(payload)", boom(), { mimeType: "application/json" })
    );
    expect(metadata.success).toBe(true);
    expect(text.trim()).toBe("3");
  });
});

describe("multi-instance lifecycle", () => {
  it("runs two independent instances and cleans them up independently", () => {
    const a = new DataWeave();
    const b = new DataWeave();
    a.initialize();
    b.initialize();
    try {
      expect(a.run("1 + 1").getString()).toBe("2");
      expect(b.run("2 + 3").getString()).toBe("5");
    } finally {
      a.cleanup();
      b.cleanup();
    }
    // After cleanup, a fresh instance still works (runtime not permanently torn down).
    const c = new DataWeave();
    c.initialize();
    try {
      expect(c.run("6 * 7").getString()).toBe("42");
    } finally {
      c.cleanup();
    }
  });

  it("initialize is idempotent and re-initialization after cleanup works", () => {
    const dw = new DataWeave();
    dw.initialize();
    dw.initialize(); // no-op, must not throw
    expect(dw.run("1").getString()).toBe("1");
    dw.cleanup();
    dw.cleanup(); // double cleanup, must not throw
    dw.initialize(); // re-init
    try {
      expect(dw.run("2").getString()).toBe("2");
    } finally {
      dw.cleanup();
    }
  });

  it("run before initialize throws a DataWeaveError", () => {
    const dw = new DataWeave();
    expect(() => dw.run("1 + 1")).toThrow(/not initialized/i);
  });

  // NOTE: initialize() with a bad library path throwing a DataWeaveError is only
  // reliably observable as the FIRST initialization in a process — the native
  // runtime is loaded process-globally (ref-counted, see g_ref_count in
  // addon.c), so once any good init has run, a later bad-path init is tolerated
  // rather than re-dlopen'd. Because this shared suite loads dwlib in earlier
  // tests, that path can't be asserted here without a dedicated isolated process
  // (a follow-up: run it via a separate vitest pool/child so no prior init has
  // happened). Verified manually: in a fresh process it throws DataWeaveError.
});

describe("concurrent execution", () => {
  it("runs many concurrent scripts on the shared singleton", async () => {
    const results = await Promise.all(
      Array.from({ length: 25 }, (_, i) => Promise.resolve().then(() => run(`${i} * 2`)))
    );
    results.forEach((r, i) => {
      expect(r.success).toBe(true);
      expect(r.getString()).toBe(String(i * 2));
    });
  });

  it("interleaves multiple concurrent streaming generators", async () => {
    const gens = Array.from({ length: 4 }, (_, i) =>
      runStreaming(`output application/json --- (1 to 200) map ($ + ${i * 1000})`)
    );
    const drained = await Promise.all(gens.map(drain));
    drained.forEach(({ metadata, text }, i) => {
      expect(metadata.success).toBe(true);
      expect(text).toContain(String(i * 1000 + 1));
    });
  });
});

// --- Error & edge-input matrix -------------------------------------------

describe("binary output", () => {
  it("marks binary output and round-trips through getBytes", () => {
    const r = run("output application/octet-stream\n---\npayload", {
      payload: { content: Buffer.from([0, 1, 2, 255]), mimeType: "application/octet-stream" },
    });
    expect(r.success).toBe(true);
    expect(r.binary).toBe(true);
    expect(r.getBytes()!.equals(Buffer.from([0, 1, 2, 255]))).toBe(true);
  });
});

describe("output charsets (buffered path)", () => {
  // Regression guard for the getString() charset bug: the runtime reports IANA
  // charset names that Node's Buffer.toString rejects; decodeBytes normalizes them.
  for (const enc of ["UTF-8", "UTF-16", "UTF-16LE", "UTF-16BE", "ISO-8859-1"]) {
    it(`decodes ${enc} output without throwing`, () => {
      const r = run(`output text/plain encoding="${enc}"\n---\n"café"`);
      expect(r.success).toBe(true);
      expect(() => r.getString()).not.toThrow();
      expect(r.getString()).toBe("café");
    });
  }

  it("substitutes unrepresentable characters for US-ASCII output", () => {
    const r = run(`output text/plain encoding="US-ASCII"\n---\n"café"`);
    expect(r.success).toBe(true);
    // 'é' is not representable in ASCII; the runtime substitutes it on encode.
    expect(r.getString()).toBe("caf?");
  });
});

describe("malformed / unsupported input", () => {
  it("fails on an unknown input mime type", () => {
    const r = run("payload", { payload: { content: "x", mimeType: "application/nonsense-xyz" } });
    expect(r.success).toBe(false);
    expect(r.error).toBeTruthy();
  });

  it("fails on an unknown output mime type", () => {
    const r = run("output application/nonsense-xyz\n---\n{ a: 1 }");
    expect(r.success).toBe(false);
    expect(r.error).toBeTruthy();
  });

  it("fails when referencing a missing input", () => {
    const r = run("missingInput + 1");
    expect(r.success).toBe(false);
    expect(r.error).toBeTruthy();
  });

  it("fails on a script with a compilation/syntax error", () => {
    const r = run("output application/json\n---\n{ unclosed:");
    expect(r.success).toBe(false);
    expect(r.error).toBeTruthy();
  });

  it("surfaces malformed input content as a failed result", () => {
    const r = run("payload.value", { payload: { content: "{ not valid json", mimeType: "application/json" } });
    expect(r.success).toBe(false);
    expect(r.error).toBeTruthy();
  });
});