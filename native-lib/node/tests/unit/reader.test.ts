import { describe, it, expect } from "vitest";
import { createChunkReader } from "../../src/reader";

/** Drains a reader with a fixed bufSize, returning the ordered chunks it yielded. */
function drain(readCb: (n: number) => Buffer | null, bufSize: number): Buffer[] {
  const out: Buffer[] = [];
  let chunk: Buffer | null;
  while ((chunk = readCb(bufSize)) !== null) {
    out.push(chunk);
  }
  return out;
}

/** Drains a reader and concatenates everything into a single Buffer. */
function drainAll(readCb: (n: number) => Buffer | null, bufSize: number): Buffer {
  return Buffer.concat(drain(readCb, bufSize));
}

async function* asyncOf(...chunks: Array<Buffer | Uint8Array>): AsyncGenerator<Buffer | Uint8Array> {
  for (const c of chunks) yield c;
}

describe("createChunkReader (sync input)", () => {
  it("reassembles the full input across a small bufSize", async () => {
    const read = await createChunkReader([Buffer.from("hello"), Buffer.from(" world")]);
    expect(drainAll(read, 3).toString()).toBe("hello world");
  });

  it("returns null immediately for empty input", async () => {
    const read = await createChunkReader([]);
    expect(read(16)).toBeNull();
  });

  it("skips empty buffers between data", async () => {
    const read = await createChunkReader([Buffer.from("ab"), Buffer.from(""), Buffer.from("cd")]);
    expect(drainAll(read, 16).toString()).toBe("abcd");
  });

  it("bounds each chunk by bufSize", async () => {
    const read = await createChunkReader([Buffer.from("abcdef")]);
    const chunks = drain(read, 2);
    expect(chunks.map((c) => c.toString())).toEqual(["ab", "cd", "ef"]);
  });

  it("returns a whole buffer when bufSize exceeds its length", async () => {
    const read = await createChunkReader([Buffer.from("abc"), Buffer.from("de")]);
    const chunks = drain(read, 100);
    expect(chunks.map((c) => c.toString())).toEqual(["abc", "de"]);
  });

  it("accepts Uint8Array chunks", async () => {
    const read = await createChunkReader([new Uint8Array([104, 105])]);
    expect(drainAll(read, 16).toString()).toBe("hi");
  });

  it("keeps returning null after exhaustion", async () => {
    const read = await createChunkReader([Buffer.from("x")]);
    drain(read, 16);
    expect(read(16)).toBeNull();
    expect(read(16)).toBeNull();
  });

  it("does not share memory with the source buffer (returns copies)", async () => {
    const source = Buffer.from("abcd");
    const read = await createChunkReader([source]);
    const chunk = read(4)!;
    source[0] = 0x7a; // mutate 'a' -> 'z' after reading
    expect(chunk.toString()).toBe("abcd");
  });
});

describe("createChunkReader (async input)", () => {
  it("reassembles the full input across a small bufSize", async () => {
    const read = await createChunkReader(asyncOf(Buffer.from("foo"), Buffer.from("bar")));
    expect(drainAll(read, 2).toString()).toBe("foobar");
  });

  it("returns null immediately for empty async input", async () => {
    const read = await createChunkReader(asyncOf());
    expect(read(16)).toBeNull();
  });

  it("pre-buffers the whole async source before any read", async () => {
    let produced = 0;
    async function* counting(): AsyncGenerator<Buffer> {
      for (const s of ["a", "b", "c"]) {
        produced++;
        yield Buffer.from(s);
      }
    }
    const read = await createChunkReader(counting());
    // createChunkReader resolving means the async source is fully consumed.
    expect(produced).toBe(3);
    expect(drainAll(read, 16).toString()).toBe("abc");
  });

  it("bounds each chunk by bufSize", async () => {
    const read = await createChunkReader(asyncOf(Buffer.from("abcde")));
    const chunks = drain(read, 2);
    expect(chunks.map((c) => c.toString())).toEqual(["ab", "cd", "e"]);
  });

  it("treats an input error as end-of-input, keeping what was buffered", async () => {
    async function* boom(): AsyncGenerator<Buffer> {
      yield Buffer.from("ok");
      throw new Error("source failed");
    }
    const read = await createChunkReader(boom());
    expect(drainAll(read, 16).toString()).toBe("ok");
  });

  it("accepts Uint8Array chunks", async () => {
    const read = await createChunkReader(asyncOf(new Uint8Array([120, 121])));
    expect(drainAll(read, 16).toString()).toBe("xy");
  });
});