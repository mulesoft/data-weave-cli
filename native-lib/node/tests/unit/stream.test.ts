import { describe, it, expect } from "vitest";
import { streamFromNative } from "../../src/stream";
import type { StreamingResult } from "../../src/types";

const okMeta = (extra: Record<string, unknown> = {}) =>
  JSON.stringify({ success: true, mimeType: "application/json", charset: "utf-8", binary: false, ...extra });

/** Fully consumes a generator, returning its yielded chunks and terminal return value. */
async function collect(
  gen: AsyncGenerator<Buffer, StreamingResult, undefined>
): Promise<{ chunks: Buffer[]; result: StreamingResult }> {
  const chunks: Buffer[] = [];
  let r = await gen.next();
  while (!r.done) {
    chunks.push(r.value);
    r = await gen.next();
  }
  return { chunks, result: r.value };
}

function deferred<T>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

describe("streamFromNative", () => {
  it("yields chunks pushed before completion, in order", async () => {
    const { chunks, result } = await collect(
      streamFromNative((cb) => {
        cb(Buffer.from("a"));
        cb(Buffer.from("b"));
        cb(Buffer.from("c"));
        return Promise.resolve(okMeta());
      })
    );
    expect(chunks.map((c) => c.toString())).toEqual(["a", "b", "c"]);
    expect(result.success).toBe(true);
    expect(result.mimeType).toBe("application/json");
  });

  it("returns success metadata with no chunks", async () => {
    const { chunks, result } = await collect(streamFromNative(() => Promise.resolve(okMeta())));
    expect(chunks).toEqual([]);
    expect(result.success).toBe(true);
  });

  it("parks the consumer until a chunk arrives, then wakes it (backpressure)", async () => {
    const meta = deferred<string>();
    let push!: (chunk: Buffer) => void;
    const gen = streamFromNative((cb) => {
      push = cb;
      return meta.promise;
    });

    // First pull starts the generator and parks — no chunk is ready yet.
    const pending = gen.next();
    // Producing a chunk should wake the parked consumer.
    push(Buffer.from("late"));
    const first = await pending;
    expect(first.done).toBe(false);
    expect(first.value!.toString()).toBe("late");

    // Completing the stream ends the generator with the parsed metadata.
    meta.resolve(okMeta({ mimeType: "text/plain" }));
    const last = await gen.next();
    expect(last.done).toBe(true);
    expect((last.value as StreamingResult).mimeType).toBe("text/plain");
  });

  it("drains chunks that arrive together with completion", async () => {
    const { chunks, result } = await collect(
      streamFromNative((cb) => {
        // Chunks buffered but not yet consumed when the native call resolves.
        cb(Buffer.from("x"));
        cb(Buffer.from("y"));
        return Promise.resolve(okMeta());
      })
    );
    expect(chunks.map((c) => c.toString())).toEqual(["x", "y"]);
    expect(result.success).toBe(true);
  });

  it("propagates a failure envelope as the terminal result", async () => {
    const { chunks, result } = await collect(
      streamFromNative(() => Promise.resolve(JSON.stringify({ success: false, error: "stream boom" })))
    );
    expect(chunks).toEqual([]);
    expect(result.success).toBe(false);
    expect(result.error).toBe("stream boom");
  });

  it("treats empty terminal metadata as a failure", async () => {
    const { result } = await collect(streamFromNative(() => Promise.resolve("")));
    expect(result.success).toBe(false);
    expect(result.error).toBe("Empty response");
  });

  it("rejects a parked consumer when native start() rejects (no hang)", async () => {
    const startGate = deferred<string>();
    const gen = streamFromNative(() => startGate.promise);

    // Park a consumer in next() BEFORE the start promise settles: no chunk is
    // ready and done is false, so next() awaits on pendingResolves.
    const pending = gen.next();

    // Now reject the native start. The parked consumer must be woken and see a
    // rejection -- on the pre-fix code done never flips and this hangs forever.
    startGate.reject(new Error("native start boom"));

    await expect(pending).rejects.toThrow("native start boom");
  });

  it("drains buffered chunks, then throws, when start() rejects after pushing chunks", async () => {
    const gen = streamFromNative((cb) => {
      cb(Buffer.from("x"));
      cb(Buffer.from("y"));
      return Promise.reject(new Error("late boom"));
    });

    // Buffered chunks yield first...
    const a = await gen.next();
    const b = await gen.next();
    expect([a.value?.toString(), b.value?.toString()]).toEqual(["x", "y"]);

    // ...then the drained generator surfaces the start error.
    await expect(gen.next()).rejects.toThrow("late boom");
  });
});