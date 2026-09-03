import { parseStreamingResult } from "./result";
import type { StreamingResult } from "./types";

/**
 * Starts a native streaming call, wiring its chunk callback to `chunkCb` and
 * resolving to the raw trailing metadata JSON once the stream completes.
 */
export type StartStreaming = (chunkCb: (chunk: Buffer) => void) => Promise<string>;

/**
 * Bridges a native push-based streaming call into a pull-based async generator.
 *
 * The native side pushes output chunks through the callback while
 * {@link StartStreaming} runs; this generator buffers them and yields in order,
 * parking the consumer when no chunk is ready and waking it on the next push or
 * on completion. After all chunks drain, it awaits the native promise and
 * returns the parsed {@link StreamingResult}.
 *
 * @param start - Launches the native call and returns its metadata promise.
 * @returns An async generator of output chunks whose return value is the terminal metadata.
 */
export async function* streamFromNative(
  start: StartStreaming
): AsyncGenerator<Buffer, StreamingResult, undefined> {
  const chunks: Buffer[] = [];
  const pendingResolves: Array<() => void> = [];
  let done = false;
  let metaRaw: string | null = null;

  const chunkCb = (chunk: Buffer) => {
    chunks.push(chunk);
    // Resolve one waiting consumer if any
    const resolve = pendingResolves.shift();
    if (resolve) {
      resolve();
    }
  };

  let startError: unknown;
  let startRejected = false;
  const wakeAll = () => {
    while (pendingResolves.length > 0) {
      const resolve = pendingResolves.shift();
      if (resolve) resolve();
    }
  };

  // Handle BOTH settlement branches. Without the rejection handler, a rejected
  // start() leaves `done` false forever: a consumer parked in next() below is
  // never woken and the generator hangs, and the rejection is unhandled
  // (review #6 #2). On rejection we record the error, flip startRejected, mark
  // completion, and wake every waiter; the error is re-thrown (by settlement
  // state, not by value -- see below) after draining any chunks that arrived
  // before the rejection. Because we handle rejection here, metaPromise itself
  // always fulfills -- `await metaPromise` below never throws.
  const metaPromise = start(chunkCb).then(
    (raw) => { metaRaw = raw; done = true; wakeAll(); },
    (err) => { startError = err; startRejected = true; done = true; wakeAll(); }
  );

  while (true) {
    if (chunks.length > 0) {
      yield chunks.shift()!;
      continue;
    }
    if (done) break;
    await new Promise<void>((resolve) => { pendingResolves.push(resolve); });
  }

  // Drain remaining chunks buffered before completion/rejection.
  while (chunks.length > 0) {
    yield chunks.shift()!;
  }

  await metaPromise;
  // Track rejection by settlement STATE, not by the rejected value: Promise.reject(undefined)
  // is valid JS, so a value sentinel (startError !== undefined) would swallow it as an empty
  // result. startRejected is only ever set in the rejection handler above (review #7 #6).
  if (startRejected) throw startError;
  return parseStreamingResult(metaRaw ?? "");
}