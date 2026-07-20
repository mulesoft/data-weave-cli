/**
 * A pull-based byte reader: `readCb(bufSize)` returns up to `bufSize` bytes as a
 * fresh {@link Buffer}, or `null` once the input is exhausted. The native
 * transform layer drives it synchronously to pull input on demand.
 */
export type ChunkReader = (bufSize: number) => Buffer | null;

/**
 * Builds a {@link ChunkReader} over an input iterable of byte chunks.
 *
 * Async iterables are fully pre-buffered up front because the native read
 * callback is invoked synchronously on the JS main thread and cannot await;
 * consumed buffers are released as they are read to bound memory. Sync
 * iterables are pulled on demand for constant-memory streaming.
 *
 * @param input - The source of byte chunks (Buffers or Uint8Arrays).
 * @returns A reader that yields the input as `bufSize`-bounded Buffers, then `null`.
 */
export async function createChunkReader(
  input: AsyncIterable<Buffer | Uint8Array> | Iterable<Buffer | Uint8Array>
): Promise<ChunkReader> {
  const isAsync = Symbol.asyncIterator in (input as object);

  if (isAsync) {
    // Async iterables must be pre-buffered because the native read callback
    // is invoked synchronously on the JS main thread and cannot await.
    const inputBuffers: (Buffer | null)[] = [];
    const asyncIter = (input as AsyncIterable<Buffer | Uint8Array>)[Symbol.asyncIterator]();
    try {
      while (true) {
        const { value, done: d } = await asyncIter.next();
        if (d) break;
        inputBuffers.push(Buffer.isBuffer(value) ? value : Buffer.from(value));
      }
    } catch { /* input error = EOF */ }

    let bufIdx = 0;
    let currentBuf: Buffer | null = null;
    let readOffset = 0;

    return (bufSize: number): Buffer | null => {
      while (true) {
        if (currentBuf && readOffset < currentBuf.length) {
          const n = Math.min(currentBuf.length - readOffset, bufSize);
          const slice = currentBuf.subarray(readOffset, readOffset + n);
          readOffset += n;
          if (readOffset >= currentBuf.length) {
            currentBuf = null;
            readOffset = 0;
          }
          return Buffer.from(slice);
        }
        if (bufIdx < inputBuffers.length) {
          currentBuf = inputBuffers[bufIdx];
          inputBuffers[bufIdx] = null; // Release memory as we consume
          bufIdx++;
          readOffset = 0;
          continue;
        }
        return null;
      }
    };
  }

  // Sync iterables are consumed on-demand — constant memory, no pre-buffering.
  const syncIter = (input as Iterable<Buffer | Uint8Array>)[Symbol.iterator]();
  let currentBuf: Buffer | null = null;
  let readOffset = 0;
  let iterDone = false;

  return (bufSize: number): Buffer | null => {
    while (true) {
      if (currentBuf && readOffset < currentBuf.length) {
        const n = Math.min(currentBuf.length - readOffset, bufSize);
        const slice = currentBuf.subarray(readOffset, readOffset + n);
        readOffset += n;
        if (readOffset >= currentBuf.length) {
          currentBuf = null;
          readOffset = 0;
        }
        return Buffer.from(slice);
      }
      if (iterDone) return null;
      const { value, done: d } = syncIter.next();
      if (d) {
        iterDone = true;
        return null;
      }
      currentBuf = Buffer.isBuffer(value) ? value : Buffer.from(value);
      readOffset = 0;
    }
  };
}