package org.mule.weave.benchmark.engine

import java.io.InputStream

/** An InputStream over an in-memory byte array that returns at most `chunkSize`
  * bytes per read() call, pacing input into the DataWeave runtime the way a
  * network/pipe feeder would. The bytes are already resident, so this models
  * "runtime reads input lazily" without a real producer thread. Mirrors the
  * native-lib runners feeding fixed-size chunks. */
class ChunkedInputStream(data: Array[Byte], chunkSize: Int) extends InputStream {
  private var pos: Int = 0

  // Single-byte read returns one byte (correct for this overload); chunk pacing
  // applies to the bulk read(b,off,len) below, which DataWeave actually calls.
  override def read(): Int = {
    if (pos >= data.length) -1
    else {
      val b = data(pos) & 0xff
      pos += 1
      b
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (pos >= data.length) -1
    else {
      val n = math.min(math.min(len, chunkSize), data.length - pos)
      System.arraycopy(data, pos, b, off, n)
      pos += n
      n
    }
  }

  override def available(): Int = data.length - pos
}
