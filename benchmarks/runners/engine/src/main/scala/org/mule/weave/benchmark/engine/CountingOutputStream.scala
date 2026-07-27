package org.mule.weave.benchmark.engine

import java.io.OutputStream

/** An OutputStream that discards all bytes and only counts how many were written.
  * Used as the write sink when benchmarking — we measure the work of producing
  * output without paying for allocation/retention of the result. */
class CountingOutputStream extends OutputStream {
  private var written: Long = 0L

  override def write(b: Int): Unit = { written += 1 }

  override def write(b: Array[Byte]): Unit = { written += b.length }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = { written += len }

  def count(): Long = written
}
