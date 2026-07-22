/**
 * Aggregate a list of samples into min/median/p90/p99/mean.
 * Percentiles use the nearest-rank method on a sorted copy.
 * @param {number[]} samples
 * @returns {{min:number, median:number, p90:number, p99:number, mean:number}}
 */
export function computeStats(samples) {
  if (!Array.isArray(samples) || samples.length === 0) {
    throw new Error("computeStats requires a non-empty array of numbers");
  }
  const sorted = [...samples].sort((a, b) => a - b);
  const n = sorted.length;
  const pct = (p) => sorted[Math.min(n - 1, Math.max(0, Math.ceil((p / 100) * n) - 1))];
  const sum = sorted.reduce((a, b) => a + b, 0);
  return { min: sorted[0], median: pct(50), p90: pct(90), p99: pct(99), mean: sum / n };
}

/**
 * Throughput in megabytes per second (decimal MB, i.e. 1e6 bytes).
 * @param {number} totalBytes
 * @param {number} elapsedMs
 * @returns {number}
 */
export function toMBps(totalBytes, elapsedMs) {
  if (elapsedMs <= 0) throw new Error("elapsedMs must be > 0");
  return totalBytes / 1e6 / (elapsedMs / 1000);
}
