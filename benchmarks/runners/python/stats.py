"""Percentile + throughput math kept behavior-compatible with
benchmarks/lib/stats.mjs so the Python runner's deltas compare cleanly against
the Node and engine runners. Any divergence is caught by test_bench.py."""

import math


def compute_stats(samples):
    """Nearest-rank percentiles on a sorted copy; mean is the arithmetic mean.
    Mirrors lib/stats.mjs:computeStats."""
    if not samples:
        raise ValueError("compute_stats requires a non-empty sequence of numbers")
    ordered = sorted(samples)
    n = len(ordered)

    def pct(p):
        idx = min(n - 1, max(0, math.ceil(p / 100 * n) - 1))
        return ordered[idx]

    return {
        "min": ordered[0],
        "median": pct(50),
        "p90": pct(90),
        "p99": pct(99),
        "mean": sum(ordered) / n,
    }


def to_mbps(total_bytes, elapsed_ms):
    """Throughput in decimal megabytes per second (1 MB = 1e6 bytes).
    Mirrors lib/stats.mjs:toMBps."""
    if elapsed_ms <= 0:
        raise ValueError("elapsed_ms must be > 0")
    return total_bytes / 1e6 / (elapsed_ms / 1000)
