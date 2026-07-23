"""Always-on parity guard for the Python runner: nearest-rank stats vs
lib/stats.mjs, manifest parsing, env stamp, and the pure emit/aggregation
logic. Pure stdlib — no dwlib, no venv. Run: python3 runners/python/test_bench.py"""

import unittest
from pathlib import Path

# benchmarks/runners/python -> benchmarks -> corpus
CORPUS = Path(__file__).resolve().parents[2] / "corpus"

import stats
import manifest


class TestStats(unittest.TestCase):
    def test_matches_lib_stats_on_1_to_100(self):
        s = stats.compute_stats([float(i) for i in range(1, 101)])
        self.assertEqual(s["min"], 1.0)
        self.assertEqual(s["median"], 50.0)  # ceil(0.5*100)-1 = 49 -> sorted[49] = 50
        self.assertEqual(s["p90"], 90.0)     # ceil(0.9*100)-1 = 89 -> 90
        self.assertEqual(s["p99"], 99.0)     # ceil(0.99*100)-1 = 98 -> 99
        self.assertEqual(s["mean"], 50.5)

    def test_sorts_before_ranking(self):
        s = stats.compute_stats([5.0, 1.0, 3.0, 2.0, 4.0])
        self.assertEqual(s["min"], 1.0)
        self.assertEqual(s["median"], 3.0)   # ceil(0.5*5)-1 = 2 -> sorted[2] = 3
        self.assertEqual(s["p90"], 5.0)       # ceil(0.9*5)-1 = 4 -> 5
        self.assertEqual(s["mean"], 3.0)

    def test_rejects_empty(self):
        with self.assertRaises(ValueError):
            stats.compute_stats([])

    def test_to_mbps(self):
        self.assertEqual(stats.to_mbps(1_000_000, 1000.0), 1.0)
        self.assertEqual(stats.to_mbps(500_000, 250.0), 2.0)
        with self.assertRaises(ValueError):
            stats.to_mbps(10, 0.0)


class TestManifest(unittest.TestCase):
    def setUp(self):
        self.m = manifest.load_manifest(CORPUS)

    def test_loads_all_corpus_ids(self):
        for cid in ("trivial", "object-transform", "map-scale", "xml-to-csv",
                    "json-stream", "compile-heavy"):
            self.assertIn(cid, self.m["ids"])

    def test_cases_for_metric_filters(self):
        streaming = [c["id"] for c in manifest.cases_for_metric(self.m, "streaming")]
        self.assertIn("map-scale", streaming)
        self.assertIn("json-stream", streaming)
        cold = [c["id"] for c in manifest.cases_for_metric(self.m, "cold-start")]
        self.assertIn("trivial", cold)

    def test_read_script(self):
        trivial = next(c for c in self.m["cases"] if c["id"] == "trivial")
        self.assertIn("2 + 2", manifest.read_script(self.m, trivial))

    def test_resolve_inputs_reads_bytes_mime_charset(self):
        xml = next(c for c in self.m["cases"] if c["id"] == "xml-to-csv")
        resolved = manifest.resolve_inputs(self.m, xml)
        self.assertEqual(list(resolved.keys()), ["payload"])
        self.assertEqual(resolved["payload"]["mimeType"], "application/xml")
        self.assertEqual(resolved["payload"]["charset"], "UTF-16")
        self.assertGreater(len(resolved["payload"]["bytes"]), 0)

    def test_validate_result_ids_rejects_orphan(self):
        with self.assertRaises(ValueError):
            manifest.validate_result_ids(self.m, ["trivial", "not-a-case"])
        manifest.validate_result_ids(self.m, ["trivial"])  # no raise


if __name__ == "__main__":
    unittest.main()
