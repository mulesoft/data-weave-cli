"""Always-on parity guard for the Python runner: nearest-rank stats vs
lib/stats.mjs, manifest parsing, env stamp, and the pure emit/aggregation
logic. Pure stdlib — no dwlib, no venv. Run: python3 runners/python/test_bench.py"""

import unittest
from pathlib import Path
import hashlib
import os
import tempfile

# benchmarks/runners/python -> benchmarks -> corpus
CORPUS = Path(__file__).resolve().parents[2] / "corpus"

import stats
import manifest
import env as envmod
import wrapper
import coldstart
import warm_bench
import emit


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


class TestEnv(unittest.TestCase):
    def test_gather_env_has_required_keys(self):
        e = envmod.gather_env()
        for k in ("os", "cpu", "runtimeVersion", "weaveVersion", "commit", "dwlibBuildId"):
            self.assertIn(k, e)
        self.assertEqual(e["runner"], "python-wrapper")
        self.assertTrue(e["runtimeVersion"].startswith("python "))
        self.assertTrue(e["weaveVersion"])  # non-empty (read from gradle.properties)

    def test_dwlib_build_id_formula(self):
        data = b"hello world" * 10
        with tempfile.NamedTemporaryFile(suffix=".dylib", delete=False) as f:
            f.write(data)
            path = f.name
        os.environ["DATAWEAVE_NATIVE_LIB"] = path
        try:
            e = envmod.gather_env()
            size = os.path.getsize(path)
            h = hashlib.sha256()
            h.update(str(size).encode())
            h.update(data[:65536])
            self.assertEqual(e["dwlibBuildId"], "dwlib-" + h.hexdigest()[:8])
        finally:
            del os.environ["DATAWEAVE_NATIVE_LIB"]
            os.unlink(path)


class TestWrapper(unittest.TestCase):
    def test_load_wrapper_exposes_api(self):
        api = wrapper.load_wrapper()
        for attr in ("DataWeave", "run", "run_transform", "run_streaming"):
            self.assertTrue(hasattr(api, attr), f"binding missing {attr}")


class TestColdstartAggregation(unittest.TestCase):
    def _manifest(self):
        return {
            "corpusDir": str(CORPUS),
            "cases": [
                {"id": "trivial", "script": "scripts/trivial.dwl",
                 "metrics": ["cold-start", "first-run"], "iterations": {"samples": 5}},
                {"id": "object-transform", "script": "scripts/object-transform.dwl",
                 "metrics": ["first-run"], "iterations": {"samples": 5}},
            ],
            "ids": {"trivial", "object-transform"},
        }

    def test_aggregates_injected_samples(self):
        calls = []

        def fake_sample(corpus_dir, case_id):
            calls.append(case_id)
            return (1.0, 2.0)  # (coldStartMs, firstRunMs)

        rows = coldstart.run_cold_start_and_first_run(
            self._manifest(), sample_fn=fake_sample, samples_override=3)

        by = {(r["id"], r["metric"]): r for r in rows}
        self.assertIn(("trivial", "cold-start"), by)
        self.assertIn(("trivial", "first-run"), by)
        self.assertIn(("object-transform", "first-run"), by)
        self.assertNotIn(("object-transform", "cold-start"), by)

        self.assertEqual(by[("trivial", "cold-start")]["unit"], "ms")
        self.assertEqual(by[("trivial", "cold-start")]["stats"]["median"], 1.0)
        self.assertEqual(by[("trivial", "first-run")]["stats"]["median"], 2.0)
        self.assertEqual(by[("trivial", "cold-start")]["iterations"], 3)
        # 2 cases sampled 3 times each.
        self.assertEqual(len(calls), 6)


class _FakeResult:
    def __init__(self, success=True, error=None):
        self.success, self.error = success, error


class _FakeMeta:
    def __init__(self, success=True, error=None):
        self.success, self.error = success, error


class _FakeStream:
    def __init__(self, chunks, meta):
        self._chunks = iter(chunks)
        self.metadata = meta

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._chunks)


class _FakeApi:
    def __init__(self, run_ok=True):
        self.run_ok = run_ok

    def run(self, script, inputs=None):
        return _FakeResult(self.run_ok, None if self.run_ok else "boom")

    def run_transform(self, script, input_stream, input_name="payload",
                      input_mime_type="application/json", input_charset=None):
        for _ in input_stream:  # consume, mimicking the real read side
            pass
        return _FakeStream([b"out-chunk"], _FakeMeta(True))


class TestWarmBench(unittest.TestCase):
    def _warm_manifest(self):
        return {
            "corpusDir": str(CORPUS),
            "cases": [{"id": "trivial", "script": "scripts/trivial.dwl",
                       "metrics": ["warm"], "iterations": {}}],
            "ids": {"trivial"},
        }

    def _streaming_manifest(self):
        return {
            "corpusDir": str(CORPUS),
            "cases": [{"id": "object-transform", "script": "scripts/object-transform.dwl",
                       "inputs": {"payload": {"file": "inputs/person-record.json",
                                              "mimeType": "application/json"}},
                       "metrics": ["streaming"], "iterations": {}}],
            "ids": {"object-transform"},
        }

    def test_warm_rows(self):
        rows = warm_bench.run_warm_and_streaming(
            _FakeApi(), self._warm_manifest(), warmup_cap=1, warm_cap=3)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["metric"], "warm")
        self.assertEqual(rows[0]["unit"], "ms")
        self.assertEqual(rows[0]["iterations"], 3)
        self.assertGreaterEqual(rows[0]["stats"]["median"], 0.0)

    def test_streaming_rows(self):
        rows = warm_bench.run_warm_and_streaming(
            _FakeApi(), self._streaming_manifest(), streaming_cap=2)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["metric"], "streaming")
        self.assertEqual(rows[0]["unit"], "MB/s")
        self.assertGreater(rows[0]["stats"]["median"], 0.0)

    def test_warm_guard_raises_on_failure(self):
        with self.assertRaises(RuntimeError):
            warm_bench.run_warm_and_streaming(
                _FakeApi(run_ok=False), self._warm_manifest(), warmup_cap=1, warm_cap=1)


class TestEmit(unittest.TestCase):
    def test_build_result_shape(self):
        env = {"runner": "python-wrapper", "os": "darwin-arm64", "cpu": "x",
               "runtimeVersion": "python 3.9.6", "weaveVersion": "2.12.0-x",
               "commit": "abc", "dwlibBuildId": "dwlib-x"}
        cases = [{"id": "trivial", "metric": "warm", "unit": "ms",
                  "stats": {"median": 1.0}, "iterations": 3}]
        r = emit.build_result(env, cases)
        self.assertEqual(r["schemaVersion"], "1.0")
        self.assertEqual(r["runner"], "python-wrapper")
        self.assertEqual(r["env"], env)
        self.assertEqual(r["cases"], cases)
        self.assertTrue(r["timestamp"].endswith("Z"))


if __name__ == "__main__":
    unittest.main()
