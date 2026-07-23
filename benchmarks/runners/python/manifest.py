"""Parse and validate corpus/manifest.json, resolve scripts + inputs.
Mirrors benchmarks/lib/manifest.mjs."""

import json
from pathlib import Path

# The only metrics a case may declare.
METRICS = ("cold-start", "first-run", "warm", "streaming")


def load_manifest(corpus_dir):
    corpus_dir = Path(corpus_dir)
    raw = json.loads((corpus_dir / "manifest.json").read_text(encoding="utf-8"))
    cases = raw.get("cases")
    if not isinstance(cases, list):
        raise ValueError("manifest.cases must be an array")
    ids = set()
    for c in cases:
        cid = c.get("id")
        if not cid:
            raise ValueError("manifest case is missing an id")
        if cid in ids:
            raise ValueError(f"duplicate case id: {cid}")
        ids.add(cid)
        metrics = c.get("metrics")
        if not isinstance(metrics, list) or not metrics:
            raise ValueError(f"case {cid} must declare a non-empty metrics[]")
        for m in metrics:
            if m not in METRICS:
                raise ValueError(f"case {cid} has unknown metric: {m}")
        script = c.get("script")
        if not script or not (corpus_dir / script).exists():
            raise ValueError(f"case {cid} script not found: {script}")
        for name, inp in (c.get("inputs") or {}).items():
            if inp.get("file") and not inp.get("generated") \
                    and not (corpus_dir / inp["file"]).exists():
                raise ValueError(f"case {cid} input '{name}' file not found: {inp['file']}")
    return {"corpusDir": str(corpus_dir), "cases": cases, "ids": ids}


def cases_for_metric(manifest, metric):
    return [c for c in manifest["cases"] if metric in c["metrics"]]


def resolve_inputs(manifest, case_obj):
    """Read a case's declared inputs into bytes."""
    corpus_dir = Path(manifest["corpusDir"])
    out = {}
    for name, inp in (case_obj.get("inputs") or {}).items():
        data = (corpus_dir / inp["file"]).read_bytes()
        out[name] = {"bytes": data, "mimeType": inp["mimeType"], "charset": inp.get("charset")}
    return out


def read_script(manifest, case_obj):
    return (Path(manifest["corpusDir"]) / case_obj["script"]).read_text(encoding="utf-8")


def validate_result_ids(manifest, result_ids):
    """Fail-fast: raise if any result id is not present in the manifest."""
    for rid in result_ids:
        if rid not in manifest["ids"]:
            raise ValueError(f"result contains orphan id not in manifest: {rid}")
