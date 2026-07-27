"""Env stamp for the Python runner, mirroring benchmarks/lib/env.mjs. Includes a
REAL dwlibBuildId — the Python runner wraps the same staged dwlib as Node, so an
unchanged lib yields the same id in both runners' result files."""

import hashlib
import os
import platform
import re
import subprocess
import sys
from pathlib import Path

# benchmarks/runners/python -> repo root
_REPO_ROOT = Path(__file__).resolve().parents[3]
_ENV_NATIVE_LIB = "DATAWEAVE_NATIVE_LIB"


def _read_weave_version():
    txt = (_REPO_ROOT / "gradle.properties").read_text(encoding="utf-8")
    m = re.search(r"^weaveVersion=(.+)$", txt, re.MULTILINE)
    if not m:
        raise RuntimeError("weaveVersion not found in gradle.properties")
    return m.group(1).strip()


def _read_commit():
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=_REPO_ROOT, stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return "unknown"


def _normalize_arch(machine):
    # Match Node's process.arch vocabulary so labels read identically across runners.
    return {"x86_64": "x64", "amd64": "x64"}.get(machine, machine)


def _cpu_model():
    try:
        if sys.platform == "darwin":
            return subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], stderr=subprocess.DEVNULL,
            ).decode().strip()
        if sys.platform.startswith("linux"):
            for line in Path("/proc/cpuinfo").read_text().splitlines():
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except Exception:
        pass
    return platform.machine() or "unknown"


def _dwlib_path():
    override = (os.environ.get(_ENV_NATIVE_LIB) or "").strip()
    if override:
        return Path(override)
    base = _REPO_ROOT / "native-lib" / "python" / "src" / "dataweave" / "native"
    for ext in (".dylib", ".so", ".dll"):
        p = base / f"dwlib{ext}"
        if p.exists():
            return p
    return None


def _read_dwlib_build_id():
    # sha256 over (size + first 64KB), same formula as lib/env.mjs.
    p = _dwlib_path()
    if p and p.exists():
        size = p.stat().st_size
        head = p.read_bytes()[:65536]
        h = hashlib.sha256()
        h.update(str(size).encode())
        h.update(head)
        return "dwlib-" + h.hexdigest()[:8]
    return "unknown"


def gather_env():
    return {
        "runner": "python-wrapper",
        "os": f"{sys.platform}-{_normalize_arch(platform.machine())}",
        "cpu": _cpu_model(),
        "runtimeVersion": f"python {platform.python_version()}",
        "weaveVersion": _read_weave_version(),
        "commit": _read_commit(),
        "dwlibBuildId": _read_dwlib_build_id(),
    }
