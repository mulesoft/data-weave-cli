"""Locate and import the DataWeave Python binding (native-lib/python). The
binding loads the staged dwlib lazily on DataWeave().initialize(), so importing
the module itself is dwlib-free."""

import os
import sys
import importlib
from pathlib import Path

# benchmarks/runners/python -> repo root -> native-lib/python/src
_SRC = Path(__file__).resolve().parents[3] / "native-lib" / "python" / "src"


def resolve_wrapper_site(src=None):
    # Override for pre-downloaded/published wrapper
    if os.environ.get("DW_BENCH_PY_SITE"):
        site = Path(os.environ["DW_BENCH_PY_SITE"])
        if not site.is_dir() or not (site / "dataweave" / "__init__.py").exists():
            raise RuntimeError(
                f"DW_BENCH_PY_SITE={site} does not contain dataweave/__init__.py "
                f"(expected a site-packages-style directory)"
            )
        return site

    return Path(src) if src is not None else _SRC


def resolve_dwlib_path(src=None):
    root = resolve_wrapper_site(src)
    for extension in (".dylib", ".so", ".dll"):
        candidate = root / "dataweave" / "native" / f"dwlib{extension}"
        if candidate.exists():
            return candidate
    return None


def load_wrapper(src=None):
    site = resolve_wrapper_site(src)
    if str(site) not in sys.path:
        sys.path.insert(0, str(site))
    if os.environ.get("DW_BENCH_PY_SITE"):
        selected_site = site.resolve()
        for name, module in list(sys.modules.items()):
            if name != "dataweave" and not name.startswith("dataweave."):
                continue
            module_file = getattr(module, "__file__", None)
            if module_file and not Path(module_file).resolve().is_relative_to(selected_site):
                del sys.modules[name]
        importlib.invalidate_caches()
    try:
        import dataweave
    except ImportError as e:
        raise RuntimeError(
            f"DataWeave Python binding not importable from {site}. "
            f"Run: ./gradlew native-lib:stagePythonNativeLib  ({e})"
        )
    return dataweave
