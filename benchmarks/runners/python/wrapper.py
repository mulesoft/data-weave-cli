"""Locate and import the DataWeave Python binding (native-lib/python). The
binding loads the staged dwlib lazily on DataWeave().initialize(), so importing
the module itself is dwlib-free."""

import sys
from pathlib import Path

# benchmarks/runners/python -> repo root -> native-lib/python/src
_SRC = Path(__file__).resolve().parents[3] / "native-lib" / "python" / "src"


def load_wrapper(src=None):
    src = Path(src) if src is not None else _SRC
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    try:
        import dataweave
    except ImportError as e:
        raise RuntimeError(
            f"DataWeave Python binding not importable from {src}. "
            f"Run: ./gradlew native-lib:stagePythonNativeLib  ({e})"
        )
    return dataweave
