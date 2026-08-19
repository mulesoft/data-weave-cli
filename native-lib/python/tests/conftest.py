import sys
from pathlib import Path

import pytest


PYTHON_SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(PYTHON_SRC_DIR))

import dataweave


@pytest.fixture(autouse=True)
def clean_dataweave_runtime():
    """Keep module-level isolate state from leaking between integration tests."""
    dataweave.cleanup()
    yield
    dataweave.cleanup()


@pytest.fixture
def collect_stream():
    def collect(stream):
        chunks = list(stream)
        return b"".join(chunks), stream.metadata

    return collect
