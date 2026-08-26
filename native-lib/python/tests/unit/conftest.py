import pytest

from dataweave import native


@pytest.fixture(autouse=True)
def _reset_shared_isolate():
    """Every unit test starts and ends with no shared isolate held.

    The isolate/lib/refcount now live at module scope in dataweave.native, so a
    test that leaves a ref behind would leak into the next test. Tests fake the
    library, so tearing down here is just clearing globals -- no real native call.
    """
    _clear()
    yield
    _clear()


def _clear():
    native._lib = None
    native._isolate = None
    native._isolate_thread = None
    native._isolate_owner_thread = None
    native._isolate_ref_count = 0
