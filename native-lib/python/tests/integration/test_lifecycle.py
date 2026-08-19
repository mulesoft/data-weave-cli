import pytest

import dataweave


@pytest.mark.integration
def test_context_manager_runs_multiple_scripts():
    with dataweave.DataWeave() as dw:
        assert dw.run("sqrt(144)").get_string() == "12"
        assert dw.run("sqrt(10000)").get_string() == "100"
