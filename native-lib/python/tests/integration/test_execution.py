from pathlib import Path

import pytest

import dataweave


@pytest.mark.integration
def test_input_value_accepts_public_mime_type_constructor_keyword():
    value = dataweave.InputValue(
        content="1234567",
        mime_type="application/csv",
        properties={"header": False, "separator": "4"},
    )

    assert value.mime_type == "application/csv"


@pytest.mark.integration
def test_runs_basic_script():
    result = dataweave.run("2 + 2", {})

    assert result.get_string() == "4"


@pytest.mark.integration
def test_runs_script_with_inputs():
    result = dataweave.run("num1 + num2", {"num1": 25, "num2": 17})

    assert result.get_string() == "42"


@pytest.mark.integration
def test_converts_utf16_xml_input_to_csv():
    xml_path = Path(__file__).resolve().parents[1] / "person.xml"
    script = """output application/csv header=true
---
[payload.person]
"""

    result = dataweave.run(
        script,
        {
            "payload": {
                "content": xml_path.read_bytes(),
                "mimeType": "application/xml",
                "charset": "UTF-16",
            }
        },
    )

    output = result.get_string() or ""
    assert result.success is True
    assert "name" in output and "age" in output
    assert "Billy" in output
    assert "31" in output


@pytest.mark.integration
def test_converts_python_list_input_automatically():
    result = dataweave.run("numbers[0]", {"numbers": [1, 2, 3]})

    assert result.get_string() == "1"
