import base64

import pytest

import dataweave
from dataweave import encoding


@pytest.mark.unit
def test_public_encoding_functions_are_used_by_compatibility_aliases():
    assert dataweave._normalize_input_value is encoding.normalize_input_value
    assert dataweave._parse_native_encoded_response is encoding.parse_native_encoded_response
    assert dataweave._parse_streaming_result is encoding.parse_streaming_result


@pytest.mark.unit
def test_normalize_plain_text_uses_text_mime_type():
    normalized = dataweave._normalize_input_value("hello")

    assert normalized == {
        "content": base64.b64encode(b"hello").decode("ascii"),
        "mimeType": "text/plain",
        "charset": "utf-8",
    }


@pytest.mark.unit
def test_normalize_explicit_bytes_preserves_mime_charset_and_properties():
    normalized = dataweave._normalize_input_value({
        "content": b"caf\xe9",
        "mimeType": "text/plain",
        "charset": "latin-1",
        "properties": {"header": False},
    })

    assert normalized == {
        "content": base64.b64encode(b"caf\xe9").decode("ascii"),
        "mimeType": "text/plain",
        "charset": "latin-1",
        "properties": {"header": False},
    }


@pytest.mark.unit
@pytest.mark.parametrize("value", [
    {"content": "body"},
    {"mimeType": "text/plain"},
])
def test_normalize_explicit_input_requires_content_and_mime_type(value):
    with pytest.raises(dataweave.DataWeaveError, match="must include both 'content' and 'mimeType'"):
        dataweave._normalize_input_value(value)


@pytest.mark.unit
def test_normalize_explicit_input_rejects_unsupported_keys():
    with pytest.raises(dataweave.DataWeaveError, match="unsupported keys: unexpected"):
        dataweave._normalize_input_value({
            "content": "body",
            "mimeType": "text/plain",
            "unexpected": True,
        })
