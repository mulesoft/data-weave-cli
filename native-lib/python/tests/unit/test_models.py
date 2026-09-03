import base64

import ctypes
import pytest

import dataweave
from dataweave import models


@pytest.mark.unit
def test_public_models_are_exported_from_models_module():
    assert models.ExecutionResult is dataweave.ExecutionResult
    assert models.InputValue is dataweave.InputValue
    assert models.StreamingResult is dataweave.StreamingResult
    assert models.DataWeaveError is dataweave.DataWeaveError
    assert models.DataWeaveScriptError is dataweave.DataWeaveScriptError
    assert models.DataWeaveLibraryNotFoundError is dataweave.DataWeaveLibraryNotFoundError
    assert models.READ_CALLBACK is dataweave.READ_CALLBACK
    assert models.WRITE_CALLBACK is dataweave.WRITE_CALLBACK


@pytest.mark.unit
def test_resolve_module_callback_has_ctx_argument():
    # thread, ctx, module_path
    assert models.RESOLVE_MODULE_CALLBACK._argtypes_ == (
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_char_p,
    )
    assert models.RESOLVE_MODULE_CALLBACK._restype_ is ctypes.c_void_p


@pytest.mark.unit
def test_input_value_encodes_text_with_its_charset():
    value = dataweave.InputValue("caf\u00e9", charset="latin-1")

    assert value.encode_content() == base64.b64encode(b"caf\xe9").decode("ascii")


@pytest.mark.unit
def test_execution_result_decodes_text_payload():
    result = dataweave.ExecutionResult(
        success=True,
        result=base64.b64encode(b"hello").decode("ascii"),
        error=None,
        binary=False,
        mime_type="text/plain",
        charset="utf-8",
    )

    assert result.get_bytes() == b"hello"
    assert result.get_string() == "hello"


@pytest.mark.unit
def test_execution_result_keeps_binary_payload_as_base64_text():
    payload = base64.b64encode(b"\x00\xff").decode("ascii")
    result = dataweave.ExecutionResult(True, payload, None, True, "application/octet-stream", None)

    assert result.get_bytes() == b"\x00\xff"
    assert result.get_string() == payload


@pytest.mark.unit
def test_execution_result_returns_none_for_unsuccessful_execution():
    result = dataweave.ExecutionResult(False, None, "failed", False, None, None)

    assert result.get_bytes() is None
    assert result.get_string() is None


@pytest.mark.unit
def test_parse_native_response_preserves_error_result():
    result = dataweave._parse_native_encoded_response('{"success": false, "error": "script failed"}')

    assert result == dataweave.ExecutionResult(False, None, "script failed", False, None, None)


@pytest.mark.unit
def test_stream_public_close_and_context_manager_close_the_underlying_generator():
    closed = []

    def generate():
        try:
            yield b"chunk"
        finally:
            closed.append(True)

    stream = dataweave.Stream(generate())

    with stream as managed:
        assert next(managed) == b"chunk"

    stream.close()

    assert closed == [True]
