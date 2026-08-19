from pathlib import Path

import pytest

import dataweave


@pytest.mark.unit
def test_parse_native_response_rejects_malformed_json():
    result = dataweave._parse_native_encoded_response("not json")

    assert result.success is False
    assert result.error.startswith("Failed to parse native JSON response:")


@pytest.mark.unit
@pytest.mark.parametrize("raw, expected_error", [
    (None, "Native returned null"),
    ("", "Native returned empty response"),
    ("[]", "Native response JSON is not an object"),
])
def test_parse_native_response_rejects_invalid_native_values(raw, expected_error):
    result = dataweave._parse_native_encoded_response(raw)

    assert result == dataweave.ExecutionResult(False, None, expected_error, False, None, None)


@pytest.mark.unit
def test_candidate_paths_prioritize_environment_override(monkeypatch, tmp_path):
    override = tmp_path / "custom-dwlib"
    monkeypatch.setenv("DATAWEAVE_NATIVE_LIB", str(override))

    paths = dataweave._candidate_library_paths()

    assert paths[0] == override
    assert paths[1:4] == [
        Path(dataweave.__file__).resolve().parent / "native" / "dwlib.dylib",
        Path(dataweave.__file__).resolve().parent / "native" / "dwlib.so",
        Path(dataweave.__file__).resolve().parent / "native" / "dwlib.dll",
    ]


@pytest.mark.unit
def test_decode_and_free_releases_native_string_when_decoding_fails(monkeypatch):
    freed = []
    runtime = dataweave.DataWeave.__new__(dataweave.DataWeave)
    runtime._thread = "thread"
    runtime._lib = type("Native", (), {"free_cstring": lambda _self, thread, ptr: freed.append((thread, ptr))})()
    monkeypatch.setattr(dataweave.ctypes, "string_at", lambda _ptr: b"\xff")

    with pytest.raises(UnicodeDecodeError):
        runtime._decode_and_free(123)

    assert freed == [("thread", 123)]
