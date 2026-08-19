import io

import pytest

import dataweave


@pytest.mark.integration
def test_callback_streams_basic_output():
    chunks = []

    def on_write(data: bytes) -> int:
        chunks.append(data)
        return 0

    result = dataweave.run_callback("2 + 2", on_write)

    assert result.success is True
    assert b"".join(chunks).decode(result.charset or "utf-8") == "4"


@pytest.mark.integration
def test_callback_streams_output_with_inputs():
    chunks = []

    def on_write(data: bytes) -> int:
        chunks.append(data)
        return 0

    result = dataweave.run_callback("num1 + num2", on_write, inputs={"num1": 25, "num2": 17})

    assert result.success is True
    assert b"".join(chunks).decode(result.charset or "utf-8") == "42"


@pytest.mark.integration
def test_callback_transforms_streamed_input_and_output():
    source = io.BytesIO(b"[10, 20, 30, 40, 50]")
    chunks = []

    def on_read(buffer_size: int) -> bytes:
        return source.read(buffer_size)

    def on_write(data: bytes) -> int:
        chunks.append(data)
        return 0

    result = dataweave.run_input_output_callback(
        "output application/json\n---\npayload map ($ * 2)",
        input_name="payload",
        input_mime_type="application/json",
        read_callback=on_read,
        write_callback=on_write,
    )

    output = b"".join(chunks).decode(result.charset or "utf-8")
    assert result.success is True
    assert "20" in output
    assert "100" in output


@pytest.mark.integration
def test_callback_accepts_large_streamed_input():
    records = b"[" + b",".join(f'{{"id":{index}}}'.encode() for index in range(1, 1001)) + b"]"
    source = io.BytesIO(records)
    chunks = []

    def on_read(buffer_size: int) -> bytes:
        return source.read(buffer_size)

    def on_write(data: bytes) -> int:
        chunks.append(data)
        return 0

    result = dataweave.run_input_output_callback(
        "output application/json\n---\nsizeOf(payload)",
        input_name="payload",
        input_mime_type="application/json",
        read_callback=on_read,
        write_callback=on_write,
    )

    assert result.success is True
    assert b"".join(chunks).decode(result.charset or "utf-8") == "1000"
