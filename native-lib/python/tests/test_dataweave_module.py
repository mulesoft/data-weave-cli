#!/usr/bin/env python3
"""
Quick test script for the DataWeave Python module.
"""

import sys
from pathlib import Path

_PYTHON_SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(_PYTHON_SRC_DIR))

import dataweave

def test_basic():
    """Test basic functionality"""
    print("Testing basic script execution...")
    try:
        result = dataweave.run("2 + 2", {})
        assert result.get_string() == "4", f"Expected '4', got '{result.get_string()}'"
        print("[OK] Basic script execution works")
        return True
    except Exception as e:
        print(f"[FAIL] Basic script execution failed: {e}")
        return False

def test_with_inputs():
    """Test script with inputs"""
    print("\nTesting script with inputs...")
    try:
        result = dataweave.run("num1 + num2", {"num1": 25, "num2": 17})
        assert result.get_string() == "42", f"Expected '42', got '{result.get_string()}'"
        print("[OK] Script with inputs works")
        return True
    except Exception as e:
        print(f"[FAIL] Script with inputs failed: {e}")
        return False

def test_context_manager():
    """Test context manager"""
    print("\nTesting with context manager...")
    try:
        with dataweave.DataWeave() as dw:

            result = dw.run("sqrt(144)")
            assert result.get_string() == "12", f"Expected '12', got '{result.get_string()}'"
            result = dw.run("sqrt(10000)")
            assert result.get_string() == "100", f"Expected '100', got '{result.get_string()}'"
            print("[OK] Script execution witch context manager works")
            return True
    except Exception as e:
        print(f"[FAIL] Script execution witch context manager failed: {e}")
        return False

def test_encoding():
    """Test reading UTF-16 XML input and producing CSV output"""
    print("\nTesting encoding (UTF-16 XML -> CSV)...")
    try:
        xml_path = (
            Path(__file__).resolve().parent / "person.xml"
        )
        xml_bytes = xml_path.read_bytes()

        script = """output application/csv header=true
---
[payload.person]
"""

        result = dataweave.run(
            script,
            {
                "payload": {
                    "content": xml_bytes,
                    "mimeType": "application/xml",
                    "charset": "UTF-16",
                }
            },
        )

        out = result.get_string() or ""
        print(f"out: \n{out}")
        assert result.success is True, f"Expected success=true, got: {result}"
        assert "name" in out and "age" in out, f"CSV header missing, got: {out!r}"
        assert "Billy" in out, f"Expected name 'Billy' in CSV, got: {out!r}"
        assert "31" in out, f"Expected age '31' in CSV, got: {out!r}"

        print("[OK] Encoding conversion works")
        return True
    except Exception as e:
        print(f"[FAIL] Encoding conversion failed: {e}")
        return False

def test_auto_conversion():
    """Test auto-conversion of different types"""
    print("\nTesting auto-conversion...")
    try:

        # Test array
        result = dataweave.run(
            "numbers[0]",
            {"numbers": [1, 2, 3]}
        )
        assert result.get_string() == "1", f"Expected '1', got '{result.get_string()}'"

        print("[OK] Auto-conversion works")
        return True
    except Exception as e:
        print(f"[FAIL] Auto-conversion failed: {e}")
        return False

def test_callback_output_basic():
    """Test callback-based output streaming"""
    print("\nTesting callback output basic...")
    try:
        chunks = []

        def on_write(data: bytes) -> int:
            chunks.append(data)
            return 0

        result = dataweave.run_callback("2 + 2", on_write)
        assert result.success is True, f"Expected success, got: {result}"
        full = b"".join(chunks)
        text = full.decode(result.charset or "utf-8")
        assert text == "4", f"Expected '4', got '{text}'"
        print(f"[OK] Callback output basic works (chunks={len(chunks)}, result='{text}')")
        return True
    except Exception as e:
        print(f"[FAIL] Callback output basic failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_callback_output_with_inputs():
    """Test callback-based output streaming with inputs"""
    print("\nTesting callback output with inputs...")
    try:
        chunks = []

        def on_write(data: bytes) -> int:
            chunks.append(data)
            return 0

        result = dataweave.run_callback("num1 + num2", on_write, inputs={"num1": 25, "num2": 17})
        assert result.success is True, f"Expected success, got: {result}"
        full = b"".join(chunks)
        text = full.decode(result.charset or "utf-8")
        assert text == "42", f"Expected '42', got '{text}'"
        print(f"[OK] Callback output with inputs works (result='{text}')")
        return True
    except Exception as e:
        print(f"[FAIL] Callback output with inputs failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_callback_input_output():
    """Test callback-based input and output streaming"""
    print("\nTesting callback input+output...")
    try:
        import io as _io

        source = _io.BytesIO(b'[10, 20, 30, 40, 50]')
        output_chunks = []

        def on_read(buf_size: int) -> bytes:
            return source.read(buf_size)

        def on_write(data: bytes) -> int:
            output_chunks.append(data)
            return 0

        script = "output application/json\n---\npayload map ($ * 2)"
        result = dataweave.run_input_output_callback(
            script,
            input_name="payload",
            input_mime_type="application/json",
            read_callback=on_read,
            write_callback=on_write,
        )
        assert result.success is True, f"Expected success, got: {result}"
        full = b"".join(output_chunks)
        text = full.decode(result.charset or "utf-8")
        assert "20" in text, f"Expected 20 in result (10*2), got: {text}"
        assert "100" in text, f"Expected 100 in result (50*2), got: {text}"
        print(f"[OK] Callback input+output works (chunks={len(output_chunks)}, result={text.strip()[:80]}...)")
        return True
    except Exception as e:
        print(f"[FAIL] Callback input+output failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_run_streaming_basic():
    """Test run_streaming yields chunks and returns metadata"""
    print("\nTesting run_streaming basic...")
    try:
        stream = dataweave.run_streaming("output application/json --- {a: 1, b: 2}")
        chunks = []
        try:
            while True:
                chunks.append(next(stream))
        except StopIteration as e:
            metadata = e.value

        full = b"".join(chunks)
        assert len(full) > 0, "Expected non-empty output"
        text = full.decode(metadata.charset or "utf-8")
        assert '"a": 1' in text or '"a":1' in text, f"Expected key 'a' in JSON, got: {text}"
        assert metadata.success is True, f"Expected success, got: {metadata}"
        assert metadata.mime_type == "application/json", f"Expected json mime, got: {metadata.mime_type}"
        print(f"[OK] run_streaming basic works (chunks={len(chunks)}, result={text.strip()[:60]})")
        return True
    except Exception as e:
        print(f"[FAIL] run_streaming basic failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_run_streaming_large():
    """Test run_streaming with large output to verify true streaming (multiple chunks)"""
    print("\nTesting run_streaming large...")
    try:
        script = 'output application/json --- (1 to 5000) map {id: $, name: "item_" ++ $}'
        stream = dataweave.run_streaming(script)
        chunks = []
        try:
            while True:
                chunks.append(next(stream))
        except StopIteration as e:
            metadata = e.value

        full = b"".join(chunks)
        text = full.decode(metadata.charset or "utf-8")
        assert metadata.success is True, f"Expected success, got: {metadata}"
        assert len(chunks) > 1, f"Expected multiple chunks for large output, got {len(chunks)}"
        assert '"id": 5000' in text or '"id":5000' in text, f"Expected last item in output"
        print(f"[OK] run_streaming large works (chunks={len(chunks)}, bytes={len(full)})")
        return True
    except Exception as e:
        print(f"[FAIL] run_streaming large failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_run_streaming_error():
    """Test run_streaming with an invalid script returns error metadata"""
    print("\nTesting run_streaming error...")
    try:
        stream = dataweave.run_streaming("output application/json --- invalid_var")
        chunks = []
        try:
            while True:
                chunks.append(next(stream))
        except StopIteration as e:
            metadata = e.value

        assert metadata.success is False, f"Expected failure, got: {metadata}"
        assert metadata.error is not None, "Expected error message"
        assert len(chunks) == 0, f"Expected no chunks on error, got {len(chunks)}"
        print(f"[OK] run_streaming error works (error={metadata.error[:60]}...)")
        return True
    except Exception as e:
        print(f"[FAIL] run_streaming error failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_run_streaming_with_inputs():
    """Test run_streaming with input bindings"""
    print("\nTesting run_streaming with inputs...")
    try:
        stream = dataweave.run_streaming("num1 + num2", {"num1": 25, "num2": 17})
        chunks = []
        try:
            while True:
                chunks.append(next(stream))
        except StopIteration as e:
            metadata = e.value

        full = b"".join(chunks)
        text = full.decode(metadata.charset or "utf-8")
        assert metadata.success is True, f"Expected success, got: {metadata}"
        assert text.strip() == "42", f"Expected '42', got '{text.strip()}'"
        print(f"[OK] run_streaming with inputs works (result='{text.strip()}')")
        return True
    except Exception as e:
        print(f"[FAIL] run_streaming with inputs failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_callback_input_output_large():
    """Test callback-based input+output streaming with large data"""
    print("\nTesting callback input+output large...")
    try:
        import io as _io

        # Build a large JSON array
        parts = [b"["]
        for i in range(1, 1001):
            if i > 1:
                parts.append(b",")
            parts.append(f'{{"id":{i}}}'.encode())
        parts.append(b"]")
        source = _io.BytesIO(b"".join(parts))
        output_chunks = []

        def on_read(buf_size: int) -> bytes:
            return source.read(buf_size)

        def on_write(data: bytes) -> int:
            output_chunks.append(data)
            return 0

        result = dataweave.run_input_output_callback(
            "output application/json\n---\nsizeOf(payload)",
            input_name="payload",
            input_mime_type="application/json",
            read_callback=on_read,
            write_callback=on_write,
        )
        assert result.success is True, f"Expected success, got: {result}"
        full = b"".join(output_chunks)
        text = full.decode(result.charset or "utf-8")
        assert text == "1000", f"Expected '1000', got '{text}'"
        print(f"[OK] Callback input+output large works (result='{text}')")
        return True
    except Exception as e:
        print(f"[FAIL] Callback input+output large failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_run_transform_basic():
    """Test run_transform with an iterable input and streaming output"""
    print("\nTesting run_transform basic...")
    try:
        input_data = [b'[10, 20, 30, 40, 50]']
        script = "output application/json\n---\npayload map ($ * 2)"
        stream = dataweave.run_transform(script, input_stream=input_data, input_mime_type="application/json")
        chunks = []
        try:
            while True:
                chunks.append(next(stream))
        except StopIteration as e:
            metadata = e.value

        full = b"".join(chunks)
        text = full.decode(metadata.charset or "utf-8")
        assert metadata.success is True, f"Expected success, got: {metadata}"
        assert "20" in text, f"Expected 20 in result (10*2), got: {text}"
        assert "100" in text, f"Expected 100 in result (50*2), got: {text}"
        print(f"[OK] run_transform basic works (chunks={len(chunks)}, result={text.strip()[:60]})")
        return True
    except Exception as e:
        print(f"[FAIL] run_transform basic failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_run_transform_large():
    """Test run_transform with large chunked input to verify streaming both directions"""
    print("\nTesting run_transform large...")
    try:
        import io as _io

        # Build a large JSON array as chunked input
        parts = [b"["]
        for i in range(1, 1001):
            if i > 1:
                parts.append(b",")
            parts.append(f'{{"id":{i}}}'.encode())
        parts.append(b"]")
        full_input = b"".join(parts)

        # Feed in 4KB chunks (simulating a file/network read)
        def chunked(data, size=4096):
            for i in range(0, len(data), size):
                yield data[i:i+size]

        script = "output application/json\n---\nsizeOf(payload)"
        stream = dataweave.run_transform(
            script,
            input_stream=chunked(full_input),
            input_mime_type="application/json",
        )
        chunks = []
        try:
            while True:
                chunks.append(next(stream))
        except StopIteration as e:
            metadata = e.value

        full = b"".join(chunks)
        text = full.decode(metadata.charset or "utf-8")
        assert metadata.success is True, f"Expected success, got: {metadata}"
        assert text == "1000", f"Expected '1000', got '{text}'"
        print(f"[OK] run_transform large works (result='{text}')")
        return True
    except Exception as e:
        print(f"[FAIL] run_transform large failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_run_transform_with_file():
    """Test run_transform reading from a file-like object"""
    print("\nTesting run_transform with file...")
    try:
        from pathlib import Path

        xml_path = Path(__file__).resolve().parent / "person.xml"

        with open(xml_path, "rb") as f:
            stream = dataweave.run_transform(
                "output application/csv header=true\n---\n[payload.person]",
                input_stream=iter(lambda: f.read(4096), b""),
                input_mime_type="application/xml",
                input_charset="UTF-16",
            )
            chunks = []
            try:
                while True:
                    chunks.append(next(stream))
            except StopIteration as e:
                metadata = e.value

        full = b"".join(chunks)
        text = full.decode(metadata.charset or "utf-8")
        assert metadata.success is True, f"Expected success, got: {metadata}"
        assert "Billy" in text, f"Expected 'Billy' in CSV, got: {text}"
        assert "31" in text, f"Expected '31' in CSV, got: {text}"
        print(f"[OK] run_transform with file works (result={text.strip()[:60]})")
        return True
    except Exception as e:
        print(f"[FAIL] run_transform with file failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests"""
    print("="*70)
    print("DataWeave Python Module - Test Suite")
    print("="*70)
    
    try:
        results = []
        results.append(test_basic())
        results.append(test_with_inputs())
        results.append(test_context_manager())
        results.append(test_encoding())
        results.append(test_auto_conversion())
        results.append(test_callback_output_basic())
        results.append(test_callback_output_with_inputs())
        results.append(test_callback_input_output())
        results.append(test_callback_input_output_large())
        results.append(test_run_streaming_basic())
        results.append(test_run_streaming_large())
        results.append(test_run_streaming_error())
        results.append(test_run_streaming_with_inputs())
        results.append(test_run_transform_basic())
        results.append(test_run_transform_large())
        results.append(test_run_transform_with_file())
        
        # Cleanup
        dataweave.cleanup()
        
        print("\n" + "="*70)
        passed = sum(results)
        total = len(results)
        print(f"Results: {passed}/{total} tests passed")
        print("="*70)
        
        if passed == total:
            print("\n[OK] All tests passed!")
            sys.exit(0)
        else:
            print(f"\n[FAIL] {total - passed} test(s) failed")
            sys.exit(1)
            
    except dataweave.DataWeaveLibraryNotFoundError as e:
        print(f"\n[ERROR] {e}")
        print("\nPlease build the native library first:")
        print("  ./gradlew nativeCompile")
        sys.exit(2)
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
