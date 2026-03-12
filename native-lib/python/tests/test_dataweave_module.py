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
        result = dataweave.run_script("2 + 2", {})
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
        result = dataweave.run_script("num1 + num2", {"num1": 25, "num2": 17})
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

        result = dataweave.run_script(
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
        result = dataweave.run_script(
            "numbers[0]",
            {"numbers": [1, 2, 3]}
        )
        assert result.get_string() == "1", f"Expected '1', got '{result.get_string()}'"

        print("[OK] Auto-conversion works")
        return True
    except Exception as e:
        print(f"[FAIL] Auto-conversion failed: {e}")
        return False

def test_streaming_basic():
    """Test basic streaming execution"""
    print("\nTesting streaming basic execution...")
    try:
        with dataweave.run_stream("2 + 2") as stream:
            assert stream.mimeType is not None, "Expected mimeType in metadata"
            result = stream.read_all_string()
            assert result == "4", f"Expected '4', got '{result}'"
        print("[OK] Streaming basic execution works")
        return True
    except Exception as e:
        print(f"[FAIL] Streaming basic execution failed: {e}")
        return False

def test_streaming_with_inputs():
    """Test streaming execution with inputs"""
    print("\nTesting streaming with inputs...")
    try:
        with dataweave.run_stream("num1 + num2", {"num1": 25, "num2": 17}) as stream:
            result = stream.read_all_string()
            assert result == "42", f"Expected '42', got '{result}'"
        print("[OK] Streaming with inputs works")
        return True
    except Exception as e:
        print(f"[FAIL] Streaming with inputs failed: {e}")
        return False

def test_streaming_chunked_read():
    """Test streaming with small chunk reads"""
    print("\nTesting streaming chunked read...")
    try:
        script = """output application/json
---
{items: (1 to 100) map {id: $, name: "item_" ++ $}}"""
        with dataweave.run_stream(script) as stream:
            chunks = []
            while True:
                chunk = stream.read(32)
                if not chunk:
                    break
                chunks.append(chunk)
            full = b"".join(chunks)
            assert len(chunks) > 1, f"Expected multiple chunks, got {len(chunks)}"
            assert b"item_1" in full, "Expected 'item_1' in result"
            assert b"item_100" in full, "Expected 'item_100' in result"
        print(f"[OK] Streaming chunked read works ({len(chunks)} chunks)")
        return True
    except Exception as e:
        print(f"[FAIL] Streaming chunked read failed: {e}")
        return False

def test_streaming_iterator():
    """Test streaming via iterator protocol"""
    print("\nTesting streaming iterator...")
    try:
        with dataweave.run_stream("output application/json --- [1,2,3]") as stream:
            chunks = list(stream)
            full = b"".join(chunks)
            text = full.decode(stream.charset or "utf-8")
            assert "1" in text and "3" in text, f"Expected [1,2,3] in result, got '{text}'"
        print("[OK] Streaming iterator works")
        return True
    except Exception as e:
        print(f"[FAIL] Streaming iterator failed: {e}")
        return False

def test_streaming_context_manager():
    """Test that streaming context manager properly cleans up"""
    print("\nTesting streaming context manager cleanup...")
    try:
        with dataweave.DataWeave() as dw:
            with dw.run_stream("sqrt(144)") as stream:
                result = stream.read_all_string()
                assert result == "12", f"Expected '12', got '{result}'"
        print("[OK] Streaming context manager works")
        return True
    except Exception as e:
        print(f"[FAIL] Streaming context manager failed: {e}")
        return False

def test_streaming_input_basic():
    """Test streaming input with a separate feeder thread"""
    print("\nTesting streaming input basic...")
    try:
        import threading

        with dataweave.DataWeave() as dw:
            input_stream = dw.open_input_stream("application/json")

            def feed():
                try:
                    input_stream.write(b'{"name": "Alice", "age": 30}')
                    input_stream.close()
                except Exception as e:
                    print(f"  Feed error: {e}")

            t = threading.Thread(target=feed)
            t.start()

            with dw.run_stream(
                "output application/json\n---\npayload.name",
                inputs={"payload": input_stream},
            ) as out:
                result = out.read_all_string()

            t.join(timeout=5)
            assert result == '"Alice"', f"Expected '\"Alice\"', got '{result}'"

        print("[OK] Streaming input basic works")
        return True
    except Exception as e:
        print(f"[FAIL] Streaming input basic failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_streaming_input_large():
    """Test streaming a large input in chunks from a feeder thread"""
    print("\nTesting streaming input large...")
    try:
        import threading

        with dataweave.DataWeave() as dw:
            input_stream = dw.open_input_stream("application/json")

            def feed():
                try:
                    # Build a large JSON array and stream it in chunks
                    data = b"["
                    for i in range(1, 501):
                        if i > 1:
                            data += b","
                        data += f'{{"id":{i},"val":"item_{i}"}}'.encode()
                    data += b"]"
                    chunk_size = 4096
                    for offset in range(0, len(data), chunk_size):
                        input_stream.write(data[offset:offset + chunk_size])
                    input_stream.close()
                except Exception as e:
                    print(f"  Feed error: {e}")

            t = threading.Thread(target=feed)
            t.start()

            with dw.run_stream(
                "output application/json\n---\nsizeOf(payload)",
                inputs={"payload": input_stream},
            ) as out:
                result = out.read_all_string()

            t.join(timeout=10)
            assert result == "500", f"Expected '500', got '{result}'"

        print("[OK] Streaming input large works")
        return True
    except Exception as e:
        print(f"[FAIL] Streaming input large failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_streaming_input_context_manager():
    """Test DataWeaveInputStream as a context manager"""
    print("\nTesting streaming input context manager...")
    try:
        import threading

        with dataweave.DataWeave() as dw:
            input_stream = dw.open_input_stream("application/json")

            def feed():
                try:
                    with input_stream:
                        input_stream.write(b'[1, 2, 3]')
                except Exception as e:
                    print(f"  Feed error: {e}")

            t = threading.Thread(target=feed)
            t.start()

            with dw.run_stream(
                "output application/json\n---\npayload[2]",
                inputs={"payload": input_stream},
            ) as out:
                result = out.read_all_string()

            t.join(timeout=5)
            assert result == "3", f"Expected '3', got '{result}'"

        print("[OK] Streaming input context manager works")
        return True
    except Exception as e:
        print(f"[FAIL] Streaming input context manager failed: {e}")
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
        results.append(test_streaming_basic())
        results.append(test_streaming_with_inputs())
        results.append(test_streaming_chunked_read())
        results.append(test_streaming_iterator())
        results.append(test_streaming_context_manager())
        results.append(test_streaming_input_basic())
        results.append(test_streaming_input_large())
        results.append(test_streaming_input_context_manager())
        
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
