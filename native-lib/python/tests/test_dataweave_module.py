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
