#!/usr/bin/env python3
"""
Example demonstrating the simplified DataWeave Python module.

This shows how easy it is to use DataWeave without dealing with
any GraalVM or native library complexity.
"""

import sys
from pathlib import Path

_PYTHON_SRC_DIR = Path(__file__).resolve().parent / "python" / "src"
sys.path.insert(0, str(_PYTHON_SRC_DIR))

import dataweave

def example_simple_functions():
    """Example using simple function API"""
    print("="*70)
    print("Example 1: Simple Function API")
    print("="*70)

    ok = True

    # Simple script execution
    print("\n📝 Simple arithmetic:")
    script = "2 + 2"
    result = dataweave.run_script(script)
    ok = assert_result(script, result, "4") and ok

    print("\n📝 Square root:")
    script = "sqrt(144)"
    result = dataweave.run_script(script)
    ok = assert_result(script, result, "12") and ok

    print("\n📝 Array operations:")
    script = "[1, 2, 3] map $ * 2"
    result = dataweave.run_script(script)
    ok = assert_result(script, result, "[\n  2, \n  4, \n  6\n]") and ok

    print("\n📝 String operations:")
    script = "upper('hello world')"
    result = dataweave.run_script(script)
    ok = assert_result(script, result, '"HELLO WORLD"') and ok

    # Script with inputs (simple values - auto-converted)
    print("\n📝 Script with inputs (auto-converted):")
    script = "num1 + num2"
    result = dataweave.run_script(script, {"num1": 25, "num2": 17})
    ok = assert_result(script, result, "42") and ok

    # Script with complex inputs
    print("\n📝 Script with complex object:")
    script = "payload.name"
    result = dataweave.run_script(script, {"payload": {"content": '{"name": "John", "age": 30}', "mimeType": "application/json"}})
    ok = assert_result(script, result, '"John"') and ok

    # Script with mixed input types
    print("\n📝 Script with mixed input types:")
    script = "greeting ++ ' ' ++ payload.name"
    result = dataweave.run_script(script, {"greeting": "Hello", "payload": {"content": '{"name": "Alice", "role": "Developer"}', "mimeType": "application/json"}})
    ok = assert_result(script, result, '"Hello Alice"') and ok

    # Binary output
    print("\n📝 Binary output:")
    script = "output application/octet-stream\n---\ndw::core::Binaries::fromBase64(\"holamund\")"
    result = dataweave.run_script(script)
    ok = assert_result(script, result, "holamund") and ok

    # Script with InputValue
    print("\n📝 Inputs:")
    input_value = dataweave.InputValue(
        content="1234567",
        mimeType="application/csv",
        properties={"header": False, "separator": "4"}
    )
    script = "in0.column_1[0]"
    result = dataweave.run_script(script, {"in0": input_value})
    ok = assert_result(script, result, '"567"') and ok

    # Cleanup when done
    dataweave.cleanup()
    print("\n✓ Cleanup completed")

    return ok


def assert_result(script, result, expected):
    print(f"   {script} = {result}")
    ok = result.get_string() == expected
    if ok:
        status = "✅"
    else:
        status = f"❌ (expected: {expected})"
    print(f"   result as string = {result.get_string()}  {status}")
    print(f"   result as bytes = {result.get_bytes()}")
    return ok


def example_context_manager():
    """Example using context manager (recommended)"""
    print("\n" + "="*70)
    print("Example 2: Context Manager API (Recommended)")
    print("="*70)

    ok = True

    with dataweave.DataWeave() as dw:
        print("\n📝 Multiple operations with same runtime:")

        script = "2 + 2"
        result = dw.run(script)
        ok = assert_result(script, result, "4") and ok

        script = "x + y + z"
        result = dw.run(script, {"x": 1, "y": 2, "z": 3})
        ok = assert_result(script, result, "6") and ok

        script = "numbers map $ * multiplier"
        result = dw.run(script, {"numbers": [1, 2, 3, 4, 5], "multiplier": 10})
        ok = assert_result(script, result, "[\n  10, \n  20, \n  30, \n  40, \n  50\n]") and ok

    print("\n✓ Context manager automatically cleaned up resources")

    return ok


def example_explicit_format():
    """Example using explicit content/mimeType format"""
    print("\n" + "="*70)
    print("Example 3: Explicit Format (Advanced)")
    print("="*70)
    
    print("\n📝 Using explicit content and mimeType:")

    ok = True

    script = "payload.message"
    result = dataweave.run_script(script, {"payload": {"content": '{"message": "Hello from JSON!", "value": 42}', "mimeType": "application/json"}})
    ok = assert_result(script, result, '"Hello from JSON!"') and ok

    script = "payload.value + offset"
    result = dataweave.run_script(script, {"payload": {"content": '{"value": 100}', "mimeType": "application/json"}, "offset": 50})
    ok = assert_result(script, result, "150") and ok

    return ok


def example_error_handling():
    """Example with error handling"""
    print("\n" + "="*70)
    print("Example 4: Error Handling")
    print("="*70)
    
    try:
        print("\n📝 Invalid script (will show error):")
        result = dataweave.run_script("invalid syntax here", {})
        print(f"   Result: {result} {'✅' if result.success == False else '❌'}")
            
    except dataweave.DataWeaveLibraryNotFoundError as e:
        print(f"❌ Library not found: {e}")
        print("   Please build the library first: ./gradlew nativeCompile")
    except dataweave.DataWeaveError as e:
        print(f"❌ DataWeave error: {e}")


def main():
    """Run all examples"""
    print("\n" + "="*70)
    print("DataWeave Python Module - Examples")
    print("="*70)
    print("\nThis module abstracts all GraalVM/native complexity!")
    print("Just import and use - no ctypes, no manual memory management.\n")
    
    try:
        all_ok = True
        all_ok = example_simple_functions() and all_ok
        all_ok = example_context_manager() and all_ok
        all_ok = example_explicit_format() and all_ok
        example_error_handling()

        print("\n" + "="*70)
        if all_ok:
            print("✓ All examples completed successfully!")
        else:
            print("✗ One or more examples failed")
        print("="*70)
        
    except dataweave.DataWeaveLibraryNotFoundError as e:
        print(f"\n❌ Error: {e}")
        print("\nPlease build the native library first:")
        print("  ./gradlew nativeCompile")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
