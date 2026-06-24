#!/usr/bin/env python3
"""
DataWeave Python Bindings - Simple Demo

Demonstrates basic usage of the DataWeave Python bindings.
"""

import sys
import os

# Add parent directory to path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import dataweave

def main():
    print("=== DataWeave Python Demo ===\n")

    # Example 1: Simple arithmetic
    print("1. Simple arithmetic:")
    result = dataweave.run("2 + 2")
    if result.success:
        output = result.get_string()
        print(f"   2 + 2 = {output}\n")
    else:
        print(f"   Error: {result.error}\n")

    # Example 2: Script with inputs
    print("2. Script with inputs:")
    inputs = {"name": "World"}
    result = dataweave.run('"Hello, " ++ name ++ "!"', inputs)
    if result.success:
        output = result.get_string()
        print(f"   {output}\n")
    else:
        print(f"   Error: {result.error}\n")

    # Example 3: JSON transformation
    print("3. JSON transformation:")
    inputs = {
        "payload": {
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ]
        }
    }
    script = "output application/json --- payload.users map { name: $.name }"
    result = dataweave.run(script, inputs)
    if result.success:
        output = result.get_string()
        print(f"   {output}\n")
    else:
        print(f"   Error: {result.error}\n")

    # Example 4: Using context manager
    print("4. Using context manager:")
    with dataweave.DataWeave() as dw:
        r1 = dw.run("10 * 5")
        r2 = dw.run("a + b", {"a": 100, "b": 23})

        if r1.success:
            print(f"   10 * 5 = {r1.get_string()}")
        if r2.success:
            print(f"   100 + 23 = {r2.get_string()}")
    print()

    # Example 5: Error handling with raise_on_error
    print("5. Error handling:")
    try:
        result = dataweave.run("invalid syntax here", raise_on_error=True)
        print(f"   Result: {result.get_string()}")
    except dataweave.DataWeaveScriptError as e:
        print(f"   Caught script error: {e.result.error[:50]}...")
    print()

    # Example 6: Using InputValue for explicit configuration
    print("6. Using InputValue helper:")
    input_value = dataweave.InputValue(
        content="1,2,3,4,5",
        mime_type="application/csv",
        properties={"header": False, "separator": ","}
    )
    result = dataweave.run("payload.column_1[0]", {"payload": input_value})
    if result.success:
        print(f"   First CSV value: {result.get_string()}\n")
    else:
        print(f"   Error: {result.error}\n")

    print("Demo complete!")

if __name__ == "__main__":
    try:
        main()
    except dataweave.DataWeaveLibraryNotFoundError:
        print("\nERROR: Native library not found!")
        print("Build it first: ./gradlew :native-lib:nativeCompile")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1)
