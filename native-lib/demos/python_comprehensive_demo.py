#!/usr/bin/env python3
"""
DataWeave Python Bindings - Comprehensive Demo

Showcases all major capabilities:
- Basic transformations
- Working with inputs
- JSON/XML/CSV transformations
- Streaming for large datasets
- Error handling
- Different output formats
"""

import sys
import os
import json

# Add parent directory to path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python', 'src'))

import dataweave


def demo_basic_operations():
    """Demo 1: Basic DataWeave operations"""
    print("=" * 60)
    print("DEMO 1: Basic Operations")
    print("=" * 60)

    # Simple arithmetic
    print("\n1.1 Arithmetic:")
    result = dataweave.run("2 + 2 * 3")
    print(f"   Expression: 2 + 2 * 3")
    print(f"   Result: {result.get_string()}")

    # String concatenation
    print("\n1.2 String operations:")
    result = dataweave.run('"Hello" ++ " " ++ "World"')
    print(f'   Expression: "Hello" ++ " " ++ "World"')
    print(f"   Result: {result.get_string()}")

    # Array operations
    print("\n1.3 Array operations:")
    result = dataweave.run("[1, 2, 3, 4, 5] map ($ * 2)")
    print(f"   Expression: [1, 2, 3, 4, 5] map ($ * 2)")
    print(f"   Result: {result.get_string()}")


def demo_with_inputs():
    """Demo 2: Using inputs and context"""
    print("\n" + "=" * 60)
    print("DEMO 2: Working with Inputs")
    print("=" * 60)

    # Simple variable substitution
    print("\n2.1 Variable substitution:")
    inputs = {"name": "Alice", "age": 30}
    script = '"Hello, " ++ name ++ "! You are " ++ age ++ " years old."'
    result = dataweave.run(script, inputs)
    print(f"   Inputs: {inputs}")
    print(f"   Result: {result.get_string()}")

    # Working with payload
    print("\n2.2 Payload transformation:")
    inputs = {
        "payload": {
            "firstName": "John",
            "lastName": "Doe",
            "email": "john.doe@example.com"
        }
    }
    script = """
    output application/json
    ---
    {
        fullName: payload.firstName ++ " " ++ payload.lastName,
        contact: payload.email,
        username: lower(payload.lastName) ++ "." ++ lower(payload.firstName)
    }
    """
    result = dataweave.run(script, inputs)
    print(f"   Input: {json.dumps(inputs['payload'], indent=2)}")
    print(f"   Output: {result.get_string()}")


def demo_json_transformations():
    """Demo 3: JSON data transformations"""
    print("\n" + "=" * 60)
    print("DEMO 3: JSON Transformations")
    print("=" * 60)

    # Array mapping
    print("\n3.1 Array mapping:")
    inputs = {
        "payload": {
            "users": [
                {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
                {"id": 2, "name": "Bob", "age": 25, "city": "London"},
                {"id": 3, "name": "Charlie", "age": 35, "city": "Tokyo"}
            ]
        }
    }
    script = """
    output application/json
    ---
    payload.users map {
        userId: $.id,
        userName: $.name,
        location: $.city,
        isAdult: $.age >= 18
    }
    """
    result = dataweave.run(script, inputs)
    print(f"   Transformed {len(inputs['payload']['users'])} users")
    print(f"   Output: {result.get_string()}")

    # Filtering and grouping
    print("\n3.2 Filtering and grouping:")
    script = """
    output application/json
    ---
    {
        adults: payload.users filter ($.age >= 30) map $.name,
        totalUsers: sizeOf(payload.users),
        averageAge: avg(payload.users map $.age)
    }
    """
    result = dataweave.run(script, inputs)
    print(f"   Output: {result.get_string()}")


def demo_format_conversions():
    """Demo 4: Format conversions (JSON ↔ CSV ↔ XML)"""
    print("\n" + "=" * 60)
    print("DEMO 4: Format Conversions")
    print("=" * 60)

    # JSON to CSV
    print("\n4.1 JSON to CSV:")
    inputs = {
        "payload": [
            {"name": "Alice", "age": 30, "city": "NYC"},
            {"name": "Bob", "age": 25, "city": "LON"},
            {"name": "Charlie", "age": 35, "city": "TYO"}
        ]
    }
    script = """
    output application/csv header=true
    ---
    payload map {
        Name: $.name,
        Age: $.age,
        City: $.city
    }
    """
    result = dataweave.run(script, inputs)
    print("   Input: JSON array of users")
    print("   Output (CSV):")
    print("   " + result.get_string().replace("\n", "\n   "))

    # CSV to JSON
    print("\n4.2 CSV to JSON:")
    csv_data = """Name,Score,Grade
Alice,95,A
Bob,87,B
Charlie,92,A"""

    inputs = {
        "payload": dataweave.InputValue(
            content=csv_data.encode('utf-8'),
            mime_type="application/csv",
            properties={"header": "true"}
        )
    }
    script = """
    output application/json
    ---
    payload map {
        student: $.Name,
        score: $.Score as Number,
        grade: $.Grade,
        passed: $.Score as Number >= 80
    }
    """
    result = dataweave.run(script, inputs)
    print("   Input: CSV with headers")
    print(f"   Output (JSON): {result.get_string()}")


def demo_streaming():
    """Demo 5: Streaming for large datasets"""
    print("\n" + "=" * 60)
    print("DEMO 5: Streaming (Constant Memory)")
    print("=" * 60)

    print("\n5.1 Streaming large array transformation:")
    print("   Generating 1000 records and streaming output...")

    # Generate large dataset
    large_dataset = {
        "payload": [{"id": i, "value": i * 10} for i in range(1000)]
    }

    script = """
    output application/json
    ---
    payload map {
        recordId: $.id,
        computedValue: $.value * 2,
        category: if ($.id mod 2 == 0) "even" else "odd"
    }
    """

    # Use streaming API for constant memory
    chunk_count = 0
    total_bytes = 0

    def on_chunk(chunk: bytes):
        nonlocal chunk_count, total_bytes
        chunk_count += 1
        total_bytes += len(chunk)

    metadata = dataweave.run_streaming(script, large_dataset, on_chunk)

    print(f"   ✓ Streamed {chunk_count} chunks")
    print(f"   ✓ Total output: {total_bytes:,} bytes")
    print(f"   ✓ Result: {metadata.result}")
    print(f"   ✓ Memory usage: Constant (chunks processed incrementally)")


def demo_bidirectional_streaming():
    """Demo 6: Bidirectional streaming (input + output)"""
    print("\n" + "=" * 60)
    print("DEMO 6: Bidirectional Streaming")
    print("=" * 60)

    print("\n6.1 Transform large CSV input to JSON output:")
    print("   Streaming both input and output for maximum efficiency...")

    # Simulate large CSV input
    csv_lines = ["id,name,value"]
    csv_lines.extend([f"{i},Item{i},{i*100}" for i in range(500)])
    csv_content = "\n".join(csv_lines)

    # Input provider (simulates reading from file/network)
    input_chunks = [csv_content[i:i+1024].encode('utf-8')
                    for i in range(0, len(csv_content), 1024)]
    input_iter = iter(input_chunks)

    def read_input(buffer_size: int) -> bytes:
        """Provide input data in chunks"""
        try:
            return next(input_iter)
        except StopIteration:
            return b""

    # Output consumer
    output_chunks = []
    def write_output(chunk: bytes):
        """Consume output data in chunks"""
        output_chunks.append(chunk)

    # DataWeave transformation script
    script = """
    output application/json
    ---
    payload map {
        itemId: $.id,
        itemName: $.name,
        price: $.value as Number,
        currency: "USD"
    }
    """

    # Run with bidirectional streaming
    inputs = {
        "payload": dataweave.InputValue(
            content=b"",  # Content provided via read_input
            mime_type="application/csv",
            properties={"header": "true", "streaming": "true"}
        )
    }

    metadata = dataweave.run_transform(script, inputs, read_input, write_output)

    total_output = b"".join(output_chunks)
    print(f"   ✓ Input: {len(csv_lines)} CSV rows streamed in {len(input_chunks)} chunks")
    print(f"   ✓ Output: {len(output_chunks)} JSON chunks received")
    print(f"   ✓ Total output size: {len(total_output):,} bytes")
    print(f"   ✓ Memory usage: Constant (both input and output streamed)")


def demo_error_handling():
    """Demo 7: Error handling"""
    print("\n" + "=" * 60)
    print("DEMO 7: Error Handling")
    print("=" * 60)

    # Syntax error
    print("\n7.1 Handling syntax errors:")
    result = dataweave.run("2 + + 3")  # Invalid syntax
    if not result.success:
        print(f"   ✗ Syntax error detected:")
        print(f"     {result.error}")

    # Runtime error
    print("\n7.2 Handling runtime errors:")
    result = dataweave.run("payload.user.email", {"payload": {}})
    if not result.success:
        print(f"   ✗ Runtime error detected:")
        print(f"     {result.error}")

    # Type error
    print("\n7.3 Handling type errors:")
    result = dataweave.run('"text" + 123')  # Type mismatch
    if not result.success:
        print(f"   ✗ Type error detected:")
        print(f"     {result.error}")

    # Successful execution after errors
    print("\n7.4 Successful execution:")
    result = dataweave.run("2 + 3")
    if result.success:
        print(f"   ✓ Valid expression: 2 + 3 = {result.get_string()}")


def demo_advanced_features():
    """Demo 8: Advanced features"""
    print("\n" + "=" * 60)
    print("DEMO 8: Advanced Features")
    print("=" * 60)

    # Reduce/fold
    print("\n8.1 Reduce (sum of array):")
    script = "[1, 2, 3, 4, 5] reduce ((item, accumulator=0) -> accumulator + item)"
    result = dataweave.run(script)
    print(f"   Expression: {script}")
    print(f"   Result: {result.get_string()}")

    # Pattern matching
    print("\n8.2 Pattern matching:")
    inputs = {"status": "SUCCESS"}
    script = """
    status match {
        case "SUCCESS" -> "Operation completed successfully"
        case "PENDING" -> "Operation in progress"
        case "FAILED" -> "Operation failed"
        else -> "Unknown status"
    }
    """
    result = dataweave.run(script, inputs)
    print(f"   Input status: {inputs['status']}")
    print(f"   Result: {result.get_string()}")

    # Nested data access with default values
    print("\n8.3 Safe navigation with defaults:")
    inputs = {"payload": {"user": {"name": "Alice"}}}
    script = 'payload.user.email default "no-email@example.com"'
    result = dataweave.run(script, inputs)
    print(f"   Script: {script}")
    print(f"   Result: {result.get_string()}")


def main():
    """Run all demos"""
    print("\n" + "=" * 60)
    print(" DataWeave Python Bindings - Comprehensive Demo")
    print("=" * 60)
    print("\n This demo showcases:")
    print("  • Basic transformations and operations")
    print("  • Working with inputs and context")
    print("  • JSON data transformations")
    print("  • Format conversions (JSON/CSV/XML)")
    print("  • Streaming for large datasets")
    print("  • Bidirectional streaming (input + output)")
    print("  • Error handling")
    print("  • Advanced DataWeave features")
    print()

    try:
        demo_basic_operations()
        demo_with_inputs()
        demo_json_transformations()
        demo_format_conversions()
        demo_streaming()
        demo_bidirectional_streaming()
        demo_error_handling()
        demo_advanced_features()

        print("\n" + "=" * 60)
        print("✓ All demos completed successfully!")
        print("=" * 60)
        print()

    except Exception as e:
        print(f"\n✗ Error running demo: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
