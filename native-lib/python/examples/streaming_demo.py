#!/usr/bin/env python3
"""
DataWeave Python Bindings - Streaming Demo

Demonstrates streaming capabilities: output streaming and bidirectional streaming.
"""

import sys
import os
import io

# Add parent directory to path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import dataweave

def main():
    print("=== DataWeave Python Streaming Demo ===\n")

    # Example 1: Output streaming
    print("1. Output streaming (small dataset):")
    stream = dataweave.run_streaming("output application/json --- (1 to 10) map {id: $}")
    chunks = []
    for chunk in stream:
        chunks.append(chunk)
        print(f"   Received chunk: {len(chunk)} bytes")

    output = b"".join(chunks).decode('utf-8')
    print(f"   Complete output: {output[:60]}...")
    print(f"   Metadata: mime_type={stream.metadata.mime_type}, success={stream.metadata.success}\n")

    # Example 2: Output streaming with larger dataset
    print("2. Output streaming (large dataset - 1000 items):")
    stream = dataweave.run_streaming("output application/json --- (1 to 1000)")

    total_bytes = 0
    chunk_count = 0
    for chunk in stream:
        total_bytes += len(chunk)
        chunk_count += 1

    print(f"   Total bytes: {total_bytes}")
    print(f"   Chunk count: {chunk_count}")
    print(f"   Success: {stream.metadata.success}\n")

    # Example 3: Bidirectional streaming (from bytes)
    print("3. Bidirectional streaming (in-memory):")
    json_input = b'[1, 2, 3, 4, 5]'
    stream = dataweave.run_transform(
        "output application/json --- payload map ($ * $)",
        input_stream=[json_input],
        input_mime_type="application/json"
    )

    output = b"".join(stream)
    print(f"   Input: {json_input.decode('utf-8')}")
    print(f"   Output: {output.decode('utf-8')}")
    print(f"   Metadata: {stream.metadata}\n")

    # Example 4: Bidirectional streaming (from generator)
    print("4. Bidirectional streaming (generator):")

    def generate_json_chunks():
        """Generator that produces JSON input in chunks"""
        yield b'[{"id":1,"name":"Alice"},'
        yield b'{"id":2,"name":"Bob"},'
        yield b'{"id":3,"name":"Charlie"}]'

    stream = dataweave.run_transform(
        "output application/json --- payload map { name: $.name }",
        input_stream=generate_json_chunks(),
        input_mime_type="application/json"
    )

    output_chunks = []
    for chunk in stream:
        output_chunks.append(chunk)
        print(f"   Output chunk: {len(chunk)} bytes")

    final_output = b"".join(output_chunks).decode('utf-8')
    print(f"   Final result: {final_output}\n")

    # Example 5: Streaming from file-like object
    print("5. Bidirectional streaming (file-like object):")

    # Create an in-memory file-like object
    csv_data = b"id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35\n"
    input_file = io.BytesIO(csv_data)

    stream = dataweave.run_transform(
        'output application/json --- payload map { name: $.name, age: $.age }',
        input_stream=iter(lambda: input_file.read(20), b""),  # Read 20 bytes at a time
        input_mime_type="application/csv",
        input_properties={"header": True}
    )

    output = b"".join(stream).decode('utf-8')
    print(f"   CSV input: {len(csv_data)} bytes")
    print(f"   JSON output: {output}")
    print(f"   Success: {stream.metadata.success}\n")

    # Example 6: Low-level callback API
    print("6. Low-level callback API:")

    json_input_data = b'[10, 20, 30, 40, 50]'
    pos = 0

    def read_callback(buf_size):
        nonlocal pos
        chunk = json_input_data[pos:pos + buf_size]
        pos += len(chunk)
        return chunk  # return b"" when done

    output_chunks_cb = []
    def write_callback(data):
        output_chunks_cb.append(data)
        return 0  # 0 = success

    result = dataweave.run_input_output_callback(
        "output application/json deferred=true --- payload map ($ * 2)",
        input_name="payload",
        input_mime_type="application/json",
        read_callback=read_callback,
        write_callback=write_callback
    )

    print(f"   Callback result: success={result.success}")
    output_cb = b"".join(output_chunks_cb).decode('utf-8')
    print(f"   Output: {output_cb}\n")

    # Example 7: Using with context manager
    print("7. Streaming with context manager:")
    with dataweave.DataWeave() as dw:
        stream = dw.run_streaming("output application/csv --- (1 to 5)")
        csv_output = b"".join(stream).decode('utf-8')
        print(f"   CSV output: {csv_output}")
    print()

    # Example 8: Error handling in streaming
    print("8. Error handling in streaming:")
    stream = dataweave.run_streaming("output application/json --- invalid syntax here")

    # Drain chunks (there may be none)
    for chunk in stream:
        print(f"   Unexpected chunk: {chunk}")

    # Check metadata for error
    if not stream.metadata.success:
        print(f"   Error caught in metadata: {stream.metadata.error[:50]}...")
    print()

    print("Streaming demo complete!")

if __name__ == "__main__":
    try:
        main()
    except dataweave.DataWeaveLibraryNotFoundError:
        print("\nERROR: Native library not found!")
        print("Build it first: ./gradlew :native-lib:nativeCompile")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
