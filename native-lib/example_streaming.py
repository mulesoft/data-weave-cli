#!/usr/bin/env python3

import sys
from pathlib import Path

_PYTHON_SRC_DIR = Path(__file__).resolve().parent / "python" / "src"
sys.path.insert(0, str(_PYTHON_SRC_DIR))

import dataweave
import resource
import psutil, os
import threading
import json
import time

def example_streaming_input_output():
    print("\nTesting streaming input and output (square numbers)...")
    try:
        start_time = time.monotonic()
        num_elements = 1_000_000 * 10
        chunk_size = 1024 * 64

        input_stream = dataweave.open_input_stream("application/json", "utf-8")

        script = """output application/json deferred=true
---
payload map ($ * $)"""

        def feed_input():
            try:
                input_stream.write(b"[")
                for i in range(num_elements):
                    if i > 0:
                        input_stream.write(b",")
                    input_stream.write(str(i).encode("utf-8"))
                input_stream.write(b"]")
            finally:
                input_stream.close()

        feeder = threading.Thread(target=feed_input, daemon=True)
        feeder.start()

        with dataweave.run_stream(script, inputs={"payload": input_stream}) as stream:
            usage = resource.getrusage(resource.RUSAGE_SELF)
            current_rss = psutil.Process(os.getpid()).memory_info().rss
            print(f">>> Output mimeType={stream.mimeType}, charset={stream.charset}, binary={stream.binary}, Max RSS: {usage.ru_maxrss / 1048576:.1f} MB, Current RSS: {current_rss / 1048576:.1f} MB ---")
            chunk_count = 0
            total_bytes = 0
            while True:
                chunk = stream.read(chunk_size)
                if not chunk:
                    break
                chunk_count += 1
                total_bytes += len(chunk)
                if chunk_count % 5000 == 0:
                    usage = resource.getrusage(resource.RUSAGE_SELF)
                    current_rss = psutil.Process(os.getpid()).memory_info().rss
                    print(f"--- chunk {chunk_count}: {len(chunk)} bytes, total: {total_bytes / 1048576:.1f} MB, Max RSS: {usage.ru_maxrss / 1048576:.1f} MB, Current RSS: {current_rss / 1048576:.1f} MB ---")

        feeder.join()

        peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1048576
        elapsed = time.monotonic() - start_time
        mins, secs = divmod(elapsed, 60)
        print(f"\n[OK] Streaming input/output done ({chunk_count} chunks, {total_bytes/ 1048576:.1f} MB, {num_elements:,} elements) - Time: {int(mins)}:{secs:06.3f}")
        print(f"Peak memory (max RSS): {peak_rss:.1f} MB")
        return True
    except Exception as e:
        print(f"[FAIL] Streaming input/output failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def example_streaming_output_larger_than_memory():
    print("\nTesting streaming output larger than memory...")
    try:
        script = """output application/json deferred=true
---
{items: (1 to pow(1000, 2)*10) map {id: $, name: "item_" ++ $}}"""
        with dataweave.run_stream(script) as stream:
            print(f">>> Output mimeType={stream.mimeType}, charset={stream.charset}, binary={stream.binary}")
            chunk_count = 0
            total_bytes = 0
            while True:
                chunk = stream.read(1024*1024*10) # deferred=true uses 8k chunks
                if not chunk:
                    break
                chunk_count += 1
                total_bytes += len(chunk)
                usage = resource.getrusage(resource.RUSAGE_SELF)
                current_rss = psutil.Process(os.getpid()).memory_info().rss
                # print script output
#                 sys.stdout.write(chunk.decode(stream.charset or "utf-8"))
#                 sys.stdout.flush()
                print(f"--- chunk {chunk_count}: {len(chunk)} bytes, total: {total_bytes / 1048576:.1f} MB, Max RSS: {usage.ru_maxrss / 1048576:.1f} MB, Current RSS: {current_rss / 1048576:.1f} MB ---")
        peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1048576
        print(f"\n[OK] Streaming done ({chunk_count} chunks, {total_bytes} bytes)")
        print(f"Peak memory (max RSS): {peak_rss:.1f} MB")
        return True
    except Exception as e:
        print(f"[FAIL] Streaming chunked read failed: {e}")
        return False


def example_streaming_adding_chunks():
    print("\nTesting streaming adding chunks...")
    try:
        script = """output application/json deferred=true
---
{items: (1 to pow(1000, 2)*10) map {id: $, name: "item_" ++ $}}"""
        with dataweave.run_stream(script) as stream:
            print(f">>> Output mimeType={stream.mimeType}, charset={stream.charset}, binary={stream.binary}")
            chunks = []
            total_bytes = 0
            while True:
                chunk = stream.read(1024*1024*16)
                if not chunk:
                    break
                total_bytes += len(chunk)
                chunks.append(chunk)
                usage = resource.getrusage(resource.RUSAGE_SELF)
                current_rss = psutil.Process(os.getpid()).memory_info().rss
                if len(chunks) % 1000 == 0:
                    print(f"--- chunk {len(chunks)}: {len(chunk)} bytes, total: {total_bytes / 1048576:.1f} MB, Max RSS: {usage.ru_maxrss / 1048576:.1f} MB, Current RSS: {current_rss / 1048576:.1f} MB ---")
            full = b"".join(chunks)
            assert len(chunks) > 1, f"Expected multiple chunks, got {len(chunks)}"
            assert b"item_1" in full, "Expected 'item_1' in result"
            assert b"item_100" in full, "Expected 'item_100' in result"
        print(f"\n[OK] Streaming chunked read works ({len(chunks)} chunks)")
        #print(f"\n[OK] Streaming done ({chunk_count} chunks, {total_bytes} bytes)")
        return True
    except Exception as e:
        print(f"[FAIL] Streaming chunked read failed: {e}")
        return False


def main():
    example_streaming_input_output()
#     example_streaming_output_larger_than_memory()
#     example_streaming_adding_chunks()


if __name__ == "__main__":
    main()
