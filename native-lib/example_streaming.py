#!/usr/bin/env python3

import sys
from pathlib import Path

_PYTHON_SRC_DIR = Path(__file__).resolve().parent / "python" / "src"
sys.path.insert(0, str(_PYTHON_SRC_DIR))

import dataweave
import resource
import psutil, os

def example_streaming_larger_than_memory():
    print("\nTesting streaming larger than memory...")
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
    example_streaming_larger_than_memory()
#     example_streaming_adding_chunks()


if __name__ == "__main__":
    main()
