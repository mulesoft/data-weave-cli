#!/usr/bin/env python3

import sys
from pathlib import Path

_PYTHON_SRC_DIR = Path(__file__).resolve().parent / "python" / "src"
sys.path.insert(0, str(_PYTHON_SRC_DIR))

import dataweave
import resource
import psutil, os
import time

def example_streaming_input_output_callback():
    print("\nTesting streaming input and output using callbacks (square numbers)...")
    try:
        start_time = time.monotonic()
        num_elements = 1_000_000 * 50

        script = """output application/json deferred=true
---
payload map ($ * $)"""

        # -- input generator (called by native on a background thread) --
        input_iter = iter(range(num_elements))
        input_started = False
        input_done = False
        pending_token = None

        def read_callback(buf_size):
            nonlocal input_started, input_done, pending_token
            if input_done:
                return b""
            parts = []
            if not input_started:
                parts.append(b"[")
                input_started = True
            remaining = buf_size - sum(len(p) for p in parts)
            if pending_token is not None:
                if len(pending_token) <= remaining:
                    parts.append(pending_token)
                    remaining -= len(pending_token)
                    pending_token = None
                else:
                    return b"".join(parts)
            try:
                while remaining > 0:
                    i = next(input_iter)
                    token = (b"," if i > 0 else b"") + str(i).encode("utf-8")
                    if len(token) > remaining:
                        pending_token = token
                        break
                    parts.append(token)
                    remaining -= len(token)
            except StopIteration:
                parts.append(b"]")
                input_done = True
            return b"".join(parts)

        # -- output collector (called by native on the calling thread) --
        chunk_count = 0
        total_bytes = 0

        def write_callback(data):
            nonlocal chunk_count, total_bytes
            chunk_count += 1
            total_bytes += len(data)
            if chunk_count % 5000 == 0:
                usage = resource.getrusage(resource.RUSAGE_SELF)
                current_rss = psutil.Process(os.getpid()).memory_info().rss
                print(f"--- chunk {chunk_count}: {len(data)} bytes, total: {total_bytes / 1048576:.1f} MB, Max RSS: {usage.ru_maxrss / 1048576:.1f} MB, Current RSS: {current_rss / 1048576:.1f} MB ---")
            return 0

        usage = resource.getrusage(resource.RUSAGE_SELF)
        current_rss = psutil.Process(os.getpid()).memory_info().rss
        print(f">>> Before run_input_output_callback, Max RSS: {usage.ru_maxrss / 1048576:.1f} MB, Current RSS: {current_rss / 1048576:.1f} MB ---")

        result = dataweave.run_input_output_callback(
            script,
            input_name="payload",
            input_mime_type="application/json",
            read_callback=read_callback,
            write_callback=write_callback,
            input_charset="utf-8",
        )

        if not result.success:
            raise Exception(result.error or "Unknown error")

        peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1048576
        elapsed = time.monotonic() - start_time
        mins, secs = divmod(elapsed, 60)
        print(f"\n[OK] Streaming input/output callback done ({chunk_count} chunks, {total_bytes / 1048576:.1f} MB, {num_elements:,} elements) - Time: {int(mins)}:{secs:06.3f}")
        print(f"Peak memory (max RSS): {peak_rss:.1f} MB")
        return True
    except Exception as e:
        print(f"[FAIL] Streaming input/output callback failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def example_streaming_run_transform():
    print("\nTesting streaming input and output using run_transform (square numbers)...")
    try:
        start_time = time.monotonic()
        num_elements = 1_000_000 * 50

        script = """output application/json deferred=true
---
payload map ($ * $)"""

        # -- input as a generator of byte chunks --
        def input_chunks():
            buf_size = 8192
            input_iter = iter(range(num_elements))
            started = False
            done = False
            pending_token = None

            while not done:
                parts = []
                if not started:
                    parts.append(b"[")
                    started = True
                remaining = buf_size - sum(len(p) for p in parts)
                if pending_token is not None:
                    if len(pending_token) <= remaining:
                        parts.append(pending_token)
                        remaining -= len(pending_token)
                        pending_token = None
                    else:
                        yield b"".join(parts)
                        continue
                try:
                    while remaining > 0:
                        i = next(input_iter)
                        token = (b"," if i > 0 else b"") + str(i).encode("utf-8")
                        if len(token) > remaining:
                            pending_token = token
                            break
                        parts.append(token)
                        remaining -= len(token)
                except StopIteration:
                    parts.append(b"]")
                    done = True
                if parts:
                    yield b"".join(parts)

        usage = resource.getrusage(resource.RUSAGE_SELF)
        current_rss = psutil.Process(os.getpid()).memory_info().rss
        print(f">>> Before run_transform, Max RSS: {usage.ru_maxrss / 1048576:.1f} MB, Current RSS: {current_rss / 1048576:.1f} MB ---")

        stream = dataweave.run_transform(
            script,
            input_stream=input_chunks(),
            input_mime_type="application/json",
            input_charset="utf-8",
        )

        chunk_count = 0
        total_bytes = 0

        for data in stream:
            chunk_count += 1
            total_bytes += len(data)
            if chunk_count % 5000 == 0:
                usage = resource.getrusage(resource.RUSAGE_SELF)
                current_rss = psutil.Process(os.getpid()).memory_info().rss
                print(f"--- chunk {chunk_count}: {len(data)} bytes, total: {total_bytes / 1048576:.1f} MB, Max RSS: {usage.ru_maxrss / 1048576:.1f} MB, Current RSS: {current_rss / 1048576:.1f} MB ---")

        if not stream.metadata.success:
            raise Exception(stream.metadata.error)

        peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1048576
        elapsed = time.monotonic() - start_time
        mins, secs = divmod(elapsed, 60)
        print(f"\n[OK] run_transform done ({chunk_count} chunks, {total_bytes / 1048576:.1f} MB, {num_elements:,} elements) - Time: {int(mins)}:{secs:06.3f}")
        print(f"Peak memory (max RSS): {peak_rss:.1f} MB")
        return True
    except Exception as e:
        print(f"[FAIL] run_transform failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def doc_example():
    json_input = b'[1,2,3,4,5]'
    pos = 0

    def read_cb(buf_size):
        nonlocal pos
        chunk = json_input[pos:pos + buf_size]
        pos += len(chunk)
        return chunk  # return b"" when done

    chunks = []
    def write_cb(data):
        chunks.append(data)
        return 0  # 0 = success

    result = dataweave.run_input_output_callback(
        "output application/json deferred=true --- payload map ($ * $)",
        input_name="payload",
        input_mime_type="application/json",
        read_callback=read_cb,
        write_callback=write_cb,
    )

    print(result)            # {"success": True}
    print(b"".join(chunks))  # [1,4,9,16,25]


def main():
    print("=" * 70)
    print("run_input_output_callback (low-level callbacks)")
    print("=" * 70)
    example_streaming_input_output_callback()

    print("\n")
    print("=" * 70)
    print("run_transform (Pythonic generator API)")
    print("=" * 70)
    example_streaming_run_transform()

    print("\n")
    doc_example()


if __name__ == "__main__":
    main()
