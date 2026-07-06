# Python binding — finding [8] regression guard

Regression guard for finding **[8]** from
`docs/reviews/2026-07-06-adversarial-native-bindings-review.md`: the streaming
binding hands native output chunks across the worker/consumer thread boundary
through a queue. The fix bounds the queue (`Queue(maxsize=512)`) and uses
`q.put(chunk, timeout=30)` in the write callback so that a slow or stalled
consumer exerts backpressure on the native producer, preventing unbounded
memory growth.

## Why it's a standalone model

The real binding is ctypes-over-cgo and needs the multi-GB GraalVM `dwlib` to
run. `test_unbounded_queue.py` is a dependency-free model that mirrors the exact
structure: a bounded `Queue(maxsize=BOUND)`, the `q.put(timeout=...)` write
callback, and a producer thread that outruns a stalled consumer. It asserts the
*desired* behavior — the queue stays bounded under a stalled consumer — and with
the fix applied, it passes.

Faithfulness check (run from `native-lib/python`):

```sh
rg -n 'Queue\(|q\.put|maxsize' src/dataweave/__init__.py
```

## Run

```sh
python3 test_unbounded_queue.py          # exit 0 == fixed
# or, if pytest is available:
python3 -m pytest test_unbounded_queue.py -v
```

With the fix applied, it **PASSES**: the producer is held at ~512 chunks and
does NOT run to completion against a stalled consumer. If the queue is reverted
to unbounded (`Queue()` with no maxsize), the test will FAIL (peak qsize ==
200 000, far above the 512 bound).
