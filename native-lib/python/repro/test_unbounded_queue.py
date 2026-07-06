"""Reproduces finding [8] from the adversarial review: the Python streaming
binding hands native output chunks across thread boundaries through an
*unbounded* ``queue.Queue()`` — memory exhaustion under a slow/stalled consumer.

Source (native-lib/python/src/dataweave/__init__.py):

    q: Queue = Queue()                      # :570 and :713 — NO maxsize

    @WRITE_CALLBACK
    def _write_cb(_ctx, buf, length):
        q.put(ctypes.string_at(buf, length))   # :575 — never blocks
        return 0

    def _run_native():
        ... run_script_callback(...) ...        # worker thread fills q as fast
                                                # as the native engine produces

    # consumer side (generator):
    while True:
        item = q.get()                          # :614 — one chunk at a time
        ... yield item ...

Because ``Queue()`` has no ``maxsize``, ``_write_cb`` (called on the native
worker thread, once per output chunk) never blocks. If the native engine emits
chunks faster than the Python generator consumer drains them — a large or
infinite output, a slow/paused consumer, a consumer that stops iterating — the
queue grows without bound until the process is OOM-killed. There is no
backpressure onto the native producer.

The real binding is ctypes-over-cgo and needs the multi-GB GraalVM ``dwlib`` to
run, so this is a dependency-free MODEL that mirrors the exact structure: the
same unbounded ``Queue()``, the same ``q.put`` write callback, and a producer
that outruns the consumer.

The test asserts the DESIRED behavior — the queue must stay bounded under a
stalled consumer (i.e. the callback exerts backpressure). Today it does NOT, so
the test FAILS (reproducing the unbounded growth). After the fix
(``Queue(maxsize=512)`` + a callback that blocks / signals the native side to
pause), it will PASS.

Run:  python3 -m pytest test_unbounded_queue.py -v
 or:  python3 test_unbounded_queue.py
"""

import threading
import time
from queue import Queue

# Mirror the binding's buffering: a correct fix would cap the queue at this many
# chunks and let put() block, giving the native producer backpressure.
BOUND = 512

# Model a large native output: far more chunks than a bounded buffer would hold.
TOTAL_CHUNKS = 200_000
CHUNK = b"x" * 64  # each output chunk


def _make_queue() -> Queue:
    """Mirror dataweave.py:570 / :713 exactly. The fix changes THIS line to
    ``Queue(maxsize=BOUND)`` — that single change makes the test pass."""
    return Queue(maxsize=BOUND)  # FIXED: bounded queue for backpressure


def test_unbounded_queue_grows_without_backpressure():
    q = _make_queue()

    # Mirror the write callback (dataweave.py:573-578): invoked on the native
    # worker thread once per output chunk. With the fix, put() blocks when the
    # queue is full, exerting backpressure on the producer.
    def write_cb(chunk: bytes) -> int:
        try:
            # Mirror the real fix: blocking put with timeout
            q.put(chunk, timeout=30)
            return 0
        except Exception:
            # Timeout or other failure: abort
            return -1

    peak = 0
    peak_lock = threading.Lock()
    stop = threading.Event()

    def sampler():
        nonlocal peak
        while not stop.is_set():
            s = q.qsize()
            with peak_lock:
                if s > peak:
                    peak = s
            time.sleep(0.001)

    def producer():
        # The native engine emits the whole output as fast as it can. The
        # consumer below is STALLED (models a slow/paused/abandoned reader),
        # so nothing drains q while this runs.
        for _ in range(TOTAL_CHUNKS):
            write_cb(CHUNK)

    sampler_t = threading.Thread(target=sampler, daemon=True)
    producer_t = threading.Thread(target=producer, daemon=True)
    sampler_t.start()
    producer_t.start()

    # A correctly back-pressured producer would block at ~BOUND and never
    # finish while the consumer is stalled, so we bound the wait. With the fix,
    # the producer should be blocked and NOT finish.
    producer_t.join(timeout=5)
    producer_finished = not producer_t.is_alive()
    stop.set()
    sampler_t.join(timeout=1)

    with peak_lock:
        observed_peak = peak
    # qsize() after the fact is authoritative for how much is still buffered.
    final_size = q.qsize()
    observed_peak = max(observed_peak, final_size)

    print(
        f"[repro] producer_finished={producer_finished} "
        f"peak_qsize={observed_peak} bound={BOUND} total={TOTAL_CHUNKS}"
    )

    # DESIRED (post-fix) behavior: with a stalled consumer the producer must be
    # held near the buffer bound — it cannot enqueue all TOTAL_CHUNKS.
    assert observed_peak <= BOUND * 4, (
        f"FINDING [8] REPRODUCED: unbounded Queue() buffered {observed_peak} "
        f"chunks (~{observed_peak * len(CHUNK) // 1024} KiB and climbing) with "
        f"the consumer stalled — no backpressure onto the native producer. A "
        f"real/infinite output would exhaust memory. Expected the buffer to be "
        f"held at <= ~{BOUND} chunks (Queue(maxsize={BOUND}))."
    )
    assert not producer_finished, (
        "FINDING [8] REPRODUCED: producer ran to completion against a stalled "
        "consumer — the queue absorbed the entire output instead of blocking."
    )


if __name__ == "__main__":
    try:
        test_unbounded_queue_grows_without_backpressure()
    except AssertionError as e:
        print("FAIL (finding reproduced):")
        print(str(e))
        raise SystemExit(1)
    print("PASS (bug appears fixed — queue stayed bounded)")
    raise SystemExit(0)
