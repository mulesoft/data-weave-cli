package org.mule.weave.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Contract test for {@link NativeLib#cleanupFeeder} + {@link NativeLib.InputCallbackFeeder}
 * (review #10 #1, Critical): the transform input feeder must be cancelled and fully joined
 * before {@code cleanupFeeder} returns, so a slow read callback that is still in-flight cannot
 * be re-invoked after the native caller frees the callback state ({@code ctx}).
 *
 * <p>The real feeder pulls input via a GraalVM {@code Word}-typed function pointer
 * ({@code cb.invoke}), which cannot be exercised from a hosted JVM test. We instead subclass
 * {@link NativeLib.InputCallbackFeeder} and override {@link NativeLib.InputCallbackFeeder#readChunk}
 * with a pure-Java stand-in that models a read callback which blocks (an in-flight
 * {@code cb.invoke}) while cleanup runs.</p>
 */
class NativeLibFeederTest {

    /**
     * A read callback that always returns data (never EOF) and blocks ~500 ms per call,
     * modelling a slow-but-returning in-flight {@code cb.invoke}.
     *
     * <p>Post-return invariant under test: after {@code cleanupFeeder} returns, the feeder thread
     * is no longer alive (so it can never touch freed callback state), and the "callback" was not
     * re-invoked after cancellation was requested.</p>
     *
     * <p>Against the pre-fix code ({@code join(5000)} then abandon, no cancel signal, and the input
     * session closed only <em>after</em> the join) the feeder loops forever writing chunks: the
     * join times out with the thread still alive, {@code isAlive()} is {@code true}, and the
     * invocation count is large — the test fails, demonstrating the use-after-free window.</p>
     */
    @Test
    void cleanupFeederCancelsAndJoinsInFlightReadCallbackBeforeReturning() throws Exception {
        InputStreamSession inputSession = new InputStreamSession("application/json", "UTF-8");
        long inputHandle = inputSession.register();

        AtomicInteger invocations = new AtomicInteger(0);
        CountDownLatch entered = new CountDownLatch(1);

        // Raw addresses are unused: readChunk is overridden and never reconstitutes them.
        NativeLib.InputCallbackFeeder feeder =
                new NativeLib.InputCallbackFeeder(0L, 0L, inputSession) {
                    @Override
                    int readChunk(byte[] dest, int max) {
                        invocations.incrementAndGet();
                        entered.countDown();
                        try {
                            // Simulate a slow in-flight cb.invoke that returns *after* cleanup
                            // has requested cancellation.
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        // Always return data (never EOF): pre-fix code would loop forever.
                        dest[0] = 'x';
                        return 1;
                    }
                };

        Thread thread = new Thread(feeder, "test-input-callback-feeder");
        thread.setDaemon(true);
        thread.start();

        // Wait until the feeder is inside the (blocking) callback, then clean up while it blocks.
        assertTrue(entered.await(2, TimeUnit.SECONDS), "feeder never entered the read callback");

        long start = System.nanoTime();
        NativeLib.cleanupFeeder(feeder, thread, inputHandle);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        // Post-return invariant: the feeder has fully exited run() — no UAF window remains.
        assertFalse(thread.isAlive(),
                "cleanupFeeder returned while the feeder thread was still alive (use-after-free window)");
        assertTrue(feeder.isCancelled(), "cleanupFeeder must have signalled cancellation");

        // The in-flight callback was allowed to return, but it was NOT re-invoked after cancel:
        // exactly one invocation proves the loop checks the cancel flag after cb.invoke returns
        // and before re-invoking it.
        assertEquals(1, invocations.get(),
                "read callback was re-invoked after cancellation (should break the loop instead)");

        // Sanity: the join waited only for the in-flight callback (~500 ms), not a finite abandon
        // timeout, and certainly did not hang.
        assertTrue(elapsedMs < 4000,
                "cleanupFeeder took unexpectedly long (" + elapsedMs + " ms)");

        System.out.printf("cleanupFeeder returned after %d ms; invocations=%d, alive=%b%n",
                elapsedMs, invocations.get(), thread.isAlive());
    }

    /**
     * Leak + exception-escape guard for the {@code transformViaCallbacks} setup region
     * (review #11 #1, High): a malformed {@code inputs} JSON string must not leak the registered
     * {@link InputStreamSession} handle and must not let the {@link org.json.JSONException} escape
     * the {@code @CEntryPoint}. It must instead resolve to a {@code success:false} envelope with the
     * handle already closed.
     *
     * <p>Driven through the package-private {@link NativeLib#setUpInputSession} seam because
     * {@code transformViaCallbacks} itself takes GraalVM {@code Word}-typed callbacks and returns a
     * {@code CCharPointer}, neither of which resolves in a hosted JVM.</p>
     */
    @Test
    void setUpInputSessionMalformedInputsReturnsErrorEnvelopeAndClosesHandle() {
        NativeLib.InputSetup setup =
                NativeLib.setUpInputSession("{not json", "payload", "application/json", "UTF-8");

        assertNotNull(setup.errorEnvelope, "malformed inputs must yield an error envelope");
        assertTrue(setup.errorEnvelope.contains("\"success\":false"),
                "envelope must be success:false, was: " + setup.errorEnvelope);
        assertNull(setup.mergedInputs, "no merged inputs on the error path");
        assertNull(InputStreamSession.get(setup.handle),
                "input session handle leaked after malformed inputs");
    }

    /**
     * Happy-path guard: valid {@code inputs} register the session, merge the stream-handle entry
     * structurally, and leave the handle live for the feeder (no behavior change). The caller
     * (via {@code cleanupFeeder}) is responsible for the eventual close.
     */
    @Test
    void setUpInputSessionValidInputsMergesEntryAndKeepsHandleLive() {
        NativeLib.InputSetup setup = NativeLib.setUpInputSession(
                "{\"other\":{\"x\":1}}", "payload", "application/json", "UTF-8");

        assertNull(setup.errorEnvelope, "valid inputs must not produce an error envelope");
        assertNotNull(setup.mergedInputs, "valid inputs must produce merged inputs");
        assertTrue(setup.mergedInputs.contains("streamHandle"),
                "merged inputs must carry the stream handle entry, was: " + setup.mergedInputs);
        assertTrue(setup.mergedInputs.contains("payload"),
                "merged inputs must carry the input binding name, was: " + setup.mergedInputs);
        assertNotNull(InputStreamSession.get(setup.handle),
                "session must remain live for the feeder on the success path");

        // Clean up the still-live session so the test leaves no handle behind.
        InputStreamSession.close(setup.handle);
        assertNull(InputStreamSession.get(setup.handle));
    }

    // ── Bounds-check on the read-callback length (review #11 #4, Medium) ─────

    /**
     * Drives the feeder to completion with the given {@code readChunk} stand-in and returns the
     * throwable (if any) that escaped {@link NativeLib.InputCallbackFeeder#run()} via the thread's
     * uncaught-exception handler. Uses a fresh registered session so the feeder's {@code finally}
     * has a real writer to close, and unregisters it afterwards so no handle leaks.
     */
    private static Throwable runFeederCapturingEscapedError(ReadChunkStub stub) throws Exception {
        InputStreamSession inputSession = new InputStreamSession("application/json", "UTF-8");
        long inputHandle = inputSession.register();
        try {
            NativeLib.InputCallbackFeeder feeder =
                    new NativeLib.InputCallbackFeeder(0L, 0L, inputSession) {
                        @Override
                        int readChunk(byte[] dest, int max) {
                            return stub.readChunk(dest, max);
                        }
                    };
            AtomicReference<Throwable> escaped = new AtomicReference<>();
            Thread thread = new Thread(feeder, "test-bounds-feeder");
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((t, e) -> escaped.set(e));
            thread.start();
            thread.join(TimeUnit.SECONDS.toMillis(5));
            assertFalse(thread.isAlive(), "feeder thread did not stop after an out-of-range length");
            stub.setFeeder(feeder);
            return escaped.get();
        } finally {
            InputStreamSession.close(inputHandle);
        }
    }

    /** Test seam mirroring {@code readChunk} plus a hook to reach the feeder after it stops. */
    private interface ReadChunkStub {
        int readChunk(byte[] dest, int max);

        default void setFeeder(NativeLib.InputCallbackFeeder feeder) {
        }
    }

    /**
     * A read callback that returns {@code max + 1} (one past the buffer) must be rejected: the
     * feeder stops as an error with {@link NativeLib.InputCallbackFeeder#getError()} naming the
     * out-of-range count, and no out-of-bounds exception escapes {@code run()}.
     */
    @Test
    void readCallbackLengthAboveMaxIsRejectedAsError() throws Exception {
        AtomicReference<NativeLib.InputCallbackFeeder> ref = new AtomicReference<>();
        AtomicInteger calls = new AtomicInteger(0);
        Throwable escaped = runFeederCapturingEscapedError(new ReadChunkStub() {
            @Override
            public int readChunk(byte[] dest, int max) {
                calls.incrementAndGet();
                return max + 1; // one byte past the destination buffer
            }

            @Override
            public void setFeeder(NativeLib.InputCallbackFeeder feeder) {
                ref.set(feeder);
            }
        });

        assertNull(escaped, "an out-of-bounds exception escaped run(): " + escaped);
        assertEquals(1, calls.get(), "feeder must stop after the first out-of-range read");
        String error = ref.get().getError();
        assertNotNull(error, "out-of-range length must be recorded as a feeder error");
        assertTrue(error.contains(Integer.toString(NativeLibFeederConstants.BUFFER + 1)),
                "error must name the out-of-range count, was: " + error);
    }

    /**
     * A read callback that returns {@code -5} (outside the {@code [-1, max]} contract) must be
     * rejected the same way: recorded feeder error naming the count, no exception out of {@code run()}.
     */
    @Test
    void readCallbackNegativeOutOfRangeLengthIsRejectedAsError() throws Exception {
        AtomicReference<NativeLib.InputCallbackFeeder> ref = new AtomicReference<>();
        Throwable escaped = runFeederCapturingEscapedError(new ReadChunkStub() {
            @Override
            public int readChunk(byte[] dest, int max) {
                return -5;
            }

            @Override
            public void setFeeder(NativeLib.InputCallbackFeeder feeder) {
                ref.set(feeder);
            }
        });

        assertNull(escaped, "an exception escaped run(): " + escaped);
        String error = ref.get().getError();
        assertNotNull(error, "out-of-range negative length must be recorded as a feeder error");
        assertTrue(error.contains("-5"), "error must name the out-of-range count, was: " + error);
    }

    /**
     * A clean EOF ({@code 0}) is not an error: the feeder stops with {@code getError() == null}.
     */
    @Test
    void readCallbackCleanEofLeavesNoFeederError() throws Exception {
        AtomicReference<NativeLib.InputCallbackFeeder> ref = new AtomicReference<>();
        Throwable escaped = runFeederCapturingEscapedError(new ReadChunkStub() {
            @Override
            public int readChunk(byte[] dest, int max) {
                return 0; // immediate EOF
            }

            @Override
            public void setFeeder(NativeLib.InputCallbackFeeder feeder) {
                ref.set(feeder);
            }
        });

        assertNull(escaped, "no exception may escape run() on clean EOF: " + escaped);
        assertNull(ref.get().getError(), "clean EOF must leave getError() == null");
    }

    /** Mirrors the package-private {@code CALLBACK_BUFFER_SIZE} used as the read {@code max}. */
    private static final class NativeLibFeederConstants {
        static final int BUFFER = 8 * 1024;
    }
}
