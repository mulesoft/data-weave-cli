package org.mule.weave.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
}
