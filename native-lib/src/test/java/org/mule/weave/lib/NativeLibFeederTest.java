package org.mule.weave.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

    // ── Join-before-getError ordering contract (review #12 #1, High) ─────

    /**
     * Contract test for the ordering {@code transformViaCallbacks} relies on: a terminal feeder
     * error set by an in-flight {@code readChunk} is only guaranteed visible <em>after</em>
     * {@code cleanupFeeder} has joined the feeder thread, not before. Before the round-12 fix,
     * {@code transformViaCallbacks} read {@code getError()} before joining the feeder in its
     * in-try path, so a callback that was still running when output reached EOF and failed only
     * after returning could have its failure missed and a {@code success:true} envelope returned
     * instead. This test proves the invariant the fix depends on: pre-join the error is not yet
     * observable, and {@code cleanupFeeder} does not return until the join completes and the
     * error becomes visible.
     */
    @Test
    void getErrorReflectsLateFailureOnlyAfterJoin() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        InputStreamSession session = new InputStreamSession("application/json", null);
        long handle = session.register();
        // A feeder whose read callback blocks until released, then reports an out-of-range
        // length (the "in-flight callback fails after output EOF" case). Returning the
        // out-of-range value directly (rather than calling the private rejectOutOfRange helper,
        // which isn't visible to this subclass) exercises run()'s own defence-in-depth check,
        // exactly like readCallbackLengthAboveMaxIsRejectedAsError above.
        NativeLib.InputCallbackFeeder feeder = new NativeLib.InputCallbackFeeder(0L, 0L, session) {
            @Override
            int readChunk(byte[] dest, int max) {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return max + 1; // one past the buffer: recorded as a feeder error, loop breaks
            }
        };
        Thread t = new Thread(feeder, "dw-input-callback-feeder-test");
        t.setDaemon(true);
        t.start();

        // Pre-join: the callback is still blocked, so no terminal error is visible yet.
        assertNull(feeder.getError());

        // Releasing + joining (via cleanupFeeder) must wait for run() to finish and make the
        // late failure observable.
        release.countDown();
        NativeLib.cleanupFeeder(feeder, t, handle);

        assertFalse(t.isAlive());
        assertNotNull(feeder.getError());
    }

    /**
     * Regression guard for review #12 #1 round 1 follow-up: {@code getErrorReflectsLateFailureOnlyAfterJoin}
     * above only proves {@code cleanupFeeder}'s own join contract — it does not touch
     * {@code transformViaCallbacks}'s (now {@link NativeLib#selectTransformResult}'s) ordering of
     * "join, then read {@code getError()}". This test drives {@code selectTransformResult}
     * itself: a read callback blocks (models an in-flight {@code cb.invoke}) and is only released
     * from a background thread strictly after the call under test has begun, so the feeder is
     * guaranteed still running — and its error not yet recorded — at the moment
     * {@code selectTransformResult} is invoked.
     *
     * <p>If {@code selectTransformResult} ever read {@code getError()} before joining the feeder
     * (i.e. reintroduced the exact round-12 #1 bug inside the extracted method), this test would
     * observe a frozen {@code success:true} envelope decided before the late failure was recorded
     * — this assertion is what would catch that regression.</p>
     */
    @Test
    void selectTransformResultObservesLateFailureOnlyAfterJoin() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        InputStreamSession inputSession = new InputStreamSession("application/json", null);
        long inputHandle = inputSession.register();
        NativeLib.InputCallbackFeeder feeder = new NativeLib.InputCallbackFeeder(0L, 0L, inputSession) {
            @Override
            int readChunk(byte[] dest, int max) {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return max + 1; // one past the buffer: recorded as a feeder error once this returns
            }
        };
        Thread t = new Thread(feeder, "dw-select-result-test");
        t.setDaemon(true);
        t.start();

        // Release the blocked callback ~100ms from now, on a separate thread, so the call under
        // test below begins while the feeder is still guaranteed to be blocked (no error
        // recorded yet). A correct implementation's join (inside cleanupFeeder) then waits for
        // this release before reading getError(); a buggy re-ordering would read getError() -- and
        // freeze the (wrong) success decision -- immediately, before the release even fires.
        Thread releaser = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
            release.countDown();
        });
        releaser.setDaemon(true);
        releaser.start();

        StreamSession outputSession = new StreamSession(
                new ByteArrayInputStream(new byte[0]), "application/json", "UTF-8", false);

        String resultJson = NativeLib.selectTransformResult(feeder, t, inputHandle, outputSession);

        assertFalse(t.isAlive());
        assertTrue(resultJson.contains("\"success\":false"),
                "selectTransformResult must observe the feeder's late failure, was: " + resultJson);
    }

    // ── Interrupted cleanup caller must block, not busy-spin (review #15 #2) ─────

    /**
     * Regression guard: before the fix, {@code cleanupFeeder}'s join-retry loop re-asserted the
     * interrupt inside its {@code catch (InterruptedException)} handler. If the caller's interrupt
     * flag was already set when {@code cleanupFeeder} was invoked, every subsequent
     * {@code thread.join()} re-threw immediately (an interrupted {@code join()} throws without
     * blocking), so the loop busy-spun at full CPU instead of blocking, even while the feeder was
     * still alive.
     *
     * <p>This test pins a feeder alive (parked in an overridden {@code readChunk} behind a latch)
     * and runs {@code cleanupFeeder} on a worker thread that arrives pre-interrupted. It then
     * measures the worker thread's own CPU time (via {@link ThreadMXBean#getThreadCpuTime(long)})
     * over a fixed wall-clock window while the feeder is held alive: the fix must let {@code join()}
     * actually block, consuming close to zero CPU, whereas the bug's tight
     * re-throw/catch/re-interrupt loop consumes close to 100% of the window.</p>
     *
     * <p>Determinism note: sampling {@link Thread#getState()} was tried first and rejected — the
     * JVM's {@code join()}/{@code wait()} interrupt check can transition the thread through a
     * momentary {@code WAITING} state even while it is, in aggregate, busy-spinning (verified: on
     * this JDK, 15/15 trial runs of a state-polling assertion falsely passed against the unfixed
     * {@code cleanupFeeder}, i.e. it never actually caught the regression). Per-thread CPU time
     * integrated over a window does not have that failure mode: it reliably reports ~0ns against
     * the fix and ~100% of the window against the bug (verified over multiple runs against both).
     * {@code cleaner} is a daemon thread specifically so that if this regression is ever
     * reintroduced, the busy-spin (which never terminates on its own, since {@code release} is
     * only counted down after this assertion) does not hang the test JVM — it only fails the
     * assertion below.</p>
     */
    @Test
    void interruptedCleanupCallerBlocksInJoinInsteadOfBusySpinning() throws Exception {
        InputStreamSession inputSession = new InputStreamSession("application/json", "UTF-8");
        long inputHandle = inputSession.register();

        CountDownLatch feederParked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        // Feeder blocks inside readChunk until released, so it stays alive across the join.
        NativeLib.InputCallbackFeeder feeder = new NativeLib.InputCallbackFeeder(0L, 0L, inputSession) {
            @Override
            int readChunk(byte[] dest, int max) {
                feederParked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return 0; // clean EOF once released, so the loop exits promptly
            }
        };
        Thread feederThread = new Thread(feeder, "test-busy-spin-feeder");
        feederThread.setDaemon(true);
        feederThread.start();
        assertTrue(feederParked.await(2, TimeUnit.SECONDS), "feeder never entered the read callback");

        AtomicBoolean restored = new AtomicBoolean(false);
        Thread cleaner = new Thread(() -> {
            Thread.currentThread().interrupt();   // caller arrives already interrupted
            NativeLib.cleanupFeeder(feeder, feederThread, inputHandle);
            restored.set(Thread.currentThread().isInterrupted());  // must be restored at the end
        });
        // Daemon: see the determinism note above — a reintroduced regression must not hang the JVM.
        cleaner.setDaemon(true);
        cleaner.start();

        // The fix blocks in join(), consuming ~no CPU; the bug spins, consuming ~all of the window.
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        long cleanerId = cleaner.getId();
        Thread.sleep(50); // let the cleanup thread reach steady state (blocked, or spinning)
        long cpuBefore = threadMXBean.getThreadCpuTime(cleanerId);
        Thread.sleep(300);
        long cpuAfter = threadMXBean.getThreadCpuTime(cleanerId);
        assertTrue(cpuBefore >= 0 && cpuAfter >= 0,
                "thread CPU time measurement unavailable on this JVM");
        long consumedNanos = cpuAfter - cpuBefore;
        assertTrue(consumedNanos < TimeUnit.MILLISECONDS.toNanos(100),
                "cleanup thread must block in join(), not busy-spin, under interruption (consumed "
                        + TimeUnit.NANOSECONDS.toMillis(consumedNanos) + "ms of CPU over a 300ms window)");

        release.countDown();          // let the feeder finish
        cleaner.join(5000);
        feederThread.join(5000);
        assertFalse(cleaner.isAlive());
        assertTrue(restored.get(), "interrupt status must be restored after cleanup");
    }
}
