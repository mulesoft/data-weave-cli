/*
 * repro_metadata_race.c — regression test for finding [23] (data race on
 * stream->metadata) in the C streaming worker, using ThreadSanitizer.
 *
 *   [23] stream_worker_thread wrote `stream->metadata` OUTSIDE the mutex;
 *        dw_stream_metadata() read it lock-free. The mutex/condvar only
 *        established happens-before for the `finished` flag, NOT for the
 *        separate `metadata` pointer, so a consumer that called
 *        dw_stream_metadata() while the worker was finishing raced on that field
 *        (stale NULL / torn pointer on weak-memory archs).
 *
 * FIX: the worker now writes stream->metadata under the lock (before setting
 * finished), and dw_stream_metadata() reads it under the lock.
 *
 * This test verifies the FIXED behavior: park the worker inside
 * run_script_callback with the mock's barrier, then release it (worker returns
 * and writes stream->metadata under the lock) while the main thread hammers
 * dw_stream_metadata() (which also locks). With the fix, TSan should report NO
 * data race on `metadata`.
 *
 * Built with -fsanitize=thread (a SEPARATE binary from the ASan repros — the
 * two sanitizers cannot be combined). Compiled together with
 * ../../src/dataweave.c so the instrumented write and read are both visible to
 * TSan. A clean run (no TSan race report) == PASS.
 *
 * Exit 0 == pass (no race). Exit nonzero == fail (race or crash).
 */

#include "../../include/dataweave.h"

#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef void (*void_fn)(void);

int main(void) {
    const char *libpath = getenv("DATAWEAVE_NATIVE_LIB");
    if (!libpath) {
        fprintf(stderr, "DATAWEAVE_NATIVE_LIB must point at the mock dwlib\n");
        return 2;
    }

    void *h = dlopen(libpath, RTLD_NOW | RTLD_GLOBAL);
    if (!h) { fprintf(stderr, "dlopen mock failed: %s\n", dlerror()); return 2; }
    void_fn enable  = (void_fn)dlsym(h, "mock_enable_barrier");
    void_fn entered = (void_fn)dlsym(h, "mock_wait_entered");
    void_fn proceed = (void_fn)dlsym(h, "mock_signal_proceed");
    if (!enable || !entered || !proceed) {
        fprintf(stderr, "mock barrier symbols missing\n");
        return 2;
    }

    dw_runtime *rt = dw_init();
    if (!rt) {
        fprintf(stderr, "dw_init failed: %s\n", dw_get_last_error());
        return 2;
    }

    enable();

    char *script = strdup("payload map $");
    char *inputs = strdup("{}");

    dw_stream *stream = dw_run_streaming(rt, script, inputs);
    if (!stream) {
        fprintf(stderr, "dw_run_streaming failed: %s\n", dw_get_last_error());
        return 2;
    }

    /* Worker is now parked inside run_script_callback */
    entered();

    /* Release the worker; it will write stream->metadata under the lock */
    proceed();

    /* Concurrently read stream->metadata (also under the lock now).
     * With the fix, both accesses are synchronized by the mutex -> no race. */
    const dw_streaming_result *m = NULL;
    for (int i = 0; i < 2000000; i++) {
        m = dw_stream_metadata(stream);
        if (m) break; /* observed the write; race window covered */
    }

    printf("[test] metadata read %s\n", m ? "succeeded" : "timed out (still NULL)");

    /* Clean up properly now that [1] is fixed (dw_stream_free joins worker) */
    dw_stream_free(stream);
    free(script);
    free(inputs);
    dw_cleanup(rt);

    printf("[PASS] no TSan data race on stream->metadata\n");
    return 0;
}
