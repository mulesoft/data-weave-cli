/*
 * repro_stream_uaf.c — regression tests for the two Critical use-after-free
 * findings in the C streaming worker, using the mock dwlib + a barrier.
 *
 *   [1] dw_run_streaming detached its worker thread and dw_stream_free never
 *       joined it. A caller that freed the stream while the worker was still in
 *       run_script_callback triggered a UAF when the worker's write callback
 *       dereferenced the freed `stream` struct. FIX: store pthread_t, join in free.
 *
 *   [2] dw_run_streaming stored the caller-owned `script`/`inputs_json`
 *       pointers without copying them. A caller that freed those buffers before
 *       the detached worker consumed them triggered a UAF. FIX: strdup on entry.
 *
 * With the fixes:
 *   [1] dw_stream_free now joins the worker thread, so it blocks until the worker
 *       finishes. No UAF occurs.
 *   [2] The wrapper owns its own copies of script/inputs_json, so the caller can
 *       free their originals immediately. No UAF occurs.
 *
 * The test verifies the FIXED behavior: built with -fsanitize=address, it should
 * exit cleanly with no ASan errors. Select mode via argv[1]:
 *   "script" -> test finding [2] fix (caller frees script/inputs)
 *   "stream" -> test finding [1] fix (caller frees stream, which joins worker)
 *
 * Exit 0 == pass (clean run). Exit nonzero == fail (ASan error / timeout).
 */

#include "../../include/dataweave.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

/* Barrier hooks exported by the mock, resolved via the wrapper's dlopen handle.
 * We declare them and load them with dlsym from the same library the wrapper
 * loaded, keeping a single shared copy of the barrier state. */
#include <dlfcn.h>

typedef void (*void_fn)(void);

/* Helper thread to release the barrier for "stream" mode, avoiding deadlock when
 * we call dw_stream_free (which now joins the worker) before the worker finishes. */
static void_fn proceed_fn = NULL;

static void *releaser_thread(void *arg) {
    (void)arg;
    usleep(10 * 1000);  /* let main thread enter dw_stream_free's join */
    proceed_fn();
    return NULL;
}

int main(int argc, char **argv) {
    const char *mode = argc > 1 ? argv[1] : "stream";

    const char *libpath = getenv("DATAWEAVE_NATIVE_LIB");
    if (!libpath) {
        fprintf(stderr, "DATAWEAVE_NATIVE_LIB must point at the mock dwlib\n");
        return 2;
    }

    /* Load the barrier hooks from the mock */
    void *h = dlopen(libpath, RTLD_NOW | RTLD_GLOBAL);
    if (!h) { fprintf(stderr, "dlopen mock failed: %s\n", dlerror()); return 2; }
    void_fn enable   = (void_fn)dlsym(h, "mock_enable_barrier");
    void_fn entered  = (void_fn)dlsym(h, "mock_wait_entered");
    proceed_fn = (void_fn)dlsym(h, "mock_signal_proceed");
    if (!enable || !entered || !proceed_fn) {
        fprintf(stderr, "mock barrier symbols missing\n");
        return 2;
    }

    dw_runtime *rt = dw_init();
    if (!rt) {
        fprintf(stderr, "dw_init failed: %s\n", dw_get_last_error());
        return 2;
    }

    enable();

    /* Heap-allocate script + inputs so we can free them out from under the
     * worker (testing finding [2] fix). */
    char *script = strdup("payload map $");
    char *inputs = strdup("{}");

    dw_stream *stream = dw_run_streaming(rt, script, inputs);
    if (!stream) {
        fprintf(stderr, "dw_run_streaming failed: %s\n", dw_get_last_error());
        return 2;
    }

    /* Wait until the worker is parked inside run_script_callback */
    entered();

    if (strcmp(mode, "script") == 0) {
        /* Finding [2] FIX TEST: the wrapper now owns copies of script/inputs,
         * so the caller can safely free the originals immediately. */
        free(script);
        free(inputs);
        script = inputs = NULL;
        printf("[test] freed caller-owned script/inputs (wrapper has its own copies)...\n");

        /* Release worker to let it finish */
        proceed_fn();

        /* Clean up stream (joins worker) */
        dw_stream_free(stream);
        stream = NULL;
        printf("[test] clean shutdown (no UAF)\n");
    } else {
        /* Finding [1] FIX TEST: dw_stream_free now joins the worker thread.
         * To avoid deadlock (calling free before proceed on the same thread),
         * spawn a helper to call proceed while we block in join. */
        printf("[test] freeing stream (will join worker)...\n");

        pthread_t releaser;
        pthread_create(&releaser, NULL, releaser_thread, NULL);

        /* This will block in pthread_join until the worker finishes */
        dw_stream_free(stream);
        stream = NULL;

        pthread_join(releaser, NULL);
        printf("[test] clean shutdown (worker joined, no UAF)\n");

        /* Caller buffers still live, free them now */
        free(script);
        free(inputs);
        script = inputs = NULL;
    }

    dw_cleanup(rt);
    printf("[PASS] mode=%s — no ASan errors\n", mode);
    return 0;
}
