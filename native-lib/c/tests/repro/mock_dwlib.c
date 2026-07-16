/*
 * mock_dwlib.c — a fake GraalVM "dwlib" shared library for reproduction tests.
 *
 * The real DataWeave native library requires a multi-GB GraalVM Native Image
 * build. The C wrapper (native-lib/c/src/dataweave.c) loads its native library
 * with dlopen()/dlsym() at runtime, resolving symbols by name. That lets us
 * substitute this mock via the DATAWEAVE_NATIVE_LIB environment variable and
 * drive the wrapper's control flow deterministically.
 *
 * A test barrier (mock_enable_barrier / mock_wait_entered / mock_signal_proceed)
 * lets a test pause the wrapper's streaming worker at the exact moment it is
 * inside run_script_callback — after the stream handle and the caller-owned
 * script/inputs pointers have been captured, but before they are dereferenced.
 * The test then frees those objects and releases the worker, so the subsequent
 * dereference is a use-after-free that AddressSanitizer catches.
 */

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

/* Opaque GraalVM types — the wrapper only ever holds pointers to these. */
typedef struct graal_isolate_t graal_isolate_t;
typedef struct graal_isolatethread_t graal_isolatethread_t;

typedef int (*WriteCallback)(void *ctx, const char *buffer, int length);
typedef int (*ReadCallback)(void *ctx, char *buffer, int bufferSize);

/* --- Deterministic test barrier ------------------------------------------ */

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_cv = PTHREAD_COND_INITIALIZER;
static int g_barrier_enabled = 0;
static int g_entered = 0;
static int g_proceed = 0;

/* Enable the barrier and reset its state. Call before dw_run_streaming. */
void mock_enable_barrier(void) {
    pthread_mutex_lock(&g_mu);
    g_barrier_enabled = 1;
    g_entered = 0;
    g_proceed = 0;
    pthread_mutex_unlock(&g_mu);
}

/* Block until the worker thread has entered run_script_callback. */
void mock_wait_entered(void) {
    pthread_mutex_lock(&g_mu);
    while (!g_entered) {
        pthread_cond_wait(&g_cv, &g_mu);
    }
    pthread_mutex_unlock(&g_mu);
}

/* Release the worker so it proceeds to dereference the captured pointers. */
void mock_signal_proceed(void) {
    pthread_mutex_lock(&g_mu);
    g_proceed = 1;
    pthread_cond_broadcast(&g_cv);
    pthread_mutex_unlock(&g_mu);
}

static void barrier_gate(void) {
    if (!g_barrier_enabled) return;
    pthread_mutex_lock(&g_mu);
    g_entered = 1;
    pthread_cond_broadcast(&g_cv);
    while (!g_proceed) {
        pthread_cond_wait(&g_cv, &g_mu);
    }
    pthread_mutex_unlock(&g_mu);
}

/* --- GraalVM isolate lifecycle (no-op stubs) ----------------------------- */

int graal_create_isolate(void *params, graal_isolate_t **isolate,
                         graal_isolatethread_t **thread) {
    (void)params;
    *isolate = (graal_isolate_t *)0x1;
    *thread = (graal_isolatethread_t *)0x1;
    return 0;
}

int graal_attach_thread(graal_isolate_t *isolate, graal_isolatethread_t **thread) {
    (void)isolate;
    *thread = (graal_isolatethread_t *)0x1;
    return 0;
}

int graal_detach_thread(graal_isolatethread_t *thread) {
    (void)thread;
    return 0;
}

int graal_tear_down_isolate(graal_isolatethread_t *thread) {
    (void)thread;
    return 0;
}

/* --- Script execution ----------------------------------------------------- */

char *run_script(graal_isolatethread_t *thread, const char *script,
                 const char *inputsJson) {
    (void)thread;
    /* Model normal use: read the inputs the wrapper handed us. */
    (void)strlen(script);
    (void)strlen(inputsJson);
    return strdup("{\"success\":true,\"result\":\"\",\"mimeType\":\"application/json\"}");
}

void free_cstring(graal_isolatethread_t *thread, char *pointer) {
    (void)thread;
    free(pointer);
}

char *run_script_callback(graal_isolatethread_t *thread, const char *script,
                          const char *inputsJson, WriteCallback cb, void *ctx) {
    (void)thread;

    /* Pause here so the test can free the stream / script before we touch them. */
    barrier_gate();

    /* Finding [2]: the wrapper passes the CALLER-OWNED script/inputs pointers
     * straight through (no strdup). Reading them models the native engine
     * consuming the script. If the caller already freed them -> use-after-free
     * (caught by ASan's strlen interceptor even though this mock is not
     * instrumented). */
    volatile size_t consumed = strlen(script) + strlen(inputsJson);
    (void)consumed;

    /* Finding [1]: invoking the write callback re-enters the wrapper's
     * stream_write_callback, which dereferences the `stream` handle. If the
     * caller already called dw_stream_free() -> use-after-free on the freed
     * stream struct (caught by ASan in the instrumented wrapper code). */
    const char *chunk = "chunk";
    if (cb) {
        cb(ctx, chunk, 5);
    }

    return strdup("{\"success\":true,\"mimeType\":\"application/json\"}");
}

char *run_script_input_output_callback(graal_isolatethread_t *thread,
                                       const char *script, const char *inputsJson,
                                       const char *inputName,
                                       const char *inputMimeType,
                                       const char *inputCharset,
                                       ReadCallback readCb, WriteCallback writeCb,
                                       void *ctx) {
    (void)thread; (void)script; (void)inputsJson; (void)inputName;
    (void)inputMimeType; (void)inputCharset; (void)readCb; (void)writeCb; (void)ctx;
    return strdup("{\"success\":true,\"mimeType\":\"application/json\"}");
}
