/*
 * DataWeave C API Implementation
 */

#include "dataweave.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdint.h>

/* Platform-specific includes and definitions */
#ifdef _WIN32
    #include <windows.h>
    #define DW_THREAD_LOCAL __declspec(thread)

    /* Windows equivalents for POSIX types */
    typedef HANDLE pthread_t;
    typedef CRITICAL_SECTION pthread_mutex_t;
    typedef CONDITION_VARIABLE pthread_cond_t;
    typedef struct { int unused; } pthread_attr_t;

    /* Dynamic library handle type */
    #define DL_HANDLE HMODULE

#else
    #include <dlfcn.h>
    #include <pthread.h>
    #define DW_THREAD_LOCAL __thread

    /* Dynamic library handle type */
    #define DL_HANDLE void*
#endif

/* Base64 encoding/decoding */
static const char base64_table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/* JSON parsing (simple, sufficient for our needs) */
#include <ctype.h>

/* Thread-local error storage */
static DW_THREAD_LOCAL char error_buffer[512] = {0};

/* GraalVM types */
typedef struct graal_isolate_t graal_isolate_t;
typedef struct graal_isolatethread_t graal_isolatethread_t;

/* Function pointers for dynamic library */
typedef int (*graal_create_isolate_fn)(void*, graal_isolate_t**, graal_isolatethread_t**);
typedef int (*graal_attach_thread_fn)(graal_isolate_t*, graal_isolatethread_t**);
typedef int (*graal_detach_thread_fn)(graal_isolatethread_t*);
typedef int (*graal_tear_down_isolate_fn)(graal_isolatethread_t*);
typedef char* (*run_script_fn)(graal_isolatethread_t*, const char*, const char*);
typedef void (*free_cstring_fn)(graal_isolatethread_t*, char*);
typedef char* (*run_script_callback_fn)(graal_isolatethread_t*, const char*, const char*, dw_write_callback, void*);
typedef char* (*run_script_input_output_callback_fn)(
    graal_isolatethread_t*, const char*, const char*,
    const char*, const char*, const char*,
    dw_read_callback, dw_write_callback, void*
);

/* Runtime structure */
struct dw_runtime {
    DL_HANDLE lib_handle;
    graal_isolate_t *isolate;
    graal_isolatethread_t *thread;

    /* Function pointers */
    graal_create_isolate_fn graal_create_isolate;
    graal_attach_thread_fn graal_attach_thread;
    graal_detach_thread_fn graal_detach_thread;
    graal_tear_down_isolate_fn graal_tear_down_isolate;
    run_script_fn run_script;
    free_cstring_fn free_cstring;
    run_script_callback_fn run_script_callback;
    run_script_input_output_callback_fn run_script_input_output_callback;
};

/* Result structures */
struct dw_execution_result {
    bool success;
    char *result_encoded;
    char *error;
    bool binary;
    char *mime_type;
    char *charset;
    unsigned char *decoded_bytes;
    size_t decoded_size;
    char *decoded_string;
};

struct dw_streaming_result {
    bool success;
    char *error;
    char *mime_type;
    char *charset;
    bool binary;
};

/* Stream structure */
typedef struct chunk_node {
    unsigned char *data;
    size_t size;
    struct chunk_node *next;
} chunk_node;

struct dw_stream {
    dw_runtime *runtime;
    pthread_t worker_thread;
    chunk_node *head;
    chunk_node *tail;
    chunk_node *current;
    dw_streaming_result *metadata;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool finished;
    bool error_occurred;
    char error_msg[512];
};

/* Helper: set error message */
static void set_error(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vsnprintf(error_buffer, sizeof(error_buffer), fmt, args);
    va_end(args);
}

/* Platform abstraction layer for dynamic library loading */
#ifdef _WIN32

static DL_HANDLE dw_dlopen(const char *path) {
    return LoadLibraryA(path);
}

static void* dw_dlsym(DL_HANDLE handle, const char *symbol) {
    return (void*)GetProcAddress(handle, symbol);
}

static int dw_dlclose(DL_HANDLE handle) {
    return FreeLibrary(handle) ? 0 : -1;
}

static const char* dw_dlerror(void) {
    static char error_msg[256];
    DWORD error = GetLastError();
    FormatMessageA(
        FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
        NULL,
        error,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        error_msg,
        sizeof(error_msg),
        NULL
    );
    return error_msg;
}

/* Windows pthread wrapper functions */
static int pthread_mutex_init(pthread_mutex_t *mutex, void *attr) {
    (void)attr;
    InitializeCriticalSection(mutex);
    return 0;
}

static int pthread_mutex_lock(pthread_mutex_t *mutex) {
    EnterCriticalSection(mutex);
    return 0;
}

static int pthread_mutex_unlock(pthread_mutex_t *mutex) {
    LeaveCriticalSection(mutex);
    return 0;
}

static int pthread_mutex_destroy(pthread_mutex_t *mutex) {
    DeleteCriticalSection(mutex);
    return 0;
}

static int pthread_cond_init(pthread_cond_t *cond, void *attr) {
    (void)attr;
    InitializeConditionVariable(cond);
    return 0;
}

static int pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex) {
    SleepConditionVariableCS(cond, mutex, INFINITE);
    return 0;
}

static int pthread_cond_signal(pthread_cond_t *cond) {
    WakeConditionVariable(cond);
    return 0;
}

static int pthread_cond_destroy(pthread_cond_t *cond) {
    (void)cond;
    /* Windows condition variables don't need cleanup */
    return 0;
}

typedef struct {
    void* (*start_routine)(void*);
    void* arg;
} pthread_start_wrapper_t;

static DWORD WINAPI pthread_start_wrapper(LPVOID param) {
    pthread_start_wrapper_t *wrapper = (pthread_start_wrapper_t*)param;
    void* (*start_routine)(void*) = wrapper->start_routine;
    void* arg = wrapper->arg;
    free(wrapper);
    start_routine(arg);
    return 0;
}

static int pthread_create(pthread_t *thread, pthread_attr_t *attr, void* (*start_routine)(void*), void *arg) {
    (void)attr;
    pthread_start_wrapper_t *wrapper = malloc(sizeof(pthread_start_wrapper_t));
    if (!wrapper) return -1;

    wrapper->start_routine = start_routine;
    wrapper->arg = arg;

    *thread = CreateThread(NULL, 0, pthread_start_wrapper, wrapper, 0, NULL);
    return (*thread == NULL) ? -1 : 0;
}

static int pthread_detach(pthread_t thread) {
    CloseHandle(thread);
    return 0;
}

#else

/* POSIX systems - use native functions */
static DL_HANDLE dw_dlopen(const char *path) {
    return dlopen(path, RTLD_NOW | RTLD_LOCAL);
}

static void* dw_dlsym(DL_HANDLE handle, const char *symbol) {
    return dlsym(handle, symbol);
}

static int dw_dlclose(DL_HANDLE handle) {
    return dlclose(handle);
}

static const char* dw_dlerror(void) {
    return dlerror();
}

#endif

/* Helper: find library path */
static const char *find_library_path(void) {
    static char path_buffer[1024];
    const char *env_path = getenv("DATAWEAVE_NATIVE_LIB");

    if (env_path && env_path[0]) {
        strncpy(path_buffer, env_path, sizeof(path_buffer) - 1);
        path_buffer[sizeof(path_buffer) - 1] = '\0';
        return path_buffer;
    }

    /* Try platform-specific names */
#ifdef __APPLE__
    const char *names[] = {"dwlib.dylib", "./dwlib.dylib", NULL};
#elif defined(_WIN32)
    const char *names[] = {"dwlib.dll", "./dwlib.dll", NULL};
#else
    const char *names[] = {"dwlib.so", "./dwlib.so", NULL};
#endif

    for (int i = 0; names[i]; i++) {
        FILE *f = fopen(names[i], "r");
        if (f) {
            fclose(f);
            strncpy(path_buffer, names[i], sizeof(path_buffer) - 1);
            path_buffer[sizeof(path_buffer) - 1] = '\0';
            return path_buffer;
        }
    }

    return NULL;
}

/* Base64 encoding */
char *dw_base64_encode(const unsigned char *data, size_t size) {
    if (!data) {
        return NULL;
    }

    /* Empty input is valid - return empty string, not NULL (to distinguish from OOM) */
    if (size == 0) {
        char *empty = malloc(1);
        if (empty) {
            empty[0] = '\0';
        }
        return empty;
    }

    size_t output_len = 4 * ((size + 2) / 3);
    char *encoded = malloc(output_len + 1);
    if (!encoded) {
        return NULL;
    }

    size_t i = 0, j = 0;
    while (i < size) {
        uint32_t octet_a = i < size ? data[i++] : 0;
        uint32_t octet_b = i < size ? data[i++] : 0;
        uint32_t octet_c = i < size ? data[i++] : 0;
        uint32_t triple = (octet_a << 16) + (octet_b << 8) + octet_c;

        encoded[j++] = base64_table[(triple >> 18) & 0x3F];
        encoded[j++] = base64_table[(triple >> 12) & 0x3F];
        encoded[j++] = base64_table[(triple >> 6) & 0x3F];
        encoded[j++] = base64_table[triple & 0x3F];
    }

    /* Add padding */
    int padding = (3 - (size % 3)) % 3;
    for (int p = 0; p < padding; p++) {
        encoded[output_len - 1 - p] = '=';
    }

    encoded[output_len] = '\0';
    return encoded;
}

/* Base64 decoding */
static int base64_decode_value(char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
}

unsigned char *dw_base64_decode(const char *encoded, size_t *out_size) {
    if (!encoded || !out_size) {
        return NULL;
    }

    size_t len = strlen(encoded);
    if (len == 0 || len % 4 != 0) {
        *out_size = 0;
        return NULL;
    }

    /* Count padding */
    size_t padding = 0;
    if (encoded[len - 1] == '=') padding++;
    if (len > 1 && encoded[len - 2] == '=') padding++;

    size_t output_len = (len * 3) / 4 - padding;
    unsigned char *decoded = malloc(output_len + 1);
    if (!decoded) {
        *out_size = 0;
        return NULL;
    }

    size_t i = 0, j = 0;
    while (i < len) {
        /* Decode 4 characters into 3 bytes */
        int sextet_a = base64_decode_value(encoded[i++]);
        int sextet_b = base64_decode_value(encoded[i++]);
        int sextet_c = (i < len && encoded[i] != '=') ? base64_decode_value(encoded[i]) : 0;
        int sextet_d = (i + 1 < len && encoded[i + 1] != '=') ? base64_decode_value(encoded[i + 1]) : 0;

        i += 2; /* Advance past sextet_c and sextet_d positions */

        /* Check for invalid characters in required sextets */
        if (sextet_a == -1 || sextet_b == -1) {
            free(decoded);
            *out_size = 0;
            return NULL;
        }

        /* Build triple from valid sextets */
        uint32_t triple = (sextet_a << 18) | (sextet_b << 12);
        if (sextet_c != -1) triple |= (sextet_c << 6);
        if (sextet_d != -1) triple |= sextet_d;

        /* Extract bytes based on padding */
        decoded[j++] = (triple >> 16) & 0xFF;
        if (j < output_len) decoded[j++] = (triple >> 8) & 0xFF;
        if (j < output_len) decoded[j++] = triple & 0xFF;
    }

    decoded[output_len] = '\0';
    *out_size = output_len;
    return decoded;
}

/* Simple JSON parsing helpers */
static char *json_get_string(const char *json, const char *key) {
    char search[256];
    snprintf(search, sizeof(search), "\"%s\"", key);
    const char *pos = strstr(json, search);
    if (!pos) return NULL;

    pos += strlen(search);
    while (*pos && (*pos == ' ' || *pos == ':' || *pos == '\t' || *pos == '\n')) pos++;

    if (*pos != '"') return NULL;
    pos++;

    const char *end = pos;
    while (*end && *end != '"') {
        if (*end == '\\') end++;
        end++;
    }

    size_t len = end - pos;
    char *result = malloc(len + 1);
    if (!result) return NULL;

    strncpy(result, pos, len);
    result[len] = '\0';
    return result;
}

static bool json_get_bool(const char *json, const char *key) {
    char search[256];
    snprintf(search, sizeof(search), "\"%s\"", key);
    const char *pos = strstr(json, search);
    if (!pos) return false;

    pos += strlen(search);
    while (*pos && (*pos == ' ' || *pos == ':' || *pos == '\t' || *pos == '\n')) pos++;

    return strncmp(pos, "true", 4) == 0;
}

/* Runtime management */
dw_runtime *dw_init_with_path(const char *lib_path) {
    error_buffer[0] = '\0';

    if (!lib_path) {
        lib_path = find_library_path();
        if (!lib_path) {
            set_error("Could not find DataWeave native library. Set DATAWEAVE_NATIVE_LIB environment variable.");
            return NULL;
        }
    }

    dw_runtime *runtime = calloc(1, sizeof(dw_runtime));
    if (!runtime) {
        set_error("Failed to allocate runtime");
        return NULL;
    }

    /* Load library */
    runtime->lib_handle = dw_dlopen(lib_path);
    if (!runtime->lib_handle) {
        set_error("Failed to load library from %s: %s", lib_path, dw_dlerror());
        free(runtime);
        return NULL;
    }

    /* Load function pointers */
    runtime->graal_create_isolate = dw_dlsym(runtime->lib_handle, "graal_create_isolate");
    runtime->graal_attach_thread = dw_dlsym(runtime->lib_handle, "graal_attach_thread");
    runtime->graal_detach_thread = dw_dlsym(runtime->lib_handle, "graal_detach_thread");
    runtime->graal_tear_down_isolate = dw_dlsym(runtime->lib_handle, "graal_tear_down_isolate");
    runtime->run_script = dw_dlsym(runtime->lib_handle, "run_script");
    runtime->free_cstring = dw_dlsym(runtime->lib_handle, "free_cstring");
    runtime->run_script_callback = dw_dlsym(runtime->lib_handle, "run_script_callback");
    runtime->run_script_input_output_callback = dw_dlsym(runtime->lib_handle, "run_script_input_output_callback");

    if (!runtime->graal_create_isolate || !runtime->run_script) {
        set_error("Required functions not found in library");
        dw_dlclose(runtime->lib_handle);
        free(runtime);
        return NULL;
    }

    /* Create isolate */
    int rc = runtime->graal_create_isolate(NULL, &runtime->isolate, &runtime->thread);
    if (rc != 0) {
        set_error("Failed to create GraalVM isolate (error code %d)", rc);
        dw_dlclose(runtime->lib_handle);
        free(runtime);
        return NULL;
    }

    return runtime;
}

dw_runtime *dw_init(void) {
    return dw_init_with_path(NULL);
}

void dw_cleanup(dw_runtime *runtime) {
    if (!runtime) return;

    if (runtime->graal_tear_down_isolate && runtime->thread) {
        runtime->graal_tear_down_isolate(runtime->thread);
    }

    if (runtime->lib_handle) {
        dw_dlclose(runtime->lib_handle);
    }

    free(runtime);
}

const char *dw_get_last_error(void) {
    return error_buffer[0] ? error_buffer : NULL;
}

/* Parse execution result from JSON */
static dw_execution_result *parse_execution_result(const char *json_response) {
    if (!json_response || !json_response[0]) {
        set_error("Empty response from native library");
        return NULL;
    }

    dw_execution_result *result = calloc(1, sizeof(dw_execution_result));
    if (!result) {
        set_error("Failed to allocate result");
        return NULL;
    }

    result->success = json_get_bool(json_response, "success");

    if (result->success) {
        result->result_encoded = json_get_string(json_response, "result");
        result->mime_type = json_get_string(json_response, "mimeType");
        result->charset = json_get_string(json_response, "charset");
        result->binary = json_get_bool(json_response, "binary");
    } else {
        result->error = json_get_string(json_response, "error");
    }

    return result;
}

/* Basic execution */
dw_execution_result *dw_run(dw_runtime *runtime, const char *script, const char *inputs_json) {
    error_buffer[0] = '\0';

    if (!runtime) {
        set_error("Runtime is NULL");
        return NULL;
    }

    if (!script) {
        set_error("Script is NULL");
        return NULL;
    }

    if (script[0] == '\0') {
        set_error("Script is empty");
        return NULL;
    }

    const char *inputs = inputs_json ? inputs_json : "{}";

    char *response = runtime->run_script(runtime->thread, script, inputs);
    if (!response) {
        set_error("Native run_script returned NULL");
        return NULL;
    }

    dw_execution_result *result = parse_execution_result(response);

    if (runtime->free_cstring) {
        runtime->free_cstring(runtime->thread, response);
    }

    return result;
}

void dw_free_result(dw_execution_result *result) {
    if (!result) return;

    free(result->result_encoded);
    free(result->error);
    free(result->mime_type);
    free(result->charset);
    free(result->decoded_bytes);
    free(result->decoded_string);
    free(result);
}

/* Result accessors */
bool dw_result_success(const dw_execution_result *result) {
    return result ? result->success : false;
}

const char *dw_result_error(const dw_execution_result *result) {
    return result ? result->error : NULL;
}

const char *dw_result_get_encoded(const dw_execution_result *result) {
    return result ? result->result_encoded : NULL;
}

const unsigned char *dw_result_get_bytes(const dw_execution_result *result, size_t *out_size) {
    if (!result || !result->result_encoded) {
        if (out_size) *out_size = 0;
        return NULL;
    }

    if (!result->decoded_bytes) {
        /* Lazy decode */
        dw_execution_result *mutable = (dw_execution_result *)result;
        mutable->decoded_bytes = dw_base64_decode(result->result_encoded, &mutable->decoded_size);
    }

    if (out_size) *out_size = result->decoded_size;
    return result->decoded_bytes;
}

const char *dw_result_get_string(const dw_execution_result *result) {
    if (!result || !result->result_encoded) {
        return NULL;
    }

    if (!result->decoded_string) {
        /* Lazy decode and convert to string */
        size_t size;
        const unsigned char *bytes = dw_result_get_bytes(result, &size);
        if (bytes) {
            dw_execution_result *mutable = (dw_execution_result *)result;
            mutable->decoded_string = malloc(size + 1);
            if (mutable->decoded_string) {
                memcpy(mutable->decoded_string, bytes, size);
                mutable->decoded_string[size] = '\0';
            }
        }
    }

    return result->decoded_string;
}

const char *dw_result_mime_type(const dw_execution_result *result) {
    return result ? result->mime_type : NULL;
}

const char *dw_result_charset(const dw_execution_result *result) {
    return result ? result->charset : NULL;
}

bool dw_result_is_binary(const dw_execution_result *result) {
    return result ? result->binary : false;
}

/* Streaming result parsing */
static dw_streaming_result *parse_streaming_result(const char *json_response) {
    if (!json_response || !json_response[0]) {
        set_error("Empty response from native library");
        return NULL;
    }

    dw_streaming_result *result = calloc(1, sizeof(dw_streaming_result));
    if (!result) {
        set_error("Failed to allocate streaming result");
        return NULL;
    }

    result->success = json_get_bool(json_response, "success");

    if (result->success) {
        result->mime_type = json_get_string(json_response, "mimeType");
        result->charset = json_get_string(json_response, "charset");
        result->binary = json_get_bool(json_response, "binary");
    } else {
        result->error = json_get_string(json_response, "error");
    }

    return result;
}

/* Streaming result accessors */
bool dw_streaming_result_success(const dw_streaming_result *result) {
    return result ? result->success : false;
}

const char *dw_streaming_result_error(const dw_streaming_result *result) {
    return result ? result->error : NULL;
}

const char *dw_streaming_result_mime_type(const dw_streaming_result *result) {
    return result ? result->mime_type : NULL;
}

const char *dw_streaming_result_charset(const dw_streaming_result *result) {
    return result ? result->charset : NULL;
}

bool dw_streaming_result_is_binary(const dw_streaming_result *result) {
    return result ? result->binary : false;
}

void dw_free_streaming_result(dw_streaming_result *result) {
    if (!result) return;

    free(result->error);
    free(result->mime_type);
    free(result->charset);
    free(result);
}

/* Callback-based output streaming */
dw_streaming_result *dw_run_callback(
    dw_runtime *runtime,
    const char *script,
    dw_write_callback callback,
    void *ctx,
    const char *inputs_json
) {
    error_buffer[0] = '\0';

    if (!runtime) {
        set_error("Runtime is NULL");
        return NULL;
    }

    if (!script) {
        set_error("Script is NULL");
        return NULL;
    }

    if (!callback) {
        set_error("Callback is NULL");
        return NULL;
    }

    if (!runtime->run_script_callback) {
        set_error("Callback streaming not supported by this library version");
        return NULL;
    }

    const char *inputs = inputs_json ? inputs_json : "{}";

    char *response = runtime->run_script_callback(runtime->thread, script, inputs, callback, ctx);
    if (!response) {
        set_error("Native run_script_callback returned NULL");
        return NULL;
    }

    dw_streaming_result *result = parse_streaming_result(response);

    if (runtime->free_cstring) {
        runtime->free_cstring(runtime->thread, response);
    }

    /* Validate result success to detect callback errors */
    if (result && !result->success && result->error) {
        set_error("%s", result->error);
    }

    return result;
}

/* Stream management */
typedef struct {
    dw_runtime *runtime;
    dw_stream *stream;
    const char *script;
    const char *inputs_json;
} stream_worker_context;

/* Write callback for stream worker thread */
static int stream_write_callback(void *ctx, const char *buffer, int length) {
    dw_stream *stream = (dw_stream *)ctx;

    /* Allocate new chunk node */
    chunk_node *node = malloc(sizeof(chunk_node));
    if (!node) {
        return -1;
    }

    node->data = malloc(length);
    if (!node->data) {
        free(node);
        return -1;
    }

    memcpy(node->data, buffer, length);
    node->size = length;
    node->next = NULL;

    /* Add to stream's linked list */
    pthread_mutex_lock(&stream->mutex);
    if (!stream->head) {
        stream->head = node;
        stream->tail = node;
    } else {
        stream->tail->next = node;
        stream->tail = node;
    }
    pthread_cond_signal(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);

    return 0;
}

static void *stream_worker_thread(void *arg) {
    stream_worker_context *ctx = (stream_worker_context *)arg;
    dw_runtime *runtime = ctx->runtime;
    dw_stream *stream = ctx->stream;

    /* Attach worker thread to isolate */
    graal_isolatethread_t *worker_thread = NULL;
    int rc = runtime->graal_attach_thread(runtime->isolate, &worker_thread);
    if (rc != 0) {
        pthread_mutex_lock(&stream->mutex);
        snprintf(stream->error_msg, sizeof(stream->error_msg),
                 "Failed to attach worker thread (code %d)", rc);
        stream->error_occurred = true;
        stream->finished = true;
        pthread_cond_signal(&stream->cond);
        pthread_mutex_unlock(&stream->mutex);
        free(ctx);
        return NULL;
    }

    /* Execute script with callback */
    char *response = runtime->run_script_callback(
        worker_thread,
        ctx->script,
        ctx->inputs_json,
        stream_write_callback,
        stream
    );

    /* Parse metadata */
    if (response) {
        stream->metadata = parse_streaming_result(response);
        if (runtime->free_cstring) {
            runtime->free_cstring(worker_thread, response);
        }
    }

    /* Detach worker thread */
    if (runtime->graal_detach_thread) {
        runtime->graal_detach_thread(worker_thread);
    }

    pthread_mutex_lock(&stream->mutex);
    stream->finished = true;
    pthread_cond_signal(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);

    free(ctx);
    return NULL;
}

dw_stream *dw_run_streaming(dw_runtime *runtime, const char *script, const char *inputs_json) {
    error_buffer[0] = '\0';

    if (!runtime) {
        set_error("Runtime is NULL");
        return NULL;
    }

    if (!script) {
        set_error("Script is NULL");
        return NULL;
    }

    if (!runtime->run_script_callback) {
        set_error("Streaming not supported by this library version");
        return NULL;
    }

    dw_stream *stream = calloc(1, sizeof(dw_stream));
    if (!stream) {
        set_error("Failed to allocate stream");
        return NULL;
    }

    stream->runtime = runtime;
    pthread_mutex_init(&stream->mutex, NULL);
    pthread_cond_init(&stream->cond, NULL);

    /* Allocate worker context */
    stream_worker_context *worker_ctx = malloc(sizeof(stream_worker_context));
    if (!worker_ctx) {
        pthread_mutex_destroy(&stream->mutex);
        pthread_cond_destroy(&stream->cond);
        free(stream);
        set_error("Failed to allocate worker context");
        return NULL;
    }

    worker_ctx->runtime = runtime;
    worker_ctx->stream = stream;
    worker_ctx->script = script;
    worker_ctx->inputs_json = inputs_json;

    /* Create worker thread to execute script and stream chunks */
    pthread_t worker_thread;
    int rc = pthread_create(&worker_thread, NULL, stream_worker_thread, worker_ctx);
    if (rc != 0) {
        free(worker_ctx);
        pthread_mutex_destroy(&stream->mutex);
        pthread_cond_destroy(&stream->cond);
        free(stream);
        snprintf(error_buffer, sizeof(error_buffer),
                 "Failed to create worker thread (error %d)", rc);
        return NULL;
    }

    /* Detach thread so it cleans up automatically */
    pthread_detach(worker_thread);

    return stream;
}

int dw_stream_next(dw_stream *stream, const unsigned char **out_buffer, size_t *out_size) {
    if (!stream || !out_buffer || !out_size) {
        return -1;
    }

    pthread_mutex_lock(&stream->mutex);

    /* Loop to re-check conditions after waking from wait */
    while (!stream->current && !stream->head) {
        if (stream->finished) {
            pthread_mutex_unlock(&stream->mutex);
            return 0;  /* EOF */
        }

        /* Wait for data or completion */
        pthread_cond_wait(&stream->cond, &stream->mutex);

        /* Re-check after waking: stream may have finished with no data */
        if (!stream->head && stream->finished) {
            pthread_mutex_unlock(&stream->mutex);
            return 0;  /* EOF */
        }
    }

    if (!stream->current) {
        stream->current = stream->head;
    }

    if (stream->current) {
        *out_buffer = stream->current->data;
        *out_size = stream->current->size;
        stream->current = stream->current->next;
        pthread_mutex_unlock(&stream->mutex);
        return 1;  /* Chunk available */
    }

    pthread_mutex_unlock(&stream->mutex);
    return stream->finished ? 0 : -1;
}

const dw_streaming_result *dw_stream_metadata(dw_stream *stream) {
    return stream ? stream->metadata : NULL;
}

void dw_stream_free(dw_stream *stream) {
    if (!stream) return;

    /* Free chunks */
    chunk_node *node = stream->head;
    while (node) {
        chunk_node *next = node->next;
        free(node->data);
        free(node);
        node = next;
    }

    if (stream->metadata) {
        dw_free_streaming_result(stream->metadata);
    }

    pthread_mutex_destroy(&stream->mutex);
    pthread_cond_destroy(&stream->cond);
    free(stream);
}

/* Bidirectional streaming */
dw_streaming_result *dw_run_transform(
    dw_runtime *runtime,
    const char *script,
    dw_read_callback read_callback,
    dw_write_callback write_callback,
    const char *input_name,
    const char *input_mime_type,
    const char *input_charset,
    void *ctx,
    const char *inputs_json
) {
    error_buffer[0] = '\0';

    if (!runtime) {
        set_error("Runtime is NULL");
        return NULL;
    }

    if (!script) {
        set_error("Script is NULL");
        return NULL;
    }

    if (!read_callback || !write_callback) {
        set_error("Callbacks are NULL");
        return NULL;
    }

    if (!runtime->run_script_input_output_callback) {
        set_error("Bidirectional streaming not supported by this library version");
        return NULL;
    }

    const char *inputs = inputs_json ? inputs_json : "{}";
    const char *name = input_name ? input_name : "payload";
    const char *mime = input_mime_type ? input_mime_type : "application/json";

    char *response = runtime->run_script_input_output_callback(
        runtime->thread,
        script,
        inputs,
        name,
        mime,
        input_charset,
        read_callback,
        write_callback,
        ctx
    );

    if (!response) {
        set_error("Native run_script_input_output_callback returned NULL");
        return NULL;
    }

    dw_streaming_result *result = parse_streaming_result(response);

    if (runtime->free_cstring) {
        runtime->free_cstring(runtime->thread, response);
    }

    return result;
}

/* Utility functions */
void dw_free_string(char *str) {
    free(str);
}

void dw_free_bytes(unsigned char *bytes) {
    free(bytes);
}

/* Helper: escape JSON string */
static char *json_escape_string(const char *str) {
    if (!str) return strdup("null");

    size_t len = strlen(str);
    char *escaped = malloc(len * 2 + 3);  /* Worst case: all chars escaped + quotes + null */
    if (!escaped) return NULL;

    char *p = escaped;
    *p++ = '"';

    for (size_t i = 0; i < len; i++) {
        char c = str[i];
        if (c == '"' || c == '\\') {
            *p++ = '\\';
            *p++ = c;
        } else if (c == '\n') {
            *p++ = '\\';
            *p++ = 'n';
        } else if (c == '\r') {
            *p++ = '\\';
            *p++ = 'r';
        } else if (c == '\t') {
            *p++ = '\\';
            *p++ = 't';
        } else {
            *p++ = c;
        }
    }

    *p++ = '"';
    *p = '\0';

    return escaped;
}

char *dw_create_input_string(const char *name, const char *content, const char *mime_type) {
    if (!name || !content) return NULL;

    char *encoded = dw_base64_encode((const unsigned char *)content, strlen(content));
    if (!encoded) return NULL;

    char *escaped_name = json_escape_string(name);
    if (!escaped_name) {
        free(encoded);
        return NULL;
    }

    const char *mime = mime_type ? mime_type : "text/plain";

    /* Note: escaped_name already includes quotes */
    size_t result_size = strlen(escaped_name) + strlen(encoded) + strlen(mime) + 200;
    char *result = malloc(result_size);
    if (!result) {
        free(encoded);
        free(escaped_name);
        return NULL;
    }

    snprintf(result, result_size,
            "{%s:{\"content\":\"%s\",\"mimeType\":\"%s\",\"charset\":\"UTF-8\"}}",
            escaped_name, encoded, mime);

    free(encoded);
    free(escaped_name);
    return result;
}

char *dw_create_input_bytes(
    const char *name,
    const unsigned char *data,
    size_t size,
    const char *mime_type,
    const char *charset
) {
    if (!name || !data) return NULL;

    char *encoded = dw_base64_encode(data, size);
    if (!encoded) return NULL;

    char *escaped_name = json_escape_string(name);
    if (!escaped_name) {
        free(encoded);
        return NULL;
    }

    const char *mime = mime_type ? mime_type : "application/octet-stream";
    const char *cs = charset ? charset : "UTF-8";

    /* Note: escaped_name already includes quotes */
    size_t result_size = strlen(escaped_name) + strlen(encoded) + strlen(mime) + strlen(cs) + 200;
    char *result = malloc(result_size);
    if (!result) {
        free(encoded);
        free(escaped_name);
        return NULL;
    }

    snprintf(result, result_size,
            "{%s:{\"content\":\"%s\",\"mimeType\":\"%s\",\"charset\":\"%s\"}}",
            escaped_name, encoded, mime, cs);

    free(encoded);
    free(escaped_name);
    return result;
}
