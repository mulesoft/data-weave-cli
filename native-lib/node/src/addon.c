#include <node_api.h>
#include <uv.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

// GraalVM function pointer types
typedef int (*graal_create_isolate_fn)(void*, void**, void**);
typedef int (*graal_attach_thread_fn)(void*, void**);
typedef int (*graal_detach_thread_fn)(void*);
typedef int (*graal_tear_down_isolate_fn)(void*);
typedef void* (*run_script_fn)(void*, const char*, const char*);
typedef void (*free_cstring_fn)(void*, void*);
typedef int (*write_callback_t)(void* ctx, const char* buf, int len);
typedef int (*read_callback_t)(void* ctx, char* buf, int buf_size);
typedef char* (*resolve_module_callback_t)(void* thread, void* ctx, const char* module_path);
typedef void* (*run_script_callback_fn)(void*, const char*, const char*, write_callback_t, void*);
typedef void* (*run_script_input_output_callback_fn)(void*, const char*, const char*, const char*, const char*, const char*, read_callback_t, write_callback_t, void*);

// Per-engine entrypoint types. Handles are Java long values and MUST be C
// long long everywhere (plain long is 32-bit on Windows LLP64 and would
// truncate a 64-bit handle).
typedef long long (*create_engine_fn)(void*);
typedef long long (*create_engine_with_resolver_fn)(void*, resolve_module_callback_t, void*);
typedef void (*destroy_engine_fn)(void*, long long);
typedef void* (*run_script_engine_fn)(void*, long long, const char*, const char*);
typedef void* (*run_script_callback_engine_fn)(void*, long long, const char*, const char*, write_callback_t, void*);
typedef void* (*run_script_input_output_callback_engine_fn)(void*, long long, const char*, const char*, const char*, const char*, const char*, read_callback_t, write_callback_t, void*);

// Global state
static uv_lib_t g_lib;
static int g_lib_loaded = 0;
static void* g_isolate = NULL;
static void* g_thread = NULL;
static int g_initialized = 0;
static int g_ref_count = 0;
static uv_mutex_t g_mutex;
// Guards initialization of the process-global g_mutex. Init() runs once per
// Worker environment that loads this addon, but g_mutex is process-global —
// re-running uv_mutex_init() on an already-initialized mutex from a second
// Worker's Init() call is undefined behavior (and can corrupt the mutex for
// every other thread already relying on it). uv_once ensures the real init
// body runs exactly once per process regardless of how many Workers load us.
static uv_once_t g_mutex_once = UV_ONCE_INIT;

static graal_create_isolate_fn fn_create_isolate = NULL;
static graal_attach_thread_fn fn_attach_thread = NULL;
static graal_detach_thread_fn fn_detach_thread = NULL;
static graal_tear_down_isolate_fn fn_tear_down_isolate = NULL;
static run_script_fn fn_run_script = NULL;
static free_cstring_fn fn_free_cstring = NULL;
static run_script_callback_fn fn_run_script_callback = NULL;
static run_script_input_output_callback_fn fn_run_script_input_output_callback = NULL;

// Per-engine entrypoints
static create_engine_fn fn_create_engine = NULL;
static create_engine_with_resolver_fn fn_create_engine_with_resolver = NULL;
static destroy_engine_fn fn_destroy_engine = NULL;
static run_script_engine_fn fn_run_script_engine = NULL;
static run_script_callback_engine_fn fn_run_script_callback_engine = NULL;
static run_script_input_output_callback_engine_fn fn_run_script_input_output_callback_engine = NULL;

// A single run may trigger resolve_module_callback multiple times (one script
// can import several modules). Native copies each returned buffer immediately,
// but the copy is made *after* our callback returns — we don't get a per-call
// "done freeing" signal, only "the whole run finished". So track every buffer
// allocated during one run and free them all once the native call returns.
typedef struct resolver_result_node {
    char* buf;
    struct resolver_result_node* next;
} resolver_result_node_t;

// Per-engine resolver bridge: one node per resolver-backed engine, passed to
// Java as the callback ctx word and forwarded back to resolve_module_callback.
//
// Unlike the streaming/transform entrypoints, runScriptEngine's native call
// executes synchronously on the very thread that invoked it from JS — no
// background uv_thread is spawned. So when native code calls back into
// resolve_module_callback(), we are already on the correct (JS) thread and
// can call directly into V8/napi. Do NOT use napi_threadsafe_function here:
// that pattern queues work for "the" JS thread to pick up and blocks the
// caller on a condition variable until it's serviced — but if the caller
// *is* the JS thread, it can never service its own queued item, causing a
// deadlock (a real bug fixed in this codebase — see Task 11 report).
//
// napi_env/napi_ref are thread-affine; each bridge records the JS thread that
// created it (owner) so resolve_module_callback can detect a mismatch — e.g. a
// streamed/transform custom-module lookup arriving on the background uv_thread
// — and fail closed (return "not found") instead of crashing.
typedef struct engine_bridge {
    long long handle;
    napi_env env;
    napi_ref resolver_js;             // NULL => resolver-less engine (no bridge created)
    uv_thread_t owner;                // JS thread that created and must run this engine
    resolver_result_node_t* results;  // buffers to free after each run on this engine
    // Lifecycle accounting, mutated only under g_mutex. A streaming/transform op
    // runs the native call on a background uv_thread that can still call back into
    // resolve_module_callback with this bridge as ctx, so the bridge must outlive
    // every in-flight op. in_flight counts ops that can still dereference this
    // bridge; destroy_pending marks that destroyEngine ran while in_flight > 0 and
    // freeing was deferred to the last op draining on the owner thread.
    int in_flight;
    bool destroy_pending;
    struct engine_bridge* next;
} engine_bridge_t;
static engine_bridge_t* g_bridges = NULL;  // linked list, guarded by g_mutex

// --- Teardown-vs-active-ops coordination (deadlock fix) ---
//
// napi_cleanup's last-release path used to synchronously join a thread that
// calls graal_tear_down_isolate(), which blocks until every GraalVM-attached
// thread detaches. A runStreaming()/runTransform() background worker stays
// attached and can be mid-delivery in napi_call_threadsafe_function(...,
// napi_tsfn_blocking), which needs the JS thread to run its callback -- but
// the JS thread is the one blocked in the join. g_active_ops tracks every
// in-flight streaming/transform op (resolver-backed or not, since teardown
// blocks on ANY attached worker) so napi_cleanup can wait for them to drain
// on a dedicated thread instead of blocking the calling JS thread.
static int g_active_ops = 0;
static bool g_teardown_pending = false;
static uv_cond_t g_teardown_cond;

// One node per cleanup() call that arrived while a teardown was already
// pending. napi_env/napi_deferred/napi_threadsafe_function are thread-affine,
// so a second cleanup() call from a different Worker's env cannot have its
// promise resolved via another env's tsfn -- each waiting caller gets its own
// node, created on its own env, resolved by the waiter thread on completion.
typedef struct teardown_waiter {
    napi_env env;
    napi_deferred deferred;
    napi_threadsafe_function tsfn;
    struct teardown_waiter* next;
} teardown_waiter_t;
static teardown_waiter_t* g_teardown_waiters = NULL;  // linked list, guarded by g_mutex

// Returns true if the buffer is now tracked (or there was nothing to track).
// Returns false only when a buffer was supplied but the tracking node could
// not be allocated — in that case the caller owns `buf` again and MUST free
// it itself, since it will never be reachable from b->results.
static bool resolver_results_track(engine_bridge_t* b, char* buf) {
    if (b == NULL || buf == NULL) return true;
    resolver_result_node_t* node = (resolver_result_node_t*)malloc(sizeof(resolver_result_node_t));
    if (node == NULL) return false;  // OOM: caller must free buf to avoid leaking it untracked.
    node->buf = buf;
    node->next = b->results;
    b->results = node;
    return true;
}

static void resolver_results_free_all(engine_bridge_t* b) {
    if (b == NULL) return;
    resolver_result_node_t* node = b->results;
    while (node != NULL) {
        resolver_result_node_t* next = node->next;
        free(node->buf);
        free(node);
        node = next;
    }
    b->results = NULL;
}

// Call under g_mutex.
static engine_bridge_t* bridge_find(long long handle) {
    for (engine_bridge_t* b = g_bridges; b != NULL; b = b->next) {
        if (b->handle == handle) return b;
    }
    return NULL;
}

// Fully dispose of a bridge: delete its napi_ref, free tracked result buffers,
// free the struct. napi_ref/napi_env are thread-affine, so this MUST run on the
// bridge's owner thread (the JS/Worker thread that created it) while that env is
// still alive. The bridge must already be unlinked from g_bridges. Do NOT hold
// g_mutex across this call — it invokes N-API. Callers that freed a bridge
// *early* (destroyEngine / streaming completion) must first drop the env cleanup
// hook via napi_remove_env_cleanup_hook so Node never invokes it on freed memory;
// the hook path itself (bridge_env_cleanup) must not remove itself and calls this
// directly.
static void bridge_finalize(engine_bridge_t* b) {
    if (b == NULL) return;
    if (b->resolver_js != NULL && b->env != NULL) {
        napi_delete_reference(b->env, b->resolver_js);
    }
    resolver_results_free_all(b);
    free(b);
}

// Env cleanup hook (F2): registered per resolver-backed bridge at creation via
// napi_add_env_cleanup_hook, so each Worker/main env disposes its OWN bridges on
// its OWN thread when that env tears down — instead of napi_cleanup deleting
// refs from whichever thread happens to release the last DataWeave instance,
// which is undefined behavior for thread-affine napi_env/napi_ref. Runs on the
// owner thread with the env still alive, which is exactly where napi_ref deletion
// is legal.
static void bridge_env_cleanup(void* arg) {
    engine_bridge_t* b = (engine_bridge_t*)arg;
    if (b == NULL) return;

    uv_mutex_lock(&g_mutex);
    // Unlink from g_bridges if still present (destroyEngine may have already
    // unlinked it while deferring a free — see below).
    engine_bridge_t** pp = &g_bridges;
    while (*pp != NULL) {
        if (*pp == b) { *pp = b->next; break; }
        pp = &(*pp)->next;
    }
    // An in-flight streaming/transform op holds a live threadsafe function that
    // keeps this env's event loop alive, so the env should never tear down while
    // in_flight > 0. Guard defensively anyway: mark destroy_pending and let the
    // op's completion path drain and finalize it (do NOT finalize here, the op's
    // background thread could still dereference this bridge).
    if (b->in_flight > 0) {
        b->destroy_pending = true;
        uv_mutex_unlock(&g_mutex);
        return;
    }
    uv_mutex_unlock(&g_mutex);

    // We are inside Node's invocation of this hook, so we must not (and need not)
    // call napi_remove_env_cleanup_hook for ourselves here.
    bridge_finalize(b);
}

// Begin a streaming/transform op on a resolver-backed engine: look up the bridge
// and mark one op in flight so it (and its napi_ref) cannot be freed while the
// background uv_thread can still call resolve_module_callback with it (F1).
// Returns the bridge pointer (stable for the op's lifetime, since in_flight > 0
// blocks both destroyEngine and the env cleanup hook from freeing it) or NULL for
// a resolver-less engine / unknown handle, in which case there is nothing to
// protect and completion must not call bridge_end_op.
static engine_bridge_t* bridge_begin_op(long long handle) {
    uv_mutex_lock(&g_mutex);
    engine_bridge_t* b = bridge_find(handle);
    if (b != NULL) b->in_flight++;
    uv_mutex_unlock(&g_mutex);
    return b;
}

// End a streaming/transform op. Runs on the owner (JS) thread from the completion
// sentinel. If destroyEngine (or the env cleanup hook) ran while this op was in
// flight, it deferred the free — already unlinked from g_bridges — so the last op
// to drain finalizes the bridge here, on the legal (owner) thread.
static void bridge_end_op(engine_bridge_t* b) {
    if (b == NULL) return;
    uv_mutex_lock(&g_mutex);
    b->in_flight--;
    bool finalize = (b->destroy_pending && b->in_flight == 0);
    uv_mutex_unlock(&g_mutex);
    if (finalize) bridge_finalize(b);
}

// --- Initialization ---

struct init_args {
  const char* lib_path;
  int result;
  char error[512];
};

static void init_thread_fn(void* arg) {
  struct init_args* args = (struct init_args*)arg;

  int rc = uv_dlopen(args->lib_path, &g_lib);
  if (rc != 0) {
    snprintf(args->error, sizeof(args->error), "Failed to load library: %s", uv_dlerror(&g_lib));
    args->result = -1;
    return;
  }
  g_lib_loaded = 1;

  uv_dlsym(&g_lib, "graal_create_isolate", (void**)&fn_create_isolate);
  uv_dlsym(&g_lib, "graal_attach_thread", (void**)&fn_attach_thread);
  uv_dlsym(&g_lib, "graal_detach_thread", (void**)&fn_detach_thread);
  uv_dlsym(&g_lib, "graal_tear_down_isolate", (void**)&fn_tear_down_isolate);
  uv_dlsym(&g_lib, "run_script", (void**)&fn_run_script);
  uv_dlsym(&g_lib, "free_cstring", (void**)&fn_free_cstring);
  uv_dlsym(&g_lib, "run_script_callback", (void**)&fn_run_script_callback);
  uv_dlsym(&g_lib, "run_script_input_output_callback", (void**)&fn_run_script_input_output_callback);

  // Load per-engine entrypoints. Every initialize() call creates an engine via
  // create_engine/create_engine_with_resolver (see dataweave.ts), so these are
  // load-time required, not optional, even though they are newer than the
  // legacy singleton symbols above.
  uv_dlsym(&g_lib, "create_engine", (void**)&fn_create_engine);
  uv_dlsym(&g_lib, "create_engine_with_resolver", (void**)&fn_create_engine_with_resolver);
  uv_dlsym(&g_lib, "destroy_engine", (void**)&fn_destroy_engine);
  uv_dlsym(&g_lib, "run_script_engine", (void**)&fn_run_script_engine);
  uv_dlsym(&g_lib, "run_script_callback_engine", (void**)&fn_run_script_callback_engine);
  uv_dlsym(&g_lib, "run_script_input_output_callback_engine", (void**)&fn_run_script_input_output_callback_engine);

  if (!fn_create_isolate || !fn_run_script || !fn_free_cstring) {
    snprintf(args->error, sizeof(args->error), "Missing required symbols in library");
    args->result = -2;
    return;
  }

  // Fail fast, with a clear message, if the loaded dwlib predates the
  // per-engine ABI (W-23692110). Without this check, the library would load
  // "successfully" here and every initialize() call would still fail later
  // deep inside createEngine()/createEngineWithResolver() with a confusing
  // "not available in native library" error instead of this one.
  if (!fn_create_engine || !fn_create_engine_with_resolver || !fn_destroy_engine ||
      !fn_run_script_engine || !fn_run_script_callback_engine ||
      !fn_run_script_input_output_callback_engine) {
    snprintf(args->error, sizeof(args->error),
             "dwlib is missing required per-engine symbols (expected in dwlib "
             "built with W-23692110 or later) - rebuild/upgrade the native library");
    args->result = -2;
    return;
  }

  void* boot_thread = NULL;
  rc = fn_create_isolate(NULL, &g_isolate, &boot_thread);
  if (rc != 0) {
    snprintf(args->error, sizeof(args->error), "graal_create_isolate failed with code %d", rc);
    args->result = rc;
    return;
  }

  // Detach the bootstrap thread immediately. This init OS thread is joined and
  // exits right after, so leaving it attached would leave a phantom attached
  // thread on the isolate — and graal_tear_down_isolate() blocks forever waiting
  // for every other attached thread to reach a safepoint (the dead init thread
  // never will). Subsequent calls (run/streaming/transform, and cleanup) attach
  // their own OS thread on demand and detach when done. Mirrors the Go binding,
  // which likewise detaches the bootstrap thread after graal_create_isolate.
  if (fn_detach_thread) {
    fn_detach_thread(boot_thread);
  }
  g_thread = NULL;

  args->result = 0;
}

static napi_value napi_initialize(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  if (argc < 1) {
    napi_throw_error(env, NULL, "initialize requires a library path argument");
    return NULL;
  }

  char lib_path[4096];
  size_t len;
  napi_get_value_string_utf8(env, argv[0], lib_path, sizeof(lib_path), &len);

  uv_mutex_lock(&g_mutex);

  // If a teardown from a prior cleanup() is still draining (the isolate is
  // being torn down on the waiter thread from Task 2), do not race a fresh
  // graal_create_isolate against it -- wait until the isolate is fully gone
  // (g_teardown_pending false AND g_isolate NULL) before proceeding. This is
  // a narrow, rare path (re-initializing mid-drain), not a fast path, so a
  // blocking wait here is acceptable and matches this function's existing
  // fully-synchronous contract.
  while (g_teardown_pending || (g_isolate != NULL && !g_initialized)) {
    uv_cond_wait(&g_teardown_cond, &g_mutex);
  }

  if (g_initialized) {
    g_ref_count++;
    uv_mutex_unlock(&g_mutex);
    return NULL;
  }

  struct init_args args;
  args.lib_path = lib_path;
  args.result = -1;
  args.error[0] = '\0';

  uv_thread_t tid;
  uv_thread_options_t opts;
  opts.flags = UV_THREAD_HAS_STACK_SIZE;
  opts.stack_size = 16 * 1024 * 1024;
  int spawn_rc = uv_thread_create_ex(&tid, &opts, init_thread_fn, &args);
  if (spawn_rc != 0) {
    uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "Failed to spawn initialization thread");
    return NULL;
  }
  uv_thread_join(&tid);

  if (args.result != 0) {
    uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, args.error[0] ? args.error : "Initialization failed");
    return NULL;
  }

  g_initialized = 1;
  g_ref_count++;
  uv_mutex_unlock(&g_mutex);
  return NULL;
}

// --- Helper: run any GraalVM call on a dedicated thread ---

struct script_call_args {
  const char* script;
  const char* inputs_json;
  char* result;
};

static void run_script_thread_fn(void* arg) {
  struct script_call_args* a = (struct script_call_args*)arg;

  void* thread = NULL;
  int rc = fn_attach_thread(g_isolate, &thread);
  if (rc != 0) {
    a->result = strdup("{\"success\":false,\"error\":\"Failed to attach GraalVM thread\"}");
    return;
  }

  void* ptr = fn_run_script(thread, a->script, a->inputs_json);
  if (ptr) {
    a->result = strdup((const char*)ptr);
    fn_free_cstring(thread, ptr);
  } else {
    a->result = strdup("");
  }

  fn_detach_thread(thread);
}

// --- runScript (synchronous from JS, but runs GraalVM on a thread) ---

static napi_value dw_napi_run_script(napi_env env, napi_callback_info info) {
  if (!g_initialized) {
    napi_throw_error(env, NULL, "Not initialized. Call initialize() first.");
    return NULL;
  }

  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  if (argc < 2) {
    napi_throw_error(env, NULL, "runScript requires (script, inputsJson)");
    return NULL;
  }

  size_t script_len, inputs_len;
  napi_get_value_string_utf8(env, argv[0], NULL, 0, &script_len);
  napi_get_value_string_utf8(env, argv[1], NULL, 0, &inputs_len);

  char* script = malloc(script_len + 1);
  char* inputs = malloc(inputs_len + 1);
  napi_get_value_string_utf8(env, argv[0], script, script_len + 1, NULL);
  napi_get_value_string_utf8(env, argv[1], inputs, inputs_len + 1, NULL);

  struct script_call_args call_args;
  call_args.script = script;
  call_args.inputs_json = inputs;
  call_args.result = NULL;

  uv_thread_t tid;
  uv_thread_options_t opts;
  opts.flags = UV_THREAD_HAS_STACK_SIZE;
  opts.stack_size = 2 * 1024 * 1024;
  int spawn_rc = uv_thread_create_ex(&tid, &opts, run_script_thread_fn, &call_args);
  if (spawn_rc != 0) {
    free(script);
    free(inputs);
    napi_throw_error(env, NULL, "Failed to spawn script execution thread");
    return NULL;
  }
  uv_thread_join(&tid);

  free(script);
  free(inputs);

  napi_value result;
  if (call_args.result) {
    napi_create_string_utf8(env, call_args.result, strlen(call_args.result), &result);
    free(call_args.result);
  } else {
    napi_create_string_utf8(env, "", 0, &result);
  }
  return result;
}

// --- Streaming output ---

// chunk_data with len == -1 is a sentinel indicating completion (buf holds meta JSON)
struct chunk_data {
  char* buf;
  int len;
};

struct streaming_work {
  uv_thread_t tid;
  napi_threadsafe_function tsfn;
  napi_deferred deferred;
  long long handle;
  char* script;
  char* inputs_json;
  // Non-NULL only for resolver-backed engines: the bridge whose in_flight count
  // this op holds. The completion sentinel calls bridge_end_op on it (F1).
  engine_bridge_t* bridge;
};

static void call_js_write(napi_env env, napi_value js_callback, void* context, void* data) {
  // data == NULL: nothing was queued, nothing to free or finalize.
  if (data == NULL) return;
  struct chunk_data* chunk = (struct chunk_data*)data;
  struct streaming_work* w = (struct streaming_work*)context;

  if (chunk->len == -1) {
    // Completion sentinel. env == NULL means the environment is tearing down
    // (e.g. a Worker terminating mid-op): we must not call any napi value or
    // JS-calling API (napi_create_string_utf8/napi_resolve_deferred need a
    // live env), but we must still perform every bit of native finalization
    // -- join the worker, release the tsfn, drop the bridge in-flight hold,
    // and free every heap field -- exactly once. Skipping this on env == NULL
    // would leak `w` and could strand a bridge marked for deferred destruction
    // indefinitely.
    if (env != NULL) {
      napi_value result;
      napi_create_string_utf8(env, chunk->buf, strlen(chunk->buf), &result);
      napi_resolve_deferred(env, w->deferred, result);
    }

    free(chunk->buf);
    free(chunk);
    free(w->script);
    free(w->inputs_json);

    uv_thread_join(&w->tid);
    napi_release_threadsafe_function(w->tsfn, napi_tsfn_release);
    // Drop the in-flight hold last, on this owner thread: if destroyEngine ran
    // during the op it deferred the free to here (F1). After this the bridge may
    // be freed, so touch nothing on it afterward.
    bridge_end_op(w->bridge);
    free(w);
    return;
  }

  // Non-sentinel data chunk. If env == NULL the environment is gone and we
  // cannot deliver it to JS; free it and return without touching `w` (its
  // finalization happens only on the sentinel, above).
  if (env == NULL) {
    free(chunk->buf);
    free(chunk);
    return;
  }

  napi_value buffer;
  void* buf_data;
  napi_create_buffer_copy(env, chunk->len, chunk->buf, &buf_data, &buffer);

  napi_value global;
  napi_get_global(env, &global);
  napi_call_function(env, global, js_callback, 1, &buffer, NULL);

  free(chunk->buf);
  free(chunk);
}

static int streaming_write_cb(void* ctx, const char* buf, int len) {
  napi_threadsafe_function tsfn = (napi_threadsafe_function)ctx;
  struct chunk_data* chunk = malloc(sizeof(struct chunk_data));
  chunk->buf = malloc(len);
  memcpy(chunk->buf, buf, len);
  chunk->len = len;

  napi_status status = napi_call_threadsafe_function(tsfn, chunk, napi_tsfn_blocking);
  if (status != napi_ok) {
    free(chunk->buf);
    free(chunk);
    return -1;
  }
  return 0;
}

static void streaming_thread_fn(void* arg) {
  struct streaming_work* w = (struct streaming_work*)arg;

  void* worker_thread = NULL;
  int rc = fn_attach_thread(g_isolate, &worker_thread);

  char* meta_result = NULL;
  if (rc != 0) {
    char err[256];
    snprintf(err, sizeof(err), "{\"success\":false,\"error\":\"Failed to attach thread (code %d)\"}", rc);
    meta_result = strdup(err);
  } else {
    void* result_ptr = fn_run_script_callback_engine(
      worker_thread, w->handle, w->script, w->inputs_json, streaming_write_cb, (void*)w->tsfn
    );
    if (result_ptr) {
      meta_result = strdup((const char*)result_ptr);
      fn_free_cstring(worker_thread, result_ptr);
    } else {
      meta_result = strdup("{\"success\":false,\"error\":\"Empty response\"}");
    }
    fn_detach_thread(worker_thread);
  }

  // Decrement here, once this thread has fully detached from the isolate --
  // not in call_js_write's completion branch. call_js_write only runs when
  // the JS thread's event loop turns, and napi_initialize's pending-teardown
  // wait (Task 3) can block that same event loop indefinitely; decrementing
  // from the JS-thread callback made the two waits circular. Decrementing
  // here ties g_active_ops to the actual invariant isolate teardown needs
  // (no GraalVM-attached thread remains), independent of the event loop.
  uv_mutex_lock(&g_mutex);
  g_active_ops--;
  uv_cond_broadcast(&g_teardown_cond);
  uv_mutex_unlock(&g_mutex);

  struct chunk_data* sentinel = malloc(sizeof(struct chunk_data));
  sentinel->buf = meta_result;
  sentinel->len = -1;
  napi_call_threadsafe_function(w->tsfn, sentinel, napi_tsfn_blocking);
}

static napi_value napi_run_script_streaming_engine(napi_env env, napi_callback_info info) {
  if (!g_initialized) {
    napi_throw_error(env, NULL, "Not initialized. Call initialize() first.");
    return NULL;
  }
  if (!fn_run_script_callback_engine) {
    napi_throw_error(env, NULL, "run_script_callback_engine not available in native library");
    return NULL;
  }

  size_t argc = 4;
  napi_value argv[4];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  if (argc < 4) {
    napi_throw_error(env, NULL, "runScriptStreamingEngine requires (handle, script, inputsJson, chunkCallback)");
    return NULL;
  }

  int64_t handle64;
  napi_get_value_int64(env, argv[0], &handle64);

  size_t script_len, inputs_len;
  napi_get_value_string_utf8(env, argv[1], NULL, 0, &script_len);
  napi_get_value_string_utf8(env, argv[2], NULL, 0, &inputs_len);

  struct streaming_work* w = calloc(1, sizeof(struct streaming_work));
  w->handle = (long long)handle64;
  w->script = malloc(script_len + 1);
  w->inputs_json = malloc(inputs_len + 1);
  napi_get_value_string_utf8(env, argv[1], w->script, script_len + 1, NULL);
  napi_get_value_string_utf8(env, argv[2], w->inputs_json, inputs_len + 1, NULL);

  napi_value resource_name;
  napi_create_string_utf8(env, "dwStreaming", NAPI_AUTO_LENGTH, &resource_name);
  napi_create_threadsafe_function(env, argv[3], NULL, resource_name, 0, 1, NULL, NULL, w, call_js_write, &w->tsfn);

  napi_value promise;
  napi_create_promise(env, &w->deferred, &promise);

  // Pin the resolver bridge (if any) for the whole op so it outlives a concurrent
  // destroyEngine/cleanup and the background thread can safely call back into
  // resolve_module_callback (F1). NULL for resolver-less engines. Must happen
  // before spawning the thread; the completion sentinel releases it via
  // bridge_end_op. No early return exists between here and the spawn.
  w->bridge = bridge_begin_op(w->handle);

  // Count this op globally so a concurrent cleanup() knows to wait for it
  // before tearing down the isolate (see g_active_ops comment above). Same
  // timing/invariant as bridge_begin_op: before spawning the worker thread,
  // no early return in between.
  uv_mutex_lock(&g_mutex);
  g_active_ops++;
  uv_mutex_unlock(&g_mutex);

  uv_thread_options_t opts;
  opts.flags = UV_THREAD_HAS_STACK_SIZE;
  opts.stack_size = 2 * 1024 * 1024;
  int spawn_rc = uv_thread_create_ex(&w->tid, &opts, streaming_thread_fn, w);

  if (spawn_rc != 0) {
    // The worker never ran, so nothing will ever decrement g_active_ops,
    // release the bridge hold, or resolve the promise -- unwind everything
    // committed above ourselves, in reverse order, mirroring call_js_write's
    // completion branch (minus uv_thread_join: there is no thread to join).
    uv_mutex_lock(&g_mutex);
    g_active_ops--;
    uv_cond_broadcast(&g_teardown_cond);
    uv_mutex_unlock(&g_mutex);

    bridge_end_op(w->bridge);
    napi_release_threadsafe_function(w->tsfn, napi_tsfn_release);

    napi_value result;
    napi_create_string_utf8(env, "{\"success\":false,\"error\":\"Failed to spawn streaming worker thread\"}", NAPI_AUTO_LENGTH, &result);
    napi_resolve_deferred(env, w->deferred, result);

    free(w->script);
    free(w->inputs_json);
    free(w);
  }

  return promise;
}

// --- Bidirectional streaming ---

struct transform_work {
  uv_thread_t tid;
  napi_threadsafe_function read_tsfn;
  napi_threadsafe_function write_tsfn;
  napi_deferred deferred;
  long long handle;
  char* script;
  char* inputs_json;
  char* input_name;
  char* input_mime_type;
  char* input_charset;
  // Non-NULL only for resolver-backed engines: the bridge whose in_flight count
  // this op holds. The completion sentinel calls bridge_end_op on it (F1).
  engine_bridge_t* bridge;
};

struct read_request {
  char* buffer;
  int buffer_size;
  int bytes_read;
  uv_mutex_t mutex;
  uv_cond_t cond;
  int ready;
};

static void call_js_read(napi_env env, napi_value js_callback, void* context, void* data) {
  if (data == NULL) return;  // nothing to signal
  struct read_request* req = (struct read_request*)data;

  if (env == NULL) {
    // N-API can invoke a threadsafe-function callback with env == NULL when
    // the environment is tearing down with items still queued (e.g. a Worker
    // terminating mid-transform). transform_read_cb is synchronously blocked
    // on req->cond waiting for this callback to signal it -- unlike
    // call_js_write/call_js_transform_write, there is no sentinel-driven path
    // that would otherwise unblock it. Treat this as a terminal read error so
    // the blocked thread wakes up, detects the failure via bytes_read == -1,
    // and the worker can detach from the isolate instead of hanging forever.
    req->bytes_read = -1;
  } else {
    napi_value buf_size_val;
    napi_create_int32(env, req->buffer_size, &buf_size_val);

    napi_value global;
    napi_get_global(env, &global);

    napi_value result;
    napi_status status = napi_call_function(env, global, js_callback, 1, &buf_size_val, &result);

    if (status == napi_ok && result != NULL) {
      bool is_buffer;
      napi_is_buffer(env, result, &is_buffer);
      if (is_buffer) {
        void* buf_data;
        size_t buf_len;
        napi_get_buffer_info(env, result, &buf_data, &buf_len);
        int n = (int)buf_len < req->buffer_size ? (int)buf_len : req->buffer_size;
        if (n > 0) memcpy(req->buffer, buf_data, n);
        req->bytes_read = n;
      } else {
        req->bytes_read = 0;
      }
    } else {
      // Clear pending exception to prevent propagation
      if (status == napi_pending_exception) {
        napi_value exception;
        napi_get_and_clear_last_exception(env, &exception);

        // Extract and log exception details before discarding
        napi_value message_prop, stack_prop;
        char message_buf[512] = {0};
        char stack_buf[2048] = {0};
        size_t message_len = 0, stack_len = 0;

        // Try to get the message property
        if (napi_get_named_property(env, exception, "message", &message_prop) == napi_ok) {
          napi_get_value_string_utf8(env, message_prop, message_buf, sizeof(message_buf), &message_len);
        }

        // Try to get the stack property
        if (napi_get_named_property(env, exception, "stack", &stack_prop) == napi_ok) {
          napi_get_value_string_utf8(env, stack_prop, stack_buf, sizeof(stack_buf), &stack_len);
        }

        // Log the exception to stderr for diagnostics
        fprintf(stderr, "[DataWeave Node addon] Read callback threw exception:\n");
        if (message_len > 0) {
          fprintf(stderr, "  Message: %s\n", message_buf);
        }
        if (stack_len > 0) {
          fprintf(stderr, "  Stack:\n%s\n", stack_buf);
        }
        if (message_len == 0 && stack_len == 0) {
          fprintf(stderr, "  (Unable to extract exception details)\n");
        }
      }
      req->bytes_read = -1;  // Signal error
    }
  }

  uv_mutex_lock(&req->mutex);
  req->ready = 1;
  uv_cond_signal(&req->cond);
  uv_mutex_unlock(&req->mutex);
}

static int transform_read_cb(void* ctx, char* buf, int buf_size) {
  struct transform_work* w = (struct transform_work*)ctx;

  struct read_request req;
  req.buffer = buf;
  req.buffer_size = buf_size;
  req.bytes_read = 0;
  req.ready = 0;
  uv_mutex_init(&req.mutex);
  uv_cond_init(&req.cond);

  napi_status status = napi_call_threadsafe_function(w->read_tsfn, &req, napi_tsfn_blocking);
  if (status != napi_ok) {
    uv_mutex_destroy(&req.mutex);
    uv_cond_destroy(&req.cond);
    return -1;
  }

  uv_mutex_lock(&req.mutex);
  while (!req.ready) {
    uv_cond_wait(&req.cond, &req.mutex);
  }
  uv_mutex_unlock(&req.mutex);

  int n = req.bytes_read;
  uv_mutex_destroy(&req.mutex);
  uv_cond_destroy(&req.cond);
  return n;
}

static int transform_write_cb(void* ctx, const char* buf, int len) {
  struct transform_work* w = (struct transform_work*)ctx;
  struct chunk_data* chunk = malloc(sizeof(struct chunk_data));
  chunk->buf = malloc(len);
  memcpy(chunk->buf, buf, len);
  chunk->len = len;

  napi_status status = napi_call_threadsafe_function(w->write_tsfn, chunk, napi_tsfn_blocking);
  if (status != napi_ok) {
    free(chunk->buf);
    free(chunk);
    return -1;
  }
  return 0;
}

static void call_js_transform_write(napi_env env, napi_value js_callback, void* context, void* data) {
  // data == NULL: nothing was queued, nothing to free or finalize.
  if (data == NULL) return;
  struct chunk_data* chunk = (struct chunk_data*)data;
  struct transform_work* w = (struct transform_work*)context;

  if (chunk->len == -1) {
    // Completion sentinel. env == NULL means the environment is tearing down
    // (e.g. a Worker terminating mid-op): we must not call any napi value or
    // JS-calling API (napi_create_string_utf8/napi_resolve_deferred need a
    // live env), but we must still perform every bit of native finalization
    // -- join the worker, release both tsfns, drop the bridge in-flight hold,
    // and free every heap field -- exactly once. Skipping this on env == NULL
    // would leak `w` and could strand a bridge marked for deferred destruction
    // indefinitely.
    if (env != NULL) {
      napi_value result;
      napi_create_string_utf8(env, chunk->buf, strlen(chunk->buf), &result);
      napi_resolve_deferred(env, w->deferred, result);
    }

    free(chunk->buf);
    free(chunk);
    free(w->script);
    free(w->inputs_json);
    free(w->input_name);
    free(w->input_mime_type);
    free(w->input_charset);

    uv_thread_join(&w->tid);
    napi_release_threadsafe_function(w->read_tsfn, napi_tsfn_release);
    napi_release_threadsafe_function(w->write_tsfn, napi_tsfn_release);
    // Drop the in-flight hold last, on this owner thread: if destroyEngine ran
    // during the op it deferred the free to here (F1). After this the bridge may
    // be freed, so touch nothing on it afterward.
    bridge_end_op(w->bridge);
    free(w);
    return;
  }

  // Non-sentinel data chunk. If env == NULL the environment is gone and we
  // cannot deliver it to JS; free it and return without touching `w` (its
  // finalization happens only on the sentinel, above).
  if (env == NULL) {
    free(chunk->buf);
    free(chunk);
    return;
  }

  napi_value buffer;
  void* buf_data;
  napi_create_buffer_copy(env, chunk->len, chunk->buf, &buf_data, &buffer);

  napi_value global;
  napi_get_global(env, &global);
  napi_call_function(env, global, js_callback, 1, &buffer, NULL);

  free(chunk->buf);
  free(chunk);
}

static void transform_thread_fn(void* arg) {
  struct transform_work* w = (struct transform_work*)arg;

  void* worker_thread = NULL;
  int rc = fn_attach_thread(g_isolate, &worker_thread);

  char* meta_result = NULL;
  if (rc != 0) {
    char err[256];
    snprintf(err, sizeof(err), "{\"success\":false,\"error\":\"Failed to attach thread (code %d)\"}", rc);
    meta_result = strdup(err);
  } else {
    void* result_ptr = fn_run_script_input_output_callback_engine(
      worker_thread, w->handle, w->script, w->inputs_json,
      w->input_name, w->input_mime_type, w->input_charset,
      transform_read_cb, transform_write_cb, (void*)w
    );

    if (result_ptr) {
      meta_result = strdup((const char*)result_ptr);
      fn_free_cstring(worker_thread, result_ptr);
    } else {
      meta_result = strdup("{\"success\":false,\"error\":\"Empty response\"}");
    }
    fn_detach_thread(worker_thread);
  }

  // See streaming_thread_fn's comment: decrement here (after detach), not in
  // call_js_transform_write's completion branch, to avoid the same
  // circular-wait deadlock against napi_initialize's pending-teardown wait.
  uv_mutex_lock(&g_mutex);
  g_active_ops--;
  uv_cond_broadcast(&g_teardown_cond);
  uv_mutex_unlock(&g_mutex);

  struct chunk_data* sentinel = malloc(sizeof(struct chunk_data));
  sentinel->buf = meta_result;
  sentinel->len = -1;
  napi_call_threadsafe_function(w->write_tsfn, sentinel, napi_tsfn_blocking);
}

static napi_value napi_run_script_transform_engine(napi_env env, napi_callback_info info) {
  if (!g_initialized) {
    napi_throw_error(env, NULL, "Not initialized. Call initialize() first.");
    return NULL;
  }
  if (!fn_run_script_input_output_callback_engine) {
    napi_throw_error(env, NULL, "run_script_input_output_callback_engine not available in native library");
    return NULL;
  }

  size_t argc = 8;
  napi_value argv[8];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  if (argc < 8) {
    napi_throw_error(env, NULL, "runScriptTransformEngine requires 8 arguments");
    return NULL;
  }

  struct transform_work* w = calloc(1, sizeof(struct transform_work));
  size_t len;

  int64_t handle64;
  napi_get_value_int64(env, argv[0], &handle64);
  w->handle = (long long)handle64;

  napi_get_value_string_utf8(env, argv[1], NULL, 0, &len);
  w->script = malloc(len + 1);
  napi_get_value_string_utf8(env, argv[1], w->script, len + 1, NULL);

  napi_get_value_string_utf8(env, argv[2], NULL, 0, &len);
  w->inputs_json = malloc(len + 1);
  napi_get_value_string_utf8(env, argv[2], w->inputs_json, len + 1, NULL);

  napi_get_value_string_utf8(env, argv[3], NULL, 0, &len);
  w->input_name = malloc(len + 1);
  napi_get_value_string_utf8(env, argv[3], w->input_name, len + 1, NULL);

  napi_get_value_string_utf8(env, argv[4], NULL, 0, &len);
  w->input_mime_type = malloc(len + 1);
  napi_get_value_string_utf8(env, argv[4], w->input_mime_type, len + 1, NULL);

  napi_valuetype type;
  napi_typeof(env, argv[5], &type);
  if (type == napi_string) {
    napi_get_value_string_utf8(env, argv[5], NULL, 0, &len);
    w->input_charset = malloc(len + 1);
    napi_get_value_string_utf8(env, argv[5], w->input_charset, len + 1, NULL);
  } else {
    w->input_charset = NULL;
  }

  napi_value resource_name;
  napi_create_string_utf8(env, "dwTransform", NAPI_AUTO_LENGTH, &resource_name);

  napi_create_threadsafe_function(env, argv[6], NULL, resource_name, 0, 1, NULL, NULL, NULL, call_js_read, &w->read_tsfn);
  napi_create_threadsafe_function(env, argv[7], NULL, resource_name, 0, 1, NULL, NULL, w, call_js_transform_write, &w->write_tsfn);

  napi_value promise;
  napi_create_promise(env, &w->deferred, &promise);

  // Pin the resolver bridge (if any) for the whole op so it outlives a concurrent
  // destroyEngine/cleanup and the background thread can safely call back into
  // resolve_module_callback (F1). NULL for resolver-less engines. Must happen
  // before spawning the thread; the completion sentinel releases it via
  // bridge_end_op. No early return exists between here and the spawn.
  w->bridge = bridge_begin_op(w->handle);

  // Count this op globally so a concurrent cleanup() knows to wait for it
  // before tearing down the isolate (see g_active_ops comment above). Same
  // timing/invariant as bridge_begin_op: before spawning the worker thread,
  // no early return in between.
  uv_mutex_lock(&g_mutex);
  g_active_ops++;
  uv_mutex_unlock(&g_mutex);

  uv_thread_options_t opts;
  opts.flags = UV_THREAD_HAS_STACK_SIZE;
  opts.stack_size = 2 * 1024 * 1024;
  int spawn_rc = uv_thread_create_ex(&w->tid, &opts, transform_thread_fn, w);

  if (spawn_rc != 0) {
    // The worker never ran, so nothing will ever decrement g_active_ops,
    // release the bridge hold, or resolve the promise -- unwind everything
    // committed above ourselves, in reverse order, mirroring
    // call_js_transform_write's completion branch (minus uv_thread_join:
    // there is no thread to join).
    uv_mutex_lock(&g_mutex);
    g_active_ops--;
    uv_cond_broadcast(&g_teardown_cond);
    uv_mutex_unlock(&g_mutex);

    bridge_end_op(w->bridge);
    napi_release_threadsafe_function(w->read_tsfn, napi_tsfn_release);
    napi_release_threadsafe_function(w->write_tsfn, napi_tsfn_release);

    napi_value result;
    napi_create_string_utf8(env, "{\"success\":false,\"error\":\"Failed to spawn transform worker thread\"}", NAPI_AUTO_LENGTH, &result);
    napi_resolve_deferred(env, w->deferred, result);

    free(w->script);
    free(w->inputs_json);
    free(w->input_name);
    free(w->input_mime_type);
    free(w->input_charset);
    free(w);
  }

  return promise;
}

// --- Resolver callback bridge ---

// Called by native code, synchronously, on the same JS thread that invoked
// runScriptEngine for a resolver-backed engine (see the comment on
// engine_bridge_t above for why this must NOT hop through
// napi_threadsafe_function). The ctx word is the engine's own engine_bridge_t*,
// passed to Java in create_engine_with_resolver and forwarded back here. Calls
// the JS resolver directly and returns its result copied onto the heap; the
// caller frees the tracked buffers after the native side has copied them.
static char* resolve_module_callback(void* thread, void* ctx, const char* module_path) {
    (void)thread;

    engine_bridge_t* bridge = (engine_bridge_t*)ctx;
    if (bridge == NULL || bridge->env == NULL || bridge->resolver_js == NULL) {
        return NULL;  // No resolver for this engine
    }

    // Guard against cross-thread napi calls. Streaming and transform execute
    // their native call on a background uv_thread (streaming_thread_fn/
    // transform_thread_fn), not the JS thread that created this bridge. If we're
    // not on the thread that owns this napi_env, calling napi_get_reference_value
    // or napi_call_function here is undefined behavior (typically a crash).
    // Fail closed instead: report "not found", which matches the documented
    // built-ins-only fallback for streaming/transform.
    uv_thread_t current = uv_thread_self();
    if (!uv_thread_equal(&current, &bridge->owner)) {
        return NULL;
    }

    napi_env env = bridge->env;

    napi_value js_callback;
    if (napi_get_reference_value(env, bridge->resolver_js, &js_callback) != napi_ok) {
        return NULL;
    }

    // NameIdentifierHelper.toWeaveFilePath (Java side, via CallbackWeaveResourceResolver)
    // always renders paths with a leading separator, e.g. "/org/test/lib.dwl". Every
    // resolver factory in resolver.ts (modulesFromMap, modulesFromDirectory, ...) and
    // their documented examples key/join on the separator-less form ("org/test/lib.dwl"),
    // so strip exactly one leading '/' here before handing the path to JS.
    const char* js_module_path = module_path;
    if (js_module_path[0] == '/') {
        js_module_path++;
    }

    napi_value module_path_str;
    if (napi_create_string_utf8(env, js_module_path, NAPI_AUTO_LENGTH, &module_path_str) != napi_ok) {
        return NULL;
    }

    napi_value undefined, result;
    napi_get_undefined(env, &undefined);
    napi_status status = napi_call_function(env, undefined, js_callback, 1, &module_path_str, &result);
    if (status != napi_ok) {
        // JS resolver threw — clear the pending exception so it doesn't leak
        // into the next napi call, extract and log its message/stack for
        // diagnostics (same pattern as the read-callback bridge above), and
        // report "not found".
        if (status == napi_pending_exception) {
            napi_value exception;
            napi_get_and_clear_last_exception(env, &exception);

            // The resolver is user-provided code; its exception message/stack
            // can carry module source, file paths, credentials, or other
            // tenant data. Logging that to stderr by default risks leaking it
            // into aggregated log systems. Only log a fixed, content-free
            // diagnostic unless the caller has opted in via
            // DATAWEAVE_RESOLVER_DEBUG=1 (checked once and cached, since
            // getenv() is not safe to call from arbitrary threads on all
            // platforms and this callback can run off the JS thread).
            static int debug_checked = 0;
            static int debug_enabled = 0;
            if (!debug_checked) {
                const char* debug_env = getenv("DATAWEAVE_RESOLVER_DEBUG");
                debug_enabled = (debug_env != NULL && strcmp(debug_env, "1") == 0);
                debug_checked = 1;
            }

            if (!debug_enabled) {
                fprintf(stderr,
                    "[DataWeave Node addon] Resolver callback threw an exception "
                    "(details suppressed; set DATAWEAVE_RESOLVER_DEBUG=1 to log "
                    "message/stack — may expose resolver-controlled data).\n");
            } else {
                napi_value message_prop, stack_prop;
                char message_buf[512] = {0};
                char stack_buf[2048] = {0};
                size_t message_len = 0, stack_len = 0;

                if (napi_get_named_property(env, exception, "message", &message_prop) == napi_ok) {
                    napi_get_value_string_utf8(env, message_prop, message_buf, sizeof(message_buf), &message_len);
                }

                if (napi_get_named_property(env, exception, "stack", &stack_prop) == napi_ok) {
                    napi_get_value_string_utf8(env, stack_prop, stack_buf, sizeof(stack_buf), &stack_len);
                }

                fprintf(stderr, "[DataWeave Node addon] Resolver callback threw exception:\n");
                if (message_len > 0) {
                    fprintf(stderr, "  Message: %s\n", message_buf);
                }
                if (stack_len > 0) {
                    fprintf(stderr, "  Stack:\n%s\n", stack_buf);
                }
                if (message_len == 0 && stack_len == 0) {
                    fprintf(stderr, "  (Unable to extract exception details)\n");
                }
            }
        } else {
            fprintf(stderr, "Resolver callback threw exception\n");
        }
        return NULL;
    }

    napi_valuetype result_type;
    napi_typeof(env, result, &result_type);

    char* result_source = NULL;
    if (result_type == napi_string) {
        size_t len;
        napi_get_value_string_utf8(env, result, NULL, 0, &len);
        result_source = (char*)malloc(len + 1);
        if (result_source != NULL) {
            napi_get_value_string_utf8(env, result, result_source, len + 1, NULL);
        }
    }
    // null/undefined/other → not found (result_source stays NULL)

    if (!resolver_results_track(bridge, result_source)) {
        // Tracking-node allocation failed (OOM): result_source would otherwise
        // be an untracked buffer that nothing ever frees. Free it here and
        // report "unresolved" instead of leaking it.
        free(result_source);
        return NULL;
    }
    return result_source;  // Native copies this immediately; we free the original after the call.
}

// --- Per-engine N-API methods ---

// createEngine() -> number
static napi_value napi_create_engine(napi_env env, napi_callback_info info) {
    (void)info;
    if (!g_initialized) { napi_throw_error(env, NULL, "Not initialized. Call initialize() first."); return NULL; }
    if (!fn_create_engine) { napi_throw_error(env, NULL, "create_engine not available in native library"); return NULL; }
    void* thread = NULL;
    if (fn_attach_thread(g_isolate, &thread) != 0) { napi_throw_error(env, NULL, "Failed to attach thread"); return NULL; }
    long long handle = fn_create_engine(thread);
    fn_detach_thread(thread);
    // A GraalVM @CEntryPoint that throws on the Java side returns the return
    // type's default value instead of propagating the exception — 0 for a
    // long long. The real handle registry only ever hands out handles >= 1, so
    // any handle <= 0 means construction failed; never hand that back to JS as
    // if it were usable.
    if (handle <= 0) { napi_throw_error(env, NULL, "create_engine returned an invalid handle"); return NULL; }
    napi_value out; napi_create_int64(env, (int64_t)handle, &out); return out;
}

// createEngineWithResolver(resolver) -> number
static napi_value napi_create_engine_with_resolver(napi_env env, napi_callback_info info) {
    if (!g_initialized) { napi_throw_error(env, NULL, "Not initialized. Call initialize() first."); return NULL; }
    if (!fn_create_engine_with_resolver) { napi_throw_error(env, NULL, "create_engine_with_resolver not available in native library"); return NULL; }
    size_t argc = 1; napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    if (argc < 1) { napi_throw_error(env, NULL, "createEngineWithResolver requires (resolverCallback)"); return NULL; }

    engine_bridge_t* bridge = (engine_bridge_t*)calloc(1, sizeof(engine_bridge_t));
    if (bridge == NULL) { napi_throw_error(env, NULL, "Failed to allocate engine bridge"); return NULL; }
    if (napi_create_reference(env, argv[0], 1, &bridge->resolver_js) != napi_ok) {
        free(bridge); napi_throw_error(env, NULL, "Failed to reference resolver callback"); return NULL;
    }
    bridge->env = env; bridge->owner = uv_thread_self(); bridge->results = NULL;

    void* thread = NULL;
    if (fn_attach_thread(g_isolate, &thread) != 0) {
        napi_delete_reference(env, bridge->resolver_js); free(bridge);
        napi_throw_error(env, NULL, "Failed to attach thread"); return NULL;
    }
    long long handle = fn_create_engine_with_resolver(thread, resolve_module_callback, (void*)bridge);
    fn_detach_thread(thread);

    // Same invalid-handle guard as napi_create_engine: a Java-side construction
    // failure surfaces here as handle == 0 (GraalVM @CEntryPoint default-value
    // semantics), and any handle <= 0 is never valid. Reject before this bridge
    // is linked into g_bridges or a cleanup hook is registered for it — at this
    // point neither has happened, so there's nothing to unlink/unhook. Still use
    // bridge_finalize (not a manual napi_delete_reference+free) because the failed
    // construction may have called resolve_module_callback (e.g. during eager
    // module setup) before ultimately failing, which can have already populated
    // bridge->results via resolver_results_track; bridge_finalize frees those
    // tracked buffers too, so nothing is dropped on the floor.
    if (handle <= 0) {
        bridge_finalize(bridge);
        napi_throw_error(env, NULL, "create_engine_with_resolver returned an invalid handle");
        return NULL;
    }

    bridge->handle = handle;
    uv_mutex_lock(&g_mutex); bridge->next = g_bridges; g_bridges = bridge; uv_mutex_unlock(&g_mutex);
    // Register a per-env cleanup hook so THIS Worker/main thread disposes this
    // bridge's napi_ref on its own thread when its env tears down (F2). napi_cleanup
    // no longer touches bridge refs. destroyEngine removes this hook before an
    // early free so Node never calls it on freed memory.
    napi_add_env_cleanup_hook(env, bridge_env_cleanup, bridge);
    napi_value out; napi_create_int64(env, (int64_t)handle, &out); return out;
}

// destroyEngine(handle) -> void
static napi_value napi_destroy_engine(napi_env env, napi_callback_info info) {
    if (!g_initialized) return NULL;
    size_t argc = 1; napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    if (argc < 1) { napi_throw_error(env, NULL, "destroyEngine requires (handle)"); return NULL; }
    int64_t handle64; napi_get_value_int64(env, argv[0], &handle64);
    long long handle = (long long)handle64;

    if (fn_destroy_engine) {
        void* thread = NULL;
        if (fn_attach_thread(g_isolate, &thread) == 0) { fn_destroy_engine(thread, handle); fn_detach_thread(thread); }
    }
    // Unlink the bridge from g_bridges, but only free it now if no streaming/
    // transform op is still in flight. A background op can still call back into
    // resolve_module_callback with this bridge as ctx (F1), so if in_flight > 0
    // we mark destroy_pending and defer the free to the completion sentinel,
    // which drains on this same owner thread. Deleting the napi_ref is only legal
    // on the owner thread, and destroyEngine is called from it, so we finalize
    // here in the common (not-in-flight) case.
    uv_mutex_lock(&g_mutex);
    engine_bridge_t** pp = &g_bridges; engine_bridge_t* found = NULL;
    while (*pp != NULL) { if ((*pp)->handle == handle) { found = *pp; *pp = found->next; break; } pp = &(*pp)->next; }
    bool defer = false;
    if (found != NULL) {
        if (found->in_flight > 0) { found->destroy_pending = true; defer = true; }
    }
    uv_mutex_unlock(&g_mutex);
    if (found != NULL) {
        // Drop the env cleanup hook: whether we finalize now or defer to the
        // draining op, the free happens explicitly, so Node must never invoke
        // the hook on this (soon-to-be or already) freed bridge.
        napi_remove_env_cleanup_hook(env, bridge_env_cleanup, found);
        if (!defer) bridge_finalize(found);
    }
    return NULL;
}

// runScriptEngine(handle, script, inputsJson) -> string
static napi_value napi_run_script_engine(napi_env env, napi_callback_info info) {
    if (!g_initialized) { napi_throw_error(env, NULL, "Not initialized. Call initialize() first."); return NULL; }
    if (!fn_run_script_engine) { napi_throw_error(env, NULL, "run_script_engine not available in native library"); return NULL; }
    size_t argc = 3; napi_value argv[3];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    if (argc < 3) { napi_throw_error(env, NULL, "runScriptEngine requires (handle, script, inputsJson)"); return NULL; }
    int64_t handle64; napi_get_value_int64(env, argv[0], &handle64);
    long long handle = (long long)handle64;

    size_t script_len, inputs_len;
    napi_get_value_string_utf8(env, argv[1], NULL, 0, &script_len);
    napi_get_value_string_utf8(env, argv[2], NULL, 0, &inputs_len);
    char* script = (char*)malloc(script_len + 1);
    char* inputs = (char*)malloc(inputs_len + 1);
    if (script == NULL || inputs == NULL) { free(script); free(inputs); napi_throw_error(env, NULL, "OOM"); return NULL; }
    napi_get_value_string_utf8(env, argv[1], script, script_len + 1, NULL);
    napi_get_value_string_utf8(env, argv[2], inputs, inputs_len + 1, NULL);

    void* thread = NULL;
    if (fn_attach_thread(g_isolate, &thread) != 0) { free(script); free(inputs); napi_throw_error(env, NULL, "Failed to attach thread"); return NULL; }

    char* result = (char*)fn_run_script_engine(thread, handle, script, inputs);

    uv_mutex_lock(&g_mutex);
    engine_bridge_t* bridge = bridge_find(handle);
    uv_mutex_unlock(&g_mutex);
    if (bridge != NULL) resolver_results_free_all(bridge);

    char* result_copy = result ? strdup(result) : NULL;
    if (result != NULL) fn_free_cstring(thread, result);
    fn_detach_thread(thread);
    free(script); free(inputs);

    napi_value out;
    if (result_copy) { napi_create_string_utf8(env, result_copy, NAPI_AUTO_LENGTH, &out); free(result_copy); }
    else { napi_create_string_utf8(env, "", 0, &out); }
    return out;
}

// --- Cleanup (must run on a separate thread to avoid V8 signal handler conflict) ---

// Called on each waiter's own env/thread (via its own napi_threadsafe_function)
// once the waiter thread has finished isolate teardown. Resolves that specific
// caller's promise, then releases its tsfn and frees the node. `data` is
// unused (NULL) -- there is nothing to report beyond "done".
//
// napi_call_threadsafe_function(..., napi_tsfn_blocking) only ENQUEUES this
// callback for the target env's event loop to run later; it does not wait for
// it to actually execute. So the waiter node and its tsfn must stay alive
// until this callback runs and must be released/freed HERE, not by the
// thread that enqueued the call (teardown_waiter_thread_fn) -- freeing there
// right after the enqueueing call would be a use-after-free once this
// callback later dereferences `context`. Same ownership pattern as
// call_js_write/call_js_transform_write freeing their own work struct from
// inside their own completion branch.
static void call_js_teardown_done(napi_env env, napi_value js_callback, void* context, void* data) {
  (void)js_callback;
  (void)data;
  teardown_waiter_t* waiter = (teardown_waiter_t*)context;
  if (waiter == NULL) return;

  if (env != NULL) {
    napi_value undefined;
    napi_get_undefined(env, &undefined);
    napi_resolve_deferred(env, waiter->deferred, undefined);
  }

  napi_release_threadsafe_function(waiter->tsfn, napi_tsfn_release);
  free(waiter);
}

static void cleanup_thread_fn(void* arg) {
  (void)arg;
  // graal_tear_down_isolate() must be passed the IsolateThread belonging to the
  // *calling* OS thread. g_thread was created by graal_create_isolate() on the
  // (now-exited, already-joined) init thread, so it is invalid here — passing it
  // trips GraalVM's "wrong IsolateThread" guard and aborts with a fatal
  // StackOverflowError during teardown. Attach this cleanup thread to the isolate
  // to obtain a valid local IsolateThread, then tear down with that.
  if (!fn_tear_down_isolate || !fn_attach_thread || !g_isolate) {
    return;
  }
  void* local_thread = NULL;
  if (fn_attach_thread(g_isolate, &local_thread) != 0 || local_thread == NULL) {
    return;
  }
  fn_tear_down_isolate(local_thread);
}

// Spawned only when napi_cleanup finds g_active_ops > 0 on the last release
// (case 5 in the design doc). Blocks until every active streaming/transform
// op has drained, performs isolate teardown exactly like cleanup_thread_fn
// does on the unchanged fast path, then resolves every caller who is waiting
// on this same teardown (there may be more than one -- see g_teardown_waiters).
static void teardown_waiter_thread_fn(void* arg) {
  (void)arg;

  uv_mutex_lock(&g_mutex);
  while (g_active_ops > 0) {
    uv_cond_wait(&g_teardown_cond, &g_mutex);
  }
  uv_mutex_unlock(&g_mutex);

  // Perform teardown exactly as the unchanged fast path does: attach a local
  // thread to the isolate (g_thread from graal_create_isolate's bootstrap
  // thread is invalid here -- see cleanup_thread_fn's comment), then tear
  // down. Ignore the return code, matching today's behavior.
  if (fn_tear_down_isolate && fn_attach_thread && g_isolate) {
    void* local_thread = NULL;
    if (fn_attach_thread(g_isolate, &local_thread) == 0 && local_thread != NULL) {
      fn_tear_down_isolate(local_thread);
    }
  }

  uv_mutex_lock(&g_mutex);
  g_thread = NULL;
  g_isolate = NULL;
  g_initialized = 0;
  g_ref_count = 0;
  g_teardown_pending = false;
  // Release any initialize() call blocked waiting for teardown to finish
  // (see Task 3).
  uv_cond_broadcast(&g_teardown_cond);
  teardown_waiter_t* waiters = g_teardown_waiters;
  g_teardown_waiters = NULL;
  uv_mutex_unlock(&g_mutex);

  // Resolve every waiting caller's promise on its own env/thread via its own
  // tsfn -- napi_deferred/napi_env are thread-affine, so this cannot be done
  // from this waiter thread directly. napi_call_threadsafe_function only
  // ENQUEUES the call for the target thread to run later; it does not wait
  // for call_js_teardown_done to execute. So do NOT free/release here --
  // call_js_teardown_done owns and releases each node after it actually runs
  // (freeing it here instead would be a use-after-free the moment the
  // enqueued callback later dereferences it).
  while (waiters != NULL) {
    teardown_waiter_t* next = waiters->next;
    napi_call_threadsafe_function(waiters->tsfn, waiters, napi_tsfn_blocking);
    waiters = next;
  }
}

// Creates a promise, a threadsafe function bound to call_js_teardown_done for
// THIS call's env, and a teardown_waiter_t node carrying both. The node is
// NOT linked into g_teardown_waiters here -- the caller does that under
// g_mutex, since callers append at two different points in napi_cleanup
// (case 3: joining an existing pending teardown; case 5: starting a new one).
// Returns NULL (and throws) if node allocation fails.
static teardown_waiter_t* teardown_waiter_create(napi_env env, napi_value* out_promise) {
  teardown_waiter_t* waiter = (teardown_waiter_t*)calloc(1, sizeof(teardown_waiter_t));
  if (waiter == NULL) {
    napi_throw_error(env, NULL, "Failed to allocate teardown waiter");
    return NULL;
  }
  waiter->env = env;

  napi_create_promise(env, &waiter->deferred, out_promise);

  napi_value resource_name;
  napi_create_string_utf8(env, "dwTeardown", NAPI_AUTO_LENGTH, &resource_name);
  napi_create_threadsafe_function(
    env, NULL, NULL, resource_name, 0, 1, NULL, NULL, waiter, call_js_teardown_done, &waiter->tsfn
  );

  return waiter;
}

// Creates an already-resolved promise -- used by napi_cleanup's two
// "nothing to wait for" branches (not-the-last-release, and last-release
// with no active ops) so the function's return type is uniformly "a
// promise" regardless of which branch runs.
static napi_value already_resolved_promise(napi_env env) {
  napi_deferred deferred;
  napi_value promise;
  napi_create_promise(env, &deferred, &promise);
  napi_value undefined;
  napi_get_undefined(env, &undefined);
  napi_resolve_deferred(env, deferred, undefined);
  return promise;
}

static napi_value napi_cleanup(napi_env env, napi_callback_info info) {
  (void)info;
  uv_mutex_lock(&g_mutex);

  // Case 1/2: not the last release (or nothing was ever initialized). Decrement
  // only if positive -- a second cleanup() call while g_ref_count is already at
  // 0 (e.g. one already dropped it while teardown is pending) must not go
  // negative.
  if (g_ref_count > 0) {
    g_ref_count--;
  }
  if (g_ref_count > 0) {
    uv_mutex_unlock(&g_mutex);
    return already_resolved_promise(env);
  }

  // Case 3: a teardown from an earlier cleanup() call is already pending
  // (possibly triggered from a different Worker/env). Join its waiter list
  // instead of spawning a second waiter thread.
  if (g_teardown_pending) {
    napi_value promise;
    teardown_waiter_t* waiter = teardown_waiter_create(env, &promise);
    if (waiter == NULL) {
      uv_mutex_unlock(&g_mutex);
      return NULL;  // teardown_waiter_create already threw
    }
    waiter->next = g_teardown_waiters;
    g_teardown_waiters = waiter;
    uv_mutex_unlock(&g_mutex);
    return promise;
  }

  // Case 4: last release, no teardown pending, and nothing active -- the
  // original, unchanged synchronous fast path.
  if (g_active_ops == 0) {
    uv_thread_t tid;
    uv_thread_options_t opts;
    opts.flags = UV_THREAD_HAS_STACK_SIZE;
    opts.stack_size = 2 * 1024 * 1024;
    int spawn_rc = uv_thread_create_ex(&tid, &opts, cleanup_thread_fn, NULL);
    if (spawn_rc == 0) {
      uv_thread_join(&tid);
    }
    // Whether or not the teardown thread ran, treat this as the last release:
    // clear global state so the addon is back to an uninitialized, re-initializable
    // state. If the spawn failed the isolate may not have been torn down (a
    // best-effort degradation, matching the fast path's existing ignore-return
    // posture), but we must not join an uninitialized tid (UB).
    g_thread = NULL;
    g_isolate = NULL;
    g_initialized = 0;
    g_ref_count = 0;
    uv_mutex_unlock(&g_mutex);
    return already_resolved_promise(env);
  }

  // Case 5: last release, but streaming/transform ops are still active.
  // Defer teardown to a dedicated waiter thread instead of blocking this JS
  // thread -- this is the deadlock fix. g_initialized/g_isolate/g_thread stay
  // set until the waiter thread finishes, matching today's behavior of
  // treating "still tearing down" as "still initialized" for concurrent
  // initialize() calls (see Task 3).
  g_teardown_pending = true;
  napi_value promise;
  teardown_waiter_t* waiter = teardown_waiter_create(env, &promise);
  if (waiter == NULL) {
    g_teardown_pending = false;
    uv_mutex_unlock(&g_mutex);
    return NULL;  // teardown_waiter_create already threw
  }
  waiter->next = NULL;
  g_teardown_waiters = waiter;

  uv_thread_t waiter_tid;
  uv_thread_options_t waiter_opts;
  waiter_opts.flags = UV_THREAD_HAS_STACK_SIZE;
  waiter_opts.stack_size = 2 * 1024 * 1024;
  int spawn_rc = uv_thread_create_ex(&waiter_tid, &waiter_opts, teardown_waiter_thread_fn, NULL);
  // Deliberately not joined -- this thread finishes on its own and resolves
  // every waiter's promise itself; joining here would reintroduce exactly
  // the blocking-JS-thread problem this fix removes.

  if (spawn_rc != 0) {
    // Best-effort degradation: if the waiter thread never starts, nothing
    // will ever clear g_teardown_pending, which would otherwise permanently
    // wedge every future initialize()/cleanup() call. Roll back to "teardown
    // did not start" -- the isolate stays up and the caller's promise still
    // resolves, mirroring the fast path's ignore-teardown-return-code posture.
    g_teardown_pending = false;
    g_teardown_waiters = NULL;

    napi_value undefined;
    napi_get_undefined(env, &undefined);
    napi_resolve_deferred(env, waiter->deferred, undefined);

    napi_release_threadsafe_function(waiter->tsfn, napi_tsfn_release);
    free(waiter);

    // The isolate never hit zero refs -- it is still live and un-torn-down,
    // so the process must not believe otherwise. g_initialized/g_isolate stay
    // untouched (still valid).
    g_ref_count = 1;

    uv_mutex_unlock(&g_mutex);
    return promise;
  }

  uv_mutex_unlock(&g_mutex);
  return promise;
}

// --- Module init ---

static void init_g_mutex(void) {
  uv_mutex_init(&g_mutex);
  uv_cond_init(&g_teardown_cond);
}

static napi_value Init(napi_env env, napi_value exports) {
  uv_once(&g_mutex_once, init_g_mutex);

  napi_value fn;

  napi_create_function(env, "initialize", NAPI_AUTO_LENGTH, napi_initialize, NULL, &fn);
  napi_set_named_property(env, exports, "initialize", fn);

  napi_create_function(env, "runScript", NAPI_AUTO_LENGTH, dw_napi_run_script, NULL, &fn);
  napi_set_named_property(env, exports, "runScript", fn);

  napi_create_function(env, "createEngine", NAPI_AUTO_LENGTH, napi_create_engine, NULL, &fn);
  napi_set_named_property(env, exports, "createEngine", fn);

  napi_create_function(env, "createEngineWithResolver", NAPI_AUTO_LENGTH, napi_create_engine_with_resolver, NULL, &fn);
  napi_set_named_property(env, exports, "createEngineWithResolver", fn);

  napi_create_function(env, "destroyEngine", NAPI_AUTO_LENGTH, napi_destroy_engine, NULL, &fn);
  napi_set_named_property(env, exports, "destroyEngine", fn);

  napi_create_function(env, "runScriptEngine", NAPI_AUTO_LENGTH, napi_run_script_engine, NULL, &fn);
  napi_set_named_property(env, exports, "runScriptEngine", fn);

  napi_create_function(env, "runScriptStreamingEngine", NAPI_AUTO_LENGTH, napi_run_script_streaming_engine, NULL, &fn);
  napi_set_named_property(env, exports, "runScriptStreamingEngine", fn);

  napi_create_function(env, "runScriptTransformEngine", NAPI_AUTO_LENGTH, napi_run_script_transform_engine, NULL, &fn);
  napi_set_named_property(env, exports, "runScriptTransformEngine", fn);

  napi_create_function(env, "cleanup", NAPI_AUTO_LENGTH, napi_cleanup, NULL, &fn);
  napi_set_named_property(env, exports, "cleanup", fn);

  return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
