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
    // True when a destroy (via destroyEngine OR the env cleanup hook) was
    // deferred because in_flight > 0; gates the deferred fn_destroy_engine
    // registry removal in bridge_end_op. round-9 (#1) introduced this for the
    // destroyEngine path; round-10 (#1) extended it to bridge_env_cleanup, which
    // must ALSO remove the Java registry entry when its free is deferred --
    // otherwise a resolver-backed engine's ScriptRuntime is left registered with
    // a CallbackWeaveResourceResolver whose ctx points at the freed bridge (UAF).
    bool deferred_registry_remove;
    struct engine_bridge* next;
} engine_bridge_t;
static engine_bridge_t* g_bridges = NULL;  // linked list, guarded by g_mutex

// One record per napi_env that has ever taken an init reference (via
// initialize()). init_refs is that env's net initialize()-minus-cleanup()
// balance. Created lazily on the env's first initialize(); registers exactly
// one env-death hook (env_init_cleanup) at creation; freed by that hook when
// its env dies (after releasing every reference the env still holds). All
// fields mutated ONLY under g_mutex.
//
// INVARIANT: g_ref_count == sum of init_refs over all records in g_env_recs.
// This is the round-13 (#5) fix: the isolate's reference count is owned per
// env, so an abandoned env (or a raw multi-engine-per-initialize() consumer)
// can only release the references IT holds -- it can never drive g_ref_count
// to zero and tear the isolate down while ANOTHER env's engines are live.
typedef struct env_init_rec {
    napi_env env;
    int init_refs;
    struct env_init_rec* next;
} env_init_rec_t;
static env_init_rec_t* g_env_recs = NULL;  // linked list, guarded by g_mutex

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
// Teardown lifecycle, all transitions under g_mutex:
//   NONE         -> no teardown queued or in progress.
//   PENDING_WAIT -> napi_cleanup Case 5 queued a teardown; the waiter thread is
//                   blocked waiting for g_active_ops to drain. The isolate is
//                   STILL LIVE and un-torn-down here, so a fresh initialize()
//                   may ADOPT it (cancel the teardown) instead of blocking the
//                   JS thread -- this is the round-5 deadlock fix.
//   TEARING_DOWN -> the waiter has passed the point of no return and is calling
//                   graal_tear_down_isolate(). Adoption is unsafe; initialize()
//                   must block here, which is deadlock-free because g_active_ops
//                   is already 0 (nothing depends on the JS event loop).
typedef enum {
  TEARDOWN_NONE = 0,
  TEARDOWN_PENDING_WAIT,
  TEARDOWN_TEARING_DOWN,
} teardown_state_t;
static teardown_state_t g_teardown_state = TEARDOWN_NONE;
// Set by an adopting initialize() to tell the waiter thread to abort its
// queued teardown and leave the live isolate intact. Read/reset by the waiter.
static bool g_teardown_cancelled = false;
// Round-14 (#2/#3): set under g_mutex when a reached-zero teardown could NOT be
// carried out (teardown-waiter alloc/spawn failed, or cleanup_thread_fn attach
// failed) and the isolate was therefore left LIVE with g_ref_count == 0 and no
// pending teardown. This is a RETRY SIGNAL, not an ownership reference:
// g_ref_count stays 0, so the invariant g_ref_count == sum(init_refs) is
// unaffected. It is cleared when the isolate is (a) actually torn down by a
// retry, or (b) adopted by a later initialize() (a new owner wants it kept).
// While set with g_active_ops > 0, the op-completion drain point retries the
// teardown once ops reach 0 (retry_stranded_teardown_locked).
static bool g_teardown_needed = false;
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

// Find this env's init record, or NULL. Caller MUST hold g_mutex.
static env_init_rec_t* env_init_rec_find_locked(napi_env env) {
    for (env_init_rec_t* r = g_env_recs; r != NULL; r = r->next) {
        if (r->env == env) return r;
    }
    return NULL;
}

// Find-or-create this env's init record and increment its init_refs. Sets
// *is_new = true iff a record was just allocated (the caller must then register
// the env-death hook on its own thread). Returns the record, or NULL only on
// calloc failure (caller must NOT bump g_ref_count in that case). Caller MUST
// hold g_mutex.
static env_init_rec_t* env_init_rec_acquire_locked(napi_env env, bool* is_new) {
    *is_new = false;
    env_init_rec_t* r = env_init_rec_find_locked(env);
    if (r == NULL) {
        r = (env_init_rec_t*)calloc(1, sizeof(env_init_rec_t));
        if (r == NULL) return NULL;
        r->env = env;
        r->init_refs = 0;
        r->next = g_env_recs;
        g_env_recs = r;
        *is_new = true;
    }
    r->init_refs++;
    return r;
}

// Sum of live per-env init references. Caller holds g_mutex. Establishes the
// value g_ref_count must equal (invariant g_ref_count == sum of init_refs); used
// to restore g_ref_count coherently when a deferred teardown cannot be spawned.
static int env_init_refs_total_locked(void) {
    int total = 0;
    for (env_init_rec_t* r = g_env_recs; r != NULL; r = r->next) total += r->init_refs;
    return total;
}

// Fully dispose of a bridge: delete its napi_ref (if the owning env is still
// alive), free tracked result buffers, free the struct. napi_ref/napi_env are
// thread-affine, so napi_delete_reference MUST run on the bridge's owner
// thread (the JS/Worker thread that created it) while that env is still
// alive -- `env_still_alive` must be false whenever the caller knows the
// owning env is tearing down/dead (e.g. the env == NULL sentinel path in
// call_js_write/call_js_transform_write), even though b->env itself is never
// cleared and stays non-NULL. When env_still_alive is false the napi_ref is
// simply skipped -- Node auto-reclaims refs when their env is destroyed, so
// nothing leaks. The bridge must already be unlinked from g_bridges. Do NOT
// hold g_mutex across this call — it invokes N-API. Callers that freed a
// bridge *early* (destroyEngine / streaming completion) must first drop the
// env cleanup hook via napi_remove_env_cleanup_hook so Node never invokes it
// on freed memory; the hook path itself (bridge_env_cleanup) must not remove
// itself and calls this directly.
// `do_registry_remove` is true when the caller must remove the Java registry
// entry (fn_destroy_engine) for this handle before freeing the record: the
// immediate destroyEngine path, or the deferred drain of either destroyEngine
// (round-9 #1) or the env cleanup hook (round-10 #1). fn_destroy_engine is
// called at most once per handle because destroyEngine and bridge_env_cleanup
// are mutually exclusive (destroyEngine removes the hook). It runs on whichever
// thread finalizes (the owner JS thread from the completion sentinel,
// destroyEngine's thread, or the env-cleanup hook thread); fn_destroy_engine
// attaches its own isolate thread, so it is not JS-thread-affine. Must be
// called WITHOUT g_mutex held (it enters GraalVM and, for env_still_alive,
// calls N-API).
// #3 (round 12): the isolate-touching registry removal. Takes a TRANSIENT
// g_active_ops reservation so graal_tear_down_isolate() cannot run across the
// attach. The teardown-state check and the g_active_ops++ are ONE critical
// section: no teardown path can interleave between "isolate is live" and
// "reservation taken". Callable from any thread NOT holding g_mutex.
static void bridge_finalize_registry(engine_bridge_t* b) {
    if (b == NULL || fn_destroy_engine == NULL) return;
    uv_mutex_lock(&g_mutex);
    // If the waiter already committed to physical teardown (TEARING_DOWN) or the
    // isolate is already gone, the Java registry died/dies with it -- nothing to
    // remove, and attaching would race graal_tear_down_isolate. Skip. Because
    // the waiter publishes TEARING_DOWN (and Case 4 holds g_mutex across its
    // g_active_ops==0 check + teardown) under this same lock, this check plus the
    // increment below cannot be split by a teardown.
    if (g_teardown_state == TEARDOWN_TEARING_DOWN || g_isolate == NULL) {
        uv_mutex_unlock(&g_mutex);
        return;
    }
    g_active_ops++;  // pins the live isolate against teardown for this attach
    uv_mutex_unlock(&g_mutex);

    void* thread = NULL;
    if (fn_attach_thread(g_isolate, &thread) == 0) {
        fn_destroy_engine(thread, b->handle);
        fn_detach_thread(thread);
    }

    // Verbatim g_active_ops release pattern.
    uv_mutex_lock(&g_mutex);
    g_active_ops--;
    uv_cond_broadcast(&g_teardown_cond);
    uv_mutex_unlock(&g_mutex);
}

// The non-isolate finalize phase: delete the resolver napi_ref (owner JS thread
// only, and only while its env is alive -- resolver-gated), free tracked result
// buffers, free the record. Touches no GraalVM isolate state, so it is safe to
// run after the g_active_ops reservation above is released.
static void bridge_finalize_free(engine_bridge_t* b, bool env_still_alive) {
    if (b == NULL) return;
    if (env_still_alive && b->resolver_js != NULL && b->env != NULL) {
        napi_delete_reference(b->env, b->resolver_js);
    }
    resolver_results_free_all(b);
    free(b);
}

// Thin wrapper preserving the original signature and every call site. Registry
// removal (if requested) runs first under its transient reservation, then the
// record is freed.
static void bridge_finalize(engine_bridge_t* b, bool env_still_alive, bool do_registry_remove) {
    if (b == NULL) return;
    if (do_registry_remove) bridge_finalize_registry(b);
    bridge_finalize_free(b, env_still_alive);
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
        // round-10 (#1): the draining op must ALSO remove the Java registry
        // entry (like destroyEngine's deferred path), or the resolver engine's
        // ScriptRuntime is left registered with a resolver ctx pointing at the
        // freed bridge. Set the deferred-registry-removal flag here.
        b->deferred_registry_remove = true;
        uv_mutex_unlock(&g_mutex);
        return;
    }
    // in_flight == 0: finalize now. The abandoned engine's init reference is
    // NOT released here (round-13 #5) -- it is released by the env-death hook
    // (env_init_cleanup) when this env dies, which owns the whole per-env
    // balance. There is nothing left to do under the lock before unlocking in
    // this branch. bridge_finalize_registry inside finalize checks teardown
    // state under g_mutex, so a torn-down/TEARING_DOWN isolate makes the
    // registry removal a correct no-op (the Java registry died with the
    // isolate).
    uv_mutex_unlock(&g_mutex);

    // We are inside Node's invocation of this hook, so we must not (and need not)
    // call napi_remove_env_cleanup_hook for ourselves here. The env is still
    // alive here -- that is the whole point of this hook's design (see above) --
    // so the napi_ref deletion in bridge_finalize is legal.
    // round-10 (#1): remove the Java registry entry too (do_registry_remove=true).
    // This hook only ever fires for a resolver-backed engine that was never
    // passed to destroyEngine (destroyEngine removes this hook), so its
    // initialize() ref was never released either -> the isolate is still live
    // and fn_destroy_engine's fresh-thread attach is legal (bridge_finalize
    // guards on g_isolate for the main-env-after-isolate-teardown corner). Not
    // removing it would leave a CallbackWeaveResourceResolver whose ctx is the
    // freed bridge -> UAF on a later invocation of this handle.
    bridge_finalize(b, /*env_still_alive=*/true, /*do_registry_remove=*/true);
}

// Increment this engine's in_flight while g_mutex is ALREADY held. Used by the
// run/streaming/transform admission paths so the per-engine pin is taken in the
// SAME critical section as the g_active_ops reservation and the lifecycle check
// -- closing the round-11 window where a concurrent destroyEngine could observe
// in_flight == 0 and free the bridge under an already-admitted op. Returns the
// record, or NULL for an unknown handle (nothing to pin; the worker/native call
// surfaces "Unknown engine handle"). Caller MUST hold g_mutex.
static engine_bridge_t* bridge_begin_op_locked(long long handle) {
    engine_bridge_t* b = bridge_find(handle);
    if (b != NULL) b->in_flight++;
    return b;
}

// A streaming/transform/run op marks one op in flight on the engine's record so
// the record (and, for resolver-backed engines, its napi_ref) cannot be freed
// while the background uv_thread runs -- and, since round-9 (#1), so that
// destroyEngine defers the Java registry removal until this op drains. Every
// engine (resolver-backed or resolver-less) now has a record, so
// bridge_begin_op_locked returns a non-NULL pointer for any known handle; the
// completion sentinel MUST call bridge_end_op on it to balance in_flight and
// run any deferred destroy. Returns NULL only for an unknown handle (nothing to
// protect, no bridge_end_op needed). The returned pointer is stable for the
// op's lifetime because in_flight > 0 blocks both destroyEngine and the env
// cleanup hook from freeing the record. Since round-11 (#2), every call site
// takes the pin atomically with its g_mutex-guarded admission check via
// bridge_begin_op_locked directly (no self-locking wrapper) -- see
// napi_run_script_streaming_engine / napi_run_script_transform_engine.

// End a streaming/transform op. Runs on the owner (JS) thread from the completion
// sentinel. If destroyEngine (or the env cleanup hook) ran while this op was in
// flight, it deferred the free — already unlinked from g_bridges — so the last op
// to drain finalizes the bridge here, on the legal (owner) thread. `env_still_alive`
// must be false when the caller is running the env == NULL sentinel path (the
// owning env is tearing down/dead), so a finalize triggered from here does not
// call napi_delete_reference on a dead env.
static void bridge_end_op(engine_bridge_t* b, bool env_still_alive) {
    if (b == NULL) return;
    uv_mutex_lock(&g_mutex);
    b->in_flight--;
    bool finalize = (b->destroy_pending && b->in_flight == 0);
    bool remove_registry = finalize && b->deferred_registry_remove;
    uv_mutex_unlock(&g_mutex);
    // remove_registry is true when either destroyEngine (round-9 #1) or the env
    // cleanup hook (round-10 #1) deferred the registry removal while this op was
    // in flight; the draining op performs it exactly once here. bridge_finalize
    // guards the call on g_isolate, so a teardown that raced ahead is a no-op.
    if (finalize) bridge_finalize(b, env_still_alive, /*do_registry_remove=*/remove_registry);
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

// Forward declaration: the env-death hook that reclaims an abandoned env's
// init references. Defined below (round-13 #5); registered here (in
// env_init_acquire_and_hook) because napi_add_env_cleanup_hook is only legal
// while the env is alive on its own JS thread, which napi_initialize is.
static void env_init_cleanup(void* arg);  // defined below (round-13 #5)

// Acquire one init reference for `env` under g_mutex, registering the env-death
// hook on first use. Returns true on success (caller then does g_ref_count++);
// on failure the caller must NOT bump g_ref_count -- it unlocks and throws.
// Caller MUST hold g_mutex; this function keeps it held on success and on the
// calloc-failure return. On hook-registration failure it rolls back the
// just-acquired init_refs (freeing the record if it drops to 0) so no orphan
// record without a death hook survives.
static bool env_init_acquire_and_hook(napi_env env) {
    bool is_new = false;
    env_init_rec_t* rec = env_init_rec_acquire_locked(env, &is_new);
    if (rec == NULL) return false;  // calloc failed
    if (is_new) {
        napi_status hs = napi_add_env_cleanup_hook(env, env_init_cleanup, rec);
        if (hs != napi_ok) {
            // Roll back: this record has no death hook, so its references would
            // never be reclaimed. Drop the one we just took; free if now empty.
            rec->init_refs--;
            if (rec->init_refs == 0) {
                env_init_rec_t** pp = &g_env_recs;
                while (*pp != NULL) { if (*pp == rec) { *pp = rec->next; break; } pp = &(*pp)->next; }
                free(rec);
            }
            return false;
        }
    }
    return true;
}

// Forward declaration: tears down g_isolate on a dedicated attached thread.
// Defined below; used here (napi_initialize's create-path acquire-failure
// recovery) and further down by isolate_ref_release_n_locked.
static void cleanup_thread_fn(void* arg);

// Forward declaration: retries a stranded teardown (round-14 #2/#3). Defined
// further below; used by the streaming/transform op-completion drain points,
// which run earlier in this file than the definition.
static void retry_stranded_teardown_locked(void);

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

  // A prior last-release could not tear the isolate down and armed the retry
  // signal (review #6 #3/#4). Because retries otherwise fire only at op
  // completion (the streaming/transform drains), a zero-op stranded isolate
  // would never be reclaimed and the adoption/fast paths below would silently
  // discard the pending teardown (review #6 #5). Drive the pending teardown to
  // completion here first: on success g_isolate/g_initialized are cleared and we
  // build a fresh isolate below; on repeated failure the live isolate is adopted
  // by the fast path (safe -- the teardown was resource reclamation, not a
  // malfunction). No-ops cheaply when nothing is stranded (flag clear -> return).
  retry_stranded_teardown_locked();

  // If a teardown from a prior cleanup() is still draining (the isolate is
  // being torn down on the waiter thread from Task 2), do not race a fresh
  // graal_create_isolate against it -- wait until the isolate is fully gone
  // before proceeding. This is a narrow, rare path (re-initializing mid-drain),
  // not a fast path, so a blocking wait here is acceptable and matches this
  // function's existing fully-synchronous contract -- except in
  // TEARDOWN_PENDING_WAIT (see below), where blocking would deadlock.
  while (g_teardown_state != TEARDOWN_NONE || (g_isolate != NULL && !g_initialized)) {
    if (g_teardown_state == TEARDOWN_PENDING_WAIT) {
      // A teardown is queued but the waiter has NOT begun physical teardown
      // (that transition to TEARING_DOWN happens under this same g_mutex), so
      // g_isolate/g_initialized are still valid. Blocking here would freeze the
      // JS event loop that an active streaming/transform worker needs in order
      // to drain g_active_ops -- the waiter would then wait forever and this
      // wait would never end (the P1 deadlock). Instead, ADOPT the live isolate:
      // cancel the queued teardown, take a fresh ref, and wake the waiter so it
      // aborts without tearing down. g_initialized is already 1, so fall through
      // to the ref-count path below is unnecessary -- return directly.
      if (!env_init_acquire_and_hook(env)) {
        uv_mutex_unlock(&g_mutex);
        napi_throw_error(env, NULL, "Failed to allocate/register env init record");
        return NULL;
      }
      g_teardown_cancelled = true;
      g_ref_count++;
      g_teardown_needed = false;  // round-14: a new owner wants the isolate kept
      uv_cond_broadcast(&g_teardown_cond);
      uv_mutex_unlock(&g_mutex);
      return NULL;
    }
    // TEARDOWN_TEARING_DOWN (or a transient g_isolate!=NULL && !g_initialized):
    // g_active_ops has already reached 0, so nothing depends on the JS event
    // loop -- this blocking wait is deadlock-free and preserves the original
    // "don't race graal_create_isolate against graal_tear_down_isolate"
    // guarantee that round 3's Task 3 added.
    uv_cond_wait(&g_teardown_cond, &g_mutex);
  }

  if (g_initialized) {
    if (!env_init_acquire_and_hook(env)) {
      uv_mutex_unlock(&g_mutex);
      napi_throw_error(env, NULL, "Failed to allocate/register env init record");
      return NULL;
    }
    g_ref_count++;
    g_teardown_needed = false;  // round-14: a new owner wants the isolate kept
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

  if (!env_init_acquire_and_hook(env)) {
    // init_thread_fn already built the isolate (g_isolate != NULL) but we have
    // not yet set g_initialized = 1. If we just unlock and throw here, we leave
    // g_isolate != NULL && g_initialized == 0 -- the exact condition the wait
    // loop above (`g_isolate != NULL && !g_initialized`) treats as "a teardown
    // is in flight". With g_teardown_state == TEARDOWN_NONE that loop cannot
    // take the TEARDOWN_PENDING_WAIT adoption branch, so it falls into
    // uv_cond_wait(&g_teardown_cond, ...) with nothing left to ever broadcast --
    // every subsequent initialize() on any env hangs forever. Every sibling
    // error path (args.result != 0 above, and the spawn-failure path before it)
    // leaves g_isolate == NULL instead, which is the recoverable state. Tear
    // the just-built isolate back down before throwing so we restore that same
    // recoverable g_isolate == NULL state.
    //
    // g_ref_count is still 0 here (we never got past this check to bump it),
    // and env_init_acquire_and_hook leaves no orphan record behind on failure
    // (calloc failure never created one; hook-registration failure rolls its
    // own record back) -- so the invariant g_ref_count == sum(init_refs) holds
    // with both sides at 0 both before and after this block.
    uv_thread_t cleanup_tid;
    uv_thread_options_t cleanup_opts;
    cleanup_opts.flags = UV_THREAD_HAS_STACK_SIZE;
    cleanup_opts.stack_size = 2 * 1024 * 1024;
    int torn_down = 0;
    int cleanup_spawn_rc = uv_thread_create_ex(&cleanup_tid, &cleanup_opts, cleanup_thread_fn, &torn_down);
    if (cleanup_spawn_rc == 0) {
      uv_thread_join(&cleanup_tid);
    }
    if (torn_down) {
      // Teardown ran (or there was nothing to tear down) -- clear the globals
      // so the next initialize() sees a clean slate. g_ref_count is already 0.
      g_thread = NULL;
      g_isolate = NULL;
      g_initialized = 0;
    } else {
      // Spawn failed, or cleanup_thread_fn's attach/teardown to the isolate
      // failed. The isolate is genuinely still alive with g_initialized == 0.
      // Without a retry signal the next initialize() would reach the wait loop's
      // `g_isolate != NULL && !g_initialized` condition with TEARDOWN_NONE (so no
      // adoption branch) and block on uv_cond_wait forever -- nothing left to
      // broadcast (review #7 #2). Arm the stranded-teardown retry so the
      // retry_stranded_teardown_locked() at the top of the next napi_initialize
      // reclaims the isolate (teardown succeeds -> fresh build). This path leaves
      // g_initialized == 0, so -- unlike the release-path twin in
      // isolate_ref_release_n_locked, which leaves g_initialized == 1 and is
      // adopted by the g_initialized-gated fast path -- recovery here relies on
      // the retry actually tearing down: it recovers the realistic TRANSIENT
      // failure, but a truly PERSISTENT graal_tear_down_isolate failure would
      // re-arm and retry each time and ultimately leave the isolate stranded
      // until process exit (best-effort degradation, not a wedge of new work).
      // g_ref_count is still 0 here, so g_teardown_needed (a retry SIGNAL, not a
      // reference) keeps the invariant g_ref_count == sum(init_refs) intact.
      // Mirrors the twin arm in teardown_waiter_thread_fn.
      g_teardown_needed = true;
    }
    uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "Failed to allocate/register env init record");
    return NULL;
  }
  g_initialized = 1;
  g_ref_count++;
  // Round-14: defensive clear. A brand-new isolate can never carry a stale
  // stranded-teardown signal for itself (a new graal_create_isolate only runs
  // when g_isolate == NULL, so this path cannot reuse a surviving stranded
  // isolate) -- but clear it here anyway at the single create-path success
  // point so no later drain retries a teardown against the isolate this
  // initialize() just created and now owns.
  g_teardown_needed = false;
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

// Round-9 (#2): static terminal-error JSON used when a worker thread cannot
// even strdup its result string (OOM). It is a file-scope constant, never
// heap-allocated, so any code path that would free a sentinel/chunk buffer
// must first check `buf != OOM_JSON` -- freeing a static pointer is UB. The
// wording matches the existing terse worker error style ("Empty response").
static const char OOM_JSON[] = "{\"success\":false,\"error\":\"Out of memory\"}";

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
  // The engine's record whose in_flight count this op holds. Since round-9 (#1)
  // every engine has a record, so this is non-NULL for any known handle (NULL only
  // for an unknown handle). The completion sentinel calls bridge_end_op on it to
  // balance in_flight and run any deferred destroy (F1).
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

    if (chunk->buf != OOM_JSON) free(chunk->buf);
    free(chunk);
    free(w->script);
    free(w->inputs_json);

    uv_thread_join(&w->tid);
    napi_release_threadsafe_function(w->tsfn, napi_tsfn_release);
    // Drop the in-flight hold last, on this owner thread: if destroyEngine ran
    // during the op it deferred the free to here (F1). After this the bridge may
    // be freed, so touch nothing on it afterward. env == NULL means this env is
    // dead/tearing down -- tell bridge_end_op (and any bridge_finalize it
    // triggers) not to touch the napi_ref, since b->env is this same dead env.
    bridge_end_op(w->bridge, /*env_still_alive=*/env != NULL);
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
  // Round-9 (#2): OOM here must not deref NULL / memcpy into NULL. Returning -1
  // aborts the native run cleanly (write-callback contract: non-zero stops the
  // DataWeave run); the worker then still produces a terminal meta_result and
  // sentinel, so the op resolves.
  struct chunk_data* chunk = malloc(sizeof(struct chunk_data));
  if (chunk == NULL) return -1;
  chunk->buf = malloc(len);
  if (chunk->buf == NULL) { free(chunk); return -1; }
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

  // Round-9 (#2): strdup can fail under OOM. meta_result must still be a valid
  // C string so the sentinel path below can deliver a terminal result -- fall
  // back to the OOM_JSON static (which must never be freed; see the guarded
  // frees below and in call_js_write).
  char* meta_result = NULL;
  if (rc != 0) {
    char err[256];
    snprintf(err, sizeof(err), "{\"success\":false,\"error\":\"Failed to attach thread (code %d)\"}", rc);
    meta_result = strdup(err);
    if (meta_result == NULL) meta_result = (char*)OOM_JSON;
  } else {
    void* result_ptr = fn_run_script_callback_engine(
      worker_thread, w->handle, w->script, w->inputs_json, streaming_write_cb, (void*)w->tsfn
    );
    if (result_ptr) {
      meta_result = strdup((const char*)result_ptr);
      if (meta_result == NULL) meta_result = (char*)OOM_JSON;
      fn_free_cstring(worker_thread, result_ptr);
    } else {
      meta_result = strdup("{\"success\":false,\"error\":\"Empty response\"}");
      if (meta_result == NULL) meta_result = (char*)OOM_JSON;
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
  // Round-14 (#2/#3): if a prior last-release could not tear the isolate down
  // and left it stranded (g_teardown_needed), retry now that this op has drained.
  retry_stranded_teardown_locked();
  uv_mutex_unlock(&g_mutex);

  // Round-9 (#2): if even the sentinel struct cannot be allocated, we cannot
  // enqueue a completion -- run the SAME native finalize the env-dead
  // (napi_closing) branch below runs, so g_active_ops (already decremented
  // above) plus the bridge in-flight hold and w are released and nothing is
  // stranded. This is the "sentinel malloc NULL -> skip enqueue + unwind like
  // the env-dead sentinel branch" path.
  struct chunk_data* sentinel = malloc(sizeof(struct chunk_data));
  if (sentinel == NULL) {
    if (meta_result != OOM_JSON) free(meta_result);
    free(w->script);
    free(w->inputs_json);
    bridge_end_op(w->bridge, /*env_still_alive=*/false);
    free(w);
    return;
  }
  sentinel->buf = meta_result;
  sentinel->len = -1;
  napi_status enq = napi_call_threadsafe_function(w->tsfn, sentinel, napi_tsfn_blocking);
  if (enq != napi_ok) {
    // The env is tearing down (napi_closing): the sentinel was dropped and
    // call_js_write will never run, so finalize here instead -- the exact same
    // native cleanup as call_js_write's sentinel branch, minus the things
    // that are illegal, impossible, or already done on this worker thread:
    //   - no napi value / deferred call (env is dead; those are env-affine)
    //   - no uv_thread_join(&w->tid): we ARE w->tid; a thread cannot join
    //     itself. The handle goes unreaped -- an unavoidable, negligible leak
    //     during a Worker teardown that is already discarding this env.
    //   - no napi_release_threadsafe_function(w->tsfn, ...): this tsfn was
    //     created with initial_thread_count = 1 and this worker is its sole
    //     producer, so Node's internal thread_count for it is exactly 1 on
    //     entry to this Push call. Node's ThreadSafeFunction::Push (the
    //     implementation behind napi_call_threadsafe_function) decrements
    //     thread_count for the calling thread BEFORE returning napi_closing,
    //     and -- if that decrement brings thread_count to 0 while the
    //     internal state is already kClosed -- Push runs `delete this` on
    //     the tsfn right there. So receiving napi_closing here already IS
    //     this thread's discharge of the tsfn (matches the doc's "destroyed
    //     when every thread ... has called napi_release_threadsafe_function()
    //     or has received a return status of napi_closing"); calling release
    //     again afterward would be a double-discharge and, whenever Push
    //     already deleted the object, a use-after-free. Omit it.
    // End the bridge op with env_still_alive=false so bridge_finalize skips
    // the thread-affine napi_delete_reference (Node auto-reclaims the ref
    // when the dead env is destroyed).
    if (sentinel->buf != OOM_JSON) free(sentinel->buf);
    free(sentinel);
    free(w->script);
    free(w->inputs_json);
    bridge_end_op(w->bridge, /*env_still_alive=*/false);
    free(w);
  }
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

  // Validate the handle before admission (round-6 #1, defense-in-depth): a
  // non-integer handle must be rejected before g_active_ops is ever reserved,
  // so there is nothing to unwind here -- simpler than reserving first and
  // unwinding on failure.
  int64_t handle64;
  if (napi_get_value_int64(env, argv[0], &handle64) != napi_ok) {
    napi_throw_error(env, NULL, "runScriptStreamingEngine: handle must be an integer");
    return NULL;
  }

  // Atomic admission: check lifecycle state and reserve the op in ONE critical
  // section, before allocating any work/tsfn/promise/bridge. Reading
  // g_initialized outside the lock and reserving g_active_ops later (the old
  // shape) let a second Worker's napi_cleanup Case-4 tear the isolate down in
  // the gap, so a freshly spawned worker attached to a dead isolate (round-6
  // #2). Rejecting on g_teardown_state != TEARDOWN_NONE also refuses new ops
  // once a teardown is queued/underway. Admit an ADOPTED isolate:
  // napi_initialize's adoption branch sets g_teardown_cancelled = true on a
  // still-live PENDING_WAIT isolate but does not reset g_teardown_state (only
  // the async waiter does), so a merely-cancelled teardown must not reject
  // here -- otherwise a valid post-adoption op throws "Not initialized". A
  // genuine (non-cancelled) PENDING_WAIT or a committed TEARING_DOWN still
  // rejects.
  uv_mutex_lock(&g_mutex);
  if (!g_initialized || (g_teardown_state != TEARDOWN_NONE && !g_teardown_cancelled)) {
    uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "Not initialized. Call initialize() first.");
    return NULL;
  }
  g_active_ops++;
  // Round-11 (#2): pin the engine in the SAME critical section as the
  // g_active_ops reservation, before any window a concurrent destroyEngine
  // could use. NULL for an unknown handle (the worker surfaces "Unknown engine
  // handle"). Stashed on w->bridge once w is allocated; every early-return
  // below releases it via bridge_end_op alongside g_active_ops.
  engine_bridge_t* pinned = bridge_begin_op_locked((long long)handle64);
  uv_mutex_unlock(&g_mutex);

  // Conversions run after the admission reservation above, so any throw here
  // must release g_active_ops before returning (round-7 #2).
  size_t script_len, inputs_len;
  if (napi_get_value_string_utf8(env, argv[1], NULL, 0, &script_len) != napi_ok) {
    bridge_end_op(pinned, /*env_still_alive=*/true);
    uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "runScriptStreamingEngine: script must be a string");
    return NULL;
  }
  if (napi_get_value_string_utf8(env, argv[2], NULL, 0, &inputs_len) != napi_ok) {
    bridge_end_op(pinned, /*env_still_alive=*/true);
    uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "runScriptStreamingEngine: inputsJson must be a string");
    return NULL;
  }

  // OOM safety (round-8): every allocation is NULL-checked before it is
  // dereferenced, and every failure path releases the g_active_ops reservation
  // taken above (mirroring napi_run_script_engine's "OOM" throw). Without this
  // an allocation failure segfaults the host process AND strands g_active_ops.
  struct streaming_work* w = calloc(1, sizeof(struct streaming_work));
  if (w == NULL) {
    // w is NULL -- do not touch w->script/w->inputs_json here.
    bridge_end_op(pinned, /*env_still_alive=*/true);
    uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "OOM");
    return NULL;
  }
  w->handle = (long long)handle64;
  w->script = malloc(script_len + 1);
  w->inputs_json = malloc(inputs_len + 1);
  if (w->script == NULL || w->inputs_json == NULL) {
    free(w->script); free(w->inputs_json); free(w);
    bridge_end_op(pinned, /*env_still_alive=*/true);
    uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "OOM");
    return NULL;
  }
  if (napi_get_value_string_utf8(env, argv[1], w->script, script_len + 1, NULL) != napi_ok ||
      napi_get_value_string_utf8(env, argv[2], w->inputs_json, inputs_len + 1, NULL) != napi_ok) {
    free(w->script); free(w->inputs_json); free(w);
    bridge_end_op(pinned, /*env_still_alive=*/true);
    uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "runScriptStreamingEngine: failed to read script/inputsJson");
    return NULL;
  }

  // Round-9 (#3, updated round-11 #2): the resource creations below run AFTER
  // g_active_ops was reserved (and after w + its buffers were allocated), and
  // the engine pin (`pinned`) was already taken at admission. A failed create
  // must release both the pin (bridge_end_op) and g_active_ops (verbatim
  // pattern), free any tsfn already created, free w + buffers, and throw --
  // otherwise the worker sees a zeroed w->tsfn/w->deferred (crash), the pin is
  // stranded (blocks destroyEngine forever), or g_active_ops is stranded
  // (teardown wedge).
  napi_value resource_name;
  if (napi_create_string_utf8(env, "dwStreaming", NAPI_AUTO_LENGTH, &resource_name) != napi_ok) {
    free(w->script); free(w->inputs_json); free(w);
    bridge_end_op(pinned, /*env_still_alive=*/true);
    uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "runScriptStreamingEngine: failed to create resource name");
    return NULL;
  }
  if (napi_create_threadsafe_function(env, argv[3], NULL, resource_name, 0, 1, NULL, NULL, w, call_js_write, &w->tsfn) != napi_ok) {
    free(w->script); free(w->inputs_json); free(w);
    bridge_end_op(pinned, /*env_still_alive=*/true);
    uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "runScriptStreamingEngine: failed to create threadsafe function");
    return NULL;
  }

  napi_value promise;
  if (napi_create_promise(env, &w->deferred, &promise) != napi_ok) {
    // The tsfn was created above; release it before freeing w (it holds w as
    // its context). No worker exists yet, so this release is the sole discharge.
    napi_release_threadsafe_function(w->tsfn, napi_tsfn_release);
    free(w->script); free(w->inputs_json); free(w);
    bridge_end_op(pinned, /*env_still_alive=*/true);
    uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "runScriptStreamingEngine: failed to create promise");
    return NULL;
  }

  // Round-11 (#2): the pin was taken at admission (bridge_begin_op_locked) in
  // the same critical section as g_active_ops, so a concurrent destroyEngine
  // could never free this bridge under the admitted op. Just record it on w;
  // the completion sentinel releases it via bridge_end_op. NULL for a
  // resolver-less/unknown engine, handled everywhere as a no-op.
  w->bridge = pinned;

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

    // Synchronous call on the JS thread -- env is live here.
    bridge_end_op(w->bridge, /*env_still_alive=*/true);
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
  // The engine's record whose in_flight count this op holds. Since round-9 (#1)
  // every engine has a record, so this is non-NULL for any known handle (NULL only
  // for an unknown handle). The completion sentinel calls bridge_end_op on it to
  // balance in_flight and run any deferred destroy (F1).
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
  // Round-9 (#2): OOM-safe, mirrors streaming_write_cb. Return -1 to abort the
  // native run cleanly; the worker still delivers a terminal sentinel.
  struct chunk_data* chunk = malloc(sizeof(struct chunk_data));
  if (chunk == NULL) return -1;
  chunk->buf = malloc(len);
  if (chunk->buf == NULL) { free(chunk); return -1; }
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

    if (chunk->buf != OOM_JSON) free(chunk->buf);
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
    // be freed, so touch nothing on it afterward. env == NULL means this env is
    // dead/tearing down -- tell bridge_end_op (and any bridge_finalize it
    // triggers) not to touch the napi_ref, since b->env is this same dead env.
    bridge_end_op(w->bridge, /*env_still_alive=*/env != NULL);
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

  // Round-9 (#2): strdup can fail under OOM; fall back to the OOM_JSON static
  // so the sentinel below still delivers a terminal result. Mirrors
  // streaming_thread_fn.
  char* meta_result = NULL;
  if (rc != 0) {
    char err[256];
    snprintf(err, sizeof(err), "{\"success\":false,\"error\":\"Failed to attach thread (code %d)\"}", rc);
    meta_result = strdup(err);
    if (meta_result == NULL) meta_result = (char*)OOM_JSON;
  } else {
    void* result_ptr = fn_run_script_input_output_callback_engine(
      worker_thread, w->handle, w->script, w->inputs_json,
      w->input_name, w->input_mime_type, w->input_charset,
      transform_read_cb, transform_write_cb, (void*)w
    );

    if (result_ptr) {
      meta_result = strdup((const char*)result_ptr);
      if (meta_result == NULL) meta_result = (char*)OOM_JSON;
      fn_free_cstring(worker_thread, result_ptr);
    } else {
      meta_result = strdup("{\"success\":false,\"error\":\"Empty response\"}");
      if (meta_result == NULL) meta_result = (char*)OOM_JSON;
    }
    fn_detach_thread(worker_thread);
  }

  // See streaming_thread_fn's comment: decrement here (after detach), not in
  // call_js_transform_write's completion branch, to avoid the same
  // circular-wait deadlock against napi_initialize's pending-teardown wait.
  uv_mutex_lock(&g_mutex);
  g_active_ops--;
  uv_cond_broadcast(&g_teardown_cond);
  // Round-14 (#2/#3): retry a stranded teardown now that this op has drained.
  retry_stranded_teardown_locked();
  uv_mutex_unlock(&g_mutex);

  // Round-9 (#2): sentinel malloc NULL -> skip enqueue and run the same native
  // finalize as the env-dead branch below (release the bridge hold + free w and
  // all fields), so g_active_ops (already decremented above) and the in-flight
  // hold are released. No self-join, no env-affine napi call, no tsfn release
  // (see the env-dead branch's citation for why releasing the tsfns here is
  // unsafe).
  struct chunk_data* sentinel = malloc(sizeof(struct chunk_data));
  if (sentinel == NULL) {
    if (meta_result != OOM_JSON) free(meta_result);
    free(w->script);
    free(w->inputs_json);
    free(w->input_name);
    free(w->input_mime_type);
    free(w->input_charset);
    bridge_end_op(w->bridge, /*env_still_alive=*/false);
    free(w);
    return;
  }
  sentinel->buf = meta_result;
  sentinel->len = -1;
  napi_status enq = napi_call_threadsafe_function(w->write_tsfn, sentinel, napi_tsfn_blocking);
  if (enq != napi_ok) {
    // See streaming_thread_fn: env tearing down, sentinel dropped, finalize
    // here. No self-join, no env-affine napi call.
    //
    // Do NOT release write_tsfn: this worker is its sole producer
    // (initial_thread_count = 1), so receiving napi_closing from this same
    // Push call already decremented Node's internal thread_count for it to 0
    // and, if the tsfn's internal state was already kClosed, already ran
    // `delete this` on it inside Push -- see streaming_thread_fn's comment
    // for the full citation. Releasing it again here would be a
    // double-discharge and potentially a use-after-free.
    //
    // Do NOT release read_tsfn either, even though this same worker is also
    // its sole producer: whether *it* has already received napi_closing (and
    // so already discharged/deleted itself the same way) depends on whether
    // the script issued reads during teardown, which this code path has no
    // way to know. We cannot prove read_tsfn's discharge state here, so --
    // consistent with the env == NULL dead-env handling elsewhere in this
    // file -- we accept the small leak of an already-tearing-down tsfn
    // rather than risk a use-after-free on an object whose state is unknown.
    //
    // End the bridge op with env_still_alive=false so bridge_finalize skips
    // the thread-affine napi_delete_reference (Node auto-reclaims the ref
    // when the dead env is destroyed).
    if (sentinel->buf != OOM_JSON) free(sentinel->buf);
    free(sentinel);
    free(w->script);
    free(w->inputs_json);
    free(w->input_name);
    free(w->input_mime_type);
    free(w->input_charset);
    bridge_end_op(w->bridge, /*env_still_alive=*/false);
    free(w);
  }
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

  // Validate the handle before admission (round-6 #1, defense-in-depth): a
  // non-integer handle must be rejected before g_active_ops is ever reserved,
  // so there is nothing to unwind here -- simpler than reserving first and
  // unwinding on failure. Keep this consistent with
  // napi_run_script_streaming_engine's ordering.
  int64_t handle64;
  if (napi_get_value_int64(env, argv[0], &handle64) != napi_ok) {
    napi_throw_error(env, NULL, "runScriptTransformEngine: handle must be an integer");
    return NULL;
  }

  // Atomic admission (see napi_run_script_streaming_engine for the full
  // rationale, round-6 #2): check lifecycle + reserve g_active_ops in one
  // critical section, before any work/tsfn/promise/bridge is committed.
  // Admit an ADOPTED isolate: napi_initialize's adoption branch sets
  // g_teardown_cancelled = true on a still-live PENDING_WAIT isolate but does
  // not reset g_teardown_state (only the async waiter does), so a
  // merely-cancelled teardown must not reject here -- otherwise a valid
  // post-adoption op throws "Not initialized". A genuine (non-cancelled)
  // PENDING_WAIT or a committed TEARING_DOWN still rejects.
  uv_mutex_lock(&g_mutex);
  if (!g_initialized || (g_teardown_state != TEARDOWN_NONE && !g_teardown_cancelled)) {
    uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "Not initialized. Call initialize() first.");
    return NULL;
  }
  g_active_ops++;
  // Round-11 (#2): pin the engine in the SAME critical section as the
  // g_active_ops reservation, before any window a concurrent destroyEngine
  // could use. NULL for an unknown handle (the worker surfaces "Unknown engine
  // handle"). Stashed on w->bridge once w is allocated; every early-return
  // below releases it via bridge_end_op alongside g_active_ops.
  engine_bridge_t* pinned = bridge_begin_op_locked((long long)handle64);
  uv_mutex_unlock(&g_mutex);

  // Conversions run after the admission reservation above, so any throw here
  // must free the partially-populated work struct AND release g_active_ops
  // before returning (round-7 #2). calloc zeroed w, so free() on an unset
  // field pointer is a safe free(NULL). TRANSFORM_FAIL centralizes the
  // unwind.
  // OOM safety (round-8): NULL-check the work struct before dereferencing it,
  // releasing the g_active_ops reservation taken above. The per-field malloc
  // checks below reuse TRANSFORM_FAIL (which frees all fields + w and unwinds);
  // this standalone branch cannot use it (the macro dereferences w).
  struct transform_work* w = calloc(1, sizeof(struct transform_work));
  if (w == NULL) {
    bridge_end_op(pinned, /*env_still_alive=*/true);
    uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "OOM");
    return NULL;
  }
  size_t len;
  w->handle = (long long)handle64;

  #define TRANSFORM_FAIL(msg) do { \
      bridge_end_op(pinned, /*env_still_alive=*/true); \
      free(w->script); free(w->inputs_json); free(w->input_name); \
      free(w->input_mime_type); free(w->input_charset); free(w); \
      uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex); \
      napi_throw_error(env, NULL, (msg)); \
      return NULL; \
  } while (0)

  if (napi_get_value_string_utf8(env, argv[1], NULL, 0, &len) != napi_ok) TRANSFORM_FAIL("runScriptTransformEngine: script must be a string");
  w->script = malloc(len + 1);
  if (w->script == NULL) TRANSFORM_FAIL("OOM");
  if (napi_get_value_string_utf8(env, argv[1], w->script, len + 1, NULL) != napi_ok) TRANSFORM_FAIL("runScriptTransformEngine: failed to read script");

  if (napi_get_value_string_utf8(env, argv[2], NULL, 0, &len) != napi_ok) TRANSFORM_FAIL("runScriptTransformEngine: inputsJson must be a string");
  w->inputs_json = malloc(len + 1);
  if (w->inputs_json == NULL) TRANSFORM_FAIL("OOM");
  if (napi_get_value_string_utf8(env, argv[2], w->inputs_json, len + 1, NULL) != napi_ok) TRANSFORM_FAIL("runScriptTransformEngine: failed to read inputsJson");

  if (napi_get_value_string_utf8(env, argv[3], NULL, 0, &len) != napi_ok) TRANSFORM_FAIL("runScriptTransformEngine: inputName must be a string");
  w->input_name = malloc(len + 1);
  if (w->input_name == NULL) TRANSFORM_FAIL("OOM");
  if (napi_get_value_string_utf8(env, argv[3], w->input_name, len + 1, NULL) != napi_ok) TRANSFORM_FAIL("runScriptTransformEngine: failed to read inputName");

  if (napi_get_value_string_utf8(env, argv[4], NULL, 0, &len) != napi_ok) TRANSFORM_FAIL("runScriptTransformEngine: inputMimeType must be a string");
  w->input_mime_type = malloc(len + 1);
  if (w->input_mime_type == NULL) TRANSFORM_FAIL("OOM");
  if (napi_get_value_string_utf8(env, argv[4], w->input_mime_type, len + 1, NULL) != napi_ok) TRANSFORM_FAIL("runScriptTransformEngine: failed to read inputMimeType");

  napi_valuetype type;
  if (napi_typeof(env, argv[5], &type) != napi_ok) TRANSFORM_FAIL("runScriptTransformEngine: invalid inputCharset argument");
  if (type == napi_string) {
    if (napi_get_value_string_utf8(env, argv[5], NULL, 0, &len) != napi_ok) TRANSFORM_FAIL("runScriptTransformEngine: inputCharset must be a string");
    w->input_charset = malloc(len + 1);
    if (w->input_charset == NULL) TRANSFORM_FAIL("OOM");
    if (napi_get_value_string_utf8(env, argv[5], w->input_charset, len + 1, NULL) != napi_ok) TRANSFORM_FAIL("runScriptTransformEngine: failed to read inputCharset");
  } else {
    w->input_charset = NULL;
  }
  #undef TRANSFORM_FAIL

  // Round-9 (#3, updated round-11 #2): check each resource creation; on
  // failure release the engine pin (`pinned`, taken at admission) via
  // bridge_end_op, release g_active_ops (verbatim), release any tsfn already
  // created, free w + all five string buffers, and throw. read_tsfn has no
  // context (NULL); write_tsfn holds w as context, so release write_tsfn
  // before freeing w if it was created.
  napi_value resource_name;
  if (napi_create_string_utf8(env, "dwTransform", NAPI_AUTO_LENGTH, &resource_name) != napi_ok) {
    free(w->script); free(w->inputs_json); free(w->input_name); free(w->input_mime_type); free(w->input_charset); free(w);
    bridge_end_op(pinned, /*env_still_alive=*/true);
    uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "runScriptTransformEngine: failed to create resource name");
    return NULL;
  }

  if (napi_create_threadsafe_function(env, argv[6], NULL, resource_name, 0, 1, NULL, NULL, NULL, call_js_read, &w->read_tsfn) != napi_ok) {
    free(w->script); free(w->inputs_json); free(w->input_name); free(w->input_mime_type); free(w->input_charset); free(w);
    bridge_end_op(pinned, /*env_still_alive=*/true);
    uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "runScriptTransformEngine: failed to create read threadsafe function");
    return NULL;
  }
  if (napi_create_threadsafe_function(env, argv[7], NULL, resource_name, 0, 1, NULL, NULL, w, call_js_transform_write, &w->write_tsfn) != napi_ok) {
    napi_release_threadsafe_function(w->read_tsfn, napi_tsfn_release);
    free(w->script); free(w->inputs_json); free(w->input_name); free(w->input_mime_type); free(w->input_charset); free(w);
    bridge_end_op(pinned, /*env_still_alive=*/true);
    uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "runScriptTransformEngine: failed to create write threadsafe function");
    return NULL;
  }

  napi_value promise;
  if (napi_create_promise(env, &w->deferred, &promise) != napi_ok) {
    napi_release_threadsafe_function(w->read_tsfn, napi_tsfn_release);
    napi_release_threadsafe_function(w->write_tsfn, napi_tsfn_release);
    free(w->script); free(w->inputs_json); free(w->input_name); free(w->input_mime_type); free(w->input_charset); free(w);
    bridge_end_op(pinned, /*env_still_alive=*/true);
    uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
    napi_throw_error(env, NULL, "runScriptTransformEngine: failed to create promise");
    return NULL;
  }

  // Round-11 (#2): the pin was taken at admission (bridge_begin_op_locked) in
  // the same critical section as g_active_ops, so a concurrent destroyEngine
  // could never free this bridge under the admitted op. Just record it on w;
  // the completion sentinel releases it via bridge_end_op. NULL for a
  // resolver-less/unknown engine, handled everywhere as a no-op.
  w->bridge = pinned;

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

    // Synchronous call on the JS thread -- env is live here.
    bridge_end_op(w->bridge, /*env_still_alive=*/true);
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
    if (!fn_create_engine) { napi_throw_error(env, NULL, "create_engine not available in native library"); return NULL; }

    // Round-14 (#1): admission in ONE g_mutex critical section (mirrors
    // bridge_finalize_registry). Require (a) a live isolate not past the point
    // of no return, (b) that THIS env owns an init reference (round-13 ownership
    // model: an env with no reference must not create engines on the shared
    // isolate -- it could otherwise attach to an isolate another env is tearing
    // down), and (c) pin the isolate with a g_active_ops reservation so
    // graal_tear_down_isolate() cannot run across the attach/create below. The
    // check and the g_active_ops++ cannot be split by a teardown because every
    // teardown transition and the g_active_ops==0 fast path also hold g_mutex.
    uv_mutex_lock(&g_mutex);
    env_init_rec_t* self = env_init_rec_find_locked(env);
    if (!g_initialized || g_isolate == NULL ||
        g_teardown_state == TEARDOWN_TEARING_DOWN ||
        self == NULL || self->init_refs == 0) {
        uv_mutex_unlock(&g_mutex);
        napi_throw_error(env, NULL, "Not initialized. Call initialize() first.");
        return NULL;
    }
    g_active_ops++;  // pins the live isolate against teardown across the attach
    uv_mutex_unlock(&g_mutex);

    void* thread = NULL;
    if (fn_attach_thread(g_isolate, &thread) != 0) {
        uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
        napi_throw_error(env, NULL, "Failed to attach thread"); return NULL;
    }
    long long handle = fn_create_engine(thread);
    fn_detach_thread(thread);
    // A GraalVM @CEntryPoint that throws on the Java side returns the return
    // type's default value instead of propagating the exception — 0 for a
    // long long. The real handle registry only ever hands out handles >= 1, so
    // any handle <= 0 means construction failed; never hand that back to JS as
    // if it were usable.
    if (handle <= 0) {
        uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
        napi_throw_error(env, NULL, "create_engine returned an invalid handle"); return NULL;
    }

    // Round-9 (#1): every engine -- resolver-backed or not -- gets a per-engine
    // record so destroyEngine can defer the registry removal (fn_destroy_engine)
    // until this engine's in-flight streaming/transform ops drain. A resolver-less
    // record leaves resolver_js/results NULL. Round-11 (#1): it now ALSO registers
    // an env cleanup hook (mirroring napi_create_engine_with_resolver), because
    // without one a Worker that creates a resolver-less engine and exits without
    // destroyEngine() would strand this record, the Java registry entry, and the
    // native-lib reference. Round-12 (#2) closed the record/registry gap via
    // bridge_finalize; round-13 (#5) moved ownership of the native-lib
    // initialize() reference to the env itself (env_init_rec), released by the
    // env-death hook env_init_cleanup, not per-engine.
    // owner is recorded for symmetry but is NOT used to restrict destruction based
    // on resolver state (see the owner guard in napi_destroy_engine, which now
    // fires for any record).
    engine_bridge_t* rec = (engine_bridge_t*)calloc(1, sizeof(engine_bridge_t));
    if (rec == NULL) {
        // Roll back the engine we just created so we don't leak a registered but
        // unrecorded handle. fn_destroy_engine attaches its own thread.
        if (fn_destroy_engine) {
            void* t2 = NULL;
            if (fn_attach_thread(g_isolate, &t2) == 0) { fn_destroy_engine(t2, handle); fn_detach_thread(t2); }
        }
        uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
        napi_throw_error(env, NULL, "Failed to allocate engine record");
        return NULL;
    }
    rec->handle = handle;
    rec->owner = uv_thread_self();
    rec->env = env;
    uv_mutex_lock(&g_mutex); rec->next = g_bridges; g_bridges = rec; uv_mutex_unlock(&g_mutex);
    // Round-11 (#1): register an env cleanup hook for EVERY engine, not just
    // resolver-backed ones. Without it, a Worker that creates a resolver-less
    // engine and exits without destroyEngine() would strand this record, the Java
    // ScriptRuntime registry entry, and the native-lib reference -- leaking
    // engines and blocking isolate teardown across Worker churn. bridge_env_cleanup
    // + bridge_finalize already handle a resolver-less record (resolver_js == NULL):
    // skip the napi_ref delete, still unlink, remove the registry entry (round-10
    // do_registry_remove=true), and free. Round-13 (#5) moved ownership of the
    // native-lib initialize() reference to the env itself (env_init_rec): this
    // per-engine hook no longer touches g_ref_count -- the reference is released
    // by the env-death hook env_init_cleanup (or by cleanup()), so an abandoned
    // env releases exactly one reference regardless of how many engines it made.
    // destroyEngine removes this hook before an early free so Node never invokes
    // it on freed memory.
    napi_status hook_st = napi_add_env_cleanup_hook(env, bridge_env_cleanup, rec);
    if (hook_st != napi_ok) {
        // Creation must be all-or-nothing (round-12 #6): without a cleanup hook a
        // Worker that abandons this engine would strand the record and the Java
        // registry entry. Unlink, remove the registry entry, free, and throw --
        // no usable handle escapes. The record was just linked on this thread
        // with in_flight==0 and its handle was never returned to JS, so no op
        // can be in flight against it.
        // Do NOT release the init reference here (fix round 1): this throw
        // propagates to initialize()'s TS catch (dataweave.ts), which sees
        // libRefAcquired==true and calls ffi.cleanup() -- that is the ONE
        // release for this creation's ref, matching every sibling
        // creation-failure path (invalid-handle guard, alloc failure) that also
        // leaves the release to the TS catch. Releasing natively here too would
        // double-decrement g_ref_count -- masked in a single-instance process
        // (the guard no-ops a second release at 0) but a live UAF hazard with a
        // second engine instance still holding a reference.
        uv_mutex_lock(&g_mutex);
        engine_bridge_t** pp = &g_bridges;
        while (*pp != NULL) { if (*pp == rec) { *pp = rec->next; break; } pp = &(*pp)->next; }
        uv_mutex_unlock(&g_mutex);
        bridge_finalize_registry(rec);
        bridge_finalize_free(rec, /*env_still_alive=*/true);
        uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
        napi_throw_error(env, NULL, "Failed to register engine cleanup hook");
        return NULL;
    }

    napi_value out; napi_create_int64(env, (int64_t)handle, &out);
    uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
    return out;
}

// createEngineWithResolver(resolver) -> number
static napi_value napi_create_engine_with_resolver(napi_env env, napi_callback_info info) {
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

    // Round-14 (#1): same admission block as napi_create_engine. Taken AFTER the
    // bridge/resolver-ref allocation (those failures touch no isolate state and
    // must not decrement a reservation not yet held) and BEFORE fn_attach_thread.
    uv_mutex_lock(&g_mutex);
    env_init_rec_t* self = env_init_rec_find_locked(env);
    if (!g_initialized || g_isolate == NULL ||
        g_teardown_state == TEARDOWN_TEARING_DOWN ||
        self == NULL || self->init_refs == 0) {
        uv_mutex_unlock(&g_mutex);
        napi_delete_reference(env, bridge->resolver_js); free(bridge);
        napi_throw_error(env, NULL, "Not initialized. Call initialize() first.");
        return NULL;
    }
    g_active_ops++;  // pins the live isolate against teardown across the attach
    uv_mutex_unlock(&g_mutex);

    void* thread = NULL;
    if (fn_attach_thread(g_isolate, &thread) != 0) {
        uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
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
        uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
        // Synchronous call on the JS thread -- env is live here.
        bridge_finalize(bridge, /*env_still_alive=*/true, /*do_registry_remove=*/false);
        napi_throw_error(env, NULL, "create_engine_with_resolver returned an invalid handle");
        return NULL;
    }

    bridge->handle = handle;
    uv_mutex_lock(&g_mutex); bridge->next = g_bridges; g_bridges = bridge; uv_mutex_unlock(&g_mutex);
    // Register a per-env cleanup hook so THIS Worker/main thread disposes this
    // bridge's napi_ref on its own thread when its env tears down (F2). napi_cleanup
    // no longer touches bridge refs. destroyEngine removes this hook before an
    // early free so Node never calls it on freed memory.
    napi_status hook_st = napi_add_env_cleanup_hook(env, bridge_env_cleanup, bridge);
    if (hook_st != napi_ok) {
        // Creation must be all-or-nothing (round-12 #6): without a cleanup hook a
        // Worker that abandons this engine would strand the record and the Java
        // registry entry. Unlink, remove the registry entry, free, and throw --
        // no usable handle escapes. The record was just linked on this thread
        // with in_flight==0 and its handle was never returned to JS, so no op
        // can be in flight against it.
        // Do NOT release the init reference here (fix round 1): this throw
        // propagates to initialize()'s TS catch (dataweave.ts), which sees
        // libRefAcquired==true and calls ffi.cleanup() -- that is the ONE
        // release for this creation's ref, matching every sibling
        // creation-failure path (resolver invalid-handle guard uses
        // bridge_finalize with do_registry_remove=false and also does NOT
        // release) that also leaves the release to the TS catch. Releasing
        // natively here too would double-decrement g_ref_count -- masked in a
        // single-instance process (the guard no-ops a second release at 0) but
        // a live UAF hazard with a second engine instance still holding a
        // reference.
        uv_mutex_lock(&g_mutex);
        engine_bridge_t** pp = &g_bridges;
        while (*pp != NULL) { if (*pp == bridge) { *pp = bridge->next; break; } pp = &(*pp)->next; }
        uv_mutex_unlock(&g_mutex);
        bridge_finalize_registry(bridge);
        bridge_finalize_free(bridge, /*env_still_alive=*/true);
        uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
        napi_throw_error(env, NULL, "Failed to register engine cleanup hook");
        return NULL;
    }
    napi_value out; napi_create_int64(env, (int64_t)handle, &out);
    uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
    return out;
}

// destroyEngine(handle) -> void
static napi_value napi_destroy_engine(napi_env env, napi_callback_info info) {
    if (!g_initialized) return NULL;
    size_t argc = 1; napi_value argv[1];
    napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    if (argc < 1) { napi_throw_error(env, NULL, "destroyEngine requires (handle)"); return NULL; }
    int64_t handle64;
    if (napi_get_value_int64(env, argv[0], &handle64) != napi_ok) {
        napi_throw_error(env, NULL, "destroyEngine: handle must be an integer");
        return NULL;
    }
    long long handle = (long long)handle64;

    // F2: a resolver-backed engine's bridge owns thread-affine N-API state --
    // a napi_ref and an env cleanup hook, both created on the engine's owning
    // JS thread. Deleting that ref (bridge_finalize) or removing that hook
    // (napi_remove_env_cleanup_hook) from another Worker's thread is undefined
    // behavior. Reject cross-thread destruction, mirroring the fail-closed
    // owner check in resolve_module_callback; the owner env's cleanup hook
    // disposes the bridge when that Worker tears down. We are on the owner
    // thread past this point, so the env cannot be concurrently tearing down
    // and the bridge stays stable between this check and the unlink below.
    // Owner-thread guard: round-11 (#1) registers an env cleanup hook for EVERY
    // engine (resolver-backed or not), so every record now carries env-affine
    // N-API state -- napi_remove_env_cleanup_hook (called below before an early
    // free) can only be invoked legally on the owner thread. The guard
    // therefore fires for any record (owned != NULL), not just resolver-backed
    // ones. bridge_finalize's napi_ref deletion stays resolver-gated
    // (resolver_js != NULL && env != NULL) -- that part is unchanged.
    uv_mutex_lock(&g_mutex);
    engine_bridge_t* owned = bridge_find(handle);
    if (owned != NULL) {
        uv_thread_t self = uv_thread_self();
        if (!uv_thread_equal(&self, &owned->owner)) {
            uv_mutex_unlock(&g_mutex);
            napi_throw_error(env, NULL,
                "destroyEngine must be called from the thread that created the engine");
            return NULL;
        }
    }

    // Round-9 (#1): unlink the record and decide, under the lock, whether the
    // registry removal (fn_destroy_engine) and the record free must be DEFERRED.
    // If an op is in flight, its worker may not yet have called
    // ScriptRuntime.get(handle) (the first statement of the Java entrypoint) --
    // removing the registry entry now would make that lookup fail with
    // "Unknown engine handle". So defer BOTH the registry removal and the free
    // to the last op draining (bridge_end_op -> bridge_finalize with
    // do_registry_remove=true), which runs on this same owner thread. When no op
    // is in flight, remove the registry entry and finalize immediately, as
    // before. Every engine now has a record, so `found` is non-NULL for both
    // resolver-backed and resolver-less engines.
    engine_bridge_t** pp = &g_bridges; engine_bridge_t* found = NULL;
    while (*pp != NULL) { if ((*pp)->handle == handle) { found = *pp; *pp = found->next; break; } pp = &(*pp)->next; }
    bool defer = false;
    // deferred_registry_remove gates the deferred registry removal in
    // bridge_end_op; set it together with destroy_pending here.
    if (found != NULL && found->in_flight > 0) { found->destroy_pending = true; found->deferred_registry_remove = true; defer = true; }
    uv_mutex_unlock(&g_mutex);

    if (found != NULL) {
        // Drop the env cleanup hook. Round-11 (#1): every engine now registers
        // one at creation (napi_create_engine / napi_create_engine_with_resolver),
        // so this removal must run unconditionally, not just for resolver-backed
        // engines. Whether we finalize now or defer, the free happens explicitly,
        // so Node must never invoke the hook on this (soon-to-be or already)
        // freed record.
        napi_remove_env_cleanup_hook(env, bridge_env_cleanup, found);
        if (!defer) {
            // Not in flight: remove the registry entry AND finalize now, on this
            // owner thread (env live). do_registry_remove=true folds the
            // fn_destroy_engine call into bridge_finalize so it happens exactly
            // once regardless of path.
            bridge_finalize(found, /*env_still_alive=*/true, /*do_registry_remove=*/true);
        }
        // else: the draining op's bridge_end_op -> bridge_finalize performs both
        // the registry removal and the free (see Step 5).
    } else {
        // No record found (should not happen now that every engine has one, but
        // stay robust to a double-destroy or an unknown handle): fall back to the
        // pre-round-9 behavior of removing the registry entry directly.
        if (fn_destroy_engine) {
            void* thread = NULL;
            if (fn_attach_thread(g_isolate, &thread) == 0) { fn_destroy_engine(thread, handle); fn_detach_thread(thread); }
        }
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
    int64_t handle64;
    if (napi_get_value_int64(env, argv[0], &handle64) != napi_ok) {
        napi_throw_error(env, NULL, "runScriptEngine: handle must be an integer");
        return NULL;
    }
    long long handle = (long long)handle64;

    size_t script_len, inputs_len;
    if (napi_get_value_string_utf8(env, argv[1], NULL, 0, &script_len) != napi_ok) {
        napi_throw_error(env, NULL, "runScriptEngine: script must be a string");
        return NULL;
    }
    if (napi_get_value_string_utf8(env, argv[2], NULL, 0, &inputs_len) != napi_ok) {
        napi_throw_error(env, NULL, "runScriptEngine: inputsJson must be a string");
        return NULL;
    }
    char* script = (char*)malloc(script_len + 1);
    char* inputs = (char*)malloc(inputs_len + 1);
    if (script == NULL || inputs == NULL) { free(script); free(inputs); napi_throw_error(env, NULL, "OOM"); return NULL; }
    if (napi_get_value_string_utf8(env, argv[1], script, script_len + 1, NULL) != napi_ok ||
        napi_get_value_string_utf8(env, argv[2], inputs, inputs_len + 1, NULL) != napi_ok) {
        free(script); free(inputs);
        napi_throw_error(env, NULL, "runScriptEngine: failed to read script/inputsJson");
        return NULL;
    }

    // Round-7 #1: reserve an active op across the isolate-touching window
    // (attach -> run -> detach) so a concurrent Worker's last cleanup()
    // (napi_cleanup Case 4) cannot observe g_active_ops == 0 and tear down
    // g_isolate while this synchronous op is attaching to or executing in it.
    // Reserve LATE (here, not at the top): the malloc/arg-extraction above do
    // not touch the isolate, so the reservation only needs to span attach..
    // detach -- giving exactly two unwind sites (attach-failure and normal
    // completion) instead of also unwinding the OOM path. Rejecting on
    // g_teardown_state != TEARDOWN_NONE also refuses to start once a teardown
    // is queued/underway. run() is fully synchronous on the JS thread, so the
    // reserve and release both happen inline (no worker thread). Admit an
    // ADOPTED isolate: napi_initialize's adoption branch sets
    // g_teardown_cancelled = true on a still-live PENDING_WAIT isolate but
    // does not reset g_teardown_state (only the async waiter does), so a
    // merely-cancelled teardown must not reject here -- otherwise a valid
    // post-adoption op throws "Not initialized". A genuine (non-cancelled)
    // PENDING_WAIT or a committed TEARING_DOWN still rejects.
    uv_mutex_lock(&g_mutex);
    if (!g_initialized || (g_teardown_state != TEARDOWN_NONE && !g_teardown_cancelled)) {
      uv_mutex_unlock(&g_mutex);
      free(script); free(inputs);
      napi_throw_error(env, NULL, "Not initialized. Call initialize() first.");
      return NULL;
    }
    g_active_ops++;
    // Round-11 (#3): pin the engine in the same critical section as the
    // g_active_ops reservation so a concurrent destroyEngine cannot free the
    // resolver bridge (still held by Java as the resolver ctx) while this
    // synchronous op attaches to Graal or runs. NULL for a resolver-less/unknown
    // handle -- bridge_end_op no-ops on NULL. Released in the attach-failure and
    // completion paths below, alongside g_active_ops.
    engine_bridge_t* bridge = bridge_begin_op_locked(handle);
    uv_mutex_unlock(&g_mutex);

    void* thread = NULL;
    if (fn_attach_thread(g_isolate, &thread) != 0) {
      bridge_end_op(bridge, /*env_still_alive=*/true);
      uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);
      free(script); free(inputs);
      napi_throw_error(env, NULL, "Failed to attach thread");
      return NULL;
    }

    char* result = (char*)fn_run_script_engine(thread, handle, script, inputs);

    // The pin taken at admission kept this record alive across the run, so no
    // second lookup is needed. resolver_results_free_all is a no-op for a
    // resolver-less/unknown engine (bridge == NULL).
    if (bridge != NULL) resolver_results_free_all(bridge);

    char* result_copy = result ? strdup(result) : NULL;
    if (result != NULL) fn_free_cstring(thread, result);
    fn_detach_thread(thread);
    free(script); free(inputs);

    // Round-11 (#3): release the per-engine pin (may finalize a destroy that a
    // concurrent Worker deferred while this op held in_flight > 0), then release
    // the global op reservation. env is live on this JS thread, so env_still_alive
    // is true. Order: bridge_end_op before the g_active_ops release, mirroring
    // streaming/transform completion.
    bridge_end_op(bridge, /*env_still_alive=*/true);
    uv_mutex_lock(&g_mutex); g_active_ops--; uv_cond_broadcast(&g_teardown_cond); uv_mutex_unlock(&g_mutex);

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

// `arg` is an int* out-param: the caller (napi_cleanup's case 4) must set it
// to 0 before spawning this thread and read it after uv_thread_join returns.
// Mirrors teardown_waiter_thread_fn's `torn_down` local exactly, so the
// caller can tell "isolate torn down / nothing to tear down" (safe to clear
// g_thread/g_isolate/g_initialized/g_ref_count) apart from "attach failed,
// isolate still alive" (must leave those globals set, or the isolate becomes
// unreachable and can never be torn down).
static void cleanup_thread_fn(void* arg) {
  int* out_torn_down = (int*)arg;
  // graal_tear_down_isolate() must be passed the IsolateThread belonging to the
  // *calling* OS thread. g_thread was created by graal_create_isolate() on the
  // (now-exited, already-joined) init thread, so it is invalid here — passing it
  // trips GraalVM's "wrong IsolateThread" guard and aborts with a fatal
  // StackOverflowError during teardown. Attach this cleanup thread to the isolate
  // to obtain a valid local IsolateThread, then tear down with that.
  if (!fn_tear_down_isolate || !fn_attach_thread || !g_isolate) {
    // Nothing to tear down (no isolate / FFI unavailable) -- safe to clear.
    *out_torn_down = 1;
    return;
  }
  void* local_thread = NULL;
  if (fn_attach_thread(g_isolate, &local_thread) != 0 || local_thread == NULL) {
    // Attach failed -- the isolate is still alive. Leave *out_torn_down at 0
    // (its caller-initialized value) so the caller does NOT clear g_isolate,
    // or it becomes unreachable and can never be torn down.
    return;
  }
  // Check the teardown return code (0 == success). On nonzero the isolate is
  // still live: leave *out_torn_down at 0 so the caller retains
  // g_isolate/g_initialized/g_ref_count and (per its own logic) arms the retry,
  // rather than orphaning a live isolate (review #6 #3). On that failure the
  // isolate was NOT destroyed, so this thread is still attached to it -- detach
  // before the helper thread exits, or the live isolate keeps a phantom
  // attached thread that can make a later retry teardown block or fail (review
  // #7 #1). On success the isolate is gone: do NOT detach (would be a UAF).
  if (fn_tear_down_isolate(local_thread) == 0) {
    *out_torn_down = 1;
  } else {
    fn_detach_thread(local_thread);
    *out_torn_down = 0;
  }
}

// Spawned only when napi_cleanup finds g_active_ops > 0 on the last release
// (case 5 in the design doc). Blocks until every active streaming/transform
// op has drained, performs isolate teardown exactly like cleanup_thread_fn
// does on the unchanged fast path, then resolves every caller who is waiting
// on this same teardown (there may be more than one -- see g_teardown_waiters).
static void teardown_waiter_thread_fn(void* arg) {
  (void)arg;

  uv_mutex_lock(&g_mutex);
  while (g_active_ops > 0 && !g_teardown_cancelled) {
    uv_cond_wait(&g_teardown_cond, &g_mutex);
  }
  bool cancelled = g_teardown_cancelled;
  if (!cancelled) {
    // Point of no return: from here an adopting initialize() must NOT reuse the
    // isolate, so publish TEARING_DOWN under the lock before we drop it to call
    // graal_tear_down_isolate().
    g_teardown_state = TEARDOWN_TEARING_DOWN;
  }
  uv_mutex_unlock(&g_mutex);

  // Perform teardown exactly as the unchanged fast path does: attach a local
  // thread to the isolate (g_thread from graal_create_isolate's bootstrap
  // thread is invalid here -- see cleanup_thread_fn's comment), then tear
  // down. Honor the return code (0 == success); a nonzero teardown leaves the
  // isolate live (review #6 #3). Skipped entirely
  // when an initialize() call adopted the live isolate instead (see
  // napi_initialize's TEARDOWN_PENDING_WAIT branch).
  bool torn_down = false;
  if (!cancelled && fn_tear_down_isolate && fn_attach_thread && g_isolate) {
    void* local_thread = NULL;
    if (fn_attach_thread(g_isolate, &local_thread) == 0 && local_thread != NULL) {
      // Check the teardown return code (0 == success). On nonzero the isolate is
      // still live -- leave torn_down false so the post-teardown block below
      // retains the isolate globals and arms the retry (review #6 #3). On that
      // failure the isolate was NOT destroyed, so this thread is still attached
      // to it -- detach before exiting or the live isolate keeps a phantom
      // attached thread that can block/fail a later retry teardown (review #7
      // #1). On success the isolate is gone: do NOT detach (would be a UAF).
      if (fn_tear_down_isolate(local_thread) == 0) {
        torn_down = true;
      } else {
        fn_detach_thread(local_thread);
        torn_down = false;
      }
    }
    // else: attach failed -- the isolate is still alive. Do NOT clear g_isolate,
    // or it becomes unreachable and can never be torn down.
  } else if (!cancelled) {
    // Nothing to tear down (no isolate / FFI unavailable) -- safe to clear.
    torn_down = true;
  }
  // if (cancelled): leave torn_down = false -- the isolate stays live for the
  // adopter; we tear nothing down.

  uv_mutex_lock(&g_mutex);
  if (!cancelled && torn_down) {
    g_thread = NULL;
    g_isolate = NULL;
    g_initialized = 0;
    g_ref_count = 0;
  } else if (!cancelled && g_isolate != NULL && g_ref_count == 0) {
    // Teardown did not happen (attach failed, or graal_tear_down_isolate
    // returned nonzero -- review #6 #3) and this async-waiter path IS the last
    // release: g_ref_count is already 0 with no owner and no pending waiter.
    // Arm the retry signal so a later op-completion drain or a fresh
    // initialize() retries teardown -- otherwise the live isolate is stranded
    // with nothing to reclaim it (review #6 #4). Mirrors the twin arm in
    // isolate_ref_release_n_locked's waiter-spawn-failure path.
    g_teardown_needed = true;
  }
  // If cancelled: g_isolate/g_initialized/g_ref_count are left exactly as the
  // adopting initialize() set them (it already did g_ref_count++ on the live
  // isolate).
  g_teardown_state = TEARDOWN_NONE;
  g_teardown_cancelled = false;
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
    napi_status enq = napi_call_threadsafe_function(waiters->tsfn, waiters, napi_tsfn_blocking);
    if (enq != napi_ok) {
      // The waiter's env is tearing down (napi_closing): call_js_teardown_done
      // will never run, so it can neither resolve waiter->deferred nor release
      // the tsfn nor free the node. Free the node here instead of leaking it
      // (one leak per Worker that terminated while this teardown was pending).
      // Do NOT napi_release_threadsafe_function(waiters->tsfn, ...): a
      // napi_closing return already discharges this tsfn's registration (Node
      // may have destroyed the tsfn object), so a release would be a
      // double-discharge/UAF -- same reasoning as the sentinel-enqueue-failure
      // paths in streaming_thread_fn/transform_thread_fn. The unresolved
      // deferred is env-affine and reclaimed when the dead env is destroyed.
      free(waiters);
    }
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

  if (napi_create_promise(env, &waiter->deferred, out_promise) != napi_ok) {
    free(waiter);
    napi_throw_error(env, NULL, "Failed to create teardown promise");
    return NULL;
  }

  napi_value resource_name;
  if (napi_create_string_utf8(env, "dwTeardown", NAPI_AUTO_LENGTH, &resource_name) != napi_ok) {
    free(waiter);
    napi_throw_error(env, NULL, "Failed to create teardown resource name");
    return NULL;
  }

  if (napi_create_threadsafe_function(
        env, NULL, NULL, resource_name, 0, 1, NULL, NULL, waiter, call_js_teardown_done, &waiter->tsfn
      ) != napi_ok) {
    free(waiter);
    napi_throw_error(env, NULL, "Failed to create teardown threadsafe function");
    return NULL;
  }

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

// Release n (>=0) initialization references at once, then make the teardown
// decision AT MOST ONCE. Caller holds g_mutex and this KEEPS it held. n==0 is a
// no-op. Equivalent to n serial single-releases for the COUNT, but guarantees
// the reached-zero teardown/waiter logic runs exactly once (a serial loop would
// re-enter the decision on an already-zero count). Used by env_init_cleanup
// (round-13 #5) to release all of a dead env's references from one decision
// point. (Previously also used by a single-release wrapper,
// isolate_ref_release_core_locked, retired in round-13 #5 once the per-engine
// finalize path stopped releasing init references directly.)
// Round-14 (#2/#3): retry a teardown that a prior last-release could not carry
// out. Caller holds g_mutex and this KEEPS it held. No-op unless a stranded
// live isolate is waiting (g_teardown_needed) with no owners and no teardown in
// progress and ops drained. Makes the reached-zero teardown decision at most
// once per call (same synchronous cleanup_thread_fn path as Case 4); on repeated
// failure it leaves g_teardown_needed set to retry on the next drain. Spawns+joins
// cleanup_thread_fn while holding g_mutex, exactly as the Case-4 /
// isolate_ref_release_n_locked g_active_ops==0 branch does; cleanup_thread_fn
// takes no lock and makes no napi call, so this is deadlock-free and thread-safe
// from any drain site.
static void retry_stranded_teardown_locked(void) {
  if (!g_teardown_needed) return;
  if (g_ref_count > 0) { g_teardown_needed = false; return; }  // adopted -> keep
  if (g_teardown_state != TEARDOWN_NONE) return;               // a teardown drives
  if (g_active_ops > 0) return;                                // wait for drain
  if (g_isolate == NULL) { g_teardown_needed = false; return; } // nothing to do
  uv_thread_t tid;
  uv_thread_options_t opts;
  opts.flags = UV_THREAD_HAS_STACK_SIZE;
  opts.stack_size = 2 * 1024 * 1024;
  int torn_down = 0;
  int spawn_rc = uv_thread_create_ex(&tid, &opts, cleanup_thread_fn, &torn_down);
  if (spawn_rc == 0) uv_thread_join(&tid);
  if (torn_down) {
    g_thread = NULL;
    g_isolate = NULL;
    g_initialized = 0;
    g_ref_count = 0;
    g_teardown_needed = false;
  }
  // else: spawn/attach failed again -- leave g_teardown_needed set so the next
  // drain (or a later initialize() adoption) retries.
}

static void isolate_ref_release_n_locked(int n) {
  if (n <= 0) return;
  if (g_ref_count >= n) g_ref_count -= n; else g_ref_count = 0;
  if (g_ref_count > 0) return;              // other envs still hold references
  if (g_teardown_state != TEARDOWN_NONE) return;  // a teardown already drives

  if (g_active_ops == 0) {
    uv_thread_t tid;
    uv_thread_options_t opts;
    opts.flags = UV_THREAD_HAS_STACK_SIZE;
    opts.stack_size = 2 * 1024 * 1024;
    int torn_down = 0;
    int spawn_rc = uv_thread_create_ex(&tid, &opts, cleanup_thread_fn, &torn_down);
    if (spawn_rc == 0) {
      uv_thread_join(&tid);
    }
    if (torn_down) {
      g_thread = NULL;
      g_isolate = NULL;
      g_initialized = 0;
      g_ref_count = 0;
    } else if (g_isolate != NULL && g_ref_count == 0) {
      // Sync teardown failed (spawn or cleanup_thread_fn attach) with the isolate
      // still live and no owners: arm the retry signal (round-14 #3). g_active_ops
      // is already 0 here, but a later op could still re-pin; the flag is cleared
      // on adoption and retried on drain or by the next initialize() (review #6
      // #5). Documented residual: if NO later op or initialize() ever occurs, the
      // isolate lingers until process exit, where the OS reclaims it -- benign
      // (single process-lifetime isolate, no ref-count violation).
      g_teardown_needed = true;
    }
    return;
  }

  // g_active_ops > 0: defer to the waiter thread, no promises attached.
  g_teardown_state = TEARDOWN_PENDING_WAIT;
  g_teardown_cancelled = false;
  g_teardown_waiters = NULL;  // no JS caller waiting
  uv_thread_t waiter_tid;
  uv_thread_options_t waiter_opts;
  waiter_opts.flags = UV_THREAD_HAS_STACK_SIZE;
  waiter_opts.stack_size = 2 * 1024 * 1024;
  int spawn_rc = uv_thread_create_ex(&waiter_tid, &waiter_opts, teardown_waiter_thread_fn, NULL);
  if (spawn_rc != 0) {
    // Best-effort degradation: the waiter thread never started, so nothing will
    // drain the isolate. Restore g_ref_count to the true remaining ownership
    // (Σ init_refs, = 0 here) to keep the invariant, and ARM the retry signal so
    // the next op-completion drain retries teardown -- otherwise this live
    // isolate has zero owners and nothing would ever tear it down (round-14 #3).
    g_teardown_state = TEARDOWN_NONE;
    g_ref_count = env_init_refs_total_locked();
    if (g_isolate != NULL && g_ref_count == 0) g_teardown_needed = true;
  }
}

// Env-death hook for a per-env init record (round-13 #5). Registered once per
// env by initialize()'s first acquire (env_init_acquire_and_hook). Node runs
// env-cleanup hooks LIFO. In the normal initialize()-then-createEngine() order
// this hook is registered BEFORE any engine's bridge_env_cleanup for the same
// env, so it runs AFTER every engine bridge has finalized on a live isolate.
// The pathological raw-ffi order (createEngine() on this env -- succeeding
// because another env already initialized -- THEN initialize() here) can
// register this hook after an engine hook, so it may run first; that is still
// safe, because bridge_finalize_registry re-checks teardown state under g_mutex
// (registry removal no-ops on a torn-down isolate) and the napi_ref delete runs
// with env_still_alive=true on this env's own live thread. Releases exactly the
// references this env still holds (n), from a single env-scoped decision point:
// because g_ref_count == sum of init_refs, releasing this env's n reaches zero
// ONLY if no other env holds a reference, so an abandoned env can never tear the
// isolate down under a live env. Runs on the dying env's own thread with the
// env alive; does only g_mutex-guarded integer/list work + free (no env-affine
// napi calls).
// Round-14 (#1): the create path now enforces per-env ownership (an env with
// init_refs == 0 is rejected), so the pathological order below -- createEngine()
// on this env BEFORE its own initialize() -- is now rejected at the create call
// rather than relying on the finalize-time teardown-state re-check.
static void env_init_cleanup(void* arg) {
    env_init_rec_t* rec = (env_init_rec_t*)arg;
    if (rec == NULL) return;
    uv_mutex_lock(&g_mutex);
    // Unlink from g_env_recs if still present.
    env_init_rec_t** pp = &g_env_recs;
    while (*pp != NULL) {
        if (*pp == rec) { *pp = rec->next; break; }
        pp = &(*pp)->next;
    }
    int n = rec->init_refs;
    rec->init_refs = 0;
    free(rec);
    // Release all n references and make the teardown decision at most once.
    isolate_ref_release_n_locked(n);
    uv_mutex_unlock(&g_mutex);
}

// Promise-less core of an isolate-reference release. Caller holds g_mutex and
// this function KEEPS it held (does not unlock). Decrements g_ref_count and, on
// the last release, drives teardown WITHOUT binding any napi promise/waiter:
//   - g_active_ops == 0 -> synchronous cleanup_thread_fn (same as Case 4).
//   - g_active_ops  > 0 -> spawn the waiter thread with an EMPTY waiter list
//                          (TEARDOWN_PENDING_WAIT); it tears down (or is adopted)
//                          with no promises to resolve.
//   - a teardown already pending (TEARDOWN_NONE != state) -> nothing to do; the
//                          existing waiter will tear down; this release just
//                          drops the count.
// Used by env_init_cleanup (round-13 #5), the env-death hook, which has no
// live JS caller to hand a promise to.
//
// Deliberately does NOT call (or get called by) release_isolate_ref_locked
// below: that promise-bearing sibling needs per-caller promise plumbing this
// core omits on purpose (binding a waiter/promise to a tearing-down env is a
// thread-affinity hazard). They share the last-release *policy* only; see
// release_isolate_ref_locked's header comment for the promise-bearing twin.
//
// The isolate reference is now owned per env (env_init_rec), not per engine
// bridge (round-13 #5): initialize()'s acquire sites and env_init_cleanup are
// the only callers that mutate g_ref_count via this function, alongside
// release_isolate_ref_locked below for the explicit cleanup() path. The
// per-engine finalize path (bridge_env_cleanup / bridge_end_op) no longer
// touches g_ref_count at all, so a raw multi-engine-per-initialize() caller's
// abandoned env fires exactly one release for the whole balance it holds,
// regardless of how many engines it created.

// Releases ONE initialization reference on the shared isolate. Caller MUST
// hold g_mutex; this function UNLOCKS g_mutex before returning (the sync and
// waiter teardown paths both require dropping the lock). Returns the napi
// promise to hand back to the JS caller. This is napi_cleanup's original
// Case 1..5 body.
static napi_value release_isolate_ref_locked(napi_env env) {
  // Case 1/2: not the last release (or nothing was ever initialized). Decrement
  // only if positive -- a second cleanup() call while g_ref_count is already at
  // 0 (e.g. one already dropped it while teardown is pending) must not go
  // negative.
  // Round-13 (#5): an env may release only a reference IT owns. If this env has
  // no outstanding init reference (a cleanup() with no matching initialize() on
  // this env, or a double-cleanup()), do NOT touch g_ref_count -- releasing here
  // would steal another env's reference and could tear the isolate down under a
  // live user. No-op: resolve immediately. (g_ref_count == sum of init_refs, so
  // this env's zero balance means it contributes nothing to release.)
  env_init_rec_t* self = env_init_rec_find_locked(env);
  if (self == NULL || self->init_refs == 0) {
    uv_mutex_unlock(&g_mutex);
    return already_resolved_promise(env);
  }
  self->init_refs--;
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
  if (g_teardown_state != TEARDOWN_NONE) {
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
    // torn_down is cleanup_thread_fn's out-param (mirrors teardown_waiter_thread_fn's
    // `torn_down` local exactly): must be initialized to 0 before the thread runs so
    // the attach-failure early-return path (which never touches it) leaves it false.
    // uv_thread_join is synchronous, so when spawn_rc == 0 this stack variable safely
    // outlives the thread's write to it.
    int torn_down = 0;
    int spawn_rc = uv_thread_create_ex(&tid, &opts, cleanup_thread_fn, &torn_down);
    if (spawn_rc == 0) {
      uv_thread_join(&tid);
    }
    // Only clear global state if the isolate was actually torn down (or there
    // was nothing to tear down). If spawn failed, the thread never ran and
    // torn_down stays 0 -- leave the globals set rather than orphaning a live
    // isolate (unreachable via these globals, could never be torn down), which
    // is a strict improvement over unconditionally clearing them here. Same
    // reasoning for cleanup_thread_fn's internal attach-failure path: the
    // isolate is still alive, g_initialized stays 1, and g_ref_count was
    // already decremented to 0 above without being reset here, so a later
    // initialize() correctly ref-counts the surviving isolate instead of
    // building a second one (identical semantics to teardown_waiter_thread_fn's
    // attach-failure path).
    if (torn_down) {
      g_thread = NULL;
      g_isolate = NULL;
      g_initialized = 0;
      g_ref_count = 0;
    } else if (g_isolate != NULL && g_ref_count == 0) {
      // cleanup_thread_fn spawn/attach failed: the isolate is still live with
      // zero owners. Arm the retry signal so a later op-completion drain or the
      // next initialize() (review #6 #5) tears it down instead of stranding it —
      // mirrors the twin arm in isolate_ref_release_n_locked. Documented residual:
      // if no later op or initialize() ever runs, the isolate lingers to process
      // exit (OS reclaims it) -- benign, no ref-count violation.
      g_teardown_needed = true;
    }
    uv_mutex_unlock(&g_mutex);
    return already_resolved_promise(env);
  }

  // Case 5: last release, but streaming/transform ops are still active.
  // Defer teardown to a dedicated waiter thread instead of blocking this JS
  // thread -- this is the deadlock fix. g_initialized/g_isolate/g_thread stay
  // set until the waiter thread finishes, matching today's behavior of
  // treating "still tearing down" as "still initialized" for concurrent
  // initialize() calls (see Task 3).
  g_teardown_state = TEARDOWN_PENDING_WAIT;
  g_teardown_cancelled = false;
  napi_value promise;
  teardown_waiter_t* waiter = teardown_waiter_create(env, &promise);
  if (waiter == NULL) {
    // The last reference was already dropped (g_ref_count == 0) but we cannot
    // build the waiter to drain the isolate. Arm the retry signal so the op
    // drain retries teardown -- without it this live isolate would have zero
    // owners and nothing to tear it down (round-14 #2).
    g_teardown_state = TEARDOWN_NONE;
    if (g_isolate != NULL && g_ref_count == 0) g_teardown_needed = true;
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
    // will ever clear g_teardown_state, which would otherwise permanently
    // wedge every future initialize()/cleanup() call. Roll back to "teardown
    // did not start" -- the isolate stays up and the caller's promise still
    // resolves, mirroring the fast path's ignore-teardown-return-code posture.
    g_teardown_state = TEARDOWN_NONE;
    g_teardown_waiters = NULL;

    napi_value undefined;
    napi_get_undefined(env, &undefined);
    napi_resolve_deferred(env, waiter->deferred, undefined);

    napi_release_threadsafe_function(waiter->tsfn, napi_tsfn_release);
    free(waiter);

    // Best-effort degradation: the isolate stays live (g_initialized/g_isolate
    // untouched) but no waiter will drain it. Restore g_ref_count to the true
    // remaining ownership (Σ init_refs) rather than a hardcoded 1: this env just
    // decremented its own init_refs above, and reaching Case 5 means g_ref_count
    // hit 0, so the sum is 0 (or whatever surviving envs still own). Hardcoding 1
    // here would strand a reference no env owns -- unreleasable by any cleanup()
    // or env-death hook -- and would break the invariant g_ref_count == Σ
    // init_refs. A later initialize() will re-acquire on the surviving isolate.
    g_ref_count = env_init_refs_total_locked();
    // Arm the retry signal: the isolate stays live with no owners and no waiter,
    // so the op-completion drain must retry teardown (round-14 #2).
    if (g_isolate != NULL && g_ref_count == 0) g_teardown_needed = true;

    uv_mutex_unlock(&g_mutex);
    return promise;
  }

  uv_mutex_unlock(&g_mutex);
  return promise;
}

static napi_value napi_cleanup(napi_env env, napi_callback_info info) {
  (void)info;
  uv_mutex_lock(&g_mutex);
  return release_isolate_ref_locked(env);  // unlocks g_mutex, returns the promise
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
