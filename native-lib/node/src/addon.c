#include <node_api.h>
#include <uv.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// GraalVM function pointer types
typedef int (*graal_create_isolate_fn)(void*, void**, void**);
typedef int (*graal_attach_thread_fn)(void*, void**);
typedef int (*graal_detach_thread_fn)(void*);
typedef int (*graal_tear_down_isolate_fn)(void*);
typedef void* (*run_script_fn)(void*, const char*, const char*);
typedef void (*free_cstring_fn)(void*, void*);
typedef int (*write_callback_t)(void* ctx, const char* buf, int len);
typedef int (*read_callback_t)(void* ctx, char* buf, int buf_size);
typedef void* (*run_script_callback_fn)(void*, const char*, const char*, write_callback_t, void*);
typedef void* (*run_script_input_output_callback_fn)(void*, const char*, const char*, const char*, const char*, const char*, read_callback_t, write_callback_t, void*);

// Global state
static uv_lib_t g_lib;
static int g_lib_loaded = 0;
static void* g_isolate = NULL;
static void* g_thread = NULL;
static int g_initialized = 0;
static int g_ref_count = 0;
static uv_mutex_t g_mutex;

static graal_create_isolate_fn fn_create_isolate = NULL;
static graal_attach_thread_fn fn_attach_thread = NULL;
static graal_detach_thread_fn fn_detach_thread = NULL;
static graal_tear_down_isolate_fn fn_tear_down_isolate = NULL;
static run_script_fn fn_run_script = NULL;
static free_cstring_fn fn_free_cstring = NULL;
static run_script_callback_fn fn_run_script_callback = NULL;
static run_script_input_output_callback_fn fn_run_script_input_output_callback = NULL;

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

  if (!fn_create_isolate || !fn_run_script || !fn_free_cstring) {
    snprintf(args->error, sizeof(args->error), "Missing required symbols in library");
    args->result = -2;
    return;
  }

  rc = fn_create_isolate(NULL, &g_isolate, &g_thread);
  if (rc != 0) {
    snprintf(args->error, sizeof(args->error), "graal_create_isolate failed with code %d", rc);
    args->result = rc;
    return;
  }

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
  uv_thread_create_ex(&tid, &opts, init_thread_fn, &args);
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
  uv_thread_create_ex(&tid, &opts, run_script_thread_fn, &call_args);
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
  char* script;
  char* inputs_json;
};

static void call_js_write(napi_env env, napi_value js_callback, void* context, void* data) {
  if (env == NULL || data == NULL) return;
  struct chunk_data* chunk = (struct chunk_data*)data;
  struct streaming_work* w = (struct streaming_work*)context;

  if (chunk->len == -1) {
    napi_value result;
    napi_create_string_utf8(env, chunk->buf, strlen(chunk->buf), &result);
    napi_resolve_deferred(env, w->deferred, result);

    free(chunk->buf);
    free(chunk);
    free(w->script);
    free(w->inputs_json);

    uv_thread_join(&w->tid);
    napi_release_threadsafe_function(w->tsfn, napi_tsfn_release);
    free(w);
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
    void* result_ptr = fn_run_script_callback(
      worker_thread, w->script, w->inputs_json, streaming_write_cb, (void*)w->tsfn
    );
    if (result_ptr) {
      meta_result = strdup((const char*)result_ptr);
      fn_free_cstring(worker_thread, result_ptr);
    } else {
      meta_result = strdup("{\"success\":false,\"error\":\"Empty response\"}");
    }
    fn_detach_thread(worker_thread);
  }

  struct chunk_data* sentinel = malloc(sizeof(struct chunk_data));
  sentinel->buf = meta_result;
  sentinel->len = -1;
  napi_call_threadsafe_function(w->tsfn, sentinel, napi_tsfn_blocking);
}

static napi_value napi_run_script_streaming(napi_env env, napi_callback_info info) {
  if (!g_initialized) {
    napi_throw_error(env, NULL, "Not initialized. Call initialize() first.");
    return NULL;
  }
  if (!fn_run_script_callback) {
    napi_throw_error(env, NULL, "run_script_callback not available in native library");
    return NULL;
  }

  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  if (argc < 3) {
    napi_throw_error(env, NULL, "runScriptStreaming requires (script, inputsJson, chunkCallback)");
    return NULL;
  }

  size_t script_len, inputs_len;
  napi_get_value_string_utf8(env, argv[0], NULL, 0, &script_len);
  napi_get_value_string_utf8(env, argv[1], NULL, 0, &inputs_len);

  struct streaming_work* w = calloc(1, sizeof(struct streaming_work));
  w->script = malloc(script_len + 1);
  w->inputs_json = malloc(inputs_len + 1);
  napi_get_value_string_utf8(env, argv[0], w->script, script_len + 1, NULL);
  napi_get_value_string_utf8(env, argv[1], w->inputs_json, inputs_len + 1, NULL);

  napi_value resource_name;
  napi_create_string_utf8(env, "dwStreaming", NAPI_AUTO_LENGTH, &resource_name);
  napi_create_threadsafe_function(env, argv[2], NULL, resource_name, 0, 1, NULL, NULL, w, call_js_write, &w->tsfn);

  napi_value promise;
  napi_create_promise(env, &w->deferred, &promise);

  uv_thread_options_t opts;
  opts.flags = UV_THREAD_HAS_STACK_SIZE;
  opts.stack_size = 2 * 1024 * 1024;
  uv_thread_create_ex(&w->tid, &opts, streaming_thread_fn, w);

  return promise;
}

// --- Bidirectional streaming ---

struct transform_work {
  uv_thread_t tid;
  napi_threadsafe_function read_tsfn;
  napi_threadsafe_function write_tsfn;
  napi_deferred deferred;
  char* script;
  char* inputs_json;
  char* input_name;
  char* input_mime_type;
  char* input_charset;
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
  if (env == NULL || data == NULL) return;
  struct read_request* req = (struct read_request*)data;

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
    req->bytes_read = 0;
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
  if (env == NULL || data == NULL) return;
  struct chunk_data* chunk = (struct chunk_data*)data;
  struct transform_work* w = (struct transform_work*)context;

  if (chunk->len == -1) {
    napi_value result;
    napi_create_string_utf8(env, chunk->buf, strlen(chunk->buf), &result);
    napi_resolve_deferred(env, w->deferred, result);

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
    free(w);
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
    void* result_ptr = fn_run_script_input_output_callback(
      worker_thread, w->script, w->inputs_json,
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

  struct chunk_data* sentinel = malloc(sizeof(struct chunk_data));
  sentinel->buf = meta_result;
  sentinel->len = -1;
  napi_call_threadsafe_function(w->write_tsfn, sentinel, napi_tsfn_blocking);
}

static napi_value napi_run_script_transform(napi_env env, napi_callback_info info) {
  if (!g_initialized) {
    napi_throw_error(env, NULL, "Not initialized. Call initialize() first.");
    return NULL;
  }
  if (!fn_run_script_input_output_callback) {
    napi_throw_error(env, NULL, "run_script_input_output_callback not available in native library");
    return NULL;
  }

  size_t argc = 7;
  napi_value argv[7];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  if (argc < 7) {
    napi_throw_error(env, NULL, "runScriptTransform requires 7 arguments");
    return NULL;
  }

  struct transform_work* w = calloc(1, sizeof(struct transform_work));
  size_t len;

  napi_get_value_string_utf8(env, argv[0], NULL, 0, &len);
  w->script = malloc(len + 1);
  napi_get_value_string_utf8(env, argv[0], w->script, len + 1, NULL);

  napi_get_value_string_utf8(env, argv[1], NULL, 0, &len);
  w->inputs_json = malloc(len + 1);
  napi_get_value_string_utf8(env, argv[1], w->inputs_json, len + 1, NULL);

  napi_get_value_string_utf8(env, argv[2], NULL, 0, &len);
  w->input_name = malloc(len + 1);
  napi_get_value_string_utf8(env, argv[2], w->input_name, len + 1, NULL);

  napi_get_value_string_utf8(env, argv[3], NULL, 0, &len);
  w->input_mime_type = malloc(len + 1);
  napi_get_value_string_utf8(env, argv[3], w->input_mime_type, len + 1, NULL);

  napi_valuetype type;
  napi_typeof(env, argv[4], &type);
  if (type == napi_string) {
    napi_get_value_string_utf8(env, argv[4], NULL, 0, &len);
    w->input_charset = malloc(len + 1);
    napi_get_value_string_utf8(env, argv[4], w->input_charset, len + 1, NULL);
  } else {
    w->input_charset = NULL;
  }

  napi_value resource_name;
  napi_create_string_utf8(env, "dwTransform", NAPI_AUTO_LENGTH, &resource_name);

  napi_create_threadsafe_function(env, argv[5], NULL, resource_name, 0, 1, NULL, NULL, NULL, call_js_read, &w->read_tsfn);
  napi_create_threadsafe_function(env, argv[6], NULL, resource_name, 0, 1, NULL, NULL, w, call_js_transform_write, &w->write_tsfn);

  napi_value promise;
  napi_create_promise(env, &w->deferred, &promise);

  uv_thread_options_t opts;
  opts.flags = UV_THREAD_HAS_STACK_SIZE;
  opts.stack_size = 2 * 1024 * 1024;
  uv_thread_create_ex(&w->tid, &opts, transform_thread_fn, w);

  return promise;
}

// --- Cleanup (must run on a separate thread to avoid V8 signal handler conflict) ---

static void cleanup_thread_fn(void* arg) {
  (void)arg;
  if (fn_tear_down_isolate && g_thread) {
    fn_tear_down_isolate(g_thread);
  }
}

static napi_value napi_cleanup(napi_env env, napi_callback_info info) {
  uv_mutex_lock(&g_mutex);
  if (g_initialized) {
    g_ref_count--;
    if (g_ref_count <= 0) {
      uv_thread_t tid;
      uv_thread_options_t opts;
      opts.flags = UV_THREAD_HAS_STACK_SIZE;
      opts.stack_size = 2 * 1024 * 1024;
      uv_thread_create_ex(&tid, &opts, cleanup_thread_fn, NULL);
      uv_thread_join(&tid);

      g_thread = NULL;
      g_isolate = NULL;
      g_initialized = 0;
      g_ref_count = 0;
    }
  }
  uv_mutex_unlock(&g_mutex);
  return NULL;
}

// --- Module init ---

static napi_value Init(napi_env env, napi_value exports) {
  uv_mutex_init(&g_mutex);

  napi_value fn;

  napi_create_function(env, "initialize", NAPI_AUTO_LENGTH, napi_initialize, NULL, &fn);
  napi_set_named_property(env, exports, "initialize", fn);

  napi_create_function(env, "runScript", NAPI_AUTO_LENGTH, dw_napi_run_script, NULL, &fn);
  napi_set_named_property(env, exports, "runScript", fn);

  napi_create_function(env, "runScriptStreaming", NAPI_AUTO_LENGTH, napi_run_script_streaming, NULL, &fn);
  napi_set_named_property(env, exports, "runScriptStreaming", fn);

  napi_create_function(env, "runScriptTransform", NAPI_AUTO_LENGTH, napi_run_script_transform, NULL, &fn);
  napi_set_named_property(env, exports, "runScriptTransform", fn);

  napi_create_function(env, "cleanup", NAPI_AUTO_LENGTH, napi_cleanup, NULL, &fn);
  napi_set_named_property(env, exports, "cleanup", fn);

  return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
