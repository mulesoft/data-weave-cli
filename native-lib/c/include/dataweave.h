/*
 * DataWeave C API
 *
 * High-level C wrapper for the DataWeave native library (dwlib).
 * Provides clean, well-documented APIs for executing DataWeave scripts
 * with proper error handling and resource management.
 *
 * Basic usage:
 *   dw_runtime *runtime = dw_init();
 *   if (!runtime) {
 *       fprintf(stderr, "Failed to initialize: %s\n", dw_get_last_error());
 *       return 1;
 *   }
 *
 *   dw_execution_result *result = dw_run(runtime, "2 + 2", NULL);
 *   if (result && result->success) {
 *       printf("Result: %s\n", dw_result_get_string(result));
 *   } else {
 *       fprintf(stderr, "Error: %s\n", result ? result->error : "Unknown");
 *   }
 *   dw_free_result(result);
 *   dw_cleanup(runtime);
 *
 * Thread safety:
 *   - dw_init() is NOT thread-safe, call once per process/thread
 *   - dw_run() and variants are thread-safe when using separate runtimes
 *   - Do not share a single runtime across threads
 */

#ifndef DATAWEAVE_H
#define DATAWEAVE_H

#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Version information */
#define DW_VERSION_MAJOR 0
#define DW_VERSION_MINOR 1
#define DW_VERSION_PATCH 0

/* Opaque types */
typedef struct dw_runtime dw_runtime;
typedef struct dw_execution_result dw_execution_result;
typedef struct dw_streaming_result dw_streaming_result;
typedef struct dw_stream dw_stream;

/* Callback types */

/**
 * Write callback for streaming output.
 *
 * @param ctx User-provided context pointer
 * @param buffer Output data (NOT null-terminated)
 * @param length Size of buffer in bytes
 * @return 0 on success, non-zero to abort execution
 */
typedef int (*dw_write_callback)(void *ctx, const char *buffer, int length);

/**
 * Read callback for streaming input.
 *
 * @param ctx User-provided context pointer
 * @param buffer Buffer to write input data into
 * @param buffer_size Maximum bytes that can be written to buffer
 * @return Number of bytes written, 0 on EOF, -1 on error
 */
typedef int (*dw_read_callback)(void *ctx, char *buffer, int buffer_size);

/* Runtime management */

/**
 * Initialize a DataWeave runtime.
 * Creates a GraalVM isolate and attaches the calling thread.
 *
 * @return Runtime handle on success, NULL on error
 *         Call dw_get_last_error() for error details
 *
 * Thread safety: NOT thread-safe
 */
dw_runtime *dw_init(void);

/**
 * Initialize a DataWeave runtime with an explicit library path.
 *
 * @param lib_path Absolute path to dwlib shared library (dylib/so/dll)
 * @return Runtime handle on success, NULL on error
 *
 * Thread safety: NOT thread-safe
 */
dw_runtime *dw_init_with_path(const char *lib_path);

/**
 * Clean up and destroy a DataWeave runtime.
 * Tears down the GraalVM isolate and releases all resources.
 * The runtime handle becomes invalid after this call.
 *
 * @param runtime Runtime to destroy (can be NULL)
 *
 * Thread safety: NOT thread-safe, do not call while other threads use runtime
 */
void dw_cleanup(dw_runtime *runtime);

/**
 * Get the last error message from the most recent API call.
 * The returned string is valid until the next API call that can fail.
 *
 * @return Error message or NULL if no error occurred
 *
 * Thread safety: Thread-local storage, safe to call from multiple threads
 */
const char *dw_get_last_error(void);

/* Basic execution */

/**
 * Execute a DataWeave script with optional inputs.
 *
 * @param runtime Initialized runtime
 * @param script DataWeave script source (UTF-8)
 * @param inputs_json JSON object mapping input names to descriptors (can be NULL)
 *                    Format: {"name": {"content": "<base64>", "mimeType": "...", ...}}
 * @return Result object on success, NULL on error
 *         Caller must free with dw_free_result()
 *
 * Thread safety: Safe when using separate runtimes per thread
 */
dw_execution_result *dw_run(dw_runtime *runtime, const char *script, const char *inputs_json);

/**
 * Free an execution result.
 *
 * @param result Result to free (can be NULL)
 */
void dw_free_result(dw_execution_result *result);

/* Result accessors */

/**
 * Check if execution succeeded.
 *
 * @param result Result object
 * @return true if successful, false on error
 */
bool dw_result_success(const dw_execution_result *result);

/**
 * Get error message from a failed execution.
 *
 * @param result Result object
 * @return Error message or NULL if no error
 */
const char *dw_result_error(const dw_execution_result *result);

/**
 * Get the raw base64-encoded result.
 *
 * @param result Result object
 * @return Base64-encoded result or NULL
 */
const char *dw_result_get_encoded(const dw_execution_result *result);

/**
 * Get the decoded result as bytes.
 *
 * @param result Result object
 * @param out_size Output parameter for byte count (can be NULL)
 * @return Pointer to decoded bytes or NULL
 *         Valid until result is freed
 */
const unsigned char *dw_result_get_bytes(const dw_execution_result *result, size_t *out_size);

/**
 * Get the decoded result as a string.
 * For text results, decodes using the result's charset.
 *
 * @param result Result object
 * @return Null-terminated string or NULL
 *         Valid until result is freed
 */
const char *dw_result_get_string(const dw_execution_result *result);

/**
 * Get the MIME type of the result.
 *
 * @param result Result object
 * @return MIME type string or NULL
 */
const char *dw_result_mime_type(const dw_execution_result *result);

/**
 * Get the charset of the result.
 *
 * @param result Result object
 * @return Charset string (e.g., "UTF-8") or NULL
 */
const char *dw_result_charset(const dw_execution_result *result);

/**
 * Check if the result is binary.
 *
 * @param result Result object
 * @return true if binary, false if text
 */
bool dw_result_is_binary(const dw_execution_result *result);

/* Streaming output */

/**
 * Execute a script and stream output chunks as they are produced.
 *
 * @param runtime Initialized runtime
 * @param script DataWeave script source
 * @param inputs_json Optional input bindings (can be NULL)
 * @return Stream handle on success, NULL on error
 *         Caller must free with dw_stream_free()
 *
 * Thread safety: Safe when using separate runtimes per thread
 */
dw_stream *dw_run_streaming(dw_runtime *runtime, const char *script, const char *inputs_json);

/**
 * Read the next chunk from a stream.
 *
 * @param stream Stream handle
 * @param out_buffer Pointer to receive chunk data (NOT null-terminated)
 * @param out_size Pointer to receive chunk size
 * @return 1 if chunk was read, 0 on EOF (stream complete), -1 on error
 */
int dw_stream_next(dw_stream *stream, const unsigned char **out_buffer, size_t *out_size);

/**
 * Get metadata after stream completes.
 * Only valid after dw_stream_next() returns 0 (EOF).
 *
 * @param stream Stream handle
 * @return Streaming result metadata or NULL if stream not complete
 *         Valid until stream is freed
 */
const dw_streaming_result *dw_stream_metadata(dw_stream *stream);

/**
 * Free a stream.
 *
 * @param stream Stream to free (can be NULL)
 */
void dw_stream_free(dw_stream *stream);

/* Streaming result accessors */

/**
 * Check if streaming execution succeeded.
 */
bool dw_streaming_result_success(const dw_streaming_result *result);

/**
 * Get error message from a failed streaming execution.
 */
const char *dw_streaming_result_error(const dw_streaming_result *result);

/**
 * Get the MIME type from streaming metadata.
 */
const char *dw_streaming_result_mime_type(const dw_streaming_result *result);

/**
 * Get the charset from streaming metadata.
 */
const char *dw_streaming_result_charset(const dw_streaming_result *result);

/**
 * Check if the streaming result is binary.
 */
bool dw_streaming_result_is_binary(const dw_streaming_result *result);

/* Callback-based output streaming */

/**
 * Execute a script and stream output via callback.
 *
 * @param runtime Initialized runtime
 * @param script DataWeave script source
 * @param callback Write callback to receive output chunks
 * @param ctx User context pointer passed to callback
 * @param inputs_json Optional input bindings (can be NULL)
 * @return Streaming result metadata on success, NULL on error
 *         Caller must free with dw_free_streaming_result()
 *
 * Thread safety: Safe when using separate runtimes per thread
 */
dw_streaming_result *dw_run_callback(
    dw_runtime *runtime,
    const char *script,
    dw_write_callback callback,
    void *ctx,
    const char *inputs_json
);

/**
 * Free a streaming result.
 */
void dw_free_streaming_result(dw_streaming_result *result);

/* Bidirectional streaming */

/**
 * Execute a script with streaming input and output.
 *
 * Input is pulled via read_callback (invoked on background thread).
 * Output is pushed to write_callback (invoked on calling thread).
 *
 * @param runtime Initialized runtime
 * @param script DataWeave script source
 * @param read_callback Callback to supply input data
 * @param write_callback Callback to receive output chunks
 * @param input_name Binding name for streamed input (e.g., "payload")
 * @param input_mime_type MIME type of streamed input
 * @param input_charset Charset of streamed input (can be NULL for UTF-8)
 * @param ctx User context pointer passed to both callbacks
 * @param inputs_json Optional additional input bindings (can be NULL)
 * @return Streaming result metadata on success, NULL on error
 *         Caller must free with dw_free_streaming_result()
 *
 * Thread safety: read_callback invoked on background thread,
 *                write_callback invoked on calling thread
 */
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
);

/* Utility functions */

/**
 * Encode data to base64.
 *
 * @param data Input data
 * @param size Size of input in bytes
 * @return Null-terminated base64 string
 *         Caller must free with dw_free_string()
 */
char *dw_base64_encode(const unsigned char *data, size_t size);

/**
 * Decode base64 string.
 *
 * @param encoded Base64-encoded string
 * @param out_size Output parameter for decoded size
 * @return Decoded data
 *         Caller must free with dw_free_bytes()
 */
unsigned char *dw_base64_decode(const char *encoded, size_t *out_size);

/**
 * Free a string allocated by the DataWeave library.
 */
void dw_free_string(char *str);

/**
 * Free bytes allocated by the DataWeave library.
 */
void dw_free_bytes(unsigned char *bytes);

/**
 * Create a JSON input descriptor for a string value.
 *
 * @param name Input binding name
 * @param content String content
 * @param mime_type MIME type (can be NULL for default)
 * @return JSON string for use with inputs_json parameter
 *         Caller must free with dw_free_string()
 */
char *dw_create_input_string(const char *name, const char *content, const char *mime_type);

/**
 * Create a JSON input descriptor for binary data.
 *
 * @param name Input binding name
 * @param data Binary data
 * @param size Size of data in bytes
 * @param mime_type MIME type (can be NULL for default)
 * @param charset Charset (can be NULL for UTF-8)
 * @return JSON string for use with inputs_json parameter
 *         Caller must free with dw_free_string()
 */
char *dw_create_input_bytes(
    const char *name,
    const unsigned char *data,
    size_t size,
    const char *mime_type,
    const char *charset
);

#ifdef __cplusplus
}
#endif

#endif /* DATAWEAVE_H */
