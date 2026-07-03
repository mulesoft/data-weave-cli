/*
 * Comprehensive test suite for DataWeave C API
 * Matches all test cases from the Python implementation
 */

#include "../include/dataweave.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/* Platform-specific includes */
#ifndef _WIN32
#include <unistd.h>
#endif

/* Test result tracking */
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST_START(name) \
    printf("\n" name "...\n")

#define TEST_OK(name) \
    do { \
        printf("[OK] " name "\n"); \
        tests_passed++; \
    } while(0)

#define TEST_FAIL(name, msg, ...) \
    do { \
        printf("[FAIL] " name ": " msg "\n", ##__VA_ARGS__); \
        tests_failed++; \
    } while(0)

#define ASSERT_TRUE(cond, msg, ...) \
    if (!(cond)) { \
        TEST_FAIL("assertion failed", msg, ##__VA_ARGS__); \
        return false; \
    }

#define ASSERT_NOT_NULL(ptr, msg) \
    if (!(ptr)) { \
        TEST_FAIL("assertion failed", msg); \
        return false; \
    }

#define ASSERT_STR_CONTAINS(haystack, needle) \
    if (!(haystack) || !strstr((haystack), (needle))) { \
        TEST_FAIL("assertion failed", "Expected '%s' to contain '%s'", (haystack), (needle)); \
        return false; \
    }

#define ASSERT_STR_EQUALS(actual, expected) \
    if (!(actual) || strcmp((actual), (expected)) != 0) { \
        TEST_FAIL("assertion failed", "Expected '%s', got '%s'", (expected), (actual)); \
        return false; \
    }

/* Test basic script execution */
static bool test_basic(dw_runtime *runtime) {
    TEST_START("Testing basic script execution");

    dw_execution_result *result = dw_run(runtime, "2 + 2", NULL);
    ASSERT_NOT_NULL(result, "Result is NULL");
    ASSERT_TRUE(dw_result_success(result), "Execution failed: %s", dw_result_error(result));

    const char *output = dw_result_get_string(result);
    ASSERT_NOT_NULL(output, "Output is NULL");
    ASSERT_STR_EQUALS(output, "4");

    dw_free_result(result);
    TEST_OK("Basic script execution works");
    return true;
}

/* Test script with inputs */
static bool test_with_inputs(dw_runtime *runtime) {
    TEST_START("Testing script with inputs");

    /* Create inputs JSON */
    const char *inputs = "{"
        "\"num1\": {\"content\": \"MjU=\", \"mimeType\": \"application/json\", \"charset\": \"UTF-8\"},"
        "\"num2\": {\"content\": \"MTc=\", \"mimeType\": \"application/json\", \"charset\": \"UTF-8\"}"
    "}";

    dw_execution_result *result = dw_run(runtime, "num1 + num2", inputs);
    ASSERT_NOT_NULL(result, "Result is NULL");
    ASSERT_TRUE(dw_result_success(result), "Execution failed: %s", dw_result_error(result));

    const char *output = dw_result_get_string(result);
    ASSERT_NOT_NULL(output, "Output is NULL");
    ASSERT_STR_EQUALS(output, "42");

    dw_free_result(result);
    TEST_OK("Script with inputs works");
    return true;
}

/* Test encoding conversion */
static bool test_encoding(dw_runtime *runtime) {
    TEST_START("Testing encoding (UTF-16 XML -> CSV)");

    /* Read UTF-16 XML file. Try several relative paths so the test works
     * regardless of where it is invoked from (CMake build dir, c/ dir, etc.). */
    const char *candidates[] = {
        "../../python/tests/person.xml",          /* run from c/build/ */
        "../python/tests/person.xml",             /* run from c/ */
        "python/tests/person.xml",                /* run from native-lib/ */
        "tests/person.xml",                       /* run from c/ alt */
        "person.xml",                             /* CWD fallback */
        NULL,
    };
    FILE *f = NULL;
    for (int i = 0; candidates[i] != NULL; i++) {
        f = fopen(candidates[i], "rb");
        if (f) break;
    }
    if (!f) {
        TEST_FAIL("test_encoding", "Could not open person.xml");
        return false;
    }

    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);

    unsigned char *xml_data = malloc(file_size);
    fread(xml_data, 1, file_size, f);
    fclose(f);

    /* Encode to base64 */
    char *encoded = dw_base64_encode(xml_data, file_size);
    free(xml_data);
    ASSERT_NOT_NULL(encoded, "Base64 encoding failed");

    /* Create input JSON */
    char inputs[4096];
    snprintf(inputs, sizeof(inputs),
        "{\"payload\": {\"content\": \"%s\", \"mimeType\": \"application/xml\", \"charset\": \"UTF-16\"}}",
        encoded);
    dw_free_string(encoded);

    const char *script = "output application/csv header=true\n---\n[payload.person]";

    dw_execution_result *result = dw_run(runtime, script, inputs);
    ASSERT_NOT_NULL(result, "Result is NULL");
    ASSERT_TRUE(dw_result_success(result), "Execution failed: %s", dw_result_error(result));

    const char *output = dw_result_get_string(result);
    ASSERT_NOT_NULL(output, "Output is NULL");
    ASSERT_STR_CONTAINS(output, "name");
    ASSERT_STR_CONTAINS(output, "age");
    ASSERT_STR_CONTAINS(output, "Billy");
    ASSERT_STR_CONTAINS(output, "31");

    dw_free_result(result);
    TEST_OK("Encoding conversion works");
    return true;
}

/* Test auto-conversion using helper function */
static bool test_auto_conversion(dw_runtime *runtime) {
    TEST_START("Testing auto-conversion");

    /* Create array input */
    const char *array_json = "[1, 2, 3]";
    char *inputs = dw_create_input_string("numbers", array_json, "application/json");
    ASSERT_NOT_NULL(inputs, "Failed to create input");

    dw_execution_result *result = dw_run(runtime, "numbers[0]", inputs);
    dw_free_string(inputs);

    ASSERT_NOT_NULL(result, "Result is NULL");
    ASSERT_TRUE(dw_result_success(result), "Execution failed: %s", dw_result_error(result));

    const char *output = dw_result_get_string(result);
    ASSERT_NOT_NULL(output, "Output is NULL");
    ASSERT_STR_EQUALS(output, "1");

    dw_free_result(result);
    TEST_OK("Auto-conversion works");
    return true;
}

/* Callback output context */
typedef struct {
    unsigned char *buffer;
    size_t size;
    size_t capacity;
} callback_buffer;

static int write_callback(void *ctx, const char *buffer, int length) {
    callback_buffer *cb = (callback_buffer *)ctx;

    /* Resize if needed */
    if (cb->size + length > cb->capacity) {
        size_t new_capacity = cb->capacity * 2 + length;
        unsigned char *new_buffer = realloc(cb->buffer, new_capacity);
        if (!new_buffer) return -1;
        cb->buffer = new_buffer;
        cb->capacity = new_capacity;
    }

    memcpy(cb->buffer + cb->size, buffer, length);
    cb->size += length;
    return 0;
}

/* Test callback-based output */
static bool test_callback_output_basic(dw_runtime *runtime) {
    TEST_START("Testing callback output basic");

    callback_buffer cb = {0};
    cb.capacity = 256;
    cb.buffer = malloc(cb.capacity);

    dw_streaming_result *result = dw_run_callback(runtime, "2 + 2", write_callback, &cb, NULL);
    ASSERT_NOT_NULL(result, "Result is NULL");
    ASSERT_TRUE(dw_streaming_result_success(result), "Execution failed: %s",
                dw_streaming_result_error(result));

    /* Null-terminate the buffer */
    cb.buffer[cb.size] = '\0';
    ASSERT_STR_EQUALS((char *)cb.buffer, "4");

    free(cb.buffer);
    dw_free_streaming_result(result);
    TEST_OK("Callback output basic works");
    return true;
}

/* Test callback output with inputs */
static bool test_callback_output_with_inputs(dw_runtime *runtime) {
    TEST_START("Testing callback output with inputs");

    callback_buffer cb = {0};
    cb.capacity = 256;
    cb.buffer = malloc(cb.capacity);

    const char *inputs = "{"
        "\"num1\": {\"content\": \"MjU=\", \"mimeType\": \"application/json\"},"
        "\"num2\": {\"content\": \"MTc=\", \"mimeType\": \"application/json\"}"
    "}";

    dw_streaming_result *result = dw_run_callback(runtime, "num1 + num2", write_callback, &cb, inputs);
    ASSERT_NOT_NULL(result, "Result is NULL");
    ASSERT_TRUE(dw_streaming_result_success(result), "Execution failed: %s",
                dw_streaming_result_error(result));

    cb.buffer[cb.size] = '\0';
    ASSERT_STR_EQUALS((char *)cb.buffer, "42");

    free(cb.buffer);
    dw_free_streaming_result(result);
    TEST_OK("Callback output with inputs works");
    return true;
}

/* Bidirectional streaming context */
typedef struct {
    const char *input_data;
    size_t input_size;
    size_t input_pos;
    callback_buffer output;
} transform_context;

static int read_callback(void *ctx, char *buffer, int buffer_size) {
    transform_context *tc = (transform_context *)ctx;

    if (tc->input_pos >= tc->input_size) {
        return 0;  /* EOF */
    }

    size_t remaining = tc->input_size - tc->input_pos;
    size_t to_read = remaining < (size_t)buffer_size ? remaining : (size_t)buffer_size;

    memcpy(buffer, tc->input_data + tc->input_pos, to_read);
    tc->input_pos += to_read;

    return (int)to_read;
}

static int transform_write_callback(void *ctx, const char *buffer, int length) {
    transform_context *tc = (transform_context *)ctx;
    return write_callback(&tc->output, buffer, length);
}

/* Test callback input+output */
static bool test_callback_input_output(dw_runtime *runtime) {
    TEST_START("Testing callback input+output");

    transform_context tc = {0};
    tc.input_data = "[10, 20, 30, 40, 50]";
    tc.input_size = strlen(tc.input_data);
    tc.output.capacity = 256;
    tc.output.buffer = malloc(tc.output.capacity);

    const char *script = "output application/json\n---\npayload map ($ * 2)";

    dw_streaming_result *result = dw_run_transform(
        runtime,
        script,
        read_callback,
        transform_write_callback,
        "payload",
        "application/json",
        NULL,
        &tc,
        NULL
    );

    ASSERT_NOT_NULL(result, "Result is NULL");
    ASSERT_TRUE(dw_streaming_result_success(result), "Execution failed: %s",
                dw_streaming_result_error(result));

    tc.output.buffer[tc.output.size] = '\0';
    const char *output = (const char *)tc.output.buffer;

    ASSERT_STR_CONTAINS(output, "20");
    ASSERT_STR_CONTAINS(output, "100");

    free(tc.output.buffer);
    dw_free_streaming_result(result);
    TEST_OK("Callback input+output works");
    return true;
}

/* Test callback input+output with large data */
static bool test_callback_input_output_large(dw_runtime *runtime) {
    TEST_START("Testing callback input+output large");

    /* Build large JSON array */
    char *input = malloc(50000);
    char *p = input;
    p += sprintf(p, "[");
    for (int i = 1; i <= 1000; i++) {
        if (i > 1) p += sprintf(p, ",");
        p += sprintf(p, "{\"id\":%d}", i);
    }
    sprintf(p, "]");

    transform_context tc = {0};
    tc.input_data = input;
    tc.input_size = strlen(input);
    tc.output.capacity = 256;
    tc.output.buffer = malloc(tc.output.capacity);

    const char *script = "output application/json\n---\nsizeOf(payload)";

    dw_streaming_result *result = dw_run_transform(
        runtime,
        script,
        read_callback,
        transform_write_callback,
        "payload",
        "application/json",
        NULL,
        &tc,
        NULL
    );

    ASSERT_NOT_NULL(result, "Result is NULL");
    ASSERT_TRUE(dw_streaming_result_success(result), "Execution failed: %s",
                dw_streaming_result_error(result));

    tc.output.buffer[tc.output.size] = '\0';
    ASSERT_STR_EQUALS((char *)tc.output.buffer, "1000");

    free(input);
    free(tc.output.buffer);
    dw_free_streaming_result(result);
    TEST_OK("Callback input+output large works");
    return true;
}

/* Test error handling */
static bool test_error_handling(dw_runtime *runtime) {
    TEST_START("Testing error handling");

    dw_execution_result *result = dw_run(runtime, "invalid syntax here", NULL);
    ASSERT_NOT_NULL(result, "Result is NULL");
    ASSERT_TRUE(!dw_result_success(result), "Expected failure");
    ASSERT_NOT_NULL(dw_result_error(result), "Expected error message");

    dw_free_result(result);
    TEST_OK("Error handling works");
    return true;
}

/* Test helper functions */
static bool test_helpers(void) {
    TEST_START("Testing helper functions");

    /* Test base64 encoding/decoding */
    const char *original = "Hello, World!";
    char *encoded = dw_base64_encode((const unsigned char *)original, strlen(original));
    ASSERT_NOT_NULL(encoded, "Encoding failed");

    size_t decoded_size;
    unsigned char *decoded = dw_base64_decode(encoded, &decoded_size);
    ASSERT_NOT_NULL(decoded, "Decoding failed");
    ASSERT_TRUE(decoded_size == strlen(original), "Size mismatch");
    ASSERT_TRUE(memcmp(decoded, original, decoded_size) == 0, "Content mismatch");

    dw_free_string(encoded);
    dw_free_bytes(decoded);

    /* Test input creation helpers */
    char *input_json = dw_create_input_string("test", "hello", "text/plain");
    ASSERT_NOT_NULL(input_json, "Input creation failed");
    ASSERT_STR_CONTAINS(input_json, "test");
    ASSERT_STR_CONTAINS(input_json, "text/plain");
    dw_free_string(input_json);

    TEST_OK("Helper functions work");
    return true;
}

/* Main test runner */
int main(void) {
    printf("======================================================================\n");
    printf("DataWeave C API - Test Suite\n");
    printf("======================================================================\n");

    /* Initialize runtime */
    dw_runtime *runtime = dw_init();
    if (!runtime) {
        fprintf(stderr, "\n[ERROR] Failed to initialize DataWeave runtime: %s\n", dw_get_last_error());
        fprintf(stderr, "\nPlease ensure:\n");
        fprintf(stderr, "  1. The native library is built: ./gradlew nativeCompile\n");
        fprintf(stderr, "  2. DATAWEAVE_NATIVE_LIB is set or dwlib is in current directory\n");
        return 2;
    }

    printf("\n[OK] Runtime initialized\n");

    /* Run tests */
    test_basic(runtime);
    test_with_inputs(runtime);
    test_encoding(runtime);
    test_auto_conversion(runtime);
    test_callback_output_basic(runtime);
    test_callback_output_with_inputs(runtime);
    test_callback_input_output(runtime);
    test_callback_input_output_large(runtime);
    test_error_handling(runtime);
    test_helpers();

    /* Cleanup */
    dw_cleanup(runtime);

    /* Print results */
    printf("\n======================================================================\n");
    printf("Results: %d/%d tests passed\n", tests_passed, tests_passed + tests_failed);
    printf("======================================================================\n");

    if (tests_failed == 0) {
        printf("\n[OK] All tests passed!\n");
        return 0;
    } else {
        printf("\n[FAIL] %d test(s) failed\n", tests_failed);
        return 1;
    }
}
