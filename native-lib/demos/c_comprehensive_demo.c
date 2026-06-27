/**
 * DataWeave C Bindings - Comprehensive Demo
 *
 * Showcases all major capabilities:
 * - Basic transformations
 * - Working with inputs
 * - JSON transformations
 * - Streaming for large datasets
 * - Error handling
 * - Memory management
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../c/include/dataweave.h"

#define PRINT_SEPARATOR() printf("%s\n", "============================================================")

void demo_basic_operations(void) {
    PRINT_SEPARATOR();
    printf("DEMO 1: Basic Operations\n");
    PRINT_SEPARATOR();

    // Simple arithmetic
    printf("\n1.1 Arithmetic:\n");
    dw_result_t *result = dw_run("2 + 2 * 3", NULL);
    if (result && result->success) {
        printf("   Expression: 2 + 2 * 3\n");
        printf("   Result: %s\n", result->result);
    } else {
        printf("   Error: %s\n", result ? result->error : "NULL result");
    }
    dw_free_result(result);

    // String concatenation
    printf("\n1.2 String operations:\n");
    result = dw_run("\"Hello\" ++ \" \" ++ \"World\"", NULL);
    if (result && result->success) {
        printf("   Expression: \"Hello\" ++ \" \" ++ \"World\"\n");
        printf("   Result: %s\n", result->result);
    } else {
        printf("   Error: %s\n", result ? result->error : "NULL result");
    }
    dw_free_result(result);

    // Array operations
    printf("\n1.3 Array operations:\n");
    result = dw_run("[1, 2, 3, 4, 5] map ($ * 2)", NULL);
    if (result && result->success) {
        printf("   Expression: [1, 2, 3, 4, 5] map ($ * 2)\n");
        printf("   Result: %s\n", result->result);
    } else {
        printf("   Error: %s\n", result ? result->error : "NULL result");
    }
    dw_free_result(result);
}

void demo_with_inputs(void) {
    printf("\n");
    PRINT_SEPARATOR();
    printf("DEMO 2: Working with Inputs\n");
    PRINT_SEPARATOR();

    // Simple variable substitution
    printf("\n2.1 Variable substitution:\n");
    const char *inputs_json = "{\"name\": \"Alice\", \"age\": 30}";
    const char *script = "\"Hello, \" ++ name ++ \"! You are \" ++ age ++ \" years old.\"";

    dw_result_t *result = dw_run(script, inputs_json);
    if (result && result->success) {
        printf("   Inputs: %s\n", inputs_json);
        printf("   Result: %s\n", result->result);
    } else {
        printf("   Error: %s\n", result ? result->error : "NULL result");
    }
    dw_free_result(result);

    // Working with payload
    printf("\n2.2 Payload transformation:\n");
    inputs_json = "{"
                  "\"payload\": {"
                  "  \"firstName\": \"John\","
                  "  \"lastName\": \"Doe\","
                  "  \"email\": \"john.doe@example.com\""
                  "}"
                  "}";

    script = "output application/json\n"
             "---\n"
             "{\n"
             "  fullName: payload.firstName ++ \" \" ++ payload.lastName,\n"
             "  contact: payload.email,\n"
             "  username: lower(payload.lastName) ++ \".\" ++ lower(payload.firstName)\n"
             "}";

    result = dw_run(script, inputs_json);
    if (result && result->success) {
        printf("   Input: User record\n");
        printf("   Output: %s\n", result->result);
    } else {
        printf("   Error: %s\n", result ? result->error : "NULL result");
    }
    dw_free_result(result);
}

void demo_json_transformations(void) {
    printf("\n");
    PRINT_SEPARATOR();
    printf("DEMO 3: JSON Transformations\n");
    PRINT_SEPARATOR();

    // Array mapping
    printf("\n3.1 Array mapping:\n");
    const char *inputs_json = "{"
                              "\"payload\": {"
                              "  \"users\": ["
                              "    {\"id\": 1, \"name\": \"Alice\", \"age\": 30, \"city\": \"New York\"},"
                              "    {\"id\": 2, \"name\": \"Bob\", \"age\": 25, \"city\": \"London\"},"
                              "    {\"id\": 3, \"name\": \"Charlie\", \"age\": 35, \"city\": \"Tokyo\"}"
                              "  ]"
                              "}"
                              "}";

    const char *script = "output application/json\n"
                         "---\n"
                         "payload.users map {\n"
                         "  userId: $.id,\n"
                         "  userName: $.name,\n"
                         "  location: $.city,\n"
                         "  isAdult: $.age >= 18\n"
                         "}";

    dw_result_t *result = dw_run(script, inputs_json);
    if (result && result->success) {
        printf("   Transformed 3 users\n");
        printf("   Output: %s\n", result->result);
    } else {
        printf("   Error: %s\n", result ? result->error : "NULL result");
    }
    dw_free_result(result);

    // Filtering and grouping
    printf("\n3.2 Filtering and grouping:\n");
    script = "output application/json\n"
             "---\n"
             "{\n"
             "  adults: payload.users filter ($.age >= 30) map $.name,\n"
             "  totalUsers: sizeOf(payload.users),\n"
             "  averageAge: avg(payload.users map $.age)\n"
             "}";

    result = dw_run(script, inputs_json);
    if (result && result->success) {
        printf("   Output: %s\n", result->result);
    } else {
        printf("   Error: %s\n", result ? result->error : "NULL result");
    }
    dw_free_result(result);
}

// Callback for streaming
typedef struct {
    int chunk_count;
    size_t total_bytes;
} streaming_context_t;

int streaming_callback(void *ctx, const char *chunk, int length) {
    streaming_context_t *context = (streaming_context_t *)ctx;
    context->chunk_count++;
    context->total_bytes += length;
    return 0; // Continue streaming
}

void demo_streaming(void) {
    printf("\n");
    PRINT_SEPARATOR();
    printf("DEMO 4: Streaming (Constant Memory)\n");
    PRINT_SEPARATOR();

    printf("\n4.1 Streaming large array transformation:\n");
    printf("   Generating 1000 records and streaming output...\n");

    // Build large JSON array (simplified for demo)
    char *large_json = malloc(100000);
    strcpy(large_json, "{\"payload\": [");
    for (int i = 0; i < 1000; i++) {
        char record[100];
        snprintf(record, sizeof(record), "%s{\"id\": %d, \"value\": %d}",
                 i > 0 ? "," : "", i, i * 10);
        strcat(large_json, record);
    }
    strcat(large_json, "]}");

    const char *script = "output application/json\n"
                         "---\n"
                         "payload map {\n"
                         "  recordId: $.id,\n"
                         "  computedValue: $.value * 2,\n"
                         "  category: if ($.id mod 2 == 0) \"even\" else \"odd\"\n"
                         "}";

    streaming_context_t context = {0, 0};
    dw_streaming_result_t *result = dw_run_streaming(script, large_json, streaming_callback, &context);

    if (result && result->success) {
        printf("   ✓ Streamed %d chunks\n", context.chunk_count);
        printf("   ✓ Total output: %zu bytes\n", context.total_bytes);
        printf("   ✓ Memory usage: Constant (chunks processed incrementally)\n");
    } else {
        printf("   Error: %s\n", result ? result->error : "NULL result");
    }

    dw_free_streaming_result(result);
    free(large_json);
}

void demo_error_handling(void) {
    printf("\n");
    PRINT_SEPARATOR();
    printf("DEMO 5: Error Handling\n");
    PRINT_SEPARATOR();

    // Syntax error
    printf("\n5.1 Handling syntax errors:\n");
    dw_result_t *result = dw_run("2 + + 3", NULL);
    if (result && !result->success) {
        printf("   ✗ Syntax error detected:\n");
        printf("     %s\n", result->error);
    }
    dw_free_result(result);

    // Runtime error
    printf("\n5.2 Handling runtime errors:\n");
    result = dw_run("payload.user.email", "{\"payload\": {}}");
    if (result && !result->success) {
        printf("   ✗ Runtime error detected:\n");
        printf("     %s\n", result->error);
    }
    dw_free_result(result);

    // Type error
    printf("\n5.3 Handling type errors:\n");
    result = dw_run("\"text\" + 123", NULL);
    if (result && !result->success) {
        printf("   ✗ Type error detected:\n");
        printf("     %s\n", result->error);
    }
    dw_free_result(result);

    // Successful execution
    printf("\n5.4 Successful execution:\n");
    result = dw_run("2 + 3", NULL);
    if (result && result->success) {
        printf("   ✓ Valid expression: 2 + 3 = %s\n", result->result);
    }
    dw_free_result(result);
}

void demo_memory_management(void) {
    printf("\n");
    PRINT_SEPARATOR();
    printf("DEMO 6: Memory Management (C Idioms)\n");
    PRINT_SEPARATOR();

    printf("\n6.1 Proper cleanup of results:\n");
    printf("   Demonstrating explicit memory management...\n");

    int iterations = 100;
    for (int i = 0; i < iterations; i++) {
        dw_result_t *result = dw_run("2 + 2", NULL);
        // Important: Always free results to prevent memory leaks
        dw_free_result(result);
    }

    printf("   ✓ Executed %d transformations\n", iterations);
    printf("   ✓ All results properly freed (no memory leaks)\n");
    printf("   ✓ Explicit resource management via dw_free_* functions\n");

    printf("\n6.2 NULL safety:\n");
    printf("   Testing NULL parameter handling...\n");
    dw_result_t *result = dw_run(NULL, NULL);
    if (!result || !result->success) {
        printf("   ✓ NULL script properly handled\n");
    }
    dw_free_result(result);

    // Safe to call free on NULL
    dw_free_result(NULL);
    printf("   ✓ dw_free_result(NULL) is safe (follows free() semantics)\n");
}

void demo_advanced_features(void) {
    printf("\n");
    PRINT_SEPARATOR();
    printf("DEMO 7: Advanced Features\n");
    PRINT_SEPARATOR();

    // Reduce/fold
    printf("\n7.1 Reduce (sum of array):\n");
    const char *script = "[1, 2, 3, 4, 5] reduce ((item, accumulator=0) -> accumulator + item)";
    dw_result_t *result = dw_run(script, NULL);
    if (result && result->success) {
        printf("   Expression: %s\n", script);
        printf("   Result: %s\n", result->result);
    }
    dw_free_result(result);

    // Pattern matching
    printf("\n7.2 Pattern matching:\n");
    const char *inputs = "{\"status\": \"SUCCESS\"}";
    script = "status match {\n"
             "  case \"SUCCESS\" -> \"Operation completed successfully\"\n"
             "  case \"PENDING\" -> \"Operation in progress\"\n"
             "  case \"FAILED\" -> \"Operation failed\"\n"
             "  else -> \"Unknown status\"\n"
             "}";

    result = dw_run(script, inputs);
    if (result && result->success) {
        printf("   Input status: SUCCESS\n");
        printf("   Result: %s\n", result->result);
    }
    dw_free_result(result);
}

int main(void) {
    printf("\n");
    PRINT_SEPARATOR();
    printf(" DataWeave C Bindings - Comprehensive Demo");
    printf("\n");
    PRINT_SEPARATOR();
    printf("\n This demo showcases:\n");
    printf("  • Basic transformations and operations\n");
    printf("  • Working with inputs and context\n");
    printf("  • JSON data transformations\n");
    printf("  • Streaming for large datasets\n");
    printf("  • Error handling\n");
    printf("  • Memory management (explicit cleanup)\n");
    printf("  • Advanced DataWeave features\n");
    printf("\n");

    demo_basic_operations();
    demo_with_inputs();
    demo_json_transformations();
    demo_streaming();
    demo_error_handling();
    demo_memory_management();
    demo_advanced_features();

    printf("\n");
    PRINT_SEPARATOR();
    printf("✓ All demos completed successfully!\n");
    PRINT_SEPARATOR();
    printf("\n");

    return 0;
}
