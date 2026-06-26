/*
 * Simple example demonstrating DataWeave C API usage
 */

#include <dataweave.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(void) {
    printf("DataWeave C API - Simple Example\n");
    printf("==================================\n\n");

    /* Initialize runtime */
    printf("Initializing DataWeave runtime...\n");
    dw_runtime *runtime = dw_init();
    if (!runtime) {
        fprintf(stderr, "Failed to initialize: %s\n", dw_get_last_error());
        fprintf(stderr, "\nPlease ensure:\n");
        fprintf(stderr, "  1. Build the native library: ./gradlew :native-lib:nativeCompile\n");
        fprintf(stderr, "  2. Set DATAWEAVE_NATIVE_LIB or copy dwlib to current directory\n");
        return 1;
    }
    printf("Runtime initialized successfully\n\n");

    /* Example 1: Basic arithmetic */
    printf("Example 1: Basic arithmetic\n");
    printf("---------------------------\n");
    printf("Script: 2 + 2\n");

    dw_execution_result *result = dw_run(runtime, "2 + 2", NULL);
    if (result && dw_result_success(result)) {
        printf("Result: %s\n", dw_result_get_string(result));
    } else {
        fprintf(stderr, "Error: %s\n", result ? dw_result_error(result) : "Unknown");
    }
    dw_free_result(result);
    printf("\n");

    /* Example 2: Script with inputs */
    printf("Example 2: Script with inputs\n");
    printf("-----------------------------\n");
    printf("Script: num1 + num2\n");
    printf("Inputs: num1=25, num2=17\n");

    /* Create inputs using helper function */
    char *inputs1 = dw_create_input_string("num1", "25", "application/json");
    char *inputs2 = dw_create_input_string("num2", "17", "application/json");

    /* Combine into single JSON object (simplified for demo) */
    const char *inputs = "{"
        "\"num1\": {\"content\": \"MjU=\", \"mimeType\": \"application/json\"},"
        "\"num2\": {\"content\": \"MTc=\", \"mimeType\": \"application/json\"}"
    "}";

    result = dw_run(runtime, "num1 + num2", inputs);
    if (result && dw_result_success(result)) {
        printf("Result: %s\n", dw_result_get_string(result));
    } else {
        fprintf(stderr, "Error: %s\n", result ? dw_result_error(result) : "Unknown");
    }

    dw_free_string(inputs1);
    dw_free_string(inputs2);
    dw_free_result(result);
    printf("\n");

    /* Example 3: Array operations */
    printf("Example 3: Array operations\n");
    printf("---------------------------\n");
    printf("Script: numbers map ($ * 2)\n");
    printf("Input: [1, 2, 3, 4, 5]\n");

    char *array_input = dw_create_input_string("numbers", "[1, 2, 3, 4, 5]", "application/json");

    result = dw_run(runtime, "output application/json\n---\nnumbers map ($ * 2)", array_input);
    if (result && dw_result_success(result)) {
        printf("Result: %s\n", dw_result_get_string(result));
    } else {
        fprintf(stderr, "Error: %s\n", result ? dw_result_error(result) : "Unknown");
    }

    dw_free_string(array_input);
    dw_free_result(result);
    printf("\n");

    /* Example 4: Error handling */
    printf("Example 4: Error handling\n");
    printf("-------------------------\n");
    printf("Script: invalid_variable\n");

    result = dw_run(runtime, "invalid_variable", NULL);
    if (!dw_result_success(result)) {
        printf("Expected error caught: %s\n", dw_result_error(result));
    } else {
        printf("Unexpected success\n");
    }
    dw_free_result(result);
    printf("\n");

    /* Example 5: Base64 encoding/decoding */
    printf("Example 5: Base64 utilities\n");
    printf("---------------------------\n");

    const char *original = "Hello, DataWeave!";
    printf("Original: %s\n", original);

    char *encoded = dw_base64_encode((const unsigned char *)original, strlen(original));
    printf("Encoded:  %s\n", encoded);

    size_t decoded_size;
    unsigned char *decoded = dw_base64_decode(encoded, &decoded_size);
    printf("Decoded:  %.*s\n", (int)decoded_size, decoded);

    dw_free_string(encoded);
    dw_free_bytes(decoded);
    printf("\n");

    /* Cleanup */
    printf("Cleaning up...\n");
    dw_cleanup(runtime);
    printf("Done!\n");

    return 0;
}
