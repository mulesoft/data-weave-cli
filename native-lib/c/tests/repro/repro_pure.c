/*
 * repro_pure.c — regression tests for the pure (no-dwlib) C wrapper bugs.
 *
 * These exercise dataweave.c helpers that need no native library:
 *   [5]  dw_base64_decode accepts a non-trailing '=' and returns garbage
 *   [13] dw_base64_decode coerces an invalid sextet to 0 instead of rejecting
 *   [12] json_get_string does not unescape JSON escapes (\n, \t, \")
 *
 * We #include the implementation directly so we can reach the static helper
 * json_get_string. Each check asserts the CORRECT behavior, so it PASSES when
 * the bug is fixed.
 *
 * Exit code = number of findings still broken (0 == all fixed).
 */

#include "../../src/dataweave.c"

#include <stdio.h>

static int failed = 0;

static void report(const char *id, int fixed, const char *desc) {
    if (fixed) {
        printf("  [%s] PASS — %s\n", id, desc);
    } else {
        printf("  [%s] FAIL — %s\n", id, desc);
        failed++;
    }
}

int main(void) {
    printf("== Pure C wrapper regression tests ==\n");

    /* [5] Non-trailing '=' in a c/d position should be rejected.
     * "AB=DEFGH" is length 8 (valid quantum count); the '=' at index 2 sits in
     * the sextet_c slot of the FIRST quad, which is not the final quantum.
     * Correct base64 decoders reject '=' anywhere but as trailing padding. */
    {
        size_t n = 12345;
        unsigned char *out = dw_base64_decode("AB=DEFGH", &n);
        int fixed = (out == NULL); /* correct behavior is NULL */
        report("5", fixed,
               "dw_base64_decode(\"AB=DEFGH\") correctly returns NULL (mid-string '=' rejected)");
        free(out);
    }

    /* [13] An invalid (non-base64) character in a c/d slot should be rejected.
     * '-' is not in the base64 alphabet. Correct behavior: reject -> NULL. */
    {
        size_t n = 12345;
        unsigned char *out = dw_base64_decode("AB-DEFGH", &n);
        int fixed = (out == NULL); /* correct behavior is NULL */
        report("13", fixed,
               "dw_base64_decode(\"AB-DEFGH\") correctly returns NULL (invalid sextet rejected)");
        free(out);
    }

    /* [12] json_get_string should unescape standard JSON escapes.
     * For {"error":"a\nb"} the value should decode to a<newline>b (3 chars). */
    {
        /* The JSON literally contains: {"error":"a\nb"}  (backslash + n) */
        const char *json = "{\"error\":\"a\\nb\"}";
        char *val = json_get_string(json, "error");
        /* Correct: val == "a\nb" (3 bytes, index 1 is a real newline 0x0A).
         * Buggy:   val == "a\\nb" (4 bytes, index 1 is a backslash 0x5C). */
        int fixed = (val != NULL && strlen(val) == 3 && val[1] == '\n');
        report("12", fixed,
               "json_get_string correctly unescapes backslash-n to newline");
        free(val);
    }

    printf("== %d finding(s) still broken, %d fixed ==\n", failed, 3 - failed);
    return failed;
}
