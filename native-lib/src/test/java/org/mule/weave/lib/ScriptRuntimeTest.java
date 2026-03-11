package org.mule.weave.lib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Base64;

class ScriptRuntimeTest {

    @Test
    void runSimpleScript() {
        ScriptRuntime runtime = ScriptRuntime.getInstance();
        
        System.out.println("Running sqrt(144) 10 times with timing:");
        System.out.println("=".repeat(50));
        
        for (int i = 1; i <= 20; i++) {
            long startTime = System.nanoTime();
            String result = runtime.run("sqrt(144)");
            long endTime = System.nanoTime();
            double executionTimeMs = (endTime - startTime) / 1_000_000.0;
            
            assertEquals("12", Result.parse(result).result);
            System.out.printf("Run %2d: %.3f ms - Result: %s%n", i, executionTimeMs, result);
        }
        
        System.out.println("=".repeat(50));
    }

    @Test
    void runParseError() {
        ScriptRuntime runtime = ScriptRuntime.getInstance();

        System.out.println("Running sqrt(144) 10 times with timing:");
        System.out.println("=".repeat(50));

        String result = runtime.run("invalid syntax here");

        String error = Result.parse(result).error;
        assertTrue(error.contains("Unable to resolve reference"));
        System.out.printf("Error: %s%n", result);

        System.out.println("=".repeat(50));
    }

    @Test
    void runWithInputs() {
        ScriptRuntime runtime = ScriptRuntime.getInstance();
        
        System.out.println("Testing runWithInputs with two integer numbers:");
        System.out.println("=".repeat(50));
        
        // Test 1: Sum 25 + 17
        int num1 = 25;
        int num2 = 17;
        int expected = num1 + num2;
        
        // Create inputs JSON with content and mimeType for each binding
        String inputsJson = String.format(
            "{\"num1\": {\"content\": \"%s\", \"mimeType\": \"application/json\"}, " +
            "\"num2\": {\"content\": \"%s\", \"mimeType\": \"application/json\"}}",
            encode(num1), encode(num2)
        );
        
        String script = "num1 + num2";
        
        System.out.printf("Test 1: %d + %d%n", num1, num2);
        System.out.printf("Script: %s%n", script);
        System.out.printf("Inputs: %s%n", inputsJson);
        
        long startTime = System.nanoTime();
        String result = Result.parse(runtime.run(script, inputsJson)).result;
        long endTime = System.nanoTime();
        double executionTimeMs = (endTime - startTime) / 1_000_000.0;
        
        System.out.printf("Result: %s%n", result);
        System.out.printf("Expected: %d%n", expected);
        System.out.printf("Execution time: %.3f ms%n", executionTimeMs);
        
        assertEquals(String.valueOf(expected), result);
        System.out.println("✓ Test 1 passed!");
        
        System.out.println("-".repeat(50));
        
        // Test 2: Sum 100 + 250
        num1 = 100;
        num2 = 250;
        expected = num1 + num2;
        
        inputsJson = String.format(
            "{\"num1\": {\"content\": \"%s\", \"mimeType\": \"application/json\"}, " +
            "\"num2\": {\"content\": \"%s\", \"mimeType\": \"application/json\"}}",
            encode(num1), encode(num2)
        );
        
        System.out.printf("Test 2: %d + %d%n", num1, num2);
        System.out.printf("Script: %s%n", script);
        
        startTime = System.nanoTime();
        result = Result.parse(runtime.run(script, inputsJson)).result;
        endTime = System.nanoTime();
        executionTimeMs = (endTime - startTime) / 1_000_000.0;
        
        System.out.printf("Result: %s%n", result);
        System.out.printf("Expected: %d%n", expected);
        System.out.printf("Execution time: %.3f ms%n", executionTimeMs);
        
        assertEquals(String.valueOf(expected), result);
        System.out.println("✓ Test 2 passed!");
        
        System.out.println("=".repeat(50));
    }

    private String encode(Object value) {
        byte[] bytes = value instanceof byte[] ? (byte[]) value : String.valueOf(value).getBytes();
        return Base64.getEncoder().encodeToString(bytes);

    }

    @Test
    void runWithXmlInput() {
        ScriptRuntime runtime = ScriptRuntime.getInstance();
        
        System.out.println("Testing runWithInputs with XML input to calculate average age:");
        System.out.println("=".repeat(50));
        
        // XML input with two people
        String xmlInput = """
            <people>
                <person>
                    <age>19</age>
                    <name>john</name>
                </person>
                <person>
                    <age>25</age>
                    <name>jane</name>
                </person>
            </people>
            """;

        String inputsJson = String.format(
            "{\"people\": {\"content\": \"%s\", \"mimeType\": \"application/xml\"}}",
            encode(xmlInput)
        );
        
        // DataWeave script to calculate average age
        String script = """
            output application/json
            ---
            avg(people.people.*person.age)
            """;
        
        System.out.printf("XML Input:%n%s%n", xmlInput);
        System.out.printf("Script:%n%s%n", script);
        
        long startTime = System.nanoTime();
        String result = runtime.run(script, inputsJson);
        long endTime = System.nanoTime();
        double executionTimeMs = (endTime - startTime) / 1_000_000.0;
        
        System.out.printf("Result: %s%n", result);
        System.out.printf("Expected: 22 (average of 19 and 25)%n");
        System.out.printf("Execution time: %.3f ms%n", executionTimeMs);
        
        // The average of 19 and 25 is 22
        assertEquals("22", Result.parse(result).result);
        System.out.println("✓ Test passed!");
        
        System.out.println("=".repeat(50));
    }

    @Test
    void runWithJsonObjectInput() {
        ScriptRuntime runtime = ScriptRuntime.getInstance();
        
        System.out.println("Testing runWithInputs with JSON object input:");
        System.out.println("=".repeat(50));
        
        String jsonInput = "{\"name\": \"John\", \"age\": 30}";
        
        String inputsJson = String.format(
            "{\"payload\": {\"content\": \"%s\", \"mimeType\": \"application/json\"}}",
            encode(jsonInput)
        );

        // DataWeave script to extract name
        String script = "output application/json\n---\npayload.name";
        
        System.out.printf("JSON Input: %s%n", jsonInput);
        System.out.printf("Script: %s%n", script);
        
        long startTime = System.nanoTime();
        String result = Result.parse(runtime.run(script, inputsJson)).result;
        long endTime = System.nanoTime();
        double executionTimeMs = (endTime - startTime) / 1_000_000.0;
        
        System.out.printf("Result: %s%n", result);
        System.out.printf("Expected: \"John\"%n");
        System.out.printf("Execution time: %.3f ms%n", executionTimeMs);
        
        assertEquals("\"John\"", result);
        System.out.println("✓ Test passed!");
        
        System.out.println("=".repeat(50));
    }

    @Test
    void runWithBinaryResult() {
        ScriptRuntime runtime = ScriptRuntime.getInstance();

        System.out.println("Running fromBase64 10 times with timing:");
        System.out.println("=".repeat(50));

        for (int i = 1; i <= 1; i++) {
            long startTime = System.nanoTime();
            Result result = Result.parse(runtime.run("import fromBase64 from dw::core::Binaries\n" +
                    "output application/octet-stream\n" +
                    "---\n" +
                    "fromBase64(\"12345678\")", ""));
            long endTime = System.nanoTime();
            double executionTimeMs = (endTime - startTime) / 1_000_000.0;

            assertEquals("12345678", result.result);
            System.out.printf("Run %2d: %.3f ms - Result: %s%n", i, executionTimeMs, result.result);
        }

        System.out.println("=".repeat(50));
    }

    @Test
    void runWithInputProperties() {
        ScriptRuntime runtime = ScriptRuntime.getInstance();
        String encodedIn0 = Base64.getEncoder().encodeToString("1234567".getBytes());
        Result result = Result.parse(runtime.run("in0.column_1[0] as Number",
                "{\"in0\": " +
                        "{\"content\": \"" + encodedIn0 + "\", " +
                        "\"mimeType\": \"application/csv\", " +
                        "\"properties\": {\"header\": false, \"separator\": \"4\"}}}"));
        assertEquals("567", result.result);

    }

    @Test
    void streamSimpleScript() throws IOException {
        ScriptRuntime runtime = ScriptRuntime.getInstance();

        System.out.println("Testing streaming simple script:");
        System.out.println("=".repeat(50));

        StreamSession session = runtime.runStreaming("sqrt(144)", null);
        assertFalse(session.isError(), "Expected successful session");
        assertNull(session.getError());
        assertNotNull(session.getMimeType());

        byte[] buf = new byte[64];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int n;
        while ((n = session.read(buf, buf.length)) > 0) {
            bos.write(buf, 0, n);
        }
        String result = bos.toString(session.getCharset());
        assertEquals("12", result);
        StreamSession.close(session.register()); // clean up handle

        System.out.println("Result: " + result);
        System.out.println("✓ Streaming simple script passed!");
        System.out.println("=".repeat(50));
    }

    @Test
    void streamWithInputs() throws IOException {
        ScriptRuntime runtime = ScriptRuntime.getInstance();

        System.out.println("Testing streaming with inputs:");
        System.out.println("=".repeat(50));

        String inputsJson = String.format(
            "{\"num1\": {\"content\": \"%s\", \"mimeType\": \"application/json\"}, " +
            "\"num2\": {\"content\": \"%s\", \"mimeType\": \"application/json\"}}",
            encode(25), encode(17)
        );

        StreamSession session = runtime.runStreaming("num1 + num2", inputsJson);
        assertFalse(session.isError());

        byte[] buf = new byte[64];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int n;
        while ((n = session.read(buf, buf.length)) > 0) {
            bos.write(buf, 0, n);
        }
        String result = bos.toString(session.getCharset());
        assertEquals("42", result);
        StreamSession.close(session.register());

        System.out.println("Result: " + result);
        System.out.println("✓ Streaming with inputs passed!");
        System.out.println("=".repeat(50));
    }

    @Test
    void streamChunkedRead() throws IOException {
        ScriptRuntime runtime = ScriptRuntime.getInstance();

        System.out.println("Testing streaming chunked read:");
        System.out.println("=".repeat(50));

        String script = "output application/json\n---\n{items: (1 to 100) map {id: $, name: \"item_\" ++ $}}";

        StreamSession session = runtime.runStreaming(script, null);
        assertFalse(session.isError());

        byte[] smallBuf = new byte[32];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int n;
        int chunkCount = 0;
        while ((n = session.read(smallBuf, smallBuf.length)) > 0) {
            bos.write(smallBuf, 0, n);
            chunkCount++;
        }
        String result = bos.toString(session.getCharset());
        assertTrue(chunkCount > 1, "Expected multiple chunks, got " + chunkCount);
        assertTrue(result.contains("item_1"));
        assertTrue(result.contains("item_100"));
        StreamSession.close(session.register());

        System.out.printf("Read %d chunks, total %d bytes%n", chunkCount, bos.size());
        System.out.println("✓ Streaming chunked read passed!");
        System.out.println("=".repeat(50));
    }

    @Test
    void streamErrorSession() {
        ScriptRuntime runtime = ScriptRuntime.getInstance();

        System.out.println("Testing streaming error session:");
        System.out.println("=".repeat(50));

        StreamSession session = runtime.runStreaming("invalid syntax here", null);
        assertTrue(session.isError());
        assertNotNull(session.getError());
        assertTrue(session.getError().contains("Unable to resolve reference"));

        System.out.println("Error: " + session.getError());
        System.out.println("✓ Streaming error session passed!");
        System.out.println("=".repeat(50));
    }

    static class Result {
        boolean success;
        String result;
        String error;
        boolean binary;
        String mimeType;
        String charset;

        static Result parse(String json) {
            Result result = new Result();

            String successString = json.substring(json.indexOf(":") + 1, json.indexOf(","));
            result.success = Boolean.parseBoolean(successString);
            if (result.success) {
                String binaryString = json.substring(json.indexOf(",\"binary\":") + 10, json.indexOf("}"));
                result.binary = Boolean.parseBoolean(binaryString);
                String resultString = json.substring(json.indexOf(",\"result\":") + 11, json.indexOf(",\"mimeType\":")-1);
                String mimeTypeString = json.substring(json.indexOf(",\"mimeType\":") + 13, json.indexOf(",\"charset\":")-1);
                result.mimeType = mimeTypeString;
                String charsetString = json.substring(json.indexOf(",\"charset\":") + 12, json.indexOf(",\"binary\":")-1);
                result.charset = charsetString;
                if (result.binary) {
                    result.result = resultString;
                } else {
                    result.result = new String(Base64.getDecoder().decode(resultString), Charset.forName(result.charset));
                }

            } else {
                result.error = json.substring(json.indexOf(",\"error\":") + 10, json.length()-2);
            }
            return result;
        }
    }

}
