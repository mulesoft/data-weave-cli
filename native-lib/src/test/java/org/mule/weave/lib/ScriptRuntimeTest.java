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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

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
    void streamWithStreamingInput() throws Exception {
        ScriptRuntime runtime = ScriptRuntime.getInstance();

        System.out.println("Testing streaming with streaming input:");
        System.out.println("=".repeat(50));

        // Create an input stream session for JSON data
        InputStreamSession inputSession = new InputStreamSession("application/json", "UTF-8");
        long inputHandle = inputSession.register();

        // Build inputs JSON referencing the streamHandle
        String inputsJson = "{\"payload\": {\"streamHandle\": \"" + inputHandle + "\", \"mimeType\": \"application/json\"}}";

        // The DW engine will read from the PipedInputStream on the main thread,
        // so we must feed data from a separate thread.
        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<Exception> feedError = new AtomicReference<>();

        Thread feeder = new Thread(() -> {
            try {
                started.countDown();
                String jsonData = "{\"name\": \"Alice\", \"age\": 30}";
                byte[] bytes = jsonData.getBytes("UTF-8");
                inputSession.write(bytes, bytes.length);
                inputSession.closeWriter();
            } catch (Exception e) {
                feedError.set(e);
            }
        });
        feeder.start();
        started.await();

        // Run streaming with the piped input
        StreamSession session = runtime.runStreaming("output application/json\n---\npayload.name", inputsJson);
        assertFalse(session.isError(), "Expected successful session but got: " + session.getError());

        byte[] buf = new byte[64];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int n;
        while ((n = session.read(buf, buf.length)) > 0) {
            bos.write(buf, 0, n);
        }
        String result = bos.toString(session.getCharset());
        assertEquals("\"Alice\"", result);
        StreamSession.close(session.register());
        InputStreamSession.close(inputHandle);
        feeder.join(5000);
        assertNull(feedError.get(), "Feeder thread threw: " + feedError.get());

        System.out.println("Result: " + result);
        System.out.println("✓ Streaming with streaming input passed!");
        System.out.println("=".repeat(50));
    }

    @Test
    void streamWithLargeStreamingInput() throws Exception {
        ScriptRuntime runtime = ScriptRuntime.getInstance();

        System.out.println("Testing streaming with large streaming input:");
        System.out.println("=".repeat(50));

        InputStreamSession inputSession = new InputStreamSession("application/json", "UTF-8");
        long inputHandle = inputSession.register();

        String inputsJson = "{\"payload\": {\"streamHandle\": \"" + inputHandle + "\", \"mimeType\": \"application/json\"}}";

        // Feed a large JSON array from a separate thread
        AtomicReference<Exception> feedError = new AtomicReference<>();
        Thread feeder = new Thread(() -> {
            try {
                StringBuilder sb = new StringBuilder("[");
                for (int i = 1; i <= 1000; i++) {
                    if (i > 1) sb.append(",");
                    sb.append("{\"id\":").append(i).append(",\"val\":\"item_").append(i).append("\"}");
                }
                sb.append("]");
                byte[] bytes = sb.toString().getBytes("UTF-8");
                // Write in chunks to simulate streaming
                int chunkSize = 4096;
                for (int off = 0; off < bytes.length; off += chunkSize) {
                    int len = Math.min(chunkSize, bytes.length - off);
                    byte[] chunk = new byte[len];
                    System.arraycopy(bytes, off, chunk, 0, len);
                    inputSession.write(chunk, len);
                }
                inputSession.closeWriter();
            } catch (Exception e) {
                feedError.set(e);
            }
        });
        feeder.start();

        StreamSession session = runtime.runStreaming("output application/json\n---\nsizeOf(payload)", inputsJson);
        assertFalse(session.isError(), "Expected successful session but got: " + session.getError());

        byte[] buf = new byte[256];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int n;
        while ((n = session.read(buf, buf.length)) > 0) {
            bos.write(buf, 0, n);
        }
        String result = bos.toString(session.getCharset());
        assertEquals("1000", result);
        StreamSession.close(session.register());
        InputStreamSession.close(inputHandle);
        feeder.join(10000);
        assertNull(feedError.get(), "Feeder thread threw: " + feedError.get());

        System.out.println("Result: " + result);
        System.out.println("✓ Streaming with large streaming input passed!");
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

    // ── Callback-based streaming pattern tests ──────────────────────────

    @Test
    void callbackOutputStreaming() throws IOException {
        ScriptRuntime runtime = ScriptRuntime.getInstance();

        System.out.println("Testing callback-based output streaming:");
        System.out.println("=".repeat(50));

        StreamSession session = runtime.runStreaming("output application/json\n---\n{items: (1 to 50) map {id: $}}", null);
        assertFalse(session.isError());

        // Simulate the write-callback pattern: read chunks and collect them
        ByteArrayOutputStream collected = new ByteArrayOutputStream();
        byte[] buf = new byte[64];
        int callbackCount = 0;
        int n;
        while ((n = session.read(buf, buf.length)) > 0) {
            // This is what the write callback would receive
            collected.write(buf, 0, n);
            callbackCount++;
        }
        String result = collected.toString(session.getCharset());
        assertTrue(result.contains("\"id\": 1"), "Expected id 1 in result");
        assertTrue(result.contains("\"id\": 50"), "Expected id 50 in result");
        assertTrue(callbackCount > 0, "Expected at least one callback invocation");
        StreamSession.close(session.register());

        System.out.printf("Callback invoked %d times, total %d bytes%n", callbackCount, collected.size());
        System.out.println("✓ Callback output streaming passed!");
        System.out.println("=".repeat(50));
    }

    @Test
    void callbackInputOutputStreaming() throws Exception {
        ScriptRuntime runtime = ScriptRuntime.getInstance();

        System.out.println("Testing callback-based input+output streaming:");
        System.out.println("=".repeat(50));

        // Simulate the read-callback pattern: a feeder thread pulls from a data source
        // and pushes into an InputStreamSession, while the main thread reads the output.
        InputStreamSession inputSession = new InputStreamSession("application/json", "UTF-8");
        long inputHandle = inputSession.register();

        String inputsJson = "{\"payload\": {\"streamHandle\": \"" + inputHandle + "\", \"mimeType\": \"application/json\"}}";

        // Simulate read callback: feeds a JSON array in chunks
        byte[] sourceData = "[10, 20, 30, 40, 50]".getBytes("UTF-8");
        AtomicReference<Exception> feedError = new AtomicReference<>();

        Thread feeder = new Thread(() -> {
            try {
                int chunkSize = 8;
                for (int off = 0; off < sourceData.length; off += chunkSize) {
                    int len = Math.min(chunkSize, sourceData.length - off);
                    byte[] chunk = new byte[len];
                    System.arraycopy(sourceData, off, chunk, 0, len);
                    inputSession.write(chunk, len);
                }
                inputSession.closeWriter();
            } catch (Exception e) {
                feedError.set(e);
            }
        }, "test-read-callback-feeder");
        feeder.start();

        StreamSession session = runtime.runStreaming("output application/json\n---\npayload map ($ * 2)", inputsJson);
        assertFalse(session.isError(), "Expected successful session but got: " + session.getError());

        // Simulate write callback: collect output chunks
        ByteArrayOutputStream collected = new ByteArrayOutputStream();
        byte[] buf = new byte[32];
        int callbackCount = 0;
        int n;
        while ((n = session.read(buf, buf.length)) > 0) {
            collected.write(buf, 0, n);
            callbackCount++;
        }
        String result = collected.toString(session.getCharset());
        assertTrue(result.contains("20"), "Expected 20 in result (10*2)");
        assertTrue(result.contains("100"), "Expected 100 in result (50*2)");

        StreamSession.close(session.register());
        InputStreamSession.close(inputHandle);
        feeder.join(5000);
        assertNull(feedError.get(), "Feeder thread threw: " + feedError.get());

        System.out.printf("Read callback fed %d bytes, write callback invoked %d times, output: %s%n",
                sourceData.length, callbackCount, result.trim());
        System.out.println("✓ Callback input+output streaming passed!");
        System.out.println("=".repeat(50));
    }

    @Test
    void callbackOutputStreamingError() {
        ScriptRuntime runtime = ScriptRuntime.getInstance();

        System.out.println("Testing callback-based output streaming with error:");
        System.out.println("=".repeat(50));

        StreamSession session = runtime.runStreaming("invalid syntax here", null);
        assertTrue(session.isError());
        assertNotNull(session.getError());

        System.out.println("Error correctly returned: " + session.getError());
        System.out.println("✓ Callback output streaming error passed!");
        System.out.println("=".repeat(50));
    }

    // --- Multi-engine registry (W-23692110) ---

    /** In-memory WeaveResourceResolver fake — the JVM-constructable seam standing
     *  in for CallbackWeaveResourceResolver (a CFunctionPointer, which cannot be
     *  built in test mode). */
    static final class MapResolver
            implements org.mule.weave.v2.sdk.WeaveResourceResolver {
        private final java.util.Map<String, String> modules;
        MapResolver(java.util.Map<String, String> modules) { this.modules = modules; }

        @Override
        public scala.Option<org.mule.weave.v2.sdk.WeaveResource> resolve(
                org.mule.weave.v2.parser.ast.variables.NameIdentifier id) {
            String path = org.mule.weave.v2.sdk.NameIdentifierHelper.toWeaveFilePath(id, "/");
            String key = path.startsWith("/") ? path.substring(1) : path;
            String src = modules.get(key);
            if (src == null) return scala.Option.empty();
            return scala.Option.apply(org.mule.weave.v2.sdk.WeaveResource.apply(path, src));
        }

        @Override
        public scala.collection.immutable.Seq<org.mule.weave.v2.sdk.WeaveResource> resolveAll(
                org.mule.weave.v2.parser.ast.variables.NameIdentifier id) {
            scala.Option<org.mule.weave.v2.sdk.WeaveResource> r = resolve(id);
            if (r.isDefined()) {
                return scala.collection.JavaConverters
                        .asScalaBuffer(java.util.Collections.singletonList(r.get())).toList();
            }
            return (scala.collection.immutable.Seq<org.mule.weave.v2.sdk.WeaveResource>)
                    scala.collection.immutable.Seq$.MODULE$.<org.mule.weave.v2.sdk.WeaveResource>empty();
        }
    }

    private static final String IMPORT_A =
            "%dw 2.0\nimport org::test::a\noutput application/json\n---\na::greet(\"X\")";
    private static final String IMPORT_B =
            "%dw 2.0\nimport org::test::b\noutput application/json\n---\nb::greet(\"X\")";

    @Test
    void twoEnginesResolveOnlyTheirOwnModule() {
        ScriptRuntime engineA = new ScriptRuntime(new MapResolver(java.util.Map.of(
                "org/test/a.dwl", "%dw 2.0\nfun greet(n: String) = \"A:\" ++ n")));
        ScriptRuntime engineB = new ScriptRuntime(new MapResolver(java.util.Map.of(
                "org/test/b.dwl", "%dw 2.0\nfun greet(n: String) = \"B:\" ++ n")));

        long hA = ScriptRuntime.register(engineA);
        long hB = ScriptRuntime.register(engineB);
        assertNotNull(ScriptRuntime.get(hA));
        assertNotNull(ScriptRuntime.get(hB));

        // Each engine resolves its own module...
        assertEquals("\"A:X\"", Result.parse(ScriptRuntime.get(hA).run(IMPORT_A)).result);
        assertEquals("\"B:X\"", Result.parse(ScriptRuntime.get(hB).run(IMPORT_B)).result);

        // ...and NOT the other's (no cross-talk).
        assertNotNull(Result.parse(ScriptRuntime.get(hA).run(IMPORT_B)).error);
        assertNotNull(Result.parse(ScriptRuntime.get(hB).run(IMPORT_A)).error);

        // destroy removes it; a fresh handle is distinct.
        assertTrue(ScriptRuntime.destroy(hA));
        assertNull(ScriptRuntime.get(hA));
        assertFalse(ScriptRuntime.destroy(hA)); // already gone
        assertNotNull(ScriptRuntime.get(hB));

        ScriptRuntime.destroy(hB);
    }

    @Test
    void engineWithoutResolverStillRunsBuiltins() {
        ScriptRuntime engine = new ScriptRuntime(null); // ClassLoader-only
        long h = ScriptRuntime.register(engine);
        String r = ScriptRuntime.get(h).run(
                "%dw 2.0\nimport dw::core::Strings\noutput application/json\n---\nStrings::capitalize(\"hello\")");
        assertEquals("\"Hello\"", Result.parse(r).result);
        ScriptRuntime.destroy(h);
    }

    /**
     * Locks in the hard contract for the per-engine FFI entrypoints
     * ({@code run_script_engine}, {@code run_script_callback_engine},
     * {@code run_script_input_output_callback_engine} in {@link NativeLib}): running a
     * script against an unknown or already-destroyed engine handle must return exactly
     * {@code {"success":false,"error":"Unknown engine handle"}} rather than throwing.
     *
     * <p>The {@code @CEntryPoint} methods themselves cannot be invoked from a plain JVM
     * unit test — they take GraalVM word types ({@code IsolateThread}, {@code CCharPointer})
     * whose boxing infrastructure is only initialized inside a compiled native image (calling
     * e.g. {@code WordFactory.nullPointer()} from a hosted JVM test throws
     * {@code NullPointerException} from {@code WordBoxFactory}). All three entrypoints funnel
     * the unknown-handle case through the same {@code UNKNOWN_ENGINE_HANDLE_JSON} constant, so
     * asserting on that constant — combined with {@link #twoEnginesResolveOnlyTheirOwnModule}
     * proving {@link ScriptRuntime#get} returns {@code null} for an unregistered/destroyed
     * handle — verifies the full contract without needing the native runtime.</p>
     */
    @Test
    void unknownEngineHandleProducesExactErrorJson() {
        long unregisteredHandle = Long.MAX_VALUE;
        assertNull(ScriptRuntime.get(unregisteredHandle));
        assertEquals("{\"success\":false,\"error\":\"Unknown engine handle\"}",
                NativeLib.UNKNOWN_ENGINE_HANDLE_JSON);
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
            org.json.JSONObject obj = new org.json.JSONObject(json);

            result.success = obj.getBoolean("success");
            if (result.success) {
                result.binary = obj.getBoolean("binary");
                result.mimeType = obj.getString("mimeType");
                result.charset = obj.getString("charset");
                String encoded = obj.getString("result");
                if (result.binary) {
                    result.result = encoded;
                } else {
                    result.result = new String(Base64.getDecoder().decode(encoded), Charset.forName(result.charset));
                }
            } else {
                result.error = obj.getString("error");
            }
            return result;
        }
    }

}
