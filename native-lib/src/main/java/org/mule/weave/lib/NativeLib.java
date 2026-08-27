package org.mule.weave.lib;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.PointerBase;
import org.graalvm.word.WordFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * GraalVM native entry points exposed for FFI consumers.
 *
 * <p>This class provides C-callable functions to execute DataWeave scripts and to free the returned
 * unmanaged strings.</p>
 */
public class NativeLib {

    /**
     * The exact JSON error payload returned by the per-engine entrypoints
     * ({@link #runScriptEngine}, {@link #runScriptCallbackEngine},
     * {@link #runScriptInputOutputCallbackEngine}) when {@code handle} does not identify a
     * live engine. Package-visible (rather than embedded as a string literal at each call
     * site) so the exact contract can be asserted directly from a JVM unit test, since the
     * {@code @CEntryPoint} methods themselves rely on GraalVM word types that only resolve
     * inside a compiled native image.
     */
    static final String UNKNOWN_ENGINE_HANDLE_JSON = "{\"success\":false,\"error\":\"Unknown engine handle\"}";

    /**
     * Frees a C string previously returned by engine entrypoints.
     *
     * @param thread the isolate thread (automatically provided by GraalVM)
     * @param pointer the pointer to the unmanaged C string to free; if null, this is a no-op
     */
    @CEntryPoint(name = "free_cstring")
    public static void freeCString(IsolateThread thread, CCharPointer pointer) {
        if (pointer.isNull()) {
            return;
        }
        UnmanagedMemory.free(pointer);
    }

    // ── Callback-based Streaming API ─────────────────────────────────────

    private static final int CALLBACK_BUFFER_SIZE = 8 * 1024;

    /**
     * Runs the streaming write-callback loop shared by the per-engine entrypoint
     * ({@link #runScriptCallbackEngine}).
     */
    private static CCharPointer streamToWriteCallback(
            ScriptRuntime runtime, String dwScript, String inputs,
            NativeCallbacks.WriteCallback writeCallback, PointerBase ctx) {
        StreamSession session = runtime.runStreaming(dwScript, inputs);

        if (session.isError()) {
            return toUnmanagedCString("{\"success\":false,\"error\":\""
                    + escapeJsonString(session.getError()) + "\"}");
        }

        try {
            byte[] buf = new byte[CALLBACK_BUFFER_SIZE];
            CCharPointer nativeBuf = UnmanagedMemory.malloc(CALLBACK_BUFFER_SIZE);
            try {
                int n;
                while ((n = session.read(buf, buf.length)) > 0) {
                    for (int i = 0; i < n; i++) {
                        nativeBuf.write(i, buf[i]);
                    }
                    int rc = writeCallback.invoke(ctx, nativeBuf, n);
                    if (rc != 0) {
                        return toUnmanagedCString("{\"success\":false,\"error\":\""
                                + "Write callback returned error: " + rc + "\"}");
                    }
                }
            } finally {
                UnmanagedMemory.free(nativeBuf);
            }
        } catch (IOException e) {
            return toUnmanagedCString("{\"success\":false,\"error\":\""
                    + escapeJsonString(e.getMessage()) + "\"}");
        } finally {
            session.closeStream();
        }

        return toUnmanagedCString("{\"success\":true"
                + ",\"mimeType\":\"" + session.getMimeType() + "\""
                + ",\"charset\":\"" + session.getCharset() + "\""
                + ",\"binary\":" + session.isBinary()
                + "}");
    }

    /**
     * Runs the input-feeder + output-streaming loop shared by the per-engine entrypoint
     * ({@link #runScriptInputOutputCallbackEngine}).
     */
    private static CCharPointer transformViaCallbacks(
            ScriptRuntime runtime, String dwScript, String inputs,
            String inName, String inMime, String inCharset,
            NativeCallbacks.ReadCallback readCallback, NativeCallbacks.WriteCallback writeCallback,
            PointerBase ctx) {

        // Create a piped input stream session for the callback-supplied input
        InputStreamSession inputSession = new InputStreamSession(inMime, inCharset);
        long inputHandle = inputSession.register();

        // Merge the stream handle into the inputs JSON
        String streamEntry = "{\"streamHandle\":\"" + inputHandle + "\",\"mimeType\":\"" + inMime + "\""
                + (inCharset != null ? ",\"charset\":\"" + inCharset + "\"" : "") + "}";
        String mergedInputs = mergeInputEntry(inputs, inName, streamEntry);

        // Start a background thread that calls the readCallback and feeds data into the pipe.
        // Word types (CCharPointer, CFunctionPointer, PointerBase) cannot be captured in
        // lambdas in GraalVM Native Image, so we use an explicit Runnable that stores their
        // raw addresses and reconstitutes them via WordFactory.
        final long readCallbackAddr = readCallback.rawValue();
        final long ctxAddr = ctx.rawValue();
        Thread feeder = new Thread(new InputCallbackFeeder(
                readCallbackAddr, ctxAddr, inputSession), "dw-input-callback-feeder");
        feeder.setDaemon(true);
        feeder.start();

        // Execute the script and stream output via the writeCallback
        StreamSession session = runtime.runStreaming(dwScript, mergedInputs);

        if (session.isError()) {
            cleanupFeeder(feeder, inputHandle);
            return toUnmanagedCString("{\"success\":false,\"error\":\""
                    + escapeJsonString(session.getError()) + "\"}");
        }

        try {
            byte[] buf = new byte[CALLBACK_BUFFER_SIZE];
            CCharPointer writeBuf = UnmanagedMemory.malloc(CALLBACK_BUFFER_SIZE);
            try {
                int n;
                while ((n = session.read(buf, buf.length)) > 0) {
                    for (int i = 0; i < n; i++) {
                        writeBuf.write(i, buf[i]);
                    }
                    int rc = writeCallback.invoke(ctx, writeBuf, n);
                    if (rc != 0) {
                        cleanupFeeder(feeder, inputHandle);
                        return toUnmanagedCString("{\"success\":false,\"error\":\""
                                + "Write callback returned error: " + rc + "\"}");
                    }
                }
            } finally {
                UnmanagedMemory.free(writeBuf);
            }
        } catch (IOException e) {
            cleanupFeeder(feeder, inputHandle);
            return toUnmanagedCString("{\"success\":false,\"error\":\""
                    + escapeJsonString(e.getMessage()) + "\"}");
        } finally {
            session.closeStream();
        }

        cleanupFeeder(feeder, inputHandle);

        return toUnmanagedCString("{\"success\":true"
                + ",\"mimeType\":\"" + session.getMimeType() + "\""
                + ",\"charset\":\"" + session.getCharset() + "\""
                + ",\"binary\":" + session.isBinary()
                + "}");
    }

    /**
     * Merges a single input entry into an existing JSON inputs string.
     */
    private static String mergeInputEntry(String existingJson, String name, String entryJson) {
        org.json.JSONObject obj = (existingJson == null || existingJson.trim().isEmpty())
                ? new org.json.JSONObject()
                : new org.json.JSONObject(existingJson);
        obj.put(name, new org.json.JSONObject(entryJson));
        return obj.toString();
    }

    /**
     * Waits for the feeder thread to finish and closes the input session.
     */
    private static void cleanupFeeder(Thread feeder, long inputHandle) {
        try {
            feeder.join(5000);
        } catch (InterruptedException ignored) {
        }
        InputStreamSession.close(inputHandle);
    }

    /**
     * Explicit {@link Runnable} that drives the read-callback loop on a background thread.
     *
     * <p>GraalVM Native Image forbids capturing {@code Word} types (such as
     * {@link CCharPointer} or {@link CFunctionPointer}) inside lambdas. This class stores
     * the raw addresses as plain {@code long} values and reconstitutes the pointers via
     * {@link WordFactory#pointer(long)} inside {@link #run()}.</p>
     *
     * <p>The feeder allocates its own native read buffer and frees it in its {@code finally}
     * block, ensuring no shared native memory between threads.</p>
     */
    private static final class InputCallbackFeeder implements Runnable {
        private final long readCallbackAddr;
        private final long ctxAddr;
        private final InputStreamSession inputSession;

        InputCallbackFeeder(long readCallbackAddr, long ctxAddr,
                            InputStreamSession inputSession) {
            this.readCallbackAddr = readCallbackAddr;
            this.ctxAddr = ctxAddr;
            this.inputSession = inputSession;
        }

        @Override
        public void run() {
            NativeCallbacks.ReadCallback cb = WordFactory.pointer(readCallbackAddr);
            PointerBase ctx = WordFactory.pointer(ctxAddr);
            CCharPointer buf = UnmanagedMemory.malloc(CALLBACK_BUFFER_SIZE);
            try {
                while (true) {
                    int n = cb.invoke(ctx, buf, CALLBACK_BUFFER_SIZE);
                    if (n <= 0) {
                        break; // 0 = EOF, negative = error
                    }
                    byte[] tmp = new byte[n];
                    for (int i = 0; i < n; i++) {
                        tmp[i] = buf.read(i);
                    }
                    inputSession.write(tmp, n);
                }
            } catch (IOException e) {
                // pipe broken – DW engine will see the error
            } finally {
                UnmanagedMemory.free(buf);
                try {
                    inputSession.closeWriter();
                } catch (IOException ignored) {
                }
            }
        }
    }

    private static String escapeJsonString(String input) {
        if (input == null) return "";
        return input
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private static CCharPointer toUnmanagedCString(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        CCharPointer ptr = UnmanagedMemory.malloc(bytes.length + 1);
        for (int i = 0; i < bytes.length; i++) {
            ptr.write(i, bytes[i]);
        }
        ptr.write(bytes.length, (byte) 0);
        return ptr;
    }

    // ── Multi-Engine FFI Entrypoints (W-23692110) ────────────────────────

    /**
     * Creates a new isolated engine (ClassLoader-only resolver) and returns its handle.
     *
     * @param thread the isolate thread
     * @return a non-zero handle identifying the new engine
     */
    @CEntryPoint(name = "create_engine")
    public static long createEngine(IsolateThread thread) {
        return ScriptRuntime.register(new ScriptRuntime());
    }

    /**
     * Creates a new isolated engine backed by a caller-supplied module resolver callback,
     * and returns its handle.
     *
     * @param thread           the isolate thread
     * @param resolverCallback callback used to resolve external modules for this engine only
     * @param ctx              opaque context pointer forwarded to every resolver invocation
     * @return a non-zero handle identifying the new engine
     */
    @CEntryPoint(name = "create_engine_with_resolver")
    public static long createEngineWithResolver(
            IsolateThread thread,
            NativeCallbacks.ResolveModuleCallback resolverCallback,
            PointerBase ctx) {
        CallbackWeaveResourceResolver resolver =
                new CallbackWeaveResourceResolver(resolverCallback, ctx);
        return ScriptRuntime.register(new ScriptRuntime(resolver));
    }

    /**
     * Destroys an engine created by {@link #createEngine} / {@link #createEngineWithResolver}.
     * A no-op if the handle is unknown or already destroyed.
     *
     * @param thread the isolate thread
     * @param handle the engine handle to remove
     */
    @CEntryPoint(name = "destroy_engine")
    public static void destroyEngine(IsolateThread thread, long handle) {
        ScriptRuntime.destroy(handle);
    }

    /**
     * Executes a DataWeave script against a specific engine.
     *
     * <p>If {@code handle} does not identify a live engine, returns
     * {@code {"success":false,"error":"Unknown engine handle"}} rather than throwing.</p>
     *
     * @param thread     the isolate thread
     * @param handle     the target engine's handle
     * @param script     the DataWeave script (C string)
     * @param inputsJson JSON-encoded inputs map (C string), may be null
     * @return the script execution result (unmanaged C string, must be freed)
     */
    @CEntryPoint(name = "run_script_engine")
    public static CCharPointer runScriptEngine(
            IsolateThread thread, long handle, CCharPointer script, CCharPointer inputsJson) {
        ScriptRuntime runtime = ScriptRuntime.get(handle);
        if (runtime == null) {
            return toUnmanagedCString(UNKNOWN_ENGINE_HANDLE_JSON);
        }
        String dwScript = CTypeConversion.toJavaString(script);
        String inputs = inputsJson.isNull() ? null : CTypeConversion.toJavaString(inputsJson);
        return toUnmanagedCString(runtime.run(dwScript, inputs));
    }

    /**
     * Executes a DataWeave script against a specific engine, streaming the result to a
     * caller-supplied write callback. See {@link #streamToWriteCallback} for the callback contract.
     *
     * <p>If {@code handle} does not identify a live engine, returns
     * {@code {"success":false,"error":"Unknown engine handle"}} rather than throwing.</p>
     *
     * @param thread        the isolate thread
     * @param handle        the target engine's handle
     * @param script        the DataWeave script (C string)
     * @param inputsJson    JSON-encoded inputs map (C string), may be null
     * @param writeCallback function pointer invoked with each output chunk; must return 0 on success
     * @param ctx           opaque context pointer forwarded to every callback invocation
     * @return an unmanaged C string with JSON metadata/error
     */
    @CEntryPoint(name = "run_script_callback_engine")
    public static CCharPointer runScriptCallbackEngine(
            IsolateThread thread, long handle, CCharPointer script, CCharPointer inputsJson,
            NativeCallbacks.WriteCallback writeCallback, PointerBase ctx) {
        ScriptRuntime runtime = ScriptRuntime.get(handle);
        if (runtime == null) {
            return toUnmanagedCString(UNKNOWN_ENGINE_HANDLE_JSON);
        }
        String dwScript = CTypeConversion.toJavaString(script);
        String inputs = inputsJson.isNull() ? null : CTypeConversion.toJavaString(inputsJson);
        return streamToWriteCallback(runtime, dwScript, inputs, writeCallback, ctx);
    }

    /**
     * Executes a DataWeave script against a specific engine, with a callback-supplied input
     * and callback-streamed output. See {@link #transformViaCallbacks} for the callback
     * contract.
     *
     * <p>If {@code handle} does not identify a live engine, returns
     * {@code {"success":false,"error":"Unknown engine handle"}} rather than throwing.</p>
     *
     * @param thread        the isolate thread
     * @param handle        the target engine's handle
     * @param script        the DataWeave script (C string)
     * @param inputsJson    JSON-encoded inputs map (C string), may be null
     * @param inputName     the binding name for the callback-supplied input (C string)
     * @param inputMimeType the MIME type of the callback-supplied input (C string)
     * @param inputCharset  the charset of the callback-supplied input (C string), may be null for UTF-8
     * @param readCallback  function pointer invoked to read the next chunk
     * @param writeCallback function pointer invoked with each output chunk; must return 0 on success
     * @param ctx           opaque context pointer forwarded to every callback invocation
     * @return an unmanaged C string with JSON metadata/error
     */
    @CEntryPoint(name = "run_script_input_output_callback_engine")
    public static CCharPointer runScriptInputOutputCallbackEngine(
            IsolateThread thread, long handle, CCharPointer script, CCharPointer inputsJson,
            CCharPointer inputName, CCharPointer inputMimeType, CCharPointer inputCharset,
            NativeCallbacks.ReadCallback readCallback, NativeCallbacks.WriteCallback writeCallback,
            PointerBase ctx) {
        ScriptRuntime runtime = ScriptRuntime.get(handle);
        if (runtime == null) {
            return toUnmanagedCString(UNKNOWN_ENGINE_HANDLE_JSON);
        }
        String dwScript = CTypeConversion.toJavaString(script);
        String inputs = inputsJson.isNull() ? null : CTypeConversion.toJavaString(inputsJson);
        String inName = CTypeConversion.toJavaString(inputName);
        String inMime = CTypeConversion.toJavaString(inputMimeType);
        String inCharset = inputCharset.isNull() ? null : CTypeConversion.toJavaString(inputCharset);
        return transformViaCallbacks(runtime, dwScript, inputs, inName, inMime, inCharset,
                readCallback, writeCallback, ctx);
    }

}
