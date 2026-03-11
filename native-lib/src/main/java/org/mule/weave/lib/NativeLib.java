package org.mule.weave.lib;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;

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
     * Native method that executes a DataWeave script with inputs and returns the result.
     * Can be called from Python via FFI.
     *
     * @param thread the isolate thread (automatically provided by GraalVM)
     * @param script the DataWeave script to execute (C string pointer)
     * @param inputsJson JSON string containing the inputs map with content (base64 encoded), mimeType, properties and charset for each binding
     * @return the script execution result base64 encoded (C string pointer)
     */
    @CEntryPoint(name = "run_script")
    public static CCharPointer runDwScriptEncoded(IsolateThread thread, CCharPointer script, CCharPointer inputsJson) {
        String dwScript = CTypeConversion.toJavaString(script);
        String inputs = CTypeConversion.toJavaString(inputsJson);

        ScriptRuntime runtime = ScriptRuntime.getInstance();
        String result = runtime.run(dwScript, inputs);
        return toUnmanagedCString(result);
    }

    /**
     * Frees a C string previously returned by {@link #runDwScriptEncoded(IsolateThread, CCharPointer, CCharPointer)}.
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

    // ── Streaming API ────────────────────────────────────────────────────

    /**
     * Executes a DataWeave script and returns an opaque handle to a streaming session.
     * The result can then be read incrementally via {@link #runScriptRead} and must be
     * closed with {@link #runScriptClose} when done.
     *
     * <p>A handle value of {@code -1} indicates an error during compilation or execution.
     * In that case call {@link #runScriptStreamError} to retrieve the error message.</p>
     *
     * @param thread     the isolate thread
     * @param script     the DataWeave script (C string)
     * @param inputsJson JSON-encoded inputs map (C string), may be null
     * @return a positive session handle, or {@code -1} on error
     */
    @CEntryPoint(name = "run_script_open")
    public static long runScriptOpen(IsolateThread thread, CCharPointer script, CCharPointer inputsJson) {
        String dwScript = CTypeConversion.toJavaString(script);
        String inputs = inputsJson.isNull() ? null : CTypeConversion.toJavaString(inputsJson);

        ScriptRuntime runtime = ScriptRuntime.getInstance();
        StreamSession session = runtime.runStreaming(dwScript, inputs);
        return session.register();
    }

    /**
     * Reads the next chunk of bytes from an open streaming session.
     *
     * @param thread     the isolate thread
     * @param handle     the session handle returned by {@link #runScriptOpen}
     * @param buffer     caller-allocated buffer to write into
     * @param bufferSize size of the buffer in bytes
     * @return the number of bytes written into {@code buffer}, {@code 0} on EOF,
     *         or {@code -1} on error (invalid handle or I/O failure)
     */
    @CEntryPoint(name = "run_script_read")
    public static int runScriptRead(IsolateThread thread, long handle, CCharPointer buffer, int bufferSize) {
        StreamSession session = StreamSession.get(handle);
        if (session == null) {
            return -1;
        }
        try {
            byte[] tmp = new byte[bufferSize];
            int n = session.read(tmp, bufferSize);
            if (n <= 0) {
                return 0;
            }
            for (int i = 0; i < n; i++) {
                buffer.write(i, tmp[i]);
            }
            return n;
        } catch (IOException e) {
            return -1;
        }
    }

    /**
     * Returns JSON metadata for an open streaming session:
     * {@code {"mimeType":"...","charset":"...","binary":true/false}}.
     *
     * <p>The caller must free the returned pointer with {@link #freeCString}.</p>
     *
     * @param thread the isolate thread
     * @param handle the session handle
     * @return an unmanaged C string with JSON metadata, or a null pointer if the handle is invalid
     */
    @CEntryPoint(name = "run_script_metadata")
    public static CCharPointer runScriptMetadata(IsolateThread thread, long handle) {
        StreamSession session = StreamSession.get(handle);
        if (session == null) {
            return CTypeConversion.toCString("").get();
        }
        String json = "{"
                + "\"mimeType\":\"" + session.getMimeType() + "\","
                + "\"charset\":\"" + session.getCharset() + "\","
                + "\"binary\":" + session.isBinary()
                + "}";
        return toUnmanagedCString(json);
    }

    /**
     * Retrieves the error message for a failed streaming session.
     *
     * <p>When a session was created from an execution failure its handle is still valid
     * and the error message can be obtained here. The caller must free the returned pointer
     * with {@link #freeCString}.</p>
     *
     * @param thread the isolate thread
     * @param handle the session handle
     * @return an unmanaged C string with the error message, or empty string if not an error session
     */
    @CEntryPoint(name = "run_script_stream_error")
    public static CCharPointer runScriptStreamError(IsolateThread thread, long handle) {
        StreamSession session = StreamSession.get(handle);
        if (session == null || session.getError() == null) {
            return toUnmanagedCString("");
        }
        return toUnmanagedCString(session.getError());
    }

    /**
     * Closes a streaming session, releasing the underlying {@link java.io.InputStream} and
     * removing the session from the registry.
     *
     * @param thread the isolate thread
     * @param handle the session handle
     */
    @CEntryPoint(name = "run_script_close")
    public static void runScriptClose(IsolateThread thread, long handle) {
        StreamSession.close(handle);
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

}
