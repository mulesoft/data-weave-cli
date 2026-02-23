package org.mule.weave.lib;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;

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
