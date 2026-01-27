package org.mule.weave.lib;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;

import java.nio.charset.StandardCharsets;

public class NativeLib {

    /**
     * TODO FIX documentation
     * Native method that executes a DataWeave script with inputs and returns the result.
     * Can be called from Python via FFI.
     *
     * Example JSON format:
     * {
     *   "payload": {"content": "{\"field\": \"value\"}", "mimeType": "application/json"},
     *   "vars": {"content": "test", "mimeType": "text/plain"}
     * }
     *
     * @param thread the isolate thread (automatically provided by GraalVM)
     * @param script the DataWeave script to execute (C string pointer)
     * @param inputsJson JSON string containing the inputs map with content and mimeType for each binding
     * @return the script execution result (C string pointer)
     */
    @CEntryPoint(name = "run_script")
    public static CCharPointer runDwScriptEncoded(IsolateThread thread, CCharPointer script, CCharPointer inputsJson) {
        String dwScript = CTypeConversion.toJavaString(script);
        String inputs = CTypeConversion.toJavaString(inputsJson);

        ScriptRuntime runtime = ScriptRuntime.getInstance();
        String result = runtime.run(dwScript, inputs);
        return toUnmanagedCString(result);
    }

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
