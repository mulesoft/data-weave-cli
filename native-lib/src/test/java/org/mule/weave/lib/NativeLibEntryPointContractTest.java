package org.mule.weave.lib;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.word.PointerBase;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeLibEntryPointContractTest {

    @Test
    void allExportsDeclareExplicitExceptionHandlers() throws NoSuchMethodException {
        assertEquals(
                CEntryPointExceptionHandlers.ReturnVoid.class,
                annotation("freeCString", IsolateThread.class, CCharPointer.class).exceptionHandler());
        assertEquals(
                CEntryPointExceptionHandlers.ReturnZero.class,
                annotation("createEngine", IsolateThread.class).exceptionHandler());
        assertEquals(
                CEntryPointExceptionHandlers.ReturnZero.class,
                annotation("createEngineWithResolver", IsolateThread.class,
                        NativeCallbacks.ResolveModuleCallback.class, PointerBase.class).exceptionHandler());
        assertEquals(
                CEntryPointExceptionHandlers.ReturnVoid.class,
                annotation("destroyEngine", IsolateThread.class, long.class).exceptionHandler());
        assertEquals(
                CEntryPointExceptionHandlers.ReturnNullPointer.class,
                annotation("runScriptEngine", IsolateThread.class, long.class,
                        CCharPointer.class, CCharPointer.class).exceptionHandler());
        assertEquals(
                CEntryPointExceptionHandlers.ReturnNullPointer.class,
                annotation("runScriptCallbackEngine", IsolateThread.class, long.class,
                        CCharPointer.class, CCharPointer.class,
                        NativeCallbacks.WriteCallback.class, PointerBase.class).exceptionHandler());
        assertEquals(
                CEntryPointExceptionHandlers.ReturnNullPointer.class,
                annotation("runScriptInputOutputCallbackEngine", IsolateThread.class, long.class,
                        CCharPointer.class, CCharPointer.class, CCharPointer.class,
                        CCharPointer.class, CCharPointer.class, NativeCallbacks.ReadCallback.class,
                        NativeCallbacks.WriteCallback.class, PointerBase.class).exceptionHandler());
    }

    @Test
    void everyUnmanagedAllocationIsCheckedBeforeDereference() throws IOException {
        String source = Files.readString(Path.of("src/main/java/org/mule/weave/lib/NativeLib.java"));
        Matcher allocations = Pattern.compile(
                "CCharPointer\\s+(\\w+)\\s*=\\s*UnmanagedMemory\\.malloc\\([^;]+;"
        ).matcher(source);
        int checked = 0;
        while (allocations.find()) {
            String variable = allocations.group(1);
            String following = source.substring(allocations.end(),
                    Math.min(source.length(), allocations.end() + 220));
            assertTrue(following.contains("if (" + variable + ".isNull())"),
                    "unchecked unmanaged allocation for " + variable);
            checked++;
        }
        assertEquals(4, checked, "all NativeLib unmanaged allocation sites must remain covered");
    }

    private static CEntryPoint annotation(String methodName, Class<?>... parameterTypes)
            throws NoSuchMethodException {
        return NativeLib.class.getDeclaredMethod(methodName, parameterTypes)
                .getAnnotation(CEntryPoint.class);
    }
}
