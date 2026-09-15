package org.mule.weave.lib;

import com.oracle.svm.core.Uninterruptible;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.word.WordFactory;

/**
 * Contains exceptions at the native C ABI boundary by returning the sentinel
 * expected by each entrypoint's declared return type.
 */
final class CEntryPointExceptionHandlers {
    private CEntryPointExceptionHandlers() {
    }

    /** Returns {@code 0} for entrypoints whose ABI uses zero as a failure sentinel. */
    static final class ReturnZero implements CEntryPoint.ExceptionHandler {
        @Uninterruptible(reason = "Return an ABI sentinel after an entrypoint exception")
        static long handle(Throwable ignored) {
            return 0L;
        }
    }

    /** Returns a null C string pointer for entrypoints whose ABI uses null on failure. */
    static final class ReturnNullPointer implements CEntryPoint.ExceptionHandler {
        @Uninterruptible(reason = "Return an ABI sentinel after an entrypoint exception")
        static CCharPointer handle(Throwable ignored) {
            return WordFactory.nullPointer();
        }
    }

    /** Suppresses exceptions from void entrypoints after their ABI-visible work has stopped. */
    static final class ReturnVoid implements CEntryPoint.ExceptionHandler {
        @Uninterruptible(reason = "Contain an exception at the C ABI boundary")
        static void handle(Throwable ignored) {
        }
    }
}
