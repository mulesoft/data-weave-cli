package org.mule.weave.lib;

import org.graalvm.nativeimage.c.function.CFunctionPointer;
import org.graalvm.nativeimage.c.function.InvokeCFunctionPointer;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.word.PointerBase;

/**
 * Function-pointer (callback) interfaces used by the callback-based streaming API in
 * {@link NativeLib}.
 *
 * <p>FFI callers pass C function pointers that conform to these signatures. The Java side
 * invokes them via {@link InvokeCFunctionPointer} to push/pull data without requiring
 * the session-based open/read|write/close round-trips.</p>
 */
public final class NativeCallbacks {

    private NativeCallbacks() {
    }

    /**
     * Callback the native caller provides to <em>receive</em> output data.
     *
     * <p>Signature in C:
     * <pre>{@code int (*WriteCallback)(void *ctx, const char *buffer, int length);}</pre>
     *
     * <p>The Java side calls this repeatedly with chunks of the script result.
     * The callback must return {@code 0} on success or a non-zero value to abort.</p>
     */
    public interface WriteCallback extends CFunctionPointer {
        @InvokeCFunctionPointer
        int invoke(PointerBase ctx, CCharPointer buffer, int length);
    }

    /**
     * Callback the native caller provides to <em>supply</em> input data.
     *
     * <p>Signature in C:
     * <pre>{@code int (*ReadCallback)(void *ctx, char *buffer, int bufferSize);}</pre>
     *
     * <p>The Java side calls this to pull the next chunk of input bytes. The callback must
     * write up to {@code bufferSize} bytes into {@code buffer} and return the number of
     * bytes written, {@code 0} on EOF, or {@code -1} on error.</p>
     */
    public interface ReadCallback extends CFunctionPointer {
        @InvokeCFunctionPointer
        int invoke(PointerBase ctx, CCharPointer buffer, int bufferSize);
    }
}
