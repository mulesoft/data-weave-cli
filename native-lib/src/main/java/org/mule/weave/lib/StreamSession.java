package org.mule.weave.lib;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds an open {@link InputStream} and associated metadata for a streaming script execution.
 *
 * <p>Instances are stored in a static registry keyed by a monotonically increasing handle so that
 * native callers can reference them across {@code @CEntryPoint} invocations.</p>
 */
public class StreamSession {

    private static final ConcurrentHashMap<Long, StreamSession> SESSIONS = new ConcurrentHashMap<>();
    private static final AtomicLong NEXT_HANDLE = new AtomicLong(1);

    private final InputStream inputStream;
    private final String mimeType;
    private final String charset;
    private final boolean binary;
    private final String error;

    StreamSession(InputStream inputStream, String mimeType, String charset, boolean binary) {
        this.inputStream = inputStream;
        this.mimeType = mimeType;
        this.charset = charset;
        this.binary = binary;
        this.error = null;
    }

    private StreamSession(String error) {
        this.inputStream = null;
        this.mimeType = null;
        this.charset = null;
        this.binary = false;
        this.error = error;
    }

    /**
     * Creates an error session that carries only an error message.
     *
     * @param error the error message
     * @return an error session
     */
    public static StreamSession ofError(String error) {
        return new StreamSession(error);
    }

    /**
     * Registers this session and returns its handle.
     *
     * @return a unique handle that callers use to reference this session
     */
    public long register() {
        long handle = NEXT_HANDLE.getAndIncrement();
        SESSIONS.put(handle, this);
        return handle;
    }

    /**
     * Looks up a previously registered session.
     *
     * @param handle the handle returned by {@link #register()}
     * @return the session, or {@code null} if not found
     */
    public static StreamSession get(long handle) {
        return SESSIONS.get(handle);
    }

    /**
     * Removes and closes a session.
     *
     * @param handle the session handle
     */
    public static void close(long handle) {
        StreamSession session = SESSIONS.remove(handle);
        if (session != null && session.inputStream != null) {
            try {
                session.inputStream.close();
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * Reads up to {@code len} bytes into the provided byte array.
     *
     * @param buf destination buffer
     * @param len maximum number of bytes to read
     * @return number of bytes actually read, or {@code -1} on EOF
     * @throws IOException if an I/O error occurs
     */
    public int read(byte[] buf, int len) throws IOException {
        return inputStream.read(buf, 0, len);
    }

    public String getMimeType() {
        return mimeType;
    }

    public String getCharset() {
        return charset;
    }

    public boolean isBinary() {
        return binary;
    }

    /**
     * Returns the error message if this is an error session, or {@code null} otherwise.
     */
    public String getError() {
        return error;
    }

    /**
     * Returns {@code true} if this session represents a failed execution.
     */
    public boolean isError() {
        return error != null;
    }
}
