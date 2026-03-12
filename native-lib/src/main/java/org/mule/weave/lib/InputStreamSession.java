package org.mule.weave.lib;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages a {@link PipedInputStream}/{@link PipedOutputStream} pair that allows FFI callers
 * to stream data into the DataWeave engine as an input binding.
 *
 * <p>The caller writes bytes into the {@link PipedOutputStream} via {@link #write(byte[], int)}
 * while the DW engine reads from the paired {@link PipedInputStream} on a separate thread.</p>
 *
 * <p>Instances are stored in a static registry keyed by a monotonically increasing handle so that
 * native callers can reference them across {@code @CEntryPoint} invocations.</p>
 */
public class InputStreamSession {

    private static final ConcurrentHashMap<Long, InputStreamSession> SESSIONS = new ConcurrentHashMap<>();
    private static final AtomicLong NEXT_HANDLE = new AtomicLong(1);

    private static final int PIPE_BUFFER_SIZE = 64 * 1024;

    private final PipedInputStream pipedInputStream;
    private final PipedOutputStream pipedOutputStream;
    private final String mimeType;
    private final String charset;

    /**
     * Creates a new input stream session with the given metadata.
     *
     * @param mimeType the MIME type of the input data
     * @param charset  the character set of the input data (may be {@code null}, defaults to UTF-8)
     */
    public InputStreamSession(String mimeType, String charset) {
        try {
            this.pipedInputStream = new PipedInputStream(PIPE_BUFFER_SIZE);
            this.pipedOutputStream = new PipedOutputStream(pipedInputStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create piped streams", e);
        }
        this.mimeType = mimeType;
        this.charset = charset != null ? charset : "UTF-8";
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
    public static InputStreamSession get(long handle) {
        return SESSIONS.get(handle);
    }

    /**
     * Removes a session from the registry and closes both ends of the pipe.
     *
     * @param handle the session handle
     */
    public static void close(long handle) {
        InputStreamSession session = SESSIONS.remove(handle);
        if (session != null) {
            try {
                session.pipedOutputStream.close();
            } catch (IOException ignored) {
            }
            try {
                session.pipedInputStream.close();
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * Writes bytes into the pipe. The DW engine will read these from the paired
     * {@link PipedInputStream}.
     *
     * @param data   the byte array to write from
     * @param length the number of bytes to write
     * @throws IOException if an I/O error occurs
     */
    public void write(byte[] data, int length) throws IOException {
        pipedOutputStream.write(data, 0, length);
    }

    /**
     * Closes the write end of the pipe, signalling EOF to the reader.
     *
     * @throws IOException if an I/O error occurs
     */
    public void closeWriter() throws IOException {
        pipedOutputStream.close();
    }

    /**
     * Returns the {@link PipedInputStream} that the DW engine should read from.
     *
     * @return the read end of the pipe
     */
    public PipedInputStream getInputStream() {
        return pipedInputStream;
    }

    public String getMimeType() {
        return mimeType;
    }

    public String getCharset() {
        return charset;
    }
}
