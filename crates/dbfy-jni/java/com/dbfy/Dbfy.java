package com.dbfy;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Java bindings for the embedded dbfy federated SQL engine.
 *
 * <p>Each instance owns a native engine handle. Call {@link #close()} to release
 * resources, or use try-with-resources. Query results are returned as Arrow IPC
 * stream bytes that the caller decodes with
 * {@code org.apache.arrow.vector.ipc.ArrowStreamReader}.
 */
public final class Dbfy implements AutoCloseable {

    static {
        System.loadLibrary("dbfy_jni");
    }

    private long handle;

    private Dbfy(long handle) {
        if (handle == 0L) {
            throw new IllegalStateException("native engine handle is null");
        }
        this.handle = handle;
    }

    public static Dbfy fromYaml(String yaml) {
        Objects.requireNonNull(yaml, "yaml");
        return new Dbfy(nativeNewFromYaml(yaml));
    }

    public static Dbfy fromPath(String path) {
        Objects.requireNonNull(path, "path");
        return new Dbfy(nativeNewFromPath(path));
    }

    public static Dbfy empty() {
        return new Dbfy(nativeNewEmpty());
    }

    /**
     * Execute a SQL query and return the result as Arrow IPC stream bytes.
     */
    public byte[] queryArrowIpc(String sql) {
        Objects.requireNonNull(sql, "sql");
        ensureOpen();
        return nativeQuery(handle, sql);
    }

    /**
     * Execute a SQL query asynchronously. The returned future is completed
     * (with Arrow IPC bytes on success, or a {@link RuntimeException} on
     * failure) by a tokio worker thread that the JVM is attached to on
     * the fly via {@code AttachCurrentThread}. Continuations registered
     * via {@code thenApply} / {@code whenComplete} run on the JVM common
     * fork-join pool unless an explicit executor is provided.
     *
     * <p>The native side spawns the query on its existing tokio runtime
     * and returns immediately, so the Java caller's thread is never
     * parked waiting for the native query.
     */
    public CompletableFuture<byte[]> queryAsyncArrowIpc(String sql) {
        Objects.requireNonNull(sql, "sql");
        ensureOpen();
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        nativeQueryAsync(handle, sql, future);
        return future;
    }

    /**
     * Render the optimized logical plan with pushdown details.
     */
    public String explain(String sql) {
        Objects.requireNonNull(sql, "sql");
        ensureOpen();
        return nativeExplain(handle, sql);
    }

    @Override
    public void close() {
        if (handle != 0L) {
            nativeFree(handle);
            handle = 0L;
        }
    }

    private void ensureOpen() {
        if (handle == 0L) {
            throw new IllegalStateException("Dbfy engine has been closed");
        }
    }

    private static native long nativeNewFromYaml(String yaml);

    private static native long nativeNewFromPath(String path);

    private static native long nativeNewEmpty();

    private static native void nativeFree(long handle);

    private static native byte[] nativeQuery(long handle, String sql);

    private static native String nativeExplain(long handle, String sql);

    private static native void nativeQueryAsync(long handle, String sql,
            CompletableFuture<byte[]> future);
}
