package com.dbfy;

import java.util.Objects;

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
}
