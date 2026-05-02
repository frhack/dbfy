package com.dbfy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Java bindings for the embedded dbfy federated SQL engine.
 *
 * <p>Each instance owns a native engine handle. Call {@link #close()} to release
 * resources, or use try-with-resources. Query results are returned as Arrow IPC
 * stream bytes that the caller decodes with
 * {@code org.apache.arrow.vector.ipc.ArrowStreamReader}.
 *
 * <h2>Async</h2>
 *
 * <p>{@link #queryAsyncArrowIpc(String)} returns immediately with a
 * {@link CompletableFuture} that the native side completes from a tokio
 * worker thread (attached to the JVM on the fly via
 * {@code AttachCurrentThread}). Continuations registered through
 * {@code thenApply} / {@code whenComplete} run on the JVM common
 * fork-join pool unless an explicit executor is provided.
 *
 * <h2>Native lib loading</h2>
 *
 * <p>The companion native library {@code libdbfy_jni.so} (Linux),
 * {@code libdbfy_jni.dylib} (macOS), or {@code dbfy_jni.dll} (Windows)
 * is loaded eagerly from the classpath at class-init time:
 * <ol>
 *   <li>Compute the current RID ({@code linux-x86_64},
 *       {@code osx-arm64}, etc) from the {@code os.name}/{@code os.arch}
 *       JVM properties.</li>
 *   <li>Look up the resource at
 *       {@code /com/dbfy/native/<rid>/<libname>} on the classpath.
 *       This is where the per-classifier "natives" jar drops the lib.</li>
 *   <li>Copy the resource to a temporary file and call
 *       {@link System#load(String)} on the path.</li>
 * </ol>
 *
 * <p>If the resource isn't on the classpath (no natives jar declared),
 * fall back to {@link System#loadLibrary(String)} so the user can still
 * point at a freshly-built {@code target/release/} via
 * {@code -Djava.library.path=…} during local development.
 */
public final class Dbfy implements AutoCloseable {

    static {
        loadNative();
    }

    /**
     * Per-platform RID + native lib name.
     */
    private static String currentRid() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        String osTag;
        if (os.contains("linux")) osTag = "linux";
        else if (os.contains("mac") || os.contains("darwin")) osTag = "osx";
        else if (os.contains("windows")) osTag = "windows";
        else throw new UnsatisfiedLinkError("unsupported OS: " + os);

        String archTag;
        if (arch.equals("amd64") || arch.equals("x86_64")) archTag = "x86_64";
        else if (arch.equals("aarch64") || arch.equals("arm64")) archTag = "arm64";
        else throw new UnsatisfiedLinkError("unsupported arch: " + arch);

        return osTag + "-" + archTag;
    }

    private static String libFilename() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (os.contains("linux")) return "libdbfy_jni.so";
        if (os.contains("mac") || os.contains("darwin")) return "libdbfy_jni.dylib";
        if (os.contains("windows")) return "dbfy_jni.dll";
        throw new UnsatisfiedLinkError("unsupported OS: " + os);
    }

    private static void loadNative() {
        String rid = currentRid();
        String libname = libFilename();
        String resource = "/com/dbfy/native/" + rid + "/" + libname;
        try (InputStream in = Dbfy.class.getResourceAsStream(resource)) {
            if (in != null) {
                Path tmp = Files.createTempFile("libdbfy_jni-", "-" + libname);
                tmp.toFile().deleteOnExit();
                Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
                System.load(tmp.toAbsolutePath().toString());
                return;
            }
        } catch (IOException e) {
            throw new UnsatisfiedLinkError(
                "failed to extract " + resource + ": " + e.getMessage());
        }
        // Fallback: vanilla loadLibrary for `-Djava.library.path=` users.
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
     * Blocks the caller's thread.
     */
    public byte[] queryArrowIpc(String sql) {
        Objects.requireNonNull(sql, "sql");
        ensureOpen();
        return nativeQuery(handle, sql);
    }

    /**
     * Execute a SQL query asynchronously. Returns immediately. The
     * future is completed (with Arrow IPC bytes on success, or a
     * {@link RuntimeException} on failure) by a tokio worker thread
     * that the JVM is attached to on the fly.
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
