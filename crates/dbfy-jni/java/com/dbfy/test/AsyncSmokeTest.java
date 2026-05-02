package com.dbfy.test;

import com.dbfy.Dbfy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Smoke test for the Java async binding. Exercises the FFI callback
 * round trip (tokio thread → AttachCurrentThread → CompletableFuture)
 * by submitting a query that is guaranteed to fail server-side
 * (no tables registered) and asserting that the error surfaces as
 * an exceptional completion of the future — never as a synchronous
 * throw on the caller's thread.
 *
 * <p>Run with:
 * <pre>
 * javac -d crates/dbfy-jni/java/out crates/dbfy-jni/java/com/dbfy/Dbfy.java \
 *       crates/dbfy-jni/java/com/dbfy/test/AsyncSmokeTest.java
 * java -cp crates/dbfy-jni/java/out \
 *      -Djava.library.path=target/release \
 *      com.dbfy.test.AsyncSmokeTest
 * </pre>
 *
 * <p>Exit code 0 if all assertions pass, 1 otherwise.
 */
public final class AsyncSmokeTest {
    public static void main(String[] args) throws Exception {
        int failures = 0;
        failures += runQueryAsyncDeliversErrorViaFuture();
        failures += runQueryAsyncCompletesOffCallerThread();
        if (failures == 0) {
            System.out.println("OK: all async tests passed");
            System.exit(0);
        } else {
            System.err.println("FAIL: " + failures + " async test(s) failed");
            System.exit(1);
        }
    }

    /**
     * Native errors must surface as <code>future.get()</code> throwing
     * <code>ExecutionException</code> — not as a synchronous throw on
     * the calling thread. The synchronous path would defeat the
     * purpose of the async API.
     */
    private static int runQueryAsyncDeliversErrorViaFuture() {
        String name = "queryAsync_delivers_error_via_future";
        try (Dbfy engine = Dbfy.empty()) {
            CompletableFuture<byte[]> fut = engine.queryAsyncArrowIpc(
                    "SELECT * FROM nonexistent_table");
            try {
                fut.get(5, TimeUnit.SECONDS);
                System.err.println("[FAIL] " + name + ": expected ExecutionException, got success");
                return 1;
            } catch (ExecutionException e) {
                if (e.getCause() == null) {
                    System.err.println("[FAIL] " + name + ": ExecutionException with no cause");
                    return 1;
                }
                System.out.println("[OK] " + name);
                return 0;
            } catch (TimeoutException | InterruptedException e) {
                System.err.println("[FAIL] " + name + ": " + e);
                return 1;
            }
        } catch (Exception unexpected) {
            System.err.println("[FAIL] " + name + ": unexpected synchronous throw: " + unexpected);
            return 1;
        }
    }

    /**
     * The async submission must return immediately — the native side
     * spawns the query on its tokio runtime, the JVM thread is never
     * parked. We measure the elapsed time of the submit call; in
     * practice it's microseconds, well under our 1-second sanity bar.
     */
    private static int runQueryAsyncCompletesOffCallerThread() {
        String name = "queryAsync_completes_off_caller_thread";
        try (Dbfy engine = Dbfy.empty()) {
            long t0 = System.nanoTime();
            CompletableFuture<byte[]> fut = engine.queryAsyncArrowIpc("SELECT 1");
            long submitNs = System.nanoTime() - t0;
            if (submitNs > 1_000_000_000L) {
                System.err.println("[FAIL] " + name + ": submit took " + submitNs + "ns, expected <1s");
                return 1;
            }
            // Drain the future to avoid leaking the native task —
            // we don't care about the outcome (no tables registered).
            try {
                fut.get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
            System.out.println("[OK] " + name + " (submit=" + (submitNs / 1000) + "us)");
            return 0;
        } catch (Exception unexpected) {
            System.err.println("[FAIL] " + name + ": " + unexpected);
            return 1;
        }
    }
}
