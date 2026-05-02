package com.dbfy;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class DbfyTest {

    @Test
    void emptyEngineCanBeCreatedAndDisposed() {
        try (Dbfy engine = Dbfy.empty()) {
            assertNotNull(engine);
        }
    }

    @Test
    void invalidYamlThrowsRuntimeException() {
        assertThrows(RuntimeException.class,
                () -> Dbfy.fromYaml("not: a: valid: dbfy: config"));
    }

    @Test
    void queryAsyncDeliversErrorViaFuture() throws Exception {
        try (Dbfy engine = Dbfy.empty()) {
            CompletableFuture<byte[]> fut = engine.queryAsyncArrowIpc(
                    "SELECT * FROM nonexistent_table");
            ExecutionException ex = assertThrows(ExecutionException.class,
                    () -> fut.get(5, TimeUnit.SECONDS));
            assertNotNull(ex.getCause(),
                    "ExecutionException must carry the native error as cause");
        }
    }

    @Test
    void queryAsyncReturnsImmediatelyToCaller() throws Exception {
        // Submit time should be microseconds — the JVM thread is
        // never parked waiting for the native query.
        try (Dbfy engine = Dbfy.empty()) {
            long t0 = System.nanoTime();
            CompletableFuture<byte[]> fut = engine.queryAsyncArrowIpc("SELECT 1");
            long submitNs = System.nanoTime() - t0;
            assertTrue(submitNs < 1_000_000_000L,
                    "submit took " + submitNs + "ns, expected <1s");
            // Drain the future so the native task doesn't outlive
            // the test (we don't care about the outcome).
            try {
                fut.get(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
        }
    }
}
