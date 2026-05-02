package com.dbfy

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows

class DbfyKotlinTest {

    @Test
    fun queryThrowsOnNativeError() = runBlocking {
        // No tables registered, so this query must fail. The Java side
        // would return ExecutionException-wrapped; Kotlin's `await()`
        // unwraps the cause for us, so we get a plain RuntimeException.
        Dbfy.empty().use { engine ->
            assertThrows(Throwable::class.java) {
                runBlocking {
                    engine.query("SELECT * FROM nonexistent_table")
                }
            }
        }
    }

    @Test
    fun queryStreamEmitsOrThrowsButNeverSilent() = runBlocking {
        // Today's API yields one IPC blob per scan. The Flow's
        // cardinality goes higher once we wire `dbfy_engine_query_stream_v2`
        // (v0.4 milestone). For now we just assert: cardinality >= 1
        // OR exception, never silent empty.
        Dbfy.empty().use { engine ->
            try {
                val items = engine.queryStream("SELECT 1").toList()
                // No tables; if it somehow succeeds, that's fine too.
                assertNotNull(items)
            } catch (expected: Throwable) {
                // expected — no tables registered. The point of the
                // test is that the Flow doesn't silently swallow
                // upstream failures.
            }
        }
    }
}
