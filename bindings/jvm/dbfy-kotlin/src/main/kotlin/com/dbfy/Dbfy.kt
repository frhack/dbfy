@file:JvmName("DbfyKt")

package com.dbfy

import kotlinx.coroutines.future.await
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/**
 * Kotlin coroutine extensions over [Dbfy].
 *
 * The underlying [Dbfy.queryAsyncArrowIpc] returns a
 * `CompletableFuture<ByteArray>` that the native side completes from
 * a tokio worker thread. We bridge that to a `suspend fun` via
 * `kotlinx.coroutines.future.await`, so the Kotlin caller's
 * coroutine is suspended until the native query terminates and
 * resumes on the coroutine's dispatcher (NOT on a tokio thread).
 *
 * Errors come through as exceptions thrown from `await()` —
 * idiomatically caught with `try { … } catch (e: Throwable) { … }`,
 * unlike the Java side which has to unwrap `ExecutionException`.
 *
 * Usage:
 *
 * ```kotlin
 * Dbfy.fromYaml(yaml).use { engine ->
 *     val ipc: ByteArray = engine.query("SELECT id, name FROM crm.customers LIMIT 10")
 *     // decode with org.apache.arrow.vector.ipc.ArrowStreamReader
 * }
 * ```
 */
public suspend fun Dbfy.query(sql: String): ByteArray =
    queryAsyncArrowIpc(sql).await()

/**
 * Streaming variant: emit the Arrow IPC bytes as a single
 * [Flow] item. Today the v0.3 native API returns the whole result
 * batch eagerly so this Flow has cardinality 1; once the native side
 * gains a true cursor (`dbfy_engine_query_stream_v2`, planned for
 * v0.4) this signature stays stable but each emission becomes one
 * RecordBatch.
 *
 * Cancelling the consuming coroutine cancels the Flow, which
 * propagates `tryCancel` to the underlying CompletableFuture.
 */
public fun Dbfy.queryStream(sql: String): Flow<ByteArray> = flow {
    val fut = queryAsyncArrowIpc(sql)
    try {
        emit(fut.await())
    } catch (cancellation: kotlinx.coroutines.CancellationException) {
        fut.cancel(true)
        throw cancellation
    }
}
