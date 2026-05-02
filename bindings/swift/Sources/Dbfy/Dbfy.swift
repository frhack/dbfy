// Dbfy — Swift bindings for the embedded dbfy federated SQL engine.
//
// Wraps the C ABI exposed by `dbfy-c` (see Sources/CDbfy/include/dbfy.h)
// with async/await methods, throwing errors instead of returning
// status codes, and Swift-native lifecycle (the `Engine` actor frees
// the native handle on deinit).
//
// Designed for both macOS and iOS (including Mac Catalyst). The
// native binary ships as a binary xcframework declared in Package.swift.

import CDbfy
import Foundation

/// Errors raised by the dbfy native engine. The associated string is
/// the message returned by `dbfy_last_error` on the calling thread,
/// e.g. "config load: invalid root path" or "execution: table not
/// found: `nonexistent_table`".
public struct DbfyError: Error, Equatable {
    public let operation: String
    public let message: String

    public var localizedDescription: String { "\(operation): \(message)" }

    fileprivate static func fromLastError(_ operation: String) -> DbfyError {
        let cstr = dbfy_last_error()
        let msg = cstr.flatMap { String(cString: $0) } ?? "unknown error (dbfy_last_error returned null)"
        return DbfyError(operation: operation, message: msg)
    }
}

/// A configured dbfy engine. Build one with `Engine.fromYaml(_:)` or
/// `Engine.fromPath(_:)`. The engine is an actor so concurrent calls
/// from multiple Swift tasks are safely serialised onto the actor's
/// executor — the underlying Rust engine is `Send + Sync` but using
/// an actor here keeps the Swift mental model consistent.
public actor Engine {

    private var handle: OpaquePointer?

    private init(_ handle: OpaquePointer) {
        self.handle = handle
    }

    /// Build an engine from an inline YAML configuration string.
    public static func fromYaml(_ yaml: String) throws -> Engine {
        let raw = yaml.withCString { dbfy_engine_new_from_yaml($0) }
        guard let raw = raw else { throw DbfyError.fromLastError("Engine.fromYaml") }
        return Engine(raw)
    }

    /// Build an engine from a path to a YAML configuration file.
    public static func fromPath(_ path: String) throws -> Engine {
        let raw = path.withCString { dbfy_engine_new_from_path($0) }
        guard let raw = raw else { throw DbfyError.fromLastError("Engine.fromPath") }
        return Engine(raw)
    }

    /// Build an engine with no preconfigured sources.
    public static func newEmpty() throws -> Engine {
        guard let raw = dbfy_engine_new_empty() else {
            throw DbfyError.fromLastError("Engine.newEmpty")
        }
        return Engine(raw)
    }

    deinit {
        if let h = handle {
            dbfy_engine_free(h)
        }
    }

    /// Render the optimised logical plan + per-source pushdown
    /// summary as a human-readable string.
    public func explain(_ sql: String) throws -> String {
        guard let h = handle else { throw DbfyError(operation: "Engine.explain", message: "engine closed") }
        let raw = sql.withCString { dbfy_engine_explain(h, $0) }
        guard let raw = raw else { throw DbfyError.fromLastError("Engine.explain") }
        defer { dbfy_string_free(raw) }
        return String(cString: raw)
    }

    /// Execute SQL synchronously and return an opaque `Result` whose
    /// Arrow batches can be exported via the C Data Interface. Blocks
    /// the actor until the native query terminates — prefer
    /// `query(_:)` for anything that may take longer than a few ms.
    public func querySync(_ sql: String) throws -> QueryResult {
        guard let h = handle else { throw DbfyError(operation: "Engine.querySync", message: "engine closed") }
        var out: OpaquePointer?
        let rc = sql.withCString { dbfy_engine_query(h, $0, &out) }
        guard rc == 0, let raw = out else { throw DbfyError.fromLastError("Engine.querySync") }
        return QueryResult(raw)
    }

    /// Execute SQL asynchronously. Returns immediately at the
    /// `await` point; the native side spawns the query on its
    /// tokio runtime and resolves the continuation when the result
    /// (or error) arrives. The continuation runs on the calling
    /// task's executor — never on a tokio thread.
    public func query(_ sql: String) async throws -> QueryResult {
        guard let h = handle else { throw DbfyError(operation: "Engine.query", message: "engine closed") }
        return try await withCheckedThrowingContinuation { continuation in
            // Box the continuation in a `Unmanaged` retained pointer
            // so it survives the round trip to the C callback. The
            // callback releases the retain count and resumes the
            // continuation exactly once.
            let box = ContinuationBox(continuation: continuation)
            let userData = Unmanaged.passRetained(box).toOpaque()
            let rc = sql.withCString { sqlPtr -> Int32 in
                dbfy_engine_query_async_v1(h, sqlPtr, dbfyQueryCallback, userData)
            }
            if rc != 0 {
                // Synchronous spawn failure (null pointer or invalid
                // UTF-8). Release the box; resume now.
                Unmanaged<ContinuationBox>.fromOpaque(userData).release()
                continuation.resume(throwing: DbfyError.fromLastError("Engine.query"))
            }
        }
    }
}

/// Opaque batch handle. The Arrow C Data Interface exporter is
/// available via `exportBatch(at:)` once a richer Arrow Swift API
/// stabilises; for v0.3 the result is a count-of-batches/rows
/// surface only.
public final class QueryResult {
    private var handle: OpaquePointer?

    fileprivate init(_ handle: OpaquePointer) {
        self.handle = handle
    }

    deinit {
        if let h = handle { dbfy_result_free(h) }
    }

    /// Number of RecordBatches the query produced.
    public var batchCount: Int {
        guard let h = handle else { return 0 }
        return Int(dbfy_result_batch_count(h))
    }

    /// Total number of rows across all batches.
    public var rowCount: Int {
        guard let h = handle else { return 0 }
        return Int(dbfy_result_row_count(h))
    }
}

// MARK: - C callback bridge --------------------------------------

/// Heap-allocated box that carries the Swift continuation across
/// the C callback. We `passRetained` on the way in and `release`
/// inside the callback so the box exists exactly as long as the
/// native query is in flight.
private final class ContinuationBox {
    let continuation: CheckedContinuation<QueryResult, Error>
    init(continuation: CheckedContinuation<QueryResult, Error>) {
        self.continuation = continuation
    }
}

/// C callback installed by `Engine.query(_:)`. Runs on a tokio
/// worker thread; the continuation it resumes is then scheduled on
/// the original Swift executor by the runtime.
private func dbfyQueryCallback(
    userData: UnsafeMutableRawPointer?,
    result: OpaquePointer?,
    errorMsg: UnsafePointer<CChar>?
) {
    guard let userData = userData else { return }
    let box = Unmanaged<ContinuationBox>.fromOpaque(userData).takeRetainedValue()
    if let result = result {
        box.continuation.resume(returning: QueryResult(result))
    } else {
        let msg = errorMsg.flatMap { String(cString: $0) } ?? "<unknown error>"
        box.continuation.resume(throwing: DbfyError(operation: "Engine.query", message: msg))
    }
}
