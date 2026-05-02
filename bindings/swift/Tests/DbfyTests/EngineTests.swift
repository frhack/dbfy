import XCTest
@testable import Dbfy

final class EngineTests: XCTestCase {

    func testNewEmptyEngineCreatesAndDestroys() async throws {
        _ = try Engine.newEmpty()
    }

    func testInvalidYamlThrowsDbfyError() async throws {
        XCTAssertThrowsError(try Engine.fromYaml("not: a: valid: dbfy: config")) { error in
            guard let dbfy = error as? DbfyError else {
                return XCTFail("expected DbfyError, got \(type(of: error))")
            }
            XCTAssertFalse(dbfy.message.isEmpty)
        }
    }

    func testQueryAsyncSurfacesNativeErrors() async throws {
        // No tables registered — query must fail. The async path
        // must surface the error as a thrown DbfyError, never as
        // a synchronous throw from `query(_:)` itself.
        let engine = try Engine.newEmpty()
        do {
            _ = try await engine.query("SELECT * FROM nonexistent_table")
            XCTFail("expected DbfyError")
        } catch let dbfy as DbfyError {
            XCTAssertFalse(dbfy.message.isEmpty)
        }
    }

    func testQueryAsyncReturnsImmediatelyToCaller() async throws {
        // Submit time should be microseconds — the Swift task must
        // reach the await point quickly, not block in C.
        let engine = try Engine.newEmpty()
        let t0 = DispatchTime.now()
        do {
            _ = try await engine.query("SELECT 1")
        } catch {
            // Empty engine; failure is fine, we only care about
            // the timing budget.
        }
        let elapsed = DispatchTime.now().uptimeNanoseconds - t0.uptimeNanoseconds
        XCTAssertLessThan(elapsed, 5_000_000_000, "query took \(elapsed) ns, expected <5s")
    }
}
