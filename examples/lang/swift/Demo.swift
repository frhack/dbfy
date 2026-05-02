// Run inside an Xcode project / SwiftPM target with the dbfy
// dependency declared:
//
//   dependencies: [
//     .package(url: "https://github.com/frhack/dbfy", from: "0.4.0"),
//   ]
//
// Then call `await runDemo()` from your async entry point.

import Dbfy
import Foundation

let yaml = """
version: 1
sources:
  posts:
    type: rest
    base_url: https://jsonplaceholder.typicode.com
    tables:
      posts:
        endpoint: { method: GET, path: /posts }
        root: "$[*]"
        columns:
          id:     { path: "$.id",     type: int64  }
          userId: { path: "$.userId", type: int64  }
          title:  { path: "$.title",  type: string }
        pushdown:
          filters:
            userId: { param: userId, operators: ["="] }
"""

@main
struct Demo {
    static func main() async throws {
        let engine = try Engine.fromYaml(yaml)

        // async/await — withCheckedThrowingContinuation under the
        // hood. Continuation resumes on the calling task's
        // executor, never on a tokio worker.
        let result = try await engine.query(
            "SELECT id, title FROM posts.posts WHERE userId = 1 LIMIT 3"
        )

        print("batches=\(result.batchCount), rows=\(result.rowCount)")
        // Once apache-arrow-swift stabilises, iterate rows here. For
        // v0.4 the Swift binding exposes counts; the Arrow C Data
        // Interface export hook will land in a follow-up.
    }
}
