// Build with the dbfy-kotlin Maven artifact:
//   implementation("com.dbfy:dbfy-kotlin:0.4.0")
// Then `kotlinc Demo.kt -include-runtime -d demo.jar && java -jar demo.jar`.

package examples.lang.kotlin

import com.dbfy.Dbfy
import com.dbfy.query
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.ipc.ArrowStreamReader
import java.io.ByteArrayInputStream

private val YAML = """
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
              id:     { path: "${'$'}.id",     type: int64  }
              userId: { path: "${'$'}.userId", type: int64  }
              title:  { path: "${'$'}.title",  type: string }
            pushdown:
              filters:
                userId: { param: userId, operators: ["="] }
""".trimIndent()

fun main() = runBlocking {
    Dbfy.fromYaml(YAML).use { engine ->
        // suspend fun — exception thrown directly, no
        // ExecutionException unwrap needed.
        val ipc: ByteArray = engine.query(
            "SELECT id, title FROM posts.posts WHERE userId = 1 LIMIT 3"
        )

        RootAllocator().use { allocator ->
            ArrowStreamReader(ByteArrayInputStream(ipc), allocator).use { reader ->
                while (reader.loadNextBatch()) {
                    val ids = reader.vectorSchemaRoot.getVector("id") as BigIntVector
                    val titles = reader.vectorSchemaRoot.getVector("title") as VarCharVector
                    for (i in 0 until ids.valueCount) {
                        println("%3d | %s".format(ids.get(i), String(titles.get(i))))
                    }
                }
            }
        }
    }
}
