// Run with:
//   ./gradlew :examples:run-java   (when wired into the gradle multi-project)
// or standalone:
//   javac -cp dbfy-jvm-0.4.1.jar Demo.java
//   java -cp dbfy-jvm-0.4.1.jar:. Demo

package examples.lang.java;

import com.dbfy.Dbfy;

import java.io.ByteArrayInputStream;
import java.util.concurrent.CompletableFuture;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

public final class Demo {

    private static final String YAML = """
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
        """;

    public static void main(String[] args) throws Exception {
        try (Dbfy engine = Dbfy.fromYaml(YAML);
             RootAllocator allocator = new RootAllocator()) {

            // CompletableFuture — never blocks, future-completion runs
            // on a tokio worker that AttachCurrentThreads to the JVM.
            CompletableFuture<byte[]> future = engine.queryAsyncArrowIpc(
                    "SELECT id, title FROM posts.posts WHERE userId = 1 LIMIT 3");

            byte[] ipc = future.get();   // block in main, fine for a demo
            try (ArrowStreamReader reader = new ArrowStreamReader(
                     new ByteArrayInputStream(ipc), allocator)) {
                while (reader.loadNextBatch()) {
                    BigIntVector ids = (BigIntVector) reader.getVectorSchemaRoot()
                            .getVector("id");
                    VarCharVector titles = (VarCharVector) reader.getVectorSchemaRoot()
                            .getVector("title");
                    for (int i = 0; i < ids.getValueCount(); i++) {
                        System.out.printf("%3d | %s%n",
                                ids.get(i),
                                new String(titles.get(i)));
                    }
                }
            }
        }
    }
}
