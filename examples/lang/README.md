# dbfy hello-world per language

The same canonical query expressed seven ways. Each example builds
an in-process engine pointed at the public `jsonplaceholder.typicode.com`
REST API (no auth, no setup), runs

```sql
SELECT id, title FROM posts.posts WHERE userId = 1 LIMIT 3
```

and prints the rows. The point is not the query — it's the
*shape* of the API in each language: where async lives, how
errors surface, what the result type looks like.

| Language       | Path            | Async API used                              |
|----------------|-----------------|---------------------------------------------|
| Rust           | `rust/`         | `engine.query(...).await`                   |
| Python         | `python/`       | sync (mirrors C# / Java patterns visually)  |
| C# / .NET      | `csharp/`       | `await engine.QueryAsync(...)`              |
| Java           | `java/`         | `engine.queryAsyncArrowIpc(sql).get()`      |
| Kotlin         | `kotlin/`       | `engine.query(sql)` (suspend)               |
| Node.js        | `node/`         | `await engine.query(sql)`                   |
| Swift          | `swift/`        | `try await engine.query(sql)`               |

All use the same YAML config inline, so you can read any one of
them and recognise the pattern in any other.
