// dbfy hello-world for .NET. Add the package:
//   dotnet add package Dbfy
// then run with `dotnet run`.

using System;
using System.Threading.Tasks;
using Dbfy;
using Apache.Arrow;
using Apache.Arrow.Ipc;

const string yaml = @"
version: 1
sources:
  posts:
    type: rest
    base_url: https://jsonplaceholder.typicode.com
    tables:
      posts:
        endpoint: { method: GET, path: /posts }
        root: ""$[*]""
        columns:
          id:     { path: ""$.id"",     type: int64 }
          userId: { path: ""$.userId"", type: int64 }
          title:  { path: ""$.title"",  type: string }
        pushdown:
          filters:
            userId: { param: userId, operators: [""=""] }
";

using var engine = Engine.FromYaml(yaml);

// Async API — never blocks the calling thread; continuations
// scheduled with RunContinuationsAsynchronously so they don't
// run on a tokio worker.
using var result = await engine.QueryAsync(
    "SELECT id, title FROM posts.posts WHERE userId = 1 LIMIT 3");

Console.WriteLine($"batches={result.BatchCount}, rows={result.RowCount}");

foreach (var batch in result.ExportAllBatches())
{
    var ids    = (Int64Array)batch.Column("id");
    var titles = (StringArray)batch.Column("title");
    for (int i = 0; i < batch.Length; i++)
    {
        Console.WriteLine($"{ids.GetValue(i),3} | {titles.GetString(i)}");
    }
}
