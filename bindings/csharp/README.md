# Dbfy — .NET binding

Each source is a SQL table: REST APIs, log files, CSV, JSONL, syslog —
one schema, cross-source JOINs. .NET wrapper over the dbfy embedded
SQL federation engine.

```csharp
using Dbfy;
using Apache.Arrow;

using var engine = Engine.FromYaml("""
    version: 1
    sources:
      gh:
        type: rest
        base_url: https://api.github.com
        auth: { type: bearer, token_env: GITHUB_TOKEN }
        tables:
          issues:
            endpoint: { method: GET, path: /repos/duckdb/duckdb/issues }
            root: "$[*]"
            columns:
              number: { path: "$.number", type: int64 }
              title:  { path: "$.title",  type: string }
              state:  { path: "$.state",  type: string }
    """);

using var result = engine.Query(
    "SELECT count(*) FROM gh.issues WHERE state = 'open'");

Console.WriteLine($"open: {result.RowCount}");

foreach (RecordBatch batch in result)
{
    // Apache.Arrow.RecordBatch — typed columns, zero-copy import via
    // the C Data Interface. Plug into Microsoft.Data.Analysis,
    // PowerBI's PowerQuery, LINQ, or any other Arrow-native consumer.
}
```

## Install

```sh
dotnet add package Dbfy
```

The NuGet package ships native binaries for `linux-x64` and
`osx-arm64` under `runtimes/<rid>/native/`. Other platforms can build
from source — see the workspace [README](https://github.com/frhack/dbfy).

## API surface

| Type | Purpose |
|---|---|
| `Engine.FromYaml(string)` | Build engine from inline YAML config |
| `Engine.FromPath(string)` | Build engine from a YAML file path |
| `Engine.NewEmpty()` | Engine with no preconfigured sources |
| `Engine.Query(sql)` | Execute SQL → `Result` |
| `Engine.Explain(sql)` | Render optimised plan + pushdown summary |
| `Result.BatchCount` / `RowCount` | Result sizes |
| `Result.GetBatch(i)` | Import the i-th batch as `Apache.Arrow.RecordBatch` |
| `IEnumerable<RecordBatch>` on `Result` | `foreach` iteration |
| `DbfyException` | Wraps `dbfy_last_error` thread-local |

## Architecture

The .NET binding is a thin P/Invoke layer over the existing C library
(`dbfy-c`, [include/dbfy.h](https://github.com/frhack/dbfy/blob/main/crates/dbfy-c/include/dbfy.h)).
The dbfy engine emits Arrow record batches via the [Arrow C Data
Interface](https://arrow.apache.org/docs/format/CDataInterface.html);
the .NET side imports them zero-copy through
`Apache.Arrow.C.CArrowArrayImporter` so each `RecordBatch` shares
buffers with the original Rust-side allocation. There is no
serialisation step.

```
┌──────────────────┐  Arrow C Data Interface (zero-copy)  ┌────────────┐
│  Rust dbfy core  │ ─────────────────────────────────▶  │  Apache    │
│  (DataFusion)    │                                       │  .Arrow    │
└──────────────────┘                                       │  C# types  │
                                                           └────────────┘
```

## Local development

```sh
# 1. Build the native dbfy library.
cargo build -p dbfy-c --release         # produces target/release/libdbfy.so

# 2. Point the .NET resolver at it.
export DBFY_NATIVE_DIR=$PWD/target/release

# 3. Build + test.
dotnet test bindings/csharp/Dbfy.sln
```

`Dbfy/Native.cs` installs an `NativeLibrary.SetDllImportResolver` that
looks at `DBFY_NATIVE_DIR` first, then falls through to the standard
runtime search path (which is what NuGet's `runtimes/<rid>/native`
populates).

## License

Apache-2.0 — same as the parent dbfy workspace.
