// Smoke tests for the C# binding.
//
// These tests don't talk to the network; they exercise the Engine
// lifecycle, error path (bad YAML), and an in-memory programmatic
// query path. The full integration story (real REST source, Arrow
// batch import) is exercised by the Rust-side tests; here we only
// need to confirm the P/Invoke surface and Arrow C Data Interface
// import work end-to-end.

using System;
using Dbfy;

namespace Dbfy.Tests;

public class EngineTests
{
    [Fact]
    public void EmptyEngineCanBeCreatedAndDisposed()
    {
        using var engine = Engine.NewEmpty();
        Assert.NotNull(engine);
    }

    [Fact]
    public void InvalidYamlThrowsDbfyException()
    {
        var ex = Assert.Throws<DbfyException>(() =>
            Engine.FromYaml("not: a: valid: dbfy: config"));
        Assert.Contains("Engine.FromYaml", ex.Operation);
        Assert.NotEmpty(ex.Message);
    }

    [Fact]
    public void ExplainOnEmptyEngineReturnsErrorOrPlan()
    {
        using var engine = Engine.NewEmpty();
        // No tables registered; either the explain succeeds with a
        // plan-against-empty-catalog message or it throws DbfyException.
        // Both paths exercise the Marshal.PtrToStringUTF8 + dbfy_string_free
        // pair. We accept either outcome.
        try
        {
            var explained = engine.Explain("SELECT 1");
            Assert.NotNull(explained);
        }
        catch (DbfyException)
        {
            // The crucial thing is we got a typed exception, not a
            // segfault or a null deref through the marshalling.
        }
    }

    [Fact]
    public void QueryErrorRoundTripsViaLastError()
    {
        using var engine = Engine.NewEmpty();
        var ex = Assert.Throws<DbfyException>(() =>
            engine.Query("SELECT * FROM nonexistent_table"));
        Assert.Contains("Engine.Query", ex.Operation);
        Assert.NotEmpty(ex.Message);
    }
}
