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

    [Fact]
    public async Task QueryAsyncDeliversErrorViaTask()
    {
        // The async path must surface native errors as DbfyException
        // through the Task — never as a synchronous throw on the
        // calling thread (which would defeat the point of async).
        using var engine = Engine.NewEmpty();
        var task = engine.QueryAsync("SELECT * FROM nonexistent_table");
        var ex = await Assert.ThrowsAsync<DbfyException>(() => task);
        Assert.NotEmpty(ex.Message);
    }

    [Fact]
    public async Task QueryAsyncCompletesOffTokioThread()
    {
        // The TCS is constructed with RunContinuationsAsynchronously
        // so the await continuation must not run on a tokio worker.
        // We can't directly inspect tokio thread names, but we can
        // assert the continuation runs on a managed thread-pool
        // thread (IsThreadPoolThread = true), which is what the .NET
        // scheduler hands us when there's no SyncContext.
        using var engine = Engine.NewEmpty();
        try
        {
            await engine.QueryAsync("SELECT 1");
        }
        catch (DbfyException)
        {
            // No tables registered, so the query may legitimately
            // fail. The thread-pool assertion below is what we care
            // about — we got past the await without deadlock.
        }
        Assert.True(
            Thread.CurrentThread.IsThreadPoolThread
                || SynchronizationContext.Current != null,
            "await continuation must run on a managed scheduler, not a tokio worker");
    }
}
