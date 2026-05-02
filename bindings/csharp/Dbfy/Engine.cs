// Public API: Dbfy.Engine — wraps the opaque DbfyEngine* handle.
//
// Lifetime: the Engine owns the native handle; Dispose / GC release
// it. All methods throw DbfyException on native failure (the message
// is the dbfy_last_error string, e.g. "config load: invalid root path").

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Dbfy;

/// <summary>
/// A configured dbfy engine. Created from an inline YAML config
/// (<see cref="FromYaml"/>) or from a path (<see cref="FromPath"/>).
/// All instances are thread-safe to share for query execution; the
/// underlying Rust engine is <c>Send + Sync</c>.
/// </summary>
public sealed class Engine : IDisposable
{
    private IntPtr _handle;
    private bool _disposed;

    private Engine(IntPtr handle)
    {
        _handle = handle;
    }

    /// <summary>
    /// Build an engine from an inline YAML configuration string. See
    /// the dbfy README for the schema. Throws
    /// <see cref="DbfyException"/> on any parse / validate / source
    /// initialisation failure.
    /// </summary>
    public static Engine FromYaml(string yaml)
    {
        ArgumentNullException.ThrowIfNull(yaml);
        var handle = Native.dbfy_engine_new_from_yaml(yaml);
        if (handle == IntPtr.Zero)
        {
            throw DbfyException.FromLastError("Engine.FromYaml");
        }
        return new Engine(handle);
    }

    /// <summary>
    /// Build an engine from a path to a YAML configuration file.
    /// </summary>
    public static Engine FromPath(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        var handle = Native.dbfy_engine_new_from_path(path);
        if (handle == IntPtr.Zero)
        {
            throw DbfyException.FromLastError("Engine.FromPath");
        }
        return new Engine(handle);
    }

    /// <summary>
    /// Build an engine with no preconfigured sources. Programmatic
    /// providers (in-memory tables, Python-defined sources, …) can
    /// still be registered later via the Rust-side API.
    /// </summary>
    public static Engine NewEmpty()
    {
        var handle = Native.dbfy_engine_new_empty();
        if (handle == IntPtr.Zero)
        {
            throw DbfyException.FromLastError("Engine.NewEmpty");
        }
        return new Engine(handle);
    }

    /// <summary>
    /// Run <paramref name="sql"/> against this engine and return a
    /// <see cref="Result"/> over the produced Arrow batches. The
    /// caller owns the returned object and must dispose it.
    /// </summary>
    public Result Query(string sql)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ThrowIfDisposed();

        IntPtr resultHandle;
        var rc = Native.dbfy_engine_query(_handle, sql, out resultHandle);
        if (rc != 0 || resultHandle == IntPtr.Zero)
        {
            throw DbfyException.FromLastError("Engine.Query");
        }
        return new Result(resultHandle);
    }

    /// <summary>
    /// Run <paramref name="sql"/> asynchronously. The native engine's
    /// tokio runtime drives the query (so the call returns immediately)
    /// and a managed continuation is woken when the query terminates.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Continuations on the returned <see cref="Task{TResult}"/> are
    /// scheduled with <c>RunContinuationsAsynchronously</c> so they do
    /// <i>not</i> run inline on a tokio worker thread — managed code
    /// always observes the result back on a thread-pool thread (or
    /// the captured <see cref="SynchronizationContext"/> if any).
    /// </para>
    /// <para>
    /// Cancellation: if <paramref name="cancellationToken"/> fires before
    /// the native side delivers a result, the returned Task transitions
    /// to <see cref="TaskStatus.Canceled"/>. The native query may still
    /// run to completion — there is no native cancellation token in
    /// the v1 ABI yet — but the managed Result, if it lands afterwards,
    /// is freed inside the callback so no leak occurs.
    /// </para>
    /// </remarks>
    public Task<Result> QueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ThrowIfDisposed();

        var tcs = new TaskCompletionSource<Result>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        // Pin a strong reference to the TCS so the GC can't move /
        // collect it before the native callback fires. The handle is
        // released in `OnQueryComplete` (success / failure) or in the
        // cancellation branch below.
        var gcHandle = GCHandle.Alloc(tcs);

        if (cancellationToken.CanBeCanceled)
        {
            // Pre-check + registration: if cancellation lands first,
            // synthesise a cancelled Task and let the native callback
            // run into a no-op (we mark the TCS, which is idempotent
            // via TrySetResult / TrySetException).
            cancellationToken.Register(() =>
            {
                tcs.TrySetCanceled(cancellationToken);
            });
        }

        unsafe
        {
            int rc;
            try
            {
                rc = Native.dbfy_engine_query_async_v1(
                    _handle,
                    sql,
                    &OnQueryComplete,
                    GCHandle.ToIntPtr(gcHandle));
            }
            catch
            {
                // P/Invoke marshalling failure — release the GCHandle
                // before propagating, otherwise we leak the TCS pin.
                gcHandle.Free();
                throw;
            }
            if (rc != 0)
            {
                gcHandle.Free();
                throw DbfyException.FromLastError("Engine.QueryAsync");
            }
        }

        return tcs.Task;
    }

    /// <summary>
    /// Native callback invoked exactly once per <see cref="QueryAsync"/>
    /// call. Translates the (result handle, error string) pair into a
    /// <see cref="TaskCompletionSource{TResult}"/> outcome.
    /// </summary>
    [UnmanagedCallersOnly(CallConvs = new[] { typeof(CallConvCdecl) })]
    private static void OnQueryComplete(IntPtr userData, IntPtr resultHandle, IntPtr errorMsg)
    {
        var gcHandle = GCHandle.FromIntPtr(userData);
        try
        {
            var tcs = (TaskCompletionSource<Result>)gcHandle.Target!;
            if (resultHandle != IntPtr.Zero)
            {
                var result = new Result(resultHandle);
                if (!tcs.TrySetResult(result))
                {
                    // The TCS was already cancelled — drop the result
                    // so we don't leak the native handle.
                    result.Dispose();
                }
            }
            else
            {
                var msg = errorMsg == IntPtr.Zero
                    ? "<unknown error>"
                    : Marshal.PtrToStringUTF8(errorMsg) ?? "<unknown error>";
                tcs.TrySetException(new DbfyException("Engine.QueryAsync", msg));
            }
        }
        finally
        {
            gcHandle.Free();
        }
    }

    /// <summary>
    /// Render the optimised logical plan plus per-source pushdown
    /// summary as a human-readable string.
    /// </summary>
    public string Explain(string sql)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ThrowIfDisposed();

        var ptr = Native.dbfy_engine_explain(_handle, sql);
        if (ptr == IntPtr.Zero)
        {
            throw DbfyException.FromLastError("Engine.Explain");
        }
        try
        {
            return Marshal.PtrToStringUTF8(ptr) ?? string.Empty;
        }
        finally
        {
            Native.dbfy_string_free(ptr);
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(Engine));
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }
        _disposed = true;
        if (_handle != IntPtr.Zero)
        {
            Native.dbfy_engine_free(_handle);
            _handle = IntPtr.Zero;
        }
        GC.SuppressFinalize(this);
    }

    ~Engine()
    {
        Dispose();
    }
}
