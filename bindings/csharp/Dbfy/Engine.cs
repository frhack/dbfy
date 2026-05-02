// Public API: Dbfy.Engine — wraps the opaque DbfyEngine* handle.
//
// Lifetime: the Engine owns the native handle; Dispose / GC release
// it. All methods throw DbfyException on native failure (the message
// is the dbfy_last_error string, e.g. "config load: invalid root path").

using System;
using System.Runtime.InteropServices;

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
