// Internal P/Invoke surface over the dbfy-c library (`libdbfy`).
//
// The native library is loaded by name; .NET's runtime will look it up
// in the standard search path, the directory containing the managed
// assembly, and — if set — the directory in `DBFY_NATIVE_DIR`. The
// nuget package ships per-RID natives under `runtimes/<rid>/native/`
// which .NET picks up automatically; for local development without
// the package, set `DBFY_NATIVE_DIR=target/release` before running
// tests.
//
// All entry points mirror `crates/dbfy-c/include/dbfy.h` exactly.
// Public Engine / Result classes wrap these calls and translate the
// `dbfy_last_error` thread-local into managed exceptions.

using System;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Dbfy;

internal static class Native
{
    private const string LibraryName = "dbfy";

    static Native()
    {
        NativeLibrary.SetDllImportResolver(typeof(Native).Assembly, Resolve);
    }

    /// <summary>
    /// Resolves <c>libdbfy</c> against the standard runtime search path
    /// plus an explicit override via <c>DBFY_NATIVE_DIR</c>. Useful when
    /// developing against a freshly-built <c>cargo build -p dbfy-c</c>
    /// before the NuGet package's <c>runtimes/</c> layout is populated.
    /// </summary>
    private static IntPtr Resolve(string name, Assembly assembly, DllImportSearchPath? searchPath)
    {
        if (name != LibraryName)
        {
            return IntPtr.Zero;
        }

        var dir = Environment.GetEnvironmentVariable("DBFY_NATIVE_DIR");
        if (!string.IsNullOrEmpty(dir))
        {
            foreach (var candidate in NativeFileNames())
            {
                var path = Path.Combine(dir, candidate);
                if (File.Exists(path) && NativeLibrary.TryLoad(path, out var handle))
                {
                    return handle;
                }
            }
        }

        // Fall through to the default resolver.
        return NativeLibrary.TryLoad(LibraryName, assembly, searchPath, out var fallback)
            ? fallback
            : IntPtr.Zero;
    }

    private static IEnumerable<string> NativeFileNames()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            yield return "libdbfy.so";
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            yield return "libdbfy.dylib";
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            yield return "dbfy.dll";
        }
    }

    // ---------------------------------------------------------------
    // P/Invoke declarations — must match dbfy-c/include/dbfy.h.
    // ---------------------------------------------------------------

    [DllImport(LibraryName, EntryPoint = "dbfy_last_error")]
    internal static extern IntPtr dbfy_last_error();

    [DllImport(LibraryName, EntryPoint = "dbfy_engine_new_from_yaml")]
    internal static extern IntPtr dbfy_engine_new_from_yaml(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string yaml);

    [DllImport(LibraryName, EntryPoint = "dbfy_engine_new_from_path")]
    internal static extern IntPtr dbfy_engine_new_from_path(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string path);

    [DllImport(LibraryName, EntryPoint = "dbfy_engine_new_empty")]
    internal static extern IntPtr dbfy_engine_new_empty();

    [DllImport(LibraryName, EntryPoint = "dbfy_engine_free")]
    internal static extern void dbfy_engine_free(IntPtr engine);

    [DllImport(LibraryName, EntryPoint = "dbfy_engine_explain")]
    internal static extern IntPtr dbfy_engine_explain(IntPtr engine,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string sql);

    [DllImport(LibraryName, EntryPoint = "dbfy_string_free")]
    internal static extern void dbfy_string_free(IntPtr s);

    [DllImport(LibraryName, EntryPoint = "dbfy_engine_query")]
    internal static extern int dbfy_engine_query(IntPtr engine,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string sql,
        out IntPtr outResult);

    /// <summary>
    /// Async-query entry point. The native side spawns the query on
    /// its tokio runtime and invokes the callback exactly once when
    /// the query terminates. The callback runs on a tokio worker
    /// thread — managed continuations must use
    /// <c>TaskCreationOptions.RunContinuationsAsynchronously</c> to
    /// avoid running user code there.
    /// </summary>
    /// <param name="engine">Engine handle.</param>
    /// <param name="sql">UTF-8 SQL query.</param>
    /// <param name="callback">Native function pointer to a static
    /// <c>[UnmanagedCallersOnly]</c> stub. Signature:
    /// <c>void(void* userData, DbfyResult* result, const char* errorMsg)</c>.
    /// On success, <paramref name="result"/> is non-null and the caller
    /// must release it via <see cref="dbfy_result_free"/>; on failure,
    /// <paramref name="result"/> is null and <paramref name="errorMsg"/>
    /// points to a UTF-8 string valid only until the callback returns.</param>
    /// <param name="userData">Opaque pointer round-tripped to the
    /// callback. Bindings stash a pinned <see cref="GCHandle"/> here.</param>
    [DllImport(LibraryName, EntryPoint = "dbfy_engine_query_async_v1")]
    internal static extern unsafe int dbfy_engine_query_async_v1(
        IntPtr engine,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string sql,
        delegate* unmanaged[Cdecl]<IntPtr, IntPtr, IntPtr, void> callback,
        IntPtr userData);

    [DllImport(LibraryName, EntryPoint = "dbfy_result_batch_count")]
    internal static extern UIntPtr dbfy_result_batch_count(IntPtr result);

    [DllImport(LibraryName, EntryPoint = "dbfy_result_row_count")]
    internal static extern UIntPtr dbfy_result_row_count(IntPtr result);

    [DllImport(LibraryName, EntryPoint = "dbfy_result_export_batch")]
    internal static extern unsafe int dbfy_result_export_batch(
        IntPtr result,
        UIntPtr index,
        void* outArray,
        void* outSchema);

    [DllImport(LibraryName, EntryPoint = "dbfy_result_free")]
    internal static extern void dbfy_result_free(IntPtr result);

    /// <summary>
    /// Pulls the thread-local last-error message and copies it to a
    /// managed string. Returns <c>null</c> when no error is recorded.
    /// </summary>
    internal static string? LastError()
    {
        var ptr = dbfy_last_error();
        return ptr == IntPtr.Zero ? null : Marshal.PtrToStringUTF8(ptr);
    }
}
