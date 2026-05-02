// Exception type that surfaces the dbfy-c thread-local error message.

using System;

namespace Dbfy;

/// <summary>
/// Raised when a native dbfy call fails. The <see cref="Exception.Message"/>
/// is the string returned by <c>dbfy_last_error()</c> on the calling thread.
/// </summary>
public sealed class DbfyException : Exception
{
    /// <summary>The function name that surfaced the error.</summary>
    public string Operation { get; }

    public DbfyException(string operation, string message)
        : base($"{operation}: {message}")
    {
        Operation = operation;
    }

    /// <summary>
    /// Build a <see cref="DbfyException"/> from the current thread's
    /// dbfy_last_error, falling back to a generic message if none is set.
    /// </summary>
    internal static DbfyException FromLastError(string operation)
    {
        var msg = Native.LastError() ?? "unknown error (dbfy_last_error returned null)";
        return new DbfyException(operation, msg);
    }
}
