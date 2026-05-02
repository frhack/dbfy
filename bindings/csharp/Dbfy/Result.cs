// Public API: Dbfy.Result — iterates over Arrow record batches produced
// by Engine.Query. Each batch is imported from the Arrow C Data
// Interface zero-copy: the struct dbfy fills (ArrowArray + ArrowSchema)
// is consumed by Apache.Arrow.C.CArrowArrayImporter, which transfers
// ownership and registers the underlying buffers' release callbacks.
//
// Lifetime: dispose Result to free the native DbfyResult handle. Each
// imported RecordBatch keeps its own buffers alive beyond Result
// disposal, so you can hold them after `Result` is gone.

using System;
using System.Collections;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.C;

namespace Dbfy;

/// <summary>
/// The set of record batches produced by <see cref="Engine.Query"/>.
/// Iteration imports each batch on demand from the Arrow C Data
/// Interface. Disposing the Result releases the native side; imported
/// batches survive (Arrow C# tracks their buffers separately).
/// </summary>
public sealed class Result : IDisposable, IEnumerable<RecordBatch>
{
    private IntPtr _handle;
    private bool _disposed;

    internal Result(IntPtr handle)
    {
        _handle = handle;
    }

    /// <summary>Number of batches in the result.</summary>
    public ulong BatchCount
    {
        get
        {
            ThrowIfDisposed();
            return (ulong)Native.dbfy_result_batch_count(_handle);
        }
    }

    /// <summary>Total rows summed across all batches.</summary>
    public ulong RowCount
    {
        get
        {
            ThrowIfDisposed();
            return (ulong)Native.dbfy_result_row_count(_handle);
        }
    }

    /// <summary>
    /// Import the batch at <paramref name="index"/> as an
    /// <see cref="Apache.Arrow.RecordBatch"/>. The transfer is
    /// zero-copy through the Arrow C Data Interface; the resulting
    /// batch owns its buffers and must be disposed independently.
    /// </summary>
    public RecordBatch GetBatch(ulong index)
    {
        ThrowIfDisposed();
        if (index >= BatchCount)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        unsafe
        {
            // Allocate the C-Data-Interface struct slots that dbfy will
            // populate. Both `Create` calls use unmanaged memory; they
            // are released by the importers below (which take ownership)
            // or, on failure, manually here.
            var arrayPtr = CArrowArray.Create();
            var schemaPtr = CArrowSchema.Create();

            try
            {
                var rc = Native.dbfy_result_export_batch(
                    _handle, (UIntPtr)index, arrayPtr, schemaPtr);
                if (rc != 0)
                {
                    throw DbfyException.FromLastError("Result.GetBatch");
                }

                // ImportSchema takes ownership of `schemaPtr` and will
                // free it via the schema's release callback. ImportRecordBatch
                // does the same for `arrayPtr`.
                var schema = CArrowSchemaImporter.ImportSchema(schemaPtr);
                var batch = CArrowArrayImporter.ImportRecordBatch(arrayPtr, schema);
                return batch;
            }
            catch
            {
                CArrowArray.Free(arrayPtr);
                CArrowSchema.Free(schemaPtr);
                throw;
            }
        }
    }

    public IEnumerator<RecordBatch> GetEnumerator()
    {
        var n = BatchCount;
        for (ulong i = 0; i < n; i++)
        {
            yield return GetBatch(i);
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(Result));
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
            Native.dbfy_result_free(_handle);
            _handle = IntPtr.Zero;
        }
        GC.SuppressFinalize(this);
    }

    ~Result()
    {
        Dispose();
    }
}
