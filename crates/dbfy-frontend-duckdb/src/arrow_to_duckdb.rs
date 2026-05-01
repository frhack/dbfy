//! Arrow → DuckDB DataChunk column writer, shared between the `dbfy_rest`
//! and `dbfy_rows_file` table functions.
//!
//! Each match arm follows the same pattern: write the typed slice (or
//! `Inserter` for variable-length strings), then walk the null mask and
//! mark null slots. Both halves are needed because `as_mut_slice` only
//! sees the raw value buffer.

use std::error::Error as StdError;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::DataType as ArrowDataType;
use dbfy_config::DataType;
use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};

pub fn arrow_data_type_to_duckdb(t: &DataType) -> LogicalTypeHandle {
    use LogicalTypeId as L;
    let id = match t {
        DataType::Boolean => L::Boolean,
        DataType::Int64 => L::Bigint,
        DataType::Float64 => L::Double,
        DataType::String => L::Varchar,
        DataType::Date => L::Date,
        DataType::Timestamp => L::Timestamp,
        DataType::Json => L::Varchar,
        DataType::Decimal | DataType::List | DataType::Struct => L::Varchar,
    };
    LogicalTypeHandle::from(id)
}

pub fn cell_type_to_duckdb(t: dbfy_config::CellTypeConfig) -> LogicalTypeHandle {
    use LogicalTypeId as L;
    use dbfy_config::CellTypeConfig as C;
    LogicalTypeHandle::from(match t {
        C::Int64 => L::Bigint,
        C::Float64 => L::Double,
        C::String => L::Varchar,
        C::Timestamp => L::Timestamp,
    })
}

pub fn write_arrow_column(
    arr: &ArrayRef,
    chunk: &mut DataChunkHandle,
    col_idx: usize,
    n: usize,
) -> Result<(), Box<dyn StdError>> {
    let mut col = chunk.flat_vector(col_idx);

    match arr.data_type() {
        ArrowDataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            {
                let slice = col.as_mut_slice::<bool>();
                for i in 0..n {
                    if !a.is_null(i) {
                        slice[i] = a.value(i);
                    }
                }
            }
            for i in 0..n {
                if a.is_null(i) {
                    col.set_null(i);
                }
            }
        }
        ArrowDataType::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            {
                let slice = col.as_mut_slice::<i64>();
                for i in 0..n {
                    if !a.is_null(i) {
                        slice[i] = a.value(i);
                    }
                }
            }
            for i in 0..n {
                if a.is_null(i) {
                    col.set_null(i);
                }
            }
        }
        ArrowDataType::Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            {
                let slice = col.as_mut_slice::<f64>();
                for i in 0..n {
                    if !a.is_null(i) {
                        slice[i] = a.value(i);
                    }
                }
            }
            for i in 0..n {
                if a.is_null(i) {
                    col.set_null(i);
                }
            }
        }
        ArrowDataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..n {
                if a.is_null(i) {
                    col.set_null(i);
                } else {
                    col.insert(i, a.value(i));
                }
            }
        }
        ArrowDataType::Date32 => {
            let a = arr.as_any().downcast_ref::<Date32Array>().unwrap();
            {
                let slice = col.as_mut_slice::<i32>();
                for i in 0..n {
                    if !a.is_null(i) {
                        slice[i] = a.value(i);
                    }
                }
            }
            for i in 0..n {
                if a.is_null(i) {
                    col.set_null(i);
                }
            }
        }
        ArrowDataType::Timestamp(_, _) => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or("expected TimestampMicrosecondArray")?;
            {
                let slice = col.as_mut_slice::<i64>();
                for i in 0..n {
                    if !a.is_null(i) {
                        slice[i] = a.value(i);
                    }
                }
            }
            for i in 0..n {
                if a.is_null(i) {
                    col.set_null(i);
                }
            }
        }
        other => {
            return Err(format!("dbfy: arrow type `{other}` is not yet writable to DuckDB").into());
        }
    }
    Ok(())
}
