// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Column access helpers that normalize OTAP column encodings to canonical Arrow types.

use arrow::array::{Array, ArrayRef, RecordBatch, StructArray, UInt32Array, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::DataType;

use super::error::LakeError;

/// Top-level column by name, if present (OTAP omits all-null columns).
#[must_use]
pub fn col<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a ArrayRef> {
    batch.column_by_name(name)
}

/// Field of a struct column, if both exist.
#[must_use]
pub fn struct_field(batch: &RecordBatch, struct_name: &str, field: &str) -> Option<ArrayRef> {
    let s = batch.column_by_name(struct_name)?;
    let s = s.as_any().downcast_ref::<StructArray>()?;
    s.column_by_name(field).cloned()
}

/// Cast `array` (possibly dictionary encoded / narrower int) to `to`, or produce an all-null array of
/// length `len` when the column is absent.
pub fn cast_or_null(
    array: Option<&ArrayRef>,
    to: &DataType,
    len: usize,
) -> Result<ArrayRef, LakeError> {
    match array {
        Some(a) if a.data_type() == to => Ok(a.clone()),
        Some(a) => Ok(cast(a, to)?),
        None => Ok(new_null_array(to, len)),
    }
}

/// Id / parent-id column as `UInt32`. Required: an error if absent.
pub fn ids_u32(
    array: Option<&ArrayRef>,
    table: &'static str,
    column: &'static str,
) -> Result<UInt32Array, LakeError> {
    let array = array.ok_or(LakeError::MissingColumn { table, column })?;
    Ok(as_u32(&cast(array, &DataType::UInt32)?))
}

/// Optional id column as `UInt32` (null ids when absent).
pub fn opt_ids_u32(array: Option<ArrayRef>, len: usize) -> Result<UInt32Array, LakeError> {
    Ok(as_u32(&cast_or_null(
        array.as_ref(),
        &DataType::UInt32,
        len,
    )?))
}

/// Downcast an array known to be `UInt32` (result of a cast or `take` on a `UInt32Array`).
#[must_use]
pub fn as_u32(array: &ArrayRef) -> UInt32Array {
    array
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("array was cast to UInt32")
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{DictionaryArray, StringArray, UInt16Array};
    use arrow::datatypes::{Field, Schema, UInt8Type};
    use std::sync::Arc;

    /// Scenario: A dictionary-encoded string column, a UInt16 id column and an absent column are read.
    /// Guarantees: Values are cast to the canonical type and an absent column yields all nulls of the batch length.
    #[test]
    fn cast_or_null_normalizes_dictionary_ints_and_absent_columns() {
        let dict: DictionaryArray<UInt8Type> = vec!["a", "b", "a"].into_iter().collect();
        let ids = UInt16Array::from(vec![1, 2, 3]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", dict.data_type().clone(), true),
            Field::new("id", DataType::UInt16, true),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(dict), Arc::new(ids)]).expect("batch");

        let s = cast_or_null(col(&batch, "s"), &DataType::Utf8, 3).expect("cast");
        let s = s.as_any().downcast_ref::<StringArray>().expect("utf8");
        assert_eq!(s.value(2), "a");
        let ids = ids_u32(col(&batch, "id"), "t", "id").expect("ids");
        assert_eq!(ids.values().to_vec(), vec![1, 2, 3]);
        let missing = cast_or_null(col(&batch, "nope"), &DataType::Int64, 3).expect("nulls");
        assert_eq!(missing.null_count(), 3);
        assert!(matches!(
            ids_u32(col(&batch, "nope"), "t", "nope"),
            Err(LakeError::MissingColumn { .. })
        ));
    }
}
