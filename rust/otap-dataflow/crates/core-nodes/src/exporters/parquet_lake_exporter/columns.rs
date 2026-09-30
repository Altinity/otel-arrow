// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Column access helpers that normalize OTAP column encodings to canonical Arrow types, and the
//! v1 cell rules (required columns, timestamps, counts). Only the column types of the OTAP schema
//! are read; every cast that can allocate more than its input is charged to the request budget.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Float64Builder, Int32Array, Int64Builder, ListBuilder, RecordBatch,
    StringArray, StructArray, TimestampMicrosecondBuilder, UInt32Array, new_null_array,
};
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Field, Float64Type, Int32Type, Int64Type, UInt32Type, UInt64Type,
};

use super::error::LakeError;
use super::limits::Budget;

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

/// A scalar type of the OTAP schema: fixed-width, or a string or binary with 32-bit offsets.
fn scalar(dt: &DataType) -> bool {
    dt.is_primitive()
        || matches!(
            dt,
            DataType::Boolean
                | DataType::Null
                | DataType::Utf8
                | DataType::Binary
                | DataType::FixedSizeBinary(_)
        )
}

/// Refuse a column whose type is not one the OTAP schema uses: a scalar, a dictionary over a
/// scalar, or a list of primitives, of a dictionary of primitives or of a struct of primitives.
/// Large and view types, other list kinds and nested dictionaries are refused before any cast,
/// because their size after a cast cannot be bounded from their buffers.
pub fn check_type(array: &ArrayRef) -> Result<(), LakeError> {
    let supported = match array.data_type() {
        DataType::Dictionary(_, value) => scalar(value),
        DataType::List(item) => match item.data_type() {
            DataType::Struct(fields) => fields.iter().all(|f| f.data_type().is_primitive()),
            DataType::Dictionary(_, value) => value.is_primitive(),
            other => other.is_primitive(),
        },
        other => scalar(other),
    };
    if supported {
        Ok(())
    } else {
        Err(LakeError::Invalid(format!(
            "column type {} is not supported",
            array.data_type()
        )))
    }
}

/// Cast `array` (possibly dictionary encoded / narrower int) to `to`, or produce an all-null array of
/// length `len` when the column is absent. A column of an unsupported type is invalid content.
pub fn cast_or_null(
    array: Option<&ArrayRef>,
    to: &DataType,
    len: usize,
) -> Result<ArrayRef, LakeError> {
    match array {
        Some(a) if a.data_type() == to => Ok(a.clone()),
        Some(a) => {
            check_type(a)?;
            Ok(cast(a, to)?)
        }
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
    check_type(array)?;
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

/// `LIST<item>` with non-null items, as the v1 list columns are declared.
#[must_use]
pub fn list_of(item: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", item, false)))
}

/// Bytes a cast of `array` to the plain type `to` allocates: nothing when the array already has
/// that type (the cast returns the same array); for a dictionary, the sum of the referenced value
/// sizes; otherwise the size of its buffers. Charged before the cast, so a small dictionary
/// referenced by many rows cannot expand without bound.
#[must_use]
pub fn cast_cost(array: &ArrayRef, to: &DataType) -> usize {
    if array.data_type() == to {
        return 0;
    }
    let Some(dict) = array.as_any_dictionary_opt() else {
        return array.get_buffer_memory_size();
    };
    let values = dict.values();
    let sizes: Vec<usize> = match values.data_type() {
        DataType::Utf8 => {
            let v = values.as_string::<i32>();
            (0..v.len())
                .map(|k| v.value_length(k) as usize + 4)
                .collect()
        }
        DataType::Binary => {
            let v = values.as_binary::<i32>();
            (0..v.len())
                .map(|k| v.value_length(k) as usize + 4)
                .collect()
        }
        DataType::FixedSizeBinary(w) => vec![*w as usize; values.len()],
        other => vec![other.primitive_width().unwrap_or(8); values.len()],
    };
    let keys = dict.keys();
    dict.normalized_keys()
        .iter()
        .enumerate()
        .filter(|(row, _)| keys.is_valid(*row))
        .map(|(_, k)| sizes.get(*k).copied().unwrap_or(0))
        .sum()
}

/// Utf8 column with nulls (and an absent column) read as `""`: the v1 string columns are required.
pub fn utf8_or_empty(
    array: Option<&ArrayRef>,
    len: usize,
    budget: &mut Budget,
) -> Result<StringArray, LakeError> {
    let Some(a) = array else {
        return Ok(StringArray::from(vec![""; len]));
    };
    budget.charge(cast_cost(a, &DataType::Utf8))?;
    let plain = cast_or_null(Some(a), &DataType::Utf8, len)?;
    let s = plain.as_string::<i32>();
    if s.null_count() == 0 {
        return Ok(s.clone());
    }
    // The column is rebuilt without nulls.
    budget.charge(s.get_buffer_memory_size())?;
    Ok(StringArray::from_iter_values(
        (0..s.len()).map(|i| if s.is_null(i) { "" } else { s.value(i) }),
    ))
}

/// Int32 column with nulls (and an absent column) read as 0 (`severity_number`).
pub fn i32_or_zero(array: Option<&ArrayRef>, len: usize) -> Result<ArrayRef, LakeError> {
    let a = cast_or_null(array, &DataType::Int32, len)?;
    let a = a.as_primitive::<Int32Type>();
    Ok(Arc::new(Int32Array::from_iter_values(
        (0..len).map(|i| if a.is_null(i) { 0 } else { a.value(i) }),
    )))
}

/// OTLP `u32` flags reinterpreted into the signed storage column; the top bit becomes the sign
/// bit. Null and absent read as 0.
#[allow(clippy::cast_possible_wrap)]
pub fn flags_i32(array: Option<&ArrayRef>, len: usize) -> Result<ArrayRef, LakeError> {
    let a = cast_or_null(array, &DataType::UInt32, len)?;
    let a = a.as_primitive::<UInt32Type>();
    Ok(Arc::new(Int32Array::from_iter_values((0..len).map(|i| {
        if a.is_null(i) { 0 } else { a.value(i) as i32 }
    }))))
}

/// The two v1 columns of a nanosecond timestamp: `TIMESTAMP(us, UTC)` and `INT64` nanoseconds.
/// 0 (also null and absent) is null in both; a negative value is null in both and counted.
pub fn timestamp_pair(
    array: Option<&ArrayRef>,
    len: usize,
    out_of_range: &mut u64,
) -> Result<(ArrayRef, ArrayRef), LakeError> {
    let ns = cast_or_null(array, &DataType::Int64, len)?;
    let ns = ns.as_primitive::<Int64Type>();
    let mut micros = TimestampMicrosecondBuilder::with_capacity(len);
    let mut nanos = Int64Builder::with_capacity(len);
    for i in 0..len {
        let v = if ns.is_null(i) { 0 } else { ns.value(i) };
        if v > 0 {
            micros.append_value(v / 1000);
            nanos.append_value(v);
        } else {
            if v < 0 {
                *out_of_range += 1;
            }
            micros.append_null();
            nanos.append_null();
        }
    }
    Ok((
        Arc::new(micros.finish().with_timezone("UTC")),
        Arc::new(nanos.finish()),
    ))
}

/// A `u64` count column as the signed storage type. A value above `i64::MAX` is invalid content.
/// Null and absent stay null, or read as 0 with `null_as_zero`.
pub fn u64_as_i64(
    array: Option<&ArrayRef>,
    len: usize,
    what: &'static str,
    null_as_zero: bool,
) -> Result<ArrayRef, LakeError> {
    let a = cast_or_null(array, &DataType::UInt64, len)?;
    let a = a.as_primitive::<UInt64Type>();
    let mut out = Int64Builder::with_capacity(len);
    for i in 0..len {
        if a.is_null(i) {
            if null_as_zero {
                out.append_value(0);
            } else {
                out.append_null();
            }
            continue;
        }
        out.append_value(
            i64::try_from(a.value(i))
                .map_err(|_| LakeError::Invalid(format!("{what} above i64::MAX")))?,
        );
    }
    Ok(Arc::new(out.finish()))
}

fn plain_list(array: &ArrayRef, item: DataType) -> Result<ArrayRef, LakeError> {
    check_type(array)?;
    Ok(cast(
        array,
        &DataType::List(Arc::new(Field::new("item", item, true))),
    )?)
}

/// A `LIST<u64>` count column as `LIST<INT64>` with non-null items. A null item or an item above
/// `i64::MAX` is invalid content. A null or absent list is an empty list with `null_as_empty`,
/// otherwise null.
pub fn list_u64_as_i64(
    array: Option<&ArrayRef>,
    len: usize,
    what: &'static str,
    null_as_empty: bool,
) -> Result<ArrayRef, LakeError> {
    let list = array.map(|a| plain_list(a, DataType::UInt64)).transpose()?;
    let list = list.as_ref().map(|l| l.as_list::<i32>());
    let mut out = ListBuilder::new(Int64Builder::new()).with_field(Field::new(
        "item",
        DataType::Int64,
        false,
    ));
    for row in 0..len {
        let Some(l) = list.filter(|l| l.is_valid(row)) else {
            out.append(null_as_empty);
            continue;
        };
        let items = l.value(row);
        let items = items.as_primitive::<UInt64Type>();
        for i in 0..items.len() {
            if items.is_null(i) {
                return Err(LakeError::Invalid(format!("null {what}")));
            }
            out.values().append_value(
                i64::try_from(items.value(i))
                    .map_err(|_| LakeError::Invalid(format!("{what} above i64::MAX")))?,
            );
        }
        out.append(true);
    }
    Ok(Arc::new(out.finish()))
}

/// A `LIST<f64>` column with non-null items (a null item is invalid content). Null and absent
/// lists as in [`list_u64_as_i64`].
pub fn list_f64(
    array: Option<&ArrayRef>,
    len: usize,
    what: &'static str,
    null_as_empty: bool,
) -> Result<ArrayRef, LakeError> {
    let list = array
        .map(|a| plain_list(a, DataType::Float64))
        .transpose()?;
    let list = list.as_ref().map(|l| l.as_list::<i32>());
    let mut out = ListBuilder::new(Float64Builder::new()).with_field(Field::new(
        "item",
        DataType::Float64,
        false,
    ));
    for row in 0..len {
        let Some(l) = list.filter(|l| l.is_valid(row)) else {
            out.append(null_as_empty);
            continue;
        };
        let items = l.value(row);
        let items = items.as_primitive::<Float64Type>();
        if items.null_count() > 0 {
            return Err(LakeError::Invalid(format!("null {what}")));
        }
        out.values().append_slice(items.values());
        out.append(true);
    }
    Ok(Arc::new(out.finish()))
}

/// Bytes each row of `batch` holds: string, binary, map and list content plus the fixed cells.
/// Used for the `ingress.max_row_bytes` check.
#[must_use]
pub fn row_bytes(batch: &RecordBatch) -> Vec<usize> {
    let mut out = vec![0_usize; batch.num_rows()];
    for column in batch.columns() {
        match column.data_type() {
            DataType::Utf8 => {
                let a = column.as_string::<i32>();
                for (i, o) in out.iter_mut().enumerate() {
                    *o += a.value_length(i) as usize + 4;
                }
            }
            DataType::Binary => {
                let a = column.as_binary::<i32>();
                for (i, o) in out.iter_mut().enumerate() {
                    *o += a.value_length(i) as usize + 4;
                }
            }
            DataType::Map(_, _) => {
                let map = column.as_map();
                let keys = map.keys().as_string::<i32>().value_offsets();
                let values = map.values().as_string::<i32>().value_offsets();
                let offsets = map.value_offsets();
                for (i, o) in out.iter_mut().enumerate() {
                    let (s, e) = (offsets[i] as usize, offsets[i + 1] as usize);
                    *o += (keys[e] - keys[s]) as usize
                        + (values[e] - values[s]) as usize
                        + 8 * (e - s)
                        + 4;
                }
            }
            DataType::List(item) => {
                let list = column.as_list::<i32>();
                let width = item.data_type().primitive_width().unwrap_or(16);
                for (i, o) in out.iter_mut().enumerate() {
                    *o += list.value_length(i) as usize * width + 4;
                }
            }
            DataType::FixedSizeBinary(w) => out.iter_mut().for_each(|o| *o += *w as usize),
            other => {
                let w = other.primitive_width().unwrap_or(1);
                out.iter_mut().for_each(|o| *o += w);
            }
        }
    }
    out
}

/// `take` of a string column whose source rows may repeat in `rows`: the copied bytes are charged
/// to the request budget first.
pub fn take_charged(
    array: &StringArray,
    rows: &UInt32Array,
    budget: &mut Budget,
) -> Result<ArrayRef, LakeError> {
    let bytes: usize = rows
        .iter()
        .flatten()
        .map(|r| array.value_length(r as usize) as usize)
        .sum();
    budget.charge(bytes)?;
    Ok(arrow::compute::take(array, rows, None)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        DictionaryArray, LargeStringArray, ListArray, ListViewArray, MapBuilder, StringBuilder,
        StringViewArray, UInt16Array, UInt64Array, UInt64Builder,
    };
    use arrow::buffer::ScalarBuffer;
    use arrow::datatypes::{Schema, TimeUnit, UInt8Type, UInt16Type};

    /// A `LIST<u64>` column from rows of optional items (`None` row: a null list).
    fn u64_lists(rows: &[Option<Vec<Option<u64>>>]) -> ArrayRef {
        let mut b = ListBuilder::new(UInt64Builder::new());
        for row in rows {
            match row {
                Some(items) => {
                    for item in items {
                        b.values().append_option(*item);
                    }
                    b.append(true);
                }
                None => b.append(false),
            }
        }
        Arc::new(b.finish())
    }

    fn is_invalid(result: Result<ArrayRef, LakeError>, what: &str) {
        match result {
            Err(LakeError::Invalid(msg)) => assert!(msg.contains(what), "{msg}"),
            other => panic!("expected an invalid-content refusal naming {what}, got {other:?}"),
        }
    }

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

    /// Scenario: Columns of types the OTAP schema does not use are read: a dictionary over LargeUtf8, a Utf8View, a LargeUtf8 and a ListView column.
    /// Guarantees: Each is refused as invalid content, naming the type, before any cast; so the size of a cast is always bounded by the buffers the cost estimate reads.
    #[test]
    fn unsupported_column_types_are_refused() {
        let large_values: ArrayRef = Arc::new(LargeStringArray::from(vec!["a", "b"]));
        let large_dict: ArrayRef = Arc::new(
            DictionaryArray::<UInt16Type>::try_new(UInt16Array::from(vec![0, 1, 0]), large_values)
                .expect("dictionary"),
        );
        let view: ArrayRef = Arc::new(StringViewArray::from(vec!["a", "b", "c"]));
        let large: ArrayRef = Arc::new(LargeStringArray::from(vec!["a", "b", "c"]));
        let list_view: ArrayRef = Arc::new(
            ListViewArray::try_new(
                Arc::new(Field::new("item", DataType::UInt64, true)),
                ScalarBuffer::from(vec![0_i32, 1, 2]),
                ScalarBuffer::from(vec![1_i32, 1, 1]),
                Arc::new(UInt64Array::from(vec![1, 2, 3])),
                None,
            )
            .expect("list view"),
        );
        for (array, name) in [
            (&large_dict, "LargeUtf8"),
            (&view, "Utf8View"),
            (&large, "LargeUtf8"),
            (&list_view, "ListView"),
        ] {
            let err = check_type(array).expect_err(name);
            assert!(matches!(err, LakeError::Invalid(_)), "{name}: {err}");
            assert!(err.to_string().contains(name), "{name}: {err}");
            is_invalid(
                cast_or_null(Some(array), &DataType::Utf8, 3),
                "not supported",
            );
        }
        // Every reader goes through the check.
        let mut budget = Budget::new(1 << 20);
        assert!(matches!(
            utf8_or_empty(Some(&view), 3, &mut budget),
            Err(LakeError::Invalid(_))
        ));
        assert!(matches!(
            ids_u32(Some(&large), "t", "id"),
            Err(LakeError::Invalid(_))
        ));
        is_invalid(
            list_u64_as_i64(Some(&list_view), 3, "bucket count", true),
            "not supported",
        );
        // The types the OTAP schema does use pass.
        let dict: ArrayRef = Arc::new(
            vec!["a", "b", "a"]
                .into_iter()
                .collect::<DictionaryArray<UInt8Type>>(),
        );
        check_type(&dict).expect("dictionary over Utf8");
        check_type(&u64_lists(&[Some(vec![Some(1)])])).expect("list of u64");
    }

    /// Scenario: A nanosecond column holds a positive time, 0, a negative time and a null; another column is absent.
    /// Guarantees: The positive time is stored as microseconds (UTC) and as the exact nanoseconds; 0, negative and null are null in both columns; only the negative one is counted; an absent column is all nulls.
    #[test]
    fn timestamp_pair_nulls_zero_and_negative() {
        let ns: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![
            Some(1_700_000_000_123_456_789),
            Some(0),
            Some(-5),
            None,
        ]));
        let mut out_of_range = 0;
        let (micros, nanos) = timestamp_pair(Some(&ns), 4, &mut out_of_range).expect("pair");
        assert_eq!(
            micros.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        let m = micros.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
        let n = nanos.as_primitive::<Int64Type>();
        assert_eq!(m.value(0), 1_700_000_000_123_456);
        assert_eq!(n.value(0), 1_700_000_000_123_456_789);
        for row in 1..4 {
            assert!(m.is_null(row) && n.is_null(row), "row {row}");
        }
        assert_eq!(out_of_range, 1);
        let (micros, nanos) = timestamp_pair(None, 3, &mut out_of_range).expect("absent");
        assert_eq!((micros.null_count(), nanos.null_count()), (3, 3));
        assert_eq!(out_of_range, 1);
    }

    /// Scenario: A Utf8 dictionary column with a null, an Int32 column with a null and a UInt32 flags column holding 0x8000_0001 and a null are read as required columns.
    /// Guarantees: Null reads as the v1 default ("" and 0), the flags keep their bits in the signed column, and no output has nulls.
    #[test]
    fn required_columns_take_defaults() {
        let mut budget = Budget::new(1 << 20);
        let dict: ArrayRef = Arc::new(
            vec![Some("a"), None, Some("a")]
                .into_iter()
                .collect::<DictionaryArray<UInt8Type>>(),
        );
        let s = utf8_or_empty(Some(&dict), 3, &mut budget).expect("utf8");
        assert_eq!(s.null_count(), 0);
        assert_eq!((s.value(0), s.value(1), s.value(2)), ("a", "", "a"));
        let absent = utf8_or_empty(None, 2, &mut budget).expect("absent");
        assert_eq!((absent.value(0), absent.value(1)), ("", ""));

        let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(9), None]));
        let i = i32_or_zero(Some(&ints), 2).expect("i32");
        assert_eq!(i.null_count(), 0);
        assert_eq!(i.as_primitive::<Int32Type>().values().to_vec(), vec![9, 0]);

        let flags: ArrayRef = Arc::new(UInt32Array::from(vec![Some(0x8000_0001), None]));
        let f = flags_i32(Some(&flags), 2).expect("flags");
        assert_eq!(f.null_count(), 0);
        assert_eq!(
            f.as_primitive::<Int32Type>().values().to_vec(),
            vec![i32::MIN + 1, 0]
        );
    }

    /// Scenario: A count column holds u64::MAX; a bucket list holds u64::MAX; a bucket list and a bounds list hold a null item; a null list is read with and without `null_as_empty`.
    /// Guarantees: Every value the signed storage type cannot hold, and every null item, is invalid content naming the field; a null list is `[]` or null as asked; items are declared non-null.
    #[test]
    fn counts_above_i64_are_invalid() {
        let big: ArrayRef = Arc::new(UInt64Array::from(vec![u64::MAX]));
        is_invalid(
            u64_as_i64(Some(&big), 1, "histogram count", true),
            "histogram count above i64::MAX",
        );
        is_invalid(
            list_u64_as_i64(
                Some(&u64_lists(&[Some(vec![Some(u64::MAX)])])),
                1,
                "bucket count",
                true,
            ),
            "bucket count above i64::MAX",
        );
        is_invalid(
            list_u64_as_i64(
                Some(&u64_lists(&[Some(vec![Some(1), None])])),
                1,
                "bucket count",
                true,
            ),
            "null bucket count",
        );
        let mut bounds = ListBuilder::new(Float64Builder::new());
        bounds.values().append_value(1.0);
        bounds.values().append_null();
        bounds.append(true);
        let bounds: ArrayRef = Arc::new(bounds.finish());
        is_invalid(
            list_f64(Some(&bounds), 1, "explicit bound", true),
            "null explicit bound",
        );

        let rows = u64_lists(&[Some(vec![Some(1), Some(2)]), None]);
        let as_empty = list_u64_as_i64(Some(&rows), 2, "bucket count", true).expect("lists");
        assert_eq!(as_empty.data_type(), &list_of(DataType::Int64));
        assert_eq!(as_empty.null_count(), 0);
        let l = as_empty.as_list::<i32>();
        assert_eq!((l.value_length(0), l.value_length(1)), (2, 0));
        let as_null = list_u64_as_i64(Some(&rows), 2, "bucket count", false).expect("lists");
        assert!(as_null.is_null(1));
        let absent = list_f64(None, 2, "explicit bound", true).expect("absent");
        assert_eq!(absent.data_type(), &list_of(DataType::Float64));
        assert_eq!(absent.null_count(), 0);
        let zero = u64_as_i64(None, 2, "count", true).expect("absent");
        assert_eq!(
            zero.as_primitive::<Int64Type>().values().to_vec(),
            vec![0, 0]
        );
        assert_eq!(
            u64_as_i64(None, 2, "count", false)
                .expect("absent")
                .null_count(),
            2
        );
    }

    /// Scenario: A dictionary of one 1000-byte string is referenced by 500 rows; a plain Utf8 column holds the same content.
    /// Guarantees: The cost of the cast is the expanded size (at least 500_000 bytes) although the dictionary's buffers are small, and 0 for the plain column, which is not copied.
    #[test]
    fn cast_cost_counts_dictionary_expansion() {
        let value = "x".repeat(1000);
        let values: ArrayRef = Arc::new(StringArray::from(vec![value.as_str()]));
        let dict: ArrayRef = Arc::new(
            DictionaryArray::<UInt16Type>::try_new(UInt16Array::from(vec![0_u16; 500]), values)
                .expect("dictionary"),
        );
        assert!(dict.get_buffer_memory_size() < 4096);
        assert!(cast_cost(&dict, &DataType::Utf8) >= 500_000);
        let plain: ArrayRef = Arc::new(StringArray::from(vec![value.as_str(); 500]));
        assert_eq!(cast_cost(&plain, &DataType::Utf8), 0);
        // A budget smaller than the expansion refuses the column before it is cast.
        let mut budget = Budget::new(100_000);
        let err = utf8_or_empty(Some(&dict), 500, &mut budget).expect_err("refused");
        assert!(
            err.to_string().contains("ingress.max_extracted_bytes"),
            "{err}"
        );
    }

    /// Scenario: A one-row string column holding 1 KiB is taken 100 times with a 50 KiB budget, then 10 times.
    /// Guarantees: The 100-fold take is refused by the budget before it copies; the 10-fold take returns 10 rows and charges 10 KiB.
    #[test]
    fn take_charged_charges_repeated_rows() {
        let value = "x".repeat(1024);
        let array = StringArray::from(vec![value.as_str()]);
        let mut budget = Budget::new(50 * 1024);
        let err = take_charged(&array, &UInt32Array::from(vec![0; 100]), &mut budget)
            .expect_err("refused");
        assert!(
            err.to_string().contains("ingress.max_extracted_bytes"),
            "{err}"
        );
        let mut budget = Budget::new(50 * 1024);
        let taken =
            take_charged(&array, &UInt32Array::from(vec![0; 10]), &mut budget).expect("fits");
        assert_eq!(taken.len(), 10);
        assert_eq!(budget.used(), 10 * 1024);
    }

    /// Scenario: A batch has a Utf8, a map and a list column; row 1 holds a 300-byte string, two map entries and three list items, row 0 holds empty cells.
    /// Guarantees: Row 1 measures its string, map and list content on top of the fixed cells; row 0 measures the fixed cells only.
    #[test]
    fn row_bytes_adds_variable_content() {
        let long = "y".repeat(300);
        let text: ArrayRef = Arc::new(StringArray::from(vec!["", long.as_str()]));
        let mut map = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        let _ = map.append(true);
        map.keys().append_value("k1");
        map.values().append_value("v1");
        map.keys().append_value("k2");
        map.values().append_value("v2");
        let _ = map.append(true);
        let map: ArrayRef = Arc::new(map.finish());
        let list = u64_lists(&[Some(vec![]), Some(vec![Some(1), Some(2), Some(3)])]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, false),
            Field::new("map", map.data_type().clone(), false),
            Field::new("list", list.data_type().clone(), true),
        ]));
        assert!(matches!(list.as_ref().data_type(), DataType::List(_)));
        let _: &ListArray = list.as_list::<i32>();
        let batch = RecordBatch::try_new(schema, vec![text, map, list]).expect("batch");
        let bytes = row_bytes(&batch);
        assert_eq!(bytes[0], 12, "three empty cells of 4 bytes each");
        // 300 + 4 for the string, 8 bytes of keys and values plus 8 per entry plus 4 for the map,
        // 3 * 8 + 4 for the list.
        assert_eq!(bytes[1], 304 + (8 + 16 + 4) + 28);
    }
}
