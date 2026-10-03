// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Row order of a file: by series_id, then time (nulls last), then arrival (docs/FORMAT.md
//! section 5). The keys of all rows of a block are sorted once; rows are then gathered from the
//! buffered batches one output slice at a time, so the extra memory is the keys plus one slice.

use std::collections::HashMap;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::compute::interleave_record_batch;
use arrow::datatypes::Int64Type;

use super::error::LakeError;

/// `sort_key` footer value of a values file.
pub const VALUES_SORT_KEY: &str = "series_id:asc:nulls_last,time_unix_nano:asc:nulls_last";
/// `sort_key` footer value of a series file.
pub const SERIES_SORT_KEY: &str = "series_id:asc:nulls_last";
/// Time column of the values sort key.
pub const TIME_COLUMN: &str = "time_unix_nano";

/// Sort key of one row and where the row lives. 32 bytes (`config::SORT_KEY_BYTES`).
#[derive(Clone, Copy, Debug)]
pub struct SortKey {
    id: u128,
    /// Stored times are at least 1; a null time is `u64::MAX`, which sorts last.
    time: u64,
    batch: u32,
    row: u32,
}

impl SortKey {
    /// The buffered batch and the row within it that this key refers to.
    #[must_use]
    pub const fn position(&self) -> (usize, usize) {
        (self.batch as usize, self.row as usize)
    }
}

/// series_id of every row of `batch` (column 0, FixedSizeBinary(16), big-endian).
pub fn series_ids(batch: &RecordBatch) -> Result<Vec<u128>, LakeError> {
    let bad = || LakeError::Conversion("series_id column is not FixedSizeBinary(16)".into());
    let ids = batch
        .column(0)
        .as_fixed_size_binary_opt()
        .filter(|a| a.value_length() == 16)
        .ok_or_else(bad)?;
    (0..ids.len())
        .map(|r| {
            Ok(u128::from_be_bytes(
                ids.value(r).try_into().map_err(|_| bad())?,
            ))
        })
        .collect()
}

/// Sorted keys of all rows of `batches`. With `by_time`, rows of one series are ordered by
/// `time_unix_nano`, nulls last.
pub fn sort_keys(batches: &[RecordBatch], by_time: bool) -> Result<Vec<SortKey>, LakeError> {
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let mut keys = Vec::with_capacity(rows);
    for (b, batch) in batches.iter().enumerate() {
        let ids = series_ids(batch)?;
        let times = if by_time {
            Some(
                batch
                    .column_by_name(TIME_COLUMN)
                    .and_then(|c| c.as_primitive_opt::<Int64Type>())
                    .ok_or_else(|| {
                        LakeError::Conversion("time_unix_nano column is not Int64".into())
                    })?,
            )
        } else {
            None
        };
        for (r, id) in ids.into_iter().enumerate() {
            let time = match times {
                Some(t) if t.is_valid(r) => u64::try_from(t.value(r)).unwrap_or(0),
                Some(_) => u64::MAX,
                None => 0,
            };
            keys.push(SortKey {
                id,
                time,
                batch: b as u32,
                row: r as u32,
            });
        }
    }
    keys.sort_unstable_by_key(|k| (k.id, k.time, k.batch, k.row));
    Ok(keys)
}

/// Smallest and largest non-null time among `keys`.
#[must_use]
pub fn time_range(keys: &[SortKey]) -> Option<(u64, u64)> {
    let mut times = keys.iter().map(|k| k.time).filter(|t| *t != u64::MAX);
    let first = times.next()?;
    Some(times.fold((first, first), |(lo, hi), t| (lo.min(t), hi.max(t))))
}

/// The rows of `keys`, in that order, as one batch. Only the batches these rows come from are
/// handed to the interleave kernel, so the cost of one slice depends on the slice, not on how many
/// batches the block buffers (a block of many small requests holds many batches).
pub fn gather(batches: &[RecordBatch], keys: &[SortKey]) -> Result<RecordBatch, LakeError> {
    let mut local: HashMap<u32, usize> = HashMap::new();
    let mut refs: Vec<&RecordBatch> = Vec::new();
    let mut order: Vec<(usize, usize)> = Vec::with_capacity(keys.len());
    for k in keys {
        let at = *local.entry(k.batch).or_insert_with(|| {
            refs.push(&batches[k.batch as usize]);
            refs.len() - 1
        });
        order.push((at, k.row as usize));
    }
    Ok(interleave_record_batch(&refs, &order)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::parquet_lake_exporter::config::SORT_KEY_BYTES;
    use arrow::array::{ArrayRef, FixedSizeBinaryArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn ids(rows: impl Iterator<Item = u128>) -> ArrayRef {
        Arc::new(FixedSizeBinaryArray::try_from_iter(rows.map(u128::to_be_bytes)).expect("ids"))
    }

    /// A values batch of `(series_id, time_unix_nano)` rows.
    fn values(rows: &[(u128, Option<i64>)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("series_id", DataType::FixedSizeBinary(16), false),
            Field::new(TIME_COLUMN, DataType::Int64, true),
        ]));
        let times: Int64Array = rows.iter().map(|r| r.1).collect();
        RecordBatch::try_new(schema, vec![ids(rows.iter().map(|r| r.0)), Arc::new(times)])
            .expect("batch")
    }

    fn rows_of(batch: &RecordBatch) -> Vec<(u128, Option<i64>)> {
        let ids = series_ids(batch).expect("ids");
        let times = batch.column(1).as_primitive::<Int64Type>();
        ids.into_iter()
            .enumerate()
            .map(|(r, id)| (id, times.is_valid(r).then(|| times.value(r))))
            .collect()
    }

    /// Scenario: Two batches hold rows of series 2, 1, 2, 1 with times 50, null, 10, 30 (the null time belongs to series 1).
    /// Guarantees: The gathered rows are ordered by series id, then time with the null last; the time range covers the non-null times only; the key is 32 bytes.
    #[test]
    fn rows_sort_by_series_then_time_nulls_last() {
        let batches = vec![
            values(&[(2, Some(50)), (1, None)]),
            values(&[(2, Some(10)), (1, Some(30))]),
        ];
        let keys = sort_keys(&batches, true).expect("keys");
        let sorted = gather(&batches, &keys).expect("gather");
        assert_eq!(
            rows_of(&sorted),
            vec![(1, Some(30)), (1, None), (2, Some(10)), (2, Some(50))]
        );
        assert_eq!(time_range(&keys), Some((10, 50)));
        assert_eq!(size_of::<SortKey>(), SORT_KEY_BYTES);
    }

    /// Scenario: A series batch (no time column) holds ids 3, 1, 2 and is sorted without time.
    /// Guarantees: Rows are ordered by series id alone, no time column is required, and every key carries time 0.
    #[test]
    fn series_sort_ignores_time() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "series_id",
            DataType::FixedSizeBinary(16),
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![ids([3_u128, 1, 2].into_iter())]).expect("batch");
        let batches = vec![batch];
        let keys = sort_keys(&batches, false).expect("keys");
        let sorted = gather(&batches, &keys).expect("gather");
        assert_eq!(series_ids(&sorted).expect("ids"), vec![1, 2, 3]);
        assert_eq!(time_range(&keys), Some((0, 0)));
    }

    /// Scenario: A values batch whose time column is missing is sorted by time.
    /// Guarantees: The sort is refused with a conversion error instead of silently ordering by arrival.
    #[test]
    fn values_sort_requires_the_time_column() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "series_id",
            DataType::FixedSizeBinary(16),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![ids([1_u128].into_iter())]).expect("batch");
        let err = sort_keys(&[batch], true).expect_err("refused");
        assert!(matches!(err, LakeError::Conversion(_)), "{err}");
    }
}
