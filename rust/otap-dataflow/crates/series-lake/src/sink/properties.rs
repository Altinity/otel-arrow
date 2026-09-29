// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Parquet writer properties and the key-value footer metadata of a file.

use super::Sink;

use crate::buffer::SortedTableBuffer;
use crate::config::{Nulls, ParquetConfig, SortOrder};
use crate::error::Result;
use crate::schema::{dataset_schema, schema_fingerprint};
use crate::sort::SortSpec;
use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowSchemaConverter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::{KeyValue, SortingColumn};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;

/// The codec every file is written with (FORMAT.md section 5).
#[must_use]
pub fn compression() -> Compression {
    Compression::ZSTD(ZstdLevel::default())
}

/// Leaf columns whose values are mostly distinct: written without a
/// dictionary and with column-chunk statistics only (FORMAT.md section 5).
pub const HIGH_ENTROPY_COLUMNS: [&str; 5] = [
    "body",
    "attrs.entries.values",
    "trace_id",
    "span_id",
    "identity_bytes",
];

/// The writer properties of every file, before its sorting columns and
/// footer metadata.
///
/// Compression, statistics and dictionary encoding are set explicitly
/// (FORMAT.md section 5): page statistics and dictionaries everywhere except
/// on [`HIGH_ENTROPY_COLUMNS`]. The row count limit is off, so
/// [`row_group_full`] alone decides where a row group ends.
#[must_use]
pub fn writer_properties(compression: Compression) -> WriterPropertiesBuilder {
    HIGH_ENTROPY_COLUMNS.iter().fold(
        WriterProperties::builder()
            .set_compression(compression)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_dictionary_enabled(true)
            .set_max_row_group_row_count(None),
        |builder, column| {
            let path = leaf_path(column);
            builder
                .set_column_dictionary_enabled(path.clone(), false)
                .set_column_statistics_enabled(path, EnabledStatistics::Chunk)
        },
    )
}

/// The Parquet path of a dotted leaf name such as `attrs.entries.values`;
/// `ColumnPath::from` keeps the dots inside one part, which matches only a
/// top-level column.
#[must_use]
pub(super) fn leaf_path(leaf: &str) -> ColumnPath {
    ColumnPath::new(leaf.split('.').map(str::to_owned).collect())
}

/// Whether the writer closes its row group now: its buffered memory reached
/// `parquet.writer_limit_bytes`, or its in-progress row group
/// `parquet.row_group_bytes`.
#[must_use]
pub fn row_group_full(cfg: &ParquetConfig, memory_size: usize, in_progress_size: usize) -> bool {
    memory_size >= cfg.writer_limit_bytes || in_progress_size >= cfg.row_group_bytes
}

/// Window length assumed when the configured interval does not fit in `i64`.
pub(super) const DEFAULT_WINDOW_SECS: i64 = 15;

/// Smallest and largest `time_unix_nano` across the sealed runs of a table.
///
/// A column of an unexpected type is skipped: the metadata is advisory, and
/// no data-derived input may panic here.
pub(super) fn time_range(batches: &[RecordBatch]) -> (Option<i64>, Option<i64>) {
    let mut lo = None;
    let mut hi = None;
    for c in batches {
        if let Some(a) = c
            .column_by_name("time_unix_nano")
            .and_then(|col| col.as_primitive_opt::<Int64Type>())
        {
            if let Some(mn) = arrow::compute::min(a) {
                lo = Some(lo.map_or(mn, |x: i64| x.min(mn)));
            }
            if let Some(mx) = arrow::compute::max(a) {
                hi = Some(hi.map_or(mx, |x: i64| x.max(mx)));
            }
        }
    }
    (lo, hi)
}

/// Parquet's native `SortingColumn` list for a file sorted by `spec`.
///
/// The list is written into every row group beside the `sort_key` key/value
/// (FORMAT.md section 5), so readers that understand the standard field
/// (DataFusion, DuckDB, ClickHouse) can use the order without knowing this
/// format. `column_idx` is the index of the column among the Parquet leaf
/// columns, not the Arrow field index: a map column has two leaves and a list
/// column one, so every column after a map shifts by one.
///
/// A `SortingColumn` can only describe a leaf column, and the list is
/// lexicographic, so it holds the longest prefix of `spec` whose columns are
/// top-level primitive, non-floating-point columns, and stops at the first key
/// that is not:
///
/// - a list column, which the row converter can sort as a whole but no leaf
///   order describes;
/// - a floating-point column, which the merge sorts on a normalized copy where
///   `-0.0` equals `+0.0` and every NaN equals every other NaN. Parquet's
///   recommended IEEE 754 total order puts `-0.0` before `+0.0` and gives NaN
///   payloads distinct positions, so declaring such a column sorted would be
///   false.
///
/// Nothing after the first excluded key is declared, since the keys after it
/// are only ordered within its ties. `None` when the prefix is empty,
/// including when sorting is disabled; `sort_key` stays the complete
/// description either way.
///
/// # Errors
/// Returns the Parquet error when the schema cannot be converted, which
/// cannot happen for a dataset schema the writer itself accepts.
pub fn native_sorting_columns(
    spec: &SortSpec,
    schema: &Schema,
) -> Result<Option<Vec<SortingColumn>>> {
    let descr = ArrowSchemaConverter::new().convert(schema)?;
    let mut out = Vec::with_capacity(spec.keys().len());
    for key in spec.keys() {
        let floating = schema
            .field_with_name(&key.column)
            .is_ok_and(|f| f.data_type().is_floating());
        if floating {
            break;
        }
        let leaf = descr.columns().iter().position(|c| {
            let parts = c.path().parts();
            parts.len() == 1 && parts[0] == key.column
        });
        let Some(leaf) = leaf.and_then(|i| i32::try_from(i).ok()) else {
            break;
        };
        out.push(SortingColumn {
            column_idx: leaf,
            descending: key.order == SortOrder::Desc,
            nulls_first: key.nulls == Nulls::First,
        });
    }
    Ok((!out.is_empty()).then_some(out))
}

impl Sink {
    /// File metadata of FORMAT.md section 5.
    ///
    /// `rows` and `time_range` are computed from the sealed runs before the merge,
    /// because the merged chunks are produced lazily and are not all available at
    /// the time the writer properties are built.
    #[must_use]
    pub fn file_metadata(
        &self,
        table: &SortedTableBuffer,
        rows: usize,
        time_range: (Option<i64>, Option<i64>),
        seq: u64,
        window_start_secs: i64,
    ) -> Vec<KeyValue> {
        let schema = dataset_schema(table.dataset(), &self.cfg);
        let window_secs =
            i64::try_from(self.cfg.window_interval.as_secs()).unwrap_or(DEFAULT_WINDOW_SECS);
        let mut kv = vec![
            KeyValue::new("format_version".into(), "1".to_string()),
            KeyValue::new("series_hash".into(), "xxh3_128/canonical_v1".to_string()),
            KeyValue::new(
                "schema_fingerprint".into(),
                format!("{:016x}", schema_fingerprint(&schema)),
            ),
            KeyValue::new("sort_key".into(), table.spec().metadata_string()),
            KeyValue::new("writer_id".into(), self.naming.writer_id.clone()),
            KeyValue::new("boot_id".into(), self.naming.boot_id.clone()),
            KeyValue::new("seq".into(), seq.to_string()),
            KeyValue::new("window_start".into(), window_start_secs.to_string()),
            KeyValue::new(
                "window_end".into(),
                window_start_secs.saturating_add(window_secs).to_string(),
            ),
            KeyValue::new("row_count".into(), rows.to_string()),
            KeyValue::new(
                "identity_config".into(),
                self.cfg.identity_config(table.dataset().signal()),
            ),
            KeyValue::new(
                "identity_config_hash".into(),
                self.cfg.identity_config_hash(table.dataset().signal()),
            ),
        ];
        if !table.dataset().is_series()
            && let (Some(lo), Some(hi)) = time_range
        {
            kv.push(KeyValue::new("min_time_unix_nano".into(), lo.to_string()));
            kv.push(KeyValue::new("max_time_unix_nano".into(), hi.to_string()));
        }
        kv
    }
}
