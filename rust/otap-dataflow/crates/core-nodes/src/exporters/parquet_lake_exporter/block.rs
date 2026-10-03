// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Per-signal block buffer, block ids and object names, and one-time Parquet encoding: rows
//! sorted, v1 writer options and footer metadata (docs/FORMAT.md sections 4 and 5).

use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch, TimestampMicrosecondArray, UInt32Array};
use arrow::compute::{filter_record_batch, take_record_batch};
use arrow::datatypes::{DataType, SchemaRef};
use object_store::path::Path;
use object_store::{PutPayload, PutPayloadMut};
use parquet::arrow::{ArrowSchemaConverter, ArrowWriter};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::{KeyValue, SortingColumn};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

use super::cache::SeriesCache;
use super::canonical::{FORMAT_VERSION, SERIES_HASH, Signal};
use super::columns::row_bytes;
use super::config::{OUTPUT_BLOCK_BYTES, ROW_GROUP_BYTES, SLICE_BYTES};
use super::error::LakeError;
use super::extract::Chunk;
use super::schema::Schemas;
use super::sort::{
    SERIES_SORT_KEY, SortKey, TIME_COLUMN, VALUES_SORT_KEY, gather, series_ids, sort_keys,
    time_range,
};
use super::window::{PartitionId, utc_stamp};

/// Leaf columns whose values are mostly distinct: written without a dictionary and with
/// column-chunk statistics only.
const HIGH_ENTROPY_COLUMNS: [&str; 5] = [
    "body",
    "attrs.entries.values",
    "trace_id",
    "span_id",
    "identity_bytes",
];

/// Identity of a block; object names derive from it before the first upload attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockId {
    /// Signal.
    pub signal: Signal,
    /// Start of the block's window, Unix seconds; names the `date=`/`hour=` partition.
    pub window_start_secs: i64,
    /// Configured writer id (`[A-Za-z0-9_.-]+`).
    pub writer_id: String,
    /// Random id of this exporter start, 32 lowercase hexadecimal digits.
    pub boot_id: String,
    /// Sequence number of the generation within the writer.
    pub seq: u64,
}

impl BlockId {
    /// The `date/hour` partition of the block.
    #[must_use]
    pub fn partition(&self) -> PartitionId {
        PartitionId::from_unix_secs(self.window_start_secs)
    }

    fn path(&self, dataset: &str) -> Path {
        let p = self.partition();
        Path::from_iter([
            "v=1".to_owned(),
            format!("signal={}", self.signal.as_str()),
            format!("dataset={dataset}"),
            format!("date={}", p.date_string()),
            format!("hour={}", p.hour_string()),
            format!(
                "part-{}-{}-{}-{:08}.parquet",
                utc_stamp(self.window_start_secs),
                self.writer_id,
                self.boot_id,
                self.seq
            ),
        ])
    }

    /// Object name of the series file.
    #[must_use]
    pub fn series_path(&self) -> Path {
        self.path("series")
    }

    /// Object name of the values file (written last; its presence means the block landed).
    #[must_use]
    pub fn values_path(&self) -> Path {
        self.path("values")
    }
}

/// `<signal>:<window_start_secs>:<writer_id>:<boot_id>:<seq>`, parsable by `FromStr` (probe from a
/// fresh process). `writer_id` never contains `:`.
impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}:{}:{}",
            self.signal.as_str(),
            self.window_start_secs,
            self.writer_id,
            self.boot_id,
            self.seq
        )
    }
}

impl FromStr for BlockId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        let [sig, start, writer_id, boot_id, seq] = parts[..] else {
            return Err(format!("invalid block id `{s}`"));
        };
        let signal = match sig {
            "logs" => Signal::Logs,
            "metrics" => Signal::Metrics,
            other => return Err(format!("invalid signal `{other}`")),
        };
        Ok(Self {
            signal,
            window_start_secs: start
                .parse()
                .map_err(|_| format!("invalid window start in `{s}`"))?,
            writer_id: writer_id.to_owned(),
            boot_id: boot_id.to_owned(),
            seq: seq.parse().map_err(|_| format!("invalid seq in `{s}`"))?,
        })
    }
}

/// Contents taken out of a block; the block is already reset.
pub struct TakenBlock {
    /// Series rows.
    pub series: Vec<RecordBatch>,
    /// Values rows.
    pub values: Vec<RecordBatch>,
    /// Requests with rows in this block.
    pub batches: BTreeSet<u64>,
}

/// A block on its way to the flush slot.
pub struct SealedBlock {
    /// Block id.
    pub id: BlockId,
    /// Series rows.
    pub series: Vec<RecordBatch>,
    /// Values rows.
    pub values: Vec<RecordBatch>,
    /// Requests with rows in this block.
    pub batches: BTreeSet<u64>,
}

/// File-level facts shared by the files of one generation.
#[derive(Clone, Copy, Debug)]
pub struct FileMeta {
    /// `emitted_at` of the series rows: the time the generation was sealed, Unix microseconds.
    pub emitted_at_micros: i64,
    /// `window_end` footer value: window start plus the interval.
    pub window_end_secs: i64,
}

/// Encoder limits; `DEFAULT` in production, scaled down (same ratios) in tests.
#[derive(Clone, Copy, Debug)]
pub struct EncodeLimits {
    /// Flush the row group once `ArrowWriter::memory_size` reaches this.
    pub row_group_bytes: usize,
    /// Input bytes per encoder write (one yield per slice).
    pub slice_bytes: usize,
}

impl EncodeLimits {
    /// Production limits. ENCODER_BYTES = 2 x ROW_GROUP_BYTES assumes slice_bytes <=
    /// row_group_bytes / 8; a row larger than `slice_bytes` forms a slice alone, which
    /// `LakeConfig::worst_case_flush_bytes` accounts for.
    pub const DEFAULT: Self = Self {
        row_group_bytes: ROW_GROUP_BYTES,
        slice_bytes: SLICE_BYTES,
    };
}

/// A block encoded once; retries resend these exact bytes.
pub struct EncodedBlock {
    /// Block id.
    pub id: BlockId,
    /// Series file; `None` when the block carries no new series row.
    pub series: Option<PutPayload>,
    /// Values file.
    pub values: PutPayload,
    /// Values rows.
    pub rows: usize,
    /// Series rows.
    pub series_rows: usize,
    /// Largest `ArrowWriter::memory_size` observed after a write.
    pub encoder_peak: usize,
    /// Largest gathered slice (Arrow memory) handed to the encoder.
    pub slice_peak: usize,
}

/// Open block of one signal.
#[derive(Default)]
pub struct Block {
    series: Vec<RecordBatch>,
    values: Vec<RecordBatch>,
    seen: HashSet<u128>,
    batches: BTreeSet<u64>,
    bytes: usize,
}

impl Block {
    /// Buffered bytes (sum of owned chunk sizes).
    #[must_use]
    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    /// True when nothing is buffered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Add a chunk of request `seq`. Of the chunk's series, only the rows this block does not hold
    /// yet and that `cache` does not show as landed in `partition` are copied from the request's
    /// series table. On error the block is unchanged. Returns true when `seq` is new to this block
    /// (the caller adds a pending reference).
    pub fn push(
        &mut self,
        chunk: Chunk,
        seq: u64,
        cache: &mut SeriesCache,
        partition: PartitionId,
    ) -> Result<bool, LakeError> {
        let table = &chunk.series;
        let needed: Vec<u32> = chunk
            .series_rows
            .iter()
            .copied()
            .filter(|&row| {
                let id = table.ids[row as usize];
                !self.seen.contains(&id) && !cache.is_committed(id, partition)
            })
            .collect();
        let series = take_record_batch(&table.rows, &UInt32Array::from(needed.clone()))?;
        for row in needed {
            let _ = self.seen.insert(table.ids[row as usize]);
        }
        self.bytes += chunk.values.get_array_memory_size() + series.get_array_memory_size();
        if series.num_rows() > 0 {
            self.series.push(series);
        }
        self.values.push(chunk.values);
        Ok(self.batches.insert(seq))
    }

    /// Take the buffered rows and reset every field (including the seen set), so nothing stale
    /// survives a failed encode or upload.
    pub fn take(&mut self) -> TakenBlock {
        self.seen.clear();
        self.bytes = 0;
        TakenBlock {
            series: std::mem::take(&mut self.series),
            values: std::mem::take(&mut self.values),
            batches: std::mem::take(&mut self.batches),
        }
    }
}

/// Drop the series rows whose series became committed in `partition` after they were buffered
/// (the block that carried them landed meanwhile). Returns the remaining batches and their ids,
/// which the caller marks committed once this block has landed.
pub fn prune_committed(
    series: Vec<RecordBatch>,
    cache: &mut SeriesCache,
    partition: PartitionId,
) -> Result<(Vec<RecordBatch>, Vec<u128>), LakeError> {
    let mut kept = Vec::with_capacity(series.len());
    let mut ids = Vec::new();
    for batch in series {
        let keep: Vec<bool> = series_ids(&batch)?
            .into_iter()
            .map(|id| {
                let keep = !cache.is_committed(id, partition);
                if keep {
                    ids.push(id);
                }
                keep
            })
            .collect();
        if keep.iter().all(|k| *k) {
            kept.push(batch);
            continue;
        }
        let batch = filter_record_batch(&batch, &BooleanArray::from(keep))?;
        if batch.num_rows() > 0 {
            kept.push(batch);
        }
    }
    Ok((kept, ids))
}

/// `Write` into fixed-size allocations (no realloc copies; at most one unfilled
/// OUTPUT_BLOCK_BYTES per file).
struct PayloadSink(PutPayloadMut);

impl Write for PayloadSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Encode `series` and `values` (consumed; dropped once encoded) into Parquet files with sorted
/// rows. Series rows get `emitted_at` stamped; no series file is produced when there are none.
/// Yields to the runtime after every slice.
pub async fn encode_block(
    id: BlockId,
    series: Vec<RecordBatch>,
    values: Vec<RecordBatch>,
    schemas: &Schemas,
    meta: FileMeta,
    limits: EncodeLimits,
) -> Result<EncodedBlock, LakeError> {
    let series_rows: usize = series.iter().map(RecordBatch::num_rows).sum();
    let rows = values.iter().map(RecordBatch::num_rows).sum();
    let mut peaks = Peaks::default();
    let signal = id.signal;
    let series_file = if series_rows == 0 {
        None
    } else {
        let series = stamp_emitted_at(series, meta.emitted_at_micros)?;
        Some(
            encode_file(
                schemas.series(signal),
                &series,
                "series",
                &id,
                meta,
                limits,
                &mut peaks,
            )
            .await?,
        )
    };
    let values_file = encode_file(
        schemas.values(signal),
        &values,
        "values",
        &id,
        meta,
        limits,
        &mut peaks,
    )
    .await?;
    Ok(EncodedBlock {
        id,
        series: series_file,
        values: values_file,
        rows,
        series_rows,
        encoder_peak: peaks.encoder,
        slice_peak: peaks.slice,
    })
}

/// Largest encoder memory and largest gathered slice seen while encoding a block.
#[derive(Default)]
struct Peaks {
    encoder: usize,
    slice: usize,
}

/// Replace the `emitted_at` column of every series batch with the seal time.
fn stamp_emitted_at(batches: Vec<RecordBatch>, micros: i64) -> Result<Vec<RecordBatch>, LakeError> {
    batches
        .into_iter()
        .map(|b| {
            let at = b.schema().index_of("emitted_at")?;
            let mut cols = b.columns().to_vec();
            cols[at] = Arc::new(
                TimestampMicrosecondArray::from(vec![micros; b.num_rows()]).with_timezone("UTC"),
            );
            Ok(RecordBatch::try_new(b.schema(), cols)?)
        })
        .collect()
}

/// Key/value metadata of a file (FORMAT.md section 5, without `schema_fingerprint`). Every value
/// is a string; the times are written only when a row has a valid time.
fn footer(
    id: &BlockId,
    meta: FileMeta,
    sort_key: &str,
    rows: usize,
    times: Option<(u64, u64)>,
) -> Vec<KeyValue> {
    let kv = |k: &str, v: String| KeyValue::new(k.to_owned(), v);
    let mut out = vec![
        kv("format_version", FORMAT_VERSION.to_owned()),
        kv("series_hash", SERIES_HASH.to_owned()),
        kv("sort_key", sort_key.to_owned()),
        kv("writer_id", id.writer_id.clone()),
        kv("boot_id", id.boot_id.clone()),
        kv("seq", id.seq.to_string()),
        kv("window_start", id.window_start_secs.to_string()),
        kv("window_end", meta.window_end_secs.to_string()),
        kv("row_count", rows.to_string()),
        kv("identity_config", id.signal.identity_config().to_owned()),
        kv("identity_config_hash", id.signal.identity_config_hash()),
    ];
    if let Some((lo, hi)) = times {
        out.push(kv("min_time_unix_nano", lo.to_string()));
        out.push(kv("max_time_unix_nano", hi.to_string()));
    }
    out
}

/// Writer options of a file: ZSTD, page statistics, dictionaries except on the high-entropy leaf
/// columns (chunk statistics there) and on top-level fixed-width columns, and the native sort
/// declaration. Fixed-width values (times, ids, point values) are mostly unique: a dictionary
/// would only add index overhead and break the B + B/8 output bound. `series_id` repeats in a
/// values file, but the Parquet 1.0 writer never dictionary-encodes FIXED_LEN_BYTE_ARRAY, so it
/// is written plain there too; the sort keeps equal ids adjacent for the compressor.
fn writer_properties(
    schema: &SchemaRef,
    values_file: bool,
    metadata: Vec<KeyValue>,
) -> Result<WriterProperties, LakeError> {
    let mut props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_key_value_metadata(Some(metadata));
    for f in schema.fields() {
        let fixed_width =
            f.data_type().is_primitive() || matches!(f.data_type(), DataType::FixedSizeBinary(_));
        if fixed_width {
            props = props.set_column_dictionary_enabled(ColumnPath::from(f.name().as_str()), false);
        }
    }
    for leaf in HIGH_ENTROPY_COLUMNS {
        let path = ColumnPath::new(leaf.split('.').map(str::to_owned).collect());
        props = props
            .set_column_dictionary_enabled(path.clone(), false)
            .set_column_statistics_enabled(path, EnabledStatistics::Chunk);
    }
    // `column_idx` counts Parquet leaf columns: a map column has two leaves.
    let leaves = ArrowSchemaConverter::new().convert(schema)?;
    let leaf = |name: &str| {
        leaves
            .columns()
            .iter()
            .position(|c| c.path().string() == name)
            .and_then(|i| i32::try_from(i).ok())
    };
    let mut sorting = Vec::new();
    for name in ["series_id", TIME_COLUMN] {
        if name == TIME_COLUMN && !values_file {
            break;
        }
        if let Some(column_idx) = leaf(name) {
            sorting.push(SortingColumn {
                column_idx,
                descending: false,
                nulls_first: false,
            });
        }
    }
    Ok(props.set_sorting_columns(Some(sorting)).build())
}

/// One Parquet file with its rows sorted, written in slices of about `limits.slice_bytes` with a
/// yield after each.
async fn encode_file(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    dataset: &str,
    id: &BlockId,
    meta: FileMeta,
    limits: EncodeLimits,
    peaks: &mut Peaks,
) -> Result<PutPayload, LakeError> {
    let values_file = dataset == "values";
    let keys: Vec<SortKey> = sort_keys(batches, values_file)?;
    let (sort_key, times) = if values_file {
        (VALUES_SORT_KEY, time_range(&keys))
    } else {
        (SERIES_SORT_KEY, None)
    };
    let props = writer_properties(
        schema,
        values_file,
        footer(id, meta, sort_key, keys.len(), times),
    )?;
    let sink = PayloadSink(PutPayloadMut::new().with_block_size(OUTPUT_BLOCK_BYTES));
    let mut writer = ArrowWriter::try_new(sink, schema.clone(), Some(props))?;
    // Slices are cut by the bytes of the rows they hold, not by a row count: the sort puts the
    // large rows of one series next to each other, so a count derived from the block's average
    // row size would gather far more than `slice_bytes` from such a stretch. One `u32` per row
    // (`config::ROW_WEIGHT_BYTES`); a row never exceeds `ingress.max_row_bytes` <= 1 GiB.
    let weights: Vec<Vec<u32>> = batches
        .iter()
        .map(|batch| {
            row_bytes(batch)
                .into_iter()
                .map(|w| u32::try_from(w).unwrap_or(u32::MAX))
                .collect()
        })
        .collect();
    let mut start = 0;
    while start < keys.len() {
        let mut end = start;
        let mut slice_bytes = 0;
        while end < keys.len() {
            let (batch, row) = keys[end].position();
            let weight = weights[batch][row] as usize;
            if end > start && slice_bytes + weight > limits.slice_bytes {
                break;
            }
            slice_bytes += weight;
            end += 1;
        }
        let slice = gather(batches, &keys[start..end])?;
        peaks.slice = peaks.slice.max(slice.get_array_memory_size());
        writer.write(&slice)?;
        drop(slice);
        peaks.encoder = peaks.encoder.max(writer.memory_size());
        if writer.memory_size() >= limits.row_group_bytes {
            writer.flush()?;
        }
        tokio::task::yield_now().await;
        start = end;
    }
    Ok(writer.into_inner()?.0.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::parquet_lake_exporter::config::{
        ENCODER_BYTES, LakeConfig, ROW_WEIGHT_BYTES, SORT_KEY_BYTES,
    };
    use crate::exporters::parquet_lake_exporter::extract::extract_logs;
    use crate::exporters::parquet_lake_exporter::limits::Limits;
    use crate::exporters::parquet_lake_exporter::test_fixtures::{
        kv, logs_request, sized_logs_request, to_otap,
    };
    use arrow::array::AsArray;
    use arrow::datatypes::{Int64Type, TimestampMicrosecondType};
    use bytes::Bytes;
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, any_value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, ResourceLogs, ScopeLogs,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
    use parquet::basic::Encoding;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use rand::{RngExt, SeedableRng, rngs::StdRng};

    /// 2026-09-21T03:15:00Z.
    const WINDOW_START: i64 = 1_789_960_500;
    const META: FileMeta = FileMeta {
        emitted_at_micros: 1_789_960_515_000_000,
        window_end_secs: 1_789_960_515,
    };
    /// Times of the `logs_request` fixture start here.
    const BASE_NS: u64 = 1_700_000_000_000_000_000;

    fn limits(max_chunk_bytes: usize) -> Limits {
        Limits {
            max_extracted_bytes: 1 << 30,
            max_row_bytes: 1 << 30,
            max_nesting_depth: 32,
            max_chunk_bytes,
        }
    }

    fn chunks(rows: usize, resources: usize, seed: usize) -> Vec<Chunk> {
        extract_logs(
            &to_otap(&logs_request(rows, resources, seed)),
            &Schemas::new(),
            &limits(1 << 30),
            "host.id",
        )
        .expect("extract")
        .chunks
    }

    fn file(payload: &PutPayload) -> SerializedFileReader<Bytes> {
        SerializedFileReader::new(Bytes::from(payload.clone())).expect("parquet file")
    }

    fn read(payload: &PutPayload) -> Vec<RecordBatch> {
        ParquetRecordBatchReader::try_new(Bytes::from(payload.clone()), 1024)
            .expect("reader")
            .map(|b| b.expect("batch"))
            .collect()
    }

    fn id(signal: Signal) -> BlockId {
        BlockId {
            signal,
            window_start_secs: WINDOW_START,
            writer_id: "w".into(),
            boot_id: "0".repeat(32),
            seq: 7,
        }
    }

    fn partition() -> PartitionId {
        id(Signal::Logs).partition()
    }

    fn rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    async fn encode(block: &mut Block, limits: EncodeLimits) -> EncodedBlock {
        let t = block.take();
        encode_block(
            id(Signal::Logs),
            t.series,
            t.values,
            &Schemas::new(),
            META,
            limits,
        )
        .await
        .expect("encode")
    }

    /// Scenario: A BlockId is formatted, parsed back, and its object names are derived.
    /// Guarantees: Both files share one name under the v1 Hive layout (version, signal, dataset, date and hour of the window start) and differ only in `dataset=`; the id round-trips through its string form, also with a `-` in the writer id; a string with too few or too many fields, or an unknown signal, fails to parse.
    #[test]
    fn block_id_round_trips_and_paths() {
        let b = id(Signal::Logs);
        let name = format!(
            "part-20260921T031500Z-w-{}-00000007.parquet",
            "0".repeat(32)
        );
        assert_eq!(
            b.values_path().as_ref(),
            format!("v=1/signal=logs/dataset=values/date=2026-09-21/hour=03/{name}")
        );
        assert_eq!(
            b.series_path().as_ref(),
            format!("v=1/signal=logs/dataset=series/date=2026-09-21/hour=03/{name}")
        );
        assert_eq!(b.partition().date_string(), "2026-09-21");
        assert_eq!(b.to_string().parse::<BlockId>(), Ok(b.clone()));
        let dashed = BlockId {
            signal: Signal::Metrics,
            writer_id: "edge-1_a.b".into(),
            ..b
        };
        assert_eq!(dashed.to_string().parse::<BlockId>(), Ok(dashed.clone()));
        assert!(
            dashed
                .values_path()
                .as_ref()
                .starts_with("v=1/signal=metrics/dataset=values/")
        );
        assert!("logs:1:w:b".parse::<BlockId>().is_err());
        assert!("logs:1:w:b:7:x".parse::<BlockId>().is_err());
        assert!("traces:1:w:b:7".parse::<BlockId>().is_err());
        assert!("logs:x:w:b:7".parse::<BlockId>().is_err());
    }

    /// Scenario: Two chunks with the same two resources are pushed into one block from two requests.
    /// Guarantees: The block keeps one series row per series_id, all values rows, and reports each request as new once.
    #[test]
    fn push_dedupes_series_within_block() {
        let mut block = Block::default();
        let mut cache = SeriesCache::new(1000);
        for (seq, c) in chunks(10, 2, 0)
            .into_iter()
            .chain(chunks(10, 2, 1))
            .enumerate()
        {
            assert!(
                block
                    .push(c, seq as u64, &mut cache, partition())
                    .expect("push")
            );
        }
        let again = chunks(10, 2, 2).remove(0);
        assert!(!block.push(again, 1, &mut cache, partition()).expect("push"));
        let t = block.take();
        assert_eq!((rows(&t.series), rows(&t.values)), (2, 30));
        assert_eq!(t.batches.into_iter().collect::<Vec<_>>(), vec![0, 1]);
    }

    /// Scenario: The cache shows one of a chunk's two series as landed in the block's partition; the same chunk is then pushed into a block of the next hour.
    /// Guarantees: The landed series row is not buffered again for its partition, but is buffered for the other hour (each hour partition is self-contained); values rows are always buffered.
    #[test]
    fn push_skips_series_committed_in_the_partition() {
        let mut cache = SeriesCache::new(1000);
        let committed = chunks(10, 2, 0).remove(0).series.ids[0];
        cache.mark_committed(committed, partition());

        let mut block = Block::default();
        let _ = block
            .push(chunks(10, 2, 0).remove(0), 1, &mut cache, partition())
            .expect("push");
        let t = block.take();
        assert_eq!(rows(&t.values), 10);
        assert_eq!(rows(&t.series), 1);
        assert_ne!(series_ids(&t.series[0]).expect("ids"), vec![committed]);

        let next_hour = PartitionId::from_unix_secs(WINDOW_START + 3600);
        assert_ne!(next_hour, partition());
        let _ = block
            .push(chunks(10, 2, 0).remove(0), 1, &mut cache, next_hour)
            .expect("push");
        let t = block.take();
        assert_eq!((rows(&t.series), rows(&t.values)), (2, 10));
    }

    /// Scenario: Two series rows are buffered; one series is marked as landed afterwards (another block carried it), then both are.
    /// Guarantees: Pruning returns only the rows still needed, with their ids; when every series has landed it returns no batch and no id.
    #[test]
    fn prune_drops_series_committed_since_push() {
        let mut cache = SeriesCache::new(1000);
        let mut block = Block::default();
        let _ = block
            .push(chunks(10, 2, 0).remove(0), 1, &mut cache, partition())
            .expect("push");
        let series = block.take().series;
        let ids = series_ids(&series[0]).expect("ids");
        assert_eq!(ids.len(), 2);

        let (kept, kept_ids) =
            prune_committed(series.clone(), &mut cache, partition()).expect("prune");
        assert_eq!((rows(&kept), kept_ids.clone()), (2, ids.clone()));

        cache.mark_committed(ids[0], partition());
        let (kept, kept_ids) =
            prune_committed(series.clone(), &mut cache, partition()).expect("prune");
        assert_eq!(rows(&kept), 1);
        assert_eq!(kept_ids, vec![ids[1]]);
        assert_eq!(series_ids(&kept[0]).expect("ids"), vec![ids[1]]);

        cache.mark_committed(ids[1], partition());
        let (kept, kept_ids) = prune_committed(series, &mut cache, partition()).expect("prune");
        assert!(kept.is_empty() && kept_ids.is_empty());
    }

    /// Scenario: A block with data is taken, and a chunk with an already-seen series_id is pushed afterwards.
    /// Guarantees: `take` resets bytes, requests and the seen set, so the new block keeps that series row and nothing stale survives.
    #[test]
    fn take_resets_all_state() {
        let mut block = Block::default();
        let mut cache = SeriesCache::new(1000);
        let _ = block
            .push(chunks(10, 1, 0).remove(0), 1, &mut cache, partition())
            .expect("push");
        assert!(block.bytes() > 0);
        assert!(!block.is_empty());
        let t = block.take();
        assert_eq!(t.batches.len(), 1);
        assert!(block.is_empty());
        assert_eq!(block.bytes(), 0);
        assert!(
            block
                .push(chunks(10, 1, 0).remove(0), 1, &mut cache, partition())
                .expect("push")
        );
        let t = block.take();
        assert_eq!(rows(&t.series), 1);
    }

    /// Scenario: A logs block of 12 rows over 3 resources is encoded.
    /// Guarantees: Both files carry the v1 footer (format version, series hash, writer, boot id, sequence, window, row count, identity config and its hash) and their own sort key; only the values file carries the time range, equal to the smallest and largest row time; there is no schema fingerprint; every series row is stamped with the seal time; the high-entropy leaves and the fixed-width columns are written without a dictionary while the map keys keep theirs.
    #[tokio::test]
    async fn encoded_files_carry_v1_footer() {
        let mut block = Block::default();
        let mut cache = SeriesCache::new(1000);
        for c in chunks(12, 3, 0) {
            let _ = block.push(c, 1, &mut cache, partition()).expect("push");
        }
        let e = encode(&mut block, EncodeLimits::DEFAULT).await;
        assert_eq!((e.rows, e.series_rows), (12, 3));
        let series = e.series.as_ref().expect("series file");
        for (payload, sort_key, rows, values_file) in [
            (series, SERIES_SORT_KEY, 3_i64, false),
            (&e.values, VALUES_SORT_KEY, 12, true),
        ] {
            let f = file(payload);
            let md = f.metadata().file_metadata();
            assert_eq!(md.num_rows(), rows);
            let kv: std::collections::HashMap<String, String> = md
                .key_value_metadata()
                .expect("kv")
                .iter()
                .filter_map(|k| Some((k.key.clone(), k.value.clone()?)))
                .collect();
            let get = |k: &str| kv.get(k).map(String::as_str);
            assert_eq!(get("format_version"), Some("1"));
            assert_eq!(get("series_hash"), Some("xxh3_128/canonical_v1"));
            assert_eq!(get("sort_key"), Some(sort_key));
            assert_eq!(get("writer_id"), Some("w"));
            assert_eq!(get("boot_id"), Some("0".repeat(32).as_str()));
            assert_eq!(get("seq"), Some("7"));
            assert_eq!(get("window_start"), Some("1789960500"));
            assert_eq!(get("window_end"), Some("1789960515"));
            assert_eq!(get("row_count"), Some(rows.to_string().as_str()));
            assert_eq!(get("identity_config"), Some("{\"series_attributes\":[]}"));
            assert_eq!(
                get("identity_config_hash"),
                Some(Signal::Logs.identity_config_hash().as_str())
            );
            assert_eq!(get("schema_fingerprint"), None);
            if values_file {
                assert_eq!(
                    get("min_time_unix_nano"),
                    Some(BASE_NS.to_string().as_str())
                );
                assert_eq!(
                    get("max_time_unix_nano"),
                    Some((BASE_NS + 11).to_string().as_str())
                );
            } else {
                assert_eq!(get("min_time_unix_nano"), None);
                assert_eq!(get("max_time_unix_nano"), None);
            }
        }
        for b in read(series) {
            let at = b.column_by_name("emitted_at").expect("emitted_at");
            let at = at.as_primitive::<TimestampMicrosecondType>();
            assert!(at.iter().all(|v| v == Some(META.emitted_at_micros)));
        }
        let f = file(&e.values);
        let schema = f.metadata().file_metadata().schema_descr_ptr();
        let dictionary = |leaf: &str| {
            let i = (0..schema.num_columns())
                .find(|&i| schema.column(i).path().string() == leaf)
                .unwrap_or_else(|| panic!("leaf {leaf}"));
            f.metadata()
                .row_group(0)
                .column(i)
                .encodings()
                .any(|enc| enc == Encoding::RLE_DICTIONARY)
        };
        assert!(dictionary("attrs.entries.keys"));
        assert!(dictionary("severity_text"));
        for leaf in [
            "attrs.entries.values",
            "body",
            "series_id",
            "time_unix_nano",
        ] {
            assert!(!dictionary(leaf), "{leaf} has a dictionary");
        }
    }

    /// Scenario: Two requests over the same 3 resources are pushed later times first and encoded.
    /// Guarantees: The values file reads back ordered by (series_id, time_unix_nano) and the series file by series_id, and every row group declares that order in its native sorting columns (leaf 0, and leaf 3 for the time of a logs values file), ascending with nulls last.
    #[tokio::test]
    async fn files_are_sorted_and_declare_it() {
        let mut block = Block::default();
        let mut cache = SeriesCache::new(1000);
        for seed in [1, 0] {
            for c in chunks(30, 3, seed) {
                let _ = block
                    .push(c, seed as u64, &mut cache, partition())
                    .expect("push");
            }
        }
        let limits = EncodeLimits {
            row_group_bytes: 512 * 1024,
            slice_bytes: 4 * 1024,
        };
        let e = encode(&mut block, limits).await;
        let values: Vec<(u128, i64)> = read(&e.values)
            .iter()
            .flat_map(|b| {
                let ids = series_ids(b).expect("ids");
                let times = b.column_by_name(TIME_COLUMN).expect("time");
                let times = times.as_primitive::<Int64Type>().values().to_vec();
                ids.into_iter().zip(times).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(values.len(), 60);
        assert!(values.is_sorted(), "values rows are not sorted");
        let distinct: HashSet<u128> = values.iter().map(|v| v.0).collect();
        assert_eq!(distinct.len(), 3);

        let series = e.series.as_ref().expect("series file");
        let ids: Vec<u128> = read(series)
            .iter()
            .flat_map(|b| series_ids(b).expect("ids"))
            .collect();
        assert_eq!(ids.len(), 3);
        assert!(ids.is_sorted());

        let asc = |column_idx: i32| SortingColumn {
            column_idx,
            descending: false,
            nulls_first: false,
        };
        for (payload, want) in [(series, vec![asc(0)]), (&e.values, vec![asc(0), asc(3)])] {
            let f = file(payload);
            assert!(f.metadata().num_row_groups() > 0);
            for group in f.metadata().row_groups() {
                assert_eq!(group.sorting_columns(), Some(&want));
            }
        }
        let schema = file(&e.values)
            .metadata()
            .file_metadata()
            .schema_descr_ptr();
        assert_eq!(schema.column(3).path().string(), TIME_COLUMN);
    }

    /// Scenario: Every series of a chunk landed in the block's partition before the chunk is pushed.
    /// Guarantees: The block buffers no series row, so encoding produces no series file, while the values file holds every row.
    #[tokio::test]
    async fn block_without_new_series_has_no_series_file() {
        let mut cache = SeriesCache::new(1000);
        let chunk = chunks(10, 2, 0).remove(0);
        for id in chunk.series.ids.clone() {
            cache.mark_committed(id, partition());
        }
        let mut block = Block::default();
        let _ = block.push(chunk, 1, &mut cache, partition()).expect("push");
        let e = encode(&mut block, EncodeLimits::DEFAULT).await;
        assert!(e.series.is_none());
        assert_eq!((e.rows, e.series_rows), (10, 0));
        assert_eq!(file(&e.values).metadata().file_metadata().num_rows(), 10);
    }

    /// Worst-case logs: unique resources, times, trace/span ids and random bodies/attribute values.
    fn worst_case_request(rows: usize) -> ExportLogsServiceRequest {
        fn text(rng: &mut StdRng, n: usize) -> String {
            (0..n)
                .map(|_| char::from(rng.random_range(0x21u8..0x7f)))
                .collect()
        }
        let mut rng = StdRng::seed_from_u64(0x4c41_4b45);
        let resource_logs = (0..rows)
            .map(|i| {
                let body = text(&mut rng, 64);
                let attributes = (0..3)
                    .map(|a| kv(&format!("attr.{a}"), text(&mut rng, 16)))
                    .collect();
                ResourceLogs {
                    resource: Some(Resource {
                        attributes: vec![
                            kv("host.name", text(&mut rng, 16)),
                            kv("pod", text(&mut rng, 16)),
                        ],
                        ..Default::default()
                    }),
                    scope_logs: vec![ScopeLogs {
                        log_records: vec![LogRecord {
                            time_unix_nano: 1_700_000_000_000_000_000 + i as u64 * 7_919,
                            observed_time_unix_nano: 1_700_000_000_000_000_000 + i as u64,
                            severity_number: (i % 24) as i32,
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(body)),
                            }),
                            attributes,
                            trace_id: (0..16).map(|_| rng.random::<u8>()).collect(),
                            span_id: (0..8).map(|_| rng.random::<u8>()).collect(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }
            })
            .collect();
        ExportLogsServiceRequest { resource_logs }
    }

    /// Scenario: A 4 MiB logs block of incompressible, all-unique rows is encoded with the encoder limits scaled down 16x (512 KiB row groups, 64 KiB slices, the production 8:1 ratio).
    /// Guarantees: The values file spans several row groups, both files together stay within B + B/8, the encoder peak stays within 2 x the row-group cap, the largest gathered slice within 2 x the slice size, and the measured total (two blocks, the encoded files, the sort keys and row weights, the largest slice, the output slack and the encoder) stays within worst_case_flush_bytes.
    #[tokio::test]
    async fn encode_memory_within_bound_across_row_groups() {
        let b = 4 * 1024 * 1024;
        let limits = EncodeLimits {
            row_group_bytes: 512 * 1024,
            slice_bytes: 64 * 1024,
        };
        let otap = to_otap(&worst_case_request(12_000));
        let mut block = Block::default();
        let mut cache = SeriesCache::new(100_000);
        let extracted =
            extract_logs(&otap, &Schemas::new(), &self::limits(b / 4), "host.id").expect("extract");
        for c in extracted.chunks {
            if block.bytes() + c.bytes() > b {
                break;
            }
            let _ = block.push(c, 1, &mut cache, partition()).expect("push");
        }
        let held = block.bytes();
        assert!(held > b / 2, "block filled to {held}");
        assert!(held <= b, "block filled to {held}");
        let e = encode(&mut block, limits).await;
        assert!(file(&e.values).metadata().num_row_groups() > 1);
        let series = e.series.as_ref().expect("series file");
        let encoded = series.content_length() + e.values.content_length();
        assert!(
            encoded <= b + b / 8,
            "encoded {encoded} > B + B/8 for B = {b}"
        );
        assert!(
            e.encoder_peak <= 2 * limits.row_group_bytes,
            "encoder peak {}",
            e.encoder_peak
        );
        assert!(
            e.slice_peak <= 2 * limits.slice_bytes,
            "slice peak {}",
            e.slice_peak
        );
        let config = LakeConfig::parse(&serde_json::json!({
            "storage": {"file": {"base_uri": "/tmp/x"}},
            "window": {"max_block_bytes": b},
            "ingress": {"max_extracted_bytes": b},
        }))
        .expect("config");
        let scaled_encoder = ENCODER_BYTES / 16;
        let sort_keys = e.rows * (SORT_KEY_BYTES + ROW_WEIGHT_BYTES);
        assert!(
            2 * b + encoded + sort_keys + e.slice_peak + 2 * OUTPUT_BLOCK_BYTES + e.encoder_peak
                <= config.worst_case_flush_bytes() - ENCODER_BYTES + scaled_encoder
        );
    }

    /// Scenario: A block holds 300 rows with 32 KiB bodies next to 20_000 small rows; sorted, the large rows are contiguous. It is encoded with the limits scaled down 16x (512 KiB row groups, 64 KiB slices).
    /// Guarantees: Slices are cut by the bytes of their rows, so a stretch of large rows never gathers more than about one slice at a time (the largest gathered slice stays within 2 x slice_bytes), the encoder peak stays within 2 x the row-group cap, and every row is written.
    #[tokio::test]
    async fn encode_slices_are_cut_by_row_bytes_for_skewed_rows() {
        let limits = EncodeLimits {
            row_group_bytes: 512 * 1024,
            slice_bytes: 64 * 1024,
        };
        let mut block = Block::default();
        let mut cache = SeriesCache::new(1000);
        let big = extract_logs(
            &to_otap(&sized_logs_request(300, 32 * 1024, 0)),
            &Schemas::new(),
            &self::limits(1 << 30),
            "host.id",
        )
        .expect("extract")
        .chunks;
        for c in big.into_iter().chain(chunks(20_000, 2, 1)) {
            let _ = block.push(c, 1, &mut cache, partition()).expect("push");
        }
        let e = encode(&mut block, limits).await;
        assert_eq!(e.rows, 20_300);
        assert!(
            e.slice_peak <= 2 * limits.slice_bytes,
            "a gathered slice held {} bytes in a 32 KiB row stretch",
            e.slice_peak
        );
        assert!(
            e.encoder_peak <= 2 * limits.row_group_bytes,
            "encoder peak {}",
            e.encoder_peak
        );
        assert!(file(&e.values).metadata().num_row_groups() > 1);
    }

    /// Scenario: A block spanning many encoder slices is encoded while a second future counts its own polls.
    /// Guarantees: The encode yields to the runtime between slices, so other work on the core thread makes progress before it completes.
    #[tokio::test]
    async fn encode_yields_between_slices() {
        let mut block = Block::default();
        let mut cache = SeriesCache::new(1000);
        for c in chunks(2_000, 4, 0) {
            let _ = block.push(c, 1, &mut cache, partition()).expect("push");
        }
        let limits = EncodeLimits {
            row_group_bytes: 512 * 1024,
            slice_bytes: 16 * 1024,
        };
        let done = std::cell::Cell::new(false);
        let polls = std::cell::Cell::new(0u32);
        let encode = async {
            let e = encode(&mut block, limits).await;
            done.set(true);
            e
        };
        let ticker = async {
            while !done.get() {
                polls.set(polls.get() + 1);
                tokio::task::yield_now().await;
            }
        };
        let (e, ()) = futures::join!(encode, ticker);
        assert_eq!(e.rows, 2_000);
        assert!(polls.get() > 2, "ticker ran {} times", polls.get());
    }
}
