// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Per-signal block buffer, block ids / object names, and one-time Parquet encoding.

use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{BooleanArray, RecordBatch, TimestampNanosecondArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, SchemaRef};
use chrono::{DateTime, Utc};
use object_store::path::Path;
use object_store::{PutPayload, PutPayloadMut};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

use super::config::{OUTPUT_BLOCK_BYTES, ROW_GROUP_BYTES, SLICE_BYTES};
use super::error::LakeError;
use super::extract::Chunk;
use super::identity::{FORMAT_VERSION, Signal};
use super::schema::Schemas;

/// Identity of a block; object names derive from it before the first upload attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockId {
    /// Signal.
    pub signal: Signal,
    /// UTC creation date `YYYY-MM-DD`.
    pub date: String,
    /// Writer id (`c<core>-<16 hex nonce>`), unique per exporter instance start.
    pub writer: String,
    /// Monotonic sequence within the writer.
    pub seq: u64,
}

impl BlockId {
    fn path(&self, dataset: &str) -> Path {
        Path::from(format!(
            "{}/{}/dt={}/{}-{:020}.parquet",
            self.signal.as_str(),
            dataset,
            self.date,
            self.writer,
            self.seq
        ))
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

/// `<signal>:<date>:<writer>:<seq>`, parsable by `FromStr` (probe from a fresh process).
impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}:{}",
            self.signal.as_str(),
            self.date,
            self.writer,
            self.seq
        )
    }
}

impl FromStr for BlockId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut it = s.splitn(4, ':');
        let (Some(sig), Some(date), Some(writer), Some(seq)) =
            (it.next(), it.next(), it.next(), it.next())
        else {
            return Err(format!("invalid block id `{s}`"));
        };
        let signal = match sig {
            "logs" => Signal::Logs,
            "metrics" => Signal::Metrics,
            other => return Err(format!("invalid signal `{other}`")),
        };
        let seq = seq.parse().map_err(|_| format!("invalid seq in `{s}`"))?;
        Ok(Self {
            signal,
            date: date.to_owned(),
            writer: writer.to_owned(),
            seq,
        })
    }
}

/// Contents taken out of a block; the block is already reset.
pub struct TakenBlock {
    /// Series rows.
    pub series: Vec<RecordBatch>,
    /// Values rows.
    pub values: Vec<RecordBatch>,
    /// Input batches with rows in this block.
    pub batches: BTreeSet<u64>,
    /// Wall-clock time of the first push (block creation; names the `dt=` partition).
    pub created: SystemTime,
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
    /// Production limits (ENCODER_BYTES = 2 x ROW_GROUP_BYTES assumes slice_bytes <=
    /// row_group_bytes / 8).
    pub const DEFAULT: Self = Self {
        row_group_bytes: ROW_GROUP_BYTES,
        slice_bytes: SLICE_BYTES,
    };
}

/// A block encoded once; retries resend these exact bytes.
pub struct EncodedBlock {
    /// Block id.
    pub id: BlockId,
    /// Series file.
    pub series: PutPayload,
    /// Values file.
    pub values: PutPayload,
    /// Values rows.
    pub rows: usize,
    /// Series rows.
    pub series_rows: usize,
    /// Largest `ArrowWriter::memory_size` observed after a write.
    pub encoder_peak: usize,
}

/// Open block of one signal.
#[derive(Default)]
pub struct Block {
    series: Vec<RecordBatch>,
    values: Vec<RecordBatch>,
    seen: HashSet<u128>,
    batches: BTreeSet<u64>,
    bytes: usize,
    opened_at: Option<(Instant, SystemTime)>,
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

    /// Age since the first push.
    #[must_use]
    pub fn age(&self, now: Instant) -> Duration {
        self.opened_at
            .map(|(t, _)| now.saturating_duration_since(t))
            .unwrap_or_default()
    }

    /// Add a chunk of input batch `seq`, dropping series candidates already in this block. On
    /// error the block is unchanged. Returns true when `seq` is new to this block (the caller adds
    /// a pending reference).
    pub fn push(&mut self, chunk: Chunk, seq: u64, now: Instant) -> Result<bool, LakeError> {
        let keep: BooleanArray = chunk
            .series_ids
            .iter()
            .map(|id| Some(!self.seen.contains(id)))
            .collect();
        let series = filter_record_batch(&chunk.series, &keep)?;
        self.seen.extend(chunk.series_ids.iter().copied());
        self.bytes += chunk.values.get_array_memory_size() + series.get_array_memory_size();
        if series.num_rows() > 0 {
            self.series.push(series);
        }
        self.values.push(chunk.values);
        let _ = self
            .opened_at
            .get_or_insert_with(|| (now, SystemTime::now()));
        Ok(self.batches.insert(seq))
    }

    /// Take the buffered rows and reset every field (including the seen set), so nothing stale
    /// survives a failed encode or upload.
    pub fn take(&mut self) -> TakenBlock {
        self.seen.clear();
        self.bytes = 0;
        let created = self
            .opened_at
            .take()
            .map_or_else(SystemTime::now, |(_, wall)| wall);
        TakenBlock {
            series: std::mem::take(&mut self.series),
            values: std::mem::take(&mut self.values),
            batches: std::mem::take(&mut self.batches),
            created,
        }
    }
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

/// Encode `series` and `values` (consumed; dropped once encoded) into two Parquet files. Series
/// rows get `written_at` stamped. Yields to the runtime after every slice.
pub async fn encode_block(
    id: BlockId,
    series: Vec<RecordBatch>,
    values: Vec<RecordBatch>,
    schemas: &Schemas,
    written_at: SystemTime,
    limits: EncodeLimits,
) -> Result<EncodedBlock, LakeError> {
    let ns = written_at
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or_default();
    let series_rows = series.iter().map(RecordBatch::num_rows).sum();
    let rows = values.iter().map(RecordBatch::num_rows).sum();
    let mut peak = 0;
    let series = stamp_written_at(series, ns)?;
    let signal = id.signal;
    let series_file = encode_file(
        schemas.series(signal),
        &series,
        "series",
        signal,
        limits,
        &mut peak,
    )
    .await?;
    drop(series);
    let values_file = encode_file(
        schemas.values(signal),
        &values,
        "values",
        signal,
        limits,
        &mut peak,
    )
    .await?;
    Ok(EncodedBlock {
        id,
        series: series_file,
        values: values_file,
        rows,
        series_rows,
        encoder_peak: peak,
    })
}

/// Replace the `written_at` column (index 1 of every series schema) with the flush time.
fn stamp_written_at(batches: Vec<RecordBatch>, ns: i64) -> Result<Vec<RecordBatch>, LakeError> {
    batches
        .into_iter()
        .map(|b| {
            let mut cols = b.columns().to_vec();
            cols[1] = Arc::new(
                TimestampNanosecondArray::from(vec![ns; b.num_rows()]).with_timezone("UTC"),
            );
            Ok(RecordBatch::try_new(b.schema(), cols)?)
        })
        .collect()
}

/// One Parquet file (ZSTD, format metadata), written in slices of about `limits.slice_bytes` with
/// a yield after each. Dictionary encoding is off for top-level fixed-width columns: their values
/// are mostly unique (times, trace ids, point values, series ids in the series file), where a
/// dictionary only adds index overhead and would break the B + B/8 output bound. `series_id` in
/// the values file repeats, so it keeps its dictionary.
async fn encode_file(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    dataset: &str,
    signal: Signal,
    limits: EncodeLimits,
    peak: &mut usize,
) -> Result<PutPayload, LakeError> {
    let mut props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_key_value_metadata(Some(vec![
            KeyValue::new(
                "otel.lake.format_version".into(),
                FORMAT_VERSION.to_string(),
            ),
            KeyValue::new(
                "otel.lake.dataset".into(),
                format!("{}/{dataset}", signal.as_str()),
            ),
        ]));
    for f in schema.fields() {
        let fixed_width =
            f.data_type().is_primitive() || matches!(f.data_type(), DataType::FixedSizeBinary(_));
        if fixed_width && !(dataset == "values" && f.name() == "series_id") {
            props = props.set_column_dictionary_enabled(ColumnPath::from(f.name().as_str()), false);
        }
    }
    let sink = PayloadSink(PutPayloadMut::new().with_block_size(OUTPUT_BLOCK_BYTES));
    let mut writer = ArrowWriter::try_new(sink, schema.clone(), Some(props.build()))?;
    for batch in batches {
        let rows = batch.num_rows();
        let per_row = (batch.get_array_memory_size() / rows.max(1)).max(1);
        let step = (limits.slice_bytes / per_row).max(1);
        let mut offset = 0;
        while offset < rows {
            let n = step.min(rows - offset);
            writer.write(&batch.slice(offset, n))?;
            *peak = (*peak).max(writer.memory_size());
            if writer.memory_size() >= limits.row_group_bytes {
                writer.flush()?;
            }
            offset += n;
            tokio::task::yield_now().await;
        }
    }
    Ok(writer.into_inner()?.0.freeze())
}

/// UTC date of `now` as `YYYY-MM-DD`.
#[must_use]
pub fn date_of(now: SystemTime) -> String {
    let dt: DateTime<Utc> = now.into();
    dt.format("%Y-%m-%d").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::parquet_lake_exporter::config::{ENCODER_BYTES, LakeConfig};
    use crate::exporters::parquet_lake_exporter::extract::extract_logs;
    use crate::exporters::parquet_lake_exporter::test_fixtures::{kv, logs_request, to_otap};
    use bytes::Bytes;
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, any_value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, ResourceLogs, ScopeLogs,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use rand::{RngExt, SeedableRng, rngs::StdRng};

    const BIG: usize = 1 << 30;

    fn chunks(rows: usize, resources: usize, seed: usize) -> Vec<Chunk> {
        extract_logs(
            &to_otap(&logs_request(rows, resources, seed)),
            &Schemas::new(),
            BIG,
        )
        .expect("extract")
    }

    fn file(payload: &PutPayload) -> SerializedFileReader<Bytes> {
        SerializedFileReader::new(Bytes::from(payload.clone())).expect("parquet file")
    }

    fn id(signal: Signal) -> BlockId {
        BlockId {
            signal,
            date: "2026-09-30".into(),
            writer: "c0-00000000000000ff".into(),
            seq: 7,
        }
    }

    async fn encode(block: &mut Block, limits: EncodeLimits) -> EncodedBlock {
        let t = block.take();
        encode_block(
            id(Signal::Logs),
            t.series,
            t.values,
            &Schemas::new(),
            SystemTime::now(),
            limits,
        )
        .await
        .expect("encode")
    }

    /// Scenario: Two chunks with the same two resources are pushed into one block from two batches.
    /// Guarantees: The block keeps one series row per series_id, all values rows, and reports each batch as new once.
    #[test]
    fn push_dedupes_series_within_block() {
        let mut block = Block::default();
        let now = Instant::now();
        for (seq, c) in chunks(10, 2, 0)
            .into_iter()
            .chain(chunks(10, 2, 1))
            .enumerate()
        {
            assert!(block.push(c, seq as u64, now).expect("push"));
        }
        let t = block.take();
        let series: usize = t.series.iter().map(RecordBatch::num_rows).sum();
        let values: usize = t.values.iter().map(RecordBatch::num_rows).sum();
        assert_eq!((series, values), (2, 20));
        assert_eq!(t.batches.into_iter().collect::<Vec<_>>(), vec![0, 1]);
    }

    /// Scenario: A block with data is taken, and a chunk with an already-seen series_id is pushed afterwards.
    /// Guarantees: `take` resets bytes, age, batches and the seen set, so the new block keeps that series row and nothing stale survives.
    #[test]
    fn take_resets_all_state() {
        let mut block = Block::default();
        let now = Instant::now();
        let _ = block
            .push(chunks(10, 1, 0).remove(0), 1, now)
            .expect("push");
        assert!(block.bytes() > 0);
        let t = block.take();
        assert_eq!(t.batches.len(), 1);
        assert!(block.is_empty());
        assert_eq!(block.bytes(), 0);
        assert_eq!(block.age(now + Duration::from_secs(5)), Duration::ZERO);
        assert!(
            block
                .push(chunks(10, 1, 0).remove(0), 1, now)
                .expect("push")
        );
        let t = block.take();
        assert_eq!(t.series.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    }

    /// Scenario: A chunk is pushed, time passes, and the block is taken.
    /// Guarantees: The block's creation time (which names its dt= partition) is the wall time of the first push, not of the take.
    #[test]
    fn take_records_creation_date() {
        let mut block = Block::default();
        let before = SystemTime::now();
        let _ = block
            .push(chunks(4, 1, 0).remove(0), 1, Instant::now())
            .expect("push");
        let after = SystemTime::now();
        std::thread::sleep(Duration::from_millis(20));
        let created = block.take().created;
        assert!(created >= before && created <= after);
    }

    /// Scenario: A logs block is encoded with a known written_at time.
    /// Guarantees: Both files are Parquet with the format_version and dataset metadata, the row counts match, and series rows carry written_at.
    #[tokio::test]
    async fn encoded_files_carry_format_metadata_and_written_at() {
        let mut block = Block::default();
        for c in chunks(12, 3, 0) {
            let _ = block.push(c, 1, Instant::now()).expect("push");
        }
        let t = block.take();
        let at = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let e = encode_block(
            id(Signal::Logs),
            t.series,
            t.values,
            &Schemas::new(),
            at,
            EncodeLimits::DEFAULT,
        )
        .await
        .expect("encode");
        assert_eq!((e.rows, e.series_rows), (12, 3));
        for (payload, dataset, rows) in [
            (&e.series, "logs/series", 3),
            (&e.values, "logs/values", 12),
        ] {
            let f = file(payload);
            let md = f.metadata().file_metadata();
            let kv: Vec<(String, Option<String>)> = md
                .key_value_metadata()
                .expect("kv")
                .iter()
                .map(|k| (k.key.clone(), k.value.clone()))
                .collect();
            assert!(kv.contains(&("otel.lake.format_version".into(), Some("1".into()))));
            assert!(kv.contains(&("otel.lake.dataset".into(), Some(dataset.into()))));
            assert_eq!(md.num_rows(), rows);
        }
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(
            Bytes::from(e.series.clone()),
            1024,
        )
        .expect("reader");
        for b in reader {
            let b = b.expect("batch");
            let w = b
                .column_by_name("written_at")
                .expect("written_at")
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ts")
                .clone();
            assert!(w.iter().all(|v| v == Some(1_800_000_000_000_000_000)));
        }
    }

    /// Scenario: A BlockId is formatted, parsed back, and its object names are derived.
    /// Guarantees: The id round-trips through its string form, and both files share a stem under <signal>/<dataset>/dt=<date>/.
    #[test]
    fn block_id_round_trips_and_paths() {
        let b = id(Signal::Metrics);
        assert_eq!(b.to_string().parse::<BlockId>(), Ok(b.clone()));
        assert_eq!(
            b.series_path().as_ref(),
            "metrics/series/dt=2026-09-30/c0-00000000000000ff-00000000000000000007.parquet"
        );
        assert_eq!(
            b.values_path().as_ref(),
            "metrics/values/dt=2026-09-30/c0-00000000000000ff-00000000000000000007.parquet"
        );
        assert!("traces:x:y:1".parse::<BlockId>().is_err());
        assert!("logs:x:y".parse::<BlockId>().is_err());
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
    /// Guarantees: The values file spans several row groups, both files together stay within B + B/8, the encoder peak stays within 2 x the row-group cap, and the measured total stays within worst_case_flush_bytes.
    #[tokio::test]
    async fn encode_memory_within_bound_across_row_groups() {
        let b = 4 * 1024 * 1024;
        let limits = EncodeLimits {
            row_group_bytes: 512 * 1024,
            slice_bytes: 64 * 1024,
        };
        let otap = to_otap(&worst_case_request(12_000));
        let mut block = Block::default();
        for c in extract_logs(&otap, &Schemas::new(), b / 4).expect("extract") {
            if block.bytes() + c.bytes() > b {
                break;
            }
            let _ = block.push(c, 1, Instant::now()).expect("push");
        }
        let held = block.bytes();
        assert!(held > b / 2, "block filled to {held}");
        let e = encode(&mut block, limits).await;
        assert!(file(&e.values).metadata().num_row_groups() > 1);
        let encoded = e.series.content_length() + e.values.content_length();
        assert!(
            encoded <= b + b / 8,
            "encoded {encoded} > B + B/8 for B = {b}"
        );
        assert!(
            e.encoder_peak <= 2 * limits.row_group_bytes,
            "encoder peak {}",
            e.encoder_peak
        );
        let config = LakeConfig {
            max_block_bytes: b,
            ..LakeConfig::parse(&serde_json::json!({"storage": {"file": {"base_uri": "/tmp/x"}}}))
                .expect("config")
        };
        let scaled_encoder = ENCODER_BYTES / 16;
        assert!(
            2 * b + encoded + 2 * OUTPUT_BLOCK_BYTES + e.encoder_peak
                <= config.worst_case_flush_bytes() - ENCODER_BYTES + scaled_encoder
        );
    }

    /// Scenario: A block spanning many encoder slices is encoded while a second future counts its own polls.
    /// Guarantees: The encode yields to the runtime between slices, so other work on the core thread makes progress before it completes.
    #[tokio::test]
    async fn encode_yields_between_slices() {
        let mut block = Block::default();
        for c in chunks(2_000, 4, 0) {
            let _ = block.push(c, 1, Instant::now()).expect("push");
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
