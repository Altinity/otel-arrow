// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Tests of the Parquet sink.

use super::properties::leaf_path;
use super::write::{UploadLedger, chunk_charge, creation_watch};
use super::*;
use crate::buffer::Block;
use crate::cache::SeriesCache;
use crate::canonical::Signal;
use crate::clock::PartitionId;
use crate::config::{LakeConfig, Nulls, SortOrder};
use crate::error::{Error, TransientError};
use crate::extract::extract;
use crate::hook_store::{HookGuard, HookStore, StoreHooks};
use crate::schema::{Dataset, dataset_schema, schema_fingerprint};
use crate::sort::SortSpec;
use crate::sort::merge_runs;
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{MultipartUpload, PutPayload, PutResult, UploadPart};
use otel_arrow_dfe_pdata::otap::memory::{CountedAllocations, record_batch_pinned_bytes};
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    AnyValue, KeyValue as OtlpKeyValue, any_value,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, LogsData, ResourceLogs, ScopeLogs,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, MetricsData,
    NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::testing::round_trip::{encode_logs, encode_metrics};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const WINDOW_START: i64 = 1_789_960_500;
const SEAL_AT_US: i64 = 1_789_960_500_000_000;
const SEQ: u64 = 7;
/// First `time_unix_nano` of the generated log records; they count down.
const FIRST_LOG_TIME: u64 = 9_000_000;

fn kv(k: &str, v: &str) -> OtlpKeyValue {
    OtlpKeyValue {
        key: k.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(v.into())),
        }),
    }
}

/// `n` log records whose bodies are `body_len` hex characters wide.
fn logs(n: usize, body_len: usize) -> LogsData {
    LogsData {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv("host.id", "h")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: (0..n)
                    .map(|i| LogRecord {
                        time_unix_nano: FIRST_LOG_TIME - i as u64,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(body(i, body_len))),
                        }),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// A body of `len` hex characters that differs for every `i`.
///
/// The bytes come from a splitmix-style mix so that ZSTD cannot fold the
/// payload away: the upload tests need the written object to actually pass
/// the 5 MiB multipart threshold.
fn body(i: usize, len: usize) -> String {
    let mut s = String::with_capacity(len);
    let mut x = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    while s.len() < len {
        x ^= x >> 33;
        x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        s.push_str(&format!("{x:016x}"));
    }
    s.truncate(len);
    s
}

fn seal_logs(cfg: &LakeConfig, n: usize, body_len: usize) -> Block {
    let mut cache = SeriesCache::new(10);
    let mut b = Block::new(WINDOW_START, SEQ, cfg.clone());
    let mut records = encode_logs(&logs(n, body_len));
    let e = extract(&mut records, cfg).expect("extract");
    let r = b.reserve(&e, &mut cache, 8).expect("reserve");
    b.admit(e, r).expect("admit");
    b.seal(SEAL_AT_US).expect("seal");
    b
}

/// A sealed block of 30 small log rows.
fn sealed_block(cfg: &LakeConfig, n: usize) -> Block {
    seal_logs(cfg, n, 8)
}

/// A sealed block whose values object is comfortably larger than one 5 MiB
/// multipart part, so that the upload tests reach an in-flight `put_part`
/// while the writer is still in its writable phase.
fn sealed_upload_block(cfg: &LakeConfig) -> Block {
    seal_logs(cfg, 24_000, 1_024)
}

/// Config whose writer hands the object store several parts during the
/// chunk loop: real 5 MiB parts, one in-flight part at a time, and row
/// groups small enough that the loop flushes repeatedly.
fn upload_config() -> LakeConfig {
    let mut cfg = LakeConfig::default();
    cfg.ingress.max_request_bytes = 64 << 20;
    cfg.ingress.max_extracted_bytes = 64 << 20;
    cfg.upload.part_bytes = 5 << 20;
    cfg.upload.concurrency = 1;
    cfg.parquet.row_group_bytes = 256 << 10;
    cfg.sorting.merge_chunk_bytes = 512 << 10;
    cfg.validate().expect("valid config");
    cfg
}

/// The cleanup allowance on tokio's clock.
pub(super) fn tokio_timer(timeout: Duration) -> AbortTimer {
    Box::pin(tokio::time::sleep(timeout))
}

fn local(dir: &tempfile::TempDir) -> Arc<dyn ObjectStore> {
    Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("fs"))
}

fn naming(writer_id: &str, boot_id: &str) -> FileNaming {
    FileNaming {
        writer_id: writer_id.into(),
        boot_id: boot_id.into(),
    }
}

fn injected() -> object_store::Error {
    object_store::Error::Generic {
        store: "test",
        source: "injected failure".into(),
    }
}

/// What the wrapping multipart upload does with each part.
#[derive(Debug, Clone, Copy)]
enum PartBehavior {
    /// Never resolve, leaving the part in flight.
    Park,
    /// Fail immediately.
    Fail,
}

/// What the wrapping multipart upload does when it is aborted.
#[derive(Debug, Clone, Copy)]
enum AbortBehavior {
    /// Delegate to the real upload.
    Delegate,
    /// Fail the abort.
    Fail,
    /// Never resolve, so only the abort timeout ends it.
    Hang,
}

/// Store hooks that let real multipart uploads start and then control how
/// their parts and aborts behave.
///
/// The creation is not delayed: the `BufWriter` must actually reach its
/// `Write` state, the only state in which `BufWriter::abort` does anything.
#[derive(Debug)]
struct ControlledMultipart {
    /// Notified when a part upload has started.
    entered: Arc<Notify>,
    /// Set once the upload has been aborted.
    aborted: Arc<AtomicBool>,
    /// Number of parts the writer handed to the upload.
    parts: Arc<AtomicUsize>,
    part: PartBehavior,
    abort: AbortBehavior,
}

/// The upload handed back by [`ControlledMultipart`].
#[derive(Debug)]
struct ControlledUpload {
    inner: Box<dyn MultipartUpload>,
    entered: Arc<Notify>,
    aborted: Arc<AtomicBool>,
    parts: Arc<AtomicUsize>,
    part: PartBehavior,
    abort: AbortBehavior,
}

#[async_trait::async_trait]
impl MultipartUpload for ControlledUpload {
    fn put_part(&mut self, _data: PutPayload) -> UploadPart {
        let _ = self.parts.fetch_add(1, Ordering::SeqCst);
        // A permit, not a broadcast: the canceller must observe the part
        // even if it subscribes afterwards.
        self.entered.notify_one();
        match self.part {
            PartBehavior::Park => Box::pin(std::future::pending()),
            PartBehavior::Fail => Box::pin(std::future::ready(Err(injected()))),
        }
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.aborted.store(true, Ordering::SeqCst);
        match self.abort {
            AbortBehavior::Delegate => self.inner.abort().await,
            AbortBehavior::Fail => {
                let _ = self.inner.abort().await;
                Err(injected())
            }
            AbortBehavior::Hang => std::future::pending().await,
        }
    }
}

#[async_trait::async_trait]
impl StoreHooks for ControlledMultipart {
    fn wrap_upload(
        &self,
        _location: &Path,
        inner: Box<dyn MultipartUpload>,
    ) -> Box<dyn MultipartUpload> {
        Box::new(ControlledUpload {
            inner,
            entered: self.entered.clone(),
            aborted: self.aborted.clone(),
            parts: self.parts.clone(),
            part: self.part,
            abort: self.abort,
        })
    }
}

/// Store hooks that make a multipart creation slow: they cancel a token as
/// the values upload is being created and yield before the creation
/// finishes, so the cancellation lands while `BufWriter` is still
/// preparing the upload. With `stall`, the creation never finishes.
#[derive(Debug)]
struct CancelDuringCreate {
    token: CancellationToken,
    stall: bool,
}

#[async_trait::async_trait]
impl StoreHooks for CancelDuringCreate {
    async fn before_multipart(&self, location: &Path) -> object_store::Result<Option<HookGuard>> {
        if location.as_ref().contains("dataset=values") {
            self.token.cancel();
            if self.stall {
                std::future::pending::<()>().await;
            }
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }
        }
        Ok(None)
    }
}

/// Config whose values table reaches the store only in `finish`: its one row
/// group stays below `parquet.row_group_bytes`, so the multipart upload is
/// created while the writer writes its footer.
fn finish_upload_config() -> LakeConfig {
    let mut cfg = upload_config();
    cfg.parquet.row_group_bytes = 64 << 20;
    cfg.validate().expect("valid config");
    cfg
}

/// An abort allowance that is already spent, so a cleanup wait ends at once.
fn spent_timer(_timeout: Duration) -> AbortTimer {
    Box::pin(std::future::ready(()))
}

/// Store hooks that cancel a token the first time the values dataset
/// object is opened, so a test can land a cancellation inside the chunk loop
/// without depending on the scheduler.
#[derive(Debug)]
struct CancelOnValues {
    token: CancellationToken,
    /// Set when the trip happened on a multipart upload, which only starts
    /// while the writer is still writable.
    tripped_multipart: Arc<AtomicBool>,
}

impl CancelOnValues {
    fn trip(&self, location: &Path, multipart: bool) -> bool {
        if !location.as_ref().contains("dataset=values") {
            return false;
        }
        if multipart {
            self.tripped_multipart.store(true, Ordering::SeqCst);
        }
        self.token.cancel();
        true
    }
}

#[async_trait::async_trait]
impl StoreHooks for CancelOnValues {
    async fn before_put(
        &self,
        location: &Path,
        _payload: &PutPayload,
    ) -> object_store::Result<Option<HookGuard>> {
        let _ = self.trip(location, false);
        Ok(None)
    }

    async fn before_multipart(&self, location: &Path) -> object_store::Result<Option<HookGuard>> {
        let _ = self.trip(location, true);
        Ok(None)
    }
}

/// Parquet files under `root` whose path contains `needle`.
fn parquet_count(root: &std::path::Path, needle: &str) -> usize {
    fn walk(p: &std::path::Path, needle: &str, n: &mut usize) {
        for e in std::fs::read_dir(p).expect("dir") {
            let e = e.expect("entry");
            let path = e.path();
            if path.is_dir() {
                walk(&path, needle, n);
            } else if path.extension().is_some_and(|x| x == "parquet")
                && path.to_string_lossy().contains(needle)
            {
                *n += 1;
            }
        }
    }
    let mut n = 0;
    walk(root, needle, &mut n);
    n
}

/// Every Parquet file under `root`.
fn walkdir_count(root: &std::path::Path) -> usize {
    parquet_count(root, "")
}

/// Key-value metadata of a written file, as a lookup closure.
fn file_kv(root: &std::path::Path, path: &Path) -> Vec<(String, String)> {
    let file = std::fs::File::open(root.join(path.as_ref())).expect("open");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).expect("reader");
    reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("kv")
        .iter()
        .map(|e| (e.key.clone(), e.value.clone().unwrap_or_default()))
        .collect()
}

fn get<'a>(kv: &'a [(String, String)], key: &str) -> &'a str {
    kv.iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .unwrap_or_else(|| panic!("missing metadata key {key}"))
}

/// The native `SortingColumn` list of every row group of a written file,
/// as `(leaf index, descending, nulls_first)`.
fn row_group_sorting(root: &std::path::Path, path: &Path) -> Vec<Option<Vec<(i32, bool, bool)>>> {
    let file = std::fs::File::open(root.join(path.as_ref())).expect("open");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).expect("reader");
    reader
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| {
            rg.sorting_columns().map(|cols| {
                cols.iter()
                    .map(|c| (c.column_idx, c.descending, c.nulls_first))
                    .collect()
            })
        })
        .collect()
}

/// Scenario: the default `values_sort` of logs and of metrics over their values schemas.
/// Guarantees: the native declaration is `series_id` (leaf 0) and `time_unix_nano`, leaf 3 in
/// logs and leaf 4 in metrics, whose `metric_name` comes first, both ascending (FORMAT.md
/// section 5).
#[test]
fn default_values_sorts_declare_the_documented_leaves() {
    let cfg = LakeConfig::default();
    for (ds, time_leaf) in [(Dataset::LogsValues, 3), (Dataset::MetricsValues, 4)] {
        let cols =
            native_sorting_columns(&SortSpec::for_dataset(ds, &cfg), &dataset_schema(ds, &cfg))
                .expect("convert")
                .expect("two keys");
        let got: Vec<_> = cols
            .iter()
            .map(|c| (c.column_idx, c.descending, c.nulls_first))
            .collect();
        assert_eq!(
            got,
            [(0, false, false), (time_leaf, false, false)],
            "{ds:?}"
        );
    }
}

/// Scenario: sort specs over the values schemas: a key after the two-leaf `attrs` map, desc
/// nulls-first, list and double keys first and in the middle, and none.
/// Guarantees: `column_idx` is the leaf index, order and nulls carry over, the list stops at the
/// first list or double key, and an empty prefix emits nothing.
#[test]
fn native_sorting_columns_use_leaf_indexes_and_a_primitive_prefix() {
    use crate::config::{DenormType, Denormalize, SortKey};
    let key = |column: &str, order, nulls| SortKey {
        column: column.into(),
        order,
        nulls,
    };
    let mut cfg = LakeConfig::default();
    cfg.logs.denormalize = vec![Denormalize {
        path: "resource.service.name".into(),
        column: "service_name".into(),
        ty: DenormType::String,
    }];
    let logs = dataset_schema(Dataset::LogsValues, &cfg);
    // Arrow field 14; `attrs` (field 13) has two leaves, so leaf 15.
    assert_eq!(logs.index_of("service_name").expect("field"), 14);
    let spec = SortSpec::new(vec![
        key("service_name", SortOrder::Asc, Nulls::Last),
        key("time_unix_nano", SortOrder::Desc, Nulls::First),
    ]);
    let cols = native_sorting_columns(&spec, &logs)
        .expect("convert")
        .expect("two keys");
    let got: Vec<_> = cols
        .iter()
        .map(|c| (c.column_idx, c.descending, c.nulls_first))
        .collect();
    assert_eq!(got, [(15, false, false), (3, true, true)]);

    let metrics = dataset_schema(Dataset::MetricsValues, &cfg);
    let spec = SortSpec::new(vec![
        key("series_id", SortOrder::Asc, Nulls::Last),
        key("bucket_counts", SortOrder::Asc, Nulls::Last),
        key("time_unix_nano", SortOrder::Asc, Nulls::Last),
    ]);
    let cols = native_sorting_columns(&spec, &metrics)
        .expect("convert")
        .expect("one key");
    assert_eq!(cols.len(), 1, "stops at the list key");
    assert_eq!(cols[0].column_idx, 0);
    let spec = SortSpec::new(vec![key("bucket_counts", SortOrder::Asc, Nulls::Last)]);
    assert_eq!(
        native_sorting_columns(&spec, &metrics).expect("convert"),
        None
    );
    let spec = SortSpec::new(vec![
        key("series_id", SortOrder::Asc, Nulls::Last),
        key("value_double", SortOrder::Asc, Nulls::Last),
        key("time_unix_nano", SortOrder::Asc, Nulls::Last),
    ]);
    let cols = native_sorting_columns(&spec, &metrics)
        .expect("convert")
        .expect("one key");
    assert_eq!(cols.len(), 1, "stops at the double key");
    assert_eq!(cols[0].column_idx, 0);
    let spec = SortSpec::new(vec![key("value_double", SortOrder::Asc, Nulls::Last)]);
    assert_eq!(
        native_sorting_columns(&spec, &metrics).expect("convert"),
        None
    );
    assert_eq!(
        native_sorting_columns(&SortSpec::new(Vec::new()), &metrics).expect("convert"),
        None
    );
}

/// Scenario: a sealed block of 30 log records is written.
/// Guarantees: during the write the sink reports exactly its largest merge's key heap, and nothing
/// afterwards.
#[tokio::test]
async fn the_sink_reports_the_merge_keys_it_holds() {
    let dir = tempfile::tempdir().expect("tmp");
    let cfg = LakeConfig::default();
    let b = sealed_block(&cfg, 30);
    let expected = b
        .tables()
        .filter(|table| !table.is_empty())
        .map(|table| {
            merge_runs(
                table.iter_snapshots().cloned().collect(),
                table.spec(),
                cfg.sorting.merge_chunk_bytes,
            )
            .expect("merge")
            .resident_key_bytes()
        })
        .max()
        .expect("a table");
    let sink = Sink::new(local(&dir), cfg.clone(), naming("w", "keys"), tokio_timer);
    assert_eq!(sink.merge_key_high_water_bytes(), 0);
    let _ = sink
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("write");
    assert_eq!(sink.merge_key_bytes(), 0, "no table is being written");
    assert_eq!(sink.merge_key_high_water_bytes(), expected);
    assert!(
        expected >= 30 * ((1 + 16) + (1 + 8)),
        "{expected} key bytes"
    );
}

/// Scenario: a 30-row logs block with tiny row groups, sorted by default, unsorted, and with a
/// denormalized double second key.
/// Guarantees: every row group carries the native list of the sorted prefix (`series_id`,
/// `time_unix_nano` by default), none when unsorted, while `sort_key` names every key.
#[tokio::test]
async fn every_row_group_carries_the_native_sorting_columns() {
    let dir = tempfile::tempdir().expect("tmp");
    let mut cfg = LakeConfig::default();
    cfg.parquet.row_group_bytes = 1;
    cfg.sorting.merge_chunk_bytes = 1;
    let b = sealed_block(&cfg, 30);
    let sink = Sink::new(local(&dir), cfg.clone(), naming("w", "sorted"), tokio_timer);
    let report = sink
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("write");
    let values = row_group_sorting(dir.path(), &report.files[1].1);
    assert!(values.len() > 1, "row groups: {}", values.len());
    for rg in &values {
        assert_eq!(
            rg.as_deref(),
            Some(&[(0, false, false), (3, false, false)][..])
        );
    }
    assert_eq!(
        get(&file_kv(dir.path(), &report.files[1].1), "sort_key"),
        "series_id:asc:nulls_last,time_unix_nano:asc:nulls_last"
    );
    for rg in row_group_sorting(dir.path(), &report.files[0].1) {
        assert_eq!(rg.as_deref(), Some(&[(0, false, false)][..]));
    }

    let mut unsorted = LakeConfig::default();
    unsorted.logs.values_sort = Vec::new();
    let b = sealed_block(&unsorted, 30);
    let sink = Sink::new(local(&dir), unsorted, naming("w", "unsorted"), tokio_timer);
    let report = sink
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("write");
    assert_eq!(
        get(&file_kv(dir.path(), &report.files[1].1), "sort_key"),
        "none"
    );
    for rg in row_group_sorting(dir.path(), &report.files[1].1) {
        assert_eq!(rg, None);
    }
    for rg in row_group_sorting(dir.path(), &report.files[0].1) {
        assert_eq!(rg.as_deref(), Some(&[(0, false, false)][..]));
    }

    let key = |column: &str| crate::config::SortKey {
        column: column.into(),
        order: SortOrder::Asc,
        nulls: Nulls::Last,
    };
    let mut double = LakeConfig::default();
    double.logs.denormalize = vec![crate::config::Denormalize {
        path: "attrs.latency".into(),
        column: "latency".into(),
        ty: crate::config::DenormType::Double,
    }];
    double.logs.values_sort = vec![key("series_id"), key("latency"), key("time_unix_nano")];
    double.validate().expect("valid double sort");
    let b = sealed_block(&double, 30);
    let sink = Sink::new(local(&dir), double, naming("w", "double"), tokio_timer);
    let report = sink
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("write");
    assert_eq!(
        get(&file_kv(dir.path(), &report.files[1].1), "sort_key"),
        "series_id:asc:nulls_last,latency:asc:nulls_last,time_unix_nano:asc:nulls_last"
    );
    for rg in row_group_sorting(dir.path(), &report.files[1].1) {
        assert_eq!(rg.as_deref(), Some(&[(0, false, false)][..]));
    }
}

/// Scenario: path components for a known window and sequence.
/// Guarantees: the Hive layout and file name of FORMAT.md section 4 are produced exactly.
#[test]
fn object_path_layout() {
    let p = object_path(
        Dataset::LogsValues,
        PartitionId::from_unix_secs(WINDOW_START),
        WINDOW_START,
        &naming("w1", "b"),
        42,
    );
    assert_eq!(
        p.as_ref(),
        "v=1/signal=logs/dataset=values/date=2026-09-21/hour=03/part-20260921T031500Z-w1-b-00000042.parquet"
    );
}

/// Scenario: a hyphenated `writer_id` such as a pod name in a file name.
/// Guarantees: the name still splits into stamp, writer id, boot id and
/// sequence, since the stamp, the boot id and the sequence hold no `-`.
#[test]
fn object_path_keeps_a_hyphenated_writer_id_recoverable() {
    let boot = FileNaming::new("x").boot_id;
    assert!(!boot.contains('-'), "{boot}");
    let p = object_path(
        Dataset::LogsValues,
        PartitionId::from_unix_secs(WINDOW_START),
        WINDOW_START,
        &naming("otel-lake-7d9f-0", &boot),
        42,
    );
    let name = p.filename().expect("file name");
    let stem = name
        .strip_prefix("part-")
        .and_then(|n| n.strip_suffix(".parquet"))
        .expect("part-*.parquet");
    let (stamp, rest) = stem.split_once('-').expect("stamp");
    let (rest, seq) = rest.rsplit_once('-').expect("seq");
    let (writer, boot_id) = rest.rsplit_once('-').expect("boot id");
    assert_eq!(
        (stamp, writer, boot_id, seq),
        (
            "20260921T031500Z",
            "otel-lake-7d9f-0",
            boot.as_str(),
            "00000042"
        )
    );
}

/// Scenario: a `writer_id` with `/` reaches `object_path` without validation.
/// Guarantees: the `/` is encoded into one file-name segment that decodes back.
#[test]
fn object_path_encodes_slash_in_writer_id_as_one_segment() {
    let p = object_path(
        Dataset::LogsValues,
        PartitionId::from_unix_secs(WINDOW_START),
        WINDOW_START,
        &naming("team/writer", "b"),
        42,
    );
    let raw = p.as_ref();
    assert_eq!(raw.matches('/').count(), 5, "path: {raw}");
    let file_name = raw.rsplit('/').next().expect("at least one segment");
    assert!(
        file_name.contains("team%2Fwriter"),
        "file name: {file_name}"
    );
    let parts: Vec<String> = p.parts().map(|part| part.as_ref().to_string()).collect();
    assert_eq!(parts.len(), 6, "parts: {parts:?}");
}

/// Scenario: a 30-row logs block written to a local directory and read back.
/// Guarantees: series next to values, values sorted, every FORMAT.md section 5 key set, row count
/// as reported, and a rewrite uses the same names.
#[tokio::test]
async fn writes_series_before_values_and_reads_back() {
    let dir = tempfile::tempdir().expect("tmp");
    let cfg = LakeConfig::default();
    let b = sealed_block(&cfg, 30);
    let sink = Sink::new(local(&dir), cfg.clone(), naming("w", "boot"), tokio_timer);
    let report = sink
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("write");
    assert_eq!(report.files.len(), 2);
    assert_eq!(report.files[0].0, Dataset::LogsSeries);
    assert_eq!(report.files[1].0, Dataset::LogsValues);
    // What the sink announces before writing is what it writes, so an
    // observer can record the names an attempt will touch.
    assert_eq!(
        sink.planned_paths(&b),
        report
            .files
            .iter()
            .map(|(_, path, _)| path.clone())
            .collect::<Vec<_>>()
    );

    let (_, values_path, values_rows) = &report.files[1];
    let kv = file_kv(dir.path(), values_path);
    assert_eq!(get(&kv, "format_version"), "1");
    assert_eq!(get(&kv, "series_hash"), "xxh3_128/canonical_v1");
    assert_eq!(
        get(&kv, "schema_fingerprint"),
        format!(
            "{:016x}",
            schema_fingerprint(&dataset_schema(Dataset::LogsValues, &cfg))
        )
    );
    assert_eq!(
        get(&kv, "sort_key"),
        "series_id:asc:nulls_last,time_unix_nano:asc:nulls_last"
    );
    assert_eq!(get(&kv, "writer_id"), "w");
    assert_eq!(get(&kv, "boot_id"), "boot");
    assert_eq!(get(&kv, "seq"), SEQ.to_string());
    assert_eq!(get(&kv, "window_start"), "1789960500");
    assert_eq!(get(&kv, "window_end"), "1789960515");
    // The metadata row count is the count the sink actually wrote.
    assert_eq!(get(&kv, "row_count"), "30");
    assert_eq!(get(&kv, "row_count"), values_rows.to_string());
    // Record i carries FIRST_LOG_TIME - i, for i in 0..30.
    assert_eq!(
        get(&kv, "min_time_unix_nano"),
        (FIRST_LOG_TIME - 29).to_string()
    );
    assert_eq!(get(&kv, "max_time_unix_nano"), FIRST_LOG_TIME.to_string());
    assert_eq!(get(&kv, "identity_config"), r#"{"series_attributes":[]}"#);
    assert_eq!(
        get(&kv, "identity_config_hash"),
        cfg.identity_config_hash(Signal::Logs)
    );

    // A series dataset has no time column, so it carries no time bounds.
    let series_kv = file_kv(dir.path(), &report.files[0].1);
    assert_eq!(
        get(&series_kv, "identity_config"),
        get(&kv, "identity_config")
    );
    assert_eq!(
        get(&series_kv, "identity_config_hash"),
        get(&kv, "identity_config_hash")
    );
    assert_eq!(get(&series_kv, "row_count"), report.files[0].2.to_string());
    assert!(!series_kv.iter().any(|(k, _)| k == "min_time_unix_nano"));
    assert!(!series_kv.iter().any(|(k, _)| k == "max_time_unix_nano"));
    assert_eq!(
        get(&series_kv, "schema_fingerprint"),
        format!(
            "{:016x}",
            schema_fingerprint(&dataset_schema(Dataset::LogsSeries, &cfg))
        )
    );

    let file = std::fs::File::open(dir.path().join(values_path.as_ref())).expect("open");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).expect("reader");
    let batches: Vec<_> = reader
        .build()
        .expect("build")
        .map(|b| b.expect("batch"))
        .collect();
    let all = arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat");
    assert_eq!(all.num_rows(), 30);
    let spec = SortSpec::new(cfg.logs.values_sort.clone());
    assert!(crate::sort::is_sorted(&all, &spec).expect("sorted"));

    // rewrite: same names, still two files on disk
    let report2 = sink
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("rewrite");
    assert_eq!(report.files[1].1, report2.files[1].1);
    assert_eq!(walkdir_count(dir.path()), 2);
}

/// Scenario: a metrics block with a gauge and a histogram.
/// Guarantees: series first, then one merged values file with the reported row count.
#[tokio::test]
async fn metrics_block_writes_series_before_the_merged_values_dataset() {
    let dir = tempfile::tempdir().expect("tmp");
    let cfg = LakeConfig::default();
    let mut cache = SeriesCache::new(10);
    let mut b = Block::new(WINDOW_START, SEQ, cfg.clone());
    let mut records = encode_metrics(&gauge_and_histogram());
    let e = extract(&mut records, &cfg).expect("extract");
    let r = b.reserve(&e, &mut cache, 8).expect("reserve");
    b.admit(e, r).expect("admit");
    b.seal(SEAL_AT_US).expect("seal");

    let sink = Sink::new(local(&dir), cfg.clone(), naming("w", "boot"), tokio_timer);
    let report = sink
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("write");
    let order: Vec<Dataset> = report.files.iter().map(|(d, _, _)| *d).collect();
    assert_eq!(order, vec![Dataset::MetricsSeries, Dataset::MetricsValues]);
    assert_eq!(parquet_count(dir.path(), "dataset=series"), 1);
    assert_eq!(parquet_count(dir.path(), "dataset=values"), 1);
    assert_eq!(walkdir_count(dir.path()), 2);
    for (ds, path, rows) in &report.files {
        assert!(*rows > 0, "{ds:?} wrote no rows");
        let kv = file_kv(dir.path(), path);
        assert_eq!(get(&kv, "row_count"), rows.to_string());
        assert_eq!(
            get(&kv, "schema_fingerprint"),
            format!("{:016x}", schema_fingerprint(&dataset_schema(*ds, &cfg)))
        );
        assert_eq!(
            kv.iter().any(|(k, _)| k == "min_time_unix_nano"),
            !ds.is_series()
        );
        assert_eq!(get(&kv, "identity_config"), "{}");
        assert_eq!(
            get(&kv, "identity_config_hash"),
            cfg.identity_config_hash(Signal::Metrics)
        );
    }
}

fn gauge_and_histogram() -> MetricsData {
    let dp = |t: u64, v: f64| NumberDataPoint {
        time_unix_nano: t,
        value: Some(number_data_point::Value::AsDouble(v)),
        attributes: vec![kv("cpu", "0")],
        ..Default::default()
    };
    MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("host.id", "h1")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![
                    Metric {
                        name: "cpu".into(),
                        unit: "1".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![dp(10, 0.5), dp(20, 0.7)],
                        })),
                        ..Default::default()
                    },
                    Metric {
                        name: "lat".into(),
                        data: Some(metric::Data::Histogram(Histogram {
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            data_points: vec![HistogramDataPoint {
                                time_unix_nano: 40,
                                count: 3,
                                sum: Some(6.0),
                                bucket_counts: vec![1, 2],
                                explicit_bounds: vec![5.0],
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Scenario: a sealed block into which nothing was ever admitted.
/// Guarantees: no dataset file is created for a zero-row dataset (FORMAT.md section 4).
#[tokio::test]
async fn empty_block_writes_no_file() {
    let dir = tempfile::tempdir().expect("tmp");
    let cfg = LakeConfig::default();
    let mut b = Block::new(WINDOW_START, SEQ, cfg.clone());
    b.seal(SEAL_AT_US).expect("seal");
    let sink = Sink::new(local(&dir), cfg, FileNaming::new("w"), tokio_timer);
    let report = sink
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("write");
    assert!(report.files.is_empty());
    assert_eq!(walkdir_count(dir.path()), 0);
}

/// Scenario: a block that was never sealed.
/// Guarantees: the sink refuses it in every build profile.
#[tokio::test]
async fn write_block_rejects_an_unsealed_block() {
    let dir = tempfile::tempdir().expect("tmp");
    let cfg = LakeConfig::default();
    let b = Block::new(WINDOW_START, SEQ, cfg.clone());
    assert!(!b.is_sealed());
    let sink = Sink::new(local(&dir), cfg, FileNaming::new("w"), tokio_timer);
    let err = sink
        .write_block(&b, &CancellationToken::new())
        .await
        .expect_err("an unsealed block is refused");
    assert!(err.to_string().contains("unsealed block"));
}

/// Scenario: `writer_limit_bytes` and `merge_chunk_bytes` so low every chunk closes a row group.
/// Guarantees: the file has several row groups and every row.
#[tokio::test]
async fn writer_limit_closes_row_groups() {
    let dir = tempfile::tempdir().expect("tmp");
    let mut cfg = LakeConfig::default();
    cfg.parquet.writer_limit_bytes = 1;
    cfg.sorting.merge_chunk_bytes = 1;
    cfg.validate().expect("valid config");
    let b = sealed_block(&cfg, 40);
    let sink = Sink::new(local(&dir), cfg, FileNaming::new("w"), tokio_timer);
    let report = sink
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("write");
    let values = report
        .files
        .iter()
        .find(|(d, _, _)| *d == Dataset::LogsValues)
        .expect("values file");
    let file = std::fs::File::open(dir.path().join(values.1.as_ref())).expect("open");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).expect("reader");
    assert!(
        reader.metadata().num_row_groups() > 1,
        "the writer memory limit must close row groups"
    );
    let rows: usize = reader
        .build()
        .expect("build")
        .map(|b| b.expect("batch").num_rows())
        .sum();
    assert_eq!(rows, 40);
}

/// Scenario: the token is cancelled before writing.
/// Guarantees: write_block returns Cancelled and leaves no completed object.
#[tokio::test]
async fn cancellation_before_writing_aborts() {
    let dir = tempfile::tempdir().expect("tmp");
    let cfg = LakeConfig::default();
    let b = sealed_block(&cfg, 5);
    let sink = Sink::new(local(&dir), cfg, FileNaming::new("w"), tokio_timer);
    let token = CancellationToken::new();
    token.cancel();
    assert!(
        sink.write_block(&b, &token)
            .await
            .is_err_and(|e| e.is_cancelled())
    );
    assert_eq!(walkdir_count(dir.path()), 0);
}

/// Scenario: a write to an always-ready in-memory store while a task on the same runtime cancels.
/// Guarantees: the chunk loop yields, so the write stops as cancelled.
#[tokio::test]
async fn cancellation_is_observed_with_an_immediately_ready_store() {
    let mut cfg = LakeConfig::default();
    // One row per chunk, so the loop makes many passes over a small block.
    cfg.sorting.merge_chunk_bytes = 1;
    cfg.validate().expect("valid config");
    let b = sealed_block(&cfg, 200);
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let sink = Sink::new(store, cfg, FileNaming::new("w"), tokio_timer);
    let token = CancellationToken::new();
    let canceller = {
        let token = token.clone();
        tokio::spawn(async move { token.cancel() })
    };
    let got = sink.write_block(&b, &token).await;
    assert!(got.as_ref().is_err_and(Error::is_cancelled), "got {got:?}");
    canceller.await.expect("cancelling task");
}

/// Scenario: the store cancels the token as the values object opens, mid chunk loop.
/// Guarantees: the write stops with Cancelled after the series file and before any values object.
#[tokio::test]
async fn cancellation_at_a_chunk_boundary() {
    let dir = tempfile::tempdir().expect("tmp");
    let cfg = upload_config();
    let b = sealed_upload_block(&cfg);
    let token = CancellationToken::new();
    let tripped_multipart = Arc::new(AtomicBool::new(false));
    let store: Arc<dyn ObjectStore> = Arc::new(HookStore::new(
        local(&dir),
        CancelOnValues {
            token: token.clone(),
            tripped_multipart: tripped_multipart.clone(),
        },
    ));
    let sink = Sink::new(store, cfg, FileNaming::new("w"), tokio_timer);
    let got = sink.write_block(&b, &token).await;
    assert!(got.as_ref().is_err_and(Error::is_cancelled), "got {got:?}");
    assert!(
        tripped_multipart.load(Ordering::SeqCst),
        "the token must fire inside the chunk loop, not at finalization"
    );
    // The series file was finalized before the token fired, and a finalized
    // file is never unwritten.
    assert_eq!(parquet_count(dir.path(), "dataset=series"), 1);
    assert_eq!(parquet_count(dir.path(), "dataset=values"), 0);
}

/// Scenario: cancellation while a multipart part that never resolves is in flight.
/// Guarantees: the upload is aborted, `write_block` returns Cancelled, no values object completes.
#[tokio::test]
async fn cancellation_inside_an_upload() {
    let dir = tempfile::tempdir().expect("tmp");
    let entered = Arc::new(Notify::new());
    let aborted = Arc::new(AtomicBool::new(false));
    let parts = Arc::new(AtomicUsize::new(0));
    let store: Arc<dyn ObjectStore> = Arc::new(HookStore::new(
        local(&dir),
        ControlledMultipart {
            entered: entered.clone(),
            aborted: aborted.clone(),
            parts: parts.clone(),
            part: PartBehavior::Park,
            abort: AbortBehavior::Delegate,
        },
    ));
    let cfg = upload_config();
    let b = sealed_upload_block(&cfg);
    let sink = Sink::new(store, cfg, FileNaming::new("w"), tokio_timer);
    let token = CancellationToken::new();
    let canceller = token.clone();
    let waiter = entered.clone();
    let handle = tokio::spawn(async move {
        waiter.notified().await;
        canceller.cancel();
    });
    let got = sink.write_block(&b, &token).await;
    handle.await.expect("canceller");
    assert!(got.as_ref().is_err_and(Error::is_cancelled), "got {got:?}");
    assert!(
        parts.load(Ordering::SeqCst) > 0,
        "the writer must have handed a real part to the upload"
    );
    assert!(
        aborted.load(Ordering::SeqCst),
        "the in-flight multipart upload must be aborted"
    );
    assert_eq!(parquet_count(dir.path(), "dataset=values"), 0);
}

/// Scenario: cancellation while the values multipart upload is still being created.
/// Guarantees: the creation finishes within `upload.abort_timeout` and the upload is then aborted;
/// no values object completes.
#[tokio::test]
async fn a_cancellation_during_multipart_creation_still_aborts_the_upload() {
    let dir = tempfile::tempdir().expect("tmp");
    let aborted = Arc::new(AtomicBool::new(false));
    let token = CancellationToken::new();
    let store: Arc<dyn ObjectStore> = Arc::new(HookStore::new(
        Arc::new(HookStore::new(
            local(&dir),
            ControlledMultipart {
                entered: Arc::new(Notify::new()),
                aborted: aborted.clone(),
                parts: Arc::new(AtomicUsize::new(0)),
                part: PartBehavior::Fail,
                abort: AbortBehavior::Delegate,
            },
        )),
        CancelDuringCreate {
            token: token.clone(),
            stall: false,
        },
    ));
    let cfg = upload_config();
    let b = sealed_upload_block(&cfg);
    let sink = Sink::new(store, cfg, FileNaming::new("w"), tokio_timer);
    let got = sink.write_block(&b, &token).await;
    assert!(got.is_err(), "a cancelled write fails: {got:?}");
    assert!(
        aborted.load(Ordering::SeqCst),
        "the upload created after the cancellation must be aborted"
    );
    assert_eq!(parquet_count(dir.path(), "dataset=values"), 0);
}

/// Scenario: cancellation while `finish` is creating the values multipart
/// upload, once with a creation that then finishes and once with one that
/// never does.
/// Guarantees: a creation that finishes within `upload.abort_timeout` is
/// aborted and no values object completes; one that does not is reported
/// as a possible orphan on the cancellation, so it is counted as an abort
/// failure rather than a clean abort.
#[tokio::test]
async fn a_cancellation_while_finish_creates_the_upload_aborts_or_reports_it() {
    for stall in [false, true] {
        let dir = tempfile::tempdir().expect("tmp");
        let aborted = Arc::new(AtomicBool::new(false));
        let parts = Arc::new(AtomicUsize::new(0));
        let token = CancellationToken::new();
        let store: Arc<dyn ObjectStore> = Arc::new(HookStore::new(
            Arc::new(HookStore::new(
                local(&dir),
                ControlledMultipart {
                    entered: Arc::new(Notify::new()),
                    aborted: aborted.clone(),
                    parts: parts.clone(),
                    part: PartBehavior::Park,
                    abort: AbortBehavior::Delegate,
                },
            )),
            CancelDuringCreate {
                token: token.clone(),
                stall,
            },
        ));
        let cfg = finish_upload_config();
        let b = sealed_upload_block(&cfg);
        let timer = if stall { spent_timer } else { tokio_timer };
        let sink = Sink::new(store, cfg, FileNaming::new("w"), timer);
        let got = sink.write_block(&b, &token).await;
        let Err(Error::Transient(TransientError::Cancelled { abort_error })) = got else {
            panic!("expected a cancellation, got {got:?}");
        };
        assert_eq!(parquet_count(dir.path(), "dataset=values"), 0);
        if stall {
            let reason = abort_error.expect("a creation left in flight is a possible orphan");
            assert!(reason.contains("dataset=values"), "{reason}");
        } else {
            assert!(
                aborted.load(Ordering::SeqCst),
                "the upload created after the cancellation must be aborted"
            );
            assert_eq!(abort_error, None);
        }
    }
}

thread_local! {
    /// Cleanup timers [`counted_timer`] has started on this thread.
    static TIMERS_STARTED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// [`tokio_timer`] that also counts the timers started on this thread.
fn counted_timer(timeout: Duration) -> AbortTimer {
    TIMERS_STARTED.with(|started| started.set(started.get() + 1));
    tokio_timer(timeout)
}

/// Store hooks for a values upload whose completion never answers: parts
/// land at once, the completion cancels `token` and then hangs, and the abort
/// never returns.
#[derive(Debug)]
struct LostCompleteOnCancel {
    token: CancellationToken,
    /// When the completion cancelled the token.
    cancelled_at: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
}

/// The upload handed back by [`LostCompleteOnCancel`].
#[derive(Debug)]
struct CancellingUpload {
    token: CancellationToken,
    cancelled_at: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
}

#[async_trait::async_trait]
impl MultipartUpload for CancellingUpload {
    fn put_part(&mut self, _data: PutPayload) -> UploadPart {
        Box::pin(std::future::ready(Ok(())))
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        *self.cancelled_at.lock().expect("lock") = Some(std::time::Instant::now());
        self.token.cancel();
        std::future::pending().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        std::future::pending().await
    }
}

#[async_trait::async_trait]
impl StoreHooks for LostCompleteOnCancel {
    fn wrap_upload(
        &self,
        location: &Path,
        inner: Box<dyn MultipartUpload>,
    ) -> Box<dyn MultipartUpload> {
        if !location.as_ref().contains("dataset=values") {
            return inner;
        }
        Box::new(CancellingUpload {
            token: self.token.clone(),
            cancelled_at: Arc::clone(&self.cancelled_at),
        })
    }
}

/// Scenario: the token fires while `finish` waits for the values upload's
/// completion, which never answers, so the upload is settled by a HEAD and an
/// abort that never returns.
/// Guarantees: the cancellation's whole cleanup runs on the one cleanup timer
/// started where the cancellation was seen, ends within one
/// `upload.abort_timeout` of it, and reports the upload as a possible orphan.
#[tokio::test]
async fn a_cancelled_completion_settles_on_the_one_cleanup_budget() {
    let dir = tempfile::tempdir().expect("tmp");
    let token = CancellationToken::new();
    let cancelled_at = Arc::new(std::sync::Mutex::new(None));
    let mut cfg = finish_upload_config();
    cfg.upload.abort_timeout = Duration::from_millis(500);
    let store: Arc<dyn ObjectStore> = Arc::new(HookStore::new(
        local(&dir),
        LostCompleteOnCancel {
            token: token.clone(),
            cancelled_at: Arc::clone(&cancelled_at),
        },
    ));
    let b = sealed_upload_block(&cfg);
    let sink = Sink::new(store, cfg, FileNaming::new("w"), counted_timer);
    TIMERS_STARTED.with(|started| started.set(0));
    let got = sink.write_block(&b, &token).await;
    let since_cancel = cancelled_at
        .lock()
        .expect("lock")
        .expect("the completion was sent")
        .elapsed();
    let Err(Error::Transient(TransientError::Cancelled { abort_error })) = got else {
        panic!("expected a cancellation, got {got:?}");
    };
    assert!(
        abort_error.is_some_and(|reason| reason.contains("dataset=values")),
        "the unsettled upload is a possible orphan"
    );
    assert_eq!(
        TIMERS_STARTED.with(std::cell::Cell::get),
        1,
        "one cleanup budget"
    );
    assert!(
        since_cancel < Duration::from_millis(900),
        "cleanup took {since_cancel:?} after the cancellation"
    );
}

/// Scenario: a part upload fails and its abort fails too.
/// Guarantees: `AbortFailed` carries both errors and no values object completes.
#[tokio::test]
async fn write_failure_with_a_failing_abort_reports_both() {
    let dir = tempfile::tempdir().expect("tmp");
    let aborted = Arc::new(AtomicBool::new(false));
    let parts = Arc::new(AtomicUsize::new(0));
    let store: Arc<dyn ObjectStore> = Arc::new(HookStore::new(
        local(&dir),
        ControlledMultipart {
            entered: Arc::new(Notify::new()),
            aborted: aborted.clone(),
            parts: parts.clone(),
            part: PartBehavior::Fail,
            abort: AbortBehavior::Fail,
        },
    ));
    let cfg = upload_config();
    let b = sealed_upload_block(&cfg);
    let sink = Sink::new(store, cfg, FileNaming::new("w"), tokio_timer);
    let got = sink.write_block(&b, &CancellationToken::new()).await;
    let Err(Error::Transient(TransientError::AbortFailed {
        source,
        abort_error,
    })) = got
    else {
        panic!("expected AbortFailed, got {got:?}");
    };
    // The failure that triggered the cleanup is preserved, not replaced.
    assert!(
        source.to_string().contains("injected failure"),
        "original failure lost: {source}"
    );
    assert!(
        abort_error.contains("injected failure"),
        "abort failure lost: {abort_error}"
    );
    assert!(parts.load(Ordering::SeqCst) > 0);
    assert!(aborted.load(Ordering::SeqCst));
    assert_eq!(parquet_count(dir.path(), "dataset=values"), 0);
}

/// Scenario: a hung abort with an hour of `upload.abort_timeout` and a timer that fires at once.
/// Guarantees: the abort ends with the caller's timer.
#[tokio::test]
async fn the_abort_is_bounded_by_the_callers_timer() {
    let dir = tempfile::tempdir().expect("tmp");
    let store: Arc<dyn ObjectStore> = Arc::new(HookStore::new(
        local(&dir),
        ControlledMultipart {
            entered: Arc::new(Notify::new()),
            aborted: Arc::new(AtomicBool::new(false)),
            parts: Arc::new(AtomicUsize::new(0)),
            part: PartBehavior::Fail,
            abort: AbortBehavior::Hang,
        },
    ));
    let mut cfg = upload_config();
    cfg.upload.abort_timeout = Duration::from_secs(3600);
    let b = sealed_upload_block(&cfg);
    let sink = Sink::new(store, cfg, FileNaming::new("w"), |_| {
        Box::pin(std::future::ready(()))
    });
    let got = tokio::time::timeout(
        Duration::from_secs(30),
        sink.write_block(&b, &CancellationToken::new()),
    )
    .await
    .expect("the abort is bounded by the caller's timer, not by tokio time");
    let Err(Error::Transient(TransientError::AbortFailed { abort_error, .. })) = got else {
        panic!("expected AbortFailed, got {got:?}");
    };
    assert!(abort_error.contains("timed out"), "{abort_error}");
}

/// Scenario: a hung abort with a one-millisecond `upload.abort_timeout`.
/// Guarantees: the cleanup ends and the abort error names the timeout.
#[tokio::test]
async fn abort_timeout_is_reported() {
    let dir = tempfile::tempdir().expect("tmp");
    let store: Arc<dyn ObjectStore> = Arc::new(HookStore::new(
        local(&dir),
        ControlledMultipart {
            entered: Arc::new(Notify::new()),
            aborted: Arc::new(AtomicBool::new(false)),
            parts: Arc::new(AtomicUsize::new(0)),
            part: PartBehavior::Fail,
            abort: AbortBehavior::Hang,
        },
    ));
    let mut cfg = upload_config();
    cfg.upload.abort_timeout = Duration::from_millis(1);
    cfg.validate().expect("valid config");
    let b = sealed_upload_block(&cfg);
    let sink = Sink::new(store, cfg, FileNaming::new("w"), tokio_timer);
    let got = sink.write_block(&b, &CancellationToken::new()).await;
    let Err(Error::Transient(TransientError::AbortFailed { abort_error, .. })) = got else {
        panic!("expected AbortFailed, got {got:?}");
    };
    assert!(
        abort_error.contains("timed out"),
        "expected a timeout reason, got {abort_error}"
    );
    assert_eq!(parquet_count(dir.path(), "dataset=values"), 0);
}

/// A store whose multipart parts all fail, with more upload slots than the
/// file has parts, so no failure surfaces before `finish` joins the parts.
fn parts_failing_at_finish(
    dir: &tempfile::TempDir,
    aborted: &Arc<AtomicBool>,
    abort: AbortBehavior,
) -> (Arc<dyn ObjectStore>, LakeConfig) {
    let store: Arc<dyn ObjectStore> = Arc::new(HookStore::new(
        local(dir),
        ControlledMultipart {
            entered: Arc::new(Notify::new()),
            aborted: aborted.clone(),
            parts: Arc::new(AtomicUsize::new(0)),
            part: PartBehavior::Fail,
            abort,
        },
    ));
    let mut cfg = upload_config();
    cfg.upload.concurrency = 64;
    cfg.validate().expect("valid config");
    (store, cfg)
}

/// Scenario: every part of the values upload fails, and the failure first
/// surfaces while the writer is finishing the file.
/// Guarantees: the multipart upload is aborted and the part's retryable
/// failure is returned; no values object completes.
#[tokio::test]
async fn a_part_failing_while_finishing_aborts_the_upload() {
    let dir = tempfile::tempdir().expect("tmp");
    let aborted = Arc::new(AtomicBool::new(false));
    let (store, cfg) = parts_failing_at_finish(&dir, &aborted, AbortBehavior::Delegate);
    let b = sealed_upload_block(&cfg);
    let sink = Sink::new(store, cfg, FileNaming::new("w"), tokio_timer);
    let got = sink.write_block(&b, &CancellationToken::new()).await;
    let error = got.expect_err("a failed part fails the write");
    assert!(
        aborted.load(Ordering::SeqCst),
        "the upload a failed finish gives up must be aborted"
    );
    assert!(
        !matches!(error, Error::Transient(TransientError::AbortFailed { .. })),
        "{error:?}"
    );
    assert!(error.is_retryable(), "{error:?}");
    assert_eq!(parquet_count(dir.path(), "dataset=values"), 0);
}

/// Scenario: a part fails while the writer is finishing the file, and the
/// abort of the upload fails too.
/// Guarantees: `AbortFailed` names the object key and both failures, so the
/// possibly orphaned upload is reported.
#[tokio::test]
async fn a_failed_abort_while_finishing_is_reported_with_its_key() {
    let dir = tempfile::tempdir().expect("tmp");
    let aborted = Arc::new(AtomicBool::new(false));
    let (store, cfg) = parts_failing_at_finish(&dir, &aborted, AbortBehavior::Fail);
    let b = sealed_upload_block(&cfg);
    let sink = Sink::new(store, cfg, FileNaming::new("w"), tokio_timer);
    let got = sink.write_block(&b, &CancellationToken::new()).await;
    let Err(Error::Transient(TransientError::AbortFailed {
        source,
        abort_error,
    })) = got
    else {
        panic!("expected AbortFailed, got {got:?}");
    };
    assert!(source.is_retryable(), "{source:?}");
    assert!(abort_error.contains("injected failure"), "{abort_error}");
    assert!(abort_error.contains("dataset=values"), "{abort_error}");
    assert!(aborted.load(Ordering::SeqCst));
}

/// Store hooks whose multipart creation fails with `error`, as the inner
/// store of the sink's own watch.
#[derive(Debug)]
struct FailingCreate {
    error: fn() -> object_store::Error,
}

#[async_trait::async_trait]
impl StoreHooks for FailingCreate {
    async fn before_multipart(&self, location: &Path) -> object_store::Result<Option<HookGuard>> {
        if location.as_ref().contains("dataset=values") {
            return Err((self.error)());
        }
        Ok(None)
    }
}

/// Scenario: CreateMultipartUpload of the values object fails without a
/// definite rejection, and then with a definite one (access denied).
/// Guarantees: only the first is reported as a possibly orphaned upload,
/// named by its key; both keep the creation's own failure as the cause.
#[tokio::test]
async fn a_creation_without_a_definite_answer_is_a_possible_orphan() {
    let cfg = upload_config();
    let b = sealed_upload_block(&cfg);
    for (error, orphan) in [
        (injected as fn() -> object_store::Error, true),
        (
            || object_store::Error::PermissionDenied {
                path: "p".into(),
                source: "denied".into(),
            },
            false,
        ),
    ] {
        let dir = tempfile::tempdir().expect("tmp");
        let store: Arc<dyn ObjectStore> =
            Arc::new(HookStore::new(local(&dir), FailingCreate { error }));
        let sink = Sink::new(store, cfg.clone(), FileNaming::new("w"), tokio_timer);
        let got = sink.write_block(&b, &CancellationToken::new()).await;
        match got {
            Err(Error::Transient(TransientError::AbortFailed {
                source,
                abort_error,
            })) if orphan => {
                assert!(
                    abort_error.contains("CreateMultipartUpload"),
                    "{abort_error}"
                );
                assert!(abort_error.contains("dataset=values"), "{abort_error}");
                assert!(source.to_string().contains("injected failure"), "{source}");
            }
            Err(error) if !orphan => assert!(
                !matches!(error, Error::Transient(TransientError::AbortFailed { .. })),
                "{error:?}"
            ),
            other => panic!("orphan={orphan}: unexpected {other:?}"),
        }
    }
}

/// Store hooks whose multipart completion of the values object fails without
/// being applied, the way a completion whose request is lost looks to the
/// writer; `abort` says how the abort that follows behaves.
#[derive(Debug)]
struct LostCompletion {
    aborted: Arc<AtomicBool>,
    abort: AbortBehavior,
}

/// The upload handed back by [`LostCompletion`].
#[derive(Debug)]
struct LostCompletionUpload {
    inner: Box<dyn MultipartUpload>,
    aborted: Arc<AtomicBool>,
    abort: AbortBehavior,
}

#[async_trait::async_trait]
impl MultipartUpload for LostCompletionUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        Err(injected())
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.aborted.store(true, Ordering::SeqCst);
        match self.abort {
            AbortBehavior::Delegate => self.inner.abort().await,
            AbortBehavior::Fail => {
                let _ = self.inner.abort().await;
                Err(injected())
            }
            AbortBehavior::Hang => std::future::pending().await,
        }
    }
}

#[async_trait::async_trait]
impl StoreHooks for LostCompletion {
    fn wrap_upload(
        &self,
        location: &Path,
        inner: Box<dyn MultipartUpload>,
    ) -> Box<dyn MultipartUpload> {
        if !location.as_ref().contains("dataset=values") {
            return inner;
        }
        Box::new(LostCompletionUpload {
            inner,
            aborted: self.aborted.clone(),
            abort: self.abort,
        })
    }
}

/// Scenario: a block is written once, then written again under the same
/// frozen names while the retry's values completion is lost and the object
/// from the first write is still in the store.
/// Guarantees: the found object counts as written but not as a commit of this
/// completion, and the retry's own upload is aborted.
#[tokio::test]
async fn an_earlier_attempts_object_does_not_settle_the_current_upload() {
    let dir = tempfile::tempdir().expect("tmp");
    let cfg = upload_config();
    let b = sealed_upload_block(&cfg);
    let first = Sink::new(local(&dir), cfg.clone(), naming("w", "boot"), tokio_timer);
    let _ = first
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("the first write succeeds");

    let aborted = Arc::new(AtomicBool::new(false));
    let store: Arc<dyn ObjectStore> = Arc::new(HookStore::new(
        local(&dir),
        LostCompletion {
            aborted: aborted.clone(),
            abort: AbortBehavior::Delegate,
        },
    ));
    let retry = Sink::new(store, cfg, naming("w", "boot"), tokio_timer);
    let report = retry
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("the object exists, so the table is written");
    assert!(
        aborted.load(Ordering::SeqCst),
        "the retry's own upload must be aborted"
    );
    assert_eq!(
        report.probed_commits, 0,
        "an object an earlier attempt wrote is not this completion's commit"
    );
}

/// Scenario: as above, but the abort of the retry's own upload fails.
/// Guarantees: the table still counts as written, and the upload is reported
/// as a possible orphan naming its key.
#[tokio::test]
async fn a_failed_abort_after_a_found_object_is_a_possible_orphan() {
    let dir = tempfile::tempdir().expect("tmp");
    let cfg = upload_config();
    let b = sealed_upload_block(&cfg);
    let first = Sink::new(local(&dir), cfg.clone(), naming("w", "boot"), tokio_timer);
    let _ = first
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("the first write succeeds");

    let store: Arc<dyn ObjectStore> = Arc::new(HookStore::new(
        local(&dir),
        LostCompletion {
            aborted: Arc::new(AtomicBool::new(false)),
            abort: AbortBehavior::Fail,
        },
    ));
    let retry = Sink::new(store, cfg, naming("w", "boot"), tokio_timer);
    let report = retry
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("the object exists, so the table is written");
    assert_eq!(report.probed_commits, 0);
    assert_eq!(report.possible_orphans.len(), 1, "{report:?}");
    assert!(
        report.possible_orphans[0].contains("dataset=values"),
        "{report:?}"
    );
}

/// Store hooks whose values completion is applied but answers an error, the
/// way a completion whose response is lost looks to the writer; the abort of
/// the completed upload then succeeds, as MinIO answers it (204), or fails
/// with an error other than `NotFound` when the field is true.
#[derive(Debug)]
struct AppliedCompletion(bool);

/// The upload handed back by [`AppliedCompletion`], and whether
/// its abort fails.
#[derive(Debug)]
struct AppliedCompletionUpload(Box<dyn MultipartUpload>, bool);

#[async_trait::async_trait]
impl MultipartUpload for AppliedCompletionUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.0.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let _ = self.0.complete().await?;
        Err(injected())
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        if self.1 {
            return Err(injected());
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl StoreHooks for AppliedCompletion {
    fn wrap_upload(
        &self,
        location: &Path,
        inner: Box<dyn MultipartUpload>,
    ) -> Box<dyn MultipartUpload> {
        if !location.as_ref().contains("dataset=values") {
            return inner;
        }
        Box::new(AppliedCompletionUpload(inner, self.0))
    }
}

/// Scenario: on a block's first write attempt the values completion is
/// applied, its response is lost, the probe finds the object and the store
/// accepts the abort of the completed upload (MinIO).
/// Guarantees: the object is this completion's commit (nothing else can have
/// written a first attempt's frozen name), counted in `probed_commits`, and
/// no orphan is reported. A later attempt keeps the earlier-attempt reading
/// (`an_earlier_attempts_object_does_not_settle_the_current_upload`).
#[tokio::test]
async fn a_first_attempts_found_object_is_its_commit_when_the_abort_succeeds() {
    let dir = tempfile::tempdir().expect("tmp");
    let cfg = upload_config();
    let b = sealed_upload_block(&cfg);
    let store: Arc<dyn ObjectStore> =
        Arc::new(HookStore::new(local(&dir), AppliedCompletion(false)));
    let sink = Sink::new(store, cfg, naming("w", "boot"), tokio_timer);
    let report = sink
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("the object exists, so the table is written");
    assert_eq!(
        report.probed_commits, 1,
        "a first attempt's found object is its own commit"
    );
    assert!(report.possible_orphans.is_empty(), "{report:?}");
}

/// Scenario: as above, but the abort of the completed upload fails with an
/// error other than `NotFound` (a 5xx or a timeout).
/// Guarantees: the found object is still this completion's commit, counted in
/// `probed_commits`, and no orphan is reported, since the completed upload
/// leaves nothing to abort.
#[tokio::test]
async fn a_first_attempts_found_object_is_its_commit_when_the_abort_fails() {
    let dir = tempfile::tempdir().expect("tmp");
    let cfg = upload_config();
    let b = sealed_upload_block(&cfg);
    let store: Arc<dyn ObjectStore> =
        Arc::new(HookStore::new(local(&dir), AppliedCompletion(true)));
    let sink = Sink::new(store, cfg, naming("w", "boot"), tokio_timer);
    let report = sink
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("the object exists, so the table is written");
    assert_eq!(
        report.probed_commits, 1,
        "a first attempt's found object is its own commit"
    );
    assert!(report.possible_orphans.is_empty(), "{report:?}");
}

/// An object store whose HEAD requests fail with a retryable error, the way
/// a store answers during an outage, delegating everything else.
#[derive(Debug)]
struct FailingHead(Arc<dyn ObjectStore>);

impl std::fmt::Display for FailingHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[async_trait::async_trait]
impl ObjectStore for FailingHead {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<PutResult> {
        self.0.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if options.head {
            return Err(injected());
        }
        self.0.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.0.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.0.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.0.copy_opts(from, to, options).await
    }
}

/// Scenario: a values completion is lost and the HEAD that should tell
/// whether the store applied it fails with a retryable error.
/// Guarantees: the upload is not aborted, since the completion may still be
/// applied; the completion's retryable error is kept and the upload is
/// reported as a possible orphan naming its key.
#[tokio::test]
async fn an_indeterminate_probe_does_not_abort_the_upload() {
    let dir = tempfile::tempdir().expect("tmp");
    let aborted = Arc::new(AtomicBool::new(false));
    let store: Arc<dyn ObjectStore> = Arc::new(FailingHead(Arc::new(HookStore::new(
        local(&dir),
        LostCompletion {
            aborted: aborted.clone(),
            abort: AbortBehavior::Delegate,
        },
    ))));
    let cfg = upload_config();
    let b = sealed_upload_block(&cfg);
    let sink = Sink::new(store, cfg, FileNaming::new("w"), tokio_timer);
    let got = sink.write_block(&b, &CancellationToken::new()).await;
    assert!(
        !aborted.load(Ordering::SeqCst),
        "an indeterminate probe must not abort the upload"
    );
    let Err(Error::Transient(TransientError::AbortFailed {
        source,
        abort_error,
    })) = got
    else {
        panic!("expected AbortFailed, got {got:?}");
    };
    assert!(source.is_retryable(), "{source:?}");
    assert!(abort_error.contains("dataset=values"), "{abort_error}");
    assert!(abort_error.contains("HEAD"), "{abort_error}");
}

/// Store hooks whose multipart uploads land every full part and park the
/// short final part, which only the writer's finish sends.
#[derive(Debug)]
struct ParkFinalPart {
    entered: Arc<Notify>,
    aborted: Arc<AtomicBool>,
    part_bytes: usize,
}

/// The upload handed back by [`ParkFinalPart`].
#[derive(Debug)]
struct ParkFinalPartUpload {
    inner: Box<dyn MultipartUpload>,
    entered: Arc<Notify>,
    aborted: Arc<AtomicBool>,
    part_bytes: usize,
}

#[async_trait::async_trait]
impl MultipartUpload for ParkFinalPartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        if data.content_length() < self.part_bytes {
            self.entered.notify_one();
            return Box::pin(std::future::pending());
        }
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.aborted.store(true, Ordering::SeqCst);
        self.inner.abort().await
    }
}

#[async_trait::async_trait]
impl StoreHooks for ParkFinalPart {
    fn wrap_upload(
        &self,
        _location: &Path,
        inner: Box<dyn MultipartUpload>,
    ) -> Box<dyn MultipartUpload> {
        Box::new(ParkFinalPartUpload {
            inner,
            entered: self.entered.clone(),
            aborted: self.aborted.clone(),
            part_bytes: self.part_bytes,
        })
    }
}

/// Scenario: the write is cancelled while the writer's finish waits for the
/// final part of the values upload.
/// Guarantees: the upload the cancelled finish let go of is aborted, the
/// write reports a cancellation with no abort failure, and no values object
/// completes.
#[tokio::test]
async fn a_cancellation_while_finishing_aborts_the_upload() {
    let dir = tempfile::tempdir().expect("tmp");
    let entered = Arc::new(Notify::new());
    let aborted = Arc::new(AtomicBool::new(false));
    let cfg = upload_config();
    let store: Arc<dyn ObjectStore> = Arc::new(HookStore::new(
        local(&dir),
        ParkFinalPart {
            entered: entered.clone(),
            aborted: aborted.clone(),
            part_bytes: cfg.upload.part_bytes,
        },
    ));
    let b = sealed_upload_block(&cfg);
    let sink = Sink::new(store, cfg, FileNaming::new("w"), tokio_timer);
    let token = CancellationToken::new();
    let canceller = {
        let token = token.clone();
        async move {
            entered.notified().await;
            token.cancel();
        }
    };
    let (got, ()) = tokio::join!(sink.write_block(&b, &token), canceller);
    let Err(Error::Transient(TransientError::Cancelled { abort_error })) = got else {
        panic!("expected a cancellation, got {got:?}");
    };
    assert_eq!(abort_error, None);
    assert!(
        aborted.load(Ordering::SeqCst),
        "the upload a cancelled finish lets go of must be aborted"
    );
    assert_eq!(parquet_count(dir.path(), "dataset=values"), 0);
}

/// A sealed block of `n` log rows whose one series is already committed
/// in the block's partition, so the block's only table is its values.
fn values_only_block(cfg: &LakeConfig, n: usize) -> Block {
    let mut cache = SeriesCache::new(10);
    let mut records = encode_logs(&logs(n, 8));
    let e = extract(&mut records, cfg).expect("extract");
    let partition = PartitionId::from_unix_secs(WINDOW_START);
    for descriptor in &e.descriptors {
        cache.mark_committed(descriptor.series_id, partition);
    }
    let mut b = Block::new(WINDOW_START, SEQ, cfg.clone());
    let r = b.reserve(&e, &mut cache, 8).expect("reserve");
    b.admit(e, r).expect("admit");
    b.seal(SEAL_AT_US).expect("seal");
    assert_eq!(
        b.tables().filter(|table| !table.is_empty()).count(),
        1,
        "only the values table has rows"
    );
    b
}

/// Scenario: a 30,000-row chunk written to an always-ready store while a ticker counts its turns.
/// Guarantees: the ticker runs at least once per pop slice and once per output column.
#[tokio::test]
async fn the_write_returns_to_the_runtime_between_bounded_slices() {
    let cfg = LakeConfig::default();
    let rows = 30_000;
    let b = sealed_block(&cfg, rows);
    let columns = dataset_schema(Dataset::LogsValues, &cfg).fields().len();
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let sink = Sink::new(store, cfg, FileNaming::new("w"), tokio_timer);
    let done = Arc::new(AtomicBool::new(false));
    let ticks = Arc::new(AtomicUsize::new(0));
    let ticker = {
        let done = Arc::clone(&done);
        let ticks = Arc::clone(&ticks);
        tokio::spawn(async move {
            while !done.load(Ordering::SeqCst) {
                let _ = ticks.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
            }
        })
    };
    let report = sink
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("write");
    done.store(true, Ordering::SeqCst);
    ticker.await.expect("ticker");
    assert_eq!(report.files.iter().map(|f| f.2).sum::<usize>(), rows + 1);
    let pop_slices = rows.div_ceil(crate::sort::MERGE_STEP_ROWS);
    let ticked = ticks.load(Ordering::SeqCst);
    assert!(
        ticked > pop_slices + columns,
        "the runtime scheduled the ticker {ticked} times during the write, fewer than \
         {pop_slices} pop slices and {columns} columns"
    );
}

/// Scenario: a 100,000-row values block cancelled at the write's first yield.
/// Guarantees: the write returns Cancelled having encoded only a fraction of the keys.
#[tokio::test]
async fn a_cancellation_during_merge_key_building_stops_the_build() {
    let mut cfg = LakeConfig::default();
    cfg.ingress.max_request_bytes = 64 << 20;
    cfg.ingress.max_extracted_bytes = 64 << 20;
    cfg.validate().expect("valid config");
    let b = values_only_block(&cfg, 100_000);
    let full = b
        .tables()
        .filter(|table| !table.is_empty())
        .map(|table| {
            merge_runs(
                table.iter_snapshots().cloned().collect(),
                table.spec(),
                cfg.sorting.merge_chunk_bytes,
            )
            .expect("merge")
            .resident_key_bytes()
        })
        .sum::<usize>();
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let sink = Sink::new(store, cfg, FileNaming::new("w"), tokio_timer);
    let token = CancellationToken::new();
    let canceller = {
        let token = token.clone();
        tokio::spawn(async move { token.cancel() })
    };
    let got = sink.write_block(&b, &token).await;
    canceller.await.expect("cancelling task");
    assert!(got.as_ref().is_err_and(Error::is_cancelled), "got {got:?}");
    let built = sink.merge_key_high_water_bytes();
    assert!(
        built < full / 2,
        "{built} of {full} merge-key bytes were encoded before the cancellation was seen"
    );
}

/// Scenario: three 10-byte buffers cut into 8, 8, 8 and 6-byte parts landing out of order, then a
/// put.
/// Guarantees: each buffer is released once all its bytes landed; the put releases everything.
#[test]
fn the_upload_ledger_releases_each_buffer_when_its_bytes_land() {
    let ledger = UploadLedger::default();
    for _ in 0..3 {
        ledger.handed(10);
    }
    assert_eq!(ledger.live(), 30);
    let first = ledger.part_started(8);
    let second = ledger.part_started(8);
    let third = ledger.part_started(8);
    ledger.part_landed(second);
    assert_eq!(
        ledger.live(),
        30,
        "bytes 8..16 landed: no buffer is whole yet"
    );
    ledger.part_landed(third);
    assert_eq!(
        ledger.live(),
        20,
        "bytes 10..20 have all landed although part 0..8 is still in flight"
    );
    ledger.part_landed(first);
    assert_eq!(ledger.live(), 10, "bytes 0..10 landed");
    let last = ledger.part_started(6);
    ledger.part_landed(last);
    assert_eq!(ledger.live(), 0);
    ledger.handed(7);
    assert_eq!(ledger.live(), 7);
    ledger.put_landed();
    assert_eq!(ledger.live(), 0);
}

/// Scenario: the same runs merged sorted and unsorted, first chunks charged as workspace.
/// Guarantees: the unsorted chunk (a block run) charges nothing; the sorted one its pinned bytes.
#[test]
fn only_a_chunk_the_merge_allocated_is_charged() {
    let cfg = LakeConfig::default();
    let b = sealed_block(&cfg, 200);
    let table = b
        .tables()
        .find(|table| !table.dataset().is_series() && !table.is_empty())
        .expect("values");
    let runs: Vec<RecordBatch> = table.iter_snapshots().cloned().collect();
    for (spec, owned) in [(table.spec().clone(), true), (SortSpec::new(vec![]), false)] {
        let mut merge = merge_runs(runs.clone(), &spec, 1 << 20).expect("merge");
        let chunk = merge.next().expect("a chunk").expect("chunk");
        let pinned = record_batch_pinned_bytes(&chunk, &mut CountedAllocations::default());
        let charged = chunk_charge(&merge, &chunk);
        if owned {
            assert!(charged >= pinned, "{charged} charged for {pinned} pinned");
        } else {
            assert_eq!(charged, 0, "the run belongs to the block");
        }
    }
}

/// Store hooks whose multipart parts wait for a permit before they
/// are uploaded, announcing each part as it is handed over.
#[derive(Debug)]
struct GatedParts {
    gate: Arc<tokio::sync::Semaphore>,
    entered: Arc<Notify>,
}

#[derive(Debug)]
struct GatedUpload {
    inner: Box<dyn MultipartUpload>,
    gate: Arc<tokio::sync::Semaphore>,
    entered: Arc<Notify>,
}

#[async_trait::async_trait]
impl MultipartUpload for GatedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let part = self.inner.put_part(data);
        let gate = Arc::clone(&self.gate);
        self.entered.notify_one();
        Box::pin(async move {
            let permit = gate.acquire().await.expect("the gate is never closed");
            drop(permit);
            part.await
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

#[async_trait::async_trait]
impl StoreHooks for GatedParts {
    fn wrap_upload(
        &self,
        _location: &Path,
        inner: Box<dyn MultipartUpload>,
    ) -> Box<dyn MultipartUpload> {
        Box::new(GatedUpload {
            inner,
            gate: Arc::clone(&self.gate),
            entered: Arc::clone(&self.entered),
        })
    }
}

/// Scenario: a values file over one 5 MiB part, the first part held by the store.
/// Guarantees: the workspace reads at least 5 MiB while held and zero after, with the peak kept.
#[tokio::test]
async fn the_sink_publishes_the_upload_bytes_a_part_in_flight_holds() {
    let dir = tempfile::tempdir().expect("tmp");
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let entered = Arc::new(Notify::new());
    let store: Arc<dyn ObjectStore> = Arc::new(HookStore::new(
        local(&dir),
        GatedParts {
            gate: Arc::clone(&gate),
            entered: Arc::clone(&entered),
        },
    ));
    let cfg = upload_config();
    let b = sealed_upload_block(&cfg);
    let sink = Sink::new(store, cfg.clone(), FileNaming::new("w"), tokio_timer);
    assert_eq!(sink.flush_workspace_bytes(), 0);
    let token = CancellationToken::new();
    let write = sink.write_block(&b, &token);
    let observe = async {
        entered.notified().await;
        let held = sink.flush_workspace_bytes();
        gate.add_permits(1 << 20);
        held
    };
    let (got, held) = tokio::join!(write, observe);
    let _ = got.expect("write");
    assert!(
        held >= cfg.upload.part_bytes,
        "{held} workspace bytes while a {} byte part was in flight",
        cfg.upload.part_bytes
    );
    assert_eq!(sink.flush_workspace_bytes(), 0, "no table is being written");
    assert!(sink.flush_workspace_high_water_bytes() >= held);
}

/// Scenario: a block whose values file takes several multipart parts, written with small row
/// groups and merge chunks.
/// Guarantees: the most flush workspace the sink measured is within
/// `LakeConfig::worst_case_flush_bytes`, the allowance a block's owner reserves for it.
#[tokio::test]
async fn the_measured_flush_workspace_stays_within_the_worst_case() {
    let dir = tempfile::tempdir().expect("tmp");
    let cfg = upload_config();
    let b = sealed_upload_block(&cfg);
    let sink = Sink::new(local(&dir), cfg.clone(), naming("w", "boot"), tokio_timer);
    let _ = sink
        .write_block(&b, &CancellationToken::new())
        .await
        .expect("write");
    let measured = sink.flush_workspace_high_water_bytes() as u64;
    assert!(measured > 0, "the write held a workspace");
    assert!(
        measured <= cfg.worst_case_flush_bytes(),
        "{measured} measured bytes above the {} byte allowance",
        cfg.worst_case_flush_bytes()
    );
}

/// Scenario: the flush allowance of the default configuration.
/// Guarantees: it is the sum of the documented terms, 2 * 8MiB + 2 * 16MiB + 3 * 96MiB +
/// (8MiB * 3 + 16MiB).
#[test]
fn the_default_worst_case_flush_is_the_documented_sum() {
    let mib = 1_u64 << 20;
    assert_eq!(
        LakeConfig::default().worst_case_flush_bytes(),
        2 * 8 * mib + 2 * 16 * mib + 3 * 96 * mib + (8 * mib * 3 + 16 * mib)
    );
}

/// Scenario: a writer step with an instantly ready operation and an already fired token.
/// Guarantees: the step reports Cancelled without polling the operation.
#[tokio::test]
async fn a_step_whose_token_has_fired_is_not_driven_again() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let sink = Sink::new(
        Arc::clone(&store),
        LakeConfig::default(),
        FileNaming::new("w"),
        tokio_timer,
    );
    let watch = creation_watch(store, Arc::new(UploadLedger::default()));
    let cancel = CancellationToken::new();
    cancel.cancel();
    let polled = std::cell::Cell::new(0);
    let op = std::future::poll_fn(|_| {
        polled.set(polled.get() + 1);
        std::task::Poll::Ready(Ok(()))
    });
    let mut cleanup = None;
    let got = sink.step(op, &watch, &cancel, &mut cleanup).await;
    assert!(got.as_ref().is_err_and(Error::is_cancelled), "got {got:?}");
    assert_eq!(
        polled.get(),
        0,
        "the operation was polled after the cancellation"
    );
}

/// Scenario: every leaf of every dataset schema and the writer properties.
/// Guarantees: high-entropy columns are real leaves without dictionary or page statistics; the sort
/// keys and `metric_name` keep both.
#[test]
fn high_entropy_columns_are_real_leaves_and_the_sort_keys_keep_page_statistics() {
    use parquet::file::properties::EnabledStatistics;
    let cfg = LakeConfig::default();
    let leaves: Vec<String> = Dataset::ALL
        .iter()
        .flat_map(|&ds| {
            let schema = dataset_schema(ds, &cfg);
            parquet::arrow::ArrowSchemaConverter::new()
                .convert(&schema)
                .expect("parquet schema")
                .columns()
                .iter()
                .map(|c| c.path().string())
                .collect::<Vec<_>>()
        })
        .collect();
    let props = writer_properties(compression()).build();
    for column in HIGH_ENTROPY_COLUMNS {
        assert!(
            leaves.iter().any(|leaf| leaf == column),
            "{column} is no leaf of any dataset"
        );
        let path = leaf_path(column);
        assert!(
            !props.dictionary_enabled(&path),
            "{column} keeps a dictionary"
        );
        assert_eq!(
            props.statistics_enabled(&path),
            EnabledStatistics::Chunk,
            "{column}"
        );
    }
    for column in [
        "series_id",
        "time_unix_nano",
        "metric_name",
        "attrs.entries.keys",
    ] {
        assert!(
            leaves.iter().any(|leaf| leaf == column),
            "{column} is no leaf"
        );
        let path = leaf_path(column);
        assert!(props.dictionary_enabled(&path), "{column}");
        assert_eq!(
            props.statistics_enabled(&path),
            EnabledStatistics::Page,
            "{column}"
        );
    }
}

/// Scenario: a logs block whose records carry a body, a trace and span id and
/// an attribute is written, and the column chunks of every file are read back.
/// Guarantees: each of the `HIGH_ENTROPY_COLUMNS`, the nested
/// `attrs.entries.values` included, is written without a dictionary page or
/// dictionary encoding and without a column index, while other string
/// leaves, nested ones included, keep both and the sort keys keep the index.
#[tokio::test]
async fn high_entropy_leaves_are_written_without_a_dictionary() {
    use parquet::basic::Encoding;
    let dir = tempfile::tempdir().expect("tmp");
    let cfg = LakeConfig::default();
    let mut data = logs(30, 8);
    for (i, record) in data.resource_logs[0].scope_logs[0]
        .log_records
        .iter_mut()
        .enumerate()
    {
        record.trace_id = vec![i as u8 + 1; 16];
        record.span_id = vec![i as u8 + 1; 8];
        record.attributes = vec![kv("request.id", &body(i, 12))];
    }
    let mut block = Block::new(WINDOW_START, SEQ, cfg.clone());
    let mut cache = SeriesCache::new(10);
    let mut records = encode_logs(&data);
    let e = extract(&mut records, &cfg).expect("extract");
    let r = block.reserve(&e, &mut cache, 8).expect("reserve");
    block.admit(e, r).expect("admit");
    block.seal(SEAL_AT_US).expect("seal");
    let sink = Sink::new(local(&dir), cfg, naming("w", "dict"), tokio_timer);
    let report = sink
        .write_block(&block, &CancellationToken::new())
        .await
        .expect("write");

    // Leaf path -> (dictionary used, column index written), over every file.
    let mut leaves = std::collections::BTreeMap::new();
    for (_, path, _) in &report.files {
        let file = std::fs::File::open(dir.path().join(path.as_ref())).expect("open");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).expect("reader");
        for rg in reader.metadata().row_groups() {
            for chunk in rg.columns() {
                let dictionary = chunk.dictionary_page_offset().is_some()
                    || chunk.encodings().any(|encoding| {
                        matches!(
                            encoding,
                            Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY
                        )
                    });
                let entry = leaves
                    .entry(chunk.column_path().string())
                    .or_insert((false, false));
                entry.0 |= dictionary;
                entry.1 |= chunk.column_index_offset().is_some();
            }
        }
    }
    for column in HIGH_ENTROPY_COLUMNS {
        let &(dictionary, index) = leaves
            .get(column)
            .unwrap_or_else(|| panic!("{column} is in no written file: {leaves:?}"));
        assert!(!dictionary, "{column} is dictionary-encoded");
        assert!(!index, "{column} carries page statistics");
    }
    for column in [
        "time_unix_nano",
        "attrs.entries.keys",
        "resource_attrs.entries.values",
    ] {
        assert_eq!(leaves.get(column), Some(&(true, true)), "{column}");
    }
    // FIXED_LEN_BYTE_ARRAY takes no dictionary in the Parquet 1.0 writer.
    assert_eq!(leaves.get("series_id"), Some(&(false, true)));
}
