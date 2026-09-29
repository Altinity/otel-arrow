// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Reference-oracle property test (README.md, "Testing").
//!
//! The oracle computes the expected output independently of `extract`: it builds
//! [`Descriptor`]s straight from the generated OTLP input and hashes them with
//! `canonical_bytes` plus `series_id`, and it builds the expected descriptor
//! *content* from the same generated strings. Nothing in the reference path calls
//! extraction, so the two implementations can genuinely disagree.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{Float64Type, Int32Type, Int64Type, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, LogsData, ResourceLogs, ScopeLogs,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, MetricsData,
    NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric, number_data_point,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::testing::round_trip::{encode_logs, encode_metrics};
use otel_arrow_dfe_series_lake::buffer::Block;
use otel_arrow_dfe_series_lake::cache::SeriesCache;
use otel_arrow_dfe_series_lake::canonical::{
    Descriptor, MetricDescriptor, MetricKind, SeriesId, Signal, Temporality, canonical_bytes,
    series_id,
};
use otel_arrow_dfe_series_lake::config::{DenormType, Denormalize, LakeConfig};
use otel_arrow_dfe_series_lake::extract::extract;
use otel_arrow_dfe_series_lake::schema::Dataset;
use otel_arrow_dfe_series_lake::sink::{FileNaming, Sink};
use otel_arrow_dfe_series_lake::sort::{SortSpec, is_sorted};
use otel_arrow_dfe_series_lake::value::Value;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use proptest::prelude::*;
use tokio_util::sync::CancellationToken;

const WINDOW_START: i64 = 1_789_960_500;
const SEAL_AT_US: i64 = 1_789_960_500_000_000;

// --------------------------------------------------------------- naming
//
// Every identity-bearing string of the generated input comes from one of these,
// so the model and the OTLP builder cannot drift apart without the test noticing.

fn host_name(h: u8) -> String {
    format!("h{h}")
}
fn resource_schema(h: u8) -> String {
    format!("https://r{h}")
}
fn scope_name(s: u8) -> String {
    format!("sc{s}")
}
fn scope_version(s: u8) -> String {
    format!("v{s}")
}
fn scope_schema(s: u8) -> String {
    format!("https://s{s}")
}
fn scope_attr_value(s: u8) -> String {
    format!("sa{s}")
}
fn logger_name(l: u8) -> String {
    format!("L{l}")
}
fn dp_name(d: u8) -> String {
    format!("d{d}")
}
fn metric_name(m: u8) -> String {
    format!("m{m}")
}
fn metric_unit(m: u8) -> String {
    format!("u{m}")
}

fn kv(k: &str, v: &str) -> KeyValue {
    KeyValue {
        key: k.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(v.into())),
        }),
    }
}

fn attr(k: &str, v: &str) -> (String, Value) {
    (k.to_string(), Value::Str(v.to_string()))
}

fn denorm(path: &str, column: &str) -> Denormalize {
    Denormalize {
        path: path.to_string(),
        column: column.to_string(),
        ty: DenormType::String,
    }
}

/// Denormalized columns of the logs and metrics series datasets, in schema order.
const LOGS_DENORM: [&str; 2] = ["host_col", "logger_col"];
const METRICS_DENORM: [&str; 2] = ["host_col", "dp_col"];

fn base_cfg(sorting: bool) -> LakeConfig {
    let mut cfg = LakeConfig::default();
    cfg.logs.series_attributes = vec!["logger.name".into()];
    cfg.logs.denormalize = vec![
        denorm("resource.host.id", LOGS_DENORM[0]),
        denorm("attrs.logger.name", LOGS_DENORM[1]),
    ];
    cfg.metrics.denormalize = vec![
        denorm("resource.host.id", METRICS_DENORM[0]),
        denorm("attrs.dp", METRICS_DENORM[1]),
    ];
    if !sorting {
        cfg.logs.values_sort.clear();
        cfg.metrics.values_sort.clear();
    }
    // Seal a run per request: maximal merge pressure. `validate()` is not called
    // here, so the run_target/max_row relation does not apply.
    cfg.sorting.run_target_bytes = 1;
    cfg.sorting.merge_chunk_bytes = 1;
    cfg.ingress.max_row_bytes = 1 << 20;
    cfg
}

// ------------------------------------------------- the expected descriptor
//
// The full content of one `series` row, built from the generator's own strings on
// the model side and read back from Parquet on the actual side. Comparing whole
// maps of these, keyed by the series id the file itself carries, checks every
// field exactly *and* that each descriptor hashes to the identity its values rows
// were written under.

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpectedMetric {
    name: String,
    unit: String,
    metric_type: String,
    temporality: String,
    is_monotonic: bool,
    description: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpectedSeries {
    resource_attrs: Vec<(String, String)>,
    resource_schema_url: String,
    scope_name: String,
    scope_version: String,
    scope_schema_url: String,
    scope_attrs: Vec<(String, String)>,
    attrs: Vec<(String, String)>,
    metric: Option<ExpectedMetric>,
    denorm: Vec<(String, Option<String>)>,
}

/// Read one `series` row back into the same shape the model produces.
fn read_series_row(b: &RecordBatch, row: usize, denorm_cols: &[&str]) -> ExpectedSeries {
    let is_metrics = b.schema().column_with_name("metric_name").is_some();
    ExpectedSeries {
        resource_attrs: map_at(b, "resource_attrs", row),
        resource_schema_url: string_at(b, "resource_schema_url", row),
        scope_name: string_at(b, "scope_name", row),
        scope_version: string_at(b, "scope_version", row),
        scope_schema_url: string_at(b, "scope_schema_url", row),
        scope_attrs: map_at(b, "scope_attrs", row),
        attrs: map_at(b, "attrs", row),
        metric: is_metrics.then(|| ExpectedMetric {
            name: string_at(b, "metric_name", row),
            unit: string_at(b, "unit", row),
            metric_type: string_at(b, "metric_type", row),
            temporality: string_at(b, "temporality", row),
            is_monotonic: b
                .column_by_name("is_monotonic")
                .expect("is_monotonic")
                .as_boolean()
                .value(row),
            description: string_at(b, "description", row),
        }),
        denorm: denorm_cols
            .iter()
            .map(|c| ((*c).to_string(), opt_string_at(b, c, row)))
            .collect(),
    }
}

/// Every descriptor of a series file, keyed by the `series_id` the file carries.
fn read_series_map(
    batches: &[RecordBatch],
    denorm_cols: &[&str],
) -> Result<BTreeMap<Vec<u8>, ExpectedSeries>, TestCaseError> {
    let mut out: BTreeMap<Vec<u8>, ExpectedSeries> = BTreeMap::new();
    for b in batches {
        for row in 0..b.num_rows() {
            let id = series_id_at(b, row);
            let seen = out.insert(id, read_series_row(b, row, denorm_cols));
            prop_assert!(seen.is_none(), "a series is described at most once");
        }
    }
    Ok(out)
}

/// Sorted key/value pairs of a map column, as the reader sees them.
fn pairs(kvs: &[(&str, String)]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = kvs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect();
    out.sort();
    out
}

/// Admit every request into one block, seal it, write it and read the files back.
async fn round_trip(
    cfg: &LakeConfig,
    requests: Vec<OtapArrowRecords>,
) -> Result<(BTreeMap<Dataset, Vec<RecordBatch>>, tempfile::TempDir), TestCaseError> {
    let dir = tempfile::tempdir().map_err(|e| TestCaseError::fail(e.to_string()))?;
    let store: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(dir.path())
            .map_err(|e| TestCaseError::fail(e.to_string()))?,
    );
    let mut cache = SeriesCache::new(10_000);
    let mut block = Block::new(WINDOW_START, 1, cfg.clone());
    for mut records in requests {
        let e = extract(&mut records, cfg).map_err(|e| TestCaseError::fail(format!("{e}")))?;
        let r = block
            .reserve(&e, &mut cache, 8)
            .map_err(|e| TestCaseError::fail(format!("{e}")))?;
        block
            .admit(e, r)
            .map_err(|e| TestCaseError::fail(format!("{e}")))?;
    }
    block
        .seal(SEAL_AT_US)
        .map_err(|e| TestCaseError::fail(format!("{e}")))?;
    let sink = Sink::new(store, cfg.clone(), FileNaming::new("oracle"), |timeout| {
        Box::pin(tokio::time::sleep(timeout))
    });
    let report = sink
        .write_block(&block, &CancellationToken::new())
        .await
        .map_err(|e| TestCaseError::fail(format!("{e}")))?;

    let mut out: BTreeMap<Dataset, Vec<RecordBatch>> = BTreeMap::new();
    for (ds, path, _) in &report.files {
        let file = std::fs::File::open(dir.path().join(path.as_ref()))
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| TestCaseError::fail(e.to_string()))?
            .build()
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        for b in reader {
            out.entry(*ds)
                .or_default()
                .push(b.map_err(|e| TestCaseError::fail(e.to_string()))?);
        }
    }
    Ok((out, dir))
}

fn series_id_at(b: &RecordBatch, row: usize) -> Vec<u8> {
    b.column_by_name("series_id")
        .expect("series_id")
        .as_fixed_size_binary()
        .value(row)
        .to_vec()
}

fn time_at(b: &RecordBatch, row: usize) -> Option<i64> {
    let t = b
        .column_by_name("time_unix_nano")
        .expect("time_unix_nano")
        .as_primitive::<Int64Type>();
    t.is_valid(row).then(|| t.value(row))
}

/// Map column read back as a sorted key/value vector.
fn map_at(b: &RecordBatch, name: &str, row: usize) -> Vec<(String, String)> {
    let m = b.column_by_name(name).expect("map column").as_map();
    let entries = m.value(row);
    let keys = entries.column(0).as_string::<i32>();
    let values = entries.column(1).as_string::<i32>();
    let mut out: Vec<(String, String)> = (0..entries.len())
        .map(|i| (keys.value(i).to_string(), values.value(i).to_string()))
        .collect();
    out.sort();
    out
}

fn string_at(b: &RecordBatch, name: &str, row: usize) -> String {
    b.column_by_name(name)
        .expect("utf8 column")
        .as_string::<i32>()
        .value(row)
        .to_string()
}

/// A nullable Utf8 cell, keeping null distinct from the empty string.
///
/// The distinction matters: pdata omits a value column whose entries are all the
/// default, so an all-empty-body request is exactly the shape that would decode as
/// null if the decoder got it wrong. `StringArray::value` reports both as `""`.
fn opt_string_at(b: &RecordBatch, name: &str, row: usize) -> Option<String> {
    let a = b
        .column_by_name(name)
        .expect("utf8 column")
        .as_string::<i32>();
    a.is_valid(row).then(|| a.value(row).to_string())
}

/// A `Timestamp(Microsecond)` cell.
fn opt_ts_us_at(b: &RecordBatch, name: &str, row: usize) -> Option<i64> {
    let a = b
        .column_by_name(name)
        .expect("timestamp column")
        .as_primitive::<TimestampMicrosecondType>();
    a.is_valid(row).then(|| a.value(row))
}

/// A nullable `Int64` cell.
fn opt_i64_at(b: &RecordBatch, name: &str, row: usize) -> Option<i64> {
    let a = b
        .column_by_name(name)
        .expect("int64 column")
        .as_primitive::<Int64Type>();
    a.is_valid(row).then(|| a.value(row))
}

/// A non-null `Int32` cell.
fn i32_at(b: &RecordBatch, name: &str, row: usize) -> i32 {
    b.column_by_name(name)
        .expect("int32 column")
        .as_primitive::<Int32Type>()
        .value(row)
}

/// A double's bit pattern, so that the sign of a zero and every NaN payload
/// are compared exactly.
fn bits(d: f64) -> String {
    format!("{:016x}", d.to_bits())
}

/// A nullable `Float64` cell as its bit pattern.
fn opt_f64_bits_at(b: &RecordBatch, name: &str, row: usize) -> Option<String> {
    let a = b
        .column_by_name(name)
        .expect("float64 column")
        .as_primitive::<Float64Type>();
    a.is_valid(row).then(|| bits(a.value(row)))
}

/// A `List<Int64>` cell.
fn list_i64_at(b: &RecordBatch, name: &str, row: usize) -> Vec<i64> {
    let l = b
        .column_by_name(name)
        .expect("list column")
        .as_list::<i32>();
    let items = l.value(row);
    let items = items.as_primitive::<Int64Type>();
    (0..items.len()).map(|i| items.value(i)).collect()
}

/// A `List<Float64>` cell as bit patterns.
fn list_f64_bits_at(b: &RecordBatch, name: &str, row: usize) -> Vec<String> {
    let l = b
        .column_by_name(name)
        .expect("list column")
        .as_list::<i32>();
    let items = l.value(row);
    let items = items.as_primitive::<Float64Type>();
    (0..items.len()).map(|i| bits(items.value(i))).collect()
}

/// The denormalized cells of a values row, in schema order.
fn denorm_at(b: &RecordBatch, columns: &[&str], row: usize) -> Vec<Option<String>> {
    columns.iter().map(|c| opt_string_at(b, c, row)).collect()
}

/// The eight columns every metrics values row starts with.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MetricsHead {
    series_id: Vec<u8>,
    producer_id: String,
    metric_name: String,
    time_us: Option<i64>,
    time_ns: Option<i64>,
    start_us: Option<i64>,
    start_ns: Option<i64>,
    flags: i32,
}

impl MetricsHead {
    fn read(b: &RecordBatch, row: usize) -> Self {
        Self {
            series_id: series_id_at(b, row),
            producer_id: string_at(b, "producer_id", row),
            metric_name: string_at(b, "metric_name", row),
            time_us: opt_ts_us_at(b, "time", row),
            time_ns: opt_i64_at(b, "time_unix_nano", row),
            start_us: opt_ts_us_at(b, "start_time", row),
            start_ns: opt_i64_at(b, "start_time_unix_nano", row),
            flags: i32_at(b, "flags", row),
        }
    }

    /// The model head: the generators emit no start time and no flags, and the
    /// microsecond column is the nanosecond one divided down.
    fn model(series_id: Vec<u8>, host: u8, metric: u8, time: u64) -> Self {
        let ns = (time > 0).then_some(time as i64);
        Self {
            series_id,
            producer_id: host_name(host),
            metric_name: metric_name(metric),
            time_us: ns.map(|t| t / 1000),
            time_ns: ns,
            start_us: None,
            start_ns: None,
            flags: 0,
        }
    }
}

/// Every column a number point fills in `metrics/values`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct NumberRow {
    head: MetricsHead,
    value_int: Option<i64>,
    value_double_bits: Option<String>,
    denorm: Vec<Option<String>>,
}

/// Every column a histogram point fills in `metrics/values`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct HistogramRow {
    head: MetricsHead,
    count: i64,
    sum_bits: Option<String>,
    min_bits: Option<String>,
    max_bits: Option<String>,
    bucket_counts: Vec<i64>,
    explicit_bounds_bits: Vec<String>,
    denorm: Vec<Option<String>>,
}

/// Assert the values file is globally ordered when sorting is on.
fn check_order(
    cfg: &LakeConfig,
    batches: &[RecordBatch],
    signal: Signal,
) -> Result<(), TestCaseError> {
    let keys = if signal == Signal::Logs {
        &cfg.logs.values_sort
    } else {
        &cfg.metrics.values_sort
    };
    if keys.is_empty() {
        return Ok(());
    }
    let spec = SortSpec::new(keys.clone());
    let schema = batches[0].schema();
    let all = arrow::compute::concat_batches(&schema, batches)
        .map_err(|e| TestCaseError::fail(e.to_string()))?;
    let ordered = is_sorted(&all, &spec).map_err(|e| TestCaseError::fail(format!("{e}")))?;
    prop_assert!(ordered, "values file is globally ordered by the sort spec");
    Ok(())
}

/// Timestamps, weighted towards small values but reaching both boundaries.
///
/// 0 means "absent" in OTLP, and `i64::MAX` is the largest nanosecond stamp the
/// storage column can hold; anything above it is refused as out of range.
fn any_time() -> impl Strategy<Value = u64> {
    prop_oneof![
        2 => Just(0u64),
        1 => Just(1u64),
        8 => 1u64..1_000_000,
        1 => 1_000_000_000_000_000_000u64..(i64::MAX as u64),
        1 => Just(i64::MAX as u64),
    ]
}

fn split<T: Clone>(items: &[T], sizes: &[u8]) -> Vec<Vec<T>> {
    let mut out = Vec::new();
    let mut idx = 0;
    let mut which = 0;
    while idx < items.len() {
        let n = usize::from(sizes[which % sizes.len()].max(1)).min(items.len() - idx);
        out.push(items[idx..idx + n].to_vec());
        idx += n;
        which += 1;
    }
    out
}

// ---------------------------------------------------------------- logs

#[derive(Debug, Clone)]
struct LogRec {
    host: u8,
    scope: u8,
    logger: u8,
    time: u64,
    body: String,
}

fn log_rec() -> impl Strategy<Value = LogRec> {
    (0u8..3, 0u8..2, 0u8..3, any_time(), "[a-z]{0,8}").prop_map(
        |(host, scope, logger, time, body)| LogRec {
            host,
            scope,
            logger,
            time,
            body,
        },
    )
}

/// The reference identity of a logs series, built without touching `extract`.
fn logs_descriptor(r: &LogRec) -> Descriptor {
    Descriptor {
        signal: Signal::Logs,
        resource_attrs: vec![attr("host.id", &host_name(r.host))].into(),
        resource_schema_url: resource_schema(r.host),
        scope_name: scope_name(r.scope),
        scope_version: scope_version(r.scope),
        scope_schema_url: scope_schema(r.scope),
        scope_attrs: vec![attr("sa", &scope_attr_value(r.scope))].into(),
        metric: None,
        attrs: vec![attr("logger.name", &logger_name(r.logger))],
    }
}

fn logs_series_id(r: &LogRec) -> SeriesId {
    series_id(&canonical_bytes(&logs_descriptor(r)))
}

/// The reference descriptor content of a logs series.
fn logs_expected(r: &LogRec) -> ExpectedSeries {
    ExpectedSeries {
        resource_attrs: pairs(&[("host.id", host_name(r.host))]),
        resource_schema_url: resource_schema(r.host),
        scope_name: scope_name(r.scope),
        scope_version: scope_version(r.scope),
        scope_schema_url: scope_schema(r.scope),
        scope_attrs: pairs(&[("sa", scope_attr_value(r.scope))]),
        attrs: pairs(&[("logger.name", logger_name(r.logger))]),
        metric: None,
        denorm: vec![
            (LOGS_DENORM[0].to_string(), Some(host_name(r.host))),
            (LOGS_DENORM[1].to_string(), Some(logger_name(r.logger))),
        ],
    }
}

fn scopes_of<T, F: Fn(&T) -> (u8, u8)>(recs: &[T], host: u8, key: F) -> Vec<u8> {
    let mut out: Vec<u8> = recs
        .iter()
        .map(&key)
        .filter(|(h, _)| *h == host)
        .map(|(_, s)| s)
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

fn hosts_of<T, F: Fn(&T) -> (u8, u8)>(recs: &[T], key: F) -> Vec<u8> {
    let mut out: Vec<u8> = recs.iter().map(&key).map(|(h, _)| h).collect();
    out.sort_unstable();
    out.dedup();
    out
}

fn logs_request(recs: &[LogRec]) -> OtapArrowRecords {
    let key = |r: &LogRec| (r.host, r.scope);
    encode_logs(&LogsData {
        resource_logs: hosts_of(recs, key)
            .into_iter()
            .map(|h| ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", &host_name(h))],
                    ..Default::default()
                }),
                schema_url: resource_schema(h),
                scope_logs: scopes_of(recs, h, key)
                    .into_iter()
                    .map(|s| ScopeLogs {
                        scope: Some(InstrumentationScope {
                            name: scope_name(s),
                            version: scope_version(s),
                            attributes: vec![kv("sa", &scope_attr_value(s))],
                            ..Default::default()
                        }),
                        schema_url: scope_schema(s),
                        log_records: recs
                            .iter()
                            .filter(|r| r.host == h && r.scope == s)
                            .map(|r| LogRecord {
                                time_unix_nano: r.time,
                                body: Some(AnyValue {
                                    value: Some(any_value::Value::StringValue(r.body.clone())),
                                }),
                                attributes: vec![kv("logger.name", &logger_name(r.logger))],
                                ..Default::default()
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect(),
    })
}

async fn logs_case(recs: Vec<LogRec>, sizes: Vec<u8>, sorting: bool) -> Result<(), TestCaseError> {
    let cfg = base_cfg(sorting);
    let requests: Vec<OtapArrowRecords> = split(&recs, &sizes)
        .iter()
        .map(|c| logs_request(c))
        .collect();
    let (files, _dir) = round_trip(&cfg, requests).await?;

    // Model, computed without extraction.
    let mut model: Vec<(Vec<u8>, Option<i64>, Option<String>)> = recs
        .iter()
        .map(|r| {
            (
                logs_series_id(r).to_vec(),
                (r.time > 0).then_some(r.time as i64),
                Some(r.body.clone()),
            )
        })
        .collect();
    let model_series: BTreeMap<Vec<u8>, ExpectedSeries> = recs
        .iter()
        .map(|r| (logs_series_id(r).to_vec(), logs_expected(r)))
        .collect();

    let values = files.get(&Dataset::LogsValues).expect("values file");
    let mut actual: Vec<(Vec<u8>, Option<i64>, Option<String>)> = Vec::new();
    for b in values {
        for row in 0..b.num_rows() {
            actual.push((
                series_id_at(b, row),
                time_at(b, row),
                opt_string_at(b, "body", row),
            ));
        }
    }
    model.sort();
    actual.sort();
    prop_assert_eq!(
        actual,
        model,
        "logs value rows as a multiset of (series_id, time, body)"
    );
    check_order(&cfg, values, Signal::Logs)?;

    let series = files.get(&Dataset::LogsSeries).expect("series file");
    let actual_series = read_series_map(series, &LOGS_DENORM)?;
    prop_assert_eq!(
        actual_series,
        model_series,
        "every logs descriptor, field by field, keyed by its own series id"
    );
    Ok(())
}

// ------------------------------------------------------- metric numbers

/// Kind, temporality and monotonicity are a function of the metric index, because
/// every data point of one OTLP metric shares them.
fn number_kind(m: u8) -> (MetricKind, Temporality, bool) {
    match m % 3 {
        0 => (MetricKind::Gauge, Temporality::Unspecified, false),
        1 => (MetricKind::Sum, Temporality::Delta, true),
        _ => (MetricKind::Sum, Temporality::Cumulative, false),
    }
}

fn hist_kind(m: u8) -> (MetricKind, Temporality) {
    if m.is_multiple_of(2) {
        (MetricKind::Histogram, Temporality::Delta)
    } else {
        (MetricKind::Histogram, Temporality::Cumulative)
    }
}

fn temporality_proto(t: Temporality) -> i32 {
    match t {
        Temporality::Delta => AggregationTemporality::Delta as i32,
        Temporality::Cumulative => AggregationTemporality::Cumulative as i32,
        Temporality::Unspecified => AggregationTemporality::Unspecified as i32,
    }
}

#[derive(Debug, Clone)]
struct NumRec {
    host: u8,
    scope: u8,
    metric: u8,
    dp: u8,
    time: u64,
    int_value: Option<i64>,
    double_value: Option<f64>,
}

/// Integers weighted towards small values but including both boundaries and zero.
fn any_int() -> impl Strategy<Value = i64> {
    prop_oneof![
        8 => -1000i64..1000,
        2 => Just(0i64),
        1 => Just(i64::MIN),
        1 => Just(i64::MAX),
    ]
}

/// Doubles weighted towards small values but including zero of both signs, the
/// smallest subnormal, the smallest normal and both extremes.
fn any_double() -> impl Strategy<Value = f64> {
    prop_oneof![
        8 => -1000.0f64..1000.0,
        1 => Just(0.0f64),
        1 => Just(-0.0f64),
        1 => Just(f64::from_bits(1)),
        1 => Just(f64::MIN_POSITIVE),
        1 => Just(f64::MAX),
        1 => Just(f64::MIN),
    ]
}

fn num_rec() -> impl Strategy<Value = NumRec> {
    (
        0u8..2,
        0u8..2,
        0u8..3,
        0u8..3,
        any_time(),
        any::<bool>(),
        any_int(),
        any_double(),
    )
        .prop_map(|(host, scope, metric, dp, time, is_int, i, d)| NumRec {
            host,
            scope,
            metric,
            dp,
            time,
            int_value: is_int.then_some(i),
            double_value: (!is_int).then_some(d),
        })
}

fn metrics_descriptor(
    host: u8,
    scope: u8,
    m: u8,
    kind: MetricKind,
    temporality: Temporality,
    is_monotonic: bool,
    dp: u8,
) -> Descriptor {
    Descriptor {
        signal: Signal::Metrics,
        resource_attrs: vec![attr("host.id", &host_name(host))].into(),
        resource_schema_url: resource_schema(host),
        scope_name: scope_name(scope),
        scope_version: scope_version(scope),
        scope_schema_url: scope_schema(scope),
        scope_attrs: vec![attr("sa", &scope_attr_value(scope))].into(),
        metric: Some(MetricDescriptor {
            name: metric_name(m),
            unit: metric_unit(m),
            kind,
            temporality,
            is_monotonic,
            description: String::new(),
        }),
        attrs: vec![attr("dp", &dp_name(dp))],
    }
}

fn metrics_expected(
    host: u8,
    scope: u8,
    m: u8,
    kind: MetricKind,
    temporality: Temporality,
    is_monotonic: bool,
    dp: u8,
) -> ExpectedSeries {
    ExpectedSeries {
        resource_attrs: pairs(&[("host.id", host_name(host))]),
        resource_schema_url: resource_schema(host),
        scope_name: scope_name(scope),
        scope_version: scope_version(scope),
        scope_schema_url: scope_schema(scope),
        scope_attrs: pairs(&[("sa", scope_attr_value(scope))]),
        attrs: pairs(&[("dp", dp_name(dp))]),
        metric: Some(ExpectedMetric {
            name: metric_name(m),
            unit: metric_unit(m),
            metric_type: kind.as_str().to_string(),
            temporality: temporality.as_str().to_string(),
            is_monotonic,
            description: String::new(),
        }),
        denorm: vec![
            (METRICS_DENORM[0].to_string(), Some(host_name(host))),
            (METRICS_DENORM[1].to_string(), Some(dp_name(dp))),
        ],
    }
}

fn metrics_request(
    build: impl Fn(u8, u8) -> Vec<Metric>,
    recs_key: &[(u8, u8)],
) -> OtapArrowRecords {
    let key = |p: &(u8, u8)| *p;
    encode_metrics(&MetricsData {
        resource_metrics: hosts_of(recs_key, key)
            .into_iter()
            .map(|h| ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", &host_name(h))],
                    ..Default::default()
                }),
                schema_url: resource_schema(h),
                scope_metrics: scopes_of(recs_key, h, key)
                    .into_iter()
                    .map(|s| ScopeMetrics {
                        scope: Some(InstrumentationScope {
                            name: scope_name(s),
                            version: scope_version(s),
                            attributes: vec![kv("sa", &scope_attr_value(s))],
                            ..Default::default()
                        }),
                        schema_url: scope_schema(s),
                        metrics: build(h, s),
                    })
                    .collect(),
            })
            .collect(),
    })
}

fn metric_indices<T, F: Fn(&T) -> (u8, u8, u8)>(recs: &[T], h: u8, s: u8, key: F) -> Vec<u8> {
    let mut out: Vec<u8> = recs
        .iter()
        .map(&key)
        .filter(|(rh, rs, _)| *rh == h && *rs == s)
        .map(|(_, _, m)| m)
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

/// The gauge and sum metrics of `recs` under host `h` and scope `s`.
fn number_metrics(recs: &[NumRec], h: u8, s: u8) -> Vec<Metric> {
    metric_indices(recs, h, s, |r| (r.host, r.scope, r.metric))
        .into_iter()
        .map(|m| {
            let (kind, temporality, is_monotonic) = number_kind(m);
            let points: Vec<NumberDataPoint> = recs
                .iter()
                .filter(|r| r.host == h && r.scope == s && r.metric == m)
                .map(|r| NumberDataPoint {
                    time_unix_nano: r.time,
                    attributes: vec![kv("dp", &dp_name(r.dp))],
                    value: Some(match (r.int_value, r.double_value) {
                        (Some(i), _) => number_data_point::Value::AsInt(i),
                        (_, Some(d)) => number_data_point::Value::AsDouble(d),
                        _ => number_data_point::Value::AsInt(0),
                    }),
                    ..Default::default()
                })
                .collect();
            Metric {
                name: metric_name(m),
                unit: metric_unit(m),
                data: Some(if kind == MetricKind::Gauge {
                    metric::Data::Gauge(Gauge {
                        data_points: points,
                    })
                } else {
                    metric::Data::Sum(Sum {
                        aggregation_temporality: temporality_proto(temporality),
                        is_monotonic,
                        data_points: points,
                    })
                }),
                ..Default::default()
            }
        })
        .collect()
}

fn number_request(recs: &[NumRec]) -> OtapArrowRecords {
    let keys: Vec<(u8, u8)> = recs.iter().map(|r| (r.host, r.scope)).collect();
    metrics_request(|h, s| number_metrics(recs, h, s), &keys)
}

/// The series id and descriptor of a number point.
fn number_identity(r: &NumRec) -> (Vec<u8>, ExpectedSeries) {
    let (kind, temporality, is_monotonic) = number_kind(r.metric);
    (
        series_id(&canonical_bytes(&metrics_descriptor(
            r.host,
            r.scope,
            r.metric,
            kind,
            temporality,
            is_monotonic,
            r.dp,
        )))
        .to_vec(),
        metrics_expected(
            r.host,
            r.scope,
            r.metric,
            kind,
            temporality,
            is_monotonic,
            r.dp,
        ),
    )
}

/// The values row a number point must produce.
fn number_model(r: &NumRec) -> NumberRow {
    NumberRow {
        head: MetricsHead::model(number_identity(r).0, r.host, r.metric, r.time),
        value_int: r.int_value,
        value_double_bits: r.double_value.map(bits),
        denorm: vec![Some(host_name(r.host)), Some(dp_name(r.dp))],
    }
}

/// Read row `row` of a values batch as a number row.
fn read_number_row(b: &RecordBatch, row: usize) -> NumberRow {
    NumberRow {
        head: MetricsHead::read(b, row),
        value_int: opt_i64_at(b, "value_int", row),
        value_double_bits: opt_f64_bits_at(b, "value_double", row),
        denorm: denorm_at(b, &METRICS_DENORM, row),
    }
}

async fn number_case(
    recs: Vec<NumRec>,
    sizes: Vec<u8>,
    sorting: bool,
) -> Result<(), TestCaseError> {
    let cfg = base_cfg(sorting);
    let requests: Vec<OtapArrowRecords> = split(&recs, &sizes)
        .iter()
        .map(|c| number_request(c))
        .collect();
    let (files, _dir) = round_trip(&cfg, requests).await?;

    let mut model: Vec<NumberRow> = recs.iter().map(number_model).collect();
    let model_series: BTreeMap<Vec<u8>, ExpectedSeries> =
        recs.iter().map(number_identity).collect();

    let values = files
        .get(&Dataset::MetricsValues)
        .expect("metrics values file");
    let mut actual: Vec<NumberRow> = Vec::new();
    for b in values {
        for row in 0..b.num_rows() {
            for col in [
                "count",
                "sum",
                "min",
                "max",
                "bucket_counts",
                "explicit_bounds",
            ] {
                prop_assert!(
                    b.column_by_name(col).expect("column").is_null(row),
                    "a number row leaves {col} null in the merged dataset"
                );
            }
            actual.push(read_number_row(b, row));
        }
    }
    model.sort();
    actual.sort();
    prop_assert_eq!(
        actual,
        model,
        "number rows as a multiset of complete rows, every column compared"
    );
    check_order(&cfg, values, Signal::Metrics)?;

    let series = files.get(&Dataset::MetricsSeries).expect("series file");
    let actual_series = read_series_map(series, &METRICS_DENORM)?;
    prop_assert_eq!(
        actual_series,
        model_series,
        "every number descriptor, field by field, keyed by its own series id"
    );
    Ok(())
}

// ---------------------------------------------------- metric histograms

#[derive(Debug, Clone)]
struct HistRec {
    host: u8,
    scope: u8,
    metric: u8,
    dp: u8,
    time: u64,
    count: u64,
}

/// Counts weighted small but reaching the largest value the `Int64` storage
/// column can hold; anything above it is refused as out of range.
fn any_count() -> impl Strategy<Value = u64> {
    prop_oneof![
        8 => 0u64..100,
        1 => Just(0u64),
        1 => Just(u64::from(u32::MAX)),
        1 => Just(i64::MAX as u64),
    ]
}

fn hist_rec() -> impl Strategy<Value = HistRec> {
    (0u8..2, 0u8..2, 0u8..2, 0u8..3, any_time(), any_count()).prop_map(
        |(host, scope, metric, dp, time, count)| HistRec {
            host,
            scope,
            metric,
            dp,
            time,
            count,
        },
    )
}

/// The histogram metrics of `recs` under host `h` and scope `s`.
fn histogram_metrics(recs: &[HistRec], h: u8, s: u8) -> Vec<Metric> {
    metric_indices(recs, h, s, |r| (r.host, r.scope, r.metric))
        .into_iter()
        .map(|m| {
            let (_, temporality) = hist_kind(m);
            Metric {
                name: metric_name(m),
                unit: metric_unit(m),
                data: Some(metric::Data::Histogram(Histogram {
                    aggregation_temporality: temporality_proto(temporality),
                    data_points: recs
                        .iter()
                        .filter(|r| r.host == h && r.scope == s && r.metric == m)
                        .map(|r| HistogramDataPoint {
                            time_unix_nano: r.time,
                            attributes: vec![kv("dp", &dp_name(r.dp))],
                            count: r.count,
                            sum: Some(r.count as f64),
                            bucket_counts: vec![r.count, 0],
                            explicit_bounds: vec![1.0],
                            ..Default::default()
                        })
                        .collect(),
                })),
                ..Default::default()
            }
        })
        .collect()
}

fn histogram_request(recs: &[HistRec]) -> OtapArrowRecords {
    let keys: Vec<(u8, u8)> = recs.iter().map(|r| (r.host, r.scope)).collect();
    metrics_request(|h, s| histogram_metrics(recs, h, s), &keys)
}

/// The series id and descriptor of a histogram point.
fn histogram_identity(r: &HistRec) -> (Vec<u8>, ExpectedSeries) {
    let (kind, temporality) = hist_kind(r.metric);
    (
        series_id(&canonical_bytes(&metrics_descriptor(
            r.host,
            r.scope,
            r.metric,
            kind,
            temporality,
            false,
            r.dp,
        )))
        .to_vec(),
        metrics_expected(r.host, r.scope, r.metric, kind, temporality, false, r.dp),
    )
}

/// The values row a histogram point must produce; `any_sum` says whether
/// some histogram point of its request has a non-zero sum (see
/// `histogram_case`).
fn histogram_model(r: &HistRec, any_sum: bool) -> HistogramRow {
    HistogramRow {
        head: MetricsHead::model(histogram_identity(r).0, r.host, r.metric, r.time),
        count: r.count as i64,
        sum_bits: any_sum.then(|| bits(r.count as f64)),
        min_bits: None,
        max_bits: None,
        bucket_counts: vec![r.count as i64, 0],
        explicit_bounds_bits: vec![bits(1.0)],
        denorm: vec![Some(host_name(r.host)), Some(dp_name(r.dp))],
    }
}

/// Read row `row` of a values batch as a histogram row.
fn read_histogram_row(b: &RecordBatch, row: usize) -> HistogramRow {
    HistogramRow {
        head: MetricsHead::read(b, row),
        count: b
            .column_by_name("count")
            .expect("count")
            .as_primitive::<Int64Type>()
            .value(row),
        sum_bits: opt_f64_bits_at(b, "sum", row),
        min_bits: opt_f64_bits_at(b, "min", row),
        max_bits: opt_f64_bits_at(b, "max", row),
        bucket_counts: list_i64_at(b, "bucket_counts", row),
        explicit_bounds_bits: list_f64_bits_at(b, "explicit_bounds", row),
        denorm: denorm_at(b, &METRICS_DENORM, row),
    }
}

async fn histogram_case(
    recs: Vec<HistRec>,
    sizes: Vec<u8>,
    sorting: bool,
) -> Result<(), TestCaseError> {
    let cfg = base_cfg(sorting);
    let chunks = split(&recs, &sizes);
    let requests: Vec<OtapArrowRecords> = chunks.iter().map(|c| histogram_request(c)).collect();
    let (files, _dir) = round_trip(&cfg, requests).await?;

    // The generator sets `sum` to the count and leaves min and max unset. The
    // model is built per request, because a `sum` of zero comes back null only
    // when every point of that request has a zero sum: pdata omits an optional
    // column whose every entry is the type default, and writes the value
    // otherwise. That is a property of the transport encoding, not of this
    // writer, which stores whatever the decoded batch holds.
    let mut model: Vec<HistogramRow> = chunks
        .iter()
        .flat_map(|chunk| {
            let any_sum = chunk.iter().any(|r| r.count != 0);
            chunk.iter().map(move |r| histogram_model(r, any_sum))
        })
        .collect();
    let model_series: BTreeMap<Vec<u8>, ExpectedSeries> =
        recs.iter().map(histogram_identity).collect();

    let values = files
        .get(&Dataset::MetricsValues)
        .expect("metrics values file");
    let mut actual: Vec<HistogramRow> = Vec::new();
    for b in values {
        for row in 0..b.num_rows() {
            for col in ["value_int", "value_double"] {
                prop_assert!(
                    b.column_by_name(col).expect("column").is_null(row),
                    "a histogram row leaves {col} null in the merged dataset"
                );
            }
            actual.push(read_histogram_row(b, row));
        }
    }
    model.sort();
    actual.sort();
    prop_assert_eq!(
        actual,
        model,
        "histogram rows as a multiset of complete rows, every column compared"
    );
    check_order(&cfg, values, Signal::Metrics)?;

    let series = files.get(&Dataset::MetricsSeries).expect("series file");
    let actual_series = read_series_map(series, &METRICS_DENORM)?;
    prop_assert_eq!(
        actual_series,
        model_series,
        "every histogram descriptor, field by field, keyed by its own series id"
    );
    Ok(())
}

// ---------------------------------------------------- mixed metric kinds

/// Histogram metric indices in a mixed request start here, so no histogram
/// shares a name with a number metric (`num_rec` draws 0..3); the offset is
/// even, so `hist_kind` keeps each index's temporality.
const MIXED_HISTOGRAM_OFFSET: u8 = 10;

/// One point of a request mixing number and histogram metrics.
#[derive(Debug, Clone)]
enum MixRec {
    Number(NumRec),
    Histogram(HistRec),
}

fn mix_rec() -> impl Strategy<Value = MixRec> {
    prop_oneof![
        num_rec().prop_map(MixRec::Number),
        hist_rec().prop_map(|mut r| {
            r.metric += MIXED_HISTOGRAM_OFFSET;
            MixRec::Histogram(r)
        }),
    ]
}

/// The number and histogram points of `recs`.
fn unmix(recs: &[MixRec]) -> (Vec<NumRec>, Vec<HistRec>) {
    let mut numbers = Vec::new();
    let mut histograms = Vec::new();
    for r in recs {
        match r {
            MixRec::Number(n) => numbers.push(n.clone()),
            MixRec::Histogram(h) => histograms.push(h.clone()),
        }
    }
    (numbers, histograms)
}

fn mixed_request(recs: &[MixRec]) -> OtapArrowRecords {
    let (numbers, histograms) = unmix(recs);
    let keys: Vec<(u8, u8)> = numbers
        .iter()
        .map(|r| (r.host, r.scope))
        .chain(histograms.iter().map(|r| (r.host, r.scope)))
        .collect();
    metrics_request(
        |h, s| {
            let mut metrics = number_metrics(&numbers, h, s);
            metrics.extend(histogram_metrics(&histograms, h, s));
            metrics
        },
        &keys,
    )
}

/// A values row of either kind, compared as one multiset.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum MixedRow {
    Number(NumberRow),
    Histogram(HistogramRow),
}

async fn mixed_case(recs: Vec<MixRec>, sizes: Vec<u8>, sorting: bool) -> Result<(), TestCaseError> {
    let cfg = base_cfg(sorting);
    let chunks = split(&recs, &sizes);
    let requests: Vec<OtapArrowRecords> = chunks.iter().map(|c| mixed_request(c)).collect();
    let (files, _dir) = round_trip(&cfg, requests).await?;

    let mut model: Vec<MixedRow> = Vec::new();
    let mut model_series: BTreeMap<Vec<u8>, ExpectedSeries> = BTreeMap::new();
    for chunk in &chunks {
        let (numbers, histograms) = unmix(chunk);
        let any_sum = histograms.iter().any(|r| r.count != 0);
        model.extend(numbers.iter().map(|r| MixedRow::Number(number_model(r))));
        model.extend(
            histograms
                .iter()
                .map(|r| MixedRow::Histogram(histogram_model(r, any_sum))),
        );
        model_series.extend(numbers.iter().map(number_identity));
        model_series.extend(histograms.iter().map(histogram_identity));
    }

    let values = files
        .get(&Dataset::MetricsValues)
        .expect("metrics values file");
    let mut actual: Vec<MixedRow> = Vec::new();
    for b in values {
        for row in 0..b.num_rows() {
            // A histogram row always has a count; a number row never has one.
            actual.push(if b.column_by_name("count").expect("count").is_null(row) {
                MixedRow::Number(read_number_row(b, row))
            } else {
                MixedRow::Histogram(read_histogram_row(b, row))
            });
        }
    }
    model.sort();
    actual.sort();
    prop_assert_eq!(
        actual,
        model,
        "number and histogram rows of the same requests as one multiset of complete rows"
    );
    check_order(&cfg, values, Signal::Metrics)?;

    let series = files.get(&Dataset::MetricsSeries).expect("series file");
    let actual_series = read_series_map(series, &METRICS_DENORM)?;
    prop_assert_eq!(
        actual_series,
        model_series,
        "every descriptor of both kinds, field by field, keyed by its own series id"
    );
    Ok(())
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt")
}

/// Every generated dataset is run twice, with sorting off and on, so no case
/// escapes either mode.
const SORTING_MODES: [bool; 2] = [false, true];

/// A zero-count histogram point of host, scope, metric and point 0 at time 0.
fn zero_histogram(count: u64) -> HistRec {
    HistRec {
        host: 0,
        scope: 0,
        metric: 0,
        dp: 0,
        time: 0,
        count,
    }
}

/// Scenario: one histogram point with count 0 and time 0 alone in a request, sorting off and on
/// (a shrunk proptest failure, kept as a plain test so a strategy change cannot drop it).
/// Guarantees: its row stores count 0 and a null sum, and its descriptor equals the model.
#[test]
fn oracle_regression_one_zero_count_histogram() {
    for sorting in SORTING_MODES {
        rt().block_on(histogram_case(vec![zero_histogram(0)], vec![1], sorting))
            .unwrap_or_else(|e| panic!("sorting {sorting}: {e}"));
    }
}

/// Scenario: two points of one histogram series in one request, counts 0 and 1, sorting off and
/// on (a shrunk proptest failure, kept as a plain test).
/// Guarantees: both rows are stored, the zero sum is kept because the request has a non-zero one,
/// and one descriptor equals the model.
#[test]
fn oracle_regression_zero_and_one_count_histograms() {
    for sorting in SORTING_MODES {
        rt().block_on(histogram_case(
            vec![zero_histogram(0), zero_histogram(1)],
            vec![2],
            sorting,
        ))
        .unwrap_or_else(|e| panic!("sorting {sorting}: {e}"));
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 32, .. ProptestConfig::default() })]

    /// Scenario: random log records over several resources and scopes in random requests, sorting
    /// off and on.
    /// Guarantees: values rows and every descriptor field equal the independent model; sorted files
    /// are globally ordered.
    #[test]
    fn oracle_logs(
        recs in prop::collection::vec(log_rec(), 1..40),
        sizes in prop::collection::vec(1u8..6, 1..8),
    ) {
        for sorting in SORTING_MODES {
            rt().block_on(logs_case(recs.clone(), sizes.clone(), sorting))?;
        }
    }

    /// Scenario: random gauge and sum points of every temporality and monotonicity in random
    /// requests, sorting off and on.
    /// Guarantees: complete rows (doubles by bit pattern) and every descriptor field equal the
    /// model.
    #[test]
    fn oracle_metric_numbers(
        recs in prop::collection::vec(num_rec(), 1..30),
        sizes in prop::collection::vec(1u8..6, 1..8),
    ) {
        for sorting in SORTING_MODES {
            rt().block_on(number_case(recs.clone(), sizes.clone(), sorting))?;
        }
    }

    /// Scenario: random histogram points of both temporalities in random requests, sorting off and
    /// on.
    /// Guarantees: complete rows (doubles by bit pattern) and every descriptor field equal the
    /// model.
    #[test]
    fn oracle_metric_histograms(
        recs in prop::collection::vec(hist_rec(), 1..30),
        sizes in prop::collection::vec(1u8..6, 1..8),
    ) {
        for sorting in SORTING_MODES {
            rt().block_on(histogram_case(recs.clone(), sizes.clone(), sorting))?;
        }
    }

    /// Scenario: gauge, sum and histogram points of disjoint metric names mixed in the same random
    /// requests, sorting off and on.
    /// Guarantees: every row of both kinds, complete (doubles by bit pattern), and every
    /// descriptor equal the model; sorted files are globally ordered across kinds.
    #[test]
    fn oracle_metric_mixed(
        recs in prop::collection::vec(mix_rec(), 1..40),
        sizes in prop::collection::vec(1u8..6, 1..8),
    ) {
        for sorting in SORTING_MODES {
            rt().block_on(mixed_case(recs.clone(), sizes.clone(), sorting))?;
        }
    }
}
