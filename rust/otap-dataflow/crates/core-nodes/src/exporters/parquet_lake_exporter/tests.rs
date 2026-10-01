// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Node-level tests of the Parquet lake exporter: ack-after-land, idempotent retries, deadline and
//! shutdown Nacks, window and byte rotation, the flush pipeline and its backpressure, the series
//! cache across blocks, request refusals and the four datasets.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use arrow::array::{Array, AsArray, RecordBatch};
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::Interests;
use otel_arrow_dfe_engine::context::{ControllerContext, PipelineContext};
use otel_arrow_dfe_engine::control::{NackCause, PipelineCompletionMsg};
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::testing::exporter::{
    TestContext, TestRuntime, create_test_pipeline_context,
};
use otel_arrow_dfe_engine::testing::test_node;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_otap::testing::{TestCallData, next_ack, next_nack};
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::trace::v1::ExportTraceServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    AnyValue, ArrayValue, KeyValue, any_value,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::trace::v1::{ResourceSpans, ScopeSpans, Span};
use otel_arrow_dfe_pdata::{OtapPayload, OtlpProtoBytes};
use otel_arrow_dfe_telemetry::metrics::MetricValue;
use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::file::reader::{FileReader, SerializedFileReader};
use prost::Message as _;
use serde_json::{Value, json};
use xxhash_rust::xxh3::xxh3_128;

use super::canonical::Signal;
use super::config::LakeConfig;
use super::extract::extract_logs;
use super::schema::Schemas;
use super::test_fixtures::{
    kv, logs_payload, logs_request, metrics_payload, sized_logs_payload, sized_logs_request,
    to_otap,
};
use super::test_store::{Faults, TestStore, list_paths};
use super::window::TestWallClock;
use super::{FlushReason, PARQUET_LAKE_EXPORTER_URN, Parked, ParquetLakeExporter, State};

/// Node id used for the Ack/Nack subscription of test pdata.
const NODE: usize = 4242;

/// 2026-09-30T10:59:50Z: ten seconds before an hour boundary.
const T0: i64 = 1_790_765_990;

/// Window used by most tests: a non-empty generation rotates within 50 ms.
const SHORT: Duration = Duration::from_millis(50);
/// Window of the tests that put several requests into one generation.
const WIDE: Duration = Duration::from_millis(200);
/// Window of the byte-driven tests: no window ends during the test, so only
/// `window.max_block_bytes` and a shutdown rotate generations.
const NEVER: Duration = Duration::from_secs(3600);

const LOGS_VALUES: &str = "signal=logs/dataset=values/";
const LOGS_SERIES: &str = "signal=logs/dataset=series/";
const METRICS_SERIES: &str = "signal=metrics/dataset=series/";

const MIB: usize = 1024 * 1024;
/// Body size of the sized requests.
const BODY: usize = 8192;
/// Rows of a request of about 360 KiB: two fit a 1 MiB block together with the first chunk of a
/// third.
const MEDIUM_ROWS: usize = 43;
/// Rows of a request of about 440 KiB: two fit a 1 MiB block, two plus one chunk do not.
const LARGE_ROWS: usize = 53;

/// Config: `extra` is merged over these values (objects are merged key by key).
fn config(dir: &std::path::Path, extra: Value) -> Value {
    let mut cfg = json!({
        "storage": {"file": {"base_uri": dir.to_str().expect("utf8 path")}},
        "window": {"flush_retry_deadline": "5s"},
        "retry_initial_backoff": "5ms",
        "retry_max_backoff": "20ms",
    });
    merge(&mut cfg, &extra);
    cfg
}

fn merge(into: &mut Value, from: &Value) {
    match (into, from) {
        (Value::Object(a), Value::Object(b)) => {
            for (k, v) in b {
                merge(a.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (a, b) => *a = b.clone(),
    }
}

/// Blocks and requests of at most 1 MiB (chunks of at most 256 KiB).
fn one_mib() -> Value {
    json!({
        "window": {"max_block_bytes": MIB},
        "ingress": {"max_extracted_bytes": MIB},
    })
}

/// The exporter with a fault store, the given window interval and a wall clock frozen at `T0`,
/// registering its metrics in `pipeline`.
fn bare_exporter_in(
    pipeline: &PipelineContext,
    config: &Value,
    store: Arc<dyn ObjectStore>,
    interval: Duration,
) -> (ParquetLakeExporter, TestWallClock) {
    let mut exporter = ParquetLakeExporter::from_config(pipeline, config).expect("valid config");
    let clock = TestWallClock::at_secs(T0);
    exporter.store_override = Some(store);
    exporter.interval_override = Some(interval);
    exporter.wall = Rc::new(clock.clone());
    (exporter, clock)
}

/// `bare_exporter_in` with a throwaway pipeline context (unit tests of the loop state).
fn bare_exporter(
    config: &Value,
    store: Arc<dyn ObjectStore>,
    interval: Duration,
) -> (ParquetLakeExporter, TestWallClock) {
    bare_exporter_in(&create_test_pipeline_context(), config, store, interval)
}

/// The `exporter.parquet_lake` counters and gauges the runtime's registry holds after the node's
/// terminal snapshot was reported (distributions are left out).
fn lake_metrics(registry: &TelemetryRegistryHandle) -> HashMap<&'static str, u64> {
    let mut out = HashMap::new();
    registry.visit_metrics_and_reset(|descriptor, _attrs, values| {
        if descriptor.name != "exporter.parquet_lake" {
            return;
        }
        for (field, value) in values {
            if let MetricValue::U64(v) = value {
                let _ = out.insert(field.name, *v);
            }
        }
    });
    out
}

/// The exporter under test with 50 ms windows.
fn exporter(
    runtime: &TestRuntime<OtapPdata>,
    config: &Value,
    store: Arc<dyn ObjectStore>,
) -> (ExporterWrapper<OtapPdata>, TestWallClock) {
    exporter_with(runtime, config, store, SHORT)
}

/// Like `exporter`, with the given window interval.
fn exporter_with(
    runtime: &TestRuntime<OtapPdata>,
    config: &Value,
    store: Arc<dyn ObjectStore>,
    interval: Duration,
) -> (ExporterWrapper<OtapPdata>, TestWallClock) {
    // The metric set lives in the runtime's registry, where the node's terminal snapshot lands.
    let pipeline = ControllerContext::new(runtime.metrics_registry()).pipeline_context_with(
        "test_group".into(),
        "test_pipeline".into(),
        0,
        1,
        0,
    );
    let (exporter, clock) = bare_exporter_in(&pipeline, config, store, interval);
    let wrapper = ExporterWrapper::local(
        exporter,
        test_node(runtime.config().name.clone()),
        Arc::new(NodeUserConfig::new_exporter_config(
            PARQUET_LAKE_EXPORTER_URN,
        )),
        runtime.config(),
    );
    (wrapper, clock)
}

fn subscribed(payload: OtapPayload) -> OtapPdata {
    OtapPdata::new_default(payload).test_subscribe_to(
        Interests::ACKS | Interests::NACKS,
        TestCallData::default().into(),
        NODE,
    )
}

#[derive(Debug, Default)]
struct Outcomes {
    acks: usize,
    /// (permanent, cause, reason)
    nacks: Vec<(bool, NackCause, String)>,
}

fn drain(ctx: &mut TestContext<OtapPdata>) -> Outcomes {
    let mut rx = ctx
        .take_pipeline_completion_receiver()
        .expect("completion receiver");
    let mut out = Outcomes::default();
    while let Ok(msg) = rx.try_recv() {
        match msg {
            PipelineCompletionMsg::DeliverAck { ack } => {
                if let Some((node, _)) = next_ack(ack) {
                    assert_eq!(node, NODE);
                    out.acks += 1;
                }
            }
            PipelineCompletionMsg::DeliverNack { nack } => {
                if let Some((node, nack)) = next_nack(nack) {
                    assert_eq!(node, NODE);
                    out.nacks.push((nack.permanent, nack.cause, nack.reason));
                }
            }
        }
    }
    out
}

/// The single Nack of a refused request: permanent, with cause `Refused`. Returns its reason.
fn refusal(out: &Outcomes) -> &str {
    assert_eq!(out.acks, 0);
    assert_eq!(out.nacks.len(), 1, "{:?}", out.nacks);
    let (permanent, cause, reason) = &out.nacks[0];
    assert!(permanent, "{reason}");
    assert_eq!(*cause, NackCause::Refused, "{reason}");
    reason
}

fn deadline_in(ms: u64) -> Instant {
    Instant::now() + Duration::from_millis(ms)
}

fn traces_payload() -> OtapPayload {
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    name: "span".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    OtlpProtoBytes::ExportTracesRequest(Bytes::from(req.encode_to_vec())).into()
}

async fn read(store: &dyn ObjectStore, path: &str) -> Bytes {
    store
        .get(&Path::from(path))
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes")
}

fn batches(file: Bytes) -> Vec<RecordBatch> {
    ParquetRecordBatchReader::try_new(file, 1024)
        .expect("reader")
        .map(|b| b.expect("batch"))
        .collect()
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

async fn rows_of(store: &dyn ObjectStore, path: &str) -> usize {
    rows(&batches(read(store, path).await))
}

fn series_ids(batches: &[RecordBatch]) -> HashSet<Vec<u8>> {
    batches
        .iter()
        .flat_map(|b| {
            let a = b.column(0).as_fixed_size_binary().clone();
            (0..a.len()).map(move |i| a.value(i).to_vec())
        })
        .collect()
}

fn matching<'a>(paths: &'a [String], part: &str) -> Vec<&'a String> {
    paths.iter().filter(|p| p.contains(part)).collect()
}

/// `signal=<s>` and `date=<d>/hour=<h>` of an object path.
fn partition_of(path: &str) -> (String, String) {
    let parts: Vec<&str> = path.split('/').collect();
    assert_eq!(parts.len(), 6, "{path}");
    assert_eq!(parts[0], "v=1", "{path}");
    (parts[1].to_owned(), format!("{}/{}", parts[3], parts[4]))
}

/// Every series_id of every values file has a row in a series file of the same signal and the
/// same `date=/hour=` directory.
async fn assert_coverage(store: &dyn ObjectStore) {
    let mut series: BTreeMap<(String, String), HashSet<Vec<u8>>> = BTreeMap::new();
    let mut values: Vec<(String, HashSet<Vec<u8>>)> = Vec::new();
    for path in list_paths(store).await {
        let ids = series_ids(&batches(read(store, &path).await));
        if path.contains("/dataset=series/") {
            series.entry(partition_of(&path)).or_default().extend(ids);
        } else {
            assert!(path.contains("/dataset=values/"), "{path}");
            values.push((path, ids));
        }
    }
    assert!(!values.is_empty(), "no values file was written");
    for (path, ids) in values {
        let known = series.get(&partition_of(&path));
        assert!(
            known.is_some_and(|known| ids.is_subset(known)),
            "{path} has series without a series row in its partition"
        );
    }
}

/// Sizes of one request of `rows` sized log records extracted under the 1 MiB limits: the sum of
/// its chunk sizes, the size of its first chunk and the number of chunks.
fn request_sizes(rows: usize) -> (usize, usize, usize) {
    let dir = std::path::Path::new("/unused");
    let limits = LakeConfig::parse(&config(dir, one_mib()))
        .expect("config")
        .limits();
    let chunks = extract_logs(
        &to_otap(&sized_logs_request(rows, BODY, 0)),
        &Schemas::new(),
        &limits,
        "host.id",
    )
    .expect("the request fits ingress.max_extracted_bytes")
    .chunks;
    (
        chunks.iter().map(super::extract::Chunk::bytes).sum(),
        chunks[0].bytes(),
        chunks.len(),
    )
}

/// Precondition of the tests that use requests of `MEDIUM_ROWS`: two requests and the first chunk
/// of a third fit a 1 MiB block, three requests do not.
fn assert_medium_requests() {
    let (s, first, chunks) = request_sizes(MEDIUM_ROWS);
    assert!(
        chunks >= 2 && 2 * s + first <= MIB && 3 * s > MIB,
        "fixture drifted: request {s} bytes in {chunks} chunks, first chunk {first}"
    );
}

/// Precondition of the tests that use requests of `LARGE_ROWS`: two requests fit a 1 MiB block,
/// two requests and the first chunk of a third do not.
fn assert_large_requests() {
    let (s, first, chunks) = request_sizes(LARGE_ROWS);
    assert!(
        2 * s <= MIB && 2 * s + first > MIB,
        "fixture drifted: request {s} bytes in {chunks} chunks, first chunk {first}"
    );
}

/// Scenario: One logs request is flushed while the values object's put is held by a closed gate, which opens later.
/// Guarantees: While the values object is missing the series object may exist but nothing is acknowledged; the request is Acked exactly once after the values object lands.
#[test]
fn ack_only_after_values_object_lands() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults::gated(LOGS_VALUES);
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults.clone()));
    let runtime = TestRuntime::new();
    let (exp, _clock) = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
    let (f, s) = (faults.clone(), store.clone());
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(10, 2, 0)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(300)).await;
            let paths = list_paths(s.as_ref()).await;
            assert_eq!(paths.len(), 1, "only the series object: {paths:?}");
            assert!(paths[0].contains(LOGS_SERIES), "{paths:?}");
            assert_eq!(f.put_hashes(LOGS_VALUES).len(), 1, "values put is waiting");
            f.commit_gate.as_ref().expect("gate").add_permits(1);
            ctx.sleep(Duration::from_millis(300)).await;
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 1);
            assert!(out.nacks.is_empty(), "{:?}", out.nacks);
            assert_eq!(list_paths(store.as_ref()).await.len(), 2);
        });
}

/// Scenario: The first two puts of the values object fail, then storage recovers.
/// Guarantees: The request is Acked exactly once, every retry rewrites the same two object names with byte-identical content, and exactly two objects exist.
#[test]
fn retry_then_ack_exactly_once_same_names() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults {
        fail_puts: Faults::first(LOGS_VALUES, 2),
        ..Faults::default()
    };
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults.clone()));
    let runtime = TestRuntime::new();
    let (exp, _clock) = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(10, 2, 0)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(500)).await;
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 1);
            assert!(out.nacks.is_empty(), "{:?}", out.nacks);
            for part in [LOGS_SERIES, LOGS_VALUES] {
                let hashes = faults.put_hashes(part);
                assert_eq!(hashes.len(), 3, "{part}");
                assert!(hashes.iter().all(|h| *h == hashes[0]), "{part}");
            }
            assert_eq!(list_paths(store.as_ref()).await.len(), 2);
        });
}

/// Scenario: The values put is stored but its response is lost once.
/// Guarantees: probe_block finds the landed block, so the request is Acked once without resending the values object.
#[test]
fn ambiguous_values_put_acks_without_resend() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults {
        ambiguous_puts: Faults::first(LOGS_VALUES, 1),
        ..Faults::default()
    };
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults.clone()));
    let runtime = TestRuntime::new();
    let (exp, _clock) = exporter(&runtime, &config(dir.path(), json!({})), store);
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(10, 2, 0)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(400)).await;
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 1);
            assert!(out.nacks.is_empty(), "{:?}", out.nacks);
            assert_eq!(faults.put_hashes(LOGS_VALUES).len(), 1);
        });
}

/// Scenario: Eight requests share one generation and every put fails until the 300 ms flush deadline.
/// Guarantees: Each request gets exactly one retryable Nack (not a shutdown Nack) and no Ack; the terminal telemetry counts one failed block and no landed one.
#[test]
fn deadline_exceeded_nacks_block_batches_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults {
        fail_puts: Faults::first("", usize::MAX),
        ..Faults::default()
    };
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults));
    let runtime = TestRuntime::new();
    let cfg = config(
        dir.path(),
        json!({"window": {"flush_retry_deadline": "300ms"}}),
    );
    let (exp, _clock) = exporter_with(&runtime, &cfg, store, WIDE);
    let registry = runtime.metrics_registry();
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            for seed in 0..8 {
                ctx.send_pdata(subscribed(logs_payload(5, 1, seed)))
                    .await
                    .expect("send");
            }
            ctx.sleep(Duration::from_millis(1_000)).await;
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 0);
            assert_eq!(out.nacks.len(), 8, "{:?}", out.nacks);
            for (permanent, cause, reason) in &out.nacks {
                assert!(!permanent);
                assert_eq!(*cause, NackCause::Unspecified);
                assert!(reason.contains("block failed"), "{reason}");
            }
            let m = lake_metrics(&registry);
            assert_eq!(m.get("blocks.failed"), Some(&1), "{m:?}");
            assert_eq!(m.get("blocks.landed"), Some(&0), "{m:?}");
            assert_eq!(m.get("flushes.time"), Some(&1), "{m:?}");
        });
}

/// Scenario: With 1 MiB blocks and no window ending, three requests of about 360 KiB arrive: the first two and the first chunk of the third fill the block, so the generation rotates in the middle of the third request; a shutdown flushes the rest.
/// Guarantees: Before the shutdown only the first generation has landed; every block holding rows lands and each request is Acked exactly once, the third only after the second generation; the values files hold every row.
#[test]
fn batch_split_across_blocks_acks_after_all_land() {
    assert_medium_requests();
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), Faults::default()));
    let runtime = TestRuntime::new();
    let cfg = config(dir.path(), one_mib());
    let (exp, _clock) = exporter_with(&runtime, &cfg, store.clone(), NEVER);
    let s = store.clone();
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            for seed in 0..3 {
                ctx.send_pdata(subscribed(sized_logs_payload(MEDIUM_ROWS, BODY, seed)))
                    .await
                    .expect("send");
            }
            ctx.sleep(Duration::from_millis(500)).await;
            let paths = list_paths(s.as_ref()).await;
            assert_eq!(matching(&paths, LOGS_VALUES).len(), 1, "{paths:?}");
            ctx.send_shutdown(deadline_in(2_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 3);
            assert!(out.nacks.is_empty(), "{:?}", out.nacks);
            let paths = list_paths(store.as_ref()).await;
            let values = matching(&paths, LOGS_VALUES);
            assert_eq!(values.len(), 2, "{paths:?}");
            let mut total = 0;
            for p in values {
                total += rows_of(store.as_ref(), p).await;
            }
            assert_eq!(total, 3 * MEDIUM_ROWS);
            assert_coverage(store.as_ref()).await;
        });
}

/// Scenario: A logs request and a metrics request are open at shutdown; the metrics series put (flushed second) is held past the shutdown deadline.
/// Guarantees: The logs block lands and is Acked, and the metrics request gets exactly one retryable NodeShutdown Nack.
#[test]
fn shutdown_flushes_open_blocks_and_nacks_the_rest() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults::gated(METRICS_SERIES);
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults));
    let runtime = TestRuntime::new();
    let cfg = config(dir.path(), json!({}));
    let (exp, _clock) = exporter_with(&runtime, &cfg, store.clone(), NEVER);
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(10, 2, 0)))
                .await
                .expect("send");
            ctx.send_pdata(subscribed(metrics_payload()))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(100)).await;
            ctx.send_shutdown(deadline_in(500), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 1);
            assert_eq!(out.nacks.len(), 1, "{:?}", out.nacks);
            let (permanent, cause, _) = &out.nacks[0];
            assert!(!permanent);
            assert_eq!(*cause, NackCause::NodeShutdown);
            let paths = list_paths(store.as_ref()).await;
            assert_eq!(paths.len(), 2, "{paths:?}");
            assert!(paths.iter().all(|p| p.contains("/signal=logs/")));
        });
}

/// Scenario: A logs and a metrics request are open when a shutdown arrives whose deadline has already passed.
/// Guarantees: Both requests get exactly one NodeShutdown Nack without encoding or writing anything.
#[test]
fn shutdown_with_expired_deadline_nacks_without_encoding() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults::default();
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults.clone()));
    let runtime = TestRuntime::new();
    let cfg = config(dir.path(), json!({}));
    let (exp, _clock) = exporter_with(&runtime, &cfg, store.clone(), NEVER);
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(10, 2, 0)))
                .await
                .expect("send");
            ctx.send_pdata(subscribed(metrics_payload()))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(100)).await;
            ctx.send_shutdown(Instant::now(), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 0);
            assert_eq!(out.nacks.len(), 2, "{:?}", out.nacks);
            assert!(
                out.nacks
                    .iter()
                    .all(|(p, c, _)| !p && *c == NackCause::NodeShutdown)
            );
            assert!(faults.put_hashes("").is_empty());
            assert!(list_paths(store.as_ref()).await.is_empty());
        });
}

/// Scenario: A logs request and a metrics request are flushed when their window ends.
/// Guarantees: The four datasets are written under `v=1/signal=<s>/dataset=<d>/date=2026-09-30/hour=10/`; the files read back with exactly the schemas of the format (names, types, nullability); every values series_id has a series row in its partition; each written series_id is XXH3-128 of the written identity_bytes; files carry `format_version` 1.
#[test]
fn logs_and_metrics_write_four_datasets() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), Faults::default()));
    let runtime = TestRuntime::new();
    let (exp, _clock) = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(10, 2, 0)))
                .await
                .expect("send");
            ctx.send_pdata(subscribed(metrics_payload()))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(400)).await;
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 2);
            assert!(out.nacks.is_empty(), "{:?}", out.nacks);
            let paths = list_paths(store.as_ref()).await;
            assert_eq!(paths.len(), 4, "{paths:?}");
            let schemas = Schemas::new();
            for (name, signal) in [("logs", Signal::Logs), ("metrics", Signal::Metrics)] {
                for (dataset, schema) in [
                    ("series", schemas.series(signal)),
                    ("values", schemas.values(signal)),
                ] {
                    let dir = format!(
                        "v=1/signal={name}/dataset={dataset}/date=2026-09-30/hour=10/part-20260930T105950Z-writer-"
                    );
                    let found = matching(&paths, &dir);
                    assert_eq!(found.len(), 1, "{dir}: {paths:?}");
                    let file = read(store.as_ref(), found[0]).await;
                    let reader = SerializedFileReader::new(file.clone()).expect("parquet");
                    let kv = reader
                        .metadata()
                        .file_metadata()
                        .key_value_metadata()
                        .expect("kv")
                        .clone();
                    assert!(
                        kv.iter()
                            .any(|k| k.key == "format_version" && k.value.as_deref() == Some("1"))
                    );
                    let read_back = batches(file);
                    assert!(rows(&read_back) > 0, "{dir}");
                    for batch in &read_back {
                        assert_eq!(batch.schema().fields(), schema.fields(), "{dir}");
                        if dataset == "series" {
                            let ids = batch.column(0).as_fixed_size_binary();
                            let identity = batch
                                .column_by_name("identity_bytes")
                                .expect("identity_bytes")
                                .as_binary::<i32>();
                            for row in 0..batch.num_rows() {
                                assert_eq!(
                                    ids.value(row),
                                    xxh3_128(identity.value(row)).to_be_bytes()
                                );
                            }
                        }
                    }
                }
            }
            assert_coverage(store.as_ref()).await;
        });
}

/// Scenario: A traces payload is sent to the exporter.
/// Guarantees: It is refused with one permanent Nack carrying NackCause::Refused, nothing is written, and the unsupported-refusal counter is 1.
#[test]
fn traces_are_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), Faults::default()));
    let runtime = TestRuntime::new();
    let (exp, _clock) = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
    let registry = runtime.metrics_registry();
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(traces_payload()))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(50)).await;
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            let reason = refusal(&out);
            assert!(reason.contains("unsupported: traces"), "{reason}");
            assert!(list_paths(store.as_ref()).await.is_empty());
            let m = lake_metrics(&registry);
            assert_eq!(m.get("requests.refused.unsupported"), Some(&1), "{m:?}");
        });
}

/// Scenario: One logs request is buffered and its window ends, well before shutdown.
/// Guarantees: The end of the window rotates the generation without any engine timer tick: its objects exist before shutdown and the request is Acked.
#[test]
fn window_rotation_lands_block() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), Faults::default()));
    let runtime = TestRuntime::new();
    let (exp, _clock) = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
    let s = store.clone();
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(10, 2, 0)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(400)).await;
            assert_eq!(list_paths(s.as_ref()).await.len(), 2);
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 1);
            assert!(out.nacks.is_empty(), "{:?}", out.nacks);
        });
}

/// Scenario: With 1 MiB blocks, three requests of about 360 KiB arrive and every put fails; the first generation (requests 1 and 2 and the first chunk of request 3) fails at its 200 ms deadline, and the second generation (the rest of request 3) fails at shutdown.
/// Guarantees: Each of the three requests is resolved exactly once with one retryable Nack and no Ack; the later failure of the second generation adds no second completion for request 3.
#[test]
fn mid_batch_block_failure_nacks_once() {
    assert_medium_requests();
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults {
        fail_puts: Faults::first("", usize::MAX),
        ..Faults::default()
    };
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults.clone()));
    let runtime = TestRuntime::new();
    let mut extra = one_mib();
    merge(
        &mut extra,
        &json!({"window": {"flush_retry_deadline": "200ms"}}),
    );
    let cfg = config(dir.path(), extra);
    let (exp, _clock) = exporter_with(&runtime, &cfg, store.clone(), NEVER);
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            for seed in 0..3 {
                ctx.send_pdata(subscribed(sized_logs_payload(MEDIUM_ROWS, BODY, seed)))
                    .await
                    .expect("send");
            }
            ctx.sleep(Duration::from_millis(700)).await;
            ctx.send_shutdown(deadline_in(2_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 0);
            assert_eq!(out.nacks.len(), 3, "{:?}", out.nacks);
            for (permanent, cause, reason) in &out.nacks {
                assert!(!permanent);
                assert_eq!(*cause, NackCause::Unspecified);
                assert!(reason.contains("block failed"), "{reason}");
            }
            // Both generations were attempted under their own names.
            let names: HashSet<String> = faults
                .puts
                .lock()
                .expect("puts")
                .iter()
                .map(|(p, _)| p.clone())
                .collect();
            assert_eq!(names.len(), 2, "{names:?}");
            assert!(list_paths(store.as_ref()).await.is_empty());
        });
}

/// Scenario: A metrics payload has data points but no univariate metrics table (as in a multivariate payload).
/// Guarantees: It is refused with a permanent Nack instead of being acknowledged with nothing written.
#[test]
fn metrics_without_univariate_root_are_refused() {
    use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
    use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), Faults::default()));
    let runtime = TestRuntime::new();
    let (exp, _clock) = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
    let full = super::test_fixtures::metrics_otap(&super::test_fixtures::metrics_request());
    let OtapArrowRecords::Metrics(_) = &full else {
        panic!("metrics records");
    };
    let mut records = OtapArrowRecords::Metrics(Default::default());
    for t in [
        ArrowPayloadType::NumberDataPoints,
        ArrowPayloadType::NumberDpAttrs,
    ] {
        if let Some(b) = full.get(t) {
            records.set(t, b.clone()).expect("set");
        }
    }
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(OtapPayload::from_otap(records)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(50)).await;
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            let reason = refusal(&out);
            assert!(reason.contains("univariate"), "{reason}");
            assert!(list_paths(store.as_ref()).await.is_empty());
        });
}

/// Scenario: A logs request carries bytes that are not a valid OTLP request, and another carries a valid but empty request.
/// Guarantees: The malformed request is refused with a permanent Nack instead of being acknowledged with nothing written, while the valid empty request is still acknowledged.
#[test]
fn malformed_otlp_is_refused_but_empty_request_is_acked() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), Faults::default()));
    let runtime = TestRuntime::new();
    let (exp, _clock) = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            let junk = OtlpProtoBytes::ExportLogsRequest(Bytes::from_static(
                b"\x0a\x05hello-not-a-proto\xff\xff",
            ));
            ctx.send_pdata(subscribed(junk.into())).await.expect("send");
            let empty = OtlpProtoBytes::ExportLogsRequest(Bytes::new());
            ctx.send_pdata(subscribed(empty.into()))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(50)).await;
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 1);
            assert_eq!(out.nacks.len(), 1, "{:?}", out.nacks);
            let (permanent, cause, reason) = &out.nacks[0];
            assert!(permanent);
            assert_eq!(*cause, NackCause::Refused);
            assert!(reason.contains("malformed"), "{reason}");
            assert!(list_paths(store.as_ref()).await.is_empty());
        });
}

/// Scenario: A valid multi-record OTLP logs request is truncated near its end before extraction.
/// Guarantees: The strict pre-conversion decode refuses the truncated request; pdata's lenient
/// conversion would instead drop the records after the corruption and acknowledge a partial batch.
/// The intact request validates.
#[test]
fn truncated_otlp_request_is_refused_not_partially_accepted() {
    let bytes = logs_request(4, 1, 0).encode_to_vec();
    assert!(bytes.len() > 8);
    let truncated =
        OtlpProtoBytes::ExportLogsRequest(Bytes::copy_from_slice(&bytes[..bytes.len() - 4]));
    let err = super::validate_otlp_request(&truncated).expect_err("truncated is refused");
    assert!(matches!(err, super::LakeError::Conversion(_)), "{err}");
    super::validate_otlp_request(&OtlpProtoBytes::ExportLogsRequest(Bytes::from(bytes)))
        .expect("the intact request validates");
}

/// Scenario: An OTLP logs request carries one more attributed log record than the converter's
/// `u16` id space can address; a small request stays within it.
/// Guarantees: The oversized request is refused permanently with a reason naming the count,
/// instead of letting pdata wrap its ids in release builds and merge one record's attributes onto
/// another; the small request validates.
#[test]
fn otlp_logs_beyond_u16_attributed_records_are_refused() {
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, ResourceLogs, ScopeLogs,
    };
    let make = |n: u64| {
        let log_records = (0..n)
            .map(|i| LogRecord {
                time_unix_nano: i + 1,
                attributes: vec![kv("k", "v".to_owned())],
                ..Default::default()
            })
            .collect();
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        OtlpProtoBytes::ExportLogsRequest(Bytes::from(req.encode_to_vec()))
    };
    let over = super::MAX_U16_ENTRIES as u64 + 1;
    let err = super::validate_otlp_request(&make(over)).expect_err("too many attributed records");
    assert!(
        matches!(&err, super::LakeError::Invalid(m) if m.contains("log records with attributes")),
        "{err}"
    );
    super::validate_otlp_request(&make(8)).expect("a small request validates");
}

/// Scenario: A logs and a metrics request share one generation, and the logs values put is held by a closed gate that opens later.
/// Guarantees: The blocks of a generation are flushed one after the other, logs first: no metrics object appears while the logs block is held, and both requests are Acked after the gate opens.
#[test]
fn generation_flushes_logs_then_metrics() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults::gated(LOGS_VALUES);
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults.clone()));
    let runtime = TestRuntime::new();
    let cfg = config(dir.path(), json!({}));
    let (exp, _clock) = exporter_with(&runtime, &cfg, store.clone(), WIDE);
    let (f, s) = (faults.clone(), store.clone());
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(metrics_payload()))
                .await
                .expect("send");
            ctx.send_pdata(subscribed(logs_payload(10, 2, 0)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(600)).await;
            assert_eq!(f.put_hashes(LOGS_VALUES).len(), 1, "logs values put waits");
            let paths = list_paths(s.as_ref()).await;
            assert!(
                paths.iter().all(|p| p.contains("/signal=logs/")),
                "{paths:?}"
            );
            assert!(f.put_hashes("signal=metrics/").is_empty());
            f.commit_gate.as_ref().expect("gate").add_permits(8);
            ctx.sleep(Duration::from_millis(400)).await;
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 2);
            assert!(out.nacks.is_empty(), "{:?}", out.nacks);
            assert_eq!(list_paths(store.as_ref()).await.len(), 4);
        });
}

/// Scenario: Three logs requests of the same two resources are sent in three consecutive windows of one hour; then the wall clock moves into the next hour and a fourth request is sent.
/// Guarantees: The series rows are written once for the hour (one series file, two rows) and again, once, for the next hour; the second and third block write only a values file; every request is Acked; every values row has its series row in the same date/hour directory; the telemetry counts 4 landed blocks rotated by time, 4 series rows and 2 cached series.
#[test]
fn series_rows_are_written_once_per_hour() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), Faults::default()));
    let runtime = TestRuntime::new();
    let (exp, clock) = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
    let registry = runtime.metrics_registry();
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            for seed in 0..3 {
                ctx.send_pdata(subscribed(logs_payload(10, 2, seed)))
                    .await
                    .expect("send");
                ctx.sleep(Duration::from_millis(300)).await;
            }
            clock.advance(Duration::from_secs(20)); // 11:00:10
            ctx.send_pdata(subscribed(logs_payload(10, 2, 3)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(300)).await;
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 4);
            assert!(out.nacks.is_empty(), "{:?}", out.nacks);
            let paths = list_paths(store.as_ref()).await;
            let count = |part: &str, hour: &str| {
                paths
                    .iter()
                    .filter(|p| p.contains(part) && p.contains(hour))
                    .count()
            };
            assert_eq!(count(LOGS_SERIES, "hour=10"), 1, "{paths:?}");
            assert_eq!(count(LOGS_VALUES, "hour=10"), 3, "{paths:?}");
            assert_eq!(count(LOGS_SERIES, "hour=11"), 1, "{paths:?}");
            assert_eq!(count(LOGS_VALUES, "hour=11"), 1, "{paths:?}");
            for p in matching(&paths, LOGS_SERIES) {
                assert_eq!(rows_of(store.as_ref(), p).await, 2, "{p}");
            }
            assert_coverage(store.as_ref()).await;
            let m = lake_metrics(&registry);
            assert_eq!(m.get("blocks.landed"), Some(&4), "{m:?}");
            assert_eq!(m.get("flushes.time"), Some(&4), "{m:?}");
            assert_eq!(m.get("rows.written"), Some(&40), "{m:?}");
            assert_eq!(m.get("series_rows.written"), Some(&4), "{m:?}");
            assert_eq!(m.get("series_cache.entries"), Some(&2), "{m:?}");
            assert_eq!(m.get("admission.closed"), Some(&0), "{m:?}");
        });
}

/// Scenario: The values put of the first logs block is held by a closed gate. A second request arrives while it is held; then the gate opens.
/// Guarantees: The second request is admitted during the upload (its block is buffered, not refused), nothing lands while the first block is held, and both requests are Acked once after their own blocks land, the second block after the first.
#[test]
fn requests_are_admitted_while_an_upload_is_held() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults::gated(LOGS_VALUES);
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults.clone()));
    let runtime = TestRuntime::new();
    let (exp, _clock) = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
    let (f, s) = (faults.clone(), store.clone());
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(10, 2, 0)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(300)).await;
            assert_eq!(
                f.put_hashes(LOGS_VALUES).len(),
                1,
                "first values put is waiting"
            );
            // A different resource set, so the second block carries its own series rows.
            ctx.send_pdata(subscribed(logs_payload(10, 3, 1)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(300)).await;
            assert_eq!(
                f.put_hashes(LOGS_VALUES).len(),
                1,
                "second block waits for the slot"
            );
            assert!(matching(&list_paths(s.as_ref()).await, LOGS_VALUES).is_empty());
            f.commit_gate.as_ref().expect("gate").add_permits(8);
            ctx.sleep(Duration::from_millis(500)).await;
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 2);
            assert!(out.nacks.is_empty(), "{:?}", out.nacks);
            let paths = list_paths(store.as_ref()).await;
            assert_eq!(matching(&paths, LOGS_VALUES).len(), 2, "{paths:?}");
            assert_eq!(faults.put_hashes(LOGS_VALUES).len(), 2);
            assert_coverage(store.as_ref()).await;
        });
}

/// Scenario: Every put fails until the first generation gives up at its 200 ms deadline; then storage recovers and a second request of the same resources arrives.
/// Guarantees: The first request gets one retryable Nack and the second is Acked; the failed block did not mark its series as written, so the second block carries the series rows and every values row has its series row.
#[test]
fn failed_block_series_is_carried_by_the_next() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults {
        fail_puts: Faults::first("", usize::MAX),
        ..Faults::default()
    };
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults.clone()));
    let runtime = TestRuntime::new();
    let cfg = config(
        dir.path(),
        json!({"window": {"flush_retry_deadline": "200ms"}}),
    );
    let (exp, _clock) = exporter(&runtime, &cfg, store.clone());
    let (f, s) = (faults.clone(), store.clone());
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(10, 2, 0)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(600)).await;
            assert!(list_paths(s.as_ref()).await.is_empty());
            // Storage recovers.
            f.fail_puts
                .as_ref()
                .expect("fault")
                .1
                .store(0, Ordering::SeqCst);
            ctx.send_pdata(subscribed(logs_payload(10, 2, 1)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(400)).await;
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 1);
            assert_eq!(out.nacks.len(), 1, "{:?}", out.nacks);
            let (permanent, cause, reason) = &out.nacks[0];
            assert!(!permanent);
            assert_eq!(*cause, NackCause::Unspecified);
            assert!(reason.contains("block failed"), "{reason}");
            let paths = list_paths(store.as_ref()).await;
            let series = matching(&paths, LOGS_SERIES);
            assert_eq!(series.len(), 1, "{paths:?}");
            assert_eq!(matching(&paths, LOGS_VALUES).len(), 1, "{paths:?}");
            assert_eq!(rows_of(store.as_ref(), series[0]).await, 2);
            assert_coverage(store.as_ref()).await;
        });
}

/// Scenario: With 1 MiB blocks, no window ending and every values put held forever, requests of about 440 KiB arrive: 1 and 2 fill the first generation, 3 rotates it into the flush slot and starts the next, 4 joins it, 5 does not fit while the slot is busy, so it is parked and admission closes; request 6 stays in the channel. Then a shutdown with a 400 ms deadline arrives.
/// Guarantees: The shutdown is handled although admission is closed and the exporter returns by its deadline. Every request gets exactly one Nack and no Ack: request 6 a plain retryable Nack (admission is closed), requests 1 to 5 a NodeShutdown Nack. No values object exists.
#[test]
fn control_flows_while_admission_is_closed() {
    assert_large_requests();
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults::gated(LOGS_VALUES);
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults.clone()));
    let runtime = TestRuntime::new();
    let cfg = config(dir.path(), one_mib());
    let (exp, _clock) = exporter_with(&runtime, &cfg, store.clone(), NEVER);
    let stopped_at = Arc::new(OnceLock::new());
    let (f, at) = (faults.clone(), stopped_at.clone());
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            for seed in 0..5 {
                ctx.send_pdata(subscribed(sized_logs_payload(LARGE_ROWS, BODY, seed)))
                    .await
                    .expect("send");
            }
            ctx.sleep(Duration::from_millis(300)).await;
            assert_eq!(
                f.put_hashes(LOGS_VALUES).len(),
                1,
                "only the first generation is in the slot"
            );
            ctx.send_pdata(subscribed(sized_logs_payload(LARGE_ROWS, BODY, 5)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(100)).await;
            let _ = at.set(Instant::now());
            ctx.send_shutdown(deadline_in(400), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let took = stopped_at.get().expect("shutdown was sent").elapsed();
            assert!(took < Duration::from_secs(2), "shutdown took {took:?}");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 0);
            assert_eq!(out.nacks.len(), 6, "{:?}", out.nacks);
            assert!(out.nacks.iter().all(|(permanent, _, _)| !permanent));
            let shutdown = out
                .nacks
                .iter()
                .filter(|(_, cause, _)| *cause == NackCause::NodeShutdown)
                .count();
            assert_eq!(shutdown, 5, "{:?}", out.nacks);
            let closed: Vec<_> = out
                .nacks
                .iter()
                .filter(|(_, cause, _)| *cause == NackCause::Unspecified)
                .collect();
            assert_eq!(closed.len(), 1, "{:?}", out.nacks);
            assert!(
                closed[0].2.contains("admission is closed"),
                "{}",
                closed[0].2
            );
            let paths = list_paths(store.as_ref()).await;
            assert!(matching(&paths, LOGS_VALUES).is_empty(), "{paths:?}");
            assert_eq!(faults.put_hashes(LOGS_VALUES).len(), 1);
        });
}

/// Scenario: The same five requests of about 440 KiB arrive while the values put of the first generation is held; after request 5 is parked the gate opens, and a shutdown follows.
/// Guarantees: Each request is Acked exactly once. The parked request enters the next generation once the slot frees: there are three generations holding requests 1 and 2, requests 3 and 4, and request 5, in that order; the telemetry counts two generations rotated by bytes, one by the shutdown, three landed blocks, 265 rows and one series row.
#[test]
fn parked_request_lands_after_the_slot_frees() {
    assert_large_requests();
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults::gated(LOGS_VALUES);
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults.clone()));
    let runtime = TestRuntime::new();
    let cfg = config(dir.path(), one_mib());
    let (exp, _clock) = exporter_with(&runtime, &cfg, store.clone(), NEVER);
    let registry = runtime.metrics_registry();
    let f = faults.clone();
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            for seed in 0..5 {
                ctx.send_pdata(subscribed(sized_logs_payload(LARGE_ROWS, BODY, seed)))
                    .await
                    .expect("send");
            }
            ctx.sleep(Duration::from_millis(300)).await;
            assert_eq!(f.put_hashes(LOGS_VALUES).len(), 1);
            f.commit_gate.as_ref().expect("gate").add_permits(16);
            ctx.sleep(Duration::from_millis(500)).await;
            ctx.send_shutdown(deadline_in(2_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 5);
            assert!(out.nacks.is_empty(), "{:?}", out.nacks);
            let paths = list_paths(store.as_ref()).await;
            let values = matching(&paths, LOGS_VALUES);
            assert_eq!(values.len(), 3, "{paths:?}");
            // Object names sort by generation sequence number.
            for (seq, (path, want)) in values
                .iter()
                .zip([2 * LARGE_ROWS, 2 * LARGE_ROWS, LARGE_ROWS])
                .enumerate()
            {
                assert!(path.ends_with(&format!("-{seq:08}.parquet")), "{path}");
                assert_eq!(rows_of(store.as_ref(), path).await, want, "{path}");
            }
            assert_coverage(store.as_ref()).await;
            let m = lake_metrics(&registry);
            assert_eq!(m.get("flushes.bytes"), Some(&2), "{m:?}");
            assert_eq!(m.get("flushes.shutdown"), Some(&1), "{m:?}");
            assert_eq!(m.get("flushes.time"), Some(&0), "{m:?}");
            assert_eq!(m.get("blocks.landed"), Some(&3), "{m:?}");
            assert_eq!(
                m.get("rows.written"),
                Some(&(5 * LARGE_ROWS as u64)),
                "{m:?}"
            );
            assert_eq!(m.get("series_rows.written"), Some(&1), "{m:?}");
        });
}

/// Scenario: The first put panics inside the flush task; a second request follows.
/// Guarantees: The panic is contained in the task: the first request gets one retryable Nack naming the failed flush task, the node keeps running, the second request is Acked and the node terminates cleanly.
#[test]
fn flush_task_panic_nacks_the_block_and_the_node_continues() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults {
        panic_puts: Faults::first("", 1),
        ..Faults::default()
    };
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults));
    let runtime = TestRuntime::new();
    let (exp, _clock) = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(10, 2, 0)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(300)).await;
            ctx.send_pdata(subscribed(logs_payload(10, 2, 1)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(400)).await;
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 1);
            assert_eq!(out.nacks.len(), 1, "{:?}", out.nacks);
            let (permanent, cause, reason) = &out.nacks[0];
            assert!(!permanent);
            assert_eq!(*cause, NackCause::Unspecified);
            assert!(reason.contains("flush task failed"), "{reason}");
            assert_coverage(store.as_ref()).await;
        });
}

/// Scenario: `ingress.max_request_bytes` is 4096 and a request of about 100 KiB arrives.
/// Guarantees: The request gets one permanent Refused Nack whose reason names the setting, the observed size and the limit; nothing is written.
#[test]
fn oversized_request_is_refused_with_the_setting() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults::default();
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults.clone()));
    let runtime = TestRuntime::new();
    let cfg = config(dir.path(), json!({"ingress": {"max_request_bytes": 4096}}));
    let (exp, _clock) = exporter(&runtime, &cfg, store.clone());
    let registry = runtime.metrics_registry();
    let mut pdata = subscribed(sized_logs_payload(12, BODY, 0));
    let observed = pdata.num_bytes().expect("OTLP bytes have a size");
    assert!(observed > 90 * 1024, "{observed}");
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(pdata).await.expect("send");
            ctx.sleep(Duration::from_millis(200)).await;
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            let reason = refusal(&out);
            assert!(reason.contains("ingress.max_request_bytes"), "{reason}");
            assert!(reason.contains(&format!("{observed} bytes")), "{reason}");
            assert!(reason.contains("(4096 bytes)"), "{reason}");
            assert!(faults.put_hashes("").is_empty());
            assert!(list_paths(store.as_ref()).await.is_empty());
            let m = lake_metrics(&registry);
            assert_eq!(m.get("requests.refused.too_large"), Some(&1), "{m:?}");
        });
}

/// Scenario: A logs request whose resource has the key `dup` twice arrives as OTLP bytes.
/// Guarantees: The whole request gets one permanent Refused Nack naming the rule (duplicate attribute key); nothing is written.
#[test]
fn invalid_content_is_refused_permanently() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), Faults::default()));
    let runtime = TestRuntime::new();
    let (exp, _clock) = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
    let registry = runtime.metrics_registry();
    let mut req = logs_request(10, 2, 0);
    let attributes = &mut req.resource_logs[0]
        .resource
        .as_mut()
        .expect("resource")
        .attributes;
    attributes.push(kv("dup", "a".into()));
    attributes.push(kv("dup", "z".into()));
    let payload: OtapPayload =
        OtlpProtoBytes::ExportLogsRequest(Bytes::from(req.encode_to_vec())).into();
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(payload)).await.expect("send");
            ctx.sleep(Duration::from_millis(200)).await;
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            let reason = refusal(&out);
            assert!(reason.contains("duplicate attribute key"), "{reason}");
            assert!(list_paths(store.as_ref()).await.is_empty());
            let m = lake_metrics(&registry);
            assert_eq!(m.get("requests.refused.invalid"), Some(&1), "{m:?}");
        });
}

/// Scenario: `ingress.max_nesting_depth` is 2 and a log attribute holds an array nested 3 deep.
/// Guarantees: The request gets one permanent Refused Nack naming `ingress.max_nesting_depth` and the limit; nothing is written.
#[test]
fn nested_value_beyond_the_depth_limit_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), Faults::default()));
    let runtime = TestRuntime::new();
    let cfg = config(dir.path(), json!({"ingress": {"max_nesting_depth": 2}}));
    let (exp, _clock) = exporter(&runtime, &cfg, store.clone());
    let registry = runtime.metrics_registry();
    let array = |values: Vec<AnyValue>| AnyValue {
        value: Some(any_value::Value::ArrayValue(ArrayValue { values })),
    };
    let one = AnyValue {
        value: Some(any_value::Value::IntValue(1)),
    };
    let mut req = logs_request(1, 1, 0);
    req.resource_logs[0].scope_logs[0].log_records[0]
        .attributes
        .push(KeyValue {
            key: "nested".into(),
            value: Some(array(vec![array(vec![array(vec![one])])])),
        });
    let payload: OtapPayload =
        OtlpProtoBytes::ExportLogsRequest(Bytes::from(req.encode_to_vec())).into();
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(payload)).await.expect("send");
            ctx.sleep(Duration::from_millis(200)).await;
            ctx.send_shutdown(deadline_in(1_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            let reason = refusal(&out);
            assert!(reason.contains("ingress.max_nesting_depth"), "{reason}");
            assert!(reason.contains("(2 levels)"), "{reason}");
            assert!(list_paths(store.as_ref()).await.is_empty());
            let m = lake_metrics(&registry);
            assert_eq!(m.get("requests.refused.too_deep"), Some(&1), "{m:?}");
        });
}

/// Admit `payload` and push its chunks, as `handle_pdata` does after the admission check.
fn admit(exp: &mut ParquetLakeExporter, st: &mut State, payload: OtapPayload) {
    let mut token = OtapPdata::new_default(payload);
    let (signal, extracted) = exp.extract(&mut token, &st.schemas).expect("extract");
    let seq = st.pending.admit(token, Instant::now());
    let _ = exp.push_chunks(st, seq, signal, extracted.chunks.into());
}

/// Scenario: The loop state takes each combination of a parked request, a due rotation and a busy flush slot.
/// Guarantees: Pdata is read unless a request is parked, or a rotation is due while the slot is busy; a busy slot alone, or a due rotation with a free slot, does not close admission.
#[tokio::test]
async fn accepting_rule() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> =
        Arc::new(TestStore::new(dir.path(), Faults::gated(LOGS_VALUES)));
    let (mut exp, _clock) = bare_exporter(&config(dir.path(), json!({})), store.clone(), NEVER);
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut st = exp.new_state(store);
            let parked = || Parked {
                seq: 99,
                signal: Signal::Logs,
                chunks: VecDeque::new(),
            };
            assert!(st.accepting(), "idle");
            st.rotation_due = Some(FlushReason::Time);
            assert!(st.accepting(), "rotation due, slot free");
            st.parked = Some(parked());
            assert!(!st.accepting(), "parked, slot free");
            st.parked = None;

            // Put a generation into the flush slot; the gate keeps it there.
            admit(&mut exp, &mut st, logs_payload(1, 1, 0));
            let _ = exp.advance(&mut st);
            assert!(st.flushing.is_some() && st.rotation_due.is_none());
            assert!(st.active.is_empty());
            assert!(st.accepting(), "slot busy, nothing due");
            st.rotation_due = Some(FlushReason::Bytes);
            assert!(!st.accepting(), "rotation due, slot busy");
            st.rotation_due = None;
            st.parked = Some(parked());
            assert!(!st.accepting(), "parked, slot busy");
        })
        .await;
}

/// Scenario: With 1 MiB blocks and a held upload, the five requests of about 440 KiB of the node test are admitted one after the other; then telemetry is observed as the CollectTelemetry handler does while admission is closed.
/// Guarantees: ACTIVE never holds more than `window.max_block_bytes` (plus allocation rounding). After the fifth request admission is closed, exactly one generation is in the flush slot, and the parked chunks measure at most `ingress.max_extracted_bytes`: the ACTIVE, FLUSHING and parked terms of the flush memory bound hold. The observed telemetry reports admission closed and one generation rotated by bytes.
#[tokio::test]
async fn closed_admission_holds_the_bounded_terms() {
    assert_large_requests();
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> =
        Arc::new(TestStore::new(dir.path(), Faults::gated(LOGS_VALUES)));
    let (mut exp, _clock) = bare_exporter(&config(dir.path(), one_mib()), store.clone(), NEVER);
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut st = exp.new_state(store);
            for seed in 0..5 {
                assert!(st.accepting(), "request {seed} is read");
                admit(
                    &mut exp,
                    &mut st,
                    sized_logs_payload(LARGE_ROWS, BODY, seed),
                );
                let _ = exp.advance(&mut st);
                assert!(
                    st.active.bytes() <= MIB + 4096,
                    "ACTIVE holds {} bytes after request {seed}",
                    st.active.bytes()
                );
            }
            assert!(!st.accepting());
            let flushing = st.flushing.as_ref().expect("a generation is in the slot");
            assert!(flushing.queued.is_empty());
            assert_eq!(flushing.current.id.seq, 0);
            assert_eq!(st.next_seq, 1, "the second generation waits in ACTIVE");
            let parked = st.parked.as_ref().expect("the fifth request is parked");
            let parked_bytes: usize = parked.chunks.iter().map(super::extract::Chunk::bytes).sum();
            assert!(parked_bytes > 0 && parked_bytes <= MIB, "{parked_bytes}");
            assert!(st.active.bytes() > MIB / 2);

            // What the CollectTelemetry handler reports in this state.
            let accepting = st.accepting();
            exp.track_admission(&mut st, accepting);
            exp.observe(&mut st);
            let m = exp.metrics.as_ref().expect("metrics");
            assert_eq!(m.admission_closed.get(), 1);
            assert_eq!(m.flushes_bytes.get(), 1);
            assert_eq!(m.cache_entries.get(), 0, "nothing landed yet");
        })
        .await;
}
