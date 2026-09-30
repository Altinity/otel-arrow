// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Node-level tests of the Parquet lake exporter: ack-after-land, idempotent retries, deadline and
//! shutdown Nacks, flush triggers and the four datasets.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, FixedSizeBinaryArray, RecordBatch};
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::Interests;
use otel_arrow_dfe_engine::control::{NackCause, PipelineCompletionMsg};
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::testing::exporter::{
    TestContext, TestRuntime, create_test_pipeline_context,
};
use otel_arrow_dfe_engine::testing::test_node;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_otap::testing::{TestCallData, next_ack, next_nack};
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::trace::v1::ExportTraceServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::trace::v1::{ResourceSpans, ScopeSpans, Span};
use otel_arrow_dfe_pdata::{OtapPayload, OtlpProtoBytes};
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::file::reader::{FileReader, SerializedFileReader};
use prost::Message as _;
use serde_json::{Value, json};

use super::test_fixtures::{logs_payload, metrics_payload};
use super::test_store::{Faults, TestStore, list_paths};
use super::{PARQUET_LAKE_EXPORTER_URN, ParquetLakeExporter};

/// Node id used for the Ack/Nack subscription of test pdata.
const NODE: usize = 4242;

/// Config: blocks flush on the first tick after 20 ms; `extra` overrides fields.
fn config(dir: &std::path::Path, extra: Value) -> Value {
    let mut cfg = json!({
        "storage": {"file": {"base_uri": dir.to_str().expect("utf8 path")}},
        "max_block_age": "20ms",
        "check_interval": "20ms",
        "upload_deadline": "5s",
        "retry_initial_backoff": "5ms",
        "retry_max_backoff": "20ms",
    });
    if let (Some(cfg), Some(extra)) = (cfg.as_object_mut(), extra.as_object()) {
        for (k, v) in extra {
            let _ = cfg.insert(k.clone(), v.clone());
        }
    }
    cfg
}

fn exporter(
    runtime: &TestRuntime<OtapPdata>,
    config: &Value,
    store: Arc<dyn ObjectStore>,
) -> ExporterWrapper<OtapPdata> {
    let pipeline = create_test_pipeline_context();
    let mut exporter = ParquetLakeExporter::from_config(&pipeline, config).expect("valid config");
    exporter.store_override = Some(store);
    ExporterWrapper::local(
        exporter,
        test_node(runtime.config().name.clone()),
        Arc::new(NodeUserConfig::new_exporter_config(
            PARQUET_LAKE_EXPORTER_URN,
        )),
        runtime.config(),
    )
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

fn series_ids(batches: &[RecordBatch]) -> HashSet<Vec<u8>> {
    batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("series_id")
                .clone();
            (0..a.len()).map(move |i| a.value(i).to_vec())
        })
        .collect()
}

/// Scenario: One logs batch is flushed while the values object's put is held by a closed gate, which opens later.
/// Guarantees: While the values object is missing the series object may exist but nothing is acknowledged; the batch is Acked exactly once after the values object lands.
#[test]
fn ack_only_after_values_object_lands() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults::gated("logs/values/");
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults.clone()));
    let runtime = TestRuntime::new();
    let exp = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
    let (f, s) = (faults.clone(), store.clone());
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(10, 2, 0)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(60)).await;
            ctx.send_timer_tick().await.expect("tick");
            ctx.sleep(Duration::from_millis(300)).await;
            let paths = list_paths(s.as_ref()).await;
            assert_eq!(paths.len(), 1, "only the series object: {paths:?}");
            assert!(paths[0].starts_with("logs/series/"));
            assert_eq!(
                f.put_hashes("logs/values/").len(),
                1,
                "values put is waiting"
            );
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
/// Guarantees: The batch is Acked exactly once, every retry rewrites the same two object names with byte-identical content, and exactly two objects exist.
#[test]
fn retry_then_ack_exactly_once_same_names() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults {
        fail_puts: Faults::first("logs/values/", 2),
        ..Faults::default()
    };
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults.clone()));
    let runtime = TestRuntime::new();
    let exp = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(10, 2, 0)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(60)).await;
            ctx.send_timer_tick().await.expect("tick");
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
            for part in ["logs/series/", "logs/values/"] {
                let hashes = faults.put_hashes(part);
                assert_eq!(hashes.len(), 3, "{part}");
                assert!(hashes.iter().all(|h| *h == hashes[0]), "{part}");
            }
            assert_eq!(list_paths(store.as_ref()).await.len(), 2);
        });
}

/// Scenario: The values put is stored but its response is lost once.
/// Guarantees: probe_block finds the landed block, so the batch is Acked once without resending the values object.
#[test]
fn ambiguous_values_put_acks_without_resend() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults {
        ambiguous_puts: Faults::first("logs/values/", 1),
        ..Faults::default()
    };
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults.clone()));
    let runtime = TestRuntime::new();
    let exp = exporter(&runtime, &config(dir.path(), json!({})), store);
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(10, 2, 0)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(60)).await;
            ctx.send_timer_tick().await.expect("tick");
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
            assert_eq!(faults.put_hashes("logs/values/").len(), 1);
        });
}

/// Scenario: Eight batches share one block and every put fails until the 300 ms upload deadline.
/// Guarantees: Each batch gets exactly one retryable Nack (not a shutdown Nack) and no Ack.
#[test]
fn deadline_exceeded_nacks_block_batches_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults {
        fail_puts: Faults::first("", usize::MAX),
        ..Faults::default()
    };
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults));
    let runtime = TestRuntime::new();
    let cfg = config(dir.path(), json!({"upload_deadline": "300ms"}));
    let exp = exporter(&runtime, &cfg, store);
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            for seed in 0..8 {
                ctx.send_pdata(subscribed(logs_payload(5, 1, seed)))
                    .await
                    .expect("send");
            }
            ctx.sleep(Duration::from_millis(60)).await;
            ctx.send_timer_tick().await.expect("tick");
            ctx.sleep(Duration::from_millis(700)).await;
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
        });
}

/// Scenario: One logs batch is larger than max_block_bytes (1 MiB), so its chunks span several blocks.
/// Guarantees: Every block holding its rows lands (several values objects) and the batch is Acked exactly once, after the last block.
#[test]
fn batch_split_across_blocks_acks_after_all_land() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), Faults::default()));
    let runtime = TestRuntime::new();
    let cfg = config(
        dir.path(),
        json!({"max_block_bytes": 1_048_576, "max_block_age": "1h", "check_interval": "1s"}),
    );
    let exp = exporter(&runtime, &cfg, store.clone());
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(8_000, 20, 0)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(500)).await;
            ctx.send_shutdown(deadline_in(5_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 1);
            assert!(out.nacks.is_empty(), "{:?}", out.nacks);
            let paths = list_paths(store.as_ref()).await;
            let values = paths
                .iter()
                .filter(|p| p.starts_with("logs/values/"))
                .count();
            assert!(values > 1, "{paths:?}");
            let mut rows = 0;
            for p in paths.iter().filter(|p| p.starts_with("logs/values/")) {
                rows += batches(read(store.as_ref(), p).await)
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>();
            }
            assert_eq!(rows, 8_000);
        });
}

/// Scenario: A logs batch and a metrics batch are open at shutdown; the metrics series put (flushed second) is held past the shutdown deadline.
/// Guarantees: The logs block lands and is Acked, and the metrics batch gets exactly one retryable NodeShutdown Nack.
#[test]
fn shutdown_flushes_open_blocks_and_nacks_the_rest() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults::gated("metrics/series/");
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults));
    let runtime = TestRuntime::new();
    let cfg = config(
        dir.path(),
        json!({"max_block_age": "1h", "check_interval": "1s"}),
    );
    let exp = exporter(&runtime, &cfg, store.clone());
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
            assert!(paths.iter().all(|p| p.starts_with("logs/")));
        });
}

/// Scenario: A logs and a metrics batch are open when a shutdown arrives whose deadline has already passed.
/// Guarantees: Both batches get exactly one NodeShutdown Nack without encoding or writing anything.
#[test]
fn shutdown_with_expired_deadline_nacks_without_encoding() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults::default();
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults.clone()));
    let runtime = TestRuntime::new();
    let cfg = config(
        dir.path(),
        json!({"max_block_age": "1h", "check_interval": "1s"}),
    );
    let exp = exporter(&runtime, &cfg, store.clone());
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

/// Scenario: A logs batch and a metrics batch are flushed by age.
/// Guarantees: The four datasets (logs/metrics x series/values) are written, every values series_id has a series row in the same block, and files carry the format metadata.
#[test]
fn logs_and_metrics_write_four_datasets() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), Faults::default()));
    let runtime = TestRuntime::new();
    let exp = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(10, 2, 0)))
                .await
                .expect("send");
            ctx.send_pdata(subscribed(metrics_payload()))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(60)).await;
            ctx.send_timer_tick().await.expect("tick");
            ctx.sleep(Duration::from_millis(300)).await;
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
            for signal in ["logs", "metrics"] {
                let series_path = paths
                    .iter()
                    .find(|p| p.starts_with(&format!("{signal}/series/dt=")))
                    .expect("series file");
                let values_path = series_path.replace("/series/", "/values/");
                assert!(paths.contains(&values_path), "{values_path}");
                let series_file = read(store.as_ref(), series_path).await;
                let values_file = read(store.as_ref(), &values_path).await;
                let series = series_ids(&batches(series_file.clone()));
                assert!(series_ids(&batches(values_file)).is_subset(&series));
                let reader = SerializedFileReader::new(series_file).expect("parquet");
                let kv = reader
                    .metadata()
                    .file_metadata()
                    .key_value_metadata()
                    .expect("kv")
                    .clone();
                assert!(kv.iter().any(
                    |k| k.key == "otel.lake.format_version" && k.value.as_deref() == Some("1")
                ));
            }
        });
}

/// Scenario: A traces payload is sent to the exporter.
/// Guarantees: It is refused with one permanent Nack carrying NackCause::Refused, and nothing is written.
#[test]
fn traces_are_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), Faults::default()));
    let runtime = TestRuntime::new();
    let exp = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
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
            assert_eq!(out.acks, 0);
            assert_eq!(out.nacks.len(), 1);
            let (permanent, cause, reason) = &out.nacks[0];
            assert!(permanent);
            assert_eq!(*cause, NackCause::Refused);
            assert!(reason.contains("traces"), "{reason}");
            assert!(list_paths(store.as_ref()).await.is_empty());
        });
}

/// Scenario: A block is older than max_block_age when a timer tick arrives, well before shutdown.
/// Guarantees: The tick flushes the block (its objects exist before shutdown) and the batch is Acked.
#[test]
fn age_flush_lands_block() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), Faults::default()));
    let runtime = TestRuntime::new();
    let exp = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
    let s = store.clone();
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(10, 2, 0)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(60)).await;
            ctx.send_timer_tick().await.expect("tick");
            ctx.sleep(Duration::from_millis(300)).await;
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

/// Scenario: One batch is larger than max_block_bytes and every put fails, so the block flushed in the middle of the batch fails before its remaining chunks are pushed.
/// Guarantees: The batch is resolved exactly once with one retryable Nack and no Ack, and the rest of the batch is not pushed into a new block.
#[test]
fn mid_batch_block_failure_nacks_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let faults = Faults {
        fail_puts: Faults::first("", usize::MAX),
        ..Faults::default()
    };
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), faults.clone()));
    let runtime = TestRuntime::new();
    let cfg = config(
        dir.path(),
        json!({
            "max_block_bytes": 1_048_576,
            "max_block_age": "1h",
            "check_interval": "1s",
            "upload_deadline": "100ms"
        }),
    );
    let exp = exporter(&runtime, &cfg, store);
    runtime
        .set_exporter(exp)
        .run_test(move |ctx| async move {
            ctx.send_pdata(subscribed(logs_payload(8_000, 20, 0)))
                .await
                .expect("send");
            ctx.sleep(Duration::from_millis(600)).await;
            ctx.send_shutdown(deadline_in(2_000), "test")
                .await
                .expect("shutdown");
        })
        .run_validation(move |mut ctx, result| async move {
            result.expect("exporter terminates cleanly");
            let out = drain(&mut ctx);
            assert_eq!(out.acks, 0);
            assert_eq!(out.nacks.len(), 1, "{:?}", out.nacks);
            let (permanent, cause, _) = &out.nacks[0];
            assert!(!permanent);
            assert_eq!(*cause, NackCause::Unspecified);
            // Only the first block was attempted: the break stops pushing the Nacked batch.
            let names: HashSet<String> = faults
                .puts
                .lock()
                .expect("puts")
                .iter()
                .map(|(p, _)| p.clone())
                .collect();
            assert_eq!(names.len(), 1, "{names:?}");
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
    let exp = exporter(&runtime, &config(dir.path(), json!({})), store);
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
            assert_eq!(out.acks, 0);
            assert_eq!(out.nacks.len(), 1, "{:?}", out.nacks);
            assert!(out.nacks[0].0);
            assert_eq!(out.nacks[0].1, NackCause::Refused);
        });
}

/// Scenario: A logs batch carries bytes that are not a valid OTLP request, and another carries a valid but empty request.
/// Guarantees: The malformed batch is refused with a permanent Nack instead of being acknowledged with nothing written, while the valid empty request is still acknowledged.
#[test]
fn malformed_otlp_is_refused_but_empty_request_is_acked() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(TestStore::new(dir.path(), Faults::default()));
    let runtime = TestRuntime::new();
    let exp = exporter(&runtime, &config(dir.path(), json!({})), store.clone());
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

/// Scenario: Both signal blocks are past max_block_age, the logs block older than the metrics block; then only the metrics block is.
/// Guarantees: A timer tick picks exactly one block, the oldest due one, so the node never runs two uploads before reading its inbox again.
#[test]
fn one_flush_per_tick_picks_the_oldest_due_block() {
    use super::block::Block;
    use super::due_signal;
    use super::extract::extract_logs;
    use super::schema::Schemas;
    use super::test_fixtures::{logs_request, to_otap};
    let chunk = || {
        extract_logs(&to_otap(&logs_request(2, 1, 0)), &Schemas::new(), 1 << 30)
            .expect("extract")
            .remove(0)
    };
    let t0 = Instant::now();
    let (mut logs, mut metrics) = (Block::default(), Block::default());
    let _ = logs.push(chunk(), 1, t0).expect("push");
    let _ = metrics
        .push(chunk(), 2, t0 + Duration::from_millis(10))
        .expect("push");
    let age = Duration::from_millis(20);
    assert_eq!(
        due_signal(&logs, &metrics, t0 + Duration::from_millis(5), age),
        None
    );
    assert_eq!(
        due_signal(&logs, &metrics, t0 + Duration::from_millis(40), age),
        Some(super::Signal::Logs)
    );
    let _ = logs.take();
    assert_eq!(
        due_signal(&logs, &metrics, t0 + Duration::from_millis(40), age),
        Some(super::Signal::Metrics)
    );
}
