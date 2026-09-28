// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared test support: request builders, the simulated clocks, the
//! gated and fault-injecting stores the store override takes, and the
//! event capture.

pub(super) use super::super::config::Config;

pub(super) use super::super::metrics::LateCommit;

pub(super) use super::super::outcome::{Outcome, WriteFailure};

pub(super) use super::super::token::{AckToken, Notifier};

pub(super) use super::super::worker::{Prepared, Worker};

pub(super) use otel_arrow_dfe_series_lake::hook_store::{HookGuard, HookStore, StoreHooks};

pub(super) use std::sync::atomic::AtomicUsize;

pub(super) use std::sync::atomic::Ordering::SeqCst;

pub(super) use object_store::{
    MultipartUpload, ObjectStore, ObjectStoreExt, PutPayload, PutResult, UploadPart,
};

pub(super) use otel_arrow_dfe_channel::mpsc;

pub(super) use otel_arrow_dfe_config::SignalType;

pub(super) use otel_arrow_dfe_engine::Interests;

pub(super) use otel_arrow_dfe_engine::clock;

pub(super) use otel_arrow_dfe_engine::control::NackCause;

pub(super) use otel_arrow_dfe_engine::control::NodeControlMsg;

pub(super) use otel_arrow_dfe_engine::control::{
    PipelineCompletionMsg, PipelineCompletionMsgReceiver, pipeline_completion_msg_channel,
};

pub(super) use otel_arrow_dfe_engine::local::exporter::EffectHandler;

pub(super) use otel_arrow_dfe_engine::local::exporter::Exporter;

pub(super) use otel_arrow_dfe_engine::local::message::LocalReceiver;

pub(super) use otel_arrow_dfe_engine::message::{ExporterInbox, Message, Receiver};

pub(super) use otel_arrow_dfe_engine::testing::{test_node, test_pipeline_runtime_services};

pub(super) use otel_arrow_dfe_otap::pdata::{Context, OtapPdata};

pub(super) use otel_arrow_dfe_pdata::OtapPayload;

pub(super) use otel_arrow_dfe_pdata::encode::{encode_logs_otap_batch, encode_metrics_otap_batch};

pub(super) use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;

pub(super) use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;

pub(super) use otel_arrow_dfe_pdata::proto::opentelemetry::collector::trace::v1::ExportTraceServiceRequest;

pub(super) use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, ResourceLogs, ScopeLogs,
};

pub(super) use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Summary, SummaryDataPoint,
    metric, number_data_point,
};

pub(super) use otel_arrow_dfe_pdata::proto::opentelemetry::trace::v1::{
    ResourceSpans, ScopeSpans, Span,
};

pub(super) use otel_arrow_dfe_pdata::views::otlp::bytes::logs::RawLogsData;

pub(super) use otel_arrow_dfe_pdata::views::otlp::bytes::metrics::RawMetricsData;

pub(super) use otel_arrow_dfe_series_lake as lake;

pub(super) use otel_arrow_dfe_telemetry::metrics::MetricSetSnapshot;

pub(super) use std::sync::Arc;

pub(super) use std::time::Duration;

/// Build an exporter effect handler wired to a completion channel of exactly
/// `capacity` slots, so a test can saturate it deterministically.
pub(in super::super) fn effects(
    capacity: usize,
) -> (
    EffectHandler<OtapPdata>,
    PipelineCompletionMsgReceiver<OtapPdata>,
) {
    let (_rx, reporter) =
        otel_arrow_dfe_telemetry::reporter::MetricsReporter::create_new_and_receiver(16);
    let mut effects = EffectHandler::new(
        test_node("series"),
        reporter,
        test_pipeline_runtime_services(),
    );
    let (tx, rx) = pipeline_completion_msg_channel(capacity);
    effects.set_pipeline_completion_msg_sender(tx);
    (effects, rx)
}

/// Assert that the completion channel holds nothing more, so a test that has
/// taken every completion it expects also proves none was delivered twice.
pub(in super::super) fn assert_no_more_completions(
    rx: &mut PipelineCompletionMsgReceiver<OtapPdata>,
) {
    if let Ok(message) = rx.try_recv() {
        panic!("an unexpected completion: {message:?}");
    }
}

/// A payload-free request that still carries a routing frame, so the
/// completion it owes is routed.
pub(in super::super) fn empty_pdata() -> OtapPdata {
    let mut context = Context::default();
    context.set_source_node(7);
    OtapPdata::new(context, OtapPayload::empty(SignalType::Logs))
}

pub(super) fn encoded<M: prost::Message>(message: &M) -> Vec<u8> {
    let mut bytes = Vec::new();
    message.encode(&mut bytes).expect("encodes");
    bytes
}

pub(super) fn logs_payload() -> OtapPayload {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_789_960_500_000_000_000,
                    event_name: "ready".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let bytes = encoded(&request);
    let view = RawLogsData::try_new(&bytes).expect("valid logs bytes");
    OtapPayload::from(encode_logs_otap_batch(&view).expect("encodes to OTAP"))
}

pub(super) fn metrics_payload() -> OtapPayload {
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "requests".to_owned(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_789_960_500_000_000_000,
                            value: Some(number_data_point::Value::AsInt(1)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let bytes = encoded(&request);
    let view = RawMetricsData::try_new(&bytes).expect("valid metrics bytes");
    OtapPayload::from(encode_metrics_otap_batch(&view).expect("encodes to OTAP"))
}

/// One well-formed OTLP traces request, kept in its wire form.
///
/// Traces are refused on the signal alone, before any conversion, so the
/// request never has to be encoded into OTAP records.
pub(super) fn traces_payload() -> OtapPayload {
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    name: "unsupported".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    otel_arrow_dfe_pdata::OtlpProtoBytes::ExportTracesRequest(bytes::Bytes::from(encoded(&request)))
        .into()
}

/// One metrics request carrying a supported gauge point and an unsupported
/// summary point, for the `unsupported` policy.
pub(super) fn mixed_metrics_payload() -> OtapPayload {
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![
                    Metric {
                        name: "requests".to_owned(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_789_960_500_000_000_000,
                                value: Some(number_data_point::Value::AsInt(1)),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    },
                    Metric {
                        name: "latency".to_owned(),
                        data: Some(metric::Data::Summary(Summary {
                            data_points: vec![SummaryDataPoint {
                                time_unix_nano: 1_789_960_500_000_000_000,
                                count: 1,
                                sum: 2.0,
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
    };
    let bytes = encoded(&request);
    let view = RawMetricsData::try_new(&bytes).expect("valid metrics bytes");
    OtapPayload::from(encode_metrics_otap_batch(&view).expect("encodes to OTAP"))
}

/// One metrics request whose only gauge point carries an exemplar.
pub(super) fn exemplar_metrics_pdata() -> OtapPdata {
    use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{Exemplar, exemplar};
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "requests".to_owned(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_789_960_500_000_000_000,
                            value: Some(number_data_point::Value::AsInt(1)),
                            exemplars: vec![Exemplar {
                                time_unix_nano: 1_789_960_500_000_000_000,
                                value: Some(exemplar::Value::AsInt(1)),
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let bytes = encoded(&request);
    let view = RawMetricsData::try_new(&bytes).expect("valid metrics bytes");
    let mut context = Context::default();
    context.set_source_node(9);
    OtapPdata::new(
        context,
        OtapPayload::from(encode_metrics_otap_batch(&view).expect("encodes to OTAP")),
    )
}

/// One mixed metrics request that still carries a routing frame.
pub(super) fn mixed_metrics_pdata() -> OtapPdata {
    let mut context = Context::default();
    context.set_source_node(7);
    OtapPdata::new(context, mixed_metrics_payload())
}

/// Take one completion and assert it is a retryable shutdown refusal.
pub(super) async fn expect_shutdown_nack(rx: &mut PipelineCompletionMsgReceiver<OtapPdata>) {
    match tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a completion arrives")
        .expect("a completion arrives")
    {
        PipelineCompletionMsg::DeliverNack { nack } => {
            assert!(!nack.permanent);
            assert_eq!(nack.cause, NackCause::NodeShutdown);
            assert_eq!(nack.reason, Outcome::Shutdown.sentence());
        }
        other => panic!("expected a nack, got {other:?}"),
    }
}

/// A worker configuration with a one-second window and a small request bound,
/// so the completion credit arithmetic is exercised at a size a test can
/// reason about. The base URI is never used: these tests hand the worker an
/// in-memory object store directly.
pub(super) fn worker_config() -> Config {
    serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-unused"}},
        "window": {"interval": "1s", "max_requests_per_block": 4}
    }))
    .expect("valid config")
}

/// One well-formed logs request that still carries a routing frame.
pub(super) fn logs_pdata() -> OtapPdata {
    let mut context = Context::default();
    context.set_source_node(7);
    OtapPdata::new(context, logs_payload())
}

/// A worker configuration whose blocks hold at most `requests` requests, so a
/// third request has to wait for the next block.
pub(super) fn worker_config_with_requests(requests: usize) -> Config {
    let mut cfg = worker_config();
    cfg.window.max_requests_per_block = requests;
    cfg.lake.ingress.max_requests_per_block = requests;
    cfg
}

/// Take one completion, require it to be an ack, and say which request it
/// belonged to.
pub(super) async fn expect_ack(rx: &mut PipelineCompletionMsgReceiver<OtapPdata>) -> Option<usize> {
    match tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a completion arrives")
        .expect("a completion arrives")
    {
        PipelineCompletionMsg::DeliverAck { ack } => ack.accepted.into_parts().0.source_node(),
        other => panic!("expected an ack, got {other:?}"),
    }
}

/// One well-formed logs request carrying `source` as its routing frame, so a
/// completion can be traced back to the request that earned it.
pub(super) fn logs_pdata_from(source: usize) -> OtapPdata {
    let mut context = Context::default();
    context.set_source_node(source);
    OtapPdata::new(context, logs_payload())
}

/// Store hooks whose writes park until a test opens their gate.
///
/// A closed gate keeps a block FLUSHING across as many window boundaries as a
/// test crosses. Opening it leaves it open: a permit is returned as soon as the
/// write that took it has passed.
#[derive(Debug)]
pub(super) struct GatedStore {
    /// Permits to write; empty until the test releases the parked flush.
    pub(super) gate: Arc<tokio::sync::Semaphore>,
    /// Writes that have reached the gate, counted before they park on it.
    ///
    /// This is the signal that a flush has actually started: a test waits for
    /// it instead of guessing that the node has got that far.
    pub(super) entered: Arc<AtomicUsize>,
}

impl GatedStore {
    /// Announce a write, then wait for the gate.
    ///
    /// The count is raised before the wait, so a test observing it knows the
    /// write is parked.
    pub(super) async fn pass(&self) {
        let _ = self.entered.fetch_add(1, SeqCst);
        let permit = self.gate.acquire().await.expect("the gate is never closed");
        drop(permit);
    }
}

#[async_trait::async_trait]
impl StoreHooks for GatedStore {
    async fn before_put(
        &self,
        _location: &object_store::path::Path,
        _payload: &PutPayload,
    ) -> object_store::Result<Option<HookGuard>> {
        self.pass().await;
        Ok(None)
    }

    async fn before_multipart(
        &self,
        _location: &object_store::path::Path,
    ) -> object_store::Result<Option<HookGuard>> {
        self.pass().await;
        Ok(None)
    }
}

/// Give the node task turns until `condition` holds, or fail the test.
///
/// Both clocks the node reads are simulated, so the node only needs turns on
/// the single-threaded runtime it shares with the test; a condition that never
/// holds fails the test after a bounded number of turns.
pub(super) async fn until(what: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if condition() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("{what} never happened");
}

/// Wait until one row-less marker request has been taken and decided.
///
/// Two observations in one, and neither is a guess about timing. The pdata
/// channel these tests use holds a single message, so handing the marker over
/// proves the request before it has been taken off the channel, and the
/// completion the marker earns proves the node has finished deciding it. A
/// request carrying no rows is decided without touching a block, so the
/// marker changes nothing else about the worker's state.
pub(super) async fn marker(
    tx: &mpsc::Sender<OtapPdata>,
    rx: &mut PipelineCompletionMsgReceiver<OtapPdata>,
    tag: usize,
) {
    let mut context = Context::default();
    context.set_source_node(tag);
    tx.send_async(OtapPdata::new(
        context,
        OtapPayload::empty(SignalType::Logs),
    ))
    .await
    .expect("the marker enqueues");
    assert_eq!(
        expect_completion(rx).await,
        Some(tag),
        "the marker request is decided"
    );
}

/// Take one completion and say which request it belonged to, ack or nack.
pub(super) async fn expect_completion(
    rx: &mut PipelineCompletionMsgReceiver<OtapPdata>,
) -> Option<usize> {
    match tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a completion arrives")
        .expect("a completion arrives")
    {
        PipelineCompletionMsg::DeliverAck { ack } => ack.accepted.into_parts().0.source_node(),
        PipelineCompletionMsg::DeliverNack { nack } => (*nack.refused).into_parts().0.source_node(),
    }
}

/// How many objects the store holds, which is two per written block.
pub(super) async fn stored_files(store: &object_store::memory::InMemory) -> usize {
    use futures::StreamExt;
    store.list(None).count().await
}

/// Read one metric value out of a terminal handoff by name and labels.
///
/// Panics when the handoff does not carry the metric.
pub(super) fn terminal_value(
    snapshots: &[MetricSetSnapshot],
    name: &str,
    labels: &[(&str, &str)],
) -> u64 {
    for snapshot in snapshots {
        let Some(index) = snapshot
            .descriptor()
            .metrics
            .iter()
            .position(|metric| metric.name == name)
        else {
            continue;
        };
        let actual: Vec<_> = snapshot.measurement_attributes().collect();
        if actual.as_slice() != labels {
            continue;
        }
        return snapshot.get_metrics()[index].to_u64_lossy();
    }
    panic!("no terminal snapshot carries {name} with {labels:?}")
}

/// Failed flushes counted in `block.write_failures{error.type}` under `error_type`.
pub(super) fn flush_failures(
    metrics: &super::super::metrics::Metrics,
    error_type: WriteFailure,
) -> u64 {
    metrics
        .flush_failures
        .get(super::super::metrics::FlushFailureAttrs { error_type })
        .failures
        .get()
}

/// Flushes counted in `flush.late_commits{outcome}` under `outcome`.
pub(super) fn late_commits(metrics: &super::super::metrics::Metrics, outcome: LateCommit) -> u64 {
    metrics
        .late_commits
        .get(super::super::metrics::LateCommitAttrs { outcome })
        .late_commits
        .get()
}

/// Flushes counted in `flush.late_commits` under every outcome.
pub(super) fn all_late_commits(metrics: &super::super::metrics::Metrics) -> u64 {
    LateCommit::ALL
        .into_iter()
        .map(|outcome| late_commits(metrics, outcome))
        .sum()
}

/// Which failure a [`FaultStore`] injects.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum Fault {
    /// Every request passes through to the inner store.
    #[default]
    None,
    /// Every write of a `series` dataset file fails.
    Series,
    /// The first write of a `values` dataset file fails, and the store heals
    /// itself in the same step so the retry succeeds.
    ValuesOnce,
    /// Every write parks until the test releases it.
    Park,
    /// A `values` multipart upload is initiated and then wedges: its parts
    /// never land and its abort never returns, while everything else passes
    /// through.
    MultipartWedge,
    /// Every write spends [`SLOW_FAILURE`] of engine-clock time and then
    /// fails, the way a cloud store's own retry loop answers a request once
    /// its `retry_timeout` is spent.
    SlowFail,
    /// Every write is refused as `PermissionDenied`, the way a store answers
    /// credentials it does not accept.
    Denied,
    /// A `values` multipart upload is completed in the underlying store, and
    /// the completion call then never returns, the way a store that commits
    /// the upload and loses its response looks to the writer.
    LostComplete,
    /// A `values` multipart upload is completed in the underlying store, and
    /// the completion call then fails with a retryable store error, the way
    /// a store that commits the upload and drops the connection looks to the
    /// writer.
    FailedComplete,
    /// The first `values` multipart upload's parts fail and its abort fails
    /// too, leaving an upload behind; the store then heals.
    ValuesOrphanOnce,
    /// A `values` multipart upload is completed in the underlying store, the
    /// completion call then times out, and the abort that follows is answered
    /// `NotFound`, the way RustFS answers an abort of a committed upload.
    CommittedCompleteTimesOut,
    /// The first `values` completion times out without committing and its
    /// abort is answered `NotFound`; the store then heals.
    UncommittedCompleteTimesOutOnce,
    /// Every write panics inside the store, which unwinds the flush task.
    Panic,
}

/// How long one write takes to fail under [`Fault::SlowFail`].
pub(super) const SLOW_FAILURE: Duration = Duration::from_secs(20);

/// An object store that injects failures at the two entry points a Parquet
/// write actually uses: a small single-shot PUT and the initiation of a
/// multipart upload.
///
/// A fault wrapper around an in-memory store. Each PUT is recorded before it
/// may fail, so a test can compare a failed attempt's path and bytes with the
/// retry's.
pub(super) type FaultStore = HookStore<Faults>;

/// A [`FaultStore`] over a fresh in-memory store, injecting nothing yet.
pub(super) fn fault_store() -> Arc<FaultStore> {
    Arc::new(HookStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        Faults::default(),
    ))
}

/// The hooks of a [`FaultStore`], and what they recorded.
#[derive(Debug, Default)]
pub(super) struct Faults {
    /// The active injection.
    mode: std::sync::Mutex<Fault>,
    /// Path and payload of every PUT, including the ones that then failed.
    pub(super) writes: std::sync::Mutex<Vec<(String, Vec<u8>)>>,
    /// Raised as each write reaches the injection point, so a test can wait
    /// for a flush to have started.
    pub(super) entered: tokio::sync::Notify,
    /// Engine-clock instant at which each write reached the injection point.
    pub(super) entered_at: std::sync::Mutex<Vec<std::time::Instant>>,
    /// Releases one parked write under [`Fault::Park`].
    pub(super) release: tokio::sync::Notify,
    /// Writes currently parked inside the store under [`Fault::Park`].
    pub(super) parked: AtomicUsize,
    /// Parked writes whose future was dropped without a release, as a
    /// cancelled node drops them.
    pub(super) parked_drops: AtomicUsize,
    /// Parts handed to a wedged multipart upload.
    pub(super) parts: Arc<AtomicUsize>,
    /// Aborts attempted against a wedged multipart upload.
    pub(super) aborts: Arc<AtomicUsize>,
    /// Uploads completed in the underlying store under
    /// [`Fault::LostComplete`].
    pub(super) completes: Arc<AtomicUsize>,
}

/// A real multipart upload whose completion lands and whose response is then
/// lost: the call completes the upload in the underlying store and then
/// never returns, or fails with a retryable store error when `fails`.
#[derive(Debug)]
pub(super) struct LostCompleteUpload {
    /// The upload the underlying store really initiated.
    pub(super) inner: Box<dyn MultipartUpload>,
    /// Completions that landed, shared with the store the test holds.
    pub(super) completes: Arc<AtomicUsize>,
    /// Whether the lost response is an error; otherwise no answer.
    pub(super) fails: bool,
    /// Whether the completion is applied before its response is lost.
    pub(super) commits: bool,
    /// Whether an abort is answered `NotFound` instead of reaching the upload.
    pub(super) abort_not_found: bool,
}

#[async_trait::async_trait]
impl MultipartUpload for LostCompleteUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        if self.commits {
            let _ = self.inner.complete().await?;
            let _ = self.completes.fetch_add(1, SeqCst);
        }
        if self.fails {
            return Err(object_store::Error::Generic {
                store: "series-test",
                source: Box::new(std::io::Error::other("connection reset after complete")),
            });
        }
        std::future::pending().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        if self.abort_not_found {
            return Err(object_store::Error::NotFound {
                path: "values".into(),
                source: "NoSuchUpload".into(),
            });
        }
        self.inner.abort().await
    }
}

/// A real multipart upload that is initiated and then never progresses.
///
/// The underlying store initiates it, so the destination genuinely holds an
/// unfinished upload; its parts then park and its abort never returns. That is
/// the shape that makes the cleanup bound matter: abandoning the upload
/// without an abort leaves a partial upload behind, and the abort is exactly
/// the call that may never come back.
#[derive(Debug)]
pub(super) struct WedgedUpload {
    /// The upload the underlying store really initiated, held so it is only
    /// released when this one is dropped.
    pub(super) _inner: Box<dyn MultipartUpload>,
    /// Parts handed to this upload, shared with the store the test holds.
    pub(super) parts: Arc<AtomicUsize>,
    /// Aborts attempted against this upload.
    pub(super) aborts: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl MultipartUpload for WedgedUpload {
    fn put_part(&mut self, _data: PutPayload) -> UploadPart {
        let _ = self.parts.fetch_add(1, SeqCst);
        // Parked, so the writer is still in the phase where the sink aborts a
        // failed upload at the deadline; past finalization a cancelled upload
        // is left to the bucket's lifecycle rule.
        Box::pin(std::future::pending())
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        std::future::pending().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let _ = self.aborts.fetch_add(1, SeqCst);
        std::future::pending().await
    }
}

/// A real multipart upload whose parts fail and whose abort fails, so the
/// underlying store keeps it as an incomplete upload.
#[derive(Debug)]
pub(super) struct OrphanedUpload {
    /// The upload the underlying store really initiated.
    pub(super) _inner: Box<dyn MultipartUpload>,
    /// Aborts attempted against this upload.
    pub(super) aborts: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl MultipartUpload for OrphanedUpload {
    fn put_part(&mut self, _data: PutPayload) -> UploadPart {
        Box::pin(std::future::ready(Err(object_store::Error::Generic {
            store: "series-test",
            source: Box::new(std::io::Error::other("injected part failure")),
        })))
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        Err(object_store::Error::Generic {
            store: "series-test",
            source: Box::new(std::io::Error::other(
                "injected complete after failed parts",
            )),
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let _ = self.aborts.fetch_add(1, SeqCst);
        Err(object_store::Error::Generic {
            store: "series-test",
            source: Box::new(std::io::Error::other("injected abort failure")),
        })
    }
}

impl Faults {
    /// Inject `fault` from now on.
    pub(super) fn set(&self, fault: Fault) {
        *self.mode.lock().expect("mode lock") = fault;
    }

    /// The active injection.
    fn mode(&self) -> Fault {
        *self.mode.lock().expect("mode lock")
    }

    /// Announce a write and apply the active injection to it.
    async fn before(&self, path: &object_store::path::Path) -> object_store::Result<()> {
        self.entered_at
            .lock()
            .expect("entered_at lock")
            .push(clock::now());
        self.entered.notify_one();
        let mode = self.mode();
        assert!(mode != Fault::Panic, "injected store panic");
        if mode == Fault::Park {
            /// Accounts for one parked write for as long as its future lives.
            ///
            /// A released write decrements the live count only; one whose
            /// future is dropped while still parked also counts as a drop.
            struct Parked<'a> {
                /// Writes still parked.
                live: &'a AtomicUsize,
                /// Parked writes whose future was dropped.
                drops: &'a AtomicUsize,
                /// Whether the park ended by release; otherwise by a drop.
                released: bool,
            }
            impl Drop for Parked<'_> {
                fn drop(&mut self) {
                    let _ = self.live.fetch_sub(1, SeqCst);
                    if !self.released {
                        let _ = self.drops.fetch_add(1, SeqCst);
                    }
                }
            }
            let _ = self.parked.fetch_add(1, SeqCst);
            let mut guard = Parked {
                live: &self.parked,
                drops: &self.parked_drops,
                released: false,
            };
            self.release.notified().await;
            guard.released = true;
        }
        if mode == Fault::SlowFail {
            clock::sleep(SLOW_FAILURE).await;
        }
        if mode == Fault::Denied {
            return Err(object_store::Error::PermissionDenied {
                path: path.to_string(),
                source: "injected: access denied".into(),
            });
        }
        let path = path.as_ref();
        let fail = mode == Fault::SlowFail
            || (mode == Fault::Series && path.contains("dataset=series/"))
            || (mode == Fault::ValuesOnce
                && path.contains("dataset=values/")
                && self.heal_values_once());
        if fail {
            return Err(object_store::Error::Generic {
                store: "series-test",
                source: Box::new(std::io::Error::other("injected store failure")),
            });
        }
        Ok(())
    }

    /// Whether [`Fault::ValuesOnce`] is still active, healing it in the same
    /// step; only an operation that entered under it may take it.
    fn heal_values_once(&self) -> bool {
        let mut mode = self.mode.lock().expect("mode lock");
        if *mode != Fault::ValuesOnce {
            return false;
        }
        *mode = Fault::None;
        true
    }
}

#[async_trait::async_trait]
impl StoreHooks for Faults {
    async fn before_put(
        &self,
        path: &object_store::path::Path,
        payload: &PutPayload,
    ) -> object_store::Result<Option<HookGuard>> {
        let bytes = payload.iter().flat_map(|b| b.iter().copied()).collect();
        self.writes
            .lock()
            .expect("writes lock")
            .push((path.to_string(), bytes));
        self.before(path).await?;
        Ok(None)
    }

    async fn before_multipart(
        &self,
        path: &object_store::path::Path,
    ) -> object_store::Result<Option<HookGuard>> {
        self.before(path).await?;
        Ok(None)
    }

    fn wrap_upload(
        &self,
        path: &object_store::path::Path,
        upload: Box<dyn MultipartUpload>,
    ) -> Box<dyn MultipartUpload> {
        if self.mode() == Fault::MultipartWedge && path.as_ref().contains("dataset=values/") {
            return Box::new(WedgedUpload {
                _inner: upload,
                parts: Arc::clone(&self.parts),
                aborts: Arc::clone(&self.aborts),
            });
        }
        let mode = self.mode();
        if mode == Fault::ValuesOrphanOnce && path.as_ref().contains("dataset=values/") {
            self.set(Fault::None);
            return Box::new(OrphanedUpload {
                _inner: upload,
                aborts: Arc::clone(&self.aborts),
            });
        }
        if matches!(
            mode,
            Fault::LostComplete
                | Fault::FailedComplete
                | Fault::CommittedCompleteTimesOut
                | Fault::UncommittedCompleteTimesOutOnce
        ) && path.as_ref().contains("dataset=values/")
        {
            if mode == Fault::UncommittedCompleteTimesOutOnce {
                self.set(Fault::None);
            }
            let not_found = matches!(
                mode,
                Fault::CommittedCompleteTimesOut | Fault::UncommittedCompleteTimesOutOnce
            );
            return Box::new(LostCompleteUpload {
                inner: upload,
                completes: Arc::clone(&self.completes),
                fails: mode != Fault::LostComplete,
                commits: mode != Fault::UncommittedCompleteTimesOutOnce,
                abort_not_found: not_found,
            });
        }
        upload
    }
}

/// Advances a simulated clock on every runtime turn, until it is dropped.
///
/// The retry backoff and deadline are measured on the engine clock, so a test
/// that needs a retry moves that clock; the deadline is reached in a finite
/// number of turns, so a condition that never holds fails the flush.
pub(super) struct Ticker(tokio::task::JoinHandle<()>);

impl Drop for Ticker {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Advance both the engine clock and the wall clock, a second per turn.
///
/// Window boundaries are aligned to wall time but waited for on the engine
/// clock, so a test that needs boundaries moves both. One boundary is reached
/// per turn, so the other select branches keep their turns.
pub(super) fn ticking_windows(
    sim: &clock::SimClock,
    wall: &Arc<lake::clock::TestWallClock>,
) -> Ticker {
    let sim = sim.clone();
    let wall = Arc::clone(wall);
    Ticker(tokio::task::spawn_local(async move {
        let mut secs = 0_i64;
        loop {
            tokio::task::yield_now().await;
            secs = secs.saturating_add(1);
            wall.set(secs.saturating_mul(1_000_000_000));
            sim.advance(Duration::from_secs(1));
        }
    }))
}

/// Advance `sim` by `total` in 100 ms steps, giving every task on the
/// runtime several turns after each step.
///
/// Stepping from the test, not from a [`Ticker`], lets the test act at a
/// chosen simulated instant.
pub(super) async fn step_for(sim: &clock::SimClock, total: Duration) {
    let step = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;
    while elapsed < total {
        sim.advance(step);
        elapsed += step;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }
}

/// Start advancing `sim` by `step` on every turn of the current runtime.
pub(super) fn ticking(sim: &clock::SimClock, step: Duration) -> Ticker {
    let sim = sim.clone();
    Ticker(tokio::task::spawn_local(async move {
        loop {
            tokio::task::yield_now().await;
            sim.advance(step);
        }
    }))
}

/// Release a finished flush's cleanup slot so the worker can rotate again.
///
/// A completed flush leaves its supervising task in the same FLUSHING slot
/// until that task has finished releasing the block, sink and write future it
/// owns. The node loop joins it in its own select branch; a test driving a
/// worker directly has to do the same before the next rotation.
pub(super) async fn drain_cleanup(worker: &mut Worker) {
    if let Some(mut job) = worker.cleaning.take() {
        job.cleanup().await.expect("the cleanup task joins");
    }
}

/// An S3 storage section with the default credential chain, for tests that
/// only load a configuration.
pub(super) fn s3_storage() -> serde_json::Value {
    serde_json::json!({"s3": {"base_uri": "s3://bucket/lake", "auth": {"type": "default"}}})
}

/// A recorded tracing field value, keeping the type it was recorded with.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum FieldValue {
    U64(u64),
    I64(i64),
    Bool(bool),
    Str(String),
    /// A value recorded through `Debug` or `Display` (`?x` or `%x`).
    Debug(String),
}

impl FieldValue {
    /// The text of a string, debug or display value.
    pub(super) fn text(&self) -> &str {
        match self {
            FieldValue::Str(s) | FieldValue::Debug(s) => s,
            _ => "",
        }
    }
}

/// One event recorded by [`capture`]: its level, name and typed fields.
#[derive(Debug, Clone)]
pub(super) struct CapturedEvent {
    pub(super) level: tracing::Level,
    pub(super) name: String,
    pub(super) fields: std::collections::BTreeMap<String, FieldValue>,
}

std::thread_local! {
    /// Events recorded on this thread while a [`Capture`] is alive.
    static CAPTURED: std::cell::RefCell<Option<Vec<CapturedEvent>>> =
        const { std::cell::RefCell::new(None) };
}

/// The layer behind [`capture`], installed once over the global registry.
///
/// A per-test scoped subscriber would race the global callsite cache: a
/// callsite another test registers first is cached as uninteresting to a
/// subscriber that did not exist yet. So every callsite is `sometimes`,
/// `enabled` is asked per event, and it records only on a thread that holds a
/// [`Capture`], which keeps parallel tests out of each other's records.
struct CaptureLayer;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CaptureLayer {
    fn register_callsite(
        &self,
        _metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }

    fn enabled(
        &self,
        _metadata: &tracing::Metadata<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        CAPTURED
            .try_with(|captured| captured.try_borrow().is_ok_and(|c| c.is_some()))
            .unwrap_or(false)
    }

    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct Fields(std::collections::BTreeMap<String, FieldValue>);
        impl tracing::field::Visit for Fields {
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                let _ = self
                    .0
                    .insert(field.name().to_owned(), FieldValue::U64(value));
            }
            fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
                let _ = self
                    .0
                    .insert(field.name().to_owned(), FieldValue::I64(value));
            }
            fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                let _ = self
                    .0
                    .insert(field.name().to_owned(), FieldValue::Bool(value));
            }
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                let _ = self
                    .0
                    .insert(field.name().to_owned(), FieldValue::Str(value.to_owned()));
            }
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                let _ = self.0.insert(
                    field.name().to_owned(),
                    FieldValue::Debug(format!("{value:?}")),
                );
            }
        }
        let mut fields = Fields(std::collections::BTreeMap::new());
        event.record(&mut fields);
        let recorded = CapturedEvent {
            level: *event.metadata().level(),
            name: event.metadata().name().to_owned(),
            fields: fields.0,
        };
        let _ = CAPTURED.try_with(|captured| {
            if let Ok(mut captured) = captured.try_borrow_mut()
                && let Some(events) = captured.as_mut()
            {
                events.push(recorded);
            }
        });
    }
}

/// Records the events of the current thread for as long as it lives.
pub(super) struct Capture;

impl Capture {
    /// Every event recorded so far with this name.
    pub(super) fn named(&self, name: &str) -> Vec<CapturedEvent> {
        CAPTURED.with(|captured| {
            captured
                .borrow()
                .iter()
                .flatten()
                .filter(|event| event.name == name)
                .cloned()
                .collect()
        })
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        CAPTURED.with(|captured| *captured.borrow_mut() = None);
    }
}

/// Start recording this thread's events through the global test subscriber.
pub(super) fn capture() -> Capture {
    static INSTALLED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    let _ = INSTALLED.get_or_init(|| {
        use tracing_subscriber::layer::SubscriberExt;
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(CaptureLayer))
            .expect("no other global subscriber in this test binary");
    });
    // Callsites registered by other tests before the install are re-asked,
    // and now answer `sometimes`.
    tracing::callsite::rebuild_interest_cache();
    CAPTURED.with(|captured| *captured.borrow_mut() = Some(Vec::new()));
    Capture
}

/// One logs request carrying `records` log records of a single series, so its
/// values file is large enough to span several multipart chunks.
pub(super) fn bulk_logs_pdata(records: usize) -> OtapPdata {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: (0..records)
                    .map(|i| LogRecord {
                        time_unix_nano: 1_789_960_500_000_000_000 + i as u64,
                        event_name: "ready".to_owned(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let bytes = encoded(&request);
    let view = RawLogsData::try_new(&bytes).expect("valid logs bytes");
    let payload = OtapPayload::from(encode_logs_otap_batch(&view).expect("encodes to OTAP"));
    let mut context = Context::default();
    context.set_source_node(7);
    OtapPdata::new(context, payload)
}

/// Build a real exporter inbox over local channels, with `capacity` pdata
/// slots.
///
/// The engine's own inbox: it releases a latched Shutdown only after the
/// buffered pdata has been force-drained past a closed admission gate.
pub(super) fn inbox(
    capacity: usize,
) -> (
    mpsc::Sender<OtapPdata>,
    mpsc::Sender<NodeControlMsg<OtapPdata>>,
    ExporterInbox<OtapPdata>,
) {
    let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<OtapPdata>>::new(8);
    let (pdata_tx, pdata_rx) = mpsc::Channel::<OtapPdata>::new(capacity);
    let inbox = ExporterInbox::new(
        Receiver::Local(LocalReceiver::mpsc(control_rx)),
        Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
        7,
        Interests::empty(),
    );
    (pdata_tx, control_tx, inbox)
}

/// A worker configuration whose storage points at a directory that exists.
///
/// Only the tests that drive the real [`Exporter::start`] entry point need it:
/// `start` builds the configured object store before the test replaces it, and
/// a local file store refuses a base URI that is not there.
pub(super) fn startable_config(requests: usize) -> Config {
    let mut cfg: Config = serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": std::env::temp_dir().to_string_lossy()}},
        "window": {"interval": "1s", "max_requests_per_block": requests}
    }))
    .expect("valid config");
    cfg.lake.ingress.max_requests_per_block = requests;
    cfg
}
