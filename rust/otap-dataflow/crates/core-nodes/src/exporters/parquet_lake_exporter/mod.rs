// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Parquet lake exporter: writes `series` and `values` Parquet datasets per signal keyed by a
//! stable series_id, and acknowledges each batch once every block holding its rows has landed.
//! See docs/FORMAT.md.
//!
//! Every admitted batch resolves exactly once: by `release_admission` once all its blocks landed,
//! by `block_failed` when a block holding its rows failed (later events are ignored), or by
//! `drain_nack` at shutdown. `Block::take` removes a block's batch set before encoding, so no
//! later flush can see it again.

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = PARQUET_LAKE_EXPORTER_URN,
    target = "otel.exporter.parquet_lake",
);

mod anyvalue;
mod attrs;
mod block;
mod columns;
pub mod config;
mod error;
mod extract;
mod identity;
pub mod metrics;
mod pending;
mod schema;
#[cfg(test)]
mod test_fixtures;
#[cfg(test)]
mod test_store;
#[cfg(test)]
mod tests;
mod upload;

// Public surface: config, metrics, and the block probe (with the id types it takes). Everything
// else is private, so no public item exposes a private type.
pub use block::BlockId;
pub use identity::Signal;
pub use upload::probe_block;

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use linkme::distributed_slice;
use object_store::ObjectStore;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::capability::auth::bearer_token_provider::BearerTokenProvider;
use otel_arrow_dfe_engine::config::ExporterConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::{AckMsg, NackCause, NackMsg, NodeControlMsg};
use otel_arrow_dfe_engine::error::{Error, ExporterErrorKind, format_error_sources};
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::local::exporter::{EffectHandler, Exporter};
use otel_arrow_dfe_engine::message::{ExporterInbox, Message};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_engine::{ConsumerEffectHandlerExtension, ExporterFactory};
use otel_arrow_dfe_otap::OTAP_EXPORTER_FACTORIES;
use otel_arrow_dfe_otap::metrics::ExporterExportMetrics;
use otel_arrow_dfe_otap::object_store::StorageType;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::otlp::OtlpProtoBytes;
use otel_arrow_dfe_pdata::{PayloadData, TryIntoWithOptions};
use otel_arrow_dfe_telemetry::common_attributes::{Outcome, SignalOutcomeAttributes};
use otel_arrow_dfe_telemetry::metrics::{MeasurementMetricSet, MetricSet, MetricSetHandler};

use self::block::{Block, EncodeLimits, TakenBlock, date_of, encode_block};
use self::config::LakeConfig;
use self::error::LakeError;
use self::pending::{Completion, PendingAcks};
use self::schema::Schemas;
use self::upload::{sync_local, upload_block};

/// URN of the Parquet lake exporter.
pub const PARQUET_LAKE_EXPORTER_URN: &str = "urn:otel:exporter:parquet_lake";

/// Parquet lake exporter.
pub struct ParquetLakeExporter {
    config: LakeConfig,
    token_provider: Option<
        Box<dyn otel_arrow_dfe_engine::shared::capability::auth::bearer_token_provider::BearerTokenProvider>,
    >,
    pdata_metrics: Option<MeasurementMetricSet<ExporterExportMetrics>>,
    metrics: Option<MetricSet<metrics::LakeMetrics>>,
    /// Writer id: unique per exporter start (core + random nonce).
    writer: String,
    /// Whether the "no upstream waits for acks" warning was already logged.
    warned_unacked: bool,
    /// Test hook: use this store instead of building one from `config.storage`.
    #[cfg(test)]
    store_override: Option<Arc<dyn ObjectStore>>,
}

/// Declares the Parquet lake exporter as a local exporter factory.
///
/// Unsafe code is temporarily used here to allow the use of `distributed_slice` macro
/// This macro is part of the `linkme` crate which is considered safe and well maintained.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Exporter)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static PARQUET_LAKE_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: PARQUET_LAKE_EXPORTER_URN,
    create: |pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             exporter_config: &ExporterConfig,
             capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        let mut exporter = ParquetLakeExporter::from_config(&pipeline, &node_config.config)?;
        if exporter.config.storage.requires_bearer_token_provider() {
            exporter.token_provider = Some(
                capabilities
                    .require_shared::<BearerTokenProvider>()
                    .map_err(|e| otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                        error: e.to_string(),
                    })?,
            );
        }
        Ok(ExporterWrapper::local(
            exporter,
            node,
            node_config,
            exporter_config,
        ))
    },
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: |value| LakeConfig::parse(value).map(|_| ()),
};

/// Loop state.
struct State {
    store: Arc<dyn ObjectStore>,
    schemas: Schemas,
    logs: Block,
    metrics: Block,
    pending: PendingAcks,
    next_block: u64,
}

impl State {
    const fn block(&mut self, signal: Signal) -> &mut Block {
        match signal {
            Signal::Logs => &mut self.logs,
            Signal::Metrics => &mut self.metrics,
        }
    }
}

/// The signal to flush on a timer tick: the oldest block at least `max_age` old. One flush per
/// tick keeps the time the node spends away from its inbox (and a pending Shutdown) to one upload.
fn due_signal(logs: &Block, metrics: &Block, now: Instant, max_age: Duration) -> Option<Signal> {
    let (l, m) = (logs.age(now), metrics.age(now));
    let (signal, age) = if l >= m {
        (Signal::Logs, l)
    } else {
        (Signal::Metrics, m)
    };
    (age >= max_age && !age.is_zero()).then_some(signal)
}

/// True when `raw` is not a valid OTLP request (strict protobuf decode). Used only for payloads
/// that converted to zero rows: pdata's lenient conversion turns garbage into an empty batch,
/// which must be refused rather than acknowledged unwritten.
fn malformed_otlp(raw: &OtlpProtoBytes) -> bool {
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::trace::v1::ExportTraceServiceRequest;
    use prost::Message as _;
    match raw {
        OtlpProtoBytes::ExportLogsRequest(b) => {
            ExportLogsServiceRequest::decode(b.clone()).is_err()
        }
        OtlpProtoBytes::ExportMetricsRequest(b) => {
            ExportMetricsServiceRequest::decode(b.clone()).is_err()
        }
        OtlpProtoBytes::ExportTracesRequest(b) => {
            ExportTraceServiceRequest::decode(b.clone()).is_err()
        }
    }
}

impl ParquetLakeExporter {
    /// Build from user config (also used by tests).
    pub fn from_config(
        pipeline: &PipelineContext,
        config: &serde_json::Value,
    ) -> Result<Self, otel_arrow_dfe_config::error::Error> {
        Ok(Self {
            config: LakeConfig::parse(config)?,
            token_provider: None,
            pdata_metrics: Some(ExporterExportMetrics::register(pipeline)),
            metrics: Some(metrics::LakeMetrics::register(pipeline)),
            writer: format!("c{}-{:016x}", pipeline.core_id(), rand::random::<u64>()),
            warned_unacked: false,
            #[cfg(test)]
            store_override: None,
        })
    }

    fn record_export(&mut self, token: &OtapPdata, outcome: Outcome, elapsed: Duration) {
        if let Some(m) = self.pdata_metrics.as_mut() {
            m.with(SignalOutcomeAttributes {
                signal: token.signal_type(),
                outcome,
            })
            .record(elapsed);
        }
    }

    /// Deliver one completion. A Nack during shutdown carries `NackCause::NodeShutdown`.
    async fn notify(
        &mut self,
        eh: &EffectHandler<OtapPdata>,
        c: Completion,
        shutdown: bool,
    ) -> Result<(), Error> {
        match c {
            Completion::Ack { token, elapsed } => {
                self.record_export(&token, Outcome::Success, elapsed);
                eh.notify_ack(AckMsg::new(token)).await
            }
            Completion::Nack {
                token,
                reason,
                elapsed,
            } => {
                self.record_export(&token, Outcome::Failure, elapsed);
                let nack = if shutdown {
                    NackMsg::new_with_cause(reason, token, NackCause::NodeShutdown)
                } else {
                    NackMsg::new(reason, token)
                };
                eh.notify_nack(nack).await
            }
        }
    }

    /// Flush `signal`'s block (if non-empty): encode, upload until `deadline`, then Ack or Nack its
    /// batches.
    async fn flush(
        &mut self,
        eh: &EffectHandler<OtapPdata>,
        st: &mut State,
        signal: Signal,
        deadline: Instant,
        shutdown: bool,
    ) -> Result<(), Error> {
        if st.block(signal).is_empty() {
            return Ok(());
        }
        let TakenBlock {
            series,
            values,
            batches,
            created,
        } = st.block(signal).take();
        let id = BlockId {
            signal,
            date: date_of(created),
            writer: self.writer.clone(),
            seq: st.next_block,
        };
        st.next_block += 1;
        let started = Instant::now();
        let outcome = if started >= deadline {
            Err("deadline passed before the block was encoded".to_owned())
        } else {
            let encoded = encode_block(
                id,
                series,
                values,
                &st.schemas,
                SystemTime::now(),
                EncodeLimits::DEFAULT,
            )
            .await;
            match encoded {
                Err(e) => Err(format!("encode failed: {e}")),
                Ok(block) => {
                    let (init, max) = (
                        self.config.retry_initial_backoff,
                        self.config.retry_max_backoff,
                    );
                    let mut r = upload_block(st.store.as_ref(), &block, deadline, init, max).await;
                    if r.is_ok()
                        && let StorageType::File { base_uri } = &self.config.storage
                        && let Err(e) = sync_local(base_uri, &block.id).await
                    {
                        r = Err(e);
                    }
                    if let Some(m) = self.metrics.as_mut() {
                        m.encoder_peak.record(block.encoder_peak as f64);
                        if let Ok(retries) = &r {
                            m.blocks_landed.inc();
                            m.upload_retries.add(u64::from(*retries));
                            m.bytes_written.add(
                                (block.series.content_length() + block.values.content_length())
                                    as u64,
                            );
                            m.rows_written.add(block.rows as u64);
                            m.series_rows_written.add(block.series_rows as u64);
                        }
                    }
                    r.map(|_| ())
                }
            }
        };
        if let Some(m) = self.metrics.as_mut() {
            m.flush_duration.record(started.elapsed().as_secs_f64());
        }
        let now = Instant::now();
        let completions: Vec<Completion> = match outcome {
            Ok(()) => batches
                .into_iter()
                .filter_map(|s| st.pending.block_landed(s, now))
                .collect(),
            Err(reason) => {
                if let Some(m) = self.metrics.as_mut() {
                    m.blocks_failed.inc();
                }
                otel_warn!(
                    "parquet_lake.block.failed",
                    error = reason.as_str(),
                    batches = batches.len()
                );
                let reason = format!("parquet_lake: block failed: {reason}");
                batches
                    .into_iter()
                    .filter_map(|s| st.pending.block_failed(s, &reason, now))
                    .collect()
            }
        };
        for c in completions {
            self.notify(eh, c, shutdown).await?;
        }
        Ok(())
    }

    async fn handle_pdata(
        &mut self,
        eh: &EffectHandler<OtapPdata>,
        st: &mut State,
        mut token: OtapPdata,
    ) -> Result<(), Error> {
        if !self.warned_unacked && !token.has_ack_or_nack_interests() {
            self.warned_unacked = true;
            otel_warn!(
                "parquet_lake.unacked_input",
                message = "a batch arrived without Ack/Nack subscribers: upstream acknowledged it before it landed (set wait_for_result: true on receivers), so a crash or failed block loses it"
            );
        }
        let payload = token.take_payload();
        let raw = match payload.data() {
            PayloadData::OtlpBytes(b) => Some(b.clone()),
            PayloadData::OtapArrowRecords(_) => None,
        };
        let max = self.config.max_chunk_bytes();
        let records: Result<OtapArrowRecords, _> = payload.try_into_with_default();
        let extracted = records
            .map_err(|e| LakeError::Conversion(e.to_string()))
            .and_then(|mut records: OtapArrowRecords| {
                records
                    .decode_transport_optimized_ids()
                    .map_err(|e| LakeError::Conversion(e.to_string()))?;
                match &records {
                    OtapArrowRecords::Logs(_) => Ok((
                        Signal::Logs,
                        extract::extract_logs(&records, &st.schemas, max)?,
                        0,
                    )),
                    OtapArrowRecords::Metrics(_) => {
                        let (chunks, orphans) =
                            extract::extract_metrics(&records, &st.schemas, max)?;
                        Ok((Signal::Metrics, chunks, orphans))
                    }
                    OtapArrowRecords::Traces(_) => Err(LakeError::UnsupportedSignal),
                }
            })
            .and_then(|(signal, chunks, orphans)| {
                if chunks.is_empty() && raw.as_ref().is_some_and(malformed_otlp) {
                    return Err(LakeError::Conversion("malformed OTLP request".into()));
                }
                Ok((signal, chunks, orphans))
            });
        let (signal, chunks, orphans) = match extracted {
            Ok(v) => v,
            Err(e) => {
                // Malformed or unsupported input: retrying cannot help.
                if let Some(m) = self.metrics.as_mut() {
                    m.batches_rejected.inc();
                }
                self.record_export(&token, Outcome::Failure, Duration::ZERO);
                return eh
                    .notify_nack(NackMsg::new_permanent_with_cause(
                        format!("parquet_lake: {e}"),
                        token,
                        NackCause::Refused,
                    ))
                    .await;
            }
        };
        if orphans > 0
            && let Some(m) = self.metrics.as_mut()
        {
            m.points_orphaned.add(orphans);
        }
        let seq = st.pending.admit(token, Instant::now());
        for chunk in chunks {
            let block = st.block(signal);
            if !block.is_empty() && block.bytes() + chunk.bytes() > self.config.max_block_bytes {
                let deadline = Instant::now() + self.config.upload_deadline;
                self.flush(eh, st, signal, deadline, false).await?;
                if !st.pending.contains(seq) {
                    // The flushed block held rows of `seq` and failed: `seq` is already Nacked.
                    break;
                }
            }
            match st.block(signal).push(chunk, seq, Instant::now()) {
                Ok(true) => st.pending.add_block(seq),
                Ok(false) => {}
                Err(e) => {
                    let reason = format!("parquet_lake: {e}");
                    if let Some(c) = st.pending.block_failed(seq, &reason, Instant::now()) {
                        self.notify(eh, c, false).await?;
                    }
                    break;
                }
            }
        }
        if let Some(c) = st.pending.release_admission(seq, Instant::now()) {
            self.notify(eh, c, false).await?;
        }
        Ok(())
    }

    /// Flush both blocks within the deadline, Nack anything left with `NodeShutdown`.
    async fn shutdown(
        &mut self,
        eh: &EffectHandler<OtapPdata>,
        st: &mut State,
        deadline: Instant,
    ) -> Result<TerminalState, Error> {
        let flush_deadline = deadline.min(Instant::now() + self.config.upload_deadline);
        self.flush(eh, st, Signal::Logs, flush_deadline, true)
            .await?;
        self.flush(eh, st, Signal::Metrics, flush_deadline, true)
            .await?;
        for c in st
            .pending
            .drain_nack("parquet_lake: shutdown before upload", Instant::now())
        {
            self.notify(eh, c, true).await?;
        }
        let mut snapshots = Vec::new();
        if let Some(m) = self.pdata_metrics.as_mut() {
            snapshots.extend(m.terminal_snapshots());
        }
        if let Some(m) = &self.metrics
            && m.needs_flush()
        {
            snapshots.push(m.snapshot());
        }
        Ok(TerminalState::new(deadline, snapshots))
    }
}

#[async_trait(?Send)]
impl Exporter<OtapPdata> for ParquetLakeExporter {
    async fn start(
        mut self: Box<Self>,
        mut inbox: ExporterInbox<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let exporter_id = effect_handler.exporter_id();
        #[cfg(test)]
        let override_store = self.store_override.take();
        #[cfg(not(test))]
        let override_store: Option<Arc<dyn ObjectStore>> = None;
        let store = match override_store {
            Some(store) => store,
            None => {
                otel_arrow_dfe_otap::object_store::from_storage_type_with_retry_and_token_provider(
                    &self.config.storage,
                    self.config.retry.as_ref(),
                    self.token_provider.take(),
                )
                .map_err(|e| {
                    let source_detail = format_error_sources(&e);
                    Error::ExporterError {
                        exporter: exporter_id.clone(),
                        kind: ExporterErrorKind::Configuration,
                        error: format!("error initializing object store {e}"),
                        source_detail,
                    }
                })?
            }
        };
        otel_info!(
            "parquet_lake.start",
            writer = self.writer.as_str(),
            max_block_bytes = self.config.max_block_bytes as u64,
            worst_case_flush_bytes = self.config.worst_case_flush_bytes() as u64,
            message = "receivers must use wait_for_result and a timeout above max_block_age plus upload_deadline"
        );
        let mut st = State {
            store,
            schemas: Schemas::new(),
            logs: Block::default(),
            metrics: Block::default(),
            pending: PendingAcks::default(),
            next_block: 0,
        };
        let _timer = effect_handler
            .start_periodic_timer(self.config.check_interval)
            .await?;
        loop {
            // Backpressure: the loop does not read its inbox while it flushes a block.
            match inbox.recv().await? {
                Message::Control(NodeControlMsg::TimerTick { .. }) => {
                    let now = Instant::now();
                    if let Some(signal) =
                        due_signal(&st.logs, &st.metrics, now, self.config.max_block_age)
                    {
                        let deadline = now + self.config.upload_deadline;
                        self.flush(&effect_handler, &mut st, signal, deadline, false)
                            .await?;
                    }
                }
                Message::Control(NodeControlMsg::CollectTelemetry {
                    mut metrics_reporter,
                }) => {
                    if let Some(m) = self.pdata_metrics.as_mut() {
                        let _ = metrics_reporter.report_measurement(m);
                    }
                    if let Some(m) = self.metrics.as_mut() {
                        let _ = metrics_reporter.report(m);
                    }
                }
                Message::Control(NodeControlMsg::Shutdown { deadline, .. }) => {
                    return self.shutdown(&effect_handler, &mut st, deadline).await;
                }
                Message::Control(_) => {}
                Message::PData(pdata) => {
                    self.handle_pdata(&effect_handler, &mut st, pdata).await?;
                }
            }
        }
    }
}
