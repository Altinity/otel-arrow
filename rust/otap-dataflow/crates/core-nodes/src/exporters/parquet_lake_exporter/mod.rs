// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Parquet lake exporter: writes Series Lake Format v1 (`series` and `values` Parquet datasets per
//! signal, keyed by a stable series_id) and acknowledges each request once every block holding its
//! rows has landed. See docs/FORMAT.md.
//!
//! One ACTIVE generation (the logs and metrics blocks of the current window) accepts input while
//! at most one generation occupies the flush slot. Its blocks are flushed one after the other, each
//! in a local task. Admission closes only when ACTIVE must rotate and the slot is busy.
//!
//! Every admitted request resolves exactly once in `PendingAcks`: by `release_admission` once all
//! its blocks landed, by `block_failed` when a block holding its rows failed (later events are
//! ignored), or by `drain_nack` at shutdown.

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = PARQUET_LAKE_EXPORTER_URN,
    target = "otel.exporter.parquet_lake",
);

mod anyvalue;
mod attrs;
mod block;
mod cache;
mod canonical;
mod columns;
pub mod config;
mod error;
mod extract;
mod flush;
mod limits;
pub mod metrics;
mod pending;
mod schema;
mod sort;
#[cfg(test)]
mod test_fixtures;
#[cfg(test)]
mod test_store;
#[cfg(test)]
mod tests;
mod upload;
mod utf8;
mod value;
mod window;

// Public surface: config, metrics, and the block probe (with the id types it takes). Everything
// else is private, so no public item exposes a private type.
pub use block::BlockId;
pub use canonical::Signal;
pub use upload::probe_block;

use std::collections::{BTreeSet, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use linkme::distributed_slice;
use object_store::ObjectStore;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::capability::auth::bearer_token_provider::BearerTokenProvider;
use otel_arrow_dfe_engine::clock;
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
use otel_arrow_dfe_pdata::{OtapPayload, PayloadData, TryIntoWithOptions};
use otel_arrow_dfe_telemetry::common_attributes::{Outcome, SignalOutcomeAttributes};
use otel_arrow_dfe_telemetry::metrics::{MeasurementMetricSet, MetricSet, MetricSetHandler};
use tokio::task::JoinError;

use self::block::{Block, EncodeLimits, FileMeta, SealedBlock, prune_committed};
use self::cache::{CacheStats, SeriesCache};
use self::config::LakeConfig;
use self::error::{LakeError, Refusal};
use self::extract::{Chunk, Extracted};
use self::flush::{FlushTask, Landed, Uploader};
use self::pending::{Completion, PendingAcks};
use self::schema::Schemas;
use self::window::{PartitionId, SystemWallClock, WallClock, Window};

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
    /// Random id of this exporter start: 32 lowercase hexadecimal digits.
    boot_id: String,
    /// Wall clock (windows, partitions, `emitted_at`).
    wall: Rc<dyn WallClock>,
    /// Whether the "no upstream waits for acks" warning was already logged.
    warned_unacked: bool,
    /// Last time a refusal was logged (at most one line per second).
    refusal_logged: Option<Instant>,
    /// Last time a repaired request was logged (at most one line per second).
    repair_logged: Option<Instant>,
    /// Test hook: use this store instead of building one from `config.storage`.
    #[cfg(test)]
    store_override: Option<Arc<dyn ObjectStore>>,
    /// Test hook: a sub-second window interval (the config accepts whole seconds only).
    #[cfg(test)]
    interval_override: Option<Duration>,
}

/// Why a generation was rotated.
#[derive(Clone, Copy, Debug)]
enum FlushReason {
    Time,
    Bytes,
    Shutdown,
}

/// The generation that accepts input: the logs and metrics blocks of the current window.
#[derive(Default)]
struct Active {
    logs: Block,
    metrics: Block,
    /// Wall time (Unix nanoseconds) of the first push; `None` while empty.
    opened_nanos: Option<i64>,
}

impl Active {
    const fn block(&mut self, signal: Signal) -> &mut Block {
        match signal {
            Signal::Logs => &mut self.logs,
            Signal::Metrics => &mut self.metrics,
        }
    }

    const fn bytes(&self) -> usize {
        self.logs.bytes() + self.metrics.bytes()
    }

    fn is_empty(&self) -> bool {
        self.logs.is_empty() && self.metrics.is_empty()
    }
}

/// The block whose flush task is running.
struct InFlight {
    id: BlockId,
    /// Requests with rows in the block.
    batches: BTreeSet<u64>,
    /// Series whose rows the block carries; marked committed once it has landed.
    series_ids: Vec<u128>,
    started: Instant,
    /// The flush task; dropping it aborts the task.
    task: FlushTask,
}

/// The generation in the flush slot: the block being flushed and those waiting for their turn.
struct Flushing {
    current: InFlight,
    queued: VecDeque<SealedBlock>,
    /// Deadline of the whole generation.
    deadline: Instant,
    meta: FileMeta,
}

/// A request whose remaining chunks wait for room in ACTIVE. It enters the next generation
/// before anything newer, because admission stays closed while it is parked.
struct Parked {
    seq: u64,
    signal: Signal,
    chunks: VecDeque<Chunk>,
}

/// Loop state.
struct State {
    uploader: Uploader,
    schemas: Schemas,
    cache: SeriesCache,
    window: Window,
    active: Active,
    flushing: Option<Flushing>,
    parked: Option<Parked>,
    /// ACTIVE must rotate as soon as the flush slot is free.
    rotation_due: Option<FlushReason>,
    pending: PendingAcks,
    next_seq: u64,
    /// When admission closed, while it is closed.
    closed_since: Option<Instant>,
    /// Cache counters already reported.
    reported: CacheStats,
}

impl State {
    /// Whether pdata may be read: no request is parked and ACTIVE is not waiting for the slot.
    const fn accepting(&self) -> bool {
        self.parked.is_none() && !(self.rotation_due.is_some() && self.flushing.is_some())
    }
}

/// The result of the task in the flush slot; never resolves while the slot is free.
async fn flush_result(
    flushing: &mut Option<Flushing>,
) -> Result<Result<Landed, LakeError>, JoinError> {
    match flushing {
        Some(f) => f.current.task.join().await,
        None => std::future::pending().await,
    }
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

/// Largest count of u16-addressed entries pdata's OTLP -> OTAP encoder can assign before a `u16`
/// id column wraps (logs: attributed records; both signals: scopes and resources; metrics: metric
/// rows). A request past this is refused here with a clear reason, rather than left to silently
/// misattribute attributes (logs, which wrap in release builds) or fail deep in the converter.
const MAX_U16_ENTRIES: usize = u16::MAX as usize;

/// Refuse a count that would overflow a `u16` id in the converter.
fn check_u16_entries(what: &str, count: usize) -> Result<(), LakeError> {
    if count > MAX_U16_ENTRIES {
        return Err(LakeError::Invalid(format!(
            "OTLP request has {count} {what}, more than the {MAX_U16_ENTRIES} the encoder can \
             address without wrapping; {}",
            error::SPLIT_HINT
        )));
    }
    Ok(())
}

/// Per-table entry counts of a decoded OTLP request, each checked against the converter's `u16`
/// ids, in the order they are reported.
struct EntryCounts([(&'static str, usize); 3]);

impl EntryCounts {
    /// Refuse the first count that would overflow a `u16` id.
    fn check(&self) -> Result<(), LakeError> {
        for &(what, count) in &self.0 {
            check_u16_entries(what, count)?;
        }
        Ok(())
    }
}

/// Strictly decode an OTLP request (prost validates structure, nesting and UTF-8) and count the
/// entries the converter addresses with `u16` ids.
fn decode_otlp(raw: &OtlpProtoBytes) -> Result<EntryCounts, prost::DecodeError> {
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::trace::v1::ExportTraceServiceRequest;
    use prost::Message as _;
    match raw {
        OtlpProtoBytes::ExportLogsRequest(b) => {
            let req = ExportLogsServiceRequest::decode(b.clone())?;
            let mut scopes = 0;
            let mut attributed = 0;
            for rl in &req.resource_logs {
                scopes += rl.scope_logs.len();
                for sl in &rl.scope_logs {
                    // pdata assigns a log id only to records that carry attributes (encode/mod.rs);
                    // only those consume the u16 space that wraps.
                    attributed += sl
                        .log_records
                        .iter()
                        .filter(|r| !r.attributes.is_empty())
                        .count();
                }
            }
            Ok(EntryCounts([
                ("log records with attributes", attributed),
                ("scopes", scopes),
                ("resources", req.resource_logs.len()),
            ]))
        }
        OtlpProtoBytes::ExportMetricsRequest(b) => {
            let req = ExportMetricsServiceRequest::decode(b.clone())?;
            let mut scopes = 0;
            let mut metrics = 0;
            for rm in &req.resource_metrics {
                scopes += rm.scope_metrics.len();
                for sm in &rm.scope_metrics {
                    metrics += sm.metrics.len();
                }
            }
            Ok(EntryCounts([
                ("metrics", metrics),
                ("scopes", scopes),
                ("resources", req.resource_metrics.len()),
            ]))
        }
        OtlpProtoBytes::ExportTracesRequest(b) => {
            // Traces are refused later as unsupported; a strict decode still rejects garbage bytes.
            let _ = ExportTraceServiceRequest::decode(b.clone())?;
            Ok(EntryCounts([("spans", 0), ("scopes", 0), ("resources", 0)]))
        }
    }
}

/// The refusal of bytes that fail the strict decode, with prost's description of the defect.
fn malformed(e: &prost::DecodeError) -> LakeError {
    LakeError::Invalid(format!("malformed OTLP request: {e}"))
}

/// Strictly decode an OTLP request and refuse one that pdata's lenient conversion would mishandle.
///
/// The lenient `TryInto<OtapArrowRecords>` path stops at the first protobuf parse error and returns
/// the records before it as a non-empty batch (an acknowledged partial batch), panics with
/// `.expect(...)` on a value whose wire type does not match its field number, and recurses without
/// a depth bound when it CBOR-encodes a deeply nested value (a stack overflow that aborts the whole
/// process). A strict `prost` decode rejects the first two. It rejects the third only while prost
/// keeps its recursion limit of 100: a build that enables prost's `no-recursion-limit` feature
/// (the default `df_engine` build does, through `jemalloc_pprof` -> `pprof_util`) decodes any
/// depth here, so a deeply nested value can still overflow the stack in this decode. On success the
/// per-table entry counts are checked so a request that would overflow a `u16` id in the converter
/// is refused here instead of silently misattributing its attributes. OTAP input does not reach
/// this path, so its ids and nesting are handled during extraction instead.
///
/// prost also rejects invalid UTF-8 in string fields. When the strict decode fails and the request
/// is a logs or metrics request whose string fields can be repaired (`utf8::repair_otlp`: each
/// invalid sequence becomes U+FFFD), the repaired bytes are checked against `max_request_bytes`,
/// strict-decoded and count-checked again, and returned with the number of repaired strings.
/// `Ok(None)`: the request is valid as it is.
fn validate_otlp_request(
    raw: &OtlpProtoBytes,
    max_request_bytes: usize,
) -> Result<Option<(OtlpProtoBytes, u64)>, LakeError> {
    let defect = match decode_otlp(raw) {
        Ok(counts) => return counts.check().map(|()| None),
        Err(e) => e,
    };
    let (tree, wrap): (utf8::OtlpTree, fn(Bytes) -> OtlpProtoBytes) = match raw {
        OtlpProtoBytes::ExportLogsRequest(_) => {
            (utf8::OtlpTree::Logs, OtlpProtoBytes::ExportLogsRequest)
        }
        OtlpProtoBytes::ExportMetricsRequest(_) => (
            utf8::OtlpTree::Metrics,
            OtlpProtoBytes::ExportMetricsRequest,
        ),
        // Refused as unsupported anyway: no repair.
        OtlpProtoBytes::ExportTracesRequest(_) => return Err(malformed(&defect)),
    };
    // Not walkable, or nothing to repair: the defect is not (only) invalid UTF-8.
    let Ok(Some(repaired)) = utf8::repair_otlp(tree, raw.as_bytes()) else {
        return Err(malformed(&defect));
    };
    let observed = repaired.bytes.len();
    if observed > max_request_bytes {
        return Err(LakeError::TooLarge {
            setting: "ingress.max_request_bytes",
            observed,
            limit: max_request_bytes,
        });
    }
    let fixed = wrap(Bytes::from(repaired.bytes));
    decode_otlp(&fixed).map_err(|e| malformed(&e))?.check()?;
    Ok(Some((fixed, repaired.strings)))
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
            boot_id: uuid::Uuid::new_v4().simple().to_string(),
            wall: Rc::new(SystemWallClock),
            warned_unacked: false,
            refusal_logged: None,
            repair_logged: None,
            #[cfg(test)]
            store_override: None,
            #[cfg(test)]
            interval_override: None,
        })
    }

    fn interval(&self) -> Duration {
        #[cfg(test)]
        if let Some(interval) = self.interval_override {
            return interval;
        }
        self.config.window.interval
    }

    /// The loop state of a fresh start, writing to `store`.
    fn new_state(&self, store: Arc<dyn ObjectStore>) -> State {
        let schemas = Schemas::new();
        State {
            uploader: Uploader {
                store,
                schemas: schemas.clone(),
                local_base: match &self.config.storage {
                    StorageType::File { base_uri } => Some(base_uri.clone()),
                    // Object-store variants exist only with the `aws` / `azure` features.
                    #[allow(unreachable_patterns)]
                    _ => None,
                },
                initial_backoff: self.config.retry_initial_backoff,
                max_backoff: self.config.retry_max_backoff,
                limits: EncodeLimits::DEFAULT,
            },
            schemas,
            cache: SeriesCache::new(self.config.series_cache.max_entries),
            window: Window::new(self.interval()),
            active: Active::default(),
            flushing: None,
            parked: None,
            rotation_due: None,
            pending: PendingAcks::default(),
            next_seq: 0,
            closed_since: None,
            reported: CacheStats::default(),
        }
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

    async fn notify_all(
        &mut self,
        eh: &EffectHandler<OtapPdata>,
        done: Vec<Completion>,
        shutdown: bool,
    ) -> Result<(), Error> {
        for c in done {
            self.notify(eh, c, shutdown).await?;
        }
        Ok(())
    }

    /// Check the request size, convert the payload and extract its rows.
    fn extract(
        &self,
        token: &mut OtapPdata,
        schemas: &Schemas,
    ) -> Result<(Signal, Extracted), LakeError> {
        let limit = self.config.ingress.max_request_bytes;
        if let Some(observed) = token.num_bytes()
            && observed > limit
        {
            return Err(LakeError::TooLarge {
                setting: "ingress.max_request_bytes",
                observed,
                limit,
            });
        }
        let payload = token.take_payload();
        // Strict-decode OTLP bytes before pdata's lenient conversion runs on them below: the
        // lenient path acks a partial batch on a parse error, panics on a malformed value,
        // overflows the stack on deep nesting, and wraps u16 ids past 65535 attributed records.
        // A request whose only defect is invalid UTF-8 is replaced by its repaired bytes.
        // OTAP input (no `raw`) is not converted from OTLP and is validated during extraction.
        let repair = match payload.data() {
            PayloadData::OtlpBytes(raw) => validate_otlp_request(raw, limit)?,
            PayloadData::OtapArrowRecords(_) => None,
        };
        let (payload, otlp_repaired) = match repair {
            Some((raw, strings)) => {
                // Free the original bytes now: only one copy of the request stays resident.
                drop(payload);
                (OtapPayload::from(raw), strings)
            }
            None => (payload, 0),
        };
        let records: Result<OtapArrowRecords, _> = payload.try_into_with_default();
        let mut records = records.map_err(|e| LakeError::Conversion(e.to_string()))?;
        records
            .decode_transport_optimized_ids()
            .map_err(|e| LakeError::Conversion(e.to_string()))?;
        let limits = self.config.limits();
        let producer = self.config.producer_id_attribute.as_str();
        let (signal, mut extracted) = match &records {
            OtapArrowRecords::Logs(_) => (
                Signal::Logs,
                extract::extract_logs(&records, schemas, &limits, producer)?,
            ),
            OtapArrowRecords::Metrics(_) => (
                Signal::Metrics,
                extract::extract_metrics(&records, schemas, &limits, producer)?,
            ),
            OtapArrowRecords::Traces(_) => return Err(LakeError::Unsupported("traces")),
        };
        extracted.strings_repaired += otlp_repaired;
        Ok((signal, extracted))
    }

    /// Count a request whose invalid UTF-8 was repaired; log it at most once per second.
    fn note_repaired(&mut self, strings: u64) {
        if let Some(m) = self.metrics.as_mut() {
            m.requests_repaired.inc();
            m.strings_repaired.add(strings);
        }
        let now = Instant::now();
        if self
            .repair_logged
            .is_none_or(|t| now.duration_since(t) >= Duration::from_secs(1))
        {
            self.repair_logged = Some(now);
            otel_warn!(
                "parquet_lake.request.repaired",
                message = "invalid UTF-8 replaced with U+FFFD; logged at most once per second",
                strings = strings
            );
        }
    }

    /// Refuse a request permanently: the identical bytes would be refused again.
    async fn refuse(
        &mut self,
        eh: &EffectHandler<OtapPdata>,
        token: OtapPdata,
        e: &LakeError,
    ) -> Result<(), Error> {
        if let Some(m) = self.metrics.as_mut() {
            match e.refusal() {
                Refusal::TooLarge => m.refused_too_large.inc(),
                Refusal::Invalid => m.refused_invalid.inc(),
                Refusal::TooDeep => m.refused_too_deep.inc(),
                Refusal::Unsupported => m.refused_unsupported.inc(),
                Refusal::Other => m.refused_other.inc(),
            }
        }
        let now = Instant::now();
        if self
            .refusal_logged
            .is_none_or(|t| now.duration_since(t) >= Duration::from_secs(1))
        {
            self.refusal_logged = Some(now);
            otel_warn!(
                "parquet_lake.request.refused",
                message = "the request was refused permanently; at most one such line per second is logged",
                error = e.to_string()
            );
        }
        self.record_export(&token, Outcome::Failure, Duration::ZERO);
        eh.notify_nack(NackMsg::new_permanent_with_cause(
            format!("parquet_lake: {e}"),
            token,
            NackCause::Refused,
        ))
        .await
    }

    async fn handle_pdata(
        &mut self,
        eh: &EffectHandler<OtapPdata>,
        st: &mut State,
        mut token: OtapPdata,
    ) -> Result<(), Error> {
        if !st.accepting() {
            // The loop reads pdata only while admission is open, except during the engine's
            // forced drain at shutdown, which delivers what is already buffered in the channel.
            self.record_export(&token, Outcome::Failure, Duration::ZERO);
            return eh
                .notify_nack(NackMsg::new(
                    "parquet_lake: admission is closed; retry the request",
                    token,
                ))
                .await;
        }
        if !self.warned_unacked && !token.has_ack_or_nack_interests() {
            self.warned_unacked = true;
            otel_warn!(
                "parquet_lake.unacked_input",
                message = "a batch arrived without Ack/Nack subscribers: upstream acknowledged it before it landed (set wait_for_result: true on receivers), so a crash or failed block loses it"
            );
        }
        let (signal, extracted) = match self.extract(&mut token, &st.schemas) {
            Ok(v) => v,
            Err(e) => return self.refuse(eh, token, &e).await,
        };
        if extracted.timestamps_out_of_range > 0
            && let Some(m) = self.metrics.as_mut()
        {
            m.timestamps_out_of_range
                .add(extracted.timestamps_out_of_range);
        }
        if extracted.strings_repaired > 0 {
            self.note_repaired(extracted.strings_repaired);
        }
        let seq = st.pending.admit(token, Instant::now());
        let done = self.push_chunks(st, seq, signal, extracted.chunks.into());
        self.notify_all(eh, done, false).await
    }

    /// Push the chunks of request `seq` into ACTIVE. When a chunk does not fit, ACTIVE is rotated
    /// if the flush slot is free; otherwise the remaining chunks are parked and admission closes.
    /// Returns the completions that became due (normally at most the request's own).
    fn push_chunks(
        &mut self,
        st: &mut State,
        seq: u64,
        signal: Signal,
        mut chunks: VecDeque<Chunk>,
    ) -> Vec<Completion> {
        let mut done = Vec::new();
        let budget = self.config.window.max_block_bytes;
        while let Some(chunk) = chunks.pop_front() {
            if !st.active.is_empty() && st.active.bytes() + chunk.bytes() > budget {
                let _ = st.rotation_due.get_or_insert(FlushReason::Bytes);
                if st.flushing.is_some() {
                    chunks.push_front(chunk);
                    st.parked = Some(Parked {
                        seq,
                        signal,
                        chunks,
                    });
                    return done;
                }
                done.extend(self.rotate(st, None));
                if !st.pending.contains(seq) {
                    // A block of this request failed while rotating: it is already Nacked.
                    return done;
                }
            }
            let opened = *st
                .active
                .opened_nanos
                .get_or_insert_with(|| self.wall.now_unix_nanos());
            let partition = PartitionId::from_unix_secs(st.window.start_secs(opened));
            match st
                .active
                .block(signal)
                .push(chunk, seq, &mut st.cache, partition)
            {
                Ok(true) => st.pending.add_block(seq),
                Ok(false) => {}
                Err(e) => {
                    // A push leaves the block unchanged; an ACTIVE that stayed empty must not
                    // keep this window start for a later request.
                    if st.active.is_empty() {
                        st.active.opened_nanos = None;
                    }
                    let reason = format!("parquet_lake: {e}");
                    done.extend(st.pending.block_failed(seq, &reason, Instant::now()));
                    return done;
                }
            }
        }
        done.extend(st.pending.release_admission(seq, Instant::now()));
        done
    }

    /// Move ACTIVE into the flush slot, which must be free, and start its first block. During a
    /// shutdown the generation's deadline is also bounded by the shutdown deadline.
    fn rotate(&mut self, st: &mut State, shutdown: Option<Instant>) -> Vec<Completion> {
        let reason = st.rotation_due.take().unwrap_or(FlushReason::Time);
        let Some(opened) = st.active.opened_nanos.take() else {
            return Vec::new();
        };
        let window_start_secs = st.window.start_secs(opened);
        let seq = st.next_seq;
        st.next_seq += 1;
        let mut queued = VecDeque::new();
        for signal in [Signal::Logs, Signal::Metrics] {
            let block = st.active.block(signal);
            if block.is_empty() {
                continue;
            }
            let taken = block.take();
            queued.push_back(SealedBlock {
                id: BlockId {
                    signal,
                    window_start_secs,
                    writer_id: self.config.writer_id.clone(),
                    boot_id: self.boot_id.clone(),
                    seq,
                },
                series: taken.series,
                values: taken.values,
                batches: taken.batches,
            });
        }
        if let Some(m) = self.metrics.as_mut() {
            match reason {
                FlushReason::Time => m.flushes_time.inc(),
                FlushReason::Bytes => m.flushes_bytes.inc(),
                FlushReason::Shutdown => m.flushes_shutdown.inc(),
            }
        }
        let own = Instant::now() + self.config.window.flush_retry_deadline;
        let deadline = shutdown.map_or(own, |d| d.min(own));
        let meta = FileMeta {
            emitted_at_micros: self.wall.now_unix_nanos() / 1000,
            window_end_secs: st.window.end_secs(window_start_secs),
        };
        self.start_next(st, queued, deadline, meta)
    }

    /// Start the next queued block of a generation: drop the series rows that landed since they
    /// were buffered, and spawn the flush task. The previous block of the slot is resolved before
    /// this runs, so the cache reflects every earlier landing. Frees the slot when nothing is
    /// left. Returns the completions of blocks that could not be started.
    fn start_next(
        &mut self,
        st: &mut State,
        mut queued: VecDeque<SealedBlock>,
        deadline: Instant,
        meta: FileMeta,
    ) -> Vec<Completion> {
        let mut done = Vec::new();
        while let Some(block) = queued.pop_front() {
            let SealedBlock {
                id,
                series,
                values,
                batches,
            } = block;
            match prune_committed(series, &mut st.cache, id.partition()) {
                Ok((series, series_ids)) => {
                    let task = st
                        .uploader
                        .spawn(id.clone(), series, values, meta, deadline);
                    st.flushing = Some(Flushing {
                        current: InFlight {
                            id,
                            batches,
                            series_ids,
                            started: Instant::now(),
                            task,
                        },
                        queued,
                        deadline,
                        meta,
                    });
                    return done;
                }
                Err(e) => {
                    done.extend(self.fail_block(st, &id, batches, &e.to_string()));
                }
            }
        }
        st.flushing = None;
        done
    }

    /// A block did not land: Nack its requests (retryable).
    fn fail_block(
        &mut self,
        st: &mut State,
        id: &BlockId,
        batches: BTreeSet<u64>,
        reason: &str,
    ) -> Vec<Completion> {
        if let Some(m) = self.metrics.as_mut() {
            m.blocks_failed.inc();
        }
        otel_warn!(
            "parquet_lake.block.failed",
            block = id.to_string(),
            error = reason,
            batches = batches.len()
        );
        let reason = format!("parquet_lake: block failed: {reason}");
        let now = Instant::now();
        batches
            .into_iter()
            .filter_map(|seq| st.pending.block_failed(seq, &reason, now))
            .collect()
    }

    /// Resolve the block whose task finished, then start the generation's next block or free the
    /// slot. A task that panicked or was aborted counts as a failed block.
    fn on_flush_done(
        &mut self,
        st: &mut State,
        result: Result<Result<Landed, LakeError>, JoinError>,
    ) -> Vec<Completion> {
        let Some(Flushing {
            current,
            queued,
            deadline,
            meta,
        }) = st.flushing.take()
        else {
            return Vec::new();
        };
        let outcome = result.unwrap_or_else(|e| Err(LakeError::Task(e.to_string())));
        if let Some(m) = self.metrics.as_mut() {
            m.flush_duration
                .record(current.started.elapsed().as_secs_f64());
        }
        let mut done = match outcome {
            Ok(landed) => {
                // Only a landed block commits its series rows.
                let partition = current.id.partition();
                for id in &current.series_ids {
                    st.cache.mark_committed(*id, partition);
                }
                if let Some(m) = self.metrics.as_mut() {
                    m.blocks_landed.inc();
                    m.upload_retries.add(u64::from(landed.retries));
                    m.bytes_written.add(landed.bytes);
                    m.rows_written.add(landed.rows);
                    m.series_rows_written.add(landed.series_rows);
                    m.encoder_peak.record(landed.encoder_peak as f64);
                    m.slice_peak.record(landed.slice_peak as f64);
                }
                let now = Instant::now();
                current
                    .batches
                    .into_iter()
                    .filter_map(|seq| st.pending.block_landed(seq, now))
                    .collect()
            }
            Err(e) => self.fail_block(st, &current.id, current.batches, &e.to_string()),
        };
        done.extend(self.start_next(st, queued, deadline, meta));
        done
    }

    /// After any event: rotate a due generation once the slot is free, then give a parked request
    /// the room that opened.
    fn advance(&mut self, st: &mut State) -> Vec<Completion> {
        let mut done = Vec::new();
        if st.flushing.is_none() && st.rotation_due.is_some() {
            if st.active.is_empty() {
                st.rotation_due = None;
            } else {
                done.extend(self.rotate(st, None));
            }
        }
        if (st.rotation_due.is_none() || st.flushing.is_none())
            && let Some(parked) = st.parked.take()
            && st.pending.contains(parked.seq)
        {
            done.extend(self.push_chunks(st, parked.seq, parked.signal, parked.chunks));
        }
        done
    }

    /// Record how long admission stays closed.
    fn track_admission(&mut self, st: &mut State, accepting: bool) {
        match (accepting, st.closed_since) {
            (false, None) => st.closed_since = Some(Instant::now()),
            (true, Some(since)) => {
                st.closed_since = None;
                if let Some(m) = self.metrics.as_mut() {
                    m.admission_closed_duration
                        .record(since.elapsed().as_secs_f64());
                }
            }
            _ => {}
        }
    }

    /// Wait for the generation in the flush slot until `deadline`; abort and Nack what is left.
    async fn drain_slot(
        &mut self,
        eh: &EffectHandler<OtapPdata>,
        st: &mut State,
        deadline: Instant,
    ) -> Result<(), Error> {
        while st.flushing.is_some() {
            let result = tokio::select! {
                biased;
                r = flush_result(&mut st.flushing) => Some(r),
                () = clock::sleep_until(deadline) => None,
            };
            let done = match result {
                Some(r) => self.on_flush_done(st, r),
                None => {
                    let Some(f) = st.flushing.take() else { break };
                    let InFlight {
                        id, batches, task, ..
                    } = f.current;
                    // Aborts the task at its next await point.
                    drop(task);
                    let reason = "shutdown deadline reached";
                    let mut done = self.fail_block(st, &id, batches, reason);
                    for b in f.queued {
                        done.extend(self.fail_block(st, &b.id, b.batches, reason));
                    }
                    done
                }
            };
            self.notify_all(eh, done, true).await?;
        }
        Ok(())
    }

    /// Let the generation in the slot finish, flush ACTIVE if time remains, Nack anything left
    /// with `NodeShutdown`. Returns by `deadline` (plus the time the engine takes to accept the
    /// completions).
    async fn shutdown(
        &mut self,
        eh: &EffectHandler<OtapPdata>,
        st: &mut State,
        deadline: Instant,
    ) -> Result<TerminalState, Error> {
        if let Some(parked) = st.parked.take()
            && let Some(c) = st.pending.block_failed(
                parked.seq,
                "parquet_lake: shutdown before the request was buffered",
                Instant::now(),
            )
        {
            self.notify(eh, c, true).await?;
        }
        self.drain_slot(eh, st, deadline).await?;
        if !st.active.is_empty() && Instant::now() < deadline {
            st.rotation_due = Some(FlushReason::Shutdown);
            let done = self.rotate(st, Some(deadline));
            self.notify_all(eh, done, true).await?;
            self.drain_slot(eh, st, deadline).await?;
        }
        for c in st
            .pending
            .drain_nack("parquet_lake: shutdown before upload", Instant::now())
        {
            self.notify(eh, c, true).await?;
        }
        // The terminal snapshot carries the final cache and admission values; a closed admission
        // period ends here, with the node.
        self.track_admission(st, true);
        self.observe(st);
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

    /// Report the cache and admission gauges with the rest of the metric set.
    fn observe(&mut self, st: &mut State) {
        let Some(m) = self.metrics.as_mut() else {
            return;
        };
        let stats = st.cache.stats();
        m.cache_entries.set(st.cache.len() as u64);
        m.cache_hits.add(stats.hits - st.reported.hits);
        m.cache_misses.add(stats.misses - st.reported.misses);
        m.cache_evictions
            .add(stats.evictions - st.reported.evictions);
        m.admission_closed.set(u64::from(st.closed_since.is_some()));
        st.reported = stats;
    }
}

/// What the node loop woke up for.
enum Event {
    Flushed(Result<Result<Landed, LakeError>, JoinError>),
    Window,
    Inbox(Message<OtapPdata>),
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
        // No `message`: the log encoder gives every field half of the remaining record buffer,
        // and a sentence here would push the numeric fields out of the record.
        otel_info!(
            "parquet_lake.start",
            writer_id = self.config.writer_id.as_str(),
            boot_id = self.boot_id.as_str(),
            window_interval_secs = self.config.window.interval.as_secs(),
            max_block_bytes = self.config.window.max_block_bytes as u64,
            worst_case_flush_bytes = self.config.worst_case_flush_bytes() as u64
        );
        let mut st = self.new_state(store);
        // The exporter's own sleep, not an engine periodic timer: the engine cancels periodic
        // timers before it drains a node.
        let mut wake = clock::sleep(st.window.until_boundary(self.wall.now_unix_nanos()));
        loop {
            // Backpressure: pdata is not read while ACTIVE waits for the flush slot or a request
            // is parked. Control messages always flow.
            let accepting = st.accepting();
            self.track_admission(&mut st, accepting);
            let event = tokio::select! {
                biased;
                r = flush_result(&mut st.flushing) => Event::Flushed(r),
                () = &mut wake => Event::Window,
                msg = inbox.recv_when(accepting) => Event::Inbox(msg?),
            };
            match event {
                Event::Flushed(result) => {
                    let done = self.on_flush_done(&mut st, result);
                    self.notify_all(&effect_handler, done, false).await?;
                }
                Event::Window => {
                    if !st.active.is_empty() {
                        let _ = st.rotation_due.get_or_insert(FlushReason::Time);
                    }
                    wake = clock::sleep(st.window.until_boundary(self.wall.now_unix_nanos()));
                }
                Event::Inbox(Message::Control(NodeControlMsg::CollectTelemetry {
                    mut metrics_reporter,
                })) => {
                    self.observe(&mut st);
                    if let Some(m) = self.pdata_metrics.as_mut() {
                        let _ = metrics_reporter.report_measurement(m);
                    }
                    if let Some(m) = self.metrics.as_mut() {
                        let _ = metrics_reporter.report(m);
                    }
                }
                Event::Inbox(Message::Control(NodeControlMsg::Shutdown { deadline, .. })) => {
                    return self.shutdown(&effect_handler, &mut st, deadline).await;
                }
                Event::Inbox(Message::Control(_)) => {}
                Event::Inbox(Message::PData(pdata)) => {
                    self.handle_pdata(&effect_handler, &mut st, pdata).await?;
                }
            }
            let done = self.advance(&mut st);
            self.notify_all(&effect_handler, done, false).await?;
        }
    }
}
