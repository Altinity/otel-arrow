// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! `exporter:series_parquet`: the factory and the node's select loop.
//!
//! The block state machine is documented in the `worker` module, the window
//! clock in `window`, and the operating contract in the README.

use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::ExporterFactory;
use otel_arrow_dfe_engine::config::ExporterConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::NodeControlMsg;
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::local::exporter::{EffectHandler, Exporter};
use otel_arrow_dfe_engine::message::{ExporterInbox, Message};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_otap::OTAP_EXPORTER_FACTORIES;
use otel_arrow_dfe_otap::log_gate::LogGate;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_series_lake as lake;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

/// Registered component identifier.
pub const SERIES_PARQUET_EXPORTER_URN: &str = "urn:otel:exporter:series_parquet";

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = SERIES_PARQUET_EXPORTER_URN,
    target = "otel.exporter.series_parquet",
);

pub mod config;
mod flush;
mod metrics;
mod outcome;
#[cfg(test)]
mod tests;
mod token;
mod window;
mod worker;

/// Completion sends the node loop takes before it yields to its other
/// branches.
const NOTIFY_BATCH: usize = 64;

/// Declares the series Parquet exporter as a local exporter factory.
///
/// Unsafe code is temporarily used here to allow the use of the
/// `distributed_slice` macro. This macro is part of the `linkme` crate which is
/// considered safe and well maintained.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Exporter)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static SERIES_PARQUET: ExporterFactory<OtapPdata> = ExporterFactory {
    name: SERIES_PARQUET_EXPORTER_URN,
    create: |pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             exporter_config: &ExporterConfig,
             capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        let config: config::Config =
            serde_json::from_value(node_config.config.clone()).map_err(|e| {
                otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                    error: e.to_string(),
                }
            })?;
        let mut exporter = SeriesParquet::new(config);
        exporter.metrics = Some(metrics::Metrics::register(&pipeline, &exporter.config.lake));
        exporter.num_cores = pipeline.num_cores();
        exporter.token_provider = otel_arrow_dfe_otap::object_store::required_token_provider(
            &exporter.config.storage,
            capabilities,
        )?;
        Ok(ExporterWrapper::local(
            exporter,
            node,
            node_config,
            exporter_config,
        ))
    },
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<config::Config>,
};

/// One independently budgeted pipeline worker.
pub struct SeriesParquet {
    config: config::Config,
    token_provider: Option<
        Box<dyn otel_arrow_dfe_engine::shared::capability::auth::bearer_token_provider::BearerTokenProvider>,
    >,
    /// Instruments registered by the factory, moved into the worker at start.
    metrics: Option<metrics::Metrics>,
    /// Cores the engine runs this pipeline on: one worker, and one memory
    /// budget, per core.
    num_cores: usize,
    /// Object store a test drives [`Exporter::start`] with instead of the
    /// configured one.
    #[cfg(test)]
    store_override: Option<Arc<dyn object_store::ObjectStore>>,
}

impl SeriesParquet {
    /// Construct an exporter from validated configuration.
    #[must_use]
    pub fn new(config: config::Config) -> Self {
        Self {
            config,
            token_provider: None,
            metrics: None,
            num_cores: 1,
            #[cfg(test)]
            store_override: None,
        }
    }
}

#[async_trait(?Send)]
impl Exporter<OtapPdata> for SeriesParquet {
    async fn start(
        mut self: Box<Self>,
        inbox: ExporterInbox<OtapPdata>,
        effects: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        if self.config.retry.is_some()
            && matches!(
                self.config.storage,
                otel_arrow_dfe_otap::object_store::StorageType::File { .. }
            )
        {
            otel_warn!(
                "series_parquet.retry_ignored_for_file_storage",
                message = "retry settings are not applied to local file storage (invalid values are still rejected)"
            );
        }
        let store = otel_arrow_dfe_otap::object_store::exporter_store(
            effects.exporter_id(),
            &self.config.storage,
            self.config.retry.as_ref(),
            self.token_provider.take(),
        )?;
        let unsigned_payload =
            otel_arrow_dfe_otap::object_store::s3_unsigned_payload(&self.config.storage);
        #[cfg(test)]
        let store = self.store_override.take().unwrap_or(store);
        let storage = self.config.storage.kind().to_owned();
        static STARTED: AtomicBool = AtomicBool::new(false);
        run_announced(
            self.config.clone(),
            store,
            Arc::new(lake::clock::SystemWallClock),
            inbox,
            effects,
            self.metrics.take(),
            Startup {
                storage,
                num_cores: self.num_cores,
                unsigned_payload,
                first_in_process: !STARTED.swap(true, Ordering::Relaxed),
            },
        )
        .await
    }
}

/// What the start event reports beyond the worker itself.
struct Startup {
    /// Storage backend name.
    storage: String,
    /// Workers the engine runs, one per core.
    num_cores: usize,
    /// Whether S3 requests sign `UNSIGNED-PAYLOAD`; `None` for other storage.
    unsigned_payload: Option<bool>,
    /// Whether this is the process's first worker to start. Every worker of
    /// a process shares the configuration checks, so only the first logs
    /// their warnings at WARN, and the others at DEBUG.
    first_in_process: bool,
}

/// Where a memory limit comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemoryLimitSource {
    /// `MemTotal` of `/proc/meminfo`.
    Meminfo,
    /// The process's cgroup memory limit.
    Cgroup,
}

impl MemoryLimitSource {
    /// The name the oversubscription warning reports.
    const fn as_str(self) -> &'static str {
        match self {
            Self::Meminfo => "meminfo",
            Self::Cgroup => "cgroup",
        }
    }
}

/// The memory this process may use: `MemTotal` of `meminfo`, or the cgroup
/// limit when it is lower or `MemTotal` is unknown.
fn memory_limit(
    meminfo: Option<&str>,
    cgroup_limit: Option<u64>,
) -> Option<(u64, MemoryLimitSource)> {
    let total = meminfo.and_then(|meminfo| {
        let line = meminfo.lines().find(|line| line.starts_with("MemTotal:"))?;
        let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
        kib.checked_mul(1024)
    });
    match (total, cgroup_limit) {
        (Some(total), Some(limit)) if limit < total => Some((limit, MemoryLimitSource::Cgroup)),
        (Some(total), _) => Some((total, MemoryLimitSource::Meminfo)),
        (None, limit) => limit.map(|limit| (limit, MemoryLimitSource::Cgroup)),
    }
}

/// [`memory_limit`] of this host and process.
fn process_memory_limit() -> Option<(u64, MemoryLimitSource)> {
    memory_limit(
        std::fs::read_to_string("/proc/meminfo").ok().as_deref(),
        otel_arrow_dfe_engine::memory_limiter::cgroup_memory_limit_bytes(),
    )
}

/// Whether `cfg` leaves `writer_id` at its default.
fn writer_id_is_default(cfg: &lake::config::LakeConfig) -> bool {
    cfg.writer_id == lake::config::LakeConfig::default().writer_id
}

/// Emit the start event (writer id, boot id, storage, budget, S3 payload
/// signing and the logs identity configuration), a warning when `writer_id`
/// is left at its default, one when every core's memory budget together
/// exceeds the memory this process may use, one when a file as large as a
/// block could need more multipart parts than S3 allows, and a statement that
/// the upstream receiver's message limit is not visible from here.
fn announce(worker: &worker::Worker, startup: &Startup) {
    // The configuration checks: at the given level for the first worker of
    // the process, at DEBUG for the others.
    macro_rules! check {
        ($first:ident, $($arg:tt)*) => {
            if startup.first_in_process {
                $first!($($arg)*);
            } else {
                otel_debug!($($arg)*);
            }
        };
    }
    let budget = worker.budget_bytes();
    let total = budget.saturating_mul(startup.num_cores as u64);
    let logs = lake::canonical::Signal::Logs;
    let identity = worker.cfg.lake.identity_config(logs);
    let identity_hash = worker.cfg.lake.identity_config_hash(logs);
    otel_info!(
        "series_parquet.start",
        writer_id = worker.cfg.lake.writer_id.as_str(),
        boot_id = worker.boot_id.as_str(),
        storage = startup.storage.as_str(),
        num_cores = startup.num_cores,
        memory_budget_bytes = budget,
        unsigned_payload = match startup.unsigned_payload {
            Some(true) => "on",
            Some(false) => "off",
            None => "not_applicable",
        },
        logs_identity_config = identity.as_str(),
        logs_identity_config_hash = identity_hash.as_str()
    );
    if let Some((limit, source)) = process_memory_limit()
        && total > limit
    {
        check!(
            otel_warn,
            "series_parquet.memory_budget.oversubscribed",
            memory_budget_bytes = budget,
            num_cores = startup.num_cores,
            total_budget_bytes = total,
            memory_limit_bytes = limit,
            memory_limit_source = source.as_str(),
            message = "every worker's memory budget together exceeds the memory this process \
                       may use (the container limit or physical memory), before the requests \
                       a durable_buffer or receiver holds for each worker; lower the block and \
                       ingress budgets or run on fewer cores"
        );
    }
    let cfg = &worker.cfg.lake;
    if writer_id_is_default(cfg) {
        check!(
            otel_warn,
            "series_parquet.writer_id.default",
            writer_id = cfg.writer_id.as_str(),
            message = "writer_id is left at its default; object names stay unique through \
                       the boot id, but a freshness check keyed on the writer id cannot tell \
                       engines apart: set it per engine, such as the pod or host name"
        );
    }
    // An exporter is given no view of the nodes upstream of it, so it cannot
    // compare the receiver's decoding limit with its own request limit.
    check!(
        otel_info,
        "series_parquet.receiver_limit.unverified",
        max_request_bytes = cfg.ingress.max_request_bytes,
        receiver_default_bytes =
            otel_arrow_dfe_otap::otap_grpc::server_settings::DEFAULT_MAX_DECODING_MESSAGE_SIZE,
        message = "this exporter cannot see the upstream receiver's \
                   max_decoding_message_size; set it to at least \
                   ingress.max_request_bytes, or the receiver refuses larger requests \
                   with INVALID_ARGUMENT, which OTLP clients drop"
    );
    let parts = cfg.parts_per_block();
    if parts > lake::config::MAX_PARTS {
        check!(
            otel_warn,
            "series_parquet.upload.parts_exceed_limit",
            max_block_bytes = cfg.ingress.max_block_bytes,
            part_bytes = cfg.upload.part_bytes,
            parts = parts,
            max_parts = lake::config::MAX_PARTS,
            message = "a file as large as window.max_block_bytes would need more multipart \
                       parts than S3 allows; raise upload.part_bytes or lower \
                       window.max_block_bytes"
        );
    }
}

/// Drive one worker with a test start event; see [`drive`].
#[cfg(test)]
async fn run(
    cfg: config::Config,
    store: Arc<dyn object_store::ObjectStore>,
    wall: Arc<dyn lake::clock::WallClock>,
    inbox: ExporterInbox<OtapPdata>,
    effects: EffectHandler<OtapPdata>,
    metrics: Option<metrics::Metrics>,
) -> Result<TerminalState, Error> {
    run_announced(
        cfg,
        store,
        wall,
        inbox,
        effects,
        metrics,
        Startup {
            storage: "test".to_owned(),
            num_cores: 1,
            unsigned_payload: None,
            first_in_process: false,
        },
    )
    .await
}

/// [`run`], with what the start event reports beyond the worker itself.
async fn run_announced(
    cfg: config::Config,
    store: Arc<dyn object_store::ObjectStore>,
    wall: Arc<dyn lake::clock::WallClock>,
    inbox: ExporterInbox<OtapPdata>,
    effects: EffectHandler<OtapPdata>,
    metrics: Option<metrics::Metrics>,
    startup: Startup,
) -> Result<TerminalState, Error> {
    let mut worker = worker::Worker::new(cfg, store, wall, effects);
    worker.set_metrics(metrics);
    announce(&worker, &startup);
    drive(&mut worker, inbox).await
}

/// Emit the outcome of one shutdown: what the worker took in over its life,
/// how it was decided, and how long the drain took.
fn summarize(worker: &worker::Worker, since: Option<Instant>, deadline_exceeded: bool) {
    let outcomes = worker.notify.outcomes();
    let acked = outcomes[outcome::Outcome::Ack as usize];
    let nacked = outcomes.iter().sum::<u64>() - acked;
    let duration =
        since.map(|since| otel_arrow_dfe_engine::clock::now().saturating_duration_since(since));
    otel_info!(
        "series_parquet.shutdown.complete",
        accepted = worker.accepted,
        acked = acked,
        nacked = nacked,
        abandoned = worker.abandoned,
        deadline_exceeded = deadline_exceeded,
        duration = ?duration
    );
}

/// Drive one worker until shutdown completes or its deadline elapses.
///
/// The branches are ordered: the shutdown deadline first, so the node cancels
/// on time under an always-ready boundary; then the window boundary, a
/// resolved flush, completion delivery, the release of a decided block's
/// flush slot, rotation, and only then a new message. `accept` is false while
/// the ACTIVE block waits to be rotated. The shutdown begins with the
/// Shutdown control message and its deadline. Before it, while the inbox has
/// latched a Shutdown and still drains the buffered requests, a request is
/// admitted while admission is open and refused with a retryable
/// `NodeShutdown` nack while it is closed (see `Notifier::force_shutdown`).
/// The worker is borrowed so a test can inspect it after the loop returns.
async fn drive(
    worker: &mut worker::Worker,
    mut inbox: ExporterInbox<OtapPdata>,
) -> Result<TerminalState, Error> {
    // The engine releases the Shutdown control message only once the upstream
    // channel is empty and closed, so its deadline here also means that no
    // request can arrive and that another receive would fail.
    let mut closed: Option<Instant> = None;
    // When the Shutdown control message arrived, for the drain duration.
    let mut closed_at: Option<Instant> = None;
    let mut notify_turns = 0_usize;
    let mut notify_failures = LogGate::new();
    loop {
        if let Some(deadline) = closed
            && worker.is_idle()
        {
            // Sampled at collection and termination only: the scan is linear
            // in the live requests.
            worker.sample_metrics();
            summarize(worker, closed_at, false);
            return Ok(TerminalState::new(deadline, worker.metric_snapshots()));
        }
        let accept = worker.accept();
        worker.observe_admission(accept);
        let deadline = worker.deadline;
        tokio::select! {
            biased;

            // The flush cut outranks every other branch: whatever is still
            // outstanding is cancelled and decided, and the decisions are
            // delivered until the shutdown deadline itself.
            () = async {
                match deadline {
                    Some(d) => otel_arrow_dfe_engine::clock::sleep_until(d).await,
                    None => std::future::pending().await,
                }
            } => {
                // An idle worker whose sender is still open reaches the cut
                // on an ordinary shutdown with nothing to decide.
                let held = !worker.is_idle();
                if held {
                    otel_warn!("series_parquet.shutdown.cut", held = true);
                } else {
                    otel_info!("series_parquet.shutdown.cut", held = false);
                }
                let deadline_exceeded = worker.abandon().await;
                if deadline_exceeded {
                    otel_warn!("series_parquet.shutdown.deadline_exceeded");
                }
                worker.sample_metrics();
                summarize(worker, closed_at, deadline_exceeded);
                return Ok(TerminalState::new(
                    worker
                        .terminal
                        .or(deadline)
                        .expect("the cut branch only fires with a deadline"),
                    worker.metric_snapshots(),
                ));
            }

            // The window boundary is the rotation trigger, so it is served
            // before anything that could add to the block that is about to be
            // sealed. `wake` re-arms the sleep whether or not it rotated, so a
            // rotation the flush slot cannot take yet still keeps its timer.
            () = worker.window.sleep.as_mut(), if deadline.is_none() => {
                worker.wake_window();
                notify_turns = 0;
            }

            // A resolved flush decides a block's requests, so it is served
            // before anything that could add to the next block.
            done = async {
                match worker.flushing.as_mut() {
                    Some(job) => job.finish().await,
                    None => std::future::pending().await,
                }
            } => {
                // The decided block moves into the cleanup half of the same
                // FLUSHING slot, so no rotation can be served here: the one a
                // parked request waits for is served, with its resume in the
                // same turn, once the cleanup branch below frees the slot.
                worker.complete(done);
                notify_turns = 0;
            }

            // Bounded so a long completion backlog cannot starve the flush and
            // receive branches; `NOTIFY_BATCH` sends are taken before the loop
            // yields and reopens them.
            result = worker.notify.next(),
                if !worker.notify.is_empty() && notify_turns < NOTIFY_BATCH => {
                if let Err(e) = result
                    && let Some(suppressed) = notify_failures.admit(otel_arrow_dfe_engine::clock::now())
                {
                    otel_warn!(
                        "series_parquet.notify.failed",
                        error = %e,
                        suppressed = suppressed
                    );
                }
                notify_turns += 1;
            }

            // A decided block's task holds the flush slot until it has
            // released the write it was cancelling; joining it frees the slot.
            // It owes no completion, so it ranks below the branches that do.
            cleaned = async {
                match worker.cleaning.as_mut() {
                    Some(job) => job.cleanup().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Err(error) = cleaned {
                    otel_warn!("series_parquet.flush.cleanup_failed", error = %error);
                }
                let _ = worker.cleaning.take();
                if worker.rotation_requested {
                    worker.rotate();
                }
                worker.resume_pending();
                notify_turns = 0;
            }

            // Always ready when it is enabled, so a requested rotation happens
            // before the next message is taken. An empty ACTIVE block needs
            // no flush slot to be replaced.
            () = std::future::ready(()),
                if worker.rotation_requested
                    && (worker.active.data.is_empty()
                        || (worker.flushing.is_none() && worker.cleaning.is_none())) => {
                worker.rotate();
                worker.resume_pending();
                notify_turns = 0;
            }

            message = inbox.recv_when(accept), if closed.is_none() => {
                notify_turns = 0;
                match message {
                    Ok(Message::PData(data)) => {
                        if accept {
                            worker.admit(data);
                        } else {
                            // Admission is closed, so the inbox force-drains
                            // it behind a latched Shutdown: refused at once.
                            worker.force_shutdown(data);
                        }
                    }
                    Ok(Message::Control(NodeControlMsg::Shutdown { deadline, reason })) => {
                        otel_info!("series_parquet.shutdown", reason = reason);
                        closed = Some(deadline);
                        closed_at = Some(otel_arrow_dfe_engine::clock::now());
                        worker.shutdown(deadline);
                    }
                    Ok(Message::Control(NodeControlMsg::CollectTelemetry {
                        mut metrics_reporter,
                    })) => {
                        worker.sample_metrics();
                        worker.report_metrics(&mut metrics_reporter);
                    }
                    Ok(Message::Control(_)) => {}
                    Err(e) => {

                        // The inbox closes only when it releases the latched
                        // Shutdown, so this is a channel failure with no
                        // deadline: everything still held is decided before
                        // the error is reported.
                        otel_warn!("series_parquet.inbox.failed", error = %e);
                        let _ = worker.abandon().await;
                        return Err(e.into());
                    }
                }
            }

            // Reopens the notification branch after its batch, once every other
            // branch has had a turn.
            () = tokio::task::yield_now(), if notify_turns >= NOTIFY_BATCH => {
                notify_turns = 0;
            }
        }
    }
}
