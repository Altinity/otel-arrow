// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! One ACTIVE block, at most one FLUSHING block, one parked request, and the
//! decisions they owe.
//!
//! A request is acknowledged only once its block is written and its
//! descriptors are committed. A block-scoped refusal parks the request instead
//! of nacking it, and admission closes while a request is parked or a rotation
//! waits for the flush slot, so the worker never needs a third block.

use super::config::Config;
use super::flush::{self, FlushDone, FlushJob, FlushShared};
use super::metrics::{
    DatasetAttrs, EmitAttrs, EmitReason, FlushAttrs, FlushReason, LateCommit, LateCommitAttrs,
    Metrics, NackAttrs,
};
use super::outcome::{self, Outcome, StorageFailed, WriteFailure};
use super::token::{AckToken, Notifier};
use super::window::Window;
use lake::buffer::Block;
use lake::cache::SeriesCache;
use lake::clock::{WallClock, nanos_to_micros, nanos_to_secs};
use lake::config::LakeConfig;
use lake::extract::Extracted;
use otel_arrow_dfe_engine::clock;
use otel_arrow_dfe_engine::local::exporter::EffectHandler;
use otel_arrow_dfe_otap::log_gate::LogGate;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::encode::count_utf8_repairs;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::{OtapPayload, TryIntoWithOptions};
use otel_arrow_dfe_series_lake as lake;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Bytes charged per bounded descriptor cache entry.
///
/// An entry of the series cache (`lru::LruCache<SeriesId, Option<PartitionId>>`)
/// is a boxed node holding the key, the value and two links, plus a hash table
/// slot holding a key pointer, a node pointer and a control byte. 128 is a
/// deliberate over-estimate of that; the assertion below checks it against the
/// node rounded up to its allocator size class and a table at half occupancy,
/// just after it grows.
pub(super) const CACHE_ENTRY_BYTES: u64 = 128;

const _: () = {
    let node = (size_of::<lake::canonical::SeriesId>()
        + size_of::<Option<lake::clock::PartitionId>>()
        + 2 * size_of::<usize>())
    .next_power_of_two();
    let slot = 2 * (2 * size_of::<usize>() + 1) * 8 / 7 + 1;
    assert!(node + slot <= CACHE_ENTRY_BYTES as usize);
};

/// How long before the shutdown deadline a `durable_buffer` in front stops
/// recording the completions it receives: its `SHUTDOWN_PERSIST_RESERVE`,
/// which it keeps for its final persist. Keep the two equal.
pub(super) const DURABLE_BUFFER_RECORDING_RESERVE: Duration = Duration::from_secs(1);

/// Time a decided completion is given to reach the node upstream: a hop
/// through the engine's completion dispatcher, which retries a full control
/// channel every 5 ms.
pub(super) const COMPLETION_DISPATCH_MARGIN: Duration = Duration::from_millis(200);

/// How long before the shutdown deadline the worker stops writing and
/// decides everything it holds, so its completions reach a durable buffer in
/// front while it still records them: [`DURABLE_BUFFER_RECORDING_RESERVE`]
/// plus [`COMPLETION_DISPATCH_MARGIN`].
pub(super) const SHUTDOWN_COMPLETION_MARGIN: Duration =
    DURABLE_BUFFER_RECORDING_RESERVE.saturating_add(COMPLETION_DISPATCH_MARGIN);

/// Allocator, runtime and library overhead a worker is allowed beyond the
/// buffers it accounts for itself. The value is a judgment, not a
/// measurement: a margin for allocator fragmentation, the worker's runtime and
/// the Parquet and object store clients' own state.
const FIXED_WORKSPACE_BYTES: u64 = 64 * 1024 * 1024;

/// Open and closed history of pdata admission, for telemetry.
///
/// The node loop reports the gate once per turn; only a change of state reads
/// the clock, so an open gate costs one comparison per turn.
#[derive(Debug, Default)]
pub(super) struct AdmissionGate {
    /// When the current closure began, while admission is closed.
    closed_since: Option<Instant>,
    /// Transitions from open to closed.
    closures: u64,
    /// Time spent closed in closures that have ended.
    closed_total: Duration,
}

impl AdmissionGate {
    /// Record whether admission is open on this loop turn.
    pub(super) fn observe(&mut self, open: bool) {
        match (open, self.closed_since) {
            (false, None) => {
                self.closed_since = Some(clock::now());
                self.closures += 1;
            }
            (true, Some(since)) => {
                self.closed_total += clock::now().saturating_duration_since(since);
                self.closed_since = None;
            }
            _ => {}
        }
    }

    /// Whether admission is closed at `now`, how many closures began, and
    /// the seconds spent closed, the closure in progress included.
    pub(super) fn sample(&self, now: Instant) -> (bool, u64, f64) {
        let current = self
            .closed_since
            .map_or(Duration::ZERO, |since| now.saturating_duration_since(since));
        (
            self.closed_since.is_some(),
            self.closures,
            (self.closed_total + current).as_secs_f64(),
        )
    }
}

/// A block together with the completions of every request admitted to it.
///
/// The block itself only counts its requests: the completions are kept beside
/// it so a flush task can own the data without owning the routing contexts.
pub(super) struct OwnedBlock {
    /// Rows, descriptors and accounting.
    pub(super) data: Block,
    /// One completion per admitted request, in admission order.
    pub(super) tokens: Vec<AckToken>,
    /// Whether this block must repeat descriptors the cache already reports
    /// committed in its partition.
    ///
    /// True only for the block that replaces one sealed inside the same aligned
    /// window (see `Block::reserve_with_reemit`).
    pub(super) reemit: bool,
    /// Descriptor rows admitted to this block, per [`EmitReason`] position.
    ///
    /// Carried with the block and reported only after a successful write, so
    /// an abandoned block credits nothing.
    pub(super) emitted: [u64; 3],
}

/// One extracted request waiting for a block that can take it.
///
/// The payload and the conversion's record batches are already gone, so
/// parking holds only the extracted rows and the completion.
pub(super) struct Pending {
    /// The rows the request contributed.
    pub(super) extracted: Extracted,
    /// The completion the request is still owed.
    pub(super) token: AckToken,
    /// Wall-clock second the request was prepared at, for window alignment.
    pub(super) admission_secs: i64,
    /// String values the conversion stored with U+FFFD in place of invalid
    /// UTF-8.
    pub(super) utf8_repairs: u64,
}

/// What preparing one request produced.
///
/// Preparation always decides the request: either its rows are ready to be
/// offered to a block, or it owes its sender a refusal. Both arms are handled
/// at the single call site, and a refusal is a normal outcome of admission.
pub(super) enum Prepared {
    /// The request's rows, ready to be offered to a block.
    Ready(Pending),
    /// The request failed and its completion is still owed; the error's
    /// [`Outcome`] decides how. The producer id is the request's, cut to
    /// [`lake::extract::PRODUCER_ID_LOG_BYTES`], when its records were
    /// converted far enough to tell.
    Failed(AckToken, lake::Error, Option<String>),
}

#[cfg(test)]
impl Prepared {
    /// Release the request undecided, for a test that only inspects it.
    pub(super) fn discard(self) {
        match self {
            Prepared::Ready(pending) => pending.token.discard(),
            Prepared::Failed(token, _, _) => token.discard(),
        }
    }
}

/// Refuse a request whose logical size is over `limit`, and one whose size
/// is unknown: `num_bytes` is `None` only when Arrow cannot size the records,
/// which a well-formed batch never causes.
pub(super) fn check_request_size(bytes: Option<usize>, limit: usize) -> lake::Result<()> {
    match bytes {
        Some(n) if n <= limit => Ok(()),
        Some(n) => Err(lake::Error::too_large(lake::SizeBudget::Request, n, limit)),
        None => Err(lake::Error::invalid(
            "the request's Arrow records cannot be measured",
        )),
    }
}

/// One [`LogGate`] per refusal outcome, so a flood of one kind of refusal
/// never hides another.
#[derive(Debug)]
pub(super) struct RefusalGates([LogGate; Outcome::ALL.len()]);

impl RefusalGates {
    /// Gates that have let no line through yet.
    pub(super) const fn new() -> Self {
        Self([const { LogGate::new() }; Outcome::ALL.len()])
    }

    /// [`LogGate::admit`] for a refusal decided as `outcome`.
    pub(super) fn admit(&mut self, outcome: Outcome, now: Instant) -> Option<u64> {
        self.0[outcome as usize].admit(now)
    }
}

/// The ACTIVE and FLUSHING pair of one exporter instance.
pub(super) struct Worker {
    /// Validated user configuration.
    pub(super) cfg: Config,
    /// The lake configuration every block of this worker is opened under.
    lake_cfg: Arc<LakeConfig>,
    /// The one block open for admission.
    pub(super) active: OwnedBlock,
    /// The one block being written, if any.
    pub(super) flushing: Option<FlushJob>,
    /// The decided block whose supervising task is still releasing what it
    /// owns.
    ///
    /// The same FLUSHING slot, not a third block: it owes no completion, and
    /// holds only the task that must finish cancelling an abandoned write
    /// before another block may be written to the same file names.
    pub(super) cleaning: Option<FlushJob>,
    /// The one extracted request no block could take yet.
    pub(super) pending: Option<Pending>,
    /// Descriptors already written for a partition.
    pub(super) cache: SeriesCache,
    /// Delivery of decided completions.
    pub(super) notify: Notifier,
    /// Whether the ACTIVE block should be sealed at the next opportunity.
    pub(super) rotation_requested: bool,
    /// When the worker decides everything it holds: the shutdown deadline
    /// less [`SHUTDOWN_COMPLETION_MARGIN`], once shutdown has begun.
    pub(super) deadline: Option<Instant>,
    /// The shutdown deadline itself, once shutdown has begun: the completions
    /// decided at [`Worker::deadline`] are delivered until then.
    pub(super) terminal: Option<Instant>,
    /// The aligned window the ACTIVE block belongs to, and its boundary sleep.
    pub(super) window: Window,
    /// Source of wall-clock time for window alignment and seal stamps.
    wall: Arc<dyn WallClock>,
    /// Shared with each flush task for the duration of its write.
    pub(super) sink: Rc<lake::sink::Sink>,
    /// Per-worker block sequence, used in file names.
    seq: u64,
    /// What will have asked for the next rotation.
    pub(super) reason: FlushReason,
    /// Largest completion token the worker has held, for capacity reporting.
    pub(super) token_high_water: usize,
    /// Telemetry scans taken, which a test pins to collections.
    #[cfg(test)]
    pub(super) samples: u64,
    /// Rate limits of the per-request refusal WARN, one per outcome.
    refusals: RefusalGates,
    /// Whether pdata admission is open, and how long it has been closed.
    pub(super) admission: AdmissionGate,
    /// Random id of this worker's incarnation, the file-name segment that
    /// keeps two runs of one `writer_id` apart.
    pub(super) boot_id: String,
    /// Requests handed to [`Worker::admit`], whatever became of them.
    pub(super) accepted: u64,
    /// Completions decided by [`Worker::abandon`] at the shutdown deadline.
    pub(super) abandoned: u64,
    /// Registered instruments, once the node has a pipeline context.
    ///
    /// `None` for a worker a test drives directly; every call site is then a
    /// no-op.
    pub(super) metrics: Option<Metrics>,
    /// The store every flush task probes, and what the tasks count as it
    /// happens, moved into the metrics on each sample.
    flush_shared: Rc<FlushShared>,
}

/// A test that drops a worker still holding completions tears them down with
/// it; a completion dropped any other way trips its drop bomb.
#[cfg(test)]
impl Drop for Worker {
    fn drop(&mut self) {
        let flushing = self
            .flushing
            .iter_mut()
            .flat_map(|job| std::mem::take(&mut job.tokens));
        let held: Vec<AckToken> = std::mem::take(&mut self.active.tokens)
            .into_iter()
            .chain(flushing)
            .chain(self.pending.take().map(|pending| pending.token))
            .collect();
        for token in held {
            token.discard();
        }
    }
}

impl Worker {
    /// Build a worker with an empty ACTIVE block aligned to the current
    /// window.
    pub(super) fn new(
        cfg: Config,
        store: Arc<dyn object_store::ObjectStore>,
        wall: Arc<dyn WallClock>,
        effects: EffectHandler<OtapPdata>,
    ) -> Self {
        let window = Window::new(cfg.window.interval, Arc::clone(&wall));
        let lake_cfg = Arc::new(cfg.lake.clone());
        let active = OwnedBlock {
            data: Block::new(window.clock.last_boundary(), 0, Arc::clone(&lake_cfg)),
            tokens: Vec::new(),
            reemit: false,
            emitted: [0; 3],
        };
        // The sink bounds its abort on the engine clock like every other wait
        // of this node, so a simulated clock governs it too.
        let naming = lake::sink::FileNaming::new(&cfg.lake.writer_id);
        let boot_id = naming.boot_id.clone();
        let sink = Rc::new(lake::sink::Sink::new(
            Arc::clone(&store),
            cfg.lake.clone(),
            naming,
            |timeout| clock::sleep_until(clock::now() + timeout),
        ));
        // One credit per request slot of the two blocks (see
        // `Notifier::has_credit` for the slot kept for shutdown).
        let notify = Notifier::new(effects, 2 * cfg.window.max_requests_per_block);
        let cache = SeriesCache::new(cfg.cache_entries);
        Self {
            cfg,
            lake_cfg,
            active,
            flushing: None,
            cleaning: None,
            pending: None,
            cache,
            notify,
            rotation_requested: false,
            deadline: None,
            terminal: None,
            window,
            wall,
            sink,
            seq: 1,
            reason: FlushReason::Time,
            token_high_water: size_of::<AckToken>(),
            #[cfg(test)]
            samples: 0,
            refusals: RefusalGates::new(),
            admission: AdmissionGate::default(),
            boot_id,
            accepted: 0,
            abandoned: 0,
            metrics: None,
            flush_shared: Rc::new(FlushShared {
                store,
                tally: flush::FlushTally::default(),
            }),
        }
    }

    /// Completions the worker still owes, wherever they currently sit.
    ///
    /// A token moves from the ACTIVE block to the FLUSHING one and from there
    /// into the notifier, so the notifier's bound counts every place at once.
    pub(super) fn live_tokens(&self) -> usize {
        self.held_tokens() + self.notify.len()
    }

    /// Completions held by a block or the parking slot, which the notifier
    /// has yet to be handed.
    fn held_tokens(&self) -> usize {
        self.active.tokens.len()
            + self.flushing.as_ref().map_or(0, |job| job.tokens.len())
            + usize::from(self.pending.is_some())
    }

    /// Refuse one request force-drained after shutdown was latched (see
    /// [`Notifier::force_shutdown`]).
    pub(super) fn force_shutdown(&mut self, data: OtapPdata) {
        self.notify.force_shutdown(data);
    }

    /// Whether one more request may be admitted.
    ///
    /// Admission stops while a rotation is pending or a request is parked, for
    /// good once shutdown has been latched, and whenever one more request
    /// could leave the notifier without the credit to decide it.
    pub(super) fn accept(&self) -> bool {
        self.deadline.is_none()
            && !self.rotation_requested
            && self.pending.is_none()
            && self.notify.has_credit(self.live_tokens())
    }

    /// Validate one request, extract its rows and release its payload.
    ///
    /// Only the extracted rows and the completion survive the call: the
    /// transport frames and claims are dropped inside [`AckToken::split`] and
    /// the conversion records here. Nothing here touches a block.
    pub(super) fn prepare(&self, data: OtapPdata) -> Prepared {
        let (token, mut payload) = AckToken::split(data);
        // `num_bytes` is an estimate of the wire representation, so the budget
        // is also enforced on the measured extracted output inside `extract`.
        if let Err(error) =
            check_request_size(payload.num_bytes(), self.cfg.lake.ingress.max_request_bytes)
        {
            return Prepared::Failed(token, error, None);
        }
        // The token's size is fixed by the route, so an oversized one is
        // refused here, before any conversion or block reservation.
        if let Err(error) = lake::config::check_token(token.bytes()) {
            return Prepared::Failed(token, error, None);
        }
        // Traces have no lake schema, so they are refused on the signal alone,
        // before any conversion and whatever the `unsupported` policy says.
        if payload.signal_type() == otel_arrow_dfe_config::SignalType::Traces {
            return Prepared::Failed(
                token,
                lake::Error::Refused(lake::RefuseReason::Unsupported("traces".into())),
                None,
            );
        }
        let (extracted, utf8_repairs) = match self.extract(payload) {
            Ok(extracted) => extracted,
            Err((error, producer)) => return Prepared::Failed(token, error, producer),
        };
        Prepared::Ready(Pending {
            extracted,
            token,
            admission_secs: nanos_to_secs(self.wall.now_unix_nanos()),
            utf8_repairs,
        })
    }

    /// Convert one payload and extract its rows, dropping the conversion;
    /// also returns how many string values the conversion repaired. A failed
    /// extraction returns the request's producer id with its error, when the
    /// conversion got far enough to read it.
    ///
    /// Split out so the conversion's record batches are dropped with the call.
    fn extract(
        &self,
        payload: OtapPayload,
    ) -> Result<(Extracted, u64), (lake::Error, Option<String>)> {
        let (records, repaired): (Result<OtapArrowRecords, _>, u64) =
            count_utf8_repairs(|| payload.try_into_with_default());
        let mut records = records.map_err(|e| {
            (
                lake::Error::invalid(format!("undecodable pdata: {e}")),
                None,
            )
        })?;
        match lake::extract::extract(&mut records, &self.cfg.lake) {
            Ok(extracted) => Ok((extracted, repaired)),
            Err(error) => Err((
                error,
                lake::extract::producer_id_of(&records, &self.cfg.lake),
            )),
        }
    }

    /// Take ownership of one request's completion and try to admit its rows.
    ///
    /// Every path ends with the request decided, its completion held by the
    /// ACTIVE block, or the whole extraction parked; none leaves it without an
    /// owner.
    pub(super) fn admit(&mut self, data: OtapPdata) {
        self.accepted += 1;
        match self.prepare(data) {
            Prepared::Ready(pending) => {
                self.token_high_water = self.token_high_water.max(pending.token.bytes());
                // Extraction outcomes are recorded exactly once, here: a
                // request that is parked and later resumed goes through
                // `offer` again but never through `prepare`, so nothing it
                // reported can be counted twice.
                if let Some(metrics) = &mut self.metrics {
                    metrics.extracted(&pending.extracted.stats);
                    metrics.repaired(pending.token.signal(), pending.utf8_repairs);
                }
                self.offer(pending);
            }
            Prepared::Failed(token, error, producer) => {
                self.token_high_water = self.token_high_water.max(token.bytes());
                self.refuse(token, &error, |_| producer);
            }
        }
    }

    /// Offer one prepared request to the ACTIVE block.
    ///
    /// A request that carries no rows is acknowledged without touching a
    /// block. Otherwise the reservation runs first, so the block is mutated
    /// only once it has accepted the request. A block-scoped refusal parks the
    /// extraction and asks for a rotation instead of refusing the sender: the
    /// same rows will fit the next block. Only one request is ever parked,
    /// because admission closes while one is waiting.
    pub(super) fn offer(&mut self, pending: Pending) {
        if pending.extracted.stats.rows == 0 {
            self.notify.push(pending.token, Outcome::Ack);
            return;
        }
        // A request that arrived after the ACTIVE block's window ended belongs
        // to the next block, not to the one still in hand.
        if self.window.admission_boundary(pending.admission_secs)
            > self.active.data.window_start_secs
        {
            self.reason = FlushReason::Time;
            self.park(pending);
            return;
        }
        let token_bytes = pending.token.bytes();
        let reservation = match self.active.data.reserve_with_reemit(
            &pending.extracted,
            &mut self.cache,
            token_bytes,
            self.active.reemit,
        ) {
            Ok(reservation) => reservation,
            // A block-scoped refusal judges whichever block is active, so the
            // request waits for the next one. An empty block fails a request
            // it cannot fit as internal, and the guard checks it, so parking
            // can never become an endless rotation.
            Err(lake::Error::Refused(
                reason @ (lake::RefuseReason::BlockFull | lake::RefuseReason::TooManyRequests),
            )) if !self.active.data.is_empty() => {
                self.reason = match reason {
                    lake::RefuseReason::BlockFull => FlushReason::Bytes,
                    _ => FlushReason::Requests,
                };
                self.park(pending);
                return;
            }
            Err(error) => {
                // The reservation refused before the block was touched, so
                // only this request is affected. A block-scoped refusal from
                // an empty block, or a worst case that misses one, is a broken
                // invariant, which is how [`Outcome::of`] reports it.
                let extracted = &pending.extracted;
                self.refuse(pending.token, &error, |cfg| extracted.producer_id(cfg));
                return;
            }
        };
        // Classified before the descriptors are consumed by `admit`. The
        // reservation has already touched every series id, so an id absent
        // from the cache before this request now reads as present but
        // uncommitted, which is exactly `New`.
        let mut emitted = [0_u64; 3];
        for &index in &reservation.new_series {
            let id = pending.extracted.descriptors[index].series_id;
            let position = if self.active.reemit {
                EmitReason::Rotation as usize
            } else if self.cache.last_committed(&id).is_some() {
                EmitReason::Partition as usize
            } else {
                EmitReason::New as usize
            };
            emitted[position] += 1;
        }
        match self.active.data.admit(pending.extracted, reservation) {
            Ok(()) => {
                self.active.tokens.push(pending.token);
                for (total, count) in self.active.emitted.iter_mut().zip(emitted) {
                    *total += count;
                }
                // The thresholds bring the rotation forward; a rotation
                // already owed for a consumed boundary stays owed.
                if self.active.data.bytes >= self.cfg.window.max_block_bytes {
                    self.reason = FlushReason::Bytes;
                    self.rotation_requested = true;
                } else if self.active.data.request_count() >= self.cfg.window.max_requests_per_block
                {
                    self.reason = FlushReason::Requests;
                    self.rotation_requested = true;
                }
            }
            Err(error) => {
                // A failed admission leaves the block partially updated, so
                // the whole ACTIVE block is failed as internal: the failure is
                // in memory and the co-tenants are not at fault. The
                // extraction was consumed by the failed admission.
                self.refuse(pending.token, &error, |_| None);
                self.fail_active(Outcome::Internal);
            }
        }
    }

    /// Park the one request the ACTIVE block could not take.
    fn park(&mut self, pending: Pending) {
        assert!(
            self.pending.is_none(),
            "admission closes while a request is parked"
        );
        self.pending = Some(pending);
        self.rotation_requested = true;
    }

    /// Offer the parked request to the block that has just opened.
    ///
    /// A no-op while a rotation is still owed, so the request is never offered
    /// to a block that is about to be sealed, and once shutdown has been
    /// latched, because [`Worker::shutdown`] has already decided it. The
    /// reservation is recomputed here against the new block's partition and
    /// the cache as it now stands, so a descriptor the failed block carried is
    /// written again by this one.
    pub(super) fn resume_pending(&mut self) {
        if self.rotation_requested || self.deadline.is_some() {
            return;
        }
        if let Some(pending) = self.pending.take() {
            self.offer(pending);
        }
    }

    /// Report a decided failure and release its completion.
    ///
    /// The error value is dropped with the payload, so it is logged here while
    /// the detail still exists; the completion carries the outcome and the
    /// reason sentence. The line names the signal and, for a size refusal,
    /// the setting that refused it, the observed size and the limit, and the
    /// request's producer id when `producer` knows it, and the error's text
    /// with its control characters escaped; it is rate limited per outcome
    /// (see [`RefusalGates`]), and `producer` is only asked for a line that is
    /// logged.
    fn refuse(
        &mut self,
        token: AckToken,
        error: &lake::Error,
        producer: impl FnOnce(&LakeConfig) -> Option<String>,
    ) {
        let outcome = Outcome::of(error);
        let sentence = Outcome::explain(error);
        if let Some(suppressed) = self.refusals.admit(outcome, clock::now()) {
            // Sizes are recorded as numbers, and an absent one is not
            // recorded at all, so telemetry never has to parse them back out
            // of a string.
            let excess = outcome::excess(error);
            let setting = excess.map(|(setting, _, _)| setting);
            let observed = excess.map(|(_, observed, _)| observed as u64);
            let limit = excess.map(|(_, _, limit)| limit as u64);
            let producer = producer(&self.cfg.lake);
            otel_warn!(
                "series_parquet.request.failed",
                outcome = outcome.label(),
                signal = ?token.signal(),
                producer_id = producer.as_deref(),
                limit_setting = setting,
                observed_bytes = observed,
                limit_bytes = limit,
                reason = %sentence,
                error = %outcome::logged_error(error),
                suppressed = suppressed
            );
        }
        self.notify.push_with(token, outcome, Some(sentence.into()));
    }

    /// An empty block for the current window, after the one in hand.
    ///
    /// The window start never moves backwards, so a wall clock that steps back
    /// cannot make a later block claim an earlier partition. A parked request
    /// consumed its boundary when it was parked, so the block opened for it
    /// covers its window even if the wall clock stepped back since.
    fn new_active(&mut self) -> OwnedBlock {
        let secs = nanos_to_secs(self.wall.now_unix_nanos());
        let start = self
            .window
            .admission_boundary(secs)
            .max(self.active.data.window_start_secs);
        let seq = self.seq;
        // A worker would have to seal one block per nanosecond for six hundred
        // years to reach this.
        self.seq = self.seq.saturating_add(1);
        // A block opened for the window the sealed one already covered cannot
        // assume that block's descriptors are durable: it writes its own
        // copies. A byte or request threshold opens one, and so does a window
        // rotated on monotonic time while the wall clock has stepped back
        // (see `window`), which keeps the floored start.
        let reemit = start == self.active.data.window_start_secs
            && (matches!(self.reason, FlushReason::Bytes | FlushReason::Requests)
                || (self.reason == FlushReason::Time && self.window.floored));
        OwnedBlock {
            data: Block::new(start, seq, Arc::clone(&self.lake_cfg)),
            tokens: Vec::new(),
            reemit,
            emitted: [0; 3],
        }
    }

    /// Discard the ACTIVE block and decide every completion it held.
    pub(super) fn fail_active(&mut self, outcome: Outcome) {
        let next = self.new_active();
        let old = std::mem::replace(&mut self.active, next);
        for token in old.tokens {
            self.notify.push(token, outcome);
        }
    }

    /// Seal the ACTIVE block and start writing it.
    ///
    /// An empty ACTIVE block writes nothing, so it is replaced at once,
    /// whatever holds the flush slot. Otherwise this is a no-op while the slot
    /// is taken, by a write or by the cleanup of a decided one: the rotation
    /// stays requested and is served once the slot frees, so two writes never
    /// share file names.
    pub(super) fn rotate(&mut self) {
        if self.active.data.is_empty() {
            // A request without rows is acknowledged before it reaches a
            // block, so an empty block owes nothing.
            debug_assert!(
                self.active.tokens.is_empty(),
                "an empty block holds no completion"
            );
            self.rotation_requested = false;
            // Replaced, not kept: a parked request may be waiting for a later
            // window than this empty block's.
            self.active = self.new_active();
            return;
        }
        if self.flushing.is_some() || self.cleaning.is_some() {
            return;
        }
        self.rotation_requested = false;
        if let Err(error) = self
            .active
            .data
            .seal(nanos_to_micros(self.wall.now_unix_nanos()))
        {
            if let Some(metrics) = &mut self.metrics {
                metrics.flush_failed(WriteFailure::Internal);
            }
            otel_warn!("series_parquet.seal.failed", error = %error);
            // Sealing is in-memory Arrow work; storage was never touched.
            self.fail_active(Outcome::Internal);
            return;
        }
        // Counted here: an empty block is replaced without a write, which is
        // not a flush.
        if let Some(metrics) = &mut self.metrics {
            metrics
                .flush
                .with(FlushAttrs {
                    reason: self.reason,
                })
                .count
                .add(1);
        }
        let next = self.new_active();
        let old = std::mem::replace(&mut self.active, next);
        let job = FlushJob::new(
            old.data,
            old.tokens,
            self.sink.clone(),
            old.emitted,
            self.cfg.window.flush_retry_deadline,
            self.cfg.lake.upload.abort_timeout,
            Rc::clone(&self.flush_shared),
        );
        if let Some(deadline) = self.deadline {
            job.cut_to(deadline);
        }
        self.flushing = Some(job);
    }

    /// The outcome a block whose flush did not succeed must be reported as:
    /// `Shutdown` once the shutdown deadline has passed, else `cause`.
    ///
    /// Every one is retryable: the rows never reached object storage, so the
    /// sender still holds the only copy.
    fn failed_outcome(&self, cause: Outcome) -> Outcome {
        if self.deadline.is_some_and(|d| clock::now() >= d) {
            Outcome::Shutdown
        } else {
            cause
        }
    }

    /// Decide the FLUSHING block from the result of its write.
    ///
    /// The cache is marked committed only here, only on success, and only
    /// against the partition of the block that was actually written, so a
    /// descriptor is treated as written exactly when its file exists.
    pub(super) fn complete(
        &mut self,
        done: Result<FlushDone, tokio::sync::oneshot::error::RecvError>,
    ) {
        // One FLUSHING slot: a decided block still unwinding holds it, so no
        // new write can have started, and no second result can arrive.
        debug_assert!(
            self.cleaning.is_none(),
            "a flush completes only while the slot has no cleanup"
        );
        let Some(mut job) = self.flushing.take() else {
            return;
        };
        if let Some(metrics) = &mut self.metrics {
            metrics.worker.flush_duration.record(
                clock::now()
                    .saturating_duration_since(job.started)
                    .as_secs_f64(),
            );
        }
        // The sentence every request of a failed block is told, shared: the
        // block failed once, for one reason.
        let mut reason: Option<Rc<str>> = None;
        let outcome = match &done {
            Ok(finished) => {
                match &finished.result {
                    Ok(report) => {
                        for id in &finished.data.pending_series {
                            self.cache.mark_committed(*id, finished.data.partition);
                        }
                        // The only place series rows are credited: the write
                        // returned success, so the file the descriptors are in
                        // exists.
                        if let Some(metrics) = &mut self.metrics {
                            for (reason, count) in
                                [EmitReason::New, EmitReason::Partition, EmitReason::Rotation]
                                    .into_iter()
                                    .zip(job.emitted)
                            {
                                if count != 0 {
                                    metrics
                                        .emitted
                                        .with(EmitAttrs { reason })
                                        .series_emitted
                                        .add(count);
                                }
                            }
                            for (dataset, _, rows) in &report.files {
                                let bucket = metrics.written.with(DatasetAttrs::from(*dataset));
                                bucket.rows_written.add(*rows as u64);
                                bucket.files_written.add(1);
                            }
                        }
                        // Every object of a block shares one file name and
                        // differs only in its dataset directory, so the first
                        // path and the count name the whole set.
                        let path = report
                            .files
                            .first()
                            .map_or_else(String::new, |(_, path, _)| path.to_string());
                        otel_info!(
                            "series_parquet.block.committed",
                            window_start = job.window_start_secs,
                            seq = job.seq,
                            path = path,
                            files = report.files.len(),
                            requests = job.tokens.len(),
                            bytes = job.bytes,
                            attempts = finished.attempts,
                            duration = ?clock::now().saturating_duration_since(job.started)
                        );
                        Outcome::Ack
                    }
                    Err(error) => {
                        let class = WriteFailure::of(error);
                        if let Some(metrics) = &mut self.metrics {
                            metrics.flush_failed(class);
                            if error.is_cancelled() {
                                metrics.worker.flush_cancelled.add(1);
                            }
                        }
                        otel_error!(
                            "series_parquet.flush.failed",
                            window_start = job.window_start_secs,
                            seq = job.seq,
                            file = &*job.trace.file,
                            requests = job.tokens.len(),
                            bytes = job.bytes,
                            attempts = finished.attempts,
                            error_type = class.label(),
                            error = %error,
                            message = "Block failed before durable completion"
                        );
                        reason = Some(Rc::from(StorageFailed(error).to_string()));
                        self.failed_outcome(Outcome::Storage)
                    }
                }
            }
            Err(error) => {
                if let Some(metrics) = &mut self.metrics {
                    metrics.flush_failed(WriteFailure::Internal);
                }
                otel_warn!(
                    "series_parquet.flush.task_failed",
                    window_start = job.window_start_secs,
                    seq = job.seq,
                    file = &*job.trace.file,
                    requests = job.tokens.len(),
                    bytes = job.bytes,
                    error = %error
                );
                // The task panicked or was dropped: a writer fault, not the store's.
                self.failed_outcome(Outcome::Internal)
            }
        };
        // A block decided at the shutdown deadline is told so, whatever the
        // write was doing at the time.
        if outcome != Outcome::Storage {
            reason = None;
        }
        for token in std::mem::take(&mut job.tokens) {
            self.notify.push_with(token, outcome, reason.clone());
        }
        // The job keeps the FLUSHING slot, owing no completion, until its task
        // has released the block, the sink handle and the write future, so no
        // next write reuses the same file names.
        self.cleaning = Some(job);
    }

    /// Bytes this worker's configuration allows it to hold: two blocks, one
    /// parked extraction, a full descriptor cache, a token per request slot,
    /// the conversion of a request, and the seal and flush workspace of
    /// [`LakeConfig::worst_case_flush_bytes`](lake::config::LakeConfig::worst_case_flush_bytes).
    /// The workspace terms are reservations, not measurements.
    ///
    /// The arithmetic saturates, so an effectively unlimited configuration
    /// reports `u64::MAX`.
    pub(super) fn budget_bytes(&self) -> u64 {
        let token = self.token_high_water.max(self.notify.token_high_water()) as u64;
        let cfg = &self.cfg.lake;
        let bytes = |n: usize| n as u64;
        let conversion = bytes(cfg.ingress.max_request_bytes).saturating_mul(4);
        [
            bytes(cfg.ingress.max_block_bytes).saturating_mul(2),
            bytes(cfg.ingress.max_extracted_bytes),
            bytes(self.cfg.cache_entries).saturating_mul(CACHE_ENTRY_BYTES),
            bytes(cfg.ingress.max_requests_per_block)
                .saturating_mul(2)
                .saturating_mul(token),
            cfg.worst_case_flush_bytes(),
            conversion,
            FIXED_WORKSPACE_BYTES,
        ]
        .into_iter()
        .fold(0_u64, u64::saturating_add)
    }

    /// Install the node's registered instruments.
    ///
    /// The shared `exporter.exports` set moves into the notifier, which is
    /// where every request is decided.
    pub(super) fn set_metrics(&mut self, mut metrics: Option<Metrics>) {
        self.notify.exports = metrics.as_mut().and_then(Metrics::take_exports);
        self.metrics = metrics;
    }

    /// Hand every instrument to the collector on a `CollectTelemetry`.
    pub(super) fn report_metrics(
        &mut self,
        reporter: &mut otel_arrow_dfe_telemetry::reporter::MetricsReporter,
    ) {
        if let Some(exports) = &mut self.notify.exports {
            let _ = reporter.report_measurement(exports);
        }
        if let Some(metrics) = &mut self.metrics {
            metrics.report(reporter);
        }
    }

    /// Take every instrument for terminal handoff.
    pub(super) fn metric_snapshots(
        &mut self,
    ) -> Vec<otel_arrow_dfe_telemetry::metrics::MetricSetSnapshot> {
        let mut out = self
            .notify
            .exports
            .as_mut()
            .map_or_else(Vec::new, |exports| exports.terminal_snapshots());
        if let Some(metrics) = &mut self.metrics {
            out.extend(metrics.snapshots());
        }
        out
    }

    /// Record whether pdata admission is open on this loop turn.
    ///
    /// A gate closed by the latched shutdown is not backpressure, so it ends
    /// any closure in progress.
    pub(super) fn observe_admission(&mut self, accept: bool) {
        self.admission.observe(accept || self.deadline.is_some());
    }

    /// Publish the gauges and the accounted total, on `CollectTelemetry` and
    /// before every terminal snapshot.
    ///
    /// The accounted total is what the worker retains: the ACTIVE block, the
    /// FLUSHING block with its merge keys and live flush workspace, the parked
    /// request, the notifier's completions, the descriptor cache and the spare
    /// capacity of the two token vectors. The budget terms are documented on
    /// [`Worker::budget_bytes`].
    pub(super) fn sample_metrics(&mut self) {
        // A worker without instruments, one a test drives directly, skips the
        // scan over the live tokens.
        if self.metrics.is_none() {
            return;
        }
        #[cfg(test)]
        {
            self.samples += 1;
        }
        // Both slot holders are the one FLUSHING block: a decided block is
        // still accounted for until its supervising task has released it.
        let flushing = self
            .flushing
            .iter()
            .chain(self.cleaning.iter())
            .map(|job| job.bytes)
            .sum::<usize>();
        let pending = self.pending.as_ref().map_or(0, |parked| {
            parked.extracted.pinned_bytes
                + parked.extracted.shared_bytes
                + parked
                    .extracted
                    .descriptors
                    .iter()
                    .map(|descriptor| descriptor.approx_bytes)
                    .sum::<usize>()
                + parked.token.bytes()
        });
        let cache = (self.cache.len() as u64).saturating_mul(CACHE_ENTRY_BYTES);
        let spare_tokens = self
            .active
            .tokens
            .capacity()
            .saturating_sub(self.active.tokens.len())
            + self
                .flushing
                .iter()
                .chain(self.cleaning.iter())
                .map(|job| job.tokens.capacity().saturating_sub(job.tokens.len()))
                .sum::<usize>();
        // The merge keys of the table being written, as the sink reports them.
        let merge_keys = self.sink.merge_key_bytes();
        // The write's merge chunk, encoder buffers and unacknowledged upload
        // bytes, read live between its bounded steps.
        let workspace = self.sink.flush_workspace_bytes();
        let accounted = self.active.data.bytes as u64
            + flushing as u64
            + merge_keys as u64
            + workspace as u64
            + pending as u64
            + cache
            + self.notify.bytes() as u64
            + (spare_tokens * size_of::<AckToken>()) as u64;
        let budget = self.budget_bytes();
        let oldest = self
            .active
            .tokens
            .iter()
            .map(AckToken::received)
            .chain(
                self.flushing
                    .iter()
                    .flat_map(|job| job.tokens.iter().map(AckToken::received)),
            )
            .chain(self.pending.iter().map(|parked| parked.token.received()))
            .chain(self.notify.oldest())
            .min();
        let requests = self.live_tokens() as u64;
        let entries = self.cache.len() as u64;
        let stats = self.cache.stats();
        let active_bytes = self.active.data.bytes as u64;
        let queued = self.notify.len() as u64;
        let notify_bytes = self.notify.bytes() as u64;
        let failures = self.notify.failures();
        let lost_acks = self.notify.lost_acks();
        let outcomes = *self.notify.outcomes();
        let parked = self.pending.is_some();
        let now = clock::now();
        if let Some(metrics) = &mut self.metrics {
            // Credited by the flush tasks as they happen, so an outage shows
            // its retries while it lasts.
            let tally = &self.flush_shared.tally;
            metrics.worker.flush_retries.add(tally.retries.take());
            metrics
                .worker
                .flush_abort_failures
                .add(tally.abort_failures.take());
            for outcome in LateCommit::ALL {
                let found = tally.late_commits[outcome as usize].take();
                if found != 0 {
                    metrics
                        .late_commits
                        .with(LateCommitAttrs { outcome })
                        .late_commits
                        .add(found);
                }
            }
            metrics.worker.cache_entries.set(entries);
            metrics.worker.cache_hits.observe(stats.hits);
            metrics.worker.cache_misses.observe(stats.misses);
            metrics.worker.cache_evictions.observe(stats.evictions);
            metrics.worker.active_bytes.set(active_bytes);
            metrics.worker.flushing_bytes.set(flushing as u64);
            metrics.worker.pending_bytes.set(pending as u64);
            metrics.worker.requests_pending.set(requests);
            metrics.worker.pending_slot.set(u64::from(parked));
            metrics.worker.notify_queued.set(queued);
            metrics.worker.notify_token_bytes.set(notify_bytes);
            metrics.worker.notify_failures.observe(failures);
            metrics.worker.notify_lost_acks.observe(lost_acks);
            metrics.worker.acks.observe(outcomes[Outcome::Ack as usize]);
            for error_type in NackAttrs::error_types() {
                metrics
                    .nacks
                    .with(NackAttrs { error_type })
                    .nacks
                    .observe(outcomes[error_type as usize]);
            }
            let (closed, closures, closed_secs) = self.admission.sample(now);
            metrics.worker.admission_closed.set(u64::from(closed));
            metrics.worker.admission_closures.observe(closures);
            metrics
                .worker
                .admission_closed_duration
                .observe(closed_secs);
            metrics.worker.oldest.set(oldest.map_or(0.0, |received| {
                now.saturating_duration_since(received).as_secs_f64()
            }));
            metrics.worker.memory_accounted_bytes.set(accounted);
            metrics.worker.flush_workspace_bytes.set(workspace as u64);
            metrics.worker.memory_budget_bytes.set(budget);
        }
    }

    /// Serve a window boundary that has fired.
    ///
    /// The boundary becomes the reason of the rotation that follows, replacing
    /// a threshold reason that never got to rotate.
    pub(super) fn wake_window(&mut self) {
        if self.window.wake() {
            self.reason = FlushReason::Time;
            self.rotation_requested = true;
        }
    }

    /// Begin the shutdown for `deadline`: the FLUSHING block's retries are
    /// cut [`SHUTDOWN_COMPLETION_MARGIN`] before it, and the ACTIVE block is
    /// sealed as soon as the flush slot frees, without waiting for its window,
    /// and flushed under the same cut (see `FlushJob::cut_to`). The margin
    /// itself is left for delivering the completions (see
    /// [`Worker::abandon`]).
    ///
    /// The earliest deadline wins, so a second, tighter shutdown cannot extend
    /// the first.
    pub(super) fn shutdown(&mut self, deadline: Instant) {
        // No block will open for the parked request, so it is decided now.
        if let Some(pending) = self.pending.take() {
            self.notify.push(pending.token, Outcome::Shutdown);
        }
        let cut = deadline
            .checked_sub(SHUTDOWN_COMPLETION_MARGIN)
            .unwrap_or(deadline);
        let latched = self.deadline.map_or(cut, |old| old.min(cut));
        self.deadline = Some(latched);
        self.terminal = Some(self.terminal.map_or(deadline, |old| old.min(deadline)));
        if let Some(job) = &self.flushing {
            job.cut_to(latched);
        }
        self.reason = FlushReason::Shutdown;
        self.rotation_requested = true;
    }

    /// Whether the worker owes nothing further.
    pub(super) fn is_idle(&self) -> bool {
        self.pending.is_none()
            && self.active.tokens.is_empty()
            && self.active.data.is_empty()
            && self.flushing.is_none()
            && self.cleaning.is_none()
            && self.notify.is_empty()
    }

    /// Decide everything still owned, once the shutdown deadline has elapsed.
    ///
    /// A flush that has already published its decision is decided by it (see
    /// [`FlushJob::try_finish`]). Every other completion is then decided, and
    /// all of them are delivered, Acks first, until [`Worker::terminal`]; a
    /// completion the engine has not taken by then is counted as a delivery
    /// failure. Only then are the two slot holders
    /// cancelled and awaited until one shared cutoff (see
    /// [`flush::cleanup_cutoff`]); past it each task is aborted and the abort
    /// awaited, so an unresponsive destination cannot hold the node open.
    ///
    /// Returns whether [`Worker::terminal`] elapsed with completions still
    /// undelivered.
    pub(super) async fn abandon(&mut self) -> bool {
        // Phase zero: a published decision wins over the deadline; taking it
        // also commits its descriptors and moves the job into the cleaning
        // slot that phase two releases.
        if let Some(job) = &mut self.flushing
            && let Some(done) = job.try_finish()
        {
            self.complete(Ok(done));
        }
        // Phase one: decide and deliver. Nothing here cancels, and only the
        // delivery awaits, bounded by the shutdown deadline itself.
        // Every completion still held by a block or the parking slot is
        // decided here by the deadline; the notifier's own queue was already
        // decided before.
        let mut abandoned = 0_u64;
        if let Some(pending) = self.pending.take() {
            abandoned += 1;
            self.notify.push(pending.token, Outcome::Shutdown);
        }
        let mut flushing = self.flushing.take();
        if let Some(job) = &mut flushing {
            abandoned += job.tokens.len() as u64;
            // Counted as the cancelled branch of `complete` counts it, so the
            // flush does not vanish from the counters.
            if let Some(metrics) = &mut self.metrics {
                metrics.worker.flush_duration.record(
                    clock::now()
                        .saturating_duration_since(job.started)
                        .as_secs_f64(),
                );
                metrics.flush_failed(WriteFailure::Cancelled);
                metrics.worker.flush_cancelled.add(1);
            }
            for token in std::mem::take(&mut job.tokens) {
                self.notify.push(token, Outcome::Shutdown);
            }
        }
        // A block whose decision has already been published owes no completion;
        // what it still owns is the write its supervisor is unwinding.
        let mut cleaning = self.cleaning.take();
        abandoned += self.active.tokens.len() as u64;
        self.fail_active(Outcome::Shutdown);
        self.abandoned += abandoned;
        let deadline_exceeded = match self.terminal {
            Some(until) => !self.notify.deliver_until(until).await,
            None => false,
        };
        self.notify.drain_now();
        // Phase two: cancel and release, with one shared bound so a node
        // holding a write and a cleanup does not wait twice.
        let deadline = flush::cleanup_cutoff(
            clock::now(),
            self.deadline,
            self.cfg.lake.upload.abort_timeout,
        );
        if let Some(job) = &mut flushing {
            job.shutdown(deadline).await;
        }
        if let Some(job) = &mut cleaning {
            job.shutdown(deadline).await;
        }
        deadline_exceeded
    }
}
