// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The one outstanding flush, split between a write task and its owner.
//!
//! A rotated block's rows move into a local task that retries storage errors
//! until an absolute deadline, while its completions stay in the [`FlushJob`],
//! so the owner can decide them at a deadline without waiting for the task to
//! unwind.

use super::metrics::LateCommit;
use super::token::AckToken;
use object_store::ObjectStore;
use object_store::path::Path;
use otel_arrow_dfe_engine::clock;
use otel_arrow_dfe_series_lake as lake;
use otel_arrow_dfe_series_lake::sink::{Presence, probe_block};
use otel_arrow_dfe_telemetry::attributes::AttributeEnum;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

/// Delay before the first retry of a failed write.
const FIRST_BACKOFF: Duration = Duration::from_millis(200);

/// Upper bound the doubling backoff is clamped to.
const MAX_BACKOFF: Duration = Duration::from_secs(10);

/// The instant the cleanup of a write decided by `deadline` must end by.
///
/// It is `abort_timeout` after the deadline itself, not after the moment the
/// expiry is observed, so a late wake or a long synchronous drain cannot
/// extend it; and it is never more than `abort_timeout` from `now`, which is
/// the bound when the write is cancelled before any deadline.
pub(super) fn cleanup_cutoff(
    now: Instant,
    deadline: Option<Instant>,
    abort_timeout: Duration,
) -> Instant {
    let fresh = now + abort_timeout;
    deadline.map_or(fresh, |deadline| (deadline + abort_timeout).min(fresh))
}

/// The instant one flush must be decided by, shared by the job and its task.
///
/// It is the block's own retry deadline until shutdown latches; from then on
/// it is the earlier of that and the latched deadline, and the backoff between
/// attempts drops to its minimum. An attempt starts only before the deadline
/// and is cancelled at it. Moving it wakes the task, which re-reads it before
/// every attempt and every wait.
struct Limit {
    /// The block's own retry deadline, taken when it was sealed.
    own: Instant,
    /// The latched shutdown deadline, once [`FlushJob::cut_to`] has set it.
    shutdown: Cell<Option<Instant>>,
    /// Wakes the task when the shutdown deadline is set or moved.
    moved: tokio::sync::Notify,
}

impl Limit {
    /// The instant the flush must be decided by.
    fn at(&self) -> Instant {
        self.shutdown
            .get()
            .map_or(self.own, |cut| cut.min(self.own))
    }

    /// The wait before the next retry, which shutdown drops to its minimum.
    fn backoff(&self, delay: Duration) -> Duration {
        if self.shutdown.get().is_some() {
            FIRST_BACKOFF
        } else {
            delay
        }
    }
}

/// A flush that has resolved, with the block it was writing.
pub(super) struct FlushDone {
    /// The sealed block, handed back so its descriptors and partition are
    /// available to the commit that only a successful write may perform.
    ///
    /// Shared: the supervising task may still be unwinding a cancelled attempt
    /// over the same block when the decision is published.
    pub(super) data: Rc<lake::buffer::Block>,
    /// What the sink returned.
    pub(super) result: lake::Result<lake::sink::FlushReport>,
    /// Write attempts this flush made, including the one that resolved it.
    pub(super) attempts: u64,
}

/// Write one sealed block, retrying storage failures until the deadline
/// `limit` holds.
///
/// The write is polled before the cancellation and the deadline, so a write
/// that finished in the poll the deadline expires in is reported as a
/// success. No attempt starts at or after the deadline.
///
/// The result is published over `result_tx` the moment it is known. The task
/// returns only once it has released everything it owns: on the deadline path
/// that means cancelling the attempt in flight and then waiting for the sink
/// to unwind it until [`cleanup_cutoff`], after which the write future is
/// dropped whether or not it cooperated.
async fn write_until(
    sink: Rc<lake::sink::Sink>,
    data: Rc<lake::buffer::Block>,
    cancel: CancellationToken,
    limit: Rc<Limit>,
    abort_timeout: Duration,
    result_tx: tokio::sync::oneshot::Sender<FlushDone>,
    trace: Rc<Trace>,
) {
    let mut attempts = 0_u64;
    let mut delay = FIRST_BACKOFF;
    // The last attempt's failure, which the deadline error carries.
    let mut last: Option<lake::Error> = None;
    let done = |attempts, result| FlushDone {
        data: Rc::clone(&data),
        attempts,
        result,
    };
    loop {
        let cancelled = cancel.is_cancelled();
        if cancelled || clock::now() >= limit.at() {
            // Ended during the backoff after a retryable failure, which may
            // be a response the store lost after committing.
            let settle = last.as_ref().map_or(Settle::Nothing, Settle::after);
            let error = if cancelled {
                last.take().unwrap_or(lake::Error::cancelled(None))
            } else {
                expired(attempts, last.take())
            };
            let _ = result_tx.send(done(attempts, Err(error)));
            let cutoff = cleanup_cutoff(clock::now(), Some(limit.at()), abort_timeout);
            trace.settle(attempts, settle, cutoff).await;
            return;
        }
        attempts += 1;
        trace.attempts.set(attempts);
        // DEBUG on the first attempt, INFO on a retry; the file name is a log
        // field, never a metric label.
        let remaining = limit.at().saturating_duration_since(clock::now());
        if attempts > 1 {
            let retries = &trace.shared.tally.retries;
            retries.set(retries.get() + 1);
            otel_info!(
                "series_parquet.flush.attempt",
                seq = trace.seq,
                attempt = attempts,
                file = &*trace.file,
                objects = trace.paths.len(),
                deadline_remaining = ?remaining
            );
        } else {
            otel_debug!(
                "series_parquet.flush.attempt",
                seq = trace.seq,
                attempt = attempts,
                file = &*trace.file,
                objects = trace.paths.len(),
                deadline_remaining = ?remaining
            );
        }
        // A child token so the attempt can be cancelled on the deadline
        // without cancelling the job itself, whose token also serves the
        // owner's drop.
        let attempt_cancel = cancel.child_token();
        let write = sink.write_block(&data, &attempt_cancel);
        tokio::pin!(write);
        // Re-armed whenever shutdown moves the deadline, without dropping the
        // write in flight.
        let result = loop {
            tokio::select! {
                biased;
                result = &mut write => break Ok(result),
                () = cancel.cancelled() => break Err(last
                    .take()
                    .unwrap_or(lake::Error::cancelled(None))),
                () = clock::sleep_until(limit.at()) => break Err(expired(attempts, last.take())),
                () = limit.moved.notified() => {}
            }
        };
        let result = match result {
            Ok(result) => result,
            Err(decided) => {
                // Publish the producer decision before awaiting any cleanup. This
                // task independently owns the block, the sink handle and the
                // pinned write future, so the owner is free to release the block's
                // completions while the attempt is still unwinding.
                let _ = result_tx.send(done(attempts, Err(decided)));
                attempt_cancel.cancel();
                let cutoff = cleanup_cutoff(clock::now(), Some(limit.at()), abort_timeout);
                let unwound = tokio::select! {
                    biased;
                    result = &mut write => Some(result),
                    () = clock::sleep_until(cutoff) => None,
                };
                trace.cleaned_up(attempts, unwound, cutoff).await;
                // Dropping the write after the bound releases the last task-owned
                // resources even if the object store future never cooperates.
                return;
            }
        };
        // Every failed attempt is logged with what the destination said,
        // retried or not.
        if let Err(error) = &result {
            trace.attempt_failed(attempts, limit.at(), error);
        }
        match result {
            Ok(report) => {
                for orphan in &report.possible_orphans {
                    trace.abort_failed(attempts, orphan);
                }
                if report.probed_commits > 0 {
                    trace.probed_commit(attempts);
                }
                let _ = result_tx.send(done(attempts, Ok(report)));
                return;
            }
            Err(error)
                if error.is_retryable() && clock::now() < limit.at() && !cancel.is_cancelled() =>
            {
                last = Some(error);
                // Never past the deadline: the wait itself must not outlive
                // the bound the block was given. Re-armed when shutdown moves
                // the deadline or drops the backoff.
                let waiting_since = clock::now();
                loop {
                    let wake = (waiting_since + limit.backoff(delay)).min(limit.at());
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => break,
                        () = clock::sleep_until(wake) => break,
                        () = limit.moved.notified() => {}
                    }
                }
                delay = delay.saturating_mul(2).min(MAX_BACKOFF);
            }
            Err(error) => {
                let settle = Settle::after(&error);
                let _ = result_tx.send(done(attempts, Err(error)));
                let cutoff = cleanup_cutoff(clock::now(), Some(limit.at()), abort_timeout);
                trace.settle(attempts, settle, cutoff).await;
                return;
            }
        }
    }
}

/// Counts a flush task records as they happen, which the worker moves into
/// its metrics whenever it samples them.
#[derive(Debug, Default)]
pub(super) struct FlushTally {
    /// Write attempts started beyond the first of their flush.
    pub(super) retries: Cell<u64>,
    /// Multipart uploads a failed write attempt may have left behind (see
    /// [`Trace::abort_failed`]).
    pub(super) abort_failures: Cell<u64>,
    /// Flush cleanups by what they found that the write had not confirmed,
    /// indexed by [`LateCommit`].
    pub(super) late_commits: [Cell<u64>; LateCommit::ALL.len()],
}

impl FlushTally {
    /// Count one cleanup that found `outcome`.
    fn found(&self, outcome: LateCommit) {
        let count = &self.late_commits[outcome as usize];
        count.set(count.get() + 1);
    }
}

/// What every flush task of one worker shares with it.
pub(super) struct FlushShared {
    /// The store the sink writes to, which a cleanup probes for a late commit.
    pub(super) store: Arc<dyn ObjectStore>,
    /// Counts the tasks record as they happen.
    pub(super) tally: FlushTally,
}

/// What one flush task reports its attempts and its cleanup under.
pub(super) struct Trace {
    /// The file name every object of the block shares; each object differs
    /// only in its dataset directory.
    pub(super) file: Rc<str>,
    /// Every object one attempt writes.
    paths: Vec<Path>,
    /// The block's per-worker sequence.
    seq: u64,
    /// Attempts the task has started so far, the one in flight included.
    attempts: Cell<u64>,
    /// The store to probe and where the counts go.
    shared: Rc<FlushShared>,
}

#[cfg(test)]
impl Trace {
    /// A trace for block `seq` writing `paths`, for a test that reports a
    /// cleanup without running a flush.
    pub(super) fn for_test(paths: Vec<Path>, seq: u64, shared: Rc<FlushShared>) -> Self {
        Self {
            file: paths.first().and_then(Path::filename).unwrap_or("").into(),
            paths,
            seq,
            attempts: Cell::new(0),
            shared,
        }
    }
}

impl Trace {
    /// Log one failed write attempt at WARN, retried or not, and count the
    /// upload it may have left behind.
    ///
    /// The only place the per-attempt WARN is emitted. The upload is counted
    /// here rather than when the flush ends, so a retry that succeeds does not
    /// hide it.
    fn attempt_failed(&self, attempt: u64, deadline: Instant, error: &lake::Error) {
        otel_warn!(
            "series_parquet.flush.attempt_failed",
            seq = self.seq,
            attempt = attempt,
            file = &*self.file,
            retryable = error.is_retryable(),
            deadline_remaining = ?deadline.saturating_duration_since(clock::now()),
            error = %error
        );
        if let Some(abort_error) = abort_failure(error) {
            self.abort_failed(attempt, abort_error);
        }
    }

    /// Report how the cancelled write of an already decided flush unwound:
    /// `None` when it had not by `cutoff`.
    ///
    /// A write that completed anyway is a late commit. One whose abort
    /// failed, or that never unwound, may have left a multipart upload to the
    /// bucket's lifecycle rule. Any other cancellation is ambiguous, since the
    /// store may have finished the upload and lost its response, so the
    /// objects are probed until `cutoff`.
    pub(super) async fn cleaned_up(
        &self,
        attempt: u64,
        unwound: Option<lake::Result<lake::sink::FlushReport>>,
        cutoff: Instant,
    ) {
        match &unwound {
            Some(Ok(_)) => self.late_commit(attempt),
            Some(Err(error)) => match abort_failure(error) {
                Some(abort_error) => self.abort_failed(attempt, abort_error),
                None => self.probe(attempt, cutoff).await,
            },
            None => self.unwound_late(attempt, "the write did not unwind by the cleanup cutoff"),
        }
    }

    /// Probe the block's objects until `cutoff` and report what was found.
    ///
    /// Every object present is a late commit; none is a clean abort; some,
    /// or a probe that failed, is reported without counting a late commit.
    async fn probe(&self, attempt: u64, cutoff: Instant) {
        match probe_block(&*self.shared.store, &self.paths, clock::sleep_until(cutoff)).await {
            Presence::All => self.late_commit(attempt),
            Presence::None => otel_debug!(
                "series_parquet.flush.cleanup",
                outcome = "aborted",
                seq = self.seq,
                attempt = attempt,
                file = &*self.file
            ),
            Presence::Some(present) => {
                self.shared.tally.found(LateCommit::Partial);
                otel_info!(
                    "series_parquet.flush.cleanup",
                    outcome = LateCommit::Partial.as_str(),
                    seq = self.seq,
                    attempt = attempt,
                    file = &*self.file,
                    present = present,
                    objects = self.paths.len(),
                    message = "some objects of a failed block exist; their rows may be stored \
                               twice once the producer retries"
                );
            }
            Presence::Unknown(probe_error) => {
                self.shared.tally.found(LateCommit::Unknown);
                otel_warn!(
                    "series_parquet.flush.cleanup",
                    outcome = LateCommit::Unknown.as_str(),
                    seq = self.seq,
                    attempt = attempt,
                    file = &*self.file,
                    probe_error = probe_error.as_str(),
                    message = "could not tell whether a failed block's objects exist"
                );
            }
        }
    }

    /// Count and log a failed block whose every object exists.
    fn late_commit(&self, attempt: u64) {
        self.shared.tally.found(LateCommit::Stored);
        otel_info!(
            "series_parquet.flush.cleanup",
            outcome = LateCommit::Stored.as_str(),
            seq = self.seq,
            attempt = attempt,
            file = &*self.file,
            message = "a failed block's objects exist although its requests were nacked; its \
                       rows may be stored twice once the producer retries"
        );
    }

    /// Count and log a block acknowledged although a multipart completion lost
    /// its response: the sink found the object, and the abort that followed
    /// showed this completion committed it (see `FlushReport::probed_commits`).
    fn probed_commit(&self, attempt: u64) {
        self.shared.tally.found(LateCommit::Acknowledged);
        otel_info!(
            "series_parquet.flush.cleanup",
            outcome = LateCommit::Acknowledged.as_str(),
            seq = self.seq,
            attempt = attempt,
            file = &*self.file,
            message = "a multipart completion lost its response and a probe found the object; \
                       the block is acknowledged"
        );
    }

    /// [`Trace::abort_failed`] for a write that did not unwind, `why`: any
    /// object of the block may have an upload left, so every key is named.
    fn unwound_late(&self, attempt: u64, why: &str) {
        let keys: Vec<&str> = self.paths.iter().map(Path::as_ref).collect();
        self.abort_failed(
            attempt,
            &format!("multipart uploads of {}: {why}", keys.join(", ")),
        );
    }

    /// Count and log a multipart upload a write attempt may have left behind:
    /// its abort failed or timed out, its creation got no definite answer, or
    /// the write did not unwind by the cleanup cutoff.
    pub(super) fn abort_failed(&self, attempt: u64, abort_error: &str) {
        let failures = &self.shared.tally.abort_failures;
        failures.set(failures.get() + 1);
        otel_warn!(
            "series_parquet.flush.cleanup",
            outcome = "abort_failed",
            seq = self.seq,
            attempt = attempt,
            file = &*self.file,
            abort_error = abort_error,
            message = "a multipart upload of a failed block may be left to the bucket \
                       lifecycle rule"
        );
    }
}

/// What a flush that ended on an attempt's own error still owes its cleanup
/// report, decided before the error is handed to the owner.
enum Settle {
    /// A retryable storage failure, which may be a response the store lost
    /// after committing: the objects are probed.
    Probe,
    /// Nothing the store could have kept, or an abort failure already
    /// counted with its attempt.
    Nothing,
}

impl Settle {
    /// What the cleanup of a flush ended by `error` must do.
    fn after(error: &lake::Error) -> Self {
        match abort_failure(error) {
            None if error.is_retryable() => Self::Probe,
            _ => Self::Nothing,
        }
    }
}

impl Trace {
    /// Report the cleanup of a flush that ended on an attempt's own error,
    /// probing until `cutoff` when that error is ambiguous.
    async fn settle(&self, attempt: u64, settle: Settle, cutoff: Instant) {
        match settle {
            Settle::Probe => self.probe(attempt, cutoff).await,
            Settle::Nothing => {}
        }
    }
}

/// Why the cleanup abort of a failed or cancelled write did not succeed, if
/// the error says it did not.
fn abort_failure(error: &lake::Error) -> Option<&str> {
    match error {
        lake::Error::Transient(
            lake::TransientError::Cancelled {
                abort_error: Some(abort_error),
            }
            | lake::TransientError::AbortFailed { abort_error, .. },
        ) => Some(abort_error),
        _ => None,
    }
}

/// The outcome of a flush whose retry deadline expired.
///
/// Carries the last attempt's failure, and is distinct from a cancellation so
/// a storage hang is never counted as a shutdown.
fn expired(attempts: u64, last: Option<lake::Error>) -> lake::Error {
    lake::Error::Transient(lake::TransientError::DeadlineExceeded {
        attempts,
        last: last.map(Box::new),
    })
}

/// One local flush task and the completions its block still owes.
pub(super) struct FlushJob {
    /// The supervising task; joined exactly once through [`FlushJob::cleanup`].
    handle: JoinHandle<()>,
    /// The producer-visible decision, published as soon as it is known.
    result_rx: tokio::sync::oneshot::Receiver<FlushDone>,
    /// Cancels the write, on an explicit request or when the job is dropped.
    pub(super) cancel: CancellationToken,
    /// Completions of every request admitted to the flushing block.
    pub(super) tokens: Vec<AckToken>,
    /// Bytes the block charged when it was sealed, snapshotted before the data
    /// moved into the task.
    pub(super) bytes: usize,
    /// When the write started, for the duration reported on completion.
    pub(super) started: Instant,
    /// Descriptor rows this block carries, per re-emission cause.
    ///
    /// Counted at admission but reported only once the write has returned
    /// success, so a series row is credited exactly when its file exists.
    pub(super) emitted: [u64; 3],
    /// The sealed block's window start, in Unix seconds, for the commit log.
    pub(super) window_start_secs: i64,
    /// The sealed block's per-worker sequence, for the commit log.
    pub(super) seq: u64,
    /// What the task reports its attempts and its cleanup under, shared with
    /// it: the file name, and the attempts started so far.
    pub(super) trace: Rc<Trace>,
    /// The deadline the task retries until, which shutdown can bring forward.
    limit: Rc<Limit>,
}

impl FlushJob {
    /// Seal off a rotated block: move its data into a write task and keep its
    /// completions.
    ///
    /// The block must already be sealed; the sink refuses an unsealed one and
    /// that refusal is reported as a retryable failure like any other. The
    /// retry deadline is absolute from this point, so a destination that fails
    /// slowly cannot extend it.
    pub(super) fn new(
        data: lake::buffer::Block,
        tokens: Vec<AckToken>,
        sink: Rc<lake::sink::Sink>,
        emitted: [u64; 3],
        retry_deadline: Duration,
        abort_timeout: Duration,
        shared: Rc<FlushShared>,
    ) -> Self {
        let cancel = CancellationToken::new();
        let bytes = data.bytes;
        let window_start_secs = data.window_start_secs;
        let seq = data.seq;
        // Every object of a block shares one file name and differs only in
        // its dataset directory, so the name and the paths are the whole set.
        let paths = sink.planned_paths(&data);
        let trace = Rc::new(Trace {
            file: paths.first().and_then(Path::filename).unwrap_or("").into(),
            paths,
            seq,
            attempts: Cell::new(0),
            shared,
        });
        let started = clock::now();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let limit = Rc::new(Limit {
            own: started + retry_deadline,
            shutdown: Cell::new(None),
            moved: tokio::sync::Notify::new(),
        });
        let handle = tokio::task::spawn_local(write_until(
            sink,
            Rc::new(data),
            cancel.clone(),
            Rc::clone(&limit),
            abort_timeout,
            result_tx,
            Rc::clone(&trace),
        ));
        Self {
            handle,
            result_rx,
            cancel,
            tokens,
            bytes,
            started,
            emitted,
            window_start_secs,
            seq,
            trace,
            limit,
        }
    }

    /// Bring the flush's deadline forward to the latched shutdown deadline
    /// (see [`Limit`]). The earliest deadline wins, so a later one never
    /// extends it.
    pub(super) fn cut_to(&self, deadline: Instant) {
        let cut = self
            .limit
            .shutdown
            .get()
            .map_or(deadline, |old| old.min(deadline));
        self.limit.shutdown.set(Some(cut));
        self.limit.moved.notify_one();
    }

    /// Wait for the flush to be decided.
    ///
    /// Cancellation safe: the decision stays in the channel, so a dropped poll
    /// neither cancels the write nor loses what it returned. This resolves at
    /// the retry deadline even when the attempt in flight has not unwound yet;
    /// [`FlushJob::cleanup`] waits for that.
    pub(super) async fn finish(
        &mut self,
    ) -> Result<FlushDone, tokio::sync::oneshot::error::RecvError> {
        (&mut self.result_rx).await
    }

    /// Take the flush's decision if it has already been published.
    ///
    /// Non-blocking. The node's shutdown-deadline branch is biased above the
    /// branch that awaits this result, so the deadline can win the poll in
    /// which a successful write published its report; taking it here keeps a
    /// block whose files exist acknowledged.
    ///
    /// `None` also covers a supervising task that died without sending, which
    /// the caller then decides exactly as it decides an unresolved flush.
    pub(super) fn try_finish(&mut self) -> Option<FlushDone> {
        self.result_rx.try_recv().ok()
    }

    /// Whether the supervising task has returned, for a test to wait on.
    #[cfg(test)]
    pub(super) fn task_finished(&self) -> bool {
        self.handle.is_finished()
    }

    /// Wait for the supervising task to release everything it owns.
    ///
    /// Cancellation safe, and bounded by the sink's `upload.abort_timeout`
    /// once the decision has been published. The FLUSHING slot stays occupied
    /// until this returns, so no next block writes the same file names while
    /// an abandoned attempt may still be in flight.
    pub(super) async fn cleanup(&mut self) -> Result<(), JoinError> {
        (&mut self.handle).await
    }

    /// Cancel the write and release the task, within `deadline`.
    ///
    /// The terminal counterpart of [`FlushJob::cleanup`]: nothing joins the
    /// task later, so it is awaited until `deadline`, then aborted and the
    /// abort awaited. An aborted task that had started a write is reported as
    /// a cleanup whose abort failed.
    pub(super) async fn shutdown(&mut self, deadline: Instant) {
        self.cancel.cancel();
        let joined = tokio::select! {
            biased;
            joined = &mut self.handle => Some(joined),
            () = clock::sleep_until(deadline) => None,
        };
        if joined.is_none() {
            self.handle.abort();
            let _ = (&mut self.handle).await;
            let attempt = self.trace.attempts.get();
            if attempt != 0 {
                self.trace.unwound_late(
                    attempt,
                    "the write task did not unwind by the cleanup cutoff and was aborted",
                );
            }
        }
    }
}

impl Drop for FlushJob {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
