// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The write loop of one table: bounded steps, cancellation and the
//! multipart abort, with the accounting of what the write holds.

use super::properties::{
    compression, native_sorting_columns, row_group_full, time_range, writer_properties,
};
use super::{AbortTimer, FlushReport, Presence, Sink, probe_block};

use crate::buffer::{Block, SortedTableBuffer};
use crate::error::{Error, Result, TransientError};
use crate::schema::dataset_schema;
use crate::sort::{MergeBuild, MergeIter, MergeStep};
use arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use otel_arrow_dfe_pdata::otap::memory::{CountedAllocations, record_batch_pinned_bytes};
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::{AsyncFileWriter, ParquetObjectWriter};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use crate::hook_store::{HookGuard, HookStore, StoreHooks};
use tokio_util::sync::CancellationToken;

/// The encoded sort keys the table being written keeps resident.
///
/// A sorted table's merge encodes the key of every row of every run up
/// front and holds them until the merge iterator is dropped, beside the
/// block the rows come from. That is heap the block's own accounting does
/// not include, so the sink publishes it for its owner to charge.
#[derive(Debug, Default)]
pub(super) struct MergeKeys {
    pub(super) current: AtomicUsize,
    pub(super) high_water: AtomicUsize,
}

impl MergeKeys {
    /// Record `bytes` as resident until the returned guard is dropped.
    pub(super) fn hold(&self, bytes: usize) -> MergeKeysHeld<'_> {
        self.current.store(bytes, AtomicOrdering::Relaxed);
        let _ = self.high_water.fetch_max(bytes, AtomicOrdering::Relaxed);
        MergeKeysHeld(self)
    }
}

/// What one table write put in the store.
pub(super) struct TableWritten {
    /// Rows the file holds.
    pub(super) rows: usize,
    /// Whether the completion's response was lost and a probe found the
    /// object this completion committed.
    pub(super) probed: bool,
    /// Why the upload of a found object may be left behind, if it may.
    pub(super) orphan: Option<String>,
}

/// How [`Sink::settle_completion`] left an unconfirmed completion.
pub(super) enum Settled {
    /// The object exists. `committed_here` when this upload committed it: the
    /// block's first write attempt, or an abort answered `NotFound`.
    Found {
        committed_here: bool,
        orphan: Option<String>,
    },
    /// The object does not exist and the upload was aborted.
    Absent { orphan: Option<String> },
    /// Whether the completion was applied is unknown; the upload was left
    /// alone and may be left behind for this reason.
    Unknown(String),
}

/// Clears the resident merge keys when a table write ends, however it ends.
pub(super) struct MergeKeysHeld<'a>(&'a MergeKeys);

impl MergeKeysHeld<'_> {
    /// Record `bytes` as what the table's merge now holds.
    pub(super) fn set(&self, bytes: usize) {
        self.0.current.store(bytes, AtomicOrdering::Relaxed);
        let _ = self.0.high_water.fetch_max(bytes, AtomicOrdering::Relaxed);
    }
}

impl Drop for MergeKeysHeld<'_> {
    fn drop(&mut self) {
        self.0.current.store(0, AtomicOrdering::Relaxed);
    }
}

/// The live flush workspace of the table being written.
///
/// The merge and encoder terms are published by the write loop at every
/// step; the upload term is read from the table's ledger when asked, so a
/// part that lands between two steps is released at once.
#[derive(Debug, Default)]
pub(super) struct FlushWorkspace {
    pub(super) merge: AtomicUsize,
    pub(super) encoder: AtomicUsize,
    pub(super) upload: std::sync::Mutex<Option<Arc<UploadLedger>>>,
    pub(super) high_water: AtomicUsize,
}

impl FlushWorkspace {
    /// Start accounting one table write, whose upload is `ledger`.
    pub(super) fn begin(&self, ledger: Arc<UploadLedger>) -> FlushWorkspaceHeld<'_> {
        *self
            .upload
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ledger);
        FlushWorkspaceHeld(self)
    }

    pub(super) fn bytes(&self) -> usize {
        let upload = self
            .upload
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map_or(0, |ledger| ledger.live());
        let bytes = self.merge.load(AtomicOrdering::Relaxed)
            + self.encoder.load(AtomicOrdering::Relaxed)
            + upload;
        let _ = self.high_water.fetch_max(bytes, AtomicOrdering::Relaxed);
        bytes
    }
}

/// Clears the flush workspace when a table write ends, however it ends.
pub(super) struct FlushWorkspaceHeld<'a>(&'a FlushWorkspace);

impl FlushWorkspaceHeld<'_> {
    /// Publish what the merge and the encoder hold at this step.
    pub(super) fn set(&self, merge: usize, encoder: usize) {
        self.0.merge.store(merge, AtomicOrdering::Relaxed);
        self.0.encoder.store(encoder, AtomicOrdering::Relaxed);
        let _ = self.0.bytes();
    }
}

impl Drop for FlushWorkspaceHeld<'_> {
    fn drop(&mut self) {
        self.0.merge.store(0, AtomicOrdering::Relaxed);
        self.0.encoder.store(0, AtomicOrdering::Relaxed);
        *self
            .0
            .upload
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

/// The upload bytes one table write holds: what the encoder handed the
/// buffered upload that the store has not acknowledged yet.
///
/// The encoder hands over one buffer per row group; the buffered upload cuts
/// parts out of that byte stream as slices, without copying, so a buffer
/// stays allocated until every part cut from it has landed. The ledger keeps
/// the stream range of every buffer and of every part, and releases a
/// buffer as soon as all of its bytes have landed, in whatever order the
/// parts land: a stalled early part keeps only the buffers it overlaps
/// charged, never the ones later parts have finished.
#[derive(Debug, Default)]
pub(super) struct UploadLedger {
    pub(super) state: std::sync::Mutex<LedgerState>,
}

#[derive(Debug, Default)]
pub(super) struct LedgerState {
    /// Handed buffers not yet released, by stream start offset: stream end
    /// offset, length, and the bytes of it that have not landed.
    pub(super) buffers: std::collections::BTreeMap<u64, (u64, usize, usize)>,
    /// Bytes handed so far: the stream offset of the next buffer.
    pub(super) handed: u64,
    /// Stream offset of the next part.
    pub(super) next_part: u64,
    /// Sum of the lengths of the buffers not yet released.
    pub(super) live: usize,
}

/// The stream range `[start, end)` of one multipart part.
pub(super) type PartSpan = (u64, u64);

impl LedgerState {
    /// The bytes `[start, end)` of the stream landed: take them off every
    /// buffer they overlap and release each buffer none of whose bytes is
    /// still outstanding.
    pub(super) fn land(&mut self, start: u64, end: u64) {
        let overlapping: Vec<u64> = self
            .buffers
            .range(..end)
            .rev()
            .take_while(|(_, (buffer_end, _, _))| *buffer_end > start)
            .map(|(buffer_start, _)| *buffer_start)
            .collect();
        for buffer_start in overlapping {
            let Some(entry) = self.buffers.get_mut(&buffer_start) else {
                continue;
            };
            let overlap = entry.0.min(end) - buffer_start.max(start);
            entry.2 = entry.2.saturating_sub(overlap as usize);
            if entry.2 == 0 {
                let len = entry.1;
                let _ = self.buffers.remove(&buffer_start);
                self.live -= len;
            }
        }
    }
}

impl UploadLedger {
    pub(super) fn state(&self) -> std::sync::MutexGuard<'_, LedgerState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The encoder handed the upload a buffer of `len` bytes.
    pub(super) fn handed(&self, len: usize) {
        if len == 0 {
            return;
        }
        let mut state = self.state();
        let start = state.handed;
        state.handed += len as u64;
        let end = state.handed;
        let _ = state.buffers.insert(start, (end, len, len));
        state.live += len;
    }

    /// A multipart part of `len` bytes was started; returns its stream
    /// range, which [`UploadLedger::part_landed`] takes.
    pub(super) fn part_started(&self, len: usize) -> PartSpan {
        let mut state = self.state();
        let start = state.next_part;
        state.next_part += len as u64;
        (start, state.next_part)
    }

    /// The part `span` landed, or was dropped with its payload.
    pub(super) fn part_landed(&self, span: PartSpan) {
        self.state().land(span.0, span.1);
    }

    /// A single-request put of everything handed so far landed, or was
    /// dropped with its payload.
    pub(super) fn put_landed(&self) {
        let mut state = self.state();
        let handed = state.handed;
        state.land(0, handed);
    }

    /// Bytes the upload holds right now.
    pub(super) fn live(&self) -> usize {
        self.state().live
    }
}

/// Marks a part landed when its upload future completes or is dropped.
pub(super) struct PartLanded {
    pub(super) ledger: Arc<UploadLedger>,
    pub(super) span: PartSpan,
}

impl Drop for PartLanded {
    fn drop(&mut self) {
        self.ledger.part_landed(self.span);
    }
}

/// Marks a single-request put landed when it completes or is dropped.
pub(super) struct PutLanded(Arc<UploadLedger>);

impl Drop for PutLanded {
    fn drop(&mut self) {
        self.0.put_landed();
    }
}

/// What one table write's multipart upload leaves once the writer has let go
/// of it.
#[derive(Debug, Default)]
pub(super) struct UploadSlot {
    /// The upload, when the writer dropped it neither completed nor aborted.
    parked: Option<Box<dyn object_store::MultipartUpload>>,
    /// Whether `complete` was called: from then on the store may have
    /// committed the object without the writer seeing it.
    completing: bool,
    /// Whether the creation failed without a definite rejection, so the store
    /// may hold an upload whose id the writer never received.
    create_unknown: bool,
}

/// An upload the writer let go of unsettled.
pub(super) struct Unsettled {
    pub(super) upload: Box<dyn object_store::MultipartUpload>,
    /// Whether its `complete` was called.
    pub(super) completing: bool,
}

/// Locks `slot`, recovering it from a poisoned lock.
fn lock_slot(slot: &std::sync::Mutex<UploadSlot>) -> std::sync::MutexGuard<'_, UploadSlot> {
    slot.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A multipart upload whose parts are entered in the table's ledger.
///
/// Dropped neither completed nor aborted, it parks the upload in the table's
/// [`UploadSlot`], so a write that fails while finishing can still abort it.
/// After a failed completion it refuses the abort that follows, which the
/// table write sends only once a probe has found no object (see
/// [`Sink::settle_completion`]).
#[derive(Debug)]
pub(super) struct LedgeredUpload {
    pub(super) inner: Option<Box<dyn object_store::MultipartUpload>>,
    pub(super) ledger: Arc<UploadLedger>,
    pub(super) slot: Arc<std::sync::Mutex<UploadSlot>>,
    /// Set once the upload completed or was aborted.
    pub(super) settled: bool,
    /// Set when `complete` returned an error: the store may have committed.
    pub(super) complete_failed: bool,
}

impl LedgeredUpload {
    fn inner(&mut self) -> &mut Box<dyn object_store::MultipartUpload> {
        self.inner
            .as_mut()
            .expect("the upload is only taken when it is dropped")
    }
}

impl Drop for LedgeredUpload {
    fn drop(&mut self) {
        if !self.settled {
            lock_slot(&self.slot).parked = self.inner.take();
        }
    }
}

#[async_trait::async_trait]
impl object_store::MultipartUpload for LedgeredUpload {
    fn put_part(&mut self, data: object_store::PutPayload) -> object_store::UploadPart {
        let landed = PartLanded {
            span: self.ledger.part_started(data.content_length()),
            ledger: Arc::clone(&self.ledger),
        };
        let part = self.inner().put_part(data);
        Box::pin(async move {
            let _landed = landed;
            part.await
        })
    }

    async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
        lock_slot(&self.slot).completing = true;
        let completed = self.inner().complete().await;
        self.settled = completed.is_ok();
        self.complete_failed = completed.is_err();
        completed
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        // `WriteMultipart::finish` aborts at once after a failed completion
        // and returns the abort's error in place of the completion's; the
        // completion's error is the one that tells a retry is safe.
        if self.complete_failed {
            return Ok(());
        }
        let aborted = self.inner().abort().await;
        self.settled = aborted.is_ok();
        aborted
    }
}

/// The encoder's side of one table's upload: every buffer it hands over is
/// entered in the table's ledger.
pub(super) struct LedgeredWriter {
    pub(super) inner: ParquetObjectWriter,
    pub(super) ledger: Arc<UploadLedger>,
}

impl LedgeredWriter {
    /// The buffered upload, for an abort.
    pub(super) fn into_buf_writer(self) -> BufWriter {
        self.inner.into_inner()
    }
}

impl AsyncFileWriter for LedgeredWriter {
    fn write(&mut self, bs: bytes::Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        self.ledger.handed(bs.len());
        self.inner.write(bs)
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        self.inner.complete()
    }
}

/// Heap the merge's work on `chunk` holds beside the block: what producing
/// it pins, plus the chunk itself when the merge allocated it.
///
/// With sorting disabled a chunk is one of the block's own runs, whose
/// buffers the block already accounts for, so it adds nothing.
pub(super) fn chunk_charge(merge: &MergeIter, chunk: &RecordBatch) -> usize {
    merge.chunk_workspace_bytes()
        + if merge.allocates_chunks() {
            record_batch_pinned_bytes(chunk, &mut CountedAllocations::default())
        } else {
            0
        }
}

/// The store one table is written through: the sink's own store, counting
/// the multipart uploads it is creating and entering the table's upload
/// bytes in its ledger.
pub(super) type CreationWatch = HookStore<Creations>;

/// `inner`, watched for the multipart uploads it is creating, with its
/// upload bytes entered in `ledger`.
pub(super) fn creation_watch(
    inner: Arc<dyn ObjectStore>,
    ledger: Arc<UploadLedger>,
) -> CreationWatch {
    HookStore::new(
        inner,
        Creations {
            state: Arc::new(CreationState::default()),
            ledger,
            slot: Arc::default(),
        },
    )
}

/// The hooks of a [`CreationWatch`].
///
/// `BufWriter::abort` can only abort an upload whose creation has finished,
/// and dropping a creation in flight leaves an upload no abort is sent for.
/// The count lets a cancelled write wait for exactly that creation.
#[derive(Debug)]
pub(super) struct Creations {
    state: Arc<CreationState>,
    /// Where the table's upload bytes are entered.
    ledger: Arc<UploadLedger>,
    /// What the table's upload leaves once the writer lets go of it.
    slot: Arc<std::sync::Mutex<UploadSlot>>,
}

/// Multipart creations in flight, and the wake-up of their end.
#[derive(Debug, Default)]
struct CreationState {
    creating: AtomicUsize,
    settled: tokio::sync::Notify,
}

/// Counts one multipart creation for as long as it is in flight.
struct Creating(Arc<CreationState>);

impl Drop for Creating {
    fn drop(&mut self) {
        let _ = self.0.creating.fetch_sub(1, SeqCst);
        self.0.settled.notify_waiters();
    }
}

impl Creations {
    /// Whether a multipart upload is being created right now.
    pub(super) fn creating(&self) -> bool {
        self.state.creating.load(SeqCst) > 0
    }

    /// The upload the writer let go of neither completed nor aborted, if any.
    pub(super) fn take_unsettled(&self) -> Option<Unsettled> {
        let mut slot = lock_slot(&self.slot);
        let completing = slot.completing;
        slot.parked
            .take()
            .map(|upload| Unsettled { upload, completing })
    }

    /// Why an upload may exist that no abort can reach: a creation that
    /// failed without a definite rejection.
    pub(super) fn create_unknown(&self) -> Option<String> {
        lock_slot(&self.slot).create_unknown.then(|| {
            "CreateMultipartUpload failed without a definite answer, so the store may hold an \
             upload whose id the writer never received"
                .to_owned()
        })
    }

    /// Resolves once no multipart upload is being created.
    pub(super) async fn settled(&self) {
        loop {
            let notified = self.state.settled.notified();
            tokio::pin!(notified);
            // Registered before the check, so a creation that ends between
            // the check and the wait still wakes it.
            let _ = notified.as_mut().enable();
            if !self.creating() {
                return;
            }
            notified.await;
        }
    }
}

#[async_trait::async_trait]
impl StoreHooks for Creations {
    async fn before_put(
        &self,
        _location: &Path,
        _payload: &object_store::PutPayload,
    ) -> object_store::Result<Option<HookGuard>> {
        Ok(Some(Box::new(PutLanded(Arc::clone(&self.ledger)))))
    }

    async fn before_multipart(&self, _location: &Path) -> object_store::Result<Option<HookGuard>> {
        let _ = self.state.creating.fetch_add(1, SeqCst);
        Ok(Some(Box::new(Creating(Arc::clone(&self.state)))))
    }

    fn multipart_failed(&self, _location: &Path, error: &object_store::Error) {
        // Every other variant is a definite answer from the store or a local
        // refusal, neither of which leaves an upload behind. `Generic` also
        // carries a definite 5xx, which counts too; and the client retried the
        // creation before this runs, each try possibly leaving an upload, so
        // one count can stand for up to `max_retries + 1` of them.
        if matches!(
            error,
            object_store::Error::Generic { .. } | object_store::Error::JoinError { .. }
        ) {
            lock_slot(&self.slot).create_unknown = true;
        }
    }

    fn wrap_upload(
        &self,
        _location: &Path,
        upload: Box<dyn object_store::MultipartUpload>,
    ) -> Box<dyn object_store::MultipartUpload> {
        Box::new(LedgeredUpload {
            inner: Some(upload),
            ledger: Arc::clone(&self.ledger),
            slot: Arc::clone(&self.slot),
            settled: false,
            complete_failed: false,
        })
    }
}

impl Sink {
    /// Best-effort abort of a still-writable upload, by `deadline`.
    ///
    /// Bounded by `deadline`, so a wedged store cannot block the flush
    /// task. Returns why the abort did not succeed, or `None` when it did.
    pub(super) async fn abort_upload(
        &self,
        writer: AsyncArrowWriter<LedgeredWriter>,
        deadline: AbortTimer,
    ) -> Option<String> {
        let mut buf: BufWriter = writer.into_inner().into_buf_writer();
        self.bounded_abort(buf.abort(), deadline).await
    }

    /// Run `abort` until `deadline`; returns why it did not succeed, or
    /// `None` when it did.
    async fn bounded_abort(
        &self,
        abort: impl Future<Output = object_store::Result<()>>,
        deadline: AbortTimer,
    ) -> Option<String> {
        tokio::select! {
            biased;
            aborted = abort => aborted.err().map(|e| e.to_string()),
            () = deadline => Some(format!(
                "abort timed out after {:?}",
                self.cfg.upload.abort_timeout
            )),
        }
    }

    /// Settle an upload whose completion was sent but not confirmed, by
    /// `deadline`, the write's one cleanup allowance.
    ///
    /// A HEAD of `path` decides whether the object exists. Only an answer,
    /// found or `NotFound`, allows the abort of the upload: any other HEAD
    /// failure leaves it alone, since the completion may still be applied.
    /// A found object holds the block's frozen bytes. On the block's first
    /// write attempt nothing else can have written it (see
    /// [`Block::begin_write_attempt`](crate::buffer::Block::begin_write_attempt)),
    /// so this completion committed it whatever the abort answers, and left no
    /// upload behind: some stores (MinIO) accept the abort of a completed
    /// upload, others refuse it. On a later attempt an earlier one may have
    /// written it, and the abort tells: `NotFound` means this completion
    /// committed, success means the upload was still open.
    async fn settle_completion(
        &self,
        mut upload: Box<dyn object_store::MultipartUpload>,
        path: &Path,
        first_attempt: bool,
        mut deadline: AbortTimer,
    ) -> Settled {
        let timed_out = || {
            format!(
                "the probe and abort of an unconfirmed completion timed out after {:?}",
                self.cfg.upload.abort_timeout
            )
        };
        let found = match probe_block(&*self.store, std::slice::from_ref(path), &mut deadline).await
        {
            Presence::All => true,
            Presence::None | Presence::Some(_) => false,
            Presence::Unknown(reason) => {
                return Settled::Unknown(format!(
                    "a HEAD could not tell whether an unconfirmed completion was applied, so the \
                     upload was not aborted: {reason}"
                ));
            }
        };
        let aborted = tokio::select! {
            biased;
            aborted = upload.abort() => aborted,
            () = &mut deadline => Err(object_store::Error::Generic {
                store: "series-lake",
                source: timed_out().into(),
            }),
        };
        let (committed_here, orphan) = match aborted {
            Ok(()) => (first_attempt, None),
            Err(object_store::Error::NotFound { .. }) => (true, None),
            Err(_) if found && first_attempt => (true, None),
            Err(error) => (false, Some(error.to_string())),
        };
        if found {
            Settled::Found {
                committed_here,
                orphan,
            }
        } else {
            Settled::Absent { orphan }
        }
    }

    /// Why the upload of `path` may be left behind, prefixed with the key an
    /// operator or a lifecycle sweep finds it by.
    fn orphan(path: &Path, reason: String) -> String {
        format!("multipart upload of {path}: {reason}")
    }

    /// Run one writable-phase writer step, racing `cancel`.
    ///
    /// A step creating the multipart upload when the token fires keeps being
    /// driven until the creation finishes or the cleanup deadline passes (see
    /// [`CreationWatch`]); that deadline is taken at the first cancellation
    /// and shared with the abort. A step blocked on anything else is dropped
    /// at once, so the abort starts with the whole allowance. Either way the
    /// step reports `Cancelled`.
    pub(super) async fn step(
        &self,
        op: impl Future<Output = parquet::errors::Result<()>>,
        watch: &CreationWatch,
        cancel: &CancellationToken,
        cleanup: &mut Option<AbortTimer>,
    ) -> Result<()> {
        tokio::pin!(op);
        // The token is polled first: once it has fired, the step is not
        // driven again, so a writer step that would have made more progress
        // on this poll does not run before the cancellation is acted on.
        tokio::select! {
            biased;
            () = cancel.cancelled() => {}
            result = &mut op => return result.map_err(Error::from),
        }
        let deadline = cleanup.get_or_insert_with(|| self.start_cleanup());
        if !watch.hooks().creating() {
            return Err(Error::cancelled(None));
        }
        tokio::select! {
            biased;
            _ = &mut op => Err(Error::cancelled(None)),
            () = watch.hooks().settled() => Err(Error::cancelled(None)),
            () = deadline => Err(Error::cancelled(Some(format!(
                "the write in flight did not finish within {:?}, so a multipart upload \
                 it was creating may be left to the bucket lifecycle rule",
                self.cfg.upload.abort_timeout
            )))),
        }
    }

    /// The cancellation a table write has just observed at one of its step
    /// boundaries.
    ///
    /// The cleanup timer is started here, where the token is seen, rather
    /// than after the write has released its merge, so dropping a large
    /// merge is spent out of the abort's allowance instead of postponing
    /// the start of it.
    pub(super) fn cancelled_here(&self, cleanup: &mut Option<AbortTimer>) -> Error {
        if cleanup.is_none() {
            *cleanup = Some(self.start_cleanup());
        }
        Error::cancelled(None)
    }

    /// Start the cleanup allowance of a failed or cancelled write.
    pub(super) fn start_cleanup(&self) -> AbortTimer {
        (self.abort_timer)(self.cfg.upload.abort_timeout)
    }

    /// Attach the outcome of the cleanup abort of the upload of `path` to the
    /// failure that triggered it, each reason prefixed with the key (see
    /// [`Self::orphan`]).
    ///
    /// A cancellation that already carries a cleanup failure keeps it: the
    /// abort that follows cannot see what the unsettled write left behind.
    pub(super) fn with_abort(cause: Error, abort_error: Option<String>, path: &Path) -> Error {
        let abort_error = abort_error.map(|reason| Self::orphan(path, reason));
        match (cause, abort_error) {
            (
                Error::Transient(TransientError::Cancelled {
                    abort_error: earlier,
                }),
                abort_error,
            ) => Error::cancelled(
                earlier
                    .map(|reason| Self::orphan(path, reason))
                    .or(abort_error),
            ),
            (cause, None) => cause,
            (cause, Some(abort_error)) => Error::Transient(TransientError::AbortFailed {
                source: Box::new(cause),
                abort_error,
            }),
        }
    }

    /// Return to the runtime, then report a cancellation seen on the way back.
    async fn checkpoint(
        &self,
        cancel: &CancellationToken,
        cleanup: &mut Option<AbortTimer>,
    ) -> Result<()> {
        tokio::task::yield_now().await;
        if cancel.is_cancelled() {
            return Err(self.cancelled_here(cleanup));
        }
        Ok(())
    }

    /// Merge a table's runs and hand every chunk to `writer`; returns the
    /// rows written.
    ///
    /// The write is a sequence of bounded steps with a [`Sink::checkpoint`]
    /// between every two of them, because the table's task shares its thread
    /// with the node loop that admits requests, delivers acks and nacks,
    /// answers telemetry and watches the shutdown deadline. Nothing about the
    /// steps reaches the file: the chunks, their order and the writer calls
    /// are the same as an unsliced write makes. `AsyncArrowWriter::write`
    /// usually completes synchronously, and so does a buffered upload whose
    /// store is ready, so every checkpoint yields explicitly.
    #[allow(clippy::too_many_arguments)]
    async fn write_chunks(
        &self,
        mut build: MergeBuild,
        writer: &mut AsyncArrowWriter<LedgeredWriter>,
        watch: &CreationWatch,
        keys: &MergeKeysHeld<'_>,
        workspace: &FlushWorkspaceHeld<'_>,
        cancel: &CancellationToken,
        cleanup: &mut Option<AbortTimer>,
    ) -> Result<usize> {
        // Step 1: encode the merge keys, one bounded slice at a time.
        while !build.step()? {
            keys.set(build.resident_key_bytes());
            self.checkpoint(cancel, cleanup).await?;
        }
        let mut merge = build.finish()?;
        keys.set(merge.resident_key_bytes());
        // Step 2: produce, encode and flush the chunks, one bounded step at a
        // time.
        let mut rows = 0usize;
        loop {
            self.checkpoint(cancel, cleanup).await?;
            let chunk = match merge.step() {
                MergeStep::Done => break,
                MergeStep::Progress => {
                    workspace.set(merge.chunk_workspace_bytes(), writer.memory_size());
                    continue;
                }
                MergeStep::Chunk(c) => c,
                MergeStep::Ready => {
                    // Build the chunk's columns in bounded steps of their
                    // own, returning to the runtime between every two.
                    let chunk = {
                        let mut builder = merge.chunk_builder();
                        loop {
                            if builder.step()? {
                                break builder.finish()?;
                            }
                            workspace.set(
                                merge.chunk_workspace_bytes() + builder.workspace_bytes(),
                                writer.memory_size(),
                            );
                            self.checkpoint(cancel, cleanup).await?;
                        }
                    };
                    merge.chunk_taken();
                    chunk
                }
            };
            workspace.set(chunk_charge(&merge, &chunk), writer.memory_size());
            // Encoding one chunk is a stretch of its own, bounded by
            // `merge_chunk_bytes`, so it gets a checkpoint of its own.
            self.checkpoint(cancel, cleanup).await?;
            self.step(writer.write(&chunk), watch, cancel, cleanup)
                .await?;
            rows += chunk.num_rows();
            drop(chunk);
            workspace.set(merge.chunk_workspace_bytes(), writer.memory_size());
            if row_group_full(
                &self.cfg.parquet,
                writer.memory_size(),
                writer.in_progress_size(),
            ) {
                // Closing a row group is the other stretch, bounded by
                // `row_group_bytes`.
                self.checkpoint(cancel, cleanup).await?;
                self.step(writer.flush(), watch, cancel, cleanup).await?;
                workspace.set(merge.chunk_workspace_bytes(), writer.memory_size());
            }
        }
        Ok(rows)
    }

    pub(super) async fn write_table(
        &self,
        table: &SortedTableBuffer,
        path: &Path,
        seq: u64,
        window_start_secs: i64,
        first_attempt: bool,
        cancel: &CancellationToken,
    ) -> Result<TableWritten> {
        let runs: Vec<RecordBatch> = table.iter_snapshots().cloned().collect();
        let total_rows: usize = runs.iter().map(RecordBatch::num_rows).sum();
        // A series table has no `time_unix_nano` column, so the scan would be
        // pure overhead and the metadata keys it feeds are values-only anyway.
        let range = if table.dataset().is_series() {
            (None, None)
        } else {
            time_range(&runs)
        };
        let schema = dataset_schema(table.dataset(), &self.cfg);
        let sorting = native_sorting_columns(table.spec(), &schema)?;
        let props = writer_properties(compression())
            .set_sorting_columns(sorting)
            .set_key_value_metadata(Some(self.file_metadata(
                table,
                total_rows,
                range,
                seq,
                window_start_secs,
            )))
            .build();
        let ledger = Arc::new(UploadLedger::default());
        let watch = Arc::new(creation_watch(self.store.clone(), Arc::clone(&ledger)));
        let buf = BufWriter::with_capacity(
            Arc::clone(&watch) as Arc<dyn ObjectStore>,
            path.clone(),
            self.cfg.upload.part_bytes,
        )
        .with_max_concurrency(self.cfg.upload.concurrency);
        let object_writer = LedgeredWriter {
            inner: ParquetObjectWriter::from_buf_writer(buf),
            ledger: Arc::clone(&ledger),
        };
        let mut writer = AsyncArrowWriter::try_new(object_writer, schema, Some(props))?;
        // Held until this function returns: the merge chunk, the encoder and
        // the upload are the table's own, and none outlives its write.
        let workspace = self.workspace.begin(ledger);

        // Phase 1: the writer is writable. Every await races the token, and every
        // failure aborts the multipart upload.
        let build = MergeBuild::new(runs, table.spec(), self.cfg.sorting.merge_chunk_bytes)?;
        // Held until this function returns, which is when the keys are
        // dropped. Raised as the keys are encoded, then set once to the
        // merge's bound for its whole life, so no later chunk is
        // under-charged.
        let keys = self.merge_keys.hold(0);
        let mut cleanup: Option<AbortTimer> = None;
        let written = self
            .write_chunks(
                build,
                &mut writer,
                &watch,
                &keys,
                &workspace,
                cancel,
                &mut cleanup,
            )
            .await;
        workspace.set(0, writer.memory_size());
        let rows = match written {
            Ok(rows) => rows,
            Err(cause) => {
                let deadline = cleanup.unwrap_or_else(|| self.start_cleanup());
                let abort_error = self
                    .abort_upload(writer, deadline)
                    .await
                    .or_else(|| watch.hooks().create_unknown());
                return Err(Self::with_abort(cause, abort_error, path));
            }
        };
        if cancel.is_cancelled() {
            let abort_error = self
                .abort_upload(
                    writer,
                    cleanup.take().unwrap_or_else(|| self.start_cleanup()),
                )
                .await
                .map(|reason| Self::orphan(path, reason));
            return Err(Error::cancelled(abort_error));
        }

        // Phase 2: finalizing. `finish` writes the footer and shuts the BufWriter
        // down; `BufWriter::abort` panics once shutdown has started, so a failed or
        // cancelled finish settles the upload the writer let go of instead (see
        // `LedgeredUpload`). A table smaller than one row group first reaches the
        // store here, so `finish` may be creating the upload when the token fires.
        let finish = self
            .step(
                async { writer.finish().await.map(|_metadata| ()) },
                &watch,
                cancel,
                &mut cleanup,
            )
            .await;
        let Err(cause) = finish else {
            return Ok(TableWritten {
                rows,
                probed: false,
                orphan: None,
            });
        };
        // Dropping the writer parks an upload a cancelled finish still held.
        drop(writer);
        let abort_error = match watch.hooks().take_unsettled() {
            Some(Unsettled {
                mut upload,
                completing: false,
            }) => {
                let deadline = cleanup.take().unwrap_or_else(|| self.start_cleanup());
                self.bounded_abort(upload.abort(), deadline).await
            }
            Some(Unsettled {
                upload,
                completing: true,
            }) => match self
                .settle_completion(
                    upload,
                    path,
                    first_attempt,
                    cleanup.take().unwrap_or_else(|| self.start_cleanup()),
                )
                .await
            {
                // A cancelled write stays cancelled: its caller has decided the
                // block, and probes for a late commit itself.
                Settled::Found {
                    committed_here,
                    orphan,
                } if !cause.is_cancelled() => {
                    return Ok(TableWritten {
                        rows,
                        probed: committed_here,
                        orphan: orphan.map(|reason| Self::orphan(path, reason)),
                    });
                }
                Settled::Found { orphan, .. } | Settled::Absent { orphan } => orphan,
                Settled::Unknown(reason) => Some(reason),
            },
            None => watch.hooks().create_unknown(),
        };
        Err(Self::with_abort(cause, abort_error, path))
    }

    /// Write every non-empty table of a sealed block, series datasets first.
    ///
    /// # Errors
    ///
    /// Returns [`TransientError::Cancelled`] when `cancel` fires, and the underlying
    /// Arrow, Parquet or object store failure otherwise.
    pub async fn write_block(
        &self,
        block: &Block,
        cancel: &CancellationToken,
    ) -> Result<FlushReport> {
        // Series rows exist from admission onwards, but they carry a
        // placeholder `emitted_at` of zero until the block's stamp transaction
        // commits. Writing an unsealed block would therefore publish unstamped
        // data. This is a runtime check, not a debug assertion: the files would
        // be just as wrong in a release build.
        if !block.is_sealed() {
            return Err(Error::internal("unsealed block"));
        }
        if cancel.is_cancelled() {
            return Err(Error::cancelled(None));
        }
        let first_attempt = block.begin_write_attempt() == 1;
        let mut report = FlushReport::default();
        for (table, path) in block
            .tables()
            .filter(|table| !table.is_empty())
            .zip(self.planned_paths(block))
        {
            let written = self
                .write_table(
                    table,
                    &path,
                    block.seq,
                    block.window_start_secs,
                    first_attempt,
                    cancel,
                )
                .await?;
            report.probed_commits += usize::from(written.probed);
            report.possible_orphans.extend(written.orphan);
            report.files.push((table.dataset(), path, written.rows));
        }
        Ok(report)
    }
}
