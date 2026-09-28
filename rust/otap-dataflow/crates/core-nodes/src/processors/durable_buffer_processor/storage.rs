// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The storage worker: one thread per buffer that runs Quiver's shutdown file
//! work off the pipeline thread, and the registry that keeps a later buffer
//! out of the core directory while that thread lives.

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use otel_arrow_dfe_quiver::QuiverEngine;
use tokio::sync::{mpsc, oneshot};

/// How long opening a core directory waits for the storage workers of
/// earlier buffers on it to end before it refuses.
pub(super) const WORKER_WAIT: Duration = Duration::from_secs(5);

/// Live storage workers per core directory, in this process.
static LIVE_WORKERS: Mutex<Option<HashMap<PathBuf, usize>>> = Mutex::new(None);

/// Registers a live storage worker on one core directory until dropped.
///
/// A buffer's storage worker can outlive the buffer, still finalizing a
/// segment or writing the progress file of its directory; a later buffer on
/// the same core must not open the directory before it ends. Directories are
/// keyed by their canonical path, so two spellings of one directory match.
///
/// It keeps a buffer out only while an earlier buffer's storage worker runs,
/// which starts at that buffer's shutdown; it does not keep two buffers that
/// both run on one directory apart, as in a `replace` rollout.
#[derive(Debug)]
pub(super) struct WorkerLease {
    dir: PathBuf,
}

/// The registry key of `dir`: its canonical path, else `dir` as given (a
/// directory not created yet has no worker).
fn lease_key(dir: &Path) -> PathBuf {
    std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf())
}

impl WorkerLease {
    /// Registers a worker on `dir`.
    pub(super) fn new(dir: &Path) -> Self {
        let dir = lease_key(dir);
        let mut live = LIVE_WORKERS.lock().unwrap_or_else(|p| p.into_inner());
        *live
            .get_or_insert_with(HashMap::new)
            .entry(dir.clone())
            .or_default() += 1;
        Self { dir }
    }

    /// Waits up to `wait` until no storage worker lives on `dir`.
    pub(super) async fn wait_for_none(dir: &Path, wait: Duration) -> Result<(), String> {
        let give_up = otel_arrow_dfe_engine::clock::now() + wait;
        let key = lease_key(dir);
        loop {
            let live = LIVE_WORKERS
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
                .and_then(|live| live.get(&key).copied())
                .unwrap_or(0);
            if live == 0 {
                return Ok(());
            }
            if otel_arrow_dfe_engine::clock::now() >= give_up {
                return Err(format!(
                    "{} is still in use by the storage thread of an earlier durable buffer",
                    dir.display()
                ));
            }
            otel_arrow_dfe_engine::clock::sleep(Duration::from_millis(20)).await;
        }
    }
}

impl Drop for WorkerLease {
    fn drop(&mut self) {
        let mut live = LIVE_WORKERS.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(live) = live.as_mut()
            && let Some(count) = live.get_mut(&self.dir)
        {
            *count -= 1;
            if *count == 0 {
                let _ = live.remove(&self.dir);
            }
        }
    }
}

/// What the final job releases after the engine's shutdown: the bundle
/// handles still in flight.
pub(super) type ReleasePayload = Box<dyn Send>;

/// A job for the storage worker.
enum Job {
    /// Finalize the open segment.
    Flush {
        engine: Arc<QuiverEngine>,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Persist the recorded progress at `not_before` (see [`serve`]).
    Persist {
        engine: Arc<QuiverEngine>,
        not_before: Instant,
    },
    /// Persist the progress, shut the engine down and drop it; the last job.
    Close {
        engine: Arc<QuiverEngine>,
        payload: ReleasePayload,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Drop the engine and bundle handles of a buffer dropped before its
    /// close, once the jobs before it are done; the last job.
    Release(ReleasePayload),
}

impl Job {
    /// Drops the job unanswered: its reply first, so the caller learns of it
    /// before a blocking drop of the engine or the payload ends.
    fn refuse(self) {
        match self {
            Self::Flush { engine, reply } => {
                drop(reply);
                drop(engine);
            }
            Self::Persist { engine, .. } => drop(engine),
            Self::Release(payload) => drop(payload),
            Self::Close {
                engine,
                payload,
                reply,
            } => {
                drop(reply);
                drop(payload);
                drop(engine);
            }
        }
    }
}

/// The failure of a job the worker did not answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Unanswered {
    /// No thread could be started for the worker.
    NoThread(String),
    /// The worker's thread could not build its runtime; it dropped the job.
    NoRuntime(String),
    /// The worker's thread panicked.
    Panicked,
}

impl std::fmt::Display for Unanswered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoThread(e) => write!(f, "no thread could run the storage worker: {e}"),
            Self::NoRuntime(e) => write!(f, "the storage worker has no runtime: {e}"),
            Self::Panicked => f.write_str("the storage worker panicked"),
        }
    }
}

/// One thread with a current-thread runtime that runs the buffer's shutdown
/// storage jobs.
///
/// Progress persists run one at a time, coalesced (see [`serve`]), since they
/// write the same file; the segment flush runs beside them, and the final
/// close persists, waits for the flush, persists again and shuts the engine
/// down. A deadline stops the pipeline waiting for a job, never the job, so a
/// segment finalize or a progress write is never cut half way.
pub(super) struct StorageWorker {
    jobs: Option<mpsc::UnboundedSender<Job>>,
    /// Why the worker is unavailable, once known.
    failure: Arc<std::sync::OnceLock<Unanswered>>,
    /// Resolves once the thread has ended.
    ended: Option<oneshot::Receiver<()>>,
}

impl StorageWorker {
    /// Starts the worker, which holds `lease` until its thread ends.
    pub(super) fn start(lease: WorkerLease, faults: &StorageFaults) -> Self {
        let failure = Arc::new(std::sync::OnceLock::new());
        let (jobs_tx, jobs_rx) = mpsc::unbounded_channel();
        let (ended_tx, ended) = oneshot::channel::<()>();
        let thread_failure = Arc::clone(&failure);
        let thread_faults = faults.clone();
        let spawned = faults.spawn_thread(move || {
            let served = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                match thread_faults.build_runtime() {
                    Ok(runtime) => {
                        let local = tokio::task::LocalSet::new();
                        local.block_on(&runtime, serve(jobs_rx, thread_faults));
                    }
                    Err(e) => {
                        let _ = thread_failure.set(Unanswered::NoRuntime(e.to_string()));
                        // Each job, and the payload a close carries, is
                        // dropped here, off the pipeline thread.
                        let mut jobs_rx = jobs_rx;
                        while let Some(job) = jobs_rx.blocking_recv() {
                            job.refuse();
                        }
                    }
                }
            }));
            // Released before the end is reported, so whoever waits for the
            // end finds the directory free.
            drop(lease);
            match served {
                Ok(()) => {
                    let _ = ended_tx.send(());
                }
                Err(_) => {
                    let _ = thread_failure.set(Unanswered::Panicked);
                }
            }
        });
        match spawned {
            Ok(()) => Self {
                jobs: Some(jobs_tx),
                failure,
                ended: Some(ended),
            },
            Err(e) => {
                let _ = failure.set(Unanswered::NoThread(e.to_string()));
                Self {
                    jobs: None,
                    failure,
                    ended: None,
                }
            }
        }
    }

    /// Why a job went unanswered.
    fn unanswered(&self) -> Unanswered {
        self.failure.get().cloned().unwrap_or(Unanswered::Panicked)
    }

    /// Submits `job`; hands it back when the worker cannot take it.
    fn submit(&self, job: Job) -> Result<(), Job> {
        match &self.jobs {
            Some(jobs) => jobs.send(job).map_err(|e| e.0),
            None => Err(job),
        }
    }

    /// Finalizes the open segment; the answer arrives on the returned future.
    pub(super) fn flush(
        &self,
        engine: Arc<QuiverEngine>,
    ) -> impl Future<Output = Result<(), String>> + use<> {
        let (reply, answer) = oneshot::channel();
        let refused = self.submit(Job::Flush { engine, reply }).is_err();
        let failure = Arc::clone(&self.failure);
        async move {
            if refused {
                return Err(unanswered(&failure).to_string());
            }
            answer
                .await
                .unwrap_or_else(|_| Err(unanswered(&failure).to_string()))
        }
    }

    /// Persists the recorded progress at `not_before`, coalesced with the
    /// other persists pending; a close drops them (see [`serve`]).
    pub(super) fn persist(&self, engine: Arc<QuiverEngine>, not_before: Instant) {
        let _ = self.submit(Job::Persist { engine, not_before });
    }

    /// Drops `payload` on the worker once the jobs before it are done; hands
    /// it back when no thread runs the worker.
    pub(super) fn release(&self, payload: ReleasePayload) -> Result<(), ReleasePayload> {
        match self.submit(Job::Release(payload)) {
            Ok(()) => Ok(()),
            Err(Job::Release(payload)) => Err(payload),
            Err(_) => unreachable!("submit hands back the job it was given"),
        }
    }

    /// Persists the progress, shuts the engine down, then drops it and
    /// `payload`. The worker ends after this job.
    ///
    /// `Err` hands `engine` and `payload` back when no thread runs the
    /// worker, so the caller decides what to do with them.
    pub(super) fn close(
        &mut self,
        engine: Arc<QuiverEngine>,
        payload: ReleasePayload,
    ) -> Result<Closing, (Arc<QuiverEngine>, ReleasePayload, Unanswered)> {
        let (reply, answer) = oneshot::channel();
        let job = Job::Close {
            engine,
            payload,
            reply,
        };
        match self.submit(job) {
            Ok(()) => {
                // No job follows the close, so the thread ends once it is done.
                drop(self.jobs.take());
                Ok(Closing {
                    answer,
                    failure: Arc::clone(&self.failure),
                    ended: self.ended.take(),
                })
            }
            Err(Job::Close {
                engine, payload, ..
            }) => Err((engine, payload, self.unanswered())),
            Err(_) => unreachable!("submit hands back the job it was given"),
        }
    }
}

/// The failure recorded in `failure`, else a panic.
fn unanswered(failure: &std::sync::OnceLock<Unanswered>) -> Unanswered {
    failure.get().cloned().unwrap_or(Unanswered::Panicked)
}

/// A close job in progress.
pub(super) struct Closing {
    answer: oneshot::Receiver<Result<(), String>>,
    failure: Arc<std::sync::OnceLock<Unanswered>>,
    ended: Option<oneshot::Receiver<()>>,
}

impl Closing {
    /// The close's result: the final persist and the engine's shutdown.
    pub(super) async fn persisted(&mut self) -> Result<Result<(), String>, Unanswered> {
        (&mut self.answer)
            .await
            .map_err(|_| unanswered(&self.failure))
    }

    /// Resolves once the worker's thread has ended, after its runtime and
    /// everything the close released were dropped; `Err` when it panicked.
    pub(super) async fn ended(&mut self) -> Result<(), Unanswered> {
        match self.ended.as_mut() {
            Some(ended) => ended.await.map_err(|_| unanswered(&self.failure)),
            None => Err(unanswered(&self.failure)),
        }
    }
}

/// Runs the jobs of one worker until the close, or until the buffer drops
/// the worker.
///
/// A persist is not waited for in line: the worker keeps one pending persist,
/// the earliest slot asked for, with its engine, and runs it when it is due,
/// so the jobs behind it are still served. Every job already queued is taken
/// at once and persists are coalesced over them, so a backlog that built up
/// behind a slow persist runs as at most one more. A queued job goes before a
/// due persist, and a close, which persists itself, drops the pending one.
async fn serve(mut jobs: mpsc::UnboundedReceiver<Job>, faults: StorageFaults) {
    let mut flush: Option<tokio::task::JoinHandle<()>> = None;
    let mut due: Option<(Instant, Arc<QuiverEngine>)> = None;
    let mut released = Vec::new();
    loop {
        let first = tokio::select! {
            biased;
            job = jobs.recv() => job,
            () = async {
                match &due {
                    Some((at, _)) => tokio::time::sleep_until((*at).into()).await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some((_, engine)) = due.take() {
                    persist_progress(engine, &faults).await;
                }
                continue;
            }
        };
        let Some(first) = first else {
            // Dropped by the buffer: what was recorded is persisted now, and
            // a flush still running is finished, not cancelled.
            if let Some((_, engine)) = due.take() {
                persist_progress(engine, &faults).await;
            }
            if let Some(flush) = flush.take() {
                let _ = flush.await;
            }
            drop(released);
            return;
        };
        let mut batch = vec![first];
        while let Ok(job) = jobs.try_recv() {
            batch.push(job);
        }
        // A persist whose slot has come runs once for the whole batch; the
        // earliest one still in the future stays pending.
        let now = Instant::now();
        let mut due_now: Option<Arc<QuiverEngine>> = None;
        if due.as_ref().is_some_and(|(at, _)| *at <= now) {
            due_now = due.take().map(|(_, engine)| engine);
        }
        let mut close = None;
        for job in batch {
            match job {
                Job::Flush { engine, reply } => {
                    let faults = faults.clone();
                    flush = Some(tokio::task::spawn_local(async move {
                        faults.before(Step::Flush).await;
                        let flushed = engine.flush().await.map_err(|e| e.to_string());
                        drop(engine);
                        faults.record("flushed");
                        let _ = reply.send(flushed);
                    }));
                }
                Job::Persist { engine, not_before } if not_before <= now => {
                    due_now = Some(engine);
                }
                Job::Persist { engine, not_before } => {
                    if due.as_ref().is_none_or(|(at, _)| not_before < *at) {
                        due = Some((not_before, engine));
                    }
                }
                // Dropped once the buffer has dropped the worker.
                Job::Release(payload) => released.push(payload),
                // No job follows the close.
                Job::Close {
                    engine,
                    payload,
                    reply,
                } => close = Some((engine, payload, reply)),
            }
        }
        if let Some((engine, payload, reply)) = close {
            drop(due_now);
            drop(due.take());
            faults.before(Step::Close).await;
            let closed = close_storage(&engine, flush.take(), &faults).await;
            let _ = reply.send(closed);
            drop(payload);
            drop(engine);
            faults.after_close().await;
            return;
        }
        if let Some(engine) = due_now {
            persist_progress(engine, &faults).await;
        }
    }
}

/// Persist the progress recorded so far, then release `engine`.
async fn persist_progress(engine: Arc<QuiverEngine>, faults: &StorageFaults) {
    faults.before(Step::Persist).await;
    if let Err(e) = faults.flush_progress(&engine).await {
        otel_arrow_dfe_telemetry::otel_error!("durable_buffer.shutdown.progress_failed", error = %e);
    }
    drop(engine);
    faults.record("persisted");
}

/// The final persist: the recorded progress, then, once the shutdown flush
/// has ended (it may resolve segments), again, then the engine's shutdown,
/// which finalizes the open segment. `Err` when the last persist or the
/// shutdown failed.
async fn close_storage(
    engine: &QuiverEngine,
    flush: Option<tokio::task::JoinHandle<()>>,
    faults: &StorageFaults,
) -> Result<(), String> {
    let mut persisted = faults.flush_progress(engine).await;
    if let Some(flush) = flush {
        let _ = flush.await;
        persisted = faults.flush_progress(engine).await;
    }
    engine.shutdown().await.map_err(|e| e.to_string())?;
    persisted.map_err(|e| format!("the final progress persist failed: {e}"))
}

/// A step of the storage worker a test can stall.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Step {
    /// The segment flush.
    Flush,
    /// A progress persist before the close.
    Persist,
    /// The final close.
    Close,
}

/// Faults the shutdown tests inject into the storage worker, and what it did;
/// every hook is a no-op outside tests.
#[cfg(not(test))]
#[derive(Clone, Default)]
pub(super) struct StorageFaults {}

#[cfg(not(test))]
impl StorageFaults {
    fn spawn_thread(&self, body: impl FnOnce() + Send + 'static) -> std::io::Result<()> {
        std::thread::Builder::new()
            .name("durable-buffer-storage".to_owned())
            .spawn(body)
            .map(drop)
    }

    fn build_runtime(&self) -> std::io::Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
    }

    #[allow(clippy::unused_async)]
    async fn before(&self, _step: Step) {}

    #[allow(clippy::unused_async)]
    async fn after_close(&self) {}

    async fn flush_progress(&self, engine: &QuiverEngine) -> Result<(), String> {
        engine
            .flush_progress()
            .await
            .map(drop)
            .map_err(|e| e.to_string())
    }

    pub(super) fn record(&self, _event: &'static str) {}

    pub(super) const fn worker_wait(&self) -> Duration {
        WORKER_WAIT
    }
}

/// Faults the shutdown tests inject into the storage worker, and what it did.
#[cfg(test)]
#[derive(Clone, Default)]
pub(super) struct StorageFaults {
    /// Steps that never finish until the gate opens.
    pub(super) stall: Vec<(Step, StallGate)>,
    /// Makes starting the worker's thread fail.
    pub(super) fail_thread: bool,
    /// Makes the worker's thread fail to build its runtime.
    pub(super) fail_runtime: bool,
    /// Makes the worker panic after the close.
    pub(super) panic_after_close: bool,
    /// Keeps the thread from ending for this long after the close: its
    /// runtime's drop waits for a blocking task.
    pub(super) stall_end: Option<Duration>,
    /// How long opening a directory waits for earlier storage workers.
    pub(super) worker_wait: Option<Duration>,
    /// Makes every progress persist fail.
    pub(super) fail_persist: bool,
    /// In order: `flushed` when the shutdown flush ended, `persisted` when a
    /// progress persist before the close ended, and the outcomes the buffer
    /// records (see `DurableBuffer::shutdown_engine`).
    pub(super) events: Arc<Mutex<Vec<&'static str>>>,
}

/// A gate a stalled step waits on until the test opens it.
#[cfg(test)]
#[derive(Clone, Default)]
pub(super) struct StallGate(Arc<tokio::sync::Notify>, Arc<std::sync::atomic::AtomicBool>);

#[cfg(test)]
impl StallGate {
    /// Lets the stalled step, and any later one, go on.
    pub(super) fn open(&self) {
        self.1.store(true, std::sync::atomic::Ordering::SeqCst);
        self.0.notify_waiters();
    }

    async fn wait(&self) {
        loop {
            let notified = self.0.notified();
            if self.1.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
impl StorageFaults {
    fn spawn_thread(&self, body: impl FnOnce() + Send + 'static) -> std::io::Result<()> {
        if self.fail_thread {
            return Err(std::io::Error::other("injected thread spawn failure"));
        }
        std::thread::Builder::new()
            .name("durable-buffer-storage".to_owned())
            .spawn(body)
            .map(drop)
    }

    fn build_runtime(&self) -> std::io::Result<tokio::runtime::Runtime> {
        if self.fail_runtime {
            return Err(std::io::Error::other("injected runtime failure"));
        }
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
    }

    async fn before(&self, step: Step) {
        for (stalled, gate) in &self.stall {
            if *stalled == step {
                gate.wait().await;
            }
        }
    }

    async fn after_close(&self) {
        assert!(!self.panic_after_close, "injected storage worker panic");
        if let Some(stall_end) = self.stall_end {
            let (started_tx, started) = oneshot::channel();
            drop(tokio::task::spawn_blocking(move || {
                let _ = started_tx.send(());
                std::thread::sleep(stall_end);
            }));
            let _ = started.await;
        }
    }

    async fn flush_progress(&self, engine: &QuiverEngine) -> Result<(), String> {
        if self.fail_persist {
            return Err("injected progress persist failure".to_owned());
        }
        engine
            .flush_progress()
            .await
            .map(drop)
            .map_err(|e| e.to_string())
    }

    pub(super) fn record(&self, event: &'static str) {
        self.events.lock().expect("events lock").push(event);
    }

    pub(super) fn worker_wait(&self) -> Duration {
        self.worker_wait.unwrap_or(WORKER_WAIT)
    }

    /// Stalls `step` until the returned gate opens.
    pub(super) fn stall(&mut self, step: Step) -> StallGate {
        let gate = StallGate::default();
        self.stall.push((step, gate.clone()));
        gate
    }
}
