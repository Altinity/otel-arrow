// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The flush of one sealed block: sort and encode, upload with retries, fsync on the file
//! backend. Each block runs in its own local task on the node's thread; the node loop awaits the
//! task next to its inbox, so an upload never stops admission.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use object_store::ObjectStore;
use tokio::task::{JoinError, JoinHandle};

use super::block::{BlockId, EncodeLimits, FileMeta, encode_block};
use super::error::LakeError;
use super::schema::Schemas;
use super::upload::{sync_local, upload_block};

/// What a landed block wrote.
#[derive(Clone, Copy, Debug)]
pub struct Landed {
    /// Upload retries.
    pub retries: u32,
    /// Bytes of the landed files.
    pub bytes: u64,
    /// Values rows.
    pub rows: u64,
    /// Series rows.
    pub series_rows: u64,
    /// Largest encoder memory observed.
    pub encoder_peak: usize,
    /// Largest gathered slice.
    pub slice_peak: usize,
}

/// A running flush task. Dropping it aborts the task, so an error exit of the node loop (a failed
/// `notify`, a closed inbox) never leaves a detached encode or upload running on the core thread.
/// Aborting a finished task does nothing.
pub struct FlushTask(JoinHandle<Result<Landed, LakeError>>);

impl FlushTask {
    /// Wait for the task. A panic or an abort is reported as `JoinError`. Cancel-safe: the task
    /// keeps running when this future is dropped, and can be awaited again.
    pub async fn join(&mut self) -> Result<Result<Landed, LakeError>, JoinError> {
        (&mut self.0).await
    }
}

impl Drop for FlushTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Everything a flush task needs besides the block. Cloned into each task; the store is the
/// shared object-store client (an `Arc` by design of the `object_store` crate).
#[derive(Clone)]
pub struct Uploader {
    /// Destination.
    pub store: Arc<dyn ObjectStore>,
    /// Dataset schemas.
    pub schemas: Schemas,
    /// Base directory of the `file` backend (fsync after landing); `None` for object stores.
    pub local_base: Option<String>,
    /// First retry backoff.
    pub initial_backoff: Duration,
    /// Largest retry backoff.
    pub max_backoff: Duration,
    /// Encoder limits.
    pub limits: EncodeLimits,
}

impl Uploader {
    /// Flush one block in a local task. The task owns the rows; it returns what landed, or the
    /// error that kept the block from landing.
    pub fn spawn(
        &self,
        id: BlockId,
        series: Vec<RecordBatch>,
        values: Vec<RecordBatch>,
        meta: FileMeta,
        deadline: Instant,
    ) -> FlushTask {
        let uploader = self.clone();
        // Local task: `!Send`, stays on this core's LocalSet; aborted when the `FlushTask` drops.
        FlushTask(tokio::task::spawn_local(async move {
            uploader.flush(id, series, values, meta, deadline).await
        }))
    }

    async fn flush(
        &self,
        id: BlockId,
        series: Vec<RecordBatch>,
        values: Vec<RecordBatch>,
        meta: FileMeta,
        deadline: Instant,
    ) -> Result<Landed, LakeError> {
        if Instant::now() >= deadline {
            return Err(LakeError::Deadline);
        }
        let block = encode_block(id, series, values, &self.schemas, meta, self.limits).await?;
        let retries = upload_block(
            self.store.as_ref(),
            &block,
            deadline,
            self.initial_backoff,
            self.max_backoff,
        )
        .await?;
        if let Some(base) = &self.local_base {
            sync_local(base, &block).await?;
        }
        let series_bytes = block.series.as_ref().map_or(0, |p| p.content_length());
        Ok(Landed {
            retries,
            bytes: (series_bytes + block.values.content_length()) as u64,
            rows: block.rows as u64,
            series_rows: block.series_rows as u64,
            encoder_peak: block.encoder_peak,
            slice_peak: block.slice_peak,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::parquet_lake_exporter::block::Block;
    use crate::exporters::parquet_lake_exporter::cache::SeriesCache;
    use crate::exporters::parquet_lake_exporter::canonical::Signal;
    use crate::exporters::parquet_lake_exporter::extract::extract_logs;
    use crate::exporters::parquet_lake_exporter::limits::Limits;
    use crate::exporters::parquet_lake_exporter::test_fixtures::{logs_request, to_otap};
    use crate::exporters::parquet_lake_exporter::test_store::{Faults, TestStore, list_paths};
    use tokio::task::LocalSet;

    const META: FileMeta = FileMeta {
        emitted_at_micros: 1_789_960_515_000_000,
        window_end_secs: 1_789_960_515,
    };

    fn id(seq: u64) -> BlockId {
        BlockId {
            signal: Signal::Logs,
            window_start_secs: 1_789_960_500,
            writer_id: "w".into(),
            boot_id: "0".repeat(32),
            seq,
        }
    }

    /// Series and values rows of a logs block of 8 rows over 2 resources.
    fn rows() -> (Vec<RecordBatch>, Vec<RecordBatch>) {
        let limits = Limits {
            max_extracted_bytes: 1 << 30,
            max_row_bytes: 1 << 30,
            max_nesting_depth: 32,
            max_chunk_bytes: 1 << 30,
        };
        let mut block = Block::default();
        let mut cache = SeriesCache::new(1000);
        let chunks = extract_logs(
            &to_otap(&logs_request(8, 2, 0)),
            &Schemas::new(),
            &limits,
            "host.id",
        )
        .expect("extract")
        .chunks;
        for c in chunks {
            let _ = block
                .push(c, 1, &mut cache, id(1).partition())
                .expect("push");
        }
        let t = block.take();
        (t.series, t.values)
    }

    fn uploader(store: Arc<TestStore>, local_base: Option<String>) -> Uploader {
        Uploader {
            store,
            schemas: Schemas::new(),
            local_base,
            initial_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(20),
            limits: EncodeLimits::DEFAULT,
        }
    }

    /// Scenario: A logs block is flushed in a local task against a local store with fsync; a second block is spawned with a deadline that has already passed.
    /// Guarantees: The first task reports what landed (8 values rows, 2 series rows, the bytes of both files, no retry) and both objects exist; the second resolves to the deadline error and writes nothing.
    #[tokio::test]
    async fn spawned_flush_lands_and_reports() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(TestStore::new(dir.path(), Faults::default()));
        let base = dir.path().to_str().expect("utf8").to_owned();
        let up = uploader(store.clone(), Some(base));
        LocalSet::new()
            .run_until(async {
                let (series, values) = rows();
                let deadline = Instant::now() + Duration::from_secs(5);
                let mut task = up.spawn(id(1), series, values, META, deadline);
                let landed = task.join().await.expect("task").expect("landed");
                assert_eq!((landed.rows, landed.series_rows, landed.retries), (8, 2, 0));
                assert!(landed.bytes > 0 && landed.encoder_peak > 0 && landed.slice_peak > 0);
                let paths = list_paths(store.as_ref()).await;
                assert_eq!(
                    paths,
                    vec![
                        id(1).series_path().to_string(),
                        id(1).values_path().to_string()
                    ]
                );
                let on_disk: u64 = paths
                    .iter()
                    .map(|p| std::fs::metadata(dir.path().join(p)).expect("file").len())
                    .sum();
                assert_eq!(landed.bytes, on_disk);

                let (series, values) = rows();
                let past = Instant::now() - Duration::from_millis(1);
                let mut task = up.spawn(id(2), series, values, META, past);
                let err = task.join().await.expect("task").expect_err("deadline");
                assert!(matches!(err, LakeError::Deadline), "{err}");
                assert_eq!(list_paths(store.as_ref()).await.len(), 2);
            })
            .await;
    }

    /// Scenario: A block is flushed against a store whose values put waits at a closed gate; once the put is waiting, the task handle is dropped and the gate is opened.
    /// Guarantees: The task is aborted at its await point instead of being detached: the values object is never written, although the gate no longer holds it back.
    #[tokio::test]
    async fn dropping_the_task_aborts_the_upload() {
        let dir = tempfile::tempdir().expect("tempdir");
        let faults = Faults::gated("dataset=values/");
        let store = Arc::new(TestStore::new(dir.path(), faults.clone()));
        let up = uploader(store.clone(), None);
        LocalSet::new()
            .run_until(async {
                let (series, values) = rows();
                let deadline = Instant::now() + Duration::from_secs(5);
                let task = up.spawn(id(1), series, values, META, deadline);
                // The series file lands and the values put reaches the gate.
                let waiting = Instant::now() + Duration::from_secs(5);
                while faults.put_hashes("dataset=values/").is_empty() {
                    assert!(Instant::now() < waiting, "the values put never started");
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                drop(task);
                faults.commit_gate.as_ref().expect("gate").add_permits(16);
                tokio::time::sleep(Duration::from_millis(100)).await;
                assert_eq!(
                    list_paths(store.as_ref()).await,
                    vec![id(1).series_path().to_string()]
                );
                assert_eq!(faults.put_hashes("dataset=values/").len(), 1);
            })
            .await;
    }
}
