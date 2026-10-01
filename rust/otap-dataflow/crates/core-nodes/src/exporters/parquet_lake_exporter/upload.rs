// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Idempotent block upload with retries, and `probe_block`.

use std::future::Future;
use std::time::{Duration, Instant};

use futures::FutureExt;
use futures_timer::Delay;
use object_store::{ObjectStore, ObjectStoreExt};

use super::block::{BlockId, EncodedBlock};
use super::error::LakeError;

/// Whether block `id` landed: its values object (written after series) exists and is non-empty.
/// The size check rejects a zero-length `values` object, which the exporter never writes (a block
/// with no rows produces no file) but a crash between the local store's rename and its fsync can
/// leave behind; treating that as landed would let a cross-process probe report lost data as
/// present.
pub async fn probe_block(
    store: &dyn ObjectStore,
    id: &BlockId,
) -> Result<bool, object_store::Error> {
    match store.head(&id.values_path()).await {
        Ok(meta) => Ok(meta.size > 0),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Make a landed block durable on the `file` backend: fsync its files and, on Unix, the
/// directories that name them (up to `base`). The local object store renames a staged file into
/// place without fsync, so an acknowledged block could otherwise vanish on power loss.
pub async fn sync_local(base: &str, block: &EncodedBlock) -> Result<(), LakeError> {
    let base = std::path::PathBuf::from(base);
    let mut files = vec![base.join(block.id.values_path().as_ref())];
    if block.series.is_some() {
        files.push(base.join(block.id.series_path().as_ref()));
    }
    // fsync blocks, so it runs off the core thread; one bounded call per landed block.
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        for f in &files {
            // Open the file writable: on Windows `FlushFileBuffers` (what `sync_all` calls)
            // requires a write handle, so a read-only handle would fail every block's fsync. The
            // file already exists, so no create or truncate; on Unix a write handle fsyncs the
            // same as a read one. Directories below must stay read-only (a directory cannot be
            // opened writable), and their fsync is Unix-only.
            std::fs::OpenOptions::new()
                .write(true)
                .open(f)?
                .sync_all()?;
            #[cfg(unix)]
            {
                let mut dir = f.parent();
                while let Some(d) = dir.filter(|d| d.starts_with(&base)) {
                    std::fs::File::open(d)?.sync_all()?;
                    dir = d.parent();
                }
            }
        }
        Ok(())
    })
    .await
    .map_err(|e| LakeError::Fsync(format!("task failed: {e}")))?
    .map_err(|e| LakeError::Fsync(e.to_string()))
}

/// Await `fut` until `deadline`.
async fn until<T>(
    deadline: Instant,
    fut: impl Future<Output = Result<T, object_store::Error>>,
) -> Result<T, LakeError> {
    let fut = fut.fuse();
    let timeout = Delay::new(deadline.saturating_duration_since(Instant::now())).fuse();
    futures::pin_mut!(fut, timeout);
    futures::select_biased! {
        r = fut => Ok(r?),
        _ = timeout => Err(LakeError::Deadline),
    }
}

/// Await a `LakeError` future until `deadline`, mapping a timeout to [`LakeError::Deadline`]. Used
/// to bound the fsync phase, which otherwise has no deadline: a hung fsync (a failing disk, an NFS
/// hard mount) would hold the single flush slot forever and freeze admission. On a timeout the
/// block is Nacked; a `spawn_blocking` fsync already in progress keeps running on the blocking
/// pool until it finishes, but it no longer blocks the core thread or the pipeline.
pub async fn until_deadline<T>(
    deadline: Instant,
    fut: impl Future<Output = Result<T, LakeError>>,
) -> Result<T, LakeError> {
    let fut = fut.fuse();
    let timeout = Delay::new(deadline.saturating_duration_since(Instant::now())).fuse();
    futures::pin_mut!(fut, timeout);
    futures::select_biased! {
        r = fut => r,
        _ = timeout => Err(LakeError::Deadline),
    }
}

/// Upload `block` (the series file when it has one, then the values file), retrying the SAME names
/// and bytes with exponential backoff until `deadline`. After a failed attempt, `probe_block`
/// checks whether the block landed anyway. Returns the number of retries on success.
pub async fn upload_block(
    store: &dyn ObjectStore,
    block: &EncodedBlock,
    deadline: Instant,
    initial_backoff: Duration,
    max_backoff: Duration,
) -> Result<u32, LakeError> {
    let mut backoff = initial_backoff;
    let mut retries = 0;
    loop {
        let attempt = async {
            if let Some(series) = &block.series {
                let _ = store.put(&block.id.series_path(), series.clone()).await?;
            }
            let _ = store
                .put(&block.id.values_path(), block.values.clone())
                .await?;
            Ok(())
        };
        let error = match until(deadline, attempt).await {
            Ok(()) => return Ok(retries),
            Err(e) => e,
        };
        // The failure may be ambiguous (the values PUT landed but its response was lost).
        if matches!(
            until(deadline, probe_block(store, &block.id)).await,
            Ok(true)
        ) {
            return Ok(retries);
        }
        if Instant::now() + backoff >= deadline {
            return Err(error);
        }
        Delay::new(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
        retries += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::parquet_lake_exporter::block::{
        Block, EncodeLimits, FileMeta, encode_block,
    };
    use crate::exporters::parquet_lake_exporter::cache::SeriesCache;
    use crate::exporters::parquet_lake_exporter::canonical::Signal;
    use crate::exporters::parquet_lake_exporter::extract::extract_logs;
    use crate::exporters::parquet_lake_exporter::limits::Limits;
    use crate::exporters::parquet_lake_exporter::schema::Schemas;
    use crate::exporters::parquet_lake_exporter::test_fixtures::{logs_request, to_otap};
    use crate::exporters::parquet_lake_exporter::test_store::{Faults, TestStore, list_paths};

    /// A logs block of 8 rows over 2 resources. With `series_committed` the cache already shows
    /// both series as landed, so the block has no series file.
    async fn encoded_with(series_committed: bool) -> EncodedBlock {
        let schemas = Schemas::new();
        let limits = Limits {
            max_extracted_bytes: 1 << 30,
            max_row_bytes: 1 << 30,
            max_nesting_depth: 32,
            max_chunk_bytes: 1 << 30,
        };
        let id = BlockId {
            signal: Signal::Logs,
            window_start_secs: 1_789_960_500,
            writer_id: "w".into(),
            boot_id: "0".repeat(32),
            seq: 1,
        };
        let mut cache = SeriesCache::new(1000);
        let mut block = Block::default();
        let chunks = extract_logs(
            &to_otap(&logs_request(8, 2, 0)),
            &schemas,
            &limits,
            "host.id",
        )
        .expect("extract")
        .chunks;
        for c in chunks {
            if series_committed {
                for series in c.series.ids.clone() {
                    cache.mark_committed(series, id.partition());
                }
            }
            let _ = block.push(c, 1, &mut cache, id.partition()).expect("push");
        }
        let t = block.take();
        let meta = FileMeta {
            emitted_at_micros: 1_789_960_515_000_000,
            window_end_secs: 1_789_960_515,
        };
        encode_block(
            id,
            t.series,
            t.values,
            &schemas,
            meta,
            EncodeLimits::DEFAULT,
        )
        .await
        .expect("encode")
    }

    async fn encoded() -> EncodedBlock {
        encoded_with(false).await
    }

    async fn upload(
        store: &TestStore,
        block: &EncodedBlock,
        deadline_ms: u64,
    ) -> Result<u32, LakeError> {
        let deadline = Instant::now() + Duration::from_millis(deadline_ms);
        let (initial, max) = (Duration::from_millis(5), Duration::from_millis(20));
        upload_block(store, block, deadline, initial, max).await
    }

    /// Scenario: The first two puts of the values object fail, then storage recovers.
    /// Guarantees: Every attempt rewrites the same two object names with byte-identical payloads in single puts, the upload succeeds after 2 retries, and exactly 2 objects exist.
    #[tokio::test]
    async fn retry_writes_same_names_and_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let faults = Faults {
            fail_puts: Faults::first("dataset=values/", 2),
            ..Faults::default()
        };
        let store = TestStore::new(dir.path(), faults.clone());
        let block = encoded().await;
        assert_eq!(upload(&store, &block, 5_000).await.expect("upload"), 2);
        for part in ["dataset=series/", "dataset=values/"] {
            let hashes = faults.put_hashes(part);
            assert_eq!(hashes.len(), 3, "{part}");
            assert!(hashes.iter().all(|h| *h == hashes[0]), "{part}");
        }
        assert_eq!(
            list_paths(&store).await,
            vec![
                block.id.series_path().to_string(),
                block.id.values_path().to_string()
            ]
        );
    }

    /// Scenario: A block is probed before any put, after only the series put, and after the values put, including with an id parsed from its string form.
    /// Guarantees: probe_block reports landed only once the values object exists, so a partial block is never taken as landed.
    #[tokio::test]
    async fn probe_block_false_true_and_partial() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = TestStore::new(dir.path(), Faults::default());
        let block = encoded().await;
        let parsed: BlockId = block.id.to_string().parse().expect("parse");
        assert_eq!(probe_block(&store, &parsed).await.ok(), Some(false));
        let series = block.series.clone().expect("series file");
        let _ = store
            .put(&block.id.series_path(), series)
            .await
            .expect("put");
        assert_eq!(probe_block(&store, &parsed).await.ok(), Some(false));
        let _ = store
            .put(&block.id.values_path(), block.values.clone())
            .await
            .expect("put");
        assert_eq!(probe_block(&store, &parsed).await.ok(), Some(true));
    }

    /// Scenario: Every put fails and the upload deadline is 150 ms.
    /// Guarantees: upload_block gives up near the deadline with the storage error or the deadline error instead of retrying forever, and writes nothing.
    #[tokio::test]
    async fn deadline_exceeded_returns_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let faults = Faults {
            fail_puts: Faults::first("", usize::MAX),
            ..Faults::default()
        };
        let store = TestStore::new(dir.path(), faults);
        let block = encoded().await;
        let started = Instant::now();
        let err = upload(&store, &block, 150).await.expect_err("gives up");
        assert!(
            matches!(err, LakeError::Store(_) | LakeError::Deadline),
            "{err}"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(list_paths(&store).await.is_empty());
    }

    /// Scenario: The values put is stored but its response is lost (an error is returned).
    /// Guarantees: The probe after the ambiguous failure sees the landed block, so the upload succeeds without resending values.
    #[tokio::test]
    async fn probe_after_ambiguous_failure_skips_resend() {
        let dir = tempfile::tempdir().expect("tempdir");
        let faults = Faults {
            ambiguous_puts: Faults::first("dataset=values/", 1),
            ..Faults::default()
        };
        let store = TestStore::new(dir.path(), faults.clone());
        let block = encoded().await;
        assert_eq!(upload(&store, &block, 5_000).await.expect("upload"), 0);
        assert_eq!(faults.put_hashes("dataset=values/").len(), 1);
    }

    /// Scenario: A block whose files are missing is synced, then the block is uploaded and synced again.
    /// Guarantees: sync_local reports an fsync error for missing files, so an unsynced block is never acknowledged, and fsyncs the landed files and their directories successfully.
    #[tokio::test]
    async fn sync_local_fsyncs_landed_files_and_fails_on_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = TestStore::new(dir.path(), Faults::default());
        let block = encoded().await;
        let base = dir.path().to_str().expect("utf8");
        let err = sync_local(base, &block)
            .await
            .expect_err("nothing written yet");
        assert!(matches!(err, LakeError::Fsync(_)), "{err}");
        assert_eq!(upload(&store, &block, 5_000).await.expect("upload"), 0);
        sync_local(base, &block).await.expect("sync");
    }

    /// Scenario: A block encoded without a series file (its series landed earlier) is uploaded and synced.
    /// Guarantees: Exactly one object, the values file, is written in one put; the block counts as landed and fsync succeeds without a series file.
    #[tokio::test]
    async fn block_without_series_uploads_one_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let faults = Faults::default();
        let store = TestStore::new(dir.path(), faults.clone());
        let block = encoded_with(true).await;
        assert!(block.series.is_none());
        assert_eq!(upload(&store, &block, 5_000).await.expect("upload"), 0);
        assert_eq!(
            list_paths(&store).await,
            vec![block.id.values_path().to_string()]
        );
        assert_eq!(faults.put_hashes("").len(), 1);
        assert_eq!(probe_block(&store, &block.id).await.ok(), Some(true));
        let base = dir.path().to_str().expect("utf8");
        sync_local(base, &block).await.expect("sync");
    }
}
