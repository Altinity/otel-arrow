// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Idempotent block upload with retries, and `probe_block`.

use std::future::Future;
use std::time::{Duration, Instant};

use futures::FutureExt;
use futures_timer::Delay;
use object_store::{ObjectStore, ObjectStoreExt};

use super::block::{BlockId, EncodedBlock};

/// Whether block `id` landed: its values object (written after series) exists.
pub async fn probe_block(
    store: &dyn ObjectStore,
    id: &BlockId,
) -> Result<bool, object_store::Error> {
    match store.head(&id.values_path()).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Make a landed block durable on the `file` backend: fsync both files and, on Unix, the
/// directories that name them (up to `base`). The local object store renames a staged file into
/// place without fsync, so an acknowledged block could otherwise vanish on power loss.
pub async fn sync_local(base: &str, id: &BlockId) -> Result<(), String> {
    let base = std::path::PathBuf::from(base);
    let files = [id.series_path(), id.values_path()].map(|p| base.join(p.as_ref()));
    // fsync blocks, so it runs off the core thread; one bounded call per landed block (serial).
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        for f in &files {
            std::fs::File::open(f)?.sync_all()?;
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
    .map_err(|e| format!("fsync task failed: {e}"))?
    .map_err(|e| format!("fsync failed: {e}"))
}

/// Await `fut` until `deadline`.
async fn until<T>(
    deadline: Instant,
    fut: impl Future<Output = Result<T, object_store::Error>>,
) -> Result<T, String> {
    let fut = fut.fuse();
    let timeout = Delay::new(deadline.saturating_duration_since(Instant::now())).fuse();
    futures::pin_mut!(fut, timeout);
    futures::select_biased! {
        r = fut => r.map_err(|e| e.to_string()),
        _ = timeout => Err("upload deadline exceeded".to_owned()),
    }
}

/// Upload `block` (series, then values), retrying the SAME names and bytes with exponential
/// backoff until `deadline`. After a failed attempt, `probe_block` checks whether the block landed
/// anyway. Returns the number of retries on success.
pub async fn upload_block(
    store: &dyn ObjectStore,
    block: &EncodedBlock,
    deadline: Instant,
    initial_backoff: Duration,
    max_backoff: Duration,
) -> Result<u32, String> {
    let mut backoff = initial_backoff;
    let mut retries = 0;
    loop {
        let attempt = async {
            let _ = store
                .put(&block.id.series_path(), block.series.clone())
                .await?;
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
        if until(deadline, probe_block(store, &block.id)).await == Ok(true) {
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
    use std::time::SystemTime;

    use super::*;
    use crate::exporters::parquet_lake_exporter::block::{Block, EncodeLimits, encode_block};
    use crate::exporters::parquet_lake_exporter::extract::extract_logs;
    use crate::exporters::parquet_lake_exporter::identity::Signal;
    use crate::exporters::parquet_lake_exporter::schema::Schemas;
    use crate::exporters::parquet_lake_exporter::test_fixtures::{logs_request, to_otap};
    use crate::exporters::parquet_lake_exporter::test_store::{Faults, TestStore, list_paths};

    async fn encoded() -> EncodedBlock {
        let schemas = Schemas::new();
        let mut block = Block::default();
        for c in extract_logs(&to_otap(&logs_request(8, 2, 0)), &schemas, 1 << 30).expect("x") {
            let _ = block.push(c, 1, Instant::now()).expect("push");
        }
        let t = block.take();
        let id = BlockId {
            signal: Signal::Logs,
            date: "2026-09-30".into(),
            writer: "c0-0000000000000001".into(),
            seq: 1,
        };
        encode_block(
            id,
            t.series,
            t.values,
            &schemas,
            SystemTime::now(),
            EncodeLimits::DEFAULT,
        )
        .await
        .expect("encode")
    }

    async fn upload(
        store: &TestStore,
        block: &EncodedBlock,
        deadline_ms: u64,
    ) -> Result<u32, String> {
        let deadline = Instant::now() + Duration::from_millis(deadline_ms);
        let (initial, max) = (Duration::from_millis(5), Duration::from_millis(20));
        upload_block(store, block, deadline, initial, max).await
    }

    /// Scenario: The first two puts of the values object fail, then storage recovers.
    /// Guarantees: Every attempt rewrites the same two object names with byte-identical payloads, the upload succeeds after 2 retries, and exactly 2 objects exist.
    #[tokio::test]
    async fn retry_writes_same_names_and_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let faults = Faults {
            fail_puts: Faults::first("values/", 2),
            ..Faults::default()
        };
        let store = TestStore::new(dir.path(), faults.clone());
        let block = encoded().await;
        assert_eq!(upload(&store, &block, 5_000).await, Ok(2));
        for part in ["series/", "values/"] {
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
        let _ = store
            .put(&block.id.series_path(), block.series.clone())
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
    /// Guarantees: upload_block gives up with an error near the deadline instead of retrying forever, and writes nothing.
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
        assert!(upload(&store, &block, 150).await.is_err());
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(list_paths(&store).await.is_empty());
    }

    /// Scenario: The values put is stored but its response is lost (an error is returned).
    /// Guarantees: The probe after the ambiguous failure sees the landed block, so the upload succeeds without resending values.
    #[tokio::test]
    async fn probe_after_ambiguous_failure_skips_resend() {
        let dir = tempfile::tempdir().expect("tempdir");
        let faults = Faults {
            ambiguous_puts: Faults::first("values/", 1),
            ..Faults::default()
        };
        let store = TestStore::new(dir.path(), faults.clone());
        let block = encoded().await;
        assert_eq!(upload(&store, &block, 5_000).await, Ok(0));
        assert_eq!(faults.put_hashes("values/").len(), 1);
    }

    /// Scenario: A landed block's two files exist under a base directory, then a block whose files are missing is synced.
    /// Guarantees: sync_local fsyncs existing files (and their directories) successfully and reports an error for missing files, so an unsynced block is never acknowledged.
    #[tokio::test]
    async fn sync_local_fsyncs_landed_files_and_fails_on_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = TestStore::new(dir.path(), Faults::default());
        let block = encoded().await;
        let base = dir.path().to_str().expect("utf8");
        assert!(
            sync_local(base, &block.id).await.is_err(),
            "nothing written yet"
        );
        assert_eq!(upload(&store, &block, 5_000).await, Ok(0));
        assert_eq!(sync_local(base, &block.id).await, Ok(()));
    }
}
