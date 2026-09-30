// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Test-only object store with fault injection on puts: failing, ambiguous (stored, then an error),
//! panicking and gated puts, plus a record of every attempted put. Wraps `LocalFileSystem`.

use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
};
use tokio::sync::Semaphore;
use xxhash_rust::xxh3::xxh3_128;

/// Puts to paths containing `.0` are affected while the counter `.1` is above zero (one per put).
pub type PutFault = Option<(&'static str, Arc<AtomicUsize>)>;

/// Shared fault switches and counters; clone freely (all fields are shared handles).
#[derive(Clone, Debug, Default)]
pub struct Faults {
    /// Fail matching puts without storing anything.
    pub fail_puts: PutFault,
    /// Store matching puts, then return an error (the response was "lost").
    pub ambiguous_puts: PutFault,
    /// Panic inside matching puts (the flush task dies).
    pub panic_puts: PutFault,
    /// When set, every put waits for one permit (`add_permits` releases puts).
    pub commit_gate: Option<Arc<Semaphore>>,
    /// When set, `commit_gate` applies only to paths containing this string.
    pub commit_gate_prefix: Option<&'static str>,
    /// (path, xxh3_128 of the payload) of every attempted put, in order.
    pub puts: Arc<Mutex<Vec<(String, u128)>>>,
}

impl Faults {
    /// Faults with a closed gate on puts to paths containing `prefix`.
    #[must_use]
    pub fn gated(prefix: &'static str) -> Self {
        Self {
            commit_gate: Some(Arc::new(Semaphore::new(0))),
            commit_gate_prefix: Some(prefix),
            ..Self::default()
        }
    }

    /// A fault for the first `n` puts to paths containing `prefix`.
    #[must_use]
    pub fn first(prefix: &'static str, n: usize) -> PutFault {
        Some((prefix, Arc::new(AtomicUsize::new(n))))
    }

    /// Recorded payload hashes of the puts to paths containing `part`.
    #[must_use]
    pub fn put_hashes(&self, part: &str) -> Vec<u128> {
        self.puts
            .lock()
            .expect("puts lock")
            .iter()
            .filter(|(p, _)| p.contains(part))
            .map(|(_, h)| *h)
            .collect()
    }
}

/// Consume one unit of `fault` if it applies to `location`.
fn hit(fault: &PutFault, location: &Path) -> bool {
    match fault {
        Some((prefix, left)) if location.as_ref().contains(prefix) => left
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok(),
        _ => false,
    }
}

fn injected(what: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "test_store",
        source: Box::new(std::io::Error::other(format!("injected {what} failure"))),
    }
}

/// Fault-injecting store over a local directory.
#[derive(Debug)]
pub struct TestStore {
    inner: LocalFileSystem,
    faults: Faults,
}

impl TestStore {
    /// Store rooted at `dir` with the given faults.
    #[must_use]
    pub fn new(dir: &std::path::Path, faults: Faults) -> Self {
        Self {
            inner: LocalFileSystem::new_with_prefix(dir).expect("local store"),
            faults,
        }
    }
}

impl fmt::Display for TestStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TestStore")
    }
}

#[async_trait]
impl ObjectStore for TestStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let mut bytes = Vec::with_capacity(payload.content_length());
        payload.iter().for_each(|b| bytes.extend_from_slice(b));
        self.faults
            .puts
            .lock()
            .expect("puts lock")
            .push((location.to_string(), xxh3_128(&bytes)));
        let gate = match self.faults.commit_gate_prefix {
            Some(prefix) if !location.as_ref().contains(prefix) => None,
            _ => self.faults.commit_gate.clone(),
        };
        if let Some(gate) = gate {
            gate.acquire().await.expect("gate open").forget();
        }
        if hit(&self.faults.panic_puts, location) {
            panic!("injected put panic");
        }
        if hit(&self.faults.fail_puts, location) {
            return Err(injected("put"));
        }
        let r = self.inner.put_opts(location, payload, opts).await?;
        if hit(&self.faults.ambiguous_puts, location) {
            return Err(injected("ambiguous put"));
        }
        Ok(r)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// All object paths under the store root, sorted.
pub async fn list_paths(store: &dyn ObjectStore) -> Vec<String> {
    use futures::StreamExt;
    let mut out: Vec<String> = store
        .list(None)
        .map(|m| m.expect("list").location.to_string())
        .collect()
        .await;
    out.sort();
    out
}
