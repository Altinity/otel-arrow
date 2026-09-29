// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Writes a sealed block to an object store as Parquet files, laid out
//! and named as FORMAT.md sections 4 and 5 define.

mod naming;
mod properties;
#[cfg(test)]
mod tests;
mod write;

pub use naming::{FileNaming, object_path};
pub use properties::{
    HIGH_ENTROPY_COLUMNS, compression, native_sorting_columns, row_group_full, writer_properties,
};
use write::{FlushWorkspace, MergeKeys};

use crate::config::LakeConfig;
use crate::schema::Dataset;
use object_store::ObjectStore;
use object_store::path::Path;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::Duration;

/// Files written for one block.
#[derive(Debug, Default)]
pub struct FlushReport {
    /// Dataset, path and row count per file, in write order.
    pub files: Vec<(Dataset, Path, usize)>,
    /// Files whose multipart completion lost its response and that a probe
    /// then found committed by that completion.
    pub probed_commits: usize,
    /// Why a multipart upload of a file found written may be left behind,
    /// each naming its key: the abort that settles it failed.
    pub possible_orphans: Vec<String>,
}

/// What a probe of a block's frozen objects found.
#[derive(Debug, PartialEq, Eq)]
pub enum Presence {
    /// Every object exists.
    All,
    /// Only this many of the objects exist.
    Some(usize),
    /// No object exists.
    None,
    /// A HEAD failed with an error other than `NotFound`, or the deadline
    /// completed first; says which.
    Unknown(String),
}

/// HEAD every path in `paths` until `deadline` completes.
///
/// Objects are atomic on the stores the sink writes to, so a path that exists
/// holds a finished file. The sink settles a lost multipart completion with it
/// and a block's owner probes a failed block's files with it.
pub async fn probe_block(
    store: &dyn ObjectStore,
    paths: &[Path],
    deadline: impl Future<Output = ()>,
) -> Presence {
    use object_store::ObjectStoreExt as _;
    tokio::pin!(deadline);
    let mut present = 0;
    for path in paths {
        let head = tokio::select! {
            biased;
            head = store.head(path) => head,
            () = &mut deadline => {
                return Presence::Unknown("the deadline passed before the probe finished".into());
            }
        };
        match head {
            Ok(_) => present += 1,
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Presence::Unknown(error.to_string()),
        }
    }
    match present {
        0 => Presence::None,
        n if n == paths.len() => Presence::All,
        n => Presence::Some(n),
    }
}

/// Parquet sink.
#[derive(Debug)]
pub struct Sink {
    store: Arc<dyn ObjectStore>,
    cfg: LakeConfig,
    naming: FileNaming,
    abort_timer: StartAbortTimer,
    merge_keys: MergeKeys,
    workspace: FlushWorkspace,
}

/// The cleanup allowance of one failed or cancelled write: completes once
/// `upload.abort_timeout` has passed since it was started.
pub type AbortTimer = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Starts an [`AbortTimer`] of the given length on the caller's clock.
///
/// The sink starts one where it first sees a write fail or be cancelled and
/// bounds the rest of that write's cleanup by it, so the caller's clock, a
/// simulated one included, is the only clock the sink waits on.
pub type StartAbortTimer = fn(Duration) -> AbortTimer;

impl Sink {
    /// New sink, bounding the cleanup of a failed or cancelled write with
    /// timers from `abort_timer`.
    #[must_use]
    pub fn new(
        store: Arc<dyn ObjectStore>,
        cfg: LakeConfig,
        naming: FileNaming,
        abort_timer: StartAbortTimer,
    ) -> Self {
        Self {
            store,
            cfg,
            naming,
            abort_timer,
            merge_keys: MergeKeys::default(),
            workspace: FlushWorkspace::default(),
        }
    }

    /// Heap the merge of the table being written holds for its sort keys.
    ///
    /// Zero between tables and outside a write. A block's owner adds it to
    /// what it accounts for while the block flushes.
    #[must_use]
    pub fn merge_key_bytes(&self) -> usize {
        self.merge_keys.current.load(AtomicOrdering::Relaxed)
    }

    /// The most `merge_key_bytes` any table write of this sink has held.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn merge_key_high_water_bytes(&self) -> usize {
        self.merge_keys.high_water.load(AtomicOrdering::Relaxed)
    }

    /// Heap the table being written holds beside its block and its merge
    /// keys, right now.
    ///
    /// Three terms: the chunk the merge is producing or has just produced,
    /// the Parquet encoder's in-progress row group, and the upload bytes the
    /// store has not acknowledged yet (the buffered part and the parts in
    /// flight; a part is a slice of an encoder buffer and keeps all of it
    /// alive, so each buffer counts whole until its last byte has landed).
    /// Zero between tables and outside a write.
    #[must_use]
    pub fn flush_workspace_bytes(&self) -> usize {
        self.workspace.bytes()
    }

    /// The most `flush_workspace_bytes` seen at any step of any table write
    /// of this sink.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn flush_workspace_high_water_bytes(&self) -> usize {
        let _ = self.workspace.bytes();
        self.workspace.high_water.load(AtomicOrdering::Relaxed)
    }
}
