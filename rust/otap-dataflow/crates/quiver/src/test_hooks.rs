// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Hooks that let tests of dependent crates stall Quiver's blocking paths.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

type Gate = Arc<(Mutex<bool>, Condvar)>;

/// The directories one kind of stall currently holds.
type Stalls = Mutex<Vec<(PathBuf, Gate)>>;

static WAL_DROP_STALLS: Stalls = Mutex::new(Vec::new());
static SEGMENT_WRITE_STALLS: Stalls = Mutex::new(Vec::new());

/// A stall of one kind over every file under a directory, until released.
struct DirStall {
    stalls: &'static Stalls,
    dir: PathBuf,
    gate: Gate,
}

impl DirStall {
    fn new(stalls: &'static Stalls, dir: PathBuf) -> Self {
        let gate: Gate = Arc::new((Mutex::new(false), Condvar::new()));
        stalls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push((dir.clone(), Arc::clone(&gate)));
        Self { stalls, dir, gate }
    }

    fn release(&self) {
        let (released, wake) = &*self.gate;
        *released
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        wake.notify_all();
    }
}

impl Drop for DirStall {
    fn drop(&mut self) {
        self.release();
        self.stalls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|(dir, _)| dir != &self.dir);
    }
}

/// Blocks while a stall in `stalls` holds a directory containing `path`.
fn wait(stalls: &Stalls, path: &Path) {
    let gate = stalls
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .find(|(dir, _)| path.starts_with(dir))
        .map(|(_, gate)| Arc::clone(gate));
    if let Some(gate) = gate {
        let (released, wake) = &*gate;
        let mut released = released
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !*released {
            released = wake
                .wait(released)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

/// Holds the drop of every WAL writer whose file lies under a directory,
/// as a wedged disk would hold its drop-time sync, until released.
pub struct WalDropStall(DirStall);

impl WalDropStall {
    /// Stalls the drop of WAL writers under `dir` from now on.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self(DirStall::new(&WAL_DROP_STALLS, dir.into()))
    }

    /// Lets every stalled and every later drop under the directory finish.
    pub fn release(&self) {
        self.0.release();
    }
}

/// Holds every segment write under a directory on its blocking thread, after
/// the file's bytes are written and before it is synced, until released.
pub struct SegmentWriteStall(DirStall);

impl SegmentWriteStall {
    /// Stalls segment writes under `dir` from now on.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self(DirStall::new(&SEGMENT_WRITE_STALLS, dir.into()))
    }

    /// Lets every stalled and every later write under the directory finish.
    pub fn release(&self) {
        self.0.release();
    }
}

/// Blocks while a [`WalDropStall`] holds a directory containing `path`.
pub(crate) fn before_wal_drop(path: &Path) {
    wait(&WAL_DROP_STALLS, path);
}

/// Blocks while a [`SegmentWriteStall`] holds a directory containing `path`.
pub(crate) fn before_segment_sync(path: &Path) {
    wait(&SEGMENT_WRITE_STALLS, path);
}
