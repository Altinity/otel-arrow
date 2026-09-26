// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! File names and object paths of a block's files.

use super::Sink;

use crate::buffer::Block;
use crate::clock::PartitionId;
use crate::schema::Dataset;
use chrono::{DateTime, Utc};
use object_store::path::Path;

/// Identity components of file names.
#[derive(Debug, Clone)]
pub struct FileNaming {
    /// Configured writer id.
    pub writer_id: String,
    /// Random id of this process incarnation.
    pub boot_id: String,
}

impl FileNaming {
    /// New naming with a fresh UUIDv4 boot id.
    #[must_use]
    pub fn new(writer_id: &str) -> Self {
        Self {
            writer_id: writer_id.to_string(),
            boot_id: uuid::Uuid::new_v4().simple().to_string(),
        }
    }
}

/// `YYYYMMDDTHHMMSSZ` of a Unix timestamp in seconds.
///
/// Negative input is clamped to the epoch, matching
/// [`PartitionId::from_unix_secs`], so a file name and the Hive partition it
/// sits in never disagree about the instant.
fn utc_stamp(unix_secs: i64) -> String {
    DateTime::<Utc>::from_timestamp(unix_secs.max(0), 0).map_or_else(
        || "19700101T000000Z".to_string(),
        |dt| dt.format("%Y%m%dT%H%M%SZ").to_string(),
    )
}

/// Object path of a dataset file (FORMAT.md section 4).
///
/// Built as one [`Path::from_iter`] over path segments, not a single
/// delimiter-joined string: a `/` inside `naming.writer_id` or
/// `naming.boot_id` is then percent-encoded into the file-name segment
/// instead of splitting it into extra directory levels.
#[must_use]
pub fn object_path(
    ds: Dataset,
    partition: PartitionId,
    window_start_secs: i64,
    naming: &FileNaming,
    seq: u64,
) -> Path {
    Path::from_iter([
        "v=1".to_string(),
        format!("signal={}", ds.signal().as_str()),
        format!("dataset={}", ds.name()),
        format!("date={}", partition.date_string()),
        format!("hour={}", partition.hour_string()),
        format!(
            "part-{}-{}-{}-{seq:08}.parquet",
            utc_stamp(window_start_secs),
            naming.writer_id,
            naming.boot_id,
        ),
    ])
}

impl Sink {
    /// Object paths this sink will write for `block`, in write order.
    ///
    /// The names follow from the block's identity (partition, window,
    /// sequence, writer and boot ids), never from the attempt, so a retry
    /// rewrites the same objects and a caller can name them in advance.
    #[must_use]
    pub fn planned_paths(&self, block: &Block) -> Vec<Path> {
        block
            .tables()
            .filter(|table| !table.is_empty())
            .map(|table| {
                object_path(
                    table.dataset(),
                    block.partition,
                    block.window_start_secs,
                    &self.naming,
                    block.seq,
                )
            })
            .collect()
    }
}
