// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Node telemetry of the Parquet lake exporter.

use otel_arrow_dfe_telemetry::instrument::{Counter, Mmsc};
use otel_arrow_dfe_telemetry_macros::metric_set;

/// `exporter.parquet_lake` metric set.
#[metric_set(name = "exporter.parquet_lake")]
#[derive(Debug, Default, Clone)]
pub struct LakeMetrics {
    /// Blocks that landed.
    #[metric(name = "blocks.landed", unit = "{block}")]
    pub blocks_landed: Counter<u64>,
    /// Blocks given up (encode failure or upload deadline).
    #[metric(name = "blocks.failed", unit = "{block}")]
    pub blocks_failed: Counter<u64>,
    /// Upload retries.
    #[metric(name = "upload.retries", unit = "{retry}")]
    pub upload_retries: Counter<u64>,
    /// Bytes of landed files.
    #[metric(name = "bytes.written", unit = "By")]
    pub bytes_written: Counter<u64>,
    /// Values rows in landed blocks.
    #[metric(name = "rows.written", unit = "{row}")]
    pub rows_written: Counter<u64>,
    /// Series rows in landed blocks.
    #[metric(name = "series_rows.written", unit = "{row}")]
    pub series_rows_written: Counter<u64>,
    /// Encode + upload time per block.
    #[metric(name = "flush.duration", unit = "s")]
    pub flush_duration: Mmsc,
    /// Peak encoder memory (`ArrowWriter::memory_size`) per encoded block; checks the
    /// ENCODER_BYTES assumption.
    #[metric(name = "encoder.peak", unit = "By")]
    pub encoder_peak: Mmsc,
    /// Data points whose parent metric row is missing (kept under an empty metric identity).
    #[metric(name = "points.orphaned", unit = "{point}")]
    pub points_orphaned: Counter<u64>,
    /// Batches refused as unsupported or invalid (permanent).
    #[metric(name = "batches.rejected", unit = "{batch}")]
    pub batches_rejected: Counter<u64>,
}
