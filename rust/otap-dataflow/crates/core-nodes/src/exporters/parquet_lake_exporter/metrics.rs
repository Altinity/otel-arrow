// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Node telemetry of the Parquet lake exporter.

use otel_arrow_dfe_telemetry::instrument::{Counter, Gauge, Mmsc};
use otel_arrow_dfe_telemetry_macros::metric_set;

/// `exporter.parquet_lake` metric set.
#[metric_set(name = "exporter.parquet_lake")]
#[derive(Debug, Default, Clone)]
pub struct LakeMetrics {
    /// Blocks that landed.
    #[metric(name = "blocks.landed", unit = "{block}")]
    pub blocks_landed: Counter<u64>,
    /// Blocks given up (encode failure, flush deadline, task failure, shutdown).
    #[metric(name = "blocks.failed", unit = "{block}")]
    pub blocks_failed: Counter<u64>,
    /// Upload retries of landed blocks.
    #[metric(name = "upload.retries", unit = "{retry}")]
    pub upload_retries: Counter<u64>,
    /// Bytes of landed files.
    #[metric(name = "bytes.written", unit = "By")]
    pub bytes_written: Counter<u64>,
    /// Values rows in landed blocks.
    #[metric(name = "rows.written", unit = "{row}")]
    pub rows_written: Counter<u64>,
    /// Series rows in landed blocks (new series, new partition, or re-emitted after a failure
    /// or an eviction).
    #[metric(name = "series_rows.written", unit = "{row}")]
    pub series_rows_written: Counter<u64>,
    /// Sort, encode and upload time per block.
    #[metric(name = "flush.duration", unit = "s")]
    pub flush_duration: Mmsc,
    /// Peak encoder memory (`ArrowWriter::memory_size`) per encoded block.
    #[metric(name = "encoder.peak", unit = "By")]
    pub encoder_peak: Mmsc,
    /// Largest sorted slice gathered into the encoder per encoded block.
    #[metric(name = "slice.peak", unit = "By")]
    pub slice_peak: Mmsc,
    /// Generations rotated because their window ended.
    #[metric(name = "flushes.time", unit = "{flush}")]
    pub flushes_time: Counter<u64>,
    /// Generations rotated because they reached `window.max_block_bytes`.
    #[metric(name = "flushes.bytes", unit = "{flush}")]
    pub flushes_bytes: Counter<u64>,
    /// Generations rotated by a shutdown.
    #[metric(name = "flushes.shutdown", unit = "{flush}")]
    pub flushes_shutdown: Counter<u64>,
    /// Series ids the cache holds.
    #[metric(name = "series_cache.entries", unit = "{entry}")]
    pub cache_entries: Gauge<u64>,
    /// Cache lookups that found the series committed in the block's partition.
    #[metric(name = "series_cache.hits", unit = "{lookup}")]
    pub cache_hits: Counter<u64>,
    /// Cache lookups that did not.
    #[metric(name = "series_cache.misses", unit = "{lookup}")]
    pub cache_misses: Counter<u64>,
    /// Cache entries dropped at the size bound.
    #[metric(name = "series_cache.evictions", unit = "{entry}")]
    pub cache_evictions: Counter<u64>,
    /// 1 while the exporter reads no pdata (ACTIVE is full or due and the flush slot is busy).
    #[metric(name = "admission.closed", unit = "{state}")]
    pub admission_closed: Gauge<u64>,
    /// Length of each period with admission closed.
    #[metric(name = "admission.closed.duration", unit = "s")]
    pub admission_closed_duration: Mmsc,
    /// Requests refused because a size limit was exceeded.
    #[metric(name = "requests.refused.too_large", unit = "{request}")]
    pub refused_too_large: Counter<u64>,
    /// Requests refused for invalid content.
    #[metric(name = "requests.refused.invalid", unit = "{request}")]
    pub refused_invalid: Counter<u64>,
    /// Requests refused for nesting beyond `ingress.max_nesting_depth`.
    #[metric(name = "requests.refused.too_deep", unit = "{request}")]
    pub refused_too_deep: Counter<u64>,
    /// Requests refused as unsupported (traces).
    #[metric(name = "requests.refused.unsupported", unit = "{request}")]
    pub refused_unsupported: Counter<u64>,
    /// Requests refused for a conversion or internal failure.
    #[metric(name = "requests.refused.other", unit = "{request}")]
    pub refused_other: Counter<u64>,
    /// Timestamps stored as null because they were negative.
    #[metric(name = "timestamps.out_of_range", unit = "{timestamp}")]
    pub timestamps_out_of_range: Counter<u64>,
}
