// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration of the Parquet lake exporter.

use std::time::Duration;

use otel_arrow_dfe_otap::object_store::{RetryOptions, StorageType};
use serde::Deserialize;

use super::limits::Limits;

/// Flush the encoder's row group once `ArrowWriter::memory_size` reaches this.
pub const ROW_GROUP_BYTES: usize = 8 * 1024 * 1024;
/// Input bytes handed to the encoder per write; bounds the overshoot past ROW_GROUP_BYTES and the
/// time between yields.
pub const SLICE_BYTES: usize = 1024 * 1024;
/// Encoder memory bound with the default slice: the ROW_GROUP_BYTES cap plus one row group of
/// overshoot and page buffers (measured in tests). `worst_case_flush_bytes` widens it when
/// `ingress.max_row_bytes` lets one row exceed ROW_GROUP_BYTES.
pub const ENCODER_BYTES: usize = 2 * ROW_GROUP_BYTES;
/// Allocation unit of encoded output; each output file wastes at most one unit of capacity.
pub const OUTPUT_BLOCK_BYTES: usize = 1024 * 1024;
/// Smallest accepted `window.max_block_bytes`.
pub const MIN_BLOCK_BYTES: usize = 1024 * 1024;
/// Largest accepted `window.max_block_bytes` (keeps the bound arithmetic and Arrow i32 offsets safe).
pub const MAX_BLOCK_BYTES: usize = 1024 * 1024 * 1024;
/// Memory of one series cache entry (key, partition, LRU links, table slot).
pub const CACHE_ENTRY_BYTES: usize = 128;
/// Memory of one sort key (series id, time, batch and row index).
pub const SORT_KEY_BYTES: usize = 32;
/// Memory of one row weight (the row's bytes as `u32`) kept next to the sort keys while a block
/// is encoded.
pub const ROW_WEIGHT_BYTES: usize = 4;
/// Lower bound of the Arrow memory of one values row; bounds the number of sort keys of a block
/// (checked by a test in `extract.rs`).
pub const MIN_VALUES_ROW_BYTES: usize = 96;
/// Largest accepted `ingress.max_nesting_depth`.
pub const MAX_NESTING_DEPTH: usize = 256;
/// Largest accepted `series_cache.max_entries` (2 GiB of cache at CACHE_ENTRY_BYTES each).
pub const MAX_CACHE_ENTRIES: usize = 16 * 1024 * 1024;
/// Longest accepted `window.interval`: a block belongs to the hour of its window start.
const MAX_INTERVAL: Duration = Duration::from_secs(3600);
/// Longest accepted `writer_id`. Object names embed it alongside a ~73-byte fixed part; keeping it
/// well under the common 255-byte filesystem `NAME_MAX` avoids `ENAMETOOLONG` on the file backend.
const MAX_WRITER_ID_LEN: usize = 100;

/// Window rotation and the budget of one block.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct WindowConfig {
    /// Aligned rotation interval; whole seconds, 1s to 1h.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Buffered Arrow bytes of one generation (logs plus metrics).
    pub max_block_bytes: usize,
    /// Give up on a generation (Nack its requests) when it has not landed this long after its
    /// flush started.
    #[serde(with = "humantime_serde")]
    pub flush_retry_deadline: Duration,
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(15),
            max_block_bytes: 64 * 1024 * 1024,
            flush_retry_deadline: Duration::from_secs(60),
        }
    }
}

/// Request limits.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct IngressConfig {
    /// Largest request as received (OTLP bytes or OTAP Arrow memory).
    pub max_request_bytes: usize,
    /// Largest extracted output of one request.
    pub max_extracted_bytes: usize,
    /// Largest values or series row, and largest attribute or body cell.
    pub max_row_bytes: usize,
    /// Deepest nesting of arrays and key/value lists in one value.
    pub max_nesting_depth: usize,
}

impl Default for IngressConfig {
    fn default() -> Self {
        Self {
            max_request_bytes: 16 * 1024 * 1024,
            max_extracted_bytes: 32 * 1024 * 1024,
            max_row_bytes: 1024 * 1024,
            max_nesting_depth: 32,
        }
    }
}

/// Series cache.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct SeriesCacheConfig {
    /// Series ids remembered with the partition of their last landed series row.
    pub max_entries: usize,
}

impl Default for SeriesCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 200_000,
        }
    }
}

/// Exporter configuration.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LakeConfig {
    /// Object-store backend (file, s3, azure).
    pub storage: StorageType,
    /// Per-request object-store retry policy.
    #[serde(default)]
    pub retry: Option<RetryOptions>,
    /// Writer id in object names and footers; `[A-Za-z0-9_.-]+`.
    #[serde(default = "default_writer_id")]
    pub writer_id: String,
    /// Resource attribute projected into the `producer_id` column.
    #[serde(default = "default_producer_id_attribute")]
    pub producer_id_attribute: String,
    /// Window rotation and block budget.
    #[serde(default)]
    pub window: WindowConfig,
    /// Request limits.
    #[serde(default)]
    pub ingress: IngressConfig,
    /// Series cache.
    #[serde(default)]
    pub series_cache: SeriesCacheConfig,
    /// First retry backoff; doubles per attempt up to `retry_max_backoff`.
    #[serde(default = "default_retry_initial_backoff", with = "humantime_serde")]
    pub retry_initial_backoff: Duration,
    /// Largest retry backoff.
    #[serde(default = "default_retry_max_backoff", with = "humantime_serde")]
    pub retry_max_backoff: Duration,
}

fn default_writer_id() -> String {
    "writer".to_owned()
}
fn default_producer_id_attribute() -> String {
    "host.id".to_owned()
}
const fn default_retry_initial_backoff() -> Duration {
    Duration::from_millis(200)
}
const fn default_retry_max_backoff() -> Duration {
    Duration::from_secs(10)
}

impl LakeConfig {
    /// Deserialize and validate (used by `validate_config` and node creation).
    pub fn parse(value: &serde_json::Value) -> Result<Self, otel_arrow_dfe_config::error::Error> {
        let config: Self = serde_json::from_value(value.clone()).map_err(|e| {
            otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                error: e.to_string(),
            }
        })?;
        config
            .validate()
            .map_err(|error| otel_arrow_dfe_config::error::Error::InvalidUserConfig { error })?;
        Ok(config)
    }

    /// Semantic validation.
    pub fn validate(&self) -> Result<(), String> {
        let id_char = |c: char| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-');
        if self.writer_id.is_empty() || !self.writer_id.chars().all(id_char) {
            return Err("writer_id must be non-empty and made of [A-Za-z0-9_.-]".into());
        }
        if self.writer_id.len() > MAX_WRITER_ID_LEN {
            return Err(format!(
                "writer_id must be at most {MAX_WRITER_ID_LEN} characters"
            ));
        }
        if self.producer_id_attribute.is_empty() {
            return Err("producer_id_attribute must not be empty".into());
        }
        let w = &self.window;
        if !(MIN_BLOCK_BYTES..=MAX_BLOCK_BYTES).contains(&w.max_block_bytes) {
            return Err(format!(
                "window.max_block_bytes must be in {MIN_BLOCK_BYTES}..={MAX_BLOCK_BYTES}"
            ));
        }
        if w.interval < Duration::from_secs(1)
            || w.interval > MAX_INTERVAL
            || w.interval.subsec_nanos() != 0
        {
            return Err("window.interval must be whole seconds between 1s and 1h".into());
        }
        for (name, d) in [
            ("window.flush_retry_deadline", w.flush_retry_deadline),
            ("retry_initial_backoff", self.retry_initial_backoff),
            ("retry_max_backoff", self.retry_max_backoff),
        ] {
            if d.is_zero() {
                return Err(format!("{name} must be > 0"));
            }
        }
        if self.retry_initial_backoff > self.retry_max_backoff {
            return Err("retry_initial_backoff must be <= retry_max_backoff".into());
        }
        let i = &self.ingress;
        for (name, v) in [
            ("ingress.max_request_bytes", i.max_request_bytes),
            ("ingress.max_extracted_bytes", i.max_extracted_bytes),
            ("ingress.max_row_bytes", i.max_row_bytes),
        ] {
            if v == 0 {
                return Err(format!("{name} must be > 0"));
            }
        }
        if !(1..=MAX_CACHE_ENTRIES).contains(&self.series_cache.max_entries) {
            return Err(format!(
                "series_cache.max_entries must be in 1..={MAX_CACHE_ENTRIES}"
            ));
        }
        if !(1..=MAX_NESTING_DEPTH).contains(&i.max_nesting_depth) {
            return Err(format!(
                "ingress.max_nesting_depth must be in 1..={MAX_NESTING_DEPTH}"
            ));
        }
        if i.max_row_bytes > i.max_extracted_bytes {
            return Err("ingress.max_row_bytes must be <= ingress.max_extracted_bytes".into());
        }
        // A request that cannot fit an empty block could never be admitted.
        if i.max_extracted_bytes > w.max_block_bytes {
            return Err("ingress.max_extracted_bytes must be <= window.max_block_bytes".into());
        }
        Ok(())
    }

    /// Upper bound of exporter-held memory:
    /// - the ACTIVE generation, the FLUSHING generation (held until encoded) and the encoded files
    ///   of the block being uploaded (B + B/8);
    /// - the sort keys and row weights of the block being encoded, and one gathered slice: slices
    ///   are cut at `SLICE_BYTES` of row content, or one row when a row is larger, and a gathered
    ///   slice's Arrow memory is up to twice its content;
    /// - the request being extracted (its rows and their chunk copies) and one parked request,
    ///   each with one chunk of slack: the running total refuses a request only after the chunk
    ///   that passes the limit has been built;
    /// - the series cache, the unfilled capacity of each output file and the encoder buffers (the
    ///   row-group cap plus the slice that crosses it and the page buffers).
    ///
    /// Excludes the received payload itself (at most `ingress.max_request_bytes` plus its OTAP
    /// form) and the per-block sets of seen series ids (16 bytes per distinct series).
    #[must_use]
    pub const fn worst_case_flush_bytes(&self) -> usize {
        // Validation bounds every input (b <= 1 GiB, e <= b, max_entries <= MAX_CACHE_ENTRIES);
        // the saturating operations keep an unvalidated config from wrapping.
        let b = self.window.max_block_bytes;
        let e = self.ingress.max_extracted_bytes;
        let slice = self.max_slice_bytes();
        let encoder_overshoot = if slice > ROW_GROUP_BYTES {
            slice
        } else {
            ROW_GROUP_BYTES
        };
        b.saturating_mul(3)
            .saturating_add(b / 8)
            .saturating_add(
                (b / MIN_VALUES_ROW_BYTES).saturating_mul(SORT_KEY_BYTES + ROW_WEIGHT_BYTES),
            )
            .saturating_add(slice.saturating_mul(2))
            .saturating_add(e.saturating_mul(3))
            .saturating_add(self.max_chunk_bytes().saturating_mul(2))
            .saturating_add(
                self.series_cache
                    .max_entries
                    .saturating_mul(CACHE_ENTRY_BYTES),
            )
            .saturating_add(2 * OUTPUT_BLOCK_BYTES + ROW_GROUP_BYTES)
            .saturating_add(encoder_overshoot)
    }

    /// Row content of the largest slice the encoder gathers: `SLICE_BYTES`, or one row when
    /// `ingress.max_row_bytes` is larger.
    #[must_use]
    pub const fn max_slice_bytes(&self) -> usize {
        if self.ingress.max_row_bytes > SLICE_BYTES {
            self.ingress.max_row_bytes
        } else {
            SLICE_BYTES
        }
    }

    /// Largest chunk pushed into a block (a single larger row forms a chunk alone).
    #[must_use]
    pub const fn max_chunk_bytes(&self) -> usize {
        self.window.max_block_bytes / 4
    }

    /// Extraction limits of one request.
    #[must_use]
    pub const fn limits(&self) -> Limits {
        Limits {
            max_extracted_bytes: self.ingress.max_extracted_bytes,
            max_row_bytes: self.ingress.max_row_bytes,
            max_nesting_depth: self.ingress.max_nesting_depth,
            max_chunk_bytes: self.max_chunk_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const MIB: usize = 1024 * 1024;

    fn base() -> serde_json::Value {
        json!({"storage": {"file": {"base_uri": "/tmp/lake"}}})
    }

    fn with(key: &str, value: serde_json::Value) -> serde_json::Value {
        let mut v = base();
        v[key] = value;
        v
    }

    /// `base()` with `section.field` set to `value`.
    fn with_in(section: &str, field: &str, value: serde_json::Value) -> serde_json::Value {
        let mut v = base();
        v[section] = json!({ field: value });
        v
    }

    /// Scenario: A config with only the storage backend is parsed.
    /// Guarantees: Every other field takes its documented default and the config validates.
    #[test]
    fn defaults_validate() {
        let c = LakeConfig::parse(&base()).expect("valid");
        assert_eq!(c.window.interval, Duration::from_secs(15));
        assert_eq!(c.window.max_block_bytes, 64 * MIB);
        assert_eq!(c.window.flush_retry_deadline, Duration::from_secs(60));
        assert_eq!(c.ingress.max_request_bytes, 16 * MIB);
        assert_eq!(c.ingress.max_extracted_bytes, 32 * MIB);
        assert_eq!(c.ingress.max_row_bytes, MIB);
        assert_eq!(c.ingress.max_nesting_depth, 32);
        assert_eq!(c.series_cache.max_entries, 200_000);
        assert_eq!(c.writer_id, "writer");
        assert_eq!(c.producer_id_attribute, "host.id");
        assert_eq!(c.retry_initial_backoff, Duration::from_millis(200));
        assert_eq!(c.retry_max_backoff, Duration::from_secs(10));
        assert_eq!(c.max_chunk_bytes(), 16 * MIB);
        assert_eq!(c.limits().max_chunk_bytes, 16 * MIB);
    }

    /// Scenario: The flush bound is computed for the default configuration, and for a configuration whose rows may reach 12 MiB.
    /// Guarantees: The default bound equals three blocks plus B/8 of encoded output, the sort keys and row weights of one block (36 bytes per row), two slices of 1 MiB, three requests, two chunks of slack, the series cache, two output allocation units and the 16 MiB encoder bound; with 12 MiB rows the slice term is two rows and the encoder term grows by the row beyond the row-group cap.
    #[test]
    fn worst_case_formula() {
        let c = LakeConfig::parse(&base()).expect("valid");
        let (b, e) = (64 * MIB, 32 * MIB);
        assert_eq!(
            c.worst_case_flush_bytes(),
            3 * b
                + b / 8
                + b / 96 * 36
                + 2 * MIB
                + 3 * e
                + 2 * (b / 4)
                + 200_000 * 128
                + 2 * MIB
                + 16 * MIB
        );
        assert_eq!(c.worst_case_flush_bytes(), 415_670_248);

        let mut v = base();
        v["ingress"] = json!({"max_row_bytes": 12 * MIB});
        let big_rows = LakeConfig::parse(&v).expect("valid");
        assert_eq!(big_rows.max_slice_bytes(), 12 * MIB);
        assert_eq!(
            big_rows.worst_case_flush_bytes(),
            c.worst_case_flush_bytes() - 2 * MIB + 2 * 12 * MIB - 8 * MIB + 12 * MIB
        );
    }

    /// Scenario: A config uses the top-level fields of the previous layout, or a field this exporter does not have.
    /// Guarantees: Each is rejected by name instead of being silently ignored.
    #[test]
    fn rejects_unknown_and_removed_fields() {
        for field in [
            "labels",
            "max_block_bytes",
            "max_block_age",
            "check_interval",
            "upload_deadline",
        ] {
            let err = LakeConfig::parse(&with(field, json!(1))).expect_err("unknown");
            assert!(err.to_string().contains(field), "{err}");
        }
        let err = LakeConfig::parse(&with("window", json!({"max_requests_per_block": 1})))
            .expect_err("unknown");
        assert!(err.to_string().contains("max_requests_per_block"), "{err}");
    }

    /// Scenario: Block sizes outside 1 MiB..=1 GiB; intervals of 500ms, 0s, 2h and 1500ms; zero deadline and backoffs; zero request limits; nesting depths 0 and 257; cache sizes 0 and one above the maximum.
    /// Guarantees: Each is rejected and the message names the offending field.
    #[test]
    fn rejects_out_of_range_values() {
        for bad in [1024, MAX_BLOCK_BYTES + 1] {
            let err = LakeConfig::parse(&with_in("window", "max_block_bytes", json!(bad)))
                .expect_err("range");
            assert!(err.to_string().contains("window.max_block_bytes"), "{err}");
        }
        for bad in ["500ms", "0s", "2h", "1500ms"] {
            let err = LakeConfig::parse(&with_in("window", "interval", json!(bad))).expect_err(bad);
            assert!(err.to_string().contains("window.interval"), "{bad}: {err}");
        }
        let err = LakeConfig::parse(&with_in("window", "flush_retry_deadline", json!("0s")))
            .expect_err("zero");
        assert!(
            err.to_string().contains("window.flush_retry_deadline"),
            "{err}"
        );
        for field in ["retry_initial_backoff", "retry_max_backoff"] {
            let err = LakeConfig::parse(&with(field, json!("0s"))).expect_err("zero");
            assert!(err.to_string().contains(field), "{err}");
        }
        for field in ["max_request_bytes", "max_extracted_bytes", "max_row_bytes"] {
            let err = LakeConfig::parse(&with_in("ingress", field, json!(0))).expect_err("zero");
            assert!(
                err.to_string().contains(&format!("ingress.{field}")),
                "{err}"
            );
        }
        for bad in [0, MAX_NESTING_DEPTH + 1] {
            let err = LakeConfig::parse(&with_in("ingress", "max_nesting_depth", json!(bad)))
                .expect_err("depth");
            assert!(
                err.to_string().contains("ingress.max_nesting_depth"),
                "{err}"
            );
        }
        for bad in [0, MAX_CACHE_ENTRIES + 1] {
            let err = LakeConfig::parse(&with_in("series_cache", "max_entries", json!(bad)))
                .expect_err("cache");
            assert!(
                err.to_string().contains("series_cache.max_entries"),
                "{err}"
            );
        }
    }

    /// Scenario: A config built in code (not validated) has a series cache of usize::MAX entries.
    /// Guarantees: The flush bound saturates at usize::MAX instead of wrapping or panicking.
    #[test]
    fn worst_case_formula_saturates() {
        let mut c: LakeConfig = serde_json::from_value(base()).expect("deserializes");
        c.series_cache.max_entries = usize::MAX;
        assert_eq!(c.worst_case_flush_bytes(), usize::MAX);
    }

    /// Scenario: The row limit exceeds the extracted limit; the extracted limit exceeds the block budget.
    /// Guarantees: Both are rejected and the message names both fields, because such a request could never be admitted.
    #[test]
    fn rejects_limits_that_cannot_fit() {
        let mut v = base();
        v["ingress"] = json!({"max_row_bytes": 2 * MIB, "max_extracted_bytes": MIB});
        let err = LakeConfig::parse(&v)
            .expect_err("row above extracted")
            .to_string();
        assert!(
            err.contains("ingress.max_row_bytes") && err.contains("ingress.max_extracted_bytes"),
            "{err}"
        );
        let mut v = base();
        v["ingress"] = json!({"max_extracted_bytes": 8 * MIB});
        v["window"] = json!({"max_block_bytes": 4 * MIB});
        let err = LakeConfig::parse(&v)
            .expect_err("extracted above block")
            .to_string();
        assert!(
            err.contains("ingress.max_extracted_bytes") && err.contains("window.max_block_bytes"),
            "{err}"
        );
    }

    /// Scenario: The writer id is empty, contains a path separator, a space or a colon, or is
    /// longer than the length limit; then a valid id at the limit is used.
    /// Guarantees: Ids that could change the object path, break its parsing, or overflow a
    /// filesystem name are rejected by field name; a well-formed id of exactly the maximum length
    /// is accepted.
    #[test]
    fn rejects_bad_writer_id() {
        for bad in ["", "a/b", "a b", "a:b"] {
            let err = LakeConfig::parse(&with("writer_id", json!(bad))).expect_err(bad);
            assert!(err.to_string().contains("writer_id"), "{bad}: {err}");
        }
        let too_long = "a".repeat(MAX_WRITER_ID_LEN + 1);
        let err = LakeConfig::parse(&with("writer_id", json!(too_long))).expect_err("too long");
        assert!(err.to_string().contains("writer_id"), "{err}");
        let at_limit = "a".repeat(MAX_WRITER_ID_LEN);
        let c = LakeConfig::parse(&with("writer_id", json!(at_limit))).expect("valid at limit");
        assert_eq!(c.writer_id.len(), MAX_WRITER_ID_LEN);
        let c = LakeConfig::parse(&with("writer_id", json!("w-1_a.b"))).expect("valid");
        assert_eq!(c.writer_id, "w-1_a.b");
        let err = LakeConfig::parse(&with("producer_id_attribute", json!(""))).expect_err("empty");
        assert!(err.to_string().contains("producer_id_attribute"), "{err}");
    }

    /// Scenario: The initial backoff exceeds the maximum backoff.
    /// Guarantees: `parse` applies the cross-field semantic checks, not only deserialization.
    #[test]
    fn parse_applies_semantic_validation() {
        let mut v = with("retry_initial_backoff", json!("20s"));
        v["retry_max_backoff"] = json!("1s");
        let err = LakeConfig::parse(&v).expect_err("semantic");
        assert!(err.to_string().contains("retry_initial_backoff"), "{err}");
    }
}
