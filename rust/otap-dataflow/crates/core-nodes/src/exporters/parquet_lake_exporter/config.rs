// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration of the Parquet lake exporter.

use std::time::Duration;

use otel_arrow_dfe_otap::object_store::{RetryOptions, StorageType};
use serde::Deserialize;

/// Flush the encoder's row group once `ArrowWriter::memory_size` reaches this.
pub const ROW_GROUP_BYTES: usize = 8 * 1024 * 1024;
/// Input bytes handed to the encoder per write; bounds the overshoot past ROW_GROUP_BYTES and the
/// time between yields.
pub const SLICE_BYTES: usize = 1024 * 1024;
/// Encoder memory bound: the ROW_GROUP_BYTES cap plus one slice of overshoot and page buffers
/// (measured in tests).
pub const ENCODER_BYTES: usize = 2 * ROW_GROUP_BYTES;
/// Allocation unit of encoded output; each output file wastes at most one unit of capacity.
pub const OUTPUT_BLOCK_BYTES: usize = 1024 * 1024;
/// Smallest accepted `max_block_bytes`.
pub const MIN_BLOCK_BYTES: usize = 1024 * 1024;
/// Largest accepted `max_block_bytes` (keeps the bound arithmetic and Arrow i32 offsets safe).
pub const MAX_BLOCK_BYTES: usize = 1024 * 1024 * 1024;
/// Number of signals with their own open block (logs, metrics).
pub const SIGNALS: usize = 2;

/// Exporter configuration.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LakeConfig {
    /// Object-store backend (file, s3, azure).
    pub storage: StorageType,
    /// Per-request object-store retry policy.
    #[serde(default)]
    pub retry: Option<RetryOptions>,
    /// Flush a signal's block once its buffered Arrow data would exceed this many bytes.
    #[serde(default = "default_max_block_bytes")]
    pub max_block_bytes: usize,
    /// Flush a non-empty block once it is this old.
    #[serde(default = "default_max_block_age", with = "humantime_serde")]
    pub max_block_age: Duration,
    /// Timer interval for block age checks.
    #[serde(default = "default_check_interval", with = "humantime_serde")]
    pub check_interval: Duration,
    /// Give up on a block (Nack its batches) when it has not landed within this time.
    #[serde(default = "default_upload_deadline", with = "humantime_serde")]
    pub upload_deadline: Duration,
    /// First retry backoff; doubles per attempt up to `retry_max_backoff`.
    #[serde(default = "default_retry_initial_backoff", with = "humantime_serde")]
    pub retry_initial_backoff: Duration,
    /// Largest retry backoff.
    #[serde(default = "default_retry_max_backoff", with = "humantime_serde")]
    pub retry_max_backoff: Duration,
}

const fn default_max_block_bytes() -> usize {
    64 * 1024 * 1024
}
const fn default_max_block_age() -> Duration {
    Duration::from_secs(60)
}
const fn default_check_interval() -> Duration {
    Duration::from_secs(1)
}
const fn default_upload_deadline() -> Duration {
    Duration::from_secs(60)
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
        if !(MIN_BLOCK_BYTES..=MAX_BLOCK_BYTES).contains(&self.max_block_bytes) {
            return Err(format!(
                "max_block_bytes must be in {MIN_BLOCK_BYTES}..={MAX_BLOCK_BYTES}"
            ));
        }
        for (name, d) in [
            ("max_block_age", self.max_block_age),
            ("check_interval", self.check_interval),
            ("upload_deadline", self.upload_deadline),
            ("retry_initial_backoff", self.retry_initial_backoff),
            ("retry_max_backoff", self.retry_max_backoff),
        ] {
            if d.is_zero() {
                return Err(format!("{name} must be > 0"));
            }
        }
        if self.check_interval > self.max_block_age {
            return Err("check_interval must be <= max_block_age".into());
        }
        if self.retry_initial_backoff > self.retry_max_backoff {
            return Err("retry_initial_backoff must be <= retry_max_backoff".into());
        }
        Ok(())
    }

    /// Upper bound of exporter-held memory while a block is flushed: both open signal blocks, the
    /// encoded bytes of the flushing block's two files (B + B/8), the unfilled capacity of each
    /// output file and the encoder buffers. Excludes the payload being processed (input records
    /// and their extracted chunks), which is bounded upstream.
    #[must_use]
    pub const fn worst_case_flush_bytes(&self) -> usize {
        let b = self.max_block_bytes;
        SIGNALS * b + b + b / 8 + 2 * OUTPUT_BLOCK_BYTES + ENCODER_BYTES
    }

    /// Largest chunk pushed into a block (a single larger row forms a chunk alone).
    #[must_use]
    pub const fn max_chunk_bytes(&self) -> usize {
        self.max_block_bytes / 4
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

    /// Scenario: A config with only the storage backend is parsed.
    /// Guarantees: Every other field takes its documented default and the config validates.
    #[test]
    fn defaults_validate() {
        let c = LakeConfig::parse(&base()).expect("valid");
        assert_eq!(c.max_block_bytes, 64 * MIB);
        assert_eq!(c.max_block_age, Duration::from_secs(60));
        assert_eq!(c.check_interval, Duration::from_secs(1));
        assert_eq!(c.upload_deadline, Duration::from_secs(60));
        assert_eq!(c.retry_initial_backoff, Duration::from_millis(200));
        assert_eq!(c.retry_max_backoff, Duration::from_secs(10));
        assert_eq!(c.max_chunk_bytes(), 16 * MIB);
    }

    /// Scenario: The flush bound is computed for the default 64 MiB block.
    /// Guarantees: It equals two open blocks, B + B/8 of encoded output, two output allocation units and the encoder bound.
    #[test]
    fn worst_case_formula() {
        let c = LakeConfig::parse(&base()).expect("valid");
        assert_eq!(
            c.worst_case_flush_bytes(),
            3 * 64 * MIB + 8 * MIB + 2 * MIB + 16 * MIB
        );
    }

    /// Scenario: Block sizes outside 1 MiB..=1 GiB and zero durations are configured.
    /// Guarantees: Each is rejected with the offending field named.
    #[test]
    fn rejects_block_bytes_outside_range_and_zero_durations() {
        for bad in [1024, MAX_BLOCK_BYTES + 1] {
            let err = LakeConfig::parse(&with("max_block_bytes", json!(bad))).expect_err("range");
            assert!(err.to_string().contains("max_block_bytes"), "{err}");
        }
        for field in [
            "max_block_age",
            "check_interval",
            "upload_deadline",
            "retry_initial_backoff",
            "retry_max_backoff",
        ] {
            let err = LakeConfig::parse(&with(field, json!("0s"))).expect_err("zero");
            assert!(err.to_string().contains(field), "{err}");
        }
    }

    /// Scenario: A config contains a field the exporter does not know.
    /// Guarantees: Deserialization rejects it instead of silently ignoring it.
    #[test]
    fn rejects_unknown_fields() {
        let err = LakeConfig::parse(&with("labels", json!({}))).expect_err("unknown");
        assert!(err.to_string().contains("labels"), "{err}");
    }

    /// Scenario: The check interval exceeds the block age, and the initial backoff exceeds the maximum.
    /// Guarantees: `parse` applies the cross-field semantic checks, not only deserialization.
    #[test]
    fn parse_applies_semantic_validation() {
        let mut v = with("max_block_age", json!("1s"));
        v["check_interval"] = json!("2s");
        assert!(LakeConfig::parse(&v).is_err());
        let mut v = with("retry_initial_backoff", json!("20s"));
        v["retry_max_backoff"] = json!("1s");
        assert!(LakeConfig::parse(&v).is_err());
    }
}
