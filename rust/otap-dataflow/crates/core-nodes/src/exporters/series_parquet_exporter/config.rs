// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! User configuration mapped onto the engine-independent lake configuration.
//!
//! This module owns the whole user schema; it maps onto
//! [`otel_arrow_dfe_series_lake::config::LakeConfig`], which has no serde form
//! of its own. Block budgets live under `window`, and byte sizes accept units
//! such as `64MiB`. [`Config`] is the validated [`RawConfig`]; every lake
//! constraint is checked here at startup, and each refusal names the dotted key
//! a user writes.

use otel_arrow_dfe_config::byte_units::deserialize_required_usize;
use otel_arrow_dfe_otap::object_store::{RetryOptions, StorageType};
use otel_arrow_dfe_series_lake::config::{
    Denormalize, ExemplarPolicy, IngressLimits, LakeConfig, MAX_WINDOW_INTERVAL, ParquetConfig,
    SignalConfig, SortKey, SortingConfig, UnsupportedPolicy, UploadConfig,
};
use serde::{Deserialize, Deserializer};
use std::time::Duration;

/// Window rotation interval and the budgets of one block.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Window {
    /// Aligned rotation interval; whole seconds only.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Retained-bytes limit of one block.
    #[serde(deserialize_with = "deserialize_required_usize")]
    pub max_block_bytes: usize,
    /// Ack tokens one block may hold.
    pub max_requests_per_block: usize,
    /// Upper bound on retrying a flush before the block is failed.
    #[serde(with = "humantime_serde")]
    pub flush_retry_deadline: Duration,
}

impl Default for Window {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(15),
            max_block_bytes: 500 << 20,
            max_requests_per_block: 4096,
            flush_retry_deadline: Duration::from_secs(60),
        }
    }
}

/// The request-level budgets a user sets under `ingress`.
///
/// Five of the lake's ingress limits: the two block-level ones are set under
/// `window`, so writing them here is an unknown field.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Ingress {
    #[serde(deserialize_with = "deserialize_required_usize")]
    max_request_bytes: usize,
    #[serde(deserialize_with = "deserialize_required_usize")]
    max_extracted_bytes: usize,
    #[serde(deserialize_with = "deserialize_required_usize")]
    max_row_bytes: usize,
    max_nesting_depth: usize,
    /// Unset means the most the block budget holds; see
    /// [`LakeConfig::max_series_per_request`].
    max_series_per_request: Option<usize>,
}

impl Default for Ingress {
    fn default() -> Self {
        let lake = IngressLimits::default();
        Self {
            max_request_bytes: lake.max_request_bytes,
            max_extracted_bytes: lake.max_extracted_bytes,
            max_row_bytes: lake.max_row_bytes,
            max_nesting_depth: lake.max_nesting_depth,
            max_series_per_request: lake.max_series_per_request,
        }
    }
}

/// The Parquet writer settings a user sets under `parquet`.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Parquet {
    #[serde(deserialize_with = "deserialize_required_usize")]
    row_group_bytes: usize,
    #[serde(deserialize_with = "deserialize_required_usize")]
    writer_limit_bytes: usize,
}

impl Default for Parquet {
    fn default() -> Self {
        let lake = ParquetConfig::default();
        Self {
            row_group_bytes: lake.row_group_bytes,
            writer_limit_bytes: lake.writer_limit_bytes,
        }
    }
}

/// The sort workspace a user sets under `sorting`.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Sorting {
    #[serde(deserialize_with = "deserialize_required_usize")]
    run_target_bytes: usize,
    #[serde(deserialize_with = "deserialize_required_usize")]
    merge_chunk_bytes: usize,
}

impl Default for Sorting {
    fn default() -> Self {
        let lake = SortingConfig::default();
        Self {
            run_target_bytes: lake.run_target_bytes,
            merge_chunk_bytes: lake.merge_chunk_bytes,
        }
    }
}

/// The multipart upload settings a user sets under `upload`.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Upload {
    #[serde(deserialize_with = "deserialize_required_usize")]
    part_bytes: usize,
    concurrency: usize,
    #[serde(with = "humantime_serde")]
    abort_timeout: Duration,
}

impl Default for Upload {
    fn default() -> Self {
        let lake = UploadConfig::default();
        Self {
            part_bytes: lake.part_bytes,
            concurrency: lake.concurrency,
            abort_timeout: lake.abort_timeout,
        }
    }
}

/// The logs settings a user sets under `logs`.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Logs {
    series_attributes: Vec<String>,
    denormalize: Vec<Denormalize>,
    values_sort: Vec<SortKey>,
}

impl Default for Logs {
    fn default() -> Self {
        let lake = SignalConfig::default();
        Self {
            series_attributes: lake.series_attributes,
            denormalize: lake.denormalize,
            values_sort: lake.values_sort,
        }
    }
}

/// The metrics settings a user sets under `metrics`; a metric's identity has
/// every point attribute, so there is no `series_attributes`.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Metrics {
    denormalize: Vec<Denormalize>,
    values_sort: Vec<SortKey>,
    exemplars: Option<ExemplarPolicy>,
}

impl Default for Metrics {
    fn default() -> Self {
        let lake = SignalConfig::default();
        Self {
            denormalize: lake.denormalize,
            values_sort: lake.values_sort,
            exemplars: lake.exemplars,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Cache {
    max_entries: usize,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            max_entries: 200_000,
        }
    }
}

/// Define a `deserialize_with` function that reads one section and prefixes
/// its error with the section's key, so a misspelled or malformed setting is
/// reported as `ingress: unknown field ...`.
macro_rules! section {
    ($name:ident, $ty:ty, $key:literal) => {
        fn $name<'de, D: Deserializer<'de>>(d: D) -> Result<$ty, D::Error> {
            <$ty>::deserialize(d)
                .map_err(|e| serde::de::Error::custom(format!(concat!($key, ": {}"), e)))
        }
    };
}

section!(window_section, Window, "window");
section!(ingress_section, Ingress, "ingress");
section!(cache_section, Cache, "series_cache");
section!(sorting_section, Sorting, "sorting");
section!(upload_section, Upload, "upload");
section!(parquet_section, Parquet, "parquet");
section!(logs_section, Logs, "logs");
section!(metrics_section, Metrics, "metrics");

/// The user document exactly as written, before cross-field validation.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    storage: StorageType,
    /// Kept as written; a cloud store merges it over [`derived_retry`].
    #[serde(default)]
    retry: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default = "writer")]
    writer_id: String,
    #[serde(default = "producer")]
    producer_id_attribute: String,
    #[serde(default, deserialize_with = "window_section")]
    window: Window,
    #[serde(default, deserialize_with = "ingress_section")]
    ingress: Ingress,
    #[serde(default, deserialize_with = "cache_section")]
    series_cache: Cache,
    #[serde(default, deserialize_with = "sorting_section")]
    sorting: Sorting,
    #[serde(default, deserialize_with = "upload_section")]
    upload: Upload,
    #[serde(default, deserialize_with = "parquet_section")]
    parquet: Parquet,
    #[serde(default)]
    unsupported: UnsupportedPolicy,
    #[serde(default, deserialize_with = "logs_section")]
    logs: Logs,
    #[serde(default, deserialize_with = "metrics_section")]
    metrics: Metrics,
}

fn writer() -> String {
    "writer".into()
}

fn producer() -> String {
    "host.id".into()
}

/// Shortest accepted `upload.abort_timeout`: the shutdown drain decides every
/// held request synchronously and then waits for cleanup only until the
/// latched deadline plus this timeout, so the timeout must leave room for that
/// decision (README.md, "The drain").
const MIN_ABORT_TIMEOUT: Duration = Duration::from_secs(1);

/// Longest accepted `window.flush_retry_deadline` and `upload.abort_timeout`,
/// so every deadline derived from them is a plain `Instant` addition.
const MAX_TIMEOUT: Duration = Duration::from_secs(60 * 60);

/// Refuse a cloud store retry budget that one write attempt could spend past
/// the block's own flush deadline.
///
/// A cloud store retries each request internally for up to
/// `retry.retry_timeout`. If that is not strictly shorter than
/// `window.flush_retry_deadline`, a single attempt against a destination that
/// keeps failing is still retrying inside the store when the block's deadline
/// expires, and the flush ends with no underlying error to report. A budget
/// the `retry` section leaves unset is derived instead (see [`derived_retry`]).
/// Local file storage applies no store retry and is not checked.
pub(super) fn check_retry_deadline(
    retry: &RetryOptions,
    flush_retry_deadline: Duration,
) -> Result<(), String> {
    let timeout = retry.retry_timeout;
    if timeout < flush_retry_deadline {
        return Ok(());
    }
    Err(format!(
        "retry.retry_timeout ({timeout:?}) must be strictly less than \
         window.flush_retry_deadline ({flush_retry_deadline:?}), or one write attempt keeps \
         retrying inside the store past the block deadline; set retry.retry_timeout below \
         the deadline, raise window.flush_retry_deadline, or omit the retry section"
    ))
}

/// The store retry options of a cloud store: object_store's defaults with
/// `retry_timeout` half of `window.flush_retry_deadline`, which leaves the
/// block time for a retry of its own after the store gives up on one
/// attempt, and every field `section` sets written over them.
fn derived_retry(
    flush_retry_deadline: Duration,
    section: Option<serde_json::Map<String, serde_json::Value>>,
) -> Result<RetryOptions, String> {
    let mut retry = written_retry(serde_json::Map::new())?;
    retry.retry_timeout = flush_retry_deadline / 2;
    let Some(section) = section else {
        return Ok(retry);
    };
    let Ok(serde_json::Value::Object(mut merged)) = serde_json::to_value(&retry) else {
        return Err("retry: the derived options do not serialize to a map".into());
    };
    merged.extend(section);
    written_retry(merged)
}

/// A `retry` section read as written, with object_store's default for every
/// field it leaves unset.
fn written_retry(
    section: serde_json::Map<String, serde_json::Value>,
) -> Result<RetryOptions, String> {
    serde_json::from_value(serde_json::Value::Object(section)).map_err(|e| format!("retry: {e}"))
}

/// Validated exporter configuration. Storage and scheduling stay outside
/// series-lake, which knows only about the lake format itself.
#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "RawConfig")]
pub struct Config {
    pub(super) storage: StorageType,
    pub(super) retry: Option<RetryOptions>,
    pub(super) lake: LakeConfig,
    pub(super) window: Window,
    pub(super) cache_entries: usize,
}

impl TryFrom<RawConfig> for Config {
    type Error = String;

    fn try_from(raw: RawConfig) -> Result<Self, String> {
        let interval = raw.window.interval;
        if interval.is_zero() || interval.subsec_nanos() != 0 || interval > MAX_WINDOW_INTERVAL {
            return Err(format!(
                "window.interval must be positive whole seconds, at most {MAX_WINDOW_INTERVAL:?}"
            ));
        }
        if raw.window.flush_retry_deadline.is_zero()
            || raw.window.flush_retry_deadline > MAX_TIMEOUT
        {
            return Err(format!(
                "window.flush_retry_deadline must be positive, at most {MAX_TIMEOUT:?}"
            ));
        }
        if raw.window.max_requests_per_block == 0 {
            return Err("window.max_requests_per_block must be at least 1".into());
        }
        // The node sizes its notification buffer from twice this count.
        if raw.window.max_requests_per_block.checked_mul(2).is_none() {
            return Err("window.max_requests_per_block overflows the notification capacity".into());
        }
        if raw.series_cache.max_entries == 0 {
            return Err("series_cache.max_entries must be positive".into());
        }
        for (key, value) in [
            ("ingress.max_request_bytes", raw.ingress.max_request_bytes),
            (
                "ingress.max_extracted_bytes",
                raw.ingress.max_extracted_bytes,
            ),
            ("ingress.max_row_bytes", raw.ingress.max_row_bytes),
            ("ingress.max_nesting_depth", raw.ingress.max_nesting_depth),
            ("sorting.run_target_bytes", raw.sorting.run_target_bytes),
            ("sorting.merge_chunk_bytes", raw.sorting.merge_chunk_bytes),
            ("parquet.row_group_bytes", raw.parquet.row_group_bytes),
            ("parquet.writer_limit_bytes", raw.parquet.writer_limit_bytes),
        ] {
            if value == 0 {
                return Err(format!("{key} must be positive"));
            }
        }
        if raw.upload.abort_timeout < MIN_ABORT_TIMEOUT || raw.upload.abort_timeout > MAX_TIMEOUT {
            return Err(format!(
                "upload.abort_timeout ({:?}) must be at least {MIN_ABORT_TIMEOUT:?}, at most \
                 {MAX_TIMEOUT:?}",
                raw.upload.abort_timeout
            ));
        }
        let mut lake = LakeConfig {
            writer_id: raw.writer_id,
            producer_id_attribute: raw.producer_id_attribute,
            window_interval: interval,
            ingress: IngressLimits {
                max_request_bytes: raw.ingress.max_request_bytes,
                max_extracted_bytes: raw.ingress.max_extracted_bytes,
                max_row_bytes: raw.ingress.max_row_bytes,
                max_nesting_depth: raw.ingress.max_nesting_depth,
                max_block_bytes: raw.window.max_block_bytes,
                max_requests_per_block: raw.window.max_requests_per_block,
                ..IngressLimits::default()
            },
            sorting: SortingConfig {
                run_target_bytes: raw.sorting.run_target_bytes,
                merge_chunk_bytes: raw.sorting.merge_chunk_bytes,
            },
            upload: UploadConfig {
                part_bytes: raw.upload.part_bytes,
                concurrency: raw.upload.concurrency,
                abort_timeout: raw.upload.abort_timeout,
            },
            parquet: ParquetConfig {
                row_group_bytes: raw.parquet.row_group_bytes,
                writer_limit_bytes: raw.parquet.writer_limit_bytes,
            },
            unsupported: raw.unsupported,
            logs: SignalConfig {
                series_attributes: raw.logs.series_attributes,
                denormalize: raw.logs.denormalize,
                values_sort: raw.logs.values_sort,
                exemplars: None,
            },
            metrics: SignalConfig {
                series_attributes: Vec::new(),
                denormalize: raw.metrics.denormalize,
                values_sort: raw.metrics.values_sort,
                exemplars: raw.metrics.exemplars,
            },
        };
        // Resolved once here, so no request derives it again. An unset limit
        // that the budgets leave no room for becomes 1, which the bound check
        // below then refuses with its numbers.
        lake.ingress.max_series_per_request = raw.ingress.max_series_per_request;
        lake.ingress.max_series_per_request = Some(lake.max_series_per_request());
        // A configuration rule's own sentence, without the request refusal
        // wrapper the lake error type carries.
        let rule = |e: otel_arrow_dfe_series_lake::Error| {
            e.invalid_detail()
                .map_or_else(|| e.to_string(), str::to_owned)
        };
        // The one cross-field rule whose lake form names a lake key
        // (`ingress.max_block_bytes`): checked here first with the key the
        // user writes. Every other cross-field rule is the lake's own, and its
        // message already names the user's key.
        lake.check_request_bound("window.max_block_bytes")
            .map_err(rule)?;
        lake.validate().map_err(rule)?;
        let retry = if matches!(raw.storage, StorageType::File { .. }) {
            // Validated, then ignored: local file storage applies no store retry.
            raw.retry.map(written_retry).transpose()?
        } else {
            let retry = derived_retry(raw.window.flush_retry_deadline, raw.retry)?;
            check_retry_deadline(&retry, raw.window.flush_retry_deadline)?;
            Some(retry)
        };
        Ok(Self {
            storage: raw.storage,
            retry,
            lake,
            window: raw.window,
            cache_entries: raw.series_cache.max_entries,
        })
    }
}
