// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Errors of the Parquet lake exporter.

/// Hint appended to a size refusal.
pub const SPLIT_HINT: &str = "split the batch upstream or raise the limit";

/// Errors converting or writing a batch.
#[derive(thiserror::Error, Debug)]
pub enum LakeError {
    /// The payload could not be converted to OTAP records.
    #[error("payload conversion failed: {0}")]
    Conversion(String),
    /// A signal this exporter does not store.
    #[error("unsupported: {0}")]
    Unsupported(&'static str),
    /// Content the format refuses: duplicate keys, a point without a metric, a bad histogram, ...
    #[error("invalid content: {0}")]
    Invalid(String),
    /// A nested value is deeper than `ingress.max_nesting_depth`, the limit carried here.
    #[error(
        "nested value deeper than ingress.max_nesting_depth ({0} levels); flatten the value or raise the limit"
    )]
    TooDeep(usize),
    /// A size limit was exceeded.
    #[error("{observed} bytes exceed {setting} ({limit} bytes); {SPLIT_HINT}")]
    TooLarge {
        /// The config setting that refused the request.
        setting: &'static str,
        /// The size measured against it.
        observed: usize,
        /// The configured limit.
        limit: usize,
    },
    /// A required column is missing.
    #[error("missing column `{column}` in table `{table}`")]
    MissingColumn {
        /// OTAP table name.
        table: &'static str,
        /// Column name.
        column: &'static str,
    },
    /// Arrow kernel failure.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// Parquet encoding failure.
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// Object storage failed or refused a request (flush path).
    #[error("object store error: {0}")]
    Store(#[from] object_store::Error),
    /// A block did not land before its flush deadline.
    #[error("flush deadline exceeded")]
    Deadline,
    /// fsync of a landed file failed (file backend).
    #[error("fsync failed: {0}")]
    Fsync(String),
    /// The flush task panicked or was aborted.
    #[error("flush task failed: {0}")]
    Task(String),
}

/// Why a request was refused, for the refusal counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Refusal {
    /// A size limit.
    TooLarge,
    /// Invalid content.
    Invalid,
    /// Nesting limit.
    TooDeep,
    /// Unsupported signal.
    Unsupported,
    /// Conversion, Arrow or Parquet failure.
    Other,
}

impl LakeError {
    /// The refusal class of this error.
    #[must_use]
    pub const fn refusal(&self) -> Refusal {
        match self {
            Self::TooLarge { .. } => Refusal::TooLarge,
            Self::Invalid(_) | Self::MissingColumn { .. } => Refusal::Invalid,
            Self::TooDeep(_) => Refusal::TooDeep,
            Self::Unsupported(_) => Refusal::Unsupported,
            // The flush-path variants never refuse a request; they end up in a retryable Nack.
            Self::Conversion(_)
            | Self::Arrow(_)
            | Self::Parquet(_)
            | Self::Store(_)
            | Self::Deadline
            | Self::Fsync(_)
            | Self::Task(_) => Refusal::Other,
        }
    }
}
