// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Errors of the Parquet lake exporter.

/// Errors converting or writing a batch.
#[derive(thiserror::Error, Debug)]
pub enum LakeError {
    /// The payload could not be converted to OTAP records.
    #[error("payload conversion failed: {0}")]
    Conversion(String),
    /// Traces are not supported.
    #[error("unsupported signal: traces")]
    UnsupportedSignal,
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
}
