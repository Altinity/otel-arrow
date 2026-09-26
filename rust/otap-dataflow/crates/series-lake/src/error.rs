// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Error types shared by the crate.

/// The size budget a too-large request exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeBudget {
    /// The logical request size (`ingress.max_request_bytes`).
    Request,
    /// The extracted output of the request (`ingress.max_extracted_bytes`).
    Extracted,
    /// One values or series row (`ingress.max_row_bytes`).
    Row,
    /// One attribute key or value, or one CBOR cell (`ingress.max_row_bytes`).
    Cell,
    /// Decoded attribute values, charged to the extracted budget
    /// (`ingress.max_extracted_bytes`).
    Table,
}

/// How far a too-large request exceeded which budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Excess {
    /// The budget that refused the request.
    pub budget: SizeBudget,
    /// The size measured against it.
    pub observed: usize,
    /// The configured limit.
    pub limit: usize,
}

impl Excess {
    /// A refusal of `observed` bytes against `limit` for `budget`.
    #[must_use]
    pub fn new(budget: SizeBudget, observed: usize, limit: usize) -> Self {
        Self {
            budget,
            observed,
            limit,
        }
    }
}

/// Why a request is permanently refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefuseReason {
    /// The request itself, its extracted output or one of its rows exceeds a
    /// budget; the excess says which, by how much.
    RequestTooLarge(Excess),
    /// The active block cannot take this request; the caller rotates and retries.
    BlockFull,
    /// The active block already holds `max_requests_per_block` tokens.
    TooManyRequests,
    /// The request carries more distinct series than
    /// `ingress.max_series_per_request`.
    TooManySeries {
        /// Series counted when the limit was passed: `limit + 1`, since
        /// extraction stops there.
        observed: usize,
        /// The configured limit.
        limit: usize,
    },
    /// The request's completion token, its routing context, holds more than
    /// the bytes a block reserves for one: the route is too deep.
    TokenTooLarge {
        /// The token's bytes.
        observed: usize,
        /// The allowance, `config::TOKEN_ALLOWANCE_BYTES`.
        limit: usize,
    },
    /// A nested value is deeper than `ingress.max_nesting_depth`, the limit
    /// carried here.
    ///
    /// Distinct from [`RefuseReason::Invalid`]: the content is well formed,
    /// it only exceeds a configured bound, so an operator can tell which
    /// setting refused it.
    TooDeep(usize),
    /// Malformed content: duplicate keys, bad histogram, ...
    Invalid(String),
    /// Unsupported signal or point kind under the reject policy.
    Unsupported(String),
}

impl std::fmt::Display for SizeBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SizeBudget::Request => "request",
            SizeBudget::Extracted => "extracted",
            SizeBudget::Row => "row",
            SizeBudget::Cell => "cell",
            SizeBudget::Table => "table",
        })
    }
}

/// One line for logs and error chains.
///
/// The two block-scoped refusals keep their variant names: a caller that
/// reports one to a sender as an internal failure quotes this text, and
/// that sentence is pinned.
impl std::fmt::Display for RefuseReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefuseReason::RequestTooLarge(Excess {
                budget,
                observed,
                limit,
            }) => write!(
                f,
                "{budget} budget exceeded: {observed} bytes, limit {limit} bytes"
            ),
            RefuseReason::TooManySeries { observed, limit } => write!(
                f,
                "series limit exceeded: at least {observed} distinct series, limit {limit}"
            ),
            RefuseReason::TokenTooLarge { observed, limit } => write!(
                f,
                "completion token of {observed} bytes exceeds the {limit}-byte allowance"
            ),
            RefuseReason::BlockFull => f.write_str("BlockFull"),
            RefuseReason::TooManyRequests => f.write_str("TooManyRequests"),
            RefuseReason::TooDeep(limit) => write!(f, "nesting deeper than {limit} levels"),
            RefuseReason::Invalid(detail) => write!(f, "invalid content: {detail}"),
            RefuseReason::Unsupported(what) => write!(f, "unsupported: {what}"),
        }
    }
}

/// Crate error, in three classes by who can act on the failure.
///
/// Only [`Error::Refused`] judges the request's own content: the identical
/// bytes will be refused again, so a caller reports it to the producer as
/// permanent. [`Error::Transient`] is a failure of the destination or of the
/// write in progress, which a later attempt may survive. [`Error::Internal`]
/// is a failure of this crate or of the libraries it drives; the request is
/// not at fault, so a caller never reports it to a producer as a permanent
/// refusal of the producer's data.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The request must be nacked as non-retryable.
    #[error("refused: {0}")]
    Refused(RefuseReason),
    /// Storage or the write in progress failed.
    #[error(transparent)]
    Transient(#[from] TransientError),
    /// A writer invariant, or an Arrow or Parquet operation, failed.
    #[error(transparent)]
    Internal(#[from] InternalError),
}

/// A failure of the destination or of the write in progress.
#[derive(Debug, thiserror::Error)]
pub enum TransientError {
    /// Object store failure.
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),
    /// Flush cancelled. `abort_error` is set when the best-effort multipart
    /// abort also failed or timed out.
    #[error("cancelled{}", match abort_error { Some(e) => format!(" (multipart abort failed: {e})"), None => String::new() })]
    Cancelled {
        /// Why the cleanup abort did not succeed, if it did not.
        abort_error: Option<String>,
    },
    /// A caller that retries the sink ran out of its retry deadline.
    ///
    /// Distinct from [`TransientError::Cancelled`], which is a decision taken
    /// by the caller, so a destination that keeps failing or never answers is
    /// not reported as a cancellation. `last` is the failure of the last
    /// attempt that returned, and `None` when the attempt in flight at the
    /// deadline was the first and had not returned.
    #[error("flush retry deadline exceeded after {attempts} attempt(s); {}", match last { Some(e) => format!("last error: {e}"), None => "no attempt returned before the deadline".to_owned() })]
    DeadlineExceeded {
        /// Attempts started before the deadline, the unfinished one included.
        attempts: u64,
        /// The failure of the last attempt that returned, if any did.
        last: Option<Box<Error>>,
    },
    /// A write failed and the best-effort multipart abort failed as well.
    #[error("{source}; multipart abort failed: {abort_error}")]
    AbortFailed {
        /// The original failure.
        source: Box<Error>,
        /// Why the cleanup abort did not succeed.
        abort_error: String,
    },
}

/// A failure of this crate or of a library it drives; the request is not at
/// fault.
#[derive(Debug, thiserror::Error)]
pub enum InternalError {
    /// Arrow failure.
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// Parquet failure. The Parquet writer wraps whatever the object store
    /// returned, so a caller that retries storage failures looks through its
    /// source chain.
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// A writer invariant did not hold.
    #[error("internal: {0}")]
    Invariant(String),
}

impl From<arrow::error::ArrowError> for Error {
    fn from(error: arrow::error::ArrowError) -> Self {
        Error::Internal(InternalError::Arrow(error))
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(error: parquet::errors::ParquetError) -> Self {
        Error::Internal(InternalError::Parquet(error))
    }
}

impl From<object_store::Error> for Error {
    fn from(error: object_store::Error) -> Self {
        Error::Transient(TransientError::ObjectStore(error))
    }
}

/// Whether an object store failure can succeed on a retry of the same write.
fn transient_store_error(error: &object_store::Error) -> bool {
    !matches!(
        error,
        object_store::Error::PermissionDenied { .. }
            | object_store::Error::Unauthenticated { .. }
            | object_store::Error::NotFound { .. }
    )
}

/// Whether an I/O failure can succeed on a retry of the same write.
fn transient_io_error(error: &std::io::Error) -> bool {
    !matches!(
        error.kind(),
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::NotFound
    )
}

/// Whether an error or any of its sources came from storage or I/O, and the
/// first such source is one a retry can cure.
fn contains_storage_error(mut error: &(dyn std::error::Error + 'static)) -> bool {
    loop {
        if let Some(store) = error.downcast_ref::<object_store::Error>() {
            return transient_store_error(store);
        }
        if let Some(io) = error.downcast_ref::<std::io::Error>() {
            // An I/O error that carries an object store error is classified
            // by what the store said.
            return match io
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<object_store::Error>())
            {
                Some(store) => transient_store_error(store),
                None => transient_io_error(io),
            };
        }
        match error.source() {
            Some(source) => error = source,
            None => return false,
        }
    }
}

/// Crate result.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Shorthand for a size refusal of `observed` bytes against `limit`.
    #[must_use]
    pub fn too_large(budget: SizeBudget, observed: usize, limit: usize) -> Self {
        Error::Refused(RefuseReason::RequestTooLarge(Excess::new(
            budget, observed, limit,
        )))
    }

    /// Shorthand for an invalid-content refusal.
    pub fn invalid(msg: impl Into<String>) -> Self {
        Error::Refused(RefuseReason::Invalid(msg.into()))
    }

    /// The sentence of an invalid-content refusal, such as a configuration
    /// rule, without the refusal wrapper; `None` for any other error.
    #[must_use]
    pub fn invalid_detail(&self) -> Option<&str> {
        match self {
            Error::Refused(RefuseReason::Invalid(detail)) => Some(detail),
            _ => None,
        }
    }

    /// Shorthand for a broken writer invariant.
    pub fn internal(msg: impl Into<String>) -> Self {
        Error::Internal(InternalError::Invariant(msg.into()))
    }

    /// Shorthand for a cancelled write, with the outcome of its cleanup abort.
    #[must_use]
    pub fn cancelled(abort_error: Option<String>) -> Self {
        Error::Transient(TransientError::Cancelled { abort_error })
    }

    /// Whether a failed write may succeed if the identical block is written
    /// again.
    ///
    /// Only a storage failure can, since the bytes and file names are frozen;
    /// an encoding failure repeats forever, and a cancellation or an expired
    /// deadline is a decision already taken. The storage origin of a Parquet
    /// error is found by walking its source chain, here beside the wrapping.
    /// A storage error no retry can cure (refused credentials, a missing
    /// bucket or prefix) is not retryable either.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Transient(TransientError::ObjectStore(e)) => transient_store_error(e),
            Error::Internal(InternalError::Parquet(parquet::errors::ParquetError::External(
                source,
            ))) => contains_storage_error(source.as_ref()),
            Error::Transient(TransientError::AbortFailed { source, .. }) => source.is_retryable(),
            _ => false,
        }
    }

    /// Whether this is a cancelled write, whatever its cleanup did.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Error::Transient(TransientError::Cancelled { .. }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A storage failure a retry can cure.
    fn offline() -> object_store::Error {
        object_store::Error::Generic {
            store: "test",
            source: Box::new(std::io::Error::other("offline")),
        }
    }

    /// A storage failure no retry cures.
    fn denied() -> object_store::Error {
        object_store::Error::PermissionDenied {
            path: "p".into(),
            source: "denied".into(),
        }
    }

    /// Scenario: an error of every class and every wrapping the sink produces.
    /// Guarantees: only a curable storage failure is retryable, however it is wrapped.
    #[test]
    fn only_a_curable_storage_failure_is_retryable() {
        use parquet::errors::ParquetError;
        let parquet = |source: Box<dyn std::error::Error + Send + Sync>| {
            Error::from(ParquetError::External(source))
        };
        let aborted = |source: Error| {
            Error::Transient(TransientError::AbortFailed {
                source: Box::new(source),
                abort_error: "timed out".into(),
            })
        };
        let cases: Vec<(Error, bool)> = vec![
            (Error::from(offline()), true),
            (Error::from(denied()), false),
            (
                Error::from(object_store::Error::NotFound {
                    path: "p".into(),
                    source: "no such bucket".into(),
                }),
                false,
            ),
            (
                Error::from(object_store::Error::Unauthenticated {
                    path: "p".into(),
                    source: "expired".into(),
                }),
                false,
            ),
            (parquet(Box::new(offline())), true),
            (parquet(Box::new(std::io::Error::other(offline()))), true),
            (parquet(Box::new(std::io::Error::other(denied()))), false),
            (
                parquet(Box::new(std::io::Error::other("disk hiccup"))),
                true,
            ),
            (
                parquet(Box::new(std::io::Error::from(
                    std::io::ErrorKind::PermissionDenied,
                ))),
                false,
            ),
            (parquet("encoder bug".into()), false),
            (
                Error::from(ParquetError::General("encoding bug".into())),
                false,
            ),
            (aborted(Error::from(offline())), true),
            (aborted(Error::from(denied())), false),
            (aborted(Error::internal("encode")), false),
            (Error::cancelled(None), false),
            (
                Error::Transient(TransientError::DeadlineExceeded {
                    attempts: 3,
                    last: Some(Box::new(Error::from(offline()))),
                }),
                false,
            ),
            (Error::internal("invariant"), false),
            (
                Error::from(arrow::error::ArrowError::ComputeError("x".into())),
                false,
            ),
            (Error::Refused(RefuseReason::BlockFull), false),
        ];
        for (error, retryable) in &cases {
            assert_eq!(error.is_retryable(), *retryable, "{error:?}");
        }
    }

    /// Scenario: every refusal reason rendered through `Display`, sizes with and without a
    /// measurement.
    /// Guarantees: each reads as one sentence; the block-scoped ones keep their variant names.
    #[test]
    fn a_refusal_displays_as_text() {
        let cases = [
            (
                Error::too_large(SizeBudget::Row, 10, 5),
                "refused: row budget exceeded: 10 bytes, limit 5 bytes",
            ),
            (
                Error::Refused(RefuseReason::TooManySeries {
                    observed: 11,
                    limit: 10,
                }),
                "refused: series limit exceeded: at least 11 distinct series, limit 10",
            ),
            (
                Error::Refused(RefuseReason::TokenTooLarge {
                    observed: 5000,
                    limit: 4096,
                }),
                "refused: completion token of 5000 bytes exceeds the 4096-byte allowance",
            ),
            (
                Error::Refused(RefuseReason::BlockFull),
                "refused: BlockFull",
            ),
            (
                Error::Refused(RefuseReason::TooManyRequests),
                "refused: TooManyRequests",
            ),
            (
                Error::Refused(RefuseReason::TooDeep(8)),
                "refused: nesting deeper than 8 levels",
            ),
            (
                Error::invalid("duplicate attribute key"),
                "refused: invalid content: duplicate attribute key",
            ),
            (
                Error::Refused(RefuseReason::Unsupported("traces".into())),
                "refused: unsupported: traces",
            ),
        ];
        for (error, text) in &cases {
            assert_eq!(error.to_string(), *text);
        }
    }
}
