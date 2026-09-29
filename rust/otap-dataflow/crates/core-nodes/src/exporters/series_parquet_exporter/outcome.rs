// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! How a request was decided, and everything its sender and the operator are
//! told about it.
//!
//! The lake reports a failure in one of three classes (see [`lake::Error`]);
//! [`Outcome::of`] is the one place a class becomes an outcome. Everything
//! downstream is derived from the outcome: the nack cause and whether the nack
//! is permanent, the `error.type` label of the `nacks` counter and the
//! `outcome` log field, and the reason sentence. Only the sentence also reads
//! the error, for the detail a sender can act on.

use otel_arrow_dfe_engine::control::NackCause;
use otel_arrow_dfe_series_lake as lake;
use otel_arrow_dfe_series_lake::extract::is_unprintable;
use otel_arrow_dfe_telemetry::attributes::AttributeEnum;
use otel_arrow_dfe_telemetry_macros::AttributeEnum;
use std::fmt::{self, Display, Formatter};

/// How a request was decided, in the form the sender is told about it.
///
/// Each refusal rule is a separate variant, so the counters keep saying which
/// rule rejected a request after the error value has been dropped.
///
/// Also the `error.type` label of the `nacks` counter. `Ack` is never recorded
/// there, and an untouched bucket is never reported. The nack variants are
/// declared in the counter's label order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, AttributeEnum)]
#[repr(usize)]
pub(super) enum Outcome {
    /// The request is durable.
    Ack,
    /// Writing the request failed; the sender may retry.
    Storage,
    /// The request exceeded `ingress.max_request_bytes`.
    RequestTooLarge,
    /// The extracted request, its decoded attributes included, exceeded
    /// `ingress.max_extracted_bytes`.
    ExtractedTooLarge,
    /// One row, attribute value or CBOR cell exceeded `ingress.max_row_bytes`.
    RowTooLarge,
    /// The request carried more distinct series than
    /// `ingress.max_series_per_request`.
    TooManySeries,
    /// The request's completion token exceeded the allowance a block reserves
    /// for one: its route is too deep.
    TokenTooLarge,
    /// A nested value exceeded `ingress.max_nesting_depth`.
    TooDeep,
    /// The request's content could not be used.
    Invalid,
    /// The request's signal is not handled by this exporter.
    Unsupported,
    /// The node shut down before the request could be decided.
    Shutdown,
    /// A writer invariant failed while handling the request; the request is
    /// not at fault and the sender may retry.
    Internal,
}

impl Outcome {
    /// Every outcome, indexed by its discriminant; the length is the derived
    /// variant count, so a variant left out does not compile.
    pub(super) const ALL: [Outcome; <Outcome as AttributeEnum>::CARDINALITY] = [
        Outcome::Ack,
        Outcome::Storage,
        Outcome::RequestTooLarge,
        Outcome::ExtractedTooLarge,
        Outcome::RowTooLarge,
        Outcome::TooManySeries,
        Outcome::TokenTooLarge,
        Outcome::TooDeep,
        Outcome::Invalid,
        Outcome::Unsupported,
        Outcome::Shutdown,
        Outcome::Internal,
    ];
}

// `ALL[o as usize] == o`, which the per-outcome counter arrays index by.
const _: () = {
    let mut i = 0;
    while i < Outcome::ALL.len() {
        assert!(Outcome::ALL[i] as usize == i);
        i += 1;
    }
};

/// Longest detail an error may contribute to a nack reason, in bytes.
///
/// The reason is the status message a producer sees and logs, so it carries
/// enough of the underlying error to act on but never an unbounded amount of
/// request-derived text.
const MAX_DETAIL_BYTES: usize = 256;

/// A bounded, printable rendering of an error for a nack reason.
///
/// Control and format characters (see [`lake::extract::is_unprintable`]),
/// including line breaks, become spaces so the reason stays one readable
/// line, and the text is cut at a character boundary once it passes
/// [`MAX_DETAIL_BYTES`].
pub(super) fn sanitized(detail: &str) -> String {
    let mut out = String::with_capacity(detail.len().min(MAX_DETAIL_BYTES + 3));
    for c in detail.chars() {
        if out.len() + c.len_utf8() > MAX_DETAIL_BYTES {
            out.push_str("...");
            break;
        }
        out.push(if is_unprintable(c) { ' ' } else { c });
    }
    out
}

/// Longest error text a refusal log line carries, in bytes before escaping.
const MAX_LOGGED_ERROR_BYTES: usize = 1024;

/// An error's text for the `error` field of a refusal log line.
///
/// A lake error can quote the request's own text, so every control and format
/// character is escaped as Rust escapes it (`\n`, `\u{1b}`, `\u{202e}`), like
/// the producer id, and
/// the text is cut at a character boundary once it passes
/// [`MAX_LOGGED_ERROR_BYTES`].
pub(super) fn logged_error(error: &lake::Error) -> String {
    let text = error.to_string();
    let mut out = String::with_capacity(text.len().min(MAX_LOGGED_ERROR_BYTES + 3));
    let mut kept = 0;
    for c in text.chars() {
        kept += c.len_utf8();
        if kept > MAX_LOGGED_ERROR_BYTES {
            out.push_str("...");
            break;
        }
        if is_unprintable(c) {
            out.extend(c.escape_default());
        } else {
            out.push(c);
        }
    }
    out
}

/// What a size budget measures, and the setting that configures it.
fn budget_names(budget: lake::SizeBudget) -> (&'static str, &'static str) {
    match budget {
        lake::SizeBudget::Request => ("request", "ingress.max_request_bytes"),
        lake::SizeBudget::Extracted => ("extracted request", "ingress.max_extracted_bytes"),
        lake::SizeBudget::Row => ("extracted row", "ingress.max_row_bytes"),
        lake::SizeBudget::Cell => ("attribute value", "ingress.max_row_bytes"),
        lake::SizeBudget::Table => ("decoded attribute table", "ingress.max_extracted_bytes"),
    }
}

/// The size budget a size refusal exceeded: the setting, the observed size
/// and the limit. `None` for any other error.
pub(super) fn excess(error: &lake::Error) -> Option<(&'static str, usize, usize)> {
    match error {
        lake::Error::Refused(lake::RefuseReason::RequestTooLarge(excess)) => {
            Some((budget_names(excess.budget).1, excess.observed, excess.limit))
        }
        _ => None,
    }
}

impl Outcome {
    /// The outcome a failed request is reported as.
    ///
    /// Only a lake refusal is permanent, and only one that judges the
    /// request's own content: the block-scoped refusals (`BlockFull`,
    /// `TooManyRequests`) judge whichever block happened to be active, so the
    /// worker parks the request instead, and one that reaches a sender is a
    /// broken invariant. Every other error is storage when the destination or
    /// the write caused it and internal otherwise; neither is a client error,
    /// because an OTLP producer drops permanently rejected data.
    pub(super) fn of(error: &lake::Error) -> Self {
        match error {
            lake::Error::Refused(reason) => match reason {
                lake::RefuseReason::RequestTooLarge(excess) => match excess.budget {
                    lake::SizeBudget::Request => Outcome::RequestTooLarge,
                    lake::SizeBudget::Extracted | lake::SizeBudget::Table => {
                        Outcome::ExtractedTooLarge
                    }
                    lake::SizeBudget::Row | lake::SizeBudget::Cell => Outcome::RowTooLarge,
                },
                lake::RefuseReason::TooManySeries { .. } => Outcome::TooManySeries,
                lake::RefuseReason::TokenTooLarge { .. } => Outcome::TokenTooLarge,
                lake::RefuseReason::TooDeep(_) => Outcome::TooDeep,
                lake::RefuseReason::Unsupported(_) => Outcome::Unsupported,
                lake::RefuseReason::Invalid(_) => Outcome::Invalid,
                lake::RefuseReason::BlockFull | lake::RefuseReason::TooManyRequests => {
                    Outcome::Internal
                }
            },
            lake::Error::Transient(_) => Outcome::Storage,
            lake::Error::Internal(_) => Outcome::Internal,
        }
    }

    /// Stable machine token of the outcome: the `error.type` label and the
    /// `outcome` log field.
    ///
    /// Never the reason a producer is told; that is a sentence, see
    /// [`Outcome::sentence`].
    pub(super) fn label(self) -> &'static str {
        self.as_str()
    }

    /// Whether the sender must change the request before retrying it.
    pub(super) fn refused(self) -> bool {
        matches!(
            self,
            Self::RequestTooLarge
                | Self::ExtractedTooLarge
                | Self::RowTooLarge
                | Self::TooManySeries
                | Self::TokenTooLarge
                | Self::TooDeep
                | Self::Invalid
                | Self::Unsupported
        )
    }

    /// The cause a nack of this outcome carries; `None` for an ack.
    pub(super) fn nack_cause(self) -> Option<NackCause> {
        match self {
            Self::Ack => None,
            Self::Shutdown => Some(NackCause::NodeShutdown),
            refused if refused.refused() => Some(NackCause::Refused),
            _ => Some(NackCause::Unspecified),
        }
    }

    /// The reason sentence a completion carries when its decision supplied
    /// none of its own: what happened and what the sender should do.
    pub(super) fn sentence(self) -> &'static str {
        match self {
            Self::Ack => "stored",
            Self::RequestTooLarge | Self::ExtractedTooLarge => {
                "the request exceeds a series_parquet size budget; split the batch upstream"
            }
            Self::RowTooLarge => {
                "a record of the request exceeds ingress.max_row_bytes; shorten its values in \
                 the producer or raise the limit"
            }
            Self::TooManySeries => {
                "the request carries more distinct series than ingress.max_series_per_request; \
                 split the batch upstream or raise the limit"
            }
            Self::TokenTooLarge => {
                "the request's routing context exceeds what series_parquet reserves for it; the \
                 pipeline route is too deep"
            }
            Self::TooDeep => {
                "the request nests values deeper than ingress.max_nesting_depth; flatten them \
                 in the producer"
            }
            Self::Invalid => "the request content is invalid; fix the producer",
            Self::Unsupported => {
                "the request carries data series_parquet does not store; route it elsewhere"
            }
            Self::Storage => {
                "series_parquet could not write the block holding this request to object \
                 storage; retry the request"
            }
            Self::Shutdown => {
                "series_parquet shut down before the request was stored; retry the request"
            }
            Self::Internal => {
                "series_parquet hit an internal error handling the request; retry the request"
            }
        }
    }

    /// The reason sentence a sender is told about one failed request: what
    /// was refused or failed, against which limit, and what to do about it.
    ///
    /// Chosen by the error's outcome, so the sentence always agrees with the
    /// nack it travels on; each sentence is one of the small [`Display`]
    /// structs below. Any text taken from the error itself is [`sanitized`],
    /// because an error can quote request content and the reason travels back
    /// to the producer.
    pub(super) fn explain(error: &lake::Error) -> String {
        match error {
            lake::Error::Refused(lake::RefuseReason::RequestTooLarge(excess)) => {
                TooLarge(excess).to_string()
            }
            lake::Error::Refused(lake::RefuseReason::Unsupported(what)) => {
                NotStored(what).to_string()
            }
            lake::Error::Refused(lake::RefuseReason::TooManySeries { observed, limit }) => {
                format!(
                    "request carries at least {observed} distinct series, more than \
                     ingress.max_series_per_request ({limit}); split the batch upstream or raise \
                     the limit"
                )
            }
            lake::Error::Refused(lake::RefuseReason::TokenTooLarge { observed, limit }) => {
                format!(
                    "the request's completion token, its routing context, holds {observed} \
                     bytes, more than the {limit}-byte allowance a block reserves for one; the \
                     pipeline route to series_parquet is too deep, remove subscribing nodes \
                     from it"
                )
            }
            lake::Error::Refused(lake::RefuseReason::TooDeep(limit)) => TooDeep(*limit).to_string(),
            lake::Error::Refused(lake::RefuseReason::Invalid(detail)) => {
                InvalidContent(detail).to_string()
            }
            error if Self::of(error) == Self::Storage => StorageFailed(error).to_string(),
            error => InternalFailure(error).to_string(),
        }
    }
}

/// A size refusal: the budget, the measured size and the limit.
struct TooLarge<'a>(&'a lake::Excess);

impl Display for TooLarge<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let (what, setting) = budget_names(self.0.budget);
        let lake::Excess {
            observed, limit, ..
        } = *self.0;
        // One record or value is refused on every split, so only the
        // request-wide budgets can be met by splitting.
        let remedy = match self.0.budget {
            lake::SizeBudget::Row | lake::SizeBudget::Cell => "shorten the value in the producer",
            lake::SizeBudget::Request | lake::SizeBudget::Extracted | lake::SizeBudget::Table => {
                "split the batch upstream"
            }
        };
        write!(
            f,
            "{what} of {observed} bytes exceeds {setting} ({limit} bytes); {remedy} or raise \
             the limit"
        )
    }
}

/// Content no dataset stores: traces, exemplars under `metrics.exemplars:
/// reject`, or a metric point kind under `unsupported: reject`.
struct NotStored<'a>(&'a str);

impl Display for NotStored<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.0 {
            "traces" => f.write_str(
                "traces are not stored by series_parquet; route traces to another exporter",
            ),
            "exemplars" => f.write_str(
                "exemplars are not stored by series_parquet, and metrics.exemplars: reject \
                 refuses a request that carries them; set metrics.exemplars: drop (the \
                 default) to store the points without their exemplars, or route the request \
                 to another exporter",
            ),
            kind => write!(
                f,
                "{} metric points are not stored by series_parquet under unsupported: reject; \
                 set unsupported: drop to keep the supported points, or route them to another \
                 exporter",
                sanitized(kind)
            ),
        }
    }
}

/// A value nested deeper than `ingress.max_nesting_depth`, the limit carried.
struct TooDeep(usize);

impl Display for TooDeep {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "a nested value exceeds ingress.max_nesting_depth ({}); flatten it in the producer \
             or raise the limit",
            self.0
        )
    }
}

/// Malformed request content, with the rule it broke.
struct InvalidContent<'a>(&'a str);

impl Display for InvalidContent<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid request content: {}; fix the producer",
            sanitized(self.0)
        )
    }
}

/// Why a write to object storage failed, in a closed classification: the
/// class a producer is told and a failed flush is counted under.
#[derive(Clone, Copy, Debug, PartialEq, Eq, AttributeEnum)]
pub(super) enum WriteFailure {
    /// The store kept failing, or did not answer, until the retry deadline.
    Deadline,
    /// The store refused the write for a reason no retry cures: credentials,
    /// permissions, or a missing bucket or path.
    PermanentStorage,
    /// The write was cancelled.
    Cancelled,
    /// Encoding the block failed.
    Encode,
    /// A writer invariant failed, or the write task was lost.
    Internal,
}

impl WriteFailure {
    /// The class of a failed write.
    ///
    /// A retryable storage error that ends a flush is one the retry deadline
    /// stopped. The Parquet writer's `External` error is how it wraps what the
    /// store returned, so it is a storage failure, never an encoding one.
    pub(super) fn of(error: &lake::Error) -> Self {
        match error {
            error if error.is_cancelled() => Self::Cancelled,
            lake::Error::Transient(lake::TransientError::DeadlineExceeded { .. }) => Self::Deadline,
            lake::Error::Transient(lake::TransientError::AbortFailed { source, .. }) => {
                Self::of(source)
            }
            error if error.is_retryable() => Self::Deadline,
            lake::Error::Transient(_)
            | lake::Error::Internal(lake::InternalError::Parquet(
                parquet::errors::ParquetError::External(_),
            )) => Self::PermanentStorage,
            lake::Error::Internal(
                lake::InternalError::Arrow(_) | lake::InternalError::Parquet(_),
            ) => Self::Encode,
            lake::Error::Internal(lake::InternalError::Invariant(_)) | lake::Error::Refused(_) => {
                Self::Internal
            }
        }
    }

    /// Stable machine token of the class: the `error.type` label of
    /// `block.write_failures` and the `error_type` log field.
    pub(super) fn label(self) -> &'static str {
        self.as_str()
    }

    /// The words a producer is told in place of the store's own error text,
    /// which names the endpoint, bucket and key layout.
    fn phrase(self) -> &'static str {
        match self {
            Self::Deadline => "unavailable",
            Self::PermanentStorage => "rejected by the store",
            Self::Cancelled => "cancelled",
            Self::Encode => "encoding failed",
            Self::Internal => "internal error",
        }
    }
}

/// A failed write to object storage, told to the requests it held: a fixed
/// sentence and the [`WriteFailure`] class. The error itself is logged, never
/// sent.
pub(super) struct StorageFailed<'a>(pub(super) &'a lake::Error);

impl Display for StorageFailed<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "could not write to object storage ({}); retry the request",
            WriteFailure::of(self.0).phrase()
        )
    }
}

/// A failure of this exporter while handling one request.
struct InternalFailure<'a>(&'a lake::Error);

impl Display for InternalFailure<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "series_parquet internal error: {}; the request is not at fault, retry it",
            sanitized(&self.0.to_string())
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every size budget a refusal can name, in declaration order.
    const BUDGETS: [lake::SizeBudget; 5] = [
        lake::SizeBudget::Request,
        lake::SizeBudget::Extracted,
        lake::SizeBudget::Row,
        lake::SizeBudget::Cell,
        lake::SizeBudget::Table,
    ];

    /// Scenario: error text carrying a line feed, an ANSI escape, a right-to-left override and a
    /// zero-width space, rendered for a nack reason and for a refusal log line.
    /// Guarantees: the reason replaces control and format (Unicode Cf) characters with spaces
    /// and the log line escapes them, so request text can neither break nor reorder either.
    #[test]
    fn request_text_cannot_break_or_reorder_a_reason_or_a_log_line() {
        let text = "a\nb\x1bc\u{202e}d\u{200b}e";
        assert_eq!(sanitized(text), "a b c d e");
        assert!(
            logged_error(&lake::Error::invalid(text)).contains("a\\nb\\u{1b}c\\u{202e}d\\u{200b}e"),
            "{}",
            logged_error(&lake::Error::invalid(text))
        );
    }

    /// Scenario: one error of every lake refusal reason and of each non-refusal class.
    /// Guarantees: error -> outcome -> (nack cause, permanent, `error.type`) is the pinned table;
    /// only a refusal of the request's own content is a permanent `Refused` nack.
    #[test]
    fn every_lake_error_maps_to_one_outcome_and_one_nack() {
        let store = || {
            lake::Error::from(object_store::Error::Generic {
                store: "test",
                source: "unreachable".into(),
            })
        };
        let mut table: Vec<(lake::Error, Outcome, NackCause, bool, &str)> = BUDGETS
            .into_iter()
            .map(|budget| {
                let (outcome, label) = match budget {
                    lake::SizeBudget::Request => (Outcome::RequestTooLarge, "request_too_large"),
                    lake::SizeBudget::Extracted | lake::SizeBudget::Table => {
                        (Outcome::ExtractedTooLarge, "extracted_too_large")
                    }
                    lake::SizeBudget::Row | lake::SizeBudget::Cell => {
                        (Outcome::RowTooLarge, "row_too_large")
                    }
                };
                (
                    lake::Error::too_large(budget, 2, 1),
                    outcome,
                    NackCause::Refused,
                    true,
                    label,
                )
            })
            .collect();
        table.extend([
            (
                lake::Error::Refused(lake::RefuseReason::TooManySeries {
                    observed: 11,
                    limit: 10,
                }),
                Outcome::TooManySeries,
                NackCause::Refused,
                true,
                "too_many_series",
            ),
            (
                lake::Error::Refused(lake::RefuseReason::TokenTooLarge {
                    observed: 5000,
                    limit: 4096,
                }),
                Outcome::TokenTooLarge,
                NackCause::Refused,
                true,
                "token_too_large",
            ),
            (
                lake::Error::Refused(lake::RefuseReason::TooDeep(32)),
                Outcome::TooDeep,
                NackCause::Refused,
                true,
                "too_deep",
            ),
            (
                lake::Error::Refused(lake::RefuseReason::Unsupported("summary".into())),
                Outcome::Unsupported,
                NackCause::Refused,
                true,
                "unsupported",
            ),
            (
                lake::Error::invalid("undecodable pdata"),
                Outcome::Invalid,
                NackCause::Refused,
                true,
                "invalid",
            ),
            (
                lake::Error::Refused(lake::RefuseReason::BlockFull),
                Outcome::Internal,
                NackCause::Unspecified,
                false,
                "internal",
            ),
            (
                lake::Error::Refused(lake::RefuseReason::TooManyRequests),
                Outcome::Internal,
                NackCause::Unspecified,
                false,
                "internal",
            ),
            (
                store(),
                Outcome::Storage,
                NackCause::Unspecified,
                false,
                "storage",
            ),
            (
                lake::Error::cancelled(None),
                Outcome::Storage,
                NackCause::Unspecified,
                false,
                "storage",
            ),
            (
                lake::Error::Transient(lake::TransientError::DeadlineExceeded {
                    attempts: 2,
                    last: Some(Box::new(store())),
                }),
                Outcome::Storage,
                NackCause::Unspecified,
                false,
                "storage",
            ),
            (
                lake::Error::Transient(lake::TransientError::AbortFailed {
                    source: Box::new(lake::Error::internal("encode")),
                    abort_error: "timed out".into(),
                }),
                Outcome::Storage,
                NackCause::Unspecified,
                false,
                "storage",
            ),
            (
                lake::Error::internal("flush failed"),
                Outcome::Internal,
                NackCause::Unspecified,
                false,
                "internal",
            ),
            (
                lake::Error::from(arrow::error::ArrowError::ComputeError("x".into())),
                Outcome::Internal,
                NackCause::Unspecified,
                false,
                "internal",
            ),
            (
                lake::Error::from(parquet::errors::ParquetError::General("x".into())),
                Outcome::Internal,
                NackCause::Unspecified,
                false,
                "internal",
            ),
        ]);
        for (error, outcome, cause, permanent, label) in &table {
            let got = Outcome::of(error);
            assert_eq!(got, *outcome, "{error:?}");
            assert_eq!(got.nack_cause(), Some(*cause), "{error:?}");
            assert_eq!(got.refused(), *permanent, "{error:?}");
            assert_eq!(got.label(), *label, "{error:?}");
            let sized = matches!(
                outcome,
                Outcome::RequestTooLarge | Outcome::ExtractedTooLarge | Outcome::RowTooLarge
            );
            assert_eq!(excess(error).is_some(), sized, "{error:?}");
        }
        assert_eq!(Outcome::Ack.nack_cause(), None);
        assert_eq!(
            Outcome::Shutdown.nack_cause(),
            Some(NackCause::NodeShutdown)
        );
        assert!(!Outcome::Shutdown.refused());
        assert_eq!(Outcome::Shutdown.label(), "shutdown");
        assert_eq!(Outcome::Ack.label(), "ack");
    }

    /// Scenario: the reason sentence for one error of every shape a sender can be told about.
    /// Guarantees: every sentence is byte for byte the pinned one and stays on one line.
    #[test]
    fn every_reason_sentence_is_pinned() {
        let sized = |budget, observed| {
            lake::Error::Refused(lake::RefuseReason::RequestTooLarge(lake::Excess {
                budget,
                observed,
                limit: 5,
            }))
        };
        let refused = |reason| lake::Error::Refused(reason);
        let cases = [
            (
                sized(lake::SizeBudget::Request, 10),
                "request of 10 bytes exceeds ingress.max_request_bytes (5 bytes); split the batch upstream or raise the limit",
            ),
            (
                sized(lake::SizeBudget::Extracted, 10),
                "extracted request of 10 bytes exceeds ingress.max_extracted_bytes (5 bytes); split the batch upstream or raise the limit",
            ),
            (
                sized(lake::SizeBudget::Row, 10),
                "extracted row of 10 bytes exceeds ingress.max_row_bytes (5 bytes); shorten the value in the producer or raise the limit",
            ),
            (
                sized(lake::SizeBudget::Cell, 10),
                "attribute value of 10 bytes exceeds ingress.max_row_bytes (5 bytes); shorten the value in the producer or raise the limit",
            ),
            (
                sized(lake::SizeBudget::Table, 10),
                "decoded attribute table of 10 bytes exceeds ingress.max_extracted_bytes (5 bytes); split the batch upstream or raise the limit",
            ),
            (
                refused(lake::RefuseReason::TooManySeries {
                    observed: 11,
                    limit: 10,
                }),
                "request carries at least 11 distinct series, more than ingress.max_series_per_request (10); split the batch upstream or raise the limit",
            ),
            (
                refused(lake::RefuseReason::TokenTooLarge {
                    observed: 5000,
                    limit: 4096,
                }),
                "the request's completion token, its routing context, holds 5000 bytes, more than the 4096-byte allowance a block reserves for one; the pipeline route to series_parquet is too deep, remove subscribing nodes from it",
            ),
            (
                refused(lake::RefuseReason::Unsupported("traces".into())),
                "traces are not stored by series_parquet; route traces to another exporter",
            ),
            (
                refused(lake::RefuseReason::Unsupported("exemplars".into())),
                "exemplars are not stored by series_parquet, and metrics.exemplars: reject refuses a request that carries them; set metrics.exemplars: drop (the default) to store the points without their exemplars, or route the request to another exporter",
            ),
            (
                refused(lake::RefuseReason::Unsupported("Summary".into())),
                "Summary metric points are not stored by series_parquet under unsupported: reject; set unsupported: drop to keep the supported points, or route them to another exporter",
            ),
            (
                refused(lake::RefuseReason::TooDeep(8)),
                "a nested value exceeds ingress.max_nesting_depth (8); flatten it in the producer or raise the limit",
            ),
            (
                lake::Error::invalid("duplicate\nkey"),
                "invalid request content: duplicate key; fix the producer",
            ),
            (
                lake::Error::cancelled(None),
                "could not write to object storage (cancelled); retry the request",
            ),
            (
                lake::Error::Transient(lake::TransientError::DeadlineExceeded {
                    attempts: 2,
                    last: None,
                }),
                "could not write to object storage (unavailable); retry the request",
            ),
            (
                lake::Error::internal("a\nb"),
                "series_parquet internal error: internal: a b; the request is not at fault, retry it",
            ),
            (
                refused(lake::RefuseReason::BlockFull),
                "series_parquet internal error: refused: BlockFull; the request is not at fault, retry it",
            ),
            (
                refused(lake::RefuseReason::TooManyRequests),
                "series_parquet internal error: refused: TooManyRequests; the request is not at fault, retry it",
            ),
        ];
        for (error, sentence) in &cases {
            assert_eq!(Outcome::explain(error), *sentence, "{error:?}");
        }
    }

    /// Scenario: a failed write of every `WriteFailure` class, from errors naming an endpoint, a
    /// bucket and a key.
    /// Guarantees: each gets its class and the fixed sentence; no text of the store's error reaches
    /// the producer.
    #[test]
    fn a_failed_write_is_told_as_a_fixed_sentence_and_a_class() {
        let secret = "http://10.0.0.7:9000/bucket/v=1/signal=logs/key.parquet";
        let generic = || {
            lake::Error::from(object_store::Error::Generic {
                store: "S3",
                source: format!("{secret}: 503 SlowDown").into(),
            })
        };
        let denied = || object_store::Error::PermissionDenied {
            path: secret.into(),
            source: "AccessDenied".into(),
        };
        let cases: Vec<(lake::Error, WriteFailure, &str)> = vec![
            (generic(), WriteFailure::Deadline, "unavailable"),
            (
                lake::Error::Transient(lake::TransientError::DeadlineExceeded {
                    attempts: 3,
                    last: Some(Box::new(generic())),
                }),
                WriteFailure::Deadline,
                "unavailable",
            ),
            (
                lake::Error::cancelled(None),
                WriteFailure::Cancelled,
                "cancelled",
            ),
            (
                lake::Error::cancelled(Some(secret.into())),
                WriteFailure::Cancelled,
                "cancelled",
            ),
            (
                lake::Error::from(denied()),
                WriteFailure::PermanentStorage,
                "rejected by the store",
            ),
            (
                lake::Error::from(object_store::Error::NotFound {
                    path: secret.into(),
                    source: "NoSuchBucket".into(),
                }),
                WriteFailure::PermanentStorage,
                "rejected by the store",
            ),
            (
                lake::Error::from(parquet::errors::ParquetError::External(Box::new(denied()))),
                WriteFailure::PermanentStorage,
                "rejected by the store",
            ),
            (
                lake::Error::Transient(lake::TransientError::AbortFailed {
                    source: Box::new(lake::Error::from(denied())),
                    abort_error: secret.into(),
                }),
                WriteFailure::PermanentStorage,
                "rejected by the store",
            ),
            (
                lake::Error::from(parquet::errors::ParquetError::General(secret.into())),
                WriteFailure::Encode,
                "encoding failed",
            ),
            (
                lake::Error::internal(secret),
                WriteFailure::Internal,
                "internal error",
            ),
        ];
        for (error, class, phrase) in &cases {
            assert_eq!(WriteFailure::of(error), *class, "{error:?}");
            assert_eq!(
                StorageFailed(error).to_string(),
                format!("could not write to object storage ({phrase}); retry the request"),
                "{error:?}"
            );
        }
        assert_eq!(
            StorageFailed(&generic()).to_string(),
            "could not write to object storage (unavailable); retry the request"
        );
    }
}
