// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Schema-aware validation of the protobuf wire framing of an OTLP request.
//!
//! The byte views read a request lazily and treat a damaged nested message as
//! absent, so a body that is broken anywhere below its top level converts into
//! a request carrying fewer rows than it holds, or none, without an error.
//! [`validate_request`] walks every length-delimited field that the OTLP
//! schema defines as a sub-message and checks the framing of each nested
//! message the same way the top level is checked, so such a body is refused
//! instead.
//!
//! What is checked, in every message of the request, as prost 0.14 checks it:
//! - every field key is a well-formed varint inside the protobuf key range,
//!   with a non-zero field number and a wire type protobuf defines;
//! - every varint terminates within ten bytes and fits a `u64`, and every
//!   fixed-width or length-delimited value lies inside the enclosing message;
//! - a field the schema knows arrives with the wire type the schema gives it
//!   (a repeated scalar may also arrive packed);
//! - a packed `fixed64`/`double` field holds a whole number of elements, and a
//!   packed varint field holds only well-formed varints;
//! - `AnyValue` arrays and key-value lists nest at most
//!   [`MAX_ANY_VALUE_NESTING_DEPTH`] levels.
//!
//! A singular field or a oneof may occur more than once, as protobuf allows:
//! the byte views read it as prost decodes it (the last scalar or oneof
//! member wins, the occurrences of a message merge), so it is not checked.
//! A known field with another wire type is refused, as prost refuses it;
//! protobuf-go would skip it as an unknown field instead.
//!
//! A field the schema does not know keeps protobuf skip semantics: its
//! framing is checked and its content is skipped. An unknown group (wire
//! types 3 and 4) is skipped when it is balanced (closed by an end key of its
//! own field number, nested groups included, each counted against the same
//! nesting limit), and a stray or mismatched end group is refused.
//! Content is not checked: no value is range-checked and a `string` field may
//! hold invalid UTF-8. The conversion to OTAP records replaces it with U+FFFD
//! in a top-level string, and refuses the request when the string or key sits
//! inside an array or key-value list value.
//!
//! Cost: each byte of the request is read once, by the innermost message that
//! holds it, so the walk is linear in the body size; it allocates nothing, and
//! its recursion depth is bounded by the schema's fixed levels plus three per
//! `AnyValue` nesting level, or one per unknown group level.

use super::decode::{
    END_GROUP, START_GROUP, SkipError, read_key, read_varint, skip_group, value_range,
};
use crate::error::Error;
use crate::proto::consts::field_num::{common, logs, metrics, resource, traces};
use crate::proto::consts::wire_types::{FIXED32, FIXED64, LEN, VARINT};

/// The deepest nesting of `AnyValue` arrays and key-value lists an OTLP
/// request may carry and still pass [`validate_request`].
///
/// A top-level attribute value or log body that is an array or a key-value
/// list is one level; each array or list inside it adds one. Scalars add
/// none. The limit bounds the recursion of this walk and of the conversion
/// that reads the checked body, not content, so it is set no lower than any
/// in-process consumer's own limit. prost's decoder stops at 100 message
/// levels, about 30 `AnyValue` levels, but no exporter decodes a checked body
/// with prost; a node that forwards it as bytes leaves its receiver's limit
/// to that receiver (protobuf-go accepts 10,000 levels).
pub const MAX_ANY_VALUE_NESTING_DEPTH: usize = super::decode::MAX_NESTING_DEPTH;

/// A broken frame, as [`Error::InvalidOtlpWireFormat`] names it.
pub use super::decode::WireProblem;

/// The OTLP message types the validator descends into.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Message {
    ExportLogsServiceRequest,
    ResourceLogs,
    ScopeLogs,
    LogRecord,
    ExportMetricsServiceRequest,
    ResourceMetrics,
    ScopeMetrics,
    Metric,
    Gauge,
    Sum,
    Histogram,
    ExponentialHistogram,
    Summary,
    NumberDataPoint,
    HistogramDataPoint,
    ExponentialHistogramDataPoint,
    Buckets,
    SummaryDataPoint,
    ValueAtQuantile,
    Exemplar,
    ExportTraceServiceRequest,
    ResourceSpans,
    ScopeSpans,
    Span,
    Event,
    Link,
    Status,
    Resource,
    EntityRef,
    InstrumentationScope,
    KeyValue,
    AnyValue,
    ArrayValue,
    KeyValueList,
}

/// What the schema says about one field number of one message.
#[derive(Clone, Copy)]
struct Field {
    kind: Kind,
    /// Set for a singular field or a oneof member, which the tests use to
    /// check that the byte views read its repeats as prost does.
    #[cfg(test)]
    singular: Option<Singular>,
}

/// How a field is encoded.
#[derive(Clone, Copy)]
enum Kind {
    /// A sub-message, always length-delimited.
    Message(Message),
    /// A value with this wire type that protobuf never packs; `string` and
    /// `bytes` are `LEN`.
    Scalar(u64),
    /// A repeated scalar with this element wire type, packed (`LEN`) or not.
    Packed(u64),
    /// Not in the schema: framing checked, content skipped.
    Unknown,
}

/// A singular field or a member of a oneof.
#[cfg(test)]
#[derive(Clone, Copy)]
struct Singular {
    /// The field's name, or the oneof's.
    name: &'static str,
    /// For a oneof member, the field number of the oneof's first member, which
    /// every member uses as its slot in the per-message bitmask; otherwise
    /// the field's own number is its slot.
    oneof: Option<u64>,
}

/// A singular field.
const fn one(kind: Kind, name: &'static str) -> Field {
    let _ = name;
    Field {
        kind,
        #[cfg(test)]
        singular: Some(Singular { name, oneof: None }),
    }
}

/// A member of the oneof `name` whose first member is field `first`.
const fn member(kind: Kind, name: &'static str, first: u64) -> Field {
    let _ = (name, first);
    Field {
        kind,
        #[cfg(test)]
        singular: Some(Singular {
            name,
            oneof: Some(first),
        }),
    }
}

/// A repeated field.
const fn many(kind: Kind) -> Field {
    Field {
        kind,
        #[cfg(test)]
        singular: None,
    }
}

/// A field number the message does not define.
const UNKNOWN: Field = many(Kind::Unknown);

impl Message {
    /// The message's name as the OTLP schema spells it.
    const fn name(self) -> &'static str {
        match self {
            Self::ExportLogsServiceRequest => "ExportLogsServiceRequest",
            Self::ResourceLogs => "ResourceLogs",
            Self::ScopeLogs => "ScopeLogs",
            Self::LogRecord => "LogRecord",
            Self::ExportMetricsServiceRequest => "ExportMetricsServiceRequest",
            Self::ResourceMetrics => "ResourceMetrics",
            Self::ScopeMetrics => "ScopeMetrics",
            Self::Metric => "Metric",
            Self::Gauge => "Gauge",
            Self::Sum => "Sum",
            Self::Histogram => "Histogram",
            Self::ExponentialHistogram => "ExponentialHistogram",
            Self::Summary => "Summary",
            Self::NumberDataPoint => "NumberDataPoint",
            Self::HistogramDataPoint => "HistogramDataPoint",
            Self::ExponentialHistogramDataPoint => "ExponentialHistogramDataPoint",
            Self::Buckets => "ExponentialHistogramDataPoint.Buckets",
            Self::SummaryDataPoint => "SummaryDataPoint",
            Self::ValueAtQuantile => "SummaryDataPoint.ValueAtQuantile",
            Self::Exemplar => "Exemplar",
            Self::ExportTraceServiceRequest => "ExportTraceServiceRequest",
            Self::ResourceSpans => "ResourceSpans",
            Self::ScopeSpans => "ScopeSpans",
            Self::Span => "Span",
            Self::Event => "Span.Event",
            Self::Link => "Span.Link",
            Self::Status => "Status",
            Self::Resource => "Resource",
            Self::EntityRef => "EntityRef",
            Self::InstrumentationScope => "InstrumentationScope",
            Self::KeyValue => "KeyValue",
            Self::AnyValue => "AnyValue",
            Self::ArrayValue => "ArrayValue",
            Self::KeyValueList => "KeyValueList",
        }
    }

    /// Whether entering this message is one more `AnyValue` nesting level.
    const fn is_value_container(self) -> bool {
        matches!(self, Self::ArrayValue | Self::KeyValueList)
    }

    /// The schema of field `num` of this message.
    #[inline]
    fn field(self, num: u64) -> Field {
        use Kind::{Message as Sub, Packed, Scalar};
        use Message as M;
        match self {
            M::ExportLogsServiceRequest => match num {
                logs::LOGS_DATA_RESOURCE => many(Sub(M::ResourceLogs)),
                _ => UNKNOWN,
            },
            M::ResourceLogs => match num {
                logs::RESOURCE_LOGS_RESOURCE => one(Sub(M::Resource), "resource"),
                logs::RESOURCE_LOGS_SCOPE_LOGS => many(Sub(M::ScopeLogs)),
                logs::RESOURCE_LOGS_SCHEMA_URL => one(Scalar(LEN), "schema_url"),
                _ => UNKNOWN,
            },
            M::ScopeLogs => match num {
                logs::SCOPE_LOG_SCOPE => one(Sub(M::InstrumentationScope), "scope"),
                logs::SCOPE_LOGS_LOG_RECORDS => many(Sub(M::LogRecord)),
                logs::SCOPE_LOGS_SCHEMA_URL => one(Scalar(LEN), "schema_url"),
                _ => UNKNOWN,
            },
            M::LogRecord => match num {
                logs::LOG_RECORD_TIME_UNIX_NANO => one(Scalar(FIXED64), "time_unix_nano"),
                logs::LOG_RECORD_OBSERVED_TIME_UNIX_NANO => {
                    one(Scalar(FIXED64), "observed_time_unix_nano")
                }
                logs::LOG_RECORD_SEVERITY_NUMBER => one(Scalar(VARINT), "severity_number"),
                logs::LOG_RECORD_SEVERITY_TEXT => one(Scalar(LEN), "severity_text"),
                logs::LOG_RECORD_BODY => one(Sub(M::AnyValue), "body"),
                logs::LOG_RECORD_ATTRIBUTES => many(Sub(M::KeyValue)),
                logs::LOG_RECORD_DROPPED_ATTRIBUTES_COUNT => {
                    one(Scalar(VARINT), "dropped_attributes_count")
                }
                logs::LOG_RECORD_FLAGS => one(Scalar(FIXED32), "flags"),
                logs::LOG_RECORD_TRACE_ID => one(Scalar(LEN), "trace_id"),
                logs::LOG_RECORD_SPAN_ID => one(Scalar(LEN), "span_id"),
                logs::LOG_RECORD_EVENT_NAME => one(Scalar(LEN), "event_name"),
                _ => UNKNOWN,
            },
            M::ExportMetricsServiceRequest => match num {
                metrics::METRICS_DATA_RESOURCE_METRICS => many(Sub(M::ResourceMetrics)),
                _ => UNKNOWN,
            },
            M::ResourceMetrics => match num {
                metrics::RESOURCE_METRICS_RESOURCE => one(Sub(M::Resource), "resource"),
                metrics::RESOURCE_METRICS_SCOPE_METRICS => many(Sub(M::ScopeMetrics)),
                metrics::RESOURCE_METRICS_SCHEMA_URL => one(Scalar(LEN), "schema_url"),
                _ => UNKNOWN,
            },
            M::ScopeMetrics => match num {
                metrics::SCOPE_METRICS_SCOPE => one(Sub(M::InstrumentationScope), "scope"),
                metrics::SCOPE_METRICS_METRICS => many(Sub(M::Metric)),
                metrics::SCOPE_METRICS_SCHEMA_URL => one(Scalar(LEN), "schema_url"),
                _ => UNKNOWN,
            },
            M::Metric => {
                // The `data` oneof.
                let data = |child| member(Sub(child), "data", metrics::METRIC_GAUGE);
                match num {
                    metrics::METRIC_NAME => one(Scalar(LEN), "name"),
                    metrics::METRIC_DESCRIPTION => one(Scalar(LEN), "description"),
                    metrics::METRIC_UNIT => one(Scalar(LEN), "unit"),
                    metrics::METRIC_GAUGE => data(M::Gauge),
                    metrics::METRIC_SUM => data(M::Sum),
                    metrics::METRIC_HISTOGRAM => data(M::Histogram),
                    metrics::METRIC_EXPONENTIAL_HISTOGRAM => data(M::ExponentialHistogram),
                    metrics::METRIC_SUMMARY => data(M::Summary),
                    metrics::METRIC_METADATA => many(Sub(M::KeyValue)),
                    _ => UNKNOWN,
                }
            }
            M::Gauge => match num {
                metrics::GAUGE_DATA_POINTS => many(Sub(M::NumberDataPoint)),
                _ => UNKNOWN,
            },
            M::Sum => match num {
                metrics::SUM_DATA_POINTS => many(Sub(M::NumberDataPoint)),
                metrics::SUM_AGGREGATION_TEMPORALITY => {
                    one(Scalar(VARINT), "aggregation_temporality")
                }
                metrics::SUM_IS_MONOTONIC => one(Scalar(VARINT), "is_monotonic"),
                _ => UNKNOWN,
            },
            M::Histogram => match num {
                metrics::HISTOGRAM_DATA_POINTS => many(Sub(M::HistogramDataPoint)),
                metrics::HISTOGRAM_AGGREGATION_TEMPORALITY => {
                    one(Scalar(VARINT), "aggregation_temporality")
                }
                _ => UNKNOWN,
            },
            M::ExponentialHistogram => match num {
                metrics::EXPONENTIAL_HISTOGRAM_DATA_POINTS => {
                    many(Sub(M::ExponentialHistogramDataPoint))
                }
                metrics::EXPONENTIAL_HISTOGRAM_AGGREGATION_TEMPORALITY => {
                    one(Scalar(VARINT), "aggregation_temporality")
                }
                _ => UNKNOWN,
            },
            M::Summary => match num {
                metrics::SUMMARY_DATA_POINTS => many(Sub(M::SummaryDataPoint)),
                _ => UNKNOWN,
            },
            M::NumberDataPoint => {
                // The `value` oneof.
                let value = member(Scalar(FIXED64), "value", metrics::NUMBER_DP_AS_DOUBLE);
                match num {
                    metrics::NUMBER_DP_ATTRIBUTES => many(Sub(M::KeyValue)),
                    metrics::NUMBER_DP_START_TIME_UNIX_NANO => {
                        one(Scalar(FIXED64), "start_time_unix_nano")
                    }
                    metrics::NUMBER_DP_TIME_UNIX_NANO => one(Scalar(FIXED64), "time_unix_nano"),
                    metrics::NUMBER_DP_AS_DOUBLE | metrics::NUMBER_DP_AS_INT => value,
                    metrics::NUMBER_DP_EXEMPLARS => many(Sub(M::Exemplar)),
                    metrics::NUMBER_DP_FLAGS => one(Scalar(VARINT), "flags"),
                    _ => UNKNOWN,
                }
            }
            M::HistogramDataPoint => match num {
                metrics::HISTOGRAM_DP_ATTRIBUTES => many(Sub(M::KeyValue)),
                metrics::HISTOGRAM_DP_START_TIME_UNIX_NANO => {
                    one(Scalar(FIXED64), "start_time_unix_nano")
                }
                metrics::HISTOGRAM_DP_TIME_UNIX_NANO => one(Scalar(FIXED64), "time_unix_nano"),
                metrics::HISTOGRAM_DP_COUNT => one(Scalar(FIXED64), "count"),
                metrics::HISTOGRAM_DP_SUM => one(Scalar(FIXED64), "sum"),
                metrics::HISTOGRAM_DP_BUCKET_COUNTS | metrics::HISTOGRAM_DP_EXPLICIT_BOUNDS => {
                    many(Packed(FIXED64))
                }
                metrics::HISTOGRAM_DP_EXEMPLARS => many(Sub(M::Exemplar)),
                metrics::HISTOGRAM_DP_FLAGS => one(Scalar(VARINT), "flags"),
                metrics::HISTOGRAM_DP_MIN => one(Scalar(FIXED64), "min"),
                metrics::HISTOGRAM_DP_MAX => one(Scalar(FIXED64), "max"),
                _ => UNKNOWN,
            },
            M::ExponentialHistogramDataPoint => match num {
                metrics::EXP_HISTOGRAM_DP_ATTRIBUTES => many(Sub(M::KeyValue)),
                metrics::EXP_HISTOGRAM_DP_START_TIME_UNIX_NANO => {
                    one(Scalar(FIXED64), "start_time_unix_nano")
                }
                metrics::EXP_HISTOGRAM_DP_TIME_UNIX_NANO => one(Scalar(FIXED64), "time_unix_nano"),
                metrics::EXP_HISTOGRAM_DP_COUNT => one(Scalar(FIXED64), "count"),
                metrics::EXP_HISTOGRAM_DP_SUM => one(Scalar(FIXED64), "sum"),
                metrics::EXP_HISTOGRAM_DP_SCALE => one(Scalar(VARINT), "scale"),
                metrics::EXP_HISTOGRAM_DP_ZERO_COUNT => one(Scalar(FIXED64), "zero_count"),
                metrics::EXP_HISTOGRAM_DP_POSITIVE => one(Sub(M::Buckets), "positive"),
                metrics::EXP_HISTOGRAM_DP_NEGATIVE => one(Sub(M::Buckets), "negative"),
                metrics::EXP_HISTOGRAM_DP_FLAGS => one(Scalar(VARINT), "flags"),
                metrics::EXP_HISTOGRAM_DP_EXEMPLARS => many(Sub(M::Exemplar)),
                metrics::EXP_HISTOGRAM_DP_MIN => one(Scalar(FIXED64), "min"),
                metrics::EXP_HISTOGRAM_DP_MAX => one(Scalar(FIXED64), "max"),
                metrics::EXP_HISTOGRAM_DP_ZERO_THRESHOLD => one(Scalar(FIXED64), "zero_threshold"),
                _ => UNKNOWN,
            },
            M::Buckets => match num {
                metrics::EXP_HISTOGRAM_BUCKET_OFFSET => one(Scalar(VARINT), "offset"),
                metrics::EXP_HISTOGRAM_BUCKET_BUCKET_COUNTS => many(Packed(VARINT)),
                _ => UNKNOWN,
            },
            M::SummaryDataPoint => match num {
                metrics::SUMMARY_DP_ATTRIBUTES => many(Sub(M::KeyValue)),
                metrics::SUMMARY_DP_START_TIME_UNIX_NANO => {
                    one(Scalar(FIXED64), "start_time_unix_nano")
                }
                metrics::SUMMARY_DP_TIME_UNIX_NANO => one(Scalar(FIXED64), "time_unix_nano"),
                metrics::SUMMARY_DP_COUNT => one(Scalar(FIXED64), "count"),
                metrics::SUMMARY_DP_SUM => one(Scalar(FIXED64), "sum"),
                metrics::SUMMARY_DP_QUANTILE_VALUES => many(Sub(M::ValueAtQuantile)),
                metrics::SUMMARY_DP_FLAGS => one(Scalar(VARINT), "flags"),
                _ => UNKNOWN,
            },
            M::ValueAtQuantile => match num {
                metrics::VALUE_AT_QUANTILE_QUANTILE => one(Scalar(FIXED64), "quantile"),
                metrics::VALUE_AT_QUANTILE_VALUE => one(Scalar(FIXED64), "value"),
                _ => UNKNOWN,
            },
            M::Exemplar => {
                // The `value` oneof.
                let value = member(Scalar(FIXED64), "value", metrics::EXEMPLAR_AS_DOUBLE);
                match num {
                    metrics::EXEMPLAR_FILTERED_ATTRIBUTES => many(Sub(M::KeyValue)),
                    metrics::EXEMPLAR_TIME_UNIX_NANO => one(Scalar(FIXED64), "time_unix_nano"),
                    metrics::EXEMPLAR_AS_DOUBLE | metrics::EXEMPLAR_AS_INT => value,
                    metrics::EXEMPLAR_SPAN_ID => one(Scalar(LEN), "span_id"),
                    metrics::EXEMPLAR_TRACE_ID => one(Scalar(LEN), "trace_id"),
                    _ => UNKNOWN,
                }
            }
            M::ExportTraceServiceRequest => match num {
                traces::TRACES_DATA_RESOURCE_SPANS => many(Sub(M::ResourceSpans)),
                _ => UNKNOWN,
            },
            M::ResourceSpans => match num {
                traces::RESOURCE_SPANS_RESOURCE => one(Sub(M::Resource), "resource"),
                traces::RESOURCE_SPANS_SCOPE_SPANS => many(Sub(M::ScopeSpans)),
                traces::RESOURCE_SPANS_SCHEMA_URL => one(Scalar(LEN), "schema_url"),
                _ => UNKNOWN,
            },
            M::ScopeSpans => match num {
                traces::SCOPE_SPANS_SCOPE => one(Sub(M::InstrumentationScope), "scope"),
                traces::SCOPE_SPANS_SPANS => many(Sub(M::Span)),
                traces::SCOPE_SPANS_SCHEMA_URL => one(Scalar(LEN), "schema_url"),
                _ => UNKNOWN,
            },
            M::Span => match num {
                traces::SPAN_TRACE_ID => one(Scalar(LEN), "trace_id"),
                traces::SPAN_SPAN_ID => one(Scalar(LEN), "span_id"),
                traces::SPAN_TRACE_STATE => one(Scalar(LEN), "trace_state"),
                traces::SPAN_PARENT_SPAN_ID => one(Scalar(LEN), "parent_span_id"),
                traces::SPAN_NAME => one(Scalar(LEN), "name"),
                traces::SPAN_KIND => one(Scalar(VARINT), "kind"),
                traces::SPAN_START_TIME_UNIX_NANO => one(Scalar(FIXED64), "start_time_unix_nano"),
                traces::SPAN_END_TIME_UNIX_NANO => one(Scalar(FIXED64), "end_time_unix_nano"),
                traces::SPAN_ATTRIBUTES => many(Sub(M::KeyValue)),
                traces::SPAN_DROPPED_ATTRIBUTES_COUNT => {
                    one(Scalar(VARINT), "dropped_attributes_count")
                }
                traces::SPAN_EVENTS => many(Sub(M::Event)),
                traces::SPAN_DROPPED_EVENTS_COUNT => one(Scalar(VARINT), "dropped_events_count"),
                traces::SPAN_LINKS => many(Sub(M::Link)),
                traces::SPAN_DROPPED_LINKS_COUNT => one(Scalar(VARINT), "dropped_links_count"),
                traces::SPAN_STATUS => one(Sub(M::Status), "status"),
                traces::SPAN_FLAGS => one(Scalar(FIXED32), "flags"),
                _ => UNKNOWN,
            },
            M::Event => match num {
                traces::SPAN_EVENT_TIME_UNIX_NANO => one(Scalar(FIXED64), "time_unix_nano"),
                traces::SPAN_EVENT_NAME => one(Scalar(LEN), "name"),
                traces::SPAN_EVENT_ATTRIBUTES => many(Sub(M::KeyValue)),
                traces::SPAN_EVENT_DROPPED_ATTRIBUTES_COUNTS => {
                    one(Scalar(VARINT), "dropped_attributes_count")
                }
                _ => UNKNOWN,
            },
            M::Link => match num {
                traces::SPAN_LINK_TRACE_ID => one(Scalar(LEN), "trace_id"),
                traces::SPAN_LINK_SPAN_ID => one(Scalar(LEN), "span_id"),
                traces::SPAN_LINK_TRACE_STATE => one(Scalar(LEN), "trace_state"),
                traces::SPAN_LINK_ATTRIBUTES => many(Sub(M::KeyValue)),
                traces::SPAN_LINK_DROPPED_ATTRIBUTES_COUNT => {
                    one(Scalar(VARINT), "dropped_attributes_count")
                }
                traces::SPAN_LINK_FLAGS => one(Scalar(FIXED32), "flags"),
                _ => UNKNOWN,
            },
            // Field 1 is the reserved `deprecated_code`, skipped as unknown.
            M::Status => match num {
                traces::SPAN_STATUS_MESSAGE => one(Scalar(LEN), "message"),
                traces::SPAN_STATUS_CODE => one(Scalar(VARINT), "code"),
                _ => UNKNOWN,
            },
            M::Resource => match num {
                resource::RESOURCE_ATTRIBUTES => many(Sub(M::KeyValue)),
                resource::RESOURCE_DROPPED_ATTRIBUTES_COUNT => {
                    one(Scalar(VARINT), "dropped_attributes_count")
                }
                resource::RESOURCE_ENTITY_REFS => many(Sub(M::EntityRef)),
                _ => UNKNOWN,
            },
            M::EntityRef => match num {
                common::ENTITY_REF_SCHEMA_URL => one(Scalar(LEN), "schema_url"),
                common::ENTITY_REF_TYPE => one(Scalar(LEN), "type"),
                common::ENTITY_REF_ID_KEYS | common::ENTITY_REF_DESCRIPTION_KEYS => {
                    many(Scalar(LEN))
                }
                _ => UNKNOWN,
            },
            M::InstrumentationScope => match num {
                common::INSTRUMENTATION_SCOPE_NAME => one(Scalar(LEN), "name"),
                common::INSTRUMENTATION_SCOPE_VERSION => one(Scalar(LEN), "version"),
                common::INSTRUMENTATION_SCOPE_ATTRIBUTES => many(Sub(M::KeyValue)),
                common::INSTRUMENTATION_DROPPED_ATTRIBUTES_COUNT => {
                    one(Scalar(VARINT), "dropped_attributes_count")
                }
                _ => UNKNOWN,
            },
            M::KeyValue => match num {
                common::KEY_VALUE_KEY => one(Scalar(LEN), "key"),
                common::KEY_VALUE_VALUE => one(Sub(M::AnyValue), "value"),
                _ => UNKNOWN,
            },
            // The `value` oneof's members may repeat: the view reads them as
            // prost does (the last member wins; `array_value` and
            // `kvlist_value` following themselves merge).
            M::AnyValue => match num {
                common::ANY_VALUE_STRING_VALUE | common::ANY_VALUE_BYTES_VALUE => many(Scalar(LEN)),
                common::ANY_VALUE_BOOL_VALUE | common::ANY_VALUE_INT_VALUE => many(Scalar(VARINT)),
                common::ANY_VALUE_DOUBLE_VALUE => many(Scalar(FIXED64)),
                common::ANY_VALUE_ARRAY_VALUE => many(Sub(M::ArrayValue)),
                common::ANY_VALUE_KVLIST_VALUE => many(Sub(M::KeyValueList)),
                _ => UNKNOWN,
            },
            M::ArrayValue => match num {
                common::ARRAY_VALUE_VALUES => many(Sub(M::AnyValue)),
                _ => UNKNOWN,
            },
            M::KeyValueList => match num {
                common::KEY_VALUE_LIST_VALUES => many(Sub(M::KeyValue)),
                _ => UNKNOWN,
            },
        }
    }
}

/// Validate the wire framing of `buf`, an OTLP message of type `root`, and of
/// every sub-message the schema defines inside it. See the module
/// documentation for what is checked.
///
/// # Errors
/// [`Error::InvalidOtlpWireFormat`] naming the problem, the innermost message
/// holding it and its byte offset in `buf`, or [`Error::OtlpNestingTooDeep`].
pub(crate) fn validate_request(buf: &[u8], root: Message) -> Result<(), Error> {
    walk(buf, 0, root, 0).map_err(|damage| match damage {
        Damage::Framing {
            problem,
            message,
            offset,
        } => Error::InvalidOtlpWireFormat {
            problem,
            message: message.name(),
            offset,
        },
        Damage::TooDeep { offset } => Error::OtlpNestingTooDeep {
            limit: MAX_ANY_VALUE_NESTING_DEPTH,
            offset,
        },
    })
}

/// Where and how a request is damaged: a small value, so the recursive walk
/// returns it cheaply and only the outermost call builds the crate error.
enum Damage {
    Framing {
        problem: WireProblem,
        message: Message,
        offset: usize,
    },
    TooDeep {
        offset: usize,
    },
}

/// Walk one message of type `message`; `base` is the offset of `buf` within
/// the request and `depth` the `AnyValue` nesting level `buf` is at.
fn walk(buf: &[u8], base: usize, message: Message, depth: usize) -> Result<(), Damage> {
    let fail = |problem: WireProblem, at: usize| Damage::Framing {
        problem,
        message,
        offset: base + at,
    };
    let mut pos = 0;
    while pos < buf.len() {
        let at = pos;
        let (field_num, wire_type, next) =
            read_key(buf, pos).map_err(|problem| fail(problem, at))?;
        let field = message.field(field_num);
        if wire_type == START_GROUP || wire_type == END_GROUP {
            if wire_type == END_GROUP {
                return Err(fail(WireProblem::StrayEndGroup, at));
            }
            if !matches!(field.kind, Kind::Unknown) {
                return Err(fail(WireProblem::WrongWireType, at));
            }
            pos = skip_group(buf, next, field_num, depth + 1, at).map_err(|skip| match skip {
                SkipError::Framing { problem, at } => fail(problem, at),
                SkipError::TooDeep { at } => Damage::TooDeep { offset: base + at },
            })?;
            continue;
        }
        let (start, end) =
            value_range(buf, wire_type, next).map_err(|problem| fail(problem, at))?;
        match field.kind {
            Kind::Unknown => {}
            Kind::Scalar(expected) => {
                if wire_type != expected {
                    return Err(fail(WireProblem::WrongWireType, at));
                }
            }
            Kind::Packed(element) => {
                if wire_type == LEN {
                    check_packed(&buf[start..end], element).map_err(|problem| fail(problem, at))?;
                } else if wire_type != element {
                    return Err(fail(WireProblem::WrongWireType, at));
                }
            }
            Kind::Message(child) => {
                if wire_type != LEN {
                    return Err(fail(WireProblem::WrongWireType, at));
                }
                let depth = depth + usize::from(child.is_value_container());
                if depth > MAX_ANY_VALUE_NESTING_DEPTH {
                    return Err(Damage::TooDeep { offset: base + at });
                }
                walk(&buf[start..end], base + start, child, depth)?;
            }
        }
        pos = end;
    }
    Ok(())
}

/// Check the payload of a packed repeated field of `element` wire type.
fn check_packed(payload: &[u8], element: u64) -> Result<(), WireProblem> {
    let whole = |width: usize, problem| {
        if payload.len().is_multiple_of(width) {
            Ok(())
        } else {
            Err(problem)
        }
    };
    match element {
        FIXED64 => whole(8, WireProblem::RaggedPackedFixed64),
        FIXED32 => whole(4, WireProblem::RaggedPackedFixed32),
        VARINT => {
            let mut pos = 0;
            while pos < payload.len() {
                let (_, next) =
                    read_varint(payload, pos).ok_or(WireProblem::TruncatedPackedVarint)?;
                pos = next;
            }
            Ok(())
        }
        other => unreachable!("packed elements are varints or fixed-width, not wire type {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
    use crate::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
    use crate::proto::opentelemetry::collector::trace::v1::ExportTraceServiceRequest;
    use crate::proto::opentelemetry::common::v1::{
        AnyValue, ArrayValue, EntityRef, InstrumentationScope, KeyValue, KeyValueList, any_value,
    };
    use crate::proto::opentelemetry::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use crate::proto::opentelemetry::metrics::v1::{
        Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
        HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary,
        SummaryDataPoint, exemplar, exponential_histogram_data_point::Buckets, metric,
        number_data_point, summary_data_point::ValueAtQuantile,
    };
    use crate::proto::opentelemetry::resource::v1::Resource;
    use crate::proto::opentelemetry::trace::v1::{
        ResourceSpans, ScopeSpans, Span, Status,
        span::{Event, Link},
    };
    use prost::Message as _;
    use prost::encoding::{WireType, encode_key, encode_varint};

    fn len_field(field: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        encode_key(field, WireType::LengthDelimited, &mut out);
        encode_varint(payload.len() as u64, &mut out);
        out.extend_from_slice(payload);
        out
    }

    fn string(value: &str) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }
    }

    /// Every kind of attribute value, nested arrays and lists included.
    fn attributes() -> Vec<KeyValue> {
        let scalars = vec![
            string("s"),
            AnyValue {
                value: Some(any_value::Value::BoolValue(true)),
            },
            AnyValue {
                value: Some(any_value::Value::IntValue(-3)),
            },
            AnyValue {
                value: Some(any_value::Value::DoubleValue(1.5)),
            },
            AnyValue {
                value: Some(any_value::Value::BytesValue(vec![1, 2])),
            },
        ];
        vec![
            KeyValue {
                key: "array".into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::ArrayValue(ArrayValue {
                        values: scalars.clone(),
                    })),
                }),
            },
            KeyValue {
                key: "map".into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::KvlistValue(KeyValueList {
                        values: vec![KeyValue {
                            key: "inner".into(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::ArrayValue(ArrayValue {
                                    values: scalars,
                                })),
                            }),
                        }],
                    })),
                }),
            },
            KeyValue {
                key: "k".into(),
                value: Some(string("v")),
            },
        ]
    }

    fn resource() -> Option<Resource> {
        Some(Resource {
            attributes: attributes(),
            dropped_attributes_count: 1,
            entity_refs: vec![EntityRef {
                schema_url: "u".into(),
                r#type: "service".into(),
                id_keys: vec!["service.name".into()],
                description_keys: vec!["d".into()],
            }],
        })
    }

    fn scope() -> Option<InstrumentationScope> {
        Some(InstrumentationScope {
            name: "n".into(),
            version: "v".into(),
            attributes: attributes(),
            dropped_attributes_count: 2,
        })
    }

    fn logs_request() -> Vec<u8> {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: resource(),
                scope_logs: vec![ScopeLogs {
                    scope: scope(),
                    log_records: vec![LogRecord {
                        time_unix_nano: 1,
                        observed_time_unix_nano: 2,
                        severity_number: 9,
                        severity_text: "INFO".into(),
                        body: Some(string("body")),
                        attributes: attributes(),
                        dropped_attributes_count: 3,
                        flags: 1,
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        event_name: "e".into(),
                    }],
                    schema_url: "s".into(),
                }],
                schema_url: "r".into(),
            }],
        }
        .encode_to_vec()
    }

    fn exemplars() -> Vec<Exemplar> {
        vec![
            Exemplar {
                filtered_attributes: attributes(),
                time_unix_nano: 1,
                span_id: vec![1; 8],
                trace_id: vec![2; 16],
                value: Some(exemplar::Value::AsDouble(0.5)),
            },
            Exemplar {
                value: Some(exemplar::Value::AsInt(-1)),
                ..Default::default()
            },
        ]
    }

    fn metrics_request() -> Vec<u8> {
        let number_point = |value| NumberDataPoint {
            attributes: attributes(),
            start_time_unix_nano: 1,
            time_unix_nano: 2,
            exemplars: exemplars(),
            flags: 1,
            value: Some(value),
        };
        let metric = |data, metadata| Metric {
            name: "m".into(),
            description: "d".into(),
            unit: "1".into(),
            metadata,
            data: Some(data),
        };
        let buckets = Some(Buckets {
            offset: -2,
            bucket_counts: vec![1, 300, 70_000],
        });
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: resource(),
                scope_metrics: vec![ScopeMetrics {
                    scope: scope(),
                    metrics: vec![
                        metric(
                            metric::Data::Gauge(Gauge {
                                data_points: vec![number_point(
                                    number_data_point::Value::AsDouble(1.0),
                                )],
                            }),
                            attributes(),
                        ),
                        metric(
                            metric::Data::Sum(Sum {
                                data_points: vec![number_point(number_data_point::Value::AsInt(
                                    -7,
                                ))],
                                aggregation_temporality: 2,
                                is_monotonic: true,
                            }),
                            vec![],
                        ),
                        metric(
                            metric::Data::Histogram(Histogram {
                                data_points: vec![HistogramDataPoint {
                                    attributes: attributes(),
                                    start_time_unix_nano: 1,
                                    time_unix_nano: 2,
                                    count: 6,
                                    sum: Some(3.0),
                                    bucket_counts: vec![1, 2, 3],
                                    explicit_bounds: vec![0.5, 1.5],
                                    exemplars: exemplars(),
                                    flags: 1,
                                    min: Some(0.1),
                                    max: Some(2.0),
                                }],
                                aggregation_temporality: 1,
                            }),
                            vec![],
                        ),
                        metric(
                            metric::Data::ExponentialHistogram(ExponentialHistogram {
                                data_points: vec![ExponentialHistogramDataPoint {
                                    attributes: attributes(),
                                    start_time_unix_nano: 1,
                                    time_unix_nano: 2,
                                    count: 9,
                                    sum: Some(4.0),
                                    scale: -1,
                                    zero_count: 1,
                                    positive: buckets.clone(),
                                    negative: buckets,
                                    flags: 1,
                                    exemplars: exemplars(),
                                    min: Some(0.0),
                                    max: Some(9.0),
                                    zero_threshold: 0.001,
                                }],
                                aggregation_temporality: 2,
                            }),
                            vec![],
                        ),
                        metric(
                            metric::Data::Summary(Summary {
                                data_points: vec![SummaryDataPoint {
                                    attributes: attributes(),
                                    start_time_unix_nano: 1,
                                    time_unix_nano: 2,
                                    count: 3,
                                    sum: 6.0,
                                    quantile_values: vec![ValueAtQuantile {
                                        quantile: 0.5,
                                        value: 2.0,
                                    }],
                                    flags: 1,
                                }],
                            }),
                            vec![],
                        ),
                    ],
                    schema_url: "s".into(),
                }],
                schema_url: "r".into(),
            }],
        }
        .encode_to_vec()
    }

    fn traces_request() -> Vec<u8> {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: resource(),
                scope_spans: vec![ScopeSpans {
                    scope: scope(),
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        trace_state: "t".into(),
                        parent_span_id: vec![3; 8],
                        flags: 1,
                        name: "span".into(),
                        kind: 2,
                        start_time_unix_nano: 1,
                        end_time_unix_nano: 2,
                        attributes: attributes(),
                        dropped_attributes_count: 1,
                        events: vec![Event {
                            time_unix_nano: 1,
                            name: "e".into(),
                            attributes: attributes(),
                            dropped_attributes_count: 1,
                        }],
                        dropped_events_count: 1,
                        links: vec![Link {
                            trace_id: vec![4; 16],
                            span_id: vec![5; 8],
                            trace_state: "l".into(),
                            attributes: attributes(),
                            dropped_attributes_count: 1,
                            flags: 1,
                        }],
                        dropped_links_count: 1,
                        status: Some(Status {
                            message: "ok".into(),
                            code: 1,
                        }),
                    }],
                    schema_url: "s".into(),
                }],
                schema_url: "r".into(),
            }],
        }
        .encode_to_vec()
    }

    fn requests() -> [(Message, Vec<u8>); 3] {
        [
            (Message::ExportLogsServiceRequest, logs_request()),
            (Message::ExportMetricsServiceRequest, metrics_request()),
            (Message::ExportTraceServiceRequest, traces_request()),
        ]
    }

    /// Scenario: prost-built logs, metrics and traces requests setting every field the validator
    /// knows (all metric types, exemplars, packed lists, entity refs, span events, links, nested
    /// values).
    /// Guarantees: each passes, so the schema table matches what a conforming encoder writes.
    #[test]
    fn a_fully_populated_request_passes() {
        for (root, body) in requests() {
            assert!(validate_request(&body, root).is_ok(), "{root:?}");
        }
    }

    /// Unknown content a newer or proto2 sender may put in any message: a
    /// balanced group of field 31 holding a varint field and a nested group
    /// of field 32, then field 31 as a length-delimited value whose bytes
    /// look like a `resource_logs` / `values` / `key` field (`0a 00`), then
    /// field 31 as a varint. No OTLP message defines field 31 or 32.
    const UNKNOWN: &[u8] = &[
        0xfb, 0x01, 0x08, 0x05, 0x83, 0x02, 0x84, 0x02, 0xfc, 0x01, // group 31
        0xfa, 0x01, 0x02, 0x0a, 0x00, // field 31, LEN, `0a 00`
        0xf8, 0x01, 0x0a, // field 31, varint
    ];

    /// Re-encode `buf`, a message of type `message`, with `UNKNOWN` placed
    /// before its first field, and every sub-message the schema defines
    /// inside it re-encoded the same way.
    fn decorate(buf: &[u8], message: Message) -> Vec<u8> {
        let mut out = UNKNOWN.to_vec();
        let mut pos = 0;
        while pos < buf.len() {
            let (field_num, wire_type, next) = read_key(buf, pos).expect("key");
            let (start, end) = value_range(buf, wire_type, next).expect("value");
            match message.field(field_num).kind {
                Kind::Message(child) => {
                    let inner = decorate(&buf[start..end], child);
                    out.extend(len_field(field_num as u32, &inner));
                }
                _ => out.extend_from_slice(&buf[pos..end]),
            }
            pos = end;
        }
        out
    }

    /// Scenario: each full request, and the same with a balanced unknown group, a look-alike
    /// unknown length-delimited field and an unknown varint before the first field of every
    /// message.
    /// Guarantees: the decorated request passes and converts to exactly the plain request's OTAP
    /// records.
    #[test]
    fn unknown_content_before_known_fields_is_skipped_by_the_views() {
        use crate::otap::OtapArrowRecords;
        use crate::{OtapPayload, OtlpProtoBytes, TryIntoWithOptions};
        use otel_arrow_dfe_config::SignalType;

        let convert = |signal, body: Vec<u8>| {
            let payload = OtapPayload::from(OtlpProtoBytes::new_from_bytes(signal, body));
            let records: OtapArrowRecords = payload.try_into_with_default().expect("converts");
            format!("{records:?}")
        };
        for ((root, body), signal) in
            requests()
                .into_iter()
                .zip([SignalType::Logs, SignalType::Metrics, SignalType::Traces])
        {
            let decorated = decorate(&body, root);
            assert!(decorated.len() > body.len() + 100, "{root:?}");
            assert!(validate_request(&decorated, root).is_ok(), "{root:?}");
            assert_eq!(
                convert(signal, decorated),
                convert(signal, body),
                "{root:?}: the unknown content changed what the views read"
            );
        }
    }

    /// A field `num` of `wire_type` holding `value` (length prefix included
    /// for `LEN`).
    fn raw_field(num: u64, wire_type: u64, value: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint((num << 3) | wire_type, &mut out);
        out.extend_from_slice(value);
        out
    }

    /// A value of `wire_type` other than `original` (length prefix included
    /// for `LEN`), of the same length, so ids stay well-formed.
    fn decoy(wire_type: u64, original: &[u8]) -> Vec<u8> {
        match wire_type {
            VARINT => vec![0x07],
            FIXED64 => vec![0x11; 8],
            FIXED32 => vec![0x22; 4],
            LEN => {
                let flipped: Vec<u8> = if original.is_empty() {
                    b"d".to_vec()
                } else {
                    original.iter().map(|b| b ^ 0x01).collect()
                };
                let mut out = Vec::new();
                encode_varint(flipped.len() as u64, &mut out);
                out.extend(flipped);
                out
            }
            other => unreachable!("wire type {other}"),
        }
    }

    /// Content of a `child` message that sets one field to a decoy: an
    /// integer for `AnyValue`, else its first singular scalar field.
    fn decoy_message(child: Message) -> Vec<u8> {
        if child == Message::AnyValue {
            return vec![0x18, 0x07];
        }
        (1..64)
            .find_map(|num| match child.field(num) {
                Field {
                    kind: Kind::Scalar(wire_type),
                    singular: Some(Singular { oneof: None, .. }),
                } => Some(raw_field(num, wire_type, &decoy(wire_type, &[]))),
                _ => None,
            })
            .unwrap_or_default()
    }

    /// Another member of the oneof field `num` of `message` belongs to.
    fn other_member(message: Message, num: u64) -> Option<u64> {
        let slot = message.field(num).singular?.oneof?;
        (1..64).find(|&other| {
            other != num
                && matches!(message.field(other).singular, Some(s) if s.oneof == Some(slot))
        })
    }

    /// Re-encode `buf`, a message of type `message`, with every occurrence of
    /// field `target.1` in every `target.0` message written as two or three
    /// occurrences; `variant` picks the arrangement: 0 puts a decoy (for a
    /// oneof, another member) first and splits a message in two, 1 splits a
    /// message around another oneof member or writes a scalar twice, 2 puts
    /// the decoy last.
    fn repeat(buf: &[u8], message: Message, target: (Message, u64), variant: usize) -> Vec<u8> {
        let mut out = Vec::new();
        let mut pos = 0;
        while pos < buf.len() {
            let (num, wire_type, next) = read_key(buf, pos).expect("key");
            let (start, end) = value_range(buf, wire_type, next).expect("value");
            let field = message.field(num);
            let content = match field.kind {
                Kind::Message(child) => repeat(&buf[start..end], child, target, variant),
                _ => buf[start..end].to_vec(),
            };
            let original = match field.kind {
                Kind::Message(_) => len_field(num as u32, &content),
                _ => buf[pos..end].to_vec(),
            };
            pos = end;
            if (message, num) != target {
                out.extend(original);
                continue;
            }
            let other = other_member(message, num);
            match field.kind {
                Kind::Message(child) => {
                    let other_decoy = other.map(|other| match message.field(other).kind {
                        Kind::Message(other_child) => {
                            len_field(other as u32, &decoy_message(other_child))
                        }
                        _ => unreachable!("a message oneof has message members"),
                    });
                    let own_decoy = len_field(num as u32, &decoy_message(child));
                    let cut = if content.is_empty() {
                        0
                    } else {
                        let (_, wire, next) = read_key(&content, 0).expect("key");
                        value_range(&content, wire, next).expect("value").1
                    };
                    let first = len_field(num as u32, &content[..cut]);
                    let rest = len_field(num as u32, &content[cut..]);
                    let decoy = other_decoy.unwrap_or(own_decoy);
                    let parts = match variant {
                        0 => vec![decoy, first, rest],
                        1 => vec![first, decoy, rest],
                        _ => vec![original, decoy],
                    };
                    out.extend(parts.concat());
                }
                Kind::Scalar(expected) => {
                    let decoy = match other {
                        Some(other) => match message.field(other).kind {
                            Kind::Scalar(other_wire) => {
                                raw_field(other, other_wire, &decoy(other_wire, &[]))
                            }
                            _ => unreachable!("a scalar oneof has scalar members"),
                        },
                        None => raw_field(num, expected, &decoy(expected, &content)),
                    };
                    let parts = match variant {
                        0 => vec![decoy, original],
                        1 => vec![original.clone(), original],
                        _ => vec![original, decoy],
                    };
                    out.extend(parts.concat());
                }
                Kind::Packed(_) | Kind::Unknown => unreachable!("not singular"),
            }
        }
        out
    }

    /// `body`, a request of type `root`, decoded by prost and encoded again:
    /// every singular field once, as prost merged it.
    fn prost_canonical(root: Message, body: &[u8]) -> Vec<u8> {
        match root {
            Message::ExportLogsServiceRequest => ExportLogsServiceRequest::decode(body)
                .expect("prost decodes it")
                .encode_to_vec(),
            Message::ExportMetricsServiceRequest => ExportMetricsServiceRequest::decode(body)
                .expect("prost decodes it")
                .encode_to_vec(),
            Message::ExportTraceServiceRequest => ExportTraceServiceRequest::decode(body)
                .expect("prost decodes it")
                .encode_to_vec(),
            other => unreachable!("{other:?} is not a request root"),
        }
    }

    /// The OTAP records the byte views convert `body` into, as text.
    fn convert(root: Message, body: Vec<u8>) -> String {
        use crate::otap::OtapArrowRecords;
        use crate::{OtapPayload, OtlpProtoBytes, TryIntoWithOptions};
        use otel_arrow_dfe_config::SignalType;
        let signal = match root {
            Message::ExportLogsServiceRequest => SignalType::Logs,
            Message::ExportMetricsServiceRequest => SignalType::Metrics,
            _ => SignalType::Traces,
        };
        let payload = OtapPayload::from(OtlpProtoBytes::new_from_bytes(signal, body));
        let records: OtapArrowRecords = payload.try_into_with_default().expect("converts");
        format!("{records:?}")
    }

    /// Every (message, field) pair of a singular field set in `buf`.
    fn singular_fields(buf: &[u8], message: Message, found: &mut Vec<(Message, u64)>) {
        let mut pos = 0;
        while pos < buf.len() {
            let (field_num, wire_type, next) = read_key(buf, pos).expect("key");
            let (start, end) = value_range(buf, wire_type, next).expect("value");
            let field = message.field(field_num);
            if field.singular.is_some() && !found.contains(&(message, field_num)) {
                found.push((message, field_num));
            }
            if let Kind::Message(child) = field.kind {
                singular_fields(&buf[start..end], child, found);
            }
            pos = end;
        }
    }

    /// Scenario: each singular field or oneof member of the full requests (118 cases over 26
    /// message types) written as several occurrences in three arrangements: a decoy value or
    /// another oneof member first and a message split in two, a message split around another
    /// member or a scalar written twice, and the decoy last.
    /// Guarantees: every body passes and the byte views convert it to exactly the OTAP records
    /// of prost's own re-encoding of it: the last scalar and oneof member win, the occurrences of
    /// a message are merged, and a oneof member merges only with its own final run.
    #[test]
    fn a_repeated_singular_field_reads_as_prost_decodes_it() {
        let mut targets = Vec::new();
        for (root, body) in requests() {
            let mut found = Vec::new();
            singular_fields(&body, root, &mut found);
            for target in found {
                for variant in 0..3 {
                    let repeated = repeat(&body, root, target, variant);
                    assert_ne!(repeated, body, "{target:?}/{variant}");
                    assert!(
                        validate_request(&repeated, root).is_ok(),
                        "{target:?}/{variant}"
                    );
                    assert!(
                        convert(root, repeated.clone())
                            == convert(root, prost_canonical(root, &repeated)),
                        "{target:?}/{variant}: the views read the repeats unlike prost"
                    );
                }
                targets.push(target);
            }
        }
        let messages: Vec<Message> = targets.iter().fold(Vec::new(), |mut all, (m, _)| {
            if !all.contains(m) {
                all.push(*m);
            }
            all
        });
        assert_eq!((targets.len(), messages.len()), (118, 26), "{messages:?}");
        for (message, field) in [
            (Message::ResourceLogs, 1),
            (Message::ScopeSpans, 1),
            (Message::LogRecord, 5),
            (Message::Metric, 5),
            (Message::NumberDataPoint, 4),
            (Message::Exemplar, 3),
            (Message::ExponentialHistogramDataPoint, 8),
            (Message::ExponentialHistogramDataPoint, 9),
            (Message::Span, 15),
            (Message::KeyValue, 2),
        ] {
            assert!(targets.contains(&(message, field)), "{message:?}.{field}");
        }
    }

    /// Scenario: every proper prefix of each full request, and each with one nested length byte
    /// changed.
    /// Guarantees: only prefixes ending on a top-level field boundary pass; a cut inside any nested
    /// message is refused.
    #[test]
    fn a_body_cut_inside_any_nested_message_is_refused() {
        for (root, body) in requests() {
            let mut boundaries = Vec::new();
            let mut pos = 0;
            while pos < body.len() {
                let (_, next) = read_varint(&body, pos).expect("key");
                let (_, end) = value_range(&body, LEN, next).expect("value");
                boundaries.push(end);
                pos = end;
            }
            for cut in 1..body.len() {
                let result = validate_request(&body[..cut], root);
                assert_eq!(
                    result.is_ok(),
                    boundaries.contains(&cut),
                    "{root:?} cut at {cut}: {result:?}"
                );
            }
        }
    }

    /// Scenario: a string `AnyValue` inside a log attribute declaring more bytes than it has.
    /// Guarantees: `InvalidOtlpWireFormat` names `AnyValue` and the offset within the request.
    #[test]
    fn deep_damage_names_the_message_and_offset() {
        let any_value = [0x0a, 0x05, b'a'];
        let key_value = [len_field(1, b"k"), len_field(2, &any_value)].concat();
        let log_record = len_field(6, &key_value);
        let scope_logs = len_field(2, &log_record);
        let resource_logs = len_field(2, &scope_logs);
        let body = len_field(1, &resource_logs);
        // Each of the five enclosing fields adds a one-byte key and a
        // one-byte length before the AnyValue's content, and the KeyValue's
        // key field adds three more.
        let expected = 5 * 2 + 3;
        match validate_request(&body, Message::ExportLogsServiceRequest) {
            Err(Error::InvalidOtlpWireFormat {
                problem,
                message,
                offset,
            }) => {
                assert_eq!(problem, WireProblem::LengthOverrun);
                assert_eq!(message, "AnyValue");
                assert_eq!(offset, expected);
                assert_eq!(body[offset], 0x0a);
            }
            other => panic!("expected a framing error, got {other:?}"),
        }
    }

    /// Scenario: unknown log record fields of every wire type, one holding an invalid message, and
    /// one overrunning its record.
    /// Guarantees: unknown fields are framed but not parsed, so only the broken frame is refused.
    #[test]
    fn unknown_fields_are_skipped_but_framed() {
        let mut record = vec![0xf8, 0x01, 0x05]; // field 31, varint
        record.extend([0xf9, 0x01, 1, 2, 3, 4, 5, 6, 7, 8]); // field 31, fixed64
        record.extend([0xfd, 0x01, 1, 2, 3, 4]); // field 31, fixed32
        record.extend(len_field(31, &[0xff, 0xff, 0xff])); // not a message
        let body = len_field(1, &len_field(2, &len_field(2, &record)));
        assert!(validate_request(&body, Message::ExportLogsServiceRequest).is_ok());

        let mut overrun = record.clone();
        overrun.extend([0xfa, 0x01, 0x09, 0x00]); // field 31, 9 bytes declared
        let body = len_field(1, &len_field(2, &len_field(2, &overrun)));
        assert!(matches!(
            validate_request(&body, Message::ExportLogsServiceRequest),
            Err(Error::InvalidOtlpWireFormat {
                message: "LogRecord",
                ..
            })
        ));
    }

    /// Scenario: known fields with a wrong wire type (`ResourceLogs` and `time_unix_nano` as
    /// varints, an `AnyValue` string as fixed32), group wire types and field number zero.
    /// Guarantees: each is refused, as prost refuses it.
    #[test]
    fn a_known_field_with_the_wrong_wire_type_is_refused() {
        let root = Message::ExportLogsServiceRequest;
        assert!(validate_request(&[0x08, 0x01], root).is_err());
        let record = [0x08, 0x01];
        let body = len_field(1, &len_field(2, &len_field(2, &record)));
        assert!(validate_request(&body, root).is_err());
        let any_value = [0x0d, 1, 2, 3, 4];
        let key_value = len_field(2, &any_value);
        let body = len_field(1, &len_field(2, &len_field(2, &len_field(6, &key_value))));
        assert!(validate_request(&body, root).is_err());
        for key in [0x0b, 0x0c, 0x0e, 0x0f, 0x02] {
            assert!(validate_request(&[key, 0x00], root).is_err(), "{key:#x}");
        }
    }

    /// Scenario: bucket counts and bounds unpacked, packed with a length not a multiple of eight,
    /// and a packed truncated varint.
    /// Guarantees: both encodings pass, and a packed field without whole elements is refused.
    #[test]
    fn packed_fields_must_hold_whole_elements() {
        let root = Message::ExportMetricsServiceRequest;
        let wrap_histogram = |point: &[u8]| {
            let histogram = len_field(1, point);
            let metric = len_field(9, &histogram);
            len_field(1, &len_field(2, &len_field(2, &metric)))
        };
        // Field 6, fixed64, twice (unpacked); field 7, fixed64 (unpacked).
        let mut unpacked = vec![0x31, 1, 0, 0, 0, 0, 0, 0, 0, 0x31, 2, 0, 0, 0, 0, 0, 0, 0];
        unpacked.extend([0x39, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f]);
        assert!(validate_request(&wrap_histogram(&unpacked), root).is_ok());
        let ragged = len_field(6, &[1, 0, 0, 0, 0, 0, 0, 0, 2]);
        assert!(validate_request(&wrap_histogram(&ragged), root).is_err());
        let ragged = len_field(7, &[0; 12]);
        assert!(validate_request(&wrap_histogram(&ragged), root).is_err());

        let wrap_buckets = |buckets: &[u8]| {
            let point = len_field(8, buckets);
            let histogram = len_field(1, &point);
            let metric = len_field(10, &histogram);
            len_field(1, &len_field(2, &len_field(2, &metric)))
        };
        assert!(validate_request(&wrap_buckets(&len_field(2, &[0x01, 0xac, 0x02])), root).is_ok());
        assert!(validate_request(&wrap_buckets(&[0x10, 0x01, 0x10, 0x02]), root).is_ok());
        assert!(validate_request(&wrap_buckets(&len_field(2, &[0x01, 0xac])), root).is_err());
    }

    /// Re-encode `buf`, a message of type `message`, with every packed
    /// repeated scalar written as its first element expanded, an empty packed
    /// chunk and the rest packed.
    fn mix_packed(buf: &[u8], message: Message) -> Vec<u8> {
        let mut out = Vec::new();
        let mut pos = 0;
        while pos < buf.len() {
            let (num, wire_type, next) = read_key(buf, pos).expect("key");
            let (start, end) = value_range(buf, wire_type, next).expect("value");
            let value = &buf[start..end];
            match message.field(num).kind {
                Kind::Message(child) => {
                    out.extend(len_field(num as u32, &mix_packed(value, child)))
                }
                Kind::Packed(element) if wire_type == LEN && !value.is_empty() => {
                    let first = match element {
                        FIXED64 => 8,
                        _ => read_varint(value, 0).expect("element").1,
                    };
                    out.extend(raw_field(num, element, &value[..first]));
                    out.extend(len_field(num as u32, &[]));
                    out.extend(len_field(num as u32, &value[first..]));
                }
                _ => out.extend_from_slice(&buf[pos..end]),
            }
            pos = end;
        }
        out
    }

    /// Scenario: the full metrics request with every packed list (histogram bucket counts and
    /// bounds, exponential bucket counts) split into an expanded first element, an empty packed
    /// chunk and a packed rest; log bodies nesting 1, 2, 10 and 30 arrays and key-value lists.
    /// Guarantees: the byte views convert each to exactly the OTAP records of prost's re-encoding.
    #[test]
    fn mixed_encodings_and_nesting_read_as_prost_decodes_them() {
        let root = Message::ExportMetricsServiceRequest;
        let body = metrics_request();
        let mixed = mix_packed(&body, root);
        assert!(mixed.len() > body.len(), "the request has packed lists");
        assert_eq!(
            convert(root, mixed.clone()),
            convert(root, prost_canonical(root, &mixed))
        );
        let root = Message::ExportLogsServiceRequest;
        for levels in [1, 2, 10, 30] {
            let body = nested_body(levels);
            assert_eq!(
                convert(root, body.clone()),
                convert(root, prost_canonical(root, &body)),
                "{levels} levels"
            );
        }
    }

    /// A log body nesting `levels` arrays, the innermost holding a string.
    fn nested_body(levels: usize) -> Vec<u8> {
        let mut value = len_field(1, b"leaf");
        for level in 0..levels {
            // Alternate arrays and key-value lists: both count alike.
            let container = if level % 2 == 0 {
                len_field(5, &len_field(1, &value))
            } else {
                let key_value = [len_field(1, b"k"), len_field(2, &value)].concat();
                len_field(6, &len_field(1, &key_value))
            };
            value = container;
        }
        let record = len_field(5, &value);
        len_field(1, &len_field(2, &len_field(2, &record)))
    }

    /// Scenario: log bodies nesting exactly at and one beyond `MAX_ANY_VALUE_NESTING_DEPTH`.
    /// Guarantees: the limit itself is accepted on a test thread's stack; one more is
    /// `OtlpNestingTooDeep`.
    #[test]
    fn nesting_is_bounded_exactly() {
        let root = Message::ExportLogsServiceRequest;
        assert!(validate_request(&nested_body(MAX_ANY_VALUE_NESTING_DEPTH), root).is_ok());
        assert!(matches!(
            validate_request(&nested_body(MAX_ANY_VALUE_NESTING_DEPTH + 1), root),
            Err(Error::OtlpNestingTooDeep {
                limit: MAX_ANY_VALUE_NESTING_DEPTH,
                ..
            })
        ));
    }

    /// Scenario: a log body nesting `MAX_ANY_VALUE_NESTING_DEPTH` levels, validated and converted
    /// to OTAP records on a thread with a 1.5 MiB stack, in whatever build the tests run.
    /// Guarantees: both fit, with a quarter to spare, in the 2 MiB stack a pipeline thread gets by
    /// default (an unoptimized build needs about 0.75 MiB to validate and 1 MiB for both).
    #[test]
    fn the_deepest_body_fits_a_pipeline_thread_stack() {
        std::thread::Builder::new()
            .stack_size(3 << 19)
            .spawn(|| {
                let body = nested_body(MAX_ANY_VALUE_NESTING_DEPTH);
                assert!(validate_request(&body, Message::ExportLogsServiceRequest).is_ok());
                let _ = convert(Message::ExportLogsServiceRequest, body);
            })
            .expect("a thread")
            .join()
            .expect("validation and conversion fit the stack");
    }

    /// A logs request whose only log record is `record`.
    fn in_log_record(record: &[u8]) -> Vec<u8> {
        len_field(1, &len_field(2, &len_field(2, record)))
    }

    /// The problem an `InvalidOtlpWireFormat` names, or a panic.
    fn problem(result: Result<(), Error>) -> WireProblem {
        match result {
            Err(Error::InvalidOtlpWireFormat { problem, .. }) => problem,
            other => panic!("expected a framing error, got {other:?}"),
        }
    }

    /// Scenario: `u64::MAX` in ten bytes, the ten-byte `80 .. 80 02` and an eleven-byte varint, as
    /// a scalar, a nested length and a packed bucket count.
    /// Guarantees: the maximum passes; bits past the 64th are refused wherever they appear, as
    /// prost refuses them.
    #[test]
    fn a_varint_that_overflows_u64_is_refused() {
        let root = Message::ExportLogsServiceRequest;
        let max = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
        let overflow = [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02];
        let mut eleven = [0x80; 11];
        eleven[10] = 0x00;
        let severity = |varint: &[u8]| in_log_record(&[&[0x10][..], varint].concat());
        assert!(validate_request(&severity(&max), root).is_ok());
        assert_eq!(
            problem(validate_request(&severity(&overflow), root)),
            WireProblem::TruncatedVarint
        );
        assert_eq!(
            problem(validate_request(&severity(&eleven), root)),
            WireProblem::TruncatedVarint
        );
        // LogRecord.body (field 5, LEN) whose length is the overflowing varint.
        let length = in_log_record(&[&[0x2a][..], &overflow].concat());
        assert_eq!(
            problem(validate_request(&length, root)),
            WireProblem::TruncatedLength
        );
        // An unknown field key that overflows.
        let key = in_log_record(&overflow);
        assert_eq!(
            problem(validate_request(&key, root)),
            WireProblem::TruncatedKey
        );
        let buckets = |counts: &[u8]| {
            let point = len_field(8, &len_field(2, counts));
            let metric = len_field(10, &len_field(1, &point));
            len_field(1, &len_field(2, &len_field(2, &metric)))
        };
        let metrics = Message::ExportMetricsServiceRequest;
        assert!(validate_request(&buckets(&max), metrics).is_ok());
        assert_eq!(
            problem(validate_request(&buckets(&overflow), metrics)),
            WireProblem::TruncatedPackedVarint
        );
    }

    /// Scenario: `AnyValue.string_value`, a `KeyValue.key` and a `Metric.name` holding `0xff` or a
    /// lone `0xc3`.
    /// Guarantees: each passes: UTF-8 is content, repaired by the conversion, not framing.
    #[test]
    fn a_string_field_is_not_checked_for_utf8() {
        let attribute = |key: &[u8], value: &[u8]| {
            let key_value = [len_field(1, key), len_field(2, value)].concat();
            in_log_record(&len_field(6, &key_value))
        };
        let metric = len_field(1, &[0xc3]);
        for (body, root) in [
            (
                attribute(b"k", &len_field(1, &[0xff])),
                Message::ExportLogsServiceRequest,
            ),
            (
                attribute(&[0xff], &len_field(1, b"v")),
                Message::ExportLogsServiceRequest,
            ),
            (
                len_field(1, &len_field(2, &len_field(2, &metric))),
                Message::ExportMetricsServiceRequest,
            ),
        ] {
            assert!(validate_request(&body, root).is_ok(), "{root:?}");
        }
    }

    /// Scenario: unknown field 31 as a group (empty, holding every wire type, holding a nested
    /// group), stray and mismatched end groups, an unclosed group, a group on a known field, and
    /// groups at and beyond the nesting limit.
    /// Guarantees: balanced unknown groups are skipped as prost skips them; every other shape is
    /// refused, and group nesting shares the `AnyValue` limit.
    #[test]
    fn unknown_groups_are_skipped_when_balanced() {
        let root = Message::ExportLogsServiceRequest;
        let start31 = [0xfb, 0x01];
        let end31 = [0xfc, 0x01];
        let start32 = [0x83, 0x02];
        let end32 = [0x84, 0x02];
        let record = |parts: &[&[u8]]| in_log_record(&parts.concat());
        assert!(validate_request(&record(&[&start31, &end31]), root).is_ok());
        let fields: &[u8] = &[
            0x08, 0x05, 0x11, 1, 2, 3, 4, 5, 6, 7, 8, 0x1a, 0x01, 0xff, 0x25, 1, 2, 3, 4,
        ];
        assert!(validate_request(&record(&[&start31, fields, &end31]), root).is_ok());
        assert!(
            validate_request(&record(&[&start31, &start32, fields, &end32, &end31]), root).is_ok()
        );

        assert_eq!(
            problem(validate_request(&record(&[&end31]), root)),
            WireProblem::StrayEndGroup
        );
        assert_eq!(
            problem(validate_request(&record(&[&start31, &end32]), root)),
            WireProblem::MismatchedEndGroup
        );
        assert_eq!(
            problem(validate_request(&record(&[&start31, fields]), root)),
            WireProblem::UnclosedGroup
        );
        // Field 1 of a log record (time_unix_nano) as a group.
        assert_eq!(
            problem(validate_request(&record(&[&[0x0b, 0x0c]]), root)),
            WireProblem::WrongWireType
        );

        let nested = |levels: usize| {
            let mut parts = vec![&start31[..]; levels];
            parts.extend(vec![&end31[..]; levels]);
            record(&parts)
        };
        assert!(validate_request(&nested(MAX_ANY_VALUE_NESTING_DEPTH), root).is_ok());
        assert!(matches!(
            validate_request(&nested(MAX_ANY_VALUE_NESTING_DEPTH + 1), root),
            Err(Error::OtlpNestingTooDeep { .. })
        ));
    }

    /// Every (message, field) pair a message of type `message` in `buf`
    /// sets, nested messages included.
    fn set_fields(buf: &[u8], message: Message, found: &mut Vec<(Message, u64)>) {
        let mut pos = 0;
        while pos < buf.len() {
            let (field_num, wire_type, next) = read_key(buf, pos).expect("key");
            let (start, end) = value_range(buf, wire_type, next).expect("value");
            if !found.contains(&(message, field_num)) {
                found.push((message, field_num));
            }
            if let Kind::Message(child) = message.field(field_num).kind {
                set_fields(&buf[start..end], child, found);
            }
            pos = end;
        }
    }

    /// Scenario: field numbers 1 to 63 of every message reachable in the schema table, against the
    /// fields the full prost requests set, and field numbers 64 to 1023.
    /// Guarantees: the table has exactly the prost fields; every singular field is below 64, the
    /// bound of the byte views' repeat tracking; every packed element is a varint or fixed-width.
    #[test]
    fn the_schema_table_matches_the_prost_types() {
        let mut messages = vec![
            Message::ExportLogsServiceRequest,
            Message::ExportMetricsServiceRequest,
            Message::ExportTraceServiceRequest,
        ];
        let mut known = Vec::new();
        let mut i = 0;
        while i < messages.len() {
            let message = messages[i];
            for num in 64..1024 {
                assert!(
                    matches!(message.field(num).kind, Kind::Unknown),
                    "{message:?}.{num}"
                );
            }
            for num in 1..64 {
                let field = message.field(num);
                if let Kind::Packed(element) = field.kind {
                    assert!(
                        matches!(element, FIXED64 | FIXED32 | VARINT),
                        "{message:?}.{num}"
                    );
                }
                if let Some(singular) = field.singular {
                    let slot = singular.oneof.unwrap_or(num);
                    assert!(slot < 64, "{message:?}.{num}");
                    assert!(
                        matches!(message.field(slot).singular, Some(s) if s.name == singular.name),
                        "{message:?}.{num}: slot {slot} is not a member of its oneof"
                    );
                }
                match field.kind {
                    Kind::Unknown => {}
                    Kind::Message(child) => {
                        known.push((message, num));
                        if !messages.contains(&child) {
                            messages.push(child);
                        }
                    }
                    Kind::Scalar(_) | Kind::Packed(_) => known.push((message, num)),
                }
            }
            i += 1;
        }
        let mut set = Vec::new();
        for (root, body) in requests() {
            set_fields(&body, root, &mut set);
        }
        let sort = |pairs: &mut Vec<(Message, u64)>| {
            pairs.sort_by_key(|(message, num)| (message.name(), *num));
        };
        sort(&mut known);
        sort(&mut set);
        assert_eq!(known, set);
    }

    /// Whether prost decodes `body` as the request type `root` names, or the
    /// decode error if it does not.
    fn prost_decode(root: Message, body: &[u8]) -> Result<(), prost::DecodeError> {
        match root {
            Message::ExportLogsServiceRequest => ExportLogsServiceRequest::decode(body).map(drop),
            Message::ExportMetricsServiceRequest => {
                ExportMetricsServiceRequest::decode(body).map(drop)
            }
            Message::ExportTraceServiceRequest => ExportTraceServiceRequest::decode(body).map(drop),
            other => unreachable!("{other:?} is not a request root"),
        }
    }

    /// One edit of a fixture body: overwrite, insert or delete the byte at an
    /// index, or cut the body there.
    #[derive(Clone, Copy, Debug)]
    enum Edit {
        Set(proptest::sample::Index, u8),
        Insert(proptest::sample::Index, u8),
        Delete(proptest::sample::Index),
        Cut(proptest::sample::Index),
    }

    fn edit() -> impl proptest::strategy::Strategy<Value = Edit> {
        use proptest::prelude::*;
        prop_oneof![
            (any::<proptest::sample::Index>(), any::<u8>()).prop_map(|(i, b)| Edit::Set(i, b)),
            (any::<proptest::sample::Index>(), any::<u8>()).prop_map(|(i, b)| Edit::Insert(i, b)),
            any::<proptest::sample::Index>().prop_map(Edit::Delete),
            any::<proptest::sample::Index>().prop_map(Edit::Cut),
        ]
    }

    fn apply(body: &mut Vec<u8>, edit: Edit) {
        if body.is_empty() && !matches!(edit, Edit::Insert(..)) {
            return;
        }
        match edit {
            Edit::Set(i, b) => {
                let i = i.index(body.len());
                body[i] = b;
            }
            Edit::Insert(i, b) => body.insert(i.index(body.len() + 1), b),
            Edit::Delete(i) => {
                let _ = body.remove(i.index(body.len()));
            }
            Edit::Cut(i) => body.truncate(i.index(body.len())),
        }
    }

    /// The number of length-delimited fields in `buf`, a message of type
    /// `message`, nested ones included, or `None` if a frame on the way is
    /// broken.
    fn len_fields(buf: &[u8], message: Message) -> Option<usize> {
        let mut count = 0;
        let mut pos = 0;
        while pos < buf.len() {
            let (field_num, wire_type, next) = read_key(buf, pos).ok()?;
            let (start, end) = value_range(buf, wire_type, next).ok()?;
            if wire_type == LEN {
                count += 1;
                if let Kind::Message(child) = message.field(field_num).kind {
                    count += len_fields(&buf[start..end], child)?;
                }
            }
            pos = end;
        }
        Some(count)
    }

    /// Re-encode `buf`, a message of type `message`, with `edit` applied to
    /// the payload of the `target`-th length-delimited field in pre-order and
    /// every enclosing length prefix rewritten to match, so the edit lands
    /// inside a well-framed field at any depth.
    fn edit_nested(
        buf: &[u8],
        message: Message,
        target: &mut Option<usize>,
        edit: Edit,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        let mut pos = 0;
        while pos < buf.len() {
            let (field_num, wire_type, next) = read_key(buf, pos).expect("key");
            let (start, end) = value_range(buf, wire_type, next).expect("value");
            if wire_type != LEN {
                out.extend_from_slice(&buf[pos..end]);
                pos = end;
                continue;
            }
            let payload = match *target {
                Some(0) => {
                    *target = None;
                    let mut payload = buf[start..end].to_vec();
                    apply(&mut payload, edit);
                    payload
                }
                Some(n) => {
                    *target = Some(n - 1);
                    match message.field(field_num).kind {
                        Kind::Message(child) => edit_nested(&buf[start..end], child, target, edit),
                        _ => buf[start..end].to_vec(),
                    }
                }
                None => buf[start..end].to_vec(),
            };
            out.extend(len_field(field_num as u32, &payload));
            pos = end;
        }
        out
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(2048))]

        /// Scenario: full requests with one to three random byte edits, on the whole body or inside
        /// one field at any depth with the enclosing lengths rewritten.
        /// Guarantees: the validator accepts exactly what prost decodes, except non-UTF-8 strings.
        #[test]
        fn the_validator_agrees_with_prost_on_edited_bodies(
            signal in 0usize..3,
            edits in proptest::collection::vec(
                (proptest::prelude::any::<proptest::sample::Index>(), edit()),
                1..4,
            ),
        ) {
            let (root, mut body) = requests()[signal].clone();
            for (target, edit) in edits {
                // Position 0 is the whole body; position i edits the i-th
                // length-delimited field. Bodies an earlier edit broke are
                // edited whole.
                let fields = len_fields(&body, root).unwrap_or(0);
                match target.index(fields + 1) {
                    0 => apply(&mut body, edit),
                    i => body = edit_nested(&body, root, &mut Some(i - 1), edit),
                }
            }
            let accepted = validate_request(&body, root);
            match prost_decode(root, &body) {
                Ok(()) => proptest::prop_assert!(accepted.is_ok(), "{accepted:?}"),
                Err(error) if error.to_string().contains("not UTF-8") => {}
                Err(error) => proptest::prop_assert!(accepted.is_err(), "prost: {error}"),
            }
        }
    }

    /// Scenario: logs, metrics and traces payloads whose nested `Resource*` is `0a 01 0a` (a tag
    /// without a length), the same payloads empty, and Arrow records.
    /// Guarantees: `OtapPayload::validate_otlp_framing` refuses the damaged bodies naming the
    /// message and passes the rest.
    #[test]
    fn the_payload_check_refuses_damage_and_passes_arrow_records() {
        use crate::OtapPayload;
        use crate::OtlpProtoBytes;
        use crate::otap::{Logs, OtapArrowRecords};
        use otel_arrow_dfe_config::SignalType;

        for (signal, message) in [
            (SignalType::Logs, "ResourceLogs"),
            (SignalType::Metrics, "ResourceMetrics"),
            (SignalType::Traces, "ResourceSpans"),
        ] {
            let damaged = OtapPayload::from(OtlpProtoBytes::new_from_bytes(
                signal,
                vec![0x0a, 0x01, 0x0a],
            ));
            match damaged.validate_otlp_framing() {
                Err(Error::InvalidOtlpWireFormat { message: named, .. }) => {
                    assert_eq!(named, message, "{signal:?}")
                }
                other => panic!("{signal:?}: expected a framing error, got {other:?}"),
            }
            let empty = OtapPayload::from(OtlpProtoBytes::empty(signal));
            assert!(empty.validate_otlp_framing().is_ok(), "{signal:?}");
        }
        let records = OtapPayload::from(OtapArrowRecords::Logs(Logs::default()));
        assert!(records.validate_otlp_framing().is_ok());
    }
}
