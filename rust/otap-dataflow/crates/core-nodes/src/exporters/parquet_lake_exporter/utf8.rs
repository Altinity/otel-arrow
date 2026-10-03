// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Repair of invalid UTF-8 in OTLP request bytes.
//!
//! The strict prost decode in `validate_otlp_request` refuses a request whose string fields are
//! not UTF-8. When it does, [`repair_otlp`] rewrites the request: each string field it knows (see
//! `Kind::field`) that is not valid UTF-8 is replaced by its lossy decoding (each maximal invalid
//! subsequence becomes U+FFFD, as `String::from_utf8_lossy` does), the length prefixes of the
//! enclosing messages are rewritten, and every other byte is copied verbatim, unknown fields
//! included. The caller strict-decodes the result again, so prost stays the judge of structural
//! validity: a string field missing from the table leaves invalid UTF-8 behind and the request is
//! refused as before, never truncated.
//!
//! The wire primitives come from `prost::encoding`. prost marks that module `#[doc(hidden)]`: its
//! derived code uses it, but it is outside prost's semver promise, so a prost upgrade can require
//! changes here. The tests below compare the output with prost's own encoding and catch a change.

use std::borrow::Cow;

use prost::encoding::{WireType, decode_key, decode_varint, encode_key, encode_varint};

/// Deepest message nesting the rewriter walks: prost's own recursion limit. The rewriter keeps
/// this bound in every build, also when prost is compiled with its `no-recursion-limit` feature.
const MAX_DEPTH: u32 = 100;

/// The request tree to rewrite.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OtlpTree {
    /// `ExportLogsServiceRequest`.
    Logs,
    /// `ExportMetricsServiceRequest`.
    Metrics,
}

/// The bytes are not a well-formed message of the expected tree, or nest deeper than `MAX_DEPTH`
/// messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Malformed;

/// A repaired request.
#[derive(Debug, PartialEq, Eq)]
pub struct Repaired {
    /// The rewritten request bytes.
    pub bytes: Vec<u8>,
    /// String fields whose bytes were replaced.
    pub strings: u64,
}

/// Rewrite `request` so that every string field of `tree` holds valid UTF-8.
///
/// `Ok(None)` when every string field already is valid UTF-8 (the request needs no change).
/// `Err(Malformed)` for a bad key, a truncated field, a string or message field with a wire type
/// other than length-delimited, a group (OTLP has none), or nesting beyond the limit.
pub fn repair_otlp(tree: OtlpTree, request: &[u8]) -> Result<Option<Repaired>, Malformed> {
    let root = match tree {
        OtlpTree::Logs => Kind::LogsRequest,
        OtlpTree::Metrics => Kind::MetricsRequest,
    };
    let mut strings = 0;
    let bytes = rewrite(root, request, MAX_DEPTH, &mut strings)?;
    Ok(bytes.map(|bytes| Repaired { bytes, strings }))
}

/// A field the rewriter repairs or descends into.
#[derive(Clone, Copy)]
enum Field {
    /// A `string` field, repeated or not.
    Str,
    /// A message field (repeated, optional or a oneof member) of this kind.
    Msg(Kind),
}

/// The OTLP messages on the paths from a logs or metrics request to its string fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    LogsRequest,
    ResourceLogs,
    ScopeLogs,
    LogRecord,
    MetricsRequest,
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
    SummaryDataPoint,
    Exemplar,
    Resource,
    EntityRef,
    InstrumentationScope,
    KeyValue,
    AnyValue,
    ArrayValue,
    KeyValueList,
}

impl Kind {
    /// The string and message fields of this message by field number, as generated in
    /// `otel_arrow_dfe_pdata::proto` (opentelemetry-proto v1). Every other field -- numbers,
    /// bytes, packed scalars, buckets, quantiles, unknown fields -- is copied verbatim.
    const fn field(self, number: u32) -> Option<Field> {
        use Field::{Msg, Str};
        let field = match (self, number) {
            (Self::LogsRequest, 1) => Msg(Self::ResourceLogs),
            (Self::MetricsRequest, 1) => Msg(Self::ResourceMetrics),
            (Self::ResourceLogs | Self::ResourceMetrics, 1) => Msg(Self::Resource),
            (Self::ResourceLogs, 2) => Msg(Self::ScopeLogs),
            (Self::ResourceMetrics, 2) => Msg(Self::ScopeMetrics),
            // schema_url
            (
                Self::ResourceLogs | Self::ScopeLogs | Self::ResourceMetrics | Self::ScopeMetrics,
                3,
            ) => Str,
            (Self::ScopeLogs | Self::ScopeMetrics, 1) => Msg(Self::InstrumentationScope),
            (Self::ScopeLogs, 2) => Msg(Self::LogRecord),
            (Self::ScopeMetrics, 2) => Msg(Self::Metric),
            // severity_text, event_name
            (Self::LogRecord, 3 | 12) => Str,
            (Self::LogRecord, 5) => Msg(Self::AnyValue),
            (Self::LogRecord, 6) => Msg(Self::KeyValue),
            // name, description, unit
            (Self::Metric, 1..=3) => Str,
            (Self::Metric, 5) => Msg(Self::Gauge),
            (Self::Metric, 7) => Msg(Self::Sum),
            (Self::Metric, 9) => Msg(Self::Histogram),
            (Self::Metric, 10) => Msg(Self::ExponentialHistogram),
            (Self::Metric, 11) => Msg(Self::Summary),
            // metadata
            (Self::Metric, 12) => Msg(Self::KeyValue),
            (Self::Gauge | Self::Sum, 1) => Msg(Self::NumberDataPoint),
            (Self::Histogram, 1) => Msg(Self::HistogramDataPoint),
            (Self::ExponentialHistogram, 1) => Msg(Self::ExponentialHistogramDataPoint),
            (Self::Summary, 1) => Msg(Self::SummaryDataPoint),
            // attributes, filtered_attributes
            (Self::NumberDataPoint | Self::SummaryDataPoint | Self::Exemplar, 7)
            | (Self::HistogramDataPoint, 9)
            | (Self::ExponentialHistogramDataPoint, 1) => Msg(Self::KeyValue),
            // exemplars
            (Self::NumberDataPoint, 5)
            | (Self::HistogramDataPoint, 8)
            | (Self::ExponentialHistogramDataPoint, 11) => Msg(Self::Exemplar),
            // Resource.attributes, InstrumentationScope.attributes
            (Self::Resource, 1) | (Self::InstrumentationScope, 3) => Msg(Self::KeyValue),
            (Self::Resource, 3) => Msg(Self::EntityRef),
            // schema_url, type, id_keys, description_keys
            (Self::EntityRef, 1..=4) => Str,
            // name, version
            (Self::InstrumentationScope, 1 | 2) => Str,
            // KeyValue.key, AnyValue.string_value
            (Self::KeyValue | Self::AnyValue, 1) => Str,
            // KeyValue.value, ArrayValue.values
            (Self::KeyValue, 2) | (Self::ArrayValue, 1) => Msg(Self::AnyValue),
            (Self::AnyValue, 5) => Msg(Self::ArrayValue),
            (Self::AnyValue, 6) => Msg(Self::KeyValueList),
            (Self::KeyValueList, 1) => Msg(Self::KeyValue),
            _ => return None,
        };
        Some(field)
    }
}

/// Rewrite one message of `kind`. `Ok(None)`: nothing changed, the caller keeps `msg` as it is.
fn rewrite(
    kind: Kind,
    msg: &[u8],
    depth_left: u32,
    strings: &mut u64,
) -> Result<Option<Vec<u8>>, Malformed> {
    // Built on the first changed field: the bytes of `msg` before it, then every later field.
    let mut out: Option<Vec<u8>> = None;
    let mut rest = msg;
    while !rest.is_empty() {
        let start = msg.len() - rest.len();
        let (number, wire_type) = decode_key(&mut rest).map_err(|_| Malformed)?;
        let replaced = match (kind.field(number), wire_type) {
            (Some(field), WireType::LengthDelimited) => {
                let len = decode_varint(&mut rest).map_err(|_| Malformed)?;
                let len = usize::try_from(len).map_err(|_| Malformed)?;
                if len > rest.len() {
                    return Err(Malformed);
                }
                let (body, tail) = rest.split_at(len);
                rest = tail;
                match field {
                    Field::Str => repair_str(body, strings),
                    Field::Msg(child) => {
                        let depth_left = depth_left.checked_sub(1).ok_or(Malformed)?;
                        rewrite(child, body, depth_left, strings)?
                    }
                }
            }
            // A string or message field is always length-delimited; prost refuses it too.
            (Some(_), _) => return Err(Malformed),
            (None, wire_type) => {
                skip_unknown(wire_type, &mut rest)?;
                None
            }
        };
        let end = msg.len() - rest.len();
        if let Some(body) = replaced {
            let buf = out.get_or_insert_with(|| {
                // Start near the input size; a repair adds at most two bytes per invalid byte.
                let mut buf = Vec::with_capacity(msg.len() + 16);
                buf.extend_from_slice(&msg[..start]);
                buf
            });
            encode_key(number, WireType::LengthDelimited, buf);
            encode_varint(body.len() as u64, buf);
            buf.extend_from_slice(&body);
        } else if let Some(buf) = out.as_mut() {
            buf.extend_from_slice(&msg[start..end]);
        }
    }
    Ok(out)
}

/// Skip the value of a field the rewriter does not know, without recursion. A group is refused:
/// OTLP has none, and skipping one means descending through its nested groups, which has no
/// bound when prost is compiled without its recursion limit.
fn skip_unknown(wire_type: WireType, rest: &mut &[u8]) -> Result<(), Malformed> {
    let len = match wire_type {
        WireType::Varint => {
            let _ = decode_varint(rest).map_err(|_| Malformed)?;
            0
        }
        WireType::ThirtyTwoBit => 4,
        WireType::SixtyFourBit => 8,
        WireType::LengthDelimited => {
            let len = decode_varint(rest).map_err(|_| Malformed)?;
            usize::try_from(len).map_err(|_| Malformed)?
        }
        WireType::StartGroup | WireType::EndGroup => return Err(Malformed),
    };
    let tail = rest.get(len..).ok_or(Malformed)?;
    *rest = tail;
    Ok(())
}

/// The lossy UTF-8 decoding of a string field, or `None` when it already is valid UTF-8.
fn repair_str(bytes: &[u8], strings: &mut u64) -> Option<Vec<u8>> {
    match String::from_utf8_lossy(bytes) {
        Cow::Borrowed(_) => None,
        Cow::Owned(repaired) => {
            *strings += 1;
            Some(repaired.into_bytes())
        }
    }
}

#[cfg(test)]
mod tests {
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
    use prost::Message as _;
    use prost::encoding::{WireType, encode_key, encode_varint};

    use super::{Malformed, OtlpTree, Repaired, repair_otlp};
    use crate::exporters::parquet_lake_exporter::test_fixtures::{
        BAD, REPAIRED, corrupt_utf8, every_string_logs, every_string_metrics, length_delimited,
        logs_request, metrics_request, nested_body_logs_request, sized_logs_request,
    };

    /// Scenario: valid logs and metrics requests (the shared fixtures, and the every-string requests with a valid marker) are passed to the rewriter.
    /// Guarantees: The rewriter reports no change for each, so a valid request is never re-encoded.
    #[test]
    fn valid_requests_are_left_unchanged() {
        for bytes in [
            logs_request(10, 2, 0).encode_to_vec(),
            sized_logs_request(3, 512, 1).encode_to_vec(),
            every_string_logs("ok").encode_to_vec(),
        ] {
            assert_eq!(repair_otlp(OtlpTree::Logs, &bytes), Ok(None));
        }
        for bytes in [
            metrics_request().encode_to_vec(),
            every_string_metrics("ok").encode_to_vec(),
        ] {
            assert_eq!(repair_otlp(OtlpTree::Metrics, &bytes), Ok(None));
        }
    }

    /// Scenario: every string field of a logs and of a metrics request (schema URLs, scope and entity-ref strings, attribute and metadata keys and values at every level, nested kvlist keys and array items, metric name, unit and description, exemplar attributes) holds three invalid bytes.
    /// Guarantees: The output equals the prost encoding of the same request with each marker read as three U+FFFD, and the repaired-string count equals the number of corrupted strings.
    #[test]
    fn every_string_field_is_repaired_exactly() {
        let mut logs = every_string_logs(BAD).encode_to_vec();
        let strings = corrupt_utf8(&mut logs);
        assert!(strings >= 20, "{strings}");
        assert_eq!(
            repair_otlp(OtlpTree::Logs, &logs),
            Ok(Some(Repaired {
                bytes: every_string_logs(REPAIRED).encode_to_vec(),
                strings,
            }))
        );
        let mut metrics = every_string_metrics(BAD).encode_to_vec();
        let strings = corrupt_utf8(&mut metrics);
        assert!(strings >= 20, "{strings}");
        assert_eq!(
            repair_otlp(OtlpTree::Metrics, &metrics),
            Ok(Some(Repaired {
                bytes: every_string_metrics(REPAIRED).encode_to_vec(),
                strings,
            }))
        );
    }

    /// Scenario: a corrupted logs request also carries top-level fields the rewriter does not know: a varint field 99, a length-delimited field 98 with a non-UTF-8 payload, and a fixed64 field 97.
    /// Guarantees: Those bytes are copied verbatim after the repaired fields, and prost decodes the result.
    #[test]
    fn unknown_fields_are_copied_verbatim() {
        let mut unknown = Vec::new();
        encode_key(99, WireType::Varint, &mut unknown);
        encode_varint(7, &mut unknown);
        unknown.extend_from_slice(&length_delimited(
            98,
            &[0xff, b'o', b'p', b'a', b'q', b'u', b'e'],
        ));
        encode_key(97, WireType::SixtyFourBit, &mut unknown);
        unknown.extend_from_slice(&42_u64.to_le_bytes());
        let mut bytes = every_string_logs(BAD).encode_to_vec();
        let strings = corrupt_utf8(&mut bytes);
        bytes.extend_from_slice(&unknown);
        let mut expected = every_string_logs(REPAIRED).encode_to_vec();
        expected.extend_from_slice(&unknown);
        assert_eq!(
            repair_otlp(OtlpTree::Logs, &bytes),
            Ok(Some(Repaired {
                bytes: expected.clone(),
                strings,
            }))
        );
        let _ = ExportLogsServiceRequest::decode(expected.as_slice())
            .expect("prost decodes the repaired request");
    }

    /// Scenario: corrupted logs requests that are also structurally broken: truncated by one byte, carrying a ResourceLogs whose schema URL has the varint wire type, or ending with a stray end-group key.
    /// Guarantees: The rewriter refuses each as malformed instead of repairing part of it.
    #[test]
    fn malformed_requests_are_refused() {
        let mut bytes = every_string_logs(BAD).encode_to_vec();
        let _ = corrupt_utf8(&mut bytes);
        assert_eq!(
            repair_otlp(OtlpTree::Logs, &bytes[..bytes.len() - 1]),
            Err(Malformed)
        );
        // ExportLogsServiceRequest.resource_logs { schema_url (field 3) as varint 5 }
        let mut wrong_type = bytes.clone();
        wrong_type.extend_from_slice(&[0x0a, 0x02, 0x18, 0x05]);
        assert_eq!(repair_otlp(OtlpTree::Logs, &wrong_type), Err(Malformed));
        let mut stray_end = bytes;
        encode_key(9, WireType::EndGroup, &mut stray_end);
        assert_eq!(repair_otlp(OtlpTree::Logs, &stray_end), Err(Malformed));
    }

    /// Scenario: a log body nests 1,000 arrays around a string of invalid bytes, far beyond the rewriter's depth limit of 100.
    /// Guarantees: The rewriter refuses it as malformed once it reaches its depth limit, so it never recurses deeper than 100 messages (no stack overflow on a default test-thread stack).
    #[test]
    fn nesting_beyond_the_limit_is_refused() {
        assert_eq!(
            repair_otlp(OtlpTree::Logs, &nested_body_logs_request(1_000)),
            Err(Malformed)
        );
    }

    /// Scenario: a corrupted logs request ends with an unknown field that opens 100,000 nested groups.
    /// Guarantees: The rewriter refuses the group as malformed without descending into it, so nested groups cannot make it recurse, whatever recursion limit prost was compiled with.
    #[test]
    fn unknown_groups_are_refused_without_recursion() {
        let mut bytes = every_string_logs(BAD).encode_to_vec();
        let _ = corrupt_utf8(&mut bytes);
        for _ in 0..100_000 {
            encode_key(99, WireType::StartGroup, &mut bytes);
        }
        assert_eq!(repair_otlp(OtlpTree::Logs, &bytes), Err(Malformed));
    }

    /// Scenario: a log body nests 40 arrays around a string of invalid bytes, within prost's recursion limit.
    /// Guarantees: The string is repaired once and prost decodes the repaired request.
    #[test]
    fn nesting_within_the_limit_is_repaired() {
        let repaired = repair_otlp(OtlpTree::Logs, &nested_body_logs_request(40))
            .expect("walkable")
            .expect("one string to repair");
        assert_eq!(repaired.strings, 1);
        let _ = ExportLogsServiceRequest::decode(repaired.bytes.as_slice())
            .expect("prost decodes the repaired request");
    }
}
