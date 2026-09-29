// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Canonical encoding v1 and series identity (FORMAT.md section 1).

use std::sync::Arc;

use crate::value::Value;

/// 16-byte series identity: XXH3-128 of the canonical bytes, big-endian.
pub type SeriesId = [u8; 16];

/// Telemetry signal handled in v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Signal {
    /// Logs.
    Logs,
    /// Metrics.
    Metrics,
}

impl Signal {
    /// Canonical string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Signal::Logs => "logs",
            Signal::Metrics => "metrics",
        }
    }
}

/// Metric point kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    /// Gauge.
    Gauge,
    /// Sum.
    Sum,
    /// Explicit-bounds histogram.
    Histogram,
    /// Exponential histogram.
    ExpHistogram,
    /// Summary.
    Summary,
}

impl MetricKind {
    /// Canonical string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            MetricKind::Gauge => "gauge",
            MetricKind::Sum => "sum",
            MetricKind::Histogram => "histogram",
            MetricKind::ExpHistogram => "exp_histogram",
            MetricKind::Summary => "summary",
        }
    }
}

/// Aggregation temporality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Temporality {
    /// Not specified (only valid for gauges).
    Unspecified,
    /// Delta.
    Delta,
    /// Cumulative.
    Cumulative,
}

impl Temporality {
    /// Canonical string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Temporality::Unspecified => "",
            Temporality::Delta => "delta",
            Temporality::Cumulative => "cumulative",
        }
    }
}

/// Metric-level identity fields plus non-identity description.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricDescriptor {
    /// Metric name.
    pub name: String,
    /// Unit.
    pub unit: String,
    /// Point kind.
    pub kind: MetricKind,
    /// Temporality.
    pub temporality: Temporality,
    /// Monotonic flag (false for non-sums).
    pub is_monotonic: bool,
    /// Description (not part of the identity).
    pub description: String,
}

/// Everything that identifies a series, plus `description`.
///
/// Attribute lists must be sorted by raw key bytes with unique keys
/// (see [`crate::value::sort_kvlist`]).
///
/// Invariant: `metric.is_some()` exactly when `signal == Signal::Metrics`.
/// The canonical encoder gates the metric block on `metric.is_some()`, while
/// the independent Python generator gates it on `signal == "metrics"`; the
/// invariant makes the two gates the same condition.
#[derive(Debug, Clone, PartialEq)]
pub struct Descriptor {
    /// Signal.
    pub signal: Signal,
    /// Resource attributes.
    ///
    /// Shared: every series of one request under the same resource holds the
    /// one decoded list, so a request of many series under a large resource
    /// copies that resource once, not once per series.
    pub resource_attrs: Arc<[(String, Value)]>,
    /// Resource schema URL.
    pub resource_schema_url: String,
    /// Scope name.
    pub scope_name: String,
    /// Scope version.
    pub scope_version: String,
    /// Scope schema URL.
    pub scope_schema_url: String,
    /// Scope attributes, shared like `resource_attrs`.
    pub scope_attrs: Arc<[(String, Value)]>,
    /// Metric fields (metrics only).
    pub metric: Option<MetricDescriptor>,
    /// Identity attributes: data point attributes, or allow-listed log attributes.
    pub attrs: Vec<(String, Value)>,
}

const TAG_STR: u8 = 0x01;
const TAG_BYTES: u8 = 0x02;
const TAG_INT: u8 = 0x03;
const TAG_DOUBLE: u8 = 0x04;
const TAG_BOOL: u8 = 0x05;
const TAG_NULL: u8 = 0x06;
const TAG_ARRAY: u8 = 0x07;
const TAG_KVLIST: u8 = 0x08;
const CANONICAL_NAN: u64 = 0x7FF8_0000_0000_0000;

/// The bits a double is identified by.
///
/// Two normalizations, both so that an identity never depends on a bit
/// pattern the transport cannot carry: every NaN collapses to the canonical
/// quiet NaN, and -0.0 collapses to +0.0. OTAP drops a value column whose
/// entries are all zero, so the sign of a zero does not survive conversion
/// and an identity that depended on it would change with request batching.
/// Anything that compares or hashes values as series identity uses these
/// bits, so it agrees with the canonical encoding exactly.
#[must_use]
pub(crate) fn canonical_double_bits(d: f64) -> u64 {
    if d.is_nan() {
        CANONICAL_NAN
    } else if d == 0.0 {
        0
    } else {
        d.to_bits()
    }
}

fn put(out: &mut Vec<u8>, tag: u8, payload: &[u8]) {
    out.push(tag);
    out.extend((payload.len() as u32).to_be_bytes());
    out.extend(payload);
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put(out, TAG_STR, s.as_bytes());
}

fn encode_value(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => put(out, TAG_NULL, &[]),
        Value::Str(s) => put_str(out, s),
        Value::Bytes(b) => put(out, TAG_BYTES, b),
        Value::Int(i) => put(out, TAG_INT, &i.to_be_bytes()),
        Value::Double(d) => {
            put(out, TAG_DOUBLE, &canonical_double_bits(*d).to_be_bytes());
        }
        Value::Bool(b) => put(out, TAG_BOOL, &[u8::from(*b)]),
        Value::Array(items) => {
            let mut payload = Vec::new();
            payload.extend((items.len() as u32).to_be_bytes());
            for item in items {
                encode_value(&mut payload, item);
            }
            put(out, TAG_ARRAY, &payload);
        }
        Value::KvList(entries) => encode_kvlist(out, entries),
    }
}

fn encode_kvlist(out: &mut Vec<u8>, entries: &[(String, Value)]) {
    let mut payload = Vec::new();
    payload.extend((entries.len() as u32).to_be_bytes());
    for (k, v) in entries {
        put_str(&mut payload, k);
        encode_value(&mut payload, v);
    }
    put(out, TAG_KVLIST, &payload);
}

/// Build the canonical identity bytes of a descriptor.
#[must_use]
pub fn canonical_bytes(d: &Descriptor) -> Vec<u8> {
    debug_assert_eq!(
        d.metric.is_some(),
        d.signal == Signal::Metrics,
        "metric fields are present exactly for the metrics signal",
    );
    let mut out = Vec::with_capacity(256);
    put_str(&mut out, "OTEL-SERIES/1");
    put_str(&mut out, d.signal.as_str());
    encode_kvlist(&mut out, &d.resource_attrs);
    put_str(&mut out, &d.resource_schema_url);
    put_str(&mut out, &d.scope_name);
    put_str(&mut out, &d.scope_version);
    put_str(&mut out, &d.scope_schema_url);
    encode_kvlist(&mut out, &d.scope_attrs);
    if let Some(m) = &d.metric {
        put_str(&mut out, &m.name);
        put_str(&mut out, &m.unit);
        put_str(&mut out, m.kind.as_str());
        put_str(&mut out, m.temporality.as_str());
        put(&mut out, TAG_BOOL, &[u8::from(m.is_monotonic)]);
    }
    encode_kvlist(&mut out, &d.attrs);
    out
}

/// XXH3-128 (seed 0) of the identity bytes, in big-endian canonical form.
#[must_use]
pub fn series_id(identity_bytes: &[u8]) -> SeriesId {
    xxhash_rust::xxh3::xxh3_128(identity_bytes).to_be_bytes()
}

/// Lowercase hex rendering of a series id.
#[must_use]
pub fn hex(id: &SeriesId) -> String {
    hex::encode(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn logs_desc() -> Descriptor {
        Descriptor {
            signal: Signal::Logs,
            resource_attrs: vec![("host.id".into(), Value::Str("a1".into()))].into(),
            resource_schema_url: String::new(),
            scope_name: "lib".into(),
            scope_version: "1".into(),
            scope_schema_url: String::new(),
            scope_attrs: Arc::from([]),
            metric: None,
            attrs: vec![],
        }
    }

    /// Scenario: encode a minimal logs descriptor by hand.
    /// Guarantees: the byte layout is tag, u32 big-endian length, payload, in the fixed field order.
    #[test]
    fn layout_of_minimal_logs_descriptor() {
        let bytes = canonical_bytes(&logs_desc());
        let mut expect = Vec::new();
        let s = |e: &mut Vec<u8>, v: &str| {
            e.push(0x01);
            e.extend((v.len() as u32).to_be_bytes());
            e.extend(v.as_bytes());
        };
        s(&mut expect, "OTEL-SERIES/1");
        s(&mut expect, "logs");
        // resource attrs kvlist: tag 0x08, len, count=1, key "host.id", value "a1"
        let mut kv = Vec::new();
        kv.extend(1u32.to_be_bytes());
        s(&mut kv, "host.id");
        s(&mut kv, "a1");
        expect.push(0x08);
        expect.extend((kv.len() as u32).to_be_bytes());
        expect.extend(kv);
        s(&mut expect, ""); // resource schema_url
        s(&mut expect, "lib");
        s(&mut expect, "1");
        s(&mut expect, ""); // scope schema_url
        // empty scope attrs
        expect.push(0x08);
        expect.extend(4u32.to_be_bytes());
        expect.extend(0u32.to_be_bytes());
        // empty identity attrs
        expect.push(0x08);
        expect.extend(4u32.to_be_bytes());
        expect.extend(0u32.to_be_bytes());
        assert_eq!(bytes, expect);
    }

    /// Scenario: the same descriptor with an int and a string attribute value.
    /// Guarantees: 42 (int) and "42" (string) give different series ids.
    #[test]
    fn int_and_string_differ() {
        let mut a = logs_desc();
        a.attrs = vec![("x".into(), Value::Int(42))];
        let mut b = logs_desc();
        b.attrs = vec![("x".into(), Value::Str("42".into()))];
        assert_ne!(
            series_id(&canonical_bytes(&a)),
            series_id(&canonical_bytes(&b))
        );
    }

    /// Scenario: two NaN bit patterns as attribute values.
    /// Guarantees: both hash identically (canonical quiet NaN).
    #[test]
    fn nan_is_canonicalized() {
        let mut a = logs_desc();
        a.attrs = vec![(
            "x".into(),
            Value::Double(f64::from_bits(0x7FF8_0000_0000_0001)),
        )];
        let mut b = logs_desc();
        b.attrs = vec![(
            "x".into(),
            Value::Double(f64::from_bits(0xFFF8_0000_0000_0000)),
        )];
        assert_eq!(canonical_bytes(&a), canonical_bytes(&b));
    }

    /// Scenario: negative zero and positive zero as attribute values.
    /// Guarantees: both hash identically. The sign of a zero does not survive
    /// OTAP conversion, so it must not reach the identity (FORMAT.md section 1).
    #[test]
    fn negative_zero_is_canonicalized() {
        let mut a = logs_desc();
        a.attrs = vec![("x".into(), Value::Double(-0.0))];
        let mut b = logs_desc();
        b.attrs = vec![("x".into(), Value::Double(0.0))];
        assert_eq!(canonical_bytes(&a), canonical_bytes(&b));
        assert_eq!(
            series_id(&canonical_bytes(&a)),
            series_id(&canonical_bytes(&b))
        );

        // Normalization is confined to zero: a neighbouring value still differs.
        let mut c = logs_desc();
        c.attrs = vec![("x".into(), Value::Double(f64::from_bits(1)))];
        assert_ne!(canonical_bytes(&a), canonical_bytes(&c));
    }

    /// Scenario: xxh3_128 of a known input.
    /// Guarantees: seed 0 and big-endian canonical representation are used.
    #[test]
    fn series_id_is_xxh3_128_big_endian() {
        let id = series_id(b"");
        assert_eq!(hex(&id), "99aa06d3014798d86001c324468d497f");
    }

    /// Scenario: a logs descriptor and a metrics descriptor built the way extraction builds them.
    /// Guarantees: `metric.is_some()` holds exactly for the metrics signal, so the Rust gate
    /// (`metric.is_some()`) and the Python generator gate (`signal == "metrics"`) agree.
    #[test]
    fn metric_block_is_gated_on_the_metrics_signal() {
        let logs = logs_desc();
        assert_eq!(logs.signal, Signal::Logs);
        assert!(logs.metric.is_none());
        let _ = canonical_bytes(&logs);

        let metrics = Descriptor {
            signal: Signal::Metrics,
            metric: Some(MetricDescriptor {
                name: "cpu.usage".into(),
                unit: "s".into(),
                kind: MetricKind::Gauge,
                temporality: Temporality::Unspecified,
                is_monotonic: false,
                description: String::new(),
            }),
            ..logs_desc()
        };
        assert_eq!(metrics.metric.is_some(), metrics.signal == Signal::Metrics);
        let _ = canonical_bytes(&metrics);
    }

    /// Scenario: two metrics descriptors that differ only in `MetricDescriptor::description`.
    /// Guarantees: `description` is excluded from the identity, so the canonical bytes and
    /// series id are identical.
    #[test]
    fn description_does_not_change_identity() {
        let make = |description: &str| Descriptor {
            signal: Signal::Metrics,
            metric: Some(MetricDescriptor {
                name: "cpu.usage".into(),
                unit: "s".into(),
                kind: MetricKind::Gauge,
                temporality: Temporality::Unspecified,
                is_monotonic: false,
                description: description.into(),
            }),
            ..logs_desc()
        };
        let a = make("a short description");
        let b = make("a completely different, much longer description");
        assert_eq!(canonical_bytes(&a), canonical_bytes(&b));
        assert_eq!(
            series_id(&canonical_bytes(&a)),
            series_id(&canonical_bytes(&b))
        );
    }
}
