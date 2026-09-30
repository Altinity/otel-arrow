// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Canonical encoding v1 and series identity (docs/FORMAT.md section 1). Every value is
//! `tag:u8 ++ len:u32_be ++ payload`. The writers below are the only encoder: the columnar
//! extraction calls them directly, and `canonical_bytes` (tests) drives them from a `Descriptor`
//! to check the golden vectors.

use xxhash_rust::xxh3::xxh3_64;

use super::value::Value;

/// `format_version` of the files and of the `v=1` path segment.
pub const FORMAT_VERSION: &str = "1";
/// `series_hash` footer value.
pub const SERIES_HASH: &str = "xxh3_128/canonical_v1";
/// First identity field: namespace and encoding version.
const NAMESPACE: &str = "OTEL-SERIES/1";

const TAG_STR: u8 = 0x01;
const TAG_BYTES: u8 = 0x02;
const TAG_INT: u8 = 0x03;
const TAG_DOUBLE: u8 = 0x04;
const TAG_BOOL: u8 = 0x05;
const TAG_NULL: u8 = 0x06;
const TAG_ARRAY: u8 = 0x07;
const TAG_KVLIST: u8 = 0x08;

/// Telemetry signal handled in v1.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Signal {
    /// Logs.
    Logs,
    /// Metrics.
    Metrics,
}

impl Signal {
    /// Canonical string, also the `signal=` path value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Logs => "logs",
            Self::Metrics => "metrics",
        }
    }

    /// `identity_config` footer value: the writer settings that select identity fields. This
    /// writer has no log attribute allow-list, which is the empty list of v1.
    #[must_use]
    pub const fn identity_config(self) -> &'static str {
        match self {
            Self::Logs => "{\"series_attributes\":[]}",
            Self::Metrics => "{}",
        }
    }

    /// `identity_config_hash` footer value: xxh3_64 of `identity_config`, 16 lowercase hex digits.
    #[must_use]
    pub fn identity_config_hash(self) -> String {
        format!("{:016x}", xxh3_64(self.identity_config().as_bytes()))
    }
}

/// Metric point kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
    /// Canonical string, also the `metric_type` column value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Gauge => "gauge",
            Self::Sum => "sum",
            Self::Histogram => "histogram",
            Self::ExpHistogram => "exp_histogram",
            Self::Summary => "summary",
        }
    }
}

/// Aggregation temporality.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Temporality {
    /// Not specified (gauges and summaries).
    Unspecified,
    /// Delta.
    Delta,
    /// Cumulative.
    Cumulative,
}

impl Temporality {
    /// Canonical string, also the `temporality` column value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "",
            Self::Delta => "delta",
            Self::Cumulative => "cumulative",
        }
    }
}

/// The bits a double is identified by: every NaN is the canonical quiet NaN, -0.0 is +0.0.
#[must_use]
pub fn double_bits(d: f64) -> u64 {
    if d.is_nan() {
        0x7FF8_0000_0000_0000
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

/// String value.
pub fn put_str(out: &mut Vec<u8>, s: &str) {
    put(out, TAG_STR, s.as_bytes());
}

/// Bytes value.
pub fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put(out, TAG_BYTES, b);
}

/// Integer value.
pub fn put_int(out: &mut Vec<u8>, v: i64) {
    put(out, TAG_INT, &v.to_be_bytes());
}

/// Double value.
pub fn put_double(out: &mut Vec<u8>, v: f64) {
    put(out, TAG_DOUBLE, &double_bits(v).to_be_bytes());
}

/// Bool value.
pub fn put_bool(out: &mut Vec<u8>, v: bool) {
    put(out, TAG_BOOL, &[u8::from(v)]);
}

/// Unset value.
pub fn put_null(out: &mut Vec<u8>) {
    put(out, TAG_NULL, &[]);
}

/// Start a kvlist of `count` entries. The caller appends each entry as `put_str(key)` followed by
/// the value, in ascending order of the raw key bytes, then calls [`end_kvlist`] with the returned
/// position.
#[must_use]
pub fn begin_kvlist(out: &mut Vec<u8>, count: usize) -> usize {
    out.push(TAG_KVLIST);
    let at = out.len();
    out.extend([0_u8; 4]);
    out.extend((count as u32).to_be_bytes());
    at
}

/// Patch the payload length of the kvlist started at `at`.
pub fn end_kvlist(out: &mut [u8], at: usize) {
    let len = (out.len() - at - 4) as u32;
    out[at..at + 4].copy_from_slice(&len.to_be_bytes());
}

/// Any value (nested values come from the decoded `ser` column).
pub fn encode_value(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => put_null(out),
        Value::Str(s) => put_str(out, s),
        Value::Bytes(b) => put_bytes(out, b),
        Value::Int(i) => put_int(out, *i),
        Value::Double(d) => put_double(out, *d),
        Value::Bool(b) => put_bool(out, *b),
        Value::Array(items) => {
            let mut payload = Vec::new();
            payload.extend((items.len() as u32).to_be_bytes());
            for item in items {
                encode_value(&mut payload, item);
            }
            put(out, TAG_ARRAY, &payload);
        }
        Value::KvList(entries) => {
            let at = begin_kvlist(out, entries.len());
            for (k, v) in entries {
                put_str(out, k);
                encode_value(out, v);
            }
            end_kvlist(out, at);
        }
    }
}

/// The two leading identity fields: namespace and signal.
#[must_use]
pub fn key_prefix(signal: Signal) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    put_str(&mut out, NAMESPACE);
    put_str(&mut out, signal.as_str());
    out
}

/// series_id: XXH3-128 (seed 0) of the identity bytes; stored as `to_be_bytes()`. Production code
/// computes it with a streaming state (`extract.rs`); this one-shot form is the test oracle.
#[cfg(test)]
#[must_use]
pub fn series_id(identity_bytes: &[u8]) -> u128 {
    xxhash_rust::xxh3::xxh3_128(identity_bytes)
}

/// Metric-level identity fields (test oracle).
#[cfg(test)]
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
}

/// Everything that identifies a series (test oracle). Attribute lists must be sorted by raw key
/// bytes with unique keys.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub struct Descriptor {
    /// Signal.
    pub signal: Signal,
    /// Resource attributes.
    pub resource_attrs: Vec<(String, Value)>,
    /// Resource schema URL.
    pub resource_schema_url: String,
    /// Scope name.
    pub scope_name: String,
    /// Scope version.
    pub scope_version: String,
    /// Scope schema URL.
    pub scope_schema_url: String,
    /// Scope attributes.
    pub scope_attrs: Vec<(String, Value)>,
    /// Metric fields (metrics only).
    pub metric: Option<MetricDescriptor>,
    /// Identity attributes: data point attributes; empty for logs in this writer.
    pub attrs: Vec<(String, Value)>,
}

/// Identity bytes of a descriptor, in the fixed field order of FORMAT.md section 1.
#[cfg(test)]
#[must_use]
pub fn canonical_bytes(d: &Descriptor) -> Vec<u8> {
    let kvlist = |out: &mut Vec<u8>, list: &[(String, Value)]| {
        let at = begin_kvlist(out, list.len());
        for (k, v) in list {
            put_str(out, k);
            encode_value(out, v);
        }
        end_kvlist(out, at);
    };
    let mut out = key_prefix(d.signal);
    kvlist(&mut out, &d.resource_attrs);
    put_str(&mut out, &d.resource_schema_url);
    put_str(&mut out, &d.scope_name);
    put_str(&mut out, &d.scope_version);
    put_str(&mut out, &d.scope_schema_url);
    kvlist(&mut out, &d.scope_attrs);
    if let Some(m) = &d.metric {
        put_str(&mut out, &m.name);
        put_str(&mut out, &m.unit);
        put_str(&mut out, m.kind.as_str());
        put_str(&mut out, m.temporality.as_str());
        put_bool(&mut out, m.is_monotonic);
    }
    kvlist(&mut out, &d.attrs);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::parquet_lake_exporter::error::LakeError;
    use crate::exporters::parquet_lake_exporter::limits::Budget;
    use crate::exporters::parquet_lake_exporter::test_fixtures::{
        descriptor_from_json, golden, hex_decode, value_from_json,
    };
    use crate::exporters::parquet_lake_exporter::value::decode_cbor;

    /// Value equality of the golden vectors: doubles compare by bit pattern, and any two NaNs are
    /// the same value (the encoder canonicalizes NaN payloads).
    fn same_value(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Double(x), Value::Double(y)) => {
                (x.is_nan() && y.is_nan()) || x.to_bits() == y.to_bits()
            }
            (Value::Array(x), Value::Array(y)) => {
                x.len() == y.len() && x.iter().zip(y).all(|(a, b)| same_value(a, b))
            }
            (Value::KvList(x), Value::KvList(y)) => {
                x.len() == y.len()
                    && x.iter()
                        .zip(y)
                        .all(|((ka, va), (kb, vb))| ka == kb && same_value(va, vb))
            }
            _ => a == b,
        }
    }

    /// Scenario: every canonical_v1 vector produced by the reference implementation's independent Python encoder (35 descriptors: scalars, NaN and zero normalization, nested values, all metric kinds, schema URLs).
    /// Guarantees: The encoder reproduces the identity bytes and the series id byte for byte, so this writer and exporter:series_parquet give the same series the same id.
    #[test]
    fn canonical_v1_golden_vectors_match() {
        let doc = golden("canonical_v1");
        let vectors = doc["vectors"].as_array().expect("vectors");
        assert_eq!(vectors.len(), 35);
        for v in vectors {
            let name = v["name"].as_str().expect("name");
            // This writer has no log attribute allow-list: a logs vector with identity attributes
            // still checks the encoder, which takes whatever list it is given.
            let bytes = canonical_bytes(&descriptor_from_json(&v["descriptor"]));
            assert_eq!(
                hex::encode(&bytes),
                v["canonical_hex"].as_str().expect("hex"),
                "{name}"
            );
            assert_eq!(
                hex::encode(series_id(&bytes).to_be_bytes()),
                v["series_id_hex"].as_str().expect("id"),
                "{name}"
            );
        }
    }

    /// Scenario: every cbor_v1 vector (48 raw `ser` cells: each major type, null and undefined keys, bignums at the int64 limits, half and single floats, indefinite lengths, duplicate keys, other tags, the nesting limit, truncation).
    /// Guarantees: The decoder yields the reference value with the same identity bytes and series id, or the same refusal class (invalid content or too deep).
    #[test]
    fn cbor_v1_golden_vectors_match() {
        let doc = golden("cbor_v1");
        let vectors = doc["vectors"].as_array().expect("vectors");
        assert_eq!(vectors.len(), 48);
        let base = golden("canonical_v1");
        let logs_minimal = base["vectors"]
            .as_array()
            .expect("vectors")
            .iter()
            .find(|v| v["name"] == "logs_minimal")
            .expect("logs_minimal")["descriptor"]
            .clone();
        for v in vectors {
            let name = v["name"].as_str().expect("name");
            let depth =
                usize::try_from(v["max_depth"].as_u64().expect("max_depth")).expect("depth");
            let cell = hex_decode(v["cbor_hex"].as_str().expect("cbor_hex"));
            let decoded = decode_cbor(&cell, depth, &mut Budget::new(1 << 30));
            match v.get("refused").map(|r| r.as_str().expect("refused")) {
                Some("invalid") => {
                    assert!(
                        matches!(decoded, Err(LakeError::Invalid(_))),
                        "{name}: {decoded:?}"
                    );
                }
                Some("too_deep") => {
                    assert!(
                        matches!(decoded, Err(LakeError::TooDeep(_))),
                        "{name}: {decoded:?}"
                    );
                }
                Some(other) => panic!("{name}: unknown refusal {other}"),
                None => {
                    let decoded = decoded.unwrap_or_else(|e| panic!("{name} must decode: {e}"));
                    assert!(
                        same_value(&decoded, &value_from_json(&v["value"])),
                        "{name}: {decoded:?}"
                    );
                    let mut d = descriptor_from_json(&logs_minimal);
                    d.attrs = vec![("k".to_owned(), decoded)];
                    let bytes = canonical_bytes(&d);
                    assert_eq!(
                        hex::encode(&bytes),
                        v["canonical_hex"].as_str().expect("hex"),
                        "{name}"
                    );
                    assert_eq!(
                        hex::encode(series_id(&bytes).to_be_bytes()),
                        v["series_id_hex"].as_str().expect("id"),
                        "{name}"
                    );
                }
            }
        }
    }

    /// Scenario: the identity_config vectors for logs with an empty allow-list and for metrics.
    /// Guarantees: The footer values this writer emits equal the reference form and hash, so files of both writers are comparable.
    #[test]
    fn identity_config_matches_the_golden_vectors() {
        let doc = golden("identity_config_v1");
        let mut checked = 0;
        for v in doc["vectors"].as_array().expect("vectors") {
            if !v["series_attributes"].as_array().expect("list").is_empty() {
                continue;
            }
            let signal = match v["signal"].as_str().expect("signal") {
                "logs" => Signal::Logs,
                _ => Signal::Metrics,
            };
            assert_eq!(
                signal.identity_config(),
                v["identity_config"].as_str().expect("config")
            );
            assert_eq!(
                signal.identity_config_hash(),
                v["identity_config_hash"].as_str().expect("hash")
            );
            checked += 1;
        }
        assert_eq!(checked, 2, "one logs and one metrics vector");
    }
}
