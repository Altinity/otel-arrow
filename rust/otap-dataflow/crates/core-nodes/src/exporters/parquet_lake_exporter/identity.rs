// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Canonical identity encoding and series_id (see docs/FORMAT.md).

use xxhash_rust::xxh3::xxh3_128;

/// Version of the canonical encoding and of the datasets.
pub const FORMAT_VERSION: u8 = 1;
/// Maximum nesting of array/map values (matches pdata's CBOR limit). A deeper value encodes as
/// EMPTY, like an undecodable one, so one bad attribute never refuses a whole batch.
pub const MAX_DEPTH: usize = 128;

/// Signal tag in the identity encoding and in object paths.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Signal {
    /// Logs (tag 1).
    Logs = 1,
    /// Metrics (tag 2).
    Metrics = 2,
}

impl Signal {
    /// Path segment.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Logs => "logs",
            Self::Metrics => "metrics",
        }
    }
}

/// Value tags (FORMAT.md section "Identity grammar").
pub mod tag {
    /// Empty value.
    pub const EMPTY: u8 = 0;
    /// UTF-8 string.
    pub const STR: u8 = 1;
    /// Signed 64-bit integer.
    pub const INT: u8 = 2;
    /// IEEE-754 double.
    pub const DOUBLE: u8 = 3;
    /// Boolean.
    pub const BOOL: u8 = 4;
    /// Byte string.
    pub const BYTES: u8 = 5;
    /// Array of values.
    pub const ARRAY: u8 = 6;
    /// Map (attribute list).
    pub const MAP: u8 = 7;
}

/// `u32le(len) || bytes`.
pub fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

/// Canonical double bits: -0.0 -> +0.0, every NaN -> 0x7ff8000000000000.
#[must_use]
pub fn double_bits(v: f64) -> u64 {
    if v.is_nan() {
        0x7ff8_0000_0000_0000
    } else if v == 0.0 {
        0
    } else {
        v.to_bits()
    }
}

/// String value.
pub fn put_str(out: &mut Vec<u8>, s: &[u8]) {
    out.push(tag::STR);
    put_bytes(out, s);
}

/// Integer value.
pub fn put_int(out: &mut Vec<u8>, v: i64) {
    out.push(tag::INT);
    out.extend_from_slice(&v.to_le_bytes());
}

/// Double value.
pub fn put_double(out: &mut Vec<u8>, v: f64) {
    out.push(tag::DOUBLE);
    out.extend_from_slice(&double_bits(v).to_le_bytes());
}

/// Bool value.
pub fn put_bool(out: &mut Vec<u8>, v: bool) {
    out.push(tag::BOOL);
    out.push(u8::from(v));
}

/// Bytes value.
pub fn put_bin(out: &mut Vec<u8>, b: &[u8]) {
    out.push(tag::BYTES);
    put_bytes(out, b);
}

/// Write an attribute list: `u32le n || (bytes(key) || value)*`, sorted by (key bytes, encoded
/// value bytes); among duplicate keys the first after sorting wins. Independent of input order.
pub fn put_attrs(out: &mut Vec<u8>, mut entries: Vec<(&[u8], Vec<u8>)>) {
    entries.sort_unstable_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(&b.1)));
    entries.dedup_by(|later, earlier| later.0 == earlier.0);
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (k, v) in entries {
        put_bytes(out, k);
        out.extend_from_slice(&v);
    }
}

/// Encode a CBOR-serialized AnyValue (pdata `ser` column) canonically. A value that cannot be
/// decoded, nests deeper than MAX_DEPTH, or has a non-text map key / out-of-range integer encodes
/// as EMPTY (FORMAT.md section 3).
pub fn put_cbor(out: &mut Vec<u8>, cbor: &[u8]) {
    let start = out.len();
    let ok = ciborium::from_reader::<ciborium::Value, _>(cbor)
        .ok()
        .and_then(|v| put_cbor_value(out, &v, 0));
    if ok.is_none() {
        out.truncate(start);
        out.push(tag::EMPTY);
    }
}

fn put_cbor_value(out: &mut Vec<u8>, v: &ciborium::Value, depth: usize) -> Option<()> {
    use ciborium::Value as C;
    if depth > MAX_DEPTH {
        return None;
    }
    match v {
        C::Null => out.push(tag::EMPTY),
        C::Text(s) => put_str(out, s.as_bytes()),
        C::Integer(i) => put_int(out, i64::try_from(i128::from(*i)).ok()?),
        C::Float(f) => put_double(out, *f),
        C::Bool(b) => put_bool(out, *b),
        C::Bytes(b) => put_bin(out, b),
        C::Array(items) => {
            out.push(tag::ARRAY);
            out.extend_from_slice(&(items.len() as u32).to_le_bytes());
            for item in items {
                put_cbor_value(out, item, depth + 1)?;
            }
        }
        C::Map(entries) => {
            out.push(tag::MAP);
            let mut enc = Vec::with_capacity(entries.len());
            for (k, val) in entries {
                let C::Text(key) = k else {
                    return None;
                };
                let mut b = Vec::new();
                put_cbor_value(&mut b, val, depth + 1)?;
                enc.push((key.as_bytes(), b));
            }
            put_attrs(out, enc);
        }
        C::Tag(_, inner) => put_cbor_value(out, inner, depth)?,
        _ => return None,
    }
    Some(())
}

/// series_id: XXH3-128 (seed 0) of the canonical key; stored as `to_be_bytes()` in
/// FixedSizeBinary(16).
#[must_use]
pub fn series_id(key: &[u8]) -> u128 {
    xxh3_128(key)
}

/// Start a key: `u8 FORMAT_VERSION || u8 signal`.
#[must_use]
pub fn key_prefix(signal: Signal) -> Vec<u8> {
    vec![FORMAT_VERSION, signal as u8]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn val(f: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        let mut out = Vec::new();
        f(&mut out);
        out
    }

    fn s(v: &str) -> Vec<u8> {
        val(|o| put_str(o, v.as_bytes()))
    }

    fn i(v: i64) -> Vec<u8> {
        val(|o| put_int(o, v))
    }

    fn d(v: f64) -> Vec<u8> {
        val(|o| put_double(o, v))
    }

    fn cbor(bytes: &[u8]) -> Vec<u8> {
        val(|o| put_cbor(o, bytes))
    }

    fn attrs(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
        val(|o| {
            put_attrs(
                o,
                entries
                    .iter()
                    .map(|(k, v)| (k.as_bytes(), v.clone()))
                    .collect(),
            );
        })
    }

    fn logs_key(resource: &[(&str, Vec<u8>)], scope: (&str, &str)) -> Vec<u8> {
        let mut k = key_prefix(Signal::Logs);
        k.extend(attrs(resource));
        put_bytes(&mut k, scope.0.as_bytes());
        put_bytes(&mut k, scope.1.as_bytes());
        k.extend(attrs(&[]));
        k
    }

    fn metric_key(name: &str, unit: &str, ty: u8, temporality: i32, monotonic: bool) -> Vec<u8> {
        let mut k = key_prefix(Signal::Metrics);
        k.extend(attrs(&[("host.name", s("h1"))]));
        put_bytes(&mut k, b"lib");
        put_bytes(&mut k, b"1.0");
        k.extend(attrs(&[]));
        put_bytes(&mut k, name.as_bytes());
        put_bytes(&mut k, unit.as_bytes());
        k.push(ty);
        k.extend_from_slice(&temporality.to_le_bytes());
        k.push(u8::from(monotonic));
        k.extend(attrs(&[("core", s("0"))]));
        k
    }

    fn cbor_of(v: &ciborium::Value) -> Vec<u8> {
        let mut out = Vec::new();
        ciborium::into_writer(v, &mut out).expect("cbor encodes");
        out
    }

    /// The FORMAT.md golden vectors: (name, canonical key).
    fn vectors() -> Vec<(&'static str, Vec<u8>)> {
        use ciborium::Value as C;
        let base = [("service.name", s("api")), ("host.name", s("h1"))];
        let nested = cbor_of(&C::Map(vec![
            (C::Text("b".into()), C::Integer(2.into())),
            (C::Text("a".into()), C::Bool(true)),
        ]));
        let array = cbor_of(&C::Array(vec![
            C::Integer(1.into()),
            C::Text("x".into()),
            C::Float(2.5),
        ]));
        vec![
            ("logs_resource_scope", logs_key(&base, ("lib", "1.0"))),
            ("logs_int_1", logs_key(&[("k", i(1))], ("", ""))),
            ("logs_str_1", logs_key(&[("k", s("1"))], ("", ""))),
            ("logs_int_0_default", logs_key(&[("k", i(0))], ("", ""))),
            (
                "logs_empty_value",
                logs_key(&[("k", vec![tag::EMPTY])], ("", "")),
            ),
            (
                "logs_duplicate_key",
                logs_key(&[("k", s("b")), ("k", s("a"))], ("", "")),
            ),
            (
                "logs_nested_map",
                logs_key(&[("m", cbor(&nested))], ("", "")),
            ),
            ("logs_array", logs_key(&[("arr", cbor(&array))], ("", ""))),
            (
                "logs_unicode_key",
                logs_key(&[("cl\u{e9}", s("v\u{e4}rde"))], ("", "")),
            ),
            (
                "logs_neg_zero_nan",
                logs_key(&[("d", d(-0.0)), ("n", d(f64::NAN))], ("", "")),
            ),
            ("metric_gauge", metric_key("cpu", "1", 1, 0, false)),
            (
                "metric_sum",
                metric_key("requests", "{request}", 2, 2, true),
            ),
            ("metric_histogram", metric_key("latency", "ms", 3, 1, false)),
            (
                "metric_exp_histogram",
                metric_key("latency.exp", "ms", 4, 1, false),
            ),
            ("metric_summary", metric_key("rpc", "ms", 5, 0, false)),
        ]
    }

    /// Frozen (name, key hex, series_id hex); mirrored in docs/FORMAT.md section 8.
    const GOLDEN: &[(&str, &str, &str)] = &[
        (
            "logs_resource_scope",
            "01010200000009000000686f73742e6e616d65010200000068310c000000736572766963652e6e616d650103000000617069030000006c696203000000312e3000000000",
            "694ace1a971682b44a4b698f7cfdeb1e",
        ),
        (
            "logs_int_1",
            "010101000000010000006b020100000000000000000000000000000000000000",
            "99296919f5925b76229ae7f5c93ca6a6",
        ),
        (
            "logs_str_1",
            "010101000000010000006b010100000031000000000000000000000000",
            "b9687c6cad6225d11d69ae54ac007462",
        ),
        (
            "logs_int_0_default",
            "010101000000010000006b020000000000000000000000000000000000000000",
            "345d3e1709380fb136d1562b3b2e1740",
        ),
        (
            "logs_empty_value",
            "010101000000010000006b00000000000000000000000000",
            "00f56a30ad7296e237f6375084921709",
        ),
        (
            "logs_duplicate_key",
            "010101000000010000006b010100000061000000000000000000000000",
            "39b47467b46e7249606c589102b22ae7",
        ),
        (
            "logs_nested_map",
            "010101000000010000006d0702000000010000006104010100000062020200000000000000000000000000000000000000",
            "ad06b0f1308ff65f0810634c90dd4196",
        ),
        (
            "logs_array",
            "010101000000030000006172720603000000020100000000000000010100000078030000000000000440000000000000000000000000",
            "ef02c97fc12d600798d0ccc297c9e741",
        ),
        (
            "logs_unicode_key",
            "01010100000004000000636cc3a9010600000076c3a4726465000000000000000000000000",
            "9c0c02d2dfcc96d610830fa743ccf5b5",
        ),
        (
            "logs_neg_zero_nan",
            "0101020000000100000064030000000000000000010000006e03000000000000f87f000000000000000000000000",
            "5368d975dfa7d7ac82f24ee4cd8e14b2",
        ),
        (
            "metric_gauge",
            "01020100000009000000686f73742e6e616d6501020000006831030000006c696203000000312e30000000000300000063707501000000310100000000000100000004000000636f7265010100000030",
            "35ed2533eb63eb8208cd6b88eca0302d",
        ),
        (
            "metric_sum",
            "01020100000009000000686f73742e6e616d6501020000006831030000006c696203000000312e3000000000080000007265717565737473090000007b726571756573747d0202000000010100000004000000636f7265010100000030",
            "a1977a29577bae41f59c18f7e9687932",
        ),
        (
            "metric_histogram",
            "01020100000009000000686f73742e6e616d6501020000006831030000006c696203000000312e3000000000070000006c6174656e6379020000006d730301000000000100000004000000636f7265010100000030",
            "d91ca6f51242dbf8fc20f9aa0837e4ed",
        ),
        (
            "metric_exp_histogram",
            "01020100000009000000686f73742e6e616d6501020000006831030000006c696203000000312e30000000000b0000006c6174656e63792e657870020000006d730401000000000100000004000000636f7265010100000030",
            "8c1cc1efcf4a63cff08c875106f94429",
        ),
        (
            "metric_summary",
            "01020100000009000000686f73742e6e616d6501020000006831030000006c696203000000312e300000000003000000727063020000006d730500000000000100000004000000636f7265010100000030",
            "1473f50e8eb245e7cb1a6bb09ee60592",
        ),
    ];

    /// Scenario: The FORMAT.md golden identities are encoded with the production encoder.
    /// Guarantees: Each canonical key and its big-endian XXH3-128 series_id match the frozen hex, so writers in any language can check themselves against FORMAT.md.
    #[test]
    fn golden_vectors_match_format_md() {
        let got: Vec<(String, String, String)> = vectors()
            .into_iter()
            .map(|(name, key)| {
                (
                    name.to_owned(),
                    hex::encode(&key),
                    hex::encode(series_id(&key).to_be_bytes()),
                )
            })
            .collect();
        let want: Vec<(String, String, String)> = GOLDEN
            .iter()
            .map(|(n, k, i)| ((*n).to_owned(), (*k).to_owned(), (*i).to_owned()))
            .collect();
        assert_eq!(got, want, "golden vectors changed:\n{got:#?}");
    }

    /// Scenario: The first golden key is spelled out byte by byte from the FORMAT.md grammar.
    /// Guarantees: The encoder layout (version, signal, u32le counts and lengths, tags) matches the written grammar, independent of the frozen hex.
    #[test]
    fn grammar_matches_hand_encoded_key() {
        let key = logs_key(&[("a", s("b"))], ("s", ""));
        let want: Vec<u8> = [
            &[1u8, 1][..],          // version 1, signal logs
            &[1, 0, 0, 0],          // 1 resource attribute
            &[1, 0, 0, 0, b'a'],    // key "a"
            &[1, 1, 0, 0, 0, b'b'], // tag str, "b"
            &[1, 0, 0, 0, b's'],    // scope name "s"
            &[0, 0, 0, 0],          // scope version ""
            &[0, 0, 0, 0],          // 0 scope attributes
        ]
        .concat();
        assert_eq!(key, want);
    }

    /// Scenario: The same attributes are given in two different orders.
    /// Guarantees: The canonical key (and so the series_id) does not depend on attribute order.
    #[test]
    fn attribute_order_does_not_change_id() {
        let a = logs_key(&[("x", s("1")), ("y", i(2))], ("", ""));
        let b = logs_key(&[("y", i(2)), ("x", s("1"))], ("", ""));
        assert_eq!(series_id(&a), series_id(&b));
    }

    /// Scenario: One attribute holds int 1 in one identity and string "1" in the other.
    /// Guarantees: Value types are part of the identity, so the series ids differ.
    #[test]
    fn typed_values_change_id() {
        let a = logs_key(&[("k", i(1))], ("", ""));
        let b = logs_key(&[("k", s("1"))], ("", ""));
        assert_ne!(series_id(&a), series_id(&b));
    }

    /// Scenario: Doubles -0.0 and +0.0, and two NaNs with different payload bits, are encoded.
    /// Guarantees: -0.0 encodes like +0.0 and every NaN encodes as 0x7ff8000000000000.
    #[test]
    fn negative_zero_and_nan_are_canonical() {
        assert_eq!(d(-0.0), d(0.0));
        let other_nan = f64::from_bits(0x7ff8_0000_0000_0001);
        assert!(other_nan.is_nan());
        assert_eq!(d(other_nan), d(f64::NAN));
        assert_eq!(double_bits(f64::NAN), 0x7ff8_0000_0000_0000);
    }

    /// Scenario: A key appears twice with different values, in both input orders.
    /// Guarantees: The entry with the smallest encoded value wins regardless of order, and the list holds the key once.
    #[test]
    fn duplicate_keys_resolve_by_value_bytes_in_any_order() {
        let a = attrs(&[("k", s("b")), ("k", s("a"))]);
        let b = attrs(&[("k", s("a")), ("k", s("b"))]);
        assert_eq!(a, b);
        assert_eq!(a, attrs(&[("k", s("a"))]));
    }

    /// Scenario: The same double is serialized as a packed CBOR float32 and as a float64.
    /// Guarantees: Both encode identically, so CBOR float packing does not change the id.
    #[test]
    fn cbor_float32_packed_equals_f64() {
        let f32_bytes = [0xfa, 0x40, 0x20, 0x00, 0x00]; // 2.5 as float32
        let f64_bytes = [0xfb, 0x40, 0x04, 0, 0, 0, 0, 0, 0]; // 2.5 as float64
        assert_eq!(cbor(&f32_bytes), cbor(&f64_bytes));
        assert_eq!(cbor(&f64_bytes), d(2.5));
    }

    /// Scenario: A CBOR value nests arrays deeper than MAX_DEPTH (128), and another is nested exactly MAX_DEPTH deep.
    /// Guarantees: The too-deep value encodes as EMPTY instead of failing (the batch is not refused), while the value at the limit keeps its structure.
    #[test]
    fn too_deep_encodes_as_empty() {
        let nested = |levels: usize| {
            let mut bytes = vec![0x81; levels]; // array(1) nested
            bytes.push(0x01);
            bytes
        };
        assert_eq!(cbor(&nested(MAX_DEPTH + 2)), vec![tag::EMPTY]);
        assert_eq!(cbor(&nested(MAX_DEPTH))[0], tag::ARRAY);
    }

    /// Scenario: Serialized values are undecodable, use an integer map key, or hold an integer beyond i64.
    /// Guarantees: Each encodes as EMPTY instead of failing, so one bad attribute never refuses a batch.
    #[test]
    fn invalid_cbor_encodes_as_empty() {
        assert_eq!(cbor(&[0xff, 0xff]), vec![tag::EMPTY]);
        assert_eq!(cbor(&[0xa1, 0x01, 0x02]), vec![tag::EMPTY]);
        let big = [0x1b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]; // u64::MAX
        assert_eq!(cbor(&big), vec![tag::EMPTY]);
    }
}
