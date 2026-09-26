// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Fuzz-style property tests for the canonical encoder and CBOR decoder (FORMAT.md section 1).

use otel_arrow_dfe_series_lake::canonical::{Descriptor, Signal, canonical_bytes, series_id};
use otel_arrow_dfe_series_lake::value::{
    DecodeLimits, Unbounded, Value, body_string_reserving, decode_cbor, map_string, rendered_len,
    sort_kvlist,
};
use proptest::prelude::*;

fn value_strategy() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<String>().prop_map(Value::Str),
        prop::collection::vec(any::<u8>(), 0..16).prop_map(Value::Bytes),
        any::<i64>().prop_map(Value::Int),
        any::<u64>().prop_map(|b| Value::Double(f64::from_bits(b))),
        any::<bool>().prop_map(Value::Bool),
    ];
    leaf.prop_recursive(4, 32, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::vec((any::<String>(), inner), 0..4).prop_map(|mut kvs| {
                kvs.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
                kvs.dedup_by(|a, b| a.0 == b.0);
                Value::KvList(kvs)
            }),
        ]
    })
}

fn to_cbor(v: &Value) -> ciborium::Value {
    match v {
        Value::Null => ciborium::Value::Null,
        Value::Str(s) => ciborium::Value::Text(s.clone()),
        Value::Bytes(b) => ciborium::Value::Bytes(b.clone()),
        Value::Int(i) => ciborium::Value::Integer((*i).into()),
        Value::Double(d) => ciborium::Value::Float(*d),
        Value::Bool(b) => ciborium::Value::Bool(*b),
        Value::Array(items) => ciborium::Value::Array(items.iter().map(to_cbor).collect()),
        Value::KvList(kvs) => ciborium::Value::Map(
            kvs.iter()
                .map(|(k, v)| (ciborium::Value::Text(k.clone()), to_cbor(v)))
                .collect(),
        ),
    }
}

fn canon_nan(v: &Value) -> Value {
    match v {
        Value::Double(d) if d.is_nan() => Value::Double(f64::NAN),
        Value::Array(items) => Value::Array(items.iter().map(canon_nan).collect()),
        Value::KvList(kvs) => {
            Value::KvList(kvs.iter().map(|(k, v)| (k.clone(), canon_nan(v))).collect())
        }
        other => other.clone(),
    }
}

fn desc(attrs: Vec<(String, Value)>) -> Descriptor {
    Descriptor {
        signal: Signal::Logs,
        resource_attrs: vec![].into(),
        resource_schema_url: String::new(),
        scope_name: String::new(),
        scope_version: String::new(),
        scope_schema_url: String::new(),
        scope_attrs: vec![].into(),
        metric: None,
        attrs,
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, .. ProptestConfig::default() })]

    /// Scenario: arbitrary value trees round-trip through CBOR.
    /// Guarantees: decode never panics and reproduces the tree (NaN payloads excepted).
    #[test]
    fn cbor_round_trip_never_panics(v in value_strategy()) {
        let mut buf = Vec::new();
        ciborium::into_writer(&to_cbor(&v), &mut buf).expect("encode");
        let decoded = decode_cbor(&buf, DecodeLimits::new(32, usize::MAX)).expect("decode");
        let a = canonical_bytes(&desc(vec![("k".into(), canon_nan(&v))]));
        let b = canonical_bytes(&desc(vec![("k".into(), canon_nan(&decoded))]));
        prop_assert_eq!(a, b);
    }

    /// Scenario: arbitrary bytes and truncated well-formed encodings fed to the CBOR decoder.
    /// Guarantees: it never panics, and what it returns re-encodes without panicking.
    #[test]
    fn cbor_decoder_survives_arbitrary_bytes(
        junk in prop::collection::vec(any::<u8>(), 0..64),
        v in value_strategy(),
        cut in 0usize..64,
    ) {
        if let Ok(value) = decode_cbor(&junk, DecodeLimits::new(32, usize::MAX)) {
            let _ = canonical_bytes(&desc(vec![("k".into(), value)]));
        }

        let mut buf = Vec::new();
        ciborium::into_writer(&to_cbor(&v), &mut buf).expect("encode");
        let truncated = &buf[..cut.min(buf.len())];
        if let Ok(value) = decode_cbor(truncated, DecodeLimits::new(32, usize::MAX)) {
            let _ = canonical_bytes(&desc(vec![("k".into(), value)]));
        }

        // A depth budget of zero refuses every container instead of recursing.
        let _ = decode_cbor(&buf, DecodeLimits::new(0, usize::MAX));
    }

    /// Scenario: an attribute list of two or more distinct keys, encoded sorted and as a verified
    /// different shuffle normalized by `sort_kvlist`.
    /// Guarantees: both encodings and series ids are byte-identical.
    #[test]
    fn encoding_is_order_independent(
        (sorted, shuffled) in prop::collection::vec((any::<String>(), value_strategy()), 2..7)
            .prop_map(|kvs| {
                let mut sorted = kvs;
                sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
                sorted.dedup_by(|a, b| a.0 == b.0);
                sorted
            })
            // Duplicate keys are removed above, so a short list can fall below the floor.
            .prop_filter("at least two distinct keys", |s| s.len() >= 2)
            .prop_flat_map(|sorted| {
                let shuffled = Just(sorted.clone()).prop_shuffle();
                (Just(sorted), shuffled)
            })
            .prop_filter("the two orders must differ", |(sorted, shuffled)| {
                let a: Vec<&str> = sorted.iter().map(|(k, _)| k.as_str()).collect();
                let b: Vec<&str> = shuffled.iter().map(|(k, _)| k.as_str()).collect();
                a != b
            }),
    ) {
        prop_assert!(sorted.len() >= 2, "the property needs at least two keys to mean anything");
        let sorted_keys: Vec<&str> = sorted.iter().map(|(k, _)| k.as_str()).collect();
        let shuffled_keys: Vec<&str> = shuffled.iter().map(|(k, _)| k.as_str()).collect();
        prop_assert_ne!(
            &sorted_keys,
            &shuffled_keys,
            "the shuffled input must reach the encoder in a different order"
        );

        // Encode the shuffled list as the extractor would: normalize, then encode.
        let mut shuffled = shuffled;
        sort_kvlist(&mut shuffled).expect("unique keys");
        let a = canonical_bytes(&desc(sorted));
        let b = canonical_bytes(&desc(shuffled));
        prop_assert_eq!(&a, &b);
        prop_assert_eq!(series_id(&a), series_id(&b));
    }
}

/// The reference decoder `decode_cbor` must agree with: ciborium's `Value`
/// tree, converted afterwards; `None` is any refusal.
fn reference_decode(bytes: &[u8], max_depth: usize) -> Option<Value> {
    fn convert(raw: ciborium::Value, depth_left: usize) -> Option<Value> {
        Some(match raw {
            ciborium::Value::Null => Value::Null,
            ciborium::Value::Bool(b) => Value::Bool(b),
            ciborium::Value::Integer(i) => Value::Int(i64::try_from(i).ok()?),
            ciborium::Value::Float(f) => Value::Double(f),
            ciborium::Value::Text(s) => Value::Str(s),
            ciborium::Value::Bytes(b) => Value::Bytes(b),
            ciborium::Value::Array(items) => {
                let depth_left = depth_left.checked_sub(1)?;
                Value::Array(
                    items
                        .into_iter()
                        .map(|item| convert(item, depth_left))
                        .collect::<Option<_>>()?,
                )
            }
            ciborium::Value::Map(entries) => {
                let depth_left = depth_left.checked_sub(1)?;
                let mut out = Vec::with_capacity(entries.len());
                for (k, v) in entries {
                    let key = match k {
                        ciborium::Value::Text(s) => s,
                        ciborium::Value::Null => String::new(),
                        _ => return None,
                    };
                    out.push((key, convert(v, depth_left)?));
                }
                sort_kvlist(&mut out).ok()?;
                Value::KvList(out)
            }
            _ => return None,
        })
    }
    let raw: ciborium::Value =
        ciborium::de::from_reader_with_recursion_limit(bytes, max_depth + 1).ok()?;
    convert(raw, max_depth)
}

/// Structural equality with doubles compared by bits.
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Double(x), Value::Double(y)) => x.to_bits() == y.to_bits(),
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same(p, q))
        }
        (Value::KvList(x), Value::KvList(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y)
                    .all(|((kp, p), (kq, q))| kp == kq && same(p, q))
        }
        _ => a == b,
    }
}

/// Arbitrary CBOR trees, including what the decoder must refuse: tags,
/// integers outside `i64` (encoded as bignums), non-text map keys and
/// duplicate keys.
fn raw_cbor_strategy() -> impl Strategy<Value = ciborium::Value> {
    let leaf = prop_oneof![
        Just(ciborium::Value::Null),
        any::<bool>().prop_map(ciborium::Value::Bool),
        any::<i64>().prop_map(|i| ciborium::Value::Integer(i.into())),
        any::<u64>().prop_map(|i| ciborium::Value::Integer(i.into())),
        (-(1_i128 << 64)..(1_i128 << 64)).prop_map(|i| ciborium::Value::Integer(
            ciborium::value::Integer::try_from(i).expect("within the CBOR integer range")
        )),
        any::<u64>().prop_map(|b| ciborium::Value::Float(f64::from_bits(b))),
        ".{0,8}".prop_map(ciborium::Value::Text),
        prop::collection::vec(any::<u8>(), 0..8).prop_map(ciborium::Value::Bytes),
    ];
    leaf.prop_recursive(6, 48, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(ciborium::Value::Array),
            prop::collection::vec(
                (
                    prop_oneof![
                        "[ab]{0,2}".prop_map(ciborium::Value::Text),
                        Just(ciborium::Value::Null),
                        any::<u8>().prop_map(|i| ciborium::Value::Integer(i.into())),
                    ],
                    inner.clone()
                ),
                0..4
            )
            .prop_map(ciborium::Value::Map),
            (0_u64..8, inner).prop_map(|(t, v)| ciborium::Value::Tag(t, Box::new(v))),
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, .. ProptestConfig::default() })]

    /// Scenario: arbitrary CBOR trees near a depth limit of 4, plus arbitrary bytes and
    /// truncations, decoded by `decode_cbor` and the ciborium-tree decoder.
    /// Guarantees: both accept the same payloads and give the same value, doubles bit for bit.
    #[test]
    fn decode_cbor_agrees_with_the_ciborium_tree_decoder(
        raw in raw_cbor_strategy(),
        junk in prop::collection::vec(any::<u8>(), 0..32),
        cut in 0usize..64,
    ) {
        let mut buf = Vec::new();
        ciborium::into_writer(&raw, &mut buf).expect("encode");
        for bytes in [&buf[..], &buf[..cut.min(buf.len())], &junk[..]] {
            let new = decode_cbor(bytes, DecodeLimits::new(4, usize::MAX)).ok();
            let old = reference_decode(bytes, 4);
            match (&new, &old) {
                (Some(a), Some(b)) => prop_assert!(same(a, b), "{a:?} != {b:?}"),
                (None, None) => {}
                _ => prop_assert!(false, "decode {new:?}, reference {old:?} for {bytes:02x?}"),
            }
        }
    }
}

/// The reference `render_v1` `map_string` must agree with: a serde_json
/// tree, printed.
fn reference_render(v: &Value) -> serde_json::Value {
    use base64::Engine as _;
    use serde_json::Value as J;
    match v {
        Value::Null => J::Null,
        Value::Str(s) => J::String(s.clone()),
        Value::Bytes(b) => J::String(base64::engine::general_purpose::STANDARD.encode(b)),
        Value::Int(i) => J::from(*i),
        Value::Double(d) if d.is_nan() => J::String("NaN".into()),
        Value::Double(d) if *d == f64::INFINITY => J::String("Infinity".into()),
        Value::Double(d) if *d == f64::NEG_INFINITY => J::String("-Infinity".into()),
        Value::Double(d) => serde_json::Number::from_f64(*d).map_or(J::Null, J::Number),
        Value::Bool(b) => J::Bool(*b),
        Value::Array(items) => J::Array(items.iter().map(reference_render).collect()),
        Value::KvList(entries) => J::Object(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), reference_render(v)))
                .collect(),
        ),
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, .. ProptestConfig::default() })]

    /// Scenario: arbitrary value trees rendered by `map_string`, the serde_json tree and the
    /// reserving body entry point.
    /// Guarantees: the renderings are identical and `rendered_len` measures them exactly.
    #[test]
    fn map_string_agrees_with_the_serde_json_tree(v in value_strategy()) {
        let expected = match &v {
            Value::Null => None,
            Value::Str(s) => Some(s.clone()),
            other => Some(reference_render(other).to_string()),
        };
        prop_assert_eq!(&map_string(&v), &expected);
        let reserved = body_string_reserving(&v, &mut Unbounded).expect("unbounded");
        prop_assert_eq!(&reserved, &expected);
        prop_assert_eq!(rendered_len(&v), expected.as_ref().map_or(0, String::len));
    }
}

/// A CBOR item as it is laid out on the wire, including what ciborium's
/// writer never emits: indefinite lengths, chunked and nested-chunked text
/// and bytes, bignums, half and single floats, simple values, stray breaks
/// and non-minimal lengths.
#[derive(Debug, Clone)]
enum Raw {
    Uint(u64, u8),
    Nint(u64, u8),
    Big(bool, Vec<u8>),
    BigChunked(bool, Vec<Vec<u8>>),
    Half(u16),
    Single(u32),
    Double(u64),
    Simple(u8),
    Break,
    Bytes(Vec<u8>, u8),
    Text(Vec<u8>, u8),
    /// Chunks of bytes (`false`) or text (`true`); a chunk of the wrong kind
    /// or a nested indefinite run is part of what is generated.
    Chunked(bool, Vec<Chunk>),
    Array(Vec<Raw>, bool),
    Map(Vec<(Raw, Raw)>, bool),
    Tag(u64, Box<Raw>),
}

#[derive(Debug, Clone)]
enum Chunk {
    Piece(Vec<u8>),
    WrongKind(Vec<u8>),
    Nested(Vec<Vec<u8>>),
}

/// A header of `major` carrying `value`, in the minimal width or, when
/// `width` asks for it and the value fits, a wider one.
fn head(out: &mut Vec<u8>, major: u8, value: u64, width: u8) {
    let m = major << 5;
    match width % 5 {
        1 if value <= u64::from(u8::MAX) => out.extend([m | 24, value as u8]),
        2 if value <= u64::from(u16::MAX) => {
            out.push(m | 25);
            out.extend((value as u16).to_be_bytes());
        }
        3 if value <= u64::from(u32::MAX) => {
            out.push(m | 26);
            out.extend((value as u32).to_be_bytes());
        }
        4 => {
            out.push(m | 27);
            out.extend(value.to_be_bytes());
        }
        _ if value < 24 => out.push(m | value as u8),
        _ if value <= u64::from(u8::MAX) => out.extend([m | 24, value as u8]),
        _ if value <= u64::from(u16::MAX) => {
            out.push(m | 25);
            out.extend((value as u16).to_be_bytes());
        }
        _ if value <= u64::from(u32::MAX) => {
            out.push(m | 26);
            out.extend((value as u32).to_be_bytes());
        }
        _ => {
            out.push(m | 27);
            out.extend(value.to_be_bytes());
        }
    }
}

fn encode_raw(item: &Raw, out: &mut Vec<u8>) {
    let len = |n: usize| n as u64;
    match item {
        Raw::Uint(v, w) => head(out, 0, *v, *w),
        Raw::Nint(v, w) => head(out, 1, *v, *w),
        Raw::Big(negative, digits) => {
            head(out, 6, if *negative { 3 } else { 2 }, 0);
            head(out, 2, len(digits.len()), 0);
            out.extend(digits);
        }
        Raw::BigChunked(negative, chunks) => {
            head(out, 6, if *negative { 3 } else { 2 }, 0);
            out.push(0x5f);
            for c in chunks {
                head(out, 2, len(c.len()), 0);
                out.extend(c);
            }
            out.push(0xff);
        }
        Raw::Half(bits) => {
            out.push(0xf9);
            out.extend(bits.to_be_bytes());
        }
        Raw::Single(bits) => {
            out.push(0xfa);
            out.extend(bits.to_be_bytes());
        }
        Raw::Double(bits) => {
            out.push(0xfb);
            out.extend(bits.to_be_bytes());
        }
        Raw::Simple(v) if *v < 24 => out.push(0xe0 | v),
        Raw::Simple(v) => out.extend([0xf8, *v]),
        Raw::Break => out.push(0xff),
        Raw::Bytes(b, w) => {
            head(out, 2, len(b.len()), *w);
            out.extend(b);
        }
        Raw::Text(t, w) => {
            head(out, 3, len(t.len()), *w);
            out.extend(t);
        }
        Raw::Chunked(text, chunks) => {
            let major = if *text { 3 } else { 2 };
            out.push(major << 5 | 31);
            for c in chunks {
                match c {
                    Chunk::Piece(p) => {
                        head(out, major, len(p.len()), 0);
                        out.extend(p);
                    }
                    Chunk::WrongKind(p) => {
                        head(out, 5 - major, len(p.len()), 0);
                        out.extend(p);
                    }
                    Chunk::Nested(pieces) => {
                        out.push(major << 5 | 31);
                        for p in pieces {
                            head(out, major, len(p.len()), 0);
                            out.extend(p);
                        }
                        out.push(0xff);
                    }
                }
            }
            out.push(0xff);
        }
        Raw::Array(items, indefinite) => {
            if *indefinite {
                out.push(0x9f);
            } else {
                head(out, 4, len(items.len()), 0);
            }
            for i in items {
                encode_raw(i, out);
            }
            if *indefinite {
                out.push(0xff);
            }
        }
        Raw::Map(entries, indefinite) => {
            if *indefinite {
                out.push(0xbf);
            } else {
                head(out, 5, len(entries.len()), 0);
            }
            for (k, v) in entries {
                encode_raw(k, out);
                encode_raw(v, out);
            }
            if *indefinite {
                out.push(0xff);
            }
        }
        Raw::Tag(t, inner) => {
            head(out, 6, *t, 0);
            encode_raw(inner, out);
        }
    }
}

/// Big-endian digits around the `i64` and `u64` limits, with and without
/// leading zeros, up to 17 bytes.
fn bignum_digits() -> impl Strategy<Value = Vec<u8>> {
    let boundary = prop_oneof![
        Just(0_u128),
        Just(i64::MAX as u128),
        Just(i64::MAX as u128 + 1),
        Just(u64::MAX as u128),
        Just(u64::MAX as u128 + 1),
        Just(i128::MAX as u128),
        Just(u128::MAX),
        any::<u128>(),
    ];
    (boundary, 0_usize..3, prop::bool::ANY).prop_map(|(v, zeros, trim)| {
        let mut digits: Vec<u8> = v.to_be_bytes().to_vec();
        if trim {
            let first = digits.iter().position(|&b| b != 0).unwrap_or(digits.len());
            let _ = digits.drain(..first);
        }
        let mut out = vec![0_u8; zeros];
        out.extend(digits);
        out
    })
}

/// Text bytes: mostly valid UTF-8 (ASCII, multi-byte, control), sometimes a
/// cut multi-byte character or arbitrary bytes.
fn text_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        6 => "[a-c\u{e9}\u{4e2d}\u{1f600}\u{1}\"]{0,6}".prop_map(String::into_bytes),
        1 => Just(vec![0xe4, 0xb8]),
        1 => prop::collection::vec(any::<u8>(), 0..4),
    ]
}

fn raw_strategy() -> impl Strategy<Value = Raw> {
    let chunk_bytes = || prop::collection::vec(any::<u8>(), 0..4);
    let leaf = prop_oneof![
        (any::<u64>(), any::<u8>()).prop_map(|(v, w)| Raw::Uint(v, w)),
        (
            prop_oneof![
                Just(i64::MAX as u64),
                Just(i64::MAX as u64 + 1),
                any::<u64>()
            ],
            any::<u8>()
        )
            .prop_map(|(v, w)| Raw::Nint(v, w)),
        (any::<bool>(), bignum_digits()).prop_map(|(n, d)| Raw::Big(n, d)),
        (any::<bool>(), prop::collection::vec(chunk_bytes(), 0..3))
            .prop_map(|(n, c)| Raw::BigChunked(n, c)),
        any::<u16>().prop_map(Raw::Half),
        any::<u32>().prop_map(Raw::Single),
        any::<u64>().prop_map(Raw::Double),
        prop_oneof![20_u8..24, any::<u8>()].prop_map(Raw::Simple),
        Just(Raw::Break),
        (chunk_bytes(), any::<u8>()).prop_map(|(b, w)| Raw::Bytes(b, w)),
        (text_bytes(), any::<u8>()).prop_map(|(t, w)| Raw::Text(t, w)),
        (
            any::<bool>(),
            prop::collection::vec(
                prop_oneof![
                    6 => text_bytes().prop_map(Chunk::Piece),
                    1 => chunk_bytes().prop_map(Chunk::WrongKind),
                    2 => prop::collection::vec(text_bytes(), 0..3).prop_map(Chunk::Nested),
                ],
                0..4
            )
        )
            .prop_map(|(text, chunks)| Raw::Chunked(text, chunks)),
    ];
    leaf.prop_recursive(5, 40, 4, |inner| {
        let key = prop_oneof![
            4 => text_bytes().prop_map(|t| Raw::Text(t, 0)),
            1 => Just(Raw::Simple(22)),
            1 => Just(Raw::Simple(23)),
            1 => inner.clone(),
        ];
        prop_oneof![
            (prop::collection::vec(inner.clone(), 0..4), any::<bool>())
                .prop_map(|(items, ind)| Raw::Array(items, ind)),
            (
                prop::collection::vec((key, inner.clone()), 0..4),
                any::<bool>()
            )
                .prop_map(|(entries, ind)| Raw::Map(entries, ind)),
            (prop_oneof![0_u64..8, Just(2), Just(3)], inner)
                .prop_map(|(t, i)| Raw::Tag(t, Box::new(i))),
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2048, .. ProptestConfig::default() })]

    /// Scenario: hand-laid CBOR (indefinite items, chunks, bignums at the limits, floats, simple
    /// values, stray breaks, non-minimal lengths), whole, truncated and mutated, at depth limits 4
    /// and 32.
    /// Guarantees: `decode_cbor` and the ciborium-tree decoder accept the same payloads and give
    /// the same value.
    #[test]
    fn decode_cbor_agrees_with_the_ciborium_tree_decoder_on_raw_encodings(
        raw in raw_strategy(),
        cut in any::<prop::sample::Index>(),
        flip in any::<prop::sample::Index>(),
        byte in any::<u8>(),
    ) {
        let mut buf = Vec::new();
        encode_raw(&raw, &mut buf);
        let truncated = buf[..cut.index(buf.len() + 1)].to_vec();
        let mut flipped = buf.clone();
        if !flipped.is_empty() {
            let at = flip.index(flipped.len());
            flipped[at] = byte;
        }
        for bytes in [&buf, &truncated, &flipped] {
            for depth in [4, 32] {
                let new = decode_cbor(bytes, DecodeLimits::new(depth, usize::MAX)).ok();
                let old = reference_decode(bytes, depth);
                match (&new, &old) {
                    (Some(a), Some(b)) => prop_assert!(same(a, b), "{a:?} != {b:?}"),
                    (None, None) => {}
                    _ => prop_assert!(
                        false,
                        "depth {depth}: decode {new:?}, reference {old:?} for {bytes:02x?}"
                    ),
                }
            }
        }
    }
}
