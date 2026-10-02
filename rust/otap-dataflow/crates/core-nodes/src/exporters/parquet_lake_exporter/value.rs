// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Owned AnyValue tree, CBOR decoding of the OTAP `ser` column and the `render_v1` rendering
//! (docs/FORMAT.md sections 1 and 2). Ported from the `series-lake` crate; the golden vectors in
//! `testdata/golden` pin the behavior.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;

use super::error::LakeError;
use super::limits::Budget;

/// An OTLP AnyValue in owned form.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Unset value.
    Null,
    /// UTF-8 string.
    Str(String),
    /// Raw bytes.
    Bytes(Vec<u8>),
    /// 64-bit integer.
    Int(i64),
    /// 64-bit float.
    Double(f64),
    /// Boolean.
    Bool(bool),
    /// Array of values.
    Array(Vec<Value>),
    /// Key/value list, sorted by raw key bytes, keys unique.
    KvList(Vec<(String, Value)>),
}

fn invalid(msg: &str) -> LakeError {
    LakeError::Invalid(msg.to_owned())
}

/// Decode one CBOR item (the first in `bytes`; trailing bytes are ignored) into a [`Value`].
/// Kvlist keys are sorted by raw bytes. A malformed item, a tag other than a bignum, an integer
/// outside `i64`, a non-text key or a duplicate key is invalid content; more than `max_depth`
/// nested arrays or maps is too deep.
///
/// The decoded tree is charged to `budget` while it is built (one node per value plus string,
/// byte and key content), so a small cell of many tiny items is refused before it expands far
/// beyond its size. Text strings and map keys that are not UTF-8 are repaired per chunk (each
/// invalid sequence becomes U+FFFD) and counted in `budget`.
pub fn decode_cbor(
    bytes: &[u8],
    max_depth: usize,
    budget: &mut Budget,
) -> Result<Value, LakeError> {
    let mut reader = CborReader {
        decoder: ciborium_ll::Decoder::from(bytes),
        len: bytes.len(),
        max_depth,
        budget,
    };
    reader.item(max_depth)
}

/// A streaming CBOR reader that builds a [`Value`] directly.
struct CborReader<'a, 'b> {
    decoder: ciborium_ll::Decoder<&'a [u8]>,
    len: usize,
    max_depth: usize,
    budget: &'b mut Budget,
}

impl CborReader<'_, '_> {
    fn header(&mut self) -> Result<ciborium_ll::Header, LakeError> {
        self.decoder
            .pull()
            .map_err(|e| LakeError::Invalid(format!("cbor decode: {e:?}")))
    }

    /// Input bytes not yet read.
    fn remaining(&mut self) -> usize {
        self.len.saturating_sub(self.decoder.offset())
    }

    /// Refuse `count` items the rest of the input cannot hold at `min_encoded` bytes each, so a
    /// declared length never sizes an allocation beyond the input.
    fn fits_input(&mut self, count: usize, min_encoded: usize) -> Result<(), LakeError> {
        match count.checked_mul(min_encoded) {
            Some(needed) if needed <= self.remaining() => Ok(()),
            _ => Err(invalid("cbor decode: truncated item")),
        }
    }

    /// One item; `depth_left` more container levels may open below this point.
    fn item(&mut self, depth_left: usize) -> Result<Value, LakeError> {
        use ciborium_ll::{Header, simple, tag};
        self.budget.charge(size_of::<Value>())?;
        Ok(match self.header()? {
            Header::Positive(x) => {
                Value::Int(i64::try_from(x).map_err(|_| invalid("cbor int out of i64"))?)
            }
            // The wire value has all bits inverted: -1 - x.
            Header::Negative(x) => {
                Value::Int(i64::try_from(x).map_err(|_| invalid("cbor int out of i64"))? ^ !0)
            }
            Header::Float(f) => Value::Double(f),
            Header::Simple(simple::FALSE) => Value::Bool(false),
            Header::Simple(simple::TRUE) => Value::Bool(true),
            Header::Simple(simple::NULL | simple::UNDEFINED) => Value::Null,
            Header::Simple(_) | Header::Break => {
                return Err(invalid("cbor decode: unexpected simple value"));
            }
            Header::Bytes(len) => Value::Bytes(self.content(len, false)?),
            Header::Text(len) => {
                let bytes = self.content(len, true)?;
                Value::Str(
                    String::from_utf8(bytes)
                        .map_err(|_| invalid("cbor decode: text is not UTF-8"))?,
                )
            }
            Header::Tag(t @ (tag::BIGPOS | tag::BIGNEG)) => self.bignum(t == tag::BIGNEG)?,
            Header::Tag(_) => return Err(invalid("unsupported cbor value")),
            Header::Array(len) => {
                let Some(depth_left) = depth_left.checked_sub(1) else {
                    return Err(LakeError::TooDeep(self.max_depth));
                };
                // No up-front reservation from the declared length: items are charged one by
                // one as they are decoded.
                let mut items = Vec::new();
                match len {
                    Some(n) => {
                        self.fits_input(n, 1)?;
                        for _ in 0..n {
                            items.push(self.item(depth_left)?);
                        }
                    }
                    None => {
                        while !self.at_break()? {
                            items.push(self.item(depth_left)?);
                        }
                    }
                }
                Value::Array(items)
            }
            Header::Map(len) => {
                let Some(depth_left) = depth_left.checked_sub(1) else {
                    return Err(LakeError::TooDeep(self.max_depth));
                };
                let mut entries: Vec<(String, Value)> = Vec::new();
                match len {
                    Some(n) => {
                        self.fits_input(n, 2)?;
                        for _ in 0..n {
                            let key = self.key()?;
                            entries.push((key, self.item(depth_left)?));
                        }
                    }
                    None => {
                        while !self.at_break()? {
                            let key = self.key()?;
                            entries.push((key, self.item(depth_left)?));
                        }
                    }
                }
                sort_kvlist(&mut entries)?;
                Value::KvList(entries)
            }
        })
    }

    /// Whether the next header ends an indefinite container, consuming it if so.
    fn at_break(&mut self) -> Result<bool, LakeError> {
        match self.header()? {
            ciborium_ll::Header::Break => Ok(true),
            other => {
                self.decoder.push(other);
                Ok(false)
            }
        }
    }

    /// A map key: text, or null/undefined for the empty key.
    fn key(&mut self) -> Result<String, LakeError> {
        use ciborium_ll::{Header, simple};
        self.budget.charge(size_of::<String>())?;
        match self.header()? {
            Header::Text(len) => {
                let bytes = self.content(len, true)?;
                String::from_utf8(bytes).map_err(|_| invalid("cbor decode: text is not UTF-8"))
            }
            Header::Simple(simple::NULL | simple::UNDEFINED) => Ok(String::new()),
            _ => Err(invalid("cbor map key is not text")),
        }
    }

    /// The content of a bytes or text item whose header gave `len`. An indefinite item is the
    /// concatenation of its chunks of the same kind; a nested indefinite chunk is flattened. A text
    /// chunk that is not UTF-8 on its own is repaired on its own (each invalid sequence becomes
    /// U+FFFD; RFC 8949 requires every chunk to be valid by itself), and a text item with a
    /// repaired chunk counts once in the budget's repaired strings.
    fn content(&mut self, len: Option<usize>, text: bool) -> Result<Vec<u8>, LakeError> {
        use ciborium_ll::Header;
        let mut out = Vec::new();
        let mut repaired = false;
        if let Some(len) = len {
            repaired = self.chunk(&mut out, len, text)?;
        } else {
            let mut open = 1_usize;
            while open > 0 {
                match (self.header()?, text) {
                    (Header::Break, _) => open -= 1,
                    (Header::Bytes(None), false) | (Header::Text(None), true) => open += 1,
                    (Header::Bytes(Some(n)), false) | (Header::Text(Some(n)), true) => {
                        repaired |= self.chunk(&mut out, n, text)?;
                    }
                    _ => return Err(invalid("cbor decode: malformed chunk")),
                }
            }
        }
        if repaired {
            self.budget.note_repaired();
        }
        Ok(out)
    }

    /// Append one chunk of `n` bytes to `out`. A text chunk that is not UTF-8 is replaced by its
    /// lossy decoding, and the growth is charged; returns whether it was.
    fn chunk(&mut self, out: &mut Vec<u8>, n: usize, text: bool) -> Result<bool, LakeError> {
        use ciborium_io::Read as _;
        self.fits_input(n, 1)?;
        self.budget.charge(n)?;
        let start = out.len();
        out.resize(start + n, 0);
        self.decoder
            .read_exact(&mut out[start..])
            .map_err(|_| invalid("cbor decode: truncated chunk"))?;
        if !text || std::str::from_utf8(&out[start..]).is_ok() {
            return Ok(false);
        }
        let fixed = String::from_utf8_lossy(&out[start..]).into_owned();
        self.budget.charge(fixed.len().saturating_sub(n))?;
        out.truncate(start);
        out.extend_from_slice(fixed.as_bytes());
        Ok(true)
    }

    /// A tag 2 or 3 bignum of at most 16 bytes, as an `i64`.
    fn bignum(&mut self, negative: bool) -> Result<Value, LakeError> {
        use ciborium_io::Read as _;
        let len = match self.header()? {
            ciborium_ll::Header::Bytes(Some(len)) if len <= 16 => len,
            _ => return Err(invalid("unsupported cbor value")),
        };
        let mut digits = [0_u8; 16];
        self.decoder
            .read_exact(&mut digits[..len])
            .map_err(|_| invalid("cbor decode: truncated bignum"))?;
        let raw = digits[..len]
            .iter()
            .fold(0_u128, |acc, &b| (acc << 8) | u128::from(b));
        let raw = i64::try_from(raw).map_err(|_| invalid("cbor int out of i64"))?;
        Ok(Value::Int(if negative { raw ^ !0 } else { raw }))
    }
}

/// Sort a key/value list by raw key bytes and refuse duplicate keys.
pub fn sort_kvlist(list: &mut [(String, Value)]) -> Result<(), LakeError> {
    list.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    if list.windows(2).any(|w| w[0].0 == w[1].0) {
        return Err(invalid("duplicate attribute key"));
    }
    Ok(())
}

/// The `render_v1` rendering of FORMAT.md section 2 as compact JSON.
#[must_use]
pub fn render_v1(v: &Value) -> String {
    let mut out = String::new();
    let _ = write_v1(v, &mut out);
    out
}

fn write_v1<W: std::fmt::Write>(v: &Value, out: &mut W) -> std::fmt::Result {
    match v {
        Value::Null => out.write_str("null"),
        Value::Str(s) => write_json_str(s, out),
        Value::Bytes(b) => write_bytes_v1(b, out),
        Value::Int(i) => write!(out, "{i}"),
        Value::Double(d) => write_double(*d, out),
        Value::Bool(b) => out.write_str(if *b { "true" } else { "false" }),
        Value::Array(items) => {
            out.write_char('[')?;
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.write_char(',')?;
                }
                write_v1(item, out)?;
            }
            out.write_char(']')
        }
        Value::KvList(entries) => {
            out.write_char('{')?;
            for (i, (k, v)) in entries.iter().enumerate() {
                if i > 0 {
                    out.write_char(',')?;
                }
                write_json_str(k, out)?;
                out.write_char(':')?;
                write_v1(v, out)?;
            }
            out.write_char('}')
        }
    }
}

/// Padded standard base64 in quotes.
pub fn write_bytes_v1<W: std::fmt::Write>(b: &[u8], out: &mut W) -> std::fmt::Result {
    out.write_char('"')?;
    // Whole groups of three bytes encode independently, so the chunks concatenate to the encoding
    // of the whole input.
    let mut buf = [0_u8; 1024];
    for chunk in b.chunks(768) {
        let n = BASE64_STANDARD
            .encode_slice(chunk, &mut buf)
            .map_err(|_| std::fmt::Error)?;
        out.write_str(std::str::from_utf8(&buf[..n]).map_err(|_| std::fmt::Error)?)?;
    }
    out.write_char('"')
}

/// A double as `render_v1` spells it: `"NaN"`, `"Infinity"`, `"-Infinity"` (quoted), otherwise
/// serde_json's shortest round-trip form.
pub fn write_double<W: std::fmt::Write>(d: f64, out: &mut W) -> std::fmt::Result {
    if d.is_nan() {
        out.write_str("\"NaN\"")
    } else if d == f64::INFINITY {
        out.write_str("\"Infinity\"")
    } else if d == f64::NEG_INFINITY {
        out.write_str("\"-Infinity\"")
    } else {
        match serde_json::Number::from_f64(d) {
            Some(n) => write!(out, "{n}"),
            None => out.write_str("null"),
        }
    }
}

/// The JSON escape of one byte of a string, as serde_json writes it.
fn json_escape(b: u8) -> Option<&'static str> {
    const CONTROL: [&str; 32] = [
        "\\u0000", "\\u0001", "\\u0002", "\\u0003", "\\u0004", "\\u0005", "\\u0006", "\\u0007",
        "\\b", "\\t", "\\n", "\\u000b", "\\f", "\\r", "\\u000e", "\\u000f", "\\u0010", "\\u0011",
        "\\u0012", "\\u0013", "\\u0014", "\\u0015", "\\u0016", "\\u0017", "\\u0018", "\\u0019",
        "\\u001a", "\\u001b", "\\u001c", "\\u001d", "\\u001e", "\\u001f",
    ];
    match b {
        b'"' => Some("\\\""),
        b'\\' => Some("\\\\"),
        0..0x20 => Some(CONTROL[usize::from(b)]),
        _ => None,
    }
}

fn write_json_str<W: std::fmt::Write>(s: &str, out: &mut W) -> std::fmt::Result {
    out.write_char('"')?;
    let mut plain = 0;
    for (i, b) in s.bytes().enumerate() {
        if let Some(escape) = json_escape(b) {
            out.write_str(&s[plain..i])?;
            out.write_str(escape)?;
            plain = i + 1;
        }
    }
    out.write_str(&s[plain..])?;
    out.write_char('"')
}

/// Attribute-map and log-body entry point: the raw string for a string, `None` for unset, the
/// compact JSON of `render_v1` otherwise. The column renderer (`anyvalue.rs`) writes the same
/// strings cell by cell; this form over an owned value is the test oracle.
#[cfg(test)]
#[must_use]
pub fn map_string(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Str(s) => Some(s.clone()),
        other => Some(render_v1(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::parquet_lake_exporter::test_fixtures::{golden, value_from_json};

    /// Scenario: every `render_v1` vector of the reference implementation: each scalar kind, doubles around the layout limits, NaN payloads, infinities, every base64 padding length, nested values.
    /// Guarantees: map_string gives exactly the stored strings of Series Lake Format v1 (raw strings, SQL null for unset, compact JSON otherwise).
    #[test]
    fn render_v1_matches_the_golden_vectors() {
        let doc = golden("render_v1");
        let vectors = doc["vectors"].as_array().expect("vectors");
        assert_eq!(vectors.len(), 30);
        for v in vectors {
            let name = v["name"].as_str().expect("name");
            let value = value_from_json(&v["value"]);
            assert_eq!(
                map_string(&value).as_deref(),
                v["map_value"].as_str(),
                "{name}"
            );
        }
    }

    /// Scenario: A declared array length of 2^32 items arrives in a 5-byte cell.
    /// Guarantees: The decoder refuses it as invalid without allocating for the declared length.
    #[test]
    fn declared_length_beyond_input_is_refused() {
        let err = decode_cbor(
            &[0x9a, 0xff, 0xff, 0xff, 0xff],
            4,
            &mut Budget::new(1 << 30),
        )
        .expect_err("refused");
        assert!(matches!(err, LakeError::Invalid(_)), "{err}");
    }

    /// Scenario: A 3 KiB cell holds an array of about 3000 one-byte integers and is decoded with a 16 KiB budget, then with a 1 MiB budget.
    /// Guarantees: The decoded tree is charged node by node while it is built, so the small cell that would expand to about 100 KiB of values is refused by `ingress.max_extracted_bytes` before it is fully built; with room it decodes to every item.
    #[test]
    fn decoding_charges_each_node_to_the_budget() {
        let n = 3_000;
        let mut cell = vec![0x9a];
        cell.extend((n as u32).to_be_bytes());
        cell.extend(std::iter::repeat_n(0x01, n));
        let err = decode_cbor(&cell, 4, &mut Budget::new(16 * 1024)).expect_err("refused");
        assert!(
            err.to_string().contains("ingress.max_extracted_bytes"),
            "{err}"
        );
        let mut budget = Budget::new(1 << 20);
        let value = decode_cbor(&cell, 4, &mut budget).expect("decodes");
        assert!(matches!(value, Value::Array(items) if items.len() == n));
        assert!(budget.used() >= n * size_of::<Value>(), "{}", budget.used());
    }

    /// Scenario: CBOR text that is not UTF-8: a definite string `c3 28`, an indefinite string whose two chunks split one code point (`c3` | `a9`), and a map key `ff`.
    /// Guarantees: Each decodes with every invalid sequence replaced by U+FFFD (per chunk for the indefinite string), and each text item counts one repaired string.
    #[test]
    fn invalid_utf8_text_is_repaired() {
        let mut budget = Budget::new(1 << 20);
        let v = decode_cbor(&[0x62, 0xc3, 0x28], 4, &mut budget).expect("repaired");
        assert!(matches!(&v, Value::Str(s) if s == "\u{FFFD}("), "{v:?}");
        assert_eq!(budget.repaired(), 1);
        let v =
            decode_cbor(&[0x7f, 0x61, 0xc3, 0x61, 0xa9, 0xff], 4, &mut budget).expect("repaired");
        assert!(
            matches!(&v, Value::Str(s) if s == "\u{FFFD}\u{FFFD}"),
            "{v:?}"
        );
        assert_eq!(budget.repaired(), 2);
        let v = decode_cbor(&[0xa1, 0x61, 0xff, 0x01], 4, &mut budget).expect("repaired");
        assert!(
            matches!(&v, Value::KvList(kvs) if kvs.len() == 1 && kvs[0].0 == "\u{FFFD}"),
            "{v:?}"
        );
        assert_eq!(budget.repaired(), 3);
    }

    /// Scenario: a CBOR map whose keys `ff` and `fe` differ only in invalid bytes.
    /// Guarantees: Both keys repair to U+FFFD and so are duplicates: the item is invalid content, as for any duplicate key.
    #[test]
    fn keys_equal_after_repair_are_duplicates() {
        let err = decode_cbor(
            &[0xa2, 0x61, 0xff, 0x01, 0x61, 0xfe, 0x02],
            4,
            &mut Budget::new(1 << 20),
        )
        .expect_err("duplicate key");
        assert!(matches!(err, LakeError::Invalid(_)), "{err}");
    }
}
