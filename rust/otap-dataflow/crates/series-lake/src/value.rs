// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Owned attribute value tree, CBOR decoding of the OTAP `ser` column and the
//! `render_v1` storage rendering (FORMAT.md section 2).

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;

use crate::error::{Error, RefuseReason, Result};

/// Limits applied while decoding one CBOR `ser` cell (FORMAT.md section 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeLimits {
    /// Maximum nesting depth of the decoded value.
    pub max_depth: usize,
    /// Maximum byte length of one string, bytes or encoded `ser` cell.
    pub max_cell_bytes: usize,
}

impl DecodeLimits {
    /// Limits from a depth and a cell byte bound.
    #[must_use]
    pub fn new(max_depth: usize, max_cell_bytes: usize) -> Self {
        Self {
            max_depth,
            max_cell_bytes,
        }
    }
}

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

/// Decode a CBOR blob from the OTAP `ser` column into a [`Value`].
///
/// Kvlist keys are sorted by raw bytes; duplicate keys are refused as invalid
/// content and nesting deeper than `limits.max_depth` as too deep. An encoded
/// cell longer than `limits.max_cell_bytes` is refused without being decoded.
/// Trailing bytes after the first item are ignored.
///
/// # Errors
/// Refuses an oversized cell as `RequestTooLarge`, nesting deeper than
/// `limits.max_depth` as `TooDeep`, and a malformed payload, a tag other than
/// a bignum, an integer outside `i64` or a duplicate key as invalid content.
pub fn decode_cbor(bytes: &[u8], limits: DecodeLimits) -> Result<Value> {
    decode_cbor_reserving(bytes, limits, &mut Unbounded)
}

/// Memory a decode may use: reserved before it is allocated, released after
/// it is freed.
pub trait Reservations {
    /// Reserve `bytes`, or refuse them.
    ///
    /// # Errors
    /// Whatever refusal the implementation chooses; the decode stops with it.
    fn reserve(&mut self, bytes: usize) -> Result<()>;

    /// Give back `bytes` reserved earlier.
    fn release(&mut self, bytes: usize);
}

/// Reservations without a limit.
#[derive(Debug, Default, Clone, Copy)]
pub struct Unbounded;

impl Reservations for Unbounded {
    fn reserve(&mut self, _bytes: usize) -> Result<()> {
        Ok(())
    }

    fn release(&mut self, _bytes: usize) {}
}

/// [`decode_cbor`], holding every allocation it makes within `reservations`.
///
/// Each buffer's capacity is reserved before it is allocated, a growing or
/// shrinking buffer's old and new capacity together while both exist, and a
/// successful decode holds exactly the result's [`value_bytes`] less one
/// [`VALUE_NODE_BYTES`], the root node, which lives in its caller's storage.
/// A refused reservation stops the decode; the caller rolls back what the
/// failed decode held.
///
/// # Errors
/// As [`decode_cbor`], and the refusal of a reservation as it is.
pub fn decode_cbor_reserving(
    bytes: &[u8],
    limits: DecodeLimits,
    reservations: &mut dyn Reservations,
) -> Result<Value> {
    if bytes.len() > limits.max_cell_bytes {
        return Err(Error::too_large(
            crate::error::SizeBudget::Cell,
            bytes.len(),
            limits.max_cell_bytes,
        ));
    }
    #[cfg(test)]
    DECODES.with(|n| n.set(n.get() + 1));
    let mut reader = CborReader {
        decoder: ciborium_ll::Decoder::from(bytes),
        len: bytes.len(),
        max_depth: limits.max_depth,
        reservations,
    };
    reader.item(limits.max_depth)
}

/// A streaming CBOR reader that builds a [`Value`] directly.
struct CborReader<'a, 'r> {
    decoder: ciborium_ll::Decoder<&'a [u8]>,
    len: usize,
    max_depth: usize,
    reservations: &'r mut dyn Reservations,
}

fn malformed<E: std::fmt::Debug>(e: ciborium_ll::Error<E>) -> Error {
    Error::invalid(format!("cbor decode: {e:?}"))
}

fn overflow() -> Error {
    Error::invalid("cbor decode: size overflow")
}

impl CborReader<'_, '_> {
    fn header(&mut self) -> Result<ciborium_ll::Header> {
        self.decoder.pull().map_err(malformed)
    }

    /// Input bytes not yet read.
    fn remaining(&mut self) -> usize {
        self.len.saturating_sub(self.decoder.offset())
    }

    /// Refuse `count` items the rest of the input cannot hold at
    /// `min_encoded` bytes each.
    fn fits_input(&mut self, count: usize, min_encoded: usize) -> Result<()> {
        let needed = count.checked_mul(min_encoded).ok_or_else(overflow)?;
        if needed > self.remaining() {
            return Err(Error::invalid("cbor decode: truncated item"));
        }
        Ok(())
    }

    /// Make room in `v` for `additional` more elements. A declared length
    /// grows to exactly what it needs, data of unknown length doubles.
    fn make_room<T>(&mut self, v: &mut Vec<T>, additional: usize, exact: bool) -> Result<()> {
        let needed = v.len().checked_add(additional).ok_or_else(overflow)?;
        let old = v.capacity();
        if needed <= old {
            return Ok(());
        }
        let target = if exact {
            needed
        } else {
            old.checked_mul(2)
                .map_or(needed, |doubled| doubled.max(needed).max(8))
        };
        // The old buffer lives until the new one holds its elements.
        let new_bytes = target.checked_mul(size_of::<T>()).ok_or_else(overflow)?;
        self.reservations.reserve(new_bytes)?;
        v.try_reserve_exact(target - v.len())
            .map_err(|_| Error::internal("cbor decode: allocation failed"))?;
        self.reservations.release(old * size_of::<T>());
        // `capacity()` is charged as soon as it is known; an allocator surplus
        // beyond it is not visible to Rust.
        self.reservations
            .reserve((v.capacity() - target) * size_of::<T>())?;
        Ok(())
    }

    /// Shrink `v` to its length, giving back the spare capacity.
    fn trim<T>(&mut self, v: &mut Vec<T>) -> Result<()> {
        let old = v.capacity();
        if old == v.len() {
            return Ok(());
        }
        self.reservations.reserve(v.len() * size_of::<T>())?;
        v.shrink_to_fit();
        self.reservations.release(old * size_of::<T>());
        // As in `make_room`: what `capacity()` reports beyond the length.
        self.reservations
            .reserve((v.capacity() - v.len()) * size_of::<T>())?;
        Ok(())
    }

    /// One item whose own node the caller holds; `depth_left` more container
    /// levels may open below this point.
    fn item(&mut self, depth_left: usize) -> Result<Value> {
        use ciborium_ll::{Header, simple, tag};
        Ok(match self.header()? {
            Header::Positive(x) => {
                Value::Int(i64::try_from(x).map_err(|_| Error::invalid("cbor int out of i64"))?)
            }
            // The wire value has all bits inverted: -1 - x.
            Header::Negative(x) => Value::Int(
                i64::try_from(x).map_err(|_| Error::invalid("cbor int out of i64"))? ^ !0,
            ),
            Header::Float(f) => Value::Double(f),
            Header::Simple(simple::FALSE) => Value::Bool(false),
            Header::Simple(simple::TRUE) => Value::Bool(true),
            Header::Simple(simple::NULL | simple::UNDEFINED) => Value::Null,
            Header::Simple(_) | Header::Break => {
                return Err(Error::invalid("cbor decode: unexpected simple value"));
            }
            Header::Bytes(len) => Value::Bytes(self.content(len, false)?),
            Header::Text(len) => {
                let bytes = self.content(len, true)?;
                Value::Str(
                    String::from_utf8(bytes)
                        .map_err(|_| Error::invalid("cbor decode: text is not UTF-8"))?,
                )
            }
            Header::Tag(t @ (tag::BIGPOS | tag::BIGNEG)) => self.bignum(t == tag::BIGNEG)?,
            Header::Tag(_) => return Err(Error::invalid("unsupported cbor value")),
            Header::Array(len) => {
                let Some(depth_left) = depth_left.checked_sub(1) else {
                    return Err(Error::Refused(RefuseReason::TooDeep(self.max_depth)));
                };
                let mut items = Vec::new();
                match len {
                    Some(n) => {
                        self.fits_input(n, 1)?;
                        self.make_room(&mut items, n, true)?;
                        for _ in 0..n {
                            items.push(self.item(depth_left)?);
                        }
                    }
                    None => {
                        while !self.at_break()? {
                            self.make_room(&mut items, 1, false)?;
                            items.push(self.item(depth_left)?);
                        }
                        self.trim(&mut items)?;
                    }
                }
                Value::Array(items)
            }
            Header::Map(len) => {
                let Some(depth_left) = depth_left.checked_sub(1) else {
                    return Err(Error::Refused(RefuseReason::TooDeep(self.max_depth)));
                };
                let mut entries: Vec<(String, Value)> = Vec::new();
                match len {
                    Some(n) => {
                        self.fits_input(n, 2)?;
                        self.make_room(&mut entries, n, true)?;
                        for _ in 0..n {
                            let key = self.key()?;
                            entries.push((key, self.item(depth_left)?));
                        }
                    }
                    None => {
                        while !self.at_break()? {
                            self.make_room(&mut entries, 1, false)?;
                            let key = self.key()?;
                            entries.push((key, self.item(depth_left)?));
                        }
                        self.trim(&mut entries)?;
                    }
                }
                sort_kvlist(&mut entries)?;
                Value::KvList(entries)
            }
        })
    }

    /// Whether the next header ends an indefinite container, consuming it if so.
    fn at_break(&mut self) -> Result<bool> {
        match self.header()? {
            ciborium_ll::Header::Break => Ok(true),
            other => {
                self.decoder.push(other);
                Ok(false)
            }
        }
    }

    /// A map key: text, or null for the empty key.
    fn key(&mut self) -> Result<String> {
        use ciborium_ll::{Header, simple};
        match self.header()? {
            Header::Text(len) => {
                let bytes = self.content(len, true)?;
                String::from_utf8(bytes)
                    .map_err(|_| Error::invalid("cbor decode: text is not UTF-8"))
            }
            Header::Simple(simple::NULL | simple::UNDEFINED) => Ok(String::new()),
            _ => Err(Error::invalid("cbor map key is not text")),
        }
    }

    /// The content of a bytes or text item whose header gave `len`.
    ///
    /// An indefinite item is a sequence of chunks of its own kind up to a
    /// break; like ciborium, a nested indefinite chunk is flattened into it.
    /// Each text chunk must be UTF-8 on its own.
    fn content(&mut self, len: Option<usize>, text: bool) -> Result<Vec<u8>> {
        use ciborium_ll::Header;
        let mut out = Vec::new();
        let Some(len) = len else {
            let mut open = 1_usize;
            while open > 0 {
                match (self.header()?, text) {
                    (Header::Break, _) => open -= 1,
                    (Header::Bytes(None), false) | (Header::Text(None), true) => open += 1,
                    (Header::Bytes(Some(n)), false) | (Header::Text(Some(n)), true) => {
                        self.chunk(&mut out, n, text, false)?;
                    }
                    _ => return Err(Error::invalid("cbor decode: malformed chunk")),
                }
            }
            self.trim(&mut out)?;
            return Ok(out);
        };
        self.chunk(&mut out, len, text, true)?;
        Ok(out)
    }

    /// Append one chunk of `n` bytes to `out`.
    fn chunk(&mut self, out: &mut Vec<u8>, n: usize, text: bool, exact: bool) -> Result<()> {
        use ciborium_io::Read as _;
        self.fits_input(n, 1)?;
        self.make_room(out, n, exact)?;
        let start = out.len();
        out.resize(start + n, 0);
        self.decoder
            .read_exact(&mut out[start..])
            .map_err(|_| Error::invalid("cbor decode: truncated chunk"))?;
        if text && std::str::from_utf8(&out[start..]).is_err() {
            return Err(Error::invalid("cbor decode: text is not UTF-8"));
        }
        Ok(())
    }

    /// A tag 2 or 3 bignum of at most 16 bytes, as an `i64`; any other tagged
    /// item is unsupported.
    fn bignum(&mut self, negative: bool) -> Result<Value> {
        use ciborium_io::Read as _;
        let len = match self.header()? {
            ciborium_ll::Header::Bytes(Some(len)) if len <= 16 => len,
            _ => return Err(Error::invalid("unsupported cbor value")),
        };
        let mut digits = [0_u8; 16];
        self.decoder
            .read_exact(&mut digits[..len])
            .map_err(|_| Error::invalid("cbor decode: truncated bignum"))?;
        let raw = digits[..len]
            .iter()
            .fold(0_u128, |acc, &b| (acc << 8) | u128::from(b));
        let raw = i64::try_from(raw).map_err(|_| Error::invalid("cbor int out of i64"))?;
        Ok(Value::Int(if negative { raw ^ !0 } else { raw }))
    }
}

/// Sort a key/value list by raw key bytes and refuse duplicate keys.
///
/// The sort is in place: with unique keys an unstable sort orders exactly as a
/// stable one, and a list with a repeated key is refused either way.
pub fn sort_kvlist(list: &mut [(String, Value)]) -> Result<()> {
    list.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    if list.windows(2).any(|w| w[0].0 == w[1].0) {
        return Err(Error::invalid("duplicate attribute key"));
    }
    Ok(())
}

/// The `render_v1` recursive rendering of FORMAT.md section 2, as compact
/// JSON written straight into a string.
///
/// Non-finite doubles and bytes use the spellings of the workspace's OTLP JSON
/// encoder (`otel_arrow_dfe_pdata::otlp::json`): `"NaN"`, `"Infinity"` and
/// `"-Infinity"`, and padded standard base64 for bytes, so a reader that
/// knows OTLP JSON reads them the same way here. The rendering as a whole is
/// not OTLP JSON: a value is not wrapped in an `AnyValue` object, integers are
/// JSON numbers, and kvlists are JSON objects with sorted keys. Strings are
/// escaped and finite doubles formatted exactly as serde_json does.
#[must_use]
pub fn render_v1(v: &Value) -> String {
    let mut out = String::new();
    let _ = write_v1(v, &mut out);
    out
}

/// Write the `render_v1` rendering of `v` to `out`; fails only if `out` does.
pub(crate) fn write_v1<W: std::fmt::Write>(v: &Value, out: &mut W) -> std::fmt::Result {
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

/// Write the `render_v1` rendering of a bytes value, padded standard base64
/// in quotes, to `out`; fails only if `out` does.
pub(crate) fn write_bytes_v1<W: std::fmt::Write>(b: &[u8], out: &mut W) -> std::fmt::Result {
    out.write_char('"')?;
    // Whole groups of three bytes encode independently, so the chunks
    // concatenate to the encoding of the whole input.
    let mut buf = [0_u8; 1024];
    for chunk in b.chunks(768) {
        let n = BASE64_STANDARD
            .encode_slice(chunk, &mut buf)
            .map_err(|_| std::fmt::Error)?;
        out.write_str(std::str::from_utf8(&buf[..n]).map_err(|_| std::fmt::Error)?)?;
    }
    out.write_char('"')
}

/// The length of [`write_bytes_v1`]'s rendering of `len` bytes: two quotes
/// and four base64 characters per started group of three bytes.
#[must_use]
pub(crate) fn rendered_bytes_len(len: usize) -> usize {
    2 + 4 * len.div_ceil(3)
}

fn write_double<W: std::fmt::Write>(d: f64, out: &mut W) -> std::fmt::Result {
    if d.is_nan() {
        out.write_str("\"NaN\"")
    } else if d == f64::INFINITY {
        out.write_str("\"Infinity\"")
    } else if d == f64::NEG_INFINITY {
        out.write_str("\"-Infinity\"")
    } else {
        // serde_json's shortest round-trip form of a finite f64.
        match serde_json::Number::from_f64(d) {
            Some(n) => write!(out, "{n}"),
            None => out.write_str("null"),
        }
    }
}

/// The JSON escape of one byte of a string, as serde_json writes it, or
/// `None` when the byte stands for itself.
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

/// A writer that only counts what it is given.
struct Measure(usize);

impl std::fmt::Write for Measure {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.0 += s.len();
        Ok(())
    }
}

/// The length of [`map_string`]'s result, `0` for [`Value::Null`], measured
/// without allocating.
#[must_use]
pub fn rendered_len(v: &Value) -> usize {
    match v {
        Value::Null => 0,
        Value::Str(s) => s.len(),
        other => {
            let mut measure = Measure(0);
            let _ = write_v1(other, &mut measure);
            measure.0
        }
    }
}

/// Attribute-map entry point: raw string for strings, `None` for unset,
/// compact JSON of `render_v1` otherwise.
#[must_use]
pub fn map_string(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Str(s) => Some(s.clone()),
        other => Some(render_v1(other)),
    }
}

/// The longest prefix of [`map_string`] of `v` that is at most `max` bytes
/// and ends on a character boundary. Rendering stops once the prefix is
/// full, so a large value costs no more than `max` bytes.
#[must_use]
pub fn map_string_prefix(v: &Value, max: usize) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Str(s) => Some(char_prefix(s, max).to_owned()),
        other => {
            let mut prefix = Prefix {
                out: String::new(),
                max,
            };
            // The error only reports that the prefix is full.
            let _ = write_v1(other, &mut prefix);
            Some(prefix.out)
        }
    }
}

/// The longest prefix of `text` that is at most `max` bytes and ends on a
/// character boundary.
#[must_use]
pub fn char_prefix(text: &str, max: usize) -> &str {
    let mut end = text.len().min(max);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// A writer that keeps the first `max` bytes written to it, cut on a
/// character boundary, and fails the write that fills it.
struct Prefix {
    out: String,
    max: usize,
}

impl std::fmt::Write for Prefix {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let kept = char_prefix(s, self.max - self.out.len());
        self.out.push_str(kept);
        if kept.len() == s.len() {
            Ok(())
        } else {
            Err(std::fmt::Error)
        }
    }
}

/// Log-body entry point: identical rules to [`map_string`].
#[must_use]
pub fn body_string(v: &Value) -> Option<String> {
    map_string(v)
}

/// [`map_string`] within `reservations`: the rendered length is measured and
/// reserved before the string is allocated, and stays reserved.
///
/// The string's `capacity()` is what is charged; an allocator surplus beyond
/// it is not visible to Rust.
///
/// # Errors
/// The refusal of the reservation, or an allocation failure.
pub fn map_string_reserving(
    v: &Value,
    reservations: &mut dyn Reservations,
) -> Result<Option<String>> {
    if let Value::Null = v {
        return Ok(None);
    }
    let len = rendered_len(v);
    reservations.reserve(len)?;
    let mut out = String::new();
    out.try_reserve_exact(len)
        .map_err(|_| Error::internal("render: allocation failed"))?;
    reservations.reserve(out.capacity() - len)?;
    match v {
        Value::Str(s) => out.push_str(s),
        other => {
            let _ = write_v1(other, &mut out);
        }
    }
    Ok(Some(out))
}

/// Log-body entry point of [`map_string_reserving`]: identical rules.
///
/// # Errors
/// As [`map_string_reserving`].
pub fn body_string_reserving(
    v: &Value,
    reservations: &mut dyn Reservations,
) -> Result<Option<String>> {
    map_string_reserving(v, reservations)
}

/// Inline bytes of one decoded value node.
pub const VALUE_NODE_BYTES: usize = size_of::<Value>();

/// Inline bytes of one owned string or byte buffer.
pub const BUFFER_HEADER_BYTES: usize = size_of::<String>();

/// Decoded footprint of a value: [`VALUE_NODE_BYTES`] per node plus string,
/// byte and key content.
#[must_use]
pub fn value_bytes(v: &Value) -> usize {
    VALUE_NODE_BYTES
        + match v {
            Value::Null | Value::Int(_) | Value::Double(_) | Value::Bool(_) => 0,
            Value::Str(s) => s.len(),
            Value::Bytes(b) => b.len(),
            Value::Array(items) => items.iter().map(value_bytes).sum(),
            Value::KvList(entries) => kv_bytes(entries),
        }
}

/// Decoded footprint of one key/value entry.
#[must_use]
pub fn entry_bytes(key: &str, value: &Value) -> usize {
    BUFFER_HEADER_BYTES + key.len() + value_bytes(value)
}

/// Decoded footprint of a key/value list.
#[must_use]
pub fn kv_bytes(list: &[(String, Value)]) -> usize {
    list.iter().map(|(k, v)| entry_bytes(k, v)).sum()
}

#[cfg(test)]
thread_local! {
    /// CBOR cells this thread has decoded.
    static DECODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// CBOR cells this thread has decoded so far.
#[cfg(test)]
pub(crate) fn decodes() -> usize {
    DECODES.with(std::cell::Cell::get)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RefuseReason;

    /// Scenario: a CBOR map with unsorted keys and nested array is decoded.
    /// Guarantees: keys come out sorted by raw bytes and nesting is preserved.
    #[test]
    fn decode_cbor_sorts_keys() {
        let mut buf = Vec::new();
        let v = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("b".into()),
                ciborium::Value::Integer(2.into()),
            ),
            (
                ciborium::Value::Text("a".into()),
                ciborium::Value::Array(vec![ciborium::Value::Bool(true), ciborium::Value::Null]),
            ),
        ]);
        ciborium::into_writer(&v, &mut buf).expect("encode test cbor");
        let got = decode_cbor(&buf, DecodeLimits::new(32, usize::MAX)).expect("decode");
        assert_eq!(
            got,
            Value::KvList(vec![
                (
                    "a".into(),
                    Value::Array(vec![Value::Bool(true), Value::Null])
                ),
                ("b".into(), Value::Int(2)),
            ])
        );
    }

    /// Scenario: a valid CBOR `ser` cell longer than `max_cell_bytes`.
    /// Guarantees: it is refused as too large before it is decoded.
    #[test]
    fn decode_cbor_refuses_an_oversized_cell_before_decoding() {
        use crate::error::{Error, RefuseReason};
        let mut buf = Vec::new();
        let v = ciborium::Value::Array(vec![ciborium::Value::Bytes(vec![0u8; 4096])]);
        ciborium::into_writer(&v, &mut buf).expect("encode test cbor");
        assert!(buf.len() > 4096);
        assert!(matches!(
            decode_cbor(&buf, DecodeLimits::new(32, 1024)),
            Err(Error::Refused(RefuseReason::RequestTooLarge(_)))
        ));
        // The same payload decodes once the cell fits the limit.
        assert!(decode_cbor(&buf, DecodeLimits::new(32, buf.len())).is_ok());
    }

    /// Scenario: a CBOR map repeats a key.
    /// Guarantees: decoding refuses the input as invalid.
    #[test]
    fn decode_cbor_rejects_duplicate_keys() {
        use crate::error::{Error, RefuseReason};
        let mut buf = Vec::new();
        let v = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("k".into()),
                ciborium::Value::Integer(1.into()),
            ),
            (
                ciborium::Value::Text("k".into()),
                ciborium::Value::Integer(2.into()),
            ),
        ]);
        ciborium::into_writer(&v, &mut buf).expect("encode test cbor");
        assert!(matches!(
            decode_cbor(&buf, DecodeLimits::new(32, usize::MAX)),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
    }

    /// Scenario: arrays nested deeper than the limit.
    /// Guarantees: decoding refuses instead of recursing without bound.
    #[test]
    fn decode_cbor_rejects_deep_nesting() {
        let mut v = ciborium::Value::Null;
        for _ in 0..5 {
            v = ciborium::Value::Array(vec![v]);
        }
        let mut buf = Vec::new();
        ciborium::into_writer(&v, &mut buf).expect("encode test cbor");
        assert!(decode_cbor(&buf, DecodeLimits::new(3, usize::MAX)).is_err());
        assert!(decode_cbor(&buf, DecodeLimits::new(5, usize::MAX)).is_ok());
    }

    /// Scenario: maps nested one below, at and one above the depth limit, and an array inside maps
    /// at the limit.
    /// Guarantees: every container level counts alike and `max_nesting_depth` is the deepest
    /// accepted.
    #[test]
    fn decode_cbor_depth_is_exact_for_nested_maps() {
        fn maps(levels: usize, leaf: ciborium::Value) -> Vec<u8> {
            let mut v = leaf;
            for _ in 0..levels {
                v = ciborium::Value::Map(vec![(ciborium::Value::Text("k".into()), v)]);
            }
            let mut buf = Vec::new();
            ciborium::into_writer(&v, &mut buf).expect("encode test cbor");
            buf
        }
        let limit = 8;
        let limits = DecodeLimits::new(limit, usize::MAX);
        let int = || ciborium::Value::Integer(1.into());
        assert!(decode_cbor(&maps(limit - 1, int()), limits).is_ok());
        assert!(decode_cbor(&maps(limit, int()), limits).is_ok());
        assert!(matches!(
            decode_cbor(&maps(limit + 1, int()), limits),
            Err(Error::Refused(RefuseReason::TooDeep(8)))
        ));
        let array = ciborium::Value::Array(vec![int()]);
        assert!(decode_cbor(&maps(limit - 1, array.clone()), limits).is_ok());
        assert!(decode_cbor(&maps(limit, array), limits).is_err());
    }

    /// Scenario: maps nested one past the limit and far past it, where ciborium's own recursion
    /// limit stops the parse.
    /// Guarantees: both are `TooDeep` with the configured limit, never invalid content.
    #[test]
    fn decode_cbor_reports_excess_depth_as_too_deep() {
        let deep = |levels: usize| {
            let mut v = ciborium::Value::Null;
            for _ in 0..levels {
                v = ciborium::Value::Array(vec![v]);
            }
            let mut buf = Vec::new();
            ciborium::into_writer(&v, &mut buf).expect("encode test cbor");
            buf
        };
        let limits = DecodeLimits::new(4, usize::MAX);
        for levels in [5, 64] {
            let result = decode_cbor(&deep(levels), limits);
            assert!(
                matches!(result, Err(Error::Refused(RefuseReason::TooDeep(4)))),
                "levels={levels}: {result:?}"
            );
        }
    }

    /// Reservations refusing a running total past `limit`, recording every
    /// request and the largest total held.
    struct Counter {
        limit: usize,
        held: usize,
        peak: usize,
        requests: Vec<usize>,
    }

    impl Counter {
        fn new(limit: usize) -> Self {
            Self {
                limit,
                held: 0,
                peak: 0,
                requests: Vec::new(),
            }
        }
    }

    impl Reservations for Counter {
        fn reserve(&mut self, bytes: usize) -> Result<()> {
            self.requests.push(bytes);
            let next = self.held + bytes;
            if next > self.limit {
                return Err(Error::too_large(
                    crate::error::SizeBudget::Table,
                    next,
                    self.limit,
                ));
            }
            self.held = next;
            self.peak = self.peak.max(next);
            Ok(())
        }

        fn release(&mut self, bytes: usize) {
            self.held -= bytes;
        }
    }

    fn encode(v: &ciborium::Value) -> Vec<u8> {
        let mut buf = Vec::new();
        ciborium::into_writer(v, &mut buf).expect("encode test cbor");
        buf
    }

    /// Scenario: a one-MiB flat CBOR array of small ints (32 MiB decoded) against a 16 MiB
    /// reservation limit.
    /// Guarantees: the array's own reservation is refused before any element is allocated.
    #[test]
    fn a_wide_cell_is_refused_before_its_tree_is_built() {
        let n = (1 << 20) - 5;
        let buf = encode(&ciborium::Value::Array(vec![
            ciborium::Value::Integer(
                0.into()
            );
            n
        ]));
        assert_eq!(buf.len(), 1 << 20);
        let mut counter = Counter::new(16 << 20);
        let result = decode_cbor_reserving(&buf, DecodeLimits::new(32, 1 << 20), &mut counter);
        assert!(matches!(
            result,
            Err(Error::Refused(RefuseReason::RequestTooLarge(_)))
        ));
        assert_eq!(counter.requests, vec![n * VALUE_NODE_BYTES]);
        assert_eq!(counter.held, 0);
    }

    /// Scenario: 200 000 singleton arrays against a 4 MiB limit, and a singleton chain at the depth
    /// limit.
    /// Guarantees: the wide cell is refused at the limit without exceeding it; the chain holds its
    /// decoded size.
    #[test]
    fn singleton_nesting_is_reserved_node_by_node() {
        let one = ciborium::Value::Array(vec![ciborium::Value::Integer(0.into())]);
        let wide = encode(&ciborium::Value::Array(vec![one; 200_000]));
        let mut counter = Counter::new(4 << 20);
        let result = decode_cbor_reserving(&wide, DecodeLimits::new(32, usize::MAX), &mut counter);
        assert!(matches!(
            result,
            Err(Error::Refused(RefuseReason::RequestTooLarge(_)))
        ));
        assert!(counter.peak <= 4 << 20);

        let mut chain = ciborium::Value::Integer(0.into());
        for _ in 0..32 {
            chain = ciborium::Value::Array(vec![chain]);
        }
        let mut counter = Counter::new(usize::MAX);
        let v = decode_cbor_reserving(
            &encode(&chain),
            DecodeLimits::new(32, usize::MAX),
            &mut counter,
        )
        .expect("at the depth limit");
        assert_eq!(counter.held + VALUE_NODE_BYTES, value_bytes(&v));
        assert_eq!(value_bytes(&v), 33 * VALUE_NODE_BYTES);
    }

    /// Scenario: indefinite text, bytes, array and map, a null key, `undefined`, a half float, a
    /// negative integer and a bignum.
    /// Guarantees: each decodes as expected and the decode holds its `value_bytes` less the root
    /// node.
    #[test]
    fn reservations_add_up_to_the_decoded_size() {
        let buf: Vec<u8> = [
            &[0xbf][..],                           // map, indefinite
            &[0x7f, 0x61, b'a', 0x61, b'b', 0xff], // key "ab", indefinite text
            &[
                0x9f, 0x5f, 0x41, 1, 0x41, 2, 0xff, 0xf7, 0xf9, 0x3c, 0x00, 0xff,
            ], // [h'0102', undefined, 1.0]
            &[0xf6, 0x38, 0x63],                   // null key -> -100
            &[0x61, b'z', 0xc2, 0x42, 0x01, 0x00], // "z" -> bignum 256
            &[0xff],
        ]
        .concat();
        let mut counter = Counter::new(usize::MAX);
        let v = decode_cbor_reserving(&buf, DecodeLimits::new(32, usize::MAX), &mut counter)
            .expect("decode");
        assert_eq!(
            v,
            Value::KvList(vec![
                (String::new(), Value::Int(-100)),
                (
                    "ab".into(),
                    Value::Array(vec![
                        Value::Bytes(vec![1, 2]),
                        Value::Null,
                        Value::Double(1.0)
                    ])
                ),
                ("z".into(), Value::Int(256)),
            ])
        );
        assert_eq!(counter.held + VALUE_NODE_BYTES, value_bytes(&v));
    }

    /// Scenario: a one-MiB indefinite byte string of one-byte chunks and an indefinite array of 200
    /// 000 ints.
    /// Guarantees: buffers double, so reservations are a few dozen, and the decode holds its
    /// decoded size.
    #[test]
    fn indefinite_items_grow_geometrically_within_their_reservations() {
        let chunks = (1 << 20) / 2 - 1;
        let mut bytes = vec![0x5f];
        for i in 0..chunks {
            bytes.extend_from_slice(&[0x41, i as u8]);
        }
        bytes.push(0xff);
        let mut counter = Counter::new(usize::MAX);
        let v = decode_cbor_reserving(&bytes, DecodeLimits::new(32, 1 << 20), &mut counter)
            .expect("decode");
        assert!(matches!(&v, Value::Bytes(b) if b.len() == chunks));
        assert!(counter.requests.len() < 64, "{}", counter.requests.len());
        assert_eq!(counter.held + VALUE_NODE_BYTES, value_bytes(&v));
        // The last growth held both buffers: more than the final content.
        assert!(counter.peak > chunks);

        let mut array = vec![0x9f];
        array.extend(std::iter::repeat_n(0x01, 200_000));
        array.push(0xff);
        let mut counter = Counter::new(usize::MAX);
        let v = decode_cbor_reserving(&array, DecodeLimits::new(32, usize::MAX), &mut counter)
            .expect("decode");
        assert!(counter.requests.len() < 128, "{}", counter.requests.len());
        assert_eq!(counter.held + VALUE_NODE_BYTES, value_bytes(&v));
    }

    /// Scenario: an indefinite byte string whose second chunk declares more than the input left,
    /// and an array declaring `u64::MAX` items.
    /// Guarantees: both are refused as invalid before the declared size is reserved.
    #[test]
    fn declared_sizes_are_checked_against_the_remaining_input() {
        let mut counter = Counter::new(usize::MAX);
        let chunk = [
            &[0x5f, 0x4a][..],
            &[0; 10],
            &[0x5a],
            &1010_u32.to_be_bytes(),
            &[0; 1000],
            &[0xff],
        ]
        .concat();
        assert_eq!(chunk.len(), 1018);
        assert!(matches!(
            decode_cbor_reserving(&chunk, DecodeLimits::new(32, usize::MAX), &mut counter),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
        assert!(
            counter.requests.iter().all(|&r| r < 1010),
            "{:?}",
            counter.requests
        );

        let mut counter = Counter::new(usize::MAX);
        let huge = [0x9b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
        assert!(matches!(
            decode_cbor_reserving(&huge, DecodeLimits::new(32, usize::MAX), &mut counter),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
        assert!(counter.requests.is_empty());
    }

    /// Scenario: a 3000-byte bytes body (4002 rendered characters) against a limit one byte short
    /// and none, and a nested body.
    /// Guarantees: the bound is reserved first, the short limit refuses with nothing held, and the
    /// string's length stays reserved.
    #[test]
    fn body_rendering_is_reserved_before_it_is_built() {
        let body = Value::Bytes(vec![0xAB; 3000]);
        let mut counter = Counter::new(4001);
        assert!(body_string_reserving(&body, &mut counter).is_err());
        assert_eq!(counter.requests, vec![4002]);
        assert_eq!(counter.held, 0);

        let nested = Value::KvList(vec![
            ("b".into(), body),
            ("d".into(), Value::Double(-2.225_073_858_507_201_4e-308)),
            ("q".into(), Value::Str("\"\\\n\u{1}".repeat(40))),
            (
                "s".into(),
                Value::Array(vec![Value::Double(f64::NEG_INFINITY)]),
            ),
        ]);
        for v in [nested, Value::Str("raw".into())] {
            let mut counter = Counter::new(usize::MAX);
            let rendered = body_string_reserving(&v, &mut counter)
                .expect("rendered")
                .expect("not null");
            assert_eq!(Some(&rendered), body_string(&v).as_ref());
            assert_eq!(counter.held, rendered.len());
            assert!(counter.requests[0] >= rendered.len());
        }
    }

    /// Scenario: render_v1 over every scalar kind and a nested kvlist.
    /// Guarantees: the JSON mapping of FORMAT.md section 2 is produced exactly.
    #[test]
    fn render_v1_mapping() {
        let v = Value::KvList(vec![
            ("b".into(), Value::Bytes(vec![0xab, 0x12])),
            ("d".into(), Value::Double(f64::NAN)),
            ("e".into(), Value::Double(1.5)),
            ("i".into(), Value::Int(42)),
            ("n".into(), Value::Null),
            ("s".into(), Value::Str("x".into())),
            ("t".into(), Value::Bool(true)),
        ]);
        assert_eq!(
            render_v1(&v),
            r#"{"b":"qxI=","d":"NaN","e":1.5,"i":42,"n":null,"s":"x","t":true}"#
        );
    }

    /// Scenario: map and body entry points on top-level scalars.
    /// Guarantees: strings are raw, null is None, other scalars are compact JSON.
    #[test]
    fn map_and_body_entry_points() {
        assert_eq!(
            map_string(&Value::Str("raw \"q\"".into())).as_deref(),
            Some("raw \"q\"")
        );
        assert_eq!(map_string(&Value::Null), None);
        assert_eq!(map_string(&Value::Int(42)).as_deref(), Some("42"));
        assert_eq!(
            map_string(&Value::Bytes(vec![0xab])).as_deref(),
            Some("\"qw==\"")
        );
        assert_eq!(
            map_string(&Value::Double(f64::INFINITY)).as_deref(),
            Some("\"Infinity\"")
        );
        assert_eq!(
            body_string(&Value::Str("hello".into())).as_deref(),
            Some("hello")
        );
        assert_eq!(body_string(&Value::Null), None);
        assert_eq!(
            body_string(&Value::Array(vec![Value::Int(1)])).as_deref(),
            Some("[1]")
        );
    }
}
