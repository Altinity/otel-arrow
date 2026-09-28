// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! various types & helper functions for decoding serialized protobuf data

use std::cell::{Cell, OnceCell};
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::rc::Rc;

use crate::error::Error;
use crate::proto::consts::wire_types;

/// Validates the wire framing of one protobuf message without decoding nested messages.
pub(super) fn validate_message_wire_format(buf: &[u8]) -> Result<(), Error> {
    let mut pos = 0;
    while pos < buf.len() {
        let (_, wire_type, next) =
            read_key(buf, pos).map_err(|_| Error::InvalidProtobufWireFormat)?;
        pos = value_range(buf, wire_type, next)
            .map_err(|_| Error::InvalidProtobufWireFormat)?
            .1;
    }
    Ok(())
}

/// Clones the parser, sharing the underlying buffer and interior-mutability state.
/// The cloned instance will share offsets and position with the original.
impl<T: FieldRanges> Clone for ProtoBytesParser<'_, T> {
    fn clone(&self) -> Self {
        Self {
            buf: self.buf,
            state: self.state.clone(),
        }
    }
}
/// `FieldRanges` defines the interface used by a protobuf parser to record and retrieve
/// field offsets within a serialized message buffer.
///
/// This trait is typically implemented for specific protobuf message types and used by a
/// parser like `ProtoBytesParser`, which scans the buffer once and calls `set_field_offset`
/// as fields are encountered. Later, accessors (e.g., view structs) call `get_field_offset`
/// or `get_repeated_field_offset` to retrieve the byte ranges corresponding to particular
/// fields.
///
/// Implementations of this trait are expected to use interior mutability (e.g., `Cell`)
/// so that parsing can occur even when the parser is accessed via a shared reference.
/// This makes it possible to lazily parse fields on-demand, without requiring full
/// up-front decoding.
///
/// For best performance, implementations should try to be light weight and avoid heap allocations
/// if possible.
pub trait FieldRanges {
    /// Creates a new, empty instance of this `FieldOffsets` implementation.
    fn new() -> Self;

    /// Returns the offset of the given scalar field number, if known.
    fn get_field_range(&self, field_num: u64) -> Option<(usize, usize)>;

    /// Records the offset of a field that was encountered during parsing.
    ///
    /// Called by the parser once per field occurrence, in buffer order. A singular field keeps
    /// its last occurrence, as prost does; a repeated field keeps its first, where iteration
    /// starts.
    fn set_field_range(&self, field_num: u64, wire_type: u64, start: usize, end: usize);
}

/// helper convert an Option of `NonZeroRange` into an `Option<(usize, usize)>` to adapt internal
/// range to expected return type in `FieldOffset`
#[inline]
#[must_use]
pub fn from_option_nonzero_range_to_primitive(
    range: Option<(NonZeroUsize, NonZeroUsize)>,
) -> Option<(usize, usize)> {
    range.map(|(start, end)| (start.get(), end.get()))
}

/// helper to convert the arguments of in the `FieldOffset` function into the internal type used
/// by many of it's impls.
#[inline]
#[must_use]
pub fn to_nonzero_range(start: usize, end: usize) -> Option<(NonZeroUsize, NonZeroUsize)> {
    Some((NonZeroUsize::new(start)?, NonZeroUsize::new(end)?))
}

/// `ProtoBytesParser` is a generic struct that encapsulates the logic for iterating through
/// a buffer containing a serialized protobuf message, identifying the offsets of its fields.
/// The intention is that we'll only need to pass over the buffer once, regardless of in which
/// order the fields are accessed.
///
/// Typically, an instance of this type is embedded within a higher-level type that implements
/// one of the pdata view traits. The buffer is parsed lazily as fields are accessed via the
/// view's methods.
///
/// This type collaborates with an implementation of the `FieldRanges` trait -- often specific
/// to the message being parsed -- by recording the offsets of fields as they are encountered,
/// and using these offsets to return byte slices for requested fields.
///
/// # Notes
/// - Multiple `ProtoBytesParser` instances may reference the same buffer. For example, a buffer
///   containing a `LogRecord` message may be shared by both a `LogsView` and an iterator over
///   the `LogRecord`'s attributes.
/// - Parsing may need to occur even when the container of this parser is accessed via a
///   shared or immutable reference. For example, view methods like `LogDataView::severity_text()`
///   might take `&self`, but parsing requires updating offsets.
///
/// To support these patterns, this type uses interior mutability for shared position and
/// field offset state. As such this type is clearly not `Send`.
///
pub struct ProtoBytesParser<'a, T: FieldRanges> {
    /// buffer containing the serialized proto message being parsed
    buf: &'a [u8],

    /// Shared parsing position and field ranges.
    state: Rc<ParserState<T>>,
}

/// The scan state a parser, its clones and its repeated-field iterators share.
struct ParserState<T> {
    /// The scan frontier: every field before it has been recorded, once.
    pos: Cell<usize>,
    field_ranges: T,
    /// One bit per field number below 64 recorded at least once.
    seen: Cell<u64>,
    /// One bit per field number below 64 recorded more than once.
    repeated: Cell<u64>,
    /// The merged occurrences of repeated singular message fields, see
    /// [`ProtoBytesParser::message_field`].
    merged: OnceCell<Box<Merged>>,
}

/// The concatenated occurrences of one singular message field, in a list
/// that only grows, so the bytes live as long as the parser state.
struct Merged {
    field_num: u64,
    bytes: Box<[u8]>,
    next: OnceCell<Box<Merged>>,
}

/// The value range and key of the field whose key starts at `pos`, or `None`
/// for a frame that cannot be read.
#[inline]
fn next_field(buf: &[u8], pos: usize) -> Option<(usize, usize, u64)> {
    let (tag, next) = read_varint(buf, pos)?;
    let (start, end) = field_range(buf, tag, next)?;
    Some((start, end, tag))
}

impl<T: FieldRanges> ParserState<T> {
    fn new() -> Self {
        Self {
            pos: Cell::new(0),
            field_ranges: T::new(),
            seen: Cell::new(0),
            repeated: Cell::new(0),
            merged: OnceCell::new(),
        }
    }

    /// Record field `field_num` of `wire_type`, whose value is `start..end`,
    /// and move the frontier past it; a field behind the frontier was
    /// recorded before and is skipped.
    #[inline]
    fn record(&self, field_num: u64, wire_type: u64, start: usize, end: usize) {
        if start < self.pos.get() {
            return;
        }
        self.field_ranges
            .set_field_range(field_num, wire_type, start, end);
        if field_num < 64 {
            let bit = 1u64 << field_num;
            if self.seen.get() & bit != 0 {
                self.repeated.set(self.repeated.get() | bit);
            }
            self.seen.set(self.seen.get() | bit);
        }
        self.pos.set(end);
    }

    /// Move the frontier over the next field of `buf`. Returns `false` at the
    /// end of `buf`, and at a frame that cannot be read, which ends the scan:
    /// what follows it reads as absent.
    #[inline]
    fn step(&self, buf: &[u8]) -> bool {
        let pos = self.pos.get();
        if pos >= buf.len() {
            return false;
        }
        let Some((start, end, tag)) = next_field(buf, pos) else {
            self.pos.set(buf.len());
            return false;
        };
        self.record(tag >> 3, tag & 7, start, end);
        true
    }

    /// Record every field of `buf`.
    #[inline]
    fn scan_to_end(&self, buf: &[u8]) {
        while self.step(buf) {}
    }

    /// Whether field `field_num` was recorded more than once.
    #[inline]
    fn is_repeated(&self, field_num: u64) -> bool {
        field_num < 64 && self.repeated.get() & (1 << field_num) != 0
    }
}

impl<'a, T> ProtoBytesParser<'a, T>
where
    T: FieldRanges,
{
    /// Create a new instance of `ProtoBytesParser`
    #[must_use]
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            state: Rc::new(ParserState::new()),
        }
    }

    /// Returns the value of the singular scalar field `field_num` as a byte slice, if present.
    ///
    /// The whole message is scanned first, so a field that occurs more than once reads as its
    /// last occurrence, the one prost keeps.
    #[inline]
    #[must_use]
    pub fn advance_to_find_field(&self, field_num: u64) -> Option<&'a [u8]> {
        self.state.scan_to_end(self.buf);
        let (start, end) = self.state.field_ranges.get_field_range(field_num)?;
        Some(&self.buf[start..end])
    }

    /// Returns the value of the scalar oneof whose members are `field_nums`, and the member's
    /// field number: the member that occurs last, as prost decodes it.
    #[must_use]
    pub fn advance_to_find_oneof(&self, field_nums: &[u64]) -> Option<(&'a [u8], u64)> {
        self.state.scan_to_end(self.buf);
        field_nums
            .iter()
            .filter_map(|&num| Some((self.state.field_ranges.get_field_range(num)?, num)))
            .max_by_key(|((start, _), _)| *start)
            .map(|((start, end), num)| (&self.buf[start..end], num))
    }

    /// Returns the value of the singular message field `field_num`, if present.
    ///
    /// Protobuf merges the occurrences of a message field, which reads as one message holding
    /// their concatenation; a field that occurs more than once is returned as that
    /// concatenation, built once and kept as long as the parser.
    #[must_use]
    pub fn message_field(&self, field_num: u64) -> Option<&[u8]> {
        let slice = self.advance_to_find_field(field_num)?;
        if !self.state.is_repeated(field_num) {
            return Some(slice);
        }
        Some(self.merged(field_num, &[field_num]))
    }

    /// Returns the value of the message-typed oneof whose members are `field_nums`, and the
    /// member's field number: the member that occurs last, with its occurrences since any other
    /// member last occurred merged as [`Self::message_field`] merges them, as prost decodes it.
    #[must_use]
    pub fn message_oneof(&self, field_nums: &[u64]) -> Option<(&[u8], u64)> {
        let (slice, field_num) = self.advance_to_find_oneof(field_nums)?;
        if !self.state.is_repeated(field_num) {
            return Some((slice, field_num));
        }
        Some((self.merged(field_num, field_nums), field_num))
    }

    /// The concatenated length-delimited occurrences of `field_num` that follow the last
    /// occurrence of any other of `members`.
    fn merged(&self, field_num: u64, members: &[u64]) -> &[u8] {
        let mut slot = &self.state.merged;
        while let Some(merged) = slot.get() {
            if merged.field_num == field_num {
                return &merged.bytes;
            }
            slot = &merged.next;
        }
        let mut bytes = Vec::new();
        let mut pos = 0;
        while let Some((start, end, tag)) = next_field(self.buf, pos) {
            pos = end;
            if tag & 7 != wire_types::LEN {
                continue;
            }
            if tag >> 3 == field_num {
                bytes.extend_from_slice(&self.buf[start..end]);
            } else if members.contains(&(tag >> 3)) {
                bytes.clear();
            }
        }
        &slot
            .get_or_init(|| {
                Box::new(Merged {
                    field_num,
                    bytes: bytes.into_boxed_slice(),
                    next: OnceCell::new(),
                })
            })
            .bytes
    }
}

/// The range of the field whose key `tag` ends at `pos`, as a message scanner
/// needs it to step to the next field: the [`value_range`] of the value, and
/// for an unknown group the range from just past its start key to just past
/// its end key, found by [`skip_group`], so a balanced group never ends a
/// scan early and hides the fields after it. Anything [`value_range`] or
/// [`skip_group`] refuses is `None`, so callers treat malformed input as an
/// absent field.
#[inline]
pub(crate) fn field_range(buf: &[u8], tag: u64, pos: usize) -> Option<(usize, usize)> {
    match tag & 7 {
        START_GROUP => {
            let end = skip_group(buf, pos, tag >> 3, 1, pos).ok()?;
            Some((pos, end))
        }
        wire_type => value_range(buf, wire_type, pos).ok(),
    }
}

/// The deepest nesting of groups [`skip_group`] steps over, which bounds its
/// recursion; prost's decoder stops at 100 message levels.
pub(crate) const MAX_NESTING_DEPTH: usize = 256;

/// A broken frame found while stepping over a field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireProblem {
    /// A field key that does not terminate or overflows a `u64`.
    TruncatedKey,
    /// A field key above the 32-bit key range or with field number zero.
    InvalidKey,
    /// A varint value that does not terminate or overflows a `u64`.
    TruncatedVarint,
    /// A length prefix that does not terminate or overflows a `u64`.
    TruncatedLength,
    /// A length-delimited value longer than what is left of its message.
    LengthOverrun,
    /// A fixed-width value longer than what is left of its message.
    TruncatedFixed,
    /// Wire type 6 or 7, or a group where no group may start.
    UnsupportedWireType,
    /// A known field with a wire type its schema does not give it.
    WrongWireType,
    /// A packed `fixed64` or `double` field that is not a multiple of 8 bytes.
    RaggedPackedFixed64,
    /// A packed `fixed32` or `float` field that is not a multiple of 4 bytes.
    RaggedPackedFixed32,
    /// A packed varint field ending inside a varint.
    TruncatedPackedVarint,
    /// An end group key with no group open.
    StrayEndGroup,
    /// An end group key of another field number than the open group.
    MismatchedEndGroup,
    /// A group still open where its enclosing message ends.
    UnclosedGroup,
}

impl WireProblem {
    /// The sentence a refusal quotes.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TruncatedKey => "truncated or overlong field key",
            Self::InvalidKey => "invalid field key",
            Self::TruncatedVarint => "truncated or overlong varint",
            Self::TruncatedLength => "truncated or overlong length prefix",
            Self::LengthOverrun => "length-delimited field overruns its message",
            Self::TruncatedFixed => "truncated fixed-width field",
            Self::UnsupportedWireType => "unsupported wire type",
            Self::WrongWireType => "wrong wire type for a known field",
            Self::RaggedPackedFixed64 => "packed fixed64 field is not a whole number of elements",
            Self::RaggedPackedFixed32 => "packed fixed32 field is not a whole number of elements",
            Self::TruncatedPackedVarint => "truncated or overlong varint in a packed field",
            Self::StrayEndGroup => "end group without a start group",
            Self::MismatchedEndGroup => "end group does not match its start group",
            Self::UnclosedGroup => "group without an end group",
        }
    }
}

impl fmt::Display for WireProblem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Wire type 3: the start of a group (proto2), carrying no length.
pub(crate) const START_GROUP: u64 = 3;
/// Wire type 4: the end of the group opened by the same field number.
pub(crate) const END_GROUP: u64 = 4;

/// Decode the field key at `pos`: its field number, its wire type and the
/// position just past it. A key outside protobuf's 32-bit range, or with
/// field number zero, is refused.
#[inline]
pub(crate) fn read_key(buf: &[u8], pos: usize) -> Result<(u64, u64, usize), WireProblem> {
    let (tag, next) = read_varint(buf, pos).ok_or(WireProblem::TruncatedKey)?;
    let field_num = tag >> 3;
    if tag > u64::from(u32::MAX) || field_num == 0 {
        return Err(WireProblem::InvalidKey);
    }
    Ok((field_num, tag & 7, next))
}

/// The byte range of the value of a field of `wire_type` whose key ends at
/// `pos`, bounds-checked against `buf`. For a length-delimited field the
/// range excludes the length prefix. Groups (wire types 3 and 4) are refused
/// here, because skipping one needs its field number: see [`skip_group`].
#[inline]
pub(crate) fn value_range(
    buf: &[u8],
    wire_type: u64,
    pos: usize,
) -> Result<(usize, usize), WireProblem> {
    match wire_type {
        wire_types::VARINT => {
            let (_, end) = read_varint(buf, pos).ok_or(WireProblem::TruncatedVarint)?;
            Ok((pos, end))
        }
        wire_types::LEN => {
            let (len, start) = read_varint(buf, pos).ok_or(WireProblem::TruncatedLength)?;
            let end = usize::try_from(len)
                .ok()
                .and_then(|len| start.checked_add(len))
                .filter(|&end| end <= buf.len())
                .ok_or(WireProblem::LengthOverrun)?;
            Ok((start, end))
        }
        wire_types::FIXED64 => fixed_range(buf, pos, 8),
        wire_types::FIXED32 => fixed_range(buf, pos, 4),
        _ => Err(WireProblem::UnsupportedWireType),
    }
}

#[inline]
fn fixed_range(buf: &[u8], pos: usize, width: usize) -> Result<(usize, usize), WireProblem> {
    pos.checked_add(width)
        .filter(|&end| end <= buf.len())
        .map(|end| (pos, end))
        .ok_or(WireProblem::TruncatedFixed)
}

/// Why [`skip_group`] could not skip a group; `at` is a position in the
/// buffer it was given.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkipError {
    /// The group's framing is broken.
    Framing { problem: WireProblem, at: usize },
    /// Groups nest deeper than
    /// [`MAX_NESTING_DEPTH`].
    TooDeep { at: usize },
}

/// Skip the group of field `field_num` whose start key is at `group_at` and
/// ends at `pos`, as prost skips an unknown group: every field inside is
/// framed and skipped, nested groups are skipped the same way, and the group
/// must close with an end key of its own field number before `buf` ends.
/// `depth` is the nesting level of this group, counted against
/// [`MAX_NESTING_DEPTH`], which bounds the
/// recursion, together with whatever nesting the caller already holds.
/// Returns the position just past the end key.
///
/// This is the one group skipper, so every scanner that steps over a group
/// accepts the same groups.
pub(crate) fn skip_group(
    buf: &[u8],
    mut pos: usize,
    field_num: u64,
    depth: usize,
    group_at: usize,
) -> Result<usize, SkipError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(SkipError::TooDeep { at: group_at });
    }
    loop {
        if pos >= buf.len() {
            return Err(SkipError::Framing {
                problem: WireProblem::UnclosedGroup,
                at: group_at,
            });
        }
        let at = pos;
        let fail = |problem| SkipError::Framing { problem, at };
        let (num, wire_type, next) = read_key(buf, pos).map_err(fail)?;
        pos = match wire_type {
            END_GROUP if num == field_num => return Ok(next),
            END_GROUP => return Err(fail(WireProblem::MismatchedEndGroup)),
            START_GROUP => skip_group(buf, next, num, depth + 1, at)?,
            _ => value_range(buf, wire_type, next).map_err(fail)?.1,
        };
    }
}

/// `RepeatedFieldProtoBytesParser` is an iterator over byte slices for some field (represented by
/// `field_num` member). It parses the protobuf serialized message as it produces slices of the
/// underlying buffer, and keeps track of the ranges for other field types it encounters.
///
/// Typically an instance of this type is embedded within an adapter iterator that yields view
/// implementations for a repeated field.
pub struct RepeatedFieldProtoBytesParser<'a, T: FieldRanges> {
    /// buffer containing the serialized proto message being parsed
    buf: &'a [u8],

    /// Shared parsing position and field ranges.
    state: Rc<ParserState<T>>,

    field_num: u64,
    expected_wire_type: u64,

    /// A second wire type the field may use, for repeated scalars that mix packed and expanded
    /// occurrences.
    other_wire_type: Option<u64>,

    /// pointer to the next range (containing the serialized message) that the iterator will yield
    /// when `next` invoked
    next_range: Option<(usize, usize)>,

    /// wire type of the occurrence at `next_range`
    next_wire_type: u64,

    values_exhausted: bool,
}

impl<'a, T> RepeatedFieldProtoBytesParser<'a, T>
where
    T: FieldRanges,
{
    /// Create a new instance of `RepeatedFieldProtoBytesParser` with the same buffer and parser
    /// state as the passed `ProtoByteParser` that will implement iterator for the given field
    #[must_use]
    pub fn from_byte_parser(
        other: &ProtoBytesParser<'a, T>,
        field_num: u64,
        expected_wire_type: u64,
    ) -> Self {
        Self {
            buf: other.buf,
            state: other.state.clone(),
            field_num,
            expected_wire_type,
            other_wire_type: None,
            next_range: None,
            next_wire_type: expected_wire_type,
            values_exhausted: false,
        }
    }

    /// Yields the next occurrence of the field with the wire type it was written with.
    fn next_occurrence(&mut self) -> Option<(&'a [u8], u64)> {
        if self.values_exhausted {
            return None;
        }

        // initialize first range
        while self.next_range.is_none() {
            // try to get the field offset if it is known
            let range = self.state.field_ranges.get_field_range(self.field_num);

            match range {
                Some(range) => self.next_range = Some(range),
                // end of buffer, field not found
                None if !self.state.step(self.buf) => return None,
                None => {}
            }
        }

        let mut range = self
            .next_range
            .expect("iter position should be initialized");

        // this is the return value
        let slice = &self.buf[range.0..range.1];
        let slice_wire_type = self.next_wire_type;

        // advance until until either we've found the next repeated value, or the end is reached
        loop {
            // if we're at end of buffer, stop iterating
            if range.0 >= self.buf.len() || range.1 >= self.buf.len() {
                self.values_exhausted = true;
                break;
            }

            let (tag, next_pos) = read_varint(self.buf, range.1)?;
            let field = tag >> 3;
            let wire_type = tag & 7;
            range = field_range(self.buf, tag, next_pos)?;
            self.state.record(field, wire_type, range.0, range.1);

            if field == self.field_num
                && (wire_type == self.expected_wire_type || Some(wire_type) == self.other_wire_type)
            {
                self.next_wire_type = wire_type;
                break;
            }
        }

        self.next_range = Some(range);

        Some((slice, slice_wire_type))
    }
}

impl<'a, T> Iterator for RepeatedFieldProtoBytesParser<'a, T>
where
    T: FieldRanges,
{
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.next_occurrence().map(|(slice, _)| slice)
    }
}

/// This is a helper trait for adapting iterators of primitive fields using the packed encoding
/// so that they can be used generically in [`RepeatedPrimitiveIter`]
trait PackedIter<'a> {
    type DecodedValue;

    fn new(buffer: &'a [u8]) -> Self;

    fn decode_value(slice: &[u8]) -> Option<Self::DecodedValue>;
}

/// Iterator for producing elements whose type is a repeated primitive where that primitive
/// can be encoded as a fixed64. e.g. `repeated double` or `repeated fixed64` and the field
/// is encoded using packed encoding.
///
/// Note this is for internal use only. The proto documentation states that decoders should
/// have the flexibility to accept both packed and expanded encodings. Unless we're sure that
/// the field is packed encoded, it's safer to use [`RepeatedFixed64Iter`] which automatically
/// handles both encodings.
pub struct PackedFixed64Iter<'a, V: FromFixed64> {
    buffer: &'a [u8],
    pos: usize,
    _pd: PhantomData<V>,
}

impl<'a, V> PackedIter<'a> for PackedFixed64Iter<'a, V>
where
    V: FromFixed64,
{
    type DecodedValue = V;

    fn new(buffer: &'a [u8]) -> Self {
        Self {
            buffer,
            pos: 0,
            _pd: PhantomData,
        }
    }

    fn decode_value(slice: &[u8]) -> Option<Self::DecodedValue> {
        let slice: [u8; 8] = slice.try_into().ok()?;
        Some(V::from_fixed64_slice(slice))
    }
}

impl<'a, V> Iterator for PackedFixed64Iter<'a, V>
where
    V: FromFixed64,
{
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos + 8 > self.buffer.len() {
            // we've reached end of buffer
            return None;
        }

        let slice: [u8; 8] = self.buffer[self.pos..self.pos + 8]
            .try_into()
            .expect("can convert slice of fixed size");
        self.pos += 8;
        Some(V::from_fixed64_slice(slice))
    }
}

/// Helper trait for converting an iterator of slices of ranges from a proto buffer into a value
/// that can be produced from a byte slice with a wire type of FIXED64 (e.g. double)
pub trait FromFixed64 {
    /// create new value from a slice that was proto serialized with wire type FIXED64
    fn from_fixed64_slice(slice: [u8; 8]) -> Self;
}

impl FromFixed64 for f64 {
    fn from_fixed64_slice(slice: [u8; 8]) -> Self {
        f64::from_le_bytes(slice)
    }
}

impl FromFixed64 for u64 {
    fn from_fixed64_slice(slice: [u8; 8]) -> Self {
        u64::from_le_bytes(slice)
    }
}

/// Iterator for producing elements whose type is a repeated primitive where that primitive
/// can be encoded as a varint. e.g. `repeated uint64`. and the field is encoded using packed
/// encoding.
///
/// Note this is for internal use only. The proto documentation states that decoders should
/// have the flexibility to accept both packed and expanded encodings. Unless we're sure that
/// the field is packed encoded, it's safer to use [`RepeatedFixed64Iter`] which automatically
/// handles both encodings.
pub struct PackedVarintIter<'a> {
    buffer: &'a [u8],
    pos: usize,
}

impl<'a> PackedIter<'a> for PackedVarintIter<'a> {
    type DecodedValue = u64;

    fn new(buffer: &'a [u8]) -> Self {
        Self { buffer, pos: 0 }
    }

    fn decode_value(slice: &[u8]) -> Option<Self::DecodedValue> {
        let (val, _) = read_varint(slice, 0)?;
        Some(val)
    }
}

// Note this only produces u64 currently as that just happens to be the only type of value in the
// OTLP data model that ends up getting encoded like that. if we ever need this to be generic over
// the return type, we could easily do that (like we do for [`PackedFixed64Iter`])
impl<'a> Iterator for PackedVarintIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let (val, next_pos) = read_varint(self.buffer, self.pos)?;
        self.pos = next_pos;

        Some(val)
    }
}

/// Trait for [`FieldRanges`] that also contain repeated primitive fields which may be encoded
/// using either packed or expanded encoding.
///
/// The expectation here is that the implementation of the trait will also discover the type
/// of encoding that is used when `FieldRanges::set_field_range` is set because the wire type
/// is also passed to this call.
pub trait RepeatedFieldEncodings: FieldRanges {
    /// returns `true` if the field is using packed encoding or `false` if the field is using
    /// expanded encoding.
    fn is_packed(&self, field_num: u64) -> bool;
}

/// Generic iterator for repeated primitive fields that are possibly using the packed encoding
///
/// This iterator will determine the encoding, and produce the decoded field values (of type `V`)
/// from the proto buffer which contains the repeated values.
pub struct RepeatedPrimitiveIter<'a, T: RepeatedFieldEncodings, V, P> {
    buf: &'a [u8],
    field_num: u64,
    state: Rc<ParserState<T>>,

    // expected wire type for the repeated field .. e.g. if the field is type `repeated double`
    // this would be wire_types::FIXED64
    repeated_wire_type: u64,

    // this will be used to iterate over either:
    // - the repeated values (in case that the encoding is expanded)
    // - the segments containing the packed fields (in case that the encoding is packed)
    field_iter: Option<RepeatedFieldProtoBytesParser<'a, T>>,

    // the packed chunk being read, if the last occurrence was packed. one instance of the packed
    // encoder (type P) is created for each segment of the buffer containing packed values
    packed_iter: Option<P>,

    _pd: PhantomData<V>,
}

impl<'a, T, V, P> RepeatedPrimitiveIter<'a, T, V, P>
where
    T: RepeatedFieldEncodings,
{
    fn from_byte_parser_inner(
        other: &ProtoBytesParser<'a, T>,
        field_num: u64,
        repeated_wire_type: u64,
    ) -> Self {
        Self {
            buf: other.buf,
            field_num,
            state: other.state.clone(),
            repeated_wire_type,
            _pd: PhantomData,

            // following fields will be initialized lazily once the first occurrence is found
            field_iter: None,
            packed_iter: None,
        }
    }
}

impl<'a, T, V, P> Iterator for RepeatedPrimitiveIter<'a, T, V, P>
where
    P: PackedIter<'a, DecodedValue = V> + Iterator<Item = V>,
    T: RepeatedFieldEncodings,
{
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        // initialize the inner iterator
        while self.field_iter.is_none() {
            let range = self.state.field_ranges.get_field_range(self.field_num);
            match range {
                Some(range) => {
                    // each occurrence may be packed or expanded, whatever the first one was
                    let first_wire_type = if self.state.field_ranges.is_packed(self.field_num) {
                        wire_types::LEN
                    } else {
                        self.repeated_wire_type
                    };
                    self.field_iter = Some(RepeatedFieldProtoBytesParser {
                        buf: self.buf,
                        state: self.state.clone(),
                        field_num: self.field_num,
                        next_range: Some(range),
                        next_wire_type: first_wire_type,
                        values_exhausted: false,
                        expected_wire_type: wire_types::LEN,
                        other_wire_type: Some(self.repeated_wire_type),
                    });
                }
                // end of buffer, field not found
                None if !self.state.step(self.buf) => return None,
                None => {}
            }
        }

        // safety: this will have been initialized already
        let field_iter = self.field_iter.as_mut().expect("field iter initialized");

        loop {
            if let Some(val) = self.packed_iter.as_mut().and_then(Iterator::next) {
                return Some(val);
            }
            self.packed_iter = None;

            // packed chunks, empty ones included, and expanded values may interleave
            let (slice, wire_type) = field_iter.next_occurrence()?;
            if wire_type == wire_types::LEN {
                self.packed_iter = Some(P::new(slice));
            } else {
                return P::decode_value(slice);
            }
        }
    }
}

/// This iterator can be used for repeated primitives whose types are are encoded as `fixed64`.
/// For example it can be used for field types such as `repeated double` or `repeated fixed64`
pub type RepeatedFixed64Iter<'a, T, V> = RepeatedPrimitiveIter<'a, T, V, PackedFixed64Iter<'a, V>>;

impl<'a, T, V> RepeatedFixed64Iter<'a, T, V>
where
    T: RepeatedFieldEncodings,
    V: FromFixed64,
{
    /// Initialize a new instance using the [`ProtoBytesParser`] which was parsing the prot buffer
    /// that contains this repeated field
    #[must_use]
    pub fn from_byte_parser(other: &ProtoBytesParser<'a, T>, field_num: u64) -> Self {
        Self::from_byte_parser_inner(other, field_num, wire_types::FIXED64)
    }
}

/// This iterator can be used for repeated primitives whose types are are encoded as `varint`.
/// For example it can be used for field types such as `repeated uint64`
pub type RepeatedVarintIter<'a, T> = RepeatedPrimitiveIter<'a, T, u64, PackedVarintIter<'a>>;

impl<'a, T> RepeatedVarintIter<'a, T>
where
    T: RepeatedFieldEncodings,
{
    /// Initialize a new instance using the [`ProtoBytesParser`] which was parsing the prot buffer
    /// that contains this repeated field
    #[must_use]
    pub fn from_byte_parser(other: &ProtoBytesParser<'a, T>, field_num: u64) -> Self {
        Self::from_byte_parser_inner(other, field_num, wire_types::VARINT)
    }
}

/// Decode the varint at `pos` in `buf`, returning its value and the position
/// just past it.
///
/// Returns `None` for a varint that runs past the end of `buf`, is longer
/// than ten bytes, or whose tenth byte carries bits beyond the 64th (a tenth
/// byte above `0x01`), as prost refuses them: such a varint does not encode a
/// `u64`, and reading it modulo 2^64 would turn damage into a value.
#[inline]
#[must_use]
pub fn read_varint(buf: &[u8], mut pos: usize) -> Option<(u64, usize)> {
    let mut out = 0u64;
    let mut shift = 0u32;

    while pos < buf.len() && shift < 64 {
        let byte = buf[pos];
        pos += 1;

        out |= ((byte & 0x7F) as u64) << shift;

        if byte < 0x80 {
            // At shift 63 only the lowest bit still fits in a u64.
            if shift == 63 && byte > 0x01 {
                return None;
            }
            return Some((out, pos));
        }

        shift += 7;
    }

    None
}

/// Decode 32 bit zigzag encoding
#[inline]
#[must_use]
pub const fn decode_sint32(val: u32) -> i32 {
    ((val >> 1) as i32) ^ -((val & 1) as i32)
}

/// Decode length from byte slice and return a new slice of the buffer and the decoded length.
#[inline]
#[must_use]
pub fn read_len_delim(buf: &[u8], pos: usize) -> Option<(&[u8], usize)> {
    let (len, mut p) = read_varint(buf, pos)?;
    let end = p.checked_add(len as usize)?;
    if end > buf.len() {
        return None;
    }
    let slice = &buf[p..end];
    p = end;
    Some((slice, p))
}

/// Reads 4 bytes from the buffer at the given position, assuming a `fixed32` protobuf field.
///
/// Returns `None` if fewer than 4 bytes remain starting at `pos`.
/// On success, returns a tuple of the 4-byte slice and the updated position (`pos + 4`).
#[inline]
#[must_use]
pub fn read_fixed32(buf: &[u8], pos: usize) -> Option<(&[u8], usize)> {
    let len = 4;
    let end = pos.checked_add(len)?;
    if end > buf.len() {
        return None;
    }
    let slice = &buf[pos..end];
    Some((slice, end))
}

/// Reads 8 bytes from the buffer at the given position, assuming a `fixed64` protobuf field.
///
/// Returns `None` if fewer than 4 bytes remain starting at `pos`.
/// On success, returns a tuple of the 4-byte slice and the updated position (`pos + 8`).
#[inline]
#[must_use]
pub fn read_fixed64(buf: &[u8], pos: usize) -> Option<(&[u8], usize)> {
    let len = 8;
    let end = pos.checked_add(len)?;
    if end > buf.len() {
        return None;
    }
    let slice = &buf[pos..end];
    Some((slice, end))
}

/// Fields like dropped_attributes_count, dropped_events_count and dropped_links count all have
/// this common logic where they're encoded as varints and interpreted to be zero if the field
/// is missing. This helper encapsulates that logic
#[inline]
#[must_use]
pub fn read_dropped_count(buf: Option<&[u8]>) -> u32 {
    match buf {
        Some(slice) => match read_varint(slice, 0) {
            Some((val, _)) => val as u32,
            None => 0,
        },
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: A protobuf message starts with a varint key equal to `u32::MAX + 1`.
    /// Guarantees: Top-level wire validation rejects keys outside protobuf's 32-bit key range.
    #[test]
    fn rejects_key_larger_than_u32_max() {
        let oversized_key = [0x80, 0x80, 0x80, 0x80, 0x10, 0x00];

        assert!(matches!(
            validate_message_wire_format(&oversized_key),
            Err(Error::InvalidProtobufWireFormat)
        ));
    }

    /// Scenario: ZigZag decoding receives the encoded endpoints of the sint32 range.
    /// Guarantees: Values using the high bit decode without signed truncation.
    #[test]
    fn decodes_full_sint32_range() {
        assert_eq!(decode_sint32(0), 0);
        assert_eq!(decode_sint32(1), -1);
        assert_eq!(decode_sint32(u32::MAX - 1), i32::MAX);
        assert_eq!(decode_sint32(u32::MAX), i32::MIN);
    }

    /// Scenario: ten-byte varints whose tenth byte is `0x01` (`u64::MAX`),
    /// `0x02` (the 65th bit set) and `0x7f`, an eleven-byte varint, and the
    /// same overflowing varint as the value of a top-level varint field.
    /// Guarantees: the maximum `u64` decodes to itself in ten bytes, and every
    /// varint carrying bits past the 64th is refused rather than read modulo
    /// 2^64, as prost refuses it: `80 80 80 80 80 80 80 80 80 02` is not
    /// zero.
    #[test]
    fn refuses_varints_that_overflow_u64() {
        let max = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
        assert_eq!(read_varint(&max, 0), Some((u64::MAX, 10)));
        let mut overflow = [0x80; 10];
        overflow[9] = 0x02;
        assert_eq!(read_varint(&overflow, 0), None);
        overflow[9] = 0x7f;
        assert_eq!(read_varint(&overflow, 0), None);
        let mut eleven = [0x80; 11];
        eleven[10] = 0x00;
        assert_eq!(read_varint(&eleven, 0), None);

        let mut field = vec![0x08];
        field.extend([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02]);
        assert!(matches!(
            validate_message_wire_format(&field),
            Err(Error::InvalidProtobufWireFormat)
        ));
    }
}
