// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! This module contains the implementation of the pdata View traits for serialized OTLP protobuf
//! bytes for messages defined in common.proto

use std::cell::Cell;
use std::num::NonZeroUsize;

use crate::proto::consts::field_num::common::{
    ANY_VALUE_ARRAY_VALUE, ANY_VALUE_BOOL_VALUE, ANY_VALUE_BYTES_VALUE, ANY_VALUE_DOUBLE_VALUE,
    ANY_VALUE_INT_VALUE, ANY_VALUE_KVLIST_VALUE, ANY_VALUE_STRING_VALUE, ARRAY_VALUE_VALUES,
    INSTRUMENTATION_DROPPED_ATTRIBUTES_COUNT, INSTRUMENTATION_SCOPE_ATTRIBUTES,
    INSTRUMENTATION_SCOPE_NAME, INSTRUMENTATION_SCOPE_VERSION, KEY_VALUE_KEY,
    KEY_VALUE_LIST_VALUES, KEY_VALUE_VALUE,
};
use crate::proto::consts::wire_types;
use crate::views::otlp::bytes::decode::{
    FieldRanges, ProtoBytesParser, RepeatedFieldProtoBytesParser, field_range,
    from_option_nonzero_range_to_primitive, read_dropped_count, read_len_delim, read_varint,
    to_nonzero_range,
};
use otel_arrow_dfe_pdata_views::views::common::{
    AnyValueView, AttributeView, InstrumentationScopeView, ValueType,
};

/// Implementation of `AttributeView` backed by protobuf serialized `KeyValue` message
pub struct RawKeyValue<'a> {
    // serialized message
    buf: &'a [u8],

    // whether the whole buffer has been scanned for `key` and `value`
    scanned: Cell<bool>,

    // the offsets of the last `key` and `value`, set by `scan`
    key_range: Cell<Option<(NonZeroUsize, NonZeroUsize)>>,
    value_range: Cell<Option<(NonZeroUsize, NonZeroUsize)>>,

    // whether `value` occurs more than once, which prost merges
    value_repeated: Cell<bool>,
}

impl<'a> RawKeyValue<'a> {
    /// Create a new RawKeyValue parser from a byte slice containing a KeyValue message.
    #[inline]
    #[must_use]
    pub const fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            scanned: Cell::new(false),
            value_range: Cell::new(None),
            key_range: Cell::new(None),
            value_repeated: Cell::new(false),
        }
    }

    /// Scan the whole buffer once, keeping the last `key` and `value`, as
    /// prost does; the scan stops at a frame that cannot be read.
    #[inline]
    fn scan(&self) {
        if self.scanned.replace(true) {
            return;
        }
        let mut pos = 0;
        while pos < self.buf.len() {
            let Some((tag, next_pos)) = read_varint(self.buf, pos) else {
                return;
            };
            let Some((start, end)) = field_range(self.buf, tag, next_pos) else {
                return;
            };
            pos = end;
            // Only `key` and `value` are read, both length-delimited; any
            // other field has been stepped over by its own wire type.
            if tag & 7 != wire_types::LEN {
                continue;
            }
            match tag >> 3 {
                KEY_VALUE_KEY => self.key_range.set(to_nonzero_range(start, end)),
                KEY_VALUE_VALUE => {
                    if self.value_range.get().is_some() {
                        self.value_repeated.set(true);
                    }
                    self.value_range.set(to_nonzero_range(start, end));
                }
                _ => {}
            }
        }
    }
}

/// RawAnyValue implements `AnyValueView` backed by a byte buffer containing protobuf serialized
/// `AnyValue` message, or by every occurrence of an `AnyValue` field in a message, which prost
/// merges into one: the occurrences are read in place, as one sequence of fields.
pub struct RawAnyValue<'a> {
    fields: Fields<'a>,

    // the variant, which will be determined from the field tag while parsing the buffer
    variant: Cell<Option<ValueType>>,

    // set with `variant`: for a scalar member, `pos..end` is the value's range; for
    // `array_value` and `kvlist_value`, the cursor is at the key of the first occurrence of the
    // member's final run instead, because prost merges consecutive occurrences of one
    // message-typed member and the view yields all of them (see [`MergedMember`]).
    value_at: Cell<Option<Cursor>>,
}

impl<'a> RawAnyValue<'a> {
    /// create a new instance of RawAnyValue
    #[inline]
    #[must_use]
    pub const fn new(buf: &'a [u8]) -> Self {
        Self::from_fields(Fields::of_message(buf))
    }

    /// The `AnyValue` that prost decodes from every length-delimited occurrence of field
    /// `field_num` in the message `buf`.
    #[inline]
    #[must_use]
    pub(crate) const fn merged(buf: &'a [u8], field_num: u64) -> Self {
        Self::from_fields(Fields::of_occurrences(buf, field_num))
    }

    const fn from_fields(fields: Fields<'a>) -> Self {
        Self {
            fields,
            value_at: Cell::new(None),
            variant: Cell::new(None),
        }
    }

    /// The range of a scalar member's value, set by `value_type`.
    fn scalar(&self, expected: ValueType) -> Option<&'a [u8]> {
        if self.value_type() != expected {
            return None;
        }
        let at = self.value_at.get()?;
        self.fields.buf.get(at.pos..at.end)
    }

    /// The fields of the member's final run, from its first occurrence on, set by
    /// `value_type`.
    fn run(&self, expected: ValueType) -> Option<Fields<'a>> {
        if self.value_type() != expected {
            return None;
        }
        Some(Fields {
            cursor: self.value_at.get()?,
            ..self.fields
        })
    }
}

/// A position in the fields of an `AnyValue`: the next field's key is at `pos`, in the
/// occurrence that ends at `end`; the next occurrence is searched for from `outer`.
#[derive(Clone, Copy)]
struct Cursor {
    outer: usize,
    pos: usize,
    end: usize,
}

/// One field of an `AnyValue`, with positions in the buffer its [`Fields`] reads.
struct Field {
    /// A cursor at the field's key.
    at: Cursor,
    tag: u64,
    /// The field's value range, without a length prefix.
    start: usize,
    end: usize,
}

/// The fields of an `AnyValue` read in place: one serialized message, or every occurrence of
/// one length-delimited field of an enclosing message, in order. A frame that cannot be read
/// ends the fields.
#[derive(Clone, Copy)]
struct Fields<'a> {
    buf: &'a [u8],
    /// The field whose occurrences hold the value; zero when `buf` is the value itself.
    occurrence_of: u64,
    cursor: Cursor,
}

impl<'a> Fields<'a> {
    const fn of_message(buf: &'a [u8]) -> Self {
        Self {
            buf,
            occurrence_of: 0,
            cursor: Cursor {
                outer: buf.len(),
                pos: 0,
                end: buf.len(),
            },
        }
    }

    const fn of_occurrences(buf: &'a [u8], field_num: u64) -> Self {
        Self {
            buf,
            occurrence_of: field_num,
            cursor: Cursor {
                outer: 0,
                pos: 0,
                end: 0,
            },
        }
    }
}

impl Iterator for Fields<'_> {
    type Item = Field;

    fn next(&mut self) -> Option<Field> {
        loop {
            let cursor = self.cursor;
            if cursor.pos < cursor.end {
                let occurrence = &self.buf[..cursor.end];
                let (tag, next) = read_varint(occurrence, cursor.pos)?;
                let (start, end) = field_range(occurrence, tag, next)?;
                self.cursor.pos = end;
                return Some(Field {
                    at: cursor,
                    tag,
                    start,
                    end,
                });
            }
            // The occurrence is exhausted: step to the next one.
            if self.occurrence_of == 0 {
                return None;
            }
            loop {
                if self.cursor.outer >= self.buf.len() {
                    return None;
                }
                let (tag, next) = read_varint(self.buf, self.cursor.outer)?;
                let (start, end) = field_range(self.buf, tag, next)?;
                self.cursor.outer = end;
                if tag == (self.occurrence_of << 3) | wire_types::LEN {
                    self.cursor.pos = start;
                    self.cursor.end = end;
                    break;
                }
            }
        }
    }
}

/// Implementation of `InstrumentationScopeView` backed by protobuf serialized
/// `InstrumentationScope` message
pub struct RawInstrumentationScope<'a> {
    bytes_parser: ProtoBytesParser<'a, InstrumentationScopeFieldOffsets>,
}

impl<'a> RawInstrumentationScope<'a> {
    /// create a new instance of `RawInstrumentationScope`
    #[inline]
    #[must_use]
    pub const fn new(bytes_parser: ProtoBytesParser<'a, InstrumentationScopeFieldOffsets>) -> Self {
        Self { bytes_parser }
    }
}

/// known field offsets for buffer containing InstrumentationScope message
pub struct InstrumentationScopeFieldOffsets {
    name: Cell<Option<(NonZeroUsize, NonZeroUsize)>>,
    version: Cell<Option<(NonZeroUsize, NonZeroUsize)>>,
    dropped_attributes_count: Cell<Option<(NonZeroUsize, NonZeroUsize)>>,
    first_attribute: Cell<Option<(NonZeroUsize, NonZeroUsize)>>,
}

impl FieldRanges for InstrumentationScopeFieldOffsets {
    fn new() -> Self {
        Self {
            name: Cell::new(None),
            version: Cell::new(None),
            dropped_attributes_count: Cell::new(None),
            first_attribute: Cell::new(None),
        }
    }

    #[inline]
    fn get_field_range(&self, field_num: u64) -> Option<(usize, usize)> {
        let range = match field_num {
            INSTRUMENTATION_SCOPE_NAME => self.name.get(),
            INSTRUMENTATION_SCOPE_VERSION => self.version.get(),
            INSTRUMENTATION_DROPPED_ATTRIBUTES_COUNT => self.dropped_attributes_count.get(),
            INSTRUMENTATION_SCOPE_ATTRIBUTES => self.first_attribute.get(),
            _ => None,
        };

        from_option_nonzero_range_to_primitive(range)
    }

    #[inline]
    fn set_field_range(&self, field_num: u64, wire_type: u64, start: usize, end: usize) {
        let range = match to_nonzero_range(start, end) {
            Some(range) => Some(range),
            None => return,
        };

        match field_num {
            INSTRUMENTATION_SCOPE_NAME if wire_type == wire_types::LEN => {
                self.name.set(range);
            }
            INSTRUMENTATION_SCOPE_VERSION if wire_type == wire_types::LEN => {
                self.version.set(range);
            }
            INSTRUMENTATION_DROPPED_ATTRIBUTES_COUNT if wire_type == wire_types::VARINT => {
                self.dropped_attributes_count.set(range);
            }
            INSTRUMENTATION_SCOPE_ATTRIBUTES
                if self.first_attribute.get().is_none() && wire_type == wire_types::LEN =>
            {
                self.first_attribute.set(range);
            }
            _ => {
                // ignore unknown field_num
            }
        }
    }
}

/* ----------------------------- ADAPTER ITERATORS ----------------------- */

/// Iterator of KeyValues - produces implementation of KeyValueView from the byte array which
/// contains a protobuf serialized repeated KeyValues
pub struct KeyValueIter<'a, T: FieldRanges> {
    bytes_parser: RepeatedFieldProtoBytesParser<'a, T>,
}

impl<'a, T> KeyValueIter<'a, T>
where
    T: FieldRanges,
{
    /// Create a new instance of `KeyValueIter`
    #[must_use]
    pub const fn new(bytes_parser: RepeatedFieldProtoBytesParser<'a, T>) -> Self {
        Self { bytes_parser }
    }
}

impl<'a, T> Iterator for KeyValueIter<'a, T>
where
    T: FieldRanges,
{
    type Item = RawKeyValue<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let slice = self.bytes_parser.next()?;
        Some(RawKeyValue::new(slice))
    }
}

/// Iterator of AnyValues - produces implementation of AnyValueView from the byte array which
/// contains a protobuf serialized repeated AnyValues
pub struct AnyValueIter<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for AnyValueIter<'a> {
    type Item = RawAnyValue<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.buf.len() {
            let (tag, next_pos) = read_varint(self.buf, self.pos)?;
            self.pos = next_pos;
            let field = tag >> 3;
            let wire_type = tag & 7;

            if field == ARRAY_VALUE_VALUES && wire_type == wire_types::LEN {
                let (slice, next_pos) = read_len_delim(self.buf, self.pos)?;
                self.pos = next_pos;

                return Some(RawAnyValue::new(slice));
            }
            // Step over any other field (unknown, or known with another wire
            // type), so its value is never read as field keys.
            let (_, end) = field_range(self.buf, tag, self.pos)?;
            self.pos = end;
        }

        None
    }
}

/* ----------------------------- TRAIT IMPLEMENTATIONS ------------------- */

impl AttributeView for RawKeyValue<'_> {
    type Val<'val>
        = RawAnyValue<'val>
    where
        Self: 'val;

    #[inline]
    fn key(&self) -> otel_arrow_dfe_pdata_views::views::common::Str<'_> {
        self.scan();
        from_option_nonzero_range_to_primitive(self.key_range.get())
            .and_then(|(start, end)| self.buf.get(start..end))
            // an empty key when none can be read
            .unwrap_or_default()
    }

    #[inline]
    fn value(&self) -> Option<Self::Val<'_>> {
        self.scan();
        let (start, end) = from_option_nonzero_range_to_primitive(self.value_range.get())?;
        if self.value_repeated.get() {
            return Some(RawAnyValue::merged(self.buf, KEY_VALUE_VALUE));
        }
        Some(RawAnyValue::new(self.buf.get(start..end)?))
    }
}

impl<'a> AnyValueView<'a> for RawAnyValue<'a> {
    type KeyValue = RawKeyValue<'a>;

    type KeyValueIter<'kv>
        = MergedMember<'a, KeyValueIter<'a, KeyValuesListFieldOffsets>>
    where
        Self: 'kv;

    type ArrayIter<'att>
        = MergedMember<'a, AnyValueIter<'a>>
    where
        Self: 'att;

    #[inline]
    fn value_type(&self) -> ValueType {
        match self.variant.get() {
            Some(variant_type) => variant_type,
            None => {
                // Every field is scanned: an unknown field may precede the
                // value, and a oneof set more than once reads as prost decodes
                // it. The last member wins, except that a message-typed member
                // (`array_value`, `kvlist_value`) following itself is merged
                // into one run whose repeated contents are concatenated. A
                // value with no member, or only unknown fields, is empty.
                let mut variant_type = ValueType::Empty;
                for field in self.fields {
                    let member = match (field.tag >> 3, field.tag & 7) {
                        (ANY_VALUE_STRING_VALUE, wire_types::LEN) => ValueType::String,
                        (ANY_VALUE_BOOL_VALUE, wire_types::VARINT) => ValueType::Bool,
                        (ANY_VALUE_INT_VALUE, wire_types::VARINT) => ValueType::Int64,
                        (ANY_VALUE_DOUBLE_VALUE, wire_types::FIXED64) => ValueType::Double,
                        (ANY_VALUE_ARRAY_VALUE, wire_types::LEN) => ValueType::Array,
                        (ANY_VALUE_KVLIST_VALUE, wire_types::LEN) => ValueType::KeyValueList,
                        (ANY_VALUE_BYTES_VALUE, wire_types::LEN) => ValueType::Bytes,
                        _ => continue,
                    };
                    let message_typed =
                        matches!(member, ValueType::Array | ValueType::KeyValueList);
                    if !(message_typed && member == variant_type) {
                        // A new run: remember where its first key is for a
                        // message-typed member, or the value itself.
                        variant_type = member;
                        self.value_at.set(Some(if message_typed {
                            field.at
                        } else {
                            Cursor {
                                pos: field.start,
                                end: field.end,
                                ..field.at
                            }
                        }));
                    }
                }

                self.variant.set(Some(variant_type));
                variant_type
            }
        }
    }

    #[inline]
    fn as_string(&self) -> Option<otel_arrow_dfe_pdata_views::views::common::Str<'_>> {
        self.scalar(ValueType::String)
    }

    #[inline]
    fn as_bool(&self) -> Option<bool> {
        // bools are encoded as varint where 1 == true and 0 == false
        let (val, _) = read_varint(self.scalar(ValueType::Bool)?, 0)?;
        Some(val == 1)
    }

    #[inline]
    fn as_bytes(&self) -> Option<&[u8]> {
        self.scalar(ValueType::Bytes)
    }

    #[inline]
    fn as_double(&self) -> Option<f64> {
        let byte_arr: [u8; 8] = self.scalar(ValueType::Double)?.try_into().ok()?;
        Some(f64::from_le_bytes(byte_arr))
    }

    #[inline]
    fn as_int64(&self) -> Option<i64> {
        let (val, _) = read_varint(self.scalar(ValueType::Int64)?, 0)?;
        Some(val as i64)
    }

    #[inline]
    fn as_array(&self) -> Option<Self::ArrayIter<'_>> {
        Some(MergedMember::new(
            self.run(ValueType::Array)?,
            ANY_VALUE_ARRAY_VALUE,
            open_array,
        ))
    }

    #[inline]
    fn as_kvlist(&self) -> Option<Self::KeyValueIter<'_>> {
        Some(MergedMember::new(
            self.run(ValueType::KeyValueList)?,
            ANY_VALUE_KVLIST_VALUE,
            open_kvlist,
        ))
    }
}

/// The elements of one serialized `ArrayValue`.
fn open_array(slice: &[u8]) -> AnyValueIter<'_> {
    AnyValueIter { buf: slice, pos: 0 }
}

/// The key-values of one serialized `KeyValueList`. Like every
/// `ProtoBytesParser`, the parser behind it allocates its shared state (one
/// `Rc`) per occurrence.
fn open_kvlist(slice: &[u8]) -> KeyValueIter<'_, KeyValuesListFieldOffsets> {
    KeyValueIter::new(RepeatedFieldProtoBytesParser::from_byte_parser(
        &ProtoBytesParser::new(slice),
        KEY_VALUE_LIST_VALUES,
        wire_types::LEN,
    ))
}

/// Iterator over the repeated contents of every occurrence of one
/// message-typed `AnyValue` member (`array_value` or `kvlist_value`) in a run,
/// in order: what prost yields when it merges a member that follows itself.
///
/// `fields` starts at the key of the run's first occurrence; every later field
/// in it is either another occurrence of the same member or a field that is
/// not a member (unknown, or a member under another wire type), because any
/// other member would have started a new run. Occurrences are found by
/// scanning `fields` as the iteration reaches them, so no list of occurrences
/// is kept. The keys and length prefixes of the run are read a second time
/// here, after `value_type` has scanned the whole value once; each kvlist
/// occurrence costs one `Rc` allocation (see [`open_kvlist`]). A repeated
/// member is rare, and the common single occurrence costs one extra key
/// read.
pub struct MergedMember<'a, I> {
    fields: Fields<'a>,
    member: u64,
    current: Option<I>,
    open: fn(&'a [u8]) -> I,
}

impl<'a, I> MergedMember<'a, I> {
    const fn new(fields: Fields<'a>, member: u64, open: fn(&'a [u8]) -> I) -> Self {
        Self {
            fields,
            member,
            current: None,
            open,
        }
    }
}

impl<I: Iterator> Iterator for MergedMember<'_, I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.current.as_mut().and_then(Iterator::next) {
                return Some(item);
            }
            // The current occurrence is exhausted: open the next one.
            loop {
                let Some(field) = self.fields.next() else {
                    self.current = None;
                    return None;
                };
                if field.tag == (self.member << 3) | wire_types::LEN {
                    self.current = Some((self.open)(&self.fields.buf[field.start..field.end]));
                    break;
                }
            }
        }
    }
}

/// known offset of KeyValuesList
pub struct KeyValuesListFieldOffsets {
    first_offset: Cell<Option<(NonZeroUsize, NonZeroUsize)>>,
}

impl FieldRanges for KeyValuesListFieldOffsets {
    fn new() -> Self {
        Self {
            first_offset: Cell::new(None),
        }
    }

    #[inline]
    fn get_field_range(&self, _field_tag: u64) -> Option<(usize, usize)> {
        self.first_offset
            .get()
            .map(|(start, end)| (start.get(), end.get()))
    }

    #[inline]
    fn set_field_range(&self, field_tag: u64, wire_type: u64, start: usize, end: usize) {
        let range = match to_nonzero_range(start, end) {
            Some(range) => Some(range),
            None => return,
        };
        if field_tag == KEY_VALUE_LIST_VALUES
            && wire_type == wire_types::LEN
            && self.first_offset.get().is_none()
        {
            self.first_offset.set(range);
        }
    }
}
impl InstrumentationScopeView for RawInstrumentationScope<'_> {
    type Attribute<'att>
        = RawKeyValue<'att>
    where
        Self: 'att;

    type AttributeIter<'att>
        = KeyValueIter<'att, InstrumentationScopeFieldOffsets>
    where
        Self: 'att;

    #[inline]
    fn name(&self) -> Option<otel_arrow_dfe_pdata_views::views::common::Str<'_>> {
        self.bytes_parser
            .advance_to_find_field(INSTRUMENTATION_SCOPE_NAME)
    }

    #[inline]
    fn version(&self) -> Option<otel_arrow_dfe_pdata_views::views::common::Str<'_>> {
        self.bytes_parser
            .advance_to_find_field(INSTRUMENTATION_SCOPE_VERSION)
    }

    #[inline]
    fn dropped_attributes_count(&self) -> u32 {
        let slice = self
            .bytes_parser
            .advance_to_find_field(INSTRUMENTATION_DROPPED_ATTRIBUTES_COUNT);
        read_dropped_count(slice)
    }

    #[inline]
    fn attributes(&self) -> Self::AttributeIter<'_> {
        KeyValueIter::new(RepeatedFieldProtoBytesParser::from_byte_parser(
            &self.bytes_parser,
            INSTRUMENTATION_SCOPE_ATTRIBUTES,
            wire_types::LEN,
        ))
    }
}

#[cfg(test)]
mod test {
    use otel_arrow_dfe_config::ConversionOptions;
    use otel_arrow_dfe_pdata_views::views::common::AttributeView;

    use crate::{
        otlp::{BoundedBuf, ProtoBuffer},
        proto::consts::{
            field_num::common::{KEY_VALUE_KEY, KEY_VALUE_VALUE},
            wire_types,
        },
        views::otlp::bytes::common::RawKeyValue,
    };

    #[test]
    fn test_kv_handles_invalid_key_range() {
        let mut protobuf = ProtoBuffer::new(ConversionOptions::default());
        protobuf
            .encode_field_tag(KEY_VALUE_KEY, wire_types::LEN)
            .unwrap();
        // length is for some reason invalid - someone gave us a bad proto bytes
        protobuf.encode_varint(5).unwrap();
        protobuf.extend_from_slice("aaa".as_bytes()).unwrap();

        let kv_view = RawKeyValue::new(protobuf.as_slice());
        let key = kv_view.key();
        assert!(key.is_empty());
    }

    #[test]
    fn test_kv_handles_invalid_value_range() {
        let mut protobuf = ProtoBuffer::new(ConversionOptions::default());
        protobuf.encode_string(KEY_VALUE_KEY, "my-key").unwrap();

        protobuf
            .encode_field_tag(KEY_VALUE_VALUE, wire_types::LEN)
            .unwrap();
        // length is for some reason invalid - someone gave us bad proto bytes
        protobuf.encode_varint(4).unwrap();
        protobuf.extend_from_slice(&[0]).unwrap();

        let kv_view = RawKeyValue::new(protobuf.as_slice());
        let value = kv_view.value();
        assert!(value.is_none());
    }

    /// Read a serialized `AnyValue` through the byte view into the prost
    /// type, recursively, so the view can be compared with prost's decoding.
    fn read_through_view(
        value: &super::RawAnyValue<'_>,
    ) -> crate::proto::opentelemetry::common::v1::AnyValue {
        use crate::proto::opentelemetry::common::v1::{
            AnyValue, ArrayValue, KeyValue, KeyValueList, any_value::Value,
        };
        use otel_arrow_dfe_pdata_views::views::common::{AnyValueView, ValueType};
        let string = |bytes: &[u8]| String::from_utf8(bytes.to_vec()).expect("utf-8");
        let value = match value.value_type() {
            ValueType::Empty => None,
            ValueType::String => Some(Value::StringValue(string(value.as_string().unwrap()))),
            ValueType::Bool => Some(Value::BoolValue(value.as_bool().unwrap())),
            ValueType::Int64 => Some(Value::IntValue(value.as_int64().unwrap())),
            ValueType::Double => Some(Value::DoubleValue(value.as_double().unwrap())),
            ValueType::Bytes => Some(Value::BytesValue(value.as_bytes().unwrap().to_vec())),
            ValueType::Array => Some(Value::ArrayValue(ArrayValue {
                values: value
                    .as_array()
                    .unwrap()
                    .map(|v| read_through_view(&v))
                    .collect(),
            })),
            ValueType::KeyValueList => Some(Value::KvlistValue(KeyValueList {
                values: value
                    .as_kvlist()
                    .unwrap()
                    .map(|kv| KeyValue {
                        key: string(kv.key()),
                        value: kv.value().map(|v| read_through_view(&v)),
                    })
                    .collect(),
            })),
        };
        AnyValue { value }
    }

    /// Scenario: `AnyValue` bodies that set the oneof more than once: a
    /// non-empty `array_value` then an empty one, two non-empty
    /// `kvlist_value`s, an `array_value` then a `string_value`, an array, a
    /// string and another array, two arrays with an unknown field between
    /// them, a string then an array, and two `string_value`s.
    /// Guarantees: the byte view reads each exactly as prost decodes it: a
    /// message-typed member that follows itself is merged (its elements
    /// concatenated, so a trailing empty occurrence loses nothing), any other
    /// later member wins, and an unknown field does not break a run.
    #[test]
    fn repeated_any_value_members_read_as_prost_decodes_them() {
        use prost::Message as _;
        let len_field = |field: u32, payload: &[u8]| {
            let mut out = Vec::new();
            prost::encoding::encode_key(
                field,
                prost::encoding::WireType::LengthDelimited,
                &mut out,
            );
            prost::encoding::encode_varint(payload.len() as u64, &mut out);
            out.extend_from_slice(payload);
            out
        };
        let string = |text: &[u8]| len_field(1, text);
        let array = |elements: &[&[u8]]| {
            let content: Vec<u8> = elements.iter().flat_map(|e| len_field(1, e)).collect();
            len_field(5, &content)
        };
        let kvlist = |pairs: &[(&[u8], &[u8])]| {
            let content: Vec<u8> = pairs
                .iter()
                .flat_map(|(k, v)| len_field(1, &[len_field(1, k), len_field(2, v)].concat()))
                .collect();
            len_field(6, &content)
        };
        let unknown = [0xf8, 0x01, 0x07];
        let a = string(b"a");
        let b = string(b"b");
        let c = string(b"c");
        let bodies: Vec<(&str, Vec<u8>, usize)> = vec![
            (
                "array then empty array",
                [array(&[&a, &b]), array(&[])].concat(),
                2,
            ),
            (
                "two kvlists",
                [kvlist(&[(b"k1", &a)]), kvlist(&[(b"k2", &b), (b"k3", &c)])].concat(),
                3,
            ),
            (
                "array then string",
                [array(&[&a]), string(b"s")].concat(),
                0,
            ),
            (
                "array, string, array",
                [array(&[&a]), string(b"s"), array(&[&b, &c])].concat(),
                2,
            ),
            (
                "arrays around an unknown field",
                [array(&[&a]), unknown.to_vec(), array(&[&b])].concat(),
                2,
            ),
            (
                "string then array",
                [string(b"s"), array(&[&a])].concat(),
                1,
            ),
            ("two strings", [string(b"x"), string(b"y")].concat(), 0),
        ];
        for (name, body, elements) in bodies {
            let prost_value = crate::proto::opentelemetry::common::v1::AnyValue::decode(&body[..])
                .expect("prost decodes it");
            let view_value = read_through_view(&super::RawAnyValue::new(&body));
            assert_eq!(view_value, prost_value, "{name}");
            let count = match &prost_value.value {
                Some(crate::proto::opentelemetry::common::v1::any_value::Value::ArrayValue(v)) => {
                    v.values.len()
                }
                Some(crate::proto::opentelemetry::common::v1::any_value::Value::KvlistValue(v)) => {
                    v.values.len()
                }
                _ => 0,
            };
            assert_eq!(count, elements, "{name}");
        }
    }

    /// Scenario: `KeyValue` bodies that repeat `value`: an empty value then a kvlist, a kvlist
    /// split over two values, an array value then a string value, a string value then an array
    /// split over two values with the key between them, and a value whose only member is
    /// unknown then a bool.
    /// Guarantees: the byte view reads the merged value in place exactly as prost decodes the
    /// `KeyValue`: message-typed members merge across occurrences and the last member wins.
    #[test]
    fn a_repeated_key_value_value_reads_as_prost_decodes_it() {
        use crate::proto::opentelemetry::common::v1::KeyValue;
        use prost::Message as _;
        let len_field = |field: u8, payload: &[u8]| {
            let mut out = vec![(field << 3) | 2, payload.len() as u8];
            out.extend_from_slice(payload);
            out
        };
        let string = |text: &[u8]| len_field(1, text);
        let array = |elements: &[&[u8]]| {
            let content: Vec<u8> = elements.iter().flat_map(|e| len_field(1, e)).collect();
            len_field(5, &content)
        };
        let kvlist = |pairs: &[(&[u8], &[u8])]| {
            let content: Vec<u8> = pairs
                .iter()
                .flat_map(|(k, v)| len_field(1, &[len_field(1, k), len_field(2, v)].concat()))
                .collect();
            len_field(6, &content)
        };
        let value = |any_value: &[u8]| len_field(2, any_value);
        let key = len_field(1, b"key");
        let a = string(b"a");
        let b = string(b"b");
        let bodies: Vec<(&str, Vec<u8>)> = vec![
            (
                "empty then kvlist",
                [key.clone(), value(&[]), value(&kvlist(&[(b"k", &a)]))].concat(),
            ),
            (
                "kvlist over two values",
                [
                    key.clone(),
                    value(&kvlist(&[(b"k1", &a)])),
                    value(&kvlist(&[(b"k2", &b)])),
                ]
                .concat(),
            ),
            (
                "array then string",
                [key.clone(), value(&array(&[&a])), value(&string(b"s"))].concat(),
            ),
            (
                "string, then an array over two values around the key",
                [
                    value(&[string(b"s"), array(&[&a])].concat()),
                    key.clone(),
                    value(&array(&[&b])),
                ]
                .concat(),
            ),
            (
                "unknown member then bool",
                [
                    key.clone(),
                    value(&[0xf8, 0x01, 0x07]),
                    value(&[0x10, 0x01]),
                ]
                .concat(),
            ),
        ];
        for (name, body) in bodies {
            let expected = KeyValue::decode(&body[..]).expect("prost decodes it");
            let view = RawKeyValue::new(&body);
            let actual = KeyValue {
                key: String::from_utf8(view.key().to_vec()).expect("utf-8"),
                value: view.value().map(|v| read_through_view(&v)),
            };
            assert_eq!(actual, expected, "{name}");
        }
    }
}
