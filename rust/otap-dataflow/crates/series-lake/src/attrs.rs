// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Reads OTAP attribute record batches into per-parent, sorted attribute lists
//! that borrow their keys, strings and bytes from the batch.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray, UInt8Array, UInt16Array,
    UInt32Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Float64Type, Int64Type, UInt8Type, UInt16Type};
use arrow::record_batch::RecordBatch;
use otel_arrow_dfe_pdata::arrays::{MaybeDictArrayAccessor, NullableArrayAccessor};
use otel_arrow_dfe_pdata::otlp::attributes::AttributeValueType;
use otel_arrow_dfe_pdata::schema::consts::{
    ATTRIBUTE_BOOL, ATTRIBUTE_BYTES, ATTRIBUTE_DOUBLE, ATTRIBUTE_INT, ATTRIBUTE_KEY, ATTRIBUTE_SER,
    ATTRIBUTE_STR, ATTRIBUTE_TYPE, PARENT_ID,
};

use crate::error::{Error, Result};
use crate::extract::Budget;
use crate::value::{
    BUFFER_HEADER_BYTES, DecodeLimits, VALUE_NODE_BYTES, Value, decode_cbor_reserving,
    rendered_bytes_len, rendered_len, value_bytes, write_bytes_v1, write_v1,
};

/// One attribute value, viewed without copying it: a string or bytes value
/// borrowed from wherever it is held, anything else as its decoded tree.
///
/// Both an attribute table's borrowed entries and an owned `(String, Value)`
/// list read through this view, so equality, hashing, rendering and the
/// decoded footprint are defined once for both. A `Tree` never holds a
/// [`Value::Str`] or [`Value::Bytes`]: [`ValueRef::of`] turns those into the
/// `Str` and `Bytes` variants, so one value has exactly one view.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ValueRef<'a> {
    /// UTF-8 string.
    Str(&'a str),
    /// Raw bytes.
    Bytes(&'a [u8]),
    /// Any other value: null, a scalar, an array or a key/value list.
    Tree(&'a Value),
}

impl<'a> ValueRef<'a> {
    /// The view of an owned value.
    #[must_use]
    pub fn of(v: &'a Value) -> Self {
        match v {
            Value::Str(s) => Self::Str(s),
            Value::Bytes(b) => Self::Bytes(b),
            other => Self::Tree(other),
        }
    }

    /// The value, owned.
    #[must_use]
    pub fn to_value(self) -> Value {
        match self {
            Self::Str(s) => Value::Str(s.to_owned()),
            Self::Bytes(b) => Value::Bytes(b.to_vec()),
            Self::Tree(v) => v.clone(),
        }
    }

    /// [`value_bytes`] of the owned value, without owning it.
    #[must_use]
    pub fn bytes(self) -> usize {
        match self {
            Self::Str(s) => VALUE_NODE_BYTES + s.len(),
            Self::Bytes(b) => VALUE_NODE_BYTES + b.len(),
            Self::Tree(v) => value_bytes(v),
        }
    }

    /// Whether the value is [`Value::Null`].
    #[must_use]
    pub fn is_null(self) -> bool {
        matches!(self, Self::Tree(Value::Null))
    }

    /// [`rendered_len`] of the owned value: the length of its attribute-map
    /// rendering, `0` for null.
    #[must_use]
    pub fn rendered_len(self) -> usize {
        match self {
            Self::Str(s) => s.len(),
            Self::Bytes(b) => rendered_bytes_len(b.len()),
            Self::Tree(v) => rendered_len(v),
        }
    }

    /// Write the attribute-map rendering of a value that is not null to
    /// `out`: a string as it is, anything else as `render_v1`
    /// ([`crate::value::map_string`]); fails only if `out` does.
    pub fn write_rendered<W: std::fmt::Write>(self, out: &mut W) -> std::fmt::Result {
        match self {
            Self::Str(s) => out.write_str(s),
            Self::Bytes(b) => write_bytes_v1(b, out),
            Self::Tree(v) => write_v1(v, out),
        }
    }

    /// [`crate::value::map_string`] of the owned value, borrowing a string
    /// instead of copying it.
    #[must_use]
    pub fn map_str(self) -> Option<Cow<'a, str>> {
        match self {
            Self::Str(s) => Some(Cow::Borrowed(s)),
            Self::Tree(Value::Null) => None,
            other => {
                let mut out = String::with_capacity(other.rendered_len());
                // Writing to a String cannot fail.
                let _ = other.write_rendered(&mut out);
                Some(Cow::Owned(out))
            }
        }
    }
}

/// One key/value attribute, read through [`ValueRef`] whether it is owned or
/// borrowed from an attribute batch.
pub(crate) trait Attr {
    /// The key.
    fn key(&self) -> &str;
    /// The value.
    fn value(&self) -> ValueRef<'_>;

    /// [`crate::value::entry_bytes`] of the owned entry.
    fn entry_bytes(&self) -> usize {
        BUFFER_HEADER_BYTES + self.key().len() + self.value().bytes()
    }

    /// The entry, owned.
    fn to_owned_entry(&self) -> (String, Value) {
        (self.key().to_owned(), self.value().to_value())
    }
}

impl<T: Attr + ?Sized> Attr for &T {
    fn key(&self) -> &str {
        (**self).key()
    }

    fn value(&self) -> ValueRef<'_> {
        (**self).value()
    }
}

impl Attr for (String, Value) {
    fn key(&self) -> &str {
        &self.0
    }

    fn value(&self) -> ValueRef<'_> {
        ValueRef::of(&self.1)
    }
}

/// [`crate::value::kv_bytes`] of a list, owned or borrowed.
pub(crate) fn attrs_bytes<E: Attr>(list: &[E]) -> usize {
    list.iter().map(Attr::entry_bytes).sum()
}

/// A value as an attribute table holds it: a string or bytes value borrowed
/// from the batch, anything else owned.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AttrValue<'c> {
    /// A string borrowed from the batch.
    Str(&'c str),
    /// Bytes borrowed from the batch.
    Bytes(&'c [u8]),
    /// Null, a scalar, or a decoded CBOR map or slice.
    Owned(Value),
}

impl AttrValue<'_> {
    /// The value's view.
    #[must_use]
    pub fn view(&self) -> ValueRef<'_> {
        match self {
            Self::Str(s) => ValueRef::Str(s),
            Self::Bytes(b) => ValueRef::Bytes(b),
            Self::Owned(v) => ValueRef::of(v),
        }
    }
}

/// One attribute of an [`AttrTable`], with the parent it belongs to.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AttrEntry<'c> {
    parent: u32,
    key: &'c str,
    value: AttrValue<'c>,
}

impl Attr for AttrEntry<'_> {
    fn key(&self) -> &str {
        self.key
    }

    fn value(&self) -> ValueRef<'_> {
        self.value.view()
    }
}

/// The columns of one OTAP attribute batch, read or cast once, which an
/// [`AttrTable`] borrows its keys, strings and bytes from.
pub(crate) struct AttrColumns {
    parent_ids: Vec<u32>,
    keys: ArrayRef,
    values: AnyValueArrays,
}

impl AttrColumns {
    /// Read the columns of an `attributes_16` or `attributes_32` batch.
    ///
    /// # Errors
    /// Refuses the batch when `parent_id` is missing or null, or a required
    /// column is absent or of the wrong type.
    pub fn new(batch: &RecordBatch) -> Result<Self> {
        let parent_ids = read_parent_ids(batch)?;
        let keys = required(batch, ATTRIBUTE_KEY, &DataType::Utf8)?;
        let values = AnyValueArrays::new(&|n| batch.column_by_name(n).cloned())?;
        Ok(Self {
            parent_ids,
            keys,
            values,
        })
    }
}

/// Where each parent's entries start in an [`AttrTable`]'s sorted entries.
#[derive(Debug, Default)]
enum ParentIndex {
    /// Entry `starts[p]..starts[p + 1]` belong to parent `p`; used when the
    /// largest parent id is small against the entry count, as OTAP's dense
    /// ids are.
    Dense(Vec<usize>),
    /// Parent ranges found by binary search.
    #[default]
    Search,
}

/// Most index slots a dense [`ParentIndex`] may have per entry, beyond
/// [`DENSE_INDEX_SLACK`]; sparser ids are searched.
const DENSE_INDEX_SLOTS_PER_ENTRY: usize = 4;

/// Index slots a dense [`ParentIndex`] may always have.
const DENSE_INDEX_SLACK: usize = 1024;

impl ParentIndex {
    /// The index of entries sorted by parent.
    fn new(entries: &[AttrEntry<'_>]) -> Self {
        let Some(last) = entries.last() else {
            return Self::Search;
        };
        let slots = last.parent as usize + 2;
        if slots > entries.len() * DENSE_INDEX_SLOTS_PER_ENTRY + DENSE_INDEX_SLACK {
            return Self::Search;
        }
        let mut starts = vec![0_usize; slots];
        for entry in entries {
            starts[entry.parent as usize + 1] += 1;
        }
        for slot in 1..slots {
            starts[slot] += starts[slot - 1];
        }
        Self::Dense(starts)
    }

    /// The range of `entries` that belongs to `parent`.
    fn range(&self, entries: &[AttrEntry<'_>], parent: u32) -> std::ops::Range<usize> {
        match self {
            Self::Dense(starts) => {
                let p = parent as usize;
                match (starts.get(p), starts.get(p + 1)) {
                    (Some(&start), Some(&end)) => start..end,
                    _ => 0..0,
                }
            }
            Self::Search => {
                let start = entries.partition_point(|e| e.parent < parent);
                let end = start + entries[start..].partition_point(|e| e.parent == parent);
                start..end
            }
        }
    }
}

/// Attributes of one OTAP attribute batch, grouped by parent id and sorted by
/// key within each parent, borrowing from the batch's [`AttrColumns`].
#[derive(Debug, Default)]
pub(crate) struct AttrTable<'c> {
    /// Every attribute of the batch, ordered by parent, then key bytes.
    entries: Vec<AttrEntry<'c>>,
    index: ParentIndex,
    /// Decoded bytes the table charged to its request's budget.
    bytes: usize,
}

/// Fetch `name` from `batch` as `to`, or `None` when the column is absent.
///
/// See [`readable`] for which columns are cast and which are kept as they
/// are.
pub(crate) fn plain(batch: &RecordBatch, name: &str, to: &DataType) -> Result<Option<ArrayRef>> {
    batch
        .column_by_name(name)
        .map(|col| readable(Arc::clone(col), to))
        .transpose()
}

/// `col` in a form the cell readers of this crate can read as `to`.
///
/// A column that already is `to` is returned as it is, and so is a
/// dictionary with `u8` or `u16` keys (the key types OTAP uses) whose values
/// are `to`, for the string, binary, fixed-size binary and 64-bit integer
/// types: [`str_ref`], [`bytes_cell`] and [`prim_at`] read through the
/// dictionary. Anything else is cast.
///
/// A cast would expand the dictionary before any budget applies, one copy of a
/// large value per row; reading through it charges each cell as it is read.
pub(crate) fn readable(col: ArrayRef, to: &DataType) -> Result<ArrayRef> {
    let through = match col.data_type() {
        t if t == to => true,
        DataType::Dictionary(key, value) => {
            matches!(**key, DataType::UInt8 | DataType::UInt16)
                && **value == *to
                && matches!(
                    to,
                    DataType::Utf8
                        | DataType::Binary
                        | DataType::FixedSizeBinary(_)
                        | DataType::Int64
                )
        }
        _ => false,
    };
    if through {
        Ok(col)
    } else {
        Ok(cast(&col, to)?)
    }
}

/// The string at `row` of a [`readable`] `Utf8` column, borrowed from it and
/// read through a dictionary; `None` when the cell or its key is null.
pub(crate) fn str_ref(a: &ArrayRef, row: usize) -> Option<&str> {
    if let Some(strings) = a.as_string_opt::<i32>() {
        return strings.is_valid(row).then(|| strings.value(row));
    }
    let (values, key) = dictionary_at(a, row)?;
    Some(values.as_string_opt::<i32>()?.value(key?))
}

/// Apply `f` to the bytes at `row` of a [`readable`] binary or fixed-size
/// binary column, reading through a dictionary without copying; `None` when
/// the cell is null.
pub(crate) fn bytes_cell<R>(a: &ArrayRef, row: usize, f: impl FnOnce(&[u8]) -> R) -> Option<R> {
    bytes_ref(a, row).map(f)
}

/// The bytes at `row` of a [`readable`] binary or fixed-size binary column,
/// borrowed from it and read through a dictionary; `None` when the cell or
/// its key is null.
pub(crate) fn bytes_ref(a: &ArrayRef, row: usize) -> Option<&[u8]> {
    fn plain(a: &dyn Array, row: usize) -> Option<Option<&[u8]>> {
        if let Some(bytes) = a.as_binary_opt::<i32>() {
            return Some(bytes.is_valid(row).then(|| bytes.value(row)));
        }
        a.as_fixed_size_binary_opt()
            .map(|bytes| bytes.is_valid(row).then(|| bytes.value(row)))
    }
    if let Some(cell) = plain(a.as_ref(), row) {
        return cell;
    }
    let (values, key) = dictionary_at(a, row)?;
    let key = key?;
    if let Some(bytes) = values.as_binary_opt::<i32>() {
        return Some(bytes.value(key));
    }
    values
        .as_fixed_size_binary_opt()
        .map(|bytes| bytes.value(key))
}

/// The value at `row` of a [`readable`] primitive column, reading through a
/// dictionary; `None` when the column is absent or the cell is null.
pub(crate) fn prim_at<T: ArrowPrimitiveType>(
    a: &Option<ArrayRef>,
    row: usize,
) -> Option<T::Native> {
    let a = a.as_ref()?;
    if let Some(values) = a.as_primitive_opt::<T>() {
        return values.is_valid(row).then(|| values.value(row));
    }
    MaybeDictArrayAccessor::<PrimitiveArray<T>>::try_new(a)
        .ok()?
        .value_at(row)
}

/// The value at `row` of a [`readable`] `Boolean` column; `None` when the
/// column is absent or the cell is null.
pub(crate) fn bool_at(a: &Option<ArrayRef>, row: usize) -> Option<bool> {
    let a = a.as_ref()?.as_boolean_opt()?;
    a.is_valid(row).then(|| a.value(row))
}

/// Refuse one cell whose payload alone is larger than a row may be.
///
/// Checked on the borrowed cell, before it is copied: the row that would hold
/// it is refused as too large in any case, and a dictionary value referenced
/// by many rows must not be copied once per row first.
fn cell_fits(len: usize, limits: DecodeLimits) -> Result<()> {
    if len > limits.max_cell_bytes {
        return Err(Error::too_large(
            crate::error::SizeBudget::Cell,
            len,
            limits.max_cell_bytes,
        ));
    }
    Ok(())
}

/// The dictionary values of a `u8`- or `u16`-keyed column and its key at
/// `row` (`None` for a null key), or `None` for a plain column.
fn dictionary_at(a: &ArrayRef, row: usize) -> Option<(&ArrayRef, Option<usize>)> {
    if let Some(d) = a.as_dictionary_opt::<UInt8Type>() {
        return Some((d.values(), d.key(row)));
    }
    a.as_dictionary_opt::<UInt16Type>()
        .map(|d| (d.values(), d.key(row)))
}

/// Rows referencing each of the `values` entries of a dictionary column.
fn reference_counts(a: &ArrayRef, values: usize) -> Vec<u32> {
    let mut refs = vec![0_u32; values];
    for row in 0..a.len() {
        if let Some((_, Some(key))) = dictionary_at(a, row)
            && let Some(count) = refs.get_mut(key)
        {
            *count += 1;
        }
    }
    refs
}

/// The seven `AnyValue` columns (type, str, int, double, bool, bytes, ser) of
/// an attribute batch or a log body struct, cast to plain types.
struct AnyValueArrays {
    types: UInt8Array,
    strs: Option<ArrayRef>,
    ints: Option<ArrayRef>,
    doubles: Option<ArrayRef>,
    bools: Option<ArrayRef>,
    bytes: Option<ArrayRef>,
    sers: Option<ArrayRef>,
}

/// The decoded `ser` dictionary values of one [`AnyValueArrays`] that a later
/// row still references.
#[derive(Default)]
struct SerCache {
    /// Decoded values by dictionary index, with the bytes each charged to the
    /// request budget.
    decoded: HashMap<usize, (Value, usize)>,
    /// Rows left to read per `ser` dictionary index, counted on first use.
    refs: Option<Vec<u32>>,
    /// Bytes of `decoded`, charged to the request budget until they leave it.
    decoded_bytes: usize,
}

impl SerCache {
    /// Give back the budget charged for the decoded dictionary values.
    fn release(self, budget: &mut Budget) {
        budget.uncharge(self.decoded_bytes);
    }
}

/// The `AnyValue` columns of a log body struct and the `ser` values decoded
/// from them so far.
pub(crate) struct AnyValueColumns {
    arrays: AnyValueArrays,
    cache: SerCache,
}

impl AnyValueArrays {
    /// Build from a column lookup (a `RecordBatch` or a `StructArray`).
    ///
    /// Keeps its own cast helper because it takes a column *lookup*, not a
    /// `RecordBatch`; [`plain`] stays the single batch-column helper.
    fn new(get: &dyn Fn(&str) -> Option<ArrayRef>) -> Result<Self> {
        let cast_opt = |name: &str, to: &DataType| -> Result<Option<ArrayRef>> {
            get(name).map(|c| readable(c, to)).transpose()
        };
        Ok(Self {
            types: cast_opt(ATTRIBUTE_TYPE, &DataType::UInt8)?
                .ok_or_else(|| Error::invalid("missing type column"))?
                .as_primitive_opt::<UInt8Type>()
                .cloned()
                .ok_or_else(|| Error::invalid("type column is not u8"))?,
            strs: cast_opt(ATTRIBUTE_STR, &DataType::Utf8)?,
            ints: cast_opt(ATTRIBUTE_INT, &DataType::Int64)?,
            doubles: cast_opt(ATTRIBUTE_DOUBLE, &DataType::Float64)?,
            bools: cast_opt(ATTRIBUTE_BOOL, &DataType::Boolean)?,
            bytes: cast_opt(ATTRIBUTE_BYTES, &DataType::Binary)?,
            sers: cast_opt(ATTRIBUTE_SER, &DataType::Binary)?,
        })
    }

    /// Typed value at a row, strings and bytes borrowed from the columns.
    ///
    /// The type tag decides the variant, so an absent value column, or a null
    /// cell in it, is the type's default, not null: pdata's OTAP encoder omits
    /// a column whose every entry is the default, and a series identity must
    /// not depend on batching (FORMAT.md section 1). Map and slice values are
    /// the exception: even an empty map is a non-empty CBOR payload, so an
    /// absent or null `ser` cell under those tags is malformed.
    ///
    /// A string, bytes or CBOR cell longer than `limits.max_cell_bytes` is
    /// refused as too large before it is decoded. The returned value's
    /// [`value_bytes`] are charged to `budget`, each part before a decoded
    /// part is allocated, and the caller owns that charge: a borrowed string
    /// or bytes value is charged what its owned copy would hold. A
    /// dictionary-encoded `ser` value is decoded once and kept in `cache`,
    /// charged, until the last row that references it takes it; the rows
    /// before get clones.
    ///
    /// # Errors
    /// Returns [`Error::Refused`] for an unknown type tag, for a map or slice
    /// whose `ser` payload is missing, for a malformed CBOR payload, for an
    /// oversized cell and for a value past the budget; the budget is then
    /// back where it was.
    fn value_at(
        &self,
        row: usize,
        limits: DecodeLimits,
        cache: &mut SerCache,
        budget: &mut Budget,
    ) -> Result<AttrValue<'_>> {
        let mark = budget.mark();
        let value = self.read_value(row, limits, cache, budget);
        if value.is_err() {
            budget.rollback(mark);
        }
        value
    }

    fn read_value(
        &self,
        row: usize,
        limits: DecodeLimits,
        cache: &mut SerCache,
        budget: &mut Budget,
    ) -> Result<AttrValue<'_>> {
        budget.charge_decoded(VALUE_NODE_BYTES)?;
        let Some(ty) = self.types.value_at(row) else {
            return Ok(AttrValue::Owned(Value::Null));
        };
        let ty = AttributeValueType::try_from(ty)
            .map_err(|_| Error::invalid(format!("attribute type {ty}")))?;
        Ok(match ty {
            AttributeValueType::Empty => AttrValue::Owned(Value::Null),
            AttributeValueType::Str => {
                let s = self
                    .strs
                    .as_ref()
                    .and_then(|a| str_ref(a, row))
                    .unwrap_or_default();
                cell_fits(s.len(), limits)?;
                budget.charge_decoded(s.len())?;
                AttrValue::Str(s)
            }
            AttributeValueType::Int => AttrValue::Owned(Value::Int(
                prim_at::<Int64Type>(&self.ints, row).unwrap_or(0),
            )),
            AttributeValueType::Double => AttrValue::Owned(Value::Double(
                prim_at::<Float64Type>(&self.doubles, row).unwrap_or(0.0),
            )),
            AttributeValueType::Bool => {
                AttrValue::Owned(Value::Bool(bool_at(&self.bools, row).unwrap_or(false)))
            }
            AttributeValueType::Bytes => {
                let b = self
                    .bytes
                    .as_ref()
                    .and_then(|a| bytes_ref(a, row))
                    .unwrap_or_default();
                cell_fits(b.len(), limits)?;
                budget.charge_decoded(b.len())?;
                AttrValue::Bytes(b)
            }
            AttributeValueType::Map | AttributeValueType::Slice => {
                AttrValue::Owned(self.ser_at(row, limits, cache, budget)?.ok_or_else(|| {
                    Error::invalid("map or slice attribute without a ser payload")
                })?)
            }
        })
    }

    /// The decoded `ser` cell at `row`, `None` when it is absent or null; its
    /// content below the root node is charged to `budget`.
    fn ser_at(
        &self,
        row: usize,
        limits: DecodeLimits,
        cache: &mut SerCache,
        budget: &mut Budget,
    ) -> Result<Option<Value>> {
        let Some(sers) = &self.sers else {
            return Ok(None);
        };
        let Some((values, key)) = dictionary_at(sers, row) else {
            return decode_charged(sers, row, limits, budget);
        };
        let Some(key) = key else {
            return Ok(None);
        };
        let refs = cache
            .refs
            .get_or_insert_with(|| reference_counts(sers, values.len()));
        let left = refs
            .get_mut(key)
            .ok_or_else(|| Error::invalid("ser dictionary key out of range"))?;
        *left = left.saturating_sub(1);
        let last = *left == 0;
        if last {
            // The kept copy's content charge passes to the row; only its own
            // node is given back.
            if let Some((value, bytes)) = cache.decoded.remove(&key) {
                budget.uncharge(VALUE_NODE_BYTES);
                cache.decoded_bytes -= bytes;
                return Ok(Some(value));
            }
        } else if let Some((value, bytes)) = cache.decoded.get(&key) {
            budget.charge_decoded(bytes - VALUE_NODE_BYTES)?;
            return Ok(Some(value.clone()));
        }
        let Some(value) = decode_charged(values, key, limits, budget)? else {
            return Ok(None);
        };
        if !last {
            let bytes = value_bytes(&value);
            budget.charge_decoded(bytes)?;
            cache.decoded_bytes += bytes;
            let _ = cache.decoded.insert(key, (value.clone(), bytes));
        }
        Ok(Some(value))
    }
}

impl AnyValueColumns {
    /// Build from a column lookup (a `RecordBatch` or a `StructArray`).
    pub(crate) fn new(get: &dyn Fn(&str) -> Option<ArrayRef>) -> Result<Self> {
        Ok(Self {
            arrays: AnyValueArrays::new(get)?,
            cache: SerCache::default(),
        })
    }

    /// Give back the budget charged for the decoded dictionary values.
    pub(crate) fn release(self, budget: &mut Budget) {
        self.cache.release(budget);
    }

    /// The owned value at a row, charged to `budget` as
    /// [`AnyValueArrays::value_at`] charges it.
    ///
    /// # Errors
    /// As [`AnyValueArrays::value_at`]; the budget is then back where it was.
    pub(crate) fn value_at(
        &mut self,
        row: usize,
        limits: DecodeLimits,
        budget: &mut Budget,
    ) -> Result<Value> {
        let value = self.arrays.value_at(row, limits, &mut self.cache, budget)?;
        Ok(match value {
            AttrValue::Str(s) => Value::Str(s.to_owned()),
            AttrValue::Bytes(b) => Value::Bytes(b.to_vec()),
            AttrValue::Owned(v) => v,
        })
    }

    /// Bytes of every string in a plain `str` column, `None` when the column
    /// is absent or dictionary encoded.
    pub(crate) fn str_bytes(&self) -> Option<usize> {
        let strings = self.arrays.strs.as_ref()?.as_string_opt::<i32>()?;
        let offsets = strings.value_offsets();
        let (first, last) = (offsets.first()?, offsets.last()?);
        usize::try_from(last - first).ok()
    }

    /// The string at `row`, borrowed, when the row's type is `Str`; `None`
    /// for any other type, which [`AnyValueColumns::value_at`] reads. The
    /// string is the one `value_at` would return, under the same cell limit.
    ///
    /// # Errors
    /// Refuses a string longer than `limits.max_cell_bytes`.
    pub(crate) fn str_value_at(&self, row: usize, limits: DecodeLimits) -> Result<Option<&str>> {
        let is_str = self
            .arrays
            .types
            .value_at(row)
            .is_some_and(|ty| AttributeValueType::try_from(ty) == Ok(AttributeValueType::Str));
        if !is_str {
            return Ok(None);
        }
        let s = self
            .arrays
            .strs
            .as_ref()
            .and_then(|a| str_ref(a, row))
            .unwrap_or("");
        cell_fits(s.len(), limits)?;
        Ok(Some(s))
    }
}

/// Decode the CBOR cell at `row` of `a`, holding the tree below its root in
/// `budget` as it is built.
fn decode_charged(
    a: &ArrayRef,
    row: usize,
    limits: DecodeLimits,
    budget: &mut Budget,
) -> Result<Option<Value>> {
    bytes_cell(a, row, |b| decode_cbor_reserving(b, limits, budget)).transpose()
}

fn required(batch: &RecordBatch, name: &str, to: &DataType) -> Result<ArrayRef> {
    plain(batch, name, to)?.ok_or_else(|| Error::invalid(format!("attribute batch lacks {name}")))
}

impl<'c> AttrTable<'c> {
    /// Build the table from the columns of an `attributes_16` or
    /// `attributes_32` batch.
    ///
    /// Keys and values are read through their dictionary (see [`readable`])
    /// and borrowed from `columns`; only map and slice values are decoded.
    /// Each entry's decoded footprint ([`crate::value::entry_bytes`] of its
    /// owned form) is charged to `budget` as it is read, so the table draws
    /// from the request's one `max_extracted_bytes` exactly as an owned copy
    /// would.
    ///
    /// # Errors
    /// Refuses the batch when an attribute type or CBOR payload is malformed,
    /// a parent has a duplicate attribute key, a cell is longer than
    /// `limits.max_cell_bytes`, or the budget is exhausted; the budget is then
    /// back where it was.
    pub(crate) fn from_columns(
        columns: &'c AttrColumns,
        limits: DecodeLimits,
        budget: &mut Budget,
    ) -> Result<Self> {
        let mark = budget.mark();
        let table = Self::read(columns, limits, budget);
        if table.is_err() {
            budget.rollback(mark);
        }
        table
    }

    fn read(columns: &'c AttrColumns, limits: DecodeLimits, budget: &mut Budget) -> Result<Self> {
        let mut cache = SerCache::default();
        let mut entries = Vec::with_capacity(columns.parent_ids.len());
        let mut bytes = 0_usize;
        for (row, &parent) in columns.parent_ids.iter().enumerate() {
            let key = match str_ref(&columns.keys, row) {
                Some(k) => {
                    cell_fits(k.len(), limits)?;
                    budget.charge_decoded(k.len())?;
                    k
                }
                None => "",
            };
            budget.charge_decoded(BUFFER_HEADER_BYTES)?;
            let value = columns.values.value_at(row, limits, &mut cache, budget)?;
            let entry = AttrEntry { parent, key, value };
            bytes += entry.entry_bytes();
            entries.push(entry);
        }
        cache.release(budget);
        // OTAP batches usually arrive grouped by parent; the stable sort keeps
        // the rest correct.
        if !entries.is_sorted_by_key(|e| e.parent) {
            entries.sort_by_key(|e| e.parent);
        }
        for list in entries.chunk_by_mut(|a, b| a.parent == b.parent) {
            // The rules of `crate::value::sort_kvlist`: raw key bytes, unique keys.
            list.sort_unstable_by(|a, b| a.key.as_bytes().cmp(b.key.as_bytes()));
            if list.windows(2).any(|w| w[0].key == w[1].key) {
                return Err(Error::invalid("duplicate attribute key"));
            }
        }
        let index = ParentIndex::new(&entries);
        Ok(Self {
            entries,
            index,
            bytes,
        })
    }

    /// Attributes of one parent, sorted by key, or an empty slice.
    #[must_use]
    pub fn get(&self, parent_id: u32) -> &[AttrEntry<'c>] {
        &self.entries[self.index.range(&self.entries, parent_id)]
    }

    /// Drop the table and give back what it charged to `budget`.
    pub(crate) fn release(self, budget: &mut Budget) {
        budget.uncharge(self.bytes);
    }
}

/// Read `parent_id` as `u32`, refusing a missing column or a null id.
///
/// The column is read through its dictionary when it has one. A null
/// `parent_id` is malformed input, not parent 0.
fn read_parent_ids(batch: &RecordBatch) -> Result<Vec<u32>> {
    let col = batch
        .column_by_name(PARENT_ID)
        .ok_or_else(|| Error::invalid("attribute batch lacks parent_id"))?;
    let value_type = match col.data_type() {
        DataType::Dictionary(_, value) => value.as_ref(),
        other => other,
    };
    let null_parent = || Error::invalid("attribute batch has a null parent_id");
    let unreadable = |e: otel_arrow_dfe_pdata::error::Error| {
        Error::invalid(format!("attribute batch parent_id: {e}"))
    };
    match value_type {
        DataType::UInt16 => {
            let ids = MaybeDictArrayAccessor::<UInt16Array>::try_new(col).map_err(unreadable)?;
            (0..ids.len())
                .map(|row| ids.value_at(row).map(u32::from).ok_or_else(null_parent))
                .collect()
        }
        DataType::UInt32 => {
            let ids = MaybeDictArrayAccessor::<UInt32Array>::try_new(col).map_err(unreadable)?;
            (0..ids.len())
                .map(|row| ids.value_at(row).ok_or_else(null_parent))
                .collect()
        }
        other => Err(Error::invalid(format!("parent_id type {other}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RefuseReason;
    use arrow::array::{
        ArrayRef, BinaryArray, Float64Array, Int64Array, StringArray, UInt8Array, UInt16Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    /// A table's lists, owned, and the bytes it charged.
    #[derive(Debug)]
    struct Owned {
        lists: HashMap<u32, Vec<(String, Value)>>,
        bytes: usize,
    }

    impl Owned {
        fn get(&self, parent: u32) -> &[(String, Value)] {
            self.lists.get(&parent).map_or(&[], Vec::as_slice)
        }
    }

    /// Read `batch` into an [`AttrTable`] and copy its lists out, checking
    /// that the parent index finds each list whole.
    fn from_batch(batch: &RecordBatch, limits: DecodeLimits, budget: &mut Budget) -> Result<Owned> {
        let columns = AttrColumns::new(batch)?;
        let table = AttrTable::from_columns(&columns, limits, budget)?;
        let mut lists: HashMap<u32, Vec<(String, Value)>> = HashMap::new();
        for entry in &table.entries {
            lists
                .entry(entry.parent)
                .or_default()
                .push(entry.to_owned_entry());
        }
        for (&parent, list) in &lists {
            let found: Vec<_> = table.get(parent).iter().map(Attr::to_owned_entry).collect();
            assert_eq!(&found, list, "index of parent {parent}");
        }
        Ok(Owned {
            lists,
            bytes: table.bytes,
        })
    }

    /// Depth 32, no cell-size bound: the size limit has its own tests.
    fn limits() -> DecodeLimits {
        DecodeLimits::new(32, usize::MAX)
    }

    /// A request budget of `limit` bytes.
    fn budget(limit: usize) -> Budget {
        let mut cfg = crate::config::LakeConfig::default();
        cfg.ingress.max_extracted_bytes = limit;
        Budget::new(&cfg)
    }

    /// A CBOR array of `n` zeros: one encoded byte and one decoded node each.
    fn cbor_zeros(n: usize) -> Vec<u8> {
        let mut ser = Vec::new();
        ciborium::into_writer(
            &ciborium::Value::Array(vec![ciborium::Value::Integer(0.into()); n]),
            &mut ser,
        )
        .expect("cbor");
        ser
    }

    /// A one-MiB CBOR cell of small ints, about 32 MiB once decoded.
    fn one_mib_of_ints() -> Vec<u8> {
        let ser = cbor_zeros((1 << 20) - 5);
        assert_eq!(ser.len(), 1 << 20);
        ser
    }

    /// Slice attributes `k`, one parent per row, whose `ser` column is
    /// `Dictionary<U16, Binary>` over `values` and row `i` references key
    /// `keys[i]`: the OTAP schema's encoding of the column.
    fn ser_dictionary_batch(values: &[&[u8]], keys: &[u16]) -> RecordBatch {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::UInt16Type;
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new(
                "ser",
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Binary)),
                true,
            ),
        ]);
        let n = u16::try_from(keys.len()).expect("rows fit u16");
        let ser = DictionaryArray::<UInt16Type>::try_new(
            UInt16Array::from(keys.to_vec()),
            Arc::new(BinaryArray::from(values.to_vec())),
        )
        .expect("dictionary");
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from_iter_values(0..n)),
            Arc::new(StringArray::from(vec!["k"; keys.len()])),
            Arc::new(UInt8Array::from(vec![6u8; keys.len()])),
            Arc::new(ser),
        ];
        RecordBatch::try_new(Arc::new(schema), cols).expect("batch")
    }

    fn refused_by_table(result: &Result<Owned>) -> bool {
        use crate::error::{Excess, SizeBudget};
        matches!(
            result,
            Err(Error::Refused(RefuseReason::RequestTooLarge(Excess {
                budget: SizeBudget::Table,
                ..
            })))
        )
    }

    fn batch() -> RecordBatch {
        let mut ser = Vec::new();
        ciborium::into_writer(
            &ciborium::Value::Array(vec![ciborium::Value::Integer(7.into())]),
            &mut ser,
        )
        .expect("cbor");
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new("str", DataType::Utf8, true),
            Field::new("int", DataType::Int64, true),
            Field::new("double", DataType::Float64, true),
            Field::new("ser", DataType::Binary, true),
        ]);
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(vec![0u16, 0, 1, 1])),
            Arc::new(StringArray::from(vec!["z", "a", "n", "arr"])),
            Arc::new(UInt8Array::from(vec![1u8, 2, 3, 6])),
            Arc::new(StringArray::from(vec![Some("v"), None, None, None])),
            Arc::new(Int64Array::from(vec![None, Some(5), None, None])),
            Arc::new(Float64Array::from(vec![None, None, Some(1.5), None])),
            Arc::new(BinaryArray::from(vec![
                None,
                None,
                None,
                Some(ser.as_slice()),
            ])),
        ];
        RecordBatch::try_new(Arc::new(schema), cols).expect("batch")
    }

    /// An attribute batch of `rows` string attributes whose parent id, key and
    /// value are all dictionary encoded, every row referencing the single
    /// value `value`, one parent per row.
    fn dictionary_batch(rows: usize, value: &str) -> RecordBatch {
        use arrow::array::{DictionaryArray, UInt8Array as Keys8};
        use arrow::datatypes::{UInt8Type, UInt16Type};
        let key_type = Box::new(DataType::UInt8);
        let schema = Schema::new(vec![
            Field::new(
                "parent_id",
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::UInt16)),
                false,
            ),
            Field::new(
                "key",
                DataType::Dictionary(key_type.clone(), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("type", DataType::UInt8, false),
            Field::new(
                "str",
                DataType::Dictionary(key_type, Box::new(DataType::Utf8)),
                true,
            ),
        ]);
        let n = u16::try_from(rows).expect("rows fit u16");
        let parents = DictionaryArray::<UInt16Type>::try_new(
            UInt16Array::from_iter_values(0..n),
            Arc::new(UInt16Array::from_iter_values(0..n)),
        )
        .expect("parents");
        let one = |s: &str| {
            DictionaryArray::<UInt8Type>::try_new(
                Keys8::from(vec![0u8; rows]),
                Arc::new(StringArray::from(vec![s])),
            )
            .expect("dictionary")
        };
        let cols: Vec<ArrayRef> = vec![
            Arc::new(parents),
            Arc::new(one("k")),
            Arc::new(UInt8Array::from(vec![1u8; rows])),
            Arc::new(one(value)),
        ];
        RecordBatch::try_new(Arc::new(schema), cols).expect("batch")
    }

    /// Scenario: a two-MiB dictionary string referenced by 256 rows under a one-MiB cell limit.
    /// Guarantees: the batch is refused on the first cell, before any copy per row.
    #[test]
    fn a_large_dictionary_value_is_refused_before_it_is_expanded() {
        let big = "x".repeat(2 << 20);
        let batch = dictionary_batch(256, &big);
        let limits = DecodeLimits::new(32, 1 << 20);
        assert!(matches!(
            from_batch(&batch, limits, &mut budget(32 << 20)),
            Err(Error::Refused(RefuseReason::RequestTooLarge(_)))
        ));
    }

    /// Scenario: a half-MiB dictionary string referenced by 256 rows (128 MiB decoded) against the
    /// 32 MiB request budget.
    /// Guarantees: the table is refused once its decoded content passes the budget.
    #[test]
    fn many_references_to_one_dictionary_value_are_bounded() {
        let value = "y".repeat(512 << 10);
        let batch = dictionary_batch(256, &value);
        let limits = DecodeLimits::new(32, 1 << 20);
        assert!(matches!(
            from_batch(&batch, limits, &mut budget(32 << 20)),
            Err(Error::Refused(RefuseReason::RequestTooLarge(_)))
        ));
    }

    /// Scenario: 64 rows reference one one-MiB dictionary `ser` array under the default limits.
    /// Guarantees: it is decoded once and refused on its first charged copy.
    #[test]
    fn a_dictionary_ser_value_is_decoded_once_and_its_copies_are_charged() {
        let value = one_mib_of_ints();
        let batch = ser_dictionary_batch(&[&value], &[0; 64]);
        let before = crate::value::decodes();
        let result = from_batch(
            &batch,
            DecodeLimits::new(32, 1 << 20),
            &mut budget(32 << 20),
        );
        assert!(refused_by_table(&result), "{result:?}");
        assert_eq!(crate::value::decodes() - before, 1);
    }

    /// Scenario: rows alternate between two dictionary `ser` arrays under a loose budget.
    /// Guarantees: each key is decoded once and every row reads its own value.
    #[test]
    fn each_distinct_dictionary_ser_value_is_decoded_once() {
        let (a, b) = (cbor_zeros(1000), cbor_zeros(2000));
        let batch = ser_dictionary_batch(&[&a, &b], &[0, 1, 0, 1, 0, 1]);
        let before = crate::value::decodes();
        let t = from_batch(&batch, limits(), &mut budget(usize::MAX)).expect("table");
        assert_eq!(crate::value::decodes() - before, 2);
        for parent in 0..6 {
            let len = if parent % 2 == 0 { 1000 } else { 2000 };
            assert!(matches!(&t.get(parent)[0].1, Value::Array(items) if items.len() == len));
        }
    }

    /// Scenario: one plain `ser` cell of a one-MiB CBOR array against 16 MiB and 64 MiB budgets.
    /// Guarantees: it is charged about 32 MiB decoded: refused under 16 MiB, accepted under 64 MiB.
    #[test]
    fn a_plain_ser_cell_is_charged_its_decoded_footprint() {
        let value = one_mib_of_ints();
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new("ser", DataType::Binary, true),
        ]);
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(vec![0u16])),
            Arc::new(StringArray::from(vec!["k"])),
            Arc::new(UInt8Array::from(vec![6u8])),
            Arc::new(BinaryArray::from(vec![value.as_slice()])),
        ];
        let batch = RecordBatch::try_new(Arc::new(schema), cols).expect("batch");
        let limits = DecodeLimits::new(32, 1 << 20);
        let refused = from_batch(&batch, limits, &mut budget(16 << 20));
        assert!(refused_by_table(&refused), "{refused:?}");
        let t = from_batch(&batch, limits, &mut budget(64 << 20)).expect("fits");
        assert!(t.bytes > VALUE_NODE_BYTES * ((1 << 20) - 5));
    }

    /// A request budget of `limit` bytes already holding `held`, and its mark.
    fn held_budget(limit: usize, held: usize) -> (Budget, crate::extract::Mark) {
        let mut b = budget(limit);
        b.charge(held).expect("fits");
        let mark = b.mark();
        (b, mark)
    }

    /// One attribute row of type `tag` whose only value column is `column`.
    fn one_row(tag: u8, column: Option<(&str, ArrayRef)>) -> RecordBatch {
        let mut fields = vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
        ];
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(vec![0u16])),
            Arc::new(StringArray::from(vec!["k"])),
            Arc::new(UInt8Array::from(vec![tag])),
        ];
        if let Some((name, col)) = column {
            fields.push(Field::new(name, col.data_type().clone(), true));
            cols.push(col);
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).expect("batch")
    }

    /// Scenario: `value_at` fails for each reason it can on a budget holding 1000 bytes.
    /// Guarantees: every failure leaves the budget at its mark.
    #[test]
    fn a_failed_value_leaves_the_budget_at_its_mark() {
        let ser = |bytes: &[u8]| -> Option<(&str, ArrayRef)> {
            Some(("ser", Arc::new(BinaryArray::from(vec![bytes])) as ArrayRef))
        };
        let long = "s".repeat(4096);
        let cases: Vec<(&str, RecordBatch, usize)> = vec![
            ("unknown type tag", one_row(42, None), 1 << 20),
            ("missing ser", one_row(5, None), 1 << 20),
            (
                "string past the budget",
                one_row(
                    1,
                    Some(("str", Arc::new(StringArray::from(vec![long.as_str()])))),
                ),
                2048,
            ),
            (
                "malformed cbor",
                one_row(6, ser(&[0x9f, 0x00, 0x00])),
                1 << 20,
            ),
        ];
        for (name, batch, limit) in cases {
            let (mut budget, mark) = held_budget(limit, 1000);
            let mut any =
                AnyValueColumns::new(&|n| batch.column_by_name(n).cloned()).expect("cols");
            assert!(any.value_at(0, limits(), &mut budget).is_err(), "{name}");
            assert_eq!(budget.mark(), mark, "{name}");
        }

        // Two rows share one dictionary value: the first decode fits, its
        // kept copy does not.
        let value = cbor_zeros(1000);
        let batch = ser_dictionary_batch(&[&value], &[0, 0]);
        let (mut budget, mark) =
            held_budget(1000 + 2 * VALUE_NODE_BYTES + 1000 * VALUE_NODE_BYTES, 1000);
        let mut any = AnyValueColumns::new(&|n| batch.column_by_name(n).cloned()).expect("cols");
        assert!(any.value_at(0, limits(), &mut budget).is_err(), "memo copy");
        assert_eq!(budget.mark(), mark, "memo copy");
    }

    /// Scenario: a batch whose first row decodes and whose second has an unknown type tag.
    /// Guarantees: the refused table leaves the budget at its mark.
    #[test]
    fn a_failed_table_leaves_the_budget_at_its_mark() {
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new("str", DataType::Utf8, true),
        ]);
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(vec![0u16, 1])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(UInt8Array::from(vec![1u8, 42])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ];
        let batch = RecordBatch::try_new(Arc::new(schema), cols).expect("batch");
        let (mut budget, mark) = held_budget(1 << 20, 1000);
        assert!(from_batch(&batch, limits(), &mut budget).is_err());
        assert_eq!(budget.mark(), mark);
    }

    /// Scenario: a batch with dictionary-encoded parent ids, keys and values.
    /// Guarantees: every row reads back through the dictionary and the prepared column stays
    /// encoded.
    #[test]
    fn a_dictionary_batch_reads_through_without_expansion() {
        let batch = dictionary_batch(3, "shared");
        let t = from_batch(&batch, limits(), &mut budget(usize::MAX)).expect("table");
        for parent in 0..3 {
            assert_eq!(
                t.get(parent),
                &[("k".to_string(), Value::Str("shared".into()))]
            );
        }
        let col = plain(&batch, "str", &DataType::Utf8)
            .expect("readable")
            .expect("present");
        assert!(matches!(col.data_type(), DataType::Dictionary(_, _)));
        assert_eq!(str_ref(&col, 2).map(str::len), Some(6));
    }

    /// Scenario: two parents with mixed value types, keys unsorted in the batch.
    /// Guarantees: each parent gets a sorted, typed attribute list; absent parents are empty.
    #[test]
    fn groups_and_sorts_by_parent() {
        let t = from_batch(&batch(), limits(), &mut budget(usize::MAX)).expect("table");
        assert_eq!(
            t.get(0),
            &[
                ("a".to_string(), Value::Int(5)),
                ("z".to_string(), Value::Str("v".into()))
            ]
        );
        assert_eq!(
            t.get(1),
            &[
                ("arr".to_string(), Value::Array(vec![Value::Int(7)])),
                ("n".to_string(), Value::Double(1.5))
            ]
        );
        assert!(t.get(7).is_empty());
    }

    /// Scenario: the same key appears twice for one parent.
    /// Guarantees: the whole batch is refused as invalid content.
    #[test]
    fn duplicate_key_is_refused() {
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new("str", DataType::Utf8, true),
        ]);
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(vec![3u16, 3])),
            Arc::new(StringArray::from(vec!["k", "k"])),
            Arc::new(UInt8Array::from(vec![1u8, 1])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ];
        let b = RecordBatch::try_new(Arc::new(schema), cols).expect("batch");
        assert!(matches!(
            from_batch(&b, limits(), &mut budget(usize::MAX)),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
    }

    /// Scenario: an attribute row carries a null `parent_id`.
    /// Guarantees: the batch is refused instead of silently attributing the row to parent 0.
    #[test]
    fn null_parent_id_is_refused() {
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, true),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new("str", DataType::Utf8, true),
        ]);
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(vec![Some(0u16), None])),
            Arc::new(StringArray::from(vec!["k", "j"])),
            Arc::new(UInt8Array::from(vec![1u8, 1])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ];
        let b = RecordBatch::try_new(Arc::new(schema), cols).expect("batch");
        assert!(matches!(
            from_batch(&b, limits(), &mut budget(usize::MAX)),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
    }

    /// Build a batch of one attribute per row with the given type tags and NO
    /// value columns at all, the shape pdata emits when every value in a batch
    /// is that type's default.
    fn batch_without_value_columns(tags: &[u8]) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
        ]);
        let keys: Vec<String> = (0..tags.len()).map(|i| format!("k{i}")).collect();
        let parents: Vec<u16> = vec![0; tags.len()];
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(parents)),
            Arc::new(StringArray::from(keys)),
            Arc::new(UInt8Array::from(tags.to_vec())),
        ];
        RecordBatch::try_new(Arc::new(schema), cols).expect("batch")
    }

    /// Scenario: absent value columns, one row per typed tag, as pdata emits all-default columns.
    /// Guarantees: each decodes to its type's default, never `Value::Null`.
    #[test]
    fn absent_value_column_decodes_as_the_type_default() {
        // Tags: str, int, double, bool, bytes, empty.
        let b = batch_without_value_columns(&[1, 2, 3, 4, 7, 0]);
        let t = from_batch(&b, limits(), &mut budget(usize::MAX)).expect("table");
        assert_eq!(
            t.get(0),
            &[
                ("k0".to_string(), Value::Str(String::new())),
                ("k1".to_string(), Value::Int(0)),
                ("k2".to_string(), Value::Double(0.0)),
                ("k3".to_string(), Value::Bool(false)),
                ("k4".to_string(), Value::Bytes(Vec::new())),
                ("k5".to_string(), Value::Null),
            ]
        );
    }

    /// Scenario: a typed value column present but null in this row.
    /// Guarantees: the cell decodes to the type's default.
    #[test]
    fn null_value_cell_decodes_as_the_type_default() {
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new("str", DataType::Utf8, true),
            Field::new("int", DataType::Int64, true),
        ]);
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(vec![0u16, 0])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(UInt8Array::from(vec![1u8, 2])),
            Arc::new(StringArray::from(vec![None::<&str>, None])),
            Arc::new(Int64Array::from(vec![None::<i64>, None])),
        ];
        let b = RecordBatch::try_new(Arc::new(schema), cols).expect("batch");
        let t = from_batch(&b, limits(), &mut budget(usize::MAX)).expect("table");
        assert_eq!(
            t.get(0),
            &[
                ("a".to_string(), Value::Str(String::new())),
                ("b".to_string(), Value::Int(0)),
            ]
        );
    }

    /// Scenario: a map or slice attribute whose `ser` payload is absent.
    /// Guarantees: the batch is refused as malformed.
    #[test]
    fn map_or_slice_without_a_ser_payload_is_refused() {
        for tag in [5u8, 6] {
            let b = batch_without_value_columns(&[tag]);
            assert!(matches!(
                from_batch(&b, limits(), &mut budget(usize::MAX)),
                Err(Error::Refused(RefuseReason::Invalid(_)))
            ));
        }
    }
}
