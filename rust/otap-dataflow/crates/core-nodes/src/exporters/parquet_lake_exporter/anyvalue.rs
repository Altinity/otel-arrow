// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! OTAP AnyValue columns: rendering to strings for `Map<Utf8, Utf8>` output and canonical
//! identity encoding. Both treat a typed row with a null value column as the type default, like the
//! pdata views (OTAP encoders omit all-default value columns).

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
    StringBuilder, UInt8Array,
};
use arrow::datatypes::DataType;
use otel_arrow_dfe_pdata::otlp::attributes::AttributeValueType;
use otel_arrow_dfe_pdata::schema::consts;

use super::columns::cast_or_null;
use super::error::LakeError;
use super::identity::{put_bin, put_bool, put_cbor, put_double, put_int, put_str, tag};

/// Canonicalized AnyValue columns of one table (attrs table or `body` struct).
pub struct AnyValueColumns {
    ty: UInt8Array,
    str_: StringArray,
    int: Int64Array,
    double: Float64Array,
    bool_: BooleanArray,
    bytes: BinaryArray,
    ser: BinaryArray,
}

impl AnyValueColumns {
    /// Build from a lookup function returning each column by its OTAP name.
    pub fn new(len: usize, get: impl Fn(&str) -> Option<ArrayRef>) -> Result<Self, LakeError> {
        let c = |name: &str, dt: &DataType| cast_or_null(get(name).as_ref(), dt, len);
        Ok(Self {
            ty: downcast::<UInt8Array>(&c(consts::ATTRIBUTE_TYPE, &DataType::UInt8)?),
            str_: downcast::<StringArray>(&c(consts::ATTRIBUTE_STR, &DataType::Utf8)?),
            int: downcast::<Int64Array>(&c(consts::ATTRIBUTE_INT, &DataType::Int64)?),
            double: downcast::<Float64Array>(&c(consts::ATTRIBUTE_DOUBLE, &DataType::Float64)?),
            bool_: downcast::<BooleanArray>(&c(consts::ATTRIBUTE_BOOL, &DataType::Boolean)?),
            bytes: downcast::<BinaryArray>(&c(consts::ATTRIBUTE_BYTES, &DataType::Binary)?),
            ser: downcast::<BinaryArray>(&c(consts::ATTRIBUTE_SER, &DataType::Binary)?),
        })
    }

    /// Render all rows into a Utf8 array (null for Empty / null type).
    #[must_use]
    pub fn render_all(&self) -> StringArray {
        self.render_where(|_| true)
    }

    /// Render all rows, emitting null where `valid(i)` is false (e.g. a null parent struct).
    #[must_use]
    pub fn render_where(&self, valid: impl Fn(usize) -> bool) -> StringArray {
        let mut out = StringBuilder::with_capacity(self.ty.len(), self.ty.len() * 16);
        for i in 0..self.ty.len() {
            if valid(i) {
                self.render_into(&mut out, i);
            } else {
                out.append_null();
            }
        }
        out.finish()
    }

    /// Append row `i` to `out` without an intermediate `String` for the common `Str` case.
    pub fn render_into(&self, out: &mut StringBuilder, i: usize) {
        if !self.ty.is_null(i)
            && self.ty.value(i) == AttributeValueType::Str as u8
            && !self.str_.is_null(i)
        {
            out.append_value(self.str_.value(i));
            return;
        }
        match self.render(i) {
            Some(s) => out.append_value(s),
            None => out.append_null(),
        }
    }

    fn value_type(&self, i: usize) -> Option<AttributeValueType> {
        if self.ty.is_null(i) {
            None
        } else {
            AttributeValueType::try_from(self.ty.value(i)).ok()
        }
    }

    /// True when row `i` is a Map/Slice with a non-empty serialized value.
    fn has_ser(&self, i: usize) -> bool {
        !self.ser.is_null(i) && !self.ser.value(i).is_empty()
    }

    /// Render row `i` (None for the EMPTY class; a typed null renders as the type default, like
    /// `canonical_into`).
    #[must_use]
    pub fn render(&self, i: usize) -> Option<String> {
        match self.value_type(i) {
            Some(AttributeValueType::Str) => Some(if self.str_.is_null(i) {
                String::new()
            } else {
                self.str_.value(i).to_owned()
            }),
            Some(AttributeValueType::Int) => Some(
                if self.int.is_null(i) {
                    0
                } else {
                    self.int.value(i)
                }
                .to_string(),
            ),
            Some(AttributeValueType::Double) => Some(
                if self.double.is_null(i) {
                    0.0
                } else {
                    self.double.value(i)
                }
                .to_string(),
            ),
            Some(AttributeValueType::Bool) => {
                Some((!self.bool_.is_null(i) && self.bool_.value(i)).to_string())
            }
            Some(AttributeValueType::Bytes) => Some(if self.bytes.is_null(i) {
                String::new()
            } else {
                hex::encode(self.bytes.value(i))
            }),
            Some(AttributeValueType::Map | AttributeValueType::Slice) if self.has_ser(i) => {
                Some(cbor_to_json_string(self.ser.value(i)))
            }
            _ => None,
        }
    }

    /// Canonical encoding of row `i` (FORMAT.md "Identity grammar"). A typed row with a null value
    /// column encodes as the type default; a null type, the Empty type, an unknown type code, or a
    /// Map/Slice with a null or empty `ser` encodes as EMPTY.
    pub fn canonical_into(&self, i: usize, out: &mut Vec<u8>) {
        match self.value_type(i) {
            Some(AttributeValueType::Str) => put_str(
                out,
                if self.str_.is_null(i) {
                    b""
                } else {
                    self.str_.value(i).as_bytes()
                },
            ),
            Some(AttributeValueType::Int) => {
                put_int(
                    out,
                    if self.int.is_null(i) {
                        0
                    } else {
                        self.int.value(i)
                    },
                );
            }
            Some(AttributeValueType::Double) => put_double(
                out,
                if self.double.is_null(i) {
                    0.0
                } else {
                    self.double.value(i)
                },
            ),
            Some(AttributeValueType::Bool) => {
                put_bool(out, !self.bool_.is_null(i) && self.bool_.value(i));
            }
            Some(AttributeValueType::Bytes) => put_bin(
                out,
                if self.bytes.is_null(i) {
                    b""
                } else {
                    self.bytes.value(i)
                },
            ),
            Some(AttributeValueType::Map | AttributeValueType::Slice) if self.has_ser(i) => {
                put_cbor(out, self.ser.value(i));
            }
            _ => out.push(tag::EMPTY),
        }
    }
}

fn downcast<T: Array + Clone + 'static>(a: &ArrayRef) -> T {
    a.as_any()
        .downcast_ref::<T>()
        .expect("cast_or_null returns the requested type")
        .clone()
}

/// Decode a CBOR-serialized AnyValue (map or slice) into a compact JSON string.
/// Undecodable bytes are rendered as hex so data is never silently dropped.
#[must_use]
pub fn cbor_to_json_string(bytes: &[u8]) -> String {
    match ciborium::from_reader::<ciborium::Value, _>(bytes) {
        Ok(v) => cbor_to_json(&v).to_string(),
        Err(_) => hex::encode(bytes),
    }
}

fn cbor_to_json(v: &ciborium::Value) -> serde_json::Value {
    use ciborium::Value as C;
    use serde_json::Value as J;
    match v {
        C::Null => J::Null,
        C::Bool(b) => J::Bool(*b),
        C::Integer(i) => {
            let i: i128 = (*i).into();
            i64::try_from(i).map_or_else(|_| J::String(i.to_string()), |v| J::Number(v.into()))
        }
        // JSON has no NaN/Infinity: keep them as strings instead of dropping them to null.
        C::Float(f) => serde_json::Number::from_f64(*f).map_or_else(
            || {
                J::String(
                    if f.is_nan() {
                        "NaN"
                    } else if *f > 0.0 {
                        "Infinity"
                    } else {
                        "-Infinity"
                    }
                    .to_owned(),
                )
            },
            J::Number,
        ),
        C::Text(s) => J::String(s.clone()),
        C::Bytes(b) => J::String(hex::encode(b)),
        C::Array(items) => J::Array(items.iter().map(cbor_to_json).collect()),
        C::Map(entries) => J::Object(
            entries
                .iter()
                .map(|(k, v)| {
                    let key = match k {
                        C::Text(s) => s.clone(),
                        other => cbor_to_json(other).to_string(),
                    };
                    (key, cbor_to_json(v))
                })
                .collect(),
        ),
        C::Tag(_, inner) => cbor_to_json(inner),
        _ => J::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn cbor(v: &ciborium::Value) -> Vec<u8> {
        let mut out = Vec::new();
        ciborium::into_writer(v, &mut out).expect("cbor encodes");
        out
    }

    fn columns() -> AnyValueColumns {
        let map = cbor(&ciborium::Value::Map(vec![(
            ciborium::Value::Text("a".into()),
            ciborium::Value::Integer(1.into()),
        )]));
        let slice = cbor(&ciborium::Value::Array(vec![
            ciborium::Value::Integer(1.into()),
            ciborium::Value::Text("x".into()),
        ]));
        let t = |v: AttributeValueType| Some(v as u8);
        let ty: ArrayRef = Arc::new(UInt8Array::from(vec![
            t(AttributeValueType::Str),
            t(AttributeValueType::Int),
            t(AttributeValueType::Double),
            t(AttributeValueType::Bool),
            t(AttributeValueType::Bytes),
            t(AttributeValueType::Map),
            t(AttributeValueType::Slice),
            t(AttributeValueType::Empty),
            t(AttributeValueType::Map),
        ]));
        let str_: ArrayRef = Arc::new(StringArray::from(vec![
            Some("hello"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ]));
        let int: ArrayRef = Arc::new(Int64Array::from(vec![
            None,
            Some(42),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ]));
        let double: ArrayRef = Arc::new(Float64Array::from(vec![
            None,
            None,
            Some(1.5),
            None,
            None,
            None,
            None,
            None,
            None,
        ]));
        let bool_: ArrayRef = Arc::new(BooleanArray::from(vec![
            None,
            None,
            None,
            Some(true),
            None,
            None,
            None,
            None,
            None,
        ]));
        let bytes: ArrayRef = Arc::new(BinaryArray::from(vec![
            None,
            None,
            None,
            None,
            Some(&[0xde_u8, 0xad][..]),
            None,
            None,
            None,
            None,
        ]));
        let ser: ArrayRef = Arc::new(BinaryArray::from(vec![
            None,
            None,
            None,
            None,
            None,
            Some(map.as_slice()),
            Some(slice.as_slice()),
            None,
            Some(&[0xff_u8, 0xff][..]),
        ]));
        AnyValueColumns::new(9, |name| match name {
            consts::ATTRIBUTE_TYPE => Some(ty.clone()),
            consts::ATTRIBUTE_STR => Some(str_.clone()),
            consts::ATTRIBUTE_INT => Some(int.clone()),
            consts::ATTRIBUTE_DOUBLE => Some(double.clone()),
            consts::ATTRIBUTE_BOOL => Some(bool_.clone()),
            consts::ATTRIBUTE_BYTES => Some(bytes.clone()),
            consts::ATTRIBUTE_SER => Some(ser.clone()),
            _ => None,
        })
        .expect("columns")
    }

    /// Scenario: One row of every AnyValue type, plus an invalid CBOR payload, is rendered.
    /// Guarantees: Scalars render as text, bytes as hex, maps/slices as compact JSON, Empty as null, and invalid CBOR as hex instead of panicking.
    #[test]
    fn every_any_value_type_renders() {
        let c = columns();
        let expected = [
            Some("hello"),
            Some("42"),
            Some("1.5"),
            Some("true"),
            Some("dead"),
            Some("{\"a\":1}"),
            Some("[1,\"x\"]"),
            None,
            Some("ffff"),
        ];
        for (i, want) in expected.iter().enumerate() {
            assert_eq!(c.render(i).as_deref(), *want, "row {i}");
        }
        let all = c.render_all();
        for (i, want) in expected.iter().enumerate() {
            let got = (!all.is_null(i)).then(|| all.value(i));
            assert_eq!(got, *want, "render_into row {i}");
        }
    }

    /// Scenario: An attrs table has only the `type` column (OTAP omits all-default value columns).
    /// Guarantees: The missing value column reads as the type default ("0" for Int) rather than an error or null.
    #[test]
    fn absent_optional_columns_render_type_defaults() {
        let ty: ArrayRef = Arc::new(UInt8Array::from(vec![AttributeValueType::Int as u8]));
        let c = AnyValueColumns::new(1, |name| {
            (name == consts::ATTRIBUTE_TYPE).then(|| ty.clone())
        })
        .expect("columns");
        assert_eq!(c.render(0).as_deref(), Some("0"));
    }

    fn canonical(c: &AnyValueColumns, i: usize) -> Vec<u8> {
        let mut out = Vec::new();
        c.canonical_into(i, &mut out);
        out
    }

    /// Scenario: Typed rows have null value columns (Int, Str, Double, Bool, Bytes), next to explicit default values, plus a Map with a null `ser`.
    /// Guarantees: Each typed null encodes and renders exactly like its explicit default, and the null Map encodes as EMPTY and renders as null.
    #[test]
    fn typed_null_equals_default() {
        let t = |v: AttributeValueType| v as u8;
        let types = [
            t(AttributeValueType::Int),
            t(AttributeValueType::Str),
            t(AttributeValueType::Double),
            t(AttributeValueType::Bool),
            t(AttributeValueType::Bytes),
        ];
        let ty: ArrayRef = Arc::new(UInt8Array::from(
            types
                .iter()
                .chain(types.iter())
                .copied()
                .chain([t(AttributeValueType::Map), t(AttributeValueType::Empty)])
                .collect::<Vec<u8>>(),
        ));
        // Rows 0..5 have null values; rows 5..10 hold the explicit defaults.
        let n = 12;
        let int: ArrayRef = Arc::new(Int64Array::from_iter((0..n).map(|i| (i == 5).then_some(0))));
        let str_: ArrayRef = Arc::new(StringArray::from_iter(
            (0..n).map(|i| (i == 6).then_some("")),
        ));
        let double: ArrayRef = Arc::new(Float64Array::from_iter(
            (0..n).map(|i| (i == 7).then_some(0.0)),
        ));
        let bool_: ArrayRef = Arc::new(BooleanArray::from_iter(
            (0..n).map(|i| (i == 8).then_some(false)),
        ));
        let bytes: ArrayRef = Arc::new(BinaryArray::from_iter(
            (0..n).map(|i| (i == 9).then_some(b"".as_slice())),
        ));
        let c = AnyValueColumns::new(n, |name| match name {
            consts::ATTRIBUTE_TYPE => Some(ty.clone()),
            consts::ATTRIBUTE_INT => Some(int.clone()),
            consts::ATTRIBUTE_STR => Some(str_.clone()),
            consts::ATTRIBUTE_DOUBLE => Some(double.clone()),
            consts::ATTRIBUTE_BOOL => Some(bool_.clone()),
            consts::ATTRIBUTE_BYTES => Some(bytes.clone()),
            _ => None,
        })
        .expect("columns");
        for i in 0..5 {
            assert_eq!(canonical(&c, i), canonical(&c, i + 5), "canonical row {i}");
            assert_eq!(c.render(i), c.render(i + 5), "render row {i}");
        }
        assert_eq!(c.render(0).as_deref(), Some("0"));
        assert_eq!(c.render(1).as_deref(), Some(""));
        assert_eq!(canonical(&c, 10), vec![tag::EMPTY]);
        assert_eq!(canonical(&c, 11), vec![tag::EMPTY]);
        assert_eq!(c.render(10), None);
    }

    /// Scenario: A serialized array holds NaN, +Infinity and -Infinity doubles.
    /// Guarantees: They render as the strings "NaN", "Infinity" and "-Infinity" instead of being dropped to JSON null.
    #[test]
    fn non_finite_nested_doubles_are_kept() {
        let arr = ciborium::Value::Array(vec![
            ciborium::Value::Float(f64::NAN),
            ciborium::Value::Float(f64::INFINITY),
            ciborium::Value::Float(f64::NEG_INFINITY),
        ]);
        assert_eq!(
            cbor_to_json_string(&cbor(&arr)),
            "[\"NaN\",\"Infinity\",\"-Infinity\"]"
        );
    }

    /// Scenario: A row is rendered with a validity predicate that marks it null.
    /// Guarantees: render_where emits null for rows whose parent struct is null.
    #[test]
    fn render_where_masks_invalid_rows() {
        let c = columns();
        let out = c.render_where(|i| i != 0);
        assert!(out.is_null(0));
        assert_eq!(out.value(1), "42");
    }
}
