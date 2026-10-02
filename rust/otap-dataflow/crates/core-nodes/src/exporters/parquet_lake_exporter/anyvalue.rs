// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! OTAP AnyValue columns: the v1 rendering for `MAP<STRING, STRING>` cells and bodies, and the
//! canonical identity encoding (docs/FORMAT.md sections 1 and 2). A typed row with an absent or
//! null value column reads as the type default, because OTAP encoders omit all-default value
//! columns. Map and slice values are decoded once, when the columns are built.

use std::collections::HashMap;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
    StringBuilder, UInt8Array,
};
use arrow::datatypes::DataType;
use otel_arrow_dfe_pdata::otlp::attributes::AttributeValueType;
use otel_arrow_dfe_pdata::schema::consts;

use super::canonical::{encode_value, put_bool, put_bytes, put_double, put_int, put_null, put_str};
use super::columns::{cast_cost, cast_or_null, utf8_lossy};
use super::error::LakeError;
use super::limits::{Budget, Limits};
use super::value::{Value, decode_cbor, render_v1, write_bytes_v1, write_double};

/// AnyValue columns of one table (attrs table or `body` struct).
pub struct AnyValueColumns {
    ty: UInt8Array,
    str_: StringArray,
    int: Int64Array,
    double: Float64Array,
    bool_: BooleanArray,
    bytes: BinaryArray,
    /// Decoded `ser` cells of the Map and Slice rows.
    nested: HashMap<usize, Value>,
}

impl AnyValueColumns {
    /// Build from a lookup function returning each column by its OTAP name, and validate every
    /// row for which `valid` is true.
    ///
    /// # Errors
    /// Invalid content for a missing type column, an unknown type code, a map or slice without a
    /// `ser` cell or with an undecodable one; too deep for nesting beyond the limit; too large
    /// for a cell above `max_row_bytes` or when the budget is exhausted.
    pub fn new(
        len: usize,
        get: impl Fn(&str) -> Option<ArrayRef>,
        valid: impl Fn(usize) -> bool,
        limits: &Limits,
        budget: &mut Budget,
    ) -> Result<Self, LakeError> {
        if len > 0 && get(consts::ATTRIBUTE_TYPE).is_none() {
            return Err(LakeError::Invalid("missing type column".into()));
        }
        for (name, plain) in [
            (consts::ATTRIBUTE_STR, DataType::Utf8),
            (consts::ATTRIBUTE_BYTES, DataType::Binary),
            (consts::ATTRIBUTE_SER, DataType::Binary),
        ] {
            if let Some(a) = get(name) {
                budget.charge(cast_cost(&a, &plain))?;
            }
        }
        let c = |name: &str, dt: &DataType| cast_or_null(get(name).as_ref(), dt, len);
        let ty = downcast::<UInt8Array>(&c(consts::ATTRIBUTE_TYPE, &DataType::UInt8)?);
        let str_ = utf8_lossy(get(consts::ATTRIBUTE_STR).as_ref(), len, budget)?;
        let bytes = downcast::<BinaryArray>(&c(consts::ATTRIBUTE_BYTES, &DataType::Binary)?);
        let ser = downcast::<BinaryArray>(&c(consts::ATTRIBUTE_SER, &DataType::Binary)?);
        let mut nested = HashMap::new();
        for i in (0..len).filter(|&i| valid(i) && !ty.is_null(i)) {
            let code = ty.value(i);
            match AttributeValueType::try_from(code) {
                Err(_) => return Err(LakeError::Invalid(format!("attribute type {code}"))),
                Ok(AttributeValueType::Str) if !str_.is_null(i) => {
                    limits.check_cell(str_.value(i).len())?;
                }
                Ok(AttributeValueType::Bytes) if !bytes.is_null(i) => {
                    limits.check_cell(bytes.value(i).len())?;
                }
                Ok(AttributeValueType::Map | AttributeValueType::Slice) => {
                    if ser.is_null(i) {
                        return Err(LakeError::Invalid(
                            "map or slice attribute without a ser payload".into(),
                        ));
                    }
                    let cell = ser.value(i);
                    limits.check_cell(cell.len())?;
                    let value = decode_cbor(cell, limits.max_nesting_depth, budget)?;
                    let _ = nested.insert(i, value);
                }
                Ok(_) => {}
            }
        }
        Ok(Self {
            ty,
            str_,
            int: downcast::<Int64Array>(&c(consts::ATTRIBUTE_INT, &DataType::Int64)?),
            double: downcast::<Float64Array>(&c(consts::ATTRIBUTE_DOUBLE, &DataType::Float64)?),
            bool_: downcast::<BooleanArray>(&c(consts::ATTRIBUTE_BOOL, &DataType::Boolean)?),
            bytes,
            nested,
        })
    }

    /// Columns of an absent table.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            ty: UInt8Array::from(Vec::<u8>::new()),
            str_: StringArray::from(Vec::<&str>::new()),
            int: Int64Array::from(Vec::<i64>::new()),
            double: Float64Array::from(Vec::<f64>::new()),
            bool_: BooleanArray::from(Vec::<bool>::new()),
            bytes: BinaryArray::from(Vec::<&[u8]>::new()),
            nested: HashMap::new(),
        }
    }

    fn value_type(&self, i: usize) -> Option<AttributeValueType> {
        if self.ty.is_null(i) {
            None
        } else {
            AttributeValueType::try_from(self.ty.value(i)).ok()
        }
    }

    /// Render every row for which `valid` is true as its map cell or body string (null for an
    /// unset value and for the other rows), charging the rendered bytes to `budget`.
    pub fn render_where(
        &self,
        valid: impl Fn(usize) -> bool,
        budget: &mut Budget,
    ) -> Result<StringArray, LakeError> {
        let mut out = StringBuilder::with_capacity(self.ty.len(), 0);
        let mut scratch = String::new();
        for i in 0..self.ty.len() {
            if !valid(i) {
                out.append_null();
                continue;
            }
            match self.value_type(i) {
                None | Some(AttributeValueType::Empty) => out.append_null(),
                Some(AttributeValueType::Str) => {
                    let s = if self.str_.is_null(i) {
                        ""
                    } else {
                        self.str_.value(i)
                    };
                    budget.charge(s.len())?;
                    out.append_value(s);
                }
                Some(other) => {
                    scratch.clear();
                    self.render_into(other, i, &mut scratch);
                    budget.charge(scratch.len())?;
                    out.append_value(&scratch);
                }
            }
        }
        Ok(out.finish())
    }

    /// The compact JSON of `render_v1` for a non-string, non-null value.
    fn render_into(&self, ty: AttributeValueType, i: usize, out: &mut String) {
        use std::fmt::Write as _;
        let _ = match ty {
            AttributeValueType::Int => {
                write!(
                    out,
                    "{}",
                    if self.int.is_null(i) {
                        0
                    } else {
                        self.int.value(i)
                    }
                )
            }
            AttributeValueType::Double => write_double(
                if self.double.is_null(i) {
                    0.0
                } else {
                    self.double.value(i)
                },
                out,
            ),
            AttributeValueType::Bool => {
                out.write_str(if !self.bool_.is_null(i) && self.bool_.value(i) {
                    "true"
                } else {
                    "false"
                })
            }
            AttributeValueType::Bytes => write_bytes_v1(
                if self.bytes.is_null(i) {
                    b""
                } else {
                    self.bytes.value(i)
                },
                out,
            ),
            AttributeValueType::Map | AttributeValueType::Slice => {
                // `new` decoded every valid map and slice row.
                match self.nested.get(&i) {
                    Some(v) => out.write_str(&render_v1(v)),
                    None => out.write_str("null"),
                }
            }
            AttributeValueType::Str | AttributeValueType::Empty => Ok(()),
        };
    }

    /// Canonical encoding of row `i` (FORMAT.md section 1).
    pub fn canonical_into(&self, i: usize, out: &mut Vec<u8>) {
        match self.value_type(i) {
            None | Some(AttributeValueType::Empty) => put_null(out),
            Some(AttributeValueType::Str) => {
                put_str(
                    out,
                    if self.str_.is_null(i) {
                        ""
                    } else {
                        self.str_.value(i)
                    },
                );
            }
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
            Some(AttributeValueType::Double) => {
                put_double(
                    out,
                    if self.double.is_null(i) {
                        0.0
                    } else {
                        self.double.value(i)
                    },
                );
            }
            Some(AttributeValueType::Bool) => {
                put_bool(out, !self.bool_.is_null(i) && self.bool_.value(i));
            }
            Some(AttributeValueType::Bytes) => {
                put_bytes(
                    out,
                    if self.bytes.is_null(i) {
                        b""
                    } else {
                        self.bytes.value(i)
                    },
                );
            }
            Some(AttributeValueType::Map | AttributeValueType::Slice) => {
                match self.nested.get(&i) {
                    Some(v) => encode_value(out, v),
                    None => put_null(out),
                }
            }
        }
    }
}

fn downcast<T: Array + Clone + 'static>(a: &ArrayRef) -> T {
    a.as_any()
        .downcast_ref::<T>()
        .expect("cast_or_null returns the requested type")
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn limits() -> Limits {
        Limits {
            max_extracted_bytes: 1 << 30,
            max_row_bytes: 1 << 20,
            max_nesting_depth: 32,
            max_chunk_bytes: 1 << 30,
        }
    }

    fn cbor(v: &ciborium::Value) -> Vec<u8> {
        let mut out = Vec::new();
        ciborium::into_writer(v, &mut out).expect("cbor encodes");
        out
    }

    /// One AnyValue row of the fixture table.
    enum Cell {
        Str(String),
        Int(i64),
        Double(f64),
        Bool(bool),
        Bytes(Vec<u8>),
        /// A Map row with this `ser` payload (`None`: a null `ser` cell).
        Map(Option<Vec<u8>>),
        /// A Slice row with this `ser` payload.
        Slice(Vec<u8>),
        /// A type code with every value column null.
        Typed(u8),
    }

    /// The columns of an attrs table, row by row.
    #[derive(Default)]
    struct Rows {
        ty: Vec<Option<u8>>,
        str_: Vec<Option<String>>,
        int: Vec<Option<i64>>,
        double: Vec<Option<f64>>,
        bool_: Vec<Option<bool>>,
        bytes: Vec<Option<Vec<u8>>>,
        ser: Vec<Option<Vec<u8>>>,
    }

    impl Rows {
        fn of(cells: Vec<Cell>) -> Self {
            let mut rows = Self::default();
            for cell in cells {
                rows.push(cell);
            }
            rows
        }

        fn push(&mut self, cell: Cell) {
            let t = |v: AttributeValueType| v as u8;
            let (mut s, mut i, mut d, mut b, mut by, mut ser) =
                (None, None, None, None, None, None);
            let ty = match cell {
                Cell::Str(v) => {
                    s = Some(v);
                    t(AttributeValueType::Str)
                }
                Cell::Int(v) => {
                    i = Some(v);
                    t(AttributeValueType::Int)
                }
                Cell::Double(v) => {
                    d = Some(v);
                    t(AttributeValueType::Double)
                }
                Cell::Bool(v) => {
                    b = Some(v);
                    t(AttributeValueType::Bool)
                }
                Cell::Bytes(v) => {
                    by = Some(v);
                    t(AttributeValueType::Bytes)
                }
                Cell::Map(v) => {
                    ser = v;
                    t(AttributeValueType::Map)
                }
                Cell::Slice(v) => {
                    ser = Some(v);
                    t(AttributeValueType::Slice)
                }
                Cell::Typed(code) => code,
            };
            self.ty.push(Some(ty));
            self.str_.push(s);
            self.int.push(i);
            self.double.push(d);
            self.bool_.push(b);
            self.bytes.push(by);
            self.ser.push(ser);
        }

        fn build_where(
            &self,
            valid: impl Fn(usize) -> bool,
            limits: &Limits,
            budget: &mut Budget,
        ) -> Result<AnyValueColumns, LakeError> {
            let binary = |cells: &[Option<Vec<u8>>]| -> ArrayRef {
                Arc::new(BinaryArray::from_iter(cells.iter().map(|c| c.as_deref())))
            };
            let ty: ArrayRef = Arc::new(UInt8Array::from(self.ty.clone()));
            let str_: ArrayRef = Arc::new(StringArray::from_iter(
                self.str_.iter().map(|c| c.as_deref()),
            ));
            let int: ArrayRef = Arc::new(Int64Array::from(self.int.clone()));
            let double: ArrayRef = Arc::new(Float64Array::from(self.double.clone()));
            let bool_: ArrayRef = Arc::new(BooleanArray::from(self.bool_.clone()));
            let (bytes, ser) = (binary(&self.bytes), binary(&self.ser));
            AnyValueColumns::new(
                self.ty.len(),
                |name| match name {
                    consts::ATTRIBUTE_TYPE => Some(ty.clone()),
                    consts::ATTRIBUTE_STR => Some(str_.clone()),
                    consts::ATTRIBUTE_INT => Some(int.clone()),
                    consts::ATTRIBUTE_DOUBLE => Some(double.clone()),
                    consts::ATTRIBUTE_BOOL => Some(bool_.clone()),
                    consts::ATTRIBUTE_BYTES => Some(bytes.clone()),
                    consts::ATTRIBUTE_SER => Some(ser.clone()),
                    _ => None,
                },
                valid,
                limits,
                budget,
            )
        }

        fn build(&self) -> AnyValueColumns {
            self.build_where(|_| true, &limits(), &mut Budget::new(1 << 30))
                .unwrap_or_else(|e| panic!("columns: {e}"))
        }

        fn refusal(&self, limits: &Limits) -> LakeError {
            self.build_where(|_| true, limits, &mut Budget::new(1 << 30))
                .err()
                .expect("refused")
        }
    }

    fn rendered(c: &AnyValueColumns) -> Vec<Option<String>> {
        let out = c
            .render_where(|_| true, &mut Budget::new(1 << 30))
            .expect("render");
        (0..out.len())
            .map(|i| (!out.is_null(i)).then(|| out.value(i).to_owned()))
            .collect()
    }

    fn canonical(c: &AnyValueColumns, i: usize) -> Vec<u8> {
        let mut out = Vec::new();
        c.canonical_into(i, &mut out);
        out
    }

    fn map_cbor() -> Vec<u8> {
        cbor(&ciborium::Value::Map(vec![(
            ciborium::Value::Text("a".into()),
            ciborium::Value::Integer(1.into()),
        )]))
    }

    /// Scenario: One row of every AnyValue type is rendered: a string, 42, 1.5, true, bytes de ad, a map {a: 1}, a slice [1, "x"] and an Empty value.
    /// Guarantees: Strings are stored raw, scalars as their JSON text, bytes as quoted padded base64, maps and slices as compact JSON, and Empty as SQL null, as Series Lake Format v1 renders them.
    #[test]
    fn every_any_value_type_renders_as_v1() {
        let slice = cbor(&ciborium::Value::Array(vec![
            ciborium::Value::Integer(1.into()),
            ciborium::Value::Text("x".into()),
        ]));
        let c = Rows::of(vec![
            Cell::Str("hello".into()),
            Cell::Int(42),
            Cell::Double(1.5),
            Cell::Bool(true),
            Cell::Bytes(vec![0xde, 0xad]),
            Cell::Map(Some(map_cbor())),
            Cell::Slice(slice),
            Cell::Typed(AttributeValueType::Empty as u8),
        ])
        .build();
        let want = [
            Some("hello"),
            Some("42"),
            Some("1.5"),
            Some("true"),
            Some("\"3q0=\""),
            Some("{\"a\":1}"),
            Some("[1,\"x\"]"),
            None,
        ];
        let got = rendered(&c);
        for (i, want) in want.iter().enumerate() {
            assert_eq!(got[i].as_deref(), *want, "row {i}");
        }
    }

    /// Scenario: The doubles 3.0, NaN, +Infinity, -Infinity and -0.0 are rendered.
    /// Guarantees: A whole double keeps its fraction, the non-finite ones are the quoted strings of the format, and negative zero keeps its sign.
    #[test]
    fn doubles_render_like_the_format() {
        let c = Rows::of(
            [3.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.0]
                .into_iter()
                .map(Cell::Double)
                .collect(),
        )
        .build();
        let got: Vec<String> = rendered(&c)
            .into_iter()
            .map(|s| s.expect("value"))
            .collect();
        assert_eq!(
            got,
            ["3.0", "\"NaN\"", "\"Infinity\"", "\"-Infinity\"", "-0.0"]
        );
    }

    /// Scenario: Typed rows have null value columns (Int, Str, Double, Bool, Bytes) next to rows holding the explicit defaults, plus an Empty row.
    /// Guarantees: Each typed null encodes and renders exactly like its explicit default, so an encoder that omits default values gives the same series id; Empty encodes as the null value and renders as null.
    #[test]
    fn typed_null_equals_default() {
        let t = |v: AttributeValueType| Cell::Typed(v as u8);
        let c = Rows::of(vec![
            t(AttributeValueType::Int),
            t(AttributeValueType::Str),
            t(AttributeValueType::Double),
            t(AttributeValueType::Bool),
            t(AttributeValueType::Bytes),
            Cell::Int(0),
            Cell::Str(String::new()),
            Cell::Double(0.0),
            Cell::Bool(false),
            Cell::Bytes(Vec::new()),
            t(AttributeValueType::Empty),
        ])
        .build();
        let got = rendered(&c);
        for i in 0..5 {
            assert_eq!(canonical(&c, i), canonical(&c, i + 5), "canonical row {i}");
            assert_eq!(got[i], got[i + 5], "render row {i}");
        }
        let text: Vec<&str> = got[..5]
            .iter()
            .map(|s| s.as_deref().expect("value"))
            .collect();
        assert_eq!(text, ["0", "", "0.0", "false", "\"\""]);
        assert_eq!(canonical(&c, 10), [0x06, 0, 0, 0, 0]);
        assert_eq!(got[10], None);
    }

    /// Scenario: An attrs table has only the `type` column (OTAP omits all-default value columns).
    /// Guarantees: The missing value column reads as the type default ("0" for Int) rather than an error or null.
    #[test]
    fn absent_optional_columns_render_type_defaults() {
        let ty: ArrayRef = Arc::new(UInt8Array::from(vec![AttributeValueType::Int as u8]));
        let c = AnyValueColumns::new(
            1,
            |name| (name == consts::ATTRIBUTE_TYPE).then(|| ty.clone()),
            |_| true,
            &limits(),
            &mut Budget::new(1 << 30),
        )
        .unwrap_or_else(|e| panic!("columns: {e}"));
        assert_eq!(rendered(&c), [Some("0".to_owned())]);
    }

    /// Scenario: A table holds (a) type code 99, (b) a Map row with a null `ser`, (c) a Map row whose `ser` is not CBOR, (d) a slice nested 3 deep with a depth limit of 2, (e) a 2 KiB string with a 1 KiB row limit, (f) rows but no type column.
    /// Guarantees: (a), (b), (c) and (f) are invalid content, (d) is too deep and carries the limit, (e) is too large and names `ingress.max_row_bytes`; none is rendered as a fallback string.
    #[test]
    fn invalid_values_are_refused() {
        let invalid = |cell: Cell| {
            let err = Rows::of(vec![cell]).refusal(&limits());
            assert!(matches!(err, LakeError::Invalid(_)), "{err}");
        };
        invalid(Cell::Typed(99));
        invalid(Cell::Map(None));
        invalid(Cell::Map(Some(vec![0xff, 0xff])));

        let one = ciborium::Value::Array(vec![ciborium::Value::Integer(1.into())]);
        let deep = ciborium::Value::Array(vec![ciborium::Value::Array(vec![one])]);
        let mut l = limits();
        l.max_nesting_depth = 2;
        let err = Rows::of(vec![Cell::Slice(cbor(&deep))]).refusal(&l);
        assert!(matches!(err, LakeError::TooDeep(2)), "{err}");

        let mut l = limits();
        l.max_row_bytes = 1024;
        let err = Rows::of(vec![Cell::Str("x".repeat(2048))]).refusal(&l);
        assert!(matches!(err, LakeError::TooLarge { .. }), "{err}");
        assert!(err.to_string().contains("ingress.max_row_bytes"), "{err}");

        let err = AnyValueColumns::new(1, |_| None, |_| true, &limits(), &mut Budget::new(1 << 30))
            .err()
            .expect("refused");
        assert!(matches!(err, LakeError::Invalid(_)), "{err}");
    }

    /// Scenario: 100 rows of a 1 KiB string are rendered with a 50 KiB request budget.
    /// Guarantees: Rendering charges every cell it writes and is refused by `ingress.max_extracted_bytes` instead of building the whole column.
    #[test]
    fn rendering_is_charged_to_the_budget() {
        let rows = Rows::of((0..100).map(|_| Cell::Str("x".repeat(1024))).collect());
        let mut budget = Budget::new(50 * 1024);
        let c = rows
            .build_where(|_| true, &limits(), &mut budget)
            .unwrap_or_else(|e| panic!("columns: {e}"));
        let err = c.render_where(|_| true, &mut budget).expect_err("refused");
        assert!(
            err.to_string().contains("ingress.max_extracted_bytes"),
            "{err}"
        );
    }

    /// Scenario: Row 0 holds the unknown type code 99 and is masked out by the validity predicate (its parent struct is null); row 1 holds 42.
    /// Guarantees: The masked row is not validated and renders as null; the other row renders normally.
    #[test]
    fn render_where_masks_invalid_rows() {
        let rows = Rows::of(vec![Cell::Typed(99), Cell::Int(42)]);
        let mut budget = Budget::new(1 << 30);
        let c = rows
            .build_where(|i| i != 0, &limits(), &mut budget)
            .unwrap_or_else(|e| panic!("columns: {e}"));
        let out = c.render_where(|i| i != 0, &mut budget).expect("render");
        assert!(out.is_null(0));
        assert_eq!(out.value(1), "42");
    }
}
