// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Grouping of an OTAP attribute table by `parent_id`: `MAP<STRING, STRING>` columns and canonical
//! key/value lists for the series identity (docs/FORMAT.md sections 1 and 2).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, MapArray, RecordBatch, StringArray, StructArray, UInt32Array};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::{sort_to_indices, take};
use arrow::datatypes::{DataType, Field, Fields};
use otel_arrow_dfe_pdata::schema::consts;

use super::anyvalue::AnyValueColumns;
use super::canonical::{begin_kvlist, end_kvlist, put_str};
use super::columns::{ids_u32, utf8_or_empty};
use super::error::LakeError;
use super::limits::{Budget, Limits};

/// Memory per attribute row of the index and of the typed value columns (parent id, order entry,
/// share of the group map, int/double/bool cells), charged to the request budget.
const ATTR_ROW_OVERHEAD: usize = 32;

// Calls of `AttrIndex::rendered` on this thread (tests check that a lookup is not repeated per
// row).
#[cfg(test)]
thread_local! {
    pub static RENDERED_LOOKUPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn entries_field() -> Arc<Field> {
    Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
        ])),
        false,
    ))
}

/// `MAP<STRING, STRING>` with non-null keys and nullable values, as every v1 map column.
#[must_use]
pub fn map_type() -> DataType {
    DataType::Map(entries_field(), false)
}

/// A required map column.
#[must_use]
pub fn map_field(name: &str) -> Field {
    Field::new(name, map_type(), false)
}

/// The attribute rows of one parent: a range of `AttrIndex::order`.
#[derive(Clone, Copy)]
struct Group {
    start: u32,
    end: u32,
    /// Bytes one map row of this parent copies: keys, rendered values and 8 bytes per entry.
    bytes: usize,
}

/// Attribute rows of one OTAP attrs table, grouped by parent id and sorted by key.
pub struct AttrIndex {
    keys: StringArray,
    /// Rendered values (map cells).
    values: StringArray,
    /// Typed values (canonical identity encoding).
    typed: AnyValueColumns,
    /// Attr row indices grouped by parent id; within a parent, ascending raw key bytes.
    order: UInt32Array,
    /// parent id -> its rows in `order`.
    groups: HashMap<u32, Group>,
}

impl AttrIndex {
    /// Index of an absent table: every parent has no attributes.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            keys: StringArray::from(Vec::<&str>::new()),
            values: StringArray::from(Vec::<Option<&str>>::new()),
            typed: AnyValueColumns::empty(),
            order: UInt32Array::from(Vec::<u32>::new()),
            groups: HashMap::new(),
        }
    }

    /// Build from an optional attrs table (`None` when the payload has no such table).
    ///
    /// # Errors
    /// Invalid content for a null parent id, a duplicate key under one parent, or an invalid
    /// value (see [`AnyValueColumns::new`]); too large when a key or the budget is exceeded.
    pub fn from_batch(
        batch: Option<&RecordBatch>,
        table: &'static str,
        limits: &Limits,
        budget: &mut Budget,
    ) -> Result<Self, LakeError> {
        let Some(batch) = batch else {
            return Ok(Self::empty());
        };
        let n = batch.num_rows();
        // The index and the typed value columns grow with the row count.
        budget.charge(n.saturating_mul(ATTR_ROW_OVERHEAD))?;
        let parents = ids_u32(
            batch.column_by_name(consts::PARENT_ID),
            table,
            consts::PARENT_ID,
        )?;
        if parents.null_count() > 0 {
            return Err(LakeError::Invalid(
                "attribute batch has a null parent_id".into(),
            ));
        }
        // A null key is the empty key.
        let keys = utf8_or_empty(batch.column_by_name(consts::ATTRIBUTE_KEY), n, budget)?;
        for i in 0..n {
            limits.check_cell(keys.value(i).len())?;
        }
        let typed = AnyValueColumns::new(
            n,
            |name| batch.column_by_name(name).cloned(),
            |_| true,
            limits,
            budget,
        )?;
        let values = typed.render_where(|_| true, budget)?;

        let sorted = sort_to_indices(&parents, None, None)?;
        let mut order: Vec<u32> = sorted.values().to_vec();
        let mut groups = HashMap::new();
        let mut start = 0usize;
        while start < order.len() {
            let pid = parents.value(order[start] as usize);
            let mut end = start + 1;
            while end < order.len() && parents.value(order[end] as usize) == pid {
                end += 1;
            }
            let key = |row: &u32| keys.value(*row as usize).as_bytes();
            let group = &mut order[start..end];
            group.sort_unstable_by(|a, b| key(a).cmp(key(b)));
            if group.windows(2).any(|w| key(&w[0]) == key(&w[1])) {
                return Err(LakeError::Invalid("duplicate attribute key".into()));
            }
            let bytes = group
                .iter()
                .map(|&row| {
                    keys.value_length(row as usize) as usize
                        + values.value_length(row as usize) as usize
                        + 8
                })
                .sum();
            let _ = groups.insert(
                pid,
                Group {
                    start: to_u32(start),
                    end: to_u32(end),
                    bytes,
                },
            );
            start = end;
        }
        Ok(Self {
            keys,
            values,
            typed,
            order: UInt32Array::from(order),
            groups,
        })
    }

    /// Attr rows of a group, in key order.
    fn rows(&self, group: Group) -> &[u32] {
        &self.order.values()[group.start as usize..group.end as usize]
    }

    /// Build a map column with one entry list per output row. `parents[i]` is the parent id of
    /// output row `i` (null or unknown -> empty map, never a null map). A parent referenced by
    /// many output rows is copied for each of them, so every output row is charged its parent's
    /// bytes before its entries are gathered.
    pub fn build_map(
        &self,
        parents: &UInt32Array,
        budget: &mut Budget,
    ) -> Result<ArrayRef, LakeError> {
        let mut gather: Vec<u32> = Vec::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(parents.len() + 1);
        offsets.push(0);
        for i in 0..parents.len() {
            if !parents.is_null(i)
                && let Some(group) = self.groups.get(&parents.value(i))
            {
                budget.charge(group.bytes)?;
                gather.extend_from_slice(self.rows(*group));
            }
            offsets.push(
                i32::try_from(gather.len()).map_err(|_| {
                    LakeError::Conversion("attribute map exceeds i32 offsets".into())
                })?,
            );
        }
        let idx = UInt32Array::from(gather);
        let keys = take(&self.keys, &idx, None)?;
        let values = take(&self.values, &idx, None)?;
        let entries_field = entries_field();
        let DataType::Struct(entry_fields) = entries_field.data_type() else {
            return Err(LakeError::Conversion("map entries are not a struct".into()));
        };
        let entries = StructArray::try_new(entry_fields.clone(), vec![keys, values], None)?;
        Ok(Arc::new(MapArray::try_new(
            entries_field,
            OffsetBuffer::new(offsets.into()),
            entries,
            None::<NullBuffer>,
            false,
        )?))
    }

    /// Append the canonical kvlist of `parent` (empty when None or unknown). Rows are already
    /// sorted by raw key bytes with unique keys. The cost is the size of the parent's attributes;
    /// callers bound how often they encode one parent (see `extract.rs`).
    pub fn kvlist_into(&self, parent: Option<u32>, out: &mut Vec<u8>) {
        let rows = parent
            .and_then(|p| self.groups.get(&p))
            .map_or(&[][..], |g| self.rows(*g));
        let at = begin_kvlist(out, rows.len());
        for &row in rows {
            let row = row as usize;
            put_str(out, self.keys.value(row));
            self.typed.canonical_into(row, out);
        }
        end_kvlist(out, at);
    }

    /// The rendered value of attribute `key` of `parent`; `""` when the parent, the key or the
    /// value is absent (the `producer_id` projection). A binary search over the parent's
    /// key-sorted rows.
    #[must_use]
    pub fn rendered(&self, parent: Option<u32>, key: &str) -> &str {
        #[cfg(test)]
        RENDERED_LOOKUPS.with(|n| n.set(n.get() + 1));
        let Some(group) = parent.and_then(|p| self.groups.get(&p)) else {
            return "";
        };
        let rows = self.rows(*group);
        let at =
            rows.partition_point(|&row| self.keys.value(row as usize).as_bytes() < key.as_bytes());
        match rows.get(at) {
            Some(&row)
                if self.keys.value(row as usize) == key && !self.values.is_null(row as usize) =>
            {
                self.values.value(row as usize)
            }
            _ => "",
        }
    }
}

fn to_u32(v: usize) -> u32 {
    // Attribute tables are indexed by u32 parent ids and bounded by Arrow i32 offsets.
    u32::try_from(v).expect("attribute table row index fits in u32")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::parquet_lake_exporter::canonical::encode_value;
    use crate::exporters::parquet_lake_exporter::value::Value;
    use arrow::array::{Int64Array, UInt8Array};
    use arrow::datatypes::Schema;
    use otel_arrow_dfe_pdata::otlp::attributes::AttributeValueType;

    fn limits() -> Limits {
        Limits {
            max_extracted_bytes: 1 << 30,
            max_row_bytes: 1 << 20,
            max_nesting_depth: 32,
            max_chunk_bytes: 1 << 30,
        }
    }

    /// An attribute value of the fixture table.
    #[derive(Clone, Copy)]
    enum V<'a> {
        Str(&'a str),
        Int(i64),
    }

    /// An attrs table of `(parent_id, key, value)` rows; a `None` parent or key is a null cell.
    fn attrs_of(rows: &[(Option<u32>, Option<&str>, V<'_>)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(consts::PARENT_ID, DataType::UInt32, true),
            Field::new(consts::ATTRIBUTE_KEY, DataType::Utf8, true),
            Field::new(consts::ATTRIBUTE_TYPE, DataType::UInt8, false),
            Field::new(consts::ATTRIBUTE_STR, DataType::Utf8, true),
            Field::new(consts::ATTRIBUTE_INT, DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from_iter(rows.iter().map(|r| r.0))),
                Arc::new(StringArray::from_iter(rows.iter().map(|r| r.1))),
                Arc::new(UInt8Array::from_iter_values(rows.iter().map(
                    |r| match r.2 {
                        V::Str(_) => AttributeValueType::Str as u8,
                        V::Int(_) => AttributeValueType::Int as u8,
                    },
                ))),
                Arc::new(StringArray::from_iter(rows.iter().map(|r| match r.2 {
                    V::Str(s) => Some(s),
                    V::Int(_) => None,
                }))),
                Arc::new(Int64Array::from_iter(rows.iter().map(|r| match r.2 {
                    V::Str(_) => None,
                    V::Int(i) => Some(i),
                }))),
            ],
        )
        .expect("attrs batch")
    }

    fn attrs(rows: &[(u32, &str, &str)]) -> RecordBatch {
        let rows: Vec<_> = rows
            .iter()
            .map(|r| (Some(r.0), Some(r.1), V::Str(r.2)))
            .collect();
        attrs_of(&rows)
    }

    fn index(batch: &RecordBatch) -> AttrIndex {
        AttrIndex::from_batch(Some(batch), "t", &limits(), &mut Budget::new(1 << 30))
            .unwrap_or_else(|e| panic!("index: {e}"))
    }

    fn refusal(batch: &RecordBatch) -> LakeError {
        AttrIndex::from_batch(Some(batch), "t", &limits(), &mut Budget::new(1 << 30))
            .err()
            .expect("refused")
    }

    fn map_entries(map: &ArrayRef, row: usize) -> Vec<(String, String)> {
        let map = map.as_any().downcast_ref::<MapArray>().expect("map");
        let entries = map.value(row);
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("keys");
        let values = entries
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("values");
        (0..keys.len())
            .map(|i| (keys.value(i).to_owned(), values.value(i).to_owned()))
            .collect()
    }

    fn kvlist(index: &AttrIndex, parent: Option<u32>) -> Vec<u8> {
        let mut out = Vec::new();
        index.kvlist_into(parent, &mut out);
        out
    }

    fn pairs(entries: &[(&str, &str)]) -> Vec<(String, String)> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    /// Scenario: Attribute rows arrive for parents 2, 1, 2 in that order, and three output rows reference parents 2, 1, 2.
    /// Guarantees: Each output map holds exactly its parent's attributes sorted by key; the column has the v1 map type and no null map.
    #[test]
    fn grouping_of_unsorted_parents_builds_per_row_maps() {
        let batch = attrs(&[(2, "c", "3"), (1, "a", "1"), (2, "b", "2")]);
        let index = index(&batch);
        let map = index
            .build_map(&UInt32Array::from(vec![2, 1, 2]), &mut Budget::new(1 << 30))
            .expect("map");
        assert_eq!(map.data_type(), &map_type());
        assert_eq!(map.data_type(), map_field("attrs").data_type());
        assert_eq!(map.null_count(), 0);
        assert_eq!(map_entries(&map, 0), pairs(&[("b", "2"), ("c", "3")]));
        assert_eq!(map_entries(&map, 1), pairs(&[("a", "1")]));
        assert_eq!(map_entries(&map, 2), pairs(&[("b", "2"), ("c", "3")]));
    }

    /// Scenario: Output rows have a null parent id and an unknown parent id.
    /// Guarantees: Both get an empty, non-null map and the canonical encoding of an empty key/value list (tag 08, length 4, count 0).
    #[test]
    fn null_or_unknown_parent_yields_empty_map_and_empty_kvlist() {
        let batch = attrs(&[(1, "a", "1")]);
        let index = index(&batch);
        let map = index
            .build_map(
                &UInt32Array::from(vec![None, Some(99)]),
                &mut Budget::new(1 << 30),
            )
            .expect("map");
        assert_eq!(map.null_count(), 0);
        assert!(map_entries(&map, 0).is_empty());
        assert!(map_entries(&map, 1).is_empty());
        let empty = [0x08, 0, 0, 0, 4, 0, 0, 0, 0];
        assert_eq!(kvlist(&index, None), empty);
        assert_eq!(kvlist(&index, Some(99)), empty);
        assert_eq!(kvlist(&AttrIndex::empty(), Some(1)), empty);
    }

    /// Scenario: The keys `b`, `a` and `B` are stored in different row orders under two parents.
    /// Guarantees: Both parents encode to the same bytes, ordered by raw key bytes (`B`, `a`, `b`), equal to the canonical encoding of that key/value list; so a series id does not depend on attribute row order.
    #[test]
    fn kvlist_is_sorted_by_key_bytes_and_ignores_row_order() {
        let batch = attrs(&[
            (1, "b", "2"),
            (1, "a", "1"),
            (1, "B", "0"),
            (2, "B", "0"),
            (2, "b", "2"),
            (2, "a", "1"),
        ]);
        let index = index(&batch);
        assert_eq!(kvlist(&index, Some(1)), kvlist(&index, Some(2)));
        let mut want = Vec::new();
        encode_value(
            &mut want,
            &Value::KvList(vec![
                ("B".into(), Value::Str("0".into())),
                ("a".into(), Value::Str("1".into())),
                ("b".into(), Value::Str("2".into())),
            ]),
        );
        assert_eq!(kvlist(&index, Some(1)), want);
    }

    /// Scenario: Parent 3 has the key `k` twice; parent 4 has it once.
    /// Guarantees: The whole table is refused as invalid content instead of picking one of the two values.
    #[test]
    fn duplicate_key_is_refused() {
        let batch = attrs(&[(3, "k", "y"), (4, "k", "x"), (3, "k", "x")]);
        let err = refusal(&batch);
        assert!(
            matches!(&err, LakeError::Invalid(msg) if msg == "duplicate attribute key"),
            "{err}"
        );
    }

    /// Scenario: (a) the parent id column holds a null; (b) the key column holds a null.
    /// Guarantees: (a) is refused as invalid content naming parent_id, because the attribute belongs to no row; (b) keeps the entry under the empty key.
    #[test]
    fn null_parent_id_is_refused_and_null_key_is_empty_key() {
        let batch = attrs_of(&[
            (Some(1), Some("a"), V::Str("1")),
            (None, Some("b"), V::Str("2")),
        ]);
        let err = refusal(&batch);
        assert!(matches!(err, LakeError::Invalid(_)), "{err}");
        assert!(err.to_string().contains("parent_id"), "{err}");

        let batch = attrs_of(&[
            (Some(1), Some("a"), V::Str("1")),
            (Some(1), None, V::Str("2")),
        ]);
        let index = index(&batch);
        let map = index
            .build_map(&UInt32Array::from(vec![1]), &mut Budget::new(1 << 30))
            .expect("map");
        assert_eq!(map_entries(&map, 0), pairs(&[("", "2"), ("a", "1")]));
    }

    /// Scenario: Parent 1 has the keys `B`, `a`, `host.id`=a1, `n`=7 (an int) and `z`; parent 2 has no attributes.
    /// Guarantees: The lookup finds every present key, including the first and the last of the sorted rows, and renders a non-string value; it gives "" for a key before, between and after the present keys, for a parent without attributes and for no parent.
    #[test]
    fn rendered_projects_one_attribute() {
        let batch = attrs_of(&[
            (Some(1), Some("z"), V::Str("last")),
            (Some(1), Some("host.id"), V::Str("a1")),
            (Some(1), Some("B"), V::Str("first")),
            (Some(1), Some("n"), V::Int(7)),
            (Some(1), Some("a"), V::Str("x")),
        ]);
        let index = index(&batch);
        assert_eq!(index.rendered(Some(1), "host.id"), "a1");
        assert_eq!(index.rendered(Some(1), "n"), "7");
        assert_eq!(index.rendered(Some(1), "B"), "first");
        assert_eq!(index.rendered(Some(1), "z"), "last");
        for absent in ["A", "host", "host.ida", "zz"] {
            assert_eq!(index.rendered(Some(1), absent), "", "{absent}");
        }
        assert_eq!(index.rendered(Some(2), "host.id"), "");
        assert_eq!(index.rendered(None, "host.id"), "");
    }

    /// Scenario: 1000 output rows reference one parent whose value is 1 KiB long, with a 100 KiB request budget.
    /// Guarantees: The map is refused by `ingress.max_extracted_bytes`, and the size observed at the refusal is below 102 KiB: gathering stops at the output row that passes the budget instead of collecting all 1000 rows first.
    #[test]
    fn build_map_is_charged() {
        let value = "x".repeat(1024);
        let batch = attrs(&[(1, "k", value.as_str())]);
        let mut budget = Budget::new(100 * 1024);
        let index = AttrIndex::from_batch(Some(&batch), "t", &limits(), &mut budget)
            .unwrap_or_else(|e| panic!("index: {e}"));
        let err = index
            .build_map(&UInt32Array::from(vec![1; 1000]), &mut budget)
            .expect_err("refused");
        match err {
            LakeError::TooLarge {
                setting, observed, ..
            } => {
                assert_eq!(setting, "ingress.max_extracted_bytes");
                assert!(observed < 102 * 1024, "{observed}");
            }
            other => panic!("expected a size refusal, got {other}"),
        }
    }
}
