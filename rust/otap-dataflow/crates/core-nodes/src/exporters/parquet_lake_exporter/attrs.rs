// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Grouping of an OTAP attribute table by `parent_id`: vectorized construction of
//! `Map<Utf8, Utf8>` columns and canonical attribute lists for series identity.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, MapArray, RecordBatch, StringArray, StructArray, UInt32Array};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::{sort_to_indices, take};
use arrow::datatypes::{DataType, Field, Fields};
use otel_arrow_dfe_pdata::schema::consts;

use super::anyvalue::AnyValueColumns;
use super::columns::{cast_or_null, ids_u32};
use super::error::LakeError;
use super::identity::put_attrs;

/// Map field layout shared by every output schema.
#[must_use]
pub fn map_field(name: &str) -> Field {
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ])),
        false,
    );
    Field::new(name, DataType::Map(Arc::new(entries), false), true)
}

/// Attribute rows of one OTAP attrs table, grouped by parent id.
pub struct AttrIndex {
    keys: StringArray,
    /// Rendered values (Map<Utf8,Utf8> output).
    values: StringArray,
    /// Typed values (canonical identity encoding).
    typed: AnyValueColumns,
    /// Attr row indices grouped by parent id, one row per (parent, key).
    order: UInt32Array,
    /// parent id -> [start, end) range into `order`.
    ranges: HashMap<u32, (u32, u32)>,
}

impl AttrIndex {
    /// Build from an optional attrs table (`None` when the payload has no such table).
    pub fn from_batch(batch: Option<&RecordBatch>, table: &'static str) -> Result<Self, LakeError> {
        let Some(batch) = batch else {
            return Ok(Self {
                keys: StringArray::from(Vec::<String>::new()),
                values: StringArray::from(Vec::<Option<String>>::new()),
                typed: AnyValueColumns::new(0, |_| None)?,
                order: UInt32Array::from(Vec::<u32>::new()),
                ranges: HashMap::new(),
            });
        };
        let n = batch.num_rows();
        let parents = ids_u32(
            batch.column_by_name(consts::PARENT_ID),
            table,
            consts::PARENT_ID,
        )?;
        let keys = cast_or_null(
            batch.column_by_name(consts::ATTRIBUTE_KEY),
            &DataType::Utf8,
            n,
        )?;
        let keys = keys
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("cast to Utf8")
            .clone();
        let typed = AnyValueColumns::new(n, |name| batch.column_by_name(name).cloned())?;
        let values = typed.render_all();

        // Group rows by parent and keep one row per key: the smallest encoded value wins, exactly
        // as in the identity (`put_attrs`), so rendered maps have unique keys that match the id.
        let sorted = sort_to_indices(&parents, None, None)?;
        let mut order: Vec<u32> = Vec::with_capacity(n);
        let mut ranges = HashMap::new();
        let mut start = 0usize;
        while start < sorted.len() {
            let pid = parents.value(sorted.value(start) as usize);
            let mut end = start + 1;
            while end < sorted.len() && parents.value(sorted.value(end) as usize) == pid {
                end += 1;
            }
            let first = order.len();
            let mut rows: Vec<u32> = (start..end)
                .map(|j| sorted.value(j))
                .filter(|&r| !keys.is_null(r as usize))
                .collect();
            rows.sort_unstable_by(|a, b| keys.value(*a as usize).cmp(keys.value(*b as usize)));
            for group in rows.chunk_by(|a, b| keys.value(*a as usize) == keys.value(*b as usize)) {
                let mut best: Option<(Vec<u8>, u32)> = None;
                if let [row] = group {
                    best = Some((Vec::new(), *row));
                } else {
                    for &row in group {
                        let mut v = Vec::new();
                        typed.canonical_into(row as usize, &mut v);
                        if best.as_ref().is_none_or(|(b, _)| v < *b) {
                            best = Some((v, row));
                        }
                    }
                }
                order.extend(best.map(|(_, row)| row));
            }
            let _ = ranges.insert(pid, (to_u32(first), to_u32(order.len())));
            start = end;
        }
        let order = UInt32Array::from(order);
        Ok(Self {
            keys,
            values,
            typed,
            order,
            ranges,
        })
    }

    /// Attr rows of `parent` (non-null, unique keys).
    fn rows_of(&self, parent: u32) -> impl Iterator<Item = usize> + '_ {
        let (s, e) = self.ranges.get(&parent).copied().unwrap_or((0, 0));
        (s..e).map(|j| self.order.value(j as usize) as usize)
    }

    /// Build a `Map<Utf8,Utf8>` column with one entry list per output row. `parents[i]` is the
    /// parent id of output row `i` (null -> empty map).
    pub fn build_map(&self, parents: &UInt32Array, field: &Field) -> Result<ArrayRef, LakeError> {
        let mut gather: Vec<u32> = Vec::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(parents.len() + 1);
        offsets.push(0);
        for i in 0..parents.len() {
            if !parents.is_null(i) {
                gather.extend(self.rows_of(parents.value(i)).map(to_u32));
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
        let DataType::Map(entries_field, sorted) = field.data_type() else {
            return Err(LakeError::Conversion(format!(
                "field `{}` is not a map",
                field.name()
            )));
        };
        let DataType::Struct(entry_fields) = entries_field.data_type() else {
            return Err(LakeError::Conversion(format!(
                "map field `{}` entries are not a struct",
                field.name()
            )));
        };
        let entries = StructArray::try_new(entry_fields.clone(), vec![keys, values], None)?;
        let map = MapArray::try_new(
            entries_field.clone(),
            OffsetBuffer::new(offsets.into()),
            entries,
            None::<NullBuffer>,
            *sorted,
        )?;
        Ok(Arc::new(map))
    }

    /// Append the canonical attribute list of `parent` (an empty list when None or unknown). Row
    /// order does not matter: `put_attrs` sorts and resolves duplicate keys by value bytes.
    pub fn canonical_into(&self, parent: Option<u32>, out: &mut Vec<u8>) {
        let mut entries = Vec::new();
        if let Some(p) = parent {
            for row in self.rows_of(p) {
                let mut v = Vec::new();
                self.typed.canonical_into(row, &mut v);
                entries.push((self.keys.value(row).as_bytes(), v));
            }
        }
        put_attrs(out, entries);
    }
}

fn to_u32(v: usize) -> u32 {
    // Attribute tables are indexed by u32 parent ids and bounded by Arrow i32 offsets.
    u32::try_from(v).expect("attribute table row index fits in u32")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt8Array;
    use arrow::datatypes::Schema;
    use otel_arrow_dfe_pdata::otlp::attributes::AttributeValueType;

    fn attrs(rows: &[(u32, &str, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(consts::PARENT_ID, DataType::UInt32, false),
            Field::new(consts::ATTRIBUTE_KEY, DataType::Utf8, false),
            Field::new(consts::ATTRIBUTE_TYPE, DataType::UInt8, false),
            Field::new(consts::ATTRIBUTE_STR, DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(UInt8Array::from_iter_values(
                    rows.iter().map(|_| AttributeValueType::Str as u8),
                )),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
            ],
        )
        .expect("attrs batch")
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

    fn canonical(index: &AttrIndex, parent: Option<u32>) -> Vec<u8> {
        let mut out = Vec::new();
        index.canonical_into(parent, &mut out);
        out
    }

    /// Scenario: Attribute rows arrive with unsorted parent ids and three output rows reference them.
    /// Guarantees: Each output row's map holds exactly its parent's attributes and the map type matches map_field.
    #[test]
    fn grouping_of_unsorted_parents_builds_per_row_maps() {
        let batch = attrs(&[(2, "b", "2"), (1, "a", "1"), (2, "c", "3")]);
        let index = AttrIndex::from_batch(Some(&batch), "t").expect("index");
        let field = map_field("attributes");
        let map = index
            .build_map(&UInt32Array::from(vec![2, 1, 2]), &field)
            .expect("map");
        assert_eq!(map.data_type(), field.data_type());
        let mut row0 = map_entries(&map, 0);
        row0.sort();
        assert_eq!(
            row0,
            vec![("b".into(), "2".into()), ("c".into(), "3".into())]
        );
        assert_eq!(map_entries(&map, 1), vec![("a".into(), "1".into())]);
    }

    /// Scenario: Output rows have a null parent id and an unknown parent id.
    /// Guarantees: Both get an empty map and an empty canonical attribute list instead of an error.
    #[test]
    fn null_or_unknown_parent_yields_empty_map_and_empty_list() {
        let batch = attrs(&[(1, "a", "1")]);
        let index = AttrIndex::from_batch(Some(&batch), "t").expect("index");
        let map = index
            .build_map(&UInt32Array::from(vec![None, Some(99)]), &map_field("m"))
            .expect("map");
        assert!(map_entries(&map, 0).is_empty());
        assert!(map_entries(&map, 1).is_empty());
        assert_eq!(canonical(&index, None), 0u32.to_le_bytes());
        assert_eq!(canonical(&index, Some(99)), 0u32.to_le_bytes());
    }

    /// Scenario: The same attribute set is stored in different row orders under two parents, and a third parent has a duplicate key.
    /// Guarantees: The canonical list ignores row order, and a duplicate key keeps the entry with the smallest encoded value in any order, both in the identity and in the rendered map (unique keys).
    #[test]
    fn canonical_list_ignores_row_order_and_resolves_duplicates_by_value() {
        let batch = attrs(&[
            (1, "b", "2"),
            (1, "a", "1"),
            (2, "a", "1"),
            (2, "b", "2"),
            (3, "k", "y"),
            (3, "k", "x"),
            (4, "k", "x"),
        ]);
        let index = AttrIndex::from_batch(Some(&batch), "t").expect("index");
        assert_eq!(canonical(&index, Some(1)), canonical(&index, Some(2)));
        assert_eq!(canonical(&index, Some(3)), canonical(&index, Some(4)));
        let map = index
            .build_map(&UInt32Array::from(vec![3]), &map_field("m"))
            .expect("map");
        assert_eq!(map_entries(&map, 0), vec![("k".into(), "x".into())]);
    }
}
