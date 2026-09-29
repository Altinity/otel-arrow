// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Logs extraction.

use std::borrow::Cow;
use std::collections::HashSet;
use std::hash::{BuildHasher, Hash, Hasher};

use arrow::datatypes::{DataType, Int32Type, TimestampNanosecondType, UInt16Type};
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_dfe_pdata::schema::consts::{
    BODY, EVENT_NAME, FLAGS, ID, NAME, OBSERVED_TIME_UNIX_NANO, RESOURCE, SCHEMA_URL, SCOPE,
    SEVERITY_NUMBER, SEVERITY_TEXT, SPAN_ID, TIME_UNIX_NANO, TRACE_ID, VERSION,
};

use hashbrown::HashTable;

use super::{
    Budget, Col, DescriptorRow, ExtractStats, Extracted, MemoHasher, Producers, Rendering, RowSink,
    SharedLists, ValuesRow, any_value_col, attr_columns, attr_table, attrs_of, denorm_col,
    descriptor_row, entry_eq, fixed_ref, flags_at, hash_kv, identity, map_cell_reserving_into,
    plain, recycle, str_col, str_of, struct_child, timestamp_pair,
};
use crate::attrs::{Attr, AttrEntry, prim_at};
use crate::canonical::{Descriptor, SeriesId, Signal};
use crate::config::LakeConfig;
use crate::error::{Error, Result};
use crate::schema::{Dataset, denorm_columns};
use crate::value::Reservations as _;
#[cfg(test)]
use crate::value::Value;
use crate::value::{DecodeLimits, body_string_reserving, value_bytes};

/// Memo key for a logs series, borrowed from the request.
///
/// `(resource_id, scope_id)` alone is not enough: the row-level scope and schema
/// strings are part of the identity, and two rows can share a resource and scope
/// id while carrying different scope names or schema URLs. `None` ids are part
/// of the key too, so a record without attributes never collapses into parent 0.
/// The identity attributes are compared by content under the canonical value
/// rules of [`entry_eq`], so two keys are equal only when their descriptors
/// encode to the same identity.
#[derive(Debug, Clone, Copy)]
struct MemoKey<'k, 't, E: Attr = AttrEntry<'t>> {
    resource_id: Option<u32>,
    scope_id: Option<u32>,
    /// Resource schema URL, scope name, scope version and scope schema URL.
    strings: [&'t str; 4],
    identity: &'k [&'t E],
}

impl<E: Attr> MemoKey<'_, '_, E> {
    fn same(&self, other: &MemoKey<'_, '_, E>) -> bool {
        self.resource_id == other.resource_id
            && self.scope_id == other.scope_id
            && self.strings == other.strings
            && self.identity.len() == other.identity.len()
            && self
                .identity
                .iter()
                .zip(other.identity)
                .all(|(a, b)| entry_eq(a, b))
    }

    fn hash_with(&self, hasher: &MemoHasher) -> u64 {
        let mut state = hasher.build_hasher();
        self.resource_id.hash(&mut state);
        self.scope_id.hash(&mut state);
        self.strings.hash(&mut state);
        hash_kv(self.identity.iter().copied(), &mut state);
        state.finish()
    }
}

/// One memo entry: the key's owned identity list, its other parts, the
/// series and the key's hash.
struct MemoEntry<'t> {
    resource_id: Option<u32>,
    scope_id: Option<u32>,
    strings: [&'t str; 4],
    identity: Box<[&'t AttrEntry<'t>]>,
    series_id: SeriesId,
    hash: u64,
}

impl<'t> MemoEntry<'t> {
    fn key(&self) -> MemoKey<'_, 't> {
        MemoKey {
            resource_id: self.resource_id,
            scope_id: self.scope_id,
            strings: self.strings,
            identity: &self.identity,
        }
    }
}

pub(crate) fn extract_logs(
    records: &OtapArrowRecords,
    cfg: &LakeConfig,
    budget: &mut Budget,
) -> Result<Extracted> {
    let limits = DecodeLimits::new(cfg.ingress.max_nesting_depth, cfg.ingress.max_row_bytes);
    let mut stats = ExtractStats::default();
    let Some(logs) = records.get(ArrowPayloadType::Logs) else {
        return Ok(Extracted {
            signal: Signal::Logs,
            descriptors: vec![],
            values: vec![],
            pinned_bytes: 0,
            merge_key_bytes: 0,
            shared_bytes: 0,
            stats,
        });
    };
    let resource_columns = attr_columns(records, ArrowPayloadType::ResourceAttrs)?;
    let resource_attrs = attr_table(resource_columns.as_ref(), limits, budget)?;
    let scope_columns = attr_columns(records, ArrowPayloadType::ScopeAttrs)?;
    let scope_attrs = attr_table(scope_columns.as_ref(), limits, budget)?;
    let log_columns = attr_columns(records, ArrowPayloadType::LogAttrs)?;
    let log_attrs = attr_table(log_columns.as_ref(), limits, budget)?;

    let ts_ns = otel_arrow_dfe_pdata::schema::timestamp_nanosecond_type();
    let time = plain(logs, TIME_UNIX_NANO, &ts_ns)?;
    let observed = plain(logs, OBSERVED_TIME_UNIX_NANO, &ts_ns)?;
    let id = plain(logs, ID, &DataType::UInt16)?;
    let severity_number = plain(logs, SEVERITY_NUMBER, &DataType::Int32)?;
    let severity_text = plain(logs, SEVERITY_TEXT, &DataType::Utf8)?;
    let event_name = plain(logs, EVENT_NAME, &DataType::Utf8)?;
    let flags_col = plain(logs, FLAGS, &DataType::UInt32)?;
    let trace_id = plain(logs, TRACE_ID, &DataType::FixedSizeBinary(16))?;
    let span_id = plain(logs, SPAN_ID, &DataType::FixedSizeBinary(8))?;
    let res_id = struct_child(logs, RESOURCE, ID, &DataType::UInt16)?;
    let res_schema = struct_child(logs, RESOURCE, SCHEMA_URL, &DataType::Utf8)?;
    let scope_id = struct_child(logs, SCOPE, ID, &DataType::UInt16)?;
    let scope_name = struct_child(logs, SCOPE, NAME, &DataType::Utf8)?;
    let scope_version = struct_child(logs, SCOPE, VERSION, &DataType::Utf8)?;
    let scope_schema = plain(logs, SCHEMA_URL, &DataType::Utf8)?;
    let mut body = any_value_col(logs, BODY)?;

    let allow: &[String] = &cfg.logs.series_attributes;
    let values_denorm = denorm_columns(Dataset::LogsValues, cfg);
    let body_bytes = body
        .as_ref()
        .and_then(|b| b.str_bytes())
        .map(|bytes| (BODY, bytes));
    let mut sink = RowSink::new(
        Dataset::LogsValues,
        cfg,
        logs.num_rows(),
        body_bytes.as_slice(),
    )?;
    let mut descriptors: Vec<DescriptorRow> = Vec::new();
    let mut seen: HashSet<SeriesId> = HashSet::new();
    let mut resources = SharedLists::default();
    let mut scopes = SharedLists::default();
    let series_columns = cfg.series_columns(Signal::Logs);
    let max_series = cfg.max_series_per_request();
    let key_strings = [&res_schema, &scope_name, &scope_version, &scope_schema];
    let mut producers = Producers::new(cfg);
    let columns = sink.width();
    let hasher = MemoHasher::default();
    let mut memo: HashTable<MemoEntry<'_>> = HashTable::new();
    let mut identity_attrs: Vec<&AttrEntry<'_>> = Vec::new();
    let is_identity = |k: &str| allow.iter().any(|a| a == k);
    // One row's cells and residual map entries, kept across rows.
    let mut spare_cols: Vec<Col<'static>> = Vec::with_capacity(columns);
    let mut spare_map: Vec<(&'static str, crate::attrs::ValueRef<'static>)> = Vec::new();

    for row in 0..logs.num_rows() {
        let rid = prim_at::<UInt16Type>(&res_id, row).map(u32::from);
        let sid = prim_at::<UInt16Type>(&scope_id, row).map(u32::from);
        let lid = prim_at::<UInt16Type>(&id, row).map(u32::from);
        let resource = attrs_of(&resource_attrs, rid);
        let scope = attrs_of(&scope_attrs, sid);
        let all_attrs = attrs_of(&log_attrs, lid);
        identity_attrs.clear();
        identity_attrs.extend(all_attrs.iter().filter(|e| is_identity(e.key())));
        let key = MemoKey {
            resource_id: rid,
            scope_id: sid,
            strings: key_strings.map(|col| str_of(col, row)),
            identity: &identity_attrs,
        };
        let hash = key.hash_with(&hasher);
        let series_id = match memo.find(hash, |entry| entry.key().same(&key)) {
            Some(entry) => entry.series_id,
            None => {
                let [
                    resource_schema_url,
                    scope_name,
                    scope_version,
                    scope_schema_url,
                ] = key.strings.map(str::to_owned);
                let descriptor = Descriptor {
                    signal: Signal::Logs,
                    resource_attrs: resources.get(&resource_attrs, rid, budget)?,
                    resource_schema_url,
                    scope_name,
                    scope_version,
                    scope_schema_url,
                    scope_attrs: scopes.get(&scope_attrs, sid, budget)?,
                    metric: None,
                    attrs: identity_attrs
                        .iter()
                        .map(|entry| entry.to_owned_entry())
                        .collect(),
                };
                // Two memo keys can name the same series; the identity is
                // checked before the row is built, so a duplicate is neither
                // built nor charged.
                let identified = identity(&descriptor);
                let id = identified.1;
                if seen.insert(id) {
                    super::check_series_count(seen.len(), max_series)?;
                    descriptors.push(descriptor_row(
                        descriptor,
                        identified,
                        Dataset::LogsSeries,
                        series_columns,
                        cfg,
                        &mut stats,
                        budget,
                    )?);
                }
                let entry = MemoEntry {
                    resource_id: rid,
                    scope_id: sid,
                    strings: key.strings,
                    identity: identity_attrs.as_slice().into(),
                    series_id: id,
                    hash,
                };
                let _ = memo.insert_unique(hash, entry, |entry| entry.hash);
                id
            }
        };

        let t_ns = prim_at::<TimestampNanosecondType>(&time, row).unwrap_or(0);
        let o_ns = prim_at::<TimestampNanosecondType>(&observed, row).unwrap_or(0);
        let (t_ns, t_us) = timestamp_pair(t_ns, &mut stats);
        let (o_ns, o_us) = timestamp_pair(o_ns, &mut stats);
        // A string body is stored as it arrived and held at its length; any
        // other body is decoded, lives only until it is rendered, and its
        // rendering is held. Either holding becomes part of the row's charge.
        let borrowed_body = match &body {
            Some(b) => b.str_value_at(row, limits)?,
            None => None,
        };
        let body_str: Option<Cow<'_, str>> = match borrowed_body {
            Some(s) => {
                Rendering(budget).reserve(s.len())?;
                Some(Cow::Borrowed(s))
            }
            None => match &mut body {
                Some(b) => {
                    let value = b.value_at(row, limits, budget)?;
                    let rendered = body_string_reserving(&value, &mut Rendering(budget))?;
                    budget.uncharge(value_bytes(&value));
                    rendered.map(Cow::Owned)
                }
                None => None,
            },
        };
        let body_held = body_str.as_ref().map_or(0, |s| s.len());
        let severity_text_str = str_of(&severity_text, row);
        let event_name_str = str_of(&event_name, row);
        let producer = producers.get(rid, resource);
        // Charge everything the row actually retains, in the form it is stored
        // in: fixed cells, the rendered body, the rendered residual attribute
        // map, the denormalized strings and the projected producer id. The map
        // and the body are charged their rendered `String::len()`, because
        // base64 encoding and JSON escaping can make the stored cell several
        // times the size of the decoded value tree.
        let mut approx = 16 + 8 * 6 + 24 + 8;
        approx += body_held;
        approx += severity_text_str.len() + event_name_str.len() + producer.len();
        let residual = all_attrs.iter().filter(|e| !is_identity(e.key()));
        let (residual_cell, residual_bytes) = map_cell_reserving_into(
            residual,
            approx,
            budget,
            recycle(std::mem::take(&mut spare_map)),
        )?;
        approx += residual_bytes;
        let severity = prim_at::<Int32Type>(&severity_number, row).unwrap_or(0);
        let mut cols = recycle(std::mem::take(&mut spare_cols));
        cols.extend([
            Col::Fixed(Some(&series_id)),
            str_col(producer),
            Col::TsUs(t_us),
            Col::Int(t_ns),
            Col::TsUs(o_us),
            Col::Int(o_ns),
            Col::Int32(Some(severity)),
            str_col(severity_text_str),
            Col::Str(body_str),
            str_col(event_name_str),
            Col::Fixed(fixed_ref(&trace_id, row)),
            Col::Fixed(fixed_ref(&span_id, row)),
            Col::Int32(Some(flags_at(&flags_col, row))),
            residual_cell,
        ]);
        for d in &values_denorm {
            let (cell, bytes) = denorm_col(d, resource, scope, all_attrs, &mut stats);
            approx += bytes;
            cols.push(cell);
        }
        let mut row = ValuesRow {
            cols,
            approx_bytes: approx,
            held_bytes: body_held + residual_bytes,
        };
        sink.push(&row, budget)?;
        stats.rows += 1;
        if let Some(Col::Map(entries)) = row.cols.iter_mut().find(|c| matches!(c, Col::Map(_))) {
            spare_map = recycle(std::mem::take(entries));
        }
        spare_cols = recycle(row.cols);
    }
    if let Some(body) = body {
        body.release(budget);
    }
    let (batches, pinned_bytes, merge_key_bytes) = sink.finish(budget)?;
    let values = if batches.is_empty() {
        vec![]
    } else {
        vec![(Dataset::LogsValues, batches)]
    };
    if descriptors.is_empty() && !values.is_empty() {
        return Err(Error::internal("values without descriptors"));
    }
    Ok(Extracted {
        signal: Signal::Logs,
        descriptors,
        values,
        pinned_bytes,
        merge_key_bytes,
        shared_bytes: resources.bytes() + scopes.bytes(),
        stats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DenormType, Denormalize, LakeConfig};
    use crate::error::{Error, RefuseReason};
    use crate::extract::{extract, series_batch};
    use crate::schema::Dataset;
    use arrow::array::{Array, ArrayRef, AsArray, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, LogsData, ResourceLogs, ScopeLogs,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use otel_arrow_dfe_pdata::testing::round_trip::encode_logs;
    use std::sync::Arc;

    fn kv(k: &str, v: &str) -> KeyValue {
        KeyValue {
            key: k.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.into())),
            }),
        }
    }

    fn logs_data() -> LogsData {
        let rec = |t: u64, body: &str, attrs: Vec<KeyValue>| LogRecord {
            time_unix_nano: t,
            observed_time_unix_nano: t + 1,
            severity_number: 9,
            severity_text: "INFO".into(),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue(body.into())),
            }),
            attributes: attrs,
            event_name: "ev".into(),
            flags: 0x0000_0101,
            trace_id: vec![0xAA; 16],
            span_id: vec![0xBB; 8],
            ..Default::default()
        };
        LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", "h1"), kv("service.name", "svc")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![
                        rec(
                            1_000,
                            "a",
                            vec![kv("logger.name", "L1"), kv("request_id", "r1")],
                        ),
                        rec(2_000, "b", vec![kv("logger.name", "L1")]),
                        rec(0, "c", vec![kv("logger.name", "L2")]),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn cfg() -> LakeConfig {
        let mut cfg = LakeConfig::default();
        cfg.logs.series_attributes = vec!["logger.name".into()];
        cfg.logs.denormalize = vec![
            Denormalize {
                path: "resource.service.name".into(),
                column: "service_name".into(),
                ty: DenormType::String,
            },
            Denormalize {
                path: "attrs.request_id".into(),
                column: "request_id".into(),
                ty: DenormType::String,
            },
        ];
        cfg
    }

    /// Scenario: three log records, two distinct logger.name values, one zero timestamp.
    /// Guarantees: two descriptors, three values rows, allow-listed attrs out of the residual map,
    /// a null timestamp and filled denormalized columns.
    #[test]
    fn extracts_logs_descriptors_and_values() {
        let mut records = encode_logs(&logs_data());
        let out = extract(&mut records, &cfg()).expect("extract");
        assert_eq!(out.signal, Signal::Logs);
        assert_eq!(out.descriptors.len(), 2);
        let (ds, batches) = &out.values[0];
        assert_eq!(*ds, Dataset::LogsValues);
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
        let b = &batches[0];
        let ids = b
            .column_by_name("series_id")
            .expect("series_id")
            .as_fixed_size_binary();
        assert_eq!(ids.value(0), ids.value(1));
        assert_ne!(ids.value(0), ids.value(2));
        let producer = b
            .column_by_name("producer_id")
            .expect("p")
            .as_string::<i32>();
        assert_eq!(producer.value(0), "h1");
        let time = b.column_by_name("time_unix_nano").expect("t");
        assert!(time.is_null(2));
        let svc = b
            .column_by_name("service_name")
            .expect("svc")
            .as_string::<i32>();
        assert_eq!(svc.value(0), "svc");
        let rid = b
            .column_by_name("request_id")
            .expect("rid")
            .as_string::<i32>();
        assert_eq!(rid.value(0), "r1");
        assert!(rid.is_null(1));
        let attrs = b.column_by_name("attrs").expect("attrs").as_map();
        // row 0 residual attrs: only request_id (logger.name is identity)
        assert_eq!(attrs.value_length(0), 1);
        let body = b.column_by_name("body").expect("body").as_string::<i32>();
        assert_eq!(body.value(0), "a");
        assert_eq!(body.value(1), "b");
        let sev_num = b
            .column_by_name("severity_number")
            .expect("sev num")
            .as_primitive::<Int32Type>();
        assert_eq!(sev_num.value(0), 9);
        let sev_text = b
            .column_by_name("severity_text")
            .expect("sev text")
            .as_string::<i32>();
        assert_eq!(sev_text.value(0), "INFO");
        let event = b
            .column_by_name("event_name")
            .expect("event_name")
            .as_string::<i32>();
        assert_eq!(event.value(0), "ev");
        let trace = b
            .column_by_name("trace_id")
            .expect("trace_id")
            .as_fixed_size_binary();
        assert_eq!(trace.value(0), [0xAA; 16]);
        let span = b
            .column_by_name("span_id")
            .expect("span_id")
            .as_fixed_size_binary();
        assert_eq!(span.value(0), [0xBB; 8]);
        let flags = b
            .column_by_name("flags")
            .expect("flags")
            .as_primitive::<Int32Type>();
        assert_eq!(flags.value(0), 0x0000_0101);
        assert_eq!(out.stats.rows, 3);
        assert!(out.pinned_bytes > 0);
        // descriptor denorm: only the identity-path column (service_name)
        assert_eq!(out.descriptors[0].denorm.len(), 1);
    }

    /// Scenario: memo keys with `0.0` against `-0.0`, NaNs of different bits, and keys differing in
    /// one scope string or the resource id.
    /// Guarantees: keys the canonical encoding cannot tell apart are equal and hash alike; others
    /// differ.
    #[test]
    fn memo_keys_follow_the_canonical_identity() {
        let hasher = MemoHasher::default();
        let entry = |v: f64| ("logger.name".to_owned(), Value::Double(v));
        let (zero, minus_zero) = (entry(0.0), entry(-0.0));
        let (nan, other_nan) = (
            entry(f64::NAN),
            entry(f64::from_bits(0xFFF8_0000_0000_0001)),
        );
        let key = |identity: &[&(String, Value)], version: &'static str, rid: u32| {
            MemoKey {
                resource_id: Some(rid),
                scope_id: Some(0),
                strings: ["", "scope", version, ""],
                identity,
            }
            .hash_with(&hasher)
        };
        let same = |a: &[&(String, Value)], b: &[&(String, Value)]| {
            let k = |identity| MemoKey {
                resource_id: Some(1),
                scope_id: Some(0),
                strings: ["", "scope", "1", ""],
                identity,
            };
            k(a).same(&k(b))
        };
        assert!(same(&[&zero], &[&minus_zero]));
        assert_eq!(key(&[&zero], "1", 1), key(&[&minus_zero], "1", 1));
        assert!(same(&[&nan], &[&other_nan]));
        assert_eq!(key(&[&nan], "1", 1), key(&[&other_nan], "1", 1));
        assert!(!same(&[&zero], &[&nan]));
        assert!(!same(&[&zero], &[]));
        let versioned = |version, rid| MemoKey {
            resource_id: Some(rid),
            scope_id: Some(0),
            strings: ["", "scope", version, ""],
            identity: &[] as &[&(String, Value)],
        };
        assert!(!versioned("1", 1).same(&versioned("2", 1)));
        assert!(!versioned("1", 1).same(&versioned("1", 2)));
        assert!(versioned("1", 1).same(&versioned("1", 1)));
    }

    /// Scenario: eight records in two scopes differing in version, identity `0.0`, `-0.0` and two
    /// NaNs, each with its own residual `request_id`.
    /// Guarantees: four series, one per scope and canonical value, each its descriptor's id.
    #[test]
    fn the_logs_memo_resolves_series_by_canonical_content() {
        let double = |k: &str, v: f64| KeyValue {
            key: k.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::DoubleValue(v)),
            }),
        };
        let scope = |version: &str| ScopeLogs {
            scope: Some(
                otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::InstrumentationScope {
                    name: "scope".into(),
                    version: version.into(),
                    ..Default::default()
                },
            ),
            log_records: [0.0, -0.0, f64::NAN, f64::from_bits(0xFFF8_0000_0000_0001)]
                .into_iter()
                .enumerate()
                .map(|(i, v)| LogRecord {
                    time_unix_nano: 1_000 + i as u64,
                    attributes: vec![double("logger.name", v), kv("request_id", &format!("r{i}"))],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let data = LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", "h1")],
                    ..Default::default()
                }),
                scope_logs: vec![scope("1"), scope("2")],
                ..Default::default()
            }],
        };
        let mut records = encode_logs(&data);
        let out = extract(&mut records, &cfg()).expect("extract");
        assert_eq!(out.descriptors.len(), 4);
        for d in &out.descriptors {
            assert_eq!(identity(&d.descriptor).1, d.series_id);
        }
        let (_, batches) = &out.values[0];
        let ids = batches[0]
            .column_by_name("series_id")
            .expect("series_id")
            .as_fixed_size_binary();
        let distinct: HashSet<&[u8]> = (0..ids.len()).map(|i| ids.value(i)).collect();
        assert_eq!(ids.len(), 8);
        assert_eq!(distinct.len(), 4);
        assert_eq!(ids.value(0), ids.value(1));
        assert_eq!(ids.value(2), ids.value(3));
        assert_ne!(ids.value(0), ids.value(2));
        assert_ne!(ids.value(0), ids.value(4));
    }

    /// Scenario: a string body one byte over the cell limit, and one exactly at it.
    /// Guarantees: the first is refused as an oversized cell; the second is stored byte for byte.
    #[test]
    fn a_string_body_is_held_to_the_cell_limit() {
        let mut cfg = cfg();
        cfg.ingress.max_row_bytes = 4096;
        let limit = DecodeLimits::new(cfg.ingress.max_nesting_depth, cfg.ingress.max_row_bytes)
            .max_cell_bytes;
        let body = |len: usize| LogsData {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_000,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("b".repeat(len))),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let mut over = encode_logs(&body(limit + 1));
        assert!(matches!(
            extract(&mut over, &cfg),
            Err(Error::Refused(RefuseReason::RequestTooLarge(
                crate::error::Excess {
                    budget: crate::error::SizeBudget::Cell,
                    ..
                }
            )))
        ));
        let small = limit - 1024;
        let mut fits = encode_logs(&body(small));
        let out = extract(&mut fits, &cfg).expect("extract");
        let stored = out.values[0].1[0]
            .column_by_name("body")
            .expect("body")
            .as_string::<i32>()
            .value(0)
            .to_owned();
        assert_eq!(stored, "b".repeat(small));
    }

    /// Scenario: a descriptor row set is turned into a series batch.
    /// Guarantees: identity_bytes hashes to series_id and maps carry the attributes.
    #[test]
    fn series_batch_round_trip() {
        let mut records = encode_logs(&logs_data());
        let out = extract(&mut records, &cfg()).expect("extract");
        let rows: Vec<&DescriptorRow> = out.descriptors.iter().collect();
        let batch =
            series_batch(&rows, 1_700_000_000_000_000, Dataset::LogsSeries, &cfg()).expect("batch");
        assert_eq!(batch.num_rows(), 2);
        let ids = batch
            .column_by_name("series_id")
            .expect("id")
            .as_fixed_size_binary();
        let ib = batch
            .column_by_name("identity_bytes")
            .expect("ib")
            .as_binary::<i32>();
        assert_eq!(ids.value(0), crate::canonical::series_id(ib.value(0)));
        let res = batch.column_by_name("resource_attrs").expect("r").as_map();
        assert_eq!(res.value_length(0), 2);
    }

    /// Scenario: max_extracted_bytes far below the request size.
    /// Guarantees: extraction stops with RequestTooLarge instead of allocating everything.
    #[test]
    fn extracted_budget_is_enforced() {
        let mut cfg = cfg();
        cfg.ingress.max_extracted_bytes = 1;
        let mut records = encode_logs(&logs_data());
        assert!(matches!(
            extract(&mut records, &cfg),
            Err(Error::Refused(RefuseReason::RequestTooLarge(_)))
        ));
    }

    /// Scenario: a 900 KiB bytes attribute, under the 1 MiB row limit decoded but 1.2 MiB as
    /// rendered base64.
    /// Guarantees: the row limit applies to the rendered cell, so the request is refused.
    #[test]
    fn a_bytes_attribute_is_charged_its_rendered_base64_size() {
        const RAW: usize = 900 << 10;
        let cfg = LakeConfig::default();
        // The premise of the test: the tree fits the limit, the rendering does not.
        let attr = vec![("blob".to_string(), Value::Bytes(vec![0xABu8; RAW]))];
        assert!(crate::value::kv_bytes(&attr) < cfg.ingress.max_row_bytes);
        assert!(crate::extract::rendered_kv_bytes(&attr) > cfg.ingress.max_row_bytes);

        let data = LogsData {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_000,
                        attributes: vec![KeyValue {
                            key: "blob".into(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::BytesValue(vec![0xAB; RAW])),
                            }),
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let mut records = encode_logs(&data);
        assert!(matches!(
            extract(&mut records, &cfg),
            Err(Error::Refused(RefuseReason::RequestTooLarge(_)))
        ));
    }

    /// Scenario: max_row_bytes below the size of a single descriptor row.
    /// Guarantees: the descriptor row is subject to the row limit, not only values rows.
    #[test]
    fn descriptor_rows_are_subject_to_the_row_limit() {
        let mut cfg = cfg();
        cfg.ingress.max_row_bytes = 8;
        let mut records = encode_logs(&logs_data());
        assert!(matches!(
            extract(&mut records, &cfg),
            Err(Error::Refused(RefuseReason::RequestTooLarge(_)))
        ));
    }

    /// Scenario: the same logs request with plain and with transport-optimized parent ids.
    /// Guarantees: both give the same series ids, descriptor count and row count.
    #[test]
    fn transport_optimized_ids_give_the_same_result() {
        let mut plain_records = encode_logs(&logs_data());
        let plain = extract(&mut plain_records, &cfg()).expect("extract plain");

        let mut optimized = encode_logs(&logs_data());
        // The whole-record encoder covers the Logs payload's own resource.id and
        // scope.id delta encoding as well as the three attribute payloads, so
        // both halves of the decode are exercised.
        optimized.encode_transport_optimized().expect("encode");
        let got = extract(&mut optimized, &cfg()).expect("extract optimized");

        let ids = |e: &Extracted| {
            let mut v: Vec<String> = e
                .descriptors
                .iter()
                .map(|d| crate::canonical::hex(&d.series_id))
                .collect();
            v.sort();
            v
        };
        assert_eq!(ids(&got), ids(&plain));
        assert_eq!(got.stats.rows, plain.stats.rows);
        let rows = |e: &Extracted| -> usize {
            e.values
                .iter()
                .flat_map(|(_, b)| b.iter())
                .map(|b| b.num_rows())
                .sum()
        };
        assert_eq!(rows(&got), rows(&plain));
    }

    /// Scenario: the same logs request with its timestamp columns tagged UTC,
    /// as OTAP producers write them, and with no time zone, as older
    /// producers wrote them.
    /// Guarantees: both extract to the same values rows.
    #[test]
    fn timestamps_without_a_time_zone_read_as_utc() {
        let mut tagged = encode_logs(&logs_data());
        let mut untagged = encode_logs(&logs_data());
        let logs = untagged.get(ArrowPayloadType::Logs).expect("logs").clone();
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        let mut stripped = 0;
        for (field, column) in logs.schema().fields().iter().zip(logs.columns()) {
            if let DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some(_)) =
                field.data_type()
            {
                let plain = column
                    .as_primitive::<TimestampNanosecondType>()
                    .clone()
                    .with_timezone_opt(None::<Arc<str>>);
                fields.push(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(plain.data_type().clone()),
                );
                columns.push(Arc::new(plain) as ArrayRef);
                stripped += 1;
            } else {
                fields.push(field.as_ref().clone());
                columns.push(Arc::clone(column));
            }
        }
        assert_eq!(stripped, 2, "the encoder tags both log timestamps");
        let schema = Schema::new_with_metadata(fields, logs.schema().metadata().clone());
        let batch = RecordBatch::try_new(Arc::new(schema), columns).expect("batch");
        untagged
            .set(ArrowPayloadType::Logs, batch)
            .expect("replace logs");

        let tagged = extract(&mut tagged, &cfg()).expect("extract tagged");
        let untagged = extract(&mut untagged, &cfg()).expect("extract untagged");
        let values = |e: &Extracted| -> Vec<RecordBatch> {
            e.values.iter().flat_map(|(_, b)| b.clone()).collect()
        };
        assert_eq!(values(&untagged), values(&tagged));
    }

    /// Scenario: one log record with attributes and one without (a null `id`).
    /// Guarantees: the second gets an empty identity list and its own series.
    #[test]
    fn records_without_attributes_do_not_inherit_log_zero() {
        let data = LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", "h1")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![
                        LogRecord {
                            time_unix_nano: 1_000,
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("a".into())),
                            }),
                            attributes: vec![kv("logger.name", "L1")],
                            ..Default::default()
                        },
                        LogRecord {
                            time_unix_nano: 2_000,
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("b".into())),
                            }),
                            attributes: vec![],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let mut records = encode_logs(&data);
        let out = extract(&mut records, &cfg()).expect("extract");
        assert_eq!(out.descriptors.len(), 2);
        let with_attrs = out
            .descriptors
            .iter()
            .find(|d| !d.descriptor.attrs.is_empty())
            .expect("attributed descriptor");
        let without = out
            .descriptors
            .iter()
            .find(|d| d.descriptor.attrs.is_empty())
            .expect("unattributed descriptor");
        assert_eq!(with_attrs.descriptor.attrs.len(), 1);
        assert_ne!(with_attrs.series_id, without.series_id);
    }

    /// Scenario: the Logs `body` column replaced by a string column and handed to
    /// `OtapArrowRecords`.
    /// Guarantees: pdata refuses it, so it cannot reach `extract` through the public API.
    #[test]
    fn pdata_refuses_a_non_struct_body_column() {
        let mut records = encode_logs(&logs_data());
        let logs = records.get(ArrowPayloadType::Logs).expect("logs").clone();
        let mut fields: Vec<Field> = Vec::new();
        let mut cols: Vec<ArrayRef> = Vec::new();
        for (i, f) in logs.schema().fields().iter().enumerate() {
            if f.name() == "body" {
                fields.push(Field::new("body", DataType::Utf8, true));
                cols.push(Arc::new(StringArray::from(vec!["x"; logs.num_rows()])) as ArrayRef);
            } else {
                fields.push(f.as_ref().clone());
                cols.push(logs.column(i).clone());
            }
        }
        let patched =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).expect("patched batch");
        assert!(records.set(ArrowPayloadType::Logs, patched).is_err());
    }

    /// Scenario: `max_extracted_bytes` exactly at, and one byte under, the measured output.
    /// Guarantees: the first is accepted and the second refused; a row is never charged twice.
    #[test]
    fn budget_is_enforced_on_the_measured_output() {
        let mut probe = encode_logs(&logs_data());
        let out = extract(&mut probe, &cfg()).expect("extract");
        // What the budget holds once the run is sealed: the decoded attribute
        // tables, the estimated descriptor rows and the measured pinned bytes of
        // the values batches. The values rows' estimates were uncharged when
        // their run was sealed.
        let tables = crate::extract::tests::table_bytes(
            &encode_logs(&logs_data()),
            &cfg(),
            &[
                ArrowPayloadType::ResourceAttrs,
                ArrowPayloadType::ScopeAttrs,
                ArrowPayloadType::LogAttrs,
            ],
        );
        let measured: usize = out
            .descriptors
            .iter()
            .map(|d| d.approx_bytes)
            .sum::<usize>()
            + out.pinned_bytes
            + out.merge_key_bytes
            + out.shared_bytes
            + tables;

        let mut exact = cfg();
        exact.ingress.max_extracted_bytes = measured;
        let mut records = encode_logs(&logs_data());
        let _accepted = extract(&mut records, &exact).expect("accepted at the measured size");

        let mut tight = cfg();
        tight.ingress.max_extracted_bytes = measured - 1;
        let mut records = encode_logs(&logs_data());
        assert!(matches!(
            extract(&mut records, &tight),
            Err(Error::Refused(RefuseReason::RequestTooLarge(_)))
        ));
    }

    /// Scenario: a denormalized `int64` column over a string resource attribute.
    /// Guarantees: the cell is null and the mismatch is counted per column and in the aggregate.
    #[test]
    fn denorm_type_mismatch_is_counted() {
        let mut cfg = cfg();
        cfg.logs.denormalize = vec![Denormalize {
            path: "resource.service.name".into(),
            column: "service_name".into(),
            ty: DenormType::Int64,
        }];
        let mut records = encode_logs(&logs_data());
        let out = extract(&mut records, &cfg).expect("extract");
        assert!(out.stats.denorm_type_mismatch > 0);
        let (_, batches) = &out.values[0];
        let svc = batches[0]
            .column_by_name("service_name")
            .expect("service_name");
        assert!(svc.is_null(0));
        assert_eq!(
            out.stats.denorm_type_mismatch_by_column.get("service_name"),
            Some(&out.stats.denorm_type_mismatch),
            "the whole aggregate is attributed to the one mismatching column"
        );
    }

    /// Scenario: a `time_unix_nano` above `i64::MAX`.
    /// Guarantees: both timestamp columns are null and `timestamp_out_of_range` counts one.
    #[test]
    fn timestamp_out_of_range_is_counted() {
        let data = LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", "h1")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: u64::MAX,
                        observed_time_unix_nano: 1_000,
                        attributes: vec![kv("logger.name", "L1")],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let mut records = encode_logs(&data);
        let out = extract(&mut records, &cfg()).expect("extract");
        assert_eq!(out.stats.timestamp_out_of_range, 1);
        let (_, batches) = &out.values[0];
        let b = &batches[0];
        assert!(b.column_by_name("time_unix_nano").expect("t").is_null(0));
        assert!(b.column_by_name("time").expect("time").is_null(0));
    }
}
