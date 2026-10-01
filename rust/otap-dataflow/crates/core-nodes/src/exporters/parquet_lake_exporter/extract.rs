// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! OTAP -> (values rows, series rows, series ids) in the Series Lake Format v1 schemas, split into
//! owned chunks under a byte budget. Content the format calls invalid refuses the whole request.
//!
//! Work and memory are bounded by the request budget (`ingress.max_extracted_bytes`): every loop
//! over request rows does constant work per row plus work whose size is charged to the budget, so
//! a small request that refers to one large entity many times is refused after a bounded amount
//! of work.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, FixedSizeBinaryArray, RecordBatch,
    StringArray, StringBuilder, StructArray, TimestampMicrosecondArray, UInt32Array,
    new_null_array,
};
use arrow::compute::{take, take_record_batch};
use arrow::datatypes::{DataType, Int32Type, SchemaRef, UInt8Type};
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_dfe_pdata::schema::consts;
use xxhash_rust::xxh3::Xxh3;

use super::anyvalue::AnyValueColumns;
use super::attrs::AttrIndex;
use super::canonical::{MetricKind, Signal, Temporality, key_prefix, put_bool, put_str};
use super::columns::{
    as_u32, cast_or_null, col, f64_or_zero, flags_i32, i32_or_zero, ids_u32, list_f64,
    list_u64_as_i64, opt_ids_u32, row_bytes, struct_field, take_charged, timestamp_pair,
    u64_as_i64, utf8_or_empty,
};
use super::error::LakeError;
use super::limits::{Budget, Limits};
use super::schema::Schemas;

/// Fixed cells of one logs values row, charged for every row before any column is built.
const LOGS_ROW_BYTES: usize = 108;
/// Fixed cells of one metrics values row.
const METRICS_ROW_BYTES: usize = 156;
/// Per metric row bookkeeping (kind, temporality, identity part, id maps), charged up front.
const METRIC_ROW_OVERHEAD: usize = 96;

/// The series rows of one input table: one row per distinct series, built once per request and
/// shared by the table's chunks.
pub struct SeriesTable {
    /// Series rows (schema: `Schemas::series(signal)`).
    pub rows: RecordBatch,
    /// series_id of each row.
    pub ids: Vec<u128>,
    /// Bytes of each row: its share of a block's budget when the row is buffered.
    pub row_bytes: Vec<usize>,
}

/// Owned values rows of one chunk, and which series rows they need. Any chunk may be the first of
/// its series in a block, so every chunk can supply its series rows; it refers to the shared table
/// instead of carrying copies.
pub struct Chunk {
    /// Values rows (schema: `Schemas::values(signal)`).
    pub values: RecordBatch,
    /// The series rows of the input table this chunk comes from.
    pub series: Rc<SeriesTable>,
    /// Rows of `series` of the distinct series in this chunk.
    pub series_rows: Vec<u32>,
    bytes: usize,
}

impl Chunk {
    /// Upper bound of what the chunk adds to a block: the memory of its values rows (accurate,
    /// because every column is freshly built) plus the bytes of all its series rows.
    #[must_use]
    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    /// series_id of each distinct series of the chunk.
    #[cfg(test)]
    pub fn series_ids(&self) -> impl Iterator<Item = u128> + '_ {
        self.series_rows
            .iter()
            .map(|&r| self.series.ids[r as usize])
    }
}

/// The extracted rows of one request.
#[derive(Default)]
pub struct Extracted {
    /// Owned chunks, each at most `Limits::max_chunk_bytes` of values (or one row).
    pub chunks: Vec<Chunk>,
    /// Timestamps stored as null because they were negative.
    pub timestamps_out_of_range: u64,
    /// Measured memory of the values chunks and the series tables (the running total).
    pub bytes: usize,
    /// Bytes charged to the request budget.
    pub charged: usize,
}

fn invalid(msg: &str) -> LakeError {
    LakeError::Invalid(msg.to_owned())
}

/// FixedSizeBinary(16) column of big-endian series ids (at least one id).
fn id_column(ids: impl IntoIterator<Item = u128>) -> Result<ArrayRef, LakeError> {
    Ok(Arc::new(FixedSizeBinaryArray::try_from_iter(
        ids.into_iter().map(u128::to_be_bytes),
    )?))
}

/// `identity_bytes` of the series of input rows `rows`.
fn identity_column(rows: &UInt32Array, ids: &[u128], keys: &HashMap<u128, Vec<u8>>) -> ArrayRef {
    Arc::new(BinaryArray::from_iter_values(rows.values().iter().map(
        |&r| keys.get(&ids[r as usize]).map_or(&[][..], Vec::as_slice),
    )))
}

/// Non-null `emitted_at` placeholder; the encoder stamps the seal time.
fn emitted_at_placeholder(n: usize) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(vec![0; n]).with_timezone("UTC"))
}

fn strings<'a>(values: impl Iterator<Item = &'a str>) -> ArrayRef {
    Arc::new(values.map(Some).collect::<StringArray>())
}

fn opt_id(ids: &UInt32Array, i: usize) -> Option<u32> {
    (!ids.is_null(i)).then(|| ids.value(i))
}

/// Refuse a batch whose largest row exceeds `ingress.max_row_bytes`.
fn check_rows(batch: &RecordBatch, limits: &Limits) -> Result<(), LakeError> {
    limits.check_row(row_bytes(batch).into_iter().max().unwrap_or(0))
}

/// Add `bytes` of extracted output to the request's running total and refuse the request as soon
/// as the total passes `ingress.max_extracted_bytes`, so extraction never builds more than the
/// limit plus one chunk.
fn account(out: &mut Extracted, bytes: usize, limits: &Limits) -> Result<(), LakeError> {
    out.bytes = out.bytes.saturating_add(bytes);
    if out.bytes > limits.max_extracted_bytes {
        return Err(LakeError::TooLarge {
            setting: "ingress.max_extracted_bytes",
            observed: out.bytes,
            limit: limits.max_extracted_bytes,
        });
    }
    Ok(())
}

/// Turn `values` (all rows of one input table, row r has id `ids[r]`) into chunks of at most
/// `limits.max_chunk_bytes` of values rows. The series rows of the table are built once, by
/// `series_rows(firsts)` for the first input row of each distinct series, and shared by the
/// chunks. A values slice that measures over budget is halved until it fits or holds one row.
/// The series table and every chunk are added to the running total of `out`.
fn split(
    values: RecordBatch,
    ids: &[u128],
    series_rows: &mut dyn FnMut(&UInt32Array) -> Result<RecordBatch, LakeError>,
    limits: &Limits,
    out: &mut Extracted,
) -> Result<(), LakeError> {
    // One series row per distinct id, in order of first appearance.
    let mut row_of: HashMap<u128, u32> = HashMap::new();
    let mut firsts: Vec<u32> = Vec::new();
    for (r, id) in ids.iter().enumerate() {
        if let Entry::Vacant(e) = row_of.entry(*id) {
            let _ = e.insert(firsts.len() as u32);
            firsts.push(r as u32);
        }
    }
    let table_ids: Vec<u128> = firsts.iter().map(|&r| ids[r as usize]).collect();
    let rows = series_rows(&UInt32Array::from(firsts))?;
    account(out, rows.get_array_memory_size(), limits)?;
    let table = Rc::new(SeriesTable {
        row_bytes: row_bytes(&rows),
        rows,
        ids: table_ids,
    });
    let chunk = |values: RecordBatch, range: std::ops::Range<usize>| -> Chunk {
        let mut seen = HashSet::new();
        let series_rows: Vec<u32> = range
            .filter_map(|r| row_of.get(&ids[r]).copied())
            .filter(|row| seen.insert(*row))
            .collect();
        let bytes = values.get_array_memory_size()
            + series_rows
                .iter()
                .map(|&r| table.row_bytes[r as usize])
                .sum::<usize>();
        Chunk {
            values,
            series: Rc::clone(&table),
            series_rows,
            bytes,
        }
    };
    let n_rows = values.num_rows();
    let max_bytes = limits.max_chunk_bytes;
    // A table that fits one chunk is used as it is, without a copy.
    if values.get_array_memory_size() <= max_bytes {
        account(out, values.get_array_memory_size(), limits)?;
        out.chunks.push(chunk(values, 0..n_rows));
        return Ok(());
    }
    let per_row = (values.get_array_memory_size() / n_rows.max(1)).max(1);
    let initial_step = (max_bytes / per_row).max(1);
    let mut step = initial_step;
    let mut start = 0;
    while start < n_rows {
        let n = step.min(n_rows - start);
        let range = UInt32Array::from_iter_values(start as u32..(start + n) as u32);
        let part = take_record_batch(&values, &range)?;
        if part.get_array_memory_size() > max_bytes && n > 1 {
            step = n / 2;
            continue;
        }
        account(out, part.get_array_memory_size(), limits)?;
        out.chunks.push(chunk(part, start..start + n));
        start += n;
        step = initial_step; // a skewed stretch does not shrink the rest of the payload
    }
    Ok(())
}

/// Resource and scope identity inputs of a root table (logs or univariate metrics).
struct Context {
    rids: UInt32Array,
    sids: UInt32Array,
    res_schema: StringArray,
    scope_name: StringArray,
    scope_version: StringArray,
    scope_schema: StringArray,
    res_dropped: ArrayRef,
    scope_dropped: ArrayRef,
    res: AttrIndex,
    scope: AttrIndex,
}

impl Context {
    fn new(
        records: &OtapArrowRecords,
        root: &RecordBatch,
        limits: &Limits,
        budget: &mut Budget,
    ) -> Result<Self, LakeError> {
        let n = root.num_rows();
        let dropped = |parent: &str| {
            cast_or_null(
                struct_field(root, parent, consts::DROPPED_ATTRIBUTES_COUNT).as_ref(),
                &DataType::Int64,
                n,
            )
        };
        Ok(Self {
            rids: opt_ids_u32(struct_field(root, consts::RESOURCE, consts::ID), n)?,
            sids: opt_ids_u32(struct_field(root, consts::SCOPE, consts::ID), n)?,
            res_schema: utf8_or_empty(
                struct_field(root, consts::RESOURCE, consts::SCHEMA_URL).as_ref(),
                n,
                budget,
            )?,
            scope_name: utf8_or_empty(
                struct_field(root, consts::SCOPE, consts::NAME).as_ref(),
                n,
                budget,
            )?,
            scope_version: utf8_or_empty(
                struct_field(root, consts::SCOPE, consts::VERSION).as_ref(),
                n,
                budget,
            )?,
            // The root table's own schema_url is the scope's.
            scope_schema: utf8_or_empty(col(root, consts::SCHEMA_URL), n, budget)?,
            res_dropped: dropped(consts::RESOURCE)?,
            scope_dropped: dropped(consts::SCOPE)?,
            res: AttrIndex::from_batch(
                records.get(ArrowPayloadType::ResourceAttrs),
                "resource_attrs",
                limits,
                budget,
            )?,
            scope: AttrIndex::from_batch(
                records.get(ArrowPayloadType::ScopeAttrs),
                "scope_attrs",
                limits,
                budget,
            )?,
        })
    }

    /// Identity fields of root row `i`, from the resource attributes to the scope attributes
    /// (FORMAT.md section 1). Its cost is the size of the row's resource and scope attributes;
    /// callers run it once per distinct context and charge the result (`ContextPrefixes`).
    fn key_into(&self, i: usize, out: &mut Vec<u8>) {
        self.res.kvlist_into(opt_id(&self.rids, i), out);
        put_str(out, self.res_schema.value(i));
        put_str(out, self.scope_name.value(i));
        put_str(out, self.scope_version.value(i));
        put_str(out, self.scope_schema.value(i));
        self.scope.kvlist_into(opt_id(&self.sids, i), out);
    }

    /// Series columns `resource_schema_url` .. `scope_attrs` for root rows. A root row can have
    /// many series, so every copy is charged.
    fn series_columns(
        &self,
        rows: &UInt32Array,
        budget: &mut Budget,
    ) -> Result<Vec<ArrayRef>, LakeError> {
        let rids = as_u32(&take(&self.rids, rows, None)?);
        let sids = as_u32(&take(&self.sids, rows, None)?);
        Ok(vec![
            take_charged(&self.res_schema, rows, budget)?,
            self.res.build_map(&rids, budget)?,
            take_charged(&self.scope_name, rows, budget)?,
            take_charged(&self.scope_version, rows, budget)?,
            take_charged(&self.scope_schema, rows, budget)?,
            self.scope.build_map(&sids, budget)?,
        ])
    }

    /// The additional series columns (dropped-attribute counts) for root rows.
    fn ext_columns(&self, rows: &UInt32Array) -> Result<[ArrayRef; 2], LakeError> {
        Ok([
            take(self.res_dropped.as_ref(), rows, None)?,
            take(self.scope_dropped.as_ref(), rows, None)?,
        ])
    }

    /// `producer_id` of each root row in `roots`: the rendered resource attribute `attribute`,
    /// `""` when absent. The attribute is looked up once per distinct resource, not once per row.
    fn producer_ids(
        &self,
        roots: impl ExactSizeIterator<Item = usize>,
        attribute: &str,
        budget: &mut Budget,
    ) -> Result<ArrayRef, LakeError> {
        let mut by_resource: HashMap<Option<u32>, &str> = HashMap::new();
        let mut out = StringBuilder::with_capacity(roots.len(), 0);
        for i in roots {
            let rid = opt_id(&self.rids, i);
            let id = *by_resource
                .entry(rid)
                .or_insert_with(|| self.res.rendered(rid, attribute));
            budget.charge(id.len())?;
            out.append_value(id);
        }
        Ok(Arc::new(out.finish()))
    }
}

/// The identity bytes of the distinct resource/scope contexts of a root table, each with the hash
/// state after those bytes. A context is encoded once and charged its encoded length, however many
/// rows share it, so the encoding work of a request is bounded by its budget. A series id
/// continues the hash from the stored state.
struct ContextPrefixes<'a> {
    by_key: HashMap<(Option<u32>, Option<u32>, &'a str, &'a str, &'a str, &'a str), u32>,
    entries: Vec<(Vec<u8>, Xxh3)>,
}

impl<'a> ContextPrefixes<'a> {
    fn new() -> Self {
        Self {
            by_key: HashMap::new(),
            entries: Vec::new(),
        }
    }

    /// Index into `entries` of the context of root row `i`. Indices are assigned in order of first
    /// appearance.
    fn of(
        &mut self,
        ctx: &'a Context,
        signal: Signal,
        i: usize,
        limits: &Limits,
        budget: &mut Budget,
    ) -> Result<u32, LakeError> {
        let key = (
            opt_id(&ctx.rids, i),
            opt_id(&ctx.sids, i),
            ctx.res_schema.value(i),
            ctx.scope_name.value(i),
            ctx.scope_version.value(i),
            ctx.scope_schema.value(i),
        );
        if let Some(at) = self.by_key.get(&key) {
            return Ok(*at);
        }
        let mut bytes = key_prefix(signal);
        ctx.key_into(i, &mut bytes);
        // Charged for every distinct context, whether or not it yields a new series: the charge
        // pays for the encoding and hashing just done, and the identity is a cell of a series row.
        budget.charge(bytes.len() + size_of::<Xxh3>())?;
        limits.check_row(bytes.len())?;
        let mut state = Xxh3::new();
        state.update(&bytes);
        let at = self.entries.len() as u32;
        self.entries.push((bytes, state));
        let _ = self.by_key.insert(key, at);
        Ok(at)
    }
}

/// Finish an identity: hash `tail` onto the context's state, and keep the full identity bytes the
/// first time a series id appears (charged).
fn finish_identity(
    context: &(Vec<u8>, Xxh3),
    tail: &[u8],
    keys: &mut HashMap<u128, Vec<u8>>,
    limits: &Limits,
    budget: &mut Budget,
) -> Result<u128, LakeError> {
    let (prefix, state) = context;
    // XXH3-128 of `prefix ++ tail` without hashing the prefix again.
    let mut hasher = state.clone();
    hasher.update(tail);
    let id = hasher.digest128();
    if let Entry::Vacant(slot) = keys.entry(id) {
        let len = prefix.len() + tail.len();
        limits.check_row(len)?;
        budget.charge(len)?;
        let mut key = Vec::with_capacity(len);
        key.extend_from_slice(prefix);
        key.extend_from_slice(tail);
        let _ = slot.insert(key);
    }
    Ok(id)
}

/// Logs of `records` as owned chunks (none when the payload has no log rows).
pub fn extract_logs(
    records: &OtapArrowRecords,
    schemas: &Schemas,
    limits: &Limits,
    producer_attribute: &str,
) -> Result<Extracted, LakeError> {
    let mut out = Extracted::default();
    let Some(logs) = records.get(ArrowPayloadType::Logs) else {
        return Ok(out);
    };
    let n = logs.num_rows();
    if n == 0 {
        return Ok(out);
    }
    let mut budget = Budget::new(limits.max_extracted_bytes);
    budget.charge(n.saturating_mul(LOGS_ROW_BYTES))?;
    let ctx = Context::new(records, logs, limits, &mut budget)?;

    // Identity: resource, schema URLs, scope. Log record attributes are not part of it, so the
    // identity ends with an empty attribute list and there is one series per context.
    let mut contexts = ContextPrefixes::new();
    let mut series_of: Vec<u128> = Vec::new();
    let mut keys: HashMap<u128, Vec<u8>> = HashMap::new();
    let mut tail = Vec::new();
    AttrIndex::empty().kvlist_into(None, &mut tail);
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let at = contexts.of(&ctx, Signal::Logs, i, limits, &mut budget)? as usize;
        if at == series_of.len() {
            // A context seen for the first time.
            let id = finish_identity(&contexts.entries[at], &tail, &mut keys, limits, &mut budget)?;
            series_of.push(id);
        }
        ids.push(series_of[at]);
    }

    let log_ids = opt_ids_u32(col(logs, consts::ID).cloned(), n)?;
    let log_attrs = AttrIndex::from_batch(
        records.get(ArrowPayloadType::LogAttrs),
        "log_attrs",
        limits,
        &mut budget,
    )?;
    let body: ArrayRef = match col(logs, consts::BODY) {
        None => new_null_array(&DataType::Utf8, n),
        Some(b) => {
            let body = b
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| invalid("column body is not an AnyValue struct"))?;
            let valid = |i: usize| body.is_valid(i);
            Arc::new(
                AnyValueColumns::new(
                    n,
                    |name| body.column_by_name(name).cloned(),
                    valid,
                    limits,
                    &mut budget,
                )?
                .render_where(valid, &mut budget)?,
            )
        }
    };
    let mut out_of_range = 0;
    let (time, time_ns) = timestamp_pair(col(logs, consts::TIME_UNIX_NANO), n, &mut out_of_range)?;
    let (observed, observed_ns) = timestamp_pair(
        col(logs, consts::OBSERVED_TIME_UNIX_NANO),
        n,
        &mut out_of_range,
    )?;
    out.timestamps_out_of_range = out_of_range;
    let vs = schemas.values(Signal::Logs);
    let values = RecordBatch::try_new(
        vs.clone(),
        vec![
            id_column(ids.iter().copied())?,
            ctx.producer_ids(0..n, producer_attribute, &mut budget)?,
            time,
            time_ns,
            observed,
            observed_ns,
            i32_or_zero(col(logs, consts::SEVERITY_NUMBER), n)?,
            Arc::new(utf8_or_empty(
                col(logs, consts::SEVERITY_TEXT),
                n,
                &mut budget,
            )?),
            body,
            Arc::new(utf8_or_empty(
                col(logs, consts::EVENT_NAME),
                n,
                &mut budget,
            )?),
            cast_or_null(
                col(logs, consts::TRACE_ID),
                &DataType::FixedSizeBinary(16),
                n,
            )?,
            cast_or_null(col(logs, consts::SPAN_ID), &DataType::FixedSizeBinary(8), n)?,
            flags_i32(col(logs, consts::FLAGS), n)?,
            log_attrs.build_map(&log_ids, &mut budget)?,
            cast_or_null(
                col(logs, consts::DROPPED_ATTRIBUTES_COUNT),
                &DataType::Int64,
                n,
            )?,
        ],
    )?;
    check_rows(&values, limits)?;
    let no_attrs = AttrIndex::empty();
    // Called once: one series row per distinct series of the request, charged to its budget.
    let mut series_rows = |rows: &UInt32Array| -> Result<RecordBatch, LakeError> {
        let mut cols = vec![
            id_column(rows.values().iter().map(|&r| ids[r as usize]))?,
            identity_column(rows, &ids, &keys),
            emitted_at_placeholder(rows.len()),
        ];
        cols.extend(ctx.series_columns(rows, &mut budget)?);
        cols.push(no_attrs.build_map(&UInt32Array::new_null(rows.len()), &mut budget)?);
        cols.extend(ctx.ext_columns(rows)?);
        let batch = RecordBatch::try_new(schemas.series(Signal::Logs).clone(), cols)?;
        check_rows(&batch, limits)?;
        Ok(batch)
    };
    split(values, &ids, &mut series_rows, limits, &mut out)?;
    out.charged = budget.used();
    Ok(out)
}

/// The kind of a data point table.
#[derive(Clone, Copy)]
enum PointKind {
    Number,
    Histogram,
    ExpHistogram,
    Summary,
}

/// One data point table: where it lives and what its points are called in refusals.
struct DpSpec {
    kind: PointKind,
    label: &'static str,
    points: ArrowPayloadType,
    points_name: &'static str,
    attrs: ArrowPayloadType,
    attrs_name: &'static str,
}

const DP_SPECS: [DpSpec; 4] = [
    DpSpec {
        kind: PointKind::Number,
        label: "number",
        points: ArrowPayloadType::NumberDataPoints,
        points_name: "number_data_points",
        attrs: ArrowPayloadType::NumberDpAttrs,
        attrs_name: "number_dp_attrs",
    },
    DpSpec {
        kind: PointKind::Histogram,
        label: "histogram",
        points: ArrowPayloadType::HistogramDataPoints,
        points_name: "histogram_data_points",
        attrs: ArrowPayloadType::HistogramDpAttrs,
        attrs_name: "histogram_dp_attrs",
    },
    DpSpec {
        kind: PointKind::ExpHistogram,
        label: "exponential histogram",
        points: ArrowPayloadType::ExpHistogramDataPoints,
        points_name: "exp_histogram_data_points",
        attrs: ArrowPayloadType::ExpHistogramDpAttrs,
        attrs_name: "exp_histogram_dp_attrs",
    },
    DpSpec {
        kind: PointKind::Summary,
        label: "summary",
        points: ArrowPayloadType::SummaryDataPoints,
        points_name: "summary_data_points",
        attrs: ArrowPayloadType::SummaryDpAttrs,
        attrs_name: "summary_dp_attrs",
    },
];

/// Charge the items of a list column before it is converted (`item_bytes` per item).
fn charge_list(
    column: Option<&ArrayRef>,
    item_bytes: usize,
    budget: &mut Budget,
) -> Result<(), LakeError> {
    let items = column
        .and_then(|c| c.as_list_opt::<i32>())
        .map_or(0, |l| l.values().len());
    budget.charge(items.saturating_mul(item_bytes))
}

/// The kind-specific values columns of a point table, by column name. Every other column of the
/// values schema is null for this kind.
fn kind_columns(
    kind: PointKind,
    points: &RecordBatch,
    n: usize,
    values_schema: &SchemaRef,
    budget: &mut Budget,
) -> Result<Vec<(&'static str, ArrayRef)>, LakeError> {
    let double = |name: &str| cast_or_null(col(points, name), &DataType::Float64, n);
    let count = |name: &str, what: &'static str| u64_as_i64(col(points, name), n, what, true);
    Ok(match kind {
        PointKind::Number => vec![
            (
                "value_int",
                cast_or_null(col(points, consts::INT_VALUE), &DataType::Int64, n)?,
            ),
            ("value_double", double(consts::DOUBLE_VALUE)?),
        ],
        PointKind::Histogram => {
            let raw_counts = col(points, consts::HISTOGRAM_BUCKET_COUNTS);
            let raw_bounds = col(points, consts::HISTOGRAM_EXPLICIT_BOUNDS);
            charge_list(raw_counts, 8, budget)?;
            charge_list(raw_bounds, 8, budget)?;
            // A histogram without a distribution stores two empty lists, not two null lists.
            let counts = list_u64_as_i64(raw_counts, n, "bucket count", true)?;
            let bounds = list_f64(raw_bounds, n, "explicit bound", true)?;
            let (c, b) = (counts.as_list::<i32>(), bounds.as_list::<i32>());
            for row in 0..n {
                let (c, b) = (c.value_length(row), b.value_length(row));
                if !((c == 0 && b == 0) || c == b + 1) {
                    return Err(invalid(
                        "histogram bucket_counts.len != explicit_bounds.len + 1",
                    ));
                }
            }
            vec![
                ("count", count(consts::HISTOGRAM_COUNT, "histogram count")?),
                ("sum", double(consts::HISTOGRAM_SUM)?),
                ("min", double(consts::HISTOGRAM_MIN)?),
                ("max", double(consts::HISTOGRAM_MAX)?),
                ("bucket_counts", counts),
                ("explicit_bounds", bounds),
            ]
        }
        PointKind::ExpHistogram => {
            let raw = |side: &str| struct_field(points, side, consts::EXP_HISTOGRAM_BUCKET_COUNTS);
            let (positive, negative) = (
                raw(consts::EXP_HISTOGRAM_POSITIVE),
                raw(consts::EXP_HISTOGRAM_NEGATIVE),
            );
            charge_list(positive.as_ref(), 8, budget)?;
            charge_list(negative.as_ref(), 8, budget)?;
            let offset = |side: &str| {
                cast_or_null(
                    struct_field(points, side, consts::EXP_HISTOGRAM_OFFSET).as_ref(),
                    &DataType::Int32,
                    n,
                )
            };
            vec![
                ("count", count(consts::HISTOGRAM_COUNT, "histogram count")?),
                ("sum", double(consts::HISTOGRAM_SUM)?),
                ("min", double(consts::HISTOGRAM_MIN)?),
                ("max", double(consts::HISTOGRAM_MAX)?),
                // scale, zero_count and zero_threshold are plain (non-optional) OTLP fields that a
                // point always sets; OTAP omits the column only when every point has the default,
                // so an absent or null cell reads as 0, not null (see docs/FORMAT.md deviations).
                (
                    "scale",
                    i32_or_zero(col(points, consts::EXP_HISTOGRAM_SCALE), n)?,
                ),
                (
                    "zero_count",
                    u64_as_i64(
                        col(points, consts::EXP_HISTOGRAM_ZERO_COUNT),
                        n,
                        "zero count",
                        true,
                    )?,
                ),
                (
                    "zero_threshold",
                    f64_or_zero(col(points, consts::EXP_HISTOGRAM_ZERO_THRESHOLD), n)?,
                ),
                ("positive_offset", offset(consts::EXP_HISTOGRAM_POSITIVE)?),
                (
                    "positive_bucket_counts",
                    list_u64_as_i64(positive.as_ref(), n, "bucket count", false)?,
                ),
                ("negative_offset", offset(consts::EXP_HISTOGRAM_NEGATIVE)?),
                (
                    "negative_bucket_counts",
                    list_u64_as_i64(negative.as_ref(), n, "bucket count", false)?,
                ),
            ]
        }
        PointKind::Summary => {
            let quantiles = col(points, consts::SUMMARY_QUANTILE_VALUES);
            charge_list(quantiles, 16, budget)?;
            vec![
                ("count", count(consts::SUMMARY_COUNT, "summary count")?),
                ("sum", double(consts::SUMMARY_SUM)?),
                (
                    "quantile_values",
                    cast_or_null(
                        quantiles,
                        values_schema
                            .field_with_name("quantile_values")?
                            .data_type(),
                        n,
                    )?,
                ),
            ]
        }
    })
}

/// Metric-level identity and series columns of the univariate metrics table.
struct MetricColumns {
    ctx: Context,
    name: StringArray,
    unit: StringArray,
    description: StringArray,
    /// Point kind of each metric row; `None` for a metric without data.
    kind: Vec<Option<MetricKind>>,
    temporality: Vec<Temporality>,
    monotonic: Vec<bool>,
    /// Identity fields of each metric row that follow the scope attributes: name, unit, type,
    /// temporality, monotonic flag (FORMAT.md section 1).
    part: Vec<Vec<u8>>,
    /// metric id -> metric row, for the rows that have a kind.
    row_of: HashMap<u32, u32>,
}

impl MetricColumns {
    fn new(
        records: &OtapArrowRecords,
        metrics: &RecordBatch,
        limits: &Limits,
        budget: &mut Budget,
    ) -> Result<Self, LakeError> {
        let m = metrics.num_rows();
        budget.charge(m.saturating_mul(METRIC_ROW_OVERHEAD))?;
        let ids = ids_u32(col(metrics, consts::ID), "univariate_metrics", consts::ID)?;
        let types = cast_or_null(col(metrics, consts::METRIC_TYPE), &DataType::UInt8, m)?;
        let types = types.as_primitive::<UInt8Type>();
        let temporalities = cast_or_null(
            col(metrics, consts::AGGREGATION_TEMPORALITY),
            &DataType::Int32,
            m,
        )?;
        let temporalities = temporalities.as_primitive::<Int32Type>();
        let monotonics = cast_or_null(col(metrics, consts::IS_MONOTONIC), &DataType::Boolean, m)?;
        let monotonics = monotonics.as_boolean();
        let mut kind = Vec::with_capacity(m);
        let mut temporality = Vec::with_capacity(m);
        let mut monotonic = Vec::with_capacity(m);
        let mut row_of = HashMap::with_capacity(m);
        let mut seen = HashSet::with_capacity(m);
        for i in 0..m {
            if ids.is_null(i) {
                return Err(invalid("metric row without id"));
            }
            let id = ids.value(i);
            // A repeated id would put one metric's points under another's series.
            if !seen.insert(id) {
                return Err(LakeError::Invalid(format!("duplicate metric id {id}")));
            }
            if types.is_null(i) {
                return Err(invalid("metric row without metric_type"));
            }
            // OTAP metric_type: 0 empty, 1 gauge, 2 sum, 3 histogram, 4 exponential histogram,
            // 5 summary.
            let k = match types.value(i) {
                0 => None,
                1 => Some(MetricKind::Gauge),
                2 => Some(MetricKind::Sum),
                3 => Some(MetricKind::Histogram),
                4 => Some(MetricKind::ExpHistogram),
                5 => Some(MetricKind::Summary),
                other => return Err(LakeError::Invalid(format!("metric_type {other}"))),
            };
            // OTLP AggregationTemporality: 1 delta, 2 cumulative; anything else is unspecified.
            let t = match (!temporalities.is_null(i)).then(|| temporalities.value(i)) {
                Some(1) => Temporality::Delta,
                Some(2) => Temporality::Cumulative,
                _ => Temporality::Unspecified,
            };
            let aggregated = matches!(
                k,
                Some(MetricKind::Sum | MetricKind::Histogram | MetricKind::ExpHistogram)
            );
            if aggregated && t == Temporality::Unspecified {
                return Err(invalid("sum or histogram with unspecified temporality"));
            }
            kind.push(k);
            temporality.push(if aggregated {
                t
            } else {
                Temporality::Unspecified
            });
            monotonic
                .push(k == Some(MetricKind::Sum) && !monotonics.is_null(i) && monotonics.value(i));
            if k.is_some() {
                let _ = row_of.insert(id, i as u32);
            }
        }
        let name = utf8_or_empty(col(metrics, consts::NAME), m, budget)?;
        let unit = utf8_or_empty(col(metrics, consts::UNIT), m, budget)?;
        let mut part = Vec::with_capacity(m);
        for i in 0..m {
            let mut p = Vec::new();
            put_str(&mut p, name.value(i));
            put_str(&mut p, unit.value(i));
            put_str(&mut p, kind[i].map_or("", MetricKind::as_str));
            put_str(&mut p, temporality[i].as_str());
            put_bool(&mut p, monotonic[i]);
            budget.charge(p.len())?;
            part.push(p);
        }
        Ok(Self {
            ctx: Context::new(records, metrics, limits, budget)?,
            name,
            unit,
            description: utf8_or_empty(col(metrics, consts::DESCRIPTION), m, budget)?,
            kind,
            temporality,
            monotonic,
            part,
            row_of,
        })
    }
}

/// Data points of all four kinds of `records` as owned chunks of the wide values table.
pub fn extract_metrics(
    records: &OtapArrowRecords,
    schemas: &Schemas,
    limits: &Limits,
    producer_attribute: &str,
) -> Result<Extracted, LakeError> {
    let mut out = Extracted::default();
    // Metrics without any data point produce no rows; do not require the root table then.
    if DP_SPECS
        .iter()
        .all(|s| records.get(s.points).is_none_or(|b| b.num_rows() == 0))
    {
        return Ok(out);
    }
    let Some(metrics) = records.get(ArrowPayloadType::UnivariateMetrics) else {
        return Err(invalid(
            "metrics without a univariate metrics table are not supported",
        ));
    };
    let mut budget = Budget::new(limits.max_extracted_bytes);
    let mc = MetricColumns::new(records, metrics, limits, &mut budget)?;
    let vs = schemas.values(Signal::Metrics);
    let ss = schemas.series(Signal::Metrics);
    let mut contexts = ContextPrefixes::new();
    // Context of each metric row, resolved on first use.
    let mut context_of: Vec<Option<u32>> = vec![None; metrics.num_rows()];
    let mut scratch = Vec::new();
    let mut out_of_range = 0;
    for spec in &DP_SPECS {
        let Some(points) = records.get(spec.points) else {
            continue;
        };
        let n = points.num_rows();
        if n == 0 {
            continue;
        }
        budget.charge(n.saturating_mul(METRICS_ROW_BYTES))?;
        let parents = ids_u32(
            col(points, consts::PARENT_ID),
            spec.points_name,
            consts::PARENT_ID,
        )?;
        let mut metric_rows = Vec::with_capacity(n);
        let mut part_bytes = 0usize;
        for i in 0..n {
            if parents.is_null(i) {
                return Err(LakeError::Invalid(format!(
                    "{} point without parent metric id",
                    spec.label
                )));
            }
            let row = *mc
                .row_of
                .get(&parents.value(i))
                .ok_or_else(|| invalid("data point references unknown metric"))?;
            part_bytes = part_bytes.saturating_add(mc.part[row as usize].len());
            metric_rows.push(row);
        }
        // For every point the metric part of the identity (name, unit, type) is copied and hashed,
        // and the metric name is stored on the values row. Charged before that work is done.
        budget.charge(part_bytes)?;
        let metric_rows = UInt32Array::from(metric_rows);
        let dp_ids = opt_ids_u32(col(points, consts::ID).cloned(), n)?;
        let dp_attrs = AttrIndex::from_batch(
            records.get(spec.attrs),
            spec.attrs_name,
            limits,
            &mut budget,
        )?;
        let mut ids = Vec::with_capacity(n);
        let mut keys: HashMap<u128, Vec<u8>> = HashMap::new();
        let mut seen_points: HashSet<u32> = HashSet::new();
        for i in 0..n {
            let mrow = metric_rows.value(i) as usize;
            let at = match context_of[mrow] {
                Some(at) => at,
                None => {
                    let at = contexts.of(&mc.ctx, Signal::Metrics, mrow, limits, &mut budget)?;
                    context_of[mrow] = Some(at);
                    at
                }
            };
            // The rest of the identity: the metric fields and the data point attributes.
            scratch.clear();
            scratch.extend_from_slice(&mc.part[mrow]);
            let part_len = scratch.len();
            let dp_id = opt_id(&dp_ids, i);
            dp_attrs.kvlist_into(dp_id, &mut scratch);
            // Encoding a point's own attributes is bounded by the attribute table. A data point
            // id that repeats (malformed input) would encode the same attributes again for every
            // point, so every repeat is charged.
            if dp_id.is_some_and(|id| !seen_points.insert(id)) {
                budget.charge(scratch.len() - part_len)?;
            }
            ids.push(finish_identity(
                &contexts.entries[at as usize],
                &scratch,
                &mut keys,
                limits,
                &mut budget,
            )?);
        }

        let (time, time_ns) =
            timestamp_pair(col(points, consts::TIME_UNIX_NANO), n, &mut out_of_range)?;
        let (start, start_ns) = timestamp_pair(
            col(points, consts::START_TIME_UNIX_NANO),
            n,
            &mut out_of_range,
        )?;
        let mut cols = vec![
            id_column(ids.iter().copied())?,
            mc.ctx.producer_ids(
                metric_rows.values().iter().map(|&m| m as usize),
                producer_attribute,
                &mut budget,
            )?,
            // The copied names were charged with `part_bytes`.
            take(&mc.name, &metric_rows, None)?,
            time,
            time_ns,
            start,
            start_ns,
            flags_i32(col(points, consts::FLAGS), n)?,
        ];
        let mut kind_cols = kind_columns(spec.kind, points, n, vs, &mut budget)?;
        for f in vs.fields().iter().skip(cols.len()) {
            cols.push(
                match kind_cols.iter().position(|(name, _)| name == f.name()) {
                    Some(at) => kind_cols.swap_remove(at).1,
                    None => new_null_array(f.data_type(), n),
                },
            );
        }
        let values = RecordBatch::try_new(vs.clone(), cols)?;
        check_rows(&values, limits)?;
        // Called once per point table: one series row per distinct series, charged to the budget.
        let mut series_rows = |rows: &UInt32Array| -> Result<RecordBatch, LakeError> {
            let mrows = as_u32(&take(&metric_rows, rows, None)?);
            let of = |m: &u32| *m as usize;
            let mut cols = vec![
                id_column(rows.values().iter().map(|&r| ids[r as usize]))?,
                identity_column(rows, &ids, &keys),
                emitted_at_placeholder(rows.len()),
            ];
            cols.extend(mc.ctx.series_columns(&mrows, &mut budget)?);
            cols.push(dp_attrs.build_map(&as_u32(&take(&dp_ids, rows, None)?), &mut budget)?);
            cols.extend([
                take_charged(&mc.name, &mrows, &mut budget)?,
                take_charged(&mc.unit, &mrows, &mut budget)?,
                strings(
                    mrows
                        .values()
                        .iter()
                        .map(|m| mc.kind[of(m)].map_or("", MetricKind::as_str)),
                ),
                strings(
                    mrows
                        .values()
                        .iter()
                        .map(|m| mc.temporality[of(m)].as_str()),
                ),
                Arc::new(BooleanArray::from(
                    mrows
                        .values()
                        .iter()
                        .map(|m| mc.monotonic[of(m)])
                        .collect::<Vec<bool>>(),
                )) as ArrayRef,
                take_charged(&mc.description, &mrows, &mut budget)?,
            ]);
            cols.extend(mc.ctx.ext_columns(&mrows)?);
            let batch = RecordBatch::try_new(ss.clone(), cols)?;
            check_rows(&batch, limits)?;
            Ok(batch)
        };
        split(values, &ids, &mut series_rows, limits, &mut out)?;
    }
    out.timestamps_out_of_range = out_of_range;
    out.charged = budget.used();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::parquet_lake_exporter::canonical::series_id;
    use crate::exporters::parquet_lake_exporter::config::MIN_VALUES_ROW_BYTES;
    use crate::exporters::parquet_lake_exporter::test_fixtures::{
        kv, logs_request, metrics_otap, metrics_request, replace_column, to_otap,
        try_replace_column,
    };
    use arrow::array::{ListBuilder, UInt64Builder};
    use arrow::compute::cast;
    use arrow::datatypes::{Float64Type, Int64Type, TimestampMicrosecondType};
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
        AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, ResourceLogs, ScopeLogs,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
        ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint,
        Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric, number_data_point,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use otel_arrow_dfe_pdata::{OtapPayload, OtlpProtoBytes, TryIntoWithOptions};
    use prost::Message;

    const KIB: usize = 1024;
    const MIB: usize = 1024 * KIB;

    /// Limits that refuse nothing a test builds.
    fn limits() -> Limits {
        Limits {
            max_extracted_bytes: 1 << 30,
            max_row_bytes: 1 << 30,
            max_nesting_depth: 32,
            max_chunk_bytes: 1 << 30,
        }
    }

    fn limits_with(change: impl FnOnce(&mut Limits)) -> Limits {
        let mut l = limits();
        change(&mut l);
        l
    }

    fn ids_of(batch: &RecordBatch) -> Vec<u128> {
        let a = batch.column(0).as_fixed_size_binary();
        (0..a.len())
            .map(|i| u128::from_be_bytes(a.value(i).try_into().expect("16 bytes")))
            .collect()
    }

    fn map_entries(batch: &RecordBatch, column: &str, row: usize) -> Vec<(String, String)> {
        let map = batch
            .column_by_name(column)
            .expect("column")
            .as_map()
            .value(row);
        let k = map.column(0).as_string::<i32>();
        let v = map.column(1).as_string::<i32>();
        let mut out: Vec<(String, String)> = (0..k.len())
            .map(|i| (k.value(i).to_owned(), v.value(i).to_owned()))
            .collect();
        out.sort();
        out
    }

    fn text(batch: &RecordBatch, column: &str, row: usize) -> String {
        batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column {column}"))
            .as_string::<i32>()
            .value(row)
            .to_owned()
    }

    fn logs_with(req: &ExportLogsServiceRequest, limits: &Limits) -> Result<Extracted, LakeError> {
        extract_logs(&to_otap(req), &Schemas::new(), limits, "host.id")
    }

    fn logs(req: &ExportLogsServiceRequest) -> Vec<Chunk> {
        logs_with(req, &limits()).expect("extract").chunks
    }

    fn metrics_with(
        req: &ExportMetricsServiceRequest,
        limits: &Limits,
    ) -> Result<Extracted, LakeError> {
        extract_metrics(&metrics_otap(req), &Schemas::new(), limits, "host.id")
    }

    fn metrics(req: &ExportMetricsServiceRequest) -> Vec<Chunk> {
        metrics_with(req, &limits()).expect("extract").chunks
    }

    /// The refusal of an extraction that must fail.
    fn refused(result: Result<Extracted, LakeError>) -> LakeError {
        result.err().expect("refused")
    }

    /// The `observed` size of a size refusal.
    fn observed(err: &LakeError) -> usize {
        match err {
            LakeError::TooLarge { observed, .. } => *observed,
            other => panic!("expected a size refusal, got {other}"),
        }
    }

    fn is_invalid(err: &LakeError, what: &str) {
        assert!(
            matches!(err, LakeError::Invalid(msg) if msg.contains(what)),
            "expected invalid content naming `{what}`, got {err}"
        );
    }

    fn all_ids(chunks: &[Chunk]) -> Vec<u128> {
        chunks.iter().flat_map(|c| ids_of(&c.values)).collect()
    }

    fn int_kv(key: &str, v: i64) -> KeyValue {
        KeyValue {
            key: key.to_owned(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(v)),
            }),
        }
    }

    /// One log record per resource, each resource with the given attributes.
    fn logs_with_resources(resources: Vec<Vec<KeyValue>>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: resources
                .into_iter()
                .enumerate()
                .map(|(i, attributes)| ResourceLogs {
                    resource: Some(Resource {
                        attributes,
                        ..Default::default()
                    }),
                    scope_logs: vec![ScopeLogs {
                        log_records: vec![LogRecord {
                            time_unix_nano: i as u64 + 1,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .collect(),
        }
    }

    /// One resource without attributes holding the given log records.
    fn logs_of(records: Vec<LogRecord>) -> ExportLogsServiceRequest {
        let mut req = logs_with_resources(vec![vec![]]);
        req.resource_logs[0].scope_logs[0].log_records = records;
        req
    }

    fn gauge_request(point_attrs: &[&str]) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource::default()),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "cpu".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: point_attrs
                                .iter()
                                .enumerate()
                                .map(|(i, a)| NumberDataPoint {
                                    time_unix_nano: i as u64 + 1,
                                    attributes: vec![kv("core", (*a).to_owned())],
                                    value: Some(number_data_point::Value::AsInt(i as i64)),
                                    ..Default::default()
                                })
                                .collect(),
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// One metric named `h` with the given data under an empty resource.
    fn metric_request(data: metric::Data) -> ExportMetricsServiceRequest {
        let mut req = gauge_request(&[]);
        let m = &mut req.resource_metrics[0].scope_metrics[0].metrics[0];
        m.name = "h".into();
        m.data = Some(data);
        req
    }

    fn histogram_request(point: HistogramDataPoint) -> ExportMetricsServiceRequest {
        metric_request(metric::Data::Histogram(Histogram {
            data_points: vec![point],
            aggregation_temporality: 1,
        }))
    }

    /// Scenario: 6 log records over 2 resources are extracted into one chunk.
    /// Guarantees: Every record is a values row; the shared series table has one row per resource whose ids match its id list, and the chunk refers to both; series rows carry the resource and scope columns; the values `attrs` map holds the 10 record attributes; the series `attrs` map is empty and non-null.
    #[test]
    fn logs_values_and_series_round_trip() {
        let chunks = logs(&logs_request(6, 2, 0));
        assert_eq!(chunks.len(), 1);
        let c = &chunks[0];
        assert_eq!(c.values.num_rows(), 6);
        assert_eq!(c.series.rows.num_rows(), 2);
        assert_eq!(ids_of(&c.series.rows), c.series.ids);
        assert_eq!(c.series.row_bytes.len(), 2);
        let mut rows = c.series_rows.clone();
        rows.sort_unstable();
        assert_eq!(rows, [0, 1]);
        let series: Vec<u128> = c.series_ids().collect();
        for id in ids_of(&c.values) {
            assert!(series.contains(&id));
        }
        for row in 0..2 {
            let res = map_entries(&c.series.rows, "resource_attrs", row);
            assert!(res.iter().any(|(k, _)| k == "host.name"), "{res:?}");
            assert!(map_entries(&c.series.rows, "attrs", row).is_empty());
        }
        let series_attrs = c.series.rows.column_by_name("attrs").expect("attrs");
        assert_eq!(series_attrs.null_count(), 0);
        assert_eq!(text(&c.series.rows, "scope_name", 0), "fixture");
        assert_eq!(map_entries(&c.values, "attrs", 0).len(), 10);
        assert_eq!(c.values.schema(), *Schemas::new().values(Signal::Logs));
        assert_eq!(c.series.rows.schema(), *Schemas::new().series(Signal::Logs));
    }

    /// Scenario: Records of one resource and scope differ in body, time and record attributes.
    /// Guarantees: Log identity is resource + scope only, so they share one series_id; a second resource gets another.
    #[test]
    fn log_identity_is_resource_and_scope_only() {
        let one = all_ids(&logs(&logs_request(20, 1, 0)));
        assert!(one.iter().all(|&id| id == one[0]));
        let mut two = all_ids(&logs(&logs_request(20, 2, 0)));
        two.sort_unstable();
        two.dedup();
        assert_eq!(two.len(), 2);
    }

    /// Scenario: One log record with two resource attributes (a string and an int), a resource schema URL, a scope with name, version, schema URL and one attribute is extracted; a gauge point with one point attribute is extracted too.
    /// Guarantees: The identity_bytes written in the series row are exactly the v1 field sequence (resource attributes, resource schema URL, scope name, version, schema URL, scope attributes, metric fields, identity attributes), and series_id is XXH3-128 of those bytes.
    #[test]
    fn identity_bytes_follow_the_v1_field_order() {
        use crate::exporters::parquet_lake_exporter::canonical::{
            Descriptor, MetricDescriptor, canonical_bytes,
        };
        use crate::exporters::parquet_lake_exporter::value::Value;
        let resource = vec![int_kv("n", 7), kv("host.id", "a1".into())];
        let mut req = logs_with_resources(vec![resource.clone()]);
        let rl = &mut req.resource_logs[0];
        rl.schema_url = "https://res.example/1".into();
        let sl = &mut rl.scope_logs[0];
        sl.schema_url = "https://scope.example/1".into();
        sl.scope = Some(InstrumentationScope {
            name: "lib".into(),
            version: "1".into(),
            attributes: vec![kv("k", "v".into())],
            ..Default::default()
        });
        let mut want = Descriptor {
            signal: Signal::Logs,
            resource_attrs: vec![
                ("host.id".into(), Value::Str("a1".into())),
                ("n".into(), Value::Int(7)),
            ],
            resource_schema_url: "https://res.example/1".into(),
            scope_name: "lib".into(),
            scope_version: "1".into(),
            scope_schema_url: "https://scope.example/1".into(),
            scope_attrs: vec![("k".into(), Value::Str("v".into()))],
            metric: None,
            attrs: vec![],
        };
        let c = &logs(&req)[0];
        let bytes = |batch: &RecordBatch| {
            batch
                .column_by_name("identity_bytes")
                .expect("identity_bytes")
                .as_binary::<i32>()
                .value(0)
                .to_vec()
        };
        assert_eq!(bytes(&c.series.rows), canonical_bytes(&want));
        assert_eq!(c.series.ids[0], series_id(&canonical_bytes(&want)));
        assert_eq!(ids_of(&c.series.rows)[0], c.series.ids[0]);

        let mut metrics = gauge_request(&["0"]);
        let rm = &mut metrics.resource_metrics[0];
        rm.resource = Some(Resource {
            attributes: resource,
            ..Default::default()
        });
        rm.schema_url = "https://res.example/1".into();
        rm.scope_metrics[0].schema_url = "https://scope.example/1".into();
        rm.scope_metrics[0].scope = Some(InstrumentationScope {
            name: "lib".into(),
            version: "1".into(),
            attributes: vec![kv("k", "v".into())],
            ..Default::default()
        });
        rm.scope_metrics[0].metrics[0].unit = "1".into();
        want.signal = Signal::Metrics;
        want.metric = Some(MetricDescriptor {
            name: "cpu".into(),
            unit: "1".into(),
            kind: MetricKind::Gauge,
            temporality: Temporality::Unspecified,
            is_monotonic: false,
        });
        want.attrs = vec![("core".into(), Value::Str("0".into()))];
        let chunks = self::metrics(&metrics);
        // The metric path hashes with a streaming state; it must equal the one-shot hash.
        assert_eq!(bytes(&chunks[0].series.rows), canonical_bytes(&want));
        assert_eq!(chunks[0].series.ids[0], series_id(&canonical_bytes(&want)));
    }

    /// Scenario: Three requests are identical except that one has a resource schema URL and another a scope schema URL.
    /// Guarantees: Both schema URLs are identity fields: the three requests get three different series ids.
    #[test]
    fn schema_urls_are_part_of_the_identity() {
        let plain = logs_request(1, 1, 0);
        let mut resource_url = plain.clone();
        resource_url.resource_logs[0].schema_url = "https://res.example/1".into();
        let mut scope_url = plain.clone();
        scope_url.resource_logs[0].scope_logs[0].schema_url = "https://res.example/1".into();
        let ids = [
            all_ids(&logs(&plain))[0],
            all_ids(&logs(&resource_url))[0],
            all_ids(&logs(&scope_url))[0],
        ];
        assert_ne!(ids[0], ids[1]);
        assert_ne!(ids[0], ids[2]);
        assert_ne!(ids[1], ids[2]);
    }

    /// Scenario: A log record has no severity, event name, flags, body, trace id, span id or time, and its resource has no producer attribute.
    /// Guarantees: The required v1 columns take their defaults (0 and ""), and the optional ones (body, trace_id, span_id, both time columns) are null.
    #[test]
    fn log_rows_take_v1_defaults() {
        let c = &logs(&logs_of(vec![LogRecord::default()]))[0];
        let v = &c.values;
        let int = |name: &str| {
            v.column_by_name(name)
                .expect("column")
                .as_primitive::<Int32Type>()
                .value(0)
        };
        assert_eq!(int("severity_number"), 0);
        assert_eq!(int("flags"), 0);
        for name in ["severity_text", "event_name", "producer_id"] {
            assert_eq!(text(v, name, 0), "", "{name}");
        }
        for name in ["body", "trace_id", "span_id", "time", "time_unix_nano"] {
            assert!(v.column_by_name(name).expect("column").is_null(0), "{name}");
        }
        for name in [
            "severity_number",
            "severity_text",
            "event_name",
            "flags",
            "producer_id",
        ] {
            assert_eq!(
                v.column_by_name(name).expect("column").null_count(),
                0,
                "{name}"
            );
        }
    }

    /// Scenario: A log record has time_unix_nano 1_700_000_000_123_456_789 and the same observed time.
    /// Guarantees: The nanosecond column holds the exact value, and the timestamp column holds it truncated to microseconds, for both time fields.
    #[test]
    fn timestamps_are_stored_twice() {
        let ns = 1_700_000_000_123_456_789_u64;
        let c = &logs(&logs_of(vec![LogRecord {
            time_unix_nano: ns,
            observed_time_unix_nano: ns,
            ..Default::default()
        }]))[0];
        for (micros, nanos) in [
            ("time", "time_unix_nano"),
            ("observed_time", "observed_time_unix_nano"),
        ] {
            let us = c.values.column_by_name(micros).expect("column");
            assert_eq!(
                us.as_primitive::<TimestampMicrosecondType>().value(0),
                1_700_000_000_123_456
            );
            let n = c.values.column_by_name(nanos).expect("column");
            assert_eq!(n.as_primitive::<Int64Type>().value(0), ns as i64);
        }
    }

    /// Scenario: A resource has host.id=a1 and service.name=svc; its logs and its gauge points are extracted with producer attribute host.id, then service.name.
    /// Guarantees: producer_id on every values row is the value of the configured resource attribute, for both signals.
    #[test]
    fn producer_id_is_the_configured_resource_attribute() {
        let resource = vec![kv("host.id", "a1".into()), kv("service.name", "svc".into())];
        let mut log_req = logs_with_resources(vec![resource.clone()]);
        let record = log_req.resource_logs[0].scope_logs[0].log_records[0].clone();
        log_req.resource_logs[0].scope_logs[0].log_records = vec![record; 3];
        let mut metric_req = gauge_request(&["0", "1"]);
        metric_req.resource_metrics[0].resource = Some(Resource {
            attributes: resource,
            ..Default::default()
        });
        for (attribute, want) in [("host.id", "a1"), ("service.name", "svc")] {
            let l = extract_logs(&to_otap(&log_req), &Schemas::new(), &limits(), attribute)
                .expect("logs")
                .chunks;
            let m = extract_metrics(
                &metrics_otap(&metric_req),
                &Schemas::new(),
                &limits(),
                attribute,
            )
            .expect("metrics")
            .chunks;
            for (chunk, rows) in [(&l[0], 3), (&m[0], 2)] {
                assert_eq!(chunk.values.num_rows(), rows);
                for row in 0..rows {
                    assert_eq!(text(&chunk.values, "producer_id", row), want);
                }
            }
        }
    }

    /// Scenario: A metrics payload holds one gauge(int), gauge(double), monotonic cumulative sum, delta histogram, delta exponential histogram and summary point.
    /// Guarantees: All six points land in the one wide values table with their kind's columns filled and the others null; series rows name every metric type with its temporality ("" for gauge and summary) and monotonic flag (true only for the sum); every values row carries its metric name.
    #[test]
    fn metrics_all_kinds_fill_the_wide_values_table() {
        let chunks = metrics(&metrics_request());
        let rows: usize = chunks.iter().map(|c| c.values.num_rows()).sum();
        assert_eq!(rows, 6);
        let non_null = |name: &str| -> usize {
            chunks
                .iter()
                .map(|c| {
                    let a = c.values.column_by_name(name).expect("column");
                    a.len() - a.null_count()
                })
                .sum()
        };
        assert_eq!(non_null("value_int"), 2);
        assert_eq!(non_null("value_double"), 1);
        assert_eq!(non_null("bucket_counts"), 1);
        assert_eq!(non_null("explicit_bounds"), 1);
        assert_eq!(non_null("scale"), 1);
        assert_eq!(non_null("positive_bucket_counts"), 1);
        assert_eq!(non_null("quantile_values"), 1);
        assert_eq!(non_null("count"), 3);
        let ints: Vec<i64> = chunks
            .iter()
            .flat_map(|c| {
                let a = c.values.column_by_name("value_int").expect("i");
                a.as_primitive::<Int64Type>()
                    .iter()
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert!(ints.contains(&42) && ints.contains(&10));
        let mut names: Vec<String> = Vec::new();
        let mut series: Vec<(String, String, bool)> = Vec::new();
        for c in &chunks {
            assert_eq!(c.values.schema(), *Schemas::new().values(Signal::Metrics));
            assert_eq!(
                c.series.rows.schema(),
                *Schemas::new().series(Signal::Metrics)
            );
            for row in 0..c.values.num_rows() {
                names.push(text(&c.values, "metric_name", row));
            }
            let monotonic = c.series.rows.column_by_name("is_monotonic").expect("flag");
            for row in 0..c.series.rows.num_rows() {
                series.push((
                    text(&c.series.rows, "metric_type", row),
                    text(&c.series.rows, "temporality", row),
                    monotonic.as_boolean().value(row),
                ));
            }
        }
        names.sort();
        assert_eq!(
            names,
            [
                "exp_histogram",
                "gauge.double",
                "gauge.int",
                "histogram",
                "sum.cumulative",
                "summary"
            ]
        );
        series.sort();
        series.dedup();
        let want = [
            ("exp_histogram", "delta", false),
            ("gauge", "", false),
            ("histogram", "delta", false),
            ("sum", "cumulative", true),
            ("summary", "", false),
        ];
        let got: Vec<(&str, &str, bool)> = series
            .iter()
            .map(|(t, temp, m)| (t.as_str(), temp.as_str(), *m))
            .collect();
        assert_eq!(got, want);
    }

    /// Scenario: An exponential histogram point leaves scale, zero_count and zero_threshold at
    /// their default of 0, so OTAP omits those all-default columns from the record.
    /// Guarantees: The exporter reads the absent columns as 0, not null, so a genuine
    /// scale/zero_count/zero_threshold of 0 (all plain, non-optional OTLP fields) is preserved
    /// instead of being stored as null.
    #[test]
    fn exp_histogram_zero_fields_are_stored_as_zero() {
        let point = ExponentialHistogramDataPoint {
            time_unix_nano: 1,
            count: 5,
            scale: 0,
            zero_count: 0,
            zero_threshold: 0.0,
            ..Default::default()
        };
        let req = metric_request(metric::Data::ExponentialHistogram(ExponentialHistogram {
            data_points: vec![point],
            aggregation_temporality: 1,
        }));
        let chunks = metrics(&req);
        assert_eq!(chunks.len(), 1);
        let v = &chunks[0].values;
        assert_eq!(v.num_rows(), 1);
        let scale = v.column_by_name("scale").expect("scale column");
        assert_eq!(scale.null_count(), 0, "scale must not be null");
        assert_eq!(scale.as_primitive::<Int32Type>().value(0), 0);
        let zero_count = v.column_by_name("zero_count").expect("zero_count column");
        assert_eq!(zero_count.null_count(), 0, "zero_count must not be null");
        assert_eq!(zero_count.as_primitive::<Int64Type>().value(0), 0);
        let zero_threshold = v
            .column_by_name("zero_threshold")
            .expect("zero_threshold column");
        assert_eq!(
            zero_threshold.null_count(),
            0,
            "zero_threshold must not be null"
        );
        assert_eq!(zero_threshold.as_primitive::<Float64Type>().value(0), 0.0);
    }

    /// Scenario: One gauge has three points with point attributes core=a, core=a and core=b.
    /// Guarantees: Point attributes are part of metric identity: equal attributes share a series_id, different ones do not, and the series row carries them in its `attrs` map.
    #[test]
    fn metric_identity_includes_point_attributes() {
        let chunks = metrics(&gauge_request(&["a", "a", "b"]));
        let ids = all_ids(&chunks);
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0], ids[1]);
        assert_ne!(ids[0], ids[2]);
        let table = &chunks[0].series.rows;
        assert_eq!(table.num_rows(), 2);
        let mut cores: Vec<_> = (0..2).map(|row| map_entries(table, "attrs", row)).collect();
        cores.sort();
        assert_eq!(
            cores,
            [
                vec![("core".to_owned(), "a".to_owned())],
                vec![("core".to_owned(), "b".to_owned())]
            ]
        );
    }

    /// Scenario: Metrics payloads break one rule each: two metric rows with one id; a sum without temporality; a point whose parent id matches no metric; a histogram with 2 counts and 2 bounds; a histogram count of u64::MAX; a bucket list with a null item.
    /// Guarantees: Each request is refused as invalid content naming the rule, and no chunk is returned.
    #[test]
    fn invalid_metrics_are_refused() {
        let run = |otap: &OtapArrowRecords| {
            refused(extract_metrics(otap, &Schemas::new(), &limits(), "host.id"))
        };
        // Two metric rows with the same id.
        let mut otap = metrics_otap(&metrics_request());
        let metrics = otap
            .get(ArrowPayloadType::UnivariateMetrics)
            .expect("metrics");
        let id = col(metrics, consts::ID).expect("id");
        let same = cast(
            &UInt32Array::from(vec![0; metrics.num_rows()]),
            id.data_type(),
        )
        .expect("cast");
        replace_column(
            &mut otap,
            ArrowPayloadType::UnivariateMetrics,
            consts::ID,
            same,
        );
        is_invalid(&run(&otap), "duplicate metric id");

        // A sum whose temporality is unspecified.
        let sum = metric_request(metric::Data::Sum(Sum {
            data_points: vec![NumberDataPoint {
                value: Some(number_data_point::Value::AsInt(1)),
                ..Default::default()
            }],
            aggregation_temporality: 0,
            is_monotonic: true,
        }));
        is_invalid(
            &refused(metrics_with(&sum, &limits())),
            "unspecified temporality",
        );

        // A point that refers to a metric id no metric row has.
        let mut otap = metrics_otap(&gauge_request(&["a"]));
        let points = otap
            .get(ArrowPayloadType::NumberDataPoints)
            .expect("points");
        let parent = col(points, consts::PARENT_ID).expect("parent_id");
        let unknown = cast(
            &UInt32Array::from(vec![999; points.num_rows()]),
            parent.data_type(),
        )
        .expect("cast");
        replace_column(
            &mut otap,
            ArrowPayloadType::NumberDataPoints,
            consts::PARENT_ID,
            unknown,
        );
        is_invalid(&run(&otap), "unknown metric");

        // Bucket counts must be one longer than the bounds (or both empty).
        let mismatched = histogram_request(HistogramDataPoint {
            count: 3,
            bucket_counts: vec![1, 2],
            explicit_bounds: vec![1.0, 5.0],
            ..Default::default()
        });
        is_invalid(
            &refused(metrics_with(&mismatched, &limits())),
            "bucket_counts.len != explicit_bounds.len + 1",
        );

        // A count the signed storage type cannot hold.
        let huge = histogram_request(HistogramDataPoint {
            count: u64::MAX,
            bucket_counts: vec![1, 2, 3],
            explicit_bounds: vec![1.0, 5.0],
            ..Default::default()
        });
        is_invalid(
            &refused(metrics_with(&huge, &limits())),
            "histogram count above i64::MAX",
        );
        let huge_bucket = histogram_request(HistogramDataPoint {
            count: 3,
            bucket_counts: vec![1, u64::MAX, 3],
            explicit_bounds: vec![1.0, 5.0],
            ..Default::default()
        });
        is_invalid(
            &refused(metrics_with(&huge_bucket, &limits())),
            "bucket count above i64::MAX",
        );

        // A bucket list with a null item.
        let mut otap = metrics_otap(&histogram_request(HistogramDataPoint {
            count: 3,
            bucket_counts: vec![1, 2, 3],
            explicit_bounds: vec![1.0, 5.0],
            ..Default::default()
        }));
        let mut counts = ListBuilder::new(UInt64Builder::new());
        counts.values().append_value(1);
        counts.values().append_null();
        counts.values().append_value(3);
        counts.append(true);
        replace_column(
            &mut otap,
            ArrowPayloadType::HistogramDataPoints,
            consts::HISTOGRAM_BUCKET_COUNTS,
            Arc::new(counts.finish()),
        );
        is_invalid(&run(&otap), "null bucket count");
    }

    /// Scenario: The parent id column of a point table is replaced by one holding a null.
    /// Guarantees: The OTAP schema check refuses the batch (parent_id is a required column), so a point without a parent metric cannot reach extraction through a validated payload; the check in `extract_metrics` is a second line of defense.
    #[test]
    fn null_parent_id_is_refused_by_the_otap_schema() {
        let mut otap = metrics_otap(&gauge_request(&["a", "b"]));
        let points = otap
            .get(ArrowPayloadType::NumberDataPoints)
            .expect("points");
        let parent = col(points, consts::PARENT_ID).expect("parent_id");
        let first = ids_u32(Some(parent), "points", consts::PARENT_ID)
            .expect("ids")
            .value(0);
        let with_null = cast(
            &UInt32Array::from(vec![Some(first), None]),
            parent.data_type(),
        )
        .expect("cast");
        let reason = try_replace_column(
            &mut otap,
            ArrowPayloadType::NumberDataPoints,
            consts::PARENT_ID,
            with_null,
        )
        .expect_err("refused by the schema check");
        assert!(reason.contains(consts::PARENT_ID), "{reason}");
    }

    /// Scenario: A resource has the key `dup` twice; separately, a log record has a duplicated attribute key.
    /// Guarantees: Both requests are refused as invalid content instead of one of the values being chosen.
    #[test]
    fn duplicate_attribute_key_refuses_the_request() {
        let resource =
            logs_with_resources(vec![vec![kv("dup", "a".into()), kv("dup", "z".into())]]);
        is_invalid(
            &refused(logs_with(&resource, &limits())),
            "duplicate attribute key",
        );
        let record = logs_of(vec![LogRecord {
            attributes: vec![kv("dup", "a".into()), kv("dup", "z".into())],
            ..Default::default()
        }]);
        is_invalid(
            &refused(logs_with(&record, &limits())),
            "duplicate attribute key",
        );
    }

    /// A log record with one attribute `nested` holding `value`.
    fn nested_attr(value: any_value::Value) -> ExportLogsServiceRequest {
        logs_of(vec![LogRecord {
            attributes: vec![KeyValue {
                key: "nested".into(),
                value: Some(AnyValue { value: Some(value) }),
            }],
            ..Default::default()
        }])
    }

    fn int_value(v: i64) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::IntValue(v)),
        }
    }

    fn array_of(values: Vec<AnyValue>) -> any_value::Value {
        any_value::Value::ArrayValue(ArrayValue { values })
    }

    /// Scenario: A log attribute holds the key/value list {b: [1, 2], a: "x"}; another holds an array nested 3 deep while the depth limit is 2.
    /// Guarantees: The first is stored as compact JSON with keys sorted; the second refuses the request as too deep, carrying the limit.
    #[test]
    fn nested_values_are_decoded_or_refused() {
        let kvlist = any_value::Value::KvlistValue(KeyValueList {
            values: vec![
                KeyValue {
                    key: "b".into(),
                    value: Some(AnyValue {
                        value: Some(array_of(vec![int_value(1), int_value(2)])),
                    }),
                },
                kv("a", "x".into()),
            ],
        });
        let c = &logs(&nested_attr(kvlist))[0];
        assert_eq!(
            map_entries(&c.values, "attrs", 0),
            [("nested".to_owned(), "{\"a\":\"x\",\"b\":[1,2]}".to_owned())]
        );

        let wrap = |inner: any_value::Value| array_of(vec![AnyValue { value: Some(inner) }]);
        let deep = wrap(wrap(array_of(vec![int_value(1)])));
        let err = refused(logs_with(
            &nested_attr(deep),
            &limits_with(|l| l.max_nesting_depth = 2),
        ));
        assert!(matches!(err, LakeError::TooDeep(2)), "{err}");
    }

    /// Scenario: (a) 2000 log records are extracted with a 64 KiB request limit; (b) one record has a 4 KiB body and (c) one resource has a 4 KiB attribute value, both with a 1 KiB row limit.
    /// Guarantees: (a) is refused naming `ingress.max_extracted_bytes`; (b) and (c) are refused naming `ingress.max_row_bytes`.
    #[test]
    fn limits_refuse_oversized_requests() {
        let err = refused(logs_with(
            &logs_request(2_000, 4, 0),
            &limits_with(|l| l.max_extracted_bytes = 64 * KIB),
        ));
        assert!(matches!(err, LakeError::TooLarge { .. }), "{err}");
        assert!(
            err.to_string().contains("ingress.max_extracted_bytes"),
            "{err}"
        );

        let row_limit = limits_with(|l| l.max_row_bytes = KIB);
        let big_body = logs_of(vec![LogRecord {
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue("b".repeat(4 * KIB))),
            }),
            ..Default::default()
        }]);
        let err = refused(logs_with(&big_body, &row_limit));
        assert!(err.to_string().contains("ingress.max_row_bytes"), "{err}");

        let big_resource = logs_with_resources(vec![vec![kv("big", "r".repeat(4 * KIB))]]);
        let err = refused(logs_with(&big_resource, &row_limit));
        assert!(err.to_string().contains("ingress.max_row_bytes"), "{err}");
    }

    /// Scenario: 10_000 log records without body or attributes, and 10_000 gauge points without attributes, are extracted.
    /// Guarantees: A values row measures at least the minimum the flush memory bound assumes, so the sort keys of a block never outnumber that bound.
    #[test]
    fn values_rows_are_at_least_the_assumed_minimum() {
        let log_req = logs_of(vec![LogRecord::default(); 10_000]);
        let mut metric_req = gauge_request(&["a"]);
        if let Some(metric::Data::Gauge(g)) =
            &mut metric_req.resource_metrics[0].scope_metrics[0].metrics[0].data
        {
            let mut point = g.data_points[0].clone();
            point.attributes.clear();
            g.data_points = vec![point; 10_000];
        }
        for (name, chunks) in [("logs", logs(&log_req)), ("metrics", metrics(&metric_req))] {
            let rows: usize = chunks.iter().map(|c| c.values.num_rows()).sum();
            let bytes: usize = chunks
                .iter()
                .map(|c| c.values.get_array_memory_size())
                .sum();
            assert_eq!(rows, 10_000, "{name}");
            assert!(
                bytes / rows >= MIN_VALUES_ROW_BYTES,
                "{name}: {}",
                bytes / rows
            );
        }
    }

    /// Scenario: 20k log records are extracted with a 64 KiB chunk budget.
    /// Guarantees: Every chunk's values measure within the budget (or the chunk holds one row), and the chunks cover every row exactly once.
    #[test]
    fn chunks_respect_max_chunk_bytes() {
        let max = 64 * KIB;
        let chunks = logs_with(
            &logs_request(20_000, 50, 0),
            &limits_with(|l| l.max_chunk_bytes = max),
        )
        .expect("extract")
        .chunks;
        assert!(chunks.len() > 1);
        for c in &chunks {
            let values = c.values.get_array_memory_size();
            assert!(values <= max || c.values.num_rows() == 1, "{values}");
            assert!(c.bytes() >= values);
        }
        let rows: usize = chunks.iter().map(|c| c.values.num_rows()).sum();
        assert_eq!(rows, 20_000);
    }

    /// Scenario: A 20k-row input is split with a budget far below its size.
    /// Guarantees: Chunks own compact buffers: a multi-row chunk measures within the budget instead of reporting its parent's buffers.
    #[test]
    fn chunk_bytes_are_owned_not_parent_sized() {
        let max = 64 * KIB;
        let otap = to_otap(&logs_request(20_000, 50, 0));
        let input = otap
            .get(ArrowPayloadType::Logs)
            .expect("logs")
            .get_array_memory_size();
        assert!(input > 10 * max);
        let l = limits_with(|l| l.max_chunk_bytes = max);
        let chunks = extract_logs(&otap, &Schemas::new(), &l, "host.id")
            .expect("extract")
            .chunks;
        assert!(chunks[0].values.num_rows() > 1);
        assert!(chunks[0].values.get_array_memory_size() <= max);
    }

    /// Scenario: The values batch of 4000 log records is split with a 16 KiB chunk budget while the request limit is 64 KiB.
    /// Guarantees: The split is refused naming `ingress.max_extracted_bytes` as soon as the running total passes the limit: the total is at most the limit plus one chunk, and only the chunks accepted before the limit exist, instead of every chunk being built first.
    #[test]
    fn extraction_stops_at_the_running_total() {
        let full = logs(&logs_request(4_000, 1, 0));
        assert_eq!(full.len(), 1);
        let values = full[0].values.clone();
        let total = values.get_array_memory_size();
        assert!(total > 256 * KIB, "{total}");
        let ids = ids_of(&values);
        let table = full[0].series.rows.clone();
        let l = limits_with(|l| {
            l.max_chunk_bytes = 16 * KIB;
            l.max_extracted_bytes = 64 * KIB;
        });
        let mut out = Extracted::default();
        let mut series = |rows: &UInt32Array| Ok(table.slice(0, rows.len()));
        let err = split(values, &ids, &mut series, &l, &mut out).expect_err("refused");
        assert!(
            err.to_string().contains("ingress.max_extracted_bytes"),
            "{err}"
        );
        assert!(out.bytes <= 80 * KIB, "{}", out.bytes);
        assert_eq!(observed(&err), out.bytes);
        assert!(!out.chunks.is_empty());
        let accepted: usize = out
            .chunks
            .iter()
            .map(|c| c.values.get_array_memory_size())
            .sum();
        assert!(accepted <= 64 * KIB, "{accepted}");
        let rows: usize = out.chunks.iter().map(|c| c.values.num_rows()).sum();
        assert!(rows < 4_000 / 2, "{rows}");
    }

    /// Scenario: A metrics payload has a metric but no data points.
    /// Guarantees: Extraction yields no chunks and no error.
    #[test]
    fn metrics_without_points_yield_no_chunks() {
        assert!(metrics(&gauge_request(&[])).is_empty());
    }

    /// Scenario: A resource and scope carry schema URLs and dropped-attribute counts.
    /// Guarantees: Series rows keep them: the schema URLs in their required columns, the counts in the additional Int64 columns.
    #[test]
    fn series_keep_schema_urls_and_dropped_counts() {
        let mut req = logs_request(2, 1, 0);
        let rl = &mut req.resource_logs[0];
        rl.schema_url = "https://res.example/1".into();
        let res = rl.resource.as_mut().expect("resource");
        res.dropped_attributes_count = 3;
        let sl = &mut rl.scope_logs[0];
        sl.schema_url = "https://scope.example/1".into();
        sl.scope.as_mut().expect("scope").dropped_attributes_count = 4;
        let c = &logs(&req)[0];
        let rows = &c.series.rows;
        let count = |name: &str| {
            rows.column_by_name(name)
                .expect("column")
                .as_primitive::<Int64Type>()
                .value(0)
        };
        assert_eq!(
            text(rows, "resource_schema_url", 0),
            "https://res.example/1"
        );
        assert_eq!(text(rows, "scope_schema_url", 0), "https://scope.example/1");
        assert_eq!(count("resource_dropped_attributes_count"), 3);
        assert_eq!(count("scope_dropped_attributes_count"), 4);
    }

    /// Scenario: A metrics payload has number data points but no univariate metrics table.
    /// Guarantees: Extraction fails (the batch is refused) instead of returning no chunks, which would ack the points unwritten.
    #[test]
    fn points_without_univariate_root_are_an_error() {
        let full = metrics_otap(&metrics_request());
        let mut records = OtapArrowRecords::Metrics(Default::default());
        let points = full
            .get(ArrowPayloadType::NumberDataPoints)
            .expect("points");
        records
            .set(ArrowPayloadType::NumberDataPoints, points.clone())
            .expect("set");
        let err = refused(extract_metrics(
            &records,
            &Schemas::new(),
            &limits(),
            "host.id",
        ));
        assert!(err.to_string().contains("univariate"), "{err}");
    }

    /// Scenario: The same logs arrive once as OTLP bytes and once as transport-optimized OTAP records.
    /// Guarantees: Both paths yield the same series ids, so ids do not depend on the wire representation.
    #[test]
    fn otlp_and_otap_inputs_give_same_series_id() {
        let req = logs_request(40, 4, 0);
        let from_otlp = all_ids(&logs(&req));
        let payload: OtapPayload =
            OtlpProtoBytes::ExportLogsRequest(req.encode_to_vec().into()).into();
        let mut records: OtapArrowRecords = payload.try_into_with_default().expect("otap");
        records.encode_transport_optimized().expect("optimize");
        records.decode_transport_optimized_ids().expect("decode");
        let from_otap = all_ids(
            &extract_logs(&records, &Schemas::new(), &limits(), "host.id")
                .expect("extract")
                .chunks,
        );
        let (mut a, mut b) = (from_otlp, from_otap);
        a.sort_unstable();
        b.sort_unstable();
        assert_eq!(a, b);
    }

    /// Scenario: Resource {k: 0} is alone in one batch (the OTAP encoder omits the all-default int column) and next to {j: 5} in another.
    /// Guarantees: The {k: 0} resource gets the same series_id and the same rendered resource_attrs in both batches.
    #[test]
    fn omitted_default_value_column_keeps_series_id() {
        let alone = logs(&logs_with_resources(vec![vec![int_kv("k", 0)]]));
        let mixed = logs(&logs_with_resources(vec![
            vec![int_kv("k", 0)],
            vec![int_kv("j", 5)],
        ]));
        let id = alone[0].series.ids[0];
        let row = mixed[0]
            .series
            .ids
            .iter()
            .position(|&x| x == id)
            .expect("same id in the mixed batch");
        let want = vec![("k".to_owned(), "0".to_owned())];
        assert_eq!(
            map_entries(&alone[0].series.rows, "resource_attrs", 0),
            want
        );
        assert_eq!(
            map_entries(&mixed[0].series.rows, "resource_attrs", row),
            want
        );
    }

    /// A gauge with `points` points whose point attribute cycles over `series` values, under one resource holding one attribute of `resource_bytes` bytes.
    fn round_robin_gauge(
        points: usize,
        series: usize,
        resource_bytes: usize,
    ) -> ExportMetricsServiceRequest {
        let attrs: Vec<String> = (0..points).map(|i| format!("s{}", i % series)).collect();
        let attrs: Vec<&str> = attrs.iter().map(String::as_str).collect();
        let mut req = gauge_request(&attrs);
        req.resource_metrics[0].resource = Some(Resource {
            attributes: vec![kv("big", "x".repeat(resource_bytes))],
            ..Default::default()
        });
        req
    }

    /// Scenario: 8000 gauge points cycle over 40 series under one resource with a 32 KiB attribute, and the chunk budget (64 KiB) forces many chunks.
    /// Guarantees: The 40 series rows are built once and shared by every chunk instead of being copied into each; the running total is exactly the series table plus the values chunks; it stays under 8 MiB, where a copy of the series rows per chunk would exceed 50 MiB. With a 512 KiB limit the request budget refuses the request before any chunk exists.
    #[test]
    fn series_rows_are_built_once_per_request() {
        let otap = metrics_otap(&round_robin_gauge(8_000, 40, 32 * KIB));
        let small_chunks = limits_with(|l| l.max_chunk_bytes = 64 * KIB);
        let out =
            extract_metrics(&otap, &Schemas::new(), &small_chunks, "host.id").expect("extract");
        assert!(out.chunks.len() > 10, "{}", out.chunks.len());
        let table = &out.chunks[0].series;
        assert_eq!(table.rows.num_rows(), 40);
        assert!(out.chunks.iter().all(|c| Rc::ptr_eq(&c.series, table)));
        let rows: usize = out.chunks.iter().map(|c| c.values.num_rows()).sum();
        assert_eq!(rows, 8_000);
        let values: usize = out
            .chunks
            .iter()
            .map(|c| c.values.get_array_memory_size())
            .sum();
        assert_eq!(out.bytes, table.rows.get_array_memory_size() + values);
        assert!(out.bytes < 8 * MIB, "{}", out.bytes);

        let tight = limits_with(|l| {
            l.max_chunk_bytes = 64 * KIB;
            l.max_extracted_bytes = 512 * KIB;
        });
        let err = refused(extract_metrics(&otap, &Schemas::new(), &tight, "host.id"));
        assert!(
            err.to_string().contains("ingress.max_extracted_bytes"),
            "{err}"
        );
    }

    /// Scenario: 2000 metrics with the same name (distinct metric rows, identical identity fields), one point each, sit under one resource with a 64 KiB attribute; the request limit is 4 MiB.
    /// Guarantees: The resource part of the identity is encoded and charged once, not once per metric row, so the request is accepted and charges under 2 MiB; all points share one series row.
    #[test]
    fn identical_metric_rows_share_one_context_prefix() {
        let mut req = gauge_request(&["a"]);
        let metric = req.resource_metrics[0].scope_metrics[0].metrics[0].clone();
        req.resource_metrics[0].scope_metrics[0].metrics = (0..2_000)
            .map(|i| {
                // The description is not an identity field; it keeps the metric rows distinct.
                let mut m = metric.clone();
                m.description = format!("d{i}");
                m
            })
            .collect();
        req.resource_metrics[0].resource = Some(Resource {
            attributes: vec![kv("big", "x".repeat(64 * KIB))],
            ..Default::default()
        });
        let l = limits_with(|l| l.max_extracted_bytes = 4 * MIB);
        let otap = metrics_otap(&req);
        let metric_rows = otap
            .get(ArrowPayloadType::UnivariateMetrics)
            .expect("metrics")
            .num_rows();
        assert_eq!(metric_rows, 2_000);
        let out = extract_metrics(&otap, &Schemas::new(), &l, "host.id").expect("extract");
        let rows: usize = out.chunks.iter().map(|c| c.values.num_rows()).sum();
        assert_eq!(rows, 2_000);
        assert_eq!(out.chunks[0].series.rows.num_rows(), 1);
        assert!(out.charged < 2 * MIB, "{}", out.charged);
    }

    /// Scenario: One resource with a 64 KiB attribute has 5000 scopes that differ only in their version (5000 distinct contexts, one log record each); the request limit is 4 MiB.
    /// Guarantees: Every distinct context is charged its encoded identity, so the request is refused by the budget after about 60 contexts instead of encoding and hashing the 64 KiB resource 5000 times: the observed size at refusal is within one identity of the limit.
    #[test]
    fn distinct_contexts_under_a_large_resource_are_charged() {
        let mut req = logs_with_resources(vec![vec![kv("big", "x".repeat(64 * KIB))]]);
        let scope_logs = req.resource_logs[0].scope_logs[0].clone();
        req.resource_logs[0].scope_logs = (0..5_000)
            .map(|i| {
                let mut sl = scope_logs.clone();
                sl.scope = Some(InstrumentationScope {
                    name: "lib".into(),
                    version: format!("v{i}"),
                    ..Default::default()
                });
                sl
            })
            .collect();
        let l = limits_with(|l| l.max_extracted_bytes = 4 * MIB);
        let err = refused(extract_logs(&to_otap(&req), &Schemas::new(), &l, "host.id"));
        assert!(
            err.to_string().contains("ingress.max_extracted_bytes"),
            "{err}"
        );
        assert!(observed(&err) < 4 * MIB + 256 * KIB, "{err}");
    }

    /// Scenario: A gauge whose unit is 64 KiB long has 200 points (the limit is 4 MiB), and the same gauge has 10 points.
    /// Guarantees: The metric part of the identity is charged for every point before it is copied and hashed, so 200 points are refused (12.8 MiB of work) and 10 points are accepted with at least 640 KiB charged.
    #[test]
    fn metric_part_is_charged_per_point() {
        let l = limits_with(|l| l.max_extracted_bytes = 4 * MIB);
        let with_points = |n: usize| {
            let attrs: Vec<String> = (0..n).map(|i| format!("s{i}")).collect();
            let attrs: Vec<&str> = attrs.iter().map(String::as_str).collect();
            let mut req = gauge_request(&attrs);
            req.resource_metrics[0].scope_metrics[0].metrics[0].unit = "u".repeat(64 * KIB);
            extract_metrics(&metrics_otap(&req), &Schemas::new(), &l, "host.id")
        };
        let err = refused(with_points(200));
        assert!(
            err.to_string().contains("ingress.max_extracted_bytes"),
            "{err}"
        );
        let ok = with_points(10).expect("accepted");
        assert!(ok.charged >= 10 * 64 * KIB, "{}", ok.charged);
    }

    /// Scenario: 200 gauge points all carry the data point id of the point whose attribute is 64 KiB long (malformed input built by replacing the id column); the limit is 4 MiB. The same points with their own ids are extracted too.
    /// Guarantees: Encoding the same attributes again for every point is charged, so the malformed request is refused by the budget; with distinct ids the request is accepted, because each point's attributes are encoded once.
    #[test]
    fn repeated_data_point_ids_are_charged() {
        let l = limits_with(|l| l.max_extracted_bytes = 4 * MIB);
        let attrs: Vec<String> = (0..200).map(|i| format!("s{i}")).collect();
        let attrs: Vec<&str> = attrs.iter().map(String::as_str).collect();
        let mut req = gauge_request(&attrs);
        if let Some(metric::Data::Gauge(g)) =
            &mut req.resource_metrics[0].scope_metrics[0].metrics[0].data
        {
            g.data_points[0].attributes = vec![kv("big", "x".repeat(64 * KIB))];
        }
        let mut otap = metrics_otap(&req);
        let _ = extract_metrics(&otap, &Schemas::new(), &l, "host.id")
            .expect("distinct ids are accepted");
        // The data point that owns the large attribute.
        let attrs = otap.get(ArrowPayloadType::NumberDpAttrs).expect("attrs");
        let keys = cast(
            col(attrs, consts::ATTRIBUTE_KEY).expect("key"),
            &DataType::Utf8,
        )
        .expect("keys");
        let big_row = keys
            .as_string::<i32>()
            .iter()
            .position(|k| k == Some("big"))
            .expect("the big attribute");
        let owner = ids_u32(col(attrs, consts::PARENT_ID), "attrs", consts::PARENT_ID)
            .expect("parents")
            .value(big_row);
        let points = otap
            .get(ArrowPayloadType::NumberDataPoints)
            .expect("points");
        let id = col(points, consts::ID).expect("id");
        let same = cast(
            &UInt32Array::from(vec![owner; points.num_rows()]),
            id.data_type(),
        )
        .expect("cast");
        replace_column(
            &mut otap,
            ArrowPayloadType::NumberDataPoints,
            consts::ID,
            same,
        );
        let err = refused(extract_metrics(&otap, &Schemas::new(), &l, "host.id"));
        assert!(
            err.to_string().contains("ingress.max_extracted_bytes"),
            "{err}"
        );
    }

    /// Scenario: 1000 log records of two resources are extracted; each resource has 200 attributes.
    /// Guarantees: The producer id attribute is looked up once per resource, not once per record.
    #[test]
    fn producer_id_is_looked_up_once_per_resource() {
        use crate::exporters::parquet_lake_exporter::attrs::RENDERED_LOOKUPS;
        let many = |tag: &str| -> Vec<KeyValue> {
            (0..200)
                .map(|i| kv(&format!("k{i:03}"), format!("{tag}{i}")))
                .collect()
        };
        let mut req = logs_with_resources(vec![many("a"), many("b")]);
        for rl in &mut req.resource_logs {
            let record = rl.scope_logs[0].log_records[0].clone();
            rl.scope_logs[0].log_records = vec![record; 500];
        }
        let otap = to_otap(&req);
        let before = RENDERED_LOOKUPS.with(std::cell::Cell::get);
        let out = extract_logs(&otap, &Schemas::new(), &limits(), "k100").expect("extract");
        assert_eq!(RENDERED_LOOKUPS.with(std::cell::Cell::get) - before, 2);
        let rows: usize = out.chunks.iter().map(|c| c.values.num_rows()).sum();
        assert_eq!(rows, 1_000);
        let ids: Vec<String> = out
            .chunks
            .iter()
            .flat_map(|c| (0..c.values.num_rows()).map(|r| text(&c.values, "producer_id", r)))
            .collect();
        assert_eq!(ids.iter().filter(|id| *id == "a100").count(), 500);
        assert_eq!(ids.iter().filter(|id| *id == "b100").count(), 500);
    }
}
