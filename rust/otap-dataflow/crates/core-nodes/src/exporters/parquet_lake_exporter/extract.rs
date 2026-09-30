// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! OTAP -> (values rows, series candidate rows, series ids), split into owned chunks under a byte
//! budget.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Int32Array, RecordBatch, StringArray,
    StructArray, TimestampNanosecondArray, UInt8Array, UInt32Array, new_null_array,
};
use arrow::compute::{take, take_record_batch};
use arrow::datatypes::DataType;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_dfe_pdata::schema::consts;

use super::anyvalue::AnyValueColumns;
use super::attrs::AttrIndex;
use super::columns::{as_u32, cast_or_null, col, ids_u32, opt_ids_u32, struct_field};
use super::error::LakeError;
use super::identity::{Signal, key_prefix, put_bytes, series_id};
use super::schema::{Schemas, ts_ns};

/// Owned rows of one chunk: values rows, one series candidate row per distinct id in the chunk,
/// and those ids.
pub struct Chunk {
    /// Values rows (schema: `Schemas::values(signal)`).
    pub values: RecordBatch,
    /// Series candidate rows (schema: `Schemas::series(signal)`), aligned with `series_ids`.
    pub series: RecordBatch,
    /// series_id of each series candidate row (distinct).
    pub series_ids: Vec<u128>,
}

impl Chunk {
    /// Memory of the chunk. Accurate because every column is freshly built by `take` (no shared
    /// parent buffers; all output columns are non-dictionary).
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.values.get_array_memory_size() + self.series.get_array_memory_size()
    }
}

/// FixedSizeBinary(16) column of big-endian series ids (at least one id).
fn id_column(ids: impl IntoIterator<Item = u128>) -> Result<ArrayRef, LakeError> {
    Ok(Arc::new(FixedSizeBinaryArray::try_from_iter(
        ids.into_iter().map(u128::to_be_bytes),
    )?))
}

/// Non-null `written_at` placeholder; the encoder stamps the flush time.
fn zero_timestamps(n: usize) -> ArrayRef {
    Arc::new(TimestampNanosecondArray::from(vec![0; n]).with_timezone("UTC"))
}

fn downcast<T: Array + 'static>(a: &ArrayRef) -> &T {
    a.as_any()
        .downcast_ref::<T>()
        .expect("cast_or_null returns the requested type")
}

fn opt_id(ids: &UInt32Array, i: usize) -> Option<u32> {
    (!ids.is_null(i)).then(|| ids.value(i))
}

fn str_or_empty(a: &StringArray, i: Option<usize>) -> &str {
    match i {
        Some(i) if !a.is_null(i) => a.value(i),
        _ => "",
    }
}

/// Split `values` (all rows of one input table, row r has id `ids[r]`) into owned chunks of at most
/// `max_bytes`. `series_rows(rows)` builds series candidate rows for the given input row indices.
/// A chunk that measures over budget is halved until it fits or holds one row.
fn split(
    values: &RecordBatch,
    ids: &[u128],
    series_rows: &dyn Fn(&UInt32Array) -> Result<RecordBatch, LakeError>,
    max_bytes: usize,
    out: &mut Vec<Chunk>,
) -> Result<(), LakeError> {
    let rows = values.num_rows();
    let per_row = (values.get_array_memory_size() / rows.max(1)).max(1);
    let initial_step = (max_bytes / per_row).max(1);
    let mut step = initial_step;
    let mut start = 0;
    while start < rows {
        let n = step.min(rows - start);
        let range = UInt32Array::from_iter_values(start as u32..(start + n) as u32);
        let mut seen = HashSet::new();
        let firsts: Vec<u32> = (start..start + n)
            .filter(|&r| seen.insert(ids[r]))
            .map(|r| r as u32)
            .collect();
        let chunk = Chunk {
            values: take_record_batch(values, &range)?,
            series_ids: firsts.iter().map(|&r| ids[r as usize]).collect(),
            series: series_rows(&UInt32Array::from(firsts))?,
        };
        if chunk.bytes() > max_bytes && n > 1 {
            step = n / 2;
            continue;
        }
        out.push(chunk);
        start += n;
        step = initial_step; // a skewed stretch does not shrink the rest of the payload
    }
    Ok(())
}

/// Resource and scope identity inputs of a root table (logs or univariate metrics).
struct Context {
    rids: UInt32Array,
    sids: UInt32Array,
    scope_name: StringArray,
    scope_version: StringArray,
    /// resource.schema_url, resource.dropped_attributes_count, schema_url (scope),
    /// scope.dropped_attributes_count: series tail columns.
    tail: [ArrayRef; 4],
    res: AttrIndex,
    scope: AttrIndex,
}

impl Context {
    fn new(records: &OtapArrowRecords, root: &RecordBatch) -> Result<Self, LakeError> {
        let n = root.num_rows();
        let utf8 = |field: &str| -> Result<StringArray, LakeError> {
            let a = cast_or_null(
                struct_field(root, consts::SCOPE, field).as_ref(),
                &DataType::Utf8,
                n,
            )?;
            Ok(downcast::<StringArray>(&a).clone())
        };
        Ok(Self {
            rids: opt_ids_u32(struct_field(root, consts::RESOURCE, consts::ID), n)?,
            sids: opt_ids_u32(struct_field(root, consts::SCOPE, consts::ID), n)?,
            scope_name: utf8(consts::NAME)?,
            scope_version: utf8(consts::VERSION)?,
            tail: [
                cast_or_null(
                    struct_field(root, consts::RESOURCE, consts::SCHEMA_URL).as_ref(),
                    &DataType::Utf8,
                    n,
                )?,
                cast_or_null(
                    struct_field(root, consts::RESOURCE, consts::DROPPED_ATTRIBUTES_COUNT).as_ref(),
                    &DataType::UInt32,
                    n,
                )?,
                cast_or_null(col(root, consts::SCHEMA_URL), &DataType::Utf8, n)?,
                cast_or_null(
                    struct_field(root, consts::SCOPE, consts::DROPPED_ATTRIBUTES_COUNT).as_ref(),
                    &DataType::UInt32,
                    n,
                )?,
            ],
            res: AttrIndex::from_batch(
                records.get(ArrowPayloadType::ResourceAttrs),
                "resource_attrs",
            )?,
            scope: AttrIndex::from_batch(records.get(ArrowPayloadType::ScopeAttrs), "scope_attrs")?,
        })
    }

    /// `attrs(resource) || bytes(scope.name) || bytes(scope.version) || attrs(scope)` of root row
    /// `i` (all defaults when None).
    fn key_into(&self, i: Option<usize>, out: &mut Vec<u8>) {
        self.res
            .canonical_into(i.and_then(|i| opt_id(&self.rids, i)), out);
        put_bytes(out, str_or_empty(&self.scope_name, i).as_bytes());
        put_bytes(out, str_or_empty(&self.scope_version, i).as_bytes());
        self.scope
            .canonical_into(i.and_then(|i| opt_id(&self.sids, i)), out);
    }

    /// `resource_attributes`, `scope_name`, `scope_version`, `scope_attributes` for root rows.
    fn series_columns(
        &self,
        rows: &UInt32Array,
        signal: Signal,
        schemas: &Schemas,
    ) -> Result<Vec<ArrayRef>, LakeError> {
        let schema = schemas.series(signal);
        let rids = as_u32(&take(&self.rids, rows, None)?);
        let sids = as_u32(&take(&self.sids, rows, None)?);
        Ok(vec![
            self.res.build_map(&rids, schema.field(2))?,
            take(&self.scope_name, rows, None)?,
            take(&self.scope_version, rows, None)?,
            self.scope.build_map(&sids, schema.field(5))?,
        ])
    }

    /// The series tail columns (resource/scope schema_url and dropped counts) for root rows.
    fn tail_columns(&self, rows: &UInt32Array) -> Result<Vec<ArrayRef>, LakeError> {
        self.tail
            .iter()
            .map(|a| Ok(take(a.as_ref(), rows, None)?))
            .collect()
    }
}

/// Logs of `records` as owned chunks (empty when the payload has no log rows).
pub fn extract_logs(
    records: &OtapArrowRecords,
    schemas: &Schemas,
    max_bytes: usize,
) -> Result<Vec<Chunk>, LakeError> {
    let Some(logs) = records.get(ArrowPayloadType::Logs) else {
        return Ok(Vec::new());
    };
    let n = logs.num_rows();
    if n == 0 {
        return Ok(Vec::new());
    }
    let ctx = Context::new(records, logs)?;
    let mut memo = HashMap::new();
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let k = (
            opt_id(&ctx.rids, i),
            opt_id(&ctx.sids, i),
            str_or_empty(&ctx.scope_name, Some(i)),
            str_or_empty(&ctx.scope_version, Some(i)),
        );
        let id = match memo.entry(k) {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                let mut key = key_prefix(Signal::Logs);
                ctx.key_into(Some(i), &mut key);
                *e.insert(series_id(&key))
            }
        };
        ids.push(id);
    }

    let log_ids = opt_ids_u32(col(logs, consts::ID).cloned(), n)?;
    let log_attrs = AttrIndex::from_batch(records.get(ArrowPayloadType::LogAttrs), "log_attrs")?;
    let body: ArrayRef =
        match col(logs, consts::BODY).and_then(|b| b.as_any().downcast_ref::<StructArray>()) {
            Some(body) => Arc::new(
                AnyValueColumns::new(n, |name| body.column_by_name(name).cloned())?
                    .render_where(|i| body.is_valid(i)),
            ),
            None => new_null_array(&DataType::Utf8, n),
        };
    let vs = schemas.values(Signal::Logs);
    let values = RecordBatch::try_new(
        vs.clone(),
        vec![
            id_column(ids.iter().copied())?,
            cast_or_null(col(logs, consts::TIME_UNIX_NANO), &ts_ns(), n)?,
            cast_or_null(col(logs, consts::OBSERVED_TIME_UNIX_NANO), &ts_ns(), n)?,
            cast_or_null(col(logs, consts::SEVERITY_NUMBER), &DataType::Int32, n)?,
            cast_or_null(col(logs, consts::SEVERITY_TEXT), &DataType::Utf8, n)?,
            body,
            log_attrs.build_map(&log_ids, vs.field(6))?,
            cast_or_null(
                col(logs, consts::TRACE_ID),
                &DataType::FixedSizeBinary(16),
                n,
            )?,
            cast_or_null(col(logs, consts::SPAN_ID), &DataType::FixedSizeBinary(8), n)?,
            cast_or_null(col(logs, consts::FLAGS), &DataType::UInt32, n)?,
            cast_or_null(col(logs, consts::EVENT_NAME), &DataType::Utf8, n)?,
            cast_or_null(
                col(logs, consts::DROPPED_ATTRIBUTES_COUNT),
                &DataType::UInt32,
                n,
            )?,
        ],
    )?;
    let series_rows = |rows: &UInt32Array| -> Result<RecordBatch, LakeError> {
        let mut cols = vec![
            id_column(rows.values().iter().map(|&r| ids[r as usize]))?,
            zero_timestamps(rows.len()),
        ];
        cols.extend(ctx.series_columns(rows, Signal::Logs, schemas)?);
        cols.extend(ctx.tail_columns(rows)?);
        Ok(RecordBatch::try_new(
            schemas.series(Signal::Logs).clone(),
            cols,
        )?)
    };
    let mut out = Vec::new();
    split(&values, &ids, &series_rows, max_bytes, &mut out)?;
    Ok(out)
}

/// Where a kind-specific values column comes from in the data point table.
enum Src {
    /// Top-level column.
    Top(&'static str),
    /// Field of a struct column.
    Nested(&'static str, &'static str),
}

/// One data point table: where it lives and which wide values columns it fills.
struct DpSpec {
    points: ArrowPayloadType,
    points_name: &'static str,
    attrs: ArrowPayloadType,
    attrs_name: &'static str,
    /// (values column name, source) of the kind-specific columns.
    columns: &'static [(&'static str, Src)],
}

const DP_SPECS: [DpSpec; 4] = [
    DpSpec {
        points: ArrowPayloadType::NumberDataPoints,
        points_name: "number_data_points",
        attrs: ArrowPayloadType::NumberDpAttrs,
        attrs_name: "number_dp_attrs",
        columns: &[
            ("int_value", Src::Top(consts::INT_VALUE)),
            ("double_value", Src::Top(consts::DOUBLE_VALUE)),
        ],
    },
    DpSpec {
        points: ArrowPayloadType::HistogramDataPoints,
        points_name: "histogram_data_points",
        attrs: ArrowPayloadType::HistogramDpAttrs,
        attrs_name: "histogram_dp_attrs",
        columns: &[
            ("count", Src::Top(consts::HISTOGRAM_COUNT)),
            ("sum", Src::Top(consts::HISTOGRAM_SUM)),
            ("min", Src::Top(consts::HISTOGRAM_MIN)),
            ("max", Src::Top(consts::HISTOGRAM_MAX)),
            ("bucket_counts", Src::Top(consts::HISTOGRAM_BUCKET_COUNTS)),
            (
                "explicit_bounds",
                Src::Top(consts::HISTOGRAM_EXPLICIT_BOUNDS),
            ),
        ],
    },
    DpSpec {
        points: ArrowPayloadType::ExpHistogramDataPoints,
        points_name: "exp_histogram_data_points",
        attrs: ArrowPayloadType::ExpHistogramDpAttrs,
        attrs_name: "exp_histogram_dp_attrs",
        columns: &[
            ("count", Src::Top(consts::HISTOGRAM_COUNT)),
            ("sum", Src::Top(consts::HISTOGRAM_SUM)),
            ("min", Src::Top(consts::HISTOGRAM_MIN)),
            ("max", Src::Top(consts::HISTOGRAM_MAX)),
            ("scale", Src::Top(consts::EXP_HISTOGRAM_SCALE)),
            ("zero_count", Src::Top(consts::EXP_HISTOGRAM_ZERO_COUNT)),
            (
                "zero_threshold",
                Src::Top(consts::EXP_HISTOGRAM_ZERO_THRESHOLD),
            ),
            (
                "positive_offset",
                Src::Nested(consts::EXP_HISTOGRAM_POSITIVE, consts::EXP_HISTOGRAM_OFFSET),
            ),
            (
                "positive_bucket_counts",
                Src::Nested(
                    consts::EXP_HISTOGRAM_POSITIVE,
                    consts::EXP_HISTOGRAM_BUCKET_COUNTS,
                ),
            ),
            (
                "negative_offset",
                Src::Nested(consts::EXP_HISTOGRAM_NEGATIVE, consts::EXP_HISTOGRAM_OFFSET),
            ),
            (
                "negative_bucket_counts",
                Src::Nested(
                    consts::EXP_HISTOGRAM_NEGATIVE,
                    consts::EXP_HISTOGRAM_BUCKET_COUNTS,
                ),
            ),
        ],
    },
    DpSpec {
        points: ArrowPayloadType::SummaryDataPoints,
        points_name: "summary_data_points",
        attrs: ArrowPayloadType::SummaryDpAttrs,
        attrs_name: "summary_dp_attrs",
        columns: &[
            ("count", Src::Top(consts::SUMMARY_COUNT)),
            ("sum", Src::Top(consts::SUMMARY_SUM)),
            ("quantile_values", Src::Top(consts::SUMMARY_QUANTILE_VALUES)),
        ],
    },
];

/// OTAP metric_type values: 1 gauge, 2 sum, 3 histogram, 4 exponential histogram, 5 summary.
fn metric_type_column(types: &ArrayRef) -> ArrayRef {
    let types = downcast::<UInt8Array>(types);
    Arc::new(
        (0..types.len())
            .map(|i| match (!types.is_null(i)).then(|| types.value(i)) {
                Some(1) => Some("gauge"),
                Some(2) => Some("sum"),
                Some(3) => Some("histogram"),
                Some(4) => Some("exponential_histogram"),
                Some(5) => Some("summary"),
                _ => None,
            })
            .collect::<StringArray>(),
    )
}

/// Metric-level identity and series columns of the univariate metrics table.
struct MetricColumns {
    ctx: Context,
    name: ArrayRef,
    description: ArrayRef,
    unit: ArrayRef,
    mtype: ArrayRef,
    temporality: ArrayRef,
    monotonic: ArrayRef,
}

impl MetricColumns {
    /// Key prefix up to (excluding) the point attributes for metric row `m` (defaults when None).
    fn prefix(&self, m: Option<usize>) -> Vec<u8> {
        let mut key = key_prefix(Signal::Metrics);
        self.ctx.key_into(m, &mut key);
        put_bytes(
            &mut key,
            str_or_empty(downcast::<StringArray>(&self.name), m).as_bytes(),
        );
        put_bytes(
            &mut key,
            str_or_empty(downcast::<StringArray>(&self.unit), m).as_bytes(),
        );
        let valid = |a: &ArrayRef| m.filter(|&i| !a.is_null(i));
        key.push(valid(&self.mtype).map_or(0, |i| downcast::<UInt8Array>(&self.mtype).value(i)));
        let temporality = valid(&self.temporality)
            .map_or(0, |i| downcast::<Int32Array>(&self.temporality).value(i));
        key.extend_from_slice(&temporality.to_le_bytes());
        key.push(u8::from(valid(&self.monotonic).is_some_and(|i| {
            downcast::<BooleanArray>(&self.monotonic).value(i)
        })));
        key
    }
}

/// Data points of all four kinds of `records` as owned chunks of the wide values table, plus the
/// number of orphan points (no matching metric row), which are kept under an empty metric identity.
pub fn extract_metrics(
    records: &OtapArrowRecords,
    schemas: &Schemas,
    max_bytes: usize,
) -> Result<(Vec<Chunk>, u64), LakeError> {
    // Metrics without any data point table produce no rows; do not require the id column then.
    if DP_SPECS
        .iter()
        .all(|s| records.get(s.points).is_none_or(|b| b.num_rows() == 0))
    {
        return Ok((Vec::new(), 0));
    }
    // Points without a univariate root (e.g. a MultivariateMetrics payload) are refused, never
    // acked unwritten.
    let Some(metrics) = records.get(ArrowPayloadType::UnivariateMetrics) else {
        return Err(LakeError::Conversion(
            "metrics without a univariate metrics table are not supported".into(),
        ));
    };
    let m = metrics.num_rows();
    let metric_ids = ids_u32(col(metrics, consts::ID), "univariate_metrics", consts::ID)?;
    let row_of: HashMap<u32, u32> = (0..m)
        .filter(|&i| !metric_ids.is_null(i))
        .map(|i| (metric_ids.value(i), i as u32))
        .collect();
    let mc = MetricColumns {
        ctx: Context::new(records, metrics)?,
        name: cast_or_null(col(metrics, consts::NAME), &DataType::Utf8, m)?,
        description: cast_or_null(col(metrics, consts::DESCRIPTION), &DataType::Utf8, m)?,
        unit: cast_or_null(col(metrics, consts::UNIT), &DataType::Utf8, m)?,
        mtype: cast_or_null(col(metrics, consts::METRIC_TYPE), &DataType::UInt8, m)?,
        temporality: cast_or_null(
            col(metrics, consts::AGGREGATION_TEMPORALITY),
            &DataType::Int32,
            m,
        )?,
        monotonic: cast_or_null(col(metrics, consts::IS_MONOTONIC), &DataType::Boolean, m)?,
    };
    let vs = schemas.values(Signal::Metrics);
    let ss = schemas.series(Signal::Metrics);
    let mut prefixes: HashMap<Option<u32>, Vec<u8>> = HashMap::new();
    let mut out = Vec::new();
    let mut orphans = 0u64;
    for spec in &DP_SPECS {
        let Some(points) = records.get(spec.points) else {
            continue;
        };
        let n = points.num_rows();
        if n == 0 {
            continue;
        }
        let parents = ids_u32(
            col(points, consts::PARENT_ID),
            spec.points_name,
            consts::PARENT_ID,
        )?;
        let metric_rows: UInt32Array = (0..n)
            .map(|i| opt_id(&parents, i).and_then(|p| row_of.get(&p).copied()))
            .collect();
        orphans += metric_rows.null_count() as u64;
        let dp_ids = opt_ids_u32(col(points, consts::ID).cloned(), n)?;
        let dp_attrs = AttrIndex::from_batch(records.get(spec.attrs), spec.attrs_name)?;
        let mut ids = Vec::with_capacity(n);
        for i in 0..n {
            let mrow = opt_id(&metric_rows, i);
            let mut key = match prefixes.entry(mrow) {
                Entry::Occupied(e) => e.get().clone(),
                Entry::Vacant(e) => e.insert(mc.prefix(mrow.map(|r| r as usize))).clone(),
            };
            dp_attrs.canonical_into(opt_id(&dp_ids, i), &mut key);
            ids.push(series_id(&key));
        }

        let mut cols = vec![
            id_column(ids.iter().copied())?,
            cast_or_null(col(points, consts::START_TIME_UNIX_NANO), &ts_ns(), n)?,
            cast_or_null(col(points, consts::TIME_UNIX_NANO), &ts_ns(), n)?,
            cast_or_null(col(points, consts::FLAGS), &DataType::UInt32, n)?,
        ];
        for f in vs.fields().iter().skip(cols.len()) {
            let src = spec.columns.iter().find(|(name, _)| name == f.name());
            cols.push(match src {
                Some((_, Src::Top(c))) => cast_or_null(col(points, c), f.data_type(), n)?,
                Some((_, Src::Nested(s, c))) => {
                    cast_or_null(struct_field(points, s, c).as_ref(), f.data_type(), n)?
                }
                None => new_null_array(f.data_type(), n),
            });
        }
        let values = RecordBatch::try_new(vs.clone(), cols)?;
        let series_rows = |rows: &UInt32Array| -> Result<RecordBatch, LakeError> {
            let mrows = as_u32(&take(&metric_rows, rows, None)?);
            let gather = |a: &ArrayRef| take(a.as_ref(), &mrows, None);
            let mut cols = vec![
                id_column(rows.values().iter().map(|&r| ids[r as usize]))?,
                zero_timestamps(rows.len()),
            ];
            cols.extend(mc.ctx.series_columns(&mrows, Signal::Metrics, schemas)?);
            cols.extend([
                gather(&mc.name)?,
                gather(&mc.description)?,
                gather(&mc.unit)?,
                metric_type_column(&gather(&mc.mtype)?),
                gather(&mc.temporality)?,
                gather(&mc.monotonic)?,
                dp_attrs.build_map(&as_u32(&take(&dp_ids, rows, None)?), ss.field(12))?,
            ]);
            cols.extend(mc.ctx.tail_columns(&mrows)?);
            Ok(RecordBatch::try_new(ss.clone(), cols)?)
        };
        split(&values, &ids, &series_rows, max_bytes, &mut out)?;
    }
    Ok((out, orphans))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporters::parquet_lake_exporter::test_fixtures::{
        kv, logs_request, metrics_otap, metrics_request, to_otap,
    };
    use arrow::array::{Int64Array, MapArray};
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, ResourceLogs, ScopeLogs,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use otel_arrow_dfe_pdata::{OtapPayload, OtlpProtoBytes, TryIntoWithOptions};
    use prost::Message;

    const BIG: usize = 1 << 30;

    fn ids_of(batch: &RecordBatch) -> Vec<u128> {
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("series_id");
        (0..a.len())
            .map(|i| u128::from_be_bytes(a.value(i).try_into().expect("16 bytes")))
            .collect()
    }

    fn map_entries(batch: &RecordBatch, column: &str, row: usize) -> Vec<(String, String)> {
        let map = batch
            .column_by_name(column)
            .expect("column")
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map")
            .value(row);
        let k = downcast::<StringArray>(map.column(0));
        let v = downcast::<StringArray>(map.column(1));
        let mut out: Vec<(String, String)> = (0..k.len())
            .map(|i| (k.value(i).to_owned(), v.value(i).to_owned()))
            .collect();
        out.sort();
        out
    }

    fn logs(req: &ExportLogsServiceRequest, max: usize) -> Vec<Chunk> {
        extract_logs(&to_otap(req), &Schemas::new(), max).expect("extract")
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

    /// Scenario: 6 log records over 2 resources are extracted into one chunk.
    /// Guarantees: Every record is a values row, the chunk has one series candidate per resource, every values id has a candidate, and candidates carry the resource and scope columns.
    #[test]
    fn logs_values_and_series_round_trip() {
        let chunks = logs(&logs_request(6, 2, 0), BIG);
        assert_eq!(chunks.len(), 1);
        let c = &chunks[0];
        assert_eq!(c.values.num_rows(), 6);
        assert_eq!(c.series.num_rows(), 2);
        assert_eq!(ids_of(&c.series), c.series_ids);
        for id in ids_of(&c.values) {
            assert!(c.series_ids.contains(&id));
        }
        for row in 0..2 {
            let res = map_entries(&c.series, "resource_attributes", row);
            assert!(res.iter().any(|(k, _)| k == "host.name"), "{res:?}");
        }
        let scope = c.series.column_by_name("scope_name").expect("scope");
        assert_eq!(downcast::<StringArray>(scope).value(0), "fixture");
        let attrs = map_entries(&c.values, "attributes", 0);
        assert_eq!(attrs.len(), 10);
    }

    /// Scenario: Records of one resource and scope differ in body, time and record attributes.
    /// Guarantees: Log identity is resource + scope only, so they share one series_id; a second resource gets another.
    #[test]
    fn log_identity_is_resource_and_scope_only() {
        let one = all_ids(&logs(&logs_request(20, 1, 0), BIG));
        assert!(one.iter().all(|&id| id == one[0]));
        let mut two = all_ids(&logs(&logs_request(20, 2, 0), BIG));
        two.sort_unstable();
        two.dedup();
        assert_eq!(two.len(), 2);
    }

    /// Scenario: A metrics payload holds one gauge(int), gauge(double), sum, histogram, exponential histogram and summary point.
    /// Guarantees: All six points land in the one wide values table with their kind's columns filled, and series rows name every metric type.
    #[test]
    fn metrics_all_kinds_fill_the_wide_values_table() {
        let chunks = extract_metrics(&metrics_otap(&metrics_request()), &Schemas::new(), BIG)
            .expect("ok")
            .0;
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
        assert_eq!(non_null("int_value"), 2);
        assert_eq!(non_null("double_value"), 1);
        assert_eq!(non_null("bucket_counts"), 1);
        assert_eq!(non_null("scale"), 1);
        assert_eq!(non_null("positive_bucket_counts"), 1);
        assert_eq!(non_null("quantile_values"), 1);
        assert_eq!(non_null("count"), 3);
        let ints: Vec<i64> = chunks
            .iter()
            .flat_map(|c| {
                let a = downcast::<Int64Array>(c.values.column_by_name("int_value").expect("i"));
                a.iter().flatten().collect::<Vec<_>>()
            })
            .collect();
        assert!(ints.contains(&42) && ints.contains(&10));
        let mut types: Vec<String> = chunks
            .iter()
            .flat_map(|c| {
                let a = downcast::<StringArray>(c.series.column_by_name("metric_type").expect("t"));
                a.iter().flatten().map(str::to_owned).collect::<Vec<_>>()
            })
            .collect();
        types.sort();
        types.dedup();
        assert_eq!(
            types,
            [
                "exponential_histogram",
                "gauge",
                "histogram",
                "sum",
                "summary"
            ]
        );
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

    /// Scenario: One gauge has three points with point attributes core=a, core=a and core=b.
    /// Guarantees: Point attributes are part of metric identity: equal attributes share a series_id, different ones do not.
    #[test]
    fn metric_identity_includes_point_attributes() {
        let chunks = extract_metrics(
            &metrics_otap(&gauge_request(&["a", "a", "b"])),
            &Schemas::new(),
            BIG,
        )
        .expect("ok")
        .0;
        let ids = all_ids(&chunks);
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0], ids[1]);
        assert_ne!(ids[0], ids[2]);
        assert_eq!(chunks[0].series.num_rows(), 2);
    }

    /// Scenario: 20k log records are extracted with a 64 KiB chunk budget.
    /// Guarantees: Every chunk measures within the budget (or holds one row), and the chunks cover every row exactly once.
    #[test]
    fn chunks_respect_max_chunk_bytes() {
        let max = 64 * 1024;
        let chunks = logs(&logs_request(20_000, 50, 0), max);
        assert!(chunks.len() > 1);
        for c in &chunks {
            assert!(
                c.bytes() <= max || c.values.num_rows() == 1,
                "{}",
                c.bytes()
            );
        }
        let rows: usize = chunks.iter().map(|c| c.values.num_rows()).sum();
        assert_eq!(rows, 20_000);
    }

    /// Scenario: A 20k-row input is split with a budget far below its size.
    /// Guarantees: Chunks own compact buffers: a multi-row chunk measures within the budget instead of reporting its parent's buffers.
    #[test]
    fn chunk_bytes_are_owned_not_parent_sized() {
        let max = 64 * 1024;
        let otap = to_otap(&logs_request(20_000, 50, 0));
        let input = otap
            .get(ArrowPayloadType::Logs)
            .expect("logs")
            .get_array_memory_size();
        assert!(input > 10 * max);
        let chunks = extract_logs(&otap, &Schemas::new(), max).expect("extract");
        assert!(chunks[0].values.num_rows() > 1);
        assert!(chunks[0].bytes() <= max);
    }

    /// Scenario: A metrics payload has a metric but no data points.
    /// Guarantees: Extraction yields no chunks and no error.
    #[test]
    fn metrics_without_points_yield_no_chunks() {
        let chunks = extract_metrics(&metrics_otap(&gauge_request(&[])), &Schemas::new(), BIG)
            .expect("ok")
            .0;
        assert!(chunks.is_empty());
    }

    /// Scenario: A resource and scope carry schema URLs and dropped-attribute counts.
    /// Guarantees: Series rows keep them in the resource/scope schema_url and dropped_attributes_count columns instead of dropping them.
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
        let c = &logs(&req, BIG)[0];
        let text = |name: &str| {
            downcast::<StringArray>(c.series.column_by_name(name).expect("column"))
                .value(0)
                .to_owned()
        };
        let count = |name: &str| {
            downcast::<UInt32Array>(c.series.column_by_name(name).expect("column")).value(0)
        };
        assert_eq!(text("resource_schema_url"), "https://res.example/1");
        assert_eq!(text("scope_schema_url"), "https://scope.example/1");
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
        let err = extract_metrics(&records, &Schemas::new(), BIG)
            .err()
            .expect("refused");
        assert!(err.to_string().contains("univariate"), "{err}");
    }

    /// Scenario: The same logs, with a duplicate resource key, arrive once as OTLP bytes and once as transport-optimized OTAP records.
    /// Guarantees: Both paths yield the same series ids, so ids do not depend on the wire representation or attribute row order.
    #[test]
    fn otlp_and_otap_inputs_give_same_series_id() {
        let mut req = logs_request(40, 4, 0);
        for rl in &mut req.resource_logs {
            let res = rl.resource.as_mut().expect("resource");
            res.attributes.push(kv("dup", "z".into()));
            res.attributes.insert(0, kv("dup", "a".into()));
        }
        let from_otlp = all_ids(&logs(&req, BIG));
        let payload: OtapPayload =
            OtlpProtoBytes::ExportLogsRequest(req.encode_to_vec().into()).into();
        let mut records: OtapArrowRecords = payload.try_into_with_default().expect("otap");
        records.encode_transport_optimized().expect("optimize");
        records.decode_transport_optimized_ids().expect("decode");
        let from_otap = all_ids(&extract_logs(&records, &Schemas::new(), BIG).expect("extract"));
        let (mut a, mut b) = (from_otlp, from_otap);
        a.sort_unstable();
        b.sort_unstable();
        assert_eq!(a, b);
    }

    /// Scenario: Resource {k: 0} is alone in one batch (the OTAP encoder omits the all-default int column) and next to {j: 5} in another.
    /// Guarantees: The {k: 0} resource gets the same series_id and the same rendered resource_attributes in both batches.
    #[test]
    fn omitted_default_value_column_keeps_series_id() {
        let alone = logs(&logs_with_resources(vec![vec![int_kv("k", 0)]]), BIG);
        let mixed = logs(
            &logs_with_resources(vec![vec![int_kv("k", 0)], vec![int_kv("j", 5)]]),
            BIG,
        );
        let id = alone[0].series_ids[0];
        let row = mixed[0]
            .series_ids
            .iter()
            .position(|&x| x == id)
            .expect("same id in the mixed batch");
        let want = vec![("k".to_owned(), "0".to_owned())];
        assert_eq!(
            map_entries(&alone[0].series, "resource_attributes", 0),
            want
        );
        assert_eq!(
            map_entries(&mixed[0].series, "resource_attributes", row),
            want
        );
    }
}
