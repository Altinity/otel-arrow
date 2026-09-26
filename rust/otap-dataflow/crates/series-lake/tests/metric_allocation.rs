// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Heap measurement of metrics extraction under dhat's allocator: its own
//! binary, because the allocator is process-wide.

use std::sync::Arc;

use arrow::array::{ArrayRef, DictionaryArray, RecordBatch, StringArray, UInt8Array, UInt16Array};
use arrow::datatypes::{DataType, Field, Schema, UInt16Type};
use otel_arrow_dfe_pdata::otap::{Metrics, OtapArrowRecords};
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
    number_data_point,
};
use otel_arrow_dfe_pdata::schema::consts::{DESCRIPTION, ID, METRIC_TYPE, NAME};
use otel_arrow_dfe_pdata::testing::round_trip::encode_metrics;
use otel_arrow_dfe_series_lake::config::LakeConfig;
use otel_arrow_dfe_series_lake::extract::extract;
use otel_arrow_dfe_series_lake::{Error, RefuseReason};

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

/// dhat's counters are process-wide, so the tests of this binary measure one
/// at a time.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// `count` gauges named `m{i}`, one point each.
fn gauges(count: usize) -> MetricsData {
    let metrics = (0..count)
        .map(|i| Metric {
            name: format!("m{i}"),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: 1,
                    value: Some(number_data_point::Value::AsInt(1)),
                    ..Default::default()
                }],
            })),
            ..Default::default()
        })
        .collect();
    MetricsData {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// `batch` with column `name` replaced by (or extended with) `column`.
fn with_column(batch: &RecordBatch, name: &str, column: ArrayRef) -> RecordBatch {
    let mut fields: Vec<Field> = Vec::new();
    let mut cols: Vec<ArrayRef> = Vec::new();
    for (i, f) in batch.schema().fields().iter().enumerate() {
        if f.name() != name {
            fields.push(f.as_ref().clone());
            cols.push(Arc::clone(batch.column(i)));
        }
    }
    fields.push(Field::new(name, column.data_type().clone(), true));
    cols.push(column);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).expect("batch")
}

/// Scenario: 2,048 gauges whose `description` is one 256 KiB value of a u16 dictionary that every
/// metric row references, extracted under dhat with the default limits.
/// Guarantees: the request is refused as too large and the heap peak stays under twice
/// `max_extracted_bytes`; the description is not copied once per metric before any budget applies.
#[test]
fn a_shared_dictionary_description_is_not_copied_per_metric() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    const METRICS: usize = 2_048;
    let mut records = encode_metrics(&gauges(METRICS));
    let batch = records
        .get(ArrowPayloadType::UnivariateMetrics)
        .expect("metrics")
        .clone();
    let description = DictionaryArray::<UInt16Type>::new(
        UInt16Array::from(vec![0_u16; METRICS]),
        Arc::new(StringArray::from(vec!["d".repeat(256 << 10)])),
    );
    let batch = with_column(&batch, DESCRIPTION, Arc::new(description));
    records
        .set(ArrowPayloadType::UnivariateMetrics, batch)
        .expect("pdata accepts a dictionary description");
    let cfg = LakeConfig::default();

    let profiler = dhat::Profiler::builder().testing().build();
    let result = extract(&mut records, &cfg);
    let peak = dhat::HeapStats::get().max_bytes;
    drop(profiler);

    assert!(
        matches!(
            result,
            Err(Error::Refused(RefuseReason::RequestTooLarge(_)))
        ),
        "{:?}",
        result.err()
    );
    let bound = 2 * cfg.ingress.max_extracted_bytes;
    assert!(peak < bound, "heap peak {peak} over {bound}");
}

/// Scenario: a `UnivariateMetrics` batch of one million gauge rows, all with id 0, extracted
/// under dhat.
/// Guarantees: it is refused as invalid with a heap peak under 16 MiB; no per-metric table is sized
/// for more rows than u16 ids can name.
#[test]
fn a_metrics_batch_past_the_u16_id_space_is_refused_before_allocating() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    const ROWS: usize = 1_000_000;
    let schema = Schema::new(vec![
        Field::new(ID, DataType::UInt16, false),
        Field::new(METRIC_TYPE, DataType::UInt8, false),
        Field::new(
            NAME,
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            false,
        ),
    ]);
    let ids = UInt16Array::from(vec![0_u16; ROWS]);
    let kinds = UInt8Array::from(vec![1_u8; ROWS]);
    let names = DictionaryArray::<UInt16Type>::new(
        UInt16Array::from(vec![0_u16; ROWS]),
        Arc::new(StringArray::from(vec!["m"])),
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(ids), Arc::new(kinds), Arc::new(names)],
    )
    .expect("batch");
    let mut records = OtapArrowRecords::Metrics(Metrics::default());
    records
        .set(ArrowPayloadType::UnivariateMetrics, batch)
        .expect("pdata accepts the batch");

    let profiler = dhat::Profiler::builder().testing().build();
    let result = extract(&mut records, &LakeConfig::default());
    let peak = dhat::HeapStats::get().max_bytes;
    drop(profiler);

    assert!(
        matches!(result, Err(Error::Refused(RefuseReason::Invalid(_)))),
        "{:?}",
        result.err()
    );
    assert!(peak < 16 << 20, "heap peak {peak}");
}
