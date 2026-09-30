// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Test support: builds decoded OTAP logs/metrics payloads with realistic wide resource attributes
//! through the production decode path (OTLP bytes -> OtapPayload -> OtapArrowRecords).

use bytes::Bytes;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint,
    Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint,
    exponential_histogram_data_point, metric, number_data_point, summary_data_point,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::{OtapArrowRecords, OtapPayload, OtlpProtoBytes, TryIntoWithOptions};
use prost::Message;

/// Number of record-level attributes per log (matches the AC-8 / AC-9 workload).
pub const RECORD_ATTRS: usize = 10;

/// Base timestamp used by the fixtures.
const BASE_NS: u64 = 1_700_000_000_000_000_000;

/// String key/value attribute.
#[must_use]
pub fn kv(key: &str, value: String) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value)),
        }),
    }
}

/// Resource with host/env/service attributes plus a pod name, unique per `i`.
#[must_use]
pub fn resource(i: usize) -> Resource {
    Resource {
        attributes: vec![
            kv("host.name", format!("host-{i:04}")),
            kv(
                "deployment.environment",
                if i.is_multiple_of(2) {
                    "prod".into()
                } else {
                    "staging".into()
                },
            ),
            kv("service.name", format!("svc-{}", i % 10)),
            kv("k8s.pod.name", format!("pod-{i}")),
        ],
        ..Default::default()
    }
}

fn scope() -> InstrumentationScope {
    InstrumentationScope {
        name: "fixture".into(),
        version: "1.0".into(),
        ..Default::default()
    }
}

/// One OTLP logs request with exactly `rows` records spread over `resources` resources, each
/// resource's records contiguous (as one producer batch would be).
#[must_use]
pub fn logs_request(rows: usize, resources: usize, seed: usize) -> ExportLogsServiceRequest {
    let resources = resources.max(1);
    let base = rows / resources;
    let extra = rows % resources;
    let mut next = seed * rows;
    let resource_logs = (0..resources)
        .map(|r| (r, base + usize::from(r < extra)))
        .map(|(r, per_resource)| {
            let log_records = (0..per_resource)
                .map(|_| {
                    let n = next;
                    next += 1;
                    let mut trace_id = [0u8; 16];
                    trace_id[..8].copy_from_slice(&(n as u64 + 1).to_be_bytes());
                    let mut span_id = [0u8; 8];
                    span_id.copy_from_slice(&(n as u64 + 7).to_be_bytes());
                    LogRecord {
                        time_unix_nano: BASE_NS + n as u64,
                        observed_time_unix_nano: BASE_NS + n as u64,
                        severity_number: 9 + (n % 4) as i32,
                        severity_text: "INFO".into(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(format!(
                                "request {n} handled"
                            ))),
                        }),
                        attributes: (0..RECORD_ATTRS)
                            .map(|a| kv(&format!("attr.{a}"), format!("v{}", (n + a) % 97)))
                            .collect(),
                        trace_id: trace_id.to_vec(),
                        span_id: span_id.to_vec(),
                        ..Default::default()
                    }
                })
                .collect();
            ResourceLogs {
                resource: Some(resource(r)),
                scope_logs: vec![ScopeLogs {
                    scope: Some(scope()),
                    log_records,
                    ..Default::default()
                }],
                ..Default::default()
            }
        })
        .filter(|rl| !rl.scope_logs[0].log_records.is_empty() || rows == 0)
        .collect();
    ExportLogsServiceRequest { resource_logs }
}

/// Decode an OTLP logs request through the production path into id-decoded OTAP records.
#[must_use]
pub fn to_otap(req: &ExportLogsServiceRequest) -> OtapArrowRecords {
    decode(OtlpProtoBytes::ExportLogsRequest(Bytes::from(
        req.encode_to_vec(),
    )))
}

fn number(name: &str, value: number_data_point::Value) -> NumberDataPoint {
    NumberDataPoint {
        start_time_unix_nano: BASE_NS,
        time_unix_nano: BASE_NS + 1,
        attributes: vec![kv("point", name.to_owned())],
        value: Some(value),
        ..Default::default()
    }
}

/// OTLP metrics request with one metric of every kind under one resource.
#[must_use]
pub fn metrics_request() -> ExportMetricsServiceRequest {
    let metrics = vec![
        Metric {
            name: "gauge.int".into(),
            unit: "1".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number("gauge.int", number_data_point::Value::AsInt(42))],
            })),
            ..Default::default()
        },
        Metric {
            name: "gauge.double".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number(
                    "gauge.double",
                    number_data_point::Value::AsDouble(0.5),
                )],
            })),
            ..Default::default()
        },
        Metric {
            name: "sum.cumulative".into(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![number(
                    "sum.cumulative",
                    number_data_point::Value::AsInt(10),
                )],
                aggregation_temporality: 2,
                is_monotonic: true,
            })),
            ..Default::default()
        },
        Metric {
            name: "histogram".into(),
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    start_time_unix_nano: BASE_NS,
                    time_unix_nano: BASE_NS + 1,
                    count: 6,
                    sum: Some(12.0),
                    bucket_counts: vec![1, 2, 3],
                    explicit_bounds: vec![1.0, 5.0],
                    min: Some(0.5),
                    max: Some(9.0),
                    ..Default::default()
                }],
                aggregation_temporality: 1,
            })),
            ..Default::default()
        },
        Metric {
            name: "exp_histogram".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    start_time_unix_nano: BASE_NS,
                    time_unix_nano: BASE_NS + 1,
                    count: 22,
                    sum: Some(40.0),
                    scale: 3,
                    zero_count: 7,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 1,
                        bucket_counts: vec![4, 5],
                    }),
                    negative: Some(exponential_histogram_data_point::Buckets {
                        offset: -2,
                        bucket_counts: vec![6],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: 1,
            })),
            ..Default::default()
        },
        Metric {
            name: "summary".into(),
            data: Some(metric::Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    start_time_unix_nano: BASE_NS,
                    time_unix_nano: BASE_NS + 1,
                    count: 3,
                    sum: 30.0,
                    quantile_values: vec![
                        summary_data_point::ValueAtQuantile {
                            quantile: 0.5,
                            value: 10.0,
                        },
                        summary_data_point::ValueAtQuantile {
                            quantile: 0.99,
                            value: 20.0,
                        },
                    ],
                    ..Default::default()
                }],
            })),
            ..Default::default()
        },
    ];
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(resource(0)),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(scope()),
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Decode an OTLP metrics request through the production path into id-decoded OTAP records.
#[must_use]
pub fn metrics_otap(req: &ExportMetricsServiceRequest) -> OtapArrowRecords {
    decode(OtlpProtoBytes::ExportMetricsRequest(Bytes::from(
        req.encode_to_vec(),
    )))
}

fn decode(bytes: OtlpProtoBytes) -> OtapArrowRecords {
    let payload: OtapPayload = bytes.into();
    let mut records: OtapArrowRecords = payload
        .try_into_with_default()
        .expect("valid OTLP converts to OTAP");
    records
        .decode_transport_optimized_ids()
        .expect("transport ids decode");
    records
}

/// OTLP logs request bytes as an `OtapPayload` (for node-level tests that send pdata).
#[must_use]
pub fn logs_payload(rows: usize, resources: usize, seed: usize) -> OtapPayload {
    OtlpProtoBytes::ExportLogsRequest(Bytes::from(
        logs_request(rows, resources, seed).encode_to_vec(),
    ))
    .into()
}

/// OTLP metrics request bytes as an `OtapPayload` (for node-level tests that send pdata).
#[must_use]
pub fn metrics_payload() -> OtapPayload {
    OtlpProtoBytes::ExportMetricsRequest(Bytes::from(metrics_request().encode_to_vec())).into()
}
