// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Test support: builds decoded OTAP logs/metrics payloads with realistic wide resource attributes
//! through the production decode path (OTLP bytes -> OtapPayload -> OtapArrowRecords), and reads
//! the golden vectors of Series Lake Format v1.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema};
use bytes::Bytes;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    AnyValue, ArrayValue, EntityRef, InstrumentationScope, KeyValue, KeyValueList, any_value,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
    HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary,
    SummaryDataPoint, exemplar, exponential_histogram_data_point, metric, number_data_point,
    summary_data_point,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::{OtapArrowRecords, OtapPayload, OtlpProtoBytes, TryIntoWithOptions};
use prost::Message;
use prost::encoding::{WireType, encode_key, encode_varint};

use super::canonical::{Descriptor, MetricDescriptor, MetricKind, Signal, Temporality};
use super::value::Value;

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

/// A golden vector file of Series Lake Format v1 (copied from the reference implementation).
#[must_use]
pub fn golden(name: &str) -> serde_json::Value {
    let raw = match name {
        "canonical_v1" => include_str!("testdata/golden/canonical_v1.json"),
        "cbor_v1" => include_str!("testdata/golden/cbor_v1.json"),
        "render_v1" => include_str!("testdata/golden/render_v1.json"),
        "identity_config_v1" => include_str!("testdata/golden/identity_config_v1.json"),
        other => panic!("unknown golden file {other}"),
    };
    serde_json::from_str(raw).expect("golden json")
}

/// Bytes of a hex string.
#[must_use]
pub fn hex_decode(s: &str) -> Vec<u8> {
    hex::decode(s).expect("hex")
}

/// A value of the golden files: `{"type": ..., "value" | "bits" | "items" | "entries": ...}`.
#[must_use]
pub fn value_from_json(j: &serde_json::Value) -> Value {
    match j["type"].as_str().expect("type") {
        "null" => Value::Null,
        "str" => Value::Str(j["value"].as_str().expect("str").to_owned()),
        "bytes" => Value::Bytes(hex_decode(j["value"].as_str().expect("hex"))),
        "int" => Value::Int(j["value"].as_i64().expect("int")),
        "double" => match j.get("bits") {
            Some(bits) => Value::Double(f64::from_bits(bits.as_u64().expect("bits"))),
            None => Value::Double(j["value"].as_f64().expect("double")),
        },
        "bool" => Value::Bool(j["value"].as_bool().expect("bool")),
        "array" => Value::Array(
            j["items"]
                .as_array()
                .expect("items")
                .iter()
                .map(value_from_json)
                .collect(),
        ),
        "kvlist" => Value::KvList(kv_from_json(&j["entries"])),
        other => panic!("unknown type {other}"),
    }
}

fn kv_from_json(j: &serde_json::Value) -> Vec<(String, Value)> {
    let mut v: Vec<(String, Value)> = j
        .as_array()
        .map(|a| {
            a.iter()
                .map(|e| {
                    (
                        e["key"].as_str().expect("key").to_owned(),
                        value_from_json(&e["value"]),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    v.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    v
}

/// A descriptor of the golden files.
#[must_use]
pub fn descriptor_from_json(j: &serde_json::Value) -> Descriptor {
    let text =
        |j: &serde_json::Value, k: &str| j.get(k).and_then(|v| v.as_str()).unwrap_or("").to_owned();
    let signal = match j["signal"].as_str().expect("signal") {
        "logs" => Signal::Logs,
        _ => Signal::Metrics,
    };
    let metric = j.get("metric").map(|m| MetricDescriptor {
        name: text(m, "name"),
        unit: text(m, "unit"),
        kind: match m["kind"].as_str().expect("kind") {
            "gauge" => MetricKind::Gauge,
            "sum" => MetricKind::Sum,
            "histogram" => MetricKind::Histogram,
            "exp_histogram" => MetricKind::ExpHistogram,
            "summary" => MetricKind::Summary,
            other => panic!("unknown kind {other}"),
        },
        temporality: match text(m, "temporality").as_str() {
            "delta" => Temporality::Delta,
            "cumulative" => Temporality::Cumulative,
            _ => Temporality::Unspecified,
        },
        is_monotonic: m
            .get("is_monotonic")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
    });
    Descriptor {
        signal,
        resource_attrs: kv_from_json(&j["resource_attrs"]),
        resource_schema_url: text(j, "resource_schema_url"),
        scope_name: text(j, "scope_name"),
        scope_version: text(j, "scope_version"),
        scope_schema_url: text(j, "scope_schema_url"),
        scope_attrs: kv_from_json(&j["scope_attrs"]),
        metric,
        attrs: kv_from_json(&j["attrs"]),
    }
}

/// A logs request of `rows` records under one resource, each with a body of `body_bytes` bytes
/// (sizes a request for the budget tests).
#[must_use]
pub fn sized_logs_request(rows: usize, body_bytes: usize, seed: usize) -> ExportLogsServiceRequest {
    let mut req = logs_request(rows, 1, seed);
    for (i, record) in req.resource_logs[0].scope_logs[0]
        .log_records
        .iter_mut()
        .enumerate()
    {
        let mut body = format!("{seed}-{i}-");
        while body.len() < body_bytes {
            body.push(char::from(b'a' + ((seed + i + body.len()) % 26) as u8));
        }
        body.truncate(body_bytes);
        record.body = Some(AnyValue {
            value: Some(any_value::Value::StringValue(body)),
        });
    }
    req
}

/// [`sized_logs_request`] as OTLP bytes (for node-level tests that send pdata).
#[must_use]
pub fn sized_logs_payload(rows: usize, body_bytes: usize, seed: usize) -> OtapPayload {
    OtlpProtoBytes::ExportLogsRequest(Bytes::from(
        sized_logs_request(rows, body_bytes, seed).encode_to_vec(),
    ))
    .into()
}

/// Replace column `name` of the OTAP batch `pt` of `records` by `column` (builds input a well-behaved
/// encoder would not send). The field takes the type of the new column and becomes nullable.
/// Returns the reason when the OTAP schema check of `OtapArrowRecords::set` refuses the batch, in
/// which case the exporter can never see such input.
pub fn try_replace_column(
    records: &mut OtapArrowRecords,
    pt: ArrowPayloadType,
    name: &str,
    column: ArrayRef,
) -> Result<(), String> {
    let batch = records.get(pt).expect("payload present").clone();
    let at = batch.schema().index_of(name).expect("column present");
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields[at] = Field::new(name, column.data_type().clone(), true)
        .with_metadata(fields[at].metadata().clone());
    let mut columns = batch.columns().to_vec();
    columns[at] = column;
    let schema = Schema::new_with_metadata(fields, batch.schema().metadata().clone());
    let batch = RecordBatch::try_new(Arc::new(schema), columns).expect("batch");
    records.set(pt, batch).map_err(|e| e.to_string())
}

/// [`try_replace_column`] for a replacement the OTAP schema accepts.
pub fn replace_column(
    records: &mut OtapArrowRecords,
    pt: ArrowPayloadType,
    name: &str,
    column: ArrayRef,
) {
    try_replace_column(records, pt, name, column).expect("the OTAP schema accepts the column");
}

/// Marker that [`corrupt_utf8`] turns into invalid UTF-8 after a request is encoded.
pub const BAD: &str = "<<BAD>>";

/// A corrupted [`BAD`] marker after repair: each of its three invalid bytes becomes U+FFFD.
pub const REPAIRED: &str = "<<\u{FFFD}\u{FFFD}\u{FFFD}>>";

/// Replace every [`BAD`] marker in encoded bytes by `<<` 0xFF 0xFE 0xFD `>>`. The length is
/// unchanged, so every protobuf length prefix stays valid, but the string is no longer UTF-8.
/// Returns how many markers were replaced.
pub fn corrupt_utf8(bytes: &mut [u8]) -> u64 {
    let marker = BAD.as_bytes();
    let mut count = 0;
    let mut i = 0;
    while i + marker.len() <= bytes.len() {
        if bytes[i..].starts_with(marker) {
            bytes[i + 2..i + 5].copy_from_slice(&[0xff, 0xfe, 0xfd]);
            count += 1;
            i += marker.len();
        } else {
            i += 1;
        }
    }
    count
}

/// A logs request in which every OTLP string field holds `m` once, after a prefix naming the
/// field: both schema URLs, the scope name and version, resource, scope and record attribute keys
/// and values, an entity ref (all four strings), severity text, event name, a string body, a
/// kvlist body whose key and nested array item hold `m`, and a record attribute `nested_attr` with
/// the same nesting (kvlist key and array item). A third record is clean (body `clean`).
#[must_use]
pub fn every_string_logs(m: &str) -> ExportLogsServiceRequest {
    let s = |prefix: &str| AnyValue {
        value: Some(any_value::Value::StringValue(format!("{prefix}{m}"))),
    };
    let kvs = |prefix: &str| {
        vec![KeyValue {
            key: format!("{prefix}.key{m}"),
            value: Some(s(&format!("{prefix}.value"))),
        }]
    };
    let record = |time: u64, body: AnyValue, prefix: &str| LogRecord {
        time_unix_nano: BASE_NS + time,
        severity_text: format!("{prefix}.severity{m}"),
        event_name: format!("{prefix}.event{m}"),
        body: Some(body),
        attributes: kvs(prefix),
        ..Default::default()
    };
    // A kvlist whose key and whose array item hold `m`.
    let nested = |prefix: &str| AnyValue {
        value: Some(any_value::Value::KvlistValue(KeyValueList {
            values: vec![KeyValue {
                key: format!("{prefix}.key{m}"),
                value: Some(AnyValue {
                    value: Some(any_value::Value::ArrayValue(ArrayValue {
                        values: vec![s(&format!("{prefix}.item"))],
                    })),
                }),
            }],
        })),
    };
    // The nested value also travels as an attribute: pdata CBOR-encodes it into the `ser` column,
    // and the exporter decodes and renders it into the `attrs` map.
    let mut string_record = record(1, s("body"), "string_body");
    string_record.attributes.push(KeyValue {
        key: "nested_attr".into(),
        value: Some(nested("nested.attr")),
    });
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: kvs("resource"),
                entity_refs: vec![EntityRef {
                    schema_url: format!("entity.schema{m}"),
                    r#type: format!("entity.type{m}"),
                    id_keys: vec![format!("entity.id{m}")],
                    description_keys: vec![format!("entity.description{m}")],
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: format!("scope.name{m}"),
                    version: format!("scope.version{m}"),
                    attributes: kvs("scope"),
                    ..Default::default()
                }),
                log_records: vec![
                    string_record,
                    record(2, nested("nested"), "kvlist_body"),
                    LogRecord {
                        time_unix_nano: BASE_NS + 3,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("clean".into())),
                        }),
                        ..Default::default()
                    },
                ],
                schema_url: format!("scope.schema{m}"),
            }],
            schema_url: format!("resource.schema{m}"),
        }],
    }
}

/// [`metrics_request`] (all five metric kinds) with `m` added once to every string field: schema
/// URLs, scope name and version, resource and scope attributes, each metric's name (suffix),
/// description, unit and metadata, every data point's attributes, and one exemplar per gauge, sum,
/// histogram and exponential histogram point with a filtered attribute.
#[must_use]
pub fn every_string_metrics(m: &str) -> ExportMetricsServiceRequest {
    let mut req = metrics_request();
    let tag = |attrs: &mut Vec<KeyValue>, prefix: &str| {
        attrs.push(kv(
            &format!("{prefix}.key{m}"),
            format!("{prefix}.value{m}"),
        ));
    };
    let exemplar = |i: usize| Exemplar {
        time_unix_nano: BASE_NS + 1,
        filtered_attributes: vec![kv(
            &format!("metric{i}.exemplar.key{m}"),
            format!("metric{i}.exemplar.value{m}"),
        )],
        value: Some(exemplar::Value::AsInt(1)),
        ..Default::default()
    };
    for rm in &mut req.resource_metrics {
        rm.schema_url = format!("resource.schema{m}");
        tag(
            &mut rm.resource.get_or_insert_with(Resource::default).attributes,
            "resource",
        );
        for sm in &mut rm.scope_metrics {
            sm.schema_url = format!("scope.schema{m}");
            let scope = sm.scope.get_or_insert_with(InstrumentationScope::default);
            scope.name = format!("scope.name{m}");
            scope.version = format!("scope.version{m}");
            tag(&mut scope.attributes, "scope");
            for (i, metric) in sm.metrics.iter_mut().enumerate() {
                metric.name = format!("{}{m}", metric.name);
                metric.description = format!("metric{i}.description{m}");
                metric.unit = format!("metric{i}.unit{m}");
                tag(&mut metric.metadata, &format!("metric{i}.metadata"));
                let point = format!("metric{i}.point");
                match metric.data.as_mut() {
                    Some(
                        metric::Data::Gauge(Gauge { data_points })
                        | metric::Data::Sum(Sum { data_points, .. }),
                    ) => {
                        for p in data_points {
                            tag(&mut p.attributes, &point);
                            p.exemplars.push(exemplar(i));
                        }
                    }
                    Some(metric::Data::Histogram(h)) => {
                        for p in &mut h.data_points {
                            tag(&mut p.attributes, &point);
                            p.exemplars.push(exemplar(i));
                        }
                    }
                    Some(metric::Data::ExponentialHistogram(h)) => {
                        for p in &mut h.data_points {
                            tag(&mut p.attributes, &point);
                            p.exemplars.push(exemplar(i));
                        }
                    }
                    Some(metric::Data::Summary(s)) => {
                        for p in &mut s.data_points {
                            tag(&mut p.attributes, &point);
                        }
                    }
                    None => {}
                }
            }
        }
    }
    req
}

/// One length-delimited protobuf field `number` holding `body`.
#[must_use]
pub fn length_delimited(number: u32, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 8);
    encode_key(number, WireType::LengthDelimited, &mut out);
    encode_varint(body.len() as u64, &mut out);
    out.extend_from_slice(body);
    out
}

/// Encoded logs request whose only record's body nests `levels` arrays around a string of three
/// invalid bytes, built without recursion: AnyValue.string_value (1), AnyValue.array_value (5) >
/// ArrayValue.values (1), then LogRecord.body (5) > ScopeLogs.log_records (2) >
/// ResourceLogs.scope_logs (2) > ExportLogsServiceRequest.resource_logs (1).
#[must_use]
pub fn nested_body_logs_request(levels: usize) -> Vec<u8> {
    let mut any = length_delimited(1, &[0xff, 0xfe, 0xfd]);
    for _ in 0..levels {
        any = length_delimited(5, &length_delimited(1, &any));
    }
    length_delimited(
        1,
        &length_delimited(2, &length_delimited(2, &length_delimited(5, &any))),
    )
}
