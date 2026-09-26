// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Every canonical golden vector, checked through OTLP -> OTAP -> extract.

use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, LogsData, ResourceLogs, ScopeLogs,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
    HistogramDataPoint, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
    Summary, SummaryDataPoint, metric, number_data_point,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::testing::round_trip::{encode_logs, encode_metrics};
use otel_arrow_dfe_series_lake::canonical::hex;
use otel_arrow_dfe_series_lake::config::{LakeConfig, UnsupportedPolicy};
use otel_arrow_dfe_series_lake::extract::extract;

/// Kinds with no values dataset in format v1.
///
/// Exponential histograms and summaries have no `number` or `histogram` schema to
/// land in, so by design `extract` never emits a descriptor for them: under the
/// `Drop` policy their points are counted and discarded, and under `Reject` the
/// whole request is refused. There is therefore no `series_id` to compare against
/// the golden vector, and `unsupported_kinds_are_dropped_or_rejected` pins that
/// behavior down instead. Every other vector round-trips with no exception.
const UNSUPPORTED_KINDS: [&str; 2] = ["exp_histogram", "summary"];

/// The vector names the round trip is allowed to skip, and only these.
const EXPECTED_SKIPS: [&str; 2] = ["metrics_exp_histogram_delta", "metrics_summary"];

fn hex_decode(s: &str) -> Vec<u8> {
    ::hex::decode(s).expect("hex")
}

fn any_value_from_json(j: &serde_json::Value) -> AnyValue {
    let v = match j["type"].as_str().expect("type") {
        "null" => None,
        "str" => Some(any_value::Value::StringValue(
            j["value"].as_str().expect("str").to_string(),
        )),
        "bytes" => Some(any_value::Value::BytesValue(hex_decode(
            j["value"].as_str().expect("hex"),
        ))),
        "int" => Some(any_value::Value::IntValue(
            j["value"].as_i64().expect("int"),
        )),
        "double" => Some(any_value::Value::DoubleValue(match j.get("bits") {
            Some(bits) => f64::from_bits(bits.as_u64().expect("bits")),
            None => j["value"].as_f64().expect("double"),
        })),
        "bool" => Some(any_value::Value::BoolValue(
            j["value"].as_bool().expect("bool"),
        )),
        "array" => Some(any_value::Value::ArrayValue(ArrayValue {
            values: j["items"]
                .as_array()
                .expect("items")
                .iter()
                .map(any_value_from_json)
                .collect(),
        })),
        "kvlist" => Some(any_value::Value::KvlistValue(KeyValueList {
            values: kvs_from_json(&j["entries"]),
        })),
        other => panic!("unknown type {other}"),
    };
    AnyValue { value: v }
}

fn kvs_from_json(j: &serde_json::Value) -> Vec<KeyValue> {
    j.as_array()
        .map(|a| {
            a.iter()
                .map(|e| KeyValue {
                    key: e["key"].as_str().expect("key").to_string(),
                    value: Some(any_value_from_json(&e["value"])),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn str_field(j: &serde_json::Value, k: &str) -> String {
    j.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string()
}

fn keys_of(j: &serde_json::Value, field: &str) -> Vec<String> {
    j.get(field)
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .map(|e| e["key"].as_str().expect("key").to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn logs_for(d: &serde_json::Value) -> LogsData {
    LogsData {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: kvs_from_json(&d["resource_attrs"]),
                ..Default::default()
            }),
            schema_url: str_field(d, "resource_schema_url"),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: str_field(d, "scope_name"),
                    version: str_field(d, "scope_version"),
                    attributes: kvs_from_json(&d["scope_attrs"]),
                    ..Default::default()
                }),
                schema_url: str_field(d, "scope_schema_url"),
                log_records: vec![LogRecord {
                    time_unix_nano: 1_000,
                    // A vector of the allow-list rule carries the record's
                    // whole attribute list beside the identity part of it.
                    attributes: kvs_from_json(d.get("record_attrs").unwrap_or(&d["attrs"])),
                    ..Default::default()
                }],
            }],
        }],
    }
}

fn metrics_for(d: &serde_json::Value) -> MetricsData {
    let m = &d["metric"];
    let name = str_field(m, "name");
    let unit = str_field(m, "unit");
    let point = NumberDataPoint {
        time_unix_nano: 1_000,
        attributes: kvs_from_json(&d["attrs"]),
        value: Some(number_data_point::Value::AsInt(1)),
        ..Default::default()
    };
    let temporality = match str_field(m, "temporality").as_str() {
        "delta" => AggregationTemporality::Delta as i32,
        "cumulative" => AggregationTemporality::Cumulative as i32,
        _ => AggregationTemporality::Unspecified as i32,
    };
    let data = match m["kind"].as_str().expect("kind") {
        "gauge" => metric::Data::Gauge(Gauge {
            data_points: vec![point],
        }),
        "sum" => metric::Data::Sum(Sum {
            aggregation_temporality: temporality,
            is_monotonic: m
                .get("is_monotonic")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            data_points: vec![point],
        }),
        "histogram" => metric::Data::Histogram(Histogram {
            aggregation_temporality: temporality,
            data_points: vec![HistogramDataPoint {
                time_unix_nano: 1_000,
                attributes: kvs_from_json(&d["attrs"]),
                count: 1,
                sum: Some(1.0),
                bucket_counts: vec![1],
                explicit_bounds: vec![],
                ..Default::default()
            }],
        }),
        // The two kinds format v1 cannot store are still built, so
        // `unsupported_kinds_are_dropped_or_rejected` can run them through the
        // real extraction path.
        "exp_histogram" => metric::Data::ExponentialHistogram(ExponentialHistogram {
            aggregation_temporality: temporality,
            data_points: vec![ExponentialHistogramDataPoint {
                time_unix_nano: 1_000,
                attributes: kvs_from_json(&d["attrs"]),
                count: 1,
                sum: Some(1.0),
                scale: 0,
                zero_count: 1,
                ..Default::default()
            }],
        }),
        "summary" => metric::Data::Summary(Summary {
            data_points: vec![SummaryDataPoint {
                time_unix_nano: 1_000,
                attributes: kvs_from_json(&d["attrs"]),
                count: 1,
                sum: 1.0,
                ..Default::default()
            }],
        }),
        other => panic!("golden vector uses an unknown metric kind {other}"),
    };
    MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: kvs_from_json(&d["resource_attrs"]),
                ..Default::default()
            }),
            schema_url: str_field(d, "resource_schema_url"),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: str_field(d, "scope_name"),
                    version: str_field(d, "scope_version"),
                    attributes: kvs_from_json(&d["scope_attrs"]),
                    ..Default::default()
                }),
                schema_url: str_field(d, "scope_schema_url"),
                metrics: vec![Metric {
                    name,
                    unit,
                    data: Some(data),
                    ..Default::default()
                }],
            }],
        }],
    }
}

/// Run every golden vector through OTLP -> OTAP -> extract and name the ones
/// whose `series_id` differs from the Python generator's.
fn deviating_vectors() -> Vec<String> {
    let raw = include_str!("golden/canonical_v1.json");
    let doc: serde_json::Value = serde_json::from_str(raw).expect("json");
    let vectors = doc["vectors"].as_array().expect("vectors");
    assert!(vectors.len() >= 30);
    let mut checked = 0usize;
    let mut skipped: Vec<&str> = Vec::new();
    let mut deviations: Vec<String> = Vec::new();
    for v in vectors {
        let name = v["name"].as_str().expect("name");
        let d = &v["descriptor"];
        let expected = v["series_id_hex"].as_str().expect("id");
        let kind = d.get("metric").map(|m| m["kind"].as_str().expect("kind"));
        if kind.is_some_and(|k| UNSUPPORTED_KINDS.contains(&k)) {
            skipped.push(name);
            continue;
        }
        let mut cfg = LakeConfig::default();
        // The vector's own allow-list, or else every identity attribute of
        // the vector.
        cfg.logs.series_attributes = match d.get("series_attributes") {
            Some(keys) => keys
                .as_array()
                .expect("series_attributes")
                .iter()
                .map(|k| k.as_str().expect("key").to_string())
                .collect(),
            None => keys_of(d, "attrs"),
        };
        // The vectors never project a producer id.
        cfg.producer_id_attribute = "__absent__".into();
        let got = if d["signal"] == "logs" {
            let mut records = encode_logs(&logs_for(d));
            extract(&mut records, &cfg).expect("extract logs")
        } else {
            let mut records = encode_metrics(&metrics_for(d));
            extract(&mut records, &cfg).expect("extract metrics")
        };
        assert_eq!(got.descriptors.len(), 1, "one series for {name}");
        if hex(&got.descriptors[0].series_id) != expected {
            deviations.push(name.to_string());
        }
        checked += 1;
    }
    // Pin the skips exactly: a new vector of an unsupported kind must be noticed,
    // not silently ignored, and a vector must never drop out of the comparison for
    // any other reason.
    assert_eq!(
        skipped,
        EXPECTED_SKIPS.to_vec(),
        "only exponential histograms and summaries may be skipped, because format v1 has no \
         values dataset for them so extraction emits no descriptor to compare; see \
         unsupported_kinds_are_dropped_or_rejected. Every other vector must round-trip."
    );
    assert_eq!(checked, vectors.len() - skipped.len());
    deviations
}

/// Scenario: every golden vector rebuilt as OTLP, converted by pdata and extracted.
/// Guarantees: each yields the `series_id` the Python generator recorded.
#[test]
fn all_golden_vectors_survive_otlp_to_otap_conversion() {
    assert_eq!(
        deviating_vectors(),
        Vec::<String>::new(),
        "every golden vector must survive the OTLP to OTAP conversion"
    );
}

/// The two golden vectors whose metric kind has no values dataset, rebuilt as OTLP.
fn unsupported_vector_requests() -> Vec<(String, MetricsData)> {
    let raw = include_str!("golden/canonical_v1.json");
    let doc: serde_json::Value = serde_json::from_str(raw).expect("json");
    doc["vectors"]
        .as_array()
        .expect("vectors")
        .iter()
        .filter(|v| EXPECTED_SKIPS.contains(&v["name"].as_str().expect("name")))
        .map(|v| {
            (
                v["name"].as_str().expect("name").to_string(),
                metrics_for(&v["descriptor"]),
            )
        })
        .collect()
}

/// Scenario: the exponential histogram and summary vectors under both unsupported policies.
/// Guarantees: `Drop` succeeds with no descriptor and counts the points; `Reject` refuses.
#[test]
fn unsupported_kinds_are_dropped_or_rejected() {
    let requests = unsupported_vector_requests();
    assert_eq!(
        requests.len(),
        2,
        "both unsupported vectors must be present"
    );

    for (name, data) in requests {
        let drop_cfg = LakeConfig {
            unsupported: UnsupportedPolicy::Drop,
            producer_id_attribute: "__absent__".into(),
            ..Default::default()
        };
        let mut records = encode_metrics(&data);
        let out = extract(&mut records, &drop_cfg).expect("drop policy must not refuse");
        assert!(
            out.descriptors.is_empty(),
            "{name} must produce no descriptor under Drop"
        );
        assert!(
            out.stats.dropped_unsupported > 0,
            "{name} must count its discarded points under Drop"
        );
        assert_eq!(
            out.values.iter().map(|(_, b)| b.len()).sum::<usize>(),
            0,
            "{name} must produce no values rows under Drop"
        );

        let reject_cfg = LakeConfig {
            unsupported: UnsupportedPolicy::Reject,
            producer_id_attribute: "__absent__".into(),
            ..Default::default()
        };
        let mut records = encode_metrics(&data);
        assert!(
            extract(&mut records, &reject_cfg).is_err(),
            "{name} must be refused under Reject"
        );
    }
}
