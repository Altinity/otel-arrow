// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Fuzz-style property tests for extraction (FORMAT.md sections 1 and 2).
//!
//! The generators cover every `AnyValue` variant, including the unset value,
//! empty strings and byte strings, both zeros, subnormals, the integer extremes
//! and bounded nested arrays and key/value lists, because those are exactly the
//! shapes where OTAP's column encoding and the canonical encoding can disagree.

use std::collections::BTreeSet;

use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    AnyValue, ArrayValue, KeyValue, KeyValueList, any_value,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, LogsData, ResourceLogs, ScopeLogs,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, MetricsData,
    NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::testing::round_trip::{encode_logs, encode_metrics};
use otel_arrow_dfe_series_lake::canonical::hex;
use otel_arrow_dfe_series_lake::config::LakeConfig;
use otel_arrow_dfe_series_lake::extract::extract;
use proptest::prelude::*;

/// One generated attribute: a key and any OTLP value.
#[derive(Debug, Clone)]
struct Attr {
    key: String,
    value: AnyValue,
}

/// Keys are short and drawn from a small alphabet so that collisions, and
/// therefore the deduplication path, happen often.
fn attr_key() -> impl Strategy<Value = String> {
    "[a-z.]{1,4}"
}

/// Scalar values, weighted so that the encoding boundaries are common: the
/// empty string, the empty byte string, both zeros, subnormals and the integer
/// extremes.
fn leaf_value() -> impl Strategy<Value = AnyValue> {
    prop_oneof![
        // The unset value: distinct from every default, and must stay distinct.
        1 => Just(AnyValue { value: None }),
        4 => "[a-zA-Z0-9]{0,6}".prop_map(|s| any_str(&s)),
        1 => Just(any_str("")),
        3 => prop::collection::vec(any::<u8>(), 0..6)
            .prop_map(|b| wrap(any_value::Value::BytesValue(b))),
        1 => Just(wrap(any_value::Value::BytesValue(Vec::new()))),
        3 => (-1000i64..1000).prop_map(|i| wrap(any_value::Value::IntValue(i))),
        1 => prop_oneof![Just(0i64), Just(i64::MIN), Just(i64::MAX)]
            .prop_map(|i| wrap(any_value::Value::IntValue(i))),
        3 => (-1000.0f64..1000.0).prop_map(|d| wrap(any_value::Value::DoubleValue(d))),
        1 => prop_oneof![
                Just(0.0f64),
                Just(-0.0f64),
                Just(f64::from_bits(1)),
                Just(f64::MIN_POSITIVE),
                Just(f64::MAX),
                Just(f64::MIN),
                Just(f64::NAN),
                Just(f64::INFINITY),
                Just(f64::NEG_INFINITY),
            ]
            .prop_map(|d| wrap(any_value::Value::DoubleValue(d))),
        2 => any::<bool>().prop_map(|b| wrap(any_value::Value::BoolValue(b))),
    ]
}

fn wrap(v: any_value::Value) -> AnyValue {
    AnyValue { value: Some(v) }
}

fn any_str(s: &str) -> AnyValue {
    wrap(any_value::Value::StringValue(s.to_string()))
}

/// Values including bounded nested arrays and key/value lists.
///
/// Nested keys are deduplicated: a kvlist with a repeated key is invalid content
/// that the CBOR decoder refuses by design, which is covered elsewhere.
fn any_value_strategy() -> impl Strategy<Value = AnyValue> {
    leaf_value().prop_recursive(3, 24, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..3)
                .prop_map(|values| wrap(any_value::Value::ArrayValue(ArrayValue { values }))),
            prop::collection::vec((attr_key(), inner), 0..3).prop_map(|entries| {
                let mut values: Vec<KeyValue> = Vec::new();
                for (key, value) in entries {
                    if !values.iter().any(|k| k.key == key) {
                        values.push(KeyValue {
                            key,
                            value: Some(value),
                        });
                    }
                }
                wrap(any_value::Value::KvlistValue(KeyValueList { values }))
            }),
        ]
    })
}

fn attr() -> impl Strategy<Value = Attr> {
    (attr_key(), any_value_strategy()).prop_map(|(key, value)| Attr { key, value })
}

/// An attribute list with unique keys; duplicate keys are invalid OTLP, tested
/// elsewhere. The count reaches zero so that an attribute-free record is covered.
fn attr_list(max: usize) -> impl Strategy<Value = Vec<Attr>> {
    prop::collection::vec(attr(), 0..max).prop_map(|attrs| {
        let mut out: Vec<Attr> = Vec::new();
        for a in attrs {
            if !out.iter().any(|x| x.key == a.key) {
                out.push(a);
            }
        }
        out
    })
}

fn kv(a: &Attr) -> KeyValue {
    KeyValue {
        key: a.key.clone(),
        value: Some(a.value.clone()),
    }
}

fn kvs(attrs: &[Attr]) -> Vec<KeyValue> {
    attrs.iter().map(kv).collect()
}

/// Split records at generated boundaries.
fn split<T: Clone>(items: &[T], sizes: &[usize]) -> Vec<Vec<T>> {
    let mut out = Vec::new();
    let mut idx = 0;
    let mut which = 0;
    while idx < items.len() {
        let n = sizes[which % sizes.len()].max(1).min(items.len() - idx);
        out.push(items[idx..idx + n].to_vec());
        idx += n;
        which += 1;
    }
    out
}

// ------------------------------------------------------- request builders

fn logs_of(records: &[Vec<Attr>], resource: &[Attr]) -> LogsData {
    LogsData {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: kvs(resource),
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: records
                    .iter()
                    .enumerate()
                    .map(|(i, attrs)| LogRecord {
                        time_unix_nano: 1_000 + i as u64,
                        attributes: kvs(attrs),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn metrics_wrap(metrics: Vec<Metric>, resource: &[Attr]) -> MetricsData {
    MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: kvs(resource),
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn numbers_of(points: &[Vec<Attr>], resource: &[Attr]) -> MetricsData {
    metrics_wrap(
        vec![Metric {
            name: "m".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: points
                    .iter()
                    .enumerate()
                    .map(|(i, attrs)| NumberDataPoint {
                        time_unix_nano: 1_000 + i as u64,
                        attributes: kvs(attrs),
                        value: Some(number_data_point::Value::AsInt(i as i64)),
                        ..Default::default()
                    })
                    .collect(),
            })),
            ..Default::default()
        }],
        resource,
    )
}

fn histograms_of(points: &[Vec<Attr>], resource: &[Attr]) -> MetricsData {
    metrics_wrap(
        vec![Metric {
            name: "h".into(),
            data: Some(metric::Data::Histogram(Histogram {
                aggregation_temporality: AggregationTemporality::Delta as i32,
                data_points: points
                    .iter()
                    .enumerate()
                    .map(|(i, attrs)| HistogramDataPoint {
                        time_unix_nano: 1_000 + i as u64,
                        attributes: kvs(attrs),
                        count: i as u64,
                        sum: Some(i as f64),
                        bucket_counts: vec![i as u64, 0],
                        explicit_bounds: vec![1.0],
                        ..Default::default()
                    })
                    .collect(),
            })),
            ..Default::default()
        }],
        resource,
    )
}

// ------------------------------------------------------------ extraction

fn cfg_with(series_attributes: Vec<String>) -> LakeConfig {
    let mut cfg = LakeConfig::default();
    cfg.logs.series_attributes = series_attributes;
    cfg
}

fn logs_ids(data: &LogsData, cfg: &LakeConfig) -> BTreeSet<String> {
    let mut records = encode_logs(data);
    let out = extract(&mut records, cfg).expect("extract logs");
    out.descriptors.iter().map(|d| hex(&d.series_id)).collect()
}

fn metric_ids(data: &MetricsData, cfg: &LakeConfig) -> BTreeSet<String> {
    let mut records = encode_metrics(data);
    let out = extract(&mut records, cfg).expect("extract metrics");
    out.descriptors.iter().map(|d| hex(&d.series_id)).collect()
}

/// Every signal's identity set for one set of records, in one request.
fn all_ids(records: &[Vec<Attr>], resource: &[Attr], cfg: &LakeConfig) -> [BTreeSet<String>; 3] {
    [
        logs_ids(&logs_of(records, resource), cfg),
        metric_ids(&numbers_of(records, resource), cfg),
        metric_ids(&histograms_of(records, resource), cfg),
    ]
}

/// The same, but delivered as several requests split at `sizes`.
fn all_ids_split(
    records: &[Vec<Attr>],
    resource: &[Attr],
    sizes: &[usize],
    cfg: &LakeConfig,
) -> [BTreeSet<String>; 3] {
    let mut out = [BTreeSet::new(), BTreeSet::new(), BTreeSet::new()];
    for chunk in split(records, sizes) {
        let ids = all_ids(&chunk, resource, cfg);
        for (acc, got) in out.iter_mut().zip(ids) {
            acc.extend(got);
        }
    }
    out
}

fn keys_of(records: &[Vec<Attr>]) -> Vec<String> {
    records.iter().flatten().map(|a| a.key.clone()).collect()
}

/// Records paired with a proptest-generated permutation of each one's attributes,
/// and a permutation of the resource attributes.
type Permuted = (Vec<Attr>, Vec<Attr>, Vec<Vec<Attr>>, Vec<Vec<Attr>>);

fn records_with_permutation(min: usize, max: usize) -> impl Strategy<Value = Permuted> {
    (attr_list(4), prop::collection::vec(attr_list(4), min..max)).prop_flat_map(
        |(resource, records)| {
            let shuffled_resource = Just(resource.clone()).prop_shuffle();
            let shuffled_records: Vec<BoxedStrategy<Vec<Attr>>> = records
                .iter()
                .cloned()
                .map(|r| Just(r).prop_shuffle().boxed())
                .collect();
            (
                Just(resource),
                shuffled_resource,
                Just(records),
                shuffled_records,
            )
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, .. ProptestConfig::default() })]

    /// Scenario: random OTLP logs, gauge and histogram points covering every attribute value
    /// variant and bounded nesting.
    /// Guarantees: extraction never panics and never yields more identities than rows.
    #[test]
    fn extract_never_panics(
        resource in attr_list(5),
        records in prop::collection::vec(attr_list(5), 1..8),
    ) {
        let cfg = cfg_with(keys_of(&records));
        for ids in all_ids(&records, &resource, &cfg) {
            prop_assert!(ids.len() <= records.len());
        }
    }

    /// Scenario: the same records with attribute lists permuted, and delivered whole or split, for
    /// all three signals.
    /// Guarantees: the set of series ids is identical in every case.
    #[test]
    fn identity_is_independent_of_attribute_order_and_framing(
        (resource, shuffled_resource, records, shuffled) in records_with_permutation(2, 8),
        sizes in prop::collection::vec(1usize..4, 1..5),
    ) {
        let cfg = cfg_with(keys_of(&records));

        let whole = all_ids(&records, &resource, &cfg);
        let permuted = all_ids(&shuffled, &shuffled_resource, &cfg);
        let framed = all_ids_split(&records, &resource, &sizes, &cfg);

        for (i, name) in ["logs", "number", "histogram"].iter().enumerate() {
            prop_assert_eq!(
                &permuted[i],
                &whole[i],
                "{} identity must not depend on attribute order",
                name
            );
            prop_assert_eq!(
                &framed[i],
                &whole[i],
                "{} identity must not depend on request framing",
                name
            );
        }
    }
}

// ------------------------------------------------- deterministic regression
//
// pdata's OTAP encoder omits a value column whose every entry is that type's
// default, so a request of nothing but default values carries no value column
// at all while the type tag still names the type. Reading that as null would
// make a series identity depend on how requests are batched, and collide with a
// genuinely null attribute. The property tests above reach this case only by
// chance; these two pin it.

/// One log record carrying a single attribute `a` with the given value.
fn one_attr(value: AnyValue) -> Vec<Attr> {
    vec![Attr {
        key: "a".into(),
        value,
    }]
}

/// The identity set of a logs request whose only identity attribute is `a`.
fn ids_for(records: Vec<Vec<Attr>>) -> BTreeSet<String> {
    let cfg = cfg_with(vec!["a".to_string()]);
    logs_ids(&logs_of(&records, &[]), &cfg)
}

/// The identity of a single record carrying exactly one attribute value.
fn single_id(value: AnyValue) -> String {
    let ids = ids_for(vec![one_attr(value)]);
    assert_eq!(ids.len(), 1, "one record is one series");
    ids.into_iter().next().expect("one id")
}

/// Every type's default value, paired with a non-default value of the same type.
///
/// A request holding only the left-hand value has no value column at all; a
/// request holding both has one. The identity must not notice the difference.
fn default_and_other() -> Vec<(&'static str, AnyValue, AnyValue)> {
    vec![
        ("empty string", any_str(""), any_str("A")),
        (
            "int zero",
            wrap(any_value::Value::IntValue(0)),
            wrap(any_value::Value::IntValue(7)),
        ),
        (
            "double zero",
            wrap(any_value::Value::DoubleValue(0.0)),
            wrap(any_value::Value::DoubleValue(1.5)),
        ),
        (
            "double negative zero",
            wrap(any_value::Value::DoubleValue(-0.0)),
            wrap(any_value::Value::DoubleValue(1.5)),
        ),
        (
            "empty bytes",
            wrap(any_value::Value::BytesValue(Vec::new())),
            wrap(any_value::Value::BytesValue(vec![1])),
        ),
    ]
}

/// Scenario: a record whose identity attribute is its type's default, alone and beside a
/// non-default value, for each default.
/// Guarantees: the series id is the same either way.
#[test]
fn default_valued_attribute_identity_does_not_depend_on_request_framing() {
    for (name, default, other) in default_and_other() {
        let alone = single_id(default.clone());
        let together = ids_for(vec![one_attr(default), one_attr(other)]);
        assert!(
            together.contains(&alone),
            "{name}: the series id changed with request framing, alone {alone} not in {together:?}"
        );
    }
}

/// Scenario: the same defaults against an unset attribute and against no attribute.
/// Guarantees: a default value is a different series from a null one.
#[test]
fn default_valued_attribute_differs_from_an_unset_or_absent_one() {
    let unset = single_id(AnyValue { value: None });
    let absent = {
        let ids = ids_for(vec![vec![]]);
        assert_eq!(ids.len(), 1, "one record is one series");
        ids.into_iter().next().expect("one id")
    };
    assert_ne!(unset, absent, "an unset value is not an absent attribute");

    for (name, default, _) in default_and_other() {
        let id = single_id(default);
        assert_ne!(id, unset, "{name} must not collide with an unset value");
        assert_ne!(
            id, absent,
            "{name} must not collide with an absent attribute"
        );
    }
}

/// Scenario: the two signed zeros as an attribute value through the real extraction path.
/// Guarantees: they share one series id (FORMAT.md section 1).
#[test]
fn both_signed_zeros_share_one_identity() {
    assert_eq!(
        single_id(wrap(any_value::Value::DoubleValue(-0.0))),
        single_id(wrap(any_value::Value::DoubleValue(0.0))),
    );
}
