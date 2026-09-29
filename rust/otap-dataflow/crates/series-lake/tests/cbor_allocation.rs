// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Heap measurement of `decode_cbor_reserving` under dhat's allocator: its own
//! binary, because the allocator is process-wide.

use otel_arrow_dfe_series_lake::value::{
    DecodeLimits, Reservations, VALUE_NODE_BYTES, Value, body_string_reserving,
    decode_cbor_reserving, value_bytes,
};
use otel_arrow_dfe_series_lake::{Error, Result, SizeBudget};

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

/// dhat's counters are process-wide, so the tests of this binary measure one
/// at a time.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Reservations up to `limit`, recording the largest total held.
struct Held {
    limit: usize,
    held: usize,
    peak: usize,
}

impl Reservations for Held {
    fn reserve(&mut self, bytes: usize) -> Result<()> {
        let next = self.held + bytes;
        if next > self.limit {
            return Err(Error::too_large(SizeBudget::Table, next, self.limit));
        }
        self.held = next;
        self.peak = self.peak.max(next);
        Ok(())
    }

    fn release(&mut self, bytes: usize) {
        self.held -= bytes;
    }
}

/// Headroom for the refusal's own error message, which is allocated after
/// the decode has stopped reserving.
const ERROR_SLACK: usize = 1024;

/// Scenario: flat, nested, indefinite and chunked payloads decoded under dhat, unbounded and
/// against limits that refuse them part way.
/// Guarantees: the heap peak never exceeds the reservation peak, and a successful decode holds its
/// decoded size less the root node.
#[test]
fn a_decode_never_allocates_more_than_it_reserved() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let ints = [&[0x1a, 0x00, 0x01, 0x86, 0xa0][..], &[0x01; 100_000]].concat();
    let indefinite_ints = [&[0x9f][..], &[0x01; 100_000], &[0xff]].concat();
    let mut chunked_bytes = vec![0x5f];
    for i in 0..100_000_u32 {
        chunked_bytes.extend_from_slice(&[0x41, i.to_le_bytes()[0]]);
    }
    chunked_bytes.push(0xff);
    let mut map = vec![0xbf];
    for i in 0..20_000_u32 {
        map.push(0x64);
        map.extend_from_slice(format!("{:04}", i % 10_000).as_bytes());
        map.push(0x01);
    }
    map.push(0xff);
    let mut text = vec![0x7f];
    for _ in 0..50_000 {
        text.extend_from_slice(&[0x62, b'h', b'i']);
    }
    text.push(0xff);
    let singletons = [&[0x19, 0xc3, 0x50][..], &[0x81, 0x00].repeat(50_000)].concat();

    for (name, payload) in [
        ("definite ints", &ints),
        ("indefinite ints", &indefinite_ints),
        ("chunked bytes", &chunked_bytes),
        ("indefinite map with repeated keys", &map),
        ("chunked text", &text),
        ("singleton arrays", &singletons),
    ] {
        for limit in [usize::MAX, 1 << 20, 64 << 10] {
            let mut held = Held {
                limit,
                held: 0,
                peak: 0,
            };
            let profiler = dhat::Profiler::builder().testing().build();
            let result =
                decode_cbor_reserving(payload, DecodeLimits::new(32, usize::MAX), &mut held);
            let allocated = dhat::HeapStats::get().max_bytes;
            drop(profiler);
            let slack = if result.is_err() { ERROR_SLACK } else { 0 };
            assert!(
                allocated <= held.peak + slack,
                "{name} at limit {limit}: allocated {allocated}, reserved {}",
                held.peak
            );
            if let Ok(value) = &result {
                assert_eq!(held.held + VALUE_NODE_BYTES, value_bytes(value), "{name}");
            }
        }
    }
}

/// Scenario: a large bytes body, a long string and a nested escaped body rendered under dhat,
/// unbounded and against a smaller limit.
/// Guarantees: rendering never allocates more than it reserved, and an oversized one is refused
/// before allocating.
#[test]
fn a_body_rendering_never_allocates_more_than_it_reserved() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let nested = Value::KvList(vec![
        ("b".into(), Value::Bytes(vec![7; 10_000])),
        ("d".into(), Value::Array(vec![Value::Double(0.1); 1000])),
        ("q".into(), Value::Str("\"\n\u{2}x".repeat(2000))),
    ]);
    for (name, body) in [
        ("bytes", Value::Bytes(vec![0xAB; 300_000])),
        ("string", Value::Str("s".repeat(300_000))),
        ("nested", nested),
    ] {
        for limit in [usize::MAX, 4096] {
            let mut held = Held {
                limit,
                held: 0,
                peak: 0,
            };
            let profiler = dhat::Profiler::builder().testing().build();
            let rendered = body_string_reserving(&body, &mut held);
            let allocated = dhat::HeapStats::get().max_bytes;
            drop(profiler);
            let slack = if rendered.is_err() { ERROR_SLACK } else { 0 };
            assert!(
                allocated <= held.peak + slack,
                "{name} at limit {limit}: allocated {allocated}, reserved {}",
                held.peak
            );
        }
    }
}

/// Scenario: a record with 47 attributes each holding a 600 000 control-character string (3.6 MB
/// each rendered), extracted under dhat at the default limits.
/// Guarantees: the request is refused as too large a row after rendering one value, and the heap
/// stays within the budget plus a constant.
#[test]
fn a_residual_map_is_refused_before_it_is_rendered_past_the_budget() {
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
        AnyValue, ArrayValue, KeyValue, any_value,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, LogsData, ResourceLogs, ScopeLogs,
    };
    use otel_arrow_dfe_series_lake::config::LakeConfig;
    use otel_arrow_dfe_series_lake::error::{Excess, RefuseReason};

    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let value = AnyValue {
        value: Some(any_value::Value::ArrayValue(ArrayValue {
            values: vec![AnyValue {
                value: Some(any_value::Value::StringValue("\u{1}".repeat(600_000))),
            }],
        })),
    };
    let data = LogsData {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_000,
                    attributes: (0..47)
                        .map(|i| KeyValue {
                            key: format!("a{i:02}"),
                            value: Some(value.clone()),
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let cfg = LakeConfig::default();
    let mut records = otel_arrow_dfe_pdata::testing::round_trip::encode_logs(&data);
    drop(data);
    drop(value);

    let profiler = dhat::Profiler::builder().testing().build();
    let result = otel_arrow_dfe_series_lake::extract::extract(&mut records, &cfg);
    let allocated = dhat::HeapStats::get().max_bytes;
    drop(profiler);
    assert!(
        matches!(
            result,
            Err(Error::Refused(RefuseReason::RequestTooLarge(Excess {
                budget: SizeBudget::Row,
                ..
            })))
        ),
        "{result:?}"
    );
    assert!(
        allocated <= cfg.ingress.max_extracted_bytes + (2 << 20),
        "allocated {allocated}"
    );
}
