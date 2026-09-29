// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Heap measurement of the OTLP to OTAP conversion of a body that repeats
//! `KeyValue.value` at every nesting level, under dhat's allocator: its own
//! binary, because the allocator is process-wide.

use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_pdata::{OtapArrowRecords, OtapPayload, OtlpProtoBytes, TryIntoWithOptions};

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

/// dhat's counters are process-wide, so the tests of this binary measure one
/// at a time.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// A length-delimited field.
fn len_field(field: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![(field << 3) | 2];
    let mut len = payload.len();
    while len >= 0x80 {
        out.push((len as u8) | 0x80);
        len >>= 7;
    }
    out.push(len as u8);
    out.extend_from_slice(payload);
    out
}

/// A logs request whose one log attribute is a chain of `depth` key-value
/// lists around a string of `leaf` bytes. With `repeat`, every `KeyValue`
/// of the chain carries an empty `value` before its real one, which prost
/// merges into the real one.
fn chain(depth: usize, leaf: usize, repeat: bool) -> Vec<u8> {
    let empty_value: &[u8] = if repeat { &[0x12, 0x00] } else { &[] };
    let mut key_value = [
        len_field(1, b"k"),
        len_field(2, &len_field(1, &vec![b's'; leaf])),
    ]
    .concat();
    for _ in 0..depth {
        let list = len_field(6, &len_field(1, &key_value));
        key_value = [
            len_field(1, b"k"),
            empty_value.to_vec(),
            len_field(2, &list),
        ]
        .concat();
    }
    let record = len_field(6, &key_value);
    len_field(1, &len_field(2, &len_field(2, &record)))
}

/// The largest heap the conversion of `body` holds at once.
fn conversion_peak(body: Vec<u8>) -> usize {
    let payload = OtapPayload::from(OtlpProtoBytes::new_from_bytes(SignalType::Logs, body));
    let profiler = dhat::Profiler::builder().testing().build();
    let records: OtapArrowRecords = payload.try_into_with_default().expect("converts");
    let peak = dhat::HeapStats::get().max_bytes;
    drop(records);
    drop(profiler);
    peak
}

/// Scenario: a logs body of 1, 4 and 16 MiB (the receiver's default and the
/// reference configs' request limits) whose attribute nests 255 key-value
/// lists, each `KeyValue` repeating `value`, converted to OTAP records.
/// Guarantees: the conversion holds no more heap than for the same body
/// without the repeats, plus a quarter, so the merge of repeated values
/// copies nothing per nesting level.
#[test]
fn a_repeated_value_at_every_level_costs_no_copy() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for mib in [1, 4, 16] {
        let leaf = (mib << 20) - 4096;
        let plain = conversion_peak(chain(255, leaf, false));
        let repeated = conversion_peak(chain(255, leaf, true));
        println!("{mib} MiB: peak {repeated} with repeats, {plain} without");
        assert!(
            repeated <= plain + plain / 4,
            "{mib} MiB: peak {repeated} with repeated values, {plain} without"
        );
    }
}
