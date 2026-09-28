// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The OTLP byte views read a singular field that occurs more than once as
//! prost decodes it: protobuf merges the concatenation of two encodings of a
//! message, so the last scalar wins and message fields merge.

use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::LogRecord;
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::{OtapArrowRecords, OtapPayload, OtlpProtoBytes, TryIntoWithOptions};
use prost::Message;

/// A length-delimited field `field` holding `payload`.
fn len_field(field: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![(field << 3) | 2];
    prost::encoding::encode_varint(payload.len() as u64, &mut out);
    out.extend_from_slice(payload);
    out
}

fn string(value: &str) -> Option<AnyValue> {
    Some(AnyValue {
        value: Some(any_value::Value::StringValue(value.to_owned())),
    })
}

fn attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: string(value),
    }
}

/// The OTAP records the byte views convert a logs `body` into, as text.
fn convert(body: Vec<u8>) -> String {
    let payload = OtapPayload::from(OtlpProtoBytes::new_from_bytes(SignalType::Logs, body));
    let records: OtapArrowRecords = payload.try_into_with_default().expect("converts");
    format!("{records:?}")
}

/// Scenario: a logs request whose resource, scope and log record are each
/// written as two encodings back to back, the first one with other values
/// for the scalars the second sets (severity, time, scope name, body) and
/// its own attributes.
/// Guarantees: the byte views convert it to exactly the OTAP records of
/// prost's decoding of it re-encoded: the second scalar wins and the
/// attributes of both encodings are kept, in order.
#[test]
fn singular_fields_written_twice_read_as_prost_decodes_them() {
    let first_record = LogRecord {
        time_unix_nano: 1,
        severity_number: 5,
        severity_text: "DEBUG".to_owned(),
        body: string("first"),
        attributes: vec![attribute("a", "1")],
        ..Default::default()
    };
    let second_record = LogRecord {
        time_unix_nano: 2,
        severity_number: 9,
        severity_text: "INFO".to_owned(),
        body: string("second"),
        attributes: vec![attribute("b", "2")],
        ..Default::default()
    };
    let first_scope = InstrumentationScope {
        name: "first".to_owned(),
        attributes: vec![attribute("s", "1")],
        ..Default::default()
    };
    let second_scope = InstrumentationScope {
        name: "second".to_owned(),
        version: "1.0".to_owned(),
        ..Default::default()
    };
    let first_resource = Resource {
        attributes: vec![attribute("host", "a")],
        ..Default::default()
    };
    let second_resource = Resource {
        attributes: vec![attribute("service", "b")],
        ..Default::default()
    };

    let record = [first_record.encode_to_vec(), second_record.encode_to_vec()].concat();
    let scope_logs = [
        len_field(1, &first_scope.encode_to_vec()),
        len_field(1, &second_scope.encode_to_vec()),
        len_field(2, &record),
    ]
    .concat();
    let resource_logs = [
        len_field(1, &first_resource.encode_to_vec()),
        len_field(1, &second_resource.encode_to_vec()),
        len_field(2, &scope_logs),
    ]
    .concat();
    let body = len_field(1, &resource_logs);

    let canonical = ExportLogsServiceRequest::decode(body.as_slice())
        .expect("prost decodes it")
        .encode_to_vec();
    assert_ne!(canonical, body, "prost merged the repeats");
    assert_eq!(convert(body), convert(canonical));
}
