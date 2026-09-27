// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! `google.rpc.RetryInfo` on gRPC refusals: the retry delay OTLP clients read
//! from the `grpc-status-details-bin` trailer.

use crate::otlp_http::RpcStatus;
use prost::Message;
use std::time::Duration;
use tonic::{Code, Status, metadata::MetadataMap};

/// The `type.googleapis.com` URL of `google.rpc.RetryInfo`.
pub const RETRY_INFO_TYPE_URL: &str = "type.googleapis.com/google.rpc.RetryInfo";

/// `google.rpc.RetryInfo`, the server's advisory delay before a retry.
///
/// See: <https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto>
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RetryInfo {
    /// Delay the client should wait before retrying.
    #[prost(message, optional, tag = "1")]
    pub retry_delay: Option<prost_types::Duration>,
}

/// Builds a status that carries `retry_delay` both as a `RetryInfo` detail
/// and as `grpc-retry-pushback-ms` metadata.
#[must_use]
pub fn status_with_retry_delay(code: Code, message: &str, retry_delay: Duration) -> Status {
    let retry_info = RetryInfo {
        retry_delay: Some(prost_types::Duration {
            seconds: i64::try_from(retry_delay.as_secs()).unwrap_or(i64::MAX),
            nanos: i32::try_from(retry_delay.subsec_nanos()).unwrap_or(0),
        }),
    };
    let details = RpcStatus {
        code: code as i32,
        message: message.to_owned(),
        details: vec![prost_types::Any {
            type_url: RETRY_INFO_TYPE_URL.to_owned(),
            value: retry_info.encode_to_vec(),
        }],
    };
    let mut metadata = MetadataMap::new();
    if let Ok(value) = retry_delay.as_millis().to_string().parse() {
        let _ = metadata.insert("grpc-retry-pushback-ms", value);
    }
    Status::with_details_and_metadata(
        code,
        message.to_owned(),
        details.encode_to_vec().into(),
        metadata,
    )
}

/// Returns the `RetryInfo` delay carried in `status`'s details, if any.
#[must_use]
pub fn retry_delay(status: &Status) -> Option<prost_types::Duration> {
    let details = status.details();
    if details.is_empty() {
        return None;
    }
    RpcStatus::decode(details)
        .ok()?
        .details
        .iter()
        .find(|any| any.type_url == RETRY_INFO_TYPE_URL)
        .and_then(|any| RetryInfo::decode(any.value.as_slice()).ok())
        .and_then(|info| info.retry_delay)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: a status is built with a 2.5 s retry delay.
    /// Guarantees: its details decode to a RetryInfo of 2 s 500,000,000 ns and
    /// its `grpc-retry-pushback-ms` is 2500.
    #[test]
    fn retry_delay_round_trips_through_details_and_metadata() {
        let status =
            status_with_retry_delay(Code::Unavailable, "busy", Duration::from_millis(2_500));

        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(
            retry_delay(&status),
            Some(prost_types::Duration {
                seconds: 2,
                nanos: 500_000_000
            })
        );
        assert_eq!(
            status
                .metadata()
                .get("grpc-retry-pushback-ms")
                .and_then(|value| value.to_str().ok()),
            Some("2500")
        );
    }

    /// Scenario: a status without details, and one whose details are not a google.rpc.Status.
    /// Guarantees: neither reports a retry delay.
    #[test]
    fn retry_delay_is_none_without_retry_info() {
        assert_eq!(retry_delay(&Status::unavailable("busy")), None);
        let garbage = Status::with_details(Code::Unavailable, "busy", vec![0xff, 0xff].into());
        assert_eq!(retry_delay(&garbage), None);
    }
}
