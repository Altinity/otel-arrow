// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! `google.rpc.RetryInfo` status details for refusals a client may retry.
//!
//! The OTLP specification makes `RESOURCE_EXHAUSTED` retryable only when the status carries a
//! `RetryInfo` detail; without it, clients such as the OpenTelemetry Collector treat the refusal
//! as permanent and drop the data. Every recoverable refusal of the receivers is built here so it
//! carries that detail.
//! See <https://opentelemetry.io/docs/specs/otlp/#failures>.

use std::time::Duration;

use bytes::Bytes;
use prost::Message as _;
use prost_types::Any;
use tonic::metadata::MetadataMap;
use tonic::{Code, Status};

use crate::otlp_http::RpcStatus;

/// The `type.googleapis.com` URL of `google.rpc.RetryInfo`.
const RETRY_INFO_TYPE_URL: &str = "type.googleapis.com/google.rpc.RetryInfo";

/// `google.rpc.RetryInfo`: how long the client should wait before it retries.
///
/// See: <https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto>
#[derive(Clone, PartialEq, ::prost::Message)]
struct RetryInfo {
    #[prost(message, optional, tag = "1")]
    retry_delay: Option<prost_types::Duration>,
}

/// The serialized `google.rpc.Status` for the `grpc-status-details-bin` trailer, with one
/// `RetryInfo` detail.
fn retry_info_details(code: Code, message: &str, retry_delay: Duration) -> Bytes {
    let info = RetryInfo {
        retry_delay: Some(prost_types::Duration {
            seconds: i64::try_from(retry_delay.as_secs()).unwrap_or(i64::MAX),
            // Below one second by definition, so it fits.
            nanos: i32::try_from(retry_delay.subsec_nanos()).unwrap_or(0),
        }),
    };
    let status = RpcStatus {
        code: code as i32,
        message: message.to_owned(),
        details: vec![Any {
            type_url: RETRY_INFO_TYPE_URL.to_owned(),
            value: info.encode_to_vec(),
        }],
    };
    Bytes::from(status.encode_to_vec())
}

/// A refusal the client may retry: `code` with a `RetryInfo` detail of `retry_delay`.
///
/// A zero delay means "retry with your own backoff". `metadata` is sent unchanged, so a caller
/// can keep `grpc-retry-pushback-ms` for clients that use gRPC's built-in retry policy.
#[must_use]
pub fn retryable_status(
    code: Code,
    message: &'static str,
    retry_delay: Duration,
    metadata: MetadataMap,
) -> Status {
    Status::with_details_and_metadata(
        code,
        message,
        retry_info_details(code, message, retry_delay),
        metadata,
    )
}

/// The refusal of a request that found every `max_concurrent_requests` slot in use. The slots
/// free up as in-flight requests complete, so the client may retry.
#[must_use]
pub fn grpc_concurrency_limit_status() -> Status {
    retryable_status(
        Code::ResourceExhausted,
        "Too many concurrent requests",
        Duration::ZERO,
        MetadataMap::new(),
    )
}

/// The retry delay of the `RetryInfo` detail in `status`, or `None` when it has none.
#[must_use]
pub fn retry_delay(status: &Status) -> Option<Duration> {
    let rpc_status = RpcStatus::decode(status.details()).ok()?;
    let any = rpc_status
        .details
        .iter()
        .find(|any| any.type_url == RETRY_INFO_TYPE_URL)?;
    let delay = RetryInfo::decode(any.value.as_slice()).ok()?.retry_delay?;
    Some(Duration::new(
        u64::try_from(delay.seconds).unwrap_or(0),
        u32::try_from(delay.nanos).unwrap_or(0),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: a retryable refusal is built with a 14.5 second delay and pushback metadata.
    /// Guarantees: The status keeps its code, message and metadata, and its details decode to a RetryInfo with exactly that delay.
    #[test]
    fn retryable_status_carries_retry_info_and_metadata() {
        let mut metadata = MetadataMap::new();
        let _ = metadata.insert("grpc-retry-pushback-ms", "14500".parse().expect("ascii"));
        let status = retryable_status(
            Code::ResourceExhausted,
            "rate limit",
            Duration::from_millis(14_500),
            metadata,
        );
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(status.message(), "rate limit");
        assert_eq!(retry_delay(&status), Some(Duration::from_millis(14_500)));
        assert_eq!(
            status
                .metadata()
                .get("grpc-retry-pushback-ms")
                .and_then(|value| value.to_str().ok()),
            Some("14500")
        );
    }

    /// Scenario: the concurrency refusal, and a plain status without details.
    /// Guarantees: The concurrency refusal is RESOURCE_EXHAUSTED with a zero-delay RetryInfo (retry with client backoff); a status without details reports no retry delay.
    #[test]
    fn concurrency_refusal_has_zero_delay_and_plain_status_has_none() {
        let status = grpc_concurrency_limit_status();
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(status.message(), "Too many concurrent requests");
        assert_eq!(retry_delay(&status), Some(Duration::ZERO));
        assert_eq!(retry_delay(&Status::resource_exhausted("no details")), None);
    }

    /// Scenario: a retryable refusal is turned into the HTTP response a tower layer returns.
    /// Guarantees: The response carries the details in the `grpc-status-details-bin` header, so a layer that answers before tonic still delivers the RetryInfo.
    #[test]
    fn layer_response_carries_the_details_header() {
        let response: http::Response<tonic::body::Body> =
            grpc_concurrency_limit_status().into_http();
        assert!(response.headers().contains_key("grpc-status-details-bin"));
    }
}
