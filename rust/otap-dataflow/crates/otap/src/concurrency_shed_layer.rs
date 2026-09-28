// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The retryable status of a gRPC request refused at the receiver's concurrency limit.

use crate::grpc_retry_info::status_with_retry_delay;
use std::time::Duration;
use tonic::{Code, Status};

/// Message of every refusal at the receiver's concurrency limit, on gRPC and HTTP.
pub const CONCURRENCY_LIMIT_MESSAGE: &str =
    "receiver concurrency limit reached (max_concurrent_requests); retry later";

/// Retry delay range, in seconds, of a refusal at the concurrency limit: a permit frees
/// within a request's lifetime, and the jitter spreads the retries of clients refused together.
pub const CONCURRENCY_LIMIT_RETRY_AFTER_SECS: std::ops::RangeInclusive<u32> = 1..=3;

/// Builds the status for a request refused at the receiver's concurrency limit, carrying
/// a retry delay drawn from [`CONCURRENCY_LIMIT_RETRY_AFTER_SECS`].
///
/// RESOURCE_EXHAUSTED with a RetryInfo delay, for the reason given in
/// [`crate::grpc_retry_info`].
#[must_use]
pub fn grpc_concurrency_limit_status() -> Status {
    status_with_retry_delay(
        Code::ResourceExhausted,
        CONCURRENCY_LIMIT_MESSAGE,
        Duration::from_secs(u64::from(rand::random_range(
            CONCURRENCY_LIMIT_RETRY_AFTER_SECS,
        ))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// Scenario: 200 gRPC refusals at the concurrency limit.
    /// Guarantees: each is RESOURCE_EXHAUSTED with a `google.rpc.RetryInfo` delay of 1 to 3
    /// whole seconds equal to its `grpc-retry-pushback-ms`, and the delays differ.
    #[test]
    fn concurrency_refusal_carries_jittered_retry_info() {
        let delays: BTreeSet<i64> = (0..200)
            .map(|_| {
                let status = grpc_concurrency_limit_status();
                assert_eq!(status.code(), Code::ResourceExhausted);
                let delay = crate::grpc_retry_info::retry_delay(&status).expect("RetryInfo");
                assert_eq!(delay.nanos, 0);
                let pushback_ms = status
                    .metadata()
                    .get("grpc-retry-pushback-ms")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned);
                assert_eq!(pushback_ms, Some((delay.seconds * 1_000).to_string()));
                delay.seconds
            })
            .collect();
        assert!(delays.iter().all(|d| (1..=3).contains(d)), "{delays:?}");
        assert!(delays.len() > 1, "{delays:?}");
    }
}
