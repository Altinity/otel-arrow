// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Tower middleware that refuses a gRPC request with a retryable status when the
//! receiver's concurrency limit has no free permit.

use crate::grpc_retry_info::status_with_retry_delay;
use futures::future::Either;
use http::{Request, Response};
use std::future::{Ready, ready};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tonic::{Code, Status, body::Body};
use tower::{Layer, Service};

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

/// Layer that answers [`grpc_concurrency_limit_status`] when the inner service
/// is not ready, instead of queueing the request.
#[derive(Clone)]
pub struct ConcurrencyShedLayer {
    on_refusal: Arc<dyn Fn() + Send + Sync>,
}

impl ConcurrencyShedLayer {
    /// Creates a layer that calls `on_refusal` for each request it refuses.
    #[must_use]
    pub fn new(on_refusal: impl Fn() + Send + Sync + 'static) -> Self {
        Self {
            on_refusal: Arc::new(on_refusal),
        }
    }
}

impl<S> Layer<S> for ConcurrencyShedLayer {
    type Service = ConcurrencyShedService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConcurrencyShedService {
            inner,
            on_refusal: self.on_refusal.clone(),
            inner_ready: false,
        }
    }
}

/// Service implementation for [`ConcurrencyShedLayer`].
///
/// When the inner service is not ready, `poll_ready` still answers ready and
/// `call` refuses. The inner limit keeps its pending permit request until the
/// service is dropped, so the service must be cloned per request, as tonic
/// does (hyper-util's `TowerToHyperService` calls `clone().oneshot(..)`).
pub struct ConcurrencyShedService<S> {
    inner: S,
    on_refusal: Arc<dyn Fn() + Send + Sync>,
    inner_ready: bool,
}

impl<S: Clone> Clone for ConcurrencyShedService<S> {
    fn clone(&self) -> Self {
        // A clone's inner service has not been polled, so it is not ready yet.
        Self {
            inner: self.inner.clone(),
            on_refusal: self.on_refusal.clone(),
            inner_ready: false,
        }
    }
}

impl<S, ReqBody> Service<Request<ReqBody>> for ConcurrencyShedService<S>
where
    S: Service<Request<ReqBody>, Response = Response<Body>>,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = Either<S::Future, Ready<Result<Self::Response, Self::Error>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner_ready = match self.inner.poll_ready(cx) {
            Poll::Ready(Ok(())) => true,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => false,
        };
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        if std::mem::take(&mut self.inner_ready) {
            return Either::Left(self.inner.call(request));
        }
        (self.on_refusal)();
        Either::Right(ready(Ok(grpc_concurrency_limit_status().into_http())))
    }
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
