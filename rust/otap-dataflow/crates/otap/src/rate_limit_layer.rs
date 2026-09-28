// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Tower middleware that rejects gRPC requests when a receiver rate bucket is exhausted.

use crate::grpc_retry_info::status_with_retry_delay;
use crate::otlp_metrics::{OtlpProtocol, OtlpReceiverMetrics};
use futures::future::Either;
use http::{Request, Response};
use otel_arrow_dfe_engine::admission::SharedAdmissionGate;
use otel_arrow_dfe_telemetry::common_attributes::ReceiverRejectionErrorType;
use parking_lot::Mutex;
use std::future::{Ready, ready};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tonic::{Code, Status, body::Body};
use tower::{Layer, Service};

/// Builds a gRPC `resource_exhausted` status carrying the retry delay as RetryInfo
/// and retry pushback metadata (see [`crate::grpc_retry_info`]).
#[must_use]
pub fn grpc_rate_limit_status(retry_after_secs: u32) -> Status {
    status_with_retry_delay(
        Code::ResourceExhausted,
        "rate limit",
        Duration::from_secs(u64::from(retry_after_secs.max(1))),
    )
}

/// Builds a gRPC `resource_exhausted` status for a weight-blind saturation refusal.
///
/// The request's weight, and so its exact recovery delay, is not known yet, so the
/// status carries a retry delay drawn from
/// [`crate::concurrency_shed_layer::CONCURRENCY_LIMIT_RETRY_AFTER_SECS`], whose jitter
/// spreads the retries of clients refused together. RFC 0002 gives this early
/// refusal no retry guidance; without RetryInfo, OTLP clients would not retry it.
#[must_use]
pub fn grpc_rate_limit_saturated_status() -> Status {
    status_with_retry_delay(
        Code::ResourceExhausted,
        "rate limit",
        Duration::from_secs(u64::from(rand::random_range(
            crate::concurrency_shed_layer::CONCURRENCY_LIMIT_RETRY_AFTER_SECS,
        ))),
    )
}

/// Builds a gRPC `resource_exhausted` status for a request larger than the configured
/// burst.
///
/// The gate refuses such a request only while memory pressure lasts and admits it once
/// pressure clears, so it carries a jittered RetryInfo delay like a saturation refusal.
/// RFC 0002 answers it with negative retry pushback instead, as a request that can never
/// succeed.
#[must_use]
pub fn grpc_rate_limit_burst_exceeded_status() -> Status {
    status_with_retry_delay(
        Code::ResourceExhausted,
        "request exceeds rate limit burst",
        Duration::from_secs(u64::from(rand::random_range(
            crate::concurrency_shed_layer::CONCURRENCY_LIMIT_RETRY_AFTER_SECS,
        ))),
    )
}

/// Layer that fails fast before concurrency limits and tonic request decoding.
#[derive(Clone)]
pub struct RateLimitLayer {
    rate_limit: Option<RateLimitContext>,
}

#[derive(Clone)]
struct RateLimitContext {
    rate_limiter: SharedAdmissionGate,
    metrics: Arc<Mutex<OtlpReceiverMetrics>>,
}

impl RateLimitLayer {
    /// Creates a new layer backed by the receiver-local rate limiter.
    #[must_use]
    pub fn new(
        rate_limiter: Option<SharedAdmissionGate>,
        metrics: Arc<Mutex<OtlpReceiverMetrics>>,
    ) -> Self {
        Self {
            rate_limit: rate_limiter.map(|rate_limiter| RateLimitContext {
                rate_limiter,
                metrics,
            }),
        }
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService {
            inner,
            rate_limit: self.rate_limit.clone(),
            reject_next_call: false,
        }
    }
}

/// Service implementation for [`RateLimitLayer`].
#[derive(Clone)]
pub struct RateLimitService<S> {
    inner: S,
    rate_limit: Option<RateLimitContext>,
    reject_next_call: bool,
}

impl<S, ReqBody> Service<Request<ReqBody>> for RateLimitService<S>
where
    S: Service<Request<ReqBody>, Response = Response<Body>>,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = Either<S::Future, Ready<Result<Self::Response, Self::Error>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self
            .rate_limit
            .as_ref()
            .is_some_and(|context| context.rate_limiter.is_instance_saturated())
        {
            self.reject_next_call = true;
            return Poll::Ready(Ok(()));
        }
        self.reject_next_call = false;
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let exhausted = self.reject_next_call;
        self.reject_next_call = false;

        if let (true, Some(context)) = (exhausted, self.rate_limit.as_ref()) {
            context
                .rate_limiter
                .record_probed_instance_saturation_refusal();
            context
                .metrics
                .lock()
                .record_rejection(OtlpProtocol::Grpc, ReceiverRejectionErrorType::RateLimit);
            let response = grpc_rate_limit_saturated_status().into_http();
            return Either::Right(ready(Ok(response)));
        }

        Either::Left(self.inner.call(request))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future;
    use otel_arrow_dfe_config::policy::{
        RateLimitAggregation, RateLimitEnforcement, RateLimitPressure, RateLimitUnit,
        RateLimiterPolicy, TokenBucketPolicy,
    };
    use otel_arrow_dfe_engine::admission::{
        AdmissionBinder, AdmissionContext, AdmissionDecision, AdmissionDimension,
    };
    use otel_arrow_dfe_engine::memory_limiter::{
        MemoryPressureLevel, MemoryPressureState, SharedReceiverAdmissionState,
    };
    use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Waker;
    use std::time::Duration;

    #[derive(Clone)]
    struct CountingService {
        poll_ready_calls: Arc<AtomicUsize>,
        call_count: Arc<AtomicUsize>,
    }

    impl CountingService {
        fn new() -> Self {
            Self {
                poll_ready_calls: Arc::new(AtomicUsize::new(0)),
                call_count: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl Service<Request<Body>> for CountingService {
        type Response = Response<Body>;
        type Error = Infallible;
        type Future = future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            let _ = self.poll_ready_calls.fetch_add(1, Ordering::Relaxed);
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Request<Body>) -> Self::Future {
            let _ = self.call_count.fetch_add(1, Ordering::Relaxed);
            future::ready(Ok(Response::new(Body::empty())))
        }
    }

    fn policy_with_mode(enforcement: RateLimitEnforcement) -> RateLimiterPolicy {
        RateLimiterPolicy {
            enforcement,
            aggregation: RateLimitAggregation::ReceiverInstance,
            unit: RateLimitUnit::RequestBytes,
            pressure: RateLimitPressure::Soft,
            token_bucket: TokenBucketPolicy {
                allow: 10,
                interval: Duration::from_secs(1),
                burst: Some(10),
            },
        }
    }

    fn policy() -> RateLimiterPolicy {
        policy_with_mode(RateLimitEnforcement::Enforce)
    }

    fn rate_gate(
        policy: RateLimiterPolicy,
        admission: SharedReceiverAdmissionState,
    ) -> SharedAdmissionGate {
        AdmissionBinder::configured("test", policy)
            .bind_shared(AdmissionDimension::Bytes, admission)
            .expect("bind test admission")
            .expect("configured test admission")
    }

    fn new_metrics() -> Arc<Mutex<OtlpReceiverMetrics>> {
        let metrics_registry_handle = TelemetryRegistryHandle::new();
        let controller_ctx =
            otel_arrow_dfe_engine::context::ControllerContext::new(metrics_registry_handle);
        let pipeline_ctx =
            controller_ctx.pipeline_context_with("grp".into(), "pipeline".into(), 0, 1, 0);
        Arc::new(Mutex::new(OtlpReceiverMetrics::register(&pipeline_ctx)))
    }

    /// Scenario: the gRPC rate-limit layer is installed without a configured limiter.
    /// Guarantees: readiness and calls pass through without retaining or cloning metrics state.
    #[test]
    fn disabled_rate_limit_is_transparent_and_does_not_retain_metrics() {
        let metrics = new_metrics();
        let inner = CountingService::new();
        let poll_ready_calls = inner.poll_ready_calls.clone();
        let call_count = inner.call_count.clone();

        assert_eq!(Arc::strong_count(&metrics), 1);
        let layer = RateLimitLayer::new(None, metrics.clone());
        assert_eq!(Arc::strong_count(&metrics), 1);
        let mut service = layer.layer(inner);
        assert_eq!(Arc::strong_count(&metrics), 1);

        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        let response = futures::executor::block_on(service.call(Request::new(Body::empty())))
            .expect("disabled rate limit should delegate to the inner service");

        assert_eq!(poll_ready_calls.load(Ordering::Relaxed), 1);
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
        assert_eq!(response.headers().get("grpc-status"), None);
        assert_eq!(Arc::strong_count(&metrics), 1);
    }

    /// Scenario: enforce and observe-only limiters both have capacity during soft pressure.
    /// Guarantees: both modes delegate unchanged and record no rate-limit rejection.
    #[test]
    fn under_capacity_rate_limit_modes_are_transparent() {
        for mode in [
            RateLimitEnforcement::Enforce,
            RateLimitEnforcement::ObserveOnly,
        ] {
            let state = MemoryPressureState::default();
            state.set_level_for_tests(MemoryPressureLevel::Soft);
            let admission = SharedReceiverAdmissionState::from_process_state(&state);
            admission.apply(state.current_update(1));
            let limiter = rate_gate(policy_with_mode(mode), admission);
            let metrics = new_metrics();
            let inner = CountingService::new();
            let poll_ready_calls = inner.poll_ready_calls.clone();
            let call_count = inner.call_count.clone();
            let mut service = RateLimitLayer::new(Some(limiter), metrics.clone()).layer(inner);

            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);
            assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
            let response = futures::executor::block_on(service.call(Request::new(Body::empty())))
                .expect("under-capacity rate limit should delegate to the inner service");

            assert_eq!(poll_ready_calls.load(Ordering::Relaxed), 1);
            assert_eq!(call_count.load(Ordering::Relaxed), 1);
            assert_eq!(response.headers().get("grpc-status"), None);
            assert_eq!(
                metrics
                    .lock()
                    .rejections_for(OtlpProtocol::Grpc, ReceiverRejectionErrorType::RateLimit,)
                    .requests
                    .get(),
                0
            );
        }
    }

    /// Scenario: a gRPC request is larger than the configured rate-limit burst.
    /// Guarantees: the response is RESOURCE_EXHAUSTED with a RetryInfo delay of 1 to 3
    /// seconds, since the same request is admitted once memory pressure clears.
    #[test]
    fn oversized_status_is_retryable() {
        let status = grpc_rate_limit_burst_exceeded_status();

        assert_eq!(status.code(), Code::ResourceExhausted);
        let delay = crate::grpc_retry_info::retry_delay(&status).expect("RetryInfo");
        assert!((1..=3).contains(&delay.seconds), "{delay:?}");
    }

    /// Scenario: 200 gRPC refusals of a saturated receiver before request weight is known.
    /// Guarantees: each is RESOURCE_EXHAUSTED with a `google.rpc.RetryInfo` delay of
    /// 1 to 3 whole seconds equal to its `grpc-retry-pushback-ms`, and the delays differ.
    #[test]
    fn saturated_status_carries_jittered_retry_info() {
        let delays: std::collections::BTreeSet<i64> = (0..200)
            .map(|_| {
                let status = grpc_rate_limit_saturated_status();
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

    /// Scenario: gRPC rejects a request after its weighted recovery delay is known.
    /// Guarantees: the authoritative refusal is RESOURCE_EXHAUSTED and carries the exact
    /// delay as a `google.rpc.RetryInfo` detail and as retry pushback.
    #[test]
    fn weighted_status_includes_retry_pushback() {
        let status = grpc_rate_limit_status(14);

        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(
            crate::grpc_retry_info::retry_delay(&status),
            Some(prost_types::Duration {
                seconds: 14,
                nanos: 0
            })
        );
        assert_eq!(
            status
                .metadata()
                .get("grpc-retry-pushback-ms")
                .and_then(|value| value.to_str().ok()),
            Some("14000")
        );
    }

    /// Scenario: the gRPC rate bucket is exhausted while soft pressure is active.
    /// Guarantees: rate fast-fail rejects before polling the inner concurrency-limited service,
    /// with RESOURCE_EXHAUSTED carrying a retry delay (RetryInfo and pushback).
    #[test]
    fn exhausted_rate_limit_short_circuits_before_inner_readiness_and_call() {
        let state = MemoryPressureState::default();
        let admission = SharedReceiverAdmissionState::from_process_state(&state);
        let limiter = rate_gate(policy(), admission.clone());
        assert_eq!(
            limiter.admit(10, AdmissionContext::EMPTY),
            AdmissionDecision::Admit
        );

        state.set_level_for_tests(MemoryPressureLevel::Soft);
        admission.apply(state.current_update(1));

        let metrics_registry_handle = TelemetryRegistryHandle::new();
        let controller_ctx =
            otel_arrow_dfe_engine::context::ControllerContext::new(metrics_registry_handle);
        let pipeline_ctx =
            controller_ctx.pipeline_context_with("grp".into(), "pipeline".into(), 0, 1, 0);
        let metrics = Arc::new(Mutex::new(OtlpReceiverMetrics::register(&pipeline_ctx)));
        let inner = CountingService::new();
        let poll_ready_calls = inner.poll_ready_calls.clone();
        let call_count = inner.call_count.clone();

        let mut service = RateLimitLayer::new(Some(limiter), metrics.clone()).layer(inner);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));

        let response = futures::executor::block_on(service.call(Request::new(Body::empty())))
            .expect("rate limit rejection should not error");

        assert_eq!(poll_ready_calls.load(Ordering::Relaxed), 0);
        assert_eq!(call_count.load(Ordering::Relaxed), 0);
        assert_eq!(
            response
                .headers()
                .get("grpc-status")
                .and_then(|v| v.to_str().ok()),
            Some("8")
        );
        assert!(response.headers().contains_key("grpc-retry-pushback-ms"));
        assert!(response.headers().contains_key("grpc-status-details-bin"));

        let metrics = metrics.lock();
        assert_eq!(
            metrics
                .rejections_for(OtlpProtocol::Grpc, ReceiverRejectionErrorType::RateLimit,)
                .requests
                .get(),
            1
        );
    }

    /// Scenario: rate exhaustion appears after the layer has polled the inner service ready.
    /// Guarantees: the layer still calls the inner service so reserved readiness is consumed.
    #[test]
    fn exhaustion_after_inner_readiness_does_not_skip_inner_call() {
        let state = MemoryPressureState::default();
        let admission = SharedReceiverAdmissionState::from_process_state(&state);
        let limiter = rate_gate(policy(), admission.clone());
        assert_eq!(
            limiter.admit(10, AdmissionContext::EMPTY),
            AdmissionDecision::Admit
        );

        let metrics_registry_handle = TelemetryRegistryHandle::new();
        let controller_ctx =
            otel_arrow_dfe_engine::context::ControllerContext::new(metrics_registry_handle);
        let pipeline_ctx =
            controller_ctx.pipeline_context_with("grp".into(), "pipeline".into(), 0, 1, 0);
        let metrics = Arc::new(Mutex::new(OtlpReceiverMetrics::register(&pipeline_ctx)));
        let inner = CountingService::new();
        let poll_ready_calls = inner.poll_ready_calls.clone();
        let call_count = inner.call_count.clone();

        let mut service = RateLimitLayer::new(Some(limiter), metrics.clone()).layer(inner);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
        state.set_level_for_tests(MemoryPressureLevel::Soft);
        admission.apply(state.current_update(1));

        let response = futures::executor::block_on(service.call(Request::new(Body::empty())))
            .expect("ready inner service should still receive the call");

        assert_eq!(poll_ready_calls.load(Ordering::Relaxed), 1);
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
        assert_eq!(
            response
                .headers()
                .get("grpc-status")
                .and_then(|v| v.to_str().ok()),
            None
        );

        let metrics = metrics.lock();
        assert_eq!(
            metrics
                .rejections_for(OtlpProtocol::Grpc, ReceiverRejectionErrorType::RateLimit,)
                .requests
                .get(),
            0
        );
    }
}
