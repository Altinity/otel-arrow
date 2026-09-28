// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Trait and structures used to implement local exporters (!Send).
//!
//! An exporter is an egress node that sends data from a pipeline to external systems, performing
//! the necessary conversions from the internal pdata format to the format required by the external
//! system.
//!
//! Exporters can operate in various ways, including:
//!
//! 1. Sending telemetry data to remote endpoints via network protocols,
//! 2. Writing data to files or databases,
//! 3. Pushing data to message queues or event buses,
//! 4. Or any other method of exporting telemetry data to external systems.
//!
//! # Lifecycle
//!
//! 1. The exporter is instantiated and configured
//! 2. The `start` method is called, which begins the exporter's operation
//! 3. The exporter processes both internal control messages and pipeline data (pdata)
//! 4. The exporter shuts down when it receives a `Shutdown` control message or encounters a fatal
//!    error
//!
//! # Thread Safety
//!
//! This implementation is designed to be used in a single-threaded environment.
//! The `Exporter` trait does not require the `Send` bound, allowing for the use of non-thread-safe
//! types.
//!
//! # Scalability
//!
//! To ensure scalability, the pipeline engine will start multiple instances of the same pipeline
//! in parallel on different cores, each with its own exporter instance.

use crate::Interests;
use crate::control::{AckMsg, NackMsg};
use crate::effect_handler::{
    CompletionPermit, EffectHandlerCore, TelemetryTimerCancelHandle, TimerCancelHandle,
};
use crate::error::Error;
use crate::message::ExporterInbox;
use crate::node::NodeId;
use crate::runtime_services::{CodecEffectHandler, PipelineRuntimeServices};
use crate::terminal_state::TerminalState;
use async_trait::async_trait;
use otel_arrow_dfe_config::transport_headers_policy::HeaderPropagationPolicy;
use otel_arrow_dfe_pdata_codec::CodecService;
use otel_arrow_dfe_telemetry::error::Error as TelemetryError;
use otel_arrow_dfe_telemetry::metrics::{MetricSet, MetricSetHandler};
use otel_arrow_dfe_telemetry::reporter::MetricsReporter;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

/// A trait for egress exporters (!Send definition).
#[async_trait( ? Send)]
pub trait Exporter<PData> {
    /// Starts the exporter and begins exporting incoming data.
    ///
    /// The pipeline engine will call this function to start the exporter in a separate task.
    /// Exporters are assigned their own dedicated task at pipeline initialization because their
    /// primary function involves interacting with the external world, and the pipeline has no
    /// prior knowledge of when these interactions will occur.
    ///
    /// The exporter is taken as `Box<Self>` so the method takes ownership of the exporter once `start` is called.
    /// This lets it move into an independent task, after which the pipeline can only
    /// reach it through the control-message channel.
    ///
    /// Because ownership is now exclusive, the code inside `start` can freely use
    /// `&mut self` to update internal state without worrying about aliasing or
    /// borrowing rules at the call-site. That keeps the public API simple (no
    /// exterior `&mut` references to juggle) while still allowing the exporter to
    /// mutate itself as much as it needs during its run loop.
    ///
    /// Exporters are expected to process both internal control messages and pipeline data messages,
    /// prioritizing control messages over data messages. This prioritization guarantee is ensured
    /// by the `ExporterInbox` implementation.
    ///
    /// # Parameters
    ///
    /// - `inbox`: An inbox that receives pdata or control messages. Control
    ///   messages are prioritized over pdata messages.
    /// - `effect_handler`: A handler to perform side effects such as network operations.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if an unrecoverable error occurs.
    ///
    /// # Cancellation Safety
    ///
    /// This method should be cancellation safe and clean up any resources when dropped.
    async fn start(
        self: Box<Self>,
        inbox: ExporterInbox<PData>,
        effect_handler: EffectHandler<PData>,
    ) -> Result<TerminalState, Error>;
}

/// A `!Send` implementation of the EffectHandler.
#[derive(Clone)]
pub struct EffectHandler<PData> {
    pub(crate) core: EffectHandlerCore<PData>,
    _pd: PhantomData<PData>,
    /// Immutable propagation policy shared by local handler clones.
    /// `None` disables propagation.
    propagation_policy: Option<Rc<HeaderPropagationPolicy>>,
}

impl<PData> EffectHandler<PData> {
    /// Creates a local exporter effect handler.
    #[must_use]
    pub fn new(
        node_id: NodeId,
        metrics_reporter: MetricsReporter,
        runtime_services: PipelineRuntimeServices,
    ) -> Self {
        EffectHandler {
            core: EffectHandlerCore::new(node_id, metrics_reporter, runtime_services),
            _pd: PhantomData,
            propagation_policy: None,
        }
    }

    /// Returns the id of the exporter associated with this handler.
    #[must_use]
    pub fn exporter_id(&self) -> NodeId {
        self.core.node_id()
    }

    /// Returns the precomputed node interests.
    #[must_use]
    pub fn node_interests(&self) -> Interests {
        self.core.node_interests()
    }

    /// Returns the propagation policy.
    ///
    /// `None` disables propagation.
    #[must_use]
    pub fn propagation_policy(&self) -> Option<&HeaderPropagationPolicy> {
        self.propagation_policy.as_deref()
    }

    /// Sets the propagation policy for transport header filtering.
    pub fn set_propagation_policy(&mut self, policy: Option<HeaderPropagationPolicy>) {
        self.propagation_policy = policy.map(Rc::new);
    }

    /// Print an info message to stdout.
    ///
    /// This method provides a standardized way for exporters to output
    /// informational messages without blocking the async runtime.
    pub async fn info(&self, message: &str) {
        self.core.info(message).await;
    }

    /// Reserve a slot in the pipeline-completion channel, waiting for room.
    ///
    /// An exporter that orders the completions it owes decides which one to
    /// send only once the slot is its own, so none is held by a send still
    /// waiting for room. Cancel safe: dropping the future gives the place up.
    pub async fn reserve_completion(&self) -> Result<CompletionPermit<PData>, Error> {
        self.core.reserve_completion().await
    }

    /// Starts a cancellable periodic timer that emits TimerTick on the control channel.
    /// Returns a handle that can be used to cancel the timer.
    ///
    /// Current limitation: Only one timer can be started by an exporter at a time.
    pub async fn start_periodic_timer(
        &self,
        duration: Duration,
    ) -> Result<TimerCancelHandle<PData>, Error> {
        self.core.start_periodic_timer(duration).await
    }

    /// Starts a cancellable periodic telemetry timer that emits CollectTelemetry.
    pub async fn start_periodic_telemetry(
        &self,
        duration: Duration,
    ) -> Result<TelemetryTimerCancelHandle<PData>, Error> {
        self.core.start_periodic_telemetry(duration).await
    }

    /// Reports metrics collected by the exporter.
    #[allow(dead_code)] // Will be used in the future. ToDo report metrics from channel and messages.
    pub(crate) fn report_metrics<M: MetricSetHandler + 'static>(
        &mut self,
        metrics: &mut MetricSet<M>,
    ) -> Result<(), TelemetryError> {
        self.core.report_metrics(metrics)
    }

    // More methods will be added in the future as needed.

    /// Sets the pipeline result message sender for this effect handler.
    ///
    /// Primarily used by tests and manual harnesses that construct an EffectHandler directly;
    /// the engine wiring sets this automatically in `prepare_runtime`.
    pub fn set_pipeline_completion_msg_sender(
        &mut self,
        pipeline_completion_msg_sender: crate::control::PipelineCompletionMsgSender<PData>,
    ) {
        self.core
            .set_pipeline_completion_msg_sender(pipeline_completion_msg_sender);
    }
}

impl<PData> CodecEffectHandler for EffectHandler<PData> {
    fn codec_service(&self) -> &CodecService {
        self.core.runtime_services.codecs()
    }
}

#[async_trait(?Send)]
impl<PData: crate::Unwindable> crate::_private::AckNackRouting<PData> for EffectHandler<PData> {
    async fn route_ack(&self, ack: AckMsg<PData>) -> Result<(), Error> {
        self.core.route_ack(ack).await
    }

    async fn route_nack(&self, nack: NackMsg<PData>) -> Result<(), Error> {
        self.core.route_nack(nack).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(missing_docs)]
    use super::*;
    use crate::Unwindable;
    use crate::completion_emission_metrics::make_completion_emission_metrics;
    use crate::context::ControllerContext;
    use crate::control::{
        Frame, PipelineCompletionMsg, RouteData, pipeline_completion_msg_channel,
    };
    use crate::entity_context::NodeTelemetryHandle;
    use crate::testing::test_node;
    use futures::FutureExt;
    use otel_arrow_dfe_config::{MetricLevel, SignalType, node::NodeKind};
    use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;
    use std::collections::HashMap;

    #[derive(Debug)]
    struct TestPData {
        frames: Vec<Frame>,
    }

    impl TestPData {
        fn with_frame(interests: Interests) -> Self {
            Self {
                frames: vec![Frame {
                    node_id: 1,
                    interests,
                    route: RouteData::default(),
                    output_items: 0,
                    input_items: 0,
                    output_size: 0,
                    input_size: 0,
                }],
            }
        }
    }

    impl Unwindable for TestPData {
        fn has_frames(&self) -> bool {
            !self.frames.is_empty()
        }

        fn pop_frame(&mut self) -> Option<Frame> {
            self.frames.pop()
        }

        fn signal(&self) -> Option<SignalType> {
            None
        }

        fn drop_payload(&mut self) {}
    }

    fn test_node_telemetry() -> (TelemetryRegistryHandle, NodeTelemetryHandle) {
        let registry = TelemetryRegistryHandle::new();
        let controller = ControllerContext::new(registry.clone());
        let pipeline_ctx = controller
            .pipeline_context_with("test_grp".into(), "test_pipeline".into(), 0, 1, 0)
            .with_node_context(
                "test_node".into(),
                "urn:test:exporter:example".into(),
                NodeKind::Exporter,
                HashMap::new(),
            );
        let entity_key = pipeline_ctx.register_node_entity();
        (
            registry,
            NodeTelemetryHandle::new(pipeline_ctx.metrics_registry(), entity_key),
        )
    }

    /// Scenario: an exporter reserves completion slots in a pipeline-completion
    /// channel of capacity one: an Ack through the first slot, a second
    /// reservation while that Ack fills the channel, then a Nack without
    /// frames through the second slot.
    /// Guarantees: a reservation resolves only once the channel has room; the
    /// permit routes the Ack without waiting and records it in the completion
    /// emission metrics; a completion with no frames is skipped and its slot
    /// given back.
    #[tokio::test]
    async fn reserve_completion_waits_for_room_and_routes_through_the_slot() {
        let (_registry, telemetry_handle) = test_node_telemetry();
        let completion_metrics =
            make_completion_emission_metrics(&Some(telemetry_handle), MetricLevel::Normal)
                .expect("completion emission metrics should be registered");
        let (completion_tx, mut completion_rx) = pipeline_completion_msg_channel(1);
        let (_metrics_rx, metrics_reporter) = MetricsReporter::create_new_and_receiver(1);
        let mut eh = EffectHandler::<TestPData>::new(
            test_node("exporter"),
            metrics_reporter,
            crate::testing::test_pipeline_runtime_services(),
        );
        eh.set_pipeline_completion_msg_sender(completion_tx);
        eh.core
            .set_completion_emission_metrics(Some(completion_metrics.clone()));

        let permit = eh.reserve_completion().await.expect("room for the Ack");
        permit
            .route_ack(AckMsg::new(TestPData::with_frame(Interests::ACKS)))
            .expect("the permit routes the Ack");

        let mut reserving = std::pin::pin!(eh.reserve_completion());
        assert!(
            reserving.as_mut().now_or_never().is_none(),
            "the Ack fills the channel"
        );
        assert!(matches!(
            completion_rx.recv().await.expect("the Ack"),
            PipelineCompletionMsg::DeliverAck { .. }
        ));
        let permit = reserving.await.expect("room after the receive");
        permit
            .route_nack(NackMsg::new("no frames", TestPData { frames: Vec::new() }))
            .expect("a Nack without frames is skipped");

        drop(
            eh.reserve_completion()
                .now_or_never()
                .expect("the skipped Nack gave its slot back")
                .expect("room"),
        );
        assert!(completion_rx.try_recv().is_err(), "nothing else was sent");
        let counts = completion_metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .counts();
        assert_eq!(counts, (1, 0));
    }
}
