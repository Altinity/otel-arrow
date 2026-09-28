// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Message definitions for the pipeline engine.

use crate::clock;
use crate::control::{AckMsg, NackMsg, NodeControlMsg};
use crate::local::message::{LocalReceiver, LocalSender};
use crate::node_local_scheduler::NodeLocalSchedulerHandle;
use crate::runtime_services::PipelineShutdownDeadline;
use crate::shared::message::{SharedReceiver, SharedSender};
use crate::{Interests, ReceivedAtNode};
use otel_arrow_dfe_channel::error::{RecvError, SendError};
use otel_arrow_dfe_channel::mpsc;
use std::future::Future;
use std::ops::Add;
use std::time::{Duration, Instant};

/// Maximum number of consecutive control messages delivered before the channel
/// forces one pdata attempt when pdata delivery is allowed.
const CONTROL_BURST_LIMIT: usize = 32;

/// Represents messages sent to nodes (receivers, processors, exporters, or connectors) within the
/// pipeline.
///
/// Messages are categorized as either pipeline data (`PData`) or control messages (`Control`).
#[derive(Debug, Clone)]
pub enum Message<PData> {
    /// A pipeline data message traversing the pipeline.
    PData(PData),

    /// A control message.
    Control(NodeControlMsg<PData>),
}

impl<Data> Message<Data> {
    /// Create a data message with the given payload.
    #[must_use]
    pub const fn data_msg(data: Data) -> Self {
        Message::PData(data)
    }

    /// Create a ACK control message with the given ID.
    #[must_use]
    pub const fn ack_ctrl_msg(ack: AckMsg<Data>) -> Self {
        Message::Control(NodeControlMsg::Ack(ack))
    }

    /// Create a NACK control message with the given ID and reason.
    #[must_use]
    pub const fn nack_ctrl_msg(nack: NackMsg<Data>) -> Self {
        Message::Control(NodeControlMsg::Nack(nack))
    }

    /// Creates a config control message with the given configuration.
    #[must_use]
    pub const fn config_ctrl_msg(config: serde_json::Value) -> Self {
        Message::Control(NodeControlMsg::Config { config })
    }

    /// Creates a timer tick control message.
    #[must_use]
    pub const fn timer_tick_ctrl_msg() -> Self {
        Message::Control(NodeControlMsg::TimerTick {})
    }

    /// Creates a shutdown control message with the given reason.
    #[must_use]
    pub fn shutdown_ctrl_msg(deadline: Instant, reason: &str) -> Self {
        Message::Control(NodeControlMsg::Shutdown {
            deadline,
            reason: reason.to_owned(),
        })
    }

    /// Checks if this message is a data message.
    #[must_use]
    pub const fn is_data(&self) -> bool {
        matches!(self, Message::PData(..))
    }

    /// Checks if this message is a control message.
    #[must_use]
    pub const fn is_control(&self) -> bool {
        matches!(self, Message::Control(..))
    }

    /// Checks if this message is a shutdown control message.
    #[must_use]
    pub const fn is_shutdown(&self) -> bool {
        matches!(self, Message::Control(NodeControlMsg::Shutdown { .. }))
    }
}

/// A generic channel Sender supporting both local and shared semantic (i.e. !Send and Send).
///
/// Rationale:
/// - Local nodes run on a single-threaded `LocalSet`, so it is safe for them to hold either a
///   local sender or a shared sender. This lets the engine select shared channels when any edge
///   requires `Send` (e.g. mixed local/shared fan-in) without extra wiring paths.
/// - Shared nodes keep `SharedSender` directly because their effect handlers must be `Send` to run
///   on multi-threaded executors (`tokio::spawn`). Wrapping in this enum would make them `!Send`
///   and introduce unnecessary branching on hot paths.
#[must_use = "A `Sender` is requested but not used."]
pub enum Sender<T> {
    /// Sender of a local channel.
    Local(LocalSender<T>),
    /// Sender of a shared channel.
    Shared(SharedSender<T>),
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        match self {
            Sender::Local(sender) => Sender::Local(sender.clone()),
            Sender::Shared(sender) => Sender::Shared(sender.clone()),
        }
    }
}

impl<T> Sender<T> {
    /// Creates a new local MPSC sender.
    pub const fn new_local_mpsc_sender(mpsc_sender: mpsc::Sender<T>) -> Self {
        Sender::Local(LocalSender::mpsc(mpsc_sender))
    }

    /// Sends a message to the channel.
    pub async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        match self {
            Sender::Local(sender) => sender.send(msg).await,
            Sender::Shared(sender) => sender.send(msg).await,
        }
    }

    /// Attempts to send a message without awaiting.
    pub fn try_send(&self, msg: T) -> Result<(), SendError<T>> {
        match self {
            Sender::Local(sender) => sender.try_send(msg),
            Sender::Shared(sender) => sender.try_send(msg),
        }
    }
}

/// A generic channel Receiver supporting both local and shared semantic (i.e. !Send and Send).
///
/// See [`Sender`] for the rationale behind using the enum in local contexts while keeping shared
/// nodes on `SharedReceiver` directly.
pub enum Receiver<T> {
    /// Receiver of a local channel.
    Local(LocalReceiver<T>),
    /// Receiver of a shared channel.
    Shared(SharedReceiver<T>),
}

impl<T> Receiver<T> {
    /// Creates a new local MPMC receiver.
    #[must_use]
    pub const fn new_local_mpsc_receiver(mpsc_receiver: mpsc::Receiver<T>) -> Self {
        Receiver::Local(LocalReceiver::mpsc(mpsc_receiver))
    }

    /// Receives a message from the channel.
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        match self {
            Receiver::Local(receiver) => receiver.recv().await,
            Receiver::Shared(receiver) => receiver.recv().await,
        }
    }

    /// Tries to receive a message from the channel.
    pub fn try_recv(&mut self) -> Result<T, RecvError> {
        match self {
            Receiver::Local(receiver) => receiver.try_recv(),
            Receiver::Shared(receiver) => receiver.try_recv(),
        }
    }

    /// Checks if the channel is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            Receiver::Local(receiver) => receiver.is_empty(),
            Receiver::Shared(receiver) => receiver.is_empty(),
        }
    }

    /// Checks whether the receive side has observed channel closure.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        match self {
            Receiver::Local(receiver) => receiver.is_closed(),
            Receiver::Shared(receiver) => receiver.is_closed(),
        }
    }
}

/// Small private adapter trait used by [`InboxCore`].
///
/// The core receive state machine is shared by:
///
/// - local processor/exporter channels, which use [`Receiver`]
/// - shared exporter channels, which use [`SharedReceiver`]
///
/// Rather than duplicating the shutdown/fairness logic for each concrete
/// receiver flavor, the core is generic over this minimal interface. The trait
/// stays private because it is an implementation detail of the channel split,
/// not part of the engine's public channel API.
trait ChannelReceiver<T> {
    fn recv(&mut self) -> impl Future<Output = Result<T, RecvError>> + '_;

    fn try_recv(&mut self) -> Result<T, RecvError>;

    fn is_empty(&self) -> bool;

    fn is_closed(&self) -> bool;
}

impl<T> ChannelReceiver<T> for Receiver<T> {
    fn recv(&mut self) -> impl Future<Output = Result<T, RecvError>> + '_ {
        Receiver::recv(self)
    }

    fn try_recv(&mut self) -> Result<T, RecvError> {
        Receiver::try_recv(self)
    }

    fn is_empty(&self) -> bool {
        Receiver::is_empty(self)
    }

    fn is_closed(&self) -> bool {
        Receiver::is_closed(self)
    }
}

impl<T> ChannelReceiver<T> for SharedReceiver<T> {
    fn recv(&mut self) -> impl Future<Output = Result<T, RecvError>> + '_ {
        SharedReceiver::recv(self)
    }

    fn try_recv(&mut self) -> Result<T, RecvError> {
        SharedReceiver::try_recv(self)
    }

    fn is_empty(&self) -> bool {
        SharedReceiver::is_empty(self)
    }

    fn is_closed(&self) -> bool {
        SharedReceiver::is_closed(self)
    }
}

/// Shutdown-drain policy for [`InboxCore::recv_with_policy`].
///
/// Both processor and exporter channels share the same multiplexing and
/// shutdown machinery, but they intentionally diverge once shutdown has been
/// latched:
///
/// - processors keep honoring admission closure during drain, because
///   `accept_pdata()` is part of their existing engine-managed contract
/// - exporters force-drain already buffered channel data during drain, because
///   exporter-side admission is a self-managed operational choice rather than a
///   processor-style engine contract
///
/// This enum lets the shared core express that difference explicitly without
/// forking the whole receive loop.
#[derive(Clone, Copy)]
enum DrainPolicy {
    /// Respect the caller's admission flag even after shutdown has been
    /// latched.
    HonorAdmission,
    /// Continue to respect normal admission before shutdown, but once shutdown
    /// is latched, allow buffered `pdata` to drain even if admission is
    /// currently closed.
    ForceDrainDuringShutdown,
}

struct InboxCore<PData, ControlRx, PDataRx> {
    control_rx: Option<ControlRx>,
    pdata_rx: Option<PDataRx>,
    local_scheduler: Option<NodeLocalSchedulerHandle<PData>>,
    /// Once a Shutdown is seen, this is set to `Some(instant)` representing the drain deadline.
    shutting_down_deadline: Option<Instant>,
    /// Holds the ControlMsg::Shutdown until after we've drained pdata.
    pending_shutdown: Option<NodeControlMsg<PData>>,
    /// Whether the control receiver outlives the released Shutdown (see
    /// [`ProcessorInbox::recv_completion`]).
    retain_completions: bool,
    /// The control receiver kept after Shutdown was released.
    completions_rx: Option<ControlRx>,
    /// The pipeline's shutdown deadline, once its runtime-control manager
    /// has accepted a shutdown; bounds a Shutdown synthesized for a closed
    /// pdata channel.
    pipeline_deadline: Option<PipelineShutdownDeadline>,
    /// Node ID for entry-frame stamping via `ReceivedAtNode`.
    node_id: usize,
    /// Node interests for entry-frame stamping via `ReceivedAtNode`.
    interests: Interests,
    /// Number of consecutive control messages delivered without a pdata message.
    consecutive_control: usize,
}

impl<PData, ControlRx, PDataRx> InboxCore<PData, ControlRx, PDataRx> {
    fn new(
        control_rx: ControlRx,
        pdata_rx: PDataRx,
        local_scheduler: Option<NodeLocalSchedulerHandle<PData>>,
        node_id: usize,
        interests: Interests,
        retain_completions: bool,
    ) -> Self {
        Self {
            control_rx: Some(control_rx),
            pdata_rx: Some(pdata_rx),
            local_scheduler,
            shutting_down_deadline: None,
            pending_shutdown: None,
            retain_completions,
            completions_rx: None,
            pipeline_deadline: None,
            node_id,
            interests,
            consecutive_control: 0,
        }
    }

    fn shutdown(&mut self) {
        self.shutting_down_deadline = None;
        self.consecutive_control = 0;
        if let Some(local_scheduler) = &self.local_scheduler {
            local_scheduler.begin_shutdown(clock::now());
        }
        let control_rx = self.control_rx.take().expect("control_rx must exist");
        if self.retain_completions {
            self.completions_rx = Some(control_rx);
        }
        drop(self.pdata_rx.take().expect("pdata_rx must exist"));
    }
}

impl<PData, ControlRx, PDataRx> InboxCore<PData, ControlRx, PDataRx>
where
    PData: ReceivedAtNode,
    ControlRx: ChannelReceiver<NodeControlMsg<PData>>,
    PDataRx: ChannelReceiver<PData>,
{
    fn control_message(&mut self, msg: NodeControlMsg<PData>) -> Message<PData> {
        self.consecutive_control = self.consecutive_control.saturating_add(1);
        Message::Control(msg)
    }

    /// Latches a Shutdown read from the control channel, or releases it at
    /// once when its deadline has passed.
    fn latch_shutdown(&mut self, deadline: Instant, reason: String) -> Option<Message<PData>> {
        if deadline <= clock::now() {
            self.shutdown();
            return Some(Message::Control(NodeControlMsg::Shutdown {
                deadline,
                reason,
            }));
        }
        if let Some(local_scheduler) = &self.local_scheduler {
            local_scheduler.begin_shutdown(clock::now());
        }
        self.shutting_down_deadline = Some(deadline);
        self.pending_shutdown = Some(NodeControlMsg::Shutdown { deadline, reason });
        None
    }

    /// A control message read while a Shutdown is latched: a further
    /// Shutdown is absorbed as data, its deadline moving the latched one
    /// earlier but never later, so the node receives Shutdown once. `None`
    /// means the message was absorbed and the receive loop continues.
    fn draining_control(&mut self, msg: NodeControlMsg<PData>) -> Option<Message<PData>> {
        let NodeControlMsg::Shutdown { deadline, .. } = msg else {
            return Some(self.control_message(msg));
        };
        if self
            .shutting_down_deadline
            .is_some_and(|latched| deadline >= latched)
        {
            return None;
        }
        self.shutting_down_deadline = Some(deadline);
        if let Some(NodeControlMsg::Shutdown {
            deadline: pending, ..
        }) = self.pending_shutdown.as_mut()
        {
            *pending = deadline;
        }
        None
    }

    /// What a receive returns once the pdata channel is closed and empty.
    ///
    /// Control messages already queued come first, so a queued Shutdown is
    /// latched with its own deadline and reason rather than replaced by a
    /// synthesized one; `None` means it was latched and the receive loop
    /// continues. With nothing queued, the Shutdown is released (see
    /// [`Self::closed_pdata_shutdown`]).
    fn closed_pdata(&mut self) -> Option<Message<PData>> {
        if self.pending_shutdown.is_none()
            && let Some(control_rx) = self.control_rx.as_mut()
            && let Ok(msg) = control_rx.try_recv()
        {
            return match msg {
                NodeControlMsg::Shutdown { deadline, reason } => {
                    self.latch_shutdown(deadline, reason)
                }
                msg => Some(self.control_message(msg)),
            };
        }
        Some(self.closed_pdata_shutdown())
    }

    fn pdata_message(&mut self, mut pdata: PData) -> Message<PData> {
        self.consecutive_control = 0;
        pdata.received_at_node(self.node_id, self.interests);
        Message::PData(pdata)
    }

    /// Releases the latched Shutdown with its own deadline and reason, or,
    /// when none has been latched, synthesizes one: with the pipeline's
    /// shutdown deadline while the pipeline is shutting down (the node's
    /// own Shutdown may still be buffered by the runtime-control manager),
    /// else one second from now.
    fn closed_pdata_shutdown(&mut self) -> Message<PData> {
        let shutdown = self.pending_shutdown.take().unwrap_or_else(|| {
            let now = clock::now();
            let deadline = self
                .pipeline_deadline
                .as_ref()
                .and_then(PipelineShutdownDeadline::get)
                .filter(|deadline| *deadline > now)
                .unwrap_or_else(|| now.add(Duration::from_secs(1)));
            NodeControlMsg::Shutdown {
                deadline,
                reason: "pdata channel closed".to_owned(),
            }
        });
        self.shutdown();
        Message::Control(shutdown)
    }

    /// Returns whether shutdown draining is allowed to pull `pdata` from the
    /// bounded input channel.
    ///
    /// In normal operation, `accept_pdata` always controls admission. During
    /// shutdown, the answer becomes role-specific:
    ///
    /// - processors still honor `accept_pdata`
    /// - exporters switch to forced draining of already buffered channel data
    ///
    /// This method is the single point where that shutdown-time distinction is
    /// decided for the shared receive loop.
    fn shutdown_drain_accepts_pdata(accept_pdata: bool, policy: DrainPolicy) -> bool {
        accept_pdata || matches!(policy, DrainPolicy::ForceDrainDuringShutdown)
    }

    fn shutdown_drain_complete(&self) -> bool {
        let pdata_rx = self.pdata_rx.as_ref().expect("pdata_rx must exist");
        // Shutdown may only be released once no upstream sender can still
        // deliver more work into this inbox. Queue emptiness alone is not
        // sufficient because an upstream node can still finish one already
        // admitted message outside the channel and send it after we observe an
        // empty buffer.
        pdata_rx.is_closed()
            && pdata_rx.is_empty()
            && self
                .local_scheduler
                .as_ref()
                .map(NodeLocalSchedulerHandle::is_drained)
                .unwrap_or(true)
    }

    fn pop_local_due(&mut self, now: Instant) -> Option<Message<PData>> {
        self.local_scheduler
            .as_ref()
            .and_then(|scheduler| scheduler.pop_due(now))
            .map(|msg| self.control_message(msg))
    }

    fn next_local_expiry_sleep(&self, now: Instant) -> Option<clock::Sleep> {
        self.local_scheduler
            .as_ref()
            .and_then(NodeLocalSchedulerHandle::next_expiry)
            .filter(|when| *when > now)
            .map(clock::sleep_until)
    }

    async fn recv_with_policy(
        &mut self,
        accept_pdata: bool,
        drain_policy: DrainPolicy,
    ) -> Result<Message<PData>, RecvError> {
        let mut sleep_until_deadline: Option<clock::Sleep> = None;

        loop {
            if self.control_rx.is_none() || self.pdata_rx.is_none() {
                // Inbox has been shutdown
                return Err(RecvError::Closed);
            }

            // When pdata is guarded (!accept_pdata), detect a closed pdata
            // channel eagerly so we don't block forever on control-only select.
            // We only probe when the buffer is empty -- try_recv on an empty
            // channel distinguishes Closed from Empty without consuming data.
            if !accept_pdata
                && self
                    .pdata_rx
                    .as_ref()
                    .expect("pdata_rx must exist")
                    .is_empty()
                && let Err(RecvError::Closed) = self
                    .pdata_rx
                    .as_mut()
                    .expect("pdata_rx must exist")
                    .try_recv()
            {
                match self.closed_pdata() {
                    Some(msg) => return Ok(msg),
                    None => continue,
                }
            }

            // Draining mode: Shutdown pending
            if let Some(dl) = self.shutting_down_deadline {
                let drain_accepts_pdata =
                    Self::shutdown_drain_accepts_pdata(accept_pdata, drain_policy);

                // Once shutdown has been latched, the stored Shutdown is released
                // only after the bounded pdata backlog is empty. This keeps the
                // channel-level drain contract explicit: upstream work that was
                // already accepted into the channel gets a chance to run first.
                if self.shutdown_drain_complete() {
                    let shutdown = self
                        .pending_shutdown
                        .take()
                        .expect("pending_shutdown must exist");
                    self.shutdown();
                    return Ok(Message::Control(shutdown));
                }

                if sleep_until_deadline.is_none() {
                    // Create a sleep timer for the deadline
                    sleep_until_deadline = Some(clock::sleep_until(dl));
                }

                let now = clock::now();
                let mut sleep_until_local = self.next_local_expiry_sleep(now);

                // Even while draining we cap control preference. This prevents a
                // sustained Ack/Nack or shutdown-control burst from starving the
                // already buffered pdata that shutdown is trying to drain.
                if drain_accepts_pdata && self.consecutive_control >= CONTROL_BURST_LIMIT {
                    match self
                        .pdata_rx
                        .as_mut()
                        .expect("pdata_rx must exist")
                        .try_recv()
                    {
                        Ok(pdata) => return Ok(self.pdata_message(pdata)),
                        Err(RecvError::Closed) => {
                            let shutdown = self
                                .pending_shutdown
                                .take()
                                .expect("pending_shutdown must exist");
                            self.shutdown();
                            return Ok(Message::Control(shutdown));
                        }
                        Err(RecvError::Empty) => {}
                    }
                }

                if !self
                    .control_rx
                    .as_ref()
                    .expect("control_rx must exist")
                    .is_empty()
                {
                    match self
                        .control_rx
                        .as_mut()
                        .expect("control_rx must exist")
                        .try_recv()
                    {
                        Ok(msg) => match self.draining_control(msg) {
                            Some(msg) => return Ok(msg),
                            None => {
                                sleep_until_deadline = None;
                                continue;
                            }
                        },
                        Err(RecvError::Empty) => {}
                        Err(e) => return Err(e),
                    }
                }

                if let Some(msg) = self.pop_local_due(now) {
                    return Ok(msg);
                }

                // Drain pdata (gated by accept_pdata) and deliver control messages.
                // Honoring accept_pdata during draining lets stateful processors
                // receive Ack/Nack to reduce in-flight state and reopen capacity.
                if drain_accepts_pdata && self.consecutive_control >= CONTROL_BURST_LIMIT {
                    tokio::select! {
                        biased;

                        _ = sleep_until_deadline.as_mut().expect("sleep_until_deadline must exist") => {
                            let shutdown = self.pending_shutdown
                                .take()
                                .expect("pending_shutdown must exist");
                            self.shutdown();
                            return Ok(Message::Control(shutdown));
                        }

                        pdata = self.pdata_rx.as_mut().expect("pdata_rx must exist").recv() => match pdata {
                            Ok(pdata) => return Ok(self.pdata_message(pdata)),
                            Err(_) => {
                                let shutdown = self.pending_shutdown
                                    .take()
                                    .expect("pending_shutdown must exist");
                                self.shutdown();
                                return Ok(Message::Control(shutdown));
                            }
                        },

                        ctrl = self.control_rx.as_mut().expect("control_rx must exist").recv() => match ctrl {
                            Ok(msg) => match self.draining_control(msg) {
                                Some(msg) => return Ok(msg),
                                None => {
                                    sleep_until_deadline = None;
                                    continue;
                                }
                            },
                            Err(e) => return Err(e),
                        },

                        _ = async {
                            if let Some(delay) = sleep_until_local.as_mut() {
                                delay.await;
                            }
                        }, if sleep_until_local.is_some() => {
                            continue;
                        },

                        _ = async {
                            if let Some(local_scheduler) = self.local_scheduler.as_ref() {
                                local_scheduler.wait_for_change().await;
                            }
                        }, if self.local_scheduler.is_some() => {
                            continue;
                        },
                    }
                } else {
                    tokio::select! {
                        biased;

                        _ = sleep_until_deadline.as_mut().expect("sleep_until_deadline must exist") => {
                            let shutdown = self.pending_shutdown
                                .take()
                                .expect("pending_shutdown must exist");
                            self.shutdown();
                            return Ok(Message::Control(shutdown));
                        }

                        ctrl = self.control_rx.as_mut().expect("control_rx must exist").recv() => match ctrl {
                            Ok(msg) => match self.draining_control(msg) {
                                Some(msg) => return Ok(msg),
                                None => {
                                    sleep_until_deadline = None;
                                    continue;
                                }
                            },
                            Err(e) => return Err(e),
                        },

                        pdata = self.pdata_rx.as_mut().expect("pdata_rx must exist").recv(), if drain_accepts_pdata => match pdata {
                            Ok(pdata) => return Ok(self.pdata_message(pdata)),
                            Err(_) => {
                                let shutdown = self.pending_shutdown
                                    .take()
                                    .expect("pending_shutdown must exist");
                                self.shutdown();
                                return Ok(Message::Control(shutdown));
                            }
                        },

                        _ = async {
                            if let Some(delay) = sleep_until_local.as_mut() {
                                delay.await;
                            }
                        }, if sleep_until_local.is_some() => {
                            continue;
                        },

                        _ = async {
                            if let Some(local_scheduler) = self.local_scheduler.as_ref() {
                                local_scheduler.wait_for_change().await;
                            }
                        }, if self.local_scheduler.is_some() => {
                            continue;
                        },
                    }
                }
            }

            // Normal mode: no shutdown yet
            let now = clock::now();
            let mut sleep_until_local = self.next_local_expiry_sleep(now);

            if accept_pdata && self.consecutive_control >= CONTROL_BURST_LIMIT {
                match self
                    .pdata_rx
                    .as_mut()
                    .expect("pdata_rx must exist")
                    .try_recv()
                {
                    Ok(pdata) => return Ok(self.pdata_message(pdata)),
                    Err(RecvError::Closed) => match self.closed_pdata() {
                        Some(msg) => return Ok(msg),
                        None => continue,
                    },
                    Err(RecvError::Empty) => {}
                }
            }

            if !self
                .control_rx
                .as_ref()
                .expect("control_rx must exist")
                .is_empty()
            {
                match self
                    .control_rx
                    .as_mut()
                    .expect("control_rx must exist")
                    .try_recv()
                {
                    Ok(NodeControlMsg::Shutdown { deadline, reason }) => {
                        match self.latch_shutdown(deadline, reason) {
                            Some(msg) => return Ok(msg),
                            None => continue,
                        }
                    }
                    Ok(msg) => return Ok(self.control_message(msg)),
                    Err(RecvError::Empty) => {}
                    Err(e) => return Err(e),
                }
            }

            if let Some(msg) = self.pop_local_due(now) {
                return Ok(msg);
            }

            if accept_pdata && self.consecutive_control >= CONTROL_BURST_LIMIT {
                tokio::select! {
                    biased;

                    pdata = self.pdata_rx.as_mut().expect("pdata_rx must exist").recv() => {
                        match pdata {
                            Ok(pdata) => return Ok(self.pdata_message(pdata)),
                            Err(RecvError::Closed) => match self.closed_pdata() {
                                Some(msg) => return Ok(msg),
                                None => continue,
                            },
                            Err(e) => return Err(e),
                        }
                        }

                    ctrl = self.control_rx.as_mut().expect("control_rx must exist").recv() => match ctrl {
                        Ok(NodeControlMsg::Shutdown { deadline, reason }) => {
                            // The first Shutdown is latched instead of returned
                            // immediately. That switches the channel into
                            // shutdown-drain mode, where it keeps delivering
                            // cleanup control and buffered pdata until either the
                            // backlog empties or the deadline expires.
                            match self.latch_shutdown(deadline, reason) {
                                Some(msg) => return Ok(msg),
                                None => continue,
                            }
                        }
                        Ok(msg) => return Ok(self.control_message(msg)),
                        Err(e)  => return Err(e),
                    },

                    _ = async {
                        if let Some(delay) = sleep_until_local.as_mut() {
                            delay.await;
                        }
                    }, if sleep_until_local.is_some() => {
                        continue;
                    },

                    _ = async {
                        if let Some(local_scheduler) = self.local_scheduler.as_ref() {
                            local_scheduler.wait_for_change().await;
                        }
                    }, if self.local_scheduler.is_some() => {
                        continue;
                    },
                }
            } else {
                tokio::select! {
                    biased;

                    ctrl = self.control_rx.as_mut().expect("control_rx must exist").recv() => match ctrl {
                        Ok(NodeControlMsg::Shutdown { deadline, reason }) => {
                            // Same shutdown latching as above, but in the
                            // control-preferred branch used when pdata admission
                            // is currently closed or control has not yet hit the
                            // fairness limit.
                            match self.latch_shutdown(deadline, reason) {
                                Some(msg) => return Ok(msg),
                                None => continue,
                            }
                        }
                        Ok(msg) => return Ok(self.control_message(msg)),
                        Err(e)  => return Err(e),
                    },

                    pdata = self.pdata_rx.as_mut().expect("pdata_rx must exist").recv(), if accept_pdata => {
                        match pdata {
                            Ok(pdata) => return Ok(self.pdata_message(pdata)),
                            Err(RecvError::Closed) => match self.closed_pdata() {
                                Some(msg) => return Ok(msg),
                                None => continue,
                            },
                            Err(e) => return Err(e),
                        }
                    },

                    _ = async {
                        if let Some(delay) = sleep_until_local.as_mut() {
                            delay.await;
                        }
                    }, if sleep_until_local.is_some() => {
                        continue;
                    },

                    _ = async {
                        if let Some(local_scheduler) = self.local_scheduler.as_ref() {
                            local_scheduler.wait_for_change().await;
                        }
                    }, if self.local_scheduler.is_some() => {
                        continue;
                    }
                }
            }
        }
    }
}

/// Processor-facing receive channel.
///
/// This preserves the existing processor contract: pdata admission is
/// controlled by the engine via `accept_pdata()`, and the admission guard
/// remains authoritative during shutdown draining.
pub struct ProcessorInbox<PData> {
    core: InboxCore<PData, Receiver<NodeControlMsg<PData>>, Receiver<PData>>,
}

impl<PData> ProcessorInbox<PData> {
    /// Creates a new processor inbox.
    #[must_use]
    pub fn new(
        control_rx: Receiver<NodeControlMsg<PData>>,
        pdata_rx: Receiver<PData>,
        node_id: usize,
        interests: Interests,
    ) -> Self {
        Self {
            core: InboxCore::new(control_rx, pdata_rx, None, node_id, interests, false),
        }
    }

    /// Creates the inbox of a processor run loop, with an explicit
    /// processor-local scheduler; it keeps the control receiver after
    /// Shutdown is released, for the completion phase (see
    /// [`ProcessorInbox::recv_completion`]).
    #[must_use]
    pub(crate) fn new_with_local_scheduler(
        control_rx: Receiver<NodeControlMsg<PData>>,
        pdata_rx: Receiver<PData>,
        local_scheduler: NodeLocalSchedulerHandle<PData>,
        node_id: usize,
        interests: Interests,
    ) -> Self {
        Self {
            core: InboxCore::new(
                control_rx,
                pdata_rx,
                Some(local_scheduler),
                node_id,
                interests,
                true,
            ),
        }
    }

    /// Bounds a Shutdown synthesized for a closed pdata channel by the
    /// pipeline's shutdown deadline once the pipeline shuts down.
    pub(crate) fn follow_pipeline_deadline(&mut self, deadline: PipelineShutdownDeadline) {
        self.core.pipeline_deadline = Some(deadline);
    }

    /// Whether the inbox has released its latched Shutdown, so no pdata or
    /// further Shutdown can be received.
    pub(crate) const fn released_shutdown(&self) -> bool {
        self.core.control_rx.is_none()
    }

    /// Drops the control receiver kept after Shutdown, so completions sent
    /// from now on are refused.
    pub(crate) fn close_completions(&mut self) {
        self.core.completions_rx = None;
    }

    /// Receives the next `Ack` or `Nack` sent to the processor after the
    /// inbox released its Shutdown, waiting until `until` or `deadline`,
    /// whichever comes first.
    ///
    /// A further Shutdown moves `deadline` earlier, never later, and is not
    /// returned; other control messages are discarded. Once the wait has
    /// ended, only the messages already queued are returned, so a completion
    /// sent in time is not lost because it was dequeued late. Returns `None`
    /// when nothing is queued then, or when the channel is closed.
    pub(crate) async fn recv_completion(
        &mut self,
        until: Instant,
        deadline: &mut Instant,
    ) -> Option<NodeControlMsg<PData>> {
        let completions = self.core.completions_rx.as_mut()?;
        loop {
            let wait_until = until.min(*deadline);
            let msg = tokio::select! {
                biased;
                () = clock::sleep_until(wait_until) => completions.try_recv().ok()?,
                msg = completions.recv() => msg.ok()?,
            };
            match msg {
                NodeControlMsg::Ack(_) | NodeControlMsg::Nack(_) => return Some(msg),
                NodeControlMsg::Shutdown {
                    deadline: other, ..
                } => {
                    *deadline = (*deadline).min(other);
                }
                _ => {}
            }
        }
    }
}

impl<PData: ReceivedAtNode> ProcessorInbox<PData> {
    /// Receives the next message while honoring the current processor
    /// admission state, including during shutdown draining.
    pub async fn recv_when(&mut self, accept_pdata: bool) -> Result<Message<PData>, RecvError> {
        self.core
            .recv_with_policy(accept_pdata, DrainPolicy::HonorAdmission)
            .await
    }
}

/// Exporter-facing receive channel.
///
/// Exporters own their receive loop directly. During shutdown draining,
/// buffered pdata is force-drained even when the exporter has temporarily
/// closed normal pdata admission.
pub struct ExporterInbox<
    PData,
    ControlRx = Receiver<NodeControlMsg<PData>>,
    PDataRx = Receiver<PData>,
> {
    core: InboxCore<PData, ControlRx, PDataRx>,
}

impl<PData, ControlRx, PDataRx> ExporterInbox<PData, ControlRx, PDataRx> {
    #[must_use]
    pub(crate) fn new_internal(
        control_rx: ControlRx,
        pdata_rx: PDataRx,
        node_id: usize,
        interests: Interests,
    ) -> Self {
        Self {
            core: InboxCore::new(control_rx, pdata_rx, None, node_id, interests, false),
        }
    }
}

#[allow(private_bounds)]
impl<PData, ControlRx, PDataRx> ExporterInbox<PData, ControlRx, PDataRx>
where
    PData: ReceivedAtNode,
    ControlRx: ChannelReceiver<NodeControlMsg<PData>>,
    PDataRx: ChannelReceiver<PData>,
{
    pub(crate) async fn recv_internal(&mut self) -> Result<Message<PData>, RecvError> {
        self.recv_when_internal(true).await
    }

    pub(crate) async fn recv_when_internal(
        &mut self,
        accept_pdata: bool,
    ) -> Result<Message<PData>, RecvError> {
        self.core
            .recv_with_policy(accept_pdata, DrainPolicy::ForceDrainDuringShutdown)
            .await
    }
}

impl<PData> ExporterInbox<PData> {
    /// Creates a new exporter inbox.
    #[must_use]
    pub fn new(
        control_rx: Receiver<NodeControlMsg<PData>>,
        pdata_rx: Receiver<PData>,
        node_id: usize,
        interests: Interests,
    ) -> Self {
        Self::new_internal(control_rx, pdata_rx, node_id, interests)
    }
}

impl<PData, ControlRx, PDataRx> ExporterInbox<PData, ControlRx, PDataRx> {
    /// Bounds a Shutdown synthesized for a closed pdata channel by the
    /// pipeline's shutdown deadline once the pipeline shuts down.
    pub(crate) fn follow_pipeline_deadline(&mut self, deadline: PipelineShutdownDeadline) {
        self.core.pipeline_deadline = Some(deadline);
    }
}

impl<PData: ReceivedAtNode> ExporterInbox<PData> {
    /// Receives the next message with pdata admission enabled.
    pub async fn recv(&mut self) -> Result<Message<PData>, RecvError> {
        self.recv_internal().await
    }

    /// Receives the next message. During shutdown draining, buffered pdata is
    /// drained even if normal exporter admission is currently closed.
    pub async fn recv_when(&mut self, accept_pdata: bool) -> Result<Message<PData>, RecvError> {
        self.recv_when_internal(accept_pdata).await
    }
}

/// Send-friendly exporter inbox type for shared exporter runtimes.
pub(crate) type SharedExporterInbox<PData> =
    ExporterInbox<PData, SharedReceiver<NodeControlMsg<PData>>, SharedReceiver<PData>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WakeupError;
    use crate::local::message::LocalReceiver;
    use crate::testing::TestMsg;
    use otel_arrow_dfe_channel::mpsc;
    use std::time::Duration;

    fn local_processor_inbox(
        wakeup_capacity: usize,
    ) -> (
        mpsc::Sender<NodeControlMsg<TestMsg>>,
        mpsc::Sender<TestMsg>,
        NodeLocalSchedulerHandle<TestMsg>,
        ProcessorInbox<TestMsg>,
    ) {
        let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<TestMsg>>::new(64);
        let (pdata_tx, pdata_rx) = mpsc::Channel::<TestMsg>::new(64);
        let scheduler = NodeLocalSchedulerHandle::new(64, wakeup_capacity);
        let inbox = ProcessorInbox::new_with_local_scheduler(
            Receiver::Local(LocalReceiver::mpsc(control_rx)),
            Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
            scheduler.clone(),
            7,
            Interests::empty(),
        );
        (control_tx, pdata_tx, scheduler, inbox)
    }

    /// Scenario: a processor-local delayed resume is scheduled for immediate
    /// delivery while the processor inbox is otherwise idle.
    /// Guarantees: the inbox surfaces the due retained payload as
    /// `NodeControlMsg::ResumeData` with the original deadline and payload.
    #[tokio::test]
    async fn processor_inbox_emits_due_delayed_resume_as_control_message() {
        let (_control_tx, _pdata_tx, scheduler, mut inbox) = local_processor_inbox(4);
        let when = Instant::now();
        scheduler
            .requeue_later(when, Box::new(TestMsg::new("delayed")))
            .expect("delayed resume should schedule");

        let message = tokio::time::timeout(Duration::from_millis(50), inbox.recv_when(true))
            .await
            .expect("inbox should wake")
            .expect("message should arrive");
        assert!(matches!(
            message,
            Message::Control(NodeControlMsg::ResumeData { when: observed, data })
                if observed == when && *data == TestMsg::new("delayed")
        ));
    }

    /// Scenario: a processor-local wakeup is scheduled for immediate delivery
    /// while the processor inbox is otherwise idle.
    /// Guarantees: the inbox surfaces the due wakeup as
    /// `NodeControlMsg::Wakeup` with the scheduled slot, deadline, and
    /// accepted revision.
    #[tokio::test]
    async fn processor_inbox_emits_due_wakeup_as_control_message() {
        let (_control_tx, _pdata_tx, scheduler, mut inbox) = local_processor_inbox(4);
        let when = Instant::now();
        let outcome = scheduler
            .set_wakeup(crate::control::WakeupSlot(0), when)
            .expect("wakeup should schedule");
        let revision = outcome.revision();

        let message = tokio::time::timeout(Duration::from_millis(50), inbox.recv_when(true))
            .await
            .expect("inbox should wake")
            .expect("message should arrive");
        assert!(matches!(
            message,
            Message::Control(NodeControlMsg::Wakeup {
                slot: crate::control::WakeupSlot(0),
                when: observed,
                revision: observed_revision,
            }) if observed == when && observed_revision == revision
        ));
    }

    /// Scenario: a processor inbox has pending pdata and a burst of due
    /// processor-local delayed resumes.
    /// Guarantees: delayed resumes participate in the existing control
    /// fairness policy, so pdata is eventually delivered instead of starving.
    #[tokio::test]
    async fn processor_inbox_delayed_resume_preserves_control_fairness() {
        let (_control_tx, pdata_tx, scheduler, mut inbox) = local_processor_inbox(4);
        pdata_tx
            .send_async(TestMsg::new("pdata"))
            .await
            .expect("pdata should enqueue");
        let when = Instant::now();
        for idx in 0..40 {
            scheduler
                .requeue_later(when, Box::new(TestMsg::new(format!("delayed-{idx}"))))
                .expect("delayed resume should schedule");
        }

        let mut delayed = 0usize;
        let mut saw_pdata = false;
        while delayed <= CONTROL_BURST_LIMIT {
            match inbox.recv_when(true).await.expect("message should arrive") {
                Message::PData(TestMsg(value)) => {
                    assert_eq!(value, "pdata");
                    saw_pdata = true;
                    break;
                }
                Message::Control(NodeControlMsg::ResumeData { .. }) => {
                    delayed += 1;
                }
                other => panic!("unexpected message {other:?}"),
            }
        }

        assert!(
            saw_pdata,
            "pdata should not starve behind processor-local delayed resumes"
        );
    }

    /// Scenario: a processor inbox has both pending pdata and a burst of due
    /// processor-local wakeups.
    /// Guarantees: wakeups still count as ordinary control traffic for the
    /// existing fairness policy, so pdata is eventually delivered instead of
    /// starving behind an unbounded wakeup burst.
    #[tokio::test]
    async fn processor_inbox_wakeup_preserves_control_fairness() {
        let (_control_tx, pdata_tx, scheduler, mut inbox) = local_processor_inbox(64);
        pdata_tx
            .send_async(TestMsg::new("pdata"))
            .await
            .expect("pdata should enqueue");
        let when = Instant::now();
        for slot in 0..40 {
            let _ = scheduler
                .set_wakeup(crate::control::WakeupSlot(slot), when)
                .expect("wakeup should schedule");
        }

        let mut wakeups = 0usize;
        let mut saw_pdata = false;
        while wakeups <= CONTROL_BURST_LIMIT {
            match inbox.recv_when(true).await.expect("message should arrive") {
                Message::PData(TestMsg(value)) => {
                    assert_eq!(value, "pdata");
                    saw_pdata = true;
                    break;
                }
                Message::Control(NodeControlMsg::Wakeup { .. }) => {
                    wakeups += 1;
                }
                other => panic!("unexpected message {other:?}"),
            }
        }

        assert!(
            saw_pdata,
            "pdata should not starve behind processor-local wakeups"
        );
    }

    /// Scenario: a normal control message is already buffered in the processor
    /// inbox when a processor-local wakeup also becomes due.
    /// Guarantees: the buffered control message is delivered first, and the
    /// due wakeup follows as ordinary control traffic rather than bypassing the
    /// existing control queue.
    #[tokio::test]
    async fn processor_inbox_keeps_buffered_control_ahead_of_due_wakeups() {
        let (control_tx, _pdata_tx, scheduler, mut inbox) = local_processor_inbox(4);
        let when = Instant::now();

        control_tx
            .send_async(NodeControlMsg::Config {
                config: serde_json::json!({"mode": "keep-control-order"}),
            })
            .await
            .expect("config should enqueue");
        let outcome = scheduler
            .set_wakeup(crate::control::WakeupSlot(0), when)
            .expect("wakeup should schedule");
        let revision = outcome.revision();

        let first = inbox.recv_when(true).await.expect("message should arrive");
        assert!(matches!(
            first,
            Message::Control(NodeControlMsg::Config { .. })
        ));

        let second = inbox.recv_when(true).await.expect("message should arrive");
        assert!(matches!(
            second,
            Message::Control(NodeControlMsg::Wakeup {
                slot: crate::control::WakeupSlot(0),
                when: observed,
                revision: observed_revision,
            }) if observed == when && observed_revision == revision
        ));
    }

    /// Scenario: shutdown has been latched and the processor-local scheduler
    /// receives a new delayed-resume request while the inbox is draining.
    /// Guarantees: new delayed resumes are rejected after shutdown latch and
    /// the caller receives the original retained payload back.
    #[tokio::test]
    async fn processor_inbox_rejects_delayed_resumes_after_shutdown_latch() {
        let (control_tx, pdata_tx, scheduler, mut inbox) = local_processor_inbox(4);
        pdata_tx
            .send_async(TestMsg::new("buffered"))
            .await
            .expect("pdata should enqueue");
        control_tx
            .send_async(NodeControlMsg::Shutdown {
                deadline: Instant::now() + Duration::from_secs(1),
                reason: "shutdown".to_owned(),
            })
            .await
            .expect("shutdown should enqueue");
        control_tx
            .send_async(NodeControlMsg::Config {
                config: serde_json::json!({"mode": "draining"}),
            })
            .await
            .expect("config should enqueue");

        let first = inbox
            .recv_when(false)
            .await
            .expect("control should arrive after shutdown latch");
        assert!(matches!(
            first,
            Message::Control(NodeControlMsg::Config { .. })
        ));

        let rejected = scheduler
            .requeue_later(Instant::now(), Box::new(TestMsg::new("rejected")))
            .expect_err("shutdown should reject new delayed resumes");
        assert_eq!(*rejected, TestMsg::new("rejected"));
    }

    /// Scenario: shutdown has been latched and the processor-local scheduler
    /// receives a new wakeup request while the inbox is draining buffered
    /// messages.
    /// Guarantees: new wakeup requests are rejected with
    /// `WakeupError::ShuttingDown` once shutdown has been latched.
    #[tokio::test]
    async fn processor_inbox_rejects_wakeups_after_shutdown_latch() {
        let (control_tx, pdata_tx, scheduler, mut inbox) = local_processor_inbox(4);
        pdata_tx
            .send_async(TestMsg::new("buffered"))
            .await
            .expect("pdata should enqueue");
        control_tx
            .send_async(NodeControlMsg::Shutdown {
                deadline: Instant::now() + Duration::from_secs(1),
                reason: "shutdown".to_owned(),
            })
            .await
            .expect("shutdown should enqueue");
        control_tx
            .send_async(NodeControlMsg::Config {
                config: serde_json::json!({"mode": "draining"}),
            })
            .await
            .expect("config should enqueue");

        let first = inbox
            .recv_when(false)
            .await
            .expect("control should arrive after shutdown latch");
        assert!(matches!(
            first,
            Message::Control(NodeControlMsg::Config { .. })
        ));
        assert_eq!(
            scheduler.set_wakeup(crate::control::WakeupSlot(1), Instant::now()),
            Err(WakeupError::ShuttingDown)
        );
    }

    /// Scenario: shutdown is latched while the processor-local scheduler still
    /// holds a future delayed resume.
    /// Guarantees: pending delayed resumes become immediately available as
    /// `ResumeData` control traffic before the latched shutdown is delivered.
    #[tokio::test]
    async fn processor_inbox_returns_pending_delayed_resumes_on_shutdown_latch() {
        let (control_tx, _pdata_tx, scheduler, mut inbox) = local_processor_inbox(4);
        let original_when = Instant::now() + Duration::from_secs(60);
        scheduler
            .requeue_later(original_when, Box::new(TestMsg::new("delayed")))
            .expect("delayed resume should schedule");
        control_tx
            .send_async(NodeControlMsg::Shutdown {
                deadline: Instant::now() + Duration::from_secs(1),
                reason: "shutdown".to_owned(),
            })
            .await
            .expect("shutdown should enqueue");
        control_tx
            .send_async(NodeControlMsg::Config {
                config: serde_json::json!({"drain": true}),
            })
            .await
            .expect("config should enqueue");

        let first = inbox
            .recv_when(false)
            .await
            .expect("control should arrive after shutdown latch");
        assert!(matches!(
            first,
            Message::Control(NodeControlMsg::Config { .. })
        ));

        let resumed = inbox
            .recv_when(false)
            .await
            .expect("delayed resume should return immediately during shutdown");
        assert!(matches!(
            resumed,
            Message::Control(NodeControlMsg::ResumeData { when, data })
                if when < original_when && *data == TestMsg::new("delayed")
        ));

        let shutdown = inbox
            .recv_when(false)
            .await
            .expect("shutdown should follow once the delayed resume drains");
        assert!(matches!(
            shutdown,
            Message::Control(NodeControlMsg::Shutdown { .. })
        ));
    }

    /// Scenario: a processor-local wakeup is pending when shutdown is latched
    /// and the inbox still has buffered pdata to drain.
    /// Guarantees: pending wakeups are dropped immediately on shutdown latch,
    /// buffered pdata still drains according to the inbox contract, and the
    /// latched shutdown is delivered after draining completes.
    #[tokio::test]
    async fn processor_inbox_drops_pending_wakeups_on_shutdown_latch() {
        let (control_tx, pdata_tx, scheduler, mut inbox) = local_processor_inbox(4);
        pdata_tx
            .send_async(TestMsg::new("buffered"))
            .await
            .expect("pdata should enqueue");
        let _ = scheduler
            .set_wakeup(crate::control::WakeupSlot(2), Instant::now())
            .expect("wakeup should schedule");
        control_tx
            .send_async(NodeControlMsg::Shutdown {
                deadline: Instant::now() + Duration::from_secs(1),
                reason: "shutdown".to_owned(),
            })
            .await
            .expect("shutdown should enqueue");
        control_tx
            .send_async(NodeControlMsg::Config {
                config: serde_json::json!({"drop": true}),
            })
            .await
            .expect("config should enqueue");

        let first = inbox
            .recv_when(false)
            .await
            .expect("control should arrive after shutdown latch");
        assert!(matches!(
            first,
            Message::Control(NodeControlMsg::Config { .. })
        ));

        let drained = inbox
            .recv_when(true)
            .await
            .expect("buffered pdata should drain");
        assert!(matches!(drained, Message::PData(TestMsg(ref value)) if value == "buffered"));

        let shutdown = inbox
            .recv_when(true)
            .await
            .expect("shutdown should follow drain");
        assert!(matches!(
            shutdown,
            Message::Control(NodeControlMsg::Shutdown { .. })
        ));
    }

    /// Scenario: an exporter has latched shutdown, its bounded inbox is
    /// temporarily empty, but an upstream sender is still alive and may still
    /// forward one already-admitted message later.
    /// Guarantees: the exporter does not release the latched shutdown on queue
    /// emptiness alone; it stays alive until that late pdata arrives and the
    /// upstream channel closes.
    #[tokio::test]
    async fn exporter_inbox_waits_for_upstream_closure_before_shutdown() {
        let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<TestMsg>>::new(16);
        let (pdata_tx, pdata_rx) = mpsc::Channel::<TestMsg>::new(16);
        let mut inbox = ExporterInbox::new(
            Receiver::Local(LocalReceiver::mpsc(control_rx)),
            Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
            9,
            Interests::empty(),
        );

        control_tx
            .send_async(NodeControlMsg::Shutdown {
                deadline: Instant::now() + Duration::from_secs(1),
                reason: "shutdown".to_owned(),
            })
            .await
            .expect("shutdown should enqueue");

        let pending = tokio::time::timeout(Duration::from_millis(20), inbox.recv_when(false)).await;
        assert!(
            pending.is_err(),
            "shutdown should stay latched while upstream senders can still deliver pdata"
        );

        pdata_tx
            .send_async(TestMsg::new("late"))
            .await
            .expect("late pdata should enqueue");

        let drained = tokio::time::timeout(Duration::from_millis(50), inbox.recv_when(false))
            .await
            .expect("late pdata should unblock the exporter inbox")
            .expect("late pdata should drain");
        assert!(matches!(drained, Message::PData(TestMsg(ref value)) if value == "late"));

        drop(pdata_tx);

        let shutdown = tokio::time::timeout(Duration::from_millis(50), inbox.recv_when(false))
            .await
            .expect("shutdown should follow once upstream closes")
            .expect("shutdown control should arrive");
        assert!(matches!(
            shutdown,
            Message::Control(NodeControlMsg::Shutdown { .. })
        ));
    }

    /// Scenario: an exporter with admission closed latches a Shutdown, is
    /// handed a control message while it drains, and upstream then drops its
    /// pdata sender.
    /// Guarantees: the next receive releases the latched Shutdown with its own
    /// deadline and reason, not a synthesized one-second Shutdown.
    #[tokio::test]
    async fn exporter_closed_pdata_releases_the_latched_shutdown() {
        let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<TestMsg>>::new(4);
        let (pdata_tx, pdata_rx) = mpsc::Channel::<TestMsg>::new(4);
        let mut inbox = ExporterInbox::new(
            Receiver::Local(LocalReceiver::mpsc(control_rx)),
            Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
            9,
            Interests::empty(),
        );
        let deadline = clock::now() + Duration::from_secs(60);
        control_tx
            .send_async(NodeControlMsg::Shutdown {
                deadline,
                reason: "admin".to_owned(),
            })
            .await
            .expect("shutdown");
        control_tx
            .send_async(NodeControlMsg::Config {
                config: serde_json::json!({}),
            })
            .await
            .expect("config");

        assert!(matches!(
            inbox.recv_when(false).await.expect("config"),
            Message::Control(NodeControlMsg::Config { .. })
        ));
        drop(pdata_tx);
        match inbox.recv_when(false).await.expect("shutdown") {
            Message::Control(NodeControlMsg::Shutdown {
                deadline: released,
                reason,
            }) => {
                assert_eq!(released, deadline);
                assert_eq!(reason, "admin");
            }
            other => panic!("expected the latched shutdown, got {other:?}"),
        }
    }

    /// Scenario: a processor with admission closed latches a Shutdown, is
    /// handed a control message while it drains, and upstream then drops its
    /// pdata sender.
    /// Guarantees: the processor inbox releases the latched Shutdown with its
    /// own deadline and reason, exactly as the exporter inbox does.
    #[tokio::test]
    async fn processor_closed_pdata_releases_the_latched_shutdown() {
        let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<TestMsg>>::new(4);
        let (pdata_tx, pdata_rx) = mpsc::Channel::<TestMsg>::new(4);
        let mut inbox = ProcessorInbox::new(
            Receiver::Local(LocalReceiver::mpsc(control_rx)),
            Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
            9,
            Interests::empty(),
        );
        let deadline = clock::now() + Duration::from_secs(60);
        control_tx
            .send_async(NodeControlMsg::Shutdown {
                deadline,
                reason: "admin".to_owned(),
            })
            .await
            .expect("shutdown");
        control_tx
            .send_async(NodeControlMsg::Config {
                config: serde_json::json!({}),
            })
            .await
            .expect("config");

        assert!(matches!(
            inbox.recv_when(false).await.expect("config"),
            Message::Control(NodeControlMsg::Config { .. })
        ));
        drop(pdata_tx);
        match inbox.recv_when(false).await.expect("shutdown") {
            Message::Control(NodeControlMsg::Shutdown {
                deadline: released,
                reason,
            }) => {
                assert_eq!(released, deadline);
                assert_eq!(reason, "admin");
            }
            other => panic!("expected the latched shutdown, got {other:?}"),
        }
    }

    /// Scenario: a Shutdown and, ahead of it, a Config are queued but not yet
    /// read when upstream drops its pdata sender, and the processor then
    /// receives with admission closed.
    /// Guarantees: the closed-pdata probe does not replace the queued Shutdown
    /// with a synthesized one-second Shutdown: the Config is delivered first,
    /// then the queued Shutdown with its own deadline and reason.
    #[tokio::test]
    async fn processor_closed_pdata_releases_a_queued_shutdown() {
        let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<TestMsg>>::new(4);
        let (pdata_tx, pdata_rx) = mpsc::Channel::<TestMsg>::new(4);
        let mut inbox = ProcessorInbox::new(
            Receiver::Local(LocalReceiver::mpsc(control_rx)),
            Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
            9,
            Interests::empty(),
        );
        let deadline = clock::now() + Duration::from_secs(60);
        control_tx
            .send_async(NodeControlMsg::Config {
                config: serde_json::json!({}),
            })
            .await
            .expect("config");
        control_tx
            .send_async(NodeControlMsg::Shutdown {
                deadline,
                reason: "admin".to_owned(),
            })
            .await
            .expect("shutdown");
        drop(pdata_tx);

        assert!(matches!(
            inbox.recv_when(false).await.expect("config"),
            Message::Control(NodeControlMsg::Config { .. })
        ));
        match inbox.recv_when(false).await.expect("shutdown") {
            Message::Control(NodeControlMsg::Shutdown {
                deadline: released,
                reason,
            }) => {
                assert_eq!(reason, "admin");
                assert_eq!(released, deadline);
            }
            other => panic!("expected the queued shutdown, got {other:?}"),
        }
    }

    /// Scenario: a Shutdown is queued but not yet read when upstream drops
    /// its pdata sender, and the exporter then receives with admission
    /// closed.
    /// Guarantees: the exporter gets the queued Shutdown with its own deadline
    /// and reason, not a synthesized one-second Shutdown.
    #[tokio::test]
    async fn exporter_closed_pdata_releases_a_queued_shutdown() {
        let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<TestMsg>>::new(4);
        let (pdata_tx, pdata_rx) = mpsc::Channel::<TestMsg>::new(4);
        let mut inbox = ExporterInbox::new(
            Receiver::Local(LocalReceiver::mpsc(control_rx)),
            Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
            9,
            Interests::empty(),
        );
        let deadline = clock::now() + Duration::from_secs(60);
        control_tx
            .send_async(NodeControlMsg::Shutdown {
                deadline,
                reason: "admin".to_owned(),
            })
            .await
            .expect("shutdown");
        drop(pdata_tx);

        match inbox.recv_when(false).await.expect("shutdown") {
            Message::Control(NodeControlMsg::Shutdown {
                deadline: released,
                reason,
            }) => {
                assert_eq!(reason, "admin");
                assert_eq!(released, deadline);
            }
            other => panic!("expected the queued shutdown, got {other:?}"),
        }
    }

    /// Scenario: the pipeline has recorded its shutdown deadline, but the
    /// exporter's own Shutdown is not queued yet (the runtime-control manager
    /// still buffers it) when upstream drops its pdata sender.
    /// Guarantees: the Shutdown synthesized for the closed channel carries
    /// the pipeline's deadline; without a recorded deadline it stays one
    /// second.
    #[tokio::test]
    async fn exporter_closed_pdata_follows_the_pipeline_deadline() {
        for recorded in [true, false] {
            let (_control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<TestMsg>>::new(4);
            let (pdata_tx, pdata_rx) = mpsc::Channel::<TestMsg>::new(4);
            let mut inbox = ExporterInbox::new(
                Receiver::Local(LocalReceiver::mpsc(control_rx)),
                Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
                9,
                Interests::empty(),
            );
            let pipeline = PipelineShutdownDeadline::default();
            let deadline = clock::now() + Duration::from_secs(60);
            if recorded {
                pipeline.latch(deadline);
            }
            inbox.follow_pipeline_deadline(pipeline);
            drop(pdata_tx);

            let before = clock::now();
            match inbox.recv_when(false).await.expect("shutdown") {
                Message::Control(NodeControlMsg::Shutdown {
                    deadline: released,
                    reason,
                }) => {
                    assert_eq!(reason, "pdata channel closed");
                    if recorded {
                        assert_eq!(released, deadline);
                    } else {
                        assert!(released >= before + Duration::from_secs(1));
                        assert!(released < deadline);
                    }
                }
                other => panic!("expected a synthesized shutdown, got {other:?}"),
            }
        }
    }

    /// An exporter inbox with a four-slot control and pdata channel, local
    /// senders kept by the caller.
    fn exporter_inbox() -> (
        mpsc::Sender<NodeControlMsg<TestMsg>>,
        mpsc::Sender<TestMsg>,
        ExporterInbox<TestMsg>,
    ) {
        let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<TestMsg>>::new(4);
        let (pdata_tx, pdata_rx) = mpsc::Channel::<TestMsg>::new(4);
        let inbox = ExporterInbox::new(
            Receiver::Local(LocalReceiver::mpsc(control_rx)),
            Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
            9,
            Interests::empty(),
        );
        (control_tx, pdata_tx, inbox)
    }

    /// Scenario: while an exporter inbox drains a latched Shutdown, the
    /// control channel brings a Shutdown with the same deadline (the runtime
    /// manager's deadline resend), then one with an earlier deadline.
    /// Guarantees: neither is returned as a Shutdown; the resend is absorbed
    /// silently, and the earlier deadline is carried by the one Shutdown
    /// released after the drain.
    #[tokio::test]
    async fn a_shutdown_during_the_drain_is_absorbed_as_its_deadline() {
        let (control_tx, pdata_tx, mut inbox) = exporter_inbox();
        let deadline = clock::now() + Duration::from_secs(60);
        let tighter = deadline - Duration::from_secs(30);
        pdata_tx
            .send_async(TestMsg::new("buffered"))
            .await
            .expect("pdata");
        for next in [deadline, deadline, tighter] {
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: next,
                    reason: "test".to_owned(),
                })
                .await
                .expect("shutdown");
        }

        assert!(matches!(
            inbox.recv_when(false).await.expect("forced data"),
            Message::PData(_)
        ));
        drop(pdata_tx);
        assert!(matches!(
            inbox.recv_when(false).await.expect("shutdown control"),
            Message::Control(NodeControlMsg::Shutdown { deadline: released, .. })
                if released == tighter
        ));
        assert!(
            inbox.recv_when(false).await.is_err(),
            "Shutdown is released once"
        );
    }

    /// Scenario: a processor inbox drains a latched Shutdown while the
    /// runtime manager's deadline resend is queued, then its pdata closes.
    /// Guarantees: the resend never reaches the processor; the one Shutdown
    /// released after the drain is the latched one.
    #[tokio::test]
    async fn a_processor_inbox_absorbs_the_deadline_resend() {
        let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<TestMsg>>::new(4);
        let (pdata_tx, pdata_rx) = mpsc::Channel::<TestMsg>::new(4);
        let mut inbox = ProcessorInbox::new(
            Receiver::Local(LocalReceiver::mpsc(control_rx)),
            Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
            9,
            Interests::empty(),
        );
        let deadline = clock::now() + Duration::from_secs(60);
        pdata_tx
            .send_async(TestMsg::new("buffered"))
            .await
            .expect("pdata");
        for _ in 0..2 {
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline,
                    reason: "test".to_owned(),
                })
                .await
                .expect("shutdown");
        }
        assert!(matches!(
            inbox.recv_when(true).await.expect("drained data"),
            Message::PData(_)
        ));
        drop(pdata_tx);
        assert!(matches!(
            inbox.recv_when(true).await.expect("shutdown"),
            Message::Control(NodeControlMsg::Shutdown { deadline: released, .. })
                if released == deadline
        ));
        assert!(inbox.released_shutdown());
        assert!(
            inbox.recv_when(true).await.is_err(),
            "Shutdown is released once"
        );
    }
}
