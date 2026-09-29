// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Payload-free completion ownership and bounded asynchronous delivery.
//!
//! [`AckToken`] is what the exporter keeps for an admitted request: its routing
//! frames and signal type. [`Notifier`] delivers decided completions over the
//! bounded engine channel: it reserves a slot first, keeping the reservation
//! across polls, and picks the completion to send only once the slot is its
//! own, so a cancelled `select` branch never drops a token and a completion
//! waiting for room never holds a slot another would take first.

use super::outcome::Outcome;
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_engine::clock;
use otel_arrow_dfe_engine::control::{AckMsg, NackMsg, nanos_since_birth};
use otel_arrow_dfe_engine::effect_handler::CompletionPermit;
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::local::exporter::EffectHandler;
use otel_arrow_dfe_otap::metrics::ExporterExportMetrics;
use otel_arrow_dfe_otap::pdata::{CompletionPermitExtension, Context, OtapPdata};
use otel_arrow_dfe_pdata::OtapPayload;
use otel_arrow_dfe_telemetry::common_attributes::{
    Outcome as ExportOutcome, SignalOutcomeAttributes,
};
use otel_arrow_dfe_telemetry::metrics::MeasurementMetricSet;
use std::collections::VecDeque;
use std::future::{Future, pending};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

/// The completion a request is still owed, without its payload.
pub(super) struct AckToken {
    /// Routing frames the ack or nack unwinds along.
    context: Context,
    /// Signal type of the request, restored on the returned empty payload.
    signal: SignalType,
    /// When the exporter took ownership of the completion.
    received: Instant,
    /// The return time the completion carries: [`nanos_since_birth`] when
    /// it was decided, zero before.
    decided_ns: u64,
    /// Fails a test that drops the token without deciding it.
    #[cfg(test)]
    bomb: DropBomb,
}

/// Panics when dropped, unless the token carrying it is decided or its owner
/// is torn down with it.
#[cfg(test)]
struct DropBomb;

#[cfg(test)]
impl Drop for DropBomb {
    fn drop(&mut self) {
        assert!(
            std::thread::panicking(),
            "an AckToken was dropped without a decision"
        );
    }
}

/// Warn, once per process, that a node upstream asked for a payload that no
/// completion of this exporter carries back.
fn warn_payload_not_returned() {
    static WARNED: AtomicBool = AtomicBool::new(false);
    if !WARNED.swap(true, Ordering::Relaxed) {
        otel_warn!(
            "series_parquet.payload.not_returned",
            message = "a node upstream asks for the payload back with a nack, but \
                       series_parquet never returns it, so a processor:retry in front of it \
                       cannot retry; retry behind durable_buffer or leave retries to the \
                       producer"
        );
    }
}

impl AckToken {
    /// Split a request into the completion it owes and its payload.
    ///
    /// The transport headers and the authorization claims derived from them
    /// are dropped here, not held for as long as the exporter holds the token.
    pub(super) fn split(data: OtapPdata) -> (Self, OtapPayload) {
        let (context, payload) = data.into_parts();
        if context.may_return_payload() {
            warn_payload_not_returned();
        }
        let context = context.into_completion_route();
        let signal = payload.signal_type();
        (
            Self {
                context,
                signal,
                received: clock::now(),
                decided_ns: 0,
                #[cfg(test)]
                bomb: DropBomb,
            },
            payload,
        )
    }

    /// Release the token undecided, because the worker or notifier holding
    /// it is being torn down by a test.
    #[cfg(test)]
    pub(super) fn discard(self) {
        std::mem::forget(self.bomb);
    }

    /// The node the request was routed from, which a test identifies it by.
    #[cfg(test)]
    pub(super) fn source(&self) -> Option<usize> {
        self.context.source_node()
    }

    /// Whether a node upstream waits for this completion: without a routing
    /// frame the engine has nowhere to deliver it.
    fn routed(&self) -> bool {
        self.context.has_context_frames()
    }

    /// Total bytes this token keeps resident, inline storage included.
    pub(super) fn bytes(&self) -> usize {
        size_of::<Self>() + self.external_bytes()
    }

    /// When the exporter took ownership of this completion.
    pub(super) fn received(&self) -> Instant {
        self.received
    }

    /// Signal type of the request.
    pub(super) fn signal(&self) -> SignalType {
        self.signal
    }

    /// Bytes owned outside the token's own inline storage.
    ///
    /// Charged separately because the inline part is already accounted for by
    /// whichever container the token currently sits in.
    pub(super) fn external_bytes(&self) -> usize {
        self.context.retained_frame_bytes()
    }

    /// Rebuild the pdata the completion is delivered on, with an empty payload.
    fn pdata(self) -> OtapPdata {
        let Self {
            context,
            signal,
            #[cfg(test)]
            bomb,
            ..
        } = self;
        #[cfg(test)]
        std::mem::forget(bomb);
        OtapPdata::new(context, OtapPayload::empty(signal))
    }
}

/// A reservation of a completion-channel slot that has been started but has
/// not resolved.
type Reserving = Pin<Box<dyn Future<Output = Result<CompletionPermit<OtapPdata>, Error>>>>;

/// One decided completion waiting for the send slot: the token, its outcome
/// and, when the decision has something more specific to say than
/// [`Outcome::sentence`], the reason sentence the sender is told.
///
/// The sentence is shared, because a failed block decides every one of its
/// requests with the same one.
type Queued = (AckToken, Outcome, Option<Rc<str>>);

/// Bounded queue of completions still to be delivered.
pub(super) struct Notifier {
    /// Handle the completions are routed through.
    effects: EffectHandler<OtapPdata>,
    /// Completions waiting for a channel slot; the front one is sent in the
    /// next slot reserved.
    queue: VecDeque<Queued>,
    /// The reservation of the next slot, kept across polls once started.
    reserving: Option<Reserving>,
    /// Maximum number of live completions: queued, and held by a block or
    /// the parking slot until they are pushed here.
    capacity: usize,
    /// Count of completions pushed, per [`Outcome`].
    outcomes: [u64; Outcome::ALL.len()],
    /// Completions the engine would not accept.
    failures: u64,
    /// The Acks among [`Notifier::failures`]: a lost Ack of a written block
    /// makes its sender, or a buffer's replay, store it again.
    lost_acks: u64,
    /// Largest single token observed, for capacity reporting.
    token_high_water: usize,
    /// The shared `exporter.exports` outcome set, when the node registered
    /// one: every decision is recorded here once, at the moment it is taken.
    pub(super) exports: Option<MeasurementMetricSet<ExporterExportMetrics>>,
}

impl Notifier {
    /// Create a notifier that may hold `capacity` live completions.
    ///
    /// The queue grows with the completions it holds and is not reserved at
    /// `capacity`, which is derived from an unbounded configuration value.
    pub(super) fn new(effects: EffectHandler<OtapPdata>, capacity: usize) -> Self {
        Self {
            effects,
            queue: VecDeque::new(),
            reserving: None,
            capacity,
            outcomes: [0; Outcome::ALL.len()],
            failures: 0,
            lost_acks: 0,
            token_high_water: 0,
            exports: None,
        }
    }

    /// Count one decision, once, when it is taken, and stamp the token with
    /// its return time.
    ///
    /// The shared export set records it by signal and by the engine-wide
    /// outcome class (`success` for an ack, `refused` for a rule the request
    /// broke, `failure` for everything the sender may retry), with the time
    /// from receipt to decision.
    ///
    /// Returns the token only when a node upstream waits for it; one that
    /// nobody waits for is released here and never takes a slot.
    fn decided(&mut self, mut token: AckToken, outcome: Outcome) -> Option<AckToken> {
        token.decided_ns = nanos_since_birth();
        self.token_high_water = self.token_high_water.max(token.bytes());
        self.outcomes[outcome as usize] += 1;
        if let Some(exports) = &mut self.exports {
            let class = match outcome {
                Outcome::Ack => ExportOutcome::Success,
                refused if refused.refused() => ExportOutcome::Refused,
                _ => ExportOutcome::Failure,
            };
            exports
                .with(SignalOutcomeAttributes {
                    signal: token.signal,
                    outcome: class,
                })
                .record(clock::now().saturating_duration_since(token.received));
        }
        if token.routed() {
            Some(token)
        } else {
            drop(token.pdata());
            None
        }
    }

    /// Live completions.
    pub(super) fn len(&self) -> usize {
        self.queue.len()
    }

    /// Whether no completion is outstanding.
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes the notifier keeps resident.
    ///
    /// Each token is charged by the queue cell it sits in plus its external
    /// buffers.
    pub(super) fn bytes(&self) -> usize {
        self.queue
            .iter()
            .map(|(token, _, _)| token.external_bytes())
            .sum::<usize>()
            + self.queue.capacity() * size_of::<Queued>()
    }

    /// When the oldest outstanding completion was taken ownership of.
    pub(super) fn oldest(&self) -> Option<Instant> {
        self.queue.iter().map(|(token, _, _)| token.received).min()
    }

    /// Counters of pushed completions, indexed by [`Outcome`].
    pub(super) fn outcomes(&self) -> &[u64; Outcome::ALL.len()] {
        &self.outcomes
    }

    /// Completions the engine would not accept.
    pub(super) fn failures(&self) -> u64 {
        self.failures
    }

    /// Acks the engine would not accept.
    pub(super) fn lost_acks(&self) -> u64 {
        self.lost_acks
    }

    /// Count a completion the engine would not accept.
    fn lost(&mut self, outcome_is_ack: bool) {
        self.failures += 1;
        self.lost_acks += u64::from(outcome_is_ack);
    }

    /// Largest single token the notifier has held.
    pub(super) fn token_high_water(&self) -> usize {
        self.token_high_water
    }

    /// Whether one more request may be admitted, given the number of
    /// completions the caller already owes.
    ///
    /// `live` counts every completion the worker owes wherever it sits: here,
    /// or held by a block or the parking slot until it is pushed. Admission
    /// stops one short of `capacity`, so a saturated worker still has a slot
    /// for the first request [`Notifier::force_shutdown`] refuses.
    pub(super) fn has_credit(&self, live: usize) -> bool {
        live + 1 < self.capacity
    }

    /// Queue one decided request.
    ///
    /// The worker owes at most `capacity` completions, counted while a block
    /// or the parking slot still held this one, so moving it here keeps the
    /// queue within its bound. A push past the bound is queued all the same:
    /// a completion is never destroyed while the channel may still gain
    /// room; only [`Notifier::drain_now`], at the shutdown deadline, counts
    /// what is left as lost.
    pub(super) fn push(&mut self, token: AckToken, outcome: Outcome) {
        self.push_with(token, outcome, None);
    }

    /// Queue one decided request with the reason sentence its sender is told.
    ///
    /// `None` falls back to [`Outcome::sentence`]. The bound is the one
    /// [`Notifier::push`] documents.
    pub(super) fn push_with(&mut self, token: AckToken, outcome: Outcome, reason: Option<Rc<str>>) {
        let Some(token) = self.decided(token, outcome) else {
            return;
        };
        self.queue.push_back((token, outcome, reason));
    }

    /// The engine call one decided completion is delivered by, in a slot
    /// already reserved.
    ///
    /// Every delivery path goes through this, so a queued completion, a
    /// completion abandoned at the shutdown deadline and a force-drained
    /// refusal are reported identically. A reservation that failed releases
    /// the token and is the delivery's error.
    ///
    /// The nack reason is a sentence, the decision's own or the outcome's
    /// default, because it is the status message the producer sees; the
    /// outcome's machine token stays the metric label.
    fn delivery(
        reserved: Result<CompletionPermit<OtapPdata>, Error>,
        token: AckToken,
        outcome: Outcome,
        reason: Option<Rc<str>>,
    ) -> Result<(), Error> {
        let decided_ns = token.decided_ns;
        let data = token.pdata();
        let permit = reserved?;
        let Some(cause) = outcome.nack_cause() else {
            return permit.notify_ack(AckMsg::new(data), decided_ns);
        };
        let sentence = reason.as_deref().unwrap_or(outcome.sentence()).to_owned();
        let nack = if outcome.refused() {
            NackMsg::new_permanent_with_cause(sentence, data, cause)
        } else {
            NackMsg::new_with_cause(sentence, data, cause)
        };
        permit.notify_nack(nack, decided_ns)
    }

    /// Deliver one completion in `reserved`, counting a failure.
    fn deliver(
        &mut self,
        reserved: Result<CompletionPermit<OtapPdata>, Error>,
        (token, outcome, reason): Queued,
    ) -> Result<(), Error> {
        let result = Self::delivery(reserved, token, outcome, reason);
        if result.is_err() {
            self.lost(outcome == Outcome::Ack);
        }
        result
    }

    /// Reserve a slot, then send the completion at the front of the queue in
    /// it.
    ///
    /// Cancellation safe: the reservation is kept across polls and holds no
    /// token, and the completion is chosen only once the slot is reserved, so
    /// a cancelled `select` branch never loses one and the queue's order,
    /// Acks first after [`Notifier::acks_first`], decides which completion
    /// the next slot goes to. With nothing outstanding this never resolves,
    /// which lets the caller use it as an idle `select` branch.
    ///
    /// The reservation is outside tokio's cooperative budget, which reports
    /// `Pending` after 128 operations in one task poll whatever room the
    /// channel has.
    pub(super) async fn next(&mut self) -> Result<(), Error> {
        if self.queue.is_empty() {
            return pending().await;
        }
        let reserved = self.reserving().as_mut().await;
        self.reserving = None;
        let front = self.queue.pop_front().expect("the queue was not empty");
        self.deliver(reserved, front)
    }

    /// The reservation in progress, started if there is none.
    fn reserving(&mut self) -> &mut Reserving {
        let effects = self.effects.clone();
        self.reserving.get_or_insert_with(|| {
            Box::pin(tokio::task::coop::unconstrained(async move {
                effects.reserve_completion().await
            }))
        })
    }

    /// A slot reserved at once, if the channel has room now.
    ///
    /// A reservation still waiting is kept only for a queued completion, so
    /// the notifier never holds a slot it has nothing to send in.
    fn reserve_now(&mut self) -> Option<Result<CompletionPermit<OtapPdata>, Error>> {
        use futures::FutureExt;

        let reserved = self.reserving().as_mut().now_or_never();
        if reserved.is_some() || self.queue.is_empty() {
            self.reserving = None;
        }
        reserved
    }

    /// Refuse one force-drained request with a retryable `NodeShutdown` nack.
    ///
    /// Called after shutdown is latched, possibly many times within one poll.
    /// The refusal is sent at once when nothing is queued and the channel has
    /// room, and otherwise waits for its turn behind what is queued, past
    /// `capacity` if need be: the engine hands over at most what its input
    /// channel buffers, and a refusal is never destroyed while the channel
    /// may still gain room before the deadline. Admission stays closed
    /// meanwhile (see [`Notifier::has_credit`]), so a held block's credit is
    /// not taken.
    pub(super) fn force_shutdown(&mut self, data: OtapPdata) {
        let (token, payload) = AckToken::split(data);
        drop(payload);
        let Some(token) = self.decided(token, Outcome::Shutdown) else {
            return;
        };
        if self.queue.is_empty()
            && let Some(reserved) = self.reserve_now()
        {
            let _ = self.deliver(reserved, (token, Outcome::Shutdown, None));
            return;
        }
        self.queue.push_back((token, Outcome::Shutdown, None));
    }

    /// Attempt one completion immediately, counting a send that would block.
    ///
    /// Used only on the path that must not park a token: the completions
    /// abandoned once the shutdown deadline has elapsed. The token is released either way, so the
    /// request ends decided or counted as a delivery failure, never silently
    /// dropped. The attempt is outside the cooperative budget, so a full
    /// completion channel is the only reason it can fail to be taken.
    fn deliver_now(&mut self, token: AckToken, outcome: Outcome, reason: Option<Rc<str>>) {
        match self.reserve_now() {
            Some(reserved) => {
                let _ = self.deliver(reserved, (token, outcome, reason));
            }
            None => {
                drop(token.pdata());
                self.lost(outcome == Outcome::Ack);
            }
        }
    }

    /// Deliver every outstanding completion, Acks first, waiting for room in
    /// the completion channel, until none is left or `until` has elapsed.
    /// Returns whether none is left.
    ///
    /// Called once the worker has decided everything at the flush cut, so the
    /// completions a successful block published there reach the node
    /// upstream while it still records them, however many more there are
    /// than the channel holds. What is left at `until` is for
    /// [`Notifier::drain_now`].
    pub(super) async fn deliver_until(&mut self, until: Instant) -> bool {
        self.acks_first();
        let deadline = clock::sleep_until(until);
        tokio::pin!(deadline);
        while !self.is_empty() {
            tokio::select! {
                biased;
                () = &mut deadline => return false,
                // A failed send is counted by `next`.
                _ = self.next() => {}
            }
        }
        true
    }

    /// Put the queued Acks ahead of every other completion, so the next
    /// slots reserved go to them: a lost Ack would store a written block
    /// again. The other completions keep their order and are only delayed.
    fn acks_first(&mut self) {
        let (acks, others): (VecDeque<Queued>, VecDeque<Queued>) = self
            .queue
            .drain(..)
            .partition(|(_, outcome, _)| *outcome == Outcome::Ack);
        self.queue = acks;
        self.queue.extend(others);
    }

    /// Attempt every outstanding completion once, without blocking.
    ///
    /// Called when the node is about to return at its shutdown deadline, after
    /// [`Notifier::deliver_until`] when a deadline was set. Whatever the engine cannot take immediately is counted as a
    /// delivery failure and released, so the node leaves nothing undecided and
    /// still returns within its deadline. Acks go first (see
    /// [`Notifier::acks_first`]), so a completion channel with room for only
    /// some takes those whose loss would store a written block again. Every attempt is outside the cooperative budget,
    /// so a completion channel with room takes them all.
    pub(super) fn drain_now(&mut self) {
        self.acks_first();
        for queued in std::mem::take(&mut self.queue) {
            let (token, outcome, reason) = queued;
            self.deliver_now(token, outcome, reason);
        }
        self.reserving = None;
    }
}

/// A test that drops a notifier still holding queued completions tears them
/// down with it undecided.
#[cfg(test)]
impl Drop for Notifier {
    fn drop(&mut self) {
        for (token, _, _) in self.queue.drain(..) {
            token.discard();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{assert_no_more_completions, effects, empty_pdata};
    use super::*;
    use otel_arrow_dfe_engine::control::{NackCause, PipelineCompletionMsg};
    use std::time::Duration;

    /// Scenario: a request carrying transport headers is split (claims cannot be planted from
    /// outside the otap crate).
    /// Guarantees: the completion keeps the routing frames but not the transport headers.
    #[test]
    fn split_drops_transport_headers() {
        use otel_arrow_dfe_config::context::ContextEntryName;
        use otel_arrow_dfe_config::transport_headers::{
            TransportHeader, TransportHeaders, ValueKind,
        };

        let mut context = Context::default();
        context.set_source_node(7);
        let mut headers = TransportHeaders::new();
        headers.push(TransportHeader::new(
            ContextEntryName::try_from("authorization").expect("a valid name"),
            ValueKind::Text,
            b"Bearer secret".to_vec(),
        ));
        context.set_transport_headers(headers);
        let data = OtapPdata::new(context, OtapPayload::empty(SignalType::Logs));
        assert!(data.transport_headers().is_some());

        let (token, _payload) = AckToken::split(data);
        assert!(token.context.transport_headers().is_none());
        assert_eq!(token.signal(), SignalType::Logs);
        token.discard();
    }

    /// Scenario: a token waits in the queue while its slot is reserved in a
    /// full channel.
    /// Guarantees: queue storage and external buffers are each charged once,
    /// before and while the slot is reserved: the reservation holds no token.
    #[tokio::test(flavor = "current_thread")]
    async fn notifier_bytes_do_not_double_count_inline_tokens() {
        let (handler, _rx) = effects(1);
        let mut notify = Notifier::new(handler, 2);

        let (first, payload) = AckToken::split(empty_pdata());
        drop(payload);
        notify.push(first, Outcome::Ack);
        notify.next().await.expect("fill completion channel");

        let (token, payload) = AckToken::split(empty_pdata());
        drop(payload);
        let external = token.external_bytes();
        notify.push(token, Outcome::Ack);
        let queue = notify.queue.capacity() * size_of::<Queued>();
        assert_eq!(notify.bytes(), queue + external);

        assert!(
            tokio::time::timeout(Duration::from_millis(1), notify.next())
                .await
                .is_err()
        );
        assert!(notify.reserving.is_some(), "a slot is being reserved");
        assert_eq!(notify.bytes(), queue + external);
    }

    /// Scenario: each outcome class is delivered through the notifier.
    /// Guarantees: ack as ack, refusal as permanent `Refused` nack, storage failure as retryable
    /// nack.
    #[tokio::test(flavor = "current_thread")]
    async fn each_outcome_is_delivered_as_its_completion() {
        let (handler, mut rx) = effects(4);
        let mut notify = Notifier::new(handler, 2);

        for outcome in [Outcome::Ack, Outcome::RowTooLarge, Outcome::Storage] {
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            notify.push(token, outcome);
            notify.next().await.expect("completion is accepted");
        }

        assert!(matches!(
            rx.recv().await.expect("ack"),
            PipelineCompletionMsg::DeliverAck { .. }
        ));
        match rx.recv().await.expect("refusal") {
            PipelineCompletionMsg::DeliverNack { nack } => {
                assert!(nack.permanent);
                assert_eq!(nack.cause, NackCause::Refused);
                assert_eq!(nack.reason, Outcome::RowTooLarge.sentence());
            }
            other => panic!("expected a nack, got {other:?}"),
        }
        match rx.recv().await.expect("storage failure") {
            PipelineCompletionMsg::DeliverNack { nack } => {
                assert!(!nack.permanent);
                assert_eq!(nack.cause, NackCause::Unspecified);
                assert_eq!(nack.reason, Outcome::Storage.sentence());
            }
            other => panic!("expected a nack, got {other:?}"),
        }
        assert_eq!(notify.outcomes()[Outcome::Ack as usize], 1);
        assert_eq!(notify.outcomes()[Outcome::RowTooLarge as usize], 1);
        assert_eq!(notify.outcomes()[Outcome::Storage as usize], 1);
        assert_eq!(notify.failures(), 0);
        assert!(notify.is_empty());
        assert!(notify.token_high_water() > 0);
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: three force-drained refusals against a channel with room for one and a notifier
    /// bound of one.
    /// Guarantees: the first is delivered, the second waits in its send and the third behind it,
    /// past the bound; all three arrive once the channel is read, none counted as a failure.
    #[tokio::test(flavor = "current_thread")]
    async fn forced_shutdown_refusals_wait_in_the_bound_then_fail() {
        let (handler, mut rx) = effects(1);
        let mut notify = Notifier::new(handler, 1);

        notify.force_shutdown(empty_pdata());
        assert_eq!(notify.failures(), 0);
        assert!(notify.is_empty());

        // The channel now holds the first refusal, so the second cannot be
        // handed over without waiting: its send waits for room.
        notify.force_shutdown(empty_pdata());
        assert_eq!(notify.failures(), 0);
        assert_eq!(notify.len(), 1);

        // The notifier is at its bound of one, yet the third waits behind
        // the second rather than being dropped while the channel is full.
        notify.force_shutdown(empty_pdata());
        assert_eq!(notify.failures(), 0);
        assert_eq!(notify.len(), 2);
        assert_eq!(notify.outcomes()[Outcome::Shutdown as usize], 3);

        for _ in 0..3 {
            match rx.recv().await.expect("shutdown refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(nack.cause, NackCause::NodeShutdown);
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            if !notify.is_empty() {
                notify.next().await.expect("the waiting refusal is sent");
            }
        }
        assert!(notify.is_empty());
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: 300 completions, more than tokio's cooperative budget, drained at the deadline
    /// into a channel with room.
    /// Guarantees: all are delivered and none is counted as a failure.
    #[tokio::test(flavor = "current_thread")]
    async fn a_deadline_drain_delivers_more_than_the_coop_budget() {
        const N: usize = 300;
        let (handler, mut rx) = effects(N);
        let mut notify = Notifier::new(handler, N + 1);
        for _ in 0..N {
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            notify.push(token, Outcome::Shutdown);
        }
        notify.drain_now();
        assert_eq!(notify.failures(), 0);
        assert!(notify.is_empty());
        let mut delivered = 0;
        while let Ok(message) = rx.try_recv() {
            assert!(matches!(message, PipelineCompletionMsg::DeliverNack { .. }));
            delivered += 1;
        }
        assert_eq!(delivered, N);
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: 300 force-drained requests within one task poll, with room in the channel.
    /// Guarantees: each gets a delivered retryable `NodeShutdown` nack.
    #[tokio::test(flavor = "current_thread")]
    async fn force_drained_refusals_beyond_the_coop_budget_are_all_delivered() {
        const N: usize = 300;
        let (handler, mut rx) = effects(N);
        let mut notify = Notifier::new(handler, 8);
        for _ in 0..N {
            notify.force_shutdown(empty_pdata());
        }
        assert_eq!(notify.failures(), 0);
        let mut delivered = 0;
        while let Ok(message) = rx.try_recv() {
            match message {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert_eq!(nack.cause, NackCause::NodeShutdown);
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            delivered += 1;
        }
        assert_eq!(delivered + notify.len(), N);
        assert_eq!(delivered, N);
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: a notifier with a capacity no queue could hold.
    /// Guarantees: creation allocates nothing and the queue grows only with what it holds.
    #[tokio::test(flavor = "current_thread")]
    async fn a_huge_capacity_is_not_preallocated() {
        let (handler, mut rx) = effects(1);
        let mut notify = Notifier::new(handler, usize::MAX / 2);
        let (token, payload) = AckToken::split(empty_pdata());
        drop(payload);
        notify.push(token, Outcome::Ack);
        notify.next().await.expect("the completion is accepted");
        assert!(matches!(
            rx.recv().await.expect("an ack"),
            PipelineCompletionMsg::DeliverAck { .. }
        ));
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: a completion waiting for a slot and a later completion
    /// queued behind it.
    /// Guarantees: the oldest outstanding completion is the one waiting.
    #[tokio::test(flavor = "current_thread")]
    async fn oldest_is_the_waiting_completion_not_the_later_one() {
        let (handler, _rx) = effects(1);
        let mut notify = Notifier::new(handler, 4);
        assert_eq!(notify.oldest(), None);

        let (first, payload) = AckToken::split(empty_pdata());
        drop(payload);
        let delivered_received = first.received;
        notify.push(first, Outcome::Ack);
        notify.next().await.expect("fill completion channel");
        assert_eq!(notify.oldest(), None);

        let (blocked, payload) = AckToken::split(empty_pdata());
        drop(payload);
        let blocked_received = blocked.received;
        notify.push(blocked, Outcome::Ack);

        // The two remaining tokens must carry distinct timestamps for the
        // choice between them to mean anything, and the system clock is what
        // `AckToken::split` reads.
        tokio::time::sleep(Duration::from_millis(2)).await;
        let (queued, payload) = AckToken::split(empty_pdata());
        drop(payload);
        let queued_received = queued.received;
        notify.push(queued, Outcome::Ack);
        assert!(blocked_received < queued_received);
        assert!(delivered_received < blocked_received);

        assert!(
            tokio::time::timeout(Duration::from_millis(1), notify.next())
                .await
                .is_err()
        );
        assert!(notify.reserving.is_some());
        assert_eq!(notify.len(), 2);
        assert_eq!(notify.oldest(), Some(blocked_received));
    }

    /// Scenario: normal completions up to one short of capacity, then two force-drained refusals
    /// into a completion channel with room for one.
    /// Guarantees: the last place takes the first refusal; the second waits past the bound
    /// instead of being dropped, and is delivered once the channel gains room.
    #[tokio::test(flavor = "current_thread")]
    async fn the_last_completion_place_is_left_for_a_forced_refusal() {
        // Capacity 4 stands for 2N with N = 2; the cap on normal live
        // completions is therefore 3.
        let (handler, mut rx) = effects(1);
        let mut notify = Notifier::new(handler, 4);

        for _ in 0..3 {
            assert!(notify.has_credit(notify.len()));
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            notify.push(token, Outcome::Ack);
        }
        assert_eq!(notify.len(), 3);
        assert!(!notify.has_credit(notify.len()));

        notify.force_shutdown(empty_pdata());
        assert_eq!(notify.len(), 4, "the last place takes the refusal");
        notify.force_shutdown(empty_pdata());
        assert_eq!(
            notify.len(),
            5,
            "a refusal past the bound waits, not dropped"
        );
        assert_eq!(notify.failures(), 0);
        assert_eq!(notify.outcomes()[Outcome::Shutdown as usize], 2);
        let mut delivered = 0;
        while !notify.is_empty() {
            if rx.try_recv().is_ok() {
                delivered += 1;
            }
            notify.next().await.expect("the next completion is sent");
        }
        while rx.try_recv().is_ok() {
            delivered += 1;
        }
        assert_eq!(delivered, 5, "every completion is delivered");
        assert_eq!(notify.failures(), 0);
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: pushes past the notifier's bound into a completion channel with room for one.
    /// Guarantees: no panic and no completion destroyed: the ones past the bound wait in the
    /// queue and all three are delivered, in order, once the channel is read.
    #[tokio::test(flavor = "current_thread")]
    async fn a_push_past_the_bound_waits_instead_of_being_dropped() {
        let (handler, mut rx) = effects(1);
        let mut notify = Notifier::new(handler, 1);
        for outcome in [Outcome::Ack, Outcome::Shutdown, Outcome::Storage] {
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            notify.push(token, outcome);
        }
        assert_eq!(notify.len(), 3, "every push waits for its send");
        assert_eq!(notify.failures(), 0);
        let mut kinds = Vec::new();
        while !notify.is_empty() {
            notify.next().await.expect("the next completion is sent");
            kinds.push(matches!(
                rx.try_recv().expect("a completion"),
                PipelineCompletionMsg::DeliverAck { .. }
            ));
        }
        assert_eq!(kinds, [true, false, false]);
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: at the deadline a refusal is queued ahead of an Ack, and the
    /// completion channel has room for one of them.
    /// Guarantees: `drain_now` delivers the Ack, whose loss would store a
    /// written block again, and counts the refusal as the lost completion;
    /// no Ack is counted lost.
    #[tokio::test(flavor = "current_thread")]
    async fn drain_now_delivers_acks_before_refusals() {
        let (handler, mut rx) = effects(1);
        let mut notify = Notifier::new(handler, 4);
        for outcome in [Outcome::Shutdown, Outcome::Ack] {
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            notify.push(token, outcome);
        }
        notify.drain_now();
        assert!(matches!(
            rx.try_recv(),
            Ok(PipelineCompletionMsg::DeliverAck { .. })
        ));
        assert_eq!(notify.failures(), 1, "the refusal found the channel full");
        assert_eq!(notify.lost_acks(), 0);
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: a refusal queued ahead of two Acks, a completion channel with
    /// room for one that nobody reads, and a delivery deadline.
    /// Guarantees: `deliver_until` sends an Ack first, returns once the
    /// deadline elapses instead of waiting for room, and leaves the rest to
    /// `drain_now`, which counts them lost.
    #[tokio::test(flavor = "current_thread")]
    async fn deliver_until_puts_acks_first_and_stops_at_its_deadline() {
        let sim = clock::SimClock::new();
        let _clock_guard = sim.install();
        let (handler, mut rx) = effects(1);
        let mut notify = Notifier::new(handler, 4);
        for outcome in [Outcome::Shutdown, Outcome::Ack, Outcome::Ack] {
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            notify.push(token, outcome);
        }
        let until = clock::now() + Duration::from_millis(200);
        {
            let delivery = notify.deliver_until(until);
            tokio::pin!(delivery);
            for _ in 0..8 {
                assert!(
                    futures::FutureExt::now_or_never(delivery.as_mut()).is_none(),
                    "a full channel is waited for until the deadline"
                );
                tokio::task::yield_now().await;
            }
            sim.advance_to(until);
            assert!(!delivery.await, "the deadline left completions undelivered");
        }
        notify.drain_now();
        assert!(notify.is_empty());
        assert!(matches!(
            rx.try_recv(),
            Ok(PipelineCompletionMsg::DeliverAck { .. })
        ));
        assert_eq!(notify.failures(), 2);
        assert_eq!(notify.lost_acks(), 1, "the second Ack found no room");
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: a refusal waits for a slot in a full completion channel when
    /// an Ack is decided; then the channel drains, with room for both before
    /// the delivery deadline.
    /// Guarantees: the Ack takes the first slot and the refusal the next:
    /// both arrive, Ack first, and nothing is lost.
    #[tokio::test(flavor = "current_thread")]
    async fn a_waiting_refusal_yields_the_next_slot_to_an_ack() {
        use futures::FutureExt;

        let sim = clock::SimClock::new();
        let _clock_guard = sim.install();
        let (handler, mut rx) = effects(1);
        let mut notify = Notifier::new(handler, 4);
        for outcome in [Outcome::Ack, Outcome::Shutdown] {
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            notify.push(token, outcome);
        }
        assert!(
            notify.next().now_or_never().is_some(),
            "the first Ack fills the channel"
        );
        assert!(
            notify.next().now_or_never().is_none(),
            "the refusal waits for a slot"
        );
        let (token, payload) = AckToken::split(empty_pdata());
        drop(payload);
        notify.push(token, Outcome::Ack);

        let until = clock::now() + Duration::from_millis(200);
        let (delivered, received) = tokio::join!(notify.deliver_until(until), async {
            let mut received = Vec::new();
            for _ in 0..3 {
                received.push(rx.recv().await.expect("a completion"));
            }
            received
        });
        assert!(delivered, "everything is delivered before the deadline");
        let kinds: Vec<_> = received
            .iter()
            .map(|msg| matches!(msg, PipelineCompletionMsg::DeliverAck { .. }))
            .collect();
        assert_eq!(kinds, [true, true, false], "Acks, then the refusal");
        assert_eq!(notify.failures(), 0);
        assert!(notify.is_empty());
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: a full completion channel nobody reads, then an Ack pushed
    /// and a refusal force-drained for requests whose context has no routing
    /// frame, so no node upstream waits for their completions.
    /// Guarantees: both are decided and counted without waiting for a slot:
    /// nothing is queued, and none is counted as a delivery failure.
    #[tokio::test(flavor = "current_thread")]
    async fn a_completion_nobody_waits_for_never_waits_for_a_slot() {
        use futures::FutureExt;

        let (handler, mut rx) = effects(1);
        let mut notify = Notifier::new(handler, 4);
        let (token, payload) = AckToken::split(empty_pdata());
        drop(payload);
        notify.push(token, Outcome::Ack);
        assert!(
            notify.next().now_or_never().is_some(),
            "the first Ack fills the channel"
        );

        let unrouted = || OtapPdata::new(Context::default(), OtapPayload::empty(SignalType::Logs));
        let (token, payload) = AckToken::split(unrouted());
        drop(payload);
        notify.push(token, Outcome::Ack);
        notify.force_shutdown(unrouted());
        assert!(notify.is_empty(), "nothing waits for a slot");
        notify.drain_now();
        assert_eq!(notify.failures(), 0);
        assert_eq!(notify.outcomes()[Outcome::Ack as usize], 2);
        assert_eq!(notify.outcomes()[Outcome::Shutdown as usize], 1);
        assert!(matches!(
            rx.try_recv(),
            Ok(PipelineCompletionMsg::DeliverAck { .. })
        ));
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: an Ack for a route that measures completion time is decided
    /// while the completion channel is full, and sent 100 ms later once a
    /// slot frees.
    /// Guarantees: its return time is the moment it was decided, as a send
    /// that waits for the channel stamps it, not the moment the slot freed.
    #[tokio::test(flavor = "current_thread")]
    async fn a_completion_waiting_for_a_slot_keeps_its_decision_time() {
        use futures::FutureExt;
        use otel_arrow_dfe_engine::Interests;
        use otel_arrow_dfe_engine::control::{CallData, nanos_since_birth};

        let sim = clock::SimClock::new();
        let _clock_guard = sim.install();
        let (handler, mut rx) = effects(1);
        let mut notify = Notifier::new(handler, 4);
        let (token, payload) = AckToken::split(empty_pdata());
        drop(payload);
        notify.push(token, Outcome::Ack);
        assert!(
            notify.next().now_or_never().is_some(),
            "the first Ack fills the channel"
        );

        let timed = empty_pdata().test_subscribe_to(
            Interests::ACKS | Interests::NODE_COMPLETION_DURATION,
            CallData::default(),
            3,
        );
        let (token, payload) = AckToken::split(timed);
        drop(payload);
        let decided = nanos_since_birth();
        notify.push(token, Outcome::Ack);
        assert!(
            notify.next().now_or_never().is_none(),
            "the Ack waits for a slot"
        );
        sim.advance(Duration::from_millis(100));
        let _first = rx.try_recv().expect("the first Ack");
        notify.next().await.expect("the Ack is sent");
        match rx.try_recv() {
            Ok(PipelineCompletionMsg::DeliverAck { ack }) => {
                assert_eq!(ack.unwind.return_time_ns, decided);
            }
            other => panic!("expected an ack, got {other:?}"),
        }
        assert_no_more_completions(&mut rx);
    }
}
