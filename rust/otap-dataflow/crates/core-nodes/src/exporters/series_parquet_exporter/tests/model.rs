// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! A proptest state machine over the worker, checked for exactly-once
//! decisions and for acknowledgements only of blocks whose files all exist.

use super::support::*;
use futures::FutureExt;
use proptest::prelude::*;
use std::collections::HashMap;

/// One step the environment takes against the worker.
#[derive(Debug, Clone)]
enum Op {
    /// A request arrives: admitted while the gate is open, force-drained once
    /// shutdown is latched, and otherwise left upstream.
    Admit,
    /// The wall and engine clocks reach the next window boundary.
    Boundary,
    /// The store injects this fault into the writes that follow.
    Store(Fault),
    /// Every write parked in the store is released.
    Release,
    /// Shutdown is latched with a deadline this many milliseconds away.
    Shutdown(u64),
    /// The engine clock moves forward this many milliseconds.
    Advance(u64),
    /// The engine takes up to this many completions.
    Drain(usize),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => Just(Op::Admit),
        1 => Just(Op::Boundary),
        1 => prop_oneof![Just(Fault::None), Just(Fault::Series), Just(Fault::Park)]
            .prop_map(Op::Store),
        1 => Just(Op::Release),
        1 => (0_u64..20_000).prop_map(Op::Shutdown),
        2 => (0_u64..3_000).prop_map(Op::Advance),
        2 => (1_usize..4).prop_map(Op::Drain),
    ]
}

/// The worker under test and what the model has seen of it.
struct Model {
    sim: clock::SimClock,
    wall: Arc<lake::clock::TestWallClock>,
    store: Arc<FaultStore>,
    worker: Worker,
    rx: PipelineCompletionMsgReceiver<OtapPdata>,
    /// Requests handed to the worker, numbered from 1 by their routing frame.
    requests: usize,
    /// Requests force-drained after the shutdown latched: their refusals wait
    /// past the notifier's bound instead of being dropped, so they are
    /// outside the admission credit.
    forced: usize,
    /// Completions received, per request.
    completions: HashMap<usize, u32>,
    /// The sequence of the block that last held each request's completion.
    block_of: HashMap<usize, u64>,
    /// The objects each block will write, by sequence, as last seen while it
    /// was ACTIVE.
    planned: HashMap<u64, Vec<String>>,
    /// Whether the shutdown deadline has decided everything.
    ended: bool,
}

impl Model {
    /// Two requests per block, a completion channel with room for two, and
    /// deadlines short enough for the clock steps to reach.
    fn new(sim: clock::SimClock) -> Self {
        let store = fault_store();
        let (handler, rx) = effects(2);
        let wall = Arc::new(lake::clock::TestWallClock::new(0));
        let mut cfg = worker_config_with_requests(2);
        cfg.window.flush_retry_deadline = Duration::from_secs(5);
        cfg.lake.upload.abort_timeout = Duration::from_millis(100);
        let worker = Worker::new(cfg, store.clone(), Arc::clone(&wall) as _, handler);
        Self {
            sim,
            wall,
            store,
            worker,
            rx,
            requests: 0,
            forced: 0,
            completions: HashMap::new(),
            block_of: HashMap::new(),
            planned: HashMap::new(),
            ended: false,
        }
    }

    /// Record which block holds each completion and which objects the ACTIVE
    /// block will write, and check the credit bound.
    fn observe(&mut self) {
        let active = self.worker.active.data.seq;
        if !self.worker.active.data.is_empty() {
            let paths = self.worker.sink.planned_paths(&self.worker.active.data);
            let _ = self
                .planned
                .insert(active, paths.iter().map(ToString::to_string).collect());
        }
        for token in &self.worker.active.tokens {
            let _ = self.block_of.insert(token.source().expect("an id"), active);
        }
        if let Some(job) = &self.worker.flushing {
            for token in &job.tokens {
                let _ = self
                    .block_of
                    .insert(token.source().expect("an id"), job.seq);
            }
        }
        assert!(
            self.worker.live_tokens()
                <= 2 * self.worker.cfg.window.max_requests_per_block + self.forced,
            "the worker owes more admitted completions than the notifier can hold"
        );
    }

    /// Whether every object block `seq` plans is in the store; a block that
    /// holds a request always plans a values file, and a series file unless
    /// every series it carries is already committed in its partition.
    async fn files_exist(&self, seq: u64) -> bool {
        use futures::StreamExt;
        let planned = self
            .planned
            .get(&seq)
            .expect("a block holding a request was seen ACTIVE");
        assert!(
            planned.iter().any(|path| path.contains("dataset=values/")),
            "block {seq} plans a values file"
        );
        let stored: Vec<String> = self
            .store
            .list(None)
            .filter_map(|meta| async move { meta.ok().map(|meta| meta.location.to_string()) })
            .collect()
            .await;
        planned.iter().all(|path| stored.contains(path))
    }

    /// Take up to `limit` completions from the channel, checking each.
    async fn receive(&mut self, limit: usize) {
        for _ in 0..limit {
            let Ok(message) = self.rx.try_recv() else {
                return;
            };
            let (id, acked) = match message {
                PipelineCompletionMsg::DeliverAck { ack } => {
                    (ack.accepted.into_parts().0.source_node(), true)
                }
                PipelineCompletionMsg::DeliverNack { nack } => {
                    ((*nack.refused).into_parts().0.source_node(), false)
                }
            };
            let id = id.expect("every request carries its id");
            let seen = self.completions.entry(id).or_insert(0);
            *seen += 1;
            assert_eq!(*seen, 1, "request {id} was decided twice");
            if acked {
                let seq = *self
                    .block_of
                    .get(&id)
                    .expect("an acknowledged request was held by a block");
                assert!(
                    self.files_exist(seq).await,
                    "request {id} was acknowledged before every file of block {seq} existed"
                );
            }
        }
    }

    /// Hand the notifier's sends up to `limit` turns.
    fn pump(&mut self, limit: usize) {
        for _ in 0..limit {
            if self.worker.notify.is_empty() || self.worker.notify.next().now_or_never().is_none() {
                return;
            }
        }
    }

    /// Give the flush tasks turns, then serve the node loop's branches that
    /// need no message, in the loop's order.
    async fn settle(&mut self) {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        for _ in 0..64 {
            self.observe();
            if self.ended {
                return;
            }
            if let Some(deadline) = self.worker.deadline
                && clock::now() >= deadline
            {
                let _ticker = ticking(&self.sim, Duration::from_millis(50));
                let _ = self.worker.abandon().await;
                self.ended = true;
                return;
            }
            let mut progressed = false;
            if self.worker.deadline.is_none()
                && self.worker.window.sleep.as_mut().now_or_never().is_some()
            {
                self.worker.wake_window();
                progressed = true;
            }
            if let Some(done) = self
                .worker
                .flushing
                .as_mut()
                .and_then(|job| job.try_finish())
            {
                self.worker.complete(Ok(done));
                progressed = true;
            }
            if let Some(job) = self.worker.cleaning.as_mut()
                && job.cleanup().now_or_never().is_some()
            {
                self.worker.cleaning = None;
                if self.worker.rotation_requested {
                    self.worker.rotate();
                }
                self.worker.resume_pending();
                progressed = true;
            }
            if self.worker.rotation_requested
                && (self.worker.active.data.is_empty()
                    || (self.worker.flushing.is_none() && self.worker.cleaning.is_none()))
            {
                self.worker.rotate();
                self.worker.resume_pending();
                progressed = true;
            }
            if !progressed {
                return;
            }
        }
    }

    /// Take one step, then let the worker react to it.
    async fn apply(&mut self, op: &Op) {
        match *op {
            Op::Admit if !self.ended => {
                let latched = self.worker.deadline.is_some();
                if latched || self.worker.accept() {
                    self.requests += 1;
                    let data = logs_pdata_from(self.requests);
                    if latched {
                        self.forced += 1;
                        self.worker.force_shutdown(data);
                    } else {
                        self.worker.admit(data);
                    }
                }
            }
            Op::Boundary => {
                self.wall.advance(1_000_000_000);
                self.sim.advance(Duration::from_secs(1));
            }
            Op::Store(fault) => self.store.hooks().set(fault),
            Op::Release => self.store.hooks().release.notify_waiters(),
            Op::Shutdown(ms) if !self.ended => self
                .worker
                .shutdown(clock::now() + Duration::from_millis(ms)),
            Op::Advance(ms) => self.sim.advance(Duration::from_millis(ms)),
            Op::Drain(limit) => {
                self.receive(limit).await;
                self.pump(limit);
            }
            Op::Admit | Op::Shutdown(_) => {}
        }
        self.settle().await;
    }

    /// Heal the store, shut down and run to the end, then check that every
    /// request was decided exactly once.
    async fn finish(&mut self) {
        if !self.ended && self.worker.deadline.is_none() {
            self.worker.shutdown(clock::now() + Duration::from_secs(30));
        }
        self.store.hooks().set(Fault::None);
        for _ in 0..2_000 {
            self.store.hooks().release.notify_waiters();
            self.receive(usize::MAX).await;
            self.pump(usize::MAX);
            if self.ended || self.worker.is_idle() {
                break;
            }
            self.sim.advance(Duration::from_millis(200));
            self.settle().await;
        }
        self.receive(usize::MAX).await;
        assert!(
            self.ended || self.worker.is_idle(),
            "the worker ends holding nothing"
        );
        assert_eq!(
            self.worker.notify.outcomes().iter().sum::<u64>(),
            self.requests as u64,
            "every request is decided exactly once"
        );
        let undelivered = (1..=self.requests)
            .filter(|id| !self.completions.contains_key(id))
            .count() as u64;
        assert_eq!(
            undelivered,
            self.worker.notify.failures(),
            "a request without a completion is a counted delivery failure"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, .. ProptestConfig::default() })]

    /// Scenario: random sequences of admissions, boundaries, store faults, releases, shutdowns,
    /// clock steps and drains, two requests per block and a completion channel of two, ended by a
    /// heal and a shutdown.
    /// Guarantees: every request gets exactly one completion or one counted delivery failure, the
    /// worker never owes more than its notifier holds, and an ack follows every file of its block.
    #[test]
    fn every_request_is_decided_exactly_once(ops in proptest::collection::vec(op(), 1..40)) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        runtime.block_on(tokio::task::LocalSet::new().run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let mut model = Model::new(sim.clone());
            for op in &ops {
                model.apply(op).await;
            }
            model.finish().await;
        }));
    }
}
