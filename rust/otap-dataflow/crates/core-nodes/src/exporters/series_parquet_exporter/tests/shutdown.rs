// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shutdown: force-drained requests, the deadline and the release of
//! both blocks.

use super::super::worker::{
    COMPLETION_DISPATCH_MARGIN, DURABLE_BUFFER_RECORDING_RESERVE, SHUTDOWN_COMPLETION_MARGIN,
};
use super::support::*;

/// Scenario: two buffered requests and a third sent after shutdown latched, the sender alive
/// until after the third, with admission open.
/// Guarantees: the node, which learns of the shutdown only from the Shutdown the inbox releases
/// once the channel closes, admits all three, decides none while the sender is alive, writes and
/// acknowledges them after the release, and `series_parquet.shutdown.complete` reports 3
/// admitted, 3 acked.
#[tokio::test(flavor = "current_thread")]
async fn requests_drained_behind_a_latched_shutdown_are_written() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<OtapPdata>>::new(8);
            let (pdata_tx, pdata_rx) = mpsc::Channel::<OtapPdata>::new(8);
            let inbox = ExporterInbox::new(
                Receiver::Local(LocalReceiver::mpsc(control_rx)),
                Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
                0,
                Interests::empty(),
            );
            let (handler, mut rx) = effects(8);

            // The engine latches this and releases it only once the upstream
            // pdata channel is both empty and closed, so the two requests
            // below are force-drained before the node ever sees it.
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            for _ in 0..2 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("a buffered request enqueues");
            }

            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let config = worker_config();
            let node = tokio::task::spawn_local(super::super::run(
                config.clone(),
                Arc::new(object_store::memory::InMemory::new()),
                Arc::new(lake::clock::TestWallClock::new(0)),
                inbox,
                handler,
                Some(super::super::metrics::Metrics::register(
                    &context,
                    &config.lake,
                )),
            ));

            // The node is not told of the latched Shutdown, so this request
            // is admitted like the two before it.
            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("a late request enqueues");
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            assert_no_more_completions(&mut rx);

            // Closing the upstream pdata channel releases the latched
            // shutdown; the control sender stays alive, as in the engine.
            drop(pdata_tx);
            for _ in 0..3 {
                assert_eq!(expect_ack(&mut rx).await, Some(7));
            }
            let terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns once the upstream channel closes")
                .expect("the node task joins");
            let terminal = match terminal {
                Ok(terminal) => terminal,
                Err(error) => panic!("unexpected node failure: {error}"),
            };
            // The counters of the interval the node ends must reach the
            // collector with it: nothing else will ever report them.
            let snapshots = terminal.metrics();
            assert_eq!(
                terminal_value(snapshots, "nacks", &[("error.type", "shutdown")]),
                0,
                "no drained request is refused"
            );
            assert_eq!(terminal_value(snapshots, "acks", &[]), 3);
            let summary = events.named("series_parquet.shutdown.complete");
            assert_eq!(summary.len(), 1, "{summary:?}");
            let field = |name: &str| summary[0].fields.get(name).cloned();
            assert_eq!(field("accepted"), Some(FieldValue::U64(3)));
            assert_eq!(field("acked"), Some(FieldValue::U64(3)));
            assert_eq!(field("nacked"), Some(FieldValue::U64(0)));
            assert_eq!(field("abandoned"), Some(FieldValue::U64(0)));
            assert_eq!(field("deadline_exceeded"), Some(FieldValue::Bool(false)));
            drop(control_tx);
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: the shutdown deadline elapses while a flush is outstanding.
/// Guarantees: the write is cancelled and every held completion gets a retryable `NodeShutdown`
/// nack.
#[tokio::test(flavor = "current_thread")]
async fn the_deadline_decides_every_outstanding_request() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store, wall, handler);

            worker.admit(logs_pdata());
            worker.rotate();
            let elapsed = clock::now()
                .checked_sub(Duration::from_secs(1))
                .expect("an instant one second in the past");
            worker.shutdown(elapsed);
            let _ = worker.abandon().await;

            assert!(worker.is_idle());
            match rx.recv().await.expect("a shutdown refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(nack.cause, NackCause::NodeShutdown);
                    assert_eq!(nack.reason, Outcome::Shutdown.sentence());
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: the deadline elapses while a multipart upload is wedged and its abort never returns.
/// Guarantees: every request is decided before any wait, and the node returns once the task is
/// released within `upload.abort_timeout`.
#[tokio::test(flavor = "current_thread")]
async fn the_deadline_returns_only_once_the_flush_task_is_released() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::MultipartWedge);
            let (handler, mut rx) = effects(8);
            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_secs(1);
            cfg.lake.upload.part_bytes = 4096;
            cfg.lake.upload.concurrency = 1;
            cfg.lake.parquet.row_group_bytes = 4096;
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);

            worker.admit(bulk_logs_pdata(20_000));
            worker.rotate();
            until("the wedged upload takes a part", || {
                store.hooks().parts.load(SeqCst) > 0
            })
            .await;
            // The decision is due now: the cut is the completion margin
            // before the deadline.
            worker.shutdown(clock::now() + SHUTDOWN_COMPLETION_MARGIN);

            let cancel = worker
                .flushing
                .as_ref()
                .expect("a rotated block is flushing")
                .cancel
                .clone();
            assert!(
                !cancel.is_cancelled(),
                "nothing is cancelled before the decision is taken"
            );

            {
                let mut abandoning = std::pin::pin!(worker.abandon());
                assert!(
                    futures::poll!(&mut abandoning).is_pending(),
                    "a wedged upload is not released in one turn"
                );
                // No runtime turn has elapsed since that poll, so both of the
                // next two assertions describe the state at the moment the
                // decision phase suspended. The completion is already in the
                // channel, and the write has not yet been able to observe the
                // cancellation, whose store-visible effect is the abort.
                assert_eq!(
                    store.hooks().aborts.load(SeqCst),
                    0,
                    "the write is cancelled only after the decision is delivered"
                );
                let delivered = futures::poll!(std::pin::pin!(rx.recv()));
                match delivered {
                    std::task::Poll::Ready(Ok(PipelineCompletionMsg::DeliverNack { nack })) => {
                        assert_eq!(nack.cause, NackCause::NodeShutdown);
                    }
                    other => panic!("expected a delivered nack, got {other:?}"),
                }
                // The cancellation belongs to the same turn as the delivery it
                // follows, so it is never deferred past the bounded wait.
                assert!(
                    cancel.is_cancelled(),
                    "the write is cancelled in the turn that decided it"
                );
                for _ in 0..64 {
                    tokio::task::yield_now().await;
                    assert!(
                        futures::poll!(&mut abandoning).is_pending(),
                        "the abort never returns, so the slot is not released"
                    );
                }
                assert!(
                    store.hooks().aborts.load(SeqCst) > 0,
                    "the abandoned upload is aborted rather than dropped"
                );

                sim.advance(Duration::from_secs(2));
                let _ = (&mut abandoning).await;
            }
            assert!(
                worker.is_idle(),
                "both slot holders are gone once the deadline returns"
            );
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: buffered pdata is force-drained with admission closed after a Shutdown latches.
/// Guarantees: the request is handed over, and refused it gets a retryable `NodeShutdown` nack,
/// not a delivery failure.
#[tokio::test(flavor = "current_thread")]
async fn forced_pdata_is_retryably_nacked() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (pdata_tx, control_tx, mut inbox) = inbox(2);
            let deadline = clock::now() + Duration::from_secs(1);
            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("the request enqueues");
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline,
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");

            // Admission is closed, yet the buffered request is still handed
            // over: that is the force-drain the exporter has to decide.
            let data = match inbox.recv_when(false).await.expect("a forced request") {
                Message::PData(data) => data,
                other => panic!("expected forced pdata, got {other:?}"),
            };

            let (handler, mut rx) = effects(1);
            let mut notify = Notifier::new(handler, 2);
            notify.force_shutdown(data);
            assert_eq!(
                notify.failures(),
                0,
                "a free completion slot takes the refusal immediately"
            );
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(nack.cause, NackCause::NodeShutdown);
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            drop(pdata_tx);
            drop(control_tx);
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: the deadline elapses with a parked write, a populated ACTIVE block and a parked
/// request.
/// Guarantees: the parked request is nacked at the latch, both blocks at the deadline; nothing is
/// left.
#[tokio::test(flavor = "current_thread")]
async fn deadline_nacks_both_blocks_and_pending() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, mut rx) = effects(16);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let mut worker = Worker::new(cfg, store.clone(), wall.clone(), handler);

            // FLUSHING: one request, rotated into a write that never returns.
            worker.admit(logs_pdata());
            worker.rotate();
            store.hooks().entered.notified().await;
            // ACTIVE: one request in the block that is still open.
            worker.admit(logs_pdata());
            // Parked: a request whose admission window is later than the one
            // the ACTIVE block was opened for, and no block can be opened for
            // it while the flush slot is held.
            wall.set(1_000_000_000);
            worker.admit(logs_pdata());
            assert!(worker.pending.is_some());
            assert_eq!(worker.active.tokens.len(), 1);
            assert_eq!(
                worker
                    .flushing
                    .as_ref()
                    .expect("a rotated block is flushing")
                    .tokens
                    .len(),
                1
            );

            worker.shutdown(clock::now());
            assert!(
                worker.pending.is_none(),
                "the parked request is decided when shutdown is latched"
            );
            assert_eq!(worker.notify.len(), 1);

            let _ticker = ticking(&sim, Duration::from_millis(50));
            let _ = worker.abandon().await;

            for _ in 0..3 {
                match rx.recv().await.expect("a shutdown refusal") {
                    PipelineCompletionMsg::DeliverNack { nack } => {
                        assert!(!nack.permanent);
                        assert_eq!(nack.cause, NackCause::NodeShutdown);
                    }
                    other => panic!("expected a nack, got {other:?}"),
                }
            }
            assert!(worker.flushing.is_none());
            assert!(worker.cleaning.is_none());
            assert!(worker.is_idle(), "nothing is left undecided");
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: shutdown latches with one block flushing and one open, and storage recovers early.
/// Guarantees: both blocks are written and acknowledged; nothing is nacked.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_commits_both_blocks_before_deadline() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, mut rx) = effects(8);
            let (pdata_tx, control_tx, inbox) = inbox(8);
            let cfg = worker_config();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let metrics = super::super::metrics::Metrics::register(&context, &cfg.lake);
            let node = tokio::task::spawn_local(super::super::run(
                cfg,
                store.clone(),
                Arc::new(lake::clock::TestWallClock::new(0)),
                inbox,
                handler,
                Some(metrics),
            ));

            // Four requests fill a block, so the node rotates it immediately
            // and its write parks.
            for _ in 0..4 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("a request of the first block enqueues");
            }
            store.hooks().entered.notified().await;
            // The fifth joins the block that replaced it; the flush slot is
            // busy, so that block is still ACTIVE when shutdown arrives.
            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("the request of the second block enqueues");
            // The inbox serves control before pdata, so the shutdown below
            // would force-drain the fifth request; the marker behind it in the
            // same channel proves by its completion that the fifth is in a
            // block.
            marker(&pdata_tx, &mut rx, 99).await;

            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            // Closing the upstream pdata channel releases the latched
            // Shutdown, as in the engine.
            drop(pdata_tx);
            // Storage heals, so both blocks can reach object storage inside
            // the deadline.
            store.hooks().set(Fault::None);
            store.hooks().release.notify_waiters();

            for _ in 0..5 {
                assert!(
                    matches!(
                        tokio::time::timeout(Duration::from_secs(5), rx.recv())
                            .await
                            .expect("a completion arrives")
                            .expect("a completion arrives"),
                        PipelineCompletionMsg::DeliverAck { .. }
                    ),
                    "a block that reached storage before the deadline is acknowledged"
                );
            }
            let terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("the node task joins")
                .expect("the node succeeds");
            let snapshots = terminal.metrics();
            // Five requests in two blocks, plus the row-less marker.
            assert_eq!(terminal_value(snapshots, "acks", &[]), 6);
            assert_eq!(
                terminal_value(snapshots, "nacks", &[("error.type", "shutdown")]),
                0
            );
            // One values file per block: the block shutdown found flushing and
            // the block it then rotated and flushed itself.
            let written: Vec<String> = store
                .hooks()
                .writes
                .lock()
                .expect("writes lock")
                .iter()
                .map(|(path, _)| path.clone())
                .filter(|path| path.contains("dataset=values/"))
                .collect();
            assert_eq!(written.len(), 2, "both blocks reached storage: {written:?}");
            drop(control_tx);
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// A node whose first block (requests 1 to 4) is FLUSHING against `store`'s
/// fault and whose ACTIVE block holds request 5, run for `before` and then
/// sent a Shutdown granting `grace`, with the upstream sender dropped so the
/// engine releases it. Returns the node, its completions and the deadline.
async fn shutdown_while_both_blocks_wait(
    store: &Arc<FaultStore>,
    sim: &clock::SimClock,
    before: Duration,
    grace: Duration,
) -> (
    tokio::task::JoinHandle<
        Result<
            otel_arrow_dfe_engine::terminal_state::TerminalState,
            otel_arrow_dfe_engine::error::Error,
        >,
    >,
    PipelineCompletionMsgReceiver<OtapPdata>,
    mpsc::Sender<NodeControlMsg<OtapPdata>>,
    std::time::Instant,
) {
    let (handler, mut rx) = effects(16);
    let (pdata_tx, control_tx, inbox) = inbox(8);
    let node = tokio::task::spawn_local(super::super::run(
        worker_config(),
        store.clone(),
        Arc::new(lake::clock::TestWallClock::new(0)),
        inbox,
        handler,
        None,
    ));
    // Four requests fill a block, so the node rotates it at once.
    for id in 1..=4 {
        pdata_tx
            .send_async(logs_pdata_from(id))
            .await
            .expect("a request of the first block enqueues");
    }
    until("the first block's write reaches the store", || {
        !store
            .hooks()
            .entered_at
            .lock()
            .expect("entered_at lock")
            .is_empty()
    })
    .await;
    pdata_tx
        .send_async(logs_pdata_from(5))
        .await
        .expect("the request of the second block enqueues");
    marker(&pdata_tx, &mut rx, 99).await;
    step_for(sim, before).await;
    let deadline = clock::now() + grace;
    control_tx
        .send_async(NodeControlMsg::Shutdown {
            deadline,
            reason: "terminate".to_owned(),
        })
        .await
        .expect("the shutdown enqueues");
    drop(pdata_tx);
    (node, rx, control_tx, deadline)
}

/// Step the simulated clock until five completions have arrived and the node
/// has returned, or `until` has passed; each completion with the instant it
/// was received.
async fn completions_until(
    sim: &clock::SimClock,
    rx: &mut PipelineCompletionMsgReceiver<OtapPdata>,
    node: &tokio::task::JoinHandle<
        Result<
            otel_arrow_dfe_engine::terminal_state::TerminalState,
            otel_arrow_dfe_engine::error::Error,
        >,
    >,
    until: std::time::Instant,
) -> Vec<(std::time::Instant, PipelineCompletionMsg<OtapPdata>)> {
    let mut got = Vec::new();
    while clock::now() < until && !(got.len() == 5 && node.is_finished()) {
        step_for(sim, Duration::from_millis(100)).await;
        while let Ok(message) = rx.try_recv() {
            got.push((clock::now(), message));
        }
    }
    got
}

/// Scenario: the store fails for 23 s (backoff at 10 s) with a second block ACTIVE; a terminate
/// grants 60 s and the store heals 32 s into it.
/// Guarantees: the backoff drops to its minimum, both blocks are written, and all five requests are
/// acknowledged before the deadline.
#[tokio::test(flavor = "current_thread")]
async fn a_store_that_heals_inside_the_grace_commits_both_blocks() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Series);
            let (node, mut rx, control_tx, deadline) = shutdown_while_both_blocks_wait(
                &store,
                &sim,
                Duration::from_secs(23),
                Duration::from_secs(60),
            )
            .await;
            // A ten-second backoff would next retry at about 52.6 s and then
            // hit the block's own 60 s deadline; this is between the two.
            step_for(&sim, Duration::from_secs(32)).await;
            store.hooks().set(Fault::None);

            let got = completions_until(&sim, &mut rx, &node, deadline).await;
            assert_eq!(
                acked_ids(got),
                vec![1, 2, 3, 4, 5],
                "both blocks are acknowledged"
            );
            assert_no_more_completions(&mut rx);
            assert!(clock::now() < deadline, "the drain ends inside the grace");
            let _ = node
                .await
                .expect("the node task joins")
                .expect("the node succeeds");
            drop(control_tx);
        })
        .await;
}

/// Scenario: the same two blocks; the store never heals and the 30 s deadline comes first.
/// Guarantees: every request is nacked `NodeShutdown` at the completion margin before the
/// deadline, and no write starts at or after that cut.
#[tokio::test(flavor = "current_thread")]
async fn a_store_that_never_heals_is_nacked_retryable_at_the_deadline() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Series);
            let (node, mut rx, control_tx, deadline) = shutdown_while_both_blocks_wait(
                &store,
                &sim,
                Duration::from_secs(23),
                Duration::from_secs(30),
            )
            .await;

            let got =
                completions_until(&sim, &mut rx, &node, deadline + Duration::from_secs(10)).await;
            assert_eq!(got.len(), 5, "every request is decided");
            let cut = deadline - SHUTDOWN_COMPLETION_MARGIN;
            for (at, message) in got {
                assert!(at >= cut, "nacked at the cut, not before it");
                assert!(at < deadline, "nacked before the deadline");
                match message {
                    PipelineCompletionMsg::DeliverNack { nack } => {
                        assert!(!nack.permanent);
                        assert_eq!(nack.cause, NackCause::NodeShutdown);
                    }
                    other => panic!("expected a nack, got {other:?}"),
                }
            }
            let entered = store
                .hooks()
                .entered_at
                .lock()
                .expect("entered_at lock")
                .clone();
            assert!(
                entered.len() > 10,
                "the block was retried until the deadline"
            );
            assert!(
                entered.iter().all(|at| *at < cut),
                "no attempt starts at or after the cut"
            );
            let _ = node
                .await
                .expect("the node task joins")
                .expect("the node succeeds");
            assert_no_more_completions(&mut rx);
            drop(control_tx);
        })
        .await;
}

/// The sorted ids of `got`, all of which must be acks.
fn acked_ids(got: Vec<(std::time::Instant, PipelineCompletionMsg<OtapPdata>)>) -> Vec<usize> {
    let mut ids: Vec<usize> = got
        .into_iter()
        .map(|(_, message)| match message {
            PipelineCompletionMsg::DeliverAck { ack } => ack
                .accepted
                .into_parts()
                .0
                .source_node()
                .expect("a routed request"),
            other => panic!("expected an ack, got {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// Scenario: every write fails fast; a terminate grants 30 s and the store heals 300 ms before the
/// cut the completion margin leaves before the deadline.
/// Guarantees: attempts keep starting until the cut, and all five requests are acknowledged.
#[tokio::test(flavor = "current_thread")]
async fn a_store_that_heals_just_before_the_deadline_still_commits() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Series);
            let (node, mut rx, control_tx, deadline) = shutdown_while_both_blocks_wait(
                &store,
                &sim,
                Duration::from_secs(1),
                Duration::from_secs(30),
            )
            .await;
            // 300 ms before the cut the completion margin leaves.
            step_for(
                &sim,
                Duration::from_secs(30) - SHUTDOWN_COMPLETION_MARGIN - Duration::from_millis(300),
            )
            .await;
            let cut = deadline - SHUTDOWN_COMPLETION_MARGIN;
            assert!(clock::now() + Duration::from_millis(300) <= cut);
            store.hooks().set(Fault::None);

            let got = completions_until(&sim, &mut rx, &node, deadline).await;
            assert!(
                got.iter().all(|(at, _)| *at <= cut),
                "decided by the cut before the deadline"
            );
            assert_eq!(acked_ids(got), vec![1, 2, 3, 4, 5]);
            let _ = node
                .await
                .expect("the node task joins")
                .expect("the node succeeds");
            assert_no_more_completions(&mut rx);
            drop(control_tx);
        })
        .await;
}

/// Scenario: a 30 s terminate latches during a 20 s failing attempt; the store heals meanwhile.
/// Guarantees: the retry and the ACTIVE block's attempt both start before the deadline and commit.
#[tokio::test(flavor = "current_thread")]
async fn a_slow_failure_does_not_stop_the_attempts_after_it() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::SlowFail);
            let (node, mut rx, control_tx, deadline) = shutdown_while_both_blocks_wait(
                &store,
                &sim,
                Duration::from_secs(1),
                Duration::from_secs(30),
            )
            .await;
            // The first attempt read its fault on entry, so it still fails.
            step_for(&sim, Duration::from_secs(9)).await;
            store.hooks().set(Fault::None);

            let got = completions_until(&sim, &mut rx, &node, deadline).await;
            assert_eq!(acked_ids(got), vec![1, 2, 3, 4, 5]);
            let entered = store
                .hooks()
                .entered_at
                .lock()
                .expect("entered_at lock")
                .clone();
            assert!(
                entered.iter().skip(1).all(|at| *at < deadline),
                "the later attempts started before the deadline"
            );
            let _ = node
                .await
                .expect("the node task joins")
                .expect("the node succeeds");
            assert_no_more_completions(&mut rx);
            drop(control_tx);
        })
        .await;
}

/// A worker whose one block is FLUSHING in a values multipart upload that
/// wedges, parts and abort alike, with a 10 s `upload.abort_timeout`, a 10 s
/// shutdown deadline latched, and the engine clock then moved 5 s past that
/// deadline before the flush task has run. Returns the worker, its
/// completions, and the absolute cutoff: the deadline plus the abort timeout.
async fn deadline_observed_late(
    sim: &clock::SimClock,
) -> (
    Worker,
    PipelineCompletionMsgReceiver<OtapPdata>,
    std::time::Instant,
) {
    let store = fault_store();
    store.hooks().set(Fault::MultipartWedge);
    let (handler, rx) = effects(8);
    let mut cfg = worker_config();
    cfg.lake.upload.abort_timeout = Duration::from_secs(10);
    cfg.lake.upload.part_bytes = 4096;
    cfg.lake.upload.concurrency = 1;
    cfg.lake.parquet.row_group_bytes = 4096;
    // Small chunks keep the writer blocked on a wedged part, in the phase
    // where a cancelled upload is aborted, when the deadline expires.
    cfg.lake.sorting.merge_chunk_bytes = 4096;
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut worker = Worker::new(cfg, store.clone(), wall, handler);
    worker.admit(bulk_logs_pdata(20_000));
    worker.rotate();
    until("the wedged upload takes a part", || {
        store.hooks().parts.load(SeqCst) > 0
    })
    .await;
    let deadline = clock::now() + Duration::from_secs(10);
    worker.shutdown(deadline);
    sim.advance(Duration::from_secs(15));
    (
        worker,
        rx,
        deadline - SHUTDOWN_COMPLETION_MARGIN + Duration::from_secs(10),
    )
}

/// Whether `future` is still pending after the runtime has had several turns.
async fn still_pending<F: Future + Unpin>(future: &mut F) -> bool {
    for _ in 0..32 {
        if futures::poll!(&mut *future).is_ready() {
            return false;
        }
        tokio::task::yield_now().await;
    }
    true
}

/// Scenario: the flush task sees its expiry 5 s late while the cancelled upload never aborts.
/// Guarantees: the write is released at the cut before the latched deadline plus `upload.abort_timeout`.
#[tokio::test(flavor = "current_thread")]
async fn a_late_expiry_is_cleaned_up_by_the_absolute_cutoff() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (mut worker, mut rx, cutoff) = deadline_observed_late(&sim).await;
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            worker.complete(done);
            worker.notify.next().await.expect("the nack is sent");
            expect_shutdown_nack(&mut rx).await;

            let mut job = worker.cleaning.take().expect("the cleanup slot");
            {
                let mut cleanup = std::pin::pin!(job.cleanup());
                sim.advance_to(cutoff - Duration::from_millis(100));
                assert!(
                    still_pending(&mut cleanup).await,
                    "the wedged abort holds the task until the cutoff"
                );
                sim.advance_to(cutoff);
                assert!(
                    !still_pending(&mut cleanup).await,
                    "the task is released at the absolute cutoff"
                );
            }
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: the node's deadline branch runs 5 s late, before the task sees its expiry, and the
/// upload never aborts.
/// Guarantees: `abandon` returns by the cut before the latched deadline plus `upload.abort_timeout`.
#[tokio::test(flavor = "current_thread")]
async fn a_late_deadline_branch_returns_by_the_absolute_cutoff() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (mut worker, mut rx, cutoff) = deadline_observed_late(&sim).await;
            {
                let mut abandoning = std::pin::pin!(worker.abandon());
                assert!(still_pending(&mut abandoning).await);
                expect_shutdown_nack(&mut rx).await;
                sim.advance_to(cutoff - Duration::from_millis(100));
                assert!(
                    still_pending(&mut abandoning).await,
                    "the wedged abort holds the node until the cutoff"
                );
                sim.advance_to(cutoff);
                assert!(
                    !still_pending(&mut abandoning).await,
                    "the node returns at the absolute cutoff"
                );
            }
            assert!(worker.is_idle());
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Requests per block in [`abandon_decides_a_full_worker_well_inside_the_floor`].
const HELD_PER_BLOCK: usize = 512;

/// Scenario: a full worker (512 requests flushing, 511 ACTIVE) at the deadline with a 1 s
/// `upload.abort_timeout`.
/// Guarantees: all 1023 are decided before the first await, well inside the 1 s floor.
#[tokio::test(flavor = "current_thread")]
async fn abandon_decides_a_full_worker_well_inside_the_floor() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, mut rx) = effects(2 * HELD_PER_BLOCK);
            let mut cfg = worker_config_with_requests(HELD_PER_BLOCK);
            cfg.lake.upload.abort_timeout = Duration::from_secs(1);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            for id in 1..=HELD_PER_BLOCK {
                worker.admit(logs_pdata_from(id));
            }
            worker.rotate();
            store.hooks().entered.notified().await;
            let mut id = HELD_PER_BLOCK;
            while worker.accept() {
                id += 1;
                worker.admit(logs_pdata_from(id));
            }
            let held = worker.live_tokens();
            assert_eq!(held, 2 * HELD_PER_BLOCK - 1, "the notifier's full credit");

            worker.shutdown(clock::now());
            {
                let mut abandoning = std::pin::pin!(worker.abandon());
                let started = std::time::Instant::now();
                let first = futures::poll!(&mut abandoning);
                let spent = started.elapsed();
                assert!(
                    first.is_pending(),
                    "the cleanup is awaited after the decision"
                );
                assert!(
                    spent < Duration::from_millis(250),
                    "deciding {held} requests took {spent:?}"
                );
                // Every decision is in the channel before the first await.
                for _ in 0..held {
                    match rx.try_recv() {
                        Ok(PipelineCompletionMsg::DeliverNack { nack }) => {
                            assert_eq!(nack.cause, NackCause::NodeShutdown);
                        }
                        other => panic!("expected a delivered nack, got {other:?}"),
                    }
                }
                let _ = abandoning.await;
            }
            assert_eq!(
                worker.notify.outcomes()[Outcome::Shutdown as usize],
                held as u64
            );
            assert_eq!(worker.notify.failures(), 0);
            assert_no_more_completions(&mut rx);
            assert!(worker.is_idle());
        })
        .await;
}

/// Scenario: a 60 s deadline latches during a parked write, the pdata sender drops during a control
/// message, and the store heals after a second.
/// Guarantees: the released Shutdown keeps its deadline and the block is acknowledged.
#[tokio::test(flavor = "current_thread")]
async fn a_closed_pdata_channel_keeps_the_latched_shutdown_deadline() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, mut rx) = effects(8);
            let (pdata_tx, control_tx, inbox) = inbox(8);
            let node = tokio::task::spawn_local(super::super::run(
                worker_config(),
                store.clone(),
                Arc::new(lake::clock::TestWallClock::new(0)),
                inbox,
                handler,
                None,
            ));

            // Four requests fill a block, so the node rotates it and its write
            // parks.
            for _ in 0..4 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("a request of the block enqueues");
            }
            store.hooks().entered.notified().await;

            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(60),
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            // Force-drained behind the latched Shutdown, which is how the node
            // learns the deadline and closes its admission.
            marker(&pdata_tx, &mut rx, 99).await;

            // The control message ends the node's current receive, so its next
            // receive starts on a pdata channel that is already closed.
            control_tx
                .send_async(NodeControlMsg::Config {
                    config: serde_json::json!({}),
                })
                .await
                .expect("the config enqueues");
            drop(pdata_tx);
            until("the node is handed the Shutdown", || {
                !events.named("series_parquet.shutdown").is_empty()
            })
            .await;
            let shutdown = events.named("series_parquet.shutdown");
            assert_eq!(
                shutdown[0].fields.get("reason").map(FieldValue::text),
                Some("test"),
                "the node is handed the latched Shutdown"
            );

            sim.advance(Duration::from_secs(2));
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            store.hooks().set(Fault::None);
            store.hooks().release.notify_waiters();

            for _ in 0..4 {
                assert!(
                    matches!(
                        tokio::time::timeout(Duration::from_secs(5), rx.recv())
                            .await
                            .expect("a completion arrives")
                            .expect("a completion arrives"),
                        PipelineCompletionMsg::DeliverAck { .. }
                    ),
                    "a block flushed inside the latched deadline is acknowledged"
                );
            }
            let _ = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("the node task joins")
                .expect("the node succeeds");
            drop(control_tx);
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: the `SeriesParquet::start` future is aborted while a write is parked forever.
/// Guarantees: the write, block, sink and store are released within the abort timeout.
#[tokio::test(flavor = "current_thread")]
async fn dropping_start_cancels_flush_task() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, _rx) = effects(2);
            let (pdata_tx, control_tx, inbox) = inbox(2);
            let mut cfg = startable_config(1);
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let mut exporter = super::super::SeriesParquet::new(cfg);
            exporter.store_override = Some(store.clone());
            let node = tokio::task::spawn_local(Box::new(exporter).start(inbox, handler));

            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("the request enqueues");
            store.hooks().entered.notified().await;
            assert_eq!(
                store.hooks().parked.load(SeqCst),
                1,
                "the write is parked inside the store"
            );

            node.abort();
            match node.await {
                Err(error) => assert!(error.is_cancelled()),
                Ok(_) => panic!("start was not aborted"),
            }
            until("the cancelled node releases the store", || {
                Arc::strong_count(&store) == 1 && store.hooks().parked.load(SeqCst) == 0
            })
            .await;
            assert_eq!(
                store.hooks().parked_drops.load(SeqCst),
                1,
                "the parked write future was dropped rather than leaked"
            );
            drop(pdata_tx);
            drop(control_tx);
        })
        .await;
}

/// Scenario: the completion channel is closed and five requests, one block each, are nacked by a
/// store refusing every write, all at one instant.
/// Guarantees: the node loop writes one `series_parquet.notify.failed` WARN for the burst, and
/// `notify.failures` still counts all five.
#[tokio::test(flavor = "current_thread")]
async fn undeliverable_completions_are_logged_once_per_interval() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (pdata_tx, control_tx, inbox) = inbox(8);
            let (handler, completion_rx) = effects(8);
            drop(completion_rx);
            let store = fault_store();
            store.hooks().set(Fault::Denied);
            let cfg = worker_config_with_requests(1);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let metrics = super::super::metrics::Metrics::register(&context, &cfg.lake);
            let node = tokio::task::spawn_local(super::super::run(
                cfg,
                store,
                Arc::new(lake::clock::TestWallClock::new(0)),
                inbox,
                handler,
                Some(metrics),
            ));
            for _ in 0..5 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("a request enqueues");
            }
            // Every block fails at once; its nack then fails to deliver.
            for _ in 0..100_000 {
                if events.named("series_parquet.flush.failed").len() == 5 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            for _ in 0..1_000 {
                tokio::task::yield_now().await;
            }
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            drop(pdata_tx);
            let terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("the node task joins")
                .expect("the node succeeds");
            assert_eq!(
                terminal_value(terminal.metrics(), "notify.failures", &[]),
                5
            );
            drop(control_tx);
        })
        .await;
    let failed = events.named("series_parquet.notify.failed");
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(failed[0].level, tracing::Level::WARN);
}

/// Scenario: a buffered backlog is force-drained into a full completion channel nobody reads.
/// Guarantees: one decision per request, undeliverable ones counted, and the node returns at the
/// deadline.
#[tokio::test(flavor = "current_thread")]
async fn saturated_inbox_shutdown_stays_bounded() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (pdata_tx, control_tx, inbox) = inbox(32);
            let (handler, _completion_rx) = effects(1);
            // One delivered completion is enough to fill the channel, so every
            // decision the node takes from here on cannot be handed over.
            let mut prime = Notifier::new(handler.clone(), 1);
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            prime.push(token, Outcome::Ack);
            prime.next().await.expect("the completion channel fills");

            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let metrics = super::super::metrics::Metrics::register(&context, &cfg.lake);
            let store = fault_store();
            store.hooks().set(Fault::Park);

            for _ in 0..32 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("the inbox fills");
            }
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "saturated".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            drop(pdata_tx);

            let _ticker = ticking(&sim, Duration::from_secs(1));
            let terminal = tokio::time::timeout(
                Duration::from_secs(2),
                super::super::run(
                    cfg,
                    store,
                    Arc::new(lake::clock::TestWallClock::new(0)),
                    inbox,
                    handler,
                    Some(metrics),
                ),
            )
            .await
            .expect("shutdown stays bounded with a full completion channel")
            .expect("the node succeeds");

            let snapshots = terminal.metrics();
            assert_eq!(
                terminal_value(snapshots, "nacks", &[("error.type", "shutdown")]),
                32,
                "every force-drained request is decided exactly once"
            );
            assert_eq!(
                terminal_value(snapshots, "notify.failures", &[]),
                32,
                "every undeliverable decision is counted rather than dropped silently"
            );
            drop(control_tx);
        })
        .await;
}

/// A worker whose one request (id 1) is FLUSHING in a parked write, with
/// shutdown latched and three requests (ids 2 to 4) force-drained into a
/// completion channel that has room for one message and is never read.
///
/// One request per block makes the notifier's capacity two, so the flushing
/// block's completion and two force-drained refusals are more than it holds.
async fn force_drained_past_a_held_block(
    store: &Arc<FaultStore>,
) -> (Worker, PipelineCompletionMsgReceiver<OtapPdata>) {
    store.hooks().set(Fault::Park);
    let (handler, rx) = effects(1);
    let mut cfg = worker_config_with_requests(1);
    cfg.lake.upload.abort_timeout = Duration::from_millis(100);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut worker = Worker::new(cfg, store.clone(), wall, handler);
    worker.admit(logs_pdata_from(1));
    worker.rotate();
    store.hooks().entered.notified().await;
    worker.shutdown(clock::now() + Duration::from_secs(30));
    for id in 2..=4 {
        worker.force_shutdown(logs_pdata_from(id));
    }
    (worker, rx)
}

/// Take `n` completions, sending the notifier's next one after each, and
/// return the ids acknowledged and the ids refused with a retryable
/// `NodeShutdown` nack, each sorted.
async fn completions_while_sending(
    worker: &mut Worker,
    rx: &mut PipelineCompletionMsgReceiver<OtapPdata>,
    n: usize,
) -> (Vec<usize>, Vec<usize>) {
    let (mut acked, mut nacked) = (Vec::new(), Vec::new());
    for _ in 0..n {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a completion arrives")
            .expect("a completion arrives")
        {
            PipelineCompletionMsg::DeliverAck { ack } => {
                acked.push(ack.accepted.into_parts().0.source_node().expect("an id"));
            }
            PipelineCompletionMsg::DeliverNack { nack } => {
                assert!(!nack.permanent);
                assert_eq!(nack.cause, NackCause::NodeShutdown);
                nacked.push((*nack.refused).into_parts().0.source_node().expect("an id"));
            }
        }
        if !worker.notify.is_empty() {
            worker
                .notify
                .next()
                .await
                .expect("the next completion is sent");
        }
    }
    acked.sort_unstable();
    nacked.sort_unstable();
    (acked, nacked)
}

/// Scenario: one request flushing in a parked write, three force-drained into a completion
/// channel of one that is full after the first, then the write released and the channel read.
/// Guarantees: no completion is destroyed while the channel may still gain room: request 1 is
/// acknowledged, requests 2 to 4 each get a retryable `NodeShutdown` nack, and nothing is
/// counted as a delivery failure.
#[tokio::test(flavor = "current_thread")]
async fn force_drain_leaves_credit_for_a_block_whose_write_is_released() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = fault_store();
            let (mut worker, mut rx) = force_drained_past_a_held_block(&store).await;
            assert_eq!(
                worker.notify.failures(),
                0,
                "a refusal past the bound waits for room instead of being dropped"
            );

            store.hooks().set(Fault::None);
            store.hooks().release.notify_waiters();
            let done = worker
                .flushing
                .as_mut()
                .expect("the block is flushing")
                .finish()
                .await;
            worker.complete(done);
            drain_cleanup(&mut worker).await;

            let (acked, nacked) = completions_while_sending(&mut worker, &mut rx, 4).await;
            assert_eq!(acked, [1]);
            assert_eq!(nacked, [2, 3, 4]);
            let outcomes = worker.notify.outcomes();
            assert_eq!(outcomes[Outcome::Shutdown as usize], 3);
            assert_eq!(outcomes[Outcome::Ack as usize], 1);
            assert_eq!(outcomes.iter().sum::<u64>(), 4);
            assert_eq!(worker.notify.failures(), 0);
            assert!(worker.is_idle());
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: the same setup, but the deadline fires while the write is parked.
/// Guarantees: no panic, undeliverable completions counted, each request decided once.
#[tokio::test(flavor = "current_thread")]
async fn force_drain_leaves_credit_for_a_block_the_deadline_decides() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            let (mut worker, mut rx) = force_drained_past_a_held_block(&store).await;

            let _ticker = ticking(&sim, Duration::from_millis(50));
            let _ = worker.abandon().await;

            expect_shutdown_nack(&mut rx).await;
            let outcomes = worker.notify.outcomes();
            assert_eq!(outcomes[Outcome::Shutdown as usize], 4);
            assert_eq!(outcomes.iter().sum::<u64>(), 4);
            assert_eq!(
                worker.notify.failures(),
                3,
                "one delivered, three counted: every decision is accounted for"
            );
            assert!(worker.is_idle());
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: boundaries arrive while a never-returning write holds the slot and the completion
/// channel is full; then telemetry and a shutdown.
/// Guarantees: the boundary timer is re-armed, control is served, and the shutdown is observed.
#[tokio::test(flavor = "current_thread")]
async fn blocked_completion_keeps_boundary_and_control_live() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (pdata_tx, control_tx, inbox) = inbox(4);
            let (handler, _completion_rx) = effects(1);
            let mut prime = Notifier::new(handler.clone(), 1);
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            prime.push(token, Outcome::Ack);
            prime.next().await.expect("the completion channel fills");

            let store = fault_store();
            store.hooks().set(Fault::Park);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let metrics = super::super::metrics::Metrics::register(&context, &cfg.lake);
            let node = tokio::task::spawn_local(super::super::run(
                cfg,
                store.clone(),
                wall.clone(),
                inbox,
                handler,
                Some(metrics),
            ));

            // Boundaries keep arriving for the rest of the test, so the
            // request reaches a sealed block whichever turn the node admits it
            // on, and every later boundary is one the node meets with its
            // flush slot already held.
            let _ticker = ticking_windows(&sim, &wall);
            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("the request enqueues");
            store.hooks().entered.notified().await;

            let (samples, reporter) =
                otel_arrow_dfe_telemetry::reporter::MetricsReporter::create_new_and_receiver(64);
            control_tx
                .send_async(NodeControlMsg::CollectTelemetry {
                    metrics_reporter: reporter,
                })
                .await
                .expect("the telemetry control enqueues");
            until(
                "telemetry is served while everything else is blocked",
                || samples.try_recv().is_ok(),
            )
            .await;

            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(1),
                    reason: "blocked completions".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            drop(pdata_tx);
            let terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("the node task joins")
                .expect("the node succeeds");
            assert_eq!(
                terminal_value(terminal.metrics(), "notify.failures", &[]),
                1,
                "the undeliverable decision is counted, not retried forever"
            );
            drop(control_tx);
        })
        .await;
}

/// Scenario: `drive` first polled with the deadline elapsed and a successful flush result already
/// published.
/// Guarantees: the block is acknowledged and its descriptors committed (see
/// `FlushJob::try_finish`).
#[tokio::test(flavor = "current_thread")]
async fn a_flush_ready_at_the_deadline_is_acknowledged_not_nacked() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, mut rx) = effects(4);
            let (pdata_tx, control_tx, inbox) = inbox(2);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let metrics = super::super::metrics::Metrics::register(&context, &cfg.lake);
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(metrics);

            worker.admit(logs_pdata());
            let id = *worker
                .active
                .data
                .pending_series
                .iter()
                .next()
                .expect("the request carries a descriptor");
            let partition = worker.active.data.partition;
            worker.rotate();

            // The write parks, then is released, and the supervising task runs
            // to completion. The loop has not been created yet, so nothing can
            // take the result it publishes: that is the state the deadline
            // branch has to be able to win.
            store.hooks().entered.notified().await;
            store.hooks().set(Fault::None);
            store.hooks().release.notify_waiters();
            until("the flush publishes its result", || {
                worker
                    .flushing
                    .as_ref()
                    .is_some_and(|job| job.task_finished())
            })
            .await;

            // An already-elapsed deadline, so the deadline sleep and the flush
            // result are both ready the first time the loop is polled.
            let elapsed = clock::now()
                .checked_sub(Duration::from_secs(1))
                .expect("an instant one second in the past");
            worker.shutdown(elapsed);

            let terminal = super::super::drive(&mut worker, inbox)
                .await
                .expect("the loop returns at its deadline");
            let snapshots = terminal.metrics();
            assert_eq!(
                terminal_value(snapshots, "nacks", &[("error.type", "shutdown")]),
                0,
                "a block whose files exist is never refused"
            );
            assert_eq!(terminal_value(snapshots, "acks", &[]), 1);

            assert!(
                worker.cache.is_committed(&id, partition),
                "a block decided by its own successful result still commits"
            );
            match rx.recv().await.expect("a completion") {
                PipelineCompletionMsg::DeliverAck { .. } => {}
                other => panic!("expected an ack for a durable block, got {other:?}"),
            }
            assert!(worker.is_idle(), "nothing is left holding a slot");
            drop(pdata_tx);
            drop(control_tx);
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: a request is buffered behind a Shutdown whose 120 s deadline
/// leaves the last block one `flush_retry_deadline` (60 s) and the completion
/// margin, and storage is healthy.
/// Guarantees: the force-drained request is admitted and written, then
/// acknowledged, instead of being refused as retryable.
#[tokio::test(flavor = "current_thread")]
async fn a_request_drained_before_the_cut_is_written() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = fault_store();
            let (handler, mut rx) = effects(8);
            let (pdata_tx, control_tx, inbox) = inbox(8);
            let node = tokio::task::spawn_local(super::super::run(
                worker_config(),
                store.clone(),
                Arc::new(lake::clock::TestWallClock::new(0)),
                inbox,
                handler,
                None,
            ));
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(120),
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("the request enqueues");
            drop(pdata_tx);
            assert_eq!(
                expect_ack(&mut rx).await,
                Some(7),
                "the drained request is written"
            );
            let _terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("join")
                .expect("the node shuts down cleanly");
            drop(control_tx);
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: shutdown latches with a 10 s deadline while a block's write never
/// returns, and the clock reaches the completion margin before the deadline.
/// Guarantees: nothing is decided before `deadline - SHUTDOWN_COMPLETION_MARGIN`,
/// and every held request is decided at it, so the completions are in the
/// channel before the deadline, while a durable buffer still records them.
#[tokio::test(flavor = "current_thread")]
async fn held_requests_are_decided_the_completion_margin_before_the_deadline() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, mut rx) = effects(8);
            let (pdata_tx, control_tx, inbox) = inbox(8);
            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_secs(1);
            let node = tokio::task::spawn_local(super::super::run(
                cfg,
                store.clone(),
                Arc::new(lake::clock::TestWallClock::new(0)),
                inbox,
                handler,
                None,
            ));
            for _ in 0..4 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("a request of the block enqueues");
            }
            store.hooks().entered.notified().await;
            let deadline = clock::now() + Duration::from_secs(10);
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline,
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            drop(pdata_tx);
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }

            sim.advance_to(deadline - SHUTDOWN_COMPLETION_MARGIN - Duration::from_millis(1));
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }
            assert!(
                rx.try_recv().is_err(),
                "nothing is decided before the margin"
            );

            sim.advance_to(deadline - SHUTDOWN_COMPLETION_MARGIN);
            for _ in 0..4 {
                expect_shutdown_nack(&mut rx).await;
            }
            assert!(clock::now() < deadline);

            let _ticker = ticking(&sim, Duration::from_millis(100));
            let _terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("join")
                .expect("the node shuts down cleanly");
            drop(control_tx);
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: shutdown latches with a 10 s deadline while a block's write never
/// returns, and the clock reaches the point where a durable buffer in front,
/// which stops recording completions `DURABLE_BUFFER_RECORDING_RESERVE`
/// before the deadline, still has `COMPLETION_DISPATCH_MARGIN` to receive
/// them.
/// Guarantees: every held request is decided by then, so an Ack or Nack the
/// exporter decides at shutdown reaches the buffer while it records it, and
/// an acknowledged block is not replayed after the restart.
#[tokio::test(flavor = "current_thread")]
async fn held_requests_are_decided_before_the_buffer_stops_recording() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, mut rx) = effects(8);
            let (pdata_tx, control_tx, inbox) = inbox(8);
            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_secs(1);
            let node = tokio::task::spawn_local(super::super::run(
                cfg,
                store.clone(),
                Arc::new(lake::clock::TestWallClock::new(0)),
                inbox,
                handler,
                None,
            ));
            for _ in 0..4 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("a request of the block enqueues");
            }
            store.hooks().entered.notified().await;
            let deadline = clock::now() + Duration::from_secs(10);
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline,
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            drop(pdata_tx);
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }

            let recording_ends = deadline - DURABLE_BUFFER_RECORDING_RESERVE;
            sim.advance_to(recording_ends - COMPLETION_DISPATCH_MARGIN);
            for _ in 0..4 {
                expect_shutdown_nack(&mut rx).await;
            }
            assert!(clock::now() < recording_ends);

            let _ticker = ticking(&sim, Duration::from_millis(100));
            let _terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("join")
                .expect("the node shuts down cleanly");
            drop(control_tx);
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: four requests in a parked write, a Shutdown with a 60 s deadline
/// latched while the upstream sender stays open, then a second Shutdown with a
/// 10 s deadline, then the sender closes.
/// Guarantees: the Shutdown the node receives carries the tighter deadline:
/// nothing is decided before `10 s - SHUTDOWN_COMPLETION_MARGIN` and every
/// held request is decided at it, before the tighter deadline.
#[tokio::test(flavor = "current_thread")]
async fn a_tighter_shutdown_latched_during_the_drain_moves_the_cut() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, mut rx) = effects(8);
            let (pdata_tx, control_tx, inbox) = inbox(8);
            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_secs(1);
            let node = tokio::task::spawn_local(super::super::run(
                cfg,
                store.clone(),
                Arc::new(lake::clock::TestWallClock::new(0)),
                inbox,
                handler,
                None,
            ));
            for _ in 0..4 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("a request of the block enqueues");
            }
            store.hooks().entered.notified().await;
            // Both are latched by the inbox while the sender is open; the
            // tighter deadline replaces the first.
            for secs in [60, 10] {
                control_tx
                    .send_async(NodeControlMsg::Shutdown {
                        deadline: clock::now() + Duration::from_secs(secs),
                        reason: "test".to_owned(),
                    })
                    .await
                    .expect("the shutdown enqueues");
                for _ in 0..32 {
                    tokio::task::yield_now().await;
                }
            }
            let deadline = clock::now() + Duration::from_secs(10);
            // The input is empty and now closed, so the latched Shutdown is
            // released.
            drop(pdata_tx);
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }

            sim.advance_to(deadline - SHUTDOWN_COMPLETION_MARGIN - Duration::from_millis(1));
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }
            assert!(
                rx.try_recv().is_err(),
                "nothing is decided before the margin"
            );

            sim.advance_to(deadline - SHUTDOWN_COMPLETION_MARGIN);
            for _ in 0..4 {
                expect_shutdown_nack(&mut rx).await;
            }
            assert!(
                clock::now() < deadline,
                "decided before the tighter deadline"
            );

            let _ticker = ticking(&sim, Duration::from_millis(100));
            let _terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("join")
                .expect("the node shuts down cleanly");
            drop(control_tx);
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: a block of eight requests whose write succeeds exactly at the
/// flush cut, `deadline - SHUTDOWN_COMPLETION_MARGIN`, while the completion
/// channel holds only two and is read as the engine reads it.
/// Guarantees: every ack is delivered before the deadline and none is counted
/// lost, so a durable buffer upstream records all of them and replays none;
/// the cut is reported as such, not as an exceeded deadline, and the terminal
/// state keeps the shutdown deadline, not the cut.
#[tokio::test(flavor = "current_thread")]
async fn acks_published_at_the_cut_outnumbering_the_channel_all_arrive() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            const REQUESTS: usize = 8;
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, mut rx) = effects(2);
            let (pdata_tx, control_tx, inbox) = inbox(2);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let cfg = worker_config_with_requests(REQUESTS);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let metrics = super::super::metrics::Metrics::register(&context, &cfg.lake);
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(metrics);
            for source in 0..REQUESTS {
                worker.admit(logs_pdata_from(source));
            }
            worker.rotate();
            store.hooks().entered.notified().await;
            store.hooks().set(Fault::None);
            store.hooks().release.notify_waiters();
            until("the flush publishes its result", || {
                worker
                    .flushing
                    .as_ref()
                    .is_some_and(|job| job.task_finished())
            })
            .await;

            // The cut is now; the deadline is the whole margin away.
            let deadline = clock::now() + SHUTDOWN_COMPLETION_MARGIN;
            worker.shutdown(deadline);

            let (terminal, acked) = tokio::join!(super::super::drive(&mut worker, inbox), async {
                let mut acked = Vec::new();
                while acked.len() < REQUESTS {
                    tokio::task::yield_now().await;
                    acked.push(expect_ack(&mut rx).await.expect("a routed ack"));
                }
                acked
            });
            let terminal = terminal.expect("the loop returns");
            assert!(clock::now() < deadline, "delivered inside the margin");
            let mut acked = acked;
            acked.sort_unstable();
            assert_eq!(acked, (0..REQUESTS).collect::<Vec<_>>());
            assert_eq!(worker.notify.lost_acks(), 0, "no ack is lost");
            assert_eq!(
                terminal_value(terminal.metrics(), "acks", &[]),
                REQUESTS as u64
            );
            assert_eq!(terminal.deadline(), deadline);
            let cut = events.named("series_parquet.shutdown.cut");
            assert_eq!(cut.len(), 1, "{cut:?}");
            assert_eq!(cut[0].level, tracing::Level::WARN, "a block was held");
            assert!(
                events
                    .named("series_parquet.shutdown.deadline_exceeded")
                    .is_empty(),
                "the deadline was not reached"
            );
            let summary = events.named("series_parquet.shutdown.complete");
            assert_eq!(summary.len(), 1, "{summary:?}");
            assert_eq!(
                summary[0].fields.get("deadline_exceeded").cloned(),
                Some(FieldValue::Bool(false))
            );
            drop(pdata_tx);
            drop(control_tx);
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: an idle worker whose upstream sender stays open, so the flush cut
/// is reached on an ordinary shutdown with nothing held or queued.
/// Guarantees: the cut is logged at INFO with nothing held, not as a WARN, and
/// the node returns with the shutdown deadline.
#[tokio::test(flavor = "current_thread")]
async fn an_idle_cut_is_not_a_warning() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (handler, mut rx) = effects(2);
            let (pdata_tx, control_tx, inbox) = inbox(2);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), fault_store(), wall, handler);
            let deadline = clock::now() + Duration::from_secs(10);
            worker.shutdown(deadline);

            let (terminal, ()) = tokio::join!(super::super::drive(&mut worker, inbox), async {
                for _ in 0..32 {
                    tokio::task::yield_now().await;
                }
                sim.advance_to(deadline - SHUTDOWN_COMPLETION_MARGIN);
            });
            let terminal = terminal.expect("the loop returns");
            assert_eq!(terminal.deadline(), deadline);
            let cut = events.named("series_parquet.shutdown.cut");
            assert_eq!(cut.len(), 1, "{cut:?}");
            assert_eq!(cut[0].level, tracing::Level::INFO);
            assert_eq!(
                cut[0].fields.get("held").cloned(),
                Some(FieldValue::Bool(false))
            );
            drop(pdata_tx);
            drop(control_tx);
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: `SeriesParquet::start` runs on local file storage once with a
/// `retry` section and once without, and is shut down at once.
/// Guarantees: only the run with a `retry` section logs one
/// `series_parquet.retry_ignored_for_file_storage` WARN, since file storage
/// applies no store retry.
#[tokio::test(flavor = "current_thread")]
async fn retry_on_file_storage_is_reported_as_ignored() {
    for with_retry in [true, false] {
        let events = capture();
        tokio::task::LocalSet::new()
            .run_until(async {
                let (handler, _rx) = effects(2);
                let (pdata_tx, control_tx, inbox) = inbox(2);
                let mut section = serde_json::json!({
                    "storage": {"file": {"base_uri": std::env::temp_dir().to_string_lossy()}},
                    "window": {"interval": "1s", "max_requests_per_block": 1}
                });
                if with_retry {
                    section["retry"] = serde_json::json!({"max_retries": 3});
                }
                let cfg: Config = serde_json::from_value(section).expect("valid config");
                let node = tokio::task::spawn_local(
                    Box::new(super::super::SeriesParquet::new(cfg)).start(inbox, handler),
                );
                control_tx
                    .send_async(NodeControlMsg::Shutdown {
                        deadline: clock::now() + Duration::from_secs(5),
                        reason: "test".to_owned(),
                    })
                    .await
                    .expect("the shutdown enqueues");
                drop(pdata_tx);
                let _ = tokio::time::timeout(Duration::from_secs(10), node)
                    .await
                    .expect("the node returns")
                    .expect("the node task joins")
                    .expect("the node succeeds");
            })
            .await;
        let warned = events
            .named("series_parquet.retry_ignored_for_file_storage")
            .len();
        assert_eq!(warned, usize::from(with_retry), "with_retry={with_retry}");
    }
}
