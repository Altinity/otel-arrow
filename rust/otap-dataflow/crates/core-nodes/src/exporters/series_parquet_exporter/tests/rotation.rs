// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Rotation and windows: aligned boundaries, clock steps and the
//! thresholds that seal a block early.

use super::support::*;

/// Scenario: a request in a 15s window, then the wall clock steps back an hour while monotonic time
/// runs.
/// Guarantees: the block rotates within one monotonic interval; its replacement keeps the floored
/// start and re-emits descriptors.
#[tokio::test(flavor = "current_thread")]
async fn a_backward_clock_step_rotates_within_one_monotonic_interval() {
    use futures::FutureExt;

    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(100_000 * 1_000_000_000));
            let mut cfg = worker_config();
            cfg.window.interval = Duration::from_secs(15);
            cfg.lake.window_interval = Duration::from_secs(15);
            let mut worker = Worker::new(
                cfg,
                Arc::new(object_store::memory::InMemory::new()),
                Arc::clone(&wall) as _,
                handler,
            );
            assert_eq!(worker.active.data.window_start_secs, 99_990);
            worker.admit(logs_pdata());
            assert_eq!(worker.active.tokens.len(), 1);

            wall.set((100_000 - 3600) * 1_000_000_000);
            let mut rotated_after = None;
            for elapsed in 1..=3600_u64 {
                sim.advance(Duration::from_secs(1));
                tokio::task::yield_now().await;
                if worker.window.sleep.as_mut().now_or_never().is_some() {
                    worker.wake_window();
                    if worker.rotation_requested {
                        rotated_after = Some(elapsed);
                        break;
                    }
                }
            }
            let rotated_after = rotated_after.expect("the block is rotated");
            assert!(
                rotated_after <= 15,
                "rotated only after {rotated_after} s of monotonic time"
            );
            worker.rotate();
            assert!(worker.flushing.is_some(), "the block is written");
            assert_eq!(
                worker.active.data.window_start_secs, 99_990,
                "the window start never moves backwards"
            );
            assert!(worker.active.reemit, "the same window re-emits descriptors");
        })
        .await;
}

/// Scenario: a 1s window opened 600 ms in, then a 1.5s backward step, so the monotonic interval
/// ends 900 ms short of the boundary.
/// Guarantees: the window rotates after one monotonic interval with its start floored.
#[tokio::test(flavor = "current_thread")]
async fn a_step_just_over_one_interval_rotates_within_one_monotonic_interval() {
    use futures::FutureExt;

    let sim = clock::SimClock::new();
    let _clock_guard = sim.install();
    let wall = Arc::new(lake::clock::TestWallClock::new(100_000_600_000_000));
    let mut window =
        super::super::window::Window::new(Duration::from_secs(1), Arc::clone(&wall) as _);
    wall.set(100_000_600_000_000 - 1_500_000_000);
    let mut rotated_after_ms = None;
    for step in 1..=30_u64 {
        sim.advance(Duration::from_millis(100));
        wall.advance(100_000_000);
        tokio::task::yield_now().await;
        if window.sleep.as_mut().now_or_never().is_some() && window.wake() {
            rotated_after_ms = Some(step * 100);
            break;
        }
    }
    let rotated_after_ms = rotated_after_ms.expect("the window is rotated");
    assert!(
        rotated_after_ms <= 1_000,
        "rotated only after {rotated_after_ms} ms of monotonic time"
    );
    assert!(
        window.floored,
        "the start stays floored at the consumed boundary"
    );
    assert_eq!(window.clock.last_boundary(), 100_000);
}

/// Scenario: the wall clock runs half a second slow over a 15s window.
/// Guarantees: a floored rotation at the monotonic bound, then the boundary rotation moves the
/// start forward.
#[tokio::test(flavor = "current_thread")]
async fn wall_clock_drift_rotates_at_the_monotonic_bound_then_at_the_boundary() {
    use futures::FutureExt;

    let sim = clock::SimClock::new();
    let _clock_guard = sim.install();
    let wall = Arc::new(lake::clock::TestWallClock::new(99_990 * 1_000_000_000));
    let mut window =
        super::super::window::Window::new(Duration::from_secs(15), Arc::clone(&wall) as _);

    sim.advance(Duration::from_secs(15));
    wall.set(100_004_500_000_000);
    tokio::task::yield_now().await;
    assert!(
        window.sleep.as_mut().now_or_never().is_some(),
        "the monotonic bound fires"
    );
    assert!(
        window.wake(),
        "the monotonic bound rotates regardless of drift"
    );
    assert!(window.floored);

    sim.advance(Duration::from_millis(500));
    wall.set(100_005 * 1_000_000_000);
    tokio::task::yield_now().await;
    assert!(
        window.sleep.as_mut().now_or_never().is_some(),
        "the boundary is still owed"
    );
    assert!(window.wake(), "the boundary itself rotates");
    assert!(
        !window.floored,
        "a boundary rotation moves the window start"
    );
    assert_eq!(window.clock.last_boundary(), 100_005);
}

/// Scenario: a boundary passes while a write holds the flush slot and the ACTIVE block is empty.
/// Guarantees: the empty block is replaced at once and admission stays open.
#[tokio::test(flavor = "current_thread")]
async fn an_empty_block_rotates_without_waiting_for_the_flush_slot() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store, Arc::clone(&wall) as _, handler);

            worker.admit(logs_pdata());
            worker.rotate();
            assert!(
                worker.flushing.is_some(),
                "the first block is being written"
            );
            assert!(worker.active.data.is_empty());

            wall.set(1_000_000_000);
            let _ = worker.window.clock.on_wake(1);
            worker.reason = super::super::metrics::FlushReason::Time;
            worker.rotation_requested = true;
            worker.rotate();
            assert!(
                !worker.rotation_requested,
                "an empty block needs no flush slot"
            );
            assert_eq!(worker.active.data.window_start_secs, 1);
            assert!(worker.accept(), "admission stays open across the boundary");
        })
        .await;
}

/// Scenario: a boundary sleep fires after a jump past several windows, then the clock steps back.
/// Guarantees: one rotation at the latest boundary, the sleep re-armed, no re-fire after the step.
#[tokio::test(flavor = "current_thread")]
async fn busy_rotation_rearms_boundary_sleep() {
    let sim = clock::SimClock::new();
    let _clock_guard = sim.install();
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut window = super::super::window::Window::new(Duration::from_secs(15), wall.clone());

    // Four windows pass while the node is busy; one sleep covers them all.
    wall.set(65_000_000_000);
    sim.advance(Duration::from_secs(15));
    window.sleep.as_mut().await;
    assert!(window.wake(), "a crossed boundary asks for a rotation");
    assert_eq!(window.clock.last_boundary(), 60);
    assert_eq!(window.clock.next_boundary(65), 75);
    assert!(
        futures::poll!(window.sleep.as_mut()).is_pending(),
        "the next boundary is armed rather than already elapsed"
    );

    // The wall clock steps back behind the boundary that was just consumed.
    wall.set(50_000_000_000);
    sim.advance(Duration::from_secs(10));
    window.sleep.as_mut().await;
    assert!(!window.wake(), "a consumed boundary is not fired twice");
    assert_eq!(window.clock.last_boundary(), 60);
    assert!(
        futures::poll!(window.sleep.as_mut()).is_pending(),
        "a too-early wake still re-arms the sleep"
    );
}

/// Scenario: two requests extracted just before and just after a 1s boundary.
/// Guarantees: the first stays in the ACTIVE block, the second is parked for the next window.
#[tokio::test(flavor = "current_thread")]
async fn admission_time_assigns_exactly_one_window() {
    let (handler, _rx) = effects(8);
    let wall = Arc::new(lake::clock::TestWallClock::new(999_999_999));
    let mut worker = Worker::new(
        worker_config(),
        Arc::new(object_store::memory::InMemory::new()),
        Arc::clone(&wall) as _,
        handler,
    );

    worker.admit(logs_pdata());
    assert_eq!(worker.active.data.window_start_secs, 0);
    assert_eq!(worker.active.tokens.len(), 1);
    assert!(
        !worker.rotation_requested,
        "one request no longer seals a block"
    );

    wall.set(1_000_000_001);
    worker.admit(logs_pdata());
    assert_eq!(worker.active.tokens.len(), 1);
    assert!(worker.pending.is_some());
    assert!(worker.rotation_requested);
    assert!(!worker.accept());
}

/// Scenario: requests at the last nanosecond of a window and at the first of the next.
/// Guarantees: the boundary nanosecond belongs to the new window.
#[tokio::test(flavor = "current_thread")]
async fn admission_at_the_exact_boundary_belongs_to_the_next_window() {
    let (handler, _rx) = effects(8);
    let wall = Arc::new(lake::clock::TestWallClock::new(999_999_999));
    let mut worker = Worker::new(
        worker_config(),
        Arc::new(object_store::memory::InMemory::new()),
        Arc::clone(&wall) as _,
        handler,
    );
    worker.admit(logs_pdata());
    assert_eq!(worker.active.data.window_start_secs, 0);
    assert_eq!(worker.active.tokens.len(), 1);

    wall.set(1_000_000_000);
    worker.admit(logs_pdata());
    assert_eq!(
        worker.active.tokens.len(),
        1,
        "the ended window takes nothing more"
    );
    assert_eq!(
        worker.pending.as_ref().map(|p| p.admission_secs),
        Some(1),
        "the boundary request waits for the window it belongs to"
    );
}

/// Scenario: two boundaries are crossed while a parked write holds the flush slot; the write is
/// then released without moving the clock.
/// Guarantees: one rotation is served as soon as the flush completes.
#[tokio::test(flavor = "current_thread")]
async fn a_boundary_crossed_while_flushing_rotates_when_the_flush_completes() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let inner = Arc::new(object_store::memory::InMemory::new());
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let entered = Arc::new(AtomicUsize::new(0));
            let store = Arc::new(HookStore::new(
                Arc::clone(&inner) as Arc<dyn ObjectStore>,
                GatedStore {
                    gate: Arc::clone(&gate),
                    entered: Arc::clone(&entered),
                },
            ));
            let writes = || entered.load(SeqCst);

            // One slot in each channel, so handing a message over is itself
            // the proof that the node has taken the previous one.
            let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<OtapPdata>>::new(1);
            let (pdata_tx, pdata_rx) = mpsc::Channel::<OtapPdata>::new(1);
            let inbox = ExporterInbox::new(
                Receiver::Local(LocalReceiver::mpsc(control_rx)),
                Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
                0,
                Interests::empty(),
            );
            let (handler, mut rx) = effects(8);
            let node = tokio::task::spawn_local(super::super::run(
                worker_config(),
                store as Arc<dyn ObjectStore>,
                Arc::clone(&wall) as _,
                inbox,
                handler,
                None,
            ));

            // The first request fills the window that starts at 0 s.
            pdata_tx
                .send_async(logs_pdata_from(1))
                .await
                .expect("the first request enqueues");
            marker(&pdata_tx, &mut rx, 10).await;

            // The 1 s boundary seals that block, and its write parks on the
            // gate: the flush slot is now held.
            wall.set(1_100_000_000);
            sim.advance(Duration::from_secs(1));
            until("the first block's write reaches the store", || writes() > 0).await;

            // The second request joins the block that replaced it, which
            // cannot be written while the flush slot is held.
            pdata_tx
                .send_async(logs_pdata_from(2))
                .await
                .expect("the second request enqueues");
            marker(&pdata_tx, &mut rx, 11).await;

            // Two more boundaries pass while the write is still parked. No
            // request is parked and no budget is reached, so nothing but
            // these boundaries can ask for the rotation that follows.
            wall.set(3_100_000_000);
            sim.advance(Duration::from_secs(2));

            // Control outranks the inbox but is outranked by the boundary, so
            // two control messages handed over through a one-slot channel
            // prove the node has served the boundary that was ready first.
            for _ in 0..2 {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    control_tx.send_async(NodeControlMsg::TimerTick {}),
                )
                .await
                .expect("control is still served while the flush is parked")
                .expect("the control message enqueues");
            }
            assert_eq!(
                stored_files(&inner).await,
                0,
                "nothing is written while the flush is parked"
            );

            // Releasing the write is the only thing that changes: the clock
            // stays where it is, so the second block can only be written by
            // the rotation the boundaries left owed.
            gate.add_permits(1);
            assert_eq!(expect_ack(&mut rx).await, Some(1));
            assert_eq!(
                expect_ack(&mut rx).await,
                Some(2),
                "the boundary's rotation is served on completion, not at the next boundary"
            );

            // The marker gives the node the turns a third rotation would need
            // before the file count is read.
            marker(&pdata_tx, &mut rx, 12).await;
            assert_eq!(
                stored_files(&inner).await,
                4,
                "two boundaries missed while flushing are one rotation, not two"
            );

            drop(pdata_tx);
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            let terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("the node task joins");
            if let Err(error) = terminal {
                panic!("unexpected node failure: {error}");
            }
            drop(control_tx);
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: a request parked for a later window while a parked write holds the slot, a newer
/// request behind it and control traffic throughout.
/// Guarantees: the parked request enters the block the flush opens, ahead of the newer one; control
/// stays served; one rotation.
#[tokio::test(flavor = "current_thread")]
async fn a_parked_request_enters_the_block_the_finished_flush_opens() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let inner = Arc::new(object_store::memory::InMemory::new());
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let entered = Arc::new(AtomicUsize::new(0));
            let store = Arc::new(HookStore::new(
                Arc::clone(&inner) as Arc<dyn ObjectStore>,
                GatedStore {
                    gate: Arc::clone(&gate),
                    entered: Arc::clone(&entered),
                },
            ));
            let writes = || entered.load(SeqCst);

            let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<OtapPdata>>::new(1);
            let (pdata_tx, pdata_rx) = mpsc::Channel::<OtapPdata>::new(1);
            let inbox = ExporterInbox::new(
                Receiver::Local(LocalReceiver::mpsc(control_rx)),
                Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
                0,
                Interests::empty(),
            );
            let (handler, mut rx) = effects(8);
            let node = tokio::task::spawn_local(super::super::run(
                worker_config(),
                store as Arc<dyn ObjectStore>,
                Arc::clone(&wall) as _,
                inbox,
                handler,
                None,
            ));

            pdata_tx
                .send_async(logs_pdata_from(1))
                .await
                .expect("the first request enqueues");
            marker(&pdata_tx, &mut rx, 10).await;

            wall.set(1_100_000_000);
            sim.advance(Duration::from_secs(1));
            until("the first block's write reaches the store", || writes() > 0).await;

            pdata_tx
                .send_async(logs_pdata_from(2))
                .await
                .expect("the second request enqueues");
            marker(&pdata_tx, &mut rx, 11).await;

            // The wall clock moves past the ACTIVE block's window without the
            // boundary sleep firing, so the third request is parked rather
            // than admitted to a block whose window has ended.
            wall.set(2_100_000_000);
            pdata_tx
                .send_async(logs_pdata_from(3))
                .await
                .expect("the third request enqueues");

            // The one-slot channel takes this only once the third request has
            // been taken off it, so the fourth is queued behind a parked one.
            // Admission is closed, so it stays on the channel.
            pdata_tx
                .send_async(logs_pdata_from(4))
                .await
                .expect("the fourth request enqueues");

            // Control is served even though pdata admission is closed and the
            // flush is parked.
            for _ in 0..2 {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    control_tx.send_async(NodeControlMsg::TimerTick {}),
                )
                .await
                .expect("control is still served while the flush is parked")
                .expect("the control message enqueues");
            }
            assert_eq!(
                stored_files(&inner).await,
                0,
                "nothing is written while the flush is parked"
            );

            // Releasing the write opens the block the parked request has been
            // waiting for, without the clock moving again.
            gate.add_permits(1);
            assert_eq!(expect_ack(&mut rx).await, Some(1));
            assert_eq!(expect_ack(&mut rx).await, Some(2));
            until("both blocks are written", || writes() == 4).await;

            // The marker is taken only once the fourth request has been, so
            // by this point both it and the parked request are admitted.
            marker(&pdata_tx, &mut rx, 12).await;
            assert_eq!(
                stored_files(&inner).await,
                4,
                "the resumed request's block is not sealed before its window ends"
            );

            // The next boundary seals the block the parked request entered,
            // together with the request that was queued behind it.
            wall.set(5_100_000_000);
            sim.advance(Duration::from_secs(2));
            assert_eq!(
                expect_ack(&mut rx).await,
                Some(3),
                "the parked request is stored before the newer one"
            );
            assert_eq!(expect_ack(&mut rx).await, Some(4));

            drop(pdata_tx);
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            let terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("the node task joins");
            if let Err(error) = terminal {
                panic!("unexpected node failure: {error}");
            }
            drop(control_tx);
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: a block reaches its request limit inside one window.
/// Guarantees: the limit requests the rotation and admission closes until the seal.
#[tokio::test(flavor = "current_thread")]
async fn the_request_limit_asks_for_a_rotation_within_a_window() {
    let (handler, _rx) = effects(8);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut worker = Worker::new(
        worker_config_with_requests(2),
        Arc::new(object_store::memory::InMemory::new()),
        wall,
        handler,
    );

    worker.admit(logs_pdata());
    assert!(!worker.rotation_requested);
    worker.admit(logs_pdata());
    assert_eq!(worker.active.tokens.len(), 2);
    assert!(worker.rotation_requested, "a full block is sealed at once");
    assert!(!worker.accept());
}

/// Scenario: a request-limited block rotates twice in one window, then a later window opens a new
/// partition.
/// Guarantees: flushes, `series.emitted` reasons (`rotation`, `partition`) and datasets are
/// labelled as they happened; an empty rotation is no flush.
#[tokio::test(flavor = "current_thread")]
async fn rotation_causes_and_flush_reasons_are_labelled() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use super::super::metrics::{DatasetAttrs, EmitAttrs, EmitReason, FlushAttrs};
            use super::super::metrics::{FlushReason, Metrics};

            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(
                worker_config_with_requests(1),
                Arc::new(object_store::memory::InMemory::new()),
                wall.clone(),
                handler,
            );
            worker.metrics = Some(Metrics::register(&context, &worker.cfg.lake));

            /// Seal the ACTIVE block, wait for its write and decide it.
            async fn flush(worker: &mut Worker) {
                worker.rotate();
                let done = worker
                    .flushing
                    .as_mut()
                    .expect("a rotated block is flushing")
                    .finish()
                    .await;
                assert!(done.as_ref().expect("the flush task joins").result.is_ok());
                worker.complete(done);
                // The decided block keeps the flush slot until its supervising
                // task has released it, so the next rotation waits for that.
                drain_cleanup(worker).await;
                // The notifier holds one credit per block here, so the ack is
                // delivered before the next block is decided.
                assert!(worker.notify.next().await.is_ok());
            }

            fn emitted(worker: &Worker, reason: EmitReason) -> u64 {
                worker
                    .metrics
                    .as_ref()
                    .expect("metrics")
                    .emitted
                    .get(EmitAttrs { reason })
                    .series_emitted
                    .get()
            }

            // A full block inside one window: the first copy of the
            // descriptor is new, and the block that replaces it must repeat it
            // because the replaced block is not durable when it is opened.
            worker.admit(logs_pdata());
            assert_eq!(worker.reason, FlushReason::Requests);
            let rows = worker.active.data.pending_series.len() as u64;
            assert!(rows > 0, "the request carries a descriptor");
            flush(&mut worker).await;
            assert!(worker.active.reemit, "the window has not moved on");
            assert_eq!(emitted(&worker, EmitReason::New), rows);

            worker.admit(logs_pdata());
            flush(&mut worker).await;
            assert_eq!(emitted(&worker, EmitReason::Rotation), rows);

            // A later window in a later storage partition: partitions are
            // date/hour, so the clock has to cross an hour for the cached
            // commit to stop applying.
            wall.set(4_000_000_000_000);
            worker.admit(logs_pdata());
            assert!(worker.pending.is_some(), "a later window parks the request");
            worker.rotate();
            assert!(
                worker.flushing.is_none(),
                "an empty block is replaced rather than written"
            );
            assert!(!worker.active.reemit);
            worker.resume_pending();
            flush(&mut worker).await;
            assert_eq!(emitted(&worker, EmitReason::Partition), rows);

            worker.sample_metrics();
            let metrics = worker.metrics.as_mut().expect("metrics");
            assert_eq!(metrics.worker.acks.get(), 3);
            // Three real writes, each sealed by the one-request limit; the
            // empty rotation the parked request forced is not one of them.
            for (reason, expected) in [
                (FlushReason::Requests, 3),
                (FlushReason::Time, 0),
                (FlushReason::Bytes, 0),
                (FlushReason::Shutdown, 0),
            ] {
                assert_eq!(
                    metrics.flush.get(FlushAttrs { reason }).count.get(),
                    expected,
                    "flush count for {reason:?}"
                );
            }
            for dataset in [
                lake::schema::Dataset::LogsSeries,
                lake::schema::Dataset::LogsValues,
            ] {
                let written = metrics.written.get(DatasetAttrs::from(dataset));
                assert_eq!(written.files_written.get(), 3, "files for {dataset:?}");
                assert!(written.rows_written.get() >= 3, "rows for {dataset:?}");
            }
        })
        .await;
}

/// Scenario: two logs requests share a block whose seal fails (the sort names a missing column).
/// Guarantees: nothing is written and both are nacked as retryable `internal` failures.
#[tokio::test(flavor = "current_thread")]
async fn a_seal_failure_nacks_every_co_tenant_as_internal() {
    let (handler, mut rx) = effects(4);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut cfg = worker_config();
    cfg.lake.logs.values_sort = vec![lake::config::SortKey {
        column: "no_such_column".into(),
        order: lake::config::SortOrder::Asc,
        nulls: lake::config::Nulls::Last,
    }];
    let mut worker = Worker::new(
        cfg,
        Arc::new(object_store::memory::InMemory::new()),
        wall,
        handler,
    );
    worker.admit(logs_pdata_from(1));
    worker.admit(logs_pdata_from(2));
    assert_eq!(worker.active.tokens.len(), 2, "both requests are admitted");

    worker.rotate();
    assert!(
        worker.flushing.is_none(),
        "a block that cannot be sealed is not written"
    );
    assert!(worker.active.tokens.is_empty());
    assert_eq!(worker.notify.outcomes()[Outcome::Internal as usize], 2);
    assert_eq!(worker.notify.outcomes()[Outcome::Storage as usize], 0);
    for expected in [1, 2] {
        worker
            .notify
            .next()
            .await
            .expect("the completion is accepted");
        match rx.recv().await.expect("a nack") {
            PipelineCompletionMsg::DeliverNack { nack } => {
                assert!(!nack.permanent);
                assert_eq!(nack.cause, NackCause::Unspecified);
                assert_eq!(nack.reason, Outcome::Internal.sentence());
                assert_eq!((*nack.refused).into_parts().0.source_node(), Some(expected));
            }
            other => panic!("expected a nack, got {other:?}"),
        }
    }
    assert_no_more_completions(&mut rx);
}

/// Scenario: a request-limit rotation is still owed when the boundary fires.
/// Guarantees: the flush is reported as time-triggered.
#[tokio::test(flavor = "current_thread")]
async fn a_boundary_rotation_is_not_reported_under_a_stale_threshold_reason() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use super::super::metrics::{FlushAttrs, FlushReason, Metrics};

            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(
                worker_config_with_requests(1),
                Arc::new(object_store::memory::InMemory::new()),
                wall.clone(),
                handler,
            );
            worker.metrics = Some(Metrics::register(&context, &worker.cfg.lake));

            worker.admit(logs_pdata());
            assert_eq!(worker.reason, FlushReason::Requests);
            assert!(worker.rotation_requested);

            // The one-second window ends before the requested rotation is
            // served, so the boundary seals the block.
            wall.set(2_000_000_000);
            worker.wake_window();
            assert_eq!(worker.reason, FlushReason::Time);

            worker.rotate();
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(
                metrics
                    .flush
                    .get(FlushAttrs {
                        reason: FlushReason::Time
                    })
                    .count
                    .get(),
                1
            );
            assert_eq!(
                metrics
                    .flush
                    .get(FlushAttrs {
                        reason: FlushReason::Requests
                    })
                    .count
                    .get(),
                0
            );
        })
        .await;
}
