// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Flush and retry: durable completion, retries, deadlines and the
//! cache commits that follow a write.

use super::support::*;

/// Scenario: a request is admitted, rotated into an in-memory store flush, and completed.
/// Guarantees: both files exist before the ack is queued, and only then is the descriptor committed
/// under the block's partition.
#[tokio::test(flavor = "current_thread")]
async fn complete_files_before_ack() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store.clone(), wall, handler);

            worker.admit(logs_pdata());
            let id = *worker
                .active
                .data
                .pending_series
                .iter()
                .next()
                .expect("the request carries a descriptor");
            let partition = worker.active.data.partition;
            assert!(!worker.cache.is_committed(&id, partition));

            worker.rotate();
            assert!(
                worker.notify.is_empty(),
                "nothing is decided before the flush resolves"
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(5), rx.recv())
                    .await
                    .is_err(),
                "a started flush is not a durable one"
            );

            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            let report = done
                .as_ref()
                .expect("the flush task joins")
                .result
                .as_ref()
                .expect("the write succeeds");
            assert_eq!(report.files.len(), 2);
            for (_, path, _) in &report.files {
                assert!(store.head(path).await.is_ok());
            }

            worker.complete(done);
            assert!(worker.cache.is_committed(&id, partition));
            assert!(worker.notify.next().await.is_ok());
            assert!(matches!(
                rx.recv().await.expect("ack"),
                PipelineCompletionMsg::DeliverAck { .. }
            ));
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: the first values write fails and the store heals before the retry.
/// Guarantees: the retry rewrites the same names with identical bytes; one ack, one retry, no flush
/// failure.
#[tokio::test(flavor = "current_thread")]
async fn values_retry_reuses_paths_and_bytes() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::ValuesOnce);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store.clone(), wall, handler);
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(logs_pdata());
            worker.rotate();
            let _ticker = ticking(&sim, Duration::from_millis(250));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert_eq!(
                done.as_ref().expect("the flush resolves").attempts,
                2,
                "the failed write is retried once"
            );

            worker.complete(done);
            worker.notify.next().await.expect("the ack is sent");
            assert!(matches!(
                rx.recv().await.expect("ack"),
                PipelineCompletionMsg::DeliverAck { .. }
            ));

            worker.sample_metrics();
            let metrics = worker.metrics.as_mut().expect("metrics");
            assert_eq!(
                metrics.worker.flush_retries.get(),
                1,
                "the extra attempt is reported as a retry"
            );
            assert!(
                metrics.flush_failures.terminal_snapshots().is_empty(),
                "a retried flush that succeeded is not a failure"
            );
            assert_eq!(metrics.worker.flush_cancelled.get(), 0);

            let writes = store.hooks().writes.lock().expect("writes lock");
            assert_eq!(writes.len(), 4, "two files, one of them written twice");
            assert_eq!(writes[0], writes[2], "the series file is rewritten as-is");
            assert_eq!(writes[1], writes[3], "the values file is rewritten as-is");
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: the series write fails until the deadline, then the same descriptor arrives again.
/// Guarantees: the block nacks retryably without marking the cache, and the next block rewrites the
/// descriptor.
#[tokio::test(flavor = "current_thread")]
async fn failed_descriptor_does_not_poison_cache() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Series);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(30);
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);

            worker.admit(logs_pdata());
            let id = *worker
                .active
                .data
                .pending_series
                .iter()
                .next()
                .expect("the request carries a descriptor");
            worker.rotate();
            let ticker = ticking(&sim, Duration::from_millis(50));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            drop(ticker);
            worker.complete(done);
            worker.notify.next().await.expect("the nack is sent");
            match rx.recv().await.expect("nack") {
                PipelineCompletionMsg::DeliverNack { nack } => assert!(
                    !nack.permanent,
                    "a storage failure is the sender's to retry"
                ),
                other => panic!("expected a retryable nack, got {other:?}"),
            }
            assert!(
                !worker
                    .cache
                    .is_committed(&id, lake::clock::PartitionId::from_unix_secs(0)),
                "a block that was never written marks nothing durable"
            );
            drain_cleanup(&mut worker).await;

            store.hooks().set(Fault::None);
            worker.admit(logs_pdata());
            assert!(
                worker.active.data.pending_series.contains(&id),
                "the descriptor is written again rather than assumed durable"
            );
            worker.rotate();
            let done = worker
                .flushing
                .as_mut()
                .expect("the second block is flushing")
                .finish()
                .await;
            worker.complete(done);
            assert_eq!(worker.notify.len(), 1, "the second block is acknowledged");
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: a flush parked across an hour boundary, the same series in the next partition, and an
/// eviction before the flush resolves.
/// Guarantees: the commit names the flushed block's partition and leaves the next one's descriptor
/// owed.
#[tokio::test(flavor = "current_thread")]
async fn overlapping_series_and_eviction_preserve_partition_coverage() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(3_599_000_000_000));
            let mut cfg = worker_config();
            cfg.cache_entries = 1;
            let mut worker = Worker::new(cfg, store.clone(), Arc::clone(&wall) as _, handler);

            worker.admit(logs_pdata());
            let id = *worker
                .active
                .data
                .pending_series
                .iter()
                .next()
                .expect("the request carries a descriptor");
            worker.rotate();
            store.hooks().entered.notified().await;

            wall.set(3_600_000_000_000);
            worker.admit(logs_pdata());
            assert!(
                worker.pending.is_some(),
                "a request for the next hour waits for its own block"
            );
            worker.cache.touch([99; 16]);

            store.hooks().set(Fault::None);
            store.hooks().release.notify_one();
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            worker.complete(done);
            assert_eq!(
                worker.cache.last_committed(&id),
                Some(lake::clock::PartitionId::from_unix_secs(3599)),
                "the commit names the partition that was written"
            );
            drain_cleanup(&mut worker).await;

            worker.rotate();
            worker.resume_pending();
            assert_eq!(
                worker.active.data.partition,
                lake::clock::PartitionId::from_unix_secs(3600)
            );
            assert!(
                worker.active.data.pending_series.contains(&id),
                "the new partition writes its own copy of the descriptor"
            );
        })
        .await;
}

/// Scenario: the same series joins the ACTIVE block while the FLUSHING block carrying it is parked
/// in the same window.
/// Guarantees: both blocks keep their own descriptor copy.
#[tokio::test(flavor = "current_thread")]
async fn same_window_overlap_keeps_both_descriptors() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store.clone(), wall, handler);

            worker.admit(logs_pdata());
            let id = *worker
                .active
                .data
                .pending_series
                .iter()
                .next()
                .expect("the request carries a descriptor");
            worker.rotate();
            store.hooks().entered.notified().await;

            worker.admit(logs_pdata());
            assert!(
                worker.active.data.pending_series.contains(&id),
                "the ACTIVE block carries its own copy"
            );
            assert_eq!(
                worker
                    .flushing
                    .as_ref()
                    .expect("a rotated block is flushing")
                    .tokens
                    .len(),
                1,
                "the flushing block still owes its own request"
            );
            let partition = worker.active.data.partition;
            assert!(!worker.cache.is_committed(&id, partition));

            store.hooks().set(Fault::None);
            store.hooks().release.notify_one();
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            worker.complete(done);
            assert!(worker.cache.is_committed(&id, partition));
            assert!(
                worker.active.data.pending_series.contains(&id),
                "a late commit does not retract a descriptor already reserved"
            );
            drain_cleanup(&mut worker).await;

            worker.rotate();
            let done = worker
                .flushing
                .as_mut()
                .expect("the second block is flushing")
                .finish()
                .await;
            let report = done
                .as_ref()
                .expect("the flush resolves")
                .result
                .as_ref()
                .expect("the write succeeds");
            assert_eq!(report.files.len(), 2, "the descriptor is written again");
            worker.complete(done);
            assert_eq!(worker.notify.len(), 2);
        })
        .await;
}

/// Scenario: a parked write reaches its retry deadline with a one-second cleanup allowance.
/// Guarantees: the retryable decision is published at the deadline and the slot stays taken until
/// the cleanup ends.
#[tokio::test(flavor = "current_thread")]
async fn retry_deadline_publishes_before_cleanup_and_reserves_slot() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, _rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(20);
            cfg.lake.upload.abort_timeout = Duration::from_secs(1);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);

            worker.admit(logs_pdata());
            worker.rotate();
            store.hooks().entered.notified().await;

            // Only the retry deadline has elapsed. The write is still parked
            // in the store, so a decision that arrives now cannot have waited
            // for either the write or the cleanup allowance.
            sim.advance(Duration::from_millis(20));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert!(
                done.as_ref().expect("the flush resolves").result.is_err(),
                "the deadline fails the block"
            );
            worker.complete(done);
            assert_eq!(
                worker.notify.outcomes()[Outcome::Storage as usize],
                1,
                "the block is nacked as retryable storage"
            );

            // The slot the cleanup holds is the same FLUSHING slot, so nothing
            // rotates into it while the abandoned write is still unwinding.
            worker.admit(logs_pdata());
            worker.rotate();
            assert!(worker.flushing.is_none(), "no second write is started");
            assert!(worker.cleaning.is_some(), "the slot is still occupied");

            let mut job = worker.cleaning.take().expect("the occupied slot");
            sim.advance(Duration::from_secs(2));
            job.cleanup().await.expect("the cleanup is bounded");

            store.hooks().set(Fault::None);
            worker.rotate();
            assert!(
                worker.flushing.is_some(),
                "the released slot takes the next block"
            );
        })
        .await;
}

/// Scenario: the one write of a flush never returns.
/// Guarantees: a deadline outcome, not a cancellation, and every request nacked retryably with a
/// reason saying no attempt returned.
#[tokio::test(flavor = "current_thread")]
async fn a_hung_write_expires_the_flush_deadline_as_its_own_outcome() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, mut rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(20);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(logs_pdata());
            worker.rotate();
            store.hooks().entered.notified().await;
            sim.advance(Duration::from_millis(20));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            let finished = done.as_ref().expect("the flush resolves");
            assert_eq!(finished.attempts, 1);
            assert!(
                matches!(
                    finished.result,
                    Err(lake::Error::Transient(
                        lake::TransientError::DeadlineExceeded {
                            attempts: 1,
                            last: None
                        }
                    ))
                ),
                "a hung write is a deadline, not a cancellation: {:?}",
                finished.result.as_ref().err()
            );
            worker.complete(done);
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(flush_failures(metrics, WriteFailure::Deadline), 1);
            assert_eq!(flush_failures(metrics, WriteFailure::Cancelled), 0);
            assert_eq!(metrics.worker.flush_cancelled.get(), 0);

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a nack") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(
                        nack.reason,
                        "could not write to object storage (unavailable); retry the request"
                    );
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            let mut job = worker.cleaning.take().expect("the cleanup slot");
            sim.advance(Duration::from_secs(10));
            job.cleanup().await.expect("the task is released");
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: every write fails after 20s, like a store retrying internally, under a 60s deadline.
/// Guarantees: retries until the deadline, each attempt counted and logged at WARN, the fixed
/// `unavailable` nack, and the last error in `series_parquet.flush.failed`.
#[tokio::test(flavor = "current_thread")]
async fn a_slowly_failing_store_surfaces_its_last_error_at_the_deadline() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let store = fault_store();
            store.hooks().set(Fault::SlowFail);
            let (handler, mut rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_secs(60);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(logs_pdata());
            worker.rotate();
            let ticker = ticking(&sim, Duration::from_millis(100));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            drop(ticker);
            let finished = done.as_ref().expect("the flush resolves");
            // Attempts start at 0 s, 20.2 s and 40.6 s; the third is still
            // failing when the deadline expires at 60 s.
            assert_eq!(finished.attempts, 3);
            match &finished.result {
                Err(lake::Error::Transient(lake::TransientError::DeadlineExceeded {
                    attempts: 3,
                    last: Some(last),
                })) => assert!(
                    last.to_string().contains("injected store failure"),
                    "{last}"
                ),
                other => panic!("expected the deadline with the last error, got {other:?}"),
            }
            worker.complete(done);
            worker.sample_metrics();
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(metrics.worker.flush_retries.get(), 2);
            assert_eq!(flush_failures(metrics, WriteFailure::Deadline), 1);
            assert_eq!(metrics.worker.flush_cancelled.get(), 0);

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a nack") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(
                        nack.reason,
                        "could not write to object storage (unavailable); retry the request"
                    );
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            assert_no_more_completions(&mut rx);
            let mut job = worker.cleaning.take().expect("the cleanup slot");
            let ticker = ticking(&sim, Duration::from_secs(1));
            job.cleanup().await.expect("the task is released");
            drop(ticker);
        })
        .await;
    let failed = events.named("series_parquet.flush.failed");
    assert_eq!(failed.len(), 1, "{failed:?}");
    let field = |name: &str| failed[0].fields.get(name).cloned();
    assert_eq!(field("window_start"), Some(FieldValue::I64(0)));
    assert_eq!(field("seq"), Some(FieldValue::U64(0)));
    assert_eq!(field("requests"), Some(FieldValue::U64(1)));
    assert!(matches!(field("bytes"), Some(FieldValue::U64(n)) if n > 0));
    assert!(
        field("file").is_some_and(|file| file.text().ends_with(".parquet")),
        "{failed:?}"
    );
    assert_eq!(
        field("error_type"),
        Some(FieldValue::Str("deadline".into()))
    );
    assert!(
        failed[0].fields.get("error").is_some_and(|error| {
            let text = error.text();
            text.contains("after 3 attempt(s); last error:")
                && text.contains("injected store failure")
        }),
        "{failed:?}"
    );
    let logged = events.named("series_parquet.flush.attempt_failed");
    assert_eq!(
        logged.len(),
        2,
        "two attempts returned a failure: {logged:?}"
    );
    for (index, event) in logged.iter().enumerate() {
        assert_eq!(event.level, tracing::Level::WARN);
        assert_eq!(
            event.fields.get("attempt"),
            Some(&FieldValue::U64(index as u64 + 1))
        );
        assert_eq!(event.fields.get("retryable"), Some(&FieldValue::Bool(true)));
        assert_eq!(event.fields.get("seq"), Some(&FieldValue::U64(0)));
        assert!(
            matches!(
                event.fields.get("deadline_remaining"),
                Some(FieldValue::Debug(_))
            ),
            "{event:?}"
        );
        assert!(
            event
                .fields
                .get("error")
                .is_some_and(|error| error.text().contains("injected store failure")),
            "{event:?}"
        );
    }
    // Attempts 2 and 3 are retries, announced at INFO with the block's
    // sequence and the time left before its deadline.
    let retries: Vec<_> = events
        .named("series_parquet.flush.attempt")
        .into_iter()
        .filter(|event| event.level == tracing::Level::INFO)
        .collect();
    assert_eq!(retries.len(), 2, "{retries:?}");
    for event in &retries {
        assert_eq!(event.fields.get("seq"), Some(&FieldValue::U64(0)));
        assert!(
            matches!(
                event.fields.get("deadline_remaining"),
                Some(FieldValue::Debug(_))
            ),
            "{event:?}"
        );
    }
}

/// Scenario: telemetry sampled while the second of two slow failing attempts is in flight.
/// Guarantees: `flush.retries` already reads 1.
#[tokio::test(flavor = "current_thread")]
async fn a_retry_is_counted_when_it_starts() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let store = fault_store();
            store.hooks().set(Fault::SlowFail);
            let (handler, _rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_secs(60);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(logs_pdata());
            worker.rotate();
            // The first attempt fails at 20 s and the second starts at 20.2 s.
            step_for(&sim, Duration::from_secs(25)).await;
            assert!(
                store
                    .hooks()
                    .entered_at
                    .lock()
                    .expect("entered_at lock")
                    .len()
                    >= 2,
                "the second attempt has reached the store"
            );
            assert!(worker.flushing.is_some(), "the flush has not resolved");
            worker.sample_metrics();
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(metrics.worker.flush_retries.get(), 1);

            let ticker = ticking(&sim, Duration::from_secs(1));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            worker.complete(done);
            let mut job = worker.cleaning.take().expect("the cleanup slot");
            job.cleanup().await.expect("the task is released");
            drop(ticker);
        })
        .await;
}

/// Scenario: an encoding bug and a storage I/O error as the same lake error type.
/// Guarantees: only the storage failure is retried.
#[test]
fn retry_classifier_distinguishes_encoding_from_storage() {
    assert!(!lake::Error::is_retryable(&lake::Error::from(
        parquet::errors::ParquetError::General("encoding bug".into())
    )));
    assert!(lake::Error::is_retryable(&lake::Error::from(
        object_store::Error::Generic {
            store: "test",
            source: Box::new(std::io::Error::other("offline")),
        }
    )));
    assert!(lake::Error::is_retryable(&lake::Error::from(
        parquet::errors::ParquetError::External(Box::new(object_store::Error::Generic {
            store: "test",
            source: Box::new(std::io::Error::other("offline")),
        }))
    )));
    assert!(!lake::Error::is_retryable(&lake::Error::cancelled(None)));
    // Refused credentials and a missing bucket or prefix are storage errors
    // that no retry cures, however they are wrapped.
    let denied = || object_store::Error::PermissionDenied {
        path: "p".into(),
        source: "denied".into(),
    };
    assert!(!lake::Error::is_retryable(&lake::Error::from(denied())));
    assert!(!lake::Error::is_retryable(&lake::Error::from(
        object_store::Error::NotFound {
            path: "p".into(),
            source: "no such bucket".into(),
        }
    )));
    assert!(!lake::Error::is_retryable(&lake::Error::from(
        parquet::errors::ParquetError::External(Box::new(std::io::Error::other(denied())))
    )));
    assert!(!lake::Error::is_retryable(&lake::Error::from(
        parquet::errors::ParquetError::External(Box::new(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied
        )))
    )));
}

/// Scenario: every write is refused as `PermissionDenied` under a 60s deadline.
/// Guarantees: the block fails on its first attempt with the fixed `rejected by the store` sentence
/// and no store text.
#[tokio::test(flavor = "current_thread")]
async fn a_permission_error_is_not_retried_until_the_deadline() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Denied);
            let (handler, mut rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_secs(60);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store, wall, handler);

            worker.admit(logs_pdata());
            worker.rotate();
            let ticker = ticking(&sim, Duration::from_millis(100));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            drop(ticker);
            let finished = done.as_ref().expect("the flush resolves");
            assert_eq!(finished.attempts, 1, "a refused credential is not retried");
            let error = finished.result.as_ref().expect_err("the write is refused");
            assert!(error.to_string().contains("access denied"), "{error}");
            worker.complete(done);
            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a nack") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(
                        nack.reason,
                        "could not write to object storage (rejected by the store); retry the \
                         request"
                    );
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            assert_no_more_completions(&mut rx);
            drain_cleanup(&mut worker).await;
        })
        .await;
}

/// Scenario: the store panics inside a flush, so the flush task unwinds and
/// its result never arrives.
/// Guarantees: every request of the block is nacked, retryable, with the
/// internal-error sentence, not the object-storage one.
#[tokio::test(flavor = "current_thread")]
async fn a_panicked_flush_task_is_nacked_as_an_internal_error() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = fault_store();
            store.hooks().set(Fault::Panic);
            let (handler, mut rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store, wall, handler);
            worker.admit(logs_pdata());
            worker.rotate();
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert!(done.is_err(), "the task unwound without a result");
            worker.complete(done);
            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a nack") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(
                        nack.reason,
                        "series_parquet hit an internal error handling the request; retry the \
                         request"
                    );
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            assert_no_more_completions(&mut rx);
            let mut job = worker
                .cleaning
                .take()
                .expect("the unwound job holds the slot");
            assert!(job.cleanup().await.is_err_and(|e| e.is_panic()));
        })
        .await;
}

/// Scenario: a first attempt fails with an error no retry can cure.
/// Guarantees: it is logged by the per-attempt WARN with its attempt number and error.
#[tokio::test(flavor = "current_thread")]
async fn a_non_retryable_failed_attempt_is_logged_at_warn() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = fault_store();
            store.hooks().set(Fault::Denied);
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store, wall, handler);
            worker.admit(logs_pdata());
            worker.rotate();
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert_eq!(done.as_ref().expect("the flush resolves").attempts, 1);
        })
        .await;
    let logged = events.named("series_parquet.flush.attempt_failed");
    assert_eq!(logged.len(), 1, "one failed attempt, one WARN: {logged:?}");
    let event = &logged[0];
    assert_eq!(event.level, tracing::Level::WARN);
    assert_eq!(event.fields.get("attempt"), Some(&FieldValue::U64(1)));
    assert_eq!(
        event.fields.get("retryable"),
        Some(&FieldValue::Bool(false)),
        "a refused credential is not retryable"
    );
    assert!(
        event
            .fields
            .get("error")
            .is_some_and(|error| error.text().contains("access denied")),
        "{event:?}"
    );
}

/// Scenario: the last object is written in the engine-clock step in which the deadline expires.
/// Guarantees: the flush reports success and the block is acknowledged.
#[tokio::test(flavor = "current_thread")]
async fn a_write_finishing_as_the_deadline_expires_is_a_success() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, _rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(20);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);

            worker.admit(logs_pdata());
            worker.rotate();
            // The series object parks first and is released well before the
            // deadline; then the values object parks, and is released in the
            // same step as the deadline.
            store.hooks().entered.notified().await;
            store.hooks().release.notify_one();
            store.hooks().entered.notified().await;
            store.hooks().release.notify_one();
            sim.advance(Duration::from_millis(20));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            let finished = done.as_ref().expect("the flush resolves");
            assert!(
                finished.result.is_ok(),
                "a finished write is not a deadline expiry: {:?}",
                finished.result.as_ref().err()
            );
        })
        .await;
}

/// Scenario: a values multipart upload wedges, parts and abort never returning, until the deadline.
/// Guarantees: the decision is published at the deadline, the cleanup ends at its allowance, no
/// object is left, and one `abort_failed` cleanup WARN is counted.
#[tokio::test(flavor = "current_thread")]
async fn a_wedged_multipart_abort_is_bounded_and_leaves_no_object() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let store = fault_store();
            store.hooks().set(Fault::MultipartWedge);
            let (handler, _rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(20);
            cfg.lake.upload.abort_timeout = Duration::from_secs(1);
            // Set past validation: a small part size reaches the multipart
            // calls with a cheap block, the small row group makes the writer
            // push parts while writing, and the small merge chunk keeps it
            // blocked on the wedged part, where an abort is attempted, when
            // the deadline expires.
            cfg.lake.upload.part_bytes = 4096;
            cfg.lake.upload.concurrency = 1;
            cfg.lake.parquet.row_group_bytes = 4096;
            cfg.lake.sorting.merge_chunk_bytes = 4096;
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(bulk_logs_pdata(20_000));
            worker.rotate();
            until("the wedged upload takes a part", || {
                store.hooks().parts.load(SeqCst) > 0
            })
            .await;

            // Only the retry deadline has elapsed: the abort allowance is
            // untouched, so a decision that arrives now cannot have waited for
            // the cleanup.
            sim.advance(Duration::from_millis(20));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert!(
                done.as_ref().expect("the flush resolves").result.is_err(),
                "the deadline fails the block"
            );
            worker.complete(done);
            assert_eq!(
                worker.notify.outcomes()[Outcome::Storage as usize],
                1,
                "the block is nacked as retryable storage"
            );

            // The slot the cleanup holds is the same FLUSHING slot.
            worker.rotate();
            assert!(worker.flushing.is_none(), "no second write is started");
            assert!(worker.cleaning.is_some(), "the slot is still occupied");
            until("the wedged upload is aborted", || {
                store.hooks().aborts.load(SeqCst) > 0
            })
            .await;

            let mut job = worker.cleaning.take().expect("the occupied slot");
            sim.advance(Duration::from_secs(2));
            job.cleanup().await.expect("the cleanup is bounded");
            drop(job);

            let path = lake::sink::object_path(
                lake::schema::Dataset::LogsValues,
                lake::clock::PartitionId::from_unix_secs(0),
                0,
                &lake::sink::FileNaming::new(&worker.cfg.lake.writer_id),
                1,
            );
            assert!(
                store.inner().head(&path).await.is_err(),
                "a wedged upload never completes an object"
            );
            worker.sample_metrics();
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(metrics.worker.flush_abort_failures.get(), 1);
            assert_eq!(all_late_commits(metrics), 0);
            assert_eq!(flush_failures(metrics, WriteFailure::Deadline), 1);
        })
        .await;
    let cleanup = events.named("series_parquet.flush.cleanup");
    assert_eq!(cleanup.len(), 1, "{cleanup:?}");
    let event = &cleanup[0];
    assert_eq!(event.level, tracing::Level::WARN);
    let field = |name: &str| event.fields.get(name).cloned();
    assert_eq!(
        field("outcome"),
        Some(FieldValue::Str("abort_failed".into()))
    );
    assert_eq!(field("seq"), Some(FieldValue::U64(0)));
    assert_eq!(field("attempt"), Some(FieldValue::U64(1)));
    assert!(
        field("file").is_some_and(|file| file.text().ends_with(".parquet")),
        "{event:?}"
    );
    assert!(
        field("abort_error").is_some_and(|error| !error.text().is_empty()),
        "{event:?}"
    );
}

/// A cleanup trace for block 7 over the two objects of `part-x.parquet`, on
/// `store`.
fn cleanup_trace(
    store: Arc<dyn ObjectStore>,
) -> (
    super::super::flush::Trace,
    std::rc::Rc<super::super::flush::FlushShared>,
    Vec<object_store::path::Path>,
) {
    let paths: Vec<object_store::path::Path> = ["dataset=series", "dataset=values"]
        .into_iter()
        .map(|dir| object_store::path::Path::from(format!("{dir}/part-x.parquet")))
        .collect();
    let shared = std::rc::Rc::new(super::super::flush::FlushShared {
        store,
        tally: super::super::flush::FlushTally::default(),
    });
    let trace = super::super::flush::Trace::for_test(paths.clone(), 7, std::rc::Rc::clone(&shared));
    (trace, shared, paths)
}

/// Scenario: every way a failed block's cleanup can end: completed anyway, cancelled with none, one
/// or both objects present, abort timed out, abort failed, not unwound, probe unfinished.
/// Guarantees: each outcome gets its level (`stored`, `aborted`, `partial`, `abort_failed`,
/// `unknown`) and counter, with sequence, attempt and file, an `abort_failed` naming the object
/// keys; `flush.late_commits` tells a nacked
/// block found stored from a partial one and from one the probe could not settle.
#[tokio::test(flavor = "current_thread")]
async fn every_cleanup_outcome_is_logged_and_counted() {
    let events = capture();
    let store = Arc::new(object_store::memory::InMemory::new());
    let (trace, shared, paths) = cleanup_trace(store.clone());
    let later = || clock::now() + Duration::from_secs(60);
    let cancelled = || Some(Err(lake::Error::cancelled(None)));
    trace
        .cleaned_up(2, Some(Ok(lake::sink::FlushReport::default())), later())
        .await;
    trace.cleaned_up(2, cancelled(), later()).await;
    let _ = store
        .put(&paths[0], PutPayload::from_static(b"series"))
        .await
        .expect("put");
    trace.cleaned_up(2, cancelled(), later()).await;
    let _ = store
        .put(&paths[1], PutPayload::from_static(b"values"))
        .await
        .expect("put");
    trace.cleaned_up(2, cancelled(), later()).await;
    trace
        .cleaned_up(
            2,
            Some(Err(lake::Error::cancelled(Some("abort timed out".into())))),
            later(),
        )
        .await;
    trace
        .cleaned_up(
            2,
            Some(Err(lake::Error::Transient(
                lake::TransientError::AbortFailed {
                    source: Box::new(lake::Error::internal("encode")),
                    abort_error: "abort refused".into(),
                },
            ))),
            later(),
        )
        .await;
    trace.cleaned_up(2, None, later()).await;
    // A store whose HEAD takes an hour, probed with a cutoff that has
    // already passed.
    let slow = object_store::throttle::ThrottledStore::new(
        object_store::memory::InMemory::new(),
        object_store::throttle::ThrottleConfig {
            wait_get_per_call: Duration::from_secs(3600),
            ..Default::default()
        },
    );
    let (slow_trace, slow_shared, _) = cleanup_trace(Arc::new(slow));
    slow_trace.cleaned_up(2, cancelled(), clock::now()).await;

    let late = |shared: &super::super::flush::FlushShared| {
        LateCommit::ALL.map(|outcome| shared.tally.late_commits[outcome as usize].get())
    };
    // Stored, acknowledged, partial, unknown.
    assert_eq!(late(&shared), [2, 0, 1, 0]);
    assert_eq!(shared.tally.abort_failures.get(), 3);
    assert_eq!(late(&slow_shared), [0, 0, 0, 1]);
    assert_eq!(slow_shared.tally.abort_failures.get(), 0);
    let logged = events.named("series_parquet.flush.cleanup");
    let summary: Vec<_> = logged
        .iter()
        .map(|event| {
            let text = |name: &str| event.fields.get(name).map(|v| v.text().to_owned());
            (event.level, text("outcome"), text("abort_error"))
        })
        .collect();
    let event = |level, outcome: &str, abort: Option<&str>| {
        (level, Some(outcome.to_owned()), abort.map(str::to_owned))
    };
    use tracing::Level;
    assert_eq!(
        summary,
        [
            event(Level::INFO, "stored", None),
            event(Level::DEBUG, "aborted", None),
            event(Level::INFO, "partial", None),
            event(Level::INFO, "stored", None),
            event(Level::WARN, "abort_failed", Some("abort timed out")),
            event(Level::WARN, "abort_failed", Some("abort refused")),
            event(
                Level::WARN,
                "abort_failed",
                Some(
                    "multipart uploads of dataset=series/part-x.parquet, \
                     dataset=values/part-x.parquet: the write did not unwind by the cleanup cutoff"
                )
            ),
            event(Level::WARN, "unknown", None),
        ]
    );
    assert_eq!(logged[2].fields.get("present"), Some(&FieldValue::U64(1)));
    assert_eq!(logged[2].fields.get("objects"), Some(&FieldValue::U64(2)));
    assert!(
        logged[7]
            .fields
            .get("probe_error")
            .is_some_and(|error| error.text().contains("deadline passed")),
        "{:?}",
        logged[7]
    );
    for event in &logged {
        assert_eq!(event.fields.get("seq"), Some(&FieldValue::U64(7)));
        assert_eq!(event.fields.get("attempt"), Some(&FieldValue::U64(2)));
        assert_eq!(
            event.fields.get("file").map(FieldValue::text),
            Some("part-x.parquet")
        );
    }
}

/// Scenario: a multipart upload completes at the store and its response is lost until the deadline.
/// Guarantees: a retryable storage nack and one `stored` cleanup; `flush.late_commits` is 1.
#[tokio::test(flavor = "current_thread")]
async fn a_completed_upload_whose_response_is_lost_is_a_late_commit() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let store = fault_store();
            store.hooks().set(Fault::LostComplete);
            let (handler, _rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(20);
            cfg.lake.upload.abort_timeout = Duration::from_secs(1);
            // Past validation on purpose, as in the wedged-upload test: a
            // small part size puts the values file on the multipart path at
            // a block size a unit test can build.
            cfg.lake.upload.part_bytes = 4096;
            cfg.lake.upload.concurrency = 1;
            cfg.lake.parquet.row_group_bytes = 4096;
            cfg.lake.sorting.merge_chunk_bytes = 4096;
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(bulk_logs_pdata(20_000));
            worker.rotate();
            until("the values upload is completed in the store", || {
                store.hooks().completes.load(SeqCst) > 0
            })
            .await;
            sim.advance(Duration::from_millis(20));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert!(
                done.as_ref().expect("the flush resolves").result.is_err(),
                "the lost response fails the block"
            );
            worker.complete(done);
            assert_eq!(worker.notify.outcomes()[Outcome::Storage as usize], 1);
            let mut job = worker.cleaning.take().expect("the cleanup slot");
            job.cleanup().await.expect("the cleanup is bounded");
            drop(job);
            worker.sample_metrics();
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(late_commits(metrics, LateCommit::Stored), 1);
            assert_eq!(all_late_commits(metrics), 1);
            assert_eq!(metrics.worker.flush_abort_failures.get(), 0);
        })
        .await;
    let cleanup = events.named("series_parquet.flush.cleanup");
    assert_eq!(cleanup.len(), 1, "{cleanup:?}");
    assert_eq!(cleanup[0].level, tracing::Level::INFO);
    assert_eq!(
        cleanup[0].fields.get("outcome"),
        Some(&FieldValue::Str("stored".into()))
    );
    assert!(
        cleanup[0]
            .fields
            .get("file")
            .is_some_and(|file| file.text().ends_with(".parquet")),
        "{cleanup:?}"
    );
}

/// Scenario: the first attempt's values multipart upload fails and its abort fails too, and the
/// retry succeeds.
/// Guarantees: the block is acknowledged and `flush.abort_failures` counts the upload the failed
/// attempt left behind, with one WARN `abort_failed` cleanup naming its key.
#[tokio::test(flavor = "current_thread")]
async fn an_upload_a_retried_attempt_leaves_behind_is_counted() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let store = fault_store();
            store.hooks().set(Fault::ValuesOrphanOnce);
            let (handler, mut rx) = effects(8);
            let mut cfg = worker_config();
            // Past validation on purpose, as in the wedged-upload test.
            cfg.lake.upload.part_bytes = 4096;
            cfg.lake.upload.concurrency = 1;
            cfg.lake.parquet.row_group_bytes = 4096;
            cfg.lake.sorting.merge_chunk_bytes = 4096;
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(bulk_logs_pdata(20_000));
            worker.rotate();
            until("the first attempt fails", || {
                !events
                    .named("series_parquet.flush.attempt_failed")
                    .is_empty()
            })
            .await;
            // The first retry waits 200 ms.
            sim.advance(Duration::from_millis(200));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            let finished = done.as_ref().expect("the flush resolves");
            assert_eq!(finished.attempts, 2);
            assert!(
                finished.result.is_ok(),
                "{:?}",
                finished.result.as_ref().err()
            );
            worker.complete(done);
            worker.notify.next().await.expect("the ack is sent");
            assert!(matches!(
                rx.recv().await.expect("ack"),
                PipelineCompletionMsg::DeliverAck { .. }
            ));
            assert_eq!(store.hooks().aborts.load(SeqCst), 1);
            worker.sample_metrics();
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(metrics.worker.flush_abort_failures.get(), 1);
            assert_eq!(all_late_commits(metrics), 0);
        })
        .await;
    let cleanup = events.named("series_parquet.flush.cleanup");
    assert_eq!(cleanup.len(), 1, "{cleanup:?}");
    assert_eq!(cleanup[0].level, tracing::Level::WARN);
    assert_eq!(
        cleanup[0].fields.get("outcome"),
        Some(&FieldValue::Str("abort_failed".into()))
    );
    assert!(
        cleanup[0]
            .fields
            .get("abort_error")
            .is_some_and(|error| error.text().contains("dataset=values/")),
        "{cleanup:?}"
    );
}

/// A worker whose values file takes the multipart path, over `store`, with
/// metrics registered.
fn multipart_worker(
    store: &Arc<FaultStore>,
    context: &otel_arrow_dfe_engine::context::PipelineContext,
    handler: EffectHandler<OtapPdata>,
) -> Worker {
    let mut cfg = worker_config();
    cfg.lake.upload.abort_timeout = Duration::from_secs(1);
    // Past validation on purpose, as in the wedged-upload test.
    cfg.lake.upload.part_bytes = 4096;
    cfg.lake.upload.concurrency = 1;
    cfg.lake.parquet.row_group_bytes = 4096;
    cfg.lake.sorting.merge_chunk_bytes = 4096;
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut worker = Worker::new(cfg, store.clone(), wall, handler);
    worker.metrics = Some(super::super::metrics::Metrics::register(
        context,
        &worker.cfg.lake,
    ));
    worker
}

/// Scenario: a values multipart completion is applied by the store, its response is lost, and the
/// abort that follows is answered `NotFound`, as RustFS answers it.
/// Guarantees: a HEAD finds the object, so the block is acknowledged on its first attempt with no
/// second upload; `flush.late_commits` is 1 with one INFO cleanup line of `outcome=acknowledged`.
#[tokio::test(flavor = "current_thread")]
async fn a_committed_upload_whose_abort_is_not_found_is_acknowledged() {
    a_committed_upload_is_acknowledged(Fault::CommittedCompleteTimesOut, true).await;
}

/// Scenario: a values multipart completion is applied by the store and then answers with a
/// retryable error, on a store that also accepts the abort that follows (MinIO answers 204).
/// Guarantees: on the block's first attempt nothing else can have written its frozen names, so
/// the found object is this completion's commit: the block is acknowledged with no second upload
/// and counted as a late commit (`outcome=acknowledged`) with one INFO cleanup line of the same
/// outcome.
#[tokio::test(flavor = "current_thread")]
async fn a_first_attempts_found_object_whose_abort_succeeds_is_a_late_commit() {
    a_committed_upload_is_acknowledged(Fault::FailedComplete, true).await;
}

/// Drive one block through `fault`, a completion the store applies and whose
/// response is lost, and check it is acknowledged, as a late commit when
/// `late`.
async fn a_committed_upload_is_acknowledged(fault: Fault, late: bool) {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let store = fault_store();
            store.hooks().set(fault);
            let (handler, mut rx) = effects(8);
            let mut worker = multipart_worker(&store, &context, handler);

            worker.admit(bulk_logs_pdata(20_000));
            worker.rotate();
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            let finished = done.as_ref().expect("the flush resolves");
            assert_eq!(finished.attempts, 1, "no second attempt");
            assert!(
                finished.result.is_ok(),
                "{:?}",
                finished.result.as_ref().err()
            );
            worker.complete(done);
            worker.notify.next().await.expect("the ack is sent");
            assert!(matches!(
                rx.recv().await.expect("ack"),
                PipelineCompletionMsg::DeliverAck { .. }
            ));
            assert_eq!(store.hooks().completes.load(SeqCst), 1, "one upload");
            worker.sample_metrics();
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(
                late_commits(metrics, LateCommit::Acknowledged),
                u64::from(late)
            );
            assert_eq!(all_late_commits(metrics), u64::from(late));
            assert_eq!(metrics.worker.flush_abort_failures.get(), 0);
        })
        .await;
    let cleanup = events.named("series_parquet.flush.cleanup");
    if !late {
        assert!(cleanup.is_empty(), "{cleanup:?}");
        return;
    }
    assert_eq!(cleanup.len(), 1, "{cleanup:?}");
    assert_eq!(cleanup[0].level, tracing::Level::INFO);
    assert_eq!(
        cleanup[0].fields.get("outcome"),
        Some(&FieldValue::Str("acknowledged".into()))
    );
}

/// Scenario: a values multipart completion is lost before the store applies it, and its abort is
/// answered `NotFound`; the store then heals.
/// Guarantees: the completion's own retryable error decides the attempt, so the block is retried
/// under the same names and acknowledged; no late commit and no abort failure are counted.
#[tokio::test(flavor = "current_thread")]
async fn an_uncommitted_upload_whose_abort_is_not_found_is_retried() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let store = fault_store();
            store.hooks().set(Fault::UncommittedCompleteTimesOutOnce);
            let (handler, mut rx) = effects(8);
            let mut worker = multipart_worker(&store, &context, handler);

            worker.admit(bulk_logs_pdata(20_000));
            worker.rotate();
            until("the first attempt fails", || {
                !events
                    .named("series_parquet.flush.attempt_failed")
                    .is_empty()
            })
            .await;
            // The first retry waits 200 ms.
            sim.advance(Duration::from_millis(200));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            let finished = done.as_ref().expect("the flush resolves");
            assert_eq!(finished.attempts, 2, "the block is retried");
            assert!(
                finished.result.is_ok(),
                "{:?}",
                finished.result.as_ref().err()
            );
            worker.complete(done);
            worker.notify.next().await.expect("the ack is sent");
            assert!(matches!(
                rx.recv().await.expect("ack"),
                PipelineCompletionMsg::DeliverAck { .. }
            ));
            worker.sample_metrics();
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(all_late_commits(metrics), 0);
            assert_eq!(metrics.worker.flush_abort_failures.get(), 0);
        })
        .await;
}

/// Scenario: one block written on its first attempt.
/// Guarantees: `series_parquet.block.committed` carries window start, sequence, first path and
/// attempts.
#[tokio::test(flavor = "current_thread")]
async fn a_committed_block_names_its_window_sequence_and_path() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(
                worker_config(),
                Arc::new(object_store::memory::InMemory::new()),
                wall,
                handler,
            );
            worker.admit(logs_pdata());
            let window = worker.active.data.window_start_secs;
            let seq = worker.active.data.seq;
            worker.rotate();
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            let path = done
                .as_ref()
                .expect("the flush task joins")
                .result
                .as_ref()
                .expect("the write succeeds")
                .files[0]
                .1
                .to_string();
            worker.complete(done);

            let logged = events.named("series_parquet.block.committed");
            assert_eq!(logged.len(), 1, "{logged:?}");
            let field = |name: &str| logged[0].fields.get(name).cloned();
            assert_eq!(field("window_start"), Some(FieldValue::I64(window)));
            assert_eq!(field("seq"), Some(FieldValue::U64(seq)));
            assert_eq!(field("path"), Some(FieldValue::Str(path)));
            assert_eq!(field("attempts"), Some(FieldValue::U64(1)));
        })
        .await;
}
