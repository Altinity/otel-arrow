// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Metrics and events: what the worker reports and when it samples.

use super::support::*;

/// Scenario: two requests whose route asks for the payload back with a nack, as
/// `processor:retry` does.
/// Guarantees: `series_parquet.payload.not_returned` is logged once at WARN, not per request.
#[test]
fn a_route_that_expects_its_payload_back_is_warned_about_once() {
    let events = capture();
    for _ in 0..2 {
        let data = logs_pdata().test_subscribe_to(
            Interests::NACKS | Interests::RETURN_DATA,
            otel_arrow_dfe_engine::control::CallData::default(),
            3,
        );
        let (token, _payload) = AckToken::split(data);
        token.discard();
    }
    let warned = events.named("series_parquet.payload.not_returned");
    assert_eq!(warned.len(), 1, "{warned:?}");
    assert_eq!(warned[0].level, tracing::Level::WARN);
}

/// Scenario: telemetry collected after an admission, while a notification waits.
/// Guarantees: the gauges count live requests and every worker instrument is present.
#[tokio::test(flavor = "current_thread")]
async fn worker_metrics_cover_live_memory_and_requests() {
    let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
    let (handler, _rx) = effects(4);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut worker = Worker::new(
        worker_config(),
        Arc::new(object_store::memory::InMemory::new()),
        wall,
        handler,
    );
    worker.metrics = Some(super::super::metrics::Metrics::register(
        &context,
        &worker.cfg.lake,
    ));

    worker.admit(logs_pdata());
    worker.sample_metrics();

    let metrics = worker.metrics.as_ref().expect("registered");
    assert_eq!(metrics.worker.requests_pending.get(), 1);
    assert!(metrics.worker.memory_accounted_bytes.get() >= worker.active.data.bytes as u64);
    assert!(
        metrics.worker.memory_budget_bytes.get() >= metrics.worker.memory_accounted_bytes.get()
    );
    assert_eq!(
        metrics.worker.snapshot().descriptor().name,
        "exporter.series_parquet"
    );
}

/// Scenario: one block is abandoned after admission and a later block commits.
/// Guarantees: `series.emitted` counts only the committed block's rows.
#[tokio::test(flavor = "current_thread")]
async fn series_emitted_requires_durable_completion() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(
                worker_config(),
                Arc::new(object_store::memory::InMemory::new()),
                wall,
                handler,
            );
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(logs_pdata());
            assert!(
                worker
                    .metrics
                    .as_mut()
                    .expect("metrics")
                    .emitted
                    .terminal_snapshots()
                    .is_empty(),
                "admission alone emits nothing"
            );
            worker.fail_active(Outcome::Storage);
            assert!(
                worker
                    .metrics
                    .as_mut()
                    .expect("metrics")
                    .emitted
                    .terminal_snapshots()
                    .is_empty(),
                "an abandoned block emits nothing"
            );

            worker.admit(logs_pdata());
            let expected = worker.active.data.pending_series.len() as u64;
            worker.rotate();
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert!(done.as_ref().expect("the flush task joins").result.is_ok());
            assert!(
                worker
                    .metrics
                    .as_mut()
                    .expect("metrics")
                    .emitted
                    .terminal_snapshots()
                    .is_empty(),
                "a resolved write that has not been completed emits nothing"
            );

            worker.complete(done);
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(
                metrics
                    .emitted
                    .get(super::super::metrics::EmitAttrs {
                        reason: super::super::metrics::EmitReason::New
                    })
                    .series_emitted
                    .get(),
                expected
            );
        })
        .await;
}

/// Scenario: a node takes several requests, one telemetry collection and a shutdown.
/// Guarantees: the worker scans itself once per collection and once for the terminal handoff, never
/// per loop turn.
#[tokio::test(flavor = "current_thread")]
async fn telemetry_is_scanned_only_when_it_is_collected() {
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
            let (handler, _rx) = effects(64);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (_reporter_rx, metrics_reporter) =
                otel_arrow_dfe_telemetry::reporter::MetricsReporter::create_new_and_receiver(16);

            for _ in 0..3 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("a request enqueues");
            }
            control_tx
                .send_async(NodeControlMsg::CollectTelemetry { metrics_reporter })
                .await
                .expect("the collection enqueues");
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            // Closing the upstream channel releases the latched shutdown once
            // the backlog has been drained.
            drop(pdata_tx);

            let mut worker = Worker::new(
                worker_config(),
                Arc::new(object_store::memory::InMemory::new()),
                Arc::new(lake::clock::TestWallClock::new(0)),
                handler,
            );
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));
            let terminal = tokio::time::timeout(
                Duration::from_secs(5),
                super::super::drive(&mut worker, inbox),
            )
            .await
            .expect("the node returns")
            .expect("the node does not fail");
            assert!(
                !terminal.metrics().is_empty(),
                "the handoff carries metrics"
            );
            assert_eq!(
                worker.samples, 2,
                "one scan for the collection and one for the terminal handoff"
            );
            drop(control_tx);
        })
        .await;
}

/// Scenario: the shutdown deadline elapses while a write is outstanding.
/// Guarantees: one `block.write_failures{error.type=cancelled}`, one cancellation and one duration are
/// counted.
#[tokio::test(flavor = "current_thread")]
async fn an_abandoned_flush_is_counted_as_cancelled() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(
                worker_config(),
                Arc::new(object_store::memory::InMemory::new()),
                wall,
                handler,
            );
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(logs_pdata());
            worker.rotate();
            assert!(worker.flushing.is_some(), "a write is outstanding");

            let _ = worker.abandon().await;
            assert_eq!(worker.abandoned, 1, "the one admitted request");
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(flush_failures(metrics, WriteFailure::Cancelled), 1);
            assert_eq!(metrics.worker.flush_cancelled.get(), 1);
            assert_eq!(metrics.worker.flush_duration.count, 1);
        })
        .await;
}

/// Scenario: a fresh worker sampled, then one request flushed and acknowledged, one refused by
/// the extraction budget, and one abandoned at a shutdown deadline already passed, each delivered.
/// Guarantees: after every decision `memory.accounted` is back to the fresh value plus what an
/// idle worker retains (the series cache and the notifier's emptied queue buffer): a decided
/// request keeps no bytes accounted.
#[tokio::test(flavor = "current_thread")]
async fn memory_accounted_returns_to_its_baseline_after_every_decision() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (handler, mut rx) = effects(8);
            let mut worker = Worker::new(
                worker_config(),
                Arc::new(object_store::memory::InMemory::new()),
                Arc::new(lake::clock::TestWallClock::new(0)),
                handler,
            );
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));
            let accounted = |worker: &mut Worker| {
                worker.sample_metrics();
                worker
                    .metrics
                    .as_ref()
                    .expect("registered")
                    .worker
                    .memory_accounted_bytes
                    .get()
            };
            // What an idle worker keeps between requests: the series cache and
            // the notifier's emptied queue buffer, both bounded by configuration.
            let retained = |worker: &Worker| {
                assert!(worker.is_idle());
                worker.cache.len() as u64 * super::super::worker::CACHE_ENTRY_BYTES
                    + worker.notify.bytes() as u64
            };
            let baseline = accounted(&mut worker);

            worker.admit(logs_pdata());
            assert!(
                accounted(&mut worker) > baseline,
                "an admitted request is charged"
            );
            worker.rotate();
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            worker.complete(done);
            worker
                .cleaning
                .as_mut()
                .expect("the decided block's task")
                .cleanup()
                .await
                .expect("the flush task joins");
            let _ = worker.cleaning.take();
            worker.notify.next().await.expect("the ack is delivered");
            assert!(matches!(
                rx.recv().await.expect("a completion"),
                PipelineCompletionMsg::DeliverAck { .. }
            ));
            assert!(!worker.cache.is_empty(), "the stored series is cached");
            assert_eq!(
                accounted(&mut worker),
                baseline + retained(&worker),
                "after an ack"
            );

            let limit = worker.cfg.lake.ingress.max_extracted_bytes;
            worker.cfg.lake.ingress.max_extracted_bytes = 100;
            worker.admit(logs_pdata());
            worker.cfg.lake.ingress.max_extracted_bytes = limit;
            worker
                .notify
                .next()
                .await
                .expect("the refusal is delivered");
            assert!(matches!(
                rx.recv().await.expect("a completion"),
                PipelineCompletionMsg::DeliverNack { .. }
            ));
            assert_eq!(
                accounted(&mut worker),
                baseline + retained(&worker),
                "after a nack"
            );

            worker.admit(logs_pdata());
            worker.rotate();
            let passed = clock::now()
                .checked_sub(Duration::from_secs(1))
                .expect("an instant one second in the past");
            worker.shutdown(passed);
            let _ = worker.abandon().await;
            assert!(matches!(
                rx.recv().await.expect("a completion"),
                PipelineCompletionMsg::DeliverNack { .. }
            ));
            assert_eq!(
                accounted(&mut worker),
                baseline + retained(&worker),
                "after an abandoned flush"
            );
        })
        .await;
}

/// Scenario: a full block, a parked request, and released completions in the notifier.
/// Guarantees: the parked and notification bytes have their own gauges and are in the accounted
/// total.
#[tokio::test(flavor = "current_thread")]
async fn pending_and_notification_bytes_are_reported() {
    let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
    let (handler, _rx) = effects(8);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut worker = Worker::new(
        worker_config_with_requests(1),
        Arc::new(object_store::memory::InMemory::new()),
        wall,
        handler,
    );
    worker.metrics = Some(super::super::metrics::Metrics::register(
        &context,
        &worker.cfg.lake,
    ));

    worker.admit(logs_pdata());
    worker.admit(logs_pdata());
    assert!(worker.pending.is_some(), "the second request is parked");
    // Releases the first block's completion into the notifier without needing
    // a write to resolve.
    worker.fail_active(Outcome::Storage);
    assert_eq!(worker.notify.len(), 1);

    worker.sample_metrics();
    let metrics = worker.metrics.as_ref().expect("metrics");
    let pending = metrics.worker.pending_bytes.get();
    let tokens = metrics.worker.notify_token_bytes.get();
    assert!(pending > 0, "the parked request retains rows and a token");
    assert!(
        tokens > 0,
        "an undelivered completion retains its queue cell"
    );
    assert!(metrics.worker.memory_accounted_bytes.get() >= pending + tokens);
    assert_eq!(metrics.worker.pending_slot.get(), 1);
}

/// Scenario: the admission gate closes for a rotation, reopens, and later closes at shutdown.
/// Guarantees: `admission.closed`, `admission.closures` and `admission.closed.duration` track the
/// backpressure closure only.
#[tokio::test(flavor = "current_thread")]
async fn admission_closure_is_visible_in_metrics() {
    let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
    let (handler, _rx) = effects(8);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut worker = Worker::new(
        worker_config(),
        Arc::new(object_store::memory::InMemory::new()),
        wall,
        handler,
    );
    worker.set_metrics(Some(super::super::metrics::Metrics::register(
        &context,
        &worker.cfg.lake,
    )));
    let sample = |worker: &mut Worker| {
        worker.sample_metrics();
        let m = &worker.metrics.as_ref().expect("metrics").worker;
        (
            m.admission_closed.get(),
            m.admission_closures.get(),
            m.admission_closed_duration.get(),
        )
    };

    worker.observe_admission(true);
    assert_eq!(sample(&mut worker), (0, 0, 0.0));

    worker.rotation_requested = true;
    for _ in 0..3 {
        let accept = worker.accept();
        assert!(!accept, "a pending rotation closes admission");
        worker.observe_admission(accept);
    }
    std::thread::sleep(Duration::from_millis(5));
    let (closed, closures, secs) = sample(&mut worker);
    assert_eq!(
        (closed, closures),
        (1, 1),
        "one closure, however many turns"
    );
    assert!(secs >= 0.005, "the closure in progress is counted: {secs}");

    worker.rotation_requested = false;
    worker.observe_admission(worker.accept());
    let (closed, closures, reopened) = sample(&mut worker);
    assert_eq!((closed, closures), (0, 1));
    assert!(reopened >= secs, "time closed only accumulates");

    worker.shutdown(clock::now() + Duration::from_secs(30));
    worker.observe_admission(worker.accept());
    let (closed, closures, _) = sample(&mut worker);
    assert_eq!(
        (closed, closures),
        (0, 1),
        "a gate closed by shutdown is not backpressure"
    );
}

/// Scenario: a refused traces request and a force-drained logs request at shutdown.
/// Guarantees: `exporter.exports` records `refused` and `failure` once each and is in the terminal
/// snapshots.
#[tokio::test(flavor = "current_thread")]
async fn decisions_are_recorded_in_the_shared_export_metrics() {
    let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
    let (handler, _rx) = effects(8);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut worker = Worker::new(
        worker_config(),
        Arc::new(object_store::memory::InMemory::new()),
        wall,
        handler,
    );
    worker.set_metrics(Some(super::super::metrics::Metrics::register(
        &context,
        &worker.cfg.lake,
    )));
    let mut traces = Context::default();
    traces.set_source_node(7);
    worker.admit(OtapPdata::new(traces, traces_payload()));
    worker.shutdown(clock::now() + Duration::from_secs(30));
    worker.force_shutdown(logs_pdata());

    let snapshots = worker.metric_snapshots();
    let exports = |signal: &str, outcome: &str| {
        snapshots
            .iter()
            .filter(|snapshot| snapshot.descriptor().name == "exporter.exports")
            .find(|snapshot| {
                snapshot.measurement_attributes().collect::<Vec<_>>()
                    == [("signal", signal), ("outcome", outcome)]
            })
            .map(|snapshot| {
                let index = snapshot
                    .descriptor()
                    .metrics
                    .iter()
                    .position(|metric| metric.name == "messages")
                    .expect("messages");
                snapshot.get_metrics()[index].to_u64_lossy()
            })
    };
    assert_eq!(exports("traces", "refused"), Some(1));
    assert_eq!(exports("logs", "failure"), Some(1));
    assert_eq!(exports("logs", "success"), None, "nothing was acked");
}

/// Scenario: a lake configuration with `writer_id` left unset, one set to the default's value
/// and one set to a pod name.
/// Guarantees: the start-up warning is due exactly for the default writer id, whether or not it
/// was written out.
#[test]
fn only_the_default_writer_id_is_warned_about() {
    use super::super::writer_id_is_default;
    let mut cfg = otel_arrow_dfe_series_lake::config::LakeConfig::default();
    assert!(writer_id_is_default(&cfg));
    cfg.writer_id = "writer".into();
    assert!(writer_id_is_default(&cfg));
    cfg.writer_id = "series-parquet-0".into();
    assert!(!writer_id_is_default(&cfg));
}

/// Scenario: `MemTotal` of 16 GiB with a 4 GiB cgroup limit, a 64 GiB one, none, and a cgroup
/// limit without meminfo.
/// Guarantees: the lower of the two is the limit and names its source; either alone is used.
#[test]
fn memory_limit_prefers_a_lower_cgroup_limit() {
    use super::super::{MemoryLimitSource, memory_limit};
    const GIB: u64 = 1 << 30;
    let meminfo = Some("MemTotal:       16777216 kB\nMemFree:  1024 kB\n");
    assert_eq!(
        memory_limit(meminfo, Some(4 * GIB)),
        Some((4 * GIB, MemoryLimitSource::Cgroup))
    );
    assert_eq!(
        memory_limit(meminfo, Some(64 * GIB)),
        Some((16 * GIB, MemoryLimitSource::Meminfo))
    );
    assert_eq!(
        memory_limit(meminfo, None),
        Some((16 * GIB, MemoryLimitSource::Meminfo))
    );
    assert_eq!(
        memory_limit(None, Some(4 * GIB)),
        Some((4 * GIB, MemoryLimitSource::Cgroup))
    );
    assert_eq!(memory_limit(None, None), None);
}

/// Scenario: a worker on more cores than any host has memory for with file storage, then one on
/// a single core with S3 storage that signs `UNSIGNED-PAYLOAD`.
/// Guarantees: the start event names the worker and its effective `unsigned_payload`; the
/// oversubscription WARN fires only for the first and says a durable_buffer's share is not in it.
#[tokio::test(flavor = "current_thread")]
async fn start_up_announces_the_worker_and_warns_on_budgets_that_cannot_hold() {
    let events = capture();
    let (handler, _rx) = effects(8);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let worker = Worker::new(
        worker_config(),
        Arc::new(object_store::memory::InMemory::new()),
        Arc::clone(&wall) as _,
        handler,
    );
    super::super::announce(
        &worker,
        &super::super::Startup {
            storage: "file".to_owned(),
            num_cores: 1 << 20,
            unsigned_payload: None,
            first_in_process: true,
        },
    );
    let start = events.named("series_parquet.start");
    assert_eq!(start.len(), 1, "{start:?}");
    let field = |name: &str| start[0].fields.get(name).cloned();
    assert_eq!(
        field("writer_id").map(|v| v.text().to_owned()),
        Some(worker.cfg.lake.writer_id.clone())
    );
    assert_eq!(
        field("boot_id").map(|v| v.text().to_owned()),
        Some(worker.boot_id.clone())
    );
    assert_eq!(field("storage"), Some(FieldValue::Str("file".into())));
    assert_eq!(
        field("unsigned_payload"),
        Some(FieldValue::Str("not_applicable".into()))
    );
    if super::super::process_memory_limit().is_some() {
        let warned = events.named("series_parquet.memory_budget.oversubscribed");
        assert_eq!(warned.len(), 1, "a million cores oversubscribe any host");
        assert_eq!(warned[0].level, tracing::Level::WARN);
        let message = warned[0].fields.get("message").map(|m| m.text().to_owned());
        assert!(
            message.is_some_and(|m| m.contains("durable_buffer")),
            "the WARN says the buffer's share is not counted: {:?}",
            warned[0]
        );
    }

    let (handler, _rx) = effects(8);
    let small = Worker::new(
        worker_config(),
        Arc::new(object_store::memory::InMemory::new()),
        wall,
        handler,
    );
    let before = events
        .named("series_parquet.memory_budget.oversubscribed")
        .len();
    super::super::announce(
        &small,
        &super::super::Startup {
            storage: "s3".to_owned(),
            num_cores: 1,
            unsigned_payload: Some(true),
            first_in_process: true,
        },
    );
    let start = events.named("series_parquet.start");
    assert_eq!(start.len(), 2);
    assert_eq!(
        start[1].fields.get("unsigned_payload"),
        Some(&FieldValue::Str("on".into()))
    );
    assert_eq!(
        events
            .named("series_parquet.memory_budget.oversubscribed")
            .len(),
        before,
        "one core's budget fits the host"
    );
}

/// Scenario: two workers of one process started with the default `ingress.max_request_bytes`
/// and the default `writer_id`.
/// Guarantees: each states that the receiver's decoding limit cannot be seen from the exporter,
/// with the request limit it must reach and the receiver's 4MiB default; the first worker of the
/// process says it at INFO and warns about the default `writer_id` at WARN, the later one says
/// both at DEBUG.
#[tokio::test(flavor = "current_thread")]
async fn start_up_states_that_the_receiver_limit_is_not_visible() {
    let events = capture();
    for first_in_process in [true, false] {
        let (handler, _rx) = effects(8);
        let worker = Worker::new(
            worker_config(),
            Arc::new(object_store::memory::InMemory::new()),
            Arc::new(lake::clock::TestWallClock::new(0)),
            handler,
        );
        super::super::announce(
            &worker,
            &super::super::Startup {
                storage: "file".to_owned(),
                num_cores: 1,
                unsigned_payload: None,
                first_in_process,
            },
        );
    }
    let stated = events.named("series_parquet.receiver_limit.unverified");
    assert_eq!(stated.len(), 2, "{stated:?}");
    assert_eq!(stated[0].level, tracing::Level::INFO);
    assert_eq!(stated[1].level, tracing::Level::DEBUG);
    let warned = events.named("series_parquet.writer_id.default");
    assert_eq!(warned.len(), 2, "{warned:?}");
    assert_eq!(warned[0].level, tracing::Level::WARN);
    assert_eq!(warned[1].level, tracing::Level::DEBUG);
    for event in &stated {
        let field = |name: &str| event.fields.get(name).cloned();
        assert_eq!(field("max_request_bytes"), Some(FieldValue::U64(16 << 20)));
        assert_eq!(
            field("receiver_default_bytes"),
            Some(FieldValue::U64(4 << 20))
        );
    }
}

/// Scenario: a 60GiB block with 5MiB parts (12,288 parts), then the defaults (63 parts).
/// Guarantees: only the first emits `series_parquet.upload.parts_exceed_limit` with both settings
/// and both counts.
#[tokio::test(flavor = "current_thread")]
async fn start_up_warns_when_a_block_could_exceed_the_multipart_part_limit() {
    let events = capture();
    let start = |cfg: Config| {
        let (handler, _rx) = effects(8);
        let worker = Worker::new(
            cfg,
            Arc::new(object_store::memory::InMemory::new()),
            Arc::new(lake::clock::TestWallClock::new(0)),
            handler,
        );
        super::super::announce(
            &worker,
            &super::super::Startup {
                storage: "file".to_owned(),
                num_cores: 1,
                unsigned_payload: None,
                first_in_process: true,
            },
        );
    };
    let big: Config = serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-unused"}},
        "window": {"max_block_bytes": "60GiB"},
        "upload": {"part_bytes": "5MiB"}
    }))
    .expect("valid config");
    start(big);
    let warned = events.named("series_parquet.upload.parts_exceed_limit");
    assert_eq!(warned.len(), 1, "{warned:?}");
    assert_eq!(warned[0].level, tracing::Level::WARN);
    let field = |name: &str| warned[0].fields.get(name).cloned();
    assert_eq!(field("max_block_bytes"), Some(FieldValue::U64(60 << 30)));
    assert_eq!(field("part_bytes"), Some(FieldValue::U64(5 << 20)));
    assert_eq!(field("parts"), Some(FieldValue::U64(12_288)));
    assert_eq!(field("max_parts"), Some(FieldValue::U64(10_000)));

    start(worker_config());
    assert_eq!(
        events
            .named("series_parquet.upload.parts_exceed_limit")
            .len(),
        1,
        "the defaults fit the part limit"
    );
}

/// Scenario: telemetry sampled while a flush's first write is held at the store gate, and after.
/// Guarantees: the held upload bytes appear in `flush.workspace` and `memory.accounted`, and are
/// zero afterwards.
#[tokio::test(flavor = "current_thread")]
async fn the_flush_workspace_is_charged_while_a_write_is_in_flight() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (handler, _rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let entered = Arc::new(AtomicUsize::new(0));
            let store = Arc::new(HookStore::new(
                Arc::new(object_store::memory::InMemory::new()),
                GatedStore {
                    gate: Arc::clone(&gate),
                    entered: Arc::clone(&entered),
                },
            ));
            let mut worker = Worker::new(worker_config(), store, wall, handler);
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));
            worker.admit(logs_pdata());
            worker.rotate();
            until("the first file write reaches the gate", || {
                entered.load(SeqCst) > 0
            })
            .await;
            worker.sample_metrics();
            let held = worker.sink.flush_workspace_bytes();
            assert!(held > 0, "the upload holds the file being written");
            let metrics = worker.metrics.as_ref().expect("registered");
            assert_eq!(metrics.worker.flush_workspace_bytes.get(), held as u64);
            let flushing = metrics.worker.flushing_bytes.get();
            assert!(flushing > 0, "the block is FLUSHING");
            assert!(
                metrics.worker.memory_accounted_bytes.get()
                    >= metrics.worker.active_bytes.get() + flushing + held as u64,
                "memory.accounted charges the flush workspace"
            );
            gate.add_permits(1 << 20);
            let done = worker
                .flushing
                .as_mut()
                .expect("a flush is running")
                .finish()
                .await;
            worker.complete(done);
            worker.sample_metrics();
            let metrics = worker.metrics.as_ref().expect("registered");
            assert_eq!(metrics.worker.flush_workspace_bytes.get(), 0);
            assert_eq!(worker.sink.flush_workspace_bytes(), 0);
        })
        .await;
}
