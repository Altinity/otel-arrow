// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Admission and parking: validation, extraction, refusals and the one
//! parked request.

use super::support::*;

/// Scenario: a flood of `too_deep` refusal lines, then one `internal` refusal inside the same
/// second, then `too_deep` again two seconds after the flood began.
/// Guarantees: each outcome has its own rate limit and suppressed count, so the `internal` line
/// is written although `too_deep` just used its interval, and the later `too_deep` line names
/// only its own suppressed lines.
#[test]
fn each_refusal_outcome_has_its_own_log_rate_limit() {
    use super::super::outcome::Outcome;
    use super::super::worker::RefusalGates;
    use std::time::{Duration, Instant};
    let start = Instant::now();
    let mut gates = RefusalGates::new();
    assert_eq!(gates.admit(Outcome::TooDeep, start), Some(0));
    assert_eq!(
        gates.admit(Outcome::TooDeep, start + Duration::from_millis(1)),
        None
    );
    assert_eq!(
        gates.admit(Outcome::TooDeep, start + Duration::from_millis(2)),
        None
    );
    assert_eq!(
        gates.admit(Outcome::Internal, start + Duration::from_millis(3)),
        Some(0)
    );
    assert_eq!(
        gates.admit(Outcome::TooDeep, start + Duration::from_secs(2)),
        Some(2)
    );
}

/// Scenario: the request-size check given a size over the limit, one at it, and no size at all,
/// which is what `num_bytes()` returns for Arrow records it cannot measure.
/// Guarantees: only a measured size over the limit is `request_too_large`; an unmeasurable
/// request is refused as `invalid` content.
#[test]
fn an_unmeasurable_request_is_invalid_not_too_large() {
    use super::super::worker::check_request_size;
    let limit = 16 << 20;
    let over = check_request_size(Some(limit + 1), limit).expect_err("over the limit");
    assert_eq!(Outcome::of(&over), Outcome::RequestTooLarge);
    check_request_size(Some(limit), limit).expect("at the limit");
    let unmeasured = check_request_size(None, limit).expect_err("no size");
    assert_eq!(Outcome::of(&unmeasured), Outcome::Invalid);
}

/// Scenario: a traces request and a logs request larger than `ingress.max_request_bytes`.
/// Guarantees: both are refused permanently with their rule and leave the ACTIVE block empty.
#[tokio::test(flavor = "current_thread")]
async fn validation_refusals_are_permanent() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store, wall, handler);

            let mut context = Context::default();
            context.set_source_node(7);
            worker.admit(OtapPdata::new(context, traces_payload()));
            // The same logs request that is admitted elsewhere, now larger
            // than the budget it is measured against.
            worker.cfg.lake.ingress.max_request_bytes = 1;
            worker.admit(logs_pdata());

            assert!(worker.active.data.is_empty());
            assert!(worker.active.tokens.is_empty());
            assert!(!worker.rotation_requested);
            // The reason is the sentence the producer sees: it names the
            // rule, the limit and the remedy, not a bare metric token.
            for reason in [
                "traces are not stored by series_parquet",
                "exceeds ingress.max_request_bytes (1 bytes); split the batch upstream",
            ] {
                assert!(worker.notify.next().await.is_ok());
                match rx.recv().await.expect("a refusal") {
                    PipelineCompletionMsg::DeliverNack { nack } => {
                        assert!(nack.permanent);
                        assert_eq!(nack.cause, NackCause::Refused);
                        assert!(nack.reason.contains(reason), "reason: {}", nack.reason);
                    }
                    other => panic!("expected a nack, got {other:?}"),
                }
            }
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: the store's root directory is replaced by a regular file (unwritable even for root,
/// unlike a chmod) before a block is written.
/// Guarantees: every request of the block is nacked as retryable and the descriptor stays
/// uncommitted.
#[tokio::test(flavor = "current_thread")]
async fn a_storage_failure_after_validation_is_retryable() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("temp dir");
            let root = dir.path().join("lake");
            std::fs::create_dir(&root).expect("create the lake root");
            let store = Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(&root)
                    .expect("local object store"),
            );
            // The store has already resolved its root, so swapping the
            // directory for a file breaks every write underneath it.
            std::fs::remove_dir_all(&root).expect("remove the lake root");
            std::fs::write(&root, b"not a directory").expect("put a file in its place");

            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            // A broken destination is retried until the block's deadline, so
            // the test gives it one reachable on a simulated clock.
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(1);
            let mut worker = Worker::new(cfg, store, wall, handler);

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
            let ticker = ticking(&sim, Duration::from_millis(50));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            drop(ticker);
            assert!(
                done.as_ref().expect("the flush task joins").result.is_err(),
                "the destination is not writable"
            );

            worker.complete(done);
            assert!(!worker.cache.is_committed(&id, partition));
            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a storage failure") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert!(nack.reason.contains("retry"), "reason: {}", nack.reason);
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: each failure class is turned into the outcome the notifier delivers.
/// Guarantees: size refusals keep their budget, nesting and unsupported keep theirs, other refusals
/// are invalid, retryable failures are storage.
#[test]
fn each_failure_class_maps_to_its_outcome() {
    assert_eq!(
        Outcome::of(&lake::Error::too_large(lake::SizeBudget::Row, 2, 1)),
        Outcome::RowTooLarge
    );
    for (budget, outcome) in [
        (lake::SizeBudget::Request, Outcome::RequestTooLarge),
        (lake::SizeBudget::Extracted, Outcome::ExtractedTooLarge),
        (lake::SizeBudget::Table, Outcome::ExtractedTooLarge),
        (lake::SizeBudget::Cell, Outcome::RowTooLarge),
    ] {
        assert_eq!(
            Outcome::of(&lake::Error::too_large(budget, 2, 1)),
            outcome,
            "{budget:?}"
        );
    }
    assert_eq!(
        Outcome::of(&lake::Error::Refused(lake::RefuseReason::TooDeep(32))),
        Outcome::TooDeep
    );
    assert_eq!(
        Outcome::of(&lake::Error::Refused(lake::RefuseReason::Unsupported(
            "signal".into()
        ))),
        Outcome::Unsupported
    );
    assert_eq!(
        Outcome::of(&lake::Error::invalid("undecodable pdata")),
        Outcome::Invalid
    );
    assert_eq!(
        Outcome::of(&lake::Error::from(object_store::Error::Generic {
            store: "test",
            source: "unreachable".into(),
        })),
        Outcome::Storage
    );
    assert_eq!(
        Outcome::of(&lake::Error::internal("flush failed")),
        Outcome::Internal
    );
}

/// Scenario: extraction fails on a writer invariant, classified and delivered as admission does.
/// Guarantees: a retryable `internal` nack whose reason carries the sanitized detail.
#[tokio::test(flavor = "current_thread")]
async fn an_internal_extraction_error_is_a_retryable_nack_with_detail() {
    let failure = lake::Error::internal("column/builder mismatch for Str(None)\nsecond line");
    assert!(matches!(failure, lake::Error::Internal(_)));
    assert_eq!(Outcome::of(&failure), Outcome::Internal);
    assert!(!Outcome::of(&failure).refused());
    let sentence = Outcome::explain(&failure);
    assert!(
        sentence.contains("column/builder mismatch for Str(None) second line"),
        "the detail is kept, on one line: {sentence}"
    );
    assert!(Outcome::of(&lake::Error::invalid("bad")).refused());

    let (handler, mut rx) = effects(1);
    let mut notify = Notifier::new(handler, 4);
    let (token, payload) = AckToken::split(empty_pdata());
    drop(payload);
    notify.push_with(token, Outcome::of(&failure), Some(sentence.clone().into()));
    notify.next().await.expect("the completion is accepted");
    match rx.recv().await.expect("a nack") {
        PipelineCompletionMsg::DeliverNack { nack } => {
            assert!(!nack.permanent);
            assert_ne!(nack.cause, NackCause::Refused);
            assert_eq!(nack.reason, sentence);
        }
        other => panic!("expected a nack, got {other:?}"),
    }
    assert_eq!(notify.outcomes()[Outcome::Internal as usize], 1);
    assert_no_more_completions(&mut rx);
}

/// Scenario: the values sort key is changed after validation to a missing column and the run target
/// is one byte, so the lake's run sort fails internally (no request content can break an extraction
/// invariant).
/// Guarantees: a retryable `internal` nack whose reason carries the lake's detail.
#[tokio::test(flavor = "current_thread")]
async fn a_real_writer_invariant_failure_is_a_retryable_nack_with_detail() {
    let (handler, mut rx) = effects(4);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut cfg = worker_config();
    cfg.lake.sorting.run_target_bytes = 1;
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
    worker.admit(logs_pdata());
    assert!(
        worker.active.tokens.is_empty(),
        "the request is not admitted"
    );
    assert_eq!(worker.notify.outcomes()[Outcome::Internal as usize], 1);
    worker
        .notify
        .next()
        .await
        .expect("the completion is accepted");
    match rx.recv().await.expect("a nack") {
        PipelineCompletionMsg::DeliverNack { nack } => {
            assert!(!nack.permanent);
            assert_ne!(nack.cause, NackCause::Refused);
            assert!(
                nack.reason.contains("sort column no_such_column missing")
                    && nack.reason.contains("retry"),
                "reason: {}",
                nack.reason
            );
        }
        other => panic!("expected a nack, got {other:?}"),
    }
    assert_no_more_completions(&mut rx);
}

/// Scenario: a metrics request is admitted, then a logs request's admission breaks a writer
/// invariant (its sort names a missing column).
/// Guarantees: the co-tenant is nacked as a retryable `internal` failure, not as storage.
#[tokio::test(flavor = "current_thread")]
async fn an_admission_failure_nacks_its_co_tenant_as_internal() {
    let (handler, mut rx) = effects(4);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut cfg = worker_config();
    cfg.lake.sorting.run_target_bytes = 1;
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
    let mut context = Context::default();
    context.set_source_node(1);
    worker.admit(OtapPdata::new(context, metrics_payload()));
    assert_eq!(
        worker.active.tokens.len(),
        1,
        "the metrics request is admitted"
    );
    worker.admit(logs_pdata_from(2));
    assert!(worker.active.tokens.is_empty(), "the block is failed");
    assert_eq!(worker.notify.outcomes()[Outcome::Internal as usize], 2);
    assert_eq!(worker.notify.outcomes()[Outcome::Storage as usize], 0);

    // The failing request is decided first, then the block it failed.
    for expected in [2, 1] {
        worker
            .notify
            .next()
            .await
            .expect("the completion is accepted");
        match rx.recv().await.expect("a nack") {
            PipelineCompletionMsg::DeliverNack { nack } => {
                assert!(!nack.permanent);
                assert_eq!(nack.cause, NackCause::Unspecified);
                if expected == 1 {
                    assert_eq!(nack.reason, Outcome::Internal.sentence());
                }
                assert_eq!((*nack.refused).into_parts().0.source_node(), Some(expected));
            }
            other => panic!("expected a nack, got {other:?}"),
        }
    }
    assert_no_more_completions(&mut rx);
}

/// Scenario: a detail longer than the bound, with control and multi-byte characters at the cut.
/// Guarantees: the reason detail is bounded, single-line and cut on a character boundary.
#[test]
fn a_reason_detail_is_bounded_and_single_line() {
    let long = "x".repeat(10_000);
    let cut = super::super::outcome::sanitized(&long);
    assert!(cut.len() <= 256 + 3);
    assert!(cut.ends_with("..."));
    let wide = "\u{e9}".repeat(1_000);
    let cut = super::super::outcome::sanitized(&wide);
    assert!(cut.len() <= 256 + 3);
    assert_eq!(super::super::outcome::sanitized("a\r\nb\tc"), "a  b c");
}

/// Scenario: an error whose text carries a line break and a terminal escape
/// sequence, as request-derived text in a lake error can, and one far longer
/// than the bound.
/// Guarantees: the refusal WARN's `error` field shows the control characters
/// escaped as Rust escapes them, keeps other characters, and is cut on a
/// character boundary.
#[test]
fn a_logged_error_escapes_control_characters() {
    use super::super::outcome::logged_error;
    let error = lake::Error::invalid("bad\nline \u{1b}[31mred \u{e9}");
    let logged = logged_error(&error);
    assert!(
        logged.ends_with("bad\\nline \\u{1b}[31mred \u{e9}"),
        "{logged}"
    );
    assert!(!logged.chars().any(char::is_control), "{logged}");
    let long = lake::Error::invalid("\u{e9}".repeat(10_000));
    let cut = logged_error(&long);
    assert!(cut.len() <= 1024 + 3);
    assert!(cut.ends_with("..."));
}

/// Scenario: the completion channel fills while a second notification waits.
/// Guarantees: a cancelled poll keeps the second context, sent exactly once later.
#[tokio::test(flavor = "current_thread")]
async fn notification_survives_cancelled_poll() {
    let (handler, mut rx) = effects(1);
    let mut notify = Notifier::new(handler, 2);

    // The first completion is handed over before the second is queued, so the
    // notifier never holds more normal completions than its cap allows.
    let (first, payload) = AckToken::split(empty_pdata());
    drop(payload);
    notify.push(first, Outcome::Ack);
    assert!(notify.next().await.is_ok());

    // The completion channel holds one message and nothing has read it, so
    // this second send cannot make progress.
    let (second, payload) = AckToken::split(empty_pdata());
    drop(payload);
    notify.push(second, Outcome::Ack);
    assert!(
        tokio::time::timeout(Duration::from_millis(5), notify.next())
            .await
            .is_err()
    );
    assert_eq!(notify.len(), 1);

    assert!(matches!(
        rx.recv().await.expect("first completion"),
        PipelineCompletionMsg::DeliverAck { .. }
    ));
    assert!(notify.next().await.is_ok());
    assert!(matches!(
        rx.recv().await.expect("second completion"),
        PipelineCompletionMsg::DeliverAck { .. }
    ));
    assert_eq!(notify.len(), 0);
    assert_no_more_completions(&mut rx);
}

/// Scenario: two requests fill a two-request block and a third needs the next one.
/// Guarantees: one request is parked, admission closes, and it enters the next block first.
#[tokio::test(flavor = "current_thread")]
async fn one_pending_request_resumes_before_new_input() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config_with_requests(2), store, wall, handler);

            worker.admit(logs_pdata());
            worker.admit(logs_pdata());
            worker.admit(logs_pdata());

            assert_eq!(worker.active.tokens.len(), 2);
            assert!(worker.pending.is_some());
            assert!(!worker.accept());

            worker.rotate();
            worker.resume_pending();
            assert!(worker.pending.is_none());
            assert_eq!(worker.active.tokens.len(), 1);
            assert_eq!(worker.live_tokens(), 3);
            assert!(worker.active.data.bytes <= worker.cfg.window.max_block_bytes);
        })
        .await;
}

/// Scenario: a request over the input budget is offered to an empty ACTIVE block.
/// Guarantees: a permanent refusal; nothing admitted or parked.
#[tokio::test(flavor = "current_thread")]
async fn oversized_input_is_refused_atomically() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(2);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.ingress.max_request_bytes = 1;
            let mut worker = Worker::new(cfg, store, wall, handler);

            worker.admit(logs_pdata());
            assert_eq!(worker.active.data.bytes, 0);
            assert!(worker.pending.is_none());

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(nack.permanent);
                    assert_eq!(nack.cause, NackCause::Refused);
                }
                other => panic!("expected a refusal, got {other:?}"),
            }
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: preparation consumes Arrow input still weakly observed from outside.
/// Guarantees: the parked extraction retains no original array and no conversion batch.
#[tokio::test(flavor = "current_thread")]
async fn prepare_releases_original_arrow_payload() {
    use otel_arrow_dfe_pdata::TryIntoWithOptions;
    use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
    use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

    let (context, payload) = logs_pdata().into_parts();
    let records: OtapArrowRecords = payload.try_into_with_default().expect("records");
    let weak = Arc::downgrade(
        records
            .get(ArrowPayloadType::Logs)
            .expect("logs batch")
            .column(0),
    );
    let store = Arc::new(object_store::memory::InMemory::new());
    let (handler, _rx) = effects(4);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let worker = Worker::new(worker_config(), store, wall, handler);

    let prepared = worker.prepare(OtapPdata::new(context, records.into()));
    assert!(matches!(prepared, Prepared::Ready(_)));
    assert!(
        weak.upgrade().is_none(),
        "the prepared output cannot pin the input arrays"
    );
    prepared.discard();
}

/// Scenario: a converted request fails the extraction budget.
/// Guarantees: a permanent refusal; nothing parked and the ACTIVE block untouched.
#[tokio::test(flavor = "current_thread")]
async fn extraction_failure_is_refused_atomically() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(2);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            // Large enough to pass the wire-size check, so the refusal can
            // only come from the measured extracted output.
            cfg.lake.ingress.max_extracted_bytes = 1;
            let mut worker = Worker::new(cfg, store, wall, handler);

            worker.admit(logs_pdata());
            assert!(worker.active.data.is_empty());
            assert!(worker.active.tokens.is_empty());
            assert!(worker.pending.is_none());

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(nack.permanent);
                    assert_eq!(nack.cause, NackCause::Refused);
                }
                other => panic!("expected an extraction refusal, got {other:?}"),
            }
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: a well-formed OTLP metrics request reaches the worker.
/// Guarantees: it is admitted through the same extraction into the ACTIVE block.
#[tokio::test(flavor = "current_thread")]
async fn metrics_are_admitted_like_logs() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, _rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store, wall, handler);

            let mut context = Context::default();
            context.set_source_node(7);
            worker.admit(OtapPdata::new(context, metrics_payload()));

            assert!(!worker.active.data.is_empty());
            assert_eq!(worker.active.tokens.len(), 1);
            assert!(worker.pending.is_none());
            assert_eq!(worker.notify.len(), 0);
        })
        .await;
}

/// An OTLP logs request of one record whose string body is `caf` then a lone
/// `0xc3`, which is not UTF-8.
fn invalid_utf8_body_pdata() -> OtapPdata {
    let len_field = |field: u32, payload: &[u8]| {
        let mut out = Vec::new();
        prost::encoding::encode_key(field, prost::encoding::WireType::LengthDelimited, &mut out);
        prost::encoding::encode_varint(payload.len() as u64, &mut out);
        out.extend_from_slice(payload);
        out
    };
    let mut record = vec![0x09];
    record.extend(1_789_960_500_000_000_000_u64.to_le_bytes());
    record.extend(len_field(5, &len_field(1, b"caf\xc3")));
    let body = len_field(1, &len_field(2, &len_field(2, &record)));
    let payload = otel_arrow_dfe_pdata::OtlpProtoBytes::ExportLogsRequest(body.into());
    let mut context = Context::default();
    context.set_source_node(7);
    OtapPdata::new(context, payload.into())
}

/// Scenario: a log body string holding `caf` and a lone `0xc3`.
/// Guarantees: it is admitted with U+FFFD in place of the invalid byte.
#[tokio::test(flavor = "current_thread")]
async fn invalid_utf8_in_a_log_body_is_stored_replaced() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let (handler, _rx) = effects(2);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let worker = Worker::new(worker_config(), store, wall, handler);
    match worker.prepare(invalid_utf8_body_pdata()) {
        Prepared::Ready(pending) => {
            assert_eq!(pending.extracted.stats.rows, 1);
            let values = format!("{:?}", pending.extracted.values);
            assert!(values.contains("caf\u{FFFD}"), "{values}");
            pending.token.discard();
        }
        Prepared::Failed(_, failure, _) => {
            panic!("refused: {failure:?}")
        }
    }
}

/// Scenario: the repaired log body, then a well-formed logs request, with telemetry.
/// Guarantees: `repaired.invalid_utf8{signal=logs}` counts one; the `metrics` bucket is untouched.
#[tokio::test(flavor = "current_thread")]
async fn a_repaired_log_body_is_counted_by_signal() {
    let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
    let (handler, _rx) = effects(4);
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
    worker.admit(invalid_utf8_body_pdata());
    worker.admit(logs_pdata());
    assert_eq!(worker.active.tokens.len(), 2, "both requests are admitted");
    let metrics = worker.metrics.as_ref().expect("registered");
    let repaired = |signal| super::super::metrics::SignalAttrs { signal };
    assert_eq!(
        metrics
            .repaired
            .get(repaired(SignalType::Logs))
            .invalid_utf8
            .get(),
        1
    );
    assert_eq!(
        metrics
            .repaired
            .get(repaired(SignalType::Metrics))
            .invalid_utf8
            .get(),
        0
    );
}

/// Scenario: an array and a key-value list attribute value holding a string with `0xc3`.
/// Guarantees: each is refused permanently as undecodable.
#[tokio::test(flavor = "current_thread")]
async fn invalid_utf8_inside_an_array_or_kvlist_value_is_refused_as_undecodable() {
    let len_field = |field: u32, payload: &[u8]| {
        let mut out = Vec::new();
        prost::encoding::encode_key(field, prost::encoding::WireType::LengthDelimited, &mut out);
        prost::encoding::encode_varint(payload.len() as u64, &mut out);
        out.extend_from_slice(payload);
        out
    };
    let invalid = len_field(1, b"caf\xc3");
    let key_value = |key: &[u8], value: &[u8]| [len_field(1, key), len_field(2, value)].concat();
    let array = len_field(5, &len_field(1, &invalid));
    let kvlist = len_field(6, &len_field(1, &key_value(b"inner", &invalid)));

    let store = Arc::new(object_store::memory::InMemory::new());
    let (handler, _rx) = effects(2);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let worker = Worker::new(worker_config(), store, wall, handler);
    for (name, value) in [("array", array), ("kvlist", kvlist)] {
        let mut record = vec![0x09];
        record.extend(1_789_960_500_000_000_000_u64.to_le_bytes());
        record.extend(len_field(6, &key_value(b"tags", &value)));
        let body = len_field(1, &len_field(2, &len_field(2, &record)));
        let payload = otel_arrow_dfe_pdata::OtlpProtoBytes::ExportLogsRequest(body.into());
        let mut context = Context::default();
        context.set_source_node(7);
        match worker.prepare(OtapPdata::new(context, payload.into())) {
            Prepared::Failed(token, failure, _) => {
                token.discard();
                assert_eq!(Outcome::of(&failure), Outcome::Invalid, "{name}");
                let sentence = Outcome::explain(&failure);
                assert!(
                    sentence.starts_with("invalid request content: undecodable pdata"),
                    "{name}: {sentence}"
                );
            }
            Prepared::Ready(_) => panic!("{name}: invalid UTF-8 inside the value was admitted"),
        }
    }
}

/// Scenario: a gauge point beside a summary point under `unsupported: reject`.
/// Guarantees: one permanent `unsupported` nack; the ACTIVE block is unchanged and nothing parked.
#[tokio::test(flavor = "current_thread")]
async fn a_mixed_metrics_request_is_rejected_atomically() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.unsupported = lake::config::UnsupportedPolicy::Reject;
            let mut worker = Worker::new(cfg, store, wall, handler);

            // A request admitted first, so the assertion is that the rejected
            // request left a non-empty block exactly as it found it rather
            // than that an empty block stayed empty.
            worker.admit(logs_pdata());
            let bytes = worker.active.data.bytes;
            let requests = worker.active.tokens.len();
            assert_eq!(requests, 1);

            worker.admit(mixed_metrics_pdata());
            assert_eq!(worker.active.data.bytes, bytes);
            assert_eq!(worker.active.tokens.len(), requests);
            assert!(worker.pending.is_none());
            assert_eq!(worker.live_tokens(), requests + 1);

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(nack.permanent);
                    assert_eq!(nack.cause, NackCause::Refused);
                    assert!(
                        nack.reason.contains("set unsupported: drop"),
                        "reason: {}",
                        nack.reason
                    );
                }
                other => panic!("expected an unsupported refusal, got {other:?}"),
            }
            // Exactly one completion, and it belongs to the rejected request:
            // the admitted one is still owed by the ACTIVE block.
            assert_eq!(worker.live_tokens(), requests);
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: the same mixed request under the default policy, written to storage.
/// Guarantees: the gauge is stored, the summary counted in `dropped.unsupported{kind=summary}`, and
/// the ack waits for the block.
#[tokio::test(flavor = "current_thread")]
async fn a_mixed_metrics_request_drops_only_the_unsupported_points() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let cfg = worker_config();
            assert_eq!(cfg.lake.unsupported, lake::config::UnsupportedPolicy::Drop);
            let mut worker = Worker::new(cfg, store, wall, handler);
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(mixed_metrics_pdata());
            let dropped = |kind| {
                worker
                    .metrics
                    .as_ref()
                    .expect("registered")
                    .dropped
                    .get(super::super::metrics::DroppedAttrs { kind })
                    .dropped_unsupported
                    .get()
            };
            assert_eq!(dropped(super::super::metrics::DroppedKind::Summary), 1);
            assert_eq!(dropped(super::super::metrics::DroppedKind::ExpHistogram), 0);
            assert_eq!(worker.active.tokens.len(), 1);
            assert!(!worker.active.data.is_empty());
            assert!(worker.pending.is_none());

            worker.rotate();
            assert!(
                worker.notify.is_empty(),
                "a dropped point decides nothing before the flush resolves"
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(5), rx.recv())
                    .await
                    .is_err(),
                "a dropped point does not acknowledge the request early"
            );
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            worker.complete(done);
            assert!(worker.notify.next().await.is_ok());
            assert!(matches!(
                rx.recv().await.expect("ack"),
                PipelineCompletionMsg::DeliverAck { .. }
            ));
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: the wall clock steps back between parking a request and opening its block.
/// Guarantees: one rotation admits the parked request; it is not parked again.
#[tokio::test(flavor = "current_thread")]
async fn a_backward_clock_step_does_not_repark_the_pending_request() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, _rx) = effects(8);
            // One-second windows, so the block opened at 100 s ends well
            // before the request prepared at 200 s.
            let wall = Arc::new(lake::clock::TestWallClock::new(100 * 1_000_000_000));
            let mut worker = Worker::new(worker_config(), store, Arc::clone(&wall) as _, handler);
            assert_eq!(worker.active.data.window_start_secs, 100);

            wall.set(200 * 1_000_000_000);
            worker.admit(logs_pdata());
            assert!(
                worker.pending.is_some(),
                "a request past the block's window waits for the next block"
            );
            assert!(worker.active.tokens.is_empty());

            // The clock now steps back behind both the parked request and the
            // block that is about to be replaced.
            wall.set(50 * 1_000_000_000);
            worker.rotate();
            worker.resume_pending();

            assert!(worker.pending.is_none(), "one rotation is enough");
            assert_eq!(worker.active.tokens.len(), 1);
            assert_eq!(worker.active.data.window_start_secs, 200);
        })
        .await;
}

/// Scenario: a logs request routed through 256 subscribing nodes, so its completion token holds
/// more than the 4 KiB allowance a block reserves for one.
/// Guarantees: it is refused permanently at ingress, before extraction and any block reservation,
/// with a reason naming the allowance, the observed size and the too-deep route; not parked.
#[tokio::test(flavor = "current_thread")]
async fn a_token_over_the_allowance_is_refused_permanently_at_ingress() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (handler, mut rx) = effects(2);
            let mut worker = Worker::new(
                worker_config(),
                Arc::new(object_store::memory::InMemory::new()),
                Arc::new(lake::clock::TestWallClock::new(0)),
                handler,
            );
            let mut data = logs_pdata();
            for node in 100..356 {
                data = data.test_subscribe_to(
                    Interests::ACKS | Interests::NACKS,
                    Default::default(),
                    node,
                );
            }
            worker.admit(data);
            assert!(worker.active.data.is_empty());
            assert!(worker.active.tokens.is_empty());
            assert!(
                worker.pending.is_none(),
                "a larger token never fits a block"
            );
            assert_eq!(worker.notify.outcomes()[Outcome::TokenTooLarge as usize], 1);
            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(nack.permanent, "reason: {}", nack.reason);
                    assert_eq!(nack.cause, NackCause::Refused);
                    assert!(
                        nack.reason.contains("4096-byte allowance")
                            && nack.reason.contains("route")
                            && nack.reason.contains("too deep"),
                        "reason: {}",
                        nack.reason
                    );
                }
                other => panic!("expected a permanent refusal, got {other:?}"),
            }
        })
        .await;
}

/// Scenario: a block budget set below what startup validation allows, so a request's worst
/// case misses an empty block.
/// Guarantees: a retryable internal nack counted as `internal`; not parked, block untouched.
#[tokio::test(flavor = "current_thread")]
async fn a_request_missing_an_empty_block_is_an_internal_nack_not_parked() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(2);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            // The per-request and extraction budgets stay wide open, so the
            // failure can only come from the block budget inside `reserve`.
            cfg.window.max_block_bytes = 1;
            cfg.lake.ingress.max_block_bytes = 1;
            let worker = Worker::new(
                cfg.clone(),
                store.clone(),
                Arc::clone(&wall) as _,
                effects(1).0,
            );
            let prepared = worker.prepare(logs_pdata());
            assert!(
                matches!(prepared, Prepared::Ready(_)),
                "preparation must succeed, or the refusal would not be the block budget"
            );
            prepared.discard();
            let mut worker = Worker::new(cfg, store, wall, handler);

            worker.admit(logs_pdata());
            assert!(worker.active.data.is_empty());
            assert!(worker.active.tokens.is_empty());
            assert!(
                worker.pending.is_none(),
                "an empty block cannot take it, and no later block will either"
            );

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a nack") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(nack.cause, NackCause::Unspecified);
                    assert!(
                        nack.reason.contains("series_parquet internal error")
                            && nack.reason.contains("exceeds max_block_bytes (1)"),
                        "reason: {}",
                        nack.reason
                    );
                }
                other => panic!("expected an internal nack, got {other:?}"),
            }
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: three requests reach the running node, two per block, on simulated clocks.
/// Guarantees: the parked request is stored before the newer one, which stays on the channel
/// meanwhile.
#[tokio::test(flavor = "current_thread")]
async fn the_parked_request_is_stored_before_a_newer_one() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<OtapPdata>>::new(8);
            let (pdata_tx, pdata_rx) = mpsc::Channel::<OtapPdata>::new(8);
            let inbox = ExporterInbox::new(
                Receiver::Local(LocalReceiver::mpsc(control_rx)),
                Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
                0,
                Interests::empty(),
            );
            let (handler, mut rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));

            // A block takes two requests and is rotated by the window or a
            // third one, which is parked.
            let mut cfg = worker_config();
            cfg.window.max_requests_per_block = 3;
            cfg.lake.ingress.max_requests_per_block = 2;
            let node = tokio::task::spawn_local(super::super::run(
                cfg,
                Arc::new(object_store::memory::InMemory::new()),
                Arc::clone(&wall) as _,
                inbox,
                handler,
                None,
            ));

            // The first two fill the room the block reserves; the third
            // cannot be reserved against it and waits for the next block.
            for source in 1..=3 {
                pdata_tx
                    .send_async(logs_pdata_from(source))
                    .await
                    .expect("a request enqueues");
            }

            // Parking the third request asks for a rotation, so the first
            // block is written without any boundary being reached and the
            // parked request enters the block that replaces it.
            for source in 1..=2 {
                assert_eq!(expect_ack(&mut rx).await, Some(source));
            }

            // Those acks are delivered after the rotation that admitted the
            // third request, so the block it sits in already exists. Its
            // window is reached by moving both clocks explicitly.
            wall.set(1_100_000_000);
            sim.advance(Duration::from_secs(1));
            assert_eq!(expect_ack(&mut rx).await, Some(3));

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

/// Scenario: one request refused by the extraction budget and one by the series limit.
/// Guarantees: the extraction WARN carries the setting, the observed size and the limit; the
/// series refusal's WARN and reason name its outcome, the count and the limit.
#[tokio::test(flavor = "current_thread")]
async fn a_size_refusal_reports_the_observed_size_and_the_limit() {
    let events = capture();
    /// The one refusal WARN logged since `before` events were recorded.
    fn refusal(events: &Capture, before: usize) -> CapturedEvent {
        let logged = events.named("series_parquet.request.failed");
        assert_eq!(logged.len(), before + 1, "{logged:?}");
        let event = logged[before].clone();
        assert_eq!(event.level, tracing::Level::WARN);
        event
    }
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let store = Arc::new(object_store::memory::InMemory::new());

    let (handler, mut rx) = effects(4);
    let mut extract = Worker::new(
        worker_config(),
        store.clone(),
        Arc::clone(&wall) as _,
        handler,
    );
    extract.cfg.lake.ingress.max_extracted_bytes = 100;
    extract.admit(logs_pdata());
    let event = refusal(&events, 0);
    let field = |name: &str| event.fields.get(name).cloned();
    assert_eq!(
        field("outcome"),
        Some(FieldValue::Str("extracted_too_large".into()))
    );
    assert_eq!(field("signal"), Some(FieldValue::Debug("Logs".into())));
    assert_eq!(
        field("limit_setting"),
        Some(FieldValue::Str("ingress.max_extracted_bytes".into()))
    );
    assert_eq!(field("limit_bytes"), Some(FieldValue::U64(100)));
    let Some(FieldValue::U64(observed)) = field("observed_bytes") else {
        panic!("observed_bytes is a number: {event:?}");
    };
    assert!(observed > 100, "{observed}");
    extract
        .notify
        .next()
        .await
        .expect("the refusal is delivered");
    match rx.recv().await.expect("a nack") {
        PipelineCompletionMsg::DeliverNack { nack } => assert!(
            nack.reason.contains(&format!(
                "extracted request of {observed} bytes exceeds ingress.max_extracted_bytes \
                 (100 bytes)"
            )),
            "reason: {}",
            nack.reason
        ),
        other => panic!("expected a nack, got {other:?}"),
    }
    assert_no_more_completions(&mut rx);

    let (handler, mut rx) = effects(4);
    let mut series = Worker::new(worker_config(), store, wall, handler);
    series.cfg.lake.ingress.max_series_per_request = Some(0);
    series.admit(logs_pdata());
    let event = refusal(&events, 1);
    let field = |name: &str| event.fields.get(name).cloned();
    assert_eq!(
        field("outcome"),
        Some(FieldValue::Str("too_many_series".into()))
    );
    series
        .notify
        .next()
        .await
        .expect("the refusal is delivered");
    match rx.recv().await.expect("a nack") {
        PipelineCompletionMsg::DeliverNack { nack } => {
            assert!(nack.permanent);
            assert_eq!(
                nack.reason,
                "request carries at least 1 distinct series, more than \
                 ingress.max_series_per_request (0); split the batch upstream or raise the limit"
            );
        }
        other => panic!("expected a nack, got {other:?}"),
    }
    assert_no_more_completions(&mut rx);
    drop(events);
}

/// One OTLP logs request whose resource carries `host.id = host`.
fn logs_pdata_from_host(host: &str) -> OtapPdata {
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "host.id".to_owned(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(host.to_owned())),
                    }),
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_789_960_500_000_000_000,
                    event_name: "ready".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let payload = otel_arrow_dfe_pdata::OtlpProtoBytes::ExportLogsRequest(encoded(&request).into());
    let mut context = Context::default();
    context.set_source_node(7);
    OtapPdata::new(context, payload.into())
}

/// Scenario: requests from `host.id = producer-7` refused by the extraction budget and by the
/// series limit, one whose 300-byte producer id is refused, and a traces request.
/// Guarantees: each refusal WARN of a request whose producer id is known names it in
/// `producer_id`, cut to at most 128 bytes on a character boundary; the traces refusal, decided
/// before any conversion, has no `producer_id` field.
#[tokio::test(flavor = "current_thread")]
async fn refusal_warnings_name_the_producer() {
    let events = capture();
    let sim = clock::SimClock::new();
    let _clock_guard = sim.install();
    let (handler, _rx) = effects(16);
    let mut worker = Worker::new(
        worker_config(),
        Arc::new(object_store::memory::InMemory::new()),
        Arc::new(lake::clock::TestWallClock::new(0)),
        handler,
    );
    let producer =
        |event: &CapturedEvent| event.fields.get("producer_id").map(|v| v.text().to_owned());
    let refuse = |worker: &mut Worker, data: OtapPdata| {
        worker.admit(data);
        sim.advance(Duration::from_secs(2));
        events
            .named("series_parquet.request.failed")
            .last()
            .cloned()
            .expect("a refusal WARN")
    };

    worker.cfg.lake.ingress.max_extracted_bytes = 100;
    let event = refuse(&mut worker, logs_pdata_from_host("producer-7"));
    assert_eq!(
        event
            .fields
            .get("outcome")
            .map(|v| v.text().to_owned())
            .as_deref(),
        Some("extracted_too_large")
    );
    assert_eq!(producer(&event).as_deref(), Some("producer-7"));

    let long = "\u{e9}".repeat(150);
    let event = refuse(&mut worker, logs_pdata_from_host(&long));
    let cut = producer(&event).expect("a producer id");
    assert_eq!(cut, "\u{e9}".repeat(64), "128 bytes of two-byte characters");

    worker.cfg.lake.ingress.max_extracted_bytes = worker_config().lake.ingress.max_extracted_bytes;
    worker.cfg.lake.ingress.max_series_per_request = Some(0);
    let event = refuse(&mut worker, logs_pdata_from_host("producer-7"));
    assert_eq!(
        event
            .fields
            .get("outcome")
            .map(|v| v.text().to_owned())
            .as_deref(),
        Some("too_many_series")
    );
    assert_eq!(producer(&event).as_deref(), Some("producer-7"));

    let mut context = Context::default();
    context.set_source_node(7);
    let event = refuse(&mut worker, OtapPdata::new(context, traces_payload()));
    assert_eq!(
        event
            .fields
            .get("outcome")
            .map(|v| v.text().to_owned())
            .as_deref(),
        Some("unsupported")
    );
    assert_eq!(producer(&event), None);
}

/// Scenario: six refusals in a burst on a simulated clock, then one a second later.
/// Guarantees: one line for the burst; the next reports the five left out.
#[tokio::test(flavor = "current_thread")]
async fn refusal_warnings_are_rate_limited() {
    let events = capture();
    let sim = clock::SimClock::new();
    let _clock_guard = sim.install();
    let (handler, _rx) = effects(16);
    let mut worker = Worker::new(
        worker_config(),
        Arc::new(object_store::memory::InMemory::new()),
        Arc::new(lake::clock::TestWallClock::new(0)),
        handler,
    );
    let traces = || {
        let mut context = Context::default();
        context.set_source_node(7);
        OtapPdata::new(context, traces_payload())
    };
    for _ in 0..6 {
        worker.admit(traces());
        sim.advance(Duration::from_millis(100));
    }
    assert_eq!(events.named("series_parquet.request.failed").len(), 1);
    sim.advance(Duration::from_secs(1));
    worker.admit(traces());
    let logged = events.named("series_parquet.request.failed");
    assert_eq!(logged.len(), 2);
    assert_eq!(
        logged[1].fields.get("suppressed"),
        Some(&FieldValue::U64(5))
    );
    assert_eq!(worker.notify.outcomes()[Outcome::Unsupported as usize], 7);
}

/// Scenario: a gauge point with an exemplar, with `metrics.exemplars` unset (also under
/// `unsupported: reject`) and then set to `reject`.
/// Guarantees: by default the exemplar is counted in `dropped.exemplars{signal=metrics}`; under
/// `reject` the request is refused naming exemplars.
#[tokio::test(flavor = "current_thread")]
async fn an_exemplar_is_dropped_and_counted_by_default_and_refused_when_asked() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (handler, _rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.unsupported = lake::config::UnsupportedPolicy::Reject;
            let mut worker = Worker::new(
                cfg,
                Arc::new(object_store::memory::InMemory::new()),
                Arc::clone(&wall) as _,
                handler,
            );
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));
            worker.admit(exemplar_metrics_pdata());
            assert_eq!(worker.active.tokens.len(), 1, "the point is admitted");
            let metrics = worker.metrics.as_ref().expect("registered");
            assert_eq!(
                metrics
                    .exemplars
                    .get(super::super::metrics::SignalAttrs {
                        signal: SignalType::Metrics
                    })
                    .dropped_exemplars
                    .get(),
                1
            );

            let (handler, mut rx) = effects(4);
            let mut cfg = worker_config();
            cfg.lake.metrics.exemplars = Some(lake::config::ExemplarPolicy::Reject);
            let mut worker = Worker::new(
                cfg,
                Arc::new(object_store::memory::InMemory::new()),
                wall,
                handler,
            );
            worker.admit(exemplar_metrics_pdata());
            assert!(worker.active.data.is_empty(), "nothing was admitted");
            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(nack.permanent);
                    assert_eq!(nack.cause, NackCause::Refused);
                    assert!(nack.reason.contains("exemplars"), "reason: {}", nack.reason);
                    assert!(
                        nack.reason.contains("metrics.exemplars: drop"),
                        "reason: {}",
                        nack.reason
                    );
                }
                other => panic!("expected an exemplar refusal, got {other:?}"),
            }
            assert_no_more_completions(&mut rx);
        })
        .await;
}
