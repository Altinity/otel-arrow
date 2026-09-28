// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration: the user document, its validation, the factory and
//! the shipped examples.

use super::support::*;

/// Scenario: the adapter maps byte strings and meets an impossible lake budget.
/// Guarantees: `64MiB` reaches `lake.ingress.max_block_bytes` exactly, and the lake's rules are
/// enforced.
#[test]
fn configuration_maps_and_validates() {
    let cfg: Config = serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "window": {"interval": "15s", "max_block_bytes": "64MiB"},
        "ingress": {"max_extracted_bytes": "16MiB"}
    }))
    .expect("valid config");
    assert_eq!(cfg.lake.ingress.max_block_bytes, 64 << 20);
    // `upload.part_bytes` below the 5 MiB S3 multipart minimum is refused by
    // `LakeConfig::validate`, which this adapter must call.
    assert!(
        serde_json::from_value::<Config>(serde_json::json!({
            "storage": {"file": {"base_uri": "/tmp/series-test"}},
            "upload": {"part_bytes": "1MiB"}
        }))
        .is_err()
    );
}

/// Scenario: every byte and duration setting of the sections this exporter maps onto the lake,
/// written with units, as a plain number, and one unreadable size.
/// Guarantees: each reaches its lake field exactly, unset fields keep the lake defaults, and the
/// unreadable size is refused naming its section.
#[test]
fn every_section_maps_its_units_onto_the_lake() {
    let cfg: Config = serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "window": {"interval": "30s", "max_block_bytes": "1GiB"},
        "ingress": {"max_request_bytes": "8MiB", "max_extracted_bytes": 1048576,
                    "max_row_bytes": "1 KiB"},
        "sorting": {"run_target_bytes": "4MiB", "merge_chunk_bytes": "2MiB"},
        "upload": {"part_bytes": "6MiB", "concurrency": 3, "abort_timeout": "10s"},
        "parquet": {"row_group_bytes": "32MiB", "writer_limit_bytes": "48MiB"},
        "logs": {"series_attributes": ["user"], "values_sort": []},
        "metrics": {"exemplars": "reject"}
    }))
    .expect("valid config");
    let lake = &cfg.lake;
    assert_eq!(lake.window_interval, Duration::from_secs(30));
    assert_eq!(lake.ingress.max_block_bytes, 1 << 30);
    assert_eq!(lake.ingress.max_request_bytes, 8 << 20);
    assert_eq!(lake.ingress.max_extracted_bytes, 1 << 20);
    assert_eq!(lake.ingress.max_row_bytes, 1024);
    assert_eq!(lake.sorting.run_target_bytes, 4 << 20);
    assert_eq!(lake.sorting.merge_chunk_bytes, 2 << 20);
    assert_eq!(lake.upload.part_bytes, 6 << 20);
    assert_eq!(lake.upload.concurrency, 3);
    assert_eq!(lake.upload.abort_timeout, Duration::from_secs(10));
    assert_eq!(lake.parquet.row_group_bytes, 32 << 20);
    assert_eq!(lake.parquet.writer_limit_bytes, 48 << 20);
    assert_eq!(lake.logs.series_attributes, ["user"]);
    assert!(lake.logs.values_sort.is_empty());
    assert_eq!(
        lake.metrics.values_sort,
        lake::config::SignalConfig::default().values_sort
    );
    assert_eq!(
        lake.metrics.exemplars,
        Some(lake::config::ExemplarPolicy::Reject)
    );
    let err = serde_json::from_value::<Config>(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "upload": {"part_bytes": "lots"}
    }))
    .expect_err("an unreadable size");
    assert!(err.to_string().starts_with("upload:"), "{err}");
}

/// Scenario: `ingress.max_series_per_request` unset under the default and under a smaller block
/// budget, and set explicitly.
/// Guarantees: unset takes the most series the block budget holds, `(B - 2E - 4KiB) / 328`,
/// the same value the lake's own default derives; an explicit value is kept.
#[test]
fn the_series_limit_defaults_to_what_the_block_budget_holds() {
    let limit = |doc: serde_json::Value| {
        let mut doc = doc;
        doc["storage"] = serde_json::json!({"file": {"base_uri": "/tmp/series-test"}});
        serde_json::from_value::<Config>(doc)
            .expect("valid config")
            .lake
            .ingress
            .max_series_per_request
    };
    let lake_default = otel_arrow_dfe_series_lake::config::LakeConfig::default();
    assert_eq!(
        limit(serde_json::json!({})),
        Some(lake_default.max_series_per_request()),
        "the exporter and the lake derive the same default"
    );
    assert_eq!(limit(serde_json::json!({})), Some(1_393_826));
    assert_eq!(
        limit(serde_json::json!({"window": {"max_block_bytes": "100MiB"}})),
        Some(((100 << 20) - (64 << 20) - 4096) / 328)
    );
    assert_eq!(
        limit(serde_json::json!({"ingress": {"max_series_per_request": 5000}})),
        Some(5000)
    );
}

/// Scenario: a block-level budget written under `ingress`.
/// Guarantees: the setting is refused, not ignored.
#[test]
fn a_block_budget_written_under_ingress_is_refused() {
    let err = serde_json::from_value::<Config>(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "ingress": {"max_block_bytes": "64MiB"}
    }))
    .expect_err("max_block_bytes does not belong to ingress");
    assert!(
        err.to_string()
            .contains("ingress: unknown field `max_block_bytes`"),
        "unexpected error: {err}"
    );
}

/// Scenario: `unsupported` omitted, and set to `reject`.
/// Guarantees: omitted means `drop`; an explicit `reject` is kept.
#[test]
fn unsupported_defaults_to_drop_and_reject_is_kept() {
    for (policy, expected) in [
        (None, lake::config::UnsupportedPolicy::Drop),
        (Some("reject"), lake::config::UnsupportedPolicy::Reject),
    ] {
        let mut doc = serde_json::json!({"storage": {"file": {"base_uri": "/tmp/series-test"}}});
        if let Some(policy) = policy {
            doc["unsupported"] = serde_json::json!(policy);
        }
        let cfg: Config = serde_json::from_value(doc).expect("valid");
        assert_eq!(cfg.lake.unsupported, expected, "{policy:?}");
    }
}

/// Scenario: `metrics.exemplars: drop`, `metrics.exemplars: reject` and `logs.exemplars`.
/// Guarantees: the first two are accepted; `logs.exemplars` is an unknown field of `logs`.
#[test]
fn the_exemplar_policy_is_validated_at_startup() {
    for (exemplars, expected) in [
        ("drop", lake::config::ExemplarPolicy::Drop),
        ("reject", lake::config::ExemplarPolicy::Reject),
    ] {
        let cfg = serde_json::from_value::<Config>(serde_json::json!({
            "storage": {"file": {"base_uri": "/tmp/series-test"}},
            "metrics": {"exemplars": exemplars}
        }))
        .expect("valid");
        assert_eq!(cfg.lake.exemplar_policy(), expected, "{exemplars}");
    }
    let err = serde_json::from_value::<Config>(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "logs": {"exemplars": "drop"}
    }))
    .expect_err("refused");
    assert!(
        err.to_string().contains("logs: unknown field `exemplars`"),
        "unexpected error: {err}"
    );
}

/// Scenario: `parquet.compression`, `sorting.enabled` and `notify_batch`, which offer no choice
/// and are not settings.
/// Guarantees: each is refused as an unknown field, named with its section.
#[test]
fn removed_settings_are_unknown_fields() {
    for (section, value, expected) in [
        (
            "parquet",
            serde_json::json!({"compression": "zstd"}),
            "parquet: unknown field `compression`",
        ),
        (
            "sorting",
            serde_json::json!({"enabled": true}),
            "sorting: unknown field `enabled`",
        ),
        (
            "notify_batch",
            serde_json::json!(64),
            "unknown field `notify_batch`",
        ),
    ] {
        let mut doc = serde_json::json!({"storage": {"file": {"base_uri": "/tmp/series-test"}}});
        doc[section] = value;
        let err = serde_json::from_value::<Config>(doc).expect_err("refused");
        assert!(err.to_string().contains(expected), "{section}: {err}");
    }
}

/// Scenario: `window.interval` is a sub-second duration.
/// Guarantees: the adapter refuses it.
#[test]
fn a_sub_second_window_interval_is_refused() {
    let err = serde_json::from_value::<Config>(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "window": {"interval": "500ms"}
    }))
    .expect_err("sub-second interval");
    assert!(
        err.to_string().contains("whole seconds"),
        "unexpected error: {err}"
    );
}

/// Scenario: a `writer_id` that is a Kubernetes pod name with hyphens.
/// Guarantees: the configuration starts with the id kept as written.
#[test]
fn a_hyphenated_writer_id_is_accepted() {
    let cfg: Config = serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "writer_id": "otel-lake-7d9f-0"
    }))
    .expect("a pod name is a valid writer id");
    assert_eq!(cfg.lake.writer_id, "otel-lake-7d9f-0");
}

/// Scenario: the factory builds file storage with no capability bound to the node.
/// Guarantees: creation succeeds without a bearer token provider.
#[test]
fn the_factory_creates_file_storage_without_a_capability() {
    use otel_arrow_dfe_config::node::NodeUserConfig;
    use otel_arrow_dfe_engine::config::ExporterConfig;
    use otel_arrow_dfe_engine::context::ControllerContext;

    let dir = tempfile::tempdir().expect("temp dir");
    let metrics_system = otel_arrow_dfe_telemetry::InternalTelemetrySystem::default();
    let controller = ControllerContext::new(metrics_system.registry());
    let pipeline = controller
        .pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0)
        .with_node_context(
            "series".into(),
            super::super::SERIES_PARQUET_EXPORTER_URN.into(),
            otel_arrow_dfe_config::node::NodeKind::Exporter,
            std::collections::HashMap::new(),
        );
    let mut node_config =
        NodeUserConfig::new_exporter_config(super::super::SERIES_PARQUET_EXPORTER_URN);
    node_config.config = serde_json::json!({
        "storage": {"file": {"base_uri": dir.path().to_str().expect("utf-8 path")}}
    });
    let created = (super::super::SERIES_PARQUET.create)(
        pipeline,
        test_node("series"),
        Arc::new(node_config),
        &ExporterConfig::new("series"),
        &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
    );
    assert!(created.is_ok(), "file storage needs no capability");
}

/// Scenario: S3 `unsigned_payload` unset over AWS, HTTPS and plain HTTP, and set explicitly.
/// Guarantees: the validated config keeps the option as written, so the store
/// constructor resolves an unset one from `AWS_UNSIGNED_PAYLOAD` and otherwise
/// signs every payload.
#[test]
fn unsigned_payload_is_left_for_the_store_to_resolve() {
    use otel_arrow_dfe_otap::object_store::StorageType;
    let resolved = |endpoint: Option<&str>, unsigned: Option<bool>| {
        let mut s3 =
            serde_json::json!({"base_uri": "s3://bucket/lake", "auth": {"type": "default"}});
        if let Some(endpoint) = endpoint {
            s3["endpoint"] = endpoint.into();
            s3["allow_http"] = true.into();
        }
        if let Some(unsigned) = unsigned {
            s3["unsigned_payload"] = unsigned.into();
        }
        let cfg: Config = serde_json::from_value(serde_json::json!({
            "storage": {"s3": s3},
            "retry": {"retry_timeout": "30s"}
        }))
        .expect("valid config");
        match cfg.storage {
            StorageType::S3 {
                unsigned_payload, ..
            } => unsigned_payload,
            other => panic!("not S3: {other:?}"),
        }
    };
    let (https, http) = (Some("https://s3.example.com"), Some("http://minio:9000"));
    assert_eq!(resolved(None, None), None);
    assert_eq!(resolved(https, None), None);
    assert_eq!(resolved(http, None), None);
    assert_eq!(resolved(https, Some(false)), Some(false));
    assert_eq!(resolved(http, Some(true)), Some(true));
}

/// Scenario: the factory builds S3 storage with no capability bound to the node.
/// Guarantees: creation succeeds; S3 authenticates through its own `auth` section.
#[test]
fn the_factory_creates_s3_storage_without_a_token_provider() {
    use otel_arrow_dfe_config::node::NodeUserConfig;
    use otel_arrow_dfe_engine::config::ExporterConfig;
    use otel_arrow_dfe_engine::context::ControllerContext;

    let metrics_system = otel_arrow_dfe_telemetry::InternalTelemetrySystem::default();
    let controller = ControllerContext::new(metrics_system.registry());
    let pipeline = controller
        .pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0)
        .with_node_context(
            "series".into(),
            super::super::SERIES_PARQUET_EXPORTER_URN.into(),
            otel_arrow_dfe_config::node::NodeKind::Exporter,
            std::collections::HashMap::new(),
        );
    let mut node_config =
        NodeUserConfig::new_exporter_config(super::super::SERIES_PARQUET_EXPORTER_URN);
    node_config.config = serde_json::json!({
        "storage": s3_storage(),
        "retry": {"retry_timeout": "30s"}
    });
    let created = (super::super::SERIES_PARQUET.create)(
        pipeline,
        test_node("series"),
        Arc::new(node_config),
        &ExporterConfig::new("series"),
        &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
    );
    assert!(created.is_ok(), "S3 storage needs no bearer token provider");
}

/// Scenario: the factory builds Azure storage, valid otherwise, with no capability bound.
/// Guarantees: creation fails before the node starts, naming the capability.
#[test]
#[cfg(feature = "azure")]
fn the_factory_refuses_azure_storage_without_a_token_provider() {
    use otel_arrow_dfe_config::node::NodeUserConfig;
    use otel_arrow_dfe_engine::config::ExporterConfig;
    use otel_arrow_dfe_engine::context::ControllerContext;

    let metrics_system = otel_arrow_dfe_telemetry::InternalTelemetrySystem::default();
    let controller = ControllerContext::new(metrics_system.registry());
    let pipeline = controller
        .pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0)
        .with_node_context(
            "series".into(),
            super::super::SERIES_PARQUET_EXPORTER_URN.into(),
            otel_arrow_dfe_config::node::NodeKind::Exporter,
            std::collections::HashMap::new(),
        );
    let mut node_config =
        NodeUserConfig::new_exporter_config(super::super::SERIES_PARQUET_EXPORTER_URN);
    node_config.config = serde_json::json!({
        "storage": {"azure": {
            "base_uri": "https://mystorageaccount.blob.core.windows.net/container"
        }},
        "retry": {"retry_timeout": "30s"}
    });
    let created = (super::super::SERIES_PARQUET.create)(
        pipeline,
        test_node("series"),
        Arc::new(node_config),
        &ExporterConfig::new("series"),
        &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
    );
    let Err(err) = created else {
        panic!("azure storage must require a bound bearer_token_provider");
    };
    assert!(
        matches!(
            err,
            otel_arrow_dfe_config::error::Error::InvalidUserConfig { .. }
        ),
        "{err}"
    );
    assert!(err.to_string().contains("bearer_token_provider"), "{err}");
}

/// Scenario: a cloud retry budget equal to, above and below the flush deadline, and file storage
/// with no retry section.
/// Guarantees: only a budget not strictly shorter is refused, naming both values.
#[test]
fn an_explicit_store_retry_budget_must_be_shorter_than_the_flush_deadline() {
    let deadline = Duration::from_secs(60);
    let retry = |timeout: &str| -> otel_arrow_dfe_otap::object_store::RetryOptions {
        serde_json::from_value(serde_json::json!({ "retry_timeout": timeout }))
            .expect("retry options")
    };
    for timeout in ["60s", "3m"] {
        let err = super::super::config::check_retry_deadline(&retry(timeout), deadline)
            .expect_err("not strictly less");
        assert!(err.starts_with("retry.retry_timeout ("), "{err}");
        assert!(err.contains("window.flush_retry_deadline (60s)"), "{err}");
    }
    assert!(super::super::config::check_retry_deadline(&retry("30s"), deadline).is_ok());
    // The shipped local example keeps loading with no retry section at all,
    // and no retry options are made up for a store that applies none.
    let cfg: Config = serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}}
    }))
    .expect("file storage applies no store retry");
    assert!(cfg.retry.is_none());
}

/// Scenario: a minimal S3 configuration without `retry`, at the default and a 20s flush deadline,
/// then with explicit budgets of 60s and 3m.
/// Guarantees: the derived budget is half the deadline with object_store's other defaults; an
/// explicit budget at or above the deadline is refused.
#[test]
fn a_minimal_s3_config_derives_its_retry_budget_from_the_flush_deadline() {
    let validate = super::super::SERIES_PARQUET.validate_config;
    validate(&serde_json::json!({"storage": s3_storage()})).expect("a minimal S3 config loads");
    for (deadline, derived) in [(None, 30), (Some("20s"), 10)] {
        let mut doc = serde_json::json!({"storage": s3_storage()});
        if let Some(deadline) = deadline {
            doc["window"] = serde_json::json!({"flush_retry_deadline": deadline});
        }
        let cfg: Config = serde_json::from_value(doc).expect("valid");
        let retry = cfg.retry.expect("a cloud store gets derived retry options");
        assert_eq!(retry.retry_timeout, Duration::from_secs(derived));
        let defaults: otel_arrow_dfe_otap::object_store::RetryOptions =
            serde_json::from_value(serde_json::json!({})).expect("defaults");
        assert_eq!(retry.max_retries, defaults.max_retries);
        assert_eq!(retry.init_backoff, defaults.init_backoff);
        assert_eq!(retry.max_backoff, defaults.max_backoff);
    }
    for timeout in ["60s", "3m"] {
        let err = validate(&serde_json::json!({
            "storage": s3_storage(),
            "retry": {"retry_timeout": timeout}
        }))
        .expect_err("an explicit budget at or above the deadline")
        .to_string();
        assert!(err.contains("retry.retry_timeout ("), "{err}");
        assert!(err.contains("window.flush_retry_deadline (60s)"), "{err}");
    }
}

/// Scenario: an S3 configuration whose `retry` section sets only `max_retries`, or only
/// `init_backoff` under a 20s flush deadline.
/// Guarantees: it starts; the fields it sets are kept and every other field, `retry_timeout`
/// included, is the one derived without a section (half the flush deadline).
#[test]
fn a_partial_retry_section_keeps_the_derived_budget() {
    let validate = super::super::SERIES_PARQUET.validate_config;
    let doc = serde_json::json!({"storage": s3_storage(), "retry": {"max_retries": 3}});
    validate(&doc).expect("a partial retry section starts");
    let retry = serde_json::from_value::<Config>(doc)
        .expect("valid")
        .retry
        .expect("retry options");
    let defaults: otel_arrow_dfe_otap::object_store::RetryOptions =
        serde_json::from_value(serde_json::json!({})).expect("defaults");
    assert_eq!(retry.max_retries, 3);
    assert_eq!(retry.retry_timeout, Duration::from_secs(30));
    assert_eq!(retry.init_backoff, defaults.init_backoff);
    let retry = serde_json::from_value::<Config>(serde_json::json!({
        "storage": s3_storage(),
        "window": {"flush_retry_deadline": "20s"},
        "retry": {"init_backoff": "50ms"}
    }))
    .expect("valid")
    .retry
    .expect("retry options");
    assert_eq!(retry.init_backoff, Duration::from_millis(50));
    assert_eq!(retry.retry_timeout, Duration::from_secs(10));
    assert_eq!(retry.max_retries, defaults.max_retries);
    let err = validate(&serde_json::json!({
        "storage": s3_storage(),
        "retry": {"max_retires": 3}
    }))
    .expect_err("a misspelled retry field")
    .to_string();
    assert!(err.contains("max_retires"), "{err}");
}

/// Scenario: `upload.abort_timeout` of 999 ms, exactly 1 s and exactly 1 h, and
/// `window.flush_retry_deadline` of exactly 1 h.
/// Guarantees: below 1 s startup is refused naming key, value and floor; both bounds are accepted.
#[test]
fn the_abort_timeout_has_a_one_second_floor() {
    let validate = super::super::SERIES_PARQUET.validate_config;
    let with = |abort: &str| {
        serde_json::json!({
            "storage": {"file": {"base_uri": "/tmp/series-config"}},
            "upload": {"abort_timeout": abort}
        })
    };
    let err = validate(&with("999ms"))
        .expect_err("below the floor")
        .to_string();
    assert!(
        err.contains("upload.abort_timeout (999ms) must be at least 1s"),
        "{err}"
    );
    validate(&with("1s")).expect("the floor itself is accepted");
    validate(&with("1h")).expect("the ceiling itself is accepted");
    validate(&serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-config"}},
        "window": {"flush_retry_deadline": "1h"}
    }))
    .expect("the flush deadline ceiling itself is accepted");
}

/// Scenario: each misspelled setting or cross-field violation applied alone to a valid base,
/// through the factory's `validate_config`.
/// Guarantees: each is rejected with the rule it broke.
#[test]
fn startup_rejects_invalid_configuration() {
    let validate = super::super::SERIES_PARQUET.validate_config;
    let base = serde_json::json!({"storage": {"file": {"base_uri": "/tmp/series-config"}}});
    validate(&base).expect("the base configuration is valid");
    let cases = [
        (
            "window",
            serde_json::json!({"interval": "0s"}),
            "window.interval",
        ),
        (
            "window",
            serde_json::json!({"interval": "500ms"}),
            "window.interval must be positive whole seconds",
        ),
        (
            "window",
            serde_json::json!({"interval": "100000000000s"}),
            "window.interval must be positive whole seconds, at most 86400s",
        ),
        (
            "window",
            serde_json::json!({"max_requests_per_block": 0}),
            "window.max_requests_per_block must be at least 1",
        ),
        (
            "window",
            serde_json::json!({"max_block_bytes": "1MiB"}),
            "window.max_block_bytes (1048576) must hold the worst case of one request",
        ),
        (
            "ingress",
            serde_json::json!({"max_series_per_request": 2_000_000}),
            "ingress.max_series_per_request (2000000) * 328 bytes per metrics series",
        ),
        (
            "ingress",
            serde_json::json!({"max_row_bytes": "3MiB"}),
            "ingress.max_row_bytes must be at most sorting.run_target_bytes / 4",
        ),
        (
            "ingress",
            serde_json::json!({"max_requset_bytes": "1MiB"}),
            "ingress: unknown field `max_requset_bytes`",
        ),
        (
            "ingress",
            serde_json::json!({"max_nesting_depth": 257}),
            "ingress.max_nesting_depth must be at most 256",
        ),
        (
            "upload",
            serde_json::json!({"concurrency": 0}),
            "upload.concurrency must be at least 1",
        ),
        (
            "upload",
            serde_json::json!({"part_bytes": "1MiB"}),
            "upload.part_bytes must be at least 5MiB",
        ),
        (
            "metrics",
            serde_json::json!({"values_sort": ["no_such_column"]}),
            "metrics.values_sort[0].column \"no_such_column\" is not a column of",
        ),
        // `attrs` is a Map column of logs/values: present, but not a type the
        // Arrow row format can sort by.
        (
            "logs",
            serde_json::json!({"values_sort": ["attrs"]}),
            "logs.values_sort[0].column \"attrs\" has type",
        ),
        (
            "logs",
            serde_json::json!({"denormalize": [{"path": "resource.x", "column": "SERIES_ID"}]}),
            "logs.denormalize[0].column \"SERIES_ID\" is a column name collision",
        ),
        (
            "logs",
            serde_json::json!({"denormalize": [{"path": "resource.x", "column": "date"}]}),
            "logs.denormalize[0].column \"date\" is named like a partition key",
        ),
        (
            "logs",
            serde_json::json!({"denormalize": ["unknown.x"]}),
            "logs.denormalize[0].path \"unknown.x\" must start with",
        ),
        (
            "metrics",
            serde_json::json!({"series_attributes": ["k8s.pod.name"]}),
            "metrics: unknown field `series_attributes`",
        ),
        (
            "writer_id",
            serde_json::json!("local 1"),
            "writer_id \"local 1\" must use only [A-Za-z0-9_.-]",
        ),
        (
            "series_cache",
            serde_json::json!({"max_entries": 0}),
            "series_cache.max_entries must be positive",
        ),
        (
            "window",
            serde_json::json!({"flush_retry_deadline": "0s"}),
            "window.flush_retry_deadline must be positive",
        ),
        (
            "window",
            serde_json::json!({"flush_retry_deadline": "3601s"}),
            "window.flush_retry_deadline must be positive, at most 3600s",
        ),
        (
            "window",
            serde_json::json!({"max_requests_per_block": 0}),
            "window.max_requests_per_block must be at least 1",
        ),
        (
            "window",
            serde_json::json!({"max_block_bytes": "lots"}),
            "window: ",
        ),
        (
            "ingress",
            serde_json::json!({"max_request_bytes": 0}),
            "ingress.max_request_bytes must be positive",
        ),
        (
            "sorting",
            serde_json::json!({"merge_chunk_bytes": 0}),
            "sorting.merge_chunk_bytes must be positive",
        ),
        (
            "parquet",
            serde_json::json!({"writer_limit_bytes": 0}),
            "parquet.writer_limit_bytes must be positive",
        ),
        (
            "upload",
            serde_json::json!({"abort_timeout": "0s"}),
            "upload.abort_timeout (0ns) must be at least 1s",
        ),
        (
            "upload",
            serde_json::json!({"abort_timeout": "999ms"}),
            "upload.abort_timeout (999ms) must be at least 1s",
        ),
        (
            "upload",
            serde_json::json!({"abort_timeout": "3601s"}),
            "upload.abort_timeout (3601s) must be at least 1s, at most 3600s",
        ),
        (
            "upload",
            serde_json::json!({"part_bytes": null}),
            "upload: byte size must not be null",
        ),
        (
            "metrics",
            serde_json::json!({"denormalise": []}),
            "metrics: unknown field `denormalise`",
        ),
    ];
    for (section, value, expected) in cases {
        let mut candidate = base.clone();
        candidate[section] = value;
        let err = validate(&candidate).expect_err(section).to_string();
        assert!(
            err.contains(expected),
            "section={section}: expected {expected:?} in {err}"
        );
    }
}
