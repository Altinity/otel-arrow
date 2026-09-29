// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bounded operational metrics for one series exporter worker.
//!
//! Every label comes from a closed enumeration or from a physical column name
//! fixed by configuration at startup, never from an error string, a path or an
//! id a request carries. Delta counters report what happened since the last
//! collection, observed counters republish a total the worker keeps, and
//! gauges report the state at the moment of sampling.

use super::outcome::{Outcome, WriteFailure};
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_otap::metrics::ExporterExportMetrics;
use otel_arrow_dfe_series_lake::config::LakeConfig;
use otel_arrow_dfe_series_lake::extract::ExtractStats;
use otel_arrow_dfe_series_lake::schema::Dataset;
use otel_arrow_dfe_telemetry::attributes::AttributeEnum;
use otel_arrow_dfe_telemetry::instrument::{Counter, Gauge, Mmsc, ObserveCounter};
use otel_arrow_dfe_telemetry::metrics::{MeasurementMetricSet, MetricSet, MetricSetSnapshot};
use otel_arrow_dfe_telemetry::reporter::MetricsReporter;
use otel_arrow_dfe_telemetry_macros::{AttributeEnum, attribute_set, metric_set};
use std::collections::BTreeMap;

/// Worker state and totals that carry no label of their own.
#[metric_set(name = "exporter.series_parquet")]
#[derive(Debug, Default, Clone)]
pub(super) struct WorkerMetrics {
    /// Series ids the bounded descriptor cache currently holds.
    #[metric(name = "series_cache.entries", unit = "{entry}")]
    pub cache_entries: Gauge<u64>,
    /// Cache lookups that found the descriptor already committed here.
    #[metric(name = "series_cache.hits", unit = "{lookup}")]
    pub cache_hits: ObserveCounter<u64>,
    /// Cache lookups that did not.
    #[metric(name = "series_cache.misses", unit = "{lookup}")]
    pub cache_misses: ObserveCounter<u64>,
    /// Cache entries dropped because the bound was reached.
    #[metric(name = "series_cache.evictions", unit = "{entry}")]
    pub cache_evictions: ObserveCounter<u64>,
    /// Bytes the ACTIVE block has charged.
    #[metric(name = "block.active", unit = "By")]
    pub active_bytes: Gauge<u64>,
    /// Bytes the FLUSHING block charged when it was sealed.
    #[metric(name = "block.flushing", unit = "By")]
    pub flushing_bytes: Gauge<u64>,
    /// Bytes the one parked request retains: its extracted rows, its
    /// descriptors and its completion token.
    #[metric(name = "block.pending", unit = "By")]
    pub pending_bytes: Gauge<u64>,
    /// Requests the worker still owes a decision, wherever they sit.
    #[metric(name = "block.requests_pending", unit = "{request}")]
    pub requests_pending: Gauge<u64>,
    /// Whether the single parking slot holds an extracted request.
    #[metric(name = "block.pending_slot_occupied", unit = "{slot}")]
    pub pending_slot: Gauge<u64>,
    /// Wall time one flush took, from rotation to completion.
    #[metric(name = "flush.duration", unit = "s")]
    pub flush_duration: Mmsc,
    /// Write attempts started beyond the first of their flush, credited when
    /// each starts.
    #[metric(name = "flush.retries", unit = "{attempt}")]
    pub flush_retries: Counter<u64>,
    /// Flushes that failed because the write was cancelled.
    #[metric(name = "flush.cancelled", unit = "{flush}")]
    pub flush_cancelled: Counter<u64>,
    /// Multipart uploads a failed write attempt may have left to the bucket's
    /// lifecycle rule: the abort failed or timed out, the creation got no
    /// definite answer, or the write did not unwind by the cleanup cutoff.
    /// Counted once per failed attempt: the client retries a creation up to
    /// `retry.max_retries` times inside one attempt, so one count can stand
    /// for up to `retry.max_retries + 1` incomplete uploads.
    #[metric(name = "flush.abort_failures", unit = "{upload}")]
    pub flush_abort_failures: Counter<u64>,
    /// Requests acknowledged as durable.
    #[metric(unit = "{message}")]
    pub acks: ObserveCounter<u64>,
    /// Decided completions still waiting to be delivered.
    #[metric(name = "notify.queued", unit = "{request}")]
    pub notify_queued: Gauge<u64>,
    /// Bytes the undelivered completions retain: queue storage and each
    /// token's external routing buffers.
    #[metric(name = "notify.token_size", unit = "By")]
    pub notify_token_bytes: Gauge<u64>,
    /// Completions the engine would not accept.
    #[metric(name = "notify.failures", unit = "{request}")]
    pub notify_failures: ObserveCounter<u64>,
    /// The Acks among `notify.failures`: each is a written block its sender,
    /// or a durable buffer's replay, stores again.
    #[metric(name = "notify.lost_acks", unit = "{request}")]
    pub notify_lost_acks: ObserveCounter<u64>,
    /// Age of the oldest completion the worker still owes.
    #[metric(name = "oldest_unacked.age", unit = "s")]
    pub oldest: Gauge<f64>,
    /// Whether pdata admission is closed at the moment of sampling: 1 while
    /// the node is not taking requests from its input channel, 0 otherwise.
    ///
    /// Admission closes while a rotation waits for the flush slot, while a
    /// request is parked, and when the completion credit is spent; the
    /// receiver upstream then reports the backpressure as its own refusals.
    #[metric(name = "admission.closed", unit = "{state}")]
    pub admission_closed: Gauge<u64>,
    /// Times admission went from open to closed.
    #[metric(name = "admission.closures", unit = "{closure}")]
    pub admission_closures: ObserveCounter<u64>,
    /// Total time admission has been closed, a closure still in progress
    /// included.
    #[metric(name = "admission.closed.duration", unit = "s")]
    pub admission_closed_duration: ObserveCounter<f64>,
    /// Point timestamps outside the representable range.
    #[metric(name = "timestamp.out_of_range", unit = "{timestamp}")]
    pub timestamp_out_of_range: Counter<u64>,
    /// Bytes the worker's configuration allows it to hold.
    #[metric(name = "memory.budget", unit = "By")]
    pub memory_budget_bytes: Gauge<u64>,
    /// Bytes the worker is accounted as holding right now.
    #[metric(name = "memory.accounted", unit = "By")]
    pub memory_accounted_bytes: Gauge<u64>,
    /// Bytes the write in progress holds beside its block and merge keys:
    /// the merge chunk, the Parquet encoder's in-progress row group and the
    /// upload bytes the store has not acknowledged. Included in
    /// `memory.accounted`.
    #[metric(name = "flush.workspace", unit = "By")]
    pub flush_workspace_bytes: Gauge<u64>,
}

/// Why a block was sealed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AttributeEnum)]
pub(super) enum FlushReason {
    /// The block's aligned window ended.
    Time,
    /// The block reached its byte budget.
    Bytes,
    /// The block reached its request budget.
    Requests,
    /// The node is shutting down.
    Shutdown,
}

/// The rotation trigger of one flush.
#[attribute_set(item, measurement)]
#[derive(Debug, Clone, Copy)]
pub(super) struct FlushAttrs {
    /// Why the block was sealed.
    pub reason: FlushReason,
}

/// Flushes started, split by what asked for the rotation.
#[metric_set(name = "exporter.series_parquet", measurement_attributes = FlushAttrs)]
#[derive(Debug, Default, Clone)]
pub(super) struct FlushMetrics {
    /// Non-empty blocks handed to a write task.
    #[metric(name = "flushes", unit = "{flush}")]
    pub count: Counter<u64>,
}

/// What the cleanup of a flush found in the store that the write had not
/// confirmed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AttributeEnum)]
#[repr(usize)]
pub(super) enum LateCommit {
    /// Every object of a failed block exists although its requests were
    /// nacked, so their rows may be stored twice once the producer retries.
    Stored,
    /// A multipart completion lost its response, a probe found the object,
    /// and the block was acknowledged.
    Acknowledged,
    /// Only some objects of a failed block exist.
    Partial,
    /// A probe could not tell whether a failed block's objects exist.
    Unknown,
}

impl LateCommit {
    /// Every outcome, indexed by its discriminant; the length is the derived
    /// variant count, so a variant left out does not compile.
    pub(super) const ALL: [LateCommit; <LateCommit as AttributeEnum>::CARDINALITY] = [
        LateCommit::Stored,
        LateCommit::Acknowledged,
        LateCommit::Partial,
        LateCommit::Unknown,
    ];
}

// `ALL[o as usize] == o`, which the per-outcome tally indexes by.
const _: () = {
    let mut i = 0;
    while i < LateCommit::ALL.len() {
        assert!(LateCommit::ALL[i] as usize == i);
        i += 1;
    }
};

/// The finding of one flush cleanup.
#[attribute_set(item, measurement)]
#[derive(Debug, Clone, Copy)]
pub(super) struct LateCommitAttrs {
    /// What the cleanup found.
    pub outcome: LateCommit,
}

/// Flush cleanups that found objects the write had not confirmed, or could
/// not tell, split by what they found.
#[metric_set(name = "exporter.series_parquet", measurement_attributes = LateCommitAttrs)]
#[derive(Debug, Default, Clone)]
pub(super) struct LateCommitMetrics {
    /// Flushes whose cleanup found this outcome.
    #[metric(name = "flush.late_commits", unit = "{flush}")]
    pub late_commits: Counter<u64>,
}

/// Why one flush did not put its block in object storage.
#[attribute_set(item, measurement)]
#[derive(Debug, Clone, Copy)]
pub(super) struct FlushFailureAttrs {
    /// The class of the failed write.
    #[attribute_key = "error.type"]
    pub error_type: WriteFailure,
}

/// Failed flushes, split by why they failed.
#[metric_set(name = "exporter.series_parquet", measurement_attributes = FlushFailureAttrs)]
#[derive(Debug, Default, Clone)]
pub(super) struct FlushFailureMetrics {
    /// Flushes that did not put their block in object storage.
    #[metric(name = "block.write_failures", unit = "{block}")]
    pub failures: Counter<u64>,
}

/// The refusal class of one nacked request: its [`Outcome`], never `ack`.
///
/// A size refusal names the budget it exceeded, one value per setting, so an
/// operator can tell which limit to raise without reading the log.
#[attribute_set(item, measurement)]
#[derive(Debug, Clone, Copy)]
pub(super) struct NackAttrs {
    /// Why the request was refused.
    #[attribute_key = "error.type"]
    pub error_type: Outcome,
}

impl NackAttrs {
    /// Every outcome a nack can carry, in label order: every outcome but
    /// `Ack`.
    pub(super) fn error_types() -> impl Iterator<Item = Outcome> {
        Outcome::ALL
            .into_iter()
            .filter(|outcome| *outcome != Outcome::Ack)
    }
}

/// Requests refused, split by the rule that refused them.
#[metric_set(name = "exporter.series_parquet", measurement_attributes = NackAttrs)]
#[derive(Debug, Default, Clone)]
pub(super) struct NackMetrics {
    /// Requests decided as a nack.
    #[metric(unit = "{message}")]
    pub nacks: ObserveCounter<u64>,
}

/// Which of a signal's two lake datasets one written file belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AttributeEnum)]
pub(super) enum DatasetKind {
    /// `dataset=series`: one descriptor row per series.
    Series,
    /// `dataset=values`: the records or points themselves.
    Values,
}

/// The signal and dataset one durable write landed in, as the two partition
/// keys of its path.
#[attribute_set(item, measurement)]
#[derive(Debug, Clone, Copy)]
pub(super) struct DatasetAttrs {
    /// Destination signal, `logs` or `metrics`.
    pub signal: SignalType,
    /// Destination dataset.
    pub dataset: DatasetKind,
}

impl From<Dataset> for DatasetAttrs {
    fn from(dataset: Dataset) -> Self {
        let (signal, dataset) = match dataset {
            Dataset::LogsSeries => (SignalType::Logs, DatasetKind::Series),
            Dataset::LogsValues => (SignalType::Logs, DatasetKind::Values),
            Dataset::MetricsSeries => (SignalType::Metrics, DatasetKind::Series),
            Dataset::MetricsValues => (SignalType::Metrics, DatasetKind::Values),
        };
        Self { signal, dataset }
    }
}

/// Rows and files that reached object storage, split by dataset.
#[metric_set(name = "exporter.series_parquet", measurement_attributes = DatasetAttrs)]
#[derive(Debug, Default, Clone)]
pub(super) struct WrittenMetrics {
    /// Rows in files the sink reported as written.
    #[metric(name = "rows.written", unit = "{row}")]
    pub rows_written: Counter<u64>,
    /// Files the sink reported as written.
    #[metric(name = "files.written", unit = "{file}")]
    pub files_written: Counter<u64>,
}

/// Why a descriptor row had to be written again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AttributeEnum)]
pub(super) enum EmitReason {
    /// The descriptor was absent from the bounded cache, or present but never
    /// committed. An eviction can produce this again for a known series.
    New,
    /// The descriptor was last committed to a different partition.
    Partition,
    /// A byte or request rotation inside one window forced the replacement
    /// block to re-emit descriptors it cannot assume are durable yet.
    Rotation,
}

/// Why one series row was emitted.
#[attribute_set(item, measurement)]
#[derive(Debug, Clone, Copy)]
pub(super) struct EmitAttrs {
    /// The re-emission cause.
    pub reason: EmitReason,
}

/// Series rows that reached object storage, split by why they were written.
#[metric_set(name = "exporter.series_parquet", measurement_attributes = EmitAttrs)]
#[derive(Debug, Default, Clone)]
pub(super) struct EmittedMetrics {
    /// Descriptor rows durably written.
    #[metric(name = "series.emitted", unit = "{row}")]
    pub series_emitted: Counter<u64>,
}

/// The kind of point the lake has no dataset for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, AttributeEnum)]
pub(super) enum DroppedKind {
    /// Exponential histogram points.
    ExpHistogram,
    /// Summary points.
    Summary,
    /// Metrics that carry no data.
    Empty,
}

/// The kind of dropped, unsupported point.
#[attribute_set(item, measurement)]
#[derive(Debug, Clone, Copy)]
pub(super) struct DroppedAttrs {
    /// Which unsupported kind was dropped.
    pub kind: DroppedKind,
}

/// Points dropped under the `unsupported` policy, and metrics without data,
/// split by kind.
#[metric_set(name = "exporter.series_parquet", measurement_attributes = DroppedAttrs)]
#[derive(Debug, Default, Clone)]
pub(super) struct DroppedMetrics {
    /// Rows the lake has no dataset for.
    #[metric(name = "dropped.unsupported", unit = "{row}")]
    pub dropped_unsupported: Counter<u64>,
}

/// The signal of the request a count belongs to.
#[attribute_set(item, measurement)]
#[derive(Debug, Clone, Copy)]
pub(super) struct SignalAttrs {
    /// Source signal, `logs` or `metrics`.
    pub signal: SignalType,
}

/// Exemplars dropped under `metrics.exemplars: drop`, split by signal; only
/// `metrics` points carry exemplars.
#[metric_set(name = "exporter.series_parquet", measurement_attributes = SignalAttrs)]
#[derive(Debug, Default, Clone)]
pub(super) struct ExemplarMetrics {
    /// Exemplars of stored or dropped points that no dataset keeps.
    #[metric(name = "dropped.exemplars", unit = "{exemplar}")]
    pub dropped_exemplars: Counter<u64>,
}

/// String values stored with U+FFFD in place of invalid UTF-8, split by
/// signal.
#[metric_set(name = "exporter.series_parquet", measurement_attributes = SignalAttrs)]
#[derive(Debug, Default, Clone)]
pub(super) struct RepairedMetrics {
    /// String values of admitted requests the OTLP conversion repaired; a
    /// dictionary value counts once per row that references it.
    #[metric(name = "repaired.invalid_utf8", unit = "{value}")]
    pub invalid_utf8: Counter<u64>,
}

/// One configured denormalized physical column.
///
/// A registration attribute: one metric set is registered per configured
/// column at startup, and no request can add another.
#[attribute_set(item, registration)]
#[derive(Debug, Clone)]
pub(super) struct ColumnAttrs {
    /// Physical column name from `logs.denormalize` or `metrics.denormalize`.
    pub column: String,
}

/// Denormalization failures of one configured column.
#[metric_set(name = "exporter.series_parquet", registration_attributes = ColumnAttrs)]
#[derive(Debug, Default, Clone)]
pub(super) struct ColumnMetrics {
    /// Values stored as null because the attribute had another type.
    #[metric(name = "denormalize.type_mismatch", unit = "{value}")]
    pub mismatch: Counter<u64>,
}

/// Every metric set one worker owns.
pub(super) struct Metrics {
    /// Unlabelled worker state and totals.
    pub worker: MetricSet<WorkerMetrics>,
    /// Flushes started, by rotation trigger.
    pub flush: MeasurementMetricSet<FlushMetrics>,
    /// Failed flushes, by failure class.
    pub flush_failures: MeasurementMetricSet<FlushFailureMetrics>,
    /// Flush cleanups that found unconfirmed objects, by finding.
    pub late_commits: MeasurementMetricSet<LateCommitMetrics>,
    /// Refused requests, by refusal class.
    pub nacks: MeasurementMetricSet<NackMetrics>,
    /// Durable rows and files, by dataset.
    pub written: MeasurementMetricSet<WrittenMetrics>,
    /// Durable descriptor rows, by re-emission cause.
    pub emitted: MeasurementMetricSet<EmittedMetrics>,
    /// Dropped unsupported points, by kind.
    pub dropped: MeasurementMetricSet<DroppedMetrics>,
    /// Dropped exemplars, by signal.
    pub exemplars: MeasurementMetricSet<ExemplarMetrics>,
    /// Repaired invalid UTF-8 string values, by signal.
    pub repaired: MeasurementMetricSet<RepairedMetrics>,
    /// One set per configured denormalized column.
    columns: BTreeMap<String, MetricSet<ColumnMetrics>>,
    /// The shared `exporter.exports` set every exporter registers, so this
    /// one appears in cross-exporter views: one terminal outcome per request,
    /// by signal, with the time from receipt to decision.
    ///
    /// Taken by the worker's notifier, which is where every decision is made;
    /// see [`Metrics::take_exports`].
    exports: Option<MeasurementMetricSet<ExporterExportMetrics>>,
}

impl Metrics {
    /// Register every set of one worker against its pipeline context.
    ///
    /// The per-column sets are registered here, once, from the configuration
    /// the node started with: a column that is not configured can never gain a
    /// metric set later, whatever a request contains.
    pub(super) fn register(ctx: &PipelineContext, cfg: &LakeConfig) -> Self {
        let mut columns: BTreeMap<String, MetricSet<ColumnMetrics>> = BTreeMap::new();
        for denormalize in cfg
            .logs
            .denormalize
            .iter()
            .chain(cfg.metrics.denormalize.iter())
        {
            if !columns.contains_key(&denormalize.column) {
                let _ = columns.insert(
                    denormalize.column.clone(),
                    ColumnMetrics::register(
                        ctx,
                        &ColumnAttrs {
                            column: denormalize.column.clone(),
                        },
                    ),
                );
            }
        }
        Self {
            worker: WorkerMetrics::register(ctx),
            flush: FlushMetrics::register(ctx),
            flush_failures: FlushFailureMetrics::register(ctx),
            late_commits: LateCommitMetrics::register(ctx),
            nacks: NackMetrics::register(ctx),
            written: WrittenMetrics::register(ctx),
            emitted: EmittedMetrics::register(ctx),
            dropped: DroppedMetrics::register(ctx),
            exemplars: ExemplarMetrics::register(ctx),
            repaired: RepairedMetrics::register(ctx),
            columns,
            exports: Some(ExporterExportMetrics::register(ctx)),
        }
    }

    /// Hand the shared export-outcome set to the component that decides each
    /// request. Returns `None` once taken.
    pub(super) fn take_exports(&mut self) -> Option<MeasurementMetricSet<ExporterExportMetrics>> {
        self.exports.take()
    }

    /// Record what extracting one request produced.
    ///
    /// Called exactly once per request, when it is extracted; resuming a
    /// parked request never counts its extraction again. A mismatch reported
    /// for a column that was not configured is ignored, so the column label
    /// stays bounded by configuration.
    pub(super) fn extracted(&mut self, stats: &ExtractStats) {
        self.worker
            .timestamp_out_of_range
            .add(stats.timestamp_out_of_range);
        for (kind, count) in [
            (DroppedKind::ExpHistogram, stats.dropped_exp_histogram),
            (DroppedKind::Summary, stats.dropped_summary),
            (DroppedKind::Empty, stats.dropped_empty_metrics),
        ] {
            if count != 0 {
                self.dropped
                    .with(DroppedAttrs { kind })
                    .dropped_unsupported
                    .add(count);
            }
        }
        // Exemplars exist only on metric points.
        if stats.dropped_exemplars != 0 {
            self.exemplars
                .with(SignalAttrs {
                    signal: SignalType::Metrics,
                })
                .dropped_exemplars
                .add(stats.dropped_exemplars);
        }
        for (column, count) in &stats.denorm_type_mismatch_by_column {
            if let Some(metrics) = self.columns.get_mut(column) {
                metrics.mismatch.add(*count);
            }
        }
    }

    /// Count one flush that did not put its block in object storage.
    pub(super) fn flush_failed(&mut self, error_type: WriteFailure) {
        self.flush_failures
            .with(FlushFailureAttrs { error_type })
            .failures
            .add(1);
    }

    /// Record the string values the conversion of one admitted request of
    /// `signal` repaired; called with [`Metrics::extracted`].
    pub(super) fn repaired(&mut self, signal: SignalType, count: u64) {
        if count != 0 {
            self.repaired
                .with(SignalAttrs { signal })
                .invalid_utf8
                .add(count);
        }
    }

    /// Hand every set to the collector on a `CollectTelemetry` message.
    pub(super) fn report(&mut self, reporter: &mut MetricsReporter) {
        if let Some(exports) = &mut self.exports {
            let _ = reporter.report_measurement(exports);
        }
        let _ = reporter.report(&mut self.worker);
        let _ = reporter.report_measurement(&mut self.flush);
        let _ = reporter.report_measurement(&mut self.flush_failures);
        let _ = reporter.report_measurement(&mut self.late_commits);
        let _ = reporter.report_measurement(&mut self.nacks);
        let _ = reporter.report_measurement(&mut self.written);
        let _ = reporter.report_measurement(&mut self.emitted);
        let _ = reporter.report_measurement(&mut self.dropped);
        let _ = reporter.report_measurement(&mut self.exemplars);
        let _ = reporter.report_measurement(&mut self.repaired);
        for metrics in self.columns.values_mut() {
            let _ = reporter.report(metrics);
        }
    }

    /// Take every set for terminal handoff, so the last interval is not lost.
    pub(super) fn snapshots(&mut self) -> Vec<MetricSetSnapshot> {
        let mut out = self.worker.terminal_snapshots();
        if let Some(exports) = &mut self.exports {
            out.extend(exports.terminal_snapshots());
        }
        out.extend(self.flush.terminal_snapshots());
        out.extend(self.flush_failures.terminal_snapshots());
        out.extend(self.late_commits.terminal_snapshots());
        out.extend(self.nacks.terminal_snapshots());
        out.extend(self.written.terminal_snapshots());
        out.extend(self.emitted.terminal_snapshots());
        out.extend(self.dropped.terminal_snapshots());
        out.extend(self.exemplars.terminal_snapshots());
        out.extend(self.repaired.terminal_snapshots());
        for metrics in self.columns.values_mut() {
            out.extend(metrics.terminal_snapshots());
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_series_lake::config::{DenormType, Denormalize};

    /// Unit words the semantic-conventions guide keeps out of metric names:
    /// the unit is metadata, never part of the name.
    const UNIT_WORDS: [&str; 10] = [
        "bytes", "byte", "seconds", "second", "secs", "ms", "ns", "us", "count", "total",
    ];

    /// Panic if any dot or underscore separated word of `name` is a unit.
    fn assert_no_unit_in_name(name: &str) {
        for word in name.split(['.', '_']) {
            assert!(
                !UNIT_WORDS.contains(&word),
                "metric name {name:?} carries the unit word {word:?}"
            );
        }
    }

    /// Scenario: names in the metrics guide's form and names ending in a unit word.
    /// Guarantees: `_bytes`, `_seconds` and `_count` suffixes are rejected.
    #[test]
    fn a_unit_in_a_metric_name_is_rejected() {
        for name in [
            "block.active",
            "notify.token_size",
            "oldest_unacked.age",
            "flushes",
        ] {
            assert_no_unit_in_name(name);
        }
        for name in [
            "block.active_bytes",
            "oldest_unacked_seconds",
            "flush.count",
        ] {
            assert!(
                std::panic::catch_unwind(|| assert_no_unit_in_name(name)).is_err(),
                "{name} must be rejected"
            );
        }
    }

    /// Assert one snapshot's descriptor name, its ordered metric names and
    /// units, and the measurement labels its bucket decodes to; no metric
    /// name may carry a unit word.
    fn assert_schema(
        snapshot: &MetricSetSnapshot,
        fields: &[(&str, &str)],
        labels: &[(&str, &str)],
    ) {
        assert_eq!(snapshot.descriptor().name, "exporter.series_parquet");
        for metric in snapshot.descriptor().metrics {
            assert_no_unit_in_name(metric.name);
        }
        let actual: Vec<_> = snapshot
            .descriptor()
            .metrics
            .iter()
            .map(|metric| (metric.name, metric.unit))
            .collect();
        assert_eq!(actual, fields);
        let actual: Vec<_> = snapshot.measurement_attributes().collect();
        assert_eq!(actual, labels);
    }

    /// Scenario: every exporter metric set is registered and each closed label bucket touched.
    /// Guarantees: names, units and label values are exactly the documented ones.
    #[test]
    fn series_metric_schema_is_exact() {
        let (ctx, registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
        let mut cfg = LakeConfig::default();
        cfg.logs.denormalize.push(Denormalize {
            path: "resource.host.id".into(),
            column: "host_col".into(),
            ty: DenormType::String,
        });
        let mut metrics = Metrics::register(&ctx, &cfg);

        assert_schema(
            &metrics.worker.snapshot(),
            &[
                ("series_cache.entries", "{entry}"),
                ("series_cache.hits", "{lookup}"),
                ("series_cache.misses", "{lookup}"),
                ("series_cache.evictions", "{entry}"),
                ("block.active", "By"),
                ("block.flushing", "By"),
                ("block.pending", "By"),
                ("block.requests_pending", "{request}"),
                ("block.pending_slot_occupied", "{slot}"),
                ("flush.duration", "s"),
                ("flush.retries", "{attempt}"),
                ("flush.cancelled", "{flush}"),
                ("flush.abort_failures", "{upload}"),
                ("acks", "{message}"),
                ("notify.queued", "{request}"),
                ("notify.token_size", "By"),
                ("notify.failures", "{request}"),
                ("notify.lost_acks", "{request}"),
                ("oldest_unacked.age", "s"),
                ("admission.closed", "{state}"),
                ("admission.closures", "{closure}"),
                ("admission.closed.duration", "s"),
                ("timestamp.out_of_range", "{timestamp}"),
                ("memory.budget", "By"),
                ("memory.accounted", "By"),
                ("flush.workspace", "By"),
            ],
            &[],
        );

        for (reason, label) in [
            (FlushReason::Time, "time"),
            (FlushReason::Bytes, "bytes"),
            (FlushReason::Requests, "requests"),
            (FlushReason::Shutdown, "shutdown"),
        ] {
            metrics.flush.with(FlushAttrs { reason }).count.add(1);
            let snapshots = metrics.flush.terminal_snapshots();
            assert_eq!(snapshots.len(), 1);
            assert_schema(
                &snapshots[0],
                &[("flushes", "{flush}")],
                &[("reason", label)],
            );
        }

        for (error_type, label) in [
            (WriteFailure::Deadline, "deadline"),
            (WriteFailure::PermanentStorage, "permanent_storage"),
            (WriteFailure::Cancelled, "cancelled"),
            (WriteFailure::Encode, "encode"),
            (WriteFailure::Internal, "internal"),
        ] {
            metrics.flush_failed(error_type);
            let snapshots = metrics.flush_failures.terminal_snapshots();
            assert_eq!(snapshots.len(), 1);
            assert_schema(
                &snapshots[0],
                &[("block.write_failures", "{block}")],
                &[("error.type", label)],
            );
        }

        for (outcome, label) in
            LateCommit::ALL
                .into_iter()
                .zip(["stored", "acknowledged", "partial", "unknown"])
        {
            metrics
                .late_commits
                .with(LateCommitAttrs { outcome })
                .late_commits
                .add(1);
            let snapshots = metrics.late_commits.terminal_snapshots();
            assert_eq!(snapshots.len(), 1);
            assert_schema(
                &snapshots[0],
                &[("flush.late_commits", "{flush}")],
                &[("outcome", label)],
            );
        }

        let labels = [
            "storage",
            "request_too_large",
            "extracted_too_large",
            "row_too_large",
            "too_many_series",
            "token_too_large",
            "too_deep",
            "invalid",
            "unsupported",
            "shutdown",
            "internal",
        ];
        for (error_type, label) in NackAttrs::error_types().zip(labels) {
            metrics
                .nacks
                .with(NackAttrs { error_type })
                .nacks
                .observe(1);
            let snapshots = metrics.nacks.terminal_snapshots();
            assert_eq!(snapshots.len(), 1);
            assert_schema(
                &snapshots[0],
                &[("nacks", "{message}")],
                &[("error.type", label)],
            );
        }

        for (dataset, signal, kind) in [
            (Dataset::LogsSeries, "logs", "series"),
            (Dataset::LogsValues, "logs", "values"),
            (Dataset::MetricsSeries, "metrics", "series"),
            (Dataset::MetricsValues, "metrics", "values"),
        ] {
            metrics
                .written
                .with(DatasetAttrs::from(dataset))
                .rows_written
                .add(1);
            let snapshots = metrics.written.terminal_snapshots();
            assert_eq!(snapshots.len(), 1);
            assert_schema(
                &snapshots[0],
                &[("rows.written", "{row}"), ("files.written", "{file}")],
                &[("signal", signal), ("dataset", kind)],
            );
        }

        for (reason, label) in [
            (EmitReason::New, "new"),
            (EmitReason::Partition, "partition"),
            (EmitReason::Rotation, "rotation"),
        ] {
            metrics
                .emitted
                .with(EmitAttrs { reason })
                .series_emitted
                .add(1);
            let snapshots = metrics.emitted.terminal_snapshots();
            assert_eq!(snapshots.len(), 1);
            assert_schema(
                &snapshots[0],
                &[("series.emitted", "{row}")],
                &[("reason", label)],
            );
        }

        for (kind, label) in [
            (DroppedKind::ExpHistogram, "exp_histogram"),
            (DroppedKind::Summary, "summary"),
            (DroppedKind::Empty, "empty"),
        ] {
            metrics
                .dropped
                .with(DroppedAttrs { kind })
                .dropped_unsupported
                .add(1);
            let snapshots = metrics.dropped.terminal_snapshots();
            assert_eq!(snapshots.len(), 1);
            assert_schema(
                &snapshots[0],
                &[("dropped.unsupported", "{row}")],
                &[("kind", label)],
            );
        }

        metrics
            .exemplars
            .with(SignalAttrs {
                signal: SignalType::Metrics,
            })
            .dropped_exemplars
            .add(1);
        let snapshots = metrics.exemplars.terminal_snapshots();
        assert_eq!(snapshots.len(), 1);
        assert_schema(
            &snapshots[0],
            &[("dropped.exemplars", "{exemplar}")],
            &[("signal", "metrics")],
        );

        for (signal, label) in [(SignalType::Logs, "logs"), (SignalType::Metrics, "metrics")] {
            metrics.repaired(signal, 1);
            let snapshots = metrics.repaired.terminal_snapshots();
            assert_eq!(snapshots.len(), 1);
            assert_schema(
                &snapshots[0],
                &[("repaired.invalid_utf8", "{value}")],
                &[("signal", label)],
            );
        }

        assert_eq!(
            metrics
                .columns
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["host_col"]
        );
        assert_schema(
            &metrics.columns["host_col"].snapshot(),
            &[("denormalize.type_mismatch", "{value}")],
            &[],
        );
        metrics
            .columns
            .get_mut("host_col")
            .expect("column")
            .mismatch
            .add(1);
        let snapshot = metrics.columns["host_col"].snapshot();
        registry.accumulate_metric_set_snapshot(
            snapshot.key(),
            snapshot.bucket(),
            snapshot.get_metrics(),
        );
        let batch = registry.drain_metric_export_batch();
        let column = batch
            .metric_sets
            .iter()
            .find(|set| {
                set.descriptor
                    .metrics
                    .iter()
                    .any(|metric| metric.name == "denormalize.type_mismatch")
            })
            .expect("column export");
        assert_eq!(
            column.item_attributes,
            vec![("column".into(), "host_col".into())]
        );
        assert!(metrics.emitted.terminal_snapshots().is_empty());
    }

    /// Scenario: one extraction reports every unsupported kind, an out-of-range timestamp and
    /// mismatches for a configured and an unconfigured column.
    /// Guarantees: each lands in its bucket and the unconfigured column registers no label.
    #[test]
    fn extraction_counters_stay_bounded_by_configuration() {
        let (ctx, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
        let mut cfg = LakeConfig::default();
        cfg.metrics.denormalize.push(Denormalize {
            path: "resource.host.id".into(),
            column: "host_col".into(),
            ty: DenormType::String,
        });
        let mut metrics = Metrics::register(&ctx, &cfg);

        let mut stats = ExtractStats {
            timestamp_out_of_range: 3,
            dropped_exp_histogram: 5,
            dropped_summary: 7,
            dropped_empty_metrics: 13,
            dropped_exemplars: 11,
            ..ExtractStats::default()
        };
        let _ = stats
            .denorm_type_mismatch_by_column
            .insert("host_col".into(), 2);
        let _ = stats
            .denorm_type_mismatch_by_column
            .insert("never_configured".into(), 9);
        metrics.extracted(&stats);

        assert_eq!(metrics.worker.timestamp_out_of_range.get(), 3);
        assert_eq!(
            metrics
                .dropped
                .get(DroppedAttrs {
                    kind: DroppedKind::ExpHistogram
                })
                .dropped_unsupported
                .get(),
            5
        );
        assert_eq!(
            metrics
                .dropped
                .get(DroppedAttrs {
                    kind: DroppedKind::Summary
                })
                .dropped_unsupported
                .get(),
            7
        );
        assert_eq!(
            metrics
                .dropped
                .get(DroppedAttrs {
                    kind: DroppedKind::Empty
                })
                .dropped_unsupported
                .get(),
            13
        );
        assert_eq!(
            metrics
                .exemplars
                .get(SignalAttrs {
                    signal: SignalType::Metrics
                })
                .dropped_exemplars
                .get(),
            11,
            "exemplars have a counter of their own, not a kind of dropped.unsupported"
        );
        assert_eq!(
            metrics
                .columns
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["host_col"]
        );
        assert_eq!(metrics.columns["host_col"].mismatch.get(), 2);
    }
}
