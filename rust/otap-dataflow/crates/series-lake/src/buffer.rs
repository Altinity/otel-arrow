// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Sorted run buffers and the ACTIVE/FLUSHING block.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow::array::{ArrayRef, Int64Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use otel_arrow_dfe_pdata::otap::memory::{CountedAllocations, record_batch_pinned_bytes};

use crate::cache::SeriesCache;
use crate::canonical::SeriesId;
use crate::clock::PartitionId;
use crate::config::{LakeConfig, check_token};
use crate::error::{Error, RefuseReason, Result};
use crate::extract::{DescriptorRow, Extracted, series_batch};
use crate::schema::Dataset;
use crate::sort::{SortSpec, sort_batch, sort_batches};

/// Building batches plus sealed sorted runs for one dataset.
pub struct SortedTableBuffer {
    dataset: Dataset,
    spec: SortSpec,
    run_target: usize,
    building: Vec<RecordBatch>,
    building_bytes: usize,
    runs: Vec<RecordBatch>,
    seen: CountedAllocations,
    rows: usize,
}

impl SortedTableBuffer {
    /// New empty buffer.
    #[must_use]
    pub fn new(dataset: Dataset, spec: SortSpec, run_target_bytes: usize) -> Self {
        Self {
            dataset,
            spec,
            run_target: run_target_bytes.max(1),
            building: Vec::new(),
            building_bytes: 0,
            runs: Vec::new(),
            seen: CountedAllocations::default(),
            rows: 0,
        }
    }

    /// Append a batch; returns the pinned bytes newly retained by this buffer.
    ///
    /// The returned value is an upper bound used while the block fills: it never
    /// sees the bytes that `seal` releases when the building batches are replaced
    /// by one sorted run. `Block::seal` recomputes the truth.
    ///
    /// Deduplication is scoped to the run being built, never wider. `seen` holds
    /// raw buffer addresses, which are only meaningful while the buffers they
    /// name are alive; every batch it has measured is still held in `building`,
    /// so no address in it can have been freed and handed back out by the
    /// allocator. A set that outlived a seal could match a freed address against
    /// a fresh batch, report zero bytes for it and stall `building_bytes` below
    /// `run_target` forever.
    ///
    /// # Errors
    /// Propagates an Arrow failure from the sort of a sealed run.
    pub fn append(&mut self, batch: RecordBatch) -> Result<usize> {
        let pinned = record_batch_pinned_bytes(&batch, &mut self.seen);
        self.rows += batch.num_rows();
        self.building_bytes += pinned;
        self.building.push(batch);
        if self.building_bytes >= self.run_target {
            self.seal()?;
        }
        Ok(pinned)
    }

    /// Sort and seal the building batches into one run.
    ///
    /// With sorting disabled the building batches become runs as they are,
    /// sharing their buffers, since a concatenation would copy every row. A
    /// sorted seal produces exactly one run for the k-way merge.
    ///
    /// No re-accounting happens here, which would be quadratic in the number of
    /// runs; `Block::seal` performs one deduplicated recount instead.
    ///
    /// # Errors
    /// Propagates an Arrow failure from the sort.
    pub fn seal(&mut self) -> Result<()> {
        if self.building.is_empty() {
            return Ok(());
        }
        if self.spec.is_empty() {
            self.runs.append(&mut self.building);
        } else {
            let sorted = sort_batches(&self.building, &self.spec)?;
            self.runs.push(sorted);
            self.building.clear();
        }
        self.building_bytes = 0;
        // The addresses in `seen` name buffers this buffer no longer measures
        // against. Start the next run with an empty set so a reused address
        // cannot silently zero out a fresh batch.
        self.seen = CountedAllocations::default();
        Ok(())
    }

    /// Sealed runs.
    #[must_use]
    pub fn runs(&self) -> &[RecordBatch] {
        &self.runs
    }

    /// Unsealed batches.
    #[must_use]
    pub fn building(&self) -> &[RecordBatch] {
        &self.building
    }

    /// Total rows.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Whether nothing was appended.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Dataset.
    #[must_use]
    pub fn dataset(&self) -> Dataset {
        self.dataset
    }

    /// Sort spec.
    #[must_use]
    pub fn spec(&self) -> &SortSpec {
        &self.spec
    }

    /// Append request-local descriptors as independently bounded sorted series runs.
    ///
    /// A request may produce several runs; each is built, sorted and measured on
    /// its own, so at most one candidate run plus the sort's own scratch is live
    /// at a time. A candidate whose measured size overshoots `run_target` is
    /// halved and rebuilt. No block-sized series batch is ever constructed.
    ///
    /// A single row may exceed `run_target` by its Arrow overhead and becomes
    /// its own run: `run_target_bytes` is a packing target, and the admission
    /// limits are `max_row_bytes` and `max_block_bytes`.
    ///
    /// # Errors
    /// Propagates a series-batch or sort failure.
    fn append_series(&mut self, rows: &[&DescriptorRow], cfg: &LakeConfig) -> Result<()> {
        let mut start = 0;
        while start < rows.len() {
            let mut end = start;
            let mut estimated = 0usize;
            while end < rows.len() {
                let next = rows[end].series_row_bytes();
                if end > start && estimated.saturating_add(next) > self.run_target {
                    break;
                }
                estimated = estimated.saturating_add(next);
                end += 1;
                if estimated >= self.run_target {
                    break;
                }
            }
            let sorted = loop {
                let batch = series_batch(&rows[start..end], 0, self.dataset, cfg)?;
                let sorted = sort_batch(&batch, &self.spec)?;
                drop(batch);
                let bytes = record_batch_pinned_bytes(&sorted, &mut CountedAllocations::default());
                if bytes <= self.run_target || end == start + 1 {
                    break sorted;
                }
                drop(sorted);
                end = start + (end - start) / 2;
            };
            self.rows += sorted.num_rows();
            self.runs.push(sorted);
            start = end;
        }
        Ok(())
    }

    /// Prepare the run that `seal` would make of the building batches, without
    /// mutating anything.
    ///
    /// The non-mutating half of [`SortedTableBuffer::seal`], used by
    /// `Block::seal` so that finalizing a values table is part of the same
    /// all-or-nothing transaction as stamping the series tables. An empty
    /// building set produces no run, and with sorting disabled the building
    /// batches become runs as they are (see [`SortedTableBuffer::seal`]).
    ///
    /// # Errors
    /// Propagates an Arrow failure from the sort.
    fn finalized(&self) -> Result<Vec<RecordBatch>> {
        if self.building.is_empty() {
            return Ok(Vec::new());
        }
        if self.spec.is_empty() {
            return Ok(self.building.clone());
        }
        Ok(vec![sort_batches(&self.building, &self.spec)?])
    }

    /// Prepare every stamp replacement without mutating the retained batches.
    ///
    /// Returns the replacement runs and building batches. Every fallible step
    /// happens here, so a caller that gets an error has a buffer that is still
    /// exactly what it was.
    ///
    /// # Errors
    /// Rejects a series batch whose `emitted_at` is not a UTC microsecond
    /// timestamp, and propagates an Arrow failure from rebuilding the batch.
    fn stamped(&self, stamp: i64) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>)> {
        let replace = |batch: &RecordBatch| -> Result<RecordBatch> {
            let schema = batch.schema();
            let index = schema.index_of("emitted_at")?;
            let expected = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")));
            if schema.field(index).data_type() != &expected {
                return Err(Error::internal(
                    "series emitted_at must be a UTC microsecond timestamp",
                ));
            }
            // One i64 per row, wrapped in the timestamp type without a cast or a
            // copy: every other column of the batch is shared with the original.
            let ints = Int64Array::from(vec![stamp; batch.num_rows()]);
            let values =
                TimestampMicrosecondArray::new(ints.values().clone(), None).with_timezone("UTC");
            let mut columns = batch.columns().to_vec();
            columns[index] = Arc::new(values) as ArrayRef;
            Ok(RecordBatch::try_new(schema, columns)?)
        };
        let runs = self.runs.iter().map(replace).collect::<Result<Vec<_>>>()?;
        let building = self
            .building
            .iter()
            .map(replace)
            .collect::<Result<Vec<_>>>()?;
        Ok((runs, building))
    }

    /// Building batches then sealed runs, for bounded snapshot copies.
    pub fn iter_snapshots(&self) -> impl Iterator<Item = &RecordBatch> {
        self.building.iter().chain(self.runs.iter())
    }
}

/// Outcome of a reservation: bytes to charge and descriptors to emit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reservation {
    /// Bytes the request will add to the block.
    pub bytes: usize,
    /// The part of `bytes` that is the ack token.
    pub token_bytes: usize,
    /// Indices into `Extracted::descriptors` whose descriptor must be written by this block.
    pub new_series: Vec<usize>,
}

/// One ACTIVE or FLUSHING block.
///
/// The block counts the requests admitted to it and nothing more: the
/// completions they are owed are held by the caller, beside the block, so a
/// flush can own the rows without owning the routing contexts.
pub struct Block {
    /// Window start (Unix seconds).
    pub window_start_secs: i64,
    /// Destination partition.
    pub partition: PartitionId,
    /// Per-worker sequence number used in file names.
    pub seq: u64,
    tables: BTreeMap<Dataset, SortedTableBuffer>,
    /// Series whose descriptor this block carries.
    pub pending_series: HashSet<SeriesId>,
    /// Accounted bytes: an upper bound while filling, exact after `seal`.
    pub bytes: usize,
    /// Requests admitted.
    requests: usize,
    token_bytes: usize,
    emitted_at_us: Option<i64>,
    /// The configuration the block was opened under, shared with every other
    /// block of the same writer; its limits decide every reservation.
    cfg: Arc<LakeConfig>,
    /// Write attempts started on this block (see [`Block::begin_write_attempt`]).
    write_attempts: AtomicU32,
}

impl Block {
    /// New empty block for a window.
    #[must_use]
    pub fn new(window_start_secs: i64, seq: u64, cfg: impl Into<Arc<LakeConfig>>) -> Self {
        Self {
            window_start_secs,
            partition: PartitionId::from_unix_secs(window_start_secs),
            seq,
            tables: BTreeMap::new(),
            pending_series: HashSet::new(),
            bytes: 0,
            requests: 0,
            token_bytes: 0,
            emitted_at_us: None,
            cfg: cfg.into(),
            write_attempts: AtomicU32::new(0),
        }
    }

    /// Count one more write attempt of this block and return its number, 1
    /// for the first.
    ///
    /// Frozen file names carry the writer's boot id and the block's sequence
    /// number, so before the first attempt no request of this process or any
    /// other could have written them: an object found under one of them during
    /// the first attempt was written by that attempt.
    pub(crate) fn begin_write_attempt(&self) -> u32 {
        self.write_attempts
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }

    fn spec_for(&self, ds: Dataset) -> SortSpec {
        SortSpec::for_dataset(ds, &self.cfg)
    }

    /// Compute what admitting `extracted` would add.
    ///
    /// Refuses before touching anything:
    /// * `TokenTooLarge` when the completion token is larger than
    ///   [`TOKEN_ALLOWANCE_BYTES`](crate::config::TOKEN_ALLOWANCE_BYTES), a
    ///   permanent refusal (see [`check_token`]);
    /// * an internal error when the request's worst case (every descriptor it
    ///   carries written by this block; see
    ///   [`LakeConfig::series_row_fixed_bytes`]) exceeds `max_block_bytes`.
    ///   [`LakeConfig::check_request_bound`] makes that unreachable for a
    ///   request that passed extraction, so under a valid configuration it is
    ///   also a debug assertion; the check never depends on the cache;
    /// * `TooManyRequests` when the block already holds `max_requests_per_block`;
    /// * `BlockFull` when the request does not fit the remaining budget.
    ///
    /// The last two tell the caller to rotate and offer the request to the next
    /// block. The block is never mutated, and the cache is only written (through
    /// `touch`) once the reservation is accepted; the `is_committed` lookups
    /// move only LRU recency, which is not correctness state.
    ///
    /// # Errors
    /// Returns one of the four refusals above.
    pub fn reserve(
        &self,
        extracted: &Extracted,
        cache: &mut SeriesCache,
        token_bytes: usize,
    ) -> Result<Reservation> {
        self.reserve_with_reemit(extracted, cache, token_bytes, false)
    }

    /// Compute what admitting `extracted` would add, optionally repeating every
    /// descriptor not already pending in this block.
    ///
    /// With `reemit` false this is exactly [`Block::reserve`]. With `reemit`
    /// true, a descriptor the cache reports committed in this partition is
    /// still reserved unless this block already carries it: a block replacing
    /// one sealed inside the same window cannot rely on its predecessor's
    /// descriptors being written. Only this reservation ignores the cache.
    ///
    /// # Errors
    /// Returns one of the four refusals documented on [`Block::reserve`].
    pub fn reserve_with_reemit(
        &self,
        extracted: &Extracted,
        cache: &mut SeriesCache,
        token_bytes: usize,
        reemit: bool,
    ) -> Result<Reservation> {
        let limits = &self.cfg.ingress;
        // The one term of the worst case extraction does not bound; within
        // the allowance, `LakeConfig::check_request_bound` makes the check
        // below unreachable.
        check_token(token_bytes)?;
        // The merge keys exist only while the block's tables are written, but
        // they are reserved from admission on, so a block and the keys of the
        // table being written stay within `max_block_bytes` together.
        let fixed = extracted
            .pinned_bytes
            .saturating_add(extracted.merge_key_bytes)
            .saturating_add(token_bytes);
        let worst = extracted
            .descriptors
            .iter()
            .map(|d| d.series_row_bytes() + limits.pending_series_entry_bytes)
            .fold(fixed, usize::saturating_add);
        if worst > limits.max_block_bytes {
            debug_assert!(
                self.cfg.validate().is_err(),
                "a request that passed ingress needs {worst} bytes of an empty block of {}",
                limits.max_block_bytes
            );
            return Err(Error::internal(format!(
                "the request's worst case in one block, {worst} bytes, exceeds \
                 max_block_bytes ({}), which the configuration bounds it by",
                limits.max_block_bytes
            )));
        }
        let mut bytes = fixed;
        let mut new_series = Vec::new();
        for (i, d) in extracted.descriptors.iter().enumerate() {
            let committed_here = cache.is_committed(&d.series_id, self.partition);
            if (reemit || !committed_here) && !self.pending_series.contains(&d.series_id) {
                new_series.push(i);
                bytes += d.series_row_bytes() + limits.pending_series_entry_bytes;
            }
        }
        if self.requests >= limits.max_requests_per_block {
            return Err(Error::Refused(RefuseReason::TooManyRequests));
        }
        if self.bytes + bytes > limits.max_block_bytes {
            return Err(Error::Refused(RefuseReason::BlockFull));
        }
        for d in &extracted.descriptors {
            cache.touch(d.series_id);
        }
        Ok(Reservation {
            bytes,
            token_bytes,
            new_series,
        })
    }

    /// Admit a reserved request, materializing series rows with a zero stamp.
    ///
    /// Descriptor rows become Arrow series rows here, in bounded sorted runs,
    /// with `emitted_at` left at zero until the block seals. Values batches
    /// accumulate into the building run of their table and are sealed into a
    /// sorted run once they cross `run_target_bytes`, so a run packs batches
    /// from as many requests as fit it.
    ///
    /// A sealed block takes nothing more. Its `emitted_at` is already fixed, so
    /// a late descriptor would be stamped with a time before it arrived, and its
    /// `bytes` is already the exact recount, which a reservation's upper-bound
    /// estimate would corrupt. The caller rotates to a new block instead.
    ///
    /// An admission that fails leaves the block partially updated on purpose:
    /// the caller's contract is to discard the whole ACTIVE block, and it owns
    /// the request's ack token separately.
    ///
    /// # Errors
    /// Refuses a sealed block, and propagates a series-construction or run
    /// sorting failure.
    pub fn admit(&mut self, extracted: Extracted, reservation: Reservation) -> Result<()> {
        if self.is_sealed() {
            return Err(Error::internal("block already sealed"));
        }
        let Extracted {
            signal,
            descriptors,
            values,
            ..
        } = extracted;
        if !reservation.new_series.is_empty() {
            let ds = Dataset::series_of(signal);
            // The reservation is public, so its indices are caller-supplied
            // data: resolve them all before anything is mutated.
            let rows: Vec<&DescriptorRow> = reservation
                .new_series
                .iter()
                .map(|&i| {
                    descriptors.get(i).ok_or_else(|| {
                        Error::internal("reservation names a descriptor the request does not carry")
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            // Take the table out for the call so that `append_series` can borrow
            // it mutably while `self.cfg` is read, then put it straight back,
            // on the error path too, so the block stays well formed.
            let mut table = self.tables.remove(&ds).unwrap_or_else(|| {
                SortedTableBuffer::new(ds, SortSpec::series(), self.cfg.sorting.run_target_bytes)
            });
            let result = table.append_series(&rows, &self.cfg);
            let _ = self.tables.insert(ds, table);
            result?;
            for row in rows {
                let _ = self.pending_series.insert(row.series_id);
            }
        }
        drop(descriptors);
        for (ds, batches) in values {
            let run_target = self.cfg.sorting.run_target_bytes;
            let spec = self.spec_for(ds);
            let table = self
                .tables
                .entry(ds)
                .or_insert_with(|| SortedTableBuffer::new(ds, spec, run_target));
            for batch in batches {
                // `append` seals a run once the building batches cross
                // `run_target`, so batches from several requests pack into one
                // run. Whatever is still building when the block seals is
                // finalized there, inside the same transaction as the stamps.
                let _ = table.append(batch)?;
            }
        }
        self.bytes += reservation.bytes;
        self.token_bytes += reservation.token_bytes;
        self.requests += 1;
        Ok(())
    }

    /// Replace only `emitted_at` buffers, committing the sealed state after all
    /// swaps succeed.
    ///
    /// Idempotent: a flush retry re-seals the same block and keeps the first
    /// stamp, so the file bytes are identical across attempts (FORMAT.md section 4).
    ///
    /// All or nothing. Every replacement batch is prepared before any retained
    /// batch is touched, so a failure leaves every batch, the accounting and the
    /// seal state as they were, and the block can be sealed again. `emitted_at`
    /// is committed last, so the sink refuses a block that never finished
    /// sealing.
    ///
    /// Sealing the series tables costs one new eight-byte timestamp per series
    /// row and nothing else: every other column is shared between the old batch
    /// and its replacement. A values table pays for finalizing whatever was
    /// still building, which is one run bounded by `run_target_bytes`.
    ///
    /// # Errors
    /// A bad series schema, or an Arrow failure finalizing a values run, leaves
    /// all retained batches, accounting and seal state unchanged.
    pub fn seal(&mut self, emitted_at_us: i64) -> Result<()> {
        if self.is_sealed() {
            return Ok(());
        }
        let mut stamped = Vec::new();
        let mut finalized = Vec::new();
        for (ds, table) in &self.tables {
            if ds.is_series() {
                stamped.push((*ds, table.stamped(emitted_at_us)?));
            } else {
                finalized.push((*ds, table.finalized()?));
            }
        }
        // All fallible Arrow work has finished. The transaction holds the
        // series tables' shared non-stamp columns plus one new eight-byte
        // timestamp per series row, and one finalized run per values table.
        for (ds, (runs, building)) in stamped {
            let table = self.tables.get_mut(&ds).expect("prepared table exists");
            table.runs = runs;
            table.building = building;
        }
        for (ds, runs) in finalized {
            let table = self.tables.get_mut(&ds).expect("prepared table exists");
            table.building.clear();
            table.runs.extend(runs);
        }
        for table in self.tables.values_mut() {
            // Every batch is already sorted: the series replacements by
            // admission, the values runs by `finalized`. Moving them cannot
            // copy a column.
            table.runs.append(&mut table.building);
            table.building_bytes = 0;
            table.seen = CountedAllocations::default();
        }
        self.bytes = self.recount();
        self.emitted_at_us = Some(emitted_at_us);
        Ok(())
    }

    /// The seal timestamp, once the block has been sealed.
    #[must_use]
    pub fn emitted_at_us(&self) -> Option<i64> {
        self.emitted_at_us
    }

    /// Whether all timestamp swaps have committed successfully.
    ///
    /// Series rows exist from admission onwards, but they carry a placeholder
    /// `emitted_at` of zero until the stamp transaction commits. A consumer that
    /// walked `tables()` of an unsealed block would read unstamped data, so the
    /// sink checks this before it writes. `seal` commits `emitted_at` only after
    /// every swap has been prepared, so a block whose seal failed reports false
    /// here.
    #[must_use]
    pub fn is_sealed(&self) -> bool {
        self.emitted_at_us.is_some()
    }

    /// One deduplicated pass over everything the block retains.
    ///
    /// Every row of the block, series rows included, is already an Arrow row by
    /// the time this runs, so the batches below are the whole payload.
    fn recount(&self) -> usize {
        let mut seen = CountedAllocations::default();
        let mut bytes = 0usize;
        for t in self.tables.values() {
            for b in t.iter_snapshots() {
                bytes += record_batch_pinned_bytes(b, &mut seen);
            }
        }
        bytes += self.pending_series.len() * self.cfg.ingress.pending_series_entry_bytes;
        bytes + self.token_bytes
    }

    /// Tables in write order: series datasets first (Dataset's Ord puts LogsSeries
    /// and MetricsSeries before their values datasets).
    pub fn tables(&self) -> impl Iterator<Item = &SortedTableBuffer> {
        self.tables.values()
    }

    /// Whether the block holds no materialized rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.values().all(SortedTableBuffer::is_empty)
    }

    /// Number of admitted requests.
    #[must_use]
    pub fn request_count(&self) -> usize {
        self.requests
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::SeriesCache;
    use crate::config::{LakeConfig, Nulls, SortKey, SortOrder, TOKEN_ALLOWANCE_BYTES};
    use crate::error::RefuseReason;
    use crate::extract::extract;
    use arrow::array::AsArray;
    use arrow::datatypes::TimestampMicrosecondType;
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, LogsData, ResourceLogs, ScopeLogs,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use otel_arrow_dfe_pdata::testing::round_trip::encode_logs;

    const SEAL_AT_US: i64 = 1_700_000_000_000_000;

    fn logs(host: &str, n: usize) -> LogsData {
        let kv = |k: &str, v: &str| KeyValue {
            key: k.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.into())),
            }),
        };
        LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", host)],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: (0..n)
                        .map(|i| LogRecord {
                            time_unix_nano: 1000 + i as u64,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn extracted(cfg: &LakeConfig, host: &str, n: usize) -> Extracted {
        let mut records = encode_logs(&logs(host, n));
        extract(&mut records, cfg).expect("extract")
    }

    fn snapshot_bytes<'a>(batches: impl Iterator<Item = &'a RecordBatch>) -> usize {
        let mut seen = CountedAllocations::default();
        batches
            .map(|batch| record_batch_pinned_bytes(batch, &mut seen))
            .sum()
    }

    /// Scenario: many distinct requests fill a block before its final timestamp is known.
    /// Guarantees: descriptors are already bounded sorted series runs and carry zero timestamps.
    #[test]
    fn admission_materializes_bounded_series_runs() {
        let mut cfg = LakeConfig::default();
        cfg.sorting.run_target_bytes = 64 * 1024;
        cfg.ingress.max_row_bytes = 16 * 1024;
        let mut cache = SeriesCache::new(100);
        let mut block = Block::new(0, 1, cfg.clone());
        for i in 0..64 {
            let e = extracted(&cfg, &format!("host-{i:03}"), 1);
            let r = block.reserve(&e, &mut cache, 0).expect("reserve");
            block.admit(e, r).expect("admit");
        }
        let table = block
            .tables()
            .find(|t| t.dataset().is_series())
            .expect("series");
        assert_eq!(table.rows(), 64);
        assert!(!block.is_sealed());
        for batch in table.iter_snapshots() {
            assert!(snapshot_bytes(std::iter::once(batch)) <= cfg.sorting.run_target_bytes);
            assert!(crate::sort::is_sorted(batch, &SortSpec::series()).expect("sorted"));
            let stamps = batch
                .column_by_name("emitted_at")
                .expect("stamp")
                .as_primitive::<TimestampMicrosecondType>();
            assert!((0..stamps.len()).all(|i| stamps.value(i) == 0));
        }
    }

    /// Scenario: retained series in completed runs and a final building batch are sealed.
    /// Guarantees: the stamp adds at most eight bytes per series row plus 64 bytes of slack and
    /// shares every other column.
    #[test]
    fn seal_peak_retained_bytes_only_adds_timestamp_values() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block = Block::new(0, 1, cfg.clone());
        for i in 0..32 {
            let e = extracted(&cfg, &format!("{}-{i}", "x".repeat(4096)), 1);
            let r = block.reserve(&e, &mut cache, 0).expect("reserve");
            block.admit(e, r).expect("admit");
        }
        let table = block.tables.get_mut(&Dataset::LogsSeries).expect("series");
        let last = table.runs.pop().expect("last run");
        table.building.push(last);
        let before: Vec<_> = block
            .tables()
            .filter(|t| t.dataset().is_series())
            .flat_map(|t| t.runs().iter().chain(t.building()))
            .cloned()
            .collect();
        let rows = block
            .tables()
            .filter(|t| t.dataset().is_series())
            .map(SortedTableBuffer::rows)
            .sum::<usize>();
        let retained = snapshot_bytes(before.iter());
        block.seal(SEAL_AT_US).expect("seal");
        // Holding every old batch keeps the complete old/new overlap resident;
        // this bounds the transaction's peak, not just its net memory change.
        let peak = snapshot_bytes(
            before.iter().chain(
                block
                    .tables()
                    .filter(|t| t.dataset().is_series())
                    .flat_map(SortedTableBuffer::iter_snapshots),
            ),
        );
        assert!(
            peak <= retained + rows * 8 + 64,
            "peak={peak}, before={retained}, rows={rows}"
        );
        let after: Vec<_> = block
            .tables()
            .filter(|t| t.dataset().is_series())
            .flat_map(SortedTableBuffer::iter_snapshots)
            .collect();
        for (old, new) in before.iter().zip(after) {
            for (index, field) in old.schema().fields().iter().enumerate() {
                if field.name() != "emitted_at" {
                    assert!(Arc::ptr_eq(old.column(index), new.column(index)));
                }
            }
        }
        assert!(block.is_sealed());
        assert_eq!(block.pending_series.len(), rows);
        let series = block
            .tables()
            .find(|t| t.dataset() == Dataset::LogsSeries)
            .expect("the series table");
        assert_eq!(series.rows(), rows);
    }

    /// Scenario: two sub-target requests, then the same two under a target each crosses.
    /// Guarantees: sub-target requests share one run sealed at seal; a crossing request seals its
    /// own run at admission.
    #[test]
    fn values_batches_pack_across_requests_until_the_run_target() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block = Block::new(0, 1, cfg.clone());
        for host in ["a", "b"] {
            let e = extracted(&cfg, host, 4);
            let r = block.reserve(&e, &mut cache, 0).expect("reserve");
            block.admit(e, r).expect("admit");
        }
        let values = &block.tables[&Dataset::LogsValues];
        assert!(
            values.runs().is_empty(),
            "a sub-target batch is not sealed into a run during admission"
        );
        assert_eq!(
            values.building().len(),
            2,
            "both requests share one building run"
        );
        block.seal(SEAL_AT_US).expect("seal");
        let values = &block.tables[&Dataset::LogsValues];
        assert_eq!(
            values.runs().len(),
            1,
            "the seal transaction finalizes both requests into one run"
        );
        assert!(values.building().is_empty());
        assert_eq!(values.rows(), 8);
        assert!(
            crate::sort::is_sorted(&values.runs()[0], values.spec()).expect("is_sorted"),
            "the finalized run is sorted by the table's spec"
        );

        let mut tight = LakeConfig::default();
        tight.sorting.run_target_bytes = 1;
        let mut cache = SeriesCache::new(100);
        let mut block = Block::new(0, 1, tight.clone());
        for host in ["a", "b"] {
            let e = extracted(&tight, host, 1);
            let r = block.reserve(&e, &mut cache, 0).expect("reserve");
            block.admit(e, r).expect("admit");
        }
        let values = &block.tables[&Dataset::LogsValues];
        assert_eq!(
            values.runs().len(),
            2,
            "each request crossed the run target on its own"
        );
        assert!(values.building().is_empty());
        block.seal(SEAL_AT_US).expect("seal");
        let values = &block.tables[&Dataset::LogsValues];
        assert_eq!(values.runs().len(), 2, "the seal adds no further run");
        assert_eq!(values.rows(), 2);
    }

    /// Scenario: an extracted descriptor lacks a configured denormalized cell.
    /// Guarantees: admission refuses malformed series content before retaining any descriptor or token.
    #[test]
    fn malformed_descriptor_fails_during_admission() {
        let mut cfg = LakeConfig::default();
        cfg.logs.denormalize.push(crate::config::Denormalize {
            path: "resource.host.id".into(),
            column: "host_col".into(),
            ty: crate::config::DenormType::String,
        });
        let mut e = extracted(&cfg, "host", 1);
        e.descriptors[0].denorm.clear();
        let mut cache = SeriesCache::new(10);
        let mut block = Block::new(0, 1, cfg.clone());
        let r = block.reserve(&e, &mut cache, 0).expect("reserve");
        assert!(block.admit(e, r).is_err());
        assert!(block.is_empty());
        assert!(!block.is_sealed());
        assert_eq!(block.request_count(), 0);
    }

    /// Scenario: two requests for the same series in one block, then the same series after commit.
    /// Guarantees: the descriptor is reserved once per block and not at all once committed in the partition.
    #[test]
    fn reserve_emits_descriptor_once_per_block_and_partition() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block = Block::new(0, 1, cfg.clone());
        let e1 = extracted(&cfg, "h", 2);
        let r1 = block.reserve(&e1, &mut cache, 16).expect("reserve 1");
        assert_eq!(r1.new_series, vec![0]);
        assert!(r1.bytes > 16);
        block.admit(e1, r1).expect("admit");
        let e2 = extracted(&cfg, "h", 1);
        let r2 = block.reserve(&e2, &mut cache, 16).expect("reserve 2");
        assert!(r2.new_series.is_empty());
        block.admit(e2, r2).expect("admit");
        assert_eq!(block.request_count(), 2);
        assert_eq!(block.pending_series.len(), 1);
        block.seal(SEAL_AT_US).expect("seal");
        let series = block
            .tables()
            .find(|t| t.dataset().is_series())
            .expect("series table");
        assert_eq!(series.rows(), 1);
        // commit in the block's partition
        for id in &block.pending_series {
            cache.mark_committed(*id, block.partition);
        }
        let next = Block::new(0, 2, cfg.clone());
        let e3 = extracted(&cfg, "h", 1);
        assert!(
            next.reserve(&e3, &mut cache, 16)
                .expect("reserve 3")
                .new_series
                .is_empty()
        );
        let other = Block::new(3600, 3, cfg.clone()); // next hour
        assert_eq!(
            other
                .reserve(&e3, &mut cache, 16)
                .expect("reserve 4")
                .new_series,
            vec![0]
        );
    }

    /// Scenario: the second retained series batch has an invalid emitted_at type, then is repaired.
    /// Guarantees: failed sealing changes no batch or stamp; retry commits all rows with one frozen stamp.
    #[test]
    fn a_failed_seal_keeps_the_block_intact_and_a_retry_succeeds() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block = Block::new(0, 1, cfg.clone());
        for host in ["first", "second"] {
            let e = extracted(&cfg, host, 1);
            let r = block.reserve(&e, &mut cache, 0).expect("reserve");
            block.admit(e, r).expect("admit");
        }
        let table = block.tables.get_mut(&Dataset::LogsSeries).expect("table");
        let good = table.runs[1].clone();
        let index = good.schema().index_of("emitted_at").expect("field");
        let mut fields = good.schema().fields().to_vec();
        fields[index] = Arc::new(Field::new("emitted_at", DataType::Int64, false));
        let mut columns = good.columns().to_vec();
        columns[index] = Arc::new(Int64Array::from(vec![0; good.num_rows()]));
        table.runs[1] =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("bad batch");
        let before: Vec<_> = block
            .tables()
            .flat_map(SortedTableBuffer::iter_snapshots)
            .cloned()
            .collect();
        let bytes = block.bytes;
        assert!(block.seal(SEAL_AT_US).is_err());
        assert!(!block.is_sealed());
        assert_eq!(block.emitted_at_us(), None);
        assert_eq!(block.bytes, bytes);
        for (old, new) in before
            .iter()
            .zip(block.tables().flat_map(SortedTableBuffer::iter_snapshots))
        {
            assert_eq!(old, new);
        }
        block
            .tables
            .get_mut(&Dataset::LogsSeries)
            .expect("table")
            .runs[1] = good;
        block.seal(SEAL_AT_US + 1).expect("retry");
        block.seal(SEAL_AT_US + 2).expect("idempotent retry");
        assert_eq!(block.emitted_at_us(), Some(SEAL_AT_US + 1));
        assert_eq!(block.tables[&Dataset::LogsSeries].rows(), 2);
    }

    /// Scenario: `emitted_at` on a block sealed once and then sealed again, as a flush retry does.
    /// Guarantees: every descriptor row carries the first seal's timestamp and the second seal does not restamp.
    #[test]
    fn emitted_at_is_stamped_at_seal_and_frozen() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block = Block::new(0, 1, cfg.clone());
        let e = extracted(&cfg, "h", 2);
        let r = block.reserve(&e, &mut cache, 16).expect("reserve");
        block.admit(e, r).expect("admit");
        block.seal(SEAL_AT_US).expect("seal");
        block.seal(SEAL_AT_US + 5_000_000).expect("re-seal");
        assert_eq!(block.emitted_at_us(), Some(SEAL_AT_US));
        let series = block
            .tables()
            .find(|t| t.dataset().is_series())
            .expect("series table");
        let batch = series.runs().first().expect("run");
        let stamps = batch.column_by_name("emitted_at").expect("emitted_at");
        let stamps = stamps.as_primitive::<TimestampMicrosecondType>();
        assert!((0..stamps.len()).all(|i| stamps.value(i) == SEAL_AT_US));
    }

    /// Scenario: a small request under the default configuration whose completion token is one
    /// byte over `TOKEN_ALLOWANCE_BYTES`, then exactly at it, offered to an empty block.
    /// Guarantees: the oversized token is refused permanently as `TokenTooLarge` before any
    /// reservation and leaves the block untouched; a token at the allowance is admitted.
    #[test]
    fn a_token_over_the_allowance_is_refused_before_reservation() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let block = Block::new(0, 1, cfg.clone());
        let e = extracted(&cfg, "h", 4);
        let refused = block.reserve(&e, &mut cache, TOKEN_ALLOWANCE_BYTES + 1);
        assert!(
            matches!(
                &refused,
                Err(Error::Refused(RefuseReason::TokenTooLarge {
                    observed: 4097,
                    limit: 4096
                }))
            ),
            "{refused:?}"
        );
        assert_eq!(block.bytes, 0);
        assert_eq!(block.request_count(), 0);
        assert!(block.pending_series.is_empty());
        assert_eq!(block.tables().count(), 0);
        let _ = block
            .reserve(&e, &mut cache, TOKEN_ALLOWANCE_BYTES)
            .expect("a token at the allowance is reserved");
    }

    /// Scenario: a block budget below what validation allows (one byte), so a small request's
    /// worst case misses an empty block.
    /// Guarantees: an internal error that leaves the block untouched; the debug assertion stays
    /// silent because the configuration does not validate.
    #[test]
    fn a_request_missing_an_empty_block_is_an_internal_error() {
        let mut cfg = LakeConfig::default();
        cfg.ingress.max_block_bytes = 1;
        let mut cache = SeriesCache::new(100);
        let block = Block::new(0, 1, cfg.clone());
        let e = extracted(&LakeConfig::default(), "h", 4);
        assert!(matches!(
            block.reserve(&e, &mut cache, 16),
            Err(Error::Internal(_))
        ));
        assert_eq!(block.bytes, 0);
        assert_eq!(block.tables().count(), 0);
    }

    /// Scenario: a validated configuration and a request whose worst case misses an empty block,
    /// which validation makes impossible for a real extraction; forged by inflating the
    /// extraction's pinned bytes to the whole block.
    /// Guarantees: debug builds fail the assertion, with no exemption for the token.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "a request that passed ingress needs")]
    fn an_empty_block_miss_under_a_valid_configuration_fails_the_debug_assertion() {
        let cfg = LakeConfig::default();
        cfg.validate().expect("valid");
        let block = Block::new(0, 1, cfg.clone());
        let mut e = extracted(&cfg, "h", 4);
        e.pinned_bytes = cfg.ingress.max_block_bytes;
        let _ = block.reserve(&e, &mut SeriesCache::new(100), 16);
    }

    /// Scenario: one request against an empty block, cold and fully committed cache, with and
    /// without reemit, one byte under and exactly at its worst case.
    /// Guarantees: every combination fails one byte short and is admitted at the worst case.
    #[test]
    fn a_request_is_decided_alike_cold_and_warm() {
        let cfg = LakeConfig::default();
        let e = extracted(&cfg, "h", 4);
        let token = 16;
        let worst = e.pinned_bytes
            + e.merge_key_bytes
            + token
            + e.descriptors
                .iter()
                .map(|d| d.series_row_bytes() + cfg.ingress.pending_series_entry_bytes)
                .sum::<usize>();
        for (limit, admitted) in [(worst - 1, false), (worst, true)] {
            let mut tight = cfg.clone();
            tight.ingress.max_block_bytes = limit;
            let block = Block::new(0, 1, tight.clone());
            for reemit in [false, true] {
                let mut cold = SeriesCache::new(100);
                let mut warm = SeriesCache::new(100);
                for d in &e.descriptors {
                    warm.mark_committed(d.series_id, block.partition);
                }
                for cache in [&mut cold, &mut warm] {
                    let outcome = block.reserve_with_reemit(&e, cache, token, reemit);
                    if admitted {
                        assert!(
                            outcome.is_ok(),
                            "limit {limit}, reemit {reemit}: {outcome:?}"
                        );
                    } else {
                        assert!(
                            matches!(outcome, Err(Error::Internal(_))),
                            "limit {limit}, reemit {reemit}: {outcome:?}"
                        );
                    }
                }
            }
        }
    }

    /// A gauge request of `n` distinct series under one resource.
    fn gauge_request(
        n: usize,
    ) -> otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::MetricsData {
        use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
            Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
            number_data_point,
        };
        let kv = |k: &str, v: String| KeyValue {
            key: k.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v)),
            }),
        };
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", "h".into())],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "cpu".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: (0..n)
                                .map(|i| NumberDataPoint {
                                    time_unix_nano: 10 + i as u64,
                                    value: Some(number_data_point::Value::AsDouble(0.5)),
                                    attributes: vec![kv("cpu", i.to_string())],
                                    ..Default::default()
                                })
                                .collect(),
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// Scenario: logs and metrics requests of one and many series, with and without a denormalized
    /// column, against an empty block and a cold cache.
    /// Guarantees: `reserve` charges exactly the worst case documented on
    /// `LakeConfig::series_row_fixed_bytes` (232 and 328 fixed bytes per series).
    #[test]
    fn the_documented_worst_case_block_cost_is_what_reserve_charges() {
        use crate::canonical::Signal;
        use otel_arrow_dfe_pdata::testing::round_trip::encode_metrics;

        let mut denormalized = LakeConfig::default();
        denormalized.logs.denormalize = vec![crate::config::Denormalize {
            path: "resource.host.id".into(),
            column: "host".into(),
            ty: crate::config::DenormType::String,
        }];
        let defaults = LakeConfig::default();
        assert_eq!(defaults.series_row_fixed_bytes(Signal::Logs), 232);
        assert_eq!(defaults.series_row_fixed_bytes(Signal::Metrics), 328);
        assert_eq!(denormalized.series_row_fixed_bytes(Signal::Logs), 232 + 16);

        let mut cases: Vec<(LakeConfig, usize, Signal, Extracted)> = Vec::new();
        for cfg in [defaults.clone(), denormalized] {
            for (host, n) in [("one", 1), ("many", 64)] {
                let e = extracted(&cfg, host, n);
                let columns = cfg.series_columns(Signal::Logs);
                cases.push((cfg.clone(), columns, Signal::Logs, e));
            }
        }
        for n in [1, 64] {
            let mut records = encode_metrics(&gauge_request(n));
            let e = extract(&mut records, &defaults).expect("extract metrics");
            assert_eq!(e.descriptors.len(), n);
            let columns = defaults.series_columns(Signal::Metrics);
            cases.push((defaults.clone(), columns, Signal::Metrics, e));
        }

        for (cfg, columns, signal, e) in cases {
            let token = 16;
            let q = cfg.ingress.pending_series_entry_bytes;
            let exact = e.pinned_bytes
                + e.merge_key_bytes
                + token
                + e.descriptors
                    .iter()
                    .map(|d| 2 * (d.approx_bytes - d.decoded_bytes) + 16 * columns + 8 + q)
                    .sum::<usize>();
            let block = Block::new(0, 1, cfg.clone());
            let reservation = block
                .reserve(&e, &mut SeriesCache::new(1024), token)
                .expect("the default block takes the request");
            assert_eq!(reservation.new_series.len(), e.descriptors.len());
            assert_eq!(reservation.bytes, exact, "{signal:?}");

            let charged = e.pinned_bytes
                + e.merge_key_bytes
                + e.descriptors.iter().map(|d| d.approx_bytes).sum::<usize>();
            let fixed = cfg.series_row_fixed_bytes(signal);
            assert_eq!(fixed, 16 * columns + 8 + q);
            assert!(exact <= 2 * charged + token + e.descriptors.len() * fixed);
        }
    }

    /// A logs request of `n` records, each its own series through `logger.name`.
    fn series_logs(n: usize) -> LogsData {
        let mut data = logs("h", n);
        for (i, record) in data.resource_logs[0].scope_logs[0]
            .log_records
            .iter_mut()
            .enumerate()
        {
            record.attributes = vec![KeyValue {
                key: "logger.name".into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(format!("l{i:07}"))),
                }),
            }];
        }
        data
    }

    /// Scenario: validated budgets that leave room for exactly 1000 series per request, and
    /// requests of 1000 and 1001 new series: logs, metrics with a unique attribute per point,
    /// and logs with a denormalized column.
    /// Guarantees: every request at the limit passes extraction and fits an empty block; one
    /// series more is refused at extraction, naming the count and the limit.
    #[test]
    fn a_request_at_the_series_limit_fits_an_empty_block() {
        use crate::config::Denormalize;
        use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
        use otel_arrow_dfe_pdata::testing::round_trip::encode_metrics;
        let mut plain = LakeConfig::default();
        plain.logs.series_attributes = vec!["logger.name".into()];
        plain.ingress.max_extracted_bytes = 1 << 20;
        plain.ingress.max_series_per_request = Some(1000);
        let (_, fixed) = plain.max_series_row_fixed_bytes();
        plain.ingress.max_block_bytes = (2 << 20) + 1000 * fixed + TOKEN_ALLOWANCE_BYTES;
        let mut denormalized = plain.clone();
        denormalized.logs.denormalize = vec![
            serde_json::from_value::<Denormalize>(serde_json::json!("resource.host.id"))
                .expect("denormalized column"),
        ];
        let logs_request = |n| encode_logs(&series_logs(n));
        let metrics_request = |n| encode_metrics(&gauge_request(n));
        let cases: [(&LakeConfig, &dyn Fn(usize) -> OtapArrowRecords); 3] = [
            (&plain, &logs_request),
            (&plain, &metrics_request),
            (&denormalized, &logs_request),
        ];
        for (cfg, request) in cases {
            cfg.validate().expect("the budgets hold the bound");
            let e = extract(&mut request(1000), cfg).expect("extraction accepts the limit");
            assert_eq!(e.descriptors.len(), 1000);
            let block = Block::new(0, 1, cfg.clone());
            let reservation = block
                .reserve(&e, &mut SeriesCache::new(4096), TOKEN_ALLOWANCE_BYTES)
                .expect("an empty block takes a request at the limit");
            assert_eq!(reservation.new_series.len(), 1000);
            assert!(matches!(
                extract(&mut request(1001), cfg),
                Err(Error::Refused(RefuseReason::TooManySeries {
                    observed: 1001,
                    limit: 1000
                }))
            ));
        }
    }

    /// Scenario: logs values sorted by a 2 KiB `body`, requests admitted into one block until it
    /// is full, the block sealed and the merge of every values table built as a flush builds it.
    /// Guarantees: the sealed block's bytes plus the merge keys of any one of its tables stay
    /// within `max_block_bytes`, so a wide sort key cannot take the block past its budget.
    #[test]
    fn a_full_block_and_the_merge_keys_of_its_table_fit_its_budget() {
        let mut cfg = LakeConfig::default();
        cfg.logs.values_sort = vec![SortKey {
            column: "body".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        }];
        cfg.sorting.run_target_bytes = 256 << 10;
        cfg.ingress.max_row_bytes = 64 << 10;
        cfg.ingress.max_extracted_bytes = 1 << 20;
        cfg.ingress.max_block_bytes = 4 << 20;
        cfg.ingress.max_series_per_request = Some(100);
        cfg.validate().expect("valid config");
        let mut block = Block::new(0, 1, cfg.clone());
        let mut cache = SeriesCache::new(100);
        for request in 0.. {
            let mut data = logs("h", 100);
            for (i, record) in data.resource_logs[0].scope_logs[0]
                .log_records
                .iter_mut()
                .enumerate()
            {
                let text = format!("{request:06}-{i:04}-");
                record.body = Some(AnyValue {
                    value: Some(any_value::Value::StringValue(
                        text.repeat(2048 / text.len()),
                    )),
                });
            }
            let e = extract(&mut encode_logs(&data), &cfg).expect("extract");
            match block.reserve(&e, &mut cache, 16) {
                Ok(reservation) => block.admit(e, reservation).expect("admit"),
                Err(Error::Refused(RefuseReason::BlockFull)) => break,
                Err(other) => panic!("request {request}: {other:?}"),
            }
        }
        block.seal(SEAL_AT_US).expect("seal");
        let keys = block
            .tables()
            .map(|table| {
                crate::sort::MergeBuild::new(
                    table.runs().to_vec(),
                    table.spec(),
                    cfg.sorting.merge_chunk_bytes,
                )
                .and_then(crate::sort::MergeBuild::finish)
                .expect("merge")
                .resident_key_bytes()
            })
            .max()
            .expect("a table");
        assert!(
            block.bytes + keys <= cfg.ingress.max_block_bytes,
            "block {} + keys {keys} > {}",
            block.bytes,
            cfg.ingress.max_block_bytes
        );
    }

    /// Scenario: a block already holding `max_requests_per_block` tokens.
    /// Guarantees: the next reservation is refused with `TooManyRequests` and the block is unchanged.
    #[test]
    fn request_count_limit_refuses_before_mutating() {
        let mut cfg = LakeConfig::default();
        cfg.ingress.max_requests_per_block = 2;
        let mut cache = SeriesCache::new(100);
        let mut block = Block::new(0, 1, cfg.clone());
        for _ in 0..2 {
            let e = extracted(&cfg, "h", 1);
            let r = block.reserve(&e, &mut cache, 16).expect("reserve");
            block.admit(e, r).expect("admit");
        }
        let before_bytes = block.bytes;
        let before_series = block.pending_series.len();
        let e = extracted(&cfg, "h", 1);
        assert!(matches!(
            block.reserve(&e, &mut cache, 16),
            Err(Error::Refused(RefuseReason::TooManyRequests))
        ));
        assert_eq!(block.bytes, before_bytes);
        assert_eq!(block.pending_series.len(), before_series);
        assert_eq!(block.request_count(), 2);
    }

    /// Scenario: a non-empty block that cannot take one more request within `max_block_bytes`.
    /// Guarantees: `BlockFull` is returned, which the node loop turns into a rotation, and the block is unchanged.
    #[test]
    fn full_block_refuses_with_block_full() {
        let cfg = LakeConfig::default();
        // A block holding one request, under `cfg`.
        let filled = |cfg: &LakeConfig| {
            let mut cache = SeriesCache::new(100);
            let mut block = Block::new(0, 1, cfg.clone());
            let first = extracted(cfg, "h", 4);
            let r = block.reserve(&first, &mut cache, 16).expect("reserve");
            block.admit(first, r).expect("admit");
            (block, cache)
        };
        let (block, mut cache) = filled(&cfg);
        let before_bytes = block.bytes;

        // A second, distinct series whose own reservation fits the limit, so the
        // only reason to refuse it is the bytes the block already holds.
        let e = extracted(&cfg, "h2", 4);
        let probe = block.reserve(&e, &mut cache, 16).expect("fits on its own");
        let mut tight = cfg.clone();
        tight.ingress.max_block_bytes = before_bytes + probe.bytes - 1;
        let (block, mut cache) = filled(&tight);
        assert_eq!(block.bytes, before_bytes);
        assert!(matches!(
            block.reserve(&e, &mut cache, 16),
            Err(Error::Refused(RefuseReason::BlockFull))
        ));
        assert_eq!(block.bytes, before_bytes);
        assert_eq!(block.request_count(), 1);
    }

    /// Scenario: a request whose values batches carry builder slack is admitted and sealed.
    /// Guarantees: the recount deduplicates shared buffers, never exceeds the reservations, and
    /// still counts one copy of the data.
    #[test]
    fn seal_recounts_shared_buffers_once() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block = Block::new(0, 1, cfg.clone());
        let e1 = extracted(&cfg, "h", 8);
        // The logical payload of the request, free of the builder capacity slack
        // that `pinned_bytes` also counts.
        let one_request_logical: usize = e1
            .values
            .iter()
            .flat_map(|(_, batches)| batches.iter())
            .map(|b| {
                otel_arrow_dfe_pdata::otap::memory::record_batch_logical_bytes(b)
                    .expect("logical bytes")
            })
            .sum();
        assert!(one_request_logical > 0);
        let r1 = block.reserve(&e1, &mut cache, 16).expect("r1");
        block.admit(e1, r1).expect("admit");
        let reserved = block.bytes;
        block.seal(SEAL_AT_US).expect("seal");
        assert!(
            block.bytes <= reserved,
            "the recount is never above the reservation upper bound"
        );
        let retained = snapshot_bytes(block.tables().flat_map(SortedTableBuffer::iter_snapshots));
        assert_eq!(
            block.bytes,
            retained + block.pending_series.len() * cfg.ingress.pending_series_entry_bytes + 16,
            "the recount is every retained buffer counted once, plus the pending-series and token overheads"
        );
        assert!(
            block.bytes >= one_request_logical,
            "one copy of the data is still counted"
        );
    }

    /// Scenario: the same batch, cloned, is appended twice.
    /// Guarantees: the second append adds zero bytes while both copies' rows are counted.
    #[test]
    fn append_counts_shared_buffers_once() {
        let cfg = LakeConfig::default();
        let e = extracted(&cfg, "h", 8);
        let (ds, batches) = e.values.into_iter().next().expect("values");
        let batch = batches.into_iter().next().expect("batch");
        let mut buf = SortedTableBuffer::new(ds, SortSpec::new(vec![]), usize::MAX);
        let first = buf.append(batch.clone()).expect("first append");
        let second = buf.append(batch.clone()).expect("second append");
        assert!(first > 0, "the first append retains the batch's buffers");
        assert_eq!(
            second, 0,
            "the clone shares them, so nothing new is retained"
        );
        assert_eq!(buf.rows(), batch.num_rows() * 2);
    }

    /// Scenario: after a seal, a batch built from buffers the previous run already counted.
    /// Guarantees: it is counted in full, so the next run can still seal.
    #[test]
    fn seal_resets_the_dedup_set_so_the_next_run_accounts_again() {
        let cfg = LakeConfig::default();
        let e = extracted(&cfg, "h", 8);
        let (ds, batches) = e.values.into_iter().next().expect("values");
        let batch = batches.into_iter().next().expect("batch");
        let mut buf =
            SortedTableBuffer::new(ds, SortSpec::new(cfg.logs.values_sort.clone()), usize::MAX);
        let first = buf.append(batch.clone()).expect("first append");
        assert!(first > 0);
        assert_eq!(buf.building_bytes, first);
        buf.seal().expect("seal");
        assert_eq!(buf.building_bytes, 0);
        // `batch` is still alive, so its buffers keep the addresses the first
        // append recorded. Only a reset set can count them again.
        let second = buf.append(batch.clone()).expect("append after seal");
        assert_eq!(second, first, "the new run accounts the batch in full");
        assert!(buf.building_bytes > 0, "building_bytes grows again");
    }

    /// Scenario: the run target is crossed repeatedly over batches dropped after sealing.
    /// Guarantees: every seal is reached despite reused addresses.
    #[test]
    fn repeated_seals_keep_firing_on_the_run_target() {
        let cfg = LakeConfig::default();
        let mut buf = SortedTableBuffer::new(
            Dataset::LogsValues,
            SortSpec::new(cfg.logs.values_sort.clone()),
            1,
        );
        for _ in 0..8 {
            let e = extracted(&cfg, "h", 4);
            let (_, batches) = e.values.into_iter().next().expect("values");
            for b in batches {
                let pinned = buf.append(b).expect("append");
                assert!(pinned > 0, "each fresh batch is accounted");
            }
            assert!(buf.building().is_empty(), "the run target sealed the batch");
        }
        assert_eq!(buf.runs().len(), 8);
        assert_eq!(buf.rows(), 32);
    }

    /// Scenario: a buffer with an empty sort spec.
    /// Guarantees: sealing keeps the appended batches as separate runs and every row survives.
    #[test]
    fn unsorted_seal_keeps_batches_as_runs() {
        let cfg = LakeConfig::default();
        let e = extracted(&cfg, "h", 6);
        let (ds, batches) = e.values.into_iter().next().expect("values");
        let batch = batches.into_iter().next().expect("batch");
        let rows = batch.num_rows();
        let mut buf = SortedTableBuffer::new(ds, SortSpec::new(vec![]), usize::MAX);
        let _ = buf.append(batch.clone()).expect("append 1");
        let _ = buf.append(batch).expect("append 2");
        buf.seal().expect("seal");
        assert_eq!(
            buf.runs().len(),
            2,
            "no concatenation happens without a sort"
        );
        assert!(buf.building().is_empty());
        assert_eq!(buf.rows(), rows * 2);
        assert_eq!(
            buf.runs().iter().map(RecordBatch::num_rows).sum::<usize>(),
            rows * 2
        );
    }

    /// Scenario: `spec_for` per dataset, with different logs and metrics sorts and with sorting
    /// off.
    /// Guarantees: series take the fixed sort, values their signal's keys; sorting off empties only
    /// the values specs.
    #[test]
    fn spec_for_maps_each_dataset_to_its_configured_sort() {
        let mut cfg = LakeConfig::default();
        cfg.metrics.values_sort = vec![SortKey {
            column: "series_id".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        }];
        assert_ne!(cfg.logs.values_sort, cfg.metrics.values_sort);
        let block = Block::new(0, 1, cfg.clone());

        assert_eq!(
            block.spec_for(Dataset::LogsSeries),
            SortSpec::series(),
            "logs series takes the fixed series sort"
        );
        assert_eq!(
            block.spec_for(Dataset::MetricsSeries),
            SortSpec::series(),
            "metrics series takes the fixed series sort"
        );
        assert_eq!(
            block.spec_for(Dataset::LogsValues).keys(),
            cfg.logs.values_sort.as_slice()
        );
        assert_eq!(
            block.spec_for(Dataset::MetricsValues).keys(),
            cfg.metrics.values_sort.as_slice(),
            "the merged metrics values dataset follows the metrics sort"
        );

        let mut off = cfg.clone();
        off.logs.values_sort.clear();
        off.metrics.values_sort.clear();
        let block = Block::new(0, 1, off.clone());
        for ds in [Dataset::LogsValues, Dataset::MetricsValues] {
            assert!(
                block.spec_for(ds).is_empty(),
                "{} is unsorted under an empty values_sort",
                ds.name()
            );
        }
        assert_eq!(block.spec_for(Dataset::LogsSeries), SortSpec::series());
    }

    /// Scenario: the runs a block produces, sorted by configured keys and with sorting off.
    /// Guarantees: they are sealed under the spec `spec_for` reports.
    #[test]
    fn block_runs_are_sealed_under_the_spec_spec_for_reports() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block = Block::new(0, 1, cfg.clone());
        let e = extracted(&cfg, "h", 40);
        let r = block.reserve(&e, &mut cache, 16).expect("reserve");
        block.admit(e, r).expect("admit");
        block.seal(SEAL_AT_US).expect("seal");
        for t in block.tables() {
            assert_eq!(t.spec(), &block.spec_for(t.dataset()));
            for run in t.runs() {
                assert!(
                    crate::sort::is_sorted(run, t.spec()).expect("is_sorted"),
                    "{} runs are sorted by its spec",
                    t.dataset().name()
                );
            }
        }
    }

    /// Scenario: a request offered to a sealed block.
    /// Guarantees: `admit` refuses it as a writer invariant, not as a refusal of the request.
    #[test]
    fn admit_after_seal_is_rejected() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block = Block::new(0, 1, cfg.clone());
        let e1 = extracted(&cfg, "h", 2);
        let r1 = block.reserve(&e1, &mut cache, 16).expect("reserve 1");
        block.admit(e1, r1).expect("admit");
        block.seal(SEAL_AT_US).expect("seal");
        let sealed_bytes = block.bytes;

        let e2 = extracted(&cfg, "h2", 2);
        let r2 = block.reserve(&e2, &mut cache, 16).expect("reserve 2");
        let err = block
            .admit(e2, r2)
            .expect_err("a sealed block admits nothing");
        assert!(matches!(err, Error::Internal(_)));
        assert!(err.to_string().contains("already sealed"));
        assert_eq!(block.bytes, sealed_bytes);
        assert_eq!(block.request_count(), 1);
        assert_eq!(block.emitted_at_us(), Some(SEAL_AT_US));
    }

    /// Scenario: a byte rotation begins another block in a partition with a committed descriptor.
    /// Guarantees: explicit re-emission reserves the descriptor again without disabling the cache globally.
    #[test]
    fn byte_rotation_can_force_descriptor_reemission() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(10);
        let e = extracted(&cfg, "h", 1);
        let id = e.descriptors[0].series_id;
        let block = Block::new(0, 1, cfg.clone());
        cache.mark_committed(id, block.partition);
        let normal = block.reserve(&e, &mut cache, 16).expect("reserve");
        assert!(normal.new_series.is_empty());
        let forced = block
            .reserve_with_reemit(&e, &mut cache, 16, true)
            .expect("reserve");
        assert_eq!(forced.new_series, vec![0]);
        assert!(forced.bytes > normal.bytes);
    }

    /// Scenario: a buffer with a tiny run target receives several batches.
    /// Guarantees: runs are sealed sorted, byte accounting grows, snapshots iterate everything.
    #[test]
    fn buffer_seals_sorted_runs() {
        let cfg = LakeConfig::default();
        let e = extracted(&cfg, "h", 50);
        let (_, batches) = e.values.into_iter().next().expect("values");
        let mut buf = SortedTableBuffer::new(
            Dataset::LogsValues,
            SortSpec::new(cfg.logs.values_sort.clone()),
            1,
        );
        let mut total = 0;
        for b in batches {
            total += buf.append(b).expect("append");
        }
        assert!(total > 0);
        assert!(!buf.runs().is_empty());
        for r in buf.runs() {
            assert!(crate::sort::is_sorted(r, buf.spec()).expect("sorted"));
        }
        assert_eq!(
            buf.iter_snapshots().map(|b| b.num_rows()).sum::<usize>(),
            50
        );
        buf.seal().expect("seal");
        assert!(buf.building().is_empty());
    }
}
