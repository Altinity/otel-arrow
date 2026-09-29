// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Sort specification, run sorting and the k-way merge that orders a
//! table's rows by its `sort_key` (FORMAT.md section 5).

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, UInt32Array};
use arrow::array::{
    ArrayData, BooleanBufferBuilder, Capacities, ListArray, MapArray, MutableArrayData,
    OffsetBufferBuilder, StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::compute::{SortColumn, SortOptions, interleave, lexsort_to_indices, take};
use arrow::datatypes::{DataType, Float64Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{Row, RowConverter, Rows, SortField};
use otel_arrow_dfe_pdata::otap::memory::{CountedAllocations, record_batch_pinned_bytes};

use crate::config::{LakeConfig, Nulls, SortKey, SortOrder};
use crate::error::{Error, Result};
use crate::schema::Dataset;

/// Ordered list of sort keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortSpec {
    keys: Vec<SortKey>,
}

impl SortSpec {
    /// Build from keys; an empty list means "no sorting".
    #[must_use]
    pub fn new(keys: Vec<SortKey>) -> Self {
        Self { keys }
    }

    /// The fixed series sort: `series_id` ascending.
    #[must_use]
    pub fn series() -> Self {
        Self::new(vec![SortKey {
            column: "series_id".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        }])
    }

    /// The sort a table of `ds` is written in under `cfg`: the fixed series
    /// sort for a series dataset, the signal's `values_sort` for a values
    /// dataset, where an empty `values_sort` writes the values unsorted.
    #[must_use]
    pub fn for_dataset(ds: Dataset, cfg: &LakeConfig) -> Self {
        if ds.is_series() {
            Self::series()
        } else if ds.signal() == crate::canonical::Signal::Logs {
            Self::new(cfg.logs.values_sort.clone())
        } else {
            Self::new(cfg.metrics.values_sort.clone())
        }
    }

    /// Whether sorting is disabled.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Keys.
    #[must_use]
    pub fn keys(&self) -> &[SortKey] {
        &self.keys
    }

    /// `sort_key` file metadata value.
    #[must_use]
    pub fn metadata_string(&self) -> String {
        if self.keys.is_empty() {
            return "none".into();
        }
        self.keys
            .iter()
            .map(|k| {
                format!(
                    "{}:{}:{}",
                    k.column,
                    match k.order {
                        SortOrder::Asc => "asc",
                        SortOrder::Desc => "desc",
                    },
                    match k.nulls {
                        Nulls::First => "nulls_first",
                        Nulls::Last => "nulls_last",
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    fn options(k: &SortKey) -> SortOptions {
        SortOptions {
            descending: k.order == SortOrder::Desc,
            nulls_first: k.nulls == Nulls::First,
        }
    }
}

/// Doubles normalized for ordering: every NaN becomes positive quiet NaN and
/// -0.0 becomes +0.0, so `total_cmp` yields the spec order.
fn normalize_key(col: &ArrayRef) -> ArrayRef {
    if col.data_type() != &DataType::Float64 {
        return col.clone();
    }
    let a = col.as_primitive::<Float64Type>();
    let out: Float64Array = arrow::compute::kernels::arity::unary(a, |v: f64| {
        if v.is_nan() {
            f64::NAN
        } else if v == 0.0 {
            0.0
        } else {
            v
        }
    });
    Arc::new(out)
}

/// Every sort column named by the spec must exist in the batch.
///
/// Checked before the short-batch early returns of [`sort_batch`] and
/// [`is_sorted`], so a misspelled sort key is reported for a one-row batch too.
fn check_columns(batch: &RecordBatch, spec: &SortSpec) -> Result<()> {
    for k in &spec.keys {
        if batch.column_by_name(&k.column).is_none() {
            return Err(Error::internal(format!("sort column {} missing", k.column)));
        }
    }
    Ok(())
}

fn sort_columns(batch: &RecordBatch, spec: &SortSpec) -> Result<Vec<SortColumn>> {
    spec.keys
        .iter()
        .map(|k| {
            let col = batch
                .column_by_name(&k.column)
                .ok_or_else(|| Error::internal(format!("sort column {} missing", k.column)))?;
            Ok(SortColumn {
                values: normalize_key(col),
                options: Some(SortSpec::options(k)),
            })
        })
        .collect()
}

/// Row indices of the sort columns `cols` in their sort order, unstable.
///
/// Several fixed-width keys, the default values sort among them, are encoded
/// once into Arrow's row format and compared as bytes: `lexsort_to_indices`
/// calls one dynamic comparator per column per comparison instead. The
/// order is the same, as the merge compares the same encoded keys. Any other
/// key is sorted by `lexsort_to_indices`, which copies no key: a row-format
/// copy of a wide string key would add a transient run-sized buffer the
/// block does not account for.
fn sort_indices(cols: &[SortColumn]) -> Result<UInt32Array> {
    let fixed = cols.iter().all(|c| {
        let dt = c.values.data_type();
        dt.primitive_width().is_some() || matches!(dt, DataType::FixedSizeBinary(_))
    });
    if cols.len() < 2 || !fixed {
        return Ok(lexsort_to_indices(cols, None)?);
    }
    let fields = cols
        .iter()
        .map(|c| {
            SortField::new_with_options(c.values.data_type().clone(), c.options.unwrap_or_default())
        })
        .collect();
    let converter = RowConverter::new(fields)?;
    let values: Vec<ArrayRef> = cols.iter().map(|c| Arc::clone(&c.values)).collect();
    let rows = converter.convert_columns(&values)?;
    let len = u32::try_from(rows.num_rows())
        .map_err(|_| Error::internal("a run has more rows than u32 indexes"))?;
    let mut order: Vec<u32> = (0..len).collect();
    order.sort_unstable_by(|&a, &b| rows.row(a as usize).cmp(&rows.row(b as usize)));
    Ok(UInt32Array::from(order))
}

/// Sort one batch by the spec. Returns the input unchanged for an empty spec.
///
/// Not stable: arrow's `lexsort_to_indices` is an unstable sort, so the relative
/// order of rows whose sort keys are equal is unspecified. Nothing in the format
/// depends on it.
pub fn sort_batch(batch: &RecordBatch, spec: &SortSpec) -> Result<RecordBatch> {
    if spec.is_empty() {
        return Ok(batch.clone());
    }
    check_columns(batch, spec)?;
    if batch.num_rows() < 2 {
        return Ok(batch.clone());
    }
    let cols = sort_columns(batch, spec)?;
    let indices = sort_indices(&cols)?;
    let taken: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| take(c, &indices, None))
        .collect::<std::result::Result<_, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), taken)?)
}

/// Sort the rows of several batches of one schema into one batch, in the
/// order [`sort_batch`] gives their concatenation.
///
/// Only the sort-key columns are concatenated; every column is then gathered
/// once, straight from the batches. Returns the concatenation for an empty
/// spec.
///
/// # Errors
/// Refuses a spec naming a column the batches lack, and propagates an Arrow
/// failure of the concatenation, the sort or the gather.
pub fn sort_batches(batches: &[RecordBatch], spec: &SortSpec) -> Result<RecordBatch> {
    let Some(first) = batches.first() else {
        return Err(Error::internal("no batches to sort"));
    };
    if let [only] = batches {
        return sort_batch(only, spec);
    }
    let schema = first.schema();
    if spec.is_empty() {
        return Ok(arrow::compute::concat_batches(&schema, batches)?);
    }
    check_columns(first, spec)?;
    let keys = spec
        .keys
        .iter()
        .map(|k| {
            let parts = batches
                .iter()
                .map(|b| {
                    b.column_by_name(&k.column)
                        .map(AsRef::as_ref)
                        .ok_or_else(|| Error::internal(format!("sort column {} missing", k.column)))
                })
                .collect::<Result<Vec<&dyn Array>>>()?;
            Ok(SortColumn {
                values: normalize_key(&arrow::compute::concat(&parts)?),
                options: Some(SortSpec::options(k)),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let indices = sort_indices(&keys)?;
    let rows: Vec<(usize, usize)> = batches
        .iter()
        .enumerate()
        .flat_map(|(b, batch)| (0..batch.num_rows()).map(move |r| (b, r)))
        .collect();
    let order: Vec<(usize, usize)> = indices.values().iter().map(|&i| rows[i as usize]).collect();
    let columns = (0..schema.fields().len())
        .map(|c| {
            let parts: Vec<&dyn Array> = batches.iter().map(|b| b.column(c).as_ref()).collect();
            interleave(&parts, &order)
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Most rows one merge step encodes keys for, or pops from the merge heap,
/// before it returns.
///
/// A step is the unit of work the sink runs between two returns to its
/// runtime, so this and [`MERGE_STEP_KEY_BYTES`] bound how long a merge holds
/// the thread it runs on. Neither affects which rows a chunk holds or their
/// order: the merge produces the same chunks however its work is sliced.
pub const MERGE_STEP_ROWS: usize = 8192;

/// Most encoded key bytes one merge step builds or compares before it returns.
///
/// Encoding and comparing keys costs time in proportion to their bytes, so a
/// wide key, such as a log body, gets proportionally fewer rows per step.
pub const MERGE_STEP_KEY_BYTES: usize = 1 << 20;

/// Rows the first key slice of a merge encodes, before any key width is
/// known.
const FIRST_KEY_SLICE_ROWS: usize = 256;

/// Fewest rows a key slice encodes, whatever the key width.
const MIN_KEY_SLICE_ROWS: usize = 16;

/// How much work one merge step may do.
#[derive(Debug, Clone, Copy)]
struct StepBudget {
    rows: usize,
    key_bytes: usize,
    /// The largest value count an `i32` offset buffer may reach: a string
    /// column's bytes, a list's items, a map's entries and their key and
    /// value bytes. Lowered by tests only.
    offset_limit: usize,
}

impl StepBudget {
    const DEFAULT: Self = Self {
        rows: MERGE_STEP_ROWS,
        key_bytes: MERGE_STEP_KEY_BYTES,
        offset_limit: i32::MAX as usize,
    };

    /// Rows of the next key slice, given the average encoded key width seen
    /// so far.
    fn key_slice_rows(self, average_key_bytes: Option<usize>) -> usize {
        let rows = match average_key_bytes {
            None => FIRST_KEY_SLICE_ROWS,
            Some(width) => self.key_bytes / width.max(1),
        };
        rows.clamp(MIN_KEY_SLICE_ROWS.min(self.rows), self.rows)
    }
}

/// A run's next row in the merge: where its key is encoded, not a copy of it.
#[derive(Debug, Clone, Copy)]
struct HeapItem {
    run: usize,
    /// Row index within the run.
    idx: usize,
    /// Key segment of the run the row's key sits in, and its offset there.
    seg: usize,
    off: usize,
}

impl HeapItem {
    /// The row's encoded key among every run's key segments.
    fn key<'k>(&self, keys: &'k [Vec<Rows>]) -> Row<'k> {
        keys[self.run][self.seg].row(self.off)
    }

    /// Merge order: the smaller key first, and for equal keys the earlier run,
    /// so that equal rows leave the merge in run order.
    fn before(&self, other: &Self, keys: &[Vec<Rows>]) -> bool {
        self.key(keys)
            .cmp(&other.key(keys))
            .then(self.run.cmp(&other.run))
            == Ordering::Less
    }
}

/// A binary min-heap of [`HeapItem`]s in merge order, comparing the keys
/// where they are encoded.
#[derive(Default)]
struct MergeHeap {
    items: Vec<HeapItem>,
}

impl MergeHeap {
    #[cfg(test)]
    fn len(&self) -> usize {
        self.items.len()
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn capacity(&self) -> usize {
        self.items.capacity()
    }

    fn push(&mut self, item: HeapItem, keys: &[Vec<Rows>]) {
        self.items.push(item);
        let mut child = self.items.len() - 1;
        while child > 0 {
            let parent = (child - 1) / 2;
            if !self.items[child].before(&self.items[parent], keys) {
                break;
            }
            self.items.swap(child, parent);
            child = parent;
        }
    }

    fn pop(&mut self, keys: &[Vec<Rows>]) -> Option<HeapItem> {
        let last = self.items.len().checked_sub(1)?;
        self.items.swap(0, last);
        let top = self.items.pop();
        self.sift_down(keys);
        top
    }

    /// The first item in merge order, left in the heap.
    fn peek(&self) -> Option<HeapItem> {
        self.items.first().copied()
    }

    /// Replace the first item with `item` and restore the heap order: one
    /// sift down, where a [`MergeHeap::pop`] and a [`MergeHeap::push`] take a
    /// sift down and a sift up. The heap must not be empty.
    fn replace_top(&mut self, item: HeapItem, keys: &[Vec<Rows>]) {
        self.items[0] = item;
        self.sift_down(keys);
    }

    /// Move the first item down to its place in merge order.
    fn sift_down(&mut self, keys: &[Vec<Rows>]) {
        let len = self.items.len();
        let mut parent = 0;
        loop {
            let (left, right) = (2 * parent + 1, 2 * parent + 2);
            let mut first = parent;
            if left < len && self.items[left].before(&self.items[first], keys) {
                first = left;
            }
            if right < len && self.items[right].before(&self.items[first], keys) {
                first = right;
            }
            if first == parent {
                break;
            }
            self.items.swap(parent, first);
            parent = first;
        }
    }
}

/// Row converter for the sort keys of a schema.
fn key_converter(schema: &Schema, spec: &SortSpec) -> Result<RowConverter> {
    let fields: Vec<SortField> = spec
        .keys
        .iter()
        .map(|k| {
            let dt = schema.field_with_name(&k.column)?.data_type().clone();
            Ok(SortField::new_with_options(dt, SortSpec::options(k)))
        })
        .collect::<std::result::Result<_, arrow::error::ArrowError>>()?;
    Ok(RowConverter::new(fields)?)
}

/// Encode a batch's normalized sort-key columns into comparable rows.
fn key_rows(batch: &RecordBatch, spec: &SortSpec, converter: &RowConverter) -> Result<Rows> {
    let cols: Vec<ArrayRef> = sort_columns(batch, spec)?
        .into_iter()
        .map(|c| c.values)
        .collect();
    Ok(converter.convert_columns(&cols)?)
}

/// Whether [`merge_key_bound`] bounds the merge keys of a sort column of
/// this type: exactly the sortable types of the lake's values columns
/// (Int32, Int64, Float64, UTC microsecond timestamps, booleans, the 8- and
/// 16-byte ids and strings), each pinned against Arrow's row format by a
/// conformance test.
#[must_use]
pub fn merge_key_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int32
            | DataType::Int64
            | DataType::Float64
            | DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _)
            | DataType::Boolean
            | DataType::FixedSizeBinary(8 | 16)
            | DataType::Utf8
    )
}

/// Row-format bytes of one non-null variable-length value of `len` bytes:
/// a sentinel, then 8-byte mini-blocks up to 32 bytes and 32-byte blocks
/// beyond, each followed by a continuation byte (`arrow_row`'s
/// `variable::padded_length`). A null or empty value takes the sentinel
/// alone.
fn encoded_value_len(len: usize) -> usize {
    const BLOCK: usize = 32;
    const MINI: usize = 8;
    if len <= BLOCK {
        1 + len.div_ceil(MINI) * (MINI + 1)
    } else {
        BLOCK / MINI + len.div_ceil(BLOCK) * (BLOCK + 1)
    }
}

/// Row-format bytes of every value of one string column, from its offsets
/// and validity.
fn encoded_column_len(offsets: &[i32], nulls: Option<&NullBuffer>) -> usize {
    offsets
        .windows(2)
        .enumerate()
        .map(|(i, pair)| {
            if nulls.is_some_and(|nulls| nulls.is_null(i)) {
                1
            } else {
                encoded_value_len(usize::try_from(pair[1] - pair[0]).unwrap_or(0))
            }
        })
        .sum()
}

/// The part of [`merge_key_bound`] that does not depend on the key values:
/// offsets, key segments and merge heap entries for `rows` rows.
fn merge_key_overhead(rows: usize) -> usize {
    let segment = size_of::<Rows>() + size_of::<usize>();
    rows * (size_of::<usize>() + (2 * segment).div_ceil(MIN_KEY_SLICE_ROWS))
        + 2 * (segment + size_of::<HeapItem>())
}

/// Upper bound on the heap a table's merge keeps for the sort keys of
/// `batch`'s rows, the batch being one of the table's runs or part of one;
/// [`MergeIter::resident_key_bytes`] is what it bounds.
///
/// Per row: the encoded key, computed from the key columns the way the row
/// format lays them out, its offset, and the share of two key segments at the
/// fewest rows a segment holds (`MIN_KEY_SLICE_ROWS`): at most one short
/// segment ends each run and each merge step. Per batch, since a run holds at
/// least one batch: two more segments and two merge heap entries. Zero for an
/// empty spec. A key column the batch lacks adds nothing: the sort itself
/// refuses it.
///
/// # Errors
/// Refuses a key type [`merge_key_supported`] does not cover.
pub fn merge_key_bound(batch: &RecordBatch, spec: &SortSpec) -> Result<usize> {
    let rows = batch.num_rows();
    if spec.is_empty() || rows == 0 {
        return Ok(0);
    }
    let mut bytes = merge_key_overhead(rows);
    for key in &spec.keys {
        let Some(column) = batch.column_by_name(&key.column) else {
            continue;
        };
        let data_type = column.data_type();
        let unbounded = || {
            Error::internal(format!(
                "sort column {} has type {data_type}, whose merge keys are not bounded",
                key.column
            ))
        };
        if !merge_key_supported(data_type) {
            return Err(unbounded());
        }
        bytes += match data_type {
            DataType::Boolean => rows * 2,
            DataType::FixedSizeBinary(width) => rows * (1 + *width as usize),
            DataType::Utf8 => {
                encoded_column_len(column.as_string::<i32>().value_offsets(), column.nulls())
            }
            other => rows * (1 + other.primitive_width().ok_or_else(unbounded)?),
        };
    }
    Ok(bytes)
}

/// Average pinned bytes per row over every run, deduplicating shared buffers
/// (tests; the build keeps the same totals as it goes).
#[cfg(test)]
fn avg_row_bytes(runs: &[RecordBatch]) -> usize {
    let mut seen = CountedAllocations::default();
    let mut bytes = 0usize;
    let mut rows = 0usize;
    for r in runs {
        bytes += record_batch_pinned_bytes(r, &mut seen);
        rows += r.num_rows();
    }
    if rows == 0 {
        return 1;
    }
    (bytes / rows).max(1)
}

/// A merge whose sort keys are still being encoded, one bounded slice per
/// [`MergeBuild::step`].
///
/// The keys of every row of every run must exist before the first row can
/// leave the merge, and encoding them is linear in the rows and in the key
/// width. Built in one call, that is one uninterrupted stretch as long as the
/// table is large; built here, it is a sequence of slices of at most
/// [`MERGE_STEP_ROWS`] rows and about [`MERGE_STEP_KEY_BYTES`] encoded bytes,
/// between which a caller on an asynchronous runtime can return to it.
///
/// Each run's keys are kept as the segments the slices produced. Every
/// segment is encoded by the one converter of the merge, so a row's encoded
/// key, and therefore every comparison and the whole output, is the same as
/// if the run had been encoded in one call.
pub struct MergeBuild {
    runs: Vec<RecordBatch>,
    schema: SchemaRef,
    spec: SortSpec,
    converter: Option<RowConverter>,
    keys: Vec<Vec<Rows>>,
    chunk_bytes: usize,
    /// The run being encoded and the first row of it not yet encoded.
    run: usize,
    offset: usize,
    /// Encoded key bytes and rows so far, for the next slice's width.
    encoded_bytes: usize,
    encoded_rows: usize,
    budget: StepBudget,
    sorted: bool,
    /// Totals kept as the slices are encoded, so that neither reporting the
    /// keys nor finishing the build scans them again: the heap every run's
    /// first row enters once encoded, the heap bytes of every segment, and
    /// the pinned bytes and rows of the runs begun so far, which set the
    /// chunk's row count.
    heap: MergeHeap,
    key_bytes: usize,
    pinned_bytes: usize,
    pinned_rows: usize,
    seen: CountedAllocations,
    /// Every run's column data, per output column, collected as each run
    /// is begun: what the chunk builder copies rows from.
    sources: Vec<Vec<ArrayData>>,
}

impl MergeBuild {
    /// Validate the runs and prepare the merge, encoding no key yet.
    ///
    /// Runs without rows are dropped. With an empty spec the merge is the
    /// unsorted pass-through and there is nothing to encode.
    ///
    /// # Errors
    ///
    /// Refuses runs whose fields differ, and a spec naming a column the
    /// runs do not have.
    pub fn new(runs: Vec<RecordBatch>, spec: &SortSpec, chunk_bytes: usize) -> Result<Self> {
        let runs: Vec<RecordBatch> = runs.into_iter().filter(|r| r.num_rows() > 0).collect();
        let schema = match runs.first() {
            Some(first) => first.schema(),
            None => Arc::new(Schema::empty()),
        };
        // Every run must share one set of fields: the merge reads column `c` of every
        // run for each output column, so a narrower run would be an out-of-bounds
        // index and a differently typed one would fail `interleave`. Only the fields
        // matter, so runs that differ solely in schema-level metadata merge fine and
        // the first run's metadata is carried into the output.
        if runs.iter().any(|r| r.schema().fields() != schema.fields()) {
            return Err(Error::internal("merge runs have different schemas"));
        }
        let sorted = !runs.is_empty() && !spec.is_empty();
        // A sorted merge builds every output column in bounded steps, which
        // only the column types of the lake datasets have.
        if sorted
            && let Some(field) = schema
                .fields()
                .iter()
                .find(|field| column_kind(field.data_type()).is_none())
        {
            return Err(Error::internal(format!(
                "column {:?} of type {} has no bounded merge",
                field.name(),
                field.data_type()
            )));
        }
        let converter = if sorted {
            Some(key_converter(&schema, spec)?)
        } else {
            None
        };
        let keys = if sorted {
            runs.iter().map(|_| Vec::new()).collect()
        } else {
            Vec::new()
        };
        Ok(Self {
            runs,
            schema,
            spec: spec.clone(),
            converter,
            keys,
            chunk_bytes,
            run: 0,
            offset: 0,
            encoded_bytes: 0,
            encoded_rows: 0,
            budget: StepBudget::DEFAULT,
            sorted,
            heap: MergeHeap::default(),
            key_bytes: 0,
            pinned_bytes: 0,
            pinned_rows: 0,
            seen: CountedAllocations::default(),
            sources: Vec::new(),
        })
    }

    /// The same build, sliced by a different budget (tests).
    #[cfg(test)]
    fn with_budget(self, rows: usize, key_bytes: usize) -> Self {
        Self {
            budget: StepBudget {
                rows,
                key_bytes,
                ..StepBudget::DEFAULT
            },
            ..self
        }
    }

    /// Whether every key has been encoded.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        !self.sorted || self.run >= self.runs.len()
    }

    /// Encode the next keys, one slice per run or part of a run, until the
    /// step's budget of rows and encoded bytes is spent. Returns whether
    /// every key is now encoded; a complete build does nothing.
    ///
    /// Many small runs, such as a series table's, share one step, so a small
    /// table is not charged a return to the runtime per run.
    ///
    /// # Errors
    ///
    /// Returns the Arrow failure of encoding a slice.
    pub fn step(&mut self) -> Result<bool> {
        let (mut rows_done, mut bytes_done) = (0usize, 0usize);
        while !self.is_complete()
            && rows_done < self.budget.rows
            && bytes_done < self.budget.key_bytes
        {
            let converter = self
                .converter
                .as_ref()
                .ok_or_else(|| Error::internal("sorted merge without a converter"))?;
            let run = &self.runs[self.run];
            let average = (self.encoded_rows > 0).then(|| self.encoded_bytes / self.encoded_rows);
            let rows = self
                .budget
                .key_slice_rows(average)
                .min(self.budget.rows - rows_done)
                .min(run.num_rows() - self.offset);
            if self.offset == 0 {
                self.pinned_bytes += record_batch_pinned_bytes(run, &mut self.seen);
                self.pinned_rows += run.num_rows();
                if self.sources.is_empty() {
                    self.sources = vec![Vec::with_capacity(self.runs.len()); run.num_columns()];
                }
                for (column, data) in self.sources.iter_mut().zip(run.columns()) {
                    column.push(data.to_data());
                }
            }
            let slice = run.slice(self.offset, rows);
            let encoded = key_rows(&slice, &self.spec, converter)?;
            let bytes: usize = encoded.lengths().sum();
            self.encoded_bytes += bytes;
            self.encoded_rows += rows;
            self.key_bytes += encoded.size();
            rows_done += rows;
            bytes_done += bytes;
            self.keys[self.run].push(encoded);
            if self.offset == 0 {
                let first = HeapItem {
                    run: self.run,
                    idx: 0,
                    seg: 0,
                    off: 0,
                };
                self.heap.push(first, &self.keys);
            }
            self.offset += rows;
            if self.offset >= run.num_rows() {
                self.run += 1;
                self.offset = 0;
            }
        }
        Ok(self.is_complete())
    }

    /// Heap the keys encoded so far hold beside the runs: every segment and
    /// the heap's own allocation. Kept as running totals, so asking costs
    /// nothing.
    #[must_use]
    pub fn resident_key_bytes(&self) -> usize {
        self.key_bytes + self.heap.capacity() * size_of::<HeapItem>()
    }

    /// Seed the merge heap and hand over the merge, encoding whatever keys
    /// are still missing first.
    ///
    /// # Errors
    ///
    /// Returns the Arrow failure of encoding a remaining slice.
    pub fn finish(mut self) -> Result<MergeIter> {
        while !self.step()? {}
        // Everything below was kept up to date by the slices, so this is
        // constant work however large the table: the heap was seeded run by
        // run, and the chunk row count comes from the running pinned totals,
        // deduplicated across runs exactly as a single pass would.
        let rows_per_chunk = if self.sorted {
            let average = self
                .pinned_bytes
                .checked_div(self.pinned_rows)
                .map_or(1, |average| average.max(1));
            (self.chunk_bytes / average).max(1)
        } else {
            1
        };
        Ok(MergeIter {
            runs: self.runs,
            schema: self.schema,
            keys: self.keys,
            heap: self.heap,
            key_bytes: self.key_bytes,
            rows_per_chunk,
            next_run: 0,
            sorted: self.sorted,
            budget: self.budget,
            sources: self.sources,
            ranges: Vec::new(),
            pending_rows: 0,
            #[cfg(test)]
            last_step_rows: 0,
        })
    }
}

/// What one [`MergeIter::step`] produced.
#[derive(Debug)]
pub enum MergeStep {
    /// Bounded work was done towards the next chunk; step again.
    Progress,
    /// Every row of the next sorted chunk is popped: build it, in bounded
    /// steps, with [`MergeIter::chunk_builder`], then call
    /// [`MergeIter::chunk_taken`].
    Ready,
    /// The next output chunk, ready as it is: one of the input runs, with
    /// sorting disabled.
    Chunk(RecordBatch),
    /// The merge is exhausted.
    Done,
}

/// Lazy k-way merge: one output chunk is materialized per `next()`.
///
/// Memory: the iterator holds the input runs, the encoded sort keys of every row
/// of every run, and one output chunk. The encoded keys cover the key columns
/// only, not the whole dataset, so they are a small fraction of the runs
/// themselves; they must stay resident because the heap compares rows from any
/// run at any point of the merge.
///
/// Ledger (approximation): the chunk row count is fixed up front from the
/// average pinned bytes per row over all runs, so a chunk whose rows happen to
/// be wider than average exceeds `chunk_bytes`. The overshoot is bounded by
/// `max_row_bytes` per row, and `max_row_bytes <= run_target_bytes / 4` is
/// validated at startup.
///
/// A chunk can be produced in bounded steps through [`MergeIter::step`]: a
/// step pops heap rows and interleaves output columns until it has done
/// about [`MERGE_STEP_ROWS`] rows of work or compared
/// [`MERGE_STEP_KEY_BYTES`] of keys. `next()` runs the same steps back to
/// back, so both produce exactly the same chunks.
pub struct MergeIter {
    runs: Vec<RecordBatch>,
    schema: SchemaRef,
    /// Sorted mode: encoded keys per run, as segments, plus the merge heap.
    keys: Vec<Vec<Rows>>,
    heap: MergeHeap,
    /// Heap bytes of every key segment, a total kept by the build.
    key_bytes: usize,
    rows_per_chunk: usize,
    /// Unsorted mode: index of the next run to hand out unchanged.
    next_run: usize,
    sorted: bool,
    budget: StepBudget,
    /// Sorted mode: every run's column data, per output column.
    sources: Vec<Vec<ArrayData>>,
    /// Sorted mode: the rows popped for the chunk being produced, as runs of
    /// consecutive rows of one input run: run, first row, rows.
    ranges: Vec<(usize, usize, usize)>,
    pending_rows: usize,
    /// Rows the last step popped.
    #[cfg(test)]
    last_step_rows: usize,
}

impl MergeIter {
    /// Heap bytes the merge holds beside its input runs, as a bound that
    /// holds for the iterator's whole life.
    ///
    /// Every run's encoded sort keys, allocated when the merge is built and
    /// released only when the iterator is dropped, plus the merge heap, whose
    /// entries point into those keys and never grow past one per run. The
    /// runs themselves are not counted: the block they came from already
    /// accounts for them. Zero in unsorted mode, which encodes no keys.
    #[must_use]
    pub fn resident_key_bytes(&self) -> usize {
        let entries = self.heap.capacity().max(self.keys.len());
        self.key_bytes + entries * size_of::<HeapItem>()
    }

    /// Whether the chunks this merge hands out are allocated by it.
    ///
    /// Sorted chunks are interleaved into buffers of their own; with sorting
    /// disabled every chunk is one of the input runs, unchanged, and shares
    /// its buffers.
    #[must_use]
    pub fn allocates_chunks(&self) -> bool {
        self.sorted
    }

    /// Heap the rows popped for the next chunk hold right now; a
    /// [`ChunkBuilder`] reports what the chunk's columns hold.
    #[must_use]
    pub fn chunk_workspace_bytes(&self) -> usize {
        self.ranges.capacity() * size_of::<(usize, usize, usize)>()
    }

    /// A builder for the chunk whose rows the last [`MergeStep::Ready`]
    /// announced.
    #[must_use]
    pub fn chunk_builder(&self) -> ChunkBuilder<'_> {
        ChunkBuilder {
            merge: self,
            columns: Vec::with_capacity(self.schema.fields().len()),
            current: None,
        }
    }

    /// The chunk of the last [`MergeStep::Ready`] was built: the next step
    /// pops the rows of the one after it.
    pub fn chunk_taken(&mut self) {
        self.ranges.clear();
        self.pending_rows = 0;
    }

    /// What the key segments and the merge heap hold right now (tests).
    #[cfg(test)]
    fn resident_key_bytes_now(&self) -> usize {
        self.keys.iter().flatten().map(Rows::size).sum::<usize>()
            + self.heap.capacity() * size_of::<HeapItem>()
    }

    /// The same merge, stepped by a different budget (tests).
    #[cfg(test)]
    fn with_budget(self, rows: usize, key_bytes: usize) -> Self {
        Self {
            budget: StepBudget {
                rows,
                key_bytes,
                ..StepBudget::DEFAULT
            },
            ..self
        }
    }

    /// The same merge with a lower offset limit (tests).
    #[cfg(test)]
    fn with_offset_limit(mut self, limit: usize) -> Self {
        self.budget.offset_limit = limit;
        self
    }

    /// Do one bounded step of producing the next chunk: pop at most the
    /// budget's rows, announce the chunk once all of its rows are popped,
    /// or, with sorting disabled, hand out the next run.
    pub fn step(&mut self) -> MergeStep {
        if !self.sorted {
            // Unsorted mode: hand out the runs in arrival order, unchanged. No
            // concatenation, so no second copy of the dataset is ever built.
            let Some(run) = self.runs.get(self.next_run) else {
                return MergeStep::Done;
            };
            self.next_run += 1;
            return MergeStep::Chunk(run.clone());
        }
        let mut work = 0usize;
        let mut compared = 0usize;
        let result = loop {
            if self.popped_enough() {
                break if self.pending_rows == 0 {
                    MergeStep::Done
                } else {
                    MergeStep::Ready
                };
            }
            if work >= self.budget.rows || compared >= self.budget.key_bytes {
                break MergeStep::Progress;
            }
            let (rows, bytes) = self.pop_slice(self.budget.rows - work);
            work += rows;
            compared += bytes;
        };
        #[cfg(test)]
        {
            self.last_step_rows = work;
        }
        result
    }

    /// Whether the rows popped so far complete a chunk: the chunk's row
    /// count is reached or the heap is exhausted.
    fn popped_enough(&self) -> bool {
        self.pending_rows >= self.rows_per_chunk || self.heap.is_empty()
    }

    /// Pop at most `max_rows` more rows of the chunk being produced, and at
    /// most about the budget's key bytes; returns the rows popped and the
    /// key bytes of the rows it entered into the heap.
    fn pop_slice(&mut self, max_rows: usize) -> (usize, usize) {
        let mut rows = 0usize;
        let mut compared = 0usize;
        while rows < max_rows && compared < self.budget.key_bytes {
            // The run of the first row stays in the heap with its next row
            // in place of this one, or leaves it when it has no next row.
            let Some(item) = self.heap.peek() else {
                break;
            };
            match self.ranges.last_mut() {
                Some(last) if last.0 == item.run && last.1 + last.2 == item.idx => last.2 += 1,
                _ => self.ranges.push((item.run, item.idx, 1)),
            }
            self.pending_rows += 1;
            rows += 1;
            let segments = &self.keys[item.run];
            let (seg, off) = if item.off + 1 < segments[item.seg].num_rows() {
                (item.seg, item.off + 1)
            } else {
                (item.seg + 1, 0)
            };
            if let Some(segment) = segments.get(seg) {
                compared += segment.row(off).as_ref().len();
                let next = HeapItem {
                    run: item.run,
                    idx: item.idx + 1,
                    seg,
                    off,
                };
                self.heap.replace_top(next, &self.keys);
            } else {
                let _ = self.heap.pop(&self.keys);
            }
            if self.pending_rows >= self.rows_per_chunk {
                break;
            }
        }
        (rows, compared)
    }
}

/// How one output column of a chunk is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnKind {
    /// Fixed width, boolean, fixed-size binary, string or binary: copied by
    /// row into a presized buffer.
    Flat,
    /// A list of fixed-width items.
    List,
    /// A map whose keys and values are strings or binaries and whose entries
    /// carry no nulls of their own.
    Map,
}

/// How a column of `data_type` is built in bounded steps; `None` for a type
/// no lake dataset has, which [`MergeBuild::new`] refuses.
fn column_kind(data_type: &DataType) -> Option<ColumnKind> {
    let bytes = |dt: &DataType| matches!(dt, DataType::Utf8 | DataType::Binary);
    match data_type {
        DataType::Boolean | DataType::FixedSizeBinary(_) => Some(ColumnKind::Flat),
        dt if bytes(dt) || dt.primitive_width().is_some() => Some(ColumnKind::Flat),
        DataType::List(item) if item.data_type().primitive_width().is_some() => {
            Some(ColumnKind::List)
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields)
                if fields.len() == 2
                    && bytes(fields[0].data_type())
                    && bytes(fields[1].data_type()) =>
            {
                Some(ColumnKind::Map)
            }
            _ => None,
        },
        _ => None,
    }
}

/// The logical `i32` offsets of a string, binary, list or map array.
fn offsets_of(data: &ArrayData) -> &[i32] {
    &data.buffers()[0].typed_data::<i32>()[data.offset()..]
}

/// Values the offsets of `data` span over rows `first..first + len`.
fn span_of(data: &ArrayData, first: usize, len: usize) -> usize {
    let offsets = offsets_of(data);
    (offsets[first + len] - offsets[first]) as usize
}

/// `n` rounded up as arrow rounds a buffer's allocation.
fn allocated(n: usize) -> usize {
    arrow::util::bit_util::round_upto_multiple_of_64(n)
}

/// The row capacity a presized `MutableArrayData` of `rows` rows of
/// `data_type` is created with.
///
/// Extending a string or binary array reserves one offset more than it
/// writes, so an offsets buffer sized for exactly `rows` rows would be
/// reallocated, and wholly copied, by the last extend of a column whose
/// size happens to be a multiple of 64 bytes. One spare row keeps every
/// extend inside the buffer.
fn row_capacity(data_type: &DataType, rows: usize) -> usize {
    match data_type {
        DataType::Utf8 | DataType::Binary => rows + 1,
        _ => rows,
    }
}

/// The capacities a presized `MutableArrayData` of `rows` rows of
/// `data_type` is created with, `bytes` being the value bytes of a string
/// or binary column.
fn flat_capacities(data_type: &DataType, rows: usize, bytes: usize) -> Capacities {
    match data_type {
        DataType::Utf8 | DataType::Binary => {
            Capacities::Binary(row_capacity(data_type, rows), Some(bytes))
        }
        _ => Capacities::Array(rows),
    }
}

/// Bytes a `MutableArrayData` created with [`flat_capacities`] allocates.
fn flat_charge(data_type: &DataType, rows: usize, bytes: usize, nulls: bool) -> usize {
    let capacity = row_capacity(data_type, rows);
    let data = match data_type {
        DataType::Utf8 | DataType::Binary => {
            allocated((capacity + 1) * size_of::<i32>()) + allocated(bytes)
        }
        DataType::Boolean => allocated(capacity.div_ceil(8)),
        DataType::FixedSizeBinary(width) => allocated(capacity * *width as usize),
        other => allocated(capacity * other.primitive_width().unwrap_or(0)),
    };
    data + if nulls { capacity.div_ceil(8) } else { 0 }
}

/// Validity of rows `start..start + len` of `data`, appended to `builder`.
fn append_validity(builder: &mut BooleanBufferBuilder, data: &ArrayData, start: usize, len: usize) {
    match data.nulls() {
        Some(nulls) => builder.append_buffer(&nulls.inner().slice(start, len)),
        None => builder.append_n(len, true),
    }
}

/// A column's buffers once allocated.
enum Built<'a> {
    Flat(MutableArrayData<'a>),
    List {
        offsets: OffsetBufferBuilder<i32>,
        nulls: Option<BooleanBufferBuilder>,
        items: MutableArrayData<'a>,
    },
    Map {
        offsets: OffsetBufferBuilder<i32>,
        nulls: Option<BooleanBufferBuilder>,
        /// Keys and values, boxed: the one variant holding two children.
        children: Box<[MutableArrayData<'a>; 2]>,
    },
}

/// One column of a chunk being built.
struct ColumnBuild<'a> {
    kind: ColumnKind,
    /// What the chunk's rows need, counted before allocation: the value
    /// bytes of a string column, a list's items, or a map's entries and
    /// their key and value bytes.
    counts: [usize; 3],
    built: Option<Built<'a>>,
    /// Bytes the allocated buffers hold, as allocated.
    charge: usize,
    /// The next range to count or copy, and the rows of it already copied.
    range: usize,
    offset: usize,
}

/// Builds one sorted chunk from the rows its [`MergeIter`] has popped, in
/// steps.
///
/// A step's budget is [`MERGE_STEP_ROWS`] elements: every row counts one,
/// and every item of a list row or entry of a map row counts one more.
/// Each column is first sized (string value bytes, list items, map entries
/// and their key and value bytes, a bounded number of ranges per step) and
/// allocated once at that size. Rows are then copied until the budget is
/// spent, a range split wherever it would pass the budget; a single row larger
/// than the budget takes a step of its own, and a row is bounded by
/// `ingress.max_row_bytes`. Completing a column freezes its buffers, so no
/// step copies more than its budget and none reallocates.
///
/// Lists and maps are assembled from their own offsets, validity and
/// presized children, because `MutableArrayData` cannot presize a map's
/// entries. A presized total an `i32` offset cannot hold is an error before
/// anything is copied.
///
/// Only the column types of the lake datasets have such a build, which
/// `every_lake_column_is_built_in_bounded_steps` pins; [`MergeBuild::new`]
/// refuses a sorted merge of any other type.
///
/// Every column is the same array an interleave of the same rows would
/// give, so the chunk, and every byte written from it, is unchanged.
pub struct ChunkBuilder<'a> {
    merge: &'a MergeIter,
    columns: Vec<ArrayRef>,
    current: Option<ColumnBuild<'a>>,
}

impl<'a> ChunkBuilder<'a> {
    /// Do one step of building the chunk, bounded for the lake's column types
    /// (see [`ChunkBuilder`]). Returns whether every column is built.
    ///
    /// # Errors
    ///
    /// Returns an offset overflow for a column whose presized total does
    /// not fit an `i32` offset, and the Arrow failure of interleaving or
    /// assembling a column.
    pub fn step(&mut self) -> Result<bool> {
        let merge = self.merge;
        let budget = merge.budget.rows;
        let columns = merge.schema.fields().len();
        let mut work = 0usize;
        while self.columns.len() < columns && work < budget {
            let c = self.columns.len();
            let data_type = merge.schema.field(c).data_type();
            let sources = &merge.sources[c];
            let column = match &mut self.current {
                Some(column) => column,
                empty => {
                    // `MergeBuild::new` refused every other type.
                    let kind = column_kind(data_type)
                        .ok_or_else(|| Error::internal("merge column without a bounded build"))?;
                    // Arrow's `MapArray` refuses null entries; the copy relies on it.
                    if kind == ColumnKind::Map
                        && sources
                            .iter()
                            .any(|source| source.child_data()[0].null_count() != 0)
                    {
                        return Err(Error::internal("merge map column with null entries"));
                    }
                    empty.insert(ColumnBuild {
                        kind,
                        counts: [0; 3],
                        built: None,
                        charge: 0,
                        range: 0,
                        offset: 0,
                    })
                }
            };
            if column.built.is_none() {
                while column.range < merge.ranges.len() && work < budget {
                    column.count(data_type, sources, merge.ranges[column.range]);
                    column.range += 1;
                    work += 1;
                }
                if column.range < merge.ranges.len() {
                    break;
                }
                column.allocate(
                    data_type,
                    sources,
                    merge.pending_rows,
                    merge.budget.offset_limit,
                )?;
            }
            while column.range < merge.ranges.len() && work < budget {
                let copied = column.copy(
                    sources,
                    merge.ranges[column.range],
                    budget - work,
                    work == 0,
                );
                if copied == 0 {
                    break;
                }
                work += copied;
            }
            if column.range < merge.ranges.len() {
                break;
            }
            let done = self
                .current
                .take()
                .ok_or_else(|| Error::internal("chunk column vanished"))?;
            self.columns.push(done.freeze(data_type)?);
        }
        Ok(self.columns.len() == columns)
    }

    /// Heap the chunk's columns hold so far: the built ones as measured,
    /// and the one being built as its buffers were allocated.
    #[must_use]
    pub fn workspace_bytes(&self) -> usize {
        self.columns
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>()
            + self.current.as_ref().map_or(0, |column| column.charge)
    }

    /// Elements the builder's buffers hold, read from their lengths: rows,
    /// plus the items of lists and the entries of maps (tests).
    #[cfg(test)]
    fn copied_elements(&self) -> usize {
        let children = |array: &ArrayRef| {
            if let Some(list) = array.as_list_opt::<i32>() {
                list.values().len()
            } else if let Some(map) = array.as_map_opt() {
                map.entries().len()
            } else {
                0
            }
        };
        let built = self
            .columns
            .iter()
            .map(|array| array.len() + children(array))
            .sum::<usize>();
        let current = match self
            .current
            .as_ref()
            .and_then(|column| column.built.as_ref())
        {
            None => 0,
            Some(Built::Flat(data)) => data.len(),
            Some(Built::List { offsets, items, .. }) => offsets.len() - 1 + items.len(),
            Some(Built::Map {
                offsets, children, ..
            }) => offsets.len() - 1 + children[0].len(),
        };
        built + current
    }

    /// What the column being built was charged when allocated (tests).
    #[cfg(test)]
    fn current_charge(&self) -> Option<usize> {
        self.current
            .as_ref()
            .filter(|column| column.built.is_some())
            .map(|column| column.charge)
    }

    /// The built chunk.
    ///
    /// # Errors
    ///
    /// Refuses a chunk whose columns are not all built, and one that does
    /// not match the merge's schema.
    pub fn finish(self) -> Result<RecordBatch> {
        if self.columns.len() != self.merge.schema.fields().len() {
            return Err(Error::internal(
                "chunk finished before every column was built",
            ));
        }
        Ok(RecordBatch::try_new(
            self.merge.schema.clone(),
            self.columns,
        )?)
    }
}

impl<'a> ColumnBuild<'a> {
    /// Count what one range of rows needs.
    fn count(
        &mut self,
        data_type: &DataType,
        sources: &[ArrayData],
        (run, first, len): (usize, usize, usize),
    ) {
        let data = &sources[run];
        match self.kind {
            ColumnKind::Flat if matches!(data_type, DataType::Utf8 | DataType::Binary) => {
                self.counts[0] += span_of(data, first, len);
            }
            ColumnKind::List => self.counts[0] += span_of(data, first, len),
            ColumnKind::Map => {
                let offsets = offsets_of(data);
                let (start, end) = (offsets[first] as usize, offsets[first + len] as usize);
                let entries = &data.child_data()[0];
                self.counts[0] += end - start;
                self.counts[1] += span_of(&entries.child_data()[0], start, end - start);
                self.counts[2] += span_of(&entries.child_data()[1], start, end - start);
            }
            ColumnKind::Flat => {}
        }
    }

    /// Allocate the column's buffers for `rows` rows, as counted, after
    /// checking every counted total fits an `i32` offset.
    fn allocate(
        &mut self,
        data_type: &DataType,
        sources: &'a [ArrayData],
        rows: usize,
        limit: usize,
    ) -> Result<()> {
        if let Some(&total) = self.counts.iter().find(|&&total| total > limit) {
            return Err(arrow::error::ArrowError::OffsetOverflowError(total).into());
        }
        let nulls = |data: &[&ArrayData]| data.iter().any(|d| d.null_count() > 0);
        let validity = |data: &[&ArrayData]| nulls(data).then(|| BooleanBufferBuilder::new(rows));
        let validity_charge = |present: bool| {
            if present {
                allocated(rows.div_ceil(8))
            } else {
                0
            }
        };
        let tops: Vec<&ArrayData> = sources.iter().collect();
        let offsets_charge = (rows + 1) * size_of::<i32>();
        match self.kind {
            ColumnKind::Flat => {
                let bytes = self.counts[0];
                let capacities = flat_capacities(data_type, rows, bytes);
                self.charge = flat_charge(data_type, rows, bytes, nulls(&tops));
                self.built = Some(Built::Flat(MutableArrayData::with_capacities(
                    tops, false, capacities,
                )));
            }
            ColumnKind::List => {
                let children: Vec<&ArrayData> =
                    sources.iter().map(|d| &d.child_data()[0]).collect();
                let items = self.counts[0];
                let item_type = children
                    .first()
                    .map_or(DataType::Null, |d| d.data_type().clone());
                let nulls_builder = validity(&tops);
                self.charge = offsets_charge
                    + validity_charge(nulls_builder.is_some())
                    + flat_charge(&item_type, items, 0, nulls(&children));
                self.built = Some(Built::List {
                    offsets: OffsetBufferBuilder::new(rows),
                    nulls: nulls_builder,
                    items: MutableArrayData::with_capacities(
                        children,
                        false,
                        Capacities::Array(items),
                    ),
                });
            }
            ColumnKind::Map => {
                let [entries, key_bytes, value_bytes] = self.counts;
                let keys: Vec<&ArrayData> = sources
                    .iter()
                    .map(|d| &d.child_data()[0].child_data()[0])
                    .collect();
                let values: Vec<&ArrayData> = sources
                    .iter()
                    .map(|d| &d.child_data()[0].child_data()[1])
                    .collect();
                let key_type = keys
                    .first()
                    .map_or(DataType::Utf8, |d| d.data_type().clone());
                let value_type = values
                    .first()
                    .map_or(DataType::Utf8, |d| d.data_type().clone());
                let nulls_builder = validity(&tops);
                self.charge = offsets_charge
                    + validity_charge(nulls_builder.is_some())
                    + flat_charge(&key_type, entries, key_bytes, nulls(&keys))
                    + flat_charge(&value_type, entries, value_bytes, nulls(&values));
                self.built = Some(Built::Map {
                    offsets: OffsetBufferBuilder::new(rows),
                    nulls: nulls_builder,
                    children: Box::new([
                        MutableArrayData::with_capacities(
                            keys,
                            false,
                            flat_capacities(&key_type, entries, key_bytes),
                        ),
                        MutableArrayData::with_capacities(
                            values,
                            false,
                            flat_capacities(&value_type, entries, value_bytes),
                        ),
                    ]),
                });
            }
        }
        self.range = 0;
        self.offset = 0;
        Ok(())
    }

    /// Copy the next rows of one range, at most `budget` elements; returns
    /// the elements copied, zero when the next row does not fit. A row
    /// larger than the whole budget is copied alone when `alone`, by a step
    /// that has copied nothing else.
    fn copy(
        &mut self,
        sources: &[ArrayData],
        (run, first, len): (usize, usize, usize),
        budget: usize,
        alone: bool,
    ) -> usize {
        let data = &sources[run];
        let start = first + self.offset;
        let (rows, elements) = match self.built.as_mut() {
            None => (len - self.offset, 0),
            Some(Built::Flat(out)) => {
                let rows = (len - self.offset).min(budget);
                out.extend(run, start, start + rows);
                (rows, rows)
            }
            Some(Built::List { offsets, nulls, .. } | Built::Map { offsets, nulls, .. }) => {
                let source = offsets_of(data);
                let mut rows = 0usize;
                let mut elements = 0usize;
                while self.offset + rows < len {
                    let at = start + rows;
                    let children = (source[at + 1] - source[at]) as usize;
                    if (rows > 0 || !alone) && elements + 1 + children > budget {
                        break;
                    }
                    offsets.push_length(children);
                    rows += 1;
                    elements += 1 + children;
                    if elements >= budget {
                        break;
                    }
                }
                if let Some(nulls) = nulls {
                    append_validity(nulls, data, start, rows);
                }
                (rows, elements)
            }
        };
        let (from, to) = match self.built.as_ref() {
            Some(Built::List { .. } | Built::Map { .. }) => {
                let source = offsets_of(data);
                (source[start] as usize, source[start + rows] as usize)
            }
            _ => (0, 0),
        };
        match self.built.as_mut() {
            Some(Built::List { items, .. }) => items.extend(run, from, to),
            Some(Built::Map { children, .. }) => {
                for child in children.iter_mut() {
                    child.extend(run, from, to);
                }
            }
            _ => {}
        }
        self.offset += rows;
        if self.offset == len {
            self.range += 1;
            self.offset = 0;
        }
        elements
    }

    /// The column, its buffers frozen as they are.
    fn freeze(self, data_type: &DataType) -> Result<ArrayRef> {
        let nulls_of = |nulls: Option<BooleanBufferBuilder>| {
            nulls.map(|mut builder| NullBuffer::new(builder.finish()))
        };
        match (self.built, data_type) {
            (Some(Built::Flat(out)), _) => Ok(arrow::array::make_array(out.freeze())),
            (
                Some(Built::List {
                    offsets,
                    nulls,
                    items,
                }),
                DataType::List(field),
            ) => Ok(Arc::new(ListArray::try_new(
                Arc::clone(field),
                offsets.finish(),
                arrow::array::make_array(items.freeze()),
                nulls_of(nulls),
            )?)),
            (
                Some(Built::Map {
                    offsets,
                    nulls,
                    children,
                }),
                DataType::Map(entries_field, ordered),
            ) => {
                let [keys, values] = *children;
                let DataType::Struct(fields) = entries_field.data_type() else {
                    return Err(Error::internal("map entries are not a struct"));
                };
                let entries = StructArray::try_new(
                    fields.clone(),
                    vec![
                        arrow::array::make_array(keys.freeze()),
                        arrow::array::make_array(values.freeze()),
                    ],
                    None,
                )?;
                Ok(Arc::new(MapArray::try_new(
                    Arc::clone(entries_field),
                    offsets.finish(),
                    entries,
                    nulls_of(nulls),
                    *ordered,
                )?))
            }
            _ => Err(Error::internal("chunk column built for another type")),
        }
    }
}

impl Iterator for MergeIter {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.step() {
                MergeStep::Progress => {}
                MergeStep::Ready => {
                    let chunk = {
                        let mut builder = self.chunk_builder();
                        loop {
                            match builder.step() {
                                Ok(true) => break builder.finish(),
                                Ok(false) => {}
                                Err(e) => break Err(e),
                            }
                        }
                    };
                    self.chunk_taken();
                    return Some(chunk);
                }
                MergeStep::Chunk(chunk) => return Some(Ok(chunk)),
                MergeStep::Done => return None,
            }
        }
    }
}

/// Merge sorted runs into globally sorted chunks of about `chunk_bytes`.
///
/// The result is an iterator: only one output chunk exists at a time, so the
/// caller (the sink) can hand each chunk to the Parquet writer and drop it. With
/// an empty spec the runs are yielded unchanged, in arrival order.
///
/// Every key is encoded before this returns; [`MergeBuild`] builds the same
/// merge in bounded slices.
///
/// # Errors
///
/// As [`MergeBuild::new`] and [`MergeBuild::finish`].
#[cfg(test)]
pub(crate) fn merge_runs(
    runs: Vec<RecordBatch>,
    spec: &SortSpec,
    chunk_bytes: usize,
) -> Result<MergeIter> {
    MergeBuild::new(runs, spec, chunk_bytes)?.finish()
}

/// Whether a batch is sorted by the spec (test helper, also used by the oracle).
///
/// Adjacent rows are compared through their encoded `arrow::row` keys, which
/// carry the spec's ascending/descending and null placement. Comparing the
/// permutation produced by `lexsort_to_indices` would be wrong: that sort is
/// unstable, so tied keys can yield a non-identity permutation for a batch that
/// is correctly ordered.
pub fn is_sorted(batch: &RecordBatch, spec: &SortSpec) -> Result<bool> {
    if spec.is_empty() {
        return Ok(true);
    }
    check_columns(batch, spec)?;
    if batch.num_rows() < 2 {
        return Ok(true);
    }
    let converter = key_converter(&batch.schema(), spec)?;
    let rows = key_rows(batch, spec, &converter)?;
    Ok(rows.iter().is_sorted())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Nulls, SortKey, SortOrder};
    use arrow::array::{ArrayRef, AsArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
    use arrow::record_batch::RecordBatch;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// Negative quiet NaN: `total_cmp` orders it below every other double, so a
    /// missing NaN normalization moves it to the front of an ascending sort.
    const NEG_NAN: u64 = 0xFFF8_0000_0000_0000;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("f", DataType::Float64, false),
            Field::new("tag", DataType::Utf8, false),
        ])
    }

    fn batch_tagged(keys: Vec<Option<i64>>, f: Vec<f64>, tags: Vec<&str>) -> RecordBatch {
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Float64Array::from(f)),
            Arc::new(StringArray::from(tags)),
        ];
        RecordBatch::try_new(Arc::new(test_schema()), cols).expect("batch")
    }

    fn batch(keys: Vec<Option<i64>>, f: Vec<f64>, tag: &str) -> RecordBatch {
        let n = keys.len();
        batch_tagged(keys, f, vec![tag; n])
    }

    fn spec() -> SortSpec {
        SortSpec::new(vec![
            SortKey {
                column: "k".into(),
                order: SortOrder::Asc,
                nulls: Nulls::Last,
            },
            SortKey {
                column: "f".into(),
                order: SortOrder::Asc,
                nulls: Nulls::Last,
            },
        ])
    }

    /// `k` ascending, `f` ascending, then `tag` ascending as a tie-break.
    fn spec_with_tag_tiebreak() -> SortSpec {
        let mut keys = spec().keys().to_vec();
        keys.push(SortKey {
            column: "tag".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        });
        SortSpec::new(keys)
    }

    fn one_key_spec(order: SortOrder, nulls: Nulls) -> SortSpec {
        SortSpec::new(vec![SortKey {
            column: "k".into(),
            order,
            nulls,
        }])
    }

    fn keys_of(b: &RecordBatch) -> Vec<Option<i64>> {
        b.column(0).as_primitive::<Int64Type>().iter().collect()
    }

    fn bits_of(b: &RecordBatch) -> Vec<u64> {
        let f = b.column(1).as_primitive::<Float64Type>();
        (0..b.num_rows()).map(|i| f.value(i).to_bits()).collect()
    }

    fn tags_of(b: &RecordBatch) -> Vec<String> {
        let t = b.column(2).as_string::<i32>();
        (0..b.num_rows()).map(|i| t.value(i).to_string()).collect()
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_string()).collect()
    }

    fn merged(runs: Vec<RecordBatch>, spec: &SortSpec, chunk_bytes: usize) -> Vec<RecordBatch> {
        merge_runs(runs, spec, chunk_bytes)
            .expect("merge")
            .collect::<Result<_>>()
            .expect("chunks")
    }

    /// Every chunk of `merge`, stepped one step at a time, and the merge and
    /// builder steps it took.
    fn drain(merge: &mut MergeIter) -> (Vec<RecordBatch>, usize) {
        let mut chunks = Vec::new();
        let mut steps = 0usize;
        loop {
            steps += 1;
            match merge.step() {
                MergeStep::Progress => {}
                MergeStep::Ready => {
                    let mut builder = merge.chunk_builder();
                    while !builder.step().expect("build step") {
                        steps += 1;
                    }
                    chunks.push(builder.finish().expect("chunk"));
                    merge.chunk_taken();
                }
                MergeStep::Chunk(chunk) => chunks.push(chunk),
                MergeStep::Done => return (chunks, steps),
            }
        }
    }

    fn concat(chunks: &[RecordBatch]) -> RecordBatch {
        arrow::compute::concat_batches(&chunks[0].schema(), chunks).expect("concat")
    }

    /// Scenario: one key group with both zero signs and both NaN signs, tagged against the raw
    /// `total_cmp` order, the tag the third key.
    /// Guarantees: -0.0 equals +0.0 and all NaNs are equal, so the tag decides; nulls sort last and
    /// payload bits are kept.
    #[test]
    fn sort_batch_normalizes_doubles() {
        let b = batch_tagged(
            vec![Some(2), None, Some(1), Some(1), Some(1), Some(1), Some(1)],
            vec![0.0, 0.0, f64::from_bits(NEG_NAN), 0.0, 1.0, f64::NAN, -0.0],
            vec!["x", "y", "n2", "a", "z", "n1", "b"],
        );
        let s = sort_batch(&b, &spec_with_tag_tiebreak()).expect("sort");
        assert_eq!(
            keys_of(&s),
            vec![Some(1), Some(1), Some(1), Some(1), Some(1), Some(2), None]
        );
        // +0.0 is tagged "a" and -0.0 is tagged "b"; raw bits would put -0.0
        // first. Both NaNs likewise: the negative one is tagged "n2".
        assert_eq!(tags_of(&s), strings(&["a", "b", "z", "n1", "n2", "x", "y"]));
        assert_eq!(
            bits_of(&s),
            vec![
                0.0f64.to_bits(),
                (-0.0f64).to_bits(),
                1.0f64.to_bits(),
                f64::NAN.to_bits(),
                NEG_NAN,
                0.0f64.to_bits(),
                0.0f64.to_bits(),
            ],
            "the payload keeps the bits it came in with"
        );
    }

    /// Scenario: every direction and null placement over one key column holding a null.
    /// Guarantees: `sort_batch` gives the named order, `is_sorted` accepts it and rejects its
    /// reverse.
    #[test]
    fn desc_and_nulls_first_orderings_round_trip() {
        let input = batch(vec![Some(1), None, Some(3), Some(2)], vec![0.0; 4], "x");
        let cases = [
            (
                SortOrder::Desc,
                Nulls::First,
                vec![None, Some(3), Some(2), Some(1)],
            ),
            (
                SortOrder::Desc,
                Nulls::Last,
                vec![Some(3), Some(2), Some(1), None],
            ),
            (
                SortOrder::Asc,
                Nulls::First,
                vec![None, Some(1), Some(2), Some(3)],
            ),
            (
                SortOrder::Asc,
                Nulls::Last,
                vec![Some(1), Some(2), Some(3), None],
            ),
        ];
        for (order, nulls, expected) in cases {
            let s = one_key_spec(order, nulls);
            let sorted = sort_batch(&input, &s).expect("sort");
            assert_eq!(keys_of(&sorted), expected, "{order:?}/{nulls:?}");
            assert!(
                is_sorted(&sorted, &s).expect("sorted"),
                "{order:?}/{nulls:?}"
            );
            let mut reversed = expected;
            reversed.reverse();
            let backwards = batch(reversed, vec![0.0; 4], "x");
            assert!(
                !is_sorted(&backwards, &s).expect("reverse"),
                "{order:?}/{nulls:?} must reject the reverse order"
            );
        }
    }

    /// Scenario: two runs sorted descending with nulls first are merged.
    /// Guarantees: the merge keeps the direction and null placement across runs.
    #[test]
    fn desc_nulls_first_merge_is_globally_sorted() {
        let s = one_key_spec(SortOrder::Desc, Nulls::First);
        let r1 =
            sort_batch(&batch(vec![Some(5), None, Some(1)], vec![0.0; 3], "a"), &s).expect("s");
        let r2 = sort_batch(&batch(vec![Some(4), Some(9)], vec![0.0; 2], "b"), &s).expect("s");
        assert_eq!(keys_of(&r1), vec![None, Some(5), Some(1)]);
        assert_eq!(keys_of(&r2), vec![Some(9), Some(4)]);
        let all = concat(&merged(vec![r1, r2], &s, 1));
        assert_eq!(
            keys_of(&all),
            vec![None, Some(9), Some(5), Some(4), Some(1)]
        );
        assert!(is_sorted(&all, &s).expect("check"));
    }

    /// Scenario: three sorted runs merged in 2-row chunks.
    /// Guarantees: the concatenation of chunks is globally sorted and contains every row exactly once.
    #[test]
    fn merge_runs_is_globally_sorted() {
        let r1 = sort_batch(
            &batch(vec![Some(5), Some(1), Some(9)], vec![0.0; 3], "a"),
            &spec(),
        )
        .expect("s");
        let r2 = sort_batch(&batch(vec![Some(2), Some(8)], vec![0.0; 2], "b"), &spec()).expect("s");
        let r3 = sort_batch(
            &batch(vec![Some(3), None, Some(4)], vec![0.0; 3], "c"),
            &spec(),
        )
        .expect("s");
        let out: Vec<RecordBatch> = merge_runs(vec![r1, r2, r3], &spec(), 1)
            .expect("merge")
            .collect::<Result<_>>()
            .expect("chunks");
        assert!(
            out.len() >= 4,
            "tiny chunk budget must yield several chunks"
        );
        let all = arrow::compute::concat_batches(&out[0].schema(), &out).expect("concat");
        assert_eq!(all.num_rows(), 8);
        assert!(is_sorted(&all, &spec()).expect("check"));
        let k = all.column(0).as_primitive::<Int64Type>();
        assert_eq!(
            k.iter().collect::<Vec<_>>(),
            vec![
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(8),
                Some(9),
                None
            ]
        );
    }

    /// Scenario: 2000 rows of a fixed-size binary, a nullable integer and a double key with
    /// NaNs and both zeros, under every order and null placement.
    /// Guarantees: the row-format sort gives the key order `lexsort_to_indices` gives.
    #[test]
    fn row_format_run_sort_orders_like_lexsort() {
        use arrow::array::FixedSizeBinaryArray;
        let n = 2000_u64;
        let mix = |i: u64| i.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(17);
        let ids: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_iter((0..n).map(|i| (mix(i) % 7).to_be_bytes()))
                .expect("ids"),
        );
        let ints: ArrayRef =
            Arc::new(Int64Array::from_iter((0..n).map(|i| {
                (mix(i + 1) % 5 != 0).then_some((mix(i + 2) % 11) as i64 - 5)
            })));
        let doubles = (0..n).map(|i| match mix(i + 3) % 6 {
            0 => f64::NAN,
            1 => f64::from_bits(NEG_NAN),
            2 => -0.0,
            3 => 0.0,
            _ => (mix(i + 4) % 9) as f64 - 4.0,
        });
        let doubles =
            normalize_key(&(Arc::new(Float64Array::from_iter_values(doubles)) as ArrayRef));
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let sort_options = SortOptions {
                    descending,
                    nulls_first,
                };
                let options = Some(sort_options);
                let cols: Vec<SortColumn> = [&ids, &ints, &doubles]
                    .into_iter()
                    .map(|values| SortColumn {
                        values: Arc::clone(values),
                        options,
                    })
                    .collect();
                let converter = RowConverter::new(
                    cols.iter()
                        .map(|c| {
                            SortField::new_with_options(c.values.data_type().clone(), sort_options)
                        })
                        .collect(),
                )
                .expect("converter");
                let values: Vec<ArrayRef> = cols.iter().map(|c| Arc::clone(&c.values)).collect();
                let rows = converter.convert_columns(&values).expect("rows");
                let keys = |indices: &UInt32Array| -> Vec<Vec<u8>> {
                    indices
                        .values()
                        .iter()
                        .map(|&i| rows.row(i as usize).as_ref().to_vec())
                        .collect()
                };
                let ours = sort_indices(&cols).expect("sort");
                let theirs = lexsort_to_indices(&cols, None).expect("lexsort");
                assert_eq!(keys(&ours), keys(&theirs), "{options:?}");
            }
        }
    }

    /// Scenario: five runs of tied and interleaved keys drained from the merge heap two ways:
    /// pop then push of the run's next row, and replace-top of the first item.
    /// Guarantees: both give the same rows in the same order, ties in run order.
    #[test]
    fn heap_replace_top_drains_like_pop_and_push() {
        let converter =
            RowConverter::new(vec![SortField::new(DataType::Int64)]).expect("converter");
        let runs: Vec<Vec<i64>> = vec![
            vec![1, 1, 4, 9],
            vec![1, 2, 2, 2, 3],
            vec![0, 5],
            vec![2, 2, 9, 9],
            vec![7],
        ];
        let keys: Vec<Vec<Rows>> = runs
            .iter()
            .map(|run| {
                let col: ArrayRef = Arc::new(Int64Array::from(run.clone()));
                vec![converter.convert_columns(&[col]).expect("rows")]
            })
            .collect();
        let start = |heap: &mut MergeHeap| {
            for run in 0..runs.len() {
                heap.push(
                    HeapItem {
                        run,
                        idx: 0,
                        seg: 0,
                        off: 0,
                    },
                    &keys,
                );
            }
        };
        let next = |item: HeapItem| {
            (item.idx + 1 < runs[item.run].len()).then_some(HeapItem {
                idx: item.idx + 1,
                off: item.off + 1,
                ..item
            })
        };
        let mut popped = Vec::new();
        let mut heap = MergeHeap::default();
        start(&mut heap);
        while let Some(item) = heap.pop(&keys) {
            popped.push((item.run, item.idx));
            if let Some(n) = next(item) {
                heap.push(n, &keys);
            }
        }
        let mut replaced = Vec::new();
        let mut heap = MergeHeap::default();
        start(&mut heap);
        while let Some(item) = heap.peek() {
            replaced.push((item.run, item.idx));
            match next(item) {
                Some(n) => heap.replace_top(n, &keys),
                None => {
                    let _ = heap.pop(&keys);
                }
            }
        }
        assert_eq!(popped.len(), runs.iter().map(Vec::len).sum::<usize>());
        assert_eq!(replaced, popped);
        let values: Vec<i64> = popped.iter().map(|&(r, i)| runs[r][i]).collect();
        assert!(values.is_sorted(), "{values:?}");
    }

    /// Scenario: one key value repeated within a run and in two other runs, every row tagged.
    /// Guarantees: each row is emitted once, ties keep run order then run index, under one-row and
    /// whole-merge budgets.
    #[test]
    fn merge_preserves_run_local_order_among_tied_keys() {
        let runs = || {
            vec![
                batch_tagged(
                    vec![Some(1), Some(7), Some(7)],
                    vec![0.0; 3],
                    vec!["r0k1", "r0k7a", "r0k7b"],
                ),
                batch_tagged(vec![Some(7), Some(9)], vec![0.0; 2], vec!["r1k7", "r1k9"]),
                batch_tagged(vec![Some(7)], vec![0.0], vec!["r2k7"]),
            ]
        };
        for r in runs() {
            assert!(is_sorted(&r, &spec()).expect("run"), "runs start sorted");
        }
        for chunk_bytes in [1usize, 1 << 20] {
            let all = concat(&merged(runs(), &spec(), chunk_bytes));
            assert_eq!(all.num_rows(), 6, "{chunk_bytes}");
            assert!(is_sorted(&all, &spec()).expect("check"), "{chunk_bytes}");
            assert_eq!(
                keys_of(&all),
                vec![Some(1), Some(7), Some(7), Some(7), Some(7), Some(9)],
                "{chunk_bytes}"
            );
            assert_eq!(
                tags_of(&all),
                strings(&["r0k1", "r0k7a", "r0k7b", "r1k7", "r2k7", "r1k9"]),
                "{chunk_bytes}"
            );
        }
    }

    /// Scenario: three runs merged with a one-row and then a three-row chunk budget.
    /// Guarantees: chunks fill to the budget (3/3/2) and every row keeps its payload.
    #[test]
    fn merge_chunks_are_bounded_and_carry_payloads() {
        let runs = || {
            vec![
                sort_batch(
                    &batch_tagged(
                        vec![Some(5), Some(1), Some(9)],
                        vec![0.0; 3],
                        vec!["a5", "a1", "a9"],
                    ),
                    &spec(),
                )
                .expect("s"),
                sort_batch(
                    &batch_tagged(vec![Some(2), Some(8)], vec![0.0; 2], vec!["b2", "b8"]),
                    &spec(),
                )
                .expect("s"),
                sort_batch(
                    &batch_tagged(
                        vec![Some(3), None, Some(4)],
                        vec![0.0; 3],
                        vec!["c3", "cn", "c4"],
                    ),
                    &spec(),
                )
                .expect("s"),
            ]
        };
        let expected_keys = vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(8),
            Some(9),
            None,
        ];
        let expected_tags = strings(&["a1", "b2", "c3", "c4", "a5", "b8", "a9", "cn"]);
        // A budget of three average rows must split the 8-row merge, which is
        // what makes the upper bound below worth asserting.
        let three_rows = avg_row_bytes(&runs()) * 3;
        for (chunk_bytes, expected_chunks) in [(1usize, vec![1; 8]), (three_rows, vec![3, 3, 2])] {
            let out = merged(runs(), &spec(), chunk_bytes);
            assert_eq!(
                out.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
                expected_chunks,
                "{chunk_bytes}"
            );
            let all = concat(&out);
            assert_eq!(keys_of(&all), expected_keys, "{chunk_bytes}");
            assert_eq!(tags_of(&all), expected_tags, "{chunk_bytes}");
        }
    }

    /// Scenario: no runs, and one run without rows.
    /// Guarantees: the merge iterator is empty in both cases.
    #[test]
    fn merge_of_no_runs_yields_no_chunks() {
        assert!(merged(vec![], &spec(), 1 << 20).is_empty());
        assert!(
            merged(vec![batch(vec![], vec![], "x")], &spec(), 1 << 20).is_empty(),
            "a run with no rows contributes nothing"
        );
        assert!(
            merged(vec![], &SortSpec::new(vec![]), 1 << 20).is_empty(),
            "the same holds with sorting disabled"
        );
    }

    /// Scenario: one sorted run under a one-row and a whole-run chunk budget.
    /// Guarantees: its rows come back in order with their payloads.
    #[test]
    fn merge_of_a_single_run_yields_it_unchanged() {
        let r = batch_tagged(
            vec![Some(1), Some(4), Some(9)],
            vec![0.0; 3],
            vec!["a", "b", "c"],
        );
        for chunk_bytes in [1usize, 1 << 20] {
            let all = concat(&merged(vec![r.clone()], &spec(), chunk_bytes));
            assert_eq!(
                keys_of(&all),
                vec![Some(1), Some(4), Some(9)],
                "{chunk_bytes}"
            );
            assert_eq!(tags_of(&all), strings(&["a", "b", "c"]), "{chunk_bytes}");
        }
    }

    /// Scenario: empty runs interleaved with runs that have rows.
    /// Guarantees: the output holds exactly the non-empty runs' rows, globally sorted.
    #[test]
    fn merge_skips_runs_with_no_rows() {
        let runs = vec![
            batch(vec![], vec![], "empty"),
            batch_tagged(vec![Some(2), Some(6)], vec![0.0; 2], vec!["a", "b"]),
            batch(vec![], vec![], "empty"),
            batch_tagged(vec![Some(4)], vec![0.0], vec!["c"]),
        ];
        let all = concat(&merged(runs, &spec(), 1 << 20));
        assert_eq!(keys_of(&all), vec![Some(2), Some(4), Some(6)]);
        assert_eq!(tags_of(&all), strings(&["a", "c", "b"]));
    }

    /// Scenario: runs whose schemas differ are merged.
    /// Guarantees: the merge refuses up front with an invalid-content error.
    #[test]
    fn mismatched_run_schemas_are_refused() {
        let wide = batch(vec![Some(1)], vec![0.0], "a");
        let narrow = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![Some(2)])) as ArrayRef],
        )
        .expect("narrow batch");
        let Err(err) = merge_runs(vec![wide, narrow], &spec(), 1 << 20) else {
            panic!("schema mismatch must be refused");
        };
        assert!(err.to_string().contains("different schemas"));
    }

    /// Scenario: two runs with identical fields and different schema metadata.
    /// Guarantees: they merge and the output carries the first run's metadata.
    #[test]
    fn runs_differing_only_in_schema_metadata_merge() {
        let plain = batch_tagged(vec![Some(2)], vec![0.0], vec!["a"]);
        let with_meta = Arc::new(
            test_schema()
                .with_metadata(HashMap::from([("origin".to_string(), "run2".to_string())])),
        );
        let tagged = RecordBatch::try_new(
            with_meta,
            vec![
                Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef,
                Arc::new(Float64Array::from(vec![0.0])) as ArrayRef,
                Arc::new(StringArray::from(vec!["b"])) as ArrayRef,
            ],
        )
        .expect("metadata batch");
        let out = merged(vec![plain, tagged], &spec(), 1 << 20);
        let all = concat(&out);
        assert_eq!(keys_of(&all), vec![Some(1), Some(2)]);
        assert_eq!(tags_of(&all), strings(&["b", "a"]));
        assert!(
            all.schema().metadata().is_empty(),
            "the first run's schema metadata is the output's"
        );
    }

    /// Scenario: two runs merged by an Int64 and a Float64 key, consumed chunk by chunk.
    /// Guarantees: every row's encoded key and the offsets are reported resident until the last
    /// chunk; unsorted mode reports zero.
    #[test]
    fn merge_reports_the_keys_it_keeps_resident() {
        let r1 = sort_batch(
            &batch(vec![Some(5), Some(1), Some(9)], vec![0.0; 3], "a"),
            &spec(),
        )
        .expect("s");
        let r2 = sort_batch(&batch(vec![Some(2), Some(8)], vec![0.0; 2], "b"), &spec()).expect("s");
        let mut merge = merge_runs(vec![r1.clone(), r2.clone()], &spec(), 1).expect("merge");
        let encoded_rows = 5 * 2 * (1 + 8);
        let offsets = (3 + 1 + 2 + 1) * size_of::<usize>();
        let built = merge.resident_key_bytes();
        assert!(
            built >= encoded_rows + offsets,
            "{built} bytes reported for {encoded_rows} key bytes and {offsets} offset bytes"
        );
        let mut rows = 0;
        while let Some(chunk) = merge.next() {
            rows += chunk.expect("chunk").num_rows();
            assert!(
                merge.resident_key_bytes() >= encoded_rows + offsets,
                "the keys of every run stay resident until the iterator is dropped"
            );
        }
        assert_eq!(rows, 5);
        let unsorted = merge_runs(vec![r1, r2], &SortSpec::new(vec![]), 1).expect("merge");
        assert_eq!(unsorted.resident_key_bytes(), 0);
    }

    /// Scenario: five runs of 1000 rows keyed by a nullable string of 0 to 300 bytes, by an
    /// Int64 with a Boolean and a FixedSizeBinary(16), and by the id then the string, each
    /// merged under the default step budget.
    /// Guarantees: the summed `merge_key_bound` of the runs is never below the keys the merge
    /// reports resident, and for the string key stays within 15 percent of them.
    #[test]
    fn merge_key_bound_covers_what_the_merge_holds() {
        use arrow::array::{BooleanArray, FixedSizeBinaryArray};
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("i", DataType::Int64, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("id", DataType::FixedSizeBinary(16), false),
        ]));
        let run = |r: usize| {
            let rows = 0..1000usize;
            let strings: Vec<Option<String>> = rows
                .clone()
                .map(|i| (i % 17 != 0).then(|| "x".repeat((r * 7 + i * 13) % 301)))
                .collect();
            let columns: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(strings)),
                Arc::new(Int64Array::from_iter_values(rows.clone().map(|i| i as i64))),
                Arc::new(BooleanArray::from_iter(
                    rows.clone().map(|i| Some(i % 2 == 0)),
                )),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(rows.map(|i| [(i % 251) as u8; 16]))
                        .expect("ids"),
                ),
            ];
            RecordBatch::try_new(schema.clone(), columns).expect("batch")
        };
        let key = |column: &str| SortKey {
            column: column.into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        };
        for (keys, tight) in [
            (vec![key("s")], true),
            (vec![key("i"), key("b"), key("id")], false),
            (vec![key("id"), key("s")], false),
        ] {
            let spec = SortSpec::new(keys);
            let runs: Vec<RecordBatch> = (0..5)
                .map(|r| sort_batch(&run(r), &spec).expect("sort"))
                .collect();
            let bound: usize = runs
                .iter()
                .map(|run| merge_key_bound(run, &spec).expect("bound"))
                .sum();
            let resident = MergeBuild::new(runs, &spec, 1 << 20)
                .and_then(MergeBuild::finish)
                .expect("merge")
                .resident_key_bytes();
            assert!(bound >= resident, "{spec:?}: bound {bound} < {resident}");
            if tight {
                assert!(
                    bound * 100 <= resident * 115,
                    "{spec:?}: bound {bound} over 1.15 x {resident}"
                );
            }
        }
    }

    /// Scenario: one-row key columns of every type `merge_key_supported` accepts: Int32, Int64,
    /// Float64, UTC microsecond timestamps, booleans and the 8- and 16-byte ids, each with a
    /// value and a null, and strings of 0, 1, 7, 8, 9, 31, 32, 33, 63, 64, 65 and 300 bytes
    /// and null.
    /// Guarantees: the encoded key size `merge_key_bound` computes is exactly the row length
    /// Arrow's row converter produces, so any change of the row format fails here.
    #[test]
    fn merge_key_bound_matches_the_row_format_exactly() {
        use arrow::array::{
            BooleanArray, FixedSizeBinaryArray, Int32Array, TimestampMicrosecondArray,
        };
        let lengths = [0usize, 1, 7, 8, 9, 31, 32, 33, 63, 64, 65, 300];
        let text: Vec<Option<String>> = lengths
            .iter()
            .map(|&n| Some("x".repeat(n)))
            .chain([None])
            .collect();
        let id = |width: i32| -> ArrayRef {
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![Some(vec![1u8; width as usize]), None].into_iter(),
                    width,
                )
                .expect("ids"),
            )
        };
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(1), None])),
            Arc::new(Int64Array::from(vec![Some(1), None])),
            Arc::new(Float64Array::from(vec![Some(1.5), None])),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1), None]).with_timezone("UTC")),
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            id(8),
            id(16),
            Arc::new(StringArray::from(text)),
        ];
        let spec = SortSpec::new(vec![SortKey {
            column: "k".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        }]);
        for column in columns {
            let data_type = column.data_type().clone();
            assert!(merge_key_supported(&data_type), "{data_type}");
            let schema = Arc::new(Schema::new(vec![Field::new("k", data_type.clone(), true)]));
            let converter =
                RowConverter::new(vec![SortField::new(data_type.clone())]).expect("converter");
            for i in 0..column.len() {
                let value = column.slice(i, 1);
                let rows = converter
                    .convert_columns(std::slice::from_ref(&value))
                    .expect("rows");
                let batch = RecordBatch::try_new(schema.clone(), vec![value]).expect("batch");
                let bound = merge_key_bound(&batch, &spec).expect("bound");
                assert_eq!(
                    bound - merge_key_overhead(1),
                    rows.row(0).as_ref().len(),
                    "{data_type}, row {i}"
                );
            }
        }
    }

    /// Scenario: every sortable column of every values dataset, with one denormalized column of
    /// each type, and types no lake column has: Int16, Float32, Decimal128, Time32, a large
    /// string, a binary and a 4-byte fixed-size binary.
    /// Guarantees: `merge_key_supported` accepts exactly the lake's sortable types, so every
    /// accepted type has a conformance case; the others are refused and `merge_key_bound`
    /// errs on them.
    #[test]
    fn merge_key_supported_is_exactly_the_lake_sort_key_types() {
        use crate::config::{DenormType, Denormalize};
        let mut cfg = LakeConfig::default();
        let denormalize: Vec<Denormalize> = [
            DenormType::String,
            DenormType::Int64,
            DenormType::Double,
            DenormType::Bool,
        ]
        .into_iter()
        .enumerate()
        .map(|(i, ty)| Denormalize {
            path: format!("resource.k{i}"),
            column: format!("k{i}"),
            ty,
        })
        .collect();
        cfg.logs.denormalize = denormalize.clone();
        cfg.metrics.denormalize = denormalize;
        for ds in Dataset::ALL.into_iter().filter(|ds| !ds.is_series()) {
            let schema = crate::schema::dataset_schema(ds, &cfg);
            for field in schema.fields() {
                let sortable =
                    RowConverter::supports_fields(&[SortField::new(field.data_type().clone())])
                        && !matches!(field.data_type(), DataType::List(_));
                assert_eq!(
                    merge_key_supported(field.data_type()),
                    sortable,
                    "{}.{}: {}",
                    ds.name(),
                    field.name(),
                    field.data_type()
                );
            }
        }
        for data_type in [
            DataType::Int16,
            DataType::Float32,
            DataType::Decimal128(38, 2),
            DataType::Time32(arrow::datatypes::TimeUnit::Second),
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::FixedSizeBinary(4),
        ] {
            assert!(!merge_key_supported(&data_type), "{data_type}");
        }
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int16, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int16Array::from(vec![1_i16])) as ArrayRef],
        )
        .expect("batch");
        let spec = SortSpec::new(vec![SortKey {
            column: "k".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        }]);
        assert!(merge_key_bound(&batch, &spec).is_err());
    }

    /// Scenario: a dictionary-encoded string column named as a sort key.
    /// Guarantees: `merge_key_supported` refuses the type and `merge_key_bound` errs rather
    /// than bound it by guesswork.
    #[test]
    fn a_dictionary_sort_key_is_refused() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;
        let dictionary: DictionaryArray<Int32Type> = vec!["a", "b", "a"].into_iter().collect();
        let data_type = dictionary.data_type().clone();
        assert!(!merge_key_supported(&data_type));
        let schema = Arc::new(Schema::new(vec![Field::new("k", data_type, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(dictionary) as ArrayRef]).expect("batch");
        let spec = SortSpec::new(vec![SortKey {
            column: "k".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        }]);
        assert!(merge_key_bound(&batch, &spec).is_err());
    }

    /// Scenario: string keys growing from one byte to hundreds as the merge advances.
    /// Guarantees: the resident figure taken at build time never falls below what the merge holds.
    #[test]
    fn resident_key_bytes_bound_keys_that_grow_during_the_merge() {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
        let run = |width_step: usize, tag: char| {
            let values: Vec<String> = (0..20)
                .map(|i| format!("{:03}{}", i, tag.to_string().repeat(1 + i * width_step)))
                .collect();
            let array: ArrayRef = Arc::new(StringArray::from(values));
            RecordBatch::try_new(schema.clone(), vec![array]).expect("batch")
        };
        let spec = SortSpec::new(vec![SortKey {
            column: "k".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        }]);
        let mut merge = merge_runs(vec![run(20, 'a'), run(30, 'b')], &spec, 1).expect("merge");
        let reported = merge.resident_key_bytes();
        let first = merge.resident_key_bytes_now();
        while let Some(chunk) = merge.next() {
            let _ = chunk.expect("chunk");
            let now = merge.resident_key_bytes_now();
            assert!(
                now <= reported,
                "{now} resident bytes above the reported {reported}"
            );
            assert_eq!(now, first, "the heap holds no copy of a key");
            assert_eq!(
                merge.resident_key_bytes(),
                reported,
                "the bound does not move"
            );
        }
    }

    /// Scenario: an empty spec (sorting disabled).
    /// Guarantees: merge concatenates runs in arrival order.
    #[test]
    fn empty_spec_concatenates() {
        let r1 = batch(vec![Some(5)], vec![0.0], "a");
        let r2 = batch(vec![Some(1)], vec![0.0], "b");
        let out: Vec<RecordBatch> = merge_runs(vec![r1, r2], &SortSpec::new(vec![]), 1 << 20)
            .expect("merge")
            .collect::<Result<_>>()
            .expect("chunks");
        assert_eq!(
            out.len(),
            2,
            "unsorted mode yields the runs as they are, without concatenating"
        );
        let all = arrow::compute::concat_batches(&out[0].schema(), &out).expect("concat");
        assert_eq!(all.column(0).as_primitive::<Int64Type>().values(), &[5, 1]);
        assert_eq!(SortSpec::new(vec![]).metadata_string(), "none");
        assert_eq!(
            spec().metadata_string(),
            "k:asc:nulls_last,f:asc:nulls_last"
        );
    }

    /// Scenario: a batch with every key tied, and one out of order.
    /// Guarantees: `is_sorted` accepts the first and rejects the second.
    #[test]
    fn is_sorted_accepts_tied_keys() {
        let tied = batch(vec![Some(7); 6], vec![1.0; 6], "x");
        assert!(is_sorted(&tied, &spec()).expect("tied"));
        let descending = batch(vec![Some(3), Some(2), Some(1)], vec![0.0; 3], "x");
        assert!(!is_sorted(&descending, &spec()).expect("descending"));
        let ordered = sort_batch(&descending, &spec()).expect("sort");
        assert!(is_sorted(&ordered, &spec()).expect("ordered"));
    }

    /// Scenario: a one-row batch sorted by a misspelled column.
    /// Guarantees: `sort_batch` and `is_sorted` name the missing column; existing keys pass.
    #[test]
    fn missing_sort_column_is_refused_even_on_short_batches() {
        let one = batch(vec![Some(1)], vec![0.0], "x");
        let bad = SortSpec::new(vec![SortKey {
            column: "kk".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        }]);
        let Err(err) = sort_batch(&one, &bad) else {
            panic!("sort_batch must refuse a missing sort column");
        };
        assert!(err.to_string().contains("sort column kk missing"));
        let Err(err) = is_sorted(&one, &bad) else {
            panic!("is_sorted must refuse a missing sort column");
        };
        assert!(err.to_string().contains("sort column kk missing"));
        assert_eq!(
            keys_of(&sort_batch(&one, &spec()).expect("sort")),
            vec![Some(1)]
        );
        assert!(is_sorted(&one, &spec()).expect("one row"));
    }

    /// Runs keyed by an Int64 `k` with ties and nulls and a Utf8 `s` of
    /// widths from 1 to about 90 bytes, each carrying a distinct tag, each
    /// sorted by `k` ascending, nulls last, then `s` descending.
    fn tie_heavy_runs() -> (Vec<RecordBatch>, SortSpec) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, false),
            Field::new("tag", DataType::Utf8, false),
        ]));
        let spec = SortSpec::new(vec![
            SortKey {
                column: "k".into(),
                order: SortOrder::Asc,
                nulls: Nulls::Last,
            },
            SortKey {
                column: "s".into(),
                order: SortOrder::Desc,
                nulls: Nulls::Last,
            },
        ]);
        let runs = (0..4)
            .map(|run| {
                let n = 37 + run * 11;
                let k: Vec<Option<i64>> = (0..n)
                    .map(|i| (i % 9 != 4).then_some(((i * 7 + run * 3) % 5) as i64))
                    .collect();
                let s: Vec<String> = (0..n)
                    .map(|i| "x".repeat(1 + (i * 13 + run) % 90) + &format!("{}", i % 3))
                    .collect();
                let tag: Vec<String> = (0..n).map(|i| format!("r{run}i{i}")).collect();
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(k)) as ArrayRef,
                        Arc::new(StringArray::from(s)) as ArrayRef,
                        Arc::new(StringArray::from(tag)) as ArrayRef,
                    ],
                )
                .expect("batch");
                sort_batch(&batch, &spec).expect("sort")
            })
            .collect();
        (runs, spec)
    }

    /// The tags of every run's rows in the order a stable sort of the runs'
    /// concatenation, in run order, by the keys of `tie_heavy_runs` gives.
    fn stable_sort_tags(runs: &[RecordBatch]) -> Vec<String> {
        let mut rows: Vec<(Option<i64>, String, String)> = Vec::new();
        for run in runs {
            let k = run.column(0).as_primitive::<Int64Type>();
            let s = run.column(1).as_string::<i32>();
            let t = run.column(2).as_string::<i32>();
            for i in 0..run.num_rows() {
                rows.push((
                    k.is_valid(i).then(|| k.value(i)),
                    s.value(i).to_string(),
                    t.value(i).to_string(),
                ));
            }
        }
        rows.sort_by(|a, b| {
            let k = match (a.0, b.0) {
                (Some(x), Some(y)) => x.cmp(&y),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            };
            k.then_with(|| b.1.cmp(&a.1))
        });
        rows.into_iter().map(|(_, _, tag)| tag).collect()
    }

    /// Scenario: tie-heavy runs with string keys merged with the default budget and with budgets
    /// down to one row and one key byte per step.
    /// Guarantees: every slicing gives the unsliced merge's chunks, which equal a stable sort in
    /// run order.
    #[test]
    fn sliced_merge_matches_the_unsliced_merge_and_a_stable_sort() {
        let (runs, spec) = tie_heavy_runs();
        let chunk_bytes = avg_row_bytes(&runs) * 23;
        let unsliced = merged(runs.clone(), &spec, chunk_bytes);
        let expected = stable_sort_tags(&runs);
        assert_eq!(tags_of(&concat(&unsliced)), expected);
        assert!(unsliced.len() > 3, "{} chunks", unsliced.len());
        for (rows, key_bytes) in [(1, 1), (2, 7), (5, 64), (16, 1 << 20), (1 << 20, 1 << 30)] {
            let mut build = MergeBuild::new(runs.clone(), &spec, chunk_bytes)
                .expect("build")
                .with_budget(rows, key_bytes);
            let mut build_steps = 0usize;
            let mut resident = 0usize;
            while !build.step().expect("key slice") {
                build_steps += 1;
                assert!(build.resident_key_bytes() >= resident, "keys only grow");
                resident = build.resident_key_bytes();
            }
            let mut merge = build.finish().expect("finish").with_budget(rows, key_bytes);
            let (chunks, steps) = drain(&mut merge);
            assert_eq!(
                chunks.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
                unsliced
                    .iter()
                    .map(RecordBatch::num_rows)
                    .collect::<Vec<_>>(),
                "chunk boundaries with {rows} rows / {key_bytes} bytes per step"
            );
            for (got, want) in chunks.iter().zip(&unsliced) {
                assert_eq!(
                    got, want,
                    "chunk content with {rows} rows / {key_bytes} bytes"
                );
            }
            if rows <= 16 {
                assert!(build_steps > runs.len(), "{build_steps} key slices");
                assert!(steps > chunks.len() * 2, "{steps} merge steps");
            }
        }
    }

    /// Scenario: tie-heavy and nested runs sorted as several batches, as one, and with an empty
    /// spec.
    /// Guarantees: several batches sort exactly as their concatenation; an empty spec concatenates.
    #[test]
    fn sorting_batches_matches_sorting_their_concatenation() {
        for (runs, spec) in [tie_heavy_runs(), nested_runs()] {
            let all = concat(&runs);
            assert_eq!(
                sort_batches(&runs, &spec).expect("batches"),
                sort_batch(&all, &spec).expect("concatenation")
            );
            assert_eq!(
                sort_batches(&runs[..1], &spec).expect("one batch"),
                sort_batch(&runs[0], &spec).expect("one")
            );
            assert_eq!(
                sort_batches(&runs, &SortSpec::new(vec![])).expect("unsorted"),
                all
            );
        }
    }

    /// Runs keyed by an Int64 `k` with ties, each row carrying a map of 0 to
    /// 5 entries with a null value now and then, a list of 0 to 4 items, a
    /// nullable string and a tag, every run sorted by `k`.
    fn nested_runs() -> (Vec<RecordBatch>, SortSpec) {
        use arrow::array::{Int64Builder, ListBuilder, MapBuilder, StringBuilder};
        let spec = SortSpec::new(vec![SortKey {
            column: "k".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        }]);
        let runs = (0..3)
            .map(|run| {
                let n = 40 + run * 13;
                let mut k = Int64Builder::new();
                let mut m = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
                let mut l = ListBuilder::new(Int64Builder::new());
                let mut v = StringBuilder::new();
                let mut tag = StringBuilder::new();
                for i in 0..n {
                    k.append_value(((i * 7 + run * 5) % 11) as i64);
                    for e in 0..(i + run) % 6 {
                        m.keys().append_value(format!("key{e}"));
                        if (i + e) % 4 == 0 {
                            m.values().append_null();
                        } else {
                            m.values().append_value("v".repeat(1 + (i * e) % 9));
                        }
                    }
                    m.append(true).expect("map row");
                    for item in 0..(i * 3 + run) % 5 {
                        l.values().append_value((i * 10 + item) as i64);
                    }
                    l.append(true);
                    if i % 5 == 3 {
                        v.append_null();
                    } else {
                        v.append_value("s".repeat(i % 13));
                    }
                    tag.append_value(format!("r{run}i{i}"));
                }
                let columns: Vec<ArrayRef> = vec![
                    Arc::new(k.finish()),
                    Arc::new(m.finish()),
                    Arc::new(l.finish()),
                    Arc::new(v.finish()),
                    Arc::new(tag.finish()),
                ];
                let schema = Arc::new(Schema::new(
                    columns
                        .iter()
                        .zip(["k", "m", "l", "v", "tag"])
                        .map(|(c, name)| Field::new(name, c.data_type().clone(), true))
                        .collect::<Vec<_>>(),
                ));
                let batch = RecordBatch::try_new(schema, columns).expect("batch");
                sort_batch(&batch, &spec).expect("sort")
            })
            .collect();
        (runs, spec)
    }

    /// The runs' rows in the merge's order, as a stable sort of their
    /// concatenation by `k` gives them: ties keep run order, then row order.
    fn stable_merge_of(runs: &[RecordBatch]) -> RecordBatch {
        let all = arrow::compute::concat_batches(&runs[0].schema(), runs).expect("concat");
        let k = all.column(0).as_primitive::<Int64Type>();
        let mut order: Vec<u32> = (0..all.num_rows() as u32).collect();
        order.sort_by_key(|&i| k.value(i as usize));
        let indices = UInt32Array::from(order);
        let columns = all
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).expect("take"))
            .collect();
        RecordBatch::try_new(all.schema(), columns).expect("batch")
    }

    /// Scenario: runs with map, list and string columns merged eight elements per step.
    /// Guarantees: no step copies more than its budget, finishing included, every element is copied
    /// once, and the chunk equals a stable sort.
    #[test]
    fn one_merge_step_does_bounded_work() {
        let (runs, spec) = nested_runs();
        let rows: usize = runs.iter().map(RecordBatch::num_rows).sum();
        let expected = stable_merge_of(&runs);
        let children: usize = [1usize, 2]
            .iter()
            .map(|&c| {
                let column = expected.column(c);
                column
                    .as_list_opt::<i32>()
                    .map(|l| l.values().len())
                    .or_else(|| column.as_map_opt().map(|m| m.entries().len()))
                    .unwrap_or(0)
            })
            .sum();
        let mut build = MergeBuild::new(runs.clone(), &spec, 1 << 30)
            .expect("build")
            .with_budget(8, 1 << 20);
        let mut encoded = 0usize;
        while !build.step().expect("slice") {
            let now = build
                .keys
                .iter()
                .flatten()
                .map(Rows::num_rows)
                .sum::<usize>();
            assert!(
                now - encoded <= 8,
                "{} rows in one key slice",
                now - encoded
            );
            encoded = now;
        }
        let mut merge = build.finish().expect("finish").with_budget(8, 1 << 20);
        let mut chunks = Vec::new();
        let mut copied = 0usize;
        loop {
            match merge.step() {
                MergeStep::Progress => assert!(merge.last_step_rows <= 8),
                MergeStep::Ready => {
                    assert!(merge.last_step_rows <= 8);
                    let mut builder = merge.chunk_builder();
                    loop {
                        let before = builder.copied_elements();
                        let done = builder.step().expect("build step");
                        let step = builder.copied_elements() - before;
                        assert!(step <= 8, "{step} elements copied in one step");
                        copied += step;
                        if done {
                            break;
                        }
                    }
                    chunks.push(builder.finish().expect("chunk"));
                    merge.chunk_taken();
                }
                MergeStep::Chunk(_) => panic!("a sorted merge builds its chunks"),
                MergeStep::Done => break,
            }
        }
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], expected);
        assert_eq!(
            copied,
            rows * expected.num_columns() + children,
            "every row, item and entry copied exactly once"
        );
    }

    /// Every buffer capacity an array's data holds, children and validity
    /// included.
    fn capacity_of(data: &ArrayData) -> usize {
        data.buffers().iter().map(|b| b.capacity()).sum::<usize>()
            + data.nulls().map_or(0, |n| n.buffer().capacity())
            + data.child_data().iter().map(capacity_of).sum::<usize>()
    }

    /// Scenario: the nested runs' chunk built eight elements per step, the charge read throughout.
    /// Guarantees: the charge equals the capacities the column's buffers really have.
    #[test]
    fn the_builder_charges_what_its_buffers_allocate() {
        let (runs, spec) = nested_runs();
        let mut merge = merge_runs(runs, &spec, 1 << 30)
            .expect("merge")
            .with_budget(8, 1 << 20);
        while !matches!(merge.step(), MergeStep::Ready) {}
        let mut builder = merge.chunk_builder();
        let mut charged: Vec<Option<usize>> = Vec::new();
        loop {
            let done = builder.step().expect("build step");
            while charged.len() < builder.columns.len() {
                charged.push(None);
            }
            if let Some(charge) = builder.current_charge() {
                let c = builder.columns.len();
                if charged.len() <= c {
                    charged.resize(c + 1, None);
                }
                charged[c] = Some(charge);
            }
            if done {
                break;
            }
        }
        let mut checked = 0;
        for (c, column) in builder.columns.iter().enumerate() {
            let Some(charge) = charged.get(c).copied().flatten() else {
                continue;
            };
            let actual = capacity_of(&column.to_data());
            assert_eq!(
                charge, actual,
                "column {c}: charged {charge}, allocated {actual}"
            );
            checked += 1;
        }
        assert!(
            checked >= 4,
            "{checked} columns were charged while being built"
        );
    }

    /// Scenario: a sorted merge of runs with a dictionary column, a type no lake dataset has.
    /// Guarantees: the merge is refused before any key is encoded, naming the column, and the same
    /// runs still pass through an unsorted merge.
    #[test]
    fn a_column_type_without_a_bounded_build_is_refused() {
        use arrow::array::{DictionaryArray, Int8Array, StringArray};
        use arrow::datatypes::Int8Type;
        let spec = SortSpec::new(vec![SortKey {
            column: "k".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        }]);
        let run = |keys: Vec<i64>, values: Vec<String>| {
            let n = values.len();
            let dictionary = DictionaryArray::<Int8Type>::try_new(
                Int8Array::from((0..n as i8).collect::<Vec<_>>()),
                Arc::new(StringArray::from(values)),
            )
            .expect("dictionary");
            let columns: Vec<ArrayRef> =
                vec![Arc::new(Int64Array::from(keys)), Arc::new(dictionary)];
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("d", columns[1].data_type().clone(), false),
            ]));
            RecordBatch::try_new(schema, columns).expect("batch")
        };
        let runs = || {
            vec![
                run(vec![1, 3], vec!["one".into(), "three".into()]),
                run(vec![2, 4], vec!["two".into(), "four".into()]),
            ]
        };
        let error = MergeBuild::new(runs(), &spec, 1 << 20)
            .err()
            .expect("a dictionary column is refused");
        assert!(error.to_string().contains("\"d\""), "{error}");
        let unsorted = merged(runs(), &SortSpec::new(vec![]), 1 << 20);
        assert_eq!(concat(&unsorted).num_rows(), 4);
    }

    /// Scenario: a string column past an offset limit lowered to 16 bytes.
    /// Guarantees: the builder refuses it with an overflow error before copying.
    #[test]
    fn an_offset_overflow_is_an_error_not_a_panic() {
        let (runs, spec) = tie_heavy_runs();
        let mut merge = merge_runs(runs, &spec, 1 << 30)
            .expect("merge")
            .with_offset_limit(16);
        while !matches!(merge.step(), MergeStep::Ready) {}
        let mut builder = merge.chunk_builder();
        let err = loop {
            match builder.step() {
                Ok(true) => panic!("the string column fits no 16-byte offset limit"),
                Ok(false) => {}
                Err(err) => break err,
            }
        };
        assert!(
            err.to_string().to_lowercase().contains("offset"),
            "unexpected error: {err}"
        );
    }

    /// Scenario: the tie-heavy runs' keys encoded two rows per step.
    /// Guarantees: resident key bytes and the heap are kept incrementally and equal a full recount.
    #[test]
    fn the_build_keeps_its_totals_as_it_goes() {
        let (runs, spec) = tie_heavy_runs();
        let mut build = MergeBuild::new(runs.clone(), &spec, 1 << 20)
            .expect("build")
            .with_budget(2, 1 << 20);
        loop {
            let done = build.step().expect("slice");
            let segments: Vec<&Rows> = build.keys.iter().flatten().collect();
            assert_eq!(
                build.resident_key_bytes(),
                segments.iter().map(|rows| rows.size()).sum::<usize>()
                    + build.heap.capacity() * size_of::<HeapItem>(),
                "segments and the heap"
            );
            assert_eq!(
                build.heap.len(),
                build.keys.iter().filter(|run| !run.is_empty()).count(),
                "every run whose first row is encoded is in the heap"
            );
            if done {
                break;
            }
        }
        let expected = merged(runs.clone(), &spec, 1 << 20);
        let merge = build.finish().expect("finish");
        assert_eq!(merge.heap.len(), runs.len());
        let got: Vec<RecordBatch> = merge.collect::<Result<_>>().expect("chunks");
        assert_eq!(got, expected);
    }

    /// Scenario: 100 one-row runs merged with the default budget.
    /// Guarantees: keys, pops and the build each take one step.
    #[test]
    fn a_small_merge_takes_one_step_per_phase() {
        let runs: Vec<RecordBatch> = (0..100)
            .map(|i| batch_tagged(vec![Some(100 - i)], vec![0.0], vec!["t"]))
            .collect();
        let mut build = MergeBuild::new(runs, &spec(), 1 << 20).expect("build");
        assert!(build.step().expect("keys"), "every key in one step");
        let mut merge = build.finish().expect("finish");
        assert!(
            matches!(merge.step(), MergeStep::Ready),
            "every row popped in one step"
        );
        let mut builder = merge.chunk_builder();
        assert!(builder.step().expect("build"), "every column in one step");
        let chunk = builder.finish().expect("chunk");
        merge.chunk_taken();
        assert_eq!(chunk.num_rows(), 100);
        assert_eq!(keys_of(&chunk)[0], Some(1));
        assert!(matches!(merge.step(), MergeStep::Done));
    }

    /// Scenario: every column of the four datasets, with a denormalized column of every type.
    /// Guarantees: each has a bounded build, so no lake merge is refused for its column types.
    #[test]
    fn every_lake_column_is_built_in_bounded_steps() {
        use crate::config::{DenormType, Denormalize, LakeConfig};
        use crate::schema::{Dataset, dataset_schema};
        let mut cfg = LakeConfig::default();
        for (i, ty) in [
            DenormType::String,
            DenormType::Int64,
            DenormType::Double,
            DenormType::Bool,
        ]
        .into_iter()
        .enumerate()
        {
            let denormalize = Denormalize {
                path: format!("resource.attr{i}"),
                column: format!("denorm_{i}"),
                ty,
            };
            cfg.logs.denormalize.push(denormalize.clone());
            cfg.metrics.denormalize.push(denormalize);
        }
        for dataset in [
            Dataset::LogsSeries,
            Dataset::LogsValues,
            Dataset::MetricsSeries,
            Dataset::MetricsValues,
        ] {
            for field in dataset_schema(dataset, &cfg).fields() {
                assert!(
                    column_kind(field.data_type()).is_some(),
                    "{dataset:?}.{}",
                    field.name()
                );
            }
        }
    }
}
