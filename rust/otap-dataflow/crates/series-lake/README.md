# series-lake

Engine-independent core of the series Parquet exporter: canonical series
identity, extraction of `series` and values datasets from OTAP Arrow
records, a bounded series cache, sorted block buffers and a Parquet sink
over `object_store`.

The storage format is specified in [docs/FORMAT.md](docs/FORMAT.md).

This crate is engine-independent: it never depends on the Dataflow engine.
The `core-nodes` exporter `exporter:series_parquet` adapts it into a Dataflow
pipeline; see its
[README](../core-nodes/src/exporters/series_parquet_exporter/README.md).

## Modules

- `canonical`: identity encoding v1 and `series_id`.
- `extract`: OTAP records to descriptors and values batches.
- `cache`: bounded LRU of series ids to their last committed partition.
- `buffer`: sorted run buffers and the block accounting model.
- `sort`: sort keys, double normalization, k-way merge.
- `sink`: Parquet files on an `object_store`, Hive layout, frozen names.
- `clock`: wall clock trait and aligned window boundaries.

## Producer id contract

Every writer that shares a lake must set `producer_id_attribute` to the same
resource attribute, and every producer must send that attribute on every
request with a value that is stable for the producer and unique to it. It
stays part of the series identity and is also projected into the
`producer_id` column of every values row, so a reader can tell which producer
a row came from without joining the `series` dataset. Two producers sending
the same value are one producer to readers, and a request whose resource
lacks the attribute gets an empty `producer_id`.

`writer_id` is a different thing: it identifies the writer process in file
names and file metadata and is never part of the identity.

## Limitations in version 1

Format-level limitations (unsupported points, exemplars, lossy attribute maps,
a zero `sum` read as null, traces) are listed in
[FORMAT.md](docs/FORMAT.md#limitations-of-version-1). The writer adds these:

- A failed or cancelled write aborts its multipart upload within
  `upload.abort_timeout`. After a completion was sent, a HEAD first tells
  whether the object exists: one that exists counts as written unless the
  write was cancelled, and the upload is aborted either way. The object is
  this completion's commit (`FlushReport::probed_commits`) on the block's
  first write attempt whatever the abort answers, since nothing else can have
  written its frozen names, and on a retry when the abort is answered
  `NotFound`. A HEAD that fails otherwise leaves the upload alone and reports
  it as a possible orphan; a completion still in flight may be applied after
  an abort. An abort that fails or times out, and a CreateMultipartUpload that
  fails without a definite answer (`object_store::Error::Generic`, which also
  covers a 5xx), are reported as `TransientError::AbortFailed` naming the
  object key, once per failed write although the client retries the creation
  up to `retry.max_retries` times inside it. A bucket lifecycle rule reclaims
  the leftovers and is required; the upload id is not reported, since
  `MultipartUpload` does not expose it.
- A merge holds the encoded sort keys of every row of the table it is
  merging, so sorting by a wide column such as `body` can hold close to a
  second copy of the table's payload. Extraction charges every values row
  its key's bound (`sort::merge_key_bound`) and the block reserves it, so
  the block and those keys stay within `max_block_bytes`.
- `merge_chunk_bytes` is an approximation from the table's mean row width, so
  a chunk of much wider rows overshoots it. With an empty `values_sort` it is
  ignored, and runs go to the writer as they are, each at most
  `run_target_bytes`.
- Sealing replaces only the `emitted_at` column of the series runs, at most
  eight additional bytes per series row, and keeps the first seal timestamp
  across flush retries. A values dataset finalizes what is still buffered
  into one more run, so the seal transient can reach
  `(V + 1) * run_target_bytes` for V buffered runs.
- One Parquet row group can start several multipart upload parts at once
  whatever `upload.concurrency` says: the object store receives the whole
  encoded row group. The burst is bounded by `parquet.row_group_bytes`.
- Number and histogram points share one `metrics/values` dataset, so each
  kind leaves the other's columns null and their Parquet min/max statistics
  are less selective.

## Reading the data

Every values row has a descriptor in the `series` dataset of the same signal
and the same `date`/`hour` partition (FORMAT.md section 6), so a query reads
the hours it needs from both datasets and joins within each hour. A
descriptor repeats within an hour (eviction, restart, several workers); the
newest one wins, ties broken by file name.

DuckDB, one day of logs:

```sql
INSTALL httpfs; LOAD httpfs;
CREATE VIEW logs_values AS
  SELECT * FROM read_parquet(
    's3://bucket/v=1/signal=logs/dataset=values/date=2026-09-21/*/*.parquet',
    hive_partitioning = true, union_by_name = true);
-- one descriptor per series and hour
CREATE VIEW logs_series AS
  SELECT * EXCLUDE (filename) FROM read_parquet(
    's3://bucket/v=1/signal=logs/dataset=series/date=2026-09-21/*/*.parquet',
    hive_partitioning = true, union_by_name = true, filename = true)
  QUALIFY row_number() OVER (PARTITION BY series_id, date, hour
                             ORDER BY emitted_at DESC, filename DESC) = 1;
SELECT v.time, s.resource_attrs, v.body
FROM logs_values v JOIN logs_series s USING (series_id, date, hour);
```

ClickHouse (`clickhouse-local` or the `file()`/`s3()` table functions) has no
`QUALIFY` and does not synthesize `date` and `hour` from the path, so this
reads one hour and breaks ties on the virtual `_path` column:

```sql
WITH series AS (
    SELECT * FROM (
        SELECT *, row_number() OVER (
            PARTITION BY series_id ORDER BY emitted_at DESC, _path DESC) AS rank
        FROM file('v=1/signal=logs/dataset=series/date=2026-09-21/hour=03/*.parquet',
                  'Parquet')
    ) WHERE rank = 1
)
SELECT v.*, s.resource_attrs
FROM file('v=1/signal=logs/dataset=values/date=2026-09-21/hour=03/*.parquet',
          'Parquet') AS v
INNER JOIN series AS s ON v.series_id = s.series_id;
```

Spark, one day:

```python
from pyspark.sql import Window
from pyspark.sql.functions import col, input_file_name, row_number

day = "s3a://bucket/v=1/signal=logs/dataset={}/date=2026-09-21/"
values = (spark.read.option("mergeSchema", "true")
          .option("basePath", "s3a://bucket/v=1/signal=logs/dataset=values/")
          .parquet(day.format("values")))
series = (spark.read.option("mergeSchema", "true")
          .option("basePath", "s3a://bucket/v=1/signal=logs/dataset=series/")
          .parquet(day.format("series"))
          .withColumn("_file", input_file_name()))
# one descriptor per series and hour
latest = (series.withColumn("rn", row_number().over(
              Window.partitionBy("series_id", "date", "hour")
                    .orderBy(col("emitted_at").desc(), col("_file").desc())))
          .filter("rn = 1").drop("rn", "_file"))
(values.join(latest, ["series_id", "date", "hour"])
       .select("time", "resource_attrs", "body"))
```

Metrics read the same way, against one values dataset: number and histogram
points share `signal=metrics/dataset=values`, and the point kind comes from
`metric_type` in the descriptor that the join already supplies.

```sql
CREATE VIEW metrics_values AS
  SELECT * FROM read_parquet(
    's3://bucket/v=1/signal=metrics/dataset=values/date=2026-09-21/*/*.parquet',
    hive_partitioning = true, union_by_name = true);
CREATE VIEW metrics_series AS
  SELECT * EXCLUDE (filename) FROM read_parquet(
    's3://bucket/v=1/signal=metrics/dataset=series/date=2026-09-21/*/*.parquet',
    hive_partitioning = true, union_by_name = true, filename = true)
  QUALIFY row_number() OVER (PARTITION BY series_id, date, hour
                             ORDER BY emitted_at DESC, filename DESC) = 1;
-- gauges and counters
SELECT v.time, s.metric_name, coalesce(v.value_double, v.value_int) AS value
FROM metrics_values v JOIN metrics_series s USING (series_id, date, hour)
WHERE s.metric_type IN ('gauge', 'sum');
-- histograms
SELECT v.time, s.metric_name, v.count, v.sum, v.bucket_counts, v.explicit_bounds
FROM metrics_values v JOIN metrics_series s USING (series_id, date, hour)
WHERE s.metric_type = 'histogram';
```

The descriptor's `metric_type` is the only supported way to tell the two point
kinds apart. Do not classify a row by which columns are null: a null list and
an empty list are distinguishable in DuckDB but not in ClickHouse, which has
no nullable `Array` and reads a null Parquet list as `[]`.

### Selecting files by event time

`date` and `hour` are the ingest time of a block's window, not event time, and
there is no manifest. A record lands in the hour it arrived in, so a query by
event time lists the hours its records can have arrived in: from the start of
its range to the end of its range plus the producers' delivery delay, which
the format does not bound. Within those hours, each values file's footer
carries `min_time_unix_nano` and `max_time_unix_nano` (FORMAT.md section 5),
and `time_unix_nano` has page statistics, so a reader skips a file or a page
from its footer without reading rows. An hour receives no more writes
`window.interval + 2 * (flush_retry_deadline + upload.abort_timeout)` after
it ends (FORMAT.md, "Partition lateness bound"); a job that summarizes an
hour, or compacts it and keeps the time bounds, waits that long.

```sql
SELECT file_name, decode(value) AS max_time_unix_nano
FROM parquet_kv_metadata(
  's3://bucket/v=1/signal=logs/dataset=values/date=2026-09-21/*/*.parquet')
WHERE decode(key) = 'max_time_unix_nano';
```

### Schema changes

The recipes use `union_by_name` / `mergeSchema` because denormalized columns
may be added over time. To detect an incompatible mix, compare the
`schema_fingerprint` key (16 lowercase hex digits) in each file's Parquet
metadata: files of one dataset with different fingerprints disagree on the
column set, a column's type or nullability, or column order, which can still
be an additive change. Compare the physical schemas before concluding that
two files are incompatible; FORMAT.md section 3 defines what the fingerprint
covers. Files whose `identity_config_hash` differs were written with
different `logs.series_attributes`, so the same log stream has different
`series_id` values in them (FORMAT.md section 5).

```sql
SELECT file_name, decode(value) AS schema_fingerprint
FROM parquet_kv_metadata('s3://bucket/**/*.parquet')
WHERE decode(key) = 'schema_fingerprint';
SELECT file_name, name, type, logical_type
FROM parquet_schema('s3://bucket/**/*.parquet');
```

## Testing

```bash
cd rust/otap-dataflow
cargo test -p otel-arrow-dfe-series-lake
```

The reference-oracle property test in `tests/oracle.rs` is the main
correctness test; `tests/golden.rs` and `tests/golden_roundtrip.rs` check the
identity encoding, CBOR decoding, schema fingerprints, `render_v1` and
`identity_config` against vectors produced by an independent Python
implementation, directly and through the real OTLP-to-OTAP conversion.
`tools/gen_golden.py` writes those vectors from FORMAT.md; CI runs it and
fails when it does not reproduce `tests/golden` byte for byte:

```bash
cd rust/otap-dataflow/crates/series-lake
python3 -m venv /tmp/series-lake-venv
/tmp/series-lake-venv/bin/pip install --require-hashes -r tools/requirements.lock.txt
/tmp/series-lake-venv/bin/python3 tools/gen_golden.py tests/golden
```
