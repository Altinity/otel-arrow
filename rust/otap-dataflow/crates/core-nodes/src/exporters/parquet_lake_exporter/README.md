# Parquet Lake Exporter

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `exporter:parquet_lake` (`urn:otel:exporter:parquet_lake`)
- Feature gate: `parquet`; the S3 and Azure backends need the `aws` / `azure` features
- Stability: Experimental
- Signals: logs, metrics (traces are refused)

## Overview

The Parquet lake exporter writes each signal as two Parquet datasets on object
storage:

- `series`: one row per distinct identity per block. For logs the identity is
  the resource and scope; for metrics it also includes the metric name, unit,
  type, temporality, monotonicity and data point attributes. Readers keep the
  latest row (`written_at`) per `series_id`.
- `values`: one row per log record or metric data point, keyed by `series_id`.

Not stored: metric exemplars (and metric metadata, which OTAP does not carry).
Metrics payloads without a univariate metrics table (multivariate metrics) are
refused. See [Data Loss and Durability](#data-loss-and-durability).

Wide, rarely changing attributes (host, env, service, ...) are therefore stored
once per block instead of on every row. `series_id` is the XXH3-128 hash of a
canonical encoding of the identity. It is the same across writers and
languages. [docs/FORMAT.md](docs/FORMAT.md) specifies the encoding, layout,
schemas and compatibility rules, with golden vectors.

## Getting Started

```yaml
parquet:
  type: exporter:parquet_lake
  config:
    storage:
      file:
        base_uri: "/tmp/otap-parquet-lake"
    max_block_bytes: 67108864
    max_block_age: 60s
    upload_deadline: 60s
```

A complete pipeline (OTLP and OTAP receivers, batching, this exporter) is in
[configs/otlp-otap-parquet-lake.yaml](../../../../../configs/otlp-otap-parquet-lake.yaml).

## Configuration

| Field | Default | Meaning |
| --- | --- | --- |
| `storage` | required | Object-store backend: `file`, `s3` or `azure` (same as `exporter:parquet`). |
| `retry` | none | Per-request object-store retry policy. |
| `max_block_bytes` | 64 MiB | Flush a signal's block before its buffered Arrow data would exceed this. Range 1 MiB to 1 GiB. |
| `max_block_age` | 60s | Flush a non-empty block once it is this old. |
| `check_interval` | 1s | Timer interval for block age checks (at most `max_block_age`). |
| `upload_deadline` | 60s | Give up on a block (Nack its batches) when it has not landed within this time. |
| `retry_initial_backoff` | 200ms | First retry backoff of a block upload; doubles per attempt. |
| `retry_max_backoff` | 10s | Largest retry backoff. |

## Delivery Semantics

- **Ack after land.** A batch is acknowledged only after every block holding
  its rows has landed, meaning both of the block's objects were written.
  Receivers must use `wait_for_result: true`, with a timeout above the
  worst-case ack delay: about `max_block_age + check_interval +
  2 x upload_deadline` plus encode time (a size-triggered flush can precede a
  tick flush). The exporter logs `parquet_lake.unacked_input` once when a batch
  arrives that nobody upstream waits on.
- **Durable on the file backend.** With `storage: file`, both files and their
  directories are fsynced before the batch is acknowledged; an fsync failure
  counts as a failed upload.
- **Idempotent retries.** A block is encoded once, and its object names are
  fixed before the first attempt. A failed upload is retried with backoff and
  rewrites the same names with the same bytes. After an ambiguous failure,
  `probe_block` checks whether the block landed anyway.
- **Deadline.** When a block has not landed within `upload_deadline`, each of
  its batches gets one retryable Nack.
- **Shutdown.** Open blocks are flushed within the shutdown deadline. Batches
  whose block could not land in time get a retryable Nack with
  `NodeShutdown`.
- **Refused input.** Traces, metrics without a univariate metrics table
  (multivariate), and malformed payloads (including OTLP bytes that are not a
  valid request) get a permanent Nack with `Refused`. A single bad attribute
  value never refuses a batch: an undecodable or too-deep value is stored as
  empty (see FORMAT.md).
- **At least once.** A crash between landing and acknowledging leads to a
  resend, so a block's rows can be written twice. A batch that is Nacked partway
  through, because a mid-payload flush or push failed, may still have rows that
  land in the next block, so a resend can duplicate them too. An upload whose
  values write lands at the very moment `upload_deadline` expires is reported as
  failed and Nacked, and the resend duplicates its rows.

## Data Loss and Durability

The situations in which data can be lost, and how this exporter handles each:

| Situation | Handling |
| --- | --- |
| Receivers ack before the data lands (`wait_for_result: false`, the receivers' default) | Cannot be enforced here; the exporter warns once (`parquet_lake.unacked_input`). Set `wait_for_result: true`. |
| Malformed OTLP bytes (pdata converts them to an empty batch) | Refused (permanent Nack) instead of acknowledged. A valid empty request is still acknowledged. |
| Power loss after an ack on the `file` backend | Files and directories are fsynced before the ack. |
| Storage outage longer than the producers' retry window, or producer queues filling during slow uploads | No local disk buffer: producers must retry and queue. Size their retry time above the expected outage and their timeout above the worst-case ack delay. Use `durable_buffer` upstream only if losing ack-after-land is acceptable. |
| Shutdown during a flush | A timer tick flushes at most one block, so a Shutdown waits for at most one upload (`upload_deadline`). Keep `upload_deadline` below the engine's shutdown deadline. |
| Deterministic encode or push failure | Retryable Nack for the block's batches: batches that only shared the block succeed on resend; the failing batch keeps failing until the producer gives up (logged as `parquet_lake.block.failed`). A permanent Nack would also drop the innocent batches. |
| Traces or multivariate metrics sent to this exporter | Refused (permanent). Route them elsewhere. |
| Undecodable or deeply nested (> 128) attribute values | Stored as empty values; the rest of the batch is written. |
| Duplicate attribute keys (invalid OTLP) | One value per key is kept (the one with the smallest encoded value), in both the identity and the maps. |
| Attribute and body values rendered as text | Types are flattened (int 7 and "7" look alike; bytes are hex; NaN/Infinity in maps and arrays are the strings `"NaN"`/`"Infinity"`). The series identity keeps the types. |
| Resource/scope `schema_url` and `dropped_attributes_count` | Stored in series columns. Exemplars are not stored; metric metadata is not carried by OTAP. |
| Metric description changes | Not part of the identity: one description per block, readers keep the latest. |
| Data points without a matching metric row (malformed input) | Stored under an empty metric identity and counted in `points.orphaned`. |
| Queries that prune on `dt=` | `dt` is the block's write date, not the event date; filter on `time_unix_nano` for event time and widen the `dt` range for late data. |
| Retention deleting series files before values files | Values can no longer be joined: keep series files at least as long as values files. |

`probe_block(store, &block_id)` is public. It reports whether a block landed,
and a `BlockId` can be parsed from its `Display` form
(`<signal>:<date>:<writer>:<seq>`).

## Memory

Buffered memory is bounded by `LakeConfig::worst_case_flush_bytes()`:

```text
2 x B                  both open signal blocks (the flushing one is held until encoded)
+ B + B/8              encoded bytes of the flushing block's two files
+ 2 x 1 MiB            unfilled last allocation of each output file
+ 16 MiB               encoder buffers (8 MiB row-group cap plus one slice)
```

`B` is `max_block_bytes`; the default of 64 MiB gives 218 MiB per exporter
instance (per core). A payload is split into chunks of at most `B/4` before it
enters a block, and a chunk that does not fit triggers a flush first. The
payload being processed (the input batch and the chunks extracted from it) is
not included: its size is bounded by the upstream batch size. Also outside the
formula: a single row larger than `B/4` forms a chunk on its own (a block can
then exceed `B` by that row), the per-block set of seen series ids (16 bytes
per distinct series), and the `B/8` encoded-size margin, which is verified with
incompressible data in tests rather than enforced at runtime (`encoder.peak`
and `bytes.written` show real values).

## Querying

Join values to the latest series row per `series_id`. In DuckDB:

```sql
WITH series AS (
  SELECT * FROM read_parquet('/tmp/otap-parquet-lake/logs/series/*/*.parquet')
  QUALIFY row_number() OVER (PARTITION BY series_id ORDER BY written_at DESC) = 1
)
SELECT s.resource_attributes, v.time_unix_nano, v.body
FROM read_parquet('/tmp/otap-parquet-lake/logs/values/*/*.parquet') v
JOIN series s USING (series_id);
```

ClickHouse (`s3()` / `file()`) and Spark read the same layout; `dt=` is a Hive
partition column.

## Telemetry

### Metric Sets

#### `exporter.exports`

The standard exporter outcome measurements (success and failure per signal).

#### `exporter.parquet_lake`

| Metric | Unit | Meaning |
| --- | --- | --- |
| `blocks.landed` | `{block}` | Blocks whose two objects were written |
| `blocks.failed` | `{block}` | Blocks given up (encode failure, upload deadline, shutdown) |
| `upload.retries` | `{retry}` | Upload retries of landed blocks |
| `bytes.written` | `By` | Bytes of landed files |
| `rows.written` | `{row}` | Values rows in landed blocks |
| `series_rows.written` | `{row}` | Series rows in landed blocks |
| `flush.duration` | `s` | Encode plus upload time per block |
| `encoder.peak` | `By` | Peak encoder memory per encoded block |
| `points.orphaned` | `{point}` | Data points without a matching metric row (stored under an empty identity) |
| `batches.rejected` | `{batch}` | Batches refused (traces, multivariate metrics, malformed input) |

### Events

- `parquet_lake.start` (info): writer id, `max_block_bytes`, `worst_case_flush_bytes`.
- `parquet_lake.block.failed` (warn): a block did not land; its batches are Nacked.
- `parquet_lake.unacked_input` (warn, once): a batch arrived without Ack/Nack subscribers, so upstream acknowledged it before it landed.

## Limits

- Uploads are serial. While a block is encoded and uploaded (up to
  `upload_deadline`), the node does not read new input, and backpressure
  reaches the receivers.
- A Shutdown that arrives while a flush runs is handled after that flush
  finishes (at most `upload_deadline`, since a tick flushes one block). Keep
  `upload_deadline` below the engine's shutdown deadline.
- Encoding runs on the core thread in slices of about 1 MiB, with a yield after
  each slice.
- Series rows are deduplicated within a block only, so a long-lived identity
  gets one series row per block.
- `series_id` uses XXH3-128, which is not collision-resistant against
  adversarial input.

## Related Docs

- [docs/FORMAT.md](docs/FORMAT.md)
- [exporter:parquet](../parquet_exporter/README.md)
