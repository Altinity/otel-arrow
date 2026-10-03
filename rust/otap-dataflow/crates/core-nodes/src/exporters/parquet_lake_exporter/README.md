# Parquet Lake Exporter

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `exporter:parquet_lake` (`urn:otel:exporter:parquet_lake`)
- Feature gate: `parquet`; the S3 and Azure backends need the `aws` / `azure` features
- Stability: Experimental
- Signals: logs, metrics (traces are refused)

## Overview

The Parquet lake exporter writes Series Lake Format version 1, the format of
`exporter:series_parquet`. Each signal is stored as two Parquet datasets on
object storage:

- `series`: one row per distinct identity. For logs the identity is the
  resource and scope (with their schema URLs); for metrics it also includes
  the metric name, unit, type, temporality, monotonicity and data point
  attributes. A series row is written when the series is new to this writer
  in the current hour partition, not with every block.
- `values`: one row per log record or metric data point, keyed by `series_id`
  and sorted by `(series_id, time_unix_nano)`.

Wide, rarely changing attributes (host, environment, service, ...) are
therefore stored about once per hour instead of on every row or in every
file. `series_id` is the XXH3-128 hash of a canonical encoding of the
identity; it is the same across writers and languages.

[docs/FORMAT.md](docs/FORMAT.md) specifies the encoding, schemas, layout and
compatibility rules. Its section
[Extensions and deviations](docs/FORMAT.md#extensions-and-deviations) lists
every difference from `exporter:series_parquet`: this exporter also stores
exponential histogram and summary points and the dropped-attribute counts,
and has no denormalized columns.

Not stored: metric exemplars. Metrics payloads without a univariate metrics
table (multivariate metrics) are refused. See
[Data Loss and Durability](#data-loss-and-durability).

## Getting Started

```yaml
parquet:
  type: exporter:parquet_lake
  config:
    storage:
      file:
        base_uri: "/tmp/otap-parquet-lake"
    writer_id: writer
    producer_id_attribute: host.id
    window:
      interval: 15s
      max_block_bytes: 67108864
      flush_retry_deadline: 60s
```

A complete pipeline (OTLP and OTAP receivers, batching, this exporter) is in
[configs/otlp-otap-parquet-lake.yaml](../../../../../configs/otlp-otap-parquet-lake.yaml).

Give every exporter instance that writes into one `base_uri` its own
`writer_id`. Instances of one pipeline on several cores may share it: each
start draws a random `boot_id`, which is part of every object name.

## Configuration

Unknown fields are rejected, including the fields of earlier builds
(`max_block_bytes`, `max_block_age`, `check_interval` and `upload_deadline`
at the top level).

| Field | Default | Meaning |
| --- | --- | --- |
| `storage` | required | Object-store backend: `file`, `s3` or `azure` (same as `exporter:parquet`). |
| `retry` | none | Per-request object-store retry policy. |
| `writer_id` | `writer` | Writer id in object names and file footers. Non-empty, made of `[A-Za-z0-9_.-]`. |
| `producer_id_attribute` | `host.id` | Resource attribute copied into the `producer_id` column of values rows (`""` when the resource does not have it). |
| `window.interval` | 15s | Blocks rotate at multiples of this interval since the Unix epoch. Whole seconds, 1s to 1h. |
| `window.max_block_bytes` | 64 MiB | Buffered Arrow bytes of one generation (the logs block plus the metrics block of a window); a generation that would exceed it rotates early. Range 1 MiB to 1 GiB. |
| `window.flush_retry_deadline` | 60s | Give up on a generation (Nack its requests) when it has not landed this long after its flush started. |
| `ingress.max_request_bytes` | 16 MiB | Largest request as received (OTLP bytes or OTAP Arrow size). |
| `ingress.max_extracted_bytes` | 32 MiB | Largest memory the extraction of one request may need. At most `window.max_block_bytes`. |
| `ingress.max_row_bytes` | 1 MiB | Largest values or series row, and largest attribute or body value. At most `ingress.max_extracted_bytes`. |
| `ingress.max_nesting_depth` | 32 | Deepest nesting of arrays and key/value lists in one value. Range 1 to 256. |
| `series_cache.max_entries` | 200000 | Series remembered as written in their hour partition. Range 1 to 16777216. |
| `retry_initial_backoff` | 200ms | First retry backoff of a block upload; doubles per attempt. |
| `retry_max_backoff` | 10s | Largest retry backoff. |

Extraction expands a request well past its wire size: the rows, the rendered
attribute maps and the identity bytes of attribute-heavy logs can reach three
to four times the OTLP byte size. `ingress.max_extracted_bytes` is measured on
that expanded form, so a request that passes `ingress.max_request_bytes` can
still be refused by `ingress.max_extracted_bytes`. Size the two together: with
the defaults (16 MiB request, 32 MiB extracted) a request of attribute-heavy
logs above roughly 9 MiB is refused. Raise `ingress.max_extracted_bytes` (up to
`window.max_block_bytes`), or bound the upstream batch size below
`max_extracted_bytes` divided by the expansion factor for your data, not just
below `max_request_bytes`.

## Delivery Semantics

- **Ack after land.** A request is acknowledged only after every block
  holding its rows has landed, meaning the block's values object was written
  (after its series object, when it has one). Receivers must use
  `wait_for_result: true`. The exporter logs `parquet_lake.unacked_input`
  once when a request arrives that nobody upstream waits on.
- **Flush pipeline.** One generation (ACTIVE) accepts input while at most one
  other generation is sorted, encoded and uploaded in the flush slot. ACTIVE
  rotates into the slot when its window ends or when it would exceed
  `window.max_block_bytes`. If the slot is still busy then, ACTIVE waits for
  it; a request that does not fit is parked, and the exporter stops reading
  pdata until the slot frees, so backpressure reaches the receivers. Control
  messages, including shutdown, are always handled.
- **Ack delay.** A request read while admission is open is resolved within
  `window.interval + 2 x window.flush_retry_deadline` (135 s with the
  defaults) plus sort and encode time: its window ends, the slot frees, its
  own generation lands or fails. A request that was parked is resolved within
  `2 x flush_retry_deadline + max(interval, flush_retry_deadline)` (180 s),
  provided its remaining chunks fit one empty generation, which holds when
  `ingress.max_extracted_bytes` is at most half of `window.max_block_bytes`
  (the defaults); with a larger limit the remainder can be parked once more
  and wait another `flush_retry_deadline`. Requests waiting in the channel
  while admission is closed wait longer still. Set receiver timeouts above
  the second value and let producers retry.
- **Durable on the file backend.** With `storage: file`, the files and their
  directories are fsynced before the request is acknowledged; an fsync
  failure counts as a failed block.
- **Idempotent retries.** A block is encoded once, and its object names are
  fixed before the first attempt. A failed upload is retried with backoff and
  rewrites the same names with the same bytes, one `put` per object. After an
  ambiguous failure, `probe_block` checks whether the block landed anyway.
- **Deadline.** When a generation has not landed within
  `window.flush_retry_deadline`, each request with rows in a failed block
  gets one retryable Nack. A flush task that fails or panics counts as a
  failed block; the exporter keeps running.
- **Shutdown.** The generation in the flush slot gets until the earlier of
  the shutdown deadline and its own deadline; ACTIVE is flushed when time
  remains. Everything left gets a retryable Nack with `NodeShutdown`. The
  exporter returns by the shutdown deadline.
- **Refused input.** A request above one of the `ingress` limits, a request
  with invalid content (a duplicate attribute key, a data point without a
  metric, a sum or histogram without temporality, histogram lists of
  mismatched lengths, a count above `i64::MAX`, an undecodable nested value,
  a column type the OTAP schema does not use), traces, multivariate metrics
  and malformed OTLP bytes each get one permanent Nack with `Refused`. The
  reason names the rule or the setting, the observed size and the limit.
  Nothing of a refused request is written.
- **At least once.** A crash between landing and acknowledging leads to a
  resend, so a block's rows can be written twice. A request with rows in two
  generations is Nacked when one of them fails, and the rows in the other may
  still land, so a resend can duplicate them. The same holds for a request
  that is Nacked at shutdown while parked, and for an upload whose values
  write lands at the very moment the deadline expires.

## Data Loss and Durability

The situations in which data can be lost, and how this exporter handles each:

| Situation | Handling |
| --- | --- |
| Receivers ack before the data lands (`wait_for_result: false`, the receivers' default) | Cannot be enforced here; the exporter warns once (`parquet_lake.unacked_input`). Set `wait_for_result: true`. |
| Malformed or truncated OTLP bytes | Every OTLP request is strictly decoded before conversion, so a request that is not a complete, valid OTLP message is refused (permanent Nack). This also stops the lenient converter from acknowledging a partial batch (records after a parse error dropped), panicking on a value whose wire type does not match its field number, or overflowing the stack on a deeply nested value. A valid empty request is still acknowledged. The reason carries the decoder's error and the refusal counts in `requests.refused.invalid`. Invalid UTF-8 alone is not malformed; see the next row. Known gap: the nesting bound relies on prost's recursion limit of 100, and the default `df_engine` build compiles prost without it (the `jemalloc` feature pulls in `pprof_util`, which enables prost's `no-recursion-limit`), so in that build a value nested thousands of levels deep can still overflow the stack during this decode. |
| Invalid UTF-8 in a string (OTLP string fields at any level, including nested values; CBOR text strings and map keys in OTAP `ser` cells; Binary-typed OTAP string columns) | Repaired, not refused: each invalid byte sequence becomes U+FFFD (the rule of Rust's `String::from_utf8_lossy`), the request is accepted and acked after it lands, and the repairs are counted in `requests.repaired` and `strings.repaired`. Strings that differ only in invalid bytes become equal after repair, so their series merge into one series id, and two attribute keys that become equal are refused as duplicate keys. A repaired OTLP request must still fit `ingress.max_request_bytes`. Traces are not repaired (they are refused as unsupported anyway). |
| An OTLP request with more than 65,535 attributed log records, metrics, scopes or resources | Refused permanently, with a reason naming the count. The OTLP-to-OTAP converter addresses these with a `u16` id that would otherwise wrap (silently misattributing log attributes in release builds) or fail deep in the converter. Split the batch upstream. OTAP input is not affected. |
| Power loss after an ack on the `file` backend | Files and directories are fsynced before the ack. |
| Storage outage longer than the producers' retry window, or producer queues filling during slow uploads | No local disk buffer: producers must retry and queue. Size their retry time above the expected outage and their timeout above the worst-case ack delay. Use `durable_buffer` upstream only if losing ack-after-land is acceptable. |
| Shutdown during a flush | The shutdown is handled at once; the flush gets until the shutdown deadline. Requests that did not land get a retryable `NodeShutdown` Nack. |
| A request above `ingress.max_request_bytes`, `ingress.max_extracted_bytes`, `ingress.max_row_bytes` or `ingress.max_nesting_depth` | Refused permanently; the reason names the setting, the observed value and the limit. Split the batch upstream or raise the limit. |
| `processor:batch` builds batches above `ingress.max_request_bytes` | The batch is refused. With the batch processor's isolation (its default) each request is then re-sent alone, so only a request that is itself too large or invalid is refused; with isolation disabled the permanent refusal reaches every request in the batch. Bound the batch size anyway: for OTLP batches set `otlp.max_size` (`sizer: bytes`) below the limit; for OTAP batches only `sizer: items` exists, so `otap.max_size` bounds the record count, not the bytes. |
| Duplicate attribute keys, a data point without a metric row, a sum or histogram without temporality, histogram lists of mismatched lengths, a count above `i64::MAX`, an undecodable nested value | Refused permanently with a reason naming the rule; the valid rows of that request are not written either. |
| Traces or multivariate metrics sent to this exporter | Refused (permanent). Route them elsewhere. |
| Deterministic encode failure of a block | Retryable Nack for the block's requests: requests that only shared the block succeed on resend; the failing request keeps failing until the producer gives up (logged as `parquet_lake.block.failed`). |
| Restart, or more series per hour than `series_cache.max_entries` | Series rows are written again; they are never lost. `series_cache.evictions` and `series_rows.written` grow. |
| Attribute and body values rendered as text | Types are flattened in the maps (int 7 and "7" look alike; bytes are quoted base64; NaN and infinities are the strings `"NaN"`, `"Infinity"`, `"-Infinity"`). The series identity keeps the types. |
| Metric description and dropped-attribute counts change | Not part of the identity. They are refreshed when the series row is written again: in the next hour, after a cache eviction or after a restart. Readers keep the latest row. |
| Timestamps of 0 or above `i64::MAX` nanoseconds | Stored as null; the out-of-range ones are counted in `timestamps.out_of_range`. |
| Exemplars | Not stored. |
| `Metric.metadata` (for example `prometheus.type`) | Not stored: the exporter does not read the metric metadata table. The request is still acknowledged. |
| A metric with no data points | Not stored, so its name, unit, description and metadata are not kept. A series row needs at least one point. |
| `base_uri` on a FUSE object-store mount (blobfuse, gcsfuse, s3fs) or NFS | The durability fsync assumes a local POSIX disk. On a network or FUSE filesystem the underlying `object_store` may report an upload as written while a deferred close fails, so a block can be acknowledged without landing. Point `base_uri` at a local disk, or use the `s3`/`azure` backends. |
| A large block on a slow upload link | The object store's per-request HTTP timeout is 30 s and is not configurable here; a block that cannot upload within it is retried until `flush_retry_deadline` and then Nacked. Keep `window.max_block_bytes` small enough to upload within 30 s on the available bandwidth. |
| Queries that prune on `date=` / `hour=` | The partition is the block's ingest window, not the event time; filter on `time_unix_nano` for event time and widen the partition range for late data. |
| Retention deleting series files before values files | Values can no longer be joined: keep the series files of a partition at least as long as its values files. |

`probe_block(store, &block_id)` is public. It reports whether a block landed,
and a `BlockId` can be parsed from its `Display` form
(`<signal>:<window_start_secs>:<writer_id>:<boot_id>:<seq>`).

## Memory

Memory held by the exporter is bounded by
`LakeConfig::worst_case_flush_bytes()`, which is logged at start:

```text
3 x B + B/8            the ACTIVE generation, the generation in the flush slot (held
                       until encoded) and the encoded files of the block being uploaded
+ (B / 96) x 36        sort keys and row weights of the block being encoded (a values row is
                       at least 96 bytes)
+ 2 x S                one sorted slice on its way into the encoder; S is the larger of 1 MiB
                       and ingress.max_row_bytes (a row larger than 1 MiB is a slice alone),
                       and the Arrow memory of a gathered slice is up to twice its row content
+ 3 x E                the request being extracted (its rows and their chunk copies)
                       and one parked request
+ 2 x B/4              one chunk of slack for each of those two requests
+ N x 128              the series cache
+ 2 x 1 MiB            unfilled last allocation of each output file
+ 8 MiB + max(8 MiB, S)  encoder buffers: the row-group cap plus the slice that crosses it and
                       the page buffers (16 MiB with the default S), measured in tests
```

`B` is `window.max_block_bytes`, `E` is `ingress.max_extracted_bytes` and `N`
is `series_cache.max_entries`. The defaults give 415670248 bytes (396.41 MiB)
per exporter instance (per core).

Not included: the received payload itself (at most
`ingress.max_request_bytes`, plus its OTAP form while it is converted), the
per-block sets of seen series ids (16 bytes per distinct series), and the
growth slack of the vectors built while a nested attribute value is decoded
(at most twice the charged nodes). The `B/8` encoded-size margin is verified
with incompressible data in tests rather than enforced at runtime
(`encoder.peak`, `slice.peak` and `bytes.written` show real values).

`ingress.max_extracted_bytes` limits the memory the extraction of one request
may need (its rows plus the intermediate copies made while building them), so
the rows it admits are somewhat smaller. Every allocation that grows with the
request is charged to this budget before it is made; a small request that
refers to one large resource or attribute many times is refused instead of
being expanded.

Sizing `series_cache.max_entries`: one entry per series seen in an hour, 128
bytes each. Above the limit the exporter stays correct and writes series rows
again; `series_cache.evictions` shows it.

## Files

With 15 s windows a writer produces up to 5760 values files per signal per
day (one per window that received data), plus one more for every generation
rotated early by `window.max_block_bytes`. Series files are fewer: one in the
first window of each hour, and one in every later window that brings new
series. A longer `window.interval` gives fewer, larger files and a longer ack
delay.

## Querying

Series rows repeat (each hour, after a restart, after a cache eviction), so
join values to one series row per `series_id` and partition. Every values
row has its series row in the same `date=`/`hour=` partition. In DuckDB:

```sql
WITH series AS (
  SELECT * FROM read_parquet(
    '/tmp/otap-parquet-lake/v=1/signal=logs/dataset=series/date=*/hour=*/*.parquet',
    hive_partitioning = true, union_by_name = true, filename = true)
  QUALIFY row_number() OVER (PARTITION BY series_id, date, hour
                             ORDER BY emitted_at DESC, filename DESC) = 1
)
SELECT s.resource_attrs, v.time_unix_nano, v.body
FROM read_parquet(
    '/tmp/otap-parquet-lake/v=1/signal=logs/dataset=values/date=*/hour=*/*.parquet',
    hive_partitioning = true, union_by_name = true) AS v
JOIN series AS s USING (series_id, date, hour);
```

For metrics, join `signal=metrics/dataset=values` to
`signal=metrics/dataset=series` the same way and take the point kind from
`s.metric_type` (`gauge`, `sum`, `histogram`, `exp_histogram`, `summary`),
never from which value columns are null. See
[docs/FORMAT.md](docs/FORMAT.md#6-reading-the-data).

## Telemetry

### Metric Sets

#### `exporter.exports`

The standard exporter outcome measurements (success and failure per signal).

#### `exporter.parquet_lake`

| Metric | Unit | Meaning |
| --- | --- | --- |
| `blocks.landed` | `{block}` | Blocks that landed |
| `blocks.failed` | `{block}` | Blocks given up (encode failure, flush deadline, task failure, shutdown) |
| `upload.retries` | `{retry}` | Upload retries of landed blocks |
| `bytes.written` | `By` | Bytes of landed files |
| `rows.written` | `{row}` | Values rows in landed blocks |
| `series_rows.written` | `{row}` | Series rows in landed blocks (new series, new hour, or written again after a failure, an eviction or a restart) |
| `flush.duration` | `s` | Sort, encode and upload time per block |
| `encoder.peak` | `By` | Peak encoder memory per encoded block |
| `slice.peak` | `By` | Largest sorted slice gathered into the encoder per encoded block (slices are cut at 1 MiB of row content, or one row; the Arrow memory of a slice is up to twice its content) |
| `flushes.time` | `{flush}` | Generations rotated because their window ended |
| `flushes.bytes` | `{flush}` | Generations rotated because they reached `window.max_block_bytes` |
| `flushes.shutdown` | `{flush}` | Generations rotated by a shutdown |
| `series_cache.entries` | `{entry}` | Series the cache holds |
| `series_cache.hits` | `{lookup}` | Lookups that found the series written in the block's partition. A series is looked up when its row is pushed and, if it was buffered, again when its block starts flushing: a series already written in the partition counts one hit, a new one two misses (or a miss and a hit when another block wrote it in between), and a repeat within one block is not looked up |
| `series_cache.misses` | `{lookup}` | Lookups that did not (same counting) |
| `series_cache.evictions` | `{entry}` | Entries dropped at `series_cache.max_entries` |
| `admission.closed` | `{state}` | 1 while the exporter reads no pdata (ACTIVE is full or due and the flush slot is busy) |
| `admission.closed.duration` | `s` | Length of each period with admission closed |
| `requests.refused.too_large` | `{request}` | Requests refused by a size limit |
| `requests.refused.invalid` | `{request}` | Requests refused for invalid content |
| `requests.refused.too_deep` | `{request}` | Requests refused for nesting beyond `ingress.max_nesting_depth` |
| `requests.refused.unsupported` | `{request}` | Requests refused as unsupported (traces) |
| `requests.refused.other` | `{request}` | Requests refused for a conversion or internal failure |
| `timestamps.out_of_range` | `{timestamp}` | Timestamps stored as null because they were negative |
| `requests.repaired` | `{request}` | Requests accepted after invalid UTF-8 in their strings was replaced with U+FFFD |
| `strings.repaired` | `{string}` | String values whose invalid UTF-8 was replaced with U+FFFD |

### Events

- `parquet_lake.start` (info): writer id, boot id, window interval, `max_block_bytes`, `worst_case_flush_bytes`.
- `parquet_lake.block.failed` (warn): a block did not land; its requests are Nacked.
- `parquet_lake.request.refused` (warn, at most one per second): a request was refused permanently, with the reason.
- `parquet_lake.request.repaired` (warn, at most one per second): invalid UTF-8 in a request was replaced with U+FFFD; carries the number of repaired strings, never their content.
- `parquet_lake.unacked_input` (warn, once): a request arrived without Ack/Nack subscribers, so upstream acknowledged it before it landed.

## Limits

- The flush task runs on the exporter's core thread. Uploads wait on the
  network without holding the thread, but sorting and encoding use it, in
  slices of about 1 MiB with a yield after each slice.
- There is one flush slot: one generation is flushed at a time, and the
  blocks of a generation (logs, then metrics) one after the other.
- The series cache is per exporter instance and lives in memory: a restart,
  and each instance of a multi-core pipeline, writes its own series rows.
- Do not point this exporter and `exporter:series_parquet` at the same
  `base_uri` unless the readers handle the differences listed in
  [docs/FORMAT.md](docs/FORMAT.md#extensions-and-deviations).
- Data written by earlier builds of this exporter (the `dt=` layout) is not
  readable with the queries above; start from an empty `base_uri`.
- `series_id` uses XXH3-128, which is not collision-resistant against
  adversarial input.

### Pipeline placement

Ack-after-land only reaches producers when the delivery contract is preserved
end to end. The following upstream behaviors defeat it and cannot be corrected
from the exporter; keep them off this path:

- Keep `wait_for_result: true` on the receivers (see Delivery Semantics). With
  the default `false`, upstream acknowledges before the block lands.
- Do not place `processor:durable_buffer` or a `processor:fanout` with
  `await_ack: none` (or as a non-primary destination) in front of this
  exporter: each acknowledges upstream before this exporter reports its
  result, bypassing ack-after-land.
- Do not rely on `processor:retry` to retry this exporter's failures. A
  retryable Nack from this exporter carries no payload (the exporter holds
  only the ack context, not a copy of every in-flight request), so the retry
  processor cannot resend it; with `exhaustion_action: mark_permanent` it
  turns a transient failure into a permanent one. Let the producers retry
  instead.
- Do not insert a node that converts the payload format (OTLP <-> OTAP)
  between `processor:batch` and this exporter: the batch processor routes the
  acknowledgement by the returned payload's format, so a conversion can strand
  it. A direct `batch -> parquet_lake` edge is correct.
- A permanent refusal from this exporter (invalid content, an ingress limit,
  traces, multivariate metrics) is only delivered as non-retryable to OTLP/HTTP
  and OTLP/gRPC clients. `processor:batch` relays it as retryable, and the OTAP
  receiver maps every Nack to a retryable status, so an OTAP producer or a
  producer behind the batch processor retries a request that will always be
  refused.

## Related Docs

- [docs/FORMAT.md](docs/FORMAT.md)
- [exporter:parquet](../parquet_exporter/README.md)
