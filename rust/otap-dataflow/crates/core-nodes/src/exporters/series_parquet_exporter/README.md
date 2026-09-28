# Series Parquet Exporter

## Metadata

- Type: `exporter:series_parquet` (`urn:otel:exporter:series_parquet`)
- Feature gate: `series-parquet` (opt-in, not in `core-exporters`); add `aws`
  for S3-compatible storage, `azure` for Azure Blob Storage
- Metric scope: `exporter.series_parquet`
- Stability: Experimental

## Overview

`exporter:series_parquet` writes logs and metric number and histogram points
as series descriptors plus narrow values datasets, on a local filesystem, on
S3-compatible object storage or on Azure Blob Storage, under
`v=1/signal=<signal>/dataset=<dataset>/date=<date>/hour=<hour>/`. Values rows
join their `series` descriptor on `series_id`; number and histogram points
share one metrics `values` dataset, and the descriptor's `metric_type` gives
the point kind. The normative format is
[`crates/series-lake/docs/FORMAT.md`](../../../../series-lake/docs/FORMAT.md).

The worker owns one ACTIVE block, at most one FLUSHING block and at most one
parked request. A request is acknowledged only once its whole block has been
written and its descriptors marked committed. Admission closes while the
ACTIVE block waits for the flush slot, so a slow destination becomes
backpressure; a request the ACTIVE block has no room for is parked and enters
the next block before anything newer. A block covers one aligned
`window.interval` window (whole seconds) and is sealed when it ends, or
earlier when it reaches `window.max_block_bytes` or
`window.max_requests_per_block`, which writes another file set for that window.
Waiting uses the monotonic clock, so a wall clock stepping back delays
acknowledgements by at most one interval.

Delivery is at-least-once: producers must retry a request on a retryable
failure or a timeout, and a retry can duplicate rows already committed.

## Getting Started

For Grafana Alloy or any OTLP producer, start from
`configs/series-parquet-buffered.yaml`, which puts a durable buffer in front of
this exporter (see [Deploying with Alloy](#deploying-with-alloy); build with
`durable-buffer` too). Use `configs/series-parquet-local.yaml` for a local
destination and `configs/series-parquet-s3.yaml` for the strict S3 deployment,
whose OK means the rows are in the bucket. From `rust/otap-dataflow`:

```bash
cargo build -p otel-arrow-dfe --bin df_engine --features series-parquet,aws
sudo install -d -o "$USER" /var/lib/otap/series-parquet
./target/debug/df_engine --config configs/series-parquet-local.yaml --validate-and-exit
./target/debug/df_engine --config configs/series-parquet-local.yaml --http-admin-bind 127.0.0.1:8080
```

Both examples run on one core; each added core is another worker with its own
budget and descriptor cache (see "Memory"). A pipeline without
`core_allocation` runs on every core of the host, so adding this exporter to it
reserves 1.64 GB of budget per core at the defaults; set
`core_allocation: {type: core_count, count: N}`.

## Delivery and shutdown

This section describes the exporter's own acknowledgement, which the strict
deployment passes straight to the producer; behind `durable_buffer` it goes
to the buffer instead (see [Deploying with Alloy](#deploying-with-alloy)). In
the strict deployment, connect `receiver:otlp` directly to this exporter with
`protocols.grpc.wait_for_result: true` and `timeout: 180s`. An OK response
means every file of that request's block completed in the object store. For
`storage: file` the local backend renames a staging file into place but never
calls `fsync`, so an acknowledged file survives a crash of this process, not
necessarily a host crash; use a cloud store, or a filesystem mounted for
synchronous writes, when an ack must survive the host.

| Outcome | gRPC status | Producer action |
| --- | --- | --- |
| Durable | OK | none |
| Content, schema or budget refusal | INVALID_ARGUMENT | fix the request |
| Storage failure, flush deadline, shutdown, internal error | UNAVAILABLE | retry |
| Receiver concurrency limit, rate limit or memory pressure | RESOURCE_EXHAUSTED with a RetryInfo delay | retry |
| Message above the receiver's rate-limit burst, under memory pressure | RESOURCE_EXHAUSTED with a RetryInfo delay, counted as `rate_limit` | retry; it is admitted once the pressure clears |
| Message above the receiver's `max_decoding_message_size` | INVALID_ARGUMENT | split the batch or raise the limit |
| Producer-side timeout | DEADLINE_EXCEEDED | retry; may duplicate |

Every nack's status message names the rule or limit that decided it and what
to do, for example `request of 20000000 bytes exceeds
ingress.max_request_bytes (16777216 bytes); split the batch upstream or raise
the limit`, with quoted detail cut to 256 bytes on one line. Refusals are
logged at WARN as `series_parquet.request.failed`, at most one line per
second for each outcome; a size refusal carries `limit_setting`,
`observed_bytes` and `limit_bytes`.

A completion never carries the request's payload back, so a
`processor:retry` in front of this exporter cannot resend anything: it ends
every retryable nack as "retry lost payload". Retry behind `durable_buffer`
(see [Running behind durable_buffer](#running-behind-durable_buffer)) or
leave retries to the producer. The first request of a process that asks for
its payload back is logged at WARN as `series_parquet.payload.not_returned`.

### Failed blocks

A storage failure is retried against the identical sealed block, same file
names and bytes, until an absolute deadline taken when the block was sealed
(`window.flush_retry_deadline`); each failed attempt is logged at WARN as
`series_parquet.flush.attempt_failed`. Encoding failures, refused credentials
and a missing bucket or path are not retried. A write that has finished when
the deadline expires is acknowledged. An unfinished one is cancelled, given at
most `upload.abort_timeout` to unwind while the flush slot stays occupied, and
its requests are nacked as retryable with a fixed reason, never the store's
error text: `could not write to object storage (<class>); retry the request`,
with the class `unavailable`, `rejected by the store`, `cancelled`, `encoding
failed` or `internal error`. Only a fully successful write marks the
descriptor cache.

A multipart completion whose response is lost may still have been applied.
The writer then sends one HEAD for that object before any abort. An object
that exists holds the block's frozen bytes, so the write goes on and the block
is acknowledged; the upload is then aborted. On the block's first write
attempt nothing else can have written its frozen names, so the found object is
this completion's commit whatever the abort answers (MinIO accepts the abort
of a completed upload); on a retry only an abort answered `NotFound` shows
this completion committed it. Either counts a late commit
(`flush.late_commits{outcome=acknowledged}`, INFO
`series_parquet.flush.cleanup`). An object that does not exist is aborted and
the completion's own error decides the retry. A HEAD that fails otherwise
leaves the upload alone, since the completion may still be applied, and counts
it in `flush.abort_failures`.

A failed block can still leave its files behind: the store may finish an
upload after the cancellation, or commit one and lose the response. After
such an ambiguous end the cleanup sends one HEAD per object name, within the
same cutoff. When every object exists the flush is a late commit too; its
requests are still nacked, so its rows may be stored twice once the producers
retry.

### Block admission

A request is judged against `window.max_block_bytes` on its worst case, as if
every series it carries were new to the block, so it gets the same answer after
a restart or a cache eviction; one that fits the worst case but not the space
left waits for the next block. The worst case is `P + T + sum over its series
of (2 * (A - D) + F)`: values rows P with their merge keys, completion token T,
each series' extracted estimate A less its decoded attribute trees D, and a
fixed F of 232 bytes per logs series or 328 per metrics series, plus 16 per
denormalized series column.

A table's merge keys exist only while the flush writes it, but they are
reserved with their rows from admission on, so a block and the keys of the
table being written stay within `window.max_block_bytes` together. The bound
per values row is the key's row-format size plus 18 bytes: 44 bytes for the
default `series_id, time_unix_nano`, and a little over the value's length for a
string key. A key as wide as the row, such as `body`, therefore halves what a
block holds. The bound also counts against `ingress.max_extracted_bytes`, and a
sort key must be a number, timestamp, boolean, id or string column.

F comes from the measured heap of a series row (DHAT, requests of 1000 to
100,000 minimal series):
342 to 365 bytes per logs row and 405 to 428 per metrics row for requests of
1000 or more minimal series, 1 to 10 bytes more per denormalized column, and
about 30 bytes of merge key besides. A minimal series is charged 844 bytes
(logs) or 1076 (metrics), at least 2.1 times what it holds, and F alone is at
least 2.6 times the heap beyond the row's content. The first series of a block
also costs 19 to 22 KB of fixed structures, which no charge includes.

Startup refuses budgets under which a request that passes ingress could miss
an empty block:

```text
2 * ingress.max_extracted_bytes + ingress.max_series_per_request * F_max + 4KiB
  <= window.max_block_bytes
```

F_max is the larger F of the two signals, and 4KiB allows for the completion
token. The token is the request's routing context, so its size comes from the
route: a request whose token is larger is refused permanently at ingress
(`nacks{error.type=token_too_large}`, naming the allowance and the observed
size), since every retry would fail alike; remove subscribing nodes from the
route to this exporter. `ingress.max_series_per_request` bounds the distinct
series of one request, and extraction refuses a request as soon as it passes
the limit (`nacks{error.type=too_many_series}`, a permanent refusal naming the
count and the limit). Unset, it is the most series the
block budget holds, `(B - 2E - 4KiB) / F_max` with B `window.max_block_bytes`
and E `ingress.max_extracted_bytes`: 1,393,826 at the defaults, and 1,328,997
with the one denormalized column per signal of the shipped configurations. So
every request that passes ingress, with a token within the allowance, fits an
empty block, and `window.max_block_bytes` only rotates blocks and bounds
memory. A worst case that still misses an empty block would be a writer defect:
it fails a debug assertion and is nacked as a retryable `internal`.

### Attempt timeouts

A request waits for the rest of its aligned window before its block is
sealed, so a producer attempt timeout below `window.interval` makes
first-attempt completion depend on arrival time: expired attempts are resent
and may store rows twice, and the symptom is a steady rate of client deadlines
while `acks` still rise. Size the attempt timeout and the receiver `timeout`
as the sum of the channel residence, a flush already in progress plus its
`upload.abort_timeout`, the rest of the window (at most `window.interval`),
its own flush (up to `window.flush_retry_deadline`) and the completion
delivery. The strict configurations (`series-parquet-local.yaml`,
`series-parquet-s3.yaml`) use 180s against a 15s window and a 60s flush
deadline. The buffered one answers the producer after the WAL write, so its
15s receiver timeout does not depend on the window.

No block-atomic snapshot is provided: series files complete before their
values files, but readers can observe a subset of a block, including rows of a
request that was later nacked, so read values first, then descriptors. There
is no write-ahead log and no spill; a restart writes new file names under a
new boot UUID. An hour partition receives no write later than
`window.interval + 2 * (window.flush_retry_deadline + upload.abort_timeout)`
after it ends, 145s at the defaults, except an upload the store completes
after the writer gave up (FORMAT.md, "Partition lateness bound").

### Shutdown waits for the current window

The engine shuts down receiver-first: the exporter is handed `Shutdown` once
every receiver has drained, or at the shared deadline. A receiver with
`wait_for_result: true` holds each response until its block is sealed,
normally at the next window boundary, so a shutdown can spend up to one
`window.interval` before the exporter's drain starts. With a window comparable
to the deadline, the deadline expires first: the admin call returns HTTP 504
and the outstanding requests are nacked as retryable.

### The drain

Until the exporter is handed its Shutdown, every block keeps its own retry
deadline. When the engine latches the exporter's Shutdown, it still hands
over the requests already buffered in the input channel, and releases the
Shutdown once the input is closed and empty, or at the deadline. A drained
request is admitted while admission is open; one drained while it is closed
(a rotation waiting for the flush slot, a parked request, or no completion
credit left) is refused as retryable at once. A later Shutdown with an
earlier deadline, latched before the release, moves the deadline the
exporter is handed. The shutdown begins with the Shutdown the exporter is
handed. From then on the exporter finishes only what it already holds, and
stops writing 1.2s before the deadline: a durable buffer in front records
acknowledgements only until 1s before the deadline (its
`SHUTDOWN_PERSIST_RESERVE`, kept for its final persist), and the other 200ms
carry the decisions to it:

- The parked request is nacked as retryable at once.
- The FLUSHING block retries until the earlier of its own deadline and 1.2s
  before the shutdown deadline, with the backoff at its 200ms minimum. An
  attempt starts only before that cut, and one still running there is
  cancelled.
- The ACTIVE block is sealed as soon as the flush slot frees and is written
  under the same rules.
- Whatever has not finished by the cut is nacked as retryable with
  `NodeShutdown`.
- From the cut to the deadline the exporter delivers every decision, Acks
  first, waiting for room in the completion channel. A decision the channel
  has not taken by the deadline is counted as a delivery failure, an Ack also
  in `notify.lost_acks`; its producer times out and retries.

An upstream that keeps its sender open until the deadline hands the
exporter its Shutdown only then, with no time left: what it holds is nacked
as retryable at once, and requests still in the input channel are dropped
undecided with it; a durable buffer in front replays both after the next
start, and a receiver resolves their waiters at its own deadline.

The node returns as soon as it holds nothing, and at the latest by the
deadline plus `upload.abort_timeout` (refused below 1s) plus the synchronous
decision of the held requests, about 9ms for the 8,191 completions a worker
can hold at the defaults. The bound is absolute, so the drain fits any
deadline: a short one nacks more, a long one commits more. An operation
already issued when the deadline cut its attempt may still complete at the
store; its block is nacked, and the retry may store those rows twice.

### Granting a deadline

Engine signal shutdown (SIGINT, SIGTERM) grants 60s and is not configurable.
The admin shutdown operation takes its own timeout, also 60s by default:

```bash
curl -X POST 'http://127.0.0.1:8080/api/v1/groups/shutdown?wait=true&timeout_secs=180'
```

There is no pipeline YAML shutdown-deadline key. Supervisors must allow the
deadline plus `upload.abort_timeout` plus the held-request decision. On
Kubernetes set `terminationGracePeriodSeconds` (30s by default) to at least
60s plus `upload.abort_timeout` and a margin, for example 75. For a longer
drain, add a `preStop` hook that calls the admin shutdown with the timeout
you want, and set the grace period above that timeout plus
`upload.abort_timeout`, since it also covers the hook.

## Configuration

Byte sizes accept integers or IEC strings such as `16MiB`; durations use
humantime strings. Unknown fields are errors, including a block budget
written under `ingress` instead of `window`. Files are always ZSTD-compressed.
An empty `values_sort` writes a signal's values unsorted.

| Setting | Default |
| --- | --- |
| `window.interval` | 15s |
| `window.max_block_bytes` | 500MiB |
| `window.max_requests_per_block` | 4096 |
| `window.flush_retry_deadline` | 60s |
| `ingress.max_request_bytes` | 16MiB |
| `ingress.max_extracted_bytes` | 32MiB |
| `ingress.max_row_bytes` | 1MiB |
| `ingress.max_nesting_depth` | 32 |
| `ingress.max_series_per_request` | derived, see [Block admission](#block-admission) |
| `series_cache.max_entries` | 200000 |
| `sorting.run_target_bytes` | 8MiB |
| `sorting.merge_chunk_bytes` | 16MiB |
| `upload.part_bytes` | 8MiB |
| `upload.concurrency` | 2 |
| `upload.abort_timeout` | 5s |
| `parquet.row_group_bytes` | 64MiB |
| `parquet.writer_limit_bytes` | 96MiB |
| `unsupported` | drop |
| `metrics.exemplars` | drop |
| `writer_id` | `writer` |
| `producer_id_attribute` | `host.id` |

Rules enforced at startup:

- Counts, byte and depth budgets, cache capacity, upload concurrency and the
  retry durations are positive; `upload.abort_timeout`
  is at least 1s; `window.flush_retry_deadline` and `upload.abort_timeout`
  are at most one hour; `ingress.max_nesting_depth` is at most 256;
  `window.interval` is whole seconds and at most one day.
- `ingress.max_row_bytes` is at most a quarter of `sorting.run_target_bytes`,
  and one request's worst case fits `window.max_block_bytes` (see
  [Block admission](#block-admission)).
- `upload.part_bytes` is between 5MiB and 5GiB; a worker warns
  (`series_parquet.upload.parts_exceed_limit`) when a file of
  `window.max_block_bytes` would need more than S3's 10,000 parts.
- For cloud storage, an explicit `retry.retry_timeout` is strictly less than
  `window.flush_retry_deadline`. Every field the `retry` section leaves unset,
  or the whole section when it is absent, takes object_store's default, except
  `retry_timeout`, which is half of `window.flush_retry_deadline`. Local file
  storage applies no store retry.
- `writer_id` uses only letters, digits, `_`, `.` and `-`, so a pod or host
  name fits; it is never part of the series identity.
- `metrics.series_attributes` and `logs.exemplars` are unknown fields, and a
  denormalized column is refused when named `v`, `signal`, `dataset`,
  `date` or `hour`.

S3 storage takes `unsigned_payload`: when true, requests are signed with SigV4
`UNSIGNED-PAYLOAD` instead of a SHA-256 of every uploaded byte, which removes
most of the upload CPU (102.5 instead of 376.4 ns per log record against
MinIO). No other checksum is sent either, so only TLS protects the bytes in
transit: set it only with an `https://` endpoint whose certificate is
verified. The value in the storage section wins, then `AWS_UNSIGNED_PAYLOAD`;
with neither set every payload is signed, as in every exporter that shares
the S3 storage section. The shipped S3 configurations name a plain-HTTP
loopback endpoint and leave it unset. The
start event reports the effective value as `unsigned_payload`. Some
S3-compatible stores and bucket policies refuse `UNSIGNED-PAYLOAD`; every
upload then fails with HTTP 403 (`block.write_failures` rises, nothing is stored).
Remove the setting for such a store.

Azure storage takes `base_uri`
(`https://<account>.blob.core.windows.net/<container>/<prefix>`) and an
optional `endpoint` that replaces the service URL the account implies, for a
private endpoint or a sovereign cloud. With an `endpoint`, `base_uri` keeps
that public-cloud form, since it only names the account, the container and
the prefix; any other host fails at startup. The Azurite emulator works only
when it serves HTTPS (`https://127.0.0.1:10000/devstoreaccount1`, Azurite
started with a certificate). It has no credential fields:
the node must bind `bearer_token_provider` to a token extension, such as
`azure_identity_auth` (managed or workload identity) or
`oauth2_client_auth`, and startup fails without that binding. The token must
be for the `https://storage.azure.com` audience. Only HTTPS is used, and an
`http://` endpoint, such as Azurite's default one, fails at startup; a store
behind a private CA is trusted through the system roots or `SSL_CERT_FILE`.
The Azure path was checked end to end against Azurite only; the failure and
throughput measurements ran on S3-compatible stores.

```yaml
extensions:
  azure_auth:
    type: "urn:microsoft:extension:azure_identity_auth"
    config:
      method: managed_identity
      scope: "https://storage.azure.com/.default"
nodes:
  exporter:
    type: exporter:series_parquet
    capabilities:
      bearer_token_provider: azure_auth
    config:
      storage:
        azure:
          base_uri: "https://myaccount.blob.core.windows.net/telemetry/otel"
```

`producer_id_attribute` projects that resource attribute into the
`producer_id` column of every values row and stays in the identity (see the
[producer id contract](../../../../series-lake/README.md#producer-id-contract)).
Logs put named record attributes into the identity with
`logs.series_attributes`; metric identity includes every point attribute, the
metric type and the temporality. Changing `logs.series_attributes` gives every
log series a new id; each file records the list in its `identity_config`
metadata (FORMAT.md section 5), so readers and compactors can tell the two
apart. `denormalize` takes a path (`resource.`,
`scope.` or `attrs.` prefix) or an object with `path`, `column` and `type`
(`string`, `int64`, `double`, `bool`); a value of the wrong non-string type is
stored as null and counted in `denormalize.type_mismatch{column}`.

The default values sort is `series_id, time_unix_nano`. For queries that
filter on service or environment, put that denormalized column first, for
example `service_name, series_id, time_unix_nano`. A sort key must be a
sortable column of that signal's values dataset, so `attrs` is refused;
`value_int` is null on every histogram row. Series files always sort by
`series_id`.

## Producers

A request is held for, on average, half a window plus the flush, so one target
rate keeps this many requests in flight:

```text
in_flight = rate * (interval / 2 + flush) / records_per_request
```

Size the producer's concurrency and this engine's receiver
`max_concurrent_requests` (summed over every producer) above it, with margin
for a whole window, and keep `rate * interval / records_per_request` under
`window.max_requests_per_block`. A receiver limit below `in_flight` caps
throughput at the receiver while `admission.closed` stays at zero.
`configs/series-parquet-s3.yaml` is sized for 100k records/s in 512-record
requests.

The OTLP gRPC receiver refuses a message above its `max_decoding_message_size`,
4MiB unless set, with INVALID_ARGUMENT, which OTLP clients do not retry, so
such a batch is dropped at the producer and counted in
`receiver.otlp.requests.rejected{error.type=payload_too_large}`. Set it to
`ingress.max_request_bytes`, as
the shipped configurations do (16MiB in the strict ones, 4MiB in the buffered
one, twice Alloy's 2MiB exports). The exporter cannot see the receiver's
setting and says so at startup, at INFO for the first worker of the process
(`series_parquet.receiver_limit.unverified`).

At-least-once begins when a request reaches this exporter: a producer queue
overflow before that is lost and invisible here, so a file source should block
on overflow. The reference Grafana Alloy producer is
[`configs/series-parquet.alloy`](../../../../../configs/series-parquet.alloy);
its tuning is described in the
[configs README](../../../../../configs/README.md#series-parquetalloy).

## Running behind durable_buffer

With `processor:durable_buffer` between the receiver and this exporter, the
producer is acknowledged once its request is in the buffer's local WAL, and
this exporter's acknowledgement goes to the buffer: an OK means "written to
this host's WAL", not "readable in the lake" (see "Durability of the
acknowledgement" under [Deploying with Alloy](#deploying-with-alloy) for what
it survives). The producer notices a slow object store only when the WAL
reaches its size cap; then, with `size_cap_policy: backpressure`, new requests
get a retryable refusal.

These losses happen after the WAL acknowledgement, so the producer never sees
them; alert on each:

- A permanent refusal by this exporter (an undecodable OTLP body, a request
  over an ingress budget or `ingress.max_series_per_request`, traces, or an
  unsupported point kind under `unsupported: reject`) drops the bundle, counted
  in the buffer's `resolved{outcome="permanently_rejected"}`.
- `size_cap_policy: drop_oldest` evicts, and `max_age` expires, acknowledged
  data.
- A damaged OTLP body that the conversion to Arrow records reads only in part
  is stored and acknowledged as the partial or empty batch it read.

At a graceful shutdown this exporter writes the requests it already holds, and
the buffer, with `shutdown.await_acks` on, records their acknowledgements.
Bundles the buffer drains after the shutdown began, such as those still in its
open segment, are admitted while admission is open (see
[The drain](#the-drain)) and refused as retryable otherwise; a refused bundle
stays in the WAL and is stored once, after the next start.

Freshness has no upper bound: with no backlog it is roughly the buffer's
segment finalisation (up to 1s) and poll (100ms), the rest of this exporter's
window and the flush; during an outage it is unbounded. The buffer's
`oldest_pending.age` gauge measures it from inside the engine; see "Alerts"
under [Deploying with Alloy](#deploying-with-alloy).

Each core runs an independent pipeline with its own receiver, buffer directory
`<path>/core_<core_id>` and exporter; `retention_size_cap` is divided between
cores, and SO_REUSEPORT spreads connections, not bytes, so monitor the maximum
fill across cores. There is no global order, and repeated series rows across
workers are expected. Give every process its own buffer directory, since the
WAL does not lock it. A changed core count moves no queue, so drain the old
shards first and keep old `core_<id>` directories until they are empty.

## Deploying with Alloy

The reference deployment is
[`configs/series-parquet-buffered.yaml`](../../../../../configs/series-parquet-buffered.yaml)
with Grafana Alloy running
[`configs/series-parquet.alloy`](../../../../../configs/series-parquet.alloy):

```text
log files -> Alloy (file tail, batch 4000, file-backed queue)
          -> OTLP gRPC -> receiver -> durable_buffer (WAL) -> series_parquet -> S3
```

An OK to Alloy means the batch is written to the WAL, in tens of
milliseconds; the buffer then retries every failed block until the store
takes it. The strict alternative, `series-parquet-s3.yaml` with
`series-parquet-strict.alloy`, answers only once the block is in the bucket
(see "Attempt timeouts"). Each setting of both files carries its reason in a
comment.

Run Alloy with `--stability.level=public-preview` (the file-backed sending
queue) and `--storage.path` on a persistent volume: it holds the file
positions and the queue across a restart. The files read their site values
from the environment: `SERIES_LOG_PATH` (the file to tail), `SERIES_SERVICE_NAME`
(the `service.name` of its lines), `OTLP_ENDPOINT` (the engine's receiver) and
`SERIES_PRODUCER_ID`, a value unique per producer, or unset to use the
hostname. The E2E suite checks both files with `alloy validate`.

Both files send plaintext OTLP (`tls { insecure = true }`), which suits an
engine on the same host or a private network. For an engine reached over an
untrusted network, give the receiver a certificate and replace the `tls` block
with one that verifies it. The engine does not authenticate the producer id:
`host.id` is whatever the producer sends, so any producer that can reach the
receiver can write rows under another's id. Where that matters, restrict who
reaches the receiver.

### Sizing

Rates are log lines; the Loki bridge makes a line about 300 bytes larger on
the wire, so 100-byte lines are about 400 bytes of OTLP each.

- **Workers.** One worker takes about 100k records/s behind the buffer: 104k
  was sustainable for 30 minutes on MinIO and 112k was not, with eight senders
  of 1000-record requests, 80% log records with 1KiB bodies and 20% metric
  points, 10k hot series and one record in 100 on a new series, and 15 s
  windows. The harness that measured it is not part of this repository, so
  treat the figure as a starting point for your own profile. Use
  `max(1, ceil(rate / 100k))` workers. The WAL device
  is the next limit: one NVMe shared with the store carried 144-152k records/s
  with four workers, so beyond that give each engine its own WAL device or run
  more engines.
- **WAL disk.** In steady state the WAL holds what the exporter has not yet
  committed, `ingest_bytes_per_s * (window.interval + flush)`: 300 MB at 40k
  lines/s. During a store outage it grows by the ingest rate: 2.6 GB after a
  150 s outage at 40k lines/s (16.8 MB/s). Size `retention_size_cap` for
  `ingest_bytes_per_s * (outage_tolerance + window.interval + 5s)` and keep the
  device's write bandwidth at twice the ingest bytes (WAL entry plus the
  finalized segment). The shipped 32GiB covers about 34 minutes at 40k
  lines/s and 13 minutes at 100k.
- **Memory.** Per worker, budget
  `memory.budget + max_in_flight * request_bytes + receiver slots * request_bytes`:
  the exporter's own budget (1.64 GB at the defaults), the bundles the buffer
  has handed to the exporter and not yet had acknowledged (`max_in_flight`,
  640 in the shipped config, 1000 by default), and requests the receiver holds
  for the WAL write. At the receiver's 4MiB limit that is
  1.64 + 640 x 4MiB + 128 x 4MiB = 1.64 + 2.68 + 0.54 = 4.86 GB whatever the
  producer, 3.25 GB with Alloy's 2MiB exports. A restart replays up to
  `max_in_flight` bundles at once, so the same bound holds after an OOM kill.
  Measured at 40k lines/s of 100-byte lines: 0.7 GB healthy,
  1.9 GB held and a 2.55 GB peak through a 150 s store outage, when the buffer
  keeps about 500 bundles at the exporter. With 4 KiB lines at the same
  15.9 MB/s, 2 MiB exports and `max_in_flight` 640: 2.7 GB held through the
  outage, and a peak of 4.0 GB (process high-water mark) in the second the
  store returned, when the buffer sent 470 bundles at once on top of the
  exporter's blocks and the allocator's retained pages. The formula counts
  live bytes; budget RSS at about 1.25 times it. The process needs this once
  per worker: N workers take N times it, and a pipeline without
  `core_allocation` has one worker per host core (N x 1.64 GB of exporter
  budget alone). The start-up WARN `series_parquet.memory_budget.oversubscribed`
  compares only the exporter budgets with the memory limit.
- **Bundles in flight.** A bundle stays in flight until its block commits,
  about `window.interval` plus the flush, so a worker carries at most
  `max_in_flight x lines per export / (window.interval + flush)`: 640 x 4000 /
  (15 s + 5 s) = 128k lines/s of 100-byte lines, above the per-worker rate.
  Raising `max_in_flight` raises that ceiling and the memory term together.
- **Producers per worker.** A request past the receiver's
  `max_concurrent_requests` gets RESOURCE_EXHAUSTED with a RetryInfo delay
  (counted in
  `receiver.otlp.requests.rejected{error.type=concurrency_limit}`), which
  Alloy retries with backoff, so a full receiver slows producers instead of
  dropping batches; a request that waits past Alloy's timeout is retried and
  may be stored twice. For throughput, keep the `num_consumers` of every
  producer that can reach one worker at most that limit: 64 producers at the
  shipped 128 and 2 consumers. For more, raise `max_concurrent_requests` and
  the pipeline's `pdata` channel capacity together (the receiver is clamped
  to it), and the memory term with them.
- **Alloy.** Two consumers carry 160k lines/s per producer at a 50 ms
  acknowledgement; the queue holds 16000 records (two batches per consumer).
  The sending queue splits every export at 2MiB whatever the line length,
  which bounds the buffer's memory and keeps every export at half the
  buffered receiver's 4MiB limit, and a `loki.process` stage cuts lines above
  512KiB (suffix included), below the exporter's 1MiB `ingress.max_row_bytes`.
  Both are in the shipped file; the truncation changes data and is counted.

### Durability of the acknowledgement

The buffer acknowledges a request once it is written to the WAL, before the
write is synced. The WAL syncs on a write at least 25 ms after its previous
sync, and the buffer's 100 ms tick finalizes and syncs the open segment. So
an acknowledged request survives a process crash (SIGKILL, OOM kill, panic):
it is in the page cache, and the restarted engine replays it, which the
SIGKILL cases below measure. A host crash or power loss can lose what was
acknowledged since the last sync, about the last 100 ms of requests; the
SIGKILL tests say nothing about that case. The WAL can sync every write
(quiver's `flush_interval` of zero), but `durable_buffer` does not expose that
setting and its cost is not measured. Where power loss must not lose
acknowledged data, use the strict deployment, whose OK means the rows are in
the bucket.

Alloy's file-backed queue has the same boundary: with `otelcol.storage.file`'s
default `fsync = false` a queued batch survives an Alloy crash but not a host
crash; `fsync = true` syncs every queue write, a disk sync per batch. Its file
positions are saved every 10 s and on a graceful stop.

### The WAL device

Put `path` on its own local NVMe: the WAL writes each request twice, syncs
every 25 ms, and a slow device raises every producer's latency. The WAL does
not lock its directory, so give every engine process its own path; each worker
uses `<path>/core_<id>`. The device must survive a process restart; a WAL on
tmpfs survives only that.

### Bucket permissions and lifecycle

The writer needs `s3:PutObject` (single PUT and every multipart step),
`s3:AbortMultipartUpload`, `s3:GetObject` (the HEAD probes after a lost
response) and `s3:ListBucket` on the bucket, without which S3 answers a HEAD of
a missing key with 403 rather than 404 and the probe cannot conclude. It never
deletes. A lifecycle rule that aborts incomplete multipart uploads
(`AbortIncompleteMultipartUpload`, for example after one day) is required, not
optional: the writer cannot abort an upload whose creation response it never
received; see "Limits".

### Alerts

| Signal | Meaning | Alert |
| --- | --- | --- |
| `oldest_pending.age` (buffer), per core | Age of the oldest WAL segment that still holds a bundle not yet stored by the exporter, measured from that segment's earliest ingestion, 0 when the WAL is drained: the freshness bound from inside the engine, at segment granularity, so it can read high while the segment's older bundles are already stored. It counts from ingestion, including for bundles replayed from the WAL after a restart; a segment already on disk at startup counts from its finalization, so the first minutes after a restart can read low (see the buffer's telemetry.md). Healthy it stays below `window.interval` plus the flush. | above 2 x `window.interval` for a few minutes |
| Newest values object per writer (external) | End-to-end freshness per producer: list the current hour's `dataset=values` prefix; object names carry the writer id, and `max(time)` per `producer_id` over the newest objects gives it. It also covers data that never reached the buffer. | older than 2 x `window.interval` plus the flush |
| `storage.bytes.used` / `storage.bytes.cap` (buffer), per core | WAL fill; each core's cap is its share. Growing while input is steady means the exporter is not keeping up. | above 50 percent; UNAVAILABLE to producers at 100 |
| `items.queued`, `in.flight` (buffer) | Items waiting in the WAL and bundles handed to the exporter; both grow when the store stalls. | growing for several windows |
| `oldest_unacked.age` (exporter) | Age of the oldest request the exporter currently owes the buffer. It does not see bundles still waiting in the WAL, and a failed block's requests restart it, so it is not a freshness bound; `oldest_pending.age` is. Healthy it stays below `window.interval` plus the flush (19 s measured maximum at 15 s windows). | above 2 x `window.interval` for a few minutes |
| `ingest.failures{failure=backpressure}` (buffer) | Requests refused because the WAL is full. | any |
| `block.write_failures{error.type=deadline\|cancelled}` (exporter), `retries.scheduled` (buffer) | Blocks the store did not take in time; the buffer retries them. | sustained |
| `block.write_failures{error.type=permanent_storage}` | Blocks the store refused for good (for example HTTP 403); the buffer retries them and they keep failing. | any |
| `block.write_failures{error.type=encode\|internal}`, `nacks{error.type=internal}` | Blocks or requests failed by the exporter itself; see the `series_parquet.flush.failed` and `series_parquet.request.failed` logs. | any |
| `notify.failures`, `notify.lost_acks` | Completions the engine would not accept: the buffer keeps the bundle in flight until a restart replays it, and a strict producer resends after its timeout. A lost Ack stores a written block again. | any |
| `flush.abort_failures` | Multipart uploads possibly left to the lifecycle rule. | any |
| `flush.late_commits` | Objects a flush cleanup found that the write had not confirmed, by `outcome` (see [Telemetry](#telemetry)); `stored` means a nacked block's rows may be stored twice. | `stored`, `partial` or `unknown`: any, for investigation |
| `resolved{outcome=permanently_rejected\|conversion_failed}` (buffer) | Data dropped after the WAL acknowledgement. | any |
| `loss.bundles`, `loss.items` (buffer) | Dropped by `drop_oldest` or expired by `max_age`, when set. | any |
| `receiver.otlp.requests.rejected{error.type=concurrency_limit}` (receiver) | Requests refused RESOURCE_EXHAUSTED with a RetryInfo delay at `max_concurrent_requests`; Alloy retries them. | sustained |
| `receiver.otlp.requests.rejected{error.type=rate_limit}` (receiver) | Requests refused RESOURCE_EXHAUSTED with a RetryInfo delay by the receiver's rate limit, including a message above its burst under memory pressure; Alloy retries them. | sustained |
| `receiver.otlp.requests.rejected{error.type=memory_pressure}` (receiver) | Requests refused RESOURCE_EXHAUSTED with a RetryInfo delay while the engine's memory limiter reports pressure; Alloy retries them. | sustained |
| `receiver.otlp.requests.rejected{error.type=payload_too_large}` (receiver) | Messages above `max_decoding_message_size`, refused INVALID_ARGUMENT and dropped by the producer. | any |
| Alloy `otelcol_exporter_send_failed_log_records_total`, "Dropping data" log lines | Batches Alloy gave up on (a permanent status such as INVALID_ARGUMENT). | any |
| Alloy `otelcol_exporter_enqueue_failed_log_records_total` | Records refused by a full queue; stays zero with `block_on_overflow`. | any |
| Alloy `loki_process_truncated_fields_total{field="line"}` | Lines cut to 512KiB by the truncate stage. | any, for investigation |

### Failure behaviour

Measured with eight Alloy producers at 40k lines/s on one worker, 15 s windows,
every line read back by DuckDB and clickhouse-local.
Without faults, a 30-minute run stored 74.4M lines once, with acknowledgements
at p50 <= 25ms and p99 <= 250ms, freshness p50 9.7s and p99 17.5s, and flat
RSS (0.47 GB median, 0.72 GB peak) and WAL (0.3 GB).

| Failure | Producer sees | Data | Measured |
| --- | --- | --- | --- |
| Object store down | Nothing: acknowledgements continue from the WAL (p99 up to 2.5s while the worker also retries blocks). | The exporter fails each block after `flush_retry_deadline`; the buffer retries it until the store returns. Freshness grows with the outage and recovers within about 20s of the store's return. | 150s on MinIO and RustFS: no loss, no duplicates, no Alloy error; WAL peak 2.6 GB, RSS 1.9 GB held, 2.6 GB peak. |
| WAL full (store down past the cap) | UNAVAILABLE; Alloy retries every 5s at most, its queue fills in about 3s and the tailer parks, so new lines wait in the log file. | Nothing is dropped while the file keeps them: a log rotation that removes an unread file before Alloy reaches it loses it. | 1GiB cap at 40k lines/s: full 53s after the store stopped (steady WAL 0.3 GB), refused for 90s, 384 UNAVAILABLE answers, no loss or duplicates, backlog drained 27s after the store returned. The shipped 32GiB absorbs about 34 minutes at 40k lines/s. |
| Engine restart (SIGTERM) | UNAVAILABLE while no engine listens; Alloy retries. | The receiver drains, the exporter writes its ACTIVE block, and the buffer records every acknowledgement before exiting; what is left stays in the WAL. | Exit in 0.16s with code 0, no duplicates, no loss. |
| Engine SIGKILL | UNAVAILABLE until the new engine listens; the exports in flight are resent. | Everything acknowledged is in the WAL and is written after the restart. Duplicates: an export whose WAL write completed but whose answer was lost is stored twice (at most `num_consumers` exports per producer), and so are WAL entries acknowledged within the last 100ms, or a block committed within the last 100ms, since the WAL position and the acknowledgements are persisted on that tick. | 8 kills on MinIO and RustFS, mid-window, 0.4s after a block commit and during a flush: no loss; 4000 duplicate lines after one kill (one resent export), none after the other seven. A kill within 100ms of a commit was not produced. |
| Alloy restart (`docker stop`, 10s grace) | Nothing. | Alloy saves its file positions and its queue on the way down. | All eight producers restarted: no loss, no duplicates. |
| Alloy SIGKILL with a full queue (engine down) | Nothing. | The file-backed queue survives; lines read after the last saved position (every 10s) are read again. | No loss; 82,963 duplicate lines over eight producers (about 2s of input each). The same kill with the queue in memory lost 40,949 lines (4.4k to 5.7k per producer). |
| Long lines | Nothing. | Exports are split at 2MiB; a line above 512KiB is cut to 512KiB, suffix included, and counted in Alloy's `loki_process_truncated_fields_total`. Without the cut, a record above `ingress.max_row_bytes` refuses its whole export after the WAL acknowledgement (`resolved{outcome=permanently_rejected}`). | 4000 lines of 8 KiB (34 MB, one batch): 16 exports of 2.09 MB and one of 0.54 MB, every line stored once; three 3 MiB lines stored truncated. With 4 KiB lines through a 150 s store outage: no loss, no duplicates, the backlog replayed at about 50k lines/s (220 MB/s), RSS peak 4.0 GB. |
| Receiver slots exhausted | RESOURCE_EXHAUSTED with a RetryInfo delay; Alloy retries with backoff. | Nothing is lost; producers slow down, and an export that waits past Alloy's timeout is resent and may be stored twice. Keep the producers' `num_consumers` per worker at most `max_concurrent_requests` for throughput (see "Sizing"). | One-slot receiver, three Alloy producers, 240s: no batch dropped, every line stored; extra copies from exports that timed out while queued behind the one slot. |
| A store that applies abandoned requests (RustFS) | Nothing. | A multipart completion held by an intermediary can be applied after the writer gave up and retried; the retry wrote the same names and bytes, so readers see one copy, but a partition can receive a write later than `window.interval + 2 * (flush_retry_deadline + upload.abort_timeout)` (55s here). | Seen on RustFS with a completion held in a proxy (FORMAT.md, "Partition lateness bound"); not reproduced here. |

## Reading and schema changes

Values rows reference repeated descriptors, so a reader joins them through a
view that keeps the latest descriptor per `series_id`, filters metrics on the
descriptor's `metric_type`, and never classifies a values row by which of its
columns are null. The
[series-lake README](../../../../series-lake/README.md#reading-the-data) has
the DuckDB, ClickHouse and Spark recipes and the `schema_fingerprint` check.
Adding nullable or denormalized columns is supported with `union_by_name` or
`mergeSchema`; changing an existing column's type, path or meaning requires a
new base URI.

## Throughput

A block's merge, Parquet and ZSTD encoding and upload run on the worker's own
thread, the one that admits requests and delivers acknowledgements, so each
worker core bounds its ingest rate. The best sustained rate measured is 684k
records/s on four worker cores (171k per core; 255k with one worker), strict
with 4096 receiver slots into local storage; 448k into MinIO and 360k into
RustFS. Encoding takes 33 to 39 percent of the CPU of logs. A flush that takes
longer than `window.interval` closes admission until it ends (`admission.closed`
reads 1); at 722k to 760k records/s on four workers the hottest worker reached
that point. For more, run more workers or engines. Behind `durable_buffer` the
WAL device is the limit first (see "Sizing" under
[Deploying with Alloy](#deploying-with-alloy)).

## Request cost

Object storage requests are the fixed cost of a short window. Assumptions: S3
Standard, PUT at 0.005 USD per 1000 requests, a 30-day month, one writer per
pipeline worker, and one values file per signal and window below
`upload.part_bytes`, so one PUT. With a stable series set, `series` adds 720
PUT/month/writer, one per hour partition.

| Window interval | Values files/hour/writer | PUT/month/writer | USD/month/writer | USD/month, 100 writers | USD/month, 1000 writers |
| --- | ---: | ---: | ---: | ---: | ---: |
| 5 s | 720 | 519120 | 2.60 | 260.00 | 2600.00 |
| 15 s | 240 | 173520 | 0.87 | 87.00 | 870.00 |
| 20 s | 180 | 130320 | 0.65 | 65.00 | 650.00 |
| 30 s | 120 | 87120 | 0.44 | 44.00 | 440.00 |
| 60 s | 60 | 43920 | 0.22 | 22.00 | 220.00 |
| 120 s | 30 | 22320 | 0.11 | 11.00 | 110.00 |

Every window with a new series adds one PUT, so churn dominates at short
windows; logs plus metrics roughly doubles the total; a file above
`upload.part_bytes` is a multipart upload (Create, one UploadPart per part,
Complete). Below about 15s files become too small for efficient row groups.

## Telemetry

Every metric set is registered as `exporter.series_parquet` and reported on
every `CollectTelemetry` message and once more at the node's terminal state.
Gauges are sampled on collection; anything that must not be missed between
collections is a counter.

| Metric | Unit | Description |
| --- | --- | --- |
| `series_cache.entries` | `{entry}` | Series ids the bounded descriptor cache holds. |
| `series_cache.hits` | `{lookup}` | Lookups that found a committed descriptor. |
| `series_cache.misses` | `{lookup}` | Lookups that did not. |
| `series_cache.evictions` | `{entry}` | Entries dropped because the bound was reached. |
| `block.active` | `By` | Bytes the ACTIVE block has charged. |
| `block.flushing` | `By` | Bytes the FLUSHING block charged when it was sealed. |
| `block.pending` | `By` | Bytes the one parked request retains. |
| `block.requests_pending` | `{request}` | Requests the worker still owes a decision. |
| `block.pending_slot_occupied` | `{slot}` | Whether the single parking slot is occupied. |
| `flush.duration` | `s` | Wall time one flush took, from rotation to completion. |
| `flush.retries` | `{attempt}` | Write attempts beyond the first of their flush, counted as each starts. |
| `flush.cancelled` | `{flush}` | Flushes that failed because the write was cancelled. |
| `flush.abort_failures` | `{upload}` | Multipart uploads a failed write attempt, retried or not, may have left to the bucket's lifecycle rule: the abort failed or timed out, CreateMultipartUpload got no definite answer, a HEAD could not tell whether a lost completion was applied, or the write did not unwind by the cleanup cutoff. Alert on it; each has a WARN `abort_failed` cleanup event naming the key. It counts failed attempts, not uploads: the object store client retries a CreateMultipartUpload up to `retry.max_retries` times inside one attempt and the store may create an upload on each, so one counted failure can stand for up to `retry.max_retries + 1` incomplete uploads; a CreateMultipartUpload answered with a 5xx is counted like one without an answer. Creations retried inside an attempt that then succeeds are not counted at all; only the `AbortIncompleteMultipartUpload` lifecycle rule guarantees no incomplete upload is kept. |
| `flush.late_commits` | `{flush}` | Flush cleanups that found objects the write had not confirmed, by `outcome`: `stored` (a failed flush whose every object exists; its nacked rows may be stored twice), `partial` (only some objects exist), `unknown` (a HEAD failed or did not finish by the cleanup cutoff), `acknowledged` (a lost completion response the probe found committed: on the block's first attempt, or on a retry whose abort was answered `NotFound`; the block was acknowledged). |
| `acks` | `{message}` | Requests acknowledged as durable. |
| `notify.queued` | `{request}` | Decided completions still waiting to be delivered. |
| `notify.token_size` | `By` | Bytes the undelivered completions retain. |
| `notify.failures` | `{request}` | Completions the engine would not accept. |
| `notify.lost_acks` | `{request}` | The Acks among `notify.failures`; each stores a written block again. |
| `oldest_unacked.age` | `s` | Age of the oldest completion the worker still owes. |
| `admission.closed` | `{state}` | 1 while the node is not taking requests from its input channel. |
| `admission.closures` | `{closure}` | Times admission went from open to closed. |
| `admission.closed.duration` | `s` | Total time admission has been closed. |
| `timestamp.out_of_range` | `{timestamp}` | Point timestamps outside the representable range. |
| `memory.budget` | `By` | Bytes the configuration allows this worker to hold. |
| `memory.accounted` | `By` | Bytes the worker is accounted as holding now. |
| `flush.workspace` | `By` | Bytes the write in progress holds beside its block and merge keys (merge chunk, in-progress row group, unacknowledged upload bytes); included in `memory.accounted`. |

| Metric | Unit | Label | Values |
| --- | --- | --- | --- |
| `flushes` | `{flush}` | `reason` | `time`, `bytes`, `requests`, `shutdown` |
| `block.write_failures` | `{block}` | `error.type` | `deadline`, `permanent_storage`, `cancelled`, `encode`, `internal` |
| `nacks` | `{message}` | `error.type` | `storage`, `request_too_large`, `extracted_too_large`, `row_too_large`, `too_many_series`, `token_too_large`, `too_deep`, `invalid`, `unsupported`, `shutdown`, `internal` |
| `rows.written`, `files.written` | `{row}`, `{file}` | `signal`, `dataset` | `logs`, `metrics`; `series`, `values` |
| `series.emitted` | `{row}` | `reason` | `new`, `partition`, `rotation` |
| `dropped.unsupported` | `{row}` | `kind` | `exp_histogram`, `summary`, `empty` (one per metric without data) |
| `dropped.exemplars` | `{exemplar}` | `signal` | `metrics` |
| `repaired.invalid_utf8` | `{value}` | `signal` | `logs`, `metrics` |
| `denormalize.type_mismatch` | `{value}` | `column` | one configured physical column name |

The `*_too_large` values of `nacks` name `ingress.max_request_bytes`,
`ingress.max_extracted_bytes` (the extracted request, decoded attributes
included) and `ingress.max_row_bytes` (one row, attribute value or CBOR cell);
`too_many_series` is `ingress.max_series_per_request`, `token_too_large` the
4KiB completion-token allowance, and `too_deep` `ingress.max_nesting_depth`. The
`column` label is fixed by configuration, so no request can add a label value.
The node also registers the shared `exporter.exports` set (`messages`,
`duration`; labels `signal` and `outcome`: `success`, `refused`, `failure`).

`series.emitted{reason=rotation}` rising means early rotations re-emit
descriptors: raise `window.max_block_bytes` or `window.max_requests_per_block`.
`series_cache.misses` or `series.emitted{reason=new}` rising with the record
rate or with restarts means an attribute in the identity is volatile; see
[Limits](#limits) for what to remove.
While `admission.closed` reads 1 the upstream receiver refuses producers with
its own limits; a rising `admission.closed.duration` with `flush.duration` near
the window interval means the destination is the limit.

### Events

| Event | Level | When |
| --- | --- | --- |
| `series_parquet.start` | INFO | Once per worker: `writer_id`, `boot_id`, `storage`, `num_cores`, `memory_budget_bytes`, `logs_identity_config` and `logs_identity_config_hash` (the `identity_config` file metadata of logs, FORMAT.md section 5). |
| `series_parquet.writer_id.default` | WARN for the first worker of the process, DEBUG after | At start, when `writer_id` is left at its default `writer`: object names stay unique through the boot id, but an external freshness check keyed on the writer id cannot tell the engines apart. |
| `series_parquet.memory_budget.oversubscribed` | WARN for the first worker of the process, DEBUG after | At start, when `memory.budget` times the engine's cores exceeds the memory the process may use: the cgroup (container) memory limit when it is lower than physical memory, else physical memory. `memory_limit_source` says which. |
| `series_parquet.upload.parts_exceed_limit` | WARN for the first worker of the process, DEBUG after | At start, when a file of `window.max_block_bytes` would need more than 10,000 parts of `upload.part_bytes`: `max_block_bytes`, `part_bytes`, `parts`, `max_parts`. |
| `series_parquet.receiver_limit.unverified` | INFO for the first worker of the process, DEBUG after | At start: the upstream receiver's `max_decoding_message_size` is not visible to the exporter and must reach `max_request_bytes`; `receiver_default_bytes` is the receiver's 4MiB default. |
| `series_parquet.payload.not_returned` | WARN, once per process | The first request whose route asks for its payload back with a nack, such as through `processor:retry`, which cannot retry through this exporter. |
| `series_parquet.request.failed` | WARN | A refusal, at most one line per second for each `outcome`, so a flood of one kind does not hide another: `outcome`, `signal`, `reason`, `error`, `suppressed` (lines of that outcome left out since the last), the size fields of a size refusal, and `producer_id`, the first value of `producer_id_attribute` in the request cut to 128 bytes with control and format characters escaped (`\n`, `\u{1b}`, `\u{202e}`), when the request was converted far enough to read it (not for a refusal by size or signal). The id is the sender's own claim, not an authenticated identity. |
| `series_parquet.flush.attempt` | DEBUG, INFO on a retry | Before each write attempt: `seq`, `attempt`, `file`, `objects`, `deadline_remaining`. |
| `series_parquet.flush.attempt_failed` | WARN | After each failed write attempt: `seq`, `attempt`, `file`, `retryable`, `deadline_remaining`, `error`. |
| `series_parquet.block.committed` | INFO | A block is durable: `window_start`, `seq`, `path`, `files`, `requests`, `bytes`, `attempts`, `duration`. |
| `series_parquet.flush.failed` | ERROR | A block failed and every request in it is nacked as retryable: `window_start`, `seq`, `file`, `requests`, `bytes`, `attempts`, `error_type`, `error`. |
| `series_parquet.flush.cleanup` | WARN, INFO for a late commit or a partial block | How the write of a failed flush ended: `outcome`, `seq`, `attempt`, `file`. `outcome` is a `flush.late_commits{outcome}` label: `stored` (INFO: every object of a nacked block exists), `acknowledged` (INFO: a lost completion committed the object and the block was acknowledged), `partial` (INFO: `present` of `objects` exist) or `unknown` (WARN, with `probe_error`: a HEAD failed or did not finish by the cleanup cutoff); or `abort_failed` (WARN, counted in `flush.abort_failures`, with `abort_error` naming the object key, or every key of the block when the write did not unwind; the upload id is not logged, as the object store client does not expose it); a clean abort is DEBUG `aborted`. |
| `series_parquet.retry_ignored_for_file_storage` | WARN | A `retry` section was given for local file storage, where it is validated and not applied. |
| `series_parquet.seal.failed` | WARN | A block could not be sealed. |
| `series_parquet.flush.task_failed`, `series_parquet.flush.cleanup_failed` | WARN | The write task or its cleanup panicked or was lost; the block's requests are nacked as `internal`, retryable. |
| `series_parquet.notify.failed` | WARN | A completion the engine would not accept, at most one line per second with `suppressed` naming the lines left out; `notify.failures` counts every one. |
| `series_parquet.inbox.failed` | WARN | The input channel failed. |
| `series_parquet.shutdown` | INFO | The Shutdown control message arrived. |
| `series_parquet.shutdown.cut` | WARN, INFO when nothing was held | The flush cut, 1.2s before the shutdown deadline, decided what was still held: `held` (a block, a parked request or an undelivered completion). A cut with nothing held logs it at INFO. |
| `series_parquet.shutdown.deadline_exceeded` | WARN | The shutdown deadline elapsed with completions still undelivered; they are counted in `notify.failures`. |
| `series_parquet.shutdown.complete` | INFO | The worker ended: `accepted`, `acked`, `nacked`, `abandoned`, `deadline_exceeded` (as the event above), `duration`. |

## Memory

Per worker, with B `window.max_block_bytes`, E `ingress.max_extracted_bytes`,
C `series_cache.max_entries`, N `window.max_requests_per_block` and T the
measured completion-token size, retained data is bounded by
`2B + E + 128C + 2NT`: two blocks, one parked request, the cache and at most
`2N - 1` live tokens. Blocks fill to B before a byte rotation, so under
sustained load retained memory approaches `2B` plus the flush workspace. The
workspace allowance is `4 * ingress.max_request_bytes` (conversion),
`2 * sorting.run_target_bytes` (sorting), `2 * sorting.merge_chunk_bytes`
(merge and encoding), `3 * parquet.writer_limit_bytes` (writer),
`upload.part_bytes * (upload.concurrency + 1) + sorting.merge_chunk_bytes`
(upload) and 64MiB of overhead; these are reservations, not measured
ceilings. `memory.accounted` reports the retained data, tokens, cache and,
during a flush, the merge keys and `flush.workspace` of the write.

The whole process needs more than the sum of these budgets. The upstream OTLP
receiver keeps every request it has not answered in memory, bounded by its
`max_concurrent_requests` slots and not by bytes, and without
`durable_buffer` it answers only once the request's block is durable. Size a
strict deployment for

```text
workers * (memory.budget + receiver slots * maximum request size)
```

with one worker per core and `ingress.max_request_bytes` as the maximum
request size. The receiver lowers `max_concurrent_requests` to the pipeline's
`pdata` channel capacity. With the 128 slots of `series-parquet-local.yaml`
and 16MiB requests the receiver term is at worst 2 GB per worker; the 2048
slots `series-parquet-s3.yaml` sizes for 100k records/s hold about 100 MB of
512-record requests, but up to 32 GB of 16MiB ones, so lower
`ingress.max_request_bytes` and the receiver's `max_decoding_message_size`
together to the largest request your producers send. With 1.216M records/s
offered to 4096 slots on each of four workers, more than they could take,
jemalloc held 16.2 GB (RSS 17 GB) while the exporters accounted 3.4 GB; the
rest was requests held by the receivers, 4096 slots times four workers times
about 1 MB. Raising `max_concurrent_requests` to lift the
strict admission ceiling multiplies this term. Behind `durable_buffer` the
receiver holds a request only until the buffer has written it to its WAL, so
the term lasts for the WAL write rather than for the window, and the buffer
adds the bundles it has handed to this exporter (see "Sizing" under
[Deploying with Alloy](#deploying-with-alloy)).

On glibc Linux the engine runs jemalloc with its background purging thread
(`background_thread on` at startup); `MALLOC_CONF` overrides it.

## The v1 acknowledgement contract

An acknowledgement promises where the request is, not that it is stored
exactly as sent.

- **Strict** (`series-parquet-s3.yaml`, `series-parquet-local.yaml`): OK means
  every file of the request's block completed in the store (see "Delivery
  and shutdown"; the local backend does not `fsync`).
- **Buffered** (`series-parquet-buffered.yaml`): OK means the request is in
  this host's WAL; the buffer retries until this exporter acknowledges it
  under the strict meaning. What can be lost after that OK is listed in
  "Running behind durable_buffer" and "Durability of the acknowledgement".
- **At least once**: a retry after a lost acknowledgement stores rows again.
- **Stored as accepted**: an acknowledged request can be stored in part or
  altered, as "What this exporter does not keep" lists. The shipped
  configurations choose this explicitly with `unsupported: drop` and
  `metrics.exemplars: drop`; with `reject` such a request is refused as a
  whole (INVALID_ARGUMENT, or behind the buffer a permanent drop counted in
  `resolved{outcome=permanently_rejected}`). These alterations are counted:
  `timestamp.out_of_range`, `dropped.unsupported{kind}`, `dropped.exemplars`,
  `repaired.invalid_utf8{signal}` and `denormalize.type_mismatch{column}`;
  the ones the table below marks "no counter" are not.

## What this exporter does not keep

For data this exporter accepts, this is the complete loss list; losses before
it and behind `durable_buffer` are in "Producers" and "Running behind
durable_buffer".

| What | Kept instead | How it shows |
| --- | --- | --- |
| Traces | Nothing: the request is refused. | `nacks{error.type=unsupported}` |
| Exponential histogram and summary points | By default the request's other points. Under `unsupported: reject`, nothing: the request is refused. | `dropped.unsupported{kind}` per point, or `nacks{error.type=unsupported}` |
| A metric without data (no gauge, sum, histogram, exponential histogram or summary set) | The request's other points; the metric has none. | `dropped.unsupported{kind=empty}` per metric |
| Exemplars, with their filtered attributes, trace and span ids | By default the point, without its exemplars. Under `metrics.exemplars: reject`, nothing: the request is refused with a reason naming exemplars. | `dropped.exemplars{signal=metrics}` per exemplar, or `nacks{error.type=unsupported}` |
| Attribute value types in the `attrs`, `resource_attrs` and `scope_attrs` maps | Every value rendered to a string by `render_v1`, so `"42"` and `42` read the same. Identity attributes keep their types in `identity_bytes` and `series_id`, and a typed denormalized column keeps one. Log record attributes outside `logs.series_attributes` keep none. | Documented, by format decision; no counter |
| The type of a log body that is not a string | The body rendered to JSON text, so `"42"` and `42` read the same; a bytes body is a quoted base64 string. | Documented; no counter |
| An optional metrics value, such as a histogram `sum`, `min` or `max`, that is exactly zero in every point of a request | Null: the OTAP transport omits a column whose every entry in a request is the type default. | Documented; no counter |
| A timestamp of zero, or one that does not fit `i64` nanoseconds | Null in both timestamp columns. | `timestamp.out_of_range` for the ones that do not fit |
| Invalid UTF-8 in a top-level string value | The value with U+FFFD, so strings that differ only in invalid bytes share a series id. | `repaired.invalid_utf8{signal}` |
| A denormalized attribute of the wrong type | Null in its typed column; the attribute stays in its map and in the identity. | `denormalize.type_mismatch{column}` |
| Metric metadata attributes | Nothing: they are never read or validated. | Documented; no counter |
| `dropped_attributes_count` of a resource, a scope, a log record or a point | Nothing: no dataset has a column for it. | Documented; no counter |
| Arrival order | Values rows are sorted by `values_sort`; rows with equal sort keys keep no defined order. | Documented |

Values rows are never deduplicated: a retry after a written block whose ack
was lost stores the rows twice. A descriptor is written once per block and not
again in a partition where the cache holds it as committed; an eviction, a
new partition or an early rotation writes it again (`series.emitted{reason}`).

## Limits

- Traces are refused on the signal alone. Unspecified temporality, duplicate
  attribute keys, excessive nesting, invalid histogram list lengths and counts
  above `INT64_MAX` refuse the whole request; duplicate keys are checked in
  the resource, scope, log record and supported point attributes.
- An OTLP body is read by the engine's conversion to Arrow records, without a
  separate check of its protobuf framing: a body the conversion cannot decode
  is refused as `invalid`, and one it reads only in part is stored as the
  records it read. Invalid UTF-8 inside an array or key-value list value is
  refused as undecodable. Nesting beyond `ingress.max_nesting_depth` is
  refused after conversion.
- The conversion to OTAP records takes at most 65,536 attributed log records
  or metrics per request, and as many scopes and resources; a larger request
  is refused whole, permanently. Keep producer batches within it: the shipped
  Alloy configurations cap every batch at 4,000 (`send_batch_max_size`).
- Dictionary-encoded columns are read through their dictionary. Every
  attribute key and value is charged as it is read: one longer than
  `ingress.max_row_bytes` refuses the request, and decoded attributes count
  against the same `ingress.max_extracted_bytes` as the extracted rows.
- A bucket lifecycle rule for incomplete multipart uploads (S3
  `AbortIncompleteMultipartUpload`, for example after one day) is required and
  is the only guarantee against incomplete uploads. The writer aborts the
  upload of every failed or cancelled write, but an abort can fail, a creation
  can fail without an answer that tells whether the upload exists, and a
  completion still in flight may be applied after the abort.
  `flush.abort_failures` is the alert signal but counts failed attempts, not
  uploads (see [Telemetry](#telemetry); measured: 54 uploads behind 9 counted
  failures with `max_retries: 5`). The WARN event names the object key but not
  the upload id, which the object store client does not expose; list the
  uploads of that key to find it. On a versioned bucket, each retry that
  reached the store leaves a noncurrent version.
- Writer limits (merge keys held during a merge, the average-based merge chunk,
  row-group upload bursts) are in the
  [series-lake README](../../../../series-lake/README.md#limitations-in-version-1),
  and format limits in
  [FORMAT.md](../../../../series-lake/docs/FORMAT.md#limitations-of-version-1).
- There is no compaction, discovery index, manifest, producer replay id or
  introspection endpoint beyond the metrics above.
- A flush runs on the worker's own core in bounded steps between which the
  loop admits requests, delivers completions and watches the shutdown
  deadline. Encoding one chunk (`sorting.merge_chunk_bytes`) and closing one
  row group (`parquet.row_group_bytes`) are not sliced; at the defaults the
  longest step measured about 25 ms.

A series identity is the complete set of resource, scope, metric and point
attributes, so a point attribute unique per point, such as a request id, makes
one series per point and the cache stops hitting (`series_cache.misses` rises
with the point rate). A resource or scope attribute that changes per process,
such as `process.pid`, `service.instance.id` or `k8s.pod.uid`, starts a new
set of series on every restart: `series.emitted{reason=new}` rises with the
restarts, and a reader's series count grows with them. Remove such attributes
upstream, in the SDK or with `processor:attribute`:

```yaml
processor:
  type: processor:attribute
  config:
    apply_to: ["signal"]
    actions:
      - {action: delete, key: request.id}
```

Streams that differed only in the deleted attribute then share one
`series_id` and their points stay separate values rows: delta sums and
histograms stay correct when summed, cumulative metrics and gauges become
ambiguous, so delete attributes only of delta metrics.

## Related Docs

- [Lake format](../../../../series-lake/docs/FORMAT.md): the normative on-disk
  format, identity encoding and compatibility rules.
- [series-lake crate](../../../../series-lake/README.md): the
  engine-independent writer this exporter embeds.
- [Core nodes catalog](../../../README.md): every built-in node and its
  feature gate.
- [Configuration guide](../../../../../docs/configuration.md): writing runtime
  YAML.
