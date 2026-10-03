# Series Lake Format, version 1

This document specifies what `exporter:parquet_lake` writes: series identity,
datasets, layout, file semantics and compatibility rules. The format is the
one `exporter:series_parquet` writes (its `series-lake` crate holds the
reference document); section numbers are the same in both documents. Every
difference between the two writers is listed in one place,
[Extensions and deviations](#extensions-and-deviations).

This file is normative for this writer: it describes what the writer
produces.

Golden vectors live in `../testdata/golden/`. They are copies of the
reference implementation's vectors, which an independent Python encoder
generates; this directory has no generator. `canonical_v1.json` pins the
identity encoding (section 1), `cbor_v1.json` the CBOR decoding of nested
values (section 1), `render_v1.json` the attribute-map rendering (section 2)
and `identity_config_v1.json` the `identity_config` footer value (section 5).

## 1. Series identity (canonical encoding v1)

`series_id = XXH3_128(identity_bytes)` with seed 0, stored as 16 bytes in the
xxHash canonical big-endian representation, rendered as 32 lowercase hex
characters in APIs and logs. `identity_bytes` is also stored in the `series`
row (section 2) so that `xxh3_128(identity_bytes) == series_id` can be
verified by any reader.

XXH3 with a fixed, public seed is not a cryptographic hash: it resists
accidental collisions only, not identities crafted to collide. The writer
trusts `series_id` without comparing `identity_bytes`: its series cache and
its per-request series table keep one row per id, so of two colliding series
in one partition only one row may be written, and readers joining on
`series_id` attribute both series' values to it. Every producer writing into
one lake is therefore one trust domain. A reader can find a collision by
looking for one `series_id` with two different `identity_bytes`.

`identity_bytes` is built from typed values. Every value is
`tag:u8 ++ len:u32_be ++ payload`:

```text
tag  type     payload
0x01 string   UTF-8 bytes (no normalization)
0x02 bytes    raw bytes
0x03 int64    8 bytes, two's complement, big-endian
0x04 double   8 bytes, IEEE 754 binary64, big-endian; any NaN is normalized
              to 0x7FF8000000000000 and -0.0 is normalized to +0.0
0x05 bool     1 byte, 0x00 or 0x01
0x06 null     empty payload (unset or absent value)
0x07 array    count:u32_be ++ values*         (elements keep their order)
0x08 kvlist   count:u32_be ++ (string key, value)*  sorted by key bytes
```

Strings reach this encoder as valid UTF-8: invalid UTF-8 is repaired on
ingress (see "Extensions and deviations", item 9), so the encoder itself has
no repair rule.

Attribute lists are encoded as `kvlist`. Identity fields are appended in this
fixed order:

```text
string "OTEL-SERIES/1"                     # namespace and format version
string signal                              # "logs" | "metrics"
kvlist resource attributes                 # all of them, including the one
                                           # selected as producer_id_attribute
string resource schema_url
string scope name
string scope version
string scope schema_url
kvlist scope attributes
# metrics only:
string metric name
string metric unit
string metric type            # gauge | sum | histogram | exp_histogram | summary
string temporality            # delta | cumulative | "" (gauge and summary)
bool   is_monotonic           # false for non-sum metrics
kvlist identity attributes    # metrics: data point attributes
                              # logs: always empty in this writer
```

For logs, this writer has no attribute allow-list: the identity attribute
list is always the empty kvlist, and a log series is its resource and scope.

Rules:

- Keys are sorted by raw UTF-8 bytes (`memcmp`, no locale, no case folding),
  recursively inside nested kvlists.
- Nested values come from the OTAP `ser` column and are decoded from CBOR
  before encoding, as the next subsection states. Nesting deeper than
  `ingress.max_nesting_depth` (default 32) refuses the request permanently as
  too deep, distinct from invalid content.
- A duplicate key inside any attribute list this writer reads (resource
  attributes, scope attributes, the log record's own attributes, or a data
  point's attributes) makes the request invalid: a permanent Nack with
  `NackCause::Refused`. This applies whether or not the list is part of the
  identity. Exemplar attribute payloads are never read, so a duplicate key
  inside one goes undetected (see Limitations).
- A sum, histogram or exponential histogram whose temporality is
  `Unspecified` is invalid (same outcome). Gauges and summaries encode
  temporality as the empty string.
- Missing string fields are encoded as empty strings; unset attribute values
  are encoded as `null`. `42` (int64) and `"42"` (string) are different
  series.
- An OTAP attribute value whose type tag is not `Empty` but whose value
  column is absent, or whose cell in that column is null, decodes as that
  type's default: `""`, `0`, `0.0`, `false`, empty bytes. This is the OTAP
  Arrow encoder's own space optimization (a value column whose every entry
  equals the type's default is dropped from the batch); reading it back as
  `null` would make a series identity depend on how requests happened to be
  batched, and would collide with a genuinely unset value, which stays
  `Empty -> null`. Map and slice values are the exception: even an empty CBOR
  map or array is a non-empty `ser` payload, so an absent or null `ser` cell
  under those type tags is invalid content rather than a default.
- An attribute row with a null key has the empty key `""`.
- The resource attribute chosen as `producer_id_attribute` stays in the
  identity like every other resource attribute; the `producer_id` column of
  section 2 is only a projection of it and is not hashed separately.
- Nothing else is part of the identity: timestamps, values, exemplars, body,
  severity, trace and span ids, flags, dropped counts, metric description,
  log record attributes.

### CBOR decoding

An OTAP attribute or body whose type is map or slice carries its value as
one CBOR item (RFC 8949) in the `ser` column. The writer decodes that item
into the typed values above as follows; any other input refuses the request
as invalid content, except nesting beyond the limit, which is too deep. The
same rules apply at every nesting level.

```text
CBOR item                               value
unsigned or negative integer            int64; outside the int64 range: invalid
tag 2 or 3 over a definite byte         int64 (n, or -1 - n); more than 16
  string of at most 16 bytes (bignum)     bytes, an indefinite string or a
                                          value outside int64: invalid
any other tag                           invalid
half, single or double float            double, widened exactly
false, true                             bool
null, undefined                         null
any other simple value, a lone break    invalid
byte string                             bytes
text string                             string; not valid UTF-8: repaired
array                                   array, items in order
map                                     kvlist, sorted by key bytes
```

- A map key is a text string, or `null` or `undefined`, which both become
  the empty key `""`; a key of any other type is invalid. Two keys that are
  equal after this mapping, at any nesting level, make the item invalid, so
  `{null: 1, "": 2}` is invalid.
- Byte strings, text strings, arrays and maps may have indefinite length.
  An indefinite string is the concatenation of its chunks, which must be of
  the same major type; a chunk that is itself indefinite is flattened into
  it.
- A text string or map key that is not valid UTF-8 is repaired, not refused:
  each chunk is decoded on its own, and each maximal invalid byte sequence
  becomes U+FFFD (the rule of Rust's `String::from_utf8_lossy` and of the
  WHATWG decoder). A code point split across chunks therefore becomes
  replacement characters. Keys that become equal after repair are duplicate
  keys and make the item invalid.
- Each array or map is one nesting level. An item with more than
  `ingress.max_nesting_depth` levels is too deep.
- Only the first item of the cell is decoded; bytes after it are ignored. A
  truncated item is invalid, and so is a declared length the rest of the
  cell cannot hold.

Every semantic field stored in a `series` row is either part of the identity
or explicitly listed as non-identity metadata. Non-identity metadata:
`description` (metrics), `emitted_at` and the dropped-attribute counts.
Consumers must not expect them to be constant per `series_id`.

## 2. Datasets

Four datasets, one Parquet schema each:

```text
signal=logs/dataset=series
signal=logs/dataset=values
signal=metrics/dataset=series
signal=metrics/dataset=values
```

`series_id FIXED_LEN_BYTE_ARRAY(16)` (required) is the first column of every
dataset. `producer_id STRING` (required, `""` when absent) is the second
column of every values dataset and absent from `series`.

`producer_id` is the value of the resource attribute named by
`producer_id_attribute`, rendered like an attribute map value (below).
That same resource attribute is part of the identity (section 1), so
`producer_id` is functionally determined by `series_id`: two requests that
hash to the same `series_id` carry the same value for the attribute, barring
a hash collision. `producer_id` is nonetheless a values column: it lets a
reader filter or group values rows by producer without a join. The attribute
should be stable across producer restarts and unique among concurrently
running producers (the role Thanos external labels play).

The listings below show the columns of version 1 in their order. This writer
appends further nullable columns to three of the datasets; they are listed
under [Extensions and deviations](#extensions-and-deviations). A column is
required unless marked `null`.

`series` (both signals), always sorted by `series_id`:

```text
identity_bytes      BINARY               # canonical bytes hashed into series_id
emitted_at          TIMESTAMP(us, UTC)   # time the generation was sealed
resource_schema_url STRING
resource_attrs      MAP<STRING, STRING>
scope_name          STRING
scope_version       STRING
scope_schema_url    STRING
scope_attrs         MAP<STRING, STRING>
attrs               MAP<STRING, STRING>  # metrics: data point attrs; logs: empty
# metrics series only:
metric_name         STRING
unit                STRING
metric_type         STRING
temporality         STRING
is_monotonic        BOOLEAN
description         STRING               # non-identity metadata
```

`logs/values`:

```text
time                     TIMESTAMP(us, UTC) null
time_unix_nano           INT64 null
observed_time            TIMESTAMP(us, UTC) null
observed_time_unix_nano  INT64 null
severity_number          INT32
severity_text            STRING
body                     STRING null          # rendering below; null for an unset body
event_name               STRING
trace_id                 FIXED_LEN_BYTE_ARRAY(16)  null when absent
span_id                  FIXED_LEN_BYTE_ARRAY(8)   null when absent
flags                    INT32
attrs                    MAP<STRING, STRING>  # all log record attributes
```

`metrics/values` (all point kinds share one schema):

```text
metric_name           STRING               # required, dictionary encoded
time                  TIMESTAMP(us, UTC) null
time_unix_nano        INT64 null
start_time            TIMESTAMP(us, UTC) null
start_time_unix_nano  INT64 null
flags                 INT32
value_int             INT64  null          # set when the point carries as_int
value_double          DOUBLE null          # set when the point carries as_double
count                 INT64  null
sum                   DOUBLE null
min                   DOUBLE null
max                   DOUBLE null
bucket_counts         LIST<INT64>  null    # non-null items
explicit_bounds       LIST<DOUBLE> null    # non-null items
```

The columns are the union of the point kinds and every per-kind column is
nullable:

- A number point fills `value_int` or `value_double` and leaves `count`,
  `sum`, `min`, `max`, `bucket_counts` and `explicit_bounds` null. A number
  point carrying no value at all leaves both value columns null; flags are
  stored as received and never inferred.
- A histogram point fills `count` and, when present, `sum`, `min` and `max`,
  and leaves `value_int` and `value_double` null. A histogram without a
  distribution stores two empty lists, not two null lists.
- Exponential histogram and summary points: see
  [Extensions and deviations](#extensions-and-deviations).

Classification rule: the point kind is not stored in the values row. It is
`metric_type` in the `series` row, and that row is the only authoritative
source of it. A reader classifies a values row by joining to the series row
(section 6), never by which columns are null and never by `count`. Null-ness
is a consequence of the point kind, not a definition of it, and a future
additive column could make any given column null for a kind that fills it
today.

File-level fact, which is not a reader rule: at the Parquet level a number
row's `bucket_counts` and `explicit_bounds` are null, while a bucket-less
histogram's are empty lists. The two encodings are different bytes and a
writer must produce them as stated here. They are not equally visible to
readers. DuckDB preserves the difference, so `bucket_counts IS NULL` and
`bucket_counts = []` are distinguishable there. ClickHouse has no nullable
`Array`, so its Parquet reader renders a null list as `[]` and collapses the
two cases. That is why classification uses the series row, which answers the
same in both.

Counts use signed 64-bit integers because Spark maps Parquet `UINT64` to
`DECIMAL(20,0)` while DuckDB maps it to `UBIGINT`; OTLP counts never
approach the signed limit. A count above `i64::MAX` is invalid, and so is a
null item in a bucket or bounds list.

Histogram validation: either both lists are empty, or
`bucket_counts.len == explicit_bounds.len + 1`. Anything else refuses the
request as invalid. Bound monotonicity and bucket totals are not checked;
such malformed distributions are stored as received.

Timestamps: the exporter sees timestamps as `i64` nanoseconds; absent and
zero are already conflated upstream as `0`. Values `>= 1` are stored raw in
the `INT64` column and as `TIMESTAMP(MICROS, isAdjustedToUTC=true)` after
integer division by 1000. `0` stores null in both columns. Negative values
(a `u64` above `i64::MAX` wrapped by the conversion cast) store null in both
columns and count `timestamps.out_of_range`. This applies to every timestamp
column.

`flags` stores the OTLP `fixed32` flags reinterpreted as a signed 32-bit
integer (the top bit becomes the sign bit); absent flags are `0`.

Attribute maps: `MAP<STRING, STRING>` with non-null keys and nullable values.
The format is intentionally lossy for attribute value types; identity hashing
and `identity_bytes` keep types. One recursive rendering
`render_v1(value) -> JSON value` is defined: string to JSON string; int to
JSON number; finite double to JSON number (spelled as below); non-finite
double to the JSON strings `"NaN"` (any NaN, whatever its sign or payload),
`"Infinity"` and `"-Infinity"`; bool to JSON bool; bytes to a JSON string of
standard base64 with padding (RFC 4648 section 4, alphabet `A-Z a-z 0-9 + /`,
`=` padding, no line breaks); unset to JSON null; array to JSON array; kvlist
to JSON object with keys sorted by raw key bytes. Two entry points use it:

- Attribute map value: for a top-level string the raw string; for a
  top-level unset the SQL null; for any other top-level value the compact
  JSON serialization of `render_v1` (so an int is `42`, bytes `0xab 0x12`
  are the JSON string `"qxI="` with its quotes, a double NaN is `"NaN"` and
  a positive infinity `"Infinity"`).
- Log body: for a string body the raw string; for an unset body the SQL
  null; otherwise the compact JSON serialization of `render_v1` (so a bytes
  body is the JSON string `"qxI="`, quotes included, never bare base64).

A finite double is spelled from its shortest round-trip decimal digits
`d1 d2 ... dn` (no trailing zeros) and the decimal exponent `e` of `d1`, so
that the value is `d1.d2...dn * 10^e`, with a leading `-` for a negative
value:

- When `-5 <= e <= 15`, fixed notation: the digits with the decimal point
  placed accordingly, zero-filled on either side as needed, and `.0`
  appended when there is no fractional digit. So `3.0`, `1.5`,
  `1000000000000000.0` (1e15), `0.00001` (1e-5) and `0.0000123`.
- Otherwise scientific notation: `d1`, then `.` and `d2...dn` when `n > 1`,
  then `e`, then the exponent with an explicit sign (`+` or `-`) and no
  zero padding. So `1e+16`, `-1e+16`, `1.2345678901234568e+16`,
  `1.7976931348623157e+308`, `1e-7`, `1.23e-6` and `5e-324`.
- Zero is `0.0`, and negative zero is `-0.0` (values columns and attribute
  cells store doubles as received; only the identity normalizes the sign).

This is what the writer's JSON serializer (serde_json) produces. The golden
vectors in `../testdata/golden/render_v1.json` pin it, including every base64
padding length, so a serializer upgrade that changed the spelling fails the
tests instead of silently changing stored strings.

`render_v1` is not OTLP JSON: a value is not wrapped in an `AnyValue` object
such as `{"intValue": "42"}`, integers are JSON numbers rather than strings,
and a kvlist is a JSON object rather than a list of key/value objects.

`render_v1` never feeds the series identity: section 1 hashes the typed
canonical encoding, so changing a spelling here changes stored strings but
no `identity_bytes` and no `series_id`.

An empty attribute list is an empty map, never null.

Inputs this writer does not store, and their policy:

- Exemplars are never stored and not counted; the parent point is stored.
- A metric without data has no points, so nothing is written for it.
- Traces: a traces request is refused with a permanent Nack
  (`NackCause::Refused`).
- Metrics payloads without a univariate metrics table (multivariate
  metrics) are refused.

## 3. Not available in this writer

`exporter:series_parquet` can materialize selected attributes as additional
typed columns (`denormalize`) and records a `schema_fingerprint` key in every
file. This writer has neither: no denormalized columns are written and the
`schema_fingerprint` key is absent.

Schema contract per dataset: within one `base_uri`, changes are additive
only (new nullable columns). Readers use union-by-name
(`union_by_name = true` in DuckDB, `mergeSchema` in Spark) so additive
columns read as nulls.

## 4. Layout and naming

```text
<base>/v=1/signal=logs/dataset=series/date=2026-09-21/hour=03/
    part-20260921T031500Z-<writer_id>-<boot_id>-<seq>.parquet
<base>/v=1/signal=logs/dataset=values/date=2026-09-21/hour=03/
    part-20260921T031500Z-<writer_id>-<boot_id>-<seq>.parquet
```

- `date` and `hour` (two digits, zero-padded) come from the block's
  `window_start` (ingest time on the writer's wall clock), never from event
  timestamps. The stamp after `part-` is `window_start` as
  `YYYYMMDDTHHMMSSZ`.
- `writer_id` comes from the config, validated to be non-empty and made of
  `[A-Za-z0-9_.-]` only, so it never contains a `/`. It may contain `-`:
  the stamp, `boot_id` and `seq` contain none, so a reader takes the stamp
  after `part-`, `seq` and `boot_id` from the right, and the rest is
  `writer_id`. `boot_id` is a random UUIDv4 generated at exporter start,
  written as 32 lowercase hexadecimal digits without hyphens. `seq` is the
  writer's generation counter, zero-padded to a minimum of 8 digits (not
  truncated if it ever needs more). One generation holds the logs block and
  the metrics block of a window; both carry the generation's `seq`.
- Names are fixed when the block is sealed and reused verbatim across
  retries. Retries overwrite the same names with the same bytes; `boot_id`
  makes collisions with other blocks or processes impossible in practice. No
  conditional-put semantics are used. Each object is written with one `put`.
- No manifest. Within a block the `series` file is written first, then the
  `values` file. A block has landed if and only if its `values` file exists.
  Completed objects become visible atomically per object (object store
  semantics). Within a generation the logs block is flushed before the
  metrics block.
- A block whose series all have a row in the partition already (section 6)
  writes no `series` file.
- Visibility guarantees, stated narrowly: readers may observe a block
  partially (its `series` file without its `values` file) while it is being
  written or after a permanent failure; rows of a Nacked request may exist in
  storage and appear again after the producer retries; there is no snapshot
  consistency across files. Referential coverage (section 6) holds for every
  visible values file because its series rows were written earlier.
- No files are written for a dataset with zero rows; an empty window writes
  nothing.

## 5. Parquet options

ZSTD; a row group is closed when the encoder holds about 8 MiB; statistics
enabled at the page level; Parquet writer version 1.0; dictionary encoding
for strings. The mostly distinct columns `body`, `attrs.entries.values`,
`trace_id`, `span_id` and `identity_bytes` are written without a dictionary
and with column-chunk statistics only. The top-level fixed-width columns
(`series_id`, timestamps, flags, counts, point values) are written plain as
well: their values are mostly unique, and a 1.0 writer does not
dictionary-encode `FIXED_LEN_BYTE_ARRAY` in any case; the sort keeps equal
`series_id` values adjacent for the compressor.

Key/value metadata of every file: `format_version=1`,
`series_hash=xxh3_128/canonical_v1`, `sort_key` (comma-separated
`column:asc|desc:nulls_first|nulls_last`), `writer_id`, `boot_id`, `seq`,
`window_start`, `window_end` (`window_start + window.interval`, also for
generations rotated before their window ended), `row_count`,
`identity_config`, `identity_config_hash`, and in values files only
`min_time_unix_nano` and `max_time_unix_nano`, present only when the file
has at least one row with a non-null `time_unix_nano`. Every value is a
string: `writer_id` and `boot_id` as in the object name, and `seq`,
`window_start`, `window_end`, `row_count`, `min_time_unix_nano` and
`max_time_unix_nano` decimal integers without padding (`window_*` in Unix
seconds, the times in Unix nanoseconds).

`sort_key` is `series_id:asc:nulls_last` in series files and
`series_id:asc:nulls_last,time_unix_nano:asc:nulls_last` in values files.
Rows of a file are in that order; rows with equal keys keep their arrival
order.

`identity_config` records the writer settings that select identity fields
(section 1), so a reader can tell files whose `series_id` values are
comparable. It is compact JSON with no whitespace, as the golden vectors in
`../testdata/golden/identity_config_v1.json` pin: for logs
`{"series_attributes":[]}` (this writer has no allow-list, which is the empty
list); for metrics `{}`, since every data point attribute is in the identity.
`identity_config_hash` is xxh3_64 (seed 0) over the UTF-8 bytes of
`identity_config`, 16 lowercase hexadecimal digits. Both are the same in the
series and values files of one signal. `producer_id_attribute` is not part
of it: it selects no identity field (section 2).

Together `(signal, dataset, partition, format_version, sort_key,
identity_config_hash)` and the column set define a compaction scope: files in
the same scope can later be merged by a k-way merge without re-sorting; files
in different scopes are never merged together. Compaction itself is out of
scope for this writer: it only writes new files (any number of writer
processes may run concurrently) and never merges or rewrites existing ones.

Native sort metadata: every row group also carries Parquet's standard
`sorting_columns` field, so generic readers (DataFusion, DuckDB, ClickHouse)
can use the order without knowing this format. `column_idx` counts Parquet
leaf columns, not top-level columns: a `MAP` column has two leaves, so every
column after one is shifted. Series files carry `series_id` (leaf 0)
ascending, nulls last; values files carry `series_id` (leaf 0) and
`time_unix_nano`, both ascending, nulls last: leaf 3 in logs, leaf 4 in
metrics, where `metric_name` precedes it. The `sort_key` key/value stays the
complete, authoritative description of the order.

## 6. Reading the data

Because series rows repeat (cache eviction, new partition, restart, several
writers), a join on `series_id` must go through a canonical view. The
coverage guarantee below puts a series row in the partition of every values
row, so the view picks one series row per series and partition, and the join
matches the partition too; a query reads only the partitions it needs:

```sql
SELECT * FROM read_parquet(
    '<base>/v=1/signal=logs/dataset=series/date=2026-09-21/*/*.parquet',
    hive_partitioning = true, union_by_name = true, filename = true)
QUALIFY row_number() OVER (PARTITION BY series_id, date, hour
                           ORDER BY emitted_at DESC, filename DESC) = 1
```

For metrics, build the same canonical view over
`signal=metrics/dataset=series` and join the single
`signal=metrics/dataset=values` dataset on `series_id` and the partition. The
join supplies the point kind for every row, so no union of per-kind datasets
is needed:

```sql
WITH series AS (
  SELECT * FROM read_parquet(
    '<base>/v=1/signal=metrics/dataset=series/date=2026-09-21/*/*.parquet',
    hive_partitioning = true, union_by_name = true, filename = true)
  QUALIFY row_number() OVER (PARTITION BY series_id, date, hour
                             ORDER BY emitted_at DESC, filename DESC) = 1
)
SELECT v.*, s.metric_type
FROM read_parquet(
    '<base>/v=1/signal=metrics/dataset=values/date=2026-09-21/*/*.parquet',
    hive_partitioning = true, union_by_name = true) AS v
JOIN series AS s USING (series_id, date, hour)
```

Filter on `s.metric_type` to read one point kind. That join is the only
supported way to classify a values row; `v.count IS NOT NULL` and the
null-ness of any other per-kind column are not substitutes for it, for the
reasons in section 2.

Series coverage guarantee: for every values row in partition `P` written by
writer process `W` (one `writer_id` and `boot_id`), at least one series row
for the same `series_id` exists in the `series` dataset of the same signal in
partition `P`, written by `W`, and that row became visible before the values
file. A reader that lists values files first and series files second
therefore always finds series rows for what it read.

How the writer keeps it: a series row is written in a block when the series
is new to the writer in the block's partition. The writer remembers a series
as written in a partition only after the block carrying its row has landed;
when that block fails, the next block with rows of the series carries the
row again. The memory is a bounded cache (`series_cache.max_entries`): an
evicted series, and every series after a restart, gets its row written again.
Repeats are harmless to the canonical view.

The tests of this writer read its files back with the Rust Parquet reader.
The queries above are the reference implementation's recipes for DuckDB 1.1
or later; they were not run against this writer's files in CI.

## Partition lateness bound (contract for compactors)

A partition hour is taken from a block's `window_start`, the ingest time of
its window, never from event timestamps. A request that is refused and later
retried is admitted again into a new window, so it lands in the partition of
its new ingest time, not in the old one. The only writes into an hour H that
can still happen after H has ended come from generations whose window started
inside H and that have not finished flushing. A writer flushes one generation
at a time: a generation whose window has closed waits until the previous
generation's flush, including its retries, has finished or has been given up
at its deadline, and only then starts its own. The two retry periods are
therefore sequential, and the last attempt of a generation started inside H
ends no later than

```text
L = window.interval + 2 * window.flush_retry_deadline
```

after the end of H (135 s with the defaults: 15 s + 2 * 60 s). A compactor
that closes hour H must therefore wait at least L after the end of H before
it lists the hour.

`window_start` and L are measured on each writer's own wall clock. A
compactor adds to L the largest skew between its clock and any writer's. A
writer whose wall clock steps back keeps writing into the hour of its
current window until its clock passes that window again, so a compactor also
adds the largest backward step it tolerates.

The bound is enforced by the writer's own deadlines, so it holds for every
write the writer completes or abandons itself. It does NOT hold when the
object store applies a `put` the writer gave up on at the deadline: the
writer has reported the block as failed, but the store may still publish the
object later. A compactor cannot tell that case apart from a complete hour,
so a compacted hour may miss such an object. Readers that do not compact are
not affected.

## Extensions and deviations

Everything in which this writer differs from `exporter:series_parquet`:

1. Exponential histogram and summary points are stored. `metrics/values` has
   these additional nullable columns after `explicit_bounds`:

   ```text
   scale                   INT32  null
   zero_count              INT64  null
   zero_threshold          DOUBLE null
   positive_offset         INT32  null
   positive_bucket_counts  LIST<INT64> null   # non-null items
   negative_offset         INT32  null
   negative_bucket_counts  LIST<INT64> null   # non-null items
   quantile_values         LIST<STRUCT<quantile DOUBLE, value DOUBLE>> null
   ```

   An exponential histogram point fills `count`, `scale`, `zero_count`,
   `zero_threshold` and the bucket columns, and, when present, `sum`, `min`
   and `max`. `scale`, `zero_count` and `zero_threshold` are required OTLP
   fields, so an absent column reads as 0, not null (see the deviation note
   below); `min` and `max` are optional and stay null when absent. A summary
   point fills `count` and, when present, `sum` and `quantile_values`. Both
   leave `value_int`, `value_double`, `bucket_counts` and `explicit_bounds`
   null. Their series rows have `metric_type` `exp_histogram` or `summary`;
   classify by `metric_type`, as for every other kind.
2. Additional nullable columns for dropped-attribute counts, all `INT64`:
   `resource_dropped_attributes_count` and `scope_dropped_attributes_count`
   at the end of both `series` datasets, and `dropped_attributes_count` at
   the end of `logs/values`. They are not part of the identity.
3. No `schema_fingerprint` key. A tool that groups files by fingerprint must
   treat files without the key as a group of their own; their column set is
   version 1 plus the columns of items 1 and 2.
4. No denormalized columns, no log attribute allow-list
   (`logs.series_attributes` is always empty, as `identity_config` states)
   and no configurable sort: files are always sorted as section 5 says.
5. Exemplars and metrics without data are skipped without a counter; there
   is no `unsupported` or exemplar policy.
6. Each object is written with one `put` (no multipart upload). On the
   `file` backend the files and their directories are fsynced before a
   request is acknowledged.
7. Row groups are closed at about 8 MiB of encoder memory instead of 64 MiB.
8. `L` has no abort-timeout term, because there is no multipart upload to
   abort.
9. Invalid UTF-8 is repaired on ingress instead of refused
   (`exporter:series_parquet` refuses it): OTLP string fields at any level,
   CBOR text strings and map keys in `ser` cells, and Binary-typed OTAP
   string columns. Each maximal invalid sequence becomes U+FFFD. Inputs both
   writers accept produce identical canonical bytes and series ids; only
   inputs the reference refuses differ. Distinct invalid strings can repair
   to the same string and so share a series id. The golden vector
   `text_not_utf8` in `testdata/golden/cbor_v1.json` holds the repaired
   value; its series id is this writer's own output, so it guards against
   regressions and is not a reference value.

Do not point this writer and `exporter:series_parquet` at the same
`base_uri` unless the readers handle items 1 to 3.

## Limitations of version 1

- Exemplars are not stored, and their attribute payloads are neither read
  nor validated.
- Duplicate-key validation covers only the attribute lists this writer
  decodes: resource, scope, the log record's own attributes and a data
  point's attributes; it applies to the whole list, not only to the part in
  the identity.
- Attribute maps are lossy for readers: values are rendered to strings by
  `render_v1`, so a string `"42"` and an integer `42` are indistinguishable in
  the `attrs` map. The identity encoding is not lossy; `series_id`
  distinguishes them. A bytes value, whether it is an attribute map cell, a
  log body, or nested inside an array or kvlist, always renders as a
  quoted JSON string of padded standard base64 (e.g. `"qxI="`), never bare
  base64.
- A histogram `sum` of exactly zero cannot be told apart from an absent sum
  after an OTAP round trip: the transport omits a column whose every entry in
  a request is the type default, so when no point of a request carries a
  non-zero sum, every one of those sums is stored as null. The same holds for
  the other genuinely optional metrics columns (`min`, `max`) under the same
  condition. The required exponential-histogram fields `scale`, `zero_count`
  and `zero_threshold` are instead read as 0 when the transport omits them,
  because the omission means every point carried the value 0; storing null
  would lose it.
- The series columns that are not part of the identity (`description`, the
  dropped-attribute counts) are written with the series row, so a change is
  visible only when the row is written again: in the next hour, after a
  cache eviction or after a restart.
- Traces are refused.

## Compatibility rules

- `format_version` is part of every file's metadata and of the `v=1` path
  segment. A change to the identity encoding, to an existing column's type or
  meaning, or to the partition layout requires a new version.
- Within one version, dataset schemas change only additively (new nullable
  columns). Readers must read with union-by-name semantics.
- `series_id` values are comparable across writers, versions of this writer,
  and languages when `format_version` matches and the files carry the same
  `identity_config_hash` (section 5). For logs this writer's ids equal those
  of `exporter:series_parquet` with an empty `logs.series_attributes`.
  `producer_id_attribute` does not affect identity: `producer_id` is a
  projected column. Any change to an identity field's value, such as a
  resource attribute or a scope version after an SDK upgrade, yields a new
  `series_id`.
