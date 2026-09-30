# Parquet Lake Format

Format version: **1**. This document specifies the layout, schemas and series
identity written by `exporter:parquet_lake`, so that other writers (in any
language) can produce compatible data and readers can query it.

## 1. Overview

Each signal (logs, metrics) is written as two Parquet datasets:

- `series`: one row per distinct identity per block. The identity is the
  resource and scope for logs; resource, scope, metric and point attributes
  for metrics. A series row can appear again in later blocks. **Readers keep
  the row with the greatest `written_at` per `series_id`.**
- `values`: one row per log record or metric data point, carrying the
  `series_id` of its identity instead of the identity itself.

A writer buffers rows in a block per signal and writes each block as two
objects: the series file, then the values file. Every `series_id` in a values
file has a row in the series file of the same block.

## 2. Layout

```text
<base>/<signal>/<dataset>/dt=YYYY-MM-DD/<writer>-<seq>.parquet
```

- `<signal>` is `logs` or `metrics`, and `<dataset>` is `series` or `values`.
- `dt` is the UTC date of the block's creation (its first row).
- `<writer>` is unique per writer instance start (for example
  `c<core>-<16 hex random nonce>`), and `<seq>` is a zero-padded 20-digit
  sequence number within the writer.
- Both files of a block share the stem `<writer>-<seq>`.
- The object names of a block are fixed before its first write attempt, and
  retries rewrite the same names with the same bytes.
- The series file is written before the values file. **A block has landed if
  and only if its values file exists.**

## 3. Identity grammar

The canonical key is a byte string. All integers are little-endian.

```text
key      := u8 version(=1) || u8 signal(1 logs, 2 metrics)
            || attrs(resource) || bytes(scope.name) || bytes(scope.version)
            || attrs(scope) [|| metric]          ; metric part for metrics only
metric   := bytes(name) || bytes(unit)
            || u8 type(1 gauge, 2 sum, 3 histogram, 4 exponential_histogram, 5 summary)
            || i32le aggregation_temporality || u8 is_monotonic(0|1)
            || attrs(point)
bytes(x) := u32le len(x) || x
attrs    := u32le n || (bytes(key) || value)*
value    := u8 tag || payload
```

| Tag | Type | Payload |
| --- | --- | --- |
| 0 | empty | none |
| 1 | string | `bytes(utf8)` |
| 2 | int | `i64le` |
| 3 | double | `u64le` canonical bits |
| 4 | bool | `u8` 0 or 1 |
| 5 | bytes | `bytes(raw)` |
| 6 | array | `u32le count` followed by `count` values, in order |
| 7 | map | `attrs` (nested key/value list) |

Rules:

- **Sorting.** `attrs` entries (top-level and nested maps) are sorted by key
  bytes and then by encoded value bytes. Among entries with the same key, the
  first after sorting (the smallest encoded value) is kept. Input order never
  matters.
- **Doubles.** -0.0 encodes as +0.0, and every NaN encodes as
  `0x7ff8000000000000`.
- **Absent typed values.** A string, int, double, bool or bytes value that is
  absent encodes as its type default (`""`, 0, +0.0, false, empty bytes). The
  rendered `Map<Utf8, Utf8>` columns show the same default (`""`, `"0"`, `"0"`,
  `"false"`, `""`).
- **Empty.** Tag 0 (and a null rendered value) covers a null type, the empty
  type, an unknown type code, and a map or array without a serialized value.
  A serialized map or array value that cannot be decoded, nests deeper than
  128 levels, has a non-text map key or an integer outside i64 also encodes as
  tag 0 as a whole (its rendered text is kept as far as it decodes, or as hex).
- **Missing fields.** A missing resource or scope contributes zero attributes.
  A missing scope name/version or metric name/unit encodes as `""`. A missing
  metric type, temporality or monotonic flag encodes as 0.
- **Depth.** Arrays and maps may nest at most 128 levels (see Empty).
- **Not in the identity:** the metric description (a series column, latest
  wins) and, for logs, the body, severity, timestamps, trace context and log
  record attributes (values columns).

## 4. series_id

```text
series_id := XXH3-128(key, seed 0)
```

It is stored as the 16 bytes of the 128-bit hash in big-endian order, in a
`FixedSizeBinary(16)` column. This is the same as the canonical XXH3-128 hex
digest (for example Python `xxhash.xxh3_128_hexdigest(key)`). Hex strings in
this document use that order.

XXH3 is fast but not collision-resistant against adversarial input. Writers
must not rely on `series_id` for security decisions.

## 5. Schemas

Timestamps are `Timestamp(Nanosecond, "UTC")`. Map columns are
`Map<Utf8, Utf8>` (`entries: {key: Utf8 not null, value: Utf8}`, unsorted),
with unique keys (for a duplicated key, the entry that wins in the identity,
section 3) and values rendered as text (NaN and infinities inside maps and
arrays as the JSON strings `"NaN"`, `"Infinity"`, `"-Infinity"`): strings as
is, numbers and bools in decimal /
`true`/`false`, bytes as lowercase hex, and maps and arrays as compact JSON.

### logs/series and metrics/series (common columns)

| Column | Type | Null | Meaning |
| --- | --- | --- | --- |
| `series_id` | FixedSizeBinary(16) | no | identity hash (section 4) |
| `written_at` | Timestamp | no | block flush time; readers keep the latest row per `series_id` |
| `resource_attributes` | Map | yes | resource attributes |
| `scope_name` | Utf8 | yes | instrumentation scope name |
| `scope_version` | Utf8 | yes | instrumentation scope version |
| `scope_attributes` | Map | yes | scope attributes |
| (last) `resource_schema_url` | Utf8 | yes | resource schema URL (not in the identity) |
| (last) `resource_dropped_attributes_count` | UInt32 | yes | not in the identity |
| (last) `scope_schema_url` | Utf8 | yes | scope schema URL (not in the identity) |
| (last) `scope_dropped_attributes_count` | UInt32 | yes | not in the identity |

### metrics/series (additional columns)

| Column | Type | Null | Meaning |
| --- | --- | --- | --- |
| `metric_name` | Utf8 | yes | metric name |
| `metric_description` | Utf8 | yes | description (not part of the identity) |
| `metric_unit` | Utf8 | yes | unit |
| `metric_type` | Utf8 | yes | `gauge`, `sum`, `histogram`, `exponential_histogram` or `summary` |
| `aggregation_temporality` | Int32 | yes | 1 delta, 2 cumulative |
| `is_monotonic` | Boolean | yes | sums only |
| `attributes` | Map | yes | data point attributes |

### logs/values

| Column | Type | Null |
| --- | --- | --- |
| `series_id` | FixedSizeBinary(16) | no |
| `time_unix_nano` | Timestamp | yes |
| `observed_time_unix_nano` | Timestamp | yes |
| `severity_number` | Int32 | yes |
| `severity_text` | Utf8 | yes |
| `body` | Utf8 | yes |
| `attributes` | Map | yes |
| `trace_id` | FixedSizeBinary(16) | yes |
| `span_id` | FixedSizeBinary(8) | yes |
| `flags` | UInt32 | yes |
| `event_name` | Utf8 | yes |
| `dropped_attributes_count` | UInt32 | yes |

### metrics/values

One wide table for all data point kinds. Columns that do not apply to a kind
are null.

| Column | Type | Kinds |
| --- | --- | --- |
| `series_id` | FixedSizeBinary(16), not null | all |
| `start_time_unix_nano` | Timestamp | all |
| `time_unix_nano` | Timestamp | all |
| `flags` | UInt32 | all |
| `int_value` | Int64 | gauge, sum |
| `double_value` | Float64 | gauge, sum |
| `count` | UInt64 | histogram, exponential histogram, summary |
| `sum` | Float64 | histogram, exponential histogram, summary |
| `min`, `max` | Float64 | histogram, exponential histogram |
| `bucket_counts` | List(UInt64) | histogram |
| `explicit_bounds` | List(Float64) | histogram |
| `scale` | Int32 | exponential histogram |
| `zero_count` | UInt64 | exponential histogram |
| `zero_threshold` | Float64 | exponential histogram |
| `positive_offset`, `negative_offset` | Int32 | exponential histogram |
| `positive_bucket_counts`, `negative_bucket_counts` | List(UInt64) | exponential histogram |
| `quantile_values` | List(Struct(quantile Float64, value Float64)) | summary |

### Not stored

Metric exemplars are not written, and metric metadata is not carried by OTAP.
The four `(last)` columns above are the final columns of both series schemas,
after the signal-specific columns.

## 6. File metadata

Every file carries Parquet key/value metadata:

| Key | Value |
| --- | --- |
| `otel.lake.format_version` | `1` |
| `otel.lake.dataset` | `logs/series`, `logs/values`, `metrics/series` or `metrics/values` |

## 7. Compatibility rules

- Any change to the identity grammar (section 3), the hash (section 4), or the
  meaning of an existing column increments `format_version`. Data of a new
  version is written under a new base path.
- Adding a nullable column keeps the version.
- Readers must ignore columns they do not know.

## 8. Golden vectors

A conforming writer produces exactly these keys and ids. Keys are written with
the grammar of section 3; ids are XXH3-128 (seed 0) in big-endian hex. The
exporter's test `golden_vectors_match_format_md` asserts every row.

| Name | Identity | Canonical key (hex) | series_id (hex) |
| --- | --- | --- | --- |
| `logs_resource_scope` | logs; resource {service.name: "api", host.name: "h1"} (either order); scope "lib" "1.0" | `01010200000009000000686f73742e6e616d65010200000068310c000000736572766963652e6e616d650103000000617069030000006c696203000000312e3000000000` | `694ace1a971682b44a4b698f7cfdeb1e` |
| `logs_int_1` | logs; resource {k: int 1}; empty scope | `010101000000010000006b020100000000000000000000000000000000000000` | `99296919f5925b76229ae7f5c93ca6a6` |
| `logs_str_1` | logs; resource {k: "1"} (differs from int 1) | `010101000000010000006b010100000031000000000000000000000000` | `b9687c6cad6225d11d69ae54ac007462` |
| `logs_int_0_default` | logs; resource {k: int 0}; also an Int row whose value column is absent | `010101000000010000006b020000000000000000000000000000000000000000` | `345d3e1709380fb136d1562b3b2e1740` |
| `logs_empty_value` | logs; resource {k: (empty value)} (Empty type) | `010101000000010000006b00000000000000000000000000` | `00f56a30ad7296e237f6375084921709` |
| `logs_duplicate_key` | logs; resource [k: "b", k: "a"] (either order) -> keeps "a" | `010101000000010000006b010100000061000000000000000000000000` | `39b47467b46e7249606c589102b22ae7` |
| `logs_nested_map` | logs; resource {m: {b: int 2, a: true}} (kvlist) | `010101000000010000006d0702000000010000006104010100000062020200000000000000000000000000000000000000` | `ad06b0f1308ff65f0810634c90dd4196` |
| `logs_array` | logs; resource {arr: [int 1, "x", double 2.5]} | `010101000000030000006172720603000000020100000000000000010100000078030000000000000440000000000000000000000000` | `ef02c97fc12d600798d0ccc297c9e741` |
| `logs_unicode_key` | logs; resource {"cl\u00e9": "v\u00e4rde"} (UTF-8) | `01010100000004000000636cc3a9010600000076c3a4726465000000000000000000000000` | `9c0c02d2dfcc96d610830fa743ccf5b5` |
| `logs_neg_zero_nan` | logs; resource {d: -0.0, n: NaN} (same as +0.0 / canonical NaN) | `0101020000000100000064030000000000000000010000006e03000000000000f87f000000000000000000000000` | `5368d975dfa7d7ac82f24ee4cd8e14b2` |
| `metric_gauge` | metrics; resource {host.name: "h1"}; scope "lib" "1.0"; gauge "cpu" unit "1"; point {core: "0"} | `01020100000009000000686f73742e6e616d6501020000006831030000006c696203000000312e30000000000300000063707501000000310100000000000100000004000000636f7265010100000030` | `35ed2533eb63eb8208cd6b88eca0302d` |
| `metric_sum` | same, sum "requests" unit "{request}", cumulative (2), monotonic | `01020100000009000000686f73742e6e616d6501020000006831030000006c696203000000312e3000000000080000007265717565737473090000007b726571756573747d0202000000010100000004000000636f7265010100000030` | `a1977a29577bae41f59c18f7e9687932` |
| `metric_histogram` | same, histogram "latency" unit "ms", delta (1) | `01020100000009000000686f73742e6e616d6501020000006831030000006c696203000000312e3000000000070000006c6174656e6379020000006d730301000000000100000004000000636f7265010100000030` | `d91ca6f51242dbf8fc20f9aa0837e4ed` |
| `metric_exp_histogram` | same, exponential histogram "latency.exp" unit "ms", delta (1) | `01020100000009000000686f73742e6e616d6501020000006831030000006c696203000000312e30000000000b0000006c6174656e63792e657870020000006d730401000000000100000004000000636f7265010100000030` | `8c1cc1efcf4a63cff08c875106f94429` |
| `metric_summary` | same, summary "rpc" unit "ms" | `01020100000009000000686f73742e6e616d6501020000006831030000006c696203000000312e300000000003000000727063020000006d730500000000000100000004000000636f7265010100000030` | `1473f50e8eb245e7cb1a6bb09ee60592` |

## 9. Querying

Join values to the latest series row per `series_id`, for example in DuckDB:

```sql
WITH series AS (
  SELECT * FROM read_parquet('<base>/logs/series/*/*.parquet')
  QUALIFY row_number() OVER (PARTITION BY series_id ORDER BY written_at DESC) = 1
)
SELECT s.resource_attributes['host.name'] AS host, v.time_unix_nano, v.body
FROM read_parquet('<base>/logs/values/*/*.parquet') v
JOIN series s USING (series_id);
```
