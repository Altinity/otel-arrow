// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Output datasets and their Arrow schemas (FORMAT.md section 2).

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

use crate::canonical::Signal;
use crate::config::{DenormSource, DenormType, Denormalize, LakeConfig};

/// One output dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Dataset {
    /// `signal=logs/dataset=series`
    LogsSeries,
    /// `signal=logs/dataset=values`
    LogsValues,
    /// `signal=metrics/dataset=series`
    MetricsSeries,
    /// `signal=metrics/dataset=values`
    ///
    /// Number and histogram points share one schema: the per-kind columns are
    /// nullable and the point kind is read from the series descriptor's
    /// `metric_type` through the join readers already perform (FORMAT.md section 2).
    MetricsValues,
}

impl Dataset {
    /// All datasets, in write order per signal (series first).
    pub const ALL: [Dataset; 4] = [
        Dataset::LogsSeries,
        Dataset::LogsValues,
        Dataset::MetricsSeries,
        Dataset::MetricsValues,
    ];

    /// Signal of the dataset.
    #[must_use]
    pub fn signal(self) -> Signal {
        match self {
            Dataset::LogsSeries | Dataset::LogsValues => Signal::Logs,
            _ => Signal::Metrics,
        }
    }

    /// Hive `dataset=` value.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Dataset::LogsSeries | Dataset::MetricsSeries => "series",
            Dataset::LogsValues | Dataset::MetricsValues => "values",
        }
    }

    /// Whether this is a series dataset.
    #[must_use]
    pub fn is_series(self) -> bool {
        matches!(self, Dataset::LogsSeries | Dataset::MetricsSeries)
    }

    /// Series dataset of the same signal.
    #[must_use]
    pub fn series_of(signal: Signal) -> Dataset {
        match signal {
            Signal::Logs => Dataset::LogsSeries,
            Signal::Metrics => Dataset::MetricsSeries,
        }
    }
}

fn ts_us() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
}

/// `MAP<STRING, STRING>` with nullable values, Parquet-compatible field names.
#[must_use]
pub fn map_string_string() -> DataType {
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
        ])),
        false,
    );
    DataType::Map(Arc::new(entries), false)
}

fn denorm_type(t: DenormType) -> DataType {
    match t {
        DenormType::String => DataType::Utf8,
        DenormType::Int64 => DataType::Int64,
        DenormType::Double => DataType::Float64,
        DenormType::Bool => DataType::Boolean,
    }
}

/// Denormalized columns that appear in a dataset.
#[must_use]
pub fn denorm_columns(ds: Dataset, cfg: &LakeConfig) -> Vec<&Denormalize> {
    let sig = match ds.signal() {
        Signal::Logs => &cfg.logs,
        Signal::Metrics => &cfg.metrics,
    };
    sig.denormalize
        .iter()
        .filter(|d| {
            if !ds.is_series() {
                return true;
            }
            match d.source() {
                Ok((DenormSource::Resource | DenormSource::Scope, _)) => true,
                Ok((DenormSource::Attrs, key)) => match ds.signal() {
                    Signal::Metrics => true,
                    Signal::Logs => sig.series_attributes.iter().any(|a| a == key),
                },
                Err(_) => false,
            }
        })
        .collect()
}

/// Arrow schema of a dataset under a configuration.
#[must_use]
pub fn dataset_schema(ds: Dataset, cfg: &LakeConfig) -> SchemaRef {
    let mut fields: Vec<Field> = vec![Field::new(
        "series_id",
        DataType::FixedSizeBinary(16),
        false,
    )];
    if !ds.is_series() {
        fields.push(Field::new("producer_id", DataType::Utf8, false));
    }
    match ds {
        Dataset::LogsSeries | Dataset::MetricsSeries => {
            fields.extend([
                Field::new("identity_bytes", DataType::Binary, false),
                Field::new("emitted_at", ts_us(), false),
                Field::new("resource_schema_url", DataType::Utf8, false),
                Field::new("resource_attrs", map_string_string(), false),
                Field::new("scope_name", DataType::Utf8, false),
                Field::new("scope_version", DataType::Utf8, false),
                Field::new("scope_schema_url", DataType::Utf8, false),
                Field::new("scope_attrs", map_string_string(), false),
                Field::new("attrs", map_string_string(), false),
            ]);
            if ds == Dataset::MetricsSeries {
                fields.extend([
                    Field::new("metric_name", DataType::Utf8, false),
                    Field::new("unit", DataType::Utf8, false),
                    Field::new("metric_type", DataType::Utf8, false),
                    Field::new("temporality", DataType::Utf8, false),
                    Field::new("is_monotonic", DataType::Boolean, false),
                    Field::new("description", DataType::Utf8, false),
                ]);
            }
        }
        Dataset::LogsValues => fields.extend([
            Field::new("time", ts_us(), true),
            Field::new("time_unix_nano", DataType::Int64, true),
            Field::new("observed_time", ts_us(), true),
            Field::new("observed_time_unix_nano", DataType::Int64, true),
            Field::new("severity_number", DataType::Int32, false),
            Field::new("severity_text", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, true),
            Field::new("event_name", DataType::Utf8, false),
            Field::new("trace_id", DataType::FixedSizeBinary(16), true),
            Field::new("span_id", DataType::FixedSizeBinary(8), true),
            Field::new("flags", DataType::Int32, false),
            Field::new("attrs", map_string_string(), false),
        ]),
        Dataset::MetricsValues => {
            // Union of the number and histogram columns. Every per-kind column
            // is nullable: a number point leaves the six histogram columns
            // null, a histogram point leaves both value columns null.
            fields.extend([
                Field::new("metric_name", DataType::Utf8, false),
                Field::new("time", ts_us(), true),
                Field::new("time_unix_nano", DataType::Int64, true),
                Field::new("start_time", ts_us(), true),
                Field::new("start_time_unix_nano", DataType::Int64, true),
                Field::new("flags", DataType::Int32, false),
                Field::new("value_int", DataType::Int64, true),
                Field::new("value_double", DataType::Float64, true),
                Field::new("count", DataType::Int64, true),
                Field::new("sum", DataType::Float64, true),
                Field::new("min", DataType::Float64, true),
                Field::new("max", DataType::Float64, true),
                Field::new(
                    "bucket_counts",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, false))),
                    true,
                ),
                Field::new(
                    "explicit_bounds",
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, false))),
                    true,
                ),
            ]);
        }
    }
    for d in denorm_columns(ds, cfg) {
        fields.push(Field::new(&d.column, denorm_type(d.ty), true));
    }
    Arc::new(Schema::new(fields))
}

/// First line of every schema rendering: the name and version of the type
/// vocabulary below. A future vocabulary change bumps the version, so its
/// fingerprints can never be confused with this one's.
pub const SCHEMA_RENDERING_HEADER: &str = "series-lake-schema/1";

/// The crate-owned rendering of a dataset schema that [`schema_fingerprint`]
/// hashes (FORMAT.md section 3).
///
/// The header line, then one line per field in schema order:
/// `<len>:<name><len>:<type>` followed by a newline, where each `<len>` is the
/// decimal byte length of the string after its colon. `<type>` is a type code
/// of the crate's own vocabulary followed by the field's nullability, `!` for a
/// required field and `?` for a nullable one; list items and map keys and
/// values carry their own nullability the same way.
///
/// The vocabulary is modelled on pdata's `SchemaIdBuilder`: it is owned here
/// and never taken from Arrow's `Display`, so an Arrow upgrade that changes
/// how a type prints cannot move a persisted fingerprint. Unlike
/// `SchemaIdBuilder`, fields are not sorted by name: the column position is
/// part of a Parquet file's physical schema, and two files whose columns are
/// in different orders cannot have their row groups concatenated by a
/// compaction that trusts the fingerprint.
///
/// The length prefixes keep the serialization unambiguous. A plain
/// `name:type;` join is not: one string column named `a:Str;b` produces the
/// same text as two string columns named `a` and `b`, so two genuinely
/// different schemas would share a fingerprint without any hash collision.
/// `LakeConfig::validate` separately rejects denormalized column names holding
/// `:` or `;`, but the encoding does not rely on that rule.
#[must_use]
pub fn schema_rendering(schema: &Schema) -> String {
    let mut out = String::with_capacity(64 * (schema.fields().len() + 1));
    out.push_str(SCHEMA_RENDERING_HEADER);
    out.push('\n');
    for f in schema.fields() {
        let name = f.name();
        let mut ty = String::new();
        render_field_type(f, &mut ty);
        out.push_str(&format!("{}:{}{}:{}\n", name.len(), name, ty.len(), ty));
    }
    out
}

/// The type code of `field` followed by its nullability marker.
fn render_field_type(field: &Field, out: &mut String) {
    render_type(field.data_type(), out);
    out.push(if field.is_nullable() { '?' } else { '!' });
}

/// The type code of one Arrow type in the `series-lake-schema/1` vocabulary.
///
/// The vocabulary covers every type a dataset schema can hold, plus the other
/// fixed-width integers and floats so that a future intrinsic column of one of
/// those types needs no vocabulary change. Anything else renders as `Unk<>`;
/// no dataset schema produces it, which a unit test asserts for every dataset.
fn render_type(dt: &DataType, out: &mut String) {
    match dt {
        DataType::Boolean => out.push_str("Bol"),
        DataType::Int8 => out.push_str("I8"),
        DataType::Int16 => out.push_str("I16"),
        DataType::Int32 => out.push_str("I32"),
        DataType::Int64 => out.push_str("I64"),
        DataType::UInt8 => out.push_str("U8"),
        DataType::UInt16 => out.push_str("U16"),
        DataType::UInt32 => out.push_str("U32"),
        DataType::UInt64 => out.push_str("U64"),
        DataType::Float32 => out.push_str("F32"),
        DataType::Float64 => out.push_str("F64"),
        DataType::Utf8 => out.push_str("Str"),
        DataType::Binary => out.push_str("Bin"),
        DataType::FixedSizeBinary(n) => out.push_str(&format!("FSB<{n}>")),
        DataType::Timestamp(unit, tz) => {
            let unit = match unit {
                TimeUnit::Second => "s",
                TimeUnit::Millisecond => "ms",
                TimeUnit::Microsecond => "us",
                TimeUnit::Nanosecond => "ns",
            };
            out.push_str(&format!("Ts<{unit},{}>", tz.as_deref().unwrap_or("")));
        }
        DataType::List(item) => {
            out.push('[');
            render_field_type(item, out);
            out.push(']');
        }
        DataType::Map(entries, _) => {
            out.push_str("Map<");
            if let DataType::Struct(kv) = entries.data_type()
                && kv.len() == 2
            {
                render_field_type(&kv[0], out);
                out.push(',');
                render_field_type(&kv[1], out);
            }
            out.push('>');
        }
        _ => out.push_str("Unk<>"),
    }
}

/// xxh3_64 (seed 0) of [`schema_rendering`]'s UTF-8 bytes, rendered in file
/// metadata as 16 lowercase hex digits.
#[must_use]
pub fn schema_fingerprint(schema: &Schema) -> u64 {
    xxhash_rust::xxh3::xxh3_64(schema_rendering(schema).as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rule sentence of a configuration refusal.
    fn rule(err: &crate::error::Error) -> String {
        err.invalid_detail()
            .map_or_else(|| err.to_string(), str::to_owned)
    }
    use crate::config::{DenormType, Denormalize, LakeConfig};

    /// Scenario: the default `logs_values` schema, frozen for format version 1.
    /// Guarantees: its fingerprint is exactly the pinned value (every dataset is pinned in
    /// `tests/golden.rs`).
    #[test]
    fn golden_fingerprint_of_the_default_logs_values_schema() {
        let cfg = LakeConfig::default();
        let fp = schema_fingerprint(&dataset_schema(Dataset::LogsValues, &cfg));
        assert_eq!(fp, 0x0151_2d4e_b446_f355_u64, "{fp:#018x}");
    }

    /// Scenario: every dataset, default and with one denormalized column of each type.
    /// Guarantees: every column type renders from the crate's vocabulary, never `Unk<>`.
    #[test]
    fn every_dataset_column_type_is_in_the_vocabulary() {
        let mut denorm = LakeConfig::default();
        for (i, ty) in [
            DenormType::String,
            DenormType::Int64,
            DenormType::Double,
            DenormType::Bool,
        ]
        .into_iter()
        .enumerate()
        {
            for sig in [&mut denorm.logs, &mut denorm.metrics] {
                sig.denormalize.push(Denormalize {
                    path: format!("resource.k{i}"),
                    column: format!("c{i}"),
                    ty,
                });
            }
        }
        for cfg in [LakeConfig::default(), denorm] {
            for ds in Dataset::ALL {
                let r = schema_rendering(&dataset_schema(ds, &cfg));
                assert!(r.starts_with("series-lake-schema/1\n"), "{r}");
                assert!(!r.contains("Unk<"), "{ds:?}: {r}");
            }
        }
    }

    /// Scenario: schemas differing only in a column's, or a list item's, nullability.
    /// Guarantees: the fingerprints differ.
    #[test]
    fn nullability_changes_the_fingerprint() {
        let one = |nullable| Schema::new(vec![Field::new("a", DataType::Int64, nullable)]);
        assert_ne!(
            schema_fingerprint(&one(true)),
            schema_fingerprint(&one(false))
        );
        let list = |nullable| {
            Schema::new(vec![Field::new(
                "l",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, nullable))),
                true,
            )])
        };
        assert_eq!(
            schema_rendering(&list(false)),
            "series-lake-schema/1\n1:l7:[I64!]?\n"
        );
        assert_ne!(
            schema_fingerprint(&list(true)),
            schema_fingerprint(&list(false))
        );
    }

    /// Scenario: one column named `a:Str;b` against columns `a` and `b`.
    /// Guarantees: the length-prefixed serialization gives different fingerprints.
    #[test]
    fn delimiter_bearing_names_do_not_collide() {
        let one = Schema::new(vec![Field::new("a:Str;b", DataType::Utf8, true)]);
        let two = Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        assert_ne!(schema_fingerprint(&one), schema_fingerprint(&two));
    }

    /// Scenario: default config, every dataset.
    /// Guarantees: the intrinsic column lists of FORMAT.md section 2 are produced in order.
    #[test]
    fn intrinsic_columns_match_spec() {
        let cfg = LakeConfig::default();
        let names = |ds: Dataset| -> Vec<String> {
            dataset_schema(ds, &cfg)
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        };
        assert_eq!(
            names(Dataset::LogsValues),
            [
                "series_id",
                "producer_id",
                "time",
                "time_unix_nano",
                "observed_time",
                "observed_time_unix_nano",
                "severity_number",
                "severity_text",
                "body",
                "event_name",
                "trace_id",
                "span_id",
                "flags",
                "attrs"
            ]
        );
        assert_eq!(
            names(Dataset::MetricsValues),
            [
                "series_id",
                "producer_id",
                "metric_name",
                "time",
                "time_unix_nano",
                "start_time",
                "start_time_unix_nano",
                "flags",
                "value_int",
                "value_double",
                "count",
                "sum",
                "min",
                "max",
                "bucket_counts",
                "explicit_bounds"
            ]
        );
        assert_eq!(
            names(Dataset::LogsSeries),
            [
                "series_id",
                "identity_bytes",
                "emitted_at",
                "resource_schema_url",
                "resource_attrs",
                "scope_name",
                "scope_version",
                "scope_schema_url",
                "scope_attrs",
                "attrs"
            ]
        );
        assert_eq!(
            names(Dataset::MetricsSeries)[10..],
            [
                "metric_name",
                "unit",
                "metric_type",
                "temporality",
                "is_monotonic",
                "description"
            ]
        );
    }

    /// Scenario: the merged `metrics/values` schema under the default config.
    /// Guarantees: every column only one point kind fills is nullable.
    #[test]
    fn metrics_values_per_kind_columns_are_nullable() {
        let cfg = LakeConfig::default();
        let schema = dataset_schema(Dataset::MetricsValues, &cfg);
        for name in [
            "value_int",
            "value_double",
            "count",
            "sum",
            "min",
            "max",
            "bucket_counts",
            "explicit_bounds",
        ] {
            let (_, f) = schema
                .column_with_name(name)
                .unwrap_or_else(|| panic!("{name} missing"));
            assert!(f.is_nullable(), "{name} must be nullable");
        }
        for name in ["series_id", "producer_id", "metric_name", "flags"] {
            let (_, f) = schema
                .column_with_name(name)
                .unwrap_or_else(|| panic!("{name} missing"));
            assert!(!f.is_nullable(), "{name} must stay required");
        }
    }

    /// Scenario: a resource-path and an attrs-path denormalized column for logs.
    /// Guarantees: values get both, series gets only the identity (resource) one; fingerprints differ.
    #[test]
    fn denormalized_columns_and_fingerprint() {
        let mut cfg = LakeConfig::default();
        cfg.logs.denormalize = vec![
            Denormalize {
                path: "resource.service.name".into(),
                column: "service_name".into(),
                ty: DenormType::String,
            },
            Denormalize {
                path: "attrs.http.status_code".into(),
                column: "http_status_code".into(),
                ty: DenormType::Int64,
            },
        ];
        let values = dataset_schema(Dataset::LogsValues, &cfg);
        assert!(values.column_with_name("service_name").is_some());
        assert!(values.column_with_name("http_status_code").is_some());
        let series = dataset_schema(Dataset::LogsSeries, &cfg);
        assert!(series.column_with_name("service_name").is_some());
        assert!(series.column_with_name("http_status_code").is_none());
        assert_ne!(schema_fingerprint(&values), schema_fingerprint(&series));
        assert_ne!(
            schema_fingerprint(&values),
            schema_fingerprint(&dataset_schema(Dataset::LogsValues, &LakeConfig::default()))
        );
    }

    /// Scenario: two denormalized columns whose names differ only by case, and
    /// a sort key naming no column.
    /// Guarantees: validation refuses each, naming the user-facing dotted key
    /// of the entry at fault.
    #[test]
    fn collision_is_a_config_error() {
        let mut cfg = LakeConfig::default();
        cfg.logs.denormalize = vec![
            Denormalize {
                path: "resource.a".into(),
                column: "Col".into(),
                ty: DenormType::String,
            },
            Denormalize {
                path: "resource.b".into(),
                column: "col".into(),
                ty: DenormType::String,
            },
        ];
        let err = cfg.validate().expect_err("case-insensitive collision");
        assert!(
            rule(&err).contains("logs.denormalize[1].column \"col\" is a column name collision"),
            "{err}"
        );
        let mut cfg = LakeConfig::default();
        cfg.logs.values_sort = vec![crate::config::SortKey {
            column: "nope".into(),
            order: crate::config::SortOrder::Asc,
            nulls: crate::config::Nulls::Last,
        }];
        let err = cfg.validate().expect_err("an unknown sort column");
        assert!(
            rule(&err).contains("logs.values_sort[0].column \"nope\" is not a column of"),
            "{err}"
        );
    }
}
