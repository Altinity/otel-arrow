// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Arrow schemas of the four datasets: the Series Lake Format v1 columns in their v1 order,
//! followed by this writer's additional nullable columns (docs/FORMAT.md section 2 and
//! "Extensions").

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

use super::attrs::map_field;
use super::canonical::Signal;
use super::columns::list_of;

/// `TIMESTAMP(us, UTC)`.
#[must_use]
pub fn ts_us() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

fn req(name: &str, dt: DataType) -> Field {
    Field::new(name, dt, false)
}

fn opt(name: &str, dt: DataType) -> Field {
    Field::new(name, dt, true)
}

fn series_id() -> Field {
    req("series_id", DataType::FixedSizeBinary(16))
}

/// v1 columns of every series schema.
fn series_head() -> Vec<Field> {
    vec![
        series_id(),
        req("identity_bytes", DataType::Binary),
        req("emitted_at", ts_us()),
        req("resource_schema_url", DataType::Utf8),
        map_field("resource_attrs"),
        req("scope_name", DataType::Utf8),
        req("scope_version", DataType::Utf8),
        req("scope_schema_url", DataType::Utf8),
        map_field("scope_attrs"),
        map_field("attrs"),
    ]
}

/// Additional columns of every series schema (not part of the identity; latest row wins).
fn series_ext() -> [Field; 2] {
    [
        opt("resource_dropped_attributes_count", DataType::Int64),
        opt("scope_dropped_attributes_count", DataType::Int64),
    ]
}

/// The four dataset schemas, built once.
#[derive(Clone, Debug)]
pub struct Schemas {
    logs_series: SchemaRef,
    logs_values: SchemaRef,
    metrics_series: SchemaRef,
    metrics_values: SchemaRef,
}

impl Default for Schemas {
    fn default() -> Self {
        Self::new()
    }
}

impl Schemas {
    /// Build all schemas.
    #[must_use]
    pub fn new() -> Self {
        let logs_values = Schema::new(vec![
            series_id(),
            req("producer_id", DataType::Utf8),
            opt("time", ts_us()),
            opt("time_unix_nano", DataType::Int64),
            opt("observed_time", ts_us()),
            opt("observed_time_unix_nano", DataType::Int64),
            req("severity_number", DataType::Int32),
            req("severity_text", DataType::Utf8),
            opt("body", DataType::Utf8),
            req("event_name", DataType::Utf8),
            opt("trace_id", DataType::FixedSizeBinary(16)),
            opt("span_id", DataType::FixedSizeBinary(8)),
            req("flags", DataType::Int32),
            map_field("attrs"),
            // Additional column.
            opt("dropped_attributes_count", DataType::Int64),
        ]);
        let mut metrics_series = series_head();
        metrics_series.extend([
            req("metric_name", DataType::Utf8),
            req("unit", DataType::Utf8),
            req("metric_type", DataType::Utf8),
            req("temporality", DataType::Utf8),
            req("is_monotonic", DataType::Boolean),
            req("description", DataType::Utf8),
        ]);
        metrics_series.extend(series_ext());
        let quantile = DataType::Struct(Fields::from(vec![
            opt("quantile", DataType::Float64),
            opt("value", DataType::Float64),
        ]));
        let metrics_values = Schema::new(vec![
            series_id(),
            req("producer_id", DataType::Utf8),
            req("metric_name", DataType::Utf8),
            opt("time", ts_us()),
            opt("time_unix_nano", DataType::Int64),
            opt("start_time", ts_us()),
            opt("start_time_unix_nano", DataType::Int64),
            req("flags", DataType::Int32),
            opt("value_int", DataType::Int64),
            opt("value_double", DataType::Float64),
            opt("count", DataType::Int64),
            opt("sum", DataType::Float64),
            opt("min", DataType::Float64),
            opt("max", DataType::Float64),
            opt("bucket_counts", list_of(DataType::Int64)),
            opt("explicit_bounds", list_of(DataType::Float64)),
            // Additional columns: exponential histogram and summary points.
            opt("scale", DataType::Int32),
            opt("zero_count", DataType::Int64),
            opt("zero_threshold", DataType::Float64),
            opt("positive_offset", DataType::Int32),
            opt("positive_bucket_counts", list_of(DataType::Int64)),
            opt("negative_offset", DataType::Int32),
            opt("negative_bucket_counts", list_of(DataType::Int64)),
            opt(
                "quantile_values",
                DataType::List(Arc::new(Field::new("item", quantile, true))),
            ),
        ]);
        Self {
            logs_series: Arc::new(Schema::new(
                series_head()
                    .into_iter()
                    .chain(series_ext())
                    .collect::<Vec<_>>(),
            )),
            logs_values: Arc::new(logs_values),
            metrics_series: Arc::new(Schema::new(metrics_series)),
            metrics_values: Arc::new(metrics_values),
        }
    }

    /// Schema of the `series` dataset of `signal`.
    #[must_use]
    pub const fn series(&self, signal: Signal) -> &SchemaRef {
        match signal {
            Signal::Logs => &self.logs_series,
            Signal::Metrics => &self.metrics_series,
        }
    }

    /// Schema of the `values` dataset of `signal`.
    #[must_use]
    pub const fn values(&self, signal: Signal) -> &SchemaRef {
        match signal {
            Signal::Logs => &self.logs_values,
            Signal::Metrics => &self.metrics_values,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: The four schemas are rendered as `name type nullability` lines.
    /// Guarantees: The leading columns are exactly the Series Lake Format v1 columns, in order, with v1 types and nullability; every additional column comes after them and is nullable.
    #[test]
    fn schemas_are_v1_plus_nullable_additions() {
        let s = Schemas::new();
        let names = |schema: &SchemaRef| -> Vec<String> {
            schema
                .fields()
                .iter()
                .map(|f| format!("{}{}", f.name(), if f.is_nullable() { "?" } else { "" }))
                .collect()
        };
        assert_eq!(
            names(s.values(Signal::Logs)),
            [
                "series_id",
                "producer_id",
                "time?",
                "time_unix_nano?",
                "observed_time?",
                "observed_time_unix_nano?",
                "severity_number",
                "severity_text",
                "body?",
                "event_name",
                "trace_id?",
                "span_id?",
                "flags",
                "attrs",
                "dropped_attributes_count?"
            ]
        );
        assert_eq!(
            names(s.series(Signal::Metrics)),
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
                "attrs",
                "metric_name",
                "unit",
                "metric_type",
                "temporality",
                "is_monotonic",
                "description",
                "resource_dropped_attributes_count?",
                "scope_dropped_attributes_count?"
            ]
        );
        assert_eq!(names(s.series(Signal::Logs)).len(), 12);
        let mv = names(s.values(Signal::Metrics));
        assert_eq!(
            &mv[..16],
            [
                "series_id",
                "producer_id",
                "metric_name",
                "time?",
                "time_unix_nano?",
                "start_time?",
                "start_time_unix_nano?",
                "flags",
                "value_int?",
                "value_double?",
                "count?",
                "sum?",
                "min?",
                "max?",
                "bucket_counts?",
                "explicit_bounds?"
            ]
        );
        assert!(mv[16..].iter().all(|n| n.ends_with('?')), "{mv:?}");
        let field = |name: &str| {
            s.values(Signal::Metrics)
                .field_with_name(name)
                .expect(name)
                .clone()
        };
        assert_eq!(field("time").data_type(), &ts_us());
        assert_eq!(field("count").data_type(), &DataType::Int64);
        assert_eq!(
            field("bucket_counts").data_type(),
            &list_of(DataType::Int64)
        );
        assert_eq!(field("flags").data_type(), &DataType::Int32);
    }
}
