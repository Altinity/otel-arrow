// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Arrow schemas of the four datasets (see docs/FORMAT.md "Schemas").

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

use super::attrs::map_field;
use super::identity::Signal;

/// Timestamp type used for all time columns.
#[must_use]
pub fn ts_ns() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
}

fn list_of(dt: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", dt, true)))
}

fn nullable(name: &str, dt: DataType) -> Field {
    Field::new(name, dt, true)
}

/// Trailing columns of every series schema: resource/scope fields that are not part of the
/// identity but are kept (latest wins, like `metric_description`).
fn series_tail() -> [Field; 4] {
    [
        nullable("resource_schema_url", DataType::Utf8),
        nullable("resource_dropped_attributes_count", DataType::UInt32),
        nullable("scope_schema_url", DataType::Utf8),
        nullable("scope_dropped_attributes_count", DataType::UInt32),
    ]
}

/// Leading columns of every series schema: `series_id`, `written_at` and the resource/scope
/// identity columns.
fn series_head() -> Vec<Field> {
    vec![
        Field::new("series_id", DataType::FixedSizeBinary(16), false),
        Field::new("written_at", ts_ns(), false),
        map_field("resource_attributes"),
        nullable("scope_name", DataType::Utf8),
        nullable("scope_version", DataType::Utf8),
        map_field("scope_attributes"),
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
        let id = || Field::new("series_id", DataType::FixedSizeBinary(16), false);
        let logs_values = Schema::new(vec![
            id(),
            nullable("time_unix_nano", ts_ns()),
            nullable("observed_time_unix_nano", ts_ns()),
            nullable("severity_number", DataType::Int32),
            nullable("severity_text", DataType::Utf8),
            nullable("body", DataType::Utf8),
            map_field("attributes"),
            nullable("trace_id", DataType::FixedSizeBinary(16)),
            nullable("span_id", DataType::FixedSizeBinary(8)),
            nullable("flags", DataType::UInt32),
            nullable("event_name", DataType::Utf8),
            nullable("dropped_attributes_count", DataType::UInt32),
        ]);
        let mut metrics_series = series_head();
        metrics_series.extend([
            nullable("metric_name", DataType::Utf8),
            nullable("metric_description", DataType::Utf8),
            nullable("metric_unit", DataType::Utf8),
            nullable("metric_type", DataType::Utf8),
            nullable("aggregation_temporality", DataType::Int32),
            nullable("is_monotonic", DataType::Boolean),
            map_field("attributes"),
        ]);
        metrics_series.extend(series_tail());
        let quantile = DataType::Struct(Fields::from(vec![
            nullable("quantile", DataType::Float64),
            nullable("value", DataType::Float64),
        ]));
        let metrics_values = Schema::new(vec![
            id(),
            nullable("start_time_unix_nano", ts_ns()),
            nullable("time_unix_nano", ts_ns()),
            nullable("flags", DataType::UInt32),
            nullable("int_value", DataType::Int64),
            nullable("double_value", DataType::Float64),
            nullable("count", DataType::UInt64),
            nullable("sum", DataType::Float64),
            nullable("min", DataType::Float64),
            nullable("max", DataType::Float64),
            nullable("bucket_counts", list_of(DataType::UInt64)),
            nullable("explicit_bounds", list_of(DataType::Float64)),
            nullable("scale", DataType::Int32),
            nullable("zero_count", DataType::UInt64),
            nullable("zero_threshold", DataType::Float64),
            nullable("positive_offset", DataType::Int32),
            nullable("positive_bucket_counts", list_of(DataType::UInt64)),
            nullable("negative_offset", DataType::Int32),
            nullable("negative_bucket_counts", list_of(DataType::UInt64)),
            nullable("quantile_values", list_of(quantile)),
        ]);
        Self {
            logs_series: Arc::new(Schema::new(
                series_head()
                    .into_iter()
                    .chain(series_tail())
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
