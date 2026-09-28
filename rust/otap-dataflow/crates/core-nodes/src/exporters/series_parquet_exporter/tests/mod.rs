// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Tests of the series Parquet exporter, by topic. Shared helpers live in
//! [`support`].

mod admission;
mod config;
mod flush;
mod metrics;
mod model;
mod rotation;
mod shutdown;
mod support;

pub(super) use support::{assert_no_more_completions, effects, empty_pdata};
