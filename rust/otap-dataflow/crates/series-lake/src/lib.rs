// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Series/values normalization and Parquet sink for OTAP telemetry.
//!
//! See `docs/FORMAT.md` for the storage format and `README.md` for the
//! modules, the producer id contract and the limits of version 1.

pub(crate) mod attrs;
pub mod buffer;
pub mod cache;
pub mod canonical;
pub mod clock;
pub mod config;
pub mod error;
pub mod extract;
/// Object store wrapper the sink observes its writes through; public only for
/// the fault injection of dependent crates' tests.
#[doc(hidden)]
pub mod hook_store;
pub mod schema;
pub mod sink;
pub mod sort;
pub mod value;

pub use error::{Error, Excess, InternalError, RefuseReason, Result, SizeBudget, TransientError};
