// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Wall clock, `date/hour` partitions and aligned window arithmetic (docs/FORMAT.md section 4).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, NaiveDate, Timelike, Utc};

/// A `date/hour` storage partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartitionId {
    /// Days since the Unix epoch (UTC).
    pub date: u32,
    /// Hour of day, 0..=23.
    pub hour: u8,
}

impl PartitionId {
    /// Partition of a Unix timestamp in seconds (negative input is clamped to the epoch).
    #[must_use]
    pub fn from_unix_secs(secs: i64) -> Self {
        let Some(dt) = DateTime::<Utc>::from_timestamp(secs.max(0), 0) else {
            return Self { date: 0, hour: 0 };
        };
        let Some(epoch) = NaiveDate::from_ymd_opt(1970, 1, 1) else {
            return Self { date: 0, hour: 0 };
        };
        let days = dt.date_naive().signed_duration_since(epoch).num_days();
        Self {
            date: u32::try_from(days).unwrap_or(0),
            hour: dt.hour() as u8,
        }
    }

    /// `YYYY-MM-DD` of the partition (UTC).
    #[must_use]
    pub fn date_string(&self) -> String {
        DateTime::<Utc>::from_timestamp(i64::from(self.date) * 86_400, 0).map_or_else(
            || "1970-01-01".to_owned(),
            |dt| dt.format("%Y-%m-%d").to_string(),
        )
    }

    /// `HH` of the partition.
    #[must_use]
    pub fn hour_string(&self) -> String {
        format!("{:02}", self.hour)
    }
}

/// `YYYYMMDDTHHMMSSZ` of a Unix timestamp in seconds (negative input is clamped to the epoch, like
/// [`PartitionId::from_unix_secs`], so a file name and its partition never disagree).
#[must_use]
pub fn utc_stamp(unix_secs: i64) -> String {
    DateTime::<Utc>::from_timestamp(unix_secs.max(0), 0).map_or_else(
        || "19700101T000000Z".to_owned(),
        |dt| dt.format("%Y%m%dT%H%M%SZ").to_string(),
    )
}

/// Source of wall-clock time (replaced in tests). Local to the node's thread, like the exporter.
pub trait WallClock {
    /// Unix time in nanoseconds.
    fn now_unix_nanos(&self) -> i64;
}

/// System wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn now_unix_nanos(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
            .unwrap_or(0)
    }
}

/// Aligned window arithmetic. Windows start at multiples of the interval since the Unix epoch.
#[derive(Debug, Clone, Copy)]
pub struct Window {
    interval_nanos: i64,
}

impl Window {
    /// Windows of `interval` (at least one nanosecond).
    #[must_use]
    pub fn new(interval: Duration) -> Self {
        Self {
            interval_nanos: i64::try_from(interval.as_nanos())
                .unwrap_or(i64::MAX)
                .max(1),
        }
    }

    /// Start (Unix seconds, floor) of the window containing `now_nanos`.
    #[must_use]
    pub const fn start_secs(&self, now_nanos: i64) -> i64 {
        let now = if now_nanos < 0 { 0 } else { now_nanos };
        (now - now % self.interval_nanos) / 1_000_000_000
    }

    /// `window_end` footer value of a window starting at `start_secs`.
    #[must_use]
    pub const fn end_secs(&self, start_secs: i64) -> i64 {
        start_secs + self.interval_nanos / 1_000_000_000
    }

    /// Time from `now_nanos` to the next window boundary, in `(0, interval]`. Computed from the
    /// wall clock on every wake, so a wall-clock step delays a rotation by at most one interval.
    #[must_use]
    pub const fn until_boundary(&self, now_nanos: i64) -> Duration {
        let now = if now_nanos < 0 { 0 } else { now_nanos };
        Duration::from_nanos((self.interval_nanos - now % self.interval_nanos) as u64)
    }
}

/// Manually driven wall clock for tests; clones share the time.
#[cfg(test)]
#[derive(Debug, Clone)]
pub struct TestWallClock(std::rc::Rc<std::cell::Cell<i64>>);

#[cfg(test)]
impl TestWallClock {
    /// Start at `unix_secs`.
    #[must_use]
    pub fn at_secs(unix_secs: i64) -> Self {
        Self(std::rc::Rc::new(std::cell::Cell::new(
            unix_secs * 1_000_000_000,
        )))
    }

    /// Advance the time.
    pub fn advance(&self, d: Duration) {
        self.0
            .set(self.0.get() + i64::try_from(d.as_nanos()).expect("fits"));
    }
}

#[cfg(test)]
impl WallClock for TestWallClock {
    fn now_unix_nanos(&self) -> i64 {
        self.0.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: 2026-09-21T03:15:07Z is mapped to a partition, a file stamp and a 15 s window; a negative time is mapped too.
    /// Guarantees: The partition is date 2026-09-21 hour 03, the window starts at 03:15:00 and ends 15 s later, the next boundary is 8 s away, and a negative time clamps to the epoch in both the partition and the stamp.
    #[test]
    fn partition_stamp_and_window_of_a_time() {
        let secs = 1_789_960_507; // 2026-09-21T03:15:07Z
        let p = PartitionId::from_unix_secs(secs);
        assert_eq!(
            (p.date_string().as_str(), p.hour_string().as_str()),
            ("2026-09-21", "03")
        );
        let w = Window::new(Duration::from_secs(15));
        let start = w.start_secs(secs * 1_000_000_000 + 5);
        assert_eq!(utc_stamp(start), "20260921T031500Z");
        assert_eq!(w.end_secs(start), start + 15);
        assert_eq!(
            w.until_boundary(secs * 1_000_000_000),
            Duration::from_secs(8)
        );
        assert_eq!(
            w.until_boundary(start * 1_000_000_000),
            Duration::from_secs(15)
        );
        assert_eq!(
            PartitionId::from_unix_secs(-5),
            PartitionId { date: 0, hour: 0 }
        );
        assert_eq!(utc_stamp(-5), "19700101T000000Z");
    }
}
