// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Wall clock abstraction, partition ids and window boundary arithmetic.

use chrono::{DateTime, NaiveDate, Timelike, Utc};

/// A `date/hour` storage partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionId {
    /// Days since the Unix epoch (UTC).
    pub date: u32,
    /// Hour of day, 0..=23.
    pub hour: u8,
}

impl PartitionId {
    /// Partition of a Unix timestamp in seconds.
    ///
    /// `secs` below zero (before 1970), not a supported wall-clock input, is
    /// clamped to zero, so both `date` and `hour` collapse to the epoch.
    #[must_use]
    pub fn from_unix_secs(secs: i64) -> Self {
        let clamped = secs.max(0);
        let Some(dt) = DateTime::<Utc>::from_timestamp(clamped, 0) else {
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

    /// `YYYY-MM-DD` of the partition (proleptic Gregorian, UTC).
    #[must_use]
    pub fn date_string(&self) -> String {
        let secs = i64::from(self.date) * 86_400;
        DateTime::<Utc>::from_timestamp(secs, 0).map_or_else(
            || "1970-01-01".to_string(),
            |dt| dt.format("%Y-%m-%d").to_string(),
        )
    }

    /// `HH` of the partition.
    #[must_use]
    pub fn hour_string(&self) -> String {
        format!("{:02}", self.hour)
    }
}

#[cfg(any(test, feature = "testing"))]
use std::sync::Arc;
#[cfg(any(test, feature = "testing"))]
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Source of wall-clock time (injectable in tests).
pub trait WallClock: Send + Sync {
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

/// Manually driven wall clock for tests.
#[cfg(any(test, feature = "testing"))]
#[derive(Debug, Clone)]
pub struct TestWallClock(Arc<AtomicI64>);

#[cfg(any(test, feature = "testing"))]
impl TestWallClock {
    /// Start at `nanos`.
    #[must_use]
    pub fn new(nanos: i64) -> Self {
        Self(Arc::new(AtomicI64::new(nanos)))
    }

    /// Set the time.
    pub fn set(&self, nanos: i64) {
        self.0.store(nanos, Ordering::SeqCst);
    }

    /// Advance the time.
    pub fn advance(&self, nanos: i64) {
        let _ = self.0.fetch_add(nanos, Ordering::SeqCst);
    }
}

#[cfg(any(test, feature = "testing"))]
impl WallClock for TestWallClock {
    fn now_unix_nanos(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Nanoseconds to whole seconds (floor).
#[must_use]
pub fn nanos_to_secs(nanos: i64) -> i64 {
    nanos.div_euclid(1_000_000_000)
}

/// Nanoseconds to microseconds (floor).
#[must_use]
pub fn nanos_to_micros(nanos: i64) -> i64 {
    nanos.div_euclid(1_000)
}

/// Result of a timer wake-up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeOutcome {
    /// A boundary was crossed; rotate with this window start.
    RotationRequested {
        /// Effective boundary (window start of the new block).
        effective_boundary: i64,
    },
    /// The wall clock is still before the expected boundary (backward step); sleep again.
    TooEarly {
        /// Boundary to sleep until.
        sleep_until: i64,
    },
}

/// Aligned window boundary arithmetic: the `window_start` of FORMAT.md
/// section 4.
#[derive(Debug, Clone)]
pub struct WindowClock {
    interval_secs: i64,
    last_boundary_secs: i64,
}

impl WindowClock {
    /// Create with `last_boundary = boundary(start)`.
    ///
    /// `start_unix_secs` below zero is clamped to zero: negative Unix
    /// timestamps (before 1970) are not a supported wall-clock input for
    /// this crate, mirroring `PartitionId::from_unix_secs`'s day-0 clamp.
    ///
    /// `interval` is truncated to whole seconds and raised to at least 1 s.
    /// A validated configuration never reaches this clamp:
    /// `LakeConfig::validate` refuses a `window_interval` that is not a whole
    /// number of seconds of at least one, because the sink's `window_secs`
    /// metadata records whole seconds too and the two must agree. The clamp
    /// stays as a guard for a `WindowClock` built directly, so a zero interval
    /// cannot divide by zero.
    #[must_use]
    pub fn new(interval: Duration, start_unix_secs: i64) -> Self {
        let interval_secs = i64::try_from(interval.as_secs()).unwrap_or(15).max(1);
        let mut c = Self {
            interval_secs,
            last_boundary_secs: 0,
        };
        c.last_boundary_secs = c.boundary(start_unix_secs.max(0));
        c
    }

    /// `floor(t / interval) * interval`.
    ///
    /// `unix_secs` below zero is clamped to zero (see `new`).
    #[must_use]
    pub fn boundary(&self, unix_secs: i64) -> i64 {
        let secs = unix_secs.max(0);
        secs.div_euclid(self.interval_secs) * self.interval_secs
    }

    /// `max(boundary(now), last_boundary)`: never moves backwards.
    #[must_use]
    pub fn effective_boundary(&self, now_unix_secs: i64) -> i64 {
        self.boundary(now_unix_secs).max(self.last_boundary_secs)
    }

    /// The next boundary to sleep until, strictly after the effective boundary.
    #[must_use]
    pub fn next_boundary(&self, now_unix_secs: i64) -> i64 {
        self.effective_boundary(now_unix_secs) + self.interval_secs
    }

    /// Handle a wake-up at `now`.
    pub fn on_wake(&mut self, now_unix_secs: i64) -> WakeOutcome {
        let expected = self.last_boundary_secs + self.interval_secs;
        if now_unix_secs < expected {
            return WakeOutcome::TooEarly {
                sleep_until: expected,
            };
        }
        let effective = self.boundary(now_unix_secs);
        self.last_boundary_secs = effective;
        WakeOutcome::RotationRequested {
            effective_boundary: effective,
        }
    }

    /// Last consumed boundary.
    #[must_use]
    pub fn last_boundary(&self) -> i64 {
        self.last_boundary_secs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Scenario: 15 s windows starting at 12:00:07.
    /// Guarantees: boundary floors to the interval, next boundary is strictly ahead even exactly on a boundary.
    #[test]
    fn boundaries_are_aligned_and_strictly_advancing() {
        let mut c = WindowClock::new(Duration::from_secs(15), 7);
        assert_eq!(c.last_boundary(), 0);
        assert_eq!(c.next_boundary(7), 15);
        assert_eq!(c.next_boundary(15), 30); // exact boundary is not re-fired
        match c.on_wake(15) {
            WakeOutcome::RotationRequested { effective_boundary } => {
                assert_eq!(effective_boundary, 15)
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(c.last_boundary(), 15);
        assert_eq!(c.next_boundary(16), 30);
    }

    /// Scenario: the wall clock steps backwards after a boundary was consumed.
    /// Guarantees: no rotation, boundaries never move backwards, sleep targets the expected boundary.
    #[test]
    fn backward_step_does_not_reopen_window() {
        let mut c = WindowClock::new(Duration::from_secs(15), 20);
        let _ = c.on_wake(30);
        match c.on_wake(25) {
            WakeOutcome::TooEarly { sleep_until } => assert_eq!(sleep_until, 45),
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(c.last_boundary(), 30);
        assert_eq!(c.effective_boundary(25), 30);
    }

    /// Scenario: a long flush; the clock wakes 50 seconds late.
    /// Guarantees: missed boundaries coalesce into one rotation at the latest boundary.
    #[test]
    fn missed_boundaries_coalesce() {
        let mut c = WindowClock::new(Duration::from_secs(15), 0);
        match c.on_wake(65) {
            WakeOutcome::RotationRequested { effective_boundary } => {
                assert_eq!(effective_boundary, 60)
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(c.next_boundary(65), 75);
    }

    /// Scenario: negative wall-clock seconds fed to `new`, `boundary` and `on_wake`.
    /// Guarantees: they clamp to zero; `on_wake` reports `TooEarly` without moving `last_boundary`.
    #[test]
    fn negative_seconds_are_clamped_without_panicking() {
        let mut c = WindowClock::new(Duration::from_secs(15), -100);
        assert_eq!(c.last_boundary(), 0);
        assert_eq!(c.boundary(-5), 0);
        match c.on_wake(-5) {
            WakeOutcome::TooEarly { sleep_until } => assert_eq!(sleep_until, 15),
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(c.last_boundary(), 0);
    }

    /// Scenario: `WindowClock::new` is given a zero or sub-second interval.
    /// Guarantees: the interval is truncated to whole seconds and raised to
    /// at least 1 s, so boundary arithmetic never divides by zero.
    #[test]
    fn sub_second_interval_is_raised_to_one_second() {
        let c = WindowClock::new(Duration::from_millis(500), 10);
        assert_eq!(c.last_boundary(), 10);
        assert_eq!(c.boundary(11), 11);
        assert_eq!(c.next_boundary(10), 11);

        let c_zero = WindowClock::new(Duration::ZERO, 10);
        assert_eq!(c_zero.last_boundary(), 10);
        assert_eq!(c_zero.boundary(11), 11);
    }

    /// Scenario: partition of Unix seconds and the civil date rendering.
    /// Guarantees: date and hour strings are zero padded and correct for a known timestamp.
    #[test]
    fn partition_strings() {
        // 2026-09-21T03:15:00Z = 1789960500
        let p = PartitionId::from_unix_secs(1_789_960_500);
        assert_eq!(p.date_string(), "2026-09-21");
        assert_eq!(p.hour_string(), "03");
        let t = TestWallClock::new(5_000_000_000);
        t.advance(1);
        assert_eq!(t.now_unix_nanos(), 5_000_000_001);
        assert_eq!(nanos_to_secs(5_000_000_001), 5);
    }

    /// Scenario: 2026-09-21, 2000-02-29 (a leap century) and 2100-03-01 (after a non-leap century).
    /// Guarantees: `date_string` matches independently known literal dates.
    #[test]
    fn date_string_round_trips_for_dates_far_from_epoch() {
        let cases: [(i64, &str); 3] = [
            (1_789_960_500, "2026-09-21"), // 2026-09-21T03:15:00Z
            (951_825_600, "2000-02-29"),   // 2000-02-29T12:00:00Z (leap day)
            (4_107_565_800, "2100-03-01"), // 2100-03-01T06:30:00Z (non-leap century)
        ];
        for (secs, expected) in cases {
            let p = PartitionId::from_unix_secs(secs);
            assert_eq!(p.date_string(), expected, "secs={secs}");
        }
    }
}
