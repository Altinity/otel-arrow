// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Aligned wall-clock windows driven by one persistent monotonic sleep.
//!
//! The sleep is a future the worker owns, not an engine periodic timer,
//! because the engine cancels periodic timers before it drains a node's
//! receivers. [`Window::wake`] also rotates after one interval of monotonic
//! time, so a wall clock stepping back holds a block for at most one interval.

use otel_arrow_dfe_engine::clock;
use otel_arrow_dfe_series_lake::clock::{WakeOutcome, WallClock, WindowClock, nanos_to_secs};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The window boundary the worker is currently waiting for.
pub(super) struct Window {
    /// Source of wall-clock time; the boundaries are aligned to it.
    pub(super) wall: Arc<dyn WallClock>,
    /// Boundary arithmetic and the last boundary that was consumed.
    pub(super) clock: WindowClock,
    /// Monotonic sleep that expires at the next boundary.
    pub(super) sleep: clock::Sleep,
    /// Monotonic instant the current window was opened or last rotated at.
    rotated_at: Instant,
    /// Window length, for the monotonic bound on a window's life.
    interval: Duration,
    /// Whether the last rotation was taken on monotonic time with the wall
    /// clock short of the boundary, so the next block keeps the same window
    /// start and must re-emit its descriptors.
    pub(super) floored: bool,
}

impl Window {
    /// Start a window aligned to the wall clock, with the next boundary armed.
    pub(super) fn new(interval: Duration, wall: Arc<dyn WallClock>) -> Self {
        let nanos = wall.now_unix_nanos();
        let clock = WindowClock::new(interval, nanos_to_secs(nanos));
        let rotated_at = clock::now();
        let sleep = Self::arm(
            clock.next_boundary(nanos_to_secs(nanos)),
            nanos,
            rotated_at + interval,
        );
        Self {
            wall,
            clock,
            sleep,
            rotated_at,
            interval,
            floored: false,
        }
    }

    /// A monotonic sleep expiring when the wall clock reaches `target_secs`,
    /// or at `cap` if that comes first.
    ///
    /// The distance is measured in wall time and handed to the monotonic clock,
    /// so a wall-clock step is absorbed by the wake that follows it; `cap`
    /// bounds a backward step to one interval. A target in the past becomes a
    /// zero delay; callers only arm a target ahead of `now_nanos`.
    fn arm(target_secs: i64, now_nanos: i64, cap: Instant) -> clock::Sleep {
        let delay = (i128::from(target_secs) * 1_000_000_000 - i128::from(now_nanos)).max(0);
        let nanos = u64::try_from(delay).unwrap_or(u64::MAX);
        let now = clock::now();
        let at = now.checked_add(Duration::from_nanos(nanos)).unwrap_or(cap);
        clock::sleep_until(at.min(cap.max(now)))
    }

    /// The monotonic instant the current window must be rotated by.
    fn deadline(&self) -> Instant {
        self.rotated_at + self.interval
    }

    /// Consume an expired sleep, re-arm it, and say whether to rotate.
    ///
    /// Re-arming is unconditional, so a wake that crossed no boundary, or a
    /// rotation blocked by an outstanding flush, keeps the timer running.
    ///
    /// A wake with the wall clock short of the boundary still rotates once one
    /// interval of monotonic time has passed since the last rotation, with no
    /// drift tolerance, so a backward step never holds a block past one
    /// interval and a slow clock costs one extra file set. The last boundary
    /// is kept, so the next block keeps the same, floored window start.
    pub(super) fn wake(&mut self) -> bool {
        let nanos = self.wall.now_unix_nanos();
        let now = nanos_to_secs(nanos);
        let (rotate, target) = match self.clock.on_wake(now) {
            WakeOutcome::RotationRequested { .. } => {
                self.floored = false;
                (true, self.clock.next_boundary(now))
            }
            WakeOutcome::TooEarly { sleep_until } => {
                let stalled = clock::now() >= self.deadline();
                if stalled {
                    self.floored = true;
                }
                (stalled, sleep_until)
            }
        };
        if rotate {
            self.rotated_at = clock::now();
        }
        let cap = self.deadline();
        self.sleep = Self::arm(target, nanos, cap);
        rotate
    }

    /// The window a request extracted at `admission_secs` belongs to.
    ///
    /// A request past the boundary the node waits for consumes that boundary
    /// here and re-arms the sleep for the next one, so the timer does not fire
    /// for it a second time.
    ///
    /// The result never moves backwards (`effective_boundary` is floored by the
    /// last consumed boundary), so a wall clock stepping back cannot reopen a
    /// written window and a parked request's window stays reachable by the
    /// block opened for it.
    pub(super) fn admission_boundary(&mut self, admission_secs: i64) -> i64 {
        let boundary = self.clock.effective_boundary(admission_secs);
        if boundary > self.clock.last_boundary() {
            let _ = self.clock.on_wake(admission_secs);
            self.rotated_at = clock::now();
            self.floored = false;
            let nanos = self.wall.now_unix_nanos();
            self.sleep = Self::arm(
                self.clock.next_boundary(nanos_to_secs(nanos)),
                nanos,
                self.deadline(),
            );
        }
        boundary
    }
}
