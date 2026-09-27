// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The rate limit of the exporter's per-request WARN lines.

use std::time::{Duration, Instant};

/// Shortest interval between two lines a [`WarnLimit`] lets through.
const INTERVAL: Duration = Duration::from_secs(1);

/// Lets at most one line through per [`INTERVAL`] and counts the rest.
///
/// Each line written says how many were left out since the previous one; the
/// exporter's metrics still count every request.
#[derive(Debug)]
pub(crate) struct WarnLimit {
    /// When the last line was let through.
    last: Option<Instant>,
    /// Lines left out since then.
    suppressed: u64,
}

impl WarnLimit {
    /// A limit that has let no line through yet.
    pub(crate) const fn new() -> Self {
        Self {
            last: None,
            suppressed: 0,
        }
    }

    /// Whether a line at `now` is written, and if it is, how many were left
    /// out since the previous one.
    pub(crate) fn admit(&mut self, now: Instant) -> Option<u64> {
        if self
            .last
            .is_some_and(|last| now.saturating_duration_since(last) < INTERVAL)
        {
            self.suppressed += 1;
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.suppressed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: lines at 0 ms, 10 ms, 500 ms, one interval after the first
    /// line, and 10 ms after that.
    /// Guarantees: the first line and the first one an interval after the
    /// last written line are let through, the latter naming the two left out
    /// between them, and every other one is suppressed and counted.
    #[test]
    fn one_line_per_interval_names_the_suppressed_lines() {
        let start = Instant::now();
        let mut limit = WarnLimit::new();
        assert_eq!(limit.admit(start), Some(0));
        assert_eq!(limit.admit(start + Duration::from_millis(10)), None);
        assert_eq!(limit.admit(start + Duration::from_millis(500)), None);
        assert_eq!(limit.admit(start + INTERVAL), Some(2));
        assert_eq!(
            limit.admit(start + INTERVAL + Duration::from_millis(10)),
            None
        );
    }
}
