// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Tracks admitted input batches until every block holding their rows has landed.
//!
//! Each admitted batch holds one "admission" reference (released after all of its chunks were
//! pushed) plus one reference per open block that received its rows. When the count reaches zero
//! the batch is acknowledged. A failed block resolves its batches immediately with a Nack; later
//! events for an already resolved batch are ignored, so every batch resolves exactly once.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use otel_arrow_dfe_otap::pdata::OtapPdata;

/// Final outcome of an admitted batch.
#[derive(Debug)]
pub enum Completion {
    /// All rows have landed.
    Ack {
        /// Payload-free pdata carrying the Ack routing context.
        token: OtapPdata,
        /// Time since admission.
        elapsed: Duration,
    },
    /// Some rows could not be written.
    Nack {
        /// Payload-free pdata carrying the Nack routing context.
        token: OtapPdata,
        /// Human-readable reason.
        reason: String,
        /// Time since admission.
        elapsed: Duration,
    },
}

#[derive(Debug)]
struct PendingBatch {
    token: OtapPdata,
    refs: u32,
    admitted_at: Instant,
}

/// Map of pending batches. Bounded by the buffered rows: every pending batch has at least one row in
/// an open or flushing block, apart from the batch currently being admitted.
#[derive(Debug, Default)]
pub struct PendingAcks {
    entries: HashMap<u64, PendingBatch>,
    next_seq: u64,
}

impl PendingAcks {
    /// Admit a payload-free token; returns its sequence number. The admission reference is held.
    pub fn admit(&mut self, token: OtapPdata, now: Instant) -> u64 {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        let _ = self.entries.insert(
            seq,
            PendingBatch {
                token,
                refs: 1,
                admitted_at: now,
            },
        );
        seq
    }

    /// Record that `seq` has rows in one more open block.
    pub fn add_block(&mut self, seq: u64) {
        if let Some(p) = self.entries.get_mut(&seq) {
            p.refs += 1;
        }
    }

    /// Release the admission reference after all chunks of `seq` were pushed.
    pub fn release_admission(&mut self, seq: u64, now: Instant) -> Option<Completion> {
        self.release(seq, now)
    }

    /// A block holding rows of `seq` landed.
    pub fn block_landed(&mut self, seq: u64, now: Instant) -> Option<Completion> {
        self.release(seq, now)
    }

    /// A block holding rows of `seq` failed: resolve `seq` with a Nack now. Later events for `seq`
    /// are ignored.
    pub fn block_failed(&mut self, seq: u64, reason: &str, now: Instant) -> Option<Completion> {
        self.entries.remove(&seq).map(|p| Completion::Nack {
            token: p.token,
            reason: reason.to_owned(),
            elapsed: now.saturating_duration_since(p.admitted_at),
        })
    }

    /// Resolve every remaining batch with a Nack (shutdown).
    pub fn drain_nack(&mut self, reason: &str, now: Instant) -> Vec<Completion> {
        self.entries
            .drain()
            .map(|(_, p)| Completion::Nack {
                token: p.token,
                reason: reason.to_owned(),
                elapsed: now.saturating_duration_since(p.admitted_at),
            })
            .collect()
    }

    /// True while `seq` is unresolved.
    #[must_use]
    pub fn contains(&self, seq: u64) -> bool {
        self.entries.contains_key(&seq)
    }

    fn release(&mut self, seq: u64, now: Instant) -> Option<Completion> {
        let p = self.entries.get_mut(&seq)?;
        p.refs = p.refs.saturating_sub(1);
        if p.refs > 0 {
            return None;
        }
        let p = self.entries.remove(&seq)?;
        Some(Completion::Ack {
            token: p.token,
            elapsed: now.saturating_duration_since(p.admitted_at),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_otap::testing::create_test_pdata;

    fn is_ack(c: Option<Completion>) -> bool {
        matches!(c, Some(Completion::Ack { .. }))
    }

    /// Scenario: A batch has rows in two blocks; the blocks land one after the other.
    /// Guarantees: No completion before the admission reference and both blocks are released; Ack after the last.
    #[test]
    fn batch_acks_only_after_admission_and_all_blocks_land() {
        let now = Instant::now();
        let mut p = PendingAcks::default();
        let seq = p.admit(create_test_pdata(), now);
        p.add_block(seq);
        p.add_block(seq);
        assert!(p.release_admission(seq, now).is_none());
        assert!(p.block_landed(seq, now).is_none());
        assert!(is_ack(p.block_landed(seq, now)));
        assert!(!p.contains(seq));
    }

    /// Scenario: A batch produced no rows, so it never reached a block.
    /// Guarantees: Releasing the admission reference acks it immediately.
    #[test]
    fn batch_with_zero_blocks_acks_on_admission_release() {
        let now = Instant::now();
        let mut p = PendingAcks::default();
        let seq = p.admit(create_test_pdata(), now);
        assert!(is_ack(p.release_admission(seq, now)));
    }

    /// Scenario: One of a batch's blocks fails, then its other block lands.
    /// Guarantees: The failure Nacks the batch once and the later landing is ignored.
    #[test]
    fn failed_block_nacks_immediately_and_ignores_later_landings() {
        let now = Instant::now();
        let mut p = PendingAcks::default();
        let seq = p.admit(create_test_pdata(), now);
        p.add_block(seq);
        p.add_block(seq);
        assert!(p.release_admission(seq, now).is_none());
        assert!(matches!(
            p.block_failed(seq, "boom", now),
            Some(Completion::Nack { ref reason, .. }) if reason == "boom"
        ));
        assert!(p.block_landed(seq, now).is_none());
        assert!(p.block_failed(seq, "again", now).is_none());
        assert!(!p.contains(seq));
    }

    /// Scenario: Three batches are pending at shutdown.
    /// Guarantees: drain_nack resolves every one of them with a Nack and empties the map.
    #[test]
    fn drain_nack_resolves_everything() {
        let now = Instant::now();
        let mut p = PendingAcks::default();
        let seqs: Vec<u64> = (0..3).map(|_| p.admit(create_test_pdata(), now)).collect();
        let drained = p.drain_nack("shutdown", now);
        assert_eq!(drained.len(), 3);
        assert!(drained.iter().all(|c| matches!(c, Completion::Nack { .. })));
        assert!(seqs.iter().all(|&s| !p.contains(s)));
    }
}
