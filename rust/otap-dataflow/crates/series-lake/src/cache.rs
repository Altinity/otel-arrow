// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bounded LRU of series ids to the partition their descriptor was last
//! committed to. The cache is an optimization, never correctness state:
//! losing entries only causes descriptor re-emission.

use std::num::NonZeroUsize;

use lru::LruCache;

use crate::canonical::SeriesId;
use crate::clock::PartitionId;

/// Cache counters since creation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    /// Lookups that found the id with the requested partition.
    pub hits: u64,
    /// Lookups that did not.
    pub misses: u64,
    /// Entries evicted by capacity.
    pub evictions: u64,
}

/// The series cache.
pub struct SeriesCache {
    inner: LruCache<SeriesId, Option<PartitionId>>,
    stats: CacheStats,
}

impl SeriesCache {
    /// Create a cache holding at most `max_entries` ids (at least 1).
    ///
    /// Nothing is allocated up front, so a bound written to mean "no practical
    /// limit" reserves nothing at startup.
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        let cap = NonZeroUsize::new(max_entries.max(1)).expect("max(1) is non-zero");
        let mut inner = LruCache::unbounded();
        inner.resize(cap);
        Self {
            inner,
            stats: CacheStats::default(),
        }
    }

    /// Whether the descriptor is known to be committed in `partition`. Touches the entry.
    pub fn is_committed(&mut self, id: &SeriesId, partition: PartitionId) -> bool {
        let hit = self.inner.get(id).is_some_and(|p| *p == Some(partition));
        if hit {
            self.stats.hits += 1;
        } else {
            self.stats.misses += 1;
        }
        hit
    }

    /// Insert the id if absent (no committed partition) and mark it most recently used.
    pub fn touch(&mut self, id: SeriesId) {
        if self.inner.get(&id).is_none() {
            self.insert(id, None);
        }
    }

    /// Record that the descriptor was committed in `partition`.
    pub fn mark_committed(&mut self, id: SeriesId, partition: PartitionId) {
        self.insert(id, Some(partition));
    }

    /// Last durable descriptor partition, without changing recency or hit counters.
    ///
    /// Unlike [`SeriesCache::is_committed`] it leaves LRU order untouched, so
    /// a caller asking where a descriptor last landed keeps no entry alive.
    #[must_use]
    pub fn last_committed(&self, id: &SeriesId) -> Option<PartitionId> {
        self.inner.peek(id).copied().flatten()
    }

    fn insert(&mut self, id: SeriesId, p: Option<PartitionId>) {
        if self.inner.len() == self.inner.cap().get() && !self.inner.contains(&id) {
            self.stats.evictions += 1;
        }
        let _ = self.inner.put(id, p);
    }

    /// Number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Counters.
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::PartitionId;

    fn id(n: u8) -> SeriesId {
        [n; 16]
    }

    /// Scenario: a cache created with `usize::MAX` entries, then used.
    /// Guarantees: creation allocates nothing and the cache works.
    #[test]
    fn an_unbounded_capacity_is_not_preallocated() {
        let mut cache = SeriesCache::new(usize::MAX);
        cache.touch(id(1));
        cache.mark_committed(id(1), PartitionId { date: 1, hour: 2 });
        assert!(cache.is_committed(&id(1), PartitionId { date: 1, hour: 2 }));
        assert_eq!(cache.len(), 1);
    }

    /// Scenario: a series is touched, then committed for hour P, then asked about P and Q.
    /// Guarantees: committed only for the partition it was marked with.
    #[test]
    fn committed_partition_tracking() {
        let p = PartitionId {
            date: 20_000,
            hour: 3,
        };
        let q = PartitionId {
            date: 20_000,
            hour: 4,
        };
        let mut c = SeriesCache::new(10);
        assert!(!c.is_committed(&id(1), p));
        c.touch(id(1));
        assert!(!c.is_committed(&id(1), p));
        c.mark_committed(id(1), p);
        assert!(c.is_committed(&id(1), p));
        assert!(!c.is_committed(&id(1), q));
        // Four lookups: absent -> miss, present with no partition -> miss,
        // present with p -> hit, asked for q -> miss.
        assert_eq!(c.stats().hits, 1);
        assert_eq!(c.stats().misses, 3);
    }

    /// Scenario: an id that was never touched, one only touched, one committed
    /// in one partition then another, all read with `last_committed`.
    /// Guarantees: `last_committed` reports the most recent committed
    /// partition (or none) and never counts as a hit or a miss.
    #[test]
    fn last_committed_reads_without_touching_stats_or_recency() {
        let p = PartitionId { date: 1, hour: 0 };
        let q = PartitionId { date: 1, hour: 1 };
        let mut c = SeriesCache::new(10);
        assert_eq!(c.last_committed(&id(1)), None, "never seen");
        c.touch(id(2));
        assert_eq!(c.last_committed(&id(2)), None, "touched but not committed");
        c.mark_committed(id(3), p);
        assert_eq!(c.last_committed(&id(3)), Some(p));
        c.mark_committed(id(3), q);
        assert_eq!(c.last_committed(&id(3)), Some(q), "the newer commit wins");
        assert_eq!(
            c.stats(),
            CacheStats::default(),
            "peeking never records a hit, miss or eviction"
        );
    }

    /// Scenario: capacity 2, two committed ids, the older peeked with `last_committed`, then a
    /// third committed.
    /// Guarantees: the peeked id is still evicted first and no counter moved.
    #[test]
    fn last_committed_does_not_refresh_recency() {
        let p = PartitionId { date: 1, hour: 0 };
        let mut c = SeriesCache::new(2);
        c.mark_committed(id(1), p);
        c.mark_committed(id(2), p);
        assert_eq!(
            c.last_committed(&id(1)),
            Some(p),
            "peek id 1 without touching it"
        );
        assert_eq!(
            c.stats(),
            CacheStats::default(),
            "the peek recorded no hit, miss or eviction"
        );
        c.mark_committed(id(3), p); // evicts whichever id is least recently used
        assert_eq!(c.len(), 2);
        assert_eq!(
            c.last_committed(&id(1)),
            None,
            "id 1 was evicted: the peek did not make it recently used"
        );
        assert_eq!(c.last_committed(&id(2)), Some(p));
        assert_eq!(c.last_committed(&id(3)), Some(p));
    }

    /// Scenario: capacity 2, three distinct ids touched in order.
    /// Guarantees: the least recently used id is evicted and counted; a lost entry reads as not committed.
    #[test]
    fn evicts_least_recently_used() {
        let p = PartitionId { date: 1, hour: 0 };
        let mut c = SeriesCache::new(2);
        c.mark_committed(id(1), p);
        c.mark_committed(id(2), p);
        assert!(c.is_committed(&id(1), p)); // 1 becomes most recent
        c.mark_committed(id(3), p); // evicts 2
        assert_eq!(c.len(), 2);
        assert_eq!(c.stats().evictions, 1);
        assert!(!c.is_committed(&id(2), p));
        assert!(c.is_committed(&id(1), p));
    }
}
