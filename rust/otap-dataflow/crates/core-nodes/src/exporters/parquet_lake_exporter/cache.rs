// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bounded LRU of series ids to the partition whose landed block carried their series row. The
//! cache is an optimization, never correctness state: losing entries only re-emits series rows.

use std::num::NonZeroUsize;

use lru::LruCache;

use super::window::PartitionId;

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
    inner: LruCache<u128, PartitionId>,
    stats: CacheStats,
}

impl SeriesCache {
    /// A cache of at most `max_entries` ids (at least 1). Nothing is allocated up front.
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        let cap = NonZeroUsize::new(max_entries.max(1)).unwrap_or(NonZeroUsize::MIN);
        let mut inner = LruCache::unbounded();
        inner.resize(cap);
        Self {
            inner,
            stats: CacheStats::default(),
        }
    }

    /// Whether the series row of `id` is known to have landed in `partition`. Touches the entry.
    pub fn is_committed(&mut self, id: u128, partition: PartitionId) -> bool {
        let hit = self.inner.get(&id).is_some_and(|p| *p == partition);
        if hit {
            self.stats.hits += 1;
        } else {
            self.stats.misses += 1;
        }
        hit
    }

    /// Record that a block carrying the series row of `id` landed in `partition`.
    pub fn mark_committed(&mut self, id: u128, partition: PartitionId) {
        if self.inner.len() == self.inner.cap().get() && !self.inner.contains(&id) {
            self.stats.evictions += 1;
        }
        let _ = self.inner.put(id, partition);
    }

    /// Number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Counters.
    #[must_use]
    pub const fn stats(&self) -> CacheStats {
        self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: An id is looked up before it is marked, after it is marked for hour 3, and for hour 4; then a cache of two entries receives a third id.
    /// Guarantees: Only a lookup for the marked partition hits; another hour misses (the series row is written again); capacity evicts the least recently used id and counts it.
    #[test]
    fn commit_is_per_partition_and_capacity_evicts_lru() {
        let (h3, h4) = (
            PartitionId { date: 1, hour: 3 },
            PartitionId { date: 1, hour: 4 },
        );
        let mut c = SeriesCache::new(2);
        assert!(!c.is_committed(1, h3));
        c.mark_committed(1, h3);
        assert!(c.is_committed(1, h3));
        assert!(!c.is_committed(1, h4));
        c.mark_committed(2, h3);
        assert!(c.is_committed(1, h3)); // 1 is now the most recently used
        c.mark_committed(3, h3); // evicts 2
        assert_eq!(c.len(), 2);
        assert!(!c.is_committed(2, h3));
        assert_eq!(
            c.stats(),
            CacheStats {
                hits: 2,
                misses: 3,
                evictions: 1
            }
        );
    }
}
