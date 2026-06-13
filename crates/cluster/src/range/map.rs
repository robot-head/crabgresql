//! Static, table-aligned key→range addressing. A `RangeMap` partitions the
//! `table_id` space into N contiguous ranges; range 0 always starts at table 0
//! (so it owns the reserved system/catalog keys) and the last range is unbounded.
//! This is a routing rule over table ids, not a slice of one shared keyspace —
//! each range is its own `sm_kv` (see the spec's storage-model note).

use catalog::TableId;

/// Identifies one range / Raft group. A small integer.
pub type RangeId = u32;

/// A static partition of the `table_id` space into contiguous ranges.
/// `boundaries` are strictly-increasing, nonzero split points: range `i` covers
/// `[boundaries[i-1], boundaries[i])` with `boundaries[-1] = 0` and
/// `boundaries[len] = +inf`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeMap {
    boundaries: Vec<TableId>,
}

impl RangeMap {
    /// The degenerate single range covering every table (today's behavior).
    pub fn single() -> Self {
        Self {
            boundaries: Vec::new(),
        }
    }

    /// Build a range map from sorted, strictly-increasing, nonzero boundaries.
    /// Panics on an invalid boundary list (a programming error, not user input).
    pub fn with_boundaries(boundaries: Vec<TableId>) -> Self {
        assert!(
            boundaries.iter().all(|&b| b != 0),
            "0 cannot be a boundary: range 0 always starts at table 0"
        );
        assert!(
            boundaries.windows(2).all(|w| w[0] < w[1]),
            "range boundaries must be strictly increasing"
        );
        Self { boundaries }
    }

    /// Number of ranges (boundaries + 1).
    pub fn range_count(&self) -> usize {
        self.boundaries.len() + 1
    }

    /// The range that owns `table_id`'s data.
    pub fn range_for_table(&self, table_id: TableId) -> RangeId {
        // partition_point = count of boundaries <= table_id = the range index.
        self.boundaries.partition_point(|&b| b <= table_id) as RangeId
    }

    /// Every range id, `0..range_count()`.
    pub fn range_ids(&self) -> impl Iterator<Item = RangeId> {
        0..self.range_count() as RangeId
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_range_covers_all_tables() {
        let m = RangeMap::single();
        assert_eq!(m.range_count(), 1);
        assert_eq!(m.range_for_table(0), 0);
        assert_eq!(m.range_for_table(7), 0);
        assert_eq!(m.range_for_table(u32::MAX), 0);
    }

    #[test]
    fn boundaries_partition_table_ids_contiguously() {
        // 3 ranges: [0,10) -> 0, [10,20) -> 1, [20,inf) -> 2.
        let m = RangeMap::with_boundaries(vec![10, 20]);
        assert_eq!(m.range_count(), 3);
        assert_eq!(m.range_for_table(0), 0); // system/catalog (table 0) is in range 0
        assert_eq!(m.range_for_table(9), 0);
        assert_eq!(m.range_for_table(10), 1); // boundary is the start of the next range
        assert_eq!(m.range_for_table(19), 1);
        assert_eq!(m.range_for_table(20), 2);
        assert_eq!(m.range_for_table(1_000), 2);
    }

    #[test]
    fn boundaries_must_be_sorted_and_nonzero() {
        // 0 cannot be a boundary (range 0 always starts at 0); boundaries strictly increasing.
        assert!(std::panic::catch_unwind(|| RangeMap::with_boundaries(vec![0, 10])).is_err());
        assert!(std::panic::catch_unwind(|| RangeMap::with_boundaries(vec![20, 10])).is_err());
    }
}
