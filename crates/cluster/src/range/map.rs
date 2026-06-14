//! Static, table-aligned key→range addressing. A `RangeMap` partitions the
//! `table_id` space into N contiguous ranges; range 0 always starts at table 0
//! (so it owns the reserved system/catalog keys) and the last range is unbounded.
//! This is a routing rule over table ids, not a slice of one shared keyspace —
//! each range is its own `sm_kv` (see the spec's storage-model note).

use catalog::TableId;
use kv::KvError;

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

/// Current range-descriptor blob format version.
pub const RANGE_MAP_VERSION: u8 = 1;

/// One range's descriptor: its id and the half-open `[start_table_id, end)` span
/// of table ids it owns. `end == None` is the unbounded last range. This is the
/// unit of the replicated meta-range layout (SP15); today it is derived from a
/// `RangeMap`'s boundaries, but storing `range_id` explicitly keeps the on-disk
/// format forward-compatible with non-positional ids (range splits, D4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeDescriptor {
    pub range_id: RangeId,
    pub start_table_id: TableId,
    pub end: Option<TableId>,
}

impl RangeMap {
    /// This map as an ordered list of range descriptors (range 0 first).
    pub fn descriptors(&self) -> Vec<RangeDescriptor> {
        (0..self.range_count() as RangeId)
            .map(|i| RangeDescriptor {
                range_id: i,
                start_table_id: if i == 0 {
                    0
                } else {
                    self.boundaries[i as usize - 1]
                },
                end: self.boundaries.get(i as usize).copied(),
            })
            .collect()
    }

    /// Serialize to the replicated descriptor blob. Format:
    /// `[version:u8][count:u32 BE]` then per range
    /// `[range_id:u32 BE][start:u32 BE][end_present:u8][end:u32 BE]`.
    pub fn to_descriptor_bytes(&self) -> Vec<u8> {
        let descs = self.descriptors();
        let mut out = vec![RANGE_MAP_VERSION];
        out.extend_from_slice(&(descs.len() as u32).to_be_bytes());
        for d in &descs {
            out.extend_from_slice(&d.range_id.to_be_bytes());
            out.extend_from_slice(&d.start_table_id.to_be_bytes());
            match d.end {
                Some(e) => {
                    out.push(1);
                    out.extend_from_slice(&e.to_be_bytes());
                }
                None => {
                    out.push(0);
                    out.extend_from_slice(&0u32.to_be_bytes());
                }
            }
        }
        out
    }

    /// Reconstruct a `RangeMap` from a descriptor blob. Returns
    /// `KvError::CorruptRow` (never panics) for truncated bytes, an unknown
    /// version, or a descriptor set that is not a contiguous `0..N` table-id
    /// partition starting at range 0 / table 0 — e.g. a forward-version blob that
    /// uses split features this slice does not support.
    pub fn from_descriptor_bytes(bytes: &[u8]) -> Result<RangeMap, KvError> {
        let mut cur = bytes;
        let version = take_u8(&mut cur)?;
        if version != RANGE_MAP_VERSION {
            return Err(KvError::CorruptRow(format!(
                "unknown range-map version {version}"
            )));
        }
        let count = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4")) as usize;
        if count == 0 {
            return Err(KvError::CorruptRow("range map has zero ranges".into()));
        }
        let mut boundaries = Vec::with_capacity(count.saturating_sub(1));
        let mut expected_start: TableId = 0;
        for i in 0..count {
            let range_id = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
            let start = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
            let end_present = take_u8(&mut cur)?;
            let end_raw = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
            if range_id as usize != i {
                return Err(KvError::CorruptRow(format!(
                    "range ids must be contiguous 0..N; got {range_id} at position {i}"
                )));
            }
            if start != expected_start {
                return Err(KvError::CorruptRow(format!(
                    "range {i} starts at {start}, expected {expected_start} (ranges must be contiguous)"
                )));
            }
            let is_last = i + 1 == count;
            match (is_last, end_present) {
                (true, 0) => {} // last range is unbounded
                (true, _) => {
                    return Err(KvError::CorruptRow("last range must be unbounded".into()));
                }
                (false, 1) => {
                    if end_raw <= start {
                        return Err(KvError::CorruptRow("range end must exceed start".into()));
                    }
                    boundaries.push(end_raw);
                    expected_start = end_raw;
                }
                (false, _) => {
                    return Err(KvError::CorruptRow("non-last range must be bounded".into()));
                }
            }
        }
        Ok(RangeMap { boundaries })
    }
}

fn take_u8(cur: &mut &[u8]) -> Result<u8, KvError> {
    let (h, rest) = cur
        .split_first()
        .ok_or_else(|| KvError::CorruptRow("truncated range map".into()))?;
    *cur = rest;
    Ok(*h)
}

fn take_n<'a>(cur: &mut &'a [u8], n: usize) -> Result<&'a [u8], KvError> {
    if cur.len() < n {
        return Err(KvError::CorruptRow("truncated range-map field".into()));
    }
    let (h, rest) = cur.split_at(n);
    *cur = rest;
    Ok(h)
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

    #[test]
    fn descriptor_bytes_round_trip() {
        for m in [
            RangeMap::single(),
            RangeMap::with_boundaries(vec![2]),
            RangeMap::with_boundaries(vec![10, 20]),
            RangeMap::with_boundaries(vec![1, 5, 100, 4096]),
        ] {
            let bytes = m.to_descriptor_bytes();
            let back = RangeMap::from_descriptor_bytes(&bytes).expect("decode");
            assert_eq!(back, m, "range map must round-trip through its blob");
        }
    }

    #[test]
    fn descriptors_describe_each_range_span() {
        let m = RangeMap::with_boundaries(vec![2]);
        let d = m.descriptors();
        assert_eq!(d.len(), 2);
        assert_eq!(
            d[0],
            RangeDescriptor {
                range_id: 0,
                start_table_id: 0,
                end: Some(2)
            }
        );
        assert_eq!(
            d[1],
            RangeDescriptor {
                range_id: 1,
                start_table_id: 2,
                end: None
            }
        );
    }

    #[test]
    fn corrupt_blobs_error_not_panic() {
        // Truncated.
        assert!(RangeMap::from_descriptor_bytes(&[RANGE_MAP_VERSION, 0, 0]).is_err());
        // Unknown version.
        assert!(RangeMap::from_descriptor_bytes(&[99, 0, 0, 0, 1]).is_err());
        // Empty range set.
        let mut empty = vec![RANGE_MAP_VERSION];
        empty.extend_from_slice(&0u32.to_be_bytes());
        assert!(RangeMap::from_descriptor_bytes(&empty).is_err());
    }

    #[test]
    fn non_contiguous_descriptor_set_is_rejected() {
        // A hand-built blob whose range 1 starts at 5 but range 0 ends at 2
        // (a gap) must be rejected, not silently accepted.
        let mut b = vec![RANGE_MAP_VERSION];
        b.extend_from_slice(&2u32.to_be_bytes()); // count = 2
        // range 0: id 0, start 0, end Some(2)
        b.extend_from_slice(&0u32.to_be_bytes());
        b.extend_from_slice(&0u32.to_be_bytes());
        b.push(1);
        b.extend_from_slice(&2u32.to_be_bytes());
        // range 1: id 1, start 5 (gap! should be 2), end None
        b.extend_from_slice(&1u32.to_be_bytes());
        b.extend_from_slice(&5u32.to_be_bytes());
        b.push(0);
        b.extend_from_slice(&0u32.to_be_bytes());
        assert!(RangeMap::from_descriptor_bytes(&b).is_err());
    }
}
