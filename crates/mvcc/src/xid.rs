//! Transaction ids. `Xid` is a plain `u64` (matching the codebase's rowid /
//! commit_ts convention). `INVALID_XID` (0) is the sentinel an `xmax` carries
//! while a version is live, and is never assigned to a real transaction (real
//! xids start at 1).

pub type Xid = u64;

/// The "no transaction" sentinel: a live version's `xmax`.
pub const INVALID_XID: Xid = 0;

/// Cross-range (global) transaction ids are allocated from this reserved high
/// half of the u64 space; every per-range local xid is `< GLOBAL_XID_BASE`. Keeps
/// range 0's global-clog keys disjoint from its own local-clog keys.
pub const GLOBAL_XID_BASE: Xid = 1 << 63;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn global_base_is_top_bit_and_above_realistic_local_xids() {
        assert_eq!(GLOBAL_XID_BASE, 1u64 << 63);
        #[allow(clippy::assertions_on_constants)]
        { assert!(1_000_000u64 < GLOBAL_XID_BASE); }
    }
}
