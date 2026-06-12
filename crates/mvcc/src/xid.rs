//! Transaction ids. `Xid` is a plain `u64` (matching the codebase's rowid /
//! commit_ts convention). `INVALID_XID` (0) is the sentinel an `xmax` carries
//! while a version is live, and is never assigned to a real transaction (real
//! xids start at 1).

pub type Xid = u64;

/// The "no transaction" sentinel: a live version's `xmax`.
pub const INVALID_XID: Xid = 0;
