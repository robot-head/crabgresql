//! mvcc: PostgreSQL-faithful multiversion concurrency control for crabgresql —
//! xids, the clog (pg_xact), xid-keyed tuple (xmin/xmax) encoding, xid-list
//! snapshots, and HeapTupleSatisfiesMVCC visibility. Concurrent writers (row
//! locks, block-and-retry, EvalPlanQual) arrive in SP6; deadlock detection SP7.

pub mod clog;
pub mod version;
pub mod visibility;
pub mod xid;

pub use visibility::{Snapshot, satisfies_mvcc};
pub use xid::{INVALID_XID, Xid};
