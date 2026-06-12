//! mvcc: commit-timestamp multiversion concurrency control primitives for
//! crabgresql — versioned keys, tombstone version values, snapshots, and
//! visibility. The durable store holds only committed versions (SP4); the
//! commit-status log (clog) arrives with concurrent writers in SP5.

pub mod clog;
pub mod snapshot;
pub mod version;

pub use snapshot::{Snapshot, visible_version};
pub use version::{commit_ts_of, decode_version, encode_version, version_key};
