//! Multi-range (D3a): static range map, co-located per-range Raft groups, and
//! key→range SQL routing. In-process; the network analog is a later sub-slice.

pub mod cluster;
pub mod map;
pub mod router;

pub use cluster::MultiRangeCluster;
pub use map::{RangeId, RangeMap};
pub use router::RangeRouter;
