//! Multi-range (D3a): static range map, co-located per-range Raft groups, and
//! key→range SQL routing. In-process; the network analog is a later sub-slice.

pub mod map;

pub use map::{RangeId, RangeMap};
