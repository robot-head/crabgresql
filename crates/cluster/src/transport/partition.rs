//! App-layer network partitions: a shared set of peer ids whose RPCs are dropped.
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::types::NodeId;

/// A bidirectional partition fault, shared by a node's transport client + server.
#[derive(Clone, Default)]
pub struct PartitionState {
    blocked: Arc<Mutex<HashSet<NodeId>>>,
}

impl PartitionState {
    /// True if RPCs to/from `peer` should be dropped.
    pub fn blocked(&self, peer: NodeId) -> bool {
        self.blocked.lock().expect("partition lock").contains(&peer)
    }
    /// Replace the blocked set (a `SetPartition` control request).
    pub fn set(&self, peers: Vec<NodeId>) {
        *self.blocked.lock().expect("partition lock") = peers.into_iter().collect();
    }
    /// Clear all partitions (`Heal`).
    pub fn heal(&self) {
        self.blocked.lock().expect("partition lock").clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_heal_toggle_blocked() {
        let p = PartitionState::default();
        assert!(!p.blocked(2));
        p.set(vec![1, 2]);
        assert!(p.blocked(1) && p.blocked(2) && !p.blocked(0));
        p.heal();
        assert!(!p.blocked(1));
    }
}
