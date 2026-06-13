//! N co-located in-process Raft groups (one per range), built over one shared
//! range-aware `Switchboard`. Each (range, node) is its own Raft replica with its
//! own applied `sm_kv`; range 0's `sm_kv` additionally holds the catalog, which
//! every data range resolves schemas from.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use executor::SqlEngine;
use kv::Kv;
use openraft::{BasicNode, ServerState};

use crate::committer::RaftCommitter;
use crate::linearizer::RaftLinearizer;
use crate::network::Switchboard;
use crate::node::Node;
use crate::range::map::{RangeId, RangeMap};
use crate::types::NodeId;

/// One range's replicas (its Raft `Node`s) across the physical node set.
struct RangeGroup {
    nodes: Vec<Node>,
}

/// An in-process multi-range cluster: `n` physical nodes, each running a replica
/// of every range. Range 0's `sm_kv` holds the catalog.
pub struct MultiRangeCluster {
    n: u64,
    map: RangeMap,
    groups: Vec<RangeGroup>, // indexed by RangeId
    sb: Switchboard,
}

impl MultiRangeCluster {
    /// Build `n` nodes × `map.range_count()` ranges and initialize each range's
    /// voting group `{0..n}`.
    pub async fn new(n: u64, map: RangeMap) -> Self {
        let sb = Switchboard::new();
        let mut groups = Vec::new();
        for r in map.range_ids() {
            let mut nodes = Vec::new();
            for id in 0..n {
                nodes
                    .push(Node::start_with_config(r, id, sb.clone(), Node::default_config()).await);
            }
            let members: BTreeMap<NodeId, BasicNode> =
                (0..n).map(|id| (id, BasicNode::default())).collect();
            nodes[0]
                .raft
                .initialize(members)
                .await
                .expect("initialize range group");
            groups.push(RangeGroup { nodes });
        }
        Self { n, map, groups, sb }
    }

    pub fn range_map(&self) -> &RangeMap {
        &self.map
    }

    pub fn switchboard(&self) -> &Switchboard {
        &self.sb
    }

    /// Range 0's applied catalog store (every data range resolves schema from it).
    pub fn catalog_kv(&self) -> Arc<dyn Kv> {
        self.groups[0].nodes[0].sm_kv.clone()
    }

    /// Block until `range` has a stable leader; return its node id.
    pub async fn wait_for_leader(&self, range: RangeId) -> NodeId {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            for node in &self.groups[range as usize].nodes {
                let m = node.raft.metrics().borrow().clone();
                if m.state == ServerState::Leader && m.current_leader == Some(m.id) {
                    return m.id;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "range {range} elected no leader"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// A replicated `SqlEngine` for `range`'s leader node: reads/writes go to that
    /// node's range replica; the catalog always resolves from range 0's store.
    pub async fn leader_engine(&self, range: RangeId) -> SqlEngine {
        let leader = self.wait_for_leader(range).await;
        let node = &self.groups[range as usize].nodes[leader as usize];
        let engine = SqlEngine::replicated(
            self.catalog_kv(),
            node.sm_kv.clone(),
            Arc::new(RaftCommitter {
                raft: node.raft.clone(),
            }),
            Arc::new(RaftLinearizer {
                raft: node.raft.clone(),
            }),
        )
        .expect("replicated engine");
        engine.reseed_counters().ok();
        engine
    }

    /// The applied `sm_kv` of `(range, node)` — for asserting where rows landed.
    pub fn sm_kv(&self, range: RangeId, node: NodeId) -> Arc<dyn Kv> {
        self.groups[range as usize].nodes[node as usize]
            .sm_kv
            .clone()
    }

    /// Pause a physical node (all its range replicas) — node-scoped fault.
    pub fn pause(&self, id: NodeId) {
        self.sb.pause(id);
    }
    pub fn resume(&self, id: NodeId) {
        self.sb.resume(id);
    }
    pub fn heal(&self) {
        self.sb.heal();
    }
    pub fn n(&self) -> u64 {
        self.n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_range_elects_a_leader() {
        // 3 nodes, 2 ranges (boundary at table_id 10).
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![10])).await;
        for r in c.range_map().range_ids() {
            let leader = c.wait_for_leader(r).await;
            assert!(leader < 3, "range {r} elected a valid leader");
        }
    }
}
