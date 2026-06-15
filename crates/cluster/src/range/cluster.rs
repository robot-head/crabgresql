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
    /// The ONE shared Global Transaction Manager for the whole cluster, opened over
    /// range 0's store and made the coordinator (`init_gtm_coordinator`). Every
    /// range's `leader_engine` copies this same `Arc<Gtm>` into its engine (via
    /// `share_gtm_to`), so any range can resolve a `Prepared(-> g)` row against
    /// range 0's global clog and the coordinator (`engines[&0]`) can drive the
    /// global decision. Held as a GTM-bearing range-0 `SqlEngine` because the
    /// cluster crate never names `Gtm` directly — it shares it through the engine.
    gtm_source: SqlEngine,
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
        let mut this = Self {
            n,
            map,
            groups,
            sb,
            // Placeholder; replaced below once range 0 has a leader. A fresh
            // in-memory engine is a cheap stand-in that is never used (the real
            // GTM-bearing source is built immediately after).
            gtm_source: SqlEngine::new(),
        };
        this.gtm_source = this.build_gtm_source().await;
        this
    }

    /// Build the ONE GTM coordinator engine: a range-0 leader engine with its GTM
    /// opened over range 0's store via `init_gtm_coordinator`. Every per-range
    /// `leader_engine` later copies this engine's `Arc<Gtm>` (see `share_gtm_to`),
    /// so the whole cluster shares a single global-xid allocator + global clog,
    /// and `engines[&0]` (the router's coordinator) drives the global decision.
    async fn build_gtm_source(&self) -> SqlEngine {
        let mut engine = self.raw_leader_engine(0).await;
        engine
            .init_gtm_coordinator()
            .expect("init GTM coordinator over range 0");
        engine
    }

    pub fn range_map(&self) -> &RangeMap {
        &self.map
    }

    pub fn switchboard(&self) -> &Switchboard {
        &self.sb
    }

    /// Range 0's **leader** applied catalog store — every range resolves table
    /// schemas from it. Pinned to the current leader (not a fixed node) because a
    /// just-committed `CREATE TABLE` is guaranteed applied on range 0's *leader*
    /// before it returns; a follower's store can lag and transiently surface
    /// `UndefinedTable` for a table resolution that races the apply. Resolved
    /// once at `RangeRouter::connect`, like the per-range leader engines.
    pub async fn catalog_kv(&self) -> Arc<dyn Kv> {
        let leader = self.wait_for_leader(0).await;
        self.groups[0].nodes[leader as usize].sm_kv.clone()
    }

    /// Await a stable, self-confirming leader for `range` and return its id, using
    /// openraft's event-based `wait` (no polling/sleep). Races a `wait` per replica
    /// for "this node reports `state == Leader` and names itself `current_leader`"
    /// and returns the first to satisfy it. Self-confirmation (not just some node's
    /// `current_leader` view) is essential: a just-resumed ex-leader transiently
    /// still names *itself* leader, so a naive `current_leader.is_some()` read can
    /// return a stale id during churn. Bounded per replica so a stuck group fails
    /// the test instead of hanging.
    pub async fn wait_for_leader(&self, range: RangeId) -> NodeId {
        let mut set = tokio::task::JoinSet::new();
        for node in &self.groups[range as usize].nodes {
            // A paused node's metrics are frozen, so it still self-reports `Leader`;
            // routing a write to it would block forever. Never treat it as leader.
            if self.sb.is_paused(node.id) {
                continue;
            }
            let raft = node.raft.clone();
            let id = node.id;
            set.spawn(async move {
                raft.wait(Some(Duration::from_secs(10)))
                    .metrics(
                        move |m| m.state == ServerState::Leader && m.current_leader == Some(id),
                        "self-confirmed leader",
                    )
                    .await
                    .map(|_| id)
                    .ok()
            });
        }
        while let Some(res) = set.join_next().await {
            if let Ok(Some(id)) = res {
                return id; // remaining waiters are aborted when `set` drops
            }
        }
        panic!("no node self-confirmed as leader for range {range} within the bound");
    }

    /// Await a leader for `range` that is **not** `old`, returning its id, using
    /// openraft's event-based `wait`. A *paused* node still reports itself `Leader`
    /// in its own metrics (pausing only drops its RPCs — openraft never tells it to
    /// step down), so after pausing/isolating `old` we must probe a node OTHER than
    /// `old` and wait until it observes a leader `l != old`. A probe stranded with
    /// `old` in the minority would time out, so we try the next probe. Bounded so a
    /// stuck group fails the test, never hangs. Mirrors the single-range
    /// `Cluster::wait_for_leader_excluding`.
    pub async fn wait_for_leader_excluding(&self, range: RangeId, old: NodeId) -> NodeId {
        let nodes = &self.groups[range as usize].nodes;
        for probe in 0..nodes.len() as u64 {
            if probe == old {
                continue;
            }
            let observed = nodes[probe as usize]
                .raft
                .wait(Some(Duration::from_secs(10)))
                .metrics(
                    |m| m.current_leader.is_some_and(|l| l != old),
                    "new leader excluding old",
                )
                .await;
            if let Ok(m) = observed
                && let Some(l) = m.current_leader.filter(|&l| l != old)
            {
                return l;
            }
        }
        panic!("no new leader (excluding {old}) for range {range} within the bound");
    }

    /// Await every replica of `range` applying up to the leader's current applied
    /// index — i.e. the latest committed write is visible on every node — using
    /// openraft's event-based `wait` (no polling/sleep). Bounded per node. Lets a
    /// test assert on follower stores deterministically instead of racing apply.
    pub async fn wait_for_replication(&self, range: RangeId) {
        let leader = self.wait_for_leader(range).await;
        let target = self.groups[range as usize].nodes[leader as usize]
            .raft
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0);
        for node in &self.groups[range as usize].nodes {
            // A paused node can't apply; don't wait on it (the caller resumes it
            // before relying on its store).
            if self.sb.is_paused(node.id) {
                continue;
            }
            node.raft
                .wait(Some(Duration::from_secs(10)))
                .metrics(
                    |m| m.last_applied.map(|l| l.index).unwrap_or(0) >= target,
                    "follower caught up to leader's applied index",
                )
                .await
                .expect("replication within bound");
        }
    }

    /// A replicated `SqlEngine` for `range`'s leader node: reads/writes go to that
    /// node's range replica; the catalog always resolves from range 0's store. The
    /// cluster's ONE shared GTM is injected so every range engine can resolve a
    /// cross-range `Prepared(-> g)` row and `engines[&0]` can drive the global
    /// decision — single-range engines built elsewhere keep `gtm: None`.
    pub async fn leader_engine(&self, range: RangeId) -> SqlEngine {
        let mut engine = self.raw_leader_engine(range).await;
        self.gtm_source.share_gtm_to(&mut engine);
        engine
    }

    /// The replicated `SqlEngine` for `range`'s leader node WITHOUT GTM injection —
    /// the base from which both `leader_engine` (which then shares the GTM in) and
    /// the GTM source itself (`build_gtm_source`, which initializes the GTM on it)
    /// are derived.
    async fn raw_leader_engine(&self, range: RangeId) -> SqlEngine {
        let leader = self.wait_for_leader(range).await;
        let node = &self.groups[range as usize].nodes[leader as usize];
        let engine = SqlEngine::replicated(
            self.catalog_kv().await,
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

    /// The leader node's `openraft::Raft` handle for `range` — for tests that need to drive a
    /// `RecoveryGate` (which reads `current_leader`/`current_term`) over the in-process cluster.
    pub async fn leader_raft(&self, range: RangeId) -> openraft::Raft<crate::types::TypeConfig> {
        let leader = self.wait_for_leader(range).await;
        self.groups[range as usize].nodes[leader as usize]
            .raft
            .clone()
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
