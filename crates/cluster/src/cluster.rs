//! In-process N-node cluster for tests: build, initialize, find the leader,
//! and inject faults via the Switchboard.

use std::collections::BTreeMap;
use std::time::Duration;

use openraft::BasicNode;

use crate::network::Switchboard;
use crate::node::Node;
use crate::types::{NodeId, WriteBatch};

/// An in-process cluster of [`Node`]s wired together by one [`Switchboard`].
/// Owns the nodes; tests drive the group and inject faults through it.
pub struct Cluster {
    /// The replicas, indexed by id (`nodes[id]`).
    pub nodes: Vec<Node>,
    /// The shared transport / fault registry.
    pub sb: Switchboard,
}

impl Cluster {
    /// Build `n` nodes and initialize a single voting group `{0..n}`.
    pub async fn new(n: u64) -> Self {
        let sb = Switchboard::new();
        let mut nodes = Vec::new();
        for id in 0..n {
            nodes.push(Node::start(id, sb.clone()).await);
        }
        let members: BTreeMap<NodeId, BasicNode> =
            (0..n).map(|id| (id, BasicNode::default())).collect();
        nodes[0].raft.initialize(members).await.expect("initialize");
        Self { nodes, sb }
    }

    /// Borrow the node with the given id.
    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[id as usize]
    }

    /// Await a stable leader and return its id. Bounded so a stuck group fails
    /// the test instead of hanging.
    pub async fn wait_for_leader(&self) -> NodeId {
        self.nodes[0]
            .raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(|m| m.current_leader.is_some(), "leader elected")
            .await
            .expect("leader elected")
            .current_leader
            .expect("leader id")
    }

    /// The current leader node as seen by node 0's metrics, if any.
    pub fn leader(&self) -> Option<&Node> {
        let id = self.nodes[0].raft.metrics().borrow().current_leader?;
        Some(self.node(id))
    }

    /// Pause (crash) a node: it drops every RPC it would send or receive.
    pub fn pause(&self, id: NodeId) {
        self.sb.pause(id);
    }

    /// Resume a previously paused node.
    pub fn resume(&self, id: NodeId) {
        self.sb.resume(id);
    }

    /// Partition a node away from every other node (cut all its links).
    pub fn isolate(&self, id: NodeId) {
        for other in 0..self.nodes.len() as u64 {
            if other != id {
                self.sb.cut(id, other);
            }
        }
    }

    /// Clear every fault: all cuts healed and all paused nodes resumed.
    pub fn heal(&self) {
        self.sb.heal();
    }

    /// Propose a raw write batch on the current leader.
    pub async fn write(&self, ops: Vec<kv::WriteOp>) -> Result<(), String> {
        let leader = self.leader().ok_or("no leader")?;
        leader
            .raft
            .client_write(WriteBatch(ops))
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}
