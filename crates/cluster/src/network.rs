//! Controllable in-process Raft transport. All RPCs go through the Switchboard,
//! which can drop them to model partitions and paused (crashed) nodes.
//!
//! The trait surface is openraft 0.9.24's split network: a
//! [`RaftNetworkFactory`] mints one [`RaftNetwork`] client ([`Conn`]) per
//! target. We carry the owning node's id as `from` so partitions are
//! directional — a cut `{a, b}` drops RPCs in both directions, but a paused
//! node drops only the RPCs it would send or receive. Blocked RPCs surface as
//! [`Unreachable`], which is what openraft expects for a peer that is down.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::types::{NodeId, TypeConfig};

/// Mutable fault state shared by every client minted from a [`Switchboard`].
#[derive(Default)]
struct Faults {
    /// Nodes that drop all inbound/outbound RPCs (crash / pause).
    paused: HashSet<NodeId>,
    /// Unordered pairs `{a, b}` whose link is cut (partition).
    cuts: HashSet<(NodeId, NodeId)>,
}

/// Shared registry of node Raft handles plus mutable fault state. Cloning is
/// cheap (it shares the underlying `Arc`s), so every node and client holds one.
#[derive(Clone, Default)]
pub struct Switchboard {
    handles: Arc<Mutex<HashMap<NodeId, openraft::Raft<TypeConfig>>>>,
    faults: Arc<Mutex<Faults>>,
}

impl Switchboard {
    /// A fresh switchboard with no registered nodes and no faults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a node's Raft handle so peers can route RPCs to it.
    pub fn register(&self, id: NodeId, raft: openraft::Raft<TypeConfig>) {
        self.handles
            .lock()
            .expect("switchboard handles")
            .insert(id, raft);
    }

    /// Drop a node's registered Raft handle (used on restart so the old handle —
    /// and the fjall Database it transitively keeps alive — is released before the
    /// node is reopened from disk).
    pub fn deregister(&self, id: NodeId) {
        self.handles
            .lock()
            .expect("switchboard handles")
            .remove(&id);
    }

    /// Pause (crash) a node: it drops every RPC it would send or receive.
    pub fn pause(&self, id: NodeId) {
        self.faults
            .lock()
            .expect("switchboard faults")
            .paused
            .insert(id);
    }

    /// Resume a previously paused node.
    pub fn resume(&self, id: NodeId) {
        self.faults
            .lock()
            .expect("switchboard faults")
            .paused
            .remove(&id);
    }

    /// Cut the link between `a` and `b` in both directions (a partition).
    pub fn cut(&self, a: NodeId, b: NodeId) {
        self.faults
            .lock()
            .expect("switchboard faults")
            .cuts
            .insert(norm(a, b));
    }

    /// Clear every fault: all cuts healed and all paused nodes resumed.
    pub fn heal(&self) {
        let mut f = self.faults.lock().expect("switchboard faults");
        f.cuts.clear();
        f.paused.clear();
    }

    /// Per-node network factory carrying the owning node's id as `from`.
    pub fn for_node(&self, from: NodeId) -> NodeFactory {
        NodeFactory {
            sb: self.clone(),
            from,
        }
    }

    /// True if an RPC from `from` to `to` should be dropped: either endpoint is
    /// paused, or the link between them is cut.
    fn blocked(&self, from: NodeId, to: NodeId) -> bool {
        let f = self.faults.lock().expect("switchboard faults");
        f.paused.contains(&from) || f.paused.contains(&to) || f.cuts.contains(&norm(from, to))
    }

    /// Clone the target's Raft handle out of the registry. Returns an owned
    /// handle so the caller never holds the mutex across an `.await`.
    fn handle(&self, to: NodeId) -> Option<openraft::Raft<TypeConfig>> {
        self.handles
            .lock()
            .expect("switchboard handles")
            .get(&to)
            .cloned()
    }
}

/// Normalize a node pair so `{a, b}` and `{b, a}` hash equal.
fn norm(a: NodeId, b: NodeId) -> (NodeId, NodeId) {
    if a <= b { (a, b) } else { (b, a) }
}

/// Per-node factory: openraft owns one `RaftNetworkFactory` per node, and it
/// mints a [`Conn`] for each peer it wants to talk to.
#[derive(Clone)]
pub struct NodeFactory {
    sb: Switchboard,
    from: NodeId,
}

/// A network client from `from` to `target`, routing through the Switchboard.
pub struct Conn {
    sb: Switchboard,
    from: NodeId,
    target: NodeId,
}

impl Conn {
    /// Build an [`Unreachable`] RPC error for a dropped or unroutable RPC. The
    /// generic `E` lets one helper serve every method's distinct error type.
    fn unreachable<E>(&self) -> RPCError<NodeId, BasicNode, E>
    where
        E: std::error::Error,
    {
        let msg = format!("node {} -> node {} unreachable", self.from, self.target);
        RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            msg,
        )))
    }

    /// Resolve the live target handle, or `None` if the RPC is blocked by a
    /// fault or the target is unregistered. Drops the switchboard locks before
    /// returning so the caller never holds a `std::sync::Mutex` across `.await`.
    fn resolve(&self) -> Option<openraft::Raft<TypeConfig>> {
        if self.sb.blocked(self.from, self.target) {
            return None;
        }
        self.sb.handle(self.target)
    }
}

impl RaftNetworkFactory<TypeConfig> for NodeFactory {
    type Network = Conn;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        Conn {
            sb: self.sb.clone(),
            from: self.from,
            target,
        }
    }
}

impl RaftNetwork<TypeConfig> for Conn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let Some(raft) = self.resolve() else {
            return Err(self.unreachable());
        };
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let Some(raft) = self.resolve() else {
            return Err(self.unreachable());
        };
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let Some(raft) = self.resolve() else {
            return Err(self.unreachable());
        };
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}
