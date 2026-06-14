//! Wire protocol: Raft RPC envelopes + a control channel, all JSON-serializable.
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};

use crate::range::RangeId;
use crate::types::{NodeId, TypeConfig};

/// One of the three Raft RPCs, as sent from a peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftRpc {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    Vote(VoteRequest<NodeId>),
}

/// The matching response (carrying openraft's `Result<Resp, RaftError>` verbatim).
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftRpcResp {
    AppendEntries(Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>),
    InstallSnapshot(
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
    ),
    Vote(Result<VoteResponse<NodeId>, RaftError<NodeId>>),
}

/// Test/harness control requests over the same node port.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlRequest {
    GetStatus,
    /// Returns `(range, current_leader)` for every Raft group registered on this node.
    RangeLeaders,
    SetPartition(Vec<NodeId>),
    Heal,
    AddLearner {
        id: NodeId,
        addr: String,
    },
    ChangeMembership(Vec<NodeId>),
    Shutdown,
}

/// A snapshot of a node's Raft metrics for the harness.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub id: NodeId,
    pub state: String,
    pub current_leader: Option<NodeId>,
    pub last_log_index: Option<u64>,
    pub last_applied: Option<u64>,
    pub members: Vec<NodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlResponse {
    Status(NodeStatus),
    /// `(range, current_leader)` for every Raft group on this node, sorted by range.
    RangeLeaders(Vec<(RangeId, Option<NodeId>)>),
    Ok,
    Err(String),
}

/// Structured cross-range 2PC requests on the node port. `BeginGlobal`,
/// `CommitGlobal`, and `GlobalBarrier` target range 0 (the GTM authority);
/// `Stage`/`Release` target the participant `range` (carried in the
/// `NodeRequest::Txn` envelope's `range`, which the server resolves the group with).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TxnRpc {
    BeginGlobal,
    Stage {
        g: u64,
        range: RangeId,
        sql: String,
    },
    CommitGlobal {
        g: u64,
        commit: bool,
    },
    Release {
        g: u64,
        range: RangeId,
        commit: bool,
    },
    GlobalBarrier,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TxnResp {
    Began {
        g: u64,
    },
    Staged,
    Committed,
    /// The global decision for `g` is Aborted (a participant won the write-once
    /// abort-race, or the client rolled back). The caller releases with abort semantics.
    Aborted,
    Released,
    Barrier {
        applied_index: u64,
    },
    /// Target was not the range's leader — caller re-resolves and retries.
    NotLeader,
    /// A retryable serialization failure / deadlock on the participant (40001 /
    /// 40P01) — surfaced to the client as retryable, not collapsed to 0A000.
    Retryable,
    Err(String),
}

/// Top-level request envelope on the node port.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeRequest {
    /// A Raft RPC for the `range`-th co-located group on this node. `range` is
    /// `#[serde(default)]` so a range-unaware payload (pre-D3a-net, or a peer
    /// that never sets it) decodes to range 0 — the single-range fast path.
    Raft {
        from: NodeId,
        #[serde(default)]
        range: RangeId,
        rpc: RaftRpc,
    },
    Control(ControlRequest),
    /// A structured 2PC RPC for the `range`-th co-located group on this node.
    Txn {
        range: RangeId,
        rpc: TxnRpc,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeResponse {
    Raft(RaftRpcResp),
    Control(ControlResponse),
    Txn(TxnResp),
}
