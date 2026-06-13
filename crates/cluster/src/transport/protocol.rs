//! Wire protocol: Raft RPC envelopes + a control channel, all postcard-serializable.
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};

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
    SetPartition(Vec<NodeId>),
    Heal,
    AddLearner { id: NodeId, addr: String },
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
    Ok,
    Err(String),
}

/// Top-level request envelope on the node port.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeRequest {
    Raft { from: NodeId, rpc: RaftRpc },
    Control(ControlRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeResponse {
    Raft(RaftRpcResp),
    Control(ControlResponse),
}
