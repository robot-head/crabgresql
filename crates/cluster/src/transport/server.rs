//! Node-protocol listener: dispatches inbound Raft RPCs to the local `Raft` and
//! answers control requests (status, partition toggle, membership, shutdown).
use std::collections::BTreeSet;
use std::sync::Arc;

use openraft::BasicNode;
use tokio::net::TcpListener;
use tokio::sync::Notify;

use super::frame::{read_msg, write_msg};
use super::partition::PartitionState;
use super::protocol::{
    ControlRequest, ControlResponse, NodeRequest, NodeResponse, NodeStatus, RaftRpc, RaftRpcResp,
};
use crate::types::{NodeId, TypeConfig};

type Raft = openraft::Raft<TypeConfig>;

/// Shared shutdown signal: a `Shutdown` control request fires it; the binary's
/// main awaits it and exits the process.
#[derive(Clone, Default)]
pub struct ShutdownSignal(Arc<Notify>);
impl ShutdownSignal {
    pub fn fire(&self) {
        self.0.notify_waiters();
    }
    pub async fn wait(&self) {
        self.0.notified().await;
    }
}

/// Serve the node protocol on `listener` until it errors. Spawns a task per
/// connection; each reads `NodeRequest`s and writes `NodeResponse`s.
pub async fn serve_node_protocol(
    listener: TcpListener,
    raft: Raft,
    partition: PartitionState,
    shutdown: ShutdownSignal,
) {
    loop {
        let Ok((sock, _)) = listener.accept().await else {
            return;
        };
        let (raft, partition, shutdown) = (raft.clone(), partition.clone(), shutdown.clone());
        tokio::spawn(async move {
            let mut sock = sock;
            loop {
                let req: NodeRequest = match read_msg(&mut sock).await {
                    Ok(r) => r,
                    Err(_) => return, // connection closed/broken
                };
                let resp = match req {
                    NodeRequest::Raft { from, rpc } => {
                        // Receive side of a partition: drop the connection so the
                        // caller sees an error (its own send-side check usually
                        // prevents the send; this covers asymmetric configs).
                        if partition.blocked(from) {
                            return;
                        }
                        NodeResponse::Raft(dispatch_raft(&raft, rpc).await)
                    }
                    NodeRequest::Control(c) => {
                        NodeResponse::Control(handle_control(&raft, &partition, &shutdown, c).await)
                    }
                };
                if write_msg(&mut sock, &resp).await.is_err() {
                    return;
                }
            }
        });
    }
}

async fn dispatch_raft(raft: &Raft, rpc: RaftRpc) -> RaftRpcResp {
    match rpc {
        RaftRpc::AppendEntries(r) => RaftRpcResp::AppendEntries(raft.append_entries(r).await),
        RaftRpc::InstallSnapshot(r) => RaftRpcResp::InstallSnapshot(raft.install_snapshot(r).await),
        RaftRpc::Vote(r) => RaftRpcResp::Vote(raft.vote(r).await),
    }
}

async fn handle_control(
    raft: &Raft,
    partition: &PartitionState,
    shutdown: &ShutdownSignal,
    req: ControlRequest,
) -> ControlResponse {
    match req {
        ControlRequest::GetStatus => {
            let m = raft.metrics().borrow().clone();
            let members: Vec<NodeId> = m.membership_config.membership().voter_ids().collect();
            ControlResponse::Status(NodeStatus {
                id: m.id,
                state: format!("{:?}", m.state),
                current_leader: m.current_leader,
                last_log_index: m.last_log_index,
                last_applied: m.last_applied.map(|l| l.index),
                members,
            })
        }
        ControlRequest::SetPartition(p) => {
            partition.set(p);
            ControlResponse::Ok
        }
        ControlRequest::Heal => {
            partition.heal();
            ControlResponse::Ok
        }
        ControlRequest::AddLearner { id, addr } => {
            match raft.add_learner(id, BasicNode { addr }, true).await {
                Ok(_) => ControlResponse::Ok,
                Err(e) => ControlResponse::Err(e.to_string()),
            }
        }
        ControlRequest::ChangeMembership(ids) => {
            let set: BTreeSet<NodeId> = ids.into_iter().collect();
            match raft.change_membership(set, false).await {
                Ok(_) => ControlResponse::Ok,
                Err(e) => ControlResponse::Err(e.to_string()),
            }
        }
        ControlRequest::Shutdown => {
            let _ = raft.shutdown().await;
            shutdown.fire();
            ControlResponse::Ok
        }
    }
}
