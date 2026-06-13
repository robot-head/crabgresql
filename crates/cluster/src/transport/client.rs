//! TCP implementation of openraft's network traits: dial a peer's node-addr,
//! send a framed Raft RPC, await the framed response. Reconnects on drop so a
//! peer restart heals on the next call; checks the local partition first.
use std::time::Duration;

use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use tokio::net::TcpStream;

use super::frame::{read_msg, write_msg};
use super::partition::PartitionState;
use super::protocol::{NodeRequest, NodeResponse, RaftRpc, RaftRpcResp};
use crate::types::{NodeId, TypeConfig};

/// One factory per node; mints a [`TcpConn`] per peer.
#[derive(Clone)]
pub struct TcpRaftNetwork {
    pub from: NodeId,
    pub partition: PartitionState,
}

impl RaftNetworkFactory<TypeConfig> for TcpRaftNetwork {
    type Network = TcpConn;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> TcpConn {
        TcpConn {
            from: self.from,
            target,
            addr: node.addr.clone(),
            partition: self.partition.clone(),
            stream: None,
        }
    }
}

/// A connection to one peer. `RaftNetwork` methods take `&mut self`, so calls are
/// serialized — one in-flight request over the held stream at a time.
pub struct TcpConn {
    from: NodeId,
    target: NodeId,
    addr: String,
    partition: PartitionState,
    stream: Option<TcpStream>,
}

impl TcpConn {
    /// Send `rpc`, returning the wire response, or `Err(())` if unreachable.
    /// Tries up to twice so a stale (peer-restarted) connection reconnects once.
    async fn call(&mut self, rpc: RaftRpc, timeout: Duration) -> Result<RaftRpcResp, ()> {
        if self.partition.blocked(self.target) {
            return Err(());
        }
        for _ in 0..2 {
            if self.stream.is_none() {
                match tokio::time::timeout(timeout, TcpStream::connect(&self.addr)).await {
                    Ok(Ok(s)) => self.stream = Some(s),
                    _ => return Err(()),
                }
            }
            let s = self.stream.as_mut().expect("connected");
            let req = NodeRequest::Raft {
                from: self.from,
                rpc: rpc.clone(),
            };
            let exchange = async {
                write_msg(s, &req).await?;
                read_msg::<_, NodeResponse>(s).await
            };
            match tokio::time::timeout(timeout, exchange).await {
                Ok(Ok(NodeResponse::Raft(resp))) => return Ok(resp),
                _ => {
                    self.stream = None; // drop + retry once (reconnect)
                }
            }
        }
        Err(())
    }

    /// Build an [`Unreachable`] RPC error for a dropped or unroutable RPC. The
    /// generic `E` lets one helper serve every method's distinct error type.
    fn unreachable<E: std::error::Error>(&self) -> RPCError<NodeId, BasicNode, E> {
        let msg = format!("node {} -> node {} unreachable", self.from, self.target);
        RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            msg,
        )))
    }
}

impl RaftNetwork<TypeConfig> for TcpConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        match self
            .call(RaftRpc::AppendEntries(rpc), option.hard_ttl())
            .await
        {
            Ok(RaftRpcResp::AppendEntries(Ok(r))) => Ok(r),
            Ok(RaftRpcResp::AppendEntries(Err(e))) => {
                Err(RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            _ => Err(self.unreachable()),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        match self
            .call(RaftRpc::InstallSnapshot(rpc), option.hard_ttl())
            .await
        {
            Ok(RaftRpcResp::InstallSnapshot(Ok(r))) => Ok(r),
            Ok(RaftRpcResp::InstallSnapshot(Err(e))) => {
                Err(RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            _ => Err(self.unreachable()),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        match self.call(RaftRpc::Vote(rpc), option.hard_ttl()).await {
            Ok(RaftRpcResp::Vote(Ok(r))) => Ok(r),
            Ok(RaftRpcResp::Vote(Err(e))) => {
                Err(RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            _ => Err(self.unreachable()),
        }
    }
}
