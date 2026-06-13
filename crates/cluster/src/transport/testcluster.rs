//! In-process multi-task cluster wired over loopback TCP — exercises the real
//! transport (serialize → socket → dispatch) without spawning OS processes.
#![cfg(test)]
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use tokio::net::TcpListener;

use super::client::TcpRaftNetwork;
use super::frame::{read_msg, write_msg};
use super::partition::PartitionState;
use super::protocol::{ControlRequest, ControlResponse, NodeRequest, NodeResponse, NodeStatus};
use super::server::{ShutdownSignal, serve_node_protocol};
use crate::store::{LogStore, StateMachineStore};
use crate::types::{NodeId, TypeConfig, WriteBatch};

pub struct TcpNode {
    pub id: NodeId,
    pub addr: String,
    pub raft: openraft::Raft<TypeConfig>,
}

pub struct TcpCluster {
    pub nodes: Vec<TcpNode>,
}

impl TcpCluster {
    /// Build `n` in-memory nodes each with a loopback node-listener, wired by
    /// `TcpRaftNetwork`, and initialize the group with their real addresses.
    pub async fn new(n: u64) -> Self {
        let cfg = Arc::new(
            openraft::Config {
                heartbeat_interval: 100,
                election_timeout_min: 300,
                election_timeout_max: 600,
                ..Default::default()
            }
            .validate()
            .expect("config"),
        );
        // Bind listeners first so addresses are known before initialize.
        let mut listeners = Vec::new();
        let mut addrs = Vec::new();
        for _ in 0..n {
            let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            addrs.push(l.local_addr().expect("addr").to_string());
            listeners.push(l);
        }
        let mut nodes = Vec::new();
        for id in 0..n {
            let partition = PartitionState::default();
            let net = TcpRaftNetwork {
                from: id,
                partition: partition.clone(),
            };
            let log = Arc::new(LogStore::default());
            let sm = Arc::new(StateMachineStore::default());
            let raft = openraft::Raft::new(id, cfg.clone(), net, log, sm)
                .await
                .expect("raft");
            let listener = listeners.remove(0);
            tokio::spawn(serve_node_protocol(
                listener,
                raft.clone(),
                partition.clone(),
                ShutdownSignal::default(),
            ));
            nodes.push(TcpNode {
                id,
                addr: addrs[id as usize].clone(),
                raft,
            });
        }
        let members: BTreeMap<NodeId, BasicNode> = (0..n)
            .map(|id| {
                (
                    id,
                    BasicNode {
                        addr: addrs[id as usize].clone(),
                    },
                )
            })
            .collect();
        nodes[0].raft.initialize(members).await.expect("initialize");
        Self { nodes }
    }

    pub async fn wait_for_leader(&self) -> NodeId {
        self.nodes[0]
            .raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(|m| m.current_leader.is_some(), "leader")
            .await
            .expect("leader")
            .current_leader
            .expect("id")
    }

    pub fn leader(&self) -> Option<&TcpNode> {
        self.nodes.iter().find(|n| {
            let m = n.raft.metrics().borrow().clone();
            m.state == openraft::ServerState::Leader && m.current_leader == Some(n.id)
        })
    }
}

impl TcpCluster {
    /// Send one control request to node `id` over its node-addr.
    pub async fn control(&self, id: NodeId, req: ControlRequest) -> ControlResponse {
        let mut s = tokio::net::TcpStream::connect(&self.nodes[id as usize].addr)
            .await
            .expect("connect");
        write_msg(&mut s, &NodeRequest::Control(req))
            .await
            .expect("write");
        match read_msg::<_, NodeResponse>(&mut s).await.expect("read") {
            NodeResponse::Control(r) => r,
            _ => panic!("expected control response"),
        }
    }

    pub async fn status(&self, id: NodeId) -> NodeStatus {
        match self.control(id, ControlRequest::GetStatus).await {
            ControlResponse::Status(s) => s,
            o => panic!("{o:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn elects_leader_and_replicates_over_tcp() {
    let c = TcpCluster::new(3).await;
    let _l = c.wait_for_leader().await;
    let leader = c.leader().expect("leader");
    // Propose a write through the real TCP transport; it must commit on a majority.
    leader
        .raft
        .client_write(WriteBatch(vec![kv::WriteOp::Put {
            key: kv::key::row_key(1, 1),
            value: b"v".to_vec(),
        }]))
        .await
        .expect("client_write");
    // Every node applies it (replication crossed real sockets).
    for n in &c.nodes {
        n.raft
            .wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(Some(2), "applied")
            .await
            .expect("apply");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_status_reports_leader() {
    let c = TcpCluster::new(3).await;
    let leader = c.wait_for_leader().await;
    let st = c.status(leader).await;
    assert_eq!(st.current_leader, Some(leader));
    assert_eq!(st.members.len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minority_partition_then_heal_over_tcp() {
    let c = TcpCluster::new(3).await;
    let leader = c.wait_for_leader().await;
    // Isolate a follower (minority = 1 node); the leader stays in the majority
    // (2 nodes) so it can still commit with quorum.
    let minority = (0..3u64).find(|&i| i != leader).expect("follower");
    // Bidirectional cut: minority blocks both others, and both others block minority.
    let others: Vec<u64> = (0..3u64).filter(|&i| i != minority).collect();
    c.control(minority, ControlRequest::SetPartition(others.clone()))
        .await;
    for &o in &others {
        c.control(o, ControlRequest::SetPartition(vec![minority]))
            .await;
    }
    // The majority (leader + one other follower) still commits.
    let l = c.leader().expect("leader");
    l.raft
        .client_write(WriteBatch(vec![kv::WriteOp::Put {
            key: kv::key::row_key(2, 2),
            value: b"w".to_vec(),
        }]))
        .await
        .expect("majority commits under partition");
    // Heal: remove all partitions so the minority can catch up.
    for id in 0..3u64 {
        c.control(id, ControlRequest::Heal).await;
    }
    c.nodes[minority as usize]
        .raft
        .wait(Some(Duration::from_secs(10)))
        .applied_index_at_least(Some(2), "minority catches up")
        .await
        .expect("catch up");
}
