//! In-process multi-task cluster wired over loopback TCP — exercises the real
//! transport (serialize → socket → dispatch) without spawning OS processes.
#![cfg(test)]
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use tokio::net::TcpListener;

use super::client::TcpRaftNetwork;
use super::partition::PartitionState;
use super::server::{ShutdownSignal, serve_node_protocol};
use crate::store::{LogStore, StateMachineStore};
use crate::types::{NodeId, TypeConfig, WriteBatch};

pub struct TcpNode {
    pub id: NodeId,
    // `addr`/`partition` are read by the control + partition tests added in Task 4
    // (`control`/`status` dial `addr`; the partition test toggles via the server).
    #[allow(dead_code)]
    pub addr: String,
    pub raft: openraft::Raft<TypeConfig>,
    #[allow(dead_code)]
    pub partition: PartitionState,
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
                partition,
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
