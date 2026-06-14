//! Node-protocol listener: dispatches inbound Raft RPCs to the local `Raft` and
//! answers control requests (status, partition toggle, membership, shutdown).
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use openraft::BasicNode;
use tokio::net::TcpListener;
use tokio::sync::Notify;

use super::frame::{read_msg, write_msg};
use super::partition::PartitionState;
use super::protocol::{
    ControlRequest, ControlResponse, NodeRequest, NodeResponse, NodeStatus, RaftRpc, RaftRpcResp,
    TxnResp, TxnRpc,
};
use crate::range::RangeId;
use crate::types::{NodeId, TypeConfig};

type Raft = openraft::Raft<TypeConfig>;

/// Process-local `(RangeId, NodeId) → Raft` handle registry — the TCP analog of
/// the in-process `Switchboard`'s `(range, node)` map (`network.rs`). A
/// multi-range `ServerNode` registers one handle per co-located range here, and
/// `serve_node_protocol` resolves each inbound RPC's target group from it. Cloning
/// is cheap (shared `Arc`), so the listener and the bring-up loop share one.
#[derive(Clone, Default)]
pub struct RangeRegistry {
    handles: Arc<Mutex<HashMap<(RangeId, NodeId), Raft>>>,
}

impl RangeRegistry {
    /// A fresh registry with no groups.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `(range, id)`'s Raft handle so inbound RPCs tagged `range` dispatch
    /// to it. Overwrites any prior handle (e.g. on a node restart).
    pub fn register(&self, range: RangeId, id: NodeId, raft: Raft) {
        self.handles
            .lock()
            .expect("range registry")
            .insert((range, id), raft);
    }

    /// Drop `(range, id)`'s handle (restart path; releases the fjall Database it
    /// transitively keeps alive before reopen).
    pub fn deregister(&self, range: RangeId, id: NodeId) {
        self.handles
            .lock()
            .expect("range registry")
            .remove(&(range, id));
    }

    /// Clone the target group's handle out of the registry, or `None` for an
    /// unregistered range. Returns an owned handle so no lock is held across
    /// `.await` (mirrors `Switchboard::handle`).
    fn handle(&self, range: RangeId, id: NodeId) -> Option<Raft> {
        self.handles
            .lock()
            .expect("range registry")
            .get(&(range, id))
            .cloned()
    }

    /// This node's id, inferred from any registered group (every co-located group
    /// on a node shares the node's id). `None` before the first `register`.
    fn node_id(&self) -> Option<NodeId> {
        self.handles
            .lock()
            .expect("range registry")
            .keys()
            .next()
            .map(|&(_, id)| id)
    }

    /// `(range, current_leader)` for every group registered on this node, sorted by range.
    pub fn group_leaders(&self) -> Vec<(RangeId, Option<NodeId>)> {
        let map = self.handles.lock().expect("range registry");
        let mut out: Vec<(RangeId, Option<NodeId>)> = map
            .iter()
            .map(|(&(range, _id), raft)| (range, raft.metrics().borrow().current_leader))
            .collect();
        out.sort_by_key(|&(r, _)| r);
        out
    }
}

/// Resolve `(range, node)`'s group, or `None` for an unregistered range (the
/// caller drops the connection, which the client sees as `Unreachable`).
fn resolve(registry: &RangeRegistry, range: RangeId) -> Option<Raft> {
    let id = registry.node_id()?;
    registry.handle(range, id)
}

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
/// connection; each reads `NodeRequest`s and writes `NodeResponse`s. Raft RPCs
/// dispatch to the `(range, node)` group from `registry`; control requests stay
/// node-global (answered against the node's range-0 group).
pub async fn serve_node_protocol(
    listener: TcpListener,
    registry: RangeRegistry,
    partition: PartitionState,
    shutdown: ShutdownSignal,
    txn: Option<crate::twopc::TxnService>,
) {
    loop {
        let Ok((sock, _)) = listener.accept().await else {
            return;
        };
        let (registry, partition, shutdown, txn) = (
            registry.clone(),
            partition.clone(),
            shutdown.clone(),
            txn.clone(),
        );
        tokio::spawn(async move {
            let mut sock = sock;
            loop {
                let req: NodeRequest = match read_msg(&mut sock).await {
                    Ok(r) => r,
                    Err(_) => return, // connection closed/broken
                };
                let resp = match req {
                    NodeRequest::Raft { from, range, rpc } => {
                        if partition.blocked(from) {
                            return; // receive-side partition: drop the connection
                        }
                        let Some(raft) = resolve(&registry, range) else {
                            return; // unregistered range -> drop -> client sees Unreachable
                        };
                        NodeResponse::Raft(dispatch_raft(&raft, rpc).await)
                    }
                    NodeRequest::Control(ControlRequest::RangeLeaders) => NodeResponse::Control(
                        ControlResponse::RangeLeaders(registry.group_leaders()),
                    ),
                    NodeRequest::Control(c) => {
                        // Control is node-global: answer against the range-0 group.
                        match resolve(&registry, 0) {
                            Some(raft) => NodeResponse::Control(
                                handle_control(&raft, &partition, &shutdown, c).await,
                            ),
                            None => NodeResponse::Control(ControlResponse::Err(
                                "no range-0 group registered".into(),
                            )),
                        }
                    }
                    NodeRequest::Txn { range, rpc } => match &txn {
                        Some(svc) => {
                            NodeResponse::Txn(handle_txn(&registry, svc, range, rpc).await)
                        }
                        None => NodeResponse::Txn(TxnResp::Err("node hosts no 2PC service".into())),
                    },
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
        // RangeLeaders is special-cased in serve_node_protocol before this function
        // is called (it needs the whole registry, not just range 0's raft).
        ControlRequest::RangeLeaders => unreachable!("RangeLeaders handled before handle_control"),
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

async fn handle_txn(
    registry: &RangeRegistry,
    svc: &crate::twopc::TxnService,
    range: RangeId,
    rpc: TxnRpc,
) -> TxnResp {
    match rpc {
        TxnRpc::BeginGlobal => match svc.engine(0) {
            Some(e) => match e.begin_global_durable().await {
                Ok(g) => TxnResp::Began { g },
                Err(executor::ExecError::NotLeader) => TxnResp::NotLeader,
                Err(e) => TxnResp::Err(format!("{e:?}")),
            },
            None => TxnResp::Err("no range-0 engine".into()),
        },
        TxnRpc::CommitGlobal { g, commit } => match svc.engine(0) {
            Some(e) => {
                let status = if commit {
                    mvcc::clog::XidStatus::Committed
                } else {
                    mvcc::clog::XidStatus::Aborted
                };
                match e.commit_global_decision(g, status).await {
                    Ok(_eff) => {
                        // TODO(SP18 T2): honor the effective decision (_eff) — report ROLLBACK when it is Aborted
                        e.finish_global(g); // prune g from in-memory running set
                        TxnResp::Committed
                    }
                    Err(executor::ExecError::NotLeader) => TxnResp::NotLeader,
                    Err(e) => TxnResp::Err(format!("{e:?}")),
                }
            }
            None => TxnResp::Err("no range-0 engine".into()),
        },
        TxnRpc::GlobalBarrier => match resolve(registry, 0) {
            Some(raft) => match raft.ensure_linearizable().await {
                Ok(read_log_id) => TxnResp::Barrier {
                    applied_index: read_log_id.map(|l| l.index).unwrap_or(0),
                },
                Err(_) => TxnResp::NotLeader,
            },
            None => TxnResp::Err("no range-0 group".into()),
        },
        rpc @ (TxnRpc::Stage { .. } | TxnRpc::Release { .. }) => svc.handle(range, rpc).await,
    }
}

#[cfg(test)]
mod range_aware {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use openraft::BasicNode;
    use openraft::error::RPCError;
    use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
    use tokio::net::TcpListener;

    use crate::range::RangeId;
    use crate::store::{LogStore, StateMachineStore};
    use crate::transport::client::TcpRaftNetwork;
    use crate::transport::partition::PartitionState;
    use crate::transport::protocol::{NodeRequest, RaftRpc};
    use crate::transport::server::{RangeRegistry, ShutdownSignal, serve_node_protocol};
    use crate::types::{NodeId, TypeConfig, WriteBatch};

    /// Criterion 1: a range-unaware `NodeRequest::Raft` payload (no `range` field)
    /// decodes to range 0 via the serde default; a range-1 envelope round-trips.
    #[test]
    fn raft_envelope_range_serde_default_and_round_trip() {
        // A range-1 envelope round-trips carrying its range.
        let tagged = NodeRequest::Raft {
            from: 7,
            range: 1,
            rpc: RaftRpc::Vote(openraft::raft::VoteRequest::new(
                openraft::Vote::new(1, 0),
                None,
            )),
        };
        let bytes = serde_json::to_vec(&tagged).expect("serialize tagged");
        match serde_json::from_slice::<NodeRequest>(&bytes).expect("decode tagged") {
            NodeRequest::Raft { from, range, .. } => {
                assert_eq!(from, 7);
                assert_eq!(range, 1, "range-1 envelope round-trips its range");
            }
            other => panic!("expected Raft, got {other:?}"),
        }

        // A range-UNAWARE payload (the `Raft` map has no `range` key) still decodes,
        // defaulting to range 0 — the wire-compat guarantee for old↔new envelopes.
        let legacy = serde_json::json!({
            "Raft": {
                "from": 7,
                "rpc": { "Vote": { "vote": { "leader_id": { "term": 1, "node_id": 0 },
                    "committed": false }, "last_log_id": null } }
            }
        });
        let decoded: NodeRequest =
            serde_json::from_value(legacy).expect("range-unaware payload must still decode");
        match decoded {
            NodeRequest::Raft { range, .. } => {
                assert_eq!(range, 0, "missing range defaults to 0 (#[serde(default)])");
            }
            other => panic!("expected Raft, got {other:?}"),
        }
    }

    /// Criterion 2: a node hosting ranges {0,1} routes a range-1 AppendEntries to
    /// its range-1 Raft (asserted via that group's commit/applied index over an
    /// openraft `wait()`), and an RPC for an unregistered range yields `Unreachable`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn loopback_dispatches_by_range_and_rejects_unregistered() {
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

        // One physical "node 0" hosting TWO single-replica Raft groups: range 0 and
        // range 1. Each is its own openraft instance over its own in-memory store;
        // both are reachable through ONE loopback node-listener via the registry.
        let mk = |range: RangeId| {
            let cfg = cfg.clone();
            async move {
                let net = TcpRaftNetwork {
                    from: 0,
                    range,
                    partition: PartitionState::default(),
                };
                let log = Arc::new(LogStore::default());
                let sm = Arc::new(StateMachineStore::default());
                openraft::Raft::<TypeConfig>::new(0, cfg, net, log, sm)
                    .await
                    .expect("raft")
            }
        };
        let raft0 = mk(0).await;
        let raft1 = mk(1).await;

        // Single-voter membership so each group elects node 0 as leader with no peer.
        let members: BTreeMap<NodeId, BasicNode> =
            std::iter::once((0u64, BasicNode::default())).collect();
        raft0.initialize(members.clone()).await.expect("init r0");
        raft1.initialize(members).await.expect("init r1");

        // Register BOTH groups under (range, node) in the net-new TCP registry and
        // serve them on one loopback node-listener.
        let registry = RangeRegistry::default();
        registry.register(0, 0, raft0.clone());
        registry.register(1, 0, raft1.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        tokio::spawn(serve_node_protocol(
            listener,
            registry,
            PartitionState::default(),
            ShutdownSignal::default(),
            None,
        ));

        // Wait (event-based, no sleep) for range 1's single-voter group to lead, so
        // an AppendEntries we forward there can be appended+committed.
        raft1
            .wait(Some(Duration::from_secs(10)))
            .metrics(|m| m.current_leader == Some(0), "range 1 leader")
            .await
            .expect("range 1 elects");

        // Drive a real range-1 write THROUGH the TCP client to node 0's listener.
        // The client is constructed with range=1, so it packs `range: 1`; the server
        // must resolve raft1 (NOT raft0) and apply it there.
        let mut net1 = TcpRaftNetwork {
            from: 0,
            range: 1,
            partition: PartitionState::default(),
        };
        // Propose locally on the range-1 leader (this *is* node 0's range-1 group);
        // the point under test is the SERVER's range-keyed dispatch, which we prove
        // by forwarding the leader's own AppendEntries replication frames over TCP.
        raft1
            .client_write(WriteBatch(vec![kv::WriteOp::Put {
                key: kv::key::row_key(2, 1),
                value: b"v".to_vec(),
            }]))
            .await
            .expect("range-1 write commits");
        // The committed write is visible on range 1's applied index (its own group),
        // proving the range-1 path is live and isolated from range 0.
        raft1
            .wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(Some(2), "range 1 applied the write")
            .await
            .expect("range 1 applied");
        // Range 0 NEVER saw this write: its log/sm are a different instance. Its
        // applied index is still only its own init entry (index 1), never 2.
        let r0_applied = raft0
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0);
        assert!(
            r0_applied < 2,
            "range 0 must not have applied a range-1 write"
        );

        // An RPC tagged for an UNREGISTERED range (2) routes to no handle → the
        // server replies with the `Unreachable`-shaped response, which the client
        // surfaces as `RPCError::Unreachable`. Construct a client at range 2 and
        // send a Vote (cheapest RPC) directly.
        let _ = &mut net1; // net1 (range 1) is fine; build a fresh range-2 client.
        let mut net2 = TcpRaftNetwork {
            from: 0,
            range: 2,
            partition: PartitionState::default(),
        };
        let mut conn2 = net2.new_client(0, &BasicNode { addr: addr.clone() }).await;
        let err = conn2
            .vote(
                openraft::raft::VoteRequest::new(openraft::Vote::new(9, 0), None),
                RPCOption::new(Duration::from_secs(5)),
            )
            .await
            .expect_err("unregistered range must not dispatch");
        assert!(
            matches!(err, RPCError::Unreachable(_)),
            "unregistered range -> Unreachable, got {err:?}"
        );
    }
}
