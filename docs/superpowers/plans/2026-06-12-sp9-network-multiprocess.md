# SP9 (D2b): Real network transport + multi-process nodes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn each in-process Raft replica into a real OS-process node that speaks Raft over TCP, serves pgwire SQL, supports runtime membership, and recovers across process boundaries — verified by a process-level crash + partition test harness.

**Architecture:** A hand-rolled, length-prefixed, postcard-encoded TCP protocol implements openraft's `RaftNetwork`/`RaftNetworkFactory` (parallel to the retained in-process `Switchboard`). One framed protocol per node carries both Raft RPCs and a control channel (status / partition toggle / membership / shutdown); pgwire SQL is a separate port. A `ServerNode` ties durable storage (SP8) + openraft-over-TCP + the listeners + one shared replicated `SqlEngine` + a reseed-on-leadership task together; the `crabgresql` binary gains a `node` subcommand. A harness in the binary's package spawns/kills/respawns real processes and drives SQL via tokio-postgres.

**Tech Stack:** Rust 2024, openraft 0.9 (serde feature), fjall (durable storage from SP8), tokio (TCP + process), postcard (wire encoding), tokio-postgres + tempfile (test harness). Pure-Rust, `#![forbid(unsafe_code)]`.

**Spec:** `docs/superpowers/specs/2026-06-12-crabgresql-sp9-network-multiprocess-design.md`

**Conventions for every task:** Windows dev box — prefix cargo with `__COMPAT_LAYER=RunAsInvoker` if a test binary fails to launch with `os error 740` (environmental, not a regression). **IDE/rust-analyzer diagnostics in this repo are routinely STALE — trust only `cargo build`/`clippy`/`test`.** End each commit message with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. Branch: `sp9-network-multiprocess` (already checked out; do NOT switch). After each task: `cargo clippy -p <crate> --all-targets -- -D warnings` must be zero-warning.

---

## File Structure

| File | Responsibility |
|---|---|
| `crates/cluster/src/transport/mod.rs` | Module root; re-exports; `MAX_FRAME` const. |
| `crates/cluster/src/transport/frame.rs` | `write_msg`/`read_msg` — u32-BE length-prefixed postcard frames over any `AsyncRead`/`AsyncWrite`. |
| `crates/cluster/src/transport/protocol.rs` | Wire enums: `NodeRequest`/`NodeResponse`/`RaftRpc`/`RaftRpcResp`/`ControlRequest`/`ControlResponse`/`NodeStatus`. |
| `crates/cluster/src/transport/partition.rs` | `PartitionState` — shared blocked-peer set (app-layer partitions). |
| `crates/cluster/src/transport/client.rs` | `TcpRaftNetwork` (`RaftNetworkFactory`) + `TcpConn` (`RaftNetwork`) over TCP. |
| `crates/cluster/src/transport/server.rs` | `serve_node_protocol` accept loop; Raft dispatch + control handlers. |
| `crates/cluster/src/transport/testcluster.rs` | `#[cfg(test)]` multi-task loopback-TCP cluster used to test the transport in-process. |
| `crates/cluster/src/server_node.rs` | `ServerNode::start` — durable node + openraft-over-TCP + listeners + shared `SqlEngine` + reseed task + self-bootstrap. |
| `crates/crabgresql/src/main.rs` | `node` subcommand wiring CLI → `ServerNode`. |
| `crates/crabgresql/tests/harness/mod.rs` | Process spawn/kill/respawn, control client, pg client, port/dir mgmt, `wait_for_leader`. |
| `crates/crabgresql/tests/multiprocess.rs` | Deterministic scenarios + the crash+partition bank nemesis. |

The in-process `Switchboard`/`Node`/`Cluster`/`network.rs` are **unchanged**.

---

### Task 1: Transport scaffold — deps, framing, protocol enums, partition state

**Files:**
- Modify: `Cargo.toml` (workspace deps), `crates/cluster/Cargo.toml`
- Create: `crates/cluster/src/transport/mod.rs`, `frame.rs`, `protocol.rs`, `partition.rs`
- Modify: `crates/cluster/src/lib.rs`

- [ ] **Step 1: Add the `postcard` dep.** In root `Cargo.toml` under `[workspace.dependencies]` add:
```toml
postcard = { version = "1", features = ["use-std"] }
```
In `crates/cluster/Cargo.toml` under `[dependencies]` add `postcard = { workspace = true }` (and confirm `tokio = { workspace = true }`, `serde = { workspace = true }`, `openraft = { workspace = true }` are present).

- [ ] **Step 2: Declare the module.** In `crates/cluster/src/lib.rs` add `pub mod transport;` (next to `mod durable;`).

- [ ] **Step 3: Write `transport/mod.rs`.**
```rust
//! Real TCP transport for Raft RPCs + a control channel, implementing openraft's
//! `RaftNetwork`/`RaftNetworkFactory` (parallel to the in-process `Switchboard`).
pub mod client;
pub mod frame;
pub mod partition;
pub mod protocol;
pub mod server;

#[cfg(test)]
mod testcluster;

/// Hard cap on a single frame to avoid allocating on garbage/oversized input.
pub const MAX_FRAME: usize = 64 * 1024 * 1024;
```
(`server`/`client` land in Tasks 2–3; add the `pub mod` lines now and create empty stub files `server.rs`/`client.rs` containing only `//! stub` so the crate compiles — they are filled in later tasks. Alternatively add the `pub mod` lines in the task that creates each file. Pick whichever keeps `cargo build` green at each commit; the stub approach is simplest.)

- [ ] **Step 4: Write the failing frame round-trip test in `transport/frame.rs`.**
```rust
//! Length-prefixed (u32 BE) postcard frames over any async byte stream.
use std::io;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::MAX_FRAME;

/// Serialize `msg` with postcard and write it as a u32-BE length prefix + body.
pub async fn write_msg<W, T>(w: &mut W, msg: &T) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let bytes =
        postcard::to_stdvec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    w.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    w.write_all(&bytes).await?;
    w.flush().await?;
    Ok(())
}

/// Read one length-prefixed postcard frame and deserialize it as `T`.
pub async fn read_msg<R, T>(r: &mut R) -> io::Result<T>
where
    R: AsyncReadExt + Unpin,
    T: DeserializeOwned,
{
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    if n > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame too large"));
    }
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    postcard::from_bytes(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frame_round_trips_over_a_duplex() {
        let (mut a, mut b) = tokio::io::duplex(1024);
        let sent = vec![("hello".to_string(), 42u64), ("x".into(), 7)];
        let writer = sent.clone();
        let h = tokio::spawn(async move { write_msg(&mut a, &writer).await.unwrap() });
        let got: Vec<(String, u64)> = read_msg(&mut b).await.unwrap();
        h.await.unwrap();
        assert_eq!(got, sent);
    }
}
```

- [ ] **Step 5: Run it.** `cargo test -p cluster --lib transport::frame` → PASS.

- [ ] **Step 6: Write `transport/protocol.rs`** (the wire enums). Verify the openraft type names compile (they derive serde under our `TypeConfig`; the `serde` feature is enabled):
```rust
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeResponse {
    Raft(RaftRpcResp),
    Control(ControlResponse),
}
```
Note: the exact openraft import paths (`openraft::raft::…`, `openraft::error::…`) and whether `AppendEntriesResponse`/`VoteResponse` are parameterized by `NodeId` vs `TypeConfig` may differ slightly in 0.9.24 — let the compiler guide the exact paths/params; the in-process `network.rs` already names these types (`AppendEntriesRequest<TypeConfig>`, `AppendEntriesResponse<NodeId>`, `VoteRequest<NodeId>`, `VoteResponse<NodeId>`, `InstallSnapshotRequest<TypeConfig>`, `InstallSnapshotResponse<NodeId>`, `RaftError<NodeId>`, `RaftError<NodeId, InstallSnapshotError>`), so copy from there.

- [ ] **Step 7: Write `transport/partition.rs` + its test.**
```rust
//! App-layer network partitions: a shared set of peer ids whose RPCs are dropped.
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::types::NodeId;

/// A bidirectional partition fault, shared by a node's transport client + server.
#[derive(Clone, Default)]
pub struct PartitionState {
    blocked: Arc<Mutex<HashSet<NodeId>>>,
}

impl PartitionState {
    /// True if RPCs to/from `peer` should be dropped.
    pub fn blocked(&self, peer: NodeId) -> bool {
        self.blocked.lock().expect("partition lock").contains(&peer)
    }
    /// Replace the blocked set (a `SetPartition` control request).
    pub fn set(&self, peers: Vec<NodeId>) {
        *self.blocked.lock().expect("partition lock") = peers.into_iter().collect();
    }
    /// Clear all partitions (`Heal`).
    pub fn heal(&self) {
        self.blocked.lock().expect("partition lock").clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_heal_toggle_blocked() {
        let p = PartitionState::default();
        assert!(!p.blocked(2));
        p.set(vec![1, 2]);
        assert!(p.blocked(1) && p.blocked(2) && !p.blocked(0));
        p.heal();
        assert!(!p.blocked(1));
    }
}
```
(`PartitionState::blocked` uses a `std::sync::Mutex` but the critical section contains **no `.await`** — it is a plain set lookup, so it is safe to hold across the synchronous call sites. Do not hold the guard across `.await`.)

- [ ] **Step 8: Verify + commit.**
```
cargo test -p cluster --lib transport
cargo clippy -p cluster --all-targets -- -D warnings
git add Cargo.toml crates/cluster/Cargo.toml crates/cluster/src/lib.rs crates/cluster/src/transport/
git commit -m "feat(cluster): TCP transport scaffold — framing, protocol enums, partition state"
```
Expected: frame + partition tests pass; clippy clean.

---

### Task 2: TCP server (Raft dispatch + control handlers)

**Files:**
- Create/replace: `crates/cluster/src/transport/server.rs`

A function that, given a bound `TcpListener` and a live `Raft<TypeConfig>` + `PartitionState`, serves the node protocol: dispatches Raft RPCs to the local raft and answers control requests. (Tested end-to-end in Task 3 via the loopback test cluster; this task is the server half.)

- [ ] **Step 1: Write `server.rs`.**
```rust
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
            let members: Vec<NodeId> =
                m.membership_config.membership().voter_ids().collect();
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
```
Verify openraft API names against 0.9.24: `metrics().borrow()` fields (`id`, `state`, `current_leader`, `last_log_index`, `last_applied`, `membership_config`), `membership().voter_ids()` (returns an iterator of `NodeId`), `add_learner(id, node, blocking)`, `change_membership(members, retain)`. The in-process code already reads `m.state`, `m.current_leader`, `m.last_log_index` (see `cluster.rs`/`sql_over_raft.rs`), so those are correct; adjust `voter_ids()`/membership accessors as the compiler requires.

- [ ] **Step 2: Add `pub mod server;` to `transport/mod.rs`** if not already (replace the stub). Build:
```
cargo build -p cluster
cargo clippy -p cluster --all-targets -- -D warnings
```
Expected: compiles clean (no test yet — exercised in Task 3).

- [ ] **Step 3: Commit.**
```
git add crates/cluster/src/transport/server.rs crates/cluster/src/transport/mod.rs
git commit -m "feat(cluster): node-protocol server — Raft dispatch + control handlers"
```

---

### Task 3: TCP client + loopback test cluster (election + replication over TCP)

**Files:**
- Create/replace: `crates/cluster/src/transport/client.rs`
- Create: `crates/cluster/src/transport/testcluster.rs`

- [ ] **Step 1: Write `client.rs`.** The `RaftNetworkFactory`/`RaftNetwork` over TCP with reconnect-on-drop + per-RPC timeout + partition check.
```rust
//! TCP implementation of openraft's network traits: dial a peer's node-addr,
//! send a framed Raft RPC, await the framed response. Reconnects on drop so a
//! peer restart heals on the next call; checks the local partition first.
use std::time::Duration;

use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RaftNetwork, RaftNetworkFactory, RPCOption};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use tokio::net::TcpStream;

use super::frame::{read_msg, write_msg};
use super::partition::PartitionState;
use super::protocol::{NodeRequest, NodeResponse, RaftRpc, RaftRpcResp};
use crate::types::{NodeId, TypeConfig};

/// One factory per node; mints a `TcpConn` per peer.
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
            let req = NodeRequest::Raft { from: self.from, rpc: rpc.clone() };
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
        match self.call(RaftRpc::AppendEntries(rpc), option.hard_ttl()).await {
            Ok(RaftRpcResp::AppendEntries(Ok(r))) => Ok(r),
            Ok(RaftRpcResp::AppendEntries(Err(e))) => {
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

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        match self.call(RaftRpc::InstallSnapshot(rpc), option.hard_ttl()).await {
            Ok(RaftRpcResp::InstallSnapshot(Ok(r))) => Ok(r),
            Ok(RaftRpcResp::InstallSnapshot(Err(e))) => {
                Err(RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            _ => Err(self.unreachable()),
        }
    }
}
```
Verify against 0.9.24: the `RaftNetwork` method signatures (the in-process `network.rs` `Conn` is the exact template — copy its method signatures and error types verbatim, only changing the body to use `self.call`). `RPCOption` timeout accessor may be `hard_ttl()` or `soft_ttl()`; use whichever the in-process code/openraft exposes (the in-process code ignores it — here use `option.hard_ttl()`, falling back to a default `Duration::from_millis(1000)` if the accessor differs).

- [ ] **Step 2: Add `pub mod client;` to `transport/mod.rs`** (replace stub). 

- [ ] **Step 3: Write `transport/testcluster.rs`** — an in-process, multi-task, loopback-TCP cluster used to test the transport without spawning processes. It mirrors `Cluster` but wires `TcpRaftNetwork` + `serve_node_protocol` instead of the `Switchboard`. Uses the **in-memory** `LogStore`/`StateMachineStore` (transport test, not a durability test) so it's fast.
```rust
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
use super::server::{serve_node_protocol, ShutdownSignal};
use crate::store::{LogStore, StateMachineStore};
use crate::types::{NodeId, TypeConfig, WriteBatch};

pub struct TcpNode {
    pub id: NodeId,
    pub addr: String,
    pub raft: openraft::Raft<TypeConfig>,
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
            let net = TcpRaftNetwork { from: id, partition: partition.clone() };
            let log = Arc::new(LogStore::default());
            let sm = Arc::new(StateMachineStore::default());
            let raft = openraft::Raft::new(id, cfg.clone(), net, log, sm).await.expect("raft");
            let listener = listeners.remove(0);
            tokio::spawn(serve_node_protocol(
                listener,
                raft.clone(),
                partition.clone(),
                ShutdownSignal::default(),
            ));
            nodes.push(TcpNode { id, addr: addrs[id as usize].clone(), raft, partition });
        }
        let members: BTreeMap<NodeId, BasicNode> = (0..n)
            .map(|id| (id, BasicNode { addr: addrs[id as usize].clone() }))
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
```
(`LogStore`/`StateMachineStore` are `pub(crate)` in `store.rs`; confirm they're reachable from `transport::testcluster` — both are in the same crate. `kv::key::row_key` is the same helper used elsewhere.)

- [ ] **Step 4: Run it (3×, non-flaky).**
```
cargo test -p cluster --lib transport::testcluster 2>&1 | grep "test result"
```
Expected: `elects_leader_and_replicates_over_tcp` passes on all 3 runs.

- [ ] **Step 5: Verify + commit.**
```
cargo clippy -p cluster --all-targets -- -D warnings
git add crates/cluster/src/transport/client.rs crates/cluster/src/transport/testcluster.rs crates/cluster/src/transport/mod.rs
git commit -m "feat(cluster): TCP RaftNetwork client + loopback test cluster (election + replication over TCP)"
```

---

### Task 4: Partition + control over TCP (in the loopback cluster)

**Files:**
- Modify: `crates/cluster/src/transport/testcluster.rs` (add tests + a control client helper)

- [ ] **Step 1: Add a control client helper + tests to `testcluster.rs`.**
```rust
use super::frame::{read_msg, write_msg};
use super::protocol::{ControlRequest, ControlResponse, NodeRequest, NodeResponse, NodeStatus};

impl TcpCluster {
    /// Send one control request to node `id` over its node-addr.
    pub async fn control(&self, id: NodeId, req: ControlRequest) -> ControlResponse {
        let mut s = tokio::net::TcpStream::connect(&self.nodes[id as usize].addr)
            .await
            .expect("connect");
        write_msg(&mut s, &NodeRequest::Control(req)).await.expect("write");
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
    let minority = (0..3u64).find(|&i| i != leader).expect("follower");
    // Isolate the minority from both other nodes (bidirectional cut).
    let others: Vec<u64> = (0..3u64).filter(|&i| i != minority).collect();
    c.control(minority, ControlRequest::SetPartition(others.clone())).await;
    for &o in &others {
        c.control(o, ControlRequest::SetPartition(vec![minority])).await;
    }
    // The majority still commits.
    let l = c.leader().expect("leader");
    l.raft
        .client_write(WriteBatch(vec![kv::WriteOp::Put {
            key: kv::key::row_key(2, 2),
            value: b"w".to_vec(),
        }]))
        .await
        .expect("majority commits under partition");
    // Heal: the minority catches up.
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
```

- [ ] **Step 2: Run (3×).** `cargo test -p cluster --lib transport::testcluster` → all pass, non-flaky.

- [ ] **Step 3: Verify + commit.**
```
cargo clippy -p cluster --all-targets -- -D warnings
git add crates/cluster/src/transport/testcluster.rs
git commit -m "test(cluster): control status + minority partition/heal over TCP"
```

---

### Task 5: `ServerNode` — durable node + listeners + shared SQL engine + reseed

**Files:**
- Create: `crates/cluster/src/server_node.rs`
- Modify: `crates/cluster/src/lib.rs` (`pub mod server_node;`)
- Modify: `crates/cluster/Cargo.toml` (ensure `executor`, `pgwire` are deps — needed to build + serve the SQL engine; add `{ workspace = true }` if missing)

This is the runnable node: durable storage (SP8) + openraft-over-TCP + the two listeners + one shared replicated `SqlEngine` + the reseed-on-leadership task + self-bootstrap.

- [ ] **Step 1: Write `server_node.rs`.**
```rust
//! A runnable replicated node: durable Raft over TCP + a pgwire SQL server over a
//! shared replicated engine + reseed-on-leadership + optional self-bootstrap.
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use executor::SqlEngine;
use openraft::{BasicNode, ServerState};
use tokio::net::TcpListener;

use crate::committer::RaftCommitter;
use crate::durable::{DurableLogStore, DurableStateMachineStore, NodeStore};
use crate::transport::client::TcpRaftNetwork;
use crate::transport::partition::PartitionState;
use crate::transport::server::{serve_node_protocol, ShutdownSignal};
use crate::types::{NodeId, TypeConfig};

/// Startup configuration for one node.
pub struct NodeConfig {
    pub id: NodeId,
    pub node_addr: String,
    pub sql_addr: String,
    pub data_dir: PathBuf,
    /// (id, node-addr) for every member, including self. Used for bootstrap.
    pub peers: Vec<(NodeId, String)>,
    pub bootstrap: bool,
}

/// A live node; `shutdown.wait()` resolves when a `Shutdown` control request fires.
pub struct ServerNode {
    pub raft: openraft::Raft<TypeConfig>,
    pub engine: Arc<SqlEngine>,
    pub shutdown: ShutdownSignal,
}

fn raft_config() -> Arc<openraft::Config> {
    Arc::new(
        openraft::Config {
            heartbeat_interval: 250,
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    )
}

impl ServerNode {
    pub async fn start(cfg: NodeConfig) -> std::io::Result<Self> {
        let store = NodeStore::open(&cfg.data_dir).expect("open node store");
        let log = DurableLogStore::open(&store).expect("durable log");
        let sm = DurableStateMachineStore::open(&store).expect("durable sm");
        let sm_kv = sm.sm_kv();

        let partition = PartitionState::default();
        let net = TcpRaftNetwork { from: cfg.id, partition: partition.clone() };
        let raft = openraft::Raft::new(cfg.id, raft_config(), net, log, sm)
            .await
            .expect("raft::new");

        // Node-protocol listener (Raft RPCs + control).
        let node_listener = TcpListener::bind(&cfg.node_addr).await?;
        let shutdown = ShutdownSignal::default();
        tokio::spawn(serve_node_protocol(
            node_listener,
            raft.clone(),
            partition.clone(),
            shutdown.clone(),
        ));

        // One shared replicated engine; reseed its counters on the leadership edge.
        let engine = Arc::new(
            SqlEngine::replicated(sm_kv, Arc::new(RaftCommitter { raft: raft.clone() }))
                .expect("replicated engine"),
        );
        tokio::spawn(reseed_on_leadership(raft.clone(), engine.clone()));

        // pgwire SQL listener.
        let sql_listener = TcpListener::bind(&cfg.sql_addr).await?;
        tokio::spawn(serve_sql(sql_listener, engine.clone()));

        if cfg.bootstrap {
            tokio::spawn(bootstrap(raft.clone(), cfg.peers.clone()));
        }

        Ok(Self { raft, engine, shutdown })
    }
}

/// Reseed xid/seq counters on each follower→leader transition so they never
/// regress below a prior leader's high-water mark. Idempotent (only bumps up).
async fn reseed_on_leadership(raft: openraft::Raft<TypeConfig>, engine: Arc<SqlEngine>) {
    let mut rx = raft.metrics();
    let mut was_leader = false;
    loop {
        let is_leader = {
            let m = rx.borrow();
            m.state == ServerState::Leader && m.current_leader == Some(m.id)
        };
        if is_leader && !was_leader {
            let _ = engine.reseed_counters();
        }
        was_leader = is_leader;
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// Bootstrapper: wait until every peer's node-addr accepts a connection, then
/// initialize the voting group. On a restart the group is already initialized, so
/// the `initialize` error is ignored.
async fn bootstrap(raft: openraft::Raft<TypeConfig>, peers: Vec<(NodeId, String)>) {
    for (_, addr) in &peers {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while tokio::net::TcpStream::connect(addr).await.is_err() {
            if tokio::time::Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let members: BTreeMap<NodeId, BasicNode> = peers
        .into_iter()
        .map(|(id, addr)| (id, BasicNode { addr }))
        .collect();
    let _ = raft.initialize(members).await; // ignore AlreadyInitialized on restart
}

/// Serve pgwire SQL: one session per connection over the shared engine.
async fn serve_sql(listener: TcpListener, engine: Arc<SqlEngine>) {
    loop {
        let Ok((sock, _)) = listener.accept().await else {
            return;
        };
        let engine = engine.clone();
        tokio::spawn(async move {
            // Reuse the pgwire server's connection handler with `engine.connect()`
            // as the session. Mirror `crates/crabgresql/src/main.rs`'s accept loop
            // (the trust-auth, no-TLS path): build a `SessionConfig`, run the
            // pgwire protocol over `sock` against `engine.connect()`.
            let _ = pgwire_serve_one(sock, engine).await;
        });
    }
}
```
**Implementation note for `serve_sql`/`pgwire_serve_one`:** the existing binary `crates/crabgresql/src/main.rs` already contains the pgwire accept+serve loop for a single `SqlEngine` (trust auth, optional TLS). Factor that per-connection serve logic into a reusable function (either here or call into a small helper in `pgwire`/the binary) that takes a `TcpStream` + an `Arc<SqlEngine>` and runs one session via `engine.connect()`. Do **not** reimplement the Postgres protocol — reuse the existing handler. The bank/SQL tests in Task 8 verify this end to end; keep this task's verification at "compiles + an in-process ServerNode serves a SELECT" (Step 2).

- [ ] **Step 2: Write a failing in-process test** in `server_node.rs` `#[cfg(test)]`: start a single-node `ServerNode` (bootstrap=true, peers=[self]) on loopback ports + a `tempfile::TempDir`, wait until it is leader (poll `raft.metrics()`), connect with `tokio-postgres` to `sql_addr`, run `CREATE TABLE t(id int4); INSERT; SELECT` and assert the row. (Add `tokio-postgres`, `tempfile` to `crates/cluster/[dev-dependencies]`.)
```rust
#[cfg(test)]
mod tests {
    use super::*;

    async fn free_port() -> String {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap().to_string();
        drop(l);
        a
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_node_serves_sql_after_election() {
        let dir = tempfile::tempdir().unwrap();
        let node_addr = free_port().await;
        let sql_addr = free_port().await;
        let node = ServerNode::start(NodeConfig {
            id: 0,
            node_addr: node_addr.clone(),
            sql_addr: sql_addr.clone(),
            data_dir: dir.path().to_path_buf(),
            peers: vec![(0, node_addr.clone())],
            bootstrap: true,
        })
        .await
        .unwrap();
        node.raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(|m| m.current_leader == Some(0), "self leader")
            .await
            .unwrap();
        // tokio-postgres connect + simple SQL.
        let conn_str = format!("host=127.0.0.1 port={} user=postgres", sql_addr.rsplit(':').next().unwrap());
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .expect("pg connect");
        tokio::spawn(connection);
        client.simple_query("CREATE TABLE t (id int4)").await.unwrap();
        client.simple_query("INSERT INTO t VALUES (1)").await.unwrap();
        let rows = client.simple_query("SELECT id FROM t").await.unwrap();
        let n = rows
            .iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count();
        assert_eq!(n, 1);
    }
}
```
Adjust the `conn_str`/auth to match the binary's default (trust auth, user from `SessionConfig`). If the pgwire server requires a specific startup user, use it.

- [ ] **Step 3: Implement until it passes.** `__COMPAT_LAYER=RunAsInvoker cargo test -p cluster --lib server_node::tests::single_node_serves_sql_after_election` → PASS. Run 3× (it does real fsync + election).

- [ ] **Step 4: Verify + commit.**
```
cargo clippy -p cluster --all-targets -- -D warnings
git add crates/cluster/src/server_node.rs crates/cluster/src/lib.rs crates/cluster/Cargo.toml
git commit -m "feat(cluster): ServerNode — durable Raft over TCP + pgwire SQL + reseed-on-leadership"
```

---

### Task 6: Binary `node` subcommand

**Files:**
- Modify: `crates/crabgresql/src/main.rs`, `crates/crabgresql/Cargo.toml`

- [ ] **Step 1: Add deps.** In `crates/crabgresql/Cargo.toml` `[dependencies]` add `cluster = { workspace = true }`.

- [ ] **Step 2: Refactor `Args` into a subcommand** (keep the existing single-server mode as the default subcommand so current behavior + tests are unchanged). Add:
```rust
#[derive(clap::Subcommand, Debug)]
enum Command {
    /// (default) Serve a single SQL engine (SP1 behavior).
    Serve(ServeArgs),     // the existing fields move here
    /// Run a replicated durable Raft node.
    Node(NodeArgs),
}

#[derive(clap::Args, Debug)]
struct NodeArgs {
    #[arg(long)] id: u64,
    #[arg(long)] node_addr: String,
    #[arg(long)] sql_addr: String,
    #[arg(long)] data_dir: std::path::PathBuf,
    /// Repeatable: id@host:port for every member (including self).
    #[arg(long = "peer", value_name = "ID@ADDR")] peers: Vec<String>,
    #[arg(long)] bootstrap: bool,
}
```
Parse `peers` (`id@addr` → `(u64, String)`). In `main`, match the subcommand: `Node(a)` → build `cluster::server_node::NodeConfig` and `ServerNode::start(cfg).await`, then `node.shutdown.wait().await` (block until a `Shutdown` control request or until killed). Keep `Serve`/default as the existing path. Make the default subcommand (no args) still work for back-compat if the existing tests invoke the binary plainly — use `#[command(subcommand)]` with a default, or keep top-level flags and add `node` as an optional subcommand. Choose the clap structure that leaves the existing `Serve` behavior and its tests untouched.

- [ ] **Step 3: Build + smoke-check.**
```
cargo build -p crabgresql
cargo clippy -p crabgresql --all-targets -- -D warnings
```
Expected: builds clean. (Functional verification is Task 7's harness.)

- [ ] **Step 4: Commit.**
```
git add crates/crabgresql/src/main.rs crates/crabgresql/Cargo.toml
git commit -m "feat(crabgresql): node subcommand — run a replicated durable Raft node"
```

---

### Task 7: Process harness + bring-up scenario

**Files:**
- Create: `crates/crabgresql/tests/harness/mod.rs`
- Create: `crates/crabgresql/tests/multiprocess.rs`
- Modify: `crates/crabgresql/Cargo.toml` (`[dev-dependencies]`: `cluster`, `tokio-postgres`, `tempfile`, `tokio`)

- [ ] **Step 1: Write the harness** (`tests/harness/mod.rs`). It spawns the just-built binary (`env!("CARGO_BIN_EXE_crabgresql")`), manages ports/dirs, and drives control + SQL.
```rust
//! Multi-process test harness: spawns `crabgresql node` children, drives the
//! control protocol + SQL, injects crashes/partitions.
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use cluster::transport::frame::{read_msg, write_msg};
use cluster::transport::protocol::{
    ControlRequest, ControlResponse, NodeRequest, NodeResponse, NodeStatus,
};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

pub struct ProcNode {
    pub id: u64,
    pub node_addr: String,
    pub sql_addr: String,
    pub dir: PathBuf,
    pub child: Child,
}

pub struct Cluster {
    pub nodes: Vec<ProcNode>,
    _tmp: TempDir, // base dir for all node data dirs; kept alive for the test
    peers_arg: Vec<String>,
}

async fn free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

impl Cluster {
    /// Spawn `n` node processes (node 0 bootstraps) and wait for a leader.
    pub async fn spawn(n: u64) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let mut info = Vec::new();
        for id in 0..n {
            let node_addr = format!("127.0.0.1:{}", free_port().await);
            let sql_addr = format!("127.0.0.1:{}", free_port().await);
            info.push((id, node_addr, sql_addr));
        }
        let peers_arg: Vec<String> =
            info.iter().map(|(id, na, _)| format!("{id}@{na}")).collect();
        let mut nodes = Vec::new();
        for (id, node_addr, sql_addr) in &info {
            let dir = tmp.path().join(format!("node-{id}"));
            std::fs::create_dir_all(&dir).unwrap();
            let child = spawn_node(*id, node_addr, sql_addr, &dir, &peers_arg, *id == 0);
            nodes.push(ProcNode {
                id: *id,
                node_addr: node_addr.clone(),
                sql_addr: sql_addr.clone(),
                dir,
                child,
            });
        }
        let c = Self { nodes, _tmp: tmp, peers_arg };
        c.wait_for_leader().await;
        c
    }

    pub async fn control(&self, id: u64, req: ControlRequest) -> Option<ControlResponse> {
        let addr = &self.nodes[id as usize].node_addr;
        let mut s = TcpStream::connect(addr).await.ok()?;
        write_msg(&mut s, &NodeRequest::Control(req)).await.ok()?;
        match read_msg::<_, NodeResponse>(&mut s).await.ok()? {
            NodeResponse::Control(r) => Some(r),
            _ => None,
        }
    }

    pub async fn status(&self, id: u64) -> Option<NodeStatus> {
        match self.control(id, ControlRequest::GetStatus).await? {
            ControlResponse::Status(s) => Some(s),
            _ => None,
        }
    }

    /// Wait (bounded) until some node reports a leader; return its id.
    pub async fn wait_for_leader(&self) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            for n in &self.nodes {
                if let Some(st) = self.status(n.id).await {
                    if let Some(l) = st.current_leader {
                        return l;
                    }
                }
            }
            assert!(tokio::time::Instant::now() < deadline, "no leader within 30s");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Wait (bounded) until node `id` has applied at least `idx`.
    pub async fn wait_applied(&self, id: u64, idx: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(st) = self.status(id).await {
                if st.last_applied.unwrap_or(0) >= idx {
                    return;
                }
            }
            assert!(tokio::time::Instant::now() < deadline, "node {id} did not apply {idx}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub fn sql_addr(&self, id: u64) -> &str {
        &self.nodes[id as usize].sql_addr
    }

    /// Open a tokio-postgres client to node `id`'s SQL port.
    pub async fn pg(&self, id: u64) -> tokio_postgres::Client {
        let addr = self.sql_addr(id);
        let port = addr.rsplit(':').next().unwrap();
        let cs = format!("host=127.0.0.1 port={port} user=postgres");
        let (client, conn) = tokio_postgres::connect(&cs, tokio_postgres::NoTls)
            .await
            .expect("pg connect");
        tokio::spawn(conn);
        client
    }

    /// Hard-kill node `id` (SIGKILL / TerminateProcess).
    pub async fn kill(&mut self, id: u64) {
        let _ = self.nodes[id as usize].child.start_kill();
        let _ = self.nodes[id as usize].child.wait().await;
    }

    /// Respawn node `id` from its existing data dir (bootstrap=false; it recovers).
    pub fn respawn(&mut self, id: u64) {
        let n = &mut self.nodes[id as usize];
        n.child = spawn_node(id, &n.node_addr, &n.sql_addr, &n.dir, &self.peers_arg, false);
    }
}

fn spawn_node(
    id: u64,
    node_addr: &str,
    sql_addr: &str,
    dir: &std::path::Path,
    peers: &[String],
    bootstrap: bool,
) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_crabgresql"));
    cmd.arg("node")
        .arg("--id").arg(id.to_string())
        .arg("--node-addr").arg(node_addr)
        .arg("--sql-addr").arg(sql_addr)
        .arg("--data-dir").arg(dir);
    for p in peers {
        cmd.arg("--peer").arg(p);
    }
    if bootstrap {
        cmd.arg("--bootstrap");
    }
    cmd.stdout(Stdio::null()).stderr(Stdio::piped()).kill_on_drop(true);
    cmd.spawn().expect("spawn node")
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for n in &mut self.nodes {
            let _ = n.child.start_kill();
        }
    }
}
```
**Note:** `cluster::transport::{frame,protocol}` must be public (they are — `pub mod` in Task 1). `kill_on_drop(true)` ensures children die if a test panics. Free-port-then-bind has a benign TOCTOU race; acceptable for tests.

- [ ] **Step 2: Write the bring-up scenario** (`tests/multiprocess.rs`).
```rust
mod harness;
use harness::Cluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bringup_elects_leader_and_serves_sql() {
    let c = Cluster::spawn(3).await;
    let leader = c.wait_for_leader().await;
    let client = c.pg(leader).await;
    client.simple_query("CREATE TABLE t (id int4)").await.unwrap();
    client.simple_query("INSERT INTO t VALUES (1)").await.unwrap();
    let rows = client.simple_query("SELECT id FROM t").await.unwrap();
    let n = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(n, 1, "leader serves SQL over the real cluster");
}
```

- [ ] **Step 3: Run (3×).**
```
__COMPAT_LAYER=RunAsInvoker cargo test -p crabgresql --test multiprocess bringup 2>&1 | grep "test result"
```
Expected: PASS. If a child fails to launch with `os error 740` on the Windows box, note it (environmental) and rely on Linux CI; otherwise it must pass. If a child crashes, read its captured stderr (add a debug print of `child.wait_with_output`) to diagnose.

- [ ] **Step 4: Verify + commit.**
```
cargo clippy -p crabgresql --all-targets -- -D warnings
git add crates/crabgresql/tests/ crates/crabgresql/Cargo.toml
git commit -m "test(crabgresql): multi-process harness + bring-up (election + SQL over real cluster)"
```

---

### Task 8: Crash recovery, runtime membership, partition + bank nemesis scenarios

**Files:**
- Modify: `crates/crabgresql/tests/multiprocess.rs`

Add the remaining scenarios. Each is a bounded `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`; all waits go through `wait_for_leader`/`wait_applied`/status polling — no fixed sleeps for correctness.

- [ ] **Step 1: Committed write survives kill-9 + respawn.**
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_write_survives_kill_and_respawn() {
    let mut c = Cluster::spawn(3).await;
    let leader = c.wait_for_leader().await;
    let client = c.pg(leader).await;
    client.simple_query("CREATE TABLE t (id int4)").await.unwrap();
    client.simple_query("INSERT INTO t VALUES (7)").await.unwrap();
    let follower = (0..3u64).find(|&i| i != leader).unwrap();
    c.wait_applied(follower, 2).await; // membership, noop, write
    c.kill(follower).await;
    c.respawn(follower);
    c.wait_applied(follower, 2).await; // recovered from disk
    // Read via the (still-)leader to confirm the cluster is healthy.
    let rows = c.pg(c.wait_for_leader().await).await
        .simple_query("SELECT id FROM t").await.unwrap();
    assert_eq!(rows.iter().filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))).count(), 1);
}
```

- [ ] **Step 2: Leader-kill failover + old leader rejoins.**
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_kill_failover_and_rejoin() {
    let mut c = Cluster::spawn(3).await;
    let old = c.wait_for_leader().await;
    let client = c.pg(old).await;
    client.simple_query("CREATE TABLE t (id int4)").await.unwrap();
    client.simple_query("INSERT INTO t VALUES (1)").await.unwrap();
    c.kill(old).await;
    // A new leader emerges among the survivors.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    let neu = loop {
        let mut found = None;
        for id in (0..3).filter(|&i| i != old) {
            if let Some(st) = c.status(id).await {
                if let Some(l) = st.current_leader { if l != old { found = Some(l); } }
            }
        }
        if let Some(l) = found { break l; }
        assert!(tokio::time::Instant::now() < deadline, "no new leader");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };
    // New leader has the committed row; a fresh write lands.
    let nl = c.pg(neu).await;
    let rows = nl.simple_query("SELECT id FROM t").await.unwrap();
    assert_eq!(rows.iter().filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))).count(), 1);
    nl.simple_query("INSERT INTO t VALUES (2)").await.unwrap();
    // Old leader respawns and rejoins.
    c.respawn(old);
    c.wait_applied(old, 2).await;
}
```

- [ ] **Step 3: Runtime learner join + leave.**
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_join_then_leave() {
    let mut c = Cluster::spawn(3).await;
    let leader = c.wait_for_leader().await;
    let client = c.pg(leader).await;
    client.simple_query("CREATE TABLE t (id int4)").await.unwrap();
    for i in 0..5 { client.simple_query(&format!("INSERT INTO t VALUES ({i})")).await.unwrap(); }
    // Spawn a 4th node (id 3) and add it as a learner, then promote.
    c.add_node(3).await; // helper: spawn a fresh node with a new dir + ports, bootstrap=false
    let addr3 = c.nodes[3].node_addr.clone();
    assert!(matches!(c.control(leader, harness::ctl_add_learner(3, addr3)).await, Some(_)));
    assert!(matches!(c.control(leader, harness::ctl_change_membership(vec![0,1,2,3])).await, Some(_)));
    c.wait_applied(3, 2).await; // learner caught up over TCP
    // Remove node 2 from the group, then kill it.
    assert!(matches!(c.control(leader, harness::ctl_change_membership(vec![0,1,3])).await, Some(_)));
    c.kill(2).await;
    // Cluster still serves.
    let l = c.wait_for_leader().await;
    c.pg(l).await.simple_query("INSERT INTO t VALUES (9)").await.unwrap();
}
```
Add harness helpers: `Cluster::add_node(&mut self, id)` (allocate ports+dir, spawn bootstrap=false, push to `nodes`/`peers_arg`), and convenience constructors `ctl_add_learner(id, addr)` / `ctl_change_membership(ids)` returning `ControlRequest`. Use `c.control(...)`'s `ControlResponse::Ok`/`Err` to assert success (treat `Err` as a test failure with the message).

- [ ] **Step 4: Bank conservation under a crash + partition nemesis.**
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bank_conserves_under_crash_and_partition_nemesis() {
    let mut c = Cluster::spawn(3).await;
    let leader = c.wait_for_leader().await;
    const ACCOUNTS: i64 = 4;
    const SEED: i64 = 100;
    let setup = c.pg(leader).await;
    setup.simple_query("CREATE TABLE accounts (id int8, bal int8)").await.unwrap();
    for id in 0..ACCOUNTS {
        setup.simple_query(&format!("INSERT INTO accounts VALUES ({id}, {SEED})")).await.unwrap();
    }
    // Concurrent transfer clients against the leader (reconnect to current leader
    // each transfer so a failover doesn't wedge the workload).
    // Nemesis: between transfer batches, kill+respawn a follower and toggle a
    // minority partition (never the leader's majority). After healing, re-resolve
    // the leader, reseed (implicit on its leadership edge), and read the total.
    // ... (full workload below)
    let total = read_total(&mut c, ACCOUNTS).await;
    assert_eq!(total, ACCOUNTS * SEED, "no acked transfer lost across crash + partition");
}
```
**Implementation guidance (workload + nemesis):** mirror the structure of `crates/cluster/tests/jepsen_bank.rs::run_durable_bank`, but drive SQL via `tokio-postgres` over the network instead of an in-process engine, and make the **nemesis** kill/respawn a follower and `SetPartition`/`Heal` a minority between transfer rounds (the leader stays in the majority; if the leader moves, the workload re-resolves it via `wait_for_leader` and reconnects). A transfer is `BEGIN; UPDATE accounts SET bal=bal-amt WHERE id=from; UPDATE …+amt… WHERE id=to; COMMIT` via `client.simple_query` (or a tokio-postgres transaction); bound each with a timeout so an indeterminate commit under a fault doesn't hang (treat timeout/error as an indeterminate transfer — never panic). `read_total` sums `SELECT bal FROM accounts WHERE id=k` over the re-resolved leader after healing. Keep counts modest (e.g. 3 clients × 8 transfers) — real processes + fsync are slow. Assert `total == ACCOUNTS*SEED` and that at least one transfer committed.

- [ ] **Step 5: Run all scenarios (3×, non-flaky).**
```
__COMPAT_LAYER=RunAsInvoker cargo test -p crabgresql --test multiprocess 2>&1 | grep "test result"
```
Expected: all pass on 3 runs. Diagnose any hang by capturing child stderr.

- [ ] **Step 6: Verify + commit.**
```
cargo clippy -p crabgresql --all-targets -- -D warnings
git add crates/crabgresql/tests/
git commit -m "test(crabgresql): crash recovery, runtime membership, partition + bank nemesis over real processes"
```

---

### Task 9: Gauntlet, traceability, finish

**Files:** Verify; no new code unless a gate fails.

- [ ] **Step 1: Gauntlet.** Run each, report PASS/FAIL:
```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
__COMPAT_LAYER=RunAsInvoker cargo test --workspace
cargo test -p pgparser --features oracle
bash scripts/check-no-native.sh        # green on Linux CI; locally only windows-sys (known false-positive)
cargo deny check
```
`postcard` is pure-Rust, so `check-no-native.sh`/`cargo deny` stay green (no new native crate). Confirm the multiprocess tests run (or are skipped only for the documented `os error 740` Windows-launch quirk — they must pass on Linux CI).

- [ ] **Step 2: Success-criteria traceability.** Confirm each spec criterion maps to a green test:

| # | Spec criterion | Verifying test(s) |
|---|---|---|
| 1 | 3 OS-process nodes elect a leader over TCP, serve SQL | `multiprocess::bringup_elects_leader_and_serves_sql` |
| 2 | Committed data survives kill-9 + respawn | `multiprocess::committed_write_survives_kill_and_respawn` |
| 3 | Runtime join (learner catch-up over TCP) + leave | `multiprocess::runtime_join_then_leave` |
| 4 | Minority partition tolerated; heal → catch-up | `transport::testcluster::minority_partition_then_heal_over_tcp`; nemesis test |
| 5 | Bank total conserved under crash + partition nemesis | `multiprocess::bank_conserves_under_crash_and_partition_nemesis` |
| 6 | Reseed-on-leadership prevents xid regression | exercised by failover + bank tests (no xid-reuse anomaly); covered by `server_node` reseed wiring |
| 7 | Pure-Rust, forbid(unsafe), gauntlet green; in-process suites pass | gauntlet (Step 1) |

If any row lacks a green test, add it.

- [ ] **Step 3: Final whole-diff review + finish.** Dispatch a code-reviewer over the SP9 diff (focus: transport correctness — reconnect-on-drop, timeout/Unreachable mapping, bidirectional partition; no `std::sync::Mutex` guard held across `.await`; `ServerNode` task lifecycle + reseed edge; harness has no correctness sleeps and always kills children). Then run `superpowers:finishing-a-development-branch`.

- [ ] **Step 4: Commit (if anything changed).**
```
git add -A
git commit -m "test(sp9): gauntlet green; D2b network/multiprocess success-criteria traceability"
```

---

## Self-Review

**Spec coverage:** node binary mode + two listeners (T6/T5); `BasicNode{addr}` routing (T3/T5); framed postcard transport (T1); Raft RPC client/server (T2/T3); control plane (T2/T4); partition state (T1) + bidirectional partition (T2 server / T3 client / T4 test); membership bring-up + runtime join/leave (T5 bootstrap / T8); SQL frontend + shared engine (T5); reseed-on-leadership (T5); cross-process recovery (T8); harness + rigor matrix incl. crash+partition bank nemesis (T7/T8); retained in-process path (untouched); deps/purity + gauntlet (T9). All spec sections map to tasks.

**Placeholder scan:** Two spots intentionally reference an existing implementation rather than re-printing it: `serve_sql`/`pgwire_serve_one` reuses the binary's existing pgwire serve loop (re-implementing the Postgres protocol would be wrong — DRY), and the bank nemesis workload mirrors `jepsen_bank::run_durable_bank` (driving SQL over tokio-postgres). Both name the exact source to copy from and the exact behavior required; these are reuse directives, not vague TODOs. All other steps carry complete code.

**Type consistency:** `NodeId = u64`, `TypeConfig`, `WriteBatch` from `crate::types` throughout; `NodeStore`/`DurableLogStore`/`DurableStateMachineStore` + `sm_kv()` from SP8; `SqlEngine::{replicated,reseed_counters,connect}` + `RaftCommitter` from SP7; wire enums (`NodeRequest`/`NodeResponse`/`RaftRpc`/`RaftRpcResp`/`ControlRequest`/`ControlResponse`/`NodeStatus`) defined in T1 and used identically in T2–T8; `PartitionState::{blocked,set,heal}`, `ShutdownSignal::{fire,wait}`, `TcpRaftNetwork{from,partition}`, `NodeConfig` fields consistent across tasks. openraft API names (RPC method signatures, `metrics()` fields, `add_learner`/`change_membership`, `RPCOption` ttl accessor) are flagged for compiler-verification against 0.9.24, with the in-process `network.rs`/`cluster.rs` as the authoritative template to copy.
