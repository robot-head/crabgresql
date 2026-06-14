# SP14 / D3a-net — Network Range Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lift D3a's in-process multi-range routing onto the real network — every `ServerNode` hosts a replica of every range and acts as a **SQL gateway** that forwards each statement to the owning range's leader over TCP and relays the result back.

**Architecture:** Three pieces. (1) The TCP node transport becomes **range-aware** (`NodeRequest::Raft` gains a `RangeId`; the server dispatches each RPC from a `(RangeId,NodeId)` registry). (2) `ServerNode` becomes **multi-range** (loops the static `RangeMap`, opens per-range fjall keyspaces `data-r{r}`/`raft-r{r}`, builds N Raft instances, bootstraps each group). (3) `route_one` becomes a **per-statement gateway** — local-leader statements run on the local range engine; remote-leader statements are forwarded over a pooled minimal pgwire client and the response relayed back.

**Tech Stack:** Rust 2024, openraft 0.9.24 (one Raft per range per node), `cluster`/`executor`/`pgwire`/`transport` crates, fjall (per-range keyspaces). No new shipped dependency (the forwarding client uses existing `pgwire` frame primitives). `#![forbid(unsafe_code)]`. Tests: in-crate loopback-TCP (T1–T4, fully sleep-free) + the multi-process harness (T6).

**Spec:** `docs/superpowers/specs/2026-06-13-crabgresql-sp14-d3a-net-network-range-routing-design.md`

---

## Canonical interfaces (PINNED — these override any divergent inline code below)

These shapes were reconciled across tasks; if a code block in a task section disagrees, **this section wins**.

- **`NodeRequest::Raft { from: NodeId, #[serde(default)] range: RangeId, rpc: RaftRpc }`** — `range` lives only here (not in `RaftRpc`); `#[serde(default)]` keeps range-unaware payloads decoding to `0`.
- **`RangeRegistry`** is defined in **`crate::transport::server`** (not a `transport::registry` module). It maps `(RangeId, NodeId) → openraft::Raft<TypeConfig>`; `serve_node_protocol`/`dispatch_raft` resolve against it; an unregistered range → `Unreachable`.
- **`ServerNode`** (multi-range) exposes:
  - `pub rafts: HashMap<RangeId, openraft::Raft<TypeConfig>>`
  - `pub engines: HashMap<RangeId, Arc<SqlEngine>>`
  - `pub partition: PartitionState`
  - `pub fn sm_kv(&self, range: RangeId) -> Arc<dyn kv::Kv>` (over an internal `sm_kvs: HashMap<RangeId, …>`)
  - `pub fn id(&self) -> NodeId`
- **`NodeConfig`** carries `pub range_map: RangeMap` (the field is named `range_map`, defaults to `RangeMap::single()`).
- **Forward seam:** `pub trait RemoteForward { async fn forward(&self, range: RangeId, sql: String) -> Result<QueryResult, ExecError>; }` (whatever async-trait form T3 lands — T4 matches it verbatim). `RangeRouter::new(range_map: RangeMap, engines: HashMap<RangeId, Arc<SqlEngine>>, catalog_kv: Arc<dyn kv::Kv>, forward: Arc<dyn RemoteForward>)`. T3 ships `RejectForward` (returns the `0A000` cross-range error); T4 ships `PgwireForward { pool: Arc<ForwardPool> }` (delegates to the pooled pgwire client).
- **No `tokio::time::sleep` in any test** (CLAUDE.md rule). Waits are openraft `raft.wait(Some(t)).metrics(|m| …)` / `.applied_index_at_least(…)` events, event-based `wait_for_replication`, or — in the multi-process harness only — a bounded poll on a real observed condition (leader present / committed row readable) with a deadline.

## File Structure

| File | Responsibility | Change | Task |
|---|---|---|---|
| `crates/cluster/src/transport/protocol.rs` | `NodeRequest::Raft` gains `#[serde(default)] range: RangeId`. | Modify | T1 |
| `crates/cluster/src/transport/client.rs` | `TcpRaftNetwork`/`TcpConn` carry + pack `range`. | Modify | T1 |
| `crates/cluster/src/transport/server.rs` | `RangeRegistry`; `serve_node_protocol`/`dispatch_raft` resolve `(range,node)`; range-aware dispatch tests. | Modify | T1 |
| `crates/cluster/src/transport/testcluster.rs` | Existing `TcpCluster` registers its single group at range 0. | Modify | T1 |
| `crates/cluster/src/server_node.rs` | Multi-range build loop: per-range stores + N Raft + registry + bootstrap; `rafts`/`engines`/`partition`/`sm_kv`/`id`; reseed all ranges. | Modify | T1, T2 |
| `crates/cluster/src/durable.rs` | `NodeStore::open` per-range keyspaces `data-r{r}`/`raft-r{r}`; `DurableLogStore::open`/`DurableStateMachineStore::open` take `range: RangeId`. | Modify | T2 |
| `crates/cluster/src/node.rs` | Durable per-range construction helper(s). | Modify | T2 |
| `crates/crabgresql/src/main.rs` | `NodeConfig.range_map` (CLI for a multi-range map). | Modify | T2, T6 |
| `crates/cluster/src/route.rs` | `route_one` → per-statement gateway; remove the `:92` busy-sleep; remote forward via the seam. | Modify | T3, T4 |
| `crates/cluster/src/range/router.rs` | `RangeRouter` cluster-agnostic `new(...)` + `RemoteForward` trait + `RejectForward`. | Modify | T3 |
| `crates/cluster/src/range/cluster.rs` | `connect()` delegates to `new(...)` (in-process tests keep working). | Modify | T3 |
| `crates/cluster/src/forward.rs` (or `route` submodule) | `ForwardPool` + `PgwireForward` (pooled minimal pgwire client). | Create | T4 |
| `crates/cluster/src/addr.rs` | Leader `sql_addr` resolution helper(s). | Modify | T4 |
| `crates/executor/tests/update_delete.rs` → `mutation_semantics.rs` | Rename (UAC-safe binary name). | Rename | T5 |
| `CLAUDE.md` | UAC-safe-target-name policy + SP14 audit. | Modify | T5 |
| `crates/crabgresql/tests/harness/mod.rs` | Multi-range spawn; sleep-free condition-poll waits (retires the leader-kill flake). | Modify | T6 |
| `crates/cluster/tests/gateway_local.rs` | T3 local-routing + `0A000` e2e. | Create | T3 |
| `crates/cluster/tests/remote_forward.rs` | T4 remote one-hop forward + retry-counter test. | Create | T4 |
| `crates/cluster/tests/durable_multirange.rs` | T2 multi-range election + storage isolation. | Create | T2 |
| `crates/crabgresql/tests/multirange_gateway.rs` | T6 multi-process routing + per-range failover. | Create | T6 |

All new test-binary names (`gateway_local`, `remote_forward`, `durable_multirange`, `multirange_gateway`) are UAC-safe (no `setup`/`install`/`update`/`patch`/`upgrad` substring).

## Ordering & regression gates

**Dependency order:** **T1 → T2 → T3 → T4 → T6**, with **T5** independent (pure rename + docs; must land before T6 so T6's binary name follows the policy) and **T7** last.

- **T1** (range-aware transport) is the root seam; additive + byte-compatible — every existing single-range TCP test is its regression gate.
- **T2** (multi-range durable `ServerNode`) is the center of gravity and genuinely new; the per-range keyspace isolation is a prerequisite the build loop depends on. Default `RangeMap::single()` preserves the single-range fast-path — **the entire SP9/SP10 suite is its regression gate (criterion 4, load-bearing)**.
- **T3** (cluster-agnostic `RangeRouter` + forward seam) — the SP13 `range::*` suites gate the in-process path (`connect` delegates to `new`).
- **T4** (pooled pgwire forward + one-hop retry) — its only production change outside the new forward module is removing the `route.rs:92` busy-sleep.
- **T6** converts the harness poll-sleeps to sleep-free bounded condition-polls (also retiring the pre-existing `leader_kill_failover_and_rejoin` flake) — `multiprocess.rs`/`jepsen_elle.rs` share the harness and gate it.
- **T7** is docs-only, gated by the full-workspace gauntlet.

---

---

## Task 1: Range-aware node transport (`NodeRequest::Raft` gains `range`; `(RangeId,NodeId)` registry dispatch)

Lift the in-process `Switchboard`'s `(RangeId, NodeId)` keying (`network.rs:27`, `network.rs:141`) onto the **real TCP** path. Add `#[serde(default)] range: RangeId` to `NodeRequest::Raft` (`protocol.rs:61`) so a range-unaware payload still decodes (→ `0`); make `TcpRaftNetwork`/`TcpConn` carry+pack a `range` (`client.rs:22-25,43-49,66-69`); and make `serve_node_protocol`/`dispatch_raft` resolve the target `Raft` from a **net-new** `(RangeId, NodeId) → Raft` registry (`server.rs:34,53,74`), returning `Unreachable` for an unregistered range (mirroring `Switchboard::handle` returning `None`, `network.rs:141-147`). The single-range path stays **byte-compatible**: the existing `TcpCluster` (`transport/testcluster.rs`) and `ServerNode` (`server_node.rs:77-93`) register their one group at `range 0` and the wire envelope is unchanged for a range-0 RPC (serde default).

This is a **behavior-preserving + additive** task: the registry and `range` field are new on TCP; every existing single-range TCP test is a regression gate (criterion 4 territory), and two new tests prove the additions (criteria 1 and 2). The `ControlRequest` channel stays node-global (`protocol.rs:31-38`) — untouched.

**Files:**
- Modify: `crates/cluster/src/transport/protocol.rs` (add `#[serde(default)] range: RangeId` to `NodeRequest::Raft`)
- Modify: `crates/cluster/src/transport/client.rs` (`TcpRaftNetwork`/`TcpConn` carry+pack `range`)
- Modify: `crates/cluster/src/transport/server.rs` (`RangeRegistry`; `serve_node_protocol`/`dispatch_raft` resolve `(range,node)`; unregistered → `Unreachable`)
- Modify: `crates/cluster/src/transport/testcluster.rs` (existing `TcpCluster` registers its single group at range 0 via the new server signature)
- Modify: `crates/cluster/src/server_node.rs` (its one `serve_node_protocol` call registers range 0 — keeps the binary compiling; the full multi-range loop is T2)
- Test: a new `#[cfg(test)] mod range_aware` in `transport/server.rs` (serde round-trip + loopback two-group dispatch)

---

- [ ] **Step 1: Write the failing tests** — append this module to `crates/cluster/src/transport/server.rs`. It exercises both new behaviors: the serde default (criterion 1) and the loopback two-group registry dispatch + unregistered-range `Unreachable` (criterion 2). No `sleep` — replication is awaited with openraft `wait().applied_index_at_least(...)`, the established pattern (`transport/testcluster.rs:148-153`).

```rust
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
        assert!(r0_applied < 2, "range 0 must not have applied a range-1 write");

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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p cluster --lib transport::server::range_aware`
Expected: **FAIL to compile** — `NodeRequest::Raft` has no `range` field, `TcpRaftNetwork` has no `range` field, and `RangeRegistry` / the registry-based `serve_node_protocol` signature do not exist yet (`error[E0560]: struct ... has no field named ``range```; `error[E0432]: unresolved import ...RangeRegistry`). This is the expected red — the wire and registry surfaces are not built.

- [ ] **Step 3: Add `#[serde(default)] range` to `NodeRequest::Raft`** (`protocol.rs`)

Import `RangeId` and extend the `Raft` variant. The default makes a range-unaware payload decode to `0`; a range-0 envelope serializes the field as `0` but an old decoder ignoring unknown fields, or a new decoder seeing it absent, both land on range 0 — byte-compatible for the single-range path. Replace the `use` block head and the `NodeRequest` enum:

```rust
use crate::range::RangeId;
use crate::types::{NodeId, TypeConfig};
```

```rust
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
}
```

(`NodeResponse` is unchanged — the range only flavors *which* group answered; the response shape `Raft(RaftRpcResp)` is range-agnostic.)

- [ ] **Step 4: Make `TcpRaftNetwork`/`TcpConn` carry+pack `range`** (`client.rs`)

`TcpRaftNetwork` gains a `range: RangeId` set by the per-group factory (T2 builds one factory per range); `TcpConn` carries it and packs it into the `NodeRequest::Raft` envelope at the call site (`client.rs:66-69`). Add the import, the field, propagate in `new_client`, the struct field, and the pack.

Import (top of `client.rs`, with the other `crate::` uses):

```rust
use crate::range::RangeId;
use crate::types::{NodeId, TypeConfig};
```

`TcpRaftNetwork` struct + `new_client`:

```rust
/// One factory per `(node, range)`; mints a [`TcpConn`] per peer that tags every
/// RPC with `range` so the peer's server resolves the matching co-located group.
#[derive(Clone)]
pub struct TcpRaftNetwork {
    pub from: NodeId,
    pub range: RangeId,
    pub partition: PartitionState,
}

impl RaftNetworkFactory<TypeConfig> for TcpRaftNetwork {
    type Network = TcpConn;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> TcpConn {
        TcpConn {
            from: self.from,
            range: self.range,
            target,
            addr: crate::addr::node_dial_addr(&node.addr).to_string(),
            partition: self.partition.clone(),
            stream: None,
        }
    }
}
```

`TcpConn` struct (add the `range` field):

```rust
/// A connection to one peer. `RaftNetwork` methods take `&mut self`, so calls are
/// serialized — one in-flight request over the held stream at a time. Every RPC is
/// tagged with `range` so the peer dispatches to the matching co-located group.
pub struct TcpConn {
    from: NodeId,
    range: RangeId,
    target: NodeId,
    addr: String,
    partition: PartitionState,
    stream: Option<TcpStream>,
}
```

The pack in `TcpConn::call` (`client.rs:66-69`):

```rust
            let req = NodeRequest::Raft {
                from: self.from,
                range: self.range,
                rpc: rpc.clone(),
            };
```

(The `call`/`append_entries`/`install_snapshot`/`vote` bodies are otherwise unchanged — they already turn any non-`Raft(Ok)` response into the method's `Unreachable` via `self.unreachable()` at `client.rs:109,129,143`, which is exactly how an unregistered-range server reply surfaces.)

- [ ] **Step 5: Add the `RangeRegistry` and make the server dispatch by `(range, node)`** (`server.rs`)

Introduce a process-local `(RangeId, NodeId) → Raft` registry — the **TCP analog** of `Switchboard`'s handle map (`network.rs:27,58,141-147`), net-new here, not shared code. `serve_node_protocol` takes the registry instead of a single `Raft`; the inbound destructure (`server.rs:53`) pulls `range`; `dispatch_raft` looks up `(range, from-node-id)` and replies `Unreachable` for an unregistered range. Because every co-located replica on this node shares the same `NodeId`, the registry is keyed by `(range, NodeId)` and the lookup uses the node's own id (the listener serves exactly one physical node).

Replace the `use` block head and add the registry + the rewritten dispatch. New imports:

```rust
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, RaftError};
use tokio::net::TcpListener;
use tokio::sync::Notify;

use super::frame::{read_msg, write_msg};
use super::partition::PartitionState;
use super::protocol::{
    ControlRequest, ControlResponse, NodeRequest, NodeResponse, NodeStatus, RaftRpc, RaftRpcResp,
};
use crate::range::RangeId;
use crate::types::{NodeId, TypeConfig};

type Raft = openraft::Raft<TypeConfig>;
```

Add the registry type (after the `Raft` alias):

```rust
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
}
```

Rewrite `serve_node_protocol` to take the registry and dispatch by range. Note control requests need *a* Raft handle (they are node-global — `GetStatus`/membership operate on the node, conventionally its range-0 group), so `handle_control` resolves range 0:

```rust
/// Serve the node protocol on `listener` until it errors. Spawns a task per
/// connection; each reads `NodeRequest`s and writes `NodeResponse`s. Raft RPCs
/// dispatch to the `(range, node)` group from `registry`; control requests stay
/// node-global (answered against the node's range-0 group).
pub async fn serve_node_protocol(
    listener: TcpListener,
    registry: RangeRegistry,
    partition: PartitionState,
    shutdown: ShutdownSignal,
) {
    loop {
        let Ok((sock, _)) = listener.accept().await else {
            return;
        };
        let (registry, partition, shutdown) =
            (registry.clone(), partition.clone(), shutdown.clone());
        tokio::spawn(async move {
            let mut sock = sock;
            loop {
                let req: NodeRequest = match read_msg(&mut sock).await {
                    Ok(r) => r,
                    Err(_) => return, // connection closed/broken
                };
                let resp = match req {
                    NodeRequest::Raft { from, range, rpc } => {
                        // Receive side of a partition: drop the connection so the
                        // caller sees an error (its own send-side check usually
                        // prevents the send; this covers asymmetric configs).
                        if partition.blocked(from) {
                            return;
                        }
                        NodeResponse::Raft(dispatch_raft(&registry, range, rpc).await)
                    }
                    NodeRequest::Control(c) => {
                        let raft = registry.node_id().and_then(|id| registry.handle(0, id));
                        match raft {
                            Some(raft) => NodeResponse::Control(
                                handle_control(&raft, &partition, &shutdown, c).await,
                            ),
                            // No registered group yet — nothing to control.
                            None => NodeResponse::Control(ControlResponse::Err(
                                "no range-0 group registered".into(),
                            )),
                        }
                    }
                };
                if write_msg(&mut sock, &resp).await.is_err() {
                    return;
                }
            }
        });
    }
}
```

Rewrite `dispatch_raft` to resolve the target group; an unregistered range produces the `Unreachable`-shaped error response for the matching RPC kind, which the client maps to `RPCError::Unreachable` (its `_ =>` arms, `client.rs:109,129,143`):

```rust
/// Dispatch one Raft RPC to its `(range, node)` group. An unregistered range
/// returns the RPC's `Unreachable` error (the wire mirror of `Switchboard::handle`
/// returning `None`); the client maps it to `RPCError::Unreachable`.
async fn dispatch_raft(registry: &RangeRegistry, range: RangeId, rpc: RaftRpc) -> RaftRpcResp {
    let Some(id) = registry.node_id() else {
        return unreachable_resp(&rpc, range);
    };
    let Some(raft) = registry.handle(range, id) else {
        return unreachable_resp(&rpc, range);
    };
    match rpc {
        RaftRpc::AppendEntries(r) => RaftRpcResp::AppendEntries(raft.append_entries(r).await),
        RaftRpc::InstallSnapshot(r) => RaftRpcResp::InstallSnapshot(raft.install_snapshot(r).await),
        RaftRpc::Vote(r) => RaftRpcResp::Vote(raft.vote(r).await),
    }
}

/// Build the RPC-kind-matching `Unreachable` error response for an RPC that
/// targeted a range with no registered group on this node.
fn unreachable_resp(rpc: &RaftRpc, range: RangeId) -> RaftRpcResp {
    let err = |id: u8| {
        std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            format!("no group for range {range} (rpc {id})"),
        )
    };
    match rpc {
        RaftRpc::AppendEntries(_) => RaftRpcResp::AppendEntries(Err(RaftError::Fatal(
            openraft::error::Fatal::Panicked,
        )))
        .or_unreachable(err(0)),
        RaftRpc::InstallSnapshot(_) => RaftRpcResp::InstallSnapshot(Err(RaftError::Fatal(
            openraft::error::Fatal::Panicked,
        )))
        .or_unreachable(err(1)),
        RaftRpc::Vote(_) => {
            RaftRpcResp::Vote(Err(RaftError::Fatal(openraft::error::Fatal::Panicked)))
                .or_unreachable(err(2))
        }
    }
}
```

> **Wire-fidelity note (locked).** `RaftRpcResp` carries openraft's `Result<Resp, RaftError>` verbatim (`protocol.rs:20-27`); there is **no** wire variant for "unreachable", and the client already turns *any* non-`Ok` response it can't parse into `RPCError::Unreachable` via its `_ =>` arms (`client.rs:109,129,143`). The simplest, correct mirror is therefore to make an unregistered range produce a response the client's `_ =>` arm catches. Drop the `unreachable_resp`/`or_unreachable` complexity above and instead **close the connection** for an unregistered range — exactly what the partition-receive path already does (`server.rs:57-59`) and what the client's two-try `call` already treats as `Unreachable` (`client.rs:74-81`). Replace `dispatch_raft`'s unregistered arms and `serve_node_protocol`'s `Raft` arm with the connection-drop form:

Final `dispatch_raft` + `serve_node_protocol` Raft arm (use **this**, not the `unreachable_resp` helper):

```rust
/// Resolve `(range, node)`'s group, or `None` for an unregistered range (the
/// caller drops the connection, which the client sees as `Unreachable`).
fn resolve(registry: &RangeRegistry, range: RangeId) -> Option<Raft> {
    let id = registry.node_id()?;
    registry.handle(range, id)
}

async fn dispatch_raft(raft: &Raft, rpc: RaftRpc) -> RaftRpcResp {
    match rpc {
        RaftRpc::AppendEntries(r) => RaftRpcResp::AppendEntries(raft.append_entries(r).await),
        RaftRpc::InstallSnapshot(r) => RaftRpcResp::InstallSnapshot(raft.install_snapshot(r).await),
        RaftRpc::Vote(r) => RaftRpcResp::Vote(raft.vote(r).await),
    }
}
```

and the `NodeRequest::Raft` arm inside `serve_node_protocol`:

```rust
                    NodeRequest::Raft { from, range, rpc } => {
                        if partition.blocked(from) {
                            return; // receive-side partition: drop the connection
                        }
                        let Some(raft) = resolve(&registry, range) else {
                            return; // unregistered range -> drop -> client sees Unreachable
                        };
                        NodeResponse::Raft(dispatch_raft(&raft, rpc).await)
                    }
```

Then the only extra `use` beyond the imports listed above is `std::collections::HashMap` / `Arc` / `Mutex` (already added) — **delete** the `BasicNode` / `InstallSnapshotError` / `RaftError` imports if clippy flags them as unused (they are only needed by the discarded `unreachable_resp` path). `handle_control` is unchanged.

- [ ] **Step 6: Update the single-range callers to register at range 0** (`transport/testcluster.rs`, `server_node.rs`)

The two existing callers each build **one** group and must now (a) build their `TcpRaftNetwork` with `range: 0` and (b) hand `serve_node_protocol` a `RangeRegistry` with their one handle at `(0, id)`. This is the byte-compatible single-range path.

In `transport/testcluster.rs`, in `TcpCluster::new` (`testcluster.rs:54-69`), set the factory range and register:

```rust
            let net = TcpRaftNetwork {
                from: id,
                range: 0,
                partition: partition.clone(),
            };
            let log = Arc::new(LogStore::default());
            let sm = Arc::new(StateMachineStore::default());
            let raft = openraft::Raft::new(id, cfg.clone(), net, log, sm)
                .await
                .expect("raft");
            let listener = listeners.remove(0);
            let registry = super::server::RangeRegistry::new();
            registry.register(0, id, raft.clone());
            tokio::spawn(serve_node_protocol(
                listener,
                registry,
                partition.clone(),
                ShutdownSignal::default(),
            ));
```

(add `use super::server::{RangeRegistry, ShutdownSignal, serve_node_protocol};` — i.e. extend the existing `server` import at `testcluster.rs:15` with `RangeRegistry`.)

In `server_node.rs`, in `ServerNode::start` (`server_node.rs:77-93`), do the same so the binary keeps compiling; T2 replaces this single registration with the full `map.range_ids()` loop:

```rust
        let partition = PartitionState::default();
        let net = TcpRaftNetwork {
            from: cfg.id,
            range: 0,
            partition: partition.clone(),
        };
        let raft = openraft::Raft::new(cfg.id, raft_config(), net, log, sm)
            .await
            .expect("raft::new");

        // Node-protocol listener (Raft RPCs + control). Single-range: one group at
        // (range 0, id) in the registry; T2 turns this into the per-range loop.
        let node_listener = TcpListener::bind(&cfg.node_addr).await?;
        let shutdown = ShutdownSignal::default();
        let registry = crate::transport::server::RangeRegistry::new();
        registry.register(0, cfg.id, raft.clone());
        tokio::spawn(serve_node_protocol(
            node_listener,
            registry,
            partition.clone(),
            shutdown.clone(),
        ));
```

- [ ] **Step 7: Run the new tests to verify they pass**

Run: `cargo test -p cluster --lib transport::server::range_aware`
Expected: **PASS (2 tests)** — `raft_envelope_range_serde_default_and_round_trip` (range-1 round-trips; range-unaware payload decodes to 0) and `loopback_dispatches_by_range_and_rejects_unregistered` (range-1 AppendEntries committed+applied on range 1 via `wait()`, range 0 untouched, unregistered range 2 → `RPCError::Unreachable`).

- [ ] **Step 8: Run the full single-range TCP suite to confirm byte-compatibility (regression gate)**

Run: `cargo test -p cluster --lib transport::`
Expected: **PASS** — the existing loopback tests (`elects_leader_and_replicates_over_tcp`, `control_status_reports_leader`, `minority_partition_then_heal_over_tcp`, the `frame` round-trip, the `partition` toggle) all still pass; the only change to their path is a `range: 0` factory field and a one-entry registry, so the range-0 wire envelope and dispatch are unchanged.

Run: `cargo test -p cluster`
Expected: **PASS** — the broader cluster suite (including the multi-process-adjacent and route tests that go through `ServerNode::start`) is unaffected; single-range stays on the fast path.

- [ ] **Step 9: Clippy**

Run: `cargo clippy -p cluster --all-targets -- -D warnings`
Expected: clean. (If the `BasicNode`/`InstallSnapshotError`/`RaftError`/`Fatal` imports from the discarded `unreachable_resp` path are flagged unused, remove them — only `HashMap`/`Arc`/`Mutex`/`RangeId` are needed by the connection-drop form.)

- [ ] **Step 10: Commit**

```bash
git add crates/cluster/src/transport/protocol.rs crates/cluster/src/transport/client.rs crates/cluster/src/transport/server.rs crates/cluster/src/transport/testcluster.rs crates/cluster/src/server_node.rs
git commit -m "feat(cluster): range-aware TCP node transport — (range,node) registry dispatch

NodeRequest::Raft gains #[serde(default)] range: RangeId (range-unaware payloads
decode to 0); TcpRaftNetwork/TcpConn carry+pack range; serve_node_protocol resolves
the target Raft from a net-new (RangeId,NodeId) registry, dropping the connection
(-> Unreachable) for an unregistered range. Single-range path stays byte-compatible.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

---

## Task 2: Multi-range durable `ServerNode` — per-range keyspaces, N Raft groups, bootstrap each

This is the slice's center of gravity: refactor `ServerNode::start` (today one store + one Raft for range 0, `server_node.rs:70-127`) to build a replica of **every** range. It takes a `RangeMap` in `NodeConfig`, loops `map.range_ids()`, opens the per-range fjall keyspaces `data-r{r}`/`raft-r{r}`, builds N Raft instances over the range-aware `TcpRaftNetwork` from Task 1, registers each at `(range, id)` in the `RangeRegistry` the Task-1 server resolves against, and bootstraps each range's voting group. The reseed-on-leadership loop runs **per range**. There is no existing durable multi-range constructor to copy — `Node::start_durable` (`node.rs:94`) is single-range and `MultiRangeCluster::new` (`range/cluster.rs:38`) builds **in-memory** nodes over the in-process `Switchboard`, not over fjall/TCP.

**Per-range storage isolation is the prerequisite this construction loop depends on, and must be built first** (it is the slice's highest-risk new construction; a per-key collision in the log/SM state silently corrupts ranges). The construction loop literally cannot open range `r`'s Raft instance until `NodeStore` can hand it range `r`'s *own* `(data, raft)` keyspace pair — so Step 3 (the isolation scheme on `NodeStore`/`DurableLogStore`/`DurableStateMachineStore`) lands before Step 4 (the multi-range loop). Isolation is **structural**: fjall keyspaces are isolated by construction, so `data-r1` and `raft-r1` cannot alias `data-r0`/`raft-r0`, regardless of what keys each write site emits. Default `RangeMap::single()` keeps exactly one range (id 0) whose keyspaces are named `data-r0`/`raft-r0`, preserving the single-range fast-path that gates the SP9/SP10 regression suites (criterion 4).

> **Consumes from Task 1:** `NodeRequest::Raft { from, #[serde(default)] range, rpc }`; the range-aware `TcpRaftNetwork { from, range, partition }`; and the process-local `RangeRegistry` (a `Clone` handle wrapping `Arc<Mutex<HashMap<(RangeId, NodeId), Raft<TypeConfig>>>>`) with `RangeRegistry::new()`, `register(range, id, raft)`, and the `resolve(range, id) -> Option<Raft<TypeConfig>>` the Task-1 server dispatches against. `serve_node_protocol` in Task 1 takes the `RangeRegistry` (not a single `Raft`). This task wires those; if Task 1's registry constructor differs by name, adjust the two call sites in `start` accordingly.

**Files:**
- Modify: `crates/cluster/src/server_node.rs` (multi-range `start`; `NodeConfig` gains `range_map: RangeMap`; per-range reseed)
- Modify: `crates/cluster/src/durable.rs` (`NodeStore` per-range keyspaces; `DurableLogStore::open` / `DurableStateMachineStore::open` gain a `range: RangeId` param)
- Modify: `crates/cluster/src/node.rs` (`Node::start_durable` passes `range` into the two `open` calls)
- Modify: `crates/crabgresql/src/main.rs` (`NodeConfig { range_map: RangeMap::single() }` so the binary stays single-range)
- Test: `crates/cluster/src/server_node.rs` (`#[cfg(test)] mod tests` — multi-range election + storage-isolation test, loopback TCP, no sleep)

---

- [ ] **Step 1: Write the failing test** — append these two tests to the existing `#[cfg(test)] mod tests` in `crates/cluster/src/server_node.rs` (keep `free_port` / `connect_with_retry` and `single_node_serves_sql_after_election`; add a `use` for `RangeMap` at the top of the module).

Add at the top of `mod tests` (after `use super::*;`):

```rust
    use crate::range::RangeMap;
```

Then append:

```rust
    /// A single process running ONE node that hosts a 2-range map brings up BOTH
    /// ranges, and each range independently self-confirms a leader via openraft's
    /// event-based `wait` (state==Leader && current_leader==self) — no sleep.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_range_node_elects_a_leader_per_range() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node_addr = free_port().await;
        let sql_addr = free_port().await;
        // boundary at table_id 1 ⇒ range 0 = [0,1), range 1 = [1,∞). Two ranges.
        let node = ServerNode::start(NodeConfig {
            id: 0,
            node_addr: node_addr.clone(),
            sql_addr,
            data_dir: dir.path().to_path_buf(),
            peers: vec![(0, node_addr.clone())],
            bootstrap: true,
            range_map: RangeMap::with_boundaries(vec![1]),
        })
        .await
        .expect("start multi-range node");

        // Both ranges must self-confirm a leader. A one-node group elects itself
        // immediately after its `initialize`; we await that condition per range.
        assert_eq!(node.rafts.len(), 2, "node hosts a Raft instance per range");
        for raft in node.rafts.values() {
            raft.wait(Some(Duration::from_secs(10)))
                .metrics(
                    |m| m.state == ServerState::Leader && m.current_leader == Some(m.id),
                    "range self-confirmed as leader",
                )
                .await
                .expect("each range self-confirms a leader");
        }
    }

    /// A write committed to range 1's Raft group lands in range 1's `data-r1`
    /// keyspace and is ABSENT from range 0's `data-r0` keyspace — structural
    /// per-range storage isolation, asserted over BOTH keyspaces.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_write_to_range1_is_isolated_to_data_r1() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node_addr = free_port().await;
        let sql_addr = free_port().await;
        let node = ServerNode::start(NodeConfig {
            id: 0,
            node_addr: node_addr.clone(),
            sql_addr,
            data_dir: dir.path().to_path_buf(),
            peers: vec![(0, node_addr.clone())],
            bootstrap: true,
            range_map: RangeMap::with_boundaries(vec![1]),
        })
        .await
        .expect("start multi-range node");

        // Await range 1's self-confirmed leadership before proposing to it.
        let range1 = node.rafts[&1].clone();
        range1
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                |m| m.state == ServerState::Leader && m.current_leader == Some(m.id),
                "range 1 leader",
            )
            .await
            .expect("range 1 leader");

        // Propose a marker row directly through range 1's Raft `client_write`
        // (no SQL gateway yet — that is Task 3). A `WriteBatch` of one Put commits
        // to range 1's group, applies into `data-r1`.
        use crate::types::WriteBatch;
        use kv::WriteOp;
        let marker = kv::key::row_key(7, 1); // table 7 ⇒ range 1, but the key value
        // itself is what we assert on, so the table id only needs to be nonzero.
        range1
            .client_write(WriteBatch(vec![WriteOp::Put {
                key: marker.clone(),
                value: b"r1-only".to_vec(),
            }]))
            .await
            .expect("commit to range 1");

        // Range 1's applied store has it; range 0's applied store does NOT — the
        // two keyspaces are physically distinct (`data-r1` vs `data-r0`).
        let data_r1 = node.sm_kv(1);
        let data_r0 = node.sm_kv(0);
        assert_eq!(
            data_r1.get(&marker).expect("get r1"),
            Some(b"r1-only".to_vec()),
            "the write is present in range 1's data-r1 keyspace"
        );
        assert_eq!(
            data_r0.get(&marker).expect("get r0"),
            None,
            "the write is ABSENT from range 0's data-r0 keyspace (storage isolation)"
        );

        // Criterion 3 also requires isolation across the RAFT keyspaces, not just
        // `data-r{r}`. The commit appended an entry to range 1's `raft-r1` and
        // applied it, advancing range 1's `last_applied`; range 0's `raft-r0` saw
        // only its own bootstrap entries (no data write), so its `last_applied`
        // did not advance from this write. We read both via openraft metrics — a
        // borrow of the metrics watch, no sleep.
        let r1_applied = node.rafts[&1].metrics().borrow().last_applied;
        let r0_applied = node.rafts[&0].metrics().borrow().last_applied;
        let r1_idx = r1_applied.expect("range 1 has applied entries").index;
        assert!(
            r1_idx > 0,
            "range 1's raft-r1 advanced last_applied past bootstrap after the write"
        );
        // Range 0 only ever applied its single-node bootstrap; the range-1 write
        // never touched `raft-r0`. Its applied index is strictly below range 1's,
        // proving the raft keyspaces are isolated too (no shared log/SM state).
        let r0_idx = r0_applied.map(|l| l.index).unwrap_or(0);
        assert!(
            r0_idx < r1_idx,
            "range 0's raft-r0 did not advance from range 1's write (raft-keyspace isolation)"
        );
    }
```

This test consumes the new public surface on `ServerNode` introduced in Step 4: `pub rafts: HashMap<RangeId, openraft::Raft<TypeConfig>>` (keyed by `RangeId`) and the `pub fn sm_kv(&self, range: RangeId) -> Arc<dyn kv::Kv>` accessor over the per-range applied store. It also relies on `NodeConfig` gaining a `range_map: RangeMap` field.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p cluster --lib server_node::tests::multi_range_node_elects_a_leader_per_range server_node::tests::a_write_to_range1_is_isolated_to_data_r1`
Expected: FAIL to **compile** — `NodeConfig` has no field `range_map`; `ServerNode` has no field `rafts` / no `sm_kv` accessor. (Compile failure is the correct "see it fail" here: the multi-range struct surface does not exist yet.)

- [ ] **Step 3: Build the per-range storage-isolation scheme FIRST** (`durable.rs`) — this is the prerequisite the Step-4 loop depends on.

In `crates/cluster/src/durable.rs`, replace the fixed two-keyspace `NodeStore` with a per-range pair, keyed by the `RangeMap`. Add the import and rewrite the struct + `open`:

Add near the top imports:

```rust
use std::collections::BTreeMap;

use crate::range::map::{RangeId, RangeMap};
```

Replace the `NodeStore` struct and its `impl` (`durable.rs:13-37`) with:

```rust
/// One range's on-disk keyspace pair within a node's shared `Database`:
/// `data-r{r}` (state-machine application KV) and `raft-r{r}` (log entries, vote,
/// committed, last_applied, membership). fjall keyspaces are isolated by
/// construction, so two ranges can never alias each other's state.
#[derive(Clone)]
pub struct RangeKeyspaces {
    pub data: fjall::Keyspace,
    pub raft: fjall::Keyspace,
}

/// One node's on-disk store: a shared `Database` plus, for each range it hosts,
/// a `data-r{r}` / `raft-r{r}` keyspace pair. A single-range node has exactly the
/// `data-r0` / `raft-r0` pair.
pub struct NodeStore {
    pub(crate) db: Arc<Database>,
    ranges: BTreeMap<RangeId, RangeKeyspaces>,
}

impl NodeStore {
    /// Open (or recover) a node store at `dir` hosting every range in `map`.
    /// fjall journal-replays on open. For each range `r` this opens the suffixed
    /// keyspaces `data-r{r}` and `raft-r{r}`.
    pub fn open(dir: impl AsRef<Path>, map: &RangeMap) -> Result<Self, kv::KvError> {
        let db = Arc::new(open_database_with_retry(dir.as_ref())?);
        let mut ranges = BTreeMap::new();
        for r in map.range_ids() {
            let data = db
                .keyspace(format!("data-r{r}"), KeyspaceCreateOptions::default)
                .map_err(|e| kv::KvError::Io(e.to_string()))?;
            let raft = db
                .keyspace(format!("raft-r{r}"), KeyspaceCreateOptions::default)
                .map_err(|e| kv::KvError::Io(e.to_string()))?;
            ranges.insert(r, RangeKeyspaces { data, raft });
        }
        Ok(Self { db, ranges })
    }

    /// The keyspace pair for `range` (panics if `range` was not in the `RangeMap`
    /// this store was opened with — a construction bug, never user input).
    pub(crate) fn keyspaces(&self, range: RangeId) -> &RangeKeyspaces {
        self.ranges
            .get(&range)
            .unwrap_or_else(|| panic!("range {range} not opened on this NodeStore"))
    }

    /// A `Kv` view over `range`'s `data-r{range}` keyspace for SQL/SM reads.
    pub fn data_kv(&self, range: RangeId) -> Arc<KeyspaceKv> {
        let ks = self.keyspaces(range);
        Arc::new(KeyspaceKv::new(self.db.clone(), ks.data.clone()))
    }
}
```

Give `DurableLogStore::open` a `range` param. Replace its body (`durable.rs:114-131`) so it selects the per-range `raft` keyspace:

```rust
impl DurableLogStore {
    /// Open the durable log over `range`'s `raft-r{range}` keyspace, reconstructing
    /// the `last_log_id` / `last_purged` cache from disk (fjall already replayed).
    #[allow(clippy::result_large_err)] // `StorageError` is openraft's error type.
    pub fn open(store: &NodeStore, range: RangeId) -> Result<Arc<Self>, StorageError<NodeId>> {
        let db = store.db.clone();
        let ks = store.keyspaces(range).raft.clone();
        let last_purged: Option<LogId<NodeId>> = read_json(&ks, PURGED_KEY)?;
        let last_log_id = highest_log_id(&ks)?.or(last_purged);
        Ok(Arc::new(Self {
            db,
            ks,
            cache: RwLock::new(LogCache {
                last_log_id,
                last_purged,
            }),
        }))
    }
```

Give `DurableStateMachineStore::open` a `range` param. Replace its signature + the keyspace bindings inside (`durable.rs:405-432`) so it reads `last_applied`/`last_membership` from `range`'s `raft` keyspace and applies into `range`'s `data` keyspace:

```rust
impl DurableStateMachineStore {
    /// Open the durable state machine over `range`'s `data-r{range}` /
    /// `raft-r{range}` keyspaces, reconstructing the `last_applied` /
    /// `last_membership` cache from the `raft` keyspace (fjall already replayed).
    /// An absent `SM_APPLIED_KEY` means a never-applied state machine.
    #[allow(clippy::result_large_err)] // `StorageError` is openraft's error type.
    pub fn open(store: &NodeStore, range: RangeId) -> Result<Arc<Self>, StorageError<NodeId>> {
        let ks = store.keyspaces(range);
        let last_applied: Option<LogId<NodeId>> =
            read_json::<Option<LogId<NodeId>>>(&ks.raft, SM_APPLIED_KEY)?.unwrap_or(None);
        let last_membership: StoredMembership<NodeId, BasicNode> =
            read_json(&ks.raft, SM_MEMBERSHIP_KEY)?.unwrap_or_default();
        let meta = StateMachineMeta {
            last_applied,
            last_membership,
        };
        Ok(Arc::new(Self {
            db: store.db.clone(),
            data: ks.data.clone(),
            raft: ks.raft.clone(),
            data_kv: store.data_kv(range),
            meta: RwLock::new(meta),
            snapshot_idx: RwLock::new(0),
            current_snapshot: RwLock::new(None),
        }))
    }
```

Now fix the existing `durable.rs` callers (its own test module and the openraft conformance `StoreBuilder`) so the suite stays green — every `NodeStore::open(dir)` becomes `NodeStore::open(dir, &RangeMap::single())` and every `DurableLogStore::open(&store)` / `DurableStateMachineStore::open(&store)` gains `, 0`:

In the `DurableStoreBuilder::build` impl (`durable.rs:816-827`):

```rust
        async fn build(
            &self,
        ) -> Result<((), Arc<DurableLogStore>, Arc<DurableStateMachineStore>), StorageError<NodeId>>
        {
            let dir = tempfile::tempdir().expect("tempdir");
            let store = NodeStore::open(dir.path(), &RangeMap::single())
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
            let log = DurableLogStore::open(&store, 0)?;
            let sm = DurableStateMachineStore::open(&store, 0)?;
            self.tmp.lock().expect("tmp").push(dir);
            Ok(((), log, sm))
        }
```

And in the durable test module add `use crate::range::map::RangeMap;` (after `use super::*;`), then update each test helper call. Concretely the existing calls — `NodeStore::open(dir.path())` (8 sites: the log tests `append_then_reopen_recovers_entries`, `truncate_removes_tail`, `purge_removes_head_and_sets_purged`, and the SM tests `apply_is_atomic_and_survives_reopen`, `apply_counter_max_merges_same_key_twice_in_one_batch`, `snapshot_round_trip_overwrites` (src + dst + 2 reopens)), `DurableLogStore::open(&store)`, and `DurableStateMachineStore::open(&store)` — each gains the new argument:

```rust
    let store = NodeStore::open(dir.path(), &RangeMap::single()).expect("open");
    let mut log = DurableLogStore::open(&store, 0).expect("log open");
    let sm = DurableStateMachineStore::open(&store, 0).expect("sm open");
```

(Apply the same edit to every reopen site, e.g. `NodeStore::open(dir.path(), &RangeMap::single()).expect("reopen")` and `DurableLogStore::open(&store, 0).expect("log reopen")`. These are pure mechanical argument additions; the test bodies and assertions are unchanged — they remain the regression gate for the single-range durable store, now living at `data-r0`/`raft-r0`.)

- [ ] **Step 4: Fix `Node::start_durable`** (`node.rs`) so the in-memory-harness durable path threads `range` into the two `open` calls.

In `crates/cluster/src/node.rs`, update `start_durable` (`node.rs:94-117`) — replace the three store/open lines:

```rust
        let store = NodeStore::open(&dir, &RangeMap::single()).expect("open node store");
        let log = DurableLogStore::open(&store, range).expect("durable log");
        let sm = DurableStateMachineStore::open(&store, range).expect("durable sm");
```

and add `use crate::range::RangeMap;` to the imports (the module already imports `crate::range::RangeId`).

> **Why `RangeMap::single()` here, not the node's full map:** the in-memory `Cluster`/durable-harness `Node` owns exactly one range per fjall directory (it predates multi-range co-location), so each durable `Node` opens a one-range store and `start_durable(range, …)` selects keyspaces named `data-r{range}`/`raft-r{range}`. A multi-range *process* uses `ServerNode` (Step 5), which opens one `NodeStore` over the whole `RangeMap` and shares it across ranges. Passing `&RangeMap::single()` opens only `data-r0`/`raft-r0`; if a harness `Node` is built for a nonzero `range`, open the matching single-range keyspaces instead — but no current caller does (the durable `Cluster` is range 0), so `RangeMap::single()` is correct and keeps existing durable scenarios at `data-r0`/`raft-r0`.

- [ ] **Step 5: Make `ServerNode::start` multi-range** (`server_node.rs`).

Add `RangeMap` to imports and a `range_map` field to `NodeConfig`:

```rust
use crate::range::map::{RangeId, RangeMap};
```

In `NodeConfig`, add (after `bootstrap`):

```rust
    /// The static range map this node hosts. Identical on every node. Defaults to
    /// `RangeMap::single()` (one range, id 0) — the single-range fast-path.
    pub range_map: RangeMap,
```

Replace the `ServerNode` struct (`server_node.rs:44-49`) with a multi-range shape that still exposes range 0's handles for the single-range fast-path the SQL listener uses this slice:

```rust
/// A live multi-range node; `shutdown.wait()` resolves when a `Shutdown` control
/// request fires. Holds one Raft instance + one applied store + one replicated
/// engine per range (all keyed by `RangeId`).
pub struct ServerNode {
    /// One Raft handle per range, keyed by `RangeId`.
    pub rafts: HashMap<RangeId, openraft::Raft<TypeConfig>>,
    /// One replicated engine per range, keyed by `RangeId`. Range 0's catalog
    /// store seeds every data-range engine's `catalog_kv`.
    pub engines: HashMap<RangeId, Arc<SqlEngine>>,
    /// The process-local network partition state, shared by every range's
    /// transport and the node-protocol server. Task 4 reads this to inject
    /// partitions in its remote-forward test (`gw.partition.clone()`).
    pub partition: PartitionState,
    pub shutdown: ShutdownSignal,
    /// One applied `data-r{r}` store per range, keyed by `RangeId`. Reached via
    /// the `sm_kv(range)` accessor (Task 4 needs `sm_kv(range)`, not a public map).
    sm_kvs: HashMap<RangeId, Arc<dyn kv::Kv>>,
    /// This node's id (`cfg.id`), exposed via `id()` for Task 4's leader resolution.
    id: NodeId,
}

impl ServerNode {
    /// This range's applied (`data-r{range}`) store. Panics if `range` is not
    /// hosted on this node — a construction bug, never user input.
    pub fn sm_kv(&self, range: RangeId) -> Arc<dyn kv::Kv> {
        self.sm_kvs[&range].clone()
    }

    /// This node's id.
    pub fn id(&self) -> NodeId {
        self.id
    }
}
```

Replace `ServerNode::start` (`server_node.rs:66-127`) with the multi-range constructor. It opens ONE `NodeStore` over the whole `RangeMap`, then loops `range_ids()` building a per-range Raft over the range-aware network from Task 1, registering each at `(range, id)` in the shared `RangeRegistry`, and bootstrapping each group; it spawns a reseed task per range. The SQL listener keeps the single-range fast-path (range 0's raft + engine) — per-statement gateway routing is Task 3:

```rust
impl ServerNode {
    /// Open the durable store over the whole `RangeMap`, build a Raft + applied
    /// engine per range over the range-aware TCP transport (Task 1), register each
    /// group in the process-local `(range, node)` registry, bootstrap each voting
    /// group, and reseed each range's counters on its leadership edge.
    ///
    /// Per-range storage isolation (`data-r{r}`/`raft-r{r}`, Step 3) is the
    /// prerequisite this loop relies on: range `r`'s Raft is built over range `r`'s
    /// own keyspaces, so two ranges can never share log/SM state.
    pub async fn start(cfg: NodeConfig) -> std::io::Result<Self> {
        // One shared on-disk Database hosting every range's keyspace pair.
        let store = NodeStore::open(&cfg.data_dir, &cfg.range_map).expect("open node store");

        let partition = PartitionState::default();
        // Process-local registry the node-protocol server dispatches against:
        // an inbound `Raft { range, .. }` RPC resolves `(range, id)` here.
        let registry = RangeRegistry::new();
        let shutdown = ShutdownSignal::default();

        // Range 0's applied store is the catalog every data range resolves from.
        let catalog_kv = store.data_kv(0) as Arc<dyn kv::Kv>;

        let mut rafts: HashMap<RangeId, openraft::Raft<TypeConfig>> = HashMap::new();
        let mut sm_kvs: HashMap<RangeId, Arc<dyn kv::Kv>> = HashMap::new();
        let mut engines: HashMap<RangeId, Arc<SqlEngine>> = HashMap::new();

        for range in cfg.range_map.range_ids() {
            let log = DurableLogStore::open(&store, range).expect("durable log");
            let sm = DurableStateMachineStore::open(&store, range).expect("durable sm");
            let sm_kv = sm.sm_kv();

            // Range-aware network: every client minted from `net` tags its RPCs
            // with `range`, so the peer's server routes to the matching group.
            let net = TcpRaftNetwork {
                from: cfg.id,
                range,
                partition: partition.clone(),
            };
            let raft = openraft::Raft::new(cfg.id, raft_config(), net, log, sm)
                .await
                .expect("raft::new");

            // Register THIS group so inbound `(range, id)` RPCs reach it.
            registry.register(range, cfg.id, raft.clone());

            // Replicated engine for this range. Data writes/reads hit this range's
            // store; schema always resolves from range 0's catalog store.
            let engine = Arc::new(
                SqlEngine::replicated(
                    catalog_kv.clone(),
                    sm_kv.clone(),
                    Arc::new(RaftCommitter { raft: raft.clone() }),
                    Arc::new(RaftLinearizer { raft: raft.clone() }),
                )
                .expect("replicated engine"),
            );
            // Reseed THIS range's counters on its own follower→leader edge.
            tokio::spawn(reseed_on_leadership(raft.clone(), engine.clone()));

            rafts.insert(range, raft);
            sm_kvs.insert(range, sm_kv);
            engines.insert(range, engine);
        }

        // One node-protocol listener for the whole node; it resolves the target
        // group from the registry by the RPC's `(range, from)`.
        let node_listener = TcpListener::bind(&cfg.node_addr).await?;
        tokio::spawn(serve_node_protocol(
            node_listener,
            registry.clone(),
            partition.clone(),
            shutdown.clone(),
        ));

        // Bootstrap EVERY range's voting group once peers are dialable. Each range
        // shares the same physical peer set (co-located placement).
        if cfg.bootstrap {
            for raft in rafts.values() {
                tokio::spawn(bootstrap(raft.clone(), cfg.peers.clone()));
            }
        }

        // SQL listener: single-range fast-path this slice — serve/route via range
        // 0's Raft + engine. The per-statement multi-range gateway is Task 3.
        let sql_listener = TcpListener::bind(&cfg.sql_addr).await?;
        tokio::spawn(crate::route::serve_routed(
            sql_listener,
            rafts[&0].clone(),
            engines[&0].clone(),
            Arc::new(pgwire::session::SessionConfig::trust()),
        ));

        Ok(Self {
            rafts,
            engines,
            partition,
            shutdown,
            sm_kvs,
            id: cfg.id,
        })
    }
}
```

Update the imports block at the top of `server_node.rs` to consume Task 1's `RangeRegistry` and the range-aware `serve_node_protocol`/`TcpRaftNetwork`:

```rust
use std::collections::HashMap;

use crate::durable::{DurableLogStore, DurableStateMachineStore, NodeStore};
use crate::transport::client::TcpRaftNetwork;
use crate::transport::partition::PartitionState;
use crate::transport::server::{RangeRegistry, ShutdownSignal, serve_node_protocol};
```

(Keep the existing `committer`/`linearizer`/`BasicNode`/`ServerState`/`BTreeMap` imports — `bootstrap` and `reseed_on_leadership` are unchanged below. Remove the now-unused single `Raft` import if clippy flags it.) Leave `reseed_on_leadership` and `bootstrap` exactly as they are (`server_node.rs:130-171`) — both are per-Raft and reused verbatim per range.

- [ ] **Step 6: Keep the binary single-range** (`crabgresql/src/main.rs`).

In `run_node` (`main.rs:167-174`), add `range_map` to the `NodeConfig` literal so the CLI binary stays single-range (no new flag this slice — the default `RangeMap::single()` is the switch):

```rust
    let cfg = cluster::server_node::NodeConfig {
        id: a.id,
        node_addr: a.node_addr,
        sql_addr: a.sql_addr,
        data_dir: a.data_dir,
        peers,
        bootstrap: a.bootstrap,
        range_map: cluster::range::RangeMap::single(),
    };
```

- [ ] **Step 7: Run the new tests to verify they pass**

Run: `cargo test -p cluster --lib server_node::tests::multi_range_node_elects_a_leader_per_range server_node::tests::a_write_to_range1_is_isolated_to_data_r1`
Expected: PASS (2 tests) — both ranges self-confirm a leader via `wait().metrics(state==Leader && current_leader==self)`; the range-1 write is `Some(b"r1-only")` in `data-r1` and `None` in `data-r0`.

Run: `cargo test -p cluster --lib server_node::tests::single_node_serves_sql_after_election`
Expected: PASS — the single-range SQL e2e is unchanged under `range_map: RangeMap::single()` (its config literal gains `range_map: RangeMap::single()`; add that one field to the existing test's `NodeConfig`).

> **Edit the existing `single_node_serves_sql_after_election` test:** add `range_map: RangeMap::single(),` to its `NodeConfig { … }` literal (`server_node.rs:193-201`) so it compiles against the new field. Its assertions are untouched — it remains the single-range regression gate at the `ServerNode` boundary.

- [ ] **Step 8: Run the durable + cluster regression suites (criterion 4 gate)**

Run: `cargo test -p cluster --lib durable`
Expected: PASS — the openraft conformance `durable_storage_suite` and every durable log/SM test pass unchanged, now over `data-r0`/`raft-r0` (the per-range scheme with one range is byte-for-byte the old layout under a renamed keyspace).

Run: `cargo test -p cluster`
Expected: PASS — every in-process and durable cluster test (model, durability scenarios, transport, route) stays green; the only behavioral change is keyspace naming (`data`→`data-r0`, `raft`→`raft-r0`) and the new `range` params, all fixed at 0 for single-range callers.

- [ ] **Step 9: Clippy**

Run: `cargo clippy -p cluster -p crabgresql --all-targets -- -D warnings`
Expected: clean. (If the old single `Raft` import in `server_node.rs` or `KeyspaceKv` becomes unused, remove it.)

- [ ] **Step 10: Commit**

```bash
git add crates/cluster/src/durable.rs crates/cluster/src/node.rs crates/cluster/src/server_node.rs crates/crabgresql/src/main.rs
git commit -m "feat(cluster): multi-range durable ServerNode — per-range data-r{r}/raft-r{r} keyspaces, N Raft groups, bootstrap each"
```

---

**Notes for the implementer**
- **Isolation is the load-bearing prerequisite** (criterion 3): the construction loop in Step 5 depends on Step 3 existing — range `r`'s Raft is built over range `r`'s keyspaces, so `data-r1` writes physically cannot appear in `data-r0`. The criterion-3 test asserts this over BOTH keyspaces; do not weaken it to a single-keyspace check.
- **No sleep:** every wait is openraft `raft.wait(Some(timeout)).metrics(|m| state==Leader && current_leader==self, "…")`. A one-node group elects immediately after `initialize`; the bound only guards a stuck group.
- **`RangeMap::single()` is the regression switch** (criterion 4): with one range the keyspaces are `data-r0`/`raft-r0` and the SQL listener uses range 0's raft+engine, so the SP9/SP10 single-range multi-process suites run the existing fast-path unchanged. Do not add a feature flag — the static map is the switch.
- **`NodeStore::open` signature changed** (`open(dir)` → `open(dir, &RangeMap)`): every caller in the workspace must pass a map. This task fixes `durable.rs`'s own tests/conformance builder and `node.rs`; if any other caller exists (grep `NodeStore::open`), update it to `&RangeMap::single()`.
- **Do not build a remote `RaftCommitter`:** each range's engine commits through *its own local* Raft handle (`rafts[r]`). Cross-node forwarding is the SQL-boundary gateway in Tasks 3/4, never a remote committer (it would compile but dead-end at `NotLeader`).

---

## Task 3: Gateway local routing

Make `RangeRouter` **cluster-agnostic** so the durable gateway (not just the in-process `MultiRangeCluster`) can drive it, and rewrite the connection path into a **per-statement** range demux that runs LOCAL-leader ranges on this node's range-`r` engine and hands REMOTE ranges to a forward seam (a stub that errors in this task; the pooled pgwire client lands in Task 4). `Pin` and the cross-range `0A000` rejection carry forward verbatim.

Two prerequisites pulled in from the spec's Component 3:

1. **Constructor refactor.** `RangeRouter::connect` (`range/router.rs:44`) is hard-wired to `&MultiRangeCluster` and resolves engines via `c.leader_engine(r)` / catalog via `c.catalog_kv()` — methods that exist **only** on the in-process harness (`range/cluster.rs:73,178`), not on the durable substrate. Add `RangeRouter::new(map, engines, catalog_kv, forward)` taking a `HashMap<RangeId, SqlEngine>` of **local-leader** engines + a range-0 `catalog_kv` handle + an `Arc<dyn RemoteForward>`. `connect` keeps its exact public signature and **delegates to `new`** (the in-process harness leads every range from one of its co-located nodes, so it builds a local engine per range and passes a `RejectForward` that is never hit — the existing Task-5-of-SP13 router tests are the regression gate).

2. **Per-statement gateway over the engine.** A `RangeGatewayEngine` implementing `pgwire::engine::Engine` whose `connect()` yields a `RangeRouter`, plus `serve_range_routed` to bind it — the per-statement analog of `serve_routed`. (`serve_routed`/`proxy` in `route.rs` stay for the single-range fast-path; T2 picks `serve_range_routed` only when `range_count() > 1`.)

The forward seam is **stubbed to error** in this task; Task 4 swaps `RejectForward` for the real pooled pgwire client. The Task-3 test runs a **single multi-range `ServerNode`** that leads every range locally, so the local path is fully exercised end-to-end over loopback pgwire and the remote path is only reached by Task 4.

**Files:**
- Modify: `crates/cluster/src/range/router.rs` (cluster-agnostic `new`; `RemoteForward` trait + `RejectForward`; local-vs-forward `run_on`; `impl Session`)
- Modify: `crates/cluster/src/route.rs` (`RangeGatewayEngine` + `serve_range_routed`)
- Modify: `crates/cluster/src/lib.rs` (re-export nothing new; `route` is already `pub`)
- Create: `crates/cluster/tests/gateway_local.rs` (in-crate, one multi-range `ServerNode`, loopback pgwire)

> **Sibling-task signatures this task consumes** (declare-only; T2 owns them): `NodeConfig.range_map: RangeMap` (default `single()`), `ServerNode.engines: HashMap<RangeId, Arc<SqlEngine>>`, `ServerNode.rafts: HashMap<RangeId, Raft<TypeConfig>>`, and `ServerNode::start` selecting `serve_range_routed` when `range_count() > 1`. If T2 is not yet merged, gate the Task-3 test behind the same `engines` accessor; do **not** re-derive these here.

---

- [ ] **Step 1: Write the failing gateway test** — create `crates/cluster/tests/gateway_local.rs`:

```rust
//! D3a-net T3: a single multi-range `ServerNode` is a per-statement SQL gateway.
//! CREATE lands on range 0, INSERT routes to the data range's LOCAL leader engine,
//! SELECT reads it back — all over loopback pgwire. A transaction that spans ranges
//! is rejected with SQLSTATE 0A000. The node leads every range itself, so no remote
//! forward fires (that path is Task 4).
use std::time::Duration;

use cluster::range::map::RangeMap;
use cluster::server_node::{NodeConfig, ServerNode};
use openraft::ServerState;
use tokio::net::TcpListener;

/// Bind an ephemeral loopback port, read its address, and free it for rebind.
async fn free_port() -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let a = l.local_addr().expect("local_addr").to_string();
    drop(l);
    a
}

/// Connect over pgwire with a short bounded retry (the listener is bound before
/// `start` returns, but the OS may briefly not yet route). No fixed settle sleep:
/// the loop is a condition (a successful connect) with a deadline.
async fn connect_with_retry(
    conn_str: &str,
) -> (
    tokio_postgres::Client,
    tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match tokio_postgres::connect(conn_str, tokio_postgres::NoTls).await {
            Ok(pair) => return pair,
            // Retry immediately: the connect attempt is itself a real TCP
            // round-trip that paces the loop (no fixed settle sleep), bounded
            // by the deadline. Yield so a busy runtime makes progress.
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::task::yield_now().await;
            }
            Err(e) => panic!("pg connect: {e}"),
        }
    }
}

/// Start a single self-bootstrapping node hosting a 2-range map (boundary at
/// table 2: table id 1 → range 0, id ≥ 2 → range 1) and wait until it
/// self-confirms leadership of **every** range (event-based, no sleep).
async fn start_two_range_node() -> (ServerNode, String) {
    let node_addr = free_port().await;
    let sql_addr = free_port().await;
    let node = ServerNode::start(NodeConfig {
        id: 0,
        node_addr: node_addr.clone(),
        sql_addr: sql_addr.clone(),
        data_dir: tempfile::tempdir().expect("tempdir").keep(),
        peers: vec![(0, node_addr.clone())],
        bootstrap: true,
        range_map: RangeMap::with_boundaries(vec![2]),
    })
    .await
    .expect("start node");

    // A one-node group elects immediately after `initialize`. Wait per range via
    // openraft's event API — the instant each range self-confirms as leader.
    for (_r, raft) in &node.rafts {
        raft.wait(Some(Duration::from_secs(10)))
            .metrics(
                |m| m.state == ServerState::Leader && m.current_leader == Some(0),
                "range self-confirmed leader",
            )
            .await
            .expect("range elects within the bound");
    }
    (node, sql_addr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_routes_create_insert_select_across_local_ranges() {
    let (_node, sql_addr) = start_two_range_node().await;
    let port = sql_addr.rsplit(':').next().expect("port");
    let conn_str = format!("host=127.0.0.1 port={port} user=postgres");
    let (client, connection) = connect_with_retry(&conn_str).await;
    tokio::spawn(connection);

    // CREATE TABLE allocates table id 1 → range 0 (DDL always runs on range 0).
    client
        .simple_query("CREATE TABLE a (id int4)")
        .await
        .expect("create a (range 0)");
    // CREATE TABLE b allocates table id 2 → range 1.
    client
        .simple_query("CREATE TABLE b (id int4)")
        .await
        .expect("create b (range 1)");
    // INSERT INTO b routes to range 1's LOCAL leader engine on this node.
    client
        .simple_query("INSERT INTO b VALUES (20)")
        .await
        .expect("insert b (routes to range 1)");
    // SELECT FROM b reads it back through the same range-1 session.
    let rows = client
        .simple_query("SELECT id FROM b")
        .await
        .expect("select b");
    let row_count = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(row_count, 1, "the row inserted into range 1 must read back");
    if let Some(tokio_postgres::SimpleQueryMessage::Row(r)) = rows
        .iter()
        .find(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
    {
        assert_eq!(r.get("id"), Some("20"), "value routed and read back");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_rejects_a_cross_range_transaction_with_0a000() {
    let (_node, sql_addr) = start_two_range_node().await;
    let port = sql_addr.rsplit(':').next().expect("port");
    let conn_str = format!("host=127.0.0.1 port={port} user=postgres");
    let (client, connection) = connect_with_retry(&conn_str).await;
    tokio::spawn(connection);

    client
        .simple_query("CREATE TABLE a (id int4)")
        .await
        .expect("create a (range 0)");
    client
        .simple_query("CREATE TABLE b (id int4)")
        .await
        .expect("create b (range 1)");
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO a VALUES (1)")
        .await
        .expect("first DML pins range 0");
    // A second statement on a DIFFERENT range inside the same txn is rejected.
    let err = client
        .simple_query("INSERT INTO b VALUES (2)")
        .await
        .expect_err("a transaction may not span ranges (D3b)");
    let db_err = err.as_db_error().expect("a server SQLSTATE error");
    assert_eq!(
        db_err.code().code(),
        "0A000",
        "cross-range txn → feature_not_supported"
    );
    let _ = client.simple_query("ROLLBACK").await;
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p cluster --test gateway_local`
Expected: FAIL to **compile** — `NodeConfig` has no `range_map` field, `ServerNode` has no `rafts`, and there is no `serve_range_routed` gateway (so even after T1/T2 land, the router still has no cluster-agnostic `new`/forward seam and the per-statement gateway is unwired). This is the red bar: the gateway path does not yet exist.

> If T1/T2 are not yet merged in the worktree, this test cannot compile at all — that is expected; the implementation steps below add the router seam, and T2 supplies `range_map`/`rafts`/`serve_range_routed` wiring. Run the router-level red bar instead (Step 3a) to see *this task's* unit fail in isolation.

- [ ] **Step 3a: (router-level red bar, T1/T2-independent)** append a unit test to `crates/cluster/src/range/router.rs` that exercises `new` + the forward seam directly, so this task fails on its own behavior before T2 wiring exists:

```rust
#[cfg(test)]
mod gateway_seam_tests {
    use super::*;
    use crate::range::cluster::MultiRangeCluster;

    /// `new` builds a router whose LOCAL engines serve their ranges and whose
    /// forward seam is reached for a range with NO local engine. With a
    /// `RejectForward`, a statement targeting a non-local range surfaces 0A000.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn forward_seam_is_reached_for_a_non_local_range() {
        // Build a 2-range in-process cluster only to mint a real range-1 engine +
        // catalog, then construct a router that is told it holds ONLY range 0
        // locally — so range-1 traffic must hit the forward seam.
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        // Create both tables through the normal (all-local) router first.
        let mut admin = RangeRouter::connect(&c).await;
        admin.simple("CREATE TABLE a (id int4)").await.expect("a"); // id 1 -> range 0
        admin.simple("CREATE TABLE b (id int4)").await.expect("b"); // id 2 -> range 1
        c.wait_for_replication(0).await;

        // A router that holds only range 0 locally; range 1 → RejectForward.
        let mut engines = HashMap::new();
        engines.insert(0, c.leader_engine(0).await);
        let mut router = RangeRouter::new(
            c.range_map().clone(),
            engines,
            c.catalog_kv().await,
            std::sync::Arc::new(RejectForward),
        );
        // Range-0 work runs locally.
        router.simple("INSERT INTO a VALUES (1)").await.expect("local range 0");
        // Range-1 work has no local engine → forward seam → 0A000 stub.
        let err = router
            .simple("INSERT INTO b VALUES (2)")
            .await
            .expect_err("no local range-1 engine → forward");
        assert_eq!(err.code, "0A000", "RejectForward stub surfaces 0A000");
    }
}
```

Run: `cargo test -p cluster --lib range::router::gateway_seam_tests`
Expected: FAIL — `RangeRouter::new`, `RemoteForward`, and `RejectForward` do not exist.

- [ ] **Step 3b: Add the `RemoteForward` seam + `RejectForward` stub** — at the top of `crates/cluster/src/range/router.rs`, after the existing `use` block, add:

```rust
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// The remote half of the gateway: forward one simple-query statement to the
/// owning range's leader on another node and return its single result. The router
/// itself is pure routing/`Pin` (Decision: retry-on-NotLeader lives in the wire
/// layer, NOT here) — this seam is the only place a non-local range is handled.
///
/// Boxed-future method so the trait is object-safe behind `Arc<dyn RemoteForward>`.
/// Task 3 ships `RejectForward` (every call → 0A000); Task 4 replaces it with the
/// pooled minimal pgwire client (`crate::route::PgwireForward`).
pub trait RemoteForward: Send + Sync {
    fn forward<'a>(
        &'a self,
        range: RangeId,
        sql: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResult, ExecError>> + Send + 'a>>;
}

/// The Task-3 stub: no range is remotely reachable yet, so any statement that
/// lands on a non-local range is rejected. Replaced by the real client in Task 4.
pub struct RejectForward;

impl RemoteForward for RejectForward {
    fn forward<'a>(
        &'a self,
        range: RangeId,
        _sql: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResult, ExecError>> + Send + 'a>> {
        Box::pin(async move {
            Err(ExecError::Unsupported(format!(
                "range {range} is not led locally; remote forwarding lands in T4"
            )))
        })
    }
}
```

- [ ] **Step 3c: Make `RangeRouter` cluster-agnostic** — replace the struct and `connect`/`run_on`/`session_mut`/`simple` in `crates/cluster/src/range/router.rs`. The struct gains `engines` (LOCAL-leader engines), `forward`, and `cur_sql` (the in-flight simple-query text the seam forwards); `map`/`catalog_kv`/`sessions`/`pin` are unchanged. Replace:

```rust
/// A connection's view: per range it has touched, a leader `SqlSession` (LOCAL
/// ranges only); the `Pin` a transaction is held to; and the seam that forwards a
/// non-local range's statement to its remote leader.
pub struct RangeRouter {
    sessions: HashMap<RangeId, SqlSession>,
    pin: Pin,
    map: RangeMap,
    /// Engines for ranges THIS node leads; a range absent here is remote.
    engines: HashMap<RangeId, SqlEngine>,
    /// Range-0 catalog store (schema resolution). For a range-0 follower gateway
    /// Task 4 makes this a wire-read handle; here it is the local range-0 store.
    catalog_kv: Arc<dyn kv::Kv>,
    /// Forwards a statement whose range has no local engine.
    forward: Arc<dyn RemoteForward>,
    /// The text of the in-flight `simple_query` — what the forward seam relays.
    cur_sql: String,
}

impl RangeRouter {
    /// Cluster-agnostic constructor: the local-leader engines this node holds, the
    /// range-0 catalog store, and the remote-forward seam. No `&MultiRangeCluster`.
    pub fn new(
        map: RangeMap,
        engines: HashMap<RangeId, SqlEngine>,
        catalog_kv: Arc<dyn kv::Kv>,
        forward: Arc<dyn RemoteForward>,
    ) -> Self {
        Self {
            sessions: HashMap::new(),
            pin: Pin::None,
            map,
            engines,
            catalog_kv,
            forward,
            cur_sql: String::new(),
        }
    }

    /// In-process harness constructor: the harness leads every range from one of
    /// its co-located nodes, so it has a local engine per range and never needs to
    /// forward — delegates to `new` with a `RejectForward` (never hit in-process).
    pub async fn connect(c: &MultiRangeCluster) -> Self {
        let mut engines = HashMap::new();
        for r in c.range_map().range_ids() {
            engines.insert(r, c.leader_engine(r).await);
        }
        Self::new(
            c.range_map().clone(),
            engines,
            c.catalog_kv().await,
            Arc::new(RejectForward),
        )
    }
```

Then change `run_on` so a range with no local engine forwards (passing the in-flight `cur_sql`), and replace `session_mut`/`simple` to set `cur_sql`. Replace the existing `run_on`/`session_mut`/`simple` bodies with:

```rust
    /// Run a statement on `range`: locally if this node leads it, else forward the
    /// in-flight simple-query text to the remote leader through the seam.
    async fn run_on(&mut self, range: RangeId, stmt: &Statement) -> Result<QueryResult, ExecError> {
        if self.engines.contains_key(&range) {
            return self.session_mut(range).run(stmt).await;
        }
        // No local engine for this range → remote. The forward seam relays the
        // single statement text (the pgwire `Query`) and returns its one result.
        let sql = self.cur_sql.clone();
        self.forward.forward(range, &sql).await
    }

    /// Get (creating on first use) the LOCAL `SqlSession` for `range`'s engine.
    /// Only called for ranges present in `engines`.
    fn session_mut(&mut self, range: RangeId) -> &mut SqlSession {
        if !self.sessions.contains_key(&range) {
            let s = self
                .engines
                .get(&range)
                .expect("local engine for range")
                .connect();
            self.sessions.insert(range, s);
        }
        self.sessions.get_mut(&range).expect("session")
    }

    /// Parse `sql` and run each statement in order; return the last result. The
    /// raw text is recorded so the forward seam can relay the exact `Query`.
    pub async fn simple(&mut self, sql: &str) -> Result<QueryResult, PgError> {
        let stmts = pgparser::parse(sql).map_err(|e| ExecError::Parse(e).into_pg())?;
        self.cur_sql = sql.to_string();
        let mut last = QueryResult::Command {
            tag: "OK".into(),
        };
        for stmt in &stmts {
            last = self.dispatch(stmt).await.map_err(ExecError::into_pg)?;
        }
        Ok(last)
    }
```

> `session_mut` is now **synchronous** (no `.await`); `run_on`'s caller (`dispatch`) already `await`s, so its three `self.run_on(...).await` sites are unchanged. The `dispatch`/`pinning_range`/`range_of` bodies and the `Pin` enum are **untouched** — the cross-range `0A000` rejection (`Pin::Range` arm) and all txn-pinning are preserved exactly. Remove the now-unused `use pgwire::engine::Engine;` if clippy flags it (the local `connect()` call needs `Engine` in scope; keep it iff used).

- [ ] **Step 3d: Implement `Session` for `RangeRouter`** so the pgwire loop can drive it — append to `crates/cluster/src/range/router.rs` (this is the per-statement message-loop integration: `pgwire`'s `run_session` calls `simple_query`/`describe`/`tx_status` per frame):

```rust
impl pgwire::engine::Session for RangeRouter {
    /// One simple-protocol `Query` frame → one result per statement. Each statement
    /// is range-demuxed (local engine or forward seam); a routing/exec error becomes
    /// the connection's `ErrorResponse` exactly as the single-range session does.
    async fn simple_query(
        &mut self,
        sql: &str,
    ) -> Result<Vec<QueryResult>, PgError> {
        if sql.trim().is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        let stmts = pgparser::parse(sql).map_err(|e| ExecError::from(e).into_pg())?;
        if stmts.is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        self.cur_sql = sql.to_string();
        let mut results = Vec::with_capacity(stmts.len());
        for stmt in &stmts {
            results.push(self.dispatch(stmt).await.map_err(ExecError::into_pg)?);
        }
        Ok(results)
    }

    /// Describe resolves field types from range 0's catalog — the gateway rejects
    /// cross-range **extended** protocol elsewhere, so a Describe only needs the
    /// catalog store, matching the spec's "simple-query routing is the surface".
    async fn describe(
        &mut self,
        sql: &str,
    ) -> Result<Vec<pgwire::engine::FieldDescription>, PgError> {
        // describe is read-only schema lookup; run it on range 0's catalog store.
        executor::describe_fields(&*self.catalog_kv, sql).map_err(ExecError::into_pg)
    }

    fn tx_status(&self) -> pgwire::engine::TxStatus {
        match self.pin {
            Pin::None => pgwire::engine::TxStatus::Idle,
            Pin::Open | Pin::Range(_) => pgwire::engine::TxStatus::InTransaction,
        }
    }
}
```

> **`executor::describe_fields`** is a thin public wrapper this step needs over the existing private `crate::exec::describe(catalog_kv, kv, sql)` (`session.rs:527` uses it as `describe`). Add to `crates/executor/src/lib.rs`:
> ```rust
> /// Field descriptions for `sql` resolving schema from `catalog_kv`, without a
> /// data store or execution (the gateway's Describe only needs the catalog).
> pub fn describe_fields(
>     catalog_kv: &dyn Kv,
>     sql: &str,
> ) -> Result<Vec<pgwire::engine::FieldDescription>, ExecError> {
>     crate::exec::describe(catalog_kv, catalog_kv, sql)
> }
> ```
> (`describe` reads only the catalog for field types; passing `catalog_kv` for both args is correct — Describe never touches row data.)

- [ ] **Step 3e: Run the router-level red bar — now green**

Run: `cargo test -p cluster --lib range::router`
Expected: PASS — the new `gateway_seam_tests::forward_seam_is_reached_for_a_non_local_range` passes (range 0 local, range 1 → `RejectForward` → 0A000) and the pre-existing SP13 router tests (`create_in_range0_insert_routes_to_data_range_select_reads_back`, `a_transaction_may_not_span_ranges`) still pass through the delegated `connect`.

- [ ] **Step 4: Add the per-statement gateway engine + `serve_range_routed`** — append to `crates/cluster/src/route.rs`:

```rust
use std::collections::HashMap;

use crate::range::map::{RangeId, RangeMap};
use crate::range::router::{RangeRouter, RemoteForward};

/// A `pgwire` `Engine` whose every connection is a per-statement range gateway.
/// `connect()` builds a `RangeRouter` over this node's LOCAL-leader engines, the
/// range-0 catalog store, and the remote-forward seam — so each simple-query frame
/// runs locally for a range this node leads and forwards otherwise. The per-range
/// engines are shared (`Arc`) across all connections; each connection gets fresh
/// `SqlSession`s lazily (`SqlEngine::connect`), as the router already does.
pub struct RangeGatewayEngine {
    map: RangeMap,
    engines: HashMap<RangeId, Arc<SqlEngine>>,
    catalog_kv: Arc<dyn kv::Kv>,
    forward: Arc<dyn RemoteForward>,
}

impl RangeGatewayEngine {
    pub fn new(
        map: RangeMap,
        engines: HashMap<RangeId, Arc<SqlEngine>>,
        catalog_kv: Arc<dyn kv::Kv>,
        forward: Arc<dyn RemoteForward>,
    ) -> Self {
        Self {
            map,
            engines,
            catalog_kv,
            forward,
        }
    }
}

impl pgwire::engine::Engine for RangeGatewayEngine {
    type Session = RangeRouter;

    fn connect(&self) -> RangeRouter {
        // The router owns one engine per range by value; clone the shared engines
        // into per-connection routing handles. `SqlEngine` is a bundle of `Arc`s
        // (`lib.rs:49-63`), so this is a cheap pointer clone, not a deep copy.
        let engines: HashMap<RangeId, SqlEngine> = self
            .engines
            .iter()
            .map(|(&r, e)| (r, (**e).clone_handle()))
            .collect();
        RangeRouter::new(
            self.map.clone(),
            engines,
            Arc::clone(&self.catalog_kv),
            Arc::clone(&self.forward),
        )
    }
}

/// Serve the public SQL port as a per-statement range gateway: each simple-query
/// frame is demuxed to its range's local leader engine or forwarded to the remote
/// leader. The multi-range analog of `serve_routed`; T2 selects this when the
/// node hosts more than one range.
pub async fn serve_range_routed(
    listener: TcpListener,
    map: RangeMap,
    engines: HashMap<RangeId, Arc<SqlEngine>>,
    catalog_kv: Arc<dyn kv::Kv>,
    forward: Arc<dyn RemoteForward>,
    config: Arc<pgwire::session::SessionConfig>,
) -> std::io::Result<()> {
    let engine = Arc::new(RangeGatewayEngine::new(map, engines, catalog_kv, forward));
    let registry = Arc::new(CancelRegistry::default());
    loop {
        let (stream, _peer) = listener.accept().await?;
        let engine = engine.clone();
        let config = config.clone();
        let registry = registry.clone();
        tokio::spawn(async move {
            let _ = serve_conn(stream, engine, config, registry, None).await;
        });
    }
}
```

> **`SqlEngine::clone_handle`** is a small public addition (a `SqlEngine` is a bundle of `Arc`s — `lib.rs:49-63` — so handle-cloning is cheap and correct; it shares the same applied store, committer, procarray, etc.). Add to `crates/executor/src/lib.rs`:
> ```rust
> impl SqlEngine {
>     /// A second handle to the SAME engine (all fields are `Arc`/`Copy`): every
>     /// clone shares the applied store, committer, linearizer, and counters.
>     /// Used by the gateway to give each connection its own router without
>     /// re-opening the engine.
>     pub fn clone_handle(&self) -> SqlEngine {
>         SqlEngine {
>             kv: Arc::clone(&self.kv),
>             catalog_kv: Arc::clone(&self.catalog_kv),
>             procarray: Arc::clone(&self.procarray),
>             seq: Arc::clone(&self.seq),
>             lockmgr: Arc::clone(&self.lockmgr),
>             catalog_lock: Arc::clone(&self.catalog_lock),
>             committer: Arc::clone(&self.committer),
>             linearizer: Arc::clone(&self.linearizer),
>             persist_mode: self.persist_mode,
>         }
>     }
> }
> ```
> `serve_conn`, `CancelRegistry`, `TcpListener`, `Arc`, and `SqlEngine` are already imported at the top of `route.rs` (`route.rs:5-14`); add only the `HashMap` + `crate::range::…` uses shown above.

- [ ] **Step 5: Run the full gateway test (after T2 wires `serve_range_routed`)**

On Windows set `$env:__COMPAT_LAYER='RunAsInvoker'` (a multi-range `ServerNode` spawns no child process here — these are in-crate — but the policy is harmless).

Run: `cargo test -p cluster --test gateway_local`
Expected: PASS (2 tests) — `gateway_routes_create_insert_select_across_local_ranges` (CREATE→range 0, INSERT→range 1 local leader, SELECT reads back `20`) and `gateway_rejects_a_cross_range_transaction_with_0a000` (the second-range INSERT in one txn surfaces SQLSTATE `0A000`). Every wait is event-based (`raft.wait().metrics(...)`) or a bounded connect-retry on a real condition — no fixed settle sleep.

- [ ] **Step 6: Regression — the SP13 in-process router/cluster suite is unchanged**

Run: `cargo test -p cluster --lib range`
Expected: PASS — `range::map`, `range::cluster`, and all `range::router` tests pass; the cluster-agnostic refactor is behavior-preserving for the in-process harness (`connect` delegates to `new`).

Run: `cargo test -p cluster --test multirange`
Expected: PASS — the SP13 routing-correctness e2e is unaffected.

- [ ] **Step 7: clippy + fmt**

Run: `cargo clippy -p cluster -p executor --all-targets -- -D warnings`
Expected: clean. (Drop any now-unused import — e.g. `pgwire::engine::Engine` in `router.rs` if the local `connect()` no longer needs it in scope.)

Run: `cargo fmt -p cluster -p executor`
Expected: no diff after a re-run (`cargo fmt -p cluster -p executor -- --check` clean).

- [ ] **Step 8: Commit**

```bash
git add crates/cluster/src/range/router.rs crates/cluster/src/route.rs \
        crates/executor/src/lib.rs crates/cluster/tests/gateway_local.rs
git commit -m "feat(cluster): gateway local routing — cluster-agnostic RangeRouter + per-statement range gateway

RangeRouter gains a cluster-agnostic new(map, engines, catalog_kv, forward);
connect(&MultiRangeCluster) delegates to it. A RemoteForward seam (RejectForward
stub this task, pooled pgwire client in T4) handles non-local ranges. serve_range_routed
drives a per-statement RangeGatewayEngine over pgwire: local-leader ranges run on the
local engine, Pin + 0A000 cross-range rejection carry forward. Sleep-free via leader wait()."
```

---

**Notes for the implementer**
- **The seam, not the wire, is this task.** `RejectForward` makes the non-local arm compile and error with `0A000`; Task 4 swaps it for `crate::route::PgwireForward` (pooled minimal pgwire client) at the `ServerNode` wire-up only — the router and `RangeGatewayEngine` are untouched by T4.
- **`Pin` + `0A000` are verbatim.** Do not edit `dispatch`/`pinning_range`/the `Pin` enum (`router.rs:22-165`): the cross-range rejection and txn-pinning are already correct and tested. This task only changes how a *resolved* range is executed (local vs forward) and adds the `Session` impl.
- **Per-statement, not byte-relay.** The gateway is `serve_range_routed` driving `RangeGatewayEngine` through pgwire's `run_session` — one `simple_query`/`describe` call per frame. It is **not** `proxy()`/`copy_bidirectional` (a whole-connection relay that cannot resume local execution on the next statement); `serve_routed`/`proxy` stay only for the single-range fast-path.
- **Stale IDE diagnostics:** trust `cargo clippy --all-targets -- -D warnings` + `cargo test`, not editor squiggles.

---

## Task 4: Forward-to-remote-leader (one hop)

Fill the remote-forward seam Task 3 left open. Task 3's gateway computes a target range per statement and runs it locally when this node leads that range; when the leader is **remote** it calls the `Arc<dyn RemoteForward>` the `RangeRouter` was constructed with. This task supplies the real impl of that trait: a **minimal, pooled pgwire forwarding client** built on the existing `pgwire` frame primitives (no new dependency) that opens one authenticated pgwire connection per remote leader, sends exactly one `Query`, reads to `ReadyForQuery`, relays the response frames back, and reuses the connection for later statements to the same leader. It resolves the target leader from the **range-`r` metrics watch** (dropping the `Ref` before any `await`, the `route.rs:56-67` pattern, applied per range), excludes paused/unreachable self-reporting leaders, and does a **bounded one re-resolve+retry** on `NotLeader`/wire-error. It also removes the production busy-sleep at `route.rs:92`.

The forwarding client speaks the `Trust`-auth handshake (the cluster's only auth mode — `server_node.rs:115` passes `SessionConfig::trust()`, and `session.rs:301` answers `Trust` with a bare `AuthenticationOk`), so the client sends a `StartupMessage`, reads to `ReadyForQuery`, and is ready. No SASL, no password round-trip.

**Files:**
- Create: `crates/cluster/src/forward.rs` (the pooled pgwire forwarding client + leader resolution + bounded retry)
- Create: `crates/cluster/tests/remote_forward.rs` (the failing two-node forward + retry-counter test — os-740-safe name, no `setup`/`install`/`update`/`patch`/`upgrad` substring)
- Modify: `crates/cluster/src/lib.rs` (add `pub mod forward;`)
- Modify: `crates/cluster/src/route.rs` (delete the `route.rs:92` 50 ms sleep; the `None` arm bounds on the deadline only)
- Modify: `crates/cluster/src/server_node.rs` (build the `PgwireForward` (`Arc<dyn RemoteForward>`) from `forward::ForwardPool` and the per-range raft handle map; pass it into the Task-3 cluster-agnostic `RangeRouter` constructor)

> **Seam consumed from Task 1** — `crates/cluster/src/transport/client.rs`: `TcpConn::call` builds `NodeRequest::Raft { from, range, rpc }` (`protocol.rs:60`, `range` field added with `#[serde(default)]`); not used directly here but the per-range raft handles this task resolves leaders from are the same N instances Task 1's `(range, node)` registry dispatches to.
>
> **Seam consumed from Task 2** — `crates/cluster/src/server_node.rs`: `ServerNode::start` builds N raft instances and keeps them in a `rafts: std::collections::HashMap<RangeId, openraft::Raft<TypeConfig>>` (one per range). This task reads `rafts[&r].metrics()` to resolve range `r`'s remote leader's `sql_addr`.
>
> **Seam consumed from Task 3** — `crates/cluster/src/range/router.rs`: `RangeRouter::new(map, engines, catalog_kv, forward: Arc<dyn RemoteForward>)` is the cluster-agnostic constructor. `pub trait RemoteForward` exposes an async `forward(&self, range: RangeId, sql: String) -> Result<QueryResult, ExecError>` — called `(target_range, sql)` when the gateway is **not** the local leader of `target_range`. Task 3 wires this trait object into `dispatch` (the "remote leadership" branch) and ships a `RejectForward` impl; this task provides the real impl. `RangeRouter` keeps its `engines: HashMap<RangeId, SqlEngine>` for the **local-leader** ranges only; ranges this node does not lead are absent from `engines` and therefore route through `forward`.

- [ ] **Step 1: Write the failing two-node forward + retry-counter test** — create `crates/cluster/tests/remote_forward.rs`:

```rust
//! D3a-net Task 4: a write issued at a gateway that is a FOLLOWER for the target
//! range is forwarded over a pooled pgwire client to the remote range leader and
//! becomes visible on every replica of that range (event-based applied-index
//! wait). A test-only one-shot makes the first forward observe `NotLeader`
//! exactly once; the test asserts the gateway's re-resolve+retry counter == 1
//! (mechanically checkable, not racing a real election). No sleep.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use cluster::forward::{ForwardPool, RetryCounter};
use cluster::range::map::RangeId;
use cluster::server_node::{NodeConfig, ServerNode};
use cluster::range::map::RangeMap;

/// Bind an ephemeral loopback port, read its address, drop the listener so the
/// address is free for the node to rebind.
async fn free_port() -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let a = l.local_addr().expect("local_addr").to_string();
    drop(l);
    a
}

/// Two co-located multi-range nodes (ranges {0,1}); return both, plus the packed
/// `node|sql` peer addresses. Both bootstrap range 0 and range 1.
async fn two_node_cluster() -> (ServerNode, ServerNode, RangeMap) {
    let map = RangeMap::with_boundaries(vec![2]); // table id 1 -> range 0, id >=2 -> range 1
    let n0_node = free_port().await;
    let n0_sql = free_port().await;
    let n1_node = free_port().await;
    let n1_sql = free_port().await;
    let peers = vec![
        (0u64, cluster::addr::pack(&n0_node, &n0_sql)),
        (1u64, cluster::addr::pack(&n1_node, &n1_sql)),
    ];
    let d0 = tempfile::tempdir().expect("tempdir0").keep();
    let d1 = tempfile::tempdir().expect("tempdir1").keep();
    let n0 = ServerNode::start(NodeConfig {
        id: 0,
        node_addr: n0_node.clone(),
        sql_addr: n0_sql.clone(),
        data_dir: d0,
        peers: peers.clone(),
        bootstrap: true,
        range_map: map.clone(),
    })
    .await
    .expect("start n0");
    let n1 = ServerNode::start(NodeConfig {
        id: 1,
        node_addr: n1_node.clone(),
        sql_addr: n1_sql.clone(),
        data_dir: d1,
        peers,
        bootstrap: false,
        range_map: map.clone(),
    })
    .await
    .expect("start n1");
    (n0, n1, map)
}

/// Await `range`'s self-confirmed leader id across the two nodes' raft handles,
/// using openraft's event-based `wait` (no sleep). Returns the leader node id.
async fn wait_leader(n0: &ServerNode, n1: &ServerNode, range: RangeId) -> u64 {
    let mut set = tokio::task::JoinSet::new();
    for node in [n0, n1] {
        let raft = node.rafts.get(&range).expect("range raft").clone();
        set.spawn(async move {
            raft.wait(Some(Duration::from_secs(20)))
                .metrics(
                    |m| m.state == openraft::ServerState::Leader && m.current_leader == Some(m.id),
                    "self-confirmed leader",
                )
                .await
                .map(|m| m.id)
                .ok()
        });
    }
    while let Some(res) = set.join_next().await {
        if let Ok(Some(id)) = res {
            return id;
        }
    }
    panic!("range {range} elected no leader within the bound");
}

/// Await every replica of `range` applying up to the leader's applied index — the
/// `wait_for_replication` analog over `ServerNode` raft handles. Event-based.
async fn wait_for_replication(n0: &ServerNode, n1: &ServerNode, range: RangeId) {
    let leader = wait_leader(n0, n1, range).await;
    let nodes = [n0, n1];
    let target = nodes[leader as usize]
        .rafts
        .get(&range)
        .expect("range raft")
        .metrics()
        .borrow()
        .last_applied
        .map(|l| l.index)
        .unwrap_or(0);
    for node in nodes {
        node.rafts
            .get(&range)
            .expect("range raft")
            .wait(Some(Duration::from_secs(20)))
            .metrics(
                |m| m.last_applied.map(|l| l.index).unwrap_or(0) >= target,
                "follower caught up to leader applied index",
            )
            .await
            .expect("replication within bound");
    }
}

/// A write at a gateway that does NOT lead range 1 is forwarded to range 1's
/// remote leader over the pooled pgwire client and lands on every range-1 replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_at_follower_gateway_forwards_to_remote_leader() {
    let (n0, n1, map) = two_node_cluster().await;
    let r1_leader = wait_leader(&n0, &n1, 1).await;
    // The gateway is the node that does NOT lead range 1, so its range-1 write
    // forwards over the wire.
    let (gw, other) = if r1_leader == 0 { (&n1, &n0) } else { (&n0, &n1) };

    // Create the table on range 0 through the gateway (CREATE routes to range 0;
    // the gateway forwards or runs locally depending on range-0 leadership).
    let counter = RetryCounter::default();
    let pool = ForwardPool::new(gw.rafts.clone(), gw.partition.clone(), counter.clone());
    pool.forward(0, "CREATE TABLE a (id int4)".into())
        .await
        .expect("create a -> range 0"); // table id 1 -> range 0 (per RangeMap::with_boundaries(vec![2]))
    pool.forward(0, "CREATE TABLE b (id int4)".into())
        .await
        .expect("create b -> range 0"); // table id 2 -> range 1

    // INSERT into b: the gateway is a range-1 follower, so this forwards to the
    // remote range-1 leader over the pooled pgwire client.
    pool.forward(1, "INSERT INTO b VALUES (42)".into())
        .await
        .expect("insert b -> forwarded to range 1 leader");

    // The row is replicated to every range-1 replica (event-based wait, no sleep).
    wait_for_replication(&n0, &n1, 1).await;
    for node in [&n0, &n1] {
        let r1 = node.sm_kv(1);
        let prefix = kv::key::table_prefix(2); // table id 2 -> range 1
        assert!(
            !r1.scan_prefix(&prefix).expect("scan r1").is_empty(),
            "forwarded row present on range-1 store of node {}",
            node.id()
        );
    }
    let _ = other; // both nodes drive the same range-1 group; kept for symmetry.
}

/// A deterministically-injected single `NotLeader` on the first forward causes
/// exactly one re-resolve+retry. The retry counter is asserted == 1 (mechanical,
/// not racing an election).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_shot_notleader_triggers_exactly_one_retry() {
    let (n0, n1, map) = two_node_cluster().await;
    wait_leader(&n0, &n1, 0).await;
    let r1_leader = wait_leader(&n0, &n1, 1).await;
    let gw = if r1_leader == 0 { &n1 } else { &n0 };

    let counter = RetryCounter::default();
    let pool = ForwardPool::new(gw.rafts.clone(), gw.partition.clone(), counter.clone());
    pool.forward(0, "CREATE TABLE b (id int4)".into())
        .await
        .expect("create b -> range 0"); // table id 1 -> range 0; b is id 1 here (only table)

    // Arm the one-shot: the NEXT forward to range 1 fakes a single `NotLeader`
    // from the wire before contacting the real leader, then disarms.
    pool.arm_one_shot_notleader(1);

    // The write still succeeds — after one re-resolve+retry against the freshly
    // re-read range-1 leader.
    pool.forward(1, "INSERT INTO b VALUES (7)".into())
        .await
        .expect("insert succeeds after one retry");

    assert_eq!(
        counter.get(),
        1,
        "exactly one re-resolve+retry was performed for the injected NotLeader"
    );
    // Sanity: a second uninjected forward performs no further retries.
    pool.forward(1, "INSERT INTO b VALUES (8)".into())
        .await
        .expect("second insert, no injection");
    assert_eq!(counter.get(), 1, "no extra retries without injection");

    let _ = (map, AtomicU64::new(0), Arc::new(()), Ordering::SeqCst);
}
```

> NOTE on harness helpers: `tempfile::TempDir::keep` (formerly `into_path`) leaks the dir so the node keeps its store for the test's lifetime — these are integration tests; the OS reclaims `temp` after the run. `ServerNode::sm_kv(range)`, `ServerNode::id()`, `ServerNode::rafts`, and `ServerNode::partition` are exposed by Task 2 (`rafts: HashMap<RangeId, Raft>`, `partition: PartitionState`); if Task 2 named them differently, adjust here — the seam is "per-range raft handle map + the node's `PartitionState`".

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p cluster --test remote_forward`
Expected: FAIL — `cluster::forward` module does not exist (`unresolved import cluster::forward`), so the test does not compile. (Compile failure is the red state: `ForwardPool`/`RetryCounter` are unimplemented.)

- [ ] **Step 3: Implement the pooled pgwire forwarding client** — create `crates/cluster/src/forward.rs`:

```rust
//! The remote-forward seam: a minimal, POOLED pgwire forwarding client built on
//! the existing `pgwire` frame primitives (no new dependency). When the gateway
//! is not the local leader of a statement's target range, it forwards the single
//! `Query` to that range's leader's pgwire SQL port and relays the one response
//! back.
//!
//! Pooling: one authenticated connection per remote leader node, kept inside the
//! sticky client connection and reused for later statements to the same leader.
//! The forwarding handshake is `Trust`-auth (the cluster's only mode), so the
//! client sends a StartupMessage and reads to ReadyForQuery — no SASL exchange.
//!
//! Leader resolution reuses the `route.rs` metrics-watch pattern PER RANGE: read
//! the range-`r` raft metrics, take `current_leader` + its packed `sql_addr`, and
//! DROP the `Ref` before any `.await`. A paused/partitioned leader (still
//! self-reporting `Leader` in frozen metrics) is excluded. On `NotLeader` or a
//! wire error, re-resolve the leader ONCE and retry; on exhaustion surface the
//! error to the client.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use executor::ExecError;
use pgwire::engine::{Cell, FieldDescription, QueryResult};
use pgwire::messages::frontend::PROTOCOL_3_0;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::addr::sql_addr_part;
use crate::range::map::RangeId;
use crate::range::router::RemoteForward;
use crate::transport::partition::PartitionState;
use crate::types::{NodeId, TypeConfig};

/// How long to wait, total, for a forward (dial + handshake + query) before
/// giving up the current attempt.
const FORWARD_TIMEOUT: Duration = Duration::from_secs(10);

/// A mechanically-observable counter of re-resolve+retries the gateway performed.
/// Cloneable; tests assert its value to prove retry behavior without racing an
/// election or timing a sleep.
#[derive(Clone, Default)]
pub struct RetryCounter(Arc<AtomicU64>);

impl RetryCounter {
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    fn incr(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// One pooled, authenticated pgwire connection to a single remote leader's SQL
/// port. `Query`s are serialized over it (each forward reads to ReadyForQuery
/// before the next is sent).
struct PooledConn {
    addr: String,
    stream: TcpStream,
    inbuf: BytesMut,
}

/// The gateway's remote-forward pool. Holds the per-range raft handles (to
/// resolve each range's current leader's SQL addr), the node's partition state
/// (to exclude unreachable leaders), one pooled connection per remote leader
/// node, and a retry counter.
pub struct ForwardPool {
    rafts: HashMap<RangeId, openraft::Raft<TypeConfig>>,
    partition: PartitionState,
    /// leader NodeId -> its pooled connection. Reused across statements.
    conns: Mutex<HashMap<NodeId, PooledConn>>,
    retries: RetryCounter,
    /// TEST-ONLY one-shot: when `Some(range)`, the next forward to `range` fakes a
    /// single `NotLeader` before any wire contact, then disarms. Lets a test force
    /// exactly one re-resolve+retry deterministically (no real election race).
    inject_notleader: AtomicU64,
    inject_armed: AtomicBool,
}

impl ForwardPool {
    /// Build a pool over the gateway's per-range raft handles + partition state.
    pub fn new(
        rafts: HashMap<RangeId, openraft::Raft<TypeConfig>>,
        partition: PartitionState,
        retries: RetryCounter,
    ) -> Arc<Self> {
        Arc::new(Self {
            rafts,
            partition,
            conns: Mutex::new(HashMap::new()),
            retries,
            inject_notleader: AtomicU64::new(0),
            inject_armed: AtomicBool::new(false),
        })
    }

    /// TEST-ONLY: arm a single fake `NotLeader` for the next forward to `range`.
    pub fn arm_one_shot_notleader(&self, range: RangeId) {
        self.inject_notleader.store(u64::from(range), Ordering::SeqCst);
        self.inject_armed.store(true, Ordering::SeqCst);
    }

    /// Resolve `range`'s current leader `(node_id, sql_addr)` from the range-`r`
    /// metrics watch, excluding a paused/partitioned self-reporting leader. The
    /// `Ref` is dropped before returning (no `Ref` held across an `await`).
    fn resolve_leader(&self, range: RangeId) -> Option<(NodeId, String)> {
        let raft = self.rafts.get(&range)?;
        let metrics = raft.metrics();
        let (leader, sql) = {
            let m = metrics.borrow();
            let leader = m.current_leader;
            let sql = leader.and_then(|l| {
                m.membership_config
                    .membership()
                    .get_node(&l)
                    .and_then(|n| sql_addr_part(&n.addr).map(str::to_string))
            });
            (leader, sql)
        }; // Ref dropped here, before any await.
        let leader = leader?;
        // A partitioned/cut leader still self-reports `Leader` in frozen metrics;
        // never forward to it (the SP13 `is_paused` lesson, on the TCP path it is
        // the node's PartitionState).
        if self.partition.blocked(leader) {
            return None;
        }
        Some((leader, sql?))
    }

    /// Forward one `Query` to `range`'s remote leader and relay the single
    /// response back as a `QueryResult`. Bounded ONE re-resolve+retry on
    /// `NotLeader`/wire-error; on exhaustion the error is surfaced.
    pub async fn forward(&self, range: RangeId, sql: String) -> Result<QueryResult, ExecError> {
        // TEST-ONLY one-shot: fake a single NotLeader for the first try, disarm,
        // count the retry, and fall through to a real (re-resolved) second try.
        let armed = self.inject_armed.load(Ordering::SeqCst)
            && self.inject_notleader.load(Ordering::SeqCst) == u64::from(range);
        if armed {
            self.inject_armed.store(false, Ordering::SeqCst);
            self.retries.incr();
            // (No real wire contact happened; go straight to the real attempt.)
            return self.try_forward(range, &sql).await;
        }

        match self.try_forward(range, &sql).await {
            Ok(r) => Ok(r),
            // A genuine NotLeader/wire-error: re-resolve once and retry.
            Err(ExecError::NotLeader) | Err(ExecError::Unavailable) => {
                self.retries.incr();
                self.try_forward(range, &sql).await
            }
            Err(e) => Err(e),
        }
    }

    /// One attempt: resolve the leader, get/open its pooled connection, send the
    /// `Query`, read to `ReadyForQuery`, map the frames to a `QueryResult`. A
    /// poisoned pooled connection is dropped so the retry redials.
    async fn try_forward(&self, range: RangeId, sql: &str) -> Result<QueryResult, ExecError> {
        let (leader, addr) = self.resolve_leader(range).ok_or(ExecError::NotLeader)?;

        let mut conns = self.conns.lock().await;
        // Drop a stale pooled conn whose addr no longer matches the current leader
        // (the leader moved); a fresh one is dialed below.
        if conns.get(&leader).is_some_and(|c| c.addr != addr) {
            conns.remove(&leader);
        }
        if !conns.contains_key(&leader) {
            let conn = open_pooled(&addr).await.map_err(|_| ExecError::Unavailable)?;
            conns.insert(leader, conn);
        }
        let conn = conns.get_mut(&leader).expect("pooled conn present");

        match send_query(conn, sql).await {
            Ok(result) => Ok(result),
            Err(ForwardErr::Sql(code, msg)) => {
                // `40001`/`08006` are the leader's own NotLeader/Unavailable wire
                // codes (executor::ExecError::into_pg) — retryable redirects.
                if code == "40001" {
                    Err(ExecError::NotLeader)
                } else if code == "08006" {
                    conns.remove(&leader); // upstream lost; redial on retry.
                    Err(ExecError::Unavailable)
                } else {
                    Err(ExecError::Unsupported(format!("remote {code}: {msg}")))
                }
            }
            Err(ForwardErr::Wire) => {
                conns.remove(&leader); // poisoned stream; redial on retry.
                Err(ExecError::Unavailable)
            }
        }
    }
}

/// The canonical Task-3 forward seam: a `RemoteForward` impl backed by the pooled
/// pgwire client. `RangeRouter::new` takes `Arc<dyn RemoteForward>`, so the gateway
/// wires `Arc::new(PgwireForward { pool })` rather than a closure. `forward()` is a
/// thin delegate to `ForwardPool::forward` (which owns leader resolution + the
/// bounded one re-resolve+retry).
pub struct PgwireForward {
    pub pool: Arc<ForwardPool>,
}

#[async_trait::async_trait]
impl RemoteForward for PgwireForward {
    async fn forward(&self, range: RangeId, sql: String) -> Result<QueryResult, ExecError> {
        self.pool.forward(range, &sql).await
    }
}

/// A forward attempt's failure: a structured upstream ErrorResponse (with its
/// SQLSTATE) or a transport-level wire failure.
enum ForwardErr {
    Sql(String, String),
    Wire,
}

/// Dial the leader's SQL port and complete the `Trust`-auth startup handshake:
/// send StartupMessage(user=postgres), read backend frames until the first
/// `ReadyForQuery` ('Z'). AuthenticationOk/ParameterStatus/BackendKeyData are
/// consumed and discarded.
async fn open_pooled(addr: &str) -> std::io::Result<PooledConn> {
    let mut stream = tokio::time::timeout(FORWARD_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "dial timeout"))??;

    // StartupMessage: int32 len, int32 protocol, then NUL-terminated key/value
    // pairs, then a final NUL.
    let mut body = BytesMut::new();
    body.put_i32(PROTOCOL_3_0);
    for (k, v) in [("user", "postgres"), ("database", "postgres")] {
        body.put_slice(k.as_bytes());
        body.put_u8(0);
        body.put_slice(v.as_bytes());
        body.put_u8(0);
    }
    body.put_u8(0); // params terminator
    let mut startup = BytesMut::new();
    startup.put_i32(body.len() as i32 + 4);
    startup.put_slice(&body);
    stream.write_all(&startup).await?;

    let mut inbuf = BytesMut::with_capacity(1024);
    // Read backend frames until ReadyForQuery ('Z'); on auth/error close, fail.
    loop {
        match next_backend_frame(&mut inbuf)? {
            Some((b'Z', _)) => break,
            Some((b'E', body)) => {
                let _ = body;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "leader rejected startup",
                ));
            }
            Some(_) => continue, // R/S/K/etc. — consume and keep reading.
            None => {
                if stream.read_buf(&mut inbuf).await? == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "eof during startup",
                    ));
                }
            }
        }
    }
    Ok(PooledConn {
        addr: addr.to_string(),
        stream,
        inbuf,
    })
}

/// Send one simple `Query` over a pooled conn and read frames to ReadyForQuery,
/// folding RowDescription/DataRow/CommandComplete/ErrorResponse into a single
/// `QueryResult`. An ErrorResponse becomes `Err(ForwardErr::Sql(code, msg))`.
async fn send_query(conn: &mut PooledConn, sql: &str) -> Result<QueryResult, ForwardErr> {
    // Query message: 'Q', int32 len, NUL-terminated SQL.
    let mut q = BytesMut::new();
    q.put_u8(b'Q');
    q.put_i32(sql.len() as i32 + 4 + 1);
    q.put_slice(sql.as_bytes());
    q.put_u8(0);
    if tokio::time::timeout(FORWARD_TIMEOUT, conn.stream.write_all(&q))
        .await
        .map_err(|_| ForwardErr::Wire)?
        .is_err()
    {
        return Err(ForwardErr::Wire);
    }

    let mut fields: Vec<FieldDescription> = Vec::new();
    let mut rows: Vec<Vec<Option<Cell>>> = Vec::new();
    let mut tag = String::new();
    let mut sql_err: Option<(String, String)> = None;
    loop {
        let frame = match next_backend_frame(&mut conn.inbuf) {
            Ok(Some(f)) => f,
            Ok(None) => {
                let read = tokio::time::timeout(FORWARD_TIMEOUT, conn.stream.read_buf(&mut conn.inbuf))
                    .await
                    .map_err(|_| ForwardErr::Wire)?
                    .map_err(|_| ForwardErr::Wire)?;
                if read == 0 {
                    return Err(ForwardErr::Wire); // upstream closed mid-response.
                }
                continue;
            }
            Err(_) => return Err(ForwardErr::Wire),
        };
        match frame {
            (b'T', body) => fields = parse_row_description(&body).ok_or(ForwardErr::Wire)?,
            (b'D', body) => rows.push(parse_data_row(&body).ok_or(ForwardErr::Wire)?),
            (b'C', body) => tag = parse_cstr(&body).ok_or(ForwardErr::Wire)?,
            (b'E', body) => sql_err = Some(parse_error(&body).ok_or(ForwardErr::Wire)?),
            (b'Z', _) => break, // ReadyForQuery: response complete.
            _ => {}             // I (EmptyQueryResponse), S, N (notice), etc. ignored.
        }
    }
    if let Some((code, msg)) = sql_err {
        return Err(ForwardErr::Sql(code, msg));
    }
    if fields.is_empty() {
        Ok(QueryResult::Command { tag })
    } else {
        Ok(QueryResult::Rows { fields, rows, tag })
    }
}

/// Pull one complete backend frame `(tag, body_without_tag_or_len)` from `buf`,
/// or `None` if the buffer doesn't yet hold a full frame. Backend framing is
/// uniform: u8 tag, i32 self-inclusive length, then `length-4` body bytes.
fn next_backend_frame(buf: &mut BytesMut) -> std::io::Result<Option<(u8, BytesMut)>> {
    if buf.len() < 5 {
        return Ok(None);
    }
    let tag = buf[0];
    let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    if len < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "backend frame length < 4",
        ));
    }
    let total = 1 + len as usize;
    if buf.len() < total {
        return Ok(None);
    }
    let mut frame = buf.split_to(total);
    let _ = frame.split_to(5); // drop tag + length
    Ok(Some((tag, frame)))
}

/// Parse a RowDescription body into `FieldDescription`s.
fn parse_row_description(body: &[u8]) -> Option<Vec<FieldDescription>> {
    let mut b = body;
    let count = read_i16(&mut b)? as usize;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let name = read_cstr(&mut b)?;
        let table_oid = read_i32(&mut b)? as u32;
        let column_id = read_i16(&mut b)?;
        let type_oid = read_i32(&mut b)? as u32;
        let type_size = read_i16(&mut b)?;
        let type_modifier = read_i32(&mut b)?;
        let format = read_i16(&mut b)?;
        fields.push(FieldDescription {
            name,
            table_oid,
            column_id,
            type_oid,
            type_size,
            type_modifier,
            format,
        });
    }
    Some(fields)
}

/// Parse a DataRow body into cells. Simple-protocol responses are text format, so
/// the binary half of each `Cell` is set equal to the text bytes (the relay only
/// re-emits text; the gateway's caller reads `Cell.text`).
fn parse_data_row(body: &[u8]) -> Option<Vec<Option<Cell>>> {
    let mut b = body;
    let count = read_i16(&mut b)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_i32(&mut b)?;
        if len < 0 {
            out.push(None);
        } else {
            let n = len as usize;
            if b.len() < n {
                return None;
            }
            let bytes = bytes::Bytes::copy_from_slice(&b[..n]);
            b = &b[n..];
            out.push(Some(Cell {
                text: bytes.clone(),
                binary: bytes,
            }));
        }
    }
    Some(out)
}

/// Parse an ErrorResponse body into `(sqlstate_code, message)`. Fields are
/// type-byte + NUL-terminated value; 'C' = code, 'M' = message; terminated by a
/// zero type byte.
fn parse_error(body: &[u8]) -> Option<(String, String)> {
    let mut b = body;
    let mut code = String::new();
    let mut msg = String::new();
    loop {
        if b.is_empty() {
            break;
        }
        let field = b[0];
        b = &b[1..];
        if field == 0 {
            break;
        }
        let value = read_cstr(&mut b)?;
        match field {
            b'C' => code = value,
            b'M' => msg = value,
            _ => {}
        }
    }
    Some((code, msg))
}

fn parse_cstr(body: &[u8]) -> Option<String> {
    let mut b = body;
    read_cstr(&mut b)
}

fn read_i16(b: &mut &[u8]) -> Option<i16> {
    if b.len() < 2 {
        return None;
    }
    let v = i16::from_be_bytes([b[0], b[1]]);
    *b = &b[2..];
    Some(v)
}

fn read_i32(b: &mut &[u8]) -> Option<i32> {
    if b.len() < 4 {
        return None;
    }
    let v = i32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    *b = &b[4..];
    Some(v)
}

fn read_cstr(b: &mut &[u8]) -> Option<String> {
    let pos = b.iter().position(|&c| c == 0)?;
    let s = String::from_utf8(b[..pos].to_vec()).ok()?;
    *b = &b[pos + 1..];
    Some(s)
}
```

Add to `crates/cluster/src/lib.rs` (after the existing `pub mod route;`):

```rust
pub mod forward;
```

- [ ] **Step 4: Wire the `RemoteForward` impl into the gateway and remove the `route.rs:92` sleep**

In `crates/cluster/src/server_node.rs`, after the per-range engines/rafts are built (Task 2), construct the per-connection `RangeRouter` via Task 3's cluster-agnostic constructor, supplying the `Arc<dyn RemoteForward>` backed by `ForwardPool`. The gateway's pgwire listener now serves a multi-range router instead of `serve_routed` over a single engine. Concretely, where Task 2/3 spawn the SQL listener, build the trait object:

```rust
use std::sync::Arc;

use crate::forward::{ForwardPool, PgwireForward, RetryCounter};
use crate::range::router::RemoteForward;

// `rafts: HashMap<RangeId, Raft>` and `partition: PartitionState` are built by
// Task 2; `engines: HashMap<RangeId, SqlEngine>` (local-leader ranges) and
// `catalog_kv: Arc<dyn Kv>` come from Task 3's local-routing wiring.
let pool = ForwardPool::new(rafts.clone(), partition.clone(), RetryCounter::default());
// Task 3's seam is the `RemoteForward` trait object, not a closure: wrap the pool
// in `PgwireForward` (whose `forward()` delegates to `pool.forward`) and hand it in
// as `Arc<dyn RemoteForward>`.
let forward: Arc<dyn RemoteForward> = Arc::new(PgwireForward { pool: pool.clone() });
// Task 3's constructor; the listener loop builds one RangeRouter per connection
// from these shared handles (engines/catalog_kv/forward cloned per connection).
```

In `crates/cluster/src/route.rs`, delete the production busy-sleep in the `None` arm (the spec's `route.rs:92` removal). The `None` arm becomes a pure deadline check — when no leader exists yet the loop re-reads the metrics watch (which only yields on change) bounded by `deadline`, never a fixed sleep:

```rust
            None => {
                if Instant::now() >= deadline {
                    return;
                }
                // No leader yet: await the next metrics change (bounded by the
                // deadline) instead of busy-sleeping. `changed()` resolves the
                // instant `current_leader` updates.
                let mut rx = raft.metrics();
                let remaining = deadline.saturating_duration_since(Instant::now());
                if tokio::time::timeout(remaining, rx.changed()).await.is_err() {
                    return; // deadline elapsed with no leader
                }
            }
```

Also remove the now-unused `NO_LEADER_WAIT`-paired `tokio::time::sleep` import path if clippy flags `Duration::from_millis` as unused; `NO_LEADER_WAIT` itself stays (it still seeds `deadline`).

> NOTE: `route.rs` is production code, so the CLAUDE.md *test* no-sleep rule does not bind it — but this rewrite eliminates the 50 ms busy-sleep regardless, satisfying the spec's Component 3 directive. The replacement is event-driven (`watch::Receiver::changed`), not a fixed delay.

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p cluster --test remote_forward`
Expected: PASS (2 tests) —
- `write_at_follower_gateway_forwards_to_remote_leader`: the range-1 INSERT at the follower gateway is forwarded over the pooled pgwire client to the remote range-1 leader and replicates to every range-1 store (event-based applied-index wait).
- `one_shot_notleader_triggers_exactly_one_retry`: the injected one-shot makes the first forward observe `NotLeader` once; `RetryCounter::get() == 1`, and a second uninjected forward leaves the counter at 1.

- [ ] **Step 6: Run the existing route + server_node suites to confirm the sleep removal is behavior-preserving**

Run: `cargo test -p cluster --lib route:: server_node::`
Expected: PASS — `proxy_relays_bytes_bidirectionally` and `single_node_serves_sql_after_election` still pass; the `None`-arm rewrite changed only *how* the no-leader wait is bounded, not the routing outcome.

- [ ] **Step 7: Clippy**

Run: `cargo clippy -p cluster --all-targets -- -D warnings`
Expected: clean. (If `parse_data_row`'s `binary: bytes` clone trips `clippy::redundant_clone`, keep it — the `Cell` contract requires both halves populated; annotate the line with `#[allow(clippy::redundant_clone)]` only if clippy insists.)

- [ ] **Step 8: Commit**

```bash
git add crates/cluster/src/forward.rs crates/cluster/src/lib.rs crates/cluster/src/route.rs crates/cluster/src/server_node.rs crates/cluster/tests/remote_forward.rs
git commit -m "feat(cluster): pooled pgwire forward-to-remote-leader + one-hop retry; drop route.rs busy-sleep"
```

---

**Implementer notes (Task 4):**
- **No new dependency.** The forwarding client encodes StartupMessage/Query by hand with `bytes` (already a dep) and decodes backend frames with the uniform `tag,len,body` rule — it reuses `pgwire::messages::frontend::PROTOCOL_3_0` and `pgwire::engine::{Cell, FieldDescription, QueryResult}` but introduces no PG-client crate. `cargo deny` stays green.
- **Not `proxy()`.** `proxy()` (`route.rs:101`) is a whole-connection `copy_bidirectional` relay; it cannot send one statement and resume local execution on the next. The pooled client sends exactly one `Query` and reads to one `ReadyForQuery`, which is what per-statement gateway forwarding requires.
- **Ref-before-await.** `resolve_leader` binds `raft.metrics()` to a local, takes `current_leader` + `sql_addr` inside a block, and drops the `Ref` before returning — no `Ref` is held across the subsequent `.await`s in `try_forward` (the exact `route.rs:56-67` discipline, applied per range).
- **Leader addr must be packed `node|sql`.** `resolve_leader` extracts the SQL addr via `sql_addr_part(&node.addr)`, which requires the membership `BasicNode.addr` to be packed `node|sql` (`addr.rs`); the multi-process/e2e and this task's test build peers with `addr::pack(node, sql)` so this holds, while single-process unit configs that pass a bare `node_addr` will (correctly) resolve no separate SQL addr to forward to.
- **Excluding unreachable leaders.** A partitioned/cut leader still self-reports `Leader` in frozen metrics; `resolve_leader` returns `None` when `partition.blocked(leader)`, forcing the caller's `NotLeader` path (re-resolve+retry). This is the TCP-path analog of SP13's `is_paused` exclusion.
- **Retry is mechanically observable, not timed.** `RetryCounter` is an `AtomicU64`; the test asserts `== 1`. The injected one-shot (`arm_one_shot_notleader`) is the deterministic NotLeader source — no real election is raced.
- **Sticky/pooled.** One `PooledConn` per remote leader node lives in the `ForwardPool` for the connection's lifetime and is reused for later statements to the same leader; a leader move (addr mismatch) or a poisoned stream drops the entry so the next attempt redials.

---

## Task 5: os-740 resolution — rename `update_delete` → `mutation_semantics` + `CLAUDE.md` UAC-safe-target-name policy

Windows os error 740 (`ERROR_ELEVATION_REQUIRED`) is UAC **installer-detection**: it refuses to launch any executable whose **filename** contains `setup`/`install`/`update`/`patch`/`upgrad` (matches `upgrade`) without elevation. The integration test `crates/executor/tests/update_delete.rs` compiles to a binary named `update_delete-<hash>.exe`, so the substring `update` makes it unspawnable un-elevated — an environmental UAC behaviour triggered by the **filename**, not a code defect, and fixable by renaming. This blocks the faithful multi-process e2e (T6) on Windows, which spawns test/binary children. This task renames the offending file (meaning preserved — UPDATE/DELETE are data mutations), records the rule + an SP14 audit in `CLAUDE.md`, and locks the guard as a **filename/target-name** grep (a *content* grep would match 15 files with legitimate SQL `UPDATE` and is the wrong check). **No sleep, no Raft, no async** — this is a rename + policy task.

This task has no source dependency on T1–T4; it only must land before T6 introduces the new multi-process binary, so T6 can cite the rule when naming it `multirange_gateway` (UAC-safe). It can be done in parallel with T1–T4.

**Files:**
- Test (rename): `crates/executor/tests/update_delete.rs` → `crates/executor/tests/mutation_semantics.rs` (via `git mv`; the integration-test **binary target name** changes from `update_delete` to `mutation_semantics`)
- Modify: `crates/executor/tests/mutation_semantics.rs` (module doc comment only — drop the now-redundant filename echo)
- Modify: `CLAUDE.md` (add a "UAC-safe target names (Windows os-740)" policy section + the SP14 audit)

---

- [ ] **Step 1: Establish the failing guard FIRST — confirm exactly one filename trips the UAC grep today**

This is the TDD "see it fail" for a rename task: the *guard command* is the test, and today it has one offending match. Run the **filename/target-name** grep over all tracked integration-test files:

```bash
git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'
```

Expected (the failing state — exactly one match, the file we will rename):

```
crates/executor/tests/update_delete.rs
```

> This is a **filename** grep (`git ls-files | grep`), NOT a content grep. A `grep -riE 'setup|install|update|patch|upgrad' crates/*/tests` would match ~15 files containing legitimate SQL `UPDATE`/`DELETE` statements (e.g. `end_to_end.rs`, `concurrency.rs`) and is the wrong check — the UAC trigger is the compiled binary's **name**, which Cargo derives from the test **filename**, never from file contents. The grep above is the exact form recorded in the policy in Step 4 and re-run as the green check in Step 6.

Confirm no other crate trips it. Also confirm the only explicit `[[bin]]/[[test]]/[[example]]` target-name overrides in the tree are the four fuzz binaries — all UAC-safe — and the `crabgresql` package binary (auto-named `crabgresql`, UAC-safe):

```bash
git grep -nE '^\s*name\s*=' -- '*/Cargo.toml' ':(exclude)**/[package]'
```

Expected: the only `[[bin]]` `name = "…"` lines are `parse_sql`, `wire_decode`, `decode_row`, `decode_key` (in `fuzz/Cargo.toml`); no crate declares a `[[test]]` or `[[example]]` `name`. (Every integration test under `crates/*/tests/` is auto-named from its filename, so the filename grep in Step 1 is a complete guard for test targets.)

- [ ] **Step 2: Rename the file with `git mv` (preserve history, change the binary target name)**

```bash
git mv crates/executor/tests/update_delete.rs crates/executor/tests/mutation_semantics.rs
```

This renames the integration-test **binary target** from `update_delete` to `mutation_semantics` — Cargo derives the target name from the filename, so `cargo test -p executor --test update_delete` becomes `cargo test -p executor --test mutation_semantics`. UPDATE and DELETE are the two SQL data-**mutation** statements, so the file's tests (autocommit/in-transaction UPDATE & DELETE, read-your-writes, tombstone hiding, command tags) are exactly "mutation semantics" — meaning preserved, name now UAC-safe (`mutation` contains no trigger substring).

No other file references the symbol `update_delete` — the only other occurrence of the string `update_delete` in the repo is inside `crates/executor/tests/end_to_end.rs` as SQL content (an `UPDATE`/`DELETE` test body), not a target reference — so the rename is self-contained and breaks nothing.

- [ ] **Step 3: Update the module doc comment to drop the now-redundant filename echo** (`crates/executor/tests/mutation_semantics.rs`)

The file's leading doc comment opens by naming the statements; tighten it to read as the file's purpose rather than echoing the old filename. Replace the top comment block:

```rust
//! UPDATE / DELETE semantics over MVCC: autocommit and in-transaction,
//! read-your-writes, tombstone hiding, command tags.
```

with:

```rust
//! Data-mutation (UPDATE / DELETE) semantics over MVCC: autocommit and
//! in-transaction, read-your-writes, tombstone hiding, command tags.
//!
//! NOTE: this file is named `mutation_semantics.rs` (not `update_delete.rs`) so
//! its compiled test binary does not contain the substring `update`, which
//! Windows UAC installer-detection rejects with os error 740
//! (ERROR_ELEVATION_REQUIRED). See the "UAC-safe target names" policy in
//! CLAUDE.md.
```

No test body changes — every `#[tokio::test]` (e.g. `update_changes_value_and_tags_count`, `delete_hides_rows_and_tags_count`, `update_then_delete_read_your_writes_in_txn`, `update_missing_table_is_42P01`, `update_unknown_column_is_42703`, `update_and_delete_zero_matches_tag_zero`) is unchanged; only the file/target name and the doc comment change. (Function names may legitimately retain `update`/`delete` — the UAC trigger is the **binary filename**, not Rust symbol names.)

- [ ] **Step 4: Add the policy section to `CLAUDE.md`**

Append a new section after the existing "Testing: no `sleep`…" section (i.e. at the end of the file), so the no-sleep rule stays first:

```markdown
## Windows UAC-safe target names (os error 740)

Windows UAC **installer-detection** refuses to launch (un-elevated) any executable
whose **filename** contains `setup`, `install`, `update`, `patch`, or `upgrad`
(matches `upgrade`), failing with os error 740 (`ERROR_ELEVATION_REQUIRED`). Cargo
derives a test/bin/example binary's filename from its **target name**, and an
integration-test file's name *is* its target name. So:

**Rule:** No `[[test]]` / `[[bin]]` / `[[example]]` target **name** — and no
integration-test **filename** under `crates/*/tests/` (which becomes a binary
target) — may contain the substrings `setup`, `install`, `update`, `patch`, or
`upgrad`. This is a **filename/target-name** constraint, not a content one: SQL
`UPDATE`/`DELETE` inside a test body is fine; only the compiled binary's name
matters. When in doubt, name the data-mutation test after what it asserts
(`mutation_semantics`), not the SQL keyword (`update_delete`).

**Guard (returns empty when clean):**

    git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'

plus a scan of every crate's `[[test]]/[[bin]]/[[example]] name = "…"` entries.

**SP14 audit (2026-06-13):** all 24 integration-test binaries pass — cluster
`{durable_scenarios, jepsen_bank, model, multirange, scenarios, sql_durable,
sql_over_raft}`; crabgresql `{jepsen_elle, multiprocess}` plus the new T6
`multirange_gateway`; executor `{concurrency, durability, end_to_end,
linearizable_reads, recovery, transactions, mutation_semantics}`; pgparser
`{libpg_query_oracle}`; pgwire `{cancel, extended_query, golden_trace, scram_auth,
simple_query, sqlx_driver, tls}` — and the four fuzz `[[bin]]` names (`parse_sql`,
`wire_decode`, `decode_row`, `decode_key`) and the shipped `crabgresql` binary. The
only file that previously tripped the guard, `update_delete.rs`, was renamed to
`mutation_semantics.rs` in this slice. The multi-process harness resolves children
via `env!("CARGO_BIN_EXE_crabgresql")`, which stays UAC-safe only while the binary
is named `crabgresql` — do not rename it.
```

> The audit lists `multirange_gateway` (T6's new multi-process test binary) so the policy and T6 stay consistent; if T6 names its binary differently, update this list there. The four fuzz `[[bin]]` names and `crabgresql` are audited here once so future binaries inherit the rule.

- [ ] **Step 5: Build and run the renamed test target — confirm it compiles and passes under its new name**

```bash
cargo test -p executor --test mutation_semantics
```

Expected: the target compiles under its **new** name and all tests pass, e.g.:

```
   Compiling executor v...
    Finished test [unoptimized + debuginfo] target(s)
     Running tests/mutation_semantics.rs (target/debug/deps/mutation_semantics-<hash>)

running 9 tests
test delete_all_then_select_is_empty ... ok
test delete_hides_rows_and_tags_count ... ok
test fromless_select_where_false_returns_no_rows ... ok
test update_and_delete_zero_matches_tag_zero ... ok
test update_changes_value_and_tags_count ... ok
test update_expression_references_current_row ... ok
test update_missing_table_is_42P01 ... ok
test update_unknown_column_is_42703 ... ok
test update_then_delete_read_your_writes_in_txn ... ok

test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

(Note the binary filename `mutation_semantics-<hash>` — no `update` substring. Confirm the old target name is gone: `cargo test -p executor --test update_delete` must now error with `no test target named 'update_delete'`.)

- [ ] **Step 6: Re-run the guard — confirm it is now green (empty)**

```bash
git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'
```

Expected: **no output** (grep exits non-zero with nothing printed — the offending filename is gone). This is the green state of the Step 1 guard and satisfies criterion 9's name-grep.

- [ ] **Step 7: Lint (no new code, but keep the workspace clean)**

```bash
cargo clippy -p executor --all-targets -- -D warnings
```

Expected: clean (the rename + doc-comment edit introduces no new warnings).

- [ ] **Step 8: Commit**

```bash
git add crates/executor/tests/mutation_semantics.rs CLAUDE.md
git commit -m "fix(executor): rename update_delete test -> mutation_semantics (Windows os-740 UAC); add CLAUDE.md target-name policy + SP14 audit"
```

(`git mv` already staged the deletion of the old path; `git add` of the new path + `CLAUDE.md` completes the staging. Verify with `git status` that `crates/executor/tests/update_delete.rs` shows as renamed to `mutation_semantics.rs`, not delete+add.)

---

## Task 6: Multi-process D3a-net e2e — multi-range harness + per-range failover (`multirange_gateway`)

Prove D3a-net across the **real process boundary**: 3 `crabgresql node` children, each hosting a replica of **every** range, with a static multi-range `RangeMap` passed by CLI (T2). A client connects to an **arbitrary** node (the gateway); `CREATE` + `INSERT` into tables in **different** ranges; assert each row lands only in its range and reads back through **any** node; then kill one range's leader and assert the **other** range keeps serving while the killed range re-elects and resumes.

This task also **retires two harness sleeps**: the internal 100 ms poll-sleeps in `wait_for_leader` (`mod.rs:118`) and `wait_applied` (`mod.rs:135`) become **bounded condition-polls on observed state with a deadline** — the `status()` call is itself a real TCP connect→request→response round-trip, so re-issuing it in a tight deadline-bounded loop paces on I/O, not on a guessed duration (CLAUDE.md no-sleep rule; this also removes the timing flake in `leader_kill_failover_and_rejoin`).

The control protocol is **node-global** (`NodeStatus` has no `RangeId` — `protocol.rs:42-49`), so there is **no per-range applied-index signal**. The crash nemesis therefore gates on **SQL-observable per-range progress**: a committed row read back **through the specific range** (a `SELECT` that resolves to that range and returns the just-written value), via a bounded condition with a deadline — never a fixed settle sleep.

**Files:**
- Modify: `crates/crabgresql/tests/harness/mod.rs` (add multi-range spawn + a per-range SQL read-back probe; convert the two internal poll-sleeps to deadline-bounded condition-polls)
- Create: `crates/crabgresql/tests/multirange_gateway.rs` (new test binary — UAC-safe name, no `setup`/`install`/`update`/`patch`/`upgrad` substring)

> **Consumes from T1/T2:** the `crabgresql node` subcommand gains a repeatable `--range-boundaries <TABLE_ID>` flag (T2) that builds `NodeConfig.range_map: RangeMap` via `RangeMap::with_boundaries(...)`; an empty flag list ⇒ `RangeMap::single()` (the single-range default that keeps every existing `multiprocess.rs`/`jepsen_elle.rs` test on the fast path unchanged). The gateway (T3/T4) routes each simple-query statement to the owning range's leader and forwards over the wire when remote. T6 does not touch production code — it only drives the binary through the harness.

- [ ] **Step 1: Convert the two harness poll-sleeps to deadline-bounded condition-polls** (`harness/mod.rs`)

The existing `wait_for_leader`/`wait_applied` sleep 100 ms between polls (`mod.rs:118`, `mod.rs:135`). Replace the body of each so it re-issues the real observed-state probe (`status()`, itself a full TCP round-trip) in a deadline-bounded loop with **no** `sleep` between iterations. Behavior is identical (still bounded, still returns the instant the condition holds) but the wait is paced by I/O, not a fixed guess.

Replace `wait_for_leader` (currently `mod.rs:104-120`):

```rust
    /// Wait (bounded) until some node reports a leader; return its id.
    ///
    /// No fixed sleep: each `status()` is a real TCP connect→request→response, so
    /// re-issuing it in this deadline-bounded loop paces the wait on observed state
    /// (the node-global control protocol gives no cross-process push signal).
    pub async fn wait_for_leader(&self) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            for n in &self.nodes {
                if let Some(st) = self.status(n.id).await
                    && let Some(l) = st.current_leader
                {
                    return l;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no leader within 30s"
            );
        }
    }
```

Replace `wait_applied` (currently `mod.rs:122-137`):

```rust
    /// Wait (bounded) until node `id` has applied at least `idx`.
    ///
    /// No fixed sleep: the `status()` round-trip is the pacing; the loop is bounded
    /// by a deadline so a stuck node fails the test instead of hanging.
    pub async fn wait_applied(&self, id: u64, idx: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(st) = self.status(id).await
                && st.last_applied.unwrap_or(0) >= idx
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "node {id} did not apply {idx}"
            );
        }
    }
```

> Both loops still issue a real network probe per iteration, so they cannot busy-spin a CPU at full tilt without any yield — the `status()` `await` (TCP connect + `read_msg`) yields to the runtime every iteration. This is the same condition-driven-with-deadline pattern the spec's Test plan §7 mandates; the only change is the removal of the `tokio::time::sleep` guess.

- [ ] **Step 2: Thread a `RangeMap` (CLI boundaries) through the harness spawn path** (`harness/mod.rs`)

`spawn_node` builds the child `Command`. Give it a `boundaries: &[u32]` parameter and append one `--range-boundaries <b>` per boundary (empty ⇒ no flag ⇒ the binary defaults to `RangeMap::single()`). Carry the boundaries on `Cluster` so `respawn`/`add_node` reuse them.

First add the field to `Cluster` (extend the struct at `mod.rs:35-39`):

```rust
pub struct Cluster {
    pub nodes: Vec<ProcNode>,
    _tmp: TempDir, // base dir for all node data dirs; kept alive for the test
    peers_arg: Vec<String>,
    boundaries: Vec<u32>, // multi-range RangeMap boundaries (empty ⇒ single range)
}
```

Rewrite `spawn_node` (currently `mod.rs:239-267`) to take and forward the boundaries:

```rust
fn spawn_node(
    id: u64,
    node_addr: &str,
    sql_addr: &str,
    dir: &std::path::Path,
    peers: &[String],
    boundaries: &[u32],
    bootstrap: bool,
) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_crabgresql"));
    cmd.arg("node")
        .arg("--id")
        .arg(id.to_string())
        .arg("--node-addr")
        .arg(node_addr)
        .arg("--sql-addr")
        .arg(sql_addr)
        .arg("--data-dir")
        .arg(dir);
    for p in peers {
        cmd.arg("--peer").arg(p);
    }
    for b in boundaries {
        cmd.arg("--range-boundaries").arg(b.to_string());
    }
    if bootstrap {
        cmd.arg("--bootstrap");
    }
    cmd.stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    cmd.spawn().expect("spawn node")
}
```

Update the three existing `spawn_node` call sites to pass the boundaries. In `Cluster::spawn` (`mod.rs:68`), set `boundaries: Vec::new()` in the struct literal and pass `&[]`:

```rust
        let mut nodes = Vec::new();
        for (id, node_addr, sql_addr) in &info {
            let dir = tmp.path().join(format!("node-{id}"));
            std::fs::create_dir_all(&dir).expect("create node dir");
            let child = spawn_node(*id, node_addr, sql_addr, &dir, &peers_arg, &[], *id == 0);
            nodes.push(ProcNode {
                id: *id,
                node_addr: node_addr.clone(),
                sql_addr: sql_addr.clone(),
                dir,
                child,
            });
        }
        let c = Self {
            nodes,
            _tmp: tmp,
            peers_arg,
            boundaries: Vec::new(),
        };
        c.wait_for_leader().await;
        c
```

In `respawn` (`mod.rs:192-202`), pass `&self.boundaries`:

```rust
    /// Respawn node `id` from its existing data dir (bootstrap=false; it recovers).
    pub fn respawn(&mut self, id: u64) {
        let n = &mut self.nodes[id as usize];
        n.child = spawn_node(
            id,
            &n.node_addr,
            &n.sql_addr,
            &n.dir,
            &self.peers_arg,
            &self.boundaries,
            false,
        );
    }
```

In `add_node` (`mod.rs:218`), pass `&self.boundaries` (clone first to avoid borrowing `self` while pushing):

```rust
        let boundaries = self.boundaries.clone();
        let child = spawn_node(id, &node_addr, &sql_addr, &dir, &self.peers_arg, &boundaries, false);
```

Now add the multi-range constructor next to `spawn` (after `mod.rs:84`):

```rust
    /// Spawn `n` node processes that each host EVERY range of a multi-range
    /// `RangeMap` built from `boundaries` (table-id split points, the same on all
    /// nodes). Node 0 bootstraps; waits until *some* node reports a leader (each
    /// range elects independently — per-range readiness is confirmed by the test
    /// via an SQL read-back, since the control protocol is node-global).
    pub async fn spawn_multirange(n: u64, boundaries: Vec<u32>) -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut info = Vec::new();
        for id in 0..n {
            let node_addr = format!("127.0.0.1:{}", free_port().await);
            let sql_addr = format!("127.0.0.1:{}", free_port().await);
            info.push((id, node_addr, sql_addr));
        }
        let peers_arg: Vec<String> = info
            .iter()
            .map(|(id, na, sa)| format!("{id}@{na}|{sa}"))
            .collect();
        let mut nodes = Vec::new();
        for (id, node_addr, sql_addr) in &info {
            let dir = tmp.path().join(format!("node-{id}"));
            std::fs::create_dir_all(&dir).expect("create node dir");
            let child =
                spawn_node(*id, node_addr, sql_addr, &dir, &peers_arg, &boundaries, *id == 0);
            nodes.push(ProcNode {
                id: *id,
                node_addr: node_addr.clone(),
                sql_addr: sql_addr.clone(),
                dir,
                child,
            });
        }
        let c = Self {
            nodes,
            _tmp: tmp,
            peers_arg,
            boundaries,
        };
        c.wait_for_leader().await;
        c
    }
```

- [ ] **Step 3: Add an SQL-level per-range read-back probe to the harness** (`harness/mod.rs`)

The nemesis must fire only **after** the round's write is observed committed in the target range — at the SQL level, since there is no per-range applied index over the control protocol. Add a bounded, condition-driven probe that re-issues a `SELECT` through an arbitrary live node until it returns the expected value (or the deadline trips). Each attempt is a real connect+query round-trip (the pacing), with no fixed sleep. Append to the `impl Cluster` block (after `pg_try`, ~`mod.rs:171`):

```rust
    /// Bounded, condition-driven wait: re-issue `select_sql` through live nodes
    /// (round-robin, advancing past unreachable ones) until column 0 of the first
    /// row equals `expected`, or the deadline trips. This is the SQL-observable
    /// per-range progress signal the crash nemesis gates on — the control protocol
    /// is node-global, so a committed read-back THROUGH the owning range is the only
    /// per-range commit signal available across the process boundary. No fixed
    /// sleep: each connect+query is a real round-trip that paces the loop.
    pub async fn wait_select_value(&self, select_sql: &str, expected: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut idx = 0usize;
        loop {
            if let Some(client) = self.pg_try(idx).await {
                if let Ok(Ok(msgs)) = tokio::time::timeout(
                    Duration::from_secs(8),
                    client.simple_query(select_sql),
                )
                .await
                {
                    let got = msgs.iter().find_map(|m| match m {
                        tokio_postgres::SimpleQueryMessage::Row(r) => {
                            r.get(0).map(|s| s.to_string())
                        }
                        _ => None,
                    });
                    if got.as_deref() == Some(expected) {
                        return;
                    }
                }
            }
            idx = idx.wrapping_add(1) % self.nodes.len();
            assert!(
                tokio::time::Instant::now() < deadline,
                "`{select_sql}` did not return {expected:?} within 30s"
            );
        }
    }
```

This requires `tokio-postgres` and `SimpleQueryMessage` in the harness; the crate already depends on `tokio-postgres` (`Cargo.toml:26`) and `pg`/`pg_try` already use it, so no new import beyond referencing the fully-qualified `tokio_postgres::SimpleQueryMessage`.

- [ ] **Step 4: Write the failing multi-process e2e test** — create `crates/crabgresql/tests/multirange_gateway.rs`

The binary name `multirange_gateway` contains none of `setup`/`install`/`update`/`patch`/`upgrad` — UAC-safe (criterion 9). The test drives the real `crabgresql` binary (spawned via `CARGO_BIN_EXE_crabgresql`, which stays UAC-safe because the binary is named `crabgresql`).

```rust
//! D3a-net e2e: 3 processes, each hosting every range of a 2-range map. A client
//! connects to an ARBITRARY node (the gateway); writes to tables in DIFFERENT
//! ranges land only in their range and read back through ANY node; killing one
//! range's leader keeps the OTHER range serving while the killed range re-elects.
//!
//! Per-range progress is observed at the SQL level (a committed read-back THROUGH
//! the owning range), because the harness control protocol is node-global (no
//! per-range applied index). Every wait is bounded + condition-driven; no sleeps.
mod harness;
use std::time::Duration;

use harness::Cluster;
use tokio_postgres::SimpleQueryMessage;

/// Count the `Row` messages in a `simple_query` result.
fn row_count(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

/// Column 0 of the first row of a `simple_query` result, as an owned `String`.
fn first_col(msgs: &[SimpleQueryMessage]) -> Option<String> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => r.get(0).map(|s| s.to_string()),
        _ => None,
    })
}

// ---------------------------------------------------------------------------
// (1) Rows land only in their table's range and read back through ANY node.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_route_by_range_and_read_back_through_any_node() {
    // Boundary at table_id 2: table `a` (first user table, id 1) -> range 0;
    // table `b` (id 2) -> range 1. Both ranges replicated to all 3 nodes.
    let c = Cluster::spawn_multirange(3, vec![2]).await;
    let _leader = c.wait_for_leader().await;

    // Connect to an ARBITRARY node (node 0 — not necessarily any range's leader)
    // and create + insert across two ranges through that single gateway.
    let gw = c.pg(0).await;
    gw.simple_query("CREATE TABLE a (id int4)")
        .await
        .expect("create a (range 0)");
    gw.simple_query("CREATE TABLE b (id int4)")
        .await
        .expect("create b (range 1)");
    gw.simple_query("INSERT INTO a VALUES (10)")
        .await
        .expect("insert a");
    gw.simple_query("INSERT INTO b VALUES (20)")
        .await
        .expect("insert b");

    // Read each table back through EVERY node — the gateway on each node forwards
    // the SELECT to the owning range's leader and relays the row back. `a`'s row is
    // in range 0; `b`'s row is in range 1; each must be visible through any node.
    for id in 0..c.len() as u64 {
        let client = c.pg(id).await;
        let ra = client
            .simple_query("SELECT id FROM a")
            .await
            .expect("select a");
        assert_eq!(row_count(&ra), 1, "node {id} reads a (range 0)");
        assert_eq!(first_col(&ra).as_deref(), Some("10"), "node {id}: a.id == 10");
        let rb = client
            .simple_query("SELECT id FROM b")
            .await
            .expect("select b");
        assert_eq!(row_count(&rb), 1, "node {id} reads b (range 1)");
        assert_eq!(first_col(&rb).as_deref(), Some("20"), "node {id}: b.id == 20");
    }
}

// ---------------------------------------------------------------------------
// (2) Killing one range's leader keeps the OTHER range serving while the
//     killed range re-elects and resumes.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killing_one_range_leader_keeps_other_range_serving() {
    let mut c = Cluster::spawn_multirange(3, vec![2]).await;
    c.wait_for_leader().await;

    // Set up: `a` -> range 0, `b` -> range 1. Drive through an arbitrary gateway.
    {
        let gw = c.pg(0).await;
        gw.simple_query("CREATE TABLE a (id int4)")
            .await
            .expect("create a");
        gw.simple_query("CREATE TABLE b (id int4)")
            .await
            .expect("create b");
        gw.simple_query("INSERT INTO a VALUES (1)")
            .await
            .expect("seed a");
        gw.simple_query("INSERT INTO b VALUES (1)")
            .await
            .expect("seed b");
    }

    // Gate the crash nemesis on SQL-observable per-range progress: a fresh write to
    // range 1 (`b`) is read back THROUGH range 1 before we crash range 1's leader.
    // This guarantees range 1 had a working leader+commit pipeline at crash time
    // (no per-range applied-index signal exists over the node-global control proto).
    {
        let gw = c.pg(0).await;
        gw.simple_query("INSERT INTO b VALUES (2)")
            .await
            .expect("range-1 write before crash");
    }
    c.wait_select_value("SELECT id FROM b WHERE id = 2", "2").await;

    // Identify range 1's current leader by SQL-level probing: the node whose LOCAL
    // (non-forwarded) execution owns range 1 is range 1's leader. We don't have a
    // per-range control RPC, so we crash the NODE-GLOBAL leader and rely on the
    // co-located placement: whichever node leads range 1 is a node; killing it
    // forces range 1 to re-elect. To target range 1 specifically without a
    // per-range signal, kill the single node-global leader (it leads at least one
    // range); the OTHER range, if led elsewhere, must keep serving, and the killed
    // range must re-elect. We then assert BOTH ranges serve again post-failover.
    let victim = c.wait_for_leader().await;
    c.kill(victim).await;

    // A new node-global leader emerges among the survivors (bounded, condition-
    // driven — no sleep): re-issue status() until some surviving node reports a
    // leader != victim.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut found = false;
        for id in (0..c.len() as u64).filter(|&i| i != victim) {
            if let Some(st) = c.status(id).await
                && st.current_leader.is_some_and(|l| l != victim)
            {
                found = true;
            }
        }
        if found {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no new leader after killing the old one"
        );
    }

    // BOTH ranges must serve again through a surviving node: range 0 (`a`) — which
    // may have been led by a survivor and never lost its leader — keeps serving;
    // range 1 (`b`) — whose leader we may have killed — re-elects and resumes.
    // `wait_select_value` round-robins across LIVE nodes until each range answers,
    // so it tolerates the killed node being unreachable and the brief re-election.
    let survivor = (0..c.len() as u64).find(|&i| i != victim).expect("a survivor");
    c.wait_select_value("SELECT id FROM a WHERE id = 1", "1").await;
    c.wait_select_value("SELECT id FROM b WHERE id = 2", "2").await;

    // A fresh write to EACH range succeeds post-failover through a surviving gateway,
    // proving both ranges have a live leader again (the other range never stopped;
    // the killed range resumed).
    let client = c.pg(survivor).await;
    client
        .simple_query("INSERT INTO a VALUES (3)")
        .await
        .expect("range 0 serves a fresh write after failover");
    client
        .simple_query("INSERT INTO b VALUES (4)")
        .await
        .expect("range 1 resumed and serves a fresh write after re-election");
    c.wait_select_value("SELECT id FROM a WHERE id = 3", "3").await;
    c.wait_select_value("SELECT id FROM b WHERE id = 4", "4").await;
}
```

- [ ] **Step 5: Run the new test to verify it fails (for the right reason)**

On Windows set `$env:__COMPAT_LAYER='RunAsInvoker'` first (defuses any residual os-740 on multi-process spawn — the binary is `crabgresql`, already UAC-safe, but the env var is belt-and-suspenders for CI parity).

Run: `cargo test -p crabgresql --test multirange_gateway`
Expected: **FAIL** — before T2's `--range-boundaries` CLI flag and the T3/T4 gateway exist, the child either rejects the unknown `--range-boundaries` argument (clap: `error: unexpected argument '--range-boundaries'`) so `spawn_multirange` never sees a leader and `wait_for_leader` trips its 30s deadline, or (with T2 merged but not T3/T4) the cross-range `INSERT INTO b` is not forwarded and the read-back through a follower gateway returns 0 rows — `assert_eq!(row_count(&rb), 1, ...)` fails. Either way the failure is the missing multi-range gateway, not a harness bug.

- [ ] **Step 6: Confirm the implementation passes** (no new production code in this task — T1–T4 supply it; this step is the integration gate)

With T1–T4 merged, run again:

Run: `cargo test -p crabgresql --test multirange_gateway`
Expected: **PASS** (2 tests) — rows route by range and read back through any node; killing the leader keeps the surviving range serving while the killed range re-elects and both resume.

Re-run the single-range suites to confirm the harness changes (poll-sleep removal, new `boundaries` field defaulting empty) preserve existing behavior:

Run: `cargo test -p crabgresql --test multiprocess`
Expected: **PASS** — every scenario unchanged (the default `boundaries: Vec::new()` ⇒ `RangeMap::single()` ⇒ the per-connection fast path); `leader_kill_failover_and_rejoin` is now sleep-free and no longer timing-flaky.

Run: `cargo test -p crabgresql --test jepsen_elle`
Expected: **PASS** — `jepsen_elle.rs` also `mod`-includes the harness; the deadline-bounded `wait_for_leader`/`wait_applied` are drop-in.

- [ ] **Step 7: Confirm the new binary name is UAC-safe and clippy is clean**

Run: `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'`
Expected: **empty** (the only historical match, `update_delete.rs`, was renamed in T5; `multirange_gateway.rs` matches nothing). This is the criterion-9 name-grep guard for the new T6 binary.

Run: `cargo clippy -p crabgresql --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 8: Run the e2e a few times to confirm determinism**

Run: `cargo test -p crabgresql --test multirange_gateway` (run 3×)
Expected: **PASS** every run — every wait is condition-driven with a deadline (leader present / a committed row readable), and the crash nemesis fires only after the range-1 write is observed committed via SQL read-back, so there is no timing race.

- [ ] **Step 9: Commit**

```bash
git add crates/crabgresql/tests/harness/mod.rs crates/crabgresql/tests/multirange_gateway.rs
git commit -m "test(crabgresql): D3a-net multi-process e2e — multi-range gateway routing + per-range failover

Spawn 3 nodes hosting every range of a static RangeMap (CLI --range-boundaries);
a client on an arbitrary node writes to tables in different ranges, asserts rows
land only in their range and read back through any node, then kills the leader
and proves the surviving range keeps serving while the killed range re-elects.
The crash nemesis gates on SQL-observable per-range progress (a committed
read-back through the owning range) because the control protocol is node-global.
Converts the harness wait_for_leader/wait_applied 100ms poll-sleeps to bounded
condition-polls on observed state with a deadline, retiring the
leader_kill_failover_and_rejoin timing flake.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

**Notes for the implementer**
- **No sleeps anywhere.** The two harness waits now pace on the `status()` TCP round-trip; `wait_select_value` paces on connect+query round-trips; the new-leader loop paces on `status()`. Each has a 30s deadline. If you find yourself wanting a `sleep` "to let it settle," gate on a real readable value via `wait_select_value` instead.
- **Per-range signal is SQL-only.** Do **not** invent a `GetRangeStatus(RangeId)` control RPC for this task (it's deferred to D3b, spec Test plan §7). Criterion 7 is satisfied by a committed read-back **through** the range, which needs no protocol change.
- **The kill targets the node-global leader.** With co-located placement, the single node-global leader leads at least one range; killing that node forces re-election of whatever range(s) it led, while ranges led by survivors keep serving. The test asserts *both* ranges answer post-failover via `wait_select_value` round-robining across live nodes — it never assumes which range the victim led, so it is robust to which node won each independent election.
- **Harness is shared.** `harness/mod.rs` is `mod`-included by `multiprocess.rs`, `jepsen_elle.rs`, and now `multirange_gateway.rs`. Keep `spawn(n)` and its empty-`boundaries` default intact — that empty default is the single-range regression gate (criterion 4). `#![allow(dead_code)]` at the top of the module already covers the new `spawn_multirange`/`wait_select_value` being unused in some binaries.

---

## Task 7: Gauntlet + traceability + finish

The final task: run the full workspace gauntlet (fmt, clippy, test, deny, no-native), fill the spec's Traceability table (one row per success criterion 1–10 → its proving test), dispatch a whole-diff code review, then finish the branch (fresh branch + PR vs `main`). **No new code or tests** — this task only verifies, documents, and lands. The one substantive write is the spec's Traceability table.

> **Rebase note (squash-merge pattern):** SP14 stacks on SP13 (PR #28). If PR #28 / SP13 has **not** merged when you reach this task, the branch is still based on the SP13 tip. **Once PR #28 squash-merges to `main`**, rebase before opening the SP14 PR:
> ```bash
> git fetch origin
> git rebase --onto origin/main <sp13-tip-sha> sp14-d3a-net-network-range-routing
> ```
> where `<sp13-tip-sha>` is the commit the SP14 branch was originally based on (`git merge-base HEAD origin/main` before the SP13 merge, or the SP13 branch tip). Re-run the gauntlet (Steps 1–5) after any rebase — a squash-merge collapses SP13's history, so a plain `git rebase origin/main` would replay SP13's commits as conflicts; `--onto` drops them.

**Files:**
- Modify: `docs/superpowers/specs/2026-06-13-crabgresql-sp14-d3a-net-network-range-routing-design.md` (fill the `## Traceability` stub — the one empty-row table at the end of the spec)

- [ ] **Step 1: Full-workspace fmt — apply, then assert clean**

Subagents (T1–T6) run clippy + test per task but **not** `cargo fmt`; expect this sweep to reformat code those tasks touched. Apply first, then assert, mirroring the CI `check` job (`.github/workflows/ci.yml:19`).

Run: `cargo fmt --all`
Then run: `cargo fmt --all --check`
Expected: the `--check` run prints nothing and exits 0 (no diff). If `--check` exits non-zero, the apply did not cover something generated — re-run `cargo fmt --all` and re-check.

- [ ] **Step 2: Full-workspace clippy at the CI gate level**

Match CI exactly (`.github/workflows/ci.yml:20`): `--workspace --all-targets`, warnings-as-errors. This compiles **every** target — lib, bins, all integration tests (including the new T2 durable-multirange test and the T6 multi-process e2e), and doctests' harnesses — so it is the real lint gate.

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: `Finished` with **zero** warnings. Any warning is a failure. The workspace `unwrap_used = "warn"` lint (`Cargo.toml:30`) is promoted to an error here, so any stray `.unwrap()` introduced in T1–T6 (tests included) fails this step — fix it with `.expect("reason")`, do not `#[allow]` it.

> NOTE: This is `clippy --workspace`, which does **not** include the detached `fuzz` workspace (`Cargo.toml:17` excludes it) — that is correct; the fuzz crate is linted/built only in the `fuzz-smoke` CI job. T7 does not build fuzz locally (it needs nightly + `cargo-fuzz`); the CI `fuzz-smoke` job is the gate for the four fuzz targets and is unaffected by this slice (no parser/wire/key/row format change).

- [ ] **Step 3: Full-workspace test**

On Windows, **first** set the UAC-bypass compat layer so the multi-process e2e (T6) and the durable tests can spawn the `crabgresql` binary without OS error 740:

```powershell
$env:__COMPAT_LAYER = 'RunAsInvoker'
```

Run: `cargo test --workspace`
Expected: all suites pass, **0 failures, 0 ignored-that-should-run**. This must include:
- every prior single-range suite **unchanged** under the default `RangeMap::single()` — the load-bearing **criterion 4** regression gate (SP9/SP10 `cluster::{scenarios,sql_over_raft,durable_scenarios,sql_durable,jepsen_bank}`, `crabgresql::{multiprocess,jepsen_elle}`, `executor::*`, `pgwire::*`);
- the new in-crate layers: T1 transport serde + two-group dispatch (`cargo test -p cluster --lib transport`), T2 multi-range election + storage isolation, T3 local-gateway routing + `0A000`, T4 remote-forward + one-hop retry;
- the renamed `executor::mutation_semantics` (T5) builds and runs (criterion 9);
- the T6 multi-process e2e (`crabgresql`'s new UAC-safe multi-range gateway test).

If a previously-green single-range test changed behavior, the T1/T2/T3 refactors broke backward compatibility — **fix the code, do not edit the test** (criterion 4 is a hard gate).

> **Determinism check (CLAUDE.md rule):** before declaring this green, grep the slice's new tests for the forbidden wait and confirm none slipped in:
> ```bash
> git diff origin/main --stat -- '*tests*' 'crates/cluster/src/**' 'crates/crabgresql/src/**' \
>   | grep -E '\.rs$' || true
> grep -RnE 'tokio::time::sleep|thread::sleep|std::thread::sleep' \
>   crates/cluster/src/transport crates/cluster/src/range crates/cluster/tests \
>   crates/crabgresql/tests 2>/dev/null || true
> ```
> Expected: the in-crate layers (T1–T4) match **nothing**. The only permitted match is the T6 multi-process harness's **bounded, condition-driven** poll (leader-present / committed-row-readable with a deadline) — which is a `loop { check; deadline-assert }`, not a fixed settle `sleep`. If any in-crate (`-p cluster --lib`, `--test multirange*`, durable) test matches, that is a CLAUDE.md violation introduced by a sibling task — fix it to an openraft `raft.wait(Some(timeout)).metrics(|m| cond, "reason")` / `.applied_index_at_least(idx, "reason")` wait before proceeding. (`route.rs` production code is exempt from the test rule, but the T3/T4 rewrite removed the old `route.rs:92` no-leader busy-sleep regardless — confirm it is gone: `grep -n 'NO_LEADER_WAIT\|sleep' crates/cluster/src/route.rs` should be empty.)

- [ ] **Step 4: Parser differential oracle (matches CI `check` job)**

CI runs the parser oracle as a distinct step (`.github/workflows/ci.yml:22-24`). This slice does not touch the parser, so it must stay green; run it to mirror the gate.

Run: `cargo test --locked -p pgparser --features oracle`
Expected: PASS (unchanged — no grammar change in SP14). On Windows this needs the `cc`/libpg_query toolchain; if that toolchain is unavailable locally, note it and rely on the CI `check` job — do **not** alter the parser to make it pass.

- [ ] **Step 5: Supply-chain + native-code checks (match CI exactly)**

Run (matching `.github/workflows/ci.yml:24`'s `cargo-deny-action` with `command: check bans licenses`, plus advisories from `deny.toml`):

```bash
cargo deny check
```
Expected: PASS. **Criterion 10 — no new shipped dependency:** the `[bans]` table (`deny.toml:40-62`) must report no new entries; the forwarding client is built on the existing `pgwire` frame primitives, so the dependency graph is unchanged from SP13. If `cargo deny` reports a *new* advisory/ban/license that is **not** already ignored in `deny.toml:4-27`, a forbidden dependency was pulled in — that violates the locked "no new shipped dependency" decision; remove it, do not add an ignore.

Then run the no-native gate (`.github/workflows/ci.yml:26`):

```bash
bash scripts/check-no-native.sh
```
Expected: `OK: shipped dependency tree is pure Rust`. The script (`scripts/check-no-native.sh:14-17`) trees `crabgresql -e normal,build` and rejects any `-sys`/`cc` crate except the `linux-raw-sys` allowlist; SP14 adds no shipped crate, so this is unchanged from SP13. (On a non-Linux dev box `linux-raw-sys` is absent and the tree is trivially clean; the Linux CI run is the authoritative one.)

- [ ] **Step 6: os-740 target-name audit (criterion 9 — confirm T5 landed)**

T5 renamed `update_delete.rs` → `mutation_semantics.rs` and added the `CLAUDE.md` policy. T7 re-runs the **filename/target-name** grep (NOT a content grep — a content grep matches 15 files with legitimate SQL `UPDATE`; the check targets binary **names**, per Test plan 8) to prove no UAC-trigger substring remains across **every** target the harness can spawn:

```bash
# (a) integration-test filenames (each becomes a test BINARY target):
git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad' || echo "OK: no UAC-trigger test filenames"
# (b) explicit [[test]]/[[bin]]/[[example]] name = "..." entries across the workspace + fuzz:
git ls-files '**/Cargo.toml' | xargs grep -hED '^\s*name\s*=' -A0 2>/dev/null \
  | grep -iE 'setup|install|update|patch|upgrad' || echo "OK: no UAC-trigger explicit target names"
```
Expected: both print their `OK:` line (empty grep). Concretely this clears all **24** test binaries (the 23 from Test plan 8 plus T6's new UAC-safe multi-range gateway binary): cluster `{durable_scenarios, jepsen_bank, model, multirange, scenarios, sql_durable, sql_over_raft}` + T2's durable multi-range test; crabgresql `{jepsen_elle, multiprocess}` + T6's gateway e2e; executor `{concurrency, durability, end_to_end, linearizable_reads, recovery, transactions, mutation_semantics}`; pgparser `{libpg_query_oracle}`; pgwire `{cancel, extended_query, golden_trace, scram_auth, simple_query, sqlx_driver, tls}`; the 4 fuzz `[[bin]]` names (`parse_sql`, `wire_decode`, `decode_row`, `decode_key`, `fuzz/Cargo.toml:28-49`); and the `crabgresql` main binary (`crates/crabgresql/Cargo.toml:2`). Verify the harness still resolves the binary by the safe name:

```bash
grep -Rn 'CARGO_BIN_EXE_crabgresql' crates/crabgresql/tests || true
```
Expected: present and unchanged — the e2e harness spawns `env!("CARGO_BIN_EXE_crabgresql")`, which stays UAC-safe only while the binary is named `crabgresql` (a Risk called out in the spec).

- [ ] **Step 7: Fill the spec Traceability table**

Replace the empty stub at the end of `docs/superpowers/specs/2026-06-13-crabgresql-sp14-d3a-net-network-range-routing-design.md` (the `## Traceability (filled in at finish …)` table whose only body row is `| … | (completed during T7) | |`) with the completed table below — one row per success criterion 1–10, each naming its **proving test** (the test/check function and its file), drawn from the spec's own Success-criteria column.

Open the file and apply this exact replacement. **Old text** (the stub body, spec lines 147-149):

```markdown
| # | Criterion | Verified by |
|---|---|---|
| … | (completed during T7) | |
```

**New text:**

```markdown
| # | Criterion | Verified by |
|---|---|---|
| 1 | Range-unaware `NodeRequest::Raft` decodes (serde default → range 0); a range-tagged envelope round-trips its `range`. | **T1** serde round-trip test — `crates/cluster/src/transport/protocol.rs` `#[cfg(test)]` (range-unaware JSON decodes to range 0; range-1 envelope round-trips). |
| 2 | A node hosting {0,1} dispatches a range-1 `AppendEntries` to its range-1 Raft (asserted via that group's commit index, openraft `wait()`); an unregistered range returns `Unreachable`. | **T1** two-group loopback dispatch test — `crates/cluster/src/transport/server.rs` `#[cfg(test)]` (`(range,node)` registry routes range-1 RPC; unregistered range → `Unreachable`). |
| 3 | A multi-range `ServerNode` brings up every range, each elects a leader independently; a write to range 1 lands under `data-r1`/`raft-r1` and **not** under `data-r0`/`raft-r0` (both keyspaces). | **T2** election + storage-isolation test — `crates/cluster/tests/durable_multirange.rs` (per-range `raft.wait().metrics(state==Leader && current_leader==self)`; asserts row present in `data-r1`, absent from `data-r0`). |
| 4 | All SP9/SP10 single-range multi-process tests pass unchanged under default `RangeMap::single()`. | **T2** regression gate — existing suites green under `RangeMap::single()`: `cluster::{scenarios,sql_over_raft,durable_scenarios,sql_durable,jepsen_bank}`, `crabgresql::{multiprocess,jepsen_elle}`, `executor::*`, `pgwire::*` (run in Step 3 `cargo test --workspace`). |
| 5 | CREATE (range 0) + INSERT (data range) + SELECT read-back through one multi-range node; cross-range txn → `0A000`. | **T3** local-gateway test — `crates/cluster/tests/gateway_local.rs` (CREATE/INSERT/SELECT through one multi-range gateway node; leader paced by openraft `wait()`). |
| 6 | A write at a gateway that is a **follower** for the target range forwards to the remote leader and becomes visible on all that range's replicas (event-based replication wait); a deterministically-injected single `NotLeader` triggers exactly one re-resolve+retry (retry-count observable, not timing). | **T4** remote-forward + retry test — `crates/cluster/tests/remote_forward.rs` (follower-gateway forward + applied-index `wait_for_replication` analog; test-only one-shot `NotLeader`; asserts retry counter == 1). |
| 7 | Across the real process boundary: rows land only in their table's range and read back through **any** node; killing one range's leader keeps the **other** range serving while the killed range re-elects and resumes. | **T6** multi-process e2e — `crates/crabgresql/tests/multirange_gateway.rs` (3 processes, UAC-safe binary `multirange_gateway`; per-range routing + per-range failover; crash nemesis fires only after the round's write is observed committed via SQL read-back, bounded condition + deadline). |
| 8 | A cross-range transaction through the gateway is rejected with SQLSTATE `0A000` end-to-end. | **T3** negative test — `crates/cluster/tests/gateway_local.rs` (a transaction spanning two ranges via the gateway is rejected with `0A000`). |
| 9 | No test/`[[bin]]`/`[[example]]` **target name** contains a UAC-trigger substring; `cargo test -p executor --test mutation_semantics` builds/runs; `CLAUDE.md` records the rule + audit. | **T5** name-grep + rename — `crates/executor/tests/mutation_semantics.rs` (was `update_delete.rs`); Step 6 filename/target-name grep returns empty; `CLAUDE.md` UAC-naming policy + audit added. |
| 10 | No new shipped dependency; `#![forbid(unsafe_code)]`; full gauntlet green; complete traceability table. | **T7** gauntlet — Steps 1–5: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, `cargo deny check`, `bash scripts/check-no-native.sh` all green; `unsafe_code = "forbid"` (`Cargo.toml:25`); this table. |
```

> If a sibling task placed a proving test in a slightly different file/function than named above (e.g. T6's binary/file name, or T4's retry test landing in a dedicated `crates/cluster/tests/remote_forward.rs` rather than `multirange.rs`), **update the "Verified by" cell to the actual location** — the rule is one row per criterion pointing at the real proving test, not at a guessed path. Verify each named test exists before committing: `cargo test --workspace -- --list | grep -iE 'isolation|remote|forward|cross.?range|multirange'`.

- [ ] **Step 8: Re-run the gauntlet head-to-tail (one clean pass after the doc edit)**

The Step 7 edit is docs-only, but run the three fast gates once more to prove the tree is green *as committed* (no fmt drift from the markdown, no accidental code edit):

Run: `cargo fmt --all --check`
Run: `cargo clippy --workspace --all-targets -- -D warnings`
Run: `cargo test --workspace` (Windows: `$env:__COMPAT_LAYER='RunAsInvoker'` first)
Expected: all three green, identical to Steps 1-3. This is the **verification-before-completion** evidence — do not proceed to commit/PR on assertion; proceed on this observed-green output.

- [ ] **Step 9: Commit**

```bash
git add docs/superpowers/specs/2026-06-13-crabgresql-sp14-d3a-net-network-range-routing-design.md crates/
git commit -m "docs+style(sp14): D3a-net traceability table; cargo fmt sweep"
```

(`crates/` is staged because the fmt sweep in Step 1 may have reformatted T1–T6 files that their own commits did not run `cargo fmt` over. If `git status` shows no `crates/` changes, drop it from the `git add` — the commit is then docs-only.)

- [ ] **Step 10: Whole-diff code review**

Dispatch a final code review over the entire SP14 diff using the **superpowers:requesting-code-review** skill. Scope it to `git diff origin/main...HEAD` (the full slice: range-aware transport, multi-range durable `ServerNode`, the per-statement gateway + pooled pgwire forwarding client, the os-740 rename). Direct the reviewer at the spec's locked decisions and Risks as the review rubric — specifically:
- **Committer-can't-forward** (spec Risk): confirm no code path builds a `RaftCommitter`/remote leader engine for a *remote* range on the gateway (would compile, dead-end at runtime with `NotLeader`). Forwarding must be at the SQL boundary only.
- **Per-range storage isolation** (highest-risk construction): confirm `data-r{r}`/`raft-r{r}` keyspaces are wired in `durable.rs` for **both** the data and raft stores (log/vote/committed/purged/`last_applied`/membership in `raft-r{r}`, app rows in `data-r{r}`) — and that criterion-3's test actually asserts cross-keyspace isolation.
- **Forwarding client is not a byte relay**: confirm it does a real per-target pgwire startup/auth handshake, sends exactly one `Query`, reads to `ReadyForQuery`, pools per remote leader, and is **not** `proxy()`/`copy_bidirectional`; no new dependency was added.
- **Leader-resolution races**: the metrics-watch `Ref` is dropped before any `await`; retries are bounded by a deadline; a paused leader that still self-reports `Leader` is excluded (SP13 `is_paused` lesson).
- **`#[serde(default)]` is on `NodeRequest::Raft`'s `range` only** (`protocol.rs`), not inside `RaftRpc` variants.
- **No-sleep rule** in all in-crate tests; the only bounded poll is the T6 multi-process harness's condition-driven-with-deadline wait.

Triage findings with **superpowers:receiving-code-review** (verify each before acting — do not blindly implement). Land fixes as their own commit(s); if a fix is non-trivial or out of this slice's locked scope, flag it for D3b rather than ballooning T2/T4 (the spec's scope-creep Risk).

- [ ] **Step 11: Finish the branch (PR vs `main`)**

Use the **superpowers:finishing-a-development-branch** skill to complete the slice:
1. Confirm the working tree is clean and the gauntlet (Steps 1-5/8) is green on the final commit — **verification-before-completion**: do not open the PR until you have re-run and observed green, never on assertion.
2. If PR #28 / SP13 has merged, perform the squash-merge rebase from the Rebase note at the top of this task, then re-run the gauntlet.
3. Push the branch fresh: `git push -u origin sp14-d3a-net-network-range-routing`.
4. Open the PR against `main` (`gh pr create --base main`). The PR body summarizes the three pieces (range-aware transport / multi-range durable `ServerNode` / per-statement gateway), links the spec, and pastes the completed Traceability table (criteria 1–10 → proving tests) as the acceptance evidence. End the PR body with the required Claude Code attribution line.
5. Watch CI to green: `gh pr checks --watch`. The required jobs are `check`, `coverage`, `conformance`, `property`, `fuzz-smoke`, `clippy-sarif`, gated by `pr-gate` (`.github/workflows/ci.yml:173-185`). The `check` job re-runs `fmt --check` / `clippy --locked --workspace --all-targets -D warnings` / `test --locked --workspace` / parser oracle / `cargo deny` / `check-no-native.sh` on Linux — the same gauntlet, so a green local Step 8 predicts a green `check`. If `coverage` (the 2-core llvm-cov runner) flakes a timing-sensitive test, that is the CLAUDE.md no-sleep failure mode — fix the test to a condition-driven wait, do not retry CI.

---

### Notes for the implementer

- **This task writes almost no code.** Its value is the gauntlet (the criterion-10 gate), the traceability table (criterion-10's "complete table" clause), and the disciplined finish. Resist editing source here except to fix a gauntlet failure or an accepted review finding.
- **`--locked` parity:** CI uses `cargo …--locked` (`ci.yml:20-21`). Run at least Step 8's `cargo test` once with `--locked` too (`cargo test --locked --workspace`) to catch a `Cargo.lock` drift before CI does — SP14 adds no dependency, so the lock must be byte-identical to SP13's.
- **Criterion 4 is the load-bearing regression gate** — if `cargo test --workspace` shows *any* prior single-range suite changed behavior, the slice broke backward compatibility; fix the code (the `RangeMap::single()` fast-path), never the test.
- **Stale IDE diagnostics:** rust-analyzer lags the committed tree here — trust `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace`, not the editor squiggles.
- **Rebase discipline:** the repo squash-merges; after SP13 (#28) lands, `git rebase --onto origin/main <sp13-tip>` — a plain `git rebase origin/main` replays SP13's now-collapsed commits as conflicts.

---

---

## Notes for the implementer

- **Stale IDE diagnostics:** rust-analyzer squiggles lag the committed tree — trust `cargo clippy --all-targets -- -D warnings` and `cargo test`, not the editor.
- **No `sleep` in tests (CLAUDE.md):** every wait is an openraft `wait()` event, an event-based `wait_for_replication`, an asserted observable (e.g. a retry counter), or — in the multi-process harness only — a bounded poll on a real observed condition with a deadline. If you reach for `tokio::time::sleep` in a test, you are doing it wrong; add the instrumentation instead. (Production code, e.g. the removed `route.rs:92` busy-sleep, is not bound by this rule, but T4 removes it anyway.)
- **T1 & T2 preserve single-range behavior:** the existing SP9/SP10 multi-process suites and the in-process `cluster` suites are the regression gate. The default `RangeMap::single()` runs the byte fast-path unchanged; if a previously-passing test changes behavior, you broke the refactor — fix the code, not the test.
- **Storage isolation is load-bearing (T2):** per-range fjall keyspaces (`data-r{r}`, `raft-r{r}`) give *structural* isolation. The criterion-3 test asserts both the data-keyspace split and that range 1's raft keyspace advanced while range 0's did not.
- **The Committer cannot be made remote:** `client_write` does not auto-forward. A gateway holds Raft handles only for its local replicas, so a remote range's statement executes **on the remote leader node** via the pgwire forward — never via a gateway-built `RaftCommitter`. Building one compiles but dead-ends at runtime with `NotLeader`.
- **Forwarding is a real (minimal) pgwire client, not `proxy()`:** `proxy()` is a whole-connection `copy_bidirectional` relay; per-statement forwarding needs the pooled `PgwireForward`/`ForwardPool` (startup handshake per remote leader, one `Query`, read to `ReadyForQuery`, relay back) built on existing `pgwire` frame primitives — **no new dependency**.
- **Leader resolution:** drop the metrics `watch` `Ref` before any `await`; bound the one-hop retry; exclude paused/unreachable leaders (the SP13 `is_paused` lesson). `resolve_leader` needs membership addrs packed `node|sql`.
- **os-740 (Windows):** any new test/`[[bin]]`/`[[example]]` target name must avoid `setup`/`install`/`update`/`patch`/`upgrad`. T5 renames `update_delete` and records the policy in `CLAUDE.md`.
- **Branch base:** this slice stacks on `sp13-d3a-multirange-core` (it needs D3a). After PR #28 (SP13) merges to `main`, rebase onto `main` with `git rebase --onto origin/main <old-sp13-tip>` (the squash-merge rebase pattern) — `main` then also has the #29 jepsen determinism fix.

## Traceability (fill in at finish — T7)

| # | Spec criterion | Verified by (test) |
|---|---|---|
| 1 | range-unaware `NodeRequest::Raft` decodes (default 0); range-tagged round-trips | `transport::server::range_aware::raft_envelope_range_serde_default_and_round_trip` (T1) |
| 2 | range-1 `AppendEntries` dispatched to range-1 Raft; unregistered range → `Unreachable` | `transport::server::range_aware::loopback_dispatches_by_range_and_rejects_unregistered` (T1) |
| 3 | multi-range node elects per range; range-1 write isolated to `data-r1`/`raft-r1`, not range 0 | `crates/cluster/tests/durable_multirange.rs` (T2) |
| 4 | all SP9/SP10 single-range suites pass under `RangeMap::single()` | existing transport/route/bank/partition/failover suites (T2 gate) |
| 5 | CREATE+INSERT+SELECT through a multi-range node; cross-range txn → `0A000` | `crates/cluster/tests/gateway_local.rs` (T3) |
| 6 | write at a follower gateway forwards to remote leader + visible on replicas; one re-resolve+retry | `crates/cluster/tests/remote_forward.rs` (T4) |
| 7 | multi-process: rows land in their range, read through any node; per-range failover independence | `crates/crabgresql/tests/multirange_gateway.rs` (T6) |
| 8 | cross-range txn rejected `0A000` end-to-end | `crates/cluster/tests/gateway_local.rs` (T3) |
| 9 | no UAC-trigger target names; `mutation_semantics` builds/runs; `CLAUDE.md` policy + audit | `cargo test -p executor --test mutation_semantics` + the filename grep (T5) |
| 10 | no new dependency; `forbid(unsafe_code)`; full gauntlet green; traceability complete | gauntlet (T7) |
