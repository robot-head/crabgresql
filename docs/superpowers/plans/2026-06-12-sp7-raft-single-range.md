# SP7: Single-range Raft replication (openraft) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replicate the existing single-node SQL engine through one in-process openraft 0.9 Raft group so committed writes survive a node loss and a new leader takes over with no data loss or counter reuse — the SQL/MVCC/concurrency stack from SP1–SP6 running unchanged on top.

**Architecture:** A new `cluster` crate holds openraft adapters (in-memory log store + a state machine that wraps a `MemKv`), a controllable in-process network with a `Switchboard` for fault injection, and a test harness. The executor gains one async `Committer` write seam: a `LocalCommitter` (byte-for-byte SP6 behavior, keeps all 224 tests green) and a `RaftCommitter` that proposes through Raft. Counter (`next_xid`/`seq`) correctness across failover is handled by max-merge-on-apply + fold-into-batch + reseed-on-leadership. Verification: openraft's own storage `Suite`, deterministic fault-injection scenarios, a SQL-over-Raft e2e, a Jepsen-style bank workload checked by Stateright, and a focused Stateright model of the counter/durability invariants.

**Tech Stack:** Rust 2024, `openraft` 0.9 (pure Rust, MIT/Apache), `tokio`, `serde`, `async-trait`; dev-only `stateright` for consistency/model checking. No new native crates (shipped tree stays pure Rust).

**Spec:** `docs/superpowers/specs/2026-06-12-crabgresql-sp7-raft-single-range-design.md`

---

## File structure

```
Cargo.toml                                  # add cluster to members + workspace deps (openraft, async-trait, stateright)
crates/kv/src/store.rs                      # derive Serialize/Deserialize on WriteOp
crates/executor/src/commit.rs               # NEW: Committer trait + LocalCommitter
crates/executor/src/lib.rs                  # SqlEngine: committer field; persist_mode; SqlEngine::replicated()
crates/executor/src/session.rs              # route data/clog writes through committer; fold counter ops in replicated mode
crates/executor/src/procarray.rs            # PersistMode (Durable|Replicated); reseed_from_applied; begin_write no-persist in replicated
crates/executor/src/seq.rs                  # PersistMode; alloc returns (start, Option<WriteOp>); reseed_from_applied
crates/executor/src/exec.rs                 # execute_write folds the seq op into its returned batch
crates/cluster/Cargo.toml                   # NEW crate manifest
crates/cluster/src/lib.rs                   # NEW: re-exports
crates/cluster/src/types.rs                 # NEW: declare_raft_types!, WriteBatch, NodeId/Node
crates/cluster/src/store.rs                 # NEW: in-memory LogStore + StateMachineStore (max-merge apply, MemKv snapshot)
crates/cluster/src/network.rs               # NEW: Switchboard + RaftNetwork/Factory
crates/cluster/src/node.rs                  # NEW: Node { raft, sm_kv }; build + initialize
crates/cluster/src/committer.rs             # NEW: RaftCommitter (executor::Committer over raft.client_write)
crates/cluster/src/cluster.rs               # NEW: Cluster test harness (spin N, faults, leader())
crates/cluster/tests/scenarios.rs           # NEW: deterministic fault-injection scenarios
crates/cluster/tests/sql_over_raft.rs       # NEW: SQL e2e incl kill-leader-mid-workload
crates/cluster/tests/jepsen_bank.rs         # NEW: Jepsen-style bank workload + Stateright checker
crates/cluster/tests/model.rs               # NEW: Stateright model of counter/durability invariants
```

Task order (each ends workspace-green): scaffold crate + serde → openraft store (Suite-tested) → network + harness (happy path) → fault-injection scenarios → Committer seam (pure refactor) → counter folding/reseed → RaftCommitter + SQL-over-Raft e2e → Jepsen bank + Stateright checker → Stateright model → gauntlet.

**Constraints (all tasks):** `#![forbid(unsafe_code)]`; `expect("reason")` never bare `unwrap`; CI runs `cargo clippy --workspace --all-targets -- -D warnings`; no `std::sync::Mutex`/`MutexGuard` held across `.await`.

---

### Task 1: Scaffold the `cluster` crate + `WriteOp` serde

**Files:**
- Modify: `Cargo.toml` (workspace members + deps)
- Modify: `crates/kv/src/store.rs`
- Create: `crates/cluster/Cargo.toml`, `crates/cluster/src/lib.rs`

- [ ] **Step 1: Add the workspace member + dependencies.** In the root `Cargo.toml`, add `"crates/cluster"` to `[workspace] members`, and add to `[workspace.dependencies]`:

```toml
openraft = { version = "0.9", features = ["serde"] }
async-trait = "0.1"
stateright = "0.31"
cluster = { path = "crates/cluster" }
```

(Pin the **0.9** line — not a 0.10 alpha. `stateright` is dev-only; `async-trait` makes the `Committer` trait object-safe.)

- [ ] **Step 2: Derive serde on `WriteOp`.** In `crates/kv/src/store.rs`, the `WriteOp` enum must be serializable so it can be a Raft `AppData`. Add `serde` to `crates/kv/Cargo.toml` dependencies (`serde = { workspace = true }`) and change the derive:

```rust
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum WriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}
```

- [ ] **Step 3: Create the crate manifest.** `crates/cluster/Cargo.toml`:

```toml
[package]
name = "cluster"
version.workspace = true
edition.workspace = true
license.workspace = true

[lints]
workspace = true

[dependencies]
kv = { workspace = true }
executor = { workspace = true }
openraft = { workspace = true }
tokio = { workspace = true }
async-trait = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
stateright = { workspace = true }
proptest = { workspace = true }
```

- [ ] **Step 4: Minimal lib.** `crates/cluster/src/lib.rs`:

```rust
//! Single-range Raft replication for crabgresql (SP7 / distribution slice D1).
//! Wraps the SP1–SP6 engine in one in-process openraft group. In-memory and
//! ephemeral: no sockets, no on-disk Raft state, no restart recovery (all D2).

mod types;
mod store;
mod network;
mod node;
mod committer;
mod cluster;

pub use cluster::Cluster;
pub use committer::RaftCommitter;
pub use node::Node;
pub use types::{TypeConfig, WriteBatch};
```

(The `mod` lines reference files created in later tasks; create empty `// placeholder` stubs now — `types.rs` etc. each containing only a doc comment — so the crate compiles. Each later task fills its stub.)

- [ ] **Step 5: Stub the modules.** Create `crates/cluster/src/{types,store,network,node,committer,cluster}.rs`, each containing a one-line `//!` doc comment, so `lib.rs` compiles. Temporarily comment out `pub use` lines whose items don't exist yet (re-enable as each task lands), or gate behind the stubs.

- [ ] **Step 6: Verify build + purity gates.**

Run: `cargo build -p cluster && cargo build -p kv`
Expected: PASS.
Run: `bash scripts/check-no-native.sh`
Expected: `OK: shipped dependency tree is pure Rust` (the `crabgresql` binary does not yet depend on `cluster`, but confirm openraft added no native crate by also running `cargo tree -p cluster -e normal,build --prefix none | grep -E '(^cc$|-sys$)' | grep -vE '^linux-raw-sys$'` → no output).
Run: `cargo deny check`
Expected: PASS (openraft is MIT/Apache; no banned crate).

- [ ] **Step 7: Commit.**

```bash
git add Cargo.toml crates/kv crates/cluster
git commit -m "feat(cluster): scaffold crate + openraft dep; WriteOp serde"
```

---

### Task 2: openraft type config + in-memory log store + state machine

**Files:**
- Create/replace: `crates/cluster/src/types.rs`, `crates/cluster/src/store.rs`

The state machine wraps an `Arc<MemKv>`; `apply` writes each `WriteBatch` into it, **max-merging** the two counter keys so out-of-order log application cannot regress them. The log store is a standard in-memory `BTreeMap<u64, Entry>` + `Vote`. Both are validated by openraft's own storage conformance suite, so correctness does not depend on hand-reproducing every signature — match them against the pinned **openraft 0.9** docs and let the Suite gate it.

- [ ] **Step 1: Type config.** `crates/cluster/src/types.rs`:

```rust
//! openraft type configuration for crabgresql's single range.

use std::io::Cursor;

/// The replicated application command: one atomic KV write batch.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WriteBatch(pub Vec<kv::WriteOp>);

pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Single-range type config: AppData is a write batch, the response is unit.
    pub TypeConfig:
        D = WriteBatch,
        R = (),
        NodeId = NodeId,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);
```

(The exact `declare_raft_types!` field set is version-specific; consult `https://docs.rs/openraft/0.9.24` and adapt if 0.9 wants fewer/more fields. The Suite test in Step 4 fails loudly if the config is wrong.)

- [ ] **Step 2: State machine + log store.** `crates/cluster/src/store.rs`. Model the boilerplate on openraft 0.9's `examples/raft-kv-memstore/src/store/mod.rs` (in-memory `StateMachineStore` + `LogStore`), with **two crabgresql-specific changes**:

  1. The state machine data is an `Arc<MemKv>` (not a `BTreeMap<String,String>`), shared so the SQL engine reads it directly.
  2. `apply` routes each `WriteOp` through a `apply_op` helper that **max-merges the counter keys**:

```rust
use std::sync::Arc;
use kv::{Kv, MemKv, WriteOp};

/// Apply one op to the state-machine store. The two monotonic counter keys
/// (`next_xid`, any table's `seq`) take the MAX of the existing and incoming
/// big-endian u64 so out-of-order Raft application never regresses them; every
/// other key is a plain put/delete.
fn apply_op(kv: &MemKv, op: &WriteOp) {
    match op {
        WriteOp::Put { key, value } if is_counter_key(key) => {
            let incoming = u64_be(value);
            let existing = kv.get(key).expect("memkv get").map(|b| u64_be(&b)).unwrap_or(0);
            let merged = existing.max(incoming);
            kv.put(key.clone(), merged.to_be_bytes().to_vec()).expect("memkv put");
        }
        WriteOp::Put { key, value } => { kv.put(key.clone(), value.clone()).expect("memkv put"); }
        WriteOp::Delete { key } => { kv.delete(key).expect("memkv delete"); }
    }
}

/// True for `/0/meta/next_xid` and any `/0/seq/<table>` key.
fn is_counter_key(key: &[u8]) -> bool {
    key == kv::key::next_xid_key().as_slice() || is_seq_key(key)
}

fn is_seq_key(key: &[u8]) -> bool {
    // `/0/seq/<u32>` — table id varies, so compare the constant prefix.
    let prefix = kv::key::seq_key(0);
    let plen = prefix.len() - 4; // drop the 4-byte table-id suffix of seq_key(0)
    key.len() == prefix.len() && key[..plen] == prefix[..plen]
}

fn u64_be(b: &[u8]) -> u64 {
    let a: [u8; 8] = b.try_into().expect("counter value is u64");
    u64::from_be_bytes(a)
}
```

  In the `apply` method, for each `Entry`'s `EntryPayload::Normal(WriteBatch(ops))`, call `apply_op(&self.sm_kv, op)` for each op, advance `last_applied_log`, and push `()` to the response vec. For `EntryPayload::Membership`/`Blank`, just advance `last_applied`. `build_snapshot` serializes `sm_kv.scan_prefix(&[])` (all pairs) + `last_applied`/membership into the `Cursor<Vec<u8>>` via `serde_json`; `install_snapshot` clears `sm_kv` and re-puts the deserialized pairs.

  Expose `pub fn sm_kv(&self) -> Arc<MemKv>` on the state-machine store so the node can hand the engine the read store.

- [ ] **Step 3: Failing Suite test.** Add to `crates/cluster/src/store.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// openraft's own storage conformance suite over our log store + state machine.
    #[tokio::test]
    async fn openraft_storage_suite() {
        openraft::testing::Suite::test_all(|| async {
            // return (LogStore, StateMachineStore) freshly constructed
            Ok((LogStore::default(), StateMachineStore::default()))
        })
        .await
        .expect("openraft storage suite passes");
    }
}
```

Run: `cargo test -p cluster openraft_storage_suite`
Expected: initially COMPILE FAIL, then PASS once the impls are complete. (Adapt the builder closure signature to the exact `Suite::test_all` shape in 0.9.)

- [ ] **Step 4: max-merge + snapshot unit tests.** Add:

```rust
#[test]
fn counter_keys_max_merge_never_regress() {
    let kv = MemKv::new();
    let k = kv::key::next_xid_key();
    apply_op(&kv, &WriteOp::Put { key: k.clone(), value: 12u64.to_be_bytes().to_vec() });
    apply_op(&kv, &WriteOp::Put { key: k.clone(), value: 11u64.to_be_bytes().to_vec() }); // out of order
    assert_eq!(u64_be(&kv.get(&k).expect("get").expect("present")), 12, "max-merge must not regress");
}

#[test]
fn non_counter_keys_are_last_writer_wins() {
    let kv = MemKv::new();
    let k = kv::key::row_key(1, 1);
    apply_op(&kv, &WriteOp::Put { key: k.clone(), value: b"a".to_vec() });
    apply_op(&kv, &WriteOp::Put { key: k.clone(), value: b"b".to_vec() });
    assert_eq!(kv.get(&k).expect("get"), Some(b"b".to_vec()));
}
```

Run: `cargo test -p cluster -- counter_keys_max_merge non_counter_keys`
Expected: PASS.

- [ ] **Step 5: clippy + commit.**

```bash
cargo clippy -p cluster --all-targets -- -D warnings
git add crates/cluster/src/types.rs crates/cluster/src/store.rs
git commit -m "feat(cluster): openraft type config + in-memory log/state-machine (max-merge counters)"
```

---

### Task 3: In-process network, Node, and Cluster harness (happy path)

**Files:**
- Create/replace: `crates/cluster/src/network.rs`, `crates/cluster/src/node.rs`, `crates/cluster/src/cluster.rs`

The `Switchboard` is a shared registry of node Raft handles plus mutable fault state; `RaftNetwork` routes `vote`/`append_entries`/`full_snapshot` through it, dropping anything the fault state blocks. `Node` bundles a `Raft` + the state machine's `sm_kv`. `Cluster` spins N nodes, initializes the group, and exposes leader lookup + fault controls.

- [ ] **Step 1: Switchboard + network.** `crates/cluster/src/network.rs`:

```rust
//! Controllable in-process Raft transport. All RPCs go through the Switchboard,
//! which can drop them to model partitions and paused (crashed) nodes.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use openraft::error::{InstallSnapshotError, RaftError};
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::BasicNode;

use crate::types::{NodeId, TypeConfig};

#[derive(Default)]
struct Faults {
    /// Nodes that drop all inbound/outbound RPCs (modeling a crash/pause).
    paused: HashSet<NodeId>,
    /// Unordered pair {a,b} is cut if present (modeling a partition).
    cuts: HashSet<(NodeId, NodeId)>,
}

#[derive(Clone, Default)]
pub struct Switchboard {
    handles: Arc<Mutex<HashMap<NodeId, openraft::Raft<TypeConfig>>>>,
    faults: Arc<Mutex<Faults>>,
}

impl Switchboard {
    pub fn new() -> Self { Self::default() }
    pub fn register(&self, id: NodeId, raft: openraft::Raft<TypeConfig>) {
        self.handles.lock().expect("sb").insert(id, raft);
    }
    pub fn pause(&self, id: NodeId) { self.faults.lock().expect("f").paused.insert(id); }
    pub fn resume(&self, id: NodeId) { self.faults.lock().expect("f").paused.remove(&id); }
    pub fn cut(&self, a: NodeId, b: NodeId) { self.faults.lock().expect("f").cuts.insert(norm(a, b)); }
    pub fn heal(&self) { self.faults.lock().expect("f").cuts.clear(); self.faults.lock().expect("f").paused.clear(); }

    fn blocked(&self, from: NodeId, to: NodeId) -> bool {
        let f = self.faults.lock().expect("f");
        f.paused.contains(&from) || f.paused.contains(&to) || f.cuts.contains(&norm(from, to))
    }
    fn handle(&self, to: NodeId) -> Option<openraft::Raft<TypeConfig>> {
        self.handles.lock().expect("sb").get(&to).cloned()
    }
}

fn norm(a: NodeId, b: NodeId) -> (NodeId, NodeId) { if a <= b { (a, b) } else { (b, a) } }

/// A network client from `from` to `target`, routing through the Switchboard.
pub struct Conn { sb: Switchboard, from: NodeId, target: NodeId }

impl RaftNetworkFactory<TypeConfig> for Switchboard {
    type Network = Conn;
    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        // `from` is the owning node; the factory is per-node (see node.rs wiring).
        Conn { sb: self.clone(), from: self.from_hint(), target }
    }
}
```

(Note: openraft's `RaftNetworkFactory` is owned per node, so give the per-node factory its own `from` id — either a thin `NodeFactory { sb, from }` wrapper implementing `RaftNetworkFactory` or store `from` on a cloned Switchboard. Implement `RaftNetwork for Conn`'s `append_entries`, `vote`, and `install_snapshot`/`full_snapshot` to call `self.sb.handle(self.target)`'s corresponding `Raft` method, returning a `Unreachable` network error when `self.sb.blocked(self.from, self.target)` or the handle is absent. Match the exact method names/error types against openraft 0.9 `RaftNetwork`.)

- [ ] **Step 2: Node.** `crates/cluster/src/node.rs`:

```rust
//! One replica: a Raft instance plus its applied state-machine store.

use std::sync::Arc;
use kv::MemKv;
use crate::network::Switchboard;
use crate::store::{LogStore, StateMachineStore};
use crate::types::{NodeId, TypeConfig};

pub struct Node {
    pub id: NodeId,
    pub raft: openraft::Raft<TypeConfig>,
    pub sm_kv: Arc<MemKv>,
}

impl Node {
    /// Build a node (not yet a cluster member). `sb` is the shared transport.
    pub async fn start(id: NodeId, sb: Switchboard) -> Self {
        let config = Arc::new(
            openraft::Config {
                // short timers keep tests fast and deterministic-in-outcome
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                ..Default::default()
            }
            .validate()
            .expect("valid raft config"),
        );
        let log = LogStore::default();
        let sm = StateMachineStore::default();
        let sm_kv = sm.sm_kv();
        let raft = openraft::Raft::new(id, config, sb.for_node(id), log, sm)
            .await
            .expect("raft::new");
        sb.register(id, raft.clone());
        Node { id, raft, sm_kv }
    }
}
```

(`sb.for_node(id)` returns the per-node network factory carrying `from = id`; add it to `network.rs`.)

- [ ] **Step 3: Cluster harness + happy-path test.** `crates/cluster/src/cluster.rs`:

```rust
//! In-process N-node cluster for tests: build, initialize, find the leader,
//! and inject faults via the Switchboard.

use std::collections::BTreeMap;
use std::time::Duration;
use openraft::BasicNode;
use crate::network::Switchboard;
use crate::node::Node;
use crate::types::{NodeId, TypeConfig, WriteBatch};

pub struct Cluster { pub nodes: Vec<Node>, pub sb: Switchboard }

impl Cluster {
    /// Build `n` nodes and initialize a single voting group {0..n}.
    pub async fn new(n: u64) -> Self {
        let sb = Switchboard::new();
        let mut nodes = Vec::new();
        for id in 0..n { nodes.push(Node::start(id, sb.clone()).await); }
        let members: BTreeMap<NodeId, BasicNode> =
            (0..n).map(|id| (id, BasicNode::default())).collect();
        nodes[0].raft.initialize(members).await.expect("initialize");
        Self { nodes, sb }
    }

    pub fn node(&self, id: NodeId) -> &Node { &self.nodes[id as usize] }

    /// Await a stable leader and return its id.
    pub async fn wait_for_leader(&self) -> NodeId {
        for n in &self.nodes {
            if let Ok(m) = n.raft
                .wait(Some(Duration::from_secs(10)))
                .metrics(|m| m.current_leader.is_some(), "leader elected")
                .await
            { if let Some(l) = m.current_leader { return l; } }
        }
        panic!("no leader elected");
    }

    pub fn leader(&self) -> Option<&Node> {
        let id = self.nodes[0].raft.metrics().borrow().current_leader?;
        Some(self.node(id))
    }

    pub fn pause(&self, id: NodeId) { self.sb.pause(id); }
    pub fn resume(&self, id: NodeId) { self.sb.resume(id); }
    pub fn isolate(&self, id: NodeId) { for o in 0..self.nodes.len() as u64 { if o != id { self.sb.cut(id, o); } } }
    pub fn heal(&self) { self.sb.heal(); }

    /// Propose a raw write batch on the current leader.
    pub async fn write(&self, ops: Vec<kv::WriteOp>) -> Result<(), String> {
        let leader = self.leader().ok_or("no leader")?;
        leader.raft.client_write(WriteBatch(ops)).await.map_err(|e| e.to_string())?;
        Ok(())
    }
}
```

- [ ] **Step 4: Happy-path test.** `crates/cluster/tests/scenarios.rs`:

```rust
use cluster::Cluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_replicates_to_all_replicas() {
    let c = Cluster::new(3).await;
    let leader = c.wait_for_leader().await;
    let k = kv::key::row_key(1, 1);
    c.write(vec![kv::WriteOp::Put { key: k.clone(), value: b"v".to_vec() }]).await.expect("write");

    // Every node's applied state machine eventually has the key.
    for id in 0..3u64 {
        c.node(id).raft
            .wait(Some(std::time::Duration::from_secs(10)))
            .applied_index_at_least(Some(2), "applied the write")
            .await
            .expect("apply");
        assert_eq!(c.node(id).sm_kv.get(&k).expect("get"), Some(b"v".to_vec()),
            "node {id} must have replicated the write");
    }
    let _ = leader;
}
```

Run: `cargo test -p cluster --test scenarios write_replicates_to_all_replicas`
Expected: PASS (no hang).

- [ ] **Step 5: clippy + commit.**

```bash
cargo clippy -p cluster --all-targets -- -D warnings
git add crates/cluster/src/network.rs crates/cluster/src/node.rs crates/cluster/src/cluster.rs crates/cluster/tests/scenarios.rs
git commit -m "feat(cluster): in-process network + Switchboard + Cluster harness; replication test"
```

---

### Task 4: Deterministic fault-injection scenarios

**Files:**
- Modify: `crates/cluster/tests/scenarios.rs`

All scenarios assert via `Raft::wait` (await metrics conditions), never `sleep`. Add a small helper in the test file:

```rust
async fn applied_to(node: &cluster::Node, idx: u64) {
    node.raft.wait(Some(std::time::Duration::from_secs(10)))
        .applied_index_at_least(Some(idx), "catch up")
        .await.expect("applied");
}
```

- [ ] **Step 1: follower catch-up after pause.** Pause one follower, commit several writes on the leader, resume → it catches up.

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_follower_catches_up_on_resume() {
    let c = Cluster::new(3).await;
    let leader = c.wait_for_leader().await;
    let follower = (0..3u64).find(|&i| i != leader).expect("a follower");
    c.pause(follower);
    for i in 0..5 {
        c.write(vec![kv::WriteOp::Put { key: kv::key::row_key(1, i), value: vec![i as u8] }]).await.expect("write");
    }
    c.resume(follower);
    applied_to(c.node(follower), 6).await; // 5 writes + initialize entry
    assert_eq!(c.node(follower).sm_kv.get(&kv::key::row_key(1, 4)).expect("get"), Some(vec![4]));
}
```

- [ ] **Step 2: leader failover with no counter reuse.** Seed the `next_xid` counter, isolate the leader, a new leader is elected, and the next allocation is above the old high-water mark.

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolating_leader_elects_new_leader_no_xid_reuse() {
    let c = Cluster::new(3).await;
    let l0 = c.wait_for_leader().await;
    // Advance next_xid to 10 through the log.
    c.write(vec![kv::WriteOp::Put { key: kv::key::next_xid_key(), value: 10u64.to_be_bytes().to_vec() }]).await.expect("seed");
    c.isolate(l0);
    // A new leader must emerge among the remaining two.
    let l1 = loop {
        if let Some(n) = c.leader() && n.id != l0 { break n.id; }
        tokio::task::yield_now().await;
        // bounded: wait_for_leader on the majority side
        let _ = c.node((0..3).find(|&i| i != l0).unwrap()).raft
            .wait(Some(std::time::Duration::from_secs(10)))
            .metrics(|m| m.current_leader.map_or(false, |x| x != l0), "new leader")
            .await.expect("new leader");
    };
    // The new leader's applied next_xid is still >= 10 (durable through Raft).
    let v = c.node(l1).sm_kv.get(&kv::key::next_xid_key()).expect("get").expect("present");
    assert!(u64::from_be_bytes(v.try_into().unwrap()) >= 10, "counter must not regress across failover");
}
```

- [ ] **Step 3: partition minority cannot commit.** Isolate the leader; a write proposed on the isolated (now minority) old leader fails; the majority side serves.

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minority_partition_cannot_commit() {
    let c = Cluster::new(3).await;
    let l0 = c.wait_for_leader().await;
    c.isolate(l0);
    // The isolated old leader cannot reach a majority: its client_write errors.
    let r = c.node(l0).raft
        .client_write(cluster::WriteBatch(vec![kv::WriteOp::Put { key: kv::key::row_key(1, 9), value: vec![9] }]))
        .await;
    assert!(r.is_err(), "minority leader must not commit");
    c.heal();
}
```

- [ ] **Step 4: snapshot install for a far-behind node.** Configure log purging (`Config { max_in_snapshot_log_to_keep: 0, snapshot_policy: LogsSinceLast(1), .. }` in `Node::start` — or set it cluster-wide for this test via a `Cluster::new_with_config`), keep a follower paused while many writes + a snapshot happen, resume → it catches up via `install_snapshot`. Assert the follower's `sm_kv` matches and its metrics show a snapshot was installed.

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn far_behind_follower_recovers_via_snapshot() {
    let c = Cluster::new_with_snapshotting(3).await; // small snapshot threshold
    let leader = c.wait_for_leader().await;
    let follower = (0..3u64).find(|&i| i != leader).expect("a follower");
    c.pause(follower);
    for i in 0..20 {
        c.write(vec![kv::WriteOp::Put { key: kv::key::row_key(1, i), value: vec![i as u8] }]).await.expect("write");
    }
    c.resume(follower);
    applied_to(c.node(follower), 21).await;
    assert_eq!(c.node(follower).sm_kv.get(&kv::key::row_key(1, 19)).expect("get"), Some(vec![19]));
}
```

- [ ] **Step 5: run all scenarios.**

Run: `cargo test -p cluster --test scenarios`
Expected: all PASS, no hangs. (If a test hangs, a fault was not cleared — every test that pauses/cuts must `resume`/`heal` before relying on quorum, and `wait` calls must target the side that has quorum.)

- [ ] **Step 6: clippy + commit.**

```bash
cargo clippy -p cluster --all-targets -- -D warnings
git add crates/cluster/src/cluster.rs crates/cluster/tests/scenarios.rs
git commit -m "test(cluster): deterministic fault-injection scenarios (catch-up, failover, partition, snapshot)"
```

---

### Task 5: `Committer` write seam in the executor (pure refactor)

**Files:**
- Create: `crates/executor/src/commit.rs`
- Modify: `crates/executor/src/lib.rs`, `crates/executor/src/session.rs`

Introduce the async write seam and route the session's **data/clog** batches through it. The local impl is `kv.write_batch`, so behavior is identical and all 224 tests pass. Counter persists are still self-handled by `ProcArray`/`SequenceManager` in this task (folding lands in Task 6).

- [ ] **Step 1: the trait + local impl.** `crates/executor/src/commit.rs`:

```rust
//! The durable-write seam. SP6 wrote one batch via `Kv::write_batch`; SP7 routes
//! those batches through a `Committer` so a replicated engine can propose them
//! through Raft instead. The local impl is byte-for-byte the SP6 write.

use std::sync::Arc;
use kv::{Kv, WriteOp};
use crate::error::ExecError;

#[async_trait::async_trait]
pub trait Committer: Send + Sync {
    /// Durably apply one atomic batch. Returns only once the batch is durable
    /// (local: written; replicated: committed to a majority AND applied).
    async fn commit(&self, ops: Vec<WriteOp>) -> Result<(), ExecError>;
}

/// Single-node committer: writes straight to the local KV (SP6 behavior).
pub struct LocalCommitter {
    pub(crate) kv: Arc<dyn Kv>,
}

#[async_trait::async_trait]
impl Committer for LocalCommitter {
    async fn commit(&self, ops: Vec<WriteOp>) -> Result<(), ExecError> {
        self.kv.write_batch(&ops)?;
        Ok(())
    }
}
```

Add `mod commit;` and `pub use commit::Committer;` to `crates/executor/src/lib.rs`, and `async-trait = { workspace = true }` to `crates/executor/Cargo.toml`.

- [ ] **Step 2: engine holds a committer.** In `crates/executor/src/lib.rs`, add to `SqlEngine`:

```rust
    pub(crate) committer: Arc<dyn crate::commit::Committer>,
```

and in `with_kv`, default it to the local committer:

```rust
    let committer: Arc<dyn crate::commit::Committer> =
        Arc::new(crate::commit::LocalCommitter { kv: Arc::clone(&kv) });
    Ok(Self { kv, procarray, seq, lockmgr, catalog_lock, committer })
```

Pass `Arc::clone(&self.committer)` into `SqlSession::new` in `connect`.

- [ ] **Step 3: session holds + uses the committer.** In `crates/executor/src/session.rs`, add `committer: Arc<dyn crate::commit::Committer>` to `SqlSession` and `new`. Replace the **data/clog** write sites with `self.committer.commit(...).await`:
  - `run_write` in-txn (line ~331): `self.committer.commit(ops).await?;`
  - `run_write` autocommit (line ~369): `self.committer.commit(ops).await` (capture result, then `finish`/`release_all`, then `?`).
  - `commit_cmd` clog (line ~152), `abort_ctx` clog (line ~111), autocommit-error clog (line ~359): these are sync methods. Make `commit_cmd`/`rollback_cmd`/`abort_ctx` `async` (they are called from `run_one`, already async) and route their single-op clog writes through `self.committer.commit(vec![mvcc::clog::put_op(xid, ...)]).await`. Update `run_one`'s match arms to `.await` them.

  Keep the `procarray.finish` / `lockmgr.release_all` ordering exactly as today (deregister before propagating a commit error). `Drop` stays sync (no committer call — a dropped session's uncommitted writes are already invisible).

- [ ] **Step 4: regression gate.**

Run: `cargo test -p executor`
Expected: all existing executor tests PASS (the local committer is exactly `kv.write_batch`).
Run: `cargo test --workspace`
Expected: all 224 tests PASS.

- [ ] **Step 5: fmt + clippy + commit.**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add crates/executor
git commit -m "refactor(executor): route durable writes through a Committer seam (LocalCommitter)"
```

---

### Task 6: Counter folding + reseed-on-leadership for the replicated path

**Files:**
- Modify: `crates/executor/src/procarray.rs`, `crates/executor/src/seq.rs`, `crates/executor/src/exec.rs`, `crates/executor/src/session.rs`, `crates/executor/src/lib.rs`

On the replicated path, `next_xid`/`seq` must **not** be persisted separately (a failover between "rows committed" and "counter bumped" would reuse an id). Instead, allocate in-memory and **fold the counter op into the same batch** as the write that triggered the allocation; the state machine max-merges (Task 2); a node **reseeds** its in-memory counters from the applied store when it becomes leader. Local (durable) behavior is unchanged.

- [ ] **Step 1: PersistMode on the managers.** In `crates/executor/src/lib.rs` add:

```rust
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum PersistMode { Durable, Replicated }
```

Thread it into `ProcArray::open(kv, mode)` and `SequenceManager::new(mode)`; the existing constructors pass `PersistMode::Durable`. `SqlEngine::with_kv` uses `Durable`.

- [ ] **Step 2: ProcArray — no self-persist in Replicated; reseed.** In `crates/executor/src/procarray.rs`, store `mode: PersistMode`. In `begin_write`, only persist when `Durable`:

```rust
pub fn begin_write(&self) -> Result<u64, ExecError> {
    let mut g = self.inner.lock().expect("procarray");
    let xid = g.next_xid;
    let new_next = xid + 1;
    if self.mode == PersistMode::Durable {
        self.kv.write_batch(&[kv::WriteOp::Put {
            key: kv::key::next_xid_key(),
            value: new_next.to_be_bytes().to_vec(),
        }])?;
    }
    g.next_xid = new_next;
    g.running.insert(xid);
    Ok(xid)
}
```

Add a reseed and an op accessor:

```rust
/// Reseed the in-memory counter from the applied store (called when this node
/// becomes leader, so it never hands out an xid the old leader already used).
pub fn reseed_from_applied(&self) -> Result<(), ExecError> {
    let durable = match self.kv.get(&kv::key::next_xid_key())? {
        Some(b) => u64::from_be_bytes(b.as_slice().try_into()
            .map_err(|_| kv::KvError::CorruptRow("next_xid not u64".into()))?),
        None => 1,
    };
    let mut g = self.inner.lock().expect("procarray");
    g.next_xid = g.next_xid.max(durable.max(1));
    Ok(())
}

/// The WriteOp that records the current next_xid (folded into the commit batch
/// in Replicated mode).
pub fn next_xid_op(&self) -> kv::WriteOp {
    let next = self.inner.lock().expect("procarray").next_xid;
    kv::WriteOp::Put { key: kv::key::next_xid_key(), value: next.to_be_bytes().to_vec() }
}
```

- [ ] **Step 3: SequenceManager — return the op; no self-persist in Replicated.** In `crates/executor/src/seq.rs`, store `mode`, and change `alloc` to return the op when replicated:

```rust
pub fn alloc(&self, kv: &dyn Kv, table: catalog::TableId, count: u64)
    -> Result<(u64, Option<kv::WriteOp>), ExecError>
{
    let mut g = self.inner.lock().expect("seqmgr");
    let next = match g.get(&table) { Some(&n) => n, None => crate::exec::read_seq_kv(kv, table)? };
    let new_next = next + count;
    let op = kv::WriteOp::Put { key: kv::key::seq_key(table), value: new_next.to_be_bytes().to_vec() };
    let folded = match self.mode {
        PersistMode::Durable => { kv.write_batch(std::slice::from_ref(&op))?; None }
        PersistMode::Replicated => Some(op),
    };
    g.insert(table, new_next);
    Ok((next, folded))
}

/// Reseed every known table counter is unnecessary — counters seed lazily from
/// `read_seq_kv` on first use, which reads the applied store. So on leadership
/// change just clear the cache so the next alloc re-seeds from applied state.
pub fn reseed_from_applied(&self) {
    self.inner.lock().expect("seqmgr").clear();
}
```

Update `seq.rs` unit tests: `alloc(...)` now returns a tuple — destructure `let (start, _op) = seq.alloc(...)?;`. Construct managers with `SequenceManager::new(PersistMode::Durable)`.

- [ ] **Step 4: exec.rs folds the seq op.** At the INSERT site (`crates/executor/src/exec.rs:112`), change:

```rust
let (start, seq_op) = seq.alloc(kv, t.id, n_rows)?;
// ... build row ops as before, using `start` ...
if let Some(op) = seq_op { ops.push(op); } // fold (Replicated); no-op for Durable
```

- [ ] **Step 5: session folds the next_xid op (Replicated).** In `crates/executor/src/session.rs`, when a write statement allocated a **fresh** xid in Replicated mode, append `self.procarray.next_xid_op()` to the batch before `committer.commit`. The session knows the mode (store `persist_mode: PersistMode` on `SqlSession`, passed from the engine). Concretely, in `run_write` (both in-txn and autocommit arms) after obtaining `ops` and before committing:

```rust
if self.persist_mode == PersistMode::Replicated {
    ops.push(self.procarray.next_xid_op());
}
```

(For an in-txn second write the xid is not fresh, but re-folding the same `next_xid` value is harmless — max-merge keeps it monotonic. Pushing it on every replicated write statement is correct and simplest.)

- [ ] **Step 6: ProcArray unit test for reseed.** Add to `procarray.rs`:

```rust
#[test]
fn replicated_begin_write_does_not_persist_but_reseed_lifts_counter() {
    let kv = Arc::new(MemKv::new());
    let pa = ProcArray::open(Arc::clone(&kv) as Arc<dyn Kv>, PersistMode::Replicated).expect("open");
    assert_eq!(pa.begin_write().expect("bw"), 1);
    // Nothing persisted (replicated mode folds via the batch, not here).
    assert!(kv.get(&kv::key::next_xid_key()).expect("get").is_none());
    // Simulate the applied store advancing to 50 (via Raft), then becoming leader.
    kv.put(kv::key::next_xid_key(), 50u64.to_be_bytes().to_vec()).expect("put");
    pa.reseed_from_applied().expect("reseed");
    assert_eq!(pa.begin_write().expect("bw"), 50, "reseed lifts the counter above applied");
}
```

Run: `cargo test -p executor procarray && cargo test -p executor seq`
Expected: PASS.

- [ ] **Step 7: regression gate.**

Run: `cargo test --workspace`
Expected: all 224 tests PASS (Durable mode unchanged: `alloc` still self-persists and returns `None`, so nothing folds; `begin_write` still persists).

- [ ] **Step 8: fmt + clippy + commit.**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add crates/executor
git commit -m "feat(executor): replicated counter folding + reseed-on-leadership (Durable path unchanged)"
```

---

### Task 7: `RaftCommitter` + `SqlEngine::replicated`; SQL-over-Raft e2e

**Files:**
- Create/replace: `crates/cluster/src/committer.rs`
- Modify: `crates/executor/src/lib.rs` (add `SqlEngine::replicated`)
- Create: `crates/cluster/tests/sql_over_raft.rs`

- [ ] **Step 1: RaftCommitter.** `crates/cluster/src/committer.rs`:

```rust
//! A Committer that proposes batches through Raft. Resolving == committed+applied.

use executor::{Committer, ExecError};
use kv::WriteOp;
use crate::types::{TypeConfig, WriteBatch};

pub struct RaftCommitter { pub(crate) raft: openraft::Raft<TypeConfig> }

#[async_trait::async_trait]
impl Committer for RaftCommitter {
    async fn commit(&self, ops: Vec<WriteOp>) -> Result<(), ExecError> {
        self.raft.client_write(WriteBatch(ops)).await
            .map_err(|e| match e {
                // not the leader -> retryable
                openraft::error::RaftError::APIError(
                    openraft::error::ClientWriteError::ForwardToLeader(_)) => ExecError::NotLeader,
                // no quorum / fatal -> unavailable (no partial state was applied)
                _ => ExecError::Unavailable,
            })?;
        Ok(())
    }
}
```

(Adapt the exact error variant paths to openraft 0.9. The two `ExecError` variants are added next.)

- [ ] **Step 2: error variants.** In `crates/executor/src/error.rs` add to `ExecError`:

```rust
    /// The write hit a node that is not the Raft leader; the client should retry.
    NotLeader,
    /// The write could not reach a majority (partition/timeout); no partial state
    /// was applied; the client should retry.
    Unavailable,
```

and to `into_pg`:

```rust
    ExecError::NotLeader => PgError::error("40001", "could not complete: not the leader, retry"),
    ExecError::Unavailable => PgError::error("08006", "connection failure: no quorum"),
```

Make `ExecError`, `Committer`, `LocalCommitter` public from `executor` (`pub use commit::{Committer, LocalCommitter};`).

- [ ] **Step 3: `SqlEngine::replicated`.** In `crates/executor/src/lib.rs`:

```rust
impl SqlEngine {
    /// Build an engine whose reads come from `sm_kv` (the applied state machine)
    /// and whose writes are proposed through `committer` (a RaftCommitter). Uses
    /// the Replicated persist mode so counters fold into the proposed batch.
    pub fn replicated(
        sm_kv: Arc<dyn Kv>,
        committer: Arc<dyn crate::commit::Committer>,
    ) -> Result<Self, ExecError> {
        let procarray = Arc::new(ProcArray::open(Arc::clone(&sm_kv), PersistMode::Replicated)?);
        Ok(Self {
            kv: sm_kv,
            procarray,
            seq: Arc::new(SequenceManager::new(PersistMode::Replicated)),
            lockmgr: Arc::new(RowLockManager::new()),
            catalog_lock: Arc::new(std::sync::Mutex::new(())),
            committer,
        })
    }

    /// Reseed counters from the applied store (call when this node becomes leader).
    pub fn reseed_counters(&self) -> Result<(), ExecError> {
        self.procarray.reseed_from_applied()?;
        self.seq.reseed_from_applied();
        Ok(())
    }
}
```

Make `SqlSession::new` receive `persist_mode`; `connect` passes the engine's mode. Add a `cluster` helper `Node::engine(&self) -> SqlEngine` that builds `SqlEngine::replicated(self.sm_kv.clone(), Arc::new(RaftCommitter { raft: self.raft.clone() }))`.

- [ ] **Step 4: leadership reseed hook.** In `cluster`, add a small task that watches a node's metrics and calls `engine.reseed_counters()` when it transitions to leader. Simplest: the SQL-over-Raft harness calls `reseed_counters()` right after `wait_for_leader()` resolves and after any failover, before issuing SQL on the new leader. Document that a production version subscribes to `Raft::metrics()` leadership changes (D2).

- [ ] **Step 5: e2e — basic SQL over Raft.** `crates/cluster/tests/sql_over_raft.rs`:

```rust
use cluster::Cluster;
use pgwire::engine::{Engine, Session};

async fn run(s: &mut executor::SqlSession, sql: &str) -> Vec<pgwire::engine::QueryResult> {
    s.simple_query(sql).await.expect("query ok")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crud_over_raft_matches_single_node() {
    let c = Cluster::new(3).await;
    let lid = c.wait_for_leader().await;
    let engine = c.node(lid).engine();
    engine.reseed_counters().expect("reseed");
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4, v text)").await;
    run(&mut s, "INSERT INTO t VALUES (1,'a'), (2,'b')").await;
    let rows = run(&mut s, "SELECT v FROM t WHERE id = 2").await;
    // assert one row, value 'b' (use the project's QueryResult accessors)
    assert_eq!(col0(&rows[0]), vec![Some("b".to_string())]);
    run(&mut s, "UPDATE t SET v='b2' WHERE id=2").await;
    let rows = run(&mut s, "SELECT v FROM t WHERE id=2").await;
    assert_eq!(col0(&rows[0]), vec![Some("b2".to_string())]);
}
```

(Reuse the existing executor test helpers `col0`/`tag_of` — copy them into the test module as the SP6 concurrency tests do.)

- [ ] **Step 6: e2e — kill leader mid-workload, data survives.** Insert rows, capture the leader, isolate it, reseed + serve on the new leader, assert all committed rows are present and a new write succeeds.

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_data_survives_leader_failover() {
    let c = Cluster::new(3).await;
    let l0 = c.wait_for_leader().await;
    { let e = c.node(l0).engine(); e.reseed_counters().expect("reseed");
      let mut s = e.connect();
      run(&mut s, "CREATE TABLE t (id int4)").await;
      for i in 0..5 { run(&mut s, &format!("INSERT INTO t VALUES ({i})")).await; } }

    c.isolate(l0);
    // new leader on the majority side
    let l1 = c.wait_for_leader_excluding(l0).await;
    let e = c.node(l1).engine();
    e.reseed_counters().expect("reseed");
    let mut s = e.connect();
    let rows = run(&mut s, "SELECT id FROM t").await;
    assert_eq!(rowcount(&rows[0]), 5, "all committed rows survive failover");
    run(&mut s, "INSERT INTO t VALUES (99)").await; // new leader accepts writes
}
```

Add `Cluster::wait_for_leader_excluding(old)` to the harness.

- [ ] **Step 7: e2e — SP6 concurrency over the replicated path.** Two sessions on the leader engine: same-row UPDATE blocks then EvalPlanQual re-finds (port one SP6 `concurrency.rs` scenario, using `c.node(lid).engine()` shared via `Arc`). Confirms row locking + MVCC work unchanged atop Raft.

- [ ] **Step 8: run + commit.**

Run: `cargo test -p cluster --test sql_over_raft`
Expected: all PASS, no hangs.

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add crates/cluster/src/committer.rs crates/executor crates/cluster/src/node.rs crates/cluster/src/cluster.rs crates/cluster/tests/sql_over_raft.rs
git commit -m "feat(cluster): RaftCommitter + SqlEngine::replicated; SQL-over-Raft e2e with failover"
```

---

### Task 8: Jepsen-style bank workload + Stateright consistency check

**Files:**
- Create: `crates/cluster/tests/jepsen_bank.rs`

A randomized concurrent **bank** workload (transfers between N accounts) against the leader, with the `Switchboard` as a nemesis. Each op is recorded as `invoke`/`ok`/`fail`/`info`. The history is checked for **conservation** (total constant) and consistency via Stateright's checker. The recorder uses a small struct so it can later emit Elle/EDN (D2).

- [ ] **Step 1: history types + recorder.**

```rust
#[derive(Clone, Debug)]
enum OpKind { Transfer { from: i32, to: i32, amount: i64 }, ReadTotal }
#[derive(Clone, Debug)]
enum Outcome { Ok { total: Option<i64> }, Fail, Info } // Info = indeterminate (timeout)
#[derive(Clone, Debug)]
struct HistEntry { process: usize, op: OpKind, outcome: Outcome }
```

- [ ] **Step 2: workload + nemesis.** Set up `accounts` table seeded so the total is a known constant `T`. Spawn `P` processes; each loops doing random transfers (a transaction: `BEGIN; UPDATE accounts SET bal=bal-amt WHERE id=from; UPDATE accounts SET bal=bal+amt WHERE id=to; COMMIT`, skipping if `from` would go negative — read first). A nemesis task concurrently injects `isolate`/`heal` and `pause`/`resume` on a timer driven by `tokio::task::yield_now`/bounded `Raft::wait`, not wall-clock sleeps where avoidable. Record every op's invoke + outcome. After the run, `heal()`, reseed the leader, and read the final total.

- [ ] **Step 3: conservation invariant.**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bank_conserves_total_under_nemesis() {
    let (history, final_total, seeded_total) = run_bank_workload(/*accounts*/ 4, /*procs*/ 3, /*ops*/ 50).await;
    assert_eq!(final_total, seeded_total, "transfers must conserve the bank total");
    // Every committed (ok) transfer kept the total constant at read points.
    for e in &history {
        if let (OpKind::ReadTotal, Outcome::Ok { total: Some(t) }) = (&e.op, &e.outcome) {
            assert_eq!(*t, seeded_total, "every observed total equals the invariant");
        }
    }
}
```

- [ ] **Step 4: Stateright register linearizability (single key over Raft).** A focused check: drive concurrent reads/writes of one register key through `ensure_linearizable()` reads and `client_write` updates under a nemesis, record a history, and feed it to `stateright::semantics::LinearizabilityTester` with a `Register` model. Assert linearizable. (This targets **write-linearizability + linearizable reads when explicitly requested**, the property D1 guarantees; it does NOT use stale local reads.)

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn register_history_is_linearizable() {
    use stateright::semantics::{LinearizabilityTester, register::Register};
    let history = run_register_workload(/*procs*/ 3, /*ops*/ 30).await;
    let mut t = LinearizabilityTester::new(Register(0u64));
    for (process, op) in history { t.serialize(process, op); } // map invoke/ret per Stateright API
    assert!(t.is_consistent(), "register history must be linearizable");
}
```

(Adapt to Stateright 0.31's exact `LinearizabilityTester` API — `on_invoke`/`on_return` vs `serialize`. The point: a real, battle-tested checker, not a hand-rolled one.)

- [ ] **Step 5: run + commit.**

Run: `cargo test -p cluster --test jepsen_bank`
Expected: PASS (no conservation violation, history linearizable).

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add crates/cluster/tests/jepsen_bank.rs
git commit -m "test(cluster): Jepsen-style bank workload + Stateright consistency checks"
```

---

### Task 9: Stateright model of the counter/durability invariants

**Files:**
- Create: `crates/cluster/tests/model.rs`

A small, self-contained Stateright `Model` (NOT a re-model of Raft) that exhaustively explores the two highest-risk integration invariants: **counter monotonicity across failover** (max-merge + reseed ⇒ no id reuse) and **commit durability** (an acked write is never lost after an election). Model the abstract system: a replicated counter value, a set of "acked" writes, a leader, and actions {allocate, propose, apply-in-any-order, elect-new-leader, reseed}.

- [ ] **Step 1: define the model.** The abstract system: a leader hands out ids from an in-memory counter; each allocation proposes a new counter value that may apply out of order (max-merged) or be discarded on failover; reseed lifts the leader's counter to the applied high-water mark. An id is **acked** the moment its allocation's proposal applies (the transaction that used it is durable). The invariant: an acked id is never handed out again.

```rust
use stateright::{Model, Property, Checker};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    applied_counter: u64,      // max-merged applied next-counter across replicas
    in_flight: Vec<u64>,       // proposed new-counter values not yet applied
    leader_inmem: u64,         // leader's in-memory next counter
    handed_out: Vec<u64>,      // every id handed to a transaction
    acked: Vec<u64>,           // ids whose allocation proposal has applied (durable)
    steps: usize,
}

struct CounterModel { max_steps: usize }

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action { Allocate, ApplyAny(usize), ElectAndReseed }
```

- [ ] **Step 2: transitions (concrete).**

```rust
impl Model for CounterModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        vec![State { applied_counter: 0, in_flight: vec![], leader_inmem: 1,
                     handed_out: vec![], acked: vec![], steps: 0 }]
    }

    fn actions(&self, s: &State, out: &mut Vec<Action>) {
        if s.steps >= self.max_steps { return; } // bound the search
        out.push(Action::Allocate);
        for i in 0..s.in_flight.len() { out.push(Action::ApplyAny(i)); }
        out.push(Action::ElectAndReseed);
    }

    fn next_state(&self, s: &State, a: Action) -> Option<State> {
        let mut n = s.clone();
        n.steps += 1;
        match a {
            // Hand out the current id; propose the new counter value (id+1).
            Action::Allocate => {
                let id = n.leader_inmem;
                n.handed_out.push(id);
                n.in_flight.push(id + 1);   // proposed new next-counter
                n.leader_inmem = id + 1;
            }
            // Apply a proposal out of order, max-merged; the id it labels is now acked.
            Action::ApplyAny(i) => {
                if i >= n.in_flight.len() { return None; }
                let proposed = n.in_flight.remove(i);
                n.applied_counter = n.applied_counter.max(proposed);
                n.acked.push(proposed - 1); // the id whose allocation this proposal recorded
            }
            // Failover: discard still-in-flight (uncommitted) proposals, then reseed
            // the new leader's counter to the applied high-water mark.
            Action::ElectAndReseed => {
                n.in_flight.clear();
                n.leader_inmem = n.leader_inmem.max(n.applied_counter);
            }
        }
        Some(n)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // SAFETY 1: no id is ever handed out twice.
            Property::<Self>::always("no id reuse", |_, s| {
                let mut v = s.handed_out.clone();
                v.sort_unstable();
                v.windows(2).all(|w| w[0] != w[1])
            }),
            // SAFETY 2: the leader's next id is strictly above every acked id, so a
            // future Allocate can never collide with a durable (acked) id.
            Property::<Self>::always("leader counter dominates acked ids", |_, s| {
                s.acked.iter().all(|&id| s.leader_inmem > id)
            }),
        ]
    }
}
```

- [ ] **Step 3: note on the reseed precondition.** SAFETY 2 holds only because `ElectAndReseed` lifts `leader_inmem` to `applied_counter`, and `applied_counter` is always `> ` every acked id (an acked id `k` came from applying the proposal value `k+1`, so `applied_counter >= k+1 > k`). The model exists to confirm this reasoning holds under *every* interleaving of allocate/apply/elect — including applies that land out of order and elections that strand in-flight proposals.

- [ ] **Step 4: check exhaustively.**

```rust
#[test]
fn counter_invariants_hold_under_all_interleavings() {
    use stateright::Checker;
    let checker = CounterModel { max_steps: 8 }.checker().spawn_bfs().join();
    checker.assert_properties(); // no property violated across the explored space
}
```

Run: `cargo test -p cluster --test model`
Expected: PASS (no counterexample). If Stateright finds one, the reseed/max-merge logic is wrong — fix Task 6, not the model.

- [ ] **Step 5: commit.**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add crates/cluster/tests/model.rs
git commit -m "test(cluster): Stateright model of counter monotonicity + commit durability"
```

---

### Task 10: Gauntlet, traceability, finish

**Files:** Verify; no new code unless a gate fails.

- [ ] **Step 1: gauntlet.** Run each, report PASS/FAIL:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test -p pgparser --features oracle
bash scripts/check-no-native.sh
cargo deny check
```

The shipped binary (`crabgresql`) still does not depend on `cluster`/openraft unless explicitly wired, so `check-no-native.sh` stays green; openraft is pure Rust regardless. If the binary is wired to offer a `--replicated` demo mode, re-run `check-no-native.sh` and confirm openraft added no `-sys`/`cc` crate.

- [ ] **Step 2: success-criteria traceability.** Confirm each spec criterion maps to a green test:

| # | Spec criterion | Verifying test(s) |
|---|---|---|
| 1 | `cluster` runs 3-replica single range; storage passes the Suite | `store::tests::openraft_storage_suite` |
| 2 | SQL stack unchanged over the replicated path; 224 tests pass | `sql_over_raft::crud_over_raft_matches_single_node`; `cargo test --workspace` |
| 3 | Committed write survives failover; no data loss / id reuse | `sql_over_raft::committed_data_survives_leader_failover`; `scenarios::isolating_leader_elects_new_leader_no_xid_reuse` |
| 4 | No-quorum → `Unavailable`, no partial state; non-leader → `NotLeader` | `scenarios::minority_partition_cannot_commit` |
| 5 | Network deterministically drives replication/catch-up/snapshot/failover/partition | `scenarios::*` |
| 6 | Jepsen bank passes Stateright + conservation; model verifies counters/durability | `jepsen_bank::*`, `model::counter_invariants_hold_under_all_interleavings` |
| 7 | All SP1–SP6 gates green; pure-Rust shipped tree; forbid(unsafe) | gauntlet (Step 1) |

If any row lacks a green test, add it before finishing.

- [ ] **Step 3: commit (if anything changed).**

```bash
git add -A
git commit -m "test(sp7): gauntlet green; success-criteria traceability"
```

---

## Final review (after all tasks)

Dispatch a code-reviewer over the whole SP7 diff (vs pre-SP7 main), then run `superpowers:finishing-a-development-branch`. Review focus:

- **No lost wakeups / no hangs:** every fault-injection test clears its faults before relying on quorum; `Raft::wait` targets the quorum side; no test depends on a wall-clock `sleep` for correctness.
- **Counter safety across failover:** max-merge on the two counter-key shapes (`next_xid`, every `seq_key`); fold-into-batch on the replicated path; reseed-on-leadership; the Stateright model finds no id reuse.
- **Local path byte-for-byte:** `PersistMode::Durable` keeps SP6 behavior (eager self-persist, no folding); all 224 tests pass unchanged.
- **No partial state on failed writes:** a `client_write` that does not commit applies nothing; `Unavailable` leaves the txn aborted (no `clog=Committed`).
- **Isolation unchanged for reads:** reads hit the applied `sm_kv` with SP5/SP6 MVCC; documented stale-read gap (no leases) is out of scope (D5).
- **Purity & licensing:** `check-no-native.sh` and `cargo deny` green with openraft added; `#![forbid(unsafe_code)]` holds in `cluster`.
- **No `std::sync::Mutex` guard across `.await`** in the executor or `cluster` (the Switchboard/manager locks are released before any await; only `tokio`/openraft primitives cross awaits).
