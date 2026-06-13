# SP13 / D3a — In-process Multi-range Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Go from one Raft group over the whole keyspace to **N co-located in-process ranges** — each its own Raft group + MVCC domain — with static, table-aligned key→range routing of SQL (single-range transactions).

**Architecture:** A pure `RangeMap` maps `table_id → RangeId`. The `Switchboard` becomes range-aware (handles keyed by `(RangeId, NodeId)`; faults stay node-scoped). A `MultiRangeCluster` runs N per-range groups, each with its own `sm_kv` + `SqlEngine`. A `RangeRouter` dispatches each statement: DDL → range 0 (the catalog lives there); single-table DML → resolve the table from range 0's catalog and execute rows on its data range. The executor gains a `catalog_kv` seam so a data-range op resolves its schema from range 0.

**Tech Stack:** Rust 2024, openraft 0.9.24 (one `TypeConfig` group per range), `executor`/`cluster`/`catalog`/`kv`/`pgparser` crates, stateright/tokio-postgres-free in-process tests. No new dependency. `#![forbid(unsafe_code)]` preserved. In-process only.

**Spec:** `docs/superpowers/specs/2026-06-13-crabgresql-sp13-d3a-multirange-core-design.md`

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `crates/cluster/src/range/mod.rs` | `range` module root; re-exports. | **Create** |
| `crates/cluster/src/range/map.rs` | `RangeId`, `RangeMap` (table_id → range, binary search). Pure. | **Create** |
| `crates/cluster/src/range/cluster.rs` | `MultiRangeCluster`: N co-located per-range groups + per-range `SqlEngine`; per-range leader access. | **Create** |
| `crates/cluster/src/range/router.rs` | `RangeRouter`: per-connection statement dispatch + single-range-txn pinning + cross-range rejection. | **Create** |
| `crates/cluster/src/network.rs` | `Switchboard` range-aware: handles keyed by `(RangeId, NodeId)`; faults node-scoped. | Modify |
| `crates/cluster/src/node.rs` | `Node` registers under `(RangeId, NodeId)`; carries its `RangeId`. | Modify |
| `crates/cluster/src/cluster.rs` | Existing single-range `Cluster` passes `RangeId(0)` to the range-aware transport. | Modify |
| `crates/cluster/src/lib.rs` | Add `pub mod range;`. | Modify |
| `crates/executor/src/exec.rs` | `execute_read`/`execute_write`/`execute_read_locking`/`describe` gain a `catalog_kv: &dyn Kv` param for catalog lookups. | Modify |
| `crates/executor/src/session.rs` | `SqlSession` gains a `catalog_kv` handle (== `kv` for single-range); passes it; exposes `run(&Statement)`. | Modify |
| `crates/executor/src/lib.rs` | `SqlEngine` gains `catalog_kv`; `replicated` gains a `catalog_kv` arg; `new`/`with_kv` use `kv`. | Modify |
| `crates/cluster/tests/multirange.rs` | Routing, cross-range rejection, failover, sharded-consistency tests. | **Create** |

**Decision — `RangeId` is a type alias** `pub type RangeId = u32;` (consistent with the codebase's `NodeId = u64` / `TableId = u32`), not a newtype. The spec's `RangeId(u32)` was illustrative.

**Ordering rationale:** T1 (pure RangeMap) and T2 (range-aware transport) and T3 (catalog_kv seam) are independent foundations; T4 (MultiRangeCluster) needs T1+T2; T5 (router) needs T3+T4; T6/T7 (tests) need T5. T2 and T3 are **behavior-preserving** refactors — existing tests are their regression gate.

---

## Task 1: `RangeMap` — pure key→range addressing

**Files:**
- Create: `crates/cluster/src/range/mod.rs`, `crates/cluster/src/range/map.rs`
- Modify: `crates/cluster/src/lib.rs`

- [ ] **Step 1: Write the failing test** — append to `crates/cluster/src/range/map.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_range_covers_all_tables() {
        let m = RangeMap::single();
        assert_eq!(m.range_count(), 1);
        assert_eq!(m.range_for_table(0), 0);
        assert_eq!(m.range_for_table(7), 0);
        assert_eq!(m.range_for_table(u32::MAX), 0);
    }

    #[test]
    fn boundaries_partition_table_ids_contiguously() {
        // 3 ranges: [0,10) -> 0, [10,20) -> 1, [20,inf) -> 2.
        let m = RangeMap::with_boundaries(vec![10, 20]);
        assert_eq!(m.range_count(), 3);
        assert_eq!(m.range_for_table(0), 0); // system/catalog (table 0) is in range 0
        assert_eq!(m.range_for_table(9), 0);
        assert_eq!(m.range_for_table(10), 1); // boundary is the start of the next range
        assert_eq!(m.range_for_table(19), 1);
        assert_eq!(m.range_for_table(20), 2);
        assert_eq!(m.range_for_table(1_000), 2);
    }

    #[test]
    fn boundaries_must_be_sorted_and_nonzero() {
        // 0 cannot be a boundary (range 0 always starts at 0); boundaries strictly increasing.
        assert!(std::panic::catch_unwind(|| RangeMap::with_boundaries(vec![0, 10])).is_err());
        assert!(std::panic::catch_unwind(|| RangeMap::with_boundaries(vec![20, 10])).is_err());
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p cluster --lib range::map`
Expected: FAIL — `RangeMap` does not exist.

- [ ] **Step 3: Implement `RangeMap`** — write the top of `crates/cluster/src/range/map.rs` (above the test module):

```rust
//! Static, table-aligned key→range addressing. A `RangeMap` partitions the
//! `table_id` space into N contiguous ranges; range 0 always starts at table 0
//! (so it owns the reserved system/catalog keys) and the last range is unbounded.
//! This is a routing rule over table ids, not a slice of one shared keyspace —
//! each range is its own `sm_kv` (see the spec's storage-model note).

use catalog::TableId;

/// Identifies one range / Raft group. A small integer.
pub type RangeId = u32;

/// A static partition of the `table_id` space into contiguous ranges.
/// `boundaries` are strictly-increasing, nonzero split points: range `i` covers
/// `[boundaries[i-1], boundaries[i])` with `boundaries[-1] = 0` and
/// `boundaries[len] = +inf`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeMap {
    boundaries: Vec<TableId>,
}

impl RangeMap {
    /// The degenerate single range covering every table (today's behavior).
    pub fn single() -> Self {
        Self { boundaries: Vec::new() }
    }

    /// Build a range map from sorted, strictly-increasing, nonzero boundaries.
    /// Panics on an invalid boundary list (a programming error, not user input).
    pub fn with_boundaries(boundaries: Vec<TableId>) -> Self {
        assert!(
            boundaries.iter().all(|&b| b != 0),
            "0 cannot be a boundary: range 0 always starts at table 0"
        );
        assert!(
            boundaries.windows(2).all(|w| w[0] < w[1]),
            "range boundaries must be strictly increasing"
        );
        Self { boundaries }
    }

    /// Number of ranges (boundaries + 1).
    pub fn range_count(&self) -> usize {
        self.boundaries.len() + 1
    }

    /// The range that owns `table_id`'s data.
    pub fn range_for_table(&self, table_id: TableId) -> RangeId {
        // partition_point = count of boundaries <= table_id = the range index.
        self.boundaries.partition_point(|&b| b <= table_id) as RangeId
    }

    /// Every range id, `0..range_count()`.
    pub fn range_ids(&self) -> impl Iterator<Item = RangeId> {
        0..self.range_count() as RangeId
    }
}
```

Create `crates/cluster/src/range/mod.rs`:

```rust
//! Multi-range (D3a): static range map, co-located per-range Raft groups, and
//! key→range SQL routing. In-process; the network analog is a later sub-slice.

pub mod cluster;
pub mod map;
pub mod router;

pub use cluster::MultiRangeCluster;
pub use map::{RangeId, RangeMap};
pub use router::RangeRouter;
```

> NOTE: `mod.rs` references `cluster`/`router` modules created in T4/T5. To keep T1 compiling on its own, for THIS task create `mod.rs` with only `pub mod map; pub use map::{RangeId, RangeMap};` and add the `cluster`/`router` lines in T4/T5.

In `crates/cluster/src/lib.rs`, add after the existing `pub mod addr;` line:

```rust
pub mod range;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p cluster --lib range::map`
Expected: PASS (3 tests).

Run: `cargo clippy -p cluster --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/cluster/src/range/ crates/cluster/src/lib.rs
git commit -m "feat(cluster): RangeMap — static table-aligned key->range addressing"
```

---

## Task 2: Range-aware transport (`Switchboard` + `Node`)

Make the in-process transport demultiplex by `(RangeId, NodeId)` so N co-located groups coexist. **Behavior-preserving for single range** — the existing `Cluster` passes `RangeId(0)` and all in-process cluster tests stay green.

**Files:**
- Modify: `crates/cluster/src/network.rs`, `crates/cluster/src/node.rs`, `crates/cluster/src/cluster.rs`

- [ ] **Step 1: Make `Switchboard` handle-routing range-aware** (`network.rs`)

Change the handle registry key from `NodeId` to `(RangeId, NodeId)`. **Faults stay keyed by `NodeId`** (a node pause/cut affects all its ranges). Concretely:

- Field: `handles: Arc<Mutex<HashMap<(RangeId, NodeId), openraft::Raft<TypeConfig>>>>`.
- `register(&self, range: RangeId, id: NodeId, raft: …)` / `deregister(&self, range: RangeId, id: NodeId)` — key by `(range, id)`.
- `for_node(&self, range: RangeId, from: NodeId) -> NodeFactory` — `NodeFactory { sb, range, from }`.
- `handle(&self, range: RangeId, to: NodeId)` — look up `(range, to)`.
- `blocked(from, to)` — **unchanged** (node pair).
- `pause`/`resume`/`cut`/`heal` — **unchanged** (node-scoped).

Add `use crate::range::RangeId;`. `NodeFactory` and `Conn` each carry `range: RangeId`; `Conn::resolve` calls `self.sb.handle(self.range, self.target)` (the `blocked` check still uses `(self.from, self.target)`). In `RaftNetworkFactory::new_client`, propagate `range: self.range`.

- [ ] **Step 2: Make `Node` range-aware** (`node.rs`)

`Node` gains `pub range: RangeId`. Its constructors take a `range` and register under it:
- `start(range: RangeId, id: NodeId, sb: Switchboard)` and `start_with_config(range, id, sb, config)` — build the Raft with network factory `sb.for_node(range, id)`, then `sb.register(range, id, raft.clone())`.
- `start_durable(range, id, sb, dir, config)` — same.
- Anywhere `Node` calls `sb.deregister(id)` (restart path), use `sb.deregister(self.range, id)`.

(The Raft `TypeConfig` is unchanged — every range uses the same command type; ranges differ only by which Raft instance/log/sm_kv they own.)

- [ ] **Step 3: Update the single-range `Cluster` to pass `RangeId(0)`** (`cluster.rs`)

In `build`, `durable`, and `new_with_snapshotting`/`new_stable_leader` paths, change `Node::start_with_config(id, sb.clone(), …)` → `Node::start_with_config(0, id, sb.clone(), …)` and the durable equivalent to pass range `0`. The single-range cluster is "range 0".

- [ ] **Step 4: Run the full in-process cluster suite to verify behavior is preserved**

Run: `cargo test -p cluster`
Expected: PASS — every existing in-process test (model, durability scenarios, etc.) still passes; the only change is the transport key gained a range dimension, fixed at 0.

Run: `cargo clippy -p cluster --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/cluster/src/network.rs crates/cluster/src/node.rs crates/cluster/src/cluster.rs
git commit -m "refactor(cluster): range-aware Switchboard/Node transport (handles keyed by (range,node))"
```

---

## Task 3: Executor `catalog_kv` seam (behavior-preserving)

Let a data-range operation resolve its table schema from a **different** kv (range 0's catalog) than the one holding its rows. Add a `catalog_kv` parameter used only for catalog lookups; for single range, `catalog_kv == kv`, so nothing changes.

**Files:**
- Modify: `crates/executor/src/exec.rs`, `crates/executor/src/session.rs`, `crates/executor/src/lib.rs`

- [ ] **Step 1: Add `catalog_kv` to the four executor functions** (`exec.rs`)

For each, add `catalog_kv: &dyn Kv` as the **first** parameter and route catalog lookups through it (row/seq/clog access stays on `kv`):

- `execute_write(catalog_kv, kv, procarray, lockmgr, seq, snapshot, xid, repeatable_read, stmt)` — the 3 internal `catalog::get_table(kv, …)` calls (INSERT/UPDATE/DELETE) become `catalog::get_table(catalog_kv, …)`.
- `execute_read(catalog_kv, kv, snapshot, own, stmt)` — `catalog::get_table(kv, name)` → `catalog::get_table(catalog_kv, name)`.
- `execute_read_locking(catalog_kv, kv, procarray, lockmgr, snapshot, xid, repeatable_read, mode, s)` — `catalog::get_table(kv, table_name)` → `catalog::get_table(catalog_kv, table_name)`.
- `describe(catalog_kv, kv, sql)` — `catalog::get_table(kv, name)` → `catalog::get_table(catalog_kv, name)`.

(DDL — `execute_ddl` — is unchanged: it runs wholly on range 0.)

- [ ] **Step 2: Give `SqlEngine`/`SqlSession` a `catalog_kv` handle** (`lib.rs`, `session.rs`)

`SqlEngine` gains `pub(crate) catalog_kv: Arc<dyn Kv>`:
- `with_kv(kv)` (single-node) and `new` → `catalog_kv: Arc::clone(&kv)`.
- `replicated(sm_kv, committer, linearizer)` → add a leading `catalog_kv: Arc<dyn Kv>` param: `replicated(catalog_kv, sm_kv, committer, linearizer)`, store it. (For a single-range replicated engine the caller passes `sm_kv` for both; the multi-range data engines pass range 0's `sm_kv`.)
- `connect()` passes `Arc::clone(&self.catalog_kv)` into `SqlSession::new`.

`SqlSession` gains `catalog_kv: Arc<dyn Kv>` (constructor param after `kv`), and at its four call sites passes `&*self.catalog_kv` as the new first arg:
- `run_select` → `execute_read(&*self.catalog_kv, &*self.kv, &snapshot, own, stmt)`.
- `run_write` → `execute_write(&*self.catalog_kv, &*kv, …)`.
- `run_select_locking` → `execute_read_locking(&*self.catalog_kv, &*kv, …)`.
- wherever `describe` is invoked (the pgwire `Session::describe` impl) → pass `&*self.catalog_kv` first.

- [ ] **Step 3: Update the two cluster `replicated(...)` call sites** for the new leading arg (`crates/cluster/src/node.rs`, `crates/cluster/src/server_node.rs`)

Both currently call `SqlEngine::replicated(sm_kv, RaftCommitter{…}, RaftLinearizer{…})`. Add `sm_kv.clone()` (or `self.sm_kv.clone()`) as the **first** arg so catalog and data are the same store (unchanged single-range behavior):
```rust
SqlEngine::replicated(sm_kv.clone(), sm_kv, RaftCommitter { raft }, RaftLinearizer { raft })
```
(adjust the exact `clone()`s to satisfy the borrow checker at each site).

- [ ] **Step 4: Run the executor + cluster suites to verify behavior is preserved**

Run: `cargo test -p executor`
Expected: PASS — all existing executor tests unchanged (catalog_kv == kv everywhere).

Run: `cargo test -p cluster` and `cargo clippy -p executor -p cluster --all-targets -- -D warnings`
Expected: PASS / clean.

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/exec.rs crates/executor/src/session.rs crates/executor/src/lib.rs crates/cluster/src/node.rs crates/cluster/src/server_node.rs
git commit -m "refactor(executor): catalog_kv seam — resolve schema from a (possibly separate) catalog store"
```

---

## Task 4: `MultiRangeCluster` — N co-located per-range groups

**Files:**
- Create: `crates/cluster/src/range/cluster.rs`
- Modify: `crates/cluster/src/range/mod.rs` (add `pub mod cluster; pub use cluster::MultiRangeCluster;`)

- [ ] **Step 1: Write the failing bring-up test** — append to `crates/cluster/src/range/cluster.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_range_elects_a_leader() {
        // 3 nodes, 2 ranges (boundary at table_id 10).
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![10])).await;
        for r in c.range_map().range_ids() {
            let leader = c.wait_for_leader(r).await;
            assert!(leader < 3, "range {r} elected a valid leader");
        }
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p cluster --lib range::cluster`
Expected: FAIL — `MultiRangeCluster` does not exist.

- [ ] **Step 3: Implement `MultiRangeCluster`** — write the top of `crates/cluster/src/range/cluster.rs`:

```rust
//! N co-located in-process Raft groups (one per range), built over one shared
//! range-aware `Switchboard`. Each (range, node) is its own Raft replica with its
//! own applied `sm_kv`; range 0's `sm_kv` additionally holds the catalog, which
//! every data range resolves schemas from.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use executor::SqlEngine;
use kv::{Kv, MemKv};
use openraft::{BasicNode, ServerState};

use crate::committer::RaftCommitter;
use crate::linearizer::RaftLinearizer;
use crate::network::Switchboard;
use crate::node::Node;
use crate::range::map::{RangeId, RangeMap};
use crate::types::NodeId;

/// One range's replicas (its Raft `Node`s) across the physical node set.
struct RangeGroup {
    nodes: Vec<Node>,
}

/// An in-process multi-range cluster: `n` physical nodes, each running a replica
/// of every range. Range 0's `sm_kv` holds the catalog.
pub struct MultiRangeCluster {
    n: u64,
    map: RangeMap,
    groups: Vec<RangeGroup>, // indexed by RangeId
    sb: Switchboard,
}

impl MultiRangeCluster {
    /// Build `n` nodes × `map.range_count()` ranges and initialize each range's
    /// voting group `{0..n}`.
    pub async fn new(n: u64, map: RangeMap) -> Self {
        let sb = Switchboard::new();
        let mut groups = Vec::new();
        for r in map.range_ids() {
            let mut nodes = Vec::new();
            for id in 0..n {
                nodes.push(Node::start_with_config(r, id, sb.clone(), Node::default_config()).await);
            }
            let members: BTreeMap<NodeId, BasicNode> =
                (0..n).map(|id| (id, BasicNode::default())).collect();
            nodes[0].raft.initialize(members).await.expect("initialize range group");
            groups.push(RangeGroup { nodes });
        }
        Self { n, map, groups, sb }
    }

    pub fn range_map(&self) -> &RangeMap {
        &self.map
    }

    pub fn switchboard(&self) -> &Switchboard {
        &self.sb
    }

    /// Range 0's applied catalog store (every data range resolves schema from it).
    pub fn catalog_kv(&self) -> Arc<dyn Kv> {
        self.groups[0].nodes[0].sm_kv.clone()
    }

    /// Block until `range` has a stable leader; return its node id.
    pub async fn wait_for_leader(&self, range: RangeId) -> NodeId {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            for node in &self.groups[range as usize].nodes {
                let m = node.raft.metrics().borrow().clone();
                if m.state == ServerState::Leader && m.current_leader == Some(m.id) {
                    return m.id;
                }
            }
            assert!(tokio::time::Instant::now() < deadline, "range {range} elected no leader");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// A replicated `SqlEngine` for `range`'s leader node: reads/writes go to that
    /// node's range replica; the catalog always resolves from range 0's store.
    /// (Co-located: every node hosts every range, so range 0's store is local.)
    pub async fn leader_engine(&self, range: RangeId) -> SqlEngine {
        let leader = self.wait_for_leader(range).await;
        let node = &self.groups[range as usize].nodes[leader as usize];
        let engine = SqlEngine::replicated(
            self.catalog_kv(),       // catalog from range 0
            node.sm_kv.clone(),      // data from this range
            Arc::new(RaftCommitter { raft: node.raft.clone() }),
            Arc::new(RaftLinearizer { raft: node.raft.clone() }),
        )
        .expect("replicated engine");
        engine.reseed_counters().ok();
        engine
    }

    /// The applied `sm_kv` of `(range, node)` — for asserting where rows landed.
    pub fn sm_kv(&self, range: RangeId, node: NodeId) -> Arc<dyn Kv> {
        self.groups[range as usize].nodes[node as usize].sm_kv.clone()
    }

    /// Pause a physical node (all its range replicas) — node-scoped fault.
    pub fn pause(&self, id: NodeId) {
        self.sb.pause(id);
    }
    pub fn resume(&self, id: NodeId) {
        self.sb.resume(id);
    }
    pub fn heal(&self) {
        self.sb.heal();
    }
    pub fn n(&self) -> u64 {
        self.n
    }
}
```

In `crates/cluster/src/range/mod.rs`, add `pub mod cluster;` and `pub use cluster::MultiRangeCluster;`.

- [ ] **Step 4: Run the bring-up test**

Run: `cargo test -p cluster --lib range::cluster`
Expected: PASS — both ranges elect a leader.

Run: `cargo clippy -p cluster --all-targets -- -D warnings`
Expected: clean. (`MemKv` import may be unused here — remove it if clippy flags it; it's listed for the test scaffolding only.)

- [ ] **Step 5: Commit**

```bash
git add crates/cluster/src/range/
git commit -m "feat(cluster): MultiRangeCluster — N co-located in-process per-range Raft groups"
```

---

## Task 5: `RangeRouter` — per-connection statement dispatch

The router parses each statement, routes DDL to range 0 and single-table DML to the table's data range (schema resolved from range 0), pins a transaction to one range, and rejects cross-range/multi-table statements.

**Files:**
- Create: `crates/cluster/src/range/router.rs`
- Modify: `crates/cluster/src/range/mod.rs` (`pub mod router; pub use router::RangeRouter;`)
- Modify: `crates/executor/src/session.rs` (expose `pub async fn run(&mut self, stmt: &Statement)`), `crates/executor/src/lib.rs` (re-export nothing new; `Statement` is `pgparser::ast::Statement`)

- [ ] **Step 1: Expose a parsed-statement entry on `SqlSession`** (`session.rs`)

`run_one` is private. Add a thin public wrapper so the router can execute an already-parsed statement on a range's session:

```rust
/// Execute one already-parsed statement (the router parses once, then routes).
pub async fn run(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
    self.run_one(stmt).await
}
```

- [ ] **Step 2: Write the failing router test** — append to `crates/cluster/src/range/router.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn create_in_range0_insert_routes_to_data_range_select_reads_back() {
        // boundary at table 2: the first user table (id 1) -> range 0;
        // later tables (id >= 2) -> range 1.
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        let mut router = RangeRouter::connect(&c).await;

        router.simple("CREATE TABLE a (id int4)").await.expect("create a"); // id 1 -> range 0
        router.simple("CREATE TABLE b (id int4)").await.expect("create b"); // id 2 -> range 1
        router.simple("INSERT INTO a VALUES (10)").await.expect("insert a");
        router.simple("INSERT INTO b VALUES (20)").await.expect("insert b");

        // Reads route to the right range and see their rows.
        assert_eq!(router.scan_one_i32("SELECT id FROM a").await, vec![10]);
        assert_eq!(router.scan_one_i32("SELECT id FROM b").await, vec![20]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_transaction_may_not_span_ranges() {
        // a -> range 0, b -> range 1 (boundary at 2). A txn that writes both is
        // rejected when the second statement's range differs from the pinned one.
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        let mut router = RangeRouter::connect(&c).await;
        router.simple("CREATE TABLE a (id int4)").await.expect("create a");
        router.simple("CREATE TABLE b (id int4)").await.expect("create b");
        router.simple("BEGIN").await.expect("begin");
        router.simple("INSERT INTO a VALUES (1)").await.expect("first DML pins range 0");
        let err = router
            .simple("INSERT INTO b VALUES (2)")
            .await
            .expect_err("a second range in one txn must be rejected");
        assert_eq!(err.code, "0A000"); // feature_not_supported (cross-range, D3b)
        router.simple("ROLLBACK").await.ok();
    }
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `cargo test -p cluster --lib range::router`
Expected: FAIL — `RangeRouter` does not exist.

- [ ] **Step 4: Implement `RangeRouter`** — write the top of `crates/cluster/src/range/router.rs`:

```rust
//! Per-connection multi-range SQL dispatch. Parses each statement, routes DDL to
//! range 0 and single-table DML to the table's data range (schema resolved from
//! range 0's catalog), pins a transaction to one range, and rejects cross-range /
//! multi-table statements (deferred to D3b).

use std::collections::HashMap;

use executor::{ExecError, SqlEngine, SqlSession};
use pgparser::ast::Statement;
use pgwire::engine::{Engine, QueryResult, Session};
use pgwire::error::PgError;

use crate::range::cluster::MultiRangeCluster;
use crate::range::map::RangeId;

/// A connection's view: one leader `SqlSession` per range it has touched, plus the
/// range a transaction (if any) is pinned to.
pub struct RangeRouter {
    sessions: HashMap<RangeId, SqlSession>,
    pinned: Option<RangeId>, // Some(_) while inside a BEGIN..COMMIT block
    // Resolved range-0 catalog + the range map + per-range leader engines.
    map: crate::range::map::RangeMap,
    engines: HashMap<RangeId, SqlEngine>,
    catalog_kv: std::sync::Arc<dyn kv::Kv>,
}

impl RangeRouter {
    /// Open a connection against the cluster: grab each range's current leader
    /// engine and the range-0 catalog store.
    pub async fn connect(c: &MultiRangeCluster) -> Self {
        let mut engines = HashMap::new();
        for r in c.range_map().range_ids() {
            engines.insert(r, c.leader_engine(r).await);
        }
        Self {
            sessions: HashMap::new(),
            pinned: None,
            map: c.range_map().clone(),
            engines,
            catalog_kv: c.catalog_kv(),
        }
    }

    /// The data range a statement targets, or an error if it's cross-range /
    /// multi-table / unresolvable. `None` means "range 0" (DDL / no table).
    fn target_range(&self, stmt: &Statement) -> Result<RangeId, ExecError> {
        match stmt {
            // DDL and txn control run on range 0 (catalog / xid coordination).
            Statement::CreateTable { .. } | Statement::DropTable { .. } => Ok(0),
            Statement::Begin { .. } | Statement::Commit | Statement::Rollback => Ok(0),
            // Single-table DML: resolve the table via range 0's catalog.
            // Single-table DML. The grammar has no joins and Insert/Update/Delete
            // carry exactly one `table`, so every statement targets one range.
            Statement::Insert { table, .. }
            | Statement::Update { table, .. }
            | Statement::Delete { table, .. } => self.range_of(table),
            // A SELECT references at most one table (`from: Option<String>`);
            // a FROM-less SELECT (e.g. `SELECT 1`) runs on range 0.
            Statement::Select(s) => match &s.from {
                Some(name) => self.range_of(name),
                None => Ok(0),
            },
        }
    }

    fn range_of(&self, table_name: &str) -> Result<RangeId, ExecError> {
        let t = catalog::get_table(&*self.catalog_kv, table_name)?;
        Ok(self.map.range_for_table(t.id))
    }

    /// Execute one already-parsed statement, honoring transaction pinning.
    async fn dispatch(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        let range = self.target_range(stmt)?;

        // Transaction range-pinning: BEGIN starts a block; the first range a DML
        // touches pins the block; a later statement on another range is rejected.
        match stmt {
            Statement::Begin { .. } => {
                self.pinned = Some(range); // provisional (range 0); first DML re-pins
            }
            Statement::Commit | Statement::Rollback => {
                // COMMIT/ROLLBACK must reach the session that holds the txn.
                let r = self.pinned.take().unwrap_or(range);
                return self.session_mut(r).await.run(stmt).await;
            }
            _ => {
                if let Some(p) = self.pinned {
                    // Inside a block: the first real DML re-pins from the provisional
                    // range 0 to its data range; thereafter all must match.
                    if p == 0 && range != 0 {
                        self.pinned = Some(range);
                    } else if range != self.pinned.unwrap() && range != 0 {
                        return Err(ExecError::Unsupported(
                            "a transaction may not span ranges yet (D3b)".into(),
                        ));
                    }
                }
            }
        }

        // If we're in a pinned block, run BEGIN on the pinned session too so its
        // snapshot/state is established there.
        let exec_range = self.pinned.unwrap_or(range);
        self.session_mut(exec_range).await.run(stmt).await
    }

    /// Get (creating on first use) the `SqlSession` for `range`'s leader engine.
    async fn session_mut(&mut self, range: RangeId) -> &mut SqlSession {
        self.sessions
            .entry(range)
            .or_insert_with(|| self.engines.get(&range).expect("engine for range").connect())
    }

    /// Parse `sql` and run each statement in order; return the last result.
    /// (Maps `ExecError` to a wire `PgError` like the single-range session does.)
    pub async fn simple(&mut self, sql: &str) -> Result<QueryResult, PgError> {
        let stmts = pgparser::parse(sql).map_err(|e| ExecError::Parse(e).into_pg())?;
        let mut last = QueryResult::Command { tag: "OK".into() };
        for stmt in &stmts {
            last = self.dispatch(stmt).await.map_err(ExecError::into_pg)?;
        }
        Ok(last)
    }
}
```

> **Parser surface (verified):** `pgparser::ast::Statement` has `CreateTable{name}`, `DropTable{name}`, `Insert{table,columns,rows}`, `Update{table,assignments,filter}`, `Delete{table,filter}`, `Select(SelectStmt)`, `Begin{isolation}`, `Commit`, `Rollback`. `SelectStmt.from` is `Option<String>` — a single table, **no joins in the grammar**. So a single statement is never cross-range; the only cross-range case is a transaction whose statements map to different ranges, handled by the txn-pinning in `dispatch` (rejected with `Unsupported` → `0A000`). Do not add join detection — it can't occur.

Also add to the router a tiny test helper used by the tests:

```rust
#[cfg(test)]
impl RangeRouter {
    /// Run a single-column int SELECT and collect the i32 values (test helper).
    async fn scan_one_i32(&mut self, sql: &str) -> Vec<i32> {
        use pgwire::engine::QueryResult;
        match self.simple(sql).await.expect("query ok") {
            QueryResult::Rows { rows, .. } => rows
                .iter()
                .map(|r| {
                    let cell = r[0].as_ref().expect("non-null");
                    std::str::from_utf8(&cell.text).unwrap().parse().unwrap()
                })
                .collect(),
            other => panic!("expected Rows, got {other:?}"),
        }
    }
}
```

In `crates/cluster/src/range/mod.rs`, add `pub mod router;` and `pub use router::RangeRouter;`.

- [ ] **Step 5: Run the router tests**

Run: `cargo test -p cluster --lib range::router`
Expected: PASS — create/insert/select routes correctly; the cross-range statement is rejected (`0A000`).

Run: `cargo clippy -p cluster --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/cluster/src/range/ crates/executor/src/session.rs
git commit -m "feat(cluster): RangeRouter — per-connection key->range SQL dispatch + single-range-txn pinning"
```

---

## Task 6: Routing-correctness & cross-range-rejection e2e tests

**Files:**
- Create: `crates/cluster/tests/multirange.rs`

- [ ] **Step 1: Write the routing/rejection integration tests**

```rust
//! D3a: SQL routes to the correct range; rows land only in that range's store;
//! cross-range statements are rejected.
use cluster::range::{MultiRangeCluster, RangeMap};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_land_only_in_their_table_range() {
    // tables: id 1 -> range 0, id 2 -> range 1 (boundary at 2).
    let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
    for r in c.range_map().range_ids() {
        c.wait_for_leader(r).await;
    }
    let mut router = cluster::range::RangeRouter::connect(&c).await;
    router.simple("CREATE TABLE a (id int4)").await.expect("a");
    router.simple("CREATE TABLE b (id int4)").await.expect("b");
    router.simple("INSERT INTO b VALUES (20)").await.expect("insert b");

    // b's rows (table id 2 -> range 1) must be present in range 1's store and
    // absent from range 0's store, on every node (applied replication).
    use kv::key::table_prefix;
    let b_prefix = table_prefix(2);
    for node in 0..c.n() {
        let r1 = c.sm_kv(1, node);
        let r0 = c.sm_kv(0, node);
        assert!(!r1.scan_prefix(&b_prefix).expect("scan r1").is_empty(), "b in range 1 node {node}");
        assert!(r0.scan_prefix(&b_prefix).expect("scan r0").is_empty(), "b absent from range 0 node {node}");
    }
}
```

(The cross-range rejection — a transaction spanning ranges — is unit-tested at the router level in Task 5's `a_transaction_may_not_span_ranges`; no need to duplicate it here.)

- [ ] **Step 2: Run it**

On Windows set `$env:__COMPAT_LAYER='RunAsInvoker'` if a test fails to launch with OS error 740 (these are in-process, so it should not be needed).

Run: `cargo test -p cluster --test multirange`
Expected: PASS (1 test in this task; more added in Task 7).

Run: `cargo clippy -p cluster --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add crates/cluster/tests/multirange.rs
git commit -m "test(cluster): D3a routing correctness + cross-range rejection"
```

---

## Task 7: Per-range failover independence & sharded consistency

**Files:**
- Modify: `crates/cluster/tests/multirange.rs`

- [ ] **Step 1: Per-range failover independence test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killing_one_range_leader_does_not_stop_another_range() {
    let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
    let l0 = c.wait_for_leader(0).await;
    let mut l1 = c.wait_for_leader(1).await;
    // We need range 0 and range 1 led by DIFFERENT nodes; nudge by pausing/healing
    // until they differ (independent elections make this quick).
    let mut tries = 0;
    while l1 == l0 {
        c.pause(l1);
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        c.resume(l1);
        l1 = c.wait_for_leader(1).await;
        tries += 1;
        assert!(tries < 20, "could not get distinct leaders for ranges 0 and 1");
    }

    let mut router = cluster::range::RangeRouter::connect(&c).await;
    router.simple("CREATE TABLE a (id int4)").await.expect("a"); // range 0
    router.simple("CREATE TABLE b (id int4)").await.expect("b"); // range 1

    // Pause range 1's leader node; range 0 (led by l0, still up) must keep serving,
    // and range 1 must re-elect and resume.
    c.pause(l1);
    let mut r0 = cluster::range::RangeRouter::connect(&c).await; // re-grabs current leaders
    r0.simple("INSERT INTO a VALUES (1)").await.expect("range 0 unaffected");
    // range 1 re-elects among the survivors:
    let new_l1 = c.wait_for_leader(1).await;
    assert_ne!(new_l1, l1, "range 1 re-elected away from the paused node");
    c.heal();
    let mut r1 = cluster::range::RangeRouter::connect(&c).await;
    r1.simple("INSERT INTO b VALUES (2)").await.expect("range 1 resumed after heal");
}
```

- [ ] **Step 2: Sharded list-append consistency under follower faults**

Reuse the SP11 list-append idea, sharded across two ranges, with a **stable window between faults** (the D5 lesson — gated reads need uninterrupted windows). Tables `la0` (range 0) and `la1` (range 1); two workers each append+read their own table; a follower-fault nemesis with a 1s window; assert each table's per-key history is strict-serializable (single-range, so the existing per-key check applies independently per table).

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sharded_list_append_is_per_range_consistent_under_follower_faults() {
    let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
    for r in c.range_map().range_ids() { c.wait_for_leader(r).await; }
    let mut setup = cluster::range::RangeRouter::connect(&c).await;
    setup.simple("CREATE TABLE la0 (k int8, v int8)").await.expect("la0"); // range 0
    setup.simple("CREATE TABLE la1 (k int8, v int8)").await.expect("la1"); // range 1

    // Drive a handful of appends per table, asserting each commits and the read
    // reflects all prior appends (per-table linearizable). Run a follower-fault
    // nemesis with a 1s stable window between faults (D5 lesson).
    let nemesis = tokio::spawn({
        let c_n = c.switchboard().clone();
        async move {
            for round in 0..6u64 {
                let victim = 1 + (round % 2); // followers 1,2 (node 0 tends to lead)
                c_n.pause(victim);
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                c_n.resume(victim);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await; // stable window
            }
        }
    });

    for (table, key) in [("la0", 1i64), ("la1", 2i64)] {
        let mut r = cluster::range::RangeRouter::connect(&c).await;
        let mut expected = Vec::new();
        for v in 1..=4i64 {
            // append v, then read the whole list for key; the read must include v.
            r.simple(&format!("INSERT INTO {table}(k, v) VALUES ({key}, {v})")).await.expect("append");
            expected.push(v);
            let got = r.scan_col_i64(&format!("SELECT v FROM {table} WHERE k = {key}")).await;
            assert_eq!(got, expected, "{table} read must reflect all prior appends");
        }
    }
    nemesis.await.ok();
}
```

(Add a `scan_col_i64` test helper on `RangeRouter` mirroring `scan_one_i32`. If a write transiently fails under a fault, wrap the `INSERT`/`SELECT` in a bounded retry — re-`connect` the router to pick up the new leader — rather than asserting first-try success.)

- [ ] **Step 3: Run the suite (a few times for the timing-sensitive tests)**

Run: `cargo test -p cluster --test multirange` (run 3× to confirm the failover/sharded tests are stable).
Expected: PASS every run.

Run: `cargo clippy -p cluster --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/cluster/tests/multirange.rs crates/cluster/src/range/router.rs
git commit -m "test(cluster): D3a per-range failover independence + sharded consistency under faults"
```

---

## Task 8: Gauntlet + traceability + finish

**Files:**
- Modify: `docs/superpowers/specs/2026-06-13-crabgresql-sp13-d3a-multirange-core-design.md` (append a traceability table)

- [ ] **Step 1: Full-workspace fmt + clippy**

Run: `cargo fmt --all` then `cargo fmt --all --check`
Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: both clean. (Subagents don't auto-fmt; expect this to reformat T1–T7 — commit it.)

- [ ] **Step 2: Full-workspace test**

On Windows set `$env:__COMPAT_LAYER='RunAsInvoker'` first.

Run: `cargo test --workspace`
Expected: all suites pass, 0 failures — including the new `cluster` multirange tests AND every prior suite (the T2/T3 behavior-preserving refactors must leave single-range behavior unchanged).

- [ ] **Step 3: Supply-chain + native checks**

Run: `cargo deny check` → pass (no new dependency).
Run: `bash scripts/check-no-native.sh` → windows-sys-only known false-positive (green on Linux CI).

- [ ] **Step 4: Append the traceability table to the spec**

Append a `## Traceability (implemented)` table mapping the spec's 8 success criteria to the tests that verify them (RangeMap unit tests → #1; `every_range_elects_a_leader` → #2; `rows_land_only_in_their_table_range` → #3; the create/insert/select router test → #4; `killing_one_range_leader_does_not_stop_another_range` → #5; `sharded_list_append_…` → #6; `a_transaction_may_not_span_ranges` → #7; `cargo deny` + workspace clippy/test → #8).

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/2026-06-13-crabgresql-sp13-d3a-multirange-core-design.md crates/
git commit -m "docs+style(sp13): D3a traceability table; cargo fmt sweep"
```

- [ ] **Step 6: Final review + finish**

Dispatch a final whole-diff code review (per `superpowers:requesting-code-review`), address findings, then use `superpowers:finishing-a-development-branch` to push a fresh branch and open the PR against `main`.

---

## Notes for the implementer

- **Stale IDE diagnostics:** rust-analyzer squiggles lag the committed tree here — trust `cargo clippy --all-targets -- -D warnings` and `cargo test`, not the editor.
- **T2 & T3 are behavior-preserving:** the existing `cluster`/`executor` suites are the regression gate. If any previously-passing test changes behavior, you broke the refactor — fix it, don't edit the test.
- **`catalog_kv` is the seam:** single-range passes `kv` for both catalog and data; only the multi-range data engines point `catalog_kv` at range 0. Do not duplicate the catalog into data ranges.
- **Faults are node-scoped:** pausing a node affects all its co-located ranges — that's intended and is enough to show per-range failover independence (pick a node that leads R but not S).
- **Parser surface (verified):** every statement is single-table — `Insert`/`Update`/`Delete` carry one `table: String`, `SelectStmt.from` is `Option<String>`, and there are no joins in the grammar. So a single statement is never cross-range; only a transaction spanning ranges is, handled by the router's txn-pinning. Don't add join detection.
- **Timing-sensitive tests** (T7): apply the D5 nemesis stable-window lesson and bounded leader-reconnect retries; confirm stability across ≥3 local runs before relying on CI.
- **`run` exposure:** `SqlSession::run` is a thin pub wrapper over the private `run_one`; keep `run_one` private.
```
