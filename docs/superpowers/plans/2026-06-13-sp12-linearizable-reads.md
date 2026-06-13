# SP12 / D5 — Linearizable Reads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the deposed-leader stale-read gap (SP11 Scenario B) by gating every read behind an openraft ReadIndex check, so a node serves a read from local MVCC state only after confirming — via a quorum heartbeat — that it is still the leader and its state machine has applied through the read point.

**Architecture:** A `Linearizer` seam mirrors the existing `Committer` seam: a no-op `LocalLinearizer` (executor) for single-node, and a `RaftLinearizer` (cluster) calling `Raft::ensure_linearizable()`. The `SqlSession` calls the gate immediately before establishing each read snapshot (once at BEGIN under REPEATABLE READ; per statement under READ COMMITTED / autocommit). On a deposed leader the gate returns the existing retryable `ExecError::NotLeader` (SQLSTATE 40001) instead of stale rows; the client reconnects and SP10 routing sends it to the real leader.

**Tech Stack:** Rust 2024, openraft 0.9.24 (`Raft::ensure_linearizable`), `async_trait`, existing executor/cluster/pgwire crates, tokio-postgres test harness. No new dependency. `#![forbid(unsafe_code)]` preserved.

**Spec:** `docs/superpowers/specs/2026-06-13-crabgresql-sp12-linearizable-reads-design.md`

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `crates/executor/src/read_gate.rs` | The linearizable-read seam: `Linearizer` trait + no-op `LocalLinearizer`. | **Create** |
| `crates/executor/src/lib.rs` | Module list, exports, `SqlEngine` construction/wiring. | Modify |
| `crates/executor/src/session.rs` | `SqlSession` holds the linearizer; gate at each read-snapshot site. | Modify |
| `crates/executor/tests/linearizable_reads.rs` | Unit test: gate rejects reads (40001, no rows) / admits reads / not-gated writes. | **Create** |
| `crates/cluster/src/linearizer.rs` | `RaftLinearizer` calling `ensure_linearizable()`, error mapping. | **Create** |
| `crates/cluster/src/lib.rs` | Module list + export. | Modify |
| `crates/cluster/src/node.rs` | In-process `Node::engine()` passes a `RaftLinearizer`. | Modify (`:120`) |
| `crates/cluster/src/server_node.rs` | `ServerNode` passes a `RaftLinearizer`. | Modify (`:96`) |
| `crates/crabgresql/tests/jepsen_elle.rs` | Flip Scenario B from "gap present" to "read is linearizable"; add `try_read`. | Modify |

**No `error.rs` change:** `ExecError::NotLeader` already exists (`error.rs:31-32`) and already maps to SQLSTATE `40001` (`error.rs:66-68`). The read gate reuses it.

**Async ripple:** `SqlSession::begin()`, `read_context()`, and `run_select()` become `async` (the gate is async). `run_select_locking()` is already async. All are called from the already-async `run_one()`, so the only edits are adding `.await` at their call sites in `run_one`.

---

## Task 1: `Linearizer` seam + read gate (executor) + unit test

Introduces the seam and the gate. After this task the executor gates reads through whatever `Linearizer` it's given; the cluster still passes a no-op `LocalLinearizer` (activated in Task 3), so cluster behavior is unchanged.

**Files:**
- Create: `crates/executor/src/read_gate.rs`
- Modify: `crates/executor/src/lib.rs`
- Modify: `crates/executor/src/session.rs`
- Modify: `crates/cluster/src/node.rs:120-125`, `crates/cluster/src/server_node.rs:95-98`
- Test: `crates/executor/tests/linearizable_reads.rs` (create)

- [ ] **Step 1: Write the failing unit test**

Create `crates/executor/tests/linearizable_reads.rs`:

```rust
//! D5: the read gate (`Linearizer`) rejects reads on a deposed leader and admits
//! them on a healthy one; writes are never gated (they go through the committer).
use std::sync::Arc;

use executor::{Committer, ExecError, Linearizer, SqlEngine};
use kv::{Kv, MemKv, WriteOp};
use pgwire::engine::{Engine, QueryResult, Session};

/// Commits straight to a shared in-memory KV (stands in for RaftCommitter).
struct MemCommitter {
    kv: Arc<dyn Kv>,
}
#[async_trait::async_trait]
impl Committer for MemCommitter {
    async fn commit(&self, ops: Vec<WriteOp>) -> Result<(), ExecError> {
        self.kv.write_batch(&ops)?;
        Ok(())
    }
}

/// A read gate that always rejects — a deposed/partitioned leader.
struct DeposedLeader;
#[async_trait::async_trait]
impl Linearizer for DeposedLeader {
    async fn ensure_readable(&self) -> Result<(), ExecError> {
        Err(ExecError::NotLeader)
    }
}

/// A read gate that always admits — a healthy leader.
struct HealthyLeader;
#[async_trait::async_trait]
impl Linearizer for HealthyLeader {
    async fn ensure_readable(&self) -> Result<(), ExecError> {
        Ok(())
    }
}

fn engine(linearizer: Arc<dyn Linearizer>) -> SqlEngine {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    SqlEngine::replicated(
        Arc::clone(&kv),
        Arc::new(MemCommitter { kv: Arc::clone(&kv) }),
        linearizer,
    )
    .expect("replicated engine")
}

#[tokio::test]
async fn deposed_leader_rejects_autocommit_read_with_40001() {
    let mut s = engine(Arc::new(DeposedLeader)).connect();
    // DDL + writes are NOT gated (they go through the committer) → succeed.
    s.simple_query("CREATE TABLE t (id int4)").await.expect("create");
    s.simple_query("INSERT INTO t VALUES (1)").await.expect("insert");
    // The read IS gated → rejected with the retryable 40001, no rows.
    let err = s
        .simple_query("SELECT id FROM t")
        .await
        .expect_err("read must be rejected on a deposed leader");
    assert_eq!(err.code, "40001", "deposed-leader read maps to retryable 40001");
}

#[tokio::test]
async fn deposed_leader_gates_read_committed_in_txn_select_not_begin() {
    let mut s = engine(Arc::new(DeposedLeader)).connect();
    s.simple_query("CREATE TABLE t (id int4)").await.expect("create");
    // Plain BEGIN is READ COMMITTED → its placeholder snapshot is refreshed per
    // statement, so BEGIN itself is NOT gated.
    s.simple_query("BEGIN").await.expect("plain begin is not gated");
    let err = s
        .simple_query("SELECT id FROM t")
        .await
        .expect_err("RC in-txn select is gated");
    assert_eq!(err.code, "40001");
    s.simple_query("ROLLBACK").await.ok();
}

#[tokio::test]
async fn deposed_leader_gates_repeatable_read_at_begin() {
    let mut s = engine(Arc::new(DeposedLeader)).connect();
    s.simple_query("CREATE TABLE t (id int4)").await.expect("create");
    // REPEATABLE READ fixes its snapshot at BEGIN, so the gate fires at BEGIN.
    let err = s
        .simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect_err("RR begin is gated");
    assert_eq!(err.code, "40001");
}

#[tokio::test]
async fn healthy_leader_admits_reads() {
    let mut s = engine(Arc::new(HealthyLeader)).connect();
    s.simple_query("CREATE TABLE t (id int4)").await.expect("create");
    s.simple_query("INSERT INTO t VALUES (1)").await.expect("insert");
    let res = s.simple_query("SELECT id FROM t").await.expect("read admitted");
    match &res[0] {
        QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
        other => panic!("expected Rows, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run the test to verify it fails (does not compile)**

Run: `cargo test -p executor --test linearizable_reads`
Expected: FAIL — `executor::Linearizer` is not found and `SqlEngine::replicated` takes 2 args, not 3.

- [ ] **Step 3: Create the `Linearizer` seam**

Create `crates/executor/src/read_gate.rs`:

```rust
//! The linearizable-read seam. Mirrors the durable-write `Committer` seam: a read
//! confirms it may observe local state before taking its MVCC snapshot. The local
//! impl is a no-op (single-node applied state is authoritative); the replicated
//! impl (`cluster::RaftLinearizer`) performs an openraft ReadIndex check.

use crate::error::ExecError;

#[async_trait::async_trait]
pub trait Linearizer: Send + Sync {
    /// Confirm this node may serve a linearizable read now. Replicated: confirm
    /// leadership via a quorum heartbeat and block until the local state machine
    /// has applied through the read log id. `Err(NotLeader)` (or `Unavailable`)
    /// if leadership can't be confirmed (deposed/partitioned), so the caller
    /// rejects the read rather than serving stale state.
    async fn ensure_readable(&self) -> Result<(), ExecError>;
}

/// Single-node / non-replicated: local applied state is authoritative, so a read
/// is always immediately serveable.
pub struct LocalLinearizer;

#[async_trait::async_trait]
impl Linearizer for LocalLinearizer {
    async fn ensure_readable(&self) -> Result<(), ExecError> {
        Ok(())
    }
}
```

- [ ] **Step 4: Register and export the module in `lib.rs`**

In `crates/executor/src/lib.rs`, add the module next to `mod commit;` (line 8):

```rust
mod read_gate;
```

Add the export next to `pub use commit::{Committer, LocalCommitter};` (line 23):

```rust
pub use read_gate::{Linearizer, LocalLinearizer};
```

- [ ] **Step 5: Add the `linearizer` field to `SqlEngine` and wire construction**

In `crates/executor/src/lib.rs`, add the field to `SqlEngine` (after `committer`, line 53):

```rust
    pub(crate) linearizer: Arc<dyn crate::read_gate::Linearizer>,
```

In `with_kv` (single-node), set it (in the returned `Self { .. }`, after `committer,`):

```rust
            linearizer: Arc::new(crate::read_gate::LocalLinearizer),
```

Change `replicated` to take a `linearizer` parameter and store it:

```rust
    pub fn replicated(
        sm_kv: Arc<dyn Kv>,
        committer: Arc<dyn crate::commit::Committer>,
        linearizer: Arc<dyn crate::read_gate::Linearizer>,
    ) -> Result<Self, ExecError> {
        let procarray = Arc::new(ProcArray::open(Arc::clone(&sm_kv), PersistMode::Replicated)?);
        Ok(Self {
            kv: sm_kv,
            procarray,
            seq: Arc::new(SequenceManager::new(PersistMode::Replicated)),
            lockmgr: Arc::new(RowLockManager::new()),
            catalog_lock: Arc::new(tokio::sync::Mutex::new(())),
            committer,
            linearizer,
            persist_mode: PersistMode::Replicated,
        })
    }
```

In `Engine::connect` (the `SqlSession::new(...)` call), pass the linearizer (add after the `committer` arg):

```rust
            Arc::clone(&self.linearizer),
```

- [ ] **Step 6: Add the `linearizer` field + constructor param to `SqlSession`**

In `crates/executor/src/session.rs`, add the field to `SqlSession` (after `committer`, line 52):

```rust
    linearizer: Arc<dyn crate::read_gate::Linearizer>,
```

Add the parameter to `SqlSession::new` (after the `committer` param) and store it in `Self { .. }`:

```rust
        committer: Arc<dyn crate::commit::Committer>,
        linearizer: Arc<dyn crate::read_gate::Linearizer>,
        persist_mode: crate::PersistMode,
    ) -> Self {
        Self {
            kv,
            procarray,
            seq,
            lockmgr,
            catalog_lock,
            committer,
            linearizer,
            persist_mode,
            state: TxnState::Idle,
        }
    }
```

- [ ] **Step 7: Gate at the read-snapshot sites; make the read helpers async**

In `crates/executor/src/session.rs`:

**(a) `begin()` → async, gate only for REPEATABLE READ** (RR fixes its snapshot here for the whole txn; RC's placeholder is refreshed — and gated — per statement):

```rust
    async fn begin(&mut self, isolation: Option<IsolationLevel>) -> Result<QueryResult, ExecError> {
        if matches!(self.state, TxnState::InTransaction(_)) {
            return Ok(QueryResult::Command { tag: "BEGIN".into() });
        }
        let rr = matches!(isolation, Some(IsolationLevel::RepeatableRead));
        // RR reuses this snapshot for the whole txn, so confirm a linearizable read
        // point BEFORE taking it. RC re-snapshots (and re-gates) per statement.
        if rr {
            self.linearizer.ensure_readable().await?;
        }
        let snapshot = self.procarray.snapshot();
        self.state = TxnState::InTransaction(TxnCtx { xid: None, snapshot, repeatable_read: rr });
        Ok(QueryResult::Command { tag: "BEGIN".into() })
    }
```

**(b) `read_context()` → async, gate before each fresh snapshot** (autocommit and RC refresh; RR reuses the begin-gated snapshot). Compute the plan first so no borrow is held across the `await`:

```rust
    async fn read_context(&mut self) -> Result<(Snapshot, Option<u64>), ExecError> {
        enum Plan {
            Auto,
            RcRefresh,
            RrReuse,
        }
        let plan = match &self.state {
            TxnState::Idle => Plan::Auto,
            TxnState::InTransaction(c) => {
                if c.repeatable_read { Plan::RrReuse } else { Plan::RcRefresh }
            }
            TxnState::Failed(_) => unreachable!("guarded in run_one"),
        };
        match plan {
            Plan::Auto => {
                self.linearizer.ensure_readable().await?;
                Ok((self.procarray.snapshot(), None))
            }
            Plan::RcRefresh => {
                self.linearizer.ensure_readable().await?;
                let snap = self.procarray.snapshot();
                match &mut self.state {
                    TxnState::InTransaction(c) => {
                        c.snapshot = snap.clone();
                        Ok((snap, c.xid))
                    }
                    _ => unreachable!(),
                }
            }
            Plan::RrReuse => match &self.state {
                TxnState::InTransaction(c) => Ok((c.snapshot.clone(), c.xid)),
                _ => unreachable!(),
            },
        }
    }
```

**(c) `run_select()` → async** (the gate is inside `read_context`):

```rust
    async fn run_select(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        let (snapshot, own) = self.read_context().await?;
        crate::exec::execute_read(&*self.kv, &snapshot, own, stmt)
    }
```

**(d) `run_select_locking()` — gate before each fresh read snapshot.** The gate calls live inside a `match &self.state` arm, so clone the `Arc` first (`let lin = Arc::clone(&self.linearizer);`) to avoid holding a `self` borrow across the `await`.

In the `TxnState::InTransaction` arm, gate at the **top** of the arm on the RC path — before `ensure_write_xid()` so a rejected read allocates no xid/lock (RR reuses the begin-gated snapshot, so no gate). Replace the start of the arm (the `self.ensure_write_xid()?;` line through the `if refresh { ... }` block, around lines 227-236):

```rust
                // RC re-snapshots per statement → gate now, before any local work.
                // RR reuses the snapshot fixed and gated at BEGIN.
                if matches!(&self.state, TxnState::InTransaction(c) if !c.repeatable_read) {
                    let lin = Arc::clone(&self.linearizer);
                    lin.ensure_readable().await?;
                }
                self.ensure_write_xid()?;
                let refresh =
                    matches!(&self.state, TxnState::InTransaction(c) if !c.repeatable_read);
                if refresh {
                    let snap = self.procarray.snapshot();
                    if let TxnState::InTransaction(c) = &mut self.state {
                        c.snapshot = snap;
                    }
                }
```

In the `TxnState::Idle` (autocommit) arm, gate before allocating the xid (around line 264) so a rejected read does no local work — insert at the top of the arm, before `let xid = self.procarray.begin_write()?;`:

```rust
                let lin = Arc::clone(&self.linearizer);
                lin.ensure_readable().await?;
```

**(e) Update the `run_one` call sites** for the now-async helpers (around lines 86, 94):

```rust
            Statement::Begin { isolation } => self.begin(*isolation).await,
```
```rust
            Statement::Select(_) => self.run_select(stmt).await,
```

- [ ] **Step 8: Update the two cluster call sites to pass `LocalLinearizer` (temporary, no behavior change)**

In `crates/cluster/src/node.rs` (the `replicated(...)` call at line 120), add a third argument:

```rust
        executor::SqlEngine::replicated(
            self.sm_kv.clone(),
            Arc::new(crate::committer::RaftCommitter { raft: self.raft.clone() }),
            Arc::new(executor::LocalLinearizer),
        )
```

In `crates/cluster/src/server_node.rs` (line 96):

```rust
            SqlEngine::replicated(
                sm_kv,
                Arc::new(RaftCommitter { raft: raft.clone() }),
                Arc::new(executor::LocalLinearizer),
            )
            .expect("replicated engine"),
```

`server_node.rs` already imports `executor::SqlEngine`, so the fully-qualified `executor::LocalLinearizer` resolves with no new `use`.

- [ ] **Step 9: Run the unit test + clippy**

Run: `cargo test -p executor --test linearizable_reads`
Expected: PASS (4 tests).

Run: `cargo clippy -p executor -p cluster --all-targets -- -D warnings`
Expected: clean (no warnings).

- [ ] **Step 10: Commit**

```bash
git add crates/executor/src/read_gate.rs crates/executor/src/lib.rs crates/executor/src/session.rs crates/executor/tests/linearizable_reads.rs crates/cluster/src/node.rs crates/cluster/src/server_node.rs
git commit -m "feat(executor): Linearizer read-gate seam; gate reads at snapshot establishment"
```

---

## Task 2: `RaftLinearizer` (cluster) — pure addition

Adds the Raft-backed gate. Not wired in yet (Task 3), so cluster behavior is still unchanged and the workspace stays green.

**Files:**
- Create: `crates/cluster/src/linearizer.rs`
- Modify: `crates/cluster/src/lib.rs:7-21`

- [ ] **Step 1: Create `RaftLinearizer`**

Create `crates/cluster/src/linearizer.rs`:

```rust
//! A Linearizer that performs an openraft ReadIndex check before a read. Mirrors
//! `RaftCommitter`: the committer linearizes writes, this linearizes reads.
//!
//! `ensure_linearizable` confirms leadership by heartbeating a quorum and blocks
//! until the local state machine has applied through the read log id. On a
//! deposed/partitioned leader the heartbeats fail and it returns an error
//! (bounded by `heartbeat_interval`), so the read is rejected rather than served
//! from stale local state.

use executor::{ExecError, Linearizer};
use openraft::BasicNode;
use openraft::error::{CheckIsLeaderError, RaftError};

use crate::types::{NodeId, TypeConfig};

/// Performs a ReadIndex check on the leader before a read. Reads still come from
/// the applied `sm_kv`; this only confirms it is safe to observe it now.
pub struct RaftLinearizer {
    pub(crate) raft: openraft::Raft<TypeConfig>,
}

#[async_trait::async_trait]
impl Linearizer for RaftLinearizer {
    async fn ensure_readable(&self) -> Result<(), ExecError> {
        self.raft.ensure_linearizable().await.map(|_read_log_id| ()).map_err(map_err)
    }
}

/// Map openraft's `ensure_linearizable` error onto an `ExecError`. A
/// `ForwardToLeader` (this node isn't the leader) is a retryable redirect →
/// `NotLeader`; a `QuorumNotEnough` (couldn't confirm leadership) or any `Fatal`
/// → `Unavailable` (also retryable). Either way the read returns no stale rows.
fn map_err(e: RaftError<NodeId, CheckIsLeaderError<NodeId, BasicNode>>) -> ExecError {
    match e {
        RaftError::APIError(CheckIsLeaderError::ForwardToLeader(_)) => ExecError::NotLeader,
        _ => ExecError::Unavailable,
    }
}
```

- [ ] **Step 2: Register and export the module**

In `crates/cluster/src/lib.rs`, add `mod linearizer;` (after `mod durable;`, line 8) and the export (after `pub use committer::RaftCommitter;`, line 18):

```rust
pub use linearizer::RaftLinearizer;
```

- [ ] **Step 3: Build + clippy**

Run: `cargo build -p cluster`
Expected: compiles (the `pub` `RaftLinearizer` is unused but, being public API, raises no dead-code warning).

Run: `cargo clippy -p cluster --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/cluster/src/linearizer.rs crates/cluster/src/lib.rs
git commit -m "feat(cluster): RaftLinearizer — ReadIndex read gate via ensure_linearizable"
```

---

## Task 3: Activate the gate on the cluster + flip Scenario B

Swaps the no-op `LocalLinearizer` for `RaftLinearizer` at both call sites, turning on linearizable reads cluster-wide. This closes the SP11 Scenario B gap, so Scenario B's assertion must flip in the same change.

**Files:**
- Modify: `crates/cluster/src/node.rs` (the `replicated(...)` call)
- Modify: `crates/cluster/src/server_node.rs` (the `replicated(...)` call)
- Modify: `crates/crabgresql/tests/jepsen_elle.rs` (flip Scenario B; add `try_read`)

- [ ] **Step 1: Swap in `RaftLinearizer` at both call sites**

In `crates/cluster/src/node.rs`, change the third `replicated` argument from `Arc::new(executor::LocalLinearizer)` to:

```rust
            Arc::new(crate::linearizer::RaftLinearizer { raft: self.raft.clone() }),
```

In `crates/cluster/src/server_node.rs`, change it to:

```rust
                Arc::new(RaftLinearizer { raft: raft.clone() }),
```

Add `use crate::linearizer::RaftLinearizer;` to `server_node.rs` imports (next to `use crate::committer::RaftCommitter;`, line 19). (Task 1 used the fully-qualified `executor::LocalLinearizer` with no `use`, so there's nothing to remove.)

- [ ] **Step 2: Add the error-tolerant `try_read` helper**

In `crates/crabgresql/tests/jepsen_elle.rs`, add next to `read_txn` (after line 564):

```rust
/// A read that tolerates a not-leader / connection error (returns `None`) instead
/// of panicking. Post-D5 the gate makes a read on a deposed leader either error
/// (`None`) or proxy to the fresh leader — never the stale value — so this is how
/// Scenario B asserts "no stale read".
async fn try_read(client: &tokio_postgres::Client, key: i64) -> Option<Vec<i64>> {
    match tokio::time::timeout(
        Duration::from_secs(10),
        client.simple_query(&format!("SELECT val FROM appends WHERE key = {key}")),
    )
    .await
    {
        Ok(Ok(msgs)) => Some(list_from(&msgs)),
        _ => None, // timeout, NotLeader (40001), or dropped connection
    }
}
```

- [ ] **Step 3: Replace Scenario B with the linearizable-read assertion**

Replace the entire `leader_failover_surfaces_stale_read_d5_gap` function (its doc comment and body, lines ~566-703) with:

```rust
/// Scenario B (post-D5) — across a leader failover, reads are LINEARIZABLE. The
/// deposed-but-not-yet-stepped-down leader L no longer serves a stale local read:
/// the ReadIndex gate (`RaftLinearizer::ensure_readable` → `Raft::ensure_linearizable`)
/// can't confirm L's leadership against the isolated majority, so L rejects the
/// read (retryable 40001) — or, if L has stepped down, proxies to L' — instead of
/// returning the stale `[1]`. A routed read reaches the new leader L' and returns
/// the fresh `[1, 2]`. The recorded history is strict-serializable.
///
/// This is the TDD flip of SP11's gap-finder: the same orchestration that used to
/// surface the stale read now proves it can't happen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failover_read_is_linearizable() {
    use cluster::transport::protocol::ControlRequest;
    const KEY: i64 = 7;
    // Establishing a failover (electing L', committing v2) is timing-bound; retry
    // a bounded number of times. The D5 assertions below are deterministic once
    // the failover is established.
    const ATTEMPTS: usize = 15;
    for _attempt in 0..ATTEMPTS {
        let c = harness::Cluster::spawn(3).await;
        let l = c.wait_for_leader().await;
        let rec = Recorder::default();
        // Seed: table + anchor, append 1 to KEY via the leader (process 0).
        {
            let setup = c.pg(l).await;
            setup
                .simple_query("CREATE TABLE appends (key int8, val int8)")
                .await
                .expect("create");
            setup
                .simple_query("CREATE TABLE anchor (key int8)")
                .await
                .expect("create anchor");
            setup
                .simple_query(&format!("INSERT INTO anchor VALUES ({KEY})"))
                .await
                .expect("seed anchor");
        }
        let ok1 = append_txn(&c.pg(l).await, &rec, 0, KEY, 1).await;
        assert!(ok1, "seed append must commit");
        // Isolate L; the majority elects L'.
        let others: Vec<u64> = (0..3u64).filter(|&i| i != l).collect();
        c.control(l, ControlRequest::SetPartition(others.clone())).await;
        for &o in &others {
            c.control(o, ControlRequest::SetPartition(vec![l])).await;
        }
        // Bounded wait for a NEW leader among the survivors.
        let neu = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            loop {
                let mut found = None;
                for &o in &others {
                    if let Some(st) = c.status(o).await
                        && st.current_leader.is_some_and(|x| x != l)
                    {
                        found = st.current_leader;
                    }
                }
                if let Some(x) = found {
                    break x;
                }
                if tokio::time::Instant::now() >= deadline {
                    break l; // no failover; retry attempt
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        if neu == l {
            for id in 0..3u64 {
                c.control(id, ControlRequest::Heal).await;
            }
            continue;
        }
        // Commit append 2 via the new leader L' (process 1).
        let ok2 = append_txn(&c.pg(neu).await, &rec, 1, KEY, 2).await;
        if !ok2 {
            for id in 0..3u64 {
                c.control(id, ControlRequest::Heal).await;
            }
            continue;
        }
        // (1) The deposed leader must NOT serve a stale read. Read DIRECTLY from L
        // (the harness→L SQL connection isn't subject to the Raft partition): the
        // gate rejects it (None) or proxies to L' (fresh) — never the stale [1].
        let direct = try_read(&c.pg(l).await, KEY).await;
        assert_ne!(
            direct,
            Some(vec![1]),
            "deposed leader served a stale read [1] — the D5 ReadIndex gate failed"
        );
        // (2) A routed read observes the fresh, linearizable value (recorded).
        let fresh = read_txn(&c.pg(neu).await, &rec, 2, KEY).await;
        assert_eq!(fresh, vec![1, 2], "routed read after failover must be fresh");
        // Heal.
        for id in 0..3u64 {
            c.control(id, ControlRequest::Heal).await;
        }
        // History {append 1→[1]; append 2→[1,2]; read→[1,2]} is strict-serializable.
        let events = rec.take_sorted();
        assert!(
            all_keys_consistent(&events),
            "post-D5 failover read history must be strict-serializable"
        );
        let edn = history_to_elle_edn(&events);
        let path = std::env::temp_dir().join("crabgresql-sp12-linearizable-read.edn");
        std::fs::write(&path, edn).expect("write edn");
        eprintln!("wrote linearizable Elle EDN history to {}", path.display());
        return; // success: failover read proven linearizable
    }
    panic!("could not establish a leader failover within the {ATTEMPTS}-attempt budget");
}
```

- [ ] **Step 4: Run the flipped scenario + Scenario A (gate now active under faults)**

On Windows, prefix test runs with the elevation-compat shim used by the other multiprocess suites:
`$env:__COMPAT_LAYER='RunAsInvoker'` (PowerShell) before the command.

Run: `cargo test -p crabgresql --test jepsen_elle -- --nocapture`
Expected: PASS — `leader_failover_read_is_linearizable` proves the fresh read; `list_append_is_strict_serializable_under_follower_faults` (Scenario A) still passes with the gate active on every in-txn SELECT; the checker meta-tests still pass.

- [ ] **Step 5: Clippy the test crate**

Run: `cargo clippy -p crabgresql --all-targets -- -D warnings`
Expected: clean (no leftover unused `try_read`, no dead Scenario-B code).

- [ ] **Step 6: Commit**

```bash
git add crates/cluster/src/node.rs crates/cluster/src/server_node.rs crates/crabgresql/tests/jepsen_elle.rs
git commit -m "feat(cluster): activate RaftLinearizer; flip Scenario B to linearizable-read gate (D5)"
```

---

## Task 4: Gauntlet + traceability + finish

**Files:**
- Modify: `docs/superpowers/specs/2026-06-13-crabgresql-sp12-linearizable-reads-design.md` (append a traceability table)

- [ ] **Step 1: Full-workspace fmt + clippy**

Run: `cargo fmt --all --check`
Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: both clean.

- [ ] **Step 2: Full-workspace test**

On Windows, set `$env:__COMPAT_LAYER='RunAsInvoker'` first (the multiprocess suites spawn node binaries).

Run: `cargo test --workspace`
Expected: all suites pass, 0 failures. In particular the executor `linearizable_reads` unit suite, the `jepsen_elle` suite (flipped Scenario B + Scenario A + meta-tests), and the existing executor `transactions`/`concurrency`/`durability` suites (regression: the no-op `LocalLinearizer` path leaves single-node reads unchanged).

- [ ] **Step 3: Supply-chain + native checks**

Run: `cargo deny check`
Expected: pass (no new dependency was added).

Run: `bash scripts/check-no-native.sh`
Expected: the known `windows-sys`-only false-positive on Windows; green on Linux CI.

- [ ] **Step 4: Append the success-criteria traceability table to the spec**

Append to `docs/superpowers/specs/2026-06-13-crabgresql-sp12-linearizable-reads-design.md`:

```markdown
## Traceability (implemented)

| # | Criterion | Verified by |
|---|---|---|
| 1 | `Linearizer` seam; single-node uses no-op `LocalLinearizer`, reads unchanged | `executor/tests/linearizable_reads.rs::healthy_leader_admits_reads` + unchanged `transactions`/`concurrency` suites |
| 2 | A rejecting gate makes a read fail with 40001 and return no rows | `linearizable_reads.rs::deposed_leader_rejects_autocommit_read_with_40001` (+ RC-in-txn, RR-at-begin variants) |
| 3 | `RaftLinearizer` rejects on a deposed leader, admits on a healthy one | `jepsen_elle.rs::leader_failover_read_is_linearizable` (reject) + Scenario A (admit under follower faults) |
| 4 | Scenario B flips: deposed read not stale, routed read fresh, history `is_consistent()` | `jepsen_elle.rs::leader_failover_read_is_linearizable` + clean EDN artifact |
| 5 | Reads under tolerated faults reflect the latest acked append | `jepsen_elle.rs::list_append_is_strict_serializable_under_follower_faults` (Scenario A) — gate active on every in-txn SELECT |
| 6 | No new dependency; `#![forbid(unsafe_code)]`; full gauntlet green | `cargo deny` + workspace clippy/test |
```

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/2026-06-13-crabgresql-sp12-linearizable-reads-design.md
git commit -m "docs(sp12): success-criteria traceability table"
```

- [ ] **Step 6: Final review + finish**

Dispatch a final whole-diff code review (per `superpowers:requesting-code-review`), address any findings, then use `superpowers:finishing-a-development-branch` to push to a fresh branch and open the PR against `main`.

---

## Notes for the implementer

- **Stale IDE diagnostics:** rust-analyzer squiggles lag the committed tree in this repo. Trust `cargo clippy --all-targets -- -D warnings` and `cargo test`, not the editor.
- **`ExecError::NotLeader` already exists** and already maps to 40001 — do **not** add a new variant.
- **Borrow-across-await:** `read_context`'s `Plan` enum exists specifically so no `&mut self.state` borrow is held across `ensure_readable().await`. Keep that shape.
- **Why writes aren't gated:** every write resolves through `committer.commit()` (Raft), which can't commit on a deposed leader, so a stale read-portion can never become a durable write. Gating writes would be redundant.
- **Scenario B is non-deterministic only in *establishing* the failover** (electing L', committing v2); the D5 assertions after that are deterministic. Keep the bounded retry loop for setup robustness.
```
