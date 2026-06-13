# SP12 / D5 — Linearizable reads (ReadIndex gate)

**Goal:** Close the stale-read gap a deposed leader exposes — pinned by SP11
Scenario B — by gating every read behind an openraft **ReadIndex** check. A node
serves a read from its local MVCC state only after confirming, via a quorum
heartbeat, that it is still the leader *and* its state machine has applied through
the read point.

**Architecture:** A `Linearizer` seam mirroring the existing `Committer` seam: a
no-op `LocalLinearizer` for single-node, and a `RaftLinearizer` (cluster crate)
that calls `Raft::ensure_linearizable()`. The session calls the gate immediately
before establishing each read snapshot. On a deposed leader the gate returns a
retryable `NotLeader` error instead of stale rows; the client reconnects and SP10
routing sends it to the real leader.

**Tech stack:** openraft 0.9.24 (`Raft::ensure_linearizable`), existing
executor/cluster/pgwire crates. No new dependency. `#![forbid(unsafe_code)]`,
pure-Rust unchanged.

---

## The gap this closes

SP11 Scenario B (`leader_failover_surfaces_stale_read_d5_gap`) deterministically
surfaces a stale read: a leader **L** is partitioned from the other two nodes but
still self-reports `Leader` for ~`election_timeout`. The majority elects **L'**;
an append `v2` is committed via L'. A client that connects directly to L and reads
gets L's stale applied state (`[v1]`, missing the acked `v2`). Today
`serve_routed` serves the read locally whenever `current_leader == me`, reading
straight from local MVCC applied state — **reads bypass Raft entirely**.

Writes are already safe: every write routes through `RaftCommitter::commit`, which
resolves only once committed to a majority *and* applied; a deposed leader's
proposal can't commit. **Reads are the only hole**, exactly what Scenario B asserts
(`!is_consistent()` — "the D5 gap is present"). This slice removes the gap and
flips that assertion to `is_consistent()`.

## Scope (D5) and what is deferred

D2 (distribution) is complete: D2a durable storage (SP8), D2b network/multiprocess
(SP9), D2c leader routing (SP10), D2d over-the-wire serializability checking (SP11).
D5 adds linearizable reads.

| In scope | Deferred |
|---|---|
| ReadIndex gate on every client-visible read (SELECT, locking SELECT). | **Leader leases** (serve reads with no round-trip while a time-bounded lease holds) — a latency optimization over ReadIndex; relies on bounded clock drift and is hand-rolled (openraft exposes no state-machine lease-read). |
| Gate fires at read-snapshot establishment (once per txn for REPEATABLE READ; per statement for READ COMMITTED / autocommit). | **Follower reads** (serving stale-but-bounded reads off followers) — needs a staleness contract; not wanted here. |
| Deposed-leader read → retryable `NotLeader` error; client reconnects + re-routes. | **Read-modify-write serializability** (write skew / G2) — a serializability concern, not read-linearizability; the engine remains SI. |
| Writes unchanged (already linearized through the committer). | **Connection-level fast-fail / force-close** on gate failure — a UX nicety; the statement-level error is sufficient and correct. |

## Decisions (locked during brainstorming)

1. **Strategy: ReadIndex per transaction.** Gate at snapshot establishment — once
   at `BEGIN` under REPEATABLE READ (the fixed snapshot serves the whole txn), per
   statement under READ COMMITTED and autocommit (each statement takes a fresh
   snapshot). This ties the gate to the MVCC snapshot lifecycle: the snapshot is
   taken *after* the gate returns, so it reflects applied ≥ `read_log_id`.
2. **Seam: a dedicated `Linearizer` trait** (not folded into `Committer`). Keeps
   single-responsibility and keeps openraft out of the `executor` crate.
3. **Failure: retryable statement error** (`NotLeader`), not a transparent
   proxy-failover (mid-session txn state can't be migrated). Matches how SP10
   routing already expects clients to retry.

## Why ReadIndex is correct and bounded

`Raft::ensure_linearizable()` is openraft's optimized ReadIndex (no blank-log
write): it calls `get_read_log_id()` (which confirms leadership by sending
heartbeats to a quorum of voters and returns `read_log_id = max(committed,
leader's-initial-blank-log-id)` plus `applied`), then blocks until
`applied_index ≥ read_log_id.index`. Returns `Ok(read_log_id)` on success, or
`Err(RaftError<CheckIsLeaderError>)` if it sees a higher term
(`ForwardToLeader`) or can't reach a quorum (`QuorumNotEnough`).

On a **partitioned** leader the quorum heartbeats each time out after
`heartbeat_interval`; the confirmation loop drains without a quorum and returns
`Err(QuorumNotEnough)` — **bounded, no hang, and faster than the election
timeout**. This is the property Scenario B relies on. On a **healthy** leader (even
under SP11 Scenario A's single-follower fault, where 2/3 voters remain reachable)
the gate confirms and the read proceeds.

## Components

### 1. `Linearizer` seam — `crates/executor/src/read_gate.rs` (new)

```rust
#[async_trait::async_trait]
pub trait Linearizer: Send + Sync {
    /// Confirm this node may serve a linearizable read now. Replicated: confirm
    /// leadership via a quorum heartbeat and block until the local state machine
    /// has applied through the read log id. Err(NotLeader) if leadership can't be
    /// confirmed (deposed/partitioned), so the caller rejects the read rather than
    /// serving stale state.
    async fn ensure_readable(&self) -> Result<(), ExecError>;
}

/// Single-node / non-replicated: local applied state is authoritative.
pub struct LocalLinearizer;

#[async_trait::async_trait]
impl Linearizer for LocalLinearizer {
    async fn ensure_readable(&self) -> Result<(), ExecError> { Ok(()) }
}
```

New error variant `ExecError::NotLeader` (retryable, carries no rows). Exported
from the crate root alongside `Committer`/`LocalCommitter`.

### 2. `RaftLinearizer` — `crates/cluster/src/linearizer.rs` (new)

```rust
use executor::{ExecError, Linearizer};
use crate::types::TypeConfig;

pub struct RaftLinearizer {
    raft: openraft::Raft<TypeConfig>,
}

impl RaftLinearizer {
    pub fn new(raft: openraft::Raft<TypeConfig>) -> Self { Self { raft } }
}

#[async_trait::async_trait]
impl Linearizer for RaftLinearizer {
    async fn ensure_readable(&self) -> Result<(), ExecError> {
        self.raft
            .ensure_linearizable()
            .await
            .map(|_read_log_id| ())
            .map_err(|_| ExecError::NotLeader)
    }
}
```

Exported from `cluster` root next to `RaftCommitter`.

### 3. Gate placement — `crates/executor/src/session.rs`

`SqlSession` gains `linearizer: Arc<dyn Linearizer>`. `begin()`, `read_context()`,
and `run_select()` become `async` (`run_select_locking` already is; all run inside
the async `run_one`). The gate fires immediately before each `procarray.snapshot()`
that feeds a read, so the snapshot reflects applied ≥ `read_log_id`:

| Read path | Isolation | Gate site |
|---|---|---|
| `begin()` | REPEATABLE READ | once, before the fixed snapshot — serves the whole txn |
| `read_context()` autocommit (Idle) | autocommit SELECT | per statement, before the fresh snapshot |
| `read_context()` RC in-txn refresh | READ COMMITTED | per statement, before the refreshed snapshot |
| `run_select_locking` RC refresh / autocommit | FOR UPDATE / FOR SHARE read | per statement, before its snapshot |

REPEATABLE-READ in-txn reads after `BEGIN` reuse the already-gated fixed snapshot —
no second gate. Write paths (`run_write`, the write half of a locking SELECT's
lock acquisition) are **not** gated: writes route through `committer.commit()`,
which can't commit on a deposed leader, so a stale read-portion can never become a
durable write. A write-only REPEATABLE READ txn pays one redundant gate at
`BEGIN`; that is a fail-fast (a non-leader can't commit anyway), not a bug.

### 4. Wiring

- `SqlEngine` gains `linearizer: Arc<dyn Linearizer>`.
  - `SqlEngine::new(...)` (single-node) → `Arc::new(LocalLinearizer)`.
  - `SqlEngine::replicated(...)` gains a `linearizer: Arc<dyn Linearizer>` param,
    threaded into each `SqlSession`.
- `cluster::ServerNode` constructs `RaftLinearizer::new(raft.clone())` next to
  `RaftCommitter` and passes it to `SqlEngine::replicated`.

### 5. Failure mapping — `crates/executor` → `crates/pgwire`

`ExecError::NotLeader` maps to a `PgError` with SQLSTATE `40001`
(`serialization_failure` — the class poolers/clients retry on), message
`"not leader: retry against the current leader"`, returning **no row data**. Inside
a transaction block it propagates through `run_one`, which transitions the block to
`Failed` (existing machinery) — the client rolls back, reconnects, and SP10 routing
sends it to the current leader.

## Success criteria

| # | Criterion | Verified by |
|---|---|---|
| 1 | `Linearizer` seam exists; single-node engine uses the no-op `LocalLinearizer` and reads are unchanged. | executor unit test (`LocalLinearizer` regression guard) + existing suites stay green |
| 2 | A gate that returns `Err` causes a read to fail with `NotLeader` and return **zero rows**. | executor unit test with a `FakeLinearizer` |
| 3 | `RaftLinearizer` rejects reads on a deposed/partitioned leader and admits them on a healthy leader. | Scenario B (reject) + Scenario A-style positive read check (admit) |
| 4 | **Scenario B flips**: the direct read to the deposed leader errors (no stale `[v1]`), a routed read returns the fresh `[v1, v2]`, and the recorded history is `is_consistent()`. | `jepsen_elle.rs` flipped Scenario B + clean EDN artifact |
| 5 | Reads under tolerated faults always reflect the latest acked append (no stale reads). | read-heavy linearizability e2e under follower faults |
| 6 | No new dependency; `#![forbid(unsafe_code)]` preserved; full gauntlet green. | gauntlet + `cargo deny` + `check-no-native` |

## Test plan

1. **Unit (executor):** `FakeLinearizer { result }` → a SELECT with `Err` yields
   `ExecError::NotLeader` and no rows; `LocalLinearizer` → reads behave exactly as
   today. Cover autocommit, READ COMMITTED in-txn, REPEATABLE READ (gate at BEGIN),
   and locking SELECT.
2. **Flip Scenario B** (`crabgresql/tests/jepsen_elle.rs`): repurpose the
   gap-finder into a linearizable-read assertion. Isolate L, elect L', append `v2`
   on L'. Assert: the direct read to deposed L returns `NotLeader` (no `[v1]`
   leaked); the harness reconnects through routing and reads `[v1, v2]`; the
   recorded history is `is_consistent()`. Emit the (now clean) EDN artifact.
3. **Positive read linearizability under follower faults:** a read-heavy workload
   (3 nodes, single-follower nemesis) where each read must observe the latest acked
   append; assert `all_keys_consistent`.
4. **Gauntlet:** `cargo fmt --check`; `cargo clippy --workspace --all-targets -D
   warnings`; full workspace test; `cargo deny`; `check-no-native`; success-criteria
   traceability table.

## Non-goals

Leader leases, follower reads, read-modify-write serializability (SSI), and
connection-level force-close on gate failure are all out of scope (see the Deferred
column above). This slice is the minimal correct ReadIndex gate and the
Scenario-B TDD flip.
