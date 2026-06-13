# crabgresql SP7: Single-range Raft replication (openraft, in-process, fault-injected)

**Date:** 2026-06-12
**Status:** Approved
**Program spec:** `docs/superpowers/specs/2026-06-11-crabgresql-program-and-wire-protocol-design.md`
**Predecessors:** SP1 (wire), SP2 (vertical slice), SP3 (durable storage), SP4 (transactions), SP5 (PG-faithful MVCC visibility), SP6 (concurrent writers) — all merged.

## Goal

Begin the program's distribution arc (roadmap item 4). SP7 is the **first slice
(D1) of a multi-slice distribution sub-project**: replicate the existing
single-node engine through a **single Raft group** so committed writes survive
the loss of a node and a new leader takes over without data loss or counter
reuse. The whole SQL/MVCC/concurrency stack from SP1–SP6 runs **unchanged** on
top of a replicated state machine.

Consensus is **not reinvented**: SP7 builds on the [`openraft`](https://crates.io/crates/openraft)
crate (pinned to the **0.9** stable line). crabgresql's work is the
**integration** — a state machine over our KV, a controllable in-process
network, the write seam that funnels durable mutations through the Raft log, and
the correctness glue (counter monotonicity across failover) — plus a rigorous,
fault-injecting test layer. We trust openraft for the consensus protocol and
test *our adapters*, not Raft safety.

Constraints unchanged: `#![forbid(unsafe_code)]` everywhere; **pure-Rust shipped
tree** (openraft and its dependencies are pure Rust, MIT/Apache-2.0 — no new
`-sys`/`cc` crates beyond the existing tokio tree); parity baseline
PostgreSQL 18.

## Scope of this slice (D1) and what is deferred

This sub-project decomposes into slices, each its own spec → plan → impl cycle:

| Slice | Scope |
|---|---|
| **D1 = SP7 (this spec)** | Single-range Raft core: openraft adapters (in-memory log + state machine), a controllable in-process network, the SQL-engine integration, full fault-injection + Jepsen-style + Stateright testing. In-process only. |
| D2 | Real network transport + multi-process nodes; **persistent** Raft log/state machine + node-restart recovery; SQL frontend routes to the leader; real over-the-wire Jepsen (Elle). |
| D3 | Range descriptors + meta/addressing range + leaseholder request routing. |
| D4 | Range splits (grow → split, each half its own Raft group). |
| D5 | Leases (local **linearizable** reads) + rebalancing / up-down-replication. |

Cross-range distributed transactions are roadmap item 5, a later sub-project.

**D1 is deliberately in-process and ephemeral:** the 3 replicas live in one
process, connected by channels, with **in-memory** Raft log *and* state machine.
There are no sockets, no on-disk Raft state, and **no node-restart recovery**
(all persistence is D2). The existing durable single-node path (`SqlEngine::open`
over `FjallKv`) is **untouched** and remains the shipping default.

## The shift

SP6 runs one `SqlEngine` over one `Arc<dyn Kv>`; a write is a synchronous
`kv.write_batch`. SP7 interposes Raft between the engine and durable state on a
new **replicated path**: a write becomes *propose → replicate to a majority →
apply*. The state machine that applies entries **is** a KV; the engine reads from
that applied KV and proposes writes through the log. Only the **leader** serves
SQL; followers replicate and stand ready to take over.

The engine's **read** path is unchanged — it reads the applied state-machine
store exactly as it reads any `Kv` today. Only the handful of durable **write**
sites change: they route through a new async **`Committer`** seam whose local
impl is byte-for-byte SP6 behavior and whose replicated impl proposes to Raft.

## Architecture

### New crate: `cluster`

All consensus/replication code lives in a new `crates/cluster` crate so openraft
stays out of the executor's concerns and there is no dependency cycle. **The
executor does not depend on `cluster`;** `cluster` depends on `kv`, `executor`,
and `openraft`. Layout:

```
crates/cluster/src/
  types.rs      # declare_raft_types!(TypeConfig); WriteBatch AppData; NodeId/Node aliases
  store.rs      # in-memory RaftLogStorage + RaftStateMachine (SM wraps Arc<MemKv>)
  network.rs    # controllable in-process RaftNetwork/Factory + Switchboard (fault injection)
  node.rs       # Node { raft, sm_kv }; build + initialize a single-range group
  committer.rs  # RaftCommitter: executor::Committer over raft.client_write
  cluster.rs    # test harness: spin N nodes, wire the Switchboard, drive faults
crates/executor/src/commit.rs   # NEW: Committer trait + LocalCommitter (in executor)
```

### openraft type config (`types.rs`)

`declare_raft_types!(TypeConfig: D = WriteBatch, R = ())` with:

- **`D` (AppData) = `WriteBatch`** — a newtype over `Vec<kv::WriteOp>`. SP7 adds
  `serde::{Serialize, Deserialize}` derives to `WriteOp` (it already derives
  `Debug, Clone, PartialEq, Eq`); serde is an existing workspace dependency.
- **`R` (AppDataResponse) = `()`** — apply returns nothing the SQL layer needs;
  `client_write` *resolving* is the commit acknowledgement.
- `NodeId = u64`, `Node = openraft::BasicNode`, `Entry = openraft::Entry<TypeConfig>`,
  `SnapshotData = std::io::Cursor<Vec<u8>>`, `AsyncRuntime = TokioRuntime`.

### State machine + log store (`store.rs`)

**State machine** (`RaftStateMachine` + a `RaftSnapshotBuilder`) wraps
`sm_kv: Arc<MemKv>`, `last_applied: Option<LogId>`, and `last_membership`:

- `apply(entries)` — for each entry, apply its `WriteBatch` to `sm_kv`. **Counter
  keys** (`kv::key::next_xid_key()` and `kv::key::seq_key(t)`) apply with
  **max-merge** (`value = max(existing, incoming)` over the big-endian `u64`),
  *not* last-writer-wins, so out-of-order application of concurrently-proposed
  commits can never regress a counter. All other keys are plain put/delete.
  Advances `last_applied`. Returns `vec![(); n]`.
- `get_snapshot_builder` / `build_snapshot` — serialize the whole `MemKv`
  (a `BTreeMap`) plus `last_applied`/membership into `Cursor<Vec<u8>>`.
- `begin_receiving_snapshot` / `install_snapshot` — deserialize and replace
  `sm_kv` contents; this is how a wiped or far-behind follower catches up.
- `applied_state()` → `(last_applied, last_membership)`.

**Log store** (`RaftLogStorage` + `RaftLogReader`) — in-memory
`BTreeMap<u64, Entry>` + a stored `Vote`: `append`, `truncate`, `purge`,
`get_log_state`, `save_vote`/`read_vote`, modeled on openraft's memstore
reference. Both impls are validated by openraft's own conformance suite
(`openraft::testing::Suite::test_all()`).

### Controllable in-process network (`network.rs`)

`RaftNetworkFactory::new_client(target)` returns a handle that implements
`RaftNetwork`'s `vote` / `append_entries` / `full_snapshot` by looking the target
node's `Raft` up through a shared **`Switchboard`** and invoking its
`raft.vote()` / `raft.append_entries()` / `raft.install_full_snapshot()`
directly (in-process RPC — no serialization, no sockets).

The `Switchboard` is the **single fault-injection point**, holding shared state
the tests mutate:

- **partition / isolate** — drop messages crossing a configured cut (e.g. split
  `{a,b} | {c}`), modeling a network partition;
- **drop / delay** — per-target message loss/delay, scripted or probabilistic;
- **pause / resume** a node — refuse its inbound/outbound RPCs, modeling a crash;
  on resume it catches up via log replication or snapshot install.

Because the state machine and log are **in-memory**, a "crash" is modeled as
pause/resume (state retained) or as isolate-then-snapshot-catchup (the node's
applied state is rebuilt from the leader). True process restart with persistent
recovery is D2.

### The `Committer` write seam (`executor::commit`, `cluster::committer`)

The one new seam in the executor:

```rust
#[async_trait]
pub trait Committer: Send + Sync {
    async fn commit(&self, ops: Vec<WriteOp>) -> Result<(), ExecError>;
}
```

- **`LocalCommitter { kv: Arc<dyn Kv> }`** (in `executor`) — `commit` calls
  `kv.write_batch(&ops)`. This is byte-for-byte SP6 behavior, so every existing
  test passes unchanged.
- **`RaftCommitter { raft }`** (in `cluster`) — `commit` calls
  `raft.client_write(WriteBatch(ops)).await`, mapping openraft errors to
  `ExecError` (see Error handling).

`SqlEngine` gains a `committer: Arc<dyn Committer>` field. The existing
constructors (`new`, `open`, `with_kv`) default it to `LocalCommitter` over their
own `kv`, preserving today's behavior exactly. A new
`SqlEngine::replicated(node: &cluster::Node)` builds an engine whose **read** path
is `node.sm_kv` (the applied state-machine store) and whose **write** path is
`RaftCommitter` over `node.raft`. There is **no separate `ReplicatedKv` type** —
reads are simply the SM's store, and `apply` writes into that same `Arc<MemKv>`,
so reads are automatically read-your-writes.

The durable data/clog-write sites in `session.rs` (per-statement write batch,
autocommit batch, COMMIT clog, ROLLBACK/abort clog) route through
`committer.commit(...).await`; the session is already async (SP6). The two
**counter** persists (`next_xid`, `seq`) are handled per-mode by the rules below
— on the local path they stay SP6's eager persist (via `LocalCommitter`); on the
replicated path they fold into the proposed batch rather than making a separate
call.

### Counter monotonicity across failover (the load-bearing correctness device)

`next_xid` (`ProcArray`) and per-table `seq` (`SequenceManager`) **must never be
reused after a leader change**, or two transactions could share an xid (MVCC
corruption) or two rows a rowid. Three cooperating rules guarantee this on the
replicated path:

1. **Max-merge on apply** (state machine) — the counter keys never regress
   regardless of the order in which concurrently-proposed commits land in the
   log. Each allocation proposes a value strictly greater than any prior
   allocation (in-memory monotonic counter under a mutex), and apply keeps the
   max, so the applied counter is the high-water mark across all replicas.
2. **Reseed on gaining leadership** — when a node becomes leader it seeds its
   in-memory `ProcArray.next_xid` and `SequenceManager` counters from its applied
   state-machine store (reads the counter keys). A new leader therefore starts
   above every value the old leader could have handed out.
3. **Fold the counter op into the triggering batch** — on the replicated path,
   `begin_write` / `alloc` allocate from the in-memory counter and **emit their
   counter op into the same proposed batch as the write that triggered the
   allocation** (the first write statement's batch for `next_xid`; each INSERT's
   batch for `seq`) rather than persisting separately, so a counter advance and
   the data it labels land in one atomic log entry. (For autocommit that is a
   single proposal carrying data + `clog` + counters; a multi-statement
   transaction proposes one batch per write statement plus the COMMIT clog, and
   max-merge keeps counters correct across them.) The SP6 single-node
   eager-persist path is unchanged.

This per-mode fork is **isolated to `ProcArray` and `SequenceManager`** and is
the trickiest part of the slice — explicitly flagged for careful review and
covered by both the failover tests and the Stateright model.

### Single range, leader-only SQL

The whole keyspace is one Raft group of **3 in-process replicas**. Only the
leader serves SQL; a non-leader returns `NotLeader`. There are no range
descriptors, no routing, and no client-visible multi-node addressing — those are
D3. A thin harness helper (`Cluster::leader()`) sends test SQL to the current
leader.

## Data flow

**Write transaction (replicated path).** The leader runs the **unchanged SP6
path** — row locks, block-and-retry, EvalPlanQual — reading from the applied
state-machine store. Each write statement proposes its batch `{row versions,
xmax stamps, + counter ops}` through `committer.commit` (i.e.
`raft.client_write`); as in SP6, an **autocommit** statement's batch also carries
`clog = Committed`, and an explicit **COMMIT** proposes the `clog = Committed`
entry. openraft replicates each entry to a majority; the leader's state machine
applies it into `sm_kv` (counters max-merged); `client_write` resolves; the
engine returns `CommandComplete`. Followers apply entries as the commit index
advances. Because apply happens *before* `client_write` returns, a subsequent
read on the leader sees the write. (Uncommitted versions are replicated like any
bytes; MVCC visibility is governed by `clog`, which only reads Committed once the
COMMIT entry applies — so followers and a post-failover leader see exactly the
same visibility as the original leader.)

**Read.** The engine takes a `ProcArray` snapshot and scans the applied `sm_kv`
synchronously, with MVCC visibility exactly as SP5/SP6. No Raft round-trip.
(Reads are read-your-writes on the leader but **not linearizable across a
failover** — a deposed-but-unaware leader could serve a stale read. Linearizable
reads need leases, deferred to D5. This is an intentional, documented gap.)

**Failover.** The leader is isolated or paused. Followers' election timeouts
fire; openraft elects a new leader. On becoming leader it reseeds its counters
from its applied state machine and resumes serving SQL. Committed transactions
(replicated to a majority) survive. In-flight transactions on the dead leader
never reached `clog = Committed` in a committed log entry, so they are treated as
aborted — no data loss, no xid reuse.

**Minority / no quorum.** A write proposed on a node that cannot reach a majority
(it is in the minority of a partition) never commits and therefore **never
applies** — openraft only applies after majority commit. The engine surfaces
`Unavailable`; no partial state is written; the client retries.

## Error handling

`raft.client_write` failures map to `ExecError`:

- **`ForwardToLeader`** (this node is not the leader) → `ExecError::NotLeader`,
  surfaced as a **retryable** client error. In D1 the harness already targets the
  leader, so this is primarily a guard.
- **No quorum / timeout / unreachable** (minority side of a partition) →
  `ExecError::Unavailable`, mapped to a connection-class SQLSTATE (`08006`). Safe
  because no partial state is ever applied; the client retries.
- **`Fatal`** (storage error / internal panic) → propagated as an internal error
  (`XX000`).

The SP6 transactional errors (`40001` serialization_failure, `40P01`
deadlock_detected) are unchanged. The two new `ExecError` variants and their
exact SQLSTATE strings are small, additive decisions; the **semantics** are fixed
here: `NotLeader` is retryable, `Unavailable` leaves no partial state. I/O errors
still map to `58030`; no panics on any path.

## Testing

Four legs, all pure-Rust `cargo test` (the consistency tooling is dev-only, like
the existing libpg_query oracle, and exempt from the shipped-tree purity rule):

1. **openraft adapter conformance.** `openraft::testing::Suite::test_all()`
   validates the in-memory log store and state machine against openraft's own
   suite. Plus targeted unit tests: counter **max-merge** apply (out-of-order
   proposals do not regress `next_xid`/`seq`), snapshot **build → install**
   round-trip restores an identical `MemKv`.

2. **Deterministic fault-injection scenarios** (`Switchboard` + `Raft::wait`, no
   sleeps — assertions await metrics conditions like "a leader exists",
   "applied_index ≥ N"):
   - **replication** — a write on the leader is applied on all 3 replicas;
   - **follower catch-up** — pause a follower, commit N writes, resume → it
     catches up via log replication; applied indices converge;
   - **snapshot install** — keep a follower paused past the log-purge threshold so
     it must receive a snapshot; resume → it catches up via `install_snapshot`;
   - **leader failover** — isolate the leader → a new leader is elected → writes
     continue → assert counters were reseeded (the next allocated xid/rowid is
     above the old leader's high-water mark, **no reuse**);
   - **partition 2 | 1** — the minority returns `Unavailable`; heal → converge;
   - **deposed leader** — isolate the leader, commit via the new leader, heal the
     old leader → the old leader's uncommitted proposals are discarded.

3. **SQL-over-Raft end-to-end.** A 3-node `Cluster`; SQL routed to the leader:
   `CREATE TABLE` / `INSERT` / `SELECT` / `UPDATE` / `DELETE` + `BEGIN`/`COMMIT`
   all behave as single-node. **Kill the leader mid-workload** → the new leader
   serves complete, consistent committed data. SP6 concurrent-writer scenarios
   (same-row conflict, EvalPlanQual, `FOR UPDATE`) run correctly over the
   replicated path.

4. **Jepsen-style consistency + Stateright.**
   - *Harness* — a randomized concurrent **bank** workload (transfers between
     accounts, each transfer one transaction) driven against the leader, with the
     `Switchboard` as a **nemesis** injecting partitions, pauses, and leader-kills
     mid-run. Every operation is recorded as an `invoke` / `ok` / `fail` / `info`
     history entry. The recorder is structured so the history can later be
     exported to Elle/EDN for real over-the-wire Jepsen in D2.
   - *Checker* — the recorded history is checked with **Stateright**'s
     consistency/linearizability tester, plus a **bank-conservation** invariant
     (the sum of balances is constant across the whole history). Write
     linearizability is checked via openraft's `ensure_linearizable()`. The
     checker targets what D1 actually guarantees — **serializability /
     conservation of committed transactions** and **write-linearizability** — and
     explicitly does **not** assert strict read-linearizability (a known D5 gap).
   - *Model* — a small, focused **Stateright `Model`** that exhaustively
     model-checks the two highest-risk integration invariants: **counter
     monotonicity across failover** (max-merge + reseed ⇒ no xid/rowid reuse under
     any interleaving) and **commit durability across leader change** (a write
     acknowledged to the client is never lost after an election). This is a model
     of *our integration logic*, not a re-model of Raft (openraft owns that).

5. **Regression / gauntlet.** All SP1–SP6 gates stay green; the existing 224
   tests pass unchanged (the local path is byte-for-byte identical). `cargo fmt
   --check`, `clippy --workspace --all-targets -D warnings`, `cargo test
   --workspace`, `scripts/check-no-native.sh` (openraft is pure Rust → green),
   `cargo deny check` (openraft is MIT/Apache, adds no native crate → green),
   parser oracle, conformance parity at or above the SP6 baseline.

## Scope boundaries (tracked OUT)

- **Deferred to later distribution slices:** real network transport / sockets,
  multi-process nodes, persistent Raft log + state machine, node-restart recovery
  (all **D2**); range descriptors + leaseholder routing (**D3**); range splits
  (**D4**); read leases / linearizable local reads + rebalancing / dynamic
  membership changes beyond the fixed 3-node group (**D5**); cross-range
  distributed transactions (roadmap item 5).
- **Deferred Jepsen depth:** the real over-the-wire Jepsen control plane (Clojure,
  JDBC client, ssh-driven nemesis) and the **Elle** transactional-anomaly checker
  arrive in D2 when the wire + multi-process cluster exist; D1 records
  Elle-exportable histories but checks them in-process with Stateright.
- **Deferred reads:** strict **read-linearizability** (stale reads from a deposed
  leader are possible without leases) — D5.
- **Pre-existing carry-overs** stay deferred: vacuum/GC of dead versions and clog
  truncation/xid freezing; `pgwire::engine::oids` INT4/TEXT duplication;
  `conformance::split_statements` Latin-1 corner; `kv::FjallKv::scan_prefix` full
  materialization; the hand-written parser reserves all keywords; `cargo deny`
  advisories masked via documented ignores pending an upstream `rustls-rustcrypto`
  bump.

## Success criteria

1. A new `cluster` crate runs a **single-range, 3-replica** Raft group on
   openraft 0.9, with an in-memory log store and a state machine that wraps a
   `MemKv`; both storage impls pass `openraft::testing::Suite::test_all()`.
2. The SQL/MVCC/concurrency stack runs **unchanged** over the replicated path via
   the `Committer` seam: `CREATE`/`INSERT`/`SELECT`/`UPDATE`/`DELETE` and
   transactions behave identically to single-node, and **all 224 existing tests
   pass** (the local path is byte-for-byte unchanged).
3. A committed write is replicated to a majority and **survives leader failover**;
   after an election the new leader serves complete, consistent committed data
   with **no data loss and no xid/rowid reuse** (counters reseeded).
4. A write that cannot reach a majority returns **`Unavailable`** with **no
   partial state**; a non-leader returns a retryable **`NotLeader`**.
5. The **controllable in-process network** deterministically drives replication,
   follower catch-up, **snapshot install**, leader failover, partition/heal, and
   deposed-leader scenarios — asserted via `Raft::wait`, no sleeps, no hangs.
6. A **Jepsen-style** bank workload under a partition/pause/leader-kill nemesis
   produces a history that passes a **Stateright** consistency check and a
   **bank-conservation** invariant; a focused **Stateright model** exhaustively
   verifies counter-monotonicity-across-failover and commit-durability.
7. All SP1–SP6 gates green; **pure-Rust shipped tree** (`check-no-native.sh`,
   `cargo deny`) and `forbid(unsafe_code)` hold with openraft added; conformance
   parity unchanged or improved.
