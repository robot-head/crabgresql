# crabgresql SP8: Durable Raft storage + node-restart recovery (distribution slice D2a)

**Date:** 2026-06-12
**Status:** Approved
**Program spec:** `docs/superpowers/specs/2026-06-11-crabgresql-program-and-wire-protocol-design.md`
**Predecessors:** SP1 (wire), SP2 (vertical slice), SP3 (durable storage), SP4 (transactions), SP5 (PG-faithful MVCC visibility), SP6 (concurrent writers), SP7 (single-range Raft, in-memory / D1) — all merged.

## Goal

Make the SP7 single-range Raft cluster **durable**: replace D1's in-memory Raft
log and state machine with fjall-backed storage so a node recovers its log,
vote, and applied state after a crash, and a write acknowledged to the client
survives the loss (and restart) of nodes as long as a majority's disks survive.
This closes D1's biggest documented gap (in-memory, ephemeral, no restart). The
SQL/MVCC/concurrency stack and the openraft integration from SP7 are unchanged;
only the storage *backing* changes from in-memory to durable.

Constraints unchanged: `#![forbid(unsafe_code)]`; **pure-Rust shipped tree**
(fjall is already a shipped dependency — no new native crate); parity baseline
PostgreSQL 18.

## Scope of this slice (D2a) and what is deferred

SP7 deferred all persistence to "D2". D2 as originally sketched bundles four
large features; it decomposes into its own slices, each a spec → plan → impl
cycle:

| Slice | Scope |
|---|---|
| **D2a = SP8 (this spec)** | Durable Raft log + state machine + node-restart recovery, in-process. Crash = drop the node; restart = reopen from disk. Deterministic restart scenarios + a crash-nemesis durability test. |
| D2b | Real TCP transport + multi-process nodes + cluster membership/join (replace the in-process `Switchboard`); persistent Raft `HardState` over the wire; node recovery across real process boundaries. |
| D2c | SQL frontend leader routing — a client hitting a non-leader is redirected/forwarded; wire the `crabgresql` binary to run a replicated durable node. |
| D2d | Real over-the-wire Jepsen + Elle against the multi-process durable cluster. |

D3 (range routing), D4 (range splits), D5 (leases / rebalancing), and cross-range
distributed transactions remain later sub-projects.

**D2a is deliberately in-process.** A "crash" is modeled by dropping a node's
`openraft::Raft` (closing its fjall `Database`); a "restart" reopens it from the
same on-disk directory. There are no sockets and no multi-process nodes (D2b).
The shipped `crabgresql` binary is **not** wired to replicated mode (it needs
leader routing — D2c — to be a usable server); D2a stays a test harness, exactly
as D1 was.

## The shift

SP7 runs each of 3 in-process nodes with an in-memory `LogStore`
(`BTreeMap<u64, Entry>` + `Vote`) and a `StateMachineStore` over an
`Arc<MemKv>`. Everything is lost when the process (or a `Node`) is dropped. SP8
gives each node an on-disk directory and persists the log, vote, committed
index, and applied state, so dropping and reopening a node recovers it.

The in-memory mode is **kept**: D1's deterministic fault-injection scenarios
(elections, partitions, snapshot-install) don't need durability and shouldn't pay
fjall/temp-dir overhead, so durable storage is a **parallel path**, not a
replacement.

## Architecture

### On-disk layout: one fjall `Database` per node (`crates/cluster/src/store.rs`, `crates/kv`)

Each node = one directory = one `fjall::Database` (`fjall` already supports
multiple keyspaces per database, cross-keyspace atomic batches via `db.batch()`,
and a single `db.persist(SyncAll)` fsync). A new `cluster::store::NodeStore`
opens the database with two keyspaces:

- **`data`** — the state machine's DB content (rows, clog, `next_xid`/`seq`),
  read directly by the SQL engine.
- **`raft`** — Raft log entries (keyed by index), `vote`, `committed`,
  `last_purged`, and the state machine's `last_applied` / `last_membership`.

`NodeStore::open(dir)` returns (a) a `Kv` **view over the `data` keyspace** for
the state machine + SQL engine, and (b) a durable **`LogStore`** over the `raft`
keyspace. Both share the one `Database`, so a single `persist(SyncAll)` makes the
log and the applied data **mutually consistent on a crash** — the crash-safety
advantage of this layout.

**`KeyspaceKv` refactor (`crates/kv`).** Today `FjallKv` owns its own
`Database` + single `data` keyspace. SP8 factors the `Kv`-over-a-fjall-keyspace
logic into a `KeyspaceKv { ks, db }` that takes an already-open keyspace and a
handle to the shared `Database` (for `persist`); `FjallKv::open(path)` becomes a
thin wrapper that opens a one-keyspace database and delegates to `KeyspaceKv`.
This is additive and preserves `FjallKv`'s exact behavior (every mutation fsyncs
as its tail) — the existing SP3 durable-storage tests stay green.

### Durable `LogStore` (over the `raft` keyspace)

Ports D1's in-memory `RaftLogStorage` + `RaftLogReader` to fjall. Keys in the
`raft` keyspace:

- `log/<u64 big-endian index>` → serialized `Entry<TypeConfig>` (serde_json — the
  same format SP7 uses for snapshots; D2a optimizes for correctness, not log
  density),
- `vote` → `Vote`, `committed` → `Option<LogId>`, `last_purged` → `Option<LogId>`.

It caches `last_log_id` and `last_purged` in memory (loaded by a one-time scan on
`open`, updated on each mutation) so `get_log_state()` is O(1). Each mutating
method ends in `persist(SyncAll)`, returning only once power-loss durable
(matching `FjallKv`'s discipline):

- **`append(entries, callback)`** — batch-insert the entries under `log/<index>`,
  `persist`, **then** fire openraft's `callback.io_completed(Ok(()))`.
  Durability *before* the ack is openraft's contract and the heart of the slice.
- **`truncate(log_id)`** — delete indices `≥ log_id.index` (log-divergence
  rollback) + persist.
- **`purge(log_id)`** — delete indices `≤ log_id.index` (post-snapshot
  compaction), advance cached `last_purged` + persist.
- **`save_vote`/`read_vote`**, **`save_committed`/`read_committed`** — put/get the
  singleton keys; saves persist.
- **`try_get_log_entries(range)`** — prefix-scan `log/`, deserialize the range.

### Durable `StateMachineStore` (over the `data` keyspace)

Same shape as D1, holding the `data`-keyspace `Kv` view (`Arc<dyn Kv>`) and
persisting `last_applied` / `last_membership` into the `raft` keyspace.

**Atomic, durable apply (the correctness crux).** Because `data` and `raft` share
one `Database`, an `apply(entries)` is one fjall batch + one `persist`: read the
current counter values, compute the max-merged results in memory, then commit
`{all row puts/deletes, the merged counter values, `last_applied` = the last
entry's `log_id`, membership}` atomically and fsync once. An applied entry and
its `last_applied` advance together — a crash cannot leave them torn. And because
the ops are individually idempotent (puts/deletes; counter max-merge), any replay
openraft performs after a restart is harmless.

**Snapshots** are built **from the durable state machine in memory**:
`build_snapshot` scans the `data` keyspace + reads `last_applied`/membership and
serializes to a `Cursor<Vec<u8>>` (used for log compaction and far-behind
follower `install_snapshot`); `install_snapshot` atomically clears + repopulates
`data` and sets `last_applied`/membership. No separate on-disk snapshot file.

**On open/restart**, the state machine loads `last_applied`/membership from the
`raft` keyspace; the `data` keyspace already holds the applied state, so openraft
resumes and re-applies only committed entries past `last_applied` (usually none).

### In-memory mode retained, and the node/cluster harness

The state machine is already storage-agnostic (`Arc<dyn Kv>`); only the log store
and the metadata persistence get a durable variant. `Node::start` chooses
in-memory (`MemKv` + the existing in-memory `LogStore`) or durable
(`NodeStore::open(dir)`).

**Crash/restart is modeled in-process.** A `Node` (durable mode) remembers its
`dir`. A **crash** drops the node's `openraft::Raft` (closing the fjall
`Database` — acked writes are already fsynced; in-flight unacked ones may or may
not have landed, which is correct). A **restart** reopens `NodeStore::open(dir)`,
reconstructs the `Raft` (openraft reads the persisted log + vote + `last_applied`
and resumes as the **same node id**), and re-registers the new Raft handle with
the `Switchboard` so it rejoins and catches up (log replay from peers, or
`install_snapshot` if far behind).

`Cluster` gains:
- `Cluster::durable(n, base_dir)` — n nodes at `base_dir/node-<id>`.
- `Cluster::restart(id)` — drop + reopen node `id` from its dir, re-register
  (graceful bounce).
- `Cluster::crash(id)` then `restart(id)` — the ungraceful nemesis form.
- The existing `pause`/`isolate`/`heal` controls still compose.

## Data flow

**Durable write.** The leader runs the unchanged SP6 path (row locks,
EvalPlanQual) reading the `data` keyspace; at COMMIT, `committer.commit` →
`raft.client_write` → openraft **appends to the durable log (fsync) on a
majority**, then the leader **applies (atomic `data`+`last_applied` batch,
fsync)** → `client_write` resolves → `CommandComplete`. A returned write is on a
**majority's disks** and survives any single crash so long as a majority's disks
survive. Reads hit the `data` keyspace with SP5/SP6 MVCC.

**Crash → restart.** Drop the Raft (Database closed, acked writes fsynced) →
reopen → openraft resumes from durable log+vote+`last_applied`, rejoins, catches
up. Committed entries (on a majority's durable logs) are never lost.

## Error handling

- fjall I/O errors map to openraft `StorageError` / `StorageIOError` (the
  `LogStore` and state-machine methods return these). A leader storage error
  fails the write (client retries) with **no torn state** (atomic batch).
- A corrupt / undeserializable log entry on open is a **hard** `StorageError` —
  the node fails to start; corruption is surfaced, never silently skipped.
- SP6/SP7 SQLSTATEs (`40001`, `40P01`, `NotLeader` retryable, `Unavailable`
  `08006`) are unchanged.
- `#![forbid(unsafe_code)]`; no panics on I/O paths (`expect` only where
  genuinely infallible — e.g. in-memory locks; fjall errors are mapped).

## Testing

Five legs, mirroring D1's structure; all pure-Rust `cargo test`.

1. **openraft storage conformance.** `openraft::testing::Suite::test_all` against
   the **durable** log store + state machine (the same gate D1 used for the
   in-memory adapters). Plus durable unit tests: append → reopen → entries
   survive; `truncate`/`purge` boundaries; `vote`/`committed` round-trip; an
   apply, then drop + reopen, leaves `data` and `last_applied` consistent, and
   re-applying the entry is idempotent (no corruption).

2. **Deterministic restart scenarios** (`Cluster::durable` + `restart`,
   `Raft::wait`-based, no sleeps): committed-write durability (write → restart the
   holding node → data survives); restarted-follower catch-up; leader crash +
   restart (new leader elected, restarted node rejoins as follower, committed data
   intact); full-cluster restart (stop all → restart all → committed state
   recovered, leader re-elected). **D1's in-memory fault scenarios stay green —
   no regression.**

3. **SQL-over-Raft durability e2e.** CREATE TABLE / INSERT on the durable leader
   → restart the whole cluster → the table and rows survive, a new write lands;
   SP6 concurrency (row-lock / EvalPlanQual) over the durable path.

4. **Crash-nemesis durability (Jepsen-style).** The chosen rigor: the SP7 bank
   workload against the durable leader with a nemesis that **crashes + restarts
   nodes (including the leader)** mid-run; the history records
   `invoke`/`ok`/`fail`/`info`; the invariant is that **every acknowledged
   transfer survives** and the bank total is conserved once all crashes/restarts
   heal. Reuses the SP7 bank-conservation / Stateright history harness, extended
   with crash + restart. (The Stateright abstract model is optional for D2a — the
   crash nemesis + restart scenarios are the primary durability proof; a small
   "durability-across-restart" invariant is added only if it earns its keep.)

5. **Gauntlet.** `cargo fmt --check`, `cargo clippy --workspace --all-targets -D
   warnings`, `cargo test --workspace`, parser oracle, `scripts/check-no-native.sh`
   (fjall is already shipped — **no new native crate**), `cargo deny check`,
   conformance parity at/above the SP7 baseline. All SP1–SP7 gates green.

## Scope boundaries (tracked OUT)

- **Deferred to later distribution slices:** real TCP transport / sockets,
  multi-process nodes, membership/join (**D2b**); SQL leader routing + wiring the
  shipped binary to replicated mode (**D2c**); over-the-wire Jepsen + Elle
  (**D2d**); range routing (D3); range splits (D4); leases / linearizable local
  reads / rebalancing (D5); cross-range distributed transactions.
- **Deferred storage depth:** on-disk snapshot files (D2a builds snapshots in
  memory from the durable state machine); log-density encoding (serde_json is used
  for log entries); torn-write / power-loss-mid-fsync simulation (that stresses
  fjall's own crash-safety — out of scope; D2a trusts fjall's fsync + journal
  replay, which SP3 already relies on).
- **Deferred reads:** strict read-linearizability (stale reads from a deposed
  leader remain possible without leases) — D5, unchanged from SP7.
- **Pre-existing carry-overs** stay deferred: vacuum/GC of dead versions and clog
  truncation/xid freezing; `pgwire::engine::oids` INT4/TEXT duplication;
  `conformance::split_statements` Latin-1 corner; the hand-written parser reserves
  all keywords; `cargo deny` advisories masked via documented ignores.

## Success criteria

1. Each node persists its Raft log, vote, committed index, and applied state in a
   single per-node fjall `Database` (`data` + `raft` keyspaces) via `NodeStore`;
   the durable log store + state machine pass
   `openraft::testing::Suite::test_all`.
2. The SQL/MVCC/concurrency stack runs **unchanged** over the durable path
   (`Cluster::durable`); CRUD + transactions behave identically to D1, and **all
   SP1–SP7 tests pass** (the in-memory path is unchanged).
3. A committed write **survives a node crash + restart**: the restarting node
   recovers from its on-disk directory and rejoins; a restarted follower catches
   up; a crashed leader's commits survive under a new leader; a full-cluster
   restart recovers all committed state.
4. **Atomic apply**: an applied entry and its `last_applied` advance together
   (single fjall batch + fsync), so no crash leaves the state machine torn; replay
   after restart is idempotent.
5. A **crash nemesis** that kills and restarts nodes (including the leader) during
   a bank workload never loses an acknowledged transfer; the bank total is
   conserved once faults heal.
6. All SP1–SP7 gates green; **pure-Rust shipped tree** (no new native crate — fjall
   is already shipped) and `#![forbid(unsafe_code)]` hold; conformance parity
   unchanged or improved.
