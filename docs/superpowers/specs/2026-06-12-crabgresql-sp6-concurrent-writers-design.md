# crabgresql SP6: Concurrent writers (row locks, block-and-retry, EvalPlanQual, deadlock detection)

**Date:** 2026-06-12
**Status:** Approved
**Program spec:** `docs/superpowers/specs/2026-06-11-crabgresql-program-and-wire-protocol-design.md`
**Predecessors:** SP1 (wire), SP2 (vertical slice), SP3 (durable storage), SP4 (transactions), SP5 (PG-faithful MVCC visibility) — all merged.

## Goal

Remove SP5's transaction-scoped writer lock and let transactions write
**concurrently**, with PostgreSQL's real conflict handling: per-row locks (the
tuple's `xmax` is the lock), **block-and-retry** when two transactions target the
same row, **EvalPlanQual** re-evaluation on wake (READ COMMITTED re-finds the
latest row; REPEATABLE READ raises `40001`), `SELECT ... FOR UPDATE`/`FOR SHARE`,
and **deadlock detection** (`40P01`). This is the payoff the SP5 foundation
(xids, clog, ProcArray, `satisfies_mvcc`, write-through) was built for. Reads
still never lock; the clog/ProcArray/visibility machinery is reused unchanged.

Constraints unchanged: `#![forbid(unsafe_code)]` everywhere; pure-Rust shipped
tree; parity baseline PostgreSQL 18.

## The shift

SP5 serialized all writers behind one `tokio::sync::Mutex` held for a writing
transaction's duration, so write-write conflicts could not occur. SP6 removes
that lock. Two transactions can now target the same row, which requires the
PostgreSQL row-locking cluster. Concurrent writers to **different** rows proceed
in parallel; writers to the **same** row serialize through the row lock.

## Architecture

### Two authorities: in-memory locks (live) and `xmax` (committed)

PostgreSQL's mental model is "the tuple's `xmax` is the row lock." crabgresql
splits that into two cooperating authorities, because a `SELECT FOR UPDATE`
holder takes a lock but writes no `xmax`, and because claiming a row must be
atomic across concurrent writers:

- **The in-memory `RowLockManager` is the authority for LIVE conflicts.** Every
  writer and locker (UPDATE, DELETE, `FOR UPDATE`, `FOR SHARE`) first calls
  `try_acquire` on the manager. It knows every in-progress holder — including
  `FOR UPDATE`/`FOR SHARE` holders that wrote nothing to disk. A conflict here
  means an in-progress transaction holds the row; the caller **blocks** on that
  holder.
- **The on-disk `xmax` is the authority for the COMMITTED outcome.** Once the
  in-memory lock is held (the prior holder, if any, has finished and released
  it), the caller re-reads the tuple's `xmax`: invalid/aborted ⇒ apply to the
  original row; a **committed** xid (a transaction that updated/deleted the row
  and committed) ⇒ **EvalPlanQual**.

So the order is always **`try_acquire` (handles live, in-memory) → recheck
`xmax` (handles committed, on-disk)**. UPDATE/DELETE write `xmax` through to disk
during the transaction (write-through, as SP5), so an in-progress UPDATE is
visible to a concurrent one both via the in-memory lock (while running) and via
`xmax` (after it commits); `FOR UPDATE`/`FOR SHARE` exist only in the in-memory
manager and leave no `xmax`, so a transaction they blocked proceeds on the
original row once they release.

### `RowLockManager` (`executor::lockmgr`)

The durable `xmax` records the *committed* outcome (for visibility), but live
conflict-and-wait between concurrent transactions needs an in-memory structure,
and claiming a row (read-`xmax`-then-stamp-`xmax`) must be atomic. A shared
(`Arc`, engine-wide like `ProcArray`) manager provides this:

- State: `Mutex<HashMap<(TableId, rowid), RowLock>>`, where `RowLock = { mode:
  Exclusive | Shared, holders: set<Xid>, waiters: Vec<Waiter> }`. Exclusive =
  UPDATE/DELETE/`FOR UPDATE`; Shared = `FOR SHARE` (multiple holders; blocks
  exclusive).
- Per-xid wake: a way to `await` until a holder xid finishes and to wake all its
  waiters (`tokio::sync::Notify` per waiting xid, or a `HashMap<Xid, Arc<Notify>>`).
- The **wait-for graph** (`Xid → Xid` it is blocked on) for deadlock detection.
- API: `try_acquire(table, rowid, mode, my_xid) -> Acquired | Conflict(holder)`;
  `wait_for(my_xid, holder) -> Ok | Deadlock` (registers the edge, runs the
  eager cycle check, then awaits the wake); `release_all(my_xid)` (drop every
  lock this xid holds and wake their waiters) — called at COMMIT/ROLLBACK.

Row locks are **transaction-scoped**: held until COMMIT/ROLLBACK, then released
together. The manager is **purely in-memory** — after a restart no transactions
are in flight, so no lock state must survive; the durable `xmax` carries the
committed outcome.

### Deadlock detection — eager wait-for-graph cycle check

Before a writer blocks, the manager adds its `(my_xid → holder)` edge and checks
whether the graph now has a cycle. If it does, the current waiter is the
**victim**: it aborts with `40P01` (`deadlock_detected`), which releases its
locks and wakes anything waiting on it. No `deadlock_timeout` timer — eager
detection is deterministic and single-process-friendly (observably equivalent to
PostgreSQL's timer-then-check for correctness, just more eager). PostgreSQL's
victim-selection heuristics are simplified to "the transaction whose wait closes
the cycle."

### `SequenceManager` (`executor::seq`)

SP5's INSERT read the durable next-rowid under the writer lock. With concurrent
INSERTs that races. SP6 adds an in-memory atomic per-table next-rowid counter,
seeded from the durable `seq` key and persisted on use (the same pattern as
`ProcArray`'s `next_xid`), so concurrent INSERTs to one table get distinct
rowids without a global lock.

### Session / engine

`SqlEngine` drops `writer_lock`; `TxnCtx` drops `writer_guard`. The engine holds
`Arc<RowLockManager>` and `Arc<SequenceManager>` alongside `Arc<ProcArray>`.
COMMIT/ROLLBACK additionally call `lockmgr.release_all(xid)` (waking waiters).
Writes no longer take a global lock; they take per-row locks through the manager.

## Data flow

**INSERT** allocates a rowid from the `SequenceManager` (no existing tuple, so no
row lock) and writes its version (`xmin = my xid`, `xmax = 0`), persisting the
seq + `next_xid` (+ clog Committed for autocommit) — as SP5, minus the global
lock.

**UPDATE / DELETE / `SELECT FOR UPDATE` / `FOR SHARE`** operate per target row
(the block-and-retry / EvalPlanQual loop):
1. From the visible scan, take a candidate row and `try_acquire` its lock
   (exclusive, or shared for `FOR SHARE`).
2. **Conflict** (held by an in-progress other xid): `wait_for(my_xid, holder)` —
   if the wait-for edge closes a cycle, abort self with `40P01`; otherwise
   `await` the holder's wake. On wake, go to step 3.
3. **EvalPlanQual / re-check** (lock now held): re-read the row's latest on-disk
   version. If a transaction that committed **after my snapshot** has set its
   `xmax` (updated/deleted it): **READ COMMITTED** takes a fresh snapshot,
   re-finds the latest live version, and re-evaluates `WHERE` — if it still
   matches, apply to that version; if not (or the row is gone), skip (contributes
   0 to the row count). **REPEATABLE READ** raises `40001`
   (`serialization_failure`). If `xmax` is invalid/aborted, apply to the original
   row.
4. Apply: UPDATE stamps the matched version's `xmax = my xid` and writes a new
   version; DELETE stamps `xmax = my xid`; `FOR UPDATE`/`FOR SHARE` take the lock
   without writing a new version. The row lock is held until COMMIT/ROLLBACK.

**COMMIT** writes clog Committed, `procarray.finish(xid)`, and
`lockmgr.release_all(xid)` (waking waiters). **ROLLBACK** writes clog Aborted and
the same releases. A crash leaves in-progress versions invisible (lazy recovery,
unchanged) and the in-memory lock state simply vanishes.

## Error handling

- `40001` (`serialization_failure`) — REPEATABLE READ write conflict on a row a
  concurrent transaction changed after this transaction's snapshot.
- `40P01` (`deadlock_detected`) — the eager wait-for-graph cycle check fired; the
  transaction is aborted and the client is expected to retry (PostgreSQL
  contract).
- Both leave the transaction in the failed state (the existing `25P02` path
  applies for subsequent statements until ROLLBACK).
- I/O errors still map to `58030`; corrupt data to `XX000`; no panics.

## Parser additions

`SelectStmt` gains `locking: Option<RowLockStrength>` (`ForUpdate` | `ForShare`).
The grammar parses a trailing `FOR UPDATE` / `FOR SHARE` clause. New keywords:
`FOR`, `SHARE` (`UPDATE` already exists). The libpg_query oracle corpus gains
accept cases for `SELECT … FOR UPDATE` and `… FOR SHARE`.

## Testing

Concurrency tests must be **deterministic**, not racy: two sessions on a
multi-thread tokio runtime are interleaved with explicit synchronization
(channels/barriers) so each scenario reproduces exactly.

- **`lockmgr` unit:** acquire/release; exclusive conflict; shared coexistence +
  shared-blocks-exclusive; `release_all` wakes waiters; the wait-for graph
  detects a 2-cycle and names a victim.
- **`seq` unit:** concurrent allocation yields distinct, monotonic rowids;
  durable seed/persist round-trips.
- **executor concurrency:** two transactions UPDATE the **same** row — one
  blocks; after the first commits, the blocked one EvalPlanQuals (RC applies to
  the new value; RR gets `40001`); after the first *aborts*, the blocked one
  proceeds on the original. Two transactions UPDATE **different** rows proceed
  concurrently. `SELECT … FOR UPDATE` blocks a concurrent UPDATE of the same row;
  two `FOR SHARE` coexist but block an UPDATE. A deliberate **deadlock** (T1 locks
  A then waits B; T2 locks B then waits A) produces exactly one `40P01` while the
  other commits.
- **e2e (wire):** two tokio-postgres connections exercise a concurrent UPDATE
  conflict and a `SELECT FOR UPDATE` block end-to-end.
- **conformance:** `FOR UPDATE`/`FOR SHARE` parse-accept cases match the PG-18
  oracle; parity holds at or above the SP5 baseline.
- **Regression / gauntlet:** all SP1–SP5 gates stay green; the existing 224 tests
  pass (non-conflicting workloads behave as before; the existing concurrent-
  INSERT test now runs against the atomic `SequenceManager` with no global lock).
  `forbid(unsafe_code)`, pure-Rust shipped tree, fmt, clippy `-D warnings`,
  parser oracle, `check-no-native.sh`, `cargo deny`, conformance.

## Scope boundaries (tracked OUT)

- **Deferred (YAGNI / later slices):** `NOWAIT`, `SKIP LOCKED`, the finer-grained
  `FOR NO KEY UPDATE` / `FOR KEY SHARE` lock strengths (SP6 collapses the four-
  level lock lattice to **exclusive + shared**); `lock_timeout` /
  `deadlock_timeout` GUCs; predicate locks / SERIALIZABLE-SSI; savepoint-scoped
  (subtransaction) lock release; multixact (multiple share-lockers persisted on
  the tuple — SP6 keeps shared-lock holder sets in memory only).
- **Deferred further (now more pressing):** vacuum/GC of dead versions — they
  accumulate faster under concurrency — and clog truncation/xid freezing.
- **Pre-existing carry-overs** stay deferred: `pgwire::engine::oids` duplicates
  INT4/TEXT; `conformance::split_statements` Latin-1 corner; `kv::FjallKv::
  scan_prefix` full materialization; the hand-written parser reserves all
  keywords; `cargo deny` advisories masked via documented ignores pending an
  upstream `rustls-rustcrypto` bump.

## Success criteria

1. Two transactions writing **different** rows run concurrently (no global
   serialization); writing the **same** row, the second blocks until the first
   ends.
2. On the blocker's commit, a blocked READ COMMITTED writer re-finds and applies
   to the latest row version (EvalPlanQual); a blocked REPEATABLE READ writer
   raises `40001`. On the blocker's abort, the writer proceeds on the original
   row.
3. `SELECT … FOR UPDATE` and `… FOR SHARE` take row locks that block conflicting
   writers (shared coexists with shared); the grammar matches the PG-18 oracle.
4. A genuine deadlock is detected and broken with `40P01` (one victim aborts, the
   other commits) — no hang.
5. Concurrent INSERTs to one table get distinct rowids with no global lock
   (atomic `SequenceManager`); no lost updates.
6. All SP1–SP5 gates green; the existing 224 tests pass; conformance parity
   unchanged or improved.
