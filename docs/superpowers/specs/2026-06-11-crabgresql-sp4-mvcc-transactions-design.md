# crabgresql SP4: Transactions + Serialized-Writer MVCC

**Date:** 2026-06-11
**Status:** Approved
**Program spec:** `docs/superpowers/specs/2026-06-11-crabgresql-program-and-wire-protocol-design.md`
**Predecessors:** SP1 (wire), SP2 (vertical slice), SP3 (durable storage) — all merged.

## Goal

Real transactions and MVCC. Add `BEGIN`/`COMMIT`/`ROLLBACK` (deferred since
SP1), commit-timestamp MVCC with snapshot-isolated reads at READ COMMITTED
(default) and REPEATABLE READ, and `UPDATE`/`DELETE` (which MVCC makes honest).
Writers stay **serialized** behind the SP3 global `write_lock`, so two
transactions can never write concurrently — write-write conflicts cannot occur
and no lock manager is needed yet. Concurrent writers + PG-faithful
block-and-retry + EvalPlanQual are **SP5**. Version GC/vacuum is deferred
further.

Constraints unchanged: `#![forbid(unsafe_code)]` everywhere; pure-Rust shipped
tree; parity baseline PostgreSQL 18.

## The enabling idea: buffer writes until commit

A transaction buffers its writes in an in-memory **write-set** and flushes them
in one atomic `kv::write_batch` at COMMIT. Combined with serialized writers,
this means **the durable store only ever holds committed versions** — there are
no uncommitted or aborted versions on disk. That collapses MVCC visibility to a
single timestamp comparison: no commit-status log (clog) is needed in SP4 (the
clog arrives in SP5, when concurrent writers must place uncommitted versions on
disk). ROLLBACK is just discarding the write-set; COMMIT reuses the existing
atomic `write_batch` for all-or-nothing durability.

## Architecture

### Commit-timestamp MVCC

- **`commit_ts`** — a durable monotonic `u64` at `/0/meta/commit_ts`, "the
  clock." Bumped once per COMMIT (and per autocommit statement's implicit
  commit). A **snapshot** is the `commit_ts` value captured at the right moment.
- **Versioned key** — the row key gains a version suffix:
  `/<table_id>/1/<rowid>/<commit_ts ENCODED DESCENDING>`. Descending so a forward
  scan over a rowid's versions hits newest-first and stops at the first version
  with `ts ≤ snapshot`. (Descending = encode `!commit_ts`, i.e.
  `u64::MAX - commit_ts`, big-endian — still order-preserving.)
- **Version value** — the existing `rowenc` row bytes plus a one-byte
  tombstone flag (DELETE writes a tombstone version; the newest visible
  tombstone means "row gone").
- **Visibility** — a version with timestamp `V` is visible to a snapshot `S`
  iff `V ≤ S`. Since only committed versions are on disk, no status lookup is
  needed.

### New `mvcc` crate

Holds the reusable, isolated primitives (deps: `kv`, `pgtypes`):
- `Snapshot` (a `commit_ts`).
- Versioned-key helpers: `version_key(table_id, rowid, commit_ts)` and the
  prefix for a rowid's versions; reuse `kv::keyenc` for the structural parts.
- Version-value encoding: `encode_version(deleted: bool, row: &[Datum])` /
  `decode_version(&[u8]) -> Result<(bool, Vec<Datum>), KvError>` (versioned,
  tombstone-aware).
- Visibility helper: given a snapshot and a scan of a rowid's versions, return
  the visible `(deleted, row)` or `None`.

### Per-connection `Session` — the architectural change

Transaction state is inherently per-connection, but today
`pgwire::engine::Engine::simple_query(&self, sql)` is stateless. SP4 makes the
engine connection-oriented:

- The `Engine` trait becomes a factory: `fn connect(&self) -> Self::Session`
  (or returns a boxed session). `simple_query`/`describe` move onto the
  `Session`, which also exposes `fn tx_status(&self) -> TxStatus`.
- `pgwire::server::run_session` creates one `Session` per wire connection and
  reads `tx_status()` to send the correct ReadyForQuery byte (`I`/`T`/`E`) —
  today hardcoded `Idle`.
- `StubEngine` (pgwire's own test engine) gains a trivial always-`Idle` session.

The per-connection `Session` (in the **executor**) owns:
- the transaction state machine: `Idle` | `InTransaction(TxnCtx)` | `Failed`;
- `TxnCtx { isolation, snapshot, write_set, seq_pending }` where `write_set` is a
  per-table map `rowid → Pending::{Row(bytes) | Tombstone}` and `seq_pending`
  tracks per-table next-rowid (read-your-writes for INSERT sequences).

The shared `SqlEngine` still owns the `Arc<dyn Kv>` and the global `write_lock`;
the `write_lock` is held across a COMMIT's `commit_ts` bump + `write_batch`.
DDL (CREATE/DROP TABLE) stays **non-transactional** — it auto-commits even inside
a block (PG's transactional DDL is a tracked gap).

## Data flow

**Snapshot timing.** Autocommit statement: snapshot = `commit_ts` at statement
start. READ COMMITTED txn: snapshot re-read at the start of each statement.
REPEATABLE READ txn: snapshot captured at the txn's first statement, reused.

**Read path** (SELECT and the read side of UPDATE/DELETE). A table scan unions
(a) committed rowids whose newest visible version isn't a tombstone and (b)
write-set rowids, deduped. Per rowid: if the write-set has a pending version,
use it (tombstone ⇒ absent); else scan that rowid's committed versions newest-
first and take the first with `ts ≤ snapshot` (tombstone/none ⇒ absent). This
yields read-your-writes + snapshot isolation.

**Writes** accumulate in the write-set, never touching disk mid-transaction:
- **INSERT** → new version at a freshly-allocated rowid (from `seq_pending`).
  Tag `INSERT 0 n`.
- **UPDATE t SET col = expr … WHERE p** → for each matching visible row, evaluate
  the SET expressions against the current row, put a new version at the *same*
  rowid. Tag `UPDATE n`.
- **DELETE FROM t WHERE p** → put a tombstone version at each matching rowid.
  Tag `DELETE n`.

**Lifecycle.**
- **BEGIN [TRANSACTION] [ISOLATION LEVEL {READ COMMITTED | REPEATABLE READ}]**
  (Idle→InTransaction): default READ COMMITTED; tag `BEGIN`; status `T`.
- **COMMIT** (InTransaction, not failed): under `write_lock`, bump `commit_ts`→
  new, tag every write-set version + sequence update with it, write them + the
  new `commit_ts` in **one atomic `write_batch`**; →Idle; tag `COMMIT`; status
  `I`. COMMIT of a `Failed` txn discards the write-set and reports `ROLLBACK`
  (matching PG).
- **ROLLBACK** (any state): discard the write-set; →Idle; tag `ROLLBACK`;
  status `I`.
- A statement that **errors inside a block** → `Failed`; status `E`. Subsequent
  statements except COMMIT/ROLLBACK → `25P02` ("current transaction is aborted,
  commands ignored until end of transaction block"). An **autocommit** statement
  that errors returns the error and stays `Idle` (no failed-block — matches PG).
- `BEGIN` inside an open block, and `COMMIT`/`ROLLBACK` with no block, are
  no-ops returning the right tag (PG's WARNING `NoticeResponse` is a tracked
  minor gap).

**Autocommit = implicit one-statement transaction.** A bare statement (session
`Idle`) takes a snapshot, executes, and commits (bump `commit_ts`, flush the
write-set) — observably identical to SP3 behavior for the existing tests.

**Multi-statement strings:** each top-level statement runs through the session
state machine in order; explicit `BEGIN…COMMIT` controls the block; bare
statements autocommit individually (PG's implicit-block-for-simple-query is a
documented minor divergence).

## Error handling

- `25P02` (in_failed_sql_transaction) for non-COMMIT/ROLLBACK statements while
  `Failed`.
- UPDATE/DELETE reuse existing codes (`42P01`/`42703`/`42804`/`22003`/`22012`).
- MVCC reads/commit thread `Result` through the fallible `Kv`; an I/O error →
  `58030`, never a panic. A corrupt version value → `KvError::CorruptRow` →
  `XX000`.

## Parser additions

`pgparser` gains: `BEGIN`/`START TRANSACTION` (with optional `ISOLATION LEVEL`),
`COMMIT`/`END`, `ROLLBACK`/`ABORT`; `UPDATE t SET c = expr [, …] [WHERE pred]`;
`DELETE FROM t [WHERE pred]`. New AST `Statement` variants: `Begin { isolation:
Option<IsolationLevel> }`, `Commit`, `Rollback`, `Update { table, assignments:
Vec<(String, Expr)>, filter: Option<Expr> }`, `Delete { table, filter:
Option<Expr> }`. The libpg_query oracle corpus gains accept cases for these.

## Testing

- **mvcc:** visibility unit tests (`V ≤ S` visible; tombstone hides; newest-first
  via descending-ts — property test on version ordering); version-value roundtrip
  incl. tombstone.
- **executor:** BEGIN/INSERT/ROLLBACK leaves nothing; BEGIN/INSERT/COMMIT
  persists; read-your-writes; snapshot isolation (RR: a concurrent commit by
  another session is invisible; RC: visible on the next statement); UPDATE
  changes the value while an older RR snapshot still sees the old; DELETE hides
  the row; failed-block `25P02`; COMMIT-of-failed reports `ROLLBACK`.
- **durability:** a committed txn's versions survive reopen; a rolled-back txn
  leaves nothing on disk.
- **e2e:** tokio-postgres `transaction()` — insert+rollback→gone, insert+commit→
  persists; `UPDATE`/`DELETE` round-trips; the wire txn-status drives a working
  multi-statement transaction.
- **conformance:** add UPDATE/DELETE and transaction statements to the corpus
  where they match the PG-18 oracle; parity holds or rises.
- **Regression:** all SP1–SP3 gates stay green; the existing 168 tests keep
  passing (autocommit observably identical). `forbid(unsafe_code)`, pure-Rust
  shipped tree, fmt, clippy, parser oracle, conformance.

## Scope boundaries (tracked OUT)

- **SP5:** concurrent writers — remove the `write_lock`; add the row-lock manager
  + block-and-retry + EvalPlanQual + the clog (uncommitted versions on disk).
- **Deferred further:** version GC/vacuum; SERIALIZABLE/SSI; SAVEPOINT/
  subtransactions; transactional DDL (SP4 CREATE/DROP auto-commit in a block);
  `SET TRANSACTION` / `default_transaction_isolation` GUC; PG's implicit
  transaction block for bare multi-statement simple-query strings; the
  `NoticeResponse` WARNING for redundant BEGIN/COMMIT/ROLLBACK.
- **Pre-existing carry-overs** stay deferred: `pgwire::engine::oids` duplicates
  INT4/TEXT; `conformance::split_statements` Latin-1 corner;
  `kv::FjallKv::scan_prefix` full materialization (matters a bit more now that
  MVCC scans every version — flagged for a streaming-iterator follow-up).

## Success criteria

1. `BEGIN; INSERT; ROLLBACK;` leaves the table unchanged; `BEGIN; INSERT;
   COMMIT;` persists (verified through psql/tokio-postgres and across a restart).
2. Read-your-writes: a SELECT inside a txn sees that txn's own uncommitted
   INSERT/UPDATE/DELETE.
3. Snapshot isolation: a REPEATABLE READ transaction does not see another
   session's commit that landed after its snapshot; a READ COMMITTED transaction
   sees it on its next statement.
4. `UPDATE … SET … WHERE` and `DELETE … WHERE` produce correct results and
   command tags, versioned in MVCC.
5. A statement error inside a transaction block puts it in the failed state
   (`25P02` until COMMIT/ROLLBACK); the wire reports `T`/`E`/`I` correctly.
6. Only committed versions reach disk (rolled-back txns leave nothing); committed
   data survives restart.
7. All SP1–SP3 gates green; the existing 168 tests still pass; conformance parity
   unchanged or improved.
