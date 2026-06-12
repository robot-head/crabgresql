# crabgresql SP5: PG-faithful MVCC visibility foundation (xid + clog + snapshots)

**Date:** 2026-06-12
**Status:** Approved
**Program spec:** `docs/superpowers/specs/2026-06-11-crabgresql-program-and-wire-protocol-design.md`
**Predecessors:** SP1 (wire), SP2 (vertical slice), SP3 (durable storage), SP4 (transactions + serialized-writer MVCC) — all merged.

## Goal

Replace SP4's commit-timestamp visibility model with PostgreSQL's **actual** MVCC
machinery — per-transaction **xid**s, a **clog** (commit-status log), tuple
headers carrying **xmin/xmax**, and **xid-list snapshots** `(xmin, xmax, xip[])`
resolved by `HeapTupleSatisfiesMVCC`. Uncommitted versions now live on disk
(tagged with their creator xid; the clog says whether that xid committed). This
is the **foundation slice**: writers stay **serialized** (the global writer lock
is simply held for the duration of a writing transaction), so write-write
conflicts still cannot occur and behavior is observably identical to SP4 — all
212 existing tests stay green. Removing the lock for true concurrency
(row locks, block-and-retry, EvalPlanQual, serialization errors) is **SP6**;
deadlock detection is **SP7**.

This slice exists because the program's north star is **literal PostgreSQL
parity**: SP6's concurrency must be built on PG's real visibility mechanism, not
a substitute. Isolating that mechanism change here — as a behavior-identical
refactor, the same discipline that made SP4's Engine→Session step safe — keeps
the concurrency slice tractable.

Constraints unchanged: `#![forbid(unsafe_code)]` everywhere; pure-Rust shipped
tree; parity baseline PostgreSQL 18.

## The model swap

SP4 identified each version by a descending `commit_ts` key suffix and resolved
visibility with a single `ts ≤ snapshot` compare. That works only because SP4
buffers writes and flushes them at commit, so the durable store holds **only
committed versions**. SP5 moves to PostgreSQL's model, where uncommitted
versions are on disk and visibility is computed from transaction ids:

- **xid** — a durable monotonic counter at `/0/meta/next_xid`. Assigned
  **lazily** at a transaction's first write (read-only transactions never burn
  an xid, exactly like PG). An autocommit write statement gets a one-shot xid.
- **Tuple header** — every on-disk version carries `xmin` (creating xid) and
  `xmax` (the xid that deleted/superseded it; an invalid sentinel when live).
  UPDATE writes a new version with a fresh `xmin` and stamps the prior version's
  `xmax`; DELETE stamps `xmax` only. The version **key** becomes
  `kv::key::row_key(table, rowid) + xid` (the creating xid, big-endian); the
  **value** is `(xmin, xmax, row)`.
- **clog (`pg_xact`)** — a durable map `xid → {InProgress | Committed | Aborted}`
  in the reserved table-0 keyspace at `/0/clog/<xid>`. The authority on whether a
  writer ever committed.
- **ProcArray + Snapshot** — a shared in-memory registry of currently-running
  transactions. A **snapshot** captures `(xmin, xmax, xip[])`: `xmax = next_xid`
  (one past the highest assigned xid), `xip[]` = the running xids in
  `[xmin, xmax)`, `xmin = min(xip)` (or `xmax` if none running). READ COMMITTED
  re-takes the snapshot per statement; REPEATABLE READ takes it once.
- **Visibility** — PG's `HeapTupleSatisfiesMVCC`: a version is visible iff its
  `xmin` is **committed and not in my snapshot** (i.e. committed *before* my
  snapshot was taken — `xmin < snapshot.xmax`, `xmin ∉ xip`, clog says
  `Committed`) **or** `xmin` is **my own xid** (read-your-writes); AND its `xmax`
  does **not** represent a delete that is visible to me (xmax invalid, OR clog
  `Aborted`, OR xmax in my snapshot / still running, OR — for my own deletes —
  xmax is my xid, which *does* hide it from me). The clog answers "committed?";
  the snapshot's xip-list answers "before my snapshot?".

## Architecture

### `mvcc` crate (extended)

- `xid.rs` — `Xid(u64)` newtype; `INVALID_XID` sentinel; `next_xid_key()` lives
  in `kv::key`. Ordering/encoding helpers for the version-key suffix.
- `clog.rs` — `XidStatus { InProgress, Committed, Aborted }`;
  `clog::get(&dyn Kv, Xid) -> Result<XidStatus, KvError>` (absent ⇒ treated as
  `InProgress`/aborted-equivalent — see recovery); a `clog::put_op(Xid, XidStatus)
  -> kv::WriteOp` for inclusion in atomic batches. One entry per xid at
  `/0/clog/<xid big-endian>` → 1 status byte (page-packing/truncation deferred).
- `snapshot.rs` — `Snapshot { xmin: Xid, xmax: Xid, xip: Vec<Xid> }` (xip kept
  sorted or as a small set); `satisfies_mvcc(xmin, xmax, snapshot, own: Option<Xid>,
  status: impl Fn(Xid) -> Result<XidStatus, KvError>) -> Result<bool, KvError>`.
  Pure function; no I/O of its own (the caller supplies the status lookup).
- `version.rs` — version key `row_key + xid`; `TupleHeader { xmin, xmax }` +
  row; `encode_version` / `decode_version` carry the header.

### `executor` (extended)

- `procarray.rs` — `ProcArray`, the shared running-transaction registry (held on
  `SqlEngine` behind an `Arc<Mutex<…>>`, like the writer lock). Responsibilities:
  allocate the next xid (durably, atomic with the write that first uses it),
  register/deregister a transaction's xid, and build a `mvcc::Snapshot` from the
  current running set plus `next_xid`. After restart it is empty.
- `session.rs` — `TxnCtx` becomes `{ xid: Option<Xid>, snapshot: Snapshot,
  writer_guard: Option<OwnedMutexGuard> }`. The global writer lock changes from
  a `std::sync::Mutex` taken at flush to a `tokio::sync::Mutex` held from a
  writing transaction's **first write** until COMMIT/ROLLBACK (so it can span the
  `.await` points between statements). Read-only transactions never acquire it.
- `exec.rs` — **write-through**: INSERT/UPDATE/DELETE write versions to the store
  immediately (tagged `xmin = my xid`, clog `InProgress`), rather than buffering
  in an in-memory write-set. SELECT scans a rowid's on-disk versions and keeps
  the one satisfying `satisfies_mvcc` (passing the txn's own xid for
  read-your-writes). **SP4's write-set overlay in `scan_live_rows` is removed.**

## Data flow

**Snapshot timing.** Autocommit: snapshot at statement start. RC txn: re-taken
per statement. RR txn: taken at the first statement, reused. (All built from the
ProcArray.)

**Reads.** A table scan groups on-disk versions by rowid and applies
`satisfies_mvcc` to each version's header; at most one version per live rowid is
visible (the MVCC chain invariant). A row the current txn inserted (xmin = own
xid) is visible to it; a row it deleted (xmax = own xid) is hidden from it.

**Writes (write-through, under the transaction-scoped writer lock).**
- First write of a transaction: lazily allocate the xid, register it in the
  ProcArray, acquire the writer lock (held until COMMIT/ROLLBACK).
- **INSERT** → version at a fresh rowid with `xmin = my xid`, `xmax = invalid`.
- **UPDATE** → for each visible matching row, write a new version
  (`xmin = my xid`) at the same rowid **and** stamp the prior version's
  `xmax = my xid`.
- **DELETE** → stamp the matching version's `xmax = my xid`.
- **Autocommit** performs all of the above plus the commit in **one atomic
  `write_batch`**: the version writes + the seq bump + the `next_xid` bump + the
  clog entry `xid → Committed`. Crash-atomic, exactly like SP4.

**Lifecycle.**
- **COMMIT** (explicit, not failed): one atomic batch writes clog `xid →
  Committed`; then deregister from the ProcArray and release the writer lock;
  tag `COMMIT`. A read-only transaction (no xid) just drops its snapshot.
- **ROLLBACK**: write clog `xid → Aborted`; deregister; release the lock. The
  dead versions remain on disk, permanently invisible. (A read-only or
  never-written transaction is a no-op beyond dropping state.)
- **Failed block / 25P02 / COMMIT-of-failed → ROLLBACK** semantics are unchanged
  from SP4 (a failed transaction aborts: clog `Aborted`).

**Crash recovery is lazy and free.** After restart the ProcArray is empty, so any
clog `InProgress` xid is in no snapshot's running set and `< next_xid`, so
`satisfies_mvcc` resolves it as not-committed ⇒ its versions are invisible —
equivalent to PostgreSQL aborting in-progress xids on recovery. No startup scan
of the clog is required. (`next_xid` is durable, so xids are never reused.)

## Error handling

- No new SQLSTATEs in this slice (serialization errors `40001` arrive with SP6).
- MVCC reads/commit thread `Result` through the fallible `Kv`; an I/O error →
  `58030`, never a panic. A corrupt tuple header or clog entry →
  `KvError::CorruptRow` → `XX000`.
- `25P02` (in_failed_sql_transaction) and the BEGIN/COMMIT/ROLLBACK tags are
  unchanged from SP4.

## Testing

- **mvcc unit:** `satisfies_mvcc` truth table — xmin committed-before-snapshot
  visible; xmin in `xip` invisible; xmin `≥ xmax` invisible; xmin aborted
  invisible; **own xid visible** (read-your-writes); xmax committed-visible
  hides the row; xmax aborted / in-progress / own-but-different-snapshot does not
  hide; xmax = own xid hides (read-your-own-delete). Snapshot construction from a
  running set (xmin/xmax/xip correct). clog roundtrip; absent entry ⇒
  aborted-equivalent.
- **recovery:** write versions under an in-progress xid, drop the engine, reopen,
  assert those versions are invisible (lazy recovery); a committed txn's versions
  remain visible across reopen.
- **executor (behavior-identical):** every SP4 transaction/isolation/UPDATE/
  DELETE test stays green unchanged — RC re-snapshots, RR holds its snapshot,
  read-your-writes (now via own-xid), rollback discards (now via aborted clog),
  failed-block `25P02`, COMMIT-of-failed → `ROLLBACK`.
- **durability:** committed transaction (incl. an UPDATE) survives reopen;
  rolled-back transaction leaves nothing **visible** (versions on disk but
  invisible); the write-through uncommitted-on-disk path is exercised.
- **e2e:** the SP4 wire transaction and UPDATE/DELETE round-trip tests stay green.
- **conformance:** unchanged; parity holds at or above the SP4 baseline.
- **Regression / gauntlet:** all SP1–SP4 gates stay green — the existing 212
  tests pass (behavior observably identical); `forbid(unsafe_code)`, pure-Rust
  shipped tree, fmt, clippy `-D warnings`, parser oracle, `check-no-native.sh`,
  `cargo deny`, conformance.

## Scope boundaries (tracked OUT)

- **SP6:** remove the transaction-scoped writer lock → true concurrent writers;
  the row-lock manager (`xmax`-as-lock), PG-faithful block-and-retry, EvalPlanQual
  re-check, `SELECT FOR UPDATE`/`FOR SHARE`, and serialization failures (`40001`)
  under REPEATABLE READ.
- **SP7:** deadlock detection (wait-for graph + `40P01` victim abort).
- **Deferred further (now more pressing, since aborted/superseded versions
  persist on disk):** vacuum/GC of dead versions; clog page-packing, truncation,
  and xid freezing / wraparound handling; SERIALIZABLE/SSI; SAVEPOINT /
  subtransactions (the clog reserves room for a sub-committed state but it is
  unused here).
- **Pre-existing carry-overs** stay deferred: `pgwire::engine::oids` duplicates
  INT4/TEXT; `conformance::split_statements` Latin-1 corner; `kv::FjallKv::
  scan_prefix` full materialization (matters yet more now that a rowid may
  accumulate dead versions on disk — flagged again for a streaming follow-up);
  the hand-written parser reserves all keywords; no raw-socket test asserts the
  wire `T`/`E`/`I` byte; `cargo deny` advisories are masked via documented
  ignores pending an upstream `rustls-rustcrypto` bump.

## Success criteria

1. Visibility is computed by `satisfies_mvcc` over xid-list snapshots + the clog
   (no `commit_ts`-based visibility remains); the `satisfies_mvcc` truth table is
   covered by unit tests.
2. Uncommitted versions are written to disk tagged with their creator xid; a
   committed transaction's versions become visible and survive a restart; a
   rolled-back or crashed (in-progress) transaction's versions are permanently
   invisible (lazy recovery), verified across reopen.
3. Read-your-writes works through own-xid visibility (no in-memory write-set);
   REPEATABLE READ does not see a transaction that committed after its snapshot;
   READ COMMITTED sees it on the next statement.
4. Writers remain serialized (the writer lock spans a writing transaction); no
   write-write conflict can occur; the existing concurrent reader-vs-autocommit-
   writer isolation tests pass unchanged.
5. All SP1–SP4 gates are green and the existing 212 tests pass with observably
   identical behavior; conformance parity is unchanged or improved.
