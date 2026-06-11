# crabgresql SP3: Durable Single-Node Storage (fjall-backed KV, durable catalog + sequences, crash recovery)

**Date:** 2026-06-11
**Status:** Approved
**Program spec:** `docs/superpowers/specs/2026-06-11-crabgresql-program-and-wire-protocol-design.md`
**Predecessors:** SP1 (wire protocol), SP2 (vertical slice) — both merged.

## Goal

Make crabgresql's data survive process restart. Swap the in-memory `MemKv` for
a durable, crash-recoverable LSM (`fjall`) behind the existing `kv::Kv` trait,
and fold the catalog and the per-table rowid allocator — currently in-memory in
`executor::SqlEngine` and lost on restart — into the same durable KV store under
a reserved keyspace. This fixes the SP2 carry-over where rowids reset and
collide on restart. Every statement still autocommits; transactions and MVCC are
SP4.

Constraints unchanged: `#![forbid(unsafe_code)]` in every crate; pure-Rust
shipped dependency tree (no C, no `-sys`). `fjall` 3.1.5 is verified pure-Rust
(only `lz4_flex` for compression; no `cc`/`-sys`/`ring`/`zstd`/`cmake`/`bindgen`);
`cargo-deny` + `scripts/check-no-native.sh` gate this. Documented fallback if a
future fjall pulls in C: `sled` (also pure-Rust). Parity baseline PostgreSQL 18.

## Architecture

### Storage engine: fjall

`fjall` 3.1.5 — a pure-Rust LSM with its own internal journal/WAL, crash
recovery, atomic write-batches, and a block cache. Durability and recovery come
from the engine, not hand-rolled code. Used as a single keyspace + single
partition; keys and values are the existing order-preserving `keyenc`/`rowenc`
encodings, unchanged.

### The `Kv` trait evolves (the SP3 seam change)

Two changes forced by durable I/O:

1. **Fallible.** `get`/`put`/`delete`/`scan_prefix` return `Result<_, KvError>`
   (disk I/O can fail; a network database must surface it, never panic). `MemKv`
   returns `Ok` (internally infallible; kept for tests and the ephemeral
   default).
2. **Atomic batches.** A new `write_batch(ops: &[WriteOp]) -> Result<(), KvError>`
   primitive commits a set of puts/deletes all-or-nothing with fsync (fjall
   write-batch). `WriteOp` is `Put { key, value }` | `Delete { key }`. This is
   how a statement's row writes + sequence bump commit atomically, and it is the
   exact seam `BEGIN`/`COMMIT` (SP4) extends.

The trait stays **synchronous** (fjall is sync); the async/transactional variant
arrives with the distributed layer, as the trait doc already notes.

Updated trait shape:

```rust
pub enum WriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

pub trait Kv: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError>;
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KvError>;
    fn delete(&self, key: &[u8]) -> Result<(), KvError>;
    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, KvError>;
    /// Apply all ops atomically and durably (fsync). All-or-nothing on crash.
    fn write_batch(&self, ops: &[WriteOp]) -> Result<(), KvError>;
}
```

`put`/`delete` may be expressed as one-op `write_batch` calls internally.

### `FjallKv` — the durable implementation

`kv::FjallKv` wraps a fjall keyspace + partition opened at a path. `get`/`scan_
prefix` map to fjall point/prefix reads; `write_batch` maps to a fjall
write-batch with persist (fsync). `FjallKv::open(path) -> Result<Self, KvError>`;
opening an existing dir recovers automatically via fjall's journal replay (no
bespoke recovery code). fjall open failures (locked dir, permissions,
incompatible format) surface here as a clean `KvError`, not a panic.

### Reserved keyspace for system metadata

All durable state lives in one KV store, partitioned by key prefix. The
encoding reuses `keyenc` (order-preserving) for the structural parts:

| Keyspace | Key | Value |
|---|---|---|
| User rows | `/<table_id≥1>/1/<rowid>` | row bytes (`rowenc`) — unchanged from SP2 |
| Catalog | `/0/catalog/<table_name>` | versioned-serialized `(table_id, Vec<Column>)` |
| Sequences | `/0/seq/<table_id>` | u64 next-rowid (big-endian) |
| Global meta | `/0/meta/next_table_id` | u32 next TableId (big-endian) |

Table-id 0 is the reserved system namespace; the sub-namespace is a short ASCII
tag (`catalog`/`seq`/`meta`) following the `/0/` prefix. Catalog values use the
same `row_version` leading-byte discipline as `rowenc` so the format can evolve.

### `catalog` crate: from owner to view

`catalog::Catalog` stops owning a `RwLock<HashMap>` + `next_id` counter and
becomes a typed view over a `&dyn Kv`:

- `create_table(kv, name, columns)`: duplicate-name check (read `/0/catalog/
  <name>` → `42P07` if present); allocate `TableId` from `/0/meta/next_table_id`;
  then **one write-batch**: put the serialized schema, put the sequence at
  `/0/seq/<table_id>` = 1, put the bumped `next_table_id`. A crash never leaves a
  half-created table.
- `get_table(kv, name)`: read + deserialize `/0/catalog/<name>` (→ `42P01` if
  absent). Deserialize failure → `KvError::CorruptRow`.
- `drop_table(kv, name)`: resolve the table; **one write-batch** deleting the
  catalog entry, the sequence key, and every row under `/<table_id>/1/` (via
  `scan_prefix` then batch-deletes). All-or-nothing.

`Catalog` is now effectively stateless (a namespace of functions over a `Kv`);
the crate keeps `Column`/`Table`/`TableId`/`CatalogError` types.

### `executor::SqlEngine`: loses in-memory state

The `Mutex<HashMap> rowids` and the in-memory catalog HashMap are removed.
- Rowid allocation reads `/0/seq/<table_id>`, bumps in memory across a
  statement's rows, and writes the final value in the INSERT's write-batch.
- `SqlEngine::new()` stays in-memory (`MemKv`) for tests and the ephemeral
  default. New `SqlEngine::open(path) -> Result<Self, _>` builds a `FjallKv`.
- DDL (`CREATE`/`DROP TABLE`) is serialized behind a process-wide mutex in the
  engine to close the `create_table` read-check-then-write TOCTOU race (no
  transaction isolation yet). DML needs no such lock; fjall handles concurrent
  row access.
- No in-memory schema cache: catalog reads hit KV every query (correctness
  first; fjall's block cache keeps it fast). A DDL-invalidated schema cache is a
  tracked future optimization, not built (YAGNI).

### Binary: `--data-dir`

`crabgresql --data-dir <path>` → durable `SqlEngine::open(path)`. Absent →
ephemeral `MemKv` (today's behavior; keeps existing tests/smoke green).

## Data flow

**INSERT.** Read the table's sequence once; for each VALUES row, eval + coerce +
encode, assign the next rowid (bumping in memory); collect every row `Put` plus
one sequence `Put` into a `WriteOp` list; `kv.write_batch(...)`. Atomic +
durable: crash leaves all rows + the bumped sequence, or none. Tag
`INSERT 0 <n>`.

**CREATE/DROP TABLE.** As in the catalog section — each is one atomic batch.

**SELECT.** Unchanged semantics: `scan_prefix` the table, decode, filter,
project, order, limit. Now returns `Result` through the fallible trait; an I/O
error → `58030`.

**Recovery.** `SqlEngine::open(path)` → `FjallKv::open(path)` → fjall journal
replay → fully recovered store. Catalog and sequences are live KV reads; there
is no separate recovery routine.

## Error handling

- `KvError` gains `Io(String)`. The executor maps `KvError::Io` →
  `PgError`/SQLSTATE `58030` (io_error), distinct from `XX000` (logical bug /
  corruption). `KvError::CorruptRow` (corrupt metadata or row) → `XX000`.
- The now-`Result` trait threads errors up through catalog → executor; no lower
  crate panics on I/O.
- `SqlEngine::open` returns a clean error on fjall open failure (no panic).

## Testing

- **kv:** existing `MemKv` property/unit tests adapted to the `Result` trait;
  new `FjallKv` suite (against a `tempfile` dir) mirroring them; a write-batch
  atomicity test (full apply, or none on simulated mid-batch failure); a
  prefix-scan test.
- **catalog:** CRUD + error-code tests run against BOTH a `MemKv` and a
  `FjallKv` backend (same assertions, two stores) — proving the view is
  backend-agnostic.
- **Durability/recovery (headline):** open temp dir → `CREATE` + multi-row
  `INSERT` → drop the engine (flush/close) → reopen via `SqlEngine::open` →
  assert schema, all rows, and next-rowid survived, and a fresh INSERT gets a
  non-colliding id. Variant: drop + recreate a table across a reopen to prove
  sequence/catalog cleanup persisted.
- **e2e:** existing tokio-postgres round-trips keep running on ephemeral MemKv
  (no regression); one new e2e starts the binary with `--data-dir`, inserts,
  restarts the binary on the same dir, and selects the data back.
- **Conformance:** unchanged corpus on the ephemeral engine; parity stays
  ~96.4% (durability doesn't change query semantics).
- **CI gates:** all SP1/SP2 gates stay green; `cargo-deny` + `check-no-native.sh`
  now also vouch for fjall's purity (shipped tree stays C-free).

## Scope boundaries (tracked OUT of scope)

- **SP4:** transactions (`BEGIN`/`COMMIT`/`ROLLBACK`), MVCC, snapshot isolation,
  version GC. `write_batch` is the seam SP4 extends.
- No `UPDATE`/`DELETE` (unchanged from SP2; honest implementation wants MVCC).
- No configurable fsync policy (default: fsync per statement commit — correctness
  over throughput; a knob is a later optimization). No compaction tuning.
- No schema change beyond CREATE/DROP. No `pg_catalog` SQL views yet (the catalog
  is stored as KV but not exposed as queryable relations — that's a later slice,
  now unblocked by storing the catalog in KV).
- Pre-existing SP2 carry-overs unrelated to storage stay deferred:
  `pgwire::engine::oids` duplicates INT4/TEXT (cosmetic); `conformance::split_
  statements` does `byte as char` (Latin-1 corner, latent for non-ASCII corpus).

## Success criteria

1. `crabgresql --data-dir <path>`: `CREATE TABLE` + `INSERT`, restart the binary
   on the same dir, `SELECT` returns the data with correct rows and types
   (verified through psql/tokio-postgres).
2. The rowid allocator is durable: after restart, a new INSERT gets a
   non-colliding rowid (the SP2 carry-over is fixed).
3. Catalog and sequences persist in the KV store; `DROP TABLE` cleanup survives
   restart.
4. A statement's writes are atomic on crash (write-batch all-or-nothing),
   verified by the atomicity test.
5. `FjallKv` and `MemKv` pass the same kv + catalog test suites (backend-
   agnostic seam).
6. All CI gates green: zero unsafe, pure-Rust shipped tree (fjall included), fmt,
   clippy, conformance parity unchanged.
