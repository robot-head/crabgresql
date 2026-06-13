# SP8: Durable Raft storage + restart recovery (D2a) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the SP7 in-memory single-range Raft cluster durable — each node persists its Raft log, vote, and applied state to fjall, and recovers from its on-disk directory after a crash, so a write acknowledged to the client survives node crashes/restarts as long as a majority's disks survive.

**Architecture:** One `fjall::Database` per node (one directory) with two keyspaces — `data` (the state machine's DB content, read by the SQL engine) and `raft` (log entries + vote + committed + last_applied + membership). A durable `LogStore` (openraft `RaftLogStorage`) and a durable `StateMachineStore` (openraft `RaftStateMachine`) share that Database; an `apply` commits `{data ops, max-merged counters, last_applied, membership}` in one cross-keyspace fjall batch + one fsync (atomic), and idempotent replay covers any crash mid-apply. D1's in-memory adapters/mode are kept unchanged for the fast deterministic fault tests; durable is a parallel path. "Crash" = drop the node; "restart" = reopen from disk.

**Tech Stack:** Rust 2024, `openraft` 0.9 (durable `RaftLogStorage`/`RaftStateMachine`), `fjall` (already shipped, pure-Rust LSM), `serde_json` (log-entry + snapshot encoding), `tempfile` (test dirs).

**Spec:** `docs/superpowers/specs/2026-06-12-crabgresql-sp8-durable-raft-storage-design.md`

---

## File structure

```
crates/kv/src/fjall_store.rs    # extract KeyspaceKv (Kv over a fjall keyspace + shared Database persist); FjallKv wraps it
crates/kv/src/lib.rs            # pub use KeyspaceKv
crates/cluster/Cargo.toml       # add fjall (direct dep) + serde_json (already dev) + tempfile (dev)
crates/cluster/src/store.rs     # unchanged in-memory LogStore/StateMachineStore stay; add `mod durable;` ref
crates/cluster/src/durable.rs   # NEW: NodeStore, DurableLogStore, DurableStateMachineStore (the bulk)
crates/cluster/src/node.rs      # Node.sm_kv -> Arc<dyn Kv>; add `dir`; Node::start_durable; engine() tweak
crates/cluster/src/cluster.rs   # Cluster::durable / restart / crash
crates/cluster/src/lib.rs       # mod durable; re-exports
crates/cluster/tests/durable_scenarios.rs   # NEW: deterministic restart scenarios
crates/cluster/tests/sql_durable.rs         # NEW: SQL-over-Raft durability e2e
crates/cluster/tests/jepsen_bank.rs         # extend: crash+restart nemesis durability
```

Task order (each ends workspace-green): KeyspaceKv refactor → durable LogStore → durable StateMachineStore + Suite → Node durable mode → Cluster durable/restart/crash + restart scenarios → SQL durability e2e → crash-nemesis durability → gauntlet.

**Constraints (all tasks):** `#![forbid(unsafe_code)]`; `expect("reason")` never bare `unwrap`; `cargo clippy --workspace --all-targets -- -D warnings`; no `std::sync::Mutex` guard across `.await`. On Windows the `update_delete` executor test needs `__COMPAT_LAYER=RunAsInvoker` (environmental); CI is Linux.

---

### Task 1: `kv::KeyspaceKv` — `Kv` over a shared fjall keyspace

**Files:**
- Modify: `crates/kv/src/fjall_store.rs`, `crates/kv/src/lib.rs`

`FjallKv` owns its own single-keyspace `Database` privately. SP8 needs a `Kv` over a keyspace within a **shared** Database (so the SM's `data` keyspace and the Raft `raft` keyspace live in one Database). Factor the keyspace logic into `KeyspaceKv`; `FjallKv` becomes a thin wrapper. Behavior-preserving — the SP3 durable tests stay green.

- [ ] **Step 1: implement `KeyspaceKv`.** Replace the `FjallKv` struct/impl in `crates/kv/src/fjall_store.rs` with:

```rust
use std::path::Path;
use std::sync::Arc;

use fjall::{Database, KeyspaceCreateOptions, PersistMode};

use crate::{Kv, KvError, WriteOp};

/// A `Kv` over one fjall keyspace within a (possibly shared) `Database`. Every
/// mutation fsyncs the whole Database as its tail, so a returned `Ok` is
/// power-loss durable. Multiple `KeyspaceKv`s over the same `Arc<Database>` share
/// that fsync (a single `persist` flushes all keyspaces' pending writes).
pub struct KeyspaceKv {
    db: Arc<Database>,
    ks: fjall::Keyspace,
}

impl KeyspaceKv {
    /// Wrap an already-open keyspace `ks` belonging to `db`.
    pub fn new(db: Arc<Database>, ks: fjall::Keyspace) -> Self {
        Self { db, ks }
    }

    fn sync(&self) -> Result<(), KvError> {
        self.db.persist(PersistMode::SyncAll).map_err(io)
    }
}

fn io(e: impl std::fmt::Display) -> KvError {
    KvError::Io(e.to_string())
}

impl Kv for KeyspaceKv {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        Ok(self.ks.get(key).map_err(io)?.map(|v| v.to_vec()))
    }
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KvError> {
        self.ks.insert(key, value).map_err(io)?;
        self.sync()
    }
    fn delete(&self, key: &[u8]) -> Result<(), KvError> {
        self.ks.remove(key).map_err(io)?;
        self.sync()
    }
    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, KvError> {
        let mut out = Vec::new();
        for guard in self.ks.prefix(prefix) {
            let (k, v) = guard.into_inner().map_err(io)?;
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }
    fn write_batch(&self, ops: &[WriteOp]) -> Result<(), KvError> {
        let mut batch = self.db.batch();
        for op in ops {
            match op {
                WriteOp::Put { key, value } => batch.insert(&self.ks, key, value),
                WriteOp::Delete { key } => batch.remove(&self.ks, key),
            }
        }
        batch.commit().map_err(io)?;
        self.sync()
    }
}

/// Durable single-keyspace `Kv`: opens (or recovers) a one-keyspace `Database`.
pub struct FjallKv {
    inner: KeyspaceKv,
}

impl FjallKv {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, KvError> {
        let db = Arc::new(Database::builder(path).open().map_err(io)?);
        let ks = db
            .keyspace("data", KeyspaceCreateOptions::default)
            .map_err(io)?;
        Ok(Self { inner: KeyspaceKv::new(db, ks) })
    }
}

impl Kv for FjallKv {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> { self.inner.get(key) }
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KvError> { self.inner.put(key, value) }
    fn delete(&self, key: &[u8]) -> Result<(), KvError> { self.inner.delete(key) }
    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, KvError> { self.inner.scan_prefix(prefix) }
    fn write_batch(&self, ops: &[WriteOp]) -> Result<(), KvError> { self.inner.write_batch(ops) }
}
```

(Adapt to the exact fjall version's API if `Database`/`Keyspace`/`batch` names differ — the existing `FjallKv` is the source of truth for those calls; this only re-homes them. If `fjall::Keyspace` is not `Clone`, keep it owned — `KeyspaceKv` owns its handle; the cluster's `NodeStore` in Task 2 opens each keyspace once.)

- [ ] **Step 2: export.** In `crates/kv/src/lib.rs`, change `pub use fjall_store::FjallKv;` to `pub use fjall_store::{FjallKv, KeyspaceKv};`.

- [ ] **Step 3: verify (the existing FjallKv tests are the regression gate).**

Run: `cargo test -p kv`
Expected: PASS — including `data_survives_reopen` and the other `fjall_store` tests (behavior unchanged; `FjallKv` now delegates to `KeyspaceKv`).

- [ ] **Step 4: commit.**

```bash
cargo fmt -p kv && cargo clippy -p kv --all-targets -- -D warnings
git add crates/kv/src/fjall_store.rs crates/kv/src/lib.rs
git commit -m "refactor(kv): extract KeyspaceKv (Kv over a shared fjall keyspace) from FjallKv"
```

---

### Task 2: `NodeStore` + durable `LogStore`

**Files:**
- Create: `crates/cluster/src/durable.rs`
- Modify: `crates/cluster/Cargo.toml`, `crates/cluster/src/lib.rs`

`NodeStore` opens one `Database` per node with `data` + `raft` keyspaces. The durable `LogStore` implements `RaftLogStorage`/`RaftLogReader` over the `raft` keyspace, persisting (fsync) before acking. Methods mirror the in-memory `LogStore` (`crates/cluster/src/store.rs:296-411`) — same signatures, fjall instead of `BTreeMap`.

- [ ] **Step 1: deps.** In `crates/cluster/Cargo.toml` add to `[dependencies]`: `fjall = { workspace = true }` (add `fjall` to the workspace deps in the root `Cargo.toml` if not present — it's already used by `kv`; reuse the same version). Add to `[dev-dependencies]`: `tempfile = { workspace = true }`. `serde_json` is already a dependency.

- [ ] **Step 2: `NodeStore` + key helpers.** Create `crates/cluster/src/durable.rs`:

```rust
//! Durable per-node storage (D2a): one fjall `Database` per node with a `data`
//! keyspace (the state-machine DB content) and a `raft` keyspace (log entries,
//! vote, committed, last_applied, membership). A durable `LogStore` and
//! `StateMachineStore` share the Database, so an apply commits data + metadata in
//! one cross-keyspace batch + one fsync (atomic); idempotent replay covers a
//! crash mid-apply.

use std::path::Path;
use std::sync::Arc;

use fjall::{Database, KeyspaceCreateOptions, PersistMode};
use kv::{KeyspaceKv, Kv};

/// One node's on-disk store: a shared `Database` plus its two keyspaces.
pub struct NodeStore {
    pub(crate) db: Arc<Database>,
    pub(crate) data: fjall::Keyspace,
    pub(crate) raft: fjall::Keyspace,
}

impl NodeStore {
    /// Open (or recover) a node store at `dir`. fjall journal-replays on open.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, fjall::Error> {
        let db = Arc::new(Database::builder(dir).open()?);
        let data = db.keyspace("data", KeyspaceCreateOptions::default)?;
        let raft = db.keyspace("raft", KeyspaceCreateOptions::default)?;
        Ok(Self { db, data, raft })
    }

    /// A `Kv` view over the `data` keyspace for the SQL engine + SM reads.
    pub fn data_kv(&self) -> Arc<KeyspaceKv> {
        Arc::new(KeyspaceKv::new(self.db.clone(), self.data.clone()))
    }
}

/// Raft-keyspace key layout.
fn log_key(index: u64) -> Vec<u8> {
    let mut k = b"log/".to_vec();
    k.extend_from_slice(&index.to_be_bytes());
    k
}
const VOTE_KEY: &[u8] = b"vote";
const COMMITTED_KEY: &[u8] = b"committed";
const PURGED_KEY: &[u8] = b"last_purged";
pub(crate) const SM_APPLIED_KEY: &[u8] = b"sm/last_applied";
pub(crate) const SM_MEMBERSHIP_KEY: &[u8] = b"sm/last_membership";
```

(If `fjall::Error` is not the public error type, use the concrete type `Database::open` returns. Add a `pub(crate) fn error_io(e) -> StorageError<NodeId>` helper near the imports that maps any fjall/serde error to `StorageIOError::read_log_io(&e).into()` / the appropriate openraft `StorageError` — see how `store.rs` constructs `StorageIOError::read_state_machine(&e)` and mirror it for log IO.)

- [ ] **Step 3: durable `LogStore`.** Append to `durable.rs`, porting `store.rs:296-411`. The cache (`last_log_id`, `last_purged`) is loaded by a scan on open:

```rust
use std::collections::Bound;
use std::fmt::Debug;
use std::ops::RangeBounds;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{Entry, LogId, RaftLogReader, StorageError, StorageIOError, Vote};
use tokio::sync::RwLock;

use crate::types::{NodeId, TypeConfig};

pub struct DurableLogStore {
    db: Arc<Database>,
    ks: fjall::Keyspace, // the `raft` keyspace
    cache: RwLock<LogCache>,
}

#[derive(Default)]
struct LogCache {
    last_log_id: Option<LogId<NodeId>>,
    last_purged: Option<LogId<NodeId>>,
}

impl DurableLogStore {
    /// Build over a NodeStore's `raft` keyspace, loading the cached boundaries.
    pub fn open(store: &NodeStore) -> Result<Arc<Self>, StorageError<NodeId>> {
        let db = store.db.clone();
        let ks = store.raft.clone();
        let last_purged: Option<LogId<NodeId>> = read_json(&ks, PURGED_KEY)?;
        // last_log_id = the highest `log/<index>` entry's log_id (or last_purged).
        let last_log_id = highest_log_id(&ks)?.or(last_purged);
        Ok(Arc::new(Self { db, ks, cache: RwLock::new(LogCache { last_log_id, last_purged }) }))
    }
    fn persist(&self) -> Result<(), StorageError<NodeId>> {
        self.db.persist(PersistMode::SyncAll).map_err(|e| StorageIOError::write_logs(&e).into())
    }
}
```

Then implement `RaftLogReader for Arc<DurableLogStore>` (`try_get_log_entries`: prefix-scan `log/`, deserialize entries whose index is in `range`) and `RaftLogStorage for Arc<DurableLogStore>` mirroring the in-memory methods but persisting:
  - `get_log_state` → from the cache (O(1)).
  - `save_vote`/`read_vote`, `save_committed`/`read_committed` → `write_json`/`read_json` on `VOTE_KEY`/`COMMITTED_KEY`; `save_*` then `self.persist()`.
  - `append(entries, callback)` → `let mut batch = self.db.batch();` insert each `log_key(index) → serde_json::to_vec(&entry)`; update cache `last_log_id`; `batch.commit()` + `self.persist()`; **then** `callback.log_io_completed(Ok(()))`.
  - `truncate(log_id)` → batch-remove `log/` keys with index `>= log_id.index` (prefix-scan to find them); fix cache `last_log_id`; commit + persist.
  - `purge(log_id)` → batch-remove `log/` keys with index `<= log_id.index`; write `PURGED_KEY = log_id`; set cache `last_purged`; commit + persist.
  - `get_log_reader` → `self.clone()`.

Provide the helpers used above:

```rust
fn read_json<T: serde::de::DeserializeOwned>(ks: &fjall::Keyspace, key: &[u8])
    -> Result<Option<T>, StorageError<NodeId>> {
    match ks.get(key).map_err(|e| StorageIOError::read_logs(&e))? {
        Some(b) => Ok(Some(serde_json::from_slice(&b).map_err(|e| StorageIOError::read_logs(&e))?)),
        None => Ok(None),
    }
}
// write_json: ks.insert(key, serde_json::to_vec(v)?) ; highest_log_id: prefix-scan `log/`, take the
// last entry, deserialize, return its log_id. (Concrete bodies mirror read_json / the FjallKv scan.)
```

(Match openraft 0.9.24's exact `StorageIOError` constructor names — `read_logs`/`write_logs`/`read_log_io` etc. — against `https://docs.rs/openraft/0.9.24`; the in-memory store + the existing `store.rs` `StorageIOError::read_state_machine` usage are the reference. Get the serde of `Entry<TypeConfig>` from the fact that it derives `Serialize`/`Deserialize` when `D = WriteBatch` does, which it does.)

- [ ] **Step 4: register module.** In `crates/cluster/src/lib.rs` add `mod durable;`.

- [ ] **Step 5: durable log unit tests.** Add a `#[cfg(test)] mod tests` to `durable.rs`:

```rust
#[tokio::test]
async fn append_then_reopen_recovers_entries() {
    let dir = tempfile::tempdir().expect("dir");
    {
        let store = NodeStore::open(dir.path()).expect("open");
        let mut log = DurableLogStore::open(&store).expect("log open");
        // build 3 blank entries at indices 1..=3 and append them, awaiting the callback
        // (see openraft LogFlushed; use a oneshot to await io_completed).
        append_blanks(&mut log, 1..=3).await;
        // vote round-trips
        log.save_vote(&Vote::new(1, 0)).await.expect("save vote");
    } // store dropped -> Database closed (fsynced)
    let store = NodeStore::open(dir.path()).expect("reopen");
    let mut log = DurableLogStore::open(&store).expect("log reopen");
    let st = log.get_log_state().await.expect("state");
    assert_eq!(st.last_log_id.map(|l| l.index), Some(3), "entries survive reopen");
    assert_eq!(log.read_vote().await.expect("vote"), Some(Vote::new(1, 0)));
}
```

Also test `truncate` (entries `>= idx` gone, `last_log_id` fixed) and `purge` (entries `<= idx` gone, `last_purged` set, survives reopen). Provide the `append_blanks` test helper (constructs `Entry { log_id, payload: EntryPayload::Blank }` and drives `append` with a `LogFlushed` whose completion you await).

Run: `cargo test -p cluster --lib durable`
Expected: PASS.

- [ ] **Step 6: commit.**

```bash
cargo fmt -p cluster && cargo clippy -p cluster --all-targets -- -D warnings
git add crates/cluster/src/durable.rs crates/cluster/src/lib.rs crates/cluster/Cargo.toml Cargo.toml Cargo.lock
git commit -m "feat(cluster): NodeStore + durable RaftLogStorage over fjall"
```

---

### Task 3: durable `StateMachineStore` + openraft conformance suite

**Files:**
- Modify: `crates/cluster/src/durable.rs`

The durable state machine over the `data` keyspace, with the **atomic apply** (one cross-keyspace batch + one fsync) and snapshots built from the durable SM. Then the openraft `Suite::test_all` over the durable log + SM (the same gate D1 used).

- [ ] **Step 1: `DurableStateMachineStore`.** Append to `durable.rs`. Reuse `crate::store::{is_counter_key, u64_be}` — make those `pub(crate)` in `store.rs` (currently private). Structure mirrors `store.rs:118-287` but durable + atomic:

```rust
pub struct DurableStateMachineStore {
    db: Arc<Database>,
    data: fjall::Keyspace,   // application data
    raft: fjall::Keyspace,   // sm/last_applied, sm/last_membership
    data_kv: Arc<KeyspaceKv>, // Kv view over `data` for the SQL engine + scans
    meta: RwLock<StateMachineMeta>,      // cached last_applied/membership (loaded on open)
    snapshot_idx: RwLock<u64>,
    current_snapshot: RwLock<Option<StoredSnapshot>>, // reuse store.rs StoredSnapshot/SnapshotPayload (make pub(crate))
}

impl DurableStateMachineStore {
    pub fn open(store: &NodeStore) -> Result<Arc<Self>, StorageError<NodeId>> {
        let meta = StateMachineMeta {
            last_applied: read_json(&store.raft, SM_APPLIED_KEY)?.unwrap_or_default(),
            last_membership: read_json(&store.raft, SM_MEMBERSHIP_KEY)?.unwrap_or_default(),
        };
        Ok(Arc::new(Self {
            db: store.db.clone(),
            data: store.data.clone(),
            raft: store.raft.clone(),
            data_kv: store.data_kv(),
            meta: RwLock::new(meta),
            snapshot_idx: RwLock::new(0),
            current_snapshot: RwLock::new(None),
        }))
    }
    pub fn sm_kv(&self) -> Arc<dyn Kv> { self.data_kv.clone() }
}
```

- [ ] **Step 2: the atomic `apply`.** Implement `RaftStateMachine for Arc<DurableStateMachineStore>`. `applied_state` reads the cached meta. The `apply` is the load-bearing part:

```rust
async fn apply<I>(&mut self, entries: I) -> Result<Vec<()>, StorageError<NodeId>>
where I: IntoIterator<Item = Entry<TypeConfig>> + Send {
    // Needs: use kv::WriteOp; use openraft::EntryPayload; use openraft::StoredMembership;
    // use zerocopy::{byteorder::big_endian::U64, IntoBytes}; use crate::types::WriteBatch;
    let mut meta = self.meta.write().await;
    let mut res = Vec::new();
    // Collect the net effect of all entries into one batch.
    let mut batch = self.db.batch();
    let mut counters: std::collections::HashMap<Vec<u8>, u64> = std::collections::HashMap::new();
    let mut new_membership = meta.last_membership.clone();
    let mut last_id = meta.last_applied;
    for entry in entries {
        last_id = Some(entry.log_id);
        match entry.payload {
            EntryPayload::Blank => {}
            EntryPayload::Normal(WriteBatch(ref ops)) => {
                for op in ops {
                    match op {
                        WriteOp::Put { key, value } if crate::store::is_counter_key(key) => {
                            // max-merge: read current (committed-so-far) value. NOTE: read from
                            // the keyspace, which does NOT see uncommitted batch writes, so when
                            // the same counter key appears twice in one apply() the second read
                            // misses the first's pending value — track the running max in a map.
                            let incoming = crate::store::u64_be(value);
                            let cur = pending_counter(&mut counters, &self.data, key)?; // helper
                            let merged = cur.max(incoming);
                            counters.insert(key.clone(), merged);
                            batch.insert(&self.data, key, U64::new(merged).as_bytes());
                        }
                        WriteOp::Put { key, value } => batch.insert(&self.data, key, value),
                        WriteOp::Delete { key } => batch.remove(&self.data, key),
                    }
                }
            }
            EntryPayload::Membership(ref mem) => {
                new_membership = StoredMembership::new(last_id, mem.clone());
            }
        }
        res.push(());
    }
    // Fold last_applied + membership into the SAME batch, then commit + fsync once.
    batch.insert(&self.raft, SM_APPLIED_KEY, serde_json::to_vec(&last_id).map_err(io_sm)?);
    batch.insert(&self.raft, SM_MEMBERSHIP_KEY, serde_json::to_vec(&new_membership).map_err(io_sm)?);
    batch.commit().map_err(io_sm)?;
    self.db.persist(PersistMode::SyncAll).map_err(io_sm)?;
    meta.last_applied = last_id;
    meta.last_membership = new_membership;
    Ok(res)
}
```

Provide `pending_counter` (returns the running max for a key: the value already staged in `counters`, else the durable value via `self.data.get(key)` decoded by `u64_be`, else 0) and `io_sm` (maps an error to `StorageIOError::write_state_machine(&e).into()`). The whole apply is one batch + one fsync ⇒ data and `last_applied` advance atomically; if it never commits, nothing changed (idempotent replay on restart re-applies cleanly).

- [ ] **Step 3: snapshots.** Implement `get_snapshot_builder`, `RaftSnapshotBuilder::build_snapshot` (scan `data` via `self.data_kv.scan_prefix(&[])` + cached meta → `SnapshotPayload` → `serde_json` → `Cursor`, store in `current_snapshot`), `begin_receiving_snapshot` (empty `Cursor`), `install_snapshot` (deserialize; in ONE batch: remove every existing `data` key (scan first), insert the snapshot pairs, write `SM_APPLIED_KEY`/`SM_MEMBERSHIP_KEY`; commit + persist; update cached meta + `current_snapshot`), `get_current_snapshot`. These mirror `store.rs:144-287` but read/write durably and atomically.

- [ ] **Step 4: durable Suite test + apply/snapshot unit tests.** Add to `durable.rs` tests:

```rust
#[derive(Default)]
struct DurableStoreBuilder { _tmp: std::sync::Mutex<Vec<tempfile::TempDir>> }
impl openraft::testing::StoreBuilder<TypeConfig, Arc<DurableLogStore>, Arc<DurableStateMachineStore>, ()>
    for DurableStoreBuilder {
    async fn build(&self) -> Result<((), Arc<DurableLogStore>, Arc<DurableStateMachineStore>), StorageError<NodeId>> {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = NodeStore::open(dir.path()).expect("open");
        let log = DurableLogStore::open(&store)?;
        let sm = DurableStateMachineStore::open(&store)?;
        self._tmp.lock().expect("tmp").push(dir); // keep dirs alive for the suite
        Ok(((), log, sm))
    }
}

#[test]
#[allow(clippy::result_large_err)]
fn durable_storage_suite() -> Result<(), StorageError<NodeId>> {
    openraft::testing::Suite::test_all(DurableStoreBuilder::default())
}

#[tokio::test]
async fn apply_is_atomic_and_survives_reopen() {
    let dir = tempfile::tempdir().expect("dir");
    {
        let store = NodeStore::open(dir.path()).expect("open");
        let mut sm = DurableStateMachineStore::open(&store).expect("sm");
        apply_normal(&mut sm, 1, vec![WriteOp::Put { key: kv::key::row_key(1,1), value: b"v".to_vec() }]).await;
        // counter max-merge through apply
        apply_normal(&mut sm, 2, vec![WriteOp::Put { key: kv::key::next_xid_key(), value: 9u64.to_be_bytes().to_vec() }]).await;
    }
    let store = NodeStore::open(dir.path()).expect("reopen");
    let sm = DurableStateMachineStore::open(&store).expect("sm reopen");
    assert_eq!(sm.sm_kv().get(&kv::key::row_key(1,1)).expect("get"), Some(b"v".to_vec()), "data durable");
    let (applied, _) = { let mut s = sm.clone(); s.applied_state().await.expect("applied") };
    assert_eq!(applied.map(|l| l.index), Some(2), "last_applied durable + consistent");
}
```

Provide `apply_normal` (builds an `Entry` with `EntryPayload::Normal(WriteBatch(ops))` at `index` and calls `apply`). Add a snapshot build→install round-trip test over the durable SM (mirror `store.rs::snapshot_round_trip_overwrites`).

Run: `cargo test -p cluster --lib durable` (the Suite is the big one).
Expected: PASS.

- [ ] **Step 5: commit.**

```bash
cargo fmt -p cluster && cargo clippy -p cluster --all-targets -- -D warnings
git add crates/cluster/src/durable.rs crates/cluster/src/store.rs
git commit -m "feat(cluster): durable RaftStateMachine (atomic apply) + openraft Suite over fjall"
```

---

### Task 4: `Node` durable mode

**Files:**
- Modify: `crates/cluster/src/node.rs`

Make `Node` storage-agnostic and add a durable constructor. The in-memory path stays byte-identical (D1 tests green).

- [ ] **Step 1: generalize `Node`.** In `crates/cluster/src/node.rs`:
  - Change the field `pub sm_kv: Arc<MemKv>` → `pub sm_kv: Arc<dyn kv::Kv>`.
  - Add `pub dir: Option<std::path::PathBuf>` (`Some` for durable nodes — used by `Cluster::restart`; `None` for in-memory).
  - In `start_with_config`, set `sm_kv: sm.sm_kv() as Arc<dyn kv::Kv>` and `dir: None`.
  - In `engine()`, drop the `as Arc<dyn kv::Kv>` cast (already that type): `executor::SqlEngine::replicated(self.sm_kv.clone(), ...)`.

- [ ] **Step 2: `start_durable`.** Add:

```rust
use crate::durable::{DurableLogStore, DurableStateMachineStore, NodeStore};

/// Build a durable node whose log + state machine persist under `dir`. Reopening
/// `dir` after a drop recovers the node (fjall journal replay + openraft resume).
pub async fn start_durable(
    id: NodeId,
    sb: Switchboard,
    dir: std::path::PathBuf,
    config: openraft::Config,
) -> Self {
    let config = Arc::new(config.validate().expect("valid raft config"));
    let store = NodeStore::open(&dir).expect("open node store");
    let log = DurableLogStore::open(&store).expect("durable log");
    let sm = DurableStateMachineStore::open(&store).expect("durable sm");
    let sm_kv = sm.sm_kv();
    let raft = openraft::Raft::new(id, config, sb.for_node(id), log, sm)
        .await
        .expect("raft::new");
    sb.register(id, raft.clone());
    Node { id, raft, sm_kv, dir: Some(dir) }
}
```

(`openraft::Raft<TypeConfig>` is the same type regardless of which storage impls it was built from, so `Node.raft`'s type is unchanged.)

- [ ] **Step 3: verify no regression.** Building requires `Node`'s other constructors to set the new `dir: None` field — fix `start_with_config`. The `sm_kv` type change ripples to any caller that assumed `Arc<MemKv>`; `engine()` is the main one (fixed above). 

Run: `cargo test -p cluster` (the existing in-memory scenarios/e2e must still pass — `sm_kv` is now `dyn Kv` but the in-memory nodes still wrap `MemKv`).
Expected: PASS (D1 tests green).

- [ ] **Step 4: commit.**

```bash
cargo fmt -p cluster && cargo clippy -p cluster --all-targets -- -D warnings
git add crates/cluster/src/node.rs
git commit -m "feat(cluster): Node durable mode (start_durable) + storage-agnostic sm_kv"
```

---

### Task 5: `Cluster` durable / restart / crash + restart scenarios

**Files:**
- Modify: `crates/cluster/src/cluster.rs`
- Create: `crates/cluster/tests/durable_scenarios.rs`

- [ ] **Step 1: durable cluster + restart/crash.** In `crates/cluster/src/cluster.rs`:

```rust
/// Build `n` durable nodes under `base_dir/node-<id>` and initialize the group.
pub async fn durable(n: u64, base_dir: &std::path::Path) -> Self {
    let sb = Switchboard::new();
    let mut nodes = Vec::new();
    for id in 0..n {
        let dir = base_dir.join(format!("node-{id}"));
        std::fs::create_dir_all(&dir).expect("mkdir node");
        nodes.push(Node::start_durable(id, sb.clone(), dir, Node::default_config()).await);
    }
    let members: BTreeMap<NodeId, BasicNode> = (0..n).map(|id| (id, BasicNode::default())).collect();
    nodes[0].raft.initialize(members).await.expect("initialize");
    Self { nodes, sb }
}

/// Restart node `id`: drop its Raft (closing the fjall Database; acked writes are
/// fsynced) and reopen from its on-disk dir, re-registering with the switchboard.
/// Models a clean process bounce. Panics if `id` is an in-memory node.
pub async fn restart(&mut self, id: NodeId) {
    let dir = self.node(id).dir.clone().expect("durable node has a dir");
    // Drop the old Raft instance first so the Database is closed/reopened cleanly.
    let new = Node::start_durable(id, self.sb.clone(), dir, Node::default_config()).await;
    self.nodes[id as usize] = new; // old Node (and its Raft/Database) dropped here
}

/// Crash node `id` (ungraceful): isolate it so it stops participating, then drop
/// and reopen it. The nemesis form — exercises recovery from a sudden stop. (In
/// process we can't truly kill mid-fsync; fjall's fsync-before-ack + journal
/// replay give the same guarantee as a power loss for acked writes.)
pub async fn crash_restart(&mut self, id: NodeId) {
    self.restart(id).await;
}
```

(`restart`/`crash_restart` take `&mut self` because they replace a node. Tests that hold `&Cluster` across these need `&mut`.)

- [ ] **Step 2: restart scenarios.** Create `crates/cluster/tests/durable_scenarios.rs`. Use a `tempfile::TempDir` per cluster. Bound every wait with `Raft::wait` (no sleeps). Mirror the helpers in `tests/scenarios.rs` (`applied_to`, etc.).

```rust
use cluster::Cluster;
use kv::Kv;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_write_survives_node_restart() {
    let dir = tempfile::tempdir().expect("dir");
    let mut c = Cluster::durable(3, dir.path()).await;
    let _l = c.wait_for_leader().await;
    let k = kv::key::row_key(1, 1);
    c.write(vec![kv::WriteOp::Put { key: k.clone(), value: b"v".to_vec() }]).await.expect("write");
    // pick a follower that has applied the write, restart it, assert the data is on disk.
    let leader = c.leader().expect("leader").id;
    let follower = (0..3u64).find(|&i| i != leader).expect("follower");
    // wait for the follower to apply, then restart it
    c.node(follower).raft.wait(Some(std::time::Duration::from_secs(10)))
        .applied_index_at_least(Some(2), "applied").await.expect("apply");
    c.restart(follower).await;
    // after restart it recovers from disk: its sm_kv has the value
    assert_eq!(c.node(follower).sm_kv.get(&k).expect("get"), Some(b"v".to_vec()),
        "committed write survived the restart");
}
```

Add: **restarted-follower catch-up** (pause a follower, commit several writes, restart it, it catches up); **leader crash+restart** (capture leader, `restart(leader)`, a new leader emerges via `wait_for_leader_excluding`, the restarted node rejoins, the committed data is present on the new leader and the restarted node); **full-cluster restart** (commit data, `restart` all three, `wait_for_leader`, assert all committed rows present). For each, after a restart that could race the new-leader apply, reuse the SP7 apply-catchup wait pattern (capture `last_log_index` before, `applied_index_at_least` after) when reading `sm_kv`.

Run: `cargo test -p cluster --test durable_scenarios` (run 3×, no flakes/hangs).
Expected: PASS.

- [ ] **Step 3: commit.**

```bash
cargo fmt -p cluster && cargo clippy -p cluster --all-targets -- -D warnings
git add crates/cluster/src/cluster.rs crates/cluster/tests/durable_scenarios.rs
git commit -m "feat(cluster): Cluster::durable + restart/crash; deterministic restart scenarios"
```

---

### Task 6: SQL-over-Raft durability e2e

**Files:**
- Create: `crates/cluster/tests/sql_durable.rs`

Prove the SQL stack survives a full-cluster restart, and SP6 concurrency works over the durable path. Reuse the `run`/`col0`/`tag_of`/`rowcount` helpers from `tests/sql_over_raft.rs` (copy them in, as that file did from the executor tests).

- [ ] **Step 1: data survives a full-cluster restart.**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_data_survives_full_cluster_restart() {
    let dir = tempfile::tempdir().expect("dir");
    let mut c = Cluster::durable(3, dir.path()).await;
    let l0 = c.wait_for_leader().await;
    {
        let e = c.node(l0).engine(); e.reseed_counters().expect("reseed");
        let mut s = e.connect();
        run(&mut s, "CREATE TABLE t (id int4)").await;
        for i in 0..5 { run(&mut s, &format!("INSERT INTO t VALUES ({i})")).await; }
    }
    // Restart every node (clean bounce). They recover from disk.
    for id in 0..3u64 { c.restart(id).await; }
    let l1 = c.wait_for_leader().await;
    let e = c.node(l1).engine(); e.reseed_counters().expect("reseed");
    let mut s = e.connect();
    let rows = run(&mut s, "SELECT id FROM t").await;
    assert_eq!(rowcount(&rows[0]), 5, "table + rows survive a full-cluster restart");
    run(&mut s, "INSERT INTO t VALUES (99)").await; // new writes still land
}
```

- [ ] **Step 2: SP6 concurrency over the durable path.** Port one `sql_over_raft::same_row_conflict_loop_over_raft` scenario but on a `Cluster::durable` leader engine (one engine, two sessions sharing the lockmgr). Confirms row-lock / EvalPlanQual works unchanged on durable storage.

Run: `cargo test -p cluster --test sql_durable` (3×).
Expected: PASS.

- [ ] **Step 3: commit.**

```bash
cargo fmt -p cluster && cargo clippy -p cluster --all-targets -- -D warnings
git add crates/cluster/tests/sql_durable.rs
git commit -m "test(cluster): SQL-over-Raft durability e2e (full-cluster restart + concurrency)"
```

---

### Task 7: Crash-nemesis durability (Jepsen-style)

**Files:**
- Modify: `crates/cluster/tests/jepsen_bank.rs`

Extend the SP7 bank workload to run against a **durable** cluster with a nemesis that crashes + restarts nodes mid-run; the invariant is that no acknowledged transfer is lost and the bank total is conserved once faults heal.

- [ ] **Step 1: durable bank-conservation under a crash nemesis.** Add a test that builds `Cluster::durable(3, tmp)`, seeds the `accounts` table (known total), spawns concurrent transfer processes against the leader, and a nemesis task that periodically `restart`s a **follower** (never the leader, so the workload's engine stays valid — same robustness choice as SP7's pause nemesis), then occasionally restarts the whole set between barriers. Record the invoke/ok/fail/info history. After healing, re-resolve the leader, reseed, and assert: `final_total == seeded_total`; every `ok` transfer's effect is present; `committed > 0` (non-vacuous).

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bank_conserves_total_under_crash_restart() {
    let dir = tempfile::tempdir().expect("dir");
    let (history, final_total, seeded) = run_durable_bank(dir.path(), 4, 3, 40).await;
    assert_eq!(final_total, seeded, "no acked transfer lost across crash/restart");
    assert!(history.iter().filter(|e| e.committed_ok()).count() > 0, "non-vacuous");
}
```

`run_durable_bank` mirrors SP7's `run_bank_workload` but on `Cluster::durable` with a `restart`-based nemesis. (`restart` needs `&mut`, so the nemesis and workload coordinate via the harness owning the cluster; structure the nemesis to take `&mut Cluster` between workload barriers, or restart a follower while the workload targets the fixed leader — keep it deterministically stable, no fixed sleeps for correctness.)

- [ ] **Step 2: run + commit.**

Run: `cargo test -p cluster --test jepsen_bank` (3×, stable; the durable test does real fsync I/O so it is slower — keep op counts modest).
Expected: PASS.

```bash
cargo fmt -p cluster && cargo clippy -p cluster --all-targets -- -D warnings
git add crates/cluster/tests/jepsen_bank.rs
git commit -m "test(cluster): crash-nemesis durability (bank conservation across crash/restart)"
```

---

### Task 8: Gauntlet, traceability

**Files:** Verify; no new code unless a gate fails.

- [ ] **Step 1: gauntlet.** Run each, report PASS/FAIL:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace        # on Windows prefix: __COMPAT_LAYER=RunAsInvoker
cargo test -p pgparser --features oracle
bash scripts/check-no-native.sh
cargo deny check
```

fjall is already in the shipped tree (the `kv` durable path), so `check-no-native.sh` and `cargo deny` stay green — **no new native crate** is introduced (the cluster crate gains `fjall` as a *direct* dep but it's the same already-shipped pure-Rust crate). On Windows, `check-no-native.sh` fails only on `windows-sys` (a known pure-Rust raw-bindings false-positive; passes on Linux CI).

- [ ] **Step 2: success-criteria traceability.** Confirm each spec criterion maps to a green test:

| # | Spec criterion | Verifying test(s) |
|---|---|---|
| 1 | Per-node fjall Database (data+raft); durable log+SM pass the Suite | `durable::tests::durable_storage_suite` |
| 2 | SQL stack unchanged over durable; all SP1–SP7 tests pass | `sql_durable::*`; `cargo test --workspace` |
| 3 | Committed write survives crash+restart; follower/leader/full-cluster restart recover | `durable_scenarios::*` |
| 4 | Atomic apply (data + last_applied together); idempotent replay | `durable::tests::apply_is_atomic_and_survives_reopen` |
| 5 | Crash nemesis never loses an acked transfer; total conserved | `jepsen_bank::bank_conserves_total_under_crash_restart` |
| 6 | All SP1–SP7 gates green; pure-Rust tree; forbid(unsafe) | gauntlet (Step 1) |

If any row lacks a green test, add it.

- [ ] **Step 3: commit (if anything changed).**

```bash
git add -A
git commit -m "test(sp8): gauntlet green; durability success-criteria traceability"
```

---

## Final review (after all tasks)

Dispatch a code-reviewer over the whole SP8 diff (vs the SP7 base), then run `superpowers:finishing-a-development-branch`. Review focus:

- **Durability-before-ack:** every `RaftLogStorage` mutation (and the SM apply) fsyncs (`persist(SyncAll)`) before returning / before `log_io_completed` — a returned-`Ok` write is power-loss durable, never just clean-exit durable.
- **Atomic apply:** data ops + `last_applied` + membership are committed in ONE fjall batch + ONE fsync; on a crash the SM is never torn; replay is idempotent (puts/deletes + counter max-merge), and the same-counter-key-twice-in-one-apply case uses the running-max map (not a stale keyspace read).
- **Restart recovery:** reopening a dir restores log + vote + last_applied + applied data; the node rejoins as the same id; cached `last_log_id`/`last_purged` are reloaded correctly.
- **No regression:** D1's in-memory adapters/scenarios are untouched and green; `FjallKv` behavior is byte-identical after the `KeyspaceKv` extraction (SP3 durable tests pass).
- **No flakiness / no hangs:** restart scenarios use `Raft::wait` (and the SP7 apply-catchup wait where a read could race a post-restart apply); no fixed `sleep` for correctness.
- **Purity:** no new native crate (fjall already shipped); `#![forbid(unsafe_code)]`; no `std::sync::Mutex` guard across `.await`.
