# SP3: Durable Single-Node Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make crabgresql's data survive restart — swap the in-memory `MemKv` for a durable, crash-recoverable fjall-backed `Kv`, and fold the catalog and per-table rowid allocator into the same durable KV store under a reserved keyspace (fixing the SP2 rowid carry-over). Statements still autocommit; transactions/MVCC are SP4.

**Architecture:** The `kv::Kv` trait evolves to fallible (`Result`-returning) with an atomic `write_batch` primitive. `FjallKv` (pure-Rust fjall LSM, journal-replay recovery) sits behind the trait alongside `MemKv`. System metadata — catalog schemas, per-table sequences, and a global next-table-id — lives in the KV store under a reserved `table-id 0` keyspace; `catalog::Catalog` becomes a stateless typed view over `&dyn Kv`. `SqlEngine` drops its in-memory `HashMap`s; `--data-dir` selects durable vs ephemeral.

**Tech Stack:** Rust 2024, fjall 3.1.5 (pure-Rust LSM, verified C-free — only `lz4_flex`), tempfile (dev). Existing: bytes, thiserror, tokio, tokio-postgres.

**Spec:** `docs/superpowers/specs/2026-06-11-crabgresql-sp3-durable-storage-design.md`

---

## File structure

```
crates/kv/
  Cargo.toml                  # + fjall (normal dep), tempfile (dev)
  src/lib.rs                  # re-export WriteOp, FjallKv
  src/store.rs                # Kv trait (now fallible + write_batch); MemKv
  src/fjall_store.rs          # FjallKv: durable Kv impl over a fjall partition
  src/error.rs                # KvError += Io(String)
  src/key.rs                  # + system-key builders (catalog/seq/meta)
  src/keyenc.rs               # unchanged (reused by system keys)
  src/rowenc.rs               # unchanged
crates/catalog/
  Cargo.toml                  # + kv (catalog now reads/writes KV)
  src/lib.rs                  # Catalog → stateless view over &dyn Kv
  src/serde.rs                # versioned (de)serialization of a table schema
crates/executor/
  src/lib.rs                  # SqlEngine: drop HashMaps; Arc<dyn Kv> + DDL mutex; new()/open(path)
  src/exec.rs                 # catalog view calls; durable sequence; atomic INSERT batch
  src/error.rs                # map KvError/CatalogError → PgError (58030 io, XX000 corrupt)
  tests/durability.rs         # open → write → drop → reopen survives
crates/crabgresql/
  src/main.rs                 # + --data-dir flag → SqlEngine::open
scripts/durable-restart-smoke.sh   # psql: insert, restart binary, select back
.github/workflows/ci.yml      # run the durable-restart smoke
```

Task order (each ends workspace-green): trait change + Result propagation → FjallKv → reserved keys + schema serde (additive) → catalog-view + executor integration → durability tests → binary `--data-dir` + restart smoke → CI/gauntlet.

---

### Task 1: kv trait → fallible + `write_batch`; propagate `Result` through executor

**Files:**
- Modify: `crates/kv/src/store.rs`, `crates/kv/src/error.rs`, `crates/kv/src/lib.rs`
- Modify: `crates/executor/src/error.rs`, `crates/executor/src/exec.rs`, `crates/executor/src/lib.rs`

The trait becomes fallible (durable I/O can fail) and gains an atomic batch
primitive. This breaks `MemKv` and every executor call site; both are updated
here so the workspace stays green. `FjallKv` and durable metadata come later —
this task is still entirely in-memory.

- [ ] **Step 1: Add the `Io` error variant.** In `crates/kv/src/error.rs`:

```rust
//! Errors from the storage layer.

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KvError {
    #[error("corrupt row encoding: {0}")]
    CorruptRow(String),
    #[error("storage I/O error: {0}")]
    Io(String),
}
```

- [ ] **Step 2: Write the failing `write_batch` test.** Append to the `tests`
module in `crates/kv/src/store.rs`:

```rust
    #[test]
    fn write_batch_applies_all_ops() {
        let kv = MemKv::new();
        kv.put(b"keep".to_vec(), b"0".to_vec()).expect("put");
        kv.write_batch(&[
            WriteOp::Put { key: b"a".to_vec(), value: b"1".to_vec() },
            WriteOp::Put { key: b"b".to_vec(), value: b"2".to_vec() },
            WriteOp::Delete { key: b"keep".to_vec() },
        ])
        .expect("batch");
        assert_eq!(kv.get(b"a").expect("get"), Some(b"1".to_vec()));
        assert_eq!(kv.get(b"b").expect("get"), Some(b"2".to_vec()));
        assert_eq!(kv.get(b"keep").expect("get"), None);
    }
```

Also update the existing `put_get_delete` and `scan_prefix_returns_sorted_matches_only`
tests to unwrap the new `Result`s with `.expect("...")` (e.g.
`kv.get(b"a").expect("get")`, `kv.put(...).expect("put")`,
`kv.scan_prefix(b"t/1/").expect("scan")`).

- [ ] **Step 3: Run — expect compile failure.** `cargo test -p kv -- store::` →
FAIL (`WriteOp` and the `Result` signatures don't exist yet).

- [ ] **Step 4: Evolve the trait + MemKv.** Replace the trait and impl in
`crates/kv/src/store.rs` (keep the module doc comment):

```rust
use std::collections::BTreeMap;
use std::sync::RwLock;

use crate::KvError;

/// One mutation in an atomic batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// An ordered byte-key/byte-value store. Synchronous for SP3; the distributed
/// layer will introduce an async, transactional variant behind this boundary.
/// All methods are fallible because a durable backend can hit I/O errors.
pub trait Kv: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError>;
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KvError>;
    fn delete(&self, key: &[u8]) -> Result<(), KvError>;
    /// All (key, value) pairs whose key starts with `prefix`, in key order.
    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, KvError>;
    /// Apply all ops atomically and durably (fsync on a durable backend).
    /// All-or-nothing across a crash.
    fn write_batch(&self, ops: &[WriteOp]) -> Result<(), KvError>;
}

/// In-memory ordered store backed by a BTreeMap. Infallible internally; returns
/// `Ok` to satisfy the fallible trait. Used for tests and the ephemeral default.
#[derive(Default)]
pub struct MemKv {
    map: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl MemKv {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Kv for MemKv {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        Ok(self.map.read().expect("kv lock").get(key).cloned())
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KvError> {
        self.map.write().expect("kv lock").insert(key, value);
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), KvError> {
        self.map.write().expect("kv lock").remove(key);
        Ok(())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, KvError> {
        Ok(self
            .map
            .read()
            .expect("kv lock")
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    fn write_batch(&self, ops: &[WriteOp]) -> Result<(), KvError> {
        // One lock acquisition = atomic against concurrent readers.
        let mut map = self.map.write().expect("kv lock");
        for op in ops {
            match op {
                WriteOp::Put { key, value } => {
                    map.insert(key.clone(), value.clone());
                }
                WriteOp::Delete { key } => {
                    map.remove(key);
                }
            }
        }
        Ok(())
    }
}
```

Make sure the `tests` module has `use super::*;` so `WriteOp` is in scope.

- [ ] **Step 5: Re-export `WriteOp`.** In `crates/kv/src/lib.rs` change the
store re-export to: `pub use store::{Kv, MemKv, WriteOp};`

- [ ] **Step 6: Run kv tests.** `cargo test -p kv` → all pass.

- [ ] **Step 7: Map `KvError` in the executor.** In `crates/executor/src/error.rs`,
the `ExecError::Kv` arm currently maps to `XX000`. Make it distinguish I/O from
corruption:

```rust
            ExecError::Kv(e) => match e {
                kv::KvError::Io(msg) => PgError::error("58030", format!("storage I/O error: {msg}")),
                kv::KvError::CorruptRow(msg) => PgError::error("XX000", format!("corrupt storage: {msg}")),
            },
```

(The `From<KvError> for ExecError` impl already exists; keep it.)

- [ ] **Step 8: Propagate `Result` at executor call sites.** In
`crates/executor/src/exec.rs`:
- INSERT row write: `engine.kv.put(kv::key::row_key(t.id, rowid), kv::rowenc::encode_row(&full))` becomes `engine.kv.put(kv::key::row_key(t.id, rowid), kv::rowenc::encode_row(&full))?;` (the `?` converts `KvError` → `ExecError` via the existing `From`).
- SELECT scan: `engine.kv.scan_prefix(&kv::key::table_prefix(t.id))` becomes `engine.kv.scan_prefix(&kv::key::table_prefix(t.id))?` (it was already inside a `.into_iter()...collect::<Result<_,_>>()?` chain that decoded rows; now `scan_prefix` itself returns `Result`, so add the `?` before `.into_iter()` — restructure to: `let scanned = engine.kv.scan_prefix(&kv::key::table_prefix(t.id))?; scanned.into_iter().map(|(_, v)| kv::rowenc::decode_row(&v)).collect::<Result<_, _>>()?`).

- [ ] **Step 9: Workspace green.** `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings` → all pass (still MemKv, still in-memory catalog). `cargo fmt --all`.

- [ ] **Step 10: Commit.**

```bash
git add crates/kv crates/executor
git commit -m "feat(kv): fallible Kv trait with atomic write_batch; propagate Result through executor"
```

---

### Task 2: `FjallKv` — durable Kv implementation

**Files:**
- Modify: `crates/kv/Cargo.toml`, `crates/kv/src/lib.rs`
- Create: `crates/kv/src/fjall_store.rs`

`FjallKv` wraps a fjall keyspace + one partition, opened at a path. Opening an
existing dir recovers via fjall's journal replay — no bespoke recovery code.

NOTE on the fjall API: fjall 3.1.5's exact method names (Config/open/
open_partition/batch/persist/PersistMode) should be confirmed against
`cargo doc --open -p fjall` or docs.rs/fjall/3.1.5. The reference body below is
the intended shape; bind it to the real API. **The tests are the precise spec** —
iterate the impl until they pass. Map every fjall error to `KvError::Io(e.to_string())`.

- [ ] **Step 1: Add deps.** In `crates/kv/Cargo.toml`:
- `[dependencies]`: `fjall = "3.1.5"`
- `[dev-dependencies]`: `tempfile = "3"`

(Add `tempfile = "3"` to `[workspace.dependencies]` in the root `Cargo.toml` and
reference it as `tempfile.workspace = true` if you prefer workspace-pinning;
either is fine — match the existing convention, which pins versions directly in
`[workspace.dependencies]`.)

- [ ] **Step 2: Write the failing tests.** `crates/kv/src/fjall_store.rs` test
module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::WriteOp;

    fn temp() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    #[test]
    fn put_get_delete_durable() {
        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");
        assert_eq!(kv.get(b"a").expect("get"), None);
        kv.put(b"a".to_vec(), b"1".to_vec()).expect("put");
        assert_eq!(kv.get(b"a").expect("get"), Some(b"1".to_vec()));
        kv.delete(b"a").expect("delete");
        assert_eq!(kv.get(b"a").expect("get"), None);
    }

    #[test]
    fn scan_prefix_ordered_matches_only() {
        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");
        kv.put(b"t/1/b".to_vec(), b"B".to_vec()).expect("put");
        kv.put(b"t/1/a".to_vec(), b"A".to_vec()).expect("put");
        kv.put(b"t/2/a".to_vec(), b"X".to_vec()).expect("put");
        assert_eq!(
            kv.scan_prefix(b"t/1/").expect("scan"),
            vec![
                (b"t/1/a".to_vec(), b"A".to_vec()),
                (b"t/1/b".to_vec(), b"B".to_vec()),
            ]
        );
    }

    #[test]
    fn write_batch_is_atomic() {
        let dir = temp();
        let kv = FjallKv::open(dir.path()).expect("open");
        kv.put(b"keep".to_vec(), b"0".to_vec()).expect("put");
        kv.write_batch(&[
            WriteOp::Put { key: b"a".to_vec(), value: b"1".to_vec() },
            WriteOp::Delete { key: b"keep".to_vec() },
        ])
        .expect("batch");
        assert_eq!(kv.get(b"a").expect("get"), Some(b"1".to_vec()));
        assert_eq!(kv.get(b"keep").expect("get"), None);
    }

    #[test]
    fn data_survives_reopen() {
        let dir = temp();
        {
            let kv = FjallKv::open(dir.path()).expect("open");
            kv.put(b"persist".to_vec(), b"yes".to_vec()).expect("put");
            // kv dropped here — must have fsynced.
        }
        let kv = FjallKv::open(dir.path()).expect("reopen");
        assert_eq!(kv.get(b"persist").expect("get"), Some(b"yes".to_vec()));
    }
}
```

- [ ] **Step 3:** `cargo test -p kv -- fjall_store::` → COMPILE FAIL.

- [ ] **Step 4: Implement** `fjall_store.rs` (reference shape — bind to the real
fjall API):

```rust
//! Durable Kv over a fjall LSM partition. Crash recovery is fjall's journal
//! replay on open; durability is fsync on each commit.

use std::path::Path;

use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};

use crate::{Kv, KvError, WriteOp};

/// Durable key-value store backed by a single fjall partition.
pub struct FjallKv {
    keyspace: Keyspace,
    part: PartitionHandle,
}

impl FjallKv {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, KvError> {
        let keyspace = Config::new(path).open().map_err(io)?;
        let part = keyspace
            .open_partition("data", PartitionCreateOptions::default())
            .map_err(io)?;
        Ok(Self { keyspace, part })
    }

    fn sync(&self) -> Result<(), KvError> {
        self.keyspace.persist(PersistMode::SyncAll).map_err(io)
    }
}

fn io(e: impl std::fmt::Display) -> KvError {
    KvError::Io(e.to_string())
}

impl Kv for FjallKv {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        Ok(self.part.get(key).map_err(io)?.map(|v| v.to_vec()))
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KvError> {
        self.part.insert(key, value).map_err(io)?;
        self.sync()
    }

    fn delete(&self, key: &[u8]) -> Result<(), KvError> {
        self.part.remove(key).map_err(io)?;
        self.sync()
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, KvError> {
        let mut out = Vec::new();
        for item in self.part.prefix(prefix) {
            let (k, v) = item.map_err(io)?;
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }

    fn write_batch(&self, ops: &[WriteOp]) -> Result<(), KvError> {
        let mut batch = self.keyspace.batch();
        for op in ops {
            match op {
                WriteOp::Put { key, value } => batch.insert(&self.part, key, value),
                WriteOp::Delete { key } => batch.remove(&self.part, key),
            }
        }
        batch.commit().map_err(io)?;
        self.sync()
    }
}
```

If the fjall API differs (e.g. `batch.commit()` already persists, or
`PersistMode` lives elsewhere), adapt while preserving: durable single ops,
atomic+durable batch, ordered prefix scan, journal-replay recovery on open.

- [ ] **Step 5: Re-export.** In `crates/kv/src/lib.rs` add `pub mod fjall_store;`
and `pub use fjall_store::FjallKv;`.

- [ ] **Step 6: Run + purity gates.** `cargo test -p kv` → all pass (Mem + Fjall).
Then prove fjall didn't drag in C: `./scripts/check-no-native.sh` (kv is in the
binary tree via executor, so fjall is now in the shipped tree — must print OK)
and `cargo deny check bans licenses` (bans ok; if a fjall transitive needs a
license added to `deny.toml`'s allow list — e.g. a permissive one — add that
specific license and note it; do NOT relax the C/`-sys` bans). `cargo fmt --all && cargo clippy -p kv --all-targets -- -D warnings`.

- [ ] **Step 7: Commit.**

```bash
git add crates/kv Cargo.toml Cargo.lock deny.toml
git commit -m "feat(kv): FjallKv durable store with journal-replay recovery"
```

---

### Task 3: Reserved system keys + table-schema serialization (additive)

**Files:**
- Modify: `crates/kv/src/key.rs`
- Modify: `crates/catalog/Cargo.toml` (add `kv`), `crates/catalog/src/lib.rs`
- Create: `crates/catalog/src/serde.rs`

Pure additions, fully unit-tested, not yet wired into the stateful `Catalog`
(that's Task 4). This gives Task 4 its building blocks.

- [ ] **Step 1: System-key tests.** Append to the `tests` module in
`crates/kv/src/key.rs`:

```rust
    #[test]
    fn system_keys_are_distinct_and_under_table_zero() {
        let cat = catalog_key("users");
        let seq = seq_key(7);
        let meta = meta_next_table_id_key();
        // All start with the reserved table-id 0 prefix.
        let zero = {
            let mut k = Vec::new();
            crate::keyenc::put_u32(&mut k, 0);
            k
        };
        assert!(cat.starts_with(&zero));
        assert!(seq.starts_with(&zero));
        assert!(meta.starts_with(&zero));
        // Distinct namespaces.
        assert_ne!(cat, seq);
        assert_ne!(seq, meta);
        assert_ne!(catalog_key("a"), catalog_key("b"));
        assert_ne!(seq_key(7), seq_key(8));
    }

    #[test]
    fn system_keys_do_not_collide_with_user_rows() {
        // User rows use table_id >= 1; system keys use table_id 0.
        assert!(!catalog_key("t").starts_with(&table_prefix(1)));
        assert!(!seq_key(1).starts_with(&table_prefix(1)));
    }
```

- [ ] **Step 2: Implement system-key builders.** Append to `crates/kv/src/key.rs`:

```rust
/// Reserved table id for system metadata (catalog, sequences, global meta).
pub const SYSTEM_TABLE_ID: u32 = 0;

fn system_prefix(tag: &str) -> Vec<u8> {
    let mut k = Vec::new();
    put_u32(&mut k, SYSTEM_TABLE_ID);
    k.extend_from_slice(tag.as_bytes());
    k.push(b'/');
    k
}

/// Key for a table's stored schema: `/0/catalog/<name>`.
pub fn catalog_key(table_name: &str) -> Vec<u8> {
    let mut k = system_prefix("catalog");
    k.extend_from_slice(table_name.as_bytes());
    k
}

/// Key for a table's next-rowid sequence: `/0/seq/<table_id>`.
pub fn seq_key(table_id: u32) -> Vec<u8> {
    let mut k = system_prefix("seq");
    put_u32(&mut k, table_id);
    k
}

/// Key for the global next-table-id counter: `/0/meta/next_table_id`.
pub fn meta_next_table_id_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"next_table_id");
    k
}
```

(The `catalog`/`seq`/`meta` tags begin with ASCII letters, so a user prefix scan
for `table_prefix(0)` would never be issued — table id 0 is reserved and never a
user table. The `tests` module already imports `super::*`.)

- [ ] **Step 3: Run.** `cargo test -p kv -- key::` → all pass.

- [ ] **Step 4: Schema-serialization tests.** `crates/catalog/src/serde.rs` test
module (the schema value stored at `catalog_key(name)`):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::Column;
    use pgtypes::ColumnType;

    #[test]
    fn roundtrip_schema() {
        let table_id = 42u32;
        let columns = vec![
            Column { name: "id".into(), ty: ColumnType::Int4 },
            Column { name: "name".into(), ty: ColumnType::Text },
            Column { name: "ok".into(), ty: ColumnType::Bool },
            Column { name: "big".into(), ty: ColumnType::Int8 },
        ];
        let bytes = serialize_schema(table_id, &columns);
        let (id, cols) = deserialize_schema(&bytes).expect("decode");
        assert_eq!(id, table_id);
        assert_eq!(cols, columns);
    }

    #[test]
    fn unknown_version_errors() {
        assert!(deserialize_schema(&[99, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn truncated_errors_not_panics() {
        assert!(deserialize_schema(&[SCHEMA_VERSION, 0, 0]).is_err());
    }
}
```

- [ ] **Step 5: Implement** `crates/catalog/src/serde.rs`:

```rust
//! Versioned (de)serialization of a table schema — the value stored under
//! `kv::key::catalog_key(name)`. Format: version byte, table_id (u32 BE),
//! column count (u32 BE), then per column: u32 name length, name bytes, type tag.

use kv::KvError;
use pgtypes::ColumnType;

use crate::Column;

/// Current schema-value format version.
pub const SCHEMA_VERSION: u8 = 1;

mod type_tag {
    pub const BOOL: u8 = 0;
    pub const INT4: u8 = 1;
    pub const INT8: u8 = 2;
    pub const TEXT: u8 = 3;
}

fn tag_of(ty: ColumnType) -> u8 {
    match ty {
        ColumnType::Bool => type_tag::BOOL,
        ColumnType::Int4 => type_tag::INT4,
        ColumnType::Int8 => type_tag::INT8,
        ColumnType::Text => type_tag::TEXT,
    }
}

fn type_of(tag: u8) -> Result<ColumnType, KvError> {
    Ok(match tag {
        type_tag::BOOL => ColumnType::Bool,
        type_tag::INT4 => ColumnType::Int4,
        type_tag::INT8 => ColumnType::Int8,
        type_tag::TEXT => ColumnType::Text,
        other => return Err(KvError::CorruptRow(format!("unknown column type tag {other}"))),
    })
}

pub fn serialize_schema(table_id: u32, columns: &[Column]) -> Vec<u8> {
    let mut out = vec![SCHEMA_VERSION];
    out.extend_from_slice(&table_id.to_be_bytes());
    out.extend_from_slice(&(columns.len() as u32).to_be_bytes());
    for c in columns {
        out.extend_from_slice(&(c.name.len() as u32).to_be_bytes());
        out.extend_from_slice(c.name.as_bytes());
        out.push(tag_of(c.ty));
    }
    out
}

pub fn deserialize_schema(bytes: &[u8]) -> Result<(u32, Vec<Column>), KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if version != SCHEMA_VERSION {
        return Err(KvError::CorruptRow(format!("unknown schema version {version}")));
    }
    let table_id = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4"));
    let ncols = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4")) as usize;
    let mut columns = Vec::with_capacity(ncols.min(1024));
    for _ in 0..ncols {
        let nlen = u32::from_be_bytes(take_n(&mut cur, 4)?.try_into().expect("4")) as usize;
        let name = String::from_utf8(take_n(&mut cur, nlen)?.to_vec())
            .map_err(|_| KvError::CorruptRow("column name is not UTF-8".into()))?;
        let ty = type_of(take_u8(&mut cur)?)?;
        columns.push(Column { name, ty });
    }
    Ok((table_id, columns))
}

fn take_u8(cur: &mut &[u8]) -> Result<u8, KvError> {
    let (h, rest) = cur.split_first().ok_or_else(|| KvError::CorruptRow("truncated schema".into()))?;
    *cur = rest;
    Ok(*h)
}

fn take_n<'a>(cur: &mut &'a [u8], n: usize) -> Result<&'a [u8], KvError> {
    if cur.len() < n {
        return Err(KvError::CorruptRow("truncated schema field".into()));
    }
    let (h, rest) = cur.split_at(n);
    *cur = rest;
    Ok(h)
}
```

- [ ] **Step 6: Wire the module + dep.** In `crates/catalog/Cargo.toml`
`[dependencies]` add `kv.workspace = true`. In `crates/catalog/src/lib.rs` add
`pub mod serde;` (above the existing items). The existing stateful `Catalog`
stays untouched in this task — `serde` is additive.

- [ ] **Step 7:** `cargo test -p kv -p catalog` → all pass. `cargo fmt --all && cargo clippy -p kv -p catalog --all-targets -- -D warnings`.

- [ ] **Step 8: Commit.**

```bash
git add crates/kv crates/catalog Cargo.toml Cargo.lock
git commit -m "feat: reserved system keys and versioned table-schema serialization"
```

---

### Task 4: Catalog → KV view + executor durable integration

**Files:**
- Modify: `crates/catalog/src/lib.rs`
- Modify: `crates/executor/src/lib.rs`, `crates/executor/src/exec.rs`, `crates/executor/src/error.rs`

The big refactor. `Catalog` stops owning a `HashMap` and becomes stateless
functions over `&dyn Kv` (using Task 3's keys + serde). `SqlEngine` drops its
`rowids` and in-memory catalog; sequences become durable KV reads/writes; INSERT
commits rows + the sequence bump in one atomic `write_batch`; DDL is serialized
behind a process mutex. Ends workspace-green on `MemKv` (existing executor tests
pass, now exercising the durable code paths in memory).

- [ ] **Step 1: Rewrite `catalog::Catalog` as a view.** Replace the stateful
struct in `crates/catalog/src/lib.rs`. Keep `Column`, `Table`, `TableId`,
`CatalogError`, `Table::column_index` exactly as they are; replace the `Catalog`
struct + its `new`/`create_table`/`drop_table`/`get_table` with free functions
(or associated functions on a unit `Catalog`) taking `&dyn Kv`. Add a
`Storage(KvError)` error variant.

```rust
use kv::{Kv, KvError, WriteOp, key};

use crate::serde::{deserialize_schema, serialize_schema};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    #[error("relation \"{0}\" already exists")]
    DuplicateTable(String),
    #[error("relation \"{0}\" does not exist")]
    UndefinedTable(String),
    #[error("column \"{0}\" does not exist")]
    UndefinedColumn(String),
    #[error("catalog storage error: {0}")]
    Storage(#[from] KvError),
}

impl CatalogError {
    pub fn sqlstate(&self) -> &'static str {
        match self {
            CatalogError::DuplicateTable(_) => "42P07",
            CatalogError::UndefinedTable(_) => "42P01",
            CatalogError::UndefinedColumn(_) => "42703",
            CatalogError::Storage(KvError::Io(_)) => "58030",
            CatalogError::Storage(KvError::CorruptRow(_)) => "XX000",
        }
    }
}

/// Create a table: allocate a TableId, persist the schema, init the sequence —
/// all in one atomic batch. Caller serializes concurrent DDL.
pub fn create_table(kv: &dyn Kv, name: &str, columns: Vec<Column>) -> Result<TableId, CatalogError> {
    if kv.get(&key::catalog_key(name))?.is_some() {
        return Err(CatalogError::DuplicateTable(name.to_string()));
    }
    let next = read_next_table_id(kv)?;
    let batch = vec![
        WriteOp::Put {
            key: key::catalog_key(name),
            value: serialize_schema(next, &columns),
        },
        WriteOp::Put {
            key: key::seq_key(next),
            value: 1u64.to_be_bytes().to_vec(),
        },
        WriteOp::Put {
            key: key::meta_next_table_id_key(),
            value: (next + 1).to_be_bytes().to_vec(),
        },
    ];
    kv.write_batch(&batch)?;
    Ok(next)
}

/// Look up a table by name.
pub fn get_table(kv: &dyn Kv, name: &str) -> Result<Table, CatalogError> {
    let bytes = kv
        .get(&key::catalog_key(name))?
        .ok_or_else(|| CatalogError::UndefinedTable(name.to_string()))?;
    let (id, columns) = deserialize_schema(&bytes)?;
    Ok(Table { id, name: name.to_string(), columns })
}

/// Drop a table: delete the catalog entry, the sequence, and all its rows — one
/// atomic batch.
pub fn drop_table(kv: &dyn Kv, name: &str) -> Result<(), CatalogError> {
    let table = get_table(kv, name)?;
    let mut ops = vec![
        WriteOp::Delete { key: key::catalog_key(name) },
        WriteOp::Delete { key: key::seq_key(table.id) },
    ];
    for (row_key, _) in kv.scan_prefix(&key::table_prefix(table.id))? {
        ops.push(WriteOp::Delete { key: row_key });
    }
    kv.write_batch(&ops)?;
    Ok(())
}

/// Read the next TableId (defaults to 1 when the meta key is absent).
fn read_next_table_id(kv: &dyn Kv) -> Result<TableId, CatalogError> {
    match kv.get(&key::meta_next_table_id_key())? {
        Some(b) => {
            let arr: [u8; 4] = b.as_slice().try_into()
                .map_err(|_| KvError::CorruptRow("next_table_id is not u32".into()))?;
            Ok(u32::from_be_bytes(arr))
        }
        None => Ok(1),
    }
}
```

Remove the old `Catalog` struct, `Inner`, `Default`/`new`, and the `RwLock`/
`HashMap` imports. Add `pub mod serde;` is already present from Task 3.

Note: `next + 1` could in theory overflow `u32` after ~4 billion tables — out of
scope for SP3; a saturating/`checked_add` guard returning a CatalogError is a
reasonable belt-and-suspenders if clippy or a reviewer flags it, but not
required.

- [ ] **Step 2: Update the catalog tests to pass a Kv.** The existing catalog
tests call `Catalog::new()` etc. Rewrite them to drive the free functions
against BOTH backends. Replace the catalog `tests` module with:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use kv::{FjallKv, MemKv};
    use pgtypes::ColumnType;

    fn cols() -> Vec<Column> {
        vec![
            Column { name: "id".into(), ty: ColumnType::Int4 },
            Column { name: "name".into(), ty: ColumnType::Text },
        ]
    }

    fn check_crud(kv: &dyn Kv) {
        let id = create_table(kv, "t", cols()).expect("create");
        let t = get_table(kv, "t").expect("lookup");
        assert_eq!(t.id, id);
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.column_index("name"), Some(1));
        // Duplicate → 42P07.
        assert_eq!(create_table(kv, "t", cols()).expect_err("dup").sqlstate(), "42P07");
        // Distinct ids.
        let id2 = create_table(kv, "u", cols()).expect("create u");
        assert_ne!(id, id2);
        // Drop → gone.
        drop_table(kv, "t").expect("drop");
        assert_eq!(get_table(kv, "t").expect_err("gone").sqlstate(), "42P01");
        // Drop missing → 42P01.
        assert_eq!(drop_table(kv, "nope").expect_err("missing").sqlstate(), "42P01");
    }

    #[test]
    fn crud_on_memkv() {
        check_crud(&MemKv::new());
    }

    #[test]
    fn crud_on_fjallkv() {
        let dir = tempfile::tempdir().expect("tempdir");
        check_crud(&FjallKv::open(dir.path()).expect("open"));
    }
}
```

Add `tempfile = "3"` to `crates/catalog/Cargo.toml` `[dev-dependencies]`.

- [ ] **Step 3: Run catalog tests.** `cargo test -p catalog` → both backends pass.
(Executor won't compile yet — that's the next steps.)

- [ ] **Step 4: Rework `SqlEngine`.** In `crates/executor/src/lib.rs`, drop the
catalog HashMap and rowids; hold the Kv + a DDL mutex:

```rust
use std::path::Path;
use std::sync::{Arc, Mutex};

use catalog::TableId;
use kv::{FjallKv, Kv, MemKv, WriteOp};
use pgwire::engine::{Engine, FieldDescription, QueryResult};
use pgwire::error::PgError;

pub use error::ExecError;

/// The SQL engine over a durable (or in-memory) KV store. Catalog and sequences
/// live in the KV store; the DDL mutex serializes catalog mutations.
pub struct SqlEngine {
    pub(crate) kv: Arc<dyn Kv>,
    pub(crate) ddl_lock: Mutex<()>,
}

impl Default for SqlEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlEngine {
    /// Ephemeral in-memory engine (tests, default when no --data-dir).
    pub fn new() -> Self {
        Self::with_kv(Arc::new(MemKv::new()))
    }

    /// Durable engine backed by a fjall store at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ExecError> {
        Ok(Self::with_kv(Arc::new(FjallKv::open(path)?)))
    }

    pub fn with_kv(kv: Arc<dyn Kv>) -> Self {
        Self { kv, ddl_lock: Mutex::new(()) }
    }

    /// Read a table's durable next-rowid (1 if unset).
    pub(crate) fn read_seq(&self, table: TableId) -> Result<u64, ExecError> {
        match self.kv.get(&kv::key::seq_key(table))? {
            Some(b) => {
                let arr: [u8; 8] = b.as_slice().try_into()
                    .map_err(|_| kv::KvError::CorruptRow("sequence is not u64".into()))?;
                Ok(u64::from_be_bytes(arr))
            }
            None => Ok(1),
        }
    }
}
```

(`ExecError` needs `From<kv::KvError>` — it already exists. `SqlEngine::open`
returns `ExecError`; the `?` on `FjallKv::open` converts `KvError`→`ExecError`.)

- [ ] **Step 5: Update `exec.rs` call sites.** Replace the catalog/sequence/INSERT
logic:

CREATE TABLE arm — serialize DDL and call the view:
```rust
        Statement::CreateTable { name, columns } => {
            let cols = columns.iter().map(|c| catalog::Column { name: c.name.clone(), ty: c.ty }).collect();
            let _guard = engine.ddl_lock.lock().expect("ddl lock");
            catalog::create_table(&*engine.kv, name, cols)?;
            Ok(QueryResult::Command { tag: "CREATE TABLE".into() })
        }
```

DROP TABLE arm:
```rust
        Statement::DropTable { name } => {
            let _guard = engine.ddl_lock.lock().expect("ddl lock");
            catalog::drop_table(&*engine.kv, name)?;
            Ok(QueryResult::Command { tag: "DROP TABLE".into() })
        }
```

INSERT arm — read the sequence once, build one atomic batch of rows + the bumped
sequence:
```rust
        Statement::Insert { table, columns, rows } => {
            let t = catalog::get_table(&*engine.kv, table)?;
            let target_idx: Vec<usize> = match columns {
                Some(cols) => cols.iter()
                    .map(|c| t.column_index(c).ok_or_else(|| ExecError::UndefinedColumn(c.clone())))
                    .collect::<Result<_, _>>()?,
                None => (0..t.columns.len()).collect(),
            };
            let mut rowid = engine.read_seq(t.id)?;
            let mut ops: Vec<WriteOp> = Vec::new();
            for row_exprs in rows {
                if row_exprs.len() != target_idx.len() {
                    return Err(ExecError::TypeMismatch(
                        "INSERT has the wrong number of expressions for the target columns".into(),
                    ));
                }
                let mut full = vec![pgtypes::Datum::Null; t.columns.len()];
                for (slot, expr) in target_idx.iter().zip(row_exprs.iter()) {
                    let v = crate::eval::eval(expr, None, &[])?;
                    full[*slot] = coerce(v, t.columns[*slot].ty)?;
                }
                ops.push(WriteOp::Put {
                    key: kv::key::row_key(t.id, rowid),
                    value: kv::rowenc::encode_row(&full),
                });
                rowid += 1;
            }
            let n = ops.len() as u64;
            ops.push(WriteOp::Put { key: kv::key::seq_key(t.id), value: rowid.to_be_bytes().to_vec() });
            engine.kv.write_batch(&ops)?;
            Ok(QueryResult::Command { tag: format!("INSERT 0 {n}") })
        }
```

SELECT/describe — replace `engine.catalog.get_table(name)` with
`catalog::get_table(&*engine.kv, name)` at both call sites (exec_select and
describe). Import `WriteOp` and keep `coerce` as-is. Remove the now-unused
`engine.next_rowid` method references (the method is deleted).

- [ ] **Step 6: Map `CatalogError` in `error.rs`.** The `ExecError::Catalog` arm
already maps via `e.sqlstate()`; since `CatalogError` now has a `Storage` variant
with `sqlstate()` returning 58030/XX000, no change is needed — but confirm the
`ExecError::Catalog(e) => PgError::error(e.sqlstate(), e.to_string())` arm still
compiles and covers it.

- [ ] **Step 7: Workspace green.** `cargo test --workspace` → all pass (executor's
existing 24+ tests now run against the durable code paths on MemKv; e2e + pgwire
+ conformance unaffected). `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 8: Commit.**

```bash
git add crates/catalog crates/executor Cargo.toml Cargo.lock
git commit -m "feat: catalog as KV view; durable sequences; atomic INSERT batch; DDL serialization"
```

---

### Task 5: Durability / recovery tests

**Files:**
- Create: `crates/executor/tests/durability.rs`

- [ ] **Step 1: Write the recovery tests.** `crates/executor/tests/durability.rs`:

```rust
//! Open a durable engine, write, drop it, reopen, and assert everything
//! survived — including the rowid allocator (the SP2 carry-over fix).

use executor::SqlEngine;
use pgwire::engine::{Cell, Engine, QueryResult};

fn text(cell: &Option<Cell>) -> Option<String> {
    cell.as_ref().map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}

async fn rows(engine: &SqlEngine, sql: &str) -> Vec<Vec<Option<Cell>>> {
    let mut results = engine.simple_query(sql).await.expect("query");
    match results.remove(0) {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[tokio::test]
async fn data_schema_and_rowid_survive_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        engine.simple_query("CREATE TABLE t (id int4, name text)").await.expect("create");
        engine.simple_query("INSERT INTO t VALUES (1,'a'),(2,'b')").await.expect("insert");
        // engine dropped here — writes were fsynced per statement.
    }

    let engine = SqlEngine::open(dir.path()).expect("reopen");
    // Rows + schema survived.
    let got = rows(&engine, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(got.iter().map(|r| text(&r[0])).collect::<Vec<_>>(),
        vec![Some("a".into()), Some("b".into())]);
    // The rowid allocator survived: a new insert does NOT collide with id 1/2.
    // (rowids are the hidden key, not the id column; insert two more and confirm
    // all four rows are present and distinct.)
    engine.simple_query("INSERT INTO t VALUES (3,'c')").await.expect("insert after reopen");
    let after = rows(&engine, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(after.len(), 3, "all rows present, no overwrite from a reset rowid");
    assert_eq!(after.iter().map(|r| text(&r[0])).collect::<Vec<_>>(),
        vec![Some("a".into()), Some("b".into()), Some("c".into())]);
}

#[tokio::test]
async fn drop_and_recreate_survive_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        engine.simple_query("CREATE TABLE t (id int4)").await.expect("create");
        engine.simple_query("INSERT INTO t VALUES (1)").await.expect("insert");
        engine.simple_query("DROP TABLE t").await.expect("drop");
        engine.simple_query("CREATE TABLE t (id int4)").await.expect("recreate");
    }
    let engine = SqlEngine::open(dir.path()).expect("reopen");
    // The recreated (empty) table survived; the dropped rows did not resurrect.
    let got = rows(&engine, "SELECT id FROM t").await;
    assert!(got.is_empty(), "dropped rows must not survive; recreated table is empty");
}
```

Add `tempfile = "3"` to `crates/executor/Cargo.toml` `[dev-dependencies]`.

- [ ] **Step 2: Run.** `cargo test -p executor --test durability` → both pass. If
`data_schema_and_rowid_survive_reopen` fails because rows were lost, the per-op
fsync in `FjallKv` (or batch persist) isn't durable on drop — fix `FjallKv`'s
`sync()`. If it fails because the rowid reset and overwrote a row, the sequence
isn't being read/persisted — fix `read_seq` / the INSERT batch.

- [ ] **Step 3:** `cargo fmt --all && cargo clippy -p executor --all-targets -- -D warnings`, then commit:

```bash
git add crates/executor Cargo.toml Cargo.lock
git commit -m "test(executor): durability and recovery across reopen"
```

---

### Task 6: Binary `--data-dir` + durable-restart smoke

**Files:**
- Modify: `crates/crabgresql/src/main.rs`
- Create: `scripts/durable-restart-smoke.sh`

- [ ] **Step 1: Add `--data-dir`.** In `crates/crabgresql/src/main.rs`, add to
`Args`:

```rust
    /// Directory for durable storage. Absent → ephemeral in-memory engine.
    #[arg(long)]
    data_dir: Option<std::path::PathBuf>,
```

Build the engine accordingly (replace the `Arc::new(SqlEngine::new())` construction):

```rust
    let engine = match &args.data_dir {
        Some(dir) => Arc::new(SqlEngine::open(dir).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("opening data dir: {e:?}"))
        })?),
        None => Arc::new(SqlEngine::new()),
    };
```

Pass `engine` into the `serve_tls`/`serve` call. (`SqlEngine::open` returns
`ExecError`; map it to `io::Error` for `main`'s `io::Result` return, or adjust
main's return type — keep it `std::io::Result<()>` and map as shown.)

- [ ] **Step 2: Verify it builds + ephemeral still works.** `cargo build -p crabgresql && ./scripts/psql-smoke.sh` → all three legs PASS (no `--data-dir` → ephemeral, unchanged).

- [ ] **Step 3: Durable-restart smoke.** `scripts/durable-restart-smoke.sh`
(chmod +x):

```bash
#!/usr/bin/env bash
# Durability across a real binary restart: insert, kill, restart on the same
# data dir, select the data back. Mirrors psql-smoke.sh's structure.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v psql >/dev/null; then
    echo "SKIP: psql not installed"
    exit 0
fi

PORT="${1:-54337}"
DATA_DIR="$(mktemp -d)"
trap 'rm -rf "$DATA_DIR"' EXIT
cargo build -p crabgresql

start() {
    ./target/debug/crabgresql --listen "127.0.0.1:${PORT}" --data-dir "$DATA_DIR" &
    echo $!
}
ready() {
    for _ in $(seq 20); do
        psql "host=127.0.0.1 port=${PORT} user=crab dbname=crab sslmode=prefer" -tAc 'SELECT 1' >/dev/null 2>&1 && return 0
        sleep 0.3
    done
    return 1
}

# First boot: create + insert.
PID=$(start); ready || { echo "FAIL: first boot not ready" >&2; kill "$PID"; exit 1; }
psql "host=127.0.0.1 port=${PORT} user=crab dbname=crab sslmode=prefer" -c \
    "CREATE TABLE t (id int4, name text); INSERT INTO t VALUES (1,'durable');"
kill "$PID"; wait "$PID" 2>/dev/null || true

# Second boot on the same data dir: the row must still be there.
PID=$(start); ready || { echo "FAIL: second boot not ready" >&2; kill "$PID"; exit 1; }
trap 'kill "$PID" 2>/dev/null || true; rm -rf "$DATA_DIR"' EXIT
out=$(psql "host=127.0.0.1 port=${PORT} user=crab dbname=crab sslmode=prefer" -tAc \
    "SELECT name FROM t WHERE id = 1")
if [ "$out" = "durable" ]; then
    echo "PASS: data survived restart -> ${out}"
else
    echo "FAIL: expected 'durable', got '${out}'" >&2
    exit 1
fi
```

- [ ] **Step 4: Run it.** `./scripts/durable-restart-smoke.sh` → `PASS: data
survived restart -> durable` (or `SKIP` if psql absent). If the second boot can't
read the row, durability is broken — debug `FjallKv` before proceeding.

- [ ] **Step 5: Commit.**

```bash
git add crates/crabgresql scripts/durable-restart-smoke.sh
git commit -m "feat(crabgresql): --data-dir durable storage; restart smoke test"
```

---

### Task 7: CI + final gauntlet

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add the durable-restart smoke to CI.** In the `conformance` job
(which already installs psql), add a step after the existing psql-smoke step:

```yaml
      - name: Durable restart smoke
        run: ./scripts/durable-restart-smoke.sh
```

- [ ] **Step 2: Validate YAML.** `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"` → no error.

- [ ] **Step 3: Full local gauntlet** — report each:
```
cargo test --workspace
cargo test -p pgparser --features oracle
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
./scripts/check-no-native.sh
cargo deny check bans licenses
./scripts/psql-smoke.sh
./scripts/durable-restart-smoke.sh
```
All green. `check-no-native.sh` now also proves fjall kept the shipped tree
pure Rust.

- [ ] **Step 4: Commit.**

```bash
git add .github
git commit -m "ci: durable restart smoke test"
```

---

## Success criteria traceability (spec → tasks)

| Spec criterion | Task(s) |
|---|---|
| `--data-dir`: data survives binary restart (psql/tokio-postgres) | 6 (+5 at engine level) |
| Durable rowid allocator — no collision after restart (carry-over fixed) | 4, 5 |
| Catalog + sequences persist in KV; DROP cleanup survives | 3, 4, 5 |
| Statement writes atomic on crash (write_batch all-or-nothing) | 1 (batch), 2 (fjall), 4 (INSERT batch) |
| FjallKv and MemKv pass the same kv + catalog suites | 2, 4 |
| CI gates green incl. pure-Rust shipped tree with fjall | 2, 7 |

## Notes for the implementer

- The fjall API names in Task 2 are a reference shape — confirm against
  `cargo doc -p fjall` / docs.rs and adapt; the tests are the spec.
- Every task ends `cargo test --workspace` green except where a step explicitly
  scopes to one crate before the dependent crate is updated in the same task.
- Durability default is fsync-per-statement (correctness over throughput); a
  configurable fsync policy is out of scope (SP4+).
- Tracked-out: transactions/MVCC (SP4 — `write_batch` is its seam),
  UPDATE/DELETE, pg_catalog SQL views, configurable durability. SP2's unrelated
  carry-overs (oids duplication, splitter Latin-1 corner) stay deferred.
