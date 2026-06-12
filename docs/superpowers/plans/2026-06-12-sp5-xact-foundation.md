# SP5: PG-faithful MVCC visibility foundation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace SP4's commit-timestamp MVCC with PostgreSQL's real machinery — per-transaction xids, a clog (commit-status log), xmin/xmax tuple headers, and xid-list snapshots `(xmin, xmax, xip[])` resolved by `satisfies_mvcc` — with uncommitted versions on disk, while writers stay serialized (so behavior is identical and all 212 existing tests stay green).

**Architecture:** Build the new pieces additively first — `mvcc::clog`, `mvcc::xid`, xid-keyed tuple encoding, `mvcc::visibility::{Snapshot, satisfies_mvcc}`, and an `executor::ProcArray` running-transaction registry — each green on its own. Then one **cutover** task rewrites the executor to write-through to disk (own-xid read-your-writes, no in-memory write-set), acquire a transaction-scoped async writer lock, and resolve reads via `satisfies_mvcc`. Finally remove the dead SP4 code and run the gauntlet.

**Tech Stack:** Rust 2024. Crates touched: `kv` (key helpers), `mvcc` (clog/xid/tuple/visibility), `executor` (ProcArray, session, exec). New dep: `tokio` already in the tree (for `tokio::sync::Mutex`).

**Spec:** `docs/superpowers/specs/2026-06-12-crabgresql-sp5-xact-foundation-design.md`

---

## File structure

```
crates/kv/src/key.rs            # + next_xid_key(), clog_key(xid)
crates/mvcc/
  src/lib.rs                    # re-exports (swap at cutover/cleanup)
  src/clog.rs                   # NEW: XidStatus, get(), put_op()
  src/xid.rs                    # NEW: Xid alias, INVALID_XID
  src/version.rs                # + version_key_xid/xid_of_key/encode_tuple/decode_tuple (SP4 fns removed at cleanup)
  src/visibility.rs             # NEW: Snapshot{xmin,xmax,xip}, satisfies_mvcc
  src/snapshot.rs               # SP4 Snapshot(u64)/visible_version — removed at cleanup
crates/executor/
  src/procarray.rs              # NEW: ProcArray (running-txn registry, snapshots, xid alloc)
  src/lib.rs                    # SqlEngine gains procarray; write_lock -> tokio::sync::Mutex
  src/session.rs                # async run_one; TxnCtx{xid,snapshot,writer_guard}; clog commit/abort; no write-set
  src/exec.rs                   # write-through execute_write + execute_read; scan_live via satisfies_mvcc
  tests/recovery.rs             # NEW: lazy crash recovery (in-progress -> invisible)
```

Task order (each ends workspace-green): clog + kv keys → xid + tuple encoding → Snapshot + satisfies_mvcc → ProcArray → **cutover** (engine/session/exec write-through) → recovery + durability tests → remove dead SP4 code → gauntlet + traceability.

---

### Task 1: kv key helpers + `mvcc::clog`

**Files:**
- Modify: `crates/kv/src/key.rs`
- Create: `crates/mvcc/src/clog.rs`
- Modify: `crates/mvcc/src/lib.rs` (add `pub mod clog;`)

Additive: a durable next-xid key, per-xid clog keys, and the clog read/write API. Nothing else uses these yet.

- [ ] **Step 1: kv key helpers.** In `crates/kv/src/key.rs`, after `commit_ts_key()`, add:

```rust
/// Key for the global next-transaction-id counter: `/0/meta/next_xid`.
pub fn next_xid_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"next_xid");
    k
}

/// Key for a transaction's commit-status-log entry: `/0/clog/<xid>`.
pub fn clog_key(xid: u64) -> Vec<u8> {
    let mut k = system_prefix("clog");
    k.extend_from_slice(&xid.to_be_bytes());
    k
}
```

- [ ] **Step 2: kv key tests.** In the `key.rs` test module add:

```rust
    #[test]
    fn xid_and_clog_keys_are_under_table_zero_and_distinct() {
        let zero = {
            let mut k = Vec::new();
            crate::keyenc::put_u32(&mut k, 0);
            k
        };
        assert!(next_xid_key().starts_with(&zero));
        assert!(clog_key(5).starts_with(&zero));
        assert_ne!(clog_key(5), clog_key(6));
        assert_ne!(next_xid_key(), meta_next_table_id_key());
        // clog keys sort by xid (order-preserving big-endian suffix).
        assert!(clog_key(5) < clog_key(6));
    }
```

Run: `cargo test -p kv key` → PASS.

- [ ] **Step 3: clog module.** Create `crates/mvcc/src/clog.rs`:

```rust
//! Commit-status log — PostgreSQL's `pg_xact`. Maps each transaction id to its
//! final outcome; the authority on whether a writer committed. An ABSENT entry
//! means the xid recorded no outcome: it is in-progress while the transaction
//! runs, and aborted-equivalent after a crash (it is then in no live snapshot).

use kv::{Kv, KvError, WriteOp};

/// A transaction's recorded outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XidStatus {
    InProgress,
    Committed,
    Aborted,
}

const S_IN_PROGRESS: u8 = 0;
const S_COMMITTED: u8 = 1;
const S_ABORTED: u8 = 2;

/// Read an xid's status. An absent entry is treated as `InProgress`
/// (aborted-equivalent once the xid is in no live snapshot — see recovery).
pub fn get(kv: &dyn Kv, xid: u64) -> Result<XidStatus, KvError> {
    match kv.get(&kv::key::clog_key(xid))? {
        None => Ok(XidStatus::InProgress),
        Some(b) => match b.first() {
            Some(&S_COMMITTED) => Ok(XidStatus::Committed),
            Some(&S_ABORTED) => Ok(XidStatus::Aborted),
            Some(&S_IN_PROGRESS) => Ok(XidStatus::InProgress),
            _ => Err(KvError::CorruptRow("bad clog status byte".into())),
        },
    }
}

/// A write-batch op recording an xid's final status.
pub fn put_op(xid: u64, status: XidStatus) -> WriteOp {
    let byte = match status {
        XidStatus::InProgress => S_IN_PROGRESS,
        XidStatus::Committed => S_COMMITTED,
        XidStatus::Aborted => S_ABORTED,
    };
    WriteOp::Put {
        key: kv::key::clog_key(xid),
        value: vec![byte],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv::MemKv;

    #[test]
    fn absent_entry_is_in_progress() {
        let kv = MemKv::new();
        assert_eq!(get(&kv, 7).expect("get"), XidStatus::InProgress);
    }

    #[test]
    fn committed_and_aborted_roundtrip() {
        let kv = MemKv::new();
        kv.write_batch(&[put_op(7, XidStatus::Committed)]).expect("put");
        kv.write_batch(&[put_op(8, XidStatus::Aborted)]).expect("put");
        assert_eq!(get(&kv, 7).expect("get"), XidStatus::Committed);
        assert_eq!(get(&kv, 8).expect("get"), XidStatus::Aborted);
    }

    #[test]
    fn corrupt_status_byte_errors() {
        let kv = MemKv::new();
        kv.write_batch(&[kv::WriteOp::Put { key: kv::key::clog_key(9), value: vec![99] }])
            .expect("put");
        assert!(get(&kv, 9).is_err());
    }
}
```

- [ ] **Step 4: register module.** In `crates/mvcc/src/lib.rs` add `pub mod clog;` (leave the existing `pub use` lines alone for now).

- [ ] **Step 5: verify + commit.**

Run: `cargo test -p kv -p mvcc && cargo clippy -p kv -p mvcc --all-targets -- -D warnings`
Expected: green.

```bash
git add crates/kv/src/key.rs crates/mvcc/src/clog.rs crates/mvcc/src/lib.rs
git commit -m "feat(mvcc): clog (pg_xact) + next_xid/clog kv keys"
```

---

### Task 2: `mvcc::xid` + xid-keyed tuple encoding

**Files:**
- Create: `crates/mvcc/src/xid.rs`
- Modify: `crates/mvcc/src/version.rs` (add new functions; keep SP4 ones until cleanup)
- Modify: `crates/mvcc/src/lib.rs` (`pub mod xid;`)

Additive: the new on-disk version format (key = `row_key + xid`; value = `(xmin, xmax, row)`). The SP4 `version_key`/`encode_version`/`visible_version` stay until the cutover switches callers (Task 7 removes them). `row_prefix_of` (strips the 8-byte suffix) already works for the new keys.

- [ ] **Step 1: xid module.** Create `crates/mvcc/src/xid.rs`:

```rust
//! Transaction ids. `Xid` is a plain `u64` (matching the codebase's rowid /
//! commit_ts convention). `INVALID_XID` (0) is the sentinel an `xmax` carries
//! while a version is live, and is never assigned to a real transaction (real
//! xids start at 1).

pub type Xid = u64;

/// The "no transaction" sentinel: a live version's `xmax`.
pub const INVALID_XID: Xid = 0;
```

- [ ] **Step 2: failing tuple-encoding tests.** In `crates/mvcc/src/version.rs`, add to the test module:

```rust
    #[test]
    fn version_key_xid_is_rowid_prefix_plus_ascending_xid() {
        let prefix = kv::key::row_key(7, 42);
        let k = version_key_xid(7, 42, 100);
        assert!(k.starts_with(&prefix));
        assert_eq!(xid_of_key(&k).expect("xid"), 100);
        // ascending: a higher xid sorts after a lower one for the same row.
        assert!(version_key_xid(7, 42, 100) < version_key_xid(7, 42, 200));
        // row_prefix_of strips the 8-byte xid suffix back to the row key.
        assert_eq!(row_prefix_of(&k).expect("prefix"), prefix.as_slice());
    }

    #[test]
    fn tuple_roundtrips_header_and_row() {
        let row = vec![Datum::Int4(1), Datum::Text("a".into())];
        let bytes = encode_tuple(5, crate::xid::INVALID_XID, &row);
        assert_eq!(decode_tuple(&bytes).expect("decode"), (5, 0, row));
        // a deleted/superseded version keeps its row bytes and carries xmax.
        let bytes = encode_tuple(5, 9, &[Datum::Int4(1)]);
        assert_eq!(decode_tuple(&bytes).expect("decode"), (5, 9, vec![Datum::Int4(1)]));
    }

    #[test]
    fn decode_tuple_rejects_corrupt() {
        assert!(decode_tuple(&[]).is_err());
        assert!(decode_tuple(&[99, 0, 0, 0, 0, 0, 0, 0, 0]).is_err()); // bad tag
        assert!(decode_tuple(&[1, 0, 0]).is_err()); // too short for header
    }
```

Run: `cargo test -p mvcc version::tests::version_key_xid_is_rowid_prefix_plus_ascending_xid` → COMPILE FAIL (functions undefined).

- [ ] **Step 3: implement the new format.** In `crates/mvcc/src/version.rs`, add (alongside the SP4 functions):

```rust
/// SP5 version key: the row key followed by the creating xid (big-endian,
/// ascending). A rowid's versions all share `kv::key::row_key(table, rowid)`.
pub fn version_key_xid(table_id: u32, rowid: u64, xid: u64) -> Vec<u8> {
    let mut k = kv::key::row_key(table_id, rowid);
    k.extend_from_slice(&xid.to_be_bytes());
    k
}

/// The creating xid encoded in a version key's 8-byte suffix.
pub fn xid_of_key(key: &[u8]) -> Result<u64, KvError> {
    if key.len() < 8 {
        return Err(KvError::CorruptRow("version key too short".into()));
    }
    let suffix: [u8; 8] = key[key.len() - 8..].try_into().expect("8 bytes");
    Ok(u64::from_be_bytes(suffix))
}

const T_TUPLE: u8 = 1;

/// Encode a tuple version: a 1-byte tag, the `xmin`/`xmax` header, then the row.
/// `xmax == INVALID_XID` (0) marks a live version. A delete keeps the row bytes
/// and sets `xmax` (PostgreSQL retains the tuple until vacuum).
pub fn encode_tuple(xmin: u64, xmax: u64, row: &[Datum]) -> Vec<u8> {
    let mut out = Vec::with_capacity(17 + row.len() * 8);
    out.push(T_TUPLE);
    out.extend_from_slice(&xmin.to_be_bytes());
    out.extend_from_slice(&xmax.to_be_bytes());
    out.extend_from_slice(&kv::rowenc::encode_row(row));
    out
}

/// Decode a tuple version into `(xmin, xmax, row)`.
pub fn decode_tuple(bytes: &[u8]) -> Result<(u64, u64, Vec<Datum>), KvError> {
    if bytes.len() < 17 || bytes[0] != T_TUPLE {
        return Err(KvError::CorruptRow("bad tuple header".into()));
    }
    let xmin = u64::from_be_bytes(bytes[1..9].try_into().expect("8 bytes"));
    let xmax = u64::from_be_bytes(bytes[9..17].try_into().expect("8 bytes"));
    let row = kv::rowenc::decode_row(&bytes[17..])?;
    Ok((xmin, xmax, row))
}
```

- [ ] **Step 4: register + verify.** In `crates/mvcc/src/lib.rs` add `pub mod xid;`.

Run: `cargo test -p mvcc && cargo clippy -p mvcc --all-targets -- -D warnings`
Expected: green (new tests pass; SP4 tests still pass).

- [ ] **Step 5: commit.**

```bash
git add crates/mvcc/src/xid.rs crates/mvcc/src/version.rs crates/mvcc/src/lib.rs
git commit -m "feat(mvcc): xid type + xid-keyed tuple (xmin/xmax) encoding"
```

---

### Task 3: `mvcc::visibility` — `Snapshot{xmin,xmax,xip}` + `satisfies_mvcc`

**Files:**
- Create: `crates/mvcc/src/visibility.rs`
- Modify: `crates/mvcc/src/lib.rs` (`pub mod visibility;`)

Additive: PostgreSQL's `HeapTupleSatisfiesMVCC` over an xid-list snapshot and the clog. The SP4 `Snapshot(u64)`/`visible_version` in `snapshot.rs` stay until cleanup; the new type lives in its own module so the names don't clash.

- [ ] **Step 1: failing visibility tests.** Create `crates/mvcc/src/visibility.rs` with the test module first (and an empty body that won't compile), or write tests then impl. Tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::clog::XidStatus;

    // A clog stub: maps xid -> status via a small closure.
    fn status_map(committed: &[u64], aborted: &[u64]) -> impl Fn(u64) -> Result<XidStatus, kv::KvError> + '_ {
        move |x| {
            if committed.contains(&x) {
                Ok(XidStatus::Committed)
            } else if aborted.contains(&x) {
                Ok(XidStatus::Aborted)
            } else {
                Ok(XidStatus::InProgress)
            }
        }
    }

    fn snap(xmax: u64, xip: &[u64]) -> Snapshot {
        let mut xip = xip.to_vec();
        xip.sort_unstable();
        let xmin = xip.first().copied().unwrap_or(xmax);
        Snapshot { xmin, xmax, xip }
    }

    #[test]
    fn committed_before_snapshot_is_visible() {
        // xmin=5 committed, snapshot xmax=10, 5 not running -> visible; live (xmax=0).
        let s = snap(10, &[]);
        assert!(satisfies_mvcc(5, 0, &s, None, status_map(&[5], &[])).expect("ok"));
    }

    #[test]
    fn running_at_snapshot_is_invisible() {
        // xmin=5 is in the snapshot's in-progress list -> invisible even if it later commits.
        let s = snap(10, &[5]);
        assert!(!satisfies_mvcc(5, 0, &s, None, status_map(&[5], &[])).expect("ok"));
    }

    #[test]
    fn started_after_snapshot_is_invisible() {
        // xmin=12 >= xmax=10 -> started after my snapshot -> invisible.
        let s = snap(10, &[]);
        assert!(!satisfies_mvcc(12, 0, &s, None, status_map(&[12], &[])).expect("ok"));
    }

    #[test]
    fn aborted_xmin_is_invisible() {
        let s = snap(10, &[]);
        assert!(!satisfies_mvcc(5, 0, &s, None, status_map(&[], &[5])).expect("ok"));
    }

    #[test]
    fn own_xid_is_visible_read_your_writes() {
        // xmin=7 is in-progress (mine), snapshot doesn't include it as committed,
        // but own=Some(7) -> visible.
        let s = snap(7, &[]);
        assert!(satisfies_mvcc(7, 0, &s, Some(7), status_map(&[], &[])).expect("ok"));
    }

    #[test]
    fn committed_visible_delete_hides_row() {
        // xmin=5 committed-visible, xmax=6 committed-visible -> deleted -> invisible.
        let s = snap(10, &[]);
        assert!(!satisfies_mvcc(5, 6, &s, None, status_map(&[5, 6], &[])).expect("ok"));
    }

    #[test]
    fn aborted_or_running_delete_does_not_hide_row() {
        let s = snap(10, &[6]); // xmax=6 still running at my snapshot
        assert!(satisfies_mvcc(5, 6, &s, None, status_map(&[5], &[])).expect("ok"));
        let s2 = snap(10, &[]);
        assert!(satisfies_mvcc(5, 6, &s2, None, status_map(&[5], &[6])).expect("ok")); // xmax aborted
    }

    #[test]
    fn own_delete_hides_row_from_me() {
        // I inserted (xmin=7) and deleted (xmax=7) in my own txn -> invisible to me.
        let s = snap(7, &[]);
        assert!(!satisfies_mvcc(7, 7, &s, Some(7), status_map(&[], &[])).expect("ok"));
    }
}
```

Run: `cargo test -p mvcc visibility` → COMPILE FAIL.

- [ ] **Step 2: implement.** Prepend to `crates/mvcc/src/visibility.rs`:

```rust
//! Snapshot-based visibility — PostgreSQL's `HeapTupleSatisfiesMVCC`. A snapshot
//! is `(xmin, xmax, xip[])`: `xmax` is one past the highest assigned xid, `xip`
//! is the set of xids that were running when the snapshot was taken, and `xmin`
//! is the lowest of those (a fast "everything below is settled" bound). The clog
//! answers "did this xid commit?"; the snapshot answers "before I started?".

use kv::KvError;

use crate::clog::XidStatus;
use crate::xid::INVALID_XID;

/// A read snapshot: the running-transaction set as of a point in time.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub xmin: u64,
    pub xmax: u64,
    pub xip: Vec<u64>, // sorted ascending
}

impl Snapshot {
    /// Was `xid` running at (or started after) the moment this snapshot was taken?
    fn is_running(&self, xid: u64) -> bool {
        xid >= self.xmax || self.xip.binary_search(&xid).is_ok()
    }
}

/// Is a transaction's effect visible to this snapshot? True iff it is the
/// caller's own transaction, or it had committed before the snapshot was taken.
fn committed_visible(
    xid: u64,
    snapshot: &Snapshot,
    own: Option<u64>,
    status: &impl Fn(u64) -> Result<XidStatus, KvError>,
) -> Result<bool, KvError> {
    if Some(xid) == own {
        return Ok(true); // my own write (read-your-writes)
    }
    if snapshot.is_running(xid) {
        return Ok(false); // running at, or started after, my snapshot
    }
    Ok(matches!(status(xid)?, XidStatus::Committed)) // settled: ask the clog
}

/// PostgreSQL `HeapTupleSatisfiesMVCC` for a tuple with header `(xmin, xmax)`:
/// visible iff its creator is visible to the snapshot AND it has not been
/// deleted/superseded by a transaction also visible to the snapshot.
pub fn satisfies_mvcc(
    xmin: u64,
    xmax: u64,
    snapshot: &Snapshot,
    own: Option<u64>,
    status: impl Fn(u64) -> Result<XidStatus, KvError>,
) -> Result<bool, KvError> {
    if !committed_visible(xmin, snapshot, own, &status)? {
        return Ok(false);
    }
    if xmax == INVALID_XID {
        return Ok(true);
    }
    Ok(!committed_visible(xmax, snapshot, own, &status)?)
}
```

- [ ] **Step 3: register + verify.** In `crates/mvcc/src/lib.rs` add `pub mod visibility;`.

Run: `cargo test -p mvcc && cargo clippy -p mvcc --all-targets -- -D warnings` → green.

- [ ] **Step 4: commit.**

```bash
git add crates/mvcc/src/visibility.rs crates/mvcc/src/lib.rs
git commit -m "feat(mvcc): xid-list Snapshot + satisfies_mvcc (HeapTupleSatisfiesMVCC)"
```

---

### Task 4: `executor::ProcArray` — running-transaction registry

**Files:**
- Create: `crates/executor/src/procarray.rs`
- Modify: `crates/executor/src/lib.rs` (`mod procarray;`)

The shared in-memory registry: it owns the next-xid counter (seeded from the durable key at open), tracks running xids, allocates xids, and builds snapshots. Not yet wired into the session (the cutover does that). Pure unit-testable logic.

- [ ] **Step 1: failing tests.** Create `crates/executor/src/procarray.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use kv::MemKv;

    #[test]
    fn fresh_store_starts_at_xid_one() {
        let pa = ProcArray::open(&MemKv::new()).expect("open");
        let s = pa.snapshot();
        assert_eq!(s.xmax, 1);
        assert!(s.xip.is_empty());
    }

    #[test]
    fn allocate_registers_running_and_snapshot_excludes_committed() {
        let pa = ProcArray::open(&MemKv::new()).expect("open");
        let x1 = pa.begin_write();
        let x2 = pa.begin_write();
        assert_eq!((x1, x2), (1, 2));
        // both running: a snapshot taken now lists them as in-progress, xmax=3.
        let s = pa.snapshot();
        assert_eq!(s.xmax, 3);
        assert_eq!(s.xip, vec![1, 2]);
        // x1 finishes: a later snapshot no longer lists it.
        pa.finish(x1);
        let s2 = pa.snapshot();
        assert_eq!(s2.xip, vec![2]);
        assert_eq!(s2.xmax, 3);
    }

    #[test]
    fn open_seeds_next_xid_from_durable_counter() {
        let kv = MemKv::new();
        kv.write_batch(&[kv::WriteOp::Put {
            key: kv::key::next_xid_key(),
            value: 42u64.to_be_bytes().to_vec(),
        }])
        .expect("seed");
        let pa = ProcArray::open(&kv).expect("open");
        assert_eq!(pa.begin_write(), 42);
        assert_eq!(pa.next_xid(), 43);
    }
}
```

Run: `cargo test -p executor procarray` → COMPILE FAIL.

- [ ] **Step 2: implement.** Prepend to `crates/executor/src/procarray.rs`:

```rust
//! The running-transaction registry (PostgreSQL's ProcArray). Shared across all
//! connections behind an `Arc`. Owns the next-xid counter (seeded from the
//! durable `/0/meta/next_xid` at open) and the set of currently-running xids,
//! and builds `mvcc::visibility::Snapshot`s. After a restart it starts empty, so
//! any clog `in-progress` xid is in no snapshot and resolves as aborted.

use std::collections::BTreeSet;
use std::sync::Mutex;

use kv::Kv;
use mvcc::visibility::Snapshot;

use crate::error::ExecError;

struct Inner {
    next_xid: u64,
    running: BTreeSet<u64>,
}

pub struct ProcArray {
    inner: Mutex<Inner>,
}

impl ProcArray {
    /// Seed the next-xid counter from the durable key (default 1 — real xids
    /// start at 1; 0 is the invalid sentinel).
    pub fn open(kv: &dyn Kv) -> Result<Self, ExecError> {
        let next_xid = match kv.get(&kv::key::next_xid_key())? {
            Some(b) => {
                let a: [u8; 8] = b
                    .as_slice()
                    .try_into()
                    .map_err(|_| kv::KvError::CorruptRow("next_xid is not u64".into()))?;
                u64::from_be_bytes(a)
            }
            None => 1,
        };
        Ok(Self {
            inner: Mutex::new(Inner {
                next_xid: next_xid.max(1),
                running: BTreeSet::new(),
            }),
        })
    }

    /// Allocate the next xid and register it as running. The caller MUST persist
    /// `next_xid()` in the same write batch as the transaction's first version so
    /// a crash cannot reuse the xid.
    pub fn begin_write(&self) -> u64 {
        let mut g = self.inner.lock().expect("procarray");
        let xid = g.next_xid;
        g.next_xid += 1;
        g.running.insert(xid);
        xid
    }

    /// The durable next-xid value to persist (one past the highest allocated).
    pub fn next_xid(&self) -> u64 {
        self.inner.lock().expect("procarray").next_xid
    }

    /// A snapshot of the currently-running transactions.
    pub fn snapshot(&self) -> Snapshot {
        let g = self.inner.lock().expect("procarray");
        let xip: Vec<u64> = g.running.iter().copied().collect(); // BTreeSet => sorted
        let xmax = g.next_xid;
        let xmin = xip.first().copied().unwrap_or(xmax);
        Snapshot { xmin, xmax, xip }
    }

    /// Deregister a finished (committed or aborted) transaction. Call only after
    /// its clog entry is durable.
    pub fn finish(&self, xid: u64) {
        self.inner.lock().expect("procarray").running.remove(&xid);
    }
}
```

- [ ] **Step 3: register + verify.** In `crates/executor/src/lib.rs` add `mod procarray;` (next to the other `mod` lines).

Run: `cargo test -p executor procarray && cargo clippy -p executor --all-targets -- -D warnings`
Expected: green (the rest of executor is untouched).

- [ ] **Step 4: commit.**

```bash
git add crates/executor/src/procarray.rs crates/executor/src/lib.rs
git commit -m "feat(executor): ProcArray running-transaction registry + snapshots"
```

---

### Task 5: Cutover — write-through executor over xid/clog/ProcArray (behavior-identical)

**Files:**
- Modify: `crates/executor/src/lib.rs` (SqlEngine: add `procarray`, switch `write_lock` to `tokio::sync::Mutex`)
- Modify: `crates/executor/src/session.rs` (async `run_one`; new `TxnCtx`; clog commit/abort; drop the write-set)
- Modify: `crates/executor/src/exec.rs` (`execute_read` + `execute_write` returning ops; `scan_live` via `satisfies_mvcc`)

This is the pivotal task: the executor stops buffering writes and instead writes versions through to disk tagged with the transaction's xid, resolves reads with `satisfies_mvcc` (passing the txn's own xid for read-your-writes), and serializes writers by holding an async writer lock for a writing transaction's duration. **No external behavior changes — all 212 existing tests must stay green** — plus new own-xid/write-through unit tests. There is no clean sub-split because the read and write paths share the on-disk format; do it as one task, committing once green.

**Key model (read before coding):**
- A writing statement (`INSERT`/`UPDATE`/`DELETE`) or DDL acquires the engine writer lock (async). For an explicit transaction the lock + the xid are acquired at the **first** write and held until `COMMIT`/`ROLLBACK`; for autocommit they are acquired and released around the one statement. Read-only statements never lock.
- `execute_write` does not write to the store itself — it **returns** `(QueryResult, Vec<WriteOp>)`. The session assembles the final batch (append the `next_xid` Put always; append a clog `Committed` Put for autocommit) and writes it once. This keeps autocommit a single atomic batch and makes explicit-txn statements write-through per statement.
- Read-your-writes: a row created by the current xid is visible because `satisfies_mvcc` is passed `own = ctx.xid`.

- [ ] **Step 1: SqlEngine — add ProcArray, async writer lock.** Rewrite `crates/executor/src/lib.rs`'s engine struct and constructors:

```rust
//! executor: turns parsed SQL into catalog/KV operations and implements the
//! pgwire `Engine` trait. SP5 swaps SP4's commit_ts MVCC for PostgreSQL's
//! xid/clog/snapshot model with uncommitted versions on disk; writers stay
//! serialized behind a transaction-scoped async writer lock.

mod error;
mod eval;
mod exec;
mod procarray;
mod session;

use std::path::Path;
use std::sync::Arc;

use kv::{FjallKv, Kv, MemKv};
use pgwire::engine::Engine;
use tokio::sync::Mutex;

pub use error::ExecError;
pub use session::SqlSession;

use crate::procarray::ProcArray;

/// The SQL engine over a durable (or in-memory) KV store. Catalog, sequences,
/// the xid counter, and the clog live in the KV store. The async writer mutex
/// serializes writing transactions engine-wide (SP5 keeps writers serialized;
/// SP6 replaces this lock with row-level concurrency). The `ProcArray` is shared
/// so every connection's snapshots see the same running-transaction set.
pub struct SqlEngine {
    pub(crate) kv: Arc<dyn Kv>,
    pub(crate) writer_lock: Arc<Mutex<()>>,
    pub(crate) procarray: Arc<ProcArray>,
}

impl Default for SqlEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlEngine {
    /// Ephemeral in-memory engine (tests, default when no --data-dir).
    pub fn new() -> Self {
        Self::with_kv(Arc::new(MemKv::new())).expect("in-memory engine never fails to open")
    }

    /// Durable engine backed by a fjall store at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ExecError> {
        Self::with_kv(Arc::new(FjallKv::open(path)?))
    }

    pub fn with_kv(kv: Arc<dyn Kv>) -> Result<Self, ExecError> {
        let procarray = Arc::new(ProcArray::open(&*kv)?);
        Ok(Self {
            kv,
            writer_lock: Arc::new(Mutex::new(())),
            procarray,
        })
    }
}

impl Engine for SqlEngine {
    type Session = SqlSession;

    fn connect(&self) -> SqlSession {
        SqlSession::new(
            Arc::clone(&self.kv),
            Arc::clone(&self.writer_lock),
            Arc::clone(&self.procarray),
        )
    }
}
```

Note `with_kv` now returns `Result` (ProcArray::open reads the store). `new()` `.expect`s (MemKv reads never fail). Any caller of `SqlEngine::with_kv(...)` must add `?`/`.expect`; check `crates/crabgresql` and tests with `grep -rn "with_kv"`.

- [ ] **Step 2: rewrite the session.** Replace the body of `crates/executor/src/session.rs`:

```rust
//! Per-connection session: runs SQL against the shared KV store. SP5 uses
//! PostgreSQL's xid/clog/snapshot MVCC: writes go through to disk tagged with
//! the transaction's xid (read-your-writes via `satisfies_mvcc` + own xid),
//! commit/rollback record the outcome in the clog, and a transaction-scoped
//! async writer lock keeps writers serialized.

use std::sync::Arc;

use kv::Kv;
use mvcc::clog::XidStatus;
use mvcc::visibility::Snapshot;
use pgparser::ast::{IsolationLevel, Statement};
use pgwire::engine::{FieldDescription, QueryResult, Session, TxStatus};
use pgwire::error::PgError;
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::error::ExecError;
use crate::procarray::ProcArray;

/// In-flight transaction context.
pub(crate) struct TxnCtx {
    /// Assigned lazily at the first write (None for a read-only transaction).
    pub(crate) xid: Option<u64>,
    /// The visibility snapshot: re-taken per statement under READ COMMITTED,
    /// fixed at BEGIN under REPEATABLE READ.
    pub(crate) snapshot: Snapshot,
    pub(crate) repeatable_read: bool,
    /// The engine writer lock, held from the first write until COMMIT/ROLLBACK.
    pub(crate) writer_guard: Option<OwnedMutexGuard<()>>,
}

enum TxnState {
    Idle,
    InTransaction(TxnCtx),
    Failed,
}

pub struct SqlSession {
    pub(crate) kv: Arc<dyn Kv>,
    writer_lock: Arc<Mutex<()>>,
    procarray: Arc<ProcArray>,
    state: TxnState,
}

impl SqlSession {
    pub fn new(kv: Arc<dyn Kv>, writer_lock: Arc<Mutex<()>>, procarray: Arc<ProcArray>) -> Self {
        Self {
            kv,
            writer_lock,
            procarray,
            state: TxnState::Idle,
        }
    }

    async fn run_one(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        if matches!(self.state, TxnState::Failed)
            && !matches!(stmt, Statement::Commit | Statement::Rollback)
        {
            return Err(ExecError::InFailedTransaction);
        }
        match stmt {
            Statement::Begin { isolation } => self.begin(*isolation),
            Statement::Commit => self.commit_cmd(),
            Statement::Rollback => self.rollback_cmd(),
            Statement::CreateTable { .. } | Statement::DropTable { .. } => self.run_ddl(stmt).await,
            Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. } => {
                self.run_write(stmt).await
            }
            Statement::Select(_) => self.run_select(stmt),
        }
    }

    fn begin(&mut self, isolation: Option<IsolationLevel>) -> Result<QueryResult, ExecError> {
        if matches!(self.state, TxnState::InTransaction(_)) {
            return Ok(QueryResult::Command { tag: "BEGIN".into() });
        }
        let rr = matches!(isolation, Some(IsolationLevel::RepeatableRead));
        // RR fixes its snapshot at BEGIN; RC leaves a placeholder refreshed per
        // statement. Either way we capture the current running set now.
        let snapshot = self.procarray.snapshot();
        self.state = TxnState::InTransaction(TxnCtx {
            xid: None,
            snapshot,
            repeatable_read: rr,
            writer_guard: None,
        });
        Ok(QueryResult::Command { tag: "BEGIN".into() })
    }

    fn commit_cmd(&mut self) -> Result<QueryResult, ExecError> {
        match std::mem::replace(&mut self.state, TxnState::Idle) {
            TxnState::InTransaction(ctx) => {
                if let Some(xid) = ctx.xid {
                    // Record the commit, then release the lock and deregister.
                    self.kv
                        .write_batch(&[mvcc::clog::put_op(xid, XidStatus::Committed)])?;
                    self.procarray.finish(xid);
                }
                drop(ctx.writer_guard); // release the writer lock (if held)
                Ok(QueryResult::Command { tag: "COMMIT".into() })
            }
            TxnState::Failed => Ok(QueryResult::Command { tag: "ROLLBACK".into() }),
            TxnState::Idle => Ok(QueryResult::Command { tag: "COMMIT".into() }),
        }
    }

    fn rollback_cmd(&mut self) -> Result<QueryResult, ExecError> {
        if let TxnState::InTransaction(ctx) = std::mem::replace(&mut self.state, TxnState::Idle) {
            if let Some(xid) = ctx.xid {
                // Best-effort abort record; the versions are already invisible
                // (in-progress in no future snapshot once deregistered), so even
                // if this write is lost the rows never become visible.
                self.kv
                    .write_batch(&[mvcc::clog::put_op(xid, XidStatus::Aborted)])?;
                self.procarray.finish(xid);
            }
            drop(ctx.writer_guard);
        }
        Ok(QueryResult::Command { tag: "ROLLBACK".into() })
    }

    fn run_select(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        let (snapshot, own) = self.read_context()?;
        crate::exec::execute_read(&*self.kv, &snapshot, own, stmt)
    }

    /// The snapshot + own-xid a read should use. Autocommit: a fresh snapshot,
    /// no own xid. In a txn: RC re-snapshots per statement, RR reuses its
    /// snapshot; own xid is the txn's (Some after its first write).
    fn read_context(&mut self) -> Result<(Snapshot, Option<u64>), ExecError> {
        match &mut self.state {
            TxnState::Idle => Ok((self.procarray.snapshot(), None)),
            TxnState::InTransaction(ctx) => {
                if !ctx.repeatable_read {
                    ctx.snapshot = self.procarray.snapshot();
                }
                Ok((ctx.snapshot.clone(), ctx.xid))
            }
            TxnState::Failed => unreachable!("guarded in run_one"),
        }
    }

    async fn run_ddl(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        // DDL is non-transactional: it auto-commits under a short-lived lock.
        let _guard = self.writer_lock.clone().lock_owned().await;
        crate::exec::execute_ddl(&*self.kv, stmt)
    }

    async fn run_write(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        match &self.state {
            TxnState::InTransaction(_) => {
                self.ensure_write_xid().await?;
                // RC refreshes the read snapshot used by UPDATE/DELETE's scan.
                let refresh =
                    matches!(&self.state, TxnState::InTransaction(c) if !c.repeatable_read);
                if refresh {
                    let s = self.procarray.snapshot();
                    if let TxnState::InTransaction(c) = &mut self.state {
                        c.snapshot = s;
                    }
                }
                let (snapshot, xid) = match &self.state {
                    TxnState::InTransaction(c) => (c.snapshot.clone(), c.xid.expect("xid set")),
                    _ => unreachable!(),
                };
                let kv = Arc::clone(&self.kv);
                let (result, mut ops) =
                    crate::exec::execute_write(&*kv, &snapshot, xid, stmt).map_err(|e| {
                        // An error inside a block fails the transaction (the lock
                        // and xid stay until COMMIT/ROLLBACK).
                        self.state = TxnState::Failed;
                        e
                    })?;
                // Persist next_xid with the statement's writes (no clog entry —
                // the txn commits later).
                ops.push(kv::WriteOp::Put {
                    key: kv::key::next_xid_key(),
                    value: self.procarray.next_xid().to_be_bytes().to_vec(),
                });
                self.kv.write_batch(&ops)?;
                Ok(result)
            }
            TxnState::Idle => {
                // Autocommit: acquire the lock, allocate an xid, execute, and
                // commit in one atomic batch (versions + next_xid + clog).
                let guard = self.writer_lock.clone().lock_owned().await;
                let xid = self.procarray.begin_write();
                let snapshot = self.procarray.snapshot();
                let kv = Arc::clone(&self.kv);
                let outcome = crate::exec::execute_write(&*kv, &snapshot, xid, stmt);
                let (result, mut ops) = match outcome {
                    Ok(v) => v,
                    Err(e) => {
                        // Autocommit error: abort and stay Idle. Record the abort
                        // (best-effort) and deregister; the lock drops with guard.
                        let _ = self
                            .kv
                            .write_batch(&[mvcc::clog::put_op(xid, XidStatus::Aborted)]);
                        self.procarray.finish(xid);
                        drop(guard);
                        return Err(e);
                    }
                };
                ops.push(kv::WriteOp::Put {
                    key: kv::key::next_xid_key(),
                    value: self.procarray.next_xid().to_be_bytes().to_vec(),
                });
                ops.push(mvcc::clog::put_op(xid, XidStatus::Committed));
                self.kv.write_batch(&ops)?;
                self.procarray.finish(xid);
                drop(guard);
                Ok(result)
            }
            TxnState::Failed => unreachable!("guarded in run_one"),
        }
    }

    /// On a transaction's first write: acquire the writer lock and allocate the
    /// xid (idempotent on later writes).
    async fn ensure_write_xid(&mut self) -> Result<(), ExecError> {
        let needs = matches!(&self.state, TxnState::InTransaction(c) if c.xid.is_none());
        if !needs {
            return Ok(());
        }
        let guard = self.writer_lock.clone().lock_owned().await;
        let xid = self.procarray.begin_write();
        if let TxnState::InTransaction(c) = &mut self.state {
            c.xid = Some(xid);
            c.writer_guard = Some(guard);
        }
        Ok(())
    }
}

impl Session for SqlSession {
    async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        if sql.trim().is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        let statements = pgparser::parse(sql).map_err(|e| ExecError::from(e).into_pg())?;
        if statements.is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        let mut results = Vec::with_capacity(statements.len());
        for stmt in statements {
            results.push(self.run_one(&stmt).await.map_err(ExecError::into_pg)?);
        }
        Ok(results)
    }

    async fn describe(&mut self, sql: &str) -> Result<Vec<FieldDescription>, PgError> {
        crate::exec::describe(&*self.kv, sql).map_err(ExecError::into_pg)
    }

    fn tx_status(&self) -> TxStatus {
        match self.state {
            TxnState::Idle => TxStatus::Idle,
            TxnState::InTransaction(_) => TxStatus::InTransaction,
            TxnState::Failed => TxStatus::Failed,
        }
    }
}
```

(Note: `describe` now takes `&dyn Kv` — update `exec::describe`'s signature in Step 3. The `run_write` error-mapping closure that sets `Failed` must not also early-return the lock; the guard lives in `ctx.writer_guard` and is released at COMMIT/ROLLBACK, which is correct for a failed block.)

- [ ] **Step 3: rewrite exec.rs read/write paths.** In `crates/executor/src/exec.rs`:
  - Delete the `use crate::session::{Pending, TxnCtx};` import and everything that referenced the write-set.
  - Change `execute_ddl` to take `kv: &dyn Kv` (it no longer takes the session/lock — the session holds the lock now):

```rust
pub(crate) fn execute_ddl(kv: &dyn Kv, stmt: &Statement) -> Result<QueryResult, ExecError> {
    match stmt {
        Statement::CreateTable { name, columns } => {
            let cols = columns.iter().map(|c| Column { name: c.name.clone(), ty: c.ty }).collect();
            catalog::create_table(kv, name, cols)?;
            Ok(QueryResult::Command { tag: "CREATE TABLE".into() })
        }
        Statement::DropTable { name } => {
            catalog::drop_table(kv, name)?;
            Ok(QueryResult::Command { tag: "DROP TABLE".into() })
        }
        _ => Err(ExecError::Unsupported("not a DDL statement".into())),
    }
}
```

  - Replace `execute_dml`/`scan_live_rows` with a read path and a write path. `scan_live` returns `(rowid, xmin_of_visible_version, row)` so UPDATE/DELETE know the matched version's key suffix and whether it is the txn's own version:

```rust
/// Scan a table's visible rows under `snapshot` (and the caller's own xid for
/// read-your-writes). Returns `(rowid, xmin, row)` for the one visible version
/// of each live row, sorted by rowid.
pub(crate) fn scan_live(
    kv: &dyn Kv,
    snapshot: &mvcc::visibility::Snapshot,
    own: Option<u64>,
    table: &catalog::Table,
) -> Result<Vec<(u64, u64, Vec<pgtypes::Datum>)>, ExecError> {
    let scanned = kv.scan_prefix(&kv::key::table_prefix(table.id))?;
    let mut out: Vec<(u64, u64, Vec<pgtypes::Datum>)> = Vec::new();
    let mut i = 0;
    while i < scanned.len() {
        let prefix = mvcc::version::row_prefix_of(&scanned[i].0)?.to_vec();
        let rowid = kv::key::rowid_of(table.id, &prefix)?;
        let mut visible: Option<(u64, Vec<pgtypes::Datum>)> = None;
        while i < scanned.len() && mvcc::version::row_prefix_of(&scanned[i].0)? == prefix.as_slice()
        {
            let (xmin, xmax, row) = mvcc::version::decode_tuple(&scanned[i].1)?;
            if mvcc::visibility::satisfies_mvcc(xmin, xmax, snapshot, own, |x| {
                mvcc::clog::get(kv, x)
            })? {
                visible = Some((xmin, row)); // the MVCC invariant: at most one
            }
            i += 1;
        }
        if let Some((xmin, row)) = visible {
            out.push((rowid, xmin, row));
        }
    }
    out.sort_by_key(|(rowid, _, _)| *rowid);
    Ok(out)
}
```

  - The read path (`execute_read`) is the old `exec_select` with the new scan (dropping `xmin` for projection) — pass `(kv, snapshot, own, select)`. Keep `resolve_projection`/`order_cmp`/`row_matches`/`datum_to_cell`/`field` unchanged. The FROM-less path is unchanged.

```rust
pub(crate) fn execute_read(
    kv: &dyn Kv,
    snapshot: &mvcc::visibility::Snapshot,
    own: Option<u64>,
    stmt: &Statement,
) -> Result<QueryResult, ExecError> {
    let Statement::Select(s) = stmt else {
        return Err(ExecError::Unsupported("not a SELECT".into()));
    };
    let table: Option<Table> = match &s.from {
        Some(name) => Some(catalog::get_table(kv, name)?),
        None => None,
    };
    let source: Vec<Vec<Datum>> = match &table {
        Some(t) => scan_live(kv, snapshot, own, t)?
            .into_iter()
            .map(|(_, _, row)| row)
            .collect(),
        None => vec![vec![]],
    };
    // ... identical WHERE / ORDER BY / LIMIT / projection logic from SP4's
    // exec_select, operating on `source` ...
}
```

  - The write path (`execute_write`) returns `(QueryResult, Vec<WriteOp>)` and never writes the store itself:

```rust
pub(crate) fn execute_write(
    kv: &dyn Kv,
    snapshot: &mvcc::visibility::Snapshot,
    xid: u64,
    stmt: &Statement,
) -> Result<(QueryResult, Vec<kv::WriteOp>), ExecError> {
    let mut ops: Vec<kv::WriteOp> = Vec::new();
    match stmt {
        Statement::Insert { table, columns, rows } => {
            if rows.is_empty() {
                return Ok((QueryResult::Command { tag: "INSERT 0 0".into() }, ops));
            }
            let t = catalog::get_table(kv, table)?;
            let target_idx = resolve_targets(&t, columns)?; // helper extracted from SP4 INSERT
            let start = read_seq_kv(kv, t.id)?;
            let mut rowid = start;
            for row_exprs in rows {
                if row_exprs.len() != target_idx.len() {
                    return Err(ExecError::TypeMismatch(
                        "INSERT has the wrong number of expressions for the target columns".into(),
                    ));
                }
                let mut full = vec![pgtypes::Datum::Null; t.columns.len()];
                for (slot, expr) in target_idx.iter().zip(row_exprs.iter()) {
                    full[*slot] = coerce(crate::eval::eval(expr, None, &[])?, t.columns[*slot].ty)?;
                }
                ops.push(kv::WriteOp::Put {
                    key: mvcc::version::version_key_xid(t.id, rowid, xid),
                    value: mvcc::version::encode_tuple(xid, mvcc::xid::INVALID_XID, &full),
                });
                rowid += 1;
            }
            let n = rowid - start;
            ops.push(kv::WriteOp::Put {
                key: kv::key::seq_key(t.id),
                value: rowid.to_be_bytes().to_vec(),
            });
            Ok((QueryResult::Command { tag: format!("INSERT 0 {n}") }, ops))
        }
        Statement::Update { table, assignments, filter } => {
            let t = catalog::get_table(kv, table)?;
            let targets: Vec<(usize, &Expr)> = assignments
                .iter()
                .map(|(col, expr)| {
                    t.column_index(col)
                        .map(|idx| (idx, expr))
                        .ok_or_else(|| ExecError::UndefinedColumn(col.clone()))
                })
                .collect::<Result<_, _>>()?;
            let mut n: u64 = 0;
            for (rowid, xmin, row) in scan_live(kv, snapshot, Some(xid), &t)? {
                if !row_matches(filter.as_ref(), Some(&t), &row)? {
                    continue;
                }
                let mut next = row.clone();
                for (idx, expr) in &targets {
                    next[*idx] = coerce(crate::eval::eval(expr, Some(&t), &row)?, t.columns[*idx].ty)?;
                }
                if xmin == xid {
                    // Updating my own uncommitted version: overwrite in place
                    // (last-write-wins within the txn; no new tuple, xmax stays
                    // invalid). PostgreSQL uses cmin/cmax here; we have no command
                    // ids, so in-place replacement is the faithful observable result.
                    ops.push(kv::WriteOp::Put {
                        key: mvcc::version::version_key_xid(t.id, rowid, xid),
                        value: mvcc::version::encode_tuple(xid, mvcc::xid::INVALID_XID, &next),
                    });
                } else {
                    // Supersede a committed version: stamp its xmax, write a new tuple.
                    ops.push(kv::WriteOp::Put {
                        key: mvcc::version::version_key_xid(t.id, rowid, xmin),
                        value: mvcc::version::encode_tuple(xmin, xid, &row),
                    });
                    ops.push(kv::WriteOp::Put {
                        key: mvcc::version::version_key_xid(t.id, rowid, xid),
                        value: mvcc::version::encode_tuple(xid, mvcc::xid::INVALID_XID, &next),
                    });
                }
                n += 1;
            }
            Ok((QueryResult::Command { tag: format!("UPDATE {n}") }, ops))
        }
        Statement::Delete { table, filter } => {
            let t = catalog::get_table(kv, table)?;
            let mut n: u64 = 0;
            for (rowid, xmin, row) in scan_live(kv, snapshot, Some(xid), &t)? {
                if !row_matches(filter.as_ref(), Some(&t), &row)? {
                    continue;
                }
                // Set xmax = my xid on the matched version (keep its row bytes).
                ops.push(kv::WriteOp::Put {
                    key: mvcc::version::version_key_xid(t.id, rowid, xmin),
                    value: mvcc::version::encode_tuple(xmin, xid, &row),
                });
                n += 1;
            }
            Ok((QueryResult::Command { tag: format!("DELETE {n}") }, ops))
        }
        _ => Err(ExecError::Unsupported("not a write statement".into())),
    }
}
```

  - Extract the INSERT target-column resolution into `fn resolve_targets(t: &Table, columns: &Option<Vec<String>>) -> Result<Vec<usize>, ExecError>` (the `match columns { Some(..) => ..., None => (0..t.columns.len()).collect() }` block from SP4) and reuse it.
  - Change `describe` to take `kv: &dyn Kv` (it only reads the catalog): `pub(crate) fn describe(kv: &dyn Kv, sql: &str) -> Result<Vec<FieldDescription>, ExecError>` and replace `&*session.kv` with `kv`.

- [ ] **Step 4: fix the exec.rs test module.** Its `run`/`connect()` helpers are unchanged (they go through `simple_query`), but `SqlEngine::new()` is unchanged in signature. Verify the whole module compiles; the assertions are unchanged (behavior identical). Add two new tests exercising the new model directly:

```rust
    #[tokio::test]
    async fn read_your_writes_via_own_xid_in_txn() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();
        s.simple_query("CREATE TABLE t (id int4)").await.expect("create");
        s.simple_query("BEGIN").await.expect("begin");
        s.simple_query("INSERT INTO t VALUES (1)").await.expect("insert");
        // Own uncommitted insert is visible to this txn (no write-set; via xid).
        assert_eq!(rows_of(&run_s(&mut s, "SELECT id FROM t").await[0]).len(), 1);
        s.simple_query("ROLLBACK").await.expect("rollback");
        assert_eq!(rows_of(&run_s(&mut s, "SELECT id FROM t").await[0]).len(), 0);
    }

    #[tokio::test]
    async fn another_session_cannot_see_uncommitted_rows() {
        let engine = SqlEngine::new();
        let mut writer = engine.connect();
        writer.simple_query("CREATE TABLE t (id int4)").await.expect("create");
        writer.simple_query("BEGIN").await.expect("begin");
        writer.simple_query("INSERT INTO t VALUES (1)").await.expect("insert");
        // A concurrent session must not see the in-progress row.
        let mut reader = engine.connect();
        assert_eq!(rows_of(&run_s(&mut reader, "SELECT id FROM t").await[0]).len(), 0);
        writer.simple_query("COMMIT").await.expect("commit");
        // After commit a fresh snapshot sees it.
        assert_eq!(rows_of(&run_s(&mut reader, "SELECT id FROM t").await[0]).len(), 1);
    }
```

Add a `run_s` helper next to `run`: `async fn run_s(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> { s.simple_query(sql).await.expect("ok") }` (import `SqlSession`).

- [ ] **Step 5: fix other `with_kv` callers.** Run `grep -rn "with_kv" crates/` and add `?`/`.expect("open")` where `SqlEngine::with_kv` is now `Result`. (The binary `crates/crabgresql` constructs the engine via `open`/`new`, which are unchanged — but verify.)

- [ ] **Step 6: run the full suite — the regression gate.**

Run: `cargo test --workspace`
Expected: **all 212 existing tests pass** plus the new ones. Behavior is observably identical: autocommit, RC/RR isolation, read-your-writes, rollback-discards, failed-block, UPDATE/DELETE, durability, e2e. If `repeatable_read_does_not_see_concurrent_commit` or `read_committed_sees_concurrent_commit_next_statement` fail, the snapshot timing (`read_context` refresh vs RR fix) is wrong — fix there. If a concurrent-reader test deadlocks, a read path is taking the writer lock — it must not.

- [ ] **Step 7: fmt + clippy + commit.**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings` → clean.

```bash
git add crates/executor
git commit -m "feat(executor): write-through xid/clog MVCC; satisfies_mvcc reads; txn-scoped async writer lock"
```

---

### Task 6: Lazy crash-recovery + durability tests

**Files:**
- Create: `crates/executor/tests/recovery.rs`
- Modify: `crates/executor/tests/durability.rs` (add an uncommitted-on-disk case)

No new production code — this proves the spec's recovery and durability criteria. After a restart the ProcArray is empty, so any clog `in-progress` xid is in no snapshot and `< next_xid`, hence `satisfies_mvcc` resolves it as not-committed ⇒ invisible.

- [ ] **Step 1: recovery test.** Create `crates/executor/tests/recovery.rs`:

```rust
//! Lazy crash recovery: versions written by a transaction that never recorded a
//! clog commit are invisible after the store is reopened (the ProcArray starts
//! empty, so the in-progress xid is in no snapshot).

use executor::SqlEngine;
use pgwire::engine::{Cell, Engine, QueryResult, Session};

fn count(r: &QueryResult) -> usize {
    match r {
        QueryResult::Rows { rows, .. } => rows.len(),
        other => panic!("expected Rows, got {other:?}"),
    }
}

async fn rows(s: &mut executor::SqlSession, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql).await.expect("ok")
}

#[tokio::test]
async fn uncommitted_versions_are_invisible_after_reopen() {
    let dir = tempfile::tempdir().expect("tmp");
    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        let mut s = engine.connect();
        rows(&mut s, "CREATE TABLE t (id int4)").await;
        rows(&mut s, "BEGIN").await;
        rows(&mut s, "INSERT INTO t VALUES (1),(2),(3)").await;
        // Drop WITHOUT commit: the engine is dropped mid-transaction, simulating
        // a crash. The versions are on disk (write-through) but clog has no entry.
    }
    let engine = SqlEngine::open(dir.path()).expect("reopen");
    let mut s = engine.connect();
    assert_eq!(count(&rows(&mut s, "SELECT id FROM t").await[0]), 0, "in-progress rows invisible");
    // The table still works for new writes after recovery.
    rows(&mut s, "INSERT INTO t VALUES (9)").await;
    assert_eq!(count(&rows(&mut s, "SELECT id FROM t").await[0]), 1);
}

#[tokio::test]
async fn committed_versions_survive_reopen() {
    let dir = tempfile::tempdir().expect("tmp");
    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        let mut s = engine.connect();
        rows(&mut s, "CREATE TABLE t (id int4)").await;
        rows(&mut s, "BEGIN").await;
        rows(&mut s, "INSERT INTO t VALUES (1),(2)").await;
        rows(&mut s, "COMMIT").await;
    }
    let engine = SqlEngine::open(dir.path()).expect("reopen");
    let mut s = engine.connect();
    assert_eq!(count(&rows(&mut s, "SELECT id FROM t").await[0]), 2);
}
```

(Note: dropping the engine mid-transaction without COMMIT/ROLLBACK is exactly the crash scenario — the `SqlSession` is dropped while `InTransaction`, no clog entry is written. If a `Drop` impl is ever added to roll back, this test still holds since a clog `Aborted` is equally invisible.)

- [ ] **Step 2: run.** `cargo test -p executor --test recovery` → PASS. If `uncommitted_versions_are_invisible_after_reopen` shows 3 rows, the visibility path is trusting an absent/in-progress clog entry as committed — fix `committed_visible`/`clog::get`.

- [ ] **Step 3: durability addition.** Confirm `crates/executor/tests/durability.rs` still passes unchanged (`cargo test -p executor --test durability`). The SP4 `committed_transaction_survives_reopen` / `rolled_back_transaction_leaves_nothing` now exercise the xid/clog path; they must stay green.

- [ ] **Step 4: commit.**

```bash
git add crates/executor/tests/recovery.rs
git commit -m "test(executor): lazy crash recovery — uncommitted versions invisible after reopen"
```

---

### Task 7: Remove dead SP4 MVCC code

**Files:**
- Modify: `crates/mvcc/src/version.rs` (remove SP4 `version_key`/`commit_ts_of`/`ts_of_key`/`encode_version`/`decode_version` + their tests)
- Delete: `crates/mvcc/src/snapshot.rs` (SP4 `Snapshot(u64)`/`visible_version`)
- Modify: `crates/mvcc/src/lib.rs` (re-exports)
- Modify: `crates/kv/src/key.rs` (remove `commit_ts_key` if unused) and `crates/executor` (remove `read_commit_ts`, `commit_ts_key` use)

The cutover left the SP4 commit-timestamp machinery unused. Remove it so there is one MVCC model. Do this only after Task 5/6 are green.

- [ ] **Step 1: find the dead symbols.** Run:

```bash
grep -rn "version_key\b\|commit_ts_of\|ts_of_key\|visible_version\|encode_version\|decode_version\|Snapshot(\|commit_ts_key\|read_commit_ts" crates/ | grep -v "version_key_xid\|xid_of_key\|encode_tuple\|decode_tuple\|visibility::Snapshot"
```

Expected after the cutover: only definitions in `mvcc/src/{version.rs,snapshot.rs}` and `kv/src/key.rs::commit_ts_key` (no live callers). If any non-test caller remains, the cutover missed a spot — fix it before deleting.

- [ ] **Step 2: remove.** Delete from `crates/mvcc/src/version.rs`: `version_key`, `commit_ts_of`, `ts_of_key`, `encode_version`, `decode_version`, the `V_ROW`/`V_TOMBSTONE` consts, and the tests that exercise only those (`row_prefix_of_strips_ts_suffix` etc. that use `version_key` — rewrite the `row_prefix_of` test to use `version_key_xid`). Keep `row_prefix_of` (still used). Delete `crates/mvcc/src/snapshot.rs`. In `crates/mvcc/src/lib.rs`:

```rust
//! mvcc: PostgreSQL-faithful multiversion concurrency control for crabgresql —
//! xids, the clog (pg_xact), xid-keyed tuple (xmin/xmax) encoding, xid-list
//! snapshots, and HeapTupleSatisfiesMVCC visibility. Concurrent writers (row
//! locks, block-and-retry, EvalPlanQual) arrive in SP6; deadlock detection SP7.

pub mod clog;
pub mod version;
pub mod visibility;
pub mod xid;

pub use visibility::{Snapshot, satisfies_mvcc};
pub use xid::{INVALID_XID, Xid};
```

Remove `commit_ts_key` from `kv/src/key.rs` and its test if no caller remains; remove `SqlSession::read_commit_ts` (already gone in the cutover — confirm).

- [ ] **Step 3: verify.** `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings` → green; the dead code is gone and nothing references it.

- [ ] **Step 4: commit.**

```bash
git add crates/mvcc crates/kv crates/executor
git commit -m "refactor(mvcc): remove dead SP4 commit-timestamp MVCC code"
```

---

### Task 8: Gauntlet, traceability, and conformance

**Files:**
- Verify only: corpus, CI gates.

- [ ] **Step 1: full release gauntlet.**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test -p pgparser --features oracle
./scripts/check-no-native.sh
cargo deny check
```

Expected: every gate green; the existing 212 tests still pass (behavior identical) plus the SP5 additions (clog, xid/tuple, visibility, procarray, read-your-writes, recovery). `check-no-native.sh` and `cargo deny` are unaffected (no new shipped deps; `tokio` was already present).

- [ ] **Step 2: conformance.** Run the conformance suite the project's usual way (`cargo test -p conformance`; the live-oracle leg needs Docker `postgres:18`). The existing corpus (`update_delete.sql`, `transactions.sql`, …) must still pass — MVCC visibility changed internally but results are identical. Record parity ≥ the SP4 baseline.

- [ ] **Step 3: success-criteria traceability.** Confirm each spec success criterion maps to a green test:

| # | Spec success criterion | Verifying test(s) |
|---|------------------------|-------------------|
| 1 | Visibility via `satisfies_mvcc` over xid-list snapshots + clog | `mvcc::visibility::tests::*` (truth table), `mvcc::clog::tests::*` |
| 2 | Uncommitted versions on disk; committed survive restart; in-progress/rolled-back invisible (lazy recovery) | `recovery::uncommitted_versions_are_invisible_after_reopen`, `recovery::committed_versions_survive_reopen`, `durability::*` |
| 3 | Read-your-writes via own xid; RR ignores later commit; RC sees it next statement | `exec::tests::read_your_writes_via_own_xid_in_txn`, `transactions::repeatable_read_does_not_see_concurrent_commit`, `transactions::read_committed_sees_concurrent_commit_next_statement`, `exec::tests::another_session_cannot_see_uncommitted_rows` |
| 4 | Writers serialized; no write-write conflict; concurrent reader-vs-writer isolation unchanged | `concurrency::*`, `transactions::*` (all green unchanged) |
| 5 | All SP1–SP4 gates green; 212 tests pass with identical behavior; conformance ≥ baseline | full gauntlet (Step 1) + conformance (Step 2) |

If any row lacks a green test, add it before finishing.

- [ ] **Step 4: commit (if any corpus/docs changed).**

```bash
git add -A
git commit -m "test(sp5): conformance + success-criteria traceability; gauntlet green"
```

---

## Final review (after all tasks)

Dispatch a final code-reviewer over the whole SP5 diff (against pre-SP5 main), then run `superpowers:finishing-a-development-branch`. Review focus:

- **Visibility correctness:** `satisfies_mvcc` exactly matches PG semantics at the boundaries — `xid >= xmax` and `xid ∈ xip` are invisible; own xid visible; a committed-visible `xmax` hides, an aborted/running/after-snapshot `xmax` does not; own delete (xmax = own) hides from self.
- **Write-through invariant:** every write goes through `execute_write` → the session's single batch; the only commit point is the clog `Committed` entry; a crash between version-write and clog-commit leaves the rows invisible (recovery test proves it).
- **Lock discipline:** reads and in-transaction read-only statements never take the writer lock; a writing transaction holds it from first write to COMMIT/ROLLBACK; the `OwnedMutexGuard` is released exactly once (drop on commit/rollback); no guard is held across a point where the same task would re-acquire it (no self-deadlock). Autocommit acquires and releases within the statement.
- **Snapshot timing:** RC refreshes per statement (reads and the read-phase of UPDATE/DELETE); RR fixes at BEGIN; autocommit takes a fresh snapshot per statement.
- **next_xid durability:** persisted in the first-write batch (explicit txn) and the commit batch (autocommit), so a restart never reuses an xid.
- **No `unsafe`, no `unwrap` on fallible paths, no panic on I/O** (KvError → SQLSTATE); `expect()` only on poisoned mutexes / provably-infallible slices.
- **Behavior identity:** the 212 SP4 tests pass unchanged; the concurrency profile change (writers serialize at transaction granularity) is the only intended difference and does not affect any committed result.
