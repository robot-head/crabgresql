# SP16 / D3c — Cross-range 2PC (in-process core) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the cross-range `0A000` rejection with an atomic two-phase commit so a transaction writing to tables in different ranges commits all-or-nothing under snapshot isolation, with a single global commit decision in range 0 — in-process (`MultiRangeCluster` / `RangeRouter`) only.

**Architecture:** Tuples keep their **per-range local xid `Li`** (no re-stamping, no write-buffering). A `Prepared(Li → G)` **clog marker** ties each participant's local xid to a global txn `G`, written durably alongside the rows; **at the moment a range joins `G`, its `Li` is deregistered from that range's ProcArray running-set** so the local snapshot stops gating the row — range 0's global clog becomes the *sole* arbiter, and both ranges flip visible together at the single `Committed(G)` instant. A global-aware resolver derefs `clog(Li) = Prepared(→G)` to range 0's global clog + a global snapshot. Single-range txns are byte-for-byte unchanged.

**Tech Stack:** Rust 2024, `mvcc`/`executor`/`cluster` crates, openraft (one group per range, reused per participant). No new shipped dependency. Tests under cargo-nextest in the `MultiRangeCluster` in-process harness + the jepsen_bank checker.

**Spec:** `docs/superpowers/specs/2026-06-14-crabgresql-sp16-d3c-crossrange-2pc-core-design.md`
**Branch:** `sp16-d3c-crossrange-2pc-core` (created, stacked on SP15).

---

## The corrected visibility model (read before any task)

This is the load-bearing correctness core; an adversarial review caught two earlier framings as wrong. The rules:

1. **A tuple's `xmin`/`xmax` are always per-range LOCAL xids `Li` (`< GLOBAL_XID_BASE`).** No tuple is ever stamped with a global xid. The global `G` lives only in (a) a clog VALUE `Prepared(Li → G)` and (b) range 0's global clog key `/0/clog/<G>`.
2. **When a range joins global txn `G` (the `Prepared(Li → G)` marker is written), `Li` is immediately deregistered from that range's ProcArray running-set** (`procarray.finish(Li)`). Consequence: for any future snapshot, `local_snapshot.is_running(Li)` is `false`, so `satisfies_mvcc`'s local gate no longer hides the row — visibility defers entirely to the resolver. This is what makes both ranges flip **atomically** at `Committed(G)` (the earlier plan deregistered at commit-time, per range, sequentially → half-applied reads; deregister-at-PREPARE fixes it).
3. **The resolver** (`global_status`), given local xid `Li`, reads this range's clog: a terminal status → today's behavior; `Prepared(G)` → consult range 0's global clog for `G`, gated by the reader's **global snapshot** (`G` running as of my global snapshot ⇒ in-doubt ⇒ invisible; else range 0's global-clog status for `G`).
4. **`satisfies_mvcc` itself is unchanged.** Its `own`/`is_running`/`status` structure is correct *given* rule 2 (deregistered `Li` ⇒ `is_running(Li)=false` ⇒ it reaches `status(Li)=global_status(Li)`). The intelligence is entirely in the `status` closure + the deregister.
5. **Read-your-writes** within a cross-range txn works via the per-range own-xid short-circuit: each range's read uses *that range's* session whose `own = Li`, and `satisfies_mvcc`'s `Some(Li)==own` check fires **before** the resolver. No `own_global` plumbing is needed (a participant only ever reads its own range's rows, stamped with its own `Li`). T3 adds a test proving this.
6. **Single-range is untouched.** A single-range engine has no GTM (rule below), never writes a `Prepared` marker, never deregisters early; `global_status` degrades to today's `clog::get`.

**Refinement vs the spec (deferred to SP17, recorded in T7):** the spec listed a durable txn record (`/0/txn/<G>`) and an active range-0 recovery sweep (Component 1, T5). **In-process they are unnecessary and are deferred to SP17:** the only "crash" in-process is the coordinator (`RangeRouter`) being dropped, which runs `SqlSession::Drop` on every participant session (deregister + release locks) — so presumed-abort + the global-clog-as-decision satisfies both arms of criterion 5 for the in-process fault model. A real node crash that strands a remote participant's locks is SP17's problem (where the durable txn record + sweep land). T7 updates the spec Non-goals to reflect this.

---

## Key constants & shapes

- `mvcc::xid::GLOBAL_XID_BASE: Xid = 1 << 63` (T1).
- `mvcc::clog::XidStatus::Prepared(u64)` — clog value `[S_PREPARED][G: u64 BE]` (T1).
- `executor::gtm::Gtm` (T2) — range 0's global xid allocator + in-memory global running-set + global snapshot. **`pub(crate)` in executor; the cluster crate never names it** — it goes through `SqlEngine` wrapper methods (T2).
- `SqlEngine` gains `gtm: Option<Arc<Gtm>>` (T2/T3): `Some(shared)` on **every** range engine of a multi-range cluster (so any range can resolve a `Prepared` row and the coordinator can drive range 0); `None` on a single-range engine.
- `SqlEngine` pub methods (T2): `begin_global() -> u64`, `commit_global_decision(g, XidStatus) -> Result<(), ExecError>`, `finish_global(g)`, `global_snapshot() -> Snapshot`. (Panic/error if called when `gtm` is `None` — only the coordinator calls them.)
- `SqlSession` pub participant API (T3): `join_global(g)`, `local_xid() -> Option<u64>`, `commit_release()`, `abort_release()`, `ensure_began()`.
- `RangeRouter::Pin::Global { ranges: BTreeSet<RangeId>, g: u64 }` (T4) — `Pin` **loses `Copy`**.

**Verify-each-task convention.** Every task ends with `cargo fmt --all`, `cargo clippy -p <crate> --all-targets -- -D warnings`, and the task's `cargo nextest run`. All green.

---

## Task 1: MVCC foundation — `Prepared(G)` clog state + `GLOBAL_XID_BASE`

**Files:** Modify `crates/mvcc/src/xid.rs`, `crates/mvcc/src/clog.rs`.

- [ ] **Step 1: Failing tests** — add to `clog.rs mod tests`:

```rust
    #[test]
    fn prepared_carries_global_xid_roundtrip() {
        let kv = MemKv::new();
        kv.write_batch(&[put_op(7, XidStatus::Prepared(crate::xid::GLOBAL_XID_BASE + 3))]).expect("put");
        assert_eq!(get(&kv, 7).expect("get"), XidStatus::Prepared(crate::xid::GLOBAL_XID_BASE + 3));
    }
    #[test]
    fn truncated_prepared_value_errors_not_panics() {
        let kv = MemKv::new();
        kv.write_batch(&[kv::WriteOp::Put { key: kv::key::clog_key(9), value: vec![3] }]).expect("put");
        assert!(get(&kv, 9).is_err());
    }
```

Add to `xid.rs` (create `#[cfg(test)] mod tests`):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn global_base_is_top_bit_and_above_realistic_local_xids() {
        assert_eq!(GLOBAL_XID_BASE, 1u64 << 63);
        assert!(1_000_000u64 < GLOBAL_XID_BASE);
    }
}
```

(Note: do NOT add a `GLOBAL_XID_BASE >= GLOBAL_XID_BASE` assert — clippy `eq_op` fails it under `-D warnings`.)

- [ ] **Step 2: Run red** — `cargo nextest run -p mvcc clog::tests::prepared xid::tests` → FAIL.
- [ ] **Step 3:** Add to `xid.rs`:

```rust
/// Cross-range (global) transaction ids are allocated from this reserved high
/// half of the u64 space; every per-range local xid is `< GLOBAL_XID_BASE`. Keeps
/// range 0's global-clog keys disjoint from its own local-clog keys.
pub const GLOBAL_XID_BASE: Xid = 1 << 63;
```

- [ ] **Step 4:** In `clog.rs`, add the variant + constant and update `get`/`put_op`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XidStatus { InProgress, Committed, Aborted, Prepared(u64) }

const S_IN_PROGRESS: u8 = 0;
const S_COMMITTED: u8 = 1;
const S_ABORTED: u8 = 2;
const S_PREPARED: u8 = 3;

pub fn get(kv: &dyn Kv, xid: u64) -> Result<XidStatus, KvError> {
    match kv.get(&kv::key::clog_key(xid))? {
        None => Ok(XidStatus::InProgress),
        Some(b) => match b.first() {
            Some(&S_COMMITTED) => Ok(XidStatus::Committed),
            Some(&S_ABORTED) => Ok(XidStatus::Aborted),
            Some(&S_IN_PROGRESS) => Ok(XidStatus::InProgress),
            Some(&S_PREPARED) => {
                let g: [u8; 8] = b.get(1..9)
                    .ok_or_else(|| KvError::CorruptRow("prepared clog missing global xid".into()))?
                    .try_into().expect("slice 1..9 is 8 bytes");
                Ok(XidStatus::Prepared(u64::from_be_bytes(g)))
            }
            _ => Err(KvError::CorruptRow("bad clog status byte".into())),
        },
    }
}

pub fn put_op(xid: u64, status: XidStatus) -> WriteOp {
    let value = match status {
        XidStatus::InProgress => vec![S_IN_PROGRESS],
        XidStatus::Committed => vec![S_COMMITTED],
        XidStatus::Aborted => vec![S_ABORTED],
        XidStatus::Prepared(g) => {
            let mut v = Vec::with_capacity(9);
            v.push(S_PREPARED);
            v.extend_from_slice(&g.to_be_bytes());
            v
        }
    };
    WriteOp::Put { key: kv::key::clog_key(xid), value }
}
```
- [ ] **Step 5: Run green** — `cargo nextest run -p mvcc` → PASS (existing `visibility.rs` tests unchanged: `satisfies_mvcc` and its `status_map` stub still only produce terminal states).
- [ ] **Step 6: Commit** — `cargo fmt --all && cargo clippy -p mvcc --all-targets -- -D warnings`, then `git commit -m "feat(sp16): Prepared(G) clog state + GLOBAL_XID_BASE"`.

---

## Task 2: GTM in range 0 + `SqlEngine` global seams

**Files:** Create `crates/executor/src/gtm.rs`; modify `crates/executor/src/lib.rs` (`mod gtm;` + `SqlEngine.gtm` field + wrapper methods); modify `crates/kv/src/key.rs` (`meta_next_global_xid_key`).

- [ ] **Step 1:** Add `meta_next_global_xid_key()` to `kv/key.rs` (`system_prefix("meta") + b"next_global_xid"`) + a distinctness key test.
- [ ] **Step 2:** Create `crates/executor/src/gtm.rs` (mirrors `ProcArray` for the global xid space):

```rust
//! Range 0's Global Transaction Manager: allocates monotonic GLOBAL xids
//! (>= GLOBAL_XID_BASE, disjoint from every range's local xids), tracks the
//! in-flight global set, and builds the global snapshot a cross-range reader
//! resolves Prepared(->G) tuples against. Backed by range 0's store; the counter
//! is max-merged by the state machine exactly like ProcArray's next_xid.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use kv::Kv;
use mvcc::visibility::Snapshot;
use mvcc::xid::GLOBAL_XID_BASE;
use zerocopy::byteorder::big_endian::U64;
use zerocopy::{FromBytes, IntoBytes};
use crate::error::ExecError;

struct Inner { next_global: u64, running: BTreeSet<u64> }

pub(crate) struct Gtm { inner: Mutex<Inner>, kv: Arc<dyn Kv> }

impl Gtm {
    pub fn open(kv: Arc<dyn Kv>) -> Result<Self, ExecError> {
        let next = match kv.get(&kv::key::meta_next_global_xid_key())? {
            Some(b) => U64::read_from_prefix(b.as_slice())
                .map_err(|_| kv::KvError::CorruptRow("next_global_xid not u64".into()))?.0.get(),
            None => GLOBAL_XID_BASE,
        };
        Ok(Self { inner: Mutex::new(Inner { next_global: next.max(GLOBAL_XID_BASE), running: BTreeSet::new() }), kv })
    }
    pub fn begin_global(&self) -> u64 {
        let mut g = self.inner.lock().expect("gtm");
        let xid = g.next_global; g.next_global = xid + 1; g.running.insert(xid); xid
    }
    pub fn next_global_xid_op(&self) -> kv::WriteOp {
        let next = self.inner.lock().expect("gtm").next_global;
        kv::WriteOp::Put { key: kv::key::meta_next_global_xid_key(), value: U64::new(next).as_bytes().to_vec() }
    }
    pub fn reseed_from_applied(&self) -> Result<(), ExecError> {
        let durable = match self.kv.get(&kv::key::meta_next_global_xid_key())? {
            Some(b) => U64::read_from_prefix(b.as_slice())
                .map_err(|_| kv::KvError::CorruptRow("next_global_xid not u64".into()))?.0.get(),
            None => GLOBAL_XID_BASE,
        };
        let mut g = self.inner.lock().expect("gtm");
        g.next_global = g.next_global.max(durable.max(GLOBAL_XID_BASE)); Ok(())
    }
    /// Consumed ONLY by `global_status` (never handed to satisfies_mvcc): xip is
    /// BTreeSet-sorted for the resolver's binary_search; xmin is unused.
    pub fn global_snapshot(&self) -> Snapshot {
        let g = self.inner.lock().expect("gtm");
        let xip: Vec<u64> = g.running.iter().copied().collect();
        let xmax = g.next_global;
        Snapshot { xmin: xip.first().copied().unwrap_or(xmax), xmax, xip }
    }
    pub fn finish_global(&self, g: u64) { self.inner.lock().expect("gtm").running.remove(&g); }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kv::MemKv;
    #[test]
    fn allocates_disjoint_monotonic_global_ids() {
        let gtm = Gtm::open(Arc::new(MemKv::new())).expect("open");
        let (a, b) = (gtm.begin_global(), gtm.begin_global());
        assert!(a >= GLOBAL_XID_BASE && b == a + 1);
        assert_eq!(gtm.global_snapshot().xip, vec![a, b]);
        gtm.finish_global(a);
        assert_eq!(gtm.global_snapshot().xip, vec![b]);
    }
    #[test]
    fn reseed_lifts_counter_and_never_regresses() {
        let kv = Arc::new(MemKv::new());
        let gtm = Gtm::open(kv.clone() as Arc<dyn Kv>).expect("open");
        assert_eq!(gtm.begin_global(), GLOBAL_XID_BASE);
        kv.put(kv::key::meta_next_global_xid_key(), (GLOBAL_XID_BASE + 50).to_be_bytes().to_vec()).expect("put");
        gtm.reseed_from_applied().expect("reseed");
        assert_eq!(gtm.begin_global(), GLOBAL_XID_BASE + 50);
    }
}
```

(Confirm the `U64::read_from_prefix(...)?.0.get()` shape against `procarray.rs:36-39` — if that returns a `(U64, &[u8])` tuple, `.0.get()` is correct; match the exact `procarray` idiom.)
- [ ] **Step 3:** In `lib.rs`: `mod gtm;`. Add `pub(crate) gtm: Option<Arc<gtm::Gtm>>` to `SqlEngine` (default `None` in `with_kv`/`new`; `replicated` keeps `None` unless wired by the cluster — see below). Add **pub wrapper methods** so the cluster crate never names `Gtm`:

```rust
    /// Allocate a global (cross-range) txn id. Coordinator-only (range 0's engine).
    pub fn begin_global(&self) -> u64 {
        self.gtm.as_ref().expect("begin_global on a non-GTM engine").begin_global()
    }
    /// Durably record the global decision (Committed/Aborted) for `g` in range 0's
    /// group, folding the global next-id advance. The atomic commit instant.
    pub async fn commit_global_decision(&self, g: u64, status: mvcc::clog::XidStatus) -> Result<(), ExecError> {
        let gtm = self.gtm.as_ref().expect("commit_global_decision on a non-GTM engine");
        self.committer.commit(vec![mvcc::clog::put_op(g, status), gtm.next_global_xid_op()]).await
    }
    /// Deregister a decided global txn from the in-memory running-set.
    pub fn finish_global(&self, g: u64) { self.gtm.as_ref().expect("gtm").finish_global(g); }
    /// The current global snapshot (for capturing a cross-range reader's horizon).
    pub fn global_snapshot(&self) -> mvcc::visibility::Snapshot {
        self.gtm.as_ref().map(|g| g.global_snapshot()).unwrap_or(NO_GLOBAL_SNAPSHOT())
    }
```

Add a **no-op global snapshot** helper (used by single-range / non-GTM reads so the `Prepared` branch is unreachable): `fn NO_GLOBAL_SNAPSHOT() -> Snapshot { Snapshot { xmin: GLOBAL_XID_BASE, xmax: GLOBAL_XID_BASE, xip: vec![] } }` (any global xid `g >= xmax` ⇒ resolver returns InProgress, but no `Prepared` tuples exist single-range, so it is never consulted).

Add a setter the cluster uses to share one GTM across all range engines: `pub fn set_gtm(&mut self, gtm: Arc<gtm::Gtm>)` — OR build it in `replicated`/`MultiRangeCluster` construction. **Decision:** `MultiRangeCluster::new` builds **one** `Arc<Gtm>` over range 0's store and injects it (via a new `SqlEngine::replicated_with_gtm(...)` or a post-build `set_gtm`) into **every** range's engine. Specify whichever is cleaner after reading `cluster.rs`'s engine construction; the constraint is *one shared GTM instance, reachable from every range engine and the coordinator*.

- [ ] **Step 4: Run + commit** — `cargo nextest run -p kv -p executor gtm:: key::tests` → PASS; `cargo fmt --all && cargo clippy -p kv -p executor --all-targets -- -D warnings`; `git commit -m "feat(sp16): GTM in range 0 + SqlEngine global-decision seams"`.

---

## Task 3: Global resolver (all clog sites) + participant session API + deregister-at-prepare

**Files:** Modify `crates/executor/src/exec.rs`, `crates/executor/src/session.rs`. **Read `exec.rs` fully first.**

### 3A — the resolver, threaded through ALL clog-read sites

`mvcc::clog::get` is read at **five** sites; every one must route through the resolver (an `UPDATE`/`DELETE` re-check that misses a site silently mis-classifies a `Prepared` row):
1. `scan_live` (≈ exec.rs:398) — read visibility.
2. `find_visible_one` (≈ exec.rs:305) — EvalPlanQual row re-fetch under UPDATE/DELETE/FOR-UPDATE.
3. `eval_plan_qual`'s `changed_since_snapshot` (≈ exec.rs:339) — concurrent-delete serialization check.
4. `execute_read` / `execute_read_locking` entry points (which build the closure they pass down).

Add the resolver:

```rust
// exec.rs. local = this range's store; global = range 0's store (catalog_kv);
// gsnap = the cross-range reader's global snapshot horizon.
pub(crate) fn global_status<'a>(
    local: &'a dyn kv::Kv, global: &'a dyn kv::Kv, gsnap: &'a mvcc::visibility::Snapshot,
) -> impl Fn(u64) -> Result<mvcc::clog::XidStatus, kv::KvError> + 'a {
    use mvcc::clog::XidStatus;
    move |xid| match mvcc::clog::get(local, xid)? {
        XidStatus::Prepared(g) => {
            if g >= gsnap.xmax || gsnap.xip.binary_search(&g).is_ok() {
                Ok(XidStatus::InProgress) // global txn in-doubt as of my global snapshot
            } else {
                Ok(mvcc::clog::get(global, g)?) // settled: range 0's global decision
            }
        }
        other => Ok(other),
    }
}
```

Thread `(global: &dyn kv::Kv, gsnap: &Snapshot)` into `scan_live`, `find_visible_one`, `eval_plan_qual`, `execute_read`, `execute_read_locking` and replace each bare `|x| clog::get(local, x)` with `global_status(local, global, gsnap)`. For a single-range engine the caller passes `global = local (== catalog_kv)` and `gsnap = NO_GLOBAL_SNAPSHOT()`, so the `Prepared` arm is unreachable and behavior is byte-for-byte unchanged. The session supplies `global` (its `catalog_kv` = range 0's store) and `gsnap` (see 3C).

### 3B — participant session API + the deregister-at-prepare + the per-write marker

Add to `SqlSession`: `global_xid: Option<u64>`. Extract `finish_current_txn(&mut self)` = the **Drop body** (`if let Some(xid)=self.local_xid() { self.procarray.finish(xid); self.lockmgr.release_all(xid); } self.state = TxnState::Idle;`) and have `Drop` call it. (It is the Drop body, NOT extracted from `commit_cmd`, which is entangled with the clog write.) Then:

```rust
    /// Mark this session a participant of global txn `g`. If it has already done a
    /// write (local xid `L`), write the Prepared(L -> g) marker AND deregister `L`
    /// from the ProcArray running-set so the local snapshot no longer gates its
    /// rows (range 0's global clog is now the sole arbiter). Idempotent.
    pub async fn join_global(&mut self, g: u64) -> Result<(), ExecError> {
        self.global_xid = Some(g);
        if let Some(local) = self.local_xid() {
            self.committer.commit(vec![mvcc::clog::put_op(local, mvcc::clog::XidStatus::Prepared(g))]).await?;
            self.procarray.finish(local); // deregister-at-PREPARE (the atomicity linchpin)
        }
        Ok(())
    }
    pub fn local_xid(&self) -> Option<u64> {
        match &self.state { TxnState::InTransaction(c) | TxnState::Failed(c) => c.xid, TxnState::Idle => None }
    }
    /// Begin a held txn on this session if it is Idle and the connection is in a
    /// transaction — so a participant's first DML is HELD, never autocommitted
    /// (closes the D3a Pin::Open looseness). Reuses `begin`.
    pub async fn ensure_began(&mut self) -> Result<(), ExecError> {
        if matches!(self.state, TxnState::Idle) { self.begin(None).await?; }
        Ok(())
    }
    pub fn commit_release(&mut self) { self.finish_current_txn(); } // rows already Prepared+durable; Committed(G) makes them visible
    pub fn abort_release(&mut self)  { self.finish_current_txn(); } // absent/Aborted(G) keeps them invisible
```

In `run_write`'s **in-txn** branch, at the splice point `session.rs:421` (`self.committer.commit(ops)`), when `self.global_xid.is_some()` push the marker into the same durable batch so the row carries it from the start, and deregister on the FIRST such write (covers the case where the escalation trigger IS the first write on this range, so `join_global` had no `local_xid` to backfill):

```rust
        if let Some(g) = self.global_xid {
            ops.push(mvcc::clog::put_op(xid, mvcc::clog::XidStatus::Prepared(g)));
        }
        // ... existing next_xid fold ...
        self.committer.commit(ops).await?;
        if self.global_xid.is_some() { self.procarray.finish(xid); } // deregister-at-prepare for first write
```

(A participant in a `Pin::Global` txn never runs `commit_cmd` — the coordinator drives `commit_release` — so no per-participant `Committed(Li)` is ever written.)

### 3C — where `gsnap` comes from (RR-correct)

Extend `TxnCtx` with `global_snapshot: Option<Snapshot>`. In `begin`: if `gtm` is present, capture `engine.global_snapshot()` — for **RR** store it in `TxnCtx` (fixed for the txn's life); for **RC** it is re-captured per statement in `read_context`/`run_write` alongside the local snapshot. Pass the captured `gsnap` (RR: the stored one; RC/autocommit: a fresh `global_snapshot()`; no GTM: `NO_GLOBAL_SNAPSHOT()`) into every `execute_read*`/`scan_live` call. The session reaches the GTM via its engine handle (the session already holds `catalog_kv`; add a `gtm: Option<Arc<Gtm>>` field to `SqlSession` mirroring the engine, set in `connect`).

### Tests

- [ ] **Resolver unit test (pure, the concrete first failing test):**

```rust
// exec.rs mod tests — global_status with two MemKv stores.
#[test]
fn global_status_derefs_prepared_to_range0_global_clog() {
    use mvcc::clog::{put_op, XidStatus}; use mvcc::xid::GLOBAL_XID_BASE; use kv::{Kv, MemKv};
    let (local, global) = (MemKv::new(), MemKv::new());
    let li = 5u64; let g = GLOBAL_XID_BASE + 1;
    local.write_batch(&[put_op(li, XidStatus::Prepared(g))]).unwrap();
    // G in-doubt (not in global clog, gsnap says running) => InProgress (invisible)
    let running = mvcc::visibility::Snapshot { xmin: g, xmax: g + 1, xip: vec![g] };
    assert_eq!(global_status(&local, &global, &running)(li).unwrap(), XidStatus::InProgress);
    // G committed + settled (gsnap moved past it) => Committed (visible)
    global.write_batch(&[put_op(g, XidStatus::Committed)]).unwrap();
    let settled = mvcc::visibility::Snapshot { xmin: g + 2, xmax: g + 2, xip: vec![] };
    assert_eq!(global_status(&local, &global, &settled)(li).unwrap(), XidStatus::Committed);
    // A plain local xid is unaffected.
    local.write_batch(&[put_op(3, XidStatus::Committed)]).unwrap();
    assert_eq!(global_status(&local, &global, &settled)(3).unwrap(), XidStatus::Committed);
}
```

- [ ] Implement 3A/3B/3C; run `cargo nextest run -p executor` → PASS (resolver test + all existing executor/MVCC transaction tests unchanged). The participant *session* behavior (join_global → invisible → Committed(G) → visible, and cross-range read-your-writes) is exercised end-to-end at T4/T5 (it needs the coordinator); do not try to unit-test it without the router.
- [ ] **Commit** — `cargo fmt --all && cargo clippy -p executor --all-targets -- -D warnings`; `git commit -m "feat(sp16): global resolver across all clog sites + participant API + deregister-at-prepare"`.

---

## Task 4: Router coordinator — `Pin::Global`, escalation, atomic COMMIT

**Files:** Modify `crates/cluster/src/range/router.rs`, `crates/cluster/src/range/cluster.rs`.

**Pin loses `Copy`.** Change `#[derive(Debug, Clone, Copy, PartialEq, Eq)]` → `#[derive(Debug, Clone, PartialEq, Eq)]` and add the variant:

```rust
enum Pin {
    None,
    Open,
    Range(RangeId),
    Global { ranges: std::collections::BTreeSet<RangeId>, g: u64 },
}
```

Then fix every by-value/`Copy` use the compiler flags: `self.pin == Pin::None` (router.rs:209) → `matches!(self.pin, Pin::None)`; the `match self.pin` arms at 215, 225, 260 → `match &self.pin` (borrow), pulling out `r`/`p` by `*r`/`*p` and cloning the `BTreeSet` only where ownership is needed (use `std::mem::replace(&mut self.pin, Pin::None)` in the COMMIT/ROLLBACK arms where the pin is consumed, mirroring `commit_cmd`); `tx_status` (358-362) add the `Pin::Global` arm (`InTransaction`).

**Escalation** (the `Pin::Range(p)` arm, today's `0A000` at router.rs:260-269): when a table-bearing statement resolves to `r != p`, escalate as strictly-sequential single-borrow steps (you cannot hold `&mut` to two sessions of one `HashMap` at once):

```rust
Pin::Range(p) => {
    let p = *p;
    if let Some(r) = pinning && r != p {
        let g = self.engines[&0].begin_global();          // coordinator = range 0's engine
        self.session_mut(p).join_global(g).await?;         // backfill Prepared(Lp->g) + deregister
        self.ensure_began_on(r).await?;                    // HELD txn on r before its first write
        self.session_mut(r).join_global(g).await?;         // mark r a participant (no xid yet => no-op backfill; the first write writes the marker)
        let mut ranges = std::collections::BTreeSet::new(); ranges.insert(p); ranges.insert(r);
        self.pin = Pin::Global { ranges, g };
        return self.run_on(r, stmt).await;
    }
    self.run_on(p, stmt).await
}
Pin::Global { ranges, g } => {
    let g = *g;
    if let Some(r) = pinning && !ranges.contains(&r) {
        self.ensure_began_on(r).await?;
        self.session_mut(r).join_global(g).await?;
        if let Pin::Global { ranges, .. } = &mut self.pin { ranges.insert(r); }
    }
    let r = pinning.unwrap_or(0);
    self.run_on(r, stmt).await
}
```

Add `ensure_began_on(&mut self, r)` = `self.session_mut(r).ensure_began().await` (only meaningful for a LOCAL range; in-process every participant range is local). **Also** close the looseness for the *single-range* `Pin::Open → Pin::Range(p)` transition (router.rs:248-257): before running the first DML, `self.ensure_began_on(exec).await?` so a non-range-0 first DML is held, not autocommitted.

**COMMIT / ROLLBACK** under `Pin::Global { ranges, g }`:

```rust
Statement::Commit | Statement::Rollback => {
    let prev = std::mem::replace(&mut self.pin, Pin::None);
    match prev {
        Pin::Global { ranges, g } => {
            let decision = if matches!(stmt, Statement::Commit) { Committed } else { Aborted };
            self.engines[&0].commit_global_decision(g, decision).await?; // ONE range-0 append = atomic instant
            for r in &ranges { self.session_mut(*r).commit_release_or_abort(decision); } // release locks + deregister
            self.engines[&0].finish_global(g);
            Ok(QueryResult::Command { tag: tag_for(stmt) })
        }
        Pin::Range(p) => self.run_on(p, &commit_or_rollback).await, // single-range: unchanged
        Pin::Open | Pin::None => self.run_on(0, &commit_or_rollback).await,
    }
}
```

(Always write a **positive** `Aborted(G)` on ROLLBACK — not mere absence — so presumed-abort is a record, not indistinguishable from a lost commit.) `commit_release_or_abort` calls `commit_release()`/`abort_release()` per the decision.

**Cluster wiring** (`cluster.rs`): build one `Arc<Gtm>` over range 0's store in `MultiRangeCluster::new` and inject it into every range's engine (see T2). `RangeRouter::connect` already holds `engines[&0]` (range 0's leader engine, GTM-bearing) — the coordinator path needs nothing more in-process. Add a **test-only coordinator pause seam** for the crash test: an `Option<…>` hook on `RangeRouter` invoked *after* the last participant `join_global` and *before* `commit_global_decision`, so T5 can deterministically drop the router between staging and the decision (no sleep).

**Scope note:** the cross-range path is wired at the `MultiRangeCluster`/`RangeRouter` level only. The `gateway_local.rs`/`ServerNode`/`serve_routed` flip (criterion 8) needs the **cross-node** global-decision path (the gateway may not lead range 0) → **deferred to SP17**; SP16 flips the in-process `router.rs::a_transaction_may_not_span_ranges` test instead. T7 records this scope in the spec.

- [ ] **TDD:** rewrite `a_transaction_may_not_span_ranges` (router.rs:420) as the concrete first failing test — `BEGIN; INSERT a (range 0); INSERT b (range 1); COMMIT;` then `SELECT`s read both back; a sibling test runs the same with `ROLLBACK` and asserts neither row. Run red, implement, run green.
- [ ] **Commit** — `cargo fmt --all && cargo clippy -p cluster --all-targets -- -D warnings`; `git commit -m "feat(sp16): cross-range 2PC coordinator (Pin::Global, escalation, atomic global commit)"`.

---

## Task 5: Cross-range correctness tests (the proof)

**Files:** new `crates/cluster/tests/crossrange_2pc.rs` (UAC-safe name — no `setup/install/update/patch/upgrad`); extend `crates/cluster/tests/jepsen_bank.rs`.

All tests drive cross-range work through a `RangeRouter` over a `MultiRangeCluster` (the only path with the 2PC coordinator), event-paced via `c.wait_for_leader(r)` — no sleep.

- [ ] **Atomic commit + abort (criterion 3):** `BEGIN; INSERT a@range0; INSERT b@range1; COMMIT;` → both visible through fresh routers; ROLLBACK variant → neither (and still neither after the GTM advances well past `G`, proving the positive `Aborted(G)`).
- [ ] **Cross-range read-your-writes (criteria, rule 5):** within the txn, after both INSERTs and before COMMIT, `SELECT FROM a` and `SELECT FROM b` on the same router both return the row (own-xid short-circuit).
- [ ] **In-doubt invisibility (criterion 4):** using the coordinator pause seam, hold a cross-range txn staged-but-undecided; a **concurrent** router's reads on each participant range see **no** `G`-row; release the seam (write `Committed(G)`) → both see it. Assert the both-or-neither across the window between the decision write and the participants' `commit_release`.
- [ ] **Crash-before-decision (criterion 5):** stage both participants, then drop the coordinator router (its `SqlSession::Drop`s release locks) before `commit_global_decision` → a fresh router sees neither row (presumed abort). A second variant writes `Committed(G)` first, then drops → both visible.
- [ ] **Cross-range UPDATE (exercises `find_visible_one`/`eval_plan_qual`):** a cross-range txn that UPDATEs rows in two ranges commits atomically and the updated values read back through both — proving sites 2/3 of the resolver, which INSERT-only tests never hit.
- [ ] **jepsen_bank cross-range (criterion 6):** partition accounts across `acct_a` (range 1) and `acct_b` (range 2); transfers between an `acct_a` and an `acct_b` account are cross-range and go through the `RangeRouter` 2PC path; the conservation checker under the crash/partition nemesis proves all-or-nothing across two Raft groups. `40P01`/timeout aborts are clean `Fail`s.
- [ ] **Commit** — `cargo fmt --all && cargo clippy -p cluster --all-targets -- -D warnings`; `git commit -m "test(sp16): cross-range atomic commit + RYW + in-doubt + crash + UPDATE + jepsen_bank"`.

---

## Task 6: jepsen_elle cross-range — OPTIONAL hardening

As before: extend the list-append workload so a txn appends to keys in different ranges with the existing `FOR UPDATE` anchor (SI, not SSI). If too large for the slice, **defer to SP17** and record it — `jepsen_bank` (T5) is the load-bearing atomicity proof.

---

## Task 7: Gauntlet + traceability + spec reconciliation + finish

- [ ] **Reconcile the spec with the in-process refinements** (do NOT silently drop promised components): add a "Refinements (deferred to SP17)" note to the spec — the durable `/0/txn/<G>` record + active recovery sweep, and the `ServerNode`/`gateway_local` `0A000` flip, move to SP17 (the cross-node slice); SP16 proves recovery implicitly (coordinator-drop self-release) and flips the in-process router test. Update Non-goals.
- [ ] **UAC guard** for the new `crossrange_2pc.rs` (clean) + CLAUDE.md audit entry; run `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` → empty.
- [ ] **Traceability table** mapping each of the 8 criteria to its test (criterion 8's `gateway_local` flip → marked SP17).
- [ ] **Full gauntlet:** `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo nextest run --workspace`; `cargo test --workspace --doc`; `cargo deny check bans licenses`; `bash ./scripts/check-no-native.sh` (pre-existing `windows-sys` flag is Windows-local only — Linux CI clean). All green.
- [ ] **Commit** (`docs(sp16): traceability + spec reconciliation + audit`); then **superpowers:finishing-a-development-branch** — standing preference option **2** (push + PR, base `main`).

---

## Self-Review notes (for the executor)

- **The visibility model section at the top is normative** — re-read it before T3. The deregister-at-prepare (rule 2) is the atomicity linchpin; without it the two ranges flip non-atomically.
- **All FIVE clog-read sites** (T3) must route through `global_status`, or cross-range UPDATE/DELETE mis-resolves. The T5 cross-range UPDATE test is the gate.
- **Single-range untouched:** no-GTM engine ⇒ `NO_GLOBAL_SNAPSHOT()` ⇒ `Prepared` branch unreachable ⇒ byte-for-byte today's behavior. The workspace regression suite (criterion 7) is the gate.
- **`ensure_began_on`** closes the existing D3a autocommit looseness for BOTH the single-range non-range-0-first-DML case and cross-range escalation — a participant's first write must be HELD, never autocommitted, or the `Prepared` backfill would retroactively hide an already-visible row.
- **No-sleep:** the only determinism point is the coordinator pause seam (T4) — an explicit hook, not a timer.
- **If the participant write-path / resolver threading proves more invasive than the 5 sites suggest, that is a BLOCKED escalation, not a place to guess.** Range-0 coordinator funnel is the known accepted scaling limit.
