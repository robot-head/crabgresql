# SP6: Concurrent writers — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove SP5's transaction-scoped writer lock so transactions write concurrently, with PostgreSQL's row locks (in-memory `RowLockManager`), block-and-retry + EvalPlanQual (READ COMMITTED re-find / REPEATABLE READ `40001`), `SELECT FOR UPDATE`/`FOR SHARE`, atomic concurrent rowid allocation, and eager wait-for-graph deadlock detection (`40P01`).

**Architecture:** Two new in-memory engine services join `ProcArray`: `executor::seq::SequenceManager` (atomic, monotonic-durable per-table rowid allocation) and `executor::lockmgr::RowLockManager` (per-row exclusive/shared locks + per-xid async wake + wait-for-graph). The write path becomes async: per target row it does `try_acquire → (block on conflict, deadlock-checked) → recheck on-disk xmax → EvalPlanQual → apply`. Reads never lock; clog/ProcArray/`satisfies_mvcc`/write-through are reused unchanged. Counters (`next_xid`, per-table seq) move to eager monotonic persistence because concurrent batches could otherwise land out of order and regress them.

**Tech Stack:** Rust 2024. `tokio::sync::Notify` for lock waits. Crates: `pgparser` (FOR UPDATE/SHARE), `executor` (seq, lockmgr, session, exec), `mvcc`/`kv` unchanged except reuse.

**Spec:** `docs/superpowers/specs/2026-06-12-crabgresql-sp6-concurrent-writers-design.md`

---

## File structure

```
crates/pgparser/src/{ast.rs,token.rs,parser.rs}  # SelectStmt.locking + FOR UPDATE/SHARE grammar
crates/pgparser/tests/libpg_query_oracle.rs       # accept cases
crates/executor/src/seq.rs                        # NEW: SequenceManager (atomic rowid alloc, monotonic persist)
crates/executor/src/lockmgr.rs                    # NEW: RowLockManager (locks, async wait, wait-for graph, deadlock)
crates/executor/src/procarray.rs                  # begin_write persists next_xid eagerly + monotonically
crates/executor/src/error.rs                      # + SerializationFailure (40001), Deadlock (40P01)
crates/executor/src/lib.rs                        # SqlEngine: drop writer_lock; add seq + lockmgr + catalog_lock
crates/executor/src/session.rs                    # no writer lock; async write path; release_all on commit/rollback
crates/executor/src/exec.rs                       # async execute_write conflict loop; FOR UPDATE/SHARE locking
crates/executor/tests/concurrency.rs              # deterministic concurrent-writer tests
crates/executor/tests/end_to_end.rs               # wire-level concurrency
```

Task order (each ends workspace-green): parser FOR UPDATE/SHARE → SequenceManager → RowLockManager → ProcArray eager persist + error variants → **cutover** (remove lock, wire services, async conflict loop) → FOR UPDATE/SHARE execution → deterministic concurrency tests → gauntlet.

---

### Task 1: Parser — `SELECT ... FOR UPDATE` / `FOR SHARE`

**Files:**
- Modify: `crates/pgparser/src/ast.rs`, `crates/pgparser/src/token.rs`, `crates/pgparser/src/parser.rs`, `crates/pgparser/tests/libpg_query_oracle.rs`

Additive grammar. `SelectStmt` gains an optional row-locking clause; the executor accepts it and (for now) ignores it (plain SELECT semantics) — actual locking lands in Task 6, keeping the workspace green meanwhile.

- [ ] **Step 1: AST.** In `crates/pgparser/src/ast.rs` add an enum and a field on `SelectStmt`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowLockStrength {
    ForUpdate,
    ForShare,
}
```

Add `pub locking: Option<RowLockStrength>,` as the last field of `SelectStmt`. (This breaks every `SelectStmt { ... }` literal — `grep -rn "SelectStmt {" crates/` and add `locking: None` to each, including the parser and any tests.)

- [ ] **Step 2: Keywords.** In `crates/pgparser/src/token.rs` add `For` and `Share` to the `Keyword` enum and its word mapping (`for`, `share`). `Update` already exists.

- [ ] **Step 3: Failing parser tests.** Add to the `parser.rs` test module:

```rust
    #[test]
    fn parses_select_for_update_and_share() {
        use crate::ast::RowLockStrength;
        match one("SELECT id FROM t FOR UPDATE") {
            Statement::Select(s) => assert_eq!(s.locking, Some(RowLockStrength::ForUpdate)),
            other => panic!("expected Select, got {other:?}"),
        }
        match one("SELECT id FROM t WHERE id > 1 FOR SHARE") {
            Statement::Select(s) => assert_eq!(s.locking, Some(RowLockStrength::ForShare)),
            other => panic!("expected Select, got {other:?}"),
        }
        match one("SELECT id FROM t") {
            Statement::Select(s) => assert_eq!(s.locking, None),
            other => panic!("expected Select, got {other:?}"),
        }
    }
```

Run: `cargo test -p pgparser -- parses_select_for_update` → FAIL.

- [ ] **Step 4: Grammar.** In the SELECT parser, after parsing ORDER BY / LIMIT (the trailing clauses), parse an optional locking clause. Find where the `SelectStmt` is built and, just before constructing it, add:

```rust
        let locking = if self.eat_keyword(Keyword::For) {
            if self.eat_keyword(Keyword::Update) {
                Some(crate::ast::RowLockStrength::ForUpdate)
            } else if self.eat_keyword(Keyword::Share) {
                Some(crate::ast::RowLockStrength::ForShare)
            } else {
                return Err(ParseError::new("expected UPDATE or SHARE after FOR", self.peek_pos()));
            }
        } else {
            None
        };
```

and set `locking` in the `SelectStmt { ... }` literal. (Adapt `eat_keyword`/`ParseError::new`/`peek_pos` to the real parser helpers — they exist from SP4.)

- [ ] **Step 5: Run** `cargo test -p pgparser` → all pass. Note: the executor's `execute_read`/`resolve_projection` don't read `locking`, so SELECTs still behave as before. `cargo test --workspace` stays green (the new field is `None` everywhere existing code builds a `SelectStmt`).

- [ ] **Step 6: Oracle.** In `crates/pgparser/tests/libpg_query_oracle.rs` add to the accepted array: `"SELECT id FROM t FOR UPDATE"`, `"SELECT id FROM t WHERE id > 1 FOR SHARE"`. Run `cargo test -p pgparser --features oracle` → pass.

- [ ] **Step 7: Commit.**

```bash
git add crates/pgparser
git commit -m "feat(pgparser): SELECT ... FOR UPDATE / FOR SHARE grammar"
```

---

### Task 2: `executor::seq::SequenceManager` — atomic, monotonic-durable rowid allocation

**Files:**
- Create: `crates/executor/src/seq.rs`
- Modify: `crates/executor/src/lib.rs` (`mod seq;`)

SP5's INSERT read the durable next-rowid inside the write batch under the global lock. With concurrent INSERTs that races (two read the same value) AND the durable value could regress if batches land out of order. `SequenceManager` fixes both: an in-memory per-table counter (seeded from disk once), allocation under a mutex, and the new value **persisted durably under that mutex before the rowid is handed out** so the durable seq is monotonic and a restart never reuses a rowid. Additive (not wired until the cutover).

- [ ] **Step 1: failing tests.** Create `crates/executor/src/seq.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use kv::MemKv;
    use std::sync::Arc;

    #[test]
    fn allocates_distinct_increasing_rowids() {
        let kv: Arc<dyn kv::Kv> = Arc::new(MemKv::new());
        let seq = SequenceManager::new();
        assert_eq!(seq.alloc(&*kv, 7, 3).expect("alloc"), 1); // rows 1,2,3
        assert_eq!(seq.alloc(&*kv, 7, 2).expect("alloc"), 4); // rows 4,5
        // a different table is independent
        assert_eq!(seq.alloc(&*kv, 8, 1).expect("alloc"), 1);
    }

    #[test]
    fn durable_seq_is_monotonic_and_seeds_a_fresh_manager() {
        let kv: Arc<dyn kv::Kv> = Arc::new(MemKv::new());
        let seq = SequenceManager::new();
        seq.alloc(&*kv, 7, 5).expect("alloc"); // consumes 1..=5, persists next=6
        // a brand-new manager (simulating restart) seeds from the durable value.
        let seq2 = SequenceManager::new();
        assert_eq!(seq2.alloc(&*kv, 7, 1).expect("alloc"), 6, "must not reuse 1..=5");
    }

    #[test]
    fn seeds_from_existing_durable_seq_key() {
        let kv: Arc<dyn kv::Kv> = Arc::new(MemKv::new());
        kv.write_batch(&[kv::WriteOp::Put {
            key: kv::key::seq_key(7),
            value: 42u64.to_be_bytes().to_vec(),
        }])
        .expect("seed");
        let seq = SequenceManager::new();
        assert_eq!(seq.alloc(&*kv, 7, 1).expect("alloc"), 42);
    }
}
```

Run `cargo test -p executor seq` → COMPILE FAIL.

- [ ] **Step 2: implement.** Prepend to `crates/executor/src/seq.rs`:

```rust
//! Atomic per-table rowid allocation for concurrent INSERTs. An in-memory
//! counter per table, seeded once from the durable `/0/seq/<table>` key, bumped
//! under a mutex, with the new value persisted durably *under the mutex* before
//! the rowid is returned — so the durable counter is monotonic and a restart
//! never reuses a rowid (a crash only leaks a gap, like a PostgreSQL sequence).

use std::collections::HashMap;
use std::sync::Mutex;

use kv::Kv;

use crate::error::ExecError;

pub(crate) struct SequenceManager {
    inner: Mutex<HashMap<catalog::TableId, u64>>,
}

impl SequenceManager {
    pub fn new() -> Self {
        Self { inner: Mutex::new(HashMap::new()) }
    }

    /// Reserve `count` consecutive rowids for `table` and return the first.
    /// Persists the new next-rowid durably before returning so it cannot regress.
    pub fn alloc(&self, kv: &dyn Kv, table: catalog::TableId, count: u64) -> Result<u64, ExecError> {
        let mut g = self.inner.lock().expect("seqmgr");
        let next = match g.get(&table) {
            Some(&n) => n,
            None => crate::exec::read_seq_kv(kv, table)?, // seed once from disk
        };
        let new_next = next + count;
        // Persist BEFORE releasing the lock and BEFORE handing out the rowid.
        kv.write_batch(&[kv::WriteOp::Put {
            key: kv::key::seq_key(table),
            value: new_next.to_be_bytes().to_vec(),
        }])?;
        g.insert(table, new_next);
        Ok(next)
    }
}
```

(`read_seq_kv` is the existing `exec.rs` helper returning 1 when unset. `catalog::TableId` is the table id type.)

- [ ] **Step 3: register + verify.** In `crates/executor/src/lib.rs` add `mod seq;`. Run `cargo test -p executor seq && cargo clippy -p executor --all-targets -- -D warnings`. If `SequenceManager` is flagged dead_code (not yet wired), add `#[allow(dead_code)]` with `// wired in by the SP6 cutover (Task 5)`.

- [ ] **Step 4: commit.**

```bash
git add crates/executor/src/seq.rs crates/executor/src/lib.rs
git commit -m "feat(executor): SequenceManager — atomic, monotonic-durable rowid allocation"
```

---

### Task 3: `executor::lockmgr::RowLockManager` — row locks, async waits, deadlock detection

**Files:**
- Create: `crates/executor/src/lockmgr.rs`
- Modify: `crates/executor/src/lib.rs` (`mod lockmgr;`)

The in-memory row-lock table: per `(table, rowid)` exclusive/shared locks, a per-xid async wake so a blocked writer resumes when the holder finishes, the wait-for graph, and **eager** cycle detection. Additive; unit-tested in isolation. This is the load-bearing concurrency primitive — get the lost-wakeup-free wait right.

- [ ] **Step 1: failing tests.** Create `crates/executor/src/lockmgr.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exclusive_conflicts_shared_coexists() {
        let m = RowLockManager::new();
        assert!(matches!(m.try_acquire(1, 1, LockMode::Exclusive, 10), Acquire::Acquired));
        // another xid conflicts on the exclusively-held row
        assert!(matches!(m.try_acquire(1, 1, LockMode::Exclusive, 11), Acquire::Conflict(10)));
        // a different row is free
        assert!(matches!(m.try_acquire(1, 2, LockMode::Shared, 11), Acquire::Acquired));
        // a second shared holder coexists
        assert!(matches!(m.try_acquire(1, 2, LockMode::Shared, 12), Acquire::Acquired));
        // an exclusive request conflicts with the shared holders
        assert!(matches!(m.try_acquire(1, 2, LockMode::Exclusive, 13), Acquire::Conflict(_)));
    }

    #[test]
    fn release_all_frees_rows_and_is_reacquirable() {
        let m = RowLockManager::new();
        m.try_acquire(1, 1, LockMode::Exclusive, 10);
        m.try_acquire(1, 2, LockMode::Exclusive, 10);
        m.release_all(10);
        assert!(matches!(m.try_acquire(1, 1, LockMode::Exclusive, 11), Acquire::Acquired));
        assert!(matches!(m.try_acquire(1, 2, LockMode::Exclusive, 11), Acquire::Acquired));
    }

    #[test]
    fn reacquire_by_same_holder_is_idempotent() {
        let m = RowLockManager::new();
        assert!(matches!(m.try_acquire(1, 1, LockMode::Exclusive, 10), Acquire::Acquired));
        assert!(matches!(m.try_acquire(1, 1, LockMode::Exclusive, 10), Acquire::Acquired));
    }

    #[tokio::test]
    async fn wait_for_resumes_when_holder_releases() {
        use std::sync::Arc;
        let m = Arc::new(RowLockManager::new());
        m.try_acquire(1, 1, LockMode::Exclusive, 10); // xid 10 holds
        let m2 = Arc::clone(&m);
        let waiter = tokio::spawn(async move {
            // xid 11 conflicts then waits for 10
            assert!(matches!(m2.try_acquire(1, 1, LockMode::Exclusive, 11), Acquire::Conflict(10)));
            m2.wait_for(11, 10).await.expect("not a deadlock");
        });
        // Give the waiter a moment to register, then release 10.
        tokio::task::yield_now().await;
        m.release_all(10);
        waiter.await.expect("waiter completes"); // must not hang
    }

    #[tokio::test]
    async fn wait_for_detects_a_two_cycle() {
        let m = RowLockManager::new();
        // 10 waits for 11 (ok), then 11 waits for 10 -> cycle -> Deadlock.
        m.wait_for_register_only(10, 11); // test helper: insert edge without awaiting
        assert!(matches!(m.check_cycle(11, 10), CycleCheck::Deadlock));
    }
}
```

Run `cargo test -p executor lockmgr` → COMPILE FAIL.

- [ ] **Step 2: implement.** Prepend to `crates/executor/src/lockmgr.rs`. The **lost-wakeup-free wait** uses tokio's `Notified::enable()` so a release that happens after the guard is dropped but before `.await` still wakes the waiter:

```rust
//! In-memory row-lock manager for concurrent writers. Per `(table, rowid)`
//! exclusive/shared locks, transaction-scoped (released at COMMIT/ROLLBACK). A
//! blocked writer awaits a per-holder `Notify`; the holder's `release_all`
//! wakes it. A wait-for graph (each waiting xid -> the xid it blocks on) is
//! checked eagerly for cycles before blocking, aborting the would-be waiter
//! with a deadlock error. Purely in-memory: after a restart no transactions are
//! in flight, so no lock state must survive.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Shared,
    Exclusive,
}

/// Result of a non-blocking lock attempt.
pub enum Acquire {
    Acquired,
    /// Held by `holder` (one of the holders, for an xid to wait on).
    Conflict(u64),
}

/// Result of the eager cycle check.
pub enum CycleCheck {
    Ok,
    Deadlock,
}

struct RowLock {
    mode: LockMode,
    holders: HashSet<u64>,
}

struct Inner {
    locks: HashMap<(catalog::TableId, u64), RowLock>,
    /// Per-holder notifier, woken when the holder releases all its locks.
    notifiers: HashMap<u64, Arc<Notify>>,
    /// wait-for graph: waiter xid -> the holder xid it is blocked on.
    wait_for: HashMap<u64, u64>,
}

pub(crate) struct RowLockManager {
    inner: Mutex<Inner>,
}

impl RowLockManager {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                locks: HashMap::new(),
                notifiers: HashMap::new(),
                wait_for: HashMap::new(),
            }),
        }
    }

    /// Non-blocking acquire. Idempotent if `my_xid` already holds compatibly.
    pub fn try_acquire(
        &self,
        table: catalog::TableId,
        rowid: u64,
        mode: LockMode,
        my_xid: u64,
    ) -> Acquire {
        let mut g = self.inner.lock().expect("lockmgr");
        match g.locks.get_mut(&(table, rowid)) {
            None => {
                let mut holders = HashSet::new();
                holders.insert(my_xid);
                g.locks.insert((table, rowid), RowLock { mode, holders });
                Acquire::Acquired
            }
            Some(lock) => {
                if lock.holders.contains(&my_xid) {
                    // Already a holder. Upgrade Shared->Exclusive only if sole holder.
                    if mode == LockMode::Exclusive && lock.mode == LockMode::Shared {
                        if lock.holders.len() == 1 {
                            lock.mode = LockMode::Exclusive;
                            Acquire::Acquired
                        } else {
                            let other = *lock.holders.iter().find(|&&h| h != my_xid).expect("other");
                            Acquire::Conflict(other)
                        }
                    } else {
                        Acquire::Acquired
                    }
                } else if mode == LockMode::Shared && lock.mode == LockMode::Shared {
                    lock.holders.insert(my_xid);
                    Acquire::Acquired
                } else {
                    Acquire::Conflict(*lock.holders.iter().next().expect("a holder"))
                }
            }
        }
    }

    /// Block until `holder` finishes, after an eager deadlock check. Returns
    /// `Err(())` if registering the wait would close a cycle (caller maps to
    /// `40P01`). Lost-wakeup-free: the `Notified` future is enabled while the
    /// guard is held, so a `release_all(holder)` after the guard drops still wakes us.
    pub async fn wait_for(&self, my_xid: u64, holder: u64) -> Result<(), ()> {
        let notify = {
            let mut g = self.inner.lock().expect("lockmgr");
            // Eager cycle check: does my_xid -> holder close a cycle?
            if matches!(check_cycle_locked(&g.wait_for, holder, my_xid), CycleCheck::Deadlock) {
                return Err(());
            }
            g.wait_for.insert(my_xid, holder);
            Arc::clone(g.notifiers.entry(holder).or_insert_with(|| Arc::new(Notify::new())))
        };
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable(); // register the waiter before any await
        notified.await;
        self.inner.lock().expect("lockmgr").wait_for.remove(&my_xid);
        Ok(())
    }

    /// Release every lock held by `my_xid`, wake anything waiting on it, and
    /// clear its wait-for edge. Called once at COMMIT/ROLLBACK.
    pub fn release_all(&self, my_xid: u64) {
        let mut g = self.inner.lock().expect("lockmgr");
        g.locks.retain(|_, lock| {
            lock.holders.remove(&my_xid);
            !lock.holders.is_empty()
        });
        g.wait_for.remove(&my_xid);
        if let Some(n) = g.notifiers.remove(&my_xid) {
            n.notify_waiters();
        }
    }

    // ---- test helpers ----
    #[cfg(test)]
    pub(crate) fn wait_for_register_only(&self, waiter: u64, holder: u64) {
        self.inner.lock().expect("lockmgr").wait_for.insert(waiter, holder);
    }
    #[cfg(test)]
    pub(crate) fn check_cycle(&self, holder: u64, my_xid: u64) -> CycleCheck {
        check_cycle_locked(&self.inner.lock().expect("lockmgr").wait_for, holder, my_xid)
    }
}

/// Would adding the edge `my_xid -> holder` create a cycle? Follow the chain
/// from `holder`; if it reaches `my_xid`, the new edge closes a cycle.
fn check_cycle_locked(wait_for: &HashMap<u64, u64>, holder: u64, my_xid: u64) -> CycleCheck {
    let mut cur = holder;
    let mut seen = HashSet::new();
    loop {
        if cur == my_xid {
            return CycleCheck::Deadlock;
        }
        if !seen.insert(cur) {
            return CycleCheck::Ok; // pre-existing cycle not involving my_xid (shouldn't happen)
        }
        match wait_for.get(&cur) {
            Some(&next) => cur = next,
            None => return CycleCheck::Ok,
        }
    }
}
```

- [ ] **Step 3: register + verify.** In `crates/executor/src/lib.rs` add `mod lockmgr;`. Run `cargo test -p executor lockmgr && cargo clippy -p executor --all-targets -- -D warnings`. Add `#[allow(dead_code)]` on `RowLockManager`/`Acquire`/`LockMode`/`CycleCheck` with `// wired in by the SP6 cutover (Task 5)` if dead-code-flagged.

- [ ] **Step 4: commit.**

```bash
git add crates/executor/src/lockmgr.rs crates/executor/src/lib.rs
git commit -m "feat(executor): RowLockManager — row locks, async waits, eager deadlock detection"
```

---

### Task 4: ProcArray eager `next_xid` persist + `40001`/`40P01` error variants

**Files:**
- Modify: `crates/executor/src/procarray.rs`, `crates/executor/src/error.rs`

Two small prerequisites for the cutover. (1) Under concurrent writers the session can no longer persist `next_xid` inside each transaction's batch — out-of-order batches would regress the durable counter and risk **xid reuse** (corruption). So `ProcArray::begin_write` persists `next_xid` durably under its own mutex (monotonic). (2) The two new SQLSTATEs.

- [ ] **Step 1: ProcArray gains a kv handle + eager persist.** Modify `crates/executor/src/procarray.rs`: `ProcArray` stores `kv: Arc<dyn Kv>`; `open` takes/stores it; `begin_write` returns `Result<u64, ExecError>` and persists the bumped `next_xid` durably **while holding the mutex** before returning:

```rust
pub(crate) struct ProcArray {
    kv: std::sync::Arc<dyn Kv>,
    inner: Mutex<Inner>,
}

impl ProcArray {
    pub fn open(kv: std::sync::Arc<dyn Kv>) -> Result<Self, ExecError> {
        let next_xid = match kv.get(&kv::key::next_xid_key())? { /* ... as today ... */ };
        Ok(Self { kv, inner: Mutex::new(Inner { next_xid: next_xid.max(1), running: BTreeSet::new() }) })
    }

    /// Allocate the next xid, register it running, and persist the bumped
    /// counter durably under the lock so it advances monotonically (a restart
    /// never reuses an xid even when commit batches land out of order).
    pub fn begin_write(&self) -> Result<u64, ExecError> {
        let mut g = self.inner.lock().expect("procarray");
        let xid = g.next_xid;
        let new_next = xid + 1;
        self.kv.write_batch(&[kv::WriteOp::Put {
            key: kv::key::next_xid_key(),
            value: new_next.to_be_bytes().to_vec(),
        }])?;
        g.next_xid = new_next;
        g.running.insert(xid);
        Ok(xid)
    }
    // next_xid(), snapshot(), finish(), running_len() unchanged.
}
```

`ProcArray::open` now takes `Arc<dyn Kv>` (was `&dyn Kv`). Update `SqlEngine::with_kv` to pass `Arc::clone(&kv)`. Update the three procarray unit tests: `ProcArray::open(Arc::new(MemKv::new()))`, and `begin_write()` now returns `Result` (add `.expect("xid")`). Drop the now-redundant `next_xid` test assertions that assumed no persist, or keep them (they still hold — `next_xid()` returns the in-memory value).

- [ ] **Step 2: error variants.** In `crates/executor/src/error.rs` add to `ExecError`:

```rust
    /// A write conflicted with a concurrently-committed change under REPEATABLE
    /// READ (40001) — the client should retry the transaction.
    SerializationFailure,
    /// A deadlock was detected and this transaction was chosen as the victim
    /// (40P01).
    Deadlock,
```

and to `into_pg`:

```rust
            ExecError::SerializationFailure => PgError::error(
                "40001",
                "could not serialize access due to concurrent update",
            ),
            ExecError::Deadlock => PgError::error("40P01", "deadlock detected"),
```

- [ ] **Step 3: verify.** `cargo test -p executor && cargo clippy -p executor --all-targets -- -D warnings`. The session still appends `next_xid` to its batch (harmless double-persist) until the cutover removes it — but `begin_write` now returns `Result`, so the session's `begin_write()` call sites need `?`. Fix those two call sites in `session.rs` (`run_write` autocommit + `ensure_write_xid`) to `self.procarray.begin_write()?`. Confirm the existing 224 tests stay green (behavior unchanged; just an extra counter persist).

- [ ] **Step 4: commit.**

```bash
git add crates/executor/src/procarray.rs crates/executor/src/error.rs crates/executor/src/session.rs crates/executor/src/lib.rs
git commit -m "feat(executor): eager monotonic next_xid persist; 40001/40P01 errors"
```

---

### Task 5: Cutover — remove the writer lock; concurrent write path with block-and-retry + EvalPlanQual

**Files:**
- Modify: `crates/executor/src/lib.rs` (drop `writer_lock`; add `seq`, `lockmgr`, `catalog_lock`)
- Modify: `crates/executor/src/session.rs` (no writer lock; async write path; `release_all` on commit/rollback)
- Modify: `crates/executor/src/exec.rs` (async `execute_write` with the conflict loop; INSERT via `SequenceManager`)

The pivotal task. After it, writers run concurrently; same-row writers block-and-retry with EvalPlanQual; **all 224 existing tests stay green** (non-conflicting workloads behave identically). `SELECT FOR UPDATE`/`FOR SHARE` execution is Task 6; deterministic concurrency tests are Task 7.

**Design recap (implement exactly):**
- `SqlEngine` drops `writer_lock`; gains `seq: Arc<SequenceManager>`, `lockmgr: Arc<RowLockManager>`, and a `catalog_lock: Arc<std::sync::Mutex<()>>` (DDL still needs serialization — concurrent CREATE TABLE races on `next_table_id`; a short sync lock around `execute_ddl` keeps DDL safe without serializing DML). `connect` passes all of them (plus `procarray`) to `SqlSession::new`.
- `TxnCtx` drops `writer_guard`. `SqlSession` drops `writer_lock`, gains `seq`/`lockmgr`/`catalog_lock`.
- `run_ddl`: take `catalog_lock` (sync `std::Mutex`, no await — `execute_ddl` is sync) around the call; no writer lock.
- `run_write`: no global lock. Autocommit — `xid = procarray.begin_write()?` (counter already persisted), `snapshot`, `execute_write(&kv, &snapshot, xid, lockmgr, isolation, stmt).await` (now async), then ONE commit batch of the returned ops + `clog::put_op(xid, Committed)` (NO `next_xid` op — ProcArray persisted it), `write_batch`, `procarray.finish(xid)`, `lockmgr.release_all(xid)`. In-txn — `ensure_write_xid` (allocate xid, no lock), `execute_write(...).await`, batch the ops (no next_xid, no clog), `write_batch`. COMMIT/ROLLBACK additionally call `lockmgr.release_all(xid)`.
- `commit_cmd`/`rollback_cmd`/`abort_ctx`: after `procarray.finish(xid)`, call `self.lockmgr.release_all(xid)`. `Drop for SqlSession`: also `release_all(xid)` (a mid-txn disconnect must free its row locks). These are sync; `release_all` is sync.
- `execute_write` becomes `async` and takes `lockmgr: &RowLockManager` and `repeatable_read: bool`. INSERT: `let start = seq.alloc(kv, t.id, n_rows)?;` (replaces `read_seq_kv` + the seq Put — `SequenceManager` persists it). UPDATE/DELETE: the per-row conflict loop below.

- [ ] **Step 1: engine + session plumbing.** Apply the struct/`connect`/`new` changes above in `lib.rs` and `session.rs` (drop `writer_lock`/`writer_guard`/`Mutex`/`OwnedMutexGuard` imports; add `seq`/`lockmgr`/`catalog_lock`). `ensure_write_xid` no longer takes a lock — it just allocates the xid: `let xid = self.procarray.begin_write()?; ctx.xid = Some(xid);`. `run_ddl` uses `let _g = self.catalog_lock.lock().expect("catalog lock"); crate::exec::execute_ddl(&*self.kv, stmt)`.

- [ ] **Step 2: the conflict loop in `execute_write` (UPDATE/DELETE).** Make `execute_write` async. For each candidate `(rowid, xmin, row)` from the initial `scan_live`, acquire the row's exclusive lock with block-and-retry, then EvalPlanQual-recheck before applying. Helper:

```rust
/// Acquire `(table, rowid)` exclusively for `xid`, blocking (deadlock-checked)
/// until free. Returns Err(Deadlock) if a cycle is detected.
async fn lock_row(
    lockmgr: &crate::lockmgr::RowLockManager,
    table: catalog::TableId,
    rowid: u64,
    mode: crate::lockmgr::LockMode,
    xid: u64,
) -> Result<(), ExecError> {
    loop {
        match lockmgr.try_acquire(table, rowid, mode, xid) {
            crate::lockmgr::Acquire::Acquired => return Ok(()),
            crate::lockmgr::Acquire::Conflict(holder) => {
                lockmgr.wait_for(xid, holder).await.map_err(|()| ExecError::Deadlock)?;
                // woken: the holder finished; loop to re-acquire.
            }
        }
    }
}

/// EvalPlanQual: after locking the row, re-read its current latest version. If a
/// transaction that committed *after* my snapshot changed it, RC re-finds the
/// latest live version (re-checking the predicate); RR raises 40001. Returns the
/// row to operate on, or None to skip (deleted / no longer matches).
fn eval_plan_qual(
    kv: &dyn Kv,
    table: &catalog::Table,
    rowid: u64,
    xid: u64,
    repeatable_read: bool,
) -> Result<Option<(u64 /*xmin*/, Vec<Datum>)>, ExecError> {
    // Re-scan just this rowid's versions.
    let prefix = kv::key::row_key(table.id, rowid);
    let scanned = kv.scan_prefix(&prefix)?;
    // Find the newest version (highest xmin) and inspect its xmax.
    // ... see Step 3 for the precise rule ...
}
```

- [ ] **Step 3: the precise EvalPlanQual rule.** Implement `eval_plan_qual` so that, with the row lock held, it inspects the rowid's current versions:
  1. Take a **fresh** ProcArray snapshot for RC (the session passes a closure or the manager); for RR reuse the txn snapshot.
  2. Find the version visible to that snapshot+own-xid via `satisfies_mvcc` (as `scan_live` does for one rowid). Also find whether the *latest committed* version carries an `xmax` committed by a transaction **not** visible to the txn snapshot (i.e. a concurrent committed update/delete).
  3. **No concurrent committed change** (the version I held is still the live one, `xmax` invalid/aborted/mine): return `Some((xmin, row))` — apply normally.
  4. **Concurrent committed UPDATE/DELETE** (the row's latest version was created/deleted by a txn that committed after my snapshot):
     - **RR** (`repeatable_read == true`): return `Err(ExecError::SerializationFailure)`.
     - **RC**: re-find the now-latest visible version under the fresh snapshot. If it exists and still matches the statement's `WHERE` (the caller re-checks `row_matches` on the returned row), return `Some` of it; if the row was deleted (no visible version), return `Ok(None)` (skip — contributes 0).

  Wire it into the UPDATE and DELETE arms: replace the direct `for (rowid, xmin, row) in scan_live(...)` body with: for each candidate rowid, `lock_row(lockmgr, t.id, rowid, Exclusive, xid).await?;` then `let Some((cur_xmin, cur_row)) = eval_plan_qual(kv, &t, rowid, xid, repeatable_read)? else { continue; };` then re-check `row_matches(filter, Some(&t), &cur_row)?` (skip if false), then apply the SET / tombstone to `cur_row`/`cur_xmin` exactly as the SP5 code does (the `xmin == xid` in-place vs supersede branch). Accumulate `ops`; the session writes them.

  NOTE: because `execute_write` now returns ops the session writes in one batch per statement, and the row locks are held until COMMIT, a concurrent writer to the same row cannot interleave between the lock and the batch write. Single-statement atomicity holds.

- [ ] **Step 4: thread isolation + lockmgr through the call.** `run_write` passes `repeatable_read` (from `ctx`, or `false` for autocommit) and `&self.lockmgr` and a way for `eval_plan_qual` to take a fresh snapshot (pass `&self.procarray` or a `Fn() -> Snapshot`). Keep signatures clean: `execute_write(kv, procarray, lockmgr, snapshot, xid, repeatable_read, stmt) -> Result<(QueryResult, Vec<WriteOp>), ExecError>` (async). Adapt the autocommit/in-txn callers.

- [ ] **Step 5: the regression gate.** `cargo test --workspace`. ALL 224 existing tests must pass — non-conflicting workloads (every existing test is single-threaded or non-conflicting) behave identically. The existing `concurrency.rs::concurrent_inserts_do_not_lose_rows` now exercises `SequenceManager` + no global lock; it must still pass (no lost rows). If it loses rows, `SequenceManager.alloc` isn't atomic/persisted correctly. If anything hangs, a row lock isn't released on commit/rollback/drop (`release_all` missing on a path) or the `Notified::enable()` wait is wrong.

- [ ] **Step 6: fmt + clippy + commit.**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add crates/executor
git commit -m "feat(executor): concurrent writers — row locks, block-and-retry, EvalPlanQual; remove writer lock"
```

---

### Task 6: `SELECT FOR UPDATE` / `FOR SHARE` execution

**Files:**
- Modify: `crates/executor/src/session.rs` (route a locking SELECT through the write-ish path), `crates/executor/src/exec.rs` (acquire row locks on the result rows)

A locking SELECT takes row locks (exclusive for `FOR UPDATE`, shared for `FOR SHARE`) on each returned row, using the transaction's xid (allocating one if the txn hasn't written), blocking + EvalPlanQual-ing like a write, but without modifying rows. It otherwise returns the rows like a normal SELECT.

- [ ] **Step 1: failing test.** Add to `crates/executor/tests/concurrency.rs` (created/extended in Task 7; if not yet present, put this in `exec.rs` tests for now): a `FOR UPDATE` SELECT returns the rows AND holds a lock that a concurrent UPDATE of the same row blocks on. (Full concurrency assertions land in Task 7; here assert at least that `SELECT ... FOR UPDATE` returns correct rows and, in a txn, a second session's `try_acquire` on the same row reports a conflict.)

- [ ] **Step 2: route locking SELECTs.** In `run_one`/`run_select`: if the statement is `Statement::Select(s)` with `s.locking.is_some()`, it needs an xid + the lockmgr, so handle it on an async path: ensure an xid (`ensure_write_xid`-style allocation; a `FOR UPDATE` in an otherwise read-only txn assigns an xid, as PostgreSQL does), take the read snapshot, call a new `execute_read_locking(kv, procarray, lockmgr, snapshot, xid, repeatable_read, s).await`. A non-locking SELECT stays on the existing sync `execute_read` path (no xid, no lock).

- [ ] **Step 3: implement `execute_read_locking`.** Like `execute_read`, but after computing the result rows it also, for each underlying source rowid, `lock_row(lockmgr, t.id, rowid, mode, xid).await?` (mode = Exclusive for `ForUpdate`, Shared for `ForShare`) with the same block-and-retry, and runs `eval_plan_qual` (RR → 40001 on a concurrent committed change; RC re-finds). The rows returned are the locked, latest versions. (`FOR UPDATE` on a FROM-less SELECT or an aggregate is out of scope — only simple `FROM table` locking SELECTs; reject others with `0A000` if needed.)

- [ ] **Step 4: commit at COMMIT/ROLLBACK** already calls `release_all(xid)` (Task 5), which frees `FOR UPDATE`/`FOR SHARE` locks too. Verify a `FOR SHARE` lock is released and a blocked exclusive waiter resumes.

- [ ] **Step 5: verify + commit.**

```bash
cargo test --workspace && cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
git add crates/executor
git commit -m "feat(executor): SELECT FOR UPDATE / FOR SHARE row locking"
```

---

### Task 7: Deterministic concurrency tests

**Files:**
- Modify: `crates/executor/tests/concurrency.rs`, `crates/executor/tests/end_to_end.rs`

Concurrency tests must be **deterministic**: drive two sessions on the multi-thread runtime with explicit synchronization (a `tokio::sync::mpsc`/`Barrier`/`Notify` between them) so each interleaving reproduces exactly. Use `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`.

- [ ] **Step 1: same-row UPDATE conflict — RC re-find.** Two sessions on one engine. T1: `BEGIN; UPDATE t SET v='a' WHERE id=1`. Signal T2 to start its `UPDATE t SET v='b' WHERE id=1` (it blocks on T1's row lock). Signal T1 to `COMMIT`. T2 wakes, EvalPlanQuals (READ COMMITTED, default): it re-finds the row T1 committed and applies `v='b'`. Assert the final value is `'b'` and `UPDATE 1`. Use a channel: T2 sends "about to update" then T1 waits a beat (or T2 signals via a shared `Notify` after `try_acquire` would block — simplest: T2 spawns, T1 sleeps a controlled `tokio::time::sleep` OR they hand off via a oneshot). Prefer a oneshot/Barrier handoff over sleeps; if a sleep is unavoidable for "let the waiter block", keep it tiny and comment it.

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_row_update_blocks_then_read_committed_refinds() {
    let engine = std::sync::Arc::new(SqlEngine::new());
    { let mut s = engine.connect();
      run(&mut s, "CREATE TABLE t (id int4, v text)").await;
      run(&mut s, "INSERT INTO t VALUES (1,'orig')").await; }

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "UPDATE t SET v='a' WHERE id=1").await; // holds the row lock

    let e2 = std::sync::Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "BEGIN").await;
        // blocks until t1 commits, then EvalPlanQual re-finds and applies
        let r = run(&mut s, "UPDATE t SET v='b' WHERE id=1").await;
        run(&mut s, "COMMIT").await;
        r
    });

    // Let t2 reach the blocking point, then commit t1 to release the lock.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await; // t2 is now blocked
    run(&mut t1, "COMMIT").await;
    let r2 = t2.await.expect("t2 joins");
    assert_eq!(tag_of(&r2[0]), "UPDATE 1");

    let mut s = engine.connect();
    let rows = run(&mut s, "SELECT v FROM t WHERE id=1").await;
    assert_eq!(col0(&rows[0]), vec![Some("b".into())]);
}
```

- [ ] **Step 2: same-row UPDATE conflict — RR 40001.** Same shape, but T2 is `BEGIN ISOLATION LEVEL REPEATABLE READ` and takes its snapshot *before* T1 commits (do a `SELECT` in T2 first to fix the snapshot), then its blocked `UPDATE` after T1 commits returns error `40001`.

- [ ] **Step 3: blocker aborts → waiter proceeds on original.** T1 updates row 1 then `ROLLBACK`; T2's blocked update wakes and applies to the original row (since T1's xmax aborted). Assert T2 succeeds with the original-based value.

- [ ] **Step 4: different rows run concurrently.** T1 updates id=1, T2 updates id=2 concurrently; neither blocks; both commit; both values updated. (No handoff needed — they don't conflict.)

- [ ] **Step 5: FOR UPDATE blocks UPDATE; FOR SHARE coexists.** T1 `BEGIN; SELECT … WHERE id=1 FOR UPDATE`; T2's `UPDATE … WHERE id=1` blocks until T1 commits. Separately, two `FOR SHARE` on the same row coexist, but an `UPDATE` blocks on them.

- [ ] **Step 6: deadlock → one 40P01.** T1 locks row A (update) then tries row B; T2 locks row B then tries row A. Drive the interleaving so each holds one and waits the other; assert exactly one transaction gets `40P01` and the other commits. Use two oneshots to sequence "T1 locked A", "T2 locked B", then both attempt the second lock.

- [ ] **Step 7: wire e2e.** In `end_to_end.rs`, two tokio-postgres connections: one holds a row via `BEGIN; UPDATE`, the other's `UPDATE` of the same row blocks until the first commits, then succeeds — proving the block-and-retry works over the wire.

- [ ] **Step 8: run + commit.** `cargo test -p executor --test concurrency --test end_to_end` → all pass (no hangs, deterministic). Then `cargo test --workspace`. Commit:

```bash
git add crates/executor/tests
git commit -m "test(executor): deterministic concurrent-writer tests (conflict, EvalPlanQual, FOR UPDATE, deadlock)"
```

---

### Task 8: Gauntlet, conformance, traceability

**Files:** Verify; corpus accept-cases.

- [ ] **Step 1: gauntlet.** Run each, report PASS/FAIL:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test -p pgparser --features oracle
./scripts/check-no-native.sh
cargo deny check
```
No new shipped deps (tokio already present), so `check-no-native.sh`/`cargo deny` stay green.

- [ ] **Step 2: conformance.** `cargo test -p conformance`. Add `FOR UPDATE`/`FOR SHARE` parse cases to the corpus only if the offline harness covers them; the live-oracle leg needs Docker `postgres:18`. Parity ≥ SP5 baseline.

- [ ] **Step 3: success-criteria traceability.** Confirm each maps to a green test:

| # | Spec criterion | Verifying test(s) |
|---|---|---|
| 1 | Different rows concurrent; same row blocks | `concurrency::different_rows_run_concurrently`, `concurrency::same_row_update_blocks_then_read_committed_refinds` |
| 2 | Blocker commit → RC re-find / RR 40001; abort → proceed | `concurrency::same_row_update_blocks_then_read_committed_refinds`, `concurrency::repeatable_read_conflict_is_40001`, `concurrency::blocker_abort_lets_waiter_proceed` |
| 3 | FOR UPDATE/FOR SHARE locks; grammar matches oracle | `concurrency::for_update_blocks_update`, `concurrency::for_share_coexists`, `pgparser::parses_select_for_update_and_share`, oracle |
| 4 | Deadlock → one 40P01, no hang | `concurrency::deadlock_yields_one_40p01`, `lockmgr::wait_for_detects_a_two_cycle` |
| 5 | Concurrent INSERT distinct rowids, no global lock | `concurrency::concurrent_inserts_do_not_lose_rows`, `seq::*` |
| 6 | All SP1–SP5 gates green; 224 tests pass; parity ≥ baseline | gauntlet (Step 1) + conformance (Step 2) |

If any row lacks a green test, add it.

- [ ] **Step 4: commit (if corpus/docs changed).**

```bash
git add -A
git commit -m "test(sp6): conformance + success-criteria traceability; gauntlet green"
```

---

## Final review (after all tasks)

Dispatch a final code-reviewer over the whole SP6 diff (vs pre-SP6 main), then run `superpowers:finishing-a-development-branch`. Review focus:

- **No lost wakeups / no hangs:** every `wait_for` resumes (the `Notified::enable()`-before-await pattern); `release_all` is called on COMMIT, ROLLBACK, COMMIT-of-failed, autocommit success/error, and session Drop — a row lock is never stranded (engine-wide stall).
- **Deadlock detection is sound:** the eager cycle check fires before blocking; exactly one victim aborts (`40P01`); no false positives on non-cyclic waits; no missed cycles.
- **Counter monotonicity:** `next_xid` and per-table seq are persisted under their mutex before use, so concurrent batches cannot regress them — no xid/rowid reuse across restart.
- **EvalPlanQual correctness:** RC re-finds the latest version and re-checks the predicate; RR raises `40001`; a concurrent DELETE makes the row contribute 0; the `xmin == xid` own-version in-place branch still holds.
- **Isolation unchanged for reads:** reads never take a row lock; `satisfies_mvcc`/snapshot timing is exactly SP5.
- **Behavior identity:** the 224 SP5 tests pass unchanged; only the concurrency *profile* changed (concurrent writers), and committed results match a serial execution for every non-conflicting workload.
- **No `unsafe`, no `unwrap` on fallible paths, no `MutexGuard`/`std::sync::Mutex` held across `.await`** (the lockmgr/seq/procarray std mutexes are released before any await; only `tokio::sync` primitives cross awaits).
