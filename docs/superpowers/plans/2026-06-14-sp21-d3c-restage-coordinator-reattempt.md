# SP21 / D3c-restage — Coordinator Re-Attempt Under Fresh `g'` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a participant range's leader moves during the cross-range 2PC STAGE window (surfacing `ExecError::NotLeader`), the coordinator/router transparently aborts the abandoned global xid `g`, mints a fresh `g'`, replays the txn's buffered cross-range write-set under `g'`, and commits — instead of surfacing a retryable abort to the client.

**Architecture:** Three independent changes in two crates. (1) `crates/executor/src/exec.rs`: harden `find_visible_one` + `scan_live` to select the highest-xmin visible version explicitly and `debug_assert!` the MVCC at-most-one-live invariant (defense-in-depth, no behavior change). (2) `crates/cluster/src/range/router.rs`: a per-txn write-set buffer (`restage_buf`) recording every held DML, plus a bounded re-attempt loop intercepting `stage_on`→`NotLeader` that aborts `g`, begins `g'`, and replays the buffer. (3) Tests: in-process re-attempt proofs (a synthetic `NotLeader` injection seam) in `router.rs`'s unit-test module, and a multi-process kill-during-stage nemesis in a new UAC-safe `crossrange_2pc_restage.rs`.

**Tech Stack:** Rust 2024, openraft, tokio, cargo-nextest. No new dependency.

---

## Background the implementer needs (read once)

**The cross-range 2PC model (already built, SP16–20).** A `RangeRouter` (`crates/cluster/src/range/router.rs`) executes a `BEGIN..COMMIT` block. The first table-bearing DML pins the txn to a range (`Pin::Range(r)`); a DML on a *second* range escalates to a global txn `Pin::Global { ranges, g }` where `g` is a global xid minted by range 0's GTM (`coord.begin_global()`). Each participant range writes a `Prepared(Li → g)` marker in its local clog. `COMMIT`/`ROLLBACK` drives `finish_txn`, which writes the single global decision (`coord.commit_global(g, commit)` → range 0's global clog) and releases every participant. A row's visibility resolves `clog[Li] = Prepared(g)` → range 0's `clog[g]` forever (no per-participant freeze).

**The fault SP21 closes.** While staging participant `r` under `g`, `r`'s leader can move (the old leader steps down / is killed). Staging then returns `ExecError::NotLeader` (minted only by `RaftCommitter`, `crates/cluster/src/committer.rs:33-39`, and surfaced over the wire by `NetCoordinator::stage_remote`, `crates/cluster/src/twopc.rs:233-252`). Today that `NotLeader` propagates up to the client as a retryable abort (SQLSTATE 40001). SP21 intercepts it and re-attempts under a fresh `g'`.

**Why fresh `g'` and not a same-`g` re-stage** (locked in the spec, `docs/superpowers/specs/2026-06-14-crabgresql-sp21-d3c-restage-coordinator-reattempt-design.md`): a same-`g` re-stage double-applies (version key is `(table_id, rowid, xid)` with no `g` — `crates/mvcc/src/version.rs:18`; the new leader hands out `Li_new > Li_old` via `reseed_from_applied`, so a second physical version appears, both resolving to `g`) and races the new leader's leadership-rise recovery sweep that abort-races `g`. A fresh `g'` re-attempt makes the abandoned `g` *correctly* aborted (its versions presumed-abort/invisible) and `g'` a clean attempt.

**Three `NotLeader` distinctions the re-attempt MUST respect:**
- `ExecError::NotLeader` is the ONLY re-attempt trigger. `NetCoordinator::stage_remote` maps `TxnResp::Retryable` → `ExecError::SerializationFailure` (`twopc.rs:248`) — a genuine 40001 conflict that must STILL surface to the client. Match exactly `Err(ExecError::NotLeader) => reattempt, Err(e) => Err(e)` — never a catch-all.
- `TwoPcClient::call` already re-resolves the leader and retries once internally (`twopc.rs:79-116`) before surfacing `NotLeader`. So a `NotLeader` reaching the router means the participant genuinely moved (or its new leader is still mid-bootstrap) — a correct trigger.
- `commit_global(g, false)` returns the EFFECTIVE decision as `bool` (`true` = a committer won the COMMIT race). In SP21's flow nothing writes `Committed(g)` before `finish_txn`, so it always returns `false`; branch on it defensively (a `true` means do NOT re-attempt — see Task 3).

**The in-process harness limitation (shapes the test strategy).** `RangeRouter::connect` (`router.rs:258-276`) wires `AlwaysLeads` (every local engine is treated as the leader) + a `LocalCoordinator` whose `stage_remote`/`release_remote` are `Unsupported` (every participant is local) + a STATIC `engines: HashMap<RangeId, SqlEngine>` captured at connect time. So in-process, `stage_on` always takes the local led path and a *real* leader move cannot be modeled (and a replay could not re-resolve a new engine). Therefore the in-process tests force the `NotLeader` with a **synthetic injection seam** (one-shot for the commit-transparently proof; always-on for the bounded-retry proof) and the replay re-routes to the same still-valid local engine. The REAL cross-node re-resolution (`TwoPcClient::await_leader` over the wire) is proven by the multi-process nemesis (Task 4).

---

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `crates/executor/src/exec.rs` | Modify `find_visible_one` (350-371), `scan_live` (467-502); add 2 unit tests near 1305 | Highest-xmin selection + at-most-one-live `debug_assert!` |
| `crates/cluster/src/range/router.rs` | Add `restage_buf` field + recording + `stage_fault` seam + re-attempt loop; add unit tests in `#[cfg(test)] mod tests` | The write-set buffer and the bounded fresh-`g'` re-attempt |
| `crates/crabgresql/tests/crossrange_2pc_restage.rs` | Create (UAC-safe name) | Multi-process kill-during-stage conservation nemesis |
| `CLAUDE.md` | Append SP21 audit line | os-740 target-name audit |
| design spec traceability section | Append at finish | Criterion → test map |

---

## Task 1: Harden `find_visible_one` and `scan_live` (executor)

**Files:**
- Modify: `crates/executor/src/exec.rs:350-371` (`find_visible_one`), `crates/executor/src/exec.rs:467-502` (`scan_live`)
- Test: `crates/executor/src/exec.rs` `#[cfg(test)] mod tests` (near the existing test at line 1305)

This task is independent of the router work and the smallest; do it first. The change is behavior-preserving on all valid inputs (≤1 live version): today both functions keep the LAST `satisfies_mvcc` match, which equals the highest xmin only because the scan returns versions in ascending-xid order. We make highest-xmin explicit (order-independent) and add a debug assertion that no more than one live version exists per row per snapshot.

- [ ] **Step 1: Write the failing test — the abandoned-`g` shadow is invisible, the committed-`g'` version is returned**

Add to the `#[cfg(test)] mod tests` module in `crates/executor/src/exec.rs` (mirror the setup machinery of `eval_plan_qual_settled_global_sees_committed_cross_range_version` at line 1305 — `MemKv`, `version_key_xid`, `encode_tuple`, `put_op`, a settled `Snapshot`):

```rust
/// SP21: after a fresh-`g'` re-attempt, a row has TWO physical versions — the
/// abandoned attempt's `Prepared(Li_old -> g)` with `g` Aborted, and the re-attempt's
/// `Prepared(Li_new -> g')` with `g'` Committed. `find_visible_one` must return the
/// committed-`g'` version (highest xmin) and never the aborted shadow; exactly one
/// version is live (the assert holds).
#[test]
fn find_visible_one_returns_committed_reattempt_over_aborted_shadow() {
    use std::sync::Arc;

    use super::{find_visible_one, global_status};
    use kv::{Kv, MemKv};
    use mvcc::clog::{XidStatus, put_op};
    use mvcc::visibility::Snapshot;
    use mvcc::xid::{GLOBAL_XID_BASE, INVALID_XID};
    use pgtypes::Datum;

    let li_old: u64 = 5; // abandoned attempt's local xid
    let li_new: u64 = 9; // re-attempt's local xid (reseed -> strictly greater)
    let g: u64 = GLOBAL_XID_BASE + 1; // abandoned global xid (Aborted)
    let g2: u64 = GLOBAL_XID_BASE + 2; // fresh global xid (Committed)

    let kv = Arc::new(MemKv::new()); // holds the local clog
    let global = MemKv::new(); // range-0 global clog

    // `find_visible_one` reads ONLY the passed `versions` slice + the local/global clogs
    // (it never touches the kv row-version store), so seed just the two clogs here.
    // Local clog: both local xids are Prepared, deref to the global clog.
    kv.write_batch(&[
        put_op(li_old, XidStatus::Prepared(g)),
        put_op(li_new, XidStatus::Prepared(g2)),
    ])
    .expect("local clog");
    // Global clog: g Aborted (abandoned), g2 Committed (re-attempt).
    global
        .write_batch(&[put_op(g, XidStatus::Aborted), put_op(g2, XidStatus::Committed)])
        .expect("global clog");

    // A settled snapshot: every xid is settled, so global_status reads the global clog.
    let settled = Snapshot { xmin: 0, xmax: u64::MAX, xip: Vec::new() };
    // The two physical versions, both live (xmax = INVALID): old value 100, new value 70.
    let versions = vec![
        (li_old, INVALID_XID, vec![Datum::Int4(100)]),
        (li_new, INVALID_XID, vec![Datum::Int4(70)]),
    ];
    let got = find_visible_one(kv.as_ref(), &global, &settled, &settled, None, &versions)
        .expect("find_visible_one ok")
        .expect("a version is visible");
    assert_eq!(got.0, li_new, "the committed re-attempt version (highest xmin) wins");
    assert_eq!(got.1, vec![Datum::Int4(70)], "value is the re-attempt's, not the aborted shadow's");
    // Sanity: the aborted shadow really is invisible under this resolver.
    let resolve = global_status(kv.as_ref(), &global, &settled);
    assert!(matches!(resolve(li_old), Ok(XidStatus::Aborted)));
}
```

- [ ] **Step 2: Run the test — it must FAIL to compile or pass trivially against today's code**

Run: `cargo test -p executor --lib find_visible_one_returns_committed_reattempt_over_aborted_shadow`
Expected: this scenario actually PASSES against today's last-wins code (only one version is live), which is fine — it documents the SP21 guarantee. Proceed; the hardening makes it order-independent and adds the assert. (If it fails, fix the test setup before continuing.)

- [ ] **Step 3: Harden `find_visible_one`**

Replace the body of `find_visible_one` (`crates/executor/src/exec.rs:358-370`, the part from `let mut visible` through `Ok(visible)`) with:

```rust
    let mut visible: Option<(u64, Vec<pgtypes::Datum>)> = None;
    let mut live_count: usize = 0;
    for (xmin, xmax, row) in versions {
        if mvcc::visibility::satisfies_mvcc(
            *xmin,
            *xmax,
            snap,
            own,
            global_status(kv, global, gsnap),
        )? {
            live_count += 1;
            // Keep the greatest-xmin live version EXPLICITLY. The MVCC at-most-one-live
            // invariant means there is normally exactly one; selecting the max removes the
            // hidden dependence on ascending scan order, so a future scan-order change can
            // never silently return a stale shadow (e.g. an aborted re-attempt's
            // `Prepared(Li_old -> g)` tuple that resolves invisible anyway).
            // NB: `is_none_or`, NOT `map_or(true, …)` — the latter trips
            // `clippy::unnecessary_map_or` under the workspace's `-D warnings` gate.
            if visible.as_ref().is_none_or(|(cur, _)| *xmin > *cur) {
                visible = Some((*xmin, row.clone()));
            }
        }
    }
    debug_assert!(
        live_count <= 1,
        "find_visible_one: {live_count} live versions for one rowid under one snapshot \
         — MVCC at-most-one-live invariant violated"
    );
    Ok(visible)
```

Also update the doc comment above `find_visible_one` (line 347-349) to: `/// Find the single version of \`rowid\` visible to \`snap\` ... Returns the greatest-xmin live version; the MVCC at-most-one-live invariant makes this the only live one, and the explicit max is order-independent (debug-asserted).`

- [ ] **Step 4: Harden `scan_live`'s inner loop identically**

In `scan_live` (`crates/executor/src/exec.rs:481-498`), replace the per-rowid inner loop so it counts live versions and selects the max. Replace lines 481-498 (from `let mut visible` through the `if let Some((xmin, row)) = visible { out.push(...) }`) with:

```rust
        let mut visible: Option<(u64, Vec<pgtypes::Datum>)> = None;
        let mut live_count: usize = 0;
        while i < scanned.len() && mvcc::version::row_prefix_of(&scanned[i].0)? == prefix.as_slice()
        {
            let (xmin, xmax, row) = mvcc::version::decode_tuple(&scanned[i].1)?;
            if mvcc::visibility::satisfies_mvcc(
                xmin,
                xmax,
                snapshot,
                own,
                global_status(kv, global, gsnap),
            )? {
                live_count += 1;
                // `is_none_or`, NOT `map_or(true, …)` — see find_visible_one above.
                if visible.as_ref().is_none_or(|(cur, _)| xmin > *cur) {
                    visible = Some((xmin, row));
                }
            }
            i += 1;
        }
        debug_assert!(
            live_count <= 1,
            "scan_live: {live_count} live versions for rowid {rowid} under one snapshot \
             — MVCC at-most-one-live invariant violated"
        );
        if let Some((xmin, row)) = visible {
            out.push((rowid, xmin, row));
        }
```

- [ ] **Step 5: Add the order-independence + assert test**

Add this second test to the same module. It builds two ARTIFICIALLY-live versions (both committed, neither deleted — an at-most-one-live violation) to exercise BOTH the explicit-max selection (in release, where the assert is compiled out) and the `debug_assert!` (in debug):

```rust
/// The explicit highest-xmin selection is order-independent, and the at-most-one-live
/// invariant is debug-asserted. Two committed, non-deleted versions of one row are an
/// artificial invariant violation: in DEBUG the assert fires (`should_panic`); in
/// RELEASE the assert is compiled out and the greater xmin is returned regardless of
/// the order the versions are presented.
///
/// Debug-profile-dependent BY DESIGN: this repo's CI runs `cargo nextest` and
/// `cargo llvm-cov nextest` in the debug profile, so the `debug_assert!` fires and the
/// `should_panic` arm is exercised. Introducing a release/opt test profile would flip
/// the expectation and require revisiting this `cfg_attr`.
#[test]
#[cfg_attr(debug_assertions, should_panic(expected = "at-most-one-live"))]
fn find_visible_one_orders_by_xmin_and_flags_multiple_live() {
    use std::sync::Arc;

    use super::find_visible_one;
    use kv::{Kv, MemKv};
    use mvcc::clog::{XidStatus, put_op};
    use mvcc::visibility::Snapshot;
    use mvcc::xid::INVALID_XID;
    use pgtypes::Datum;

    let kv = Arc::new(MemKv::new());
    let global = MemKv::new();
    kv.write_batch(&[
        put_op(5, XidStatus::Committed),
        put_op(9, XidStatus::Committed),
    ])
    .expect("clog");
    let settled = Snapshot { xmin: 0, xmax: u64::MAX, xip: Vec::new() };

    // Present them in DESCENDING order so last-wins would pick the LOWER xmin; the
    // explicit max must still pick 9.
    let versions = vec![
        (9u64, INVALID_XID, vec![Datum::Int4(70)]),
        (5u64, INVALID_XID, vec![Datum::Int4(100)]),
    ];
    let got = find_visible_one(kv.as_ref(), &global, &settled, &settled, None, &versions)
        .expect("ok"); // only reached in release builds
    assert_eq!(got.expect("visible").0, 9, "highest xmin regardless of presentation order");
}
```

- [ ] **Step 6: Run the executor tests + the in-crate clippy gate**

Run: `cargo test -p executor --lib`
Expected: PASS. The first new test passes; the second `should_panic`s in debug (the default `cargo test` profile), confirming the assert fires. The existing `eval_plan_qual_settled_global_sees_committed_cross_range_version` and all other executor tests stay green (behavior unchanged for ≤1 live).

Run: `cargo clippy -p executor --all-targets -- -D warnings`
Expected: clean. (This catches the `is_none_or` lint and any unused import IN-TASK rather than at the Task 5 gauntlet three commits later — the codebase uses `is_none_or`/`is_some_and` exclusively and `map_or(true|false, …)` is a hard `-D warnings` error here.)

- [ ] **Step 7: Commit**

```bash
git add crates/executor/src/exec.rs
git commit -m "feat(sp21): harden find_visible_one/scan_live — explicit highest-xmin + at-most-one-live debug_assert

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Per-txn cross-range write-set buffer (router)

**Files:**
- Modify: `crates/cluster/src/range/router.rs` (struct 163-196, `new` 202-223, `dispatch` arms 343-419, `finish_txn` 426-427)
- Test: `crates/cluster/src/range/router.rs` `#[cfg(test)] mod tests`

The buffer is the replay log: every held table-bearing DML of a cross-range txn, recorded as `(target range, parsed Statement, exact source SQL)`. **The correctness trap** (do not miss it): the buffer must start filling at the FIRST held DML — including the led-local DML that ran *before* any `g` existed (it pins range `p` and runs via `run_on`, not `stage_on`). A replay that only replays statements staged *after* escalation would silently DROP `p`'s rows. Record at every held-DML point.

`Statement` derives `Clone` (`crates/pgparser/src/ast.rs:5`); it is only `PartialEq` (not `Eq`), so the buffer is a `Vec`, never a set.

- [ ] **Step 1: Write the failing buffer-capture test**

Add to `crates/cluster/src/range/router.rs`'s `#[cfg(test)] mod tests` (mirror `two_range_cluster` setup from the existing tests — `MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2]))`, `wait_for_leader`, `RangeRouter::connect`):

```rust
/// The write-set buffer captures EVERY held cross-range DML, including the
/// led-local first DML that ran before `g` existed (the silent-data-loss trap).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restage_buffer_captures_full_cross_range_write_set() {
    let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
    for r in c.range_map().range_ids() {
        c.wait_for_leader(r).await;
    }
    let mut admin = RangeRouter::connect(&c).await;
    admin.simple("CREATE TABLE a (id int4)").await.expect("a"); // id 1 -> range 0
    admin.simple("CREATE TABLE b (id int4)").await.expect("b"); // id 2 -> range 1
    admin.simple("INSERT INTO a VALUES (10)").await.expect("seed a");
    admin.simple("INSERT INTO b VALUES (20)").await.expect("seed b");
    drop(admin);

    let mut router = RangeRouter::connect(&c).await;
    router.simple("BEGIN").await.expect("begin");
    router.simple("UPDATE a SET id = 11 WHERE id = 10").await.expect("a pins range 0");
    router.simple("UPDATE b SET id = 21 WHERE id = 20").await.expect("b escalates");
    // The buffer holds both held DMLs, range 0's FIRST (pre-escalation) then range 1's.
    assert_eq!(
        router.restage_buf_ranges(),
        vec![0, 1],
        "buffer must capture the pre-escalation local DML (range 0) AND the staged DML (range 1)"
    );
    router.simple("COMMIT").await.expect("commit");
    // Cleared at txn close.
    assert!(router.restage_buf_ranges().is_empty(), "buffer cleared on finish_txn");
}
```

- [ ] **Step 2: Run it to confirm it fails to compile**

Run: `cargo test -p cluster --lib restage_buffer_captures_full_cross_range_write_set`
Expected: FAIL — `restage_buf_ranges` does not exist.

- [ ] **Step 3: Add the field + initializer + test accessor**

In the `RangeRouter` struct (after `cur_sql: String,` at `router.rs:188`), add:

```rust
    /// Per-txn cross-range write-set: every held table-bearing DML of a `BEGIN..COMMIT`
    /// block as `(target range, parsed statement, exact source SQL)`, in execution
    /// order. The replay log for the SP21 fresh-`g'` re-attempt; empty outside an
    /// escalatable cross-range txn, cleared at `finish_txn`. Carries the source `String`
    /// because the remote stage path relays `cur_sql` text while the local path runs the
    /// parsed `Statement`.
    restage_buf: Vec<(RangeId, Statement, String)>,
```

In `RangeRouter::new` (the `Self { ... }` literal, after `cur_sql: String::new(),` at `router.rs:219`), add:

```rust
            restage_buf: Vec::new(),
```

Add a `#[cfg(test)]` accessor next to `staged_global_xid` (`router.rs:238`):

```rust
    /// Test-only: the ordered target ranges currently in the re-stage write-set buffer.
    #[cfg(test)]
    fn restage_buf_ranges(&self) -> Vec<RangeId> {
        self.restage_buf.iter().map(|(r, _, _)| *r).collect()
    }
```

- [ ] **Step 4: Add a recording helper and call it at every held-DML point**

Add a private helper near `stage_on` (`router.rs:493`):

```rust
    /// Record one held table-bearing DML into the per-txn re-stage write-set buffer.
    /// Called at every point a DML is held (run locally OR staged) inside a txn that
    /// can escalate, so a fresh-`g'` re-attempt can replay the complete write-set.
    fn record_write(&mut self, range: RangeId, stmt: &Statement) {
        self.restage_buf.push((range, stmt.clone(), self.cur_sql.clone()));
    }
```

In `dispatch`, add `self.record_write(...)` calls at the four held-DML sites (only when the router can escalate — a non-escalatable router never re-attempts, so skip the buffer overhead). Insert each call immediately BEFORE the held execution:

1. `Pin::Open` led-local first DML (before `router.rs:353` `return self.run_on(r, stmt).await;`):

```rust
                        self.pin = Pin::Range(r);
                        self.ensure_began_on(r).await?;
                        if self.can_escalate() {
                            self.record_write(r, stmt);
                        }
                        return self.run_on(r, stmt).await;
```

2. `Pin::Open` remote-first escalation (before `router.rs:370` `self.stage_on(r, g, stmt).await`):

```rust
                    self.pin = Pin::Global { ranges, g };
                    self.record_write(r, stmt);
                    self.stage_on(r, g, stmt).await
```

3. `Pin::Range(p)` escalation (before `router.rs:400` `return self.stage_on(r, g, stmt).await;`). The already-pinned `p`'s DML was recorded at site 1, so record only `r`:

```rust
                    self.pin = Pin::Global { ranges, g };
                    self.record_write(r, stmt);
                    return self.stage_on(r, g, stmt).await;
```

4. `Pin::Range(p)` same-range run (before `router.rs:403` `self.run_on(p, stmt).await`). These are additional held DMLs on the already-pinned local range; they must replay too:

```rust
                // Same range (or no-table statement): run on the pinned session.
                if pinning == Some(p) && self.can_escalate() {
                    self.record_write(p, stmt);
                }
                self.run_on(p, stmt).await
```

5. `Pin::Global` steady-state staging (before `router.rs:415` `return self.stage_on(r, g, stmt).await;`):

```rust
                    return {
                        self.record_write(r, stmt);
                        self.stage_on(r, g, stmt).await
                    };
```

(Site 4's `pinning == Some(p)` guard avoids recording a no-table statement — DDL/FROM-less SELECT — that runs on the pinned session but carries no row write.)

- [ ] **Step 5: Clear the buffer at `finish_txn`**

At the top of `finish_txn` (`crates/cluster/src/range/router.rs:426`), the buffer must be cleared on every arm. Insert a bare clear BEFORE the `match` (the txn is closing; the write-set is no longer needed):

```rust
    async fn finish_txn(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        self.restage_buf.clear();
        match std::mem::replace(&mut self.pin, Pin::None) {
```

- [ ] **Step 6: Run the buffer test + full cluster regression**

Run: `cargo test -p cluster --lib restage_buffer_captures_full_cross_range_write_set`
Expected: PASS.
Run: `cargo nextest run -p cluster`
Expected: PASS — the buffer is additive; all existing cross-range tests stay green.

- [ ] **Step 7: Commit**

```bash
git add crates/cluster/src/range/router.rs
git commit -m "feat(sp21): per-txn cross-range write-set buffer in the router

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Bounded fresh-`g'` re-attempt loop + synthetic stage-fault seam (router)

**Files:**
- Modify: `crates/cluster/src/range/router.rs` (struct, `new`, `stage_on` 494, the three `dispatch` stage sites; add the re-attempt fn + constant)
- Test: `crates/cluster/src/range/router.rs` `#[cfg(test)] mod tests`

This is the core. The re-attempt intercepts `stage_on`→`NotLeader`, aborts the abandoned `g` (durable Abort + release staged participants), mints `g'`, and replays the buffer. Bounded to avoid an infinite loop under flapping leaders; on exhaustion it surfaces today's retryable abort.

- [ ] **Step 1: Write the failing transparent-commit test**

Add to `router.rs`'s `#[cfg(test)] mod tests` (mirror `cross_range_update_commits_atomically` + `coordinator_pause_seam_holds_a_txn_in_doubt`):

```rust
/// SP21: a participant-leader move during STAGE (modeled by a one-shot synthetic
/// `NotLeader` injected on range 1's first stage) is absorbed by the coordinator: the
/// cross-range txn COMMITs transparently, both rows read back with the re-attempt
/// values (exactly one live version each), and the abandoned `g` is durably Aborted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn participant_move_during_stage_reattempts_and_commits() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
    for r in c.range_map().range_ids() {
        c.wait_for_leader(r).await;
    }
    let mut admin = RangeRouter::connect(&c).await;
    admin.simple("CREATE TABLE a (id int4)").await.expect("a"); // id 1 -> range 0
    admin.simple("CREATE TABLE b (id int4)").await.expect("b"); // id 2 -> range 1
    admin.simple("INSERT INTO a VALUES (10)").await.expect("seed a");
    admin.simple("INSERT INTO b VALUES (20)").await.expect("seed b");
    drop(admin);

    let mut router = RangeRouter::connect(&c).await;
    // Inject ONE synthetic NotLeader on range 1's first stage; capture the abandoned g.
    let fired = Arc::new(AtomicBool::new(false));
    let abandoned_g = Arc::new(AtomicU64::new(0));
    {
        let fired = fired.clone();
        let abandoned_g = abandoned_g.clone();
        router.set_stage_fault(Box::new(move |range, g| {
            if range == 1 && !fired.swap(true, Ordering::SeqCst) {
                abandoned_g.store(g, Ordering::SeqCst);
                Some(ExecError::NotLeader)
            } else {
                None
            }
        }));
    }

    router.simple("BEGIN").await.expect("begin");
    router.simple("UPDATE a SET id = 11 WHERE id = 10").await.expect("a pins range 0");
    // This UPDATE escalates and stages range 1 -> the injected NotLeader fires -> the
    // coordinator aborts g, mints g', and replays [a, b] under g'. The client sees Ok.
    router.simple("UPDATE b SET id = 21 WHERE id = 20").await.expect("b re-attempts and stages");
    router.simple("COMMIT").await.expect("commit");

    // A fresh router reads the re-attempt values through the global clog: exactly one
    // live version per row (the g' version; the aborted-g shadow is invisible).
    let mut fresh = RangeRouter::connect(&c).await;
    assert_eq!(fresh.scan_one_i32("SELECT id FROM a").await, vec![11]);
    assert_eq!(fresh.scan_one_i32("SELECT id FROM b").await, vec![21]);

    // The abandoned g is durably Aborted: re-deciding it Committed still reads back
    // Aborted (write-once), proving the coordinator finalized it as Aborted.
    let g = abandoned_g.load(Ordering::SeqCst);
    assert!(g != 0, "the synthetic fault fired and captured the abandoned g");
    let range0 = c.leader_engine(0).await;
    assert_eq!(
        range0
            .commit_global_decision(g, mvcc::clog::XidStatus::Committed)
            .await
            .expect("re-decide"),
        mvcc::clog::XidStatus::Aborted,
        "abandoned g was durably Aborted by the re-attempt"
    );
}
```

- [ ] **Step 2: Run it to confirm it fails to compile**

Run: `cargo test -p cluster --lib participant_move_during_stage_reattempts_and_commits`
Expected: FAIL — `set_stage_fault` does not exist.

- [ ] **Step 3: Add the synthetic stage-fault seam**

In the `RangeRouter` struct (after `before_global_decision` at `router.rs:195`, inside the same `#[cfg(test)]` region or a new one):

```rust
    /// Test-only synthetic stage fault: invoked at the top of `stage_on` with the target
    /// range + global xid. Returning `Some(err)` makes the stage fail with that error
    /// WITHOUT touching the engine — the in-process way to model a participant-leader
    /// move (`Some(ExecError::NotLeader)`) deterministically (the harness's static engine
    /// map cannot model a real cross-node move). `None` in production (inert).
    #[cfg(test)]
    stage_fault: Option<Box<dyn FnMut(RangeId, u64) -> Option<ExecError> + Send>>,
```

In `RangeRouter::new` (after `before_global_decision: None,` at `router.rs:221`):

```rust
            #[cfg(test)]
            stage_fault: None,
```

Add the setter next to `set_before_global_decision` (`router.rs:229`):

```rust
    /// Install the test-only synthetic stage fault (see `stage_fault`).
    #[cfg(test)]
    fn set_stage_fault(&mut self, hook: Box<dyn FnMut(RangeId, u64) -> Option<ExecError> + Send>) {
        self.stage_fault = Some(hook);
    }
```

Fire it at the TOP of `stage_on` (immediately after the `async fn stage_on(...) -> ... {` opening at `router.rs:499`, before the `if self.engines.contains_key(&range)` line):

```rust
        #[cfg(test)]
        if let Some(hook) = self.stage_fault.as_mut() {
            if let Some(err) = hook(range, g) {
                return Err(err);
            }
        }
```

- [ ] **Step 4: Add the re-attempt constant and function**

Add near the top of the `impl RangeRouter` block (or as a module const above the struct):

```rust
/// Maximum fresh-`g'` re-attempts for one cross-range statement before the coordinator
/// gives up and surfaces the retryable abort to the client (graceful degradation to the
/// pre-SP21 behavior). Bounds churn under a persistently flapping participant leader.
const MAX_REATTEMPTS: usize = 5;
```

Add the re-attempt function as a method on `RangeRouter` (near `stage_on`):

```rust
    /// A participant's stage returned `NotLeader` (its leader moved). Transparently
    /// re-attempt the whole cross-range txn under a fresh `g'`: abort the abandoned `g`
    /// (durable Abort + release every staged participant), mint `g'`, and replay the
    /// buffered write-set. Bounded by `MAX_REATTEMPTS`; on exhaustion returns
    /// `Err(NotLeader)` (today's retryable abort) leaving the last `g` staged for the
    /// client's ROLLBACK to release. `failing_r` is the range whose stage just failed —
    /// it never staged under the current `g`, so it is excluded from the release loop.
    async fn reattempt_under_fresh_g(
        &mut self,
        failing_r: RangeId,
    ) -> Result<QueryResult, ExecError> {
        use std::collections::BTreeSet;
        let coord = self.coordinator.as_ref().expect("coordinator").clone();
        let mut failing_r = failing_r;
        for _ in 0..MAX_REATTEMPTS {
            let (ranges, g) = match &self.pin {
                Pin::Global { ranges, g } => (ranges.clone(), *g),
                _ => return Err(ExecError::Unsupported("re-attempt outside a global txn".into())),
            };
            // 1. Durably abort the abandoned g (write-once). It is unreachable for this to
            //    return the COMMITTED effective decision (nothing writes Committed(g)
            //    before finish_txn); if it ever does, do NOT re-attempt (would double-
            //    apply) — surface the retryable abort instead.
            let effective = coord.commit_global(g, false).await?;
            if effective {
                debug_assert!(false, "abandoned g committed during re-attempt");
                return Err(ExecError::NotLeader);
            }
            // 2. Release every participant that STAGED under g (all of `ranges` except the
            //    range whose stage just failed — it never staged). Local: abort_release;
            //    remote: release_remote is idempotent (a no-op for an unknown (g,range)).
            //    SCOPE NOTE: excluding `failing_r` conflates "the failing range" with "a
            //    never-staged range", valid only while each range is touched at most once
            //    per cross-range txn (the bank workload + every test — the spec's bounded
            //    scope). A future multi-statement-per-range caller that re-touches an
            //    already-staged range then fails would leak that lock until a sweep reclaims
            //    it; the debug_assert makes that unsupported case loud rather than silent.
            debug_assert!(
                self.restage_buf.iter().filter(|(r, _, _)| *r == failing_r).count() <= 1,
                "SP21 re-attempt assumes single-stage-per-range; range {failing_r} was re-touched"
            );
            for r in ranges.iter().copied().filter(|&r| r != failing_r) {
                if self.engines.contains_key(&r) && self.leads.leads(r) {
                    self.session_mut(r).abort_release();
                } else {
                    let _ = coord.release_remote(g, r, false).await;
                }
            }
            // 3. Mint a fresh g'. If range 0 (the coordinator) itself moved, begin_global
            //    returns NotLeader — that is a coordinator move (out of SP21 scope);
            //    propagate it (g is already aborted + released, nothing leaks).
            let g2 = coord.begin_global().await?;
            // 4. Replay the buffered write-set under g'. A range joins the new participant
            //    set only AFTER it successfully stages, so `ranges` (and what finish_txn
            //    later releases) tracks exactly the staged set.
            self.pin = Pin::Global { ranges: BTreeSet::new(), g: g2 };
            let buf = self.restage_buf.clone();
            let mut last = QueryResult::Command { tag: "OK".into() };
            let mut moved: Option<RangeId> = None;
            for (br, bstmt, bsql) in &buf {
                self.cur_sql = bsql.clone();
                match self.stage_on(*br, g2, bstmt).await {
                    Ok(q) => {
                        last = q;
                        if let Pin::Global { ranges, .. } = &mut self.pin {
                            ranges.insert(*br);
                        }
                    }
                    Err(ExecError::NotLeader) => {
                        moved = Some(*br);
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
            match moved {
                None => return Ok(last), // clean replay: the current statement staged under g'
                Some(m) => failing_r = m, // a participant moved during replay; loop under g''
            }
        }
        // Budget exhausted: surface today's retryable abort. The last g is left staged in
        // `self.pin`; the client's ROLLBACK runs finish_txn, which aborts it + releases.
        Err(ExecError::NotLeader)
    }
```

- [ ] **Step 5: Route the three stage sites through the re-attempt interceptor**

At each of the three `stage_on` call sites in `dispatch` that stage under a global `g`, wrap the result so a `NotLeader` drives the re-attempt. Replace each `self.stage_on(r, g, stmt).await` (sites 2, 3, 5 from Task 2) with the matching-arm form below.

Site at `router.rs:370` (Pin::Open remote-first), after `self.record_write(r, stmt);`:

```rust
                    match self.stage_on(r, g, stmt).await {
                        Err(ExecError::NotLeader) => self.reattempt_under_fresh_g(r).await,
                        other => other,
                    }
```

Site at `router.rs:400` (Pin::Range escalation), after `self.record_write(r, stmt);`:

```rust
                    return match self.stage_on(r, g, stmt).await {
                        Err(ExecError::NotLeader) => self.reattempt_under_fresh_g(r).await,
                        other => other,
                    };
```

Site at `router.rs:415` (Pin::Global steady), after `self.record_write(r, stmt);`:

```rust
                    return match self.stage_on(r, g, stmt).await {
                        Err(ExecError::NotLeader) => self.reattempt_under_fresh_g(r).await,
                        other => other,
                    };
```

(The `stage_on` calls INSIDE `reattempt_under_fresh_g`'s replay loop are NOT wrapped — they handle `NotLeader` directly via the `moved` branch.)

- [ ] **Step 6: Run the transparent-commit test**

Run: `cargo test -p cluster --lib participant_move_during_stage_reattempts_and_commits`
Expected: PASS — the txn commits, both values read back, the abandoned `g` is Aborted.

- [ ] **Step 7: Write + run the bounded-retry-exhaustion test**

Add to the same module:

```rust
/// SP21: under a participant whose stage ALWAYS returns NotLeader (a persistently
/// flapping leader), the coordinator gives up after the bounded budget and surfaces the
/// retryable abort — no infinite loop, no hang. (Range 0's stages succeed; only range 1
/// flaps, so each round cleanly aborts + re-begins.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_reattempt_gives_up_under_persistent_move() {
    let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
    for r in c.range_map().range_ids() {
        c.wait_for_leader(r).await;
    }
    let mut admin = RangeRouter::connect(&c).await;
    admin.simple("CREATE TABLE a (id int4)").await.expect("a");
    admin.simple("CREATE TABLE b (id int4)").await.expect("b");
    admin.simple("INSERT INTO a VALUES (10)").await.expect("seed a");
    admin.simple("INSERT INTO b VALUES (20)").await.expect("seed b");
    drop(admin);

    let mut router = RangeRouter::connect(&c).await;
    // Range 1 ALWAYS returns NotLeader.
    router.set_stage_fault(Box::new(
        |range, _g| if range == 1 { Some(ExecError::NotLeader) } else { None },
    ));

    router.simple("BEGIN").await.expect("begin");
    router.simple("UPDATE a SET id = 11 WHERE id = 10").await.expect("a pins range 0");
    // The escalating UPDATE b re-attempts MAX_REATTEMPTS times, each failing, then
    // surfaces the retryable abort (NotLeader -> 40001). Bounded; must return, not hang.
    let err = router.simple("UPDATE b SET id = 21 WHERE id = 20").await.unwrap_err();
    assert!(
        format!("{err:?}").contains("40001") || format!("{err:?}").to_lowercase().contains("not"),
        "exhausted re-attempt surfaces the retryable abort, got {err:?}"
    );
    // The client rolls back; finish_txn releases the last staged g (no stranded lock).
    router.simple("ROLLBACK").await.expect("rollback");

    // A fresh txn proceeds — no stranded locks from the exhausted attempt. Clear the
    // fault first so this control txn can stage range 1.
    let mut after = RangeRouter::connect(&c).await;
    after.simple("BEGIN").await.expect("begin");
    after.simple("UPDATE a SET id = 12 WHERE id = 11").await.expect("a");
    after.simple("ROLLBACK").await.expect("rollback");
    assert_eq!(after.scan_one_i32("SELECT id FROM a").await, vec![10], "a unchanged");
}
```

Run: `cargo test -p cluster --lib bounded_reattempt_gives_up_under_persistent_move`
Expected: PASS — returns the retryable abort within the budget, no hang; a later txn proceeds (no stranded lock). (`PgError`'s `Debug`/`Display` carries SQLSTATE `40001` for `NotLeader` — `crates/executor/src/error.rs:66-68`; the assertion tolerates either the code or the word "not leader".)

- [ ] **Step 8: Run the whole cluster suite + clippy + fmt**

Run: `cargo nextest run -p cluster`
Expected: PASS — all cross-range + routing tests green.
Run: `cargo clippy -p cluster --all-targets -- -D warnings`
Expected: clean.
Run: `cargo fmt -p cluster`

- [ ] **Step 9: Commit**

```bash
git add crates/cluster/src/range/router.rs
git commit -m "feat(sp21): bounded fresh-g' re-attempt on a participant-leader move during STAGE

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Multi-process kill-during-stage conservation nemesis

**Files:**
- Create: `crates/crabgresql/tests/crossrange_2pc_restage.rs` (UAC-safe target name — no `setup/install/update/patch/upgrad`)
- Modify: `CLAUDE.md` (SP21 audit line)

This proves **conservation + liveness** under a real participant-range leader killed during the STAGE window. The conservation oracle is the decisive correctness gate — a double-apply or lost write breaks the bank total.

**What this test does and does not guarantee (accuracy note).** `TwoPcClient::call` already re-resolves a moved leader and retries once over the wire (`twopc.rs:79-116`, `await_leader` blocks up to `TXN_TIMEOUT`). When range 1's 4-node quorum re-elects within that window, the move is absorbed *below* the router and `ExecError::NotLeader` never reaches the coordinator — so `reattempt_under_fresh_g` may execute zero times while conservation still holds. This test is therefore NOT flaky (conservation + liveness hold regardless of whether the re-attempt branch fires), but it does NOT deterministically exercise the SP21 re-attempt branch. The **deterministic** proof that the re-attempt branch executes lives in the in-process synthetic-injection tests (Task 3). This test's value is that the system stays correct + live when a participant leader is killed during cross-range staging — a fault class SP18/SP19 (which killed coordinators / non-leaders) did not target.

The structure mirrors the proven `crossrange_2pc_nemesis.rs` exactly; the ONLY behavioral change is the victim: kill the **range-1 participant leader** (acct_b, the second UPDATE's target) instead of a non-leader.

- [ ] **Step 1: Create the test file by copying + adapting the nemesis**

Create `crates/crabgresql/tests/crossrange_2pc_restage.rs`. Copy the ENTIRE module-local helper block verbatim from `crates/crabgresql/tests/crossrange_2pc_nemesis.rs:154-295` (`ctl_set_partition`, `ctl_heal`, `Lcg`, `connect`, `cross_transfer`, `first_i64`, `read_total_cross_until_ok`, `try_read_total_cross`) — this verbatim duplication is the established convention (the same block is already copied into `crossrange_2pc_replicated.rs`; do NOT factor a shared helper). Then write the test body:

```rust
//! SP21 D3c-restage: cross-range 2PC conserves the bank total under a multi-process
//! nemesis that kills the PARTICIPANT-RANGE LEADER (acct_b, range 1) during the STAGE
//! window — while the coordinator (a worker's gateway) stays alive. The coordinator's
//! transparent fresh-`g'` re-attempt (or, on exhaustion, a client-visible retryable
//! abort that nets zero) keeps the total conserved with no half-applied transfer.
mod harness;
use harness::Cluster;
use std::time::Duration;

use cluster::transport::protocol::ControlRequest;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_range_bank_conserves_total_under_participant_leader_kill() {
    const ACCOUNTS: i64 = 4;
    const SEED: i64 = 100;
    const PROCS: usize = 3;
    const OPS: usize = 8;
    const MIN_ROUNDS: usize = 4;
    let seeded_total = 2 * ACCOUNTS * SEED;

    // 5 nodes, boundary [2]: acct_a (id 1) -> range 0, acct_b (id 2) -> range 1. Killing
    // range 1's leader leaves a 4-node quorum to re-elect.
    let mut c = Cluster::spawn_multirange(5, vec![2]).await;
    let committed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    c.exec_until_ok("CREATE TABLE acct_a (id int8, bal int8)").await;
    c.exec_until_ok("CREATE TABLE acct_b (id int8, bal int8)").await;
    for id in 0..ACCOUNTS {
        c.exec_until_ok(&format!("INSERT INTO acct_a VALUES ({id}, {SEED})")).await;
        c.exec_until_ok(&format!("INSERT INTO acct_b VALUES ({id}, {SEED})")).await;
    }

    let addrs: Vec<String> = (0..c.len()).map(|i| c.sql_addr(i as u64).to_string()).collect();
    let mut workers = Vec::new();
    for process in 0..PROCS {
        let addrs = addrs.clone();
        let sig = committed.clone();
        workers.push(tokio::spawn(async move {
            use std::sync::atomic::Ordering;
            let mut rng = Lcg::new(0x9E37_79B9_u64.wrapping_mul(process as u64 + 1));
            let mut n = 0usize;
            for _ in 0..OPS {
                let node = addrs[process % addrs.len()].clone();
                let Some(client) = connect(&node).await else { continue };
                let from = (rng.next() % ACCOUNTS as u64) as i64;
                let mut to = (rng.next() % ACCOUNTS as u64) as i64;
                if to == from {
                    to = (to + 1) % ACCOUNTS;
                }
                let amt = 1 + (rng.next() % 20) as i64;
                if cross_transfer(&client, from, to, amt).await {
                    n += 1;
                    sig.fetch_add(1, Ordering::Relaxed);
                }
            }
            n
        }));
    }

    // Nemesis: kill the PARTICIPANT-RANGE LEADER (range 1) so a cross-range txn that has
    // pinned range 0 hits a Stage->NotLeader on range 1 and must re-attempt under g'.
    // Pace on a committed-op progress signal (no settle-sleep); await recovered quorum.
    use std::sync::atomic::Ordering;
    let mut round = 0usize;
    while !workers.iter().all(|w| w.is_finished()) || round < MIN_ROUNDS {
        let victim = c.range_leader(1).await; // the acct_b participant leader
        let before = committed.load(Ordering::Relaxed);
        if round.is_multiple_of(2) {
            c.kill(victim).await;
            c.respawn(victim);
        } else {
            let others: Vec<u64> = (0..c.len() as u64).filter(|&i| i != victim).collect();
            let _ = c.control(victim, ctl_set_partition(others.clone())).await;
            for &o in &others {
                let _ = c.control(o, ctl_set_partition(vec![victim])).await;
            }
            for id in 0..c.len() as u64 {
                let _ = c.control(id, ctl_heal()).await;
            }
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while committed.load(Ordering::Relaxed) == before
            && !workers.iter().all(|w| w.is_finished())
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(100)).await; // harness poll cadence, not a settle-sleep
        }
        c.range_leader(0).await;
        c.range_leader(1).await;
        round += 1;
    }
    let mut total_committed = 0usize;
    for w in workers {
        total_committed += w.await.expect("worker");
    }

    for id in 0..c.len() as u64 {
        let _ = c.control(id, ctl_heal()).await;
    }
    c.range_leader(0).await;
    c.range_leader(1).await;

    // RECOVERY-REQUIRED: a post-heal transfer touching every account pair must commit
    // within bound (a stranded participant lock would block this forever).
    for id in 0..ACCOUNTS {
        let other = (id + 1) % ACCOUNTS;
        c.exec_until_ok(&format!(
            "BEGIN; UPDATE acct_a SET bal = bal - 0 WHERE id = {id}; UPDATE acct_b SET bal = bal + 0 WHERE id = {other}; COMMIT"
        )).await;
    }

    let total = read_total_cross_until_ok(&c, ACCOUNTS).await;
    assert_eq!(
        total, seeded_total,
        "cross-range transfers conserve the total under participant-leader-kill nemesis (got {total}, want {seeded_total})"
    );
    assert!(total_committed > 0, "the workload must commit at least one transfer (non-vacuous)");
}
```

- [ ] **Step 2: Run the new nemesis test (2-3× for non-flakiness)**

Run: `cargo nextest run -p crabgresql --test crossrange_2pc_restage`
Expected: PASS. Run it 2-3 times to confirm it is not flaky on a loaded machine.

- [ ] **Step 3: Append the SP21 CLAUDE.md audit line + run the UAC guard**

Add a new `**SP21 (2026-06-14):**` paragraph to `CLAUDE.md`'s Windows UAC section (after the SP20 paragraph), modeled on the SP18/SP19 entries: note the one new binary `crabgresql::crossrange_2pc_restage` is UAC-safe (no forbidden substring); the crabgresql list now reads `{crossrange_2pc_net, crossrange_2pc_nemesis, crossrange_2pc_replicated, crossrange_2pc_restage, jepsen_elle, meta_range_gateway, multiprocess, multirange_gateway}`; SP21 adds no new dependency; the full guard returns empty.

Run: `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'`
Expected: empty output (the new `crossrange_2pc_restage.rs` does not trip the guard).

- [ ] **Step 4: Commit**

```bash
git add crates/crabgresql/tests/crossrange_2pc_restage.rs CLAUDE.md
git commit -m "test(sp21): multi-process participant-leader-kill-during-stage conservation nemesis

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Gauntlet, traceability, finish

**Files:**
- Modify: `docs/superpowers/specs/2026-06-14-crabgresql-sp21-d3c-restage-coordinator-reattempt-design.md` (Traceability section)

- [ ] **Step 1: Full-workspace format + lint**

Run: `cargo fmt --all --check`
Expected: clean (if not, run `cargo fmt --all` and re-stage).
Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 2: Full test suite + doctests**

Run: `cargo nextest run --workspace`
Expected: PASS (all suites, including the new SP21 in-process tests and the multi-process nemesis).
Run: `cargo test --workspace --doc`
Expected: PASS (nextest does not run doctests).

- [ ] **Step 3: Dependency audit**

Run: `cargo deny check`
Expected: PASS — no new dependency was added (confirm `Cargo.lock` is unchanged apart from nothing).

- [ ] **Step 4: Fill in the spec's Traceability section**

Replace the placeholder Traceability section of the spec with a table mapping each success criterion 1–7 to its proving test:

| # | Criterion | Test |
|---|---|---|
| 1 | Transparent commit on mid-stage move | `router.rs::participant_move_during_stage_reattempts_and_commits` |
| 2 | Exactly one live version, correct value | same test (fresh-router read-back) + `exec.rs::find_visible_one_returns_committed_reattempt_over_aborted_shadow` |
| 3 | Abandoned `g` durably Aborted, locks freed | same test (write-once re-decide assertion) + `bounded_reattempt_gives_up_under_persistent_move` (no stranded lock) |
| 4 | Bounded retry, no hang | `router.rs::bounded_reattempt_gives_up_under_persistent_move` |
| 5 | `find_visible_one` highest-xmin + at-most-one-live | `exec.rs::find_visible_one_orders_by_xmin_and_flags_multiple_live` + executor regression |
| 6 | Conservation + liveness under participant-leader kill during STAGE (the deterministic re-attempt-branch proof is criteria 1–4) | `crossrange_2pc_restage.rs::cross_range_bank_conserves_total_under_participant_leader_kill` |
| 7 | Gauntlet green, no new dep, UAC-safe | this task |

- [ ] **Step 5: Commit the traceability + finish**

```bash
git add docs/superpowers/specs/2026-06-14-crabgresql-sp21-d3c-restage-coordinator-reattempt-design.md
git commit -m "docs(sp21): traceability table — criteria 1-7 mapped to proving tests

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 6: Finish the branch**

Use superpowers:finishing-a-development-branch. The user's standing preference is option 2 (push to a fresh non-force branch + create a PR). PR body ends with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.

---

## Self-Review

**Spec coverage:** Spec components (A) write-set buffer → Task 2; (B) re-attempt loop → Task 3; (C) `find_visible_one` hardening → Task 1; (D.1) in-process tests → Task 3; (D.2) multi-process nemesis → Task 4. Success criteria 1–7 → Task 5 traceability table. The two locked decisions (explicitly abort the abandoned `g`; silent replay of already-returned statements) are implemented in `reattempt_under_fresh_g` (Task 3 Step 4: `commit_global(g,false)` + release loop; the replay discards intermediate results via `let mut last = ...` and returns only the final stage). All covered.

**Deviation from the spec's test plan (flag to the user):** the spec mapped criteria 1 & 2 to an "in-process pause-during-stage test" and criterion 6 to a multi-process nemesis that exercises the re-attempt branch. Anchor mapping + the adversarial plan review revealed two harness realities: (1) the in-process harness (`AlwaysLeads` + `LocalCoordinator` + static engine map) cannot model a real cross-node leader move, so the plan proves criteria 1–4 in-process via a **synthetic `NotLeader` injection** seam (deterministic, no election; the replay re-routes to the same still-valid local engine); and (2) `TwoPcClient` already re-resolves a moved leader over the wire and retries once, so the multi-process nemesis (Task 4) may absorb the move *below* the router — it reliably proves **conservation + liveness** under a participant-leader kill, but does NOT deterministically exercise the re-attempt branch. Net: the **deterministic** re-attempt-mechanism proof (abort `g` → mint `g'` → replay → commit/exhaust) lives in the in-process tests (criteria 1–4); the multi-process test proves the system stays correct + live under the real fault (criterion 6). Same criteria, honest harness split; no behavior or scope change. (A future option, deliberately deferred to keep this slice tight: a process-side re-attempt counter surfaced via a control RPC, asserted to advance — adds a `ControlRequest` variant + plumbing for a stronger criterion-6.)

**Placeholder scan:** No TBD/TODO; every code step shows complete code. Task 4 copies an existing verbatim helper block (an explicit, established convention) — not a placeholder.

**Type consistency:** `restage_buf: Vec<(RangeId, Statement, String)>` defined in Task 2, consumed in Task 3 (`reattempt_under_fresh_g` clones it). `stage_fault: Option<Box<dyn FnMut(RangeId, u64) -> Option<ExecError> + Send>>` defined and fired (Task 3 Steps 3) and installed by `set_stage_fault` (used by both Task 3 tests). `record_write(&mut self, RangeId, &Statement)`, `reattempt_under_fresh_g(&mut self, RangeId) -> Result<QueryResult, ExecError>`, `MAX_REATTEMPTS: usize` consistent across steps. `commit_global(g, false) -> Result<bool, ExecError>`, `release_remote(g, r, false)`, `begin_global() -> Result<u64, ExecError>` match the `GlobalCoordinator` trait (`router.rs:41-51`). `find_visible_one`/`scan_live` signatures unchanged.

**Known out-of-scope edge case (documented):** a single cross-range txn that touches one range, then a DIFFERENT range, then RE-touches the first range and that re-touch hits a mid-stage move — the first abort would exclude the re-touched range from release via `filter(r != failing_r)`, leaking its earlier-staged lock. No workload or test re-touches a range within a cross-range txn (the bank transfers each range once), so this does not arise; it is consistent with the spec's bounded scope and noted for a future multi-statement-per-range slice.
