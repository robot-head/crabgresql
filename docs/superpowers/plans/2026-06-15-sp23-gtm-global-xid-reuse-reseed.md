# SP23 / GTM global-xid reuse across range-0 leadership change — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A range-0 leadership change never reuses a global xid: the range-0 rise sweep apply-waits, reseeds the GTM counter from the now-current applied state, settles inherited in-doubt markers, then opens range 0's `RecoveryGate` — and the gate blocks `begin_global` (allocation) + participant writes until then.

**Architecture:** Extend SP22's per-range `RecoveryGate` to range 0. The range-0 leadership-rise sweep (`resolve_in_doubt_on_leadership`) gains a `reseed_gtm`/`reseed_counters` step *after* its apply-wait and *before* `mark_served`. `begin_global` (handled in `transport/server.rs::handle_txn`) is gated on range 0's gate via a new `TxnService::is_serving` accessor. The recovery scan is bounded at `GLOBAL_XID_BASE` so range 0's mixed clog (participant markers + global decisions) doesn't corrupt the watermark.

**Tech Stack:** Rust 2024, openraft 0.9.24, arc-swap, tokio, cargo-nextest, stateright. **No new dependency.**

**Stacked on SP22** (branch `sp23-gtm-global-xid-reuse-reseed` off `sp22-d3c-settle-before-serve`). Rebase `--onto origin/main` once SP22 (PR #38) squash-merges. A throwaway reference implementation of the range-0 registration + sweep + scan-bound (NOT the reseed/gate-begin parts) is on branch `sp23-range0-probe` (commit `f022514`).

---

## Background the implementer needs (read once)

**The bug** (probe-confirmed, deterministic). `Gtm::begin_global` (`executor/src/gtm.rs:59`) bumps the **in-memory** `next_global` and returns `g`; `begin_global_durable` (`executor/src/lib.rs:219`) commits `next_global = g+1` to quorum *before* returning `g`. On a range-0 leader kill, the new leader's `reseed_on_leadership` (`cluster/src/server_node.rs:789`) calls `reseed_gtm` → `Gtm::reseed_from_applied` (`gtm.rs:75`) on the **rising edge with no apply-wait**, reading the *applied* store, which lags the committed advance. So in-memory `next_global` regresses below an allocated `g`; `begin_global` re-hands-out `g`; `TxnService::stage`'s `staged_local_for(g)` idempotency no-op aliases the prior txn's stale `Prepared(-> g)` marker → a duplicate live MVCC version on range 0 → a `+money` tear or a 2-live `debug_assert` crash-loop.

**The fix** = apply-currency before reseed + reseed-before-allocate. Reuse SP22's rise-sweep apply-wait; do the reseed inside the gate-opening sweep; gate `begin_global` on range 0's gate.

**SP22 anchors this builds on (already in the branch):**
- `cluster::RecoveryGate` (`cluster/src/recovery_gate.rs`): `is_serving(range)`, `mark_served(range, term)`, `register_range(range, raft)`. A range NOT registered → `is_serving` returns `true` (ungated). Range 0 is currently NOT registered.
- `resolve_in_doubt_on_leadership` (`cluster/src/server_node.rs`, the rise sweep): for a registered range, apply-waits (`ensure_linearizable` + `applied_index_at_least`, bounded by `SETTLE_TIMEOUT`), settles in-doubt markers, `mark_served`. Spawned per DATA range (`r != 0`).
- `TxnService { gate: Option<Arc<RecoveryGate>> }` (`cluster/src/twopc.rs`). The gate is a single shared `Arc` constructed once per bring-up path.
- The two write checks: `TxnService::stage` + `RangeRouter::dispatch` (gate Insert/Update/Delete).

**openraft / counter facts (confirmed):**
- `SqlEngine::reseed_gtm()` (`lib.rs:233`) → `Gtm::reseed_from_applied` (lift-only `max`). `SqlEngine::reseed_counters()` (`lib.rs:148`) reseeds procarray + seq.
- `SqlEngine::begin_global_durable()` (`lib.rs:219`) is leader-only; on a non-leader the committer returns `ExecError::NotLeader`.
- `mvcc::xid::GLOBAL_XID_BASE` = `1<<63`; local xids `< BASE`, global xids `>= BASE`. `kv::key::clog_key(xid)`, `kv::key::clog_scan_end()`, `kv::key::clog_xid_of(&k)` exist.

**Clippy:** workspace denies warnings; uses `is_some_and`/`is_none_or` (never `map_or(true/false, …)`). `#![forbid(unsafe_code)]`. No `sleep`-to-settle in tests (CLAUDE.md); the multi-process harness's bounded 100ms poll cadence is allowed.

---

## File structure

| File | Change | Responsibility |
|---|---|---|
| `crates/executor/src/lib.rs` | Modify | bound `in_doubt_globals_from` + `staged_local_for` scans at `GLOBAL_XID_BASE`; range-0-safe-scan unit test |
| `crates/executor/src/gtm.rs` | Modify | unit test: stale in-memory counter + reseed-prevents-reuse (teeth + positive) |
| `crates/cluster/src/server_node.rs` | Modify | register range 0 in the gate + spawn its sweep (both bring-up paths); the sweep reseeds (apply-waited) before `mark_served` |
| `crates/cluster/src/twopc.rs` | Modify | `TxnService::is_serving(range)` accessor (exposes the gate) |
| `crates/cluster/src/transport/server.rs` | Modify | gate `BeginGlobal` on range 0's gate (retryable `NotLeader`) |
| `crates/cluster/tests/crossrange_2pc_gtm_reuse_model.rs` | **Create** | Stateright model: reseed-before-allocate (teeth + positive) |
| `crates/crabgresql/tests/range0_leader_kill_drain.rs` | **Create** | multi-process range-0-leader-kill stable-window nemesis (UAC-safe name) |
| `CLAUDE.md` | Modify | SP23 audit line |

**Wiring order (each task green):** T1 bounds the scan (no-op on data ranges). T2 registers range 0 + adds the apply-waited reseed to the sweep (the g-reuse fix). T3 gates `begin_global`. T4 model, T5 nemesis, T6 finish.

---

## Task 1: Range-0-safe recovery scan (executor)

**Files:** Modify `crates/executor/src/lib.rs` (the `in_doubt_globals_from` + `staged_local_for` scans); add a `#[cfg(test)]` unit test.

- [ ] **Step 1: Write the failing test.** In `crates/executor/src/lib.rs`'s `#[cfg(test)] mod tests` (or the nearest recovery-scan test module), add a test that seeds a range-0-style clog with BOTH local participant markers (`Prepared(L0 -> g)` at `L0 < GLOBAL_XID_BASE`) AND global-decision entries (`Committed`/`Aborted` at `g >= GLOBAL_XID_BASE`), then asserts the scan returns only in-doubt participant `g`s and the watermark stays below `GLOBAL_XID_BASE`:

```rust
#[tokio::test]
async fn in_doubt_scan_ignores_global_decision_entries_on_range_0() {
    use mvcc::xid::GLOBAL_XID_BASE;
    let kv: std::sync::Arc<dyn kv::Kv> = std::sync::Arc::new(kv::MemKv::new());
    // Local participant marker for an in-doubt g (g's decision absent → in-doubt).
    let g_indoubt = GLOBAL_XID_BASE + 7;
    kv.put(kv::key::clog_key(5), mvcc::clog::encode(mvcc::clog::XidStatus::Prepared(g_indoubt))).expect("put L0 marker");
    // A global DECISION entry keyed in the global space — must NOT drive the watermark.
    kv.put(kv::key::clog_key(GLOBAL_XID_BASE + 3), mvcc::clog::encode(mvcc::clog::XidStatus::Committed)).expect("put global decision");
    let engine = SqlEngine::open_in_memory_with_kv(kv.clone()); // see Step 3 note on the real constructor
    let (gs, new_lo) = engine.in_doubt_globals_from(0).await.expect("scan");
    assert_eq!(gs, vec![g_indoubt], "only the in-doubt local participant g is returned");
    assert!(new_lo < GLOBAL_XID_BASE, "the watermark never jumps into the global-xid space (got {new_lo})");
}
```
**Adapt to the real test scaffolding:** read the existing `in_doubt_globals_from` tests in this file for the exact engine/kv constructor (`MemKv`, the `SqlEngine` test builder, and `mvcc::clog::encode`/`decode` helpers); mirror them. The KEY assertions to preserve: only `g_indoubt` is returned, and `new_lo < GLOBAL_XID_BASE`. Use the catalog_kv == kv (range-0 self) so the decidedness check reads the same store.

- [ ] **Step 2: Run → FAIL.** `cargo nextest run -p executor in_doubt_scan_ignores_global_decision_entries_on_range_0`. Expected: FAIL — without the bound, the scan iterates the `GLOBAL_XID_BASE+3` decision key, sets `max_li = GLOBAL_XID_BASE+3`, and `new_lo = GLOBAL_XID_BASE+4` (>= `GLOBAL_XID_BASE`), tripping the `new_lo < GLOBAL_XID_BASE` assert.

- [ ] **Step 3: Bound both scans.** In `in_doubt_globals_from` (`lib.rs:277`) and `staged_local_for` (`lib.rs:327`), change the scan upper bound from `kv::key::clog_scan_end()` to `kv::key::clog_key(mvcc::xid::GLOBAL_XID_BASE)`:
```rust
            .scan_range(&kv::key::clog_key(scan_lo), &kv::key::clog_key(mvcc::xid::GLOBAL_XID_BASE))?
```
Add `use mvcc::xid::GLOBAL_XID_BASE;` at the top of the file or fully-qualify (it's already referenced in `lib.rs` comments). On a data range every clog xid is `< GLOBAL_XID_BASE`, so the result set is identical; on range 0 the global-decision entries (keyed `>= GLOBAL_XID_BASE`) are excluded.

- [ ] **Step 4: Run → PASS.** `cargo nextest run -p executor in_doubt_scan_ignores_global_decision_entries_on_range_0` → PASS. Then `cargo nextest run -p executor` → all green (the bound must not regress existing recovery/idempotency tests).

- [ ] **Step 5: Lint + commit.**
`cargo clippy -p executor --all-targets -- -D warnings` clean; `cargo fmt -p executor`.
```bash
git add crates/executor/src/lib.rs
git commit -m "fix(sp23): bound the recovery scan at GLOBAL_XID_BASE (range-0-safe watermark)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Range-0 rise sweep — apply-wait → reseed → settle → open the gate

**Files:** Modify `crates/cluster/src/server_node.rs` (register range 0 + spawn its sweep in both bring-up paths; the sweep reseeds after the apply-wait). Add a unit test demonstrating reseed-prevents-reuse in `crates/executor/src/gtm.rs`.

- [ ] **Step 1: GTM reuse teeth test (executor).** In `crates/executor/src/gtm.rs`'s `#[cfg(test)] mod tests`, add a test that simulates a freshly-risen leader whose in-memory counter is stale relative to the durable counter, and asserts that WITHOUT a reseed the GTM re-hands-out an allocated `g`, and WITH `reseed_from_applied` it does not:
```rust
#[test]
fn stale_in_memory_counter_reuses_g_until_reseed() {
    let kv = std::sync::Arc::new(MemKv::new());
    let gtm = Gtm::open(kv.clone() as Arc<dyn Kv>).expect("open"); // in-memory next_global == BASE
    // A PRIOR leader durably allocated through BASE+4 (begin_global_durable committed next=BASE+5).
    kv.put(kv::key::meta_next_global_xid_key(), (GLOBAL_XID_BASE + 5).to_be_bytes().to_vec()).expect("put");
    // TEETH: a new leader that does NOT reseed re-hands-out BASE (already allocated by the prior leader).
    assert_eq!(gtm.begin_global(), GLOBAL_XID_BASE, "without reseed, the stale counter reuses g (the bug)");
    gtm.finish_global(GLOBAL_XID_BASE);
    // POSITIVE: after reseed_from_applied (durable value current), the next g is past every allocation.
    gtm.reseed_from_applied().expect("reseed");
    assert!(gtm.begin_global() >= GLOBAL_XID_BASE + 5, "after reseed, g is never reused");
}
```
(Mirror the existing `reseed_lifts_counter_and_never_regresses` test for imports — `MemKv`, `GLOBAL_XID_BASE`, `kv::key::meta_next_global_xid_key`.) Run: `cargo nextest run -p executor stale_in_memory_counter_reuses_g_until_reseed` → PASS (this documents the invariant the cluster apply-wait + gate enforce; no new executor code needed beyond Task 1).

- [ ] **Step 2: Add the reseed to the rise sweep.** In `crates/cluster/src/server_node.rs`, `resolve_in_doubt_on_leadership` (the SP22 rise sweep), AFTER the apply-wait (`ensure_linearizable` + `applied_index_at_least`) and BEFORE the settle/`mark_served`, add the reseed so the in-memory counters reflect the now-current applied high-water-mark:
```rust
                // SP23: reseed the GTM (and local) counters from the now-applied store BEFORE
                // opening the gate, so a range-0 leader never hands out a reused global xid.
                // The apply-wait above guarantees the applied store reflects every committed
                // begin_global_durable advance. reseed_* is lift-only (never regresses).
                engine.reseed_gtm().ok();
                engine.reseed_counters().ok();
```
Place this inside the `settled` async block (after `applied_index_at_least`, before `in_doubt_globals_from`), so a failed apply-wait skips it (the gate stays closed and retries — the SP22 retry-while-closed behavior). On a DATA-range engine `reseed_gtm` is a no-op (no GTM), so this is safe for every range's sweep.

- [ ] **Step 3: Register range 0 + spawn its sweep (both bring-up paths).** In `start_static` AND `start_replicated`, range 0 is built specially OUTSIDE the `filter(|&r| r != 0)` pending loop. After the gate and `sweep_client` (a `TwoPcClient`) both exist, add — `register_range` strictly BEFORE the spawn (the SP22 wedge-prevention ordering):
```rust
        gate.register_range(0, rafts[&0].clone());
        tokio::spawn(resolve_in_doubt_on_leadership(
            rafts[&0].clone(),
            0,
            cfg.id,
            engines[&0].clone(),
            sweep_client.clone(),
            gate.clone(),
        ));
```
Use the real in-scope range-0 bindings (`r0_raft`/`r0_engine` if still owned, else `rafts[&0]`/`engines[&0]`). Range 0's sweep abort-races inherited `Prepared(L0 -> g)` markers via `client.call(0, CommitGlobal{g, false})` (write-once-safe; the scan is range-0-bounded by Task 1). The probe (`sp23-range0-probe`) validated this registration compiles + runs.

- [ ] **Step 4: Build + run + lint.**
`cargo build -p cluster` → compiles.
`cargo nextest run -p cluster` → PASS (the gate now also gates range-0 participant writes via the existing SP22 `stage`/`dispatch` checks; in-process suites use a `None` gate so they're unaffected).
`cargo nextest run -p crabgresql --test crossrange_2pc_nemesis --test crossrange_2pc_replicated` → PASS (no regression; the range-0 sweep + reseed don't disturb the existing nemeses).
`cargo clippy -p cluster --all-targets -- -D warnings` clean; `cargo fmt -p cluster`; `cargo nextest run -p executor` → PASS.

- [ ] **Step 5: Commit.**
```bash
git add crates/cluster/src/server_node.rs crates/executor/src/gtm.rs
git commit -m "fix(sp23): range-0 rise sweep apply-waits, reseeds the GTM counter, then opens the gate

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Gate `begin_global` (GTM allocation) on range 0's gate

**Files:** Modify `crates/cluster/src/twopc.rs` (`TxnService::is_serving` accessor) + `crates/cluster/src/transport/server.rs` (gate `BeginGlobal`). Add an in-process gate test.

- [ ] **Step 1: Add the gate accessor.** In `crates/cluster/src/twopc.rs`, add a method on `TxnService` exposing the gate (returns `true` when there is no gate, mirroring `is_serving`'s ungated default):
```rust
    /// SP23: is `range` currently serving WRITES/ALLOCATION (its rise sweep settled the current
    /// term)? `true` when no gate is wired (in-process / never-recovering harness).
    pub fn is_serving(&self, range: RangeId) -> bool {
        self.gate.as_ref().is_none_or(|g| g.is_serving(range))
    }
```
(`is_none_or`: `None` ⇒ `true` (ungated); `Some(g)` ⇒ `g.is_serving(range)`. The `gate` field is `Option<Arc<RecoveryGate>>`, added in SP22.)

- [ ] **Step 2: Write the failing gate test.** In `twopc.rs`'s `#[cfg(test)] mod tests` (mirror SP22's `stage_is_gated_until_the_range_is_settled`), assert `begin_global` is gated on range 0:
```rust
/// SP23: BeginGlobal (GTM allocation) is rejected (retryable NotLeader) while range 0's rise
/// sweep has not settled the current term; admitted once the gate opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn begin_global_is_gated_until_range_0_is_settled() {
    let (node, _sql) = crate::server_node::testonly_two_range_node().await;
    let gate = crate::recovery_gate::RecoveryGate::new(node.id());
    gate.register_range(0, node.rafts[&0].clone());
    let svc = TxnService::new(node.engines.clone(), Some(gate.clone()));

    // Gate closed (sentinel term) → BeginGlobal is rejected, retryable.
    assert!(matches!(
        crate::transport::server::handle_txn_for_test(&svc, 0, TxnRpc::BeginGlobal).await,
        TxnResp::NotLeader
    ), "BeginGlobal is gated while range 0 is unsettled");

    // Open the gate for the current term → BeginGlobal proceeds.
    let term = node.rafts[&0].metrics().borrow().current_term;
    gate.mark_served(0, term);
    assert!(matches!(
        crate::transport::server::handle_txn_for_test(&svc, 0, TxnRpc::BeginGlobal).await,
        TxnResp::Began { .. }
    ), "after the gate opens, BeginGlobal allocates");
}
```
**If `handle_txn` is private/awkward to call from a test:** instead gate the allocation *inside* `TxnService` by adding a `begin_global_gated` wrapper, OR assert via a small `#[cfg(test)] pub(crate)` shim `handle_txn_for_test` in `transport/server.rs` that forwards to `handle_txn`. Pick whichever keeps the check at the BeginGlobal site (Step 3) testable; read the real `handle_txn` visibility first and choose.

- [ ] **Step 3: Run → FAIL** (BeginGlobal not yet gated): `cargo nextest run -p cluster begin_global_is_gated_until_range_0_is_settled`.

- [ ] **Step 4: Gate `BeginGlobal`.** In `crates/cluster/src/transport/server.rs::handle_txn`, the `TxnRpc::BeginGlobal` arm (currently `match svc.engine(0) { Some(e) => match e.begin_global_durable().await { … } }`), add the gate check FIRST:
```rust
        TxnRpc::BeginGlobal => {
            // SP23: gate GTM allocation until range 0's rise sweep has reseeded the counter +
            // settled (retryable — the coordinator re-resolves + retries on NotLeader). Prevents
            // handing out a global xid from a stale (pre-reseed) counter on a freshly-risen leader.
            if !svc.is_serving(0) {
                return TxnResp::NotLeader;
            }
            match svc.engine(0) {
                Some(e) => match e.begin_global_durable().await {
                    Ok(g) => TxnResp::Began { g },
                    Err(executor::ExecError::NotLeader) => TxnResp::NotLeader,
                    Err(e) => TxnResp::Err(format!("{e:?}")),
                },
                None => TxnResp::Err("no range-0 engine".into()),
            }
        }
```
Leave `CommitGlobal`, `GlobalBarrier`, and `Stage`/`Release` UNCHANGED (they are recovery paths; gating them would deadlock the sweep's own `CommitGlobal` abort-races).

- [ ] **Step 5: Run → PASS.** `cargo nextest run -p cluster begin_global_is_gated_until_range_0_is_settled` → PASS.

- [ ] **Step 6: Smoke run + lint.**
`cargo nextest run -p cluster` → PASS.
`cargo nextest run -p crabgresql --test crossrange_2pc_nemesis` → PASS (a normal range-0 rise opens the gate fast enough that BeginGlobal still proceeds — confirms no over-gating of the coordinator).
`cargo clippy -p cluster --all-targets -- -D warnings` clean; `cargo fmt -p cluster`.

- [ ] **Step 7: Commit.**
```bash
git add crates/cluster/src/twopc.rs crates/cluster/src/transport/server.rs
git commit -m "fix(sp23): gate BeginGlobal (GTM allocation) on range 0's recovery gate

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Stateright model — reseed-before-allocate (teeth + positive)

**Files:** Create `crates/cluster/tests/crossrange_2pc_gtm_reuse_model.rs` (UAC-safe name).

- [ ] **Step 1: Write the model.** Mirror `crates/cluster/tests/crossrange_2pc_settle_model.rs` (the SP22 model: `Model`/`State`/`Action`/`properties`, positive + teeth tests, canonical sorted `Vec`s, bounded `max_steps`). Model: a durable global-xid counter (`durable_next`), an in-memory counter (`mem_next`), the set of `allocated` g's, the per-row MVCC versions keyed by their creating `g`, and a per-term serving gate (`served_term`, `current_term`). Actions:
- `Alloc` — `begin_global`: **gated** — admitted only when `!self.reseed_before_alloc || s.served_term == s.current_term`. Allocates `g = mem_next`, `mem_next += 1`, `durable_next = max(durable_next, mem_next)`, `allocated.insert(g)`. A row version is staged under `g`.
- `Rise` — new term: `current_term += 1`; **regress** `mem_next` below `durable_next` (model the apply-lag: `mem_next = some value < durable_next`), gate closes.
- `Reseed` — the rise sweep: `mem_next = max(mem_next, durable_next)` THEN `served_term = current_term` (opens the gate). The only gate-opener.
- `Decide(commit)` — a `g`'s version goes live.

Toggle `reseed_before_alloc: bool`. With it ON, `Alloc` is admitted only after `Reseed` (so `mem_next >= durable_next` ⇒ never re-allocates an `allocated` g). With it OFF, `Alloc` after a `Rise` (mem regressed) re-allocates an already-`allocated` g → two versions under the same `g` → duplicate live.

Properties (`always`):
- **(teeth invariant)** `"no global xid is allocated twice"` — every `Alloc` produces a `g` not already in `allocated` (equivalently `live_count() <= 1` per row). The load-bearing invariant the gate-off model must violate.
- corroborating: `"at most one live version per row"`.

- [ ] **Step 2: Positive test.** `reseed_before_alloc: true` → `checker.assert_properties()` + `unique_state_count() > 1`. No property weakened.

- [ ] **Step 3: Teeth test.** `reseed_before_alloc: false` → `!discoveries().is_empty()` AND `discoveries().contains_key("no global xid is allocated twice")`. The checker MUST find the reused `g` when allocation isn't gated on reseed.

- [ ] **Step 4: Run + lint.**
`cargo nextest run -p cluster --test crossrange_2pc_gtm_reuse_model` → 2 passed.
`cargo clippy -p cluster --test crossrange_2pc_gtm_reuse_model -- -D warnings` clean; `cargo fmt -p cluster`.

- [ ] **Step 5: Commit.**
```bash
git add crates/cluster/tests/crossrange_2pc_gtm_reuse_model.rs
git commit -m "test(sp23): Stateright model — reseed-before-allocate prevents global-xid reuse (teeth + positive)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Multi-process range-0-leader-kill stable-window nemesis

**Files:** Create `crates/crabgresql/tests/range0_leader_kill_drain.rs` (UAC-safe — no `setup/install/update/patch/upgrad`); Modify `CLAUDE.md`.

- [ ] **Step 1: Create the nemesis (copy + adapt the probe's drain harness).** Copy `crates/crabgresql/tests/crossrange_2pc_nemesis.rs` VERBATIM (whole file, helper block included). Edits:
1. Rename the test fn: `range0_participant_leader_kill_conserves_total`.
2. Victim = the RANGE-0 leader (the GTM/coordinator AND `acct_a` participant): `let victim = c.range_leader(0).await;`.
3. **FULL-DRAIN stable window between kills** (single failover at a time, NO overlapping — the design's in-scope case): after the existing kill+recovery, BEFORE the next round, (a) drive a settle-aware zero-sum cross-range barrier `c.exec_until_ok("BEGIN; UPDATE acct_a SET bal = bal - 0 WHERE id = 0; UPDATE acct_b SET bal = bal + 0 WHERE id = 0; COMMIT").await` (confirms both ranges admit writes + the GTM re-resolved + reseeded), then (b) wait for the workload to commit a GENEROUS window (`const STABLE_WINDOW_OPS: u64 = 5;`, polling the committed-op counter, bounded by a ~20s deadline) so any in-flight pre-kill 2PC fully resolves before the next kill. Keep the conservation oracle (`read_total_cross_until_ok`) + non-vacuity asserts UNCHANGED; do NOT weaken the at-most-one-live `debug_assert`. (The probe branch `sp23-range0-probe`'s `crossrange_2pc_range0_probe.rs` is the starting reference for the drain logic.)

- [ ] **Step 2: Run it 3× (non-flaky).** `cargo nextest run -p crabgresql --test range0_leader_kill_drain` three times → PASS all 3 (conservation holds, no 2-live crash-loop, non-vacuous `total_committed > 0`). If it still tears/wedges, STOP and report the mechanism (it would be a genuinely NEW finding beyond the now-fixed g-reuse — do NOT weaken any assert; capture got-vs-want + whether the 2-live assert fired).

- [ ] **Step 3: CLAUDE.md audit line + UAC guard.** Append an `**SP23 (2026-06-15):**` paragraph to the UAC-safe-target-names section noting the two new test binaries `cluster::crossrange_2pc_gtm_reuse_model` + `crabgresql::range0_leader_kill_drain` (both UAC-safe), no new dependency, and that the crabgresql integration-test list now includes `range0_leader_kill_drain`.
Run: `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` → empty.

- [ ] **Step 4: Commit.**
```bash
git add crates/crabgresql/tests/range0_leader_kill_drain.rs CLAUDE.md
git commit -m "test(sp23): multi-process range-0-leader-kill stable-window nemesis — conservation under GTM reseed

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Gauntlet, traceability, finish

- [ ] **Step 1: Full-workspace gauntlet.**
- `cargo fmt --all --check` → clean (else `cargo fmt --all`).
- `cargo clippy --workspace --all-targets -- -D warnings` → clean.
- `cargo nextest run --workspace --profile ci` → all pass (the CI profile serializes the multi-process suites — the authoritative run; the default profile over-parallelizes them on a 2-core machine).
- `cargo test --workspace --doc` → pass.
- `cargo deny check` → ok (no new dependency).

- [ ] **Step 2: Fill the spec Traceability section.** In the SP23 spec, replace the placeholder with criterion → test:

| # | Criterion | Test |
|---|---|---|
| 1 | No global-xid reuse across a range-0 leadership change | `executor::gtm::tests::stale_in_memory_counter_reuses_g_until_reseed` + the range-0 rise-sweep reseed |
| 2 | `begin_global` gated until range-0 settled | `cluster::twopc::tests::begin_global_is_gated_until_range_0_is_settled` |
| 3 | reseed-before-allocate invariant, no-reseed caught | `cluster::tests::crossrange_2pc_gtm_reuse_model` (teeth + positive) |
| 4 | Range-0-leader-kill nemesis conserves, 3× non-flaky | `range0_leader_kill_drain::range0_participant_leader_kill_conserves_total` |
| 5 | Range-0-safe scan; recovery paths ungated; suites unchanged | `executor::in_doubt_scan_ignores_global_decision_entries_on_range_0` + regression |
| 6 | Gauntlet green, no new dep, UAC-safe | this task |

- [ ] **Step 3: Commit traceability.**
```bash
git add docs/superpowers/specs/2026-06-15-crabgresql-sp23-gtm-global-xid-reuse-reseed-design.md
git commit -m "docs(sp23): traceability table — criteria 1-6 mapped to proving tests

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 4: Finish.** Use superpowers:finishing-a-development-branch. Standing preference: option 2 (push a fresh non-force branch + PR). The PR is **stacked on SP22 (#38)** — base it on the SP22 branch (or note the stack); rebase `--onto origin/main` once SP22 squash-merges. PR body ends with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.

---

## Self-Review

**Spec coverage:** range-0-safe scan (component 3) → T1; range-0 rise sweep apply-wait + reseed + register (components 1) → T2; gate `begin_global` (component 2) + accessor → T3; range-0 participant write gating (component 4) → T2 (registration enables the SP22 checks); GTM-not-gated (component 5) → T3 (only BeginGlobal gated). All 6 success criteria mapped (T6 table). Executor reuse teeth → T2 Step 1; gate test → T3; model → T4; nemesis → T5.

**Incremental-green ordering:** T1 (scan bound, no-op on data ranges) → T2 (register range 0 + apply-waited reseed; the g-reuse fix) → T3 (gate BeginGlobal). Each task builds + tests green; the nemesis (T5) validates end-to-end convergence.

**Placeholder scan:** every code step shows complete code; T1/T3 note "read the real constructor/visibility and adapt" (a concrete instruction against the live source, not a placeholder); T4/T5 reference the SP22 model + the probe harness to mirror (established convention). No TBD/TODO.

**Type consistency:** `TxnService::is_serving(range) -> bool` (T3) uses `is_none_or` consistently with SP22's `is_some_and`. `RecoveryGate::{is_serving,mark_served,register_range}` used per SP22. `reseed_gtm`/`reseed_counters` (engine, `.ok()` — both return `Result`) per `lib.rs`. The scan bound `clog_key(GLOBAL_XID_BASE)` used identically in both scan sites. `resolve_in_doubt_on_leadership(raft, range, id, engine, client, gate)` signature matches SP22's T3.

**Known risks folded:** the GTM-not-gated requirement (only BeginGlobal; CommitGlobal/Barrier/Release/Stage's recovery use stays ungated); the register-before-spawn ordering (T2 Step 3); the reseed inside the gate-opening sweep (structural reseed-before-open); the range-0 mixed-clog scan bound (T1); the full-drain nemesis (single-failover-at-a-time, the in-scope case) with the cascading-overlap case explicitly a spec non-goal.
