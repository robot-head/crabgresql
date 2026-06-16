# SP24 / D3c abort-atomicity half-leak — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make cross-range 2PC abort-atomic — once a global txn `g` is `Aborted`, no participant's half of `g` is ever visible, even across a participant-leader failover with a re-stage.

**Architecture:** A probe root-caused the long-deferred "cascading-failover" tear as an **abort-atomicity half-leak**: a globally-aborted cross-range txn (the recovery abort-race wrote `Aborted(g)` during a participant-leader kill; SP18 write-once kept it over the coordinator's later `COMMIT`) leaves ONE participant half durably visible. The abort-*decision* path is correct (a clean in-process abort-race-vs-commit test passes); the leak is in **participant re-stage under leader failover** (held-session lost → a re-stage mints a half not fenced to the original global decision). Task 1 pins the exact escape with a deterministic reproduction; Task 2 fences it; Tasks 3–4 prove it (Stateright model with teeth + a converging multi-process participant-leader-kill bank).

**Tech Stack:** Rust 2024, openraft, `executor::SqlEngine` + `cluster::range::{MultiRangeCluster, RangeRouter}` + `cluster::twopc::TxnService`, `mvcc::clog`, Stateright, cargo-nextest (`--profile ci`).

---

## Background the implementer needs

- **Cross-range visibility is lazy:** a participant write stamps a `Prepared(Li -> g)` clog marker in the same durable (Raft-quorum) batch as the row version (`crates/executor/src/session.rs` ~558-566). A row resolves visible by `clog[local][Li] -> clog[global][g]` via `exec::global_status` (`crates/executor/src/exec.rs:329-345`). Releases write NO per-participant clog (`session.rs:690-699`): `commit_release`/`abort_release` only free locks; visibility is purely the global decision.
- **The global decision is write-once** (`SqlEngine::commit_global_decision`, `crates/executor/src/lib.rs:242-262`): the first terminal `clog[g]` wins; the call returns the EFFECTIVE decision via read-back. The recovery abort-race (`twopc::resolve_in_doubt`, `crates/cluster/src/twopc.rs:393-411`) writes `Aborted(g)` via `CommitGlobal{commit:false}`.
- **Participant stage + idempotency:** `TxnService::stage` (`crates/cluster/src/twopc.rs:460-516`) — on a re-stage with no in-memory held session it consults `SqlEngine::staged_local_for(g)` (`lib.rs:329`) and returns `Staged` (no-op) if a `Prepared(-> g)` marker already exists; otherwise it opens a held session and runs the write. The router's `stage_on` (`crates/cluster/src/range/router.rs:516-536`) stages locally if it leads the range, else over RPC (`stage_remote`). Escalation allocates a fresh `g` per escalation (`router.rs` ~388/415).
- **In-process primitives for tests:** `MultiRangeCluster::new(n, RangeMap::with_boundaries(..))`, `c.wait_for_leader(range)`, `c.leader_engine(range)` (→ `SqlEngine` with `begin_global_durable`, `commit_global_decision`, `in_doubt_globals`), `c.pause(node)`/`c.resume(node)`, and `RangeRouter::connect(&c)`. See `crates/cluster/tests/crossrange_2pc.rs` (esp. `global_decision_is_write_once_and_returns_effective` and `cross_range_update_rolls_back_atomically`).
- **No `sleep`** in tests/harness (CLAUDE.md): wait on openraft events / progress signals; multi-process harness polls keep a small interval + deadline.
- **UAC target-name rule** (CLAUDE.md): no `[[test]]`/`[[bin]]` target name or `crates/*/tests/*.rs` filename may contain `setup`/`install`/`update`/`patch`/`upgrad`.

---

## File Structure

- **Modify** `crates/cluster/tests/crossrange_2pc.rs` — Task 1 adds the deterministic failover-re-stage reproduction (`aborted_global_leaves_no_participant_half_visible_under_failover`); Task 2 turns it green. In-process, drives GTM + router primitives directly.
- **Modify** the fix site (named by Task 1) — one (or two) of: `crates/cluster/src/twopc.rs` (`stage` / `resolve_in_doubt`), `crates/cluster/src/range/router.rs` (`stage_on` / escalation), `crates/executor/src/session.rs` (participant write / `join_global`), `crates/executor/src/lib.rs` (`staged_local_for`). Task 2.
- **Create** `crates/cluster/tests/crossrange_2pc_abort_atomicity_model.rs` — Task 3 Stateright model with teeth (UAC-safe name).
- **Create** `crates/crabgresql/tests/participant_kill_bank.rs` — Task 4 multi-process participant-leader-kill cross-range bank nemesis (UAC-safe name: no forbidden substring).
- **Modify** `CLAUDE.md` — Task 5 audit paragraph (SP24 binaries).

---

## Task 1: Deterministic reproduction — pin the exact escape (RED)

**Goal:** A single in-process test that reliably reproduces the abort-atomicity half-leak by injecting a participant-leader failover + re-stage, and whose failure message names the leaked half. This is the discovery task: its RED output reveals the exact mechanism (which clog state the leaked half carries), which the controller uses to finalize Task 2's fix.

**Files:**
- Modify/Test: `crates/cluster/tests/crossrange_2pc.rs`

- [ ] **Step 1: Write the failing test.** Append to `crates/cluster/tests/crossrange_2pc.rs`. Start from the proven-correct no-failover control (which PASSES — included as an assertion that the abort path itself is sound) and then add the failover+re-stage that the probe identified. Drive it with the in-process cluster's pause/re-elect; if pausing the participant leader does not lose the held session and force a re-stage, escalate within this same test to a direct re-stage: stage the participant under `g`, then explicitly re-run the participant write on a fresh session/new leader, then abort `g` via the abort-race, then assert invisibility.

```rust
/// SP24: a cross-range txn whose global `g` is ABORTED by the recovery abort-race
/// during a PARTICIPANT-LEADER failover must leave NO participant half visible —
/// even though the participant re-stages on its new leader. A visible `b` half is
/// the abort-atomicity leak (money created/destroyed).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aborted_global_leaves_no_participant_half_visible_under_failover() {
    use mvcc::clog::XidStatus;
    // 5 nodes so range 1 keeps a 3-node quorum when its leader is paused.
    let c = MultiRangeCluster::new(5, RangeMap::with_boundaries(vec![2])).await;
    for r in c.range_map().range_ids() {
        c.wait_for_leader(r).await;
    }
    let mut r = RangeRouter::connect(&c).await;
    r.simple("CREATE TABLE a (id int4)").await.expect("a"); // id 1 -> range 0
    r.simple("CREATE TABLE b (id int4)").await.expect("b"); // id 2 -> range 1
    r.simple("INSERT INTO a VALUES (10)").await.expect("seed a");
    r.simple("INSERT INTO b VALUES (20)").await.expect("seed b");

    // Begin a cross-range txn; stage a (range 0) and b (range 1) under g.
    r.simple("BEGIN").await.expect("begin");
    r.simple("UPDATE a SET id = 11 WHERE id = 10").await.expect("pin range 0");
    r.simple("UPDATE b SET id = 21 WHERE id = 20").await.expect("escalate -> g");
    let g = c.leader_engine(1).await.in_doubt_globals().await.expect("scan")[0];

    // Fail over range 1's leader (drops the in-memory held session for (g, range 1)),
    // forcing a re-stage on the new leader when the coordinator retries / re-resolves.
    let victim = c.range_leader(1).await;
    c.pause(victim);
    c.wait_for_leader_excluding(1, victim).await; // new range-1 leader rises

    // The recovery abort-race wins the decision for g (Aborted), as on a real kill.
    assert_eq!(
        c.leader_engine(0).await.commit_global_decision(g, XidStatus::Aborted).await.expect("abort-race"),
        XidStatus::Aborted
    );
    // Coordinator's COMMIT now sees write-once Aborted; txn is globally ABORTED.
    let _ = r.simple("COMMIT").await;
    c.resume(victim);

    // Neither half may be visible. A fresh router resolving via range 0's global clog
    // must read the PRE-txn values on BOTH ranges. `21` on b is the leak.
    let mut fresh = RangeRouter::connect(&c).await;
    assert_eq!(scan_i32(&mut fresh, "SELECT id FROM a").await, vec![10], "a half leaked");
    assert_eq!(scan_i32(&mut fresh, "SELECT id FROM b").await, vec![20], "b half leaked (abort-atomicity)");
}
```

- [ ] **Step 2: Run it; confirm it FAILS (reproduces the leak).**

Run: `cargo nextest run -p cluster --test crossrange_2pc aborted_global_leaves_no_participant_half_visible_under_failover --profile ci`
Expected: FAIL on the `b` assertion (`left: [21], right: [20]`), i.e. the aborted half is visible. If it does NOT fail (the in-process pause/re-elect does not lose the held session / force a re-stage), strengthen the injection within this test until it does — options, in order: (a) pause the victim BEFORE `COMMIT` and issue a second `UPDATE b ... WHERE id = 21` to drive a re-stage on the new leader; (b) add a minimal `#[cfg(test)] pub(crate)` seam on `TxnService` to drop the held session for `(g, range)` then re-stage; (c) if in-process genuinely cannot reproduce it, move this reproduction to a focused multi-process test under `crates/crabgresql/tests/` mirroring the kill-every-round nemesis but asserting on a single tagged txn. **Do not proceed to Task 2 until this test reliably reproduces the leak (run it 5× — it must fail every time).**

- [ ] **Step 3: REPORT the exact mechanism (no fix yet).** With the test reliably RED, determine and write down (as a comment block at the top of the test) the EXACT clog/version state of the leaked `b` row on the new leader: is the visible version's xid `Prepared(-> g')` for a DIFFERENT `g'` that committed, `Committed` locally, or `Prepared(-> g)` mis-resolving against `clog[g]=Aborted`? Cite the precise file:line the leaked version was written at (instrument with a temporary `eprintln!` if needed, then remove it). **This report is the deliverable of Task 1** — the controller uses it to finalize Task 2's fix.

- [ ] **Step 4: Commit.**

```bash
git add crates/cluster/tests/crossrange_2pc.rs
git commit -m "test(sp24): reproduce the abort-atomicity half-leak under participant-leader failover (RED)"
```

**STATUS to report:** `DONE` with the exact-mechanism report from Step 3, or `BLOCKED` if no injection reproduces it (include what was tried).

---

## Task 2: Fence the re-stage to the global decision (GREEN)

**Goal:** Enforce the invariant **`aborted(g) ⇒ 0 visible halves`** by closing the exact escape Task 1 named. The controller refines this task's concrete diff from Task 1's report before dispatch; the acceptance criterion is fixed (Task 1's test green + no regressions).

**Files:**
- Modify: the site named by Task 1 — most likely `crates/cluster/src/twopc.rs` (`stage` idempotency / `resolve_in_doubt`) and/or `crates/cluster/src/range/router.rs` (escalation must not mint a fresh `g'` for an already-staged participant) and/or `crates/executor/src/session.rs` (participant re-stage must reuse `g` and the same version identity).
- Test: `crates/cluster/tests/crossrange_2pc.rs` (Task 1's test).

**Invariant-driven fix (the controller supplies the exact diff from Task 1's report; the strongest a-priori candidate):** a participant re-stage for a cross-range txn MUST be strictly idempotent on `(g, range, rowid)` — it reuses the SAME `g` and supersedes (not duplicates) any prior staged version for that row, so a single global decision governs exactly one live version per row; and the router must NEVER re-escalate an already-staged participant under a fresh `g'`. Then a global abort of `g` makes the (single) staged version resolve `Aborted` → invisible, with no second version under a different decision to leak.

- [ ] **Step 1: Apply the minimal fix** to the file named by Task 1 (controller-provided concrete diff). Keep it minimal and local; do not touch the commit path (proven correct).

- [ ] **Step 2: Run Task 1's test; confirm it PASSES (5×, non-flaky).**

Run: `cargo nextest run -p cluster --test crossrange_2pc aborted_global_leaves_no_participant_half_visible_under_failover --profile ci` (repeat 5×)
Expected: PASS every time.

- [ ] **Step 3: Run the cross-range regression; confirm no breakage.**

Run: `cargo nextest run -p cluster --test crossrange_2pc --profile ci`
Expected: all cross-range 2PC tests PASS (commit, rollback, three-range, write-once, the new failover test).

- [ ] **Step 4: Commit.**

```bash
git add -A
git commit -m "fix(sp24): fence participant re-stage to the global decision (abort atomicity)"
```

---

## Task 3: Stateright model with teeth — `aborted(g) ⇒ 0 visible halves`

**Goal:** An exhaustive model of the abort path (participant stage, leader-failover re-stage, abort-race, concurrent commit attempt) whose safety invariant is `aborted(g) ⇒ 0 visible halves`, with a MANDATORY teeth test proving the checker catches the un-fenced variant. Mirror `crates/cluster/tests/crossrange_2pc_gtm_reuse_model.rs` and `crossrange_2pc_settle_model.rs`.

**Files:**
- Create: `crates/cluster/tests/crossrange_2pc_abort_atomicity_model.rs`

- [ ] **Step 1: Write the model + positive + teeth tests.** Pure `stateright::Model` (no openraft/SQL/IO), canonical sorted `Vec`s, bounded `max_steps`. State: per participant range a set of staged versions `{g, decision_seen}`; a global `decision: Option<{g, Committed|Aborted}>` (write-once); a `fence: bool` config toggle (true = real system: a re-stage reuses `g` and supersedes; false = broken: a re-stage may mint a fresh `g'` that commits independently). Actions: `Stage(range,g)`, `Failover(range)` (drops held state, enables a re-stage), `ReStage(range)`, `AbortRace(g)`, `Commit(g)`. Property `always "abort_atomicity"`: a STATE predicate — for every range, the count of versions that resolve VISIBLE for a txn whose global decision is `Aborted` is 0. Positive test: `fence=true` → `assert_properties()` holds, `unique_state_count() > 1`. Teeth test: `fence=false` → `!checker.discoveries().is_empty()` AND a discovery names `"abort_atomicity"`.

- [ ] **Step 2: Run it.**

Run: `cargo nextest run -p cluster --test crossrange_2pc_abort_atomicity_model --profile ci`
Expected: both tests PASS (positive holds; teeth catches the broken variant).

- [ ] **Step 3: Verify UAC + commit.**

Run: `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` → expect EMPTY.
```bash
git add crates/cluster/tests/crossrange_2pc_abort_atomicity_model.rs
git commit -m "test(sp24): Stateright abort-atomicity model with teeth"
```

---

## Task 4: Converging multi-process participant-leader-kill bank

**Goal:** A multi-process nemesis that kills a PARTICIPANT-range leader while cross-range transfers are in flight and asserts the bank total is conserved — the empirical end-to-end proof the fix closes the leak. Must pass repeatedly, non-flaky, under `--profile ci`.

**Files:**
- Create: `crates/crabgresql/tests/participant_kill_bank.rs` (UAC-safe name)

- [ ] **Step 1: Write the test.** Mirror `crates/crabgresql/tests/crossrange_2pc_nemesis.rs` (the committed, green sibling): 5 nodes, boundary `[2]` (`acct_a`→range 0/participant-or-coordinator note below, `acct_b`→range 1), `Cluster::spawn_multirange`. Seed via `exec_until_ok`. Continuous worker(s) doing cross-range transfers (bounded-retry, indeterminate-on-COMMIT-error). Nemesis: each round kill+respawn (even) / partition+heal (odd) the **participant** range leader (`c.range_leader(1)`), paced on a committed-op progress signal (NO sleep), awaiting recovered quorum before the next fault. After the run: heal, await leaders, post-heal recovery-required transfers touching every pair (`exec_until_ok`), then the bounded-retry authoritative conservation read (`read_total_cross_until_ok`). Assert `total == seeded_total` AND non-vacuity (`total_committed > 0`).

- [ ] **Step 2: Run it 5×; confirm conservation holds.**

Run: `for i in 1 2 3 4 5; do cargo nextest run -p crabgresql --test participant_kill_bank --profile ci; done`
Expected: PASS all 5 (no `got X, want Y` tear, no 30s wedge). If it flakes on liveness (non-vacuity / wedge) but never tears, tune pacing per `nemesis-needs-stable-windows-under-read-latency`; a TEAR means Task 2 is incomplete — return to Task 1's report.

- [ ] **Step 3: Verify UAC + commit.**

Run: `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` → expect EMPTY.
```bash
git add crates/crabgresql/tests/participant_kill_bank.rs
git commit -m "test(sp24): multi-process participant-leader-kill bank conserves under abort atomicity"
```

---

## Task 5: Gauntlet + traceability + finish

**Goal:** Whole-workspace green, docs updated, clean tree, PR.

**Files:**
- Modify: `CLAUDE.md` (SP24 audit paragraph)

- [ ] **Step 1: Confirm no probe scaffolding remains.**

Run: `git status --short` (clean) and `git grep -n "PROBE2PC\|overlap_probe" -- crates/ || echo CLEAN` → expect CLEAN. Remove any stray scratch (`crates/crabgresql/tests/overlap_probe.rs` must not exist).

- [ ] **Step 2: fmt + clippy + full nextest + doctests.**

Run:
```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace --profile ci
cargo test --workspace --doc
```
Expected: fmt clean, clippy clean, all nextest PASS, doctests PASS.

- [ ] **Step 3: UAC guard + add the CLAUDE.md audit paragraph.** Append an `**SP24 (2026-06-15):**` paragraph to CLAUDE.md listing the two new test binaries (`cluster::crossrange_2pc_abort_atomicity_model`, `crabgresql::participant_kill_bank`), confirming both are UAC-safe and the guard `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty.

- [ ] **Step 4: Commit + finish.**

```bash
git add -A
git commit -m "chore(sp24): gauntlet green + CLAUDE.md SP24 audit (abort-atomicity half-leak fix)"
```
Then use `superpowers:finishing-a-development-branch` to push + open the PR.

---

## Self-review notes (for the controller)

- **Spec coverage:** invariant (T2/T3), pin-the-line (T1), model-with-teeth (T3), converging nemesis (T4), gauntlet + revert scaffolding (T5) — all spec success criteria mapped.
- **Known soft spot:** T1's exact reproduction injection and T2's exact diff are determined by T1's RED report (a pin-then-fix bug). The controller MUST review T1's reported mechanism and supply T2's concrete diff before dispatching T2 — do not let T2 proceed on a guess. If T1 reports the leak is a fresh-`g'` re-escalation, the fix is in `router.rs` escalation; if a duplicate `Prepared(-> g)` version, in `twopc::stage`/`session.rs` supersession; if a mis-resolution, in `exec::global_status`/`find_visible_one`.
- **Never weaken** the conservation oracle or the model's `abort_atomicity` property to make a run pass — a counterexample in the real variant is a genuine finding.
