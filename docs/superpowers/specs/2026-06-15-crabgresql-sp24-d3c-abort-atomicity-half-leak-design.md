# SP24 / D3c-abort-atomicity: cross-range 2PC abort-atomicity half-leak

**Slice:** SP24 (the long-deferred "cascading-failover" cross-range 2PC tear — root-caused by a
runtime probe to an **abort-atomicity half-leak**, not a commit-durability or range-0 problem).

**Status:** design (probe-first complete; root cause established with hard evidence).

## Problem

Under a participant-range leader kill while cross-range transfers are in flight, the bank total is
not conserved (~21% of hard-nemesis runs): a globally **ABORTED** cross-range transaction leaves
**one participant's half durably visible**, so money is created or destroyed by exactly that
transfer's amount. This is the residual that SP21, SP22, and SP23 each circled and deferred.

## Root cause (established by a runtime per-`g` probe, 2026-06-15)

The bug is **ABORT atomicity**, the inverse of what every prior slice hardened (all of SP16/18/22/23
hardened the COMMIT path — quorum-before-ack, write-once decision, settle-before-serve,
reseed-before-allocate). That is exactly why static analysis (a 5-seam adversarial workflow refuted
14/14 hypotheses) and code reads keep "proving" it cannot happen: they all reason about a LOST
COMMITTED half, but the bug is an ABORTED half that stays VISIBLE.

Evidence chain:

1. **It is not range-0-specific.** Killing a PURE participant-range leader (never the coordinator)
   still tears ~21%. The "reserve range 0 / meta-only" idea was empirically INVALIDATED (it only cut
   range-0-kill failures 43% → 14/21%). Leader co-location is real but not the cause.
2. **The error equals an aborted transfer's amount.** Per-`g` tracing on a tear: the single globally
   **Aborted** `g` (its decision line read `[(Aborted,Aborted),(Committed,Aborted)]` — the recovery
   abort-race wrote `Aborted(g)` first, then the coordinator's COMMIT requested `Committed` but SP18
   write-once correctly kept `Aborted`) had amount 18, and the conservation surplus was exactly +18
   on the surviving range's row. Confirmed across 3 tear instances; bidirectional ± = whichever half
   (debit/credit) of the aborted `g` leaks.
3. **The abort-DECISION path is correct.** A deterministic in-process test (stage both halves under
   `g`, write `Aborted(g)` out-of-band as the abort-race, then COMMIT) leaves BOTH halves invisible —
   it PASSES. So the leak is not in the decision/resolution of a single staged `g`.
4. **It is not a local-autocommit escape.** Zero autocommit writes of bank tables occur on a tear, so
   the leaked half is not a half that committed locally bypassing `g`.

**Therefore the leak is in PARTICIPANT RE-STAGE under leader failover:** when a participant's leader
is killed mid-transaction, its in-memory held-session is lost; a re-stage on the new leader (the
`staged_local_for` idempotency path + a fresh-`g'` escalation retry) can create a participant version
that is NOT fenced to the original global decision — so when the original `g` aborts, that re-staged
half survives as visible. Visibility is purely `clog[Li] → clog[g]`, so the surviving half must carry
either a second `Prepared(Lb' → g')` whose `g'` committed, or a marker that no longer resolves against
the aborted `g`. (Pinning which of these is Task 1 — it needs the failover re-stage reproduced
deterministically, which the no-failover in-process path in evidence #3 does not exercise.)

## Invariant the fix must enforce

**Abort atomicity:** once `clog[g] = Aborted`, NO participant's half of `g` is or ever becomes
visible — across a participant-leader failover, a re-stage, and any concurrent coordinator commit
attempt. Equivalently: every version a participant writes for a cross-range txn resolves **only** via
that txn's single global decision; a failover-induced re-stage must not mint a half that outlives an
abort.

## Design

1. **Pin the exact escape (Task 1, red test).** A deterministic reproduction that injects the
   failover re-stage: stage a participant under `g`, lose its held session (simulate leader change),
   re-stage, abort `g` via the abort-race, and assert the participant half is invisible. Built in
   `crates/cluster/tests/crossrange_2pc.rs` (in-process, drives the GTM + router primitives directly;
   adds a minimal test seam if the re-stage cannot otherwise be driven). This names the precise line
   the half leaks through before any fix code.
2. **Close it.** Fence the participant re-stage to the global decision: a re-stage for `(g, range)`
   must reuse the SAME `g` and the SAME logical version identity (never a fresh `g'` that can commit
   independently), and the resolver/visibility for a cross-range participant version must defer to
   `clog[g]` with no escape. The exact change is determined by Task 1's finding; candidates include
   making the re-stage strictly idempotent on `(g, range, rowid)` and removing any fresh-`g'`
   re-escalation of an already-staged participant.
3. **Stateright model with teeth.** Model the abort path: a participant `g`, an abort-race, a
   leader-failover re-stage, and a concurrent commit attempt; invariant = `aborted(g) ⇒ 0 visible
   halves`. A boolean toggle for the broken (un-fenced re-stage) variant; a MANDATORY teeth test that
   asserts the checker CATCHES the leak and names the property; `unique_state_count() > 1`.
4. **Converging multi-process nemesis.** A participant-leader-kill cross-range bank (derived from the
   probe's `overlap_probe`) conserves the total — must pass repeatedly, non-flaky, under `--profile
   ci`. No `sleep`; pace on committed-op progress; bounded-retry the authoritative read.
5. **Gauntlet + finish.** Full workspace nextest (`--profile ci`) + doctests + clippy + fmt + the UAC
   target-name guard; CLAUDE.md audit paragraph; revert ALL probe scaffolding; traceability table; PR.

## Non-goals

- **Commit-path durability** (covered by SP16/18/22/23 and proven correct here).
- **Reserving range 0 as meta-only / leadership anti-affinity** — empirically does not fix this bug.
- **General multi-failover (two overlapping participant kills)** beyond what the single-participant
  failover re-stage requires; if a residual remains under cascading kills it is a separate slice.

## Risks (and mitigations)

- **Re-stage fencing could deadlock or wedge a legitimate retry.** Mitigation: the fix keeps the
  participant retry idempotent (reuse `g`), so a retry never blocks; the nemesis (Task 4) and the
  existing `crossrange_2pc_{nemesis,replicated}` regression catch a wedge.
- **Task 1 may need a test seam to drive the failover re-stage.** Mitigation: a minimal `#[cfg(test)]`
  hook (mirroring the existing pause/`staged_local_for` seams), removed-or-gated so it cannot affect
  production behavior.
- **The exact fix is not yet named** (Task 1 names it). Mitigation: the invariant (abort atomicity) is
  precise and testable; the model + nemesis are written against the invariant, not a guessed line, so
  they validate whatever Task 1 reveals.

## Success criteria

1. The Task 1 reproduction goes red on `main` and green after the fix.
2. `aborted(g) ⇒ 0 visible halves` holds: no cross-range bank run creates/destroys money under a
   participant-leader-kill nemesis (Task 4 passes 3×+ non-flaky under `--profile ci`).
3. A Stateright model with teeth catches the un-fenced re-stage variant.
4. Full gauntlet green; all probe scaffolding reverted; UAC guard clean.

## As-shipped (SP24, 2026-06-15) — CORRECTED approach + scoped outcome

The probe-first investigation refined this design twice:

1. **Root cause corrected (re-stage fence → LOST LOCKS).** The first fix attempt (a session-level
   `effective_global_xid` fence that adopts an in-doubt `g_old`) turned the in-process reproduction
   green but a faithful multi-process *pure-participant* nemesis still tore. A reassessment drill pinned
   the real trigger with hard evidence: **row locks live only in the in-memory `RowLockManager`; a
   participant-leader kill wipes the lock table while the in-doubt `Prepared(Li→g)` version stays
   durable**, so the row has no live lock holder and concurrent writers mint competing versions each
   under its own `g` (one torn row had seven independently-committed versions). The fence only collapses
   one `g` per stage and cannot serialize N writers.

2. **Fix shipped = re-acquire in-doubt row locks on leadership rise** (textbook 2PC participant
   recovery): `executor::SqlEngine::reacquire_in_doubt_locks` + `RowLockManager::reacquire_exclusive`,
   wired into `server_node::resolve_in_doubt_on_leadership` BEFORE `mark_served` (settle-before-serve for
   locks), held until each `g` resolves. The `effective_global_xid` fence is kept as defense-in-depth.
   Proven by the in-process reproduction + an executor unit test with teeth (a concurrent writer BLOCKS
   on the re-acquired lock) + the Stateright model with teeth (`crossrange_2pc_abort_atomicity_model`).
   Zero regressions; full gauntlet green (419 passed, 1 skipped).

3. **Scope correction (success criterion #2 partially DEFERRED).** The multi-process
   `participant_kill_bank` nemesis is committed **`#[ignore]`'d**: it still tears, but the evidence shows
   a SEPARATE, pre-existing failure — a *committed* cross-range txn's killed-participant half is **LOST**
   (single live version, no competing version), distinct from the abort-atomicity half-leak SP24 fixes.
   This is the **SP22/SP23-deferred committed-half-survival / non-atomic-2PC-commit** gap, reproducible
   independent of all SP24 work, and explicitly a non-goal of this slice. Closing it (durably reconstruct
   + re-apply a committed `g`'s killed-participant half on the risen leader) is a dedicated future slice;
   `participant_kill_bank` is the ready acceptance test (un-`#[ignore]` it when that slice lands). SP24
   therefore ships criteria #1, #3, #4 fully and #2 scoped to abort-atomicity (the converging nemesis
   for the residual is the next slice's deliverable).
