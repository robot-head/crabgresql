# SP21 / D3c-restage — Coordinator re-attempts under a fresh global xid on a participant-leader move

**Date:** 2026-06-14
**Slice:** SP21 (D3c-restage)
**Status:** SUPERSEDED — see the CORRECTION below. The fresh-`g'` re-attempt design in this
document was abandoned during implementation; what actually shipped is different.

---

## CORRECTION (as-shipped) — read this first

**The re-attempt design below was based on a wrong hypothesis and was NOT shipped.** During
implementation, the multi-process participant-leader-kill nemesis — driven by SP21's own new
`find_visible_one`/`scan_live` at-most-one-live `debug_assert!` — proved that:

1. **The re-attempt never fires on the motivating fault.** A killed participant leader
   surfaces as `ExecError::Unavailable` (a transport error), not `ExecError::NotLeader`, so
   `reattempt_under_fresh_g` was dead code on that path (confirmed: it fired 0× across
   instrumented runs).
2. **The real defects were PRE-EXISTING (SP18-era), not a missing re-attempt.** Two distinct
   safety bugs, both reproduced on the SP20 base with all SP21 code removed:
   - **Non-idempotent participant `Stage`.** `TwoPcClient::call` retries `Stage` on a
     transport failure; `TxnService::stage` allocated a fresh local xid per call, so a retry
     across a leader failover wrote a SECOND `Prepared(-> g)` version → two live versions on
     commit (torn/doubled balances).
   - **Pre-decision lock release.** On leadership loss, `release_all_for_range` freed a held
     row lock with its `g` still in-doubt (violating the `eval_plan_qual` invariant), letting
     a concurrent writer create a second, non-superseding version of the row.

### What shipped (this slice)

- **T1 — MVCC hardening / the detector:** `find_visible_one` + `scan_live` select the
  greatest-xmin live version explicitly and `debug_assert!` at-most-one-live. This is what
  caught the entire cascade.
- **Idempotent participant `Stage` per `(g, range)`:** `SqlEngine::staged_local_for` + a
  held-session-aware check in `TxnService::stage` (a cross-leader retry that finds an existing
  durable `Prepared(-> g)` is a no-op). Verified by a deterministic unit test (red→green) + a
  Stateright model with teeth (`crates/cluster/tests/crossrange_2pc_model.rs`).
- **Leadership-loss resolve-then-release:** `TxnService::resolve_and_release_for_range` drives
  each held `(g, range)` through its write-once decision before releasing, so a lock is never
  freed pre-decision.
- The fresh-`g'` re-attempt (the original design, T2/T3) was implemented, reviewed, then
  **reverted** as the wrong root cause.

### Deferred to a dedicated future slice

**Full participant-leader-kill multi-process recovery robustness.** A KILLED leader (process
death, no graceful handler) leaves a *session-less* in-doubt marker on the new leader; a
writer that touches that row before the marker settles can still create a duplicate.
Incremental patches (a periodic session-less in-doubt sweep + an executor in-doubt writer
guard) eliminate the crash/hang but do not converge to a conserved total. The correct fix is
structural — **settle-before-serve**: gate a range's writes on its leadership-rise in-doubt
sweep (with an apply-wait) so no writer reads an unsettled inherited marker. That redesign
gets its own spec + adversarial plan review. The participant-leader-kill nemesis is not
committed until that lands.

---

## Summary

Closes the last open cross-range 2PC fault window: a **participant range's leader moving
during the STAGE window**. Today that surfaces a retryable abort to the client (SP18/SP19
scope was "post-prepare recovery only"). SP21 makes the **coordinator transparently
re-attempt the escalated transaction under a fresh global xid `g'`** instead — the client
sees one outcome (a commit, or, only after a bounded retry budget is exhausted, the same
retryable error it gets today).

The design deliberately re-attempts under a **fresh `g'`** rather than surgically re-staging
the moved participant under the same `g`, because re-staging under the same `g` is racy
against the recovery sweep (below) and would double-apply. A fresh-`g'` re-attempt sidesteps
both hazards with no new idempotency machinery and no change to the SP18 write-once /
recovery invariants.

## Where this sits: the roadmap

The D3c cross-range-transaction arc is complete and consolidated (SP16 core → SP17 network →
SP18 fault-hardened → SP19 replicated layout → SP20 recovery-scan GC). SP18/SP19 explicitly
deferred the **mid-transaction re-stage** (a leader move *during* staging) as a Non-goal,
keeping the retryable-abort-then-client-retry for that window. SP21 finishes the 2PC
fault-tolerance story by handling that window coordinator-side, before the project pivots to
breadth (cross-range reads) or the next depth tier (D4 range splits).

## The two hazards a naive re-stage hits (why fresh-`g'`)

Both are confirmed in the current tree (the anchor map):

1. **Double-apply.** A participant's local xid `Li` is allocated fresh-per-session
   (`procarray::begin_write`), and `reseed_from_applied` guarantees a new leader hands out
   `Li_new > Li_old`. Tuple versions are keyed by xmin only (`version_key_xid`, no `g`
   component). So a same-`g` re-stage on the new leader writes a **second** physical version of
   the row (`Li_new`), and both `Prepared(Li_old→g)` and `Prepared(Li_new→g)` resolve to the
   same `g`. When `g` commits, both versions go visible simultaneously — two live versions of
   one row. `find_visible_one` (`crates/executor/src/exec.rs:350-371`) masks it today by
   accidental last-wins sort order, but it is a latent wrong-value / permanent-shadow-version
   bug.

2. **Recovery-sweep race (the decisive one).** The new leader's leadership-rise sweep
   (`resolve_in_doubt_on_leadership`, `crates/cluster/src/server_node.rs:600-650`) fires **on
   the rising edge** — before the coordinator has even detected the move — and abort-races the
   already-durable `Prepared(Li_old→g)` marker's `g` via the write-once path
   (`CommitGlobal{g, commit:false}`). A same-`g` re-stage must *fight* a sweep that already
   fired and tends to win (it starts earlier), aborting `g` out from under the re-stage.
   Fencing it would require the sweep to know `g` is "coordinator-active" — reversing SP18's
   deliberate no-durable-coordinator-record decision.

**A fresh-`g'` re-attempt dissolves both:** the abandoned `g` is *correctly* abort-raced by the
sweep (its coordinator gave up on it), its staged versions become invisible (presumed-abort),
and the re-attempt under a brand-new `g'` is a clean attempt with no double-apply and nothing
to fence.

## Decisions (locked during brainstorming)

1. **Fresh-`g'` whole-txn re-attempt, not surgical same-`g` re-stage.** On a participant
   `Stage` → `NotLeader`, abort `g` and replay the txn's cross-range write-set under a fresh
   `g'`. No carried-`Li`, no sweep fencing, no MVCC/clog-format change.
2. **Explicitly abort the abandoned `g`** (do not merely abandon it to the sweep): `Release{commit:false}`
   to every already-staged participant (frees locks promptly) + `CommitGlobal{g, commit:false}`
   so `g` is *durably Aborted* — deterministic, prompt, and leaves nothing for the sweep.
3. **Bounded retry loop.** Re-attempt on further `NotLeader` up to a budget (N attempts within
   `TXN_TIMEOUT`); on exhaustion, surface the retryable abort to the client (graceful
   degradation to today's SP18 behavior). No sleep — driven by the `NotLeader` responses +
   leader re-resolution.
4. **Harden `find_visible_one`** (defense-in-depth, independent of the re-attempt): explicitly
   return the **highest-xid** visible version and `debug_assert!` at-most-one live version per
   row per snapshot — converting today's accidental last-wins into a documented contract, so a
   stale abandoned-`g` shadow version can never be read.

## Components

### 1. Per-txn cross-range write-set buffer (`crates/cluster/src/range/router.rs`)

In an **escalatable** router (`can_escalate()`), record the ordered table-bearing statements
of a `BEGIN..COMMIT` block — each `(range_pin, Statement)` as it executes — into a per-txn
buffer. This is the replay log: when a mid-stage failure forces a fresh `g'`, the buffer holds
exactly the writes that must be redone. Cleared on `finish_txn` (COMMIT/ROLLBACK) and on a
non-escalatable single-range close. (Most txns never escalate; the buffer is a small
`Vec<(Pin-target, Statement)>` dropped at txn end.)

### 2. Re-attempt on `Stage` → `NotLeader` (the core mechanism)

In the incremental-staging path (the `Pin::Global` arm and the escalation arms,
`router.rs:340-420`), when staging a table-bearing statement returns `ExecError::NotLeader`
(that participant range's leader moved):
1. **Abort the current `g`:** `coord.release_remote(g, r, false)` / local `abort_release` for
   every range already staged under `g`, then `coord.commit_global(g, /*commit=*/false)` to
   durably Abort `g`. (Both reuse existing seams — `finish_txn`'s release loop +
   `commit_global`.)
2. **Begin a fresh `g'`:** `coord.begin_global()`.
3. **Replay the buffer under `g'`:** re-execute every buffered table-bearing statement through
   the normal routing, re-resolving each participant's *current* leader (the moved one now
   resolves to the new leader). The local range re-joins `g'`; each remote participant is
   re-staged on its current leader.
4. **Silent replay:** the statements that already returned a result to the client replay
   without re-emitting output; only the *currently-executing* statement returns its result to
   the client (as normal). The client sees one coherent statement stream + one final
   COMMIT/ROLLBACK.
5. **Bounded retry:** if a replay itself hits `NotLeader`, repeat from step 1 with another fresh
   `g''`, up to the budget; on exhaustion, abort and surface the retryable error.

### 3. `find_visible_one` hardening (`crates/executor/src/exec.rs:350-371`)

Make the visible-version selection explicitly pick the **highest xmin** among visible versions
(documented, not incidental) and add a `debug_assert!` that at most one *live* version is
visible per row per snapshot. Pure defense-in-depth: after the fresh-`g'` design the abandoned
versions are invisible (Aborted `g`), so this never changes observable behavior — but it
permanently fences any stale-shadow-version read against a future iteration-order refactor.

### 4. The leader-re-resolution the re-attempt reuses

`TwoPcClient` already re-resolves a range's current leader on `NotLeader` (SP17), and the SP17
per-range-leaders control query exists. The re-attempt loop reuses these to find the moved
participant's new leader — no new resolution machinery.

## Why it's safe (no invariant breakage)

- **No double-apply:** each attempt uses a fresh `g'`; the abandoned `g` is durably **Aborted**,
  so all its staged versions resolve invisible (presumed-abort). The old versions are dead, not
  committed.
- **No sweep fencing:** the recovery sweep (`resolve_in_doubt_on_leadership`) and the silence
  sweeper (`participant_silence_sweeper`) abort-racing an *abandoned* `g` is *correct* — the
  coordinator already aborted it. The active `g'` is driven by the live coordinator; if its
  leader also moves, it just becomes the next abandoned `g`.
- **Write-once untouched:** each `g` gets exactly one decision. The coordinator writes the
  abandoned `g`'s decision (Aborted) and the fresh `g'`'s decision (Committed/Aborted). SP18's
  first-writer-wins clog apply is unchanged.
- **Client semantics:** at-most-once. The client's single txn yields one outcome; per-statement
  results are emitted once (the replay is internal).

## Success criteria

| # | Criterion | Verified by |
|---|---|---|
| 1 | A participant-leader move during STAGE no longer surfaces a client abort within the retry budget: the txn commits transparently. | in-process pause-during-stage test |
| 2 | After a mid-stage re-attempt, there is **exactly one live version** of each written row, with the correct (re-attempt) value — no double-apply. | in-process test asserting one visible version + value |
| 3 | The abandoned `g` is durably **Aborted** and its staged participants' locks are freed promptly (a concurrent writer proceeds). | in-process test |
| 4 | Bounded retry: under continuous mid-stage churn the coordinator gives up after the budget and surfaces the retryable error (no infinite loop, no hang). | in-process test with repeated forced moves |
| 5 | `find_visible_one` returns the highest-xid visible version and debug-asserts at-most-one live; all SP16–20 MVCC/cross-range suites pass unchanged. | `executor` unit test + regression gate |
| 6 | Cross-range bank total conserved under a multi-process nemesis that kills a **participant leader during STAGE**, with no client-visible retry storms. | multi-process kill-during-stage nemesis |
| 7 | Full gauntlet green; no new dependency; `#![forbid(unsafe_code)]`; traceability. | gauntlet + traceability |

## Test plan

**Sleep policy.** The in-process tests use the SP16 `before_global_decision`-style deterministic
pause/hook seam (`router.rs:433-436`) plus a new "pause-during-stage" hook to force a leader
move mid-STAGE — no sleeps. The multi-process nemesis paces on a committed-op progress signal +
bounded poll cadence (the established harness pattern). The retry loop is driven by `NotLeader`
responses, never a timer.

1. **Pause-during-stage re-attempt (in-process)** — using `MultiRangeCluster` / the router's
   test seams: stage participant A, then force participant B's range leader to move as B is
   staged; assert the coordinator aborts `g`, re-attempts under `g'`, the txn commits, and a
   post-commit read shows exactly one live version of each row with the correct value
   (criteria 1–3).
2. **Bounded-retry exhaustion (in-process)** — force a leader move on every attempt; assert the
   coordinator gives up after the budget and returns the retryable error, with no hang and no
   half-applied state (criterion 4).
3. **`find_visible_one` hardening (executor unit)** — two versions of one row visible under one
   snapshot: assert the highest-xid is returned and the debug-assert fires in debug builds
   (criterion 5).
4. **Multi-process kill-during-stage nemesis** — a UAC-safe `crossrange_2pc_*` binary (no
   `setup/install/update/patch/upgrad` in the target name, per the os-740 rule) that kills a
   participant leader during the STAGE window while a cross-range bank workload runs; assert
   conservation + bounded re-attempts (criterion 6).
5. **Regression** — all SP16–20 cross-range conservation + recovery suites stay green (the
   re-attempt must not change any happy-path decision or any recovery behavior).
6. **Gauntlet** — `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D
   warnings`; `cargo nextest run --workspace` + `cargo test --workspace --doc`; `cargo deny
   check`; UAC guard; traceability.

## Non-goals (explicit → later)

- **Byte reclamation of abandoned-`g` shadow versions.** A re-attempt leaves the old `g`'s
  tuple versions on disk, invisible (Aborted `g`). Reclaiming them is the MVCC **vacuum arc**
  (the SP20 Non-goal), not this slice.
- **A durable coordinator record.** SP18 deliberately avoided one; this design needs none (the
  live coordinator drives the active `g'`; abandoned `g`s are handled by the existing sweeps).
- **Re-staging across a coordinator/gateway crash.** If the *coordinator* (gateway) dies
  mid-txn, the SP18 participant self-resolve path finalizes the in-doubt `g` (unchanged). SP21
  handles only a *participant range leader* move while the coordinator stays alive.
- **Surgical same-`g` re-stage / carried-`Li` idempotency / `g`-keyed version identity** — the
  rejected approaches; not built.

## Risks (and mitigations)

- **The re-attempt must abort the old `g` before (or concurrently with) beginning `g'`, and must
  not leak a half-aborted `g`.** Mitigated by reusing `finish_txn`'s release loop +
  `commit_global(g, false)` (a single durable write-once Abort), and proven by criterion 3 (locks
  freed) + the conservation oracle.
- **Replay must be faithful and silent.** Re-running buffered statements must produce the same
  writes under `g'` and must not re-emit client output. Mitigated by buffering the exact parsed
  statements + replaying through the same routing with output suppressed for already-returned
  statements; proven by criterion 2 (one live version, correct value).
- **Unbounded churn could loop.** Mitigated by the bounded retry budget (criterion 4); on
  exhaustion the client gets today's retryable error — never worse than SP18.
- **The buffer adds per-txn overhead.** Only in escalatable routers; a small `Vec` of parsed
  statements, dropped at txn end. Negligible; no hot-path lock.
- **`find_visible_one` change must not alter happy-path reads.** It only formalizes the existing
  last-wins as highest-xid + adds a debug assert; the SP16–20 MVCC suites are the regression
  guard (criterion 5).

## Traceability

(Appended at finish — maps each success criterion 1–7 to its proving test.)
