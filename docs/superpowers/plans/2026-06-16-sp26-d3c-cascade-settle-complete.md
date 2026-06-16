# SP26 / D3c plan — settle-COMPLETE-before-serve (cross-range 2PC atomicity under an overlapping range-0 failover)

Design: `docs/superpowers/specs/2026-06-16-crabgresql-sp26-d3c-cascade-settle-complete-design.md`.
Branch: `claude/amazing-newton-stk3vw`.

Closes the SP22/SP23-deferred cascading/overlapping-failover atomicity tear for the range-0
overlapping-leader-kill case. Disciplined order: reproduce → model the root cause + fix → fix →
validate → document.

## Task 1 — Reproduce + characterize (empirical ground truth)
- [x] Build a scratch overlapping-range-0-kill probe (kill `range_leader(0)` every round, in-flight
  2PC, NO drain). It tears the bank total ~1-in-3, BIDIRECTIONALLY (+/−money).
- [x] Per-account instrumentation: +money = a DUPLICATE (two live versions); −money traced to a
  lagging-FOLLOWER read of a committed cross-range credit (read from the range-0 leader → 14/14
  clean), i.e. a read-staleness, not a durability loss.

## Task 2 — Model the root cause + fix (Stateright, with teeth)
- [x] `crates/cluster/tests/crossrange_2pc_overlap_settle_model.rs`: one MVCC row, an inherited
  in-doubt marker, the rise sweep decomposed into `AbortRace{lands}` / `CommitInherited` /
  `MarkServed` / `NewWrite` / `Rise`, with the `settle_complete` toggle. Positive test (fix on →
  at-most-one-live holds, `unique_state_count > 1`) + teeth test (fix off → checker catches the
  duplicate and names `"at most one live version per row"`).

## Task 3 — Fix (server_node)
- [x] `resolve_in_doubt_on_leadership`: after the abort-race loop, RE-SCAN `in_doubt_globals_from`;
  if any marker is still in-doubt, FAIL the settle (gate stays closed, retries); only on an empty
  re-scan advance the watermark + `mark_served`. Genuine settle-COMPLETE-before-serve. Preserves
  committed-half survival (only non-terminal `g`s are abort-raced). No new dependency.

## Task 4 — Validate
- [x] Fix re-runs the overlapping probe: +money tears gone; durable state conserved (authoritative
  read 14/14 clean).
- [x] Promote the probe to a committed nemesis `crates/crabgresql/tests/range0_cascade_kill_bank.rs`
  (authoritative conservation read from the GTM-home range-0 leader). Passes 4× non-flaky.
- [x] Regression: `crossrange_2pc_{nemesis,replicated}`, `range0_leader_kill_drain`,
  `participant_kill_bank`, full `cluster` + `executor` suites, doctests, fmt, clippy.

## Task 5 — Document + UAC + finish
- [x] Spec + this plan.
- [x] CLAUDE.md SP26 audit paragraph (two new binaries: `cluster::crossrange_2pc_overlap_settle_model`,
  `crabgresql::range0_cascade_kill_bank` — both UAC-safe; no new dependency). UAC guard returns empty.
- [ ] Commit, push `-u origin claude/amazing-newton-stk3vw`, open a ready-for-review PR.

## Non-goals (deferred — see spec)
- Cross-range read linearizability on a lagging follower gateway under extreme range-0 churn (the
  −money read-staleness; a separate read-path concern).
- Partition-driven simultaneous dual-range failover (covered by `jepsen_bank`).
