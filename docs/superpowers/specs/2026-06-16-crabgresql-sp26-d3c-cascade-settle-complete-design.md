# SP26 / D3c — settle-COMPLETE-before-serve: cross-range 2PC atomicity under an overlapping range-0 failover

**Date:** 2026-06-16
**Status:** as-shipped (the SP22/SP23-deferred cascading-failover atomicity tear is fixed for the
range-0 overlapping-leader-kill case; the read-path follower GTM-staleness it surfaced is scoped to a
follow-up — see Non-goals)

## Problem

SP16–SP25 built cross-range 2PC over Raft with range 0 as the GTM (global-xid allocator) + global
decision clog home + `acct_a` participant, and hardened it slice by slice: write-once decisions
(SP18), the recovery watermark (SP20), participant-Stage idempotency (SP21), the settle-before-serve
`RecoveryGate` for data ranges (SP22), the GTM reseed-before-allocate fix (SP23), exhaustive models
(SP24), and committed-half survival on a clean participant-leader kill (SP25). Two slices (SP22, SP23)
EXPLICITLY DEFERRED the **cascading / overlapping-failover** case: a range-0 leader killed mid-2PC
while a prior failover is *still recovering*, with no full-drain stable window between kills. SP22
recorded that "the participant-leader-kill problem has more layers than settle-before-serve closes"
and that incremental patches "shift the failure mode (wedge↔tear) without converging."

This slice closes that gap for the range-0 overlapping-leader-kill case.

## Reproduction (empirical ground truth)

An overlapping range-0-leader-kill nemesis (kill the range-0 leader every round, in-flight 2PC, NO
drain) tears the cross-range bank total ~1-in-3 runs, **bidirectionally** (+money and −money). The
+money tears are a genuine **duplicate** (two live MVCC versions of one row → the at-most-one-live
invariant violated). The −money tears turned out to be a **lagging-follower read artifact** (below).

## Root cause (the duplicate)

A participant half's visibility resolves ENTIRELY through range-0's write-once `clog[g]`
(`commit_release`/`abort_release` are symmetric — they only free locks, write no per-participant clog
entry), so the two halves of a committed `g` can never disagree on the *decision*. The tear is
therefore the at-most-one-live (duplicate) class, and it comes from the rise sweep opening its write
gate too early:

`server_node::resolve_in_doubt_on_leadership` (the SP22 settle-before-serve sweep) opens the gate
(`mark_served`) on `settled.is_ok()` — i.e. once apply-wait + `reseed_gtm` succeed — **regardless of
whether every inherited in-doubt `Prepared(Li -> g)` marker's abort-race actually landed.** The
abort-race (`client.call(0, CommitGlobal{g, false})`) is best-effort + warn-only, and `CommitGlobal`
is un-gated. Under an OVERLAPPING failover (the risen leader itself loses leadership again mid-sweep),
an abort-race can fail to land, leaving a marker in-doubt — yet the gate opens anyway. A new gated
write then lands, reads the still-in-doubt marker as INVISIBLE, and supersedes the *older* committed
head instead of the marker. When the marker is finally decided COMMITTED, its version and the new
write's version are BOTH live (neither supersedes the other): the duplicate that tears the total.

This is the same MVCC at-most-one-live consequence the SP22 `crossrange_2pc_settle_model` and SP23
`crossrange_2pc_gtm_reuse_model` guard, via a hole they do not cover: those models assume the sweep,
once it runs, fully settles. The overlapping-failover dimension — where the abort-race can FAIL to
land — is new.

## Fix — settle-COMPLETE-before-serve

`resolve_in_doubt_on_leadership` opens the gate ONLY once the sweep has driven every inherited in-doubt
marker to a durable terminal decision. After the abort-race loop it **re-scans** `in_doubt_globals_from`;
if any marker is still in-doubt, the settle FAILS (returns `Err`) so the gate stays CLOSED and the
sweep retries on the next wake. Only when the re-scan is empty does it advance the watermark and
`mark_served`. This is genuine settle-*before*-serve, and it CONVERGES (unlike the incremental patches
SP22 warned against): the gate stays closed until a *stable* leader can finalize every marker, then
opens. It preserves committed-half survival (SP25): `in_doubt_globals_from` returns only NON-terminal
`g`s, so a durably-committed marker is never abort-raced — it is left to resolve live. No new
dependency; the change is local to the sweep.

The fix is one boolean of behavior; nothing else in the recovery path changes. `CommitGlobal` stays
un-gated (gating it is unnecessary — the model proves the re-scan gate alone closes the hole — and
risks deadlocking the sweep's own abort-race, which routes through `CommitGlobal`).

## Components

- **A. `server_node::resolve_in_doubt_on_leadership`** — the settle-complete re-scan before
  `mark_served` (the fix).
- **B. `cluster::crossrange_2pc_overlap_settle_model`** — the exhaustive Stateright model with teeth
  (the deterministic proof).
- **C. `crabgresql::range0_cascade_kill_bank`** — the multi-process overlapping-range-0-leader-kill
  nemesis (the empirical end-to-end proof + convergence/conservation regression).

## Testing / traceability

| # | Claim | Proof |
|---|---|---|
| 1 | The rise sweep never opens the gate while an inherited marker is in-doubt (under any overlapping interleaving). | `crossrange_2pc_overlap_settle_model::settle_complete_before_serve_upholds_at_most_one_live` (positive, `unique_state_count > 1`). |
| 2 | That requirement is load-bearing: removing it produces a two-live-versions counterexample. | `crossrange_2pc_overlap_settle_model::opening_the_gate_before_settling_double_lives_is_caught` (teeth — names `"at most one live version per row"`). |
| 3 | An overlapping range-0 leader kill conserves the durable cross-range bank total. | `range0_cascade_kill_bank::range0_cascade_leader_kill_conserves_total` (multi-process, authoritative read; non-flaky). |
| 4 | The fix does not regress the happy path or the existing single-failover nemeses. | `crossrange_2pc_{nemesis,replicated}`, `range0_leader_kill_drain`, `participant_kill_bank`, full `cluster`/`executor` suites. |

## Non-goals (explicit → later)

- **Cross-range read linearizability on a lagging follower gateway.** Reproducing the tear surfaced a
  −money signature that is a READ-staleness, not a durability loss: a follower gateway queried right
  after a burst of range-0 churn can resolve a just-committed `acct_b` credit as still-in-doubt
  (its local range-0 replica's GTM view lags) and under-report it. An authoritative read (the GTM
  home, range 0's leader) always sees the committed credit — the durable state is conserved. The
  conservation oracle therefore reads through the range-0 leader. Tightening cross-range read
  linearizability for lagging followers under extreme churn is a separate read-path concern (SP12 /
  SP24 territory), deferred.
- **Overlapping range-0 *and* range-1 co-failover beyond what a single-node kill induces.** The
  nemesis kills one node (which co-leads range 0 and sometimes range 1), the established cascading
  fault; a partition-driven simultaneous dual-range failover is covered by `jepsen_bank` and is not
  re-targeted here.

## Success criteria

1. The overlapping-range-0-kill nemesis conserves the total (was a ~1-in-3 tear). — MET (C).
2. The settle-complete requirement is proven exhaustively with teeth. — MET (B).
3. No regression of the SP16–SP25 suites. — MET (traceability #4).
