# SP23 / D3c — GTM global-xid reuse across a range-0 leadership change

**Date:** 2026-06-15
**Slice:** SP23 (range-0 leadership-rise recovery — the root cause of the deferred participant-leader-kill non-convergence)
**Status:** design
**Stacked on:** SP22 (PR #38, unmerged) — needs SP22's `RecoveryGate` + rise-sweep + write checks.
Rebase `--onto origin/main` once SP22 squash-merges.

## Summary

A multi-process probe (2026-06-15) precisely root-caused the participant-leader-kill non-convergence
that SP21 and SP22 deferred. It is **not** a settle-before-serve gap or a vague "cascading-failover
2PC atomicity" problem — it is **GTM global-xid reuse from reseed apply-lag**:

1. `begin_global_durable` correctly commits `next_global = g+1` to quorum *before* handing out `g`.
2. The range-0 leader is killed; a new range-0 leader rises.
3. `reseed_on_leadership` reseeds the GTM counter **on the rising edge with no apply-wait**:
   `Gtm::reseed_from_applied` reads the *applied* store, which lags the committed counter advance on
   a freshly-risen leader. So the in-memory `next_global` regresses below an already-allocated `g`.
4. `begin_global` re-hands-out that `g`. The SP21 `staged_local_for(g)` idempotency no-op (in
   `TxnService::stage`) then matches the prior txn's stale `Prepared(-> g)` marker and either
   short-circuits a stage that should write a fresh version, or lets two versions go live — a
   **duplicate live MVCC version** on range 0 → a `+money` conservation tear, or a 2-live
   `debug_assert` crash-loop (the "wedge"). The probe reproduced this deterministically with perfect
   correlation across 42 runs.

This is the **task-#19 / SP7 "xid reuse across failover" class**, now at the GTM global counter.

SP23 eliminates it structurally: range 0's leadership-rise recovery **apply-waits, reseeds the GTM
counter from the now-current applied state, settles inherited in-doubt markers, then opens range 0's
`RecoveryGate`** — and range 0's gate, while closed, blocks **both** GTM allocation (`begin_global`)
**and** participant writes. So no `g` is ever allocated from a stale (regressed) counter, and no
participant write reads an unsettled inherited marker.

## Root cause (confirmed by the probe)

- `Gtm::begin_global` (`executor/src/gtm.rs:59`) bumps the **in-memory** `next_global` and returns
  `g`; the **durable** `meta_next_global_xid` is advanced by `begin_global_durable`'s
  `committer.commit(next_global_xid_op)` (quorum-durable before `g` is returned) and max-merged by
  the state machine on apply (never regresses durably).
- `Gtm::reseed_from_applied` (`gtm.rs:75`) lifts the in-memory counter to the **applied** durable
  value (`max`, never regresses below it). On a freshly-risen leader the latest committed advance
  may not be **applied** yet (apply-lag), so the reseed reads a stale value and the in-memory
  counter regresses below an allocated `g`.
- `reseed_on_leadership` (`cluster/src/server_node.rs:789`) calls `reseed_gtm()` on the rising edge
  (`is_leader && !was_leader`) with **no apply-wait** — the apply-lag window.
- Range 0 is also UNGATED/UNSWEPT (SP22 deferred it; both bring-up loops `filter(|&r| r != 0)`), so
  even the in-memory counter is read by `begin_global` before any reseed completes during the
  recovery window.

## Decisions (locked during brainstorming)

1. **Apply-wait before the GTM reseed.** The reseed must read the *committed* high-water-mark, not a
   lagged applied value — so it never regresses below an allocated `g`. Reuse SP22's rise-sweep
   apply-wait idiom (`ensure_linearizable` + `applied_index_at_least`, bounded).
2. **Gate `begin_global` (GTM allocation) on range 0's `RecoveryGate`** — one apply-wait per
   leadership rise, reusing SP22 infrastructure — rather than an `ensure_linearizable` round-trip on
   *every* cross-range begin (which would double cross-range begin latency).
3. **Reseed inside the gate-opening sweep.** The range-0 rise sweep
   (`resolve_in_doubt_on_leadership`) does, in order: apply-wait → `reseed_gtm` (+ `reseed_counters`)
   → settle inherited markers → `mark_served`. The reseed happens in the same task that opens the
   gate, so "reseed completed" is exactly "gate open" — no cross-task coordination.
4. **Fold in the range-0 settle-before-serve extension** (register range 0 in the gate; the sweep
   settles inherited range-0 participant markers; gate range-0 participant writes). It is the *same*
   sweep and closes the in-principle range-0 unsettled-marker-read gap, with the recovery scan bounded
   at `GLOBAL_XID_BASE` (range 0's clog mixes participant markers with the global decision clog).
5. **Do NOT gate `CommitGlobal` / `GlobalBarrier` / `Release`.** Those are recovery paths (the sweep
   itself abort-races via `CommitGlobal`); gating them on a closed range-0 gate would deadlock
   recovery. Only `BeginGlobal` (allocation) and participant `Stage`/DML writes are gated.
6. **Keep the SP21 `staged_local_for` idempotency.** It is correct *once `g` is never reused*; SP23
   removes the reuse, so the no-op is safe again. (Do NOT add a `staged_local_for` no-op to the
   gateway-local `stage_on` branch — that one was separately confirmed unsafe and SP22 left it as a
   gate-only check.)

## Components

### 1. Range-0 rise sweep: apply-wait → reseed → settle → open (`server_node.rs`)
Register range 0 in the gate and spawn `resolve_in_doubt_on_leadership` for it (both bring-up paths),
with `register_range` strictly before the spawn (the SP22 wedge-prevention ordering). The sweep, after
its apply-wait and before `mark_served`, calls `engine.reseed_gtm()` and `engine.reseed_counters()` so
the in-memory GTM/local counters reflect the now-current applied high-water-mark. Then it settles
inherited markers (`in_doubt_globals_from` → abort-race → `advance_clog_scan_lo`) and `mark_served`.

### 2. Gate `begin_global` on range 0's gate (`transport/server.rs::handle_txn`)
In the `TxnRpc::BeginGlobal` arm, before `engine(0).begin_global_durable()`, check range 0's gate
(via a new `TxnService` accessor, e.g. `is_serving(0)`); if closed, return retryable `TxnResp::NotLeader`
(the coordinator re-resolves + retries). `CommitGlobal`/`GlobalBarrier`/`Release` are unchanged.

### 3. Range-0-safe recovery scan (`executor/src/lib.rs`)
Bound the `in_doubt_globals_from` and `staged_local_for` scans at `kv::key::clog_key(GLOBAL_XID_BASE)`
so the range-0 recovery scan covers only local participant markers (`< GLOBAL_XID_BASE`) and the
watermark never jumps into the global-decision space (`>= GLOBAL_XID_BASE`). No-op on data ranges.

### 4. Gate the range-0 participant write path
Range 0 is now registered, so the existing SP22 checks (`TxnService::stage` + `RangeRouter::dispatch`)
automatically gate range-0 participant Insert/Update/Delete. Confirm the gateway-local `stage_on`
branch stays GATE-ONLY (no `staged_local_for` no-op).

### 5. Verify the GTM coordinator path is not gated
`begin_global_durable`/`commit_global_decision` reach range 0 via `handle_txn`'s Begin/CommitGlobal —
only `BeginGlobal` gets the new gate check; `CommitGlobal`/`GlobalBarrier`/`Release` and the sweep's
own `CommitGlobal` abort-races are never gated, so a closed range-0 gate cannot deadlock 2PC recovery.

## Why it's safe

- **No reused `g`.** Allocation (`begin_global`) is admitted only after the apply-waited reseed lifts
  the in-memory counter to the committed high-water-mark, which is `>= ` every durably-allocated `g`.
  So `staged_local_for(g)` can never alias a stale marker.
- **No unsettled-marker read.** Range-0 participant writes are admitted only after the sweep settled
  every inherited `Prepared(L0 -> g)` marker for the current term (the SP22 invariant, now on range 0).
- **Recovery not deadlocked.** Only allocation + participant writes are gated; the sweep's
  `CommitGlobal` abort-races and the coordinator's commit/release run ungated.
- **Durable counter already monotone.** The state machine max-merges `next_global`, so the *durable*
  value never regresses; SP23 only closes the *in-memory* regression window on a new leader.
- **The pre-existing rising-edge reseed is retained and harmless.** Both bring-up paths already spawn
  `reseed_on_leadership` for range 0 (the un-apply-waited rising-edge `reseed_gtm` — the bug). SP23
  keeps it (it also reseeds the local procarray/seq counters): `reseed_from_applied` is lift-only
  (`max`, never regresses), and since `begin_global` is gated until the apply-waited sweep reseed +
  `mark_served`, no allocation can ever observe a stale-but-lifted counter. Only the *networked*
  allocation path (`handle_txn`'s `BeginGlobal`) is gated; the in-process `LocalCoordinator` path is
  not, which is safe because the in-process harness wires a `None` gate and never regresses the
  counter (it does not kill the range-0 leader).

## Success criteria

| # | Criterion | Verified by |
|---|---|---|
| 1 | A range-0 leadership change never reuses a global xid (the in-memory counter never regresses below an allocated `g`). | executor unit test: stale-applied reseed + apply-wait; the GTM never hands out a previously-allocated `g`. |
| 2 | `begin_global` is rejected (retryable) while range 0's rise reseed+settle is incomplete, and admitted once complete. | in-process gate test on the `BeginGlobal` path. |
| 3 | The g-reuse → duplicate-version invariant holds under all interleavings; the no-gate / no-reseed variant is caught. | Stateright model with teeth. |
| 4 | The range-0-leader-kill stable-window nemesis conserves the bank total and passes 3× non-flaky (no 2-live crash, no recovery wedge, no tear). | multi-process nemesis (the probe's drain-aware harness). |
| 5 | The range-0-safe scan bound; reads/`CommitGlobal`/`GlobalBarrier`/`Release` never gated; existing suites unchanged. | executor unit test + regression gate. |
| 6 | Full gauntlet green; no new dependency; `#![forbid(unsafe_code)]`; traceability. | gauntlet + traceability. |

## Test plan

**Sleep policy.** Condition-driven waits only (openraft `wait().applied_index_at_least`, the metrics
watch, the multi-process harness's bounded poll cadence) — no `sleep`-to-settle, per CLAUDE.md.

1. **Executor unit test (the core):** open a `Gtm`, allocate `g`, durably advance the counter, then
   simulate a stale-applied reseed (durable value lags) and assert that WITHOUT an apply-wait the
   counter regresses (teeth), and WITH the apply-waited reseed the GTM never re-hands-out `g`.
2. **Stateright model with teeth:** model the global-xid counter + a durable allocation + a leadership
   reseed (apply-current vs apply-lagged) + the `staged_local_for` no-op. Property: no `g` is allocated
   twice → no duplicate live version. Toggle `apply_wait_reseed`: `true` upholds it; `false` (the bug)
   makes the checker find the reused `g` / duplicate. Mirror the SP22 settle-model structure.
3. **In-process gate test:** `begin_global` returns retryable while range 0's gate is closed and
   succeeds after `mark_served` for the term.
4. **Multi-process range-0-leader-kill nemesis** (`crates/crabgresql/tests/...` UAC-safe name): kill
   `c.range_leader(0)` with a FULL-DRAIN stable window between kills (single failover at a time, no
   overlapping). Conservation + non-vacuity, **3× non-flaky**. (The probe's drain harness is the start.)
5. **Regression:** `crossrange_2pc_{nemesis,replicated}`, the in-process cross-range suites, and the
   SP21/SP22 tests stay green.
6. **Gauntlet:** fmt; clippy `-D warnings`; `nextest --workspace --profile ci` + doctests; `deny`;
   UAC guard; traceability.

## Non-goals (explicit → later)

- **The local-xid (procarray/seq) reseed apply-lag** beyond what the shared apply-wait incidentally
  fixes. SP23's apply-wait covers all counters on the range-0 rise; a dedicated audit of every range's
  local-xid reseed apply-lag is out of scope unless the nemesis surfaces one.
- **A residual cascading-failover (overlapping coordinator+participant) 2PC gap**, if any remains
  after the g-reuse fix. The nemesis uses a full-drain stable window (single failover at a time); a
  *kill-every-round* (overlapping) nemesis is a separate future slice if a residual is found.
- **Changing the SP21 idempotent `Stage` / `staged_local_for`** beyond the range-0-safe scan bound —
  it is correct once `g` is never reused.
- **Gating reads / `CommitGlobal` / `Release`** — they cannot reuse a `g` or create a duplicate.

## Risks (and mitigations)

- **The apply-wait index must be the committed no-op for the term.** Too low → reseed still stale;
  too high → hang. Mitigation: reuse SP22's proven `ensure_linearizable` + `applied_index_at_least`,
  bounded by a timeout; criterion 1/4 re-expose a missed advance as a reused `g`.
- **Gating `begin_global` could stall the coordinator if the gate never opens.** Mitigation: the
  sweep re-fires while the gate is closed (SP22's retry-while-closed), opens on a bounded settle; the
  coordinator retries `NotLeader`. A permanently-wedged range is observable (the SP22 debug log).
- **The reseed must run before `mark_served`.** Putting the reseed inside the gate-opening sweep makes
  this ordering structural (no cross-task race).
- **Residual after the g-reuse fix.** If the nemesis still tears, capture the mechanism (it would be a
  genuinely new finding, not the now-fixed g-reuse) and scope it explicitly rather than patching inline.

## Traceability

(Appended at finish — maps each success criterion 1–6 to its proving test.)
