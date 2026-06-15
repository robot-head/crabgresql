# SP22 / D3c-settle-before-serve — A newly-risen range leader settles inherited in-doubt 2PC markers before serving writes

**Date:** 2026-06-14
**Slice:** SP22 (D3c-settle-before-serve)
**Status:** as-shipped (data-range settle-before-serve; range-0 participant + multi-process
participant-leader-kill nemesis DEFERRED — see "CORRECTION (as-shipped)")
**Stacked on:** SP21 (PR #37, unmerged) — needs SP21's idempotent `Stage`, resolve-then-release,
and the `find_visible_one`/`scan_live` at-most-one-live detector. Rebase `--onto origin/main`
once SP21 squash-merges.

## Summary

Closes the last open cross-range 2PC fault: a **participant-range leader killed during the
STAGE window**. A killed leader (process death) leaves a durable `Prepared(Li -> g)` marker on
the new leader with **no in-memory lock or held session** (locks aren't replicated). A writer
that touches that row on the new leader *before* the marker settles reads `g` as in-doubt,
supersedes the older version, and writes a SECOND non-superseding version — so two MVCC
versions go live when both `g`s commit. SP21's at-most-one-live `debug_assert!` then crashes
every replica's read path (→ universal 08006 → recovery hangs), or with the assert relaxed the
bank conservation total tears (e.g. 790/817 vs 800).

SP22 eliminates the window **structurally**: a newly-risen range leader does not serve *writes*
for that range until its leadership-rise in-doubt sweep — preceded by an apply-wait so the scan
sees every inherited marker — has settled all of them. A per-range **term-based recovery gate**
enforces this. No writer ever reads an unsettled inherited marker, so no duplicate is ever
created.

## CORRECTION (as-shipped)

**SP22 ships the settle-before-serve gate proven for DATA ranges; the range-0 participant
extension and the multi-process participant-leader-kill nemesis are DEFERRED.** The slice's own
multi-process nemesis (`crossrange_2pc_restage`, kill `range_leader(1)` every round) did not
converge, and root-cause digging (2026-06-15) found the participant-leader-kill problem has more
layers than settle-before-serve alone closes:

- **Range 0 is a 2PC participant on every cross-range transfer** (acct_a lives in range 0) but was
  ungated/unswept (the bring-up loops `filter(|&r| r != 0)`). The "range 0 = non-goal" justification
  ("the nemesis only kills `range_leader(1)`") is **unsound**: killing the range-1 leader kills a
  node that is also a range-0 voter/sometimes-leader, so the nemesis churns range 0 too. A correct
  range-0 extension also needs the recovery scan bounded at `GLOBAL_XID_BASE` (range 0's clog mixes
  participant markers with the global decision clog).
- **An idempotency no-op on the gateway-local participant stage is UNSAFE under global-xid reuse**
  (a GTM reseed across range-0 churn can reuse a `g`, so `staged_local_for(g)` matches a stale
  marker → a committed-with-missing-half tear). The gate, not idempotency, is the load-bearing
  local-stage protection.
- Even with range 0 gated+swept and that no-op removed, a **residual conservation tear + recovery
  wedge** persist under kill-every-round — a deeper 2PC-atomicity-under-cascading-failover gap whose
  failure mode shifts (wedge↔tear) without converging, exactly the incremental-patch anti-pattern.

**As-shipped, SP22 = T1–T5:** `RecoveryGate` + the leadership-rise apply-wait/settle/`mark_served`
sweep + the two write-path checks + the Stateright settle model, proven by in-crate unit tests, the
model-with-teeth, and the existing `crossrange_2pc_nemesis`/`crossrange_2pc_replicated` regression
(all green). Deferred to a dedicated slice (chip-tracked): the range-0 participant gate+sweep+scan-
bound, and a *converging* participant-leader-kill nemesis (with stable windows, per the no-starve
rule) for the cascading-failover 2PC gap. The deferred design is captured in the plan's "Task 6.5"
section; do NOT re-attempt the incremental approach.

## Why incremental patches failed (do not repeat)

SP21 tried, and reverted, two symptom-patches that *raced* the window rather than eliminating it
(failure mode shifted hang → 790 → 817 without converging):

- A **periodic session-less in-doubt sweep** that abort-races markers with no held session.
- An **executor `eval_plan_qual` guard** returning `SerializationFailure` on a foreign in-doubt
  marker.

The correct fix gates *writes* until recovery completes, so the duplicate is never created.
Reads are never gated — a read resolves an in-doubt version as invisible and cannot create a
duplicate.

## What SP21 already shipped (the foundation this builds on)

- **The detector:** `find_visible_one`/`scan_live` select the greatest-xmin live version and
  `debug_assert!(live_count <= 1)` (`crates/executor/src/exec.rs`). SP22 does NOT weaken it.
- **Idempotent participant `Stage` per `(g, range)`:** `SqlEngine::staged_local_for` + the
  held-session-aware check in `TxnService::stage` (`crates/cluster/src/twopc.rs`).
- **Leadership-loss resolve-then-release:** `TxnService::resolve_and_release_for_range`, called
  from `release_on_leadership_loss` (`crates/cluster/src/server_node.rs`).

## Decisions (locked during brainstorming)

1. **Term-based airtight gate**, not a flag. A write to range R is admitted only when R's
   leadership-rise sweep has completed *for the current Raft term* — derived atomically from the
   term, so there is NO rise-edge race window. (A flag set on the rising edge has a microsecond
   window between the metrics flipping to leader and the watcher setting the flag — the exact
   class of race that made the patches fail.)
2. **Gate writes only; reads pass.** A read cannot create a duplicate.
3. **Gate at the cluster write entry points**, not the committer. Every write to a range funnels
   through its `RaftCommitter`, but the rise sweep's OWN writes (the watermark advance) go
   through the same committer — a committer-level gate would deadlock the sweep. So the check
   lives at `TxnService::stage` (remote participant) and the gateway router's local-led write
   path.
4. **Apply-wait before the scan.** The rise sweep waits until this node has applied through its
   committed index before scanning the durable clog, so `in_doubt_globals_from` sees every
   inherited marker (today the sweep can run before applying them — the apply-lag miss).

## Components

### 1. `RecoveryGate` (new, `crates/cluster`)

Per-range recovery state, shared (`Arc`) across `TxnService`, the gateway router, and the
rise-sweep tasks. Holds, per range, the Raft handle + an `Arc<AtomicU64>` `served_term`:

```rust
pub struct RecoveryGate {
    // per range: (raft handle, last term whose rise sweep completed)
    ranges: HashMap<RangeId, (openraft::Raft<TypeConfig>, Arc<AtomicU64>)>,
    id: NodeId,
}
```

- `served_term` initializes to a sentinel (`0`) below any real Raft term, so a range is **gated
  by default** and on **every** fresh rise until its sweep opens it.
- `is_serving(range) -> bool`: this node IS R's current leader **and**
  `served_term[range] == current Raft term` (read from `raft.metrics()`). A range this node does
  not host returns `true` (not this node's concern — the write forwards/rejects via the normal
  not-local-leader path).
- `mark_served(range, term)`: store `served_term[range] = term`. Called by the rise sweep after
  it settles.

Rationale for a standalone struct over a `served_term` field on `SqlEngine`: the term comparison
needs the per-range Raft handle (a cluster type), which the engine (executor) does not hold;
keeping the gate in `cluster` avoids leaking a Raft handle / a recovery concept into the executor
layer. Paths that don't wire a gate (the in-process `MultiRangeCluster`) hold `Option<Arc<RecoveryGate>>`
= `None` and treat it as "always serving" (the in-process harness has no killed-leader recovery).

### 2. Rise-sweep changes — `resolve_in_doubt_on_leadership` (`server_node.rs`)

On the rising edge for range R at term T, in order:
1. **Apply-wait:** read the node's committed index from `raft.metrics()`, then
   `raft.wait(Some(timeout)).applied_index_at_least(Some(committed), "settle-before-serve")`.
   This makes the subsequent durable scan see every inherited marker.
2. **Settle (unchanged):** `engine.in_doubt_globals_from(scan_lo)` → abort-race each in-doubt `g`
   via `CommitGlobal{g, commit:false}` → `advance_clog_scan_lo(new_lo)`.
3. **Open the gate:** `gate.mark_served(R, T)`.

The gate is closed for R whenever `served_term[R] != current_term`, i.e. from the instant the
node rises at term T until step 3 — so writes are rejected for the entire settle window.

### 3. Write-path gate checks (two points, one gate)

- **`TxnService::stage(g, range, sql)`** — at the top (alongside the existing engine-present and
  idempotency checks): if `!gate.is_serving(range)` → return `TxnResp::NotLeader`. The coordinator
  already re-resolves + retries on `NotLeader`, so a brief recovery window just delays the stage.
- **Gateway router local-led WRITE path** — before running a table-bearing
  `Insert`/`Update`/`Delete` on a locally-led range (`stage_on`/`run_on`'s local branch): if
  `!gate.is_serving(range)` → `Err(ExecError::NotLeader)` → SQLSTATE 40001 → client retries.
  `Select` (and DDL / txn-control) pass through ungated.

### 4. Wiring (`server_node.rs`)

`server_node` constructs one `Arc<RecoveryGate>` from its per-range `rafts` + `cfg.id`, then
threads it into: `TxnService::new` (new field), each `resolve_in_doubt_on_leadership` spawn, and
the gateway router construction (`spawn_sql_gateway` → the per-connection router). Both the static
and replicated bring-up paths wire it identically.

## Why it's safe (no invariant breakage)

- **No unsettled-marker read by a writer.** A range serves writes only after its rise sweep
  (with apply-wait) settled every inherited in-doubt marker for the current term. So when a write
  is admitted, every `Prepared(-> g)` marker on its rows has a terminal `g` — exactly the
  `eval_plan_qual` invariant the SP21 bug violated.
- **Airtight against leadership flap.** `served_term` is compared to the live Raft term; a flap to
  a new term re-closes the gate until that term's sweep completes.
- **Liveness bounded.** The settle window is one apply-wait + one bounded clog scan (~tens of ms);
  gated writes get a retryable error the bank workload + `exec_until_ok` already retry. The
  sweep's abort-race + the SP21 fixes guarantee markers reach a terminal decision.
- **SP21 fixes untouched.** Idempotent `Stage` and resolve-then-release remain; SP22 only adds the
  gate + apply-wait.

## Success criteria

| # | Criterion | Verified by | Status |
|---|---|---|---|
| 1 | A participant-range leader killed during STAGE no longer corrupts a row: the cross-range bank total is conserved. | multi-process participant-leader-kill nemesis | **DEFERRED** — the kill-every-round nemesis did not converge; the participant-leader-kill problem has more layers than the data-range gate closes (range-0 participant gap + cascading-failover 2PC). Deferred to a dedicated slice. |
| 2 | That nemesis passes **3× non-flaky**. | repeated runs | **DEFERRED** (with #1). |
| 3 | The gate rejects writes to a range whose current-term rise sweep has not completed, and admits them once it has. | unit test on `RecoveryGate` + `TxnService::stage` gate test + the router real-gate test | **MET** |
| 4 | The settle-before-serve invariant holds under all interleavings; the no-gate variant is caught. | Stateright model with teeth (`crossrange_2pc_settle_model.rs`) | **MET** |
| 5 | Reads are never gated; the happy path and existing cross-range suites are unchanged. | regression gate (`crossrange_2pc_{nemesis,replicated}` + full suite) | **MET** |
| 6 | Full gauntlet green; no new dependency; `#![forbid(unsafe_code)]`; traceability. | gauntlet + traceability | **MET** |

The *rise-path* opening (a genuine leadership rise calling `mark_served`) is exercised end-to-end
only by the multi-process suites; the in-process gate tests open the gate by calling `mark_served`
directly. With the participant-leader-kill nemesis deferred, the existing `crossrange_2pc_nemesis`
(kill a non-participant-leader) + `crossrange_2pc_replicated` provide the multi-process rise-path
coverage that stays green with the gate enforced.

## Test plan

**Sleep policy.** All waits are condition-driven (openraft `wait().applied_index_at_least`, the
metrics watch, bounded poll cadence for the multi-process harness) — no `sleep`-to-settle, per
CLAUDE.md.

1. **Stateright model with teeth** (`crates/cluster/tests/crossrange_2pc_settle_model.rs`) — model
   a row's MVCC versions + inherited in-doubt markers + a per-term serving gate. Property:
   at-most-one-live AND no write supersedes while an inherited marker is unsettled. Toggle
   `settle_before_serve`: `true` upholds the invariants; `false` (no gate — the bug) makes the
   checker find the duplicate. Mirrors `model.rs`'s positive + teeth structure.
2. **`RecoveryGate` unit test** (in-process) — construct a gate over a single-node Raft; assert
   `is_serving` is false at a fresh term and true after `mark_served(term)`; assert a write path
   wired to a closed gate returns the retryable error and a post-`mark_served` write proceeds.
3. **Multi-process kill-during-stage nemesis** — **DEFERRED** (did not converge; see "CORRECTION
   (as-shipped)"). The data-range rise path stays covered by the existing `crossrange_2pc_nemesis`
   + `crossrange_2pc_replicated` regression with the gate enforced.
4. **Regression** — `crossrange_2pc_{nemesis,replicated}`, the in-process cross-range suites, and
   the SP21 idempotent-Stage / find_visible_one tests stay green.
5. **Gauntlet** — `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D
   warnings`; `cargo nextest run --workspace` + `cargo test --workspace --doc`; `cargo deny check`;
   UAC guard; traceability.

## Non-goals (explicit → later)

- **Gating reads.** Reads cannot create a duplicate; gating them would only add latency.
- **Byte reclamation of aborted shadow versions** (the MVCC vacuum arc — the SP20 non-goal).
- **Any change to the SP21 fixes** (idempotent `Stage`, resolve-then-release) or to the
  at-most-one-live detector.
- **Coordinator/gateway-crash re-stage** — handled by the SP18 participant self-resolve; SP22 is
  only about a *participant range leader* dying while the coordinator stays alive.
- **Range 0 as a 2PC participant — DEFERRED (not, as originally claimed, harmless).** The bring-up
  loops register/sweep only data ranges (`r != 0`); range 0 (the GTM/global-clog home) gets no
  recovery gate or rise sweep, so a cross-range txn whose participant write lands on range 0 is
  ungated. The original "harmless because the nemesis kills only `range_leader(1)`" claim was
  **wrong** (see "CORRECTION (as-shipped)"): killing the range-1 leader kills a node that is also a
  range-0 voter/sometimes-leader. A correct range-0 extension (register + spawn its sweep; bound the
  recovery scan at `GLOBAL_XID_BASE`; gate the gateway-local participant write WITHOUT the unsafe
  `staged_local_for` no-op) is the first half of the deferred dedicated slice; the cascading-failover
  2PC-atomicity gap is the second half.

## Risks (and mitigations)

- **The apply-wait must use the right index.** Waiting on too low an index would let the scan miss
  a marker; too high would hang. Mitigation: wait on the node's committed index captured at the
  rising edge (after openraft commits the leader's blank no-op), bounded by a timeout; proven by
  criterion 2 (the nemesis would re-expose a missed marker as a 2-live crash).
- **The gate must close on EVERY rise, not just the first.** A `served_term` compared to the live
  term re-closes automatically on a new term; a stale-flag design would not. Proven by criterion 3
  + the repeated-kill nemesis.
- **Over-gating throughput.** All writes to a range pause for its settle window (~tens of ms) on
  each rise. Acceptable (rare event, retryable); the workload tolerates it. If a sweep's abort-race
  can't reach range 0 it leaves the gate closed (safe — writes retry) until the next tick / rise.
- **Threading the gate into the per-connection router.** Mitigated by an `Option<Arc<RecoveryGate>>`
  (absent ⇒ always-serving) so in-process and any never-recovering path are unaffected.

## Traceability

| # | Criterion | Proving artifact | Status |
|---|---|---|---|
| 1 | Conservation under participant-leader kill | (multi-process participant-leader-kill nemesis) | DEFERRED |
| 2 | Nemesis 3× non-flaky | (repeated runs) | DEFERRED |
| 3 | Gate rejects pre-settle, admits post-settle | `cluster::recovery_gate::tests::gate_is_closed_at_a_fresh_term_and_opens_on_mark_served` + `cluster::twopc` `stage_is_gated_until_the_range_is_settled` + the `range::router` real-gate test (`MultiRangeCluster::leader_raft`) | MET |
| 4 | Settle-before-serve invariant, no-gate caught | `cluster::tests::crossrange_2pc_settle_model` (positive + teeth) | MET |
| 5 | Reads ungated; existing suites unchanged | `crossrange_2pc_{nemesis,replicated}` + the in-process cross-range suites + full workspace nextest | MET |
| 6 | Gauntlet green, no new dep, UAC-safe | T7 gauntlet (`fmt`/`clippy -D warnings`/`nextest`/doctests/`deny`/UAC guard) | MET |

**As-shipped scope:** the settle-before-serve `RecoveryGate` (one new dependency-free `cluster`
module) + rise-sweep apply-wait/`mark_served` + the two write-path checks + one new Stateright test
binary (`crossrange_2pc_settle_model`). No new runtime dependency. Criteria 1–2 (the multi-process
participant-leader-kill empirical proof) are deferred to the dedicated follow-up slice.
