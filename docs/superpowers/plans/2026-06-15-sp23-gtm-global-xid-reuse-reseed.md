# SP23 / GTM global-xid reuse across range-0 leadership change — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A range-0 leadership change never reuses a global xid: the range-0 rise sweep apply-waits, reseeds the GTM counter from the now-current applied state, settles inherited in-doubt markers, then opens range 0's `RecoveryGate` — and the gate blocks `begin_global` (allocation) + participant writes until then.

**Architecture:** Extend SP22's per-range `RecoveryGate` to range 0. The range-0 leadership-rise sweep (`resolve_in_doubt_on_leadership`) gains a `reseed_gtm`/`reseed_counters` step *after* its apply-wait and *before* `mark_served`. `begin_global` (handled in `transport/server.rs::handle_txn`) is gated on range 0's gate via a new `TxnService::is_serving` accessor. The recovery scan is bounded at `GLOBAL_XID_BASE` so range 0's mixed clog (participant markers + global decisions) doesn't corrupt the watermark.

**Tech Stack:** Rust 2024, openraft 0.9.24, arc-swap, tokio, cargo-nextest, stateright. **No new dependency.**

**Stacked on SP22** (branch `sp23-gtm-global-xid-reuse-reseed` off `sp22-d3c-settle-before-serve`). Rebase `--onto origin/main` once SP22 (PR #38) squash-merges. A throwaway reference implementation of the range-0 registration + sweep + scan-bound (NOT the reseed/gate-begin parts) is on branch `sp23-range0-probe` (commit `f022514`).

---

## Background the implementer needs (read once)

**The bug** (probe-confirmed, deterministic). `Gtm::begin_global` (`executor/src/gtm.rs:59`) bumps the **in-memory** `next_global` and returns `g`; `begin_global_durable` (`executor/src/lib.rs:219`) commits `next_global = g+1` to quorum *before* returning `g`. On a range-0 leader kill, the new leader's `reseed_on_leadership` (`cluster/src/server_node.rs:759`; the rising-edge `reseed_counters()` then `reseed_gtm()` calls are at `:768-769`) calls `reseed_gtm` → `Gtm::reseed_from_applied` (`gtm.rs:75`) on the **rising edge with no apply-wait**, reading the *applied* store, which lags the committed advance. (`BeginGlobal` is served by the private free fn `handle_txn` in `transport/server.rs:235`; its `BeginGlobal` arm is at `:242`.) So in-memory `next_global` regresses below an allocated `g`; `begin_global` re-hands-out `g`; `TxnService::stage`'s `staged_local_for(g)` idempotency no-op aliases the prior txn's stale `Prepared(-> g)` marker → a duplicate live MVCC version on range 0 → a `+money` tear or a 2-live `debug_assert` crash-loop.

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

> **Watermark mechanics (READ — the test design depends on it).** `in_doubt_globals_from` computes `new_scan_lo = first_undecided.or_else(|| max_li.map(|m| m+1))` (`lib.rs:301-304`). `first_undecided` is the smallest `Li` of an in-doubt **`Prepared`** marker; it takes precedence via `or_else`. A global-decision key (`Committed`/`Aborted`, never `Prepared`) only sets `max_li`, NOT `first_undecided`/`gs`. So the **only** way the `GLOBAL_XID_BASE` bound moves `new_lo` is when there is NO in-doubt local marker (`first_undecided == None`, so `gs` is empty) and a high global-decision key would otherwise push `max_li` (and `new_lo = max_li+1`) into the global space. The teeth test MUST therefore use the no-in-doubt-marker case; a test that seeds an in-doubt local marker is **vacuous** (its `new_lo` is `first_undecided` regardless of the bound). Construct via `SqlEngine::with_kv(kv.clone()).expect("engine")` (`lib.rs:94`, sets `catalog_kv == kv`); seed via `kv.write_batch(&[mvcc::clog::put_op(li, status)]).expect(..)` (the sibling tests' idiom — there is NO `mvcc::clog::encode`; `put_op(xid, status) -> WriteOp` and `decode(&[u8])` are the only clog helpers).

- [ ] **Step 1: Write the failing watermark-teeth test.** In `crates/executor/src/lib.rs`'s `#[cfg(test)] mod tests` (mirror `in_doubt_globals_from_bounds_the_scan_and_advances_past_terminal`):

```rust
#[tokio::test]
async fn in_doubt_scan_watermark_stays_below_global_xid_base_on_range_0() {
    use mvcc::xid::GLOBAL_XID_BASE;
    let kv: std::sync::Arc<dyn kv::Kv> = std::sync::Arc::new(kv::MemKv::new());
    // A terminal LOCAL entry (so the scan has a local row) and a GLOBAL-decision entry keyed
    // high in the global-xid space (range 0 mixes participant markers with the global clog).
    // NO in-doubt local marker → first_undecided == None → the watermark is max_li+1, which the
    // bound must keep below GLOBAL_XID_BASE.
    kv.write_batch(&[
        mvcc::clog::put_op(5, mvcc::clog::XidStatus::Committed),
        mvcc::clog::put_op(GLOBAL_XID_BASE + 3, mvcc::clog::XidStatus::Committed),
    ]).expect("seed");
    let engine = SqlEngine::with_kv(kv.clone()).expect("engine"); // catalog_kv == kv (range-0 self)
    let (gs, new_lo) = engine.in_doubt_globals_from(0).await.expect("scan");
    assert!(gs.is_empty(), "no in-doubt markers → empty (got {gs:?})");
    assert!(
        new_lo < GLOBAL_XID_BASE,
        "the watermark never jumps into the global-xid space (got {new_lo})"
    );
}
```

- [ ] **Step 2: Run → FAIL.** `cargo nextest run -p executor in_doubt_scan_watermark_stays_below_global_xid_base_on_range_0`. Expected: FAIL — without the bound the scan iterates the `GLOBAL_XID_BASE+3` decision key, sets `max_li = GLOBAL_XID_BASE+3`, so `new_lo = GLOBAL_XID_BASE+4` (>= `GLOBAL_XID_BASE`), tripping the `new_lo < GLOBAL_XID_BASE` assert. (Confirm it actually fails before adding the bound — this is the teeth.)

- [ ] **Step 3: Bound both scans.** In `in_doubt_globals_from` (`lib.rs:277`) and `staged_local_for` (`lib.rs:327`), change the scan upper bound from `kv::key::clog_scan_end()` to `kv::key::clog_key(mvcc::xid::GLOBAL_XID_BASE)`:
```rust
            .scan_range(&kv::key::clog_key(scan_lo), &kv::key::clog_key(mvcc::xid::GLOBAL_XID_BASE))?
```
Add `use mvcc::xid::GLOBAL_XID_BASE;` at the top of the file or fully-qualify (it's already referenced in `lib.rs` comments). On a data range every clog xid is `< GLOBAL_XID_BASE`, so the result set is identical; on range 0 the global-decision entries (keyed `>= GLOBAL_XID_BASE`) are excluded.

- [ ] **Step 4: Run → PASS + add the gs-correctness test.** `cargo nextest run -p executor in_doubt_scan_watermark_stays_below_global_xid_base_on_range_0` → PASS. Then add a second test asserting the scan still returns the in-doubt LOCAL participant `g` (and never a global-decision xid):
```rust
#[tokio::test]
async fn in_doubt_scan_returns_only_local_participant_markers_on_range_0() {
    use mvcc::xid::GLOBAL_XID_BASE;
    let kv: std::sync::Arc<dyn kv::Kv> = std::sync::Arc::new(kv::MemKv::new());
    let g_indoubt = GLOBAL_XID_BASE + 7; // its decision is absent → in-doubt
    kv.write_batch(&[
        mvcc::clog::put_op(5, mvcc::clog::XidStatus::Prepared(g_indoubt)), // local participant marker
        mvcc::clog::put_op(GLOBAL_XID_BASE + 3, mvcc::clog::XidStatus::Committed), // a global decision
    ]).expect("seed");
    let engine = SqlEngine::with_kv(kv.clone()).expect("engine");
    let (gs, _new_lo) = engine.in_doubt_globals_from(0).await.expect("scan");
    assert_eq!(gs, vec![g_indoubt], "only the in-doubt local participant g is returned, never a global decision");
}
```
`cargo nextest run -p executor in_doubt_scan` → both pass. Then `cargo nextest run -p executor` → all green (the bound must not regress existing recovery/idempotency tests).

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
                // FAIL-CLOSED: like every step in this block, a reseed error aborts the settle so
                // the gate stays CLOSED and retries on the next wake — a silently-failed reseed must
                // NOT let `mark_served` open the gate against a possibly-regressed counter.
                engine.reseed_gtm().map_err(|_| ())?;
                engine.reseed_counters().map_err(|_| ())?;
```
Place this inside the `settled` async block (after `applied_index_at_least`, before `in_doubt_globals_from`), so a failed apply-wait skips it (the gate stays closed and retries — the SP22 retry-while-closed behavior). **Use `.map_err(|_| ())?` (the block's fail-closed pattern), NOT `.ok()`** (`.ok()` discards the error and falls through to `mark_served`, opening the gate against an un-reseeded counter — the exact reuse SP23 prevents). Both `reseed_gtm`/`reseed_counters` return `Result<(), ExecError>` (`lib.rs:148,233`). On a DATA-range engine `reseed_gtm` is a no-op (it guards on `self.gtm.is_some()`), so this is safe for every range's sweep.

- [ ] **Step 3: Register range 0 + spawn its sweep — BEFORE the listener serves `BeginGlobal` (the timing differs per path).** Range 0's gate entry MUST exist before any `BeginGlobal` RPC can be served on this node, or `is_serving(0)` returns the unregistered-default `true` (ungated) and a node that becomes range-0 leader in that window hands out a reused `g`. The spawn template (register strictly BEFORE the spawn — the SP22 wedge-prevention ordering):
```rust
        gate.register_range(0, rafts[&0].clone());
        tokio::spawn(resolve_in_doubt_on_leadership(
            rafts[&0].clone(), 0, cfg.id, engines[&0].clone(), <range0_sweep_client>, gate.clone(),
        ));
```
Range 0's sweep abort-races inherited `Prepared(L0 -> g)` markers via `client.call(0, CommitGlobal{g, false})` (write-once-safe; the scan is range-0-bounded by Task 1). Placement (call out the difference explicitly):
  - **`start_static`:** `rafts` is fully populated and the gate is built before the SQL gateway serves, so the existing late site (after `sweep_client`, before the data-range loop) is fine — but register range 0 BEFORE `serve_node_protocol`/the gateway spawn. Use `sweep_client.clone()`.
  - **`start_replicated` (the load-bearing fix — MUST-FIX from plan review):** the node listener that serves `BeginGlobal` is spawned at `serve_node_protocol` (~server_node.rs:473), but the Phase-2 all-ranges `sweep_client` isn't built until ~:522 — leaving `BeginGlobal` UNGATED from :473 to :522 (a real reuse window on a Phase-2 range-0 leader). **Register range 0 + spawn its sweep right after `r0_engine` is `Arc`-wrapped (~:456) and BEFORE `serve_node_protocol` (~:473).** Range 0's sweep only resolves range 0 (self-loopback abort-races), so build a **range-0-only** `TwoPcClient` there — `crate::twopc::TwoPcClient::new(<a rafts map containing only {0: r0_raft}>, partition.clone())` — rather than waiting for the all-ranges `sweep_client`. (Confirm `TwoPcClient::new(rafts, partition)`'s real signature; a single-entry map is valid.)
  The probe (`sp23-range0-probe`) validated the registration + sweep compile + run on the static path; the replicated early-registration is the new requirement this must-fix adds.

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
**The test calls a `handle_txn_for_test` shim — add it.** `handle_txn` is a private free fn with the REAL signature `async fn handle_txn(registry: &RangeRegistry, svc: &TxnService, range: RangeId, rpc: TxnRpc) -> TxnResp` (4 params, `registry` FIRST). The `BeginGlobal` arm never consults `registry`, so an empty one is safe. Add to `transport/server.rs`:
```rust
#[cfg(test)]
pub(crate) async fn handle_txn_for_test(svc: &crate::twopc::TxnService, range: RangeId, rpc: TxnRpc) -> TxnResp {
    handle_txn(&RangeRegistry::new(), svc, range, rpc).await
}
```
(`RangeRegistry::new()` is reachable; confirm the real `handle_txn` signature + the `RangeRegistry`/`TxnRpc`/`TxnResp` import paths before writing it.) Place the new `is_serving(0)` gate check at the very TOP of the `BeginGlobal` arm (Step 4) so the registry is irrelevant to the gated path.

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

Properties (`always`) — **MUST be STATE predicates** (`stateright::Property::always` evaluates over STATES, not transitions; a transition-time "the `g` this `Alloc` produced wasn't already in `allocated`" check is INVALID — by the time a state is observed, inserting an already-present `g` into the set is a no-op and the duplicate is invisible, so the teeth would not fire). So model it as row VERSIONS: every `Alloc` stages a row version keyed by its `g`, and a reused `g` produces TWO versions that both go live.
- **(teeth invariant — load-bearing)** `"at most one live version per row"` — `live_count(row) <= 1` over the modeled versions (mirrors `crossrange_2pc_settle_model.rs`'s `live_count() <= 1`). A reused `g` (gate off) yields two live versions → violated. This is the property the teeth test asserts is in `discoveries()`.
- corroborating (optional): `"no two live versions share a creator g"`.
("no global xid is allocated twice" is the MECHANISM described in prose, NOT the checker property.)

- [ ] **Step 2: Positive test.** `reseed_before_alloc: true` → `checker.assert_properties()` + `unique_state_count() > 1`. No property weakened.

- [ ] **Step 3: Teeth test.** `reseed_before_alloc: false` → `!discoveries().is_empty()` AND `discoveries().contains_key("at most one live version per row")`. The checker MUST find the duplicate (the reused `g` stages a second live version) when allocation isn't gated on reseed.

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

- [ ] **Step 2: Run it 3× — the GOAL is 3× non-flaky; the FALLBACK is documented (do NOT block the slice on a possibly-residual e2e).** `cargo nextest run -p crabgresql --test range0_leader_kill_drain` three times. **If it PASSES 3×:** done — criterion 4 met end-to-end. **If it still tears/wedges:** the g-reuse fix is proven by the model + unit/gate teeth regardless; the residual is the spec's explicitly-deferred cascading-failover gap (a NEW finding beyond the now-fixed g-reuse, NOT a reason to weaken any assert). In that case: capture the exact mechanism (got-vs-want, whether the 2-live `debug_assert` fired, correlation with overlapping failovers), commit the nemesis `#[ignore = "residual cascading-failover gap — see SP23 spec non-goal; reproduce with --ignored"]` with a doc comment describing the repro + mechanism, scope the residual to a follow-up slice (mirroring how SP22 deferred its participant-leader-kill nemesis), and let T6 FINISH on the converged portion (model + unit/gate teeth + the existing regression suites). Either way, do NOT weaken the at-most-one-live `debug_assert` or the conservation oracle.

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
| 5 | Range-0-safe scan; recovery paths ungated; suites unchanged | `executor` `in_doubt_scan_watermark_stays_below_global_xid_base_on_range_0` + `in_doubt_scan_returns_only_local_participant_markers_on_range_0` + regression |
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

**Type consistency:** `TxnService::is_serving(range) -> bool` (T3) uses `is_none_or` consistently with SP22's `is_some_and`. `RecoveryGate::{is_serving,mark_served,register_range}` used per SP22. `reseed_gtm`/`reseed_counters` (engine, both return `Result<(), ExecError>`) are used **fail-closed** via `.map_err(|_| ())?` in the sweep's `settled` block — NOT `.ok()` (must-fix #3). The scan bound `clog_key(GLOBAL_XID_BASE)` used identically in both scan sites. `resolve_in_doubt_on_leadership(raft, range, id, engine, client, gate)` signature matches SP22's T3.

**Known risks folded (incl. plan-review):** the GTM-not-gated requirement (only BeginGlobal; CommitGlobal/Barrier/Release/Stage's recovery use stays ungated); the register-before-spawn ordering (T2 Step 3); the reseed inside the gate-opening sweep (structural reseed-before-open) with FAIL-CLOSED `?` (must-fix #3); the **replicated-path early registration** before the listener serves BeginGlobal (must-fix #4); the range-0 mixed-clog scan bound + **non-vacuous watermark teeth** (T1, must-fix #1/#2); the full-drain nemesis with a documented `#[ignore]` fallback (should-fix). 

**Pre-existing `reseed_on_leadership` retained (should-fix C):** both bring-up paths ALREADY spawn `reseed_on_leadership` for range 0 (the un-apply-waited rising-edge `reseed_gtm` at server_node.rs:768-769 — the bug). SP23 INTENTIONALLY keeps it (it also reseeds the local procarray/seq counters; do NOT special-case range 0 out of it). It is harmless: `Gtm::reseed_from_applied` is lift-only (`max`, never regresses), and `begin_global` is gated behind `mark_served`, which fires only AFTER the apply-waited sweep reseed — so no allocation can observe a stale-but-lifted counter. Optionally add a comment at server_node.rs:768 noting the rising-edge range-0 reseed is now superseded by the gate-opening sweep and retained only as a harmless lift.

**Second allocation path noted (nit):** `begin_global` has two non-test callers — `handle_txn`'s `BeginGlobal` (server.rs:243, the networked `NetCoordinator` path, NOW GATED) and the in-process `LocalCoordinator::begin_global` (router.rs:62, calls `engine.begin_global_durable()` directly, NOT gated). This is acceptable: the in-process `MultiRangeCluster` wires a `None` gate and never kills the range-0 leader to regress the counter; the multi-process nemesis (the only path that regresses) uses the gated `NetCoordinator → handle_txn`. T3/the spec note that only the networked allocation path is gated.
