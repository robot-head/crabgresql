# SP22 / D3c settle-before-serve Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A newly-risen range leader does not serve *writes* for that range until its leadership-rise in-doubt sweep (preceded by an apply-wait) has settled every inherited 2PC `Prepared(-> g)` marker — eliminating the killed-participant-leader duplicate-version window.

**Architecture:** A new per-range, term-based `RecoveryGate` (cluster) holds each range's Raft handle + an `Arc<AtomicU64> served_term`. A write to range R is admitted only when this node leads R *and* `served_term[R] == R's current Raft term`. The rise sweep apply-waits, settles, then `mark_served(R, term)`. The gate is checked at the two cluster write entry points: `TxnService::stage` and the router's local-led write path. Reads are never gated.

**Tech Stack:** Rust 2024, openraft 0.9.24, arc-swap, tokio, cargo-nextest, stateright. **No new dependency.**

**Stacked on SP21** (branch `sp22-d3c-settle-before-serve` off the SP21 branch). Rebase `--onto origin/main` once SP21 (PR #37) squash-merges.

---

## Background the implementer needs (read once)

**The bug** (deferred from SP21; see the SP22 design spec). A killed participant-range leader leaves a durable `Prepared(Li -> g)` marker on the new leader with **no in-memory lock** (locks aren't replicated). A writer touching that row before the marker settles reads `g` as in-doubt, supersedes the *older* version, and writes a SECOND non-superseding version → two MVCC versions go live when both `g`s commit → `scan_live`'s at-most-one-live `debug_assert!` (SP21) crashes every reader, or the bank total tears.

**Why a term-based gate (not a flag).** A flag set on the rising edge has a microsecond race between the metrics flipping to leader and the watcher setting the flag — the exact race class that made SP21's incremental patches fail. Deriving the gate from `served_term == current Raft term` is atomic with leadership: a node that won an election is at term ≥ 1, and `served_term` starts at the sentinel `0`, so a range is **gated by default** on every fresh rise until its sweep opens it.

**openraft 0.9.24 API (CONFIRMED against the vendored crate):**
- Leadership term: `raft.metrics().borrow().current_term` (a `u64`). Do NOT use `vote.leader_id().term`.
- Leader check: `raft.metrics().borrow().current_leader == Some(id)`.
- `m.committed` does NOT exist. For the apply-wait *target* use `raft.ensure_linearizable().await` → `Result<Option<LogId<NodeId>>, RaftError<…>>`; `.map(|r| r.map(|l| l.index))` is the committed index for this leadership term (the blank no-op). Do NOT use `m.last_applied.index` (waiting for `applied >= applied` is a no-op and reintroduces the apply-lag miss).
- Apply-wait: `raft.wait(Some(timeout)).applied_index_at_least(Some(idx), "reason").await`.
- **Borrow rule:** `raft.metrics().borrow()` yields a `watch::Ref` that MUST be dropped (scoped `{ }`) before any `.await` — a `Ref` held across await deadlocks the watch channel.

**Clippy:** the workspace denies warnings and rejects `map_or(true|false, …)` (use `is_some_and` / `is_none_or`). `#![forbid(unsafe_code)]`. No `sleep`-to-settle in tests (CLAUDE.md); the multi-process harness's bounded 100ms poll cadence is allowed.

---

## File structure

| File | Change | Responsibility |
|---|---|---|
| `crates/cluster/src/recovery_gate.rs` | **Create** | `RecoveryGate` (growable per-range `served_term` + `is_serving`/`mark_served`/`register_range`) + unit tests |
| `crates/cluster/src/lib.rs` | Modify | `pub mod recovery_gate; pub use recovery_gate::RecoveryGate;` |
| `crates/cluster/src/twopc.rs` | Modify | `TxnService` gains `gate` field; `stage` gains the gate check |
| `crates/cluster/src/range/router.rs` | Modify | `RangeRouter` gains `gate` field; `dispatch` gains the local-led-write gate check |
| `crates/cluster/src/range/cluster.rs` | Modify | add `MultiRangeCluster::leader_raft` accessor (T4 router-test teeth) |
| `crates/cluster/src/route.rs` | Modify | `RangeGatewayEngine` + `serve_range_routed` thread the gate into the router |
| `crates/cluster/src/server_node.rs` | Modify | construct + register the gate; rise-sweep apply-wait + `mark_served`; thread the gate (both bring-up paths) |
| `crates/cluster/tests/crossrange_2pc_settle_model.rs` | **Create** | Stateright settle-before-serve model (teeth + positive) |
| `crates/crabgresql/tests/crossrange_2pc_restage.rs` | **Create** | multi-process participant-leader-kill nemesis (UAC-safe) |
| `CLAUDE.md` | Modify | SP22 audit line |

**Wiring order (each task leaves the tree green):** T1 builds the gate (standalone, unit-tested). T2 *threads the gate field* everywhere + constructs/registers it — but does NOT check it, so behavior is unchanged. T3 makes the rise sweep *open* the gate (`mark_served`) — still no check, so unchanged. T4 adds the *checks* — now enforced, and (because T3 opens it on rise) normal writes pass after a brief settle. T5/T6 are the model + nemesis; T7 finishes.

---

## Task 1: `RecoveryGate` struct + unit tests

**Files:**
- Create: `crates/cluster/src/recovery_gate.rs`
- Modify: `crates/cluster/src/lib.rs`

- [ ] **Step 1: Register the module.** In `crates/cluster/src/lib.rs`, add `pub mod recovery_gate;` (alphabetically, near `pub mod range;`/`pub mod route;`) and, after `pub use linearizer::RaftLinearizer;`, add `pub use recovery_gate::RecoveryGate;`.

- [ ] **Step 2: Write the gate + its unit tests.** Create `crates/cluster/src/recovery_gate.rs`:

```rust
//! Per-range, term-based recovery gate for settle-before-serve (SP22). A newly-risen
//! range leader must not serve WRITES for that range until its leadership-rise in-doubt
//! sweep has settled every inherited `Prepared(-> g)` marker. A write to range R is
//! admitted only when this node leads R AND R's last-settled term equals R's CURRENT Raft
//! term — derived atomically from the term, so there is no rise-edge race. `served_term`
//! starts at the sentinel 0; a node that won an election is at term >= 1, so a range is
//! gated-by-default on every fresh rise until its sweep calls `mark_served`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;

use crate::range::RangeId;
use crate::types::{NodeId, TypeConfig};

/// Sentinel below any real Raft leadership term (a won election is term >= 1).
const UNSETTLED: u64 = 0;

pub struct RecoveryGate {
    /// Per range: (raft handle, last term whose rise sweep completed). Growable via
    /// `register_range` (copy-on-write) so the replicated bring-up — which constructs the
    /// gate before its data ranges exist — can add ranges as their Raft groups come up,
    /// exactly like `TxnService`'s engines map.
    ranges: ArcSwap<HashMap<RangeId, (openraft::Raft<TypeConfig>, Arc<AtomicU64>)>>,
    id: NodeId,
}

impl RecoveryGate {
    pub fn new(id: NodeId) -> Arc<Self> {
        Arc::new(Self {
            ranges: ArcSwap::from_pointee(HashMap::new()),
            id,
        })
    }

    /// Register a range's Raft handle (gated-by-default). Idempotent: re-registering keeps
    /// the existing `served_term` Arc. Copy-on-write so lock-free `is_serving` readers never
    /// block.
    pub fn register_range(&self, range: RangeId, raft: openraft::Raft<TypeConfig>) {
        self.ranges.rcu(|cur| {
            let mut m = (**cur).clone();
            m.entry(range)
                .or_insert_with(|| (raft.clone(), Arc::new(AtomicU64::new(UNSETTLED))));
            m
        });
    }

    /// True iff this node currently leads `range` AND its rise sweep has settled the current
    /// term. A range not registered here is "not this node's concern" → `true` (such a write
    /// rejects via the normal not-local-leader path instead). Re-reads the LIVE term every
    /// call so a leadership flap re-closes the gate until the new term is settled.
    pub fn is_serving(&self, range: RangeId) -> bool {
        let ranges = self.ranges.load();
        let Some((raft, served)) = ranges.get(&range) else {
            return true;
        };
        let (leader, term) = {
            let m = raft.metrics();
            let m = m.borrow();
            (m.current_leader, m.current_term)
        };
        leader == Some(self.id) && served.load(Ordering::Acquire) == term
    }

    /// Open the gate for `range` at `term` — called by the rise sweep AFTER it has settled
    /// every inherited in-doubt marker for `term`. A no-op for an unregistered range.
    pub fn mark_served(&self, range: RangeId, term: u64) {
        if let Some((_, served)) = self.ranges.load().get(&range) {
            served.store(term, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn gate_is_closed_at_a_fresh_term_and_opens_on_mark_served() {
        // A single-node 2-range ServerNode: this node leads every range at a stable term >= 1.
        let (node, _sql) = crate::server_node::testonly_two_range_node().await;
        let term = node.rafts[&1].metrics().borrow().current_term;
        assert!(term >= 1, "a leader is at term >= 1");

        let gate = RecoveryGate::new(node.id());
        gate.register_range(1, node.rafts[&1].clone());

        // Gated-by-default: the sentinel served_term (0) != the live term, even though we lead.
        assert!(
            !gate.is_serving(1),
            "a freshly-registered range is gated until its rise sweep settles the term"
        );
        // The rise sweep opens it for this term.
        gate.mark_served(1, term);
        assert!(gate.is_serving(1), "writes are admitted once the term is settled");

        // An unregistered range is not this gate's concern.
        assert!(gate.is_serving(999), "a non-hosted range is not gated here");
    }
}
```

(`node.id()` — `ServerNode` exposes `pub fn id(&self) -> NodeId` returning `cfg.id`; `testonly_two_range_node` always boots id `0`, so the literal `0` is a safe fallback if `id()` is missing. `node.rafts` is `pub`.)

- [ ] **Step 3: Run the gate test.**

Run: `cargo test -p cluster --lib recovery_gate`
Expected: PASS — gated at the fresh term, serving after `mark_served`, ungated for a non-hosted range.

- [ ] **Step 4: Clippy + commit.**

Run: `cargo clippy -p cluster --all-targets -- -D warnings` → clean.
```bash
git add crates/cluster/src/recovery_gate.rs crates/cluster/src/lib.rs
git commit -m "feat(sp22): RecoveryGate — per-range term-based settle-before-serve gate

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Thread the gate everywhere (construct, register, hold — NOT checked yet)

This task gives `TxnService` and the router a `gate: Option<Arc<RecoveryGate>>` field, and makes `server_node` construct ONE gate, register each range into it, and pass that one `Arc` into `TxnService`, the rise-sweep spawns, and the gateway router. **No gate is checked yet**, so behavior is unchanged and all existing tests stay green. The single-shared-`Arc` discipline is the load-bearing requirement (a second gate would never see the sweep's `mark_served`).

**Files:** `crates/cluster/src/twopc.rs`, `crates/cluster/src/range/router.rs`, `crates/cluster/src/route.rs`, `crates/cluster/src/server_node.rs`

- [ ] **Step 1: `TxnService` gets the field.** In `crates/cluster/src/twopc.rs`, the struct (currently):

```rust
#[derive(Clone)]
pub struct TxnService {
    engines: Arc<ArcSwap<HashMap<RangeId, Arc<SqlEngine>>>>,
    held: Arc<Mutex<HashMap<(u64, RangeId), HeldEntry>>>,
}

impl TxnService {
    pub fn new(engines: HashMap<RangeId, Arc<SqlEngine>>) -> Self {
        Self {
            engines: Arc::new(ArcSwap::from_pointee(engines)),
            held: Arc::new(Mutex::new(HashMap::new())),
        }
    }
```

becomes (add the field + param; `Option<Arc<…>>` clones by Arc so all `#[derive(Clone)]` copies share one gate):

```rust
#[derive(Clone)]
pub struct TxnService {
    engines: Arc<ArcSwap<HashMap<RangeId, Arc<SqlEngine>>>>,
    held: Arc<Mutex<HashMap<(u64, RangeId), HeldEntry>>>,
    /// Settle-before-serve gate (SP22): `Some` on a real node, `None` for in-process /
    /// never-recovering test harnesses (treated as always-serving).
    // `#[allow(dead_code)]` until Task 4 reads it in the `stage` gate check. REMOVE the
    // allow in Task 4. (`#[derive(Clone)]` does NOT suppress this lint — rustc explicitly
    // ignores derived `Clone` in dead-code analysis, so a written-but-never-read field is a
    // hard `-D warnings` error.)
    #[allow(dead_code)]
    gate: Option<Arc<crate::recovery_gate::RecoveryGate>>,
}

impl TxnService {
    pub fn new(
        engines: HashMap<RangeId, Arc<SqlEngine>>,
        gate: Option<Arc<crate::recovery_gate::RecoveryGate>>,
    ) -> Self {
        Self {
            engines: Arc::new(ArcSwap::from_pointee(engines)),
            held: Arc::new(Mutex::new(HashMap::new())),
            gate,
        }
    }
```

- [ ] **Step 2: Fix the `TxnService::new` callers (pass `None`).** Grep the WHOLE crate: `grep -rn "TxnService::new" crates/cluster` — the `#[cfg(test)]` callers in `twopc.rs` (around :542, 602, 712, 768, 852, 877) and ANY in `crates/cluster/tests/*` become `TxnService::new(node.engines.clone(), None)` / `TxnService::new(only0, None)`. (server_node's two production callers are handled in Step 6.)

- [ ] **Step 3: `RangeRouter` gets the field.** In `crates/cluster/src/range/router.rs`, add a field after `leads`:

```rust
    leads: Arc<dyn LeadsRange>,
    /// Settle-before-serve gate (SP22): `None` for the in-process harness (always serving).
    // `#[allow(dead_code)]` until Task 4 reads it in the `dispatch` write check; REMOVE then.
    #[allow(dead_code)]
    gate: Option<Arc<crate::recovery_gate::RecoveryGate>>,
```

and add a trailing param to the single `RangeRouter::new` constructor + the `Self { … }` literal:

```rust
    pub fn new(
        map: RangeMap,
        engines: HashMap<RangeId, SqlEngine>,
        leads: Arc<dyn LeadsRange>,
        catalog_kv: Arc<dyn kv::Kv>,
        forward: Arc<dyn RemoteForward>,
        coordinator: Option<Arc<dyn GlobalCoordinator>>,
        gate: Option<Arc<crate::recovery_gate::RecoveryGate>>,
    ) -> Self {
        Self {
            sessions: HashMap::new(),
            pin: Pin::None,
            map, engines, leads, catalog_kv, forward, coordinator, gate,
            cur_sql: String::new(),
            #[cfg(test)]
            before_global_decision: None,
        }
    }
```

- [ ] **Step 4: Fix ALL `RangeRouter::new` callers (append a NEW trailing `None` gate arg).** Grep the WHOLE crate: `grep -rn "RangeRouter::new" crates/cluster`. There are exactly four non-definition callers; each gains a SECOND trailing `None` (a new last arg AFTER the existing coordinator `None` — do not repurpose the coordinator slot):
  - `crates/cluster/src/route.rs:170` (production) — handled in Step 5 (pass `self.gate.clone()`).
  - `crates/cluster/src/range/router.rs:268` (in-process `connect`) — trailing `None`.
  - `crates/cluster/src/range/router.rs:~1012` and `~1086` (the two `#[cfg(test)]` seam tests) — trailing `None`.
  - `crates/cluster/tests/jepsen_bank.rs:1021` (the `router_over` integration-test helper) — trailing `None`. **This `tests/` caller is NOT under `src`; it only surfaces when `cargo nextest run -p cluster` builds the integration-test binaries, so add it now or T2 Step 7 fails to build.** Its `AlwaysLeads`/`RejectForward` in-process router never recovers → `None` (always-serving) is correct.

- [ ] **Step 5: Thread the gate through `route.rs`.** In `crates/cluster/src/route.rs`, `RangeGatewayEngine` gains a `gate: Option<Arc<crate::recovery_gate::RecoveryGate>>` field (next to `coordinator`), `RangeGatewayEngine::new` gains a matching trailing param, and `Engine::connect` forwards `self.gate.clone()` as the new trailing `RangeRouter::new` arg. `serve_range_routed` (already `#[allow(clippy::too_many_arguments)]`) gains a trailing `gate: Option<Arc<crate::recovery_gate::RecoveryGate>>` param and forwards it into `RangeGatewayEngine::new`. (Read the live `serve_range_routed`/`RangeGatewayEngine::new` signatures and place the new param last.)

- [ ] **Step 6: `server_node` constructs + registers + threads the gate (BOTH paths).**

In `crates/cluster/src/server_node.rs`:

**Static path (`start_static`).** Construct the gate ONCE, right after the `sweep_client` `TwoPcClient::new` (server_node.rs:242) and BEFORE the `for (range, raft, sm_kv, mut engine) in pending` loop (:243):
```rust
        let gate = crate::recovery_gate::RecoveryGate::new(cfg.id);
```
Then, **inside** that pending loop, register each range — and this ordering is **load-bearing**:

> `gate.register_range(range, raft.clone());` MUST execute **strictly BEFORE** the `tokio::spawn(resolve_in_doubt_on_leadership(...))` (server_node.rs:252) in the same loop iteration.

(Tests run on a multi-thread tokio runtime and these single-node ranges elect almost immediately, so the spawned sweep can run on another worker and hit its rising edge before `register_range`. `mark_served` on an unregistered range is a no-op, `register_range` then creates a fresh sentinel-`0` entry, and on a stable single leader there is NO second rising edge — so a register-after-spawn race would wedge the gate closed forever. Registering first guarantees the very first sweep iteration sees a registered, gated-by-default range.) Then:
- `TxnService::new(engines.clone())` (server_node.rs:263) → `TxnService::new(engines.clone(), Some(gate.clone()))`.
- **Leave the `resolve_in_doubt_on_leadership` spawn call UNCHANGED in this task** (it stays the current 4-arg `(raft.clone(), cfg.id, engine.clone(), sweep_client.clone())`). Task 3 changes that function's signature to take `range` + `gate` and updates this spawn site. (The rise sweep doesn't touch the gate until Task 3, so T2 is behavior-neutral.)
- **`spawn_sql` gets the gate param too** (it is a separate wrapper from `spawn_sql_gateway`): add a trailing `gate: Option<Arc<crate::recovery_gate::RecoveryGate>>` to `spawn_sql` (server_node.rs:328, already `#[allow(clippy::too_many_arguments)]`); at its call site (server_node.rs:302) pass `Some(gate.clone())`; inside `spawn_sql`, forward it to `spawn_sql_gateway` on the `range_count() > 1` branch and pass **`None`** on the single-range `serve_routed` branch (no gateway router → ungated). `spawn_sql_gateway` and `serve_range_routed` likewise gain a trailing `Option<Arc<…>>` param (forwarded into `RangeGatewayEngine::new` per Step 5).

**Replicated path (`start_replicated`).** Construct the gate BEFORE `TxnService::new` (which runs at ~server_node.rs:438 with only range 0 in `rafts`): `let gate = crate::recovery_gate::RecoveryGate::new(cfg.id);` then `TxnService::new(engines.clone(), Some(gate.clone()))`. Because `RecoveryGate` is growable, register each data range inside the Phase-2 pending loop — and as in the static path, `gate.register_range(range, raft.clone());` MUST run **before** both `txn.register_engine(range, …)` and the `resolve_in_doubt_on_leadership` spawn for that range. (The node listener is already accepting `Stage` RPCs from ~server_node.rs:440, so registering the gate first makes the range gated-by-default the instant it becomes servable — no one-iteration ungated window, and the first sweep iteration sees a registered range.) Pass `Some(gate.clone())` into `spawn_sql_gateway`. (The rise-sweep spawn in this loop is updated in Task 3.)

`cfg.id` (`NodeId`, `Copy`) is available at every site. **Build the gate ONCE and pass the SAME `Arc` everywhere** — do not construct a second gate inside `spawn_sql_gateway`.

- [ ] **Step 7: Build + run the full cluster suite (no behavior change).**

Run: `cargo build -p cluster` → compiles (the compiler flags any missed `TxnService::new`/`RangeRouter::new` call site; fix each with a trailing `None`/`Some(gate)`).
Run: `cargo nextest run -p cluster` → PASS (the gate is held but never checked — zero behavior change).
Run: `cargo clippy -p cluster --all-targets -- -D warnings` → clean. `cargo fmt -p cluster`.

- [ ] **Step 8: Commit.**
```bash
git add crates/cluster/src/twopc.rs crates/cluster/src/range/router.rs crates/cluster/src/route.rs crates/cluster/src/server_node.rs
git commit -m "feat(sp22): thread the RecoveryGate through TxnService, the router, and both bring-up paths

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Rise sweep apply-wait + `mark_served` (opens the gate)

The rise sweep now (1) waits until this node has applied through its committed index — so the durable clog scan sees every inherited marker (closing the apply-lag miss) — and (2) calls `gate.mark_served(range, term)` after settling. Still no write check, so behavior is unchanged except that the gate now *opens* on each rise.

**Files:** `crates/cluster/src/server_node.rs`

- [ ] **Step 1: Add the settle constants.** Near the top of `server_node.rs` (or reuse the pattern of `twopc::TXN_TIMEOUT = Duration::from_secs(10)`):
```rust
/// Bound for the settle-before-serve apply-wait (and the linearizable read that derives its
/// target) on a leadership rise. On timeout the gate stays CLOSED (writes keep getting a
/// retryable NotLeader) and the sweep RE-TRIES on the next wake — never open the gate after a
/// failed settle.
const SETTLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Retry cadence for a CLOSED gate under continuous leadership. The rise sweep normally wakes
/// on a metrics change, but a FAILED settle may leave no further metrics change to wake it; this
/// caps the wait so a wedged-closed gate keeps retrying its settle. A recovery heartbeat (cf.
/// `participant_silence_sweeper`'s 500ms tick), NOT a settle-sleep.
const SETTLE_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);
```

- [ ] **Step 2: Change the rise-sweep signature + body.** `resolve_in_doubt_on_leadership` gains `range: RangeId` and `gate: Arc<crate::recovery_gate::RecoveryGate>` (non-`Option` — both spawn sites always have the gate). Replace the whole loop so it re-fires the settle **whenever we lead `range` but the gate is still closed for the current term** (not only on the rising edge) and opens the gate ONLY in the `Ok` arm. **Why not rise-edge-only (`is_leader && !was_leader`):** if a settle FAILS (quorum blip / `SETTLE_TIMEOUT`), `mark_served` is skipped and the gate stays CLOSED; under *continuous* leadership there is no second rising edge, so a `!was_leader` trigger would never retry and the gate wedges closed forever — every write to `range` returns NotLeader/40001 indefinitely (and the one-shot T6 workload, which does not retry a gated 40001, then deadlocks). Re-firing on `!gate.is_serving(range)` (which re-reads the live term) covers both a fresh rise *and* a failed prior settle. Both `ensure_linearizable` and the apply-wait are bounded by `SETTLE_TIMEOUT` so a win-then-lose-quorum flap cannot freeze the task before it loops back to the wake:

```rust
async fn resolve_in_doubt_on_leadership(
    raft: openraft::Raft<TypeConfig>,
    range: RangeId,
    id: NodeId,
    engine: Arc<SqlEngine>,
    client: std::sync::Arc<crate::twopc::TwoPcClient>,
    gate: Arc<crate::recovery_gate::RecoveryGate>,
) {
    use crate::transport::protocol::TxnRpc;
    let mut rx = raft.metrics();
    loop {
        // Re-fire while we lead `range` but its gate is still closed for the CURRENT term —
        // covers a fresh rise (sentinel != term) AND a FAILED prior settle (a wedged gate must
        // keep retrying under continuous leadership, or it deadlocks every write to `range`).
        // `is_serving` re-reads the live term, so a flap to a new term re-closes + re-settles.
        let is_leader = rx.borrow().current_leader == Some(id);
        if is_leader && !gate.is_serving(range) {
            // The leadership term we are settling (read + drop the Ref before awaiting).
            let term = { rx.borrow().current_term };
            // Apply-wait: settle through this term's committed index so the durable clog
            // scan below sees EVERY inherited marker (closes the apply-lag miss). The
            // `ensure_linearizable` index is the committed no-op for this term (the
            // Range0Barrier idiom). BOTH the linearizable read and the apply-wait are bounded
            // by SETTLE_TIMEOUT — a node that wins then loses quorum must not freeze here.
            // On any error / timeout, leave the gate CLOSED and retry on the next wake — never
            // open it after a failed settle.
            let settled = async {
                let wait_to = tokio::time::timeout(SETTLE_TIMEOUT, raft.ensure_linearizable())
                    .await
                    .map_err(|_| ())? // timed out
                    .map_err(|_| ())? // RaftError
                    .map(|l| l.index);
                raft.wait(Some(SETTLE_TIMEOUT))
                    .applied_index_at_least(wait_to, "settle-before-serve")
                    .await
                    .map_err(|_| ())?;
                let scan_lo = engine.clog_scan_lo().unwrap_or(0);
                let (gs, new_lo) = engine.in_doubt_globals_from(scan_lo).await.map_err(|_| ())?;
                for g in gs {
                    if let Err(e) = client
                        .call(0, TxnRpc::CommitGlobal { g, commit: false })
                        .await
                    {
                        tracing::warn!(g, ?e, "recovery abort-race failed; g stays in-doubt");
                    }
                }
                if let Err(e) = engine.advance_clog_scan_lo(new_lo).await {
                    tracing::debug!(new_lo, ?e, "watermark advance not durable; safe to re-scan");
                }
                Ok::<(), ()>(())
            }
            .await;
            if settled.is_ok() {
                // Every inherited marker for `term` is now terminal → open the gate.
                gate.mark_served(range, term);
            }
        }
        // Wake on the next metrics change, but cap the wait so a FAILED settle is retried even
        // when no further metrics change arrives. A dropped sender (raft shutdown) ends the
        // task. The cap is a recovery heartbeat (cf. `participant_silence_sweeper`), not a
        // settle-sleep — a successful settle leaves `is_serving` true so the body no-ops.
        if let Ok(Err(_)) = tokio::time::timeout(SETTLE_RETRY_INTERVAL, rx.changed()).await {
            return; // metrics sender dropped → raft gone
        }
    }
}
```

(If `ensure_linearizable` proves too strict in practice, the fallback `wait_to` is `{ rx.borrow().last_log_index }` read in the same scoped borrow as `term` — but prefer `ensure_linearizable` per the anchor map. Note: `is_serving(range)` requires `range` to be registered in the gate; T2 Step 6 pins `register_range` strictly BEFORE this spawn so the very first iteration sees a registered, closed gate.)

- [ ] **Step 3: Update the two spawn sites.** In both `start_static` and `start_replicated`, the `resolve_in_doubt_on_leadership` spawn (currently `(raft.clone(), cfg.id, engine.clone(), sweep_client.clone())`) becomes `(raft.clone(), range, cfg.id, engine.clone(), sweep_client.clone(), gate.clone())`. (`range` is the loop variable; `gate` is from Task 2.)

- [ ] **Step 4: Build + run the cluster suite + the existing multi-process nemeses.**

Run: `cargo build -p cluster` → compiles.
Run: `cargo nextest run -p cluster` → PASS (gate opens on rise but is still unchecked → no behavior change).
Run: `cargo nextest run -p crabgresql --test crossrange_2pc_nemesis --test crossrange_2pc_replicated` → PASS (no regression; the sweep still settles + now also opens the gate).
Run: `cargo clippy -p cluster --all-targets -- -D warnings` → clean. `cargo fmt -p cluster`.

- [ ] **Step 5: Commit.**
```bash
git add crates/cluster/src/server_node.rs
git commit -m "feat(sp22): rise sweep apply-waits then opens the recovery gate (mark_served)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: The two gate checks (enforce settle-before-serve)

Now the gate is enforced: a write to an unsettled range gets a retryable `NotLeader`/40001 and retries until the rise sweep (Task 3) opens the gate. Reads pass.

**Files:** `crates/cluster/src/twopc.rs`, `crates/cluster/src/range/router.rs`

- [ ] **Step 1: Write the failing `TxnService::stage` gate test.** In `twopc.rs`'s `#[cfg(test)] mod tests` (mirror `stage_then_release_holds_then_frees_a_per_g_session`):

```rust
/// SP22: a participant Stage to a range whose rise sweep has not settled the current term is
/// rejected (retryable NotLeader); once the gate is opened for the term, the stage proceeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stage_is_gated_until_the_range_is_settled() {
    let (node, _sql) = crate::server_node::testonly_two_range_node().await;
    let gate = crate::recovery_gate::RecoveryGate::new(node.id());
    gate.register_range(1, node.rafts[&1].clone());
    let svc = TxnService::new(node.engines.clone(), Some(gate.clone()));

    // DDL so table b (id 2) lands in range 1, seed a row (these go through the engines directly,
    // not the gated stage path).
    let mut ddl = node.engines[&0].connect();
    ddl.run(&parse_one("CREATE TABLE _placeholder (id int4)")).await.expect("placeholder");
    ddl.run(&parse_one("CREATE TABLE b (id int4)")).await.expect("b");
    let mut seed = node.engines[&1].connect();
    seed.run(&parse_one("INSERT INTO b VALUES (20)")).await.expect("seed");
    let g = node.engines[&0].begin_global_durable().await.expect("g");

    // Gate closed (sentinel term) → Stage is rejected, retryable.
    assert!(matches!(
        svc.handle(1, TxnRpc::Stage { g, range: 1, sql: "UPDATE b SET id = 21 WHERE id = 20".into() }).await,
        TxnResp::NotLeader
    ), "a stage to an unsettled range is gated (retryable)");

    // Open the gate for the current term → Stage proceeds.
    let term = node.rafts[&1].metrics().borrow().current_term;
    gate.mark_served(1, term);
    assert!(matches!(
        svc.handle(1, TxnRpc::Stage { g, range: 1, sql: "UPDATE b SET id = 21 WHERE id = 20".into() }).await,
        TxnResp::Staged
    ), "after the gate opens, the stage proceeds");
}
```

- [ ] **Step 2: Run → FAIL** (the stage is not yet gated, so the first assertion fails).
Run: `cargo nextest run -p cluster stage_is_gated_until_the_range_is_settled`

- [ ] **Step 3: Add the `stage` check** (and REMOVE the now-stale allow). In `TxnService::stage`, immediately AFTER the engine-present check (`if self.engine(range).is_none() { return TxnResp::NotLeader; }`) and BEFORE the idempotency block:
```rust
        // SP22 settle-before-serve: reject a stage to a range whose rise sweep has not yet
        // settled the current term (retryable — the coordinator re-resolves + retries on
        // NotLeader). Placed before the idempotency clog read, which is unsafe on an
        // unsettled range.
        if self.gate.as_ref().is_some_and(|g_| !g_.is_serving(range)) {
            return TxnResp::NotLeader;
        }
```
The field is now read, so DELETE the `#[allow(dead_code)] // read in Task 4` line above `gate:` on the `TxnService` struct (T2 Step 1). (A stale `#[allow(dead_code)]` is not itself a `-D warnings` error, but the T2 comment promised its removal; delete it for cleanliness.)

- [ ] **Step 4: Run → PASS.**
Run: `cargo nextest run -p cluster stage_is_gated_until_the_range_is_settled` → PASS.

- [ ] **Step 5: Add the router local-led-write check.** In `RangeRouter::dispatch`, right after `let pinning = self.pinning_range(stmt)?;` (and before the `Begin/Commit/Rollback` match):
```rust
        // SP22 settle-before-serve: reject a locally-led WRITE (Insert/Update/Delete) on a
        // range whose rise sweep has not settled the current term. Reads (Select) and
        // DDL/txn-control pass ungated. Retryable NotLeader -> 40001 -> client retries.
        if matches!(
            stmt,
            Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. }
        ) && let Some(r) = pinning
            && self.engines.contains_key(&r)
            && self.leads.leads(r)
            && self.gate.as_ref().is_some_and(|g_| !g_.is_serving(r))
        {
            return Err(ExecError::NotLeader);
        }
```
This single chokepoint covers all three local-led write sites (`Pin::Open` first-DML, `Pin::Range` same-range run, and `stage_on`'s local branch) because `pinning_range` yields the table range for every write. (Confirm `Statement` is in scope — it is, used throughout `dispatch`.) The field is now read, so DELETE the `#[allow(dead_code)] // read in Task 4` line above `gate:` on the `RangeRouter` struct (T2 Step 3).

- [ ] **Step 6: Add a `MultiRangeCluster::leader_raft` accessor, then a real-gate router test.** The router gate check reads `is_serving(range)`, which needs the range's `openraft::Raft` handle — but `MultiRangeCluster` exposes only engines (`leader_engine`), no raft. Without a real raft the test could only exercise the `None`-gate no-op path, which proves nothing. So first add a one-line public accessor to `MultiRangeCluster` in `crates/cluster/src/range/cluster.rs` (mirror `raw_leader_engine`'s body):
```rust
    /// The leader node's `openraft::Raft` handle for `range` — for tests that need to drive a
    /// `RecoveryGate` (which reads `current_leader`/`current_term`) over the in-process cluster.
    pub async fn leader_raft(&self, range: RangeId) -> openraft::Raft<TypeConfig> {
        let leader = self.wait_for_leader(range).await;
        self.groups[range as usize].nodes[leader as usize].raft.clone()
    }
```
(`TypeConfig` is already imported in `cluster.rs`; `groups`/`nodes`/`raft` are the same fields `raw_leader_engine` uses.) Then, in `router.rs`'s `#[cfg(test)] mod tests`, add a real-gate test (mirror `coordinator_pause_seam_holds_a_txn_in_doubt`'s router construction):
```rust
    let gate = crate::recovery_gate::RecoveryGate::new(/* the cluster's leader node id for range 1 */);
    gate.register_range(1, c.leader_raft(1).await); // gated-by-default (sentinel term)
    let mut router = RangeRouter::new(/* …existing args… */, Some(gate.clone()));
    // A local-led UPDATE to the gated range is rejected (retryable); a SELECT passes.
    assert!(matches!(router.dispatch(&update_stmt).await, Err(ExecError::NotLeader)));
    let _ = router.dispatch(&select_stmt).await; // ungated read path
    // Open the gate for the live term → the UPDATE proceeds.
    let term = c.leader_raft(1).await.metrics().borrow().current_term;
    gate.mark_served(1, term);
    assert!(router.dispatch(&update_stmt).await.is_ok());
```
(`register_range` needs the gate's `id` to match the range-1 leader so `is_serving`'s `leader == Some(id)` holds — pass `c.wait_for_leader(1).await` as the `RecoveryGate::new` id. Use the same DDL-then-seed setup the seam tests use so the `UPDATE`/`SELECT` target a real table in range 1.)

- [ ] **Step 7: Run the cluster suite + a gated multi-process smoke run + clippy + fmt.**
Run: `cargo nextest run -p cluster` → PASS (in-process suites use `None` gate → unaffected; the three new gate tests pass).
Run: `cargo nextest run -p crabgresql --test crossrange_2pc_nemesis` → PASS. **Why here:** every in-process `-p cluster` suite uses a `None` gate (the `is_some_and` short-circuits to no-check), so they cannot exercise an enforced `Some` gate. This already-green multi-process suite drives a real leader rise through a `Some` gate — verifying that the T4 enforcement does NOT over-gate (a normal rise opens the gate fast enough that gated writes still pass), AT the task that introduces enforcement, instead of first surfacing in T6.
Run: `cargo clippy -p cluster --all-targets -- -D warnings` → clean. `cargo fmt -p cluster`.

- [ ] **Step 8: Commit.**
```bash
git add crates/cluster/src/twopc.rs crates/cluster/src/range/router.rs
git commit -m "feat(sp22): enforce the recovery gate at TxnService::stage and the router write path

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Stateright settle-before-serve model (teeth + positive)

**Files:** Create `crates/cluster/tests/crossrange_2pc_settle_model.rs` (UAC-safe name)

- [ ] **Step 1: Write the model.** Mirror `crates/cluster/tests/crossrange_2pc_model.rs`'s structure (a `Model` with `State`/`Action`/`properties`, a positive test, and a teeth test).

**The model must make the duplicate manifestable** — the bug needs BOTH the inherited marker's own `g` AND the new stage's `g` to go live. Seed the inherited marker as a write whose global decision is **already COMMIT** (the dangerous case: the coordinator durably committed `g_old` before the participant leader was killed; the new leader inherited a `Prepared(li -> g_old)` with no lock and `g_old` resolves to committed). A `Decide` that writes only the NEW stage's `g` would leave the inherited version never-live and `live_count()` would stay `<= 1` even with the gate off — so the teeth would not fire. Make the inherited `g_old` committable.

Model ONE row's MVCC versions + an `inherited_marker: Option<InDoubt { li, g_old, decision: bool }>` (a session-less marker; seed `decision: true` = destined-commit) + a per-term serving gate (`served_term`, `current_term`). A version goes live when its creating `g` is decided-commit AND (for the inherited one) the marker has been resolved/applied. Actions:
- `Begin` / `Stage` — the new cross-range write under `g_new` that **supersedes the currently-live version**. **Gated:** admitted only when `!self.settle_before_serve || s.served_term == s.current_term`. With the gate ON, a `Stage` while the inherited marker is unsettled returns `None` (no transition) — the writer never reads the unsettled marker.
- `ResolveInherited` — the inherited `g_old`'s version becomes live per its seeded `decision` (this is what makes the duplicate possible when an un-gated `Stage` already superseded the *old* base instead of `g_old`'s version).
- `DecideNew(bool)` — decide `g_new` (commit → its version goes live).
- `Settle` — the rise sweep: resolve the inherited marker (apply `g_old`'s decision) THEN `served_term = current_term`. This is the only path that opens the gate.
- `Rise` — new term: `current_term += 1`, the gate re-closes (until the next `Settle`).

Toggle `settle_before_serve: bool`. With it ON, the only admitted ordering is settle-then-stage (the new write supersedes `g_old`'s resolved version, so exactly one stays live). With it OFF, `Stage` can run while the marker is unsettled, supersede the *stale base*, and leave both `g_old`'s version and `g_new`'s version live.

Properties (`always`):
- **(teeth invariant)** `"at most one live version"` — `s.live_count() <= 1` (the SP21 MVCC detector; this is the load-bearing invariant the gate-off model must violate).
- `"no write supersedes an unsettled inherited marker"` — corroborating: no version is created while `inherited_marker` is unresolved and `served_term != current_term`.

- [ ] **Step 2: Positive test** (mirror `idempotent_stage_upholds_at_most_one_live`): `settle_before_serve: true` → `checker.assert_properties()` + `unique_state_count() > 1`.

- [ ] **Step 3: Teeth test** (mirror `non_idempotent_stage_double_apply_is_caught`): `settle_before_serve: false` → `!discoveries().is_empty()` and `discoveries().contains_key("at most one live version")` (the load-bearing teeth invariant — `live_count()` exceeds 1 once both `g_old` and `g_new` go live). The checker must FIND the duplicate when the gate is off. (If the second property `"no write supersedes an unsettled inherited marker"` is also violated in the same trace, asserting it too is fine, but `"at most one live version"` is the one that MUST appear in `discoveries()`.)

- [ ] **Step 4: Run + clippy + fmt.**
Run: `cargo nextest run -p cluster --test crossrange_2pc_settle_model` → 2 passed (teeth catches the duplicate, positive holds).
Run: `cargo clippy -p cluster --test crossrange_2pc_settle_model -- -D warnings` → clean. `cargo fmt -p cluster`.

- [ ] **Step 5: Commit.**
```bash
git add crates/cluster/tests/crossrange_2pc_settle_model.rs
git commit -m "test(sp22): Stateright settle-before-serve model (teeth + positive)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Multi-process participant-leader-kill nemesis

> **DEFERRED (did not converge).** The kill-`range_leader(1)`-every-round nemesis exposed that
> the participant-leader-kill problem has more layers than the data-range gate closes (range-0
> participant gap + an unsafe local-stage idempotency no-op + a residual cascading-failover 2PC
> tear/wedge that does NOT converge with incremental patches). Per the user's decision, SP22 ships
> the proven T1–T5 and DEFERS this nemesis + Task 6.5 to a dedicated slice. See the spec's
> "CORRECTION (as-shipped)" and the `sp22-range0-participant-gap-and-unsafe-local-stage-noop` memory.
> The recipe below is retained for the dedicated slice (and MUST use stable windows, not
> kill-every-round, per the no-starve rule).

**Files:** Create `crates/crabgresql/tests/crossrange_2pc_restage.rs` (UAC-safe); Modify `CLAUDE.md`

- [ ] **Step 1: Create the nemesis by copying + adapting.** Copy `crates/crabgresql/tests/crossrange_2pc_nemesis.rs` VERBATIM (the whole file — module-local helper block included; this verbatim duplication is the established convention). Then TWO edits:
1. The test fn name + doc: `cross_range_bank_conserves_total_under_participant_leader_kill`.
2. The victim selector (currently picks a non-leader):
```rust
        let l0 = c.range_leader(0).await;
        let l1 = c.range_leader(1).await;
        let victim = (0..c.len() as u64)
            .find(|&i| i != l0 && i != l1)
            .expect("a non-leader exists");
```
→ replace with `let victim = c.range_leader(1).await;` (kill the acct_b/range-1 participant leader every round). Keep the `kill+respawn` / `partition` alternation, the committed-op-paced bounded poll (`sleep(100ms)` inside a 5s-deadline loop — the allowed cross-process cadence, NOT a settle-sleep), the post-heal recovery transfers, and `read_total_cross_until_ok` (the bounded-retry conservation oracle).

3. **Add a settle-aware between-rounds barrier (load-bearing for non-flakiness).** This nemesis differs from the original in a way the original's pacing does NOT cover: the original kills a node leading NEITHER range, so both range leaders stay up; THIS kills the range-1 leader every round, so after each kill range 1 must re-elect **and complete the new T3 settle** before any range-1 write is admitted. During that window the workload's `cross_transfer` (which is **one-shot** — on a gated `40001` it `ROLLBACK`s and returns `false`, it does NOT use `exec_until_ok`) makes no progress, so `committed` does not advance and the 5s committed-op poll can hit its deadline with the gate still closed — then the next kill lands on a range-1 leader that never opened, and after enough rounds `total_committed` can stay 0 (non-vacuity flake) or recovery transfers can run short. So, immediately AFTER the existing post-fault recovered-quorum gate (`c.range_leader(0).await; c.range_leader(1).await;`) and BEFORE `round += 1`, add a settle-aware barrier that paces the next fault on *real cross-range write progress* (not the clock — consistent with the CLAUDE.md nemesis rule and `exec_until_ok`'s bounded-retry):
```rust
        // Settle-aware barrier: the just-killed range-1 leader gates writes until its rise
        // sweep settles. Drive ONE zero-sum cross-range op to completion (exec_until_ok
        // retries the brief 40001) so the next round starts from a range-1 leader that ADMITS
        // writes — paces on real progress, never a sleep.
        c.exec_until_ok(
            "BEGIN; UPDATE acct_a SET bal = bal - 0 WHERE id = 0; UPDATE acct_b SET bal = bal + 0 WHERE id = 0; COMMIT",
        )
        .await;
```
(Amount 0 conserves the total, so this barrier does not perturb the conservation oracle; it only proves range 1 is writable again. It is NOT counted in `total_committed`, which still measures only the worker `cross_transfer`s.) Verify the 3×-non-flaky requirement **empirically** (Step 2), not by assuming the pacing absorbs the settle window.

- [ ] **Step 2: Run it 3× (non-flaky).**
Run: `cargo nextest run -p crabgresql --test crossrange_2pc_restage` three times.
Expected: PASS all 3 — conservation holds (no 2-live crash, no recovery hang), non-vacuity (`total_committed > 0`). If it fails, the gate/sweep wiring has a gap (do NOT weaken the at-most-one-live assert; diagnose the gate — most likely the single-shared-`Arc` discipline or the apply-wait index).

- [ ] **Step 3: Append the SP22 CLAUDE.md audit line + run the UAC guard.** Add a `**SP22 (2026-06-14):**` paragraph to the UAC section (after the SP21 one), noting the two new test binaries `cluster::crossrange_2pc_settle_model` + `crabgresql::crossrange_2pc_restage` (both UAC-safe), no new dependency, and that the crabgresql list now includes `crossrange_2pc_restage`.
Run: `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` → empty.

- [ ] **Step 4: Commit.**
```bash
git add crates/crabgresql/tests/crossrange_2pc_restage.rs CLAUDE.md
git commit -m "test(sp22): multi-process participant-leader-kill nemesis — conservation under settle-before-serve

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6.5: Extend settle-before-serve to range 0 (the participant gap the T6 nemesis exposed)

> **DEFERRED to a dedicated slice.** The range-0 gate+sweep+scan-bound is structurally correct and
> regression-clean, but it is necessary-not-sufficient: with it (and the unsafe `staged_local_for`
> local-stage no-op removed), a residual cascading-failover conservation tear + recovery wedge
> persist under kill-every-round. The empirically-validated learnings (range-0 scan must bound at
> `GLOBAL_XID_BASE`; the local-stage no-op is UNSAFE under xid-reuse; the gate — not idempotency —
> is the load-bearing local-stage protection; the GTM ops are NOT gated) are recorded for the
> dedicated slice. This section is retained as that slice's starting design.

**Why this exists.** T6's first run BLOCKED: the participant-leader-kill nemesis tore the bank total (e.g. 804 vs 800) and crash-looped the 2-live `debug_assert` (surfacing as the barrier timeout). Two independent investigations converged on the root cause: **range 0 is a 2PC participant on EVERY cross-range transfer** (`acct_a` lives in range 0) but it is **ungated and unswept** — both bring-up loops `filter(|&r| r != 0)` for gate registration and the rise-sweep spawn, so `is_serving(0)` returns the unregistered-range default `true` and neither write check ever gates a range-0 write. The spec listed "range 0 as a 2PC participant" as a non-goal, justified by "the nemesis only kills `range_leader(1)`" — but that is **unsound**: killing the range-1 leader kills a whole *node*, which is also a range-0 voter (and ~1/5 of the time its leader), so the nemesis churns range 0 onto an ungated/unswept leader and a subsequent range-0 participant write supersedes an unsettled inherited `Prepared(L0 -> g)` marker → the duplicate-version / torn-total bug, on range 0. Separately, the gateway-LOCAL participant stage path (`RangeRouter::stage_on`'s local branch) bypasses the SP21 `staged_local_for(g)` idempotency that the remote `TxnService::stage` path has. The user chose the **complete fix**: bring range 0 under the same settle-before-serve discipline + make the local participant stage idempotent.

**The GTM subtlety (why range 0 isn't just "another data range").** Range 0 hosts the global decision clog: `commit_global_decision` writes `clog[g]` at `clog_key(g)` for `g >= mvcc::xid::GLOBAL_XID_BASE` (`1<<63`), in the SAME keyspace as range-0 participant markers `Prepared(L0 -> g)` at `clog_key(L0)` (`L0 < GLOBAL_XID_BASE`). The rise sweep's `in_doubt_globals_from` scans `clog_key(scan_lo)..clog_scan_end()` and advances the watermark to `max_li + 1`; on range 0 `max_li` would pick up a GLOBAL xid and jump the watermark past `GLOBAL_XID_BASE`, then miss every future participant marker. So the recovery scan must be **bounded to the local-participant xid space** before range 0 can be swept. (The GTM ops `begin_global_durable`/`commit_global_decision` are coordinator RPCs handled by `TxnService`'s `Begin`/`CommitGlobal` arms — NOT the `stage`/`dispatch` DML path — so gating range-0 WRITES does NOT gate the GTM; verify this so the gate cannot deadlock the coordinator.)

**Files:** `crates/executor/src/lib.rs` (scan bound + test), `crates/cluster/src/server_node.rs` (range-0 gate+sweep, all bring-up paths), `crates/cluster/src/range/router.rs` (local-stage gate+idempotency), `crates/crabgresql/tests/crossrange_2pc_restage.rs` (re-run).

### Part A — Range-0-safe recovery scan (executor)
- [ ] In `in_doubt_globals_from` (`crates/executor/src/lib.rs:277`) AND `staged_local_for` (`:327`), change the scan upper bound from `kv::key::clog_scan_end()` to `kv::key::clog_key(mvcc::xid::GLOBAL_XID_BASE)` so the scan covers only local-participant xids (`< GLOBAL_XID_BASE`) and never the global-decision entries. On a data range this is a no-op (no global xids exist there); on range 0 it keeps `max_li`/the watermark in the local space. Add `use mvcc::xid::GLOBAL_XID_BASE;` or fully-qualify.
- [ ] **Unit test** (`#[cfg(test)]` in the executor, mirroring the existing recovery-scan tests): build a range-0-style engine (or seed a `kv` directly) with a MIX of `Prepared(L0 -> g)` participant markers at `L0 < GLOBAL_XID_BASE` AND global-decision entries `Committed`/`Aborted` at `g >= GLOBAL_XID_BASE`; assert `in_doubt_globals_from(0)` returns ONLY the in-doubt participant `g`s and that `new_scan_lo < GLOBAL_XID_BASE` (the watermark never jumps into the global space). This is the new-logic teeth for Part A.

### Part B — Register range 0 in the gate + spawn its rise sweep (server_node, EVERY bring-up path)
- [ ] In `start_static` AND `start_replicated` (and any other path that builds ranges — grep `resolve_in_doubt_on_leadership` spawn sites and `register_range` call sites), register range 0 in the gate and spawn its rise sweep, with the SAME register-before-spawn ordering as data ranges. Range 0 is built specially (before/outside the `filter(|&r| r != 0)` pending loop), so add explicitly after the gate + `sweep_client` exist:
  ```rust
  gate.register_range(0, rafts[&0].clone());
  tokio::spawn(resolve_in_doubt_on_leadership(
      rafts[&0].clone(), 0, cfg.id, engines[&0].clone(), sweep_client.clone(), gate.clone(),
  ));
  ```
  (Use the actual `r0_raft`/`r0_engine` bindings if they're still in scope at that point, else `rafts[&0]`/`engines[&0]`.) Range 0's sweep abort-races inherited `Prepared(L0 -> g)` markers via `client.call(0, CommitGlobal{g, false})` — the authoritative global-decision write, write-once-safe.
- [ ] **Verify the GTM is not gated:** confirm `begin_global_durable`/`commit_global_decision` reach range 0 via `TxnService`'s `Begin`/`CommitGlobal` handlers (NOT `stage`), so a closed range-0 gate never blocks the coordinator. (Read `TxnService::handle` / the coordinator path.) If any GTM op DID route through the gated `stage`/`dispatch` DML path, STOP and report — gating range 0 would deadlock 2PC.

### Part C — Gate the gateway-local participant stage (router) — GATE ONLY, NO idempotency no-op
> **CORRECTED after T6 digging:** the originally-planned `staged_local_for(g)` idempotency no-op on
> the local branch is **UNSAFE** and must NOT be added. The router coordinates with a FRESH `g` per
> escalation, so it never legitimately re-stages the same `g` locally; and under GTM **xid reuse** (a
> range-0 reseed across churn can reuse a `g`), `staged_local_for(g)` matches a STALE marker → the
> no-op returns success-without-staging → the coordinator commits `g` with a missing half → a
> money-creating tear (confirmed: removing the no-op eliminated the frequent +N tears). The GATE
> alone is the correct, load-bearing local-stage protection.
- [ ] In `RangeRouter::stage_on`'s LOCAL branch (`crates/cluster/src/range/router.rs:522-526`), before `join_global`/`run`, add ONLY the gate check:
  ```rust
      if self.engines.contains_key(&range) && self.leads.leads(range) {
          // Gate the local participant version-creating write on a freshly-risen, unsettled
          // leader (retryable NotLeader -> 40001 -> client retries). NO staged_local_for no-op
          // here — see the CORRECTED note above (unsafe under GTM xid reuse).
          if self.gate.as_ref().is_some_and(|g_| !g_.is_serving(range)) {
              return Err(ExecError::NotLeader);
          }
          self.ensure_began_on(range).await?;
          self.session_mut(range).join_global(g).await?;
          return self.session_mut(range).run(stmt).await;
      }
  ```

### Part D — Re-run the nemesis 3× (the proof)
- [ ] `cargo nextest run -p crabgresql --test crossrange_2pc_restage` THREE times → PASS all 3 (conservation holds, no 2-live crash-loop, non-vacuous). If it STILL tears, STOP and report with output — do NOT weaken any assert.
- [ ] Regression: `cargo nextest run -p crabgresql --test crossrange_2pc_nemesis --test crossrange_2pc_replicated` and `cargo nextest run -p cluster` and `-p executor` → green (range-0 gating must not regress the happy path or the existing nemeses).
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean; `cargo fmt --all`.

### Part E — Model coverage note
The existing T5 Stateright model proves "an unsettled inherited marker + an un-gated write → duplicate; gated → safe" for a generic participant range. Range 0 is now just another gated participant range, so the model's invariant covers it; Part A's range-0 scan-bound is proven by its dedicated executor unit test (the only genuinely new logic). Record this in the traceability note rather than adding a redundant model.

### Part F — Commit + reconcile the spec non-goal
- [ ] Commit the fix across the three crates. Then UPDATE the spec: REMOVE "Range 0 as a 2PC participant" from Non-goals (it is now in scope and fixed) and add a short "Range-0 participant gap (found + fixed in T6.5)" note to the design's risk/decision history.
```bash
git add crates/executor/src/lib.rs crates/cluster/src/server_node.rs crates/cluster/src/range/router.rs docs/superpowers/specs/2026-06-14-crabgresql-sp22-d3c-settle-before-serve-design.md
git commit -m "fix(sp22): bring range 0 under settle-before-serve (gate+sweep+range-0-safe scan) + idempotent local stage

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Gauntlet, traceability, finish

> **As-shipped (T6/T6.5 DEFERRED):** the gauntlet runs over the proven T1–T5 only; there is no
> `crossrange_2pc_restage` in the tree (it didn't converge and was not committed). The spec's
> Traceability + Success-criteria tables are already filled with the as-shipped status (3–6 MET,
> 1–2 DEFERRED).

- [ ] **Step 1: Full-workspace gauntlet.**
- `cargo fmt --all --check` → clean (else `cargo fmt --all`).
- `cargo clippy --workspace --all-targets -- -D warnings` → clean.
- `cargo nextest run --workspace` → all pass (incl. the existing `crossrange_2pc_{nemesis,replicated}` multi-process regression with the gate enforced).
- `cargo test --workspace --doc` → pass.
- `cargo deny check` → ok (no new dependency).

- [ ] **Step 2: (Done above)** The spec Traceability + Success-criteria tables are filled with the as-shipped status. Criterion 3 (the gate reject/admit) is proven by `recovery_gate::tests` + `twopc` `stage_is_gated_until_the_range_is_settled` + the router real-gate test; criterion 4 by `crossrange_2pc_settle_model`; criteria 1–2 (the multi-process participant-leader-kill empirical proof) are DEFERRED.

- [ ] **Step 3: Commit traceability.**
```bash
git add docs/superpowers/specs/2026-06-14-crabgresql-sp22-d3c-settle-before-serve-design.md
git commit -m "docs(sp22): traceability table — criteria 1-6 mapped to proving tests

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 4: Finish.** Use superpowers:finishing-a-development-branch. Standing preference: option 2 (push a fresh non-force branch + PR). The PR is **stacked on SP21 (#37)** — base it on the SP21 branch (or note the stack); rebase `--onto origin/main` once SP21 squash-merges. PR body ends with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.

---

## Self-Review

**Spec coverage:** RecoveryGate → T1; gate wiring → T2; rise-sweep apply-wait + mark_served → T3; the two write checks → T4; Stateright model → T5; nemesis → T6; gauntlet/traceability/finish → T7. All 6 success criteria mapped (T7 table). Components A–F of the spec all covered.

**Incremental-green ordering:** T2 threads the field (unchecked) → no behavior change. T3 opens the gate on rise (still unchecked) → no behavior change. T4 enforces the check, and because T3 opens it, normal writes pass after a brief settle → existing multi-process suites stay green. The participant-leader-kill fix is proven by T6.

**Placeholder scan:** every code step shows complete code; T2/T5/T6 reference exact existing files to thread/copy (an established convention), not placeholders. No TBD/TODO.

**Type consistency:** `RecoveryGate` (`is_serving`/`mark_served`/`register_range`, `Arc<Self>`) used consistently; `gate: Option<Arc<RecoveryGate>>` on `TxnService` + `RangeRouter` (Option) vs `Arc<RecoveryGate>` (non-Option) on the rise sweep — matches the spec (the seams are Option; the sweep always has one). `TxnService::new(engines, gate)` and `RangeRouter::new(…, gate)` signatures consistent across tasks. `is_some_and`/`is_none_or` (never `map_or(true, …)`). openraft API (`current_term`, `ensure_linearizable`, `applied_index_at_least`) used per the confirmed anchor map.

**Known risks folded:** the replicated ordering trap (growable gate + register-per-range), the single-shared-`Arc` requirement (T2 Step 6 explicit), the borrow-before-await rule (T3 scoped `term` read), the apply-wait index (`ensure_linearizable`, never `last_applied.index`), `mark_served` only on the Ok arm (gate stays closed on a failed settle), and the in-process/seam `None` gate.

**Adversarial plan-review findings folded (revise-then-implement):**
- **must-fix #1** — `#[allow(dead_code)]` on BOTH `TxnService.gate` and `RangeRouter.gate` in T2 (a `#[derive(Clone)]` does NOT suppress the dead-code lint — rustc ignores derived `Clone` in dead-code analysis), removed in T4 when the `is_serving` checks read them. `RangeGatewayEngine.gate` is read by `connect` in T2 → no allow.
- **must-fix #2** — the 4th `RangeRouter::new` caller `jepsen_bank.rs:1021` (`router_over`); T2 Step 4 greps the whole crate and enumerates all four callers, each new caller getting a NEW trailing `None`.
- **must-fix #3** — the rise sweep re-fires on `is_leader && !gate.is_serving(range)` (not rise-edge-only `!was_leader`), so a FAILED settle keeps retrying under continuous leadership instead of wedging the gate closed; `mark_served` only on `Ok`. Bounded `SETTLE_RETRY_INTERVAL` wake so a failed settle retries with no further metrics change.
- **must-fix #4** — static path pins `register_range` strictly BEFORE the `resolve_in_doubt_on_leadership` spawn (else `mark_served`-before-`register` is a no-op that wedges a stable single leader closed forever).
- **should-fix A** — `ensure_linearizable` wrapped in `tokio::time::timeout(SETTLE_TIMEOUT, …)` so a win-then-lose-quorum flap cannot freeze the sweep before it re-polls.
- **should-fix B** — `spawn_sql` (the single-range wrapper, distinct from `spawn_sql_gateway`) gains the gate param explicitly; `None` on the `serve_routed` branch.
- **should-fix C** — T4 Step 7 adds a `crossrange_2pc_nemesis` smoke run (the first `Some`-gate traffic path) so an over-gating regression surfaces AT enforcement, not two tasks later.
- **should-fix D** — T6 adds a settle-aware between-rounds barrier (one `exec_until_ok` zero-sum cross-range op) because killing `range_leader(1)` every round + the one-shot `cross_transfer` (no `exec_until_ok`) can otherwise starve non-vacuity; verified 3× empirically.
- **should-fix E** — `MultiRangeCluster::leader_raft` accessor added so the T4 router test has REAL-gate teeth (not just the `None` no-op path).
- **should-fix F** — the T5 model seeds the inherited marker's `g_old` as destined-commit (with a `ResolveInherited`/`Settle` path) so the duplicate manifests with the gate off and the `"at most one live version"` teeth invariant actually fires.
- **nits** — range-0-as-participant recorded as a spec Non-goal; replicated register order pinned before `register_engine`; criterion-3 rise-path coverage attributed to the T6 nemesis in the traceability note.
