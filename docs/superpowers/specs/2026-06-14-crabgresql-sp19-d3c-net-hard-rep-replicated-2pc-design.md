# SP19 / D3c-net-hard-rep — Cross-range 2PC over the replicated meta-range layout

**Date:** 2026-06-14
**Slice:** SP19 (D3c-net-hard-rep)
**Status:** design

## Summary

Wire the already-built, fault-hardened cross-range two-phase-commit (2PC) machinery —
the participant `TxnService`, the gateway/coordinator dispatch, and the full SP18
recovery set — onto the **replicated meta-range bring-up path** (`start_replicated`),
which today wires `None` for the node-level 2PC service. Prove it with a multi-process
end-to-end test that boots via the replicated descriptor path (not the static seed),
conserves a cross-range bank total under a crash/partition nemesis, **and** survives a
full-cluster restart.

No change to the 2PC protocol or the SP18 write-once correctness core. This slice is
pure topology generalization + a growable engines registry + a proving test.

## Where this sits: the roadmap

SP16 built the in-process cross-range 2PC core; SP17 put it over the network (static
co-located layout); SP18 hardened it against coordinator crashes and participant-leader
failovers (write-once global decision + participant self-resolve), proven by a
multi-process crash nemesis. **Every guarantee SP18 proved holds only on the STATIC
bring-up** (`RangeLayout::Static`), the layout the SP18 e2e uses. The **replicated**
meta-range layout (SP15 / D3b) — the path an HA cluster actually boots through, learning
its range descriptors from a replicated meta range — still passes `None` for the
`TxnService` (`crates/cluster/src/server_node.rs::start_replicated`, the listener bind at
~`:435-441`). So on a replicated node every `Stage`/`Release`/`BeginGlobal`/
`CommitGlobal`/`GlobalBarrier` RPC returns `TxnResp::Err("node hosts no 2PC service")`,
and none of SP18's recovery runs.

SP19 closes the verbatim SP18 Non-goal "2PC over the replicated (meta-range) node
layout." It is the lowest-risk, highest-readiness continuation of the D3c arc: all the
2PC + recovery machinery already exists and works on static; this slice makes it reachable
on the shipping topology. It does **not** open D4 (range splits); but the growable engines
registry it introduces is deliberately forward-compatible with D4's runtime range changes.

## The load-bearing constraints (why the design is shaped this way)

1. **The node listener must stay bound throughout the two-phase replicated bootstrap.**
   In `start_replicated`, Phase 1 brings up range 0 and binds the node listener; Phase 2
   then `wait_for_range_map`s the committed descriptor blob and builds the data ranges.
   Range 0's Raft needs its AppendEntries/vote RPCs served by this node's listener while
   the blocking Phase-2 wait runs, and `seed_if_absent` needs range-0 leadership. The
   listener therefore **cannot be closed and rebound later** (this rules out "just bind
   once at the very end"). The fix must keep the early listener live.

2. **The complete engines map exists only at the end of Phase 2.** At listener-bind time
   (Phase 1) only range 0's engine exists; the authoritative `RangeMap` is unknown until
   `wait_for_range_map` returns, and the data-range engines are not built until the Phase-2
   loop completes. The static path mirrors this invariant — it builds `TxnService` over the
   *complete* map only after every engine is ready.

3. **`TxnService` is structurally non-growable today.** It stores `engines:
   HashMap<RangeId, Arc<SqlEngine>>` **by value** at construction and never mutates it;
   `engine(range)` and `session_handle(range)` resolve via `engines.get(&range)`, returning
   absent for any range not present at construction. A `TxnService` built in Phase 1 over
   `{0: engine0}` can serve global/coordinator ops (which only need `engine(0)`) but can
   never serve `Stage`/`Release` for a data range absent from its constructor map — and
   those engines don't exist when the listener binds. So the engines map must become
   **growable / late-populated**.

4. **The range-0 read barrier is a bootstrap dependency.** `Range0Barrier::ensure_readable`
   RPCs `call(0, GlobalBarrier)` when this node does not lead range 0, routed through the
   same `serve_node_protocol` task. Any data-range cross-range read needs that listener
   live. This reinforces constraint 1 (keep the listener up) and is satisfied because the
   listener is already bound in Phase 1.

5. **No `await` under a synchronous lock.** Whatever growable primitive holds the engines
   map, the dispatch path must never hold a lock guard across an `.await` — mirroring the
   existing held-map discipline. `arc-swap` satisfies this by construction: a load yields an
   owned snapshot, no guard to hold.

## Decisions (locked during brainstorming)

1. **Growable engines registry via `arc-swap` (Approach B).** Change `TxnService.engines`
   to `Arc<ArcSwap<HashMap<RangeId, Arc<SqlEngine>>>>`. Reads are lock-free (`load()` →
   owned snapshot); a new range is added with a copy-on-write `rcu()` swap. Chosen over a
   `std::sync::RwLock` because it (a) eliminates the no-`await`-under-guard footgun, (b) is
   the right shape for D4's runtime range mutation (readers concurrent with rare writers),
   and (c) is a tiny, vetted, established crate — consistent with preferring established
   crates over hand-rolled concurrency. Chosen over Approach A (keep an early `None` listener
   + late-install via an interior-mutable cell) because B is cleaner given `TxnService` is
   already `Clone` over a shared held-map: making the engines map shared too means the
   installed clone and the Phase-2 registrations share one growable cell, with **no**
   `serve_node_protocol` signature change. Approach C (defer the listener bind) is rejected:
   it breaks range-0 Raft liveness during the blocking Phase-2 wait (constraint 1).

2. **Wire the FULL fault-hardened recovery set onto the replicated path**, not just minimal
   dispatch: `release_on_leadership_loss` (per range), `resolve_in_doubt_on_leadership` (per
   data range), and `participant_silence_sweeper` (per node) — the exact set that already
   runs on static. The point of the slice is to make SP18's hardening reachable on the
   replicated layout, so recovery is in scope.

3. **A `Stage` for an unregistered range returns a *retryable* `TxnResp::NotLeader`, not a
   hard `Err`.** In the co-located replicated layout every node eventually hosts every range,
   so "range absent from the growable map" means "this participant is still mid-bootstrap,"
   not "wrong node." Returning `NotLeader` lets the coordinator's `TwoPcClient` re-resolve once
   within `TXN_TIMEOUT` and surfaces a *retryable* `40001` to the client rather than an
   unsupported-feature `0A000`/hard error; it auto-succeeds only if Raft leadership actually
   shifts during the window (otherwise the client retries the whole txn). Conservation holds
   either way (a hard-`Err` `Stage` is a clean nil abort) — the value is a retryable signal, not
   a guaranteed in-flight save. (`Release` already returns an idempotent
   `TxnResp::Released` no-op for an absent `(g, range)` — a session can only have been staged
   on a node whose engine was registered — so it needs no change.)

4. **The proving e2e covers BOTH crash/partition conservation AND a full-cluster restart.**
   Boot via the replicated path, run the SP18 cross-range conservation crash nemesis, then
   stop every node and respawn every node (re-reading the immutable descriptor blob via
   `wait_for_range_map` and the durable 2PC state), and assert conservation + a post-restart
   recovery round still hold.

## Components

### 1. Growable engines registry (`crates/cluster/src/twopc.rs`)

`TxnService.engines: HashMap<RangeId, Arc<SqlEngine>>` becomes
`Arc<ArcSwap<HashMap<RangeId, Arc<SqlEngine>>>>`.

- `TxnService::new(engines: HashMap<RangeId, Arc<SqlEngine>>)` keeps its signature (the
  static path passes the complete map; the replicated path passes `{0: engine0}`). Internally
  it wraps the map in `Arc::new(ArcSwap::from_pointee(engines))`.
- `engine(&self, range) -> Option<Arc<SqlEngine>>` — now returns an **owned** `Arc`
  (`self.engines.load().get(&range).cloned()`). Backward-compatible with the two
  `handle_txn` `svc.engine(0)` call sites (they bind `Some(e)` and call via `Deref`) and
  with `session_handle` (which already `.clone()`s).
- `register_engine(&self, range: RangeId, engine: Arc<SqlEngine>)` — new; copy-on-write
  insert via `self.engines.rcu(|cur| { let mut m = (**cur).clone(); m.insert(range,
  engine.clone()); m })` (the `rcu` closure receives `&Arc<HashMap>`). Idempotent
  (re-registering a range replaces with the same Arc).
- `session_handle` calls `self.engine(range)?` (owned). Constraint 3 (retryable absent-range)
  is implemented inside **`stage`**: it resolves the engine FIRST (before `pgparser::parse`)
  and short-circuits to `TxnResp::NotLeader` when `session_handle` is `None`, replacing the
  current hard `TxnResp::Err("no engine for range …")`. (`handle()` just delegates `Stage` to
  `stage`; `Release` already returns an idempotent `TxnResp::Released` for an absent
  `(g, range)` and is unchanged.)
- The `held` map (`Arc<Mutex<…>>`, `tokio::sync::Mutex`) is **unchanged** — its discipline
  and all of `release_all_for_range` / `sweep_stale` / `resolve_in_doubt` are untouched.

`arc-swap` is added to `crates/cluster/Cargo.toml` (and the workspace `Cargo.toml` deps
table, matching the workspace-inherited dependency style).

### 2. Replicated bring-up wiring (`crates/cluster/src/server_node.rs::start_replicated`)

- **Phase 1:** after range 0 is built + Arc-wrapped, construct `TxnService::new({0: engine0})`
  and pass `Some(txn.clone())` to `serve_node_protocol` (instead of `None`). Global/coordinator
  ops work the instant range 0 is up; the listener stays bound through Phase 2 unchanged.
  Keep the `txn` handle.
- **Phase 2:** after `wait_for_range_map`, in the data-range build loop, as each engine is
  Arc-wrapped call `txn.register_engine(range, engine.clone())` (so `Stage`/`Release` for that
  range works as soon as it is online). After the loop (the `rafts` map is complete), build the
  `sweep_client`/`sweeper_client` `TwoPcClient`s over the complete `rafts` map (mirroring static
  `:481`), inject the `Range0Barrier`s (already done at `:482-491`), and spawn the full recovery
  set: `resolve_in_doubt_on_leadership` per data range, `release_on_leadership_loss` per range,
  and `participant_silence_sweeper` once — using `txn.clone()` and the post-loop clients, exactly
  as static does (`server_node.rs:252-282`).
- The static path (`start_static`) is unchanged except for the `engine()` owned-return ripple
  (it still passes a complete map to `new` and never calls `register_engine`).

### 3. Multi-process replicated e2e (`crates/crabgresql/tests/crossrange_2pc_replicated.rs`)

UAC-safe filename (no `setup/install/update/patch/upgrad`). Reuses `mod harness;`.

- Boot via `Cluster::spawn_multirange_replicated(5, vec![2])` — 5 nodes / 2 ranges (a victim
  leading neither range always exists, as relied on by the SP18 nemesis), learning the layout
  from the meta range.
- Seed `acct_a` (range 0) and `acct_b` (range 1) through `c.exec_until_ok(...)` (bounded retry,
  resilient to transient `08006` at bring-up — the established setup pattern).
- Run the SP18 cross-range conservation workload (round-robin gateways) + crash/partition
  nemesis (non-leader victim, paced on a committed-op progress signal), asserting conservation +
  the post-heal all-pairs recovery round + non-vacuity — the same oracle as
  `crossrange_2pc_nemesis.rs`, now on the replicated boot path.
- **Then a full-cluster restart:** heal, await leaders, `kill` every node, `respawn` every node
  (each re-reads the immutable descriptor blob via `wait_for_range_map` and recovers its durable
  2PC state), await leaders on both ranges, run a post-restart all-pairs recovery round, and
  assert conservation still holds. This proves the descriptor blob **and** durable 2PC state
  survive a full restart on the replicated layout, and that recovery frees any txn left in-doubt
  across the restart.

One harness fix (no new helpers): `respawn` now honors the cluster's replicated
bring-up mode. `Cluster` gains a `replicated: bool` flag (set by the constructors:
`false` for `spawn`/`spawn_multirange`, `true` for `spawn_multirange_replicated`),
and `respawn` passes it to `spawn_node` instead of the previously-hardcoded `false`.
This makes a respawned replicated node re-boot through `start_replicated` /
`wait_for_range_map` — genuinely exercising the descriptor-blob re-read on restart
(previously a respawn re-booted through the STATIC path, so the restart proved durable-
state survival on the static layout, not the descriptor-blob re-read this slice
requires). Everything else (`spawn_multirange_replicated`, `kill`, `range_leader`,
`exec_until_ok`, `pick_live_gateway`) already exists and is unchanged; the static-
respawn tests keep `replicated=false` and behave exactly as before.

## Data flow

### A cross-range transfer on the replicated layout (happy path)

1. A gateway (any node) receives `BEGIN; UPDATE acct_a …; UPDATE acct_b …; COMMIT`. The second
   UPDATE touches a different range → escalate to global 2PC.
2. Gateway → range 0 leader: `BeginGlobal` → `g`. (`engine(0)` is present from Phase 1.)
3. Gateway → each participant node: `Stage{g, range, sql}`. The participant's `TxnService` —
   now installed and with its data-range engine registered — parks a held `SqlSession`, writes
   `Prepared(Li→g)`, holds the row lock. If a participant is still mid-bootstrap (range not yet
   registered), it returns `NotLeader` and the coordinator retries.
4. Gateway → range 0 leader: `CommitGlobal{g, commit}` → write-once `clog[g]` decision →
   effective decision returned.
5. Gateway → each participant: `Release{g, range, committed}` following the effective decision.
   Both rows flip atomically.

### Recovery (coordinator crash) — identical to SP18, now on replicated nodes

If the coordinator crashes after `Stage` before the decision, the alive participant's held
session self-resolves within `PARTICIPANT_SILENCE_TIMEOUT` (`participant_silence_sweeper`), or a
failed-over participant's durable `Prepared(→g)` marker is finalized on the new leader's rise
(`resolve_in_doubt_on_leadership`), both via the write-once abort-race against range 0. On
leadership loss, `release_all_for_range` frees held locks promptly. These watchers now run on
replicated nodes (they did not before this slice).

## Success criteria

| # | Criterion | Verified by |
|---|---|---|
| 1 | A replicated node serves every Txn RPC (`BeginGlobal`/`CommitGlobal`/`GlobalBarrier`/`Stage`/`Release`) — no more `TxnResp::Err("node hosts no 2PC service")`. | e2e bring-up + a cross-range commit through a replicated gateway |
| 2 | `TxnService`'s engines map is growable: `register_engine` adds a range, `engine`/`session_handle` see it, reads are lock-free and never hold a guard across `await`. | `twopc.rs` unit test for register/lookup + clippy |
| 3 | The full recovery set (`release_on_leadership_loss`, `resolve_in_doubt_on_leadership`, `participant_silence_sweeper`) runs on the replicated path. | e2e recovery under the nemesis |
| 4 | A `Stage` for an unregistered range returns retryable `TxnResp::NotLeader`, not a hard `Err` (`Release` is already an idempotent no-op). | `twopc.rs` unit test |
| 5 | The cross-range bank total is conserved under a multi-process crash/partition nemesis on the **replicated** boot path, and the workload makes progress. | `crossrange_2pc_replicated` nemesis conservation + non-vacuity |
| 6 | Conservation + recovery survive a **full-cluster restart** (descriptor blob + durable 2PC state re-read). | `crossrange_2pc_replicated` restart round |
| 7 | All SP16/SP17/SP18 in-process + networked cross-range suites pass unchanged; `arc-swap` is the only new dependency; `#![forbid(unsafe_code)]`; full gauntlet green; traceability. | regression gate + gauntlet |

## Test plan

**Sleep policy.** In-crate unit tests (register/lookup, retryable-absent-range) are deterministic
and sleep-free. The multi-process e2e uses the harness's bounded poll cadence + committed-op
pacing (the sanctioned cross-process exception), never a settle-sleep. Setup DDL uses
`exec_until_ok` (bounded retry). The full-cluster-restart waits on `range_leader` (a real
condition), not a fixed sleep.

1. **Growable map (unit, `twopc.rs`)** — build a `TxnService` with `{0}`, assert `engine(1)` is
   `None`; `register_engine(1, e1)`; assert `engine(1)` is `Some`; a re-register replaces
   idempotently. Deterministic, in-crate.
2. **Retryable absent range (unit, `twopc.rs`)** — `handle(range=7, Stage{g, range:7, …})` on a
   `TxnService` lacking range 7 returns `TxnResp::NotLeader` (not `Err`). Deterministic.
3. **Replicated cross-range commit (e2e)** — boot replicated, commit a cross-range transfer
   through a non-leading gateway, assert both rows reflect it (a replicated node now serves the
   2PC). (Subsumed by the nemesis test's first committed transfer + the conservation oracle.)
4. **Replicated crash/partition conservation (e2e)** — `crossrange_2pc_replicated`: the SP18
   nemesis on the replicated boot path; conservation + post-heal recovery round + non-vacuity.
5. **Full-cluster restart durability (e2e)** — after the nemesis, kill all + respawn all, await
   leaders, post-restart recovery round, assert conservation. UAC-safe binary.
6. **Regression** — the SP16/SP17 `crossrange_2pc` / `crossrange_2pc_net` and the SP18
   `crossrange_2pc_nemesis` + `jepsen_bank` cross-range suites stay green (the `engine()`
   owned-return and the growable map must not regress the static path).
7. **Gauntlet** — `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D
   warnings`; `cargo nextest run --workspace` + `cargo test --workspace --doc`; `cargo deny
   check` (must accept `arc-swap`); UAC guard; traceability.

## Non-goals (explicit → later)

- **Dynamic descriptor changes / range splits / merges (D4).** The growable engines registry is
  forward-compatible with runtime range mutation, but this slice only exercises the
  boot-time-learned, immutable descriptor set (D3b is relocate-only). D4 builds the actual
  runtime-mutation machinery.
- **Any change to the 2PC protocol or the write-once correctness core.** Unchanged from SP18.
- **2PC on a non-co-located layout** (ranges not hosted on every node). The retryable-`NotLeader`
  decision assumes the co-located model (every node eventually hosts every range); a future
  non-co-located layout will need a distinct "not hosted here" vs "not ready yet" signal.
- **GC of settled markers / clog records, mid-txn re-stage, cross-range SSI** — still deferred
  (their own future slices).

## Risks (and mitigations)

- **The growable-map read path must never hold a lock across `await`.** Mitigated by `arc-swap`:
  `load()` yields an owned snapshot (`Arc<HashMap>`), so there is no guard to hold — the footgun
  is removed by construction, not by discipline. (This is the main reason `arc-swap` was chosen
  over `std::sync::RwLock`.)
- **A participant mid-bootstrap could spuriously abort a cross-range txn.** A `Stage` for a
  range not yet registered returns retryable `NotLeader` (decision 3), so the coordinator retries
  within `TXN_TIMEOUT` rather than aborting; and a hard failure is still a clean nil abort
  (conservation holds, the workload counts only committed transfers). The bounded boot window
  (`boot_timeout`) keeps the retry from hanging.
- **Watcher-vs-dispatch ordering at bootstrap.** A `Stage` could park a held session for a range
  before that range's `release_on_leadership_loss` watcher is spawned. This window is tiny
  (bootstrap), and `participant_silence_sweeper` is the correctness backstop (it self-resolves any
  stranded held session). The static path tolerates the same ordering. Mitigated by spawning the
  per-range watchers in the Phase-2 loop, close to each engine's registration.
- **Adding a shipped dependency.** `arc-swap` is small, ubiquitous, MIT/Apache, advisory-free;
  `cargo deny` accepts it. The slice adds no other dependency.
- **Restart must not lose the descriptor blob or durable 2PC state.** Both live in the durable KV;
  `seed_if_absent` is write-once (a no-op on restart) and `wait_for_range_map` re-reads the
  committed blob. The restart e2e is exactly the proof that this holds.
- **The `engine()` owned-return ripple.** Changing `engine()` from `&Arc` to owned `Arc` touches
  every caller; `cargo` makes the breakage explicit and the call sites (exactly `handle_txn` ×2 at
  `server.rs:242,250` and `session_handle`) all already use the value by `Deref`/`clone`. Low risk,
  mechanically verified (the adversarial plan review confirmed no other `TxnService::engine` caller).
- **The post-restart conservation read must be bounded-retry.** A one-shot `read_total_cross`
  (`.expect`, no timeout) against a just-respawned node can hit a transient `08006` / a range still
  applying the recovered blob — the exact flake class that failed PR #34's `check` job. Mitigated by
  a `read_total_cross_until_ok` helper that re-resolves a live gateway and retries each SELECT under a
  bounded deadline (never a settle-sleep).
- **The all-node restart depends on leaderless re-election from persisted membership.** `respawn`
  passes `bootstrap=false`, so every node re-forms quorum on both ranges from its persisted Raft
  membership alone (no bootstrap hint). Standard Raft recovers this, but no existing test exercises a
  whole-cluster replicated restart — so the plan de-risks it with an isolated re-election check before
  the e2e relies on it; if the cluster cannot re-elect leaderless, that is an openraft restart-recovery
  issue to fix, not something to mask with a longer timeout.
- **CI wall-clock budget.** The e2e stacks a 5-node replicated initial boot (≤60s `boot_timeout`/node),
  the nemesis loop, a full-cluster kill+respawn (another ≤60s window), and the post-restart round —
  target ≤~120s. Keep `MIN_ROUNDS`/`OPS` at the SP18 minimum; if it approaches the nextest per-test
  timeout on the 2-core runner, give `crossrange_2pc_replicated` its own slow concurrency group /
  extended timeout in `.config/nextest.toml`.

## Traceability

Each success criterion (1–7) maps to its concrete proving test (all shipped and green at the SP19 gauntlet).

| # | Criterion | Task | Proving test(s) |
|---|---|---|---|
| 1 | A replicated node serves every Txn RPC (no more `None` 2PC service) | T2/T3 | `crabgresql::crossrange_2pc_replicated::replicated_cross_range_bank_conserves_under_nemesis_and_restart` (the workload commits cross-range transfers through replicated gateways — impossible if Txn RPCs returned `Err("node hosts no 2PC service")`) |
| 2 | `TxnService`'s engines map is growable (register/lookup, lock-free reads, CoW preserves prior entries) | T1 | `cluster::twopc::tests::register_engine_makes_a_range_servable` |
| 3 | The full recovery set (`release_on_leadership_loss`, `resolve_in_doubt_on_leadership`, `participant_silence_sweeper`) runs on the replicated path | T2/T3 | `crossrange_2pc_replicated` recovery under the nemesis (post-heal all-pairs round commits — a stranded lock would block it); wired in `start_replicated` mirroring `start_static` |
| 4 | A `Stage` for an unregistered range returns retryable `TxnResp::NotLeader`, not a hard `Err` | T1 | `cluster::twopc::tests::stage_for_an_unregistered_range_is_retryable_not_a_hard_err` |
| 5 | Cross-range bank total conserved under a multi-process crash/partition nemesis on the **replicated** boot path; workload makes progress | T3 | `crossrange_2pc_replicated` nemesis conservation `assert_eq!` + `assert!(total_committed > 0)` |
| 6 | Conservation + recovery survive a **full-cluster restart** (descriptor blob re-read via `start_replicated`/`wait_for_range_map` + durable 2PC state) | T3 | `crossrange_2pc_replicated` restart round (kill-all → respawn-all through the replicated path → post-restart recovery round + conservation `assert_eq!`) |
| 7 | All SP16/SP17/SP18 suites pass unchanged; `arc-swap` the only new dependency; `#![forbid(unsafe_code)]`; full gauntlet green; traceability | T4 | regression gate (`cluster::crossrange_2pc`, `cluster::jepsen_bank` cross-range, `crabgresql::crossrange_2pc_net`/`crossrange_2pc_nemesis`); full gauntlet (`cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo nextest run --workspace`; `cargo test --workspace --doc`; `cargo deny check` accepts `arc-swap`); UAC guard; this table |
