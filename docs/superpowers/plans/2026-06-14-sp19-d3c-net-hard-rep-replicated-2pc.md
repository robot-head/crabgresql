# SP19 / D3c-net-hard-rep — Cross-range 2PC over the replicated layout — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the fault-hardened cross-range 2PC + full recovery onto the replicated meta-range bring-up (`start_replicated`), which today passes `None`, and prove it with a multi-process e2e that conserves a cross-range bank total under a crash/partition nemesis on the replicated boot path AND across a full-cluster restart.

**Architecture:** Make `TxnService`'s engines map growable (`arc-swap`, lock-free reads / RCU writes). On `start_replicated`, install `Some(txn)` over `{0: engine0}` in Phase 1 (listener stays bound for range-0 Raft), then `register_engine` each data range live in Phase 2 and spawn the full recovery set (`release_on_leadership_loss`, `resolve_in_doubt_on_leadership`, `participant_silence_sweeper`) exactly as `start_static`. A `Stage` for an unregistered range returns retryable `NotLeader` so a mid-bootstrap participant is retried, not spuriously aborted. No 2PC-protocol or write-once-core change.

**Tech Stack:** Rust 2024, tokio, openraft, `arc-swap` (new dep), cargo-nextest. Spec: `docs/superpowers/specs/2026-06-14-crabgresql-sp19-d3c-net-hard-rep-replicated-2pc-design.md`.

**Reference anchors (read before starting):**
- Static reference wiring: `crates/cluster/src/server_node.rs::start_static` (lines ~198-309) — the exact `TxnService::new` + watcher-spawn pattern to mirror.
- The gap: `crates/cluster/src/server_node.rs::start_replicated` (lines ~401-519) — Phase-1 `serve_node_protocol(..., None)` at ~434-441; Phase-2 build loop ~468-491.
- `crates/cluster/src/twopc.rs` — `TxnService` (struct ~295-311), `engine`/`session_handle`/`stage`/`release` (~309-440), `#[cfg(test)] mod tests` (~455+).
- `crates/cluster/src/transport/server.rs::handle_txn` (~235-285) — the two `svc.engine(0)` call sites.
- E2e template: `crates/crabgresql/tests/crossrange_2pc_nemesis.rs` (SP18). Harness: `crates/crabgresql/tests/harness/mod.rs` — `spawn_multirange_replicated` (~150), `kill`/`respawn` (~404/410), `range_leader`, `exec_until_ok`, `pick_live_gateway`.

**Stale-IDE warning:** rust-analyzer squiggles in this repo lag the committed tree and are routinely wrong mid-edit. Trust `cargo build`/`clippy`/`nextest` only.

---

## Task 1: Growable engines registry (`arc-swap`) + retryable-`NotLeader` `Stage`

**Files:**
- Modify: `Cargo.toml` (workspace root — add `arc-swap` to `[workspace.dependencies]`)
- Modify: `crates/cluster/Cargo.toml` (add `arc-swap = { workspace = true }`)
- Modify: `crates/cluster/src/twopc.rs` (growable `engines`, `register_engine`, owned `engine`, `Stage` absent → `NotLeader`)
- Test: `crates/cluster/src/twopc.rs` `#[cfg(test)] mod tests`

- [ ] **Step 1: Add the `arc-swap` dependency**

In the workspace root `Cargo.toml`, under `[workspace.dependencies]` (keep the list alphabetically ordered if it is), add:
```toml
arc-swap = "1"
```
In `crates/cluster/Cargo.toml`, under `[dependencies]` (after `async-trait` to keep it tidy), add:
```toml
arc-swap = { workspace = true }
```

- [ ] **Step 2: Write the failing unit tests**

Add to `crates/cluster/src/twopc.rs`'s `#[cfg(test)] mod tests` (it already imports `TxnService`, `TwoPcClient`, `TxnRpc`, `TxnResp`, `parse_one`, and uses `crate::server_node::testonly_two_range_node`):
```rust
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn register_engine_makes_a_range_servable() {
        let (node, _sql) = crate::server_node::testonly_two_range_node().await;
        // Build a service holding ONLY range 0 (the replicated Phase-1 shape).
        let mut only0 = std::collections::HashMap::new();
        only0.insert(0u32, node.engines[&0].clone());
        let svc = TxnService::new(only0);
        assert!(svc.engine(1).is_none(), "range 1 absent before registration");
        svc.register_engine(1, node.engines[&1].clone());
        assert!(svc.engine(1).is_some(), "range 1 present after registration");
        // Idempotent re-register is fine.
        svc.register_engine(1, node.engines[&1].clone());
        assert!(svc.engine(1).is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stage_for_an_unregistered_range_is_retryable_not_a_hard_err() {
        let (node, _sql) = crate::server_node::testonly_two_range_node().await;
        let mut only0 = std::collections::HashMap::new();
        only0.insert(0u32, node.engines[&0].clone());
        let svc = TxnService::new(only0);
        // A well-formed Stage for a range this service does not host yet must be
        // RETRYABLE (the participant is mid-bootstrap), not a hard Err that would
        // make the coordinator abort the global txn.
        let resp = svc
            .handle(
                7,
                TxnRpc::Stage {
                    g: 1_000_000,
                    range: 7,
                    sql: "UPDATE b SET id = 1 WHERE id = 0".into(),
                },
            )
            .await;
        assert!(
            matches!(resp, TxnResp::NotLeader),
            "expected NotLeader for an unregistered range, got {resp:?}"
        );
    }
```
Note: `RangeId` is `u32` (the engines map keys are `u32`); use `0u32`/`1`/`7` literals as above.

- [ ] **Step 3: Run the tests — expect FAIL** (`register_engine` does not exist; `engine` returns `&Arc`; `Stage` on absent range returns `Err`).

Run: `cargo nextest run -p cluster -E 'test(register_engine_makes_a_range_servable) | test(stage_for_an_unregistered_range_is_retryable_not_a_hard_err)'`
Expected: compile error (no `register_engine`) / assertion failure.

- [ ] **Step 4: Make the engines map growable**

In `crates/cluster/src/twopc.rs`, add the import near the other `use`s at the top of the file:
```rust
use arc_swap::ArcSwap;
```
Change the `TxnService` struct (currently `engines: HashMap<RangeId, Arc<SqlEngine>>`):
```rust
#[derive(Clone)]
pub struct TxnService {
    engines: Arc<ArcSwap<HashMap<RangeId, Arc<SqlEngine>>>>,
    held: Arc<Mutex<HashMap<(u64, RangeId), HeldEntry>>>,
}
```
Update `new` to wrap the map, and change `engine` to return an OWNED `Arc` (lock-free load); add `register_engine`:
```rust
    pub fn new(engines: HashMap<RangeId, Arc<SqlEngine>>) -> Self {
        Self {
            engines: Arc::new(ArcSwap::from_pointee(engines)),
            held: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Look up the engine for `range`. Returns an OWNED `Arc` (lock-free snapshot via
    /// `arc-swap`); `None` if this node does not (yet) host that range.
    pub fn engine(&self, range: RangeId) -> Option<Arc<SqlEngine>> {
        self.engines.load().get(&range).cloned()
    }

    /// Register a data-range engine so this node can serve `Stage`/`Release` for it.
    /// Copy-on-write (`rcu`) so concurrent lock-free readers never block; idempotent
    /// (re-registering replaces the entry). Used by the replicated bring-up to add data
    /// ranges live as they come online (the `rcu` closure receives `&Arc<HashMap>`).
    pub fn register_engine(&self, range: RangeId, engine: Arc<SqlEngine>) {
        self.engines.rcu(|cur| {
            let mut m = (**cur).clone();
            m.insert(range, engine.clone());
            m
        });
    }
```
Update `session_handle` to use the owned `engine` (it currently does `self.engines.get(&range)?.clone()`):
```rust
    async fn session_handle(&self, g: u64, range: RangeId) -> Option<HeldSession> {
        let engine = self.engine(range)?;
        let mut held = self.held.lock().await;
        Some(
            held.entry((g, range))
                .or_insert_with(|| HeldEntry {
                    session: Arc::new(Mutex::new(engine.connect())),
                    joined_at: tokio::time::Instant::now(),
                })
                .session
                .clone(),
        )
    }
```
In `stage`, change the absent-engine branch from a hard `Err` to retryable `NotLeader` (currently `return TxnResp::Err(format!("no engine for range {range}"))`):
```rust
        let Some(handle) = self.session_handle(g, range).await else {
            // This node does not host `range` yet (mid-bootstrap on the replicated
            // layout). RETRYABLE: the coordinator re-resolves rather than aborting.
            return TxnResp::NotLeader;
        };
```
Leave `release` unchanged (it already returns the idempotent `TxnResp::Released` no-op for an absent `(g, range)`).

- [ ] **Step 5: Fix the `engine()` call sites (owned `Arc` ripple)**

`crates/cluster/src/transport/server.rs::handle_txn` has two `match svc.engine(0) { Some(e) => … }` sites (BeginGlobal ~242, CommitGlobal ~250). These bind `Some(e)` and call `e.begin_global_durable()` / `e.commit_global_decision(...)` via `Deref`, so they compile unchanged against an owned `Option<Arc<SqlEngine>>`. Run `cargo build -p cluster --all-targets` and fix any other caller the compiler flags (there should be none beyond these and `session_handle`).

- [ ] **Step 6: Run tests + verify**

Run:
- `cargo nextest run -p cluster -E 'test(register_engine_makes_a_range_servable) | test(stage_for_an_unregistered_range_is_retryable_not_a_hard_err)'` → PASS
- `cargo nextest run -p cluster` → no regressions (the static path uses `TxnService::new(complete_map)` and never calls `register_engine`; the SP18 `twopc::tests` must stay green)
- `cargo clippy -p cluster --all-targets -- -D warnings` → clean
- `cargo fmt --all`

- [ ] **Step 7: Commit**
```bash
git add Cargo.toml crates/cluster/Cargo.toml crates/cluster/src/twopc.rs
git commit -m "feat(sp19): growable TxnService engines registry (arc-swap) + retryable Stage

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Wire `Some(txn)` + full recovery onto `start_replicated`

**Files:**
- Modify: `crates/cluster/src/server_node.rs` (`start_replicated`)

This task has no new unit test — the replicated bring-up requires a multi-process cluster, so its behavioral proof is the Task 3 e2e. Verification here is: the static path stays green, the crate builds, clippy is clean.

- [ ] **Step 1: Phase 1 — build `TxnService` over `{0}` and install `Some(txn)`**

In `start_replicated`, after `engines.insert(0, r0_engine);` (~line 428) and BEFORE the `bind_with_retry`, build the service. Replace the existing Phase-1 listener block (the "future work" comment + `bind_with_retry` + `serve_node_protocol(..., None)` at ~430-441) with:
```rust
        // The node listener binds with only range 0 ready, but it DOES host the 2PC
        // service from the start: `TxnService`'s engines map is growable, so global/
        // coordinator ops (Begin/Commit/GlobalBarrier — they need only engine(0)) work
        // immediately, and data ranges are registered live in Phase 2. The listener
        // must stay bound throughout Phase 2 to serve range-0 Raft RPCs.
        let txn = crate::twopc::TxnService::new(engines.clone());
        let node_listener = bind_with_retry(&cfg.node_addr).await?;
        tokio::spawn(serve_node_protocol(
            node_listener,
            registry.clone(),
            partition.clone(),
            shutdown.clone(),
            Some(txn.clone()),
        ));
```
(`txn` is kept for Phase 2; `TxnService` is `Clone` and shares both the held-map and the growable engines cell, so the installed clone sees Phase-2 registrations.)

- [ ] **Step 2: Phase 2 — register each data range + a sweep client**

In the Phase-2 barrier-injection loop (currently ~482-491, `for (range, raft, sm_kv, mut engine) in pending`), the engines are Arc-wrapped. Add a `sweep_client` alongside `barrier_client` (just before the loop, ~481) and, inside the loop after `let engine = Arc::new(engine);`, register the engine and spawn the leadership-rise sweep (mirroring `start_static` lines 242 + 252-257):
```rust
        let barrier_client = crate::twopc::TwoPcClient::new(rafts.clone(), partition.clone());
        // Cross-range recovery: the per-DATA-range client the leadership-rise sweep uses
        // to abort-race in-doubt `Prepared(-> g)` markers against range 0 (write-once).
        let sweep_client = crate::twopc::TwoPcClient::new(rafts.clone(), partition.clone());
        for (range, raft, sm_kv, mut engine) in pending {
            let barrier: Arc<dyn executor::Linearizer> = Arc::new(
                crate::twopc::Range0Barrier::new(rafts[&0].clone(), cfg.id, barrier_client.clone()),
            );
            engine.set_range0_barrier(barrier);
            let engine = Arc::new(engine);
            // Register this data range so the listener can serve Stage/Release for it.
            txn.register_engine(range, engine.clone());
            tokio::spawn(reseed_on_leadership(raft.clone(), engine.clone()));
            // On THIS data-range's leadership rising edge, finalize a failed-over
            // participant's durable in-doubt markers.
            tokio::spawn(resolve_in_doubt_on_leadership(
                raft.clone(),
                cfg.id,
                engine.clone(),
                sweep_client.clone(),
            ));
            sm_kvs.insert(range, sm_kv);
            engines.insert(range, engine);
        }
```

- [ ] **Step 3: Phase 2 — spawn the leadership-loss + silence-sweeper watchers**

After the barrier-injection loop completes (the `rafts` map is complete), and BEFORE the `spawn_sql_gateway` call (~496), add the remaining recovery watchers, mirroring `start_static` lines 268-282:
```rust
        // Per-range leadership-loss release: free held 2PC locks promptly when this
        // node loses a range's leadership. All watchers share the same Arc held-map.
        for (&range, raft) in &rafts {
            tokio::spawn(release_on_leadership_loss(
                raft.clone(),
                range,
                cfg.id,
                txn.clone(),
            ));
        }
        // Coordinator-silence recovery: a per-node heartbeat self-resolves held 2PC
        // sessions whose coordinator crashed after staging but before deciding.
        let sweeper_client = crate::twopc::TwoPcClient::new(rafts.clone(), partition.clone());
        tokio::spawn(participant_silence_sweeper(txn.clone(), sweeper_client));
```

- [ ] **Step 4: Verify**

Run:
- `cargo build -p cluster --all-targets` → clean
- `cargo nextest run -p cluster` → no regressions (static path + all SP16-18 cluster suites green)
- `cargo clippy -p cluster --all-targets -- -D warnings` → clean
- `cargo fmt --all`

Confirm by inspection that no `serve_node_protocol(..., None)` remains in `start_replicated` and that `txn` is moved/cloned correctly (the Phase-1 spawn takes `Some(txn.clone())`, Phase 2 uses `txn`).

- [ ] **Step 5: Commit**
```bash
git add crates/cluster/src/server_node.rs
git commit -m "feat(sp19): wire 2PC service + full recovery onto the replicated bring-up

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Multi-process replicated e2e (crash nemesis + full-cluster restart)

**Files:**
- Create: `crates/crabgresql/tests/crossrange_2pc_replicated.rs` (UAC-safe filename; reuses `mod harness;`)

- [ ] **Step 1: Write the e2e**

Model it on `crates/crabgresql/tests/crossrange_2pc_nemesis.rs` (the SP18 test). Two differences: boot via `Cluster::spawn_multirange_replicated(5, vec![2])`, and after the nemesis + conservation + recovery, add a full-cluster restart round. Reuse the SP18 module-local helpers verbatim (`Lcg`, `connect`, `cross_transfer`, `read_total_cross`, `first_i64`, `ctl_set_partition`, `ctl_heal`) — copy them from `crossrange_2pc_nemesis.rs`.
```rust
//! SP19 D3c-net-hard-rep: cross-range 2PC over the REPLICATED meta-range layout.
//! Boots via the replicated descriptor path (not the static seed), conserves a
//! cross-range bank total under a multi-process crash/partition nemesis that kills
//! mid-transaction coordinators, AND survives a full-cluster restart (descriptor blob
//! + durable 2PC state re-read).
mod harness;
use harness::Cluster;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_cross_range_bank_conserves_under_nemesis_and_restart() {
    const ACCOUNTS: i64 = 4;
    const SEED: i64 = 100;
    const PROCS: usize = 3;
    const OPS: usize = 8;
    const MIN_ROUNDS: usize = 4;
    let seeded_total = 2 * ACCOUNTS * SEED;

    // 5 nodes, boundary [2], REPLICATED bring-up: every node learns the {acct_a->range 0,
    // acct_b->range 1} layout from the meta range. 5 nodes / 2 ranges keep a quorum when
    // a non-leader is faulted.
    let mut c = Cluster::spawn_multirange_replicated(5, vec![2]).await;
    let committed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    // Seed via exec_until_ok (bounded retry; the gateway may lag quorum at bring-up).
    c.exec_until_ok("CREATE TABLE acct_a (id int8, bal int8)").await;
    c.exec_until_ok("CREATE TABLE acct_b (id int8, bal int8)").await;
    for id in 0..ACCOUNTS {
        c.exec_until_ok(&format!("INSERT INTO acct_a VALUES ({id}, {SEED})")).await;
        c.exec_until_ok(&format!("INSERT INTO acct_b VALUES ({id}, {SEED})")).await;
    }

    // Workers spread one-per-node across nodes 0..PROCS (each pins to its own gateway),
    // so a coordinator is often a non-leading gateway the nemesis kills mid-txn.
    let addrs: Vec<String> = (0..c.len())
        .map(|i| c.sql_addr(i as u64).to_string())
        .collect();
    let mut workers = Vec::new();
    for process in 0..PROCS {
        let addrs = addrs.clone();
        let sig = committed.clone();
        workers.push(tokio::spawn(async move {
            use std::sync::atomic::Ordering;
            let mut rng = Lcg::new(0x9E37_79B9_u64.wrapping_mul(process as u64 + 1));
            let mut n = 0usize;
            for _ in 0..OPS {
                let node = addrs[process % addrs.len()].clone();
                let Some(client) = connect(&node).await else { continue };
                let from = (rng.next() % ACCOUNTS as u64) as i64;
                let mut to = (rng.next() % ACCOUNTS as u64) as i64;
                if to == from { to = (to + 1) % ACCOUNTS; }
                let amt = 1 + (rng.next() % 20) as i64;
                if cross_transfer(&client, from, to, amt).await {
                    n += 1;
                    sig.fetch_add(1, Ordering::Relaxed);
                }
            }
            n
        }));
    }

    // Nemesis: fault a non-leader victim only (keep quorum on both ranges), paced on a
    // committed-op progress signal, awaiting recovered quorum before the next fault.
    use std::sync::atomic::Ordering;
    let mut round = 0usize;
    while !workers.iter().all(|w| w.is_finished()) || round < MIN_ROUNDS {
        let l0 = c.range_leader(0).await;
        let l1 = c.range_leader(1).await;
        let victim = (0..c.len() as u64).find(|&i| i != l0 && i != l1).expect("a non-leader exists");
        let before = committed.load(Ordering::Relaxed);
        if round.is_multiple_of(2) {
            c.kill(victim).await;
            c.respawn(victim);
        } else {
            let others: Vec<u64> = (0..c.len() as u64).filter(|&i| i != victim).collect();
            let _ = c.control(victim, ctl_set_partition(others.clone())).await;
            for &o in &others { let _ = c.control(o, ctl_set_partition(vec![victim])).await; }
            for id in 0..c.len() as u64 { let _ = c.control(id, ctl_heal()).await; }
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while committed.load(Ordering::Relaxed) == before
            && !workers.iter().all(|w| w.is_finished())
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(100)).await; // bounded poll cadence
        }
        c.range_leader(0).await;
        c.range_leader(1).await;
        round += 1;
    }
    let mut total_committed = 0usize;
    for w in workers { total_committed += w.await.expect("worker"); }

    // Heal; await leaders; recovery-required all-pairs round (proves no lock stranded).
    for id in 0..c.len() as u64 { let _ = c.control(id, ctl_heal()).await; }
    c.range_leader(0).await;
    c.range_leader(1).await;
    for id in 0..ACCOUNTS {
        let other = (id + 1) % ACCOUNTS;
        c.exec_until_ok(&format!(
            "BEGIN; UPDATE acct_a SET bal = bal - 0 WHERE id = {id}; UPDATE acct_b SET bal = bal + 0 WHERE id = {other}; COMMIT"
        )).await;
    }

    // Conservation after the nemesis.
    {
        let gw = c.pick_live_gateway().await;
        let reader = c.pg(gw).await;
        let total = read_total_cross(&reader, ACCOUNTS).await;
        assert_eq!(total, seeded_total,
            "replicated cross-range transfers conserve the total under the nemesis (got {total}, want {seeded_total})");
    }
    assert!(total_committed > 0, "the workload must commit at least one transfer (non-vacuous)");

    // FULL-CLUSTER RESTART: stop every node, then respawn every node. Each replicated
    // node re-reads the immutable descriptor blob via wait_for_range_map and recovers
    // its durable 2PC state; the leadership-rise sweep finalizes any txn left in-doubt.
    for id in 0..c.len() as u64 { c.kill(id).await; }
    for id in 0..c.len() as u64 { c.respawn(id); }
    c.range_leader(0).await;
    c.range_leader(1).await;

    // Post-restart recovery-required round: an all-pairs amount-0 transfer must commit
    // (a lock stranded across the restart would block exec_until_ok to its deadline).
    for id in 0..ACCOUNTS {
        let other = (id + 1) % ACCOUNTS;
        c.exec_until_ok(&format!(
            "BEGIN; UPDATE acct_a SET bal = bal - 0 WHERE id = {id}; UPDATE acct_b SET bal = bal + 0 WHERE id = {other}; COMMIT"
        )).await;
    }

    // Conservation STILL holds after the full restart.
    let gw = c.pick_live_gateway().await;
    let reader = c.pg(gw).await;
    let total = read_total_cross(&reader, ACCOUNTS).await;
    assert_eq!(total, seeded_total,
        "the descriptor blob + durable 2PC state survive a full-cluster restart (got {total}, want {seeded_total})");
}
```
Then paste the module-local helpers from `crossrange_2pc_nemesis.rs` (everything below its test fn: `Lcg`, `connect`, `cross_transfer`, `read_total_cross`, `first_i64`, `ctl_set_partition`, `ctl_heal`) verbatim.

- [ ] **Step 2: Run it — iterate to green**

Run: `cargo nextest run -p crabgresql --test crossrange_2pc_replicated` (2-3× to confirm non-flaky). The replicated boot adds a ~60s `boot_timeout` window; nextest's concurrency groups serialize the multi-process suites. If a worker hangs, confirm `connect`/`cross_transfer` use bounded `tokio::time::timeout` (copied from the SP18 helpers). If the restart's `exec_until_ok` times out, the recovery path is not firing post-restart — confirm `start_replicated` spawns the full watcher set (Task 2) and that `respawn` re-reads the durable state. Do NOT add a settle-sleep; do NOT weaken the conservation/restart assertions.

- [ ] **Step 3: Run the whole crate + guards + commit**

Run:
- `cargo nextest run -p crabgresql` → no regressions (multiprocess, jepsen_elle, multirange_gateway, meta_range_gateway, crossrange_2pc_net, crossrange_2pc_nemesis all green)
- `cargo clippy -p crabgresql --all-targets -- -D warnings` → clean; `cargo fmt --all`
- UAC guard: `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` → empty
```bash
git add crates/crabgresql/tests/crossrange_2pc_replicated.rs
git commit -m "test(sp19): multi-process replicated-layout cross-range bank — nemesis + full-cluster restart

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Gauntlet + traceability + CLAUDE.md + finish

**Files:**
- Modify: `CLAUDE.md` (SP19 UAC audit line)
- Modify: `docs/superpowers/specs/2026-06-14-crabgresql-sp19-d3c-net-hard-rep-replicated-2pc-design.md` (fill the Traceability section)

- [ ] **Step 1: UAC guard** — `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` (expect empty). Confirm no new `[[test]]/[[bin]]` names with forbidden substrings (`crossrange_2pc_replicated` is clean).

- [ ] **Step 2: Add the SP19 line to CLAUDE.md** (after the SP18 line):
```markdown
**SP19 (2026-06-14):** one new binary — `crabgresql::crossrange_2pc_replicated` (multi-process cross-range 2PC over the replicated meta-range layout, nemesis + full-cluster restart) — UAC-safe (no `setup/install/update/patch/upgrad` substring). The crabgresql list now reads `{crossrange_2pc_net, crossrange_2pc_nemesis, crossrange_2pc_replicated, jepsen_elle, meta_range_gateway, multiprocess, multirange_gateway}`. SP19 added the `arc-swap` dependency (growable `TxnService` engines registry) and no test target with a forbidden substring; the full guard returns empty.
```

- [ ] **Step 3: Fill the Traceability section** in the SP19 spec, mapping each success criterion (1-7) → task → proving test (e.g. criterion 2 → T1 `register_engine_makes_a_range_servable`; 4 → T1 `stage_for_an_unregistered_range_is_retryable_not_a_hard_err`; 5/6 → T3 `replicated_cross_range_bank_conserves_under_nemesis_and_restart`; 7 → T4 gauntlet).

- [ ] **Step 4: Full gauntlet** (all must be green):
```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
cargo test --workspace --doc
cargo deny check
```
`cargo deny check` MUST accept `arc-swap` (MIT/Apache, advisory-free). If `deny.toml` has a license/source allowlist that rejects it, add the minimal allowance and note it in the commit. If `cargo fmt --all --check` reports diffs, run `cargo fmt --all` and re-commit.

- [ ] **Step 5: Commit**
```bash
git add CLAUDE.md docs/superpowers/specs/2026-06-14-crabgresql-sp19-d3c-net-hard-rep-replicated-2pc-design.md
git commit -m "docs(sp19): traceability table + CLAUDE.md UAC audit for crossrange_2pc_replicated

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 6: Finish the branch** — superpowers:finishing-a-development-branch, option 2 (push fresh non-force branch + PR against `main`). This branch is stacked on SP18 (PR #34); if #34 has merged, rebase `--onto origin/main <sp18-tip>` first so the PR diff is SP19-only. PR body ends with the Claude Code generated-with line.

---

## Notes for the implementer

- **Stale IDE diagnostics:** trust `cargo build`/`clippy`/`nextest`, never the editor.
- **No `sleep` in tests:** the only `tokio::time::sleep` is the SP18 nemesis's bounded poll cadence (gated by the committed-op signal + `workers.is_finished()` + a deadline) — keep it exactly. The restart round waits on `range_leader` (a real condition) and `exec_until_ok` (bounded retry), never a fixed sleep. Setup DDL uses `exec_until_ok`.
- **`arc-swap` discipline:** `engine()` loads an OWNED snapshot — there is no guard to hold across `await`. `register_engine` uses `rcu` (copy-on-write). Do not introduce a `std` lock on the engines map.
- **The static path is unchanged** except the `engine()` owned-return ripple. The SP16/SP17/SP18 `crossrange_2pc` / `crossrange_2pc_net` / `crossrange_2pc_nemesis` / `jepsen_bank` cross-range suites are the regression guard — they must stay green at every task.
- **`respawn` re-reads durable state:** a replicated node respawned with `bootstrap=false` skips `seed_if_absent` and reads the committed descriptor blob via `wait_for_range_map`; durable `Prepared` markers + `clog` decisions are recovered by the range engines, and `resolve_in_doubt_on_leadership` finalizes any in-doubt `g` on the post-restart leadership rise.
