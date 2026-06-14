# SP18 / D3c-net-hard — Fault-hardened cross-range 2PC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make SP17's cross-range 2PC survive real coordinator crashes and participant-leader failovers — no transaction strands locks forever, no in-doubt transaction stays invisible forever — proven by a multi-process crash/partition-nemesis cross-range bank that conserves its total.

**Architecture:** A **write-once global decision** (first-writer-wins on `clog[g]`, enforced in the deterministic state-machine apply, with an effective-decision read-back) makes participant abort-races safe. A stranded participant **self-resolves** by writing `Aborted(g)` itself via the existing `CommitGlobal{commit:false}` RPC and honoring the effective decision it gets back. Two triggers drive self-resolve: a coordinator-silence timeout on held sessions (alive participant) and a durable leadership-rise sweep over `Prepared(→g)` markers (failed-over participant). The global clog stays the sole arbiter — no durable coordinator record.

**Tech Stack:** Rust 2024; the `mvcc`/`executor`/`cluster`/`kv` crates; openraft (deterministic per-range apply; the metrics-watch leadership edges; the SP17 range-0 read barrier); the SP9 multi-process harness. No new shipped dependency. `#![forbid(unsafe_code)]` unchanged. cargo-nextest; doctests via `cargo test --workspace --doc`.

**Branch:** `sp18-d3c-net-hard-crossrange-recovery` (created, stacked on the SP17 branch tip `0b92182`). Diff against `origin/main`; rebase `--onto origin/main` after SP17 (PR #33) squash-merges.

---

## Locked design decisions (from brainstorming + the anchor map)

1. **Recovery = participant self-resolve, no durable `/0/txn/<g>` record.** The global clog is the sole arbiter.
2. **Crash-nemesis proof = multi-process** (real OS-process kills).
3. **Scope = post-prepare recovery only.** A leader move *during* staging still surfaces a retryable abort (SP17 behavior). Mid-txn re-stage → a later slice.

**Internal decisions (locked, from the anchor map):**
- **Write-once via Option A — a clog-key-aware conditional `apply` arm, no new `WriteOp` variant.** Mirrors the existing `is_counter_key` max-merge. Lands in **both** state machines: `crates/cluster/src/store.rs` (`apply_op`) and `crates/cluster/src/durable.rs` (`apply`, with an intra-batch `decided` map mirroring its `counters` map). Keeps the replicated Raft-log format stable.
- **"Write-once" = keep an existing TERMINAL decision (`Committed`/`Aborted`); overwrite anything else.** Safe for every existing clog write (a local xid is decided once; a `Prepared(→g)` marker is non-terminal and may be re-stamped with the same value) — only the cross-`g` abort-race (participant `Aborted` vs coordinator `Committed`) is affected, which is exactly the race we serialize.
- **The effective decision is read back with `mvcc::clog::get(self.kv, g)`** after `commit_global_decision`'s `committer.commit` returns (RaftCommitter = committed-to-majority-AND-applied-on-leader). No new committer API.
- **`resolve_in_doubt(g)` = abort-race write, not a read.** A stranded participant sends `CommitGlobal{g, commit:false}` to range 0's leader (via the existing `TwoPcClient`); the write-once handler returns the **effective** decision (`Committed` if a coordinator already won, `Aborted` if the participant won); the participant then `commit_release`/`abort_release` accordingly. No separate read RPC.
- **`commit_global` returns the effective decision** (`Result<bool, ExecError>`, true = committed). `finish_txn` releases participants and reports `COMMIT`/`ROLLBACK` per the *returned* decision, never its intended one (coordinator honesty under the race).
- **Two sweepers, both via the abort-race RPC:** (T3) a bounded per-node timeout sweeper over aged held sessions; (T4) a per-range leadership-rise watcher over durable `Prepared(→g)` markers. The write-once decision makes double-resolution idempotent.
- **Two seams to add:** `pub fn kv::key::clog_prefix()` (clog isn't prefix-scannable today) and `pub fn SqlEngine::in_doubt_globals()` (the engine's stores are `pub(crate)` to executor; the cluster-crate sweep needs a public scan seam).

---

## File structure (what changes, and why)

**New files:**
- `crates/crabgresql/tests/crossrange_2pc_nemesis.rs` — the multi-process crash/partition-nemesis cross-range bank (UAC-safe name).

**Modified files:**
- `crates/mvcc/src/clog.rs` — `pub fn is_terminal(value: &[u8]) -> bool` (decode the status byte) for the apply check.
- `crates/kv/src/key.rs` — `pub fn clog_prefix() -> Vec<u8>` + `pub fn clog_xid_of(key: &[u8]) -> Option<u64>`.
- `crates/cluster/src/store.rs` — `pub(crate) fn is_clog_key(key: &[u8]) -> bool`; the write-once arm in `apply_op`.
- `crates/cluster/src/durable.rs` — the write-once arm in `apply` (+ intra-batch `decided` map).
- `crates/executor/src/lib.rs` — `commit_global_decision` returns `Result<XidStatus, ExecError>` (read-back); `pub async fn in_doubt_globals(&self) -> Result<Vec<u64>, ExecError>`.
- `crates/cluster/src/range/router.rs` — `GlobalCoordinator::commit_global -> Result<bool, ExecError>`; `LocalCoordinator` honors it; `finish_txn` reports per the effective decision.
- `crates/cluster/src/transport/protocol.rs` — `TxnResp::Aborted` variant (the effective decision on the wire).
- `crates/cluster/src/transport/server.rs` — `handle_txn` `CommitGlobal` returns the effective decision (`Committed`/`Aborted`).
- `crates/cluster/src/twopc.rs` — `NetCoordinator::commit_global` maps `Aborted`; `HeldEntry { session, joined_at }`; the per-node timeout sweeper; a `resolve_in_doubt` helper.
- `crates/cluster/src/server_node.rs` — spawn the timeout sweeper + the per-range leadership-rise sweep watcher (static path).
- `CLAUDE.md` — SP18 UAC audit line.

---

## Task 1: Write-once global decision (conditional apply in both state machines + effective-decision read-back)

**Files:**
- Modify: `crates/mvcc/src/clog.rs` (add `is_terminal` after `put_op`, ~line 61)
- Modify: `crates/cluster/src/store.rs` (add `is_clog_key` next to `is_counter_key` ~line 62; the write-once arm in `apply_op` ~line 52)
- Modify: `crates/cluster/src/durable.rs` (the write-once arm in `apply` ~line 596; import the helper ~line 419)
- Modify: `crates/executor/src/lib.rs` (`commit_global_decision` return type + read-back, lib.rs:240-257)
- Test: `crates/cluster/src/store.rs` inline `#[cfg(test)]`; `crates/cluster/tests/crossrange_2pc.rs` (engine-level read-back) or an executor test

- [ ] **Step 1: Write the failing test (in-memory write-once apply)**

Add to `crates/cluster/src/store.rs`'s test module (find `#[cfg(test)] mod tests`):
```rust
    #[test]
    fn clog_decision_is_write_once_first_writer_wins() {
        use mvcc::clog::{get as clog_get, put_op, XidStatus};
        use mvcc::xid::GLOBAL_XID_BASE;
        let kv = MemKv::new();
        let g = GLOBAL_XID_BASE + 9;

        // First terminal decision: Aborted. apply_op keeps it write-once.
        apply_op(&kv, &put_op(g, XidStatus::Aborted));
        assert_eq!(clog_get(&kv, g).expect("get"), XidStatus::Aborted);

        // A LATER, DIFFERENT terminal decision (Committed) must NOT overwrite it.
        apply_op(&kv, &put_op(g, XidStatus::Committed));
        assert_eq!(clog_get(&kv, g).expect("get"), XidStatus::Aborted,
            "first terminal decision wins; a contending Committed is dropped");

        // A non-terminal write (Prepared) for a DIFFERENT xid is unaffected.
        let li = 7u64;
        apply_op(&kv, &put_op(li, XidStatus::Prepared(g)));
        assert_eq!(clog_get(&kv, li).expect("get"), XidStatus::Prepared(g));
    }
```
(`apply_op`, `MemKv`, and the test module already exist in `store.rs`. `MemKv::new()` + `apply_op(&kv, &op)` is the in-memory apply primitive.)

- [ ] **Step 2: Run it — expect FAIL**

Run: `cargo nextest run -p cluster --lib store::tests::clog_decision_is_write_once_first_writer_wins`
Expected: FAIL — today `apply_op`'s plain `Put` arm is last-writer-wins, so the second `apply_op` overwrites `Aborted` with `Committed`.

- [ ] **Step 3: Add the `is_terminal` decode helper to `mvcc::clog`**

In `crates/mvcc/src/clog.rs`, after `put_op` (~line 61, before `#[cfg(test)]`):
```rust
/// True iff `value` (a clog entry's bytes) encodes a TERMINAL decision
/// (Committed/Aborted) — the statuses the write-once global decision must keep.
/// `Prepared`/`InProgress`/empty are non-terminal.
pub fn is_terminal(value: &[u8]) -> bool {
    matches!(value.first(), Some(&S_COMMITTED) | Some(&S_ABORTED))
}
```
(`S_COMMITTED`/`S_ABORTED` are the existing private constants at clog.rs:17-18 — `is_terminal` is in the same module so it can read them.)

- [ ] **Step 4: Add `is_clog_key` + the write-once arm to the in-memory apply (`store.rs`)**

Next to `is_counter_key` (~store.rs:62) add:
```rust
/// True for any `/0/clog/<xid>` key (the commit-status log). Decision writes to
/// these are WRITE-ONCE: a terminal status is never overwritten (see `apply_op`).
pub(crate) fn is_clog_key(key: &[u8]) -> bool {
    let prefix = kv::key::clog_prefix();
    key.len() >= prefix.len() && key[..prefix.len()] == prefix[..]
}
```
(`clog_prefix` is added in Task 4 Step 3 — for Task 1 add it now too: `pub fn clog_prefix() -> Vec<u8>` in `crates/kv/src/key.rs`, see Task 4; it is `system_prefix("clog")`.)

In `apply_op` (store.rs:40-60), insert a new arm BETWEEN the `is_counter_key` arm and the plain `Put` arm:
```rust
        WriteOp::Put { key, value } if is_clog_key(key) => {
            // Write-once: keep an existing TERMINAL decision (Committed/Aborted);
            // otherwise (absent / InProgress / Prepared) apply the incoming value.
            let keep = kv
                .get(key)
                .expect("memkv get")
                .is_some_and(|b| mvcc::clog::is_terminal(&b));
            if !keep {
                kv.put(key.clone(), value.clone()).expect("memkv put");
            }
        }
```

- [ ] **Step 5: Add `clog_prefix` to `kv::key` (so `is_clog_key` compiles)**

In `crates/kv/src/key.rs`, after `clog_key` (~line 84):
```rust
/// The shared prefix of every `/0/clog/<xid>` entry (for prefix scans + the
/// write-once apply check). `clog_key(x)` is `clog_prefix() ++ put_u64(x)`.
pub fn clog_prefix() -> Vec<u8> {
    system_prefix("clog")
}
```

- [ ] **Step 6: Run the in-memory test — expect PASS; then add the durable apply arm**

Run: `cargo nextest run -p cluster --lib store::tests::clog_decision_is_write_once_first_writer_wins` → PASS.

In `crates/cluster/src/durable.rs` `apply` (durable.rs:555-629), the op loop folds ops into one `batch`. Add a write-once arm mirroring the `counters` intra-batch defense. Import the helper at durable.rs:419 (`use crate::store::{… is_clog_key …};`). Before the loop, add `let mut decided: HashMap<Vec<u8>, bool> = HashMap::new();` next to `counters`. Insert the arm between the counter-key arm and the plain `Put`:
```rust
                            WriteOp::Put { key, value } if is_clog_key(key) => {
                                // Write-once across THIS apply AND the durable value
                                // (mirrors the `counters` pending-map defense).
                                let already_terminal = match decided.get(key) {
                                    Some(&t) => t,
                                    None => self
                                        .data
                                        .get(key)
                                        .map_err(|e| StorageIOError::write_state_machine(&e))?
                                        .is_some_and(|b| mvcc::clog::is_terminal(&b)),
                                };
                                if !already_terminal {
                                    batch.insert(&self.data, key, value);
                                    decided.insert(key.clone(), mvcc::clog::is_terminal(value));
                                } else {
                                    // keep existing terminal; record it stays terminal
                                    decided.insert(key.clone(), true);
                                }
                            }
```
(Confirm the durable store imports `mvcc::clog` or use the fully-qualified `mvcc::clog::is_terminal`. `is_clog_key` must be re-exported from `store.rs` — it is `pub(crate)`, same crate, so the `use crate::store::is_clog_key` works.)

- [ ] **Step 7: `commit_global_decision` returns the effective decision (read-back)**

In `crates/executor/src/lib.rs`, change `commit_global_decision` (lib.rs:240-257):
```rust
    pub async fn commit_global_decision(
        &self,
        g: u64,
        status: mvcc::clog::XidStatus,
    ) -> Result<mvcc::clog::XidStatus, ExecError> {
        let gtm = self
            .gtm
            .as_ref()
            .expect("commit_global_decision on a non-GTM engine");
        self.committer
            .commit(vec![mvcc::clog::put_op(g, status), gtm.next_global_xid_op()])
            .await?;
        // Write-once: the apply keeps any prior terminal decision, so the EFFECTIVE
        // decision (what is actually recorded) may differ from `status` if a
        // participant won an abort-race. `commit` guarantees applied-on-leader, and
        // `self.kv` is range 0's applied store, so this read-back is authoritative.
        Ok(mvcc::clog::get(self.kv.as_ref(), g)?)
    }
```
This changes the return type. Update the two callers (they currently `?` and discard): `LocalCoordinator::commit_global` (router.rs:67-76) and `handle_txn`'s `CommitGlobal` arm (server.rs:250-267) — both handled in Task 2. For Task 1's compile, temporarily adapt them to `let _eff = self.range0.commit_global_decision(g, status).await?;` (router) and `match e.commit_global_decision(g, status).await { Ok(_eff) => { … } … }` (server) — Task 2 makes them honor `_eff`.

- [ ] **Step 8: Add the engine-level read-back test + run all**

Add to `crates/cluster/tests/crossrange_2pc.rs` (in-process, has a GTM-bearing range-0 engine via `MultiRangeCluster`):
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_decision_is_write_once_and_returns_effective() {
    use mvcc::clog::XidStatus;
    let c = two_range_cluster().await; // existing helper: MultiRangeCluster + RangeRouter
    let e = c.leader_engine(0).await;  // GTM-bearing range-0 engine
    let g = e.begin_global_durable().await.expect("alloc g");
    // First decision Aborted; effective == Aborted.
    assert_eq!(e.commit_global_decision(g, XidStatus::Aborted).await.expect("decide"), XidStatus::Aborted);
    // A contending Committed loses; effective stays Aborted (write-once).
    assert_eq!(e.commit_global_decision(g, XidStatus::Committed).await.expect("decide"), XidStatus::Aborted,
        "first terminal decision wins; commit_global_decision returns the effective decision");
}
```
(Confirm `two_range_cluster()` / `c.leader_engine(0)` exist in `crossrange_2pc.rs`'s helpers — the SP16 tests use a `MultiRangeCluster`; adapt to the real constructor names. If `leader_engine` returns a `SqlEngine` by value, call methods on it directly.)

Run: `cargo nextest run -p cluster --lib store` , `cargo nextest run -p cluster --test crossrange_2pc`, `cargo nextest run -p executor`, `cargo nextest run -p cluster` (no regressions — esp. jepsen_bank cross-range conservation, which must still hold). `cargo clippy -p cluster -p executor -p mvcc -p kv --all-targets -- -D warnings`.

- [ ] **Step 9: Commit**

```bash
git add crates/mvcc/src/clog.rs crates/kv/src/key.rs crates/cluster/src/store.rs crates/cluster/src/durable.rs crates/executor/src/lib.rs crates/cluster/tests/crossrange_2pc.rs
git commit -m "feat(sp18): write-once global decision (conditional apply, both state machines) + effective read-back

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Coordinator honors the effective decision (abort-race honesty)

**Files:**
- Modify: `crates/cluster/src/transport/protocol.rs` (add `TxnResp::Aborted` after `Committed`, ~line 95; add it to the round-trip test `twopc.rs:470-517`)
- Modify: `crates/cluster/src/transport/server.rs` (`handle_txn` `CommitGlobal` returns the effective decision, server.rs:250-267)
- Modify: `crates/cluster/src/range/router.rs` (`GlobalCoordinator::commit_global -> Result<bool, ExecError>`; `LocalCoordinator` impl; `finish_txn` honors it, router.rs:46/67/426-463)
- Modify: `crates/cluster/src/twopc.rs` (`NetCoordinator::commit_global` maps `Aborted`, twopc.rs:247-257)
- Test: `crates/cluster/tests/crossrange_2pc.rs` (coordinator reports ROLLBACK when a participant pre-aborted)

- [ ] **Step 1: Write the failing test (coordinator honesty under a pre-written abort)**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_reports_rollback_when_decision_already_aborted() {
    use mvcc::clog::XidStatus;
    let c = two_range_cluster().await;
    let mut router = RangeRouter::connect(&c).await;
    router.simple("CREATE TABLE a (id int4)").await.expect("a"); // range 0
    router.simple("CREATE TABLE b (id int4)").await.expect("b"); // range 1
    router.simple("BEGIN").await.expect("begin");
    router.simple("INSERT INTO a VALUES (1)").await.expect("stage a");
    router.simple("INSERT INTO b VALUES (2)").await.expect("escalate b");

    // Simulate a participant winning the abort-race: pre-write Aborted(g) for the
    // in-flight global xid via the range-0 engine BEFORE the coordinator commits.
    let g = router.staged_global_xid().expect("a global txn is staged"); // test accessor (SP16)
    let e = c.leader_engine(0).await;
    e.commit_global_decision(g, XidStatus::Aborted).await.expect("participant aborts g");

    // The coordinator's COMMIT must observe the effective Aborted and report ROLLBACK,
    // NOT a false COMMIT. Both rows must be invisible.
    let tag = router.simple("COMMIT").await.expect("commit returns a tag");
    assert!(format!("{tag:?}").contains("ROLLBACK"),
        "coordinator that lost the abort-race reports ROLLBACK, got {tag:?}");
    let a = scan_i32(&mut router, "SELECT id FROM a").await;
    let b = scan_i32(&mut router, "SELECT id FROM b").await;
    assert!(a.is_empty() && b.is_empty(), "an aborted cross-range txn leaves neither row");
}
```
(Uses the SP16 test accessors `staged_global_xid()` + the `scan_i32` helper already in `crossrange_2pc.rs`; confirm their exact names. The `COMMIT` result tag is a `QueryResult::Command { tag }` — match on the real shape.)

- [ ] **Step 2: Run it — expect FAIL** (today `commit_global` ignores the effective decision and reports COMMIT, inserting both rows).

Run: `cargo nextest run -p cluster --test crossrange_2pc coordinator_reports_rollback_when_decision_already_aborted`

- [ ] **Step 3: Add `TxnResp::Aborted` (the effective decision on the wire)**

In `crates/cluster/src/transport/protocol.rs` `TxnResp` (~line 90, after `Committed`):
```rust
    /// The global decision for `g` is Aborted (the coordinator or a participant
    /// lost the write-once abort-race). The caller releases with abort semantics.
    Aborted,
```
Add `TxnResp::Aborted` to the round-trip serde test array (`twopc.rs:470-517`).

- [ ] **Step 4: `handle_txn` CommitGlobal returns the effective decision**

In `crates/cluster/src/transport/server.rs` (server.rs:250-267), replace the `Ok(())` arm to map the effective decision:
```rust
                match e.commit_global_decision(g, status).await {
                    Ok(effective) => {
                        e.finish_global(g); // prune g from in-memory running set
                        match effective {
                            mvcc::clog::XidStatus::Committed => TxnResp::Committed,
                            mvcc::clog::XidStatus::Aborted => TxnResp::Aborted,
                            // A decision read-back can only be terminal here.
                            other => TxnResp::Err(format!("non-terminal effective decision {other:?}")),
                        }
                    }
                    Err(executor::ExecError::NotLeader) => TxnResp::NotLeader,
                    Err(e) => TxnResp::Err(format!("{e:?}")),
                }
```

- [ ] **Step 5: `GlobalCoordinator::commit_global -> Result<bool, ExecError>` + both impls**

In `crates/cluster/src/range/router.rs`, change the trait method (router.rs:46):
```rust
    /// Write the single global decision and return the EFFECTIVE outcome
    /// (true = committed, false = aborted — e.g. a participant won the abort-race).
    async fn commit_global(&self, g: u64, commit: bool) -> Result<bool, ExecError>;
```
`LocalCoordinator::commit_global` (router.rs:67-76):
```rust
    async fn commit_global(&self, g: u64, commit: bool) -> Result<bool, ExecError> {
        let status = if commit { mvcc::clog::XidStatus::Committed } else { mvcc::clog::XidStatus::Aborted };
        let effective = self.range0.commit_global_decision(g, status).await?;
        self.range0.finish_global(g);
        Ok(matches!(effective, mvcc::clog::XidStatus::Committed))
    }
```
`NetCoordinator::commit_global` (twopc.rs:247-257):
```rust
    async fn commit_global(&self, g: u64, commit: bool) -> Result<bool, ExecError> {
        match self.client.call(0, TxnRpc::CommitGlobal { g, commit }).await {
            Ok(TxnResp::Committed) => Ok(true),
            Ok(TxnResp::Aborted) => Ok(false),
            Ok(TxnResp::NotLeader) => Err(ExecError::NotLeader),
            _ => Err(ExecError::Unavailable),
        }
    }
```

- [ ] **Step 6: `finish_txn` reports per the effective decision**

In `crates/cluster/src/range/router.rs` `finish_txn`'s `Pin::Global` arm (router.rs:426-463), replace the `coord.commit_global(g, commit).await?;` line + the release loop so the *returned* decision drives both the per-participant release semantics AND the client-facing tag:
```rust
        let committed = coord.commit_global(g, commit).await?; // the EFFECTIVE decision
        for r in &ranges {
            if self.engines.contains_key(r) && self.leads.leads(*r) {
                let s = self.session_mut(*r);
                if committed { s.commit_release() } else { s.abort_release() }
            } else {
                let _ = coord.release_remote(g, *r, committed).await; // best-effort
            }
        }
        Ok(QueryResult::Command {
            tag: if committed { "COMMIT".into() } else { "ROLLBACK".into() },
        })
```
(`commit` here was `matches!(stmt, Statement::Commit)`. A client `ROLLBACK` passes `commit=false` → `coord.commit_global(g,false)` writes/keeps `Aborted` → `committed=false` → `ROLLBACK` tag. A client `COMMIT` that loses the race → `committed=false` → honest `ROLLBACK`. Keep the `#[cfg(test)] before_global_decision` seam call before `commit_global`.)

- [ ] **Step 7: Run + regressions**

Run: `cargo nextest run -p cluster --test crossrange_2pc` (incl. the new test + the SP16/SP17 atomic-commit/rollback tests), `cargo nextest run -p cluster --test gateway_local`, `cargo nextest run -p cluster --test jepsen_bank` (cross-range conservation), `cargo nextest run -p cluster`, `cargo clippy -p cluster --all-targets -- -D warnings`.

- [ ] **Step 8: Commit**

```bash
git add crates/cluster/src/transport/protocol.rs crates/cluster/src/transport/server.rs crates/cluster/src/range/router.rs crates/cluster/src/twopc.rs crates/cluster/tests/crossrange_2pc.rs
git commit -m "feat(sp18): coordinator honors the effective (write-once) decision; TxnResp::Aborted

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Coordinator-silence timeout — self-resolve aged held sessions

**Files:**
- Modify: `crates/cluster/src/twopc.rs` (`HeldEntry { session, joined_at }`; thread through `held` consumers; `resolve_in_doubt`; `sweep_stale`; a `PARTICIPANT_SILENCE_TIMEOUT` const)
- Modify: `crates/cluster/src/server_node.rs` (spawn the per-node sweeper on the static path)
- Test: `crates/cluster/tests/gateway_local.rs` (a staged participant whose coordinator goes silent self-resolves within `T`, freeing its lock)

- [ ] **Step 1: Write the failing test**

The single in-crate `ServerNode` (which leads every range) stages a participant directly via `TxnService`, never sends a decision, and asserts the sweeper self-resolves it (the held session disappears + a blocked writer to the staged row proceeds). Add to `crates/cluster/tests/gateway_local.rs`:
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_silent_coordinator_is_recovered_by_the_timeout_sweeper() {
    use mvcc::xid::GLOBAL_XID_BASE;
    let (node, _sql) = start_two_range_node().await;
    let svc = node.txn_service().expect("static node hosts a TxnService"); // accessor added below
    let client = node.twopc_client();                                       // accessor added below

    // Seed a row on range 1, then STAGE a held participant for a global xid g that
    // NEVER receives a decision (the coordinator "crashed").
    let mut seed = node.engines[&1].connect();
    seed.run(&parse_one("CREATE TABLE b (id int4)")).await.expect("create b");
    seed.run(&parse_one("INSERT INTO b VALUES (20)")).await.expect("seed b");
    let g = node.engines[&0].begin_global_durable().await.expect("alloc g");
    assert!(matches!(svc.handle(1, TxnRpc::Stage { g, range: 1, sql: "UPDATE b SET id = 21 WHERE id = 20".into() }).await, TxnResp::Staged));
    assert!(svc.holds(g, 1).await, "the participant holds g");

    // Drive the sweeper with a zero timeout (every held session is "stale"): it
    // resolves g via the abort-race (no coordinator wrote a decision -> Aborted),
    // releasing the held session. Assert via the registry condition (no sleep).
    svc.sweep_stale(&client, std::time::Duration::ZERO).await;
    assert!(!svc.holds(g, 1).await, "the timeout sweeper self-resolved + released g");

    // The global decision is now Aborted (presumed-abort), so the row stays at 20.
    let mut reader = node.engines[&1].connect();
    let rows = scan_i32(&mut reader, "SELECT id FROM b").await; // helper; b still 20
    assert_eq!(rows, vec![20], "presumed-abort: the staged update is invisible");
}
```
(`parse_one`/`scan_i32` helpers: add small local helpers or reuse the in-crate ones. `node.txn_service()`, `node.twopc_client()` accessors are added in Step 4. Driving `sweep_stale` directly with `Duration::ZERO` makes the test deterministic — no waiting on a background interval.)

- [ ] **Step 2: Run it — expect FAIL** (`HeldEntry`, `sweep_stale`, `resolve_in_doubt`, the accessors do not exist).

Run: `cargo nextest run -p cluster --test gateway_local a_silent_coordinator_is_recovered_by_the_timeout_sweeper`

- [ ] **Step 3: `HeldEntry { session, joined_at }` + thread it through `twopc.rs`**

Replace the `HeldSession` alias (twopc.rs:278) and the `held` field (twopc.rs:283):
```rust
type HeldSession = Arc<Mutex<executor::SqlSession>>;

/// A held participant session plus the instant it joined `g` (for the
/// coordinator-silence timeout). The instant is set ONCE at first stage; a re-Stage
/// must NOT reset it (a chatty coordinator can't keep a doomed txn alive forever).
struct HeldEntry {
    session: HeldSession,
    joined_at: tokio::time::Instant,
}
```
Change the field to `held: Arc<Mutex<HashMap<(u64, RangeId), HeldEntry>>>`. Update the consumers:
- `session_handle` (twopc.rs:334-344): `or_insert_with(|| HeldEntry { session: Arc::new(Mutex::new(engine.connect())), joined_at: tokio::time::Instant::now() })`, then return `.session.clone()`.
- `release` (twopc.rs:367): `self.held.lock().await.remove(&(g, range))` now yields an `Option<HeldEntry>`; lock `entry.session` for the release.
- `release_all_for_range` (twopc.rs:303-318): collect `entry.session` from the removed entries.
- `holds` (twopc.rs:298, `#[cfg(test)]`): unchanged (`contains_key`).

Add the timeout const near `TXN_TIMEOUT` (twopc.rs:23):
```rust
/// How long a participant holds a staged-but-undecided session before it
/// self-resolves against range 0's global clog (presumed-abort if still in-doubt).
/// Well above normal commit latency so a healthy txn is never prematurely aborted.
const PARTICIPANT_SILENCE_TIMEOUT: Duration = Duration::from_secs(5);
```

- [ ] **Step 4: `resolve_in_doubt` + `sweep_stale` on `TxnService`**

Add after `release_all_for_range` (twopc.rs:318). `resolve_in_doubt` writes the abort-race to range 0's leader (write-once) and returns the effective decision; `sweep_stale` resolves every aged entry:
```rust
    /// Resolve an in-doubt `(g, range)` against range 0 via the WRITE-ONCE abort-race:
    /// send CommitGlobal{commit:false}; the effective decision comes back (Committed if
    /// a coordinator already won, Aborted if we won). Release the held session per the
    /// decision. Idempotent: a missing entry is a no-op; a re-resolve hits write-once.
    async fn resolve_in_doubt(&self, client: &TwoPcClient, g: u64, range: RangeId) {
        let committed = match client.call(0, TxnRpc::CommitGlobal { g, commit: false }).await {
            Ok(TxnResp::Committed) => true,
            Ok(TxnResp::Aborted) => false,
            _ => return, // range 0 unreachable: leave it for the next sweep tick
        };
        if let Some(entry) = self.held.lock().await.remove(&(g, range)) {
            let mut session = entry.session.lock().await;
            if committed { session.commit_release() } else { session.abort_release() }
        }
    }

    /// Self-resolve every held session older than `timeout` (coordinator-silence
    /// recovery). Snapshots stale `(g, range)` under a brief map lock, drops the
    /// guard, then resolves each via `resolve_in_doubt`.
    pub async fn sweep_stale(&self, client: &TwoPcClient, timeout: Duration) {
        let now = tokio::time::Instant::now();
        let stale: Vec<(u64, RangeId)> = {
            let held = self.held.lock().await;
            held.iter()
                .filter(|(_, e)| now.duration_since(e.joined_at) >= timeout)
                .map(|(&k, _)| k)
                .collect()
        };
        for (g, range) in stale {
            self.resolve_in_doubt(client, g, range).await;
        }
    }
```
(`TwoPcClient` is `Arc<Self>`-constructed; the sweeper holds an `Arc<TwoPcClient>` and passes `&*client`. Confirm `client.call(0, …)` resolves range 0's leader — it does, per SP17.)

- [ ] **Step 5: Spawn the per-node bounded sweeper (`server_node.rs`) + add the test accessors**

In `crates/cluster/src/server_node.rs` static path (after the `release_on_leadership_loss` spawn loop ~:264, BEFORE `Some(txn)` moves `txn` into `serve_node_protocol` ~:270), spawn the sweeper with a `TwoPcClient` and a clone of `txn`:
```rust
        let sweeper_client = crate::twopc::TwoPcClient::new(rafts.clone(), partition.clone());
        tokio::spawn(participant_silence_sweeper(txn.clone(), sweeper_client));
```
Add the sweeper free function next to `release_on_leadership_loss` (~server_node.rs:545). It is timer-based, so it uses a BOUNDED interval (not a storm, not a settle-sleep waiting on a condition — a recovery heartbeat):
```rust
/// Periodically self-resolve held 2PC sessions whose coordinator has gone silent
/// (no decision within the timeout) against range 0's global clog. Bounded cadence
/// (a recovery heartbeat), NOT a settle-sleep: each tick resolves only sessions
/// already past `PARTICIPANT_SILENCE_TIMEOUT`.
async fn participant_silence_sweeper(
    txn: crate::twopc::TxnService,
    client: std::sync::Arc<crate::twopc::TwoPcClient>,
) {
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(500));
    loop {
        tick.tick().await;
        txn.sweep_stale(&client, crate::twopc::PARTICIPANT_SILENCE_TIMEOUT).await;
    }
}
```
Make `PARTICIPANT_SILENCE_TIMEOUT` `pub(crate)` so server_node.rs sees it. Add the `#[cfg(test)]` accessors on `ServerNode` (server_node.rs, near the struct) so the Step-1 test can drive a sweep directly:
```rust
    #[cfg(test)]
    pub(crate) fn txn_service(&self) -> Option<crate::twopc::TxnService> { self.txn.clone() }
    #[cfg(test)]
    pub(crate) fn twopc_client(&self) -> std::sync::Arc<crate::twopc::TwoPcClient> {
        crate::twopc::TwoPcClient::new(self.rafts.clone(), self.partition.clone())
    }
```
(This requires `ServerNode` to RETAIN the `TxnService` — today `txn` is moved into `serve_node_protocol` and not stored. Add a `txn: Option<crate::twopc::TxnService>` field to `ServerNode`, set to `Some(txn.clone())` on the static path / `None` on the replicated path, before the move. `rafts`/`partition` are already `ServerNode` fields per the SP17 map.)

- [ ] **Step 6: Run + regressions**

Run: `cargo nextest run -p cluster --test gateway_local` (incl. the new sweeper test + all SP17 tests), `cargo nextest run -p cluster`, `cargo clippy -p cluster --all-targets -- -D warnings`.

- [ ] **Step 7: Commit**

```bash
git add crates/cluster/src/twopc.rs crates/cluster/src/server_node.rs crates/cluster/tests/gateway_local.rs
git commit -m "feat(sp18): coordinator-silence timeout sweeper self-resolves stranded participants

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Durable leadership-rise sweep — finalize in-doubt markers on a failed-over participant

**Files:**
- Modify: `crates/kv/src/key.rs` (`clog_xid_of` decoder; `clog_prefix` was added in Task 1)
- Modify: `crates/executor/src/lib.rs` (`pub async fn in_doubt_globals(&self) -> Result<Vec<u64>, ExecError>`)
- Modify: `crates/cluster/src/server_node.rs` (a per-range leadership-rise sweep watcher)
- Test: `crates/executor` unit test for `in_doubt_globals` + `crates/cluster/tests/gateway_local.rs` (a durable Prepared marker is finalized on leadership)

- [ ] **Step 1: Write the failing test (`in_doubt_globals` scan)**

The engine scans its OWN range's clog for `Prepared(→g)` markers whose `g` is undecided in range 0's clog, returning the distinct `g`s. Add an executor test (`crates/executor/src/lib.rs` tests or a dedicated test file) using an in-memory engine:
```rust
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_doubt_globals_lists_undecided_prepared_markers() {
        use mvcc::clog::{put_op, XidStatus};
        use mvcc::xid::GLOBAL_XID_BASE;
        // A single-store engine where kv == catalog_kv (range 0's own engine shape).
        let engine = SqlEngine::new(); // in-memory; confirm the real test constructor
        let kv = engine.local_store(); // accessor added in Step 3 (or use a MemKv directly)
        let g_undecided = GLOBAL_XID_BASE + 1;
        let g_committed = GLOBAL_XID_BASE + 2;
        // Two local participants prepared into two global xids.
        kv.write_batch(&[put_op(11, XidStatus::Prepared(g_undecided))]).expect("p1");
        kv.write_batch(&[put_op(12, XidStatus::Prepared(g_committed))]).expect("p2");
        // g_committed is decided; g_undecided is not.
        kv.write_batch(&[put_op(g_committed, XidStatus::Committed)]).expect("decide");
        let mut got = engine.in_doubt_globals().await.expect("scan");
        got.sort();
        assert_eq!(got, vec![g_undecided], "only undecided Prepared markers are returned");
    }
```
(Adapt to the real in-memory engine constructor — `crates/executor` tests build an engine over a `MemKv`; mirror an existing test's setup. If a public `local_store` accessor is undesirable, write the markers through a committer and read via `in_doubt_globals` only.)

- [ ] **Step 2: Run it — expect FAIL** (`in_doubt_globals` does not exist).

- [ ] **Step 3: `clog_xid_of` + `in_doubt_globals`**

In `crates/kv/src/key.rs` (after `clog_prefix`):
```rust
/// Decode the xid from a `/0/clog/<xid>` key, or `None` if `key` is not a clog key.
pub fn clog_xid_of(key: &[u8]) -> Option<u64> {
    let prefix = clog_prefix();
    if key.len() != prefix.len() + 8 || key[..prefix.len()] != prefix[..] {
        return None;
    }
    let mut rest = &key[prefix.len()..];
    crate::keyenc::take_u64(&mut rest).ok()
}
```
In `crates/executor/src/lib.rs` (near `commit_global_decision`):
```rust
    /// Scan THIS range's clog for in-doubt `Prepared(Li -> g)` markers and return the
    /// distinct `g`s that are NOT yet decided in range 0's global clog. Used by the
    /// leadership-rise recovery sweep to finalize a failed-over participant's txns.
    pub async fn in_doubt_globals(&self) -> Result<Vec<u64>, ExecError> {
        use std::collections::BTreeSet;
        let mut gs: BTreeSet<u64> = BTreeSet::new();
        for (k, v) in self.kv.scan_prefix(&kv::key::clog_prefix())? {
            if kv::key::clog_xid_of(&k).is_none() {
                continue;
            }
            if let mvcc::clog::XidStatus::Prepared(g) = mvcc::clog::decode(&v)? {
                // Undecided iff range 0's global clog has no terminal decision yet.
                if !matches!(
                    mvcc::clog::get(self.catalog_kv.as_ref(), g)?,
                    mvcc::clog::XidStatus::Committed | mvcc::clog::XidStatus::Aborted
                ) {
                    gs.insert(g);
                }
            }
        }
        Ok(gs.into_iter().collect())
    }
```
This needs a value-decoder. Add `pub fn decode(value: &[u8]) -> Result<XidStatus, KvError>` to `crates/mvcc/src/clog.rs` (factor it out of `get`, which becomes `get(kv,xid) = decode(&kv.get(clog_key(xid))?.unwrap_or_default())` — or keep `get` and add `decode` that `get` calls). Confirm `self.kv`/`self.catalog_kv` field access compiles inside `lib.rs` (it does — same crate). If the Step-1 test needs `local_store`, add `#[cfg(test)] pub(crate) fn local_store(&self) -> &Arc<dyn Kv> { &self.kv }`.

- [ ] **Step 4: The leadership-rise sweep watcher (`server_node.rs`)**

Add a sibling watcher to `release_on_leadership_loss` (rising edge), spawned per DATA range. It scans the range's in-doubt globals on the rising edge and resolves each via the abort-race RPC. Add next to `release_on_leadership_loss` (~server_node.rs:545):
```rust
/// On the RISING edge of this node's leadership for `range`, finalize any in-doubt
/// `Prepared(-> g)` markers in this range's durable clog whose coordinator died:
/// resolve each undecided `g` via the WRITE-ONCE abort-race against range 0. This
/// heals a failed-over participant so its rows resolve (invisible for presumed-abort)
/// rather than staying in-doubt forever. Mirrors `release_on_leadership_loss`'s
/// metrics-watch loop (no sleep).
async fn resolve_in_doubt_on_leadership(
    raft: openraft::Raft<TypeConfig>,
    range: RangeId,
    id: NodeId,
    engine: Arc<SqlEngine>,
    client: std::sync::Arc<crate::twopc::TwoPcClient>,
) {
    let mut rx = raft.metrics();
    let mut was_leader = false;
    loop {
        let is_leader = rx.borrow().current_leader == Some(id);
        if is_leader && !was_leader {
            if let Ok(gs) = engine.in_doubt_globals().await {
                for g in gs {
                    // Abort-race write to range 0 (write-once); decision is final after.
                    let _ = client.call(0, crate::twopc::wire::commit_global_abort(g)).await;
                }
            }
        }
        was_leader = is_leader;
        if rx.changed().await.is_err() {
            return;
        }
    }
}
```
Use the existing RPC directly: `client.call(0, TxnRpc::CommitGlobal { g, commit: false })` (drop the `wire::` helper — inline the `TxnRpc::CommitGlobal { g, commit: false }`; import `TxnRpc` in server_node.rs). Spawn it in the static-path DATA-range loop (server_node.rs:240-249) where `range`, `engine`, `cfg.id`, `rafts`, and a `TwoPcClient` are in scope:
```rust
        tokio::spawn(resolve_in_doubt_on_leadership(
            raft.clone(),
            range,
            cfg.id,
            engine.clone(),
            crate::twopc::TwoPcClient::new(rafts.clone(), partition.clone()),
        ));
```
(Range 0's own engine does not need this — its `Prepared` markers belong to data ranges; the data-range engines carry the markers. Confirm `rafts`/`partition` are in scope at the data-range spawn loop — per the map they are.)

- [ ] **Step 5: Add the durable-marker leadership test**

Add to `crates/cluster/tests/gateway_local.rs` (single node leads everything, so leadership "rises" at bring-up; stage a participant, leave g undecided, then trigger the sweep):
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_durable_prepared_marker_is_finalized_by_the_leadership_sweep() {
    use mvcc::xid::GLOBAL_XID_BASE;
    let (node, _sql) = start_two_range_node().await;
    let mut seed = node.engines[&1].connect();
    seed.run(&parse_one("CREATE TABLE b (id int4)")).await.expect("b");
    seed.run(&parse_one("INSERT INTO b VALUES (20)")).await.expect("seed");
    let g = node.engines[&0].begin_global_durable().await.expect("g");
    // Stage a held participant (writes Prepared(Li -> g) durably), then DROP the held
    // session WITHOUT a decision (simulate the participant leader crashing): the
    // durable marker persists, the in-memory session is gone.
    let svc = node.txn_service().expect("txn service");
    assert!(matches!(svc.handle(1, TxnRpc::Stage { g, range: 1, sql: "UPDATE b SET id = 21 WHERE id = 20".into() }).await, TxnResp::Staged));
    svc.release_all_for_range(1).await; // drop in-memory session; durable Prepared marker stays

    // The marker is in-doubt; finalize it via the engine scan + abort-race.
    let gs = node.engines[&1].in_doubt_globals().await.expect("scan");
    assert_eq!(gs, vec![g], "the durable Prepared marker is in-doubt");
    node.engines[&0].commit_global_decision(g, mvcc::clog::XidStatus::Aborted).await.expect("finalize");
    // Now resolved: the row is invisible (presumed-abort).
    let mut reader = node.engines[&1].connect();
    assert_eq!(scan_i32(&mut reader, "SELECT id FROM b").await, vec![20]);
}
```
(This exercises `in_doubt_globals` + the write-once finalize directly; the spawned `resolve_in_doubt_on_leadership` is exercised end-to-end in the T5 nemesis. `release_all_for_range` drops the in-memory session but leaves the durable `Prepared` marker — confirm that is its behavior.)

- [ ] **Step 6: Run + regressions**

Run: `cargo nextest run -p executor in_doubt_globals_lists_undecided_prepared_markers`, `cargo nextest run -p cluster --test gateway_local`, `cargo nextest run -p executor`, `cargo nextest run -p cluster`, `cargo clippy -p cluster -p executor -p mvcc -p kv --all-targets -- -D warnings`.

- [ ] **Step 7: Commit**

```bash
git add crates/kv/src/key.rs crates/mvcc/src/clog.rs crates/executor/src/lib.rs crates/cluster/src/server_node.rs crates/cluster/tests/gateway_local.rs
git commit -m "feat(sp18): durable leadership-rise sweep finalizes in-doubt Prepared markers

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Multi-process crash/partition-nemesis cross-range bank

**Files:**
- Create: `crates/crabgresql/tests/crossrange_2pc_nemesis.rs` (UAC-safe; reuses `mod harness;`)

- [ ] **Step 1: Write the e2e (it is the test)**

The workload runs cross-range transfers at random gateways; the nemesis kills random nodes (incl. mid-txn coordinators) + partitions, paced on worker progress; the oracle asserts conservation AND that recovery freed every stranded lock (a post-heal full-coverage transfer round must commit). Model the nemesis loop on `multiprocess.rs:336-370` and the transfer/oracle on `jepsen_bank.rs` cross-range helpers.
```rust
//! SP18 D3c-net-hard: cross-range 2PC conserves the bank total under a multi-process
//! crash/partition nemesis that kills random nodes INCLUDING mid-transaction
//! coordinators. Recovery (write-once decision + participant self-resolve) ensures
//! no transfer is half-applied and no lock is stranded forever.
mod harness;
use harness::Cluster;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_range_bank_conserves_total_under_crash_nemesis() {
    const ACCOUNTS: i64 = 4;
    const SEED: i64 = 100;
    const PROCS: usize = 3;
    const OPS: usize = 8;
    const MIN_ROUNDS: usize = 4;
    let seeded_total = 2 * ACCOUNTS * SEED; // two tables, two ranges

    // 3 nodes, boundary [2]: acct_a (id 1) -> range 0, acct_b (id 2) -> range 1.
    let mut c = Cluster::spawn_multirange(3, vec![2]).await;
    let admin = c.pg(0).await;
    admin.simple_query("CREATE TABLE acct_a (id int8, bal int8)").await.expect("a");
    admin.simple_query("CREATE TABLE acct_b (id int8, bal int8)").await.expect("b");
    for id in 0..ACCOUNTS {
        admin.simple_query(&format!("INSERT INTO acct_a VALUES ({id}, {SEED})")).await.expect("seed a");
        admin.simple_query(&format!("INSERT INTO acct_b VALUES ({id}, {SEED})")).await.expect("seed b");
    }
    drop(admin);

    // Workers connect round-robin across all nodes (so a coordinator is often a
    // NON-leading gateway, and the nemesis kills coordinators mid-txn).
    let addrs: Vec<String> = (0..c.len()).map(|i| c.sql_addr(i as u64).to_string()).collect();
    let mut workers = Vec::new();
    for process in 0..PROCS {
        let addrs = addrs.clone();
        workers.push(tokio::spawn(async move {
            let mut rng = Lcg::new(0x9E37_79B9_u64.wrapping_mul(process as u64 + 1));
            let mut committed = 0usize;
            for _ in 0..OPS {
                let node = addrs[process % addrs.len()].clone();
                let Some(client) = connect(&node).await else { continue };
                let from = (rng.next() % ACCOUNTS as u64) as i64;
                let mut to = (rng.next() % ACCOUNTS as u64) as i64;
                if to == from { to = (to + 1) % ACCOUNTS; }
                let amt = 1 + (rng.next() % 20) as i64;
                if cross_transfer(&client, from, to, amt).await { committed += 1; }
            }
            committed
        }));
    }

    // Nemesis: kill+respawn / partition+heal a rotating victim (ANY node, incl.
    // coordinators), paced on worker progress + a MIN_ROUNDS floor. No settle-sleep.
    let mut round = 0usize;
    while !workers.iter().all(|w| w.is_finished()) || round < MIN_ROUNDS {
        let victim = (round % c.len()) as u64;
        if round.is_multiple_of(2) {
            c.kill(victim).await;
            c.respawn(victim);
        } else {
            let others: Vec<u64> = (0..c.len() as u64).filter(|&i| i != victim).collect();
            let _ = c.control(victim, ctl_set_partition(others.clone())).await;
            for &o in &others { let _ = c.control(o, ctl_set_partition(vec![victim])).await; }
            for id in 0..c.len() as u64 { let _ = c.control(id, ctl_heal()).await; }
        }
        round += 1;
    }
    let mut total_committed = 0usize;
    for w in workers { total_committed += w.await.expect("worker"); }

    // Heal; wait for leaders on both ranges.
    for id in 0..c.len() as u64 { let _ = c.control(id, ctl_heal()).await; }
    c.range_leader(0).await;
    c.range_leader(1).await;

    // RECOVERY-REQUIRED check: a post-heal transfer touching EVERY account pair must
    // commit within bound. A coordinator-crash-stranded lock that recovery failed to
    // free would block this forever -> exec_until_ok panics at its deadline -> fail.
    for id in 0..ACCOUNTS {
        let other = (id + 1) % ACCOUNTS;
        c.exec_until_ok(&format!(
            "BEGIN; UPDATE acct_a SET bal = bal - 0 WHERE id = {id}; UPDATE acct_b SET bal = bal + 0 WHERE id = {other}; COMMIT"
        )).await; // amount 0: touches+locks both rows, conserves total, requires no funds
    }

    // CONSERVATION oracle: sum both tables across both ranges == seeded total.
    let gw = c.pick_live_gateway().await;
    let reader = c.pg(gw).await;
    let total = read_total_cross(&reader, ACCOUNTS).await;
    assert_eq!(total, seeded_total,
        "cross-range transfers conserve the bank total under crash+partition nemesis (got {total}, want {seeded_total})");
    assert!(total_committed > 0, "the workload must commit at least one transfer (non-vacuous)");
}
```
Add the module-local helpers (mirror `multiprocess.rs`/`jepsen_bank.rs` — `Lcg`, `connect`, `cross_transfer` (the `BEGIN; UPDATE acct_a -amt; UPDATE acct_b +amt; COMMIT` bounded sequence with best-effort ROLLBACK), `read_total_cross` (sum `acct_a`+`acct_b` over `0..ACCOUNTS`), `first_i64`, and `ctl_set_partition`/`ctl_heal` thin wrappers over `cluster::transport::protocol::ControlRequest::{SetPartition,Heal}`). Copy the exact shapes from `crates/crabgresql/tests/multiprocess.rs` (`transfer` lines 712-745, `read_total` lines 259-269, `Lcg` lines 243-255, `first_i64` lines 73-80) adapting to two tables.

- [ ] **Step 2: Run it — iterate to green (a real distributed test; expect to debug)**

Run: `cargo nextest run -p crabgresql --test crossrange_2pc_nemesis` (run 2-3× to confirm non-flaky). If a worker hangs, confirm `cross_transfer`/`connect` use bounded `tokio::time::timeout` (10s per statement, like `multiprocess::transfer`), and that the nemesis paces on `workers[].is_finished()` (NOT a sleep). If the recovery check times out, the recovery path (T3 sweeper / T4 sweep) is not firing — confirm `PARTICIPANT_SILENCE_TIMEOUT` (5s) is short enough relative to the test's bounded waits and the sweeper is spawned on the static path. Do NOT add a settle-sleep; do NOT weaken the conservation/recovery assertions.

- [ ] **Step 3: Run the whole crabgresql crate + commit**

Run: `cargo nextest run -p crabgresql` (no regressions: multiprocess, jepsen_elle, multirange_gateway, meta_range_gateway, crossrange_2pc_net). UAC guard: `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty.
```bash
git add crates/crabgresql/tests/crossrange_2pc_nemesis.rs
git commit -m "test(sp18): multi-process crash/partition-nemesis cross-range bank conserves under coordinator crashes

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Gauntlet + traceability + CLAUDE.md + finish

**Files:**
- Modify: `CLAUDE.md` (SP18 UAC audit line)
- Modify: `docs/superpowers/specs/2026-06-14-crabgresql-sp18-d3c-net-hard-fault-recovery-design.md` (append a traceability table)

- [ ] **Step 1: UAC guard** — `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` (expect empty). Confirm no new `[[test]]/[[bin]]` names with forbidden substrings.

- [ ] **Step 2: Add the SP18 line to CLAUDE.md** (after the SP17 line):
```markdown
**SP18 (2026-06-14):** one new binary — `crabgresql::crossrange_2pc_nemesis` (multi-process crash/partition-nemesis cross-range bank) — UAC-safe. The crabgresql list now also includes `crossrange_2pc_nemesis`.
```

- [ ] **Step 3: Append a traceability table** to the SP18 spec mapping each success criterion (1–7) → task → test.

- [ ] **Step 4: Full gauntlet** (all must be green):
```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
cargo test --workspace --doc
cargo deny check
```
(If `cargo fmt --all --check` reports diffs, run `cargo fmt --all` and re-commit.)

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md docs/superpowers/specs/2026-06-14-crabgresql-sp18-d3c-net-hard-fault-recovery-design.md
git commit -m "docs(sp18): traceability table + CLAUDE.md UAC audit for crossrange_2pc_nemesis

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 6: Finish the branch** — superpowers:finishing-a-development-branch, option 2 (push fresh non-force branch + PR against `main`; rebase `--onto origin/main` first if SP17 PR #33 has merged). PR body ends with the Claude Code generated-with line.

---

## Notes for the implementer

- **Stale IDE diagnostics:** rust-analyzer squiggles lag the committed tree and are routinely wrong mid-edit. Trust `cargo build`/`clippy`/`nextest`, never the editor.
- **No `sleep` in tests:** in-crate waits use openraft `wait().metrics(...)` or drive `sweep_stale`/`commit_global_decision` directly (deterministic). The T5 nemesis uses the harness's bounded poll cadence + `workers[].is_finished()` pacing; the production `PARTICIPANT_SILENCE_TIMEOUT` heartbeat is not a test sleep.
- **The write-once decision is the correctness core.** It must be enforced in the deterministic apply of BOTH state machines (in-memory `store.rs` + durable `durable.rs`), with the durable path's intra-batch `decided` map mirroring `counters`. A read-then-write at the client/committer layer is NOT atomic and is wrong.
- **`commit` semantics in `finish_txn`:** after SP18, the client-facing tag and the per-participant release both follow the *returned* effective decision, never the intended one — a coordinator that lost the abort-race must report `ROLLBACK`.
- **Confirm the SP16/SP17 test accessors** (`two_range_cluster`, `leader_engine`, `RangeRouter::connect`, `staged_global_xid`, `scan_i32`, `start_two_range_node`, `parse_one`) against the real test files before pasting; names may differ slightly.
- **Regression watch:** the SP16/SP17 in-process `crossrange_2pc` + `jepsen_bank` cross-range conservation must stay green at every task — they are the load-bearing proof that write-once + the effective-decision change didn't break the happy path.

