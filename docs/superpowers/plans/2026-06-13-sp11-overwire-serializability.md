# SP11 (D2d): Over-the-wire serializability checking — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Check transactional **strict serializability** of a list-append workload run over the real multi-process cluster (D2b/D2c) under faults — a leader-fixed passing gate plus a leader-failover gap-finder that documents the D5 stale-read gap.

**Architecture:** A new test binary drives single-key list-append transactions over tokio-postgres against the existing process harness, records each transaction as a linearizability op (invoke at txn-start, return at commit), and feeds the per-key history to a stateright `LinearizabilityTester` over a custom `AppendList` reference object. Test-only — no production code changes.

**Tech Stack:** Rust 2024, stateright (already a dev-dep — `LinearizabilityTester`/`SequentialSpec`), tokio-postgres, the D2b/D2c process harness. Pure-Rust, no new dependency.

**Spec:** `docs/superpowers/specs/2026-06-13-crabgresql-sp11-overwire-serializability-design.md`

**Conventions for every task:** Windows dev box — run multiprocess tests with `__COMPAT_LAYER=RunAsInvoker cargo test ...` (real child processes spawn fine under that shim). **IDE/rust-analyzer diagnostics are routinely STALE — trust only `cargo build`/`clippy`/`test`.** Repo denies `clippy::unwrap_used` even in tests — use `.expect("msg")`. End each commit message with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. Branch: `sp11-d2d-serializability` (already checked out; do NOT switch). After each task: `cargo clippy -p crabgresql --all-targets -- -D warnings` zero-warning.

**Verified facts (do not re-derive):**
- The engine has **no** SQL `SERIAL`/`SEQUENCE`/`nextval`. A table scan returns rows in internal **rowid order** (monotonic, leader-assigned at INSERT = commit order), so `SELECT val FROM appends WHERE key=:k` (no `ORDER BY`) yields key `:k`'s appends in commit order.
- stateright `SequentialSpec` (from `stateright::semantics`): `type Op; type Ret; fn invoke(&mut self, op: &Self::Op) -> Self::Ret;` (+ a defaulted `is_valid_step`). `LinearizabilityTester::<ThreadId: Ord, RefObj: SequentialSpec>::new(refobj)`, `.on_invoke(tid, op) -> Result<&mut Self, String>`, `.on_return(tid, ret) -> Result<&mut Self, String>`, `.is_consistent() -> bool`. Imports: `use stateright::semantics::{ConsistencyTester, LinearizabilityTester, SequentialSpec};` (mirror `jepsen_bank.rs`).
- The harness (`crates/crabgresql/tests/harness/mod.rs`) `Cluster`: `spawn(n)`, `pg(id)`/`pg_try(id)`, `kill(&mut,id)`/`respawn`, `control(id, ControlRequest)`, `status(id) -> Option<NodeStatus>`, `wait_for_leader() -> u64`, `sql_addr(id)`, `nodes`/`len()`. `ControlRequest`/`ControlResponse`/`NodeStatus` are `cluster::transport::protocol::*` (pub).

---

## File Structure

| File | Responsibility |
|---|---|
| `crates/crabgresql/tests/jepsen_elle.rs` | **(new)** The whole slice: `AppendList` reference object + ops, the history recorder (invoke/return events with a global seq), the per-key checker, the EDN emitter, Scenario A (passing gate), Scenario B (D5 gap-finder), the positive-control meta-test, the EDN-format test. Reuses `mod harness;`. |

No production-code change. The in-process `jepsen_bank.rs` and `multiprocess.rs` are untouched.

---

### Task 1: The `AppendList` reference object + positive-control meta-test

**Files:** Create `crates/crabgresql/tests/jepsen_elle.rs`.

The stateright reference object + a meta-test proving the checker has teeth (rejects a known non-serializable history). All in-process — no cluster.

- [ ] **Step 1: Write the reference object + meta-tests.**
```rust
//! Over-the-wire serializability checking (SP11 / D2d): a single-key list-append
//! workload run against the real multi-process cluster, recorded as a
//! linearizability history and checked for strict serializability with stateright.
mod harness;

use stateright::semantics::{ConsistencyTester, LinearizabilityTester, SequentialSpec};

// ---------------------------------------------------------------------------
// Reference object: a per-key append-only list. Each transaction is ONE atomic
// op against it — `AppendRead(v)` (append v, return the new list) for a
// writing txn, or `Read` (return the list) for a read-only txn. Linearizability
// of these atomic ops over one key == strict serializability of that key.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct AppendList(Vec<i64>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum ListOp {
    AppendRead(i64),
    Read,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ListRet(Vec<i64>); // the observed list

impl SequentialSpec for AppendList {
    type Op = ListOp;
    type Ret = ListRet;
    fn invoke(&mut self, op: &ListOp) -> ListRet {
        if let ListOp::AppendRead(v) = op {
            self.0.push(*v);
        }
        ListRet(self.0.clone())
    }
}

#[test]
fn checker_accepts_a_serial_list_history() {
    // append 1 -> [1]; append 2 -> [1,2]; read -> [1,2]. Strictly serial, valid.
    let mut t: LinearizabilityTester<usize, AppendList> =
        LinearizabilityTester::new(AppendList::default());
    t.on_invoke(0, ListOp::AppendRead(1)).expect("inv").on_return(0, ListRet(vec![1])).expect("ret");
    t.on_invoke(1, ListOp::AppendRead(2)).expect("inv").on_return(1, ListRet(vec![1, 2])).expect("ret");
    t.on_invoke(2, ListOp::Read).expect("inv").on_return(2, ListRet(vec![1, 2])).expect("ret");
    assert!(t.is_consistent(), "a serial append/read history must be accepted");
}

#[test]
fn checker_rejects_a_stale_read_history() {
    // append 1 (returns) THEN append 2 (returns) THEN a read that observed [1] —
    // the read started after append 2 completed, so it must see 2; it didn't.
    // This is exactly the D5 stale-read shape. The checker MUST reject it (teeth).
    let mut t: LinearizabilityTester<usize, AppendList> =
        LinearizabilityTester::new(AppendList::default());
    t.on_invoke(0, ListOp::AppendRead(1)).expect("inv").on_return(0, ListRet(vec![1])).expect("ret");
    t.on_invoke(1, ListOp::AppendRead(2)).expect("inv").on_return(1, ListRet(vec![1, 2])).expect("ret");
    t.on_invoke(2, ListOp::Read).expect("inv").on_return(2, ListRet(vec![1])).expect("ret");
    assert!(!t.is_consistent(), "a read missing an already-acked append must be rejected");
}
```
Note: the order of `on_invoke`/`on_return` calls IS the real-time order — `LinearizabilityTester` records, at each invoke, what has completed across threads, so feeding fully-completed `append 2` before the `read`'s invoke enforces "2 precedes read". This is why the stale-read history is rejected.

- [ ] **Step 2: Run the meta-tests.**
```
cargo test -p crabgresql --test jepsen_elle checker_ 2>&1 | grep "test result"
```
Expected: both pass — the checker accepts a valid history and rejects the stale-read one (it has teeth).

- [ ] **Step 3: Verify + commit.**
```
cargo clippy -p crabgresql --all-targets -- -D warnings
cargo fmt -p crabgresql
git add crates/crabgresql/tests/jepsen_elle.rs
git commit -m "test(crabgresql): AppendList stateright reference object + checker teeth meta-tests"
```

---

### Task 2: History recorder (invoke/return events) + EDN emitter

**Files:** Modify `crates/crabgresql/tests/jepsen_elle.rs`.

The event model (so concurrent workers can record off-thread and replay into the single-threaded tester in real-time order) + the Elle-compatible EDN emitter.

- [ ] **Step 1: Add the event model + per-key checker + EDN emitter.**
```rust
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// One recorded linearizability event. `seq` is a global real-time order (an
/// invoke is stamped when the txn BEGINs; a return when it COMMITs), so replaying
/// events in `seq` order reconstructs the real-time interleaving the tester needs.
#[derive(Clone, Debug)]
enum Event {
    Invoke { process: usize, key: i64, seq: u64, op: ListOp },
    Return { process: usize, key: i64, seq: u64, ret: ListRet },
}

fn ev_seq(e: &Event) -> u64 {
    match e {
        Event::Invoke { seq, .. } | Event::Return { seq, .. } => *seq,
    }
}

/// Shared recorder: workers push invoke/return events stamped from one global
/// counter; we sort by seq and replay after the workload.
#[derive(Clone, Default)]
struct Recorder {
    events: Arc<Mutex<Vec<Event>>>,
    seq: Arc<AtomicU64>,
}

impl Recorder {
    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }
    fn push(&self, e: Event) {
        self.events.lock().expect("recorder lock").push(e);
    }
    fn take_sorted(&self) -> Vec<Event> {
        let mut v = self.events.lock().expect("recorder lock").clone();
        v.sort_by_key(ev_seq);
        v
    }
}

/// Check strict serializability PER KEY: partition the events by key, feed each
/// key's events (in global seq order) into a fresh `LinearizabilityTester`, and
/// require every key to be consistent. Returns true iff all keys are consistent.
fn all_keys_consistent(events: &[Event]) -> bool {
    let mut keys: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
    for e in events {
        match e {
            Event::Invoke { key, .. } | Event::Return { key, .. } => {
                keys.insert(*key);
            }
        }
    }
    keys.into_iter().all(|k| key_consistent(events, k))
}

fn key_consistent(events: &[Event], key: i64) -> bool {
    let mut t: LinearizabilityTester<usize, AppendList> =
        LinearizabilityTester::new(AppendList::default());
    for e in events {
        match e {
            Event::Invoke { process, key: ek, op, .. } if *ek == key => {
                // An invoke with no later matching return (indeterminate commit)
                // is left in-flight — the tester may place or omit it.
                t.on_invoke(*process, op.clone()).expect("on_invoke");
            }
            Event::Return { process, key: ek, ret, .. } if *ek == key => {
                t.on_return(*process, ret.clone()).expect("on_return");
            }
            _ => {}
        }
    }
    t.is_consistent()
}

/// Emit the jepsen/elle `:list-append` EDN for a recorded history (paired
/// invoke/return per (process, key)). Each txn becomes one op map:
/// `{:process p, :type :ok, :f :txn, :value [[:append k v] [:r k [list...]]]}`.
/// Indeterminate (invoke with no return) → `:type :info` with just the append.
fn history_to_elle_edn(events: &[Event]) -> String {
    use std::fmt::Write as _;
    // Pair each Invoke with the next Return for the same (process, key).
    let mut out = String::new();
    out.push_str("; crabgresql SP11 list-append history (jepsen/elle EDN)\n");
    let mut i = 0;
    let evs = events;
    // Simple pairing: walk in seq order; for each Invoke, find the next Return
    // with the same process+key.
    let mut used = vec![false; evs.len()];
    for (idx, e) in evs.iter().enumerate() {
        if let Event::Invoke { process, key, op, .. } = e {
            // find matching return
            let ret = evs.iter().enumerate().skip(idx + 1).find(|(j, r)| {
                !used[*j]
                    && matches!(r, Event::Return { process: rp, key: rk, .. } if rp == process && rk == key)
            });
            let (append_clause, value, ok) = match op {
                ListOp::AppendRead(v) => (format!("[:append {key} {v}]"), Some(*v), true),
                ListOp::Read => (String::new(), None, true),
            };
            let _ = ok;
            match ret {
                Some((j, Event::Return { ret: ListRet(list), .. })) => {
                    used[j] = true;
                    let list_str = list.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(" ");
                    let mut value_vec = String::new();
                    if !append_clause.is_empty() {
                        value_vec.push_str(&append_clause);
                        value_vec.push(' ');
                    }
                    let _ = value;
                    let _ = write!(value_vec, "[:r {key} [{list_str}]]");
                    let _ = writeln!(
                        out,
                        "{{:process {process}, :type :ok, :f :txn, :value [{value_vec}]}}"
                    );
                }
                _ => {
                    // indeterminate: invoke with no return
                    let _ = writeln!(
                        out,
                        "{{:process {process}, :type :info, :f :txn, :value [{append_clause}]}}"
                    );
                }
            }
        }
        i = idx;
    }
    let _ = i;
    out
}

#[test]
fn edn_format_round_trips_a_small_history() {
    let r = Recorder::default();
    let s0 = r.next_seq();
    r.push(Event::Invoke { process: 0, key: 1, seq: s0, op: ListOp::AppendRead(5) });
    let s1 = r.next_seq();
    r.push(Event::Return { process: 0, key: 1, seq: s1, ret: ListRet(vec![5]) });
    let edn = history_to_elle_edn(&r.take_sorted());
    assert!(edn.contains("[:append 1 5]"), "append clause present: {edn}");
    assert!(edn.contains("[:r 1 [5]]"), "read clause present: {edn}");
    assert!(edn.contains(":type :ok"), "ok type present: {edn}");
}
```
(If clippy flags the `i`/`used`/`value` scaffolding as awkward, simplify the emitter — the REQUIREMENT is: produce one `{:process … :type :ok|:info :f :txn :value [[:append k v] [:r k [list]]]}` map per transaction, validated by `edn_format_round_trips_a_small_history`. Keep it clippy-clean; the exact loop shape is yours.)

- [ ] **Step 2: Run.** `cargo test -p crabgresql --test jepsen_elle edn_format 2>&1 | grep "test result"` → passes.

- [ ] **Step 3: Verify + commit.**
```
cargo clippy -p crabgresql --all-targets -- -D warnings
cargo fmt -p crabgresql
git add crates/crabgresql/tests/jepsen_elle.rs
git commit -m "test(crabgresql): list-append history recorder (invoke/return events) + Elle EDN emitter"
```

---

### Task 3: The list-append workload + Scenario A (leader-fixed passing gate)

**Files:** Modify `crates/crabgresql/tests/jepsen_elle.rs`.

Run the workload over the real cluster under a followers-only nemesis, record the history, assert strict serializability holds.

- [ ] **Step 1: Add the workload + Scenario A.**
```rust
use std::time::Duration;
use tokio_postgres::SimpleQueryMessage;

/// Read the `val` column of a `simple_query` `SELECT` result as an ordered Vec.
fn list_from(msgs: &[SimpleQueryMessage]) -> Vec<i64> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some(r.get(0).expect("val col").parse::<i64>().expect("i64"))
            }
            _ => None,
        })
        .collect()
}

/// Run ONE list-append transaction against `client`: BEGIN; INSERT append; SELECT
/// the key's list; COMMIT. Records an Invoke (at BEGIN) and, on a clean COMMIT, a
/// Return with the observed list. An indeterminate COMMIT (timeout/error) leaves
/// the Invoke in-flight (no Return) and rolls back. Returns true if it committed.
async fn append_txn(
    client: &tokio_postgres::Client,
    rec: &Recorder,
    process: usize,
    key: i64,
    val: i64,
) -> bool {
    let inv = rec.next_seq();
    rec.push(Event::Invoke { process, key, seq: inv, op: ListOp::AppendRead(val) });
    let bounded = |sql: String| async move {
        tokio::time::timeout(Duration::from_secs(10), client.simple_query(&sql)).await
    };
    if bounded("BEGIN".into()).await.map(|r| r.is_ok()).unwrap_or(false) == false {
        let _ = client.simple_query("ROLLBACK").await;
        return false;
    }
    if bounded(format!("INSERT INTO appends(key, val) VALUES ({key}, {val})"))
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false)
        == false
    {
        let _ = client.simple_query("ROLLBACK").await;
        return false;
    }
    let list = match bounded(format!("SELECT val FROM appends WHERE key = {key}")).await {
        Ok(Ok(msgs)) => list_from(&msgs),
        _ => {
            let _ = client.simple_query("ROLLBACK").await;
            return false;
        }
    };
    match bounded("COMMIT".into()).await {
        Ok(Ok(_)) => {
            let ret = rec.next_seq();
            rec.push(Event::Return { process, key, seq: ret, ret: ListRet(list) });
            true
        }
        _ => {
            let _ = client.simple_query("ROLLBACK").await;
            false // indeterminate: Invoke stays in-flight, no Return
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_append_is_strict_serializable_under_follower_faults() {
    let mut c = harness::Cluster::spawn(3).await;
    let leader = c.wait_for_leader().await;
    const KEYS: i64 = 2;
    const PROCS: usize = 2;
    const OPS: usize = 6;
    // Seed the table via a routed connection (some node accepts).
    {
        let mut idx = 0;
        let setup = loop {
            if let Some(cl) = c.pg_try(idx).await { break cl; }
            idx += 1;
            assert!(idx < 30, "no node accepted the setup connection");
        };
        setup.simple_query("CREATE TABLE appends (key int8, val int8)").await.expect("create");
    }
    let rec = Recorder::default();
    let n_nodes = c.len();
    // Workers: each connects to round-robin nodes (routing exercises the proxy) and
    // appends globally-unique values to keys round-robin.
    let mut workers = Vec::new();
    for p in 0..PROCS {
        let rec = rec.clone();
        let addrs: Vec<String> = (0..n_nodes).map(|i| c.sql_addr(i as u64).to_string()).collect();
        workers.push(tokio::spawn(async move {
            for i in 0..OPS {
                let key = ((p + i) as i64) % KEYS;
                let val = (p as i64) * 1000 + i as i64 + 1; // globally unique, > 0
                // round-robin connect; skip (record nothing) if no node accepts.
                let mut connected = None;
                for a in 0..addrs.len() {
                    let node = (p + i + a) % addrs.len();
                    let port = addrs[node].rsplit(':').next().expect("port");
                    let cs = format!("host=127.0.0.1 port={port} user=postgres");
                    if let Ok(Ok((cl, conn))) = tokio::time::timeout(
                        Duration::from_secs(8),
                        tokio_postgres::connect(&cs, tokio_postgres::NoTls),
                    ).await {
                        tokio::spawn(conn);
                        connected = Some(cl);
                        break;
                    }
                }
                if let Some(cl) = connected {
                    let _ = append_txn(&cl, &rec, p, key, val).await;
                }
            }
        }));
    }
    // Followers-only nemesis (leader fixed → quorum kept), one fault at a time,
    // is_finished() termination.
    let followers: Vec<u64> = (0..3u64).filter(|&i| i != leader).collect();
    let mut round = 0usize;
    const MIN_ROUNDS: usize = 3;
    while !workers.iter().all(|w| w.is_finished()) || round < MIN_ROUNDS {
        let victim = followers[round % followers.len()];
        if round % 2 == 0 {
            c.kill(victim).await;
            c.respawn(victim);
        } else {
            let others: Vec<u64> = (0..3u64).filter(|&i| i != victim).collect();
            c.control(victim, cluster::transport::protocol::ControlRequest::SetPartition(others)).await;
            for &o in (0..3u64).collect::<Vec<_>>().iter().filter(|&&i| i != victim) {
                c.control(o, cluster::transport::protocol::ControlRequest::SetPartition(vec![victim])).await;
            }
            for id in 0..3u64 {
                c.control(id, cluster::transport::protocol::ControlRequest::Heal).await;
            }
        }
        round += 1;
    }
    for w in workers { let _ = w.await; }
    for id in 0..3u64 {
        c.control(id, cluster::transport::protocol::ControlRequest::Heal).await;
    }
    let events = rec.take_sorted();
    // Non-vacuous: at least a few committed transactions (Returns) were recorded.
    let returns = events.iter().filter(|e| matches!(e, Event::Return { .. })).count();
    assert!(returns >= 3, "workload must commit several transactions (got {returns})");
    // The committed path is strict-serializable per key.
    assert!(all_keys_consistent(&events), "list-append history must be strict-serializable");
}
```
NOTE: the nemesis is followers-only / one-fault-at-a-time so the leader keeps quorum (the workload's writes commit); clients round-robin across nodes (exercising D2c routing). The `Invoke`-at-BEGIN / `Return`-at-COMMIT recording gives the tester each txn's real-time window, so a read reflecting its snapshot (snapshot isolation) is NOT a false positive.

- [ ] **Step 2: Run (3×, non-flaky).**
```
__COMPAT_LAYER=RunAsInvoker cargo test -p crabgresql --test jepsen_elle list_append_is_strict 2>&1 | grep "test result"
```
Expected: passes 3× (the committed single-key list-append path is strict-serializable). If it FALSE-fails (anomaly reported on the leader-fixed path): check the recording stamps Invoke at BEGIN and Return at COMMIT (not a single point), confirm `list_from` parses the SELECT in row order, and confirm the nemesis never faults two nodes at once (leader must keep quorum). Do NOT weaken the assertion to pass.

- [ ] **Step 3: Verify + commit.**
```
cargo clippy -p crabgresql --all-targets -- -D warnings
cargo fmt -p crabgresql
git add crates/crabgresql/tests/jepsen_elle.rs
git commit -m "test(crabgresql): list-append workload + strict-serializability passing gate under follower faults"
```

---

### Task 4: Scenario B (leader-failover gap-finder) + EDN artifact

**Files:** Modify `crates/crabgresql/tests/jepsen_elle.rs`.

Orchestrate a stale read on a deposed-but-not-yet-stepped-down leader; assert the checker FLAGS it (the D5 gap is present); emit the EDN artifact.

- [ ] **Step 1: Add the gap-finder.**
```rust
/// A single-key read transaction: BEGIN; SELECT the list; COMMIT. Records an
/// Invoke(Read) + Return(list). Used for the stale read in the gap-finder.
async fn read_txn(client: &tokio_postgres::Client, rec: &Recorder, process: usize, key: i64) -> Vec<i64> {
    let inv = rec.next_seq();
    rec.push(Event::Invoke { process, key, seq: inv, op: ListOp::Read });
    let msgs = tokio::time::timeout(
        Duration::from_secs(10),
        client.simple_query(&format!("SELECT val FROM appends WHERE key = {key}")),
    ).await.expect("read not timed out").expect("read ok");
    let list = list_from(&msgs);
    let ret = rec.next_seq();
    rec.push(Event::Return { process, key, seq: ret, ret: ListRet(list.clone()) });
    list
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failover_surfaces_stale_read_d5_gap() {
    use cluster::transport::protocol::ControlRequest;
    const KEY: i64 = 7;
    // The gap is a timing window (a deposed leader serving a stale local read
    // before it steps down). Try a bounded number of times to hit it.
    for attempt in 0..8 {
        let mut c = harness::Cluster::spawn(3).await;
        let l = c.wait_for_leader().await;
        let rec = Recorder::default();
        // Seed: append 1 to KEY via the leader (process 0).
        {
            let setup = c.pg(l).await;
            setup.simple_query("CREATE TABLE appends (key int8, val int8)").await.expect("create");
        }
        let v1 = 1;
        let ok1 = append_txn(&c.pg(l).await, &rec, 0, KEY, v1).await;
        assert!(ok1, "seed append must commit");
        // Isolate the leader L; the majority elects L'.
        let others: Vec<u64> = (0..3u64).filter(|&i| i != l).collect();
        c.control(l, ControlRequest::SetPartition(others.clone())).await;
        for &o in &others {
            c.control(o, ControlRequest::SetPartition(vec![l])).await;
        }
        // Wait (bounded) for a NEW leader among the survivors.
        let neu = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            loop {
                let mut found = None;
                for &o in &others {
                    if let Some(st) = c.status(o).await
                        && st.current_leader.is_some_and(|x| x != l)
                    {
                        found = st.current_leader;
                    }
                }
                if let Some(x) = found { break x; }
                if tokio::time::Instant::now() >= deadline { break l; } // no failover; retry attempt
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        if neu == l {
            // failover didn't happen in time; heal and retry
            for id in 0..3u64 { c.control(id, ControlRequest::Heal).await; }
            continue;
        }
        // Commit append 2 via the new leader L' (process 1). Routed: connect to a
        // survivor; it proxies to L'.
        let ok2 = append_txn(&c.pg(neu).await, &rec, 1, KEY, 2).await;
        if !ok2 {
            for id in 0..3u64 { c.control(id, ControlRequest::Heal).await; }
            continue; // couldn't commit on L' in time; retry
        }
        // Within L's lease window, read KEY DIRECTLY from the deposed-but-not-yet-
        // stepped-down L (process 2). L still self-reports leader, so serve_routed
        // serves locally from L's stale state — observing [1], missing the acked 2.
        let stale = read_txn(&c.pg(l).await, &rec, 2, KEY).await;
        // Heal regardless.
        for id in 0..3u64 { c.control(id, ControlRequest::Heal).await; }
        if stale == vec![1] {
            // Got the stale read. The checker MUST flag the violation (the read
            // missed the acked append 2) — this documents the D5 gap.
            let events = rec.take_sorted();
            assert!(
                !all_keys_consistent(&events),
                "stale read [1] after acked append 2 must be a serializability violation (D5 gap); \
                 if this FAILS because the read was fresh, D5 may be fixed — flip to assert(all_keys_consistent)"
            );
            // Emit the EDN artifact for offline real-Elle cross-validation.
            let edn = history_to_elle_edn(&events);
            let path = std::env::temp_dir().join("crabgresql-sp11-d5-gap.edn");
            std::fs::write(&path, edn).expect("write edn");
            eprintln!("wrote Elle EDN history to {}", path.display());
            return; // success: gap surfaced + flagged
        }
        // Read wasn't stale (L already stepped down, or routed to L'): retry.
        let _ = attempt;
    }
    panic!("could not surface a stale read within the attempt budget (the D5 window is timing-bound)");
}
```
NOTE on determinism: with `election_timeout` 1000–2000 ms (the node's config), the deposed leader L self-reports leader for ~1–2 s after isolation — wide enough to commit on L' and read L. The 8-attempt budget absorbs the occasional miss (L stepped down first, or the failover/commit was slow). The EDN artifact is written only on the success path. **Connecting directly to L** (`c.pg(l)`): the app-layer partition only drops inter-node Raft RPCs, so the harness→L SQL connection still reaches L, and L self-serves (it still thinks it's leader).

- [ ] **Step 2: Run (3×, non-flaky).**
```
__COMPAT_LAYER=RunAsInvoker cargo test -p crabgresql --test jepsen_elle leader_failover_surfaces 2>&1 | grep "test result"
```
Expected: passes 3× (the stale read is surfaced within the budget and the checker flags it). If it exhausts the budget (panics): widen the window (confirm the node's `election_timeout` is the long 1000–2000 ms config) or increase the attempt budget; confirm the read targets L directly and L still self-reports leader at read time (add an `eprintln!` of `c.status(l)` to diagnose). Do NOT make it pass by removing the anomaly assertion.

- [ ] **Step 3: Verify + commit.**
```
cargo clippy -p crabgresql --all-targets -- -D warnings
cargo fmt -p crabgresql
git add crates/crabgresql/tests/jepsen_elle.rs
git commit -m "test(crabgresql): leader-failover gap-finder surfaces the D5 stale read; emit Elle EDN artifact"
```

---

### Task 5: Gauntlet, traceability, finish

**Files:** Verify; no new code unless a gate fails.

- [ ] **Step 1: Gauntlet.** Run each, report PASS/FAIL:
```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
__COMPAT_LAYER=RunAsInvoker cargo test --workspace
cargo test -p pgparser --features oracle
bash scripts/check-no-native.sh        # green on Linux CI; locally only windows-sys (known false-positive)
cargo deny check
```
No new dependency and no production-code change, so `check-no-native.sh` / `cargo deny` are unaffected. Confirm the new `jepsen_elle` tests run under the compat shim (and on Linux CI).

- [ ] **Step 2: Success-criteria traceability.** Confirm each spec criterion maps to a green test:

| # | Spec criterion | Verifying test(s) |
|---|---|---|
| 1 | List-append over the real cluster, checked for strict serializability via stateright | `jepsen_elle::list_append_is_strict_serializable_under_follower_faults` |
| 2 | Scenario A (leader-fixed, follower faults + routing) passes | same as #1 |
| 3 | Checker has teeth — a non-serializable history is rejected | `jepsen_elle::checker_rejects_a_stale_read_history` |
| 4 | Scenario B surfaces a stale read; checker flags it (D5 gap documented) | `jepsen_elle::leader_failover_surfaces_stale_read_d5_gap` |
| 5 | Elle-compatible EDN artifact emitted | `jepsen_elle::edn_format_round_trips_a_small_history`; the artifact write in #4 |
| 6 | Pure-Rust, no new dep, no production change; gauntlet green; prior suites pass | gauntlet (Step 1) |

If any row lacks a green test, add it.

- [ ] **Step 3: Final whole-diff review + finish.** Dispatch a code-reviewer over the SP11 diff (focus: the recording captures real-time order via Invoke@BEGIN/Return@COMMIT + the global seq — so Scenario A isn't a false-positive and Scenario B's stale read is genuinely flagged; the per-key partitioning is correct; indeterminate txns are in-flight invokes; the gap-finder's bounded-retry can't hang and surfaces a REAL stale read; no production-code change). Then run `superpowers:finishing-a-development-branch`.

- [ ] **Step 4: Commit (if anything changed).**
```
git add -A
git commit -m "test(sp11): gauntlet green; D2d over-the-wire serializability traceability"
```

---

## Self-Review

**Spec coverage:** the `AppendList` stateright reference object + strict-serializability check (T1); the checker teeth meta-test (T1); the history recorder with real-time invoke/return events (T2); the EDN emitter + format test (T2); the single-key list-append workload over the real cluster via tokio-postgres + routing (T3); Scenario A leader-fixed passing gate under followers-only faults (T3); Scenario B leader-failover gap-finder surfacing + flagging the D5 stale read (T4); EDN artifact emission (T4); gauntlet + traceability (T5); test-only / no production change (all tasks); no new dependency (stateright/tokio-postgres already dev-deps). All spec sections map to tasks.

**Placeholder scan:** the EDN emitter step notes "the exact loop shape is yours" but states the precise required output (one `:txn` op-map per transaction) and pins it with a unit test — a latitude note with a concrete contract, not a vague TODO. The workload SQL, the reference object, the recorder, both scenarios, and the meta-tests carry complete code.

**Type consistency:** `AppendList`/`ListOp{AppendRead,Read}`/`ListRet(Vec<i64>)` (T1) used identically in the recorder, checker, EDN, and both scenarios; `Event{Invoke,Return}` + `Recorder{next_seq,push,take_sorted}` (T2) used by `append_txn`/`read_txn` and the scenarios (T3/T4); `all_keys_consistent`/`key_consistent` (T2) called by both scenarios; `list_from` (T3) used by `append_txn` + `read_txn`; `LinearizabilityTester::{new,on_invoke,on_return,is_consistent}` + `SequentialSpec` match the verified stateright 0.31 API; the harness methods (`spawn`/`pg`/`pg_try`/`kill`/`control`/`status`/`wait_for_leader`/`sql_addr`/`len`) match the D2b/D2c harness.
