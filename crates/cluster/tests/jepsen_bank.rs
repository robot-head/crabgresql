//! Jepsen-style consistency testing (SP7 Task 8): a randomized concurrent
//! workload run against the replicated SQL engine *while a nemesis injects
//! faults*, with every operation recorded into a history that is then checked
//! for a safety property.
//!
//! Three workloads live here:
//!
//! 1. **Bank conservation** (`bank_conserves_total_under_nemesis`, the primary
//!    deliverable). N accounts are seeded so the total `T = N * seed` is known.
//!    Concurrent "processes" each run a transfer transaction
//!    (`BEGIN; UPDATE -amt; UPDATE +amt; COMMIT`) between two random accounts.
//!    A transfer nets zero, so *as long as each transaction is atomic* the total
//!    is conserved no matter how transfers interleave or fail. The nemesis pauses
//!    and resumes a **follower** throughout, exercising real replication faults
//!    (the leader must still reach a majority) without moving the leader — which
//!    keeps the workload's engine stable and the test deterministic. After the
//!    run we heal, re-resolve the leader, and assert the final total equals the
//!    seeded invariant. Conservation here is a real property of SP4-SP6
//!    transaction atomicity carried over Raft; a violation would be a genuine bug.
//!
//! 2. **Register linearizability** (`register_history_is_linearizable`, the
//!    secondary deliverable). Concurrent processes read/write a single key
//!    through the cluster under a light nemesis. Each invoke/return is fed to
//!    stateright's [`LinearizabilityTester`] over its [`register::Register`]
//!    reference object, and we assert the recorded history is linearizable.
//!    Writes go through Raft and reads hit the leader's applied state machine
//!    (write-through-Raft + read-applied-on-leader); because the nemesis only
//!    perturbs a follower the leader never changes, so this is a genuine
//!    linearizable path (no stale cross-failover reads, which D1 does not
//!    guarantee — that is the documented D5 gap).
//!
//! 3. **Durable bank conservation under crash/restart**
//!    (`bank_conserves_total_under_crash_restart`, the SP8 deliverable). The same
//!    bank workload, but on a **durable** (fjall-backed) cluster while a nemesis
//!    **crashes and restarts followers** — one at a time so the leader keeps a
//!    majority — mid-run. The leader is held fixed (the shared engine pins its
//!    on-disk `Database`, whose dir fjall locks exclusively), so the nemesis only
//!    ever bounces followers. After the workload the WHOLE set is power-cycled
//!    once (drop the engine to free the leader's lock, then crash+restart every
//!    node), the leader is re-resolved, and the final total is read from a fresh
//!    engine. The invariant is the SP8 durability claim: no acknowledged transfer
//!    is lost across crash/restart or a full-set power loss — the bank total is
//!    conserved because every acked COMMIT was fsync'd and survives journal replay.
//!
//! The history recorder (`HistEntry` / `OpKind` / `Outcome`) is a plain
//! `Vec<HistEntry>` carrying a process id, an op, and an outcome — enough to
//! later serialize to Elle/EDN, though we do not actually serialize here.
//!
//! Everything is bounded: every commit that could block under a fault is wrapped
//! in `tokio::time::timeout`, and a stuck/erroring commit becomes an `info`
//! (indeterminate) history entry rather than a hang.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use executor::{SqlEngine, SqlSession};
use pgwire::engine::{Cell, Engine, QueryResult, Session};
use stateright::semantics::register::{Register, RegisterOp, RegisterRet};
use stateright::semantics::{ConsistencyTester, LinearizabilityTester};

// ---------------------------------------------------------------------------
// History model (a Vec<HistEntry> is enough to later emit Elle/EDN).
// ---------------------------------------------------------------------------

/// The logical operation a process attempted.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OpKind {
    /// A bank transfer of `amt` from account `from` to account `to`.
    Transfer { from: i64, to: i64, amt: i64 },
    /// A read of the bank's total across all accounts (used by the checker
    /// loop and the final assertion).
    ReadTotal,
    /// Single-register read of the one key.
    RegRead,
    /// Single-register write of a value to the one key.
    RegWrite(i64),
}

/// The outcome of an attempted operation.
///
/// `Info` is the indeterminate case: a commit that errored with `Unavailable` /
/// a timeout, where we cannot know whether the transaction took effect. It is
/// recorded, never dropped, so the history stays honest.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    /// The operation definitely succeeded. `total` carries an observed total for
    /// `ReadTotal`; `value` carries the value read for `RegRead`.
    Ok {
        total: Option<i64>,
        value: Option<i64>,
    },
    /// The operation definitely did not take effect (e.g. overdraw skip, a clean
    /// serialization/abort, or a `BEGIN`-side error before any commit).
    Fail,
    /// Indeterminate: the commit errored or timed out and the effect is unknown.
    Info,
}

impl Outcome {
    fn ok_unit() -> Self {
        Outcome::Ok {
            total: None,
            value: None,
        }
    }
    fn ok_total(t: i64) -> Self {
        Outcome::Ok {
            total: Some(t),
            value: None,
        }
    }
    fn ok_value(v: i64) -> Self {
        Outcome::Ok {
            total: None,
            value: Some(v),
        }
    }
}

/// One recorded history entry: which process, what it attempted, how it ended.
/// (We fold invoke+return into a single completed entry; the process id plus the
/// op/outcome pair is all an Elle/EDN export would need.) This folded form is fine
/// for the bank *invariant* check, but NOT for the register *linearizability*
/// check, which needs real-time invoke/return ordering — see [`RegEvent`].
#[derive(Debug, Clone)]
struct HistEntry {
    /// Recorded for a future Elle/EDN export (see module docs); not otherwise read
    /// — the register linearizability check uses the real-time [`RegEvent`] log.
    #[allow(dead_code)]
    process: usize,
    op: OpKind,
    outcome: Outcome,
}

/// A timed register operation event for the linearizability checker. The global
/// `seq` is assigned at the *instant* of the invoke or the return; feeding events
/// in `seq` order reconstructs the real-time partial order, so concurrent ops
/// across processes overlap and a read that legally observed another process's
/// just-committed write is recognised as linearizable. (A folded per-process
/// history imposes a false total order and mis-flags such reads — the original
/// source of this test's flakiness.) `thread` is a unique per-op id (the invoke
/// seq), mirroring the jepsen_elle recorder, so an indeterminate write's stranded
/// invoke never collides with the same process's next op.
enum RegEvent {
    Invoke {
        seq: u64,
        thread: u64,
        op: RegisterOp<i64>,
    },
    Return {
        seq: u64,
        thread: u64,
        ret: RegisterRet<i64>,
    },
}

fn reg_seq(e: &RegEvent) -> u64 {
    match e {
        RegEvent::Invoke { seq, .. } | RegEvent::Return { seq, .. } => *seq,
    }
}

// ---------------------------------------------------------------------------
// Small SQL helpers (mirroring tests/sql_over_raft.rs).
// ---------------------------------------------------------------------------

fn tag_of(r: &QueryResult) -> &str {
    match r {
        QueryResult::Command { tag } => tag,
        QueryResult::Rows { tag, .. } => tag,
        o => panic!("unexpected result: {o:?}"),
    }
}

/// Column 0 of the first row, parsed as an `i64` (panics if missing/non-numeric).
fn first_i64(r: &QueryResult) -> Option<i64> {
    match r {
        QueryResult::Rows { rows, .. } => rows.first().map(|row| {
            let c: &Cell = row[0].as_ref().expect("non-null int");
            std::str::from_utf8(&c.text)
                .expect("utf8")
                .parse::<i64>()
                .expect("i64")
        }),
        o => panic!("unexpected result: {o:?}"),
    }
}

/// Is this `PgError` a retry/indeterminate class (not-leader, no-quorum,
/// serialization, deadlock)? Such an error means the commit did not durably and
/// observably succeed; for a COMMIT it is indeterminate, for a mid-txn statement
/// it is a clean failure (no clog Committed marker was written).
fn is_unavailable_class(code: &str) -> bool {
    // 40001 not-leader/serialization, 08006 no-quorum, 40P01 deadlock, 08* conn.
    code == "40001" || code == "40P01" || code.starts_with("08")
}

/// Read the whole bank's total by summing each account's balance. Aggregates
/// (`SUM`) are not in the SQL subset yet, so we read the N balances and add them
/// in Rust. Reads hit the leader's applied state machine.
async fn read_total(s: &mut SqlSession, accounts: i64) -> i64 {
    let mut total = 0;
    for id in 0..accounts {
        let r = s
            .simple_query(&format!("SELECT bal FROM accounts WHERE id = {id}"))
            .await
            .expect("read balance");
        total += first_i64(&r[0]).expect("balance row");
    }
    total
}

// ---------------------------------------------------------------------------
// Primary deliverable: bank conservation under a follower-pause nemesis.
// ---------------------------------------------------------------------------

/// Run the randomized bank workload and return `(history, final_total,
/// seeded_total)`. `accounts` accounts are each seeded to `SEED`, so the
/// invariant total is `accounts * SEED`. `procs` processes each perform `ops`
/// transfer transactions concurrently while a nemesis pauses/resumes a follower.
async fn run_bank_workload(accounts: i64, procs: usize, ops: usize) -> (Vec<HistEntry>, i64, i64) {
    const SEED: i64 = 100;
    let seeded_total = accounts * SEED;

    let c = Arc::new(cluster::Cluster::new(3).await);
    let leader = c.wait_for_leader().await;

    // ONE engine for the leader, shared (Arc) across all processes so they share
    // the RowLockManager / ProcArray — concurrent writers to the same account
    // serialize through real row locks, exactly as in single-node SP6.
    let engine = Arc::new(c.node(leader).engine());
    engine.reseed_counters().expect("reseed");

    // Seed the accounts table.
    {
        let mut s = engine.connect();
        s.simple_query("CREATE TABLE accounts (id int8, bal int8)")
            .await
            .expect("create");
        for id in 0..accounts {
            s.simple_query(&format!("INSERT INTO accounts VALUES ({id}, {SEED})"))
                .await
                .expect("seed");
        }
    }

    // Nemesis: pause/resume a FOLLOWER on a loose schedule. Pausing a follower
    // (never the leader) injects a real replication fault — the leader must still
    // reach a majority of {leader, other-follower} to commit — without moving the
    // leader, so the workload's engine stays valid and the test is deterministic.
    let followers: Vec<u64> = (0..3u64).filter(|&n| n != leader).collect();
    let lraft = c.node(leader).raft.clone();
    let nemesis_cluster = Arc::clone(&c);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let nemesis_stop = Arc::clone(&stop);
    let nemesis = tokio::spawn(async move {
        use std::sync::atomic::Ordering::Relaxed;
        let mut i = 0usize;
        while !nemesis_stop.load(Relaxed) {
            let victim = followers[i % followers.len()];
            // Pause the victim and wait (event-based) for the leader to commit
            // progress while it is down — the leader keeps quorum via {leader, other
            // follower}. Bounded; if the workload has finished the wait times out and
            // the loop re-checks stop. No fixed sleep.
            let before = lraft
                .metrics()
                .borrow()
                .last_applied
                .map(|l| l.index)
                .unwrap_or(0);
            nemesis_cluster.pause(victim);
            let _ = lraft
                .wait(Some(Duration::from_secs(2)))
                .metrics(
                    move |m| m.last_applied.map(|l| l.index).unwrap_or(0) > before,
                    "progress committed under fault",
                )
                .await;
            // Resume and wait for the victim to catch up before perturbing again, so
            // the fault cadence is paced on real replication progress, not the clock.
            nemesis_cluster.resume(victim);
            let target = lraft
                .metrics()
                .borrow()
                .last_applied
                .map(|l| l.index)
                .unwrap_or(0);
            let vraft = nemesis_cluster.node(victim).raft.clone();
            let _ = vraft
                .wait(Some(Duration::from_secs(2)))
                .metrics(
                    move |m| m.last_applied.map(|l| l.index).unwrap_or(0) >= target,
                    "victim caught up before next fault",
                )
                .await;
            i += 1;
        }
        // Always leave the cluster healthy for the final read.
        nemesis_cluster.heal();
    });

    // Worker processes. Each uses a deterministic but per-process-seeded LCG so
    // runs are varied yet reproducible-in-spirit and never depend on wall clock.
    let mut workers = Vec::new();
    for process in 0..procs {
        let engine = Arc::clone(&engine);
        workers.push(tokio::spawn(async move {
            let mut history: Vec<HistEntry> = Vec::new();
            let mut rng = Lcg::new(0x9E3779B9_u64.wrapping_mul(process as u64 + 1));
            let mut s = engine.connect();
            for _ in 0..ops {
                let from = (rng.next() % accounts as u64) as i64;
                let mut to = (rng.next() % accounts as u64) as i64;
                if to == from {
                    to = (to + 1) % accounts;
                }
                let amt = 1 + (rng.next() % 20) as i64;
                let entry = do_transfer(&engine, &mut s, from, to, amt, process).await;
                history.push(entry);
            }
            history
        }));
    }

    // Collect all process histories.
    let mut history: Vec<HistEntry> = Vec::new();
    for w in workers {
        history.extend(w.await.expect("worker joined"));
    }

    // Stop the nemesis and heal before the final read.
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    nemesis.await.expect("nemesis joined");
    c.heal();

    // Re-resolve the leader (it should not have moved, but be robust) and reseed
    // before the authoritative final read.
    let final_leader = c.wait_for_leader().await;
    let final_engine = if final_leader == leader {
        engine
    } else {
        let e = Arc::new(c.node(final_leader).engine());
        e.reseed_counters().expect("reseed");
        e
    };
    let mut s = final_engine.connect();
    let final_total = read_total(&mut s, accounts).await;
    history.push(HistEntry {
        process: usize::MAX,
        op: OpKind::ReadTotal,
        outcome: Outcome::ok_total(final_total),
    });

    (history, final_total, seeded_total)
}

/// Like `run_bank_workload` but on a DURABLE cluster with a crash+restart nemesis.
/// `accounts` accounts seeded to SEED (total = accounts*SEED). `procs` processes
/// each do `ops` transfers against the fixed leader while the main task crashes &
/// restarts FOLLOWERS one at a time (leader fixed so the shared engine stays
/// valid). After the workload, the WHOLE set is power-cycled once, then the final
/// total is read. Returns (history, final_total, seeded_total).
///
/// The nemesis is inline (not spawned) because `crash_restart` takes `&mut self`
/// on `Cluster`, which cannot be wrapped in `Arc` the way the sibling
/// `run_bank_workload` does for its pause/resume nemesis.
async fn run_durable_bank(
    base_dir: &std::path::Path,
    accounts: i64,
    procs: usize,
    ops: usize,
) -> (Vec<HistEntry>, i64, i64) {
    const SEED: i64 = 100;
    let seeded_total = accounts * SEED;

    let mut c = cluster::Cluster::durable(3, base_dir).await; // owned + mut: crash_restart(&mut self) is called inline by the nemesis below
    let leader = c.wait_for_leader().await;
    let engine = Arc::new(c.node(leader).engine());
    engine.reseed_counters().expect("reseed");

    // Seed accounts (same as run_bank_workload).
    {
        let mut s = engine.connect();
        s.simple_query("CREATE TABLE accounts (id int8, bal int8)")
            .await
            .expect("create");
        for id in 0..accounts {
            s.simple_query(&format!("INSERT INTO accounts VALUES ({id}, {SEED})"))
                .await
                .expect("seed");
        }
    }

    // Spawn workers (they hold ONLY Arc<engine>; never a cluster ref).
    let followers: Vec<u64> = (0..3u64).filter(|&n| n != leader).collect();
    let mut workers = Vec::new();
    for process in 0..procs {
        let engine = Arc::clone(&engine);
        workers.push(tokio::spawn(async move {
            let mut history: Vec<HistEntry> = Vec::new();
            let mut rng = Lcg::new(0x9E3779B9_u64.wrapping_mul(process as u64 + 1));
            let mut s = engine.connect();
            for _ in 0..ops {
                let from = (rng.next() % accounts as u64) as i64;
                let mut to = (rng.next() % accounts as u64) as i64;
                if to == from {
                    to = (to + 1) % accounts;
                }
                let amt = 1 + (rng.next() % 20) as i64;
                history.push(do_transfer(&engine, &mut s, from, to, amt, process).await);
            }
            history
        }));
    }

    // Crash+restart a follower at a time, round-robin, until every worker task has
    // finished — `is_finished()` is true whether a worker completes OR panics, so a
    // worker panic surfaces at the join below instead of hanging this loop. A small
    // MIN_RESTARTS floor guarantees a few crashes even if the workload is quick. No
    // fixed sleeps: each crash_restart's real shutdown+fsync+reopen I/O paces the loop.
    let mut restarts = 0usize;
    const MIN_RESTARTS: usize = 4;
    while !workers.iter().all(|w| w.is_finished()) || restarts < MIN_RESTARTS {
        let victim = followers[restarts % followers.len()];
        c.crash_restart(victim).await;
        restarts += 1;
    }

    // Join workers (their Arc<engine> clones drop here).
    let mut history: Vec<HistEntry> = Vec::new();
    for w in workers {
        history.extend(w.await.expect("worker joined"));
    }

    // Whole-set power-cycle barrier: drop the workload engine FIRST so the leader's
    // dir lock is free, then crash+restart EVERY node, re-resolve the leader, and
    // rebuild a fresh engine for the authoritative final read. Proves the seeded +
    // transferred state survives a full power loss of all replicas.
    drop(engine);
    for id in 0..3u64 {
        c.crash_restart(id).await;
    }
    let final_leader = c.wait_for_leader().await;
    let final_engine = c.node(final_leader).engine();
    final_engine.reseed_counters().expect("reseed");
    let mut s = final_engine.connect();
    let final_total = read_total(&mut s, accounts).await;
    history.push(HistEntry {
        process: usize::MAX,
        op: OpKind::ReadTotal,
        outcome: Outcome::ok_total(final_total),
    });

    (history, final_total, seeded_total)
}

/// Perform one transfer transaction and return its recorded history entry.
///
/// Reads `from`'s balance first and skips (records `Fail`) if it would overdraw.
/// Wraps the COMMIT in a timeout so a stuck commit under a fault becomes `Info`
/// (indeterminate), never a hang. Any mid-transaction error rolls the block back
/// (no clog Committed marker is ever written, so the txn's effect is nil) and is
/// recorded as `Fail`; a COMMIT that errors/times out is recorded as `Info`.
async fn do_transfer(
    engine: &Arc<SqlEngine>,
    s: &mut SqlSession,
    from: i64,
    to: i64,
    amt: i64,
    process: usize,
) -> HistEntry {
    let op = OpKind::Transfer { from, to, amt };
    let fail = |outcome| HistEntry {
        process,
        op: op.clone(),
        outcome,
    };

    // Helper: bounded statement execution. A blocking statement (e.g. waiting on
    // a row lock held by a paused-then-resumed peer) must not hang the test.
    async fn stmt(s: &mut SqlSession, sql: &str) -> Result<Vec<QueryResult>, StmtErr> {
        match tokio::time::timeout(Duration::from_secs(10), s.simple_query(sql)).await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(e)) => Err(StmtErr::Pg(e.code)),
            Err(_) => Err(StmtErr::Timeout),
        }
    }

    // BEGIN.
    if let Err(e) = stmt(s, "BEGIN").await {
        // A failed BEGIN never wrote anything; recover the session and fail.
        let _ = recover(engine, s).await;
        return fail(match e {
            StmtErr::Timeout => Outcome::Info,
            StmtErr::Pg(_) => Outcome::Fail,
        });
    }

    // Read `from`'s balance; skip the transfer (clean Fail) if it would overdraw.
    match stmt(s, &format!("SELECT bal FROM accounts WHERE id = {from}")).await {
        Ok(r) => {
            let bal = first_i64(&r[0]).unwrap_or(0);
            if bal < amt {
                let _ = stmt(s, "ROLLBACK").await;
                return fail(Outcome::Fail);
            }
        }
        Err(_) => {
            let _ = recover(engine, s).await;
            return fail(Outcome::Fail);
        }
    }

    // The two updates. A mid-txn error here leaves no Committed clog marker, so
    // the txn is all-or-nothing nil; roll back and record Fail.
    let upd1 = format!("UPDATE accounts SET bal = bal - {amt} WHERE id = {from}");
    let upd2 = format!("UPDATE accounts SET bal = bal + {amt} WHERE id = {to}");
    if stmt(s, &upd1).await.is_err() || stmt(s, &upd2).await.is_err() {
        let _ = recover(engine, s).await;
        return fail(Outcome::Fail);
    }

    // COMMIT. This is the only point whose failure is genuinely indeterminate:
    // the clog Committed batch may or may not have reached a majority. Record
    // Info, not Fail, and never let it hang.
    match stmt(s, "COMMIT").await {
        Ok(_) => fail(Outcome::ok_unit()),
        Err(StmtErr::Timeout) => {
            let _ = recover(engine, s).await;
            fail(Outcome::Info)
        }
        Err(StmtErr::Pg(code)) if is_unavailable_class(&code) => {
            let _ = recover(engine, s).await;
            fail(Outcome::Info)
        }
        // Any other COMMIT error means the commit batch was rejected outright
        // (clean abort): all-or-nothing nil, a definite Fail.
        Err(StmtErr::Pg(_)) => {
            let _ = recover(engine, s).await;
            fail(Outcome::Fail)
        }
    }
}

/// A statement error: a Postgres SQLSTATE, or a hard timeout under a fault.
enum StmtErr {
    Pg(String),
    Timeout,
}

/// Reset a session that may be in a Failed/aborted block by issuing a bounded
/// ROLLBACK, and replace it with a fresh connection if even that is stuck. This
/// guarantees the next transfer starts from a clean session.
async fn recover(engine: &Arc<SqlEngine>, s: &mut SqlSession) -> bool {
    if tokio::time::timeout(Duration::from_secs(5), s.simple_query("ROLLBACK"))
        .await
        .is_ok()
    {
        return true;
    }
    *s = engine.connect();
    false
}

/// A tiny deterministic LCG (Numerical Recipes constants) so workloads are
/// pseudo-random without pulling in `rand` or depending on the wall clock.
struct Lcg(u64);
impl Lcg {
    fn new(seed: u64) -> Self {
        Lcg(seed ^ 0xDEAD_BEEF_CAFE_F00D)
    }
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bank_conserves_total_under_nemesis() {
    let (history, final_total, seeded_total) =
        run_bank_workload(/*accounts*/ 4, /*procs*/ 3, /*ops*/ 40).await;

    assert_eq!(
        final_total, seeded_total,
        "transfers must conserve the bank total"
    );

    // Every committed/observed total read must equal the invariant.
    for e in &history {
        if let (OpKind::ReadTotal, Outcome::Ok { total: Some(t), .. }) = (&e.op, &e.outcome) {
            assert_eq!(
                *t, seeded_total,
                "every observed total equals the invariant"
            );
        }
    }

    // Sanity: the workload actually exercised the system (committed at least one
    // transfer), otherwise "conservation" would be vacuously true.
    let committed = history
        .iter()
        .filter(|e| {
            matches!(e.op, OpKind::Transfer { .. }) && matches!(e.outcome, Outcome::Ok { .. })
        })
        .count();
    assert!(committed > 0, "workload must commit at least one transfer");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bank_conserves_total_under_crash_restart() {
    let dir = tempfile::tempdir().expect("dir");
    let (history, final_total, seeded) = run_durable_bank(
        dir.path(),
        /*accounts*/ 4,
        /*procs*/ 3,
        /*ops*/ 12,
    )
    .await;

    assert_eq!(
        final_total, seeded,
        "no acked transfer lost across crash/restart + full-set power-cycle"
    );

    // Every observed ReadTotal equals the invariant.
    for e in &history {
        if let (OpKind::ReadTotal, Outcome::Ok { total: Some(t), .. }) = (&e.op, &e.outcome) {
            assert_eq!(*t, seeded, "every observed total equals the invariant");
        }
    }
    // Non-vacuous: at least one transfer actually committed.
    let committed = history
        .iter()
        .filter(|e| {
            matches!(e.op, OpKind::Transfer { .. }) && matches!(e.outcome, Outcome::Ok { .. })
        })
        .count();
    assert!(
        committed > 0,
        "workload must commit at least one transfer (non-vacuous)"
    );
}

// ---------------------------------------------------------------------------
// Secondary deliverable: single-register linearizability via stateright.
// ---------------------------------------------------------------------------

/// Run a concurrent single-register read/write workload under a light follower
/// nemesis, recording each op into a `Vec<HistEntry>` *and* directly into a
/// stateright [`LinearizabilityTester`]. Returns the recorded history and
/// whether the tester deemed it linearizable.
///
/// Writes propose through Raft (`UPDATE`), reads hit the leader's applied state
/// machine (read-applied-on-leader). Because the nemesis only pauses a follower,
/// the leader never changes, so reads are taken from the same authoritative
/// applied log position writes commit into — a genuinely linearizable path. (D1
/// has no read leases, so a *cross-failover* read could be stale; we deliberately
/// do not move the leader here. That stale-read-across-failover linearizability
/// is the documented D5 gap, out of scope for D1.)
///
/// We use stateright's own `ConsistencyTester` API (`on_invoke` / `on_return` /
/// `is_consistent`) — the externally-recorded-history interface it documents —
/// rather than a hand-rolled checker. To keep the linearizability search (which
/// is exponential in the worst case) tractable, the workload is intentionally
/// small (few processes, few ops). Indeterminate writes are recorded as an
/// invoke with NO matching return, which the tester treats as an in-flight op it
/// may place anywhere or omit — the honest modeling of an unknown outcome.
async fn run_register_workload() -> (Vec<HistEntry>, bool) {
    // Determinism comes primarily from feeding the history to the checker in
    // real-time order (see `try_register_run` / [`RegEvent`]), so a legal
    // concurrent read is never mis-flagged. As a secondary guard the premise also
    // wants the leader FIXED for the run; the nemesis only faults a follower so the
    // leader keeps quorum, but an extremely CPU-starved runner could still stall it
    // into an election. Rather than sleep-and-hope, if a run's leader term advanced
    // (an election happened) we discard it and retry on a fresh cluster. No `sleep`;
    // with no election the first attempt succeeds.
    const ATTEMPTS: usize = 10;
    for _ in 0..ATTEMPTS {
        if let Some(result) = try_register_run().await {
            return result;
        }
    }
    panic!("could not complete a register run with a fixed leader within {ATTEMPTS} attempts");
}

/// One register run over a fresh cluster. Returns `None` if a spurious election
/// moved the leader mid-run (the linearizability premise was void), so the caller
/// retries — keeping the test deterministic without relying on timing.
async fn try_register_run() -> Option<(Vec<HistEntry>, bool)> {
    let c = Arc::new(cluster::Cluster::new_stable_leader(3).await);
    let leader = c.wait_for_leader().await;
    let term0 = c.node(leader).raft.metrics().borrow().current_term;
    let engine = Arc::new(c.node(leader).engine());
    engine.reseed_counters().expect("reseed");

    // One-key register backed by a single row; start it at a known value 0.
    {
        let mut s = engine.connect();
        s.simple_query("CREATE TABLE reg (id int8, v int8)")
            .await
            .expect("create");
        s.simple_query("INSERT INTO reg VALUES (0, 0)")
            .await
            .expect("seed");
    }

    // Sleep-free follower-fault nemesis: pause a follower, wait (event-based) for the
    // workload to commit progress on the leader while it is down, resume, then wait
    // for the follower to catch up before the next fault. Paced on real progress,
    // never the clock; the leader keeps quorum throughout (leader + other follower).
    let follower = (0..3u64).find(|&n| n != leader).expect("a follower");
    let lraft = c.node(leader).raft.clone();
    let fraft = c.node(follower).raft.clone();
    let nemesis_cluster = Arc::clone(&c);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let nemesis_stop = Arc::clone(&stop);
    let nemesis = tokio::spawn(async move {
        use std::sync::atomic::Ordering::Relaxed;
        while !nemesis_stop.load(Relaxed) {
            let before = lraft
                .metrics()
                .borrow()
                .last_applied
                .map(|l| l.index)
                .unwrap_or(0);
            nemesis_cluster.pause(follower);
            // Wait for the workload to make progress under the fault (bounded; if the
            // workload has finished, the wait times out and the loop re-checks stop).
            let _ = lraft
                .wait(Some(Duration::from_secs(2)))
                .metrics(
                    move |m| m.last_applied.map(|l| l.index).unwrap_or(0) > before,
                    "progress committed under fault",
                )
                .await;
            nemesis_cluster.resume(follower);
            let target = lraft
                .metrics()
                .borrow()
                .last_applied
                .map(|l| l.index)
                .unwrap_or(0);
            let _ = fraft
                .wait(Some(Duration::from_secs(2)))
                .metrics(
                    move |m| m.last_applied.map(|l| l.index).unwrap_or(0) >= target,
                    "follower caught up before next fault",
                )
                .await;
        }
        nemesis_cluster.heal();
    });

    // A modest, fixed number of processes/ops so the linearizability search stays
    // cheap. Each process alternates writes and reads.
    const PROCS: usize = 3;
    const OPS: usize = 6;
    // Global sequence + shared event log. Each op timestamps its invoke and (if
    // determinate) its return with a fresh `seq`, so the events sort into the true
    // real-time order across all processes.
    let seq = Arc::new(AtomicU64::new(0));
    let events: Arc<Mutex<Vec<RegEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let mut workers = Vec::new();
    for process in 0..PROCS {
        let engine = Arc::clone(&engine);
        let seq = Arc::clone(&seq);
        let events = Arc::clone(&events);
        workers.push(tokio::spawn(async move {
            let mut local: Vec<HistEntry> = Vec::new();
            let mut rng = Lcg::new(0x1234_5678 ^ process as u64);
            let mut s = engine.connect();
            for k in 0..OPS {
                // Alternate: even k → write a process-tagged value, odd k → read.
                if k % 2 == 0 {
                    let val = (process as i64 + 1) * 100 + k as i64;
                    // Stamp the invoke the instant the op starts; `thread` (the
                    // invoke seq) is a unique per-op id.
                    let thread = seq.fetch_add(1, Ordering::SeqCst);
                    events.lock().expect("events").push(RegEvent::Invoke {
                        seq: thread,
                        thread,
                        op: RegisterOp::Write(val),
                    });
                    let sql = format!("UPDATE reg SET v = {val} WHERE id = 0");
                    let r =
                        tokio::time::timeout(Duration::from_secs(10), s.simple_query(&sql)).await;
                    let outcome = match r {
                        Ok(Ok(rs)) if tag_of(&rs[0]) == "UPDATE 1" => Outcome::ok_value(val),
                        // A write that did not clearly succeed is indeterminate.
                        _ => Outcome::Info,
                    };
                    // Only a definite commit returns; an indeterminate write leaves
                    // its invoke in-flight (the tester may linearize it or drop it).
                    if matches!(outcome, Outcome::Ok { .. }) {
                        let rseq = seq.fetch_add(1, Ordering::SeqCst);
                        events.lock().expect("events").push(RegEvent::Return {
                            seq: rseq,
                            thread,
                            ret: RegisterRet::WriteOk,
                        });
                    }
                    local.push(HistEntry {
                        process,
                        op: OpKind::RegWrite(val),
                        outcome,
                    });
                } else {
                    let thread = seq.fetch_add(1, Ordering::SeqCst);
                    events.lock().expect("events").push(RegEvent::Invoke {
                        seq: thread,
                        thread,
                        op: RegisterOp::Read,
                    });
                    let r = tokio::time::timeout(
                        Duration::from_secs(10),
                        s.simple_query("SELECT v FROM reg WHERE id = 0"),
                    )
                    .await;
                    let outcome = match r {
                        Ok(Ok(rs)) => match first_i64(&rs[0]) {
                            Some(v) => Outcome::ok_value(v),
                            None => Outcome::Info,
                        },
                        _ => Outcome::Info,
                    };
                    // A read that observed a value returns it; a read with no value
                    // is left in-flight (no observation to constrain linearization).
                    if let Outcome::Ok { value: Some(v), .. } = outcome {
                        let rseq = seq.fetch_add(1, Ordering::SeqCst);
                        events.lock().expect("events").push(RegEvent::Return {
                            seq: rseq,
                            thread,
                            ret: RegisterRet::ReadOk(v),
                        });
                    }
                    local.push(HistEntry {
                        process,
                        op: OpKind::RegRead,
                        outcome,
                    });
                }
                let _ = rng.next();
            }
            local
        }));
    }

    // Each worker's HistEntry list (used only for the observed-reads count); the
    // real-time event log in `events` is what the linearizability check consumes.
    let mut by_process: Vec<Vec<HistEntry>> = vec![Vec::new(); PROCS];
    for (process, w) in workers.into_iter().enumerate() {
        by_process[process] = w.await.expect("worker joined");
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    nemesis.await.expect("nemesis joined");
    c.heal();

    // Secondary guard on the fixed-leader premise: the nemesis only faults a
    // follower, so the leader keeps quorum and its term should not change — but an
    // extremely CPU-starved runner could still stall the leader into an election,
    // and an ungated stale read on the *deposed* leader (the documented D5 gap) is
    // out of scope here. If the term advanced or the leader moved, discard this run
    // so the caller retries on a fresh cluster. (Ordinary concurrency is handled by
    // the real-time modeling below, not by this guard.)
    {
        let m = c.node(leader).raft.metrics();
        let m = m.borrow();
        if m.current_term != term0 || m.current_leader != Some(leader) {
            return None;
        }
    }

    // Flatten for the returned history (used by the test only to assert the
    // workload actually observed some reads).
    let history: Vec<HistEntry> = by_process.iter().flatten().cloned().collect();

    // Feed the events into stateright's LinearizabilityTester IN REAL-TIME ORDER
    // (sorted by the global seq stamped at each invoke/return). Concurrent ops
    // across processes then overlap, so a read that legally observed another
    // process's just-committed write is recognised as linearizable — the per-process
    // feed this replaced imposed a false total order and mis-flagged such reads.
    // Thread id = the unique per-op invoke seq. The register starts at 0.
    let mut evs = events.lock().expect("events");
    evs.sort_by_key(reg_seq);
    let mut tester: LinearizabilityTester<u64, Register<i64>> =
        LinearizabilityTester::new(Register(0));
    for e in evs.drain(..) {
        match e {
            RegEvent::Invoke { thread, op, .. } => {
                tester.on_invoke(thread, op).expect("valid invoke");
            }
            RegEvent::Return { thread, ret, .. } => {
                tester.on_return(thread, ret).expect("valid return");
            }
        }
    }
    drop(evs);

    Some((history, tester.is_consistent()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn register_history_is_linearizable() {
    let (history, linearizable) = run_register_workload().await;

    // The history must actually contain observed reads, else linearizability is
    // vacuous.
    let observed_reads = history
        .iter()
        .filter(|e| {
            matches!(e.op, OpKind::RegRead)
                && matches!(e.outcome, Outcome::Ok { value: Some(_), .. })
        })
        .count();
    assert!(
        observed_reads > 0,
        "workload must observe at least one read"
    );

    assert!(
        linearizable,
        "single-register history over Raft (leader fixed) must be linearizable; \
         a violation would indicate the read-applied-on-leader path returned a \
         value inconsistent with the committed write order"
    );
}

/// Guards that the stateright checker is *actually checking* — i.e. it is not
/// vacuously accepting every history. We feed it a sequential (non-concurrent)
/// history that no register can produce — a read returning a value that was
/// never written, and out of "real time" order — and assert it is rejected.
/// If this ever passes the checker, the linearizability assertion above would be
/// meaningless, so this test pins the checker's real behavior.
#[test]
fn stateright_rejects_a_known_nonlinearizable_register_history() {
    let mut tester: LinearizabilityTester<usize, Register<i64>> =
        LinearizabilityTester::new(Register(0));
    // Thread 0 writes 1 and returns; then thread 0 reads and observes 7 — a value
    // that was never written. Sequenced (no concurrency), so no interleaving can
    // excuse it: not linearizable.
    tester
        .on_invoke(0, RegisterOp::Write(1))
        .expect("valid invoke")
        .on_return(0, RegisterRet::WriteOk)
        .expect("valid return")
        .on_invoke(0, RegisterOp::Read)
        .expect("valid invoke")
        .on_return(0, RegisterRet::ReadOk(7))
        .expect("valid return");
    assert!(
        !tester.is_consistent(),
        "checker must reject a read of a never-written value"
    );
}
