//! Jepsen-style consistency testing (SP7 Task 8): a randomized concurrent
//! workload run against the replicated SQL engine *while a nemesis injects
//! faults*, with every operation recorded into a history that is then checked
//! for a safety property.
//!
//! Two workloads live here:
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
//! The history recorder (`HistEntry` / `OpKind` / `Outcome`) is a plain
//! `Vec<HistEntry>` carrying a process id, an op, and an outcome — enough to
//! later serialize to Elle/EDN, though we do not actually serialize here.
//!
//! Everything is bounded: every commit that could block under a fault is wrapped
//! in `tokio::time::timeout`, and a stuck/erroring commit becomes an `info`
//! (indeterminate) history entry rather than a hang.

use std::sync::Arc;
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
/// op/outcome pair is all an Elle/EDN export would need.)
#[derive(Debug, Clone)]
struct HistEntry {
    process: usize,
    op: OpKind,
    outcome: Outcome,
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
    let nemesis_cluster = Arc::clone(&c);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let nemesis_stop = Arc::clone(&stop);
    let nemesis = tokio::spawn(async move {
        let mut i = 0usize;
        while !nemesis_stop.load(std::sync::atomic::Ordering::Relaxed) {
            let victim = followers[i % followers.len()];
            nemesis_cluster.pause(victim);
            tokio::time::sleep(Duration::from_millis(40)).await;
            nemesis_cluster.resume(victim);
            tokio::time::sleep(Duration::from_millis(30)).await;
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
    let c = Arc::new(cluster::Cluster::new(3).await);
    let leader = c.wait_for_leader().await;
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

    // Light nemesis: pause/resume one follower throughout (leader fixed).
    let follower = (0..3u64).find(|&n| n != leader).expect("a follower");
    let nemesis_cluster = Arc::clone(&c);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let nemesis_stop = Arc::clone(&stop);
    let nemesis = tokio::spawn(async move {
        while !nemesis_stop.load(std::sync::atomic::Ordering::Relaxed) {
            nemesis_cluster.pause(follower);
            tokio::time::sleep(Duration::from_millis(50)).await;
            nemesis_cluster.resume(follower);
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        nemesis_cluster.heal();
    });

    // A modest, fixed number of processes/ops so the linearizability search stays
    // cheap. Each process alternates writes and reads.
    const PROCS: usize = 3;
    const OPS: usize = 6;
    let mut workers = Vec::new();
    for process in 0..PROCS {
        let engine = Arc::clone(&engine);
        workers.push(tokio::spawn(async move {
            let mut local: Vec<HistEntry> = Vec::new();
            let mut rng = Lcg::new(0x1234_5678 ^ process as u64);
            let mut s = engine.connect();
            for k in 0..OPS {
                // Alternate: even k → write a process-tagged value, odd k → read.
                if k % 2 == 0 {
                    let val = (process as i64 + 1) * 100 + k as i64;
                    let sql = format!("UPDATE reg SET v = {val} WHERE id = 0");
                    let r =
                        tokio::time::timeout(Duration::from_secs(10), s.simple_query(&sql)).await;
                    let outcome = match r {
                        Ok(Ok(rs)) if tag_of(&rs[0]) == "UPDATE 1" => Outcome::ok_value(val),
                        // A write that did not clearly succeed is indeterminate.
                        _ => Outcome::Info,
                    };
                    local.push(HistEntry {
                        process,
                        op: OpKind::RegWrite(val),
                        outcome,
                    });
                } else {
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

    // Preserve per-process order (each process's ops are sequential), which is
    // what the linearizability tester needs (one in-flight op per thread).
    let mut by_process: Vec<Vec<HistEntry>> = vec![Vec::new(); PROCS];
    for (process, w) in workers.into_iter().enumerate() {
        by_process[process] = w.await.expect("worker joined");
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    nemesis.await.expect("nemesis joined");
    c.heal();

    // Flatten for the returned history (process order is preserved within each
    // thread, which is all the checker requires).
    let history: Vec<HistEntry> = by_process.iter().flatten().cloned().collect();

    // Feed the history into stateright's LinearizabilityTester. Thread id = the
    // process id (Copy + Ord + Debug). The register starts at 0.
    let mut tester: LinearizabilityTester<usize, Register<i64>> =
        LinearizabilityTester::new(Register(0));
    for thread in &by_process {
        for e in thread {
            match (&e.op, &e.outcome) {
                (OpKind::RegWrite(v), Outcome::Ok { .. }) => {
                    tester
                        .on_invoke(e.process, RegisterOp::Write(*v))
                        .expect("valid invoke")
                        .on_return(e.process, RegisterRet::WriteOk)
                        .expect("valid return");
                }
                (OpKind::RegWrite(v), Outcome::Info) => {
                    // Indeterminate write: record the invoke but NO return, so the
                    // tester treats it as in-flight (it may linearize it anywhere
                    // or leave it out) — the honest modeling of an unknown effect.
                    tester
                        .on_invoke(e.process, RegisterOp::Write(*v))
                        .expect("valid invoke");
                }
                (OpKind::RegRead, Outcome::Ok { value: Some(v), .. }) => {
                    tester
                        .on_invoke(e.process, RegisterOp::Read)
                        .expect("valid invoke")
                        .on_return(e.process, RegisterRet::ReadOk(*v))
                        .expect("valid return");
                }
                // A read with no value is dropped (nothing observed to constrain).
                _ => {}
            }
        }
    }

    (history, tester.is_consistent())
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
