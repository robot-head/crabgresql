//! SQL-over-Raft end-to-end (SP7 Task 7): the full SP1-SP6 SQL/MVCC/concurrency
//! stack runs over a 3-node in-process Raft group via a `RaftCommitter`.
//!
//! Reads hit each node's applied state machine (`sm_kv`); writes propose through
//! Raft, so a batch resolves only once committed to a majority and applied. The
//! tests prove (a) CRUD matches single-node, (b) committed data — including a
//! `CREATE TABLE` routed through Raft — survives leader failover, and (c) the
//! SP6 row-locking / EvalPlanQual conflict loop works unchanged atop Raft.

use std::sync::Arc;
use std::time::Duration;

use executor::SqlSession;
use kv::Kv;
use pgwire::engine::{Cell, Engine, QueryResult, Session};

// ---------------------------------------------------------------------------
// Helpers (mirroring crates/executor/tests/concurrency.rs)
// ---------------------------------------------------------------------------

async fn run(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql).await.expect("query ok")
}

fn tag_of(r: &QueryResult) -> &str {
    match r {
        QueryResult::Command { tag } => tag,
        QueryResult::Rows { tag, .. } => tag,
        o => panic!("{o:?}"),
    }
}

/// Column 0 of every row, decoded as UTF-8 text (NULL → None).
fn col0(r: &QueryResult) -> Vec<Option<String>> {
    match r {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row[0]
                    .as_ref()
                    .map(|c: &Cell| String::from_utf8(c.text.to_vec()).expect("utf8"))
            })
            .collect(),
        o => panic!("{o:?}"),
    }
}

/// Number of rows in a `Rows` result.
fn rowcount(r: &QueryResult) -> usize {
    match r {
        QueryResult::Rows { rows, .. } => rows.len(),
        o => panic!("{o:?}"),
    }
}

/// After a failover, wait for `node` (the new leader) to APPLY up to `idx` — the
/// pre-failover leader's last log index, captured before isolation. Winning the
/// election places the committed entries in the new leader's log, but apply lags
/// the leadership signal, so reading `sm_kv` immediately races (it did on CI).
async fn wait_applied(node: &cluster::Node, idx: Option<u64>) {
    if let Some(i) = idx {
        node.raft
            .wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(Some(i), "new leader applied pre-failover commits")
            .await
            .expect("apply catch-up");
    }
}

// ---------------------------------------------------------------------------
// (a) CRUD over Raft matches single-node
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crud_over_raft_matches_single_node() {
    let c = cluster::Cluster::new(3).await;
    let lid = c.wait_for_leader().await;
    let engine = c.node(lid).engine();
    engine.reseed_counters().expect("reseed");
    let mut s = engine.connect();

    run(&mut s, "CREATE TABLE t (id int4, v text)").await;
    run(&mut s, "INSERT INTO t VALUES (1,'a'), (2,'b')").await;

    let rows = run(&mut s, "SELECT v FROM t WHERE id = 2").await;
    assert_eq!(col0(&rows[0]), vec![Some("b".to_string())]);

    run(&mut s, "UPDATE t SET v='b2' WHERE id=2").await;
    let rows = run(&mut s, "SELECT v FROM t WHERE id=2").await;
    assert_eq!(col0(&rows[0]), vec![Some("b2".to_string())]);

    // DELETE round-trips too.
    let del = run(&mut s, "DELETE FROM t WHERE id=1").await;
    assert_eq!(tag_of(&del[0]), "DELETE 1");
    let rows = run(&mut s, "SELECT id FROM t").await;
    assert_eq!(rowcount(&rows[0]), 1);
}

// ---------------------------------------------------------------------------
// (b) Committed data (incl. a CREATE TABLE) survives leader failover
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_data_survives_leader_failover() {
    let c = cluster::Cluster::new(3).await;
    let l0 = c.wait_for_leader().await;
    {
        let e = c.node(l0).engine();
        e.reseed_counters().expect("reseed");
        let mut s = e.connect();
        run(&mut s, "CREATE TABLE t (id int4)").await;
        for i in 0..5 {
            run(&mut s, &format!("INSERT INTO t VALUES ({i})")).await;
        }
    }

    // Kill the leader; a surviving-majority node must take over.
    let commit_idx = c.node(l0).raft.metrics().borrow().last_log_index;
    c.isolate(l0);
    let l1 = c.wait_for_leader_excluding(l0).await;
    assert_ne!(l1, l0, "a new, different leader took over");
    wait_applied(c.node(l1), commit_idx).await;

    let e = c.node(l1).engine();
    e.reseed_counters().expect("reseed");
    let mut s = e.connect();

    // The CREATE TABLE and all 5 INSERTs were committed through Raft, so the new
    // leader's applied state machine has both the table and its rows.
    let rows = run(&mut s, "SELECT id FROM t").await;
    assert_eq!(
        rowcount(&rows[0]),
        5,
        "all committed rows (and the table) survive failover"
    );

    // The new leader accepts fresh writes.
    let ins = run(&mut s, "INSERT INTO t VALUES (99)").await;
    assert_eq!(tag_of(&ins[0]), "INSERT 0 1");
    let rows = run(&mut s, "SELECT id FROM t").await;
    assert_eq!(rowcount(&rows[0]), 6, "new leader's write landed");

    c.heal();
}

// ---------------------------------------------------------------------------
// (c) SP6 concurrency over the replicated path
// ---------------------------------------------------------------------------

/// Ported from `executor::tests::concurrency::same_row_update_blocks_then_read_committed_refinds`,
/// but over the replicated engine. Two sessions SHARE one engine (and thus one
/// `RowLockManager`/`ProcArray`): T1 holds the row lock via an open UPDATE; T2's
/// UPDATE on the same row blocks until T1 commits, then READ COMMITTED
/// EvalPlanQual re-finds the row and applies T2's change on top. Proves row
/// locking + MVCC + the conflict loop work unchanged when writes go through Raft.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_row_conflict_loop_over_raft() {
    let c = cluster::Cluster::new(3).await;
    let lid = c.wait_for_leader().await;

    // ONE engine, shared so both sessions use the same lockmgr/procarray.
    let engine = Arc::new(c.node(lid).engine());
    engine.reseed_counters().expect("reseed");

    {
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id int4, v text)").await;
        run(&mut s, "INSERT INTO t VALUES (1,'orig')").await;
    }

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "UPDATE t SET v='a' WHERE id=1").await; // holds the row lock

    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "BEGIN").await;
        // blocks until T1 releases, then EvalPlanQual re-finds the row at v='a'
        let r = run(&mut s, "UPDATE t SET v='b' WHERE id=1").await;
        run(&mut s, "COMMIT").await;
        tag_of(&r[0]).to_string()
    });

    // Let T2 reach the blocking acquire, then release the lock by committing T1.
    tokio::time::sleep(Duration::from_millis(100)).await;
    run(&mut t1, "COMMIT").await;

    let tag = tokio::time::timeout(Duration::from_secs(10), t2)
        .await
        .expect("t2 did not hang")
        .expect("t2 join");
    assert_eq!(tag, "UPDATE 1");

    let mut s = engine.connect();
    assert_eq!(
        col0(&run(&mut s, "SELECT v FROM t WHERE id=1").await[0]),
        vec![Some("b".into())]
    );
}

// ---------------------------------------------------------------------------
// (d) A committed locking-SELECT (FOR UPDATE) bumps next_xid across failover
// ---------------------------------------------------------------------------

/// Read the applied `next_xid` high-water mark from a node's state machine.
/// Stored as a big-endian u64; absent means a fresh store (1).
fn applied_next_xid(node: &cluster::Node) -> u64 {
    match node
        .sm_kv
        .get(&kv::key::next_xid_key())
        .expect("get next_xid")
    {
        Some(b) => u64::from_be_bytes(b.as_slice().try_into().expect("next_xid is u64")),
        None => 1,
    }
}

/// Regression for the committed-xid-reuse window in the replicated path: a
/// transaction that allocates an xid *only* via a locking SELECT (`FOR UPDATE`)
/// inside `BEGIN … COMMIT` writes NO data rows, so its `next_xid` advance reaches
/// the replicated state machine ONLY if the COMMIT batch folds `next_xid`.
/// Without the commit-time fold, only `clog[N]=Committed` replicates; after
/// failover the new leader reseeds from a stale `next_xid` (= N) and re-hands-out
/// xid N — whose clog entry is durably Committed — yielding dirty reads.
///
/// Teeth: we capture the xid the FOR-UPDATE txn will allocate (the applied
/// `next_xid` right before its BEGIN, since `begin_write` hands out exactly that
/// value), commit it, fail the leader over, and assert the new leader's applied
/// `next_xid` is *strictly greater* than that xid — i.e. the new leader can never
/// re-hand-out the committed xid. WITHOUT the fix the bump is lost, so the new
/// leader's applied `next_xid` equals the committed xid (reuse possible) and the
/// assertion FAILS; WITH the fix it is one greater and the assertion PASSES.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_for_update_bumps_next_xid_across_failover() {
    let c = cluster::Cluster::new(3).await;
    let l0 = c.wait_for_leader().await;
    let engine0 = c.node(l0).engine();
    engine0.reseed_counters().expect("reseed");

    {
        let mut s = engine0.connect();
        // These replicate data + fold next_xid via their write entries.
        run(&mut s, "CREATE TABLE t (id int4)").await;
        run(&mut s, "INSERT INTO t VALUES (1)").await;
    }

    // The xid the FOR-UPDATE txn is about to allocate: `begin_write` hands out the
    // current `next_xid`. In Replicated mode it is NOT persisted at allocation, so
    // the applied store here still equals the value about to be handed out.
    let for_update_xid = applied_next_xid(c.node(l0));

    // The dangerous case: a transaction whose only xid allocation is a locking
    // SELECT. It commits clog[N]=Committed with NO data write. The commit-time
    // next_xid fold must carry the bump (N -> N+1) into the replicated state.
    {
        let mut s = engine0.connect();
        run(&mut s, "BEGIN").await;
        let rows = run(&mut s, "SELECT id FROM t WHERE id=1 FOR UPDATE").await;
        assert_eq!(col0(&rows[0]), vec![Some("1".to_string())]);
        run(&mut s, "COMMIT").await;
    }

    // With the fix, the old leader's applied next_xid advanced past the FOR-UPDATE
    // xid; without it, it is unchanged (still == for_update_xid).
    let x_l0 = applied_next_xid(c.node(l0));
    assert!(
        x_l0 > for_update_xid,
        "leader's applied next_xid ({x_l0}) did not advance past the committed FOR UPDATE xid ({for_update_xid})"
    );

    // Fail the leader over; a surviving-majority node takes over. Capture the
    // commit's log index first so we can wait for the new leader to APPLY it
    // before reading sm_kv (election gives it the entry; apply lags — this read
    // raced on CI otherwise).
    let commit_idx = c.node(l0).raft.metrics().borrow().last_log_index;
    c.isolate(l0);
    let l1 = c.wait_for_leader_excluding(l0).await;
    assert_ne!(l1, l0, "a new, different leader took over");
    wait_applied(c.node(l1), commit_idx).await;

    // The committed FOR-UPDATE txn's next_xid bump replicated to the new leader,
    // so its applied next_xid is strictly above the committed xid — that xid can
    // never be re-handed-out. WITHOUT the fix the bump was lost: l1's applied
    // next_xid == for_update_xid, and a reseeded leader would reuse the xid whose
    // clog is durably Committed (dirty reads). This assertion is the teeth.
    let x_l1 = applied_next_xid(c.node(l1));
    assert!(
        x_l1 > for_update_xid,
        "new leader's applied next_xid ({x_l1}) would re-hand-out the durably-committed FOR UPDATE xid ({for_update_xid}) — the bump was lost across failover"
    );

    // Stronger check: after reseed the new leader allocates a fresh, non-reused
    // xid. A fresh INSERT lands and a clean SELECT sees both rows with no phantom.
    let e1 = c.node(l1).engine();
    e1.reseed_counters().expect("reseed");
    let mut s = e1.connect();
    let ins = run(&mut s, "INSERT INTO t VALUES (2)").await;
    assert_eq!(tag_of(&ins[0]), "INSERT 0 1");
    let rows = run(&mut s, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(
        col0(&rows[0]),
        vec![Some("1".to_string()), Some("2".to_string())],
        "new leader sees both rows, no phantom from a reused xid"
    );

    c.heal();
}
