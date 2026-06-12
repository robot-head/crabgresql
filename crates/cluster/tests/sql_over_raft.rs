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
    c.isolate(l0);
    let l1 = c.wait_for_leader_excluding(l0).await;
    assert_ne!(l1, l0, "a new, different leader took over");

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
