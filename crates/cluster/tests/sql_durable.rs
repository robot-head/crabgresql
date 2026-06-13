//! SQL-over-Raft durability e2e (SP8 T6): proves the full SP1-SP6 SQL/MVCC/
//! concurrency stack survives a full-cluster restart over durable (fjall) storage,
//! and that SP6 row-locking / EvalPlanQual concurrency works unchanged when the
//! storage backend is durable rather than in-memory.

use std::sync::Arc;
use std::time::Duration;

use executor::SqlSession;
use pgwire::engine::{Cell, Engine, QueryResult, Session};

// ---------------------------------------------------------------------------
// Helpers (verbatim from crates/cluster/tests/sql_over_raft.rs lines 10-53)
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
// Scenario 1 — full-cluster restart survival
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_data_survives_full_cluster_restart() {
    let dir = tempfile::tempdir().expect("dir");
    let mut c = cluster::Cluster::durable(3, dir.path()).await;
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
    // Restart every node (clean bounce). They recover from disk.
    for id in 0..3u64 {
        c.restart(id).await;
    }
    let l1 = c.wait_for_leader().await;
    let e = c.node(l1).engine();
    e.reseed_counters().expect("reseed");
    let mut s = e.connect();
    let rows = run(&mut s, "SELECT id FROM t").await;
    assert_eq!(
        rowcount(&rows[0]),
        5,
        "table + rows survive a full-cluster restart"
    );
    // New writes still land after recovery.
    let ins = run(&mut s, "INSERT INTO t VALUES (99)").await;
    assert_eq!(tag_of(&ins[0]), "INSERT 0 1");
    let rows = run(&mut s, "SELECT id FROM t").await;
    assert_eq!(rowcount(&rows[0]), 6, "post-restart write landed");
}

// ---------------------------------------------------------------------------
// Scenario 2 — SP6 row-locking / EvalPlanQual concurrency over the durable path
// ---------------------------------------------------------------------------

/// SP6 row-locking / EvalPlanQual conflict loop over the DURABLE replicated path.
/// Two sessions share one engine (one RowLockManager/ProcArray): T1 holds the row
/// lock via an open UPDATE; T2's UPDATE on the same row blocks until T1 commits,
/// then READ COMMITTED EvalPlanQual re-finds the row and applies T2's change on
/// top. Proves row locking + MVCC + the conflict loop work unchanged when the
/// storage is durable fjall (not in-memory).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_concurrency_over_durable() {
    let dir = tempfile::tempdir().expect("dir");
    let c = cluster::Cluster::durable(3, dir.path()).await;
    let lid = c.wait_for_leader().await;
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
        let r = run(&mut s, "UPDATE t SET v='b' WHERE id=1").await; // blocks on T1
        run(&mut s, "COMMIT").await;
        tag_of(&r[0]).to_string()
    });
    tokio::time::sleep(Duration::from_millis(100)).await; // let T2 reach the blocking acquire
    run(&mut t1, "COMMIT").await; // release the lock
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
