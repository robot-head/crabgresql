use std::sync::Arc;

use executor::SqlEngine;
use pgwire::engine::{Engine, QueryResult};

/// Regression test for the concurrent-INSERT lost-update bug.
///
/// Before the fix, two concurrent INSERTs both read seq=N, both allocate
/// rowids starting at N, and the second batch's `Put` overwrites the first's
/// rows at the same keys — silent data loss.
///
/// After the fix (write_lock serializes all writes), every read-modify-write
/// on the sequence is atomic from the perspective of other writers, so all N
/// rows survive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_inserts_do_not_lose_rows() {
    let engine = Arc::new(SqlEngine::new());
    engine
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");

    // Fan out N concurrent single-row inserts into the same table.
    const N: usize = 50;
    let mut handles = Vec::new();
    for i in 0..N {
        let e = Arc::clone(&engine);
        handles.push(tokio::spawn(async move {
            e.simple_query(&format!("INSERT INTO t VALUES ({i})"))
                .await
                .expect("insert");
        }));
    }
    for h in handles {
        h.await.expect("join");
    }

    // All N rows must be present — none overwritten by a colliding rowid.
    let mut results = engine
        .simple_query("SELECT id FROM t")
        .await
        .expect("select");
    let rows = match results.remove(0) {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {other:?}"),
    };
    assert_eq!(
        rows.len(),
        N,
        "concurrent inserts lost rows (rowid collision): got {} of {}",
        rows.len(),
        N
    );
}
