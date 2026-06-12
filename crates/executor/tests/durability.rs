//! Open a durable engine, write, drop it, reopen, and assert everything
//! survived — including the rowid allocator (the SP2 carry-over fix).

use executor::SqlEngine;
use pgwire::engine::{Cell, Engine, QueryResult, Session};

fn text(cell: &Option<Cell>) -> Option<String> {
    cell.as_ref()
        .map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}

async fn rows(engine: &SqlEngine, sql: &str) -> Vec<Vec<Option<Cell>>> {
    let mut results = engine.connect().simple_query(sql).await.expect("query");
    match results.remove(0) {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[tokio::test]
async fn data_schema_and_rowid_survive_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        engine
            .connect()
            .simple_query("CREATE TABLE t (id int4, name text)")
            .await
            .expect("create");
        engine
            .connect()
            .simple_query("INSERT INTO t VALUES (1,'a'),(2,'b')")
            .await
            .expect("insert");
        // engine dropped here — writes were fsynced per statement.
    }

    let engine = SqlEngine::open(dir.path()).expect("reopen");
    // Rows + schema survived.
    let got = rows(&engine, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(
        got.iter().map(|r| text(&r[0])).collect::<Vec<_>>(),
        vec![Some("a".into()), Some("b".into())]
    );
    // The rowid allocator survived: a new insert does NOT collide with id 1/2.
    // (rowids are the hidden key, not the id column; insert two more and confirm
    // all four rows are present and distinct.)
    engine
        .connect()
        .simple_query("INSERT INTO t VALUES (3,'c')")
        .await
        .expect("insert after reopen");
    let after = rows(&engine, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(
        after.len(),
        3,
        "all rows present, no overwrite from a reset rowid"
    );
    assert_eq!(
        after.iter().map(|r| text(&r[0])).collect::<Vec<_>>(),
        vec![Some("a".into()), Some("b".into()), Some("c".into())]
    );
}

#[tokio::test]
async fn drop_and_recreate_survive_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        engine
            .connect()
            .simple_query("CREATE TABLE t (id int4)")
            .await
            .expect("create");
        engine
            .connect()
            .simple_query("INSERT INTO t VALUES (1)")
            .await
            .expect("insert");
        engine
            .connect()
            .simple_query("DROP TABLE t")
            .await
            .expect("drop");
        engine
            .connect()
            .simple_query("CREATE TABLE t (id int4)")
            .await
            .expect("recreate");
    }
    let engine = SqlEngine::open(dir.path()).expect("reopen");
    // The recreated (empty) table survived; the dropped rows did not resurrect.
    let got = rows(&engine, "SELECT id FROM t").await;
    assert!(
        got.is_empty(),
        "dropped rows must not survive; recreated table is empty"
    );
}
