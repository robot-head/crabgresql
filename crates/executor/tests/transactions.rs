use executor::SqlEngine;
use pgwire::engine::{Cell, Engine, QueryResult, Session, TxStatus};

#[allow(dead_code)]
fn text(c: &Option<Cell>) -> Option<String> {
    c.as_ref()
        .map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}
async fn rows(s: &mut executor::SqlSession, sql: &str) -> Vec<Vec<Option<Cell>>> {
    match s.simple_query(sql).await.expect("q").remove(0) {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[tokio::test]
async fn rollback_discards_writes() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    s.simple_query("BEGIN").await.expect("begin");
    assert_eq!(s.tx_status(), TxStatus::InTransaction);
    s.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    assert_eq!(rows(&mut s, "SELECT id FROM t").await.len(), 1);
    s.simple_query("ROLLBACK").await.expect("rollback");
    assert_eq!(s.tx_status(), TxStatus::Idle);
    assert_eq!(
        rows(&mut s, "SELECT id FROM t").await.len(),
        0,
        "rollback discarded the insert"
    );
}

#[tokio::test]
async fn commit_persists_writes() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    s.simple_query("BEGIN").await.expect("begin");
    s.simple_query("INSERT INTO t VALUES (1),(2)")
        .await
        .expect("insert");
    s.simple_query("COMMIT").await.expect("commit");
    assert_eq!(s.tx_status(), TxStatus::Idle);
    assert_eq!(rows(&mut s, "SELECT id FROM t").await.len(), 2);
}

#[tokio::test]
async fn repeatable_read_does_not_see_concurrent_commit() {
    let engine = SqlEngine::new();
    let mut setup = engine.connect();
    setup
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    setup
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("seed");
    let mut reader = engine.connect();
    reader
        .simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin rr");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 1);
    let mut writer = engine.connect();
    writer
        .simple_query("INSERT INTO t VALUES (2)")
        .await
        .expect("concurrent insert");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 1);
    reader.simple_query("COMMIT").await.expect("commit");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 2);
}

#[tokio::test]
async fn read_committed_sees_concurrent_commit_next_statement() {
    let engine = SqlEngine::new();
    let mut setup = engine.connect();
    setup
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    setup
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("seed");
    let mut reader = engine.connect();
    reader.simple_query("BEGIN").await.expect("begin rc");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 1);
    let mut writer = engine.connect();
    writer
        .simple_query("INSERT INTO t VALUES (2)")
        .await
        .expect("concurrent insert");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 2);
}

#[tokio::test]
async fn error_in_block_fails_transaction_until_rollback() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    s.simple_query("BEGIN").await.expect("begin");
    let err = s
        .simple_query("SELECT * FROM nope")
        .await
        .expect_err("undefined table");
    assert_eq!(err.code, "42P01");
    assert_eq!(s.tx_status(), TxStatus::Failed);
    let err = s.simple_query("SELECT 1").await.expect_err("aborted block");
    assert_eq!(err.code, "25P02");
    s.simple_query("ROLLBACK").await.expect("rollback");
    assert_eq!(s.tx_status(), TxStatus::Idle);
    s.simple_query("SELECT 1").await.expect("works again");
}
