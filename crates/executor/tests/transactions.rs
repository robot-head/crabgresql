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

#[tokio::test]
async fn commit_of_failed_block_reports_rollback() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    s.simple_query("BEGIN").await.expect("begin");
    s.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    // Error inside the block → Failed state.
    let err = s
        .simple_query("SELECT * FROM nope")
        .await
        .expect_err("undefined table");
    assert_eq!(err.code, "42P01");
    assert_eq!(s.tx_status(), TxStatus::Failed);
    // COMMIT of a failed block must report the ROLLBACK tag and discard the write-set.
    let res = s.simple_query("COMMIT").await.expect("commit-of-failed");
    match &res[0] {
        QueryResult::Command { tag } => assert_eq!(tag, "ROLLBACK"),
        other => panic!("expected Command(ROLLBACK), got {other:?}"),
    }
    assert_eq!(s.tx_status(), TxStatus::Idle);
    // The INSERT was discarded.
    assert_eq!(rows(&mut s, "SELECT id FROM t").await.len(), 0);
}

#[tokio::test]
async fn repeatable_read_sees_old_value_after_concurrent_update() {
    let engine = SqlEngine::new();
    let mut setup = engine.connect();
    setup
        .simple_query("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create");
    setup
        .simple_query("INSERT INTO t VALUES (1, 'old')")
        .await
        .expect("seed");

    let mut reader = engine.connect();
    reader
        .simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin rr");
    // snapshot taken; reader sees 'old'
    let r = rows(&mut reader, "SELECT name FROM t").await;
    assert_eq!(text(&r[0][0]), Some("old".into()));

    // Another session updates the row and commits (autocommit).
    let mut writer = engine.connect();
    writer
        .simple_query("UPDATE t SET name = 'new' WHERE id = 1")
        .await
        .expect("concurrent update");

    // RR reader still sees 'old' (its snapshot predates the update's commit).
    let r = rows(&mut reader, "SELECT name FROM t").await;
    assert_eq!(
        text(&r[0][0]),
        Some("old".into()),
        "RR must not see the concurrent UPDATE"
    );
    reader.simple_query("COMMIT").await.expect("commit");
    // After commit, a fresh read sees 'new'.
    let r = rows(&mut reader, "SELECT name FROM t").await;
    assert_eq!(text(&r[0][0]), Some("new".into()));
}
