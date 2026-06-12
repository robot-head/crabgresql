//! UPDATE / DELETE semantics over MVCC: autocommit and in-transaction,
//! read-your-writes, tombstone hiding, command tags.

use executor::SqlEngine;
use pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(s: &mut impl Session, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql).await.expect("ok")
}

fn tag_of(r: &QueryResult) -> &str {
    match r {
        QueryResult::Command { tag } => tag,
        QueryResult::Rows { tag, .. } => tag,
        other => panic!("expected a tagged result, got {other:?}"),
    }
}

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
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[tokio::test]
async fn update_changes_value_and_tags_count() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4, name text)").await;
    run(&mut s, "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").await;
    let r = run(&mut s, "UPDATE t SET name = 'z' WHERE id > 1").await;
    assert_eq!(tag_of(&r[0]), "UPDATE 2");
    let r = run(&mut s, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(
        col0(&r[0]),
        vec![Some("a".into()), Some("z".into()), Some("z".into())]
    );
}

#[tokio::test]
async fn update_expression_references_current_row() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4)").await;
    run(&mut s, "INSERT INTO t VALUES (1),(2),(3)").await;
    let r = run(&mut s, "UPDATE t SET id = id + 10").await;
    assert_eq!(tag_of(&r[0]), "UPDATE 3");
    let r = run(&mut s, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(
        col0(&r[0]),
        vec![Some("11".into()), Some("12".into()), Some("13".into())]
    );
}

#[tokio::test]
async fn delete_hides_rows_and_tags_count() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4)").await;
    run(&mut s, "INSERT INTO t VALUES (1),(2),(3)").await;
    let r = run(&mut s, "DELETE FROM t WHERE id = 2").await;
    assert_eq!(tag_of(&r[0]), "DELETE 1");
    let r = run(&mut s, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(col0(&r[0]), vec![Some("1".into()), Some("3".into())]);
}

#[tokio::test]
async fn delete_all_then_select_is_empty() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4)").await;
    run(&mut s, "INSERT INTO t VALUES (1),(2)").await;
    assert_eq!(tag_of(&run(&mut s, "DELETE FROM t").await[0]), "DELETE 2");
    assert!(col0(&run(&mut s, "SELECT id FROM t").await[0]).is_empty());
}

#[tokio::test]
async fn update_then_delete_read_your_writes_in_txn() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4, name text)").await;
    run(&mut s, "INSERT INTO t VALUES (1,'a'),(2,'b')").await;
    run(&mut s, "BEGIN").await;
    run(&mut s, "UPDATE t SET name = 'x' WHERE id = 1").await;
    run(&mut s, "DELETE FROM t WHERE id = 2").await;
    let r = run(&mut s, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(col0(&r[0]), vec![Some("x".into())]);
    run(&mut s, "ROLLBACK").await;
    let r = run(&mut s, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(col0(&r[0]), vec![Some("a".into()), Some("b".into())]);
}

#[tokio::test]
#[allow(non_snake_case)]
async fn update_missing_table_is_42P01() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    let err = s
        .simple_query("UPDATE nope SET a = 1")
        .await
        .expect_err("no table");
    assert_eq!(err.code, "42P01");
}

#[tokio::test]
async fn update_unknown_column_is_42703() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4)").await;
    let err = s
        .simple_query("UPDATE t SET nope = 1")
        .await
        .expect_err("no column");
    assert_eq!(err.code, "42703");
}
