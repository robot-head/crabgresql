use std::sync::Arc;

use executor::SqlEngine;
use pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

async fn spawn() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(pgwire::server::serve(
        listener,
        Arc::new(SqlEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));
    port
}

async fn connect_new() -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(spawn().await)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

async fn rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    use tokio_postgres::SimpleQueryMessage;
    let mut out = Vec::new();
    for m in client.simple_query(sql).await.expect("query") {
        if let SimpleQueryMessage::Row(row) = m {
            out.push(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_string))
                    .collect(),
            );
        }
    }
    out
}

async fn err_code(client: &tokio_postgres::Client, sql: &str) -> String {
    client
        .simple_query(sql)
        .await
        .expect_err("expected error")
        .as_db_error()
        .expect("db error")
        .code()
        .code()
        .to_string()
}

#[tokio::test]
async fn simple_cte_later_cte_and_forward_reference() {
    let c = connect_new().await;
    assert_eq!(
        rows(
            &c,
            "WITH a AS (SELECT 1 AS x), b AS (SELECT x + 1 AS y FROM a) SELECT y FROM b"
        )
        .await,
        vec![vec![Some("2".into())]]
    );
    assert_eq!(
        err_code(
            &c,
            "WITH b AS (SELECT * FROM a), a AS (SELECT 1 AS x) SELECT * FROM b"
        )
        .await,
        "42P01"
    );
}

#[tokio::test]
async fn cte_shadows_base_table_and_can_be_reused() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE src (x int4)")
        .await
        .expect("create src");
    c.simple_query("INSERT INTO src VALUES (9)")
        .await
        .expect("insert src");
    assert_eq!(
        rows(
            &c,
            "WITH src AS (SELECT 1 AS x) SELECT a.x, b.x FROM src a, src b"
        )
        .await,
        vec![vec![Some("1".into()), Some("1".into())]]
    );
}

#[tokio::test]
async fn cte_column_aliases_and_recursive_error() {
    let c = connect_new().await;
    assert_eq!(
        rows(&c, "WITH c(y) AS (SELECT 7 AS x) SELECT y FROM c").await,
        vec![vec![Some("7".into())]]
    );
    assert_eq!(
        err_code(&c, "WITH c(y, z) AS (SELECT 7 AS x) SELECT * FROM c").await,
        "42601"
    );
    assert_eq!(
        err_code(&c, "WITH RECURSIVE r AS (SELECT 1 AS x) SELECT * FROM r").await,
        "0A000"
    );
}

#[tokio::test]
async fn values_and_set_operation_ctes_work() {
    let c = connect_new().await;
    assert_eq!(
        rows(
            &c,
            "WITH v(x) AS (VALUES (2), (1)) SELECT x FROM v ORDER BY x"
        )
        .await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
    assert_eq!(
        rows(
            &c,
            "WITH u(x) AS (SELECT 1 UNION SELECT 2) SELECT x FROM u ORDER BY x DESC"
        )
        .await,
        vec![vec![Some("2".into())], vec![Some("1".into())]]
    );
    assert_eq!(
        rows(
            &c,
            "WITH a AS (SELECT 1 AS x), \
             u AS (SELECT x FROM a UNION SELECT 2) \
             SELECT x FROM u ORDER BY x"
        )
        .await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
    assert_eq!(
        rows(
            &c,
            "WITH a AS (SELECT 1 AS x), \
             u AS (SELECT (SELECT x FROM a) AS x UNION SELECT 2) \
             SELECT x FROM u ORDER BY x"
        )
        .await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
}

#[tokio::test]
async fn nested_with_scopes_through_derived_tables_subqueries_and_describe() {
    let c = connect_new().await;
    assert_eq!(
        rows(
            &c,
            "WITH c AS (VALUES (1)) SELECT * FROM (WITH c AS (VALUES (2)) SELECT * FROM c) AS d(x)"
        )
        .await,
        vec![vec![Some("2".into())]]
    );
    assert_eq!(
        rows(
            &c,
            "WITH c AS (VALUES (1)) SELECT EXISTS (WITH d AS (SELECT * FROM c) SELECT 1 FROM d)"
        )
        .await,
        vec![vec![Some("t".into())]]
    );

    let stmt = c
        .prepare("WITH c(x) AS (VALUES (1)) SELECT x FROM c")
        .await
        .expect("describe CTE select");
    let names: Vec<_> = stmt.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, vec!["x"]);

    let stmt = c
        .prepare("WITH u(x) AS (SELECT 1 UNION SELECT 2) SELECT x FROM u")
        .await
        .expect("describe set-op CTE select");
    let names: Vec<_> = stmt.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, vec!["x"]);

    let stmt = c
        .prepare(
            "WITH c(x) AS (VALUES (1)) SELECT * FROM (WITH c(y) AS (VALUES (2)) SELECT y FROM c) AS d",
        )
        .await
        .expect("describe nested CTE shadowing");
    let names: Vec<_> = stmt.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, vec!["y"]);

    let stmt = c
        .prepare("SELECT (WITH c AS (SELECT 1 AS x) SELECT x FROM c)")
        .await
        .expect("describe scalar subquery CTE");
    assert_eq!(stmt.columns().len(), 1);
    assert_eq!(
        stmt.columns()[0].type_(),
        &tokio_postgres::types::Type::INT4
    );

    let stmt = c
        .prepare("WITH c(x) AS (VALUES (1)) SELECT (WITH c(y) AS (VALUES (2)) SELECT y FROM c)")
        .await
        .expect("describe scalar subquery CTE shadowing");
    assert_eq!(stmt.columns().len(), 1);
    assert_eq!(
        stmt.columns()[0].type_(),
        &tokio_postgres::types::Type::INT4
    );

    let stmt = c
        .prepare("WITH c(x) AS (VALUES (1)) VALUES ((SELECT x FROM c))")
        .await
        .expect("describe VALUES scalar subquery CTE");
    assert_eq!(stmt.columns().len(), 1);
    assert_eq!(
        stmt.columns()[0].type_(),
        &tokio_postgres::types::Type::INT4
    );

    assert_eq!(
        rows(&c, "WITH c(x) AS (VALUES (1)) VALUES ((SELECT x FROM c))").await,
        vec![vec![Some("1".into())]]
    );

    assert_eq!(
        rows(
            &c,
            "WITH c(x) AS (VALUES (1)) VALUES ((SELECT x FROM c)) UNION SELECT 2 ORDER BY 1"
        )
        .await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
}

#[tokio::test]
async fn locking_select_rejects_ctes() {
    let c = connect_new().await;
    assert_eq!(
        err_code(&c, "WITH c AS (SELECT 1 AS x) SELECT * FROM c FOR UPDATE").await,
        "0A000"
    );
    assert_eq!(
        err_code(
            &c,
            "WITH RECURSIVE c AS (SELECT 1 AS x) SELECT * FROM c FOR UPDATE"
        )
        .await,
        "0A000"
    );
}

#[tokio::test]
async fn nested_locking_inside_cte_body_is_rejected() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE t (x int4)")
        .await
        .expect("create t");
    c.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert t");
    assert_eq!(
        err_code(
            &c,
            "WITH c AS (SELECT * FROM (SELECT x FROM t FOR UPDATE) d) SELECT * FROM c"
        )
        .await,
        "0A000"
    );
}

#[tokio::test]
async fn locking_inside_cte_body_is_rejected() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE t (x int4)")
        .await
        .expect("create t");
    c.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert t");
    assert_eq!(
        err_code(&c, "WITH c AS (SELECT x FROM t FOR UPDATE) SELECT * FROM c").await,
        "0A000"
    );
}
