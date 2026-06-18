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
async fn top_level_queries_keep_existing_behavior() {
    let c = connect_new().await;
    assert_eq!(rows(&c, "SELECT 1").await, vec![vec![Some("1".into())]]);
    assert_eq!(
        rows(&c, "VALUES (2), (1) ORDER BY 1").await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
    assert_eq!(
        rows(&c, "SELECT 1 UNION SELECT 2 ORDER BY 1").await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
}

#[tokio::test]
async fn describe_top_level_query_exprs() {
    let c = connect_new().await;

    let stmt = c.prepare("SELECT 1 AS one").await.expect("describe select");
    assert_eq!(stmt.columns()[0].name(), "one");

    let stmt = c.prepare("VALUES (1, 'a')").await.expect("describe values");
    assert_eq!(stmt.columns()[0].name(), "column1");
    assert_eq!(stmt.columns()[1].name(), "column2");

    let stmt = c
        .prepare("SELECT 1 AS x UNION SELECT 2")
        .await
        .expect("describe set op");
    assert_eq!(stmt.columns()[0].name(), "x");
}

#[tokio::test]
async fn locking_select_still_uses_locking_path() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    c.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    assert_eq!(
        rows(&c, "SELECT id FROM t FOR UPDATE").await,
        vec![vec![Some("1".into())]]
    );
    assert_eq!(err_code(&c, "VALUES (1) FOR UPDATE").await, "42601");
}
