use std::sync::Arc;

use pgwire::session::SessionConfig;
use pgwire::stub::StubEngine;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

async fn spawn_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(pgwire::server::serve(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));
    port
}

async fn connect(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

#[tokio::test]
async fn prepare_and_query_select_1_binary_format() {
    let client = connect(spawn_server().await).await;
    // tokio-postgres uses Parse/Describe/Bind/Execute and requests BINARY results.
    let stmt = client.prepare("SELECT 1").await.expect("prepare");
    let rows = client.query(&stmt, &[]).await.expect("query");
    assert_eq!(rows.len(), 1);
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
}

#[tokio::test]
async fn version_via_extended_protocol() {
    let client = connect(spawn_server().await).await;
    let rows = client.query("SELECT version()", &[]).await.expect("query");
    let v: &str = rows[0].get(0);
    assert!(v.starts_with("PostgreSQL 18"));
}

#[tokio::test]
async fn error_skips_until_sync_and_session_recovers() {
    let client = connect(spawn_server().await).await;
    let err = client
        .query("SELECT * FROM nope", &[])
        .await
        .expect_err("must fail");
    assert_eq!(err.as_db_error().expect("db error").code().code(), "0A000");
    // tokio-postgres sends Sync after the failed exchange; a healthy
    // implementation recovers and serves the next query.
    let rows = client.query("SELECT 1", &[]).await.expect("recovered");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
}

#[tokio::test]
async fn reusing_a_prepared_statement_works() {
    let client = connect(spawn_server().await).await;
    let stmt = client.prepare("SELECT 1").await.expect("prepare");
    for _ in 0..3 {
        let rows = client.query(&stmt, &[]).await.expect("query");
        let v: i32 = rows[0].get(0);
        assert_eq!(v, 1);
    }
}

#[tokio::test]
async fn execute_returns_affected_count_path() {
    let client = connect(spawn_server().await).await;
    // execute() returns the CommandComplete row count for the Rows path.
    let n = client.execute("SELECT 1", &[]).await.expect("execute");
    assert_eq!(n, 1);
}

#[tokio::test]
async fn empty_query_via_extended_protocol() {
    let client = connect(spawn_server().await).await;
    // Parse("") → describe → NoData; Execute → EmptyQueryResponse.
    // tokio-postgres surfaces EmptyQueryResponse as an Ok result with zero rows.
    let rows = client.query("", &[]).await.expect("empty ok");
    assert!(rows.is_empty());
}
