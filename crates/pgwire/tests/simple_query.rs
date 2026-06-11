use std::sync::Arc;

use pgwire::session::SessionConfig;
use pgwire::stub::StubEngine;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

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
async fn trust_auth_and_select_1() {
    let client = connect(spawn_server().await).await;
    let messages = client.simple_query("SELECT 1").await.expect("query");
    let row = messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("one row");
    assert_eq!(row.get(0), Some("1"));
}

#[tokio::test]
async fn version_query_works() {
    let client = connect(spawn_server().await).await;
    let messages = client
        .simple_query("SELECT version()")
        .await
        .expect("query");
    let row = messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .expect("one row");
    assert!(row.get(0).expect("value").starts_with("PostgreSQL 18"));
}

#[tokio::test]
async fn unsupported_query_returns_0a000_and_session_survives() {
    let client = connect(spawn_server().await).await;
    let err = client
        .simple_query("SELECT * FROM nope")
        .await
        .expect_err("must fail");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "0A000");
    // The session must still be usable after an ERROR (not FATAL).
    let messages = client
        .simple_query("SELECT 1")
        .await
        .expect("session survives");
    assert!(
        messages
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::Row(_)))
    );
}

#[tokio::test]
async fn empty_query_returns_cleanly() {
    let client = connect(spawn_server().await).await;
    // tokio-postgres surfaces EmptyQueryResponse as zero rows
    let messages = client.simple_query("   ").await.expect("empty ok");
    assert!(
        !messages
            .iter()
            .any(|m| matches!(m, SimpleQueryMessage::Row(_)))
    );
}

#[tokio::test]
async fn three_sequential_queries_on_one_session() {
    let client = connect(spawn_server().await).await;
    for _ in 0..3 {
        let messages = client.simple_query("SELECT 1").await.expect("query");
        let row = messages
            .iter()
            .find_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            })
            .expect("one row");
        assert_eq!(row.get(0), Some("1"));
    }
}
