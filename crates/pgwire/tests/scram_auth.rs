use std::collections::HashMap;
use std::sync::Arc;

use pgwire::session::{AuthMode, SessionConfig};
use pgwire::stub::StubEngine;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

async fn spawn_scram_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let mut users = HashMap::new();
    users.insert("crab".to_string(), "hunter2".to_string());
    let config = SessionConfig {
        auth: AuthMode::ScramSha256 { users },
        ..SessionConfig::trust()
    };
    tokio::spawn(pgwire::server::serve(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(config),
    ));
    port
}

#[tokio::test]
async fn correct_password_authenticates_and_queries() {
    let port = spawn_scram_server().await;
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .password("hunter2")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("scram connect");
    tokio::spawn(conn);
    let rows = client.query("SELECT 1", &[]).await.expect("query");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
}

#[tokio::test]
async fn wrong_password_is_rejected_with_28p01() {
    let port = spawn_scram_server().await;
    let result = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .password("wrong")
        .dbname("crab")
        .connect(NoTls)
        .await;
    let err = result.map(|_| ()).expect_err("must fail");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "28P01");
}

#[tokio::test]
async fn unknown_user_is_rejected() {
    let port = spawn_scram_server().await;
    let result = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("mallory")
        .password("whatever")
        .dbname("crab")
        .connect(NoTls)
        .await;
    assert!(result.is_err());
}
