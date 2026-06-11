use std::sync::Arc;

use pgwire::session::SessionConfig;
use pgwire::stub::StubEngine;
use sqlx::Connection;
use tokio::net::TcpListener;

#[tokio::test]
async fn sqlx_connects_and_queries() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(pgwire::server::serve(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));

    let url = format!("postgres://crab@127.0.0.1:{port}/crab");
    let mut conn = sqlx::postgres::PgConnection::connect(&url)
        .await
        .expect("sqlx connect");
    let row: (i32,) = sqlx::query_as("SELECT 1")
        .fetch_one(&mut conn)
        .await
        .expect("query");
    assert_eq!(row.0, 1);
}
