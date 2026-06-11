use std::sync::Arc;
use std::time::Duration;

use pgwire::session::SessionConfig;
use pgwire::stub::StubEngine;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn cancel_request_interrupts_running_query() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(pgwire::server::serve(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));

    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);

    // tokio-postgres implements CancelRequest from the BackendKeyData we sent.
    let cancel_token = client.cancel_token();
    let query = tokio::spawn(async move { client.simple_query("SELECT pg_sleep(30)").await });

    tokio::time::sleep(Duration::from_millis(200)).await;
    cancel_token.cancel_query(NoTls).await.expect("cancel sent");

    let result = tokio::time::timeout(Duration::from_secs(5), query)
        .await
        .expect("query must end promptly after cancel")
        .expect("join");
    let err = result.expect_err("query must be cancelled");
    assert_eq!(err.as_db_error().expect("db error").code().code(), "57014");
}
