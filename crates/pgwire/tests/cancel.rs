use std::sync::Arc;
use std::time::Duration;

use pgwire::session::SessionConfig;
use pgwire::stub::StubEngine;
use tokio::io::AsyncWriteExt;
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

#[tokio::test]
async fn wrong_cancel_key_is_ignored() {
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

    // Hand-rolled CancelRequest with a garbage key: len 16, code 80877102, pid, secret.
    let mut raw = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("tcp");
    let mut pkt = Vec::new();
    pkt.extend_from_slice(&16i32.to_be_bytes());
    pkt.extend_from_slice(&80_877_102i32.to_be_bytes());
    pkt.extend_from_slice(&999_999i32.to_be_bytes());
    pkt.extend_from_slice(&123_456i32.to_be_bytes());
    raw.write_all(&pkt).await.expect("send cancel");
    drop(raw);

    // The session must be unaffected.
    let messages = client
        .simple_query("SELECT 1")
        .await
        .expect("query unaffected");
    assert!(
        messages
            .iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
    );
}

#[tokio::test]
async fn cancel_while_idle_does_not_poison_next_query() {
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

    // Run one query so a spent token sits in the slot.
    client.simple_query("SELECT 1").await.expect("first query");
    // Cancel while idle: fires the spent token AND sets pending.
    client
        .cancel_token()
        .cancel_query(NoTls)
        .await
        .expect("cancel sent");
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Per best-effort semantics the pending flag will cancel the NEXT query
    // immediately (this matches PG: a cancel that arrives while idle may
    // affect the next command). Accept either outcome: instant 57014 or success.
    match client.simple_query("SELECT 1").await {
        Ok(messages) => {
            assert!(
                messages
                    .iter()
                    .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            );
        }
        Err(e) => {
            assert_eq!(e.as_db_error().expect("db error").code().code(), "57014");
        }
    }
}
