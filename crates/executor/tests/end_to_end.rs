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
async fn create_insert_select_roundtrip() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
        .await
        .expect("insert");
    // Extended protocol with binary results (exercises describe + binary cells).
    let rows = client
        .query(
            "SELECT name FROM t WHERE id > 1 ORDER BY id DESC LIMIT 5",
            &[],
        )
        .await
        .expect("select");
    assert_eq!(rows.len(), 2);
    let first: &str = rows[0].get(0);
    let second: &str = rows[1].get(0);
    assert_eq!((first, second), ("c", "b"));
}

#[tokio::test]
async fn select_expression_typed_int4() {
    let client = connect(spawn().await).await;
    let rows = client
        .query("SELECT 2 + 3 AS five", &[])
        .await
        .expect("select");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 5);
}

#[tokio::test]
async fn undefined_table_errors_but_session_survives() {
    let client = connect(spawn().await).await;
    let err = client
        .batch_execute("SELECT * FROM nope")
        .await
        .expect_err("no table");
    assert_eq!(err.as_db_error().expect("db").code().code(), "42P01");
    // Session still usable.
    let rows = client.query("SELECT 1", &[]).await.expect("recovered");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
}

#[tokio::test]
async fn parameterized_query_is_unsupported_0a000() {
    let client = connect(spawn().await).await;
    // The SP2 slice is literals-only; $1 parameters must reach SQLSTATE 0A000
    // (feature_not_supported), not a panic or a wrong code, through the real
    // engine.  tokio-postgres sends this via the extended protocol (Parse →
    // Describe); the server rejects it at Parse time when infer_type hits
    // Expr::Param and returns ExecError::Unsupported → 0A000.
    let err = client
        .query("SELECT $1", &[&5_i32])
        .await
        .expect_err("parameters are not supported in the SP2 slice");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "0A000");
    // Session survives — a normal query still works after the failed exchange.
    let rows = client
        .query("SELECT 1", &[])
        .await
        .expect("session survives");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 1);
}
