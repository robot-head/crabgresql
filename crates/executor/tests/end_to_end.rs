use std::sync::Arc;
use std::time::Duration;

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
async fn wire_transaction_commit_and_rollback() {
    let mut client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create");

    // Rollback path: tokio-postgres transaction dropped without commit.
    {
        let tx = client.transaction().await.expect("begin");
        tx.batch_execute("INSERT INTO t VALUES (1,'a')")
            .await
            .expect("insert");
        // drop without commit → ROLLBACK sent over the wire
    }
    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after rollback");
    assert_eq!(rows.len(), 0, "rolled-back insert must be gone");

    // Commit path.
    {
        let tx = client.transaction().await.expect("begin");
        tx.batch_execute("INSERT INTO t VALUES (2,'b')")
            .await
            .expect("insert");
        tx.commit().await.expect("commit");
    }
    let rows = client
        .query("SELECT id FROM t", &[])
        .await
        .expect("select after commit");
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    assert_eq!(id, 2);
}

#[tokio::test]
async fn wire_update_delete_roundtrip() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
        .await
        .expect("insert");

    let updated = client
        .execute("UPDATE t SET name = 'z' WHERE id > 1", &[])
        .await
        .expect("update");
    assert_eq!(updated, 2, "UPDATE must report 2 affected rows");

    let deleted = client
        .execute("DELETE FROM t WHERE id = 1", &[])
        .await
        .expect("delete");
    assert_eq!(deleted, 1, "DELETE must report 1 affected row");

    let rows = client
        .query("SELECT id, name FROM t ORDER BY id", &[])
        .await
        .expect("select");
    assert_eq!(rows.len(), 2);
    let names: Vec<&str> = rows.iter().map(|r| r.get::<_, &str>(1)).collect();
    assert_eq!(names, vec!["z", "z"]);
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

/// Wire-protocol version of the blocking UPDATE test.
///
/// conn1 opens a transaction and locks a row via UPDATE; conn2's UPDATE on
/// the same row blocks over the wire. After conn1 commits, conn2 completes
/// and reports exactly 1 affected row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_concurrent_update_blocks_then_succeeds() {
    // Each connection needs its own port/engine so they share the same engine.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let engine = Arc::new(SqlEngine::new());
    tokio::spawn(pgwire::server::serve(
        listener,
        Arc::clone(&engine),
        Arc::new(pgwire::session::SessionConfig::trust()),
    ));

    let conn1 = connect(port).await;
    let conn2 = connect(port).await;

    // Set up the table.
    conn1
        .batch_execute("CREATE TABLE t (id int4, v text)")
        .await
        .expect("create");
    conn1
        .batch_execute("INSERT INTO t VALUES (1,'orig')")
        .await
        .expect("insert");

    // T1: open a transaction and lock row 1.
    conn1
        .batch_execute("BEGIN; UPDATE t SET v='a' WHERE id=1")
        .await
        .expect("t1 begin+update");

    // T2: issue an UPDATE that will block.
    let t2 = tokio::spawn(async move {
        conn2
            .execute("UPDATE t SET v='b' WHERE id=1", &[])
            .await
            .expect("t2 update")
    });

    // let T2 reach the blocking acquire
    tokio::time::sleep(Duration::from_millis(100)).await;
    conn1.batch_execute("COMMIT").await.expect("t1 commit");

    let affected = tokio::time::timeout(Duration::from_secs(10), t2)
        .await
        .expect("t2 did not hang")
        .expect("t2 join");
    assert_eq!(affected, 1, "t2 must have updated exactly 1 row");
}
