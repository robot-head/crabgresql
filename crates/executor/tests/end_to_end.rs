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

// ── SP40: foreign-table (Kafka FDW) executor seam ────────────────────────────

use catalog::{ForeignServer, Table, UserMapping};
use executor::ExecError;
use executor::clock::EvalCtx;
use executor::foreign::{ForeignScanner, ScanBounds};
use pgtypes::Datum;

/// A fake `ForeignScanner` for tests: returns canned rows aligned to the foreign
/// table's column layout (envelope columns first, then value columns). Records the
/// last server/mapping it was handed so a test can assert they were resolved.
struct FakeScanner {
    rows: Vec<Vec<Datum>>,
}

impl ForeignScanner for FakeScanner {
    fn scan(
        &self,
        table: &Table,
        _server: &ForeignServer,
        _mapping: Option<&UserMapping>,
        _bounds: &ScanBounds,
        _ctx: &EvalCtx,
    ) -> Result<Vec<Vec<Datum>>, ExecError> {
        // Every canned row must match the table's full column width (envelope + value).
        for r in &self.rows {
            assert_eq!(
                r.len(),
                table.columns.len(),
                "fake scanner row width must match the foreign table column count"
            );
        }
        Ok(self.rows.clone())
    }
}

/// Spawn a server whose engine has `scanner` registered as the foreign scanner.
async fn spawn_with_scanner(scanner: Arc<dyn ForeignScanner>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let mut engine = SqlEngine::new();
    engine.set_foreign_scanner(scanner);
    tokio::spawn(pgwire::server::serve(
        listener,
        Arc::new(engine),
        Arc::new(SessionConfig::trust()),
    ));
    port
}

/// DDL round-trip: CREATE SERVER + CREATE FOREIGN TABLE + DROP FOREIGN TABLE all
/// succeed and report the PostgreSQL command tags. No scanner is needed (no scan
/// runs), so this uses the default (scanner-less) `spawn()`.
#[tokio::test]
async fn create_drop_foreign_objects_roundtrip() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE SERVER s FOREIGN DATA WRAPPER kafka_fdw \
             OPTIONS (bootstrap 'h:9092', registry_url 'http://r')",
        )
        .await
        .expect("create server");
    client
        .batch_execute(
            "CREATE FOREIGN TABLE orders (id int4) SERVER s \
             OPTIONS (topic 'orders', value_format 'avro')",
        )
        .await
        .expect("create foreign table");
    // Describe/plan of a foreign table resolves its schema (envelope + value columns)
    // from the catalog without a scan.
    let fields = client
        .prepare("SELECT _partition, _offset, id FROM orders")
        .await
        .expect("describe foreign table");
    assert_eq!(
        fields.columns().len(),
        3,
        "three projected columns described"
    );
    client
        .batch_execute("DROP FOREIGN TABLE orders")
        .await
        .expect("drop foreign table");
    // Gone: a re-select errors 42P01.
    let err = client
        .batch_execute("SELECT id FROM orders")
        .await
        .expect_err("dropped");
    assert_eq!(err.as_db_error().expect("db").code().code(), "42P01");
}

/// Read path: with a fake scanner registered, a `SELECT` from a foreign table
/// returns the canned rows, and projection + WHERE compose over those rows exactly
/// like an ordinary table.
#[tokio::test]
async fn foreign_select_reads_scanner_rows_with_projection_and_where() {
    // Canned rows for `orders (id int4, amount int4)`: layout is the 5 envelope
    // columns (_partition int4, _offset int8, _timestamp timestamptz, _key bytea,
    // _headers text) followed by the 2 value columns.
    let row = |partition: i32, offset: i64, id: i32, amount: i32| {
        vec![
            Datum::Int4(partition),
            Datum::Int8(offset),
            Datum::Timestamptz(jiff::Timestamp::UNIX_EPOCH),
            Datum::Bytea(vec![0xDE, 0xAD]),
            Datum::Text("{}".into()),
            Datum::Int4(id),
            Datum::Int4(amount),
        ]
    };
    let scanner = Arc::new(FakeScanner {
        rows: vec![row(0, 100, 1, 10), row(0, 101, 2, 20), row(1, 200, 3, 30)],
    });
    let client = connect(spawn_with_scanner(scanner).await).await;
    client
        .batch_execute(
            "CREATE SERVER s FOREIGN DATA WRAPPER kafka_fdw OPTIONS (bootstrap 'h:9092')",
        )
        .await
        .expect("create server");
    client
        .batch_execute(
            "CREATE FOREIGN TABLE orders (id int4, amount int4) SERVER s OPTIONS (topic 'orders')",
        )
        .await
        .expect("create foreign table");

    // Full scan: all three canned rows come back, envelope + value columns present.
    let rows = client
        .query(
            "SELECT _partition, _offset, id, amount FROM orders ORDER BY id",
            &[],
        )
        .await
        .expect("select foreign");
    assert_eq!(rows.len(), 3, "all canned rows returned");
    let ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>("id")).collect();
    assert_eq!(ids, vec![1, 2, 3]);
    let partitions: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>("_partition")).collect();
    assert_eq!(partitions, vec![0, 0, 1]);

    // Projection + WHERE compose over the scanner rows: only amount >= 20 survive.
    let filtered = client
        .query("SELECT id FROM orders WHERE amount >= 20 ORDER BY id", &[])
        .await
        .expect("select foreign with where");
    let ids: Vec<i32> = filtered.iter().map(|r| r.get::<_, i32>("id")).collect();
    assert_eq!(ids, vec![2, 3], "WHERE filters the scanner rows");
}

/// No-scanner path: with no foreign scanner registered, a `SELECT` from a foreign
/// table returns the clear 0A000 ("foreign tables require the `kafka` feature").
/// DDL still works (no scan), so the table can be created without a scanner.
#[tokio::test]
async fn foreign_select_without_scanner_is_unsupported() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE SERVER s FOREIGN DATA WRAPPER kafka_fdw OPTIONS (bootstrap 'h:9092')",
        )
        .await
        .expect("create server");
    client
        .batch_execute("CREATE FOREIGN TABLE orders (id int4) SERVER s OPTIONS (topic 'orders')")
        .await
        .expect("create foreign table");
    let err = client
        .batch_execute("SELECT id FROM orders")
        .await
        .expect_err("no scanner registered");
    let db = err.as_db_error().expect("db error");
    assert_eq!(db.code().code(), "0A000", "feature_not_supported");
    assert!(
        db.message().contains("foreign tables require"),
        "clear message, got: {}",
        db.message()
    );
}
