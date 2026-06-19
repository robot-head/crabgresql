//! End-to-end round-trip differential test for the Kafka FDW.
//!
//! Everything runs in one test process — no spawned binaries:
//!
//! 1. an in-process crabka broker + schema registry ([`harness::KafkaStack`]),
//! 2. an Avro schema registered + 3 known records produced to `orders`,
//! 3. an `executor::SqlEngine` with [`kafka_fdw::KafkaFdw`] registered, served
//!    over pgwire on an ephemeral port,
//! 4. a `tokio-postgres` client that runs `CREATE SERVER` + `CREATE USER
//!    MAPPING` + `IMPORT FOREIGN SCHEMA`, then `SELECT`s the rows back.
//!
//! Assertions: the projected values + envelope offsets match what was produced;
//! offset pushdown (`WHERE _partition = 0 AND _offset >= 1`) returns the
//! expected subset; and a topic produced as raw bytes (no registry subject)
//! comes back as `bytea` via the raw-fallback path.

#![cfg(feature = "kafka")]

mod harness;

use std::sync::Arc;

use bytes::Bytes;
use executor::SqlEngine;
use harness::KafkaStack;
use pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

/// Avro schema for `orders`: `id` (int → int4) + `total` (double → float8).
const ORDERS_SCHEMA: &str = r#"{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "total", "type": "double"}
  ]
}"#;

/// The three known records produced to `orders`, in produce (== offset) order.
const ORDERS: [(i32, f64); 3] = [(1, 10.5), (2, 20.0), (3, 30.25)];

/// Serve a pgwire engine (with the real `KafkaFdw` registered) on an ephemeral
/// port and return that port.
async fn serve_engine() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let mut engine = SqlEngine::new();
    engine.set_foreign_scanner(Arc::new(kafka_fdw::KafkaFdw));
    tokio::spawn(pgwire::server::serve(
        listener,
        Arc::new(engine),
        Arc::new(SessionConfig::trust()),
    ));
    port
}

/// Connect a `tokio-postgres` client to the in-process pgwire server.
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

/// Confluent-frame an Avro record body under `schema_id`.
fn avro_frame(schema: &apache_avro::Schema, schema_id: u32, id: i32, total: f64) -> Bytes {
    let mut rec = apache_avro::types::Record::new(schema).expect("avro record");
    rec.put("id", id);
    rec.put("total", total);
    let body = apache_avro::to_avro_datum(schema, rec).expect("encode avro datum");
    crabka_schema_serde::wire::encode(schema_id, &body)
}

/// The whole round trip. `multi_thread` is required: the FDW scan drives async
/// fetch via `block_in_place`, and the broker/registry tasks must run
/// concurrently with the test body.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_fdw_roundtrip_avro_and_raw_fallback() {
    let stack = KafkaStack::start().await;

    // ── produce: avro "orders" + raw-bytes "events" ────────────────────────
    stack.create_topic("orders", 1).await;
    stack.create_topic("events", 1).await;

    let avro_schema = apache_avro::Schema::parse_str(ORDERS_SCHEMA).expect("parse orders schema");
    let schema_id = stack.register_avro("orders-value", ORDERS_SCHEMA).await;

    let mut produced_offsets = Vec::new();
    for (id, total) in ORDERS {
        let frame = avro_frame(&avro_schema, schema_id, id, total);
        let offset = stack.produce("orders", 0, frame).await;
        produced_offsets.push(offset);
    }
    // Offsets must be the monotonic 0,1,2 the assertions below rely on.
    assert_eq!(
        produced_offsets,
        vec![0, 1, 2],
        "produced offsets must be a dense monotonic 0..3"
    );

    // Raw-fallback topic: no registry subject, verbatim payload.
    let raw_payload = Bytes::from_static(b"raw-event-payload");
    let raw_offset = stack.produce("events", 0, raw_payload.clone()).await;
    assert_eq!(raw_offset, 0, "single raw record lands at offset 0");

    // ── pgwire + FDW DDL ────────────────────────────────────────────────────
    let client = connect(serve_engine().await).await;

    client
        .batch_execute(&format!(
            "CREATE SERVER s FOREIGN DATA WRAPPER kafka_fdw \
             OPTIONS (bootstrap '{}', registry_url '{}')",
            stack.bootstrap(),
            stack.registry_url(),
        ))
        .await
        .expect("create server");
    client
        .batch_execute("CREATE USER MAPPING FOR PUBLIC SERVER s")
        .await
        .expect("create user mapping");

    // IMPORT FOREIGN SCHEMA materializes `orders` (avro → id int4, total float8)
    // and `events` (raw → value bytea). Restrict to those two topics.
    client
        .batch_execute("IMPORT FOREIGN SCHEMA kafka LIMIT TO (orders, events) FROM SERVER s")
        .await
        .expect("import foreign schema");

    // ── assertion 1: full read matches produced values + monotonic offsets ──
    let rows = client
        .query(
            "SELECT id, total, _partition, _offset FROM orders ORDER BY _offset",
            &[],
        )
        .await
        .expect("select orders");
    assert_eq!(rows.len(), 3, "all three avro records returned");
    for (i, (expect_id, expect_total)) in ORDERS.iter().enumerate() {
        let id: i32 = rows[i].get("id");
        let total: f64 = rows[i].get("total");
        let partition: i32 = rows[i].get("_partition");
        let offset: i64 = rows[i].get("_offset");
        assert_eq!(id, *expect_id, "row {i} id");
        assert!(
            (total - *expect_total).abs() < f64::EPSILON,
            "row {i} total: got {total}, want {expect_total}"
        );
        assert_eq!(partition, 0, "row {i} _partition");
        assert_eq!(offset, i as i64, "row {i} _offset monotonic");
    }

    // ── assertion 2: offset pushdown returns exactly the expected subset ─────
    let pushed = client
        .query(
            "SELECT id, _offset FROM orders WHERE _partition = 0 AND _offset >= 1 ORDER BY _offset",
            &[],
        )
        .await
        .expect("select pushdown");
    assert_eq!(pushed.len(), 2, "_offset >= 1 keeps offsets 1 and 2");
    let pushed_ids: Vec<i32> = pushed.iter().map(|r| r.get::<_, i32>("id")).collect();
    assert_eq!(pushed_ids, vec![2, 3], "offsets 1,2 → ids 2,3");

    // ── assertion 3: raw-fallback topic comes back as bytea ──────────────────
    let raw_rows = client
        .query("SELECT value, _offset FROM events ORDER BY _offset", &[])
        .await
        .expect("select events (raw)");
    assert_eq!(raw_rows.len(), 1, "one raw record");
    let value: Vec<u8> = raw_rows[0].get("value");
    assert_eq!(
        value,
        raw_payload.to_vec(),
        "raw value round-trips verbatim as bytea"
    );

    stack.shutdown().await;
}
