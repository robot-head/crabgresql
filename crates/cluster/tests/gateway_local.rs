//! D3a-net T3: a single multi-range `ServerNode` is a per-statement SQL gateway.
//! CREATE lands on range 0, INSERT routes to the data range's LOCAL leader engine,
//! SELECT reads it back — all over loopback pgwire. A transaction that spans ranges
//! is rejected with SQLSTATE 0A000. The node leads every range itself, so no remote
//! forward fires (that path is Task 4).
use std::time::Duration;

use cluster::range::map::RangeMap;
use cluster::server_node::{NodeConfig, RangeLayout, ServerNode};
use openraft::ServerState;
use tokio::net::TcpListener;

/// Bind an ephemeral loopback port, read its address, and free it for rebind.
async fn free_port() -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let a = l.local_addr().expect("local_addr").to_string();
    drop(l);
    a
}

/// Connect over pgwire with a short bounded retry (the listener is bound before
/// `start` returns, but the OS may briefly not yet route). No fixed settle sleep:
/// the loop is a condition (a successful connect) with a deadline.
async fn connect_with_retry(
    conn_str: &str,
) -> (
    tokio_postgres::Client,
    tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match tokio_postgres::connect(conn_str, tokio_postgres::NoTls).await {
            Ok(pair) => return pair,
            // Retry immediately: the connect attempt is itself a real TCP
            // round-trip that paces the loop (no fixed settle sleep), bounded
            // by the deadline. Yield so a busy runtime makes progress.
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::task::yield_now().await;
            }
            Err(e) => panic!("pg connect: {e}"),
        }
    }
}

/// Start a single self-bootstrapping node hosting a 2-range map (boundary at
/// table 2: table id 1 → range 0, id ≥ 2 → range 1) and wait until it
/// self-confirms leadership of **every** range (event-based, no sleep).
async fn start_two_range_node() -> (ServerNode, String) {
    // Retry on a port-bind race: free_port binds-then-drops to find a free port, so
    // under heavy test contention another binder can steal it before ServerNode
    // rebinds. Bounded so a genuinely stuck bind still fails the test.
    let mut attempts = 0;
    let (node, sql_addr) = loop {
        let node_addr = free_port().await;
        let sql_addr = free_port().await;
        match ServerNode::start(NodeConfig {
            id: 0,
            node_addr: node_addr.clone(),
            sql_addr: sql_addr.clone(),
            data_dir: tempfile::tempdir().expect("tempdir").keep(),
            peers: vec![(0, node_addr.clone())],
            bootstrap: true,
            layout: RangeLayout::Static(RangeMap::with_boundaries(vec![2])),
        })
        .await
        {
            Ok(node) => break (node, sql_addr),
            Err(e) => {
                attempts += 1;
                assert!(
                    attempts < 16,
                    "start_two_range_node: bind race did not clear: {e:?}"
                );
            }
        }
    };

    // A one-node group elects immediately after `initialize`. Wait per range via
    // openraft's event API — the instant each range self-confirms as leader.
    for raft in node.rafts.values() {
        raft.wait(Some(Duration::from_secs(10)))
            .metrics(
                |m| m.state == ServerState::Leader && m.current_leader == Some(0),
                "range self-confirmed leader",
            )
            .await
            .expect("range elects within the bound");
    }
    (node, sql_addr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_routes_create_insert_select_across_local_ranges() {
    let (_node, sql_addr) = start_two_range_node().await;
    let port = sql_addr.rsplit(':').next().expect("port");
    let conn_str = format!("host=127.0.0.1 port={port} user=postgres");
    let (client, connection) = connect_with_retry(&conn_str).await;
    tokio::spawn(connection);

    // CREATE TABLE allocates table id 1 → range 0 (DDL always runs on range 0).
    client
        .simple_query("CREATE TABLE a (id int4)")
        .await
        .expect("create a (range 0)");
    // CREATE TABLE b allocates table id 2 → range 1.
    client
        .simple_query("CREATE TABLE b (id int4)")
        .await
        .expect("create b (range 1)");
    // INSERT INTO b routes to range 1's LOCAL leader engine on this node.
    client
        .simple_query("INSERT INTO b VALUES (20)")
        .await
        .expect("insert b (routes to range 1)");
    // SELECT FROM b reads it back through the same range-1 session.
    let rows = client
        .simple_query("SELECT id FROM b")
        .await
        .expect("select b");
    let row_count = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(row_count, 1, "the row inserted into range 1 must read back");
    if let Some(tokio_postgres::SimpleQueryMessage::Row(r)) = rows
        .iter()
        .find(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
    {
        assert_eq!(r.get("id"), Some("20"), "value routed and read back");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_rejects_a_cross_range_transaction_with_0a000() {
    let (_node, sql_addr) = start_two_range_node().await;
    let port = sql_addr.rsplit(':').next().expect("port");
    let conn_str = format!("host=127.0.0.1 port={port} user=postgres");
    let (client, connection) = connect_with_retry(&conn_str).await;
    tokio::spawn(connection);

    client
        .simple_query("CREATE TABLE a (id int4)")
        .await
        .expect("create a (range 0)");
    client
        .simple_query("CREATE TABLE b (id int4)")
        .await
        .expect("create b (range 1)");
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO a VALUES (1)")
        .await
        .expect("first DML pins range 0");
    // A second statement on a DIFFERENT range inside the same txn is rejected.
    let err = client
        .simple_query("INSERT INTO b VALUES (2)")
        .await
        .expect_err("a transaction may not span ranges (D3b)");
    let db_err = err.as_db_error().expect("a server SQLSTATE error");
    assert_eq!(
        db_err.code().code(),
        "0A000",
        "cross-range txn → feature_not_supported"
    );
    let _ = client.simple_query("ROLLBACK").await;
}
