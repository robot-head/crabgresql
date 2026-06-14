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
async fn begin_global_durable_persists_next_global() {
    use mvcc::xid::GLOBAL_XID_BASE;
    let (node, _sql_addr) = start_two_range_node().await;
    let engine = node.engines.get(&0).expect("range-0 engine");
    assert!(
        engine.has_gtm(),
        "range-0 engine must carry the GTM after wiring"
    );

    let g0 = engine.begin_global_durable().await.expect("alloc g0");
    assert!(g0 >= GLOBAL_XID_BASE, "global xids live above the base");
    let g1 = engine.begin_global_durable().await.expect("alloc g1");
    assert_eq!(g1, g0 + 1, "allocations are monotonic");

    // The advance is durable (no raw byte decode — avoids endianness coupling):
    // a reseed of a fresh in-memory counter never regresses below the persisted
    // value, so a subsequent allocation stays strictly monotone past g1.
    engine.reseed_gtm().expect("reseed");
    let g2 = engine.begin_global_durable().await.expect("alloc g2");
    assert!(
        g2 > g1,
        "post-reseed allocation never regresses below the durable counter"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_escalates_a_cross_range_transaction_without_0a000() {
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
    client
        .simple_query("INSERT INTO b VALUES (2)")
        .await
        .expect("second range escalates, no 0A000");
    client
        .simple_query("COMMIT")
        .await
        .expect("atomic cross-range commit succeeds");

    let a = client
        .simple_query("SELECT id FROM a")
        .await
        .expect("select a");
    assert_eq!(row_count(&a), 1, "range-0 row committed and visible");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_leaders_control_reports_each_range() {
    use cluster::transport::frame::{read_msg, write_msg};
    use cluster::transport::protocol::{
        ControlRequest, ControlResponse, NodeRequest, NodeResponse,
    };
    let (node, _sql) = start_two_range_node().await;
    let mut s = tokio::net::TcpStream::connect(node.node_addr())
        .await
        .expect("dial node port");
    write_msg(&mut s, &NodeRequest::Control(ControlRequest::RangeLeaders))
        .await
        .expect("send");
    let resp: NodeResponse = read_msg(&mut s).await.expect("recv");
    let leaders = match resp {
        NodeResponse::Control(ControlResponse::RangeLeaders(v)) => v,
        other => panic!("expected RangeLeaders, got {other:?}"),
    };
    assert_eq!(leaders.len(), 2, "two ranges reported");
    for (_r, leader) in leaders {
        assert_eq!(
            leader,
            Some(0),
            "single self-bootstrapping node leads every range"
        );
    }
}

// Counts SimpleQueryMessage::Row entries (reused by T5's full-visibility tests).
fn row_count(msgs: &[tokio_postgres::SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_commits_a_cross_range_transaction_atomically() {
    let (_node, sql_addr) = start_two_range_node().await;
    let port = sql_addr.rsplit(':').next().expect("port");
    let conn_str = format!("host=127.0.0.1 port={port} user=postgres");
    let (client, connection) = connect_with_retry(&conn_str).await;
    tokio::spawn(connection);
    client
        .simple_query("CREATE TABLE a (id int4)")
        .await
        .expect("a");
    client
        .simple_query("CREATE TABLE b (id int4)")
        .await
        .expect("b");
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO a VALUES (1)")
        .await
        .expect("pin range 0");
    client
        .simple_query("INSERT INTO b VALUES (2)")
        .await
        .expect("escalate range 1");
    client
        .simple_query("COMMIT")
        .await
        .expect("atomic cross-range commit");
    let a = client
        .simple_query("SELECT id FROM a")
        .await
        .expect("select a");
    let b = client
        .simple_query("SELECT id FROM b")
        .await
        .expect("select b");
    assert_eq!(row_count(&a), 1, "range-0 row committed");
    assert_eq!(row_count(&b), 1, "range-1 row committed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_rolls_back_a_cross_range_transaction_atomically() {
    let (_node, sql_addr) = start_two_range_node().await;
    let port = sql_addr.rsplit(':').next().expect("port");
    let conn_str = format!("host=127.0.0.1 port={port} user=postgres");
    let (client, connection) = connect_with_retry(&conn_str).await;
    tokio::spawn(connection);
    client
        .simple_query("CREATE TABLE a (id int4)")
        .await
        .expect("a");
    client
        .simple_query("CREATE TABLE b (id int4)")
        .await
        .expect("b");
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO a VALUES (1)")
        .await
        .expect("a");
    client
        .simple_query("INSERT INTO b VALUES (2)")
        .await
        .expect("b");
    client.simple_query("ROLLBACK").await.expect("rollback");
    let a = client
        .simple_query("SELECT id FROM a")
        .await
        .expect("select a");
    let b = client
        .simple_query("SELECT id FROM b")
        .await
        .expect("select b");
    assert_eq!(row_count(&a), 0, "range-0 row rolled back");
    assert_eq!(row_count(&b), 0, "range-1 row rolled back");
}
