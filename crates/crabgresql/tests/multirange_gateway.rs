//! D3a-net e2e: 3 processes, each hosting every range of a 2-range map. A client
//! connects to an ARBITRARY node (the gateway); writes to tables in DIFFERENT
//! ranges land only in their range and read back through ANY node; killing one
//! range's leader keeps the OTHER range serving while the killed range re-elects.
//!
//! Per-range progress is observed at the SQL level (a committed read-back THROUGH
//! the owning range), because the harness control protocol is node-global (no
//! per-range applied index). Every wait is bounded + condition-driven.
//!
//! This is intentionally ONE test. Each scenario spawns a 3-node × 2-range cluster
//! = 6 Raft instances; libtest runs a binary's tests concurrently, so two such
//! clusters at once (12 Raft instances) starve each other's Raft progress past the
//! bounded deadlines on a constrained (2-core / coverage-instrumented) runner.
//! Keeping a single test caps the binary at one cluster at a time — the routing and
//! failover assertions are both exercised within it.
mod harness;
use std::time::Duration;

use harness::Cluster;
use tokio_postgres::SimpleQueryMessage;

/// Count the `Row` messages in a `simple_query` result.
fn row_count(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

/// Column 0 of the first row of a `simple_query` result, as an owned `String`.
fn first_col(msgs: &[SimpleQueryMessage]) -> Option<String> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => r.get(0).map(|s| s.to_string()),
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d3a_net_routes_by_range_and_survives_per_range_failover() {
    // Boundary at table_id 2: table `a` (first user table, id 1) -> range 0;
    // table `b` (id 2) -> range 1. Both ranges replicated to all 3 nodes.
    let mut c = Cluster::spawn_multirange(3, vec![2]).await;
    c.wait_for_leader().await;

    // --- Routing correctness: write across two ranges through ONE arbitrary gateway
    // (node 0 — not necessarily any range's leader), then read each back through
    // EVERY node. Each node's gateway forwards the SELECT to the owning range's
    // leader and relays the row back. ---
    {
        let gw = c.pg(0).await;
        gw.simple_query("CREATE TABLE a (id int4)")
            .await
            .expect("create a (range 0)");
        gw.simple_query("CREATE TABLE b (id int4)")
            .await
            .expect("create b (range 1)");
        gw.simple_query("INSERT INTO a VALUES (10)")
            .await
            .expect("insert a");
        gw.simple_query("INSERT INTO b VALUES (20)")
            .await
            .expect("insert b");
    }
    for id in 0..c.len() as u64 {
        let client = c.pg(id).await;
        let ra = client
            .simple_query("SELECT id FROM a")
            .await
            .expect("select a");
        assert_eq!(row_count(&ra), 1, "node {id} reads a (range 0)");
        assert_eq!(
            first_col(&ra).as_deref(),
            Some("10"),
            "node {id}: a.id == 10"
        );
        let rb = client
            .simple_query("SELECT id FROM b")
            .await
            .expect("select b");
        assert_eq!(row_count(&rb), 1, "node {id} reads b (range 1)");
        assert_eq!(
            first_col(&rb).as_deref(),
            Some("20"),
            "node {id}: b.id == 20"
        );
    }

    // --- Per-range failover. Gate the crash on SQL-observable per-range progress: a
    // fresh range-1 write read back THROUGH range 1 proves range 1 had a working
    // leader+commit pipeline at crash time (no per-range applied-index signal exists
    // over the node-global control protocol). ---
    c.pg(0)
        .await
        .simple_query("INSERT INTO b VALUES (30)")
        .await
        .expect("range-1 write before crash");
    c.wait_select_value("SELECT id FROM b WHERE id = 30", "30")
        .await;

    // Kill the node-global leader. Control resolves range 0, so this is range 0's
    // leader; co-located placement means that node also hosts a range-1 replica.
    // Whichever range it led must re-elect; a range led by a survivor keeps serving.
    let victim = c.wait_for_leader().await;
    c.kill(victim).await;

    // A new node-global leader emerges among the survivors (bounded, condition-driven
    // — re-probe status() on the harness poll cadence until some surviving node
    // reports a leader != victim).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut found = false;
        for id in (0..c.len() as u64).filter(|&i| i != victim) {
            if let Some(st) = c.status(id).await
                && st.current_leader.is_some_and(|l| l != victim)
            {
                found = true;
            }
        }
        if found {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no new leader after killing the old one"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // BOTH ranges serve again through surviving nodes: the prior rows stay readable
    // (round-robin past the killed node), and a FRESH write to EACH range commits.
    // We retry the writes through live nodes until they commit: during the brief
    // re-election window a forward to the just-deposed leader returns a RETRYABLE
    // 40001, which a correct client retries — the assertion is that the range
    // RESUMES, not that the first attempt mid-election wins. The read-backs prove the
    // writes are durable. (Re-applying an `INSERT (id)` is harmless: the read-back
    // asserts the value is present, not a row count.)
    c.wait_select_value("SELECT id FROM a WHERE id = 10", "10")
        .await;
    c.wait_select_value("SELECT id FROM b WHERE id = 20", "20")
        .await;
    c.exec_until_ok("INSERT INTO a VALUES (11)").await;
    c.exec_until_ok("INSERT INTO b VALUES (40)").await;
    c.wait_select_value("SELECT id FROM a WHERE id = 11", "11")
        .await;
    c.wait_select_value("SELECT id FROM b WHERE id = 40", "40")
        .await;
}
