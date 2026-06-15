//! SP15 e2e: 3 processes in Replicated mode. Node 0 seeds the range layout into
//! the meta range; nodes 1 and 2 are given NO `--range-boundaries` and learn the
//! layout from the meta range. A client connects to a node that was never told the
//! boundaries, writes a row into each range, and reads them back through a
//! different node — proving the layout came from the replicated meta range, not
//! config. One test (a 3-node × 2-range cluster = 6 Raft instances) to keep the
//! binary from running two such clusters at once on a constrained runner.
mod harness;
use std::time::Duration;

use harness::Cluster;
use tokio_postgres::SimpleQueryMessage;

/// Bounded-retry a write on `gw`, retrying ONLY on the retryable `40001` a freshly-risen
/// range leader returns while its settle-before-serve `RecoveryGate` is still closed (the gate
/// rejects BEFORE executing, so the row is never inserted — safe to retry on the SAME
/// connection without double-applying, which an exact `row_count == 1` read-back requires).
/// Bounded poll cadence (the allowed multi-process harness cadence), never a settle-sleep.
async fn write_until_served(gw: &tokio_postgres::Client, sql: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match gw.simple_query(sql).await {
            Ok(_) => return,
            Err(e) if e.code().map(|c| c.code()) == Some("40001") => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "`{sql}` not served within 30s (gate stayed closed)"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("`{sql}` failed (non-retryable): {e}"),
        }
    }
}

fn row_count(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

fn first_col(msgs: &[SimpleQueryMessage]) -> Option<String> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => r.get(0).map(|s| s.to_string()),
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_layout_is_learned_from_the_meta_range() {
    // Boundary at table_id 2 seeded by node 0 only; nodes 1 and 2 get no
    // boundaries. table `a` (id 1) -> range 0; table `b` (id 2) -> range 1.
    let c = Cluster::spawn_multirange_replicated(3, vec![2]).await;
    c.wait_for_leader().await;

    // Connect to node 1 — which was NEVER given the boundaries. Its gateway routes
    // by the layout it read from the meta range.
    {
        let gw = c.pg(1).await;
        gw.simple_query("CREATE TABLE a (id int4)")
            .await
            .expect("create a (range 0)");
        gw.simple_query("CREATE TABLE b (id int4)")
            .await
            .expect("create b (range 1)");
        gw.simple_query("INSERT INTO a VALUES (10)")
            .await
            .expect("insert a");
        // INSERT INTO b lands on range 1's leader, which may still be inside its
        // settle-before-serve window on a freshly-risen leader (retryable 40001) — bounded-retry.
        write_until_served(&gw, "INSERT INTO b VALUES (20)").await;
    }

    // Read both back through node 2 (also never given boundaries).
    let client = c.pg(2).await;
    let ra = client
        .simple_query("SELECT id FROM a")
        .await
        .expect("select a");
    assert_eq!(row_count(&ra), 1, "node 2 reads a (range 0)");
    assert_eq!(first_col(&ra).as_deref(), Some("10"), "a.id == 10");
    let rb = client
        .simple_query("SELECT id FROM b")
        .await
        .expect("select b");
    assert_eq!(row_count(&rb), 1, "node 2 reads b (range 1)");
    assert_eq!(first_col(&rb).as_deref(), Some("20"), "b.id == 20");
}
