//! SP15 e2e: 3 processes in Replicated mode. Node 0 seeds the range layout into
//! the meta range; nodes 1 and 2 are given NO `--range-boundaries` and learn the
//! layout from the meta range. A client connects to a node that was never told the
//! boundaries, writes a row into each range, and reads them back through a
//! different node — proving the layout came from the replicated meta range, not
//! config. One test (a 3-node × 2-range cluster = 6 Raft instances) to keep the
//! binary from running two such clusters at once on a constrained runner.
mod harness;
use harness::Cluster;
use tokio_postgres::SimpleQueryMessage;

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
        gw.simple_query("INSERT INTO b VALUES (20)")
            .await
            .expect("insert b");
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
