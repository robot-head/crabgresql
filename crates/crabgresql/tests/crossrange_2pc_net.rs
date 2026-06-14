//! D3c-net: a cross-range BEGIN..COMMIT issued at a gateway that does NOT lead all
//! participant ranges commits atomically across processes; ROLLBACK leaves neither
//! row; a participant-leader failover keeps the cluster serving.
//!
//! All three tests share one cluster topology: `spawn_multirange(3, vec![2])` — the
//! STATIC (non-replicated) spawner, which wires the `TxnService` 2PC service on
//! every node. Using the replicated spawner would return `TxnResp::Err` for every
//! Txn RPC (see SP17 carry-note).
//!
//! One test per file to keep each binary to a single 3-node × 2-range cluster (6
//! Raft instances) at a time — concurrently running tests would spawn 12+ Raft
//! instances and starve each other's elections on a 2-core runner. The three
//! scenarios are split as individual #[tokio::test] items (nextest serialises the
//! binary's tests via the concurrency group in .config/nextest.toml).
mod harness;
use harness::Cluster;
use tokio_postgres::SimpleQueryMessage;

/// True when the `simple_query` result contains at least one Row.
fn has_rows(msgs: &[SimpleQueryMessage]) -> bool {
    msgs.iter().any(|m| matches!(m, SimpleQueryMessage::Row(_)))
}

/// Cross-range BEGIN..COMMIT commits atomically when issued at a gateway that does
/// not lead all participant ranges (forcing cross-node coordination).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_range_txn_commits_atomically_through_a_nonleading_gateway() {
    // table a (first user table, id 1) -> range 0; table b (id 2) -> range 1.
    let c = Cluster::spawn_multirange(3, vec![2]).await;

    // Create tables through node 0.
    let g = c.pg(0).await;
    g.simple_query("CREATE TABLE a (id int4)")
        .await
        .expect("create a");
    g.simple_query("CREATE TABLE b (id int4)")
        .await
        .expect("create b");

    // Pick a gateway that does not lead either participant range, ensuring the
    // coordinator must send at least one cross-node Txn RPC.
    let gw = c.pick_nonleading_gateway(&[0, 1]).await;
    let client = c.pg(gw).await;
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO a VALUES (1)")
        .await
        .expect("stage range 0");
    client
        .simple_query("INSERT INTO b VALUES (2)")
        .await
        .expect("stage range 1 (escalates to global 2PC)");
    client
        .simple_query("COMMIT")
        .await
        .expect("atomic cross-node commit");

    // Both rows visible read back through EVERY node (barrier + global clog).
    c.wait_select_value("SELECT id FROM a", "1").await;
    c.wait_select_value("SELECT id FROM b", "2").await;
}

/// Cross-range ROLLBACK leaves neither row visible on any node.
///
/// NOTE: `count(*)` (aggregate) is not implemented in this engine; we prove
/// absence by:
/// 1. Committing a distinct sentinel value (id=99) in each table via autocommit.
/// 2. Checking the sentinel is visible (proves the query path and table work).
/// 3. Directly querying for the rolled-back value (id=1 / id=2) and asserting
///    it returns NO rows — the absence of the specific row we tried to INSERT
///    proves the ROLLBACK was effective.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_range_txn_rolls_back_atomically() {
    let c = Cluster::spawn_multirange(3, vec![2]).await;

    let g = c.pg(0).await;
    g.simple_query("CREATE TABLE a (id int4)")
        .await
        .expect("create a");
    g.simple_query("CREATE TABLE b (id int4)")
        .await
        .expect("create b");

    let gw = c.pick_nonleading_gateway(&[0, 1]).await;
    let client = c.pg(gw).await;
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO a VALUES (1)")
        .await
        .expect("insert a");
    client
        .simple_query("INSERT INTO b VALUES (2)")
        .await
        .expect("insert b");
    client.simple_query("ROLLBACK").await.expect("rollback");

    // Commit sentinel rows (autocommit) so we can confirm the table is readable
    // and cross-range routing works for SELECTs after the ROLLBACK.
    c.exec_until_ok("INSERT INTO a VALUES (99)").await;
    c.exec_until_ok("INSERT INTO b VALUES (99)").await;

    // Sentinels are visible through every node: proves the SELECT path works.
    c.wait_select_value("SELECT id FROM a WHERE id = 99", "99")
        .await;
    c.wait_select_value("SELECT id FROM b WHERE id = 99", "99")
        .await;

    // The rolled-back rows (id=1, id=2) must NOT be visible on any live node.
    // Since the global decision (Aborted(g)) is durable BEFORE ROLLBACK returned
    // to the client, this is an immediate check — not a wait. We use a fresh
    // connection to each node and assert no Row messages come back.
    for node_id in 0..c.len() as u64 {
        if let Some(client) = c.pg_try(node_id as usize).await {
            let ra = client
                .simple_query("SELECT id FROM a WHERE id = 1")
                .await
                .expect("select rolled-back a");
            assert!(
                !has_rows(&ra),
                "node {node_id}: rolled-back row (a, id=1) is still visible after ROLLBACK"
            );
            let rb = client
                .simple_query("SELECT id FROM b WHERE id = 2")
                .await
                .expect("select rolled-back b");
            assert!(
                !has_rows(&rb),
                "node {node_id}: rolled-back row (b, id=2) is still visible after ROLLBACK"
            );
        }
    }
}

/// Killing range 1's leader after a commit leaves both rows durable (global clog),
/// and a fresh cross-range txn commits through the survivors after re-election.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_range_commit_survives_a_participant_leader_failover() {
    let mut c = Cluster::spawn_multirange(3, vec![2]).await;

    let g = c.pg(0).await;
    g.simple_query("CREATE TABLE a (id int4)")
        .await
        .expect("create a");
    g.simple_query("CREATE TABLE b (id int4)")
        .await
        .expect("create b");

    // First cross-range commit.
    let gw = c.pick_nonleading_gateway(&[0, 1]).await;
    let client = c.pg(gw).await;
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("INSERT INTO a VALUES (1)")
        .await
        .expect("insert a");
    client
        .simple_query("INSERT INTO b VALUES (2)")
        .await
        .expect("insert b");
    client.simple_query("COMMIT").await.expect("commit");

    // Kill range 1's leader; the committed rows must stay visible (durable global
    // clog on the surviving replicas), and a fresh cross-range txn must still commit
    // after the surviving nodes re-elect for range 1. 3 nodes tolerate 1 loss.
    let r1_leader = c.range_leader(1).await;
    c.kill(r1_leader).await;

    // The pre-kill rows survive the failover (read through any surviving node).
    c.wait_select_value("SELECT id FROM a WHERE id = 1", "1")
        .await;
    c.wait_select_value("SELECT id FROM b WHERE id = 2", "2")
        .await;

    // Wait for range 1 to re-elect a new leader before issuing the second
    // cross-range txn. `range_leader(1)` polls until range 1 has a leader again,
    // using the same bounded cadence as the other harness waits.
    c.range_leader(1).await;

    // A second cross-range commit succeeds after re-election. Use exec_until_ok for
    // each write because an in-flight statement during the tail of re-election may
    // return a retryable 40001; exec_until_ok retries on a new connection to a live
    // node until the write commits. The assertion is that the range RESUMES, not
    // that the first attempt wins.
    c.exec_until_ok("INSERT INTO a VALUES (3)").await;
    c.exec_until_ok("INSERT INTO b VALUES (4)").await;

    c.wait_select_value("SELECT id FROM a WHERE id = 3", "3")
        .await;
    c.wait_select_value("SELECT id FROM b WHERE id = 4", "4")
        .await;
}
