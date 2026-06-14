//! SP15 in-crate proof: a node in Replicated mode sources its range layout from
//! the meta range (range 0), not from its own `--range-boundaries`. Two
//! ServerNodes over loopback TCP; node 0 seeds the layout, node 1 learns it.
//! Deterministic — `start()` returns only after the committed blob is read, so
//! `node.range_map` is the authoritative map.

use std::time::Duration;

use cluster::range::map::RangeMap;
use cluster::server_node::{NodeConfig, RangeLayout, ServerNode};

async fn free_port() -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let a = l.local_addr().expect("local_addr").to_string();
    drop(l);
    a
}

/// Bring up a 2-node Replicated cluster: node 0 bootstraps with `seed`, node 1
/// joins with `joiner_seed`. Retries on the `free_port` bind race (bounded), the
/// established pattern from `remote_forward.rs`.
async fn two_node_replicated(
    seed: RangeMap,
    joiner_seed: Option<RangeMap>,
) -> (ServerNode, ServerNode) {
    let mut last_err = None;
    for _ in 0..16 {
        match try_two_node_replicated(seed.clone(), joiner_seed.clone()).await {
            Ok(pair) => return pair,
            Err(e) => last_err = Some(e),
        }
    }
    panic!("two_node_replicated: port race did not clear in 16 attempts: {last_err:?}");
}

async fn try_two_node_replicated(
    seed: RangeMap,
    joiner_seed: Option<RangeMap>,
) -> std::io::Result<(ServerNode, ServerNode)> {
    let n0_node = free_port().await;
    let n0_sql = free_port().await;
    let n1_node = free_port().await;
    let n1_sql = free_port().await;
    let peers = vec![
        (0u64, cluster::addr::pack(&n0_node, &n0_sql)),
        (1u64, cluster::addr::pack(&n1_node, &n1_sql)),
    ];
    let d0 = tempfile::tempdir().expect("tempdir0").keep();
    let d1 = tempfile::tempdir().expect("tempdir1").keep();

    // Start node 0 (bootstrap, seeds the layout) and node 1 concurrently — node 1
    // must be up to form range 0's quorum so node 0 can become leader and seed.
    let n0_cfg = NodeConfig {
        id: 0,
        node_addr: n0_node.clone(),
        sql_addr: n0_sql.clone(),
        data_dir: d0,
        peers: peers.clone(),
        bootstrap: true,
        layout: RangeLayout::Replicated { seed: Some(seed) },
    };
    let n1_cfg = NodeConfig {
        id: 1,
        node_addr: n1_node.clone(),
        sql_addr: n1_sql.clone(),
        data_dir: d1,
        peers,
        bootstrap: false,
        layout: RangeLayout::Replicated { seed: joiner_seed },
    };
    let (n0, n1) = tokio::try_join!(ServerNode::start(n0_cfg), ServerNode::start(n1_cfg))?;
    Ok((n0, n1))
}

/// Criterion 3: a node started with NO seed derives the bootstrap node's
/// committed range map from the meta range.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_seed_node_derives_committed_range_map() {
    let seed = RangeMap::with_boundaries(vec![2]);
    let (n0, n1) = two_node_replicated(seed.clone(), None).await;
    assert_eq!(n0.range_map, seed, "bootstrap node uses its seed");
    assert_eq!(
        n1.range_map, seed,
        "joiner with no boundaries learns the layout from the meta range"
    );
}

/// Criterion 4 (load-bearing): a node started with a WRONG seed still routes by
/// the committed descriptors. Committed `[2]` ⇒ table id 2 is range 1; the
/// joiner's wrong seed `[3]` alone would put id 2 in range 0. The joiner follows
/// the committed map.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_seed_node_routes_by_committed_map() {
    let committed = RangeMap::with_boundaries(vec![2]);
    let wrong = RangeMap::with_boundaries(vec![3]);
    let (_n0, n1) = two_node_replicated(committed.clone(), Some(wrong)).await;
    assert_eq!(
        n1.range_map, committed,
        "the meta range overrides the joiner's wrong local seed"
    );
    assert_eq!(
        n1.range_map.range_for_table(2),
        1,
        "table id 2 routes to range 1 per the committed map, not range 0 per the wrong seed"
    );
}

/// Await `range`'s self-confirmed leader across the two nodes (openraft event
/// wait, no sleep).
async fn wait_leader(n0: &ServerNode, n1: &ServerNode, range: u32) -> u64 {
    let mut set = tokio::task::JoinSet::new();
    for node in [n0, n1] {
        let raft = node.rafts.get(&range).expect("range raft").clone();
        set.spawn(async move {
            raft.wait(Some(Duration::from_secs(20)))
                .metrics(
                    |m| m.state == openraft::ServerState::Leader && m.current_leader == Some(m.id),
                    "self leader",
                )
                .await
                .map(|m| m.id)
                .ok()
        });
    }
    while let Some(res) = set.join_next().await {
        if let Ok(Some(id)) = res {
            return id;
        }
    }
    panic!("range {range} elected no leader");
}

/// Await every replica of `range` applying up to the leader's applied index — the
/// `wait_for_replication` analog from `remote_forward.rs`. Captures the leader's
/// applied index as a relative target AFTER the write, then waits each replica to
/// reach it. Event-based, no sleep, no vacuous fixed index.
async fn wait_for_replication(n0: &ServerNode, n1: &ServerNode, range: u32) {
    let leader = wait_leader(n0, n1, range).await;
    let nodes = [n0, n1];
    let target = nodes[leader as usize]
        .rafts
        .get(&range)
        .expect("range raft")
        .metrics()
        .borrow()
        .last_applied
        .map(|l| l.index)
        .unwrap_or(0);
    for node in nodes {
        node.rafts
            .get(&range)
            .expect("range raft")
            .wait(Some(Duration::from_secs(20)))
            .metrics(
                |m| m.last_applied.map(|l| l.index).unwrap_or(0) >= target,
                "follower caught up to leader applied index",
            )
            .await
            .expect("replication within bound");
    }
}

/// Criterion 5: routing through a replicated node lands rows in the range the
/// committed boundaries dictate. Drive writes via the forward pool (as
/// remote_forward.rs does) so the test does not depend on which node leads which
/// range. Committed `[2]`: table `a` (id 1) ⇒ range 0, table `b` (id 2) ⇒ range 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_routing_lands_rows_in_the_committed_range() {
    use cluster::forward::{ForwardPool, RetryCounter};
    let (n0, n1) = two_node_replicated(RangeMap::with_boundaries(vec![2]), None).await;
    wait_leader(&n0, &n1, 0).await;
    wait_leader(&n0, &n1, 1).await;

    // Use node 0's forward pool: it resolves each range's leader and forwards.
    let pool = ForwardPool::new(
        n0.rafts.clone(),
        n0.partition.clone(),
        RetryCounter::default(),
    );
    pool.forward(0, "CREATE TABLE a (id int4)".into())
        .await
        .expect("create a -> range 0"); // table id 1
    pool.forward(0, "CREATE TABLE b (id int4)".into())
        .await
        .expect("create b -> range 0 (DDL routes to range 0)"); // table id 2
    pool.forward(1, "INSERT INTO b VALUES (42)".into())
        .await
        .expect("insert b -> range 1");

    // The forwarded INSERT is committed+applied on range 1's leader (forward()
    // returns post-apply); wait for it to replicate to EVERY range-1 replica using
    // the relative-target pattern (NOT a vacuous index >= 1 — bootstrap + election
    // already push range 1's last_applied past 1 before any INSERT). Then assert the
    // row is in range 1's store and absent from range 0's, on BOTH nodes.
    wait_for_replication(&n0, &n1, 1).await;
    let prefix = kv::key::table_prefix(2); // table id 2 ⇒ range 1
    for node in [&n0, &n1] {
        assert!(
            !node
                .sm_kv(1)
                .scan_prefix(&prefix)
                .expect("scan r1")
                .is_empty(),
            "row for table id 2 is on range 1's store of node {}",
            node.id()
        );
        assert!(
            node.sm_kv(0)
                .scan_prefix(&prefix)
                .expect("scan r0")
                .is_empty(),
            "row for table id 2 is NOT on range 0's store of node {}",
            node.id()
        );
    }
}

/// Write-once guard: once the descriptor blob is committed, a second seed with a
/// DIFFERENT map does NOT rewrite it. This is the immutable-local-read invariant —
/// a stray rewrite would make every node's local (un-leader-confirmed) read stale.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn descriptor_blob_is_write_once() {
    use cluster::range::meta::{read_range_map, seed_if_absent};
    let committed = RangeMap::with_boundaries(vec![2]);
    let (n0, _n1) = two_node_replicated(committed.clone(), None).await;

    // The blob is already present (seeded at bring-up). A second seed attempt with
    // a DIFFERENT map must be a no-op: create-if-absent sees the existing blob and
    // skips the write. (read_range_map reads the local applied store, which already
    // holds the committed [2], so no leader round-trip is needed.)
    seed_if_absent(
        &n0.rafts[&0],
        n0.sm_kv(0).as_ref(),
        &RangeMap::with_boundaries(vec![3]),
    )
    .await
    .expect("second seed is a no-op");

    assert_eq!(
        read_range_map(n0.sm_kv(0).as_ref()).expect("read"),
        Some(committed),
        "the committed blob was NOT overwritten by the second seed"
    );
}
