//! D3a-net Task 4: a write issued at a gateway that is a FOLLOWER for the target
//! range is forwarded over a pooled pgwire client to the remote range leader and
//! becomes visible on every replica of that range (event-based applied-index
//! wait). A test-only one-shot makes the first forward observe `NotLeader`
//! exactly once; the test asserts the gateway's re-resolve+retry counter == 1
//! (mechanically checkable, not racing a real election). No sleep.

use std::time::Duration;

use cluster::forward::{ForwardPool, RetryCounter};
use cluster::range::map::{RangeId, RangeMap};
use cluster::server_node::{NodeConfig, RangeLayout, ServerNode};

/// Bind an ephemeral loopback port, read its address, drop the listener so the
/// address is free for the node to rebind.
async fn free_port() -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let a = l.local_addr().expect("local_addr").to_string();
    drop(l);
    a
}

/// Two co-located multi-range nodes (ranges {0,1}); return both. Both bootstrap
/// range 0 and range 1.
///
/// Retries on a port-bind race: `free_port` binds-then-drops to discover a free
/// port, so under heavy test contention another binder can steal it before the
/// node rebinds, and `ServerNode::start`'s `bind` returns `Err`. We retry with
/// fresh ports rather than flake — bounded so a genuinely stuck bind still fails.
async fn two_node_cluster() -> (ServerNode, ServerNode) {
    let mut last_err = None;
    for _ in 0..16 {
        match try_two_node_cluster().await {
            Ok(pair) => return pair,
            Err(e) => last_err = Some(e),
        }
    }
    panic!("two_node_cluster: port-bind race did not clear in 16 attempts: {last_err:?}");
}

async fn try_two_node_cluster() -> std::io::Result<(ServerNode, ServerNode)> {
    let map = RangeMap::with_boundaries(vec![2]); // table id 1 -> range 0, id >=2 -> range 1
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
    let n0 = ServerNode::start(NodeConfig {
        id: 0,
        node_addr: n0_node.clone(),
        sql_addr: n0_sql.clone(),
        data_dir: d0,
        peers: peers.clone(),
        bootstrap: true,
        layout: RangeLayout::Static(map.clone()),
    })
    .await?;
    let n1 = ServerNode::start(NodeConfig {
        id: 1,
        node_addr: n1_node.clone(),
        sql_addr: n1_sql.clone(),
        data_dir: d1,
        peers,
        bootstrap: false,
        layout: RangeLayout::Static(map),
    })
    .await?;
    Ok((n0, n1))
}

/// Await `range`'s self-confirmed leader id across the two nodes' raft handles,
/// using openraft's event-based `wait` (no sleep). Returns the leader node id.
async fn wait_leader(n0: &ServerNode, n1: &ServerNode, range: RangeId) -> u64 {
    let mut set = tokio::task::JoinSet::new();
    for node in [n0, n1] {
        let raft = node.rafts.get(&range).expect("range raft").clone();
        set.spawn(async move {
            raft.wait(Some(Duration::from_secs(20)))
                .metrics(
                    |m| m.state == openraft::ServerState::Leader && m.current_leader == Some(m.id),
                    "self-confirmed leader",
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
    panic!("range {range} elected no leader within the bound");
}

/// Await every replica of `range` applying up to the leader's applied index — the
/// `wait_for_replication` analog over `ServerNode` raft handles. Event-based.
async fn wait_for_replication(n0: &ServerNode, n1: &ServerNode, range: RangeId) {
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

/// A write at a gateway that does NOT lead range 1 is forwarded to range 1's
/// remote leader over the pooled pgwire client and lands on every range-1 replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_at_follower_gateway_forwards_to_remote_leader() {
    let (n0, n1) = two_node_cluster().await;
    let r1_leader = wait_leader(&n0, &n1, 1).await;
    // The gateway is the node that does NOT lead range 1, so its range-1 write
    // forwards over the wire.
    let gw = if r1_leader == 0 { &n1 } else { &n0 };

    // Create the table on range 0 through the gateway (CREATE routes to range 0;
    // the gateway forwards or runs locally depending on range-0 leadership).
    let counter = RetryCounter::default();
    let pool = ForwardPool::new(gw.rafts.clone(), gw.partition.clone(), counter.clone());
    pool.forward(0, "CREATE TABLE a (id int4)".into())
        .await
        .expect("create a -> range 0"); // table id 1 -> range 0 (per RangeMap::with_boundaries(vec![2]))
    pool.forward(0, "CREATE TABLE b (id int4)".into())
        .await
        .expect("create b -> range 0"); // table id 2 -> range 1

    // INSERT into b: the gateway is a range-1 follower, so this forwards to the
    // remote range-1 leader over the pooled pgwire client.
    pool.forward(1, "INSERT INTO b VALUES (42)".into())
        .await
        .expect("insert b -> forwarded to range 1 leader");

    // The row is replicated to every range-1 replica (event-based wait, no sleep).
    wait_for_replication(&n0, &n1, 1).await;
    for node in [&n0, &n1] {
        let r1 = node.sm_kv(1);
        let prefix = kv::key::table_prefix(2); // table id 2 -> range 1
        assert!(
            !r1.scan_prefix(&prefix).expect("scan r1").is_empty(),
            "forwarded row present on range-1 store of node {}",
            node.id()
        );
    }
}

/// A deterministically-injected single `NotLeader` on the first forward causes
/// exactly one re-resolve+retry. The retry counter is asserted == 1 (mechanical,
/// not racing an election).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_shot_notleader_triggers_exactly_one_retry() {
    let (n0, n1) = two_node_cluster().await;
    wait_leader(&n0, &n1, 0).await;
    let r1_leader = wait_leader(&n0, &n1, 1).await;
    let gw = if r1_leader == 0 { &n1 } else { &n0 };

    let counter = RetryCounter::default();
    let pool = ForwardPool::new(gw.rafts.clone(), gw.partition.clone(), counter.clone());
    pool.forward(0, "CREATE TABLE b (id int4)".into())
        .await
        .expect("create b -> range 0"); // table id 1 -> range 0; b is id 1 here (only table)

    // Arm the one-shot: the NEXT forward to range 1 fakes a single `NotLeader`
    // from the wire before contacting the real leader, then disarms.
    pool.arm_one_shot_notleader(1);

    // The write still succeeds — after one re-resolve+retry against the freshly
    // re-read range-1 leader.
    pool.forward(1, "INSERT INTO b VALUES (7)".into())
        .await
        .expect("insert succeeds after one retry");

    assert_eq!(
        counter.get(),
        1,
        "exactly one re-resolve+retry was performed for the injected NotLeader"
    );
    // Sanity: a second uninjected forward performs no further retries.
    pool.forward(1, "INSERT INTO b VALUES (8)".into())
        .await
        .expect("second insert, no injection");
    assert_eq!(counter.get(), 1, "no extra retries without injection");
}
