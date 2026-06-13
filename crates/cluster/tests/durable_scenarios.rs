//! Durable-cluster restart/recovery scenarios (SP8 T5). Each builds a 3-node
//! durable cluster under a `TempDir`, drives it through restarts (clean bounces,
//! leader crashes, full-group bounces), and asserts committed state is recovered
//! from disk after the fjall `Database` is closed and reopened. No `sleep`: every
//! wait is bounded by `Raft::wait`, so a stuck recovery fails fast instead of
//! hanging.

use std::time::Duration;

use cluster::Cluster;

/// Bounded wait until `node` has applied at least up to log index `idx`.
async fn applied_to(node: &cluster::Node, idx: u64) {
    node.raft
        .wait(Some(Duration::from_secs(10)))
        .applied_index_at_least(Some(idx), "catch up")
        .await
        .expect("applied");
}

/// The leader's current `last_log_index`, used as a concrete catch-up target so we
/// never hardcode fragile log-layout numbers.
fn last_log_index(node: &cluster::Node) -> u64 {
    node.raft
        .metrics()
        .borrow()
        .last_log_index
        .expect("has last_log_index")
}

/// (1) A committed write applied by a follower survives that follower's restart:
/// after a clean bounce (fjall close + reopen, journal replay), the value is read
/// back from the durable `data` keyspace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_write_survives_node_restart() {
    let dir = tempfile::tempdir().expect("dir");
    let mut c = Cluster::durable(3, dir.path()).await;
    let _l = c.wait_for_leader().await;

    let k = kv::key::row_key(1, 1);
    c.write(vec![kv::WriteOp::Put {
        key: k.clone(),
        value: b"v".to_vec(),
    }])
    .await
    .expect("write");

    let leader = c.leader().expect("leader").id;
    let follower = (0..3u64).find(|&i| i != leader).expect("follower");
    // idx0 = membership, idx1 = leader noop, idx2 = the write.
    applied_to(c.node(follower), 2).await;

    c.restart(follower).await;

    assert_eq!(
        c.node(follower).sm_kv.get(&k).expect("get"),
        Some(b"v".to_vec()),
        "committed write survived the restart"
    );
}

/// (2) A follower that MISSED a burst of writes (paused across them) catches up
/// after restart + resume. The pause fault is keyed by id, so it persists across
/// the restart; the reopened replica starts from its pre-pause durable state and
/// replication backfills it once the link is restored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_follower_catches_up() {
    let dir = tempfile::tempdir().expect("dir");
    let mut c = Cluster::durable(3, dir.path()).await;
    let leader = c.wait_for_leader().await;
    let follower = (0..3u64).find(|&i| i != leader).expect("follower");

    c.pause(follower);
    for i in 0..5u64 {
        c.write(vec![kv::WriteOp::Put {
            key: kv::key::row_key(2, i),
            value: vec![i as u8],
        }])
        .await
        .expect("write");
    }
    // Capture the catch-up target while the leader still leads (the writes are
    // committed on the surviving majority).
    let target = last_log_index(c.node(leader));

    c.restart(follower).await; // reopens with only its pre-pause state
    c.resume(follower); // link restored; replication backfills it

    applied_to(c.node(follower), target).await;
    for i in 0..5u64 {
        assert_eq!(
            c.node(follower)
                .sm_kv
                .get(&kv::key::row_key(2, i))
                .expect("get"),
            Some(vec![i as u8]),
            "restarted follower must have row {i} after catch-up"
        );
    }
}

/// (3) The leader is restarted; a DIFFERENT leader must emerge, and once the old
/// leader rejoins it must carry BOTH the row committed under it and the row
/// committed under its successor.
///
/// Race control: a freshly reopened durable node can re-win before the other two
/// time out (reopen is sub-election-timeout), which would make
/// `wait_for_leader_excluding(old)` hang. We isolate the restarted old leader
/// briefly so the majority is forced to elect someone else, then heal and let it
/// catch up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_crash_and_restart() {
    let dir = tempfile::tempdir().expect("dir");
    let mut c = Cluster::durable(3, dir.path()).await;
    let old = c.wait_for_leader().await;

    let k1 = kv::key::row_key(3, 1);
    c.write(vec![kv::WriteOp::Put {
        key: k1.clone(),
        value: b"a".to_vec(),
    }])
    .await
    .expect("write a");
    // Ensure the row is durable on every node before we bounce the leader.
    for id in 0..3u64 {
        applied_to(c.node(id), 2).await;
    }

    c.restart(old).await; // old leader bounces (close + reopen from disk)
    c.isolate(old); // keep it out so a different node must win
    let neu = c.wait_for_leader_excluding(old).await;
    assert_ne!(neu, old, "a different node must lead after the bounce");

    // Write the second row on the CONFIRMED new leader directly, not via
    // `c.write()`/`c.leader()`: the just-isolated old leader still reports
    // `state == Leader` for itself until it notices it lost quorum, so a
    // leader-by-self-report probe can pick it — and a `client_write` on an
    // isolated leader never commits (it blocks forever waiting for a majority).
    // `neu` is reachable to node `!= old`, a majority, so this commits promptly.
    let k2 = kv::key::row_key(3, 2);
    c.node(neu)
        .raft
        .client_write(cluster::WriteBatch(vec![kv::WriteOp::Put {
            key: k2.clone(),
            value: b"b".to_vec(),
        }]))
        .await
        .expect("write b on new leader");

    c.heal(); // let old rejoin and catch up

    // Old catches up to the new leader's log tip.
    let target = last_log_index(c.node(neu));
    applied_to(c.node(old), target).await;

    assert_eq!(
        c.node(old).sm_kv.get(&k1).expect("get"),
        Some(b"a".to_vec()),
        "restarted old leader must still have the pre-crash row"
    );
    assert_eq!(
        c.node(old).sm_kv.get(&k2).expect("get"),
        Some(b"b".to_vec()),
        "restarted old leader must have the post-crash row"
    );
    // The new leader has both rows too.
    assert_eq!(
        c.node(neu).sm_kv.get(&k1).expect("get"),
        Some(b"a".to_vec())
    );
    assert_eq!(
        c.node(neu).sm_kv.get(&k2).expect("get"),
        Some(b"b".to_vec())
    );
}

/// (4) Whole-cluster bounce: commit several rows, restart ALL three nodes
/// sequentially (each closes + reopens its fjall `Database`), re-elect a leader
/// from the persisted vote/log, and assert every committed row is present on
/// every node — the durable `data` keyspace survived the full power-cycle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_cluster_restart() {
    let dir = tempfile::tempdir().expect("dir");
    let mut c = Cluster::durable(3, dir.path()).await;
    let leader = c.wait_for_leader().await;

    const N: u64 = 4;
    for i in 0..N {
        c.write(vec![kv::WriteOp::Put {
            key: kv::key::row_key(4, i),
            value: vec![i as u8],
        }])
        .await
        .expect("write");
    }
    // Make the rows durable+applied everywhere before the bounce.
    let target = last_log_index(c.node(leader));
    for id in 0..3u64 {
        applied_to(c.node(id), target).await;
    }

    // Whole group bounces; durable state must persist across every reopen.
    for id in 0..3u64 {
        c.restart(id).await;
    }
    c.wait_for_leader().await; // re-election from persisted vote/log

    // Each node already had the rows applied+fsynced before the bounce, so the
    // durable `data` keyspace holds them after reopen.
    for id in 0..3u64 {
        for i in 0..N {
            assert_eq!(
                c.node(id).sm_kv.get(&kv::key::row_key(4, i)).expect("get"),
                Some(vec![i as u8]),
                "node {id} must have row {i} after full-cluster restart"
            );
        }
    }
}
