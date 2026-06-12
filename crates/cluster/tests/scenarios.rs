//! End-to-end scenarios over the in-process cluster harness. T3 proves the
//! happy path: a write proposed on the leader replicates and applies on every
//! replica. Fault-injection scenarios land in T4.

use std::time::Duration;

use cluster::Cluster;
use kv::Kv;

/// Wait (bounded) until `node` has applied at least up to log index `idx`. Never
/// sleeps: a stuck replica fails the test fast instead of hanging.
async fn applied_to(node: &cluster::Node, idx: u64) {
    node.raft
        .wait(Some(Duration::from_secs(10)))
        .applied_index_at_least(Some(idx), "catch up")
        .await
        .expect("applied");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_replicates_to_all_replicas() {
    let c = Cluster::new(3).await;
    let _leader = c.wait_for_leader().await;

    let k = kv::key::row_key(1, 1);
    c.write(vec![kv::WriteOp::Put {
        key: k.clone(),
        value: b"v".to_vec(),
    }])
    .await
    .expect("write");

    // Observed log layout: index 0 is the membership entry committed by
    // `initialize`, index 1 is the leader's blank no-op on election, and index 2
    // is the first client write. Wait for every replica to apply at least up to
    // the write, then assert the value landed — no fixed sleeps, the bounded
    // `wait` fails fast if stuck.
    for id in 0..3u64 {
        c.node(id)
            .raft
            .wait(Some(Duration::from_secs(10)))
            .applied_index_at_least(Some(2), "applied the write")
            .await
            .expect("apply");
        assert_eq!(
            c.node(id).sm_kv.get(&k).expect("get"),
            Some(b"v".to_vec()),
            "node {id} must have replicated the write"
        );
    }
}

/// A follower paused during a burst of writes must catch up once resumed — the
/// leader keeps committing on the surviving majority, and replication backfills
/// the paused replica when its link returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_follower_catches_up_on_resume() {
    let c = Cluster::new(3).await;
    let leader = c.wait_for_leader().await;
    let follower = (0..3u64).find(|&i| i != leader).expect("a follower");

    c.pause(follower);
    for i in 0..5 {
        c.write(vec![kv::WriteOp::Put {
            key: kv::key::row_key(1, i),
            value: vec![i as u8],
        }])
        .await
        .expect("write");
    }
    c.resume(follower);

    // Log layout: index 0 = membership, index 1 = leader no-op, indices 2..=6 =
    // the five writes. The paused follower must apply up to index 6.
    applied_to(c.node(follower), 6).await;
    assert_eq!(
        c.node(follower)
            .sm_kv
            .get(&kv::key::row_key(1, 4))
            .expect("get"),
        Some(vec![4]),
        "resumed follower must have the last write"
    );
}

/// Isolating the leader forces the surviving majority to elect a new one, and a
/// monotonic counter seeded before the failover must not regress: it was
/// committed on a majority that includes the future leader, so it survives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolating_leader_elects_new_leader_no_xid_reuse() {
    let c = Cluster::new(3).await;
    let l0 = c.wait_for_leader().await;

    // Advance next_xid to 10 through the log. With 3 nodes this commits on a
    // 2-node majority, which (after l0 is isolated) still forms the new quorum.
    c.write(vec![kv::WriteOp::Put {
        key: kv::key::next_xid_key(),
        value: 10u64.to_be_bytes().to_vec(),
    }])
    .await
    .expect("seed");

    c.isolate(l0);
    let l1 = c.wait_for_leader_excluding(l0).await;
    assert_ne!(l1, l0, "a different node must lead after isolation");

    // The new leader's applied next_xid is still >= 10 (durable through Raft).
    let v = c
        .node(l1)
        .sm_kv
        .get(&kv::key::next_xid_key())
        .expect("get")
        .expect("present");
    assert!(
        u64::from_be_bytes(v.try_into().expect("u64")) >= 10,
        "counter must not regress across failover"
    );
    c.heal();
}

/// An isolated leader cannot reach a majority, so a write proposed on it must not
/// commit. openraft returns an error (or, defensively, we time out) — either way
/// the proposal does not succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minority_partition_cannot_commit() {
    let c = Cluster::new(3).await;
    let l0 = c.wait_for_leader().await;
    c.isolate(l0);

    // Bound the call: an isolated leader may block until openraft notices it lost
    // quorum. A timeout is itself proof it did not commit; a prompt error is the
    // same outcome. Either way `is_err()`/timeout means "did not commit".
    let client_write = c
        .node(l0)
        .raft
        .client_write(cluster::WriteBatch(vec![kv::WriteOp::Put {
            key: kv::key::row_key(1, 9),
            value: vec![9],
        }]));
    let outcome = tokio::time::timeout(Duration::from_secs(5), client_write).await;
    match outcome {
        Err(_elapsed) => { /* timed out waiting for quorum -> did not commit */ }
        Ok(r) => assert!(r.is_err(), "minority leader must not commit"),
    }

    c.heal();
}

/// A follower paused long past a snapshot boundary loses the log entries it would
/// need for replay (the aggressive snapshot policy purges them), so the leader
/// must repair it with an installed snapshot. Either way it must fully catch up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn far_behind_follower_recovers_via_snapshot() {
    let c = Cluster::new_with_snapshotting(3).await;
    let leader = c.wait_for_leader().await;
    let follower = (0..3u64).find(|&i| i != leader).expect("a follower");

    c.pause(follower);
    for i in 0..20 {
        c.write(vec![kv::WriteOp::Put {
            key: kv::key::row_key(1, i),
            value: vec![i as u8],
        }])
        .await
        .expect("write");
    }
    c.resume(follower);

    // Log layout: index 0 = membership, index 1 = leader no-op, indices 2..=21 =
    // the twenty writes. The follower must apply up to index 21.
    applied_to(c.node(follower), 21).await;
    assert_eq!(
        c.node(follower)
            .sm_kv
            .get(&kv::key::row_key(1, 19))
            .expect("get"),
        Some(vec![19]),
        "recovered follower must have the last write"
    );
    // Evidence the follower was repaired via a snapshot install (not pure log
    // replay): its metrics report an installed snapshot. Under this aggressive
    // policy the leader also purges the entries the follower missed, so a
    // snapshot is the only way it could have caught up.
    let snap = c.node(follower).raft.metrics().borrow().snapshot;
    assert!(
        snap.is_some(),
        "far-behind follower should show an installed snapshot, got {snap:?}"
    );
}
