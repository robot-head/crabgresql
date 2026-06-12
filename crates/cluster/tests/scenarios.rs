//! End-to-end scenarios over the in-process cluster harness. T3 proves the
//! happy path: a write proposed on the leader replicates and applies on every
//! replica. Fault-injection scenarios land in T4.

use std::time::Duration;

use cluster::Cluster;
use kv::Kv;

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
