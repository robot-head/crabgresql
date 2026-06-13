//! D3a: SQL routes to the correct range; rows land only in that range's store.
use cluster::range::{MultiRangeCluster, RangeMap, RangeRouter};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_land_only_in_their_table_range() {
    // tables: id 1 -> range 0, id 2 -> range 1 (boundary at 2).
    let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
    for r in c.range_map().range_ids() {
        c.wait_for_leader(r).await;
    }
    let mut router = RangeRouter::connect(&c).await;
    router.simple("CREATE TABLE a (id int4)").await.expect("a");
    router.simple("CREATE TABLE b (id int4)").await.expect("b");
    router.simple("INSERT INTO b VALUES (20)").await.expect("insert b");

    // b's rows (table id 2 -> range 1) must be present in range 1's store and
    // absent from range 0's store, on every node (applied replication).
    use kv::key::table_prefix;
    let b_prefix = table_prefix(2);
    for node in 0..c.n() {
        let r1 = c.sm_kv(1, node);
        let r0 = c.sm_kv(0, node);
        assert!(
            !r1.scan_prefix(&b_prefix).expect("scan r1").is_empty(),
            "b's rows must be present in range 1 on node {node}"
        );
        assert!(
            r0.scan_prefix(&b_prefix).expect("scan r0").is_empty(),
            "b's rows must be absent from range 0 on node {node}"
        );
    }
}
