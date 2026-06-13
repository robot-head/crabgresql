//! D3a: SQL routes to the correct range; rows land only in that range's store;
//! per-range failover is independent and per-range list-append stays consistent
//! under follower faults.
use cluster::range::{MultiRangeCluster, RangeMap, RangeRouter};
use pgwire::engine::QueryResult;
use std::time::Duration;

/// Run a single-column `int8` SELECT through the public router API and collect the
/// values in row order, RECONNECTING on a transient wire error (a stale leader
/// during churn) within a bounded budget. Integration tests can't reach the
/// router's `#[cfg(test)]` helper, so this mirrors it over only the public
/// `QueryResult`/`Cell` surface (`Cell::text` is the text-format value encoding).
async fn scan_col_i64(router: &mut RangeRouter, c: &MultiRangeCluster, sql: &str) -> Vec<i64> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        match router.simple(sql).await {
            Ok(QueryResult::Rows { rows, .. }) => {
                return rows
                    .iter()
                    .map(|r| {
                        let cell = r[0].as_ref().expect("non-null cell");
                        std::str::from_utf8(&cell.text)
                            .expect("utf8 text")
                            .parse::<i64>()
                            .expect("i64 value")
                    })
                    .collect();
            }
            Ok(other) => panic!("expected Rows for `{sql}`, got {other:?}"),
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "`{sql}` never read within budget (last error {}: {})",
                    e.code,
                    e.message
                );
                // Reconnect re-resolves current leaders (event-based wait), then retry.
                *router = RangeRouter::connect(c).await;
                tokio::task::yield_now().await;
            }
        }
    }
}

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
    router
        .simple("INSERT INTO b VALUES (20)")
        .await
        .expect("insert b");

    // b's rows (table id 2 -> range 1) must replicate to range 1's store on EVERY
    // node, and never appear in range 0's store. Wait (event-based, no sleep) for
    // every range-1 replica to apply up to the leader's index — then the rows are
    // deterministically present, with no apply race. The range-0 absence check is
    // not a timing race (range 0 never receives b's rows) — it is the routing
    // correctness assertion.
    use kv::key::table_prefix;
    let b_prefix = table_prefix(2);
    c.wait_for_replication(1).await;
    for node in 0..c.n() {
        let r1 = c.sm_kv(1, node);
        assert!(
            !r1.scan_prefix(&b_prefix).expect("scan r1").is_empty(),
            "b's rows must be present in range 1 on node {node}"
        );
        let r0 = c.sm_kv(0, node);
        assert!(
            r0.scan_prefix(&b_prefix).expect("scan r0").is_empty(),
            "b's rows must be absent from range 0 on node {node}"
        );
    }
}

/// Run `sql` (expected to succeed) through a router that may be pointing at a stale
/// leader during election churn. On any wire error, RECONNECT (the router caches
/// each range's leader at connect time, so it must re-`connect` to pick up a new
/// leader) and retry, within a bounded budget. Returns once the statement succeeds.
///
/// This is for statements that are idempotent-or-don't-matter on retry (DDL, or an
/// INSERT whose duplication the caller tolerates / guards separately). For appends
/// that must land exactly once, use [`append_once`].
async fn run_with_reconnect(router: &mut RangeRouter, c: &MultiRangeCluster, sql: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        match router.simple(sql).await {
            Ok(_) => return,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "`{sql}` never succeeded within budget (last error {}: {})",
                    e.code,
                    e.message
                );
                // Reconnect to pick up whatever leaders exist now, then retry.
                // Reconnect re-resolves each range's current leader via the
                // event-based `wait_for_leader`, so it blocks until leaders exist —
                // that is the pacing. `yield_now` (not a timed sleep) just cedes the
                // scheduler so a tight retry can't starve the runtime.
                *router = RangeRouter::connect(c).await;
                tokio::task::yield_now().await;
            }
        }
    }
}

/// Read the current list of `v` values for `key` in `table`, reconnecting on a
/// wire error (stale leader during churn) within a bounded budget.
async fn read_list_with_reconnect(
    router: &mut RangeRouter,
    c: &MultiRangeCluster,
    table: &str,
    key: i64,
) -> Vec<i64> {
    scan_col_i64(router, c, &format!("SELECT v FROM {table} WHERE k = {key}")).await
}

/// Append `v` to `table`'s list for `key` EXACTLY ONCE, even across faults.
///
/// The hazard: under a fault an INSERT may commit on the leader but the ack be lost
/// (the router then sees a wire error and reconnects). A blind retry would
/// double-append and break the strict-equality consistency check. So before each
/// (re)try we read the current list: if `v` is already present, the append already
/// landed and we stop; otherwise we attempt the INSERT. Values are unique per key
/// in this test, so presence of `v` is an exact "did it land" oracle.
async fn append_once(
    router: &mut RangeRouter,
    c: &MultiRangeCluster,
    table: &str,
    key: i64,
    v: i64,
) {
    let insert = format!("INSERT INTO {table}(k, v) VALUES ({key}, {v})");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        // Already landed (e.g. a prior try committed but lost its ack)?
        if read_list_with_reconnect(router, c, table, key)
            .await
            .contains(&v)
        {
            return;
        }
        match router.simple(&insert).await {
            Ok(_) => return,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "append {v} to `{table}` k={key} never succeeded (last error {}: {})",
                    e.code,
                    e.message
                );
                // Reconnect re-resolves each range's current leader via the
                // event-based `wait_for_leader`, so it blocks until leaders exist —
                // that is the pacing. `yield_now` (not a timed sleep) just cedes the
                // scheduler so a tight retry can't starve the runtime.
                *router = RangeRouter::connect(c).await;
                tokio::task::yield_now().await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killing_one_range_leader_does_not_stop_another_range() {
    let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
    for r in c.range_map().range_ids() {
        c.wait_for_leader(r).await;
    }
    let mut router = RangeRouter::connect(&c).await;
    run_with_reconnect(&mut router, &c, "CREATE TABLE a (id int4)").await; // id 1 -> range 0
    run_with_reconnect(&mut router, &c, "CREATE TABLE b (id int4)").await; // id 2 -> range 1

    // Kill range 1's leader. Only ONE node goes down, so:
    //  - range 0 keeps a quorum (2/3) and MUST keep serving — whether it was co-led
    //    by that node (it re-elects in its own group and still commits) or led
    //    elsewhere (untouched). Either way "killing range 1's leader does not STOP
    //    range 0" holds.
    //  - range 1 re-elects in its OWN group, independently.
    // Fully deterministic: no pre-arranged distinct leaders, no sleeps.
    let l1 = c.wait_for_leader(1).await;
    c.pause(l1);

    // Range 0 stays available through range 1's leader fault (reconnect-retry routes
    // to range 0's current leader, re-electing first if it happened to be co-led).
    let mut r0 = RangeRouter::connect(&c).await;
    run_with_reconnect(&mut r0, &c, "INSERT INTO a VALUES (1)").await;
    assert_eq!(
        scan_col_i64(&mut r0, &c, "SELECT id FROM a").await,
        vec![1],
        "range 0 stayed available through range 1's leader fault"
    );

    // Range 1 re-elects away from the paused node (its own group recovers). Probe
    // with `wait_for_leader_excluding`: the paused ex-leader still reports itself
    // `Leader` in its own frozen metrics, so a naive read could return it.
    let new_l1 = c.wait_for_leader_excluding(1, l1).await;
    assert_ne!(
        new_l1, l1,
        "range 1 re-elected away from the paused node {l1}"
    );

    // Heal; range 1 serves writes again.
    c.heal();
    let mut r1 = RangeRouter::connect(&c).await;
    run_with_reconnect(&mut r1, &c, "INSERT INTO b VALUES (2)").await;
    assert_eq!(
        scan_col_i64(&mut r1, &c, "SELECT id FROM b").await,
        vec![2],
        "range 1 resumed and serves writes after heal"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sharded_list_append_is_per_range_consistent_under_follower_faults() {
    let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
    for r in c.range_map().range_ids() {
        c.wait_for_leader(r).await;
    }
    let mut setup = RangeRouter::connect(&c).await;
    run_with_reconnect(&mut setup, &c, "CREATE TABLE la0 (k int8, v int8)").await; // range 0
    run_with_reconnect(&mut setup, &c, "CREATE TABLE la1 (k int8, v int8)").await; // range 1

    // Per-range list-append under follower faults — fully deterministic, no sleeps.
    // Each round pauses a node that leads NEITHER range, so both ranges keep quorum
    // (leader + one follower) and every append commits THROUGH the fault. We append
    // the round's value to both ranges and assert read-your-writes while that node
    // is down, then resume it and wait (event-based) for it to re-apply up to each
    // range's leader before the next round. Faults are interleaved with real
    // workload progress, never paced by a clock. (Leader *changes* are covered by
    // `killing_one_range_leader_does_not_stop_another_range`; this isolates the
    // follower-fault path so it is reproducible.)
    const APPENDS: i64 = 6;
    let mut r0 = RangeRouter::connect(&c).await;
    let mut r1 = RangeRouter::connect(&c).await;
    let mut exp0 = Vec::new();
    let mut exp1 = Vec::new();
    for v in 1..=APPENDS {
        // With 3 nodes and at most 2 distinct range leaders, a node leading neither
        // range always exists; pausing it keeps both ranges available.
        let l0 = c.wait_for_leader(0).await;
        let l1 = c.wait_for_leader(1).await;
        let victim = (0..c.n())
            .find(|&n| n != l0 && n != l1)
            .expect("a node leading neither range");
        c.pause(victim);

        append_once(&mut r0, &c, "la0", 1, v).await;
        exp0.push(v);
        assert_eq!(
            read_list_with_reconnect(&mut r0, &c, "la0", 1).await,
            exp0,
            "la0 (range 0) read-your-writes through a follower fault"
        );
        append_once(&mut r1, &c, "la1", 2, v).await;
        exp1.push(v);
        assert_eq!(
            read_list_with_reconnect(&mut r1, &c, "la1", 2).await,
            exp1,
            "la1 (range 1) read-your-writes through a follower fault"
        );

        c.resume(victim);
        c.wait_for_replication(0).await;
        c.wait_for_replication(1).await;
    }

    // Final cross-check: each table holds exactly its own appends, nothing leaked.
    let all: Vec<i64> = (1..=APPENDS).collect();
    let mut check = RangeRouter::connect(&c).await;
    assert_eq!(
        read_list_with_reconnect(&mut check, &c, "la0", 1).await,
        all,
        "la0 (range 0) final list reflects all appends and nothing else"
    );
    assert_eq!(
        read_list_with_reconnect(&mut check, &c, "la1", 2).await,
        all,
        "la1 (range 1) final list reflects all appends and nothing else"
    );
}
