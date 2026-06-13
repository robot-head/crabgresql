//! D3a: SQL routes to the correct range; rows land only in that range's store;
//! per-range failover is independent and per-range list-append stays consistent
//! under follower faults.
use cluster::range::{MultiRangeCluster, RangeMap, RangeRouter};
use pgwire::engine::QueryResult;
use std::time::Duration;

/// Run a single-column `int8` SELECT through the public router API and collect the
/// values in row order. Integration tests can't reach the router's `#[cfg(test)]`
/// `scan_one_i32`, so this mirrors it over only the public `QueryResult`/`Cell`
/// surface (`Cell::text` is the text-format encoding of the value).
async fn scan_col_i64(router: &mut RangeRouter, sql: &str) -> Vec<i64> {
    match router.simple(sql).await.expect("select ok") {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|r| {
                let cell = r[0].as_ref().expect("non-null cell");
                std::str::from_utf8(&cell.text)
                    .expect("utf8 text")
                    .parse::<i64>()
                    .expect("i64 value")
            })
            .collect(),
        other => panic!("expected Rows, got {other:?}"),
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
    // node, and never appear in range 0's store. Only the leader is guaranteed
    // applied when INSERT returns; followers apply asynchronously, so poll
    // (bounded) for the rows to land in range 1 before asserting. The range-0
    // absence check is NOT a timing race (range 0 never receives b's rows) so it
    // is asserted immediately — that is the routing-correctness assertion.
    use kv::key::table_prefix;
    let b_prefix = table_prefix(2);
    for node in 0..c.n() {
        let r1 = c.sm_kv(1, node);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if !r1.scan_prefix(&b_prefix).expect("scan r1").is_empty() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "b's rows must replicate to range 1 on node {node} within 5s"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
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
                *router = RangeRouter::connect(c).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Read the current list of `v` values for `key` in `table`, reconnecting on any
/// wire error (stale-leader during churn) within a bounded budget.
async fn read_list_with_reconnect(
    router: &mut RangeRouter,
    c: &MultiRangeCluster,
    table: &str,
    key: i64,
) -> Vec<i64> {
    let sql = format!("SELECT v FROM {table} WHERE k = {key}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        match router.simple(&sql).await {
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
                    "read of `{table}` k={key} never succeeded (last error {}: {})",
                    e.code,
                    e.message
                );
                *router = RangeRouter::connect(c).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
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
                *router = RangeRouter::connect(c).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killing_one_range_leader_does_not_stop_another_range() {
    let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
    let mut l0 = c.wait_for_leader(0).await;
    let mut l1 = c.wait_for_leader(1).await;

    // We need range 0 and range 1 led by DIFFERENT physical nodes so that pausing
    // range 1's leader leaves range 0's leader up. Elections are independent, so
    // nudging range 1 (pause+resume its leader) quickly lands on a distinct node.
    // After each nudge we re-read BOTH leaders: the nudge can also move range 0
    // (pausing l1 when it transiently co-led, or background churn), so we must not
    // trust a stale l0. A ~700ms stable window after resume lets both ranges settle
    // before we re-check (D5 stable-window lesson).
    let mut tries = 0;
    while l1 == l0 {
        c.pause(l1);
        tokio::time::sleep(Duration::from_millis(400)).await;
        c.resume(l1);
        tokio::time::sleep(Duration::from_millis(700)).await;
        l0 = c.wait_for_leader(0).await;
        l1 = c.wait_for_leader(1).await;
        tries += 1;
        assert!(
            tries < 40,
            "could not get distinct leaders for ranges 0 and 1 (l0={l0}, l1={l1})"
        );
    }

    let mut router = RangeRouter::connect(&c).await;
    run_with_reconnect(&mut router, &c, "CREATE TABLE a (id int4)").await; // id 1 -> range 0
    run_with_reconnect(&mut router, &c, "CREATE TABLE b (id int4)").await; // id 2 -> range 1

    // Pause range 1's leader node. Range 0 is led by l0 (a different, still-up node)
    // so it MUST keep serving; range 1 must re-elect away from the paused node.
    assert_ne!(l0, l1, "precondition: distinct leaders before the fault");
    c.pause(l1);

    // Range 0 keeps serving: an INSERT into a range-0 table commits. The router's
    // cached engines may be stale (l1 was a leader for range 1); reconnecting routes
    // range 0 to l0, which is unaffected. Bounded reconnect-retry covers any
    // transient no-leader window.
    let mut r0 = RangeRouter::connect(&c).await;
    run_with_reconnect(&mut r0, &c, "INSERT INTO a VALUES (1)").await;
    assert_eq!(
        scan_col_i64(&mut r0, "SELECT id FROM a").await,
        vec![1],
        "range 0 stayed available through range 1's leader fault"
    );

    // Range 1 re-elects among the survivors, away from the paused node. We must
    // probe with `wait_for_leader_excluding`: the paused ex-leader still reports
    // itself `Leader` in its own metrics (pausing only drops its RPCs), so the naive
    // scan would return the stale paused node.
    let new_l1 = c.wait_for_leader_excluding(1, l1).await;
    assert_ne!(
        new_l1, l1,
        "range 1 re-elected away from the paused node {l1}"
    );

    // Heal; range 1 resumes — an INSERT into a range-1 table commits after the
    // router reconnects to the (possibly new) range-1 leader.
    c.heal();
    let mut r1 = RangeRouter::connect(&c).await;
    run_with_reconnect(&mut r1, &c, "INSERT INTO b VALUES (2)").await;
    assert_eq!(
        scan_col_i64(&mut r1, "SELECT id FROM b").await,
        vec![2],
        "range 1 resumed and serves writes after heal"
    );
}

/// One table's list-append workload: append 1..=`appends`, and after EACH append
/// read the whole list back and assert it reflects ALL prior appends in order
/// (per-table linearizable / read-your-writes). A short pace between cycles makes
/// the workload SPAN multiple nemesis rounds — including the windows in which a node
/// is paused — so the consistency property is exercised concurrently with faults,
/// not vacuously before them. Append + read each reconnect-and-retry on transient
/// wire errors (stale leader during churn); appends land exactly-once.
async fn list_append_workload(
    c: &MultiRangeCluster,
    table: &str,
    key: i64,
    appends: i64,
    pace: Duration,
) {
    let mut r = RangeRouter::connect(c).await;
    let mut expected = Vec::new();
    for v in 1..=appends {
        append_once(&mut r, c, table, key, v).await;
        expected.push(v);
        let got = read_list_with_reconnect(&mut r, c, table, key).await;
        assert_eq!(
            got, expected,
            "`{table}` read must reflect all prior appends (key {key})"
        );
        tokio::time::sleep(pace).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sharded_list_append_is_per_range_consistent_under_follower_faults() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let c = Arc::new(MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await);
    for r in c.range_map().range_ids() {
        c.wait_for_leader(r).await;
    }
    let mut setup = RangeRouter::connect(&c).await;
    run_with_reconnect(&mut setup, &c, "CREATE TABLE la0 (k int8, v int8)").await; // range 0
    run_with_reconnect(&mut setup, &c, "CREATE TABLE la1 (k int8, v int8)").await; // range 1

    // A follower-fault nemesis: pause a node, hold it briefly, resume, then leave a
    // 1s STABLE WINDOW so the workload's gated reads + writes can complete before the
    // next fault (D5 lesson — zero-gap faults starve the workload). Faults are
    // node-scoped; pausing node 1 or 2 usually hits a follower of both ranges, but if
    // it hits a leader the appends' reconnect-retry absorbs the leader change. The
    // nemesis loops until the workload signals done (bounded), so faults overlap the
    // *entire* workload rather than running before or after it.
    let done = Arc::new(AtomicBool::new(false));
    let nemesis = tokio::spawn({
        let sb = c.switchboard().clone();
        let done = Arc::clone(&done);
        async move {
            let mut round = 0u64;
            while !done.load(Ordering::Relaxed) {
                let victim = 1 + (round % 2); // alternate followers 1 and 2
                sb.pause(victim);
                tokio::time::sleep(Duration::from_millis(150)).await;
                sb.resume(victim);
                tokio::time::sleep(Duration::from_secs(1)).await; // stable window
                round += 1;
            }
            // Leave the cluster clean for the post-workload cross-check.
            sb.heal();
        }
    });

    // Drive BOTH ranges' workloads CONCURRENTLY so range-0 and range-1 traffic is
    // truly simultaneous (per-range independence under load), each paced (~250ms/
    // cycle) so the ~6 cycles span several nemesis rounds and overlap paused windows.
    let w0 = tokio::spawn({
        let c = Arc::clone(&c);
        async move { list_append_workload(&c, "la0", 1, 6, Duration::from_millis(250)).await }
    });
    let w1 = tokio::spawn({
        let c = Arc::clone(&c);
        async move { list_append_workload(&c, "la1", 2, 6, Duration::from_millis(250)).await }
    });
    // Propagate workload panics (the in-workload asserts are the real property).
    w0.await.expect("la0 workload");
    w1.await.expect("la1 workload");

    done.store(true, Ordering::Relaxed);
    nemesis.await.ok();

    // Final cross-check after faults are healed: each table holds exactly its own
    // appends, in order, and nothing leaked across ranges.
    let mut check = RangeRouter::connect(&c).await;
    assert_eq!(
        read_list_with_reconnect(&mut check, &c, "la0", 1).await,
        vec![1, 2, 3, 4, 5, 6],
        "la0 (range 0) final list reflects all appends and nothing else"
    );
    assert_eq!(
        read_list_with_reconnect(&mut check, &c, "la1", 2).await,
        vec![1, 2, 3, 4, 5, 6],
        "la1 (range 1) final list reflects all appends and nothing else"
    );
}
