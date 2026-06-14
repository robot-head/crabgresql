//! Cross-range 2PC correctness proofs (SP16 / D3c, Task 5).
//!
//! These exercise the parts of the cross-range two-phase-commit path that the
//! INSERT-only router tests in `range::router` do NOT reach. Every cross-range
//! transaction is driven through a [`RangeRouter`] over a [`MultiRangeCluster`] —
//! the only path that carries the 2PC coordinator (range 0's GTM) — and every
//! wait is openraft's event-based `wait_for_leader` (no `sleep`; see CLAUDE.md).
//!
//! 1. **Cross-range UPDATE** (`cross_range_update_commits_atomically`): a single
//!    cross-range txn UPDATEs an existing row in a table in range 0 AND an existing
//!    row in a table in range 1, commits, and both updated values read back through
//!    a fresh router. UPDATE re-fetches each target row under the MVCC visibility
//!    re-check (`find_visible_one`) and runs the concurrent-change serialization
//!    check (`eval_plan_qual`) — resolver sites 2 and 3, which the INSERT-only
//!    commit/rollback tests never hit. A miss there would silently mis-classify a
//!    `Prepared(-> g)` row and either lose the update or read a stale value.
//!
//! 2. **Cross-range UPDATE rollback** (`cross_range_update_rolls_back_atomically`):
//!    the same shape rolled back leaves BOTH rows at their pre-txn values, proving
//!    the positive `Aborted(g)` keeps the staged updates invisible on both ranges.

use std::collections::BTreeSet;

use cluster::range::{MultiRangeCluster, RangeMap, RangeRouter};

/// Run `sql` and return column 0 of every row parsed as `i32`, in row order.
async fn scan_i32(router: &mut RangeRouter, sql: &str) -> Vec<i32> {
    use pgwire::engine::QueryResult;
    match router.simple(sql).await.expect("query ok") {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|r| {
                let cell = r[0].as_ref().expect("non-null");
                std::str::from_utf8(&cell.text)
                    .expect("utf8")
                    .parse()
                    .expect("i32")
            })
            .collect(),
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// A 3-node cluster with a range boundary at table id 2, so the first user table
/// (id 1) lands in range 0 and the second (id 2) lands in range 1 — the minimal
/// two-range topology the cross-range escalation needs. Every range's leader is
/// awaited (event-based) before the router connects.
async fn two_range_cluster() -> MultiRangeCluster {
    let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
    for r in c.range_map().range_ids() {
        c.wait_for_leader(r).await;
    }
    c
}

/// A cross-range transaction that UPDATEs a pre-existing row in a table in range 0
/// and a pre-existing row in a table in range 1 commits atomically; both updated
/// values read back through a fresh router. This drives the resolver through
/// `find_visible_one`/`eval_plan_qual` (the UPDATE row re-fetch + concurrent-change
/// check), which INSERT-only cross-range tests never exercise.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_range_update_commits_atomically() {
    let c = two_range_cluster().await;
    let mut router = RangeRouter::connect(&c).await;

    // Two tables in two ranges, each seeded with one row in its OWN single-range
    // (autocommit) transaction so the seeded rows are plain committed local rows.
    router.simple("CREATE TABLE a (id int4)").await.expect("a"); // id 1 -> range 0
    router.simple("CREATE TABLE b (id int4)").await.expect("b"); // id 2 -> range 1
    router
        .simple("INSERT INTO a VALUES (10)")
        .await
        .expect("seed a");
    router
        .simple("INSERT INTO b VALUES (20)")
        .await
        .expect("seed b");

    // One cross-range txn that UPDATEs a row in each range. The first UPDATE pins
    // range 0; the second escalates to a global txn `g`; COMMIT writes the single
    // Committed(g) decision both ranges flip at.
    router.simple("BEGIN").await.expect("begin");
    router
        .simple("UPDATE a SET id = 11 WHERE id = 10")
        .await
        .expect("update a pins range 0");
    router
        .simple("UPDATE b SET id = 21 WHERE id = 20")
        .await
        .expect("update b escalates to 2PC");

    // Read-your-writes within the txn: each range's own-xid short-circuit shows the
    // updated value before COMMIT.
    assert_eq!(scan_i32(&mut router, "SELECT id FROM a").await, vec![11]);
    assert_eq!(scan_i32(&mut router, "SELECT id FROM b").await, vec![21]);

    router.simple("COMMIT").await.expect("commit");

    // Both updated values are visible to the committing router after COMMIT.
    assert_eq!(scan_i32(&mut router, "SELECT id FROM a").await, vec![11]);
    assert_eq!(scan_i32(&mut router, "SELECT id FROM b").await, vec![21]);

    // And to a fresh router resolving each Prepared(-> g) row through range 0's
    // global clog — the all-or-nothing flip is durable, not session-local.
    let mut fresh = RangeRouter::connect(&c).await;
    assert_eq!(scan_i32(&mut fresh, "SELECT id FROM a").await, vec![11]);
    assert_eq!(scan_i32(&mut fresh, "SELECT id FROM b").await, vec![21]);
}

/// The ROLLBACK sibling of the UPDATE test: a cross-range txn that UPDATEs a row in
/// each range and then ROLLs BACK leaves BOTH rows at their original values, to the
/// same router and a fresh one. The positive `Aborted(g)` keeps both staged updates
/// invisible (the old row version stays current on both ranges).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_range_update_rolls_back_atomically() {
    let c = two_range_cluster().await;
    let mut router = RangeRouter::connect(&c).await;

    router.simple("CREATE TABLE a (id int4)").await.expect("a"); // id 1 -> range 0
    router.simple("CREATE TABLE b (id int4)").await.expect("b"); // id 2 -> range 1
    router
        .simple("INSERT INTO a VALUES (10)")
        .await
        .expect("seed a");
    router
        .simple("INSERT INTO b VALUES (20)")
        .await
        .expect("seed b");

    router.simple("BEGIN").await.expect("begin");
    router
        .simple("UPDATE a SET id = 11 WHERE id = 10")
        .await
        .expect("update a pins range 0");
    router
        .simple("UPDATE b SET id = 21 WHERE id = 20")
        .await
        .expect("update b escalates to 2PC");
    router.simple("ROLLBACK").await.expect("rollback");

    // Both rows keep their pre-txn values, to the committing router and a fresh one.
    assert_eq!(scan_i32(&mut router, "SELECT id FROM a").await, vec![10]);
    assert_eq!(scan_i32(&mut router, "SELECT id FROM b").await, vec![20]);
    let mut fresh = RangeRouter::connect(&c).await;
    assert_eq!(scan_i32(&mut fresh, "SELECT id FROM a").await, vec![10]);
    assert_eq!(scan_i32(&mut fresh, "SELECT id FROM b").await, vec![20]);
}

/// A cross-range txn that touches THREE ranges (a third table in range 2) commits
/// all-or-nothing too: the escalation grows `Pin::Global`'s participant set as each
/// new range is touched, and the single Committed(g) decision flips all three at
/// once. Proves the participant `BTreeSet` is not hard-wired to two ranges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_range_transaction_commits_atomically() {
    // Boundaries at 2 and 3: a(id 1)->range 0, b(id 2)->range 1, d(id 3)->range 2.
    let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2, 3])).await;
    let touched: BTreeSet<_> = c.range_map().range_ids().collect();
    assert_eq!(touched, BTreeSet::from([0, 1, 2]), "three ranges");
    for r in c.range_map().range_ids() {
        c.wait_for_leader(r).await;
    }
    let mut router = RangeRouter::connect(&c).await;
    router.simple("CREATE TABLE a (id int4)").await.expect("a");
    router.simple("CREATE TABLE b (id int4)").await.expect("b");
    router.simple("CREATE TABLE d (id int4)").await.expect("d");

    router.simple("BEGIN").await.expect("begin");
    router.simple("INSERT INTO a VALUES (1)").await.expect("a");
    router.simple("INSERT INTO b VALUES (2)").await.expect("b");
    router.simple("INSERT INTO d VALUES (3)").await.expect("d");
    router.simple("COMMIT").await.expect("commit");

    let mut fresh = RangeRouter::connect(&c).await;
    assert_eq!(scan_i32(&mut fresh, "SELECT id FROM a").await, vec![1]);
    assert_eq!(scan_i32(&mut fresh, "SELECT id FROM b").await, vec![2]);
    assert_eq!(scan_i32(&mut fresh, "SELECT id FROM d").await, vec![3]);
}
