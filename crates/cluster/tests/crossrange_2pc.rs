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

/// Diagnostic (Task-1 probe): dump table `t`'s row versions on `range`'s leader
/// store, each version's `(xmin, xmax)` and the clog status of `xmin` on this
/// range, plus — when `xmin`'s local clog is `Prepared(-> g)` — the GLOBAL clog
/// status of `g` on range 0. This is exactly the `clog[local][Li] -> clog[global][g]`
/// chain `exec::global_status` resolves a cross-range row through, so the dump shows
/// which clog state a leaked half carries. Returns a printable multi-line string.
#[allow(dead_code)]
async fn dump_versions(c: &MultiRangeCluster, range: u32, table_id: u32) -> String {
    use std::fmt::Write as _;
    let leader = c.wait_for_leader(range).await;
    let local = c.sm_kv(range, leader);
    let global = c.sm_kv(0, c.wait_for_leader(0).await);
    let mut out = String::new();
    let prefix = kv::key::table_prefix(table_id);
    let mut end = prefix.clone();
    *end.last_mut().expect("non-empty table prefix") += 1;
    for (k, v) in local.scan_range(&prefix, &end).expect("scan versions") {
        let (xmin, xmax, row) = mvcc::version::decode_tuple(&v).expect("decode tuple");
        let local_status = mvcc::clog::get(local.as_ref(), xmin).expect("clog get");
        let resolved = match local_status {
            mvcc::clog::XidStatus::Prepared(g) => format!(
                "Prepared(-> {g}) ; global clog[{g}] = {:?}",
                mvcc::clog::get(global.as_ref(), g).expect("global clog get")
            ),
            other => format!("{other:?}"),
        };
        let _ = writeln!(
            out,
            "  key={k:?} xmin={xmin} xmax={xmax} row={row:?} local_clog[{xmin}]={resolved}"
        );
    }
    out
}

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

/// Bounded-retry authoritative read: connect a FRESH router per attempt and run
/// `sql`, returning column 0 as `i32`s on the first success. A transient `40001 not
/// the leader` during post-failover re-election is retried (re-resolving a live
/// leader each attempt) under a deadline, so a stuck cluster fails the test instead
/// of hanging — and the read never spuriously panics on an election race. No sleep:
/// each attempt awaits a self-confirmed leader on both ranges first.
async fn read_i32_until_ok(c: &MultiRangeCluster, sql: &str) -> Vec<i32> {
    use pgwire::engine::QueryResult;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        let mut fresh = RangeRouter::connect(c).await;
        match fresh.simple(sql).await {
            Ok(QueryResult::Rows { rows, .. }) => {
                return rows
                    .iter()
                    .map(|r| {
                        std::str::from_utf8(&r[0].as_ref().expect("non-null").text)
                            .expect("utf8")
                            .parse()
                            .expect("i32")
                    })
                    .collect();
            }
            Ok(other) => panic!("expected Rows, got {other:?}"),
            Err(e) if tokio::time::Instant::now() < deadline => {
                // Transient election race (e.g. 40001 not-the-leader); re-resolve+retry.
                let _ = e;
            }
            Err(e) => panic!("authoritative read failed within deadline: {e:?}"),
        }
    }
}

/// Await — bounded, on the real condition (no sleep) — that EVERY range-0 replica's
/// applied store records a TERMINAL (`Committed`/`Aborted`) global decision for each
/// `g` in `gs`. A just-elected range-0 leader can self-report as leader while still
/// lagging on apply; this guarantees the cross-range resolver sees a fully-settled
/// global clog no matter which range-0 node a reader lands on, removing the apply-lag
/// race from the abort-atomicity assertion. Polls the applied stores directly (a
/// bounded cadence over an in-process condition, per the cross-process-harness rule).
async fn await_global_settled(c: &MultiRangeCluster, gs: &[u64]) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let all_settled = (0..c.n()).all(|node| {
            let kv = c.sm_kv(0, node);
            gs.iter().all(|&g| {
                matches!(
                    mvcc::clog::get(kv.as_ref(), g),
                    Ok(mvcc::clog::XidStatus::Committed | mvcc::clog::XidStatus::Aborted)
                )
            })
        });
        if all_settled {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "range-0 global decisions {gs:?} did not settle on every replica within the bound"
        );
        // Yield to the runtime so Raft apply makes progress; bounded poll cadence.
        tokio::task::yield_now().await;
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

/// The global decision is WRITE-ONCE and `commit_global_decision` returns the
/// EFFECTIVE decision via read-back: a first `Aborted` decision wins over a
/// later contending `Committed`, and the second call observes the `Aborted` that
/// was actually recorded (not the `Committed` it asked for). This is the
/// abort-race serialization a stranded participant relies on in later tasks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_decision_is_write_once_and_returns_effective() {
    use mvcc::clog::XidStatus;
    let c = two_range_cluster().await; // existing helper in crossrange_2pc.rs
    let e = c.leader_engine(0).await; // GTM-bearing range-0 engine (pub)
    let g = e.begin_global_durable().await.expect("alloc g");
    assert_eq!(
        e.commit_global_decision(g, XidStatus::Aborted)
            .await
            .expect("decide"),
        XidStatus::Aborted
    );
    assert_eq!(
        e.commit_global_decision(g, XidStatus::Committed)
            .await
            .expect("decide"),
        XidStatus::Aborted,
        "first terminal decision wins; commit_global_decision returns the effective decision"
    );
}

/// SP24 / Task 1 (RED): a cross-range txn whose global `g` is ABORTED leaves NO
/// participant half visible — even across a participant-leader FAILOVER that forces
/// the participant to RE-STAGE on its new leader. A visible `b` half is the
/// abort-atomicity leak (money created/destroyed).
///
/// We drive the participant directly through range-1 leader engines (not the router
/// COMMIT, which would block on the paused old leader) so the failover re-stage is
/// deterministic and the leaked clog/version state is inspectable. This mirrors what
/// the multi-process nemesis does: an in-flight transfer's first attempt stages
/// `acct_b` under `g`, the range-1 leader is killed, the coordinator/worker RETRIES
/// the whole transfer under a fresh `g'` (re-staging `b` on the new leader), and the
/// recovery abort-race aborts the ORIGINAL `g` — yet the second, `g'`-fenced version
/// survives.
///
/// MECHANISM (Step-3 report, from the captured version dump on the range-1 leader):
/// row `b` carries THREE versions —
///   - `xmin=1, xmax=3, row=20, clog[1]=Committed`             (the seed, SUPERSEDED by
///                                                              the re-stage xid 3),
///   - `xmin=2, xmax=0, row=21, clog[2]=Prepared(-> g)`,  `global clog[g]=Aborted`,
///   - `xmin=3, xmax=0, row=21, clog[3]=Prepared(-> g')`, `global clog[g']=Committed`.
/// The visible answer is `xmin=3` (b=21): it is the SECOND staged version, minted by
/// the failover RE-STAGE at `crates/executor/src/session.rs:558-560` (the
/// `Prepared(xid -> g)` stamp in `run_write`) under a FRESH global xid `g'`. The
/// retry's COMMIT wrote `clog[global][g'] = Committed`, so `exec::global_status`
/// (`crates/executor/src/exec.rs:336-341`) resolves `Prepared(3 -> g')` to `Committed`
/// → VISIBLE; and because that re-stage set the seed's `xmax=3`, the seed is hidden
/// too. The ORIGINAL transfer's decision `clog[global][g] = Aborted` (the recovery
/// abort-race → `commit_global_decision`, `crates/executor/src/lib.rs:242-262`)
/// governs ONLY the first version (`xmin=2, Prepared(-> g)`), which is correctly
/// invisible. So the leak is NOT a mis-resolution of the aborted `g` — that arm is
/// correct — it is the **fresh-`g'` re-escalation of an already-staged participant**:
/// the router/coordinator mints a SECOND cross-range version under a NEW, independent
/// global decision instead of fencing the re-stage to the original `g`, so an abort of
/// `g` does not cover the surviving `g'` half. (Escalation allocates a fresh `g` per
/// escalation: `crates/cluster/src/range/router.rs:388`/`:415`; the participant
/// re-stage is not fenced to a prior `(g, range)` stage on a NEW global xid.)
/// FIX DIRECTION (Task 2): fence the participant re-stage so a row that already carries
/// `Prepared(-> g_old)` for an in-doubt cross-range txn cannot be re-staged under a
/// different `g'` (reuse `g_old` / resolve-then-supersede), so exactly one global
/// decision governs each row and an abort of that decision hides the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aborted_global_leaves_no_participant_half_visible_under_failover() {
    use mvcc::clog::XidStatus;
    use pgwire::engine::{Engine, Session};
    // 5 nodes so range 1 keeps a 3-node quorum when its leader is paused.
    let c = MultiRangeCluster::new(5, RangeMap::with_boundaries(vec![2])).await;
    for r in c.range_map().range_ids() {
        c.wait_for_leader(r).await;
    }
    // Schema + seeded committed rows (a in range 0, b in range 1), via a normal router.
    let mut admin = RangeRouter::connect(&c).await;
    admin.simple("CREATE TABLE a (id int4)").await.expect("a"); // id 1 -> range 0
    admin.simple("CREATE TABLE b (id int4)").await.expect("b"); // id 2 -> range 1
    admin
        .simple("INSERT INTO a VALUES (10)")
        .await
        .expect("seed a");
    admin
        .simple("INSERT INTO b VALUES (20)")
        .await
        .expect("seed b");
    drop(admin);
    let b_table_id = catalog::get_table(&*c.catalog_kv().await, "b")
        .expect("b table")
        .id;

    // ── Attempt 1: a cross-range transfer stages b under the ORIGINAL global g. ──
    let g = c
        .leader_engine(0)
        .await
        .begin_global_durable()
        .await
        .expect("begin g");
    {
        let mut s_b = c.leader_engine(1).await.connect();
        s_b.ensure_began().await.expect("begin b session");
        s_b.simple_query("UPDATE b SET id = 21 WHERE id = 20")
            .await
            .expect("stage b @ g");
        s_b.join_global(g).await.expect("join g"); // Prepared(Lb -> g) durable
        // The participant-range-1 leader is killed mid-transaction: the held session is
        // LOST (dropped here), its in-memory state gone — exactly as on a real crash.
    }

    // ── Failover: kill range 1's leader; a new leader rises with the durable
    //    Prepared(Lb -> g) marker in its log. ──
    let victim = c.wait_for_leader(1).await;
    c.pause(victim);
    c.wait_for_leader_excluding(1, victim).await;

    // ── Attempt 2 (the coordinator/worker RETRY): re-stage b on the NEW leader under a
    //    FRESH global g', then COMMIT g'. This is the whole-transfer retry the bank
    //    worker does on an indeterminate COMMIT. ──
    let g2 = c
        .leader_engine(0)
        .await
        .begin_global_durable()
        .await
        .expect("begin g'");
    {
        let mut s_b2 = c.leader_engine(1).await.connect();
        s_b2.ensure_began().await.expect("begin b session 2");
        s_b2.simple_query("UPDATE b SET id = 21 WHERE id = 20")
            .await
            .expect("re-stage b @ g'");
        s_b2.join_global(g2).await.expect("join g'"); // Prepared(Lb' -> g') durable
        // g' COMMITS (the retry succeeds), making the SECOND version eligible.
        assert_eq!(
            c.leader_engine(0)
                .await
                .commit_global_decision(g2, XidStatus::Committed)
                .await
                .expect("commit g'"),
            XidStatus::Committed
        );
        s_b2.commit_release();
    }

    // ── The ORIGINAL transfer is globally ABORTED by the recovery abort-race. ──
    assert_eq!(
        c.leader_engine(0)
            .await
            .commit_global_decision(g, XidStatus::Aborted)
            .await
            .expect("abort-race g"),
        XidStatus::Aborted
    );
    c.resume(victim);

    // DETERMINISM (no sleep): the global decisions (Aborted(g), Committed(g')) were
    // Raft-committed via range 0's leader, but a freshly-elected range-0 leader can lag
    // on apply and still self-report as leader — so a reader resolving the global clog
    // against it would transiently see g/g' as in-doubt and the leak would hide by luck.
    // Await — on a real condition, bounded — that EVERY range-0 replica has applied both
    // terminal decisions, so the authoritative read resolves a fully-settled global clog
    // regardless of which range-0 leader it lands on. Likewise await the re-staged g'
    // version on every range-1 replica.
    await_global_settled(&c, &[g, g2]).await;
    c.wait_for_replication(1).await;

    // The transfer that wrote b is globally ABORTED (g = Aborted). Abort atomicity
    // requires b to read its PRE-txn value (20). A `21` is the leaked, g'-fenced half.
    let dump_b = dump_versions(&c, 1, b_table_id).await;
    let got = read_i32_until_ok(&c, "SELECT id FROM b").await;
    assert_eq!(
        got,
        vec![20],
        "b half leaked (abort-atomicity): the aborted transfer's b stayed visible.\nrange-1 b versions:\n{dump_b}"
    );
}
