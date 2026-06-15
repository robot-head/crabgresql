//! SP23: cross-range 2PC conserves the bank total across a range-0 leadership change,
//! exercised as a TRUE single-failover STABLE-WINDOW nemesis. Each round drives a batch of
//! cross-range transfers to completion (fully settled), QUIESCES, then kills the range-0
//! leader (the GTM/coordinator home AND `acct_a`'s participant) with NO transfer in flight,
//! waits for recovery, and drives a FRESH batch. Because the kill lands on a quiescent
//! cluster, this isolates single-failover GTM-reseed recovery (the in-scope case) from the
//! deferred cascading/overlapping mid-2PC failover case (a spec non-goal — partition + mid-
//! flight coverage lives in `jepsen_bank` / `multiprocess`). This is the EMPIRICAL
//! end-to-end complement to the SP23 reseed-before-allocate fix: it exercises the real
//! 5-process system (TCP + durable Raft + 2PC + the range-0 rise sweep that reseeds the GTM
//! before reopening the gate) and asserts money is conserved across each range-0 failover.
//!
//! The DETERMINISTIC teeth for the fix live in the Stateright model
//! `cluster::crossrange_2pc_gtm_reuse_model` (its `no_reseed_*` teeth test catches the broken
//! variant). This e2e cannot carry those teeth: the apply-lag that triggers a reused global
//! xid only opens when `BeginGlobal` is served WHILE a prior commit is still applying on a
//! fresh leader — and forcing that empirically requires in-flight 2PC across the kill, which
//! is the deferred cascading/overlapping-failover case. Quiescing to stay in-scope (and
//! converge) necessarily closes that window, so this test is a CONVERGENCE / conservation
//! regression guard, not a teeth test. (Confirmed: with the rise-sweep reseed disabled, a
//! quiesced run still passes on a fast host — the model is what catches that regression.)
mod harness;
use harness::Cluster;
use std::time::Duration;

/// SP23 Task 5: change the RANGE-0 leader each round on a QUIESCENT cluster and assert the
/// cross-range bank total is conserved. Range 0 is the GTM/coordinator home AND `acct_a`'s
/// participant. Each round: (1) commit a pre-kill batch (allocates global xids), (2) quiesce
/// barrier, (3) hard-kill+respawn the range-0 leader (forces a leadership RISE with zero
/// in-flight 2PC), (4) wait for recovery + prove range 0 serves `BeginGlobal` again, (5)
/// commit a post-recovery batch — the FIRST allocations after the failover, which MUST read
/// a reseeded GTM. With the reseed-before-allocate rise sweep (apply-wait -> reseed_gtm ->
/// settle -> mark_served) a reused global xid never duplicates a live version, so the total
/// is conserved. Non-vacuity is structural: every driven transfer is committed or the run
/// panics, so a passing run provably moved money across the failover.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range0_participant_leader_kill_conserves_total() {
    const ACCOUNTS: i64 = 4;
    const SEED: i64 = 100;
    const ROUNDS: usize = 4;
    const BATCH: usize = 3;
    let seeded_total = 2 * ACCOUNTS * SEED; // two tables, two ranges

    // 5 nodes, boundary [2] (5 nodes keep a quorum when one node is faulted): acct_a
    // (table id 1) -> range 0, acct_b (id 2) -> range 1.
    let mut c = Cluster::spawn_multirange(5, vec![2]).await;

    // Seed via `exec_until_ok` (bounded retry across nodes): right after bring-up the
    // gateway we land on may not yet have a quorum view, so an unretried first DDL can hit a
    // transient `08006 no-quorum`. Sequential `exec_until_ok` calls each commit before the
    // next begins, so acct_a (id 1 -> range 0) is still created before acct_b (id 2 ->
    // range 1). Setup is fault-free, so a retried statement was cleanly rejected, never
    // double-applied.
    c.exec_until_ok("CREATE TABLE acct_a (id int8, bal int8)")
        .await;
    c.exec_until_ok("CREATE TABLE acct_b (id int8, bal int8)")
        .await;
    for id in 0..ACCOUNTS {
        c.exec_until_ok(&format!("INSERT INTO acct_a VALUES ({id}, {SEED})"))
            .await;
        c.exec_until_ok(&format!("INSERT INTO acct_b VALUES ({id}, {SEED})"))
            .await;
    }

    // Deterministic-in-spirit transfer stream (no `rand`, no wall clock); rotates gateways
    // so coordinators are often NON-leading gateways (forwarded 2PC).
    let mut rng = Lcg::new(0x5DEE_CE66_D1A4_2B3C);
    let mut gw = 0u64;
    let mut total_committed = 0usize;

    for _round in 0..ROUNDS {
        // (1) PRE-KILL batch: commit BATCH cross-range transfers to completion, each
        // awaited and fully settled. Every transfer escalates to global 2PC, so begin_global
        // advances the GTM `next_global` to quorum — establishing the pre-failover
        // allocation that a broken reseed could later regress below and REUSE.
        for _ in 0..BATCH {
            let (from, to, amt) = pick(&mut rng, ACCOUNTS);
            commit_one_transfer(&c, gw, from, to, amt).await;
            total_committed += 1;
            gw = gw.wrapping_add(1);
        }

        // (2) QUIESCE barrier: a settle-aware zero-sum cross-range write confirms BOTH ranges
        // admit writes and nothing is in flight before the kill. Amount 0 conserves the total.
        c.exec_until_ok(
            "BEGIN; UPDATE acct_a SET bal = bal - 0 WHERE id = 0; UPDATE acct_b SET bal = bal + 0 WHERE id = 0; COMMIT",
        )
        .await;

        // (3) SINGLE-FAILOVER fault on the QUIESCENT cluster: hard-kill the range-0 leader
        // (GTM/coordinator/participant home) with NO transfer in flight, then respawn it.
        // This forces a range-0 leadership RISE with zero in-flight 2PC, isolating
        // single-failover GTM-reseed recovery from the deferred cascading (overlapping
        // mid-2PC) failover case.
        let victim = c.range_leader(0).await;
        c.kill(victim).await;
        c.respawn(victim);

        // (4) RECOVERY GATE: wait for a leader on BOTH ranges, then prove range 0 SERVES
        // `BeginGlobal` again with a cross-range barrier write. The barrier only commits once
        // the freshly-risen range-0 leader has finished its rise sweep (apply-wait ->
        // reseed_gtm -> settle -> mark_served) and reopened the gate — the multi-process
        // witness that recovery completed BEFORE we allocate fresh global xids.
        c.range_leader(0).await;
        c.range_leader(1).await;
        c.exec_until_ok(
            "BEGIN; UPDATE acct_a SET bal = bal - 0 WHERE id = 0; UPDATE acct_b SET bal = bal + 0 WHERE id = 0; COMMIT",
        )
        .await;

        // (5) POST-RECOVERY batch: the FIRST cross-range allocations after the range-0
        // failover. begin_global here MUST read a RESEEDED GTM counter; if reseed lagged the
        // applied store (the SP23 bug) it would reuse a pre-failover global xid ->
        // staged_local_for aliases a stale Prepared(->g) marker -> a duplicate live MVCC
        // version -> money created. With reseed-before-allocate there is no reuse, so these
        // commit and the total stays conserved.
        for _ in 0..BATCH {
            let (from, to, amt) = pick(&mut rng, ACCOUNTS);
            commit_one_transfer(&c, gw, from, to, amt).await;
            total_committed += 1;
            gw = gw.wrapping_add(1);
        }
    }

    // Wait for leaders on both ranges (the cluster is already healed — kill/respawn leaves
    // no partition state).
    c.range_leader(0).await;
    c.range_leader(1).await;

    // RECOVERY-REQUIRED check: a transfer touching EVERY account pair must commit within
    // bound — a coordinator-crash-stranded lock that recovery failed to free would wedge
    // this forever -> exec_until_ok panics at its deadline -> fail.
    for id in 0..ACCOUNTS {
        let other = (id + 1) % ACCOUNTS;
        c.exec_until_ok(&format!(
            "BEGIN; UPDATE acct_a SET bal = bal - 0 WHERE id = {id}; UPDATE acct_b SET bal = bal + 0 WHERE id = {other}; COMMIT"
        )).await; // amount 0: touches+locks both rows, conserves total, requires no funds
    }

    // CONSERVATION oracle: sum both tables across both ranges == seeded total. Bounded-retry
    // the authoritative read (re-resolve a live gateway per attempt) so a transient failure
    // under heavy CI contention does not panic — the PR #34 flake class.
    let total = read_total_cross_until_ok(&c, ACCOUNTS).await;
    assert_eq!(
        total, seeded_total,
        "cross-range transfers conserve the bank total across range-0 single-failover GTM reseed (got {total}, want {seeded_total})"
    );
    // NON-VACUITY (structural): every driven transfer is committed (or `commit_one_transfer`
    // panics), so a passing run provably moved money across each range-0 failover. The assert
    // documents the count and guards a future refactor that stops driving commits.
    assert_eq!(
        total_committed,
        ROUNDS * 2 * BATCH,
        "the workload must commit every driven transfer (non-vacuous): got {total_committed}"
    );
}

// ---------------------------------------------------------------------------
// Module-local helpers.
// ---------------------------------------------------------------------------

/// Pick a `(from, to, amt)` cross-range transfer (distinct accounts, amount in `1..=20`).
fn pick(rng: &mut Lcg, accounts: i64) -> (i64, i64, i64) {
    let from = (rng.next() % accounts as u64) as i64;
    let mut to = (rng.next() % accounts as u64) as i64;
    if to == from {
        to = (to + 1) % accounts;
    }
    let amt = 1 + (rng.next() % 20) as i64;
    (from, to, amt)
}

/// Drive ONE cross-range transfer to a definite COMMIT, bounded-retry across rotating
/// gateways. Called only inside QUIESCENT (non-faulted) or just-recovered windows, so it
/// commits quickly; the bounded retry only absorbs a transient `08006`/gate-settle right
/// after a failover (a CLEAN pre-work rejection — `cross_transfer` only returns `false`
/// without committing, never half-applies). Panics if it cannot commit within the deadline:
/// a quiescent, recovered cluster MUST accept a cross-range transfer, so that is a real
/// liveness failure, not a flake to swallow.
async fn commit_one_transfer(c: &Cluster, gw_seed: u64, from: i64, to: i64, amt: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut idx = (gw_seed % c.len() as u64) as usize;
    loop {
        if let Some(client) = connect(c.sql_addr(idx as u64)).await
            && cross_transfer(&client, from, to, amt).await
        {
            return;
        }
        idx = (idx + 1) % c.len();
        assert!(
            tokio::time::Instant::now() < deadline,
            "a quiescent recovered cluster must commit a cross-range transfer within 30s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await; // harness poll cadence, not a settle-sleep
    }
}

/// A tiny deterministic LCG so the workload is varied yet reproducible-in-spirit without
/// pulling in `rand` or depending on the wall clock. (Same shape as `multiprocess.rs`.)
struct Lcg(u64);
impl Lcg {
    fn new(seed: u64) -> Self {
        Lcg(seed ^ 0xDEAD_BEEF_CAFE_F00D)
    }
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
}

/// Resilient connect: the nemesis kills the very gateways we may connect to, so this returns
/// `None` on any connection failure or timeout — the caller rotates to the next gateway
/// rather than panicking. Bounded by a 10s timeout.
async fn connect(addr: &str) -> Option<tokio_postgres::Client> {
    let port = addr.rsplit(':').next()?;
    let cs = format!("host=127.0.0.1 port={port} user=postgres");
    match tokio::time::timeout(
        Duration::from_secs(10),
        tokio_postgres::connect(&cs, tokio_postgres::NoTls),
    )
    .await
    {
        Ok(Ok((client, conn))) => {
            tokio::spawn(conn);
            Some(client)
        }
        _ => None,
    }
}

/// Perform one CROSS-RANGE transfer transaction over `client`. Returns `true` iff it
/// committed. `acct_a` lives in range 0, `acct_b` in range 1, so the
/// `BEGIN; UPDATE acct_a; UPDATE acct_b; COMMIT` escalates to global 2PC.
///
/// Each statement is bounded by a 10s timeout (like `multiprocess::transfer`). On any
/// error/timeout the transfer is INDETERMINATE: issue a best-effort bounded `ROLLBACK`
/// (ignore its result) and return `false`. A transfer nets zero, so only definitely-
/// committed ones move money — and they conserve the total.
async fn cross_transfer(client: &tokio_postgres::Client, from: i64, to: i64, amt: i64) -> bool {
    async fn stmt(client: &tokio_postgres::Client, sql: &str) -> bool {
        matches!(
            tokio::time::timeout(Duration::from_secs(10), client.simple_query(sql)).await,
            Ok(Ok(_))
        )
    }
    async fn rollback(client: &tokio_postgres::Client) {
        let _ = tokio::time::timeout(Duration::from_secs(5), client.simple_query("ROLLBACK")).await;
    }

    if !stmt(client, "BEGIN").await {
        rollback(client).await;
        return false;
    }
    let upd1 = format!("UPDATE acct_a SET bal = bal - {amt} WHERE id = {from}");
    let upd2 = format!("UPDATE acct_b SET bal = bal + {amt} WHERE id = {to}");
    if !stmt(client, &upd1).await || !stmt(client, &upd2).await {
        rollback(client).await;
        return false;
    }
    if stmt(client, "COMMIT").await {
        true
    } else {
        rollback(client).await;
        false
    }
}

/// Parse column 0 of the first row of a `simple_query` result as an `i64`.
fn first_i64(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Option<i64> {
    msgs.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(r) => {
            Some(r.get(0).expect("column 0").parse::<i64>().expect("i64"))
        }
        _ => None,
    })
}

/// Authoritative cross-range conservation read, bounded-retry. Re-resolves a LIVE gateway
/// each attempt; on any connect/read failure (transient `08006`, a gateway caught mid-fault,
/// heavy CI contention) it re-resolves and retries, bounded by a 30s deadline. No settle-
/// sleep — paced by the real round-trip + a short poll. Replaces an unretried one-shot read
/// that could panic on a transient failure (the PR #34 flake).
async fn read_total_cross_until_ok(c: &Cluster, accounts: i64) -> i64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(total) = try_read_total_cross(c, accounts).await {
            return total;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "conservation read did not succeed within 30s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// One attempt: pick a live gateway, sum every account over both tables (`acct_a` in range 0,
/// `acct_b` in range 1); `None` on any failure (so the caller re-resolves and retries — never
/// a partial/fabricated total). (`SUM` is not in the SQL subset yet, so add in Rust.)
async fn try_read_total_cross(c: &Cluster, accounts: i64) -> Option<i64> {
    let gw = c.pick_live_gateway().await;
    let client = connect(c.sql_addr(gw)).await?;
    let mut total = 0i64;
    for table in ["acct_a", "acct_b"] {
        for id in 0..accounts {
            let rows = tokio::time::timeout(
                Duration::from_secs(10),
                client.simple_query(&format!("SELECT bal FROM {table} WHERE id = {id}")),
            )
            .await
            .ok()?
            .ok()?;
            total += first_i64(&rows)?;
        }
    }
    Some(total)
}
