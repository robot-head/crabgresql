//! Cross-range 2PC committed-half survival under a PARTICIPANT-leader kill (the in-scope case).
//!
//! A multi-process crash nemesis hard-kills the leader of range 1 (`acct_b`'s home — a pure 2PC
//! PARTICIPANT, NOT the GTM/coordinator) while cross-range transfers are IN FLIGHT, so a kill can
//! land between the global COMMIT decision (range 0's `g -> Committed`) and the participant's
//! local release. The committed `g`'s `acct_b` half — a durable `Prepared(Lb -> g)` version whose
//! held session died with the old leader — must SURVIVE: the newly-risen range-1 leader
//! reconstructs every inherited in-doubt marker (its rise sweep drives each to its durable global
//! decision) before serving, and the staged version + that decision re-apply the committed half as
//! the sole live version. If a freshly-risen leader served a write before that reconstruction, it
//! could supersede the committed half with a non-superseding version and tear the bank total.
//!
//! SCOPE (mirrors `range0_leader_kill_drain`'s single-failover scoping): the nemesis kills only a
//! node that leads range 1 but NOT range 0, so range 0 (GTM/coordinator/`acct_a`) stays stable —
//! isolating PARTICIPANT-range committed-half survival from the deferred range-0/coordinator
//! co-failover case (GTM global-xid reuse under an in-flight range-0 leadership change; a spec
//! non-goal, see SP23). Each round fully recovers (a settle-aware barrier) before the next kill,
//! so kills never overlap recovery (the deferred cascading case).
mod harness;
use harness::Cluster;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn participant_leader_kill_conserves_total() {
    const ACCOUNTS: i64 = 4;
    const SEED: i64 = 100;
    const PROCS: usize = 3;
    const OPS: usize = 8;
    const MIN_ROUNDS: usize = 4;
    let seeded_total = 2 * ACCOUNTS * SEED; // two tables, two ranges

    // 5 nodes, boundary [2] (5 nodes keep a quorum when one node is faulted): acct_a (id 1) ->
    // range 0, acct_b (id 2) -> range 1.
    let mut c = Cluster::spawn_multirange(5, vec![2]).await;
    let committed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)); // committed-op progress signal
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

    // Workers spread one-per-node across nodes 0..PROCS (each pins to its own gateway), so a
    // coordinator is often a NON-leading gateway and the nemesis kills the participant leader
    // mid-txn.
    let addrs: Vec<String> = (0..c.len())
        .map(|i| c.sql_addr(i as u64).to_string())
        .collect();
    let mut workers = Vec::new();
    for process in 0..PROCS {
        let addrs = addrs.clone();
        let sig = committed.clone();
        workers.push(tokio::spawn(async move {
            use std::sync::atomic::Ordering;
            let mut rng = Lcg::new(0x9E37_79B9_u64.wrapping_mul(process as u64 + 1));
            let mut n = 0usize;
            for _ in 0..OPS {
                let node = addrs[process % addrs.len()].clone();
                let Some(client) = connect(&node).await else {
                    continue;
                };
                let from = (rng.next() % ACCOUNTS as u64) as i64;
                let mut to = (rng.next() % ACCOUNTS as u64) as i64;
                if to == from {
                    to = (to + 1) % ACCOUNTS;
                }
                let amt = 1 + (rng.next() % 20) as i64;
                if cross_transfer(&client, from, to, amt).await {
                    n += 1;
                    sig.fetch_add(1, Ordering::Relaxed);
                }
            }
            n
        }));
    }

    // Nemesis: each round hard-kill the RANGE-1 (acct_b participant) leader mid-2PC (only when it
    // does NOT also lead range 0 — see SCOPE in the module doc). Pace the next fault on a
    // committed-op progress signal (no settle-sleep), and await a recovered quorum (both ranges
    // have a leader) plus a settle-aware barrier before advancing.
    use std::sync::atomic::Ordering;
    let mut round = 0usize;
    // Overall nemesis deadline (liveness guard) so a never-diverging leadership layout can never
    // hang the loop — it ends the nemesis and proceeds to the conservation oracle.
    let nemesis_deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    while (!workers.iter().all(|w| w.is_finished()) || round < MIN_ROUNDS)
        && tokio::time::Instant::now() < nemesis_deadline
    {
        // Kill a node that leads range 1 (the acct_b PARTICIPANT) but NOT range 0 (the
        // GTM/coordinator/acct_a home), so the kill is a clean PARTICIPANT failover with range 0
        // stable — isolating committed-half survival on the participant range from the deferred
        // cascading range-0/coordinator co-failover case (a spec non-goal; range-0 churn lives in
        // `range0_leader_kill_drain`).
        let l1 = c.range_leader(1).await;
        let l0 = c.range_leader(0).await;
        if l1 == l0 {
            // The two ranges currently share a leader; killing it would churn range 0 too. Wait
            // for the layout to diverge (paced on the harness poll cadence). If the workload is
            // already done we cannot manufacture a clean participant-only kill, so stop.
            if workers.iter().all(|w| w.is_finished()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        let victim = l1; // leads range 1, not range 0 — a clean participant failover
        let before = committed.load(Ordering::Relaxed);
        c.kill(victim).await;
        c.respawn(victim);
        // Wait for the workload to commit at least one op under the fault OR finish, bounded
        // (paced on real progress, never the clock); then let quorum recover.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while committed.load(Ordering::Relaxed) == before
            && !workers.iter().all(|w| w.is_finished())
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(100)).await; // harness poll cadence, not a settle-sleep
        }
        c.range_leader(0).await; // recovered-quorum gate before the next fault
        c.range_leader(1).await;
        // Settle-aware barrier: the just-killed range-1 leader gates writes until its rise sweep
        // settles. Drive ONE zero-sum cross-range op to completion (exec_until_ok retries the
        // brief 40001) so the next round starts from a range-1 leader that ADMITS writes — paces
        // on real progress, never a sleep. Amount 0 conserves the total.
        c.exec_until_ok(
            "BEGIN; UPDATE acct_a SET bal = bal - 0 WHERE id = 0; UPDATE acct_b SET bal = bal + 0 WHERE id = 0; COMMIT",
        )
        .await;
        round += 1;
    }
    let mut total_committed = 0usize;
    for w in workers {
        total_committed += w.await.expect("worker");
    }

    // Wait for leaders on both ranges (kill/respawn leaves no partition state).
    c.range_leader(0).await;
    c.range_leader(1).await;

    // RECOVERY-REQUIRED check: a post-heal transfer touching EVERY account pair must commit
    // within bound. A coordinator-crash-stranded lock that recovery failed to free would block
    // this forever -> exec_until_ok panics at its deadline -> fail.
    for id in 0..ACCOUNTS {
        let other = (id + 1) % ACCOUNTS;
        c.exec_until_ok(&format!(
            "BEGIN; UPDATE acct_a SET bal = bal - 0 WHERE id = {id}; UPDATE acct_b SET bal = bal + 0 WHERE id = {other}; COMMIT"
        )).await; // amount 0: touches+locks both rows, conserves total, requires no funds
    }

    // CONSERVATION oracle: sum both tables across both ranges == seeded total.
    let total = read_total_cross_until_ok(&c, ACCOUNTS).await;
    assert_eq!(
        total, seeded_total,
        "cross-range transfers conserve the bank total under participant-leader kill (got {total}, want {seeded_total})"
    );
    assert!(
        total_committed > 0,
        "the workload must commit at least one transfer (non-vacuous)"
    );
}

// ---------------------------------------------------------------------------
// Module-local helpers.
// ---------------------------------------------------------------------------

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

/// Resilient connect: the nemesis kills the very gateways workers connect to, so this returns
/// `None` on any connection failure or timeout — the worker just `continue`s to its next op
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

/// Perform one CROSS-RANGE transfer transaction over `client`. Returns `true` iff it committed.
/// `acct_a` lives in range 0, `acct_b` in range 1, so the
/// `BEGIN; UPDATE acct_a; UPDATE acct_b; COMMIT` escalates to global 2PC.
///
/// Each statement is bounded by a 10s timeout. On any error/timeout the transfer is
/// INDETERMINATE: issue a best-effort bounded `ROLLBACK` (ignore its result) and return
/// `false`. A transfer nets zero, so only definitely-committed ones move money — and they
/// conserve the total.
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

/// Authoritative cross-range conservation read, bounded-retry. Re-resolves a LIVE gateway each
/// attempt; on any connect/read failure it re-resolves and retries, bounded by a 30s deadline.
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

/// One attempt: pick a live gateway, sum every account over both tables; `None` on any failure
/// (so the caller re-resolves and retries — never a partial/fabricated total).
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
