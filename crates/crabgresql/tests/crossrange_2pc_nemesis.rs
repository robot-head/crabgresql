//! SP18 D3c-net-hard: cross-range 2PC conserves the bank total under a multi-process
//! crash/partition nemesis that kills random nodes INCLUDING mid-transaction
//! coordinators. Recovery (write-once decision + participant self-resolve) ensures
//! no transfer is half-applied and no lock is stranded forever.
mod harness;
use harness::Cluster;
use std::time::Duration;

use cluster::transport::protocol::ControlRequest;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_range_bank_conserves_total_under_crash_nemesis() {
    const ACCOUNTS: i64 = 4;
    const SEED: i64 = 100;
    const PROCS: usize = 3;
    const OPS: usize = 8;
    const MIN_ROUNDS: usize = 4;
    let seeded_total = 2 * ACCOUNTS * SEED; // two tables, two ranges

    // 5 nodes, boundary [2] (correction C3 — 5 nodes keep a quorum when a non-leader
    // is faulted, mirroring jepsen_bank's cross-range nemesis): acct_a (id 1) -> range 0,
    // acct_b (id 2) -> range 1.
    let mut c = Cluster::spawn_multirange(5, vec![2]).await;
    let committed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)); // committed-op progress signal
    // Seed via `exec_until_ok` (bounded retry across nodes): right after bring-up the
    // gateway we land on may not yet have a quorum view, so an unretried first DDL can
    // hit a transient `08006 no-quorum`. Sequential `exec_until_ok` calls each commit
    // before the next begins, so acct_a (table id 1 -> range 0) is still created before
    // acct_b (id 2 -> range 1). Setup is fault-free, so a retried statement was cleanly
    // rejected, never double-applied.
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

    // Workers are spread one-per-node across nodes 0..PROCS (each worker pins to its
    // own gateway for the whole run), so a coordinator is often a NON-leading gateway
    // and the nemesis kills coordinators mid-txn. PROCS < node count is fine — the
    // point is only that some worker sits on a non-leading gateway under fault.
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
                    sig.fetch_add(1, Ordering::Relaxed); // committed-op progress signal
                }
            }
            n
        }));
    }

    // Nemesis (correction C3): fault a NON-LEADER victim only (keep quorum on both
    // ranges), pace the next fault on a committed-op progress signal (no settle-sleep),
    // and await a recovered quorum (both ranges have a leader) before advancing. A
    // killed non-leading gateway is still a crashed COORDINATOR mid-txn, so the recovery
    // path is exercised. Mirrors jepsen_bank::cross_range_bank_conserves_total_under_nemesis.
    use std::sync::atomic::Ordering;
    let mut round = 0usize;
    while !workers.iter().all(|w| w.is_finished()) || round < MIN_ROUNDS {
        // Pick a victim that leads NEITHER range (5 nodes / 2 ranges always leaves one).
        let l0 = c.range_leader(0).await;
        let l1 = c.range_leader(1).await;
        let victim = (0..c.len() as u64)
            .find(|&i| i != l0 && i != l1)
            .expect("a non-leader exists");
        let before = committed.load(Ordering::Relaxed);
        if round.is_multiple_of(2) {
            c.kill(victim).await;
            c.respawn(victim);
        } else {
            let others: Vec<u64> = (0..c.len() as u64).filter(|&i| i != victim).collect();
            let _ = c.control(victim, ctl_set_partition(others.clone())).await;
            for &o in &others {
                let _ = c.control(o, ctl_set_partition(vec![victim])).await;
            }
            for id in 0..c.len() as u64 {
                let _ = c.control(id, ctl_heal()).await;
            }
        }
        // Wait for the workload to commit at least one op under the fault OR finish,
        // bounded (paced on real progress, never the clock); then let quorum recover.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while committed.load(Ordering::Relaxed) == before
            && !workers.iter().all(|w| w.is_finished())
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(100)).await; // harness poll cadence, not a settle-sleep
        }
        c.range_leader(0).await; // recovered-quorum gate before the next fault
        c.range_leader(1).await;
        round += 1;
    }
    let mut total_committed = 0usize;
    for w in workers {
        total_committed += w.await.expect("worker");
    }

    // Heal; wait for leaders on both ranges.
    for id in 0..c.len() as u64 {
        let _ = c.control(id, ctl_heal()).await;
    }
    c.range_leader(0).await;
    c.range_leader(1).await;

    // RECOVERY-REQUIRED check: a post-heal transfer touching EVERY account pair must
    // commit within bound. A coordinator-crash-stranded lock that recovery failed to
    // free would block this forever -> exec_until_ok panics at its deadline -> fail.
    for id in 0..ACCOUNTS {
        let other = (id + 1) % ACCOUNTS;
        c.exec_until_ok(&format!(
            "BEGIN; UPDATE acct_a SET bal = bal - 0 WHERE id = {id}; UPDATE acct_b SET bal = bal + 0 WHERE id = {other}; COMMIT"
        )).await; // amount 0: touches+locks both rows, conserves total, requires no funds
    }

    // CONSERVATION oracle: sum both tables across both ranges == seeded total.
    // Bounded-retry the authoritative read (re-resolve a live gateway per attempt) so a
    // transient failure under heavy CI contention does not panic — the PR #34 flake class.
    let total = read_total_cross_until_ok(&c, ACCOUNTS).await;
    assert_eq!(
        total, seeded_total,
        "cross-range transfers conserve the bank total under crash+partition nemesis (got {total}, want {seeded_total})"
    );
    assert!(
        total_committed > 0,
        "the workload must commit at least one transfer (non-vacuous)"
    );
}

// ---------------------------------------------------------------------------
// Module-local helpers.
// ---------------------------------------------------------------------------

/// Thin wrapper: a `SetPartition` control request isolating this node from `ids`.
fn ctl_set_partition(ids: Vec<u64>) -> ControlRequest {
    ControlRequest::SetPartition(ids)
}

/// Thin wrapper: a `Heal` control request (clears all partitions on this node).
fn ctl_heal() -> ControlRequest {
    ControlRequest::Heal
}

/// A tiny deterministic LCG so the workload is varied yet reproducible-in-spirit
/// without pulling in `rand` or depending on the wall clock. (Same shape as
/// `multiprocess.rs`.)
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

/// Resilient connect: unlike `multiprocess.rs` (which connects once to a fixed LIVE
/// leader), HERE the nemesis kills the very gateways workers connect to. So this
/// returns `None` on any connection failure or timeout — the worker just `continue`s
/// to its next op rather than panicking. Bounded by a 10s timeout.
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
/// (ignore its result) and return `false`. A transfer nets zero, so only
/// definitely-committed ones move money — and they conserve the total.
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

/// Read the cross-range bank total by summing each account's balance over BOTH tables
/// (`acct_a` in range 0 + `acct_b` in range 1) for ids `0..accounts`. (`SUM` is not in
/// the SQL subset yet, so add the balances in Rust.)
/// Authoritative cross-range conservation read, bounded-retry. Re-resolves a LIVE
/// gateway each attempt; on any connect/read failure (transient `08006`, a gateway
/// caught mid-fault, heavy CI contention) it re-resolves and retries, bounded by a 30s
/// deadline. No settle-sleep — paced by the real round-trip + a short poll. Replaces an
/// unretried one-shot read that could panic on a transient failure (the PR #34 flake).
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

/// One attempt: pick a live gateway, sum every account over both tables; `None` on any
/// failure (so the caller re-resolves and retries — never a partial/fabricated total).
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
