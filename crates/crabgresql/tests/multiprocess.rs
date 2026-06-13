mod harness;
use std::time::Duration;

use harness::Cluster;
use tokio_postgres::SimpleQueryMessage;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bringup_elects_leader_and_serves_sql() {
    let c = Cluster::spawn(3).await;
    let leader = c.wait_for_leader().await;
    let client = c.pg(leader).await;
    client
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create table");
    client
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    let rows = client
        .simple_query("SELECT id FROM t")
        .await
        .expect("select");
    assert_eq!(
        row_count(&rows),
        1,
        "leader serves SQL over the real cluster"
    );
}

// ---------------------------------------------------------------------------
// (0b) Client on a follower is transparently routed to the leader.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_on_follower_is_routed_to_leader() {
    let c = harness::Cluster::spawn(3).await;
    let leader = c.wait_for_leader().await;
    let follower = (0..3u64).find(|&i| i != leader).expect("a follower");
    // Connect to the FOLLOWER's SQL port — the proxy routes us to the leader.
    let client = c.pg(follower).await;
    client
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    client
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    let rows = client
        .simple_query("SELECT id FROM t")
        .await
        .expect("select");
    let n = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(n, 1, "SQL on a follower works (proxied to the leader)");
}

// ---------------------------------------------------------------------------
// Shared SQL helpers over tokio-postgres `simple_query`.
// ---------------------------------------------------------------------------

/// Count the `Row` messages in a `simple_query` result.
fn row_count(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

/// Parse column 0 of the first row of a `simple_query` result as an `i64`.
fn first_i64(msgs: &[SimpleQueryMessage]) -> Option<i64> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => {
            Some(r.get(0).expect("column 0").parse::<i64>().expect("i64"))
        }
        _ => None,
    })
}

/// Assert a control response is `Ok`; turn `Err`/no-response into a test failure.
fn assert_ctl_ok(resp: Option<cluster::transport::protocol::ControlResponse>) {
    use cluster::transport::protocol::ControlResponse;
    match resp {
        Some(ControlResponse::Ok) => {}
        Some(ControlResponse::Err(e)) => panic!("control failed: {e}"),
        Some(other) => panic!("unexpected control response: {other:?}"),
        None => panic!("control failed: leader unreachable"),
    }
}

// ---------------------------------------------------------------------------
// (1) Committed write survives a kill + respawn (recovered over the wire).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_write_survives_kill_and_respawn() {
    let mut c = Cluster::spawn(3).await;
    let leader = c.wait_for_leader().await;
    let client = c.pg(leader).await;
    client
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create table");
    client
        .simple_query("INSERT INTO t VALUES (7)")
        .await
        .expect("insert");
    let follower = (0..3u64).find(|&i| i != leader).expect("a follower");
    // membership + leader noop + write => applied index reaches at least 2.
    c.wait_applied(follower, 2).await;
    c.kill(follower).await;
    c.respawn(follower);
    // The respawned follower recovers its committed state from disk and is brought
    // current over the wire by the leader.
    c.wait_applied(follower, 2).await;
    // Read via the (re-resolved) leader to confirm the committed row survived.
    let l = c.wait_for_leader().await;
    let rows = c
        .pg(l)
        .await
        .simple_query("SELECT id FROM t")
        .await
        .expect("select");
    assert_eq!(row_count(&rows), 1, "committed row survives kill + respawn");
}

// ---------------------------------------------------------------------------
// (2) Leader kill → failover to a new leader → old leader respawns + rejoins.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_kill_failover_and_rejoin() {
    let mut c = Cluster::spawn(3).await;
    let old = c.wait_for_leader().await;
    let client = c.pg(old).await;
    client
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create table");
    client
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    c.kill(old).await;

    // A new leader (!= old) emerges among the survivors.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let neu = loop {
        let mut found = None;
        for id in (0..3).filter(|&i| i != old) {
            if let Some(st) = c.status(id).await
                && let Some(l) = st.current_leader
                && l != old
            {
                found = Some(l);
            }
        }
        if let Some(l) = found {
            break l;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no new leader within 30s after killing the old one"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    // The committed row survived the failover, and a fresh write lands on the new leader.
    let nl = c.pg(neu).await;
    let rows = nl.simple_query("SELECT id FROM t").await.expect("select");
    assert_eq!(row_count(&rows), 1, "committed data survived failover");
    nl.simple_query("INSERT INTO t VALUES (2)")
        .await
        .expect("write to new leader");

    // The old leader respawns and rejoins, catching up to the post-failover log.
    c.respawn(old);
    c.wait_applied(old, 2).await;
}

// ---------------------------------------------------------------------------
// (3) Runtime membership: learner join + promote, then leave + kill.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_join_then_leave() {
    let mut c = Cluster::spawn(3).await;
    let leader = c.wait_for_leader().await;
    let client = c.pg(leader).await;
    client
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create table");
    for i in 0..5 {
        client
            .simple_query(&format!("INSERT INTO t VALUES ({i})"))
            .await
            .expect("insert");
    }

    // Spawn a 4th node (id 3), add it as a learner, then promote it into the group.
    c.add_node(3).await;
    let addr3 = c.nodes[3].node_addr.clone();
    assert_ctl_ok(c.control(leader, harness::ctl_add_learner(3, addr3)).await);
    assert_ctl_ok(
        c.control(leader, harness::ctl_change_membership(vec![0, 1, 2, 3]))
            .await,
    );
    // Capture the leader's last_log_index so node 3 catches up over TCP to it.
    let target = c
        .status(leader)
        .await
        .and_then(|s| s.last_log_index)
        .expect("leader last_log_index");
    c.wait_applied(3, target).await;

    // Remove node 2 from the group, then kill it; the cluster stays healthy.
    assert_ctl_ok(
        c.control(leader, harness::ctl_change_membership(vec![0, 1, 3]))
            .await,
    );
    c.kill(2).await;

    let l = c.wait_for_leader().await;
    c.pg(l)
        .await
        .simple_query("INSERT INTO t VALUES (9)")
        .await
        .expect("cluster healthy after reconfig");
}

// ---------------------------------------------------------------------------
// (4) Bank conservation under a crash + partition nemesis (the climax).
// ---------------------------------------------------------------------------

/// A tiny deterministic LCG so the workload is varied yet reproducible-in-spirit
/// without pulling in `rand` or depending on the wall clock.
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

/// Read the bank's total by summing each account's balance over `client`.
/// (`SUM` is not in the SQL subset yet, so add the N balances in Rust.)
async fn read_total(client: &tokio_postgres::Client, accounts: i64) -> i64 {
    let mut total = 0;
    for id in 0..accounts {
        let r = client
            .simple_query(&format!("SELECT bal FROM accounts WHERE id = {id}"))
            .await
            .expect("read balance");
        total += first_i64(&r).expect("balance row");
    }
    total
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bank_conserves_under_crash_and_partition_nemesis() {
    const ACCOUNTS: i64 = 4;
    const SEED: i64 = 100;
    const PROCS: usize = 2;
    const OPS: usize = 6;
    const MIN_ROUNDS: usize = 3;
    let seeded_total = ACCOUNTS * SEED;

    let mut c = Cluster::spawn(3).await;
    // FIXED leader: the workers target it the whole run and the nemesis only ever
    // faults a FOLLOWER, so the leader always keeps a 2/3 quorum and can commit.
    let leader = c.wait_for_leader().await;
    let followers: Vec<u64> = (0..3u64).filter(|&i| i != leader).collect();

    // Seed the accounts so the invariant total is known.
    {
        let setup = c.pg(leader).await;
        setup
            .simple_query("CREATE TABLE accounts (id int8, bal int8)")
            .await
            .expect("create");
        for id in 0..ACCOUNTS {
            setup
                .simple_query(&format!("INSERT INTO accounts VALUES ({id}, {SEED})"))
                .await
                .expect("seed");
        }
    }

    // Workers: each opens ONE tokio-postgres client to the FIXED leader's sql_addr
    // (owning a cloned String — they must NOT borrow the Cluster, so the main task
    // keeps &mut self for the nemesis) and runs OPS transfers. A transfer nets zero,
    // so as long as each txn is atomic the total is conserved however they interleave
    // or fail. Each statement is bounded by a 10s timeout; any error/timeout rolls
    // back (best-effort) and counts the transfer as indeterminate (NOT committed).
    let leader_sql = c.sql_addr(leader).to_string();
    let mut workers = Vec::new();
    for process in 0..PROCS {
        let sql_addr = leader_sql.clone();
        workers.push(tokio::spawn(async move {
            let port = sql_addr.rsplit(':').next().expect("port");
            let cs = format!("host=127.0.0.1 port={port} user=postgres");
            let (client, conn) = tokio_postgres::connect(&cs, tokio_postgres::NoTls)
                .await
                .expect("worker pg connect");
            tokio::spawn(conn);

            let mut rng = Lcg::new(0x9E37_79B9_u64.wrapping_mul(process as u64 + 1));
            let mut committed = 0usize;
            for _ in 0..OPS {
                let from = (rng.next() % ACCOUNTS as u64) as i64;
                let mut to = (rng.next() % ACCOUNTS as u64) as i64;
                if to == from {
                    to = (to + 1) % ACCOUNTS;
                }
                let amt = 1 + (rng.next() % 20) as i64;
                if transfer(&client, from, to, amt).await {
                    committed += 1;
                }
            }
            committed
        }));
    }

    // Nemesis (INLINE — owns &mut Cluster). One fault at a time, FOLLOWERS ONLY,
    // round-robin, NEVER both followers at once: the leader always keeps one healthy
    // follower for quorum. Alternate (a) kill+respawn and (b) brief partition+heal.
    // No fixed sleeps — the kill/respawn/control I/O paces the loop.
    let mut round = 0usize;
    while !workers.iter().all(|w| w.is_finished()) || round < MIN_ROUNDS {
        let victim = followers[round % followers.len()];
        if round.is_multiple_of(2) {
            c.kill(victim).await;
            c.respawn(victim);
        } else {
            // Bidirectionally isolate `victim` from the other two nodes, then heal.
            let others: Vec<u64> = (0..3u64).filter(|&i| i != victim).collect();
            let _ = c
                .control(
                    victim,
                    cluster::transport::protocol::ControlRequest::SetPartition(others.clone()),
                )
                .await;
            for &o in &others {
                let _ = c
                    .control(
                        o,
                        cluster::transport::protocol::ControlRequest::SetPartition(vec![victim]),
                    )
                    .await;
            }
            for id in 0..3u64 {
                let _ = c
                    .control(id, cluster::transport::protocol::ControlRequest::Heal)
                    .await;
            }
        }
        round += 1;
    }

    // Join the workers; sum their committed counts.
    let mut total_committed = 0usize;
    for w in workers {
        total_committed += w.await.expect("worker joined");
    }

    // Heal everything and let the cluster settle, then read the authoritative total.
    for id in 0..3u64 {
        let _ = c
            .control(id, cluster::transport::protocol::ControlRequest::Heal)
            .await;
    }
    let final_leader = c.wait_for_leader().await;
    let final_total = read_total(&c.pg(final_leader).await, ACCOUNTS).await;

    assert_eq!(
        final_total, seeded_total,
        "no acked transfer lost across crash + partition (got {final_total}, want {seeded_total})"
    );
    assert!(
        total_committed > 0,
        "workload must commit at least one transfer (non-vacuous)"
    );
}

/// Perform one transfer transaction over `client`. Returns `true` iff it committed.
///
/// `BEGIN; UPDATE -amt; UPDATE +amt; COMMIT`, each statement bounded by a 10s
/// timeout. On any error/timeout the transfer is indeterminate: issue a best-effort
/// bounded `ROLLBACK` (ignore its result) and return `false`. A transfer nets zero,
/// so only definitely-committed ones move money — and they conserve the total.
async fn transfer(client: &tokio_postgres::Client, from: i64, to: i64, amt: i64) -> bool {
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
    let upd1 = format!("UPDATE accounts SET bal = bal - {amt} WHERE id = {from}");
    let upd2 = format!("UPDATE accounts SET bal = bal + {amt} WHERE id = {to}");
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
