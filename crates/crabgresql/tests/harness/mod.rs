//! Multi-process test harness: spawns `crabgresql node` children, drives the
//! control protocol + SQL, injects crashes/partitions.
//!
//! This is a shared test-support module: it exposes the full harness API
//! (kill/respawn/wait_applied/add_node/…) used by the scenarios in
//! `multiprocess.rs` — the crash-recovery, runtime-membership, and
//! crash+partition bank-nemesis tests exercise the whole surface.
//!
//! Each test binary that `mod`-includes this file compiles it independently, so
//! dead-code is judged per binary: `jepsen_elle.rs` exercises only the
//! crash+partition subset, while `multiprocess.rs` drives the membership and
//! apply-lag helpers. Every item here is live in *some* consumer, so we allow
//! dead_code module-wide rather than churn per-item attributes as binaries vary.
#![allow(dead_code)]
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use cluster::transport::frame::{read_msg, write_msg};
use cluster::transport::protocol::{
    ControlRequest, ControlResponse, NodeRequest, NodeResponse, NodeStatus,
};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

pub struct ProcNode {
    pub id: u64,
    pub node_addr: String,
    pub sql_addr: String,
    pub dir: PathBuf,
    pub child: Child,
}

pub struct Cluster {
    pub nodes: Vec<ProcNode>,
    _tmp: TempDir, // base dir for all node data dirs; kept alive for the test
    peers_arg: Vec<String>,
    boundaries: Vec<u32>, // multi-range RangeMap boundaries (empty ⇒ single range)
    replicated: bool,     // bring-up mode: true ⇒ nodes route through start_replicated
}

async fn free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let p = l.local_addr().expect("local_addr").port();
    drop(l);
    p
}

impl Cluster {
    /// Spawn `n` node processes (node 0 bootstraps) and wait for a leader.
    pub async fn spawn(n: u64) -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut info = Vec::new();
        for id in 0..n {
            let node_addr = format!("127.0.0.1:{}", free_port().await);
            let sql_addr = format!("127.0.0.1:{}", free_port().await);
            info.push((id, node_addr, sql_addr));
        }
        let peers_arg: Vec<String> = info
            .iter()
            .map(|(id, na, sa)| format!("{id}@{na}|{sa}"))
            .collect();
        let mut nodes = Vec::new();
        for (id, node_addr, sql_addr) in &info {
            let dir = tmp.path().join(format!("node-{id}"));
            std::fs::create_dir_all(&dir).expect("create node dir");
            let child = spawn_node(
                *id,
                node_addr,
                sql_addr,
                &dir,
                &peers_arg,
                &[],
                false,
                *id == 0,
            );
            nodes.push(ProcNode {
                id: *id,
                node_addr: node_addr.clone(),
                sql_addr: sql_addr.clone(),
                dir,
                child,
            });
        }
        let c = Self {
            nodes,
            _tmp: tmp,
            peers_arg,
            boundaries: Vec::new(),
            replicated: false,
        };
        c.wait_for_leader().await;
        c
    }

    /// Spawn `n` node processes that each host EVERY range of a multi-range
    /// `RangeMap` built from `boundaries` (table-id split points, the same on all
    /// nodes). Node 0 bootstraps; waits until *some* node reports a leader (each
    /// range elects independently — per-range readiness is confirmed by the test
    /// via an SQL read-back, since the control protocol is node-global).
    pub async fn spawn_multirange(n: u64, boundaries: Vec<u32>) -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut info = Vec::new();
        for id in 0..n {
            let node_addr = format!("127.0.0.1:{}", free_port().await);
            let sql_addr = format!("127.0.0.1:{}", free_port().await);
            info.push((id, node_addr, sql_addr));
        }
        let peers_arg: Vec<String> = info
            .iter()
            .map(|(id, na, sa)| format!("{id}@{na}|{sa}"))
            .collect();
        let mut nodes = Vec::new();
        for (id, node_addr, sql_addr) in &info {
            let dir = tmp.path().join(format!("node-{id}"));
            std::fs::create_dir_all(&dir).expect("create node dir");
            let child = spawn_node(
                *id,
                node_addr,
                sql_addr,
                &dir,
                &peers_arg,
                &boundaries,
                false,
                *id == 0,
            );
            nodes.push(ProcNode {
                id: *id,
                node_addr: node_addr.clone(),
                sql_addr: sql_addr.clone(),
                dir,
                child,
            });
        }
        let c = Self {
            nodes,
            _tmp: tmp,
            peers_arg,
            boundaries,
            replicated: false,
        };
        c.wait_for_leader().await;
        c
    }

    /// Spawn `n` Replicated-mode node processes. Node 0 bootstraps WITH
    /// `boundaries` (it seeds the descriptor blob); nodes 1.. start with NO
    /// boundaries and learn the layout from the meta range. All pass
    /// `--replicated-ranges`.
    pub async fn spawn_multirange_replicated(n: u64, boundaries: Vec<u32>) -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut info = Vec::new();
        for id in 0..n {
            let node_addr = format!("127.0.0.1:{}", free_port().await);
            let sql_addr = format!("127.0.0.1:{}", free_port().await);
            info.push((id, node_addr, sql_addr));
        }
        let peers_arg: Vec<String> = info
            .iter()
            .map(|(id, na, sa)| format!("{id}@{na}|{sa}"))
            .collect();
        let mut nodes = Vec::new();
        for (id, node_addr, sql_addr) in &info {
            let dir = tmp.path().join(format!("node-{id}"));
            std::fs::create_dir_all(&dir).expect("create node dir");
            // Only the bootstrap node (0) carries the boundaries seed.
            let node_boundaries: &[u32] = if *id == 0 { &boundaries } else { &[] };
            let child = spawn_node(
                *id,
                node_addr,
                sql_addr,
                &dir,
                &peers_arg,
                node_boundaries,
                true,
                *id == 0,
            );
            nodes.push(ProcNode {
                id: *id,
                node_addr: node_addr.clone(),
                sql_addr: sql_addr.clone(),
                dir,
                child,
            });
        }
        // `boundaries` is retained for respawn parity, but a respawned replicated
        // node re-boots through `start_replicated` (because `respawn` honors
        // `self.replicated`) and reads its layout from the already-committed
        // descriptor blob in range 0's log via `wait_for_range_map` — so its
        // CLI boundaries are irrelevant after the first boot.
        let c = Self {
            nodes,
            _tmp: tmp,
            peers_arg,
            boundaries,
            replicated: true,
        };
        c.wait_for_leader().await;
        c
    }

    pub async fn control(&self, id: u64, req: ControlRequest) -> Option<ControlResponse> {
        let addr = &self.nodes[id as usize].node_addr;
        let mut s = TcpStream::connect(addr).await.ok()?;
        write_msg(&mut s, &NodeRequest::Control(req)).await.ok()?;
        match read_msg::<_, NodeResponse>(&mut s).await.ok()? {
            NodeResponse::Control(r) => Some(r),
            _ => None,
        }
    }

    pub async fn status(&self, id: u64) -> Option<NodeStatus> {
        match self.control(id, ControlRequest::GetStatus).await? {
            ControlResponse::Status(s) => Some(s),
            _ => None,
        }
    }

    /// Wait (bounded) until some node reports a leader; return its id.
    ///
    /// Cross-process polling cadence: the node-global control protocol gives no push
    /// signal, so we re-probe `status()` on a small fixed POLL INTERVAL, bounded by a
    /// deadline, returning the instant a leader is observed. The interval is NOT a
    /// "settle" guess (the CLAUDE.md prohibition is on in-process waits that should
    /// use openraft events and on fixed completion-duration guesses) — it is the poll
    /// period, and it is load-bearing: a zero-gap loop opens control connections fast
    /// enough to starve the nodes' Raft progress under test contention (the
    /// stable-window lesson), so the interval both paces the probe and leaves the
    /// cluster room to elect. This matches the established multi-process harness.
    pub async fn wait_for_leader(&self) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            for n in &self.nodes {
                if let Some(st) = self.status(n.id).await
                    && let Some(l) = st.current_leader
                {
                    return l;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no leader within 30s"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Wait (bounded) until node `id` has applied at least `idx`.
    ///
    /// Same cross-process polling cadence as `wait_for_leader`: a small fixed POLL
    /// INTERVAL between `status()` probes (not a settle-guess), bounded by a deadline
    /// so a stuck node fails the test instead of hanging, and gentle enough not to
    /// storm the control port.
    pub async fn wait_applied(&self, id: u64, idx: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(st) = self.status(id).await
                && st.last_applied.unwrap_or(0) >= idx
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "node {id} did not apply {idx}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub fn sql_addr(&self, id: u64) -> &str {
        &self.nodes[id as usize].sql_addr
    }

    /// Number of nodes in the cluster (for round-robin client placement).
    // A test cluster is never empty, so `is_empty` would be unreachable noise.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Connect a tokio-postgres client to node `id % len` (deterministic round-robin
    /// so workers spread connections across all nodes, exercising the proxy on
    /// followers). Returns `None` if that node is unreachable (killed/partitioned),
    /// so the caller can advance to the next node.
    pub async fn pg_try(&self, id: usize) -> Option<tokio_postgres::Client> {
        let node = id % self.nodes.len();
        let addr = &self.nodes[node].sql_addr;
        let port = addr.rsplit(':').next()?;
        let cs = format!("host=127.0.0.1 port={port} user=postgres");
        match tokio::time::timeout(
            Duration::from_secs(8),
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

    /// Open a tokio-postgres client to node `id`'s SQL port, retrying until the
    /// listener accepts (bounded by a deadline).
    ///
    /// A one-shot connect races the node's SQL-listener startup: in replicated-meta
    /// mode (SP15) a node's SQL gateway binds only at the END of its two-phase
    /// bootstrap (range 0 up → descriptor blob read → data ranges built), which can
    /// lag `wait_for_leader` (which observes only range 0's control port) on a slow
    /// runner — so the gateway port may still refuse connections the instant a
    /// leader is reported. Retry on `ConnectionRefused` until the listener is up,
    /// using the same bounded cross-process POLL INTERVAL as the other harness waits
    /// (a poll cadence, not a settle guess); a genuinely dead node fails the test at
    /// the deadline instead of flaking.
    pub async fn pg(&self, id: u64) -> tokio_postgres::Client {
        let addr = self.sql_addr(id);
        let port = addr.rsplit(':').next().expect("sql_addr has a port");
        let cs = format!("host=127.0.0.1 port={port} user=postgres");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            match tokio::time::timeout(
                Duration::from_secs(8),
                tokio_postgres::connect(&cs, tokio_postgres::NoTls),
            )
            .await
            {
                Ok(Ok((client, conn))) => {
                    tokio::spawn(conn);
                    return client;
                }
                _ => {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "pg connect to node {id} did not succeed within 30s (listener never came up)"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// Bounded, condition-driven wait: re-issue `select_sql` through live nodes
    /// (round-robin, advancing past unreachable ones) until column 0 of the first
    /// row equals `expected`, or the deadline trips. This is the SQL-observable
    /// per-range progress signal the crash nemesis gates on — the control protocol
    /// is node-global, so a committed read-back THROUGH the owning range is the only
    /// per-range commit signal available across the process boundary. Uses the same
    /// bounded POLL INTERVAL as the other harness waits (poll cadence, not a settle
    /// guess) so it does not storm the SQL port.
    pub async fn wait_select_value(&self, select_sql: &str, expected: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut idx = 0usize;
        loop {
            if let Some(client) = self.pg_try(idx).await
                && let Ok(Ok(msgs)) =
                    tokio::time::timeout(Duration::from_secs(8), client.simple_query(select_sql))
                        .await
            {
                let got = msgs.iter().find_map(|m| match m {
                    tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(|s| s.to_string()),
                    _ => None,
                });
                if got.as_deref() == Some(expected) {
                    return;
                }
            }
            idx = idx.wrapping_add(1) % self.nodes.len();
            assert!(
                tokio::time::Instant::now() < deadline,
                "`{select_sql}` did not return {expected:?} within 30s"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Execute a write, retrying through live nodes until it commits or the deadline
    /// trips. Tolerates the transient retryable `40001` ("not the leader") during a
    /// re-election window — a correct client retries on a retryable error, and the
    /// test's claim is that the range RESUMES, not that the first attempt in the
    /// election window wins. Same bounded poll cadence as the other harness waits.
    /// (Callers pair this with a `wait_select_value` read-back to prove durability;
    /// the statement should be safe to apply more than once — e.g. a presence-checked
    /// INSERT whose read-back asserts the value exists, not a row count.)
    pub async fn exec_until_ok(&self, sql: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut idx = 0usize;
        loop {
            if let Some(client) = self.pg_try(idx).await
                && matches!(
                    tokio::time::timeout(Duration::from_secs(8), client.simple_query(sql)).await,
                    Ok(Ok(_))
                )
            {
                return;
            }
            idx = idx.wrapping_add(1) % self.nodes.len();
            assert!(
                tokio::time::Instant::now() < deadline,
                "`{sql}` did not commit within 30s"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Hard-kill node `id` (SIGKILL / TerminateProcess).
    pub async fn kill(&mut self, id: u64) {
        let _ = self.nodes[id as usize].child.start_kill();
        let _ = self.nodes[id as usize].child.wait().await;
    }

    /// Respawn node `id` from its existing data dir (bootstrap=false; it recovers).
    pub fn respawn(&mut self, id: u64) {
        let boundaries = self.boundaries.clone();
        let replicated = self.replicated;
        let n = &mut self.nodes[id as usize];
        n.child = spawn_node(
            id,
            &n.node_addr,
            &n.sql_addr,
            &n.dir,
            &self.peers_arg,
            &boundaries,
            replicated, // honor the cluster's bring-up mode (was hardcoded false)
            false,      // bootstrap=false: recover from the durable store
        );
    }

    /// Spawn a brand-new node `id` (fresh ports + data dir under the shared
    /// TempDir) with the SAME peer list, bootstrap=false. It is not yet a Raft
    /// member — it is reachable so the leader can `AddLearner` it and replicate
    /// over TCP. Pushes a `ProcNode` to `self.nodes` (expects `id == nodes.len()`).
    pub async fn add_node(&mut self, id: u64) {
        assert_eq!(
            id as usize,
            self.nodes.len(),
            "add_node expects the next contiguous id"
        );
        let node_addr = format!("127.0.0.1:{}", free_port().await);
        let sql_addr = format!("127.0.0.1:{}", free_port().await);
        let dir = self._tmp.path().join(format!("node-{id}"));
        std::fs::create_dir_all(&dir).expect("create node dir");
        let boundaries = self.boundaries.clone();
        let child = spawn_node(
            id,
            &node_addr,
            &sql_addr,
            &dir,
            &self.peers_arg,
            &boundaries,
            false,
            false,
        );
        self.nodes.push(ProcNode {
            id,
            node_addr,
            sql_addr,
            dir,
            child,
        });
    }

    /// A node id that leads NONE of `ranges` (so the gateway must coordinate
    /// remotely), or node 0 if no such node exists.
    pub async fn pick_nonleading_gateway(&self, ranges: &[u32]) -> u64 {
        for n in &self.nodes {
            if let Some(ControlResponse::RangeLeaders(v)) =
                self.control(n.id, ControlRequest::RangeLeaders).await
            {
                let leads_any = v
                    .iter()
                    .any(|(r, l)| ranges.contains(r) && *l == Some(n.id));
                if !leads_any {
                    return n.id;
                }
            }
        }
        0
    }

    /// The current leader of `range` (polls live nodes' RangeLeaders, bounded).
    pub async fn range_leader(&self, range: u32) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            for n in &self.nodes {
                if let Some(ControlResponse::RangeLeaders(v)) =
                    self.control(n.id, ControlRequest::RangeLeaders).await
                    && let Some((_, Some(l))) = v.iter().find(|(r, l)| *r == range && l.is_some())
                {
                    return *l;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no leader for range {range} within 30s"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// First node that answers status (a live gateway to issue SQL through).
    pub async fn pick_live_gateway(&self) -> u64 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            for n in &self.nodes {
                if self.status(n.id).await.is_some() {
                    return n.id;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no live gateway within 30s"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// Convenience: an `AddLearner` control request.
pub fn ctl_add_learner(id: u64, addr: String) -> ControlRequest {
    ControlRequest::AddLearner { id, addr }
}

/// Convenience: a `ChangeMembership` control request.
pub fn ctl_change_membership(ids: Vec<u64>) -> ControlRequest {
    ControlRequest::ChangeMembership(ids)
}

#[allow(clippy::too_many_arguments)]
fn spawn_node(
    id: u64,
    node_addr: &str,
    sql_addr: &str,
    dir: &std::path::Path,
    peers: &[String],
    boundaries: &[u32],
    replicated: bool,
    bootstrap: bool,
) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_crabgresql"));
    cmd.arg("node")
        .arg("--id")
        .arg(id.to_string())
        .arg("--node-addr")
        .arg(node_addr)
        .arg("--sql-addr")
        .arg(sql_addr)
        .arg("--data-dir")
        .arg(dir);
    for p in peers {
        cmd.arg("--peer").arg(p);
    }
    for b in boundaries {
        cmd.arg("--range-boundaries").arg(b.to_string());
    }
    if replicated {
        cmd.arg("--replicated-ranges");
    }
    if bootstrap {
        cmd.arg("--bootstrap");
    }
    cmd.stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    cmd.spawn().expect("spawn node")
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for n in &mut self.nodes {
            let _ = n.child.start_kill();
        }
    }
}
