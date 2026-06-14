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
            let child = spawn_node(*id, node_addr, sql_addr, &dir, &peers_arg, &[], *id == 0);
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
    /// No fixed sleep: each `status()` is a real TCP connect→request→response, so
    /// re-issuing it in this deadline-bounded loop paces the wait on observed state
    /// (the node-global control protocol gives no cross-process push signal).
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
        }
    }

    /// Wait (bounded) until node `id` has applied at least `idx`.
    ///
    /// No fixed sleep: the `status()` round-trip is the pacing; the loop is bounded
    /// by a deadline so a stuck node fails the test instead of hanging.
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

    /// Open a tokio-postgres client to node `id`'s SQL port.
    pub async fn pg(&self, id: u64) -> tokio_postgres::Client {
        let addr = self.sql_addr(id);
        let port = addr.rsplit(':').next().expect("sql_addr has a port");
        let cs = format!("host=127.0.0.1 port={port} user=postgres");
        let (client, conn) = tokio_postgres::connect(&cs, tokio_postgres::NoTls)
            .await
            .expect("pg connect");
        tokio::spawn(conn);
        client
    }

    /// Bounded, condition-driven wait: re-issue `select_sql` through live nodes
    /// (round-robin, advancing past unreachable ones) until column 0 of the first
    /// row equals `expected`, or the deadline trips. This is the SQL-observable
    /// per-range progress signal the crash nemesis gates on — the control protocol
    /// is node-global, so a committed read-back THROUGH the owning range is the only
    /// per-range commit signal available across the process boundary. No fixed
    /// sleep: each connect+query is a real round-trip that paces the loop.
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
        let n = &mut self.nodes[id as usize];
        n.child = spawn_node(
            id,
            &n.node_addr,
            &n.sql_addr,
            &n.dir,
            &self.peers_arg,
            &boundaries,
            false,
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
        );
        self.nodes.push(ProcNode {
            id,
            node_addr,
            sql_addr,
            dir,
            child,
        });
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

fn spawn_node(
    id: u64,
    node_addr: &str,
    sql_addr: &str,
    dir: &std::path::Path,
    peers: &[String],
    boundaries: &[u32],
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
