//! Multi-process test harness: spawns `crabgresql node` children, drives the
//! control protocol + SQL, injects crashes/partitions.
//!
//! This is a shared test-support module: it exposes the full harness API
//! (kill/respawn/wait_applied/…) for every scenario in `multiprocess.rs`. The
//! bring-up scenario alone doesn't touch the fault-injection surface, so the
//! module-level allow keeps the build warning-free until the crash/membership/
//! nemesis scenarios exercise the rest.
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
            .map(|(id, na, _)| format!("{id}@{na}"))
            .collect();
        let mut nodes = Vec::new();
        for (id, node_addr, sql_addr) in &info {
            let dir = tmp.path().join(format!("node-{id}"));
            std::fs::create_dir_all(&dir).expect("create node dir");
            let child = spawn_node(*id, node_addr, sql_addr, &dir, &peers_arg, *id == 0);
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

    /// Hard-kill node `id` (SIGKILL / TerminateProcess).
    pub async fn kill(&mut self, id: u64) {
        let _ = self.nodes[id as usize].child.start_kill();
        let _ = self.nodes[id as usize].child.wait().await;
    }

    /// Respawn node `id` from its existing data dir (bootstrap=false; it recovers).
    pub fn respawn(&mut self, id: u64) {
        let n = &mut self.nodes[id as usize];
        n.child = spawn_node(
            id,
            &n.node_addr,
            &n.sql_addr,
            &n.dir,
            &self.peers_arg,
            false,
        );
    }
}

fn spawn_node(
    id: u64,
    node_addr: &str,
    sql_addr: &str,
    dir: &std::path::Path,
    peers: &[String],
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
