//! A runnable replicated node: durable Raft over TCP + a pgwire SQL server over a
//! shared replicated engine + reseed-on-leadership + optional self-bootstrap.
//!
//! `ServerNode::start` opens the durable per-node store (SP8), wires openraft over
//! the real TCP transport (SP9 Tasks 1–4), binds the node-protocol listener and a
//! pgwire SQL listener, and shares ONE replicated [`SqlEngine`] between them. A
//! reseed task bumps the engine's xid/seq counters on each follower→leader edge so
//! a new leader never hands out an id below a prior leader's high-water mark.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use executor::SqlEngine;
use openraft::{BasicNode, ServerState};
use tokio::net::{TcpListener, TcpStream};

use crate::committer::RaftCommitter;
use crate::durable::{DurableLogStore, DurableStateMachineStore, NodeStore};
use crate::transport::client::TcpRaftNetwork;
use crate::transport::partition::PartitionState;
use crate::transport::server::{ShutdownSignal, serve_node_protocol};
use crate::types::{NodeId, TypeConfig};

/// Startup configuration for one node.
pub struct NodeConfig {
    /// This node's Raft id.
    pub id: NodeId,
    /// `host:port` the node-protocol listener binds (Raft RPCs + control).
    pub node_addr: String,
    /// `host:port` the pgwire SQL listener binds.
    pub sql_addr: String,
    /// Directory for this node's durable fjall store.
    pub data_dir: PathBuf,
    /// `(id, node-addr)` for every member, including self. Used for bootstrap.
    pub peers: Vec<(NodeId, String)>,
    /// When true, this node initializes the voting group once every peer's
    /// node-addr accepts a connection.
    pub bootstrap: bool,
}

/// A live node; `shutdown.wait()` resolves when a `Shutdown` control request fires.
pub struct ServerNode {
    pub raft: openraft::Raft<TypeConfig>,
    pub engine: Arc<SqlEngine>,
    pub shutdown: ShutdownSignal,
}

/// Long leader-stable timers (heartbeat 250ms, election 1000–2000ms) — the same
/// config that fixed coverage-instrumented election flakiness in SP8.
fn raft_config() -> Arc<openraft::Config> {
    Arc::new(
        openraft::Config {
            heartbeat_interval: 250,
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    )
}

impl ServerNode {
    /// Open the durable store, wire openraft over TCP, bind both listeners, and
    /// share one replicated engine. Spawns the node-protocol server, the SQL
    /// server, the reseed task, and (if `bootstrap`) the initializer.
    pub async fn start(cfg: NodeConfig) -> std::io::Result<Self> {
        let store = NodeStore::open(&cfg.data_dir).expect("open node store");
        let log = DurableLogStore::open(&store).expect("durable log");
        let sm = DurableStateMachineStore::open(&store).expect("durable sm");
        let sm_kv = sm.sm_kv();

        let partition = PartitionState::default();
        let net = TcpRaftNetwork {
            from: cfg.id,
            partition: partition.clone(),
        };
        let raft = openraft::Raft::new(cfg.id, raft_config(), net, log, sm)
            .await
            .expect("raft::new");

        // Node-protocol listener (Raft RPCs + control).
        let node_listener = TcpListener::bind(&cfg.node_addr).await?;
        let shutdown = ShutdownSignal::default();
        tokio::spawn(serve_node_protocol(
            node_listener,
            raft.clone(),
            partition.clone(),
            shutdown.clone(),
        ));

        // One shared replicated engine; reseed its counters on the leadership edge.
        let engine = Arc::new(
            SqlEngine::replicated(sm_kv, Arc::new(RaftCommitter { raft: raft.clone() }))
                .expect("replicated engine"),
        );
        tokio::spawn(reseed_on_leadership(raft.clone(), engine.clone()));

        // pgwire SQL listener with leader routing: serves locally when this node
        // is the leader, else byte-proxies to the leader's pgwire port.
        let sql_listener = TcpListener::bind(&cfg.sql_addr).await?;
        tokio::spawn(crate::route::serve_routed(
            sql_listener,
            raft.clone(),
            engine.clone(),
            Arc::new(pgwire::session::SessionConfig::trust()),
        ));

        if cfg.bootstrap {
            tokio::spawn(bootstrap(raft.clone(), cfg.peers.clone()));
        }

        Ok(Self {
            raft,
            engine,
            shutdown,
        })
    }
}

/// Reseed xid/seq counters on each follower→leader transition so they never
/// regress below a prior leader's high-water mark. Idempotent (only bumps up).
async fn reseed_on_leadership(raft: openraft::Raft<TypeConfig>, engine: Arc<SqlEngine>) {
    let mut rx = raft.metrics();
    let mut was_leader = false;
    loop {
        let is_leader = {
            let m = rx.borrow();
            m.state == ServerState::Leader && m.current_leader == Some(m.id)
        };
        if is_leader && !was_leader {
            let _ = engine.reseed_counters();
        }
        was_leader = is_leader;
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// Bootstrapper: wait until every peer's node-addr accepts a connection, then
/// initialize the voting group. On a restart the group is already initialized, so
/// the `initialize` error is ignored.
async fn bootstrap(raft: openraft::Raft<TypeConfig>, peers: Vec<(NodeId, String)>) {
    for (_, addr) in &peers {
        // `addr` may be packed as "node_addr|sql_addr"; connect only to the
        // node-protocol half so the TCP dial resolves.
        let dial = crate::addr::node_dial_addr(addr);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while TcpStream::connect(dial).await.is_err() {
            if tokio::time::Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let members: BTreeMap<NodeId, BasicNode> = peers
        .into_iter()
        .map(|(id, addr)| (id, BasicNode { addr }))
        .collect();
    let _ = raft.initialize(members).await; // ignore AlreadyInitialized on restart
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bind an ephemeral loopback port, read its address, and drop the listener so
    /// the address is free for the node to rebind.
    async fn free_port() -> String {
        let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let a = l.local_addr().expect("local_addr").to_string();
        drop(l);
        a
    }

    /// A single-node group bootstraps, elects itself, and serves SQL over pgwire:
    /// CREATE/INSERT/SELECT through a real tokio-postgres connection returns 1 row.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_node_serves_sql_after_election() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node_addr = free_port().await;
        let sql_addr = free_port().await;
        let node = ServerNode::start(NodeConfig {
            id: 0,
            node_addr: node_addr.clone(),
            sql_addr: sql_addr.clone(),
            data_dir: dir.path().to_path_buf(),
            peers: vec![(0, node_addr.clone())],
            bootstrap: true,
        })
        .await
        .expect("start node");

        // A one-node group elects immediately after `initialize`.
        node.raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(|m| m.current_leader == Some(0), "self leader")
            .await
            .expect("self leader");

        // Connect over pgwire and run simple SQL. The SQL listener is bound before
        // start() returns; leadership (awaited above) gives the serve task time to
        // reach `accept`, but poll-connect briefly in case the OS is slow to route.
        let port = sql_addr.rsplit(':').next().expect("port");
        let conn_str = format!("host=127.0.0.1 port={port} user=postgres");
        let (client, connection) = connect_with_retry(&conn_str).await;
        tokio::spawn(connection);

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
        let n = rows
            .iter()
            .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
            .count();
        assert_eq!(n, 1, "SELECT must return exactly one row");
    }

    /// Connect with a short bounded retry so a momentarily-not-yet-accepting SQL
    /// listener is tolerated without a fixed sleep.
    async fn connect_with_retry(
        conn_str: &str,
    ) -> (
        tokio_postgres::Client,
        tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            match tokio_postgres::connect(conn_str, tokio_postgres::NoTls).await {
                Ok(pair) => return pair,
                Err(e) if tokio::time::Instant::now() < deadline => {
                    let _ = e;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(e) => panic!("pg connect: {e}"),
            }
        }
    }
}
