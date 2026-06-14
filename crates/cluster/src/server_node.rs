//! A runnable replicated node: durable Raft over TCP + a pgwire SQL server over a
//! shared replicated engine + reseed-on-leadership + optional self-bootstrap.
//!
//! `ServerNode::start` opens the durable per-node store (SP8), wires openraft over
//! the real TCP transport (SP9 Tasks 1–4), binds the node-protocol listener and a
//! pgwire SQL listener, and shares ONE replicated [`SqlEngine`] between them. A
//! reseed task bumps the engine's xid/seq counters on each follower→leader edge so
//! a new leader never hands out an id below a prior leader's high-water mark.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use executor::SqlEngine;
use openraft::{BasicNode, ServerState};
use tokio::net::{TcpListener, TcpStream};

use crate::committer::RaftCommitter;
use crate::durable::{DurableLogStore, DurableStateMachineStore, NodeStore};
use crate::linearizer::RaftLinearizer;
use crate::range::map::{RangeId, RangeMap};
use crate::transport::client::TcpRaftNetwork;
use crate::transport::partition::PartitionState;
use crate::transport::server::{RangeRegistry, ShutdownSignal, serve_node_protocol};
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
    /// The static range map this node hosts. Identical on every node. Defaults to
    /// `RangeMap::single()` (one range, id 0) — the single-range fast-path.
    pub range_map: RangeMap,
}

/// A live multi-range node; `shutdown.wait()` resolves when a `Shutdown` control
/// request fires. Holds one Raft instance + one applied store + one replicated
/// engine per range (all keyed by `RangeId`).
pub struct ServerNode {
    /// One Raft handle per range, keyed by `RangeId`.
    pub rafts: HashMap<RangeId, openraft::Raft<TypeConfig>>,
    /// One replicated engine per range, keyed by `RangeId`. Range 0's catalog
    /// store seeds every data-range engine's `catalog_kv`.
    pub engines: HashMap<RangeId, Arc<SqlEngine>>,
    /// The process-local network partition state, shared by every range's
    /// transport and the node-protocol server. Task 4 reads this to inject
    /// partitions in its remote-forward test (`gw.partition.clone()`).
    pub partition: PartitionState,
    pub shutdown: ShutdownSignal,
    /// One applied `data-r{r}` store per range, keyed by `RangeId`. Reached via
    /// the `sm_kv(range)` accessor (Task 4 needs `sm_kv(range)`, not a public map).
    sm_kvs: HashMap<RangeId, Arc<dyn kv::Kv>>,
    /// This node's id (`cfg.id`), exposed via `id()` for Task 4's leader resolution.
    id: NodeId,
}

impl ServerNode {
    /// This range's applied (`data-r{range}`) store. Panics if `range` is not
    /// hosted on this node — a construction bug, never user input.
    pub fn sm_kv(&self, range: RangeId) -> Arc<dyn kv::Kv> {
        self.sm_kvs[&range].clone()
    }

    /// This node's id.
    pub fn id(&self) -> NodeId {
        self.id
    }
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
    /// Open the durable store over the whole `RangeMap`, build a Raft + applied
    /// engine per range over the range-aware TCP transport (Task 1), register each
    /// group in the process-local `(range, node)` registry, bootstrap each voting
    /// group, and reseed each range's counters on its leadership edge.
    ///
    /// Per-range storage isolation (`data-r{r}`/`raft-r{r}`, Step 3) is the
    /// prerequisite this loop relies on: range `r`'s Raft is built over range `r`'s
    /// own keyspaces, so two ranges can never share log/SM state.
    pub async fn start(cfg: NodeConfig) -> std::io::Result<Self> {
        // One shared on-disk Database hosting every range's keyspace pair.
        let store = NodeStore::open(&cfg.data_dir, &cfg.range_map).expect("open node store");

        let partition = PartitionState::default();
        // Process-local registry the node-protocol server dispatches against:
        // an inbound `Raft { range, .. }` RPC resolves `(range, id)` here.
        let registry = RangeRegistry::new();
        let shutdown = ShutdownSignal::default();

        // Range 0's applied store is the catalog every data range resolves from.
        let catalog_kv = store.data_kv(0) as Arc<dyn kv::Kv>;

        let mut rafts: HashMap<RangeId, openraft::Raft<TypeConfig>> = HashMap::new();
        let mut sm_kvs: HashMap<RangeId, Arc<dyn kv::Kv>> = HashMap::new();
        let mut engines: HashMap<RangeId, Arc<SqlEngine>> = HashMap::new();

        for range in cfg.range_map.range_ids() {
            let log = DurableLogStore::open(&store, range).expect("durable log");
            let sm = DurableStateMachineStore::open(&store, range).expect("durable sm");
            let sm_kv = sm.sm_kv();

            // Range-aware network: every client minted from `net` tags its RPCs
            // with `range`, so the peer's server routes to the matching group.
            let net = TcpRaftNetwork {
                from: cfg.id,
                range,
                partition: partition.clone(),
            };
            let raft = openraft::Raft::new(cfg.id, raft_config(), net, log, sm)
                .await
                .expect("raft::new");

            // Register THIS group so inbound `(range, id)` RPCs reach it.
            registry.register(range, cfg.id, raft.clone());

            // Replicated engine for this range. Data writes/reads hit this range's
            // store; schema always resolves from range 0's catalog store.
            let engine = Arc::new(
                SqlEngine::replicated(
                    catalog_kv.clone(),
                    sm_kv.clone(),
                    Arc::new(RaftCommitter { raft: raft.clone() }),
                    Arc::new(RaftLinearizer { raft: raft.clone() }),
                )
                .expect("replicated engine"),
            );
            // Reseed THIS range's counters on its own follower→leader edge.
            tokio::spawn(reseed_on_leadership(raft.clone(), engine.clone()));

            rafts.insert(range, raft);
            sm_kvs.insert(range, sm_kv);
            engines.insert(range, engine);
        }

        // One node-protocol listener for the whole node; it resolves the target
        // group from the registry by the RPC's `(range, from)`.
        let node_listener = TcpListener::bind(&cfg.node_addr).await?;
        tokio::spawn(serve_node_protocol(
            node_listener,
            registry.clone(),
            partition.clone(),
            shutdown.clone(),
        ));

        // Bootstrap EVERY range's voting group once peers are dialable. Each range
        // shares the same physical peer set (co-located placement).
        if cfg.bootstrap {
            for raft in rafts.values() {
                tokio::spawn(bootstrap(raft.clone(), cfg.peers.clone()));
            }
        }

        // SQL listener. A single-range node keeps the leader-routing fast-path
        // (`serve_routed`: serve locally if leader, else byte-proxy to the leader).
        // A multi-range node is a per-statement range gateway (`serve_range_routed`,
        // Task 3): each simple-query frame runs on the range's local-leader engine
        // or is forwarded to the remote leader through the seam (a `RejectForward`
        // stub here — Task 4 swaps in the pooled pgwire client).
        let sql_listener = TcpListener::bind(&cfg.sql_addr).await?;
        let sql_config = Arc::new(pgwire::session::SessionConfig::trust());
        if cfg.range_map.range_count() > 1 {
            tokio::spawn(crate::route::serve_range_routed(
                sql_listener,
                cfg.range_map.clone(),
                engines.clone(),
                catalog_kv.clone(),
                Arc::new(crate::range::router::RejectForward),
                sql_config,
            ));
        } else {
            tokio::spawn(crate::route::serve_routed(
                sql_listener,
                rafts[&0].clone(),
                engines[&0].clone(),
                sql_config,
            ));
        }

        Ok(Self {
            rafts,
            engines,
            partition,
            shutdown,
            sm_kvs,
            id: cfg.id,
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
    use crate::range::RangeMap;

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
            range_map: RangeMap::single(),
        })
        .await
        .expect("start node");

        // A one-node group elects immediately after `initialize`.
        node.rafts[&0]
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

    /// A single process running ONE node that hosts a 2-range map brings up BOTH
    /// ranges, and each range independently self-confirms a leader via openraft's
    /// event-based `wait` (state==Leader && current_leader==self) — no sleep.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_range_node_elects_a_leader_per_range() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node_addr = free_port().await;
        let sql_addr = free_port().await;
        // boundary at table_id 1 ⇒ range 0 = [0,1), range 1 = [1,∞). Two ranges.
        let node = ServerNode::start(NodeConfig {
            id: 0,
            node_addr: node_addr.clone(),
            sql_addr,
            data_dir: dir.path().to_path_buf(),
            peers: vec![(0, node_addr.clone())],
            bootstrap: true,
            range_map: RangeMap::with_boundaries(vec![1]),
        })
        .await
        .expect("start multi-range node");

        // Both ranges must self-confirm a leader. A one-node group elects itself
        // immediately after its `initialize`; we await that condition per range.
        assert_eq!(node.rafts.len(), 2, "node hosts a Raft instance per range");
        for raft in node.rafts.values() {
            raft.wait(Some(Duration::from_secs(10)))
                .metrics(
                    |m| m.state == ServerState::Leader && m.current_leader == Some(m.id),
                    "range self-confirmed as leader",
                )
                .await
                .expect("each range self-confirms a leader");
        }
    }

    /// A write committed to range 1's Raft group lands in range 1's `data-r1`
    /// keyspace and is ABSENT from range 0's `data-r0` keyspace — structural
    /// per-range storage isolation, asserted over BOTH keyspaces.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_write_to_range1_is_isolated_to_data_r1() {
        let dir = tempfile::tempdir().expect("tempdir");
        let node_addr = free_port().await;
        let sql_addr = free_port().await;
        let node = ServerNode::start(NodeConfig {
            id: 0,
            node_addr: node_addr.clone(),
            sql_addr,
            data_dir: dir.path().to_path_buf(),
            peers: vec![(0, node_addr.clone())],
            bootstrap: true,
            range_map: RangeMap::with_boundaries(vec![1]),
        })
        .await
        .expect("start multi-range node");

        // Await range 1's self-confirmed leadership before proposing to it.
        let range1 = node.rafts[&1].clone();
        range1
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                |m| m.state == ServerState::Leader && m.current_leader == Some(m.id),
                "range 1 leader",
            )
            .await
            .expect("range 1 leader");

        // Propose a marker row directly through range 1's Raft `client_write`
        // (no SQL gateway yet — that is Task 3). A `WriteBatch` of one Put commits
        // to range 1's group, applies into `data-r1`.
        use crate::types::WriteBatch;
        use kv::WriteOp;
        let marker = kv::key::row_key(7, 1); // table 7 ⇒ range 1, but the key value
        // itself is what we assert on, so the table id only needs to be nonzero.
        range1
            .client_write(WriteBatch(vec![WriteOp::Put {
                key: marker.clone(),
                value: b"r1-only".to_vec(),
            }]))
            .await
            .expect("commit to range 1");

        // Range 1's applied store has it; range 0's applied store does NOT — the
        // two keyspaces are physically distinct (`data-r1` vs `data-r0`).
        let data_r1 = node.sm_kv(1);
        let data_r0 = node.sm_kv(0);
        assert_eq!(
            data_r1.get(&marker).expect("get r1"),
            Some(b"r1-only".to_vec()),
            "the write is present in range 1's data-r1 keyspace"
        );
        assert_eq!(
            data_r0.get(&marker).expect("get r0"),
            None,
            "the write is ABSENT from range 0's data-r0 keyspace (storage isolation)"
        );

        // Criterion 3 also requires isolation across the RAFT keyspaces, not just
        // `data-r{r}`. The commit appended an entry to range 1's `raft-r1` and
        // applied it, advancing range 1's `last_applied`; range 0's `raft-r0` saw
        // only its own bootstrap entries (no data write), so its `last_applied`
        // did not advance from this write. We read both via openraft metrics — a
        // borrow of the metrics watch, no sleep.
        let r1_applied = node.rafts[&1].metrics().borrow().last_applied;
        let r0_applied = node.rafts[&0].metrics().borrow().last_applied;
        let r1_idx = r1_applied.expect("range 1 has applied entries").index;
        assert!(
            r1_idx > 0,
            "range 1's raft-r1 advanced last_applied past bootstrap after the write"
        );
        // Range 0 only ever applied its single-node bootstrap; the range-1 write
        // never touched `raft-r0`. Its applied index is strictly below range 1's,
        // proving the raft keyspaces are isolated too (no shared log/SM state).
        let r0_idx = r0_applied.map(|l| l.index).unwrap_or(0);
        assert!(
            r0_idx < r1_idx,
            "range 0's raft-r0 did not advance from range 1's write (raft-keyspace isolation)"
        );
    }
}
