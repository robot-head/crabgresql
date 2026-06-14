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
use crate::range::meta::{seed_if_absent, wait_for_range_map};
use crate::transport::client::TcpRaftNetwork;
use crate::transport::partition::PartitionState;
use crate::transport::server::{RangeRegistry, ShutdownSignal, serve_node_protocol};
use crate::types::{NodeId, TypeConfig};

/// A built-but-not-yet-Arc'd data range: `(range, raft, applied store, engine)`.
/// Collected during bring-up so the range-0 read barrier can be injected on each
/// engine (with `&mut`) once the per-range `rafts` map is complete.
type PendingRange = (
    RangeId,
    openraft::Raft<TypeConfig>,
    Arc<dyn kv::Kv>,
    SqlEngine,
);

/// Where a node's range layout comes from.
pub enum RangeLayout {
    /// Static config: the range map is fixed at startup, identical on every node
    /// (today's behavior — the SP9/SP10/SP13/SP14 path). The single-range default
    /// is `RangeLayout::Static(RangeMap::single())`.
    Static(RangeMap),
    /// Replicated: the authoritative range map is read from the meta range (range
    /// 0) at a two-phase bootstrap. `seed` is `Some` only on the bootstrap node,
    /// which writes it as the initial descriptor blob; a joining node passes
    /// `None` and learns the layout from the meta range.
    Replicated { seed: Option<RangeMap> },
}

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
    /// Where this node's range layout comes from. Defaults to
    /// `RangeLayout::Static(RangeMap::single())` (single range — the fast path).
    pub layout: RangeLayout,
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
    /// `host:port` the node-protocol listener is bound to (`cfg.node_addr`).
    node_addr: String,
    /// The authoritative range map this node brought up (Static config or the
    /// committed Replicated descriptor blob). Exposed so tests can assert a node
    /// derived its layout from the meta range rather than its own seed.
    pub range_map: RangeMap,
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

    /// The `host:port` address the node-protocol listener is bound to.
    pub fn node_addr(&self) -> &str {
        &self.node_addr
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

/// Bind a TCP listener, retrying briefly on `AddrInUse`. A node's configured port
/// can be transiently contended — by a `TIME_WAIT` socket from a prior instance, or
/// (in tests) by the `free_port` bind-drop-rebind idiom racing a concurrent binder
/// under heavy contention. Bounded, so a genuinely-occupied port still fails fast.
async fn bind_with_retry(addr: &str) -> std::io::Result<TcpListener> {
    let mut attempts = 0;
    loop {
        match TcpListener::bind(addr).await {
            Ok(l) => return Ok(l),
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse && attempts < 20 => {
                attempts += 1;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Build one range's Raft group + applied store + replicated engine over its
/// per-range keyspaces, register it in the `(range, node)` registry. Returns
/// `(raft, sm_kv, engine)` — engine is UN-Arc'd so the caller can mutate it
/// (e.g. `init_gtm_coordinator` on range 0) before Arc-wrapping. Callers are
/// responsible for spawning `reseed_on_leadership` after Arc-wrapping.
async fn build_range_group(
    store: &NodeStore,
    range: RangeId,
    id: NodeId,
    partition: &PartitionState,
    registry: &RangeRegistry,
    catalog_kv: &Arc<dyn kv::Kv>,
) -> (openraft::Raft<TypeConfig>, Arc<dyn kv::Kv>, SqlEngine) {
    let log = DurableLogStore::open(store, range).expect("durable log");
    let sm = DurableStateMachineStore::open(store, range).expect("durable sm");
    // Annotate as the trait object so the returned trio coerces cleanly (the
    // original inlined code relied on the HashMap-insert site for this coercion).
    let sm_kv: Arc<dyn kv::Kv> = sm.sm_kv();

    let net = TcpRaftNetwork {
        from: id,
        range,
        partition: partition.clone(),
    };
    let raft = openraft::Raft::new(id, raft_config(), net, log, sm)
        .await
        .expect("raft::new");
    registry.register(range, id, raft.clone());

    let engine = SqlEngine::replicated(
        catalog_kv.clone(),
        sm_kv.clone(),
        Arc::new(RaftCommitter { raft: raft.clone() }),
        Arc::new(RaftLinearizer { raft: raft.clone() }),
    )
    .expect("replicated engine");

    (raft, sm_kv, engine)
}

impl ServerNode {
    pub async fn start(cfg: NodeConfig) -> std::io::Result<Self> {
        match cfg.layout {
            RangeLayout::Static(_) => Self::start_static(cfg).await,
            RangeLayout::Replicated { .. } => Self::start_replicated(cfg).await,
        }
    }

    async fn start_static(cfg: NodeConfig) -> std::io::Result<Self> {
        let RangeLayout::Static(map) = cfg.layout else {
            unreachable!("start_static called with non-static layout")
        };
        let store = NodeStore::open(&cfg.data_dir, &map).expect("open node store");
        let partition = PartitionState::default();
        let registry = RangeRegistry::new();
        let shutdown = ShutdownSignal::default();
        let catalog_kv = store.data_kv(0) as Arc<dyn kv::Kv>;

        let mut rafts: HashMap<RangeId, openraft::Raft<TypeConfig>> = HashMap::new();
        let mut sm_kvs: HashMap<RangeId, Arc<dyn kv::Kv>> = HashMap::new();
        let mut engines: HashMap<RangeId, Arc<SqlEngine>> = HashMap::new();

        // Range 0 is special: init the GTM coordinator before Arc-wrapping.
        let (r0_raft, r0_sm_kv, mut r0_engine) =
            build_range_group(&store, 0, cfg.id, &partition, &registry, &catalog_kv).await;
        r0_engine
            .init_gtm_coordinator()
            .expect("init GTM coordinator over range 0");
        let r0_engine = Arc::new(r0_engine);
        tokio::spawn(reseed_on_leadership(r0_raft.clone(), r0_engine.clone()));
        rafts.insert(0, r0_raft);
        sm_kvs.insert(0, r0_sm_kv);
        engines.insert(0, r0_engine);

        // Data ranges (all ranges except 0). Build each trio FIRST (engines still
        // un-Arc'd so the range-0 barrier can be injected with `&mut`), collecting
        // them until `rafts` is complete — the barrier's `TwoPcClient` needs the
        // full per-range raft map.
        let mut pending: Vec<PendingRange> = Vec::new();
        for range in map.range_ids().filter(|&r| r != 0) {
            let (raft, sm_kv, engine) =
                build_range_group(&store, range, cfg.id, &partition, &registry, &catalog_kv).await;
            rafts.insert(range, raft.clone());
            pending.push((range, raft, sm_kv, engine));
        }
        // Cross-node visibility: every DATA-range engine gets a range-0 read barrier
        // so its cross-range resolver reads a caught-up local range-0 replica. Range
        // 0's own engine needs none (it reads its own current store). Built over the
        // now-complete `rafts` map.
        let barrier_client = crate::twopc::TwoPcClient::new(rafts.clone(), partition.clone());
        for (range, raft, sm_kv, mut engine) in pending {
            let barrier: Arc<dyn executor::Linearizer> = Arc::new(
                crate::twopc::Range0Barrier::new(rafts[&0].clone(), cfg.id, barrier_client.clone()),
            );
            engine.set_range0_barrier(barrier);
            let engine = Arc::new(engine);
            tokio::spawn(reseed_on_leadership(raft.clone(), engine.clone()));
            sm_kvs.insert(range, sm_kv);
            engines.insert(range, engine);
        }

        let node_listener = bind_with_retry(&cfg.node_addr).await?;
        let txn = crate::twopc::TxnService::new(engines.clone());
        // Spawn a per-range leadership-loss watcher: on the falling edge of this
        // node's leadership for a range, release (presumed-abort) every held 2PC
        // session for that range so locks are freed promptly. TxnService is Clone
        // (shares its Arc held-map), so every watcher shares the same registry.
        for (&range, raft) in &rafts {
            tokio::spawn(release_on_leadership_loss(
                raft.clone(),
                range,
                cfg.id,
                txn.clone(),
            ));
        }
        tokio::spawn(serve_node_protocol(
            node_listener,
            registry.clone(),
            partition.clone(),
            shutdown.clone(),
            Some(txn),
        ));

        if cfg.bootstrap {
            for raft in rafts.values() {
                tokio::spawn(bootstrap(raft.clone(), cfg.peers.clone()));
            }
        }

        let sql_listener = bind_with_retry(&cfg.sql_addr).await?;
        let sql_config = Arc::new(pgwire::session::SessionConfig::trust());
        Self::spawn_sql(
            sql_listener,
            &map,
            &rafts,
            &engines,
            &partition,
            &catalog_kv,
            cfg.id,
            sql_config,
        );

        Ok(Self {
            rafts,
            engines,
            partition,
            shutdown,
            sm_kvs,
            id: cfg.id,
            node_addr: cfg.node_addr,
            range_map: map,
        })
    }

    /// Spawn the SQL listener: the per-statement range gateway for a multi-range
    /// node, or the single-range leader-routing fast path for one range.
    #[allow(clippy::too_many_arguments)]
    fn spawn_sql(
        sql_listener: TcpListener,
        map: &RangeMap,
        rafts: &HashMap<RangeId, openraft::Raft<TypeConfig>>,
        engines: &HashMap<RangeId, Arc<SqlEngine>>,
        partition: &PartitionState,
        catalog_kv: &Arc<dyn kv::Kv>,
        id: NodeId,
        sql_config: Arc<pgwire::session::SessionConfig>,
    ) {
        if map.range_count() > 1 {
            Self::spawn_sql_gateway(
                sql_listener,
                map,
                rafts,
                engines,
                partition,
                catalog_kv,
                id,
                sql_config,
            );
        } else {
            tokio::spawn(crate::route::serve_routed(
                sql_listener,
                rafts[&0].clone(),
                engines[&0].clone(),
                sql_config,
            ));
        }
    }

    /// Spawn the SQL listener as the per-statement range gateway, unconditionally
    /// (Replicated mode — see `start_replicated`).
    #[allow(clippy::too_many_arguments)]
    fn spawn_sql_gateway(
        sql_listener: TcpListener,
        map: &RangeMap,
        rafts: &HashMap<RangeId, openraft::Raft<TypeConfig>>,
        engines: &HashMap<RangeId, Arc<SqlEngine>>,
        partition: &PartitionState,
        catalog_kv: &Arc<dyn kv::Kv>,
        id: NodeId,
        sql_config: Arc<pgwire::session::SessionConfig>,
    ) {
        let pool = crate::forward::ForwardPool::new(
            rafts.clone(),
            partition.clone(),
            crate::forward::RetryCounter::default(),
        );
        let forward: Arc<dyn crate::range::router::RemoteForward> =
            Arc::new(crate::forward::PgwireForward { pool });
        let leads: Arc<dyn crate::range::router::LeadsRange> = Arc::new(NodeLeadership {
            rafts: rafts.clone(),
            id,
        });
        // Cross-range 2PC over the network: every global op (begin/stage/commit/
        // release) is an RPC to the relevant range's leader — even to self via
        // loopback — so the path is uniform whether or not this gateway leads it.
        let client = crate::twopc::TwoPcClient::new(rafts.clone(), partition.clone());
        let coordinator: Arc<dyn crate::range::router::GlobalCoordinator> =
            Arc::new(crate::twopc::NetCoordinator::new(client));
        tokio::spawn(crate::route::serve_range_routed(
            sql_listener,
            map.clone(),
            engines.clone(),
            leads,
            catalog_kv.clone(),
            forward,
            coordinator,
            sql_config,
        ));
    }

    /// How long the two-phase bootstrap waits for range 0 to elect + the
    /// descriptor blob to apply. Long enough to survive a slow CI election; a
    /// genuinely stuck cluster fails the start rather than hanging forever.
    async fn start_replicated(cfg: NodeConfig) -> std::io::Result<Self> {
        let RangeLayout::Replicated { seed } = cfg.layout else {
            unreachable!("start_replicated called with non-replicated layout")
        };
        let boot_timeout = Duration::from_secs(60);

        // Phase 1: bring up range 0 (the meta range) ONLY, from the static seed.
        let mut store =
            NodeStore::open(&cfg.data_dir, &RangeMap::single()).expect("open node store");
        let partition = PartitionState::default();
        let registry = RangeRegistry::new();
        let shutdown = ShutdownSignal::default();
        let catalog_kv = store.data_kv(0) as Arc<dyn kv::Kv>;

        let mut rafts: HashMap<RangeId, openraft::Raft<TypeConfig>> = HashMap::new();
        let mut sm_kvs: HashMap<RangeId, Arc<dyn kv::Kv>> = HashMap::new();
        let mut engines: HashMap<RangeId, Arc<SqlEngine>> = HashMap::new();

        let (r0_raft, r0_sm_kv, mut r0_engine) =
            build_range_group(&store, 0, cfg.id, &partition, &registry, &catalog_kv).await;
        r0_engine
            .init_gtm_coordinator()
            .expect("init GTM coordinator over range 0");
        let r0_engine = Arc::new(r0_engine);
        tokio::spawn(reseed_on_leadership(r0_raft.clone(), r0_engine.clone()));
        rafts.insert(0, r0_raft.clone());
        sm_kvs.insert(0, r0_sm_kv);
        engines.insert(0, r0_engine);

        // Replicated path: the node listener binds before the full engines map is
        // built (only range 0 is ready), so it hosts no 2PC service yet — cross-range
        // 2PC over the replicated layout is future work. The static path (used by the
        // multi-process e2e) wires Some(txn).
        let node_listener = bind_with_retry(&cfg.node_addr).await?;
        tokio::spawn(serve_node_protocol(
            node_listener,
            registry.clone(),
            partition.clone(),
            shutdown.clone(),
            None,
        ));

        // Bootstrap range 0's voting group (bootstrap node only) and, once we lead
        // it, seed the descriptor blob if absent. A bootstrap node defines the
        // cluster, so it must seed SOME layout; an absent seed (a direct caller
        // passing `seed: None` with `bootstrap: true`) defaults to a single range
        // rather than hanging the whole cluster on the blob wait.
        if cfg.bootstrap {
            tokio::spawn(bootstrap(r0_raft.clone(), cfg.peers.clone()));
            let seed_map = seed.unwrap_or_else(RangeMap::single);
            r0_raft
                .wait(Some(boot_timeout))
                .metrics(|m| m.current_leader == Some(cfg.id), "self range-0 leader")
                .await
                .map_err(|e| std::io::Error::other(format!("await range-0 leadership: {e}")))?;
            // create-if-absent: writes the blob once, at create; a restart finds it
            // present and does not rewrite it (the write-once invariant).
            seed_if_absent(&r0_raft, catalog_kv.as_ref(), &seed_map).await?;
        }

        // Phase 2: every node waits for the committed blob to apply locally, then
        // decodes the authoritative map.
        let map = wait_for_range_map(&r0_raft, catalog_kv.as_ref(), boot_timeout).await?;

        // Build each data range named by the descriptors. Collect the trios FIRST
        // (engines un-Arc'd) so the range-0 barrier can be injected once `rafts` is
        // complete — the barrier's `TwoPcClient` needs the full per-range raft map.
        let mut pending: Vec<PendingRange> = Vec::new();
        for range in map.range_ids().filter(|&r| r != 0) {
            store.open_range(range).expect("open data range keyspace");
            let (raft, sm_kv, engine) =
                build_range_group(&store, range, cfg.id, &partition, &registry, &catalog_kv).await;
            if cfg.bootstrap {
                tokio::spawn(bootstrap(raft.clone(), cfg.peers.clone()));
            }
            rafts.insert(range, raft.clone());
            pending.push((range, raft, sm_kv, engine));
        }
        // Cross-node visibility: every DATA-range engine gets a range-0 read barrier
        // (range 0's own engine reads its own current store, so it needs none).
        let barrier_client = crate::twopc::TwoPcClient::new(rafts.clone(), partition.clone());
        for (range, raft, sm_kv, mut engine) in pending {
            let barrier: Arc<dyn executor::Linearizer> = Arc::new(
                crate::twopc::Range0Barrier::new(rafts[&0].clone(), cfg.id, barrier_client.clone()),
            );
            engine.set_range0_barrier(barrier);
            let engine = Arc::new(engine);
            tokio::spawn(reseed_on_leadership(raft.clone(), engine.clone()));
            sm_kvs.insert(range, sm_kv);
            engines.insert(range, engine);
        }

        // A replicated node always serves through the gateway (even at one range):
        // the layout is dynamic in principle, so the byte-proxy fast path — which is
        // a static single-range optimization — does not apply here.
        let sql_listener = bind_with_retry(&cfg.sql_addr).await?;
        let sql_config = Arc::new(pgwire::session::SessionConfig::trust());
        Self::spawn_sql_gateway(
            sql_listener,
            &map,
            &rafts,
            &engines,
            &partition,
            &catalog_kv,
            cfg.id,
            sql_config,
        );

        Ok(Self {
            rafts,
            engines,
            partition,
            shutdown,
            sm_kvs,
            id: cfg.id,
            node_addr: cfg.node_addr,
            range_map: map,
        })
    }
}

/// The real per-node leadership predicate for the gateway: this node leads `range`
/// iff range `range`'s Raft currently reports `current_leader == Some(self.id)`.
/// Backed by the per-range raft handles. `leads` borrows the metrics watch, reads
/// `current_leader`, and drops the `Ref` before returning — it is synchronous, so
/// no `Ref` is held across an `.await` in the caller (`RangeRouter::run_on`).
struct NodeLeadership {
    rafts: HashMap<RangeId, openraft::Raft<TypeConfig>>,
    id: NodeId,
}

impl crate::range::router::LeadsRange for NodeLeadership {
    fn leads(&self, range: RangeId) -> bool {
        self.rafts
            .get(&range)
            .map(|r| r.metrics().borrow().current_leader == Some(self.id))
            .unwrap_or(false)
    }
}

/// Drop every held 2PC session for `range` on the FALLING edge of this node's
/// leadership (follower/candidate after having been leader). Sessions are presumed-
/// aborted; the global clog remains the sole arbiter of commit/abort status. No
/// sleep — mirrors `reseed_on_leadership`'s `raft.metrics()` + `rx.changed()` loop.
async fn release_on_leadership_loss(
    raft: openraft::Raft<TypeConfig>,
    range: RangeId,
    id: NodeId,
    txn: crate::twopc::TxnService,
) {
    let mut rx = raft.metrics();
    let mut was_leader = false;
    loop {
        let is_leader = rx.borrow().current_leader == Some(id);
        if was_leader && !is_leader {
            txn.release_all_for_range(range).await; // free held locks; global clog is the arbiter
        }
        was_leader = is_leader;
        if rx.changed().await.is_err() {
            return;
        }
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
            let _ = engine.reseed_gtm(); // lift next_global past every durable allocation
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

/// Start a self-bootstrapping two-range node and wait until it leads every range.
/// Used by unit tests in `twopc` that need a live in-process node without going
/// through the integration-test gateway. ONE node, id 0, `RangeMap::with_boundaries(vec![2])`.
#[cfg(test)]
pub(crate) async fn testonly_two_range_node() -> (ServerNode, String) {
    use crate::range::RangeMap;
    let mut attempts = 0u32;
    loop {
        let node_addr = {
            let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let a = l.local_addr().expect("local_addr").to_string();
            drop(l);
            a
        };
        let sql_addr = {
            let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let a = l.local_addr().expect("local_addr").to_string();
            drop(l);
            a
        };
        match ServerNode::start(NodeConfig {
            id: 0,
            node_addr: node_addr.clone(),
            sql_addr: sql_addr.clone(),
            data_dir: tempfile::tempdir().expect("tempdir").keep(),
            peers: vec![(0, node_addr.clone())],
            bootstrap: true,
            layout: RangeLayout::Static(RangeMap::with_boundaries(vec![2])),
        })
        .await
        {
            Ok(node) => {
                for raft in node.rafts.values() {
                    raft.wait(Some(Duration::from_secs(10)))
                        .metrics(
                            |m| m.state == ServerState::Leader && m.current_leader == Some(0),
                            "range self-confirmed leader",
                        )
                        .await
                        .expect("range elects within the bound");
                }
                return (node, sql_addr);
            }
            Err(_) => {
                attempts += 1;
                assert!(
                    attempts < 16,
                    "testonly_two_range_node: bind race did not clear"
                );
            }
        }
    }
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
            layout: RangeLayout::Static(RangeMap::single()),
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
            layout: RangeLayout::Static(RangeMap::with_boundaries(vec![1])),
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
            layout: RangeLayout::Static(RangeMap::with_boundaries(vec![1])),
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

        // Capture range 0's applied index once it has settled (its single-node
        // bootstrap entry applied), BEFORE the range-1 write — so the raft-keyspace
        // isolation check below is deterministic (assert range 0 did NOT advance),
        // not a brittle cross-range index race that flakes under CPU contention.
        node.rafts[&0]
            .wait(Some(Duration::from_secs(10)))
            .metrics(|m| m.last_applied.is_some(), "range 0 settled")
            .await
            .expect("range 0 settled");
        let r0_before = node.rafts[&0]
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0);

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
        // applied it, advancing range 1's `last_applied` past its bootstrap; range
        // 0's `raft-r0` saw no data write, so its `last_applied` is UNCHANGED from
        // the value captured before the write. Asserting "range 0 did not advance"
        // (rather than a cross-range `r0 < r1` index comparison) is deterministic:
        // a settled single-node group appends nothing without a client write, so
        // there is no timing race. Read via the metrics watch — no sleep.
        let r1_idx = node.rafts[&1]
            .metrics()
            .borrow()
            .last_applied
            .expect("range 1 has applied entries")
            .index;
        assert!(
            r1_idx > 0,
            "range 1's raft-r1 advanced last_applied past bootstrap after the write"
        );
        let r0_after = node.rafts[&0]
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0);
        assert_eq!(
            r0_after, r0_before,
            "range 0's raft-r0 did not advance from range 1's write (raft-keyspace isolation)"
        );
    }
}
