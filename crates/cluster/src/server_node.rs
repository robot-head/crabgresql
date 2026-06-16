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

/// Bound for the settle-before-serve apply-wait (and the linearizable read that derives its
/// target) on a leadership rise. On timeout the gate stays CLOSED (writes keep getting a
/// retryable NotLeader) and the sweep RE-TRIES on the next wake — never open the gate after a
/// failed settle.
const SETTLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Retry cadence for a CLOSED gate under continuous leadership. The rise sweep normally wakes
/// on a metrics change, but a FAILED settle may leave no further metrics change to wake it; this
/// caps the wait so a wedged-closed gate keeps retrying its settle. A recovery heartbeat (cf.
/// `participant_silence_sweeper`'s 500ms tick), NOT a settle-sleep.
const SETTLE_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

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
        // Cross-range recovery: a per-DATA-range client the leadership-rise sweep uses
        // to abort-race in-doubt `Prepared(-> g)` markers against range 0 (write-once).
        let sweep_client = crate::twopc::TwoPcClient::new(rafts.clone(), partition.clone());
        // Settle-before-serve gate (SP22): ONE gate per bring-up, shared via `Arc` into
        // the TxnService, the gateway router, and (Task 3) the rise sweeps. Each data
        // range is registered (gated-by-default) BELOW, BEFORE its rise-sweep spawn.
        let gate = crate::recovery_gate::RecoveryGate::new(cfg.id);
        for (range, raft, sm_kv, mut engine) in pending {
            let barrier: Arc<dyn executor::Linearizer> = Arc::new(
                crate::twopc::Range0Barrier::new(rafts[&0].clone(), cfg.id, barrier_client.clone()),
            );
            engine.set_range0_barrier(barrier);
            let engine = Arc::new(engine);
            // Register the range (gated-by-default) BEFORE spawning its rise sweep, so a
            // sweep's `mark_served` never no-ops on an unregistered range and wedges the
            // gate closed forever on a stable single leader (no second rising edge).
            gate.register_range(range, raft.clone());
            tokio::spawn(reseed_on_leadership(raft.clone(), engine.clone()));
            // On THIS data-range's leadership rising edge, finalize a failed-over
            // participant's durable in-doubt markers (range 0 holds only global xids),
            // then OPEN the recovery gate for the settled term.
            tokio::spawn(resolve_in_doubt_on_leadership(
                raft.clone(),
                range,
                cfg.id,
                engine.clone(),
                sweep_client.clone(),
                gate.clone(),
            ));
            sm_kvs.insert(range, sm_kv);
            engines.insert(range, engine);
        }

        // SP23: register range 0 in the gate and spawn its rise sweep, mirroring the data
        // ranges above. Range 0 is built specially OUTSIDE the pending loop, so its
        // registration happens here, AFTER `gate`/`sweep_client` exist and (load-bearing)
        // BEFORE `serve_node_protocol` below — so a node that becomes range-0 leader can never
        // serve `BeginGlobal` while its gate reads the unregistered-default `true` (ungated).
        // The GTM coordinator path (Begin/CommitGlobal/GlobalBarrier in handle_txn) does NOT go
        // through the gated stage/dispatch DML, so a closed range-0 gate cannot deadlock it; it
        // gates only range-0 PARTICIPANT writes (the SP22 stage/dispatch checks).
        gate.register_range(0, rafts[&0].clone());
        tokio::spawn(resolve_in_doubt_on_leadership(
            rafts[&0].clone(),
            0,
            cfg.id,
            engines[&0].clone(),
            sweep_client.clone(),
            gate.clone(),
        ));

        let node_listener = bind_with_retry(&cfg.node_addr).await?;
        let txn = crate::twopc::TxnService::new(engines.clone(), Some(gate.clone()));
        // Spawn a per-range leadership-loss watcher: on the falling edge of this node's
        // leadership for a range, resolve-then-release every held 2PC session for that
        // range (drive each `g` to its global decision, then release per the decision) so
        // a lock is never freed while its `g` is in-doubt. TxnService is Clone (shares its
        // Arc held-map), so every watcher shares the same registry.
        let loss_client = crate::twopc::TwoPcClient::new(rafts.clone(), partition.clone());
        for (&range, raft) in &rafts {
            tokio::spawn(release_on_leadership_loss(
                raft.clone(),
                range,
                cfg.id,
                txn.clone(),
                loss_client.clone(),
            ));
        }
        // Coordinator-silence recovery: a per-node heartbeat self-resolves held 2PC
        // sessions whose coordinator crashed after staging but before deciding. The
        // sweeper shares the SAME Arc held-map as the listener (TxnService is Clone),
        // so it sees the sessions the listener parks. `txn.clone()` MUST precede the
        // `Some(txn)` move below.
        let sweeper_client = crate::twopc::TwoPcClient::new(rafts.clone(), partition.clone());
        tokio::spawn(participant_silence_sweeper(txn.clone(), sweeper_client));
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
            Some(gate.clone()),
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
        gate: Option<Arc<crate::recovery_gate::RecoveryGate>>,
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
                gate,
            );
        } else {
            // Single-range byte-proxy fast path: no gateway router → ungated.
            let _ = gate;
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
        gate: Option<Arc<crate::recovery_gate::RecoveryGate>>,
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
            gate,
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

        // The node listener binds with only range 0 ready, but it DOES host the 2PC
        // service from the start: `TxnService`'s engines map is growable, so global/
        // coordinator ops (Begin/Commit/GlobalBarrier — they need only engine(0)) work
        // immediately, and data ranges are registered live in Phase 2. The listener
        // must stay bound throughout Phase 2 to serve range-0 Raft RPCs.
        // Settle-before-serve gate (SP22): ONE growable gate, constructed here with only
        // range 0 known; each data range is registered live in the Phase-2 loop. Shared
        // via `Arc` into the TxnService, the gateway router, and (Task 3) the rise sweeps.
        let gate = crate::recovery_gate::RecoveryGate::new(cfg.id);
        let txn = crate::twopc::TxnService::new(engines.clone(), Some(gate.clone()));

        // SP23 (LOAD-BEARING): register range 0 in the gate and spawn its rise sweep BEFORE
        // `serve_node_protocol` below binds the listener that serves `BeginGlobal`. The Phase-2
        // all-ranges `sweep_client` is not built until much later (after `wait_for_range_map`),
        // which would leave `BeginGlobal` UNGATED across that whole window — a node that becomes
        // range-0 leader there would read the unregistered-default `is_serving(0) == true` and
        // hand out a reused global xid. Range 0's sweep only resolves range 0 (self-loopback
        // abort-races against its own clog), so a range-0-ONLY `TwoPcClient` suffices here. The
        // GTM coordinator path (Begin/CommitGlobal/GlobalBarrier in handle_txn) bypasses the
        // gated stage/dispatch DML, so a closed range-0 gate cannot deadlock the coordinator.
        let r0_sweep_client = crate::twopc::TwoPcClient::new(
            HashMap::from([(0, r0_raft.clone())]),
            partition.clone(),
        );
        gate.register_range(0, r0_raft.clone());
        tokio::spawn(resolve_in_doubt_on_leadership(
            r0_raft.clone(),
            0,
            cfg.id,
            engines[&0].clone(),
            r0_sweep_client,
            gate.clone(),
        ));

        let node_listener = bind_with_retry(&cfg.node_addr).await?;
        tokio::spawn(serve_node_protocol(
            node_listener,
            registry.clone(),
            partition.clone(),
            shutdown.clone(),
            Some(txn.clone()),
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
        // Cross-range recovery: the per-DATA-range client the leadership-rise sweep uses
        // to abort-race in-doubt `Prepared(-> g)` markers against range 0 (write-once).
        let sweep_client = crate::twopc::TwoPcClient::new(rafts.clone(), partition.clone());
        for (range, raft, sm_kv, mut engine) in pending {
            let barrier: Arc<dyn executor::Linearizer> = Arc::new(
                crate::twopc::Range0Barrier::new(rafts[&0].clone(), cfg.id, barrier_client.clone()),
            );
            engine.set_range0_barrier(barrier);
            let engine = Arc::new(engine);
            // Register this data range (gated-by-default) in the SP22 gate BEFORE both
            // `register_engine` and the rise-sweep spawn, so a sweep's `mark_served`
            // never no-ops on an unregistered range and wedges the gate closed forever.
            gate.register_range(range, raft.clone());
            // Register this data range so the listener can serve Stage/Release for it.
            txn.register_engine(range, engine.clone());
            tokio::spawn(reseed_on_leadership(raft.clone(), engine.clone()));
            // On THIS data-range's leadership rising edge, finalize a failed-over
            // participant's durable in-doubt markers, then OPEN the recovery gate for
            // the settled term.
            tokio::spawn(resolve_in_doubt_on_leadership(
                raft.clone(),
                range,
                cfg.id,
                engine.clone(),
                sweep_client.clone(),
                gate.clone(),
            ));
            sm_kvs.insert(range, sm_kv);
            engines.insert(range, engine);
        }

        // Per-range leadership-loss resolve-then-release: when this node loses a range's
        // leadership, drive each held `g` to its global decision and release per that
        // decision, so a lock is never freed while its `g` is in-doubt. All watchers share
        // the same Arc held-map.
        let loss_client = crate::twopc::TwoPcClient::new(rafts.clone(), partition.clone());
        for (&range, raft) in &rafts {
            tokio::spawn(release_on_leadership_loss(
                raft.clone(),
                range,
                cfg.id,
                txn.clone(),
                loss_client.clone(),
            ));
        }
        // Coordinator-silence recovery: a per-node heartbeat self-resolves held 2PC
        // sessions whose coordinator crashed after staging but before deciding.
        let sweeper_client = crate::twopc::TwoPcClient::new(rafts.clone(), partition.clone());
        tokio::spawn(participant_silence_sweeper(txn.clone(), sweeper_client));

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
            Some(gate.clone()),
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
    client: std::sync::Arc<crate::twopc::TwoPcClient>,
) {
    let mut rx = raft.metrics();
    let mut was_leader = false;
    loop {
        let is_leader = rx.borrow().current_leader == Some(id);
        if was_leader && !is_leader {
            // Resolve-then-release: drive each held `g` through its WRITE-ONCE global
            // decision and release per that decision, so a lock is NEVER freed while its
            // `g` is in-doubt (the `eval_plan_qual` invariant). A blind `abort_release`
            // here freed the lock pre-decision, letting a concurrent writer create a
            // second, non-superseding version of the row → two live versions on commit.
            txn.resolve_and_release_for_range(&client, range).await;
        }
        was_leader = is_leader;
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// Settle-before-serve rise sweep (SP22). While this node leads `range` but its recovery
/// gate is still closed for the current term, apply-wait through this term's committed index,
/// then finalize any in-doubt `Prepared(-> g)` markers in this range's durable clog whose
/// coordinator died: abort-race each undecided `g` against range 0 (write-once). On a clean
/// settle, `mark_served` OPENS the gate so writes to `range` are admitted; on any error or
/// timeout the gate stays CLOSED and the sweep retries on the next wake — never opened after a
/// failed settle. Heals a failed-over participant so its rows resolve (invisible for
/// presumed-abort) rather than staying in-doubt forever. No sleep — wakes on a metrics change,
/// capped at `SETTLE_RETRY_INTERVAL` so a wedged-closed gate keeps retrying.
async fn resolve_in_doubt_on_leadership(
    raft: openraft::Raft<TypeConfig>,
    range: RangeId,
    id: NodeId,
    engine: Arc<SqlEngine>,
    client: std::sync::Arc<crate::twopc::TwoPcClient>,
    gate: Arc<crate::recovery_gate::RecoveryGate>,
) {
    use crate::transport::protocol::TxnRpc;
    let mut rx = raft.metrics();
    loop {
        // Re-fire while we lead `range` but its gate is still closed for the CURRENT term —
        // covers a fresh rise (sentinel != term) AND a FAILED prior settle (a wedged gate must
        // keep retrying under continuous leadership, or it deadlocks every write to `range`).
        // `is_serving` re-reads the live term, so a flap to a new term re-closes + re-settles.
        let is_leader = rx.borrow().current_leader == Some(id);
        // In the transiently-deposed window (`current_leader` still names self but
        // `state != Leader`) the body may fire, but `is_serving`'s stricter `state == Leader`
        // check keeps the gate closed and `ensure_linearizable` fast-fails (returns
        // `ForwardToLeader` once a higher term is seen) — a cheap no-op retry, not a hang.
        if is_leader && !gate.is_serving(range) {
            // The leadership term we are settling (read + drop the Ref before awaiting).
            let term = { rx.borrow().current_term };
            // Apply-wait: settle through this term's committed index so the durable clog
            // scan below sees EVERY inherited marker (closes the apply-lag miss). The
            // `ensure_linearizable` index is the committed no-op for this term. BOTH the
            // linearizable read and the apply-wait are bounded by SETTLE_TIMEOUT — a node that
            // wins then loses quorum must not freeze here. On any error / timeout, leave the
            // gate CLOSED and retry on the next wake — never open it after a failed settle.
            let settled = async {
                let wait_to = tokio::time::timeout(SETTLE_TIMEOUT, raft.ensure_linearizable())
                    .await
                    .map_err(|_| ())? // timed out
                    .map_err(|_| ())? // RaftError
                    .map(|l| l.index);
                raft.wait(Some(SETTLE_TIMEOUT))
                    .applied_index_at_least(wait_to, "settle-before-serve")
                    .await
                    .map_err(|_| ())?;
                // SP23: reseed the GTM (and local) counters from the now-applied store BEFORE
                // opening the gate, so a range-0 leader never hands out a reused global xid. The
                // apply-wait above guarantees the applied store reflects every committed
                // begin_global_durable advance. reseed_* is lift-only (never regresses). FAIL-CLOSED:
                // a reseed error aborts the settle so the gate stays CLOSED and retries — a
                // silently-failed reseed must NOT let `mark_served` open the gate against a
                // possibly-regressed counter.
                engine.reseed_gtm().map_err(|_| ())?;
                engine.reseed_counters().map_err(|_| ())?;
                let scan_lo = engine.clog_scan_lo().unwrap_or(0);
                let (gs, _) = engine
                    .in_doubt_globals_from(scan_lo)
                    .await
                    .map_err(|_| ())?;
                for g in gs {
                    if let Err(e) = client
                        .call(0, TxnRpc::CommitGlobal { g, commit: false })
                        .await
                    {
                        tracing::warn!(g, ?e, "recovery abort-race failed; g stays in-doubt");
                    }
                }
                // SETTLE-COMPLETE-BEFORE-SERVE: re-scan AFTER the abort-races and open the gate
                // only when EVERY inherited marker has been driven to a durable terminal decision
                // (the re-scan is empty). The abort-race above is best-effort — under an OVERLAPPING
                // (cascading) failover a `CommitGlobal` abort-race can fail to land (this leader
                // loses leadership again before it commits), leaving a marker in-doubt. Opening the
                // gate with a marker still in-doubt lets a new gated write supersede AROUND it (the
                // in-doubt version is invisible), and when that marker is later committed BOTH
                // versions go live: the MVCC at-most-one-live violation that tears the cross-range
                // bank total. If any marker remains in-doubt, FAIL the settle so the gate stays
                // CLOSED and the sweep retries once a stable leader can finalize them — genuine
                // settle-BEFORE-serve, converging instead of racing the window. Proven by the
                // Stateright model `crossrange_2pc_overlap_settle_model`.
                let (still_in_doubt, new_lo) = engine
                    .in_doubt_globals_from(scan_lo)
                    .await
                    .map_err(|_| ())?;
                if !still_in_doubt.is_empty() {
                    return Err(());
                }
                if let Err(e) = engine.advance_clog_scan_lo(new_lo).await {
                    tracing::debug!(new_lo, ?e, "watermark advance not durable; safe to re-scan");
                }
                Ok::<(), ()>(())
            }
            .await;
            if settled.is_ok() {
                // Every inherited marker for `term` is now terminal → open the gate.
                gate.mark_served(range, term);
            } else {
                // Settle failed (apply-wait timeout / not-yet-quorum / scan error): the gate
                // stays CLOSED and we retry on the next wake. Logged so a permanently-wedged
                // range (a leader that can never linearize) is observable rather than silently
                // rejecting every write.
                tracing::debug!(
                    range,
                    term,
                    "settle-before-serve did not complete; gate stays closed, will retry"
                );
            }
        }
        // Wake on the next metrics change, but cap the wait so a FAILED settle is retried even
        // when no further metrics change arrives. A dropped sender (raft shutdown) ends the
        // task. The cap is a recovery heartbeat, not a settle-sleep — a successful settle
        // leaves `is_serving` true so the body no-ops.
        if let Ok(Err(_)) = tokio::time::timeout(SETTLE_RETRY_INTERVAL, rx.changed()).await {
            return; // metrics sender dropped → raft gone
        }
    }
}

/// Periodically self-resolve held 2PC sessions whose coordinator has gone silent
/// (no decision within `PARTICIPANT_SILENCE_TIMEOUT`) against range 0's global clog.
/// Bounded cadence (a recovery heartbeat): each tick resolves only sessions already
/// past the timeout.
async fn participant_silence_sweeper(
    txn: crate::twopc::TxnService,
    client: std::sync::Arc<crate::twopc::TwoPcClient>,
) {
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(500));
    loop {
        tick.tick().await;
        txn.sweep_stale(&client, crate::twopc::PARTICIPANT_SILENCE_TIMEOUT)
            .await;
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
