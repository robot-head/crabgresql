//! Networked cross-range 2PC. This module holds the coordinator-side pooled
//! node-port client (`TwoPcClient`, mirroring `forward::ForwardPool`'s
//! leader-resolution + bounded retry but speaking the structured node protocol
//! instead of pgwire), the `GlobalCoordinator` impl that drives it
//! (`NetCoordinator`), the participant-side held-session registry (`TxnService`),
//! and the follower-ReadIndex range-0 read barrier (`Range0Barrier`).
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use executor::{ExecError, Linearizer, SqlEngine};
use pgwire::engine::Engine;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::range::RangeId;
use crate::range::router::GlobalCoordinator;
use crate::transport::frame::{read_msg, write_msg};
use crate::transport::partition::PartitionState;
use crate::transport::protocol::{NodeRequest, NodeResponse, TxnResp, TxnRpc};
use crate::types::{NodeId, TypeConfig};

const TXN_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a participant holds a staged-but-undecided session before it self-
/// resolves against range 0's global clog (presumed-abort if still in-doubt). Well
/// above normal commit latency so a healthy txn is never prematurely aborted.
pub(crate) const PARTICIPANT_SILENCE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct PooledConn {
    addr: String,
    stream: Arc<Mutex<TcpStream>>,
}

/// Sends `TxnRpc`s to the current leader of a target range; pools one node-port
/// connection per target node.
pub struct TwoPcClient {
    rafts: HashMap<RangeId, openraft::Raft<TypeConfig>>,
    partition: PartitionState,
    conns: Mutex<HashMap<NodeId, PooledConn>>,
}

impl TwoPcClient {
    pub fn new(
        rafts: HashMap<RangeId, openraft::Raft<TypeConfig>>,
        partition: PartitionState,
    ) -> Arc<Self> {
        Arc::new(Self {
            rafts,
            partition,
            conns: Mutex::new(HashMap::new()),
        })
    }

    fn resolve_leader(&self, range: RangeId) -> Option<(NodeId, String)> {
        let raft = self.rafts.get(&range)?;
        let metrics = raft.metrics();
        let (leader, addr) = {
            let m = metrics.borrow();
            let leader = m.current_leader;
            let addr = leader.and_then(|l| {
                m.membership_config
                    .membership()
                    .get_node(&l)
                    .map(|n| crate::addr::node_dial_addr(&n.addr).to_string())
            });
            (leader, addr)
        };
        let leader = leader?;
        if self.partition.blocked(leader) {
            return None;
        }
        Some((leader, addr?))
    }

    /// Event-driven (no sleep) wait for a resolvable leader — see forward::ForwardPool::await_leader for the metrics-lag rationale.
    async fn await_leader(&self, range: RangeId) -> Option<(NodeId, String)> {
        let raft = self.rafts.get(&range)?;
        let deadline = tokio::time::Instant::now() + TXN_TIMEOUT;
        loop {
            if let Some(found) = self.resolve_leader(range) {
                return Some(found);
            }
            let mut rx = raft.metrics();
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            if tokio::time::timeout(remaining, rx.changed()).await.is_err() {
                return None;
            }
        }
    }

    /// Send one `TxnRpc` to `target_range`'s leader, bounded re-resolve+retry once
    /// on `NotLeader`/wire failure.
    // Err(()) = transport/leader-resolution failure; a participant-level retryable is
    // carried as Ok(TxnResp::Retryable). NetCoordinator maps these to ExecError.
    pub async fn call(&self, target_range: RangeId, rpc: TxnRpc) -> Result<TxnResp, ()> {
        for attempt in 0..2 {
            let (leader, addr) = self.await_leader(target_range).await.ok_or(())?;
            let env = NodeRequest::Txn {
                range: target_range,
                rpc: rpc.clone(),
            };
            match self.exchange(leader, &addr, &env).await {
                Ok(TxnResp::NotLeader) if attempt == 0 => continue,
                Ok(resp) => return Ok(resp),
                Err(()) if attempt == 0 => continue,
                Err(()) => return Err(()),
            }
        }
        Err(())
    }

    async fn exchange(&self, leader: NodeId, addr: &str, env: &NodeRequest) -> Result<TxnResp, ()> {
        if self.partition.blocked(leader) {
            return Err(());
        }
        // Map lock held ONLY to get-or-dial + clone the per-conn handle out.
        let conn = {
            let mut conns = self.conns.lock().await;
            let needs_dial = conns.get(&leader).is_none_or(|c| c.addr != addr);
            if needs_dial {
                let stream = tokio::time::timeout(TXN_TIMEOUT, TcpStream::connect(addr))
                    .await
                    .map_err(|_| ())?
                    .map_err(|_| ())?;
                conns.insert(
                    leader,
                    PooledConn {
                        addr: addr.to_string(),
                        stream: Arc::new(Mutex::new(stream)),
                    },
                );
            }
            conns.get(&leader).expect("pooled conn present").clone()
        }; // map guard dropped here, before any network I/O

        // Per-connection lock: serializes only THIS leader's in-flight request.
        let mut stream = conn.stream.lock().await;
        let exchange = async {
            write_msg(&mut *stream, env).await?;
            read_msg::<_, NodeResponse>(&mut *stream).await
        };
        match tokio::time::timeout(TXN_TIMEOUT, exchange).await {
            Ok(Ok(NodeResponse::Txn(resp))) => Ok(resp),
            _ => {
                drop(stream);
                // Drop the poisoned conn so the next attempt redials — but only if
                // it is still the same handle (don't clobber a concurrent redial).
                let mut conns = self.conns.lock().await;
                if conns
                    .get(&leader)
                    .is_some_and(|c| Arc::ptr_eq(&c.stream, &conn.stream))
                {
                    conns.remove(&leader);
                }
                Err(())
            }
        }
    }
}

/// A follower-capable range-0 read barrier. Fetches range 0's linearizable applied
/// index from its leader (via the `GlobalBarrier` RPC), then blocks until this
/// node's local range-0 replica has applied through it — making a participant's
/// `global_status` reads of range 0's clog correct over the network. If this node
/// IS range 0's leader, a local `ensure_linearizable()` is authoritative (skip the
/// RPC).
pub struct Range0Barrier {
    range0: openraft::Raft<TypeConfig>,
    id: NodeId,
    client: Arc<TwoPcClient>,
}

impl Range0Barrier {
    pub fn new(range0: openraft::Raft<TypeConfig>, id: NodeId, client: Arc<TwoPcClient>) -> Self {
        Self { range0, id, client }
    }
}

#[async_trait::async_trait]
impl Linearizer for Range0Barrier {
    async fn ensure_readable(&self) -> Result<(), ExecError> {
        let leads0 = self.range0.metrics().borrow().current_leader == Some(self.id);
        let barrier_index = if leads0 {
            self.range0
                .ensure_linearizable()
                .await
                .map(|r| r.map(|l| l.index).unwrap_or(0))
                .map_err(|_| ExecError::Unavailable)?
        } else {
            match self.client.call(0, TxnRpc::GlobalBarrier).await {
                Ok(TxnResp::Barrier { applied_index }) => applied_index,
                Ok(TxnResp::NotLeader) => return Err(ExecError::NotLeader),
                _ => return Err(ExecError::Unavailable),
            }
        };
        self.range0
            .wait(Some(TXN_TIMEOUT))
            .applied_index_at_least(Some(barrier_index), "range-0 read barrier")
            .await
            .map(|_| ())
            .map_err(|_| ExecError::Unavailable)
    }
}

/// Networked coordinator: every global op is an RPC to the relevant range's
/// leader (range 0 for begin/commit, the participant range for stage/release).
/// Always RPCs — even to self via loopback — so the path is uniform.
pub struct NetCoordinator {
    client: Arc<TwoPcClient>,
}

impl NetCoordinator {
    pub fn new(client: Arc<TwoPcClient>) -> Self {
        Self { client }
    }
}

#[async_trait::async_trait]
impl GlobalCoordinator for NetCoordinator {
    async fn begin_global(&self) -> Result<u64, ExecError> {
        match self.client.call(0, TxnRpc::BeginGlobal).await {
            Ok(TxnResp::Began { g }) => Ok(g),
            Ok(TxnResp::NotLeader) => Err(ExecError::NotLeader),
            _ => Err(ExecError::Unavailable),
        }
    }
    async fn stage_remote(&self, g: u64, range: RangeId, sql: &str) -> Result<(), ExecError> {
        match self
            .client
            .call(
                range,
                TxnRpc::Stage {
                    g,
                    range,
                    sql: sql.to_string(),
                },
            )
            .await
        {
            Ok(TxnResp::Staged) => Ok(()),
            Ok(TxnResp::NotLeader) => Err(ExecError::NotLeader),
            Ok(TxnResp::Retryable) => Err(ExecError::SerializationFailure), // preserve 40001 retryability
            Ok(TxnResp::Err(e)) => Err(ExecError::Unsupported(e)),
            _ => Err(ExecError::Unavailable),
        }
    }
    async fn commit_global(&self, g: u64, commit: bool) -> Result<bool, ExecError> {
        match self
            .client
            .call(0, TxnRpc::CommitGlobal { g, commit })
            .await
        {
            Ok(TxnResp::Committed) => Ok(true),
            Ok(TxnResp::Aborted) => Ok(false),
            Ok(TxnResp::NotLeader) => Err(ExecError::NotLeader),
            _ => Err(ExecError::Unavailable),
        }
    }
    async fn release_remote(&self, g: u64, range: RangeId, commit: bool) -> Result<(), ExecError> {
        match self
            .client
            .call(range, TxnRpc::Release { g, range, commit })
            .await
        {
            Ok(TxnResp::Released) => Ok(()),
            Ok(TxnResp::NotLeader) => Err(ExecError::NotLeader),
            _ => Err(ExecError::Unavailable),
        }
    }
}

/// Participant-side held-session registry. Lives on each node; resolves the
/// node's per-range engines and keeps one `SqlSession` per in-flight `(G, range)`
/// it participates in, detached from any TCP connection so a later `Release(G)`
/// from a different connection finds it. Each session is its OWN `Arc<Mutex>` so
/// the map lock is held only for lookup/insert/remove — NEVER across session work
/// (holding the map lock across `session.run().await` would deadlock a Stage that
/// blocks on a row lock held by another g against the Release that frees it).
type HeldSession = Arc<Mutex<executor::SqlSession>>;

/// A held participant session + the instant it joined `g` (for the coordinator-
/// silence timeout). `joined_at` is set ONCE at first stage (or_insert_with gives
/// first-insert semantics); a re-Stage must NOT reset it, or a chatty coordinator
/// could keep a doomed txn alive forever.
struct HeldEntry {
    session: HeldSession,
    joined_at: tokio::time::Instant,
}

#[derive(Clone)]
pub struct TxnService {
    engines: Arc<ArcSwap<HashMap<RangeId, Arc<SqlEngine>>>>,
    held: Arc<Mutex<HashMap<(u64, RangeId), HeldEntry>>>,
    /// Settle-before-serve gate (SP22): `Some` on a real node, `None` for in-process /
    /// never-recovering test harnesses (treated as always-serving).
    // `#[allow(dead_code)]` until Task 4 reads it in the `stage` gate check; removed then.
    #[allow(dead_code)]
    gate: Option<Arc<crate::recovery_gate::RecoveryGate>>,
}

impl TxnService {
    pub fn new(
        engines: HashMap<RangeId, Arc<SqlEngine>>,
        gate: Option<Arc<crate::recovery_gate::RecoveryGate>>,
    ) -> Self {
        Self {
            engines: Arc::new(ArcSwap::from_pointee(engines)),
            held: Arc::new(Mutex::new(HashMap::new())),
            gate,
        }
    }

    /// Look up the engine for `range`. Returns an OWNED `Arc` (lock-free snapshot via
    /// `arc-swap`); `None` if this node does not (yet) host that range. `load()` yields a
    /// `Guard` that auto-derefs to `&Arc<HashMap>`; `get()` + `cloned()` produce an owned
    /// `Arc<SqlEngine>` and the `Guard` drops at end-of-expression — so no guard is ever
    /// held across an `await` (the load-bearing arc-swap discipline).
    pub fn engine(&self, range: RangeId) -> Option<Arc<SqlEngine>> {
        self.engines.load().get(&range).cloned()
    }

    /// Register a data-range engine so this node can serve `Stage`/`Release` for it.
    /// Copy-on-write (`rcu`) so concurrent lock-free readers never block; idempotent
    /// (re-registering replaces the entry). The `rcu` closure receives `&Arc<HashMap>`.
    pub fn register_engine(&self, range: RangeId, engine: Arc<SqlEngine>) {
        self.engines.rcu(|cur| {
            let mut m = (**cur).clone();
            m.insert(range, engine.clone());
            m
        });
    }

    #[cfg(test)]
    pub async fn holds(&self, g: u64, range: RangeId) -> bool {
        self.held.lock().await.contains_key(&(g, range))
    }

    /// Drop every held session for `range` WITHOUT consulting the global clog
    /// (presumed-abort, freeing locks immediately). This frees a lock while its `g` may
    /// still be in-doubt, so it is NOT used on leadership loss anymore (see
    /// `resolve_and_release_for_range` for that path + the invariant it preserves); it
    /// remains the explicit "drop-as-if-crashed" primitive used by recovery tests.
    pub async fn release_all_for_range(&self, range: RangeId) {
        let victims: Vec<HeldSession> = {
            let mut held = self.held.lock().await;
            let keys: Vec<(u64, RangeId)> =
                held.keys().copied().filter(|&(_, r)| r == range).collect();
            keys.into_iter()
                .filter_map(|k| held.remove(&k).map(|e| e.session))
                .collect()
        };
        for s in victims {
            s.lock().await.abort_release();
        }
    }

    /// On leadership LOSS of `range`, resolve every held `(g, range)` session through its
    /// WRITE-ONCE global decision and release per that decision — NOT a blind
    /// `abort_release`. This preserves the load-bearing invariant `eval_plan_qual` relies
    /// on (a participant frees a row's lock only AFTER its global decision is durable, so
    /// every `g` with a `Prepared` marker on a locked row has already settled). Freeing the
    /// lock while `g` is still in-doubt lets a concurrent writer acquire it and write a
    /// SECOND, non-superseding version of the row, so TWO versions resolve live when both
    /// `g`s later commit (the at-most-one-live violation that crashes reads). Reuses
    /// `resolve_in_doubt` (the same per-decision release the silence sweep uses), so a lock
    /// is never freed pre-decision. Best-effort: a session whose range 0 is momentarily
    /// unreachable stays held (lock retained — correct) and is resolved by the silence
    /// sweep or the next leadership loss.
    pub async fn resolve_and_release_for_range(&self, client: &TwoPcClient, range: RangeId) {
        let keys: Vec<(u64, RangeId)> = {
            let held = self.held.lock().await;
            held.keys().copied().filter(|&(_, r)| r == range).collect()
        };
        for (g, r) in keys {
            self.resolve_in_doubt(client, g, r).await;
        }
    }

    /// Resolve an in-doubt `(g, range)` against range 0 via the WRITE-ONCE abort-race:
    /// send CommitGlobal{commit:false}; the effective decision comes back (Committed if
    /// a coordinator already won, Aborted if we won). Release the held session per the
    /// decision. Idempotent: a missing entry is a no-op; a re-resolve hits write-once.
    async fn resolve_in_doubt(&self, client: &TwoPcClient, g: u64, range: RangeId) {
        let committed = match client
            .call(0, TxnRpc::CommitGlobal { g, commit: false })
            .await
        {
            Ok(TxnResp::Committed) => true,
            Ok(TxnResp::Aborted) => false,
            _ => return, // range 0 unreachable: leave it for the next sweep tick
        };
        let entry = { self.held.lock().await.remove(&(g, range)) };
        if let Some(entry) = entry {
            let mut session = entry.session.lock().await;
            if committed {
                session.commit_release()
            } else {
                session.abort_release()
            }
        }
    }

    /// Self-resolve every held session older than `timeout` (coordinator-silence
    /// recovery). Snapshots stale `(g, range)` keys under a brief map lock, drops the
    /// guard, then resolves each via `resolve_in_doubt`.
    pub async fn sweep_stale(&self, client: &TwoPcClient, timeout: Duration) {
        let now = tokio::time::Instant::now();
        let stale: Vec<(u64, RangeId)> = {
            let held = self.held.lock().await;
            held.iter()
                .filter(|(_, e)| now.duration_since(e.joined_at) >= timeout)
                .map(|(&k, _)| k)
                .collect()
        };
        for (g, range) in stale {
            self.resolve_in_doubt(client, g, range).await;
        }
    }

    /// Dispatch a participant-targeted `TxnRpc` (`Stage`/`Release`). Global ops
    /// are handled by `server::handle_txn` against range 0's engine/raft.
    pub async fn handle(&self, _range: RangeId, rpc: TxnRpc) -> TxnResp {
        match rpc {
            TxnRpc::Stage { g, range: r, sql } => self.stage(g, r, &sql).await,
            TxnRpc::Release {
                g,
                range: r,
                commit,
            } => self.release(g, r, commit).await,
            _ => TxnResp::Err("non-participant rpc routed to TxnService".into()),
        }
    }

    /// Get-or-create the held session for `(g, range)` under a BRIEF map lock,
    /// returning a clone of its `Arc<Mutex>` (map guard dropped before any await).
    async fn session_handle(&self, g: u64, range: RangeId) -> Option<HeldSession> {
        let engine = self.engine(range)?;
        let mut held = self.held.lock().await;
        Some(
            held.entry((g, range))
                .or_insert_with(|| HeldEntry {
                    session: Arc::new(Mutex::new(engine.connect())),
                    joined_at: tokio::time::Instant::now(),
                })
                .session
                .clone(),
        )
    }

    async fn stage(&self, g: u64, range: RangeId, sql: &str) -> TxnResp {
        // Check the participant engine is hosted FIRST (a presence check only — does NOT
        // create a held session). An unregistered range means this node is still mid-
        // bootstrap on the replicated layout — RETRYABLE: return NotLeader (the coordinator
        // re-resolves) instead of a hard Err, and skip parsing entirely.
        if self.engine(range).is_none() {
            return TxnResp::NotLeader;
        }
        // IDEMPOTENCY: when there is NO in-memory held session for (g, range) but a durable
        // `Prepared(Li -> g)` marker already exists on this range, this is a Stage(g) RETRY
        // that landed on a NEW leader after the original leader staged-then-died (the held-
        // session map is per-process and was lost with the old leader). The first attempt's
        // row version is durable + replicated; staging again would write a SECOND
        // `Prepared(-> g)` version → two live versions when g commits. Return Staged (no-op).
        // When a held session DOES exist, this is the SAME leader handling another statement of
        // the same txn — fall through and run it on that session (preserves multi-statement
        // participants). KNOWN-SCOPE: a multi-statement-same-range txn whose leader fails over
        // mid-way then retries a LATER statement would be short-circuited here; the bank
        // workload (one statement per range per txn) never hits this, matching the slice scope.
        let has_held = { self.held.lock().await.contains_key(&(g, range)) };
        if !has_held {
            let engine = self.engine(range).expect("engine present (checked above)");
            match engine.staged_local_for(g).await {
                Ok(Some(_)) => return TxnResp::Staged,
                Ok(None) => {}
                Err(e) => return map_exec_err(e),
            }
        }
        let stmt = match pgparser::parse(sql) {
            Ok(mut v) if v.len() == 1 => v.pop().expect("one statement"),
            _ => return TxnResp::Err("stage expects exactly one statement".into()),
        };
        // Only NOW get-or-create the held session — so a parse failure above never orphans
        // an idle held entry. (Re-checks the engine; a NotLeader here means the range was
        // deregistered in the race window — still retryable.)
        let Some(handle) = self.session_handle(g, range).await else {
            return TxnResp::NotLeader;
        };
        let mut session = handle.lock().await; // per-session lock only; map lock dropped
        if let Err(e) = session.ensure_began().await {
            return map_exec_err(e);
        }
        if let Err(e) = session.join_global(g).await {
            return map_exec_err(e);
        }
        match session.run(&stmt).await {
            Ok(_) => TxnResp::Staged,
            Err(e) => map_exec_err(e),
        }
    }

    async fn release(&self, g: u64, range: RangeId, commit: bool) -> TxnResp {
        let entry = { self.held.lock().await.remove(&(g, range)) };
        if let Some(entry) = entry {
            let mut session = entry.session.lock().await;
            if commit {
                session.commit_release()
            } else {
                session.abort_release()
            }
        }
        TxnResp::Released // unknown (g,range) -> idempotent no-op success
    }
}

/// Map an `ExecError` from a staged statement to a wire response, PRESERVING the
/// retryable serialization-failure class (collapsing 40001 to 0A000 would make a
/// retryable conflict look like an unsupported feature).
fn map_exec_err(e: executor::ExecError) -> TxnResp {
    use executor::ExecError;
    match e {
        ExecError::NotLeader => TxnResp::NotLeader,
        ExecError::SerializationFailure | ExecError::Deadlock => TxnResp::Retryable,
        other => TxnResp::Err(format!("{other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use pgwire::engine::Engine;

    use crate::transport::protocol::{NodeRequest, NodeResponse, TxnResp, TxnRpc};
    use crate::twopc::{TwoPcClient, TxnService};

    fn parse_one(sql: &str) -> pgparser::ast::Statement {
        pgparser::parse(sql)
            .expect("parse")
            .into_iter()
            .next()
            .expect("one statement")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stage_then_release_holds_then_frees_a_per_g_session() {
        let (node, _sql) = crate::server_node::testonly_two_range_node().await;
        let svc = TxnService::new(node.engines.clone(), None);

        // DDL (CREATE TABLE) must run through range-0's engine (catalog lives there).
        // Table id 2 is assigned by the counter → falls in range 1 (boundary at 2).
        // First allocate table id 1 (range 0's "a" slot) so that the next allocation
        // gives id 2 (range 1). We create TWO tables so the second lands in range 1.
        let mut ddl = node.engines[&0].connect();
        ddl.run(&parse_one("CREATE TABLE _placeholder (id int4)"))
            .await
            .expect("create placeholder → table id 1, range 0");
        ddl.run(&parse_one("CREATE TABLE b (id int4)"))
            .await
            .expect("create b → table id 2, range 1");

        // Insert the seed row via range-1's engine (DML goes to the data range).
        let mut seed = node.engines[&1].connect();
        seed.run(&parse_one("INSERT INTO b VALUES (20)"))
            .await
            .expect("seed b");

        let g: u64 = mvcc::xid::GLOBAL_XID_BASE + 7;
        match svc
            .handle(
                1,
                TxnRpc::Stage {
                    g,
                    range: 1,
                    sql: "UPDATE b SET id = 21 WHERE id = 20".into(),
                },
            )
            .await
        {
            TxnResp::Staged => {}
            other => panic!("expected Staged, got {other:?}"),
        }
        assert!(
            svc.holds(g, 1).await,
            "Stage parks a held session under (g, range)"
        );

        match svc
            .handle(
                1,
                TxnRpc::Release {
                    g,
                    range: 1,
                    commit: true,
                },
            )
            .await
        {
            TxnResp::Released => {}
            other => panic!("expected Released, got {other:?}"),
        }
        assert!(!svc.holds(g, 1).await, "Release drops the held session");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_silent_coordinator_is_recovered_by_the_timeout_sweeper() {
        let (node, _sql) = crate::server_node::testonly_two_range_node().await;
        let svc = TxnService::new(node.engines.clone(), None);
        let client = TwoPcClient::new(node.rafts.clone(), node.partition.clone());

        // Seed a row on range 1, then STAGE a held participant for a global xid g that
        // NEVER receives a decision (the coordinator "crashed"). DDL (CREATE TABLE)
        // runs through range-0's engine (catalog lives there); a placeholder takes
        // table id 1 (range 0) so `b` gets id 2, which falls in range 1 (boundary 2).
        let mut ddl = node.engines[&0].connect();
        ddl.run(&parse_one("CREATE TABLE _placeholder (id int4)"))
            .await
            .expect("create placeholder → table id 1, range 0");
        ddl.run(&parse_one("CREATE TABLE b (id int4)"))
            .await
            .expect("create b → table id 2, range 1");
        let mut seed = node.engines[&1].connect();
        seed.run(&parse_one("INSERT INTO b VALUES (20)"))
            .await
            .expect("seed b");
        let g = node.engines[&0]
            .begin_global_durable()
            .await
            .expect("alloc g");
        assert!(
            matches!(
                svc.handle(
                    1,
                    TxnRpc::Stage {
                        g,
                        range: 1,
                        sql: "UPDATE b SET id = 21 WHERE id = 20".into()
                    }
                )
                .await,
                TxnResp::Staged
            ),
            "stage parks a held participant"
        );
        assert!(svc.holds(g, 1).await, "the participant holds g");

        // Drive the sweeper with a ZERO timeout (every held session is "stale"): it
        // resolves g via the abort-race (no coordinator wrote a decision -> Aborted),
        // releasing the held session. Assert via the registry condition (no sleep).
        svc.sweep_stale(&client, std::time::Duration::ZERO).await;
        assert!(
            !svc.holds(g, 1).await,
            "the timeout sweeper self-resolved + released g"
        );
    }

    #[test]
    fn txn_rpc_round_trips_through_json() {
        let reqs = vec![
            NodeRequest::Txn {
                range: 0,
                rpc: TxnRpc::BeginGlobal,
            },
            NodeRequest::Txn {
                range: 2,
                rpc: TxnRpc::Stage {
                    g: 1 << 63,
                    range: 2,
                    sql: "UPDATE b SET id=21".into(),
                },
            },
            NodeRequest::Txn {
                range: 0,
                rpc: TxnRpc::CommitGlobal {
                    g: 1 << 63,
                    commit: true,
                },
            },
            NodeRequest::Txn {
                range: 2,
                rpc: TxnRpc::Release {
                    g: 1 << 63,
                    range: 2,
                    commit: false,
                },
            },
            NodeRequest::Txn {
                range: 0,
                rpc: TxnRpc::GlobalBarrier,
            },
        ];
        for r in reqs {
            let bytes = serde_json::to_vec(&r).expect("encode");
            let back: NodeRequest = serde_json::from_slice(&bytes).expect("decode");
            assert_eq!(format!("{r:?}"), format!("{back:?}"));
        }
        for resp in [
            TxnResp::Began { g: 1 << 63 },
            TxnResp::Staged,
            TxnResp::Committed,
            TxnResp::Aborted,
            TxnResp::Released,
            TxnResp::Barrier { applied_index: 7 },
            TxnResp::NotLeader,
            TxnResp::Retryable,
            TxnResp::Err("boom".into()),
        ] {
            let env = NodeResponse::Txn(resp);
            let bytes = serde_json::to_vec(&env).expect("encode");
            let back: NodeResponse = serde_json::from_slice(&bytes).expect("decode");
            assert_eq!(format!("{env:?}"), format!("{back:?}"));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_durable_prepared_marker_is_finalized_by_the_leadership_sweep() {
        let (node, _sql) = crate::server_node::testonly_two_range_node().await;
        let svc = TxnService::new(node.engines.clone(), None);
        // Seed a row on range 1 (create a placeholder table id 1 on range 0 first so b gets id 2).
        let mut seed0 = node.engines[&0].connect();
        seed0
            .run(&parse_one("CREATE TABLE placeholder (id int4)"))
            .await
            .expect("placeholder");
        seed0
            .run(&parse_one("CREATE TABLE b (id int4)"))
            .await
            .expect("b"); // id 2 -> range 1
        let mut seed1 = node.engines[&1].connect();
        seed1
            .run(&parse_one("INSERT INTO b VALUES (20)"))
            .await
            .expect("seed");
        let g = node.engines[&0].begin_global_durable().await.expect("g");
        // Stage a held participant (writes Prepared(Li -> g) durably), then DROP the in-memory
        // session WITHOUT a decision (simulate the participant leader crashing): the durable
        // marker persists, the in-memory session is gone.
        assert!(matches!(
            svc.handle(
                1,
                TxnRpc::Stage {
                    g,
                    range: 1,
                    sql: "UPDATE b SET id = 21 WHERE id = 20".into()
                }
            )
            .await,
            TxnResp::Staged
        ));
        svc.release_all_for_range(1).await; // drop in-memory session; durable Prepared marker stays
        // The marker is in-doubt; finalize it.
        let gs = node.engines[&1].in_doubt_globals().await.expect("scan");
        assert_eq!(gs, vec![g], "the durable Prepared marker is in-doubt");
        node.engines[&0]
            .commit_global_decision(g, mvcc::clog::XidStatus::Aborted)
            .await
            .expect("finalize");
        // Now resolved: no longer in-doubt.
        let gs2 = node.engines[&1].in_doubt_globals().await.expect("scan2");
        assert!(
            gs2.is_empty(),
            "after the write-once Aborted decision, g is no longer in-doubt"
        );
    }

    /// Participant `Stage` is IDEMPOTENT across a leader failover: a retried `Stage(g)` that
    /// lands on a new leader (no in-memory held session, but the durable `Prepared(-> g)`
    /// marker from the first attempt persists) must NOT write a second row version. After
    /// committing g, the row reads its single updated value (no double-apply, exactly one live
    /// version — a second would trip the executor's at-most-one-live debug_assert on read).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stage_is_idempotent_across_a_participant_leader_failover() {
        let (node, _sql) = crate::server_node::testonly_two_range_node().await;
        let svc = TxnService::new(node.engines.clone(), None);
        let mut ddl = node.engines[&0].connect();
        ddl.run(&parse_one("CREATE TABLE _placeholder (id int4)"))
            .await
            .expect("placeholder"); // id 1 -> range 0
        ddl.run(&parse_one("CREATE TABLE b (id int4)"))
            .await
            .expect("b"); // id 2 -> range 1
        let mut seed = node.engines[&1].connect();
        seed.run(&parse_one("INSERT INTO b VALUES (20)"))
            .await
            .expect("seed");
        let g = node.engines[&0].begin_global_durable().await.expect("g");

        // First stage: durably writes Prepared(Li -> g) + the updated row version.
        assert!(matches!(
            svc.handle(
                1,
                TxnRpc::Stage {
                    g,
                    range: 1,
                    sql: "UPDATE b SET id = 21 WHERE id = 20".into()
                }
            )
            .await,
            TxnResp::Staged
        ));
        // The participant leader 'dies': drop the in-memory held session; the durable marker stays.
        svc.release_all_for_range(1).await;
        assert!(
            !svc.holds(g, 1).await,
            "held session dropped (leader 'died')"
        );

        // RETRY the SAME Stage(g) on the 'new leader': durable marker present, no held session
        // -> idempotent no-op, NO second version.
        assert!(matches!(
            svc.handle(
                1,
                TxnRpc::Stage {
                    g,
                    range: 1,
                    sql: "UPDATE b SET id = 21 WHERE id = 20".into()
                }
            )
            .await,
            TxnResp::Staged
        ));

        // Commit g globally; read b: exactly one application (21), exactly one live version.
        node.engines[&0]
            .commit_global_decision(g, mvcc::clog::XidStatus::Committed)
            .await
            .expect("commit");
        let mut read = node.engines[&1].connect();
        let result = read
            .run(&parse_one("SELECT id FROM b"))
            .await
            .expect("read b");
        // Extract column 0 of every row as i32 and assert exactly [21] (NOT doubled / not 2 rows).
        let ids = match result {
            pgwire::engine::QueryResult::Rows { rows, .. } => rows
                .iter()
                .map(|r| {
                    std::str::from_utf8(&r[0].as_ref().expect("non-null").text)
                        .expect("utf8")
                        .parse::<i32>()
                        .expect("i32")
                })
                .collect::<Vec<_>>(),
            other => panic!("expected Rows, got {other:?}"),
        };
        assert_eq!(
            ids,
            vec![21],
            "b updated exactly once (no double-apply across the stage retry)"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn register_engine_makes_a_range_servable() {
        let (node, _sql) = crate::server_node::testonly_two_range_node().await;
        let mut only0 = std::collections::HashMap::new();
        only0.insert(0u32, node.engines[&0].clone());
        let svc = TxnService::new(only0, None);
        assert!(
            svc.engine(1).is_none(),
            "range 1 absent before registration"
        );
        svc.register_engine(1, node.engines[&1].clone());
        assert!(
            svc.engine(1).is_some(),
            "range 1 present after registration"
        );
        svc.register_engine(1, node.engines[&1].clone()); // idempotent
        assert!(svc.engine(1).is_some());
        // Copy-on-write preserves prior entries: registering range 1 must not drop
        // the originally-constructed range 0 (would fail a `store`-whole-map impl).
        assert!(
            svc.engine(0).is_some(),
            "register preserves prior entries (copy-on-write)"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stage_for_an_unregistered_range_is_retryable_not_a_hard_err() {
        let (node, _sql) = crate::server_node::testonly_two_range_node().await;
        let mut only0 = std::collections::HashMap::new();
        only0.insert(0u32, node.engines[&0].clone());
        let svc = TxnService::new(only0, None);
        let resp = svc
            .handle(
                7,
                TxnRpc::Stage {
                    g: 1_000_000,
                    range: 7,
                    sql: "UPDATE b SET id = 1 WHERE id = 0".into(),
                },
            )
            .await;
        assert!(
            matches!(resp, TxnResp::NotLeader),
            "expected NotLeader for an unregistered range, got {resp:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_scan_watermark_advances_and_persists() {
        let (node, _sql) = crate::server_node::testonly_two_range_node().await;
        let eng = &node.engines[&1]; // a data range
        assert_eq!(eng.clog_scan_lo().expect("lo"), 0, "watermark starts at 0");
        // advance_clog_scan_lo persists and is monotone (a lower value is a no-op).
        eng.advance_clog_scan_lo(5).await.expect("advance");
        assert_eq!(eng.clog_scan_lo().expect("lo"), 5);
        eng.advance_clog_scan_lo(3).await.expect("no-op");
        assert_eq!(eng.clog_scan_lo().expect("lo"), 5, "watermark is monotone");
        // An empty clog scan from the current watermark leaves it unchanged.
        let (gs, new_lo) = eng.in_doubt_globals_from(5).await.expect("scan");
        assert!(gs.is_empty());
        assert_eq!(new_lo, 5, "empty scan -> watermark unchanged");
    }
}
