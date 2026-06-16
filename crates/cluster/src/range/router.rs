//! Per-connection multi-range SQL dispatch. Parses each statement, routes DDL to
//! range 0 and single-table DML to the table's data range (schema resolved from
//! range 0's catalog), and pins a transaction to one range. A transaction that
//! touches a second range ESCALATES to a cross-range global txn (`Pin::Global`)
//! committed all-or-nothing by a single decision in range 0 (D3c 2PC), instead of
//! being rejected. A single DML statement still carries one table; a `SELECT` may
//! now reference several tables (SP33 joins), but a single statement spanning more
//! than one range is rejected `0A000` (cross-range joins are not supported).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin as FuturePin;
use std::sync::Arc;

use executor::{ExecError, SqlEngine, SqlSession};
use pgparser::ast::Statement;
use pgwire::engine::{Engine, QueryResult};
use pgwire::error::PgError;

use crate::range::cluster::MultiRangeCluster;
use crate::range::map::{RangeId, RangeMap};

/// The remote half of the gateway: forward one simple-query statement to the
/// owning range's leader on another node and return its single result. The router
/// itself is pure routing/`Pin` (Decision: retry-on-NotLeader lives in the wire
/// layer, NOT here) — this seam is the only place a non-local range is handled.
///
/// Boxed-future method so the trait is object-safe behind `Arc<dyn RemoteForward>`.
/// Task 3 ships `RejectForward` (every call → 0A000); Task 4 replaces it with the
/// pooled minimal pgwire client (`crate::route::PgwireForward`).
pub trait RemoteForward: Send + Sync {
    fn forward<'a>(
        &'a self,
        range: RangeId,
        sql: &'a str,
    ) -> FuturePin<Box<dyn Future<Output = Result<QueryResult, ExecError>> + Send + 'a>>;
}

/// Drives the cross-range 2PC global operations for the gateway. `LocalCoordinator`
/// (in-process tests) calls the local range-0 engine + local participant sessions;
/// `NetCoordinator` (networked gateway) RPCs to the relevant range leaders.
#[async_trait::async_trait]
pub trait GlobalCoordinator: Send + Sync {
    async fn begin_global(&self) -> Result<u64, ExecError>;
    /// Stage `sql` on a REMOTE participant `range` inside held txn `g`.
    async fn stage_remote(&self, g: u64, range: RangeId, sql: &str) -> Result<(), ExecError>;
    /// Write the single global decision and return the EFFECTIVE outcome
    /// (true = committed, false = aborted — e.g. a participant won the abort-race).
    async fn commit_global(&self, g: u64, commit: bool) -> Result<bool, ExecError>;
    /// Release a REMOTE participant `range`'s held txn `g`.
    async fn release_remote(&self, g: u64, range: RangeId, commit: bool) -> Result<(), ExecError>;
}

/// In-process coordinator over a local GTM-bearing range-0 engine. Participants are
/// always local in `MultiRangeCluster`, so `stage_remote`/`release_remote` are unreachable.
pub struct LocalCoordinator {
    pub range0: SqlEngine,
}

#[async_trait::async_trait]
impl GlobalCoordinator for LocalCoordinator {
    async fn begin_global(&self) -> Result<u64, ExecError> {
        self.range0.begin_global_durable().await
    }
    async fn stage_remote(&self, _g: u64, range: RangeId, _sql: &str) -> Result<(), ExecError> {
        Err(ExecError::Unsupported(format!(
            "local coordinator has no remote range {range}"
        )))
    }
    async fn commit_global(&self, g: u64, commit: bool) -> Result<bool, ExecError> {
        let status = if commit {
            mvcc::clog::XidStatus::Committed
        } else {
            mvcc::clog::XidStatus::Aborted
        };
        let effective = self.range0.commit_global_decision(g, status).await?;
        self.range0.finish_global(g);
        Ok(matches!(effective, mvcc::clog::XidStatus::Committed))
    }
    async fn release_remote(
        &self,
        _g: u64,
        range: RangeId,
        _commit: bool,
    ) -> Result<(), ExecError> {
        Err(ExecError::Unsupported(format!(
            "local coordinator has no remote range {range}"
        )))
    }
}

/// Whether THIS node currently leads a given range. The gateway runs a statement
/// locally only when it both holds a local engine for the range AND currently
/// leads it; otherwise it forwards to the remote leader. In D3a-net's co-located
/// topology every node holds a replica (hence a local engine) of every range, so
/// a local-engine check alone would never forward — this predicate is what makes a
/// FOLLOWER gateway forward instead of running its local follower committer (which
/// returns `ForwardToLeader` → SQLSTATE 40001).
///
/// Object-safe behind `Arc<dyn LeadsRange>`. The implementation is synchronous —
/// it borrows a metrics watch, compares, and drops the `Ref` before returning, so
/// no `Ref` is ever held across an `.await`.
pub trait LeadsRange: Send + Sync {
    fn leads(&self, range: RangeId) -> bool;
}

/// An always-true `LeadsRange`: every range this router holds locally is treated as
/// led locally. Used by the in-process harness (`RangeRouter::connect`), which
/// builds each range's LEADER engine via `leader_engine`, so local execution is
/// already the leader — preserving the SP13 `range::*` behavior.
pub struct AlwaysLeads;

impl LeadsRange for AlwaysLeads {
    fn leads(&self, _range: RangeId) -> bool {
        true
    }
}

/// The Task-3 stub: no range is remotely reachable yet, so any statement that
/// lands on a non-local range is rejected. Replaced by the real client in Task 4.
pub struct RejectForward;

impl RemoteForward for RejectForward {
    fn forward<'a>(
        &'a self,
        range: RangeId,
        _sql: &'a str,
    ) -> FuturePin<Box<dyn Future<Output = Result<QueryResult, ExecError>> + Send + 'a>> {
        Box::pin(async move {
            Err(ExecError::Unsupported(format!(
                "range {range} is not led locally; remote forwarding lands in T4"
            )))
        })
    }
}

/// Where a transaction is pinned. Distinguishing `Open` (a BEGIN block exists but
/// no table-bearing statement has run yet) from `Range(_)` is essential: the first
/// DML pins the txn *to its range even when that range is 0*, so a later statement
/// on a different range can escalate it to a cross-range `Global` txn. (A bare
/// `Option<RangeId>` conflated "provisional, unpinned" with "pinned to range 0".)
#[derive(Debug, Clone, PartialEq, Eq)]
enum Pin {
    /// No open transaction (autocommit): each statement routes to its own range.
    None,
    /// Inside BEGIN..COMMIT, not yet pinned by a table-bearing statement.
    Open,
    /// Inside BEGIN..COMMIT, pinned to this range by the first DML / FROM SELECT.
    Range(RangeId),
    /// Inside BEGIN..COMMIT, escalated to a cross-range global txn `g` spanning
    /// `ranges` (every participant has joined `g` and writes a `Prepared(-> g)`
    /// marker). COMMIT/ROLLBACK drives the single global decision through range 0.
    /// `Pin` loses `Copy` for this owning `BTreeSet` — every prior by-value use is
    /// rewritten to borrow (`match &self.pin`) or consume (`mem::replace`).
    Global {
        ranges: std::collections::BTreeSet<RangeId>,
        g: u64,
    },
}

/// A connection's view: per range it has touched, a leader `SqlSession` (LOCAL
/// ranges only); the `Pin` a transaction is held to; and the seam that forwards a
/// non-local range's statement to its remote leader.
pub struct RangeRouter {
    sessions: HashMap<RangeId, SqlSession>,
    pin: Pin,
    map: RangeMap,
    /// Engines for ranges this node holds a replica of; a range absent here is
    /// remote. Holding an engine does NOT imply leadership — see `leads`.
    engines: HashMap<RangeId, SqlEngine>,
    /// Whether THIS node currently leads a range. A statement runs locally only
    /// when `engines` holds the range AND this returns true; otherwise it forwards.
    leads: Arc<dyn LeadsRange>,
    /// Settle-before-serve gate (SP22): `None` for the in-process harness (always serving).
    gate: Option<Arc<crate::recovery_gate::RecoveryGate>>,
    /// Range-0 catalog store (schema resolution). For a range-0 follower gateway
    /// Task 4 makes this a wire-read handle; here it is the local range-0 store.
    catalog_kv: Arc<dyn kv::Kv>,
    /// Forwards a statement whose range has no local engine.
    forward: Arc<dyn RemoteForward>,
    /// Drives the cross-range 2PC global operations (begin/stage/commit/release).
    /// `Some` when this router can escalate a txn to cross-range 2PC: a
    /// `LocalCoordinator` in-process or a `NetCoordinator` on the networked gateway.
    /// `None` for routers that never escalate (unit-test seam routers).
    coordinator: Option<Arc<dyn GlobalCoordinator>>,
    /// The EXACT source text of the statement currently being dispatched (set per
    /// statement from `parse_with_source`) — what the forward seam relays for a
    /// non-local range. Per-statement, NOT the whole `;`-separated frame, so a frame
    /// mixing a local and a remote range forwards only the remote statement and never
    /// re-runs the local one on the remote node.
    cur_sql: String,
    /// Test-only coordinator pause seam: invoked inside the global COMMIT/ROLLBACK
    /// path AFTER every participant has staged (joined `g`) and BEFORE
    /// `commit_global_decision` writes the global decision. A crash test installs a
    /// hook that drops/strands the router at exactly the staged-but-undecided point
    /// — deterministically, with no sleep. `None` in production (the seam is inert).
    #[cfg(test)]
    before_global_decision: Option<Box<dyn FnMut() + Send>>,
}

impl RangeRouter {
    /// Cluster-agnostic constructor: the local engines this node holds, a predicate
    /// for which of those ranges this node currently leads, the range-0 catalog
    /// store, and the remote-forward seam. No `&MultiRangeCluster`.
    pub fn new(
        map: RangeMap,
        engines: HashMap<RangeId, SqlEngine>,
        leads: Arc<dyn LeadsRange>,
        catalog_kv: Arc<dyn kv::Kv>,
        forward: Arc<dyn RemoteForward>,
        coordinator: Option<Arc<dyn GlobalCoordinator>>,
        gate: Option<Arc<crate::recovery_gate::RecoveryGate>>,
    ) -> Self {
        Self {
            sessions: HashMap::new(),
            pin: Pin::None,
            map,
            engines,
            leads,
            gate,
            catalog_kv,
            forward,
            coordinator,
            cur_sql: String::new(),
            #[cfg(test)]
            before_global_decision: None,
        }
    }

    /// Install the test-only coordinator pause seam (see `before_global_decision`).
    /// The hook fires once per global COMMIT/ROLLBACK, just before the global
    /// decision is written.
    #[cfg(test)]
    fn set_before_global_decision(&mut self, hook: Box<dyn FnMut() + Send>) {
        self.before_global_decision = Some(hook);
    }

    /// Test-only: the global xid this txn has escalated to, if it is currently a
    /// staged cross-range txn (`Pin::Global`). Used by the crash-before/after-
    /// decision tests to drive the global decision out-of-band (simulating a
    /// coordinator that crashes after the decision is durable but before it
    /// releases the participants) without going through the normal COMMIT close.
    #[cfg(test)]
    fn staged_global_xid(&self) -> Option<u64> {
        match &self.pin {
            Pin::Global { g, .. } => Some(*g),
            _ => None,
        }
    }

    /// Test-only: the coordinator engine (range 0's GTM-bearing engine), so a crash
    /// test can write the global decision directly for a known `g`.
    #[cfg(test)]
    fn coordinator_engine(&self) -> &SqlEngine {
        &self.engines[&0]
    }

    /// In-process harness constructor: the harness leads every range from one of
    /// its co-located nodes, so it has a local engine per range and never needs to
    /// forward — delegates to `new` with an `AlwaysLeads` predicate (every local
    /// engine IS the range's leader engine) and a `RejectForward` (never hit
    /// in-process). This preserves the SP13 `range::*` behavior exactly.
    pub async fn connect(c: &MultiRangeCluster) -> Self {
        let mut engines = HashMap::new();
        for r in c.range_map().range_ids() {
            engines.insert(r, c.leader_engine(r).await);
        }
        // The in-process coordinator drives 2PC directly against range 0's local
        // GTM-bearing engine; every participant is local, so it never RPCs.
        let coordinator: Arc<dyn GlobalCoordinator> = Arc::new(LocalCoordinator {
            range0: c.leader_engine(0).await,
        });
        Self::new(
            c.range_map().clone(),
            engines,
            Arc::new(AlwaysLeads),
            c.catalog_kv().await,
            Arc::new(RejectForward),
            Some(coordinator),
            None,
        )
    }

    /// The concrete data range a *table-bearing* statement targets — the only kind
    /// that pins a transaction. `Insert`/`Update`/`Delete` carry exactly one table.
    /// A `SELECT` may reference several base tables (SP33 joins): it pins iff all of
    /// them live on one range; a SELECT spanning ranges is rejected `0A000`, and a
    /// FROM-less SELECT carries no table and returns `None`, so it never pins.
    /// Everything else (DDL, txn-control) carries no table and returns `None`.
    fn pinning_range(&self, stmt: &Statement) -> Result<Option<RangeId>, ExecError> {
        match stmt {
            Statement::Insert { table, .. }
            | Statement::Update { table, .. }
            | Statement::Delete { table, .. } => self.range_of(table).map(Some),
            Statement::Select(s) => {
                let mut ranges = std::collections::BTreeSet::new();
                collect_select_ranges(self, s, &mut ranges)?;
                match ranges.len() {
                    0 => Ok(None), // FROM-less -> range 0, unpinned
                    1 => Ok(Some(
                        *ranges.iter().next().expect("len()==1 has one element"),
                    )),
                    _ => Err(ExecError::Unsupported(
                        "cross-range joins or subqueries are not supported".into(),
                    )),
                }
            }
            // DDL and transaction control resolve to range 0 but do not pin: a txn
            // can still be pinned to a data range by a later DML.
            Statement::CreateTable { .. }
            | Statement::DropTable { .. }
            | Statement::Begin { .. }
            | Statement::Commit
            | Statement::Rollback => Ok(None),
        }
    }

    fn range_of(&self, table_name: &str) -> Result<RangeId, ExecError> {
        let t = catalog::get_table(&*self.catalog_kv, table_name)?;
        Ok(self.map.range_for_table(t.id))
    }

    /// Execute one already-parsed statement, honoring transaction range-pinning.
    ///
    /// Routing rules:
    /// - Autocommit (`Pin::None`): every statement runs on its own range's session
    ///   (a table-bearing statement on its table's range; DDL/txn-control/FROM-less
    ///   SELECT on range 0).
    /// - Inside a txn: BEGIN opens it; the first table-bearing statement pins it to
    ///   that table's range (even range 0). A later table-bearing statement on a
    ///   different range is rejected (0A000, deferred to D3b). All statements in a
    ///   pinned txn — including DDL/FROM-less SELECT that target range 0 — run on
    ///   the pinned session so they share one transaction's xid + locks. COMMIT /
    ///   ROLLBACK close the block and clear the pin.
    async fn dispatch(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        let pinning = self.pinning_range(stmt)?;
        // SP22 settle-before-serve: reject a locally-led WRITE (Insert/Update/Delete) on a
        // range whose rise sweep has not settled the current term. Reads (Select) and
        // DDL/txn-control pass ungated. Retryable NotLeader -> 40001 -> client retries.
        // A locking SELECT (FOR UPDATE/FOR SHARE) is intentionally ungated: it creates no new
        // row version or Prepared(-> g) marker, so it cannot produce the duplicate-version
        // hazard the gate prevents, and it resolves any inherited in-doubt row via the
        // under-lock global-clog read in eval_plan_qual — exact regardless of settle state.
        if matches!(
            stmt,
            Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. }
        ) && let Some(r) = pinning
            && self.engines.contains_key(&r)
            && self.leads.leads(r)
            && self.gate.as_ref().is_some_and(|g_| !g_.is_serving(r))
        {
            return Err(ExecError::NotLeader);
        }
        match stmt {
            Statement::Begin { .. } => {
                // Idempotent like PG: a BEGIN inside a block leaves the pin as-is.
                if matches!(self.pin, Pin::None) {
                    self.pin = Pin::Open;
                }
                return self.run_on(0, stmt).await;
            }
            Statement::Commit | Statement::Rollback => {
                return self.finish_txn(stmt).await;
            }
            _ => {}
        }

        match &self.pin {
            // Autocommit: route each statement independently to its target range
            // (table-bearing -> its range; otherwise range 0).
            Pin::None => self.run_on(pinning.unwrap_or(0), stmt).await,
            // The first table-bearing statement of the txn pins it. A LOCALLY-led
            // range pins to `Pin::Range(r)` and runs locally; a range this gateway
            // does NOT lead immediately escalates to a cross-range global txn `g`
            // and STAGES the statement as a held participant — this is what lets a
            // gateway that leads nothing still coordinate.
            Pin::Open => match pinning {
                Some(r) => {
                    if self.engines.contains_key(&r) && self.leads.leads(r) {
                        // Hold the first DML even when it lands on a NON-range-0
                        // range: BEGIN ran only on range 0's session, so this range's
                        // session is still Idle and the DML would otherwise autocommit.
                        // `ensure_began_on` opens a held txn on `r` first so the write
                        // is held until COMMIT.
                        self.pin = Pin::Range(r);
                        self.ensure_began_on(r).await?;
                        return self.run_on(r, stmt).await;
                    }
                    // The very first participant is remote: escalate now and stage it.
                    if !self.can_escalate() {
                        return Err(ExecError::Unsupported(
                            "a transaction may not span ranges yet (D3b)".into(),
                        ));
                    }
                    let coord = self
                        .coordinator
                        .as_ref()
                        .expect("coordinator to escalate")
                        .clone();
                    let g = coord.begin_global().await?;
                    let mut ranges = std::collections::BTreeSet::new();
                    ranges.insert(r);
                    self.pin = Pin::Global { ranges, g };
                    self.stage_on(r, g, stmt).await
                }
                None => self.run_on(0, stmt).await, // DDL / FROM-less SELECT: range 0, stay unpinned.
            },
            // Already pinned to `p`: a table-bearing statement on another range `r`
            // escalates the txn to a cross-range global txn `g`. The already-pinned
            // local range `p` joins `g` via its local session; `r` is staged (locally
            // if led, else over RPC) through `stage_on`.
            Pin::Range(p) => {
                let p = *p;
                if let Some(r) = pinning
                    && r != p
                {
                    if !self.can_escalate() {
                        return Err(ExecError::Unsupported(
                            "a transaction may not span ranges yet (D3b)".into(),
                        ));
                    }
                    let coord = self
                        .coordinator
                        .as_ref()
                        .expect("coordinator to escalate")
                        .clone();
                    let g = coord.begin_global().await?;
                    // Backfill Prepared(Lp -> g) on the already-written local `p`.
                    self.session_mut(p).join_global(g).await?;
                    let mut ranges = std::collections::BTreeSet::new();
                    ranges.insert(p);
                    ranges.insert(r);
                    self.pin = Pin::Global { ranges, g };
                    return self.stage_on(r, g, stmt).await;
                }
                // Same range (or no-table statement): run on the pinned session.
                self.run_on(p, stmt).await
            }
            // Already global: a table-bearing statement on any range is staged into
            // `g` (joining the set if new); a no-table statement runs on range 0.
            Pin::Global { ranges, g } => {
                let g = *g;
                if let Some(r) = pinning {
                    if !ranges.contains(&r)
                        && let Pin::Global { ranges, .. } = &mut self.pin
                    {
                        ranges.insert(r);
                    }
                    return self.stage_on(r, g, stmt).await;
                }
                // A no-table statement (DDL / FROM-less SELECT) runs on range 0.
                self.run_on(0, stmt).await
            }
        }
    }

    /// COMMIT / ROLLBACK: consumes the pin (`mem::replace` mirrors `commit_cmd`).
    /// Under `Pin::Global` this drives the single global decision through range 0;
    /// otherwise it is the unchanged single-range close.
    async fn finish_txn(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        match std::mem::replace(&mut self.pin, Pin::None) {
            Pin::Global { ranges, g } => {
                let commit = matches!(stmt, Statement::Commit);
                // Test-only pause seam: every participant has staged (joined `g`);
                // fire the hook BEFORE the decision so a crash test can strand the
                // coordinator at the staged-but-undecided point.
                #[cfg(test)]
                if let Some(hook) = self.before_global_decision.as_mut() {
                    hook();
                }
                let coord = self.coordinator.as_ref().expect("coordinator").clone();
                // The single atomic instant: one range-0 append both ranges flip at.
                // The RETURNED decision is the EFFECTIVE one (write-once): a COMMIT
                // that lost the abort-race comes back `false`, and must become an
                // honest ROLLBACK for both the release semantics AND the client tag.
                let committed = coord.commit_global(g, commit).await?;
                // Release each participant's locks + deregister (no per-participant
                // clog write — the decision was recorded once, globally, above).
                // Best-effort: the decision is durable and final, so a single remote
                // release failure must NOT strand the other participants (a stranded
                // remote lock is the known SP18 liveness gap, covered by
                // release-on-leadership-loss in T6).
                for r in &ranges {
                    if self.engines.contains_key(r) && self.leads.leads(*r) {
                        let session = self.session_mut(*r);
                        if committed {
                            session.commit_release();
                        } else {
                            session.abort_release();
                        }
                    } else {
                        let _ = coord.release_remote(g, *r, committed).await;
                    }
                }
                Ok(QueryResult::Command {
                    tag: if committed {
                        "COMMIT".into()
                    } else {
                        "ROLLBACK".into()
                    },
                })
            }
            // Single-range or never-pinned: unchanged close on the pinned session
            // (or range 0).
            Pin::Range(p) => self.run_on(p, stmt).await,
            Pin::Open | Pin::None => self.run_on(0, stmt).await,
        }
    }

    /// Whether this router can escalate a txn to cross-range 2PC: it has a wired
    /// `GlobalCoordinator`. True for both the in-process `MultiRangeCluster`
    /// (`LocalCoordinator`) and the networked gateway (`NetCoordinator`); false only
    /// for unit-test seam routers that never escalate.
    fn can_escalate(&self) -> bool {
        self.coordinator.is_some()
    }

    /// Open a held txn on `range`'s session if it is Idle, so a participant's first
    /// write is HELD (never autocommitted). Only meaningful for a LOCAL range; in
    /// the in-process harness every participant range is local. Used both for the
    /// single-range non-range-0 first-DML case and for cross-range escalation.
    async fn ensure_began_on(&mut self, range: RangeId) -> Result<(), ExecError> {
        self.session_mut(range).ensure_began().await
    }

    /// Run a participant statement on `range` inside global txn `g`: locally if led,
    /// else Stage over RPC. NEVER routes a remote range through session_mut (which panics).
    async fn stage_on(
        &mut self,
        range: RangeId,
        g: u64,
        stmt: &Statement,
    ) -> Result<QueryResult, ExecError> {
        if self.engines.contains_key(&range) && self.leads.leads(range) {
            // Settle-before-serve, last write path: a locally-led participant Stage is a
            // version-creating WRITE, so gate it on a freshly-risen, still-settling leader exactly
            // like `dispatch`'s direct-write check and the remote `TxnService::stage` check. The
            // newly-risen participant leader reconstructs every inherited in-doubt `Prepared(-> g)`
            // marker (driving each to its durable global decision) in its rise sweep BEFORE the
            // gate opens; admitting a local stage before that sweep settles could supersede an
            // unsettled marker with a non-superseding version and destroy a committed `g`'s half.
            // GATE ONLY — no `staged_local_for` idempotency no-op here: the router coordinates with
            // a FRESH `g` per escalation (never legitimately re-stages the same `g` locally), and
            // such a no-op is unsafe under GTM xid reuse. Retryable NotLeader -> 40001 -> retry.
            if self.gate.as_ref().is_some_and(|g_| !g_.is_serving(range)) {
                return Err(ExecError::NotLeader);
            }
            self.ensure_began_on(range).await?;
            self.session_mut(range).join_global(g).await?;
            return self.session_mut(range).run(stmt).await;
        }
        let coord = self
            .coordinator
            .as_ref()
            .expect("coordinator for cross-range")
            .clone();
        // A remote Stage returns no rows (the participant holds its result); the
        // gateway reports a generic command tag, like any held-write in a txn.
        coord.stage_remote(g, range, &self.cur_sql).await?;
        Ok(QueryResult::Command { tag: "OK".into() })
    }

    /// Run a statement on `range`: locally only when this node holds a local engine
    /// for the range AND currently leads it; otherwise forward to the remote leader
    /// through the seam.
    ///
    /// The leadership check is essential under co-located placement (every node
    /// holds a replica of every range): without it, a statement landing on a
    /// FOLLOWER gateway would run the local follower `RaftCommitter`, which returns
    /// `ForwardToLeader` → `ExecError::NotLeader` → SQLSTATE 40001 instead of being
    /// forwarded. `leads` borrows a metrics watch and drops the `Ref` before
    /// returning (synchronous — no `Ref` held across the `.await` below).
    ///
    /// `cur_sql` is the EXACT source of the statement currently being dispatched (set
    /// per statement from `parse_with_source`), so the forward relays only this
    /// statement: a `;`-separated frame mixing a local and a remote range forwards
    /// only the remote statement and never re-runs the local one on the remote node.
    async fn run_on(&mut self, range: RangeId, stmt: &Statement) -> Result<QueryResult, ExecError> {
        if self.engines.contains_key(&range) && self.leads.leads(range) {
            return self.session_mut(range).run(stmt).await;
        }
        // Not the local leader of this range → forward this statement's text.
        let sql = self.cur_sql.clone();
        self.forward.forward(range, &sql).await
    }

    /// Get (creating on first use) the LOCAL `SqlSession` for `range`'s engine.
    /// Only called for ranges present in `engines`.
    fn session_mut(&mut self, range: RangeId) -> &mut SqlSession {
        if !self.sessions.contains_key(&range) {
            let s = self
                .engines
                .get(&range)
                .expect("local engine for range")
                .connect();
            self.sessions.insert(range, s);
        }
        self.sessions.get_mut(&range).expect("session")
    }

    /// Parse `sql` and run each statement in order; return the last result. The
    /// raw text is recorded so the forward seam can relay the exact `Query`.
    pub async fn simple(&mut self, sql: &str) -> Result<QueryResult, PgError> {
        let stmts = pgparser::parse_with_source(sql).map_err(|e| ExecError::Parse(e).into_pg())?;
        let mut last = QueryResult::Command { tag: "OK".into() };
        for (stmt, src) in &stmts {
            // Record THIS statement's exact source so a forward relays only it.
            self.cur_sql = src.clone();
            last = self.dispatch(stmt).await.map_err(ExecError::into_pg)?;
        }
        Ok(last)
    }
}

/// Collect every base-table range a SELECT references into `out` — its FROM clause
/// (base tables / joins / derived tables, SP33) AND every uncorrelated subquery
/// nested in its expression clauses (projection / WHERE / HAVING / GROUP BY /
/// ORDER BY, SP34). The router enforces that all of them live on one range (else
/// 0A000). Free function so it borrows the router immutably while walking the
/// borrowed `&SelectStmt` — no borrow friction.
fn collect_select_ranges(
    router: &RangeRouter,
    s: &pgparser::ast::SelectStmt,
    out: &mut std::collections::BTreeSet<RangeId>,
) -> Result<(), ExecError> {
    use pgparser::ast::SelectItem;
    for te in &s.from {
        collect_table_expr_ranges(router, te, out)?;
    }
    for item in &s.projection {
        if let SelectItem::Expr { expr, .. } = item {
            collect_expr_ranges(router, expr, out)?;
        }
    }
    if let Some(f) = &s.filter {
        collect_expr_ranges(router, f, out)?;
    }
    if let Some(h) = &s.having {
        collect_expr_ranges(router, h, out)?;
    }
    for g in &s.group_by {
        collect_expr_ranges(router, g, out)?;
    }
    for o in &s.order_by {
        collect_expr_ranges(router, &o.expr, out)?;
    }
    Ok(())
}

fn collect_table_expr_ranges(
    router: &RangeRouter,
    te: &pgparser::ast::TableExpr,
    out: &mut std::collections::BTreeSet<RangeId>,
) -> Result<(), ExecError> {
    use pgparser::ast::TableExpr;
    match te {
        TableExpr::Table { name, .. } => {
            out.insert(router.range_of(name)?);
        }
        TableExpr::Derived { subquery, .. } => {
            collect_select_ranges(router, subquery, out)?;
        }
        TableExpr::Join { left, right, .. } => {
            collect_table_expr_ranges(router, left, out)?;
            collect_table_expr_ranges(router, right, out)?;
        }
    }
    Ok(())
}

/// SP34: collect ranges referenced by subqueries nested in an expression. Recurses
/// through every `Expr` variant that can hold a subquery or a sub-expression; a
/// subquery node recurses into its full SELECT via `collect_select_ranges`.
fn collect_expr_ranges(
    router: &RangeRouter,
    e: &pgparser::ast::Expr,
    out: &mut std::collections::BTreeSet<RangeId>,
) -> Result<(), ExecError> {
    use pgparser::ast::{Expr, FuncArgs};
    match e {
        Expr::ScalarSubquery(s) | Expr::Exists(s) => collect_select_ranges(router, s, out)?,
        Expr::InSubquery {
            expr, subquery, ..
        }
        | Expr::Quantified {
            expr, subquery, ..
        } => {
            collect_expr_ranges(router, expr, out)?;
            collect_select_ranges(router, subquery, out)?;
        }
        Expr::Unary { expr, .. } => collect_expr_ranges(router, expr, out)?,
        Expr::Binary { left, right, .. } => {
            collect_expr_ranges(router, left, out)?;
            collect_expr_ranges(router, right, out)?;
        }
        Expr::Func(fc) => {
            if let FuncArgs::Exprs(args) = &fc.args {
                for a in args {
                    collect_expr_ranges(router, a, out)?;
                }
            }
        }
        Expr::IsNull { expr, .. } => collect_expr_ranges(router, expr, out)?,
        Expr::InList { expr, list, .. } => {
            collect_expr_ranges(router, expr, out)?;
            for x in list {
                collect_expr_ranges(router, x, out)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_ranges(router, expr, out)?;
            collect_expr_ranges(router, low, out)?;
            collect_expr_ranges(router, high, out)?;
        }
        Expr::Like { expr, pattern, .. } => {
            collect_expr_ranges(router, expr, out)?;
            collect_expr_ranges(router, pattern, out)?;
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            if let Some(o) = operand {
                collect_expr_ranges(router, o, out)?;
            }
            for (c, r) in whens {
                collect_expr_ranges(router, c, out)?;
                collect_expr_ranges(router, r, out)?;
            }
            if let Some(el) = else_result {
                collect_expr_ranges(router, el, out)?;
            }
        }
        Expr::Cast { expr, .. } => collect_expr_ranges(router, expr, out)?,
        Expr::IntLiteral(_)
        | Expr::NumericLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BoolLiteral(_)
        | Expr::NullLiteral
        | Expr::Column { .. }
        | Expr::Param(_)
        | Expr::Const { .. } => {}
    }
    Ok(())
}

impl pgwire::engine::Session for RangeRouter {
    /// One simple-protocol `Query` frame → one result per statement. Each statement
    /// is range-demuxed (local engine or forward seam); a routing/exec error becomes
    /// the connection's `ErrorResponse` exactly as the single-range session does.
    async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        if sql.trim().is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        let stmts = pgparser::parse_with_source(sql).map_err(|e| ExecError::from(e).into_pg())?;
        if stmts.is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        let mut results = Vec::with_capacity(stmts.len());
        for (stmt, src) in &stmts {
            // Record THIS statement's exact source so a forward relays only it.
            self.cur_sql = src.clone();
            results.push(self.dispatch(stmt).await.map_err(ExecError::into_pg)?);
        }
        Ok(results)
    }

    /// Describe resolves field types from range 0's catalog — the gateway rejects
    /// cross-range **extended** protocol elsewhere, so a Describe only needs the
    /// catalog store, matching the spec's "simple-query routing is the surface".
    async fn describe(
        &mut self,
        sql: &str,
    ) -> Result<Vec<pgwire::engine::FieldDescription>, PgError> {
        // describe is read-only schema lookup; run it on range 0's catalog store.
        executor::describe_fields(&*self.catalog_kv, sql).map_err(ExecError::into_pg)
    }

    fn tx_status(&self) -> pgwire::engine::TxStatus {
        match self.pin {
            Pin::None => pgwire::engine::TxStatus::Idle,
            Pin::Open | Pin::Range(_) | Pin::Global { .. } => {
                pgwire::engine::TxStatus::InTransaction
            }
        }
    }
}

#[cfg(test)]
impl RangeRouter {
    async fn scan_one_i32(&mut self, sql: &str) -> Vec<i32> {
        use pgwire::engine::QueryResult;
        match self.simple(sql).await.expect("query ok") {
            QueryResult::Rows { rows, .. } => rows
                .iter()
                .map(|r| {
                    let cell = r[0].as_ref().expect("non-null");
                    std::str::from_utf8(&cell.text)
                        .expect("utf8")
                        .parse()
                        .expect("i32")
                })
                .collect(),
            other => panic!("expected Rows, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn create_in_range0_insert_routes_to_data_range_select_reads_back() {
        // boundary at table 2: the first user table (id 1) -> range 0;
        // later tables (id >= 2) -> range 1.
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        let mut router = RangeRouter::connect(&c).await;

        router
            .simple("CREATE TABLE a (id int4)")
            .await
            .expect("create a"); // id 1 -> range 0
        router
            .simple("CREATE TABLE b (id int4)")
            .await
            .expect("create b"); // id 2 -> range 1
        router
            .simple("INSERT INTO a VALUES (10)")
            .await
            .expect("insert a");
        router
            .simple("INSERT INTO b VALUES (20)")
            .await
            .expect("insert b");

        assert_eq!(router.scan_one_i32("SELECT id FROM a").await, vec![10]);
        assert_eq!(router.scan_one_i32("SELECT id FROM b").await, vec![20]);
    }

    /// SP33: a single `SELECT` join that SPANS ranges is rejected `0A000` by the
    /// router with its cross-range message (`pinning_range` walks the join's base
    /// tables, dedups their ranges, and rejects when more than one is touched),
    /// while an ordinary single-table SELECT on a data range still routes and reads
    /// back fine — so the multi-table walk did not break single-table routing. (This
    /// asserts only the routing decision; the executor's nested-loop join execution
    /// for same-range joins is covered by the `executor` crate's tests.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cross_range_join_is_rejected_while_single_table_select_routes() {
        // boundary at table 2: a (id 1) -> range 0, b (id 2) -> range 1.
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        let mut router = RangeRouter::connect(&c).await;
        router
            .simple("CREATE TABLE a (id int4)")
            .await
            .expect("create a"); // id 1 -> range 0
        router
            .simple("CREATE TABLE b (id int4)")
            .await
            .expect("create b"); // id 2 -> range 1
        router
            .simple("INSERT INTO b VALUES (20)")
            .await
            .expect("seed b");

        // A join spanning range 0 (a) and range 1 (b) -> rejected 0A000 BY THE ROUTER.
        // The router rejects (`cross-range joins are not supported`) before the
        // statement ever reaches an engine, so this is the router's decision, not the
        // executor's not-yet-implemented join builder.
        let err = router
            .simple("SELECT * FROM a JOIN b ON a.id = b.id")
            .await
            .expect_err("cross-range join rejected");
        assert_eq!(
            err.code, "0A000",
            "a join spanning ranges surfaces 0A000, got {err:?}"
        );
        assert!(
            err.message.contains("cross-range"),
            "the router (not the executor) rejected it; got {err:?}"
        );

        // A single-table SELECT on its data range still routes and reads back — the
        // multi-table FROM walk did not regress ordinary single-table routing.
        assert_eq!(router.scan_one_i32("SELECT id FROM b").await, vec![20]);
    }

    /// SP34: a single SELECT whose SUBQUERY references a table on another range is
    /// rejected `0A000` by the router (`pinning_range` now walks subquery expressions
    /// too), while a co-located subquery routes and reads back fine.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cross_range_subquery_is_rejected_while_colocated_runs() {
        // boundary at table 2: a (id 1) -> range 0, b (id 2) -> range 1.
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        let mut router = RangeRouter::connect(&c).await;
        router.simple("CREATE TABLE a (id int4)").await.expect("a"); // id 1 -> range 0
        router.simple("CREATE TABLE b (id int4)").await.expect("b"); // id 2 -> range 1
        router.simple("INSERT INTO a VALUES (1)").await.expect("seed a");
        router.simple("INSERT INTO b VALUES (1)").await.expect("seed b");

        // a (range 0) referencing b (range 1) in a subquery -> rejected 0A000.
        let err = router
            .simple("SELECT id FROM a WHERE id IN (SELECT id FROM b)")
            .await
            .expect_err("cross-range subquery rejected");
        assert_eq!(err.code, "0A000", "got {err:?}");
        assert!(err.message.contains("cross-range"), "got {err:?}");

        // A co-located subquery (both on range 0) routes and reads back.
        assert_eq!(
            router
                .scan_one_i32("SELECT id FROM a WHERE id IN (SELECT id FROM a)")
                .await,
            vec![1]
        );
    }

    /// A cross-range transaction now escalates to two-phase commit instead of being
    /// rejected: `BEGIN; INSERT a@range0; INSERT b@range1; COMMIT;` commits both rows
    /// all-or-nothing, and fresh routers read both back through the global clog.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cross_range_transaction_commits_atomically() {
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        let mut router = RangeRouter::connect(&c).await;
        router
            .simple("CREATE TABLE a (id int4)")
            .await
            .expect("create a"); // id 1 -> range 0
        router
            .simple("CREATE TABLE b (id int4)")
            .await
            .expect("create b"); // id 2 -> range 1
        router.simple("BEGIN").await.expect("begin");
        router
            .simple("INSERT INTO a VALUES (1)")
            .await
            .expect("first DML pins range 0");
        router
            .simple("INSERT INTO b VALUES (2)")
            .await
            .expect("second range escalates to 2PC, not rejected");
        // Read-your-writes within the txn: both rows visible to the same router.
        assert_eq!(router.scan_one_i32("SELECT id FROM a").await, vec![1]);
        assert_eq!(router.scan_one_i32("SELECT id FROM b").await, vec![2]);
        router.simple("COMMIT").await.expect("commit");

        // Both rows visible to the same router after COMMIT.
        assert_eq!(router.scan_one_i32("SELECT id FROM a").await, vec![1]);
        assert_eq!(router.scan_one_i32("SELECT id FROM b").await, vec![2]);

        // And to a fresh router resolving through the global clog.
        let mut fresh = RangeRouter::connect(&c).await;
        assert_eq!(fresh.scan_one_i32("SELECT id FROM a").await, vec![1]);
        assert_eq!(fresh.scan_one_i32("SELECT id FROM b").await, vec![2]);
    }

    /// The coordinator pause seam fires exactly between staging and the global
    /// decision: while a cross-range COMMIT is parked at the seam (all participants
    /// have staged their `Prepared(-> g)` markers but `Committed(g)` is NOT yet
    /// written), a concurrent fresh router sees NEITHER row (in-doubt invisibility);
    /// once released, the decision lands and both rows become visible. The seam is a
    /// rendezvous (channel), never a sleep — the concurrent read happens exactly when
    /// the COMMIT is parked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coordinator_pause_seam_holds_a_txn_in_doubt() {
        let c = Arc::new(MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await);
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        let mut setup = RangeRouter::connect(&c).await;
        setup
            .simple("CREATE TABLE a (id int4)")
            .await
            .expect("create a"); // id 1 -> range 0
        setup
            .simple("CREATE TABLE b (id int4)")
            .await
            .expect("create b"); // id 2 -> range 1
        drop(setup);

        // The committing router stages both participants, then parks at the seam.
        let mut router = RangeRouter::connect(&c).await;
        router.simple("BEGIN").await.expect("begin");
        router.simple("INSERT INTO a VALUES (1)").await.expect("a");
        router.simple("INSERT INTO b VALUES (2)").await.expect("b");

        // Rendezvous: the seam signals it has parked (staged, pre-decision) and then
        // blocks until the test releases it. Synchronous channels are fine — the hook
        // runs on a runtime worker thread, the blocking recv is bounded by the test.
        let (parked_tx, parked_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        router.set_before_global_decision(Box::new(move || {
            parked_tx.send(()).expect("signal parked");
            release_rx.recv().expect("await release");
        }));

        let commit = tokio::spawn(async move {
            router.simple("COMMIT").await.expect("commit");
            router
        });

        // Wait until the COMMIT is parked at the seam (staged, undecided).
        parked_rx.recv().expect("seam parked");

        // A concurrent fresh router sees NEITHER row: the participants are staged
        // (Prepared) but range 0's global clog has no Committed(g) yet → in-doubt.
        let mut concurrent = RangeRouter::connect(&c).await;
        assert_eq!(
            concurrent.scan_one_i32("SELECT id FROM a").await,
            Vec::<i32>::new(),
            "range-0 row in-doubt while staged"
        );
        assert_eq!(
            concurrent.scan_one_i32("SELECT id FROM b").await,
            Vec::<i32>::new(),
            "range-1 row in-doubt while staged"
        );

        // Release the seam; the decision lands and the COMMIT completes.
        release_tx.send(()).expect("release seam");
        let _router = commit.await.expect("commit task");

        // Now both rows are visible through a fresh router.
        let mut after = RangeRouter::connect(&c).await;
        assert_eq!(after.scan_one_i32("SELECT id FROM a").await, vec![1]);
        assert_eq!(after.scan_one_i32("SELECT id FROM b").await, vec![2]);
    }

    /// The ROLLBACK sibling: the same cross-range txn rolled back leaves NEITHER row
    /// visible, through fresh routers (the global clog records a positive Aborted(G)).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cross_range_transaction_rolls_back_atomically() {
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        let mut router = RangeRouter::connect(&c).await;
        router
            .simple("CREATE TABLE a (id int4)")
            .await
            .expect("create a"); // id 1 -> range 0
        router
            .simple("CREATE TABLE b (id int4)")
            .await
            .expect("create b"); // id 2 -> range 1
        router.simple("BEGIN").await.expect("begin");
        router
            .simple("INSERT INTO a VALUES (1)")
            .await
            .expect("first DML pins range 0");
        router
            .simple("INSERT INTO b VALUES (2)")
            .await
            .expect("second range escalates to 2PC");
        router.simple("ROLLBACK").await.expect("rollback");

        // Neither row is visible, to the same router or a fresh one.
        assert_eq!(
            router.scan_one_i32("SELECT id FROM a").await,
            Vec::<i32>::new()
        );
        assert_eq!(
            router.scan_one_i32("SELECT id FROM b").await,
            Vec::<i32>::new()
        );
        let mut fresh = RangeRouter::connect(&c).await;
        assert_eq!(
            fresh.scan_one_i32("SELECT id FROM a").await,
            Vec::<i32>::new()
        );
        assert_eq!(
            fresh.scan_one_i32("SELECT id FROM b").await,
            Vec::<i32>::new()
        );
    }

    /// Crash-BEFORE-decision (criterion 5, presumed-abort arm). Both participants
    /// are staged (each has written its `Prepared(-> g)` marker), then the
    /// coordinator router is DROPPED before any global decision is written — the
    /// in-process analog of the coordinator crashing mid-2PC. Dropping the router
    /// runs each participant session's `Drop` (releases row locks); range 0's
    /// global clog never records a decision and `g` is never `finish_global`'d, so
    /// it stays in-doubt forever. A fresh router therefore sees NEITHER row: the
    /// resolver treats every `Prepared(-> g)` tuple whose `g` is still running as
    /// invisible. This is presumed-abort without any active recovery sweep (the
    /// sweep + a durable txn record are SP17's cross-node problem).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn crash_before_global_decision_presumes_abort() {
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        let mut admin = RangeRouter::connect(&c).await;
        admin.simple("CREATE TABLE a (id int4)").await.expect("a"); // id 1 -> range 0
        admin.simple("CREATE TABLE b (id int4)").await.expect("b"); // id 2 -> range 1
        drop(admin);

        // Stage both participants but DO NOT commit: the second INSERT escalates to
        // a global txn and both rows now carry `Prepared(-> g)` markers.
        let mut router = RangeRouter::connect(&c).await;
        router.simple("BEGIN").await.expect("begin");
        router.simple("INSERT INTO a VALUES (1)").await.expect("a");
        router.simple("INSERT INTO b VALUES (2)").await.expect("b");
        assert!(
            router.staged_global_xid().is_some(),
            "txn escalated to a staged global txn"
        );
        // The coordinator crashes mid-2PC, before writing the decision.
        drop(router);

        // A fresh router sees neither row — both are in-doubt (no Committed(g),
        // and g is still in the GTM running-set), so presumed abort.
        let mut fresh = RangeRouter::connect(&c).await;
        assert_eq!(
            fresh.scan_one_i32("SELECT id FROM a").await,
            Vec::<i32>::new(),
            "range-0 row in-doubt after coordinator crash → invisible"
        );
        assert_eq!(
            fresh.scan_one_i32("SELECT id FROM b").await,
            Vec::<i32>::new(),
            "range-1 row in-doubt after coordinator crash → invisible"
        );
    }

    /// Crash-AFTER-decision (criterion 5, commit-durable arm). Both participants are
    /// staged, then the global decision `Committed(g)` is written durably to range
    /// 0's global clog (and `g` is settled) OUT-OF-BAND — modeling a coordinator
    /// that crashes the instant after the decision is durable but BEFORE it releases
    /// the participants (no `commit_release` runs; the router is dropped instead). A
    /// fresh router still sees BOTH rows: range 0's global clog is the sole arbiter,
    /// so a durable `Committed(g)` makes both `Prepared(-> g)` tuples visible even
    /// though the original coordinator never cleaned up. The dropped router's
    /// session `Drop`s release the (now-committed) locks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn crash_after_global_decision_commits() {
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        let mut admin = RangeRouter::connect(&c).await;
        admin.simple("CREATE TABLE a (id int4)").await.expect("a"); // id 1 -> range 0
        admin.simple("CREATE TABLE b (id int4)").await.expect("b"); // id 2 -> range 1
        drop(admin);

        let mut router = RangeRouter::connect(&c).await;
        router.simple("BEGIN").await.expect("begin");
        router.simple("INSERT INTO a VALUES (1)").await.expect("a");
        router.simple("INSERT INTO b VALUES (2)").await.expect("b");
        let g = router
            .staged_global_xid()
            .expect("txn escalated to a staged global txn");

        // The decision is made durable in range 0's global clog and settled in the
        // GTM, but the coordinator crashes (is dropped) before releasing the
        // participants via the normal COMMIT close.
        router
            .coordinator_engine()
            .commit_global_decision(g, mvcc::clog::XidStatus::Committed)
            .await
            .expect("durable global Committed(g)");
        router.coordinator_engine().finish_global(g);
        drop(router); // crash after decision, before commit_release

        // A fresh router sees BOTH rows: the durable Committed(g) is the global
        // arbiter; both Prepared(-> g) tuples resolve to it.
        let mut fresh = RangeRouter::connect(&c).await;
        assert_eq!(fresh.scan_one_i32("SELECT id FROM a").await, vec![1]);
        assert_eq!(fresh.scan_one_i32("SELECT id FROM b").await, vec![2]);
    }

    /// Coordinator honesty (SP18 T2). A participant wins the write-once abort-race:
    /// `Aborted(g)` is written to range 0's global clog BEFORE the coordinator runs
    /// its COMMIT close. Because the global decision is write-once, the coordinator's
    /// `commit_global_decision(g, Committed)` reads back the EFFECTIVE `Aborted`, so
    /// the COMMIT must report an honest `ROLLBACK` (never a false `COMMIT`) and
    /// release every participant with abort semantics — leaving NEITHER row visible.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coordinator_reports_rollback_when_decision_already_aborted() {
        use mvcc::clog::XidStatus;
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        let mut router = RangeRouter::connect(&c).await;
        router.simple("CREATE TABLE a (id int4)").await.expect("a"); // id 1 -> range 0
        router.simple("CREATE TABLE b (id int4)").await.expect("b"); // id 2 -> range 1
        router.simple("BEGIN").await.expect("begin");
        router
            .simple("INSERT INTO a VALUES (1)")
            .await
            .expect("stage a");
        router
            .simple("INSERT INTO b VALUES (2)")
            .await
            .expect("escalate b");
        // A participant wins the abort-race: pre-write Aborted(g) BEFORE the
        // coordinator commits.
        let g = router.staged_global_xid().expect("a global txn is staged");
        router
            .coordinator_engine()
            .commit_global_decision(g, XidStatus::Aborted)
            .await
            .expect("participant aborts g");
        // The coordinator's COMMIT must observe the effective Aborted and report
        // ROLLBACK.
        let tag = router.simple("COMMIT").await.expect("commit returns a tag");
        assert!(
            format!("{tag:?}").contains("ROLLBACK"),
            "coordinator that lost the abort-race reports ROLLBACK, got {tag:?}"
        );
        // Both rows invisible (an aborted cross-range txn leaves neither), to the
        // same router and a fresh one resolving through the global clog.
        assert_eq!(
            router.scan_one_i32("SELECT id FROM a").await,
            Vec::<i32>::new()
        );
        assert_eq!(
            router.scan_one_i32("SELECT id FROM b").await,
            Vec::<i32>::new()
        );
        let mut fresh = RangeRouter::connect(&c).await;
        assert_eq!(
            fresh.scan_one_i32("SELECT id FROM a").await,
            Vec::<i32>::new()
        );
        assert_eq!(
            fresh.scan_one_i32("SELECT id FROM b").await,
            Vec::<i32>::new()
        );
    }
}

#[cfg(test)]
mod gateway_seam_tests {
    use super::*;
    use crate::range::cluster::MultiRangeCluster;

    /// `new` builds a router whose LOCAL engines serve their ranges and whose
    /// forward seam is reached for a range with NO local engine. With a
    /// `RejectForward`, a statement targeting a non-local range surfaces 0A000.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn forward_seam_is_reached_for_a_non_local_range() {
        // Build a 2-range in-process cluster only to mint a real range-1 engine +
        // catalog, then construct a router that is told it holds ONLY range 0
        // locally — so range-1 traffic must hit the forward seam.
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        // Create both tables through the normal (all-local) router first.
        let mut admin = RangeRouter::connect(&c).await;
        admin.simple("CREATE TABLE a (id int4)").await.expect("a"); // id 1 -> range 0
        admin.simple("CREATE TABLE b (id int4)").await.expect("b"); // id 2 -> range 1
        c.wait_for_replication(0).await;

        // A router that holds only range 0 locally; range 1 → RejectForward.
        // `AlwaysLeads` keeps range-0 local execution on (it holds range 0's leader
        // engine); range 1 has no local engine, so it forwards regardless of leads.
        let mut engines = HashMap::new();
        engines.insert(0, c.leader_engine(0).await);
        let mut router = RangeRouter::new(
            c.range_map().clone(),
            engines,
            Arc::new(AlwaysLeads),
            c.catalog_kv().await,
            Arc::new(RejectForward),
            None,
            None,
        );
        // Range-0 work runs locally.
        router
            .simple("INSERT INTO a VALUES (1)")
            .await
            .expect("local range 0");
        // Range-1 work has no local engine → forward seam → 0A000 stub.
        let err = router
            .simple("INSERT INTO b VALUES (2)")
            .await
            .expect_err("no local range-1 engine → forward");
        assert_eq!(err.code, "0A000", "RejectForward stub surfaces 0A000");
    }

    /// A forward seam that executes the forwarded statement on the target range's
    /// real leader engine — the in-process analog of forwarding to the remote leader.
    /// Lets us assert a mixed-range multi-statement frame applies each statement
    /// exactly once (no double-apply from relaying the whole frame).
    struct EngineForward {
        engines: HashMap<RangeId, SqlEngine>,
    }
    impl RemoteForward for EngineForward {
        fn forward<'a>(
            &'a self,
            range: RangeId,
            sql: &'a str,
        ) -> FuturePin<
            Box<dyn std::future::Future<Output = Result<QueryResult, ExecError>> + Send + 'a>,
        > {
            Box::pin(async move {
                let mut s = self
                    .engines
                    .get(&range)
                    .expect("engine for forwarded range")
                    .connect();
                let stmts = pgparser::parse(sql).map_err(ExecError::Parse)?;
                let mut last = QueryResult::Command { tag: "OK".into() };
                for stmt in &stmts {
                    last = s.run(stmt).await?;
                }
                Ok(last)
            })
        }
    }

    /// A single autocommit simple-query frame mixing a LOCAL-leader range (`a`) and a
    /// REMOTE range (`b`) applies each statement EXACTLY ONCE: `a` runs locally and
    /// only `b`'s statement text is forwarded — so `a` is not duplicated by re-running
    /// the whole frame on `b`'s leader. (Regression for the whole-frame-forward bug.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_multi_statement_frame_forwards_each_statement_individually() {
        // boundary at 2: a (id 1) -> range 0 (local), b (id 2) -> range 1 (forwarded).
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        let mut admin = RangeRouter::connect(&c).await;
        admin.simple("CREATE TABLE a (id int4)").await.expect("a");
        admin.simple("CREATE TABLE b (id int4)").await.expect("b");
        c.wait_for_replication(0).await;
        c.wait_for_replication(1).await;

        // Router holds range 0 locally; range 1 forwards to its real leader engine.
        let mut local = HashMap::new();
        local.insert(0, c.leader_engine(0).await);
        let mut remote = HashMap::new();
        remote.insert(1u32, c.leader_engine(1).await);
        let mut router = RangeRouter::new(
            c.range_map().clone(),
            local,
            Arc::new(AlwaysLeads),
            c.catalog_kv().await,
            Arc::new(EngineForward { engines: remote }),
            None,
            None,
        );

        router
            .simple("INSERT INTO a VALUES (1); INSERT INTO b VALUES (2)")
            .await
            .expect("mixed-range multi-statement frame");

        // Exactly one row in each table — the local statement was NOT re-run on the
        // remote node (which a whole-frame forward would have done).
        assert_eq!(
            row_count(&mut router, "SELECT id FROM a").await,
            1,
            "a: exactly one row"
        );
        assert_eq!(
            row_count(&mut router, "SELECT id FROM b").await,
            1,
            "b: exactly one row"
        );
    }

    async fn row_count(r: &mut RangeRouter, sql: &str) -> usize {
        match r.simple(sql).await.expect("select") {
            QueryResult::Rows { rows, .. } => rows.len(),
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    /// SP22 settle-before-serve at the router write path: a locally-led WRITE to a range whose
    /// rise sweep has not settled the current term is rejected (`ExecError::NotLeader` →
    /// retryable 40001), while a READ on the same range passes ungated. Once the gate is opened
    /// for the term (the rise sweep's `mark_served`), the same WRITE succeeds.
    ///
    /// The router is built local-led for every range (the `connect` idiom: all-local `engines` +
    /// `AlwaysLeads`) so a range-1 `UPDATE` genuinely hits the local-led chokepoint
    /// (`engines.contains_key(1) && leads.leads(1)`), the only place the gate check fires.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn router_gates_a_local_led_write_until_the_range_is_settled() {
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        // Seed the schema + a row in range 1 through a normal (ungated) router first.
        let mut admin = RangeRouter::connect(&c).await;
        admin.simple("CREATE TABLE a (id int4)").await.expect("a"); // id 1 -> range 0
        admin.simple("CREATE TABLE b (id int4)").await.expect("b"); // id 2 -> range 1
        admin
            .simple("INSERT INTO b VALUES (20)")
            .await
            .expect("seed b");
        drop(admin);

        // A gate over range 1: id = range-1's leader node so `is_serving`'s
        // `current_leader == Some(id)` can hold; gated-by-default (sentinel term) until
        // `mark_served`.
        let leader1 = c.wait_for_leader(1).await;
        let gate = crate::recovery_gate::RecoveryGate::new(leader1);
        gate.register_range(1, c.leader_raft(1).await);

        // Build a local-led-everything router (the `connect` idiom) but WITH the gate.
        let mut engines = HashMap::new();
        for r in c.range_map().range_ids() {
            engines.insert(r, c.leader_engine(r).await);
        }
        let coordinator: Arc<dyn GlobalCoordinator> = Arc::new(LocalCoordinator {
            range0: c.leader_engine(0).await,
        });
        let mut router = RangeRouter::new(
            c.range_map().clone(),
            engines,
            Arc::new(AlwaysLeads),
            c.catalog_kv().await,
            Arc::new(RejectForward),
            Some(coordinator),
            Some(gate.clone()),
        );

        // Gate closed: a locally-led range-1 WRITE is rejected (retryable NotLeader)...
        let err = router
            .simple("UPDATE b SET id = 21 WHERE id = 20")
            .await
            .expect_err("gated write rejected");
        assert_eq!(
            err.code, "40001",
            "a write to an unsettled range is retryable (NotLeader -> 40001), got {err:?}"
        );
        // ...but a READ on the same range passes ungated.
        assert_eq!(
            router.scan_one_i32("SELECT id FROM b").await,
            vec![20],
            "reads are not gated"
        );

        // Open the gate for the current term → the same write now succeeds.
        let term = c.leader_raft(1).await.metrics().borrow().current_term;
        gate.mark_served(1, term);
        router
            .simple("UPDATE b SET id = 21 WHERE id = 20")
            .await
            .expect("after the gate opens, the write proceeds");
        assert_eq!(
            router.scan_one_i32("SELECT id FROM b").await,
            vec![21],
            "the write applied once the range settled"
        );
    }

    /// Settle-before-serve at the CROSS-RANGE escalation write path (`stage_on`'s local branch) —
    /// the last write path the gate covers. A locally-led participant `Stage` into a global txn
    /// is a version-creating write, so it is gated until the participant range's rise sweep has
    /// settled the current term, exactly like the direct-write `dispatch` check. Without it, a
    /// participant stage on a freshly-risen leader (before its rise sweep reconstructs the
    /// inherited `Prepared(-> g)` markers) could supersede an unsettled committed half with a
    /// non-superseding version and tear the cross-range total.
    ///
    /// A ONE-node cluster leads BOTH ranges on the same node, so a single gate `id` governs both:
    /// range 0 is OPENED (its `acct_a` leg passes `dispatch`'s gate) while range 1 stays CLOSED, so
    /// the escalated `acct_b` leg hits the `stage_on` local-branch gate. The `connect`-idiom router
    /// (all-local engines + `AlwaysLeads`) makes `acct_b`'s stage take that local branch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn router_gates_a_local_led_participant_stage_until_the_range_is_settled() {
        let c = MultiRangeCluster::new(1, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        // Seed acct_a (id 1 -> range 0) + acct_b (id 2 -> range 1) through a normal router.
        let mut admin = RangeRouter::connect(&c).await;
        admin
            .simple("CREATE TABLE acct_a (id int4, bal int4)")
            .await
            .expect("acct_a");
        admin
            .simple("CREATE TABLE acct_b (id int4, bal int4)")
            .await
            .expect("acct_b");
        admin
            .simple("INSERT INTO acct_a VALUES (0, 100)")
            .await
            .expect("seed acct_a");
        admin
            .simple("INSERT INTO acct_b VALUES (0, 100)")
            .await
            .expect("seed acct_b");
        drop(admin);

        // ONE gate over BOTH ranges (id = node 0, which leads both). Open range 0 for its term so
        // the `acct_a` leg passes `dispatch`'s gate; leave range 1 CLOSED (gated-by-default) so the
        // escalated `acct_b` stage is rejected by `stage_on`'s local-branch check.
        let node0 = c.wait_for_leader(0).await;
        let gate = crate::recovery_gate::RecoveryGate::new(node0);
        gate.register_range(0, c.leader_raft(0).await);
        gate.register_range(1, c.leader_raft(1).await);
        gate.mark_served(0, c.leader_raft(0).await.metrics().borrow().current_term);

        let mut engines = HashMap::new();
        for r in c.range_map().range_ids() {
            engines.insert(r, c.leader_engine(r).await);
        }
        let coordinator: Arc<dyn GlobalCoordinator> = Arc::new(LocalCoordinator {
            range0: c.leader_engine(0).await,
        });
        let mut router = RangeRouter::new(
            c.range_map().clone(),
            engines,
            Arc::new(AlwaysLeads),
            c.catalog_kv().await,
            Arc::new(RejectForward),
            Some(coordinator),
            Some(gate.clone()),
        );

        // BEGIN + the `acct_a` leg (range 0, OPEN) pass; the escalated `acct_b` stage (range 1,
        // CLOSED) is rejected — retryable NotLeader -> 40001 — proving the local participant stage
        // is gated. (The `acct_a` leg already passed `dispatch`'s gate, isolating the stage path.)
        router.simple("BEGIN").await.expect("begin");
        router
            .simple("UPDATE acct_a SET bal = bal - 10 WHERE id = 0")
            .await
            .expect("acct_a leg passes (range 0 open)");
        let err = router
            .simple("UPDATE acct_b SET bal = bal + 10 WHERE id = 0")
            .await
            .expect_err("escalated acct_b stage rejected while range 1 is unsettled");
        assert_eq!(
            err.code, "40001",
            "a participant stage to an unsettled range is retryable (NotLeader -> 40001), got {err:?}"
        );
        router
            .simple("ROLLBACK")
            .await
            .expect("rollback half-staged");

        // Open range 1 for its term → a fresh cross-range txn stages `acct_b` locally and commits.
        gate.mark_served(1, c.leader_raft(1).await.metrics().borrow().current_term);
        router.simple("BEGIN").await.expect("begin2");
        router
            .simple("UPDATE acct_a SET bal = bal - 10 WHERE id = 0")
            .await
            .expect("acct_a leg");
        router
            .simple("UPDATE acct_b SET bal = bal + 10 WHERE id = 0")
            .await
            .expect("acct_b stage now admitted once range 1 settled");
        router.simple("COMMIT").await.expect("commit");
        // Conserved: -10 on acct_a, +10 on acct_b — the cross-range txn committed atomically.
        assert_eq!(
            router.scan_one_i32("SELECT bal FROM acct_a").await,
            vec![90]
        );
        assert_eq!(
            router.scan_one_i32("SELECT bal FROM acct_b").await,
            vec![110]
        );
    }
}
