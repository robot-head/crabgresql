//! Per-connection multi-range SQL dispatch. Parses each statement, routes DDL to
//! range 0 and single-table DML to the table's data range (schema resolved from
//! range 0's catalog), pins a transaction to one range, and rejects a transaction
//! that would span ranges (deferred to D3b). Single statements are never
//! cross-range — the grammar has no joins and every DML carries one table.

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
/// on a different range can be rejected. (A bare `Option<RangeId>` conflated
/// "provisional, unpinned" with "pinned to range 0".)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pin {
    /// No open transaction (autocommit): each statement routes to its own range.
    None,
    /// Inside BEGIN..COMMIT, not yet pinned by a table-bearing statement.
    Open,
    /// Inside BEGIN..COMMIT, pinned to this range by the first DML / FROM SELECT.
    Range(RangeId),
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
    /// Range-0 catalog store (schema resolution). For a range-0 follower gateway
    /// Task 4 makes this a wire-read handle; here it is the local range-0 store.
    catalog_kv: Arc<dyn kv::Kv>,
    /// Forwards a statement whose range has no local engine.
    forward: Arc<dyn RemoteForward>,
    /// The EXACT source text of the statement currently being dispatched (set per
    /// statement from `parse_with_source`) — what the forward seam relays for a
    /// non-local range. Per-statement, NOT the whole `;`-separated frame, so a frame
    /// mixing a local and a remote range forwards only the remote statement and never
    /// re-runs the local one on the remote node.
    cur_sql: String,
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
    ) -> Self {
        Self {
            sessions: HashMap::new(),
            pin: Pin::None,
            map,
            engines,
            leads,
            catalog_kv,
            forward,
            cur_sql: String::new(),
        }
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
        Self::new(
            c.range_map().clone(),
            engines,
            Arc::new(AlwaysLeads),
            c.catalog_kv().await,
            Arc::new(RejectForward),
        )
    }

    /// The concrete data range a *table-bearing* statement targets — the only kind
    /// that pins a transaction. `Insert`/`Update`/`Delete` and a `SELECT ... FROM t`
    /// carry exactly one table; everything else (DDL, txn-control, `SELECT` with no
    /// FROM) carries no table and returns `None`, so it never pins.
    fn pinning_range(&self, stmt: &Statement) -> Result<Option<RangeId>, ExecError> {
        match stmt {
            Statement::Insert { table, .. }
            | Statement::Update { table, .. }
            | Statement::Delete { table, .. } => self.range_of(table).map(Some),
            Statement::Select(s) => match &s.from {
                Some(name) => self.range_of(name).map(Some),
                None => Ok(None),
            },
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
        match stmt {
            Statement::Begin { .. } => {
                // Idempotent like PG: a BEGIN inside a block leaves the pin as-is.
                if self.pin == Pin::None {
                    self.pin = Pin::Open;
                }
                return self.run_on(0, stmt).await;
            }
            Statement::Commit | Statement::Rollback => {
                let exec = match self.pin {
                    Pin::Range(r) => r,
                    Pin::Open | Pin::None => 0,
                };
                self.pin = Pin::None;
                return self.run_on(exec, stmt).await;
            }
            _ => {}
        }

        match self.pin {
            // Autocommit: route each statement independently to its target range
            // (table-bearing -> its range; otherwise range 0).
            Pin::None => self.run_on(pinning.unwrap_or(0), stmt).await,
            // The first table-bearing statement of the txn pins it to that
            // statement's range — even range 0. Thereafter a table-bearing
            // statement on a *different* range is rejected (the `Pin::Range` arm
            // below, SQLSTATE 0A000), so a txn whose first DML is on range 0 is
            // correctly held single-range.
            //
            // KNOWN D3a LIMITATION (no data-integrity or durability impact; full
            // cross-range / 2PC semantics are deferred to D3b): if a txn opened
            // with BEGIN runs only range-0 work (DDL / FROM-less SELECT) — staying
            // `Pin::Open` — and then its first *table-bearing* statement lands on a
            // NON-range-0 range, the BEGIN executed only on range 0's `SqlSession`,
            // so the data range's session is still `TxnState::Idle`. That DML
            // therefore runs through `run_write`'s AUTOCOMMIT branch (commits at
            // once in a single Raft batch) instead of being held until COMMIT, and
            // a later ROLLBACK on that range is a no-op (the row already committed).
            // The row still commits atomically and durably through the correct
            // range's consensus — only cross-range transactionality is loose. This
            // mirrors existing behavior: DDL is already non-transactional
            // (`run_ddl`) and a FROM-less SELECT carries no transactional payload.
            Pin::Open => {
                let exec = match pinning {
                    Some(r) => {
                        self.pin = Pin::Range(r);
                        r
                    }
                    None => 0, // DDL / FROM-less SELECT: run on range 0, stay unpinned.
                };
                self.run_on(exec, stmt).await
            }
            // Already pinned: a table-bearing statement on another range is rejected;
            // range-0-targeting (no-table) statements run on the pinned session.
            Pin::Range(p) => {
                if let Some(r) = pinning
                    && r != p
                {
                    return Err(ExecError::Unsupported(
                        "a transaction may not span ranges yet (D3b)".into(),
                    ));
                }
                self.run_on(p, stmt).await
            }
        }
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
            Pin::Open | Pin::Range(_) => pgwire::engine::TxStatus::InTransaction,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_transaction_may_not_span_ranges() {
        let c = MultiRangeCluster::new(3, RangeMap::with_boundaries(vec![2])).await;
        for r in c.range_map().range_ids() {
            c.wait_for_leader(r).await;
        }
        let mut router = RangeRouter::connect(&c).await;
        router
            .simple("CREATE TABLE a (id int4)")
            .await
            .expect("create a");
        router
            .simple("CREATE TABLE b (id int4)")
            .await
            .expect("create b");
        router.simple("BEGIN").await.expect("begin");
        router
            .simple("INSERT INTO a VALUES (1)")
            .await
            .expect("first DML pins range 0");
        let err = router
            .simple("INSERT INTO b VALUES (2)")
            .await
            .expect_err("a second range in one txn must be rejected");
        assert_eq!(err.code, "0A000");
        router.simple("ROLLBACK").await.ok();
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
}
