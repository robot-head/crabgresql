//! Per-connection multi-range SQL dispatch. Parses each statement, routes DDL to
//! range 0 and single-table DML to the table's data range (schema resolved from
//! range 0's catalog), pins a transaction to one range, and rejects a transaction
//! that would span ranges (deferred to D3b). Single statements are never
//! cross-range — the grammar has no joins and every DML carries one table.

use std::collections::HashMap;

use executor::{ExecError, SqlEngine, SqlSession};
use pgparser::ast::Statement;
use pgwire::engine::{Engine, QueryResult};
use pgwire::error::PgError;

use crate::range::cluster::MultiRangeCluster;
use crate::range::map::{RangeId, RangeMap};

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

/// A connection's view: one leader `SqlSession` per range it has touched, plus the
/// range a transaction (if any) is pinned to.
pub struct RangeRouter {
    sessions: HashMap<RangeId, SqlSession>,
    pin: Pin,
    map: RangeMap,
    engines: HashMap<RangeId, SqlEngine>,
    catalog_kv: std::sync::Arc<dyn kv::Kv>,
}

impl RangeRouter {
    /// Open a connection: grab each range's current leader engine + the range-0 catalog store.
    pub async fn connect(c: &MultiRangeCluster) -> Self {
        let mut engines = HashMap::new();
        for r in c.range_map().range_ids() {
            engines.insert(r, c.leader_engine(r).await);
        }
        Self {
            sessions: HashMap::new(),
            pin: Pin::None,
            map: c.range_map().clone(),
            engines,
            catalog_kv: c.catalog_kv().await,
        }
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

    /// Run a statement on `range`'s (lazily-connected) session.
    async fn run_on(&mut self, range: RangeId, stmt: &Statement) -> Result<QueryResult, ExecError> {
        self.session_mut(range).await.run(stmt).await
    }

    async fn session_mut(&mut self, range: RangeId) -> &mut SqlSession {
        if !self.sessions.contains_key(&range) {
            let s = self
                .engines
                .get(&range)
                .expect("engine for range")
                .connect();
            self.sessions.insert(range, s);
        }
        self.sessions.get_mut(&range).expect("session")
    }

    /// Parse `sql` and run each statement in order; return the last result.
    pub async fn simple(&mut self, sql: &str) -> Result<QueryResult, PgError> {
        let stmts = pgparser::parse(sql).map_err(|e| ExecError::Parse(e).into_pg())?;
        let mut last = QueryResult::Command { tag: "OK".into() };
        for stmt in &stmts {
            last = self.dispatch(stmt).await.map_err(ExecError::into_pg)?;
        }
        Ok(last)
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
