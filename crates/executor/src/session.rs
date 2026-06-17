//! Per-connection session: runs SQL against the shared KV store. SP6 uses
//! PostgreSQL's xid/clog/snapshot MVCC with concurrent writers: writes go
//! through to disk tagged with the transaction's xid (read-your-writes via
//! `satisfies_mvcc` + own xid), commit/rollback record the outcome in the clog,
//! row-level conflicts serialize through the `RowLockManager` (held until
//! COMMIT/ROLLBACK and freed by `release_all`), and DDL serializes among DDLs
//! behind a small `catalog_lock`.

use std::sync::Arc;

use kv::Kv;
use mvcc::clog::XidStatus;
use mvcc::visibility::Snapshot;
use pgparser::ast::{IsolationLevel, RowLockStrength, Statement};
use pgwire::engine::{FieldDescription, QueryResult, Session, TxStatus};
use pgwire::error::PgError;

use crate::error::ExecError;
use crate::lockmgr::RowLockManager;
use crate::procarray::ProcArray;
use crate::seq::SequenceManager;

/// In-flight transaction context.
pub(crate) struct TxnCtx {
    /// Assigned lazily at the first write (None for a read-only transaction).
    pub(crate) xid: Option<u64>,
    /// The visibility snapshot: re-taken per statement under READ COMMITTED,
    /// fixed at BEGIN under REPEATABLE READ.
    pub(crate) snapshot: Snapshot,
    pub(crate) repeatable_read: bool,
    /// The GLOBAL snapshot the cross-range resolver (`exec::global_status`) gates
    /// `Prepared(-> g)` rows against. Captured at BEGIN for REPEATABLE READ (fixed
    /// for the txn's life), re-captured per statement for READ COMMITTED. `None`
    /// on a non-GTM (single-range) engine — reads then use `NO_GLOBAL_SNAPSHOT()`
    /// and the `Prepared` branch is unreachable.
    pub(crate) global_snapshot: Option<Snapshot>,
    /// The `(table_id, rowid)` set this transaction's local xid has written, in
    /// write order (deduped is unnecessary — the abort-atomicity fence only scans
    /// these rows' versions, and a repeated entry just re-scans). Used by the
    /// cross-range re-stage fence (`effective_global_xid`): when a participant
    /// re-stage lands on a row that already carries an in-doubt `Prepared(-> g_old)`
    /// marker (a prior attempt staged it then its leader died), the `Prepared`
    /// marker this write/`join_global` stamps must ADOPT `g_old` rather than mint a
    /// SECOND version under a fresh `g'` that could commit independently — so each
    /// cross-range row resolves under EXACTLY ONE global decision (abort atomicity).
    pub(crate) written_rows: Vec<(u32, u64)>,
    /// SP37: the transaction-start instant (captured from the session clock at
    /// BEGIN). `now()`/`current_timestamp` are PG transaction-stable, so every
    /// statement in this block evaluates them against this single instant.
    pub(crate) txn_now: jiff::Timestamp,
}

/// Per-connection transaction state. `Failed` carries the aborted block's
/// context so its xid (and any row locks it holds) stay held until
/// COMMIT/ROLLBACK, which records the abort in the clog and releases them.
enum TxnState {
    Idle,
    InTransaction(TxnCtx),
    Failed(TxnCtx),
}

/// SP37: the transactional state of a configuration parameter (currently only
/// `timezone`). Mirrors PostgreSQL's GUC commit/rollback semantics:
///
/// - `committed` is the value that survives across transactions.
/// - `txn_session_override` is a `SET <name>` made inside an open transaction —
///   it becomes the new `committed` value on COMMIT, and is discarded on ROLLBACK.
/// - `txn_local_override` is a `SET LOCAL <name>` — it shadows the session value
///   for the rest of the transaction but is ALWAYS discarded at end-of-transaction
///   (both COMMIT and ROLLBACK), never promoted.
///
/// `effective()` resolves the value a statement sees: a LOCAL override wins, else
/// a (txn) session override, else the committed value. An autocommit `SET` is a
/// transaction of its own, so the session driver applies it then `commit()`s
/// immediately (so it persists); an autocommit `SET LOCAL` is `commit()`ed too,
/// which drops it — matching PostgreSQL.
#[derive(Debug, Clone)]
pub(crate) struct GucState {
    committed: String,
    txn_session_override: Option<String>,
    txn_local_override: Option<String>,
}

impl Default for GucState {
    fn default() -> Self {
        Self {
            committed: "UTC".into(),
            txn_session_override: None,
            txn_local_override: None,
        }
    }
}

impl GucState {
    /// The value a statement sees right now: LOCAL override > session override >
    /// committed.
    pub(crate) fn effective(&self) -> &str {
        self.txn_local_override
            .as_deref()
            .or(self.txn_session_override.as_deref())
            .unwrap_or(&self.committed)
    }

    /// `SET <name> = v` (non-LOCAL): stage a session override (promoted on COMMIT).
    pub(crate) fn set_session(&mut self, v: String) {
        self.txn_session_override = Some(v);
    }

    /// `SET LOCAL <name> = v`: stage a local override (always dropped at txn end).
    pub(crate) fn set_local(&mut self, v: String) {
        self.txn_local_override = Some(v);
    }

    /// `RESET <name>`: stage a session override back to the built-in default
    /// (`UTC`). Like any session override, it is promoted on COMMIT.
    pub(crate) fn reset(&mut self) {
        self.txn_session_override = Some("UTC".into());
    }

    /// End-of-transaction COMMIT: promote any pending session override to the
    /// committed value, and always discard the local override.
    pub(crate) fn commit(&mut self) {
        if let Some(v) = self.txn_session_override.take() {
            self.committed = v;
        }
        self.txn_local_override = None;
    }

    /// End-of-transaction ROLLBACK: discard both pending overrides.
    pub(crate) fn rollback(&mut self) {
        self.txn_session_override = None;
        self.txn_local_override = None;
    }
}

/// Reconstruct the global visibility snapshot from range 0's DURABLE state (never
/// an in-memory running set — correction C2). xmax = next_global_xid; xip = [] (a
/// g < xmax is resolved by reading range 0's global clog directly). Caller must
/// have barriered range 0's replica current first.
pub(crate) fn durable_global_snapshot(range0: &dyn Kv) -> Result<Snapshot, ExecError> {
    use mvcc::xid::GLOBAL_XID_BASE;
    Ok(Snapshot {
        xmin: GLOBAL_XID_BASE,
        xmax: crate::gtm::read_next_global(range0)?,
        xip: vec![],
    })
}

/// One connection's view of the engine. Holds shared handles to the KV store,
/// the ProcArray, the SequenceManager, the RowLockManager, and the DDL catalog
/// lock, plus this connection's transaction state. Not shared between
/// connections.
pub struct SqlSession {
    pub(crate) kv: Arc<dyn Kv>,
    /// The store catalog (schema) lookups resolve through. Same as `kv` for the
    /// single-range engine; range 0's store for a multi-range data node.
    catalog_kv: Arc<dyn Kv>,
    procarray: Arc<ProcArray>,
    seq: Arc<SequenceManager>,
    lockmgr: Arc<RowLockManager>,
    catalog_lock: Arc<tokio::sync::Mutex<()>>,
    committer: Arc<dyn crate::commit::Committer>,
    linearizer: Arc<dyn crate::read_gate::Linearizer>,
    persist_mode: crate::PersistMode,
    /// Range 0's GTM, shared from the engine. `Some` on every range engine of a
    /// multi-range cluster (so any range can capture a global snapshot and
    /// resolve a `Prepared` row); `None` on a single-range engine.
    gtm: Option<Arc<crate::gtm::Gtm>>,
    /// A range-0 read barrier (data-range engines only). Before any read that
    /// consults range 0's global clog (the cross-range resolver), this catches the
    /// node's LOCAL range-0 replica up to range 0's linearizable applied index, so a
    /// `Committed(g)` is actually present when `global_status` reads it. `None` on
    /// range 0's own engine (it reads its own current store) and on a single-range
    /// engine.
    range0_barrier: Option<Arc<dyn crate::read_gate::Linearizer>>,
    /// Set when this session is enlisted as a participant in a cross-range global
    /// txn `g` (Task 4's coordinator calls `join_global`). While set, each local
    /// write also stamps a `Prepared(local_xid -> g)` clog marker and deregisters
    /// the local xid at prepare time. `None` for ordinary single-range txns.
    global_xid: Option<u64>,
    /// SP37: the injectable clock (shared from the engine). Backs the per-statement
    /// `EvalCtx`'s `now`/`stmt_now` and `clock_timestamp()`. `SystemClock` in
    /// production; a `FixedClock` in tests for deterministic temporal evaluation.
    clock: Arc<dyn crate::clock::Clock>,
    /// SP37: the transactional `timezone` GUC. `effective()` feeds the per-statement
    /// `EvalCtx`'s `time_zone`; `SET`/`SHOW`/`RESET timezone` mutate/read it, and
    /// COMMIT/ROLLBACK promote/revert it in lockstep with the transaction outcome.
    guc: GucState,
    state: TxnState,
}

impl SqlSession {
    // Threads the engine's shared handles (kv, procarray, seq, lockmgr, catalog
    // lock, committer, linearizer) plus persist mode into a per-connection
    // session; the count is inherent to the seam, not a smell.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kv: Arc<dyn Kv>,
        catalog_kv: Arc<dyn Kv>,
        procarray: Arc<ProcArray>,
        seq: Arc<SequenceManager>,
        lockmgr: Arc<RowLockManager>,
        catalog_lock: Arc<tokio::sync::Mutex<()>>,
        committer: Arc<dyn crate::commit::Committer>,
        linearizer: Arc<dyn crate::read_gate::Linearizer>,
        persist_mode: crate::PersistMode,
        gtm: Option<Arc<crate::gtm::Gtm>>,
        range0_barrier: Option<Arc<dyn crate::read_gate::Linearizer>>,
        clock: Arc<dyn crate::clock::Clock>,
    ) -> Self {
        Self {
            kv,
            catalog_kv,
            procarray,
            seq,
            lockmgr,
            catalog_lock,
            committer,
            linearizer,
            persist_mode,
            gtm,
            range0_barrier,
            global_xid: None,
            clock,
            guc: GucState::default(),
            state: TxnState::Idle,
        }
    }

    /// Build the per-statement evaluation context. `now` is the transaction-start
    /// instant (PG transaction-stable) inside a txn, else this statement's instant.
    fn eval_ctx(&self) -> crate::clock::EvalCtx {
        let stmt_now = self.clock.now();
        let now = match &self.state {
            TxnState::InTransaction(c) | TxnState::Failed(c) => c.txn_now,
            TxnState::Idle => stmt_now,
        };
        // SP37: the effective session zone (validated at SET time, so `get`
        // succeeds; `unwrap_or(UTC)` is a defensive fallback). `UTC` is
        // special-cased to the const so the common case never touches the tzdb.
        let tzname = self.guc.effective();
        let time_zone = if tzname.eq_ignore_ascii_case("UTC") {
            jiff::tz::TimeZone::UTC
        } else {
            jiff::tz::TimeZone::get(tzname).unwrap_or(jiff::tz::TimeZone::UTC)
        };
        crate::clock::EvalCtx {
            now,
            stmt_now,
            time_zone,
            clock: Arc::clone(&self.clock),
        }
    }

    /// SP37: `SET [LOCAL] <name> = <value>`. Only `timezone` is a real, mutable
    /// parameter; `datestyle`/`intervalstyle` are accepted ONLY at their PG default
    /// (a no-op, so the conformance corpus's standard preamble succeeds), and any
    /// other name is unrecognized (42704). Returns the `SET` command tag.
    ///
    /// Transactional application mirrors PostgreSQL: inside an open block a `SET`
    /// stages a session override and `SET LOCAL` stages a local override (promoted/
    /// reverted by COMMIT/ROLLBACK); in autocommit the change is its own
    /// transaction, so it is applied then immediately committed (a bare `SET LOCAL`
    /// in autocommit is therefore dropped, matching PG).
    fn set_guc(
        &mut self,
        local: bool,
        name: &str,
        value: &pgparser::ast::SetValue,
    ) -> Result<QueryResult, ExecError> {
        use pgparser::ast::SetValue;
        if !name.eq_ignore_ascii_case("timezone") {
            // A handful of parameters are tolerated at their PG default value so a
            // standard session preamble (psql/sqlx) does not error; a non-default
            // value is an invalid value (22023); any other name is unknown (42704).
            let default_ok = match name {
                "datestyle" => {
                    matches!(value, SetValue::Value(v) if v.eq_ignore_ascii_case("ISO, MDY"))
                        || matches!(value, SetValue::Default)
                }
                "intervalstyle" => {
                    matches!(value, SetValue::Value(v) if v.eq_ignore_ascii_case("postgres"))
                        || matches!(value, SetValue::Default)
                }
                _ => return Err(ExecError::UnrecognizedParameter(name.to_string())),
            };
            if !default_ok {
                let shown = match value {
                    SetValue::Value(v) => v.clone(),
                    SetValue::Default => "DEFAULT".into(),
                };
                return Err(ExecError::InvalidParameterValue(shown));
            }
            // Accepted no-op (default value). Still returns the SET tag.
            return Ok(QueryResult::Command { tag: "SET".into() });
        }
        // Resolve the zone string and validate it (UTC special-cases to the const).
        let zone = match value {
            SetValue::Default => "UTC".to_string(),
            SetValue::Value(v) => v.clone(),
        };
        if !zone.eq_ignore_ascii_case("UTC") && jiff::tz::TimeZone::get(&zone).is_err() {
            return Err(ExecError::InvalidParameterValue(zone));
        }
        // Apply with the right transactional scope.
        let in_txn = matches!(self.state, TxnState::InTransaction(_));
        if in_txn {
            if local {
                self.guc.set_local(zone);
            } else {
                self.guc.set_session(zone);
            }
        } else {
            // Autocommit: this SET is its own transaction. A plain SET persists
            // (set_session + commit); a SET LOCAL is committed too, which drops it.
            if local {
                self.guc.set_local(zone);
            } else {
                self.guc.set_session(zone);
            }
            self.guc.commit();
        }
        Ok(QueryResult::Command { tag: "SET".into() })
    }

    /// SP37: `RESET <name>` — reset the parameter to its built-in default. Only
    /// `timezone` is recognized (any other name is 42704). Transactional like SET.
    fn reset_guc(&mut self, name: &str) -> Result<QueryResult, ExecError> {
        if !name.eq_ignore_ascii_case("timezone") {
            return Err(ExecError::UnrecognizedParameter(name.to_string()));
        }
        self.guc.reset();
        if matches!(self.state, TxnState::Idle) {
            self.guc.commit(); // autocommit: persist the reset immediately
        }
        Ok(QueryResult::Command {
            tag: "RESET".into(),
        })
    }

    /// SP37: `SHOW <name>` — return the parameter's effective value as a single
    /// text row (column name `TimeZone`, matching PostgreSQL). Only `timezone` is
    /// recognized (any other name is 42704). A read, so it does NOT mutate the GUC.
    fn show_guc(&self, name: &str) -> Result<QueryResult, ExecError> {
        use bytes::Bytes;
        use pgwire::engine::Cell;
        if !name.eq_ignore_ascii_case("timezone") {
            return Err(ExecError::UnrecognizedParameter(name.to_string()));
        }
        let value = self.guc.effective().as_bytes().to_vec();
        let field = FieldDescription {
            name: "TimeZone".into(),
            table_oid: 0,
            column_id: 0,
            type_oid: pgtypes::ColumnType::Text.oid(),
            type_size: pgtypes::ColumnType::Text.type_size(),
            type_modifier: -1,
            format: 0,
        };
        Ok(QueryResult::Rows {
            fields: vec![field],
            rows: vec![vec![Some(Cell {
                text: Bytes::from(value.clone()),
                binary: Bytes::from(value),
            })]],
            tag: "SHOW".into(),
        })
    }

    /// Catch range 0's LOCAL replica up to its leader's linearizable applied index
    /// (data-range engines only; a no-op on range 0's own engine and single-range
    /// engines). Run AFTER the own-range `linearizer.ensure_readable()` and BEFORE
    /// any read that consults range 0's global clog (the cross-range resolver).
    async fn ensure_global_readable(&self) -> Result<(), ExecError> {
        if let Some(b) = &self.range0_barrier {
            b.ensure_readable().await?;
        }
        Ok(())
    }

    /// Execute one already-parsed statement (the router parses once, then routes).
    pub async fn run(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        self.run_one(stmt).await
    }

    async fn run_one(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        if matches!(self.state, TxnState::Failed(_))
            && !matches!(stmt, Statement::Commit | Statement::Rollback)
        {
            return Err(ExecError::InFailedTransaction);
        }
        let result = match stmt {
            Statement::Begin { isolation } => self.begin(*isolation).await,
            Statement::Commit => self.commit_cmd().await,
            Statement::Rollback => self.rollback_cmd().await,
            Statement::CreateTable { .. } | Statement::DropTable { .. } => self.run_ddl(stmt).await,
            Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. } => {
                self.run_write(stmt).await
            }
            Statement::Select(s) if s.locking.is_some() => self.run_select_locking(s).await,
            Statement::Select(_) => self.run_select(stmt).await,
            // SP38: real handler lands in Task 6; this temporary arm keeps the
            // exhaustive match complete so the crate compiles in the interim.
            Statement::SetOperation(_) => Err(ExecError::Unsupported(
                "set operations not yet wired (SP38 Task 6)".into(),
            )),
            // SP37: GUC control. These are NOT exempt from the failed-txn guard
            // above (only COMMIT/ROLLBACK are), so a SET in an aborted block is
            // rejected — matching PostgreSQL.
            Statement::Set { local, name, value } => self.set_guc(*local, name, value),
            Statement::Reset { name } => self.reset_guc(name),
            Statement::Show { name } => self.show_guc(name),
        };
        // Any error inside a transaction block aborts it (PostgreSQL 25P02): the
        // block stays Failed (carrying its ctx, so the xid and any row locks it
        // holds stay held) until COMMIT/ROLLBACK releases them. Autocommit errors
        // leave us Idle (the statement was its own transaction).
        if result.is_err()
            && let TxnState::InTransaction(ctx) = std::mem::replace(&mut self.state, TxnState::Idle)
        {
            self.state = TxnState::Failed(ctx);
        }
        result
    }

    /// Record an aborted transaction's outcome (clog Aborted + deregister) and
    /// release its row locks. Shared by ROLLBACK and COMMIT-of-failed.
    async fn abort_ctx(&self, ctx: TxnCtx) -> Result<(), ExecError> {
        if let Some(xid) = ctx.xid {
            // Best-effort abort record; the versions are already invisible
            // (in-progress in no future snapshot once deregistered), so even if
            // this write is lost the rows never become visible.
            let r = self
                .committer
                .commit(vec![mvcc::clog::put_op(xid, XidStatus::Aborted)])
                .await;
            // Deregister even if the abort record failed to write: restart
            // re-seeds the ProcArray empty and the rows stay invisible (no clog
            // Committed), so a phantom running xid must not be stranded here.
            self.procarray.finish(xid);
            // Free every row this transaction locked, waking any blocked writers.
            self.lockmgr.release_all(xid);
            r?;
        }
        Ok(())
    }

    async fn begin(&mut self, isolation: Option<IsolationLevel>) -> Result<QueryResult, ExecError> {
        if matches!(self.state, TxnState::InTransaction(_)) {
            // BEGIN inside a block is a no-op (PostgreSQL warns and keeps going).
            return Ok(QueryResult::Command {
                tag: "BEGIN".into(),
            });
        }
        let rr = matches!(isolation, Some(IsolationLevel::RepeatableRead));
        // RR reuses this snapshot for the whole txn, so confirm a linearizable read
        // point BEFORE taking it. RC re-snapshots (and re-gates) per statement, so
        // it leaves a placeholder here and is not gated at BEGIN.
        if rr {
            self.linearizer.ensure_readable().await?;
            self.ensure_global_readable().await?; // range 0 caught up before the gsnap
        }
        let snapshot = self.procarray.snapshot();
        // RR fixes its GLOBAL snapshot at BEGIN too (so a Prepared(-> g) row's
        // in-doubt-ness is stable for the whole txn); RC re-captures per statement,
        // so leave it None here. Reconstructed from range 0's DURABLE state (after
        // the barrier above); NO_GLOBAL_SNAPSHOT() on a single-range engine.
        let global_snapshot = if rr {
            Some(self.global_read_snapshot(None)?)
        } else {
            None
        };
        self.state = TxnState::InTransaction(TxnCtx {
            xid: None,
            snapshot,
            repeatable_read: rr,
            global_snapshot,
            written_rows: Vec::new(),
            // PG transaction-stable `now()`/`current_timestamp`: fix it once at BEGIN.
            txn_now: self.clock.now(),
        });
        Ok(QueryResult::Command {
            tag: "BEGIN".into(),
        })
    }

    async fn commit_cmd(&mut self) -> Result<QueryResult, ExecError> {
        match std::mem::replace(&mut self.state, TxnState::Idle) {
            TxnState::InTransaction(ctx) => {
                if let Some(xid) = ctx.xid {
                    // Record the commit. Deregister xid BEFORE propagating any
                    // write error so the xid never stays stuck in the running set.
                    let mut ops = vec![mvcc::clog::put_op(xid, XidStatus::Committed)];
                    // In Replicated mode, fold the next_xid advance into the
                    // committed batch (the state machine max-merges it). A txn
                    // that allocated its xid only via a locking SELECT (FOR
                    // UPDATE / FOR SHARE) wrote no rows, so without this its
                    // next_xid bump would never reach the replicated state
                    // machine — after failover the new leader would reseed from a
                    // stale next_xid and re-hand-out this xid, whose clog entry is
                    // durably Committed (dirty reads). Redundant-but-harmless for
                    // data-writing txns: their write entry already folded
                    // next_xid and this COMMIT entry is ordered after it.
                    if self.persist_mode == crate::PersistMode::Replicated {
                        ops.push(self.procarray.next_xid_op());
                    }
                    let r = self.committer.commit(ops).await;
                    self.procarray.finish(xid);
                    // Free every row this transaction locked, waking waiters.
                    self.lockmgr.release_all(xid);
                    r?;
                }
                // SP37: a real COMMIT of an open block promotes any staged session
                // GUC override and drops any LOCAL override.
                self.guc.commit();
                Ok(QueryResult::Command {
                    tag: "COMMIT".into(),
                })
            }
            // COMMIT of a failed transaction behaves as a ROLLBACK.
            TxnState::Failed(ctx) => {
                self.abort_ctx(ctx).await?;
                // SP37: a failed block discards every staged GUC override.
                self.guc.rollback();
                Ok(QueryResult::Command {
                    tag: "ROLLBACK".into(),
                })
            }
            TxnState::Idle => Ok(QueryResult::Command {
                tag: "COMMIT".into(),
            }),
        }
    }

    async fn rollback_cmd(&mut self) -> Result<QueryResult, ExecError> {
        match std::mem::replace(&mut self.state, TxnState::Idle) {
            TxnState::InTransaction(ctx) | TxnState::Failed(ctx) => self.abort_ctx(ctx).await?,
            TxnState::Idle => {}
        }
        // SP37: ROLLBACK discards every staged GUC override (session and LOCAL).
        self.guc.rollback();
        Ok(QueryResult::Command {
            tag: "ROLLBACK".into(),
        })
    }

    /// The GLOBAL snapshot a read should resolve `Prepared(-> g)` rows against.
    /// RR reuses the one captured at BEGIN (`stored`); RC / autocommit capture a
    /// fresh one from the GTM. A non-GTM (single-range) engine has no GTM, so this
    /// is `NO_GLOBAL_SNAPSHOT()` and the resolver's `Prepared` branch is
    /// unreachable (no `Prepared` tuple ever exists there).
    fn global_read_snapshot(&self, stored: Option<&Snapshot>) -> Result<Snapshot, ExecError> {
        if let Some(s) = stored {
            return Ok(s.clone()); // RR reuses the durable snapshot taken at BEGIN
        }
        // Any engine that can see cross-range Prepared rows reconstructs gsnap from
        // range 0's DURABLE state. The in-memory GTM running set is NEVER consulted
        // (correction C2): a network commit prunes g on one node only, so a range-0
        // running-set read would hide its own just-committed row cluster-wide.
        if self.gtm.is_some() || self.range0_barrier.is_some() {
            return durable_global_snapshot(&*self.catalog_kv);
        }
        Ok(crate::NO_GLOBAL_SNAPSHOT()) // single-range engine: no global xids exist
    }

    async fn run_select(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        let (snapshot, own, gsnap) = self.read_context().await?;
        let ctx = self.eval_ctx();
        crate::exec::execute_read(
            &*self.catalog_kv,
            &*self.kv,
            &*self.catalog_kv,
            &gsnap,
            &snapshot,
            own,
            stmt,
            &ctx,
        )
    }

    /// Locking SELECT (FOR UPDATE / FOR SHARE). Allocates an xid if none is
    /// active, takes row locks, EvalPlanQual-rechecks each row, and returns
    /// the surviving rows. Autocommit: finish + release_all at statement end
    /// (success and error). In-txn: locks persist until COMMIT/ROLLBACK.
    async fn run_select_locking(
        &mut self,
        s: &pgparser::ast::SelectStmt,
    ) -> Result<QueryResult, ExecError> {
        let mode = match s.locking {
            Some(RowLockStrength::ForUpdate) => crate::lockmgr::LockMode::Exclusive,
            Some(RowLockStrength::ForShare) => crate::lockmgr::LockMode::Shared,
            None => unreachable!("run_one only routes here when locking.is_some()"),
        };

        match &self.state {
            TxnState::InTransaction(_) => {
                // RC re-snapshots (and re-gates) per statement; RR reuses the
                // snapshot fixed and gated at BEGIN. Gate iff we re-snapshot.
                let refresh =
                    matches!(&self.state, TxnState::InTransaction(c) if !c.repeatable_read);
                if refresh {
                    // Gate before any local work (xid allocation, snapshot).
                    self.linearizer.ensure_readable().await?;
                    self.ensure_global_readable().await?; // range 0 caught up too
                }
                // Allocate an xid if the txn has not done a write yet (a FOR
                // UPDATE in a read-only txn still needs one, like PG).
                self.ensure_write_xid()?;
                if refresh {
                    let snap = self.procarray.snapshot();
                    if let TxnState::InTransaction(c) = &mut self.state {
                        c.snapshot = snap;
                    }
                }
                // RC re-captures the global snapshot per statement; RR reuses the
                // one fixed at BEGIN. NO_GLOBAL_SNAPSHOT() on a non-GTM engine.
                let gsnap = match &self.state {
                    TxnState::InTransaction(c) if c.repeatable_read => {
                        self.global_read_snapshot(c.global_snapshot.as_ref())?
                    }
                    _ => self.global_read_snapshot(None)?,
                };
                let (snapshot, xid, repeatable_read) = match &self.state {
                    TxnState::InTransaction(c) => (
                        c.snapshot.clone(),
                        c.xid.expect("xid set by ensure_write_xid"),
                        c.repeatable_read,
                    ),
                    _ => unreachable!(),
                };
                let kv = Arc::clone(&self.kv);
                let ctx = self.eval_ctx();
                // Errors propagate to run_one which transitions to Failed,
                // keeping the xid + locks until COMMIT/ROLLBACK.
                crate::exec::execute_read_locking(
                    &*self.catalog_kv,
                    &*kv,
                    &*self.catalog_kv,
                    &gsnap,
                    &self.procarray,
                    &self.lockmgr,
                    &snapshot,
                    xid,
                    repeatable_read,
                    mode,
                    s,
                    &ctx,
                )
                .await
            }
            TxnState::Idle => {
                // Autocommit read takes a fresh snapshot → gate before any local
                // work (xid allocation, snapshot).
                self.linearizer.ensure_readable().await?;
                self.ensure_global_readable().await?; // range 0 caught up too
                // Autocommit: allocate an xid, run the locking SELECT, then
                // immediately release the locks (implicit txn ends at statement
                // end — there is no open block to hold them).
                let xid = self.procarray.begin_write()?;
                let snapshot = self.procarray.snapshot();
                let gsnap = self.global_read_snapshot(None)?;
                let kv = Arc::clone(&self.kv);
                let ctx = self.eval_ctx();
                let result = crate::exec::execute_read_locking(
                    &*self.catalog_kv,
                    &*kv,
                    &*self.catalog_kv,
                    &gsnap,
                    &self.procarray,
                    &self.lockmgr,
                    &snapshot,
                    xid,
                    false, // autocommit is always READ COMMITTED
                    mode,
                    s,
                    &ctx,
                )
                .await;
                // Release regardless of success or error.
                self.procarray.finish(xid);
                self.lockmgr.release_all(xid);
                result
            }
            TxnState::Failed(_) => unreachable!("guarded in run_one"),
        }
    }

    /// The (local snapshot, own-xid, global snapshot) a read should use.
    /// Autocommit: a fresh local + global snapshot, no own xid. In a txn: RC
    /// re-snapshots both per statement, RR reuses the local + global snapshots
    /// fixed at BEGIN; own xid is the txn's (Some after its first write). Gates
    /// before establishing a fresh snapshot (autocommit + RC); RR was gated at
    /// BEGIN. The global snapshot is `NO_GLOBAL_SNAPSHOT()` on a non-GTM engine.
    async fn read_context(&mut self) -> Result<(Snapshot, Option<u64>, Snapshot), ExecError> {
        enum Plan {
            Auto,
            RcRefresh,
            RrReuse,
        }
        // Decide the plan under a short borrow, then release it before awaiting
        // the gate (no `self` borrow held across the await).
        let plan = match &self.state {
            TxnState::Idle => Plan::Auto,
            TxnState::InTransaction(c) => {
                if c.repeatable_read {
                    Plan::RrReuse
                } else {
                    Plan::RcRefresh
                }
            }
            TxnState::Failed(_) => unreachable!("guarded in run_one"),
        };
        match plan {
            Plan::Auto => {
                self.linearizer.ensure_readable().await?;
                self.ensure_global_readable().await?; // range 0 caught up before the gsnap
                let gsnap = self.global_read_snapshot(None)?;
                Ok((self.procarray.snapshot(), None, gsnap))
            }
            Plan::RcRefresh => {
                self.linearizer.ensure_readable().await?;
                self.ensure_global_readable().await?; // range 0 caught up before the gsnap
                let snap = self.procarray.snapshot();
                // RC re-captures the global snapshot per statement too.
                let gsnap = self.global_read_snapshot(None)?;
                match &mut self.state {
                    TxnState::InTransaction(c) => {
                        c.snapshot = snap.clone();
                        Ok((snap, c.xid, gsnap))
                    }
                    _ => unreachable!(),
                }
            }
            Plan::RrReuse => match &self.state {
                TxnState::InTransaction(c) => {
                    let gsnap = self.global_read_snapshot(c.global_snapshot.as_ref())?;
                    Ok((c.snapshot.clone(), c.xid, gsnap))
                }
                _ => unreachable!(),
            },
        }
    }

    /// DDL is non-transactional and writes through immediately. All DDL funnels
    /// through the leader's catalog_lock held ACROSS the Raft commit, so DDL is
    /// globally serialized (next_table_id read+bump+commit is atomic; low
    /// throughput, fine for D1 — concurrent-DDL optimization is a later slice).
    /// The tokio Mutex is intentionally held across .await (allowed: it is an
    /// async mutex).
    async fn run_ddl(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        let _g = self.catalog_lock.lock().await;
        let (result, ops) = crate::exec::execute_ddl(&*self.kv, stmt)?;
        self.committer.commit(ops).await?;
        Ok(result)
    }

    async fn run_write(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        match &self.state {
            TxnState::InTransaction(_) => {
                self.ensure_write_xid()?;
                // UPDATE/DELETE's eval_plan_qual re-check reads range 0's global clog
                // to resolve a cross-range supersede, so catch range 0's replica up
                // before the gsnap capture. (RR already barriered at BEGIN; the
                // barrier is idempotent.)
                self.ensure_global_readable().await?;
                // RC refreshes the read snapshot used by UPDATE/DELETE's scan.
                let refresh =
                    matches!(&self.state, TxnState::InTransaction(c) if !c.repeatable_read);
                if refresh {
                    let s = self.procarray.snapshot();
                    if let TxnState::InTransaction(c) = &mut self.state {
                        c.snapshot = s;
                    }
                }
                // RC re-captures the global snapshot per statement; RR reuses the
                // one fixed at BEGIN. NO_GLOBAL_SNAPSHOT() on a non-GTM engine. The
                // UPDATE/DELETE re-check resolves a cross-range supersede through it.
                let gsnap = match &self.state {
                    TxnState::InTransaction(c) if c.repeatable_read => {
                        self.global_read_snapshot(c.global_snapshot.as_ref())?
                    }
                    _ => self.global_read_snapshot(None)?,
                };
                let (snapshot, xid, repeatable_read) = match &self.state {
                    TxnState::InTransaction(c) => (
                        c.snapshot.clone(),
                        c.xid.expect("xid set"),
                        c.repeatable_read,
                    ),
                    _ => unreachable!(),
                };
                let kv = Arc::clone(&self.kv);
                let ctx = self.eval_ctx();
                // An error here propagates to run_one, which transitions the
                // block to Failed (keeping the xid + row locks until
                // COMMIT/ROLLBACK, which calls release_all). In Durable mode
                // ProcArray persisted next_xid eagerly, so no next_xid op; the
                // txn commits later, so no clog op. In Replicated mode we fold the
                // next_xid op into this batch (the state machine max-merges it;
                // re-folding on a later write in the same txn is harmless).
                let (result, mut ops) = crate::exec::execute_write(
                    &*self.catalog_kv,
                    &*kv,
                    &*self.catalog_kv,
                    &gsnap,
                    &self.procarray,
                    &self.lockmgr,
                    &self.seq,
                    &snapshot,
                    xid,
                    repeatable_read,
                    stmt,
                    &ctx,
                )
                .await?;
                // Record the (table_id, rowid)s this write touched (from the version
                // Puts it built) so the abort-atomicity fence (`effective_global_xid`)
                // can scan them for an inherited in-doubt `Prepared(-> g_old)` marker.
                // Read BEFORE the marker push below so the fence sees only pre-existing
                // versions (the new `xmin` version is not committed to `self.kv` yet).
                let touched: Vec<(u32, u64)> = ops
                    .iter()
                    .filter_map(|op| match op {
                        kv::WriteOp::Put { key, .. } => kv::key::table_rowid_of(key),
                        _ => None,
                    })
                    .collect();
                if let TxnState::InTransaction(c) = &mut self.state {
                    c.written_rows.extend(touched);
                }
                // A participant in a cross-range global txn `g` stamps a
                // Prepared(xid -> g) marker into the SAME durable batch so the row
                // carries it from the start, and deregisters `xid` from the
                // ProcArray running-set at prepare time (the atomicity linchpin):
                // the local snapshot then no longer gates the row, deferring
                // visibility entirely to range 0's global clog. This also covers
                // the case where the escalation trigger IS this range's first
                // write, so `join_global` had no local xid to backfill. Idempotent
                // on later writes of the same txn (the marker key/value is stable
                // and `finish` is a set-remove). The stamped global xid is FENCED to
                // any in-doubt decision already governing a touched row
                // (`effective_global_xid` — SP24 abort atomicity): a failover re-stage
                // adopts the original `g_old` instead of this attempt's fresh `g`, so a
                // row never carries two competing global decisions.
                if let Some(g) = self.global_xid {
                    let eff = self.effective_global_xid(g)?;
                    self.global_xid = Some(eff);
                    ops.push(mvcc::clog::put_op(xid, XidStatus::Prepared(eff)));
                }
                if self.persist_mode == crate::PersistMode::Replicated {
                    ops.push(self.procarray.next_xid_op());
                }
                self.committer.commit(ops).await?;
                if self.global_xid.is_some() {
                    self.procarray.finish(xid); // deregister-at-prepare
                }
                Ok(result)
            }
            TxnState::Idle => {
                // Autocommit UPDATE/DELETE's eval_plan_qual re-check reads range 0's
                // global clog, so catch range 0's replica up before the gsnap capture.
                self.ensure_global_readable().await?;
                // Autocommit: allocate an xid, execute (taking row locks), and
                // commit in one atomic batch (versions + clog). No global writer
                // lock; next_xid was persisted eagerly by begin_write.
                let xid = self.procarray.begin_write()?;
                let snapshot = self.procarray.snapshot();
                let gsnap = self.global_read_snapshot(None)?;
                let kv = Arc::clone(&self.kv);
                let ctx = self.eval_ctx();
                let outcome = crate::exec::execute_write(
                    &*self.catalog_kv,
                    &*kv,
                    &*self.catalog_kv,
                    &gsnap,
                    &self.procarray,
                    &self.lockmgr,
                    &self.seq,
                    &snapshot,
                    xid,
                    false,
                    stmt,
                    &ctx,
                )
                .await;
                let (result, mut ops) = match outcome {
                    Ok(v) => v,
                    Err(e) => {
                        // Autocommit error: abort and stay Idle. Record the abort
                        // (best-effort), deregister, and free this xid's row locks.
                        let _ = self
                            .committer
                            .commit(vec![mvcc::clog::put_op(xid, XidStatus::Aborted)])
                            .await;
                        self.procarray.finish(xid);
                        self.lockmgr.release_all(xid);
                        return Err(e);
                    }
                };
                ops.push(mvcc::clog::put_op(xid, XidStatus::Committed));
                // In Replicated mode, fold the next_xid advance into the same
                // batch as the rows + clog (the state machine max-merges it); in
                // Durable mode begin_write already persisted it eagerly.
                if self.persist_mode == crate::PersistMode::Replicated {
                    ops.push(self.procarray.next_xid_op());
                }
                // Deregister xid and free its row locks BEFORE propagating any
                // write error so neither the running set nor the lock table is
                // left holding a finished xid on a commit-batch failure.
                let r = self.committer.commit(ops).await;
                self.procarray.finish(xid);
                self.lockmgr.release_all(xid);
                r?;
                Ok(result)
            }
            TxnState::Failed(_) => unreachable!("guarded in run_one"),
        }
    }

    /// On a transaction's first write: allocate the xid (idempotent on later
    /// writes). No lock — concurrency is row-level via the RowLockManager.
    fn ensure_write_xid(&mut self) -> Result<(), ExecError> {
        let needs = matches!(&self.state, TxnState::InTransaction(c) if c.xid.is_none());
        if !needs {
            return Ok(());
        }
        let xid = self.procarray.begin_write()?;
        if let TxnState::InTransaction(c) = &mut self.state {
            c.xid = Some(xid);
        }
        Ok(())
    }

    /// The current transaction's local xid, if one has been allocated (`None` for
    /// an idle session or a read-only txn that has not yet written). For a
    /// participant in a global txn this is the per-range local `Li` the
    /// `Prepared(Li -> g)` marker ties to the global `g`.
    pub fn local_xid(&self) -> Option<u64> {
        match &self.state {
            TxnState::InTransaction(c) | TxnState::Failed(c) => c.xid,
            TxnState::Idle => None,
        }
    }

    /// Begin a held txn on this session if it is Idle, so a participant's first
    /// DML is HELD (never autocommitted): the coordinator can then drive its
    /// COMMIT/ROLLBACK and the `Prepared` marker is written before any of its rows
    /// become eligible to commit on their own. Idempotent (no-op if already in a
    /// txn). Reuses `begin`.
    pub async fn ensure_began(&mut self) -> Result<(), ExecError> {
        if matches!(self.state, TxnState::Idle) {
            self.begin(None).await?;
        }
        Ok(())
    }

    /// The global xid the `Prepared(Li -> ·)` marker for THIS participant write must
    /// carry — the **abort-atomicity fence**. Normally `g` (the txn's own global xid),
    /// but if any row this txn's local xid `Li` has written already carries a SIBLING
    /// version under an in-doubt `Prepared(-> g_old)` marker for a DIFFERENT global
    /// txn `g_old`, the marker ADOPTS `g_old` instead.
    ///
    /// Why: a participant whose leader is killed mid-cross-range-txn loses its in-memory
    /// held session; the coordinator/worker retries the WHOLE transfer under a FRESH
    /// global `g'`, re-staging the same row on the NEW leader. Without this fence the
    /// re-stage mints a SECOND live version of the row stamped `Prepared(-> g')`, so the
    /// row is governed by TWO independent global decisions (`g_old` and `g'`); if
    /// `g_old` aborts but `g'` commits the `g'`-version stays visible — money created or
    /// destroyed (the SP24 abort-atomicity half-leak). Adopting `g_old` keeps the row
    /// under EXACTLY ONE global decision: when `g_old` is later aborted by the recovery
    /// abort-race, every version of the row resolves invisible (the pre-txn value
    /// re-surfaces); when `g_old` commits, exactly one version is live. The retry's `g'`
    /// then governs no version of this row — which is correct, since the row was already
    /// enlisted in `g_old`.
    ///
    /// Only an IN-DOUBT `g_old` is adopted (read range 0's global clog via `catalog_kv`,
    /// which the caller has already barriered current): a `g_old` that is already
    /// terminally decided imposes no surviving enlistment, so the write proceeds under
    /// its own `g`. Bank txns touch one row per range, so at most one `g_old` is found;
    /// if several rows disagree the smallest in-doubt `g_old` is adopted deterministically
    /// (canonical, fingerprint-stable). A non-GTM (single-range) engine has no global
    /// clog and no `Prepared` rows, so this returns `g` unchanged.
    fn effective_global_xid(&self, g: u64) -> Result<u64, ExecError> {
        let li = match self.local_xid() {
            Some(li) => li,
            None => return Ok(g), // no local write yet → nothing to fence
        };
        let written = match &self.state {
            TxnState::InTransaction(c) | TxnState::Failed(c) => &c.written_rows,
            TxnState::Idle => return Ok(g),
        };
        let mut adopted: Option<u64> = None;
        for &(table_id, rowid) in written {
            let prefix = kv::key::row_key(table_id, rowid);
            for (_k, v) in self.kv.scan_prefix(&prefix)? {
                let (xmin, _xmax, _row) = mvcc::version::decode_tuple(&v)?;
                if xmin == li {
                    continue; // this txn's OWN version — never fences itself
                }
                // A sibling version under an in-doubt `Prepared(-> g_old != g)` marker
                // means `g_old` still governs this row; adopt it.
                if let XidStatus::Prepared(g_old) = mvcc::clog::get(self.kv.as_ref(), xmin)?
                    && g_old != g
                    && !matches!(
                        mvcc::clog::get(self.catalog_kv.as_ref(), g_old)?,
                        XidStatus::Committed | XidStatus::Aborted
                    )
                {
                    adopted = Some(adopted.map_or(g_old, |a| a.min(g_old)));
                }
            }
        }
        Ok(adopted.unwrap_or(g))
    }

    /// Enlist this session as a participant of global txn `g`. If it has already
    /// done a write (local xid `Li` allocated), write the `Prepared(Li -> g)`
    /// marker durably AND deregister `Li` from the ProcArray running-set so the
    /// local snapshot no longer gates its rows — range 0's global clog becomes the
    /// sole arbiter, which is what makes both ranges flip visible atomically at
    /// the single `Committed(g)` instant (the deregister-at-PREPARE linchpin). If
    /// no write has happened yet there is nothing to backfill: the first write's
    /// commit batch (see `run_write`) carries the marker and deregisters then.
    /// Idempotent. The stamped marker is FENCED to any in-doubt global decision
    /// already governing this txn's written rows (`effective_global_xid` — SP24
    /// abort atomicity), so a failover re-stage never mints a second version under a
    /// competing decision.
    pub async fn join_global(&mut self, g: u64) -> Result<(), ExecError> {
        self.global_xid = Some(g);
        if let Some(local) = self.local_xid() {
            let eff = self.effective_global_xid(g)?;
            self.global_xid = Some(eff);
            self.committer
                .commit(vec![mvcc::clog::put_op(local, XidStatus::Prepared(eff))])
                .await?;
            self.procarray.finish(local); // deregister-at-PREPARE (the atomicity linchpin)
        }
        Ok(())
    }

    /// Release this participant's resources after the coordinator's global COMMIT.
    /// The rows are already `Prepared` + durable and their local xid is already
    /// deregistered, so the single `Committed(g)` write makes them visible; here
    /// we only free row locks and reset to Idle (NO per-participant clog write).
    pub fn commit_release(&mut self) {
        self.finish_current_txn();
    }

    /// Release this participant's resources after the coordinator's global ABORT.
    /// The rows stay invisible (range 0's global clog is absent/`Aborted(g)`); we
    /// only free row locks and reset to Idle (NO per-participant clog write).
    pub fn abort_release(&mut self) {
        self.finish_current_txn();
    }

    /// Deregister the current txn's xid from the ProcArray and free its row locks,
    /// then reset to Idle. Writes NO clog entry — used by `Drop` (presumed-abort
    /// on disconnect) and by the global participant `commit_release`/`abort_release`
    /// (the decision was recorded once, globally, by the coordinator).
    fn finish_current_txn(&mut self) {
        if let Some(xid) = self.local_xid() {
            self.procarray.finish(xid);
            self.lockmgr.release_all(xid);
        }
        self.global_xid = None;
        self.state = TxnState::Idle;
    }
}

impl Drop for SqlSession {
    /// A connection dropped mid-transaction (client disconnect) must not leak
    /// its xid in the ProcArray, nor leave its row locks held forever (which
    /// would hang any writer blocked on them). Deregister the xid so it stops
    /// pinning snapshots' xmin, and free its row locks. The uncommitted versions
    /// stay invisible (no clog Committed entry). This is presumed-abort: a global
    /// participant dropped before the coordinator's decision releases its locks
    /// and its rows never become visible (range 0's global clog has no
    /// `Committed(g)`).
    fn drop(&mut self) {
        self.finish_current_txn();
    }
}

impl Session for SqlSession {
    async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        if sql.trim().is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        let statements = pgparser::parse(sql).map_err(|e| ExecError::from(e).into_pg())?;
        if statements.is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        let mut results = Vec::with_capacity(statements.len());
        for stmt in statements {
            results.push(self.run_one(&stmt).await.map_err(ExecError::into_pg)?);
        }
        Ok(results)
    }

    async fn describe(&mut self, sql: &str) -> Result<Vec<FieldDescription>, PgError> {
        crate::exec::describe(&*self.catalog_kv, &*self.kv, sql).map_err(ExecError::into_pg)
    }

    fn tx_status(&self) -> TxStatus {
        match self.state {
            TxnState::Idle => TxStatus::Idle,
            TxnState::InTransaction(_) => TxStatus::InTransaction,
            TxnState::Failed(_) => TxStatus::Failed,
        }
    }
}

#[cfg(test)]
mod tests {
    use pgwire::engine::{Engine, Session};

    use crate::SqlEngine;

    /// SP37: the GUC transactional state machine — PostgreSQL's commit-keeps,
    /// rollback-reverts, and SET-LOCAL-always-reverts semantics for `timezone`.
    #[test]
    fn guc_timezone_transactional_semantics() {
        use crate::session::GucState;
        let mut g = GucState::default();
        assert_eq!(g.effective(), "UTC");
        g.set_session("America/New_York".into());
        g.commit();
        assert_eq!(g.effective(), "America/New_York");
        g.set_session("UTC".into());
        assert_eq!(g.effective(), "UTC");
        g.rollback();
        assert_eq!(g.effective(), "America/New_York");
        g.set_session("UTC".into());
        g.commit();
        assert_eq!(g.effective(), "UTC");
        g.set_local("America/New_York".into());
        assert_eq!(g.effective(), "America/New_York");
        g.commit();
        assert_eq!(g.effective(), "UTC");
        g.set_session("America/New_York".into());
        g.commit();
        g.reset();
        g.commit();
        assert_eq!(g.effective(), "UTC");
    }

    /// Extract the single text cell of a one-row, one-column result.
    fn single_text(results: &[pgwire::engine::QueryResult]) -> String {
        use pgwire::engine::QueryResult;
        match results {
            [QueryResult::Rows { rows, .. }] => {
                let cell = rows[0][0].as_ref().expect("non-null cell");
                String::from_utf8(cell.text.to_vec()).expect("utf8")
            }
            other => panic!("expected one Rows result, got {other:?}"),
        }
    }

    /// SP37: `SET TIME ZONE` flows through the GUC into `eval_ctx()`, so a
    /// `timestamptz` renders in the session zone; `SHOW timezone` reads it back;
    /// and a ROLLBACK reverts a `SET` made inside a transaction.
    #[tokio::test]
    async fn set_timezone_flows_into_rendering_and_show() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();

        // Default zone is UTC.
        let show = s.simple_query("SHOW timezone").await.expect("show");
        assert_eq!(single_text(&show), "UTC");
        let utc = s
            .simple_query("SELECT TIMESTAMPTZ '2024-01-15 12:00:00+00'")
            .await
            .expect("select utc");
        assert_eq!(single_text(&utc), "2024-01-15 12:00:00+00");

        // SET TIME ZONE (autocommit) persists and feeds eval_ctx().
        s.simple_query("SET TIME ZONE 'America/New_York'")
            .await
            .expect("set tz");
        let show_ny = s.simple_query("SHOW timezone").await.expect("show ny");
        assert_eq!(single_text(&show_ny), "America/New_York");
        let ny = s
            .simple_query("SELECT TIMESTAMPTZ '2024-01-15 12:00:00+00'")
            .await
            .expect("select ny");
        assert_eq!(single_text(&ny), "2024-01-15 07:00:00-05");

        // A SET inside a transaction reverts on ROLLBACK.
        s.simple_query("BEGIN").await.expect("begin");
        s.simple_query("SET TIME ZONE 'UTC'")
            .await
            .expect("set utc");
        let inside = s.simple_query("SHOW timezone").await.expect("show inside");
        assert_eq!(single_text(&inside), "UTC");
        s.simple_query("ROLLBACK").await.expect("rollback");
        let after = s.simple_query("SHOW timezone").await.expect("show after");
        assert_eq!(single_text(&after), "America/New_York");

        // A SET inside a transaction persists on COMMIT.
        s.simple_query("BEGIN").await.expect("begin2");
        s.simple_query("SET TIME ZONE 'UTC'")
            .await
            .expect("set utc2");
        s.simple_query("COMMIT").await.expect("commit");
        let committed = s
            .simple_query("SHOW timezone")
            .await
            .expect("show committed");
        assert_eq!(single_text(&committed), "UTC");
    }

    /// SP37: SET LOCAL is always reverted at end-of-transaction; an unknown
    /// parameter is 42704; a bad zone is 22023.
    #[tokio::test]
    async fn set_local_reverts_and_errors_have_right_sqlstate() {
        let engine = SqlEngine::new();
        let mut s = engine.connect();

        s.simple_query("BEGIN").await.expect("begin");
        s.simple_query("SET LOCAL TIME ZONE 'America/New_York'")
            .await
            .expect("set local");
        let inside = s.simple_query("SHOW timezone").await.expect("show local");
        assert_eq!(single_text(&inside), "America/New_York");
        // COMMIT drops a LOCAL override (never promoted).
        s.simple_query("COMMIT").await.expect("commit");
        let after = s.simple_query("SHOW timezone").await.expect("show after");
        assert_eq!(single_text(&after), "UTC");

        // Unknown parameter → 42704.
        let unknown = s
            .simple_query("SET nonexistent_param = 'x'")
            .await
            .expect_err("unknown param");
        assert_eq!(unknown.code, "42704");
        let unknown_show = s
            .simple_query("SHOW nonexistent_param")
            .await
            .expect_err("unknown show");
        assert_eq!(unknown_show.code, "42704");

        // Invalid zone → 22023.
        let bad = s
            .simple_query("SET timezone = 'Not/AZone'")
            .await
            .expect_err("bad zone");
        assert_eq!(bad.code, "22023");
    }

    /// A session dropped while a write transaction is open (client disconnect)
    /// must deregister its xid from the ProcArray so it no longer pins
    /// `snapshot().xmin`.
    #[tokio::test]
    async fn dropping_a_session_mid_txn_deregisters_its_xid() {
        let engine = SqlEngine::new();

        {
            let mut s = engine.connect();
            s.simple_query("CREATE TABLE t (id int4)")
                .await
                .expect("create");
            s.simple_query("BEGIN").await.expect("begin");
            s.simple_query("INSERT INTO t VALUES (1)")
                .await
                .expect("insert");
            assert_eq!(
                engine.procarray.running_len(),
                1,
                "xid must be registered while the transaction is open"
            );
            // s is dropped here, mid-transaction (no COMMIT/ROLLBACK)
        }

        assert_eq!(
            engine.procarray.running_len(),
            0,
            "xid must be deregistered when the session is dropped mid-transaction"
        );
    }
}
