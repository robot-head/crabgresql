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
}

/// Per-connection transaction state. `Failed` carries the aborted block's
/// context so its xid (and any row locks it holds) stay held until
/// COMMIT/ROLLBACK, which records the abort in the clog and releases them.
enum TxnState {
    Idle,
    InTransaction(TxnCtx),
    Failed(TxnCtx),
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
            state: TxnState::Idle,
        }
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
            Some(self.global_read_snapshot(None))
        } else {
            None
        };
        self.state = TxnState::InTransaction(TxnCtx {
            xid: None,
            snapshot,
            repeatable_read: rr,
            global_snapshot,
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
                Ok(QueryResult::Command {
                    tag: "COMMIT".into(),
                })
            }
            // COMMIT of a failed transaction behaves as a ROLLBACK.
            TxnState::Failed(ctx) => {
                self.abort_ctx(ctx).await?;
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
        Ok(QueryResult::Command {
            tag: "ROLLBACK".into(),
        })
    }

    /// The GLOBAL snapshot a read should resolve `Prepared(-> g)` rows against.
    /// RR reuses the one captured at BEGIN (`stored`); RC / autocommit capture a
    /// fresh one from the GTM. A non-GTM (single-range) engine has no GTM, so this
    /// is `NO_GLOBAL_SNAPSHOT()` and the resolver's `Prepared` branch is
    /// unreachable (no `Prepared` tuple ever exists there).
    fn global_read_snapshot(&self, stored: Option<&Snapshot>) -> Snapshot {
        if let Some(s) = stored {
            return s.clone(); // RR reuses the durable snapshot taken at BEGIN
        }
        // Any engine that can see cross-range Prepared rows reconstructs gsnap from
        // range 0's DURABLE state. The in-memory GTM running set is NEVER consulted
        // (correction C2): a network commit prunes g on one node only, so a range-0
        // running-set read would hide its own just-committed row cluster-wide.
        if self.gtm.is_some() || self.range0_barrier.is_some() {
            return durable_global_snapshot(&*self.catalog_kv)
                .unwrap_or_else(|_| crate::NO_GLOBAL_SNAPSHOT());
        }
        crate::NO_GLOBAL_SNAPSHOT() // single-range engine: no global xids exist
    }

    async fn run_select(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        let (snapshot, own, gsnap) = self.read_context().await?;
        crate::exec::execute_read(
            &*self.catalog_kv,
            &*self.kv,
            &*self.catalog_kv,
            &gsnap,
            &snapshot,
            own,
            stmt,
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
                        self.global_read_snapshot(c.global_snapshot.as_ref())
                    }
                    _ => self.global_read_snapshot(None),
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
                let gsnap = self.global_read_snapshot(None);
                let kv = Arc::clone(&self.kv);
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
                let gsnap = self.global_read_snapshot(None);
                Ok((self.procarray.snapshot(), None, gsnap))
            }
            Plan::RcRefresh => {
                self.linearizer.ensure_readable().await?;
                self.ensure_global_readable().await?; // range 0 caught up before the gsnap
                let snap = self.procarray.snapshot();
                // RC re-captures the global snapshot per statement too.
                let gsnap = self.global_read_snapshot(None);
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
                    let gsnap = self.global_read_snapshot(c.global_snapshot.as_ref());
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
                        self.global_read_snapshot(c.global_snapshot.as_ref())
                    }
                    _ => self.global_read_snapshot(None),
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
                )
                .await?;
                // A participant in a cross-range global txn `g` stamps a
                // Prepared(xid -> g) marker into the SAME durable batch so the row
                // carries it from the start, and deregisters `xid` from the
                // ProcArray running-set at prepare time (the atomicity linchpin):
                // the local snapshot then no longer gates the row, deferring
                // visibility entirely to range 0's global clog. This also covers
                // the case where the escalation trigger IS this range's first
                // write, so `join_global` had no local xid to backfill. Idempotent
                // on later writes of the same txn (the marker key/value is stable
                // and `finish` is a set-remove).
                if let Some(g) = self.global_xid {
                    ops.push(mvcc::clog::put_op(xid, XidStatus::Prepared(g)));
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
                let gsnap = self.global_read_snapshot(None);
                let kv = Arc::clone(&self.kv);
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

    /// Enlist this session as a participant of global txn `g`. If it has already
    /// done a write (local xid `Li` allocated), write the `Prepared(Li -> g)`
    /// marker durably AND deregister `Li` from the ProcArray running-set so the
    /// local snapshot no longer gates its rows — range 0's global clog becomes the
    /// sole arbiter, which is what makes both ranges flip visible atomically at
    /// the single `Committed(g)` instant (the deregister-at-PREPARE linchpin). If
    /// no write has happened yet there is nothing to backfill: the first write's
    /// commit batch (see `run_write`) carries the marker and deregisters then.
    /// Idempotent.
    pub async fn join_global(&mut self, g: u64) -> Result<(), ExecError> {
        self.global_xid = Some(g);
        if let Some(local) = self.local_xid() {
            self.committer
                .commit(vec![mvcc::clog::put_op(local, XidStatus::Prepared(g))])
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
