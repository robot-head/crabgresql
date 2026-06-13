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
}

/// Per-connection transaction state. `Failed` carries the aborted block's
/// context so its xid (and any row locks it holds) stay held until
/// COMMIT/ROLLBACK, which records the abort in the clog and releases them.
enum TxnState {
    Idle,
    InTransaction(TxnCtx),
    Failed(TxnCtx),
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
            state: TxnState::Idle,
        }
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
        }
        let snapshot = self.procarray.snapshot();
        self.state = TxnState::InTransaction(TxnCtx {
            xid: None,
            snapshot,
            repeatable_read: rr,
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

    async fn run_select(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        let (snapshot, own) = self.read_context().await?;
        crate::exec::execute_read(&*self.catalog_kv, &*self.kv, &snapshot, own, stmt)
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
                // Autocommit: allocate an xid, run the locking SELECT, then
                // immediately release the locks (implicit txn ends at statement
                // end — there is no open block to hold them).
                let xid = self.procarray.begin_write()?;
                let snapshot = self.procarray.snapshot();
                let kv = Arc::clone(&self.kv);
                let result = crate::exec::execute_read_locking(
                    &*self.catalog_kv,
                    &*kv,
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

    /// The snapshot + own-xid a read should use. Autocommit: a fresh snapshot,
    /// no own xid. In a txn: RC re-snapshots per statement, RR reuses its
    /// snapshot; own xid is the txn's (Some after its first write). Gates before
    /// establishing a fresh snapshot (autocommit + RC); RR was gated at BEGIN.
    async fn read_context(&mut self) -> Result<(Snapshot, Option<u64>), ExecError> {
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
                Ok((self.procarray.snapshot(), None))
            }
            Plan::RcRefresh => {
                self.linearizer.ensure_readable().await?;
                let snap = self.procarray.snapshot();
                match &mut self.state {
                    TxnState::InTransaction(c) => {
                        c.snapshot = snap.clone();
                        Ok((snap, c.xid))
                    }
                    _ => unreachable!(),
                }
            }
            Plan::RrReuse => match &self.state {
                TxnState::InTransaction(c) => Ok((c.snapshot.clone(), c.xid)),
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
                // RC refreshes the read snapshot used by UPDATE/DELETE's scan.
                let refresh =
                    matches!(&self.state, TxnState::InTransaction(c) if !c.repeatable_read);
                if refresh {
                    let s = self.procarray.snapshot();
                    if let TxnState::InTransaction(c) = &mut self.state {
                        c.snapshot = s;
                    }
                }
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
                    &self.procarray,
                    &self.lockmgr,
                    &self.seq,
                    &snapshot,
                    xid,
                    repeatable_read,
                    stmt,
                )
                .await?;
                if self.persist_mode == crate::PersistMode::Replicated {
                    ops.push(self.procarray.next_xid_op());
                }
                self.committer.commit(ops).await?;
                Ok(result)
            }
            TxnState::Idle => {
                // Autocommit: allocate an xid, execute (taking row locks), and
                // commit in one atomic batch (versions + clog). No global writer
                // lock; next_xid was persisted eagerly by begin_write.
                let xid = self.procarray.begin_write()?;
                let snapshot = self.procarray.snapshot();
                let kv = Arc::clone(&self.kv);
                let outcome = crate::exec::execute_write(
                    &*self.catalog_kv,
                    &*kv,
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
}

impl Drop for SqlSession {
    /// A connection dropped mid-transaction (client disconnect) must not leak
    /// its xid in the ProcArray, nor leave its row locks held forever (which
    /// would hang any writer blocked on them). Deregister the xid so it stops
    /// pinning snapshots' xmin, and free its row locks. The uncommitted versions
    /// stay invisible (no clog Committed entry).
    fn drop(&mut self) {
        let xid = match &self.state {
            TxnState::InTransaction(ctx) | TxnState::Failed(ctx) => ctx.xid,
            TxnState::Idle => None,
        };
        if let Some(xid) = xid {
            self.procarray.finish(xid);
            self.lockmgr.release_all(xid);
        }
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
