//! Per-connection session: runs SQL against the shared KV store. SP5 uses
//! PostgreSQL's xid/clog/snapshot MVCC: writes go through to disk tagged with
//! the transaction's xid (read-your-writes via `satisfies_mvcc` + own xid),
//! commit/rollback record the outcome in the clog, and a transaction-scoped
//! async writer lock keeps writers serialized.

use std::sync::Arc;

use kv::Kv;
use mvcc::clog::XidStatus;
use mvcc::visibility::Snapshot;
use pgparser::ast::{IsolationLevel, Statement};
use pgwire::engine::{FieldDescription, QueryResult, Session, TxStatus};
use pgwire::error::PgError;
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::error::ExecError;
use crate::procarray::ProcArray;

/// In-flight transaction context.
pub(crate) struct TxnCtx {
    /// Assigned lazily at the first write (None for a read-only transaction).
    pub(crate) xid: Option<u64>,
    /// The visibility snapshot: re-taken per statement under READ COMMITTED,
    /// fixed at BEGIN under REPEATABLE READ.
    pub(crate) snapshot: Snapshot,
    pub(crate) repeatable_read: bool,
    /// The engine writer lock, held from the first write until COMMIT/ROLLBACK.
    pub(crate) writer_guard: Option<OwnedMutexGuard<()>>,
}

/// Per-connection transaction state. `Failed` carries the aborted block's
/// context so its writer lock and xid stay held until COMMIT/ROLLBACK (which
/// records the abort in the clog and releases them).
enum TxnState {
    Idle,
    InTransaction(TxnCtx),
    Failed(TxnCtx),
}

/// One connection's view of the engine. Holds shared handles to the KV store,
/// the engine-wide async writer mutex, and the shared ProcArray, plus this
/// connection's transaction state. Not shared between connections.
pub struct SqlSession {
    pub(crate) kv: Arc<dyn Kv>,
    writer_lock: Arc<Mutex<()>>,
    procarray: Arc<ProcArray>,
    state: TxnState,
}

impl SqlSession {
    pub(crate) fn new(
        kv: Arc<dyn Kv>,
        writer_lock: Arc<Mutex<()>>,
        procarray: Arc<ProcArray>,
    ) -> Self {
        Self {
            kv,
            writer_lock,
            procarray,
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
            Statement::Begin { isolation } => self.begin(*isolation),
            Statement::Commit => self.commit_cmd(),
            Statement::Rollback => self.rollback_cmd(),
            Statement::CreateTable { .. } | Statement::DropTable { .. } => self.run_ddl(stmt).await,
            Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. } => {
                self.run_write(stmt).await
            }
            Statement::Select(_) => self.run_select(stmt),
        };
        // Any error inside a transaction block aborts it (PostgreSQL 25P02): the
        // block stays Failed (carrying its ctx, so the writer lock and xid stay
        // held) until COMMIT/ROLLBACK releases them. Autocommit errors leave us
        // Idle (the statement was its own transaction).
        if result.is_err()
            && let TxnState::InTransaction(ctx) = std::mem::replace(&mut self.state, TxnState::Idle)
        {
            self.state = TxnState::Failed(ctx);
        }
        result
    }

    /// Record an aborted transaction's outcome (clog Aborted + deregister) and
    /// release its writer lock. Shared by ROLLBACK and COMMIT-of-failed.
    fn abort_ctx(&self, ctx: TxnCtx) -> Result<(), ExecError> {
        if let Some(xid) = ctx.xid {
            // Best-effort abort record; the versions are already invisible
            // (in-progress in no future snapshot once deregistered), so even if
            // this write is lost the rows never become visible.
            self.kv
                .write_batch(&[mvcc::clog::put_op(xid, XidStatus::Aborted)])?;
            self.procarray.finish(xid);
        }
        drop(ctx.writer_guard);
        Ok(())
    }

    fn begin(&mut self, isolation: Option<IsolationLevel>) -> Result<QueryResult, ExecError> {
        if matches!(self.state, TxnState::InTransaction(_)) {
            // BEGIN inside a block is a no-op (PostgreSQL warns and keeps going).
            return Ok(QueryResult::Command {
                tag: "BEGIN".into(),
            });
        }
        let rr = matches!(isolation, Some(IsolationLevel::RepeatableRead));
        // RR fixes its snapshot at BEGIN; RC leaves a placeholder refreshed per
        // statement. Either way we capture the current running set now.
        let snapshot = self.procarray.snapshot();
        self.state = TxnState::InTransaction(TxnCtx {
            xid: None,
            snapshot,
            repeatable_read: rr,
            writer_guard: None,
        });
        Ok(QueryResult::Command {
            tag: "BEGIN".into(),
        })
    }

    fn commit_cmd(&mut self) -> Result<QueryResult, ExecError> {
        match std::mem::replace(&mut self.state, TxnState::Idle) {
            TxnState::InTransaction(ctx) => {
                if let Some(xid) = ctx.xid {
                    // Record the commit. Deregister xid BEFORE propagating any
                    // write error so the xid never stays stuck in the running set.
                    let r = self
                        .kv
                        .write_batch(&[mvcc::clog::put_op(xid, XidStatus::Committed)]);
                    self.procarray.finish(xid);
                    r?;
                }
                drop(ctx.writer_guard); // release the writer lock (if held)
                Ok(QueryResult::Command {
                    tag: "COMMIT".into(),
                })
            }
            // COMMIT of a failed transaction behaves as a ROLLBACK.
            TxnState::Failed(ctx) => {
                self.abort_ctx(ctx)?;
                Ok(QueryResult::Command {
                    tag: "ROLLBACK".into(),
                })
            }
            TxnState::Idle => Ok(QueryResult::Command {
                tag: "COMMIT".into(),
            }),
        }
    }

    fn rollback_cmd(&mut self) -> Result<QueryResult, ExecError> {
        match std::mem::replace(&mut self.state, TxnState::Idle) {
            TxnState::InTransaction(ctx) | TxnState::Failed(ctx) => self.abort_ctx(ctx)?,
            TxnState::Idle => {}
        }
        Ok(QueryResult::Command {
            tag: "ROLLBACK".into(),
        })
    }

    fn run_select(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        let (snapshot, own) = self.read_context()?;
        crate::exec::execute_read(&*self.kv, &snapshot, own, stmt)
    }

    /// The snapshot + own-xid a read should use. Autocommit: a fresh snapshot,
    /// no own xid. In a txn: RC re-snapshots per statement, RR reuses its
    /// snapshot; own xid is the txn's (Some after its first write).
    fn read_context(&mut self) -> Result<(Snapshot, Option<u64>), ExecError> {
        match &mut self.state {
            TxnState::Idle => Ok((self.procarray.snapshot(), None)),
            TxnState::InTransaction(ctx) => {
                if !ctx.repeatable_read {
                    ctx.snapshot = self.procarray.snapshot();
                }
                Ok((ctx.snapshot.clone(), ctx.xid))
            }
            TxnState::Failed(_) => unreachable!("guarded in run_one"),
        }
    }

    async fn run_ddl(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        // DDL is non-transactional: it writes through immediately. If the open
        // transaction already holds the writer lock (it has written), run under
        // that guard — re-acquiring the non-reentrant writer mutex from the same
        // task would deadlock. Otherwise take a short-lived guard.
        let already_held =
            matches!(&self.state, TxnState::InTransaction(c) if c.writer_guard.is_some());
        if already_held {
            crate::exec::execute_ddl(&*self.kv, stmt)
        } else {
            let _guard = self.writer_lock.clone().lock_owned().await;
            crate::exec::execute_ddl(&*self.kv, stmt)
        }
    }

    async fn run_write(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        match &self.state {
            TxnState::InTransaction(_) => {
                self.ensure_write_xid().await?;
                // RC refreshes the read snapshot used by UPDATE/DELETE's scan.
                let refresh =
                    matches!(&self.state, TxnState::InTransaction(c) if !c.repeatable_read);
                if refresh {
                    let s = self.procarray.snapshot();
                    if let TxnState::InTransaction(c) = &mut self.state {
                        c.snapshot = s;
                    }
                }
                let (snapshot, xid) = match &self.state {
                    TxnState::InTransaction(c) => (c.snapshot.clone(), c.xid.expect("xid set")),
                    _ => unreachable!(),
                };
                let kv = Arc::clone(&self.kv);
                // An error here propagates to run_one, which transitions the
                // block to Failed (keeping the lock + xid until COMMIT/ROLLBACK).
                let (result, mut ops) = crate::exec::execute_write(&*kv, &snapshot, xid, stmt)?;
                // Persist next_xid with the statement's writes (no clog entry —
                // the txn commits later).
                ops.push(kv::WriteOp::Put {
                    key: kv::key::next_xid_key(),
                    value: self.procarray.next_xid().to_be_bytes().to_vec(),
                });
                self.kv.write_batch(&ops)?;
                Ok(result)
            }
            TxnState::Idle => {
                // Autocommit: acquire the lock, allocate an xid, execute, and
                // commit in one atomic batch (versions + next_xid + clog).
                let guard = self.writer_lock.clone().lock_owned().await;
                let xid = self.procarray.begin_write();
                let snapshot = self.procarray.snapshot();
                let kv = Arc::clone(&self.kv);
                let outcome = crate::exec::execute_write(&*kv, &snapshot, xid, stmt);
                let (result, mut ops) = match outcome {
                    Ok(v) => v,
                    Err(e) => {
                        // Autocommit error: abort and stay Idle. Record the abort
                        // (best-effort) and deregister; the lock drops with guard.
                        let _ = self
                            .kv
                            .write_batch(&[mvcc::clog::put_op(xid, XidStatus::Aborted)]);
                        self.procarray.finish(xid);
                        drop(guard);
                        return Err(e);
                    }
                };
                ops.push(kv::WriteOp::Put {
                    key: kv::key::next_xid_key(),
                    value: self.procarray.next_xid().to_be_bytes().to_vec(),
                });
                ops.push(mvcc::clog::put_op(xid, XidStatus::Committed));
                // Deregister xid BEFORE propagating any write error so the xid
                // never stays stuck in the running set on a commit-batch failure.
                let r = self.kv.write_batch(&ops);
                self.procarray.finish(xid);
                drop(guard);
                r?;
                Ok(result)
            }
            TxnState::Failed(_) => unreachable!("guarded in run_one"),
        }
    }

    /// On a transaction's first write: acquire the writer lock and allocate the
    /// xid (idempotent on later writes).
    async fn ensure_write_xid(&mut self) -> Result<(), ExecError> {
        let needs = matches!(&self.state, TxnState::InTransaction(c) if c.xid.is_none());
        if !needs {
            return Ok(());
        }
        let guard = self.writer_lock.clone().lock_owned().await;
        let xid = self.procarray.begin_write();
        if let TxnState::InTransaction(c) = &mut self.state {
            c.xid = Some(xid);
            c.writer_guard = Some(guard);
        }
        Ok(())
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
        crate::exec::describe(&*self.kv, sql).map_err(ExecError::into_pg)
    }

    fn tx_status(&self) -> TxStatus {
        match self.state {
            TxnState::Idle => TxStatus::Idle,
            TxnState::InTransaction(_) => TxStatus::InTransaction,
            TxnState::Failed(_) => TxStatus::Failed,
        }
    }
}
