//! Per-connection session: runs SQL against the shared KV store. SP4 Task 5
//! adds the transaction state machine — BEGIN/COMMIT/ROLLBACK, an in-memory
//! write-set buffered until COMMIT, write-set-overlay reads (read-your-writes),
//! and READ COMMITTED vs REPEATABLE READ snapshots.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use catalog::TableId;
use kv::Kv;
use pgparser::ast::{IsolationLevel, Statement};
use pgwire::engine::{FieldDescription, QueryResult, Session, TxStatus};
use pgwire::error::PgError;

use crate::error::ExecError;

/// A buffered write in a transaction's write-set, keyed by `(TableId, rowid)`.
#[derive(Clone)]
pub(crate) enum Pending {
    /// An encoded live-row version value — already `encode_version(false, row)`.
    Row(Vec<u8>),
    /// A delete: the row is invisible to this txn and tombstoned at COMMIT.
    /// Constructed by UPDATE/DELETE in Task 6; the read/flush paths already
    /// handle it so the overlay and commit are complete now.
    #[allow(dead_code)]
    Tombstone,
}

/// In-flight transaction context: the snapshot the txn reads at, its isolation,
/// the buffered write-set, and per-table next-rowid counters.
#[derive(Default)]
pub(crate) struct TxnCtx {
    pub(crate) snapshot: u64,
    pub(crate) repeatable_read: bool,
    pub(crate) writes: HashMap<(TableId, u64), Pending>,
    pub(crate) seq: HashMap<TableId, u64>,
}

/// Per-connection transaction state.
enum TxnState {
    Idle,
    InTransaction(TxnCtx),
    Failed,
}

/// One connection's view of the engine. Holds shared handles to the KV store
/// and the engine-wide write mutex, plus this connection's transaction state.
/// Not shared between connections.
pub struct SqlSession {
    pub(crate) kv: Arc<dyn Kv>,
    pub(crate) write_lock: Arc<Mutex<()>>,
    state: TxnState,
}

impl SqlSession {
    pub fn new(kv: Arc<dyn Kv>, write_lock: Arc<Mutex<()>>) -> Self {
        Self {
            kv,
            write_lock,
            state: TxnState::Idle,
        }
    }

    /// Read the global commit timestamp (0 if unset).
    pub(crate) fn read_commit_ts(&self) -> Result<u64, ExecError> {
        match self.kv.get(&kv::key::commit_ts_key())? {
            Some(b) => {
                let arr: [u8; 8] = b
                    .as_slice()
                    .try_into()
                    .map_err(|_| kv::KvError::CorruptRow("commit_ts is not u64".into()))?;
                Ok(u64::from_be_bytes(arr))
            }
            None => Ok(0),
        }
    }

    fn run_one(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        if matches!(self.state, TxnState::Failed)
            && !matches!(stmt, Statement::Commit | Statement::Rollback)
        {
            return Err(ExecError::InFailedTransaction);
        }
        match stmt {
            Statement::Begin { isolation } => self.begin(*isolation),
            Statement::Commit => self.commit_cmd(),
            Statement::Rollback => self.rollback_cmd(),
            Statement::CreateTable { .. } | Statement::DropTable { .. } => {
                crate::exec::execute_ddl(self, stmt)
            }
            _ => self.run_dml(stmt),
        }
    }

    fn begin(&mut self, isolation: Option<IsolationLevel>) -> Result<QueryResult, ExecError> {
        if matches!(self.state, TxnState::InTransaction(_)) {
            // BEGIN inside a block is a no-op (PostgreSQL warns and keeps going).
            return Ok(QueryResult::Command {
                tag: "BEGIN".into(),
            });
        }
        let rr = matches!(isolation, Some(IsolationLevel::RepeatableRead));
        let mut ctx = TxnCtx {
            repeatable_read: rr,
            ..Default::default()
        };
        if rr {
            // REPEATABLE READ fixes its snapshot at BEGIN.
            ctx.snapshot = self.read_commit_ts()?;
        }
        self.state = TxnState::InTransaction(ctx);
        Ok(QueryResult::Command {
            tag: "BEGIN".into(),
        })
    }

    fn commit_cmd(&mut self) -> Result<QueryResult, ExecError> {
        match std::mem::replace(&mut self.state, TxnState::Idle) {
            TxnState::InTransaction(ctx) => {
                // flush requires the write_lock to already be held, so take it
                // here to make the commit_ts read-modify-write atomic.
                let _guard = self.write_lock.lock().expect("write lock");
                self.flush(ctx)?;
                Ok(QueryResult::Command {
                    tag: "COMMIT".into(),
                })
            }
            // COMMIT of a failed transaction behaves as a ROLLBACK.
            TxnState::Failed => Ok(QueryResult::Command {
                tag: "ROLLBACK".into(),
            }),
            TxnState::Idle => Ok(QueryResult::Command {
                tag: "COMMIT".into(),
            }),
        }
    }

    fn rollback_cmd(&mut self) -> Result<QueryResult, ExecError> {
        // Discard the write-set and any failed state.
        self.state = TxnState::Idle;
        Ok(QueryResult::Command {
            tag: "ROLLBACK".into(),
        })
    }

    fn run_dml(&mut self, stmt: &Statement) -> Result<QueryResult, ExecError> {
        match &self.state {
            TxnState::InTransaction(_) => {
                // RC re-snapshots per statement; RR keeps its BEGIN snapshot.
                let refresh =
                    matches!(&self.state, TxnState::InTransaction(c) if !c.repeatable_read);
                if refresh {
                    let ts = self.read_commit_ts()?;
                    if let TxnState::InTransaction(c) = &mut self.state {
                        c.snapshot = ts;
                    }
                }
                // Break the aliasing borrow of self.kv vs &mut self.state.
                let kv = Arc::clone(&self.kv);
                let result = {
                    let ctx = match &mut self.state {
                        TxnState::InTransaction(c) => c,
                        _ => unreachable!(),
                    };
                    crate::exec::execute_dml(&*kv, ctx, stmt)
                };
                if result.is_err() {
                    self.state = TxnState::Failed;
                }
                result
            }
            TxnState::Idle => {
                // Implicit one-statement transaction (autocommit): hold the
                // write_lock across read_seq → execute_dml → flush so the whole
                // read-modify-write is atomic vs concurrent writers. flush does
                // NOT re-lock (it requires the lock already held).
                let kv = Arc::clone(&self.kv);
                let _guard = self.write_lock.lock().expect("write lock");
                let mut ctx = TxnCtx {
                    snapshot: self.read_commit_ts()?,
                    ..Default::default()
                };
                let result = crate::exec::execute_dml(&*kv, &mut ctx, stmt)?;
                self.flush(ctx)?;
                Ok(result)
            }
            TxnState::Failed => unreachable!("guarded in run_one"),
        }
    }

    /// Write a committed write-set to the store. CALLER MUST HOLD `write_lock`.
    /// A read-only transaction (empty write-set and seq) is a no-op and does not
    /// bump the global commit_ts.
    fn flush(&self, ctx: TxnCtx) -> Result<(), ExecError> {
        if ctx.writes.is_empty() && ctx.seq.is_empty() {
            return Ok(());
        }
        let new_ts = self.read_commit_ts()? + 1;
        let mut ops: Vec<kv::WriteOp> = Vec::new();
        for ((table, rowid), pending) in &ctx.writes {
            let value = match pending {
                Pending::Row(v) => v.clone(),
                Pending::Tombstone => mvcc::encode_version(true, &[]),
            };
            ops.push(kv::WriteOp::Put {
                key: mvcc::version_key(*table, *rowid, new_ts),
                value,
            });
        }
        for (table, next) in &ctx.seq {
            ops.push(kv::WriteOp::Put {
                key: kv::key::seq_key(*table),
                value: next.to_be_bytes().to_vec(),
            });
        }
        ops.push(kv::WriteOp::Put {
            key: kv::key::commit_ts_key(),
            value: new_ts.to_be_bytes().to_vec(),
        });
        self.kv.write_batch(&ops)?;
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
            results.push(self.run_one(&stmt).map_err(ExecError::into_pg)?);
        }
        Ok(results)
    }

    async fn describe(&mut self, sql: &str) -> Result<Vec<FieldDescription>, PgError> {
        crate::exec::describe(self, sql).map_err(ExecError::into_pg)
    }

    fn tx_status(&self) -> TxStatus {
        match self.state {
            TxnState::Idle => TxStatus::Idle,
            TxnState::InTransaction(_) => TxStatus::InTransaction,
            TxnState::Failed => TxStatus::Failed,
        }
    }
}
