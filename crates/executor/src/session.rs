//! Per-connection session: runs SQL against the shared KV store. SP4 Task 3
//! ships autocommit only; the transaction state machine arrives in Task 5.

use std::sync::{Arc, Mutex};

use catalog::TableId;
use kv::Kv;
use pgwire::engine::{FieldDescription, QueryResult, Session, TxStatus};
use pgwire::error::PgError;

use crate::error::ExecError;

/// One connection's view of the engine. Holds shared handles to the KV store
/// and the engine-wide write mutex; transaction state will live here in later
/// SP4 tasks. Not shared between connections.
pub struct SqlSession {
    pub(crate) kv: Arc<dyn Kv>,
    pub(crate) write_lock: Arc<Mutex<()>>,
}

impl SqlSession {
    pub fn new(kv: Arc<dyn Kv>, write_lock: Arc<Mutex<()>>) -> Self {
        Self { kv, write_lock }
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

    /// Read a table's durable next-rowid (1 if unset).
    pub(crate) fn read_seq(&self, table: TableId) -> Result<u64, ExecError> {
        match self.kv.get(&kv::key::seq_key(table))? {
            Some(b) => {
                let arr: [u8; 8] = b
                    .as_slice()
                    .try_into()
                    .map_err(|_| kv::KvError::CorruptRow("sequence is not u64".into()))?;
                Ok(u64::from_be_bytes(arr))
            }
            None => Ok(1),
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
            results.push(crate::exec::execute(self, &stmt).map_err(ExecError::into_pg)?);
        }
        Ok(results)
    }

    async fn describe(&mut self, sql: &str) -> Result<Vec<FieldDescription>, PgError> {
        crate::exec::describe(self, sql).map_err(ExecError::into_pg)
    }

    fn tx_status(&self) -> TxStatus {
        TxStatus::Idle
    }
}
