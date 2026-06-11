//! executor: turns parsed SQL into catalog/KV operations and implements the
//! pgwire `Engine` trait. The real engine behind the wire protocol for SP2.

mod error;
mod eval;
mod exec;

use std::path::Path;
use std::sync::{Arc, Mutex};

use catalog::TableId;
use kv::{FjallKv, Kv, MemKv};
use pgwire::engine::{Engine, FieldDescription, QueryResult};
use pgwire::error::PgError;

pub use error::ExecError;

/// The SQL engine over a durable (or in-memory) KV store. Catalog and sequences
/// live in the KV store; the DDL mutex serializes catalog mutations.
pub struct SqlEngine {
    pub(crate) kv: Arc<dyn Kv>,
    pub(crate) ddl_lock: Mutex<()>,
}

impl Default for SqlEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlEngine {
    /// Ephemeral in-memory engine (tests, default when no --data-dir).
    pub fn new() -> Self {
        Self::with_kv(Arc::new(MemKv::new()))
    }

    /// Durable engine backed by a fjall store at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ExecError> {
        Ok(Self::with_kv(Arc::new(FjallKv::open(path)?)))
    }

    pub fn with_kv(kv: Arc<dyn Kv>) -> Self {
        Self {
            kv,
            ddl_lock: Mutex::new(()),
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

impl Engine for SqlEngine {
    async fn simple_query(&self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        if sql.trim().is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        let statements = pgparser::parse(sql).map_err(|e| ExecError::from(e).into_pg())?;
        if statements.is_empty() {
            return Ok(vec![QueryResult::Empty]);
        }
        let mut results = Vec::with_capacity(statements.len());
        for stmt in statements {
            results.push(exec::execute(self, &stmt).map_err(ExecError::into_pg)?);
        }
        Ok(results)
    }

    async fn describe(&self, sql: &str) -> Result<Vec<FieldDescription>, PgError> {
        exec::describe(self, sql).map_err(ExecError::into_pg)
    }
}
