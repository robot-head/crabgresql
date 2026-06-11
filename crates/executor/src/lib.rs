//! executor: turns parsed SQL into catalog/KV operations and implements the
//! pgwire `Engine` trait. The real engine behind the wire protocol for SP2.

mod error;
mod eval;
mod exec;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use catalog::{Catalog, TableId};
use kv::{Kv, MemKv};
use pgwire::engine::{Engine, FieldDescription, QueryResult};
use pgwire::error::PgError;

pub use error::ExecError;

/// The SQL engine: a catalog, a KV store, and per-table rowid counters.
pub struct SqlEngine {
    pub(crate) catalog: Arc<Catalog>,
    // Used from Task 16 onward; scaffolded here so exec.rs reaches it.
    #[allow(dead_code)]
    pub(crate) kv: Arc<dyn Kv>,
    // Used from Task 16 onward; scaffolded here so exec.rs reaches it.
    #[allow(dead_code)]
    pub(crate) rowids: Mutex<HashMap<TableId, u64>>,
}

impl Default for SqlEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlEngine {
    pub fn new() -> Self {
        Self::with_kv(Arc::new(MemKv::new()))
    }

    pub fn with_kv(kv: Arc<dyn Kv>) -> Self {
        Self {
            catalog: Arc::new(Catalog::new()),
            kv,
            rowids: Mutex::new(HashMap::new()),
        }
    }

    /// Allocate the next rowid for a table (monotonic per table).
    /// Used from Task 16 onward.
    #[allow(dead_code)]
    pub(crate) fn next_rowid(&self, table: TableId) -> u64 {
        let mut ids = self.rowids.lock().expect("rowid lock");
        let n = ids.entry(table).or_insert(1);
        let id = *n;
        *n += 1;
        id
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
