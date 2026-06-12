//! executor: turns parsed SQL into catalog/KV operations and implements the
//! pgwire `Engine` trait. SP5 swaps SP4's commit_ts MVCC for PostgreSQL's
//! xid/clog/snapshot model with uncommitted versions on disk. SP6 removes the
//! global writer lock: writers run concurrently, serialized only at the row
//! level via the `RowLockManager`, with rowid allocation via the
//! `SequenceManager` and DDL serialized behind a small catalog lock.

mod commit;
mod error;
mod eval;
mod exec;
mod lockmgr;
mod procarray;
mod seq;
mod session;

use std::path::Path;
use std::sync::Arc;

use kv::{FjallKv, Kv, MemKv};
use pgwire::engine::Engine;

pub use commit::{Committer, LocalCommitter};
pub use error::ExecError;
pub use session::SqlSession;

use crate::lockmgr::RowLockManager;
use crate::procarray::ProcArray;
use crate::seq::SequenceManager;

/// The SQL engine over a durable (or in-memory) KV store. Catalog, sequences,
/// the xid counter, and the clog live in the KV store. Writers run concurrently
/// (SP6): row-level conflicts serialize through the `RowLockManager`, rowid
/// allocation goes through the `SequenceManager`, and DDL serializes among DDLs
/// behind `catalog_lock`. The `ProcArray` is shared so every connection's
/// snapshots see the same running-transaction set.
pub struct SqlEngine {
    pub(crate) kv: Arc<dyn Kv>,
    pub(crate) procarray: Arc<ProcArray>,
    pub(crate) seq: Arc<SequenceManager>,
    pub(crate) lockmgr: Arc<RowLockManager>,
    pub(crate) catalog_lock: Arc<tokio::sync::Mutex<()>>,
    pub(crate) committer: Arc<dyn crate::commit::Committer>,
}

impl Default for SqlEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlEngine {
    /// Ephemeral in-memory engine (tests, default when no --data-dir).
    pub fn new() -> Self {
        Self::with_kv(Arc::new(MemKv::new())).expect("in-memory engine never fails to open")
    }

    /// Durable engine backed by a fjall store at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ExecError> {
        Self::with_kv(Arc::new(FjallKv::open(path)?))
    }

    pub fn with_kv(kv: Arc<dyn Kv>) -> Result<Self, ExecError> {
        let procarray = Arc::new(ProcArray::open(Arc::clone(&kv))?);
        let committer: Arc<dyn crate::commit::Committer> =
            Arc::new(crate::commit::LocalCommitter {
                kv: Arc::clone(&kv),
            });
        Ok(Self {
            kv,
            procarray,
            seq: Arc::new(SequenceManager::new()),
            lockmgr: Arc::new(RowLockManager::new()),
            catalog_lock: Arc::new(tokio::sync::Mutex::new(())),
            committer,
        })
    }
}

impl Engine for SqlEngine {
    type Session = SqlSession;

    fn connect(&self) -> SqlSession {
        SqlSession::new(
            Arc::clone(&self.kv),
            Arc::clone(&self.procarray),
            Arc::clone(&self.seq),
            Arc::clone(&self.lockmgr),
            Arc::clone(&self.catalog_lock),
            Arc::clone(&self.committer),
        )
    }
}
