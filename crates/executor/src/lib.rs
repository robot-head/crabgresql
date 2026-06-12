//! executor: turns parsed SQL into catalog/KV operations and implements the
//! pgwire `Engine` trait. SP5 swaps SP4's commit_ts MVCC for PostgreSQL's
//! xid/clog/snapshot model with uncommitted versions on disk; writers stay
//! serialized behind a transaction-scoped async writer lock.

mod error;
mod eval;
mod exec;
mod procarray;
mod session;

use std::path::Path;
use std::sync::Arc;

use kv::{FjallKv, Kv, MemKv};
use pgwire::engine::Engine;
use tokio::sync::Mutex;

pub use error::ExecError;
pub use session::SqlSession;

use crate::procarray::ProcArray;

/// The SQL engine over a durable (or in-memory) KV store. Catalog, sequences,
/// the xid counter, and the clog live in the KV store. The async writer mutex
/// serializes writing transactions engine-wide (SP5 keeps writers serialized;
/// SP6 replaces this lock with row-level concurrency). The `ProcArray` is shared
/// so every connection's snapshots see the same running-transaction set.
pub struct SqlEngine {
    pub(crate) kv: Arc<dyn Kv>,
    pub(crate) writer_lock: Arc<Mutex<()>>,
    pub(crate) procarray: Arc<ProcArray>,
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
        let procarray = Arc::new(ProcArray::open(&*kv)?);
        Ok(Self {
            kv,
            writer_lock: Arc::new(Mutex::new(())),
            procarray,
        })
    }
}

impl Engine for SqlEngine {
    type Session = SqlSession;

    fn connect(&self) -> SqlSession {
        SqlSession::new(
            Arc::clone(&self.kv),
            Arc::clone(&self.writer_lock),
            Arc::clone(&self.procarray),
        )
    }
}
