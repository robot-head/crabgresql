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
mod read_gate;
mod seq;
mod session;

use std::path::Path;
use std::sync::Arc;

use kv::{FjallKv, Kv, MemKv};
use pgwire::engine::Engine;

pub use commit::{Committer, LocalCommitter};
pub use error::ExecError;
pub use read_gate::{Linearizer, LocalLinearizer};
pub use session::SqlSession;

use crate::lockmgr::RowLockManager;
use crate::procarray::ProcArray;
use crate::seq::SequenceManager;

/// Whether the counter managers (`ProcArray`, `SequenceManager`) persist their
/// counters themselves (`Durable` — the local/single-node path) or fold the
/// counter advance into the commit batch for the replicated state machine to
/// max-merge (`Replicated` — the Raft path, reseeded on leadership change).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum PersistMode {
    Durable,
    Replicated,
}

/// The SQL engine over a durable (or in-memory) KV store. Catalog, sequences,
/// the xid counter, and the clog live in the KV store. Writers run concurrently
/// (SP6): row-level conflicts serialize through the `RowLockManager`, rowid
/// allocation goes through the `SequenceManager`, and DDL serializes among DDLs
/// behind `catalog_lock`. The `ProcArray` is shared so every connection's
/// snapshots see the same running-transaction set.
pub struct SqlEngine {
    pub(crate) kv: Arc<dyn Kv>,
    /// The store catalog lookups (table name→id→schema) resolve through. For the
    /// single-range engine this is the same store as `kv`; under multi-range
    /// sharding the catalog lives only on range 0, so a data range's engine
    /// points this at range 0's store while `kv` holds its own rows.
    pub(crate) catalog_kv: Arc<dyn Kv>,
    pub(crate) procarray: Arc<ProcArray>,
    pub(crate) seq: Arc<SequenceManager>,
    pub(crate) lockmgr: Arc<RowLockManager>,
    pub(crate) catalog_lock: Arc<tokio::sync::Mutex<()>>,
    pub(crate) committer: Arc<dyn crate::commit::Committer>,
    pub(crate) linearizer: Arc<dyn crate::read_gate::Linearizer>,
    pub(crate) persist_mode: PersistMode,
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
        let procarray = Arc::new(ProcArray::open(Arc::clone(&kv), PersistMode::Durable)?);
        let committer: Arc<dyn crate::commit::Committer> =
            Arc::new(crate::commit::LocalCommitter {
                kv: Arc::clone(&kv),
            });
        Ok(Self {
            catalog_kv: Arc::clone(&kv),
            kv,
            procarray,
            seq: Arc::new(SequenceManager::new(PersistMode::Durable)),
            lockmgr: Arc::new(RowLockManager::new()),
            catalog_lock: Arc::new(tokio::sync::Mutex::new(())),
            committer,
            linearizer: Arc::new(crate::read_gate::LocalLinearizer),
            persist_mode: PersistMode::Durable,
        })
    }

    /// Build an engine whose reads come from `sm_kv` (the applied state machine)
    /// and whose writes are proposed through `committer` (a RaftCommitter). Uses
    /// the Replicated persist mode so counters fold into the proposed batch.
    ///
    /// `catalog_kv` is the store catalog (schema) lookups resolve through. For a
    /// single-range node it is the same `Arc` as `sm_kv`; a multi-range data
    /// node passes range 0's applied store here while `sm_kv` holds its own rows.
    pub fn replicated(
        catalog_kv: Arc<dyn Kv>,
        sm_kv: Arc<dyn Kv>,
        committer: Arc<dyn crate::commit::Committer>,
        linearizer: Arc<dyn crate::read_gate::Linearizer>,
    ) -> Result<Self, ExecError> {
        let procarray = Arc::new(ProcArray::open(
            Arc::clone(&sm_kv),
            PersistMode::Replicated,
        )?);
        Ok(Self {
            catalog_kv,
            kv: sm_kv,
            procarray,
            seq: Arc::new(SequenceManager::new(PersistMode::Replicated)),
            lockmgr: Arc::new(RowLockManager::new()),
            catalog_lock: Arc::new(tokio::sync::Mutex::new(())),
            committer,
            linearizer,
            persist_mode: PersistMode::Replicated,
        })
    }

    /// Reseed counters from the applied store (call when this node becomes leader).
    pub fn reseed_counters(&self) -> Result<(), ExecError> {
        self.procarray.reseed_from_applied()?;
        self.seq.reseed_from_applied();
        Ok(())
    }
}

impl Engine for SqlEngine {
    type Session = SqlSession;

    fn connect(&self) -> SqlSession {
        SqlSession::new(
            Arc::clone(&self.kv),
            Arc::clone(&self.catalog_kv),
            Arc::clone(&self.procarray),
            Arc::clone(&self.seq),
            Arc::clone(&self.lockmgr),
            Arc::clone(&self.catalog_lock),
            Arc::clone(&self.committer),
            Arc::clone(&self.linearizer),
            self.persist_mode,
        )
    }
}
