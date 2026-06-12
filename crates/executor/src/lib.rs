//! executor: turns parsed SQL into catalog/KV operations and implements the
//! pgwire `Engine` trait. The real engine behind the wire protocol for SP2.

mod error;
mod eval;
mod exec;
mod session;

use std::path::Path;
use std::sync::{Arc, Mutex};

use kv::{FjallKv, Kv, MemKv};
use pgwire::engine::Engine;

pub use error::ExecError;
pub use session::SqlSession;

/// The SQL engine over a durable (or in-memory) KV store. Catalog and sequences
/// live in the KV store; the write mutex serializes all writes (INSERT, CREATE
/// TABLE, DROP TABLE). SELECT is lock-free (reads are safe concurrently).
///
/// As of SP4 Task 3 the engine is a connection factory: it produces one
/// [`SqlSession`] per connection. The KV store and the write mutex are shared
/// (`Arc`) across all sessions so writes remain serialized engine-wide.
///
/// SP3 is single-node autocommit with no MVCC, so one global write mutex is
/// correct and simple — it is exactly the seam that SP4's transactions replace.
pub struct SqlEngine {
    pub(crate) kv: Arc<dyn Kv>,
    pub(crate) write_lock: Arc<Mutex<()>>,
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
            write_lock: Arc::new(Mutex::new(())),
        }
    }
}

impl Engine for SqlEngine {
    type Session = SqlSession;

    fn connect(&self) -> SqlSession {
        SqlSession::new(Arc::clone(&self.kv), Arc::clone(&self.write_lock))
    }
}
