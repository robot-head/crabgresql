//! One replica: a Raft instance plus its applied state-machine store.
//!
//! A `Node` is built but not yet a cluster member — [`Cluster`] initializes the
//! voting group separately. The `sm_kv` handle is the same `Arc<MemKv>` the
//! state machine applies committed writes into, so tests (and later the SQL
//! engine) can read replicated state directly.
//!
//! [`Cluster`]: crate::cluster::Cluster

use std::sync::Arc;

use kv::MemKv;

use crate::network::Switchboard;
use crate::store::{LogStore, StateMachineStore};
use crate::types::{NodeId, TypeConfig};

/// A single Raft replica: its `Raft` handle and a handle to the applied state.
pub struct Node {
    /// This node's id within the single range.
    pub id: NodeId,
    /// The openraft handle used to propose writes and inspect metrics.
    pub raft: openraft::Raft<TypeConfig>,
    /// Shared, committed application state (the same `Arc` the state machine
    /// applies into). Cloning is cheap and reflects applied writes live.
    pub sm_kv: Arc<MemKv>,
}

impl Node {
    /// The default Raft config: short timers so in-process elections are fast and
    /// deterministic under the multi-thread test runtime.
    pub fn default_config() -> openraft::Config {
        openraft::Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        }
    }

    /// Build a node (not yet a cluster member) with the default config. `sb` is
    /// the shared transport; the node registers its Raft handle so peers can
    /// reach it.
    pub async fn start(id: NodeId, sb: Switchboard) -> Self {
        Self::start_with_config(id, sb, Self::default_config()).await
    }

    /// Build a node with an explicit Raft `config`. Lets tests pass an aggressive
    /// snapshot policy (see [`Cluster::new_with_snapshotting`]) so a far-behind
    /// follower must be repaired by an installed snapshot rather than log replay.
    ///
    /// [`Cluster::new_with_snapshotting`]: crate::cluster::Cluster::new_with_snapshotting
    pub async fn start_with_config(id: NodeId, sb: Switchboard, config: openraft::Config) -> Self {
        let config = Arc::new(config.validate().expect("valid raft config"));

        let log = Arc::new(LogStore::default());
        let sm = Arc::new(StateMachineStore::default());
        let sm_kv = sm.sm_kv();

        // The split-storage traits are implemented for `Arc<LogStore>` and
        // `Arc<StateMachineStore>`, which is exactly what `Raft::new` wants.
        let raft = openraft::Raft::new(id, config, sb.for_node(id), log, sm)
            .await
            .expect("raft::new");

        sb.register(id, raft.clone());
        Node { id, raft, sm_kv }
    }

    /// Build a replicated `SqlEngine` over this node's applied state machine plus
    /// a Raft committer. Reads hit `sm_kv` (the applied store); writes propose
    /// through `raft` (committed == applied to a majority).
    ///
    /// The caller MUST call [`executor::SqlEngine::reseed_counters`] on the
    /// returned engine after this node (re-)acquires leadership and before
    /// issuing SQL on it, so the xid/sequence counters never regress below what
    /// a previous leader already handed out. (An automatic reseed via a
    /// metrics-subscription on leadership change is deferred to D2.)
    ///
    /// Each call builds a fresh engine (its own `ProcArray`/`RowLockManager`).
    /// To share row locks and the running-transaction set across sessions, call
    /// this once, wrap the engine in an `Arc`, and `connect()` it repeatedly.
    pub fn engine(&self) -> executor::SqlEngine {
        executor::SqlEngine::replicated(
            self.sm_kv.clone() as Arc<dyn kv::Kv>,
            Arc::new(crate::committer::RaftCommitter {
                raft: self.raft.clone(),
            }),
        )
        .expect("replicated engine")
    }
}
