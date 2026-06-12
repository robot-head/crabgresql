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
    /// Build a node (not yet a cluster member). `sb` is the shared transport;
    /// the node registers its Raft handle with it so peers can reach it.
    pub async fn start(id: NodeId, sb: Switchboard) -> Self {
        let config = Arc::new(
            openraft::Config {
                // Short timers keep in-process elections fast and deterministic
                // under the multi-thread test runtime.
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                ..Default::default()
            }
            .validate()
            .expect("valid raft config"),
        );

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
}
