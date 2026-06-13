//! openraft type configuration for crabgresql's single range.

use std::io::Cursor;

/// The replicated application command: one atomic KV write batch.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WriteBatch(pub Vec<kv::WriteOp>);

/// Raft node identifier. A small integer per replica in the single range.
pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Single-range type config: `AppData` is a write batch, the response is unit.
    pub TypeConfig:
        D = WriteBatch,
        R = (),
        NodeId = NodeId,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);
