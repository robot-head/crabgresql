//! A Linearizer that performs an openraft ReadIndex check before a read. Mirrors
//! `RaftCommitter`: the committer linearizes writes, this linearizes reads.
//!
//! `ensure_linearizable` confirms leadership by heartbeating a quorum and blocks
//! until the local state machine has applied through the read log id. On a
//! deposed/partitioned leader the heartbeats fail and it returns an error
//! (bounded by `heartbeat_interval`), so the read is rejected rather than served
//! from stale local state.

use executor::{ExecError, Linearizer};
use openraft::BasicNode;
use openraft::error::{CheckIsLeaderError, RaftError};

use crate::types::{NodeId, TypeConfig};

/// Performs a ReadIndex check on the leader before a read. Reads still come from
/// the applied `sm_kv`; this only confirms it is safe to observe it now.
pub struct RaftLinearizer {
    pub(crate) raft: openraft::Raft<TypeConfig>,
}

#[async_trait::async_trait]
impl Linearizer for RaftLinearizer {
    async fn ensure_readable(&self) -> Result<(), ExecError> {
        self.raft
            .ensure_linearizable()
            .await
            .map(|_read_log_id| ())
            .map_err(map_err)
    }
}

/// Map openraft's `ensure_linearizable` error onto an `ExecError`. A
/// `ForwardToLeader` (this node saw a higher term / isn't the leader) is a
/// retryable redirect → `NotLeader` (SQLSTATE 40001); a `QuorumNotEnough`
/// (couldn't reach a quorum to confirm leadership) or any `Fatal` → `Unavailable`
/// (SQLSTATE 08006, also retryable). Note the *partitioned*-leader case (a leader
/// isolated from its followers — the D5 Scenario-B case) yields `QuorumNotEnough`,
/// so its read surfaces 08006, not 40001. Either way the read returns no stale rows.
fn map_err(e: RaftError<NodeId, CheckIsLeaderError<NodeId, BasicNode>>) -> ExecError {
    match e {
        RaftError::APIError(CheckIsLeaderError::ForwardToLeader(_)) => ExecError::NotLeader,
        _ => ExecError::Unavailable,
    }
}
