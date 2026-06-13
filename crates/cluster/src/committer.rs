//! A Committer that proposes batches through Raft. Resolving == committed+applied.
//!
//! `client_write` returns only once the batch is committed to a majority AND
//! applied to this leader's state machine, which is exactly the durability the
//! `Committer` contract promises.

use executor::{Committer, ExecError};
use kv::WriteOp;
use openraft::error::{ClientWriteError, RaftError};

use crate::types::{NodeId, TypeConfig, WriteBatch};

/// Proposes write batches through Raft. Reads happen elsewhere (against the
/// applied `sm_kv`); this only handles the write/propose side.
pub struct RaftCommitter {
    pub(crate) raft: openraft::Raft<TypeConfig>,
}

#[async_trait::async_trait]
impl Committer for RaftCommitter {
    async fn commit(&self, ops: Vec<WriteOp>) -> Result<(), ExecError> {
        self.raft
            .client_write(WriteBatch(ops))
            .await
            .map_err(map_err)?;
        Ok(())
    }
}

/// Map openraft's `client_write` error onto an `ExecError`.
///
/// `client_write` returns `RaftError<NodeId, ClientWriteError<NodeId, Node>>`.
/// A `ForwardToLeader` (this node is not the leader) is a retryable client
/// redirect → `NotLeader`. Everything else — a `Fatal` (timeout / shutdown /
/// no quorum) or any other API error — means the batch did not commit and no
/// partial state was applied → `Unavailable` (also retryable).
fn map_err(e: RaftError<NodeId, ClientWriteError<NodeId, openraft::BasicNode>>) -> ExecError {
    match e {
        RaftError::APIError(ClientWriteError::ForwardToLeader(_)) => ExecError::NotLeader,
        _ => ExecError::Unavailable,
    }
}
