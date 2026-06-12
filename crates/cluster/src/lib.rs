//! Single-range Raft replication for crabgresql (SP7 / distribution slice D1).
//! Wraps the SP1-SP6 engine in one in-process openraft group. In-memory and
//! ephemeral: no sockets, no on-disk Raft state, no restart recovery (all D2).

mod types;
mod store;
mod network;
mod node;
mod committer;
mod cluster;

pub use cluster::Cluster;
pub use network::Switchboard;
pub use node::Node;
pub use types::{TypeConfig, WriteBatch};
