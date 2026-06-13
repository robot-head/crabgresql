//! Single-range Raft replication for crabgresql (SP7 / distribution slice D1).
//! Wraps the SP1-SP6 engine in one in-process openraft group. In-memory and
//! ephemeral: no sockets, no on-disk Raft state, no restart recovery (all D2).

pub mod addr;
mod cluster;
mod committer;
mod durable;
mod linearizer;
mod network;
mod node;
pub mod route;
pub mod server_node;
mod store;
pub mod transport;
mod types;

pub use cluster::Cluster;
pub use committer::RaftCommitter;
pub use linearizer::RaftLinearizer;
pub use network::Switchboard;
pub use node::Node;
pub use types::{TypeConfig, WriteBatch};
