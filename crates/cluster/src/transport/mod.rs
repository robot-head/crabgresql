//! Real TCP transport for Raft RPCs + a control channel, implementing openraft's
//! `RaftNetwork`/`RaftNetworkFactory` (parallel to the in-process `Switchboard`).
pub mod client;
pub mod frame;
pub mod partition;
pub mod protocol;
pub mod server;

#[cfg(test)]
mod testcluster;

/// Hard cap on a single frame to avoid allocating on garbage/oversized input.
pub const MAX_FRAME: usize = 64 * 1024 * 1024;
