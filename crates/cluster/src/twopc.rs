//! Networked cross-range 2PC: a pooled node-port client (coordinator side). The
//! participant-side held-session registry (`TxnService`) and `NetCoordinator` land
//! in later SP17 tasks. Mirrors `forward::ForwardPool`'s leader-resolution +
//! bounded retry, but speaks the structured node protocol instead of pgwire.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::range::RangeId;
use crate::transport::frame::{read_msg, write_msg};
use crate::transport::partition::PartitionState;
use crate::transport::protocol::{NodeRequest, NodeResponse, TxnResp, TxnRpc};
use crate::types::{NodeId, TypeConfig};

const TXN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct PooledConn {
    addr: String,
    stream: Arc<Mutex<TcpStream>>,
}

/// Sends `TxnRpc`s to the current leader of a target range; pools one node-port
/// connection per target node.
pub struct TwoPcClient {
    rafts: HashMap<RangeId, openraft::Raft<TypeConfig>>,
    partition: PartitionState,
    conns: Mutex<HashMap<NodeId, PooledConn>>,
}

impl TwoPcClient {
    pub fn new(
        rafts: HashMap<RangeId, openraft::Raft<TypeConfig>>,
        partition: PartitionState,
    ) -> Arc<Self> {
        Arc::new(Self {
            rafts,
            partition,
            conns: Mutex::new(HashMap::new()),
        })
    }

    fn resolve_leader(&self, range: RangeId) -> Option<(NodeId, String)> {
        let raft = self.rafts.get(&range)?;
        let metrics = raft.metrics();
        let (leader, addr) = {
            let m = metrics.borrow();
            let leader = m.current_leader;
            let addr = leader.and_then(|l| {
                m.membership_config
                    .membership()
                    .get_node(&l)
                    .map(|n| crate::addr::node_dial_addr(&n.addr).to_string())
            });
            (leader, addr)
        };
        let leader = leader?;
        if self.partition.blocked(leader) {
            return None;
        }
        Some((leader, addr?))
    }

    /// Event-driven (no sleep) wait for a resolvable leader — see forward::ForwardPool::await_leader for the metrics-lag rationale.
    async fn await_leader(&self, range: RangeId) -> Option<(NodeId, String)> {
        let raft = self.rafts.get(&range)?;
        let deadline = tokio::time::Instant::now() + TXN_TIMEOUT;
        loop {
            if let Some(found) = self.resolve_leader(range) {
                return Some(found);
            }
            let mut rx = raft.metrics();
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            if tokio::time::timeout(remaining, rx.changed()).await.is_err() {
                return None;
            }
        }
    }

    /// Send one `TxnRpc` to `target_range`'s leader, bounded re-resolve+retry once
    /// on `NotLeader`/wire failure.
    // Err(()) = transport/leader-resolution failure; a participant-level retryable is carried as Ok(TxnResp::Retryable). T4 maps these to ExecError.
    // used in SP17 T4
    #[allow(dead_code)]
    pub async fn call(&self, target_range: RangeId, rpc: TxnRpc) -> Result<TxnResp, ()> {
        for attempt in 0..2 {
            let (leader, addr) = self.await_leader(target_range).await.ok_or(())?;
            let env = NodeRequest::Txn {
                range: target_range,
                rpc: rpc.clone(),
            };
            match self.exchange(leader, &addr, &env).await {
                Ok(TxnResp::NotLeader) if attempt == 0 => continue,
                Ok(resp) => return Ok(resp),
                Err(()) if attempt == 0 => continue,
                Err(()) => return Err(()),
            }
        }
        Err(())
    }

    async fn exchange(&self, leader: NodeId, addr: &str, env: &NodeRequest) -> Result<TxnResp, ()> {
        if self.partition.blocked(leader) {
            return Err(());
        }
        // Map lock held ONLY to get-or-dial + clone the per-conn handle out.
        let conn = {
            let mut conns = self.conns.lock().await;
            let needs_dial = conns.get(&leader).is_none_or(|c| c.addr != addr);
            if needs_dial {
                let stream = tokio::time::timeout(TXN_TIMEOUT, TcpStream::connect(addr))
                    .await
                    .map_err(|_| ())?
                    .map_err(|_| ())?;
                conns.insert(
                    leader,
                    PooledConn {
                        addr: addr.to_string(),
                        stream: Arc::new(Mutex::new(stream)),
                    },
                );
            }
            conns.get(&leader).expect("pooled conn present").clone()
        }; // map guard dropped here, before any network I/O

        // Per-connection lock: serializes only THIS leader's in-flight request.
        let mut stream = conn.stream.lock().await;
        let exchange = async {
            write_msg(&mut *stream, env).await?;
            read_msg::<_, NodeResponse>(&mut *stream).await
        };
        match tokio::time::timeout(TXN_TIMEOUT, exchange).await {
            Ok(Ok(NodeResponse::Txn(resp))) => Ok(resp),
            _ => {
                drop(stream);
                // Drop the poisoned conn so the next attempt redials — but only if
                // it is still the same handle (don't clobber a concurrent redial).
                let mut conns = self.conns.lock().await;
                if conns
                    .get(&leader)
                    .is_some_and(|c| Arc::ptr_eq(&c.stream, &conn.stream))
                {
                    conns.remove(&leader);
                }
                Err(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::transport::protocol::{NodeRequest, NodeResponse, TxnResp, TxnRpc};

    #[test]
    fn txn_rpc_round_trips_through_json() {
        let reqs = vec![
            NodeRequest::Txn {
                range: 0,
                rpc: TxnRpc::BeginGlobal,
            },
            NodeRequest::Txn {
                range: 2,
                rpc: TxnRpc::Stage {
                    g: 1 << 63,
                    range: 2,
                    sql: "UPDATE b SET id=21".into(),
                },
            },
            NodeRequest::Txn {
                range: 0,
                rpc: TxnRpc::CommitGlobal {
                    g: 1 << 63,
                    commit: true,
                },
            },
            NodeRequest::Txn {
                range: 2,
                rpc: TxnRpc::Release {
                    g: 1 << 63,
                    range: 2,
                    commit: false,
                },
            },
            NodeRequest::Txn {
                range: 0,
                rpc: TxnRpc::GlobalBarrier,
            },
        ];
        for r in reqs {
            let bytes = serde_json::to_vec(&r).expect("encode");
            let back: NodeRequest = serde_json::from_slice(&bytes).expect("decode");
            assert_eq!(format!("{r:?}"), format!("{back:?}"));
        }
        for resp in [
            TxnResp::Began { g: 1 << 63 },
            TxnResp::Staged,
            TxnResp::Committed,
            TxnResp::Released,
            TxnResp::Barrier { applied_index: 7 },
            TxnResp::NotLeader,
            TxnResp::Retryable,
            TxnResp::Err("boom".into()),
        ] {
            let env = NodeResponse::Txn(resp);
            let bytes = serde_json::to_vec(&env).expect("encode");
            let back: NodeResponse = serde_json::from_slice(&bytes).expect("decode");
            assert_eq!(format!("{env:?}"), format!("{back:?}"));
        }
    }
}
