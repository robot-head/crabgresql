//! Leader routing: the public SQL listener serves the connection locally when
//! this node is the leader, else byte-proxies it to the current leader's pgwire
//! port. The leader's SQL address is resolved from Raft membership (each peer's
//! `BasicNode.addr` is packed `"node|sql"`).
use std::sync::Arc;
use std::time::{Duration, Instant};

use executor::SqlEngine;
use pgwire::server::{CancelRegistry, serve_conn};
use pgwire::session::SessionConfig;
use tokio::net::{TcpListener, TcpStream};

use crate::addr::sql_addr_part;
use crate::types::TypeConfig;

/// How long a connection waits for a leader to exist before being closed.
const NO_LEADER_WAIT: Duration = Duration::from_secs(5);
/// Timeout for dialing the leader's pgwire port.
const PROXY_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Serve the public SQL port with leader routing.
pub async fn serve_routed(
    listener: TcpListener,
    raft: openraft::Raft<TypeConfig>,
    engine: Arc<SqlEngine>,
    config: Arc<SessionConfig>,
) -> std::io::Result<()> {
    // One registry shared across this node's leader-local connections so a
    // Postgres CancelRequest (a separate connection) can find its target.
    let registry = Arc::new(CancelRegistry::default());
    loop {
        let (stream, _peer) = listener.accept().await?;
        let raft = raft.clone();
        let engine = engine.clone();
        let config = config.clone();
        let registry = registry.clone();
        tokio::spawn(async move {
            route_one(stream, raft, engine, config, registry).await;
        });
    }
}

async fn route_one(
    stream: TcpStream,
    raft: openraft::Raft<TypeConfig>,
    engine: Arc<SqlEngine>,
    config: Arc<SessionConfig>,
    registry: Arc<CancelRegistry>,
) {
    let deadline = Instant::now() + NO_LEADER_WAIT;
    loop {
        // Resolve target from metrics WITHOUT holding the watch Ref across await.
        // Bind the `watch::Receiver` to a local so the `Ref` it yields is not a
        // borrow of a temporary; the `Ref` (and the receiver) drop at the end of
        // this block, before any `.await`.
        let metrics = raft.metrics();
        let (me, leader, leader_sql) = {
            let m = metrics.borrow();
            let leader = m.current_leader;
            let sql = leader.and_then(|l| {
                m.membership_config
                    .membership()
                    .get_node(&l)
                    .and_then(|n| sql_addr_part(&n.addr).map(str::to_string))
            });
            (m.id, leader, sql)
        };
        match leader {
            Some(l) if l == me => {
                let _ = serve_conn(stream, engine, config, registry, None).await;
                return;
            }
            Some(l) => {
                if let Some(addr) = leader_sql {
                    proxy(stream, &addr).await;
                } else {
                    // Leader known but its membership addr has no `|sql` half —
                    // unreachable in production (bootstrap and add_learner both
                    // pack `node|sql`), but drop with a diagnostic rather than
                    // silently if a caller ever registers an un-packed addr.
                    tracing::warn!(
                        leader = l,
                        "leader has no SQL address in membership; dropping client"
                    );
                }
                return;
            }
            None => {
                if Instant::now() >= deadline {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Byte-relay `client` to the leader's pgwire port until either side closes.
/// Drops cleanly (closes `client`) if the leader is unreachable or the dial
/// times out — the client just retries.
async fn proxy(mut client: TcpStream, leader_sql_addr: &str) {
    if let Ok(Ok(mut upstream)) =
        tokio::time::timeout(PROXY_CONNECT_TIMEOUT, TcpStream::connect(leader_sql_addr)).await
    {
        let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
    }
}

use std::collections::HashMap;

use crate::range::map::{RangeId, RangeMap};
use crate::range::router::{RangeRouter, RemoteForward};

/// A `pgwire` `Engine` whose every connection is a per-statement range gateway.
/// `connect()` builds a `RangeRouter` over this node's LOCAL-leader engines, the
/// range-0 catalog store, and the remote-forward seam — so each simple-query frame
/// runs locally for a range this node leads and forwards otherwise. The per-range
/// engines are shared (`Arc`) across all connections; each connection gets fresh
/// `SqlSession`s lazily (`SqlEngine::connect`), as the router already does.
pub struct RangeGatewayEngine {
    map: RangeMap,
    engines: HashMap<RangeId, Arc<SqlEngine>>,
    catalog_kv: Arc<dyn kv::Kv>,
    forward: Arc<dyn RemoteForward>,
}

impl RangeGatewayEngine {
    pub fn new(
        map: RangeMap,
        engines: HashMap<RangeId, Arc<SqlEngine>>,
        catalog_kv: Arc<dyn kv::Kv>,
        forward: Arc<dyn RemoteForward>,
    ) -> Self {
        Self {
            map,
            engines,
            catalog_kv,
            forward,
        }
    }
}

impl pgwire::engine::Engine for RangeGatewayEngine {
    type Session = RangeRouter;

    fn connect(&self) -> RangeRouter {
        // The router owns one engine per range by value; clone the shared engines
        // into per-connection routing handles. `SqlEngine` is a bundle of `Arc`s
        // (`lib.rs:49-63`), so this is a cheap pointer clone, not a deep copy.
        let engines: HashMap<RangeId, SqlEngine> = self
            .engines
            .iter()
            .map(|(&r, e)| (r, (**e).clone_handle()))
            .collect();
        RangeRouter::new(
            self.map.clone(),
            engines,
            Arc::clone(&self.catalog_kv),
            Arc::clone(&self.forward),
        )
    }
}

/// Serve the public SQL port as a per-statement range gateway: each simple-query
/// frame is demuxed to its range's local leader engine or forwarded to the remote
/// leader. The multi-range analog of `serve_routed`; T2 selects this when the
/// node hosts more than one range.
pub async fn serve_range_routed(
    listener: TcpListener,
    map: RangeMap,
    engines: HashMap<RangeId, Arc<SqlEngine>>,
    catalog_kv: Arc<dyn kv::Kv>,
    forward: Arc<dyn RemoteForward>,
    config: Arc<SessionConfig>,
) -> std::io::Result<()> {
    let engine = Arc::new(RangeGatewayEngine::new(map, engines, catalog_kv, forward));
    let registry = Arc::new(CancelRegistry::default());
    loop {
        let (stream, _peer) = listener.accept().await?;
        let engine = engine.clone();
        let config = config.clone();
        let registry = registry.clone();
        tokio::spawn(async move {
            let _ = serve_conn(stream, engine, config, registry, None).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// `proxy` faithfully relays bytes both directions to an upstream.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn proxy_relays_bytes_bidirectionally() {
        let upstream = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind upstream");
        let upstream_addr = upstream.local_addr().expect("addr").to_string();
        tokio::spawn(async move {
            let (mut s, _) = upstream.accept().await.expect("accept");
            let mut buf = [0u8; 5];
            s.read_exact(&mut buf).await.expect("read");
            s.write_all(&buf).await.expect("echo");
        });
        let front = TcpListener::bind("127.0.0.1:0").await.expect("bind front");
        let front_addr = front.local_addr().expect("addr").to_string();
        let up = upstream_addr.clone();
        tokio::spawn(async move {
            let (client, _) = front.accept().await.expect("accept");
            proxy(client, &up).await;
        });
        let mut c = TcpStream::connect(&front_addr)
            .await
            .expect("connect front");
        c.write_all(b"hello").await.expect("write");
        let mut got = [0u8; 5];
        c.read_exact(&mut got).await.expect("read echo");
        assert_eq!(&got, b"hello");
    }
}
