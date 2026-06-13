# SP10 (D2c): SQL leader routing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the replicated cluster a transparently-usable Postgres endpoint — a SQL client connecting to any node is byte-proxied to the leader (which serves it on its single engine), so stock clients can point at any node.

**Architecture:** A non-leader node's public SQL listener byte-relays (`tokio::io::copy_bidirectional`) the connection to the current leader's pgwire port; the leader serves locally. The leader's SQL address rides in Raft membership by packing `BasicNode.addr = "<node_addr>|<sql_addr>"`. No engine, storage, or transport change — purely a frontend routing layer over D2b.

**Tech Stack:** Rust 2024, openraft 0.9, tokio (TCP + `copy_bidirectional`, already enabled), pgwire (from-scratch Postgres protocol), tokio-postgres (tests). Pure-Rust, `#![forbid(unsafe_code)]`. No new dependency.

**Spec:** `docs/superpowers/specs/2026-06-12-crabgresql-sp10-sql-leader-routing-design.md`

**Conventions for every task:** Windows dev box — run the multiprocess tests with `__COMPAT_LAYER=RunAsInvoker cargo test ...` (real child processes spawn fine under that shim). **IDE/rust-analyzer diagnostics are routinely STALE — trust only `cargo build`/`clippy`/`test`.** Repo lints deny `clippy::unwrap_used` even in tests (use `.expect("msg")`). End each commit message with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. Branch: `sp10-sql-leader-routing` (already checked out; do NOT switch). After each task: `cargo clippy -p <crate> --all-targets -- -D warnings` must be zero-warning.

---

## File Structure

| File | Responsibility |
|---|---|
| `crates/pgwire/src/server.rs` | Expose `pub async fn serve_conn` (extract `serve_tls`'s per-connection body) so the cluster route layer can serve a leader-local connection. |
| `crates/cluster/src/addr.rs` | **(new)** Pure helpers: `node_dial_addr(&str) -> &str` and `sql_addr_part(&str) -> Option<&str>` for the packed `"node|sql"` `BasicNode.addr`. |
| `crates/cluster/src/route.rs` | **(new)** `serve_routed` (public SQL accept loop: serve-local vs proxy vs no-leader bounded-wait) + `proxy` (byte-relay) + leader-sql-addr resolution. |
| `crates/cluster/src/transport/client.rs` | `new_client` dials the `node_addr` half of `BasicNode.addr`. |
| `crates/cluster/src/server_node.rs` | Swap the `serve_tls` spawn for `serve_routed`; pack `BasicNode.addr = "node|sql"` in `bootstrap`. |
| `crates/crabgresql/src/main.rs` | `--peer ID@node_addr|sql_addr` parsing (the part after `@` is the packed addr). |
| `crates/crabgresql/tests/harness/mod.rs` | `pg_round_robin`/random-node connect helper for the nemesis. |
| `crates/crabgresql/tests/multiprocess.rs` | Routing scenarios + random-node bank nemesis. |

The in-process `Switchboard`/`Node`/`Cluster`, the durable storage (`durable.rs`), and the SQL/MVCC engine are **unchanged**.

---

### Task 1: pgwire `serve_conn` extraction

**Files:**
- Modify: `crates/pgwire/src/server.rs`

Expose the per-connection serve body so the cluster route layer can serve a leader-local connection with a shared cancel registry. No behavior change to `serve`/`serve_tls`.

- [ ] **Step 1: Add `serve_conn`.** In `crates/pgwire/src/server.rs`, `handle_conn` is a private `async fn handle_conn<E: Engine>(stream: TcpStream, engine: Arc<E>, config: Arc<SessionConfig>, registry: Arc<CancelRegistry>, tls: Option<TlsAcceptor>) -> io::Result<()>`. `CancelRegistry` is already `pub` with `Default`. Add a thin public wrapper above `handle_conn`:
```rust
/// Serve a SINGLE already-accepted connection (the per-connection body of
/// [`serve_tls`]). Exposed so a front-end router (the cluster's leader-routing
/// layer) can serve a leader-local connection itself. `registry` is shared
/// across a server's connections so a Postgres CancelRequest on a separate
/// connection can find its target.
pub async fn serve_conn<E: Engine>(
    stream: TcpStream,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
    registry: Arc<CancelRegistry>,
    tls: Option<TlsAcceptor>,
) -> std::io::Result<()> {
    handle_conn(stream, engine, config, registry, tls).await
}
```

- [ ] **Step 2: Route `serve_tls` through it (no behavior change).** In `serve_tls`'s accept loop, replace the `handle_conn(stream, engine, config, registry, tls)` call inside the spawned task with `serve_conn(stream, engine, config, registry, tls)`. (Same arguments; this just proves `serve_conn` is the real path.)

- [ ] **Step 3: Verify no behavior change.**
```
cargo build -p pgwire
cargo clippy -p pgwire --all-targets -- -D warnings
__COMPAT_LAYER=RunAsInvoker cargo test -p pgwire
```
Expected: all existing pgwire tests pass (`simple_query`, `extended_query`, `cancel`, `scram_auth`, `tls`, etc.) — `serve_conn` is behavior-identical to the old inline `handle_conn` call. Report the `test result:` lines (especially `cancel` — it exercises the shared registry).

- [ ] **Step 4: Commit.**
```
cargo fmt -p pgwire
git add crates/pgwire/src/server.rs
git commit -m "feat(pgwire): expose serve_conn (per-connection serve) for front-end routing"
```

---

### Task 2: Address encoding (`"node|sql"`) + transport client + CLI

**Files:**
- Create: `crates/cluster/src/addr.rs`
- Modify: `crates/cluster/src/lib.rs`, `crates/cluster/src/transport/client.rs`, `crates/crabgresql/src/main.rs`

Pack both addresses into `BasicNode.addr`; the transport client dials the node half; the CLI parses `ID@node|sql`.

- [ ] **Step 1: Write `addr.rs` + its tests.**
```rust
//! Helpers for the packed node address `"<node_addr>|<sql_addr>"` carried in
//! `openraft::BasicNode.addr`. The node half is the Raft RPC / control listener;
//! the sql half is the pgwire listener (used by leader-routing to proxy SQL).

/// The Raft RPC / control address — the part before `|`. Returns the whole
/// string when there is no `|` (an un-packed addr, e.g. the in-process
/// `testcluster`), so existing callers are unaffected.
pub fn node_dial_addr(addr: &str) -> &str {
    addr.split('|').next().unwrap_or(addr)
}

/// The pgwire SQL address — the part after `|`, or `None` if the addr is not
/// packed with a sql half.
pub fn sql_addr_part(addr: &str) -> Option<&str> {
    addr.split('|').nth(1)
}

/// Pack a node + sql address into the `BasicNode.addr` form.
pub fn pack(node_addr: &str, sql_addr: &str) -> String {
    format!("{node_addr}|{sql_addr}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packs_and_splits() {
        let a = pack("127.0.0.1:5001", "127.0.0.1:6001");
        assert_eq!(a, "127.0.0.1:5001|127.0.0.1:6001");
        assert_eq!(node_dial_addr(&a), "127.0.0.1:5001");
        assert_eq!(sql_addr_part(&a), Some("127.0.0.1:6001"));
    }

    #[test]
    fn unpacked_addr_is_node_addr() {
        // The in-process testcluster sets a node-only addr (no `|`).
        assert_eq!(node_dial_addr("127.0.0.1:5001"), "127.0.0.1:5001");
        assert_eq!(sql_addr_part("127.0.0.1:5001"), None);
    }
}
```

- [ ] **Step 2: Declare the module.** In `crates/cluster/src/lib.rs` add `pub mod addr;` (next to `pub mod transport;`).

- [ ] **Step 3: Run the helper tests.** `cargo test -p cluster --lib addr` → both pass.

- [ ] **Step 4: Transport client dials the node half.** In `crates/cluster/src/transport/client.rs`, `new_client` currently does `addr: node.addr.clone()`. Change to dial only the node half:
```rust
async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> TcpConn {
    TcpConn {
        from: self.from,
        target,
        addr: crate::addr::node_dial_addr(&node.addr).to_string(),
        partition: self.partition.clone(),
        stream: None,
    }
}
```
(For the in-process `testcluster`, `node.addr` has no `|`, so `node_dial_addr` returns it unchanged — no behavior change there.)

- [ ] **Step 5: Verify transport unaffected.**
```
cargo build -p cluster
cargo clippy -p cluster --all-targets -- -D warnings
cargo test -p cluster --lib transport
```
Expected: the loopback `testcluster` tests (election/replication/partition over TCP) still pass — the dial address is unchanged for un-packed addrs.

- [ ] **Step 6: CLI `--peer ID@node|sql` parsing.** In `crates/crabgresql/src/main.rs`, the `--peer` parser splits `id@addr` into `(u64, String)` where `addr` becomes `BasicNode.addr`. The packed form `ID@node_addr|sql_addr` already works with the existing `split_once('@')` (the part after `@` is `"node|sql"`, stored verbatim as the peer's addr). **Confirm** the parser does `split_once('@')` (not `split('@')` or anything that would choke on the `|`), and that the parsed `(id, "node|sql")` is what's placed into the `NodeConfig.peers` vec and ultimately the bootstrap `BasicNode.addr`. Update the `--peer` help text to `ID@node_addr|sql_addr`. No logic change should be needed beyond the help text if the parser already splits only on the first `@`; verify by building.

- [ ] **Step 7: Build the binary.**
```
cargo build -p crabgresql
cargo clippy -p crabgresql --all-targets -- -D warnings
```
Expected: clean.

- [ ] **Step 8: Commit.**
```
cargo fmt -p cluster -p crabgresql
git add crates/cluster/src/addr.rs crates/cluster/src/lib.rs crates/cluster/src/transport/client.rs crates/crabgresql/src/main.rs
git commit -m "feat(cluster): packed node|sql BasicNode.addr; transport dials node half"
```

---

### Task 3: The route layer (`serve_routed` + proxy + leader resolution)

**Files:**
- Create: `crates/cluster/src/route.rs`
- Modify: `crates/cluster/src/lib.rs`
- Modify: `crates/cluster/Cargo.toml` (ensure `pgwire` is a dep — it is, from SP9)

- [ ] **Step 1: Write `route.rs`.**
```rust
//! Leader routing: the public SQL listener serves the connection locally when
//! this node is the leader, else byte-proxies it to the current leader's pgwire
//! port. The leader's SQL address is resolved from Raft membership (each peer's
//! `BasicNode.addr` is packed `"node|sql"`).
use std::sync::Arc;
use std::time::{Duration, Instant};

use executor::SqlEngine;
use pgwire::server::{serve_conn, CancelRegistry};
use pgwire::session::SessionConfig;
use tokio::net::{TcpListener, TcpStream};

use crate::addr::sql_addr_part;
use crate::types::{NodeId, TypeConfig};

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
        let (me, leader, leader_sql) = {
            let m = raft.metrics().borrow();
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
                // We are the leader: serve locally.
                let _ = serve_conn(stream, engine, config, registry, None).await;
                return;
            }
            Some(_) => {
                // A different leader: proxy to its SQL port (if resolvable).
                if let Some(addr) = leader_sql {
                    proxy(stream, &addr).await;
                }
                // else: leader known but unresolvable sql addr — drop (close).
                return;
            }
            None => {
                if Instant::now() >= deadline {
                    return; // no leader within the bound: close the connection
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                // re-check
            }
        }
    }
}

/// Byte-relay `client` to the leader's pgwire port until either side closes.
async fn proxy(mut client: TcpStream, leader_sql_addr: &str) {
    match tokio::time::timeout(PROXY_CONNECT_TIMEOUT, TcpStream::connect(leader_sql_addr)).await {
        Ok(Ok(mut upstream)) => {
            let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
        }
        _ => { /* leader unreachable: drop the client connection (it retries) */ }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// `proxy` faithfully relays bytes both directions to an upstream.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn proxy_relays_bytes_bidirectionally() {
        // Upstream: an echo server (reads a line, writes it back).
        let upstream = TcpListener::bind("127.0.0.1:0").await.expect("bind upstream");
        let upstream_addr = upstream.local_addr().expect("addr").to_string();
        tokio::spawn(async move {
            let (mut s, _) = upstream.accept().await.expect("accept");
            let mut buf = [0u8; 5];
            s.read_exact(&mut buf).await.expect("read");
            s.write_all(&buf).await.expect("echo");
        });
        // A client connects to a "front" listener whose accepted stream we proxy.
        let front = TcpListener::bind("127.0.0.1:0").await.expect("bind front");
        let front_addr = front.local_addr().expect("addr").to_string();
        let up = upstream_addr.clone();
        tokio::spawn(async move {
            let (client, _) = front.accept().await.expect("accept");
            proxy(client, &up).await;
        });
        let mut c = TcpStream::connect(&front_addr).await.expect("connect front");
        c.write_all(b"hello").await.expect("write");
        let mut got = [0u8; 5];
        c.read_exact(&mut got).await.expect("read echo");
        assert_eq!(&got, b"hello");
    }
}
```
- [ ] **Step 2: Declare the module.** In `crates/cluster/src/lib.rs` add `pub mod route;`.

- [ ] **Step 3: Verify openraft membership API.** The `m.membership_config.membership().get_node(&l)` call must match openraft 0.9.24. The control handler in `transport/server.rs` already uses `m.membership_config.membership().voter_ids()`, so `membership()` is correct; confirm `get_node(&NodeId) -> Option<&BasicNode>` is the right accessor (it is in 0.9.24; if the name differs, find the node-by-id accessor and adapt). `RaftMetrics.id`, `.current_leader` are already read elsewhere.

- [ ] **Step 4: Run the proxy unit test (3×).**
```
cargo build -p cluster --all-targets
cargo clippy -p cluster --all-targets -- -D warnings
cargo test -p cluster --lib route 2>&1 | grep "test result"
```
Expected: `proxy_relays_bytes_bidirectionally` passes on all 3 runs.

- [ ] **Step 5: Commit.**
```
cargo fmt -p cluster
git add crates/cluster/src/route.rs crates/cluster/src/lib.rs
git commit -m "feat(cluster): leader-routing layer (serve_routed + byte proxy + leader resolution)"
```

---

### Task 4: Wire `serve_routed` into `ServerNode` + pack `BasicNode.addr`

**Files:**
- Modify: `crates/cluster/src/server_node.rs`
- Modify: `crates/crabgresql/tests/multiprocess.rs` (add the first routing test)

- [ ] **Step 1: Pack the bootstrap `BasicNode.addr`.** In `crates/cluster/src/server_node.rs`, the `bootstrap` fn builds `BTreeMap<NodeId, BasicNode>` from `peers: Vec<(NodeId, String)>`. The peer string is now already the packed `"node|sql"` (from the CLI / `NodeConfig.peers`), so `BasicNode { addr }` already carries both — confirm `bootstrap` uses the peer string verbatim as `addr` (no change needed if it does). If `NodeConfig.peers` for self-bootstrap is built elsewhere from `node_addr` only, pack it: when constructing the self/peer entries, use `crate::addr::pack(&node_addr, &sql_addr)`. **Confirm** every `BasicNode.addr` that reaches `initialize`/`add_learner` is the packed form.

- [ ] **Step 2: Swap `serve_tls` → `serve_routed`.** In `ServerNode::start`, replace:
```rust
let sql_listener = TcpListener::bind(&cfg.sql_addr).await?;
tokio::spawn(pgwire::server::serve_tls(
    sql_listener,
    engine.clone(),
    Arc::new(pgwire::session::SessionConfig::trust()),
    None,
));
```
with:
```rust
let sql_listener = TcpListener::bind(&cfg.sql_addr).await?;
tokio::spawn(crate::route::serve_routed(
    sql_listener,
    raft.clone(),
    engine.clone(),
    Arc::new(pgwire::session::SessionConfig::trust()),
));
```
(`engine` is `Arc<SqlEngine>`; `serve_routed` takes exactly that.)

- [ ] **Step 3: Write the first routing test** in `crates/crabgresql/tests/multiprocess.rs`:
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_on_follower_is_routed_to_leader() {
    let c = harness::Cluster::spawn(3).await;
    let leader = c.wait_for_leader().await;
    let follower = (0..3u64).find(|&i| i != leader).expect("a follower");
    // Connect to the FOLLOWER's SQL port — the proxy routes us to the leader.
    let client = c.pg(follower).await;
    client.simple_query("CREATE TABLE t (id int4)").await.expect("create");
    client.simple_query("INSERT INTO t VALUES (1)").await.expect("insert");
    let rows = client.simple_query("SELECT id FROM t").await.expect("select");
    let n = rows.iter().filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))).count();
    assert_eq!(n, 1, "SQL on a follower works (proxied to the leader)");
}
```
(`harness::Cluster::spawn` already builds the `--peer` list; since the CLI now packs `node|sql`, the harness must pass the packed peer string — see Step 4.)

- [ ] **Step 4: Harness passes packed peers.** In `crates/crabgresql/tests/harness/mod.rs`, `spawn` builds `peers_arg: Vec<String>` as `format!("{id}@{node_addr}")`. Change it to pack the sql addr too: `format!("{id}@{node_addr}|{sql_addr}")` (each node's `--peer` entry now carries both). `spawn_node` passes `--peer` unchanged. (The node-addr-only form would leave followers unable to resolve the leader's sql port.) Confirm `add_node` (runtime join) likewise builds a packed `--peer` list and the `AddLearner` it sends carries the new node's packed `"node|sql"`.

- [ ] **Step 5: Verify (bring-up still green + new routing test).**
```
cargo build -p crabgresql --tests
cargo clippy -p crabgresql --all-targets -- -D warnings
__COMPAT_LAYER=RunAsInvoker cargo test -p crabgresql --test multiprocess bringup 2>&1 | grep "test result"
__COMPAT_LAYER=RunAsInvoker cargo test -p crabgresql --test multiprocess client_on_follower 2>&1 | grep "test result"
```
Expected: the D2b `bringup_elects_leader_and_serves_sql` still passes (now via `serve_routed`; a leader-local connection serves through `serve_conn`), and `client_on_follower_is_routed_to_leader` passes. Run the latter 3×. If the follower test hangs: confirm the harness passes packed `--peer` (Step 4), that `serve_routed` resolves the leader's sql addr from membership, and that the leader serves locally (not proxying to itself).

- [ ] **Step 6: Commit.**
```
cargo fmt -p cluster -p crabgresql
git add crates/cluster/src/server_node.rs crates/crabgresql/tests/harness/mod.rs crates/crabgresql/tests/multiprocess.rs
git commit -m "feat(cluster): serve SQL via leader-routing in ServerNode; route client-on-follower e2e"
```

---

### Task 5: Multiprocess routing scenarios

**Files:**
- Modify: `crates/crabgresql/tests/multiprocess.rs`

Add the remaining deterministic routing scenarios (each a bounded `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`; all waits via the harness's status-polling helpers — no fixed correctness sleeps).

- [ ] **Step 1: `every_node_serves`.**
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_node_serves_sql() {
    let c = harness::Cluster::spawn(3).await;
    let _l = c.wait_for_leader().await;
    // Seed via node 0.
    let setup = c.pg(0).await;
    setup.simple_query("CREATE TABLE t (id int4)").await.expect("create");
    setup.simple_query("INSERT INTO t VALUES (42)").await.expect("insert");
    // Every node (leader or follower) serves the read (proxied as needed).
    for id in 0..3u64 {
        let client = c.pg(id).await;
        let rows = client.simple_query("SELECT id FROM t").await.expect("select");
        let n = rows.iter().filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))).count();
        assert_eq!(n, 1, "node {id} serves the row (directly or via proxy)");
    }
}
```

- [ ] **Step 2: `routing_follows_failover`.**
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routing_follows_failover() {
    let mut c = harness::Cluster::spawn(3).await;
    let old = c.wait_for_leader().await;
    {
        let client = c.pg(old).await;
        client.simple_query("CREATE TABLE t (id int4)").await.expect("create");
        client.simple_query("INSERT INTO t VALUES (1)").await.expect("insert");
    }
    c.kill(old).await;
    // A new leader emerges among the survivors.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let mut found = None;
        for id in (0..3u64).filter(|&i| i != old) {
            if let Some(st) = c.status(id).await {
                if let Some(l) = st.current_leader { if l != old { found = Some(l); } }
            }
        }
        if found.is_some() { break; }
        assert!(tokio::time::Instant::now() < deadline, "no new leader");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    // A NEW connection to a SURVIVING node reaches the new leader and sees the data.
    let survivor = (0..3u64).find(|&i| i != old).expect("survivor");
    let client = c.pg(survivor).await;
    let rows = client.simple_query("SELECT id FROM t").await.expect("select after failover");
    let n = rows.iter().filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))).count();
    assert_eq!(n, 1, "post-failover connection routes to the new leader and sees committed data");
}
```

- [ ] **Step 3: `no_leader_connection_is_bounded`.** Break quorum (isolate two of three so no node has a majority), then assert a connection attempt to a follower completes-or-fails within a bound (does not hang). Use the harness `control(id, SetPartition(...))` to isolate. A tokio-postgres `connect` wrapped in `tokio::time::timeout` must RETURN (either an error, or — if it blocks on the bounded no-leader wait then the socket closes — a connection error) within, say, 20s (the route layer's `NO_LEADER_WAIT` is 5s, so the connect resolves to an error well inside the bound):
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_leader_connection_is_bounded() {
    let c = harness::Cluster::spawn(3).await;
    let leader = c.wait_for_leader().await;
    // Isolate the leader AND one follower from each other and from the third,
    // so no node can form a majority → no leader. (Partition all pairs.)
    for a in 0..3u64 {
        let others: Vec<u64> = (0..3u64).filter(|&b| b != a).collect();
        c.control(a, cluster::transport::protocol::ControlRequest::SetPartition(others)).await;
    }
    // A connect to any node must not hang: bounded by the route layer's no-leader
    // wait (5s) then a closed socket → tokio-postgres returns an error.
    let target = (0..3u64).find(|&i| i != leader).unwrap_or(0);
    let port = c.sql_addr(target).rsplit(':').next().expect("port").to_string();
    let cs = format!("host=127.0.0.1 port={port} user=postgres");
    let res = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        tokio_postgres::connect(&cs, tokio_postgres::NoTls),
    ).await;
    assert!(res.is_ok(), "connect attempt must resolve within the bound, not hang");
    assert!(res.expect("not timed out").is_err(), "with no leader, the connection is refused/closed");
    // Heal so teardown is clean.
    for id in 0..3u64 { c.control(id, cluster::transport::protocol::ControlRequest::Heal).await; }
}
```
(Confirm `cluster::transport::protocol::ControlRequest` is `pub` — it is. If a no-leader window is hard to force deterministically with partitions, an alternative is to kill two of three nodes; either way the assertion is "connect resolves within the bound".)

- [ ] **Step 4: Run all routing scenarios (3×, non-flaky).**
```
__COMPAT_LAYER=RunAsInvoker cargo test -p crabgresql --test multiprocess 2>&1 | grep "test result"
```
Expected: all routing tests + the prior scenarios pass on 3 runs. Diagnose any hang via a faulted child's stderr (the harness pipes it).

- [ ] **Step 5: Commit.**
```
cargo fmt -p crabgresql
git add crates/crabgresql/tests/multiprocess.rs
git commit -m "test(crabgresql): leader-routing scenarios (every-node, failover, no-leader bound)"
```

---

### Task 6: Random-node bank nemesis

**Files:**
- Modify: `crates/crabgresql/tests/harness/mod.rs`, `crates/crabgresql/tests/multiprocess.rs`

Re-run the crash+partition bank conservation nemesis, but with clients connecting to RANDOM nodes (routed through the proxy) rather than the known leader.

- [ ] **Step 1: Harness round-robin connect helper.** In `crates/crabgresql/tests/harness/mod.rs` add:
```rust
impl Cluster {
    /// Number of nodes (for round-robin client placement).
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
    /// Connect a tokio-postgres client to node `id % len` (deterministic
    /// round-robin so a worker spreads its connections across all nodes —
    /// exercising the proxy on followers). Returns None if that node is
    /// unreachable (killed/partitioned), so the caller can advance.
    pub async fn pg_try(&self, id: usize) -> Option<tokio_postgres::Client> {
        let node = id % self.nodes.len();
        let addr = &self.nodes[node].sql_addr;
        let port = addr.rsplit(':').next()?;
        let cs = format!("host=127.0.0.1 port={port} user=postgres");
        match tokio::time::timeout(
            std::time::Duration::from_secs(8),
            tokio_postgres::connect(&cs, tokio_postgres::NoTls),
        ).await {
            Ok(Ok((client, conn))) => {
                tokio::spawn(conn);
                Some(client)
            }
            _ => None,
        }
    }
}
```

- [ ] **Step 2: Random-node nemesis test.** Add to `multiprocess.rs` a variant of `bank_conserves_under_crash_and_partition_nemesis` where each worker, for each transfer, connects via `pg_try(worker_index + attempt)` (advancing the round-robin index on every failure so it tries a different node), runs the `BEGIN; UPDATE; UPDATE; COMMIT` transfer bounded by timeouts, and counts indeterminate on any error/timeout. The nemesis is unchanged (followers-only, one-fault-at-a-time, leader fixed). After heal, read `final_total` via the leader and assert `final_total == seeded_total` and `committed > 0`.
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bank_conserves_with_random_node_clients() {
    let mut c = harness::Cluster::spawn(3).await;
    let _leader = c.wait_for_leader().await;
    const ACCOUNTS: i64 = 4;
    const SEED: i64 = 100;
    {
        // Seed via whatever node accepts (round-robin until one works).
        let mut idx = 0;
        let setup = loop {
            if let Some(cl) = c.pg_try(idx).await { break cl; }
            idx += 1;
            assert!(idx < 30, "no node accepted the seed connection");
        };
        setup.simple_query("CREATE TABLE accounts (id int8, bal int8)").await.expect("create");
        for id in 0..ACCOUNTS {
            setup.simple_query(&format!("INSERT INTO accounts VALUES ({id}, {SEED})")).await.expect("seed");
        }
    }
    // Workers connect to random (round-robin) nodes; nemesis faults followers.
    // ... (mirror run_durable_bank's worker+nemesis structure from
    //      crates/cluster/tests/jepsen_bank.rs, but each transfer opens its
    //      connection via c.pg_try(worker*K + attempt), advancing on failure;
    //      nemesis = followers-only kill/respawn + partition/heal, one at a time;
    //      terminate the nemesis loop on `!workers.iter().all(|w| w.is_finished())`.)
    // After join + heal + wait_for_leader: read total over a working connection.
    let total = read_total_any(&mut c, ACCOUNTS).await; // sums SELECT bal per id
    assert_eq!(total, ACCOUNTS * SEED, "no acked transfer lost across crash+partition with routed clients");
}
```
**Implementation guidance:** reuse the existing `bank_conserves_under_crash_and_partition_nemesis` structure (workers + inline followers-only nemesis + `is_finished()` termination). The ONLY differences: (a) each transfer connects via `pg_try(round_robin_index)` instead of always the leader, advancing the index on connection failure (so a worker hitting a killed/partitioned node just tries the next); (b) `read_total` at the end loops `pg_try` until a node accepts. Keep counts modest (PROCS=2, OPS=6, ACCOUNTS=4). The conservation assert and the followers-only/one-fault/leader-fixed nemesis are unchanged.

- [ ] **Step 3: Run (3×, non-flaky).**
```
__COMPAT_LAYER=RunAsInvoker cargo test -p crabgresql --test multiprocess bank 2>&1 | grep "test result"
```
Expected: both bank tests (the D2b leader-targeted one + the new random-node one) pass on 3 runs. Conservation holds (`final_total == 400`), `committed > 0`.

- [ ] **Step 4: Commit.**
```
cargo fmt -p crabgresql
git add crates/crabgresql/tests/harness/mod.rs crates/crabgresql/tests/multiprocess.rs
git commit -m "test(crabgresql): bank conservation under crash+partition with routed random-node clients"
```

---

### Task 7: Gauntlet, traceability, finish

**Files:** Verify; no new code unless a gate fails.

- [ ] **Step 1: Gauntlet.** Run each, report PASS/FAIL:
```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
__COMPAT_LAYER=RunAsInvoker cargo test --workspace
cargo test -p pgparser --features oracle
bash scripts/check-no-native.sh        # green on Linux CI; locally only windows-sys (known false-positive)
cargo deny check
```
No new dependency was added, so `check-no-native.sh` / `cargo deny` are unaffected (still only the `windows-sys` Windows false-positive). Confirm the multiprocess tests run (or are skipped only for the documented `os error 740` Windows-launch quirk — they pass under the compat shim and on Linux CI).

- [ ] **Step 2: Success-criteria traceability.** Confirm each spec criterion maps to a green test:

| # | Spec criterion | Verifying test(s) |
|---|---|---|
| 1 | A client on ANY node can run SQL (follower proxies to leader) | `multiprocess::client_on_follower_is_routed_to_leader`; `every_node_serves_sql` |
| 2 | New connections follow failover to the new leader | `multiprocess::routing_follows_failover` |
| 3 | No-leader connection is bounded, not hung | `multiprocess::no_leader_connection_is_bounded` |
| 4 | Bank total conserved under crash+partition with random-node clients | `multiprocess::bank_conserves_with_random_node_clients` |
| 5 | Proxy/resolution unit tests; gauntlet; D1 + D2b suites still pass | `route::tests::proxy_relays_bytes_bidirectionally`; `addr::tests::*`; gauntlet (Step 1) |

If any row lacks a green test, add it.

- [ ] **Step 3: Final whole-diff review + finish.** Dispatch a code-reviewer over the SP10 diff (focus: the route layer — no `metrics().borrow()` Ref held across `.await`; the no-leader bounded wait can't hang; the proxy drops cleanly on an unreachable leader; the `node|sql` parsing doesn't break the in-process `testcluster` or the transport dial; `serve_conn` is behavior-identical for the existing pgwire path; no engine/storage/transport regression). Then run `superpowers:finishing-a-development-branch`.

- [ ] **Step 4: Commit (if anything changed).**
```
git add -A
git commit -m "test(sp10): gauntlet green; D2c leader-routing success-criteria traceability"
```

---

## Self-Review

**Spec coverage:** address encoding `node|sql` (T2); transport dials node half (T2); `serve_conn` extraction (T1); `serve_routed` + proxy + leader resolution + no-leader bounded wait (T3); wired into ServerNode + bootstrap packs addr + CLI (T2/T4); routing scenarios incl. failover + no-leader bound (T4/T5); random-node bank nemesis (T6); proxy/resolution unit tests (T2/T3); gauntlet + traceability (T7); retained in-process/durable/transport paths (untouched). All spec sections map to tasks. The transaction-semantics-across-leader-change note in the spec is an *existing-engine property* (no code in this plan) — correctly no task.

**Placeholder scan:** The T6 nemesis body references "mirror `run_durable_bank`'s structure" rather than re-printing ~80 lines — it names the exact source file/function and the precise deltas (connect via `pg_try(round_robin)`, advance on failure; everything else identical), which is a reuse directive, not a vague TODO. All other steps carry complete code.

**Type consistency:** `node_dial_addr`/`sql_addr_part`/`pack` (T2) used identically in T3/T4; `serve_conn(stream, engine, config, registry, tls)` (T1) called from T3 with those exact args; `serve_routed(listener, raft, engine, config)` (T3) called from T4 with `Arc<SqlEngine>`; `BasicNode.addr` packed form threads through CLI (T2) → bootstrap (T4) → membership → transport dial (T2) + proxy resolution (T3); `pg_try`/`len` (T6) consistent. openraft accessors (`metrics().membership_config.membership().get_node`, `.current_leader`, `.id`) flagged for compiler-verification against 0.9.24 with the existing `transport/server.rs` control handler as the template.
