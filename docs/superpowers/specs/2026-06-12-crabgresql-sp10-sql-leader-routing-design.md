# crabgresql SP10: SQL leader routing (distribution slice D2c)

**Date:** 2026-06-12
**Status:** Approved
**Program spec:** `docs/superpowers/specs/2026-06-11-crabgresql-program-and-wire-protocol-design.md`
**Predecessors:** SP1–SP6 (single-node SQL/MVCC/concurrency), SP7 (single-range Raft, in-memory / D1), SP8 (durable Raft storage / D2a), SP9 (real network transport + multi-process nodes / D2b) — all merged or in review.

## Goal

Make the replicated cluster a **transparently-usable Postgres endpoint**: a SQL
client connecting to **any** node reaches the leader. A non-leader node accepts
the client connection and **byte-proxies** it to the current leader's pgwire port;
the leader serves it on its single engine. After D2b, a client that hit a follower
got a `ForwardToLeader`/`NotLeader` error (the test harness sidestepped this by
targeting the known leader); D2c removes that wart so any stock Postgres client
(psql, tokio-postgres, sqlx) can point at any node and just work.

Constraints unchanged: `#![forbid(unsafe_code)]`; **pure-Rust shipped tree** (no
new dependency — `tokio::io::copy_bidirectional` is already available); parity
baseline PostgreSQL 18.

## Scope of this slice (D2c) and what is deferred

D2 decomposes into D2a–D2d (each its own spec → plan → impl cycle):

| Slice | Scope |
|---|---|
| D2a = SP8 (merged) | Durable Raft log + state machine + node-restart recovery. |
| D2b = SP9 (in review) | Real TCP transport + multi-process node binary + membership + the node serving pgwire SQL + cross-process recovery + a process-level crash+partition harness. |
| **D2c = SP10 (this spec)** | **SQL leader routing** — a client hitting a non-leader is byte-proxied to the leader, so the cluster is one logical Postgres endpoint at any node. |
| D2d | Real over-the-wire **Jepsen + Elle** against the multi-process durable cluster. |

D3 (range routing), D4 (range splits), D5 (leases / linearizable reads /
rebalancing), an MVCC vacuum/GC slice, and cross-range distributed transactions
remain later sub-projects.

**Everything routes to the leader.** Because the proxy is a transparent byte-relay,
reads and writes both execute on the leader's single engine — correct MVCC and
SP6 row-locking, and **strongly consistent reads** (no stale follower reads). The
transparent byte-proxy choice *is* this scope: a byte-relay cannot selectively
serve reads locally. Follower-local reads / read scaling / linearizable follower
reads are **D5** and explicitly out of scope here.

**No new persistence, no new transport, no engine change.** D2c is purely a
frontend routing layer over D2b. The durable storage (D2a), the openraft wiring,
the Raft TCP transport (D2b), and the SQL/MVCC engine (SP1–SP6) are untouched. The
in-process `Switchboard`/`Cluster` path is also untouched.

## Architecture

### Address encoding (the enabling mechanism)

openraft's `BasicNode.addr` currently holds only a node's Raft RPC address (the
`--node-addr`). D2c packs **both** addresses into it:

```
BasicNode.addr = "<node_addr>|<sql_addr>"
```

This rides in Raft membership, so it replicates to every node and survives runtime
join/leave (D2b's `add_learner`/`change_membership` carry it).

- **Transport client** (`cluster/src/transport/client.rs`): `new_client` dials
  `node.addr.split('|').next().unwrap()` — the node_addr — for Raft RPCs.
  `split('|').next()` returns the whole string when there is no `|`, so the
  in-process loopback `testcluster` (which sets node-addr only) is **unaffected**.
- **Leader SQL resolution** (the proxy): from `raft.metrics()`, read
  `current_leader: Option<NodeId>` and the membership config; look up the leader's
  `BasicNode`, take `addr.split('|').nth(1)` → the leader's sql_addr.
- **CLI** (`crabgresql node`): `--peer ID@node_addr|sql_addr` (the part after `@`
  is the full packed `BasicNode.addr`). The bootstrapper's `initialize` map and the
  `AddLearner` control request both carry `"node|sql"`. `NodeConfig` already has
  `node_addr` + `sql_addr`, so self-bootstrap packs them.

### The routing layer (`cluster/src/route.rs`, new)

`serve_routed(listener, raft, engine, session_config)` owns the public sql-addr
accept loop. Per accepted connection (spawned task):

1. Read `raft.metrics().borrow()`: `current_leader`, this node's `id`, membership.
2. **`current_leader == Some(self_id)`** → serve locally:
   `pgwire::server::serve_conn(stream, engine, session_config, None)` (the existing
   per-connection handler).
3. **`current_leader == Some(other)`** → resolve `other`'s sql_addr from membership
   → `proxy(stream, sql_addr)`.
4. **`current_leader == None`** → **bounded-wait** for a leader (poll metrics up to a
   deadline, e.g. a few seconds). If one appears, route as above (a transient
   election window just yields a slightly-slow connect, no error). If the deadline
   passes, close the socket (the client retries). This keeps the proxy a pure
   byte-relay — it never needs to speak pgwire to emit an error.

`proxy(client: TcpStream, leader_sql_addr)`: connect to `leader_sql_addr` (bounded
timeout); on failure, close the client (leader unreachable → client retries). On
success, `tokio::io::copy_bidirectional(&mut client, &mut upstream)` relays the
full pgwire session (startup, auth, simple + extended query, results) verbatim
until either side closes.

**pgwire `serve_conn` extraction** (`pgwire/src/server.rs`): `serve_tls` currently
accept-loops and spawns a private `handle_conn` per connection. Extract a
`pub async fn serve_conn<E: Engine>(stream, engine: Arc<E>, config: Arc<SessionConfig>, tls: Option<TlsAcceptor>) -> io::Result<()>`;
`serve_tls` calls it in its loop (no behavior change). `serve_routed` calls it for
the leader-local case.

### Edge cases

- **Routing is decided at connection time.** An established proxied connection keeps
  relaying to its original target. If that target loses leadership, its writes fail
  (`NotLeader` surfaces as a SQL error, relayed to the client) or, if it crashed,
  the upstream socket drops and the proxy closes the client. Either way the client
  reconnects and is re-routed to the new leader. Standard proxy semantics.
- **Stale routing view is self-correcting.** If a follower's metrics still name an
  old leader, it proxies there; that node rejects the write (`NotLeader`); the
  client gets a fast error and retries; the follower's metrics soon update. A
  transient blip, never corruption.
- **The leader never proxies to itself** (the `self == leader` check serves locally).
- **TLS passes through transparently.** A byte-relay does not decrypt; a TLS
  client's handshake relays to the leader, which terminates it. (Trust/no-TLS, the
  test default, is unaffected.)

### Transaction semantics across a leader change (existing-engine property)

D2c's proxy is transparent to transactions — they execute on the leader's engine
exactly as for a directly-connected client. It is worth recording *why* a leader
change mid-transaction is safe, because the proxy inherits (does not implement)
this guarantee:

The engine replicates **incrementally** (mirroring PostgreSQL): each in-transaction
write statement proposes its new row versions through Raft immediately
(`session.rs` `run_write`), tagged with the txn's xid but **without a commit
marker** (so invisible); the SQL `COMMIT` proposes only `clog[xid]=Committed`
(+ `next_xid`), which **atomically flips visibility** for those versions.

On a leader change mid-transaction: the `COMMIT`'s `client_write` returns
`ForwardToLeader → NotLeader`, so `clog[xid]` is never set — the replicated
versions stay **invisible forever**; the transaction has no visible effect anywhere
(clean all-or-nothing abort, enforced by the clog gate). The orphan versions can
never be revived because `next_xid` is folded into the **same atomic batch** as the
row versions, so the moment any version tagged `xid` replicates, `next_xid ≥ xid+1`
replicates with it; the new leader reseeds from applied `next_xid` and never
re-hands-out that xid (this is exactly what SP7's "FOR-UPDATE xid-reuse across
failover" fix protects). Residuals, all pre-existing and not D2c concerns: orphan
invisible versions accumulate (no MVCC vacuum/GC yet — a later slice); the
universal "indeterminate commit" (leader commits the marker then crashes before
ack); and the impossibility of transparent transaction failover (the client retries
on a leader change, as in any Raft/Postgres system).

## Testing

### Proxy / resolution unit tests (in-process, `cluster`)

- **Leader-sql-addr resolution:** given a `BasicNode.addr = "node|sql"` and a
  current_leader, the right sql_addr is extracted; `self == leader` yields the
  serve-local decision (vs proxy).
- **Byte-relay:** spin a dummy echo upstream on loopback, connect a client through
  `proxy`, assert faithful bidirectional round-trip — exercises the
  `copy_bidirectional` plumbing without a full cluster.

### Deterministic multiprocess routing scenarios (extend `crabgresql/tests/multiprocess.rs`)

- `client_on_follower_is_routed` — tokio-postgres to a **follower**'s sql-addr; CREATE/INSERT/SELECT works (proxied to the leader).
- `every_node_serves` — connect to all 3 nodes in turn; each works.
- `routing_follows_failover` — commit data; kill the leader; after a new leader emerges, a **new** connection to any node reaches it and sees the data.
- `no_leader_connection_is_bounded` — break quorum (isolate a majority); a connect attempt completes-or-closes within a deadline, never hangs.

### Random-node bank nemesis (extend the D2b crash+partition nemesis)

The crash+partition bank-conservation nemesis, but each transfer client connects to
a **random** node (not the known leader) and reconnects to another random node on
failure. The nemesis stays followers-only / one-fault-at-a-time so the leader keeps
quorum. After healing, assert `final_total == seeded_total` and `committed > 0` —
proving transparent routing holds under the same faults.

Harness: add a `pg_any()`/`pg_random()` helper (connect to a random node); existing
`pg(id)` now works on any node. The in-process `Switchboard` D1/D2a suites and the
D2b multiprocess suite remain green.

## Crate structure

- `crates/pgwire/src/server.rs` — extract `pub async fn serve_conn` from `serve_tls`.
- `crates/cluster/src/route.rs` **(new)** — `serve_routed` + `proxy` + leader-sql-addr resolution + no-leader bounded wait.
- `crates/cluster/src/server_node.rs` — swap the `serve_tls` spawn for `serve_routed`; pack `BasicNode.addr = "node|sql"` in bootstrap.
- `crates/cluster/src/transport/client.rs` — `new_client` dials the `node_addr` half of `BasicNode.addr`.
- `crates/crabgresql/src/main.rs` — `--peer ID@node|sql` parsing.
- Tests: `cluster` route unit tests; `crabgresql/tests/multiprocess.rs` routing scenarios + random-node nemesis.

## Dependencies & purity

No new dependency. `tokio::io::copy_bidirectional` is in tokio's already-enabled
`io-util` feature. `#![forbid(unsafe_code)]` (workspace lint) intact; `cargo-deny`
and `scripts/check-no-native.sh` stay green (modulo the pre-existing `windows-sys`
Windows-only false-positive).

## Success criteria

1. A SQL client connecting to **any** node (leader or follower) can run queries — a follower transparently proxies to the leader.
2. After a failover, a new connection to any node reaches the new leader and sees committed data.
3. A no-leader connection is **bounded** (completes-or-closes within a deadline), never hung.
4. The bank total is conserved under the crash + partition nemesis with clients on **random** nodes; `committed > 0`.
5. Proxy/resolution unit tests pass; full gauntlet green; in-process (D1) + D2b multiprocess suites still pass.

## Deferred

Follower-local / read-scaling / linearizable follower reads (D5); MVCC vacuum/GC of
orphan versions (separate slice); transparent transaction failover (not achievable —
universal); range routing / splits / leases (D3–D5).
