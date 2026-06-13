# crabgresql SP9: Real network transport + multi-process nodes (distribution slice D2b)

**Date:** 2026-06-12
**Status:** Approved
**Program spec:** `docs/superpowers/specs/2026-06-11-crabgresql-program-and-wire-protocol-design.md`
**Predecessors:** SP1 (wire), SP2 (vertical slice), SP3 (durable storage), SP4 (transactions), SP5 (PG-faithful MVCC), SP6 (concurrent writers), SP7 (single-range Raft, in-memory / D1), SP8 (durable Raft storage + restart recovery / D2a) — all merged.

## Goal

Turn each replica from an in-process `Node` (wired by the in-memory `Switchboard`)
into a real, independently-addressable **OS process** that:

- talks Raft over **TCP** (a hand-rolled, length-prefixed, postcard-encoded
  protocol implementing openraft's `RaftNetwork`/`RaftNetworkFactory`),
- serves SQL over **pgwire** (a runnable replicated SQL server),
- recovers across **real process boundaries** from its on-disk directory, and
- supports **runtime membership** (join a learner, promote it, remove a node).

The crash model becomes faithful: a "crash" is `kill -9` of a process (true power
loss, true process isolation); a "restart" respawns the binary from the same
data dir. This is exactly the substrate D2d's over-the-wire Jepsen/Elle needs.

Constraints unchanged: `#![forbid(unsafe_code)]`; **pure-Rust shipped tree** (the
new deps — `tokio-util`, `postcard` — are pure Rust); parity baseline PostgreSQL 18.

## Scope of this slice (D2b) and what is deferred

D2 decomposes into D2a–D2d (each its own spec → plan → impl cycle):

| Slice | Scope |
|---|---|
| D2a = SP8 (merged) | Durable Raft log + state machine + node-restart recovery, in-process. |
| **D2b = SP9 (this spec)** | Real TCP transport + multi-process node binary + cluster membership (static bring-up **and** runtime join/leave) + the node serving pgwire SQL + cross-process recovery + a process-level crash+partition test harness. |
| D2c | SQL frontend leader **routing** — a client hitting a non-leader is redirected/forwarded (D2b's harness targets the known leader directly). |
| D2d | Real over-the-wire **Jepsen + Elle** against the multi-process durable cluster. |

D3 (range routing), D4 (range splits), D5 (leases / linearizable reads /
rebalancing), and cross-range distributed transactions remain later sub-projects.

**What D2b adds is transport + process lifecycle, not persistence.** All durable
state (vote, log, committed, last_applied, applied data) is already on disk from
D2a; D2b adds listener bind + reconnect-on-drop, so recovery crosses a real
process boundary with nothing carried in memory — the strongest test of D2a's
durability. The in-process `Switchboard`/`Node`/`Cluster` path is **retained**
unchanged as the fast path for D1's deterministic fault scenarios and D2a's
durable-storage tests.

**Decisions locked during brainstorming.** Real OS processes (not multi-task);
static membership **plus** runtime join/leave; the node serves pgwire SQL with the
harness targeting the known leader (auto-redirect stays in D2c); rigor =
crash/restart + kill-9 nemesis **plus** app-layer network partitions; transport =
hand-rolled framed serde over tokio TCP (not HTTP, not a generalization of the
pgwire framing).

## Architecture

### Node process & binary mode (`crates/crabgresql/src/main.rs`)

The binary already uses clap and serves one SQL engine on `--listen`. Refactor
`Args` into a subcommand: the existing single-server mode stays the default; add

```
crabgresql node --id <u64> --node-addr <host:port> --sql-addr <host:port> \
                --data-dir <path> --peers <id@host:port,...> [--bootstrap]
```

On start the node: (1) opens `NodeStore::open(data_dir)` (durable, from D2a);
(2) builds `openraft::Raft` with the **TCP `RaftNetwork`** (§ transport);
(3) starts two listeners — the internal **node protocol** (`--node-addr`: Raft
RPCs + control) and the **pgwire SQL** server (`--sql-addr`); (4) builds one
shared `Arc<SqlEngine>` and a reseed-on-leadership task; (5) if `--bootstrap` and
the group is not already initialized, runs `initialize` once peers are reachable.

**Peer addressing via `BasicNode`.** Today the code uses `BasicNode::default()`
(empty addr). D2b populates `BasicNode { addr }` with each peer's `--node-addr` at
membership time (`initialize`/`add_learner` carry the address), so the
currently-ignored `_node: &BasicNode` in `new_client` becomes the dial target.
`--peers` feeds the bootstrapper's `initialize` map and each node's own listen
addr; outbound routing thereafter uses the replicated membership addresses.

### Raft RPC transport + control plane (`crates/cluster/src/transport/`)

New module (`frame`, `protocol`, `client`, `server`, `partition`), a parallel TCP
implementation of the same traits the `Switchboard` implements in-process. One
framed protocol over `--node-addr` carries both Raft RPCs and control; pgwire SQL
is a separate Postgres protocol on `--sql-addr` (two ports per node).

**Framing & encoding.** Length-delimited frames (`tokio_util::codec::LengthDelimitedCodec`,
u32 BE prefix, a generous `max_frame_length` for snapshot chunks), **postcard**-encoded
(compact pure-Rust serde — all openraft request/response/error types derive
`Serialize`/`Deserialize` under our `TypeConfig`). Envelope:

```rust
enum NodeRequest  { Raft { from: NodeId, rpc: RaftRpc }, Control(ControlRequest) }
enum RaftRpc      { AppendEntries(AppendEntriesRequest<TypeConfig>),
                    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
                    Vote(VoteRequest<NodeId>) }
enum NodeResponse { Raft(RaftRpcResponse), Control(ControlResponse) }
// RaftRpcResponse variants carry Result<Resp, RaftError<..>> (serialized over the wire).
```

**Client (`TcpConn` + `TcpRaftNetwork`).** `new_client(target, node)` →
`TcpConn { from, target, addr: node.addr, partition, conn: Option<Framed<TcpStream>> }`.
Each RPC method: (1) if `partition.blocked(target)` → `RPCError::Unreachable`;
(2) lazily connect, reconnecting if the held stream broke (this is how a peer
restart heals — the next RPC redials); (3) send `Raft{from, rpc}`, await the
response under the `RPCOption` timeout; (4) I/O error/timeout → drop the conn,
return `Unreachable`; remote `Err` → `RemoteError`; `Ok` → the response. Because
`RaftNetwork` methods take `&mut self`, calls on one `TcpConn` are serialized —
one in-flight request per connection, so simple request/response over the held
stream is correct (no multiplexing).

**Server.** A `TcpListener` on `--node-addr`; per connection a task reads
`NodeRequest`s and writes `NodeResponse`s. For `Raft{from, rpc}`: if
`partition.blocked(from)` → drop (the *receive* side of a partition), else
dispatch to the local `raft.append_entries/install_snapshot/vote(req).await` and
serialize the `Result`. Checking the partition on **both** send and receive makes
a cut bidirectional, exactly like `Switchboard::cut`.

**Partition state.** `PartitionState { blocked: Mutex<HashSet<NodeId>> }`, shared
by this node's client + server, toggled via control. Portable app-layer faults —
no OS firewall, identical on Windows and Linux CI — driving the partition nemesis.

**Control protocol** (same node port): `GetStatus` → `{ id, state, current_leader,
last_log_index, last_applied, members }` (from `raft.metrics()`); `SetPartition(Vec<NodeId>)`
/ `Heal`; `AddLearner{id,addr}` and `ChangeMembership(Vec<NodeId>)` (leader-only,
runtime reconfig); `Shutdown` (graceful — `raft.shutdown()` then exit, for
clean-restart scenarios; crashes use `kill -9`). The harness uses `GetStatus` to
find the leader + wait for apply-catchup, and the rest to inject faults and
reconfigure.

### Membership: bring-up + runtime join/leave

**Bring-up.** The harness spawns 3 processes with `--peers` and `--bootstrap` on
one; the bootstrapper waits until peers' node-ports answer `GetStatus`, then
`raft.initialize({0→BasicNode{addr0}, …})`. Membership (incl. addresses)
replicates to all nodes.

**Join.** Spawn node 3 (fresh `--data-dir`); send `AddLearner{3, addr3}` to the
leader → `raft.add_learner(3, …, blocking=true)` streams log/snapshot to it **over
TCP**; then `ChangeMembership({0,1,2,3})` promotes it to voter. This exercises
over-the-wire catch-up.

**Leave.** `ChangeMembership({0,1,3})` to the leader removes node 2, after which
its process can be killed.

### SQL frontend on the node

The node runs the existing pgwire server on `--sql-addr`, backed by **one shared
`Arc<SqlEngine>`** (`SqlEngine::replicated` over the NodeStore's `data` keyspace +
a `RaftCommitter` on the local raft). All client connections share that engine, so
SP6 row-lock/MVCC semantics hold across sessions (same `RowLockManager`/`ProcArray`),
exactly as the in-process bank test relied on. On a follower, `client_write`
returns `ForwardToLeader` → surfaced as a SQL error (no auto-redirect — D2c); the
harness connects only to the known leader.

**Reseed-on-leadership (closes the SP7/8 deferral).** SP8's `node.rs` left
"automatic reseed via metrics-subscription on leadership change" to D2. A task on
`raft.metrics()` (a `watch` receiver), on the follower→leader edge, calls
`engine.reseed_counters()` so xid/seq never regress below a prior leader's
high-water mark. Reseed is idempotent (only bumps counters upward) and the harness
waits for leader status before writing, so the edge-triggered reseed lands first;
the SP7 commit-time counter fold remains the durable backstop.

### Cross-process recovery

Crash = `kill -9` (the OS releases fjall's `.lock` immediately). Restart = respawn
with the same `--data-dir`/`--id`/`--node-addr`: `NodeStore::open` journal-replays,
openraft resumes from the persisted vote/log/last_applied, the node re-binds its
listeners, and peers' `TcpConn`s reconnect on their next RPC and backfill it. D2b
adds no new persistence — only listener bind + reconnect-on-drop, which the
transport already does. (The in-process SM-worker-Arc lock race that D2a's restart
must guard against cannot occur here — a killed process releases its lock at the
OS level.)

## Testing

### Process-level harness (`crates/crabgresql/tests/multiprocess.rs` + a `harness` module)

The harness lives in the **binary's** package so `env!("CARGO_BIN_EXE_crabgresql")`
resolves the just-built binary. It depends on `cluster` (to reuse the
`ControlRequest`/`Response` types + frame/postcard codec) and dev-deps
`tokio-postgres` (drive SQL), `tempfile`, `tokio`. Responsibilities: pick free
localhost ports + temp dirs per node; spawn nodes via `tokio::process::Command`
(capturing stdout/stderr for diagnostics); poll `GetStatus` for readiness/leader;
open a control client and a tokio-postgres client to the leader; inject faults —
`child.kill()` (SIGKILL on Unix / TerminateProcess on Windows), respawn (same dir
+ ports), `SetPartition`/`Heal`, graceful `Shutdown`. **No fixed sleeps for
correctness** — every wait polls `GetStatus`/`last_applied`.

### Rigor matrix

Deterministic scenarios (each a bounded `#[tokio::test]`):

1. **Bring-up + election over TCP** — 3 nodes elect a leader; a leader write is visible.
2. **Committed write survives kill-9 + respawn** — recovered from disk, rejoins.
3. **Follower catch-up over the wire** — a node misses writes (down/partitioned), then catches up via TCP replication.
4. **Leader-kill failover** — kill the leader process; a new leader emerges; committed data present; old leader respawns and rejoins.
5. **Runtime learner join + over-TCP catch-up** — add node 3, it catches up a non-trivial log/snapshot over the wire, is promoted, serves reads.
6. **Runtime leave** — remove node 2 via `ChangeMembership`, then kill it; cluster continues.
7. **Minority partition** — isolate 1 node via `SetPartition` (both sides); the majority keeps committing; heal → the minority catches up.

**Nemesis (Jepsen deliverable):** **bank conservation under a crash + partition
nemesis.** Seed `accounts` (known total) via SQL on the leader; concurrent
tokio-postgres transfer clients drive `BEGIN; UPDATE −amt; UPDATE +amt; COMMIT`
against the leader; a nemesis kills/respawns **followers** and isolates a
**minority** between barriers (the leader stays in the majority, so the clients'
target stays valid; if the leader moves, clients reconnect to the new leader). A
transfer nets zero, so as long as each txn is atomic the total is conserved across
crashes and partitions. After healing, re-resolve the leader, reseed, and assert
`final_total == seeded_total` and `committed > 0` (non-vacuous).

The in-process `Switchboard` D1/D2a suites remain the fast path, unchanged.

### Known risk (Windows dev box)

Spawning child node processes may hit the same `os error 740` elevation quirk
noted for the `update_delete` executor test. The multiprocess suite is validated
on **Linux CI** and, on Windows, run via the `__COMPAT_LAYER=RunAsInvoker` shim or
skipped (an environment quirk, not a code defect) — consistent with the existing
update_delete note.

## Crate layout

- New `crates/cluster/src/transport/{mod,frame,protocol,client,server,partition}.rs` — TCP `RaftNetwork` + control protocol + partition state.
- A `ServerNode` (new `server_node.rs`, or an extension of `node.rs`) tying together NodeStore + openraft-over-TCP + the two listeners + shared `Arc<SqlEngine>` + reseed task — the runnable node.
- `crates/crabgresql/src/main.rs` — the `node` subcommand wiring.
- `crates/crabgresql/tests/multiprocess.rs` (+ `harness` module) — process spawn/kill/respawn, control + SQL clients, scenarios, nemesis.
- In-process `Switchboard`/`Node`/`Cluster` unchanged.

## Dependencies & purity

- `cluster`: add `tokio-util` (codec) + `postcard` (encoding) — pure Rust; `tokio` already present.
- `crabgresql`: add `cluster` (node mode); dev-deps `tokio-postgres`, `tempfile` (harness). `clap`/`tokio`/`pgwire`/`executor` already present.
- No native crate is introduced; `cargo-deny` and `scripts/check-no-native.sh` stay green (modulo the pre-existing `windows-sys` Windows-only false-positive). `#![forbid(unsafe_code)]` preserved (workspace lint).

## Success criteria

1. Three real OS-process nodes elect a leader over TCP and serve SQL on the leader.
2. Committed data survives `kill -9` + respawn of any node (recovered from disk over the wire).
3. Runtime join (learner catch-up over TCP) and leave both work.
4. A minority network partition is tolerated (majority progresses); heal → catch-up.
5. The bank total is conserved under the crash + partition nemesis (no acked transfer lost); `committed > 0`.
6. Reseed-on-leadership prevents xid regression across a real leader change.
7. Pure-Rust tree, `forbid(unsafe)`, full gauntlet green; in-process D1/D2a suites still pass.

## Deferred

Auto leader-redirect for arbitrary clients (D2c); over-the-wire Elle/Jepsen
analysis (D2d); range routing / splits / leases / linearizable cross-failover
reads (D3–D5). The implementation plan will sequence D2b across tasks (transport →
node binary → bring-up → runtime reconfig → SQL frontend → harness/nemesis).
