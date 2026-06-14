# SP14 / D3a-net — Network range routing (per-statement gateway forwarding over TCP)

**Predecessors:** SP1–SP6 (single-node SQL/MVCC/concurrency), SP7 (single-range Raft / D1), SP8 (durable Raft storage / D2a), SP9 (real TCP transport + multi-process `ServerNode` / D2b), SP10 (SQL leader routing — `serve_routed`, `node|sql` address encoding / D2c), SP11 (over-the-wire serializability / D2d), SP12 (linearizable reads / D5), **SP13 (in-process multi-range core — `RangeMap`, range-aware in-process `Switchboard`, `MultiRangeCluster`, `RangeRouter` / D3a)** — all merged or in review.

**Goal:** Lift D3a's verified in-process multi-range routing onto the **real network**. Make every `ServerNode` host a replica of **every** range and act as a **SQL gateway**: a client connects to *any* node, and each statement is forwarded to the **owning range's leader** across the cluster, then the result is relayed back. The SP7→SP9 pattern, executed a second time (build the core in-process in D3a, add the network in a follow-up), and the project's own next roadmap sub-slice.

**Architecture:** Three pieces. (1) The TCP node transport becomes **range-aware** — `NodeRequest::Raft` carries a `RangeId` (with a serde default for wire compatibility), and the server dispatches each inbound RPC from a `(RangeId, NodeId)` handle registry that is **net-new on the TCP path** (the in-process `Switchboard` already keys this way and is the design *template*, not shared code). (2) `ServerNode` becomes **multi-range** — it loops over the static `RangeMap`, opens **per-range fjall keyspaces** for each range's log + state machine under one `NodeStore`, builds N Raft instances over the range-aware transport, and bootstraps each group. This is genuinely new construction with no existing durable analog. (3) `serve_routed`/`route_one` becomes a **per-statement gateway** — for each simple-query statement it computes the target range from the `RangeMap` (DDL/catalog → range 0; single-table DML/SELECT → the table's range, schema resolved from range 0's leader); if this node leads that range it runs the statement on the local range-leader engine, otherwise it forwards the statement to the range leader's SQL port over a **pooled, minimal pgwire forwarding client** and relays the single response back.

**Tech stack:** Rust 2024, openraft 0.9.24 (one Raft instance per range per node), existing `cluster`/`executor`/`pgwire`/`transport` crates, fjall (durable per-range keyspaces). **No new shipped dependency** (the forwarding client is built on the existing `pgwire` frame primitives, not a new PG-client crate). `#![forbid(unsafe_code)]`, pure-Rust unchanged. Tests: in-crate loopback-TCP for the deterministic unit/integration layers, the multi-process harness for the e2e gateway/fault proof (Windows-safe — see §4).

---

## Where this sits: the D3 decomposition

| Sub-slice | Scope |
|---|---|
| D3a = SP13 | In-process multi-range core: static range map, N co-located per-range Raft groups + MVCC, key→range SQL routing, single-range transactions. Tested in-process. ✅ |
| **D3a-net = SP14 (this spec)** | **Network range routing**: multi-range `ServerNode` + range-aware TCP transport + a per-statement gateway that forwards each statement to the owning range's leader across nodes and relays the result back. The multi-process analog of D3a. |
| D3b | Cross-range read scatter-gather (`SELECT` fans out across ranges) + range descriptors move into a replicated **meta range** + a structured query RPC + prepared-statements-across-ranges. |
| later | Cross-range distributed transactions (2PC over Raft groups); dynamic placement / rebalancing. |
| D4 | Range splits. |

Everything below D3a-net in that table is **out of scope** for this slice.

## The load-bearing constraint (why the design is shaped this way)

`RaftCommitter::commit` calls `openraft::Raft::client_write` (`committer.rs:39`), which **does not auto-forward**: on a follower it returns `ForwardToLeader`/`NotLeader`. In-process this is invisible because `MultiRangeCluster::leader_engine(r)` hands back the **leader node's local** Raft handle. But a gateway process holds Raft handles only for its **own** co-located replicas, so it **cannot** build a working `RaftCommitter` for a *remote* range's leader. A remote range's statement therefore must execute **on the remote leader node**.

The chosen mechanism forwards at the **SQL boundary** (Decision 5): when the target leader is remote, the gateway sends that statement to the leader's pgwire SQL port and the leader node's normal local `SqlEngine::replicated` runs it. The executor, the committer, and the `catalog_kv` seam are unchanged. **A reviewer or implementer who reaches for a remote `RaftCommitter` will compile but dead-end at runtime with `NotLeader`** — the spec locks SQL-boundary forwarding.

## Decisions (locked during brainstorming)

1. **Slice = D3a-net** (network range routing), chosen over D3b (whose scatter-gather has no SQL surface — no joins, single-table `FROM`) and cross-range 2PC (which wants this network layer first).
2. **Co-located placement, carried forward.** Every node hosts a Raft replica of every range; the gateway routes by "which range's leader," resolved over the wire.
3. **Leader targeting: resolve + forward directly (one hop).** The receiving node resolves the target range's leader from its **own** range-`r` Raft metrics watch (dropping the `Ref` before any `await`) and forwards directly; on `NotLeader`/stale-leader/wire error it re-reads metrics **once** and retries against the fresh leader, bounded by a deadline; on exhaustion the error is surfaced to the client. A paused/unreachable node that still self-reports `Leader` is excluded (the SP13 `is_paused` lesson). No polling, no sleep.
4. **RangeMap stays static config** — identical boundaries on every node via `NodeConfig`/CLI. The replicated meta range is D3b.
5. **Forwarding = send the statement to the leader's pgwire SQL port via a minimal pgwire client**, *not* a structured RPC (deferred to D3b) and *not* the existing `proxy()` (which is a whole-connection relay — see Component 3). Built on existing `pgwire` frame primitives: no new dependency.
6. **One slice, minimal viable cut** — T1 range-aware transport, T2 multi-range durable `ServerNode`, T3 local gateway, T4 remote forward, T5 os-740 rename + `CLAUDE.md`, T6 multi-process e2e, T7 gauntlet. Defer structured RPC, prepared-statements-across-ranges, replicated RangeMap, per-range control to D3b.

**Internal decisions (locked, with resolution):**

- **`range` lives only in `NodeRequest::Raft { from, range, rpc }`** (`protocol.rs:61`), `#[serde(default)]` so a range-unaware payload still decodes (value 0) — not inside the `RaftRpc` variants. Simpler serde, no openraft call-site change; matches the in-process `(range, node)` registry-key design. (One binary talks to itself, so only new↔new is strictly needed; the serde default makes the claim precise.)
- **Per-range durable storage isolation (scheme):** **per-range keyspaces.** `NodeStore::open` (`durable.rs:22`) creates, for each range `r`, the keyspaces `data-r{r}` and `raft-r{r}`; `DurableLogStore::open` (`durable.rs:118`) and `DurableStateMachineStore::open` (`durable.rs:411`) gain a `range: RangeId` parameter selecting the suffixed keyspaces. Isolation must span **both** keyspaces: app rows live in `data` (what `sm_kv()` exposes) while the Raft log, vote, committed/purged markers, and the SM `last_applied`/`membership` live in the **separate** `raft` keyspace (`durable.rs:65-83`). fjall keyspaces are isolated by construction, so this is structural isolation, not a per-key prefix that every write site must remember. This is the slice's highest-risk new construction and is asserted by a dedicated test (criterion 3).
- **Single-range backward compatibility:** `NodeConfig` defaults to `RangeMap::single()` (N=1, range 0). All SP9/SP10 multi-process tests then run the existing per-connection fast-path unchanged. The static map **is** the switch — no feature flag. This regression gate is load-bearing.
- **Retry-on-NotLeader lives in the gateway/route layer**, not in `RangeRouter` (which stays a pure routing/`Pin` component).
- **Cross-range extended protocol (Parse/Bind/Execute) is rejected** with the existing `0A000` path; simple-query routing is the locked surface.
- **Connections are sticky to the gateway node**; a pinned transaction never migrates gateways. **Stated limitation (untested this slice):** a gateway crash aborts its in-flight pinned transaction.
- **Catalog availability:** every data-range statement resolves its schema from range 0's **current leader**, read **over the wire** when this node is a range-0 follower (Component 3) via the existing SP12 linearizable read against the range-0 leader engine — **no new RPC type**. If range 0 has no leader, the catalog read fails the statement with a bounded retryable error (`NotLeader`/`Unavailable`), never an indefinite wait. **Untested this slice** (listed in Non-goals).
- **Control requests stay node-global** (`GetStatus`/`SetPartition`/`Heal`/`Shutdown`): pausing/cutting a node takes all its co-located ranges, the realistic co-located fault. Per-range control is deferred.

## Components

### 1. Range-aware node transport (`crates/cluster/src/transport/{protocol,client,server}.rs`)

Add `#[serde(default)] range: RangeId` to `NodeRequest::Raft` (`protocol.rs:61`, today `Raft { from, rpc }`). The inbound destructure at `server.rs:53` and `dispatch_raft` (`server.rs:74-80`, today calling `raft.append_entries` against a **single** held `Raft`) change so the server resolves the target group from a **`(RangeId, NodeId)`→`Raft` registry** and dispatches there; an unregistered range returns `Unreachable` (mirroring `Switchboard::handle` returning `None`). `TcpRaftNetwork`/`TcpConn` (`client.rs:22-49`, **no `range` field today**) gain a `range` set by the per-group factory and packed at `client.rs:66-69`. `frame.rs` is generic over `Serialize`/`Deserialize`. `ControlRequest` stays node-global. The registry and `range` field are **net-new on TCP** — the in-process `Switchboard` is the template, not shared code.

### 2. Multi-range durable `ServerNode` (`crates/cluster/src/server_node.rs`, `durable.rs`, `crabgresql/src/main.rs`)

Refactor `ServerNode::start` (`server_node.rs:70-127`, today one store + one Raft for range 0) to build **every** range: take a `RangeMap` in `NodeConfig` (static, identical on all nodes), loop `map.range_ids()`, open the per-range keyspaces from the isolation scheme above, build N Raft instances over the range-aware `TcpRaftNetwork` from Component 1, register each at `(range, id)` in the process-local handle registry the Component 1 server resolves against, and bootstrap each range's voting group. The reseed-on-leadership loop monitors **all** ranges. **There is no existing durable multi-range constructor to copy:** `Node::start_durable` is single-range, and `MultiRangeCluster::new` (`cluster.rs:38-56`) builds **in-memory** nodes over the in-process `Switchboard`. The construction loop is new code and **strictly depends on the per-range storage-isolation scheme being defined first**. Default `RangeMap::single()` preserves the single-range path. This is the slice's center of gravity.

### 3. The gateway — per-statement range demux + forward (`crates/cluster/src/route.rs`, `range/router.rs`, `addr.rs`)

Today `route_one` (`route.rs:43-96`) makes a **one-shot** local-vs-proxy decision per connection, and `proxy()` (`route.rs:101-107`) runs `tokio::io::copy_bidirectional` over the **whole** stream — a dumb full-connection relay that **cannot** forward one statement and resume local execution on the next. D3a-net rewrites `route_one` into a **per-statement message loop**: read each simple-query frame, parse, compute the target range (range-0 catalog resolution; `RangeMap::range_for_table`), and:

- **Local leadership:** run the statement on the local range-`r` leader engine, reusing `RangeRouter`'s dispatch/`Pin` logic.
- **Remote leadership:** resolve the leader's `sql_addr` from range-`r` membership/metrics (`addr.rs sql_addr_part`/`pack`) and forward the single statement over a **pooled, minimal pgwire forwarding client** — the gateway opens (once, then pools per remote leader within the sticky connection) an authenticated pgwire connection to that SQL port (the leader's own `StartupMessage`/auth handshake), sends exactly that one `Query`, reads until `ReadyForQuery`, relays the response frames back to the client, and reuses the connection for later statements to the same leader. On error, the bounded one-hop re-resolve+retry of Decision 3 applies. This forwarding client is built on the existing `pgwire` frame read/write primitives (no new dependency); only the dial+timeout helper is reused from today's proxy path, **not** `proxy()` itself.

This rewrite also **removes** the production 50 ms no-leader busy-sleep at `route.rs:92` (the `None` arm under `NO_LEADER_WAIT`): the gateway resolves the leader from the metrics watch and bounds by the deadline without polling. (`route.rs` is production code, so the CLAUDE.md *test* no-sleep rule does not bind it — but the rewrite eliminates the sleep regardless.)

**`RangeRouter` constructor refactor (prerequisite):** `RangeRouter::connect` (`range/router.rs:44`) is hard-wired to `&MultiRangeCluster` and resolves engines via `c.leader_engine(r)` / catalog via `c.catalog_kv()` — methods that exist **only** on the in-process harness type (`cluster.rs:73-76`), not on the durable `ServerNode` substrate. `RangeRouter` must gain a **cluster-agnostic constructor** (e.g. a `HashMap<RangeId, SqlEngine>` of local-leader engines + a `catalog_kv` handle + a remote-forward closure) before its dispatch/`Pin` logic can be reused by the gateway. On a range-0 **follower** gateway there is no local range-0 leader, so `catalog_kv` cannot be a local in-process handle — the catalog is read over the wire from range 0's current leader (the SP12 linearizable read, no new RPC). `Pin` state and the cross-range `0A000` rejection carry forward verbatim; cross-range extended protocol is rejected. The listener signature and client-facing bytes are unchanged.

### 4. os-740 resolution (`crates/executor/tests/`, `CLAUDE.md`)

os error 740 (`ERROR_ELEVATION_REQUIRED`) is Windows' **UAC installer-detection** behaviour: it refuses to launch any executable whose **filename** contains `setup`/`install`/`update`/`patch`/`upgrad` (matches `upgrade`) without elevation. The `update_delete` test could not spawn purely because its binary name contains "update" — an environmental UAC behaviour, **triggered by the filename and fixable by renaming**, not a code defect. Rename `crates/executor/tests/update_delete.rs` → `crates/executor/tests/mutation_semantics.rs` (UPDATE/DELETE are data mutations; meaning preserved; binary name becomes UAC-safe). Add a `CLAUDE.md` policy: `[[test]]`/`[[bin]]`/`[[example]]` target names — and integration-test filenames, which *become* binary targets — must not contain those substrings, with the SP14 audit confirming every current target is UAC-safe and that any **new** multi-process test binary must follow the rule. This unblocks the faithful multi-process e2e on Windows.

## Data flow (remote-leader INSERT example)

1. `INSERT INTO users …` arrives at Node 1 (the gateway) over pgwire; the client connected to Node 1 because it can connect to *any* node.
2. The gateway resolves `users` → `(table_id, schema)` from **range 0's current leader** (a linearizable catalog read — local if Node 1 leads range 0, else over the wire), and `RangeMap::range_for_table` → `RangeId(1)`.
3. The gateway reads range-1 metrics: range 1's leader is **Node 2** (not local).
4. The gateway forwards the single `Query` over its pooled pgwire client connection to Node 2's SQL port; Node 2's local range-1 leader engine writes rows via range 1's `RaftCommitter` (range 1's Raft group, replicated to all nodes), commits via range 1's clog.
5. The response frames are relayed back to the client. If Node 2 had just lost range-1 leadership, the forward returns `NotLeader`; the gateway re-reads range-1 metrics once and retries against the new leader.

## Tasks (legend)

- **T1** Range-aware node transport (`NodeRequest::Raft` gains `range`; server `(range,node)` registry dispatch).
- **T2** Multi-range durable `ServerNode` (per-range keyspaces; N Raft instances; bootstrap each group).
- **T3** Gateway local routing (per-statement range demux; `RangeRouter` cluster-agnostic constructor; local execution; `0A000`).
- **T4** Remote forward (pooled pgwire forwarding client; one-hop bounded retry; cross-process catalog read).
- **T5** os-740 (rename `update_delete`→`mutation_semantics`; `CLAUDE.md` policy + audit).
- **T6** Multi-process e2e (multi-range harness; UAC-safe binary name; routing + per-range failover).
- **T7** Gauntlet + traceability + finish.

## Success criteria

| # | Criterion | Task / verified by |
|---|---|---|
| 1 | A range-unaware `NodeRequest::Raft` payload decodes (serde default → range 0) and a range-tagged envelope round-trips carrying its `range`. | **T1** serde round-trip test |
| 2 | A node hosting ranges {0,1} dispatches an `AppendEntries` tagged range 1 to its range-1 Raft (assert via that group's commit index, openraft `wait()`); an RPC for an unregistered range returns `Unreachable`. | **T1** two-group loopback test |
| 3 | A multi-range `ServerNode` brings up every range and each range elects a leader independently; a write to range 1 appears under `data-r1`/`raft-r1` and **not** under `data-r0`/`raft-r0` (storage isolation, both keyspaces). | **T2** election + isolation test |
| 4 | All SP9/SP10 single-range multi-process tests pass unchanged under the default `RangeMap::single()`. | **T2** existing transport/route/bank/partition/failover suites |
| 5 | CREATE (range 0) + INSERT (data range) + SELECT read-back succeed through a single multi-range node; a cross-range transaction returns SQLSTATE `0A000`. | **T3** local-gateway test |
| 6 | A write issued at a gateway that is a **follower** for the target range is forwarded to the remote leader and becomes visible on all that range's replicas (event-based replication wait); a deterministically-injected single `NotLeader` triggers exactly one re-resolve+retry (asserted via a retry-count observable, not timing). | **T4** remote-forward + retry test |
| 7 | Across the real process boundary: rows land only in their table's range and read back through **any** node; killing one range's leader keeps the **other** range serving while the killed range re-elects and resumes. | **T6** multi-process e2e |
| 8 | A cross-range transaction through the gateway is rejected with SQLSTATE `0A000` end-to-end. | **T3** negative test |
| 9 | No test/`[[bin]]`/`[[example]]` **target name** contains a UAC trigger substring; `cargo test -p executor --test mutation_semantics` builds/runs; `CLAUDE.md` records the rule + audit. | **T5** name-grep check (see Test plan 8) |
| 10 | No new shipped dependency; `#![forbid(unsafe_code)]`; full gauntlet green; a complete success-criteria traceability table. | **T7** gauntlet |

## Test plan

**Sleep policy.** The in-crate layers (T1–T4) are fully sleep-free — every wait is an openraft `wait().metrics(...)` event or a bounded condition on observed state. The multi-process e2e (T6) reuses the SP9 harness, which observes remote nodes only through the **node-global** control protocol; where it must poll (no cross-process push signal exists — the same situation as the existing multi-process suites and the deferred jepsen_elle Scenario B failover poll) it polls a **real condition** (leader present, a committed row readable) with a **bounded deadline**, never a guessed duration, and the nemesis fires only **after** the round's write is observed committed in the target range. (Per-range applied-index push signalling would need a harness-only `GetRangeStatus(RangeId)` control RPC or a server-side blocking wait — out of scope this slice; criterion 7 therefore gates on **SQL-observable** per-range progress, a committed read-back through the specific range, which needs no protocol change.)

1. **Serde compatibility (T1)** — a range-unaware `NodeRequest::Raft` JSON decodes to range 0; a range-1 envelope round-trips. Pure serde, no I/O.
2. **Range-aware dispatch (T1)** — loopback two-group test: a node hosting {0,1} routes a range-1 `AppendEntries` to its range-1 Raft (assert via that group's commit index, `wait()`); an unregistered range yields `Unreachable`.
3. **Multi-range election + storage isolation (T2)** — single process, one node, 2-range map; each range self-confirms a leader via `raft.wait().metrics(state==Leader && current_leader==self)`; write to range 1, assert the row is present in `data-r1` and absent from `data-r0`.
4. **Single-range regression (T2)** — `single_node_serves_sql_after_election` and the SP9/SP10 multi-process bank/partition/failover suites pass unchanged under `RangeMap::single()`.
5. **Local-only gateway routing (T3)** — one multi-range node: CREATE (range 0) + INSERT (range 1 local leader) + SELECT read-back; a cross-range txn returns `0A000`. Paced by leader `wait()`.
6. **Remote one-hop forward (T4)** — two nodes, gateway is a follower for the target range; a write at the gateway forwards to the remote leader and is visible on all range replicas (event-based applied-index wait, the `wait_for_replication` analog). A **test-only one-shot** makes the first forward observe `NotLeader` exactly once (e.g. first target is a known follower), and the test asserts a **retry counter == 1** (mechanically checkable, not racing a real election).
7. **Multi-process routing + per-range failover (T6)** — 3 processes each hosting all ranges, UAC-safe binary name (e.g. `multirange_gateway`); client connects to an arbitrary node; rows land only in their range and read back through any node; killing one range's leader keeps the other range serving while the killed range re-elects and resumes. The crash nemesis fires only **after** the round's write is observed committed in the target range (SQL read-back), via a bounded condition with a deadline.
8. **os-740 naming (T5)** — rename `update_delete.rs` → `mutation_semantics.rs`; `cargo test -p executor --test mutation_semantics` builds/runs; the check is a **filename/target-name** grep, not a content grep: `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty, plus a scan of every crate's `[[test]]/[[bin]]/[[example]] name = "…"` entries and the 4 fuzz `[[bin]]` names (`parse_sql`, `wire_decode`, `decode_row`, `decode_key`) and the `crabgresql` main binary. (Today exactly one filename matches — `update_delete.rs`; a *content* grep matches 15 files with legitimate SQL `UPDATE`, so the check must target names. The integration-test binaries this clears: cluster `{durable_scenarios, jepsen_bank, model, multirange, scenarios, sql_durable, sql_over_raft}`; crabgresql `{jepsen_elle, multiprocess}`; executor `{concurrency, durability, end_to_end, linearizable_reads, recovery, transactions, mutation_semantics}`; pgparser `{libpg_query_oracle}`; pgwire `{cancel, extended_query, golden_trace, scram_auth, simple_query, sqlx_driver, tls}` — 23 today, 24 after T6.)
9. **Gauntlet (T7)** — `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace` + fuzz-smoke; `cargo deny`; `check-no-native`; success-criteria traceability.

## Non-goals (explicit)

- **Cross-range transactions / 2PC** — rejected with `0A000`; later.
- **Cross-range extended protocol (Parse/Bind/Execute) across ranges** and **prepared-statements-across-ranges** — rejected with `0A000`; simple-query routing is the surface; D3b.
- **Scatter-gather / multi-table SELECT** — no joins, single-table `FROM`; D3b.
- **Structured `QueryRequest`/`QueryResponse` RPC** — pgwire forwarding only; D3b.
- **Replicated meta range / dynamic range descriptors** — RangeMap stays static config; D3b.
- **Per-range control requests** — control stays node-global.
- **Gateway-crash-aborts-pinned-transaction behaviour** and **range-0-no-leader catalog path** — stated as bounded behaviour in Internal decisions but **not tested this slice** (no half-applied-state / no-indefinite-hang assertions added); revisited when distributed transactions land.
- **Range splits / rebalancing / data sharding across nodes (non-co-located)** — D4 and beyond.

## Risks (and mitigations)

- **Per-range storage isolation** is the highest-risk new construction — colliding per-range log/SM state silently corrupts ranges. Mitigated by **per-range keyspaces** (`data-r{r}`/`raft-r{r}`, structural fjall isolation spanning both the data and raft keyspaces) **and** the dedicated criterion-3 assertion, not assumed.
- **Committer-can't-forward**: building a remote leader engine on the gateway compiles but fails at runtime with `NotLeader`. The spec locks SQL-boundary forwarding.
- **Per-statement pgwire forwarding is not a byte relay**: it needs a real (minimal) pgwire client with a startup handshake per remote target and connection pooling; mis-modelling it as `copy_bidirectional` would corrupt the protocol. T4 builds it on the `pgwire` frame primitives.
- **Leader-resolution races**: the metrics watch `Ref` must drop before the `await`; retries are bounded; a paused leader still self-reports `Leader` and is excluded (SP13 `is_paused`).
- **Catalog funnels through range 0's leader**: a slow/hung range-0 leader degrades all data-range statements with no stale fallback — acceptable for D3a-net, bounded by the gateway retry deadline; the cross-process catalog read reuses the SP12 linearizable read (no new RPC).
- **Single-range regression surface is large** (all SP9/SP10 tests use the per-connection fast-path): `RangeMap::single()` must truly preserve it. Criterion 4 is the load-bearing gate.
- **Scope creep toward D3b** (structured RPC, prepared statements, meta range, per-range control): fenced in Non-goals; T2/T4 must not balloon.
- **os-740 is silent until a Windows spawn fails**: a future trigger-substring binary won't fail on CI Linux; the `CLAUDE.md` hard rule + the T5/T6 name-grep are the only guard. The harness path `env!(CARGO_BIN_EXE_crabgresql)` stays safe only while the binary is named `crabgresql`.

## Traceability (implemented)

| # | Criterion | Verified by |
|---|---|---|
| 1 | range-unaware `NodeRequest::Raft` decodes (serde default → 0); range-tagged round-trips | `cluster::transport::server::range_aware::raft_envelope_range_serde_default_and_round_trip` (T1) |
| 2 | range-1 `AppendEntries` dispatched to range-1 Raft; unregistered range → `Unreachable` | `cluster::transport::server::range_aware::loopback_dispatches_by_range_and_rejects_unregistered` (T1) |
| 3 | multi-range `ServerNode` elects per range; range-1 write isolated to `data-r1`/`raft-r1`, not range 0 | `cluster::server_node::tests::{multi_range_node_elects_a_leader_per_range, a_write_to_range1_is_isolated_to_data_r1}` (T2) |
| 4 | SP9/SP10 single-range suites pass under default `RangeMap::single()` | `cluster::{scenarios,sql_over_raft,durable_scenarios,sql_durable}` + `crabgresql::{multiprocess,jepsen_elle}` green (T2 gate; full `cargo test --workspace`) |
| 5 | CREATE+INSERT+SELECT through a multi-range node; cross-range txn → `0A000` | `crates/cluster/tests/gateway_local.rs` (T3) |
| 6 | follower-gateway write forwards to remote leader + visible on replicas; one re-resolve+retry (counter == 1) | `crates/cluster/tests/remote_forward.rs` (T4) |
| 7 | multi-process: rows route by range + read back through any node; per-range failover independence | `crates/crabgresql/tests/multirange_gateway.rs::d3a_net_routes_by_range_and_survives_per_range_failover` (T6) |
| 8 | cross-range transaction rejected `0A000` end-to-end | `crates/cluster/tests/gateway_local.rs` (cross-range-transaction case, T3) |
| 9 | no UAC-trigger target names; `mutation_semantics` builds/runs; `CLAUDE.md` policy + audit | `cargo test -p executor --test mutation_semantics` + the filename grep + `CLAUDE.md` (T5) |
| 10 | no new shipped dependency; `#![forbid(unsafe_code)]`; full gauntlet green | `cargo deny` ok + `cargo fmt --all --check` + `cargo clippy --workspace --all-targets -- -D warnings` + `cargo test --workspace` (0 failures) (T7) |

**Note (deviations from the plan, recorded):** T2's election + storage-isolation tests landed inline in `server_node.rs` (`mod tests`), not a separate `tests/durable_multirange.rs` file. T6's two e2e tests were merged into a single test (`d3a_net_routes_by_range_and_survives_per_range_failover`) so libtest never runs two 6-Raft clusters concurrently on a 2-core runner. The `route_one` gateway needed a **leadership-aware** routing decision (`RangeRouter` gained a `LeadsRange` seam) so a follower-with-a-local-engine forwards instead of running locally — a T3/T4 gap surfaced by T6's co-located multi-process topology.
