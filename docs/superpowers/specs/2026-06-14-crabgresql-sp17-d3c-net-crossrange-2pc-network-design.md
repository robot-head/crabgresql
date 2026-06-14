# SP17 / D3c-net — Cross-range 2PC over the network (minimal mechanism)

**Predecessors:** SP1–SP12 (single-node SQL/MVCC, single-range Raft, durable storage, real TCP transport, SQL leader routing, over-the-wire serializability, linearizable reads), SP13 (in-process multi-range core), SP14 (network range routing), SP15 (replicated range descriptors), **SP16 (cross-range distributed transactions — in-process 2PC core)** — all merged or in review.

**Goal:** Lift SP16's in-process two-phase commit onto the **real network**. A cross-range `BEGIN…COMMIT` issued at **any** gateway commits atomically across nodes — even when the gateway leads **neither** range 0 **nor** the participant ranges. The gateway *coordinates*: it makes RPCs to range 0's leader (allocate the global xid, write the single global decision) and to each participant range's leader (stage writes, prepare, commit/abort) over a new **structured node-transport RPC**; each participant leader holds a **per-G server-side session**. This is the network analog of SP16, executed as the project's third in-process→network move (after SP7→SP9 and SP13→SP14). **Minimal cut:** the mechanism + the leader-stable happy path + the `gateway_local` `0A000` flip + a multi-process commit e2e. The crash/partition-nemesis `jepsen_bank` proof, the durable transaction record + active recovery sweep, and full failover-survival are the explicit follow-up (**SP18**).

**Architecture:** Five pieces. (1) A **range-0 GTM network service** — a new `NodeRequest::Txn` family carrying `BeginGlobal` (range 0's leader allocates `G`) and `CommitGlobal { g, decision }` (range 0's leader's existing `commit_global_decision` Raft append). (2) A **participant RPC + per-G held-session registry** — `Stage`/`Prepare`/`Commit`/`Abort` RPCs to a participant range's leader, which holds a per-`G` `SqlSession` in a server-side registry keyed by `G` (detached from the TCP connection so a later `Commit(G)` finds it), reusing SP16's `SqlSession` participant API verbatim. (3) The **gateway coordinator** — `RangeRouter`'s `Pin::Global` path drives **remote** participants by RPC when it does not lead a range, with NotLeader-retry. (4) The **load-bearing correctness piece** — **linearizable global-clog visibility**: a participant resolving a foreign `Prepared(→G)` gates its global-clog reads by a **range-0 ReadIndex** once per statement (reusing SP12), so two nodes' range-0 replicas can never diverge into partial visibility. (5) **Basic liveness** — a node losing range-r leadership drops its held per-`G` sessions for range r (always safe — the global clog is the sole arbiter).

**Tech stack:** Rust 2024, existing `cluster`/`executor`/`mvcc` crates, openraft, the SP9 node transport (`NodeRequest`/`frame`), the SP14 `ForwardPool` leader-resolution + retry, the SP12 `RaftLinearizer`. **No new shipped dependency.** `#![forbid(unsafe_code)]` unchanged. Tests: in-crate `ServerNode` clusters (`gateway_local`) + the SP9 multi-process harness for the cross-node e2e (UAC-safe binary name).

---

## Where this sits: the roadmap

| Slice | Scope |
|---|---|
| SP16 (D3c) | Cross-range distributed transactions — **in-process** 2PC core. ✅ in review (PR #32) |
| **SP17 (D3c-net) — this spec** | **2PC over the network — minimal mechanism:** structured prepare/commit/abort RPC, per-G participant session registry, range-0 GTM network service, linearizable global-clog visibility, the gateway coordinator. Leader-stable happy path + `gateway_local` flip + multi-process commit e2e. |
| SP18 (D3c-net-hard) | Durable txn record + active recovery sweep; full failover-survival (re-stage on a mid-txn leader move; participant self-heal + lingering-lock timeout after a coordinator crash); the cross-range `jepsen_bank` conservation proof under a crash/partition nemesis. |
| later | Full cross-range serializability (SSI); dynamic placement; range splits (D4). |

Everything below SP17 in that table is **out of scope** for this slice.

## The load-bearing constraints (why the design is shaped this way)

1. **SP16's 2PC is four synchronous in-memory calls on co-located leader engines.** The coordinator (`RangeRouter`) (a) allocates `G` via `self.engines[&0].begin_global()`, (b) stages writes by running statements on each participant's **local** `SqlSession`, (c) writes the decision via `self.engines[&0].commit_global_decision(g, …)`, and (d) releases via `session.commit_release()/abort_release()` (`router.rs:313,315,374,377-385`). All four assume the coordinator holds a **local leader engine** for range 0 **and** every participant range. The locality crutch is explicit: `MultiRangeCluster::leader_engine` copies the **one** `Arc<Gtm>` into every range engine via `share_gtm_to` (`cluster.rs:213-217`). On a real `ServerNode` this never happens — `build_range_group` calls `SqlEngine::replicated` and never `init_gtm_coordinator`/`share_gtm_to`, so `gtm` is `None` everywhere and `can_escalate` (`router.rs:406-408`, `= engines[&0].is_some_and(has_gtm)`) is false → cross-range is rejected with `0A000`. **SP17 must turn (a)–(d) into remote calls.**

2. **The pgwire SQL-text forward cannot carry 2PC.** `ForwardPool::forward` (`forward.rs`) dials the remote leader's pgwire port and re-sends one statement as a bare `Query` that runs as a fresh **autocommit** statement — one `Q`, read to `ReadyForQuery`, no `BEGIN`/held-txn/global-xid state. It **cannot** stage a write-set into a held remote transaction, express a 2-phase vote, or send a release. **SP17 needs a structured RPC** (a new `NodeRequest::Txn` variant + `server.rs` dispatch + a client send path) — the node transport's first non-Raft, non-Control structured request/response.

3. **The GTM is one in-process `Arc<Gtm>` at range 0; over the network there is exactly one authority — range 0's leader.** `begin_global`, the in-memory running-set, the durable `next_global_xid` counter, and the global snapshot (`gtm.rs`) cannot be answered by a follower or another node's range-0 replica. `share_gtm_to` does not cross processes. So `G` allocation and the decision write **must** go to range 0's **leader**.

4. **Reading the local range-0 replica for visibility is incorrect over the network.** The `global_status` resolver (`exec.rs:329-345`) reads `global` = the engine's `catalog_kv` = the **local** range-0 replica (`server_node.rs:135`, `store.data_kv(0)`). Two participant nodes' range-0 replicas can apply `Committed(G)` at different times → range 1's reader sees `G` committed while range 2's reader does not → **partial visibility**, which breaks the single-global-clog atomicity guarantee. The global-clog read must be made **linearizable** against range 0's leader. **A reviewer or implementer who leaves the resolver reading the bare local replica will pass single-node tests and silently break cross-node atomicity** — the spec locks the per-statement range-0 ReadIndex gate.

5. **A held participant session must outlive its TCP connection.** SP16's participant `SqlSession` holds its xid + row locks across statements until `commit_release` — driven by a `&mut` from the coordinator's task. Over the network the participant session lives on the **participant's leader node** and must survive the coordinator's prepare→commit round-trip, addressable from a (possibly reconnected) coordinator. Nothing today detaches a txn from its connection's task. **SP17 adds a per-G server-side session registry** on each participant leader. The `SqlSession` participant API itself (`ensure_began`/`join_global`/`commit_release`/`abort_release`, `session.rs:606-646`) is reused **unchanged** — it was already written to be coordinator-driven.

## Decisions (locked during brainstorming)

1. **Scope = minimal mechanism + commit e2e.** The cross-node 2PC mechanism + the leader-stable happy path + the `gateway_local` `0A000` flip + a multi-process commit e2e. **Basic liveness** only (release-held-sessions-on-leadership-loss). The crash/partition-nemesis `jepsen_bank`, the durable txn record + recovery sweep, and full failover-survival → **SP18**.
2. **Coordinator = the gateway** that received the client txn. It owns the `Pin::Global` participant set (in-memory) and makes RPCs outward: `BeginGlobal`/`CommitGlobal` to range 0's leader, `Stage`/`Prepare`/`Commit`/`Abort` to each participant's leader. Matches the current `RangeRouter` shape (it already owns the Pin state machine + the forward seam). Not a range-0-leader-arbiter model.
3. **Participant model = stateful held session per G.** Each participant leader keeps a server-side `G → SqlSession` registry; the coordinator sends control RPCs keyed by `G`. Not coordinator-ships-the-write-set.
4. **Visibility = linearizable global-clog read, gated once per statement by a range-0 ReadIndex** (SP12 reuse), not a per-tuple forward.

**Internal decisions (locked, with resolution):**

- **The structured RPC rides the existing node transport.** Add `NodeRequest::Txn(TxnRpc)` + `NodeResponse::Txn(TxnResp)` to `transport/protocol.rs`, a `server.rs` dispatch arm that resolves the target `(range, node)` group via the existing `RangeRegistry`, and a client send path (a `TcpConn` method or a small dedicated pooled client mirroring `ForwardPool`'s per-leader pooling). `TxnRpc` = `BeginGlobal | CommitGlobal { g, decision } | Stage { g, range, sql } | Prepare { g } | Commit { g } | Abort { g }`. (Allocation/decision RPCs target range 0; stage/prepare/commit/abort target the participant range.)
- **Concurrency:** 2PC RPCs use their **own** pooled connections per target node (mirroring `ForwardPool`), **not** the openraft `RaftNetwork` conns (which serialize one in-flight RPC per `TcpConn`), so a coordinator can fan `Stage`/`Prepare` to N participants in parallel.
- **The range-0 GTM service.** `BeginGlobal` → on range 0's leader, `engine.begin_global()` (the in-memory counter bump) → returns `G`. `CommitGlobal { g, decision }` → range 0's leader's `engine.commit_global_decision(g, decision)` (the existing single Raft append — the atomic instant). Both reach range 0's leader via metrics-watch leader resolution + a bounded NotLeader/Unavailable retry (reuse `ForwardPool`'s `resolve_leader`/`RetryCounter`). On a node that **is** range 0's leader the gateway calls the local engine directly (no self-RPC).
- **The global snapshot is reconstructed from range 0's durable clog state, not the in-memory running-set.** The resolver's in-doubt gate (`g` running as of my global snapshot) is answered by range 0's **durable** state: `global_xmax = next_global_xid`, and a `g` is in-flight iff its global clog entry is absent (no `Committed`/`Aborted`) and `g < global_xmax`. This makes the global snapshot **failover-safe** (any node / a re-elected range-0 leader reconstructs it from durable state) and makes `finish_global` a **no-op** (the in-memory running-set is no longer load-bearing for visibility). The participant captures this global snapshot as part of its read context, gated by the range-0 ReadIndex (below).
- **Linearizable global-clog visibility (the correctness core).** Each engine that may resolve a foreign global xid (every data-range engine on a `ServerNode`) gains a **range-0 linearizer** (a `RaftLinearizer` over the node's range-0 Raft handle, alongside the engine's own-range linearizer). Before a statement's reads resolve any global xid, the session ensures the range-0 read point is linearizable (`ensure_readable` on the range-0 linearizer — one ReadIndex per statement, mirroring `session.rs read_context`), then the per-tuple `global_status` reads are sync against the now-caught-up **local** range-0 replica. Two participant nodes that each gate past the `Committed(G)` append both see it → no partial visibility. A reader gated **before** the commit sees `G` absent → invisible (correct SI). If the node is **not** a range-0 replica at all (not the co-located topology), the read forwards to range 0's leader — but in D3a-net's co-located placement every node hosts a range-0 replica, so the local-gated read is the path.
- **The gateway coordinator.** `can_escalate` becomes "the cluster has a reachable range-0 leader" (always true over the network) instead of "this engine has a local GTM." On escalation the coordinator allocates `G` via `BeginGlobal`. For a participant range it **leads locally**, it uses today's local `SqlSession` path; for a **remote** range it sends `Stage { g, range, sql }` RPCs (the held remote session) instead of the SQL-text forward. `COMMIT` sends `CommitGlobal` to range 0's leader (the atomic instant), then `Commit`/`Abort` to every participant (local → `commit_release`; remote → `Commit { g }` RPC). `ROLLBACK` → `Abort`. A NotLeader on any participant RPC during the leader-stable cut surfaces a **retryable** error to the client (the whole txn retries); full mid-txn re-stage is SP18.
- **Per-G held-session registry on participant leaders.** A process-local `Mutex<HashMap<u64, SqlSession>>` (keyed by `G`) on the node, populated by `Stage` (first stage creates the held session via the range's engine `.connect()` + `ensure_began` + `join_global(g)`), consulted by `Prepare`/`Commit`/`Abort`, removed on `Commit`/`Abort`. Detached from the TCP connection: the `Stage` RPC parks the session under `G`; a later `Commit(G)` from any connection finds it.
- **Basic liveness (release-on-leadership-loss).** A node losing range-r leadership drops every held per-G session whose range is r (call `abort_release` → free locks). Always **safe**: post-`Committed(G)` the durable `Prepared(Li→g)` rows stay visible via the resolver (the dropped locks were only for concurrency control); pre-decision they are invisible (presumed abort). Wired off the per-range metrics watch (the same `reseed_on_leadership`/`NodeLeadership` machinery). **Deferred to SP18:** a *non-failing* participant whose coordinator crashed mid-protocol keeps its held session (locks) until a timeout — a **liveness** gap, never a safety gap.
- **Per-range leadership in the control protocol.** Add a `GetRangeLeaders` (or extend `GetStatus`) so the multi-process e2e can deterministically pick a gateway that does **not** lead all participant ranges, and so a future nemesis can target a specific range's leader. Test-support; node-global `current_leader` is insufficient.
- **`0A000` is retained for genuinely-unsupported cases** (cross-range *extended* protocol; a participant range whose leader is unreachable after retries), not removed.

## Components

### 1. `NodeRequest::Txn` transport + the range-0 GTM service (`transport/{protocol,server,client}.rs`, `forward.rs` or a new `txn_client.rs`, `server_node.rs`)

Add the `Txn` request/response variants and the server dispatch arm (resolve `(range, node)` via `RangeRegistry`; range-0 RPCs reach the range-0 group, participant RPCs the participant group). `BeginGlobal`/`CommitGlobal` call the local range-0 engine's `begin_global`/`commit_global_decision`. A pooled 2PC client (per-target-node connections, parallel-capable) sends the RPCs with bounded NotLeader-retry.

### 2. Participant RPC + per-G held-session registry (`server_node.rs`, a new `participant.rs` or in `route.rs`)

The server-side `G → SqlSession` registry on each node; `Stage`/`Prepare`/`Commit`/`Abort` handlers driving the existing `SqlSession` participant API. Reuses `ensure_began`/`join_global`/`commit_release`/`abort_release` unchanged.

### 3. The gateway coordinator (`range/router.rs`)

`Pin::Global` extended to drive remote participants: network `can_escalate`, `BeginGlobal` on escalation, `Stage` RPCs for remote ranges, `CommitGlobal` + per-participant `Commit`/`Abort`. NotLeader → retryable client error.

### 4. Linearizable global-clog visibility (`executor/{lib,session,exec}.rs`, `server_node.rs`)

A range-0 linearizer threaded into each data-range engine; the per-statement range-0 ReadIndex gate before global-xid resolution; the global snapshot reconstructed from range 0's durable clog state (`finish_global` → no-op for visibility).

### 5. Liveness + test support (`server_node.rs`, `transport/protocol.rs`, harness)

Release-held-sessions-on-leadership-loss; per-range leadership in the control protocol.

## Data flow (a cross-range transfer at a non-leading gateway)

Tables `acct_x` (range 1) and `acct_y` (range 2); the client connects to **node N3**, which leads **neither** range 0 (led by N1) nor range 1 (N2) nor range 2 (N3 leads range 2, say).

1. `BEGIN; UPDATE acct_x …` at N3's gateway. `acct_x` → range 1, led by **N2** (remote). N3 escalates only on the second range, so first it pins range 1.
2. `UPDATE acct_y …` → range 2, led by N3 (local) ≠ range 1. **Escalate:** N3 sends `BeginGlobal` → **N1** (range 0's leader) → `G`. N3 sends `Stage { G, range 1, "UPDATE acct_x …" }` → **N2**, which opens a held `SqlSession` under `G`, runs the UPDATE (held, `Prepared(L1→G)`); N3 stages range 2 on its **local** held session (`Prepared(L2→G)`).
3. `COMMIT` → N3 sends `CommitGlobal { G, Committed }` → **N1** (range 0's leader): one Raft append writes `Committed(G)` — **the atomic instant**. Then N3 sends `Commit { G }` → N2 (release) and `commit_release` locally for range 2.
4. A reader on **any** node resolving an `acct_x`/`acct_y` row's `Prepared(→G)` gates a range-0 ReadIndex, reads `Committed(G)` from its caught-up range-0 replica → the row is visible on **both** ranges. Before step 3's append (or before the reader gates past it) → invisible on **both**. No reader ever sees the transfer half-applied; if step 2 had failed, `CommitGlobal{Aborted}` (or no append) → presumed abort, neither row visible.

## Tasks (legend)

- **T1** `NodeRequest::Txn` transport (variant + server dispatch + pooled 2PC client) + the range-0 GTM service (`BeginGlobal`/`CommitGlobal`).
- **T2** Participant RPC + per-G held-session registry (`Stage`/`Prepare`/`Commit`/`Abort`).
- **T3** The gateway coordinator (network `can_escalate`, remote staging/commit, NotLeader-retry).
- **T4** Linearizable global-clog visibility (range-0 read-gate + durable-state global snapshot + `finish_global` no-op).
- **T5** Release-on-leadership-loss + per-range leadership in the control protocol.
- **T6** `gateway_local` `0A000`→atomic-commit flip + multi-process cross-node commit e2e (UAC-safe binary).
- **T7** Gauntlet + traceability + finish.

## Success criteria

| # | Criterion | Task / verified by |
|---|---|---|
| 1 | A `NodeRequest::Txn` round-trips (serde + loopback dispatch); `BeginGlobal` to range 0's leader returns a global xid `≥ GLOBAL_XID_BASE`; `CommitGlobal` durably writes the decision to range 0's group. | **T1** transport + GTM-service tests |
| 2 | A `Stage`/`Prepare`/`Commit`/`Abort` sequence to a participant leader opens, holds, and releases a per-G `SqlSession` (xid + locks held across `Stage`→`Commit`, freed on `Commit`/`Abort`); the held session survives a different connection issuing the `Commit`. | **T2** participant-registry test |
| 3 | A cross-range `BEGIN…COMMIT` issued at a gateway that leads **neither** range 0 **nor** a participant range commits **atomically** — both rows visible through **any** node; a `ROLLBACK` variant leaves neither. | **T3/T6** cross-node atomic-commit test |
| 4 | Two participant nodes never diverge into partial visibility: a reader on either, once gated past the `Committed(G)` range-0 append, sees the row; before, neither does. (The linearizable-read gate.) | **T4** visibility test |
| 5 | `gateway_local`'s former cross-range `0A000` rejection now asserts a successful atomic cross-range commit on the `ServerNode`/`serve_range_routed` path. | **T6** gateway_local flip |
| 6 | A node losing range-r leadership releases its held per-G sessions for range r (locks freed); a concurrent writer that was blocked proceeds. Atomicity is unaffected (the global clog remains the arbiter). | **T5** liveness test |
| 7 | All single-range and SP16 in-process cross-range suites pass unchanged; no new shipped dependency; `#![forbid(unsafe_code)]`; full gauntlet green; traceability table. | **T7** regression gate + gauntlet |
| 8 | Across the real process boundary: a cross-range transfer at an arbitrary gateway commits and reads back through any node; per-range failover keeps other ranges serving. | **T6** multi-process e2e |

## Test plan

**Sleep policy.** In-crate layers (T1–T5) are sleep-free — every wait is an openraft `wait().metrics(...)` event or a bounded condition. The multi-process e2e (T6) reuses the SP9/SP14 harness's **bounded poll cadence** (a small interval + deadline, never a settle guess); the cross-node in-doubt window is observed via an SQL/control-observable signal, not a sleep. First contact with a freshly-spawned node uses the bounded-retry connect (the SP15 `Cluster::pg` lesson).

1. **Txn RPC serde + dispatch (T1)** — a `NodeRequest::Txn` round-trips; a loopback two-group cluster dispatches `BeginGlobal` to range 0's group and returns `G`; `CommitGlobal` appends `Committed(G)` (assert via the group's applied clog, openraft `wait()`).
2. **Participant registry (T2)** — `Stage`→`Prepare`→`Commit` on a participant leader holds then frees the per-G session; a `Commit(G)` over a *second* connection still resolves the parked session; an `Abort(G)` releases without a clog write.
3. **Cross-node atomic commit (T3)** — in-crate `ServerNode` 3-node cluster; a cross-range `BEGIN…COMMIT` issued at a node that leads neither range 0 nor a participant range → both rows read back through every node; a `ROLLBACK` variant → neither. Paced by leader `wait()`.
4. **Linearizable visibility (T4)** — a cross-range commit; a reader on each participant node gated past the decision sees both rows; a reader gated before sees neither — no node disagrees. (Reconstruct the in-doubt window deterministically via the staged-but-undecided state.)
5. **`gateway_local` flip (T6)** — replace the `0A000` cross-range-rejection assertion with a successful atomic cross-range commit on the `serve_range_routed` path; update the module docstrings.
6. **Liveness (T5)** — a participant holding a per-G session loses range-r leadership (kill/step-down) → its session is released, a blocked writer proceeds; the committed/aborted outcome is still correct via the global clog.
7. **Multi-process e2e (T6)** — 3 processes; a cross-range transfer issued at an arbitrary gateway (chosen via the new per-range leadership query so it doesn't lead all participants) commits and reads back through any node; per-range failover keeps other ranges serving. UAC-safe binary name (e.g. `crossrange_2pc_net`).
8. **Gauntlet (T7)** — `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo nextest run --workspace` + `cargo test --workspace --doc` + fuzz-smoke; `cargo deny`; `check-no-native`; traceability.

## Non-goals (explicit → SP18 unless noted)

- **The crash/partition-nemesis cross-range `jepsen_bank` conservation proof** in the multi-process setting — SP18 (needs the durable record + recovery).
- **The durable transaction record (`/0/txn/<G>`) + active recovery sweep** — SP18. The minimal cut keeps the participant set in-memory (`Pin::Global`); a coordinator-node crash is a **liveness** gap (lingering held locks on a *non-failing* participant), never a safety gap (the global clog stays the arbiter; in-doubt rows are invisible).
- **Full failover-survival** — re-staging a participant on a mid-txn leader move; participant self-heal + a lingering-lock timeout after a coordinator crash. SP18. (SP17: a NotLeader mid-txn → retryable client abort; leader-loss → release held sessions.)
- **GC of settled `Prepared(Li→G)` markers / decided `/0/clog/<G>` records** — later.
- **Cross-range extended protocol (Parse/Bind/Execute)** across ranges — stays `0A000`.
- **SSI / full cross-range serializability** — later. SI, with write-skew documented (as single-range).

## Risks (and mitigations)

- **Linearizable visibility is the correctness core.** Leaving the resolver reading the bare local range-0 replica passes single-node tests and silently breaks cross-node atomicity (partial visibility). Mitigated by the per-statement range-0 ReadIndex gate (criterion 4 asserts no two nodes disagree) and the durable-state global snapshot (failover-safe). This is SP17's analog of SP16's deregister-at-prepare — spend the rigor here.
- **The held-session registry leaks on a coordinator crash (minimal cut).** A non-failing participant keeps locks until SP18's timeout/recovery. Bounded to a **liveness** issue (blocked writers), never a safety one — flagged in Non-goals; the leader-loss release covers the common failover case.
- **Idempotency under NotLeader-retry.** A re-sent `Stage`/`CommitGlobal` must not double-apply. Mitigated by keying everything on `G`: `Stage` is idempotent by the `Prepared(Li→g)` write-key + the per-G registry (a second `Stage` for an existing `G` session is a no-op-or-append), and `CommitGlobal` is the write-once decision record (reuse the SP15 write-once invariant + the clog's decision-is-final property).
- **Range-0 funnel + per-statement ReadIndex latency.** Every cross-range commit and every foreign-xid-resolving read touches range 0's leader. Accepted (the documented single-global-clog cost); the ReadIndex is amortized once per statement, not per tuple.
- **The structured RPC is the node port's first non-Raft/non-Control request.** Mitigated by mirroring the existing `Control` precedent (a structured request/response already dispatched node-globally) and the `RangeRegistry` group resolution.
- **Scope creep toward SP18** (durable record, recovery sweep, nemesis, self-heal): fenced in Non-goals; T1–T6 build only the leader-stable mechanism + release-on-leadership-loss.
