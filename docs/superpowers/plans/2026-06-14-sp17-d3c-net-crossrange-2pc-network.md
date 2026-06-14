# SP17 / D3c-net — Cross-range 2PC over the network (minimal mechanism) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A cross-range `BEGIN…COMMIT` issued at any gateway commits atomically across nodes, even when the gateway leads neither range 0 nor the participant ranges — the gateway coordinates by RPC and each participant leader holds a per-G server-side session.

**Architecture:** Add a structured `NodeRequest::Txn` family to the node transport carrying `BeginGlobal`/`CommitGlobal`/`Stage`/`Release`/`GlobalBarrier`. The range-0 leader is the single GTM authority (durable global-xid allocation + the one global-clog decision append). Each participant leader keeps a process-local `(G, range) → SqlSession` registry, reusing SP16's participant API verbatim. A `GlobalCoordinator` seam lets the `RangeRouter` drive participants locally (in-process `MultiRangeCluster`, unchanged) or by RPC (the networked gateway). Cross-node visibility is made correct by a per-statement range-0 read barrier (follower ReadIndex) plus a `gsnap` reconstructed from durable range-0 state.

**Tech Stack:** Rust 2024; the `cluster`/`executor`/`mvcc` crates; openraft (`ensure_linearizable`, `wait().applied_index_at_least`); the SP9 node transport (`frame`/`protocol`/`server`/`client`); the SP14 `ForwardPool` leader-resolution + retry pattern; the SP12 `Linearizer` seam. No new shipped dependency. `#![forbid(unsafe_code)]` unchanged. Tests under cargo-nextest; doctests via `cargo test --workspace --doc`.

**Branch:** `sp17-d3c-net-crossrange-2pc-network` (already created, stacked on the SP16 branch tip `dc085a6`). Diff against `origin/main`, never local `main`.

---

## Locked design decisions (from brainstorming + the anchor map)

1. **Scope = minimal mechanism + commit e2e.** Leader-stable happy path. A NotLeader mid-txn → retryable abort. Release-held-sessions-on-leadership-loss is the only liveness guard. The crash/partition-nemesis `jepsen_bank`, the durable txn record + active recovery sweep, and full failover-survival (re-stage / self-heal) are **SP18**.
2. **Coordinator = the gateway** (the node that received the txn). It owns the in-memory `Pin::Global` participant set and drives RPCs outward.
3. **Participant = stateful held session per G** on each participant leader, keyed by `(G, range)`.
4. **Visibility = a per-statement range-0 read barrier** (follower ReadIndex) + `gsnap` reconstructed from durable range-0 state.
5. **Global-xid allocation is durable** (`begin_global` persists `next_global` before returning) — prevents global-xid reuse across a range-0 leader change.
6. **`GlobalCoordinator` seam has two impls** — `LocalCoordinator` (in-process `MultiRangeCluster`, preserves SP16 behavior) and `NetCoordinator` (the networked gateway, always RPCs — exercising the wire even on a single node via loopback-to-self).
7. **Inside an explicit txn, any statement on a non-locally-led range is staged as a global participant.** A txn touching only locally-led ranges stays a local single-range txn (today's path). A txn touching any remote range escalates to `Pin::Global` (even with one participant). This is what lets a gateway that leads nothing still hold every participant. Cross-range **reads inside an open txn** (snapshot isolation across ranges) stay out of scope; the e2e reads back via autocommit `SELECT` after `COMMIT`.

---

## Review corrections (adversarial plan review, 2026-06-14 — BINDING)

An adversarial multi-agent review (5 lenses, per-finding verification) confirmed 17 findings. The fixes below are folded into the tasks; where a code block elsewhere in this plan disagrees with one of these, **this section wins**. The two CRITICALs are the load-bearing ones — read them before T5.

**C1 (CRITICAL) — `next_global_xid` is BIG-endian on disk.** `gtm.rs` writes/reads it with `zerocopy::byteorder::big_endian::U64` (`gtm.rs:13/55-60/32`). Any reconstruction MUST decode big-endian; any test vector MUST write `to_be_bytes()`. Native-endian + `to_le_bytes` is self-consistent (the unit test passes green) but mis-decodes the allocator's real bytes → `xmax` clamps to `GLOBAL_XID_BASE` → **every committed cross-range row resolves InProgress (invisible) forever** in the wired path. Fix: add one shared decoder in `gtm.rs` used by both `Gtm::open` and `durable_global_snapshot` so the layout can never drift (see C2).

**C2 (CRITICAL) — visibility `gsnap` is ALWAYS reconstructed from durable range-0 state; the in-memory GTM running set is NEVER used for visibility.** The network commit never prunes `g` from the range-0 leader's in-memory `Gtm.running`, so a range-0-leader read using `gtm.global_snapshot()` would see its own just-committed range-0 row as in-doubt → invisible cluster-wide (gateways forward range-0 reads to that leader). Fix (replaces the T5 Step 3 `global_read_snapshot` and the BEGIN/RR capture):
- `global_read_snapshot(None)` and the RR BEGIN capture both reconstruct from durable range-0 state whenever the engine can see cross-range rows (`self.gtm.is_some() || self.range0_barrier.is_some()`), never `gtm.global_snapshot()`.
- Add a **shared** helper in `gtm.rs` and reuse it in `durable_global_snapshot`:
  ```rust
  // crates/executor/src/gtm.rs — used by Gtm::open AND session::durable_global_snapshot
  pub(crate) fn read_next_global(kv: &dyn Kv) -> Result<u64, ExecError> {
      use mvcc::xid::GLOBAL_XID_BASE;
      use zerocopy::byteorder::big_endian::U64;
      use zerocopy::FromBytes;
      let next = match kv.get(&kv::key::meta_next_global_xid_key())? {
          Some(b) => {
              let (v, _) = U64::read_from_prefix(b.as_slice())
                  .map_err(|_| kv::KvError::CorruptRow("next_global_xid not u64".into()))?;
              v.get()
          }
          None => GLOBAL_XID_BASE,
      };
      Ok(next.max(GLOBAL_XID_BASE))
  }
  ```
  `durable_global_snapshot(range0)` returns `Snapshot { xmin: GLOBAL_XID_BASE, xmax: gtm::read_next_global(range0)?, xip: vec![] }`.
- **Prune the running set on commit** (bounds memory; the set is now visibility-irrelevant): in `handle_txn`'s `CommitGlobal` arm, after `commit_global_decision` succeeds, call `e.finish_global(g)` on the range-0 engine. (`LocalCoordinator::commit_global` already calls `finish_global`.)
- Net effect: `finish_global` is genuinely a visibility no-op; a range-0 leader change is transparent; the in-process `MultiRangeCluster` stays correct because `begin_global_durable` (used by `LocalCoordinator`) persists `next_global` at allocation, so the durable reconstruction matches the in-doubt logic.

**H1 (HIGH) — remote SQL errors must keep their SQLSTATE class, not collapse to 0A000.** Add a `Retryable` variant to `TxnResp` (T1) and split `map_exec_err` (T3): `SerializationFailure`/`Deadlock` → `TxnResp::Retryable`, `NotLeader` → `TxnResp::NotLeader`, else → `TxnResp::Err(msg)`. In `NetCoordinator::stage_remote` (T4): `Retryable` → `Err(ExecError::SerializationFailure)`, `NotLeader` → `Err(ExecError::NotLeader)`, `Err(m)` → `Err(ExecError::Unsupported(m))`. This preserves 40001 retryability on the remote-participant path. (`TwoPcClient::call` must also `continue` retry on `Retryable` the same as `NotLeader`? No — `Retryable` is a serialization failure the CLIENT retries, not the pooled client; return it.)

**H2 (HIGH) — `TxnService` must not hold the map lock across session work.** Map value is `Arc<tokio::sync::Mutex<executor::SqlSession>>`. `stage`/`release`/`holds`/`release_all_for_range` lock the map only to get/insert/remove+clone the `Arc`, drop the map guard, then lock the per-session `Arc`. This prevents the hold-and-wait deadlock (a `Stage` on g1 blocking on a row lock held by g2 while pinning the map mutex that `Release(g2)` needs). Corrected code in T3 Step 3 below.

**H3 (HIGH) — `serve_node_protocol` per-connection clone must include `txn`.** The accept loop re-clones shared state per connection (`server.rs:111`). Change to `let (registry, partition, shutdown, txn) = (registry.clone(), partition.clone(), shutdown.clone(), txn.clone());` (`TxnService: Clone`).

**H4 (HIGH) — task ordering: the gateway_local atomic-commit FLIP belongs in T5, not T4.** At T4's commit, range-1's engine has `gtm=None` and no barrier yet, so `SELECT id FROM b` resolves invisible and the "both rows visible" assertion fails. **Move** `gateway_commits_a_cross_range_transaction_atomically` + `gateway_rolls_back_a_cross_range_transaction_atomically` into T5 (after `durable_global_snapshot` + the `Range0Barrier` are wired). T4's own test instead asserts only that escalation no longer errors `0A000` (the `BEGIN/INSERT a/INSERT b/COMMIT` all return `Ok`, and range-0's `a` reads back — range-0's engine has the GTM) — see corrected T4 Step 1.

**Mechanical corrections (apply where the inline text differs):**
- **M1 — static bring-up double-builds range 0.** `start_static` loops `for range in map.range_ids()` (includes range 0). Special-case range 0 out (init its GTM), then loop `map.range_ids().filter(|&r| r != 0)` for data ranges (mirroring the replicated path).
- **M2 — `RangeRouter::new` has FOUR call sites**, not two: `route.rs:166` (gateway), `router.rs:203` (`connect`), and the two in-module test sites `router.rs:853`/`router.rs:926`. Make the new field `coordinator: Option<Arc<dyn GlobalCoordinator>>`; the two test sites pass `None`.
- **M3 — `RangeRouter::connect` lives in `router.rs:198` (not `cluster.rs`).** Edit `router.rs` `connect` to build `LocalCoordinator { range0: c.leader_engine(0).await }` (uses the param `c`, not `self`) and pass `Some(Arc::new(local))`. `cluster.rs` needs no change.
- **M4 — test KV API:** use `kv::MemKv::new()` + `.write_batch(&[WriteOp])` (copy `exec.rs` SP16 tests), NOT `kv::mem::MemKv`/`.apply`. Build the durable `next_global` value with `(g+1).to_be_bytes().to_vec()`.
- **M5 — `ServerNode` has no `node_addr`.** Add `pub fn node_addr(&self) -> &str` (store `cfg.node_addr` in the struct at bring-up) so the T6 control test reaches the node port WITHOUT changing `start_two_range_node`'s return arity (which would break the two existing destructures at `gateway_local.rs:94/135`).
- **M6 — best-effort release in `finish_txn`.** After `commit_global` succeeds, do NOT `?`-propagate a per-participant release failure — iterate every participant, attempt release, and still return `COMMIT`/`ROLLBACK` success (the durable decision is the source of truth; a lingering remote lock is the SP18 liveness gap, covered by release-on-leadership-loss). Never let one participant's failure skip the others.
- **M7 — fix the false endianness prose** at the old T2 Step 1 note and T5 Step 1/3 notes: the layout is **big-endian**, not "native/LE".

---

## File structure (what changes, and why)

**New files:**
- `crates/cluster/src/twopc.rs` — the networked 2PC module: `TwoPcClient` (pooled node-port client, leader-resolve + bounded retry, modeled on `ForwardPool`), `NetCoordinator` (the `GlobalCoordinator` impl), and `TxnService` (the participant-side per-`(G,range)` held-session registry + `handle_txn` dispatch body). One cohesive ~350-line module; client and service share the `TxnRpc`/`TxnResp` types.
- `crates/crabgresql/tests/crossrange_2pc_net.rs` — the multi-process cross-node e2e (UAC-safe name: no `setup/install/update/patch/upgrad`).

**Modified files:**
- `crates/cluster/src/transport/protocol.rs` — add `NodeRequest::Txn { range, rpc }`, `NodeResponse::Txn`, and the `TxnRpc`/`TxnResp` enums.
- `crates/cluster/src/transport/server.rs` — `serve_node_protocol` gains a `txn: Option<TxnService>` param; a `NodeRequest::Txn` dispatch arm; the four call sites update.
- `crates/cluster/src/server_node.rs` — wire a GTM into every node's range-0 engine; build the `TxnService` + `NetCoordinator` and thread them into `serve_node_protocol` / `spawn_sql_gateway`; the range-0 read barrier handle; release-on-leadership-loss; the per-range-leaders control answer.
- `crates/cluster/src/range/router.rs` — `GlobalCoordinator` trait + `LocalCoordinator`; rewire `dispatch`/`finish_txn`/`can_escalate` to the coordinator seam; the escalate-on-remote-range rule.
- `crates/cluster/src/range/cluster.rs` — wire `LocalCoordinator` into the in-process `RangeRouter::connect` so `MultiRangeCluster` stays green.
- `crates/cluster/src/lib.rs` — `pub mod twopc;`.
- `crates/executor/src/lib.rs` — `begin_global_durable`; a `range0_barrier: Option<Arc<dyn Linearizer>>` engine field threaded through `with_kv`/`replicated`/`clone_handle`/`connect`; a `share_range0_barrier_to` setter; `reseed_gtm`.
- `crates/executor/src/session.rs` — thread `range0_barrier`; `ensure_global_readable`; reconstruct `gsnap` from durable range-0 state; gate the read sites.
- `crates/executor/src/gtm.rs` — (no signature change; `finish_global` already a removal-only no-op for visibility).
- `crates/cluster/tests/gateway_local.rs` — flip the `0A000` rejection test to an atomic cross-range commit.
- `crates/cluster/src/transport/protocol.rs` — `ControlRequest::RangeLeaders` + a `ControlResponse::RangeLeaders(Vec<(RangeId, Option<NodeId>)>)`.
- `CLAUDE.md` — SP17 UAC-safe target audit line.

---

## Task 1: `NodeRequest::Txn` wire protocol + pooled node-port 2PC client

**Files:**
- Modify: `crates/cluster/src/transport/protocol.rs` (add `TxnRpc`/`TxnResp` after `ControlResponse` at line 57; `NodeRequest::Txn` after line 71; `NodeResponse::Txn` after line 77)
- Create: `crates/cluster/src/twopc.rs`
- Modify: `crates/cluster/src/lib.rs` (add `pub mod twopc;` near the other `pub mod` lines, e.g. beside `mod linearizer;`/`pub use linearizer::RaftLinearizer;` around line 10/22)
- Test: an inline `#[cfg(test)] mod tests` in `crates/cluster/src/twopc.rs`

- [ ] **Step 1: Write the failing test — wire types round-trip + client reaches a stub server**

Add to the bottom of the new `crates/cluster/src/twopc.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::protocol::{NodeRequest, NodeResponse, TxnResp, TxnRpc};

    #[test]
    fn txn_rpc_round_trips_through_json() {
        let reqs = vec![
            NodeRequest::Txn { range: 0, rpc: TxnRpc::BeginGlobal },
            NodeRequest::Txn { range: 2, rpc: TxnRpc::Stage { g: 1 << 63, range: 2, sql: "UPDATE b SET id=21".into() } },
            NodeRequest::Txn { range: 0, rpc: TxnRpc::CommitGlobal { g: 1 << 63, commit: true } },
            NodeRequest::Txn { range: 2, rpc: TxnRpc::Release { g: 1 << 63, range: 2, commit: false } },
            NodeRequest::Txn { range: 0, rpc: TxnRpc::GlobalBarrier },
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
            TxnResp::Err("boom".into()),
        ] {
            let env = NodeResponse::Txn(resp);
            let bytes = serde_json::to_vec(&env).expect("encode");
            let back: NodeResponse = serde_json::from_slice(&bytes).expect("decode");
            assert_eq!(format!("{env:?}"), format!("{back:?}"));
        }
    }
}
```

- [ ] **Step 2: Run it — expect a compile failure (types do not exist yet)**

Run: `cargo test -p cluster --lib twopc::tests::txn_rpc_round_trips_through_json`
Expected: FAIL to compile — `TxnRpc`, `TxnResp`, `NodeRequest::Txn`, `NodeResponse::Txn` undefined.

- [ ] **Step 3: Add the wire types to `protocol.rs`**

After `ControlResponse` (line 57) insert:

```rust
/// Structured cross-range 2PC requests on the node port. `BeginGlobal`,
/// `CommitGlobal`, and `GlobalBarrier` target range 0 (the GTM authority);
/// `Stage`/`Release` target the participant `range`. The envelope's `range`
/// (see `NodeRequest::Txn`) is what the server resolves the target group with.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TxnRpc {
    /// Durably allocate a global xid on range 0's leader.
    BeginGlobal,
    /// Stage `sql` on participant `range` inside the held txn `g`.
    Stage { g: u64, range: RangeId, sql: String },
    /// Write the single global decision on range 0's leader.
    CommitGlobal { g: u64, commit: bool },
    /// Release participant `range`'s held txn `g` (free locks; no clog write).
    Release { g: u64, range: RangeId, commit: bool },
    /// Return range 0's linearizable applied index (the read barrier).
    GlobalBarrier,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TxnResp {
    Began { g: u64 },
    Staged,
    Committed,
    Released,
    Barrier { applied_index: u64 },
    /// The target was not the range's leader — the caller re-resolves and retries.
    NotLeader,
    /// A retryable serialization failure / deadlock on the participant (40001 /
    /// 40P01) — surfaced to the CLIENT as a retryable error, not collapsed to
    /// 0A000 (correction H1).
    Retryable,
    Err(String),
}
```

In `NodeRequest` (after `Control(ControlRequest),`, line 71) insert:

```rust
    /// A structured 2PC RPC for the `range`-th co-located group on this node.
    Txn { range: RangeId, rpc: TxnRpc },
```

In `NodeResponse` (after `Control(ControlResponse),`, line 77) insert:

```rust
    Txn(TxnResp),
```

- [ ] **Step 4: Scaffold `crates/cluster/src/twopc.rs` with the pooled client**

Create `crates/cluster/src/twopc.rs`. Model the pool on `forward.rs` (per-leader pooled node-port conn, `await_leader` no-sleep wait, bounded one-retry). Dial the **node** port (`crate::addr::node_dial_addr`), exchange `NodeRequest::Txn`/`NodeResponse::Txn` via `frame::write_msg`/`read_msg`:

```rust
//! Networked cross-range 2PC: a pooled node-port client (coordinator side) and a
//! participant-side held-session registry (`TxnService`, added in Task 3). The
//! coordinator resolves each target range's leader from its own per-range Raft
//! metrics and exchanges `TxnRpc`/`TxnResp` over the node port, mirroring
//! `forward::ForwardPool`'s leader-resolution + bounded retry, but speaking the
//! structured node protocol instead of pgwire.
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

/// Per-leader pooled node-port connection (one in-flight exchange at a time).
struct PooledConn {
    addr: String,
    stream: TcpStream,
}

/// A pooled client that sends `TxnRpc`s to the current leader of a target range.
/// `rafts` provides each range's leader (via openraft metrics); `conns` pools one
/// node-port connection per target node.
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

    /// Resolve `range`'s leader node id + its node-dial address, or `None` if no
    /// reachable leader (Ref dropped before any await; partitioned leader excluded
    /// — the SP14 `is_paused` lesson).
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

    /// Await a resolvable leader for `range` without sleeping (mirrors
    /// `forward::ForwardPool::await_leader`): poll on `metrics().changed()`.
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

    /// Send one `TxnRpc` to `range`'s current leader, with a bounded
    /// re-resolve+retry on `NotLeader`/wire failure. `envelope_range` is what the
    /// server resolves the target group with (range 0 for global ops, the
    /// participant range for Stage/Release).
    pub async fn call(&self, target_range: RangeId, rpc: TxnRpc) -> Result<TxnResp, ()> {
        for attempt in 0..2 {
            let (leader, addr) = self.await_leader(target_range).await.ok_or(())?;
            let env = NodeRequest::Txn { range: target_range, rpc: rpc.clone() };
            match self.exchange(leader, &addr, &env).await {
                Ok(TxnResp::NotLeader) if attempt == 0 => continue, // re-resolve + retry once
                Ok(resp) => return Ok(resp),
                Err(()) if attempt == 0 => continue,
                Err(()) => return Err(()),
            }
        }
        Err(())
    }

    /// One pooled exchange: (re)dial when there is no conn or the leader moved;
    /// drop a poisoned conn so the retry redials.
    async fn exchange(
        &self,
        leader: NodeId,
        addr: &str,
        env: &NodeRequest,
    ) -> Result<TxnResp, ()> {
        if self.partition.blocked(leader) {
            return Err(());
        }
        let mut conns = self.conns.lock().await;
        let needs_dial = conns.get(&leader).is_none_or(|c| c.addr != addr);
        if needs_dial {
            let stream = tokio::time::timeout(TXN_TIMEOUT, TcpStream::connect(addr))
                .await
                .map_err(|_| ())?
                .map_err(|_| ())?;
            conns.insert(leader, PooledConn { addr: addr.to_string(), stream });
        }
        let conn = conns.get_mut(&leader).expect("pooled conn present");
        let exchange = async {
            write_msg(&mut conn.stream, env).await?;
            read_msg::<_, NodeResponse>(&mut conn.stream).await
        };
        match tokio::time::timeout(TXN_TIMEOUT, exchange).await {
            Ok(Ok(NodeResponse::Txn(resp))) => Ok(resp),
            _ => {
                conns.remove(&leader); // poisoned/unexpected → redial on retry
                Err(())
            }
        }
    }
}
```

Add `pub mod twopc;` to `crates/cluster/src/lib.rs` beside the other module declarations.

- [ ] **Step 5: Run the test — expect PASS**

Run: `cargo test -p cluster --lib twopc::tests::txn_rpc_round_trips_through_json`
Expected: PASS. Then `cargo build -p cluster` to confirm the new module compiles (the server dispatch arm for `NodeRequest::Txn` is added in Task 2/3; until then the `match req` in `server.rs` is non-exhaustive — add a temporary `NodeRequest::Txn { .. } => NodeResponse::Txn(TxnResp::Err("txn unsupported".into())),` arm in `serve_node_protocol` so the crate compiles; Task 3 replaces it).

- [ ] **Step 6: Commit**

```bash
git add crates/cluster/src/transport/protocol.rs crates/cluster/src/twopc.rs crates/cluster/src/lib.rs crates/cluster/src/transport/server.rs
git commit -m "feat(sp17): NodeRequest::Txn wire protocol + pooled node-port 2PC client

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Durable GTM network service (begin_global_durable + range-0 GTM wiring + reseed)

**Files:**
- Modify: `crates/executor/src/lib.rs` (add `begin_global_durable` after `begin_global` at line 199; add `reseed_gtm` near `reseed_counters`)
- Modify: `crates/cluster/src/server_node.rs` (`build_range_group` returns an un-`Arc`'d engine so the caller can wire the GTM; init the GTM on range 0; reseed the GTM on range-0 leadership)
- Test: `crates/cluster/tests/gateway_local.rs` (new test `begin_global_durable_persists_next_global`)

- [ ] **Step 1: Write the failing test**

Add to `crates/cluster/tests/gateway_local.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn begin_global_durable_persists_next_global() {
    use mvcc::xid::GLOBAL_XID_BASE;
    let (node, _sql_addr) = start_two_range_node().await;
    let engine = node.engines.get(&0).expect("range-0 engine");
    assert!(engine.has_gtm(), "range-0 engine must carry the GTM after wiring");

    // First durable allocation returns >= the global base and advances the counter.
    let g0 = engine.begin_global_durable().await.expect("alloc g0");
    assert!(g0 >= GLOBAL_XID_BASE, "global xids live above the base");
    let g1 = engine.begin_global_durable().await.expect("alloc g1");
    assert_eq!(g1, g0 + 1, "allocations are monotonic");

    // The advance is durable (no raw byte decode — avoids endianness coupling):
    // a reseed of a fresh in-memory counter never regresses below the persisted
    // value, so a subsequent allocation stays strictly monotone past g1.
    engine.reseed_gtm().expect("reseed");
    let g2 = engine.begin_global_durable().await.expect("alloc g2");
    assert!(g2 > g1, "post-reseed allocation never regresses below the durable counter");
}
```

(`node.engines` is a public field — `server_node.rs:69`. `reseed_gtm` is added in Step 3. The on-disk `next_global_xid` layout is **big-endian** — see correction C1; this test deliberately asserts via `reseed_gtm` + a fresh allocation rather than decoding bytes, so it is byte-order-agnostic.)

- [ ] **Step 2: Run it — expect failure**

Run: `cargo nextest run -p cluster --test gateway_local begin_global_durable_persists_next_global`
Expected: FAIL — `has_gtm()` is false (no GTM wired) and `begin_global_durable`/`reseed_gtm` do not exist.

- [ ] **Step 3: Add `begin_global_durable` + `reseed_gtm` to `SqlEngine` (`executor/src/lib.rs`)**

After `begin_global` (line 199) add:

```rust
    /// Durably allocate a global xid: bump the in-memory counter, then persist
    /// `next_global` through range 0's committer BEFORE returning, so any later
    /// range-0 leader reseeds past `g` and a global xid is never reused across a
    /// range-0 leader change. Only succeeds on range 0's leader (the committer
    /// rejects non-leaders → `ExecError::NotLeader`).
    pub async fn begin_global_durable(&self) -> Result<u64, ExecError> {
        let gtm = self
            .gtm
            .as_ref()
            .expect("begin_global_durable on a non-GTM engine");
        let g = gtm.begin_global();
        self.committer.commit(vec![gtm.next_global_xid_op()]).await?;
        Ok(g)
    }

    /// Lift the GTM's in-memory `next_global` to the durable value (never
    /// regresses). Called on the range-0 leadership rising edge.
    pub fn reseed_gtm(&self) -> Result<(), ExecError> {
        if let Some(gtm) = self.gtm.as_ref() {
            gtm.reseed_from_applied()?;
        }
        Ok(())
    }
```

(`reseed_from_applied` already exists at `gtm.rs:63-76`, currently `#[allow(dead_code)]` — remove that attribute since it now has a caller.)

- [ ] **Step 4: Wire the GTM into every node's range-0 engine (`server_node.rs`)**

Refactor `build_range_group` (line 138) to return the engine **before** `Arc`-wrapping, and move the `reseed_on_leadership` spawn to the call site, so the caller can `init_gtm_coordinator()` on range 0:

- Change the return type (line 145) from `… Arc<SqlEngine>)` to `… SqlEngine)`; build `engine` as a plain `SqlEngine` (drop the `Arc::new(` wrapper at line 162-170); **delete** the `tokio::spawn(reseed_on_leadership(...))` at line 171 (it moves to the caller).
- At the **static** call site (line 198-204) and the **replicated** range-0 build (line 334-338), after building range 0's engine, wire the GTM and `Arc`-wrap, then spawn the (now GTM-aware) reseed:

```rust
    // Range 0 hosts the GTM. Every node's range-0 engine carries it; its
    // *authority* is gated by range-0 leadership (begin_global_durable's commit
    // and commit_global_decision only succeed on the leader).
    let (r0_raft, r0_sm_kv, mut r0_engine) =
        build_range_group(&store, 0, cfg.id, &partition, &registry, &catalog_kv).await;
    r0_engine
        .init_gtm_coordinator()
        .expect("init GTM coordinator over range 0");
    let r0_engine = Arc::new(r0_engine);
    tokio::spawn(reseed_on_leadership(r0_raft.clone(), r0_engine.clone()));
    rafts.insert(0, r0_raft);
    sm_kvs.insert(0, r0_sm_kv);
    engines.insert(0, r0_engine);
```

For each **data** range (the loop), `Arc`-wrap and spawn reseed without a GTM:

```rust
    let (raft, sm_kv, engine) =
        build_range_group(&store, range, cfg.id, &partition, &registry, &catalog_kv).await;
    let engine = Arc::new(engine);
    tokio::spawn(reseed_on_leadership(raft.clone(), engine.clone()));
    rafts.insert(range, raft);
    sm_kvs.insert(range, sm_kv);
    engines.insert(range, engine);
```

(For the **static single-range** path where `range_count() == 1`, range 0 is the only range — the GTM is harmless but unused, since no cross-range escalation occurs.)

- [ ] **Step 5: Make `reseed_on_leadership` reseed the GTM (`server_node.rs:435-451`)**

In the rising-edge arm (line 442-444), add the GTM reseed beside the counter reseed:

```rust
        if is_leader && !was_leader {
            let _ = engine.reseed_counters();
            let _ = engine.reseed_gtm(); // lift next_global past every durable allocation
        }
```

- [ ] **Step 6: Run the test — expect PASS**

Run: `cargo nextest run -p cluster --test gateway_local begin_global_durable_persists_next_global`
Expected: PASS. Then `cargo nextest run -p cluster --test gateway_local` (all gateway_local tests still pass) and `cargo nextest run -p executor` (executor regressions green).

- [ ] **Step 7: Commit**

```bash
git add crates/executor/src/lib.rs crates/executor/src/gtm.rs crates/cluster/src/server_node.rs crates/cluster/tests/gateway_local.rs
git commit -m "feat(sp17): durable global-xid allocation + range-0 GTM wired into every node

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Per-G held-session participant registry + the `handle_txn` server dispatch

**Files:**
- Modify: `crates/cluster/src/twopc.rs` (add `TxnService` + the `handle_txn` dispatch body)
- Modify: `crates/cluster/src/transport/server.rs` (`serve_node_protocol` gains a `txn: Option<TxnService>` param; the `NodeRequest::Txn` arm dispatches to it; the four call sites pass it — `None` for the test harnesses that do not host 2PC)
- Modify: `crates/cluster/src/server_node.rs` (construct the `TxnService` and pass it into both `serve_node_protocol` call sites)
- Test: `crates/cluster/src/twopc.rs` inline test `stage_then_release_holds_then_frees_a_per_g_session` (drives the service directly, no network)

- [ ] **Step 1: Write the failing test**

The participant registry is exercised directly (no socket) by building a 2-range in-crate node and driving `TxnService` against its engines. Add to `crates/cluster/src/twopc.rs` `mod tests`:

```rust
    // A held per-(G, range) session survives across calls: Stage opens + holds the
    // txn (writes a Prepared marker, keeps the row lock), a Release frees it. The
    // service is what a participant leader runs; here we drive it in-process.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stage_then_release_holds_then_frees_a_per_g_session() {
        use mvcc::clog::{get as clog_get, XidStatus};
        let (node, _sql) = crate::server_node::testonly_two_range_node().await;
        let svc = TxnService::new(node.engines.clone());

        // Seed a row on range 1 (table id 2) via an autocommit session first.
        let mut seed = node.engines[&1].connect();
        seed.run(&parse_one("CREATE TABLE b (id int4)")).await.expect("create b");
        seed.run(&parse_one("INSERT INTO b VALUES (20)")).await.expect("seed b");

        let g: u64 = mvcc::xid::GLOBAL_XID_BASE + 7;
        // Stage a write into a held txn g on range 1.
        match svc.handle(1, TxnRpc::Stage { g, range: 1, sql: "UPDATE b SET id = 21 WHERE id = 20".into() }).await {
            TxnResp::Staged => {}
            other => panic!("expected Staged, got {other:?}"),
        }
        // The held session still exists (not yet released).
        assert!(svc.holds(g, 1).await, "Stage parks a held session under (g, range)");

        // Release (commit) frees it.
        match svc.handle(1, TxnRpc::Release { g, range: 1, commit: true }).await {
            TxnResp::Released => {}
            other => panic!("expected Released, got {other:?}"),
        }
        assert!(!svc.holds(g, 1).await, "Release drops the held session");
    }
```

Add the small parse helper if not already present in the test module:

```rust
    fn parse_one(sql: &str) -> pgparser::ast::Statement {
        pgparser::parse(sql).expect("parse").into_iter().next().expect("one statement")
    }
```

And add a `#[cfg(test)]` constructor to `server_node.rs` that builds the in-crate 2-range node the test needs (mirrors `gateway_local::start_two_range_node`, but lives in the crate so unit tests can reach it):

```rust
#[cfg(test)]
pub(crate) async fn testonly_two_range_node() -> (ServerNode, String) {
    // single self-bootstrapping node, 2-range static map (boundary at table 2)
    // … identical body to gateway_local::start_two_range_node, returning (node, sql_addr) …
}
```

- [ ] **Step 2: Run it — expect failure**

Run: `cargo nextest run -p cluster --lib twopc::tests::stage_then_release_holds_then_frees_a_per_g_session`
Expected: FAIL — `TxnService`, `handle`, `holds`, `testonly_two_range_node` do not exist.

- [ ] **Step 3: Implement `TxnService` in `twopc.rs`**

```rust
use crate::types::TypeConfig; // already imported
use executor::SqlEngine;
use crate::transport::protocol::TxnResp as _; // (TxnResp already imported above)

/// Participant-side held-session registry. Lives on each node; resolves the
/// node's per-range engines and keeps one `SqlSession` per in-flight `(G, range)`
/// it participates in, detached from any TCP connection so a later `Release(G)`
/// from a different connection finds it. Each session is its OWN `Arc<Mutex>` so
/// the map lock is held only for lookup/insert/remove — NEVER across session work
/// (correction H2: holding the map lock across `session.run().await` deadlocks a
/// `Stage(g1)` that blocks on a row lock held by `g2` against the `Release(g2)`
/// that needs the same map lock).
type HeldSession = Arc<Mutex<executor::SqlSession>>;

#[derive(Clone)]
pub struct TxnService {
    engines: HashMap<RangeId, Arc<SqlEngine>>,
    held: Arc<Mutex<HashMap<(u64, RangeId), HeldSession>>>,
}

impl TxnService {
    pub fn new(engines: HashMap<RangeId, Arc<SqlEngine>>) -> Self {
        Self { engines, held: Arc::new(Mutex::new(HashMap::new())) }
    }

    #[cfg(test)]
    pub async fn holds(&self, g: u64, range: RangeId) -> bool {
        self.held.lock().await.contains_key(&(g, range))
    }

    /// Drop every held session for `range` (presumed-abort), freeing its locks.
    /// Called on the loss of `range` leadership. Always safe: the global clog is
    /// the sole arbiter (a committed g's durable Prepared rows stay visible; an
    /// undecided g's rows are invisible).
    pub async fn release_all_for_range(&self, range: RangeId) {
        // Take the sessions OUT under a brief map lock, drop the guard, then abort.
        let victims: Vec<HeldSession> = {
            let mut held = self.held.lock().await;
            let keys: Vec<(u64, RangeId)> =
                held.keys().copied().filter(|&(_, r)| r == range).collect();
            keys.into_iter().filter_map(|k| held.remove(&k)).collect()
        };
        for s in victims {
            s.lock().await.abort_release();
        }
    }

    /// Dispatch one participant-targeted `TxnRpc` (`Stage`/`Release`). Global ops
    /// (`BeginGlobal`/`CommitGlobal`/`GlobalBarrier`) are handled by the server
    /// against range 0's engine/raft — see `server::handle_txn`.
    pub async fn handle(&self, _range: RangeId, rpc: TxnRpc) -> TxnResp {
        match rpc {
            TxnRpc::Stage { g, range: r, sql } => self.stage(g, r, &sql).await,
            TxnRpc::Release { g, range: r, commit } => self.release(g, r, commit).await,
            // Global ops never reach the service.
            _ => TxnResp::Err("non-participant rpc routed to TxnService".into()),
        }
    }

    /// Get-or-create the held session for `(g, range)` under a BRIEF map lock,
    /// returning a clone of its `Arc<Mutex>` (map guard dropped before any await).
    async fn session_handle(&self, g: u64, range: RangeId) -> Option<HeldSession> {
        let engine = self.engines.get(&range)?.clone();
        let mut held = self.held.lock().await;
        Some(
            held.entry((g, range))
                .or_insert_with(|| Arc::new(Mutex::new(engine.connect())))
                .clone(),
        )
    }

    async fn stage(&self, g: u64, range: RangeId, sql: &str) -> TxnResp {
        let stmt = match pgparser::parse(sql) {
            Ok(mut v) if v.len() == 1 => v.pop().expect("one statement"),
            _ => return TxnResp::Err("stage expects exactly one statement".into()),
        };
        let Some(handle) = self.session_handle(g, range).await else {
            return TxnResp::Err(format!("no engine for range {range}"));
        };
        // Per-session lock only (the map lock was already dropped) — different g's
        // progress independently; a Release for another g is never blocked behind
        // this stage.
        let mut session = handle.lock().await;
        // First stage on this (g, range): begin a held txn + enlist as participant.
        if let Err(e) = session.ensure_began().await {
            return map_exec_err(e);
        }
        if let Err(e) = session.join_global(g).await {
            return map_exec_err(e);
        }
        match session.run(&stmt).await {
            Ok(_) => TxnResp::Staged,
            Err(e) => map_exec_err(e),
        }
    }

    async fn release(&self, g: u64, range: RangeId, commit: bool) -> TxnResp {
        // Remove from the map under a brief lock, then release the session.
        let handle = { self.held.lock().await.remove(&(g, range)) };
        if let Some(handle) = handle {
            let mut session = handle.lock().await;
            if commit { session.commit_release() } else { session.abort_release() }
        }
        // Releasing an unknown (g, range) is a no-op success (idempotent retry).
        TxnResp::Released
    }
}

/// Map an `ExecError` from a staged statement to a wire response, PRESERVING the
/// retryable serialization-failure class (correction H1: collapsing 40001 to
/// 0A000 would make a retryable conflict look like an unsupported feature).
fn map_exec_err(e: executor::ExecError) -> TxnResp {
    use executor::ExecError;
    match e {
        ExecError::NotLeader => TxnResp::NotLeader,
        ExecError::SerializationFailure | ExecError::Deadlock => TxnResp::Retryable,
        other => TxnResp::Err(other.to_string()),
    }
}
```

(Confirm `executor::SqlSession`, `executor::SqlEngine`, `executor::ExecError` and `SqlSession::{ensure_began, join_global, commit_release, abort_release, run}` are exported — `session.rs:110/606/622/637/644` and `lib.rs` re-exports. `pgparser::parse` returns `Result<Vec<Statement>, _>`. Confirm the exact `ExecError` variant names `SerializationFailure`/`Deadlock` against `executor/src/error.rs`; if a name differs, match it.)

- [ ] **Step 4: Add the `handle_txn` server dispatch (`transport/server.rs`)**

Add a `txn: Option<TxnService>` param to `serve_node_protocol` (line 101-106) and the dispatch arm inside `let resp = match req` (between the `Control` arm end, line 140, and the match close):

```rust
                    NodeRequest::Txn { range, rpc } => {
                        match &txn {
                            Some(svc) => NodeResponse::Txn(handle_txn(&registry, svc, range, rpc).await),
                            None => NodeResponse::Txn(TxnResp::Err("node hosts no 2PC service".into())),
                        }
                    }
```

Add the handler beside `handle_control` (after line 204). Global ops use range 0's engine/raft; participant ops delegate to the service:

```rust
async fn handle_txn(
    registry: &RangeRegistry,
    svc: &crate::twopc::TxnService,
    range: RangeId,
    rpc: TxnRpc,
) -> TxnResp {
    use crate::transport::protocol::TxnRpc;
    match rpc {
        TxnRpc::BeginGlobal => match svc.engine(0) {
            Some(e) => match e.begin_global_durable().await {
                Ok(g) => TxnResp::Began { g },
                Err(executor::ExecError::NotLeader) => TxnResp::NotLeader,
                Err(e) => TxnResp::Err(e.to_string()),
            },
            None => TxnResp::Err("no range-0 engine".into()),
        },
        TxnRpc::CommitGlobal { g, commit } => match svc.engine(0) {
            Some(e) => {
                let status = if commit {
                    mvcc::clog::XidStatus::Committed
                } else {
                    mvcc::clog::XidStatus::Aborted
                };
                match e.commit_global_decision(g, status).await {
                    Ok(()) => {
                        e.finish_global(g); // prune g from the in-memory running set (C2; bounds memory)
                        TxnResp::Committed
                    }
                    Err(executor::ExecError::NotLeader) => TxnResp::NotLeader,
                    Err(e) => TxnResp::Err(e.to_string()),
                }
            }
            None => TxnResp::Err("no range-0 engine".into()),
        },
        TxnRpc::GlobalBarrier => match resolve(registry, 0) {
            Some(raft) => match raft.ensure_linearizable().await {
                Ok(read_log_id) => TxnResp::Barrier {
                    applied_index: read_log_id.map(|l| l.index).unwrap_or(0),
                },
                Err(_) => TxnResp::NotLeader,
            },
            None => TxnResp::Err("no range-0 group".into()),
        },
        // Stage / Release: delegate to the held-session registry.
        rpc @ (TxnRpc::Stage { .. } | TxnRpc::Release { .. }) => svc.handle(range, rpc).await,
    }
}
```

Add the `engine(range)` accessor to `TxnService` (so `handle_txn` reaches range 0's engine):

```rust
    pub fn engine(&self, range: RangeId) -> Option<&Arc<SqlEngine>> {
        self.engines.get(&range)
    }
```

(`ensure_linearizable()` on a follower returns `Err(ForwardToLeader …)` → mapped to `NotLeader`; `read_log_id` is `Option<LogId>` in this openraft version — confirm the exact return type and adapt the `.map(...)` accordingly. The participant resolves the barrier only from range 0's **leader**.)

- [ ] **Step 5: Update the four `serve_node_protocol` call sites**

- `server_node.rs:207-212` (static) and `:344` (replicated): build a `TxnService::new(engines.clone())` (engines already in scope; `engines: HashMap<RangeId, Arc<SqlEngine>>`) and pass `Some(txn)`.
- `transport/server.rs:316-321` (the `range_aware` test) and `transport/testcluster.rs:67`: pass `None` (these harnesses host no SQL engines / no 2PC).

Import `TxnResp`, `TxnRpc` into `server.rs` from `crate::transport::protocol`.

**Correction H3 — the per-connection clone tuple must include `txn`.** `serve_node_protocol`'s accept loop re-clones shared state for each spawned connection task (`server.rs:111`). Extend it so `txn` is available in every connection:

```rust
        let (registry, partition, shutdown, txn) =
            (registry.clone(), partition.clone(), shutdown.clone(), txn.clone());
```

(`TxnService` derives `Clone` over `Arc`s, so this is a cheap refcount bump; without it the owned `txn` is moved into the first connection's `move` task and the loop fails to compile.)

- [ ] **Step 6: Run the test + regressions — expect PASS**

Run: `cargo nextest run -p cluster --lib twopc::tests::stage_then_release_holds_then_frees_a_per_g_session`
Expected: PASS. Then `cargo nextest run -p cluster` (transport + gateway_local + crossrange_2pc all green; the temporary `Txn` stub arm from Task 1 is now replaced).

- [ ] **Step 7: Commit**

```bash
git add crates/cluster/src/twopc.rs crates/cluster/src/transport/server.rs crates/cluster/src/transport/testcluster.rs crates/cluster/src/server_node.rs
git commit -m "feat(sp17): per-(G,range) held-session participant registry + handle_txn dispatch

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: The gateway coordinator — `GlobalCoordinator` seam, escalate-on-remote-range, NetCoordinator

**Files:**
- Modify: `crates/cluster/src/range/router.rs` (`GlobalCoordinator` trait + `LocalCoordinator`; rewire `dispatch`/`finish_txn`/`can_escalate`; the escalate-on-remote-range rule; new `coordinator` field + constructor arg)
- Modify: `crates/cluster/src/twopc.rs` (`NetCoordinator` impl of `GlobalCoordinator`)
- Modify: `crates/cluster/src/range/cluster.rs` (in-process `RangeRouter::connect` wires `LocalCoordinator`)
- Modify: `crates/cluster/src/route.rs` + `crates/cluster/src/server_node.rs` (the gateway wires `NetCoordinator`)
- Modify: `crates/cluster/tests/gateway_local.rs` (flip the `0A000` rejection test → atomic cross-range commit)

- [ ] **Step 1: Write the failing test (escalation no longer rejects with `0A000`)**

Per correction **H4**, T4 proves *escalation wiring* only — it does NOT assert range-1 (`b`) visibility, which needs T5's durable-`gsnap` reconstruction (range-1's engine has `gtm=None` and no barrier until T5). Replace the body of `gateway_rejects_a_cross_range_transaction_with_0a000` (`gateway_local.rs:133-166`) and rename it:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_escalates_a_cross_range_transaction_without_0a000() {
    let (_node, sql_addr) = start_two_range_node().await;
    let port = sql_addr.rsplit(':').next().expect("port");
    let conn_str = format!("host=127.0.0.1 port={port} user=postgres");
    let (client, connection) = connect_with_retry(&conn_str).await;
    tokio::spawn(connection);

    client.simple_query("CREATE TABLE a (id int4)").await.expect("create a (range 0)");
    client.simple_query("CREATE TABLE b (id int4)").await.expect("create b (range 1)");
    client.simple_query("BEGIN").await.expect("begin");
    client.simple_query("INSERT INTO a VALUES (1)").await.expect("first DML pins range 0");
    // A second statement on a DIFFERENT range escalates to cross-range 2PC — it no
    // longer errors 0A000 (the whole point of T4's coordinator wiring).
    client.simple_query("INSERT INTO b VALUES (2)").await.expect("second range escalates");
    client.simple_query("COMMIT").await.expect("atomic cross-range commit succeeds");

    // Range-0's own engine carries the GTM, so `a` reads back here. (Range-1 `b`
    // read-back is asserted in T5, after durable-gsnap visibility is wired.)
    let a = client.simple_query("SELECT id FROM a").await.expect("select a");
    assert_eq!(row_count(&a), 1, "range-0 row committed and visible");
}

// Counts SimpleQueryMessage::Row entries in a simple_query result (reused by T5).
fn row_count(msgs: &[tokio_postgres::SimpleQueryMessage]) -> usize {
    msgs.iter().filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))).count()
}
```

- [ ] **Step 2: Run it — expect failure**

Run: `cargo nextest run -p cluster --test gateway_local gateway_escalates_a_cross_range_transaction_without_0a000`
Expected: FAIL — escalation is still rejected (`can_escalate()` false on the gateway path, no coordinator wired) so `INSERT INTO b` errors `0A000`.

- [ ] **Step 3: Add the `GlobalCoordinator` seam to `router.rs`**

Beside `RemoteForward` (line 30-36) add:

```rust
/// Drives the cross-range 2PC global operations on behalf of the gateway.
/// `LocalCoordinator` (in-process tests) calls the local range-0 engine + local
/// participant sessions; `NetCoordinator` (networked gateway) RPCs to the
/// relevant range leaders.
#[async_trait::async_trait]
pub trait GlobalCoordinator: Send + Sync {
    /// Durably allocate a global xid (range 0's leader).
    async fn begin_global(&self) -> Result<u64, ExecError>;
    /// Stage `sql` on a REMOTE participant `range` inside held txn `g`.
    async fn stage_remote(&self, g: u64, range: RangeId, sql: &str) -> Result<(), ExecError>;
    /// Write the single global decision (range 0's leader).
    async fn commit_global(&self, g: u64, commit: bool) -> Result<(), ExecError>;
    /// Release a REMOTE participant `range`'s held txn `g`.
    async fn release_remote(&self, g: u64, range: RangeId, commit: bool) -> Result<(), ExecError>;
}
```

Add a `LocalCoordinator` (used by the in-process `MultiRangeCluster`; it only ever does begin/commit against a local GTM-bearing range-0 engine — all participants are local there, so the `*_remote` methods are unreachable):

```rust
/// In-process coordinator over a local GTM-bearing range-0 engine. Participants
/// are always local in `MultiRangeCluster`, so `stage_remote`/`release_remote`
/// are never called.
pub struct LocalCoordinator {
    pub range0: SqlEngine,
}

#[async_trait::async_trait]
impl GlobalCoordinator for LocalCoordinator {
    async fn begin_global(&self) -> Result<u64, ExecError> {
        self.range0.begin_global_durable().await
    }
    async fn stage_remote(&self, _g: u64, range: RangeId, _sql: &str) -> Result<(), ExecError> {
        Err(ExecError::Unsupported(format!("local coordinator has no remote range {range}")))
    }
    async fn commit_global(&self, g: u64, commit: bool) -> Result<(), ExecError> {
        let status = if commit { mvcc::clog::XidStatus::Committed } else { mvcc::clog::XidStatus::Aborted };
        self.range0.commit_global_decision(g, status).await?;
        self.range0.finish_global(g);
        Ok(())
    }
    async fn release_remote(&self, _g: u64, range: RangeId, _commit: bool) -> Result<(), ExecError> {
        Err(ExecError::Unsupported(format!("local coordinator has no remote range {range}")))
    }
}
```

- [ ] **Step 4: Add the `coordinator` field + constructor arg to `RangeRouter`**

In the `RangeRouter` struct (line 110-138) add `coordinator: Option<Arc<dyn GlobalCoordinator>>,` (None on a single-range/in-process router that never escalates; Some when a coordinator is wired). Add the arg to `RangeRouter::new` (line 144-163) and both call sites: `route.rs:166-173` (gateway) passes `Some(net_coordinator)`, `cluster.rs` in-process `RangeRouter::connect` passes `Some(Arc::new(LocalCoordinator { range0 }))`.

- [ ] **Step 5: Rewire `can_escalate`, the escalation arms, and `finish_txn`**

`can_escalate` (line 406-408) becomes:

```rust
fn can_escalate(&self) -> bool {
    self.coordinator.is_some()
}
```

In `dispatch`, apply the **escalate-on-remote-range** rule. Replace the `Pin::Range(p)` escalation arm (line 297-329) and add remote-aware staging via a new helper `stage_on(range, g, stmt)`:

```rust
    // A participant range `r` inside an open global txn `g`: run locally if led,
    // else Stage over RPC. Never panics on a remote range (unlike session_mut).
    async fn stage_on(&mut self, range: RangeId, g: u64, stmt: &Statement) -> Result<QueryResult, ExecError> {
        if self.engines.contains_key(&range) && self.leads.leads(range) {
            self.ensure_began_on(range).await?;
            self.session_mut(range).join_global(g).await?;
            return self.session_mut(range).run(stmt).await;
        }
        let coord = self.coordinator.as_ref().expect("coordinator for cross-range").clone();
        coord.stage_remote(g, range, &self.cur_sql).await
    }
```

Escalation now fires whenever a txn statement targets a range that is **not** the single locally-led pinned range. The `Pin::Open`/`Pin::Range(p)` handling becomes: if the new range `r` is locally led and equals/initialises the pin and no other range is involved → keep the local single-range path; otherwise escalate:

```rust
    Pin::Range(p) => {
        let p = *p;
        if let Some(r) = pinning && r != p {
            if !self.can_escalate() {
                return Err(ExecError::Unsupported("a transaction may not span ranges yet (D3b)".into()));
            }
            let coord = self.coordinator.as_ref().expect("coordinator").clone();
            let g = coord.begin_global().await?;
            // Backfill the already-pinned local range p as a participant.
            self.session_mut(p).join_global(g).await?;
            let mut ranges = std::collections::BTreeSet::new();
            ranges.insert(p);
            ranges.insert(r);
            self.pin = Pin::Global { ranges, g };
            return self.stage_on(r, g, stmt).await;
        }
        self.run_on(p, stmt).await
    }
```

And the **first** table-bearing statement on a **remote** range inside a txn must escalate immediately (a gateway that leads nothing). In the `Pin::Open` arm, when the pinning range `r` is not locally led, escalate to a single-participant global txn:

```rust
    Pin::Open => {
        if let Some(r) = pinning {
            let locally_led = self.engines.contains_key(&r) && self.leads.leads(r);
            if locally_led {
                self.pin = Pin::Range(r);
                return self.run_on(r, stmt).await;
            }
            // Remote first participant: escalate now so it is HELD (not autocommit-forwarded).
            if !self.can_escalate() {
                return Err(ExecError::Unsupported("a transaction may not span ranges yet (D3b)".into()));
            }
            let coord = self.coordinator.as_ref().expect("coordinator").clone();
            let g = coord.begin_global().await?;
            let mut ranges = std::collections::BTreeSet::new();
            ranges.insert(r);
            self.pin = Pin::Global { ranges, g };
            return self.stage_on(r, g, stmt).await;
        }
        // No table (DDL / FROM-less SELECT): run on range 0.
        self.run_on(0, stmt).await
    }
```

The `Pin::Global` arm (line 333-348) routes each statement via `stage_on` if the range is new, else runs it:

```rust
    Pin::Global { ranges, g } => {
        let g = *g;
        if let Some(r) = pinning {
            let known = ranges.contains(&r);
            if !known {
                if let Pin::Global { ranges, .. } = &mut self.pin { ranges.insert(r); }
                return self.stage_on(r, g, stmt).await;
            }
            // Already a participant: run locally if led, else Stage again (idempotent on the held session).
            return self.stage_on(r, g, stmt).await;
        }
        self.run_on(0, stmt).await
    }
```

`finish_txn`'s `Pin::Global` arm (line 355-393) uses the coordinator for the decision + releases each participant local-or-remote:

```rust
    Pin::Global { ranges, g } => {
        let commit = matches!(stmt, Statement::Commit);
        #[cfg(test)]
        if let Some(hook) = self.before_global_decision.as_mut() { hook(); }
        let coord = self.coordinator.as_ref().expect("coordinator").clone();
        // The single atomic instant. After this returns Ok the decision is durable
        // and is the sole source of truth.
        coord.commit_global(g, commit).await?;
        // Release every participant BEST-EFFORT (correction M6): the decision is
        // already final, so a release failure (a remote leader moved) must NOT
        // abort the loop and strand the OTHER participants' releases. A lingering
        // remote lock is the bounded SP18 liveness gap (release-on-leadership-loss).
        for r in &ranges {
            if self.engines.contains_key(r) && self.leads.leads(*r) {
                let s = self.session_mut(*r);
                if commit { s.commit_release() } else { s.abort_release() }
            } else {
                let _ = coord.release_remote(g, *r, commit).await; // best-effort
            }
        }
        Ok(QueryResult::Command { tag: if commit { "COMMIT".into() } else { "ROLLBACK".into() } })
    }
```

(Note: `Pin` is `Clone` not `Copy`; the `std::mem::replace(&mut self.pin, Pin::None)` at line 356 still yields an owned `Pin::Global { ranges, g }` to match on — keep it.)

- [ ] **Step 6: Implement `NetCoordinator` in `twopc.rs`**

```rust
use crate::range::router::GlobalCoordinator;
use executor::ExecError;

/// Networked coordinator: every global op is an RPC to the relevant range's
/// leader (range 0 for begin/commit, the participant range for stage/release).
/// Always RPCs — even to self via loopback — so the path is uniform.
pub struct NetCoordinator {
    client: Arc<TwoPcClient>,
}

impl NetCoordinator {
    pub fn new(client: Arc<TwoPcClient>) -> Self {
        Self { client }
    }
}

#[async_trait::async_trait]
impl GlobalCoordinator for NetCoordinator {
    async fn begin_global(&self) -> Result<u64, ExecError> {
        match self.client.call(0, TxnRpc::BeginGlobal).await {
            Ok(TxnResp::Began { g }) => Ok(g),
            Ok(TxnResp::NotLeader) => Err(ExecError::NotLeader),
            _ => Err(ExecError::Unavailable),
        }
    }
    async fn stage_remote(&self, g: u64, range: RangeId, sql: &str) -> Result<(), ExecError> {
        match self.client.call(range, TxnRpc::Stage { g, range, sql: sql.to_string() }).await {
            Ok(TxnResp::Staged) => Ok(()),
            Ok(TxnResp::NotLeader) => Err(ExecError::NotLeader),
            // Preserve the retryable serialization-failure class (correction H1) —
            // do NOT collapse 40001 into a non-retryable 0A000.
            Ok(TxnResp::Retryable) => Err(ExecError::SerializationFailure),
            Ok(TxnResp::Err(e)) => Err(ExecError::Unsupported(e)),
            _ => Err(ExecError::Unavailable),
        }
    }
    async fn commit_global(&self, g: u64, commit: bool) -> Result<(), ExecError> {
        match self.client.call(0, TxnRpc::CommitGlobal { g, commit }).await {
            Ok(TxnResp::Committed) => Ok(()),
            Ok(TxnResp::NotLeader) => Err(ExecError::NotLeader),
            _ => Err(ExecError::Unavailable),
        }
    }
    async fn release_remote(&self, g: u64, range: RangeId, commit: bool) -> Result<(), ExecError> {
        match self.client.call(range, TxnRpc::Release { g, range, commit }).await {
            Ok(TxnResp::Released) => Ok(()),
            Ok(TxnResp::NotLeader) => Err(ExecError::NotLeader),
            _ => Err(ExecError::Unavailable),
        }
    }
}
```

- [ ] **Step 7: Wire `NetCoordinator` into the gateway + `LocalCoordinator` into `MultiRangeCluster`**

- `server_node.rs spawn_sql_gateway` (line 280-311): build a `TwoPcClient::new(rafts.clone(), partition.clone())`, wrap as `Arc::new(NetCoordinator::new(client))`, and thread it through `serve_range_routed` → `RangeGatewayEngine::new` → `RangeRouter::new`. Add the `coordinator: Arc<dyn GlobalCoordinator>` param to `serve_range_routed` (route.rs:180) and `RangeGatewayEngine` (route.rs:129-152), cloned into each `connect()` (route.rs:157-173).
- `cluster.rs` in-process `RangeRouter::connect` (the harness path used by `crossrange_2pc.rs`): build `LocalCoordinator { range0: self.leader_engine(0).await }` (a GTM-bearing range-0 engine via the existing `gtm_source`) and pass `Some(Arc::new(local))`. This keeps `MultiRangeCluster`'s router escalating exactly as in SP16 (now through the seam, with durable begin_global).

- [ ] **Step 8: Run the escalation test + in-process regressions — expect PASS**

Run: `cargo nextest run -p cluster --test gateway_local gateway_escalates_a_cross_range_transaction_without_0a000` and `cargo nextest run -p cluster --test crossrange_2pc`
Expected: PASS — the gateway now escalates without `0A000` and commits (begin/commit RPC loopback-to-self exercises the wire; range-0 `a` reads back). The full both-rows / rollback visibility assertions land in T5 once durable-`gsnap` is wired. The in-process `crossrange_2pc` suite still passes through `LocalCoordinator`.

- [ ] **Step 9: Commit**

```bash
git add crates/cluster/src/range/router.rs crates/cluster/src/twopc.rs crates/cluster/src/range/cluster.rs crates/cluster/src/route.rs crates/cluster/src/server_node.rs crates/cluster/tests/gateway_local.rs
git commit -m "feat(sp17): gateway coordinator — GlobalCoordinator seam, escalate-on-remote-range, NetCoordinator

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Linearizable global-clog visibility — range-0 read barrier + durable `gsnap`

This is SP17's correctness core (the analog of SP16's deregister-at-prepare). Two pieces: (a) a per-statement **range-0 read barrier** so a participant's local range-0 replica is caught up before the global clog is read; (b) reconstruct `gsnap` from the participant's durable range-0 state (it has no in-memory GTM running set).

**Files:**
- Modify: `crates/executor/src/lib.rs` (engine field `range0_barrier: Option<Arc<dyn Linearizer>>`; `share_range0_barrier_to`; thread through `with_kv`/`replicated`/`clone_handle`/`connect`)
- Modify: `crates/executor/src/session.rs` (thread the field; `ensure_global_readable`; `durable_global_snapshot`; gate the read sites)
- Modify: `crates/cluster/src/twopc.rs` (a `Range0Barrier` impl of `executor::Linearizer` doing the GlobalBarrier RPC + local `wait().applied_index_at_least`)
- Modify: `crates/cluster/src/server_node.rs` (construct + inject the `Range0Barrier` into every data-range engine)
- Test: `crates/executor/src/exec.rs` `mod tests` — `durable_global_snapshot_resolves_committed_against_range0` (focused, deterministic; the barrier itself is proven end-to-end in Task 7)

- [ ] **Step 1: Write the failing test (durable-state gsnap reconstruction)**

The participant resolves a `Prepared(Li→g)` row against range 0's durable clog using a `gsnap` rebuilt from durable `next_global_xid` (not an in-memory running set). Add to `crates/executor/src/exec.rs` `mod tests` (mirrors the SP16 `eval_plan_qual_*` reconstruction test — build two in-memory `kv` stores, one for the local range, one for range 0's clog):

```rust
    #[test]
    fn durable_global_snapshot_resolves_committed_against_range0() {
        use kv::{Kv, MemKv};
        use mvcc::clog::{put_op, XidStatus};
        use mvcc::xid::GLOBAL_XID_BASE;
        let local = MemKv::new();   // this range's clog
        let global = MemKv::new();  // range 0's global clog + meta
        let g = GLOBAL_XID_BASE + 5;

        // Local row's deleter is Prepared(Li -> g); Li = 3.
        local.write_batch(&[put_op(3, XidStatus::Prepared(g))]).expect("local prepared");
        // Range 0: g committed, and next_global persisted past g (BIG-endian, the
        // exact on-disk layout the GTM allocator writes — correction C1).
        global.write_batch(&[put_op(g, XidStatus::Committed)]).expect("global committed");
        global
            .write_batch(&[kv::WriteOp::Put {
                key: kv::key::meta_next_global_xid_key(),
                value: (g + 1).to_be_bytes().to_vec(),
            }])
            .expect("persist next_global");

        // gsnap reconstructed from durable range-0 state: xmax = next_global, xip = [].
        let gsnap = crate::session::durable_global_snapshot(&global).expect("rebuild gsnap");
        let resolve = crate::exec::global_status(&local, &global, &gsnap);
        assert_eq!(resolve(3).expect("resolve"), XidStatus::Committed,
            "a committed cross-range deleter resolves Committed via range 0's durable clog");

        // A still-in-doubt g' (no decision, but allocated) resolves InProgress (invisible).
        let g2 = GLOBAL_XID_BASE + 6;
        local.write_batch(&[put_op(4, XidStatus::Prepared(g2))]).expect("local prepared 2");
        global
            .write_batch(&[kv::WriteOp::Put {
                key: kv::key::meta_next_global_xid_key(),
                value: (g2 + 1).to_be_bytes().to_vec(),
            }])
            .expect("advance next_global past g2");
        let gsnap2 = crate::session::durable_global_snapshot(&global).expect("rebuild gsnap2");
        let resolve2 = crate::exec::global_status(&local, &global, &gsnap2);
        assert_eq!(resolve2(4).expect("resolve g2"), XidStatus::InProgress,
            "an allocated-but-undecided cross-range deleter is invisible");
    }
```

(Store API verified: the in-memory KV is `kv::MemKv` with `MemKv::new()` + `.write_batch(&[WriteOp])` + `.get` — copy the setup from the SP16 `eval_plan_qual_settled_global_*` / `global_status_derefs_*` tests in this same module. `next_global_xid` is **big-endian** on disk (`gtm.rs` uses `zerocopy::byteorder::big_endian::U64`); `(v).to_be_bytes()` matches `U64::new(v).as_bytes()` for a `u64`, so the test exercises the real layout.)

- [ ] **Step 2: Run it — expect failure**

Run: `cargo nextest run -p executor durable_global_snapshot_resolves_committed_against_range0`
Expected: FAIL — `session::durable_global_snapshot` does not exist.

- [ ] **Step 3: Add `durable_global_snapshot` + reconstruct `gsnap` on participant nodes (`session.rs`)**

Add the shared big-endian decoder to `gtm.rs` (correction **C1/C2** — used by BOTH `Gtm::open` and the new `durable_global_snapshot`, so the layout cannot drift), then the free fn in `session.rs`:

```rust
// crates/executor/src/gtm.rs
pub(crate) fn read_next_global(kv: &dyn Kv) -> Result<u64, ExecError> {
    use mvcc::xid::GLOBAL_XID_BASE;
    use zerocopy::FromBytes;
    use zerocopy::byteorder::big_endian::U64; // MATCHES the writer next_global_xid_op
    let next = match kv.get(&kv::key::meta_next_global_xid_key())? {
        Some(b) => {
            let (v, _) = U64::read_from_prefix(b.as_slice())
                .map_err(|_| kv::KvError::CorruptRow("next_global_xid not u64".into()))?;
            v.get()
        }
        None => GLOBAL_XID_BASE,
    };
    Ok(next.max(GLOBAL_XID_BASE))
}
```
(Refactor `Gtm::open` at `gtm.rs:29-45` to call `read_next_global(&*kv)?` so there is a single decode site.)

```rust
// crates/executor/src/session.rs
/// Reconstruct the global visibility snapshot from range 0's DURABLE state (never
/// an in-memory running set — see correction C2). `xmax = next_global_xid`;
/// `xip = []` — a `g < xmax` is resolved by reading range 0's global clog directly
/// (absent ⇒ in-doubt). The caller must have barriered range 0's replica current
/// first.
pub(crate) fn durable_global_snapshot(range0: &dyn Kv) -> Result<Snapshot, ExecError> {
    use mvcc::xid::GLOBAL_XID_BASE;
    Ok(Snapshot { xmin: GLOBAL_XID_BASE, xmax: crate::gtm::read_next_global(range0)?, xip: vec![] })
}
```

Change `global_read_snapshot` (`session.rs:257-263`) so visibility is ALWAYS reconstructed from durable range-0 state — never `gtm.global_snapshot()` (correction **C2**):

```rust
    fn global_read_snapshot(&self, stored: Option<&Snapshot>) -> Snapshot {
        // RR reuses the snapshot captured (durably) at BEGIN.
        if let Some(s) = stored {
            return s.clone();
        }
        // Any engine that can see cross-range Prepared rows — the range-0 leader's
        // own GTM-bearing engine OR a participant data-range engine with a range-0
        // barrier — reconstructs gsnap from range 0's DURABLE state. The in-memory
        // GTM running set is NEVER consulted for visibility, so finish_global is a
        // no-op for correctness and a range-0 leader change is transparent.
        if self.gtm.is_some() || self.range0_barrier.is_some() {
            return durable_global_snapshot(&*self.catalog_kv)
                .unwrap_or_else(|_| crate::NO_GLOBAL_SNAPSHOT());
        }
        // Single-range engine: no global xids exist.
        crate::NO_GLOBAL_SNAPSHOT()
    }
```

And the RR BEGIN capture (`session.rs:183-187`) reconstructs durably too (after the range-0 barrier at `:177`): replace `self.gtm.as_ref().map(|g| g.global_snapshot())` with `Some(self.global_read_snapshot(None))` (guarded by `rr`).

- [ ] **Step 4: Add the engine field + thread it; add `ensure_global_readable`; gate the read sites**

- `SqlEngine` (`lib.rs:68`, beside `gtm`): `pub(crate) range0_barrier: Option<Arc<dyn crate::read_gate::Linearizer>>,`. Set `None` in `with_kv`/`replicated`; clone in `clone_handle` (line 161, beside `gtm`); pass into `connect` (line 273). Add:

```rust
    /// Inject a range-0 read barrier (a `Linearizer` over range 0's Raft handle)
    /// so this engine's reads catch range 0's replica up before resolving a
    /// cross-range `Prepared(-> g)` row. `None` ⇒ single-range / range-0 itself.
    pub fn share_range0_barrier_to(&self, other: &mut SqlEngine) {
        other.range0_barrier = self.range0_barrier.as_ref().map(Arc::clone);
    }
    pub fn set_range0_barrier(&mut self, b: Arc<dyn crate::read_gate::Linearizer>) {
        self.range0_barrier = Some(b);
    }
```

- `SqlSession` (`session.rs:67`, beside `gtm`): `range0_barrier: Option<Arc<dyn crate::read_gate::Linearizer>>,` + constructor arg (`session.rs:91`) + `connect` wiring. Add the helper:

```rust
    /// Catch range 0's local replica up (follower ReadIndex) before any read that
    /// may resolve a cross-range `Prepared(-> g)` row. No-op on a range-0 / single-
    /// range engine (`None`).
    async fn ensure_global_readable(&self) -> Result<(), ExecError> {
        if let Some(b) = &self.range0_barrier {
            b.ensure_readable().await?;
        }
        Ok(())
    }
```

- Gate the read sites: immediately **after** each existing own-range `self.linearizer.ensure_readable().await?` and **before** `global_read_snapshot(...)` / the `execute_*` calls, call `self.ensure_global_readable().await?`:
  - `read_context` Plan::Auto (after `session.rs:406`) and Plan::RcRefresh (after `:411`).
  - BEGIN / RR (after `session.rs:177`, before the global snapshot is fixed at `:184`).
  - `run_select_locking` (after `:300` and `:348`).
  - `run_write` UPDATE/DELETE: before the `global_read_snapshot` capture at `session.rs:462` (this path has no own-range gate today; add `self.ensure_global_readable().await?` just before capturing `gsnap`, because `eval_plan_qual` reads range 0's *settled* global clog).

- [ ] **Step 5: Implement `Range0Barrier` (`twopc.rs`) + inject it (`server_node.rs`)**

`Range0Barrier` implements `executor::Linearizer`: fetch range 0's linearizable applied index from its leader (GlobalBarrier RPC; or local `ensure_linearizable` when this node leads range 0), then wait for the local range-0 replica to apply through it:

```rust
use executor::Linearizer;

/// A follower-capable range-0 read barrier. `ensure_readable` fetches range 0's
/// linearizable applied index from its leader, then blocks until this node's
/// local range-0 replica has applied through it (openraft `wait`). This is what
/// makes a participant's `global_status` reads of range 0's clog correct.
pub struct Range0Barrier {
    range0: openraft::Raft<TypeConfig>,
    id: NodeId,
    client: Arc<TwoPcClient>,
}

impl Range0Barrier {
    pub fn new(range0: openraft::Raft<TypeConfig>, id: NodeId, client: Arc<TwoPcClient>) -> Self {
        Self { range0, id, client }
    }
}

#[async_trait::async_trait]
impl Linearizer for Range0Barrier {
    async fn ensure_readable(&self) -> Result<(), executor::ExecError> {
        // If we lead range 0, a local ReadIndex is authoritative.
        let leads0 = self.range0.metrics().borrow().current_leader == Some(self.id);
        let barrier_index = if leads0 {
            self.range0
                .ensure_linearizable()
                .await
                .map(|r| r.map(|l| l.index).unwrap_or(0))
                .map_err(|_| executor::ExecError::Unavailable)?
        } else {
            match self.client.call(0, TxnRpc::GlobalBarrier).await {
                Ok(TxnResp::Barrier { applied_index }) => applied_index,
                Ok(TxnResp::NotLeader) => return Err(executor::ExecError::NotLeader),
                _ => return Err(executor::ExecError::Unavailable),
            }
        };
        // Wait for our local replica to apply through the barrier (no sleep).
        self.range0
            .wait(Some(TXN_TIMEOUT))
            .applied_index_at_least(Some(barrier_index), "range-0 read barrier")
            .await
            .map(|_| ())
            .map_err(|_| executor::ExecError::Unavailable)
    }
}
```

Inject it in `server_node.rs` after the engines + `TwoPcClient` exist: build `let barrier = Arc::new(Range0Barrier::new(rafts[&0].clone(), cfg.id, client.clone()));` and `engine.set_range0_barrier(barrier.clone())` (or `r0_engine.share_range0_barrier_to(&mut engine)`) for **every data-range** engine before `Arc`-wrapping (range 0's own engine needs no barrier — it reads its own clog; leave its `range0_barrier` `None`). Because `clone_handle` propagates the field (lib.rs:161), the gateway's per-connection routers inherit it.

(Confirm openraft's `Wait::applied_index_at_least` signature — CLAUDE.md cites `.applied_index_at_least(idx, "reason")`; `idx` may be `Option<u64>` or `u64` in this version. Match `cluster::cluster`'s existing `wait().applied_index_at_least` usage exactly.)

- [ ] **Step 6: Add the gateway_local full atomic-commit + rollback tests (moved from T4 per H4)**

Now that durable-`gsnap` reconstruction is wired, range-1 (`b`) reads back. Add to `crates/cluster/tests/gateway_local.rs` (they reuse the `row_count` helper added in T4):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_commits_a_cross_range_transaction_atomically() {
    let (_node, sql_addr) = start_two_range_node().await;
    let port = sql_addr.rsplit(':').next().expect("port");
    let conn_str = format!("host=127.0.0.1 port={port} user=postgres");
    let (client, connection) = connect_with_retry(&conn_str).await;
    tokio::spawn(connection);
    client.simple_query("CREATE TABLE a (id int4)").await.expect("a");
    client.simple_query("CREATE TABLE b (id int4)").await.expect("b");
    client.simple_query("BEGIN").await.expect("begin");
    client.simple_query("INSERT INTO a VALUES (1)").await.expect("pin range 0");
    client.simple_query("INSERT INTO b VALUES (2)").await.expect("escalate range 1");
    client.simple_query("COMMIT").await.expect("atomic cross-range commit");
    // BOTH rows visible after COMMIT — range-1 `b` resolves via the durable gsnap.
    let a = client.simple_query("SELECT id FROM a").await.expect("select a");
    let b = client.simple_query("SELECT id FROM b").await.expect("select b");
    assert_eq!(row_count(&a), 1, "range-0 row committed");
    assert_eq!(row_count(&b), 1, "range-1 row committed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gateway_rolls_back_a_cross_range_transaction_atomically() {
    let (_node, sql_addr) = start_two_range_node().await;
    let port = sql_addr.rsplit(':').next().expect("port");
    let conn_str = format!("host=127.0.0.1 port={port} user=postgres");
    let (client, connection) = connect_with_retry(&conn_str).await;
    tokio::spawn(connection);
    client.simple_query("CREATE TABLE a (id int4)").await.expect("a");
    client.simple_query("CREATE TABLE b (id int4)").await.expect("b");
    client.simple_query("BEGIN").await.expect("begin");
    client.simple_query("INSERT INTO a VALUES (1)").await.expect("a");
    client.simple_query("INSERT INTO b VALUES (2)").await.expect("b");
    client.simple_query("ROLLBACK").await.expect("rollback");
    let a = client.simple_query("SELECT id FROM a").await.expect("select a");
    let b = client.simple_query("SELECT id FROM b").await.expect("select b");
    assert_eq!(row_count(&a), 0, "range-0 row rolled back");
    assert_eq!(row_count(&b), 0, "range-1 row rolled back");
}
```

- [ ] **Step 7: Run the focused unit test + the flip tests + regressions — expect PASS**

Run: `cargo nextest run -p executor durable_global_snapshot_resolves_committed_against_range0`, then `cargo nextest run -p executor`, then `cargo nextest run -p cluster --test gateway_local --test crossrange_2pc`.
Expected: PASS — the unit test exercises the durable-`gsnap` resolver; the gateway_local flip tests now see both rows (single node leads every range, so the barrier is a fast local ReadIndex and `durable_global_snapshot` reads the node's own current range-0 store); the in-process `crossrange_2pc` stays green (`LocalCoordinator` + `begin_global_durable`, durable-`gsnap` reconstruction matches the in-doubt logic). Also run `cargo nextest run -p cluster --test jepsen_bank` to confirm SP16 cross-range conservation still holds under the durable-`gsnap` change.

- [ ] **Step 8: Commit**

```bash
git add crates/executor/src/lib.rs crates/executor/src/session.rs crates/executor/src/exec.rs crates/executor/src/gtm.rs crates/cluster/src/twopc.rs crates/cluster/src/server_node.rs crates/cluster/tests/gateway_local.rs
git commit -m "feat(sp17): range-0 read barrier + durable-state global snapshot (cross-node visibility)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Release-on-leadership-loss + per-range-leaders control query

**Files:**
- Modify: `crates/cluster/src/transport/protocol.rs` (`ControlRequest::RangeLeaders` + `ControlResponse::RangeLeaders(Vec<(RangeId, Option<NodeId>)>)`)
- Modify: `crates/cluster/src/transport/server.rs` (`RangeRegistry` gets a public `groups()` iterator; the control dispatch passes `&registry`; `handle_control` answers `RangeLeaders`)
- Modify: `crates/cluster/src/server_node.rs` (a falling-edge release task per range, calling `TxnService::release_all_for_range`)
- Test: `crates/cluster/tests/gateway_local.rs` (`range_leaders_control_reports_each_range`) + an in-crate liveness assertion

- [ ] **Step 1: Write the failing test (per-range-leaders query)**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn range_leaders_control_reports_each_range() {
    use cluster::transport::protocol::{ControlRequest, ControlResponse, NodeRequest, NodeResponse};
    use cluster::transport::frame::{read_msg, write_msg};
    let (node, _sql) = start_two_range_node().await;
    // Dial this node's node port and ask for per-range leaders.
    let mut s = tokio::net::TcpStream::connect(&node_addr_of(&node)).await.expect("dial node port");
    write_msg(&mut s, &NodeRequest::Control(ControlRequest::RangeLeaders)).await.expect("send");
    let resp: NodeResponse = read_msg(&mut s).await.expect("recv");
    let leaders = match resp {
        NodeResponse::Control(ControlResponse::RangeLeaders(v)) => v,
        other => panic!("expected RangeLeaders, got {other:?}"),
    };
    // Both ranges report node 0 as leader (single self-bootstrapping node).
    assert_eq!(leaders.len(), 2, "two ranges reported");
    for (_r, leader) in leaders {
        assert_eq!(leader, Some(0), "single node leads every range");
    }
}
```

(Add a tiny `node_addr_of(&ServerNode) -> String` helper in the test, or expose the node's node-port addr — `ServerNode` already stores peers; if no accessor exists, capture the `node_addr` from `start_two_range_node` and return it alongside `sql_addr`. Adjust `start_two_range_node`'s return tuple to also yield the node addr.)

- [ ] **Step 2: Run it — expect failure**

Run: `cargo nextest run -p cluster --test gateway_local range_leaders_control_reports_each_range`
Expected: FAIL — `ControlRequest::RangeLeaders` / `ControlResponse::RangeLeaders` undefined.

- [ ] **Step 3: Add the control variants + registry iterator + handler**

- `protocol.rs`: add `RangeLeaders` to `ControlRequest` (line 38) and `RangeLeaders(Vec<(RangeId, Option<NodeId>)>)` to `ControlResponse` (line 54).
- `server.rs RangeRegistry`: add a public accessor (the existing `handle`/`node_id` are private):

```rust
    /// (range, current_leader) for every group registered on this node.
    pub fn group_leaders(&self) -> Vec<(RangeId, Option<NodeId>)> {
        let map = self.handles.lock().expect("range registry");
        let mut out: Vec<(RangeId, Option<NodeId>)> = map
            .iter()
            .map(|(&(range, _id), raft)| (range, raft.metrics().borrow().current_leader))
            .collect();
        out.sort_by_key(|&(r, _)| r);
        out
    }
```

- `serve_node_protocol` Control arm: the `RangeLeaders` request needs the whole registry (not just range 0's raft). Special-case it before `resolve(&registry, 0)`:

```rust
                    NodeRequest::Control(ControlRequest::RangeLeaders) => {
                        NodeResponse::Control(ControlResponse::RangeLeaders(registry.group_leaders()))
                    }
                    NodeRequest::Control(c) => { /* existing range-0 path */ }
```

- [ ] **Step 4: Add the release-on-leadership-loss task (`server_node.rs`)**

Spawn one watcher per range (beside `reseed_on_leadership`) that, on the **falling** edge of leadership for that range, calls `txn.release_all_for_range(range)`:

```rust
async fn release_on_leadership_loss(
    raft: openraft::Raft<TypeConfig>,
    range: RangeId,
    id: NodeId,
    txn: crate::twopc::TxnService,
) {
    let mut rx = raft.metrics();
    let mut was_leader = false;
    loop {
        let is_leader = rx.borrow().current_leader == Some(id);
        if was_leader && !is_leader {
            txn.release_all_for_range(range).await; // free held locks; presumed-abort
        }
        was_leader = is_leader;
        if rx.changed().await.is_err() {
            return;
        }
    }
}
```

Spawn it for each range after the `TxnService` is built (it shares the same `held` map via `TxnService::clone`). Wire at both bring-up paths (static `:204`+, replicated `:384`+).

- [ ] **Step 5: Run the control test + an in-crate liveness check — expect PASS**

Run: `cargo nextest run -p cluster --test gateway_local`
Expected: PASS. (A full kill-driven liveness assertion runs in the multi-process e2e, Task 7; the in-crate test here covers the control query + that the release task compiles and is spawned.)

- [ ] **Step 6: Commit**

```bash
git add crates/cluster/src/transport/protocol.rs crates/cluster/src/transport/server.rs crates/cluster/src/server_node.rs crates/cluster/tests/gateway_local.rs
git commit -m "feat(sp17): release-held-sessions-on-leadership-loss + per-range-leaders control query

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Multi-process cross-node 2PC e2e

**Files:**
- Create: `crates/crabgresql/tests/crossrange_2pc_net.rs` (UAC-safe; reuses `mod harness;`)
- Modify: `crates/crabgresql/tests/harness/mod.rs` only if a per-range-leaders helper is needed (add `range_leaders(id) -> Vec<(u32, Option<u64>)>` wrapping `ControlRequest::RangeLeaders`)

- [ ] **Step 1: Write the e2e (it is the test)**

```rust
//! D3c-net: a cross-range BEGIN..COMMIT issued at a gateway that does NOT lead
//! all participant ranges commits atomically across processes; ROLLBACK leaves
//! neither row; per-range failover keeps the cluster serving.
mod harness;
use harness::Cluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_range_txn_commits_atomically_through_a_nonleading_gateway() {
    // 3 processes, 2 ranges (boundary at table 2): table a (id 1) -> range 0,
    // table b (id 2) -> range 1.
    let c = Cluster::spawn_multirange(3, vec![2]).await;

    // Create + seed via any gateway (autocommit).
    let g = c.pg(0).await;
    g.simple_query("CREATE TABLE a (id int4)").await.expect("create a");
    g.simple_query("CREATE TABLE b (id int4)").await.expect("create b");

    // Pick a gateway that leads NEITHER range 0 nor range 1, if one exists; else
    // any node (the coordinator path still RPCs out for the non-led range).
    let gw = c.pick_nonleading_gateway(&[0, 1]).await;
    let client = c.pg(gw).await;
    client.simple_query("BEGIN").await.expect("begin");
    client.simple_query("INSERT INTO a VALUES (1)").await.expect("stage range 0");
    client.simple_query("INSERT INTO b VALUES (2)").await.expect("stage range 1 (escalates)");
    client.simple_query("COMMIT").await.expect("atomic cross-node commit");

    // Both rows are visible read back through EVERY node (barrier + global clog).
    c.wait_select_value("SELECT id FROM a", "1").await;
    c.wait_select_value("SELECT id FROM b", "2").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_range_txn_rolls_back_atomically() {
    let c = Cluster::spawn_multirange(3, vec![2]).await;
    let g = c.pg(0).await;
    g.simple_query("CREATE TABLE a (id int4)").await.expect("a");
    g.simple_query("CREATE TABLE b (id int4)").await.expect("b");
    let gw = c.pick_nonleading_gateway(&[0, 1]).await;
    let client = c.pg(gw).await;
    client.simple_query("BEGIN").await.expect("begin");
    client.simple_query("INSERT INTO a VALUES (1)").await.expect("a");
    client.simple_query("INSERT INTO b VALUES (2)").await.expect("b");
    client.simple_query("ROLLBACK").await.expect("rollback");
    // Neither row exists (poll a bounded window through live nodes).
    c.wait_select_value("SELECT count(*) FROM a", "0").await;
    c.wait_select_value("SELECT count(*) FROM b", "0").await;
}
```

Add the `pick_nonleading_gateway` helper to `harness/mod.rs` (uses the new `RangeLeaders` control query; falls back to node 0 if every node leads some target range):

```rust
/// A node id that leads NONE of `ranges` (so the gateway must coordinate
/// remotely), or 0 if no such node exists.
pub async fn pick_nonleading_gateway(&self, ranges: &[u32]) -> u64 {
    for n in &self.nodes {
        if let Some(ControlResponse::RangeLeaders(v)) =
            self.control(n.id, ControlRequest::RangeLeaders).await
        {
            let leads_any = v.iter().any(|(r, l)| ranges.contains(r) && *l == Some(n.id));
            if !leads_any {
                return n.id;
            }
        }
    }
    0
}
```

- [ ] **Step 2: Run it — iterate to green**

Run: `cargo nextest run -p crabgresql --test crossrange_2pc_net`
Expected: PASS. If a read-back flakes, confirm the barrier waits on the **local** range-0 replica (not a sleep) and that `wait_select_value` polls through live nodes (the permitted cross-process cadence). Do not add sleeps; if a wait cannot be expressed as a condition, add the instrumentation.

- [ ] **Step 3: Add the per-range failover variant**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_range_commit_survives_a_participant_leader_failover() {
    let mut c = Cluster::spawn_multirange(3, vec![2]).await;
    let g = c.pg(0).await;
    g.simple_query("CREATE TABLE a (id int4)").await.expect("a");
    g.simple_query("CREATE TABLE b (id int4)").await.expect("b");
    // Commit a cross-range txn.
    let gw = c.pick_nonleading_gateway(&[0, 1]).await;
    let client = c.pg(gw).await;
    client.simple_query("BEGIN").await.expect("begin");
    client.simple_query("INSERT INTO a VALUES (1)").await.expect("a");
    client.simple_query("INSERT INTO b VALUES (2)").await.expect("b");
    client.simple_query("COMMIT").await.expect("commit");
    // Kill range 1's leader; the row stays visible (durable global clog) and a new
    // cross-range txn still commits after re-election.
    let r1_leader = c.range_leader(1).await;
    c.kill(r1_leader);
    c.wait_select_value("SELECT id FROM b", "2").await; // survives failover
    let gw2 = c.pick_live_gateway().await;
    let c2 = c.pg(gw2).await;
    c2.simple_query("BEGIN").await.expect("begin");
    c2.simple_query("INSERT INTO a VALUES (3)").await.expect("a2");
    c2.simple_query("INSERT INTO b VALUES (4)").await.expect("b2");
    c2.simple_query("COMMIT").await.expect("commit after failover");
    c.wait_select_value("SELECT id FROM a WHERE id = 3", "3").await;
    c.wait_select_value("SELECT id FROM b WHERE id = 4", "4").await;
}
```

(Add small harness helpers `range_leader(range) -> u64` (via `RangeLeaders`) and `pick_live_gateway() -> u64` (first node that answers `status`), mirroring the existing `wait_for_leader` cadence. If killing range 1's leader also removes a quorum member needed by the gateway, choose the victim so a quorum remains — 3 nodes tolerate one loss.)

- [ ] **Step 4: Update CLAUDE.md UAC audit (carry into Task 8) + run the whole crabgresql test crate**

Run: `cargo nextest run -p crabgresql`
Expected: PASS (existing `multirange_gateway`, `multiprocess`, `jepsen_elle`, `meta_range_gateway` plus the new `crossrange_2pc_net`).

- [ ] **Step 5: Commit**

```bash
git add crates/crabgresql/tests/crossrange_2pc_net.rs crates/crabgresql/tests/harness/mod.rs
git commit -m "test(sp17): multi-process cross-node 2PC e2e (commit/rollback/participant failover)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Gauntlet + traceability + CLAUDE.md + finish

**Files:**
- Modify: `CLAUDE.md` (SP17 UAC-safe target audit line)
- Modify: `docs/superpowers/specs/2026-06-14-crabgresql-sp17-d3c-net-crossrange-2pc-network-design.md` (append a traceability table mapping each success criterion → task → test)

- [ ] **Step 1: UAC-safe target guard (returns empty when clean)**

Run:
```bash
git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'
```
Expected: empty (the new `crossrange_2pc_net.rs` contains none of the forbidden substrings). Confirm no new `[[test]]/[[bin]]/[[example]] name = "…"` entries were added with forbidden substrings.

- [ ] **Step 2: Add the SP17 line to CLAUDE.md**

Under the SP-history block, after the SP16 line, add:

```markdown
**SP17 (2026-06-14):** one new binary — `crabgresql::crossrange_2pc_net` (cross-range 2PC over the network e2e) — UAC-safe. The crabgresql list now also includes `crossrange_2pc_net`.
```

- [ ] **Step 3: Append the traceability table to the spec**

| # | Criterion | Task | Test |
|---|---|---|---|
| 1 | `NodeRequest::Txn` round-trips; BeginGlobal returns `g ≥ BASE`; CommitGlobal durably decides | T1, T2 | `twopc::tests::txn_rpc_round_trips_through_json`, `begin_global_durable_persists_next_global` |
| 2 | Stage/Release hold then free a per-G session across connections | T3 | `twopc::tests::stage_then_release_holds_then_frees_a_per_g_session` |
| 3 | Cross-range commit through a non-leading gateway is atomic; ROLLBACK leaves neither | T4, T7 | `gateway_commits_a_cross_range_transaction_atomically`, `cross_range_txn_commits_atomically_through_a_nonleading_gateway`, `*_rolls_back_*` |
| 4 | No partial visibility: committed cross-range version resolves; in-doubt invisible | T5 | `durable_global_snapshot_resolves_committed_against_range0` + the T7 read-back through every node |
| 5 | `gateway_local`'s former `0A000` now asserts atomic commit | T4 | `gateway_commits_a_cross_range_transaction_atomically` |
| 6 | Leadership loss releases held sessions; per-range leaders queryable | T6 | `range_leaders_control_reports_each_range` + T7 failover variant |
| 7 | All prior suites green; no new dep; gauntlet | T8 | full gauntlet below |
| 8 | Cross-node transfer commits + reads back through any node; per-range failover keeps serving | T7 | `cross_range_commit_survives_a_participant_leader_failover` |

- [ ] **Step 4: Run the full gauntlet**

Run each; all must be green:
```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
cargo test --workspace --doc
cargo deny check
```
Expected: all PASS. (If `cargo fmt --all --check` reports diffs, run `cargo fmt --all` and re-commit — implementers run clippy/test but not fmt; bake it in here.) Run the no-native guard if the repo has one (e.g. `check-no-native`).

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md docs/superpowers/specs/2026-06-14-crabgresql-sp17-d3c-net-crossrange-2pc-network-design.md
git commit -m "docs(sp17): traceability table + CLAUDE.md UAC audit for crossrange_2pc_net

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 6: Finish the branch**

Use superpowers:finishing-a-development-branch. Per standing preference: option 2 (push to a fresh non-force branch name + open a PR against `main`). PR body ends with the Claude Code generated-with line.

---

## Notes for the implementer

- **Stale IDE diagnostics:** rust-analyzer squiggles lag the committed tree. Trust `cargo clippy`/`cargo nextest`, never the editor's inline diagnostics.
- **No `sleep` in tests:** in-crate waits use openraft `wait().metrics(...)` / `applied_index_at_least`; the multi-process harness uses its existing bounded poll cadence (interval + deadline), never a settle-sleep. If a wait cannot be a condition, add instrumentation.
- **`Pin` is `Clone`, not `Copy`** — keep the `mem::replace` / `match &self.pin` idioms; do not reintroduce by-value uses.
- **`session_mut` panics on a missing local engine** (`router.rs:449`) — every remote-participant path must go through the coordinator (`stage_remote`/`release_remote`), never `session_mut`.
- **The barrier is the correctness core.** Leaving any participant read of range 0's clog ungated silently breaks cross-node atomicity. The focused unit test covers `gsnap`; the multi-process read-back-through-every-node (T7) is the end-to-end proof.
- **Endianness:** `next_global_xid` is `zerocopy` native-endian; reuse `gtm.rs`'s exact `U64` reader/writer so the durable-`gsnap` decode matches the allocator's encode on every target.
- **Confirm openraft signatures** against the in-tree version before pasting: `Raft::ensure_linearizable` return type (`Option<LogId>` vs `LogId`) and `Wait::applied_index_at_least` arg (`u64` vs `Option<u64>`) — mirror the existing `cluster::cluster` / `linearizer.rs` usage.

