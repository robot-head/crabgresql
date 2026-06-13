# crabgresql SP11: Over-the-wire serializability checking (distribution slice D2d)

**Date:** 2026-06-13
**Status:** Approved
**Program spec:** `docs/superpowers/specs/2026-06-11-crabgresql-program-and-wire-protocol-design.md`
**Predecessors:** SP1–SP6 (single-node SQL/MVCC/concurrency), SP7 (single-range Raft, in-memory / D1), SP8 (durable Raft storage / D2a), SP9 (real network transport + multi-process nodes / D2b), SP10 (SQL leader routing / D2c) — D2a–D2c merged or in review.

## Goal

Add **transactional serializability checking** over the real multi-process cluster
(D2b/D2c): run a transactional workload over the wire (real `crabgresql node`
processes, tokio-postgres clients) under a fault schedule, record the history, and
check it for serializability anomalies. This catches what the conservation-only
bank test cannot — a schedule can conserve a sum yet still be non-serializable.

This is the "real over-the-wire Jepsen (Elle)" deliverable SP7 deferred to D2. The
D2b/D2c Rust harness already *is* the Jepsen control plane — it spawns / kills /
partitions real OS processes and drives SQL via tokio-postgres — so D2d does **not**
add a Clojure control plane or ssh nemesis. It adds the workload, the history, the
checker, and the analysis.

Constraints unchanged: `#![forbid(unsafe_code)]`; pure-Rust; **no new dependency**.

## Scope of this slice (D2d) and what is deferred

D2 decomposes into D2a–D2d:

| Slice | Scope |
|---|---|
| D2a = SP8 (merged) | Durable Raft storage + restart recovery. |
| D2b = SP9 (merged) | Real TCP transport + multi-process nodes + harness + crash/partition bank nemesis. |
| D2c = SP10 (in review) | SQL leader routing (any node is a usable Postgres endpoint). |
| **D2d = SP11 (this spec)** | **Over-the-wire serializability checking** — a list-append workload + a stateright strict-serializability checker over the real cluster, with a passing gate and a D5 gap-finder. |

D3 (range routing), D4 (splits), **D5** (leases / linearizable reads — the gap this
slice documents), an MVCC vacuum/GC slice, and cross-range distributed transactions
remain later sub-projects.

**Test-only slice.** D2d adds **no production code** — the SQL/MVCC engine, leader
routing, Raft transport, and durable storage are exercised as-is. Everything new is
test code + test-support (the workload, the history recorder, the stateright
reference object, the EDN emitter, the scenarios). The in-process `jepsen_bank.rs`
(conservation + register linearizability) and the D2b/D2c multiprocess suite are
untouched.

## Architecture

### Checker — stateright (the vetted crate, extended to transactions)

`jepsen_bank.rs` already uses stateright's `LinearizabilityTester` over a
`register::Register` reference object for the single-register test. D2d reuses that
pattern for **transactions**: a custom test-support reference object implementing
stateright's sequential-spec trait (the one `Register` implements) models the
database as `key → Vec<value>`, with two atomic operations:

- `Append(key, val)` — append `val` to `key`'s list.
- `Read(key) → Vec<value>` — return `key`'s current list.

Each SQL transaction becomes **one** atomic operation against that object, so
`LinearizabilityTester` checking the recorded history == checking **strict
serializability** (linearizability of atomic transactions is strict
serializability). Pure-Rust, no JVM, no reinvented checker.

**Teeth.** Like the register test's `stateright_rejects_a_known_nonlinearizable_*`
guard, a synthetic positive-control meta-test feeds a known non-serializable history
(e.g. a read that observes a list missing an acked append) and asserts the checker
**rejects** it — so a vacuously-accepting checker is ruled out. Scenario B
(below) provides a second teeth-check on a *real* anomaly.

**Bounded history.** stateright's linearizability search is exponential in the worst
case, so the history is intentionally small (a few clients, a few keys, a handful of
appends each) — exactly as the existing register test keeps its workload small.
Larger histories are the use case for the EDN export → real Elle (below).

### Workload — single-key list-append (serializable-by-construction)

Strict serializability is *stronger* than the engine's guarantee (REPEATABLE READ is
snapshot isolation, which permits write-skew / G2; there is no SSI). So the workload
must avoid cross-key write-skew: each transaction appends a globally-unique value to
**one** key and reads that key's list back:

```sql
BEGIN;
INSERT INTO appends(key, val) VALUES (:k, :v);
SELECT val FROM appends WHERE key = :k;
COMMIT;
```

**Ordering mechanism (verified against the engine).** crabgresql has **no** SQL
`SERIAL`/`SEQUENCE`/`nextval`. Instead, every INSERT is assigned a monotonic
internal **rowid** by the `SequenceManager`, allocated on the leader at execution
time — i.e. in commit order — and rows are stored kv-sorted by `row_key(table, rowid)`.
A table scan (`scan_live` → `kv.scan_prefix(table_prefix)`) therefore returns rows in
rowid order, so the unordered `SELECT val FROM appends WHERE key = :k` yields key
`:k`'s appends **in commit order** with no sequence column and no `ORDER BY`. That
commit order is the leader's serialization of the Raft log, so the read returns the
list in its true applied order; the test's stateright reference object treats that
read order as the linearization order. (The list order thus depends on the engine's
rowid-ordered scan — the actual storage behavior the test exercises.)

Because all writes flow through one Raft log and a single key has no write-skew, such
a history *is* strict-serializable — so the check **should pass**, and any
replication / routing / MVCC violation (notably a stale read) shows up as a hole in
an observed list.

**Indeterminate outcomes** are handled as the register test does: an indeterminate
COMMIT → the append is recorded as an invoke-with-no-return (in-flight; the tester
may place it anywhere or omit it); an indeterminate / timed-out read → no
observation. The history stays honest; nothing is dropped silently as "ok".

### Scenario A — leader-fixed passing gate

Spawn 3 durable nodes; the leader stays fixed (followers-only crash/restart +
minority partitions, **one fault at a time** so the leader keeps quorum — the
D2b/D2c robustness choice). A few tokio-postgres clients connect to **random** nodes
(exercising D2c routing) and run single-key list-append transactions over a small key
set, recording `{append(key,val), read(key)→observed list, outcome}` each. The
nemesis runs inline (followers-only, `is_finished()` termination). After heal, the
history is fed to the list-append reference object + `LinearizabilityTester` and we
**assert `is_consistent()`** — strict serializability holds over the real wire under
the faults the system tolerates. This is the green gate.

### Scenario B — leader-failover gap-finder (documents D5)

Surfaces a stale read deterministically via control-channel orchestration:

1. Seed key K (append v1 → committed; K = `[v1]`).
2. `SetPartition` to isolate the current leader **L** from the other two. L still
   self-reports leader (`state == Leader`) for ~`election_timeout` (1–2 s with the
   long timers) before it loses quorum and steps down.
3. The surviving majority elects **L'**; an append v2 to K is committed via L'
   (K = `[v1, v2]`, acked).
4. **Within the window**, the harness connects directly to L's SQL port and reads K.
   L self-reports leader, so `serve_routed` serves locally from L's stale applied
   state → returns `[v1]`, missing the acked v2 — a stale read. (The app-layer
   partition only drops inter-node Raft RPCs; the harness→L SQL connection still
   reaches L.)
5. History: v2 acked on L'; read observed `[v1]`. The checker finds the violation (a
   read missing an acked append → not linearizable) and we **assert `!is_consistent()`**
   — i.e. assert the D5 stale-read gap *is present*.

This is a *passing* test that pins the current (gap-having) behavior. When D5 adds a
read-index / leader lease, the stale read disappears and the assertion flips to
`is_consistent()` — a clean TDD handoff to D5. It also gives the checker a second
teeth-check on a *real* anomaly.

**Determinism.** The ~1–2 s window (step 4 before L steps down, with
`election_timeout` 1000–2000 ms) is wide for a single read; the orchestration is
bounded-retry — if L already stepped down (the read routes away or returns fresh),
redo isolate→commit→read (bounded attempts). Reliable, not flaky.

### EDN export (the "Elle" artifact, light)

A `history_to_elle_edn(&[HistEntry]) -> String` emitter produces the jepsen/elle
`:list-append` op-map format
(`{:process p :type :ok :f :txn :value [[:append k v] [:r k [list...]]]}`), written
to a file as a test artifact, plus a unit test validating the format on a sample
history. This fulfills SP7's "Elle-exportable histories" promise and lets anyone run
*real* Elle offline on a larger history — but the **in-CI gate is stateright**; no
JVM/Clojure is wired into CI. EDN is hand-formatted (no new dependency).

## Testing

A new `crates/crabgresql/tests/jepsen_elle.rs` holds: the list-append workload + the
history recorder, the stateright list-append reference object, the EDN emitter, and
the tests — Scenario A (passing gate), Scenario B (D5 gap-finder), the synthetic
positive-control meta-test, and the EDN-format unit test. It reuses the D2b/D2c
harness (`crates/crabgresql/tests/harness/mod.rs`); a small harness addition is
allowed if needed (a helper to read a key's ordered list, or to connect to a specific
node for the gap-finder). The in-process `jepsen_bank.rs` and the D2b/D2c
`multiprocess.rs` suites stay green.

All waits bounded (`wait_for_leader` / `status` polling / `tokio::time::timeout`); no
fixed correctness sleeps beyond poll backoffs; the nemesis uses `is_finished()`
termination. Run under `__COMPAT_LAYER=RunAsInvoker` on the Windows dev box; the
multiprocess suite runs on Linux CI.

## Crate structure

- `crates/crabgresql/tests/jepsen_elle.rs` **(new)** — workload, recorder, stateright list-append reference object, EDN emitter, scenarios A/B, positive control, EDN-format test.
- `crates/crabgresql/tests/harness/mod.rs` — small additions only if needed (read-list helper / connect-to-specific-node).
- No production-code change.

## Dependencies & purity

None new. `stateright`, `tokio-postgres`, `tempfile` are already dev-dependencies;
the EDN emitter is hand-formatted. `#![forbid(unsafe_code)]` intact; `cargo-deny`
and `scripts/check-no-native.sh` unaffected (modulo the pre-existing `windows-sys`
Windows-only false-positive).

## Success criteria

1. A single-key list-append workload runs over the real multi-process cluster; the recorded history is checked for **strict serializability** via a stateright list-append reference object.
2. **Scenario A** (leader-fixed, followers-only faults + routing) **passes** — the committed path is strict-serializable over the real wire.
3. The checker has **teeth** — a synthetic non-serializable history is **rejected** (positive control).
4. **Scenario B** (leader-failover) surfaces a stale read and the checker **flags it** — asserting the **D5 gap is present** (the assertion flips when D5 lands).
5. An Elle-compatible **EDN artifact** is emitted (offline real-Elle cross-validation enabled).
6. Pure-Rust, **no new dependency**, **no production-code change**; full gauntlet green; in-process (D1) + D2b/D2c multiprocess suites still pass.

## Deferred

Real Elle in CI (JVM/Clojure — the EDN export enables it offline); SI/RC-specific
anomaly-class checking (D2d checks strict serializability of a serializable-by-
construction workload); larger histories (stateright's search is bounded → small;
real Elle for scale); **D5** itself (read-index / leases — the fix for the gap
Scenario B documents); range routing / splits (D3–D4).
