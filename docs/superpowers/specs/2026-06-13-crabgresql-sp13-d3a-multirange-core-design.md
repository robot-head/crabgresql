# SP13 / D3a — In-process multi-range core (range map + per-range Raft groups + routing)

**Predecessors:** SP1–SP6 (single-node SQL/MVCC/concurrency), SP7 (single-range Raft, in-memory / D1), SP8 (durable Raft storage / D2a), SP9 (real network transport + multi-process nodes / D2b), SP10 (SQL leader routing / D2c), SP11 (over-the-wire serializability checking / D2d), SP12 (linearizable reads / D5) — all merged or in review.

**Goal:** Go from **one** Raft group covering the whole keyspace to **N co-located ranges**, each its own in-process Raft group and MVCC domain, with key→range routing of SQL. Prove multi-range consensus, key-routed reads/writes, and **per-range failover independence** in-process — the SP7→SP9 pattern (build the core in-process, add the network in a follow-up).

**Architecture:** A static, table-aligned `RangeMap` partitions the keyspace into N contiguous spans. A `MultiRangeCluster` runs N independent in-process Raft groups (reusing today's `Node`/`Cluster`/`Switchboard`), each with its own applied `sm_kv` + `SqlEngine` (so its own `procarray`/`seq`/`clog`). **Range 0** additionally holds the global catalog. A `RangeRouter` dispatches each SQL statement: DDL and catalog resolution to range 0; single-table DML to the table's data range, executing rows there with the schema resolved from range 0.

**Tech stack:** Rust 2024, openraft 0.9.24 (one `TypeConfig` group per range), existing `executor`/`cluster`/`catalog`/`kv` crates, stateright (tests). No new dependency. `#![forbid(unsafe_code)]`, pure-Rust unchanged. In-process only (no sockets — that's the next sub-slice).

---

## Where this sits: the D3 decomposition

D2 (distribution infra) is complete and D5 (linearizable reads) shipped. **D3 (range routing)** is the next roadmap spine — going from a single range to many. It's large, so it's decomposed:

| Sub-slice | Scope |
|---|---|
| **D3a = SP13 (this spec)** | **In-process multi-range core**: static range map, N co-located per-range Raft groups + MVCC, key→range SQL routing, single-range/single-table transactions. Tested in-process. |
| D3a-net | Network range routing: every node a gateway that forwards each statement to the target range's leader across nodes (the multi-process analog, built on `ServerNode`). |
| D3b | Cross-range read scatter-gather (a `SELECT` scanning multiple ranges fans out) + range descriptors move into a replicated **meta range** (vs static config). |
| later | Cross-range distributed transactions (2PC over Raft groups); dynamic placement / rebalancing. |
| D4 | Range splits (grow → split a span, each half its own Raft group). |

Everything below D3a in that table is **out of scope** for this slice.

## Decisions (locked during brainstorming)

1. **Partition: key-range, table-aligned.** Ranges are contiguous key spans whose boundaries fall on `table_id` values, so each table lives wholly in one range; a single-table operation is single-range. (Aligns with D4, which later splits a span *within* a table.)
2. **Placement: co-located.** Every node runs a Raft replica of every range (N groups per node). Routing is purely "which range's leader." True data-sharding-across-nodes is a later sub-slice.
3. **Reach: in-process core.** Reuse the `Switchboard`/`Cluster` harness; the router invokes the target range's leader engine directly (no cross-node forwarding). Network routing is D3a-net.
4. **Range 0 doubles as the system/catalog range** (catalog + `next_table_id` at the low system keys, plus data for tables in its span) — no dedicated empty system range.
5. **Single-range, single-table transactions.** Cross-range/multi-table statements are rejected with a clear deferred-feature error.

## Components

### 1. `RangeMap` — key→range addressing (`crates/cluster/src/range/map.rs`, new)

```rust
pub struct RangeId(pub u32);

/// A static, table-aligned partition of the key space into N contiguous spans.
/// `boundaries` are sorted table_id split points; range i covers
/// table_ids [boundaries[i-1], boundaries[i]) (range 0 starts at 0 and also
/// owns the sub-table system/catalog keys; the last range ends at +inf).
pub struct RangeMap { boundaries: Vec<catalog::TableId> }

impl RangeMap {
    pub fn with_boundaries(boundaries: Vec<catalog::TableId>) -> Self;
    pub fn range_count(&self) -> usize;
    pub fn range_for_table(&self, table_id: catalog::TableId) -> RangeId; // binary search
}
```

Pure, unit-testable, no I/O. `range_for_table` is a **routing rule** (which range owns a table's *data*), not a literal partition of one shared keyspace — see the storage-model note below.

### Storage model note (load-bearing)

Each range is a **separate** `sm_kv` (an independent store), not a disjoint slice of one keyspace. The existing key layout (`kv::key`) puts all MVCC/system metadata under the reserved `SYSTEM_TABLE_ID = 0` (`/0/catalog/<name>`, `/0/meta/next_table_id`, `/0/meta/next_xid`, `/0/clog/<xid>`, `/0/seq/<table_id>`), below user rows (`table_id ≥ 1`). Consequences for D3a:

- **Per-range MVCC namespace:** every range's kv independently uses the table-0 namespace for **its own** `next_xid`, clog, and the rowid `seq` of the tables it owns. xids/clog are range-local (fine — transactions are single-range). The table-0 keys are *not* shared across ranges; they're duplicated per store with range-local contents.
- **Range-0-authoritative globals:** the **catalog** (`/0/catalog/<name>`) and the **`next_table_id`** counter are authoritative **only in range 0's kv**. DDL allocates the globally-unique `table_id` and writes the schema there; a data range's kv has no catalog entries, which is exactly why a data-range op must resolve its `&Table` from range 0 (Component 3).

### 2. `MultiRangeCluster` — N co-located in-process Raft groups (`crates/cluster/src/range/cluster.rs`, new)

Builds N independent Raft groups over one shared `Switchboard`. Each range gets its own `sm_kv` + `SqlEngine` (constructed via `SqlEngine::replicated(catalog_kv, sm_kv, RaftCommitter{range_raft}, RaftLinearizer{range_raft})`, where `catalog_kv` is range 0's `sm_kv` — so each range has the SP12 linearizable-read gate and its own MVCC domain, and resolves schemas from range 0). The cluster exposes, per range, that range's current **leader engine** (analogous to SP7's `Cluster::leader()`), so the router/harness can execute against the right leader.

**Required `Switchboard` extension (range-aware routing).** Today the `Switchboard` registers Raft handles by `NodeId` (one group: `HashMap<NodeId, Raft>`). With N co-located groups, each node runs N Raft instances, so handle routing must demultiplex by **`(RangeId, NodeId)`**: `register(range, node, raft)`, `for_node(range, from) -> NodeFactory{range, from}`, and `Conn` resolves the target by `(range, target_node)`. **Faults stay node-scoped** (`pause`/`cut` keyed by `NodeId`) — a crashed/partitioned node realistically takes all its co-located range replicas with it; that is sufficient to demonstrate per-range failover independence (pause the node leading range R; range S, led by a different node, keeps quorum and serves). Range-scoped partitions are a possible later refinement, not needed for D3a.

Concretely this is N parallel instances of the existing single-range bring-up keyed by `RangeId`, sharing the node set and the (now range-aware) transport.

### 3. Executor change — resolve schema once, execute rows against the data range

Today `execute_read`/`execute_write`/`execute_read_locking`/`describe` call `catalog::get_table(kv, name)` against the *same* kv that holds rows. D3a separates the two with a **`catalog_kv` seam**: each function gains a `catalog_kv: &dyn Kv` parameter used only for the `get_table` lookup, while rows/seq/clog stay on the data `kv`. `SqlEngine`/`SqlSession` hold a `catalog_kv` handle. For a single-range engine `catalog_kv == kv` (behavior-preserving); a multi-range **data** engine points `catalog_kv` at **range 0's** `sm_kv` (co-located, locally readable). DDL is unchanged (runs wholly on range 0).

This is the only change to existing execution logic, and it's mechanical and behavior-preserving — the existing executor suite is the regression gate.

### 4. `RangeRouter` / multi-range session entry point (`crates/cluster/src/range/router.rs`, new)

The per-connection entry point that owns the `RangeMap` and per-range leader-engine handles. For each statement:

- **`CREATE TABLE` / `DROP TABLE`** → range 0's session (`execute_ddl` as today).
- **Single-table DML** → the target table is resolved from range 0's catalog (name→id→schema); `RangeMap::range_for_table(id)` → `RangeId`; the row op executes on that range's leader session, with `catalog_kv` pointed at range 0 for any schema lookup.
- **Cross-range transaction** (a txn whose statements map to >1 range) → `Err(ExecError::Unsupported(…D3b…))`. Note a *single* statement is never cross-range: the grammar has no joins and every DML carries one `table`.

Transactions: `BEGIN`/`COMMIT` are tracked per connection; all DML in a txn must map to the **same** range (the first DML pins the txn's range; a later statement on a different range errors). This keeps a transaction within one range's MVCC/Raft group.

## Data flow (single-table INSERT example)

1. `INSERT INTO orders …` arrives on the multi-range session.
2. Router resolves `orders` → `(table_id=7, schema)` from range 0's catalog (a local, linearizable read of range 0).
3. `RangeMap::range_for_table(7)` → `RangeId(2)`.
4. The op executes on range 2's **leader** engine: take range 2's snapshot/xid, write rows via range 2's `RaftCommitter` (range 2's Raft group), commit via range 2's clog. Range 0 and other ranges are untouched.

## Success criteria

| # | Criterion | Verified by |
|---|---|---|
| 1 | `RangeMap` maps table_ids to contiguous table-aligned ranges (binary search, boundary cases). | `range::map` unit tests |
| 2 | N co-located in-process Raft groups come up; each range elects a leader independently. | `MultiRangeCluster` bring-up test |
| 3 | A single-table write/read routes to and is served from the table's range only (other ranges untouched). | routing test: write to tables in different ranges, assert per-range `sm_kv` contents |
| 4 | DDL writes the catalog to range 0; DML resolves schema from range 0 and rows from the data range. | end-to-end create-then-insert-then-select across ranges |
| 5 | **Per-range failover independence**: killing range R's leader re-elects R and resumes, while range S (different leader) keeps serving. | failover test toggling one range's leader |
| 6 | A sharded workload (tables across ≥2 ranges) is per-range consistent under follower faults. | list-append/bank workload across ranges (D5 nemesis stable-window applied) |
| 7 | A transaction spanning ranges is rejected with the deferred-feature error. (A *single* statement is inherently single-table — the grammar has no joins — so it is never cross-range.) | negative test: a txn whose 2nd statement targets another range returns `Unsupported` |
| 8 | No new dependency; `#![forbid(unsafe_code)]`; full gauntlet green. | `cargo deny` + workspace clippy/test |

## Test plan

1. **`RangeMap` unit tests** — boundaries, binary search, the lowest range owning system/catalog keys, single-range degenerate case (N=1 behaves like today).
2. **Bring-up** — `MultiRangeCluster::new(3 nodes, N ranges)`; assert each range reaches a stable leader.
3. **Routing** — create tables that fall in different ranges; INSERT into each; assert rows land in the correct range's `sm_kv` and are absent from others; SELECT returns them.
4. **Per-range failover** — with range R and range S led by *different* nodes (elections are independent; the test waits for/selects this condition), pause the node leading range R. Assert range R re-elects among the surviving nodes and a subsequent op on R commits, while range S — still led by an unpaused node and holding quorum — commits throughout. Heal, and confirm R is fully back.
5. **Sharded consistency** — the SP11 list-append workload spread over tables in ≥2 ranges, follower-fault nemesis with a stable window (per the D5 lesson), checked per-range strict-serializable.
6. **Cross-range rejection** — a transaction whose second statement targets another range returns `Unsupported`. (Single statements can't be cross-range: the grammar has no joins and every DML carries one table.)
7. **Gauntlet** — `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace`; `cargo deny`; `check-no-native`; success-criteria traceability.

## Non-goals (explicit)

- **Network/multi-process range routing** (cross-node gateway leader-forwarding) — D3a-net.
- **Cross-range reads** (scatter-gather scans) and **multi-table** statements — D3b.
- **Cross-range distributed transactions** (2PC) — later.
- **Dynamic range descriptors / meta range** — D3b (D3a uses a static `RangeMap`).
- **Range splits / rebalancing** — D4 and beyond.
- **Data sharding across nodes** (non-co-located placement) — later sub-slice.

## Traceability (implemented)

| # | Criterion | Verified by |
|---|---|---|
| 1 | `RangeMap` maps table_ids to contiguous table-aligned ranges (binary search, boundaries) | `cluster::range::map::tests` (`one_range_covers_all_tables`, `boundaries_partition_table_ids_contiguously`, `boundaries_must_be_sorted_and_nonzero`) |
| 2 | N co-located in-process Raft groups come up; each range elects a leader independently | `range::cluster::tests::every_range_elects_a_leader` |
| 3 | A single-table write/read routes to and is served from its range only | `tests/multirange.rs::rows_land_only_in_their_table_range` (asserts per-range `sm_kv` contents on every node) |
| 4 | DDL writes the catalog to range 0; DML resolves schema from range 0 and rows from the data range | `range::router::tests::create_in_range0_insert_routes_to_data_range_select_reads_back` |
| 5 | Per-range failover independence: killing range R's leader re-elects R while range S keeps serving | `tests/multirange.rs::killing_one_range_leader_does_not_stop_another_range` (uses `wait_for_leader_excluding`) |
| 6 | A sharded workload (tables across ≥2 ranges) is per-range consistent under follower faults | `tests/multirange.rs::sharded_list_append_is_per_range_consistent_under_follower_faults` (1s stable-window nemesis, exactly-once appends) |
| 7 | A transaction spanning ranges is rejected (single statements are never cross-range) | `range::router::tests::a_transaction_may_not_span_ranges` (SQLSTATE `0A000`) |
| 8 | No new shipped dependency; `#![forbid(unsafe_code)]`; full gauntlet green | `cargo deny` + workspace clippy/test (only dev/internal deps — `catalog`/`pgparser` — added to the `cluster` crate) |

**Note (D3b):** a transaction that starts with range-0 no-table work (DDL / FROM-less SELECT) and later pins to a data range runs that statement auto-committed on the data range (it still commits durably and atomically through that range's Raft; only cross-range transactionality is loose). Documented in `router.rs`; tightened in D3b.
