# SP15 / D3b-meta — Replicated range descriptors (relocate-only)

**Predecessors:** SP1–SP6 (single-node SQL/MVCC/concurrency), SP7 (single-range Raft / D1), SP8 (durable Raft storage / D2a), SP9 (real TCP transport + multi-process `ServerNode` / D2b), SP10 (SQL leader routing / D2c), SP11 (over-the-wire serializability / D2d), SP12 (linearizable reads / D5), SP13 (in-process multi-range core — `RangeMap`, `MultiRangeCluster`, `RangeRouter` / D3a), **SP14 (network range routing — multi-range durable `ServerNode`, range-aware TCP transport, per-statement gateway forwarding / D3a-net)** — all merged or in review.

**Goal:** Move the range layout from **static `--range-boundaries` config** into a **Raft-replicated descriptor record** committed in range 0. Every node sources its routing boundaries from that committed record instead of trusting that the operator handed every node identical flags. This closes the split-brain-routing hazard — two gateways configured with disagreeing boundaries route the same table to different ranges — by making the boundaries replicated truth that every node reads from one place. This is the first piece of D3b (range descriptors move into a replicated meta range), cut **relocate-only**: descriptors become replicated state read at bootstrap, but are written once at cluster-create and are immutable thereafter. Runtime mutation (splits/moves) and the lifecycle machinery it needs are explicitly deferred to D4.

**Architecture:** Three pieces. (1) A **descriptor data model** — `RangeDescriptor { range_id, start_table_id, end }` and a versioned blob encoding, with `RangeMap` gaining the ability to serialize itself to / reconstruct itself from that blob; `RangeMap`'s routing API (`range_for_table` / `range_ids` / `range_count`) is unchanged, so it becomes a **cache built from replicated state** rather than from CLI. (2) A **meta-range descriptor store** — the blob lives under a new system key `/0/meta/range_map` in range 0's keyspace, written + read through the existing range-0 KV seam, exactly like the catalog. (3) A **two-phase `ServerNode` bootstrap** in a new *Replicated* layout mode: bring up range 0 first from a minimal static seed, (bootstrap node only) write the seed boundaries into the meta blob, then every node waits for the committed blob to apply locally, decodes the authoritative `RangeMap`, and brings up the data ranges it names. A default *Static* mode preserves today's single-pass bring-up verbatim, keeping the entire SP9/SP10/SP13/SP14 regression surface green.

**Tech stack:** Rust 2024, openraft 0.9.24, existing `cluster`/`executor`/`catalog`/`kv` crates, fjall (per-range keyspaces). **No new shipped dependency.** `#![forbid(unsafe_code)]`, pure-Rust unchanged. Tests: pure unit (serde), single-process durable `ServerNode` (deterministic bring-up + routing), and the multi-process harness (the cross-process "node learns layout from the meta range" proof — UAC-safe binary name, see §Risks).

---

## Where this sits: the D3 decomposition

| Sub-slice | Scope |
|---|---|
| D3a = SP13 | In-process multi-range core: static range map, N co-located per-range Raft groups + MVCC, key→range SQL routing, single-range transactions. ✅ |
| D3a-net = SP14 | Network range routing: multi-range durable `ServerNode` + range-aware TCP transport + per-statement gateway forwarding to the owning range's leader. ✅ (in review) |
| **D3b-meta = SP15 (this spec)** | **Replicated range descriptors (relocate-only):** the table→range layout moves from static config into a Raft-replicated blob in range 0; every node reads its boundaries from that committed record at bootstrap. Descriptors are immutable after cluster-create. |
| D3b-rest | Cross-range read scatter-gather (`SELECT` fans out across ranges) + a structured query RPC + prepared-statements-across-ranges. |
| later | Cross-range distributed transactions (2PC over Raft groups); dynamic placement / rebalancing. |
| D4 | **Range splits** — and with them: *runtime* descriptor mutation, dynamic keyspace creation after serving, leader-confirmed descriptor reads, per-descriptor epochs / cache invalidation, stable non-positional `range_id` allocation on split. |

Everything below D3b-meta in that table is **out of scope** for this slice.

## The load-bearing constraint (why relocate-only is the right cut)

Today the range layout is consumed in two structurally different ways, and only one of them is hard to make dynamic:

1. **Routing** (`RangeRouter::range_of`, `range/router.rs:187-190`): a *synchronous, per-statement* `map.range_for_table(table_id)` on the hot path. Relocating this is cheap — keep `RangeMap`'s synchronous API, just build the map from the replicated blob instead of CLI.
2. **Physical bring-up** (`NodeStore::open` `durable.rs:38-51`, the `ServerNode::start` per-range loop `server_node.rs:141-177`): the range *set* is enumerated once and frozen — keyspaces (`data-r{r}`/`raft-r{r}`) are created up front, one Raft group + engine is built per range, and `keyspaces()` (`durable.rs:55-59`) panics for any range not opened at construction.

Making the **routing map** replicated is the whole value (it closes the split-brain hazard) and is contained. Making the **range set itself mutate at runtime** — a range appearing after the node is already serving — forces keyspaces, Raft groups, engines, and the leadership/forward maps to all become add-after-start, a `ServerNode` lifecycle refactor. That refactor only pays off when there is a *reason* for the set to change at runtime: **range splits (D4)**. So this slice relocates the layout into replicated state and reads it at a **two-phase bootstrap** (still "build a fixed set, then serve" — just sourced from the meta range), and leaves runtime mutation to D4. **A reviewer or implementer who reaches for runtime `add_range` / dynamic keyspace creation is building D4** — the spec locks the bootstrap-time read.

A second consequence: because descriptors are immutable after create, a node may read the blob from its **own local range-0 applied store** (after waiting for the committed write to apply) and trust it — the value never changes, so a local read of the applied value is correct. This avoids adding the first non-Raft/non-Control structured RPC or a SQL-`SELECT`-over-pgwire descriptor fetch. When descriptors become mutable in D4, reads must upgrade to leader-confirmed; that upgrade is called out in Non-goals.

## Decisions (locked during brainstorming)

1. **Scope = relocate-only.** Descriptors become replicated state read at bootstrap; they are written once at cluster-create and immutable thereafter. Runtime mutation / splits / dynamic keyspace lifecycle → D4.
2. **Home = range 0, single committed blob.** The descriptor set lives under one new system key `/0/meta/range_map` in range 0's keyspace, committed through range 0's existing Raft group — alongside the catalog, `next_table_id`, sequences, and clog (all under `SYSTEM_TABLE_ID=0`, `kv/src/key.rs:26-69`). No dedicated meta range (which would add a second metadata consensus group and a "who describes the descriptor range?" meta-of-meta bootstrap). One blob, not one-key-per-range: relocate-only reads the whole set once and writes it atomically; the per-key form pays off only for incremental split mutation (D4 may migrate it).
3. **Descriptor contents = `range_id` + table-id span + version byte. No membership.** Placement is co-located (every node hosts every range) and per-range Raft membership is *already* openraft-replicated and durable (`SM_MEMBERSHIP_KEY`, `durable.rs:581-597`). Carrying members/addresses in the descriptor would duplicate that and risk drift. Routing needs only table-id-span → `range_id`; the range's leader/address is resolved at runtime from its own Raft metrics, exactly as today (`forward.rs:113-135`).
4. **Read = event-based "blob present" wait, then local read.** Each node, after bringing up its range-0 replica, waits (bounded, event-driven on openraft applied state — *no sleep*) until the committed descriptor blob is present in its local range-0 applied store, then reads it locally. Correct because the blob is immutable this slice. No new RPC, no SQL forward, no per-statement quorum read.
5. **Mode switch = explicit `NodeConfig` layout mode; default Static.** `RangeLayout::Static(RangeMap)` is today's path verbatim (the load-bearing SP9/SP10/SP13/SP14 regression gate); `RangeLayout::Replicated { seed: Option<RangeMap> }` is the new two-phase bootstrap. "Where boundaries come from" is a genuinely separate axis from "how many ranges," so an explicit mode is the honest cut (unlike SP14, where `RangeMap::single()`'s N=1 *was* the fast-path switch). Default `Static(RangeMap::single())` preserves every existing path.
6. **Stay narrow — catalog reads untouched.** Catalog reads remain today's direct local `Kv::get` (`server_node.rs:135`, un-linearized). Closing that known staleness gap shares the range-0 read seam but is a separate decision, deferred.
7. **CLI `--range-boundaries` becomes seed-only.** In Replicated mode it seeds the initial blob *on the bootstrap node only*; a joining node does not need it, and a joining node given a *wrong* value still routes by the replicated truth (criterion 4). A new `--replicated-ranges` flag selects Replicated mode.

**Internal decisions (locked, with resolution):**

- **The minimal static seed for Replicated mode** is: this node's `id`, the peer address book (`Vec<(NodeId, "node|sql")>` — openraft needs addresses to dial, as today `server_node.rs:30`), the `bootstrap` flag, and the implicit constant **"the meta range is range 0, voters = the node set."** Range 0 is always co-located and always present, so it needs no descriptor to bring itself up — its self-layout is the bootstrap constant (matching `map.rs`'s invariant that range 0 always starts at table 0). Everything else (the data-range boundaries) is read from the blob.
- **`RangeMap` gains a versioned blob codec, not `serde::Serialize`.** `to_descriptor_bytes(&self) -> Vec<u8>` and `from_descriptor_bytes(&[u8]) -> Result<RangeMap, DescriptorError>` mirror `catalog/src/serde.rs` (version byte + count + per-range `range_id`/`start`/`end`). The decode path returns `Result` (a corrupt or forward-version blob degrades to a clean error, never the `with_boundaries` panic at `map.rs:31-41`). `range_id` is stored in the descriptor; at create it equals the positional index, but storing it explicitly decouples the concept from position so D4 can allocate split ids without renumbering — `range_ids()` yields the stored ids.
- **A `version` field is stored but routing does not act on it.** It is a forward-compatibility / D4 hook (epoch-based cache invalidation lives with mutation); this slice never invalidates a cache because descriptors never change.
- **`NodeStore` gains range-0-first / on-demand keyspace open.** `NodeStore::open_range(dir, range)` (or `ensure_range`) creates+opens a single range's `data-r{r}`/`raft-r{r}` pair on demand, reusing the retained `Arc<Database>` (`durable.rs:31`). Replicated-mode `start()` opens range 0 first, reads the blob, then opens each data range. This is *deferred-to-after-meta-read* creation at startup — **not** runtime "appears while serving" creation (that's D4). Static mode keeps `NodeStore::open(dir, &RangeMap)` opening everything up front.
- **The bootstrap node writes the seed blob as range 0's leader.** After bringing up + initializing range 0, the bootstrap node waits (event-based) for a range-0 leader and, if the blob is absent, the leader commits it through range 0's `RaftCommitter`. The write is a create-if-absent (idempotent across restart): a restarting bootstrap node finds the blob already present and skips the seed.
- **The gateway is always used in Replicated mode.** `server_node.rs:205`'s static `range_count > 1 ? serve_range_routed : serve_routed` branch stays a one-time decision *because the set is fixed at the end of the two-phase bootstrap*; Replicated mode routes through the gateway regardless of count. The single-range byte-proxy fast-path remains the Static-mode path (the SP9/SP10 regression gate).
- **Resolve the latent `node.rs:102` hazard if touched.** `Node::start_durable` opens `NodeStore` with `RangeMap::single()` then opens a log/SM for an arbitrary `range` arg (`node.rs:102-104`), which would panic in `keyspaces()` for a non-zero range. This slice does not need to fix it, but `open_range` makes the correct fix trivial; note it, fix only if a task lands on that path.

## Components

### 1. Descriptor data model (`crates/cluster/src/range/map.rs`, new `descriptor.rs`)

`RangeDescriptor { range_id: RangeId, start_table_id: TableId, end: Option<TableId> }` reproduces today's half-open table-id span `[start, end)` (`end == None` = unbounded last range), matching `range_for_table`'s `partition_point` semantics (`map.rs:49-52`). A `RangeMap` is equivalently a `Vec<RangeDescriptor>` ordered by `start_table_id`. New codec: `to_descriptor_bytes` / `from_descriptor_bytes(...) -> Result<RangeMap, DescriptorError>` (versioned, mirroring `catalog/src/serde.rs`). `RangeMap`'s existing routing API (`range_for_table`, `range_ids`, `range_count`, `single`) is **unchanged** — it now has two constructors (`with_boundaries` for Static/seed config; `from_descriptor_bytes` for Replicated reads) feeding the same internal representation. This is a pure, fully unit-testable addition with no I/O.

### 2. Meta-range descriptor store (`crates/kv/src/key.rs`, `crates/cluster/src/range/meta_store.rs` or inline)

A new system key `meta_range_map_key()` under the existing `/0/meta/` tag (`kv/src/key.rs:51-69` is the precedent — `next_table_id`, `next_xid`, clog all live there). `write_range_map(committer, &RangeMap)` commits the blob through range 0's write seam (the same `RaftCommitter`/`WriteBatch` path `create_table` uses, `catalog/src/lib.rs:65-101`); `read_range_map(kv) -> Result<Option<RangeMap>>` is a single `Kv::get` + `from_descriptor_bytes`. The **"blob present" wait** is an event-based bounded condition on range 0's openraft applied state (await applied-index / key-present, the `wait()` pattern from `Cluster::wait_for_leader`), never a sleep.

### 3. Two-phase `ServerNode` bootstrap (`crates/cluster/src/server_node.rs`, `durable.rs`, `crabgresql/src/main.rs`)

`NodeConfig` gains `layout: RangeLayout` (`Static(RangeMap)` default, or `Replicated { seed: Option<RangeMap> }`). `ServerNode::start` (`server_node.rs:124-253`) branches:

- **Static:** today's path verbatim — `NodeStore::open(dir, &map)`, single-pass per-range loop, `serve_routed` for N=1 / `serve_range_routed` for N>1. Zero behavior change.
- **Replicated:**
  1. `NodeStore::open_range(dir, 0)`; build + bootstrap range 0's Raft from the static seed (peers + node set), wire `catalog_kv = store.data_kv(0)` as today (`server_node.rs:135`).
  2. If `bootstrap` and the meta blob is absent: wait (event-based) for a range-0 leader, then commit the seed `RangeMap` (from `cfg.layout`'s `seed`, i.e. the CLI `--range-boundaries`) to `/0/meta/range_map`.
  3. **All nodes:** wait (event-based, bounded) until the committed blob is present in the local range-0 applied store; decode it → the authoritative `RangeMap`.
  4. For each data range in the authoritative map (`range_ids()` minus 0): `open_range`, build + bootstrap its Raft (membership = the node set, as today), build its replicated engine (`catalog_kv` = range 0's store, as today).
  5. Serve via `serve_range_routed` (the gateway) regardless of count.

`crabgresql/src/main.rs` (`run_node`, `139-194`): a new `--replicated-ranges` flag selects Replicated mode; `--range-boundaries` populates the `seed` (used only on the bootstrap node). The gateway (`route.rs`, `range/router.rs`) is **unchanged** — it already routes off a `RangeMap`; that map now originates from the blob.

## Data flow (Replicated-mode bring-up)

Table ids are allocated densely from 1 (`catalog`'s `next_table_id` defaults to 1, `lib.rs:145-154`), so demonstrative boundaries are small: boundary `[2]` ⇒ range 0 owns table ids `[0,2)` (system id 0 + the first user table, id 1), range 1 owns `[2,∞)` (the second user table, id 2, onward).

1. Node 1 (`--bootstrap --replicated-ranges --range-boundaries 2`) starts: opens `data-r0`/`raft-r0`, builds range 0's Raft, `initialize`s it with the node set.
2. Node 1 sees a range-0 leader, finds `/0/meta/range_map` absent, and commits the seed `RangeMap::with_boundaries([2])` (ranges 0 = tables `[0,2)`, 1 = tables `[2,∞)`) through range 0's Raft.
3. Nodes 2 and 3 (`--replicated-ranges`, **no `--range-boundaries`**) start: each opens range 0, joins range 0's group, and waits until the committed blob applies to its local range-0 store.
4. Every node decodes the blob → the same authoritative `RangeMap` (2 ranges). Each then `open_range`s `data-r1`/`raft-r1`, builds range 1's Raft + engine, and serves the gateway.
5. A client connects to any node and issues `CREATE TABLE a` (id 1 → range 0) and `CREATE TABLE b` (id 2 → range 1), then `INSERT INTO b …`: the gateway routes by the *replicated* boundaries to range 1's leader. Node 2 — which was never told `2` — routes identically, because it read the layout from the meta range (criteria 3, 4, 7).

## Tasks (legend)

- **T1** Descriptor model + versioned blob codec (`RangeDescriptor`, `to_/from_descriptor_bytes`, `DescriptorError`); `RangeMap` API preserved. Pure unit tests.
- **T2** Meta-range descriptor store (system key; `write_range_map`/`read_range_map`; event-based "blob present" wait).
- **T3** `NodeStore::open_range` on-demand keyspace open (range-0-first; Static path unchanged).
- **T4** Two-phase `ServerNode::start` Replicated mode + `NodeConfig::layout` + CLI `--replicated-ranges` / seed-only `--range-boundaries`.
- **T5** Deterministic in-crate / single-process tests (round-trip; no-seed node derives the same map; wrong-seed node routes by replicated truth; Static-mode regression).
- **T6** Multi-process e2e (a node joins with no boundary config, learns the layout from the meta range, routes + reads a row in each range across the process boundary; UAC-safe binary name).
- **T7** Gauntlet + traceability + finish.

## Success criteria

| # | Criterion | Task / verified by |
|---|---|---|
| 1 | A `RangeMap` round-trips through `to_descriptor_bytes`/`from_descriptor_bytes`; a corrupt or forward-version blob yields `Err(DescriptorError)`, never a panic. | **T1** serde unit tests |
| 2 | In Replicated mode a bootstrap node commits its seed boundaries; `/0/meta/range_map` is present in range 0's committed state and decodes to the seed `RangeMap`. | **T2/T4** store + bring-up test |
| 3 | A Replicated-mode node started with **no** `--range-boundaries` brings up range 0, reads the blob, and derives an authoritative `RangeMap` **equal to the bootstrap node's** — layout came from the meta range, not config. | **T5** no-seed test |
| 4 | A node started with **deliberately wrong** seed boundaries still routes by the replicated descriptors (the meta range overrides local config). The split-brain-routing hazard is closed. | **T5** wrong-seed test (load-bearing) |
| 5 | Routing through a Replicated-mode node: `CREATE` (range 0) + `INSERT` into a data-range table + `SELECT` read-back all route by the replicated boundaries; a table whose id falls in range 1 lands in `data-r1`. | **T5** routing test |
| 6 | All SP9/SP10 single-range and SP13/SP14 static multi-range suites pass unchanged in Static mode. | **T4** regression gate (full `cargo nextest run --workspace`) |
| 7 | Across the real process boundary: a node that joins a running cluster **without** boundary config learns the layout from the meta range and correctly routes + reads a row in each range, read back through **any** node. | **T6** multi-process e2e |
| 8 | No new shipped dependency; `#![forbid(unsafe_code)]`; full gauntlet green; complete success-criteria traceability table. | **T7** gauntlet |

## Test plan

**Sleep policy.** The in-crate layers (T1–T5) are fully sleep-free — every wait (range-0 leader, "blob present", per-range leader, replication visibility) is an openraft `wait().metrics(...)` event or a bounded condition on observed applied state, mirroring `Cluster::wait_for_leader` and the SP14 multi-range harness. The multi-process e2e (T6) reuses the SP9/SP14 harness, which observes remote nodes only through the node-global control protocol; where it must poll a real condition (a leader present, a committed row readable through a node) it uses a **bounded poll cadence with a deadline** (the cross-process harness rule — a small interval, never a fixed settle-sleep), never a guessed duration.

1. **Descriptor codec (T1)** — `RangeMap::with_boundaries([…])` → `to_descriptor_bytes` → `from_descriptor_bytes` round-trips for single, two-range, and many-range maps; truncated / bad-version bytes → `Err`. Pure, no I/O.
2. **Meta store write+read (T2)** — single-process: bring up range 0, `write_range_map`, await the blob applied, `read_range_map` returns the same map; reading an absent blob returns `Ok(None)`.
3. **`open_range` (T3)** — open range 0, then `open_range(1)`; assert `data-r1`/`raft-r1` exist and `keyspaces(1)` no longer panics, while Static `NodeStore::open(&single())` still opens only range 0.
4. **No-seed derivation (T5)** — single-process, two `ServerNode`s in Replicated mode sharing a meta range: node A bootstraps with seed `[2]`; node B starts with `seed = None`; assert B's authoritative `RangeMap` equals A's (criterion 3). Paced by range-0 leader `wait()` + blob-present wait.
5. **Wrong-seed override (T5)** — the committed blob says `[2]` (range 0 = `[0,2)`, range 1 = `[2,∞)`); node B starts with a *wrong* `seed = Some([3])` (which alone would mean range 0 = `[0,3)`, range 1 = `[3,∞)`). The two maps disagree on table id 2: B's seed routes it to range 0, the committed map to range 1. Assert B's effective `range_for_table(2)` is range 1 — B follows the committed map, not its own seed (criterion 4).
6. **Replicated routing (T5)** — one Replicated-mode multi-range node with committed boundary `[2]`: `CREATE TABLE a` (id 1 → range 0) + `CREATE TABLE b` (id 2 → range 1) + `INSERT`/`SELECT` read-back on each; assert each row is in its expected `data-r{r}`; a cross-range txn still returns `0A000` (carried from SP14). Paced by leader `wait()`.
7. **Static regression (T4)** — the existing `cluster::{scenarios, sql_over_raft, durable_scenarios, sql_durable, multirange, gateway_local, remote_forward}` and `crabgresql::{multiprocess, jepsen_elle, multirange_gateway}` suites pass unchanged under default `Static` layout.
8. **Multi-process e2e (T6)** — 3 processes; node 1 `--bootstrap --replicated-ranges --range-boundaries 2`, nodes 2–3 `--replicated-ranges` with **no boundaries**; a client connects to node 2 (which never saw `2`), `CREATE`s a table landing in each range, `INSERT`s, and reads them back through node 3; rows land only in their range. UAC-safe binary name (e.g. `meta_range_gateway`). Bounded-condition waits, no settle-sleep.
9. **Gauntlet (T7)** — `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo nextest run --workspace` + `cargo test --workspace --doc` + fuzz-smoke; `cargo deny`; `check-no-native`; success-criteria traceability.

## Non-goals (explicit)

- **Runtime descriptor mutation** — splits, merges, moves, rebalancing; descriptors are immutable after cluster-create. D4.
- **Dynamic keyspace / Raft-group creation *after* serving** — the two-phase bootstrap builds a fixed set then serves; a range appearing while a node is already serving is D4.
- **Per-range membership / placement in the descriptor** — co-located placement stays; membership remains openraft-replicated per range. Non-co-located placement and online range moves are D4+.
- **Leader-confirmed per-statement descriptor reads** — the immutable-blob local read is correct this slice; mutable descriptors (D4) will require forwarding meta reads to the meta leader (pgwire forward or a new RPC) and a descriptor cache with epoch invalidation.
- **Linearizing catalog reads** — the existing stale-local catalog read (`server_node.rs:135`) is untouched ("stay narrow").
- **A structured meta-read Node RPC / a SQL system table for descriptors** — not needed; the blob is read from the local applied store.
- **Replicated address book** — the static peer list remains the address source; replicating it is a later refinement.
- **Cross-range transactions / scatter-gather / structured query RPC** — D3b-rest and beyond.

## Risks (and mitigations)

- **Two-phase bootstrap is the slice's center of gravity** — getting the ordering wrong (read descriptors before range 0 has the committed blob, or before a range-0 leader exists) yields a node that brings up the wrong range set or hangs. Mitigated by strictly event-based waits (range-0 leader, then blob-present) with bounded deadlines, and by criterion 3/7 asserting a no-seed node derives the *exact* committed map.
- **Static-mode regression surface is large** (all SP9/SP10/SP13/SP14 tests). `RangeLayout::Static` must be today's path byte-for-byte. Criterion 6 (`Static` default, full workspace suite) is the load-bearing gate; the default `NodeConfig.layout = Static(RangeMap::single())` keeps every existing call site behaving identically.
- **Immutable-read correctness depends on the blob truly never changing this slice.** If any task introduces a descriptor write after create, the local-applied read becomes a staleness bug. The spec locks single-write-at-create; a guard test asserts the blob is write-once (a second bring-up of an existing cluster does not rewrite it).
- **Bootstrap chicken-and-egg** — a node must bring up range 0 before it can read the descriptors that tell it which *other* ranges to host. Resolved by making range 0's self-layout the bootstrap constant (range 0 always exists, co-located, voters = node set), read from the static seed, never from a descriptor.
- **`open_range` / incremental keyspace open** touches the durable store's invariant that the range set is fixed at `open`. Mitigated by keeping Static-mode `NodeStore::open` unchanged and adding `open_range` as a separate, on-demand path used only by Replicated bring-up; the retained `Arc<Database>` (`durable.rs:31`) already supports it.
- **os-740 (Windows UAC)** — the new multi-process e2e binary must avoid `setup/install/update/patch/upgrad` in its target name (CLAUDE.md policy). `meta_range_gateway` is clean; the T6 task and the gauntlet name-grep re-verify.
- **Scope creep toward D4** (runtime mutation, dynamic keyspaces, epochs, stable id allocation): fenced in Non-goals; T3/T4 must build *bootstrap-time* range-set construction only.
