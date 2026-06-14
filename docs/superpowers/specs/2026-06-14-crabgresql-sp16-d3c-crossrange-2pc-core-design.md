# SP16 / D3c — Cross-range distributed transactions (in-process 2PC core)

**Predecessors:** SP1–SP6 (single-node SQL/MVCC/concurrency), SP7–SP9 (single-range Raft, durable storage, real TCP transport), SP10–SP12 (SQL leader routing, over-the-wire serializability, linearizable reads), SP13 (in-process multi-range core — `RangeMap`, `MultiRangeCluster`, `RangeRouter`), SP14 (network range routing), **SP15 (replicated range descriptors — relocate-only)** — all merged or in review.

**Goal:** Replace the cross-range `0A000` rejection with a real **two-phase commit** so a transaction that writes rows in more than one range commits **atomically** — every range commits or every range aborts — under snapshot isolation, with a **single global commit decision recorded in range 0**. This makes crabgresql a distributed *database* (atomic multi-shard transactions), not a sharded key-value store. Cut **in-process**: the protocol, the global transaction manager, the prepared/in-doubt visibility state, and crash recovery are built and proven deterministically in the `MultiRangeCluster` harness. The cross-node structured prepare/commit RPC and the multi-process e2e are the explicit follow-up (SP17) — the same "build the core in-process, add the network next" rhythm the project already ran for SP13→SP14.

**Architecture:** Four pieces. (1) A **global transaction manager (GTM)** rooted in range 0 (the system/meta range, where the clog already lives at `/0/clog/`): a monotonic global-xid allocator in a namespace disjoint from per-range xids, a global running-set, the global clog extended with a third **`Prepared`** (in-doubt) state, and a durable per-transaction record. (2) A **2-phase participant API** on the executor (`prepare` / `commit_prepared` / `abort_prepared`) beside the existing one-shot `commit`, where `prepare` durably stages a participant range's write-set (tuples stamped with the global xid, held by the existing row locks) through that range's own Raft group without making it visible. (3) A **coordinator** in the gateway session (`RangeRouter`): `Pin` generalizes from one range to a participant *set*, the `0A000` guard becomes participant registration, `COMMIT` drives PREPARE-all-then-decide, `ROLLBACK` aborts all participants. (4) A **global-xid-aware MVCC visibility resolver**: a tuple stamped with a global xid resolves its commit decision through range 0's clog (and a global snapshot component), so both ranges flip from invisible to visible at the *same* instant — range 0's `Committed(G)` apply.

**Tech stack:** Rust 2024, existing `mvcc`/`executor`/`cluster` crates, openraft (one group per range, reused per participant). **No new shipped dependency.** `#![forbid(unsafe_code)]` unchanged. Tests: the `MultiRangeCluster` in-process harness (deterministic, sleep-free via openraft events) plus the existing `jepsen_bank` (conservation) and `jepsen_elle` (strict-serializability) checkers extended to cross-range workloads.

---

## Where this sits: the roadmap

| Slice | Scope |
|---|---|
| SP13 (D3a) | In-process multi-range core. ✅ |
| SP14 (D3a-net) | Network range routing. ✅ merged |
| SP15 (D3b-meta) | Replicated range descriptors (relocate-only). ✅ in review |
| **SP16 (D3c) — this spec** | **Cross-range distributed transactions: in-process 2PC core** (global txn manager in range 0, PREPARE/in-doubt state, single global commit decision, recovery), atomic commit + snapshot isolation. |
| SP17 (D3c-net) | 2PC over the network: a structured prepare/commit/abort RPC replacing SQL-text forwarding, plus the multi-process e2e. |
| later | Full cross-range serializability (SSI); dynamic placement; range splits (D4). |

Everything below SP16 in that table is **out of scope** for this slice.

## The load-bearing constraints (why the design is shaped this way)

1. **Everything transactional is per-range today.** Each range builds its own `SqlEngine` with its own `ProcArray` seeded from its own `data-r{r}` keyspace, so the `next_xid` counter, the running-set, the **clog** (`/0/clog/<xid>` *in that range's keyspace*), and the snapshot are all per-range (`server_node.rs:138-174`, `procarray.rs`). Range 1 and range 2 each independently hand out xid 1, 2, 3. There is **no** global xid, **no** global commit record, **no** in-doubt state, and **no** cross-range snapshot. None of these are *assumed away* deep in the MVCC math — they are simply **absent**, and 2PC must supply them.

2. **The MVCC algorithm is range-agnostic and reusable as-is.** `satisfies_mvcc(xmin, xmax, snapshot, own_xid, status)` (`visibility.rs:52-70`) makes no assumption about which range a tuple lives in, and it takes the clog as a **closure** (`status: impl Fn(u64) -> XidStatus`). So "for a global xid, consult range 0's clog instead of the local one" is a **resolver swap**, not an algorithm change. The tuple header already stores an explicit `u64` xid in both key and value (`version.rs:18,41-47`), so a globally-allocated xid needs **no on-disk format change**.

3. **The clog is logically rooted in range 0.** `clog_key(xid)` is `/0/clog/<xid>` (table 0 ⇒ range 0, `key.rs`). So range 0's clog is the natural home for the *single global commit decision* both ranges consult — both ranges flip visible at the same instant (range 0's clog apply), eliminating partial-visibility skew **by construction**. Range 0 already hosts the catalog and (SP15) the range descriptors, and **every range engine already holds a handle to range 0's store** (`catalog_kv`), so resolving a global xid reuses an existing seam rather than adding a new cross-range read path.

4. **The atomicity boundary today is one `WriteBatch` = one Raft append to one range** (`committer.rs:21-27`, `types.rs:6-7`). A single-range commit folds rows + the `Committed` clog byte + the `next_xid` bump into that one append, so a version and its commit decision flip together. 2PC must **split durability from visibility**: a PREPARE is durable-but-not-visible (intents readers skip until the global decision), and the global `Committed(G)` is a *separate* append to range 0. **A reviewer or implementer who tries to make a cross-range write atomic with one batch will dead-end — no primitive commits across two Raft groups.** The spec locks the prepare-then-decide ordering.

5. **The rejection is one guard.** `RangeRouter::dispatch` rejects the second range of a pinned txn at `router.rs:260-269` (`Pin::Range(p)` arm → `ExecError::Unsupported` → SQLSTATE `0A000`). This single seam becomes "register the new range as a participant," and `COMMIT` becomes coordinator-driven. The per-range lazy `session_mut` (`router.rs:299-309`) already supports multiple per-range sessions in one connection, so multi-range sessions are structurally supported.

## Decisions (locked during brainstorming)

1. **First cut = in-process core.** Built and proven in `MultiRangeCluster` (direct `leader_engine(r)` access to each participant, no wire). The cross-node structured prepare/commit RPC + multi-process e2e are **SP17**.
2. **Guarantee = atomic commit + snapshot isolation.** A cross-range txn is all-or-nothing and becomes visible at one global commit point, under the **same** snapshot-isolation level today's single-range txns give. **Write-skew remains a documented known anomaly** (as it is single-range — `jepsen_elle` uses a `FOR UPDATE` anchor row to avoid it). Full cross-range serializability / SSI is out (later).
3. **Commit decision = single global clog in range 0.** The coordinator writes one `Committed(G)` (or `Aborted(G)`) to range 0's Raft group; every range resolves a global xid's visibility through range 0's clog. Not replicated per-range copies (which would reintroduce a partial-visibility window).
4. **Coordinator = the gateway session (`RangeRouter`), txn record anchored on range 0.** Recovery is **presumed-abort**: an in-doubt txn with no durable `Committed` record is aborted and its prepared intents discarded.

**Internal decisions (locked, with resolution):**

- **Global xid namespace.** Global (cross-range) transaction ids are allocated by range 0 from a reserved high half of the `u64` space (`GLOBAL_XID_BASE = 1 << 63`); per-range local xids stay `< GLOBAL_XID_BASE`. A tuple's `xmin`/`xmax` therefore *self-identifies* whether the visibility resolver consults the **local** clog (`xid < GLOBAL_XID_BASE`, today's path, unchanged) or **range 0's global** clog (`xid >= GLOBAL_XID_BASE`). No tuple-format change; the dispatch is a single numeric test. Single-range transactions are completely unaffected (they keep using their range-local low xids and local clog).
- **The third clog state.** `XidStatus` gains `Prepared` beside `Committed`/`Aborted`/`InProgress`. A global xid is `Prepared` (in-doubt) between PREPARE and the global decision; rows stamped with a `Prepared` global xid are **invisible to all readers** (treated like in-progress). `Committed` → visible; `Aborted`/absent → invisible (the existing "absent = aborted-equivalent" invariant carries the presumed-abort default).
- **The global snapshot component.** For cross-range **repeatable read** to be consistent, a reader's snapshot captures a global component from range 0 — `global_xmax = next_global_xid` and `global_xip = range 0's running global-xid set` — exactly mirroring the local `Snapshot{xmin,xmax,xip}`. A tuple with a global `xmin` runs `satisfies_mvcc` against this global snapshot + range 0's clog. Under read committed the global component is re-read per statement (as the local snapshot is). The global running-set lives in range 0's GTM and is maintained by the coordinator (add on global-xid allocation, remove on the global decision).
- **PREPARE durability + intents.** A participant's PREPARE is one `client_write` to that range's Raft group carrying the txn's buffered `WriteOp`s (tuples stamped with `G`, `xmax`/`xmin` as usual) **plus** a per-range `Prepared`-intent marker keyed by `G`, all majority-durable when it returns (the existing `Committer` contract). The row locks the txn already holds are the prepare locks. No new durability mechanism — one Raft batch per participant, reusing `RaftCommitter`.
- **The two-phase participant API.** The executor gains a `TwoPhaseParticipant` capability (`prepare(global_xid, writeset) -> Vote`, `commit_prepared(global_xid)`, `abort_prepared(global_xid)`) beside the unchanged one-shot `Committer::commit` (which autocommit and DDL keep using). `commit_prepared` makes the staged tuples visible *by virtue of* the global decision already being `Committed` — in the global-clog model it is mostly a local cleanup (drop the prepare marker + release locks), because visibility is gated by range 0's clog, not a per-range commit byte.
- **The coordinator + txn record.** `Pin::Range(RangeId)` → `Pin::Ranges(BTreeSet<RangeId>)` (a participant set; a single-range txn keeps a one-element set and the existing fast path). On `COMMIT`, the coordinator: (1) allocates nothing new — the global xid was allocated at the txn's first cross-range escalation; (2) writes a durable **transaction record** to range 0 (`/0/txn/<G>` → `{participants, Pending}`); (3) PREPAREs each participant; (4) on all-prepared, writes `Committed(G)` to range 0's clog + flips the txn record to `Committed` + removes `G` from the global running-set (one range-0 append = the atomic commit instant); (5) tells each participant to `commit_prepared`. On any prepare failure/timeout it writes `Aborted(G)` and `abort_prepared`s every participant.
- **Single-range stays single-range.** A txn that only ever touches one range never escalates to a global xid, never writes a txn record, and commits via today's one-shot path verbatim. Global machinery engages **only** when a second range is touched inside an explicit transaction. This preserves the entire single-range regression surface and the autocommit fast path.
- **Recovery (presumed-abort).** On range-0 leadership change / startup, a recovery sweep scans `/0/txn/` for `Pending` records; for each, it drives the decision: if range 0's clog already has `Committed(G)` → finish commit (ensure each participant `commit_prepared`); else → write `Aborted(G)` and `abort_prepared` each participant. A participant restarting with a `Prepared` intent for `G` resolves it by reading range 0's clog/txn record (the SP15 `seed_if_absent`/`wait_for_range_map` "read-the-committed-record-on-startup" pattern). Reusing "absent global clog = aborted" makes presumed-abort the default with no extra state.
- **Deadlock policy.** Two cross-range txns can acquire ranges in opposite orders. First cut: **coordinator-side timeout on PREPARE** → abort (the existing intra-range `40P01` deadlock path and the jepsen harness's clean-`Fail` handling of it cover the per-range case; the cross-range case degrades to a timeout-abort that the client retries). Lock-ordering by range id and a global detector are out.

## Components

### 1. Global transaction manager (GTM) in range 0 (`crates/mvcc/src/clog.rs`, `crates/executor/src/`, new `gtm` module)

A small set of range-0-keyed structures: `next_global_xid` (`/0/meta/next_global_xid`, seeded ≥ `GLOBAL_XID_BASE`, max-merged like `next_xid` in `store.rs:40-60`), the global running-set, the global clog (extended `XidStatus`), and txn records (`/0/txn/<G>`). Because these live under table 0, they ride range 0's Raft group and are reachable by every range engine through the existing `catalog_kv` handle. The GTM exposes: allocate-global-xid (register running), record-prepare/commit/abort decision, read-global-status(G), and the global-snapshot read.

### 2. Prepared clog state + global-xid-aware visibility (`crates/mvcc/src/{clog,visibility}.rs`)

`XidStatus` gains `Prepared`. The visibility resolver passed to `satisfies_mvcc` becomes global-aware: for `xid < GLOBAL_XID_BASE` it reads the engine's local clog (today's behavior, byte-for-byte); for `xid >= GLOBAL_XID_BASE` it reads range 0's global clog and evaluates against the global snapshot component. `Prepared` and absent → invisible; `Committed` (and before the global snapshot) → visible; `Aborted` → invisible. The `satisfies_mvcc` core math is untouched.

### 3. Two-phase participant API on the executor (`crates/executor/src/session.rs`, `lib.rs`)

Beside `Committer::commit`, a participant path: `prepare(G, writeset)` stages the buffered writes stamped with `G` + a `Prepared`-intent marker via the range's `RaftCommitter` (one durable append) and returns a vote; `commit_prepared(G)` / `abort_prepared(G)` release locks + drop the intent (commit makes the rows live by virtue of range 0's `Committed(G)`; abort discards). The session opens a *held* txn on every participant range (closing the documented D3a looseness at `router.rs:235-247`, where a BEGIN-then-first-DML-on-a-new-range fell into autocommit).

### 4. Coordinator + participant-set router (`crates/cluster/src/range/router.rs`, `route.rs`)

`Pin::Ranges(set)`; the `0A000` guard (`router.rs:260-269`) becomes "escalate to a global xid (if not already) + register range `r` as a participant + open its held session." `COMMIT` (`router.rs:214-221`, `session.rs:172-213`) fans out the 2PC coordinator over the participant set; `ROLLBACK` aborts all. In-process the coordinator reaches each participant via `MultiRangeCluster::leader_engine(r)`; the recovery sweep runs against range 0.

## Data flow (a cross-range transfer)

`BEGIN; UPDATE acct_x SET bal=bal-100 WHERE id=1; UPDATE acct_y SET bal=bal+100 WHERE id=1; COMMIT;` where table `acct_x` lives in **range 1** and table `acct_y` lives in **range 2**. (Ranges are table-aligned — a table lives wholly in one range — so a *cross-range* transaction touches **different tables in different ranges**, not different rows of one table. The bank workload below partitions accounts across two such tables.)

1. `BEGIN` — no pin yet.
2. First `UPDATE` (table `acct_x`) resolves to range 1; the txn pins to range 1 (single participant, **local xid** so far — no global machinery).
3. Second `UPDATE` (table `acct_y`) resolves to range 2 ≠ the pin. **Escalation:** the coordinator allocates a **global xid `G`** from range 0 (registers `G` running), re-stamps the txn as global, registers range 2 as a second participant, and opens a held session on range 2. (Range 1's already-staged write is re-tagged with `G` — see Risks.)
4. `COMMIT` — coordinator writes the txn record `/0/txn/G → {ranges:{1,2}, Pending}` to range 0. **PREPARE:** range 1 durably stages its row version (xmin/xmax = `G`) + `Prepared` intent via range 1's Raft (votes yes); range 2 the same. Both prepared.
5. **Decision:** coordinator writes `Committed(G)` to range 0's clog + flips the record to `Committed` + removes `G` from the global running-set — **one range-0 Raft append: the atomic commit instant.** Then `commit_prepared` on ranges 1 and 2 (drop intents, release locks).
6. Any reader on range 1 or range 2 that decodes a `G`-stamped tuple resolves `G` via range 0's clog: before step 5 → `Prepared`/absent → invisible on **both**; after step 5 → `Committed` → visible on **both**. No reader ever sees the transfer half-applied. If step 4 had failed, the coordinator writes `Aborted(G)` and both ranges discard — neither balance changes (conservation holds).

## Tasks (legend)

- **T1** `XidStatus::Prepared` + global-xid namespace constant + the global-aware visibility resolver (pure `mvcc`, unit-tested).
- **T2** GTM in range 0: global-xid allocator (`next_global_xid`, max-merge), global clog read/write, global running-set + global snapshot read, txn record.
- **T3** Two-phase participant API on the executor (`prepare`/`commit_prepared`/`abort_prepared`) + held per-range sessions.
- **T4** `RangeRouter` coordinator: `Pin::Ranges`, escalate-on-second-range, replace the `0A000` guard, `COMMIT` 2PC fan-out, `ROLLBACK` abort-all.
- **T5** Recovery (presumed-abort): range-0 sweep of `Pending` txn records + participant in-doubt resolution on restart/leadership change.
- **T6** Cross-range correctness tests: flip the `0A000` test to atomic-commit; in-doubt invisibility; `jepsen_bank` with `from`/`to` in different ranges (conservation); a crash-between-phases recovery test.
- **T7** Gauntlet + traceability + finish.

## Success criteria

| # | Criterion | Task / verified by |
|---|---|---|
| 1 | `XidStatus::Prepared` round-trips; the visibility resolver routes `xid < GLOBAL_XID_BASE` to the local clog and `xid >= GLOBAL_XID_BASE` to range 0's clog; a `Prepared` xid is invisible. | **T1** unit tests |
| 2 | Range 0 allocates monotonically-increasing global xids `≥ GLOBAL_XID_BASE`, disjoint from per-range local xids; the counter survives a range-0 leadership change (max-merge / reseed). | **T2** allocator test |
| 3 | A cross-range `BEGIN…COMMIT` writing to two tables that live in different ranges commits **atomically**: after COMMIT both rows are visible through both ranges; a forced abort leaves neither. | **T4/T6** atomic-commit test |
| 4 | While a cross-range txn is **prepared but undecided**, its rows are invisible to a concurrent reader on either participant range; they become visible only after `Committed(G)`. | **T3/T6** in-doubt-visibility test |
| 5 | A coordinator crash **between PREPARE and the decision** is resolved atomically by recovery (presumed-abort ⇒ both ranges roll back; or, if `Committed(G)` was already durable, both commit) — never half-applied. | **T5/T6** crash-between-phases test |
| 6 | `jepsen_bank` with accounts partitioned across two account **tables in different ranges** (so a transfer between them is **cross-range**) conserves the total under a crash/partition nemesis (the conservation invariant = cross-range atomicity). | **T6** jepsen_bank cross-range |
| 7 | All existing single-range transaction/MVCC suites pass unchanged; single-range txns never touch the global machinery (no global xid, no txn record). | **T1–T4** regression gate (`cargo nextest run --workspace`) |
| 8 | The former `gateway_rejects_a_cross_range_transaction_with_0a000` now asserts a successful cross-range commit; no new shipped dependency; `#![forbid(unsafe_code)]`; full gauntlet green; traceability table. | **T6/T7** |

## Test plan

**Sleep policy.** Every layer is in-process and sleep-free — waits are openraft `wait().metrics(...)` events or bounded conditions on observed applied state (the established `MultiRangeCluster` pattern). The crash-between-phases test injects the coordinator crash deterministically (a test seam that drops the coordinator after PREPARE returns and before the decision append), then drives recovery and asserts the outcome — no timing guess.

1. **Prepared state + resolver (T1)** — `XidStatus::Prepared` encode/decode; a resolver stub returns local vs global clog by the `GLOBAL_XID_BASE` test; a `Prepared` global xid → invisible, `Committed` → visible. Pure `mvcc`, no I/O.
2. **Global allocator (T2)** — range 0 hands out `G0 < G1 < …`, all `≥ GLOBAL_XID_BASE` and disjoint from a range's local xids; after a simulated range-0 leadership change the next global xid does not regress (max-merge).
3. **Atomic cross-range commit (T3/T4)** — `MultiRangeCluster` 2-range map; `BEGIN; INSERT into a range-1 table; INSERT into a range-2 table; COMMIT;` — both rows read back through both ranges; a variant that forces an abort (e.g. a prepare conflict) leaves neither. Paced by leader `wait()`.
4. **In-doubt invisibility (T3)** — drive a cross-range txn to the prepared-but-undecided point (a test seam pauses before the decision); a concurrent snapshot read on each participant sees **no** `G`-row; after the decision append, both see it.
5. **Crash-between-phases recovery (T5)** — coordinator crashes after both PREPAREs return, before the decision; recovery (presumed-abort) rolls both back and a re-read shows neither row; a second variant where `Committed(G)` was made durable first recovers to *commit* on both. Deterministic seam, openraft-event paced.
6. **jepsen_bank cross-range (T6)** — partition the bank accounts across two account tables in different ranges (e.g. `acct_a` in range 1, `acct_b` in range 2) so a transfer between an `acct_a` account and an `acct_b` account is cross-range; the conservation checker (total invariant) under the existing crash/partition nemesis proves all-or-nothing across two Raft groups. `40P01`/timeout aborts are clean `Fail`s the workload retries.
7. **Single-range regression (T1–T4)** — `executor::{transactions, concurrency}`, `cluster::{sql_over_raft, jepsen_bank single-range}`, and the full workspace suite pass unchanged; assert a single-range txn allocates a **local** xid and writes **no** `/0/txn/` record.
8. **Gauntlet (T7)** — `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo nextest run --workspace` + `cargo test --workspace --doc` + fuzz-smoke; `cargo deny`; `check-no-native`; traceability.

## Non-goals (explicit)

- **The network / cross-node path** — the structured prepare/commit/abort RPC that replaces SQL-text forwarding, and the multi-process e2e. **SP17.** (In-process, the coordinator reaches each participant's leader engine directly; the forward seam's SQL-text relay cannot carry a 2-phase vote.)
- **Full cross-range serializability / SSI / write-skew prevention** — the guarantee is atomic commit + snapshot isolation; write-skew is documented (as single-range). Later.
- **Distributed snapshots / global timestamps / HLC / external consistency** — the global snapshot component is range-0-clog-derived, not a synchronized clock.
- **Cross-range deadlock detection** — coordinator-side timeout/abort only (no lock-ordering, no global detector).
- **Throughput of the range-0 commit funnel** — routing every cross-range commit + global clog through range 0's Raft group is a known scaling limit (the cost of the single-global-clog choice); acceptable for the core, revisited later.
- **Range splits / placement** — D4 and beyond.

## Risks (and mitigations)

- **In-doubt visibility is the load-bearing correctness piece.** A bug that makes a `Prepared` global xid visible (or a committed one invisible on one range) breaks atomicity. Mitigated by: the resolver swap being the *only* visibility change (the `satisfies_mvcc` core is untouched), the dedicated in-doubt-invisibility test (criterion 4), and `jepsen_bank` conservation (criterion 6) as an independent oracle. The exact **global snapshot horizon for repeatable read** is detailed in T1/T2 and validated by the cross-range jepsen workload.
- **Escalation re-tags an already-staged write.** When the txn touches range 1 with a local xid and then escalates on touching range 2, range 1's buffered write must be re-stamped from the local xid to the global `G`. Mitigated by **buffering participant writes until PREPARE** (writes are not yet durable/visible pre-COMMIT), so re-stamping is a buffer rewrite, not a committed-state change. The alternative — allocate the global xid eagerly at BEGIN for any explicit txn — is simpler but pessimistically globalizes single-range txns; the spec prefers escalate-on-second-range and re-stamps the buffer. (If buffering proves awkward, the fallback is eager-global-xid-on-BEGIN-of-an-explicit-multi-statement-txn, decided in the plan.)
- **Coordinator durability.** `RangeRouter` is per-connection and in-memory; the *durable* state is the range-0 txn record + clog, so any node (via the range-0 recovery sweep) can finish an orphaned in-doubt txn. Mitigated by writing the `Pending` record **before** PREPARE and the decision **before** `commit_prepared`, so recovery always has a durable anchor; presumed-abort covers the no-decision case.
- **Idempotency under Raft re-proposal / leader churn.** Every 2PC step (prepare, decision, commit_prepared, abort) must be idempotent, keyed by the global `G`, so a retried step does not double-apply. Mitigated by keying all records on `G` and making the decision a write-once record (the SP15 write-once invariant, reused) and the counter a max-merge.
- **Range-0 hotspot.** All cross-range commits funnel through range 0's group — a throughput/availability coupling. Accepted for the in-process core; flagged as the cost of the chosen single-global-clog model.
- **Scope creep toward SP17 / SSI** (structured RPC, multi-process, serializability): fenced in Non-goals; T3/T4 build only the in-process protocol against `leader_engine(r)`.
