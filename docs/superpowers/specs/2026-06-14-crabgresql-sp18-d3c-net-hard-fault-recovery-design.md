# SP18 / D3c-net-hard — Fault-hardened cross-range 2PC (participant self-resolve + crash-nemesis proof)

**Predecessors:** SP13 (in-process multi-range core), SP14 (network range routing), SP15 (replicated range descriptors), SP16 (cross-range 2PC — in-process core), **SP17 (cross-range 2PC over the network — leader-stable mechanism)**.

**Goal:** Make SP17's cross-range 2PC survive **real coordinator crashes and participant-leader failovers**, and prove it under a multi-process crash/partition nemesis. After SP18: no transaction strands locks indefinitely, no in-doubt transaction stays invisible forever, and the cross-range bank **total is always conserved** even when nodes (including mid-transaction coordinators) are killed and the network is partitioned.

**The shape in one paragraph.** SP17 made a cross-range `BEGIN…COMMIT` commit atomically over the network *when leaders are stable*. Its documented gap (fenced in SP17's Non-goals) is fault recovery: a **coordinator that crashes mid-protocol** leaves an alive participant holding locks with no decision ever arriving, and an **in-doubt `G` whose coordinator died** stays invisible forever because the presumed-abort is never recorded. SP18 closes both via **participant self-resolve** against range 0's global clog, made safe by a **write-once global decision** (first-writer-wins on `clog[G]`). A stranded participant resolves its own in-doubt `G`s — `Committed → finalize`, else it *wins the write-once race* by writing `Aborted(G)` itself and releases — so a recovering coordinator that later tries to commit *loses* the race and reports rollback. No durable coordinator transaction record is needed (the global clog stays the sole arbiter, as in SP16/SP17). A multi-process crash/partition-nemesis cross-range bank, with the conservation oracle, is the load-bearing proof.

**Tech stack:** Rust 2024; the existing `cluster`/`executor`/`mvcc` crates; openraft (deterministic state-machine apply for the conditional decision; the metrics watch for the leadership-rise sweep; the SP17 range-0 read barrier for the linearizable clog read); the SP9 multi-process harness (`crates/crabgresql/tests/harness`) for the nemesis. **No new shipped dependency.** `#![forbid(unsafe_code)]` unchanged. Tests under cargo-nextest; doctests via `cargo test --workspace --doc`.

**Branch:** `sp18-d3c-net-hard-crossrange-recovery` (created, stacked on the SP17 branch tip `0b92182`). Diff against `origin/main`, never local `main`; rebase `--onto origin/main` after SP17 (PR #33) squash-merges.

---

## Where this sits: the roadmap

| Slice | Scope |
|---|---|
| SP16 (D3c) | Cross-range 2PC — in-process core. ✅ merged (PR #32) |
| SP17 (D3c-net) | Cross-range 2PC over the network — leader-stable mechanism. ✅ in review (PR #33) |
| **SP18 (D3c-net-hard) — this spec** | **Fault-hardened cross-range 2PC:** write-once global decision; participant self-resolve (coordinator-silence timeout + durable leadership-rise sweep); the multi-process crash/partition-nemesis cross-range bank conservation proof. |
| later | Mid-transaction re-stage on a leader move; 2PC over the replicated (meta-range) node layout; GC of settled `Prepared`/decided clog records; full cross-range serializability (SSI). |

Everything below SP18 in that table is **out of scope** for this slice (see Non-goals).

## The load-bearing constraints (why the design is shaped this way)

1. **SP17 leaves two fault gaps, both bounded to a single mechanism.** (a) A coordinator (a gateway) that crashes after `Stage` but before `Commit`/`Abort` leaves the participant's held `SqlSession` (and its in-memory row locks) parked in the per-`(G,range)` `TxnService` registry with no decision arriving; SP17's only release is `release_on_leadership_loss`, which never fires if that participant keeps its leadership → **locks linger** (a *liveness* gap; SP17 confirmed it is never a *safety* gap because the durable `Prepared(Li→G)` rows stay invisible until a decision). (b) An in-doubt `G` (allocated + staged, never decided) whose coordinator died stays **invisible forever** because no `Aborted(G)` is ever written. SP18 fixes both with one resolver.

2. **The global clog is already the sole arbiter — recovery needs no new durable record.** SP16/SP17 made every cross-range row's visibility defer to `clog[G]` in range 0 (the `global_status` resolver + the durable-state `gsnap`). So "resolving" an in-doubt `G` is exactly *deciding `clog[G]`*. A participant holding its own durable `Prepared(Li→G)` markers + read access to range 0's global clog (the SP17 barrier) has everything it needs to self-resolve. A `/0/txn/<G>` coordinator record would be redundant machinery layered on top of the arbiter that already exists. **This is why participant self-resolve, not a coordinator sweep, is the model.**

3. **Presumed-abort is safe ONLY if a participant can make the abort decision *stick* against a slow coordinator.** A stranded participant that times out cannot just locally abort — the coordinator might still be alive and about to write `Committed(G)`. The resolution: the participant **writes `Aborted(G)` to the global clog itself**, and the clog decision is **write-once**. If the participant wins, the coordinator's later `Committed(G)` write is a no-op-keep (the participant's `Aborted` stands) and the coordinator reads back `Aborted` and reports rollback. If the coordinator already wrote `Committed(G)`, the participant's `Aborted` write is a no-op-keep and the participant reads back `Committed` and commits. Exactly one decision is final. **Without write-once, two writers could split the decision → a transfer could be half-applied → conservation breaks.** Write-once is therefore the correctness core.

4. **Write-once must be enforced at the deterministic apply, not by read-then-write.** A read-`clog[G]`-then-write-if-absent across the range-0 Raft boundary is **not** atomic — two concurrent resolvers could both read-absent and both write different decisions. The conditional check must happen **inside the state-machine apply** of the decision op (which openraft serializes per range and replays identically on every replica), so first-writer-wins is a deterministic property of the log order, not a racy client-side check.

5. **Recovery must be deterministically testable without a settle-sleep.** The coordinator-silence timeout is a *production* time-based mechanism (a bounded duration `T`). The multi-process nemesis test sets a short `T` and waits on the **workload-progresses** condition (a blocked writer to a stranded row eventually succeeds once recovery fires) — a real, deadline-bounded wait, *not* a fixed settle-sleep. The system's own timer drives recovery; the test observes the resulting progress. This is consistent with the no-sleep rule (wait on the real condition; pace the nemesis on progress).

## Decisions (locked during brainstorming)

1. **Recovery model = participant self-resolve, no durable `/0/txn/<G>` record.** Each participant resolves its own in-doubt `G`s against range 0's global clog; the global clog stays the sole arbiter.
2. **Crash-nemesis proof = multi-process** (real OS-process kills via the `crabgresql` harness), not in-process simulation.
3. **Scope = post-prepare recovery only.** A leader that moves *during* staging still surfaces a retryable abort to the client (SP17 behavior). Mid-transaction re-stage → a later slice.

**Internal decisions (locked, with resolution):**

- **The write-once decision is a clog-aware conditional apply.** Add a decision op whose state-machine apply keeps any existing terminal `clog[G]` (idempotent if the same status, no-op-keep if a different one) — first-writer-wins by log order. `commit_global_decision` writes via this op and then **reads back the effective decision** linearizably (the SP17 range-0 read path) to learn whether it won. The coordinator's `COMMIT` and the participant's resolver both consult the effective decision.
- **`resolve_in_doubt(g)` is the single resolver** used by both triggers: linearizably read `clog[g]` (SP17 barrier); `Committed → commit_release`; `Aborted → abort_release`; absent → write-once `Aborted(g)` then act on the effective decision. It reads/writes only range 0's global clog (the participant's local range work is the existing `commit_release`/`abort_release`).
- **Trigger (a): a coordinator-silence timeout on held sessions.** The per-`(G,range)` `TxnService` entry records the instant it joined `G`. A bounded-interval per-node sweeper (no-sleep: a bounded `tokio::time::interval` / metrics-paced loop) resolves any held session older than `T`. On resolution it runs `resolve_in_doubt(g)` and `commit_release`/`abort_release` per the effective decision, then drops the entry. `T` is configurable (production: a few seconds; the nemesis test: short).
- **Trigger (b): a durable leadership-rise sweep.** Extend the existing range-leadership rising-edge hook (SP17 wires `reseed_on_leadership` there) so that, when this node becomes range-r's leader, it scans range-r's durable clog for in-doubt `Prepared(Li→g)` markers (local xids whose status is `Prepared(g)` and whose `g` is undecided in the global clog) and runs `resolve_in_doubt(g)` for each distinct `g`. This finalizes the global decision for a `g` whose participant leader died, so its rows are never invisible forever. (No in-memory locks to free on the new leader — they died with the old leader.)
- **Coordinator effective-decision check.** SP17's `NetCoordinator::commit_global`/`LocalCoordinator::commit_global` learn the effective decision from the write-once write and surface it: if the coordinator intended `Committed` but the effective decision is `Aborted` (a participant won the abort-race), the coordinator releases participants with **abort** semantics and reports `ROLLBACK`/a retryable error to the client — never a false `COMMIT`.
- **The participant's own range stays local.** Self-resolve only decides the *global* `clog[g]`; freeing the participant's row locks + resetting its session is the existing `commit_release`/`abort_release` (no clog write on the participant's own range — unchanged from SP16/SP17).
- **The nemesis kills coordinators mid-transaction.** The workload issues cross-range transfers at random gateways; the nemesis kills random nodes (a killed gateway *is* a crashed coordinator) and induces partitions, paced on a committed-op progress signal. Conservation + progress are the oracles.

## Components

### 1. Write-once global decision (`crates/mvcc/src/clog.rs`, `crates/executor/src/{lib,commit}.rs`, range-0 state machine)

A clog-aware conditional decision op + a `commit_global_decision` that returns the **effective** terminal decision (read-back). The conditional keep lives in the deterministic apply path so first-writer-wins is by log order.

### 2. The `resolve_in_doubt` resolver (`crates/executor/src/{lib,session}.rs` + `crates/cluster/src/twopc.rs`)

One function: linearizable `clog[g]` read (SP17 barrier) → terminal-or-write-once-Aborted → effective decision → `commit_release`/`abort_release`. Reused by both triggers and by the coordinator's effective-decision check.

### 3. Coordinator-silence timeout (`crates/cluster/src/twopc.rs` `TxnService`, `crates/cluster/src/server_node.rs`)

A join-instant on each held `(G,range)` entry + a bounded per-node sweeper that resolves entries older than `T`.

### 4. Durable leadership-rise sweep (`crates/cluster/src/server_node.rs`, `crates/executor/src/lib.rs`)

A range-r durable-clog scan for in-doubt `Prepared(Li→g)` markers on the leadership rising edge, resolving each distinct `g`.

### 5. Multi-process crash-nemesis cross-range bank (`crates/crabgresql/tests/crossrange_2pc_nemesis.rs`, harness additions)

A cross-range transfer workload + a crash/partition nemesis paced on progress + the conservation + progress oracles. UAC-safe filename (`nemesis` has no forbidden substring).

## Data flow (a coordinator crash mid-transaction)

Tables `acct_x` (range 1, led by **N2**) and `acct_y` (range 2, led by **N3**); a transfer `BEGIN; UPDATE acct_x -= 5; UPDATE acct_y += 5; COMMIT` issued at gateway **N1** (the coordinator).

1. N1 escalates: `BeginGlobal` → range 0's leader → `G`; `Stage` → **N2** (holds a per-`G` `SqlSession`, writes `Prepared(L1→G)`, holds the `acct_x` row lock) and `Stage` → **N3** (holds `Prepared(L2→G)`, `acct_y` lock).
2. **N1 crashes** after staging, before `CommitGlobal`. The transfer is in-doubt: `clog[G]` is absent; both rows are invisible (presumed-abort by absence); N2 and N3 each hold a parked held session + an in-memory row lock. A concurrent writer to `acct_x` now **blocks** on N2's held lock.
3. **Trigger (a) fires on N2 (and N3):** the held session for `G` ages past `T`. N2 runs `resolve_in_doubt(G)`: reads `clog[G]` linearizably → absent → **writes `Aborted(G)`** (wins the write-once race; no coordinator is alive to contend) → effective decision `Aborted` → `abort_release` frees the `acct_x` lock and discards `Prepared(L1→G)`'s visibility (already invisible). N3 does the same for `acct_y`. The blocked concurrent writer proceeds. **No money moved** (atomic abort) → conservation holds.
4. If instead N1 had managed to write `CommitGlobal{G, Committed}` *before* crashing, step 3's `resolve_in_doubt(G)` reads back `Committed` → `commit_release`; both rows become visible atomically → the transfer applied in full → conservation holds.
5. If a **participant** (say N2) crashes too: its in-memory session + lock vanish (lock-safe), its durable `Prepared(L1→G)` persists (rows resolve via `clog[G]`). When a new node becomes range-1's leader, **trigger (b)** sweeps the durable `Prepared(→G)` marker and runs `resolve_in_doubt(G)`, finalizing the decision so `acct_x`'s row is never invisible forever.

No interleaving yields a half-applied transfer: the single write-once `clog[G]` decision is the all-or-nothing instant, and every recovery path routes through it.

## Tasks (legend)

- **T1** Write-once global decision: clog-aware conditional decision op (deterministic keep-existing apply) + `commit_global_decision` returns the effective decision via linearizable read-back.
- **T2** `resolve_in_doubt(g)` resolver + the coordinator's effective-decision check (commit/rollback honestly).
- **T3** Coordinator-silence timeout: per-`(G,range)` join-instant + bounded per-node sweeper.
- **T4** Durable leadership-rise sweep over range-r `Prepared(→g)` markers.
- **T5** Multi-process crash/partition-nemesis cross-range bank + conservation + progress oracles.
- **T6** Gauntlet + traceability + CLAUDE.md UAC entry + finish.

## Success criteria

| # | Criterion | Task / verified by |
|---|---|---|
| 1 | The global decision is **write-once**: a second decision write with a *different* status keeps the first; the writer reads back the effective decision. A same-status re-write is idempotent. | **T1** decision unit test |
| 2 | `resolve_in_doubt(g)` returns `Committed`/`Aborted` for a decided `g`, and for an undecided `g` writes `Aborted(g)` and returns `Aborted` — and a concurrent coordinator `Committed(g)` write loses to it (deterministic by log order). | **T1/T2** resolver test |
| 3 | A coordinator that crashes after `Stage` and before the decision leaves no lock stranded: the alive participant's held session self-resolves within `T`, frees its lock, and a previously-blocked writer proceeds. Nothing is half-applied. | **T3** timeout test |
| 4 | An in-doubt `g` whose participant leader crashed is finalized on the new leader's rise (durable-marker sweep) — its rows resolve (invisible for a presumed-abort, visible for a committed `g`) rather than staying in-doubt forever. | **T4** sweep test |
| 5 | A coordinator whose intended `Committed` lost the abort-race reports `ROLLBACK`/retryable and releases participants with abort semantics — never a false `COMMIT`. | **T2** coordinator-honesty test |
| 6 | **The cross-range bank total is conserved** under a multi-process crash/partition nemesis that kills random nodes (incl. mid-txn coordinators), and the cluster makes progress (no permanent stranding). | **T5** nemesis conservation + progress |
| 7 | All SP16/SP17 in-process + networked cross-range suites pass unchanged; no new shipped dependency; `#![forbid(unsafe_code)]`; full gauntlet green; traceability table. | **T6** regression gate + gauntlet |

## Test plan

**Sleep policy.** In-crate layers (T1–T4) are sleep-free — every wait is an openraft `wait().metrics(...)` event or a bounded condition; the write-once apply + resolver are deterministic. The coordinator-silence timeout `T` is a *production* bounded duration; the multi-process nemesis (T5) sets a short `T` and waits on the **workload-progresses** condition via the harness's bounded poll cadence (interval + deadline), never a settle-sleep. The nemesis is paced on a committed-op progress signal, not the clock.

1. **Write-once decision (T1)** — apply two decisions for `g` with different statuses (in either order) against a range-0 store; assert the first is kept and the read-back reports it; a same-status re-write is a no-op. Deterministic, in-crate.
2. **Resolver (T1/T2)** — `resolve_in_doubt(g)` on a `Committed`/`Aborted`/absent `g`; for absent, assert it writes `Aborted` and returns `Aborted`; assert a contending `Committed` write after the participant's `Aborted` loses (the effective decision stays `Aborted`).
3. **Coordinator-silence timeout (T3)** — an in-crate `ServerNode` (or `MultiRangeCluster`) stages a participant for `G`, drops/silences the coordinator (the SP16 `before_global_decision` seam or a withheld decision), advances past `T`; assert the held session self-resolves (`abort_release`), the lock frees (a previously-blocked writer completes), and nothing is half-applied. Paced on the writer-completes condition.
4. **Leadership-rise sweep (T4)** — leave a durable `Prepared(→g)` marker for an undecided `g` on a range whose leader then changes (kill + re-elect, awaited via `wait().metrics`); assert the new leader's sweep finalizes `g` (writes `Aborted(g)`) so the row resolves invisible — and a separately-committed `g'` resolves visible.
5. **Coordinator honesty (T2)** — a participant wins the abort-race for `G`; the coordinator's `commit_global` reads back `Aborted`; assert the client sees `ROLLBACK`/retryable and both participants are abort-released, not committed.
6. **Multi-process crash-nemesis bank (T5)** — `crossrange_2pc_nemesis`: N processes, a cross-range bank (accounts split across ≥2 ranges), a transfer workload at random gateways, a nemesis killing random nodes (incl. coordinators mid-txn) + partitions, paced on progress; the oracle reads all accounts and asserts the total is invariant throughout and that the workload makes progress (stranded txns recover). UAC-safe binary.
7. **Gauntlet (T6)** — `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo nextest run --workspace` + `cargo test --workspace --doc`; `cargo deny check`; UAC guard; traceability.

## Non-goals (explicit → later)

- **Mid-transaction re-stage on a leader move** (a participant range's leadership moves *during* staging) — deferred per the locked scope; SP18 keeps SP17's retryable-abort-then-client-retry for that window.
- **2PC over the replicated (meta-range) node layout** — the static layout hosts the 2PC service; the replicated bring-up still wires `None`. A later coverage slice.
- **GC / compaction of settled `Prepared(Li→G)` markers and decided `clog[G]` records** — they accumulate; reclamation is later.
- **Full cross-range serializability (SSI)** — SI with documented write-skew, as single-range.
- **A durable `/0/txn/<G>` coordinator record + coordinator-driven sweep** — explicitly *not* built; participant self-resolve + the write-once global clog subsume its safety role.

## Risks (and mitigations)

- **Write-once is the correctness core.** A read-then-write or a last-writer-wins clog put would let a participant-abort and a coordinator-commit split the decision → a half-applied transfer → conservation breaks. Mitigated by enforcing the keep-existing check in the *deterministic state-machine apply* (first-writer-wins by log order, replicated identically) and proving it with the decision unit test (criterion 1) + the conservation oracle (criterion 6). This is SP18's analog of SP16's deregister-at-prepare — spend the rigor here.
- **A premature timeout aborts a still-live transaction.** If `T` is shorter than a slow-but-healthy commit, a participant could abort-race a `G` the coordinator was about to commit. This is *safe* (the coordinator then reads back `Aborted` and reports rollback — no split, no lost money; the client retries) but harms liveness/throughput. Mitigated by choosing `T` well above the normal commit latency in production, and a short-but-adequate `T` in the test paced on progress; documented as a latency/throughput tradeoff, never a safety one.
- **The leadership-rise sweep scans the durable clog.** A large clog makes the sweep O(in-doubt markers). Mitigated by scanning only in-doubt `Prepared(→g)` entries (terminal-status rows are skipped) and amortizing over the rare leadership-change event; GC of settled markers is a later slice.
- **The nemesis must pace on progress, not sleep.** A zero-gap or settle-sleep nemesis starves recovery on the 2-core CI runner (the documented lesson). Mitigated by pacing the next fault on a committed-op progress signal and waiting on the workload-completes condition with a bounded deadline.
- **Determinism of the conditional apply across replicas.** The keep-existing check must read only the applied state machine (no wall-clock, no per-node state) so every replica computes the identical decision. Mitigated by implementing it as a pure function of `clog[G]`'s current applied value + the incoming op.
- **Scope creep toward mid-txn re-stage / a durable record:** fenced in Non-goals; T1–T5 build only the write-once decision + participant self-resolve + the nemesis proof.

## Traceability (criterion → task → proving test)

Each success criterion above is proven by the concrete test(s) below (all shipped and green at the SP18 gauntlet).

| # | Criterion | Task | Proving test(s) |
|---|---|---|---|
| 1 | Write-once global decision (first-writer-wins; effective read-back; idempotent same-status) | T1 | `cluster::store::tests::clog_decision_is_write_once_first_writer_wins`; `cluster::store::tests::is_clog_key_matches_only_clog_keys`; `cluster::durable::…::apply_clog_keeps_first_terminal_same_key_twice_in_one_batch` (durable intra-batch fold) |
| 2 | Resolver returns the effective decision; a contending coordinator-commit loses to a participant-abort by log order | T1/T2 | Write-once tests (criterion 1) establish the first-writer-wins arbiter; `cluster::range::router::tests::coordinator_reports_rollback_when_decision_already_aborted` exercises the contending-write outcome end to end |
| 3 | Coordinator crash after `Stage` strands no lock: the alive participant self-resolves within `T`, frees its lock, nothing half-applied | T3 | `cluster::twopc::tests::a_silent_coordinator_is_recovered_by_the_timeout_sweeper` |
| 4 | An in-doubt `g` whose participant leader crashed is finalized on the new leader's rise (durable-marker sweep); rows resolve rather than staying in-doubt | T4 | `cluster::twopc::tests::a_durable_prepared_marker_is_finalized_by_the_leadership_sweep`; `executor::tests::in_doubt_globals_lists_undecided_prepared_markers` (the scan that drives the sweep) |
| 5 | A coordinator whose intended `Committed` lost the abort-race reports `ROLLBACK`/retryable and abort-releases — never a false `COMMIT` | T2 | `cluster::range::router::tests::coordinator_reports_rollback_when_decision_already_aborted` |
| 6 | The cross-range bank total is conserved under a multi-process crash/partition nemesis (incl. mid-txn coordinator kills), and the cluster makes progress (no permanent stranding) | T5 | `crabgresql::crossrange_2pc_nemesis::cross_range_bank_conserves_total_under_crash_nemesis` (conservation oracle + all-pairs post-heal recovery round + non-vacuity) |
| 7 | All SP16/SP17 in-process + networked cross-range suites pass unchanged; no new shipped dependency; `#![forbid(unsafe_code)]`; full gauntlet green; traceability | T6 | `cluster::crossrange_2pc` + `cluster::jepsen_bank` cross-range conservation (regression gate, incl. the bounded-retry flake fix on the authoritative read); `crabgresql::crossrange_2pc_net`; full gauntlet (`cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo nextest run --workspace`; `cargo test --workspace --doc`; `cargo deny check`); UAC guard; this table |
