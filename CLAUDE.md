# crabgresql — agent instructions

A from-scratch PostgreSQL-compatible distributed database in Rust 2024, built one
vertical "slice" at a time.

## Test runner: cargo-nextest

Tests run under **cargo-nextest**: `cargo nextest run --workspace` (CI uses
`--profile ci`). The heavy distributed suites are capped via concurrency groups in
`.config/nextest.toml` — multi-process suites run serially, in-process cluster
suites are capped — so they don't starve each other's Raft elections on the 2-core
runner, while everything else runs at full parallelism. nextest does **not** run
doctests; run those separately with `cargo test --workspace --doc`.

## Testing: no `sleep` — make tests deterministic via instrumentation

**Do not write tests (or test harness code) that use `sleep`/`tokio::time::sleep`
to wait for something to happen, or to "let the system settle".** A `sleep` is a
guess about timing; on a slow or CPU-starved runner (e.g. the 2-core / llvm-cov CI
machine) the guess is wrong and the test flakes.

Instead, **wait on the actual condition or event**, bounded by a timeout so a stuck
system fails the test instead of hanging:

- **Wait on Raft state via openraft's event API**, not a poll-sleep loop. Await a
  leader / applied-index / membership condition with
  `raft.wait(Some(timeout)).metrics(|m| cond, "reason")` (or
  `.applied_index_at_least(idx, "reason")`). It returns the instant the condition
  holds. See `Cluster::wait_for_leader` / `wait_for_leader_excluding` in
  `crates/cluster/src/cluster.rs` for the established pattern, and mirror it in any
  new harness (e.g. `MultiRangeCluster`).
- **Wait for replication/visibility** by awaiting the follower's applied index (or
  by reading until the committed value is present *through a bounded, condition-
  driven wait that is not a fixed sleep*), not by sleeping a fixed duration.
- **Pace a fault-injection nemesis on workload progress, not the clock.** Drive the
  next fault off a real signal that the workload has made progress (e.g. a
  committed-op counter / channel the workload updates, or an awaited applied
  index), rather than `sleep`-ing a "stable window". The nemesis advances exactly
  when there is progress to perturb.

If a wait genuinely cannot be expressed as a condition, that is a signal the system
lacks the instrumentation a deterministic test needs — **add the instrumentation**
(a metric, a notifier, a progress counter) rather than reaching for `sleep`.

The goal: every test is deterministic and never flaky — it passes or fails on
behavior, never on timing.

## Testing: model-check every new feature with Stateright

**Every new feature gets a Stateright model that exhaustively checks its core safety
invariant** — alongside the empirical tests, not instead of them. The multi-process
nemeses and jepsen suites only *sample* fault interleavings; a `stateright::Model`
explores *every* interleaving up to a bounded step budget, so it finds the adversarial
ordering a sampled run misses and reports a *minimal, deterministic* counterexample.
This is not optional polish: SP21's torn-commit (a participant `Stage` double-staged
across a leader failover) slipped past the in-process tests entirely, and a Stateright
model — exhaustive over begin/stage/retry/decide — pinned it deterministically.

The discipline (mirror `crates/cluster/tests/model.rs` and
`crates/cluster/tests/crossrange_2pc_model.rs`):

- **Abstract, not the runtime.** Model the *logic* (the state machine + its invariant)
  as a pure `Model` — no openraft, no SQL engine, no I/O — so the BFS is fast and total.
  Keep every `Vec` canonical (sorted) so logically-equal states fingerprint equally and
  the search dedups + terminates. Bound the search with a `max_steps` budget.
- **A boolean toggle for the broken variant.** Express the fix as a config flag
  (`reseed`, `fold_on_commit`, `idempotent_stage`): `true` is the real system, `false`
  is the deliberately-broken one.
- **Teeth tests are MANDATORY.** A passing `assert_properties()` is meaningless unless
  you also prove the checker CATCHES the bug: a teeth test runs the broken variant and
  asserts `!checker.discoveries().is_empty()` AND that `discoveries()` names the specific
  safety property. A model with no teeth is worse than no model — it gives false
  confidence. Also assert `unique_state_count() > 1` so a "passing" run is not vacuous.
- **Never weaken a property to make a model pass.** A counterexample in the *correct*
  variant is a genuine design finding — investigate it, do not relax the invariant.

Scale the model to the feature — a pure-data or single-node refactor with no
concurrency/fault dimension may not warrant one — but anything touching 2PC,
replication, recovery, leadership, locking, MVCC visibility, or cross-range consistency
does, and gets a model with teeth as part of the slice.

## Mutation testing: cargo-mutants on subsystems (CI nightly)

Coverage proves a line *ran*; a mutant proves a line *matters*. **cargo-mutants**
makes a small behavioral change to the source — `a + b` -> `a - b`, `<` -> `<=`, or
replacing a function body with a default return — and reruns that crate's tests. A
mutant the tests still pass on ("missed" / survived) is a real gap: a behavior change
no test catches. It is the dual of the Stateright discipline above — exhaustive over
*source edits* rather than over *interleavings*.

- **Where it runs.** `.github/workflows/mutants.yml` runs on a SCHEDULE plus manual
  dispatch (mirroring `fuzz-nightly`; it is far too slow for the PR gate, since the
  suite reruns once per mutant). Each subsystem is its own matrix shard. The matrix
  starts with the pure, fast, deterministic crates — `kv`, `mvcc`, `pgtypes`,
  `catalog`, `pgparser` — and deliberately EXCLUDES the slow, timing-sensitive
  distributed suites (`cluster`, `crabgresql`): rerunning their Raft elections per
  mutant is slow and flaky. `executor`/`pgwire` are the next to fold in.
- **Config** lives in `.cargo/mutants.toml`: `test_tool = "nextest"` (matches the rest
  of CI), `test_workspace = false` (a mutant runs ONLY the mutated crate's own tests,
  so a subsystem run measures *that* subsystem's adequacy and stays fast), and a
  generous `minimum_test_timeout` so an infinite-loop mutant is bounded by the timeout
  on a starved runner rather than left to hang.
- **The discipline:** treat a surviving mutant like a surviving Stateright
  counterexample — investigate it and add the test that kills it. The nightly is
  informational (it REPORTS survivors via the job summary + an uploaded `mutants.out`
  artifact; it does not fail the PR), so the practice is to drive each subsystem to
  zero survivors over time. A mutant genuinely undetectable by a unit test — e.g.
  `KeyspaceKv::sync`, an fsync whose only observable effect is power-loss durability —
  is EXCLUDED via `exclude_re` *with a rationale*, so every remaining survivor stays
  actionable. Never exclude a mutant just to silence it.
- **Baseline (this slice):** `kv`, `mvcc`, `pgtypes`, `catalog`, and the FULL `pgparser`
  crate (lexer, parser, and the keyword table) are at zero missed mutants — every
  survivor the per-subsystem sweeps surfaced was killed with a targeted unit test. TWO
  mutants are excluded with a rationale (never merely to silence): `KeyspaceKv::sync` (a
  power-loss-only fsync, undetectable in-process) and `Parser::expr`'s `l_bp < min_bp`
  Pratt check — a provably EQUIVALENT mutant, since the odd-left / even-right binding-
  power scheme makes `l_bp == min_bp` unreachable, so `<` and `<=` decide identically on
  every input. The nightly drives each subsystem to zero the same way as new gaps surface.

## Windows UAC-safe target names (os error 740)

Windows UAC **installer-detection** refuses to launch (un-elevated) any executable
whose **filename** contains `setup`, `install`, `update`, `patch`, or `upgrad`
(matches `upgrade`), failing with os error 740 (`ERROR_ELEVATION_REQUIRED`). Cargo
derives a test/bin/example binary's filename from its **target name**, and an
integration-test file's name *is* its target name. So:

**Rule:** No `[[test]]` / `[[bin]]` / `[[example]]` target **name** — and no
integration-test **filename** under `crates/*/tests/` (which becomes a binary
target) — may contain the substrings `setup`, `install`, `update`, `patch`, or
`upgrad`. This is a **filename/target-name** constraint, not a content one: SQL
`UPDATE`/`DELETE` inside a test body is fine; only the compiled binary's name
matters. When in doubt, name the data-mutation test after what it asserts
(`mutation_semantics`), not the SQL keyword (`update_delete`).

**Guard (returns empty when clean):**

    git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'

plus a scan of every crate's `[[test]]/[[bin]]/[[example]] name = "…"` entries.

**SP14 audit (2026-06-13):** every integration-test binary passes — cluster
`{durable_scenarios, gateway_local, jepsen_bank, model, multirange, remote_forward,
scenarios, sql_durable, sql_over_raft}`; crabgresql `{jepsen_elle, multiprocess}` plus the new T6
`multirange_gateway`; executor `{concurrency, durability, end_to_end,
linearizable_reads, recovery, transactions, mutation_semantics}`; pgparser
`{libpg_query_oracle}`; pgwire `{cancel, extended_query, golden_trace, scram_auth,
simple_query, sqlx_driver, tls}` — and the four fuzz `[[bin]]` names (`parse_sql`,
`wire_decode`, `decode_row`, `decode_key`) and the shipped `crabgresql` binary. The
only file that previously tripped the guard, `update_delete.rs`, was renamed to
`mutation_semantics.rs` in this slice. The multi-process harness resolves children
via `env!("CARGO_BIN_EXE_crabgresql")`, which stays UAC-safe only while the binary
is named `crabgresql` — do not rename it.

**SP15 (2026-06-13):** two new binaries — `cluster::meta_range_replicated` and `crabgresql::meta_range_gateway` — both UAC-safe (no `setup/install/update/patch/upgrad` substring). The cluster list now reads `{durable_scenarios, gateway_local, jepsen_bank, meta_range_replicated, model, multirange, remote_forward, scenarios, sql_durable, sql_over_raft}`; the crabgresql list reads `{jepsen_elle, meta_range_gateway, multiprocess, multirange_gateway}`.

**SP16 (2026-06-14):** one new binary — `cluster::crossrange_2pc` (cross-range two-phase-commit proofs) — UAC-safe. The cluster list now also includes `crossrange_2pc`.

**SP17 (2026-06-14):** one new binary — `crabgresql::crossrange_2pc_net` (cross-range 2PC over the network, multi-process e2e) — UAC-safe (no `setup/install/update/patch/upgrad` substring). The crabgresql list now reads `{crossrange_2pc_net, jepsen_elle, meta_range_gateway, multiprocess, multirange_gateway}`. SP17 added no new test target with a forbidden substring; the full guard `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty.

**SP18 (2026-06-14):** one new binary — `crabgresql::crossrange_2pc_nemesis` (multi-process crash/partition-nemesis cross-range bank, fault-hardened 2PC recovery) — UAC-safe (no `setup/install/update/patch/upgrad` substring). The crabgresql list now reads `{crossrange_2pc_net, crossrange_2pc_nemesis, jepsen_elle, meta_range_gateway, multiprocess, multirange_gateway}`. SP18 added no new test target with a forbidden substring; the full guard `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty.

**SP19 (2026-06-14):** one new binary — `crabgresql::crossrange_2pc_replicated` (multi-process cross-range 2PC over the replicated meta-range layout, nemesis + full-cluster restart) — UAC-safe (no `setup/install/update/patch/upgrad` substring). The crabgresql list now reads `{crossrange_2pc_net, crossrange_2pc_nemesis, crossrange_2pc_replicated, jepsen_elle, meta_range_gateway, multiprocess, multirange_gateway}`. SP19 added the `arc-swap` dependency (growable `TxnService` engines registry) and no test target with a forbidden substring; the full guard `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty.

**SP20 (2026-06-14):** NO new test binary — the recovery-scan watermark (`clog_scan_lo`, bounds the leadership-rise in-doubt scan) is proven by in-crate `kv`/`executor`/`cluster` unit tests + the existing `crossrange_2pc_{replicated,nemesis}` e2e. No new dependency. The crabgresql binary list is unchanged. The full guard `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty.

**SP21 (2026-06-14):** one new test binary — `cluster::crossrange_2pc_model` (Stateright model of cross-range 2PC participant-`Stage` idempotency) — UAC-safe (no `setup/install/update/patch/upgrad`). No new `crabgresql` binary; no new dependency. The cross-range 2PC fixes — idempotent participant `Stage` per `(g, range)` (`executor::SqlEngine::staged_local_for` + the held-session-aware check in `twopc::TxnService::stage`) and the leadership-loss **resolve-then-release** (`twopc::resolve_and_release_for_range`) — are proven by in-crate `executor`/`cluster` unit tests + the Stateright model + the existing `crossrange_2pc_{nemesis,replicated}` e2e (regression). The full guard returns empty. **NOTE:** SP21's original "fresh-`g'` re-attempt" design was abandoned after the multi-process participant-leader-kill nemesis (driven by SP21's own `find_visible_one`/`scan_live` at-most-one-live `debug_assert!`) proved the real defects were PRE-EXISTING (SP18-era) — non-idempotent stage + pre-decision lock release — not a missing re-attempt. Full participant-leader-kill recovery robustness (a "settle-before-serve" redesign that gates a range's writes on its leadership-rise in-doubt sweep) is **deferred to a dedicated future slice**; the participant-leader-kill multi-process nemesis is not committed (it does not yet pass reliably against the deferred gap).

**SP22 (2026-06-14/15):** one new test binary — `cluster::crossrange_2pc_settle_model` (Stateright model of settle-before-serve: an un-gated write past an unsettled inherited in-doubt marker → duplicate live version; gated → safe) — UAC-safe (no `setup/install/update/patch/upgrad`). No new `crabgresql` binary; no new dependency. **SP22 ships the settle-before-serve gate for DATA ranges**: `cluster::RecoveryGate` (new dependency-free module — per-range, term-based `is_serving`/`mark_served`, gated-by-default), the leadership-rise sweep's apply-wait + `mark_served` (`server_node::resolve_in_doubt_on_leadership`), and the two write-path checks (`twopc::TxnService::stage` + `range::router::RangeRouter::dispatch`, Insert/Update/Delete only — reads/DDL/`FOR UPDATE` pass ungated). Proven by in-crate `cluster` unit tests (`recovery_gate`, the `stage`/router gate tests) + the Stateright model with teeth + the existing `crossrange_2pc_{nemesis,replicated}` regression. The cluster integration-test list now also includes `crossrange_2pc_settle_model`; the full guard `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty. **DEFERRED to a dedicated slice** (the participant-leader-kill multi-process nemesis did not converge): (1) **range 0 as a 2PC participant** is ungated/unswept — extending the gate+sweep needs the recovery scan bounded at `mvcc::xid::GLOBAL_XID_BASE` (range 0's clog mixes participant markers with the global decision clog) and the gateway-LOCAL participant stage gated **WITHOUT** a `staged_local_for` idempotency no-op (that no-op is UNSAFE under GTM xid-reuse → a committed-with-missing-half money tear); (2) a residual **cascading-failover 2PC-atomicity** gap (rare tear + recovery wedge under kill-every-round) — incremental patches shift the failure mode (wedge↔tear) without converging, so do NOT re-attempt inline. The deferred design is captured in the SP22 plan's "Task 6.5" section + the spec's "CORRECTION (as-shipped)".

**SP23 (2026-06-15):** two new test binaries — `cluster::crossrange_2pc_gtm_reuse_model` (Stateright model of GTM reseed-before-allocate: a reused global xid allocated before the range-0 rise sweep applied + reseeded → duplicate live version; reseed-then-allocate → safe) and `crabgresql::range0_leader_kill_drain` (multi-process nemesis that kills the RANGE-0 leader — GTM/coordinator home AND `acct_a` participant — each round, with a FULL-DRAIN stable window between kills, and asserts cross-range bank conservation) — both UAC-safe (no `setup/install/update/patch/upgrad` substring). No new dependency. **SP23 ships the GTM global-xid reuse/reseed fix for range-0 leadership change**: the range-0-safe recovery scan bound (`mvcc::xid::GLOBAL_XID_BASE`), the range-0 rise sweep (apply-wait → `reseed_gtm` → settle → open), and the `begin_global` gate on range-0's recovery gate — proven by in-crate `kv`/`executor`/`cluster` unit tests (T1–T3 gate teeth) + the Stateright model with teeth (T4) + the new `range0_leader_kill_drain` nemesis (T5, passed 3× non-flaky — single-failover recovery, the in-scope case). The crabgresql integration-test list now reads `{crossrange_2pc_nemesis, crossrange_2pc_net, crossrange_2pc_replicated, jepsen_elle, meta_range_gateway, multiprocess, multirange_gateway, range0_leader_kill_drain}`; the cluster list now also includes `crossrange_2pc_gtm_reuse_model`. The `range0_leader_kill_drain` nemesis is NOT `#[ignore]`'d (it converges with the full-drain stable window). The deferred cascading- (overlapping-) failover 2PC-atomicity gap from SP22 remains out of scope. The full guard `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty.

**SP24 (2026-06-15):** four new Stateright test binaries — `cluster::linearizable_read_model`, `cluster::mvcc_write_conflict_model`, `cluster::write_once_decision_model`, `cluster::recovery_watermark_model` — all UAC-safe (no `setup/install/update/patch/upgrad` substring). No new `crabgresql` binary; no new dependency. **SP24 backfills exhaustive models with teeth for four previously-unmodeled distributed interactions**, each a pure abstract `Model` (no openraft / SQL engine / I/O) with a boolean broken-variant toggle, canonical sorted state, a bounded step budget, and BOTH a positive `assert_properties()` (+ `unique_state_count() > 1`) test and a teeth test that proves the checker CATCHES the bug and names the specific safety property: (1) **linearizable reads / ReadIndex** (`linearizer::RaftLinearizer` + `twopc::Range0Barrier`) — a partitioned former leader serving a read off frozen local state is a stale read; the `read_index_check` gate rejects it (invariants: *no stale read / monotonic reads*, *a partitioned leader never serves a read*). (2) **single-range MVCC snapshot isolation + row-lock first-committer-wins** (`executor::lockmgr` + `mvcc::visibility` + `executor::procarray`) — two concurrent writers each superseding the stale snapshot base lose an update → two live versions; the `lock_writes` row lock serializes them (invariants: *at most one live version*, *no lost update*). (3) **write-once global-decision agreement / abort-race** (`store::apply_op` `is_terminal` keep + `executor::commit_global_decision` read-back-effective) — an overwritable decision lets one party commit while another aborts the same `g`; `write_once` keeps the first decision immutable (invariants: *global decision agreement*, *terminal decision is stable*). (4) **recovery clog-scan watermark** (`executor::in_doubt_globals_from` / `advance_clog_scan_lo`) — dropping the `first_undecided` floor advances the watermark past an in-doubt marker beneath a terminal one, orphaning it; the floor keeps every in-doubt marker discoverable (invariants: *no in-doubt marker below the watermark*, *every in-doubt g stays discoverable*). The cluster integration-test list now also includes `{linearizable_read_model, mvcc_write_conflict_model, recovery_watermark_model, write_once_decision_model}`. The full guard `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty.

**SP25 (2026-06-16):** one new test binary — `crabgresql::participant_kill_bank` (multi-process nemesis: cross-range 2PC **committed-half survival** under a clean PARTICIPANT-leader kill) — UAC-safe (no `setup/install/update/patch/upgrad` substring). No new `cluster` integration-test binary; no new dependency. **SP25 proves + completes committed-half survival on the participant range.** When a participant range's leader is killed mid-2PC (between the global `g -> Committed` decision and the participant's local release), the committed `g`'s half is a durable `Prepared(Lb -> g)` version whose in-memory held session died with the old leader; the newly-risen participant leader reconstructs every inherited in-doubt marker (its rise sweep `resolve_in_doubt_on_leadership` drives each to its durable global decision) before serving, and the staged version + that decision re-apply the committed half as the sole live version — so it SURVIVES. The slice closes the **last ungated write path**: `range::router::RangeRouter::stage_on`'s local branch (the gateway-local participant stage) now applies the same settle-before-serve gate as `dispatch` and the remote `twopc::TxnService::stage` — GATE ONLY, no `staged_local_for` idempotency no-op (unsafe under GTM xid reuse). Proven by the new `cluster` unit test `router_gates_a_local_led_participant_stage_until_the_range_is_settled` (teeth: a local participant stage is rejected `40001` while its range is unsettled, admitted once `mark_served`), the existing Stateright model `crossrange_2pc_settle_model` (the at-most-one-live committed-half-survival invariant, with teeth), and the new `participant_kill_bank` nemesis (passes 6×, non-flaky). The crabgresql integration-test list now reads `{crossrange_2pc_nemesis, crossrange_2pc_net, crossrange_2pc_replicated, jepsen_elle, meta_range_gateway, multiprocess, multirange_gateway, participant_kill_bank, range0_leader_kill_drain}`. **SCOPE (mirrors SP23's single-failover scoping):** `participant_kill_bank` kills only a node that leads range 1 but NOT range 0 (range 0 = GTM/coordinator/`acct_a` stays stable), with full recovery between kills — isolating PARTICIPANT-range committed-half survival from the DEFERRED range-0/coordinator co-failover case (GTM global-xid reuse under an in-flight range-0 leadership change; the SP22/SP23 cascading-failover non-goal). The full guard `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty.

**SP26 (2026-06-16):** two new test binaries — `cluster::crossrange_2pc_overlap_settle_model` (Stateright model of settle-COMPLETE-before-serve: a rise sweep that opens its write gate while an inherited in-doubt marker is still in-doubt → a new write supersedes AROUND it → duplicate live version when it commits; settle-complete → safe) and `crabgresql::range0_cascade_kill_bank` (multi-process nemesis: an OVERLAPPING range-0-leader kill — GTM + global-clog home + `acct_a` participant — every round with NO full-drain window, asserting cross-range bank conservation via an AUTHORITATIVE read) — both UAC-safe (no `setup/install/update/patch/upgrad` substring). No new dependency. **SP26 closes the SP22/SP23-deferred cascading/overlapping-failover atomicity tear for the range-0 overlapping-leader-kill case.** Root cause: `server_node::resolve_in_doubt_on_leadership` opened the gate (`mark_served`) on apply-wait/reseed success REGARDLESS of whether every inherited in-doubt `Prepared(Li -> g)` marker's (best-effort, warn-only) abort-race actually landed; under an overlapping failover (the risen leader churns mid-sweep) a marker stays in-doubt when the gate opens, a new gated write supersedes the OLDER head (the in-doubt marker is invisible), and when the marker later commits BOTH versions go live (the at-most-one-live torn total). Fix: the sweep RE-SCANS `in_doubt_globals_from` after the abort-races and `mark_served` ONLY when no marker remains in-doubt (else the settle fails and the gate stays closed, retrying until a stable leader finalizes every marker — genuine settle-*before*-serve, which CONVERGES). Preserves committed-half survival (only non-terminal `g`s are abort-raced). Proven by the Stateright model with teeth + the new `range0_cascade_kill_bank` nemesis (passes 4× non-flaky) + the existing `crossrange_2pc_{nemesis,replicated}` / `range0_leader_kill_drain` / `participant_kill_bank` regression. The crabgresql integration-test list now reads `{crossrange_2pc_nemesis, crossrange_2pc_net, crossrange_2pc_replicated, jepsen_elle, meta_range_gateway, multiprocess, multirange_gateway, participant_kill_bank, range0_cascade_kill_bank, range0_leader_kill_drain}`; the cluster list now also includes `crossrange_2pc_overlap_settle_model`. **DEFERRED (a NEW finding, scoped — see the SP26 spec Non-goals):** reproducing the tear surfaced a `−money` signature that is a READ-staleness, not a durability loss — a lagging FOLLOWER gateway can transiently resolve a just-committed cross-range `acct_b` credit as still-in-doubt (its local range-0 GTM view lags) and under-report it; an authoritative read (the GTM home, range-0's leader) always sees it, so the conservation oracle reads there. Tightening cross-range read linearizability for lagging followers under extreme range-0 churn is a separate read-path concern (SP12/SP24 territory). The full guard `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty.

**SP27 (2026-06-16):** the FIRST breadth slice after eleven straight distribution-depth slices (SP16–SP26) — **aggregates + `GROUP BY`/`HAVING`**: `COUNT(*)`, `COUNT(x)`, `SUM`, `MIN`, `MAX`, their `DISTINCT` forms, multi-key `GROUP BY`, and `HAVING`. One new test binary — `executor::aggregates` (end-to-end over the wire: grouped counts/sums, `HAVING`, `DISTINCT`, empty-input, result types, and the `42803`/`42883`/`0A000` error surface) — UAC-safe (no `setup/install/update/patch/upgrad` substring). No new `cluster`/`crabgresql` binary; **no new dependency**. Parser: new keywords `GROUP`/`HAVING`/`DISTINCT`/`ALL`, an `Expr::Func(FuncCall{name,distinct,args: Star|Exprs})` node, and `SelectStmt.{group_by,having}`. Types: `Datum` now derives `Eq + Hash` (sound — no float) so it keys group maps + `DISTINCT` sets. Executor: a new `executor::agg` module (aggregate registry, per-group `DISTINCT`-aware accumulators, insertion-ordered grouping, the data-independent grouped-validator + grouped-evaluator, `execute_aggregate`); `eval`/`infer_type` learn an `Expr::Func` arm; `exec::execute_read` routes aggregate queries after `WHERE`; `execute_read_locking` rejects aggregation `0A000`; new `ExecError::{Grouping(42803),UndefinedFunction(42883)}`. **NO Stateright model — deliberate and justified:** a whole table lives on one range (`RangeMap::range_for_table`), so an aggregate query executes entirely inside one `execute_read` on one engine — a pure, deterministic fold over the already-correct MVCC-visible row set, with NO cross-range scatter, no new lock/visibility rule, and no new interleaving. This is exactly CLAUDE.md's "pure-data / single-node refactor with no concurrency/fault dimension may not warrant one" carve-out; a model of a fold would have an interleaving-free state space and merely restate the unit tests. Proven instead by `pgparser` parser tests, 17 `executor::agg` unit tests (every function, `NULL`-skip, `DISTINCT`, empty/all-null group, `SUM` overflow → `22003`, grouping/undefined/nested-aggregate errors), the `executor::aggregates` wire test, and `conformance/corpus/aggregates.sql` (diffed against PG 18 in CI). **Type deviations (documented):** `SUM(int8)` returns `int8` (PG: `numeric`) — in-range sums print identically, out-of-`i64` raises `22003`; **`AVG` is deferred** to a future `numeric`/float-type slice. The full guard `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty.

**SP28 (2026-06-16):** breadth wave 2 — **predicate + conditional expression breadth**: `expr IS [NOT] NULL`, `expr [NOT] IN (list)` (value-list, not a subquery), `expr [NOT] BETWEEN low AND high`, `expr [NOT] LIKE`/`ILIKE` (with `%`/`_` wildcards + `\` escape), `CASE` (searched + simple), `SELECT DISTINCT`, and `OFFSET`. One new test binary — `executor::predicates` (end-to-end over the wire: three-valued NULL semantics for IS NULL/IN/BETWEEN/LIKE, CASE bucketing, DISTINCT dedup, LIMIT/OFFSET paging, and the `42804`/`0A000` error surface) — UAC-safe (no `setup/install/update/patch/upgrad` substring). No new `cluster`/`crabgresql` binary; **no new dependency** (the LIKE matcher is hand-written, iterative backtracking over `Vec<char>`). Parser: new keywords `IS`/`IN`/`BETWEEN`/`LIKE`/`ILIKE`/`CASE`/`WHEN`/`THEN`/`ELSE`/`OFFSET` (reusing `END`), five `Expr` variants (`IsNull`/`InList`/`Between`/`Like`/`Case`), and `SelectStmt.{distinct,offset}`; the postfix predicates bind at the comparison level in the Pratt loop, with a two-token lookahead disambiguating infix `NOT IN`/`NOT BETWEEN`/`NOT LIKE` from prefix `NOT`, and BETWEEN's bounds parsed above `AND`'s precedence so `a BETWEEN 1 AND 2 AND b` groups correctly. Executor: shared pure-`Datum` combinators in `eval` (`eval_in_list`/`eval_between`/`eval_like`/`like_match`/`eval_case`) reused by both scalar `eval` and `agg::eval_grouped`; `infer_type` types predicates as `bool` and unifies CASE branch types (int4→int8 promotion, NULL-branch type-neutral, incompatible → `42804`); `agg`'s four `Expr` matches (`contains_aggregate`/`collect_specs`/`validate_grouped`/`eval_grouped`) all learn the new variants; `exec::project_order_limit` + `agg::execute_aggregate` thread DISTINCT (dedup via `HashSet<Vec<Datum>>`) and OFFSET (shared `apply_offset_limit`, OFFSET-then-LIMIT); `execute_read_locking` rejects `DISTINCT`/aggregation `0A000`; new `TypeError::InvalidEscape (22025)` for a trailing-`\` LIKE pattern. **NO Stateright model — deliberate and justified (identical to SP27):** every feature is a deterministic scalar/row transform over the already-correct, MVCC-visible, single-range row set inside one `execute_read` on one engine (a whole table lives on one range) — no new lock, visibility rule, write path, or interleaving; a model of a fold would have an interleaving-free state space and merely restate the unit tests. Proven instead by `pgparser` parser + libpg_query-oracle tests, `executor::{eval,agg}` unit tests (every NULL-semantics edge case + the LIKE matcher in isolation: `%`/`_`/escape/ILIKE-fold/22025), the `executor::predicates` wire test, and `conformance/corpus/predicates.sql` (diffed against PG 18 in CI). **Documented deviations:** ILIKE folds ASCII only; a trailing lone `\` in a LIKE pattern is `22025` (PG-compatible); predicate/CASE output columns derive the name `?column?`; an all-NULL `CASE` types as `text`; `SELECT DISTINCT … ORDER BY <not-in-list>` is `0A000` (PG: `42P10`); negative `OFFSET`/`LIMIT` clamp to 0 (PG raises `22023`). The full guard `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty.

**SP29 (2026-06-16):** breadth wave 3 — **scalar (row) functions + the `||` operator**: string `length`/`char_length`/`character_length`, `upper`, `lower`, `btrim`/`ltrim`/`rtrim`, `substr`/`substring` (the comma form), `replace`, `concat`; math `abs`, `mod`; null/conditional `coalesce`, `nullif`, `greatest`, `least`; and the `||` string-concatenation operator. One new test binary — `executor::scalar_functions` (end-to-end over the wire: the string/math/conditional functions, `||`, the WHERE/ORDER BY/aggregate composition, and the `42883`/`42809`/`42804` error surface) — UAC-safe (no `setup/install/update/patch/upgrad` substring). No new `cluster`/`crabgresql` binary; **no new dependency**. Parser: NO new keyword (every function name is a plain identifier resolved by the executor, reusing SP27's `Expr::Func(FuncCall)` node), only a new `||` token (`Token::Concat`) + `BinaryOp::Concat`; the Pratt ladder inserts `||` between the comparison level (5/6) and the additive operators — like PostgreSQL, `||` binds TIGHTER than `< > = <= >= <>`/`BETWEEN`/`IN`/`LIKE`/`AND`/`OR` but LOOSER than `+ - * /` — so `+ - * /` and the unary-minus operand power shift up by two (odd-l_bp/even-r_bp preserved). Types: `pgtypes::ops` gains `rem` (the `mod` value op; `wrapping_rem` makes `i32::MIN % -1` the mathematically-correct `0`, never a 22003 trap) and `concat` (the `||` value op — NULL-propagating, each operand rendered via its canonical wire text encoding so `||`/`concat`/DataRow never disagree). Executor: a new `executor::func` module (the scalar-function registry + per-function arity/type validation + the pure combinators), wired through `eval`'s `Expr::Func` arm (scalar dispatch, else the SP27 aggregate-context error), `infer_type` (`scalar_result_type` + the `||` "≥1 text operand" plan-time rule), `apply_binary` (`BinaryOp::Concat` → `ops::concat`), and `agg`'s `collect_specs`/`validate_grouped`/`eval_grouped` (a scalar function may wrap aggregates / grouped columns — same shared-combinator/closure-differs pattern as SP28); new `ExecError::WrongObjectType` (42809, `DISTINCT` on a non-aggregate); `eval::{unify_types,unify_branch}` are now `pub(crate)` (shared by `CASE` and `coalesce`/`greatest`/`least`). **NO Stateright model — deliberate and justified (identical to SP27/SP28):** every function is a pure, deterministic scalar transform over the already-correct, MVCC-visible, single-range row set inside one `execute_read`/`eval` on one engine (a whole table lives on one range) — no new lock, visibility rule, write path, or interleaving; a model of a scalar fold would have an interleaving-free state space and merely restate the unit tests (CLAUDE.md's "pure-data / single-node refactor" carve-out). Proven instead by `pgparser` lexer/parser tests (the `||` token + its precedence/associativity), `pgtypes::ops` unit tests (`rem` sign/promotion/zero/`MIN%-1`, `concat` rendering/NULL — the mutation-baseline crate stays at zero survivors), 10 `executor::func` unit tests (every function, NULL strictness, `coalesce` short-circuit, the error surface), the `executor::scalar_functions` wire test, and `conformance/corpus/scalar_functions.sql` (diffed against PG 18 in CI). **Documented deviations:** `upper`/`lower` fold full-Unicode while the slice's lexer treats string-literal bytes verbatim (so the corpus stays ASCII); the "`||` requires ≥1 text operand" rule is enforced at plan time over PROJECTED expressions (matching PG's plan-time operator resolution), so a numeric-only `||` in a non-projected `WHERE`/`HAVING` position computes a text result instead of raising 42883 — the same plan-time-vs-runtime split the engine already has for arithmetic type errors; `coalesce`/`greatest`/`least` do not coerce the returned Datum to the unified type (text output is correct; a mixed int4/int8 result's binary OID/width may differ); `substr`'s negative-length 22011 is surfaced as 42804. The full guard `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty.
