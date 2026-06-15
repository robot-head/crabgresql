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
