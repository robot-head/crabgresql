# crabgresql — agent instructions

A from-scratch PostgreSQL-compatible distributed database in Rust 2024, built one
vertical "slice" at a time.

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
