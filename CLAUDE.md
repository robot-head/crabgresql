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
