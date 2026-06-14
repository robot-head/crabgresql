# SP20 / D3c-gc-scan — Bound the cross-range recovery scan via a per-range watermark

**Date:** 2026-06-14
**Slice:** SP20 (D3c-gc-scan)
**Status:** design

## Summary

The leadership-rise recovery sweep (`resolve_in_doubt_on_leadership` → `in_doubt_globals`)
prefix-scans a data range's **entire** `/0/clog` keyspace on every leadership rise, so its
cost is **O(all cross-range txns this range ever participated in)** — terminal `Prepared(Li→G)`
markers are scanned, decoded, found-already-decided, and skipped, but never removed. On a
long-running cluster each leadership change gets slower without bound. This is the documented
SP18/SP19 risk ("the leadership-rise sweep is O(in-doubt markers)").

SP20 bounds that scan with a **durable per-range watermark** `scan_lo`: the smallest local xid
`Li` at/after which the recovery scan must still look. The scan starts at `clog_key(scan_lo)`
instead of the whole prefix, and `scan_lo` advances past the contiguous prefix of markers whose
global `G` is durably terminal. **Nothing is deleted and no row's visibility changes** — this is
a scan-start optimization, not garbage collection of bytes.

## Where this sits: the roadmap

The D3c cross-range-transaction arc is complete through SP19 (in-process 2PC → network →
fault-hardened → replicated layout). SP20 is the first **consolidation** slice: it pays down the
one documented unbounded-cost liability that arc created, with the lowest risk of any candidate.

**Why this is NOT byte-reclaiming GC** (the load-bearing finding from the anchor map): a
cross-range-written row is **never frozen locally**. `commit_release`/`abort_release`
(`crates/executor/src/session.rs:690-699`) write **no** per-participant clog entry — the comment
is explicit: "the single `Committed(g)` write makes them visible; here we only free row locks…
(NO per-participant clog write)". Forever after, a row's visibility is resolved by `global_status`
(`crates/executor/src/exec.rs:329-345`): read `clog[Li]=Prepared(g)` locally, then dereference to
range 0's `clog[g]`. So **deleting a `Prepared(Li→G)` marker** makes `clog[Li]` absent → `get`
returns `InProgress` (`crates/mvcc/src/clog.rs:33`) → a *committed* row silently **vanishes**; and
**deleting a terminal `clog[G]`** flips every committed row of `G` invisible cluster-wide. True
byte reclamation therefore needs an MVCC **freeze** pass (rewrite `Prepared` rows into a
decision-independent form) plus a durable oldest-active-`G` **horizon** — neither exists; that is a
future **vacuum arc**, explicitly out of scope here.

SP20 attacks only the **scan cost**, which is safely boundable with no deletion and no freeze.

## The load-bearing constraints (why the design is shaped this way)

1. **Markers are never deleted; visibility is untouched.** `global_status`/`eval_plan_qual`
   (`exec.rs:340,422`) must keep resolving every row exactly as today. The watermark governs only
   *where the recovery scan starts*, never what exists.

2. **`scan_lo` must never advance past a marker whose `G` is not durably terminal.** The
   leadership-rise sweep abort-races each in-doubt `G` to a terminal decision (`CommitGlobal{g,
   commit:false}` → write-once). That abort-race is what protects against a **late zombie commit**:
   a delayed coordinator `CommitGlobal{commit:true}` arriving after everyone presumed-aborted finds
   `clog[g]` already write-once-locked `Aborted` and cannot flip it. If the watermark skipped an
   in-doubt marker, the sweep would stop abort-racing it and a late zombie commit could resurrect a
   presumed-aborted, client-rolled-back txn — a consistency violation. So the watermark may advance
   **only past markers whose `G` is already terminal** (a re-decision of which is a write-once
   no-op); every still-in-doubt `G` keeps being scanned and abort-raced every leadership rise.

3. **The watermark must be durable and survive failover/restart.** It is written through the data
   range's Raft committer (replicated, fsync-durable) and re-read by the next leader. It only ever
   advances (monotone), so a stale-but-lower value is always safe (a larger scan, never an unsafe
   skip).

4. **Real cost reduction needs a physical range scan.** `in_doubt_globals` uses
   `kv::Kv::scan_prefix` (`crates/kv/src/store.rs:25`), which materializes *every* clog key. To
   actually start at `clog_key(scan_lo)` (not just filter after materializing), the `Kv` trait needs
   a bounded **range scan**; both backends support it natively (fjall range query; `MemKv`'s
   `BTreeMap::range`).

## Decisions (locked during brainstorming)

1. **Watermark-only, no deletion (Approach A).** Bound the recovery **scan cost**; do not delete
   any marker or `clog[G]`. Byte reclamation is deferred to a future vacuum/freeze arc (see
   Non-goals). This is the only safe single-slice move given constraint above.

2. **Per-range scan watermark, no global horizon.** `scan_lo` is local to each data range's clog
   and governs only that range's recovery scan. No global oldest-active-`G` / advancing global
   `xmin` is introduced (that is the byte-reclamation horizon, out of scope).

3. **Host on the existing leadership-rise sweep.** Advance the watermark inside
   `resolve_in_doubt_on_leadership` (`crates/cluster/src/server_node.rs:600-626`) — the edge-driven,
   no-sleep, stable-leader sweep that already runs `in_doubt_globals`. (Not the per-node 500ms
   `participant_silence_sweeper`.)

4. **Prove the cost-bound with a deterministic executor unit test, not a new debug RPC.** A unit
   test over an in-memory engine asserts the scan examines only `[scan_lo, end)`, returns the correct
   in-doubt set, and advances the watermark only past the terminal prefix. No `CountKeys` control RPC
   is added. A separate correctness check (recovery + conservation still hold across a leadership
   change/restart with the watermark active) reuses the existing `crossrange_2pc_replicated` /
   `crossrange_2pc_nemesis` harness.

## Components

### 1. Bounded range scan on the `Kv` trait (`crates/kv/src/store.rs`, `fjall_store.rs`)

Add `fn scan_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, KvError>`
(start inclusive, end exclusive), implemented for `MemKv` (`BTreeMap::range(start..end)`) and the
fjall-backed store (a native range query). `scan_prefix` stays (other callers unaffected). A small
helper computes the clog prefix's exclusive upper bound (the prefix with its trailing byte
incremented, or `clog_key(u64::MAX)`'s successor) so the recovery scan covers `[clog_key(scan_lo),
clog_prefix_end)`.

### 2. Durable per-range watermark key (`crates/kv/src/key.rs`, written via the range committer)

A system key outside the clog prefix — e.g. `clog_gc_lo_key()` = `system_prefix("clog_gc_lo")` — so
it is never itself returned by a clog scan. Value = `Li` big-endian (`u64`). Read with the range's
applied store; written through the data range's Raft committer (the same durable path SQL writes
use), so it is replicated and survives restart/failover. Absent ⇒ `scan_lo = 0` (scan from the
start — safe, just the current full-scan behavior).

### 3. `in_doubt_globals` becomes watermark-aware (`crates/executor/src/lib.rs:274-291`)

Add `in_doubt_globals_from(&self, scan_lo: u64) -> Result<(Vec<u64>, u64), ExecError>`: scan
`self.kv.scan_range(clog_key(scan_lo), clog_prefix_end)`; for each `Prepared(Li→G)` marker, classify
`G` via `self.catalog_kv`'s clog (terminal vs not). Return `(in_doubt_gs, new_scan_lo)` where
`new_scan_lo` = the smallest scanned `Li` whose `G` is **not** durably terminal, or the end-of-scan
sentinel if every scanned marker is terminal. The existing `in_doubt_globals()` becomes
`in_doubt_globals_from(0)` projecting just the `Vec<u64>` (preserving its current callers/tests).

**Data-range scope.** The watermark and `in_doubt_globals_from` apply only on **data ranges**
(`range != 0`) — the only ranges where `resolve_in_doubt_on_leadership` is spawned (per the SP18/SP19
wiring). A data range's clog holds only local-xid (`Li < 2⁶³`) entries (ordinary + `Prepared`
markers). Range 0 additionally holds the global `clog[G]` decisions at the **high** end of the clog
keyspace (`G ≥ GLOBAL_XID_BASE = 2⁶³`) and does **not** run the recovery sweep, so an `Li` watermark
never interacts with global-`G` entries. The implementation must keep the watermark/`from`-scan a
data-range-only path (do not invoke it on range 0).

### 4. Advance + persist in the sweep (`crates/cluster/src/server_node.rs:600-626`)

`resolve_in_doubt_on_leadership`: on the rising edge, read the durable `scan_lo`, call
`engine.in_doubt_globals_from(scan_lo)`, abort-race each returned in-doubt `G` (unchanged), then —
because those races make their `G`s terminal — durably write the advanced `scan_lo` (the returned
`new_scan_lo`, never decreasing) through the range committer. A failed abort-race (range 0
unreachable) leaves that `G` non-terminal, so `new_scan_lo` does not pass it — it is re-scanned next
rise. The watermark write is once per leadership rise (rare), only when it advances.

### 5. The safety invariant (the correctness core)

`scan_lo` advances **only** past markers whose `G` is durably terminal. Consequences:
- Every still-in-doubt `G` is scanned and abort-raced on every leadership rise (zombie-commit
  protection intact — constraint 2).
- Skipped markers all have a terminal, write-once-locked `G`; re-deciding them would be a no-op, so
  not scanning them changes nothing.
- The scan is bounded by `[oldest-still-undecided Li, end)` — O(in-flight + recently-settled),
  not O(all-txns-ever).
- The watermark only ever advances; a stale lower value just yields a larger (still-correct) scan.

## Data flow (a leadership rise after many cross-range txns)

1. A data range has 1,000,000 `Prepared(Li→G)` markers, all but the most recent 3 already terminal.
2. A node wins that range's leadership. `resolve_in_doubt_on_leadership` reads `scan_lo` (≈ the
   999,997th marker), calls `in_doubt_globals_from(scan_lo)`.
3. The scan materializes only `[scan_lo, end)` ≈ 3 markers, returns the 3 in-doubt `G`s.
4. The sweep abort-races those 3 `G`s → terminal. `new_scan_lo` = end-of-scan; it is written durably.
5. The next leadership rise scans ≈ 0 markers. The recovery scan is bounded regardless of lifetime
   txn count. (Before SP20: step 3 scanned all 1,000,000.)

## Success criteria

| # | Criterion | Verified by |
|---|---|---|
| 1 | `Kv::scan_range(start, end)` returns exactly the keys in `[start, end)` in order, for `MemKv` and the fjall store. | `kv` unit tests |
| 2 | `in_doubt_globals_from(scan_lo)` scans only `[clog_key(scan_lo), end)`, returns the correct in-doubt `G`s, and computes `new_scan_lo` = smallest scanned `Li` with a non-terminal `G` (end if all terminal). | `executor` unit test |
| 3 | The recovery scan cost is **bounded**: with N terminal markers below `scan_lo` and k in-doubt above it, the scan examines O(k + markers-above-watermark), not O(N+k). Deterministically asserted (scanned-count). | `executor` unit test |
| 4 | The watermark **never advances past a non-terminal `G`** (a still-in-doubt marker holds it back); it is monotone and durable across restart/leadership change. | `executor` + in-crate `cluster` test |
| 5 | Markers are never deleted and **row visibility is unchanged** — all SP16/SP17/SP18/SP19 cross-range conservation + recovery suites pass unchanged with the watermark active. | regression gate (`crossrange_2pc`, `jepsen_bank` cross-range, `crossrange_2pc_net/nemesis/replicated`) |
| 6 | Recovery correctness holds with the watermark: a failed-over participant with in-doubt markers above `scan_lo` is still finalized (abort-raced), and conservation holds across a leadership change/restart. | reuse `crossrange_2pc_replicated`/`nemesis` (extend if needed) |
| 7 | Full gauntlet green; no new shipped dependency; `#![forbid(unsafe_code)]`; traceability. | gauntlet + traceability |

## Test plan

**Sleep policy.** The cost-bound proof is an in-process deterministic unit test (no waits). The
correctness-across-restart check reuses the existing harness's bounded poll cadence + condition
waits — no settle-sleep.

1. **`scan_range` (kv unit)** — populate a `MemKv` (and, where feasible, the fjall store) with keys
   straddling `[start, end)`; assert the returned set is exactly the in-range keys, ordered. Edge
   cases: empty range, start==end, start below all / above all keys.
2. **`in_doubt_globals_from` (executor unit)** — build an in-memory engine; write a mix of terminal
   (`G` decided in `catalog_kv`) and in-doubt `Prepared(Li→G)` markers at increasing `Li`; assert:
   (a) `from(0)` returns all in-doubt `G`s and `new_scan_lo` = smallest in-doubt `Li`; (b)
   `from(scan_lo)` scans only `[scan_lo, end)` (assert the scanned-key count equals `end - scan_lo`,
   not the total); (c) an in-doubt marker below a terminal one holds the watermark back.
3. **Watermark durability + monotonicity (in-crate cluster test)** — using `testonly_two_range_node`
   or the in-process cluster: stage participants, decide some, leave one in-doubt; drive a leadership
   rise; assert `scan_lo` advanced past the terminal prefix, stopped at the in-doubt marker, and
   persists (re-read) — and never decreases on a second rise.
4. **Recovery correctness with the watermark (e2e)** — reuse `crossrange_2pc_replicated` /
   `crossrange_2pc_nemesis`: with the watermark active, a coordinator-crash in-doubt `G` is still
   finalized on the new leader's rise and the bank total is conserved across the nemesis + restart.
   (Extend an existing test rather than add a binary if practical.)
5. **Regression** — the full SP16–SP19 cross-range conservation + recovery suites stay green (the
   watermark must not change any decision or visibility).
6. **Gauntlet** — `cargo fmt --all --check`; `cargo clippy --workspace --all-targets -- -D
   warnings`; `cargo nextest run --workspace` + `cargo test --workspace --doc`; `cargo deny check`;
   UAC guard; traceability.

## Non-goals (explicit → later)

- **Byte reclamation / true GC** — deleting `Prepared(Li→G)` markers or terminal `clog[G]` records.
  Unsafe without an MVCC **freeze** pass (rewrite cross-range rows into decision-independent form)
  and a durable **oldest-active-`G` horizon**; that is a future **vacuum arc**, not this slice.
- **Global `clog[G]` truncation** (range 0) — same blocker (freeze + horizon).
- **MVCC version vacuum** of ordinary single-range tuples — separate, larger subsystem.
- **A durable in-flight-`G` set / advancing global `xmin`** — only needed for byte reclamation.
- **A per-`G` participant registry** — marker handling stays per-range; not required for the scan
  watermark.

## Risks (and mitigations)

- **Advancing the watermark past an in-doubt `G` would silently disable zombie-commit protection.**
  The invariant (advance only past durably-terminal `G`) is the whole correctness core; mitigated by
  computing `new_scan_lo` strictly as the smallest scanned `Li` whose `G` is non-terminal, proven by
  the executor unit test (criterion 4) and the recovery e2e (criterion 6). A wrong watermark is the
  one real landmine — spend the rigor here.
- **The watermark write must be durable + replicated**, else a new leader could re-use a stale or
  lost value. Mitigated by writing through the range's Raft committer (same path as SQL writes) and
  by monotonicity (a lower value only enlarges the scan, never skips unsafely).
- **`scan_range` correctness across backends.** A subtly-wrong range bound could drop in-doubt
  markers (missing a recovery) or over-scan (no harm). Mitigated by the kv unit test (criterion 1)
  and by choosing an inclusive-start/exclusive-end convention matching `BTreeMap::range` and the
  fjall range query.
- **Misreading the slice as freeing disk.** This slice bounds scan *time*, not disk *bytes*;
  markers/clog still grow on disk. Documented in the spec + commit messages; byte reclamation is the
  explicit Non-goal (vacuum arc).
- **No new failure mode.** No deletion, no protocol change, no new RPC — the blast radius is the
  recovery scan's start offset and a monotone durable counter.

## Traceability

(Appended at finish — maps each success criterion 1–7 to its proving test.)
