# SP20 / D3c-gc-scan — Recovery-scan watermark — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound the leadership-rise recovery scan (`resolve_in_doubt_on_leadership` → `in_doubt_globals`) from O(all cross-range txns ever) to O(markers-since-oldest-still-in-doubt), via a durable per-range `scan_lo` watermark. No deletion, no visibility change.

**Architecture:** Add a bounded `Kv::scan_range(start, end)`; a durable per-range watermark key (`meta` namespace); a watermark-aware `in_doubt_globals_from(scan_lo) -> (Vec<g>, new_scan_lo)` on `SqlEngine` (the existing `in_doubt_globals()` delegates to `from(0)`); and advance+persist the watermark inside the leadership-rise sweep, **only past markers whose G is durably terminal** (preserving SP18's zombie-commit protection). Prove the cost-bound with a deterministic executor unit test.

**Tech Stack:** Rust 2024, openraft, fjall, cargo-nextest. Spec: `docs/superpowers/specs/2026-06-14-crabgresql-sp20-d3c-gc-scan-recovery-watermark-design.md`.

**Reference anchors (read before starting):**
- `crates/kv/src/store.rs` — `Kv` trait (`:19-29`), `MemKv::scan_prefix` (`:59-68`), `write_batch`.
- `crates/kv/src/fjall_store.rs` — the two `scan_prefix` impls (`:55`, `:114`) to mirror for `scan_range`.
- `crates/kv/src/key.rs` — `clog_key` (`:80`), `clog_prefix` (`:88`), `clog_xid_of` (`:93`), `system_prefix`, `meta_next_global_xid_key` (`:73`, the watermark-key sibling), `keyenc::put_u64` (big-endian, so clog keys sort by xid).
- `crates/executor/src/lib.rs` — `in_doubt_globals` (`:274-291`), `commit_global_decision` (`:242-262`, the committer-write pattern), the `committer` field (`:61`).
- `crates/executor/src/commit.rs` — `Committer::commit(&self, ops: Vec<WriteOp>)` (`:12-15`).
- `crates/mvcc/src/clog.rs` — `is_terminal` (`:70-72`), `decode`, `get`, `XidStatus`.
- `crates/cluster/src/server_node.rs` — `resolve_in_doubt_on_leadership` (`:600-626`, the sweep host).

**Stale-IDE warning:** rust-analyzer squiggles in this repo lag the committed tree and are routinely wrong mid-edit (E0599/E0063/E0425 were false-flagged repeatedly in prior slices). Trust `cargo build`/`clippy`/`nextest` ONLY.

---

## Task 1: Bounded `Kv::scan_range` + clog watermark key helpers

**Files:**
- Modify: `crates/kv/src/store.rs` (`Kv` trait + `MemKv` impl)
- Modify: `crates/kv/src/fjall_store.rs` (both backend impls)
- Modify: `crates/kv/src/key.rs` (`clog_scan_lo_key`, `clog_scan_end`)
- Test: `crates/kv/src/store.rs` `#[cfg(test)]`

- [ ] **Step 1: Write the failing `scan_range` unit test**

Add to `crates/kv/src/store.rs`'s `#[cfg(test)] mod tests` (mirror `scan_prefix_returns_sorted_matches_only`):
```rust
    #[test]
    fn scan_range_returns_inclusive_start_exclusive_end_in_order() {
        let kv = MemKv::new();
        for i in [1u8, 3, 5, 7, 9] {
            kv.put(vec![b'k', i], vec![i]).expect("put");
        }
        // [k3, k7) -> k3, k5 (k7 excluded).
        let got = kv.scan_range(&[b'k', 3], &[b'k', 7]).expect("scan_range");
        assert_eq!(got, vec![(vec![b'k', 3], vec![3]), (vec![b'k', 5], vec![5])]);
        // start below all, end above all -> everything.
        let all = kv.scan_range(&[b'k', 0], &[b'k', 255]).expect("scan_range");
        assert_eq!(all.len(), 5);
        // empty range (start == end).
        assert!(kv.scan_range(&[b'k', 5], &[b'k', 5]).expect("scan").is_empty());
        // start above all.
        assert!(kv.scan_range(&[b'k', 200], &[b'k', 255]).expect("scan").is_empty());
    }
```

- [ ] **Step 2: Run it — expect FAIL** (`scan_range` does not exist). `cargo nextest run -p kv -E 'test(scan_range)'`.

- [ ] **Step 3: Add `scan_range` to the `Kv` trait + `MemKv`**

In `crates/kv/src/store.rs`, add to the `Kv` trait (after `scan_prefix`):
```rust
    /// All (key, value) pairs with `start <= key < end`, in key order
    /// (inclusive start, exclusive end).
    #[allow(clippy::type_complexity)]
    fn scan_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, KvError>;
```
Implement for `MemKv` (mirroring `scan_prefix`):
```rust
    fn scan_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, KvError> {
        Ok(self
            .map
            .read()
            .expect("kv lock")
            .range(start.to_vec()..end.to_vec())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
```

- [ ] **Step 4: Implement `scan_range` for the fjall backend(s)**

In `crates/kv/src/fjall_store.rs`, add `scan_range` next to each `scan_prefix`. The real impl is in the keyspace wrapper (`KeyspaceKv`, `:55`): mirror its `scan_prefix` body but use fjall's bounded range query — `self.<keyspace>.range(start.to_vec()..end.to_vec())` (fjall 3.x `Keyspace::range<K, R: RangeBounds<K>>` returns the SAME `Iter` type as the prefix query, with `[start, end)` semantics) — matching the existing error handling and `(Vec<u8>, Vec<u8>)` collection exactly. The other `scan_prefix` (`FjallKv`, `:114`) just **delegates** to the inner `KeyspaceKv`; add a one-line `scan_range` delegate there too. (Read both at `:55`/`:114` and mirror their structure.)

- [ ] **Step 5: Add the watermark key + clog scan-end helpers (`crates/kv/src/key.rs`)**

After `meta_next_global_xid_key` (`:73`):
```rust
/// Key for a DATA range's recovery-scan watermark: the smallest local xid `Li`
/// at/after which the leadership-rise recovery scan must still look. Lives in the
/// `meta` namespace (disjoint from the `/0/clog/` prefix, so a clog scan never
/// returns it). Stored per-range in that range's own store. Value = `Li` big-endian.
pub fn clog_scan_lo_key() -> Vec<u8> {
    let mut k = system_prefix("meta");
    k.extend_from_slice(b"clog_scan_lo");
    k
}

/// Exclusive upper bound for a scan over the whole `/0/clog/` keyspace: the clog
/// prefix with its trailing byte incremented (the prefix's successor). `clog_prefix`
/// is `system_prefix("clog")`, i.e. `…clog` followed by the `/` separator, so the
/// last byte `0x2f` increments to `0x30` with no carry. This is strictly greater than
/// `clog_key(u64::MAX)`, so it covers every clog entry. (Relies on the prefix never
/// ending in `0xFF` — true for the `/`-separated system prefixes.)
pub fn clog_scan_end() -> Vec<u8> {
    let mut p = clog_prefix();
    let last = p.last_mut().expect("clog prefix is non-empty");
    *last += 1; // 0x2f ('/') -> 0x30; no carry
    p
}
```
Add a Task 1 unit assertion (in `key.rs` tests or the store tests): `assert!(clog_scan_end() > clog_key(u64::MAX));` so the bound is pinned above every clog key.

- [ ] **Step 6: Run + verify**

Run: `cargo nextest run -p kv` → all pass (incl. the new `scan_range` test); `cargo clippy -p kv --all-targets -- -D warnings`; `cargo fmt --all`. Confirm `scan_range(clog_key(lo), clog_scan_end())` would cover exactly the clog entries with xid >= lo (clog keys are `clog_prefix() ++ put_u64(xid)`, big-endian, so they sort by xid and all fall below `clog_scan_end()`).

- [ ] **Step 7: Commit**
```bash
git add crates/kv/src/store.rs crates/kv/src/fjall_store.rs crates/kv/src/key.rs
git commit -m "feat(sp20): Kv::scan_range + clog watermark key helpers

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `in_doubt_globals_from` + watermark read/write on `SqlEngine`

**Files:**
- Modify: `crates/executor/src/lib.rs` (`in_doubt_globals_from`, `clog_scan_lo`, `advance_clog_scan_lo`; `in_doubt_globals` delegates)
- Test: `crates/executor/src/lib.rs` `#[cfg(test)]`

- [ ] **Step 1: Write the failing unit tests**

Use a **two-store** engine that mirrors a real DATA range — `kv`/`sm_kv` holds ONLY this range's local-`Li` markers; `catalog_kv` is a SEPARATE store holding the global-`G` decisions. (A single-store `with_kv` engine where `kv == catalog_kv` would put the global-`G` clog keys, at `G ≥ GLOBAL_XID_BASE = 2⁶³`, into the SAME clog the scan walks, so the scan would visit them and `max_li` would leap to ~2⁶³ — the cost-bound assertion would pass for the wrong reason. Do NOT use a single-store engine here.) Build the two-store engine via `SqlEngine::replicated(catalog_kv, sm_kv, committer, linearizer)` (`lib.rs:122-144`), reusing the same `LocalCommitter`/linearizer that `with_kv` builds internally (`lib.rs:96-107`) but passing **distinct** `catalog_kv` and `sm_kv`:
```rust
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_doubt_globals_from_bounds_the_scan_and_advances_past_terminal() {
        use mvcc::clog::{put_op, XidStatus};
        use mvcc::xid::GLOBAL_XID_BASE;
        // Two stores: sm_kv = this data range's local clog; catalog_kv = range 0's
        // global-G clog. (Mirror with_kv's internal LocalCommitter + linearizer, but
        // with DISTINCT stores — confirm the exact committer/linearizer types at lib.rs:96-107.)
        let sm_kv: std::sync::Arc<dyn kv::Kv> = std::sync::Arc::new(kv::MemKv::new());
        let catalog_kv: std::sync::Arc<dyn kv::Kv> = std::sync::Arc::new(kv::MemKv::new());
        let committer = std::sync::Arc::new(crate::commit::LocalCommitter { kv: std::sync::Arc::clone(&sm_kv) });
        let linearizer = std::sync::Arc::new(crate::read_gate::NoopLinearizer); // the test linearizer with_kv uses
        let engine = SqlEngine::replicated(std::sync::Arc::clone(&catalog_kv), std::sync::Arc::clone(&sm_kv), committer, linearizer).expect("engine");

        let (g_term, g_doubt) = (GLOBAL_XID_BASE + 1, GLOBAL_XID_BASE + 2);
        // Local markers at Li = 10 (terminal G), 11 (in-doubt G), 12 (terminal G) — sm_kv ONLY.
        sm_kv.write_batch(&[put_op(10, XidStatus::Prepared(g_term))]).unwrap();
        sm_kv.write_batch(&[put_op(11, XidStatus::Prepared(g_doubt))]).unwrap();
        sm_kv.write_batch(&[put_op(12, XidStatus::Prepared(g_term))]).unwrap();
        // Global decisions — catalog_kv ONLY (the range-0 clog).
        catalog_kv.write_batch(&[put_op(g_term, XidStatus::Committed)]).unwrap();
        // from(0): only g_doubt is in-doubt; watermark stops at the in-doubt Li (11).
        let (gs, lo) = engine.in_doubt_globals_from(0).await.expect("scan");
        assert_eq!(gs, vec![g_doubt]);
        assert_eq!(lo, 11, "watermark = smallest in-doubt Li");
        // Decide g_doubt; now from(11) finds nothing in-doubt and advances to ONE PAST
        // the largest local Li (12) — proving the scan saw only local markers, not G keys.
        catalog_kv.write_batch(&[put_op(g_doubt, XidStatus::Aborted)]).unwrap();
        let (gs2, lo2) = engine.in_doubt_globals_from(11).await.expect("scan");
        assert!(gs2.is_empty());
        assert_eq!(lo2, 13, "all terminal -> watermark = one past the largest local Li (12)");
    }
```
(Confirm the exact `LocalCommitter` field and the test linearizer type against `with_kv`'s body at `lib.rs:96-107` — if `NoopLinearizer` is named differently, use whatever `with_kv` constructs. If the `replicated(...)` wiring is awkward to call directly, add a small `#[cfg(test)]` two-store helper in the executor test module; do NOT fall back to a single-store engine for this assertion — the exact `lo2 == 13` is what proves success criterion 3.)

- [ ] **Step 2: Run it — expect FAIL** (`in_doubt_globals_from` does not exist).

- [ ] **Step 3: Implement `in_doubt_globals_from` + delegate**

In `crates/executor/src/lib.rs`, replace `in_doubt_globals` (`:274-291`) with a watermark-aware core + a delegating wrapper:
```rust
    /// Scan THIS range's clog from `scan_lo` for in-doubt `Prepared(Li -> g)` markers.
    /// Returns `(in_doubt_gs, new_scan_lo)` where `new_scan_lo` is the smallest scanned
    /// `Li` whose `g` is NOT durably terminal (so it must keep being swept), or one past
    /// the largest scanned `Li` if every scanned marker is terminal (or `scan_lo` if the
    /// range is empty). `new_scan_lo` NEVER passes a non-terminal `g` — that is the
    /// recovery (zombie-commit) safety invariant. Markers are never deleted.
    pub async fn in_doubt_globals_from(&self, scan_lo: u64) -> Result<(Vec<u64>, u64), ExecError> {
        use std::collections::BTreeSet;
        let mut gs: BTreeSet<u64> = BTreeSet::new();
        let mut first_undecided: Option<u64> = None;
        let mut max_li: Option<u64> = None;
        for (k, v) in self
            .kv
            .scan_range(&kv::key::clog_key(scan_lo), &kv::key::clog_scan_end())?
        {
            let Some(li) = kv::key::clog_xid_of(&k) else {
                continue;
            };
            max_li = Some(li);
            if let mvcc::clog::XidStatus::Prepared(g) = mvcc::clog::decode(&v)? {
                let terminal = matches!(
                    mvcc::clog::get(self.catalog_kv.as_ref(), g)?,
                    mvcc::clog::XidStatus::Committed | mvcc::clog::XidStatus::Aborted
                );
                if !terminal {
                    gs.insert(g);
                    first_undecided.get_or_insert(li);
                }
            }
        }
        // Advance only past the contiguous terminal prefix: stop at the first undecided
        // Li, else one past the largest scanned Li, else leave scan_lo unchanged.
        let new_scan_lo = first_undecided
            // `max_li` is a local `Li < GLOBAL_XID_BASE` on a real data range, so this
            // never saturates; `saturating_add` is belt-and-suspenders.
            .or_else(|| max_li.map(|m| m.saturating_add(1)))
            .unwrap_or(scan_lo)
            .max(scan_lo); // monotone
        Ok((gs.into_iter().collect(), new_scan_lo))
    }

    /// Back-compat: the full-scan in-doubt set (callers that don't track a watermark).
    pub async fn in_doubt_globals(&self) -> Result<Vec<u64>, ExecError> {
        Ok(self.in_doubt_globals_from(0).await?.0)
    }
```
Add the durable watermark read/write (mirror `commit_global_decision`'s committer use, `:251`):
```rust
    /// Read this range's durable recovery-scan watermark (`0` if absent/unset).
    pub fn clog_scan_lo(&self) -> Result<u64, ExecError> {
        match self.kv.get(&kv::key::clog_scan_lo_key())? {
            Some(b) if b.len() == 8 => Ok(u64::from_be_bytes(b[..8].try_into().expect("8 bytes"))),
            _ => Ok(0),
        }
    }

    /// Durably advance this range's recovery-scan watermark (monotone; a no-op if `lo`
    /// is not greater than the current value). Proposed through the range committer.
    pub async fn advance_clog_scan_lo(&self, lo: u64) -> Result<(), ExecError> {
        if lo <= self.clog_scan_lo()? {
            return Ok(());
        }
        self.committer
            .commit(vec![kv::store::WriteOp::Put {
                key: kv::key::clog_scan_lo_key(),
                value: lo.to_be_bytes().to_vec(),
            }])
            .await
    }
```
(Confirm the `WriteOp` import path — it is `kv::store::WriteOp` per `kv/src/store.rs:11`. If the executor already imports `WriteOp`, use the existing path.)

- [ ] **Step 4: Run + regressions**

Run: `cargo nextest run -p executor -E 'test(in_doubt_globals)'` (the new test + the SP18 `in_doubt_globals_lists_undecided_prepared_markers` via the delegate) → PASS; `cargo nextest run -p executor` (no regressions); `cargo clippy -p executor --all-targets -- -D warnings`; `cargo fmt --all`.

- [ ] **Step 5: Commit**
```bash
git add crates/executor/src/lib.rs
git commit -m "feat(sp20): in_doubt_globals_from + durable per-range recovery-scan watermark

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Advance the watermark in the leadership-rise sweep

**Files:**
- Modify: `crates/cluster/src/server_node.rs` (`resolve_in_doubt_on_leadership`)
- Test: `crates/cluster/src/twopc.rs` (or `server_node.rs`) in-crate `#[cfg(test)] mod tests`

- [ ] **Step 1: Wire the watermark into the sweep**

In `resolve_in_doubt_on_leadership` (`crates/cluster/src/server_node.rs:600-626`), read the watermark, scan from it, abort-race, then advance the watermark durably. Replace the rising-edge body:
```rust
        if is_leader && !was_leader {
            // Start the recovery scan at this range's durable watermark, not the whole
            // clog — bounding the scan to markers at/after the oldest still-in-doubt Li.
            let scan_lo = engine.clog_scan_lo().unwrap_or(0);
            if let Ok((gs, new_lo)) = engine.in_doubt_globals_from(scan_lo).await {
                for g in gs {
                    // Best-effort: a failed abort-race (range 0 unreachable) leaves `g`
                    // non-terminal, so `new_lo` does not pass it and it is re-swept next
                    // rise. Log so a permanently-stuck range-0 is observable.
                    if let Err(e) = client.call(0, TxnRpc::CommitGlobal { g, commit: false }).await {
                        tracing::warn!(g, ?e, "recovery abort-race failed; g stays in-doubt, will re-scan next rise");
                    }
                }
                // Advance past the contiguous terminal prefix (monotone, durable). Safe:
                // `new_lo` never passes a marker whose g was non-terminal at scan time,
                // so every in-doubt g keeps being swept (zombie-commit protection). The
                // write is best-effort: a NotLeader rejection just leaves the old (lower)
                // durable value, which only enlarges the next scan — never an unsafe skip.
                if let Err(e) = engine.advance_clog_scan_lo(new_lo).await {
                    tracing::debug!(new_lo, ?e, "watermark advance not durable (e.g. NotLeader); next leader re-scans from the old watermark — safe");
                }
            }
        }
```
(Keep the surrounding `loop`/`rx.changed()` structure and the `use crate::transport::protocol::TxnRpc;` import unchanged.)

- [ ] **Step 2: Add the in-crate watermark test**

In `crates/cluster/src/twopc.rs`'s `#[cfg(test)] mod tests` (it has `testonly_two_range_node` + the SP18/SP19 fixtures), add a deterministic test that drives the watermark advance directly (no multi-process needed):
```rust
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_scan_watermark_advances_past_terminal_and_persists() {
        let (node, _sql) = crate::server_node::testonly_two_range_node().await;
        let eng = &node.engines[&1]; // a data range
        assert_eq!(eng.clog_scan_lo().expect("lo"), 0, "watermark starts at 0");
        // Drive in_doubt_globals_from over an empty clog: nothing in-doubt, watermark
        // stays 0 (no markers). Then stage a participant + decide it terminal, and
        // assert advance_clog_scan_lo persists and is monotone.
        eng.advance_clog_scan_lo(5).await.expect("advance");
        assert_eq!(eng.clog_scan_lo().expect("lo"), 5);
        eng.advance_clog_scan_lo(3).await.expect("no-op"); // lower -> no-op
        assert_eq!(eng.clog_scan_lo().expect("lo"), 5, "watermark is monotone");
    }
```
(Strengthen this if practical to stage a real `Prepared` marker via the SP18/SP19 fixtures, decide it, and assert the watermark advances past it on a sweep — but the monotonic-durable check above is the minimum. Reuse the exact fixture pattern from the SP19 `a_durable_prepared_marker_is_finalized_by_the_leadership_sweep` test.)

- [ ] **Step 3: Verify**

Run: `cargo build -p cluster --all-targets`; `cargo nextest run -p cluster` (no regressions — the SP18/SP19 recovery + crossrange + jepsen_bank suites must stay green; the watermark must not change any decision); `cargo clippy -p cluster --all-targets -- -D warnings`; `cargo fmt --all`.

- [ ] **Step 4: Commit**
```bash
git add crates/cluster/src/server_node.rs crates/cluster/src/twopc.rs
git commit -m "feat(sp20): leadership-rise sweep advances the recovery-scan watermark

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Regression e2e + gauntlet + traceability + finish

**Files:**
- Modify: `CLAUDE.md` (SP20 note)
- Modify: `docs/superpowers/specs/2026-06-14-crabgresql-sp20-d3c-gc-scan-recovery-watermark-design.md` (traceability table)

- [ ] **Step 1: Recovery correctness with the watermark (reuse the e2e)**

Confirm the watermark is active and harmless end-to-end: `cargo nextest run -p crabgresql --test crossrange_2pc_replicated` and `--test crossrange_2pc_nemesis` (2× each) — a coordinator-crash in-doubt `g` is still finalized on the new leader's rise and the bank total is conserved across the nemesis + restart. (These already exercise `resolve_in_doubt_on_leadership`; the watermark is now in that path. No test change needed unless a stronger marker-count assertion is wanted — if so, it belongs at the executor unit level, NOT a new multi-process binary.) No new test binary is added by this slice.

- [ ] **Step 2: UAC guard** — `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` (expect empty). SP20 adds no new test target.

- [ ] **Step 3: Add the SP20 line to CLAUDE.md** (after the SP19 line):
```markdown
**SP20 (2026-06-14):** no new test binary — the recovery-scan watermark is proven by `kv`/`executor`/`cluster` unit tests + the existing `crossrange_2pc_{replicated,nemesis}` e2e. No new dependency. The full guard `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` returns empty.
```

- [ ] **Step 4: Fill the Traceability section** in the SP20 spec, mapping each success criterion (1–7) → task → proving test (1 → T1 `scan_range_returns_inclusive_start_exclusive_end_in_order`; 2/3/4 → T2 `in_doubt_globals_from_bounds_the_scan_and_advances_past_terminal` + T3 `recovery_scan_watermark_advances_past_terminal_and_persists`; 5/6 → the SP16–19 cross-range suites + `crossrange_2pc_{replicated,nemesis}`; 7 → T4 gauntlet).

- [ ] **Step 5: Full gauntlet** (all green):
```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
cargo test --workspace --doc
cargo deny check
```
(If `cargo fmt --all --check` reports diffs, run `cargo fmt --all` and re-commit.)

- [ ] **Step 6: Commit**
```bash
git add CLAUDE.md docs/superpowers/specs/2026-06-14-crabgresql-sp20-d3c-gc-scan-recovery-watermark-design.md
git commit -m "docs(sp20): traceability table + CLAUDE.md note for the recovery-scan watermark

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 7: Finish the branch** — superpowers:finishing-a-development-branch, option 2 (push fresh non-force branch + PR against `main`). SP20 branches off `main` (SP19 already merged), so no rebase is needed unless `main` advanced. PR body ends with the Claude Code generated-with line.

---

## Notes for the implementer

- **Stale IDE diagnostics:** trust `cargo build`/`clippy`/`nextest`, never the editor.
- **The safety invariant is the whole slice:** `new_scan_lo` must NEVER pass a marker whose `g` was non-terminal at scan time. The recovery sweep's abort-race of in-doubt `g`s is what prevents a late zombie-commit; skipping an in-doubt marker would silently disable that. The unit test (T2) pins this; do not "optimize" the watermark past an undecided `g`.
- **No deletion, no visibility change:** markers and `clog[G]` are never removed; `global_status`/`eval_plan_qual` resolve every row exactly as before. The SP16–19 cross-range conservation + recovery suites are the regression guard — green at every task.
- **Data-range scope:** the watermark + `in_doubt_globals_from` run only where `resolve_in_doubt_on_leadership` is spawned (data ranges, `range != 0`). Range 0 holds the global `clog[G]` decisions at the high keyspace end and does not run the sweep — do not apply the watermark there.
- **Monotonicity + durability:** `advance_clog_scan_lo` is a no-op for a non-increasing value and writes through the range committer (replicated, survives restart/failover). A stale-low watermark only enlarges the scan, never skips unsafely.
- **Big-endian clog keys:** `clog_key` uses `keyenc::put_u64` (big-endian), so clog keys sort by xid and `scan_range(clog_key(lo), clog_scan_end())` yields markers with `Li >= lo` in order. Confirm `keyenc::put_u64` is big-endian before relying on the ordering.
