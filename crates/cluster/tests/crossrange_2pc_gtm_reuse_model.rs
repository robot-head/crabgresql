//! Exhaustive Stateright model of the SP23 **reseed-before-allocate** safety invariant:
//! a newly-risen range-0 leader must lift its in-memory GTM counter past every durably
//! allocated global xid BEFORE it serves `BeginGlobal`, so it can never re-hand-out a
//! global xid a prior leader already allocated. No global-xid reuse ⇒ no duplicate MVCC
//! version.
//!
//! ## The real bug this guards
//!
//! Range 0's `Gtm` (crates/executor/src/gtm.rs) hands out monotonic global xids from an
//! in-memory `next_global` counter; the DURABLE `meta_next_global_xid_key` is advanced by
//! `begin_global_durable` (quorum-committed BEFORE `g` is handed out) and max-merged on
//! apply, so it never regresses. When a range-0 leader is killed and a new one rises, the
//! riser's in-memory `next_global` is rebuilt by `Gtm::open` from the *applied* store, which
//! LAGS the committed counter advance on a freshly-risen leader. So `next_global` regresses
//! BELOW an already-allocated `g`. If that riser serves `BeginGlobal` BEFORE reseeding, it
//! re-hands-out an already-allocated `g`.
//!
//! A reused `g` is then assigned to a fresh cross-range write — a DIFFERENT logical write
//! from the one the prior leader had already committed under that same `g`. The participant's
//! SP21 `staged_local_for(g)` idempotency check matches the prior txn's `Prepared(-> g)`
//! marker; combined with the write-once global clog (the prior txn already decided `g` =
//! COMMIT), the fresh write's version inherits that commit and BOTH versions keyed by `g` go
//! live. Two live versions of the row violate the MVCC at-most-one-live invariant
//! (`scan_live`/`find_visible_one` `debug_assert!`) and balances tear — the design spec's
//! `+money` conservation tear / 2-live wedge, reproduced deterministically across 42 probe
//! runs. This is the SP7 "xid reuse across failover" class, now at the GTM global counter.
//!
//! The `gtm.rs` unit test `stale_in_memory_counter_reuses_g_until_reseed` pins the reuse at
//! the allocator level; this model proves the END-TO-END consequence (two live versions) and
//! that the reseed-before-allocate gate prevents it.
//!
//! The SP23 fix is the range-0 rise sweep ordering (crates/cluster/src/server_node.rs,
//! `resolve_in_doubt_on_leadership`): apply-wait → `reseed_gtm()` (lift `next_global` to the
//! now-applied durable counter) → settle → `mark_served` (open the gate); plus the
//! `BeginGlobal` gate in `twopc.rs`/`transport::server` that refuses to allocate until range
//! 0's gate is open. So the riser always reseeds before its first allocation and never reuses
//! a global xid.
//!
//! ## The abstract model
//!
//! A durable global-xid counter (`durable_next`, max-merged, monotone), the new leader's
//! in-memory counter (`mem_next`, which a `Rise` regresses to model the apply-lag /
//! stale-`Gtm::open`), the set of `allocated` global xids, ONE row's MVCC versions (each a
//! distinct staged write tagged with its creating `g`), the write-once global clog, and a
//! per-term serving gate (`served_term`/`current_term`). `Alloc` (begin_global) is GATED with
//! the fix on (admitted only when `served_term == current_term`); it hands out `g = mem_next`,
//! advances both counters, records `g`, and STAGES a fresh write that re-reads the visible
//! head and supersedes it. A reused `g` (already in `allocated`) inherits the prior holder's
//! committed clog decision, so its fresh version goes live alongside the prior one — TWO live
//! versions sharing the reused `g`.
//!
//! The init seeds the *prior leader's* committed write under `g = BASE` as the row's live head
//! (so a later reuse of `BASE` produces a genuine duplicate). `mem_next` starts at `BASE`
//! (lagging `durable_next = BASE+1`): an un-gated `Alloc` re-hands-out `BASE`. `Reseed` (the
//! rise sweep) lifts `mem_next` to `durable_next` THEN opens the gate — the ONLY gate-opener,
//! mirroring reseed-then-mark_served. `Rise` opens a new term and regresses `mem_next`.
//!
//! `reseed_before_alloc = true` is the fix; `false` is the pre-fix bug. The teeth test proves
//! the checker CATCHES the duplicate with the gate off; the positive test proves the invariant
//! HOLDS with it on. Mirrors the SP22 settle model in `crossrange_2pc_settle_model.rs` and the
//! SP21 Stage-idempotency model in `crossrange_2pc_model.rs` (positive + teeth tests).

use stateright::{Checker, Model, Property};

/// The committed base version the row starts with (the seed). Its creator `g` is always
/// decided-committed, so the seed is live until a committed write supersedes it.
const G_SEED: u64 = 0;

/// The global-xid floor a fresh allocator starts at (abstracts `GLOBAL_XID_BASE`). Kept
/// small (1) so the bounded BFS stays tiny while still exercising allocate / regress /
/// re-allocate. The seed uses `G_SEED = 0`, disjoint from every allocated `g >= BASE`.
const BASE: u64 = 1;

/// One MVCC row version. `tag` is the staging write's unique identity (so two DISTINCT writes
/// that were assigned the SAME reused `creator_g` are kept as two separate versions — the BFS
/// must not dedup them, and a self-supersede must not mask the duplicate). `creator_g` is the
/// global xid the staging write was assigned; `xmax_tag` is the `tag` of the write that
/// superseded (deleted) this version, if any.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Version {
    tag: u64,
    creator_g: u64,
    xmax_tag: Option<u64>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    /// The row's versions, kept sorted so logically-equal states fingerprint equally
    /// (the BFS dedups on `Hash`).
    versions: Vec<Version>,
    /// Write-once global clog: `(g, committed)`, kept sorted by `g`. A `g` appears at most
    /// once (the first decision wins — `commit_global`'s write-once semantics). A REUSED `g`
    /// therefore shares ONE clog decision across both writes assigned it.
    decided: Vec<(u64, bool)>,
    /// The durable `meta_next_global_xid_key` counter: max-merged by the state machine, so it
    /// only ever rises. Every committed `begin_global_durable` advance lands here.
    durable_next: u64,
    /// The current (risen) leader's in-memory `Gtm::next_global`. A `Rise` regresses it to
    /// `BASE` (the apply-lag / stale-`Gtm::open` window); `Reseed` lifts it back. `Alloc`
    /// hands out exactly this value, then bumps both counters.
    mem_next: u64,
    /// Every global xid handed out by some allocator, kept sorted. A re-handed-out `g`
    /// (already in this set) is the reuse the fix prevents.
    allocated: Vec<u64>,
    /// Next staging-write tag to hand out (monotone, identifies each staged version).
    next_tag: u64,
    /// The term whose rise-sweep has reseeded + completed (the serving gate is open for it).
    served_term: u64,
    /// The current leadership term. The gate is open iff `served_term == current_term`.
    current_term: u64,
    /// Step counter, bounds the exhaustive search so the state space is finite.
    steps: usize,
}

impl State {
    fn committed(&self, g: u64) -> bool {
        self.decided.iter().any(|&(gg, c)| gg == g && c)
    }

    /// The `creator_g` of the version with this `tag` (used to resolve a deleter's commit
    /// status — a supersession only takes effect if the deleting write's `g` committed).
    fn creator_g_of_tag(&self, tag: u64) -> Option<u64> {
        self.versions
            .iter()
            .find(|v| v.tag == tag)
            .map(|v| v.creator_g)
    }

    /// A version is live iff its creating write's `g` committed AND it is not superseded by a
    /// committed deleter (the deleting write's `g` committed). Two versions whose creating
    /// writes were assigned the SAME committed `g` (a reused global xid) therefore both go
    /// live — exactly the GTM-reuse duplicate.
    fn live(&self, v: &Version) -> bool {
        if !self.committed(v.creator_g) {
            return false;
        }
        match v.xmax_tag {
            None => true,
            Some(x) => match self.creator_g_of_tag(x) {
                Some(xg) => !self.committed(xg),
                None => true,
            },
        }
    }

    fn live_count(&self) -> usize {
        self.versions.iter().filter(|v| self.live(v)).count()
    }

    /// The live version a fresh write under global xid `g` re-reads + supersedes. MVCC chains
    /// are xid-monotone: a version is only superseded by a write with a STRICTLY HIGHER xid
    /// (`deleter_g > creator_g`). So a write supersedes the highest-`tag` live version whose
    /// `creator_g < g`.
    ///
    /// This is the load-bearing mechanic. For a FRESH (correctly monotone) `g`, the current
    /// head's `creator_g < g`, so the write supersedes the head and extends the chain — exactly
    /// one live version. For a REUSED `g` (a stale-counter re-allocation, `g ==` the prior
    /// holder's `creator_g`), the prior holder is NOT strictly below `g`, so this write CANNOT
    /// supersede it — it leaves the prior `g` version un-superseded and supersedes only an older
    /// version (the seed). Both the prior holder's version and this fresh version are then live
    /// under the one committed reused `g` — the duplicate. (This mirrors the SP21 orphan: the
    /// non-monotone write fails to supersede the head it should have.)
    fn visible_tag_below_g(&self, g: u64) -> Option<u64> {
        self.versions
            .iter()
            .filter(|v| self.live(v) && v.creator_g < g)
            .map(|v| v.tag)
            .max()
    }

    /// The gate is open iff this term's rise-sweep has reseeded + completed.
    fn gate_open(&self) -> bool {
        self.served_term == self.current_term
    }

    /// Is there a staged write whose global decision is still pending? The coordinator drives
    /// ONE logical cross-range write on the row at a time: it decides the in-flight `g` before
    /// beginning the next. This isolates the GTM-reuse concern from the *concurrent* stale-base
    /// stage interleaving that the SP21/SP22 models (`crossrange_2pc_model.rs` /
    /// `crossrange_2pc_settle_model.rs`) already own — two DISTINCT, correctly-non-reused xids
    /// both reading the seed because the first is still in-doubt is THAT bug, not this one.
    fn has_pending_stage(&self) -> bool {
        self.versions.iter().any(|v| {
            v.creator_g != G_SEED && !self.decided.iter().any(|&(gg, _)| gg == v.creator_g)
        })
    }

    fn canonicalize(&mut self) {
        self.versions.sort();
        self.decided.sort();
        self.allocated.sort_unstable();
    }
}

/// `reseed_before_alloc` toggles the SP23 fix: `true` = an `Alloc` (begin_global) is admitted
/// only once the term's rise-sweep has reseeded the GTM counter past every durable allocation
/// and opened the gate (`served_term == current_term`); `false` = the pre-fix bug where a
/// risen leader serves `BeginGlobal` immediately, before reseeding, off a counter that may lag
/// the durable value.
struct GtmReuseModel {
    max_steps: usize,
    reseed_before_alloc: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    /// begin_global + stage a fresh cross-range write: hand out `g = mem_next`, advance both
    /// counters, record `g`, and stage a version (re-reading + superseding the visible head).
    /// GATED: with the fix on it is admitted only when `served_term == current_term`. After a
    /// `Rise` regressed `mem_next` (gate off), this re-hands-out the already-allocated `BASE`
    /// — a fresh version whose `creator_g == BASE` shares the prior write's committed clog
    /// decision, so both go live.
    Alloc,
    /// Decide the staged write's global xid `g` (write-once); commit makes its version live.
    /// Targets the most-recent staged-but-undecided `g`.
    Decide(bool),
    /// The rise sweep: lift `mem_next` to `durable_next` (reseed_from_applied) THEN open the
    /// gate for the current term (mark_served). This is the ONLY path that opens the gate, and
    /// it reseeds BEFORE opening — exactly the server_node ordering.
    Reseed,
    /// A new leadership term: bump `current_term` (gate re-closes) AND regress `mem_next` to
    /// `BASE`, modelling the riser's stale in-memory counter / apply-lag.
    Rise,
}

impl Model for GtmReuseModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            // The prior range-0 leader allocated global xid BASE, staged a committed write
            // (tag 1) on the row under it (superseding the seed, tag 0), then died. So the
            // row's live head is the BASE write and the seed is dead. For a REUSED `g` to
            // manifest as a duplicate, the original holder of that `g` must already have a
            // committed version on the row — this is it.
            versions: vec![
                Version {
                    tag: 0,
                    creator_g: G_SEED,
                    xmax_tag: Some(1),
                },
                Version {
                    tag: 1,
                    creator_g: BASE,
                    xmax_tag: None,
                },
            ],
            decided: vec![(G_SEED, true), (BASE, true)],
            // The prior leader durably committed `next_global = BASE+1` (it allocated BASE),
            // then died. The riser's in-memory counter is rebuilt at BASE and has NOT yet
            // reseeded: `mem_next` lags `durable_next` by one allocation — the stale-counter
            // window the fix protects. An un-reseeded Alloc here re-hands-out BASE.
            durable_next: BASE + 1,
            mem_next: BASE,
            // BASE was already handed out (by the prior leader) — so a riser that allocates
            // `mem_next == BASE` re-uses it.
            allocated: vec![BASE],
            next_tag: 2,
            // A leader has just risen (term 1) but has NOT yet reseeded/swept (served_term 0):
            // the gate starts CLOSED, modelling the rise-before-reseed window.
            served_term: 0,
            current_term: 1,
            steps: 0,
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        if s.steps >= self.max_steps {
            return;
        }
        out.push(Action::Alloc);
        out.push(Action::Decide(true));
        out.push(Action::Decide(false));
        out.push(Action::Reseed);
        out.push(Action::Rise);
    }

    fn next_state(&self, s: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut n = s.clone();
        n.steps += 1;
        match a {
            Action::Alloc => {
                // The serving gate: with the fix on, refuse to allocate until this term's
                // rise-sweep has reseeded + opened the gate. A blocked Alloc adds no new state
                // — the allocator never hands out off a stale counter.
                if self.reseed_before_alloc && !n.gate_open() {
                    return None;
                }
                // One logical cross-range write in flight on the row at a time — the
                // coordinator decides the staged `g` before allocating the next. Keeps the
                // model focused on the GTM-reuse invariant and out of the separate concurrent
                // stale-base territory the SP21/SP22 models own.
                if n.has_pending_stage() {
                    return None;
                }
                let g = n.mem_next;
                // Advance the in-memory counter and max-merge the durable counter (the
                // begin_global_durable Raft-commit). durable_next only ever rises.
                n.mem_next = g + 1;
                n.durable_next = n.durable_next.max(n.mem_next);
                // Record the allocation. With the gate off after a Rise, `g == BASE` is
                // ALREADY in `allocated` — that is the reuse.
                if !n.allocated.contains(&g) {
                    n.allocated.push(g);
                }
                // Stage a fresh write under `g`, superseding the live version below `g`
                // (xid-monotone supersession). For a FRESH `g` this is the head, so the chain
                // extends correctly (one live). For a REUSED `g` the prior holder (`creator_g
                // == g`) is NOT below `g`, so it is left un-superseded and only the seed is
                // superseded — the prior holder's version and this fresh version BOTH stay live
                // under the shared committed `g`: the duplicate.
                let tag = n.next_tag;
                n.next_tag += 1;
                if let Some(vtag) = n.visible_tag_below_g(g)
                    && let Some(v) = n.versions.iter_mut().find(|v| v.tag == vtag)
                {
                    v.xmax_tag = Some(tag);
                }
                n.versions.push(Version {
                    tag,
                    creator_g: g,
                    xmax_tag: None,
                });
            }
            Action::Decide(commit) => {
                // Decide the most-recent staged-but-undecided write's `g` (write-once). A
                // reused `g` is already decided (the prior holder decided it), so this only
                // fires for a genuinely-fresh `g`.
                let pending = n
                    .versions
                    .iter()
                    .filter(|v| {
                        v.creator_g != G_SEED && !n.decided.iter().any(|&(gg, _)| gg == v.creator_g)
                    })
                    .map(|v| v.creator_g)
                    .max();
                match pending {
                    Some(g) => n.decided.push((g, commit)),
                    None => return None, // nothing to decide — no new state
                }
            }
            Action::Reseed => {
                // The rise sweep: lift mem_next to the durable counter (reseed_from_applied),
                // THEN open the gate (mark_served). Idempotent — a Reseed that changes nothing
                // adds no state.
                let lifted = n.mem_next.max(n.durable_next);
                if lifted == n.mem_next && n.gate_open() {
                    return None;
                }
                n.mem_next = lifted;
                n.served_term = n.current_term;
            }
            Action::Rise => {
                // A new leadership term: the gate re-closes (served_term now lags) AND the
                // riser's in-memory counter regresses to BASE — the stale-`Gtm::open` window.
                // The durable counter persists (it only rises), so after the regress
                // mem_next < durable_next and an un-reseeded Alloc reuses BASE.
                n.current_term += 1;
                n.mem_next = BASE;
            }
        }
        n.canonicalize();
        Some(n)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety (the MVCC invariant `scan_live`/`find_visible_one` debug_assert): a row
            // has AT MOST ONE live version under any snapshot. Two live versions whose creating
            // writes share one reused global xid `g` is exactly the GTM-reuse violation the
            // reseed-before-allocate gate prevents. THIS is the load-bearing, teeth-bearing
            // invariant.
            Property::<Self>::always("at most one live version per row", |_, s| {
                s.live_count() <= 1
            }),
            // Corroborating, mechanism-side: no two LIVE versions share a creator `g`. A reused
            // global xid stages two versions under one `g`; once `g` is committed (the prior
            // holder committed it) both are live and share that creator — the literal
            // global-xid reuse. (Stated over live versions so an aborted/in-doubt duplicate is
            // not flagged.)
            Property::<Self>::always("no two live versions share a creator g", |_, s| {
                let mut creators: Vec<u64> = s
                    .versions
                    .iter()
                    .filter(|v| s.live(v))
                    .map(|v| v.creator_g)
                    .collect();
                creators.sort_unstable();
                creators.windows(2).all(|w| w[0] != w[1])
            }),
        ]
    }
}

/// The fixed system: `Alloc` (begin_global) is gated on the term's rise-sweep having reseeded
/// the GTM counter past every durable allocation and opened the gate (`reseed_before_alloc =
/// true`). Exhaustively explore every interleaving of alloc / decide / reseed / rise and
/// assert NO property is ever violated. A counterexample here would mean reseed-before-
/// allocate is itself unsound — a genuine design finding; do not weaken the properties.
#[test]
fn reseed_before_allocate_upholds_at_most_one_live() {
    let checker = GtmReuseModel {
        max_steps: 7,
        reseed_before_alloc: true,
    }
    .checker()
    .spawn_bfs()
    .join();

    checker.assert_properties();
    assert!(
        checker.unique_state_count() > 1,
        "model checking must have explored a non-trivial state space"
    );
}

/// Teeth: the SAME model with the allocation gate REMOVED (the pre-fix bug — a risen range-0
/// leader serves `BeginGlobal` immediately, before reseeding, off an in-memory counter that
/// lags the durable value). An `Alloc` re-hands-out the already-allocated `BASE`; the fresh
/// write's version inherits BASE's committed clog decision and both versions keyed by `BASE`
/// go live. This asserts the checker actually CATCHES the duplicate — `discoveries()` is
/// non-empty and names the load-bearing `"at most one live version per row"` property —
/// proving the passing test above is meaningful and not vacuously accepting everything.
#[test]
fn no_reseed_reuses_global_xid_and_double_lives_is_caught() {
    let checker = GtmReuseModel {
        max_steps: 7,
        reseed_before_alloc: false,
    }
    .checker()
    .spawn_bfs()
    .join();

    let discoveries = checker.discoveries();
    assert!(
        !discoveries.is_empty(),
        "removing the allocation gate must produce a property counterexample; if empty the \
         model has no teeth and the passing test is meaningless"
    );
    assert!(
        discoveries.contains_key("at most one live version per row"),
        "expected an 'at most one live version per row' counterexample from the no-reseed \
         variant (a reused global xid stages two versions that both go live), got: {:?}",
        discoveries.keys().collect::<Vec<_>>()
    );
}
