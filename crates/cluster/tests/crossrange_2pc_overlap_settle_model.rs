//! Exhaustive Stateright model of the **settle-COMPLETE-before-serve** safety invariant under
//! an OVERLAPPING (cascading) range-0 failover: a freshly-risen range-0 leader must not open its
//! write gate (`mark_served`) until its rise sweep has driven EVERY inherited in-doubt
//! `Prepared(Li -> g)` marker to a DURABLE TERMINAL decision (committed or aborted). If it opens
//! the gate while a marker is still in-doubt, a new gated write supersedes the wrong (older)
//! visible head — the in-doubt marker is invisible to it — and when that marker later commits,
//! BOTH versions go live: two live versions of one row (the MVCC at-most-one-live violation),
//! which is the cross-range bank's torn total.
//!
//! ## The real bug this guards (the SP22/SP23-deferred cascading-failover gap)
//!
//! Range 0 is overloaded: GTM + global-decision clog home + `acct_a` participant. When its leader
//! is killed mid-2PC, the newly-risen leader's rise sweep (`resolve_in_doubt_on_leadership` in
//! crates/cluster/src/server_node.rs) is supposed to settle every inherited in-doubt marker before
//! serving writes (SP22 "settle-before-serve"). But as shipped it opens the gate on
//! `settled.is_ok()` — i.e. once apply-wait + `reseed_gtm` succeed — REGARDLESS of whether each
//! inherited marker's abort-race actually landed. The abort-race (`client.call(0, CommitGlobal{g,
//! false})`) is best-effort + warn-only, and `CommitGlobal` is un-gated. Under an OVERLAPPING
//! failover (the risen leader itself loses leadership again, or a still-alive coordinator commits
//! the marker), an inherited marker can remain in-doubt when the gate opens. A new gated write then
//! lands, reads the in-doubt marker as invisible, and supersedes the older committed head instead.
//! When the inherited marker is finally decided COMMITTED, its version and the new write's version
//! are BOTH live (neither supersedes the other) → the duplicate. Empirically this is the bidirectional
//! `+money`/`-money` torn total reproduced by an overlapping range-0-leader-kill nemesis (the
//! `range0_leader_kill_drain` single-failover scoping deliberately drains to avoid exactly this).
//!
//! This is the MVCC at-most-one-live class (`scan_live`/`find_visible_one` `debug_assert!`), the
//! SAME consequence the SP22 `crossrange_2pc_settle_model` and SP23 `crossrange_2pc_gtm_reuse_model`
//! guard — but via a DIFFERENT hole those two do not cover: the SP22 model proves a *single*
//! gated-vs-ungated write past an inherited marker; it assumes the sweep, once it runs, fully
//! settles. This model removes that assumption and proves the load-bearing requirement that
//! `mark_served` is conditional on the sweep DRIVING every inherited in-doubt marker terminal —
//! the overlapping-failover dimension where the abort-race can fail to land.
//!
//! ## The abstract model
//!
//! One MVCC row (`acct_a` on range 0). It starts with a committed seed (`G_SEED`) superseded by an
//! INHERITED staged write under `g = BASE` (a `Prepared(L0 -> BASE)` marker) that is still IN-DOUBT
//! — the half a killed coordinator left behind. A leader has just risen (`current_term = 1`,
//! `served_term = 0`: gate CLOSED). The rise sweep is decomposed into the actions a real overlapping
//! failover can interleave:
//!
//! - `AbortRace`: the sweep drives an in-doubt marker's `g` to Aborted (write-once, presumed-abort).
//!   It can FAIL to land (modelling the risen leader losing leadership mid-sweep) — that is the
//!   `abort_lands` non-determinism the fix must tolerate.
//! - `CommitInherited`: a still-alive coordinator commits the inherited in-doubt `g` (write-once).
//!   `CommitGlobal` is un-gated, so this can fire at any time — including after the gate opens.
//! - `MarkServed`: open the gate for the current term. With the fix (`settle_complete = true`) it is
//!   admitted ONLY when NO marker is still in-doubt (every staged `g` is durably terminal) — genuine
//!   settle-before-serve. With the fix off it fires unconditionally (the as-shipped bug).
//! - `NewWrite`: a fresh gated write under a new monotone `g` (begin_global is reseed-safe, so `g`
//!   is never reused — that hole is the GTM-reuse model's). GATED: admitted only when the gate is
//!   open. It supersedes the highest live version below `g`; an in-doubt inherited marker is
//!   invisible, so it supersedes the older committed head instead — the duplicate seed.
//! - `Rise`: a new leadership term (the gate re-closes) — the overlapping failover that can strand
//!   a half-finished sweep.
//!
//! `settle_complete = true` is the fix; `false` is the as-shipped bug. The teeth test proves the
//! checker CATCHES the duplicate with the fix off; the positive test proves the invariant HOLDS with
//! it on. Mirrors `crossrange_2pc_gtm_reuse_model.rs` / `crossrange_2pc_settle_model.rs`.

use stateright::{Checker, Model, Property};

/// The committed base version the row starts with (the seed). Its creator `g` is always
/// decided-committed, so the seed is live until a committed write supersedes it.
const G_SEED: u64 = 0;

/// The global-xid floor a fresh allocator starts at (abstracts `GLOBAL_XID_BASE`). Kept small so
/// the bounded BFS stays tiny. The seed uses `G_SEED = 0`, disjoint from every staged `g >= BASE`.
const BASE: u64 = 1;

/// One MVCC row version. `tag` is the staging write's unique identity (two DISTINCT writes are kept
/// as two separate versions — the BFS must not dedup them, and a self-supersede must not mask the
/// duplicate). `creator_g` is the global xid the staging write was assigned; `xmax_tag` is the `tag`
/// of the write that superseded (deleted) this version, if any.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Version {
    tag: u64,
    creator_g: u64,
    xmax_tag: Option<u64>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    /// The row's versions, kept sorted so logically-equal states fingerprint equally.
    versions: Vec<Version>,
    /// Write-once global clog: `(g, committed)`, kept sorted by `g`. A `g` appears at most once
    /// (the first decision wins — `commit_global_decision`'s write-once semantics).
    decided: Vec<(u64, bool)>,
    /// Next global xid a `NewWrite` allocates. Monotone (reseed-safe — never reused), so a new
    /// write's `g` is always strictly above every staged `g`.
    next_g: u64,
    /// Next staging-write tag (monotone, identifies each staged version).
    next_tag: u64,
    /// The term whose rise-sweep has settled + opened the gate.
    served_term: u64,
    /// The current leadership term. The gate is open iff `served_term == current_term`.
    current_term: u64,
    /// Step counter, bounds the exhaustive search.
    steps: usize,
}

impl State {
    fn committed(&self, g: u64) -> bool {
        self.decided.iter().any(|&(gg, c)| gg == g && c)
    }

    fn decided_at_all(&self, g: u64) -> bool {
        g == G_SEED || self.decided.iter().any(|&(gg, _)| gg == g)
    }

    fn creator_g_of_tag(&self, tag: u64) -> Option<u64> {
        self.versions
            .iter()
            .find(|v| v.tag == tag)
            .map(|v| v.creator_g)
    }

    /// A version is live iff its creating write's `g` committed AND it is not superseded by a
    /// committed deleter. Two live versions of one row is the at-most-one-live violation.
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

    /// The live version a fresh write under global xid `g` re-reads + supersedes. Supersession is
    /// xid-monotone: a version is superseded only by a write with a STRICTLY HIGHER xid. So a write
    /// supersedes the highest-`tag` live version whose `creator_g < g`. An in-doubt inherited marker
    /// is NOT live (its `g` is undecided), so a write that lands while it is in-doubt skips it and
    /// supersedes the older committed head — leaving the marker un-superseded. When the marker later
    /// commits, both it and the new write are live: the duplicate.
    fn visible_tag_below_g(&self, g: u64) -> Option<u64> {
        self.versions
            .iter()
            .filter(|v| self.live(v) && v.creator_g < g)
            .map(|v| v.tag)
            .max()
    }

    fn gate_open(&self) -> bool {
        self.served_term == self.current_term
    }

    /// Every staged (non-seed) `g` that is not yet durably terminal — the in-doubt markers the rise
    /// sweep must drive terminal before serving. (A `g` is "staged" iff a version was created under
    /// it.)
    fn in_doubt_markers(&self) -> Vec<u64> {
        let mut gs: Vec<u64> = self
            .versions
            .iter()
            .map(|v| v.creator_g)
            .filter(|&g| g != G_SEED && !self.decided_at_all(g))
            .collect();
        gs.sort_unstable();
        gs.dedup();
        gs
    }

    fn canonicalize(&mut self) {
        self.versions.sort();
        self.decided.sort();
    }
}

/// `settle_complete` toggles the fix: `true` = `MarkServed` is admitted only once the rise sweep has
/// driven every inherited in-doubt marker to a durable terminal decision (re-scan empty) — genuine
/// settle-before-serve; `false` = the as-shipped bug where the gate opens on apply-wait/reseed
/// success regardless, so a still-in-doubt inherited marker can be superseded-around by a new write.
struct OverlapSettleModel {
    max_steps: usize,
    settle_complete: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    /// The rise sweep's abort-race for an in-doubt marker `g` (write-once Aborted, presumed-abort).
    /// `lands == false` models the abort-race FAILING to take effect (the risen leader lost
    /// leadership mid-sweep) — a no-op that leaves `g` in-doubt.
    AbortRace { g: u64, lands: bool },
    /// A still-alive coordinator commits an in-doubt marker `g` (write-once). `CommitGlobal` is
    /// un-gated, so this can fire at any time, including after the gate opens.
    CommitInherited { g: u64 },
    /// Open the gate for the current term. The fix gates this on the in-doubt set being empty.
    MarkServed,
    /// A fresh gated write under a new monotone `g`. Admitted only when the gate is open.
    NewWrite,
    /// A new leadership term (overlapping failover): the gate re-closes.
    Rise,
}

impl Model for OverlapSettleModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            // A committed seed (tag 0, g = G_SEED) superseded by an INHERITED staged write under
            // g = BASE (tag 1, a `Prepared(L0 -> BASE)` marker) that is still in-doubt — the half a
            // killed coordinator left behind. A leader has just risen (term 1) but has not settled
            // (served_term 0): the gate is CLOSED.
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
            decided: vec![(G_SEED, true)],
            // A fresh write allocates strictly above the inherited BASE marker (monotone/reseed-safe).
            next_g: BASE + 1,
            next_tag: 2,
            served_term: 0,
            current_term: 1,
            steps: 0,
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        if s.steps >= self.max_steps {
            return;
        }
        for g in s.in_doubt_markers() {
            out.push(Action::AbortRace { g, lands: true });
            out.push(Action::AbortRace { g, lands: false });
            out.push(Action::CommitInherited { g });
        }
        out.push(Action::MarkServed);
        out.push(Action::NewWrite);
        out.push(Action::Rise);
    }

    fn next_state(&self, s: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut n = s.clone();
        n.steps += 1;
        match a {
            Action::AbortRace { g, lands } => {
                // Write-once: a no-op if `g` is already decided. `lands == false` models the
                // abort-race failing to take effect (leadership lost mid-sweep) — no new state.
                if !lands || n.decided_at_all(g) {
                    return None;
                }
                n.decided.push((g, false));
            }
            Action::CommitInherited { g } => {
                // A still-alive coordinator's write-once COMMIT of an in-doubt marker. No-op if `g`
                // is already decided (the abort-race or a prior commit won).
                if n.decided_at_all(g) {
                    return None;
                }
                n.decided.push((g, true));
            }
            Action::MarkServed => {
                // Already serving this term, or — with the fix — an inherited marker is still
                // in-doubt: no state change / refuse to open the gate.
                if n.gate_open() {
                    return None;
                }
                if self.settle_complete && !n.in_doubt_markers().is_empty() {
                    return None;
                }
                n.served_term = n.current_term;
            }
            Action::NewWrite => {
                // GATED: a version-creating write is refused until the gate is open. With the fix
                // this is exactly "after the sweep settled every inherited marker"; without it the
                // gate may be open while a marker is in-doubt.
                if !n.gate_open() {
                    return None;
                }
                let g = n.next_g;
                n.next_g += 1;
                let tag = n.next_tag;
                n.next_tag += 1;
                // The new write is immediately decided-committed (the coordinator drives it to
                // commit); its supersession only takes effect because its `g` commits. This keeps
                // the model focused on the inherited-marker hole, not the new write's own 2PC.
                n.decided.push((g, true));
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
            Action::Rise => {
                // A new leadership term: the gate re-closes (served_term now lags). Models the
                // overlapping failover that strands a half-finished sweep.
                n.current_term += 1;
            }
        }
        n.canonicalize();
        Some(n)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety (the MVCC `scan_live`/`find_visible_one` debug_assert): a row has AT MOST ONE
            // live version under any snapshot. Two live versions — one inherited marker the gate
            // opened past, one new write that superseded around it — is the cross-range torn total.
            Property::<Self>::always("at most one live version per row", |_, s| {
                s.live_count() <= 1
            }),
            // Corroborating: the gate is never open while an inherited marker is still in-doubt.
            // This is the literal settle-before-serve contract; the duplicate above is its
            // consequence. Stated as an invariant so a counterexample names the protocol gap
            // directly. (Holds vacuously when the fix is on; the teeth test below exercises the
            // off variant against the at-most-one-live property, the load-bearing one.)
            Property::<Self>::always(
                "gate never open with an in-doubt inherited marker",
                |_, s| !s.gate_open() || s.in_doubt_markers().is_empty(),
            ),
        ]
    }
}

/// The fixed system: `MarkServed` requires the rise sweep to have driven every inherited in-doubt
/// marker terminal (`settle_complete = true`). Exhaustively explore every interleaving of
/// abort-race (landing or not) / commit / mark-served / new-write / rise and assert NO property is
/// ever violated. A counterexample here would mean settle-COMPLETE-before-serve is itself unsound —
/// a genuine design finding; do not weaken the properties.
#[test]
fn settle_complete_before_serve_upholds_at_most_one_live() {
    let checker = OverlapSettleModel {
        max_steps: 8,
        settle_complete: true,
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

/// Teeth: the SAME model with the settle-complete gate REMOVED (`settle_complete = false`, the
/// as-shipped bug — the rise sweep opens the gate on apply-wait/reseed success regardless of whether
/// every inherited marker landed terminal). An overlapping interleaving — abort-race fails to land,
/// `MarkServed` opens the gate with the inherited marker still in-doubt, a `NewWrite` supersedes the
/// older head around it, then `CommitInherited` decides the marker committed — produces two live
/// versions. Asserts the checker actually CATCHES the duplicate (`discoveries()` non-empty and names
/// the load-bearing property), proving the passing test above is meaningful, not vacuous.
#[test]
fn opening_the_gate_before_settling_double_lives_is_caught() {
    let checker = OverlapSettleModel {
        max_steps: 8,
        settle_complete: false,
    }
    .checker()
    .spawn_bfs()
    .join();

    let discoveries = checker.discoveries();
    assert!(
        !discoveries.is_empty(),
        "removing the settle-complete gate must produce a property counterexample; if empty the \
         model has no teeth and the passing test is meaningless"
    );
    assert!(
        discoveries.contains_key("at most one live version per row"),
        "expected an 'at most one live version per row' counterexample from the no-settle variant \
         (a write that supersedes around a still-in-doubt inherited marker, which then commits), \
         got: {:?}",
        discoveries.keys().collect::<Vec<_>>()
    );
}
