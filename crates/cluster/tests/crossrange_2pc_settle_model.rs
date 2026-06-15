//! Exhaustive Stateright model of the cross-range 2PC **settle-before-serve** safety
//! invariant (SP22 / D3c).
//!
//! ## The real bug this guards
//!
//! When a participant range's leader is killed mid-2PC, the new leader inherits the dead
//! leader's durable in-doubt markers — `Prepared(Li -> g_old)` versions with **no held
//! session** (the lock map is in-memory and died with the old leader). The dangerous case
//! is an inherited marker whose global decision is already **COMMIT**: the coordinator
//! durably committed `g_old` before the participant leader died, so `g_old`'s version is
//! *destined to go live* the moment the new leader resolves the marker against the clog.
//!
//! If the new leader **serves writes before it has swept those inherited in-doubt markers**,
//! a fresh cross-range write `Stage(g_new)` re-reads the visible row, sees the *stale base*
//! (the inherited `g_old` version is not yet resolved/live), and supersedes THAT base instead
//! of `g_old`'s version. The inherited marker then resolves to committed — `g_old`'s version
//! goes live, un-superseded — and `g_new`'s version commits live too. Both are live: the
//! MVCC at-most-one-live invariant (`scan_live`/`find_visible_one` `debug_assert!`) is
//! violated and balances tear, exactly as in SP18/SP21.
//!
//! The SP22 fix is a per-term **serving gate** (`RecoveryGate`): on a leadership rise the
//! range runs an in-doubt sweep that resolves every inherited marker, and only when that
//! sweep completes does the gate open for the new term. The two write-path checks refuse to
//! admit a `Stage` until the gate is open — so the writer can never read an unsettled marker
//! and the new write always supersedes `g_old`'s *resolved* version, keeping exactly one live.
//!
//! ## The abstract model
//!
//! ONE row's MVCC versions + a session-less `inherited_marker: Option<InDoubt>` (seeded
//! `decision: true` = destined-commit, the dangerous case) + a per-term serving gate
//! (`served_term` / `current_term`). A version is *live* iff its creating `g` is
//! decided-commit, it is not superseded by a committed deleter, AND — for the inherited
//! version — the marker has been resolved/applied. The decisive interleaving the checker
//! explores: with the gate OFF a `Stage(g_new)` runs while the inherited marker is unsettled,
//! supersedes the *stale base* (the seed) rather than `g_old`'s yet-unresolved version, and
//! when the marker later resolves to committed BOTH `g_old`'s and `g_new`'s versions go live.
//!
//! `settle_before_serve = true` is the fix (a `Stage` is gated on `served_term ==
//! current_term`, which only `Settle` — the rise sweep — establishes); `false` is the
//! pre-fix bug (writes serve immediately, before the inherited markers are swept). The teeth
//! test proves the checker CATCHES the duplicate with the gate off; the positive test proves
//! the invariant HOLDS with it on. Mirrors the SP21 Stage-idempotency model in
//! `crossrange_2pc_model.rs` (positive + teeth tests) and the SP7 counter model in `model.rs`.

use stateright::{Checker, Model, Property};

/// The committed base version the row starts with (the seed). Its `g` is always
/// decided-committed, so the seed is live until a committed write supersedes it.
const G_SEED: u64 = 0;
/// The inherited in-doubt marker's global xid (the write the dead leader had Prepared).
const G_OLD: u64 = 1;
/// The fresh cross-range write's global xid (issued by the newly-risen leader).
const G_NEW: u64 = 2;

/// One MVCC row version: created under global xid `creator_g`, optionally superseded
/// (deleted) by the global xid `xmax_g` (the `Stage` that re-read and replaced it).
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Version {
    creator_g: u64,
    xmax_g: Option<u64>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    /// The row's versions, kept sorted so logically-equal states fingerprint equally
    /// (the BFS dedups on `Hash`).
    versions: Vec<Version>,
    /// Write-once global clog: `(g, committed)`, kept sorted by `g`. A `g` appears at most
    /// once (the first decision wins).
    decided: Vec<(u64, bool)>,
    /// The session-less inherited in-doubt marker the new leader found in its durable store
    /// on rise: `true` once present, resolved by `ResolveInherited`/`Settle`. Its destined
    /// decision is seeded into `decided[G_OLD]` already (the coordinator durably committed it).
    inherited_present: bool,
    /// Whether the inherited marker has been resolved/applied — i.e. `G_OLD`'s version is
    /// now eligible to go live per the clog. Only `ResolveInherited` or `Settle` sets this.
    inherited_resolved: bool,
    /// Whether the fresh cross-range write `G_NEW` has been staged onto the row.
    new_staged: bool,
    /// The term whose rise-sweep has completed (the serving gate is open for this term).
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

    /// Whether a deleter global xid `x` is in effect — i.e. it actually removes the version
    /// it supersedes. A committed deleter normally is; but the inherited `G_OLD` supersession
    /// only takes effect once the inherited marker has been resolved/applied (until then the
    /// inherited write is not yet visible, so the seed it superseded is still the live base —
    /// exactly the stale-base window the gate protects).
    fn deleter_in_effect(&self, x: u64) -> bool {
        if !self.committed(x) {
            return false;
        }
        if x == G_OLD && !self.inherited_resolved {
            return false;
        }
        true
    }

    /// A version is live iff its creator's `g` committed, it is not superseded by an
    /// in-effect committed deleter, AND (for the inherited `G_OLD` version) its marker has
    /// been resolved/applied — an unresolved inherited marker is not yet visible.
    fn live(&self, v: &Version) -> bool {
        if !self.committed(v.creator_g) {
            return false;
        }
        if v.creator_g == G_OLD && !self.inherited_resolved {
            return false;
        }
        match v.xmax_g {
            None => true,
            Some(x) => !self.deleter_in_effect(x),
        }
    }

    fn live_count(&self) -> usize {
        self.versions.iter().filter(|v| self.live(v)).count()
    }

    /// The currently-visible version a fresh `Stage` re-reads + supersedes: the highest-`g`
    /// version that is live RIGHT NOW. With the inherited marker unsettled, `G_OLD`'s
    /// version is invisible, so this returns the stale base (the seed) — exactly the stale
    /// read the gate exists to prevent.
    fn visible_creator_g(&self) -> Option<u64> {
        self.versions
            .iter()
            .filter(|v| self.live(v))
            .map(|v| v.creator_g)
            .max()
    }

    /// The gate is open iff this term's rise-sweep has completed.
    fn gate_open(&self) -> bool {
        self.served_term == self.current_term
    }

    fn canonicalize(&mut self) {
        self.versions.sort();
        self.decided.sort();
    }
}

/// `settle_before_serve` toggles the SP22 fix: `true` = a `Stage` is admitted only once the
/// term's rise-sweep has settled the inherited markers (`served_term == current_term`);
/// `false` = the pre-fix bug where the new leader serves writes immediately, before the
/// inherited in-doubt markers are swept.
struct SettleModel {
    max_steps: usize,
    settle_before_serve: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    /// The fresh cross-range write under `G_NEW`. GATED: with the fix on it is admitted
    /// only when `served_term == current_term`. It re-reads the visible row and supersedes
    /// whatever is visible — the stale base if the inherited marker is unsettled.
    Stage,
    /// Resolve the inherited marker out-of-band (not via the rise sweep): `G_OLD`'s version
    /// becomes live per its seeded clog decision. Models the marker resolving on its own
    /// (e.g. a later read or the coordinator's resolve) AFTER a write already served.
    ResolveInherited,
    /// Decide the fresh write's global xid `G_NEW` (write-once); commit makes its version live.
    DecideNew(bool),
    /// The rise sweep: resolve the inherited marker THEN open the gate for the current term.
    /// This is the ONLY path that opens the gate.
    Settle,
    /// A new leadership term: the gate re-closes until the next `Settle`.
    Rise,
}

impl Model for SettleModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            // The seed plus the inherited Prepared(-> G_OLD) version. When the dead leader
            // Prepared `G_OLD` it re-read the seed and SUPERSEDED it (set the seed's
            // `xmax_g = G_OLD`) — so the single correct visible chain is seed -> G_OLD. The
            // seed therefore goes dead the instant `G_OLD` is resolved+committed. `G_OLD` is
            // already decided-COMMIT (the coordinator durably committed it before the
            // participant leader died — the dangerous case), so once the marker resolves
            // `G_OLD`'s version is destined to go live as the sole head.
            versions: vec![
                Version {
                    creator_g: G_SEED,
                    xmax_g: Some(G_OLD),
                },
                Version {
                    creator_g: G_OLD,
                    xmax_g: None,
                },
            ],
            decided: vec![(G_SEED, true), (G_OLD, true)],
            inherited_present: true,
            inherited_resolved: false,
            new_staged: false,
            // A leader has just risen (term 1) but has NOT yet swept (served_term 0): the
            // gate starts CLOSED, modelling the rise-before-sweep window the fix protects.
            served_term: 0,
            current_term: 1,
            steps: 0,
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        if s.steps >= self.max_steps {
            return;
        }
        out.push(Action::Stage);
        out.push(Action::ResolveInherited);
        out.push(Action::DecideNew(true));
        out.push(Action::DecideNew(false));
        out.push(Action::Settle);
        out.push(Action::Rise);
    }

    fn next_state(&self, s: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut n = s.clone();
        n.steps += 1;
        match a {
            Action::Stage => {
                // The serving gate: with the fix on, refuse to admit a write until this
                // term's rise-sweep has settled the inherited markers. A blocked Stage adds
                // no new state (no transition) — the writer never reads the unsettled marker.
                if self.settle_before_serve && !n.gate_open() {
                    return None;
                }
                // A write stages G_NEW at most once.
                if n.new_staged {
                    return None;
                }
                // Re-read the visible row and supersede it. With the inherited marker
                // unsettled (gate off) the visible head is the STALE BASE (the seed), so the
                // inherited G_OLD version is left un-superseded — the double-apply seed.
                if let Some(vg) = n.visible_creator_g()
                    && let Some(v) = n.versions.iter_mut().find(|v| v.creator_g == vg)
                {
                    v.xmax_g = Some(G_NEW);
                }
                n.versions.push(Version {
                    creator_g: G_NEW,
                    xmax_g: None,
                });
                n.new_staged = true;
            }
            Action::ResolveInherited => {
                if !n.inherited_present {
                    return None;
                }
                if n.inherited_resolved {
                    return None; // already resolved — no new state
                }
                // The marker resolves per its seeded clog decision (G_OLD is committed): its
                // version becomes live-eligible.
                n.inherited_resolved = true;
            }
            Action::DecideNew(commit) => {
                if !n.new_staged {
                    return None;
                }
                // Write-once: the first decision for G_NEW wins.
                if !n.decided.iter().any(|&(gg, _)| gg == G_NEW) {
                    n.decided.push((G_NEW, commit));
                } else {
                    return None; // no new state
                }
            }
            Action::Settle => {
                // The rise sweep: resolve every inherited marker, THEN open the gate for the
                // current term. Idempotent — a Settle that changes nothing adds no state.
                if n.inherited_resolved && n.gate_open() {
                    return None;
                }
                n.inherited_resolved = true;
                n.served_term = n.current_term;
            }
            Action::Rise => {
                // A new leadership term re-closes the gate (served_term now lags) until the
                // next Settle. A fresh rise also re-presents the (already-resolved-or-not)
                // inherited marker as un-swept for the new term: model the conservative case
                // by leaving `inherited_resolved` as-is but re-closing the gate.
                n.current_term += 1;
            }
        }
        n.canonicalize();
        Some(n)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety (the MVCC invariant `scan_live`/`find_visible_one` debug_assert): a row
            // has AT MOST ONE live version under any snapshot. Two live versions — the
            // inherited G_OLD and the fresh G_NEW — is exactly the settle-before-serve
            // violation the gate prevents. This is the load-bearing, teeth-bearing invariant.
            Property::<Self>::always("at most one live version", |_, s| s.live_count() <= 1),
            // Corroborating, mechanism-side: a fresh write must never supersede the STALE
            // BASE past an un-swept inherited marker. The fresh `G_NEW` write re-reads the
            // visible head and replaces it; the only correct head it can replace (once the
            // committed inherited `G_OLD` exists) is `G_OLD`'s own version. If instead the
            // seed is left bearing `xmax_g == Some(G_NEW)` while `G_OLD`'s version was NOT the
            // one superseded, the writer read the stale base before the marker was swept —
            // exactly the pre-fix behavior. Equivalently: whenever `G_NEW` exists and `G_OLD`
            // is committed, `G_OLD`'s version must carry `xmax_g == Some(G_NEW)`.
            Property::<Self>::always(
                "no write supersedes an unsettled inherited marker",
                |_, s| {
                    let g_new_exists = s.versions.iter().any(|v| v.creator_g == G_NEW);
                    if !g_new_exists || !s.committed(G_OLD) {
                        return true;
                    }
                    s.versions
                        .iter()
                        .any(|v| v.creator_g == G_OLD && v.xmax_g == Some(G_NEW))
                },
            ),
        ]
    }
}

/// The fixed system: a write is gated on the term's rise-sweep having settled the inherited
/// markers (`settle_before_serve = true`). Exhaustively explore every interleaving of
/// stage / resolve / decide / settle / rise and assert NO property is ever violated. A
/// counterexample here would mean settle-before-serve is itself unsound — a genuine design
/// finding; do not weaken the properties.
#[test]
fn settle_before_serve_upholds_at_most_one_live() {
    let checker = SettleModel {
        max_steps: 7,
        settle_before_serve: true,
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

/// Teeth: the SAME model with the serving gate REMOVED (the pre-fix bug — the new leader
/// serves writes immediately, before sweeping the inherited in-doubt markers). A fresh
/// `Stage` runs while the inherited `G_OLD` marker is unsettled, supersedes the stale base,
/// and when the marker resolves to committed BOTH `G_OLD`'s and `G_NEW`'s versions go live.
/// This asserts the checker actually CATCHES the duplicate — `discoveries()` is non-empty
/// and names the load-bearing `"at most one live version"` property — proving the passing
/// test above is meaningful and not vacuously accepting everything.
#[test]
fn no_settle_serves_stale_and_double_lives_is_caught() {
    let checker = SettleModel {
        max_steps: 7,
        settle_before_serve: false,
    }
    .checker()
    .spawn_bfs()
    .join();

    let discoveries = checker.discoveries();
    assert!(
        !discoveries.is_empty(),
        "removing the serving gate must produce a property counterexample; if empty the \
         model has no teeth and the passing test is meaningless"
    );
    assert!(
        discoveries.contains_key("at most one live version"),
        "expected an 'at most one live version' counterexample from the no-gate variant \
         (the inherited G_OLD and fresh G_NEW versions both go live), got: {:?}",
        discoveries.keys().collect::<Vec<_>>()
    );
}
